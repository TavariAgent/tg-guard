# -*- coding: utf-8 -*-
# convergence_engine.py
"""
Pure convergence engine for worker-pattern adaptation.

Replaces prometheus_convergence.py entirely. All pressure signals are derived
from internally recorded execution data — no external metrics libraries,
no text serialization round trips, no OS-level CPU polling.

Signal architecture:
    ASI (Accumulative Swing Index)
        Built from executor-duration batches reported by _execute_token.
        Each set of tasks completing at the same timestamp forms one bar.
        Open  = fastest executor duration in the batch (no-drag baseline).
        Close = mean executor duration across the batch.
        Swing = Close - Open (deviation from baseline, accumulated over time).
        A rising ASI indicates drag building on a core.
        A flat or falling ASI indicates healthy convergence.

    Queue wait p95
        Recorded per-token at dequeue time in the worker loop.
        5-second accumulation window — fastest-reacting signal.
        Spikes immediately on saturation.

    Weight heuristic utilization
        Rolling score derived from task weight (heavy/medium/light) tags.
        10-second accumulation window — leading pressure indicator.

All deques are bounded to MAILBOX_MAX — the maximum tokens any core can hold
at one time. This keeps history aligned with actual system capacity rather
than a theoretical throughput estimate. Per-signal windows are enforced at
read time, not at write time, so no data is discarded prematurely.

Public interface (matches PrometheusConvergenceEngine exactly):
    record_execution_sample(core_id, executor_duration)
    record_wait_sample(core_id, wait_seconds)
    record_task_weight(core_id, weight_name)
    analyze_cores(worker_pool) -> List[CorePressure]
    recommend_adjustments(pressures) -> Dict[int, WorkerPattern]
    apply_pattern(core_id, pattern, worker_pool)
    get_convergence_status() -> dict
"""

import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any

from .token_options import option
from .threading_metrics import get_metrics
from .tg_print import tg_print

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Hard ceiling on sample retention — no sample older than this is ever used
_WINDOW_HARD_CEILING = 30.0  # seconds

# Per-signal accumulation windows (all within the hard ceiling)
_WINDOW_WAIT      = 5.0   # queue wait — fast spike detection
_WINDOW_WEIGHT    = 10.0  # weight heuristic — leading indicator
_WINDOW_DURATION  = 10.0  # executor duration — for ASI bar construction
_WINDOW_ASI       = 30.0  # ASI history — full ceiling, trend needs range

# Deque capacity aligned to MAILBOX_MAX — the maximum tokens a core can ever
# hold at one time is bounded by the mailbox depth, so history can never
# meaningfully exceed that count. Read once at import time; MAILBOX_MAX is a
# frozen setting that must be configured before coordinator.start().
_DEQUE_MAXLEN = option.MAILBOX_MAX

# Weight numeric scores — heavy task scores 3x a light task
_WEIGHT_SCORE: Dict[str, float] = {
    'heavy':  3.0,
    'medium': 2.0,
    'light':  1.0,
}


# ---------------------------------------------------------------------------
# Enums and dataclasses
# ---------------------------------------------------------------------------

class WorkerPattern(Enum):
    """Per-core worker allocation patterns."""
    HEAVY  = 2  # 2 workers — high contention, reduce context switching
    MEDIUM = 3  # 3 workers — balanced default
    LIGHT  = 4  # 4 workers — underutilized, expand throughput


@dataclass
class CorePressure:
    """Observed pressure summary and pattern recommendation for one core."""
    core_id:            int
    queue_depth:        int
    worker_utilization: float          # 0–100 %
    queue_wait_p95:     Optional[float]  # seconds
    avg_task_duration:  Optional[float]  # seconds
    asi:                float          # accumulated swing index
    asi_trend:          float          # slope over recent ASI history
    pressure_level:     str            # 'overloaded' | 'balanced' | 'underutilized'
    recommended_pattern: WorkerPattern


# ---------------------------------------------------------------------------
# ConvergenceEngine
# ---------------------------------------------------------------------------

class ConvergenceEngine:
    """
    Convergence engine driven entirely by internal execution metrics.

    Receives executor-duration batches, queue wait samples, and task weight
    records directly from the worker pipeline. Derives per-core pressure
    from an Accumulative Swing Index plus wait and utilization heuristics,
    then recommends and applies worker-pattern adjustments.
    """

    def __init__(
            self,
            topology: Any,
            worker_queue: Any,
            queue_wait_threshold: Optional[float] = None,
            utilization_high:     Optional[float] = None,
            utilization_low:      Optional[float] = None,
            queue_depth_factor:   Optional[float] = None,
    ) -> None:
        """
        Initialize the convergence engine.

        Args:
            topology:             CPU topology provider (physical_cores count).
            worker_queue:         Live CorePinnedStaggeredQueue reference.
                                  Used to read core_queue_depth and
                                  core_patterns directly.
            queue_wait_threshold: p95 queue wait above this triggers overload.
            utilization_high:     Utilization % indicating saturation.
            utilization_low:      Utilization % indicating underutilization.
            queue_depth_factor:   Multiplier for queue-depth overload check.
        """
        self.topology     = topology
        self.worker_queue = worker_queue
        self.metrics      = get_metrics()

        # Thresholds — fall back to global options when not provided
        self.queue_wait_threshold = (
            queue_wait_threshold
            if queue_wait_threshold is not None
            else option.QUEUE_WAIT_THRESHOLD
        )
        self.utilization_high = (
            utilization_high
            if utilization_high is not None
            else option.UTILIZATION_HIGH
        )
        self.utilization_low = (
            utilization_low
            if utilization_low is not None
            else option.UTILIZATION_LOW
        )
        self.queue_depth_factor = (
            queue_depth_factor
            if queue_depth_factor is not None
            else option.QUEUE_DEPTH_FACTOR
        )

        num_cores = topology.physical_cores

        # Current pattern per core — start LIGHT (most workers, least pressure assumed)
        self.core_patterns: Dict[int, WorkerPattern] = {
            core_id: WorkerPattern.LIGHT
            for core_id in range(1, num_cores + 1)
        }

        # ── Rolling sample deques ────────────────────────────────────────────
        # Each entry is (monotonic_timestamp, value).
        # All bounded at _DEQUE_MAXLEN; window enforcement happens at read time.

        # Executor durations from _execute_token — raw material for ASI bars
        # Stored as (timestamp, duration) for batch grouping at poll time
        self._duration_history: Dict[int, deque[tuple[float, float]]] = {
            core_id: deque(maxlen=_DEQUE_MAXLEN)
            for core_id in range(1, num_cores + 1)
        }

        # Queue wait times from worker loop dequeue
        self._wait_history: Dict[int, deque[tuple[float, float]]] = {
            core_id: deque(maxlen=_DEQUE_MAXLEN)
            for core_id in range(1, num_cores + 1)
        }

        # Task weight scores for heuristic utilization
        self._weight_history: Dict[int, deque[tuple[float, float]]] = {
            core_id: deque(maxlen=_DEQUE_MAXLEN)
            for core_id in range(1, num_cores + 1)
        }

        # ── ASI state ────────────────────────────────────────────────────────

        # Live accumulated swing value per core
        self._asi: Dict[int, float] = {
            core_id: 0.0
            for core_id in range(1, num_cores + 1)
        }

        # ASI history for trend calculation: (timestamp, asi_value)
        self._asi_history: Dict[int, deque[tuple[float, float]]] = {
            core_id: deque(maxlen=_DEQUE_MAXLEN)
            for core_id in range(1, num_cores + 1)
        }

        # Convergence change log — last 100 pattern transitions
        self.convergence_history: List[dict[str, Any]] = []

        tg_print('convergence', f'ConvergenceEngine initialized  '
                                f'cores={num_cores}  ' f'wait_window={_WINDOW_WAIT}s  '
                                f'weight_window={_WINDOW_WEIGHT}s  ' f'asi_window={_WINDOW_ASI}s  '
                                f'ceiling={_WINDOW_HARD_CEILING}s')

    # ── Sample ingestion ─────────────────────────────────────────────────────

    def record_execution_sample(self, core_id: int, executor_duration: float) -> None:
        """Record one executor duration stamped at call time.

        Called from _execute_token immediately after run_in_executor returns,
        before any bookkeeping. This is the purest available measure of actual
        CPU cost for a task — admission and queue wait are excluded.

        Args:
            core_id:           1-based physical core the token ran on.
            executor_duration: Wall time of the run_in_executor call in seconds.
        """
        if core_id not in self._duration_history:
            return
        self._duration_history[core_id].append((time.monotonic(), executor_duration))
        self._update_asi(core_id)

    def record_wait_sample(self, core_id: int, wait_seconds: float) -> None:
        """Record a queue wait time sample.

        Called from the worker loop at dequeue time:
            wait = time.perf_counter() - float(token.metadata.tags['enqueued_at'])

        Args:
            core_id:      1-based physical core.
            wait_seconds: Time the token spent waiting in the mailbox.
        """
        if core_id not in self._wait_history:
            return
        self._wait_history[core_id].append((time.monotonic(), wait_seconds))

    def record_task_weight(self, core_id: int, weight_name: str) -> None:
        """Record an incoming task weight for heuristic utilization.

        Called when a task is routed so the engine can gauge per-core pressure
        from the frequency and heaviness of recent work.

        Args:
            core_id:     1-based physical core the task was routed to.
            weight_name: One of 'heavy', 'medium', or 'light'.
        """
        if core_id not in self._weight_history:
            return
        score = _WEIGHT_SCORE.get(weight_name.lower(), 2.0)
        self._weight_history[core_id].append((time.monotonic(), score))

    # ── ASI construction ─────────────────────────────────────────────────────

    def _update_asi(self, core_id: int) -> None:
        """Rebuild the ASI bar from the current duration window and accumulate.

        Groups duration samples within _WINDOW_DURATION into one bar.
        A bar requires at least 2 samples — single-task windows produce no swing.

        Bar definition:
            Open  = min(durations)   — the no-drag baseline for this batch
            Close = mean(durations)  — where the group actually landed
            Swing = Close - Open     — drag deviation, always >= 0

        The swing is added to the running ASI. The ASI is then snapshotted
        into _asi_history for trend analysis.
        """
        history = self._duration_history.get(core_id)
        if not history:
            return

        now    = time.monotonic()
        cutoff = now - _WINDOW_DURATION

        # Collect durations within the active window
        recent = [dur for ts, dur in history if ts >= cutoff]

        if len(recent) < 2:
            # Not enough data for a meaningful bar — no swing recorded
            return

        open_  = min(recent)
        close  = sum(recent) / len(recent)
        swing  = close - open_   # deviation from the fastest task's baseline

        # Accumulate — rising ASI = drag building, flat/falling = healthy
        self._asi[core_id] += swing

        # Snapshot for trend analysis
        self._asi_history[core_id].append((now, self._asi[core_id]))

        tg_print('convergence',
                 f'Core {core_id} ASI bar  '
                 f'open={open_:.4f}s  close={close:.4f}s  '
                 f'swing={swing:.4f}  asi={self._asi[core_id]:.4f}',
                 level='debug')

    def _get_asi_trend(self, core_id: int) -> float:
        """Return the slope of the ASI over the recent history window.

        Uses a simple rise-over-run between the oldest and newest ASI snapshots
        within _WINDOW_ASI. Positive = ASI rising (drag building).
        Negative = ASI falling (core recovering). Zero = stable.

        Returns:
            Slope in ASI-units per second. 0.0 if insufficient history.
        """
        history = self._asi_history.get(core_id)
        if not history or len(history) < 2:
            return 0.0

        now    = time.monotonic()
        cutoff = now - _WINDOW_ASI

        window = [(ts, val) for ts, val in history if ts >= cutoff]
        if len(window) < 2:
            return 0.0

        # Oldest and newest points in the window
        ts_old, asi_old = window[0]
        ts_new, asi_new = window[-1]

        elapsed = ts_new - ts_old
        if elapsed < 1e-9:
            return 0.0

        return (asi_new - asi_old) / elapsed

    # ── Utilization heuristic ────────────────────────────────────────────────

    def gauge_utilization(self, core_id: int) -> float:
        """Return heuristic utilization (0–100) for core_id.

        Derived from the average weight score of tasks that arrived within
        the last _WINDOW_WEIGHT seconds, scaled by task frequency.

        A stream of heavy tasks at full window capacity → ~100 %.
        A stream of light tasks → ~33 %.
        No recent tasks → 0 %.

        Returns:
            Float in [0.0, 100.0].
        """
        history = self._weight_history.get(core_id)
        if not history:
            return 0.0

        now    = time.monotonic()
        cutoff = now - _WINDOW_WEIGHT

        recent_scores = [score for ts, score in history if ts >= cutoff]
        if not recent_scores:
            return 0.0

        avg_score      = sum(recent_scores) / len(recent_scores)
        max_score      = _WEIGHT_SCORE['heavy']  # 3.0

        # Frequency factor: window_capacity is the max samples we expect in
        # _WINDOW_WEIGHT at the observed 7-task-per-tick rate (~70 tasks/s * 10s)
        window_capacity = 700.0
        frequency_factor = min(1.0, len(recent_scores) / window_capacity)

        utilization = (avg_score / max_score) * frequency_factor * 100.0
        return round(utilization, 2)

    # ── p95 helper ───────────────────────────────────────────────────────────

    @staticmethod
    def _compute_p95(
            history: deque[tuple[float, float]],
            window_seconds: float = _WINDOW_WAIT
    ) -> Optional[float]:
        """Compute p95 from a rolling (timestamp, value) deque.

        Args:
            history:        Deque of (monotonic_timestamp, value) pairs.
            window_seconds: Only samples within this many seconds are used.

        Returns:
            p95 value in the same units as the recorded values, or None if
            fewer than 2 samples exist in the window.
        """
        if not history:
            return None

        now    = time.monotonic()
        recent = sorted(v for ts, v in history if ts >= now - window_seconds)

        if len(recent) < 2:
            return None

        idx = min(int(len(recent) * 0.95), len(recent) - 1)
        return recent[idx]

    # ── Core analysis ────────────────────────────────────────────────────────

    def analyze_cores(self, worker_pool: Any) -> List[CorePressure]:
        """Analyze all physical cores using live internal metrics.

        Reads queue depth directly from the worker queue's live dict —
        no text serialization, no Prometheus round trip.

        Args:
            worker_pool: WorkerPoolInterface (provides workers_per_core).

        Returns:
            List of CorePressure summaries, one per physical core.
        """
        pressures = []

        for core_id in range(1, self.topology.physical_cores + 1):
            pressure = self._analyze_single_core(core_id, worker_pool)
            pressures.append(pressure)

        return pressures

    def _analyze_single_core(self, core_id: int, worker_pool: Any) -> CorePressure:
        """Build one CorePressure summary from live internal state.

        Args:
            core_id:     1-based physical core index.
            worker_pool: WorkerPoolInterface for workers_per_core.

        Returns:
            CorePressure with all signals populated.
        """
        # Queue depth — read directly from the live worker queue dict
        queue_depth = self.worker_queue.core_queue_depth.get(core_id, 0)

        # Heuristic utilization from weight history
        utilization = self.gauge_utilization(core_id)

        # Queue wait p95 — 5s window, fastest signal
        wait_p95 = self._compute_p95(
            self._wait_history[core_id],
            window_seconds=_WINDOW_WAIT
        )

        # Average executor duration — 10s window
        avg_duration = self._compute_p95(
            self._duration_history[core_id],
            window_seconds=_WINDOW_DURATION
        )

        # ASI and its trend
        asi       = self._asi.get(core_id, 0.0)
        asi_trend = self._get_asi_trend(core_id)

        # Classify pressure
        pressure_level, recommended = self._assess_pressure(
            core_id,
            queue_depth,
            utilization,
            wait_p95,
            asi_trend,
            worker_pool,
        )

        return CorePressure(
            core_id=core_id,
            queue_depth=int(queue_depth),
            worker_utilization=utilization,
            queue_wait_p95=wait_p95,
            avg_task_duration=avg_duration,
            asi=asi,
            asi_trend=asi_trend,
            pressure_level=pressure_level,
            recommended_pattern=recommended,
        )

    def _assess_pressure(
            self,
            core_id:       int,
            queue_depth:   float,
            utilization:   float,
            wait_p95:      Optional[float],
            asi_trend:     float,
            worker_pool:   Any,
    ) -> Tuple[str, WorkerPattern]:
        """Classify core pressure and return the recommended worker pattern.

        Rules are evaluated in priority order. The first rule that fires wins.

        Rule 1 — Queue wait spike:    wait p95 > threshold           → OVERLOADED
        Rule 2 — Deep queue:          depth > workers * depth_factor  → OVERLOADED
        Rule 3 — Hot utilization:     util > high AND any queue depth → OVERLOADED
        Rule 4 — Rising ASI trend:    asi_trend > 0 AND util > low   → OVERLOADED
        Rule 5 — Underutilized:       util < low                     → UNDERUTILIZED
        Rule 6 — Falling ASI:         asi_trend < 0 AND util < high  → UNDERUTILIZED
        Rule 7 — Default:             everything else                 → BALANCED

        Args:
            core_id:     For logging only.
            queue_depth: Current mailbox depth for this core.
            utilization: Heuristic utilization 0–100.
            wait_p95:    p95 queue wait in seconds, or None.
            asi_trend:   ASI slope — positive = drag building.
            worker_pool: Provides workers_per_core for depth threshold.

        Returns:
            (pressure_level, WorkerPattern) tuple.
        """
        workers_per_core = worker_pool.workers_per_core if worker_pool else 4

        # Coerce None to 0.0 so all comparisons are safe
        wait_p95    = wait_p95    if wait_p95    is not None else 0.0
        queue_depth = queue_depth if queue_depth is not None else 0.0
        utilization = utilization if utilization is not None else 0.0

        # Rule 1 — queue wait spike
        if wait_p95 > self.queue_wait_threshold:
            tg_print('convergence',
                     f'Core {core_id}: wait_p95={wait_p95:.3f}s '
                     f'> threshold={self.queue_wait_threshold}s → overloaded')
            return 'overloaded', WorkerPattern.HEAVY

        # Rule 2 — deep queue
        depth_threshold = workers_per_core * self.queue_depth_factor
        if queue_depth > depth_threshold:
            tg_print('convergence',
                     f'Core {core_id}: queue_depth={queue_depth} '
                     f'> {depth_threshold} → overloaded')
            return 'overloaded', WorkerPattern.HEAVY

        # Rule 3 — hot utilization with queued work
        if utilization > self.utilization_high and queue_depth > 0.0:
            tg_print('convergence',
                     f'Core {core_id}: util={utilization:.1f}% '
                     f'> {self.utilization_high}% with queued work → overloaded')
            return 'overloaded', WorkerPattern.HEAVY

        # Rule 4 — rising ASI drag while core isn't idle
        if asi_trend > 0.0 and utilization > self.utilization_low:
            tg_print('convergence',
                     f'Core {core_id}: asi_trend={asi_trend:.4f} rising '
                     f'util={utilization:.1f}% → overloaded')
            return 'overloaded', WorkerPattern.HEAVY

        # Rule 5 — underutilized by utilization signal
        if utilization < self.utilization_low:
            tg_print('convergence',
                     f'Core {core_id}: util={utilization:.1f}% '
                     f'< {self.utilization_low}% → underutilized')
            return 'underutilized', WorkerPattern.LIGHT

        # Rule 6 — ASI actively falling and core not saturated
        if asi_trend < 0.0 and utilization < self.utilization_high:
            tg_print('convergence',
                     f'Core {core_id}: asi_trend={asi_trend:.4f} falling '
                     f'util={utilization:.1f}% → underutilized')
            return 'underutilized', WorkerPattern.LIGHT

        # Rule 7 — default
        return 'balanced', WorkerPattern.MEDIUM

    # ── Recommendations and application ──────────────────────────────────────

    def recommend_adjustments(
            self,
            core_pressures: List[CorePressure]
    ) -> Dict[int, WorkerPattern]:
        """Return only the per-core pattern changes that differ from current state.

        Args:
            core_pressures: Output of analyze_cores().

        Returns:
            Dict mapping core_id → new WorkerPattern for cores that need change.
        """
        adjustments = {}

        for pressure in core_pressures:
            current     = self.core_patterns[pressure.core_id]
            recommended = pressure.recommended_pattern

            if current != recommended:
                adjustments[pressure.core_id] = recommended

                tg_print('convergence',
                         f'Core {pressure.core_id}: '
                         f'{current.name} → {recommended.name}  '
                         f'reason={pressure.pressure_level}  '
                         f'asi={pressure.asi:.4f}  '
                         f'trend={pressure.asi_trend:.4f}')
                tg_print('convergence',
                         f'depth={pressure.queue_depth}  '
                         f'util={pressure.worker_utilization:.1f}%' +
                         (f'  wait_p95={pressure.queue_wait_p95:.3f}s'
                          if pressure.queue_wait_p95 else ''),
                         level='debug')

        return adjustments

    def apply_pattern(
            self,
            core_id:     int,
            pattern:     WorkerPattern,
            worker_pool: Any = None,
    ) -> None:
        """Apply a recommended pattern, update metrics, and log the transition.

        Args:
            core_id:     1-based physical core.
            pattern:     New WorkerPattern to apply.
            worker_pool: If provided and supports set_pattern(), the live pool
                         is updated immediately.
        """
        old_pattern = self.core_patterns[core_id]
        self.core_patterns[core_id] = pattern

        self.metrics.update_pattern(core_id, pattern.value)

        if old_pattern != pattern:
            if worker_pool and hasattr(worker_pool, 'set_pattern'):
                try:
                    worker_pool.set_pattern(core_id, pattern.value)
                    tg_print('convergence',
                             f'Core {core_id}: pattern applied to worker pool',
                             level='dispatch')
                except Exception as e:
                    tg_print('convergence',
                             f'Core {core_id}: could not apply pattern: {e}',
                             level='warn')

            self.metrics.record_convergence_change(
                core_id,
                old_pattern.value,
                pattern.value,
            )

            self.convergence_history.append({
                'timestamp':    time.time(),
                'core_id':      core_id,
                'from_pattern': old_pattern.name,
                'to_pattern':   pattern.name,
            })

            # Reset ASI on pattern change — the system is in a new state,
            # old accumulated drag is no longer representative
            self._asi[core_id] = 0.0
            tg_print('convergence',
                     f'Core {core_id}: ASI reset after pattern change',
                     level='debug')

            tg_print('convergence',
                     f'Core {core_id}: '
                     f'{old_pattern.name} ({old_pattern.value} workers) '
                     f'→ {pattern.name} ({pattern.value} workers)',
                     level='state')

    # ── Status and diagnostics ───────────────────────────────────────────────

    def get_weight_summary(self, core_id: int) -> dict[str, int | float]:
        """Return a diagnostic summary of recent weight observations."""
        history = self._weight_history.get(core_id)
        if not history:
            return {'recent_tasks': 0, 'avg_weight': 0.0, 'heuristic_util': 0}

        now    = time.monotonic()
        cutoff = now - _WINDOW_WEIGHT
        recent = [(ts, s) for ts, s in history if ts >= cutoff]

        if not recent:
            return {'recent_tasks': 0, 'avg_weight': 0.0, 'heuristic_util': 0}

        avg = sum(s for _, s in recent) / len(recent)
        return {
            'recent_tasks':   len(recent),
            'avg_weight':     round(avg, 2),
            'heuristic_util': self.gauge_utilization(core_id),
        }

    def get_asi_summary(self, core_id: int) -> dict[str, float | str]:
        """Return current ASI state and trend for one core."""
        return {
            'asi':       round(self._asi.get(core_id, 0.0), 6),
            'trend':     round(self._get_asi_trend(core_id), 6),
            'direction': (
                'rising'  if self._get_asi_trend(core_id) > 0 else
                'falling' if self._get_asi_trend(core_id) < 0 else
                'stable'
            ),
        }

    def get_convergence_status(self) -> dict[str, Any]:
        """Return a full convergence state snapshot."""
        distribution = {
            'heavy':  sum(1 for p in self.core_patterns.values() if p == WorkerPattern.HEAVY),
            'medium': sum(1 for p in self.core_patterns.values() if p == WorkerPattern.MEDIUM),
            'light':  sum(1 for p in self.core_patterns.values() if p == WorkerPattern.LIGHT),
        }

        return {
            'core_patterns': {
                core_id: pattern.name
                for core_id, pattern in self.core_patterns.items()
            },
            'pattern_distribution': distribution,
            'total_changes':        len(self.convergence_history),
            'recent_changes':       self.convergence_history[-5:] if self.convergence_history else [],
            'asi': {
                core_id: self.get_asi_summary(core_id)
                for core_id in self.core_patterns
            },
            'weight_gauge': {
                core_id: self.get_weight_summary(core_id)
                for core_id in self.core_patterns
            },
        }
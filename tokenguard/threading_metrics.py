# -*- coding: utf-8 -*-
# threading_metrics.py
"""
Internal metrics for token-managed execution.

Provides counters, gauges, and histograms for task lifecycle events,
queue behavior, worker state, and convergence pattern changes.

All metrics are stored as plain Python dicts and counters — no external
monitoring library required. State is accessible via get_snapshot() for
logging, diagnostics, or external export if needed.
"""

import threading
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple


class ThreadingMetrics:
    """Central metrics store for the execution system.

    Maintains the same logical signals as the previous Prometheus-backed
    implementation — submission counters, completion counters, failure
    counters, queue depth gauges, worker state gauges, duration
    histograms, and convergence change counters — using plain dicts
    and thread-safe accumulators.

    All methods are drop-in replacements for the previous API.
    """

    def __init__(self):
        self._lock = threading.Lock()

        # ── Task lifecycle counters ──────────────────────────────────────────

        # Total submissions per operation_type
        self._tasks_submitted: Dict[str, int] = defaultdict(int)

        # Total completions keyed by (operation_type, core_id)
        self._tasks_completed: Dict[Tuple[str, int], int] = defaultdict(int)

        # Total failures keyed by (operation_type, core_id)
        self._tasks_failed: Dict[Tuple[str, int], int] = defaultdict(int)

        # ── Duration tracking ────────────────────────────────────────────────
        # Keyed by (operation_type, core_id)
        # Stored as (sum, count) for average calculation without retaining
        # every sample — memory stays flat regardless of task volume.

        self._duration_sum:   Dict[Tuple[str, int], float] = defaultdict(float)
        self._duration_count: Dict[Tuple[str, int], int]   = defaultdict(int)
        self._duration_min:   Dict[Tuple[str, int], float] = {}
        self._duration_max:   Dict[Tuple[str, int], float] = {}

        # ── Queue wait tracking ──────────────────────────────────────────────
        # Keyed by core_id — same (sum, count) pattern

        self._wait_sum:   Dict[int, float] = defaultdict(float)
        self._wait_count: Dict[int, int]   = defaultdict(int)
        self._wait_max:   Dict[int, float] = {}

        # ── Live gauges ──────────────────────────────────────────────────────

        # Current mailbox depth per core_id
        self._queue_depth: Dict[int, int] = defaultdict(int)

        # Current busy/idle worker counts per core_id
        self._workers_busy: Dict[int, int] = defaultdict(int)
        self._workers_idle: Dict[int, int] = defaultdict(int)

        # Derived utilization percentage per core_id
        self._worker_utilization: Dict[int, float] = defaultdict(float)

        # Current worker pattern per core_id (2=HEAVY, 3=MEDIUM, 4=LIGHT)
        self._worker_pattern: Dict[int, int] = {}

        # ── Convergence counters ─────────────────────────────────────────────
        # Keyed by (core_id, from_pattern, to_pattern)

        self._convergence_changes: Dict[Tuple[int, int, int], int] = defaultdict(int)

        # Total convergence events for quick summary
        self._total_convergence_changes: int = 0

    # ── Task lifecycle ───────────────────────────────────────────────────────

    def record_task_submission(self, operation_type: str):
        """Increment the submitted-task counter for an operation type."""
        with self._lock:
            self._tasks_submitted[operation_type] += 1

    def record_task_completion(self, operation_type: str, core_id: int, duration: float):
        """Record a successful completion and accumulate execution duration."""
        key = (operation_type, core_id)
        with self._lock:
            self._tasks_completed[key] += 1
            self._duration_sum[key]   += duration
            self._duration_count[key] += 1

            # Track min/max without storing every sample
            if key not in self._duration_min or duration < self._duration_min[key]:
                self._duration_min[key] = duration
            if key not in self._duration_max or duration > self._duration_max[key]:
                self._duration_max[key] = duration

    def record_task_failure(self, operation_type: str, core_id: int):
        """Increment the failed-task counter for an operation/core pair."""
        with self._lock:
            self._tasks_failed[(operation_type, core_id)] += 1

    # ── Queue wait ───────────────────────────────────────────────────────────

    def record_queue_wait(self, core_id: int, wait_time: float):
        """Accumulate queue wait time for a core."""
        with self._lock:
            self._wait_sum[core_id]   += wait_time
            self._wait_count[core_id] += 1
            if core_id not in self._wait_max or wait_time > self._wait_max[core_id]:
                self._wait_max[core_id] = wait_time

    # ── Live gauges ──────────────────────────────────────────────────────────

    def update_queue_depth(self, core_id: int, depth: int):
        """Set the current queue depth for a core."""
        with self._lock:
            self._queue_depth[core_id] = depth

    def update_worker_state(self, core_id: int, busy_count: int, idle_count: int):
        """Update busy/idle worker counts and derived utilization for a core."""
        with self._lock:
            self._workers_busy[core_id] = busy_count
            self._workers_idle[core_id] = idle_count
            total = busy_count + idle_count
            if total > 0:
                self._worker_utilization[core_id] = (busy_count / total) * 100.0
            else:
                self._worker_utilization[core_id] = 0.0

    def update_pattern(self, core_id: int, pattern_value: int):
        """Set the active worker-pattern for a core."""
        with self._lock:
            self._worker_pattern[core_id] = pattern_value

    # ── Convergence ──────────────────────────────────────────────────────────

    def record_convergence_change(
            self,
            core_id:      int,
            from_pattern: int,
            to_pattern:   int,
    ):
        """Increment the convergence-triggered pattern-change counter."""
        with self._lock:
            self._convergence_changes[(core_id, from_pattern, to_pattern)] += 1
            self._total_convergence_changes += 1

    # ── Snapshot ─────────────────────────────────────────────────────────────

    def get_snapshot(self) -> dict:
        """Return a point-in-time snapshot of all metrics.

        Safe to call from any thread. The snapshot is a plain dict suitable
        for logging, JSON serialization, or external export.
        """
        with self._lock:
            # Build per-(op, core) duration averages
            duration_stats = {}
            for (op, core), count in self._duration_count.items():
                key = f'{op}:core_{core}'
                duration_stats[key] = {
                    'count': count,
                    'avg':   round(self._duration_sum[(op, core)] / count, 6) if count else 0.0,
                    'min':   round(self._duration_min.get((op, core), 0.0), 6),
                    'max':   round(self._duration_max.get((op, core), 0.0), 6),
                }

            # Build per-core wait averages
            wait_stats = {}
            for core_id, count in self._wait_count.items():
                wait_stats[f'core_{core_id}'] = {
                    'count': count,
                    'avg':   round(self._wait_sum[core_id] / count, 6) if count else 0.0,
                    'max':   round(self._wait_max.get(core_id, 0.0), 6),
                }

            # Convergence change breakdown
            convergence = {
                f'core_{c}_from_{f}_to_{t}': n
                for (c, f, t), n in self._convergence_changes.items()
            }

            return {
                'tasks': {
                    'submitted':  dict(self._tasks_submitted),
                    'completed':  {f'{op}:core_{c}': n for (op, c), n in self._tasks_completed.items()},
                    'failed':     {f'{op}:core_{c}': n for (op, c), n in self._tasks_failed.items()},
                },
                'duration':  duration_stats,
                'wait':      wait_stats,
                'gauges': {
                    'queue_depth':        dict(self._queue_depth),
                    'workers_busy':       dict(self._workers_busy),
                    'workers_idle':       dict(self._workers_idle),
                    'worker_utilization': {k: round(v, 2) for k, v in self._worker_utilization.items()},
                    'worker_pattern':     dict(self._worker_pattern),
                },
                'convergence': {
                    'total_changes': self._total_convergence_changes,
                    'breakdown':     convergence,
                },
            }

    def get_core_summary(self, core_id: int) -> dict:
        """Return a focused summary for one core — useful for per-core diagnostics."""
        with self._lock:
            wait_count = self._wait_count.get(core_id, 0)
            return {
                'core_id':      core_id,
                'queue_depth':  self._queue_depth.get(core_id, 0),
                'workers_busy': self._workers_busy.get(core_id, 0),
                'workers_idle': self._workers_idle.get(core_id, 0),
                'utilization':  round(self._worker_utilization.get(core_id, 0.0), 2),
                'pattern':      self._worker_pattern.get(core_id),
                'avg_wait':     round(
                    self._wait_sum[core_id] / wait_count, 6
                ) if wait_count else 0.0,
                'max_wait':     round(self._wait_max.get(core_id, 0.0), 6),
            }


# ── Global singleton ─────────────────────────────────────────────────────────

_global_metrics: Optional[ThreadingMetrics] = None
_global_lock = threading.Lock()


def get_metrics() -> ThreadingMetrics:
    """Return the process-global metrics instance, creating it if needed."""
    global _global_metrics

    if _global_metrics is None:
        with _global_lock:
            if _global_metrics is None:
                _global_metrics = ThreadingMetrics()

    assert _global_metrics is not None
    return _global_metrics


def reset_metrics() -> ThreadingMetrics:
    """Replace the process-global metrics instance — primarily for tests."""
    global _global_metrics

    with _global_lock:
        _global_metrics = ThreadingMetrics()

    return _global_metrics
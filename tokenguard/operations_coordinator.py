# -*- coding: utf-8 -*-
# operations_coordinator.py
"""
Runtime orchestration for token-managed execution.
"""

import asyncio
import json
import threading
import time
from collections import deque
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, Dict, List, Any

from .overflow_guard import OverflowGuard
from .tg_print import tg_print
from .guard_house import GuardHouse
from .token_options import option
from .admission_gate import AdmissionGate
from .threading_metrics import get_metrics
from .token_system import global_token_pool
from .topology_detector import TopologyDetector
from .core_affinity_queue import CoreAffinityQueue
from .core_pinned_staggered_queue import CorePinnedStaggeredQueue
from .convergence_engine import ConvergenceEngine


@dataclass
class ExecutionRecord:
    """Immutable summary of one completed token execution.

    Used for recent-execution inspection, UI display, and optional
    JSON history export.
    """
    token_id: str
    method_name: str
    success: bool
    execution_time: float
    timestamp: float
    core_id: int
    worker_id: str
    operation_type: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary representation of the execution record."""
        return asdict(self)


class WorkerPoolInterface:
    """Minimal worker-pool adapter exposed to the convergence engine.

    This wrapper provides pool size information and pattern-control hooks
    without exposing the full worker queue implementation.
    """

    def __init__(self, worker_queue: 'CorePinnedStaggeredQueue'):
        self.worker_queue = worker_queue
        self.num_cores = worker_queue.num_cores
        self.workers_per_core = worker_queue.workers_per_core
        self.num_workers = worker_queue.total_workers

    def set_core_pattern(self, core_id: int, pattern: int) -> None:
        """Update the active worker pattern for a core via the worker queue."""
        self.worker_queue.set_core_pattern(core_id, pattern)

    def get_pool_stats(self) -> dict[str, Any]:
        """Return worker-pool counts needed by convergence analysis."""
        return {
            'total_workers': self.num_workers,
            'num_cores': self.num_cores,
            'workers_per_core': self.workers_per_core
        }


class OperationsCoordinator:
    """Owns runtime startup, component wiring, and orderly shutdown."""

    def __init__(
            self,
            workers_per_core: Optional[int] = None,
            enable_convergence: Optional[bool] = False,
            auto_block_dangerous: Optional[bool] = None,
    ):
        tg_print('coordinator', '=' * 60)
        tg_print('coordinator', 'Initializing...')
        tg_print('coordinator', '=' * 60)

        self.enable_convergence = enable_convergence if enable_convergence is not False else option.ENABLE_CONVERGENCE
        self.auto_block_dangerous = auto_block_dangerous if auto_block_dangerous is not None else option.AUTO_BLOCK_DANGEROUS
        self.workers_per_core: int = workers_per_core if workers_per_core is not None else option.WORKERS_PER_CORE
        self.num_executors = option.NUM_EXECUTORS

        tg_print('coordinator', 'Detecting CPU topology...')
        detector = TopologyDetector()
        self.topology = detector.detect()
        detector.print_topology(self.topology)

        tg_print('coordinator', 'Creating foundation components...')

        # Overflow guard (memory protection)
        self.overflow_guard = OverflowGuard()
        tg_print('coordinator', 'Overflow guard initialized')

        # Guard House (method reputation tracking)
        self.guard_house = GuardHouse(auto_block_dangerous=self.auto_block_dangerous)
        tg_print('coordinator', 'Guard House initialized')

        # Core affinity policy (routing rules)
        self.affinity_queue = CoreAffinityQueue(self.topology, self.workers_per_core)
        tg_print('coordinator', 'Core affinity policy created')

        # Metrics
        self.metrics = get_metrics()

        self.recent_executions: deque[ExecutionRecord] = deque(maxlen=option.RECENT_EXECUTIONS_MAX)
        self._executions_lock = threading.RLock()
        tg_print('coordinator', 'Building execution pipeline...')

        # Worker queue - does routing AND execution
        self.worker_queue = CorePinnedStaggeredQueue(
            num_cores=self.topology.logical_cores,
            workers_per_core=self.workers_per_core,
            coordinator=self
        )

        # Worker pool interface - for convergence control
        self.worker_pool = WorkerPoolInterface(self.worker_queue)

        # Admission gate - pure pass-through
        self.gate = AdmissionGate(
            token_pool=global_token_pool,
            worker_queue=self.worker_queue,
            worker_pool=self.worker_pool,
        )

        tg_print('coordinator', 'Worker queue created')
        tg_print('coordinator', 'Admission gate configured')

        # Convergence engine (optional)
        self.convergence: Optional[ConvergenceEngine] = None
        if self.enable_convergence:
            tg_print('coordinator', 'Configuring convergence engine...')
            self.convergence = ConvergenceEngine(
                topology=self.topology,
                worker_queue=self.worker_queue,
                queue_wait_threshold=option.QUEUE_WAIT_THRESHOLD,
                utilization_high=option.UTILIZATION_HIGH,
                utilization_low=option.UTILIZATION_LOW,
                queue_depth_factor=option.QUEUE_DEPTH_FACTOR,
            )
            tg_print('coordinator', 'Prometheus convergence enabled')

        # State
        self._active = False
        self._convergence_task: Optional[asyncio.Task[Any]] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None

        tg_print('coordinator', 'Ready!')
        tg_print('coordinator',
                 f'Cores: {self.topology.physical_cores}')
        tg_print('coordinator',
                 f'Workers per core: {self.workers_per_core}')
        tg_print('coordinator',
                 f'Total workers: {self.topology.physical_cores * self.workers_per_core}')
        tg_print('coordinator',
                 f'Convergence: {"ENABLED" if self.enable_convergence else "disabled"}')
        tg_print('coordinator', '=' * 60)

    def print_guard_house_dashboard(self) -> None:
        """Print the current Guard House heatmap dashboard."""
        self.guard_house.print_heatmap()

    def get_guard_house_stats(self) -> dict[str, Any]:
        """Return Guard House summary statistics."""
        return self.guard_house.get_stats()

    def record_execution(self, record: ExecutionRecord) -> None:
        """Append a completed execution record to the recent-history buffer.

        This operation is protected by an internal re-entrant lock.
        """
        with self._executions_lock:
            self.recent_executions.append(record)

    def get_recent_executions(self, limit: int = 50) -> List[dict[str, Any]]:
        """Return up to ``limit`` recent execution records, newest first."""
        with self._executions_lock:
            # Convert to list, take last N, reverse for newest-first
            recent = list(self.recent_executions)[-limit:]
            return [rec.to_dict() for rec in reversed(recent)]

    def dump_execution_history(self, filepath: Optional[Path] = None) -> str:
        """Write the current recent-execution buffer to a JSON file.

        If no path is provided, a timestamped filename is generated in the
        current working directory.

        Returns:
            The path written, as a string.
        """
        if filepath is None:
            filepath = Path(f'execution_history_{int(time.time())}.json')
        with self._executions_lock:
            data = {
                'timestamp': time.time(),
                'total_executions': len(self.recent_executions),
                'executions': [rec.to_dict() for rec in self.recent_executions]
            }
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        tg_print('coordinator', f'Execution history dumped to: {filepath}')
        return str(filepath)

    def start(self) -> None:
        """Start the coordinator runtime and initialize the control plane.

        Startup performs the following steps:

        1. Mark the coordinator active.
        2. Publish shared safety components into the global token pool.
        3. Start the background event-loop thread.
        4. Initialize the worker queue and admission gate on that loop.
        5. Start convergence monitoring when enabled.

        This method returns after the event loop has been created and the
        startup sequence has been dispatched.
        """
        if self._active:
            tg_print('coordinator', 'Already running!', level='warn')
            return

        tg_print('coordinator', 'Starting...')
        self._active = True

        global_token_pool._guard_house = self.guard_house
        # Start event loop in background thread
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True,
            name="Coordinator-EventLoop"
        )
        self._loop_thread.start()

        # Wait for loop to be ready
        while self._event_loop is None:
            time.sleep(0.01)

        tg_print('coordinator', 'Event loop started')
        tg_print('coordinator', 'Worker queue started')
        tg_print('coordinator', 'Admission gate started')

        if self.convergence:
            tg_print('coordinator', 'Convergence monitoring started')

        tg_print('coordinator', 'Started successfully!')

    def stop(self) -> None:
        """Stop the coordinator and shut down runtime components in order.

        Shutdown proceeds in reverse dependency order:

        1. Stop convergence monitoring.
        2. Stop the admission gate.
        3. Stop the worker queue.
        4. Stop the background event loop.
        5. Join the loop thread before returning.

        This method is intended to provide a graceful shutdown path for the
        control plane and worker pipeline.
        """
        if not self._active:
            return
        if self._event_loop is None:
            return

        tg_print('coordinator', 'Stopping...')
        self._active = False

        # Stop convergence first
        if self._convergence_task:
            asyncio.run_coroutine_threadsafe(
                self._stop_convergence(), self._event_loop
            ).result(timeout=5.0)

        asyncio.run_coroutine_threadsafe(
            self._stop_execution(), self._event_loop
        ).result(timeout=5.0)

        # Stop event loop
        self._event_loop.call_soon_threadsafe(self._event_loop.stop)  # Type: ignore
        if self._loop_thread:
            self._loop_thread.join(timeout=5.0)

        tg_print('coordinator', 'All components stopped')
        tg_print('coordinator', 'Shutdown complete')

    def _run_event_loop(self) -> None:
        """Own and run the coordinator's background asyncio event loop.

        This method is executed on the dedicated loop thread. It creates the
        event loop, publishes the loop and async token queue into the global
        token pool, starts the worker queue and admission gate, optionally
        starts convergence monitoring, and then runs the loop until shutdown.
        """
        self._event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._event_loop)

        # Tell the token pool about our event loop
        global_token_pool._event_loop = self._event_loop
        global_token_pool._token_queue = asyncio.Queue()

        # Start components
        self._event_loop.run_until_complete(self.worker_queue.start(self.num_executors))
        self._event_loop.run_until_complete(self.gate.start())

        # Start convergence if enabled
        if self.convergence:
            self._convergence_task = self._event_loop.create_task(self._convergence_loop())

        try:
            self._event_loop.run_forever()
        finally:
            self._event_loop.close()

    async def _convergence_loop(self) -> None:
        """Periodically analyze worker pressure and apply pattern adjustments."""
        while self._active:
            try:
                await asyncio.sleep(5.0)
                assert self.convergence is not None
                core_pressures = self.convergence.analyze_cores(self.worker_pool)
                adjustments = self.convergence.recommend_adjustments(core_pressures)

                if adjustments:
                    tg_print(
                        'convergence',
                        f'Applying {len(adjustments)} pattern adjustment(s)...',
                    )
                    for core_id, new_pattern in adjustments.items():
                        tg_print(
                            'convergence',
                            f'Core {core_id} -> pattern {new_pattern}',
                            level='dispatch',
                        )
                        self.convergence.apply_pattern(core_id, new_pattern, self.worker_pool)

            except Exception as e:
                tg_print('convergence', f'Error: {e}', level='error')
                await asyncio.sleep(5.0)

    async def _stop_convergence(self) -> None:
        """Cancel and await the background convergence task, if running."""
        if self._convergence_task:
            self._convergence_task.cancel()
            try:
                await self._convergence_task
            except asyncio.CancelledError:
                pass

    async def _stop_execution(self) -> None:
        """Stop the admission gate and worker queue."""
        await self.gate.stop()
        await self.worker_queue.stop()

    # Admin-facing API
    def get_stats(self) -> Dict[str, Any]:
        """Return a composite snapshot of coordinator and subsystem statistics."""
        return {
            'active': self._active,
            'topology': {
                'physical_cores': self.topology.physical_cores,
                'logical_cores': self.topology.logical_cores,
                'workers_per_core': self.workers_per_core,
                'total_workers': self.topology.physical_cores * self.workers_per_core
            },
            'token_pool': global_token_pool.get_stats(),
            'admission_gate': self.gate.get_stats(),
            'worker_queue': self.worker_queue.get_stats(),
            'affinity_distribution': self.affinity_queue.get_affinity_report(),
            'convergence': self.convergence.get_convergence_status() if self.convergence else None
        }

    def get_affinity_report(self) -> None:
        """Print the current core-affinity distribution report."""
        self.affinity_queue.print_affinity_report()

    @staticmethod
    def kill_token(token_id: str, reason: str = "admin_override") -> bool:
        """Kill a token by id via the global token pool."""
        return global_token_pool.kill_token(token_id, reason)

    @staticmethod
    def kill_all_by_operation(operation_type: str, reason: str = "admin_bulk_kill") -> int:
        """Kill all tokens matching an operation type."""
        return global_token_pool.kill_all_by_operation(operation_type, reason)

    @staticmethod
    def pause_admission() -> None:
        """Pause token admission while continuing to accept submissions."""
        global_token_pool.pause()

    @staticmethod
    def resume_admission() -> None:
        """Resume token admission from the global token pool."""
        global_token_pool.resume()

    @staticmethod
    def drain_pool() -> int:
        """Kill all tokens still waiting for admission and return the count."""
        return global_token_pool.drain()

    @staticmethod
    def drain_operation(operation_type: str, reason: str = "admin_per-token_drain") -> int:
        """Drain a specific token"""
        return global_token_pool.drain(operation_type, reason)

    @staticmethod
    def pause_operation(operation_type: str, reason: str = "admin_per-token_pause") -> None:
        """Pause a specific token"""
        return global_token_pool.pause(operation_type, reason)

    @staticmethod
    def resume_operation(operation_type: str, reason: str = "admin_per-token_resume") -> None:
        """Resume a specific token"""
        return global_token_pool.resume(operation_type, reason)


# Global decorator singleton
_global_coordinator: Optional[OperationsCoordinator] = None
_coordinator_lock = threading.Lock()


def get_global_coordinator() -> OperationsCoordinator:
    """Return the process-global coordinator, creating and starting it if needed.

    This function exists primarily to support decorator-driven submission paths
    that need a running coordinator without explicit manual wiring.
    """
    global _global_coordinator

    if _global_coordinator is None:
        with _coordinator_lock:
            if _global_coordinator is None:
                _global_coordinator = OperationsCoordinator()
                _global_coordinator.start()

    assert _global_coordinator is not None  # narrows Optional → concrete type
    return _global_coordinator


def set_global_coordinator(coordinator: OperationsCoordinator) -> None:
    """Replace the process-global coordinator instance.

    Intended for tests or for applications that construct the coordinator
    manually with custom configuration.
    """
    global _global_coordinator
    with _coordinator_lock:
        _global_coordinator = coordinator

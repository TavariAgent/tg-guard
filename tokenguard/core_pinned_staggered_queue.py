# -*- coding: utf-8 -*-
# core_pinned_staggered_queue.py
"""
Core-pinned mailbox routing with staggered position assignment.

This module maps token weight classes onto valid core ranges and assigns
monotonic staggered positions that resolve to specific per-worker mailboxes.

Routing happens before mailbox placement:
    HEAVY  -> Core 1 range
    MEDIUM -> Core 2+ range
    LIGHT  -> Core 3+ range

This keeps mailbox placement aligned with the configured affinity policy.
"""
import time
import asyncio
import pickle
from functools import partial
from typing import Dict, List, Tuple, Any, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from .token_options import option
from .threading_metrics import get_metrics
from .token_system import TaskToken, TokenState
from .admission_gate import WorkerTaskQueue
from .core_affinity_queue import TaskWeight
from .sticky_token import sticky_registry
from .hash_conductor import conductor
from .unhashable_checker import HashPolicy, fast_make_hashable
from .tg_print import tg_print


class CorePinnedStaggeredQueue(WorkerTaskQueue):
    """Mailbox execution queue with core-aware staggered routing.

    Tokens are classified by routing weight, restricted to valid core ranges,
    assigned a staggered global position, and then placed into a specific
    per-worker mailbox for execution.

    This layer performs actual mailbox placement. It is the execution-facing
    counterpart to the affinity policy layer.
    """

    def __init__(
            self,
            num_cores: int,
            coordinator: Any,
            workers_per_core: Optional[int] = None,
    ):
        """
        Initialize the pinned staggered mailbox queue.

        Args:
            num_cores: Number of physical cores exposed to the routing model.
            workers_per_core: Number of mailbox workers created per core.
            coordinator: OperationsCoordinator reference used for history,
                retry, overflow, and Guard House callbacks.
        """
        super().__init__()
        self.coordinator = coordinator
        self.num_cores = num_cores
        self.workers_per_core: int = workers_per_core if workers_per_core is not None else option.WORKERS_PER_CORE
        self.total_workers = num_cores * self.workers_per_core

        # Metrics
        self.metrics = get_metrics()

        # Mailboxes: one asyncio.Queue per worker (core_id, local_i)
        self.mailboxes: Dict[Tuple[int, int], asyncio.Queue[TaskToken[Any]]] = {}

        # Least-loaded routing helpers
        self.worker_queue_sizes: Dict[int, int] = {i: 0 for i in range(self.total_workers)}

        # Routing helpers
        self.core_queue_depth: Dict[int, int] = {c: 0 for c in range(1, self.num_cores + 1)}
        self.core_busy: Dict[int, int] = {c: 0 for c in range(1, self.num_cores + 1)}

        # Capped mailbox length to prevent runaway memory (DOS safety)
        self.MAILBOX_MAX = option.MAILBOX_MAX

        self.core_patterns: Dict[int, int] = {}
        for core_id in range(1, num_cores + 1):
            self.core_patterns[core_id] = self.workers_per_core

        # Initialize stats
        self.total_executed = 0
        self.total_failed = 0

        # Core-to-worker mapping
        # Core 1: workers [0, 1, 2, 3]
        # Core 2: workers [4, 5, 6, 7] etc.
        self.core_workers: Dict[int, List[int]] = {}
        for core_id in range(1, num_cores + 1):
            start_worker = (core_id - 1) * self.workers_per_core
            self.core_workers[core_id] = list(range(start_worker, start_worker + self.workers_per_core))

        # Position tracking by core
        # Each core tracks its next available position
        self.core_position_counters: Dict[int, int] = {}
        for core_id in range(1, num_cores + 1):
            self.core_position_counters[core_id] = (core_id - 1) * self.workers_per_core

        # State
        self._active = False
        self._execution_tasks = []

        # Executor pools
        self._thread_executor = ThreadPoolExecutor(
            max_workers=self.total_workers,
            thread_name_prefix="tg_io"
        )
        self._process_executor = ProcessPoolExecutor(max_workers=self.num_cores)

        tg_print('worker', f'CorePinnedQueue initialized  '
                           f'cores={num_cores}  '
                           f'workers_per_core={workers_per_core}  '
                           f'total={self.total_workers}')
        for core_id, workers in self.core_workers.items():
            tg_print('worker', f'Core {core_id}: workers {workers}', level='debug')

    def _worker_index(self, core_id: int, local_i: int) -> int:
        """Return the flattened worker index for a core/local-worker pair."""
        return (core_id - 1) * self.workers_per_core + local_i

    def _choose_local_worker_least_loaded(self, core_id: int) -> int:
        """Return the active local worker with the smallest current mailbox depth."""
        active = max(1, min(self.workers_per_core, int(self.core_patterns.get(core_id, self.workers_per_core))))
        best: int = 0
        best_size: int = self.mailboxes[(core_id, 0)].qsize()
        for i in range(1, active):
            size: int = self.mailboxes[(core_id, i)].qsize()
            if size < best_size:
                best_size = size
                best = i
        return best

    def set_core_pattern(self, core_id: int, pattern_value: int) -> None:
        """Set the number of active mailbox workers for a core."""
        tg_print('worker', f'Core {core_id} pattern set to {pattern_value}', level='dispatch')
        self.core_patterns[core_id] = int(pattern_value)
        self.metrics.update_pattern(core_id, int(pattern_value))
        # Re-sync busy/idle metrics so the new pattern is reflected immediately
        self._sync_worker_state(core_id)

    # Alias for callers that use set_pattern
    def set_pattern(self, core_id: int, pattern_value: int) -> None:
        """Alias for set_core_pattern()."""
        self.set_core_pattern(core_id, pattern_value)

    async def _execute_token(self, token: TaskToken[Any], worker_id: str, core_id: int) -> None:
        """Execute one admitted token on its already-selected core path.

        This method performs the lifecycle transition to EXECUTING, runs the
        wrapped callable through the executor-backed path, stores the result or
        error on the token, records execution history for the coordinator, and
        triggers retry/Guard House hooks when configured.
        """
        # Transition to executing
        if not token.transition_state(TokenState.EXECUTING):
            tg_print('worker', f'{worker_id} failed to transition {token.token_id}', level='warn')
            return

        start_time = time.time()
        t0 = time.perf_counter()
        success = False

        try:
            loop = asyncio.get_running_loop()
            tags = token.metadata.tags

            # Warn on conflicting signals before resolving
            if tags.get("storage_speed") and tags.get("process_pool"):
                tg_print('worker',
                         f'{token.token_id} has both storage_speed and process_pool — '
                         f'defaulting to thread executor', level='warn')

            # Executor routing:
            # storage_speed tag  → thread  (IO confirmed)
            # process_pool: True → process (CPU, explicit opt-in)
            # neither            → thread  (safe default)
            if tags.get("process_pool") and not tags.get("storage_speed"):
                try:
                    pickle.dumps(token.args)
                    pickle.dumps(token.kwargs)
                    pickle.dumps(tags['_func_module'])
                    pickle.dumps(tags['_func_qualname'])
                    result = await loop.run_in_executor(
                        self._process_executor,
                        self._tg_process_bootstrap,
                        tags['_func_module'],
                        tags['_func_qualname'],
                        token.args,
                        token.kwargs,
                    )
                except (pickle.PicklingError, TypeError, AttributeError) as e:
                    tg_print('worker',
                             f'{token.token_id} process pool pickle failed — '
                             f'falling back to thread executor  error={e}',
                             level='warn')
                    result = await loop.run_in_executor(
                        self._thread_executor,
                        self._execute_token_wrapped,
                        token
                    )
            else:
                result = await loop.run_in_executor(
                    self._thread_executor,
                    self._execute_token_wrapped,
                    token
                )
            token.set_result(result)
            self.total_executed += 1
            success = True
            tg_print('worker', f'{worker_id} completed {token.token_id}', level='state')

        except Exception as e:
            # Failed!
            token.set_error(e)
            self.total_failed += 1
            tg_print('worker', f'{worker_id} failed {token.token_id}: {e}', level='error')

        finally:
            if self.coordinator and self.coordinator.convergence:
                self.coordinator.convergence.record_execution_sample(
                    core_id, time.perf_counter() - t0
                )
            execution_duration = time.time() - start_time

            if execution_duration > 30.0:
                tg_print('worker', f'{token.token_id} '
                                   f'slow execution: {execution_duration:.1f}s  '
                                   f'op={token.metadata.operation_type}', level='warn')

            if self.coordinator:
                from .operations_coordinator import ExecutionRecord
                # Record execution
                record = ExecutionRecord(
                    token_id=token.token_id,
                    operation_type=token.metadata.operation_type,
                    method_name=token.func.__name__,
                    success=success,
                    execution_time=execution_duration,
                    timestamp=time.time(),
                    core_id=core_id,
                    worker_id=worker_id,
                )
                self.coordinator.record_execution(record)

            guard = None
            should_retry = False
            failure_type = None
            if self.coordinator and hasattr(self.coordinator, 'overflow_guard'):
                guard = self.coordinator.overflow_guard

            if guard and self.coordinator:
                # Determine failure type
                if not success:
                    if execution_duration > 60.0:
                        failure_type = 'timeout'
                    elif execution_duration < 10.0:
                        failure_type = 'quick_fail'
                    else:
                        failure_type = 'error'

                    should_retry = guard.should_retry(
                        token.token_id,
                        execution_duration,
                        success=False,
                        operation_type=token.metadata.operation_type,
                        token_tags=token.metadata.tags,
                        skip=failure_type in ('timeout', 'quick_fail'),
                    )
                else:
                    # Record success (for retry tracking)
                    guard.record_success(token.token_id, execution_duration)

                if should_retry:
                    tg_print('worker',
                             f'{worker_id} triggering retry for {token.token_id}', level='dispatch')
                    retry_token = guard.create_retry_token(token, execution_duration)

                    if retry_token:
                        tg_print('worker',
                                 f'{worker_id} created retry: {retry_token.token_id}', level='dispatch')
                    else:
                        tg_print('worker',
                                 f'{worker_id} retries exhausted for {token.token_id}', level='warn')

                # Record result in Guard House
                if hasattr(self.coordinator, 'guard_house'):
                    self.coordinator.guard_house.record_execution_result(
                        method_name=token.func.__name__,
                        operation_type=token.metadata.operation_type,
                        success=success,
                        execution_time=execution_duration,
                        failure_type=failure_type,
                        complexity_score=token.metadata.tags.get('complexity_score')
                    )

            # Release the sticky-core pin
            sticky_key: str = (
                    token.metadata.tags.get("sticky_anchor")
                    or token.metadata.operation_type
                    or ""
            )
            sticky_registry.unmark(sticky_key, token.args)

    async def _execute_token_with_metrics(self, token: "TaskToken[Any]", worker_id: str, core_id: int) -> None:
        """Execute one token while updating worker-state and outcome metrics."""
        op_type = token.metadata.tags.get("operation_type", "unknown")

        self.core_busy[core_id] = self.core_busy.get(core_id, 0) + 1
        self._sync_worker_state(core_id)

        t0 = time.perf_counter()
        try:
            await self._execute_token(token, worker_id, core_id)
            self.metrics.record_task_completion(op_type, core_id, time.perf_counter() - t0)

        except Exception:
            self.metrics.record_task_failure(op_type, core_id)
            raise

        finally:
            self.core_busy[core_id] = max(0, self.core_busy.get(core_id, 0) - 1)
            self._sync_worker_state(core_id)

    def _sync_worker_state(self, core_id: int) -> None:
        """Push current busy/idle counts to metrics using the live pattern value."""
        active_workers = self.core_patterns.get(core_id, self.workers_per_core)
        busy = min(active_workers, self.core_busy.get(core_id, 0))
        idle = max(0, active_workers - busy)
        self.metrics.update_worker_state(core_id, busy, idle)

    def get_core_for_weight(self, weight: TaskWeight) -> List[int]:
        """Return the eligible core range for a routing weight.

        The returned list follows the active affinity policy, with fallback to
        lower-indexed cores on small-core systems when necessary.
        """
        if weight == TaskWeight.HEAVY:
            # Heavy only core 1
            return [1]

        elif weight == TaskWeight.MEDIUM:
            # Cores 2+ (never Core 1)
            if self.num_cores >= 2:
                return list(range(2, self.num_cores + 1))
            else:
                # Fallback for single-core systems
                return [1]

        else:
            # Light core 3+ (never Cores 1-2)
            if self.num_cores >= 3:
                return list(range(3, self.num_cores + 1))
            elif self.num_cores >= 2:
                # Fallback: use Core 2+ if only 2 cores
                return list(range(2, self.num_cores + 1))
            else:
                # Fallback for single-core
                return [1]

    # Weight string → enum — O(1) lookup, replaces the if/elif chain
    _WEIGHT_MAP: Dict[str, TaskWeight] = {
        'heavy': TaskWeight.HEAVY,
        'light': TaskWeight.LIGHT,
    }

    @staticmethod
    def classify_token_weight(token: TaskToken[Any]) -> TaskWeight:
        """Infer routing weight from token tags or operation-type naming.

        Explicit weight tags take precedence over operation-type heuristics.
        """
        # Check tags first — O(1) map lookup, default to MEDIUM on miss
        if 'weight' in token.metadata.tags:
            return CorePinnedStaggeredQueue._WEIGHT_MAP.get(
                token.metadata.tags['weight'].lower(), TaskWeight.MEDIUM
            )

        # Check operation_type suffix
        op_type = (token.metadata.operation_type or "").lower()
        if op_type.endswith('_heavy') or 'heavy' in op_type:
            return TaskWeight.HEAVY
        elif op_type.endswith('_light') or 'light' in op_type:
            return TaskWeight.LIGHT

        # Default to medium
        return TaskWeight.MEDIUM

    def choose_worker_for_core(self, core_id: int) -> int:
        """Choose the least-loaded active worker slot for the given core."""
        active: int = self.core_patterns.get(core_id, self.workers_per_core)
        base: int = (core_id - 1) * self.workers_per_core
        best: int = 0
        best_size: int = self.worker_queue_sizes[base]
        for i in range(1, active):
            size: int = self.worker_queue_sizes[base + i]
            if size < best_size:
                best_size = size
                best = i
        return best

    def assign_position_for_token(self, token: TaskToken[Any]) -> int:
        """Assign a staggered global route position for a token.

        The assigned position respects token weight, valid-core range, current
        per-core pattern, and each core's next position counter.
        """

        # Classify weight
        weight = self.classify_token_weight(token)

        # Get valid cores for this weight
        valid_cores = self.get_core_for_weight(weight)

        # Position loading is front bound and assignment is ranged for valid tasks
        chosen_core: int = valid_cores[0]
        best_count: int = self.core_position_counters[valid_cores[0]]
        for c in valid_cores[1:]:
            count: int = self.core_position_counters[c]
            if count < best_count:
                best_count = count
                chosen_core = c

        # Get the current pattern for this core
        active_workers = self.core_patterns.get(chosen_core, self.workers_per_core)

        # Calculate position using ONLY active workers
        base_position = (chosen_core - 1) * self.workers_per_core
        position_in_cycle = self.core_position_counters[chosen_core] % active_workers

        # Calculate actual position
        position = base_position + position_in_cycle

        # Increment counter
        self.core_position_counters[chosen_core] += 1

        tg_print('worker', f'Routed {token.token_id}  weight={weight.value}  '
                           f'pos={position}  core={chosen_core}  pattern={active_workers}', level='dispatch')
        return position

    async def put(self, token: "TaskToken[Any]") -> None:
        """Route a token to a mailbox and apply bounded enqueue backpressure.

        The token is tagged with enqueue timing metadata, assigned a route
        position, resolved to a core/local-worker mailbox, and enqueued without
        dropping work. If the chosen mailbox is full, the queue retries with the
        least-loaded active worker and then awaits capacity if necessary.

        Sticky-token enforcement: if this (op_name, args) key is already
        inflight on a core, the token is forced to that same core regardless of
        weight-based routing.  This prevents a second worker domain from
        touching the same data concurrently, which would cause cache misses and
        cross-domain data races.  The pin is released when the token completes.

        """
        # Tag + enqueue timestamp
        op_type = (
                getattr(token, "operation_type", None)
                or getattr(token.metadata, "operation_type", None)
                or token.metadata.tags.get("operation_type", "unknown")
        )
        token.metadata.tags["operation_type"] = op_type
        token.metadata.tags["enqueued_at"] = time.perf_counter()

        self.metrics.record_task_submission(op_type)

        # Sticky-core resolution
        # Compute the weight-based candidate core first, then let the sticky
        # registry either confirm it (first arrival) or redirect to the already-
        # pinned core (subsequent arrivals with identical op+args).
        weight = self.classify_token_weight(token)
        position = self.assign_position_for_token(token)
        token.metadata.tags["route_position"] = position

        # Derive target from position
        worker_index = position % self.total_workers
        candidate_core = (worker_index // self.workers_per_core) + 1
        candidate_local = worker_index % self.workers_per_core

        # Use sticky_anchor tag as the key name if provided, fall back to op_type
        sticky_name = token.metadata.tags.get("sticky_anchor") or op_type
        core_id = self._put_routing_block(token, sticky_name, candidate_core)
        token.metadata.tags["sticky_core"] = core_id

        if core_id != candidate_core:
            # Redirected by sticky registry — pick best local on the pinned core.
            local_i = self._choose_local_worker_least_loaded(core_id)
        else:
            local_i = candidate_local
            # Pattern lock: only active locals are eligible
            active = max(1, min(self.workers_per_core, int(self.core_patterns.get(core_id, self.workers_per_core))))
            if local_i >= active:
                local_i = self._choose_local_worker_least_loaded(core_id)

        q = self.mailboxes[(core_id, local_i)]

        # Enqueue (fast path)
        try:
            q.put_nowait(token)
        except asyncio.QueueFull:
            # Soft fallback: try least-loaded active worker again (queues can fill unevenly)
            local_i = self._choose_local_worker_least_loaded(core_id)
            q = self.mailboxes[(core_id, local_i)]
            tg_print('worker', f'Mailbox full on core {core_id} '
                               f'falling back to least-loaded worker {local_i}', level='warn')
            # If still full, await a slot (true backpressure) instead of dropping
            await q.put(token)

        # Per-core depth gauge
        self.core_queue_depth[core_id] += 1
        self.metrics.update_queue_depth(core_id, self.core_queue_depth[core_id])

        # Record task weight for heuristic convergence gauge
        if self.coordinator and hasattr(self.coordinator, 'convergence') and self.coordinator.convergence:
            self.coordinator.convergence.record_task_weight(core_id, weight.value)

    async def start(self, num_executors: int = 4) -> None:
        """Create per-worker mailboxes and start all worker-loop tasks.

        I used the inherited start(...) method as a typed configuration
        handoff point. That let the coordinator pass startup configuration
        across module boundaries without needing a separate setter or tighter
        coupling to the concrete queue implementation.

        This method is idempotent while the queue is already active.
        """
        if self._active:
            return

        self._active = True
        self._execution_tasks = []

        # Create per-worker mailboxes (loop context safe)
        for core_id in range(1, self.num_cores + 1):
            for local_i in range(self.workers_per_core):
                key = (core_id, local_i)
                if key not in self.mailboxes:
                    self.mailboxes[key] = asyncio.Queue(maxsize=self.MAILBOX_MAX)

        tg_print('worker', f'Starting {self.total_workers} mailbox workers...')
        for worker_idx in range(self.total_workers):
            core_id = (worker_idx // self.workers_per_core) + 1
            local_i = worker_idx % self.workers_per_core
            worker_id = f"worker_{worker_idx}_core_{core_id}"

            task = asyncio.create_task(
                self._worker_loop(worker_idx, worker_id, core_id, local_i),
                name=worker_id,
            )
            self._execution_tasks.append(task)

        tg_print('worker', f'Started {self.total_workers} workers across {self.num_cores} cores')

    async def _worker_loop(
            self,
            worker_idx: int,
            worker_id: str,
            core_id: int,
            local_i: int
    ) -> None:  # Don't del "worker_idx"!
        """Continuously consume one mailbox and execute admitted tokens."""
        q = self.mailboxes[(core_id, local_i)]
        tg_print('worker', f'{worker_id} started  core={core_id}  local={local_i}', level='state')

        while self._active:
            try:
                token = await q.get()  # blocks efficiently until a token arrives

                # Update depth gauge (dequeue)
                self.core_queue_depth[core_id] = max(0, self.core_queue_depth[core_id] - 1)
                self.metrics.update_queue_depth(core_id, self.core_queue_depth[core_id])

                # Queue wait
                enq: float | None = token.metadata.tags.get("enqueued_at")
                if enq is not None:
                    wait = time.perf_counter() - enq
                    self.metrics.record_queue_wait(core_id, wait)
                    if self.coordinator and self.coordinator.convergence:
                        self.coordinator.convergence.record_wait_sample(core_id, wait)

                if token.is_killed():
                    continue

                await self._execute_token_with_metrics(token, worker_id, core_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                tg_print('worker', f'{worker_id} loop error: {e}', level='error')
                await asyncio.sleep(0.05)

        tg_print('worker', f'{worker_id} stopped', level='state')

    async def stop(self) -> None:
        """Cancel worker tasks, stop mailbox consumption, and await shutdown."""
        if not self._active:
            return

        tg_print('worker', 'Stopping all workers...')

        self._active = False

        # Cancel all worker tasks
        for task in self._execution_tasks:
            task.cancel()

        # Wait for them to finish
        await asyncio.gather(*self._execution_tasks, return_exceptions=True)

        self._execution_tasks = []

        tg_print('worker', 'All workers stopped')

    def get_stats(self) -> dict[str, Any]:
        """Return queue configuration, counters, and per-core position state."""
        return {
            'num_cores': self.num_cores,
            'workers_per_core': self.workers_per_core,
            'total_workers': self.total_workers,
            'total_executed': self.total_executed,
            'total_failed': self.total_failed,
            'core_position_counters': dict(self.core_position_counters)
        }

    @staticmethod
    def _put_routing_block(token: TaskToken[Any], op_type: str, candidate_core: int) -> int:
        """
        Drop-in replacement for the sticky_registry.mark() call in put().
        Shows the routing decision tree for the conductor integration.
        """
        # ── Resolve hash policy
        _policy_raw = token.metadata.tags.get("hash_policy", HashPolicy.STANDARD)
        if isinstance(_policy_raw, str):
            try:
                hash_policy = HashPolicy(_policy_raw)
            except ValueError:
                tg_print(
                    "conductor",
                    f"Unknown hash_policy '{_policy_raw}' on token="
                    f"{getattr(token, 'token_id', '?')} — defaulting to STANDARD",
                    level="warn",
                )
                hash_policy = HashPolicy.STANDARD
        else:
            hash_policy = _policy_raw

        # ── Routing decision tree
        external_calls = (
                getattr(token.metadata, "external_calls", None)
                or token.metadata.tags.get("external_calls")
        )

        if external_calls:
            # Lead token — generate a fresh seed domain and pin to this core.
            core_id = conductor.charge(token, candidate_core)

        elif token.metadata.tags.get("conductor_seed"):
            core_id = conductor.register_child(token, candidate_core)

        else:
            has_sticky = "sticky_anchor" in token.metadata.tags
            if external_calls or has_sticky:
                sticky_name = token.metadata.tags.get("sticky_anchor") or op_type

                # Gate route_args on hash policy
                if hash_policy == HashPolicy.NONE:
                    route_args: tuple[Any, ...] = ()
                elif hash_policy == HashPolicy.FAST:
                    route_args = tuple(fast_make_hashable(a) for a in token.args)
                else:  # STANDARD or FULL — current behaviour, unchanged
                    route_args = token.args if external_calls else ()

                core_id = sticky_registry.mark(sticky_name, route_args, candidate_core)
            else:
                tg_print(
                    "conductor",
                    f"No routing  token={getattr(token, 'token_id', '?')}  "
                    f"op={op_type}  no seed, no sticky — free routing",
                    level="warn",
                )
                core_id = candidate_core

        return core_id

    @staticmethod
    def _execute_token_wrapped(token: TaskToken[Any]) -> Any:
        """Shows the wrapped callable pattern for _execute_token."""
        bound_func = partial(token.func, *token.args, **token.kwargs)

        def _conducted() -> Any:
            conductor.activate(token)  # sets thread-local seed in executor thread
            try:
                return bound_func()
            finally:
                conductor.deactivate()  # always clears, even on exception

        return _conducted()
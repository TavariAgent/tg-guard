# -*- coding: utf-8 -*-
# admission_gate.py
"""Admission and execution-queue base contracts.

This module defines the admission gate that transfers eligible tokens from the
token pool into the execution queue, along with the minimal abstract interface
implemented by concrete worker-queue backends.
"""

import asyncio
import importlib
from typing import Optional

from .token_system import TaskToken, TokenPool, TokenState
from .tg_print import tg_print


class AdmissionGate:
    """Pass-through admission layer between the token pool and worker queue."""

    def __init__(
            self,
            token_pool: TokenPool,
            worker_queue: 'WorkerTaskQueue',
            worker_pool,
            policy: Optional = None
    ):
        self.token_pool = token_pool
        self.worker_queue = worker_queue
        self.worker_pool = worker_pool
        self.policy = policy
        self._active = False
        self._loop_task: Optional[asyncio.Task] = None

        # Metrics
        self.total_admitted = 0

    async def start(self):
        """Start the background admission loop."""
        if self._active:
            return
        self._active = True
        self._loop_task = asyncio.create_task(self._admission_loop())
        tg_print('gate', 'Admission gate started — full saturation mode')

    async def stop(self):
        """Stop the admission loop and await task cancellation."""
        self._active = False
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        tg_print('gate', 'Admission gate stopped')

    async def _admission_loop(self):
        """Continuously move eligible tokens from the pool into the worker queue."""
        tg_print('gate', 'Admission loop started — unrestricted flow')

        while self._active:
            try:
                # Get next token from pool
                token = await self.token_pool.get_next_token()

                # Skip killed tokens
                if token.is_killed():
                    tg_print('gate', f'Skipping killed token {token.token_id}', level='debug')
                    continue

                # Route directly to execution
                await self._admit_token(token)

            except asyncio.CancelledError:
                break
            except Exception as e:
                tg_print('gate', f'Error in admission loop: {e}', level='error')
                await asyncio.sleep(0.1)  # Breif pause only on errors!

    async def _admit_token(self, token: TaskToken):
        """Transition a token into the admitted state and enqueue it for execution."""
        if token.state == TokenState.CREATED:
            token.transition_state(TokenState.WAITING)

        if not token.transition_state(TokenState.ADMITTED):
            tg_print(
                'gate',
                f'Failed to admit token {token.token_id} (state: {token.state})',
                level='warn',
            )
            return

        # Route to worker queue
        await self.worker_queue.put(token)

        # Update metrics
        self.total_admitted += 1
        self.token_pool.total_admitted += 1

    def get_stats(self) -> dict:
        """Return admission-gate state and throughput counters."""
        return {
            'active': self._active,
            'total_admitted': self.total_admitted,
            'mode': 'full_saturation'
        }


# ============================================================================
# Worker Task Queue - Minimal Base Class
# ============================================================================

class WorkerTaskQueue:
    """Abstract base class for execution-queue backends.

    Concrete subclasses are responsible for mailbox creation, token placement,
    worker startup, execution, and queue-specific metrics.
    """

    def __init__(self):
        """Initialize shared worker-queue state and counters."""
        super().__init__()
        self._active = False
        self._execution_tasks = []

        # Metrics
        self.total_executed = 0
        self.total_failed = 0

    async def start(self, num_executors):
        """Subclass-specific startup logic to initialize worker mailboxes and execution tasks."""
        raise NotImplementedError("Subclass must implement start()")

    async def stop(self):
        """Stop all execution tasks and await their cancellation."""
        self._active = False

        # Cancel all executor tasks
        for task in self._execution_tasks:
            task.cancel()

        # Wait for them to finish
        await asyncio.gather(*self._execution_tasks, return_exceptions=True)
        self._execution_tasks = []
        tg_print('worker', 'Stopped all executors')

    async def put(self, token: TaskToken):
        """Enqueue one admitted token for backend-specific execution routing."""
        raise NotImplementedError("Subclass must implement put()")

    def get_stats(self) -> dict:
        """Return base execution counters and active-task count."""
        return {
            'num_executors': len(self._execution_tasks),
            'total_executed': self.total_executed,
            'total_failed': self.total_failed
        }

    @staticmethod
    def _tg_process_bootstrap(module_name: str, qualname: str, args: tuple, kwargs: dict):
        """Module-level bootstrap for ProcessPoolExecutor dispatch.

        Accepts plain string identifiers instead of a function reference,
        avoiding the @wraps identity mismatch that causes PicklingError.

        The subprocess reimports the module, retrieves the wrapper by qualname,
        then accesses __wrapped__ to reach the original unwrapped callable.
        """
        mod = importlib.import_module(module_name)
        obj = mod
        for part in qualname.split('.'):
            obj = getattr(obj, part)
        # obj is the wrapper — __wrapped__ is the original set by @wraps
        original = getattr(obj, '__wrapped__', obj)
        return original(*args, **kwargs)

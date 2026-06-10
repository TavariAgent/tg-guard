# -*- coding: utf-8 -*-
# token_system.py
"""
Core token primitives for token-managed execution.

This module defines token lifecycle state, token metadata, the task token
container, the token pool, and the user-facing decorator used to convert
ordinary callables into token-managed submissions.

Execution is not performed here. This module is responsible for request
capture, token creation, lifecycle bookkeeping, and admission handoff to
the coordinator/event-loop layer.
"""
from __future__ import annotations

import types
import asyncio
import itertools
import operator
import threading
import time
from collections.abc import Iterator
from concurrent.futures import Future
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Callable, Any, Optional, Dict, ParamSpec, Generic, TypeVar

from .hash_conductor import get_active_seed, conductor
from .tg_print import tg_print

_token_id_counter = itertools.count()


class TokenState(Enum):
    """Lifecycle states for a token-managed task.

    Tokens move from creation to admission, execution, and a terminal state.
    Terminal states are COMPLETED, FAILED, KILLED, and TIMEOUT.
    """
    CREATED = "created"  # Just created, in pool
    WAITING = "waiting"  # Waiting for admission
    ADMITTED = "admitted"  # Passed gate, in worker queue
    EXECUTING = "executing"  # Currently running on worker
    COMPLETED = "completed"  # Finished successfully
    FAILED = "failed"  # Execution failed
    KILLED = "killed"  # Admin killed this token
    TIMEOUT = "timeout"  # Exceeded time limit


@dataclass
class TokenMetadata:
    """Per-token metadata used for routing, timing, and observability.

    Stores the declared operation type, creation time, optional routing tags,
    and lifecycle timestamps populated as the token moves through admission
    and execution.
    """
    created_at: float
    max_execution_time: float = 30.0  # 30 seconds default, can be overridden
    tags: Dict[str, Any] = field(default_factory=dict)

    # Tracking
    operation_type: Optional[str] = None
    admitted_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    def age(self) -> float:
        """Return the token age in seconds since creation."""
        return time.time() - self.created_at

    def wait_time(self) -> Optional[float]:
        return (self.admitted_at - self.created_at) if self.admitted_at else None

    def execution_time(self) -> Optional[float]:
        if self.started_at:
            return (self.completed_at or time.time()) - self.started_at
        return None


@dataclass(frozen=True)
class FuncIdentity:
    """Immutable function identity stamp captured at decoration time.

    Stored in token tags so the process pool bootstrap can reimport
    the correct callable without touching token.func directly.
    """
    module: str
    qualname: str


def get_func_identity(func: Callable) -> FuncIdentity:
    """Extract module and qualname with type-safe narrowing.

    isinstance against types.FunctionType is the narrowing anchor —
    the type checker recognises FunctionType as carrying __module__
    and __qualname__, resolving the Callable attribute warning.
    """
    if isinstance(func, types.FunctionType):
        return FuncIdentity(
            module=func.__module__,
            qualname=func.__qualname__
        )
    # Fallback for non-function callables (classes, partials, etc.)
    return FuncIdentity(
        module=getattr(func, '__module__', '') or '',
        qualname=getattr(func, '__qualname__', '') or getattr(func, '__name__', '')
    )


T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")


class TaskToken(Generic[T]):
    """Represents a deferred task submission managed by the token system."""

    def __init__(
            self,
            token_id: str,
            func: Callable,
            args: tuple[Any, ...],
            kwargs: dict,
            metadata: TokenMetadata
    ):
        self.token_id = token_id
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.metadata = metadata

        # State management
        self.on_state_change: Optional[Callable[[TaskToken[Any], TokenState, TokenState], None]] = None
        self.state: TokenState = TokenState.CREATED
        self._state_lock = threading.Lock()

        # Result delivery
        self._result_future: Future[T] = Future()
        self._result: Optional[T] = None
        self._error: Optional[Exception] = None

        # Admin control
        self._kill_requested = threading.Event()
        self._killed_reason: Optional[str] = None

    # ── __await__
    #
    # This bridges TaskToken's internal concurrent.futures.Future to whatever asyncio
    # event loop is currently running, making the token a native awaitable.
    #
    # Before (call-site):
    #   result = token.get(timeout=30.0)         # blocks the calling thread
    #
    # After (call-site):
    #   result = await token                     # suspends the coroutine, non-blocking
    #   results = await asyncio.gather(*tokens)  # gather a whole batch at once

    def __await__(self):
        """Make TaskToken directly awaitable from any asyncio context.

        Wraps the internal concurrent.futures.Future with asyncio.wrap_future(),
        which ties it to the currently running event loop. The coroutine suspends
        (non-blocking) until the token resolves, fails, or is killed.

        Usage::

            result = await token
            results = await asyncio.gather(*tokens)

        Note: the decorated function is still synchronous and runs on a worker
        thread. __await__ only affects how the *caller* waits for the result.
        """
        return asyncio.wrap_future(self._result_future).__await__()

    # ── Result Proxies
    #
    # All dunders below delegate to the resolved result via _resolve().
    # _resolve() blocks if the token is not yet COMPLETED, same behaviour as .get().
    #
    # Deliberately excluded:
    #   __repr__  — keeps token identity visible in debuggers/logs, not the result
    #   __eq__    — delegating this breaks dict lookups in TokenPool.tokens{}
    #   __hash__  — Python nulls this automatically if __eq__ is overridden; left
    #               paired with __eq__ to avoid silent breakage


    def _resolve(self) -> T:
        # Fast path — already resolved, no blocking needed
        if self._result_future.done():
            return self._result_future.result()

        # Guard — never block the event loop thread
        try:
            asyncio.get_running_loop()
            is_async = True
        except RuntimeError:
            is_async = False

        if is_async:
            raise RuntimeError(
                f"Token '{self.token_id}' not yet resolved — "
                f"cannot block the event loop. Use 'await token' instead."
            )

        # Sync context — safe to block, sticky anchor holds here
        # until the coordinator delivers the result
        return self._result_future.result()

    # ── Type Conversion

    def __bool__(self):
        return bool(self._resolve())

    def __int__(self):
        return int(self._resolve())

    def __float__(self):
        return float(self._resolve())

    def __complex__(self):
        return complex(self._resolve())

    def __index__(self):
        return operator.index(self._resolve())

    def __bytes__(self):
        return bytes(self._resolve())

    def __str__(self):
        return str(self._resolve())

    def __format__(self, spec):
        return format(self._resolve(), spec)

    # ── Collection

    def __len__(self):
        return len(self._resolve())

    def __length_hint__(self):
        return operator.length_hint(self._resolve())

    # ── __iter__
    # What this does:
    #
    # This is to resolve tokens undergoeing iterations and will assist in type
    # checking for syncrounous collections, it goes alongside the __await__.

    def __iter__(self) -> Iterator[Any]:
        result = self._result_future.result()
        if not hasattr(result, '__iter__'):
            raise TypeError(
                f"TaskToken[{type(result).__name__}] is not iterable"
            )
        return iter(result)

    def __reversed__(self) -> Iterator[Any]:
        result = self._result_future.result()
        reversible = hasattr(result, '__reversed__') or (
                hasattr(result, '__len__') and hasattr(result, '__getitem__')
        )
        if not reversible:
            raise TypeError(
                f"TaskToken[{type(result).__name__}] is not reversible"
            )
        return reversed(result)

    def __contains__(self, item):
        return item in self._resolve()

    def __getitem__(self, key):
        return self._resolve()[key]

    def __setitem__(self, key, val):
        self._resolve()[key] = val

    def __delitem__(self, key):
        del self._resolve()[key]

    # ── Unary Arithmetic

    def __neg__(self):
        return -self._resolve()

    def __pos__(self):
        return +self._resolve()

    def __abs__(self):
        return abs(self._resolve())

    def __invert__(self):
        return ~self._resolve()

    # ── Binary Arithmetic
    # Reflected variants (r-prefix) handle cases where the left operand is not
    # a TaskToken — e.g. 4.0 * token — Python falls back to token.__rmul__(4.0).

    def __add__(self, other):
        return self._resolve() + other

    def __radd__(self, other):
        return other + self._resolve()

    def __sub__(self, other):
        return self._resolve() - other

    def __rsub__(self, other):
        return other - self._resolve()

    def __mul__(self, other):
        return self._resolve() * other

    def __rmul__(self, other):
        return other * self._resolve()

    def __truediv__(self, other):
        return self._resolve() / other

    def __rtruediv__(self, other):
        return other / self._resolve()

    def __floordiv__(self, other):
        return self._resolve() // other

    def __rfloordiv__(self, other):
        return other // self._resolve()

    def __mod__(self, other):
        return self._resolve() % other

    def __rmod__(self, other):
        return other % self._resolve()

    def __pow__(self, other):
        return self._resolve() ** other

    def __rpow__(self, other):
        return other ** self._resolve()

    def __matmul__(self, other):
        return self._resolve() @ other

    def __rmatmul__(self, other):
        return other @ self._resolve()

    # ── Bitwise

    def __and__(self, other):
        return self._resolve() & other

    def __rand__(self, other):
        return other & self._resolve()

    def __or__(self, other):
        return self._resolve() | other

    def __ror__(self, other):
        return other | self._resolve()

    def __xor__(self, other):
        return self._resolve() ^ other

    def __rxor__(self, other):
        return other ^ self._resolve()

    def __lshift__(self, other):
        return self._resolve() << other

    def __rlshift__(self, other):
        return other << self._resolve()

    def __rshift__(self, other):
        return self._resolve() >> other

    def __rrshift__(self, other):
        return other >> self._resolve()

    # ── Comparison

    def __lt__(self, other):
        return self._resolve() < other

    def __le__(self, other):
        return self._resolve() <= other

    def __gt__(self, other):
        return self._resolve() > other

    def __ge__(self, other):
        return self._resolve() >= other

    # ── Context Manager

    def __enter__(self):
        return self._resolve().__enter__()

    def __exit__(self, *args):
        return self._resolve().__exit__(*args)

    # ── Callable

    def __call__(self, *args, **kwargs):
        return self._resolve()(*args, **kwargs)

    def transition_state(self, new_state: TokenState) -> bool:
        """Attempt a validated lifecycle transition with tg_print visibility."""
        cb = None
        old_state = None
        with self._state_lock:
            valid_transitions = {
                TokenState.CREATED: {TokenState.WAITING, TokenState.KILLED},
                TokenState.WAITING: {TokenState.ADMITTED, TokenState.KILLED, TokenState.TIMEOUT},
                TokenState.ADMITTED: {TokenState.EXECUTING, TokenState.KILLED, TokenState.TIMEOUT},
                TokenState.EXECUTING: {TokenState.COMPLETED, TokenState.FAILED, TokenState.KILLED, TokenState.TIMEOUT},
                TokenState.COMPLETED: set(),
                TokenState.FAILED: set(),
                TokenState.KILLED: set(),
                TokenState.TIMEOUT: set(),
            }

            if new_state not in valid_transitions.get(self.state, set()):
                return False

            old_state = self.state
            self.state = new_state
            cb = getattr(self, "on_state_change", None)

            # timestamps
            now = time.time()
            if new_state == TokenState.ADMITTED:
                self.metadata.admitted_at = now
            elif new_state == TokenState.EXECUTING:
                self.metadata.started_at = now
            elif new_state in {TokenState.COMPLETED, TokenState.FAILED, TokenState.KILLED, TokenState.TIMEOUT}:
                self.metadata.completed_at = now
                conductor.on_complete(self)
                if self.metadata.tags.get("conductor_seed"):
                    tg_print("conductor", f"Decremented  token={self.token_id}  state={new_state.value}",
                             level="dispatch")

        # Emit state transition visibility
        tg_print(
            'token',
            f'{self.token_id}  {old_state.value} -> {new_state.value}'
            f'op={self.metadata.operation_type}',
            level='state',
        )

        if cb:
            cb(self, old_state, new_state)

        return True

    def kill(self, reason: str = "admin_override"):
        """Kills the active token."""
        self._kill_requested.set()
        self._killed_reason = reason

        if self.transition_state(TokenState.KILLED):
            # Set exception in future
            self._result_future.set_exception(
                TaskKilledException(f"Token killed: {reason}")
            )
            tg_print('token', f'{self.token_id} killed — {reason}', level='warn')
            return True
        return False

    def is_killed(self) -> bool:
        """Check if this token has been killed."""
        return self._kill_requested.is_set()

    def set_result(self, result: Any):
        """Store a successful result and transition the token to COMPLETED."""
        self._result = result
        self.transition_state(TokenState.COMPLETED)
        self._result_future.set_result(result)

    def set_error(self, error: Exception):
        """Store an execution error and transition state."""
        self._error = error
        self.transition_state(TokenState.FAILED)
        self._result_future.set_exception(error)
        tg_print('token', f'{self.token_id} failed — {error}', level='error')

    def get(self, timeout: Optional[float] = None) -> T:
        """Block until the token resolves or the timeout expires."""
        return self._result_future.result(timeout=timeout)

    def get_status(self) -> dict:
        """Return a snapshot of token state and timing information."""
        return {
            'token_id': self.token_id,
            'state': self.state.value,
            'operation_type': self.metadata.operation_type,
            'created_at': self.metadata.created_at,
            'age': self.metadata.age(),
            'wait_time': self.metadata.wait_time(),
            'execution_time': self.metadata.execution_time(),
            'tags': self.metadata.tags,
            'killed': self.is_killed(),
            'killed_reason': self._killed_reason,
            'has_result': self._result_future.done()
        }


class TaskKilledException(Exception):
    pass


class TokenPool:
    """Thread-safe registry and admission queue for task tokens.

    The pool owns created tokens, exposes inspection and administrative
    controls, and provides the async handoff point used by the admission loop.
    Token creation is synchronous and immediate; execution is deferred until
    a coordinator retrieves and admits the token.
    """

    def __init__(self):
        self.tokens: Dict[str, TaskToken[Any]] = {}
        self._lock = threading.Lock()

        self._token_queue: Optional[asyncio.Queue[TaskToken[Any]]] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self.default_on_state_change = None

        # Metrics
        self.total_created = 0
        self.total_killed = 0
        self.total_admitted = 0

        # Admin controls
        self._paused = threading.Event()
        self._paused.set()  # Start unpaused

        # --- NEW PER-OPERATION STATE ---
        self._paused_operations: set[str] = set()
        self._paused_holding: Dict[str, list[TaskToken]] = {}

    def register_retry_token(self, token: TaskToken):
        """Register a retry token directly, bypassing the admission gate."""
        with self._lock:
            self.tokens[token.token_id] = token

        if self._event_loop:
            asyncio.run_coroutine_threadsafe(
                self._token_queue.put(token), self._event_loop
            )

        token.transition_state(TokenState.WAITING)

    def create_token(
            self,
            func: Callable[[P], R],
            args: tuple[Any, ...],
            kwargs: dict,
            operation_type: Optional[str] = None,
            tags: Dict[str, Any] | None = None
    ) -> "TaskToken[R]":
        self.total_created += 1
        token_id = f"{operation_type}_{time.time_ns()}_{next(_token_id_counter)}"

        metadata = TokenMetadata(
            operation_type=operation_type,
            created_at=time.time(),
            tags=tags or {}
        )

        token = TaskToken(token_id, func, args, kwargs, metadata)

        with self._lock:
            self.tokens[token_id] = token

        tg_print('pool', f'Token created  {token_id}  op={operation_type}', level='debug')

        token.transition_state(TokenState.WAITING)

        if self._event_loop:
            asyncio.run_coroutine_threadsafe(
                self._token_queue.put(token),
                self._event_loop
            )
        else:
            tg_print('pool', 'No event loop — token queued but not dispatched', level='warn')

        return token

    async def get_next_token(self):
        """Wait for and return the next token eligible for admission.

        If the pool is globally paused, this waits. If a specific token's
        operation_type is paused, it routes it to a holding area and grabs the next.
        """
        assert self._token_queue is not None, "TokenPool not started — call coordinator.start() first"

        while True:
            # Wait if globally paused
            while not self._paused.is_set():
                await asyncio.sleep(0.1)

            # FIFO
            token = await self._token_queue.get()

            # 0.5. Check gaurd for broken objects.
            # Guard: confirms the dequeued object is a genuine TokenGate token.
            # Handles None, accidental foreign enqueue, and satisfies the type checker.
            # isinstance narrows the type fully from this point down.
            if not isinstance(token, TaskToken):
                continue

            # 1. Skip if it was drained/killed while waiting in the queue
            if token.is_killed() or token.state == TokenState.KILLED:
                continue

            op_type = token.metadata.operation_type  # still Optional[str] here

            # 2. Check if this specific operation is paused
            with self._lock:
                is_op_paused = op_type is not None and op_type in self._paused_operations

            if is_op_paused:
                assert op_type is not None  # narrowed: str from here down
                # Route to holding area and loop to get the next token
                with self._lock:
                    if op_type not in self._paused_holding:
                        self._paused_holding[op_type] = []
                    self._paused_holding[op_type].append(token)
                continue

            return token

    def get_all_tokens(self) -> Dict[str, TaskToken]:
        """Return a shallow snapshot of all registered tokens."""
        with self._lock:
            return dict(self.tokens)

    def get_tokens_by_state(self, state: TokenState) -> list[TaskToken]:
        """Return all tokens currently in the requested lifecycle state."""
        with self._lock:
            return [t for t in self.tokens.values() if t.state == state]

    def get_tokens_by_operation(self, operation_type: str) -> list[TaskToken]:
        """Return all tokens matching the given operation type."""
        with self._lock:
            return [
                t for t in self.tokens.values()
                if t.metadata.operation_type == operation_type
            ]

    def kill_token(self, token_id: str, reason: str = "admin_override") -> bool:
        """Kill a registered token by id."""
        with self._lock:
            if token_id in self.tokens:
                if self.tokens[token_id].kill(reason):
                    self.total_killed += 1
                    return True
        return False

    def kill_all_by_operation(self, operation_type: str, reason: str = "admin_bulk_kill"):
        """Kill all tokens with the given operation type."""
        tokens = self.get_tokens_by_operation(operation_type)
        killed = 0
        for token in tokens:
            if token.kill(reason):
                killed += 1
        self.total_killed += killed
        tg_print('pool', f'Killed {killed} tokens of type {operation_type}')
        return killed

    def pause(self, operation_type: str, reason: str = "admin_pause"):
        """Pause token or pool."""
        if operation_type:
            with self._lock:
                self._paused_operations.add(operation_type)
            tg_print('pool', f'PAUSED operation: {operation_type}  ({reason})', level='warn')
        else:
            self._paused.clear()
            tg_print('pool', f'PAUSED globally ({reason}) — tokens will accumulate', level='warn')

    def resume(self, operation_type: str, reason: str = "admin_resume"):
        """Resume token or pool."""
        if operation_type:
            tokens_to_requeue = []
            with self._lock:
                self._paused_operations.discard(operation_type)

                # Retrieve held tokens
                if operation_type in self._paused_holding:
                    tokens_to_requeue = self._paused_holding.pop(operation_type)

            # Re-insert held tokens back into the async admission queue
            if self._event_loop and tokens_to_requeue:
                for token in tokens_to_requeue:
                    asyncio.run_coroutine_threadsafe(
                        self._token_queue.put(token), self._event_loop
                    )
            tg_print(
                'pool',
                f'RESUMED operation: {operation_type}  ({reason})'
                f' — requeued {len(tokens_to_requeue)} held tokens',
            )
        else:
            self._paused.set()
            tg_print('pool', f'RESUMED globally ({reason}) — tokens will admit')

    def drain(self, operation_type: str, reason: str = "admin_drain") -> int:
        """Drain the token or pool."""
        waiting = self.get_tokens_by_state(TokenState.WAITING)
        killed = 0

        for token in waiting:
            if operation_type is None or token.metadata.operation_type == operation_type:
                if token.kill(reason):
                    killed += 1
        self.total_killed += killed
        tg_print('pool', f'DRAINED — killed {killed} waiting tokens  op={operation_type}', level='warn')
        return killed

    def get_stats(self) -> dict:
        """Get current metrics about the token pool."""
        tokens_by_state = {s.value: len(self.get_tokens_by_state(s)) for s in TokenState}
        return {
            'total_created': self.total_created,
            'total_killed': self.total_killed,
            'total_admitted': self.total_admitted,
            'current_tokens': len(self.tokens),
            'tokens_by_state': tokens_by_state,
            'paused': not self._paused.is_set(),
        }

    @staticmethod
    def _get_loop():
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop


# DECORATOR - The user-facing API
def task_token_guard(
        operation_type: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorate a callable so calls return TaskToken instead of executing immediately.

    The wrapper performs optional code analysis, optional quarantine checks,
    optional storage-speed throttling, and token creation through the global
    token pool.

    Args:
        operation_type: Stable operation label used for metadata and routing.
        tags: Optional routing and policy tags.
              'weight':         'heavy' | 'medium' | 'light'
              'storage_speed':  'FAST' | 'SLOW' | 'MODERATE' | 'INSANE'
              'process_pool':   True | False - ProcessPoolExecutor opt-in
              'sticky_anchor':  create the sticky routing identifier
              'external_calls': list of downstream calls this token dispatches.

    Returns:
        A decorator that replaces direct execution with token submission.

    Notes:
        The wrapped callable is not executed at call time. It is captured as a
        token-managed task for later admission and execution.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, "TaskToken[R]"]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> "TaskToken[R]":
            final_tags = dict(tags) if tags else {}  # Immutable
            final_func = func

            if 'storage_speed' in final_tags:
                # Storage throttling requested!
                speed_tier = final_tags['storage_speed']
                from .storage_throttle import get_storage_throttle
                throttle_mgr = get_storage_throttle()

                # Create throttled wrapper
                def throttled_func(*inner_args, **inner_kwargs):
                    # Execute with storage throttling
                    return throttle_mgr.throttle(speed_tier, func, *inner_args, **inner_kwargs)

                final_func = throttled_func

                # Add metadata to tags
                final_tags['storage_throttled'] = 'True'
                final_tags['throttle_tier'] = speed_tier

                # Optional: Log first time we see this operation
                if not hasattr(wrapper, '_storage_logged'):
                    tg_print(
                        'storage',
                        f"Auto-throttling enabled: '{operation_type}' -> {speed_tier} tier",
                    )
                    wrapper._storage_logged = True

            # Dispatch visibility — shows before token enters the pool
            tg_print(
                'pool',
                f'Submitting  fn={func.__name__}  op={operation_type}',
                level='dispatch',
            )

            seed = get_active_seed()  # resolve seed first
            if seed:
                final_tags["conductor_seed"] = seed  # in tags before token exists

            # Lock in function identity for process pool routing
            unwrapped: Any = func
            while hasattr(unwrapped, '__wrapped__'):
                unwrapped = unwrapped.__wrapped__
            identity = get_func_identity(unwrapped)
            final_tags['_func_module'] = identity.module
            final_tags['_func_qualname'] = identity.qualname

            token = global_token_pool.create_token(
                func=final_func,
                args=args,
                kwargs=kwargs,
                operation_type=operation_type,
                tags=final_tags
            )

            if seed:
                conductor.pre_register(seed)  # hold the domain open before put() runs

            cb = getattr(global_token_pool, "default_on_state_change", None)
            if cb is not None and getattr(token, "on_state_change", None) is None:
                token.on_state_change = cb

            return token

        return wrapper

    return decorator


# Global instance
global_token_pool = TokenPool()

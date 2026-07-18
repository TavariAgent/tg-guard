# TokenGuard

> Lightweight, decorator-first task routing for Python.
---

## What It Does

TokenGuard routes your functions to dedicated CPU-pinned workers automatically. Decorate a function, call it, and it dispatches to the right worker without blocking the caller. No manual thread management, minimal boilerplate, no complexity creep between threaded and async contexts.

The routing layers account for task difficulty: `'heavy'` tasks start loading front-bound on the first system core while `'light'` and `'medium'` bind toward the second and third. You get the performance characteristics of a well-tuned thread pool without having to build or maintain one.


---

## How It Works

When you decorate a function with `@task_token_guard`, calling it no longer executes it directly. Instead, a `TaskToken` is created and handed to the coordinator's admission queue. The coordinator routes the token to a pinned mailbox worker based on its weight class and current core load, executes it there, and delivers the result. The caller keeps moving immediately — no blocking, no manual thread management.

The staggered position system ensures tokens are spread across workers in a predictable, thread-safe sequence. Each core tracks its own monotonic counter and position arithmetic naturally shuffles assignments across the worker slots when worker counts change. The stride stays globally consistent and uses a valid range which is determined by the current active worker formation.

---

## Installation

```bash
pip install tg-guard
```

---

## Quick Start

### Give it a try (in your own code)

```python
import asyncio
from tokenguard import task_token_guard, OperationsCoordinator

coordinator = OperationsCoordinator()  # Links the token decorator event bus
coordinator.start() # Starts the bus

@task_token_guard(  # Threads the callsite
    operation_type='resize_image', 
    tags={'weight': 'heavy'}
)
def resize_image(path, size):
    ...

# Caller is not blocked — resize_image runs on a worker
token = resize_image('photo.jpg', (1920, 1080))

# Result available when ready
result = token.get(timeout=30.0)

# Or awaitable
results  = await asyncio.gather(*tokens, return_exceptions=True)

# Always clean up the coordinator on shutdown or after the work is completed.
coordinator.stop()
```

---

## Configuration

TokenGuard ships with sensible defaults. Use `option` and `tg_option` at module
level to override them before starting the coordinator.

```python
from tokenguard import option, tg_option

# Example Coordinator settings (I suggest testing these for yourself.)
option.enable_convergence(False)  # default: True
option.num_executors(12)          # default: 8
option.mailbox_max(500)           # default: 100 — max tokens per worker mailbox
option.recent_executions_max(50)  # default: 100  — history buffer size

# Logging
tg_option.enable('coordinator', 'worker')
tg_option.debug('coordinator', on=True)
tg_option.silence('convergence')
tg_option.silence_all()
tg_option.enable_all()
```

> `workers_per_core` can also be adjusted but convergence must be disabled. (Never exceed 4 or go under 2 for workers.)

---

## Decorator Reference

### `@task_token_guard(operation_type, tags)`

| Parameter        | Type   | Required  | Description                                                  |
|------------------|--------|-----------|--------------------------------------------------------------|
| `operation_type` | `str`  | Yes       | Stable label used for routing, metrics, and admin operations |
| `tags`           | `dict` | No        | Routing and policy hints — see Tag Reference below           |

The decorated function returns a `TaskToken` instead of executing. The caller is not blocked.

---

## Tag Reference

| Tag              | Values                                                                              | Effect                                                                                                                                              |
|------------------|-------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `weight`         | `'heavy'` `'medium'` `'light'`                                                      | Routes token to a specific core range. Heavy → core 1+. Medium → core 2+. Light → core 3+. Defaults to `medium`                                     |
| `storage_speed`  | `'FAST'` `'SLOW'` `'MODERATE'` `'INSANE'`                                           | Wraps the function with storage throttling. Mutually exclusive with `process_pool`                                                                  |
| `process_pool`   | `True`                                                                              | Routes to `ProcessPoolExecutor` instead of thread pool. Args must be picklable. Falls back to thread executor if pickling fails                     |
| `sticky_anchor`  | any `str`                                                                           | Pins all tokens sharing this key to the same core. Useful when a group of operations must stay cache-local                                          |
| `hash_policy`    | `HashPolicy.STANDARD` `HashPolicy.FAST` `HashPolicy.NONE`                           | Controls how args are hashed for external calls routing. See Domain Hashing                                                                         |
| `digest_policy`  | `DigestPolicy.FULL` `DigestPolicy.MINIMAL` `DigestPolicy.FAST` `DigestPolicy.SHORT` | Determines hash length and digest complexity, overlapping hash prevention.                                                                          |
| `external_calls` | `list[str]`                                                                         | Marks this token as a lead token that will dispatch child tokens. Opens a conductor seed domain — all children route to the same core automatically |


> Note: `HashPolicy.NONE` must be declared if you attempt to decorate an un-hashable type, the system will not not operate on these otherwise.

---

## Working With Tokens

Decorated functions return a `TaskToken`. The most common pattern is fire-and-forget:

```python
process_token_file(path)   # dispatched, caller continues
```

When you need the result:

```python
@task_token_guard(
    operation_type='process_file', 
    tags={'weight': 'medium'}
)
def process_token_file(path):
    # ... do work ...
    return  # Happens instantly, caller continues!

# Context based resolution
result = token.get(timeout=30.0)

# Await in async context as well
result = await token

# Or gather a batch
results = await asyncio.gather(token_a, token_b, token_c)
```

### Typing

`TaskToken` is generic over its result type. When you annotate concretely, the type checker knows what `.get()` returns and can verify downstream usage.

### Annotating Tokens

```python
@task_token_guard(operation_type='compute', tags={'weight': 'medium'})
def compute(n: int) -> int:
    return n * n

token: TaskToken[int] = compute(10)
result: int = token.get(timeout=30.0)  # type checker knows this is int
```
>The type flows from the function's return annotation into the token. If the function returns `str`, the token is `TaskToken[str]`, and so on.

### Type Conversion Methods

The double underscored methods (__int__, __float__, __bool__, __str__, etc.) have concrete return types regardless of what T is:

```python
token: TaskToken[int] = compute(10)

n: int   = int(token)    # ✓ — __int__ returns int
f: float = float(token)  # ✓ — __float__ returns float
b: bool  = bool(token)   # ✓ — __bool__ returns bool
s: str   = str(token)    # ✓ — __str__ returns str
```

### Arithmetic & Operators

Arithmetic dunders `(+, -, *, /, **, bitwise ops)` and comparisons return `Any` because the result type depends on what `T` actually is at runtime — the type checker cannot verify this without knowing both operands:

```python
token: TaskToken[int] = compute(10)

result = token + 5      # Any — works at runtime, opaque to type checker
result = token * 2.0    # Any — same

# For a concrete type, unwrap first:
result: int = int(token) + 5    # ✓ — int + int = int, checker is happy
result: int = token.get() + 5   # ✓ — same
```
### Batches

```python
tokens: list[TaskToken[int]] = [compute(i) for i in range(10)]
results: list[int] = [t.get() for t in tokens]

# Or gathered
results = await asyncio.gather(*tokens)
```

### Mixed-type Batches

```python
from typing import Any
from tokenguard import TaskToken

tokens: list[TaskToken[Any]] = [
    compute(10),          # TaskToken[int]
    process_file("x"),    # TaskToken[dict]
]
```

---

## Token Lifecycle

```
CREATED → WAITING → ADMITTED → EXECUTING → COMPLETED
                                         → FAILED
    ↓         ↓         ↓          
KILLED / TIMEOUT (valid from any non-terminal state)
```

Terminal states are permanent unless failed. An `EXECUTING`, `KILLED` or `COMPLETED` token cannot be re-queued unless failed and re-admitted.

---

## Domain Hashing and Sticky Routing

Two mechanisms prevent concurrent access to the same data from different cores.

**Sticky anchor** pins a group of tokens to one core by a shared key:

```python
@task_token_guard(
    operation_type='write_user_record',
    tags={
        'weight': 'medium',
        'sticky_anchor': 'user_records',
    }
)
def write_user_record(user_id, data):
    ...
```

All tokens with `sticky_anchor='user_records'` land on the same core until the inflight token completes. The pin releases automatically on completion.

**Conductor domains** handle coordinated fan-out — a lead token dispatches children and all of them pin to the same core automatically:

```python
from tokenguard import HashPolicy, DigestPolicy
@task_token_guard(
    operation_type='orchestrate_pipeline',
    tags={
        'weight': 'medium',
        'hash_policy': HashPolicy.FAST,
        'digest_policy': DigestPolicy.FAST,
        'external_calls': ['stage_a', 'stage_b', 'stage_c'],
    }
)
def orchestrate_pipeline(data):
    stage_a(data)
    stage_b(data)
    stage_c(data)
```

The lead token charges a conductor seed domain. Any token emitted inside that execution inherits the domain and is routed to the same core, regardless of weight class.

**Hash policy** controls how args are hashed for sticky key resolution:

| Value                  | Behavior                                               |
|------------------------|--------------------------------------------------------|
| `HashPolicy.STANDARD`  | Default. Uses token args directly                      |
| `HashPolicy.FAST`      | Converts args to hashable form before hashing          |
| `HashPolicy.NONE`      | Skips arg-based hashing — uses key name only           |
| `DigestPolicy.FULL`    | Uses the entire 64 char hash length to prevent overlap |
| `DigestPolicy.MINIMAL` | SHA-256 truncated to 8 chars  (32-bit space)           |
| `DigestPolicy.SHORT`   | SHA-256 truncated to 16 chars (64-bit space)           |
| `DigestPolicy.FAST`    | Uses Blake2s 8 byte digest/w 64 char hex               |

---

## OperationsCoordinator

One instance per process. Start it before any decorated functions are called.

```python
from tokenguard import OperationsCoordinator

coordinator = OperationsCoordinator()
coordinator.start()

# ... application main runs ...

coordinator.stop() # Cleans up the coordinator.
```

### Convergence Engine

When `enable_convergence=True`, TokenGuard monitors per-core utilization and adjusts active worker counts every 5 seconds. Leave these at their defaults unless you have a specific reason to change them:

| Setting                | Default | Description                                     |
|------------------------|---------|-------------------------------------------------|
| `utilization_high`     | `80.0`  | Utilization % above which workers scale up      |
| `utilization_low`      | `15.0`  | Utilization % below which workers scale down    |
| `queue_wait_threshold` | `4.0`   | Queue wait time (seconds) that triggers scaling |
| `queue_depth_factor`   | `3`     | Multiplier applied to queue depth pressure      |

These are set the same way using `option`:

```python
option.utilization_high(75.0)
option.utilization_low(25.0)
option.queue_wait_threshold(5.0)
option.queue_depth_factor(4)
``` 

---

## Admin API

Pool-level controls for interfaces, dashboards, or operational tooling. No WebSocket layer is included — these are the clean primitives to build on.

```python
# Kill individual or grouped tokens
coordinator.kill_token(token_id)
coordinator.kill_all_by_operation('resize_image')

# Pause and resume admission (tokens still accept, just queue)
coordinator.pause_admission()
coordinator.resume_admission()

# Per-operation pause / resume
coordinator.pause_operation('resize_image')
coordinator.resume_operation('resize_image')

# Drain waiting tokens
coordinator.drain_pool()
coordinator.drain_operation('resize_image')

# Observability
stats = coordinator.get_stats()
coordinator.print_guard_house_dashboard()
coordinator.dump_execution_history('history.json')
coordinator.get_affinity_report()
```

`get_stats()` returns a composite snapshot covering topology, token pool state, admission gate, worker queue, and convergence status.

---

## Logging

All TokenGuard output goes through `tg_print`. Every channel is off by default.

```python
from tokenguard import tg_option

tg_option.enable('coordinator', 'worker')      # turn on specific channels
tg_option.debug('coordinator', on=True)        # enable debug level for a channel
tg_option.silence('convergence')               # turn off a channel
tg_option.silence_all()                        # quiet everything
tg_option.enable_all()                         # turn everything on
```

Available channels: `gate` `pool` `token` `coordinator` `convergence` `worker` `storage` `guard` `overflow` `affinity` `sticky` `conductor`

> **Note:** Do not enable `convergence` in a REPL. It produces continuous output on a fast poll interval.

---

## TokenGate vs TokenGuard

TokenGuard is a stable, focused branch. TokenGate is the experimental surface where new subsystems are developed and tested before being considered for a branch.

| Feature                             | TokenGate  | TokenGuard  |
|-------------------------------------|------------|-------------|
| `@task_token_guard` decorator       | ✓          | ✓           |
| Pinned staggered queue              | ✓          | ✓           |
| Convergence engine                  | ✓          | ✓           |
| Storage throttle                    | ✓          | ✓           |
| Process pool routing                | ✓          | ✓           |
| Domain hashing / sticky routing     | ✓          | ✓           |
| Admin API (pause/resume/drain/kill) | ✓          | ✓           |
| WebSocket orchestration             | ✓          | —           |
| Allocation optimizer                | ✓          | —           |
| Code inspector                      | ✓          | —           |

---

## Stability & Releases

```
Versioning:
0  .  1  .  0  .  0
│     │     │     └── Patch      — bug fixes, typo corrections
│     │     └──────── Minor      — small improvements, non-breaking additions  
│     └────────────── Update     — meaningful feature additions or tuning
└──────────────────── Major      — architectural changes, breaking API shifts
```

TokenGuard follows a slow, deliberate release cadence by design. The core routing and execution model is stable — updates here are fixes and minor improvements, not architectural experiments.

New subsystems and experimental features are developed in TokenGate first. If something proves solid there, it may eventually be branched into TokenGuard. This means you can build on TokenGuard without worrying about unexpected API changes while you're still learning the performance characteristics and boundaries of the system.

## Requirements
- Python *3.12+ (Earlier versions generally work but aren't tested in depth.)
- Windows, macOS, Linux

[LICENSE](LICENSE.txt)
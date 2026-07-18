# -*- coding: utf-8 -*-
# max_concurrency_test.py
"""
Maximum Concurrency Test for TokenGate
=======================================
Demonstrates TokenGate's natural async cadence: submit a batch of varied
synchronous operations, await all of them together, scale up, repeat.

No internal controls are touched. The orchestrator speaks only the public
API — decorated function calls and ``await token``.

Structure
---------
  4 operation types  (cpu_crunch, string_transform, data_sort, hash_chain)
  15 scaling waves   (4 → 65536 tokens)
  1 pattern          submit batch → await asyncio.gather(*tokens) → report

Metrics
-------
  tok/s             tokens completed per active-second this wave
                    (excludes inter-wave sleep — pure submission+execution time)
  avg latency (ms)  wave_elapsed_ms / token count
  concurrency ratio (wave1_lat × N) / elapsed — speedup vs single-token baseline
  overlap ratio     Σ task_execution_times / elapsed
                    measures how much actual CPU work was compressed by parallelism
                    1.0 = fully sequential, N = N tasks running simultaneously
                    values like 15× mean 15× more work happened than wall-time alone

Timing notes
------------
  Per-wave timing uses time.perf_counter_ns() — integer nanoseconds, no
  floating-point accumulation error on sub-millisecond waves.

  total_active_time  = Σ wave_elapsed  (excludes inter-wave sleep entirely)
  total_wall_time    = perf_counter_ns() from orchestrator start to end
                       (includes sleep, scheduling gaps, print overhead)

  Overall tok/s in the summary is based on total_active_time so it reflects
  true throughput, not calendar time.

Run from the repo root:
  python demo/max_concurrency_test.py
"""

import asyncio
import hashlib
import random
import string
import time
from typing import List, Optional, Any, Callable

from ..operations_coordinator import OperationsCoordinator
from ..token_system import task_token_guard, TaskToken

# ──────────────────────────────────────────────────────────────────[...]
# SYNCHRONOUS OPERATIONS
# ──────────────────────────────────────────────────────────────────[...]

@task_token_guard(operation_type='cpu_crunch', tags={'weight': 'light'})
def cpu_crunch(n: int) -> int:
    """Sum primes up to n — lightweight CPU-bound work."""
    total = 0
    for i in range(2, n):
        if all(i % j != 0 for j in range(2, int(i ** 0.5) + 1)):
            total += i
    return total


@task_token_guard(operation_type='string_ops', tags={'weight': 'light'})
def string_transform(seed: int) -> str:
    """Generate and mangle a string — lightweight string work."""
    rng = random.Random(seed)
    chars = [rng.choice(string.ascii_letters) for _ in range(300)]
    text = ''.join(chars)
    return text[::-1].upper().replace('A', '4').replace('E', '3').replace('I', '1')


@task_token_guard(operation_type='data_transform', tags={'weight': 'medium'})
def data_sort(size: int) -> List[int]:
    """Sort a random list — medium CPU work."""
    rng = random.Random(size)
    data = [rng.randint(0, 100_000) for _ in range(size)]
    return sorted(data)


@task_token_guard(operation_type='hash_compute', tags={'weight': 'heavy'})
def hash_chain(seed: str, iterations: int) -> str:
    """Iterative SHA-256 chain — heavier CPU-bound work."""
    h = seed.encode()
    for _ in range(iterations):
        h = hashlib.sha256(h).digest()
    return h.hex()


# ──────────────────────────────────────────────────────────────────[...]
# OPERATION REGISTRY
# ──────────────────────────────────────────────────────────────────[...]

OPERATIONS: list[Callable[[int], TaskToken[Any]]] = [
    lambda i: cpu_crunch(60 + (i % 25)),
    lambda i: string_transform(i * 31),
    lambda i: data_sort(400 + (i % 300)),
    lambda i: hash_chain(f"wave_seed_{i}", 120 + (i % 80)),
]


# ──────────────────────────────────────────────────────────────────[...]
# BATCH SUBMISSION
# ──────────────────────────────────────────────────────────────────[...]

def submit_batch(count: int) -> list[TaskToken[Any]]:
    """Submit ``count`` tokens round-robin across all operation types."""
    return [OPERATIONS[i % len(OPERATIONS)](i) for i in range(count)]


# ──────────────────────────────────────────────────────────────────[...]
# OVERLAP RATIO
#
# After all tokens in a wave complete, each token carries its own
# execution_time() — the real wall-clock span from when it started running
# on a worker to when it returned. Summing these across the whole batch and
# dividing by the wave's elapsed time answers:
#
#   "How much total work happened compared to what one thread could have done?"
#
# overlap_ratio = Σ token.metadata.execution_time() / wave_elapsed
#
# Examples:
#   ratio = 1.0  → all tasks ran back-to-back on a single thread
#   ratio = 8.0  → on average, 8 tasks were running at the same moment
#   ratio = 0.5  → tasks were so fast that scheduling overhead dominated
#
# Because execution_time() only covers actual CPU time (not queue wait),
# this metric strips out admission latency and shows pure parallelism.
# ──────────────────────────────────────────────────────────────────

def compute_overlap_ratio(tokens: List[TaskToken[Any]], elapsed: float) -> tuple[float, float]:
    """
    Return (overlap_ratio, sum_task_seconds).

    Tokens that failed or never started report None for execution_time()
    and are excluded from the sum so they don't deflate the ratio.
    """
    task_times = [
        exec_time
        for t in tokens
        if (exec_time := t.metadata.execution_time()) is not None
    ]
    if not task_times or elapsed <= 0:
        return 0.0, 0.0

    sum_task_time = sum(task_times)
    return sum_task_time / elapsed, sum_task_time


# ──────────────────────────────────────────────────────────────────
# ASYNC ORCHESTRATOR
# ──────────────────────────────────────────────────────────────────

RELEASE_TARGETS = [4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]

# Inter-wave pause — gives the coordinator's convergence metrics a breath.
# asyncio.gather guarantees all tokens are done before this runs, so it is
# dead time and is deliberately excluded from active-time accounting.
_INTER_WAVE_SLEEP = 0.10


async def run_wave(
    wave_num:           int,
    target:             int,
    baseline_lat_ms:    Optional[float],
) -> dict[str, Any]:
    """Submit a batch of tokens and await all of them concurrently."""

    print(f"\n[WAVE {wave_num}/{len(RELEASE_TARGETS)}]  target = {target} tokens")

    tokens: list[TaskToken[Any]] = submit_batch(target)
    print(f"  {len(tokens)} tokens submitted — awaiting...")

    # Integer nanoseconds — no float accumulation error on sub-ms waves
    t_start_ns  = time.perf_counter_ns()
    results     = await asyncio.gather(*tokens, return_exceptions=True)
    elapsed_ns  = time.perf_counter_ns() - t_start_ns

    elapsed     = elapsed_ns / 1_000_000_000          # seconds  (display only)
    elapsed_ms  = elapsed_ns / 1_000_000               # ms       (display only)

    successes   = sum(1 for r in results if not isinstance(r, Exception))
    failures    = sum(1 for r in results if isinstance(r, Exception))

    # tok/s based on integer ns — immune to float rounding on fast waves
    tok_per_sec = (target * 1_000_000_000) / elapsed_ns if elapsed_ns > 0 else float('inf')
    avg_lat_ms  = elapsed_ms / target

    # Concurrency ratio — speedup vs single-token sequential baseline
    conc_ratio  = (
        (baseline_lat_ms * target) / elapsed_ms
        if baseline_lat_ms is not None else 1.0
    )

    # Overlap ratio — actual parallel CPU work vs wave wall time
    overlap_ratio, sum_task_s = compute_overlap_ratio(tokens, elapsed)

    print(
        f"  {successes}/{target} ok  |  {failures} failed  |  "
        f"{elapsed_ms:.3f}ms  ({tok_per_sec:.1f} tok/s)  "
        f"lat {avg_lat_ms:.3f}ms  conc {conc_ratio:.2f}×  "
        f"overlap {overlap_ratio:.2f}×  (Σ task={sum_task_s * 1000:.2f}ms)"
    )

    return {
        'wave':          wave_num,
        'target':        target,
        'successes':     successes,
        'failures':      failures,
        'elapsed_ns':    elapsed_ns,
        'elapsed':       elapsed,
        'tok_per_sec':   tok_per_sec,
        'avg_lat_ms':    avg_lat_ms,
        'conc_ratio':    conc_ratio,
        'overlap_ratio': overlap_ratio,
        'sum_task_ms':   sum_task_s * 1000,
    }


async def orchestrator(coordinator: OperationsCoordinator) -> None:
    print()
    print("=" * 86)
    print("  MAX CONCURRENCY TEST — TokenGate")
    print(f"  Operations : {len(OPERATIONS)}")
    print(f"  Waves      : {RELEASE_TARGETS}")
    print("=" * 86)

    # ── Warmup ────────────────────────────────────────────────────────────
    print("\n  [WARMUP]  Priming workers with 256 tokens (not recorded)...")
    warmup_tokens = submit_batch(256)
    await asyncio.gather(*warmup_tokens, return_exceptions=True)
    await asyncio.sleep(_INTER_WAVE_SLEEP)
    print("  [WARMUP]  Done.\n")
    # ─────────────────────────────────────────────────────────────────────

    wall_start_ns   = time.perf_counter_ns()
    wave_results    = []
    total_active_ns = 0                    # Σ wave elapsed only — no sleep, no gaps
    baseline_lat_ms = None

    for wave_num, target in enumerate(RELEASE_TARGETS, start=1):
        result = await run_wave(wave_num, target, baseline_lat_ms)
        wave_results.append(result)
        total_active_ns += result['elapsed_ns']

        if wave_num == 1:
            baseline_lat_ms = result['avg_lat_ms']

        # Short pause — excluded from active-time accounting
        await asyncio.sleep(_INTER_WAVE_SLEEP)

    total_wall_ns   = time.perf_counter_ns() - wall_start_ns

    # ── Derived summary scalars ───────────────────────────────────────────
    total_active_s   = total_active_ns  / 1_000_000_000
    total_wall_s     = total_wall_ns    / 1_000_000_000
    total_tokens     = sum(r['target']        for r in wave_results)
    total_ok         = sum(r['successes']     for r in wave_results)
    total_fail       = sum(r['failures']      for r in wave_results)
    overall_lat      = sum(r['avg_lat_ms']    for r in wave_results) / len(wave_results)
    peak_conc        = max(r['conc_ratio']    for r in wave_results)
    peak_overlap     = max(r['overlap_ratio'] for r in wave_results)

    # Volume-weighted: total_tokens / Σ wave_elapsed — the real sustained rate.
    # Dominated by the large waves which have the most tokens.
    overall_tokps    = total_tokens / total_active_s if total_active_s > 0 else float('inf')

    # Peak single-wave throughput — best the system hit at its sweet-spot batch size.
    peak_tokps       = max(r['tok_per_sec'] for r in wave_results)

    # Arithmetic mean of per-wave rates — useful for comparing against peak
    # but not a throughput number: small waves inflate it.
    mean_tokps       = sum(r['tok_per_sec'] for r in wave_results) / len(wave_results)

    token_weighted_tokps = sum(r['tok_per_sec'] * r['target'] for r in wave_results) / total_tokens

    # ── FINAL SUMMARY ─────────────────────────────────────────────────────
    print()
    print("=" * 86)
    print("  RESULTS SUMMARY")
    print("=" * 86)

    h = (f"  {'Wave':<6} {'Tokens':<8} {'OK':<5} {'Fail':<5} "
         f"{'Time':>9}  {'Tok/s':>9}  {'Lat(ms)':>8}  {'Conc':>6}  {'Overlap':>8}  {'ΣTask(ms)':>10}")
    div = "  " + "-" * (len(h) - 2)
    print(h)
    print(div)

    for r in wave_results:
        print(
            f"  {r['wave']:<6} {r['target']:<8} {r['successes']:<5} {r['failures']:<5} "
            f"{r['elapsed'] * 1000:>8.3f}ms  {r['tok_per_sec']:>9.1f}  "
            f"{r['avg_lat_ms']:>7.3f}ms  {r['conc_ratio']:>5.2f}×  "
            f"{r['overlap_ratio']:>7.2f}×  {r['sum_task_ms']:>9.2f}ms"
        )

    print(div)
    print(
        f"  {'TOTAL':<6} {total_tokens:<8} {total_ok:<5} {total_fail:<5} "
        f"  active {total_active_s:.3f}s  wall {total_wall_s:.3f}s"
    )
    print()
    print(f"  Throughput — sustained (volume-weighted) : {overall_tokps:>10,.1f} tok/s")
    print(f"  Throughput — peak single wave            : {peak_tokps:>10,.1f} tok/s")
    print(f"  Throughput — token-weighted mean         : {token_weighted_tokps:>10,.1f} tok/s")
    print(f"  Throughput — arithmetic mean per wave    : {mean_tokps:>10,.1f} tok/s")
    print()
    print(f"  Avg latency across waves                 : {overall_lat:>10.3f} ms/token")
    print(f"  Peak concurrency ratio                   : {peak_conc:>10.2f}×")
    print(f"  Peak overlap ratio                       : {peak_overlap:>10.2f}×")
    print()
    print(f"  Active time  = Σ wave elapsed only  (excludes {_INTER_WAVE_SLEEP * len(RELEASE_TARGETS):.2f}s inter-wave sleep)")
    print(f"  Wall time    = full orchestrator span including sleep and scheduling")
    print()
    print("  Sustained        = total_tokens / Σ wave_elapsed — pulled by large slow waves.")
    print("  Peak             = fastest single wave — ceiling at sweet-spot batch size.")
    print("  Token-wtd mean   = Σ(rate × tokens) / total_tokens — each token votes equally.")
    print("  Mean             = arithmetic average of per-wave rates — inflated by small waves.")
    print()
    print("  Overlap ratio = Σ(individual task times) / wave elapsed time")
    print("  Values above 1× indicate true parallel execution.")
    print("  Values approaching N = N tasks running simultaneously.")
    print("=" * 86)
    print()


# ──────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────

async def main() -> None:
    coordinator = OperationsCoordinator()
    coordinator.start()
    print(coordinator.convergence)
    try:
        await orchestrator(coordinator)
    finally:
        coordinator.stop()


if __name__ == "__main__":
    asyncio.run(main())
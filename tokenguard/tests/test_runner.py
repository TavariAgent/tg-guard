# -*- coding: utf-8 -*-
# test_runner.py
"""
TokenGuard — Test Suite

  start          Start the OperationsCoordinator
  stop           Stop the coordinator
  status         Show coordinator state and current option values

  1  defaults    Core routing with library defaults
  2  configured  Same suite with explicit option overrides applied first
  3  convergence Sustained CPU + I/O load benchmark
  4  sticky      Sticky token cache storm
  5  conductor   Hash conductor domain anchoring

  help / ?       Show this message
  exit / quit    Stop coordinator and exit
"""

import asyncio
import os
import tempfile
import time
from typing import Optional, Any

from ..token_options import option, tg_option
from ..operations_coordinator import OperationsCoordinator

from .ops.cpu_ops import (
    trivial_operation, simple_operation, string_operation,
    moderate_operation, prime_operation, complex_operation,
    heavy_operation, fibonacci_operation,
)
from .ops.io_ops import write_json_fast, append_log_slow, write_blob_moderate
from .ops.chain_ops import run_chain_demo, run_parallel_chains_demo
from .ops.cache_storm import run_cache_storm_test
from .ops.hash_conductor_test import run_hash_conductor_test

# ── Shared state ──────────────────────────────────────────────────────────────

_coordinator: Optional[OperationsCoordinator] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _require_coordinator() -> bool:
    if _coordinator is None:
        print("  [!] Coordinator not running. Type 'start' first.")
        return False
    return True


async def _gather_batch(label: str, tokens: list[Any], expected: int) -> list[Any]:
    """Await all tokens concurrently, assert count, return results."""
    results = []
    failed = 0
    gathered = await asyncio.gather(*tokens, return_exceptions=True)
    for i, result in enumerate(gathered):
        if isinstance(result, Exception):
            failed += 1
            print(f"    ✗ [{label}] task {i} failed: {result}")
        else:
            results.append(result)
    status = "✓" if len(results) == expected else "✗"
    print(f"  {status} {label:<28}  {len(results)}/{expected} resolved   {failed} failed")
    assert len(results) == expected, f"{label}: expected {expected}, got {len(results)}"
    return results


async def _run_core_suite(base_dir: str) -> None:
    """CPU, I/O, and chain proofs — shared by defaults and configured modes."""

    # ── CPU ────────────────────────────────────────────────────────────────
    print("\n  [ CPU ]")
    await _gather_batch("trivial", [trivial_operation(x) for x in range(20)], 20)
    await _gather_batch("simple", [simple_operation(n) for n in [100, 200, 300, 150, 250] * 4], 20)
    await _gather_batch("string", [string_operation(n) for n in [10, 20, 30, 15, 25] * 3], 15)
    await _gather_batch("moderate", [moderate_operation(n) for n in [500, 1000, 1500, 750, 1250] * 3], 15)
    await _gather_batch("prime", [prime_operation(n) for n in [97, 101, 103, 107, 109, 113, 127, 131, 137, 139,
                                                               100, 102, 104, 106, 108]], 15)
    await _gather_batch("complex", [complex_operation(n) for n in [10, 15, 20, 12, 18] * 2], 10)
    await _gather_batch("heavy", [heavy_operation(n) for n in [100, 150, 200, 120, 180] * 2], 10)
    await _gather_batch("fibonacci", [fibonacci_operation(n) for n in [100, 200, 300, 150, 250, 180, 220, 280]], 8)

    # ── I/O ────────────────────────────────────────────────────────────────
    print("\n  [ I/O ]")
    await _gather_batch("write_json", [write_json_fast(
        os.path.join(base_dir, "json", f"r_{i}.json"), {"i": i, "sq": i * i}
    ) for i in range(10)], 10)
    await _gather_batch("append_log", [append_log_slow(
        os.path.join(base_dir, "logs", "out.log"), f"entry {i}"
    ) for i in range(10)], 10)
    await _gather_batch("write_blob", [write_blob_moderate(
        os.path.join(base_dir, "blob", f"b_{i}.bin"), 8
    ) for i in range(6)], 6)

    # ── Chains ─────────────────────────────────────────────────────────────
    print("\n  [ Chains ]")
    run_chain_demo(chain_count=5)
    run_parallel_chains_demo(chain_count=4)


# ── Command handlers ──────────────────────────────────────────────────────────

def cmd_start() -> None:
    global _coordinator
    if _coordinator is not None:
        print("  Coordinator already running.")
        return
    _coordinator = OperationsCoordinator()
    _coordinator.start()
    print("  ✓ Coordinator started.")


def cmd_stop() -> None:
    global _coordinator
    if _coordinator is None:
        print("  Coordinator is not running.")
        return
    _coordinator.stop()
    _coordinator = None
    print("  ✓ Coordinator stopped.")


def cmd_status() -> None:
    state = "running" if _coordinator else "stopped"
    print(f"\n  Coordinator : {state}")
    print(f"\n{option.status()}")
    print(f"\n{tg_option.status()}")


def cmd_defaults() -> None:
    if not _require_coordinator(): return
    print("\n══ DEFAULTS ═════════════════════════════════════════════════════════")
    print("  No option overrides — all values at library defaults.\n")
    print(option.status())
    base_dir = tempfile.mkdtemp(prefix="tg_defaults_")
    t0 = time.monotonic()
    asyncio.run(_run_core_suite(base_dir))
    elapsed = time.monotonic() - t0
    print(f"\n  ✓ Defaults suite complete  ({elapsed:.2f}s)")
    print("════════════════════════════════════════════════════════════════════")


# Configure Options
def cmd_configured() -> None:
    if not _require_coordinator(): return
    print("\n══ CONFIGURED ═══════════════════════════════════════════════════════")
    print("  Applying option overrides...\n")

    option.utilization_high(60.0)
    option.utilization_low(10.0)
    option.queue_wait_threshold(5.0)
    option.max_retries(3)
    option.min_failure_duration(8.0)
    tg_option.enable('coordinator', 'worker')

    print(option.status())
    print()

    base_dir = tempfile.mkdtemp(prefix="tg_configured_")
    t0 = time.monotonic()
    asyncio.run(_run_core_suite(base_dir))
    elapsed = time.monotonic() - t0

    tg_option.silence('coordinator', 'worker')
    print(f"\n  ✓ Configured suite complete  ({elapsed:.2f}s)")
    print("════════════════════════════════════════════════════════════════════")


async def _convergence_benchmark() -> None:
    # Escalating wave sizes — each wave builds on the previous load profile
    # so convergence has time to observe pressure and hot-swap worker counts.
    WAVE_SIZES = [2_000, 8_000, 40_000, 60_000, 40_000]
    TOTAL = sum(WAVE_SIZES)

    # Task mix per wave:
    #   40% moderate  — medium weight, real queue depth
    #   30% heavy     — pins core 1, forces convergence to respond
    #   20% log append — I/O with storage throttle
    #   10% fibonacci — process pool, different pressure profile
    def _build_wave(n: int, wave: int, base_dir: str) -> list[Any]:
        mod_count = int(n * 0.40)
        heavy_count = int(n * 0.30)
        io_count = int(n * 0.20)
        fib_count = n - mod_count - heavy_count - io_count  # remainder = ~10%

        tokens: list[Any] = []
        mod_args = [500, 1000, 750, 1250, 800, 900, 600, 1100]
        heavy_args = [80, 100, 120, 90, 110, 95, 105, 85]
        fib_args = [100, 150, 200, 120, 180, 130, 160, 140]

        tokens += [moderate_operation(mod_args[i % len(mod_args)]) for i in range(mod_count)]
        tokens += [heavy_operation(heavy_args[i % len(heavy_args)]) for i in range(heavy_count)]
        tokens += [append_log_slow(
            os.path.join(base_dir, f"w{wave}", "bench.log"),
            f"wave={wave} token={i}"
        ) for i in range(io_count)]
        tokens += [fibonacci_operation(fib_args[i % len(fib_args)]) for i in range(fib_count)]
        return tokens

    print("\n══ CONVERGENCE BENCHMARK ════════════════════════════════════════════")
    print(f"  Waves          : {len(WAVE_SIZES)}  (escalating load)")
    print(f"  Wave sizes     : {' → '.join(f'{n:,}' for n in WAVE_SIZES)}")
    print(f"  Total tokens   : {TOTAL:,}")
    print(f"  Task mix       : 40% moderate  30% heavy  20% I/O  10% fibonacci")
    print(f"  Convergence    : enabled — watch for pattern adjustments between waves\n")

    tg_option.enable('convergence')
    base_dir = tempfile.mkdtemp(prefix="tg_convergence_")
    wave_times = []
    wave_counts = []
    total_resolved = 0
    total_failed = 0

    for wave, wave_size in enumerate(WAVE_SIZES, start=1):
        print(f"  ── Wave {wave}/{len(WAVE_SIZES)}  ({wave_size:,} tokens) " + "─" * 35)
        wave_start = time.monotonic()

        all_tokens = _build_wave(wave_size, wave, base_dir)
        submitted = len(all_tokens)

        gathered = await asyncio.gather(*all_tokens, return_exceptions=True)
        resolved = sum(1 for r in gathered if not isinstance(r, Exception))
        failed = submitted - resolved

        wave_elapsed = time.monotonic() - wave_start
        throughput = resolved / wave_elapsed if wave_elapsed > 0 else 0
        wave_times.append(wave_elapsed)
        wave_counts.append(submitted)
        total_resolved += resolved
        total_failed += failed

        print(f"    Submitted  : {submitted:,}")
        print(f"    Resolved   : {resolved:,}   Failed: {failed}")
        print(f"    Wall time  : {wave_elapsed:.2f}s")
        print(f"    Throughput : {throughput:.1f} tokens/s\n")

    tg_option.silence('convergence')

    total_wall = sum(wave_times)
    avg_wave = total_wall / len(wave_times)
    overall_tp = total_resolved / total_wall if total_wall > 0 else 0

    print(f"  ┌──────────────────────────────────────────────────┐")
    print(f"  │  Convergence Benchmark Summary                   │")
    print(f"  ├──────────────────────────────────────────────────┤")
    print(f"  │  Total tokens   : {TOTAL:<8,}                       │")
    print(f"  │  Total resolved : {total_resolved:<8,}                       │")
    print(f"  │  Total failed   : {total_failed:<8,}                       │")
    print(f"  │  Total wall time: {total_wall:<6.2f}s                        │")
    print(f"  │  Avg per wave   : {avg_wave:<6.2f}s                        │")
    print(f"  │  Overall tp     : {overall_tp:<8.1f} tokens/s              │")
    print(f"  └──────────────────────────────────────────────────┘")

    print(f"\n  Per-wave breakdown:")
    for i, (t, n) in enumerate(zip(wave_times, wave_counts)):
        tp = n / t if t > 0 else 0
        print(f"    W{i + 1}  {n:>6,} tokens  {t:6.2f}s  {tp:8.1f} tokens/s")

    if _coordinator:
        stats = _coordinator.get_stats()
        conv = stats.get('convergence')
        if conv:
            print(f"\n  Final convergence state:")
            for k, v in conv.items():
                print(f"    {k:<32} {v}")

    print("\n  ✓ Convergence benchmark complete.")
    print("════════════════════════════════════════════════════════════════════")


def cmd_convergence() -> None:
    if not _require_coordinator(): return
    asyncio.run(_convergence_benchmark())


def cmd_sticky() -> None:
    if not _require_coordinator(): return
    run_cache_storm_test(coordinator=_coordinator)


def cmd_conductor() -> None:
    if not _require_coordinator(): return
    run_hash_conductor_test(coordinator=_coordinator)


def cmd_help() -> None:
    print("""
  ── TokenGuard Test Suite ─────────────────────────────────────────────

   start          Start the OperationsCoordinator
   stop           Stop the coordinator
   status         Show coordinator state and current option values

   1  defaults    Core routing with library defaults — no option changes
   2  configured  Same suite with explicit option overrides applied first
   3  convergence Sustained load benchmark — CPU + I/O waves, throughput report
   4  sticky      Sticky token cache storm — registry contract under pressure
   5  conductor   Hash conductor — seed domain anchoring across concurrent leads

   help / ?       Show this message
   exit / quit    Stop coordinator and exit

  ──────────────────────────────────────────────────────────────────────
""")


# ── Dispatch ──────────────────────────────────────────────────────────────────

COMMANDS = {
    'start': cmd_start,
    'stop': cmd_stop,
    'status': cmd_status,
    '1': cmd_defaults, 'defaults': cmd_defaults,
    '2': cmd_configured, 'configured': cmd_configured,
    '3': cmd_convergence, 'convergence': cmd_convergence,
    '4': cmd_sticky, 'sticky': cmd_sticky,
    '5': cmd_conductor, 'conductor': cmd_conductor,
    '?': cmd_help, 'help': cmd_help,
}

BANNER = """
╔═════════════════════════════════════════════════════╗
║               TokenGuard — Test Suite               ║
║ Type a number or keyword.  'help' to list commands. ║
║ Start here: 'start'  then  '1' through '5'          ║
╚═════════════════════════════════════════════════════╝
"""


# ── REPL ──────────────────────────────────────────────────────────────────────

def repl() -> None:
    print(BANNER)
    while True:
        try:
            raw = input("tokenguard> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not raw:
            continue

        low = raw.lower()

        if low in ('exit', 'quit'):
            break

        handler = COMMANDS.get(low)
        if handler:
            try:
                handler()
            except AssertionError as e:
                print(f"  [FAIL] {e}")
            except Exception as e:
                print(f"  [ERROR] {type(e).__name__}: {e}")
        else:
            print(f"  Unknown command: '{raw}'.  Type 'help' or '?' for options.")

    if _coordinator:
        print("  Stopping coordinator...")
        cmd_stop()
    print("  Goodbye.")


if __name__ == "__main__":
    repl()

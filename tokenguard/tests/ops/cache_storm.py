# -*- coding: utf-8 -*-
# demo/cache_storm.py
"""
Cache Storm Test
================
Stress-tests the StickyTokenRegistry under high-concurrency same-key pressure.

                          ┌─────────────────────────────────┐
  Phase 1 — Anchor        │  Submit N slow tokens, one per  │
                          │  distinct arg value.  Each key  │
                          │  gets pinned to a core by the   │
                          │  sticky registry.               │
                          └─────────────────────────────────┘
                                         │
                          ┌─────────────────────────────────┐
  Phase 2 — Storm         │  While anchors are still        │
  (injection thread)      │  inflight, fire STORM_WAVES     │
                          │  bursts of tokens using the     │
                          │  EXACT same (op_type, args).    │
                          │  Natural load-balancing would   │
                          │  scatter these to other cores.  │
                          │  The sticky registry must       │
                          │  redirect them all back.        │
                          └─────────────────────────────────┘
                                         │
                          ┌─────────────────────────────────┐
  Analysis                │  After all tokens resolve,      │
                          │  group by arg value and check   │
                          │  that every token's sticky_core │
                          │  tag matches the first-seen     │
                          │  core for that key.             │
                          │                                 │
                          │  Miss  = key landed on >1 core  │
                          │  (must be zero for the sticky   │
                          │  contract to hold)              │
                          └─────────────────────────────────┘

Redirection count uses route_position (the natural assignment before sticky
enforcement) vs sticky_core (the actual core after enforcement).  This is
available without modifying any queue code because put() already tags both
fields onto every token.
"""

import threading
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from ...sticky_token import sticky_registry
from .cache_storm_ops import ANCHOR_HOLD_SECS, storm_anchor_op


# ── Test parameters ────────────────────────────────────────────────────────────

# One unique sticky key is created per value.  Four keys spread across
# the medium-core range (cores 2+) so the storm can hit multiple domains.
ANCHOR_ARGS: List[int] = [101, 202, 303, 404]

# Number of same-key bursts fired from the injection thread.
# Total storm tokens = STORM_WAVES × len(ANCHOR_ARGS).
STORM_WAVES: int = 6

# Pause between waves — short enough that many are still inflight simultaneously.
STORM_WAVE_DELAY: float = 0.06


# ── Helpers ────────────────────────────────────────────────────────────────────

def _resolve_queue_geometry(coordinator: Any) -> Tuple[Optional[int], Optional[int]]:
    """Try to pull total_workers / workers_per_core from the coordinator."""
    q = getattr(coordinator, "worker_queue", None)
    if q is not None and hasattr(q, "get_stats"):
        stats = q.get_stats()
        return stats.get("total_workers"), stats.get("workers_per_core")
    return None, None


def _natural_core(
    route_position: int,
    total_workers: int,
    workers_per_core: int,
) -> int:
    """Return the core the token *would* have gone to without sticky enforcement."""
    worker_idx = route_position % total_workers
    return (worker_idx // workers_per_core) + 1


# ── Main entry point ───────────────────────────────────────────────────────────

def run_cache_storm_test(coordinator: Any = None) -> None:
    """Execute the cache storm test and print a full report.

    Args:
        coordinator: Optional OperationsCoordinator reference.  Supplying it
                     enables redirection counting; omitting it skips that metric
                     but does not affect miss detection.
    """

    total_storm = STORM_WAVES * len(ANCHOR_ARGS)
    total_expected = len(ANCHOR_ARGS) + total_storm

    print("\n── CACHE STORM TEST ────────────────────────────────────────────────")
    print(f"  Keys        : {ANCHOR_ARGS}")
    print(f"  Storm waves : {STORM_WAVES}  ({total_storm} storm tokens)")
    print(f"  Anchor hold : {ANCHOR_HOLD_SECS}s  (storm window)")
    print(f"  Total tokens: {total_expected}\n")

    # ── Phase 1: Submit anchors ────────────────────────────────────────────────
    print("  Phase 1 — Anchoring keys...")

    anchor_tokens: List[Tuple[int, Any]] = []
    for n in ANCHOR_ARGS:
        token = storm_anchor_op(n)
        anchor_tokens.append((n, token))

    # Let the registry mark all keys before the storm starts
    time.sleep(0.06)

    # Snapshot: shows which core each key was pinned to
    pin_snapshot: Dict[str, int] = sticky_registry.snapshot()
    print(f"\n  Registry after anchor phase ({len(pin_snapshot)} keys pinned):")
    for key_repr, core_id in pin_snapshot.items():
        print(f"    {key_repr!r:55s}→  Core {core_id}")

    if not pin_snapshot:
        print("  [!] No keys visible in registry — anchors may have completed "
              "too quickly.  Increase ANCHOR_HOLD_SECS in cache_storm_ops.py.")

    # ── Phase 2: Storm ─────────────────────────────────────────────────────────
    print(f"\n  Phase 2 — Firing {STORM_WAVES} waves from injection thread...")

    storm_tokens: List[Tuple[int, Any]] = []
    storm_lock = threading.Lock()

    def _storm_worker() -> None:
        for wave in range(1, STORM_WAVES + 1):
            for n in ANCHOR_ARGS:
                t = storm_anchor_op(n)
                with storm_lock:
                    storm_tokens.append((n, t))
            time.sleep(STORM_WAVE_DELAY)
            snap = sticky_registry.snapshot()
            print(
                f"    wave {wave:02d}/{STORM_WAVES}  "
                f"inflight keys: {len(snap):2d}  "
                f"storm submitted: {wave * len(ANCHOR_ARGS):3d}",
                flush=True,
            )

    storm_thread = threading.Thread(
        target=_storm_worker, daemon=True, name="storm-injector"
    )
    storm_thread.start()
    storm_thread.join()

    # ── Wait for all tokens to resolve ────────────────────────────────────────
    timeout = max(30.0, ANCHOR_HOLD_SECS * 4)
    print(f"\n  Resolving {total_expected} tokens (timeout {timeout:.0f}s each)...")

    all_tokens: List[Tuple[int, Any, str]] = (
        [(n, t, "anchor") for n, t in anchor_tokens]
        + [(n, t, "storm") for n, t in storm_tokens]
    )

    resolved = 0
    failed = 0
    for n, token, kind in all_tokens:
        try:
            result = token.get(timeout=timeout)
            expected = n * n
            if result != expected:
                print(f"    [!] Wrong result  {kind}({n}): got {result}, want {expected}")
            resolved += 1
        except Exception as exc:
            failed += 1
            print(f"    ✗  {kind}({n}) failed: {exc}")

    # ── Analysis ───────────────────────────────────────────────────────────────
    print("\n  Analysing core fidelity...\n")

    total_workers, workers_per_core = (None, None)
    if coordinator is not None:
        total_workers, workers_per_core = _resolve_queue_geometry(coordinator)

    # Group sticky_core observations by arg value
    cores_seen: Dict[int, Set[int]] = defaultdict(set)
    redirections = 0
    geometry_available = total_workers and workers_per_core

    for n, token, kind in all_tokens:
        sticky_core = token.metadata.tags.get("sticky_core")
        route_pos   = token.metadata.tags.get("route_position")

        if sticky_core is not None:
            cores_seen[n].add(sticky_core)

        if total_workers is not None and workers_per_core is not None and sticky_core is not None and route_pos is not None:
            nat = _natural_core(int(route_pos), total_workers, workers_per_core)
            if nat != sticky_core:
                redirections += 1

    # Per-key report
    misses = 0
    for n in ANCHOR_ARGS:
        cores = sorted(cores_seen.get(n, set()))
        tokens_for_key = sum(
            1 for an, _, _ in all_tokens if an == n
        )
        if len(cores) > 1:
            misses += 1
            print(f"  MISS   n={n:>4}  cores seen: {cores}  ({tokens_for_key} tokens)")
        else:
            core_label = cores[0] if cores else "?"
            print(
                f"  ✓      n={n:>4}  all {tokens_for_key:>2} tokens → Core {core_label}"
            )

    # ── Summary box ───────────────────────────────────────────────────────────
    miss_label = "✓ NONE" if misses == 0 else f"✗ {misses} KEY(S) BROKEN"

    print(f"""
  ┌──────────────────────────────────────────────┐
  │  Cache Storm Summary                         │
  ├──────────────────────────────────────────────┤
  │  Tokens resolved: {resolved:>4} / {total_expected:<4}                │
  │  Tokens failed:  {failed:>4}                        │
  │  Anchor tokens:  {len(ANCHOR_ARGS):>4}                        │
  │  Storm tokens:    {total_storm:>4}                       │""")

    if geometry_available:
        print(
            f"  │  Redirections:    {redirections:>4}                       │"
        )
    else:
        print(
            f"  │  Redirections:      ? (pass coordinator for count) │"
        )

    print(f"  │  Misses: {miss_label:<28}        │")
    print(f"  └──────────────────────────────────────────────┘")

    verdict = (
        "  ✓ Sticky registry held under cache storm — zero misses.\n"
        if misses == 0
        else f"  ✗ {misses} key(s) routed to multiple cores — sticky contract broken.\n"
    )
    print(verdict)
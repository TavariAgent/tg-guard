# -*- coding: utf-8 -*-
# demo/hash_conductor_test.py
"""
Hash Conductor Test
===================
Verifies that the HashConductor correctly anchors a lead token and all
tokens it spawns to a single core domain for the lifetime of the chain.

What is checked
    Core fidelity   — every child's sticky_core matches its lead's sticky_core
    Seed fidelity   — every child's conductor_seed matches its lead's conductor_seed
    Domain release  — conductor snapshot is empty after all tokens resolve
                      (no leaked seeds)

What a miss means
    Any child that landed on a different core than its lead.  This would
    indicate the conductor seed was not propagated correctly through the
    executor thread boundary, or register_child() routed incorrectly.

Structure
    N_LEADS lead tokens are submitted concurrently.  Each lead spawns
    CHILDREN_PER_LEAD children from inside its executor thread.  Leads
    are independent so their seeds must never overlap.
"""

from collections import defaultdict
from typing import Any, Optional

from ...hash_conductor import conductor
from .hash_conductor_ops import CHILDREN_PER_LEAD, conductor_lead_op

N_LEADS: int = 3   # independent concurrent lead tokens


# ── Helpers ────────────────────────────────────────────────────────────────────

def _resolve_queue_geometry(coordinator: Any):
    q = getattr(coordinator, "worker_queue", None)
    if q is not None and hasattr(q, "get_stats"):
        s = q.get_stats()
        return s.get("total_workers"), s.get("workers_per_core")
    return None, None


# ── Main entry point ───────────────────────────────────────────────────────────

def run_hash_conductor_test(coordinator: Optional[Any] = None) -> None:
    """Execute the hash conductor test and print a full report."""

    total_tokens = N_LEADS * (1 + CHILDREN_PER_LEAD)

    print("\n── HASH CONDUCTOR TEST ─────────────────────────────────────────────")
    print(f"  Lead tokens     : {N_LEADS}")
    print(f"  Children / lead : {CHILDREN_PER_LEAD}")
    print(f"  Total tokens    : {total_tokens}\n")

    # ── Submit leads ───────────────────────────────────────────────────────────
    print("  Submitting lead tokens...")
    lead_tokens = [(n, conductor_lead_op(n)) for n in range(1, N_LEADS + 1)]

    # ── Phase 1: resolve leads ─────────────────────────────────────────────────
    # Each lead returns a list of child TaskToken objects.
    # Leads are fast (just a list comprehension) so this resolves quickly.
    print("  Phase 1 — resolving leads (returns child tokens)...")

    lead_results = []   # [(lead_n, lead_token, [child_tokens])]
    failed = 0

    for lead_n, token in lead_tokens:
        try:
            child_tokens = token.get(timeout=15)
            lead_results.append((lead_n, token, child_tokens))
        except Exception as exc:
            failed += 1
            print(f"  ✗  lead_n={lead_n} failed: {type(exc).__name__}: {exc}")

    # ── Phase 2: resolve children ──────────────────────────────────────────────
    print(f"  Phase 2 — resolving {len(lead_results) * CHILDREN_PER_LEAD} children...\n")

    results = []
    for lead_n, lead_token, child_tokens in lead_results:
        child_data = []
        for child_token in child_tokens:
            try:
                child_token.get(timeout=15)
                child_data.append({
                    "n":    child_token.args[0] if child_token.args else "?",
                    "core": child_token.metadata.tags.get("sticky_core"),
                    "seed": child_token.metadata.tags.get("conductor_seed"),
                })
            except Exception as exc:
                failed += 1
                print(f"  ✗  child failed: {type(exc).__name__}: {exc}")

        results.append({
            "lead_n":    lead_n,
            "lead_core": lead_token.metadata.tags.get("sticky_core"),
            "lead_seed": lead_token.metadata.tags.get("conductor_seed"),
            "children":  child_data,
        })

    # ── Analysis ───────────────────────────────────────────────────────────────
    print("  Analysing domain fidelity...\n")

    misses       = 0
    seed_clashes = 0
    seen_seeds   = {}   # seed → lead_n (catches cross-lead seed collisions)

    for r in results:
        lead_n    = r["lead_n"]
        lead_core = r["lead_core"]
        lead_seed = r["lead_seed"]
        children  = r["children"]
        seed_short = lead_seed[:12] + "…" if lead_seed else "?"

        # Check for seed uniqueness across leads
        if lead_seed in seen_seeds:
            seed_clashes += 1
            print(f"  SEED CLASH  lead_n={lead_n} shares seed with lead_n={seen_seeds[lead_seed]}")
        else:
            seen_seeds[lead_seed] = lead_n

        # Check core and seed fidelity for each child
        child_cores = [c["core"] for c in children]
        child_seeds = [c["seed"] for c in children]

        core_ok = all(c == lead_core for c in child_cores)
        seed_ok = all(s == lead_seed for s in child_seeds)

        if core_ok and seed_ok:
            print(
                f"  ✓  lead_n={lead_n}  seed={seed_short}  "
                f"all {1 + len(children)} tokens → Core {lead_core}"
            )
        else:
            misses += 1
            bad_cores  = [c for c in child_cores if c != lead_core]
            bad_seeds  = [s for s in child_seeds if s != lead_seed]
            print(f"  MISS  lead_n={lead_n}  lead_core={lead_core}  seed={seed_short}")
            if bad_cores:
                print(f"         children on wrong cores : {bad_cores}")
            if bad_seeds:
                print(f"         children with wrong seed: {[s[:12]+'…' for s in bad_seeds if s]}")

        # Instead of just listing bad cores, show the full picture
        if not (core_ok and seed_ok):
            misses += 1
            print(f"  MISS  lead_n={lead_n}  expected_core={lead_core}  seed={seed_short}")
            for i, c in enumerate(children):
                status = "✓" if c["core"] == lead_core else "✗"
                print(f"         child[{i}]  {status}  core={c['core']}  expected={lead_core}")

    # ── Leak check ─────────────────────────────────────────────────────────────
    snapshot = conductor.snapshot()
    leaked   = len(snapshot)
    if leaked:
        print(f"\n  [!] Conductor still holds {leaked} unreleased seed(s):")
        for seed_prefix, info in snapshot.items():
            print(f"      seed={seed_prefix}…  core={info['core']}  pending={info['pending']}")
    else:
        print(f"\n  ✓  Conductor snapshot clean — all seeds released.")

    # ── Summary ────────────────────────────────────────────────────────────────
    resolved = len(results)

    print(f"""
  ┌──────────────────────────────────────────────┐
  │  Hash Conductor Summary                      │
  ├──────────────────────────────────────────────┤
  │  Leads resolved   : {resolved:>2} / {N_LEADS:<2}                  │
  │  Leads failed     : {failed:>2}                       │
  │  Children / lead  : {CHILDREN_PER_LEAD:>2}                       │
  │  Seed clashes  : {"✓ NONE" if not seed_clashes else f"✗ {seed_clashes}":.<28}│
  │  Domain misses : {"✓ NONE" if not misses else f"✗ {misses} LEAD(S) BROKEN":.<28}│
  │  Leaked seeds  : {"✓ NONE" if not leaked else f"✗ {leaked} SEED(S) LEAKED":.<28}│
  └──────────────────────────────────────────────┘""")

    if not misses and not leaked and not seed_clashes:
        print("\n  ✓ Hash conductor held — all chains anchored to their domain.\n")
    else:
        print("\n  ✗ Conductor contract broken — see report above.\n")
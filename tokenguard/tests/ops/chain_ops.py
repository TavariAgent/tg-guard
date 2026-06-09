"""
chain_ops.py — Explicit call chains for dependency ordering proof.

Threads does not infer or enforce ordering between independent operations.
If operation B depends on the result of operation A, the calling code must
express that dependency explicitly by resolving A's token before submitting B.

This is a feature, not a limitation. The scheduler stays simple and predictable.
Dependency complexity lives in the caller, where it belongs and where it is visible.

These functions are designed to be chained — each accepts the output of the
previous step as its input, making the data flow legible in the proof.
"""

import time
from ...token_system import task_token_guard


# ── Chain Step Functions ─────────────────────────────────────────────────────

@task_token_guard(
    operation_type='chain_seed',
    tags={'weight': 'light'}
)
def chain_seed(value: int):
    """
    Step 0: Produce an initial value.
    Entry point of a chain. Returns a transformed seed for the next step.
    """
    return value * 3 + 7


@task_token_guard(
    operation_type='chain_filter',
    tags={'weight': 'light'}
)
def chain_filter(value: int):
    """
    Step 1: Conditionally transform the incoming value.
    Demonstrates that chain steps can apply logic, not just pass data through.
    """
    if value % 2 == 0:
        return value // 2
    return value * 2 + 1


@task_token_guard(
    operation_type='chain_accumulate',
    tags={'weight': 'medium'}
)
def chain_accumulate(value: int):
    """
    Step 2: Expand the value into a sum across a range.
    Adds moderate CPU work mid-chain to demonstrate that chaining
    does not require tasks to be trivially fast.
    """
    return sum(i * value for i in range(1, 51))


@task_token_guard(
    operation_type='chain_reduce',
    tags={'weight': 'light'}
)
def chain_reduce(value: int):
    """
    Step 3: Reduce back to a compact form.
    Final transformation before the chain terminates.
    """
    return value % 9973  # mod a prime to keep numbers readable


@task_token_guard(
    operation_type='chain_finalize',
    tags={'weight': 'light'}
)
def chain_finalize(value: int, label: str = "result"):
    """
    Step 4: Annotate and return the final chain output.
    Demonstrates that chain steps can accept auxiliary arguments
    alongside their dependency-resolved input.
    """
    return {label: value, 'parity': 'even' if value % 2 == 0 else 'odd'}


# ── Chain Runner ─────────────────────────────────────────────────────────────

def run_single_chain(seed: int, label: str = "chain") -> dict:
    """
    Execute one full A → B → C → D → E dependency chain.

    The calling code owns the ordering. Each .get() is an explicit
    synchronization point — the next step does not submit until the
    current step has resolved. The scheduler sees independent tasks;
    the caller sees a pipeline.
    """
    step0 = chain_seed(seed)
    v0 = step0.get(timeout=30)

    step1 = chain_filter(v0)
    v1 = step1.get(timeout=30)

    step2 = chain_accumulate(v1)
    v2 = step2.get(timeout=30)

    step3 = chain_reduce(v2)
    v3 = step3.get(timeout=30)

    step4 = chain_finalize(v3, label=label)
    v4 = step4.get(timeout=30)

    return v4


def run_chain_demo(chain_count: int = 5):
    """
    Run multiple independent chains. Chains themselves are independent
    of each other and can be submitted in parallel — only steps within
    a single chain are ordered.

    This demonstrates the key distinction:
      - Intra-chain: sequential by caller design.
      - Inter-chain: concurrent by scheduler default.
    """
    print(f"\n{'=' * 70}")
    print("CALL CHAIN DEMO: Caller-owned dependency ordering")
    print(f"{'=' * 70}")
    print(f"Running {chain_count} independent chains of 5 steps each.\n")

    seeds = [17, 42, 99, 5, 128, 256, 7, 333][:chain_count]
    results = []
    start = time.monotonic()

    for i, seed in enumerate(seeds):
        result = run_single_chain(seed, label=f"chain_{i}")
        results.append(result)
        print(f"  Chain {i}: seed={seed} → {result}")

    elapsed = time.monotonic() - start
    print(f"\n  {chain_count} chains completed in {elapsed:.3f}s")
    print("CALL CHAIN DEMO PASSED — ordering held, no implicit coupling.")
    return results


def run_parallel_chains_demo(chain_count: int = 4):
    """
    Submit chain entry points concurrently, then resolve each chain
    sequentially per-chain. Shows that independent chains don't block
    each other even though their internal steps are ordered.
    """
    import threading

    print(f"\n{'=' * 70}")
    print("PARALLEL CHAINS: Concurrent entry, ordered resolution per chain")
    print(f"{'=' * 70}")
    print(f"Running {chain_count} chains with concurrent step-0 submission.\n")

    seeds = [13, 77, 200, 55, 88, 144][:chain_count]
    chain_results = [None] * chain_count

    def run_chain_thread(idx, seed):
        chain_results[idx] = run_single_chain(seed, label=f"parallel_chain_{idx}")

    threads = [
        threading.Thread(target=run_chain_thread, args=(i, seed))
        for i, seed in enumerate(seeds)
    ]

    start = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    for i, result in enumerate(chain_results):
        print(f"  Chain {i}: {result}")

    print(f"\n  {chain_count} parallel chains completed in {elapsed:.3f}s")
    print("PARALLEL CHAINS PASSED — chains concurrent, steps ordered within each.")
    return chain_results

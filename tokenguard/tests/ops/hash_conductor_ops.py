# -*- coding: utf-8 -*-
# demo/hash_conductor_ops.py
"""
Operations used exclusively by the hash conductor test.

conductor_lead_op
    Decorated with external_calls so _put_routing_block charges it with a
    fresh seed domain.  During execution it spawns CHILDREN_PER_LEAD child
    tokens, blocks on each one synchronously, and returns their routing info
    so the test can verify all tokens landed on the same core.

conductor_child_op
    Lightweight child.  Carries no external_calls — it should inherit the
    lead's conductor_seed via the stamp in task_token_guard and be routed
    to the lead's pinned core by register_child().
"""

import time
from typing import Any

from ...unhashable_checker import HashPolicy, DigestPolicy
from ...token_system import task_token_guard

CHILDREN_PER_LEAD: int   = 4
CHILD_SLEEP_SECS:  float = 0.15


@task_token_guard(
    operation_type="conductor_child",
    tags={"weight": "medium"},
)
def conductor_child_op(n: int) -> int:
    """Child token — should land on the same core as its lead."""
    time.sleep(CHILD_SLEEP_SECS)
    return n * n


@task_token_guard(
    operation_type="conductor_lead",
    tags={"weight": "medium",
          "hash_policy": HashPolicy.FAST,
          "digest_policy": DigestPolicy.FAST,
          "external_calls": ["conductor_child"]},
)
def conductor_lead_op(lead_n: int) -> list[Any]:
    """Spawn child tokens and return them immediately.

    The conductor seed is stamped on each child during the list comprehension
    — that is all the lead needs to do.  Blocking here on token.get() would
    occupy an executor thread while waiting for children that also need
    executor threads, creating unnecessary pressure on the thread pool.

    The test resolves children externally in a second phase after the lead
    completes.
    """
    return [
        conductor_child_op(lead_n * 100 + i)
        for i in range(CHILDREN_PER_LEAD)
    ]
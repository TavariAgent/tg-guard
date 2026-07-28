# -*- coding: utf-8 -*-
# demo/cache_storm_ops.py
"""
Operations used exclusively by the cache storm test.

A single decorated function covers both roles:
  - Anchor  : first call for (op_type, n) pins the key to a core and holds it
              inflight for ANCHOR_HOLD_SECS via a sleep.
  - Storm   : subsequent calls with the same n share the same (op_type, args)
              key and must be redirected to the already-pinned core by the
              sticky registry.

Using one function for both roles keeps the sticky key identical between the
anchor and every storm token — which is the exact condition we want to stress.
"""

import time

from ...token_system import task_token_guard

# How long each anchor occupies its core before completing.
# Must be long enough for all storm waves to fire before the anchor finishes.
ANCHOR_HOLD_SECS: float = 1.5


@task_token_guard(operation_type='storm_anchor', tags={'weight': 'medium', 'sticky_anchor': 'storm_token'})
def storm_anchor_op(n: int) -> int:
    """
    Slow stub used as both the anchor and the storm payload.

    The same function, the same operation_type, the same args →
    the same (op_type, frozen_args) key in the sticky registry.
    Return value is n² so results are trivially verifiable.
    """
    time.sleep(ANCHOR_HOLD_SECS)
    return n * n
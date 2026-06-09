# -*- coding: utf-8 -*-
# sticky_token.py
"""
Sticky token registry: pins inflight (operation, frozen-args) keys to cores.

When the first token for a given (op_name, args) key is placed on a core, an
OperationMarker records that core.  Any token that arrives later with the same
key is forced to the same core — keeping all mutations within a single mailbox
domain and preventing cross-domain data races and spurious cache misses.

The marker is removed once the token completes (success or failure), allowing
the next token for that key to be freely routed again.
"""

import threading
from typing import Any, Dict, Optional, Tuple

from .unhashable_checker import make_hashable, safe_args_key
from .tg_print import tg_print


# Argument freezing
def freeze(val: Any) -> Any:
    """Recursively convert a value into a hashable representation.

    Dicts are sorted by key so that argument order differences do not produce
    different keys for semantically identical inputs.
    """
    if isinstance(val, dict):
        return tuple(sorted((k, freeze(v)) for k, v in val.items()))
    if isinstance(val, (list, tuple)):
        return tuple(freeze(v) for v in val)
    if isinstance(val, set):
        return frozenset(freeze(v) for v in val)
    return make_hashable(val)


# OperationMarker
class OperationMarker:
    """Lightweight record of the core owning a specific (op, args) pair."""

    __slots__ = ("core_id",)

    def __init__(self, core_id: int):
        self.core_id = core_id


# StickyTokenRegistry
_InflightKey = Tuple[str, Any]


class StickyTokenRegistry:
    """Thread-safe registry that pins (op_name, frozen-args) keys to cores.

    Usage inside the queue:

        At put() time — returns the core to actually use:
        core_id = registry.mark(op_name, token.args, candidate_core_id)

        At execution-complete time — releases the pin:
        registry.unmark(op_name, token.args)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._markers: Dict[_InflightKey, OperationMarker] = {}

    # Public API
    def mark(self, op_name: str, args: tuple[Any, ...], core_id: int) -> int:
        """Record the core for (op_name, frozen-args) and return the core to use.

        If no marker exists yet, one is created for *core_id* and *core_id* is
        returned (normal first-arrival path).

        If a marker already exists (a second token arrived before the first
        completed), the *existing* core_id is returned — the caller must route
        to that core to honour the sticky contract.
        """
        key = (op_name, safe_args_key(args))
        with self._lock:
            if key not in self._markers:
                self._markers[key] = OperationMarker(core_id)
                tg_print(
                    "sticky",
                    f"Marked    op={op_name}  core={core_id}  key_len={len(self._markers)}",
                    level="dispatch",
                )
                return core_id

            existing_core = self._markers[key].core_id
            if existing_core != core_id:
                tg_print(
                    "sticky",
                    f"Redirected  op={op_name}  "
                    f"candidate_core={core_id} -> pinned_core={existing_core}",
                    level="dispatch",
                )
            return existing_core

    def unmark(self, op_name: str, args: tuple) -> None:
        """Remove the inflight marker for (op_name, frozen-args)."""
        key = self._make_key(op_name, args)
        with self._lock:
            if self._markers.pop(key, None) is not None:
                tg_print(
                    "sticky",
                    f"Unmarked  op={op_name}  remaining={len(self._markers)}",
                    level="dispatch",
                )

    def get_pinned_core(self, op_name: str, args: tuple) -> Optional[int]:
        """Return the pinned core for a key, or None if not yet inflight."""
        key = self._make_key(op_name, args)
        with self._lock:
            marker = self._markers.get(key)
            return marker.core_id if marker else None

    def is_inflight(self, op_name: str, args: tuple) -> bool:
        """Return True if a marker exists for this (op_name, args) key."""
        key = self._make_key(op_name, args)
        with self._lock:
            return key in self._markers

    def snapshot(self) -> Dict[str, int]:
        """Return a {key_repr: core_id} snapshot for observability."""
        with self._lock:
            return {str(k): m.core_id for k, m in self._markers.items()}

    # Internal helpers
    @staticmethod
    def _make_key(op_name: str, args: tuple) -> _InflightKey:
        # Guard: skip freeze() entirely on empty-args path (conductor unmark,
        # sticky-only tokens). freeze(()) always returns () — this avoids
        # the recursive call on the majority path through on_complete/unmark.
        return op_name, freeze(args) if args else ()


# Module-level singleton
sticky_registry = StickyTokenRegistry()

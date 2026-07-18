# -*- coding: utf-8 -*-
# hash_conductor.py
"""
Hash Conductor — seed-based core domain anchoring for token call chains.

                          ┌──────────────────────────────────────┐
  Problem                 │  A lead token dispatches a chain of  │
                          │  child tokens.  Under normal load    │
                          │  balancing those children scatter    │
                          │  across cores, causing cross-domain  │
                          │  cache misses even when all the work │
                          │  is logically related.               │
                          └──────────────────────────────────────┘
                                           │
                          ┌──────────────────────────────────────┐
  Solution                │  When a lead token is charged, a     │
                          │  digest is derived from its          │
                          │  token_id and external_calls using   │
                          │  the configured DigestPolicy.That    │
                          │  seed is pinned to whichever core    │
                          │  the lead lands on. Any child token  │
                          │  created during the lead's execution │
                          │  inherits the seed and is routed to  │
                          │  the same core domain automatically. │
                          └──────────────────────────────────────┘

Seed uniqueness
    Default (DigestPolicy.FULL):
        Seed = SHA-256( token_id + ":" + freeze(external_calls) ) — 64-char hex
    DigestPolicy.SHORT:  SHA-256 truncated to 16 chars  (64-bit space)
    DigestPolicy.FAST:   BLAKE2s 8-byte digest → 16-char hex (64-bit space)
    DigestPolicy.MINIMAL: SHA-256 truncated to 8 chars  (32-bit space)

    Collision semantics for MINIMAL
        Collisions merge two logical domain chains into a shared mailbox
        cluster.  The least-loaded mechanism compensates, but heavy tasks
        may fall back from their primary core under saturation, inertly
        reducing performance without data corruption.

Parallel seeds
    Each lead call generates its own seed independently.  No special
    handling needed — parallel leads never share a seed.

Domain lifetime
    The seed domain is alive with charge() until the last token that
    carries the seed (lead or child) calls on_complete().  pending
    count starts at 1 (the lead itself) and increments for each
    registered child.  Release fires when the count reaches zero.

Thread model
    Child tokens are created inside the executor thread where the lead
    function runs.  The active seed is stored in a threading.local so
    it is visible to the task_token_guard decorator at token-creation
    time, before the submission crosses back to the event loop.

Integration points (see inline notes)
    1. put()              — charge leads; register children
    2. _execute_token()   — activate seed in executor thread wrapper
    3. task_token_guard() — stamp active seed onto new tokens
"""
from __future__ import annotations
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .token_system import TaskToken

import hashlib
import threading
from typing import Dict, Optional

from .sticky_token import freeze, sticky_registry
from .unhashable_checker import DigestPolicy
from .tg_print import tg_print


# Thread-local active seed
# Set inside the executor thread just before the lead function runs.
# Read by task_token_guard to stamp the seed onto any token created
# during that execution.  Cleared after the function returns.
_active_seed: threading.local = threading.local()


def get_active_seed() -> Optional[str]:
    """Return the seed active in the current executor thread, or None."""
    return getattr(_active_seed, "value", None)


def _set_active_seed(seed: Optional[str]) -> None:
    _active_seed.value = seed


# HashConductor
class HashConductor:
    """Proactive seed-based core domain anchor for token call chains.

    Usage inside the queue:

        # At put() time — lead token (has external_calls):
        core_id = conductor.charge(token, candidate_core)

        # At put() time — child token (active seed present):
        core_id = conductor.register_child(token, candidate_core)

        # At execution time — wrap the callable:
        def _wrapped():
            conductor.activate(token)
            try:
                return token.func(*token.args, **token.kwargs)
            finally:
                conductor.deactivate()

        # At completion time:
        conductor.on_complete(token)

    Usage inside task_token_guard (token creation):

        seed = get_active_seed()
        if seed:
            token.metadata.tags["conductor_seed"] = seed
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cores: Dict[str, int] = {}
        self._pending: Dict[str, int] = {}
        self._grace_cores: Dict[str, int] = {}
        self._GRACE_MAX = 256

    # Public API
    @staticmethod
    def generate_seed(token: "TaskToken[Any]", policy: DigestPolicy = DigestPolicy.FULL) -> str:
        """Derive a unique domain seed from a lead token.

        Policy controls digest algorithm and output length:

            FULL    — SHA-256 full 64-char hex (default).
            SHORT   — SHA-256 truncated to 16 chars (64-bit space).
            FAST    — BLAKE2s 8-byte digest → 16-char hex (64-bit space,
                      lower compute cost than SHA-256).
            MINIMAL — SHA-256 truncated to 8 chars (32-bit space).
                      Collisions are benign but shift load distribution —
                      see module docstring for full collision semantics.
        """
        token_id = getattr(token, "token_id", id(token))
        external_calls = (
                getattr(token.metadata, "external_calls", None)
                or token.metadata.tags.get("external_calls", "")
        )
        raw = f"{token_id}:{freeze(external_calls)}".encode()

        if policy == DigestPolicy.FAST:
            # BLAKE2s — fastest, 64-bit collision space
            return hashlib.blake2s(raw, digest_size=8).hexdigest()
        elif policy == DigestPolicy.SHORT:
            # SHA-256 truncated — 64-bit space, familiar algorithm
            return hashlib.sha256(raw).hexdigest()[:16]
        elif policy == DigestPolicy.MINIMAL:
            # SHA-256 truncated — 32-bit space, lowest overhead
            return hashlib.sha256(raw).hexdigest()[:8]
        else:
            # FULL — SHA-256 complete digest (default)
            return hashlib.sha256(raw).hexdigest()

    def charge(self, token: "TaskToken[Any]", candidate_core: int) -> int:
        """Assign a seed domain to a lead token and pin it to a core.

        Reads digest_policy from the token's tags to select the seed
        algorithm. Generates a fresh seed, stamps it onto the token's
        metadata, pins the seed to candidate_core via the sticky registry,
        and opens the pending count at 1 (the lead itself).

        Returns the actual pinned core (always candidate_core on first
        call; the sticky registry handles collisions if the same seed
        somehow appeared earlier).
        """
        # Resolve digest policy from token tag
        _dp_raw = token.metadata.tags.get("digest_policy", DigestPolicy.FULL)
        if isinstance(_dp_raw, str):
            try:
                digest_policy = DigestPolicy(_dp_raw)
            except ValueError:
                tg_print(
                    "conductor",
                    f"Unknown digest_policy '{_dp_raw}' on token="
                    f"{getattr(token, 'token_id', '?')} — defaulting to FULL",
                    level="warn",
                )
                digest_policy = DigestPolicy.FULL
        else:
            digest_policy = _dp_raw

        seed = self.generate_seed(token, digest_policy)
        token.metadata.tags["conductor_seed"] = seed

        # Pin via the sticky registry using the seed as the op key.
        # Empty args — the seed itself is the unique identifier.
        core_id = sticky_registry.mark(seed, (), candidate_core)

        with self._lock:
            self._cores[seed] = core_id
            self._pending[seed] = 1  # lead token counts toward the domain

        tg_print(
            "conductor",
            f"Charged   token={getattr(token, 'token_id', '?')}  "
            f"core={core_id}  seed={seed[:12]}…  digest={digest_policy.value}",
            level="dispatch",
        )
        return core_id

    def register_child(self, token: "TaskToken[Any]", candidate_core: int) -> int:
        """Stamp the active seed onto a child token and route it to the domain.

        Called from put() when get_active_seed() returns a value during
        a child token's submission.  Increments the pending count so the
        domain stays alive until this child also completes.

        Returns the pinned core for the active seed, or candidate_core
        if the seed is no longer tracked (e.g. lead already released).
        """
        seed = token.metadata.tags.get("conductor_seed") or get_active_seed()
        if not seed:
            return candidate_core

        with self._lock:
            if seed in self._cores:
                core_id = self._cores[seed]
            elif seed in self._grace_cores:
                core_id = self._grace_cores[seed]
                tg_print(
                    "conductor",
                    f"Grace hit  token={getattr(token, 'token_id', '?')}  "
                    f"seed={seed[:12]}…  core={core_id}",
                    level="dispatch",
                )
            else:
                tg_print(
                    "conductor",
                    f"Child fallthrough  token={getattr(token, 'token_id', '?')}  "
                    f"seed={seed[:12]}…  DOMAIN GONE — returning candidate_core={candidate_core}",
                    level="warn",
                )
                return candidate_core

        tg_print(
            "conductor",
            f"Registered child  token={getattr(token, 'token_id', '?')}  "
            f"core={core_id}  seed={seed[:12]}…  "
            f"pending={self._pending.get(seed, '?')}",
            level="dispatch",
        )
        return core_id

    def pre_register(self, seed: str) -> None:
        """Increment pending count at child token creation time.

        Called from task_token_guard while still in the executor thread,
        before the child crosses to the event loop for put(). Keeps the
        seed domain alive even if the lead completes before the child
        reaches register_child() in put().
        """
        with self._lock:
            if seed in self._pending:
                self._pending[seed] += 1

    @staticmethod
    def activate(token: "TaskToken[Any]") -> Optional[str]:
        """Set the active seed in the executor thread before the lead runs.

        Must be called from *inside* the function passed to run_in_executor
        so the thread-local is set in the correct thread.

        Returns the seed, or None if this token carries no seed.
        """
        seed = token.metadata.tags.get("conductor_seed")
        _set_active_seed(seed)
        return seed

    @staticmethod
    def deactivate() -> None:
        """Clear the active seed after the lead function returns."""
        _set_active_seed(None)

    def on_complete(self, token: "TaskToken[Any]") -> None:
        """Decrement the pending count for a token's seed.

        When the count reaches zero (lead + all children done) the seed
        domain is released from both the conductor and the sticky registry.
        """
        seed = token.metadata.tags.get("conductor_seed")
        if not seed:
            return

        assert isinstance(seed, str), f"conductor_seed tag must be str, got {type(seed)}"

        release = False
        released_core = None
        with self._lock:
            if seed in self._pending:
                self._pending[seed] -= 1
                if self._pending[seed] <= 0:
                    release = True
                    released_core = self._cores.pop(seed, None)
                    self._pending.pop(seed, None)
                    if released_core is not None:
                        self._grace_cores[seed] = released_core
                        if len(self._grace_cores) > self._GRACE_MAX:
                            self._grace_cores.pop(next(iter(self._grace_cores)))

        if release:
            sticky_registry.unmark(seed, ())
            tg_print("conductor", f"Released  seed={seed[:12]}…", level="dispatch")

    def snapshot(self) -> Dict[str, dict[str, int | str]]:
        """Return a {seed_prefix: {core, pending}} snapshot for observability.

        seed[:12] is safe for all DigestPolicy values — MINIMAL seeds are
        8 chars and Python's slice never raises on out-of-bounds.
        """
        with self._lock:
            return {
                seed[:12]: {"core": core, "pending": self._pending.get(seed, 0)}
                for seed, core in self._cores.items()
            }


# Module-level singleton
conductor = HashConductor()

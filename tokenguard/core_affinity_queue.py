# -*- coding: utf-8 -*-
# core_affinity_queue.py
"""
Core-affinity policy and reporting for token-managed routing.

This module defines task-weight categories, per-weight core preference rules,
and lightweight affinity metrics used to describe how work is distributed
across physical cores.

It does not own mailbox routing or execution. Actual token placement is
performed by the pinned worker queue layer.
"""

import threading
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional
from .tg_print import tg_print


class TaskWeight(Enum):
    """Routing weight classes used by the affinity policy."""
    HEAVY = "heavy"  # High difficulty work gets Core 1+
    MEDIUM = "medium"  # Balanced work gets Core 2+
    LIGHT = "light"  # Simple work gets Core 3+


@dataclass
class CorePreference:
    """Allowed-core set and preferred starting core for one weight class."""
    allowed_cores: List[int]  # Cores this weight can use
    preferred_core: int  # First choice


class CoreAffinityPolicy:
    """Builds and exposes per-weight core eligibility rules.

    The policy derives allowed-core chains from detected physical core count
    and preserves weight isolation rules where possible.
    """

    def __init__(self, num_cores: int):
        self.num_cores = num_cores
        self._build_preferences()

    def _build_preferences(self):
        """Construct per-weight core preference chains from available core count."""

        # Heavy can use ALL cores, prefers Core 1
        heavy_cores = list(range(1, self.num_cores + 1))

        # Medium starts at Core 2 (NEVER Core 1)
        medium_cores = list(range(2, self.num_cores + 1)) if self.num_cores >= 2 else [1]

        # Light starts at Core 3 (NEVER Cores 1-2)
        light_cores = list(range(3, self.num_cores + 1)) if self.num_cores >= 3 else medium_cores

        self.preferences = {
            TaskWeight.HEAVY: CorePreference(allowed_cores=heavy_cores, preferred_core=heavy_cores[0]),
            TaskWeight.MEDIUM: CorePreference(allowed_cores=medium_cores, preferred_core=medium_cores[0]),
            TaskWeight.LIGHT: CorePreference(allowed_cores=light_cores, preferred_core=light_cores[0]),
        }

        tg_print('affinity', f'Policy built for {self.num_cores} cores')
        tg_print('affinity', f'Heavy:  {heavy_cores}  preferred={heavy_cores[0]}')
        tg_print('affinity', f'Medium: {medium_cores}  preferred={medium_cores[0]}')
        tg_print('affinity', f'Light:  {light_cores}  preferred={light_cores[0]}')

    def get_preference_chain(self, weight: TaskWeight) -> List[int]:
        """Return allowed cores for the given weight in preference order."""
        return self.preferences[weight].allowed_cores

    def can_use_core(self, weight: TaskWeight, core_id: int) -> bool:
        """Return whether the given core is eligible for the given weight."""
        return core_id in self.preferences[weight].allowed_cores


class CoreAffinityQueue:
    """Policy and reporting layer for weight-based core affinity.

    This class classifies tokens, exposes the allowed core chain for each
    weight class, and records routing outcomes reported by the execution
    queue layer.

    It does not place tokens into mailboxes directly.
    """

    def __init__(self, topology, workers_per_core: Optional[int] = None):
        self.topology = topology
        self.workers_per_core = workers_per_core
        self.num_cores = topology.physical_cores

        # Build affinity policy
        self.policy = CoreAffinityPolicy(self.num_cores)

        # Simple counters (no CoreQueue objects!)
        self._affinity_counts = {
            core_id: {'heavy': 0, 'medium': 0, 'light': 0}
            for core_id in range(1, self.num_cores + 1)
        }

        # Metrics
        self.total_routed = 0
        self.routing_failures = 0
        self._routing_lock = threading.Lock()

    def get_valid_cores_for_weight(self, weight: TaskWeight) -> List[int]:
        """Return the allowed core chain for the given weight."""
        return self.policy.get_preference_chain(weight)

    def record_task_routed(self, core_id: int, weight: TaskWeight):
        """Record one completed routing decision reported by the queue layer."""
        with self._routing_lock:
            self.total_routed += 1
            self._affinity_counts[core_id][weight.value] += 1

    def get_affinity_report(self) -> dict:
        """Return per-core weight distribution percentages and totals."""
        return {
            f'core_{core_id}': (
                {
                    'heavy': (counts['heavy'] / total) * 100,
                    'medium': (counts['medium'] / total) * 100,
                    'light': (counts['light'] / total) * 100,
                    'total_tasks': total,
                }
                if (total := sum(counts.values())) > 0 else
                {'heavy': 0.0, 'medium': 0.0, 'light': 0.0, 'total_tasks': 0}
            )
            for core_id, counts in self._affinity_counts.items()
        }

    def get_stats(self) -> dict:
        """Return a composite snapshot of affinity configuration and routing totals."""
        return {
            'num_cores': self.num_cores,
            'workers_per_core': self.workers_per_core,
            'total_routed': self.total_routed,
            'routing_failures': self.routing_failures,
            'affinity_distribution': self.get_affinity_report(),
        }

    def print_affinity_report(self):
        """Print a human-readable per-core affinity distribution report."""
        print()
        print("=" * 70)
        print("CORE AFFINITY REPORT")
        print("=" * 70)

        report = self.get_affinity_report()

        for core_id in range(1, self.num_cores + 1):
            if stats := report.get(f'core_{core_id}'):
                print(f"\nCore {core_id}:")
                print(f"  Heavy:  {stats['heavy']:>5.1f}%")
                print(f"  Medium: {stats['medium']:>5.1f}%")
                print(f"  Light:  {stats['light']:>5.1f}%")
                print(f"  Total:  {stats['total_tasks']} tasks")

        print("=" * 70)

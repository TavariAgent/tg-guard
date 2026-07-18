# -*- coding: utf-8 -*-
# topology_detector.py
"""
CPU Topology Detection

Discovers the physical structure of the CPU
- Physical vs logical cores
- SMT/Hyperthreading configuration
- Core affinity availability
- NUMA topology (advanced)
"""

import psutil
import os
from dataclasses import dataclass
from typing import List, Dict, Set


@dataclass
class CPUTopology:
    """
    Complete CPU topology information.

    Attributes:
        physical_cores: Number of physical CPU cores
        logical_cores: Number of logical cores (with SMT)
        smt_enabled: Whether SMT/Hyperthreading is active
        smt_ratio: Logical cores per physical core
        available_cores: Set of core IDs available to this process
        core_mapping: Maps logical cores to physical cores
    """
    physical_cores: int
    logical_cores: int
    smt_enabled: bool
    smt_ratio: float
    available_cores: Set[int]
    core_mapping: Dict[int, int]  # logical_id -> physical_id


class TopologyDetector:
    """
    Detects and analyzes CPU topology.

    Provides detailed information about the CPU structure
    to enable intelligent worker allocation and core affinity.
    """

    @staticmethod
    def detect() -> CPUTopology:
        """
        Detect CPU topology.

        Returns complete topology information including
        physical/logical core counts and mappings.
        """
        physical = psutil.cpu_count(logical=False) or 1
        logical = psutil.cpu_count(logical=True) or 1

        # Detect SMT
        smt_enabled = logical > physical
        smt_ratio = logical / physical if physical > 0 else 1.0

        # Get available cores for this process
        try:
            available = set(os.sched_getaffinity(0))  # type: ignore[attr-defined]
        except AttributeError:
            # Windows doesn't have sched_getaffinity
            available = set(range(logical))

        # Build core mapping
        # Assumption: logical cores 0,1 map to physical 0, etc.
        # This is typical but not guaranteed - more complex detection
        # would require reading /proc/cpuinfo or platform-specific APIs
        core_mapping = {}
        for logical_id in range(logical):
            physical_id = logical_id // int(smt_ratio)
            core_mapping[logical_id] = physical_id

        return CPUTopology(
            physical_cores=physical,
            logical_cores=logical,
            smt_enabled=smt_enabled,
            smt_ratio=smt_ratio,
            available_cores=available,
            core_mapping=core_mapping
        )

    @staticmethod
    def get_physical_core_groups(topology: CPUTopology) -> Dict[int, List[int]]:
        """
        Group logical cores by their physical core.

        Returns:
            Dict mapping physical_core_id -> [logical_core_ids]

        Example:
            {
                0: [0, 1], # Physical core 0 has logical cores 0,1
                1: [2, 3], # Physical core 1 has logical cores 2,3
                ...
            }
        """
        groups: Dict[int, List[int]] = {}

        for logical_id, physical_id in topology.core_mapping.items():
            if physical_id not in groups:
                groups[physical_id] = []
            groups[physical_id].append(logical_id)

        return groups

    @staticmethod
    def recommend_worker_counts(topology: CPUTopology) -> Dict[str, int]:
        physical = topology.physical_cores
        smt_mult = topology.smt_ratio if topology.smt_enabled else 1.0

        return {
            'light': int(physical * 4 * smt_mult),  # 8 cores × 4 × 2 = 64
            'medium': int(physical * 3 * smt_mult),  # 8 cores × 3 × 2 = 48
            'heavy': int(physical * 2 * smt_mult),  # 8 cores × 2 × 2 = 32
        }

    @staticmethod
    def print_topology(topology: CPUTopology) -> None:
        """Print human-readable topology information."""
        print("CPU Topology Detected:")
        print(f"  Physical cores: {topology.physical_cores}")
        print(f"  Logical cores: {topology.logical_cores}")
        print(f"  SMT enabled: {topology.smt_enabled}")
        print(f"  SMT ratio: {topology.smt_ratio:.1f}x")
        print(f"  Available cores: {len(topology.available_cores)}")

        recommendations = TopologyDetector.recommend_worker_counts(topology)
        print(f"\nRecommended worker counts:")
        print(f"  Light workload (I/O bound): {recommendations['light']}")
        print(f"  Medium workload (mixed): {recommendations['medium']}")
        print(f"  Heavy workload (CPU bound): {recommendations['heavy']}")


# Quick test
if __name__ == '__main__':
    detector = TopologyDetector()
    topology = detector.detect()
    detector.print_topology(topology)

    print("\nPhysical core groupings:")
    groups = detector.get_physical_core_groups(topology)
    for physical_id, logical_ids in sorted(groups.items()):
        print(f"  Physical core {physical_id}: logical cores {logical_ids}")

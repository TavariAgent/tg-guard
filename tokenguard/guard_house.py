# -*- coding: utf-8 -*-
# guard_house.py
"""
Guard House - method reputation, health tracking, and optional safety blocking.

Tracks post-execution method outcomes, timing, and failure patterns in order
to classify method health and provide developer-facing diagnostics.

When auto-blocking is enabled, Guard House can also reject methods with
sustained extreme failure rates before execution.
"""
import time
import threading
from typing import Dict, Optional, List, Callable, Any, cast
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict
from .tg_print import tg_print


class MethodBlockedException(Exception):
    """Raised when a method is auto-blocked due to a high failure rate."""
    pass


class MethodHealth(Enum):
    """Health status classification for methods."""
    EXCELLENT = "excellent"  # >95% completion
    HEALTHY = "healthy"  # 80-95% completion
    AT_RISK = "at_risk"  # 50-80% completion
    PROBLEMATIC = "problematic"  # <50% completion
    UNKNOWN = "unknown"  # Not enough data


@dataclass
class MethodReputation:
    """Aggregated execution, timing, and health history for one method."""
    method_name: str
    operation_type: Optional[str] = None

    # Execution counts
    total_attempts: int = 0
    successful_completions: int = 0
    failed_executions: int = 0

    # Timing statistics
    total_execution_time: float = 0.0
    min_execution_time: float = float('inf')
    max_execution_time: float = 0.0

    # Failure patterns
    timeout_count: int = 0  # Tasks >60s
    error_count: int = 0  # Exceptions
    quick_failure_count: int = 0  # Tasks <10s that failed

    # Tracking
    health_status: MethodHealth = MethodHealth.UNKNOWN
    first_seen_at: float = 0.0
    last_execution_at: float = 0.0

    def get_completion_rate(self) -> float:
        """Calculate completion rate percentage."""
        if self.total_attempts == 0:
            return 0.0
        return (self.successful_completions / self.total_attempts) * 100

    def get_failure_rate(self) -> float:
        """Calculate failure rate percentage."""
        if self.total_attempts == 0:
            return 0.0
        return (self.failed_executions / self.total_attempts) * 100

    def get_avg_execution_time(self) -> float:
        """Calculate average execution time."""
        if self.total_attempts == 0:
            return 0.0
        return self.total_execution_time / self.total_attempts

    def update_health_status(self) -> MethodHealth:
        """
        Determine health status based on completion rate.

        Classification:
        - EXCELLENT: >95% completion
        - HEALTHY: 80-95% completion
        - AT_RISK: 50-80% completion
        - PROBLEMATIC: <50% completion
        - UNKNOWN: <5 attempts
        """
        # Need minimum data
        if self.total_attempts < 5:
            self.health_status = MethodHealth.UNKNOWN
            return self.health_status

        completion_rate = self.get_completion_rate()

        if completion_rate >= 95.0:
            self.health_status = MethodHealth.EXCELLENT
        elif completion_rate >= 80.0:
            self.health_status = MethodHealth.HEALTHY
        elif completion_rate >= 50.0:
            self.health_status = MethodHealth.AT_RISK
        else:
            self.health_status = MethodHealth.PROBLEMATIC

        return self.health_status

    def get_recommendation(self) -> str:
        """Return an actionable recommendation based on observed failure patterns."""
        if self.health_status == MethodHealth.EXCELLENT:
            return "Method performing excellently"

        if self.health_status == MethodHealth.HEALTHY:
            return "Method performing well"

        # Analyze failure patterns
        recommendations = []

        if self.timeout_count > self.total_attempts * 0.3:
            recommendations.append("High timeout rate - consider optimization or timeouts")

        if self.error_count > self.total_attempts * 0.3:
            recommendations.append("High error rate - check error handling")

        if self.quick_failure_count > 3:
            recommendations.append("Quick failures detected - check input validation")

        if not recommendations:
            recommendations.append("Review implementation for reliability")

        return " | ".join(recommendations)


class GuardHouse:
    """Tracks method reputation and optionally blocks persistently unsafe methods.

    Guard House records post-execution outcomes, classifies method health,
    exposes developer-facing diagnostics, and can enforce pre-execution
    blocking when a method exceeds the configured danger threshold.
    """

    def __init__(self, auto_block_dangerous: bool = True):
        """
         Initialize Guard House.

         Args:
             auto_block_dangerous: Whether methods with sustained extreme failure
                 rates should be blocked before future execution attempts.
         """
        # Configuration
        self.auto_block_dangerous = auto_block_dangerous

        # Reputation tracking
        self.method_reputations: Dict[str, MethodReputation] = {}
        self._lock = threading.RLock()

        # Statistics
        self.total_methods_tracked = 0
        self.total_executions_monitored = 0
        self.monitoring_start_time = time.time()

        # Blocked methods (if auto_block enabled)
        self.blocked_methods: set[str] = set()

        mode = "Active Protection" if auto_block_dangerous else "Passive Monitoring"
        tg_print('guard', f'{mode} initialized')
        tg_print('guard', 'Mode: Post-execution analysis')
        if auto_block_dangerous:
            tg_print('guard', 'Auto-blocking: ENABLED (>90% failure rate)')
        else:
            tg_print('guard', 'Auto-blocking: DISABLED (observation only)')

    def record_execution_result(
            self,
            method_name: str,
            success: bool,
            execution_time: float,
            operation_type: Optional[str] = None,
            failure_type: Optional[str] = None,
            complexity_score: Optional[float] = None
    ) -> None:
        """
        Record post-execution outcome data and update method reputation.

        This is the primary post-execution integration point used to maintain
        health classification, failure-pattern counts, timing statistics, and
        complexity baselines.
        """
        with self._lock:
            # Create reputation if first time seeing this method
            if method_name not in self.method_reputations:
                self.method_reputations[method_name] = MethodReputation(
                    method_name=method_name,
                    operation_type=operation_type,
                    first_seen_at=time.time()
                )
                self.total_methods_tracked += 1

            rep = self.method_reputations[method_name]

            # Update counts
            rep.total_attempts += 1
            self.total_executions_monitored += 1

            if success:
                rep.successful_completions += 1
            else:
                rep.failed_executions += 1

                # Track failure patterns
                if failure_type == 'timeout':
                    rep.timeout_count += 1
                elif failure_type == 'error':
                    rep.error_count += 1
                elif failure_type == 'quick_fail':
                    rep.quick_failure_count += 1

            # Update timing
            rep.total_execution_time += execution_time
            rep.last_execution_at = time.time()

            if execution_time < rep.min_execution_time:
                rep.min_execution_time = execution_time
            if execution_time > rep.max_execution_time:
                rep.max_execution_time = execution_time

            # AUTO-BLOCK CHECK (if enabled)
            if self.auto_block_dangerous:
                failure_rate = rep.get_failure_rate()

                # Block if >90% failure rate and at least 10 attempts
                if failure_rate > 90.0 and rep.total_attempts >= 10:
                    if method_name not in self.blocked_methods:
                        self.blocked_methods.add(method_name)
                        tg_print('guard', f'AUTO-BLOCKED: {method_name}', level='warn')
                        tg_print('guard', f'Failure rate: {failure_rate:.1f}% '
                                          f'({rep.failed_executions}/{rep.total_attempts})', level='warn')
                        tg_print('guard', 'This method will be rejected on future calls', level='warn')

    def check_method_allowed(self, func: Callable[..., Any]) -> None:
        """
        Enforce auto-blocking before execution when enabled.

        Raises:
            MethodBlockedException: If the method is currently blocked.
        """
        if not self.auto_block_dangerous:
            return  # Auto-blocking disabled

        method_name = func.__name__

        with self._lock:
            if method_name in self.blocked_methods:
                rep = self.method_reputations.get(method_name)
                if rep:
                    failure_rate = rep.get_failure_rate()
                    raise MethodBlockedException(
                        f"Method '{method_name}' is auto-blocked due to high failure rate "
                        f"({failure_rate:.1f}%, {rep.failed_executions}/{rep.total_attempts} attempts). "
                        f"Review method implementation before re-enabling."
                    )
                else:
                    # Shouldn't happen, but handle gracefully
                    raise MethodBlockedException(
                        f"Method '{method_name}' is auto-blocked."
                    )

    def get_methods_by_health(self, health: MethodHealth) -> List[MethodReputation]:
        """Get all methods with specified health status."""
        with self._lock:
            return [
                rep for rep in self.method_reputations.values()
                if rep.health_status == health
            ]

    def get_top_methods(self, limit: int = 10, by: str = 'attempts') -> List[MethodReputation]:
        """
        Get top methods by criteria.

        Args:
            limit: How many to return
            by: Sort by 'attempts', 'failures', 'completion_rate'
        """
        with self._lock:
            methods = list(self.method_reputations.values())

            if by == 'attempts':
                methods.sort(key=lambda r: r.total_attempts, reverse=True)
            elif by == 'failures':
                methods.sort(key=lambda r: r.failed_executions, reverse=True)
            elif by == 'completion_rate':
                methods.sort(key=lambda r: r.get_completion_rate())

            return methods[:limit]

    def get_reputation(self, method_name: str) -> Optional[MethodReputation]:
        """Get reputation for a specific method."""
        with self._lock:
            return self.method_reputations.get(method_name)

    def get_stats(self) -> dict[str, int | float | dict[str, int]]:
        """Return aggregate monitoring and health-distribution statistics."""
        with self._lock:
            health_counts: Dict[str, int] = defaultdict(int)
            for rep in self.method_reputations.values():
                health_counts[rep.health_status.value] += 1

            uptime = time.time() - self.monitoring_start_time

            return {
                'total_methods_tracked': self.total_methods_tracked,
                'total_executions_monitored': self.total_executions_monitored,
                'monitoring_uptime_seconds': uptime,
                'health_distribution': health_counts
            }

    def print_heatmap(self) -> None:
        """Print the main method-health dashboard grouped by health class."""
        # Collect all data first (inside lock)
        with self._lock:
            stats = self.get_stats()
            excellent = self.get_methods_by_health(MethodHealth.EXCELLENT)
            healthy = self.get_methods_by_health(MethodHealth.HEALTHY)
            at_risk = self.get_methods_by_health(MethodHealth.AT_RISK)
            problematic = self.get_methods_by_health(MethodHealth.PROBLEMATIC)

        # Print everything (outside lock)
        print()
        print("=" * 70)
        print("GUARD HOUSE - METHOD PERFORMANCE HEATMAP")
        print("=" * 70)
        print()

        uptime_hours = cast(float, stats['monitoring_uptime_seconds']) / 3600

        print(f"Monitoring Stats:")
        print(f"  Methods tracked: {stats['total_methods_tracked']}")
        print(f"  Executions monitored: {stats['total_executions_monitored']}")
        print(f"  Uptime: {uptime_hours:.2f} hours")
        print()

        # Excellent performers
        if excellent:
            print("🟢 EXCELLENT PERFORMERS (>95% success):")
            for rep in excellent[:5]:
                print(f"  ├─ {rep.method_name}")
                print(
                    f"  │  Rate: {rep.get_completion_rate():.1f}% ({rep.successful_completions}/{rep.total_attempts})")
                print(f"  │  Avg time: {rep.get_avg_execution_time():.2f}s")
                print(f"  │")
            if len(excellent) > 5:
                print(f"  └─ ... and {len(excellent) - 5} more")
            print()

        # Healthy methods
        if healthy:
            print("🟢 HEALTHY METHODS (80-95% success):")
            for rep in healthy[:5]:
                print(f"  ├─ {rep.method_name}")
                print(
                    f"  │  Rate: {rep.get_completion_rate():.1f}% ({rep.successful_completions}/{rep.total_attempts})")
                print(f"  │  Avg time: {rep.get_avg_execution_time():.2f}s")
                print(f"  │")
            if len(healthy) > 5:
                print(f"  └─ ... and {len(healthy) - 5} more")
            print()

        # At-risk methods
        if at_risk:
            print("🟡 AT RISK (50-80% success) - Needs Attention:")
            for rep in at_risk:
                print(f"  ├─ {rep.method_name}")
                print(
                    f"  │  Rate: {rep.get_completion_rate():.1f}% ({rep.successful_completions}/{rep.total_attempts})")
                print(f"  │  Timeouts: {rep.timeout_count}, Errors: {rep.error_count}")
                print(f"  │  Action: {rep.get_recommendation()}")
                print(f"  │")
            print()

        # Problematic methods
        if problematic:
            print("🔴 PROBLEMATIC (<50% success) - Requires Immediate Attention:")
            for rep in problematic:
                print(f"  ├─ {rep.method_name}")
                print(
                    f"  │  Rate: {rep.get_completion_rate():.1f}% ({rep.successful_completions}/{rep.total_attempts})")
                print(f"  │  Timeouts: {rep.timeout_count}, Errors: {rep.error_count}")
                print(f"  │  Max time: {rep.max_execution_time:.2f}s")
                print(f"  │  ⚠️  ACTION REQUIRED: {rep.get_recommendation()}")
                print(f"  │")
            print()

        print("=" * 70)

    def print_top_issues(self, limit: int = 5) -> None:
        """Print the highest-failure methods as a quick issue summary."""
        print()
        print("=" * 70)
        print("TOP PROBLEM METHODS")
        print("=" * 70)
        print()

        top_failures = self.get_top_methods(limit=limit, by='failures')

        for i, rep in enumerate(top_failures, 1):
            if rep.failed_executions == 0:
                continue

            print(f"{i}. {rep.method_name}")
            print(f"   Failures: {rep.failed_executions}/{rep.total_attempts} ({rep.get_failure_rate():.1f}%)")
            print(f"   Timeouts: {rep.timeout_count}, Errors: {rep.error_count}")
            print(f"   Recommendation: {rep.get_recommendation()}")
            print()

        print("=" * 70)

# -*- coding: utf-8 -*-
# storage_throttle.py
"""
Storage Speed Throttle Manager

Automatic I/O throttling based on storage speed tiers.

Philosophy: "Just tag your storage speed and forget about it!"

Speed Tiers:
- SLOW: 10 concurrent writes (HDD, network drives)
- MODERATE: 25 concurrent writes (SATA SSD)
- FAST: 50 concurrent writes (NVMe like MP700)
- INSANE: 70 concurrent writes (Optane, RAM disk)

Usage:
    @task_token_guard(operation_type='save_json', tags={'storage_speed': 'FAST'})
    def save_json(data):
        with open('file.json', 'w') as f:
            json.dump(data, f)
        # Automatic 50-concurrent throttle!
"""

import threading
import time
from typing import Dict, Callable, Any, Optional
from dataclasses import dataclass

from .tg_print import tg_print


# SPEED TIER CONFIGURATION
STORAGE_SPEEDS = {
    'SLOW': 10,  # HDD, network drives, slow storage
    'MODERATE': 25,  # SATA SSD, decent performance
    'FAST': 50,  # NVMe like MP700, high performance
    'INSANE': 70  # Optane, RAM disk, extreme performance
}


@dataclass
class ThrottleStats:
    """Statistics for a single throttle tier."""
    speed_tier: str
    max_concurrent: int
    current_active: int
    total_operations: int
    total_wait_time: float

    def avg_wait_time(self) -> float:
        """Calculate average wait time."""
        if self.total_operations == 0:
            return 0.0
        return self.total_wait_time / self.total_operations


class StorageThrottle:
    """
    Single storage throttle for a specific speed tier.

    Manages I/O concurrency for one storage speed level.
    """

    def __init__(self, speed_tier: str, max_concurrent: int):
        """
        Initialize storage throttle.

        Args:
            speed_tier: Speed tier name (SLOW/MODERATE/FAST/INSANE)
            max_concurrent: Maximum concurrent I/O operations
        """
        self.speed_tier = speed_tier
        self.max_concurrent = max_concurrent

        # Semaphore to limit concurrent I/O
        self._semaphore = threading.Semaphore(max_concurrent)

        # Lock to prevent thread contention
        self._lock = threading.Lock()

        # Metrics
        self.current_active = 0
        self.total_operations = 0
        self.total_wait_time = 0.0

    def throttle(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function with I/O throttling.

        Args:
            func: Function to execute (should perform I/O)
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result
        """
        # Record attempt
        with self._lock:
            self.total_operations += 1

        # Wait for I/O slot
        wait_start = time.perf_counter()
        acquired = self._semaphore.acquire(blocking=True)
        wait_duration = time.perf_counter() - wait_start

        if not acquired:
            raise Exception(f"Failed to acquire I/O slot for {self.speed_tier}")

        try:
            # Track wait time and active count
            with self._lock:
                self.total_wait_time += wait_duration
                self.current_active += 1

            # Execute the I/O operation
            result = func(*args, **kwargs)

            return result

        finally:
            # Release I/O slot
            with self._lock:
                self.current_active -= 1
            self._semaphore.release()

    def get_stats(self) -> ThrottleStats:
        """Get current throttle statistics."""
        with self._lock:
            return ThrottleStats(
                speed_tier=self.speed_tier,
                max_concurrent=self.max_concurrent,
                current_active=self.current_active,
                total_operations=self.total_operations,
                total_wait_time=self.total_wait_time
            )


class StorageThrottleManager:
    """
    Manages multiple storage throttles for different speed tiers.

    Automatically creates and manages throttles for each speed tier.
    """

    def __init__(self, custom_speeds: Optional[Dict[str, int]] = None):
        """
        Initialize storage throttle manager.

        Args:
            custom_speeds: Custom speed tier configuration (uses defaults if None)
        """
        self.speed_config = custom_speeds or STORAGE_SPEEDS.copy()

        # Create throttle for each speed tier
        self.throttles: Dict[str, StorageThrottle] = {}
        for tier, limit in self.speed_config.items():
            self.throttles[tier] = StorageThrottle(tier, limit)

        tg_print('storage', 'StorageThrottleManager initialized')
        for tier, limit in self.speed_config.items():
            tg_print('storage', f'  {tier}: {limit} concurrent I/O', level='debug')

    def get_throttle(self, speed_tier: str) -> StorageThrottle:
        """
        Get throttle for a specific speed tier.

        Args:
            speed_tier: Speed tier name (SLOW/MODERATE/FAST/INSANE)

        Returns:
            StorageThrottle for that tier
        """
        # Normalize tier name
        tier = speed_tier.upper()

        if tier not in self.throttles:
            # Unknown tier - default to MODERATE
            tg_print('storage', f"Unknown tier '{speed_tier}' — defaulting to MODERATE", level='warn')
            tier = 'moderate'

        return self.throttles[tier]

    def throttle(self, speed_tier: str, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with the appropriate throttling for speed tier.

        Args:
            speed_tier: Speed tier (SLOW/MODERATE/FAST/INSANE)
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result
        """
        throttle = self.get_throttle(speed_tier)
        return throttle.throttle(func, *args, **kwargs)

    def get_stats(self) -> Dict[str, ThrottleStats]:
        """Get statistics for all throttles."""
        return {
            tier: throttle.get_stats()
            for tier, throttle in self.throttles.items()
        }

    def print_stats(self):
        """Print statistics."""
        print()
        print("=" * 70)
        print("STORAGE THROTTLE STATISTICS")
        print("=" * 70)
        print()

        stats = self.get_stats()

        for tier in ['SLOW', 'MODERATE', 'FAST', 'INSANE']:
            if tier not in stats:
                continue

            stat = stats[tier]

            print(f"{tier}:")
            print(f"  Max concurrent: {stat.max_concurrent}")
            print(f"  Current active: {stat.current_active}")
            print(f"  Total operations: {stat.total_operations}")

            if stat.total_operations > 0:
                print(f"  Total wait time: {stat.total_wait_time:.2f}s")
                print(f"  Avg wait time: {stat.avg_wait_time():.3f}s")

            print()

        print("=" * 70)

    def update_speed_limit(self, speed_tier: str, new_limit: int):
        """
        Update the concurrent limit for a speed tier (hot reconfiguration).

        Args:
            speed_tier: Speed tier to update
            new_limit: New concurrent I/O limit
        """
        tier = speed_tier.upper()

        if tier in self.throttles:
            old_limit = self.speed_config[tier]
            self.speed_config[tier] = new_limit

            # Create new throttle with new limit
            self.throttles[tier] = StorageThrottle(tier, new_limit)

            tg_print('storage', f'Speed limit updated: {tier}  {old_limit} -> {new_limit}')
        else:
            tg_print('storage', f'Unknown tier in update_speed_limit: {tier}', level='warn')


# Global Throttle Singleton
_global_storage_throttle: Optional[StorageThrottleManager] = None
_storage_lock = threading.Lock()


def get_storage_throttle() -> StorageThrottleManager:
    """
    Get the global storage throttle manager.

    Creates one if it doesn't exist (a singleton pattern).
    """
    global _global_storage_throttle

    if _global_storage_throttle is None:
        with _storage_lock:
            if _global_storage_throttle is None:
                _global_storage_throttle = StorageThrottleManager()

    assert _global_storage_throttle is not None
    return _global_storage_throttle


def configure_storage_throttle(custom_speeds: Optional[Dict[str, int]] = None):
    """
    Configure global storage throttle (call at startup).

    Args:
        custom_speeds: Custom speed tier configuration
            Example: {'FAST': 60, 'SLOW': 5}
    """
    global _global_storage_throttle

    with _storage_lock:
        _global_storage_throttle = StorageThrottleManager(custom_speeds)

    return _global_storage_throttle

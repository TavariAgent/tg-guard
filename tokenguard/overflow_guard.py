# -*- coding: utf-8 -*-
# overflow_guard.py
"""
Overflow guard for pre-execution allocation hints and bounded retry recovery.

Combines code inspection, confidence-based allocation state, retry eligibility
checks, and backup-token tracking in one component.

Retries are recreated with bumped allocations and re-injected into the global
token pool under bounded retry policies derived from complexity level.
"""

import os
import sys
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional, Callable, Any

# Add the directory containing this file to the Python path
# This makes imports work regardless of where the project is cloned
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
from .tg_print import tg_print
from .token_options import option
from .token_system import global_token_pool, TaskToken, TokenMetadata


@dataclass
class RetryPolicy:
    """Retry limits and allocation bump rate for one complexity level."""
    max_retries: int
    # TODO: Implement new static retry policy


@dataclass
class BackupToken:
    """Retry-tracking state for one failed original token."""
    original_token_id: str
    current_retry_count: int
    max_retries: int
    created_at: float
    last_retry_at: float
    func: Callable
    args: tuple[Any, ...]
    kwargs: dict
    operation_type: Optional[str] = None

    def can_retry(self) -> bool:
        """Check if more retries allowed."""
        return self.current_retry_count < self.max_retries


class OverflowGuard:
    """Pre-execution allocation guard and bounded retry manager.

    Inspects tokens before execution, derives initial allocation guidance,
    tracks retry state for failed tokens, and recreates retry tokens when
    retry policy and failure conditions permit.
    """
    def __init__(self, base_budget_mb: int = 50):
        """
        Initialize overflow guard.
        """
        # Backup token pool
        self.BACKUP_TOKENS: Dict[str, BackupToken] = {}
        self._backup_lock = threading.Lock()

        # Metrics
        self.total_retries_created = 0
        self.total_retries_succeeded = 0
        self.total_retries_exhausted = 0

    def should_retry(
            self, token_id: str,
            execution_duration: float,
            success: bool,
            operation_type: Optional[str] = None,
            token_tags: Optional[dict] = None,
            skip: bool = False,
    ) -> bool:
        """
        Determine if a task should be retried.

        Retry criteria:
        - Execution took 10-60 seconds (failure zone)
        - Task did not succeed
        - We haven't exhausted retries
        - NOT a FINALE task (designed to fail/hang)

        Args:
            token_id: Token that executed
            execution_duration: How long it took (seconds)
            success: Did it succeed?
            operation_type: Operation type (optional, for exclusion checks)
            token_tags: Token tags (optional, for exclusion checks)
            skip: To pass the retry under given conditions (e.g. outside of the retry zone)

        Returns:
            True if should retry
        """
        if skip:
            return False
        if success:
            return False

        # EXCLUDE I/O OPERATIONS (flagged with allow_retries=false)
        # File writes, database ops, network requests should NOT retry
        # because the same args → same target → corruption/conflicts
        if token_tags and token_tags.get('allow_retries') == 'false':
            tg_print('overflow',
                     f'Skipping retry for I/O operation: {operation_type}', level='warn')
            tg_print('overflow',
                     'Reason: I/O operations with same args risk data corruption', level='warn')
            return False  # ← BLOCK RETRY

        # Check the duration threshold
        in_failure_zone = (
                option.MIN_FAILURE_DURATION <= execution_duration <= option.MAX_FAILURE_DURATION
        )

        if not in_failure_zone:
            # Too fast (error) or too slow (timeout) - don't retry
            return False

        # Check if we have retries left
        with self._backup_lock:
            if token_id in self.BACKUP_TOKENS:
                backup = self.BACKUP_TOKENS[token_id]
                return backup.can_retry()

        # First failure - can create backup
        return True

    def create_retry_token(self, original_token: TaskToken, execution_duration: float) -> Optional[TaskToken]:
        """Create a retry token with bumped allocation under the active retry policy."""
        with self._backup_lock:
            token_id = original_token.token_id

            # Check if this is a retry or first failure
            if token_id in self.BACKUP_TOKENS:
                # This is a retry that failed
                backup = self.BACKUP_TOKENS[token_id]

                if not backup.can_retry():
                    tg_print('overflow', f'Retries exhausted for {token_id}', level='warn')
                    self.total_retries_exhausted += 1
                    return None

                backup.current_retry_count += 1
                backup.last_retry_at = time.time()

                tg_print('overflow',
                         f'Retry {backup.current_retry_count}/{backup.max_retries} for {token_id}')

            else:
                # First failure - create backup entry
                # Get complexity from optimizer (or inspect again)
                operation_type: str = original_token.metadata.operation_type or "unnamed"

                # Create backup tracking
                backup = BackupToken(
                    original_token_id=token_id,
                    current_retry_count=1,
                    max_retries=option.MAX_RETRIES,
                    created_at=time.time(),
                    last_retry_at=time.time(),
                    func=original_token.func,
                    args=original_token.args,
                    kwargs=original_token.kwargs,
                    operation_type=operation_type
                )

                self.BACKUP_TOKENS[token_id] = backup
                tg_print('overflow', f'Creating backup for {token_id}')

            # Create the retry token
            retry_metadata = TokenMetadata(
                operation_type=backup.operation_type,
                created_at=time.time(),
                tags={
                    'retry': 'true',
                    'retry_count': str(backup.current_retry_count),
                    'original_token': token_id,
                }
            )

            retry_token = TaskToken(
                token_id=f"{token_id}_retry_{backup.current_retry_count}",
                func=backup.func,
                args=backup.args,
                kwargs=backup.kwargs,
                metadata=retry_metadata
            )

            # Inject directly into the token pool (BYPASS GATE!)
            self._inject_retry_to_pool(retry_token)

            self.total_retries_created += 1

            return retry_token

    @staticmethod
    def _inject_retry_to_pool(retry_token: TaskToken):
        """Inject a retry token directly into the global token pool and async queue."""

        # Add to pool's token dict
        global_token_pool.register_retry_token(retry_token)
        tg_print('overflow',
                 f'Injected retry token {retry_token.token_id} directly to pool', level='state')

    def record_success(self, token_id: str, execution_duration: float):
        """Record a successful retry outcome in aggregate statistics."""
        with self._backup_lock:
            if token_id in self.BACKUP_TOKENS:
                tg_print('overflow', f'Retry succeeded for {token_id}!')
                self.total_retries_succeeded += 1
                # Keep the backup entry for stats, mark as succeeded

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive guard statistics."""
        with self._backup_lock:
            active_backups = sum(1 for b in self.BACKUP_TOKENS.values() if b.can_retry())
            exhausted_backups = sum(1 for b in self.BACKUP_TOKENS.values() if not b.can_retry())

            return {
                'total_retries_created': self.total_retries_created,
                'total_retries_succeeded': self.total_retries_succeeded,
                'total_retries_exhausted': self.total_retries_exhausted,
                'active_backups': active_backups,
                'exhausted_backups': exhausted_backups,
            }
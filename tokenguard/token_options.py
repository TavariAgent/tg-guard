# -*- coding: utf-8 -*-
# token_options.py
"""
TokenGuard — Global Options

Set defaults before calling coordinator.start(). Live settings can be
changed at any time and are read at decision points during execution.

Usage:
    from tokenguard import option, tg_option

    option.mailbox_max(500)
    option.utilization_high(80.0)
    tg_option.enable('coordinator', 'worker')

    coordinator = OperationsCoordinator()
    coordinator.start()

Frozen settings (read once at start — ignored if set after coordinator.start()):
    mailbox_max, num_executors, enable_convergence,
    auto_block_dangerous, recent_executions_max

Live settings (can be changed at any time):
    utilization_high, utilization_low, queue_wait_threshold,
    queue_depth_factor, min_failure_duration, max_failure_duration,
    max_retries
"""


# ================================================================== #
# TokenGuard Options
# ================================================================== #

class _Option:
    """
    Global defaults store for OperationsCoordinator and related components.

    Class attributes are the live values. Call the setter methods to change
    them. The coordinator reads frozen settings once at start(); live settings
    are read at decision time throughout execution.
    """

    # ---- Frozen at start() ----
    MAILBOX_MAX:             int   = 100
    NUM_EXECUTORS:           int   = 8
    ENABLE_CONVERGENCE:      bool  = True
    AUTO_BLOCK_DANGEROUS:    bool  = False
    RECENT_EXECUTIONS_MAX:   int   = 100
    WORKERS_PER_CORE:        int   = 4

    # ---- Live ----
    UTILIZATION_HIGH:        float = 80.0
    UTILIZATION_LOW:         float = 15.0
    QUEUE_WAIT_THRESHOLD:    float = 4.0
    QUEUE_DEPTH_FACTOR:      float = 3.0
    MIN_FAILURE_DURATION:    float = 10.0
    MAX_FAILURE_DURATION:    float = 60.0
    MAX_RETRIES:             int   = 2

    # ---- Frozen setters ----

    @classmethod
    def mailbox_max(cls, value: int) -> None:
        """Max tokens per worker mailbox. Frozen at start() — do not change after coordinator starts."""
        cls.MAILBOX_MAX = int(value)

    @classmethod
    def num_executors(cls, value: int) -> None:
        """Thread pool executor count. Frozen at start()."""
        cls.NUM_EXECUTORS = int(value)

    @classmethod
    def enable_convergence(cls, value: bool) -> None:
        """Enable the convergence engine. Frozen at start()."""
        cls.ENABLE_CONVERGENCE = bool(value)

    @classmethod
    def auto_block_dangerous(cls, value: bool) -> None:
        """Guard House auto-blocking on high failure rates. Frozen at start()."""
        cls.AUTO_BLOCK_DANGEROUS = bool(value)

    @classmethod
    def recent_executions_max(cls, value: int) -> None:
        """Execution history buffer size. Frozen at start()."""
        cls.RECENT_EXECUTIONS_MAX = int(value)

    @classmethod
    def workers_per_core(cls, value: int) -> None:
        """Workers per core ceiling. Frozen at start() — convergence handles live scaling."""
        cls.WORKERS_PER_CORE = int(value)

    # ---- Live setters ----

    @classmethod
    def utilization_high(cls, value: float) -> None:
        """Core utilization % above which workers scale up."""
        cls.UTILIZATION_HIGH = float(value)

    @classmethod
    def utilization_low(cls, value: float) -> None:
        """Core utilization % below which workers scale down."""
        cls.UTILIZATION_LOW = float(value)

    @classmethod
    def queue_wait_threshold(cls, value: float) -> None:
        """Queue wait time in seconds that triggers scaling consideration."""
        cls.QUEUE_WAIT_THRESHOLD = float(value)

    @classmethod
    def queue_depth_factor(cls, value: float) -> None:
        """Multiplier applied to queue depth pressure signal."""
        cls.QUEUE_DEPTH_FACTOR = float(value)

    @classmethod
    def min_failure_duration(cls, value: float) -> None:
        """Minimum execution duration in seconds to be considered a retryable failure."""
        cls.MIN_FAILURE_DURATION = float(value)

    @classmethod
    def max_failure_duration(cls, value: float) -> None:
        """Maximum execution duration in seconds before a task is considered a timeout."""
        cls.MAX_FAILURE_DURATION = float(value)

    @classmethod
    def max_retries(cls, value: int) -> None:
        """Maximum retry attempts for a failed token."""
        cls.MAX_RETRIES = int(value)

    @classmethod
    def status(cls) -> str:
        """Return a human-readable summary of all current option values."""
        frozen: list[tuple[str, float | int]] = [
            ('mailbox_max',           cls.MAILBOX_MAX),
            ('num_executors',         cls.NUM_EXECUTORS),
            ('enable_convergence',    cls.ENABLE_CONVERGENCE),
            ('auto_block_dangerous',  cls.AUTO_BLOCK_DANGEROUS),
            ('recent_executions_max', cls.RECENT_EXECUTIONS_MAX),
        ]
        live: list[tuple[str, float | int]] = [
            ('utilization_high', cls.UTILIZATION_HIGH),
            ('utilization_low', cls.UTILIZATION_LOW),
            ('queue_wait_threshold', cls.QUEUE_WAIT_THRESHOLD),
            ('queue_depth_factor', cls.QUEUE_DEPTH_FACTOR),
            ('min_failure_duration', cls.MIN_FAILURE_DURATION),
            ('max_failure_duration', cls.MAX_FAILURE_DURATION),
            ('max_retries', cls.MAX_RETRIES),
        ]
        lines = ['TokenGuard Options', '  -- frozen at start() --']
        for name, val in frozen:
            lines.append(f'    {name:<24} {val}')
        lines.append('  -- live --')
        for name, val in live:
            lines.append(f'    {name:<24} {val}')
        return '\n'.join(lines)


# ================================================================== #
# TGPrint Options
# ================================================================== #

class _TGOption:
    """
    Convenience wrapper around TGPrint channel controls.

    All methods are live — safe to call at any point during execution.
    Proxies directly into TGPrint class attributes.
    """

    @staticmethod
    def enable(*channels: str) -> None:
        """Enable one or more log channels.  tg_option.enable('coordinator', 'worker')"""
        from .tg_print import TGPrint
        TGPrint.enable(*channels)

    @staticmethod
    def silence(*channels: str) -> None:
        """Silence one or more log channels.  tg_option.silence('convergence')"""
        from .tg_print import TGPrint
        TGPrint.silence(*channels)

    @staticmethod
    def silence_all() -> None:
        """Silence every log channel."""
        from .tg_print import TGPrint
        TGPrint.silence_all()

    @staticmethod
    def enable_all() -> None:
        """Enable every log channel."""
        from .tg_print import TGPrint
        TGPrint.enable_all()

    @staticmethod
    def debug(*channels: str, on: bool = True) -> None:
        """Enable or disable debug-level output for channels.
        tg_option.debug('coordinator', on=True)
        """
        from .tg_print import TGPrint
        if on:
            TGPrint.debug_on(*channels)
        else:
            TGPrint.debug_off(*channels)

    @staticmethod
    def status() -> str:
        """Return TGPrint channel state summary."""
        from .tg_print import TGPrint
        return TGPrint.status()


# ================================================================== #
# Public singletons
# ================================================================== #

option   = _Option()
tg_option = _TGOption()
# -*- coding: utf-8 -*-
# tg_print.py
"""
TokenGate — Modular Print System

Simple, flag-controlled output for all TokenGate modules.

Global control:
    TGPrint.enabled = False     # silence everything
    TGPrint.enabled = True      # restore everything

Per-channel control:
    TGPrint.channels['gate']        = False   # silence admission gate
    TGPrint.channels['convergence'] = False   # silence convergence engine
    TGPrint.channels['pool']        = True    # re-enable pool prints

Usage (in any TokenGate module):
    from .tg_print import tg_print

    tg_print('gate',        'Admission gate started')
    tg_print('pool',        f'Token {token_id} admitted', level='debug')
    tg_print('convergence', f'Applying {n} adjustments', level='info')
    tg_print('token',       f'{token_id} -> EXECUTING',  level='state')
    tg_print('worker',      f'Core {core_id} dispatched branch', level='dispatch')

Levels:
    'info'     — general operational message (default)
    'debug'    — verbose detail, silenced unless debug=True per channel
    'state'    — token lifecycle transition
    'dispatch' — thread/worker dispatch event (lookahead visibility)
    'warn'     — something unexpected but non-fatal
    'error'    — something failed

Thread safety:
    All output goes through a single threading.Lock so concurrent
    worker threads never interleave mid-line. This makes TokenGate
    safe to run alongside any REPL or interactive shell.
"""

from __future__ import annotations
import threading
from typing import Literal

Level = Literal['info', 'debug', 'state', 'dispatch', 'warn', 'error']

# ================================================================== #
# Channel registry — one entry per TokenGate module
# ================================================================== #

_DEFAULT_CHANNELS: dict[str, bool] = {
    'gate': False,  # admission_gate.py
    'pool': False,  # token_system.py  TokenPool
    'token': False,  # token_system.py  TaskToken lifecycle
    'coordinator': False,  # operations_coordinator.py
    'convergence': False,  # prometheus_convergence.py --- DO NOT USE IN A REPL ---
    'worker': False,  # core_pinned_staggered_queue.py
    'storage': False,  # storage_throttle.py
    'guard': False,  # guard_house.py
    'overflow': False,  # overflow_guard.py
    'affinity': False,  # core_affinity_queue.py
    'sticky': False,  # sticky_token.py
    'conductor': False,  # hash_conductor.py
}

# Per-channel debug flag — channels only show 'debug' level when True
_DEBUG_CHANNELS: dict[str, bool] = {
    ch: False for ch in _DEFAULT_CHANNELS
}


# ================================================================== #
# TGPrint — the printer
# ================================================================== #

class TGPrint:
    """
    Static-class printer for all TokenGate modules.

    Class attributes act as globals — any module that imports tg_print
    shares the same flags.
    """
    enabled: bool = True
    channels: dict[str, bool] = dict(_DEFAULT_CHANNELS)
    debug: dict[str, bool] = dict(_DEBUG_CHANNELS)
    _lock: threading.Lock = threading.Lock()

    # Level prefixes — kept short so output stays scannable
    _PREFIXES: dict[str, str] = {
        'info': '',
        'debug': '[dbg]',
        'state': '[->]',
        'dispatch': '[||]',
        'warn': '[!]',
        'error': '[!!]',
    }

    # Channel display tags — right-padded for alignment
    _TAGS: dict[str, str] = {
        'gate': '[GATE]',
        'pool': '[POOL]',
        'token': '[TOKEN]',
        'coordinator': '[COORD]',
        'convergence': '[CONVERGENCE]',
        'worker': '[WORKER]',
        'storage': '[STORAGE]',
        'guard': '[GUARD]',
        'overflow': '[OVERFLOW]',
        'affinity': '[AFFINITY]',
        'sticky': '[STICKY]',
        'conductor': '[CONDUCTOR]',
    }

    @classmethod
    def out(
            cls,
            channel: str,
            message: str,
            level: Level = 'info',
    ) -> None:
        """
        Write a message if global and channel flags permit.

        Thread-safe — acquires the class lock before writing.
        Never raises; silently swallows write errors so a logging
        failure never takes down a worker thread.
        """
        if not cls.enabled:
            return
        if not cls.channels.get(channel, True):
            return
        if level == 'debug' and not cls.debug.get(channel, False):
            return

        tag = cls._TAGS.get(channel, f'[{channel.upper():<11}]')
        prefix = cls._PREFIXES.get(level, '')
        line = f"{tag}{prefix}{message}"

        with cls._lock:
            try:
                print(line, flush=True)
            except Exception:
                pass  # never crash a worker thread over a print

    # ------------------------------------------------------------------ #
    # Convenience controls
    # ------------------------------------------------------------------ #

    @classmethod
    def silence(cls, *channels: str) -> None:
        """Turn off one or more channels.  silence('gate', 'convergence')"""
        for ch in channels:
            cls.channels[ch] = False

    @classmethod
    def enable(cls, *channels: str) -> None:
        """Turn on one or more channels.  enable('gate', 'worker')"""
        for ch in channels:
            cls.channels[ch] = True

    @classmethod
    def debug_on(cls, *channels: str) -> None:
        """Enable debug-level output for channels."""
        for ch in channels:
            cls.debug[ch] = True

    @classmethod
    def debug_off(cls, *channels: str) -> None:
        """Disable debug-level output for channels."""
        for ch in channels:
            cls.debug[ch] = False

    @classmethod
    def silence_all(cls) -> None:
        """Silence every channel (global flag stays True — easy to re-enable)."""
        for ch in cls.channels:
            cls.channels[ch] = False

    @classmethod
    def enable_all(cls) -> None:
        """Enable every channel."""
        for ch in cls.channels:
            cls.channels[ch] = True

    @classmethod
    def status(cls) -> str:
        """Return a human-readable summary of current channel states."""
        lines = [f"TGPrint  global={'ON' if cls.enabled else 'OFF'}"]
        for ch, active in sorted(cls.channels.items()):
            dbg = '  +debug' if cls.debug.get(ch) else ''
            state = 'on ' if active else 'off'
            lines.append(f"  {ch:<14}  {state}{dbg}")
        return '\n'.join(lines)


# ================================================================== #
# Module-level shortcut — import this in every TokenGate module
# ================================================================== #

def tg_print(channel: str, message: str, level: Level = 'info') -> None:
    """Shortcut for TGPrint.out().  Import this, not the class."""
    TGPrint.out(channel, message, level)

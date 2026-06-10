# -*- coding: utf-8 -*-
# __init__.py
"""
TokenGuard — Lightweight, decorator-first task routing for Python.

    pip install tg-guard

Basic usage:
    from tokenguard import task_token_guard, OperationsCoordinator

    coordinator = OperationsCoordinator()
    coordinator.start()

    @task_token_guard(operation_type='my_task')
    def my_task(data):
        ...

    token = my_task(data)
    result = token.get(timeout=30.0)

    coordinator.stop()
"""

from .operations_coordinator import OperationsCoordinator
from .token_system import TaskToken, task_token_guard
from .token_options import option, tg_option
from .sticky_token import sticky_registry
from .unhashable_checker import HashPolicy, DigestPolicy

__version__ = "0.1.0.0"
__author__  = "Tavari"
__license__ = "MIT"

__all__ = [
    "OperationsCoordinator",
    "task_token_guard",
    "TaskToken",
    "option",
    "sticky_registry",
    "tg_option",
    "HashPolicy",
    "DigestPolicy",
]
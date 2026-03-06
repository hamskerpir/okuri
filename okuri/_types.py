from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any, ParamSpec, TypeVar, TypeVarTuple

P = ParamSpec("P")
T = TypeVar("T")
Ts = TypeVarTuple("Ts")

Connection = Callable[[], AbstractAsyncContextManager[Any]]

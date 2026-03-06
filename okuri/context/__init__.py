from contextvars import ContextVar

from okuri.context.run import RunContext
from okuri.context.system import SystemContext

__all__ = [
    "SystemContext",
    "RunContext",
    "sys_ctx",
    "run_ctx",
]

sys_ctx: ContextVar[SystemContext] = ContextVar("sys_ctx")
run_ctx: ContextVar[RunContext] = ContextVar("run_ctx")

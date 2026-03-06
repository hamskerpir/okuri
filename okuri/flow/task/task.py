import inspect
from collections.abc import Awaitable, Callable
from dataclasses import make_dataclass
from typing import Any, Generic, NoReturn, Self

from pydantic import TypeAdapter

from okuri._types import P, T
from okuri.context import run_ctx
from okuri.context.run import RunContext
from okuri.flow.contract import RPCCall
from okuri.flow.route import Route


class Task(Generic[P, T]):
    def __init__(
        self,
        name: str,
        version: str,
        execution_fn: Callable[P, Awaitable[T]],
        tags: list[str] | None,
        timeout: int | None,
        retries: int,
    ):
        self.name = name
        self.fn: Callable[P, Awaitable[T]] = execution_fn
        self.route = Route(name=execution_fn.__name__, version=version)
        self.tags: list[str] | None = tags
        self.timeout: int | None = timeout
        self.retries: int = retries

        self._tp = TypeAdapter(inspect.signature(self.fn).return_annotation)
        self._tp_args = TypeAdapter(
            make_dataclass(
                self.name,
                {
                    k: p.annotation
                    for k, p in inspect.signature(self.fn).parameters.items()
                },
            )
        )

    @property
    def typecaster(self) -> TypeAdapter[Any]:
        return self._tp

    def attach(self, *, parent: Route) -> Self:
        self.route.parent = parent
        return self

    async def invoke(self, *args: P.args, **kwargs: P.kwargs) -> T | None | NoReturn:
        try:
            ctx: RunContext = run_ctx.get()
        except LookupError:  # non-runtime, do locally
            return await self.fn(*args, **kwargs)

        rpc_call = RPCCall(args=args, kwargs=kwargs)  # type: ignore[arg-type]

        try:
            # serving from cache
            return ctx.lookup(self, rpc_call)
        except LookupError:
            await ctx.delegate(self, rpc_call)  # raises asyncio.CancelledError

        raise NotImplementedError("Should never happen.")

    __call__ = invoke

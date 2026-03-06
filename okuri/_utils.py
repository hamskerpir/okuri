import asyncio
import contextlib
import inspect
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any, NoReturn

import logging

from okuri._types import P, T

system_log = logging.getLogger("okuri")


def validate(fn: Callable[P, Awaitable[T]]) -> None | NoReturn:
    if not inspect.iscoroutinefunction(fn):
        raise TypeError("Workflow function must be an async function")

    validation_errors: list[str] = []

    sig: inspect.Signature = inspect.signature(fn)
    if sig.return_annotation is inspect.Signature.empty:
        validation_errors.append(
            f"[*] Function '{fn.__name__}' must has a annotated return type"
        )

    def validate_param(p: inspect.Parameter) -> None | NoReturn:
        if p.annotation is inspect.Signature.empty:
            validation_errors.append(
                f"[*] Parameter '{p.name}' in function '{fn.__name__}' must be annotated"
            )
        return None

    tuple(map(validate_param, sig.parameters.values()))

    if validation_errors:
        raise TypeError("\n" + "\n".join(validation_errors))

    return None


async def to_thread_async(
    fn: Callable[P, Coroutine[None, None, T]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    def wrapper() -> T:
        return asyncio.run(fn(*args, **kwargs))

    return await asyncio.to_thread(wrapper)


async def cancel_current_task() -> NoReturn:  # type: ignore[misc]  # ty:ignore[invalid-return-type]
    task = asyncio.current_task()
    if task is None:
        raise asyncio.CancelledError

    task.cancel()

    # pass control to the loop
    await asyncio.sleep(0)  # raises CancelledError

@asynccontextmanager
async def background(c: Coroutine[None, None, Any], /) -> AsyncGenerator[None]:
    task = asyncio.create_task(c)
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

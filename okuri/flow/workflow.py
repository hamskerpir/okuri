import asyncio
import logging
import uuid
from asyncio import TaskGroup
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine
from contextlib import AbstractAsyncContextManager, asynccontextmanager, nullcontext
from typing import Any, NoReturn, overload

import nats
from nats.aio.client import Client
from nats.js import JetStreamContext

from okuri._types import P, T
from okuri._utils import cancel_current_task, validate
from okuri.context import RunContext, SystemContext, run_ctx, sys_ctx
from okuri.exc import InitializationError
from okuri.flow.contract import RPCCall
from okuri.flow.route import Route
from okuri.flow.state.smachine import StateMachine, StateMachineRuntime
from okuri.flow.task.runner import TasksRunner
from okuri.flow.task.task import Task
from okuri.resource.manager import NatsResourceManager

_C = Callable[[], Awaitable[Client]]

logger = logging.getLogger("okuri")


class Workflow:
    def __init__(
        self,
        name: str,
        version: str = "v1",
        # nats
        replication: int = 1,
        # WIP
        ## tasks and wf heartbeat, if not overriden. Possible to turn off.
        # heartbeat: float | timedelta | None = timedelta(seconds=20),
        ## global timeout
        # wf_timeout: timedelta | None = None
    ):
        self.name = name
        self.replication: int = replication
        self.tasks: list[Task[Any, Any]] = []
        self.version: str = version
        self.resource_manager: NatsResourceManager = NatsResourceManager(self)
        self.taskGroup = TasksRunner(self.tasks)

        # deferred initialization attributes
        # machine needs a function (that declares flow) that will be decorated later
        self.machine: StateMachine[Any, Any] | None = None
        # connect needs a function (providing connection) that will be decorated later
        self.connect: _C = nats.connect
        self._lifecycle: Callable[[], AbstractAsyncContextManager[None]] = (
            lambda: nullcontext()
        )

    def task(
        self,
        name: str,
        version: str = "v1",
        tags: list[str] | None = None,
        timeout: int | None = None,
        retries: int = 1,
    ) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
        def decorator(fn: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
            validate(fn)

            task: Task[P, T] = Task(name, version, fn, tags, timeout, retries)
            if self.machine is not None:
                task.attach(parent=self.machine.route)

            self.tasks.append(task)

            return task  # type: ignore[return-value]

        return decorator

    def export(self) -> tuple[StateMachine[Any, Any], list[Task[Any, Any]]]:
        if self.machine is None:
            raise InitializationError(
                "Workflow is not defined. Did you forget to decorate a function?"
            )

        return self.machine, self.tasks

    def bind(self, **_: Any) -> Callable[[_C], _C]:
        def decorator(fn: _C) -> _C:
            validate(fn)

            self.connect = fn
            return fn

        return decorator

    def lifecycle(
        self, fn: Callable[[], AsyncGenerator[None]]
    ) -> Callable[[], AsyncGenerator[None]]:
        # TODO: set contextlib.asclosing()
        self._lifecycle = asynccontextmanager(fn)

        return fn

    @overload
    def _assign_state_machine(
        self, f: Callable[P, Coroutine[Any, Any, T]], **__: Any
    ) -> StateMachine[P, T]: ...

    @overload
    def _assign_state_machine(
        self, f: None = None, **__: Any
    ) -> Callable[[Callable[P, Coroutine[Any, Any, T]]], StateMachine[P, T]]: ...

    def _assign_state_machine(
        self, f: Callable[P, Coroutine[Any, Any, T]] | None = None, **__: Any
    ) -> (
        Callable[[Callable[P, Coroutine[Any, Any, T]]], StateMachine[P, T]]
        | StateMachine[P, T]
    ):
        def decorator(fn: Callable[P, Coroutine[Any, Any, T]]) -> StateMachine[P, T]:
            validate(fn)

            self.machine = StateMachine(
                Route(
                    name=self.name,
                    version=self.version,
                ),
                fn,
            )
            for task in self.tasks:
                task.attach(parent=self.machine.route)

            return self.machine

        if f is not None:
            return decorator(f)

        return decorator

    __call__ = _assign_state_machine

    # TODO: fill this decorator
    def on_fail(self) -> None:
        pass

    async def serve(  # type: ignore[misc]
        self,
        tag: str | None = None,
    ) -> NoReturn:
        if self.connect is None:
            raise InitializationError(
                "Workflow is not connected. Did you forget to decorate a function?"
            )  # TODO: more informative error
        if self.machine is None:
            raise InitializationError(
                "Workflow is not defined. Did you forget to decorate a function?"
            )  # TODO: more informative error

        client: Client = await self.connect()
        js: JetStreamContext = client.jetstream()

        sys_ctx.set(
            SystemContext(
                resource_manager=self.resource_manager,
                js=js,
            )
        )

        # gather would not return value, as both `.run` methods have NoReturn
        # TODO: add contextlib.aclosing()
        async with self._lifecycle():
            try:
                await asyncio.gather(
                    self.taskGroup.run(tag),
                    StateMachineRuntime(self.machine).run(),
                )
            except (KeyboardInterrupt, asyncio.CancelledError):
                pass
            finally:
                # TODO: handle graceful shutdown
                await client.close()

    async def delegate(self, *args: Any, run_id: str = "", **kwargs: Any) -> None:
        if self.connect is None:
            raise InitializationError(
                "Workflow is not connected. Did you forget to decorate a function?"
            )  # TODO: more informative error
        if not run_id:
            run_id = uuid.uuid4().hex

        client: Client = await self.connect()
        js: JetStreamContext = client.jetstream()

        await js.publish(
            self.resource_manager.workflow_subject(run_id),
            RPCCall(
                args=args,  # type: ignore[arg-type]
                kwargs=kwargs,
            )
            .model_dump_json()
            .encode(),
        )

    @property
    def log(self) -> logging.LoggerAdapter:
        c: RunContext = run_ctx.get()
        return logging.LoggerAdapter(logging.getLogger("okuri"), {"run_id": c.run_id})

    @staticmethod
    async def parallel(
        *coro: Coroutine[None, None, Any] | Awaitable[Any]
    ) -> Any | tuple[Any, ...] | NoReturn:
        tasks = []
        async with TaskGroup() as tg:
            for c in coro:
                tasks.append(tg.create_task(c))  # type: ignore

        if any(t.cancelled() for t in tasks):
            await cancel_current_task()  # raises: asyncio.CancelledError

        if len(coro) > 1:
            return tuple(t.result() for t in tasks)
        return tasks[0].result()

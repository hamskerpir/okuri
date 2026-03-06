import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from inspect import BoundArguments, Signature
from typing import Any, Generic, NoReturn

from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig
from pydantic import BaseModel
from pydantic.dataclasses import is_pydantic_dataclass

from okuri._types import P, T
from okuri._utils import background, to_thread_async
from okuri.context import sys_ctx
from okuri.flow.contract import Metadata, RPCCall, RPCRequest, RPCResponse
from okuri.flow.msg_ctrl import Heartbeat
from okuri.flow.task.task import Task
from okuri.resource.manager import NatsResourceManager

logger = logging.getLogger("okuri")


def is_tagged_factory(tag: str | None) -> Callable[[Task[Any, Any]], bool]:
    def is_tagged(task: Task[Any, Any]) -> bool:
        if task.tags is None:
            return tag is None
        return any(t == tag for t in task.tags)

    return is_tagged


# TODO: creates event loop for each call -> expensive. Mb create constant pool with threads with pre-installed EL
@dataclass
class TaskLoop(Generic[P, T]):
    task: Task[P, T]
    subscribe: Callable[[], Awaitable[JetStreamContext.PullSubscription]]
    main_loop: asyncio.AbstractEventLoop = field(default_factory=asyncio.get_running_loop)

    # TODO: restart if connection lost
    async def start(self) -> NoReturn:
        sub: JetStreamContext.PullSubscription = await self.subscribe()
        logger.info(
            "Task runner started listening for messages: task=%s",
            self.task.name,
        )

        while True:
            for msg in await sub.fetch(timeout=None):
                # note: timeout can be rescheduled if neeeded.
                async with (background(Heartbeat(msg).start()),):
                    await to_thread_async(self._handle, msg),  # type: ignore[func-returns-value]

    async def fail(self, e: Exception, md5: str, msg: Msg) -> None:
        if msg.metadata.num_delivered >= self.task.retries:
            await self._send_response(
                msg,
                RPCResponse(
                    name=self.task.name,
                    result=None,
                    fail=str(e) or e.__repr__(),
                    md5=md5,
                )
                .model_dump_json(exclude_none=True)
                .encode(),
            )
            await msg.term()
            return

        await msg.nak()
        return

    async def _send_response(self, msg: Msg, payload: bytes) -> None:
        """Send a response back to NATS."""

        headers: Metadata = Metadata.model_validate(msg.headers)
        topic: str = headers.reply_to
        if topic is None:
            return

        asyncio.run_coroutine_threadsafe(
            msg._client.publish(topic, payload, headers=msg.headers),
            self.main_loop,
        )

    async def _handle(self, msg: Msg) -> None:
        """Process a single message within task.

        Args:
            msg: nats message received from State Machine

        Returns:
            None
            ---
            Real return is sent back to NATS as a response message. (to the state machine)
        """
        task_input, md5 = self._preprocess(
            msg, self.task.fn
        )  # type: BoundArguments, str

        try:
            async with asyncio.timeout(self.task.timeout) as _:
                result: T | None = await self.task.fn(
                    *task_input.args, **task_input.kwargs
                )
        # TODO: handle asyncio timeout separately?
        except Exception as e:
            return await self.fail(e, md5, msg)

        await self._send_response(
            msg,
            RPCResponse(
                name=self.task.name,
                result=result,
                md5=md5,
            )
            .model_dump_json(exclude_none=True)
            .encode(),
        )
        return await msg.ack()

    @staticmethod
    def _preprocess(
        msg: Msg, fn: Callable[P, Awaitable[T]]
    ) -> tuple[BoundArguments, str]:
        req: RPCRequest = RPCRequest.model_validate_json(msg.data)
        inpt: RPCCall = req.call

        signature: Signature = inspect.signature(fn)
        bound: BoundArguments = signature.bind(*inpt.args, **inpt.kwargs)
        bound.apply_defaults()

        # TODO: more reach serialization support, for dataclasses...
        for k, p in signature.parameters.items():
            if issubclass(p.annotation, BaseModel) or is_pydantic_dataclass(
                p.annotation
            ):
                if isinstance(bound.arguments[k], dict):
                    bound.arguments[k] = p.annotation(**bound.arguments[k])

        return bound, inpt.hash()


class TasksRunner:
    def __init__(self, tasks: list[Task[Any, Any]]):
        self.tasks: list[Task[Any, Any]] = tasks

    @staticmethod
    def loop_runner(
        task: Task[P, T],
        /,
        js: JetStreamContext,
        manager: NatsResourceManager,
    ) -> TaskLoop[P, T]:
        consumer: ConsumerConfig = manager.task_consumer(task.route)

        async def new_sub_fn() -> JetStreamContext.PullSubscription:
            logger.debug(
                f"Subscription ({consumer.durable_name}) on {consumer.filter_subject}"
            )

            return await js.pull_subscribe(
                subject=consumer.filter_subject,  # ty:ignore[invalid-argument-type]
                durable=consumer.durable_name,
                stream=manager.control_of_workflow().stream.name,
            )

        return TaskLoop(task, new_sub_fn)

    async def run(self, tag: str | None) -> NoReturn:  # type: ignore[misc]
        sys = sys_ctx.get()
        manager: NatsResourceManager = sys.resource_manager
        js: JetStreamContext = sys.js

        # filter tasks by tag
        tasks: list[Task[Any, Any]] = list(filter(is_tagged_factory(tag), self.tasks))

        # create and start loops for each task
        loops: list[TaskLoop[Any, Any]] = [
            self.loop_runner(task, js, manager) for task in tasks
        ]

        await asyncio.gather(*[loop.start() for loop in loops])

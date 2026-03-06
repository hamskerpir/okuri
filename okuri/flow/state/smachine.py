import asyncio
import contextlib
import logging
from asyncio import Queue
from collections.abc import Awaitable, Callable, Coroutine
from contextvars import Token
from typing import Any, Generic, NoReturn, TypeVar

from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, StreamConfig  # noqa: F401

from okuri._types import P, T
from okuri._utils import background, to_thread_async
from okuri.context import run_ctx, sys_ctx
from okuri.context.run import Delegation, RunContext
from okuri.flow.contract import Metadata, RPCCall, RPCRequest, RPCResponse
from okuri.flow.msg_ctrl import Heartbeat
from okuri.flow.route import Route
from okuri.flow.state._utils import (
    _fetch_raw_rpc_responses,
    _fetch_wf_args,
    _smachine_subscription,
    _waiting_parallels,
)
from okuri.flow.task.history import History
from okuri.resource.manager import NatsResourceManager, id_from_subject

D = TypeVar("D")

logger = logging.getLogger("okuri")


class StateMachine(Generic[P, T]):
    def __init__(self, route: Route, fn: Callable[P, Coroutine[Any, Any, T]]):
        self.route: Route = route
        self.fn: Callable[P, Coroutine[Any, Any, T]] = fn

    async def awake(self, *args: P.args, **kwargs: P.kwargs) -> T:
        return await self.fn(*args, **kwargs)

    __call__ = awake


class StateMachineRuntime:
    def __init__(self, machine: StateMachine[Any, Any]):
        self.machine: StateMachine[Any, Any] = machine

    async def run(self) -> NoReturn:
        js: JetStreamContext = sys_ctx.get().js
        subscribe: Callable[[], Awaitable[JetStreamContext.PullSubscription]] = (
            await _smachine_subscription(js)
        )

        # TODO: handle lost connection, etc.
        sub: JetStreamContext.PullSubscription = await subscribe()

        while True:
            # TODO: process in different tasks
            for msg in await sub.fetch(timeout=None):  # type: Msg
                logger.debug(
                    "State Machine got message: subject=%s, body=%s",
                    msg.subject,
                    msg.data,
                )

                # TODO: extract to separate function
                meta: Metadata
                if len(msg.headers or {}) <= 1:  # only nats metadata
                    meta = Metadata(reply_to="", checkpoint=0)
                else:
                    meta = Metadata.model_validate(msg.headers)

                async with background(Heartbeat(msg).start()):
                    try:
                        await self.process(js, msg, meta)

                    except Exception as e:
                        logger.error("Failed to process message: msg=%s, e=%s", msg, e)

                # TODO: handle network error
                #   in case of net err, we should rety later.
                await msg.ack()

    async def process(self, js: JetStreamContext, msg: Msg, meta: Metadata) -> None:
        if msg.subject.endswith("results"):
            resp = RPCResponse.model_validate_json(msg.data)
            if resp.fail:
                return

        run_id: str = id_from_subject(msg.subject)
        rm: NatsResourceManager = sys_ctx.get().resource_manager

        wf_call: RPCCall = await _fetch_wf_args(js, rm, run_id)

        msgs: list[Msg]
        if meta.checkpoint == 0:
            msgs = []
        else:
            msgs = await _fetch_raw_rpc_responses(js, rm, run_id)

            if _waiting_parallels(msg, msgs):
                return

        q: Queue[Delegation] = Queue()

        t: Token[RunContext] = run_ctx.set(
            RunContext(
                run_id,
                History(
                    [RPCResponse.model_validate_json(msg.data) for msg in msgs],
                ),
                delegation_q=q,
            )
        )
        with contextlib.suppress(asyncio.CancelledError):
            _ = await to_thread_async(self.machine.fn, *wf_call.args, **wf_call.kwargs)

        run_ctx.reset(t)

        await self.delegate(
            run_id,
            [q.get_nowait() for _ in range(q.qsize())],
            parent=meta.checkpoint,
        )

    @staticmethod
    async def delegate(
        run_id: str,
        delegations: list[Delegation],
        *,
        parent: int,
    ) -> None:
        ctx = sys_ctx.get()
        rm: NatsResourceManager = ctx.resource_manager
        js: JetStreamContext = ctx.js

        headers: Metadata = Metadata(
            reply_to=rm.result_inbox(run_id),
            checkpoint=parent + len(delegations),
        )

        # TODO: batched publishing
        for delegate in delegations:  # type: Delegation
            subject: str = rm.task_inbox(delegate.task.route, run_id)

            logger.debug(
                "Sending task RPC call: task=%s, subject=%s",
                delegate.task.name,
                subject,
            )
            await js.publish(
                subject,
                (
                    RPCRequest(
                        name=delegate.task.name,
                        call=delegate.call,
                    )
                    .model_dump_json()
                    .encode()
                ),
                headers=headers.model_dump(),
            )

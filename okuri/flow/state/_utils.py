import logging
from collections.abc import Awaitable, Callable

from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import (  # noqa: F401
    AckPolicy,
    ConsumerConfig,
    ConsumerInfo,
    DeliverPolicy,
    StreamConfig,
)

from okuri.context import sys_ctx
from okuri.flow.contract import Metadata, RPCCall
from okuri.resource.manager import NatsResourceManager

############# _utils #############
# TODO: this should be under Class with LRU caching layer
##################################


async def _fetch_raw_rpc_responses(
    js: JetStreamContext,
    rm: NatsResourceManager,
    run: str,
) -> list[Msg]:
    stream_cfg, _ = rm.control_of_workflow()  # type: StreamConfig, ConsumerConfig
    subject: str = rm.result_inbox(run)

    # TODO: document the limits on consumer, see `pull_subscribe` attributes
    # create ephemeral consumer to fetch messages for run
    sub: JetStreamContext.PullSubscription = await js.pull_subscribe(
        subject,
        durable=None,
        stream=stream_cfg.name,
        config=ConsumerConfig(
            deliver_policy=DeliverPolicy.ALL,
            # we're just reproducing messages, no need to ack
            ack_policy=AckPolicy.NONE,
        ),
    )

    info: ConsumerInfo = await sub.consumer_info()
    pending: int = info.num_pending or sub.pending_msgs
    msgs: list[Msg] = []

    batch: int = min(50, pending)
    for __ in range(0, pending, batch):
        # TODO: handle timeout and retries
        msgs.extend(
            await sub.fetch(batch=batch, timeout=1.0),
        )

    msgs.sort(key=lambda msg: msg.metadata.sequence.stream)  # just to be sure

    return msgs


# TODO: RLU cache
async def _fetch_wf_args(
    js: JetStreamContext,
    rm: NatsResourceManager,
    run_id: str,
) -> RPCCall:
    stream_cfg, _ = rm.control_of_workflow()  # type: StreamConfig, ConsumerConfig

    sub: JetStreamContext.PullSubscription = await js.pull_subscribe(
        rm.workflow_subject(run_id),
        durable=None,
        stream=stream_cfg.name,
        config=ConsumerConfig(
            deliver_policy=DeliverPolicy.LAST,
            # we're just reproducing messages, no need to ack
            ack_policy=AckPolicy.NONE,
        ),
    )

    msgs = await sub.fetch()
    if len(msgs) == 0:
        # TODO: graceful shutdown of workflow
        raise RuntimeError("No workflow arguments found")

    return RPCCall.model_validate_json(msgs[0].data)


async def _smachine_subscription(
    js: JetStreamContext,
) -> Callable[[], Awaitable[JetStreamContext.PullSubscription]]:
    rm: NatsResourceManager = sys_ctx.get().resource_manager

    stream, consumer = rm.control_of_workflow()  # type: StreamConfig, ConsumerConfig

    async def sub() -> JetStreamContext.PullSubscription:
        wf_start: str = rm.workflow_subject("*")
        logging.getLogger("okuri").info(
            "Creating pull subscription for state machine, subjects: %s",
            [wf_start, rm.result_inbox("*")],
        )

        # TODO: in this way cons is getting only messages from wf_start.
        return await js.pull_subscribe(
            subject=rm.workflow_subject(">"),
            durable=consumer.durable_name,
            stream=stream.name,
            config=ConsumerConfig(
                filter_subjects=[
                    wf_start,
                    rm.result_inbox("*"),
                ],
            ),
        )

    return sub


def _waiting_parallels(msg: Msg, results: list[Msg]) -> bool:
    if len(results) == 0:  # is initial request?
        return False

    headers: Metadata = Metadata(**results[-1].headers)  # type: ignore[arg-type]

    if not len(results) == headers.checkpoint:
        return True

    return not msg.metadata.sequence.stream == results[-1].metadata.sequence.stream

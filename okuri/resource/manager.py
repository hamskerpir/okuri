from typing import TYPE_CHECKING, Final, Literal, NamedTuple

import logging

from nats.js import JetStreamContext
from nats.js.api import (
    ConsumerConfig,
    ConsumerInfo,
    RetentionPolicy,
    StorageType,
    StoreCompression,
    StreamConfig,
    StreamInfo,
)

from okuri.exc import InitializationError
from okuri.flow.route import Route

if TYPE_CHECKING:
    from okuri.flow.workflow import Workflow

logger = logging.getLogger("okuri")


class WorkflowControl(NamedTuple):
    # main stream, where all messages for workflow are sent
    stream: StreamConfig
    # consumer for results from tasks
    consumer: ConsumerConfig


class Resources(NamedTuple):
    streams: list[StreamInfo]
    consumers: list[ConsumerInfo]


# just helper for wildcard
ALL: Literal["*"] = "*"

WF: Final[str] = "okuri.{wf}.{wf_version}.{run_id}"
RESULTS: Final[str] = WF + ".results"
TASK: Final[str] = WF + ".{task_name}.{task_version}"


def id_from_subject(topic: str, /, *, key: Literal["{run_id}"] = "{run_id}") -> str:
    inx: int = RESULTS.split(".").index(key)
    return topic.split(".")[inx]


class NatsResourceManager:
    def __init__(self, workflow: "Workflow") -> None:
        self.wf: Workflow = workflow
        self._resources: Resources = Resources(streams=[], consumers=[])

    def control_of_workflow(self) -> WorkflowControl:
        state: Route = self.wf.machine.route  # type: ignore
        replicas: int = self.wf.replication

        stream = StreamConfig(
            name=f"{state.name}_{state.version}",
            description=f"Stream for flow {state.name} {state.version}",
            subjects=[WF.format(wf=state.name, wf_version=state.version, run_id=">")],
            metadata={
                # TODO: check that this does not conflict with pulumi
                "managed_by": "okuri",
                "version": state.version,
            },
            retention=RetentionPolicy.LIMITS,
            storage=StorageType.FILE,
            num_replicas=replicas,
            compression=StoreCompression.S2,
            allow_msg_schedules=True,
        )
        consumer = ConsumerConfig(
            durable_name=f"okuri_{state.name}_{state.version}",
            description=f"Results consumer for flow {state.name} {state.version}",
            filter_subjects=[
                self.result_inbox(ALL),
                self.workflow_subject(ALL),
            ],
            headers_only=False,
        )
        return WorkflowControl(stream=stream, consumer=consumer)

    def workflow_subject(self, run_id: str) -> str:
        return WF.format(wf=self.wf.name, wf_version=self.wf.version, run_id=run_id)

    def result_inbox(self, run_id: str) -> str:
        return RESULTS.format(
            wf=self.wf.name, wf_version=self.wf.version, run_id=run_id
        )

    def task_consumer(self, task: Route) -> ConsumerConfig:
        stream: StreamConfig = self.control_of_workflow().stream

        return ConsumerConfig(
            durable_name=f"okuri_{task.name}_{task.version}",
            description=f"Consumer for task {task.name}",
            filter_subject=TASK.format(
                wf=self.wf.machine.route.name,  # type: ignore[union-attr]
                wf_version=stream.metadata["version"] if stream.metadata else "unknown",
                run_id=ALL,
                task_name=task.name,
                task_version=task.version,
            ),
            headers_only=False,
        )

    def task_inbox(self, task: Route, run_id: str) -> str:
        return TASK.format(
            wf=self.wf.name,
            wf_version=self.wf.version,
            run_id=run_id,
            task_name=task.name,
            task_version=task.version,
        )

    async def sync_with_nats(self, js: JetStreamContext, /) -> None:
        if self.wf.machine is None:
            raise InitializationError(
                "Workflow is not defined. Did you forget to decorate a function?"
            )

        ctrl: WorkflowControl = self.control_of_workflow()

        stream: StreamInfo = await js.add_stream(ctrl.stream)
        logger.info(
            "Workflow Stream successfully synced: stream=%s, replication=%s",
            stream.config.name,
            stream.config.num_replicas,
        )
        self._resources.streams.append(stream)

        state_consumer: ConsumerInfo = await js.add_consumer(
            stream.config.name,  # type: ignore[arg-type]
            ctrl.consumer,
        )
        logger.info(
            "Workflow Results Consumer (State Machine) successfully synced: consumer=%s, stream=%s",
            state_consumer.name,
            stream.config.name,
        )
        self._resources.consumers.append(state_consumer)

        for task in self.wf.tasks:
            consumer: ConsumerConfig = self.task_consumer(task=task.route)
            task_consumer: ConsumerInfo = await js.add_consumer(
                stream=stream.config.name,  # type: ignore[arg-type]
                config=consumer,
            )
            self._resources.consumers.append(task_consumer)

            logger.info(
                "Task Consumer successfully synced: task=%s, version=%s, consumer=%s, stream=%s",
                task.route.name,
                task.route.version,
                consumer.durable_name,
                stream.config.name,
            )

    async def delete_from_nats(self, js: JetStreamContext, /) -> None:
        for consumer in self._resources.consumers:
            await js.delete_consumer(consumer.stream_name, consumer.name)
            logger.info(
                "Consumer successfully deleted: consumer=%s, stream=%s",
                consumer.name,
                consumer.stream_name,
            )

        for stream in self._resources.streams:
            await js.delete_stream(stream.config.name)  # type: ignore[arg-type]
            logger.info(
                "Stream successfully deleted: stream=%s",
                stream.config.name,
            )

    def pulumi(self) -> None:
        pass

import asyncio
from asyncio.tasks import Task
from dataclasses import dataclass, field
from time import time
from typing import Any, Self

from nats.aio.msg import Msg


@dataclass
class Heartbeat:
    msg: Msg
    interval: float = 20.0
    _next_sync: float = field(default_factory=lambda: time())

    def __post_init__(self) -> None:
        self._next_sync: float = time() + self.interval

    async def start(self) -> None:
        while not self.msg.is_acked:
            if time() > self._next_sync:
                await self.msg.in_progress()
            await asyncio.sleep(max(self.interval / 20, 1))


@dataclass(slots=True)
class HeartbeatContext:
    hb: Heartbeat
    _task: Task[None] | None = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: type[Exception], exc: Exception, tb: Any) -> None:
        pass

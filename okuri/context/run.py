from asyncio import Queue
from typing import TYPE_CHECKING, Any, NamedTuple, NoReturn

from okuri._types import T
from okuri._utils import cancel_current_task
from okuri.flow.contract import RPCCall
from okuri.flow.task.history import History

if TYPE_CHECKING:
    from okuri.flow.task.task import Task


class Delegation(NamedTuple):
    task: "Task[Any, Any]"
    call: RPCCall


class RunContext:
    def __init__(
        self,
        run_id: str,
        history: History,
        delegation_q: Queue[Delegation],
    ) -> None:
        self.run_id: str = run_id
        self._history: History = history
        self._delegation_q: Queue[Delegation] = delegation_q

    def lookup(self, task: "Task[Any, T]", params: RPCCall) -> T:
        return self._history.pop(task, params.hash())

    async def delegate(self, task: "Task[Any, T]", params: RPCCall) -> NoReturn:
        await self._delegation_q.put(Delegation(task, params))
        await cancel_current_task()

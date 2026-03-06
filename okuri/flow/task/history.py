from collections import defaultdict
from typing import TYPE_CHECKING, Any

from okuri._types import T
from okuri.flow.contract import RPCResponse

if TYPE_CHECKING:
    from okuri.flow.task.task import Task


class History:
    def __init__(self, results: list[RPCResponse]):
        self._results: dict[str, list[RPCResponse]] = defaultdict(list)
        for entry in results:
            self._results[f"{entry.name}{entry.md5}"].append(entry)

    def len(self) -> int:
        return len(self._results)

    def pop(self, task: "Task[Any, T]", md5: str) -> T:
        entry: list[RPCResponse] | None = self._results.get(
            f"{task.name}{md5}",
            None,
        )
        if not entry:
            raise LookupError(f"Task {task.name} with md5 {md5} not found in history")

        # TODO: check for non-deterministic
        result = entry.pop().result
        return task.typecaster.validate_python(result)  # type: ignore[no-any-return]

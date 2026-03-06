from json import JSONEncoder
from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field

from okuri.reference import Backend, Reference
from okuri.reference.internal.registry import Registry, backend
from okuri.reference.types import Schema

AnyReference = TypeVar("AnyReference", bound=Reference[Any])


_ReferenceUniqueKey: Literal["#okuri::reference"] = "#okuri::reference"


class ReferenceBox(BaseModel, Generic[AnyReference]):
    type: Literal["#okuri::reference"] = Field(alias="#type", default=_ReferenceUniqueKey, init=False)
    proto: Schema

    data: AnyReference


class RefMarshaller(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Reference):
            return ReferenceBox(
                proto=backend(o).SCHEMA,
                data=o,
            ).model_dump(by_alias=True)

        return super().default(o)

    @classmethod
    def object_hook(cls, obj: dict[str, Any]) -> Any:
        if obj.get("#type") == _ReferenceUniqueKey:
            back: type[Backend[Any, Any]] = Registry.routes[obj["proto"]]

            return back.REF.model_validate(obj["data"])

        return obj

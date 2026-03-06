from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, NamedTuple, TypeVar

from okuri.exc import InternalResourceNotFoundError
from okuri.reference.internal.reference import Reference
from okuri.reference.internal.registry import Registry, reference_origin, reference_type
from okuri.reference.types import DataT

TransportT = TypeVar("TransportT")
ApplicationT = TypeVar("ApplicationT")


class TypeCaster(NamedTuple, Generic[ApplicationT, TransportT]):
    dump: Callable[[ApplicationT], TransportT]
    load: Callable[[TransportT], ApplicationT]


@dataclass
class TypeCasterLazy(Generic[DataT]):
    """A lazy type caster that resolves the actual caster on dump, not on initialization.
    It allows to delay the resolution of internal type until the data is known (on dump).
    """

    ref: Reference[DataT]

    @property
    def caster(self) -> TypeCaster[DataT, Any]:
        t = type(self.ref)
        try:
            caster = Registry.casts[reference_origin(t)][reference_type(self.ref)]
        except KeyError as e:
            raise InternalResourceNotFoundError(
                f"No type caster bound for reference type '{t.__name__}' with internal type '{reference_type(self.ref)}'"
            ) from e
        return caster

    def dump(self, data: DataT) -> Any:
        self.ref.__pydantic_generic_metadata__["args"] = (type(data),)
        return self.caster.dump(data)

    def load(self, data: Any) -> DataT:
        return self.caster.load(data)

from collections.abc import Callable
from typing import Any

from okuri._types import Connection, T
from okuri.reference import Reference
from okuri.reference.helper import TypeCaster
from okuri.reference.internal.registry import Registry, reference_type


def bind(ref: type[Reference[Any]], name: str = "") -> Callable[[Connection], Connection]:
    def decorator(fn: Connection) -> Connection:
        Registry.connections[ref][name] = fn

        return fn

    return decorator


def cast(ref: type[Reference[T]], caster: TypeCaster[T, Any]) -> None:
    kls: type[Reference[Any]] = ref.__pydantic_generic_metadata__["origin"]  # type: ignore[assignment]
    Registry.casts[kls][reference_type(ref)] = caster

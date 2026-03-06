from abc import ABCMeta
from collections import defaultdict
from typing import TYPE_CHECKING, Any, ClassVar

from okuri._types import Connection
from okuri.exc import InternalResourceNotFoundError
from okuri.reference.types import DataT, Schema

if TYPE_CHECKING:
    from okuri.reference.helper import TypeCaster
    from okuri.reference.internal.backend import Backend
    from okuri.reference.internal.reference import Reference


class Registry(metaclass=ABCMeta):  # noqa: B024
    """Just a namespace for registries.

    Attributes:
        connections: A mapping of connection names to backend types.
        routes: A mapping of schema types to backend types.
        casts: A mapping of reference types to type casters.
    """

    connections: ClassVar[
        dict[
            type["Reference[Any]"],
            dict[str, Connection],
        ]
    ] = defaultdict(dict)

    routes: ClassVar[dict[Schema, type["Backend[Any, Any]"]]] = {}

    casts: ClassVar[
        dict[
            type["Reference[Any]"],
            dict[type, "TypeCaster[Any, Any]"],
        ]
    ] = defaultdict(dict)


def reference_origin(ref: "Reference[DataT] | type[Reference[DataT]]") -> type["Reference[Any]"]:
    origin: type[Reference[Any]] = ref.__pydantic_generic_metadata__["origin"]  # type: ignore[assignment]
    return origin


def reference_type(ref: "Reference[DataT] | type[Reference[DataT]]") -> type[DataT]:
    return ref.__pydantic_generic_metadata__["args"][0]  # type: ignore[no-any-return]


def backend(ref: "Reference[DataT]") -> "Backend[DataT, Any]":
    ref_type = type(ref)
    for schema, back in Registry.routes.items():
        if back.REF == reference_origin(ref_type):
            return Registry.routes[schema](ref)

    raise InternalResourceNotFoundError(f"No backend registered for reference type {ref_type.__name__}")

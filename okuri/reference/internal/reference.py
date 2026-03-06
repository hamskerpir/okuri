from abc import ABCMeta
from typing import (
    Any,
    Generic,
    final,
)

from pydantic import BaseModel

from okuri.reference.internal.registry import backend
from okuri.reference.types import DataT


class Reference(BaseModel, Generic[DataT], metaclass=ABCMeta):
    # TODO: typecaster with basemodel metadata:
    # >>> Some[str].__pydantic_generic_metadata__
    # {'origin': <class '__main__.Some'>, 'args': (<class 'str'>,), 'parameters': ()}

    # proto is used to keep code clean, as python typing system does block TypeVars for class variables for a good
    # reason, but edge cases, like that one, then are marked as incorrect as well...

    # delegated methods for backend
    @final
    async def upload(self, data: DataT, **backend_kwargs: Any) -> Any:
        return await backend(self).upload(data, **backend_kwargs)

    @final
    async def get(self, **backend_kwargs: Any) -> DataT:
        return await backend(self).download(**backend_kwargs)

    def __class_getitem__(cls, item):  # type: ignore[no-untyped-def]
        return super().__class_getitem__(item)

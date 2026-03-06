import asyncio
from abc import ABCMeta, abstractmethod
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Any, ClassVar, Generic, Protocol, Self, TypeVar

from okuri._types import T, Ts
from okuri.exc import InitializationError, InternalResourceNotFoundError
from okuri.reference.helper import TypeCasterLazy
from okuri.reference.internal.reference import Reference
from okuri.reference.internal.registry import Registry, reference_origin
from okuri.reference.types import DataT, Schema

T_cov = TypeVar("T_cov", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)
ConnectionT = TypeVar("ConnectionT")


class Reader(Protocol[T_cov]):
    async def read(self, n: int = -1) -> T_cov: ...


class Writer(Protocol[T_contra]):
    async def write(self, data: T_contra) -> int: ...



class Backend(Generic[DataT, ConnectionT], metaclass=ABCMeta):
    SCHEMA: ClassVar[Schema]
    REF: ClassVar[type[Reference[Any]]]  # there Any is DataT, but ClassVar should not contain TypeVars

    def __init__(self, ref: Reference[DataT]) -> None:
        self.ref: Reference[DataT] = ref
        self.bind_hint: str = ""

    @staticmethod
    def submit(fn: Callable[[*Ts], T], *args: *Ts) -> Awaitable[T]:
        default_executor: None = None  # asynci would use ThreadPoolExecutor by default
        # TODO: wait_for and set timeout!

        # Для оркестратора важно понимать, насколько загружен твой ThreadPoolExecutor. Если очередь переполнена, новые задачи в системе должны ждать (Backpressure).
        #
        # В стандартном ThreadPoolExecutor нет удобного способа посмотреть размер очереди «из коробки», но ты можешь использовать его внутренности (хоть это и не очень приветствуется) или обернуть его:
        #
        #     executor._work_queue.qsize() — позволяет узнать, сколько задач ждут выполнения. Если это число растет, значит, твой оркестратор «захлебывается» и нужно либо шкалировать процессы, либо увеличивать количество тредов.
        return asyncio.get_event_loop().run_in_executor(default_executor, fn, *args)

    @property
    def caster(self) -> TypeCasterLazy[DataT]:
        return TypeCasterLazy(self.ref)

    def bind(self, name: str, /) -> Self:
        self.bind_hint = name
        return self

    def connection(self) -> AbstractAsyncContextManager[ConnectionT]:
        try:
            conn_factory = Registry.connections[reference_origin(self.ref)][self.bind_hint]
        except KeyError as e:
            raise InternalResourceNotFoundError(
                f"No connection factory bound for reference type {type(self.ref).__name__}. Did you forget to bind one?"
            ) from e

        return conn_factory()

    @abstractmethod
    async def upload(self, data: DataT, **backend_kwargs: Any) -> None: ...

    @abstractmethod
    async def download(self, **backend_kwargs: Any) -> DataT: ...

    @abstractmethod
    async def reader(self) -> AbstractAsyncContextManager[Reader[DataT]]: ...

    @abstractmethod
    async def writer(self) -> AbstractAsyncContextManager[Writer[DataT]]: ...

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        try:
            schema = cls.SCHEMA
            referer = cls.REF
        except AttributeError as e:
            raise InitializationError(
                f"Backend subclass {cls.__name__} is missing required class variable: {e.args[0]}"
            ) from e

        if not issubclass(referer, Reference):
            raise InitializationError(
                f"Backend subclass {cls.__name__} has invalid REF class variable: must be a subclass of Reference"
            )

        if not isinstance(schema, Schema):
            raise InitializationError(
                f"Backend subclass {cls.__name__} has invalid SCHEMA class variable: must be of type Schema"
            )

        # OK
        Registry.routes[schema] = cls

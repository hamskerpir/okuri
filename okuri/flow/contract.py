from hashlib import md5
from typing import Annotated, Any, Self, TypeVar

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_serializer, model_validator

from okuri.reference import Reference

AnyBaseModel = TypeVar("AnyBaseModel", bound=BaseModel)

_ArgValue = (
    # Primitive JSON types and common containers
    str
    | list[Any]
    | dict[str, Any]
    | int
    | float
    | bool
    | None
    # Pydantic models
    | AnyBaseModel  # type: ignore[misc]
    # Okuri decoder handled types
    | Reference[Any]
)


class Metadata(BaseModel):
    reply_to: Annotated[str, Field(alias="X-Okuri-Reply-To", description="Subject to send reply to")]
    # Shows a number of messages that would show that we can continue
    # with processing; all parallels have ended.
    checkpoint: Annotated[int, Field(alias="X-Okuri-Checkpoint")]

    model_config = ConfigDict(populate_by_name=True)

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        kwargs["by_alias"] = True
        return super().model_dump(**kwargs)

    @field_serializer("checkpoint")
    def serialize_checkpoint(self, val: int, _: Any) -> str:
        return str(val)


class RPCCall(BaseModel):
    args: list[_ArgValue]  # type: ignore[valid-type]
    kwargs: dict[str, _ArgValue]  # type: ignore[valid-type]

    def hash(self) -> str:
        return md5(
            self.model_dump_json().encode(),
        ).hexdigest()


class RPCRequest(BaseModel):
    """Request to process a function / task.

    Attributes:
        name: Name of the function to run.
        call: Arguments to pass to the function.
        checksum: MD5 checksum of the call arguments,
            needed to distinguish between different calls with the same arguments.
    """

    name: str
    call: RPCCall

    @computed_field
    def checksum(self) -> str:
        return self.call.hash()


class RPCResponse(BaseModel):
    name: str
    # md5 is checksum of args in rpc call
    md5: str
    result: _ArgValue | tuple[_ArgValue, ...] | None = None  # type: ignore[valid-type]
    fail: str | None = None

    def success(self) -> bool:
        return self.fail is None

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        if not ((self.fail is None) ^ (self.result is None)):  # xor
            raise ValueError("Either result or fail must be set.")

        return self

from typing import ClassVar

from pydantic import BaseModel, field_validator


class Route(BaseModel):
    name: str
    version: str

    parent: "Route | None" = None

    # for us important to forbid these chars in task names,
    # so topic would not divide incorrectly, other validations on NATS
    forbidden_chars: ClassVar[str] = ". >*"

    @field_validator("version", "name")
    def validate(cls, v: str) -> str:  # type: ignore[override]
        if any(char in v for char in cls.forbidden_chars):
            raise ValueError(f"Invalid name/version - {v}. Can not contain `{cls.forbidden_chars}`")
        return v

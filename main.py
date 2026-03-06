import asyncio
from collections.abc import AsyncGenerator

import nats
from nats.aio.client import Client
from pydantic import BaseModel

from okuri.flow.workflow import Workflow
from okuri.resource.manager import NatsResourceManager

wf = Workflow(name="demo")


class User(BaseModel):
    name: str
    age: int


@wf.task(
    "Add user",
)
async def add_user(username: str, age: int) -> User:
    return User(name=username, age=age)


@wf.task(
    "Validate user's age",
    retries=3,
)
async def validate_age(user: User, /) -> bool:
    return user.age >= 18


@wf
async def user_validation(age: int) -> None:
    user, *_ = await wf.parallel(
        add_user("Sofia", age),
        add_user("Sofia", age),
        add_user("Sofia", age),
    )

    if await validate_age(user):
        print(f"User: {user.name}, is adult")
        return

    print(f"User: {user.name}, is minor")


@wf.lifecycle
async def lifecycle() -> AsyncGenerator[None]:
    manager: NatsResourceManager = NatsResourceManager(wf)
    nc = await nats.connect()
    js = nc.jetstream()

    await manager.sync_with_nats(js)
    await nc.close()

    yield

    nc = await nats.connect()
    js = nc.jetstream()

    await manager.delete_from_nats(js)
    await nc.close()


@wf.bind()
async def _() -> Client:
    return await nats.connect()


if __name__ == "__main__":

    async def main() -> None:
        task = asyncio.create_task(wf.serve())
        await asyncio.sleep(0.5)
        await asyncio.create_task(wf.delegate(age=17))

        await task

    asyncio.run(main())

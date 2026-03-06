# Okuri

Modern minimalistic, lightweight workflow engine and distributed task queue for Python over NATS.

## Key Features

- Zero Bloat (2 Dependencies): Built exclusively on `nats-py` and `pydantic`.
- (NOT IMPLEMENTED YET) Replay & State Machine: Okuri's state-machine architecture allows you to replay failed workflows deterministically, to instantly reproduce production bugs locally.
- First-Class Versioning: Deploy with confidence. Okuri natively supports workflow and task versioning, giving you control over it.
- (NOT IMPLEMENTED YET) Built for HA & IaC: Designed from the ground up for High Availability. Ready for modern DevOps pipelines with included Pulumi templates.
- (NOT IMPLEMENTED YET) UI: Comes with a dashboard built for developers. Monitor worker health, trace task workflows.

## Code examples

Let's walk through basics of framework:

### Define workflow instance

```python
from okuri.flow.workflow import Workflow

wf = Workflow("hello-world", version="v1")
```

### Connect to the NATS instance
```python
import nats

@wf.bind()
async def connection_to_nats() -> nats.Client:
    # connect to locall nats instance (you can run it with docker)
    return await nats.connect()
```

### Define a lifecycle

This is just an example to create needed NATS resources until no IaC is implemented in the framework.

```python
from okuri.resource.manager import NatsResourceManager

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
```

### Create a workflow
```python
from pydantic import BaseModel

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
    retries=3,  # you can add retries if task would raise an exception
    timeout=10,  # you can set timeout for the task
    tags=["foo", "bar"]  # set tags to select workers to run on machine
)
async def validate_age(user: User, /) -> bool:
    return user.age >= 18


@wf
async def user_validation(age: int) -> None:
    # sending tasks to work in parallel (not that asyncio.grather would not work here)
    alex, *_ = await wf.parallel(
        add_user("Alex", age),
        add_user("Boris", age * 2),
        add_user("Donald", age // 2),
    )

    if await validate_age(alex):
        print(f"User: {alex.name}, is adult")
        return

    print(f"User: {alex.name}, is minor")
```

### Start a workflow
```python
async def main() -> None:
    # start working on tasks
    # if you want to select a tasks to run locally run `wf.serve(tag="foo")`
    task = asyncio.create_task(wf.serve())
    await asyncio.sleep(0.5)

    # start delegate the task!
    await wf.delegate(age=17)
    await task # runs forever

asyncio.run(main())
```

from typing import NamedTuple, TypeVar

DataT = TypeVar("DataT")


class Schema(NamedTuple):
    dialect: str
    """Dialect of engine database is using under

    Some examples:
    - postgresql
    - mysql
    - sqlite
    - mongodb
    - redis
    - cassandra

    This and driver is then dumped (in format `{dialect}+{driver}`)to the message of NATS, and then on receive
    backend would be chosen based on that (who supports that dialect).
    """

    driver: str | None = None
    """Driver of the database engine.

    Some examples:
    - asyncpg
    - aiomysql
    - aiosqlite
    - motor
    - aioredis
    - cassandra-driver

    May be omitted, then default driver for the dialect would be used.
    """

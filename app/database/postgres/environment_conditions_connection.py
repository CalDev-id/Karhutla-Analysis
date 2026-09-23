from contextlib import contextmanager
from threading import Lock
from time import sleep
from typing import Generator, Optional

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from app.config import environment


class EnvironmentConditionsConfigurationError(Exception):
    """PostgreSQL environment variables are incomplete."""


class EnvironmentConditionsConnectionError(Exception):
    """The environment_conditions PostgreSQL database cannot be reached."""


_pool: Optional[ThreadedConnectionPool] = None
_pool_lock = Lock()


def _config() -> dict:
    host = environment("POSTGRESQL_HOST")
    port = environment("POSTGRESQL_PORT")
    user = environment("POSTGRESQL_USER")
    password = environment("POSTGRESQL_PASSWORD")
    if not all((host, port, user, password)):
        raise EnvironmentConditionsConfigurationError
    try:
        return {
            "host": host,
            "port": int(port),
            "user": user,
            "password": password,
            "dbname": "environment_conditions",
            "connect_timeout": 10,
        }
    except ValueError as exc:
        raise EnvironmentConditionsConfigurationError from exc


def _connection_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is not None:
            return _pool
        last_error = None
        for attempt in range(3):
            try:
                _pool = ThreadedConnectionPool(1, 5, **_config())
                return _pool
            except psycopg2.Error as exc:
                last_error = exc
                if attempt < 2:
                    sleep(attempt + 1)
        raise EnvironmentConditionsConnectionError from last_error


@contextmanager
def connection() -> Generator:
    database_connection = None
    pool = _connection_pool()
    try:
        database_connection = pool.getconn()
        if database_connection.closed:
            pool.putconn(database_connection, close=True)
            database_connection = pool.getconn()
        database_connection.autocommit = True
        yield database_connection
    except psycopg2.Error:
        if database_connection is not None:
            pool.putconn(database_connection, close=True)
            database_connection = None
        raise
    finally:
        if database_connection is not None:
            pool.putconn(database_connection)

import os
from contextlib import contextmanager
from threading import Lock
from time import sleep
from typing import Generator, Optional

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv


class PostgreSQLConfigurationError(Exception):
    """PostgreSQL environment variables are incomplete."""


class PostgreSQLConnectionError(Exception):
    """PostgreSQL cannot be reached or authenticated."""


_pool: Optional[ThreadedConnectionPool] = None
_pool_lock = Lock()


def _config() -> dict:
    load_dotenv()
    host = os.getenv("POSTGRESQL_HOST")
    port = os.getenv("POSTGRESQL_PORT")
    user = os.getenv("POSTGRESQL_USER")
    password = os.getenv("POSTGRESQL_PASSWORD")
    if not all((host, port, user, password)):
        raise PostgreSQLConfigurationError
    try:
        return {
            "host": host, "port": int(port), "user": user, "password": password,
            "dbname": "environmental_conditions", "connect_timeout": 10,
        }
    except ValueError as exc:
        raise PostgreSQLConfigurationError from exc


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
        raise PostgreSQLConnectionError from last_error


@contextmanager
def connection() -> Generator:
    database_connection = None
    pool = _connection_pool()
    last_error = None
    try:
        database_connection = pool.getconn()
        if database_connection.closed:
            pool.putconn(database_connection, close=True)
            database_connection = pool.getconn()
        database_connection.autocommit = True
        yield database_connection
    except psycopg2.Error as exc:
        last_error = exc
        if database_connection is not None:
            pool.putconn(database_connection, close=True)
            database_connection = None
        raise
    finally:
        if database_connection is not None:
            pool.putconn(database_connection)

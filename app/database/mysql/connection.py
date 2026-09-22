from contextlib import contextmanager
from time import sleep
from typing import Generator

import pymysql

from app.config import environment


class MySQLConfigurationError(Exception):
    """MySQL environment variables are incomplete."""


class MySQLConnectionError(Exception):
    """MySQL cannot be reached or authenticated."""


def _config() -> dict[str, object]:
    host = environment("MYSQL_HOST")
    port = environment("MYSQL_PORT")
    user = environment("MYSQL_USER")
    password = environment("MYSQL_PASSWORD")
    if not all((host, port, user, password)):
        raise MySQLConfigurationError
    try:
        return {
            "host": host,
            "port": int(port),
            "user": user,
            "password": password,
            "autocommit": True,
            "connect_timeout": 10,
            "read_timeout": 30,
            "write_timeout": 30,
        }
    except ValueError as exc:
        raise MySQLConfigurationError from exc


@contextmanager
def connection() -> Generator[pymysql.connections.Connection, None, None]:
    database_connection = None
    last_error = None
    for attempt in range(3):
        try:
            database_connection = pymysql.connect(**_config())
            break
        except pymysql.MySQLError as exc:
            last_error = exc
            if attempt < 2:
                sleep(attempt + 1)
    if database_connection is None:
        raise MySQLConnectionError from last_error
    try:
        yield database_connection
    finally:
        database_connection.close()


def list_databases() -> list[str]:
    with connection() as database_connection:
        with database_connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            return [row[0] for row in cursor.fetchall()]

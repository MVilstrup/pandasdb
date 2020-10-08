from pandasdb import Table
from pandasdb.connections import PostgresConnection

import pandas as pd
import pytest
from pandasdb.utils import AutoComplete, string_to_python_attr


@pytest.fixture(scope="session", autouse=True)
def postgres_db():
    conn = PostgresConnection(host=None, schema="test", username=None, password=None, database="test", port=5432,
                              tunnel=None, ssh_username=None, ssh_key=None)

    tables = []

    """
    User Table
    """
    columns = []
    for name, type in [("id", int), ("name", str), ("age", int)]:
        col = conn.ops.Column(name, type)
        columns.append(col)

    tables.append(Table("user", conn.schema, lambda: conn, False, *columns))

    """
    Car Table
    """
    columns = []
    for name, type in [("id", int), ("user_id", int), ("name", str), ("hp", int)]:
        col = conn.ops.Column(name, type)
        columns.append(col)

    tables.append(Table("car", conn.schema, lambda: conn, False, *columns))

    conn._Ts = AutoComplete("Tables", {string_to_python_attr(table.name): table for table in tables})

    return conn

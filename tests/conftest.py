from pandasdb import Table
from pandasdb.connections import PostgresConnection

import pandas as pd
import pytest
from pandasdb.utils import AutoComplete, string_to_python_attr
from pandasdb.connections.sql.sql_query import SQLQuery


@pytest.fixture(scope="session", autouse=True)
def postgres_db():
    conn = PostgresConnection(host=None, name="test", type="POSTGRES", schema="test", username=None, password=None,
                              database="test", port=5432,
                              tunnel=None, ssh_username=None, ssh_key=None)

    tables = []

    """
    User Table
    """
    columns = []
    for name, type in [("id", int), ("name", str), ("age", int), ("primary_user_id", int)]:
        col = conn.ops.Column(name, type)
        columns.append(col)

    tables.append(Table("user", conn.schema, conn, False, columns))

    """
    Car Table
    """
    columns = []
    for name, type in [("id", int), ("user_id", int), ("name", str), ("hp", int)]:
        col = conn.ops.Column(name, type)
        columns.append(col)

    tables.append(Table("car", conn.schema, conn, False, columns))

    conn._TAB = AutoComplete("Tables", {string_to_python_attr(table.name): table for table in tables})

    return conn


@pytest.fixture(scope="session", autouse=True)
def clean_sql():
    return lambda s: str(s).replace("\n", " ").strip()

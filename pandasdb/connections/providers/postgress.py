from pandasdb.connections.sql_connection import SQLConnection
import pandasdb.operators as ops
import pandasdb.types as types
from pandasdb.column import Column
from pandasdb.operators import Operator
from pandasdb.table import Table

import pandas as pd
import psycopg2
from functools import partial
import numpy as np

from pandasdb.utils import iterable


def handle_values(value):
    if isinstance(value, str):
        return f"'{value}'"

    if iterable(value):
        if isinstance(value, dict):
            return f"{tuple(value.keys())}"

        if not isinstance(value, Column):
            return f"({', '.join([handle_values(v) for v in value])})"

    return f"{value}"


class SupportedOps:

    def VALUE_OR_OPERATION(self, val):
        checks = [isinstance(val, t) for t in [int, float, bool, str, np.ndarray, list, dict, tuple, set]]
        if any(checks):
            return partial(ops.Value, symbol="VALUE", supported_ops=self, format=handle_values)(val)
        else:
            return val

    def __init__(self):
        supported_ops = {
            # Numeric Operators
            ops.ADD.__name__: partial(ops.ADD, symbol="+", supported_ops=self),
            ops.SUB.__name__: partial(ops.SUB, symbol="-", supported_ops=self),
            ops.MUL.__name__: partial(ops.MUL, symbol="*", supported_ops=self),
            ops.DIV.__name__: partial(ops.DIV, symbol="/", supported_ops=self),
            ops.POW.__name__: partial(ops.POW, symbol="^", supported_ops=self),
            ops.MOD.__name__: partial(ops.MOD, symbol="%", supported_ops=self),

            # Logical Operators
            ops.AND.__name__: partial(ops.AND, symbol="AND", supported_ops=self, format=lambda x: f"({x})"),
            ops.OR.__name__: partial(ops.OR, symbol="OR", supported_ops=self, format=lambda x: f"({x})"),
            ops.NOT.__name__: partial(ops.NOT, symbol="~", supported_ops=self),

            # Comparison Operators
            ops.LT.__name__: partial(ops.LT, symbol="<", supported_ops=self),
            ops.GT.__name__: partial(ops.GT, symbol=">", supported_ops=self),
            ops.LE.__name__: partial(ops.LE, symbol="<=", supported_ops=self),
            ops.GE.__name__: partial(ops.GE, symbol=">=", supported_ops=self),
            ops.EQ.__name__: partial(ops.EQ, symbol="=", supported_ops=self),
            ops.NE.__name__: partial(ops.NE, symbol="!=", supported_ops=self),

            # Higher level Operators
            ops.IN.__name__: partial(ops.IN, symbol="IN", supported_ops=self),
            ops.NOTIN.__name__: partial(ops.NOTIN, symbol="NOT IN", supported_ops=self),
            ops.LIKE.__name__: partial(ops.LIKE, symbol="LIKE", supported_ops=self),
            ops.LIMIT.__name__: partial(ops.LIMIT, symbol="LIMIT", supported_ops=self),
            ops.OFFSET.__name__: partial(ops.OFFSET, symbol="OFFSET", supported_ops=self),
            ops.ALIAS.__name__: partial(ops.ALIAS, symbol="AS", supported_ops=self),
            ops.ORDER_BY.__name__: partial(ops.ORDER_BY, symbol="ORDER BY", supported_ops=self),
            ops.GROUP_BY.__name__: partial(ops.GROUP_BY, symbol="GROUP BY", supported_ops=self),
            ops.JOIN.__name__: partial(ops.JOIN, symbol="JOIN", supported_ops=self),
            ops.WHERE.__name__: partial(ops.WHERE, symbol="WHERE", supported_ops=self),
            ops.HAVING.__name__: partial(ops.HAVING, symbol="HAVING", supported_ops=self),

            # Function Operators
            ops.MIN.__name__: partial(ops.MIN, symbol="MIN", supported_ops=self),
            ops.MAX.__name__: partial(ops.MAX, symbol="MAX", supported_ops=self),
            ops.AVG.__name__: partial(ops.AVG, symbol="AVG", supported_ops=self),
            ops.SUM.__name__: partial(ops.SUM, symbol="SUM", supported_ops=self),
            ops.COUNT.__name__: partial(ops.COUNT, symbol="COUNT", supported_ops=self),
            ops.SUBSTRING.__name__: partial(ops.SUBSTRING, symbol="SUBSTRING", supported_ops=self),

            # Constant Operators
            ops.SELECT.__name__: partial(ops.SELECT, symbol="SELECT", supported_ops=self),
            ops.ASC.__name__: partial(ops.ASC, symbol="ASC", supported_ops=self),
            ops.DESC.__name__: partial(ops.DESC, symbol="DESC", supported_ops=self),
            ops.ALL.__name__: partial(ops.ALL, symbol="*", supported_ops=self),
            ops.Value.__name__: self.VALUE_OR_OPERATION,

            # Column
            Column.__name__: partial(Column, symbol="VALUE", supported_ops=self)
        }

        for name, operation in supported_ops.items():
            setattr(self, name, operation)


class PostgresConnection(SQLConnection):
    ops = SupportedOps()

    def __init__(self, host, schema, username, password, database, port=5432, tunnel=None, ssh_username=None,
                 ssh_key=None):
        SQLConnection.__init__(self, host=host, schema=schema, username=username, password=password, port=port,
                               database=database, tunnel=tunnel, ssh_username=ssh_username, ssh_key=ssh_key)
        self.reserfved_words = ["INDEX"]

    def connect(self):
        try:
            return psycopg2.connect(user=self.username,
                                    password=self.password,
                                    host=self.host,
                                    port=self.port,
                                    database=self.database)
        except Exception as exp:
            raise ConnectionError("Could not connect to database. Error: {}".format(exp))

    @staticmethod
    def accepted_types(self, operator):
        accepted_types = {
            # Numeric Operators
            ops.ADD.__name__: types.NUMBERS,
            ops.SUB.__name__: types.NUMBERS,
            ops.MUL.__name__: types.NUMBERS,
            ops.DIV.__name__: types.NUMBERS,
            ops.MOD.__name__: types.NUMBERS,
            ops.POW.__name__: types.NUMBERS,

            # Logical Operators
            ops.AND.__name__: types.LOGICAL,
            ops.OR.__name__: types.LOGICAL,
            ops.NOT.__name__: types.LOGICAL,

            # Comparison Operators
            ops.LT.__name__: types.NUMBERS,
            ops.GT.__name__: types.NUMBERS,
            ops.LE.__name__: types.NUMBERS,
            ops.GE.__name__: types.NUMBERS,
            ops.EQ.__name__: types.ALL,
            ops.NE.__name__: types.ALL,

            # Higher level Operators
            ops.IN.__name__: types.ALL,
            ops.NOTIN.__name__: types.ALL,
            ops.LIKE.__name__: types.ALL,
            ops.LIMIT.__name__: types.NUMBERS,
            ops.OFFSET.__name__: types.NUMBERS,
            ops.ALIAS.__name__: types.IDENTIFIERS,
            ops.ORDER_BY.__name__: types.IDENTIFIERS,
            ops.GROUP_BY.__name__: types.IDENTIFIERS,
            ops.JOIN.__name__: types.IDENTIFIERS,
            ops.WHERE.__name__: types.IDENTIFIERS,
            ops.HAVING.__name__: types.IDENTIFIERS,

            # Function Operators
            ops.MIN.__name__: types.NUMBERS,
            ops.MAX.__name__: types.NUMBERS,
            ops.AVG.__name__: types.SUMMABLE,
            ops.SUM.__name__: types.SUMMABLE,
            ops.COUNT.__name__: types.ALL,
            ops.SUBSTRING.__name__: types.STRING,

            # Constant Operators
            ops.SELECT.__name__: types.ALL,
            ops.ASC.__name__: types.ALL,
            ops.DESC.__name__: types.ALL,
            ops.ALL.__name__: types.ALL,

            Column.__name__: types.ALL
        }

        if operator.__class__.__name__ not in accepted_types:
            raise ValueError("{} is not implemented in SQLite".format(operator.__class__.__name__))

        return accepted_types[operator.__class__.__name__]

    def execute(self, sql) -> pd.DataFrame:
        return pd.read_sql_query(sql, self.conn)

    def get_tables(self):
        sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='{}'
        AND table_catalog = current_database()
        AND table_type='BASE TABLE'
        """.format(self.schema)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        table_names = list(map(lambda name: name[0], cursor.fetchall()))

        tables = []
        for name in table_names:
            columns = self.get_columns(name)
            tables.append(Table(name, self.schema, lambda: self, False, *columns))

        return tables

    def get_columns(self, table):
        cursor = self.conn.cursor()
        cursor.execute(f'select * from {self.schema}.{table} limit 1')
        col_names = [description[0] for description in cursor.description]

        col_types = []
        for row in cursor:
            for col in row:
                col_types.append(type(col))
            break

        columns = []
        for name, dtype in zip(col_names, col_types):
            if not name.startswith("_"):
                columns.append(self.ops.Column(name, dtype))

        return columns

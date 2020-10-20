from psycopg2._psycopg import InterfaceError

from pandasdb.connections.sql.sql_connection import SQLConnection

import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor
from pandasdb.connections.sql.providers.postgress.operations import SupportedOps
from pandasdb.utils import ID
from pandasdb.record import Record
from functools import lru_cache


class PostgresConnection(SQLConnection):
    ops = SupportedOps()

    def __init__(self, name, host, schema, username, password, database, type, port=5432, tunnel=None,
                 ssh_username=None,
                 ssh_key=None):
        SQLConnection.__init__(self, name=name, host=host, schema=schema, username=username, password=password,
                               port=port,
                               database=database, tunnel=tunnel, ssh_username=ssh_username, ssh_key=ssh_key, type=type)
        self.reserved_words = ["INDEX"]

    def connect(self):
        try:
            return psycopg2.connect(user=self.username,
                                    password=self.password,
                                    host=self.host,
                                    port=self.port,
                                    database=self.database)
        except Exception as exp:
            raise ConnectionError("Could not connect to database. Error: {}".format(exp))

    # @staticmethod
    # def accepted_types(self, operator):
    #     accepted_types = {
    #         # Numeric Operators
    #         ops.ADD.__name__: types.NUMBERS,
    #         ops.SUB.__name__: types.NUMBERS,
    #         ops.MUL.__name__: types.NUMBERS,
    #         ops.DIV.__name__: types.NUMBERS,
    #         ops.MOD.__name__: types.NUMBERS,
    #         ops.POW.__name__: types.NUMBERS,
    #
    #         # Logical Operators
    #         ops.AND.__name__: types.LOGICAL,
    #         ops.OR.__name__: types.LOGICAL,
    #         ops.NOT.__name__: types.LOGICAL,
    #
    #         # Comparison Operators
    #         ops.LT.__name__: types.NUMBERS,
    #         ops.GT.__name__: types.NUMBERS,
    #         ops.LE.__name__: types.NUMBERS,
    #         ops.GE.__name__: types.NUMBERS,
    #         ops.EQ.__name__: types.ALL,
    #         ops.NE.__name__: types.ALL,
    #
    #         # Higher level Operators
    #         ops.IN.__name__: types.ALL,
    #         ops.NOTIN.__name__: types.ALL,
    #         ops.LIKE.__name__: types.ALL,
    #         ops.LIMIT.__name__: types.NUMBERS,
    #         ops.OFFSET.__name__: types.NUMBERS,
    #         ops.ALIAS.__name__: types.IDENTIFIERS,
    #         ops.ORDER_BY.__name__: types.IDENTIFIERS,
    #         ops.GROUP_BY.__name__: types.IDENTIFIERS,
    #         ops.JOIN.__name__: types.IDENTIFIERS,
    #         ops.WHERE.__name__: types.IDENTIFIERS,
    #         ops.HAVING.__name__: types.IDENTIFIERS,
    #
    #         # Function Operators
    #         ops.MIN.__name__: types.NUMBERS,
    #         ops.MAX.__name__: types.NUMBERS,
    #         ops.AVG.__name__: types.SUMMABLE,
    #         ops.SUM.__name__: types.SUMMABLE,
    #         ops.COUNT.__name__: types.ALL,
    #         ops.SUBSTRING.__name__: types.STRING,
    #
    #         # Constant Operators
    #         ops.SELECT.__name__: types.ALL,
    #         ops.ASC.__name__: types.ALL,
    #         ops.DESC.__name__: types.ALL,
    #         ops.ALL.__name__: types.ALL,
    #
    #         Column.__name__: types.ALL
    #     }
    #
    #     if operator.__class__.__name__ not in accepted_types:
    #         raise ValueError("{} is not implemented in SQLite".format(operator.__class__.__name__))
    #
    #     return accepted_types[operator.__class__.__name__]

    def execute(self, sql) -> pd.DataFrame:
        try:
            return pd.read_sql_query(sql, self.conn)
        except InterfaceError:
            self._restart_connection()
            self.execute(sql)

    def stream(self, sql, batch_size):
        for record in self._execute_sql(sql, name=ID(), cursor_factory=DictCursor, itersize=batch_size):
            record = dict(record)
            for key in [key for key in record.keys()]:
                if key.startswith("_"):
                    record.pop(key)

            yield Record(**record)

    def _execute_sql(self, sql, name=None, itersize=2000, **kwargs):
        try:
            cursor = self.conn.cursor(name, **kwargs)
            cursor.itersize = itersize
            cursor.execute(sql)
            return cursor
        except Exception as exp:
            self.conn.rollback()
            raise Exception(exp)

    @lru_cache
    def get_tables(self, timeout=10):
        from pandasdb.table import Table

        sql = """
        SET statement_timeout = '{}s';
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='{}'
        AND table_catalog = current_database()
        AND table_type='BASE TABLE'
        """.format(timeout, self.schema)

        cursor = self._execute_sql(sql)

        table_names = list(map(lambda name: name[0], cursor.fetchall()))

        tables = []
        for name in table_names:
            if name.startswith("_"):
                continue

            columns = self.get_columns(name)
            tables.append(Table(name, self.schema, self, False, columns))

        return tables

    @lru_cache
    def get_columns(self, table, timeout=5):
        cursor = self._execute_sql(f"SET statement_timeout = '{timeout}s'; select * from {self.schema}.{table} limit 1")

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

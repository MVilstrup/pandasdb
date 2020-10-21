from functools import lru_cache

import pandas as pd
import psycopg2
from psycopg2._psycopg import InterfaceError
from psycopg2.extras import DictCursor

from pandasdb.connections.sql.providers.redshift.operations import SupportedOps
from pandasdb.connections.sql.sql_connection import SQLConnection
from pandasdb.record import Record
from pandasdb.utils import ID


class RedshiftConnection(SQLConnection):
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

    def get_tables(self, timeout=10):
        from pandasdb.table import Table

        sql = """
        select t.table_name
        from information_schema.tables t
        where t.table_schema = '{}' and t.table_type = 'BASE TABLE'
        and table_catalog = current_database()
        order by t.table_name;
        """.format(self.schema)
        print(sql)

        cursor = self._execute_sql(sql)
        tables = cursor.fetchall()
        print(tables)
        table_names = list(map(lambda name: name[0], tables))
        print(table_names)

        tables = []
        for name in table_names:
            if name.startswith("_"):
                continue

            columns = self.get_columns(name)
            tables.append(Table(name, self.schema, self, False, columns))

        return tables

    @lru_cache
    def get_columns(self, table, timeout=5):
        cursor = self._execute_sql(f"select * from {self.schema}.{table} limit 1")

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

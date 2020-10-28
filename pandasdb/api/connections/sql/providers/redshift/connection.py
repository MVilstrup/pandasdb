from functools import lru_cache

import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor
from sqlalchemy import create_engine

from pandasdb.connections.sql.providers.redshift.operations import SupportedOps
from pandasdb.connections.sql.sql_connection import SQLConnection
from pandasdb.record import Record
from pandasdb.sql.utils import ID


class RedshiftConnection(SQLConnection):

    def __init__(self, name, host, schema, username, password, database, type, port=5432, tunnel=None,
                 ssh_username=None,
                 ssh_key=None):
        SQLConnection.__init__(self, name=name, host=host, schema=schema, username=username, password=password,
                               port=port,
                               database=database, tunnel=tunnel, ssh_username=ssh_username, ssh_key=ssh_key, type=type)
        self.reserved_words = ["INDEX"]

    @property
    def ops(self):
        return SupportedOps()

    @property
    def _conn_func(self):
        return psycopg2.connect

    @property
    def _engine_func(self):
        def engine(user, password, host, port, database):
            return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}").connect()

        return engine

    def execute(self, sql) -> pd.DataFrame:
        with self.conn as conn:
            return pd.read_sql_query(sql, conn)

    def stream(self, sql, batch_size):
        for record in self._execute_sql(sql, name=ID(), cursor_factory=DictCursor, itersize=batch_size):
            record = dict(record)
            for key in [key for key in record.keys()]:
                if key.startswith("_"):
                    record.pop(key)

            yield Record(**record)

    def _execute_sql(self, sql, name=None, itersize=2000, **kwargs):
        with self.conn as conn:
            try:
                cursor = conn.cursor(name, **kwargs)
                cursor.itersize = itersize
                cursor.execute(sql)
                return cursor
            except Exception as exp:
                conn.rollback()
                raise Exception(exp)


    def get_tables(self, with_columns=True):
        from pandasdb.table import Table

        sql = """
        select t.table_name
        from information_schema.tables t
        where t.table_schema = '{}' and t.table_type = 'BASE TABLE'
        and table_catalog = current_database()
        order by t.table_name;
        """.format(self.schema)

        cursor = self._execute_sql(sql)
        tables = cursor.fetchall()
        table_names = list(map(lambda name: name[0], tables))
        if not with_columns:
            return table_names

        tables = []
        for name in table_names:
            if name.startswith("_") and not self._hidden_columns_included:
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
            if not (name.startswith("_") and not self._hidden_columns_included):
                columns.append(self.ops.Column(name, dtype))

        return columns


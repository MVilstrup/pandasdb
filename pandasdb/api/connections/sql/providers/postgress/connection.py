from functools import lru_cache

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from psycopg2.extras import DictCursor

from pandasdb.connections.sql.providers.postgress.operations import SupportedOps
from pandasdb.connections.sql.sql_connection import SQLConnection
from pandasdb.record import Record
from pandasdb.sql.utils import ID


class PostgresConnection(SQLConnection):

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
        def engine(user, password, host, port, database, **kwargs):
            return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}").connect()

        return engine

    def execute(self, sql) -> pd.DataFrame:
        with self.conn as conn:
            return pd.read_sql_query(sql, conn)

    def stream(self, sql, batch_size):
        with self.conn as conn:
            try:
                cursor = conn.cursor(ID(), cursor_factory=DictCursor)
                cursor.itersize = batch_size
                cursor.execute(sql)
                for record in cursor:
                    record = dict(record)
                    for key in [key for key in record.keys()]:
                        if key.startswith("_"):
                            record.pop(key)

                    yield Record(**record)
            except Exception as exp:
                conn.rollback()
                cursor.close()
                conn.commit()
                raise Exception(exp)

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

    @lru_cache
    def get_tables(self, with_columns=True):
        from pandasdb.table import Table

        sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='{}'
        AND table_catalog = current_database()
        AND table_type='BASE TABLE'
        """.format(self.schema)

        cursor = self._execute_sql(sql)

        table_names = list(map(lambda name: name[0], cursor.fetchall()))
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

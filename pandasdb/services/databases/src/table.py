from functools import partial, lru_cache

from pandasdb.communication.errors.io import PandasDBIOError
from pandasdb.communication.errors.validation import PandasDBDataValidationError
from pandasdb.libraries.overrides.src.lazy import LazyLoader
from pandasdb.libraries.overrides.src.representable import Representable
from sqlalchemy import Column
from sqlalchemy.engine import Connection
import pandas as pd

from pandasdb.libraries.pandas import convert_types


class Table(LazyLoader, Representable):

    def __init__(self, name, schema, is_view, should_setup=True, selected=None):
        LazyLoader.__init__(self, should_setup)
        Representable.__init__(self)

        self.name = name
        self.schema = schema
        self.is_view = is_view

        self._columns = None
        self._indexes = None
        self._primary_keys = None
        self._foreign_keys = None

        self._selected_columns = selected
        self._dublication_endpoints = []

        self._offset = None
        self._limit = None

    def __setup__(self, timeout=10):
        # @no:format
        columns = self.schema.database.inspect(lambda inspect: inspect.get_columns(self.name, self.schema.name), asynchronous=True)
        indexes = self.schema.database.inspect(lambda inspect: inspect.get_indexes(self.name, self.schema.name), asynchronous=True)
        primary_key = self.schema.database.inspect(lambda inspect: inspect.get_pk_constraint(self.name, self.schema.name), asynchronous=True)
        foreign_keys = self.schema.database.inspect(lambda inspect: inspect.get_foreign_keys(self.name, self.schema.name), asynchronous=True)


        try:
            self._columns = list(map(lambda conf: Column(conf.pop("name"), conf.pop("type"), **conf), columns.result(timeout)))
            self._indexes = indexes.result(timeout)
            self._primary_keys = primary_key.result(timeout)
            self._foreign_keys = foreign_keys.result(timeout)
        except TimeoutError:
            raise PandasDBIOError(f"Attempt to search {self.name} timed out. This is most likely a connection issue")
        # @do:format

        for column in self._columns:
            setattr(self, column.name, self.copy([column.name]))

    @property
    def columns(self):
        return [c.name for c in self._columns] if self._columns is not None else []

    @property
    def sql(self):
        full_table_name = f"{self.schema.database.name}.{self.schema.name}.{self.name}"
        columns = "*" if not self._selected_columns else ", ".join(self._selected_columns)
        limit = f"LIMIT {self._limit}" if self._limit is not None else ""
        offset = f"OFFSET {self._offset}" if self._offset is not None else ""

        return f"SELECT {columns} FROM {full_table_name} {limit} {offset}"

    @property
    def iloc(self):
        class ILOC:
            def __init__(self, table):
                self.table: Table = table

            def __getitem__(self, item):
                if isinstance(item, int):
                    offset = item if item >= 0 else len(self.table) + item
                    return self.table.copy(limit=1, offset=offset - 1)
                elif isinstance(item, slice):
                    start, stop = item.start, item.stop

                    offset, limit = None, None
                    if start is not None:
                        offset = start if start >= 0 else len(self.table) + start
                        offset -= 1

                    if stop is not None:
                        limit = stop if stop >= 0 else len(self.table) + stop

                    return self.table.copy(limit=limit, offset=offset)

        return ILOC(self)

    @property
    @lru_cache
    def size(self):
        full_table_name = f"{self.schema.database.name}.{self.schema.name}.{self.name}"
        return self.schema.database.do(lambda conn: conn.scalar(f"SELECT COUNT(*) FROM {full_table_name}"))

    def copy(self, columns=None, limit=None, offset=None):
        table = Table(self.name, self.schema, self.is_view, should_setup=False, selected=columns)
        table._columns = self._columns
        table._indexes = self._indexes
        table._primary_keys = self._primary_keys
        table._foreign_keys = self._foreign_keys
        table._limit = self._limit if not limit else limit
        table._offset = self._offset if not offset else offset
        return table

    def __validate__(self, df: pd.DataFrame):
        missing_columns = set(self.columns) - set(df.columns)
        if missing_columns:
            raise PandasDBDataValidationError(f"{missing_columns} not found in {df.columns}")

        return df[self.columns]

    def replace(self, df: pd.DataFrame):
        def upload(connection, schema):
            self.__validate__(df).to_sql(self.name,
                                         connection,
                                         schema=schema,
                                         if_exists="replace",
                                         index=False,
                                         chunksize=10000,
                                         method="multi")

        jobs = [self.schema.database.do(partial(upload, schema=self.schema.name), asynchronous=True)]

        for schema in self._dublication_endpoints:
            jobs.append(schema.database.do(partial(upload, schema=schema.name), asynchronous=True))

        for job in jobs:
            job.result()

    def __type_validation__(self, df):
        for column_name, dtype in zip(df.columns, df.dtypes):
            df[column_name] = convert_types(df[column_name], dtype)

        return df

    @lru_cache
    def head(self, limit=5):
        try:
            df = self.df(limit)
            return df if not isinstance(df, pd.Series) else df.to_frame()
        except:
            if self._columns:
                return pd.DataFrame({col.name: [pd.NA] for col in self._columns})

    def tail(self, limit=5):
        return self.iloc[-limit:].head()

    def df(self, limit=None, asynchronous=False):
        def read(connection: Connection):
            df = pd.read_sql(self.sql, connection)
            df = self.__type_validation__(df)
            return df if len(df.columns) > 1 else df[df.columns[0]]

        if limit is not None:
            return self.copy(limit=limit).df(asynchronous=asynchronous)
        else:
            return self.schema.database.do(read, asynchronous=asynchronous)

    def __len__(self):
        return self.size

    def __getitem__(self, target):
        self.__do_setup__()

        if isinstance(target, str):
            target = [target]
        elif isinstance(target, list):
            target = target
        else:
            raise KeyError(target)

        for column in target:
            assert column in self.columns, f"{column} not found in {self.schema.database.name}.{self.schema.name}.{self.name}"

        return self.copy(target)

    def __on_head_failure__(self):
        print(f"Could not load the data from {self.schema.database.name}.{self.schema.name}.{self.name}")
        print("This is most likely a connection or permission issue.")
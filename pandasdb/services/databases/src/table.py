from functools import partial

from pandasdb.libraries.overrides.src.lazy import LazyLoader
from pandasdb.libraries.overrides.src.representable import Representable
from sqlalchemy import Column
from sqlalchemy.engine import Connection
import pandas as pd


class PandasDBDataValidationError(object):
    pass


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

    def __setup__(self, timeout=10):
        # @no:format
        columns = self.schema.database.inspect(lambda inspect: inspect.get_columns(self.name, self.schema.name), asyncronous=True)
        indexes = self.schema.database.inspect(lambda inspect: inspect.get_indexes(self.name, self.schema.name), asyncronous=True)
        primary_key = self.schema.database.inspect(lambda inspect: inspect.get_pk_constraint(self.name, self.schema.name), asyncronous=True)
        foreign_keys = self.schema.database.inspect(lambda inspect: inspect.get_foreign_keys(self.name, self.schema.name), asyncronous=True)


        try:
            self._columns = list(map(lambda conf: Column(conf.pop("name"), conf.pop("type"), **conf), columns.result(timeout)))
            self._indexes = indexes.result(timeout)
            self._primary_keys = primary_key.result(timeout)
            self._foreign_keys = foreign_keys.result(timeout)
        except TimeoutError:
            raise PandasDBIOExeption(f"Attempt to search {self.name} timed out. This is most likely a connection issue")
        # @do:format

        for column in self._columns:
            setattr(self, column.name, self.copy([column.name]))

    @property
    def columns(self):
        return [c.name for c in self._columns] if self._columns is not None else []

    def copy(self, columns):
        table = Table(self.name, self.schema, self.is_view, should_setup=False, selected=columns)
        table._columns = self._columns
        table._indexes = self._indexes
        table._primary_keys = self._primary_keys
        table._foreign_keys = self._foreign_keys
        return table

    def __validate__(self, df: pd.DataFrame):
        missing_columns = set(self.columns) - set(df.columns)
        if missing_columns:
            raise PandasDBDataValidationError(f"{missing_columns} not found in {df.columns}")

        return df[self.columns]

    def head(self, limit=5):
        try:
            df = self.df(limit)
            return df if not isinstance(df, pd.Series) else df.to_frame()
        except:
            if self._columns:
                return pd.DataFrame({col.name: [pd.NA] for col in self._columns})

    def replace(self, df: pd.DataFrame):
        df = df.iloc[:1000]

        def upload(connection, schema):
            self.__validate__(df).to_sql(self.name,
                                         connection,
                                         schema=schema,
                                         if_exists="replace",
                                         index=False,
                                         chunksize=10000,
                                         method="multi")

        jobs = [self.schema.database.do(partial(upload, schema=self.schema.name), asyncronous=True)]

        for schema in self._dublication_endpoints:
            jobs.append(schema.database.do(partial(upload, schema=schema.name), asyncronous=True))

        for job in jobs:
            job.result()

    def df(self, limit=None):
        def read(connection: Connection):
            to_select = "*" if not self._selected_columns else ", ".join(self._selected_columns)
            read_limit = f" LIMIT {limit}" if limit is not None else ""
            SQL = f"SELECT {to_select} FROM {self.schema.database.name}.{self.schema.name}.{self.name} {read_limit}"
            df = pd.read_sql(SQL, connection)
            return df if len(df.columns) > 1 else df[df.columns[0]]

        return self.schema.database.do(read)

    def __getitem__(self, target):
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
        # @no:format
        print(f"Could not load the data from {self.schema.database.name}.{self.schema.name}.{self.name}. This is most likely a connection or permission issue.")
        # @do:format
from pandasdb.libraries.overrides.lazy import LazyLoader
from pandasdb.libraries.overrides.representable import Representable
from sqlalchemy.engine import Connection
import pandas as pd


class Table(LazyLoader, Representable):
    def __init__(self, name, schema, is_view):
        LazyLoader.__init__(self)
        Representable.__init__(self)

        self.name = name
        self.schema = schema
        self.is_view = is_view

    def __setup__(self):
        pass

    def __lt__(self, other):
        return self.name < other.name

    def head(self, limit=5):
        return self.df(limit)

    def df(self, limit=None):
        def read(connection: Connection):
            SQL = f"SELECT * FROM {self.schema.database.name}.{self.schema.name}.{self.name}"
            if limit is not None:
                SQL += f" LIMIT {limit}"

            return pd.read_sql(SQL, connection)

        return self.schema.database.do(read)

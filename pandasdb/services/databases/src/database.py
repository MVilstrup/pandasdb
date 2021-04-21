from pandasdb.communication.errors.io import PandasDBIOError
from pandasdb.libraries.configuration.src.database_config import DatabaseConfiguration
from pandasdb.libraries.connections.alchemy import AlchemyConnection
import pandas as pd

from pandasdb.libraries.overrides import LazyLoader, Representable
from pandasdb.services.databases.src.schema import Schema


class Database(AlchemyConnection, LazyLoader, Representable):

    def __init__(self, configuration: DatabaseConfiguration):
        AlchemyConnection.__init__(self, configuration)
        LazyLoader.__init__(self)
        Representable.__init__(self)

        self.name = configuration.database
        self.schemas = []

    def __setup__(self, timeout=10):
        # @no:format
        try:
            ignored = {"information_schema"}
            for schema in self.inspect(lambda inspector: inspector.get_schema_names(), timeout=timeout):
                if schema not in ignored:
                    setattr(self, schema, Schema(schema, database=self))
                    self.schemas.append(schema)
        except TimeoutError:
            raise PandasDBIOError(f"Attempt to search database {self.name} timed out. This is most likely a connection or permission issue")

        if not self.schemas:
            raise PandasDBIOError(f"Attempt to search database {self.name} failed to find any schemas. This is most likely a connection or permission issue")

        # @do:format

    def head(self):
        if self.schemas:
            return pd.DataFrame({"schemas": self.schemas})

    def __on_head_failure__(self):
        print(f"Failed to find any schemas in {self.name} for user {self.configuration.username}. This is most likely a connection or permission issue.")

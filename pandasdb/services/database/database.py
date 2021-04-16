from pandasdb.libraries.configuration.database_configuration import DatabaseConfiguration
from pandasdb.libraries.connections.alchemy import AlchemyConnection
from pandasdb.libraries.overrides.lazy import LazyLoader

from pandasdb.services.database.schema import Schema


class DataBase(AlchemyConnection, LazyLoader):

    def __init__(self, configuration: DatabaseConfiguration):
        AlchemyConnection.__init__(self, configuration)
        LazyLoader.__init__(self)

        self.name = configuration.database

    def __setup__(self):
        for schema in self.inspect(lambda inspector: inspector.get_schema_names()):
            setattr(self, schema, Schema(schema, database=self))

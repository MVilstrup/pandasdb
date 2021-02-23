from typing import List

from pandasdb.io.config.configuration import Configuration
from pandasdb.io.connection import PostgreSQLConnection, MySQLConnection, SQLiteConnection


class BaseConfiguration:

    def __init__(self, connections=None):
        self.connections = connections if connections else self._all_connections()
        for connection_info in self.connections:
            setattr(self, connection_info.schema, self._generate_connection(connection_info))

    @staticmethod
    def _parse_info(**info):
        return Configuration(**info)

    @staticmethod
    def _is_valid():
        raise NotImplementedError()

    @staticmethod
    def _all_connections() -> List[Configuration]:
        raise NotImplementedError()

    @staticmethod
    def _generate_connection(configuration: Configuration):
        def lazy_load():
            if configuration.type in ["POSTGRES", "REDSHIFT"]:
                db = PostgreSQLConnection(configuration)
            elif configuration.type == "MYSQL":
                db = MySQLConnection(configuration)
            elif configuration.type == "SQLITE":
                db = SQLiteConnection(configuration)
            else:
                raise ValueError(f"{configuration.type} not supported")

            return db

        return lazy_load

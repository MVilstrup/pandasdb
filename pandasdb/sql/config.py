import os
from pandasdb.sql.utils import json
from functools import partial
from pandasdb.sql.connection import PostgreSQLConnection, MySQLConnection, SQLiteConnection, PandasConnection


class Databases:
    _CONNECTION_PATH = os.path.expanduser(f"~{os.sep}.pandas_db{os.sep}connections.json")
    _CONFIG_PATH = os.path.expanduser(f"~{os.sep}.pandas_db{os.sep}config.json")

    def __init__(self, connections=None):
        if not connections:
            self.connections = self._ensure_file(Databases._CONNECTION_PATH)
            # self._conf = self._ensure_file(Databases._CONFIG_PATH)
        else:
            self.connections = connections
        for database_name, connection_info in self.connections.items():
            setattr(self, connection_info["schema"],
                    partial(self._generate_connection, name=database_name, info=connection_info))

        #setattr("IN_MEMORY", PandasConnection(name="IN_MEMORY"))

    @staticmethod
    def _ensure_file(name):
        if not os.path.exists(name):
            folder_path = os.sep.join(name.split(os.sep)[:-1])
            if not os.path.isdir(folder_path):
                os.makedirs(folder_path)

            existing_data = {}
        else:
            existing_data = json.load(open(name))

        return existing_data

    @staticmethod
    def _generate_connection(name, info):
        if "name" not in info:
            info["name"] = name

        db_type = info["type"]
        if db_type in ["POSTGRES", "REDSHIFT"]:
            return PostgreSQLConnection(**info)
        if db_type == "MYSQL":
            return MySQLConnection(**info)
        if db_type == "SQLITE":
            return SQLiteConnection(**info)


databases = Databases()

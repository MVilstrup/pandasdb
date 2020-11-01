import os
import json
from functools import partial
from pandasdb.sql.connection import PostgreSQLConnection, MySQLConnection, SQLiteConnection


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
            setattr(self, database_name, partial(self._generate_connection, name=database_name, info=connection_info))

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

    def _generate_connection(self, name, info):
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

# class DataBaseConfig:
#     def __init__(self, connections, update_func):
#         self.add = type("Add", (), {
#             "postgres": self._postgres,
#             "redshift": self._redshift,
#             "from_template": self._from_template
#         })
#
#         self._connections = connections
#         self.update_func = update_func
#
#     def rename(self, conn, name):
#         info = self._connections.pop(conn.name)
#         self._connections[name] = info
#         self.update_func()
#
#     def _from_template(self, conn, name, schema=None, username=None, password=None, host=None, database=None, port=None,
#                        ssh_key=None, tunnel=None, ssh_username=None):
#         kwargs = {
#             "name": name,
#             "type": conn.db_type,
#             "schema": schema if schema is not None else conn.schema,
#             "username": username if username is not None else conn.username,
#             "password": password if password is not None else conn.password,
#             "tunnel": tunnel if tunnel is not None else conn.tunnel,
#             "ssh_username": ssh_username if ssh_username is not None else conn.ssh_username,
#             "host": host if host is not None else conn.host,
#             "database": database if database is not None else conn.database,
#             "port": port if port is not None else conn.port,
#             "ssh_key": ssh_key if ssh_key is not None else conn.ssh_key,
#         }
#
#         self._add_db(**kwargs)
#
#     def _postgres(self, **kwargs):
#         from pandasdb._config import Config
#         self._add_db(Config.POSTGRES_TYPE, **kwargs)
#
#     def _redshift(self, **kwargs):
#         from pandasdb._config import Config
#         self._add_db(Config.REDSHIFT_TYPE, **kwargs)
#
#     def _add_db(self, type, name, schema, username, password, host, database, port, ssh_key=None, tunnel=None,
#                 ssh_username=None):
#         configuration = {
#             "name": name,
#             "type": type,
#             "schema": schema,
#             "username": username,
#             "password": password,
#             "tunnel": tunnel,
#             "ssh_username": ssh_username,
#             "host": host,
#             "database": database,
#             "port": port,
#             "ssh_key": ssh_key,
#         }
#
#         self._connections[name] = configuration
#         self.update_func()

import os
import os
import sys
import json

from pandasdb._config.add_dbs import DataBaseConfig
from pandasdb.connections import PostgresConnection, RedshiftConnection


class Config:
    POSTGRES_TYPE = "POSTGRES"
    REDSHIFT_TYPE = "REDSHIFT"
    CONNECTION_PATH = os.path.expanduser(f"~{os.sep}.pandas_db{os.sep}connections.json")
    CONFIG_PATH = os.path.expanduser(f"~{os.sep}.pandas_db{os.sep}config.json")

    def __init__(self):
        self.connections = self._ensure_file(Config.CONNECTION_PATH)
        self._conf = self._ensure_file(Config.CONFIG_PATH)
        for key, val in self._conf.items():
            setattr(self, key, val)

        self._update_module()
        self.databases = DataBaseConfig(self.connections, self._update_connections)

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

    def _update_connections(self):
        json.dump(self.connections, open(Config.CONNECTION_PATH, "w"), indent=4, sort_keys=True)
        self._update_module()

    def _update_module(self):
        if os.path.exists(Config.CONNECTION_PATH):
            def to_db(name, info):
                if "name" not in info:
                    info["name"] = name
                db_type = info["type"]
                if db_type == Config.POSTGRES_TYPE:
                    return PostgresConnection(**info)
                else:
                    return RedshiftConnection(**info)

            dbs = type("DataBaseList", (), {name: to_db(name, info) for name, info in self.connections.items()})
            setattr(sys.modules["pandasdb"], "Databases", dbs)


config = Config()

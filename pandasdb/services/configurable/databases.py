import os

from pandasdb.libraries.configuration.database_configuration import DatabaseConfiguration
from pandasdb.libraries.utils import json

type_mapper = {
    "POSTGRES": "postgresql",
    "REDSHIFT": "postgresql",
}


def database_configurations():
    CONNECTION_PATH = os.path.expanduser(f"~{os.sep}.pandas_db{os.sep}connections.json")

    def is_valid():
        if not os.path.exists(CONNECTION_PATH):
            return False
        try:
            json.load(open(CONNECTION_PATH))
        except:
            return False

        return True

    configurations = []
    if is_valid():
        for conf in json.load(open(CONNECTION_PATH)).values():
            conf["provider"] = type_mapper[conf.pop("type")]

            configurations.append((conf["database"], DatabaseConfiguration(**conf)))

    return configurations

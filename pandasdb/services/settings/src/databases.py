import json
import os

from pandasdb.libraries.configuration.src.database_config import DatabaseConfiguration
from pandasdb.libraries.configuration.src.tunnelled_config import TunnelledConfiguration

type_mapper = {
    "POSTGRES": "postgresql+psycopg2",
    "REDSHIFT": "postgresql+psycopg2",
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

    if is_valid():
        for conf in json.load(open(CONNECTION_PATH)).values():
            conf["provider"] = type_mapper[conf.pop("type")]

            conf_obj = DatabaseConfiguration(**conf)
            if conf.get("tunnel"):
                conf_obj = TunnelledConfiguration(conf_obj, **conf)

            yield conf["database"], conf_obj

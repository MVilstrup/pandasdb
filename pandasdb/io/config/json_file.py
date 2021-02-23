import os
from typing import List

from pandasdb.io.config.configuration import Configuration
from pandasdb.io.config.base import BaseConfiguration
from pandasdb.sql.utils import json


class JsonFileConfiguration(BaseConfiguration):
    _CONNECTION_PATH = os.path.expanduser(f"~{os.sep}.pandas_db{os.sep}connections.json")

    @staticmethod
    def _is_valid(self):
        if not os.path.exists(JsonFileConfiguration._CONNECTION_PATH):
            return False
        try:
            json.load(open(JsonFileConfiguration._CONNECTION_PATH))
        except:
            return False

        return True

    @staticmethod
    def _all_connections() -> List[Configuration]:
        configurations = []
        for conf in json.load(open(JsonFileConfiguration._CONNECTION_PATH)).values():
            configurations.append(Configuration(**conf))

        return configurations

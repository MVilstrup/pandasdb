from functools import partial

from pandasdb.services.configurable.databases import database_configurations
from pandasdb.services.database.database import DataBase


class Databases:
    def __init__(self):
        for name, configuration in database_configurations():
            setattr(self, name, DataBase(configuration))


databases = Databases()

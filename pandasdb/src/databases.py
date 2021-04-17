from pandasdb.services.settings import database_configurations
from pandasdb.services.databases import Database


class AllDatabases:
    def __init__(self):
        for name, configuration in database_configurations():
            setattr(self, name, Database(configuration))


databases = AllDatabases()
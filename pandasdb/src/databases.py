from pandasdb.services.databases import Database
from pandasdb.services.settings import Settings


class AllDatabases:
    def __init__(self):
        for name, configuration in Settings.database_settings():
            setattr(self, name, Database(configuration))


databases = AllDatabases()
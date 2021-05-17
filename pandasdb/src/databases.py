from pandasdb.services.databases import Database
from pandasdb.services.settings import Settings


class AllDatabases:
    def __init__(self):
        settings = Settings.database_settings()
        if settings is not None:
            for name, configuration in settings:
                setattr(self, name, Database(configuration))


databases = AllDatabases()

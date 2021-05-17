import os

from pandasdb.services.settings.src.databases import database_settings
from pandasdb.services.settings.src.sheets import sheets_settings


class Settings:
    HOME = os.path.expanduser(os.path.join("~", ".pandas_db"))
    PLUGINS = os.path.expanduser(os.path.join("~", ".pandas_db", "plugins"))

    """ PLUGINS """
    GSUITE = "gsuite"

    @staticmethod
    def ensure_home_folder():
        if not os.path.exists(Settings.HOME):
            try:
                os.mkdir(Settings.HOME)
                return True
            except OSError:
                return False
        else:
            return True

    @staticmethod
    def ensure_plugins_folder():
        Settings.ensure_home_folder()
        if not os.path.exists(Settings.PLUGINS):
            try:
                os.mkdir(Settings.PLUGINS)
                return True
            except OSError:
                return False
        else:
            return True

    @staticmethod
    def ensure_plugin(name):
        if Settings.ensure_plugins_folder():
            PLUGIN = os.path.join(Settings.PLUGINS, name)

            if not os.path.exists(PLUGIN):
                try:
                    os.mkdir(PLUGIN)
                    return True
                except OSError:
                    return False
            else:
                return True

    @staticmethod
    def database_settings():
        if Settings.ensure_home_folder():
            return database_settings()

    @staticmethod
    def sheets_settings():
        if Settings.ensure_plugins_folder() and Settings.ensure_plugin(Settings.GSUITE):
            return sheets_settings()

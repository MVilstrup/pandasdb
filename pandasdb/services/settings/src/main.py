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
            os.mkdir(Settings.HOME)

    @staticmethod
    def ensure_plugins_folder():
        Settings.ensure_home_folder()
        if not os.path.exists(Settings.PLUGINS):
            os.mkdir(Settings.PLUGINS)

    @staticmethod
    def ensure_plugin(name):
        Settings.ensure_plugins_folder()
        PLUGIN = os.path.join(Settings.PLUGINS, name)

        if not os.path.exists(PLUGIN):
            os.mkdir(PLUGIN)

    @staticmethod
    def database_settings():
        try:
            Settings.ensure_home_folder()
            return database_settings()
        except OSError:
            return None


    @staticmethod
    def sheets_settings():
        try:
            Settings.ensure_plugins_folder()
            Settings.ensure_plugin(Settings.GSUITE)
            return sheets_settings()
        except OSError:
            return None
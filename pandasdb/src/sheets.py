from pandasdb.libraries.connections.qsuite import GSuiteConnection
from pandasdb.services.settings import Settings

from pandasdb.services.sheets.src.google_sheets import GSheets


def initiate_sheets():
    # Ensure google folder

    settings = Settings.sheets_settings()
    if settings is not None:
        return GSheets(GSuiteConnection(settings["gsuite"]))


sheets = initiate_sheets()

from functools import partial
import pandas as pd

from pandasdb.libraries.connections.qsuite import GSuiteConnection
from pandasdb.libraries.overrides import LazyLoader, Representable
from pandasdb.libraries.utils import name_to_attr
from pandasdb.services.sheets.src.work_sheet import WorkSheet


class GSheets(LazyLoader, Representable):

    def __init__(self, gsuite: GSuiteConnection):
        LazyLoader.__init__(self)
        Representable.__init__(self)
        self._gsuite = gsuite
        self._sheets = {}

    def __setup__(self, timeout=10):
        for file_info in self._gsuite.do(lambda conn: conn.list_spreadsheet_files()):
            if file_info["kind"] == 'drive#file':
                name, attr_name = file_info["name"], name_to_attr(file_info["name"])
                setattr(self, attr_name, WorkSheet(name, partial(self._open, name)))
                self._sheets[attr_name] = name

    def _open(self, name):
        return self._gsuite.do(lambda conn: conn.open(name))

    def head(self):
        return pd.DataFrame({"sheets": self._sheets.keys(), "actual_names": self._sheets.values()})

    def __on_head_failure__(self):
        pass

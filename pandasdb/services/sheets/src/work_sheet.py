from pandasdb.libraries.overrides import LazyLoader
from pandasdb.libraries.utils import name_to_attr
import pandas as pd

from pandasdb.services.sheets.src.pane import Pane


class WorkSheet(LazyLoader):
    def __init__(self, name, worksheet_callable):
        LazyLoader.__init__(self)

        self.name = name
        self._panes = {}

        self._worksheet_callable = worksheet_callable
        self._worksheet = None

    def __setup__(self, timeout=10):
        self._worksheet = self._worksheet_callable()

        for pane in self._worksheet.worksheets():
            setattr(self, name_to_attr(pane.title), Pane(pane.title, pane))
            self._panes[name_to_attr(pane.title)] = pane.title

    def head(self):
        return pd.DataFrame({"panes": self._panes.keys(), "actual_names": self._panes.values()})

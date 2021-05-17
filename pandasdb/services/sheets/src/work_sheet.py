from pandasdb.libraries.overrides import LazyLoader
from pandasdb.libraries.utils import name_to_attr

from pandasdb.services.sheets.src.pane import Pane


class WorkSheet(LazyLoader):
    def __init__(self, name, worksheet_callable):
        LazyLoader.__init__(self)

        self.name = name
        self._panes = {}
        self._panes_order = []

        self._worksheet_callable = worksheet_callable
        self._worksheet = None

    def __setup__(self, timeout=10):
        self._worksheet = self._worksheet_callable()

        for pane in self._worksheet.worksheets():
            name = name_to_attr(pane.title)
            setattr(self, name, Pane(pane.title, pane))
            self._panes[name] = pane.title
            self._panes_order.append(name)

    def head(self, limit=5):
        return getattr(self, self._panes_order[0]).head(limit)

    def df(self):
        return getattr(self, self._panes_order[0]).df()

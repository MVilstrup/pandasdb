import pandas as pd

from pandasdb.libraries.overrides import LazyLoader, Representable


class Pane(LazyLoader, Representable):
    def __init__(self, name, gc_pane):
        LazyLoader.__init__(self)
        Representable.__init__(self)

        self.name = name
        self._pane = gc_pane

    def df(self):
        return pd.DataFrame(self._pane.get_all_records())

    def head(self):
        return self.df().head()

    def __setup__(self, timeout=10):
        pass


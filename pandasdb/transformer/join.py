import pandas as pd
from pandasdb.transformer import Combine


class JoinFlow:
    def __init__(self, apply_func):
        self._on_apply = apply_func

    def apply(self, df):
        self._on_apply(df)

    def __call__(self, df):
        return self._on_apply(df)


class _Join:

    def __init__(self):
        self._left = None
        self._left_result = None
        self._right = None
        self._right_result = None
        self._on = None
        self._how = None

    def _fill_left(self, df):
        self._left_result = self._left(df)
        return self

    def _fill_right(self, df):
        self._right_result = self._right(df)
        return self

    def on(self, on):
        self._on = on
        return self

    def how(self, how):
        self._on = how
        return self

    def left(self, *transformers):
        self._left = Combine(*transformers)
        return JoinFlow(self._fill_left)

    def right(self, *transformers):
        self._right = Combine(*transformers)
        return JoinFlow(self._fill_right)

    def compute(self):
        return self()

    def __call__(self):
        on_left, on_right = self._on if isinstance(self._on, (list, tuple)) else self._on, self._on

        self._left_result.set_index(on_left, inplace=True)
        self._right_result.set_index(on_right, inplace=True)

        return self._left_result.join(self._right_result, how=self._how)


class Join:
    _base = lambda: _Join()

    @staticmethod
    def left(*transformers):
        join = Join._base()
        return join.left(*transformers)

    @staticmethod
    def right(*transformers):
        join = Join._base()
        return join.right(*transformers)

    @staticmethod
    def on(on):
        join = Join._base()
        return join.on(on)

    @staticmethod
    def how(how):
        join = _Join()
        return join.how(how)


class _LeftJoin(_Join):
    def __init__(self):
        Join.__init__(self)
        self._how = "left"


class LeftJoin(Join):
    _base = lambda: _LeftJoin()


class _RightJoin(_Join):
    def __init__(self):
        Join.__init__(self)
        self._how = "left"


class RightJoin(Join):
    _base = lambda: _RightJoin()


class _InnerJoin(_Join):
    def __init__(self):
        Join.__init__(self)
        self._how = "left"


class InnerJoin(Join):
    _base = lambda: _InnerJoin()

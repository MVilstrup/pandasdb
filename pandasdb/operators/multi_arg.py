from copy import deepcopy

from pandasdb.operators.operator import Operator
from pandasdb.operators.value import Value
from pandasdb.utils import maybe_copy


class MultiArgOperator(Operator):
    def __init__(self, columns, supported_ops, symbol, format=lambda x: x):
        Operator.__init__(self, supported_ops, symbol, format)
        if not isinstance(columns, list):
            columns = [columns]
        self.columns = self._ops.Value(columns)

    def copy(self):
        return MultiArgOperator(list(map(maybe_copy, self.columns)), supported_ops=self._ops, symbol=self.symbol, format=self.format)

    @property
    def children(self):
        return self.columns

    def add_accept_types(self, accept_func):
        for column in self.columns:
            try:
                column.add_accept_types(accept_func)
            except:
                pass

        self.accepts = accept_func(self)

    def __str__(self):
        symbol = self.symbol if self.symbol is not None else self.__class__.__name__
        return self.format(f"{symbol} {' '.join(map(str, self.columns))}")

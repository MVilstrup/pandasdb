from pandasdb.operators.operator import Operator
from pandasdb.sql.utils import maybe_copy


class MultiArgOperator(Operator):
    def __init__(self, columns, supported_ops, symbol, format=lambda x: x):
        Operator.__init__(self, supported_ops, symbol, format)
        if not (isinstance(columns, list) or isinstance(columns, tuple)):
            columns = [columns]

        self.columns = columns

    def copy(self):
        return MultiArgOperator(list(map(maybe_copy, self.columns)), supported_ops=self._ops, symbol=self.symbol,
                                format=self.format)

    @property
    def children(self):
        return self.columns

    def __str__(self):
        symbol = self.symbol if self.symbol is not None else self.__class__.__name__
        return self.format(f"{symbol} {', '.join(map(str, self.columns))}")

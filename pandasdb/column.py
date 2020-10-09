from pandasdb.operators import Operator, IN, NOT_IN, LIKE, SUBSTRING, Value
import pandasdb.functions as funcs
from pandasdb.utils import iterable


class Column(Operator):

    def __init__(self, name, dtype, supported_ops, symbol):
        self.name = name
        self.dtype = dtype
        self.table = None
        Operator.__init__(self, supported_ops, symbol)

    @property
    def full_name(self):
        return f"{self.table.full_name}.{self.name}" if self.table is not None else self.name

    def min(self):
        return self.table.select(funcs.min(self._ops, self))

    def mean(self):
        return self.table.select(funcs.mean(self._ops, self))

    def avg(self):
        return self.mean()

    def max(self):
        return self.table.select(funcs.max(self._ops, self))

    def count(self):
        return self.table.select(funcs.count(self._ops, self))

    def sum(self):
        return self.table.select(funcs.sum(self._ops, self))

    def df(self):
        table = self.table.select(self)
        return table.df()

    def __str__(self):
        return self.full_name

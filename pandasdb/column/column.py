from copy import deepcopy

from pandasdb.operators import Operator
import pandasdb.functions as funcs
from pandasdb.utils import AutoComplete
from collections import defaultdict


class Column(Operator):

    def __init__(self, name, dtype, supported_ops, symbol):
        self.name = name
        self.dtype = dtype
        self.table = None
        Operator.__init__(self, supported_ops, symbol)

    def copy(self):
        return Column(name=self.name, dtype=self.dtype, supported_ops=self._ops, symbol=self.symbol)

    @property
    def length(self):
        return self.table.length

    @property
    def sql(self):
        return self.table.select(self).sql

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

    def head(self, n=5):
        return self.table.select(self).head(n)

    def take(self, amount, offset):
        return self.table.select(self).take(amount, offset)

    def df(self):
        table = self.table.select(self)
        return table.df()

    def __str__(self):
        return self.full_name

from pandasdb.operators.operator import Operator
from pandasdb.sql.utils import iterable, maybe_copy


class Value(Operator):

    def __init__(self, right, supported_ops, symbol, format):
        Operator.__init__(self, supported_ops, symbol, format)
        self.right = right
        self.dtype = type(right) if not hasattr(right, "dtype") else right.dtype

    def copy(self):
        return Value(right=maybe_copy(self.right), supported_ops=self._ops, symbol=self.symbol, format=self.format)

    @property
    def children(self):
        children = [self.right]
        if hasattr(self.right, "children"):
            children += self.right.children
        return children

    def __iter__(self):
        if iterable(self.right):
            return iter(self.right)

    def __str__(self):
        return f"{self.format(self.right)}"

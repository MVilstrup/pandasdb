from pandasdb.operators.operator import Operator
from pandasdb.utils import iterable


class Value(Operator):

    def __init__(self, right, supported_ops, symbol, format=lambda x: x):
        Operator.__init__(self, supported_ops, symbol, format)
        self.right = right
        self.dtype = type(right)

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

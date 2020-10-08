from pandasdb.operators.operator import Operator
from pandasdb.operators.value import Value


class UnaryOperator(Operator):

    def __init__(self, right, supported_ops, symbol, format=lambda x: x):
        Operator.__init__(self, supported_ops, symbol, format)
        self.right = self._ops.Value(right)  # self.check_dtype(right)
        self.dtype = type(right)

    @property
    def children(self):
        children = [self.right]
        if hasattr(self.right, "children"):
            children += self.right.children
        return children

    def add_accept_types(self, accept_func):
        try:
            self.right.add_accept_types(accept_func)
        except:
            pass
        self.accepts = accept_func(self)

    def __str__(self):
        symbol = self.symbol if self.symbol is not None else self.__class__.__name__
        return self.format(f"{symbol} {self.right}")
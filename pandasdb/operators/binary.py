from pandasdb.operators.operator import Operator


class BinaryOperator(Operator):

    def __init__(self, left, right, supported_ops, symbol, format=lambda x: x, **kwargs):
        Operator.__init__(self, supported_ops, symbol, format)
        self.left, self.right = self._ops.Value(left), self._ops.Value(right)

    @property
    def children(self):
        children = []
        if self.left:
            children += [self.left]
            if hasattr(self.left, "children"):
                children += self.left.children

        if self.right:
            children += [self.right]
            if hasattr(self.right, "children"):
                children += self.right.children

        return children

    def add_accept_types(self, accept_func):
        children = [self.left, self.right]
        for child in children:
            try:
                child.add_accept_types(accept_func)
            except:
                pass

        self.accepts = accept_func(self)

    def __str__(self):
        symbol = self.symbol if self.symbol is not None else self.__class__.__name__

        return self.format("{} {} {}".format(self.left, symbol, self.right))


from copy import deepcopy

from pandasdb.operators.operator import Operator
from pandasdb.utils import iterable, maybe_copy


class FunctionOperator(Operator):

    def __init__(self, arguments, supported_ops, symbol, format=lambda x: x):
        Operator.__init__(self, supported_ops, symbol, format)

        if not issubclass(type(arguments), Operator) and not iterable(arguments):
            arguments = [arguments]

        self.arguments = arguments

    def copy(self):
        return FunctionOperator(list(map(maybe_copy, self.arguments)), supported_ops=self._ops, symbol=self.symbol, format=self.format)

    @property
    def children(self):
        children = []
        for arg in self.arguments:
            children += [arg] + arg.children
        return children

    def add_accept_types(self, accept_func):
        for argument in self.arguments:
            try:
                argument.add_accept_types(accept_func)
            except:
                pass

        self.accepts = accept_func(self)

    def __str__(self):
        symbol = self.symbol if self.symbol is not None else self.__class__.__name__
        return self.format(f"{symbol}({self.arguments})")

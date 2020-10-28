from pandasdb.operators.operator import Operator


class ConstantOperator(Operator):

    def copy(self):
        _class = type(self)
        return _class(symbol=self.symbol, supported_ops=self._ops)

    def __str__(self):
        return self.format(self.symbol if self.symbol else self.__class__.__name__)
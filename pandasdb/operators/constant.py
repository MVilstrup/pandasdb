from pandasdb.operators.operator import Operator


class ConstantOperator(Operator):
    def __str__(self):
        return self.format(self.symbol if self.symbol else self.__class__.__name__)
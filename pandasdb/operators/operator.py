from copy import deepcopy


class Operator:

    def __init__(self, supported_ops, symbol, format=lambda x: x):
        self.left = None
        self.right = None
        self._ops = supported_ops
        self.symbol = symbol
        self.format = format

    def copy(self):
        assert NotImplementedError("copy should be implemented by children")

    def get_type(self, value):
        try:
            return value.dtype
        except:
            return type(value)

    def check_dtype(self, *types):
        err_str = "{} operation ({}) does not accept input: {} of type: {}"

        if len(types) > 1:
            return types
        else:
            return types[0]

    def _format_left(self, x):
        return x

    def _format_right(self, x):
        return x

    def add_accept_types(self, accept_func):
        self.accepts = accept_func(self)

    @property
    def children(self):
        return [self.left, self.right]

    def includes(self, operator):
        return any([isinstance(child, operator) for child in self.children]) or isinstance(self, operator)

    def cast(self, alias):
        return self.alias(alias)

    def alias(self, alias):
        return self._ops.ALIAS(self, alias)

    def __add__(self, other):
        return self._ops.ADD(self, other)

    def __radd__(self, other):
        return self._ops.ADD(other, self)

    def __sub__(self, other):
        return self._ops.SUB(self, other)

    def __rsub__(self, other):
        return self._ops.SUB(other, self)

    def __truediv__(self, other):
        return self._ops.DIV(self, other)

    def __rtruediv__(self, other):
        return self._ops.DIV(other, self)

    def __and__(self, other):
        return self._ops.AND(self, other)

    def __rand__(self, other):
        return self._ops.AND(other, self)

    def __or__(self, other):
        return self._ops.OR(self, other)

    def __ror__(self, other):
        return self._ops.OR(other, self)

    def __invert__(self):
        return self._ops.NOT(self)

    def __lt__(self, other):
        return self._ops.LT(self, other)

    def __rlt__(self, other):
        return self._ops.LT(other, self)

    def __le__(self, other):
        return self._ops.LE(self, other)

    def __rle__(self, other):
        return self._ops.LE(other, self)

    def __eq__(self, other):
        return self._ops.EQ(self, other)

    def __req__(self, other):
        return self._ops.EQ(other, self)

    def __ne__(self, other):
        return self._ops.NE(self, other)

    def __rne__(self, other):
        return self._ops.NE(other, self)

    def __ge__(self, other):
        return self._ops.GE(self, other)

    def __rge__(self, other):
        return self._ops.GE(other, self)

    def __gt__(self, other):
        return self._ops.GT(self, other)

    def __rgt__(self, other):
        return self._ops.GT(other, self)

    def __repr__(self):
        return str(self)

    def __dir__(self):
        res = dir(type(self)) + list(self.__dict__.keys())
        return res

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        return self.format(self.str_operator if self.str_operator else "{}".format(self.__class__.__name__))

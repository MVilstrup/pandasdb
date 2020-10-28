from functools import partial
import numpy as np
import pandasdb.operators as ops
from pandasdb.column import Column
from pandasdb.sql.utils import iterable


def handle_values(value):
    if isinstance(value, str):
        return f"'{value}'"

    if iterable(value):
        if isinstance(value, dict):
            return f"{tuple(value.keys())}"

        if not isinstance(value, Column):
            return f"({', '.join([handle_values(v) for v in value])})"

    return f"{value}"


class SupportedOps:

    def VALUE_OR_OPERATION(self, val):
        checks = [isinstance(val, t) for t in [int, float, bool, str, np.ndarray, list, dict, tuple, set]]
        if any(checks):
            return partial(ops.Value, symbol="VALUE", supported_ops=self, format=handle_values)(val)
        else:
            return val

    def __init__(self):
        supported_ops = {
            # Numeric Operators
            ops.ADD.__name__: partial(ops.ADD, symbol="+", supported_ops=self),
            ops.SUB.__name__: partial(ops.SUB, symbol="-", supported_ops=self),
            ops.MUL.__name__: partial(ops.MUL, symbol="*", supported_ops=self),
            ops.DIV.__name__: partial(ops.DIV, symbol="/", supported_ops=self),
            ops.POW.__name__: partial(ops.POW, symbol="^", supported_ops=self),
            ops.MOD.__name__: partial(ops.MOD, symbol="%", supported_ops=self),

            # Logical Operators
            ops.AND.__name__: partial(ops.AND, symbol="AND", supported_ops=self, format=lambda x: f"({x})"),
            ops.OR.__name__: partial(ops.OR, symbol="OR", supported_ops=self, format=lambda x: f"({x})"),
            ops.NOT.__name__: partial(ops.NOT, symbol="~", supported_ops=self),

            # Comparison Operators
            ops.LT.__name__: partial(ops.LT, symbol="<", supported_ops=self),
            ops.GT.__name__: partial(ops.GT, symbol=">", supported_ops=self),
            ops.LE.__name__: partial(ops.LE, symbol="<=", supported_ops=self),
            ops.GE.__name__: partial(ops.GE, symbol=">=", supported_ops=self),
            ops.EQ.__name__: partial(ops.EQ, symbol="=", supported_ops=self),
            ops.NE.__name__: partial(ops.NE, symbol="!=", supported_ops=self),

            # Higher level Operators
            ops.IN.__name__: partial(ops.IN, symbol="IN", supported_ops=self),
            ops.NOT_IN.__name__: partial(ops.NOT_IN, symbol="NOT IN", supported_ops=self),
            ops.LIKE.__name__: partial(ops.LIKE, symbol="LIKE", supported_ops=self),
            ops.NOT_LIKE.__name__: partial(ops.LIKE, symbol="NOT LIKE", supported_ops=self),

            ops.ALIAS.__name__: partial(ops.ALIAS, symbol="AS", supported_ops=self),

            ops.GROUP_BY.__name__: partial(ops.GROUP_BY, symbol="GROUP BY", supported_ops=self),
            ops.JOIN.__name__: partial(ops.JOIN, symbol="JOIN", supported_ops=self),
            ops.WHERE.__name__: partial(ops.WHERE, symbol="WHERE", supported_ops=self),
            ops.HAVING.__name__: partial(ops.HAVING, symbol="HAVING", supported_ops=self),

            # Function Operators
            ops.MIN.__name__: partial(ops.MIN, symbol="MIN", supported_ops=self),
            ops.MAX.__name__: partial(ops.MAX, symbol="MAX", supported_ops=self),
            ops.AVG.__name__: partial(ops.AVG, symbol="AVG", supported_ops=self),
            ops.SUM.__name__: partial(ops.SUM, symbol="SUM", supported_ops=self),
            ops.COUNT.__name__: partial(ops.COUNT, symbol="COUNT", supported_ops=self),
            ops.SUBSTRING.__name__: partial(ops.SUBSTRING, symbol="SUBSTRING", supported_ops=self),

            # Constant Operators
            ops.SELECT.__name__: partial(ops.SELECT, symbol="SELECT", supported_ops=self),
            ops.ALL.__name__: partial(ops.ALL, symbol="*", supported_ops=self),
            ops.Value.__name__: self.VALUE_OR_OPERATION,
            ops.JSON.__name__: partial(ops.JSON, symbol="->", supported_ops=self),

            # Meta operators
            ops.ORDER_BY.__name__: partial(ops.ORDER_BY, symbol="ORDER BY", supported_ops=self),
            ops.ASC.__name__: partial(ops.ASC, symbol="ASC", supported_ops=self),
            ops.DESC.__name__: partial(ops.DESC, symbol="DESC", supported_ops=self),
            ops.LIMIT.__name__: partial(ops.LIMIT, symbol="LIMIT", supported_ops=self),
            ops.OFFSET.__name__: partial(ops.OFFSET, symbol="OFFSET", supported_ops=self),
            ops.DISTINCT_ON.__name__: partial(ops.DISTINCT_ON, symbol="SELECT DISTINCT ON", supported_ops=self),

            # Column
            Column.__name__: partial(Column, symbol="VALUE", supported_ops=self)
        }

        for name, operation in supported_ops.items():
            setattr(self, name, operation)

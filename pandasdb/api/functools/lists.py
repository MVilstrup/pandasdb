from pandasdb.column import Column
from pandasdb.sql.utils import iterable, curr_func


def is_in(column, values):
    func = curr_func()
    assert isinstance(column, Column), f"{func} can only be applied to Columns"
    assert iterable(values), "values should be a list"

    return column._ops.IN(column, column._ops.Value(values))


def not_in(column, values):
    func = curr_func()
    assert isinstance(column, Column), f"{func} can only be applied to Columns"
    assert iterable(values), "values should be a list"

    return column._ops.NOT_IN(column, column._ops.Value(values))


def all(*conditions):
    all = conditions[0]
    for condition in conditions:
        all = all._ops.AND(all.condition)

    return all


def any(*conditions):
    any = conditions[0]
    for condition in conditions:
        any = any._ops.OR(any.condition)

    return any

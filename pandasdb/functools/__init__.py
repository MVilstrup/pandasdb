from pandasdb.column import Column
from pandasdb.utils import iterable, curr_func


def is_in(column, values):
    assert isinstance(column, Column), f"{curr_func()} can only be applied to Columns"
    assert iterable(values), "values should be a list"

    return column._ops.IN(column, column._ops.Value(values))


def not_in(column, values):
    assert isinstance(column, Column), f"{curr_func()} can only be applied to Columns"
    assert iterable(values), "values should be a list"

    return column._ops.NOTIN(column, column._ops.Value(values))


def like(column, pattern):
    assert isinstance(column, Column), f"{curr_func()} can only be applied to Columns"
    assert isinstance(pattern, str), "pattern should be a string"

    return column._ops.LIKE(column, column._ops.Value(pattern))


def starts_with(column, pattern):
    assert isinstance(column, Column), f"{curr_func()} can only be applied to Columns"
    assert isinstance(pattern, str), "pattern should be a string"

    return column._ops.LIKE(column, column._ops.Value("{}%".format(pattern)))


def ends_with(column, pattern):
    assert isinstance(column, Column), f"{curr_func()} can only be applied to Columns"
    assert isinstance(pattern, str), "pattern should be a string"

    return column._ops.LIKE(column, column._ops.Value("%{}".format(pattern)))


def substr(column, start, length):
    assert isinstance(column, Column), f"{curr_func()} can only be applied to Columns"

    return column._ops.SUBSTRING(column, column._ops.Value(start), column._ops.Value(length))
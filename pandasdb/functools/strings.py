from pandasdb import Column
from pandasdb.utils import curr_func


def like(column, pattern):
    assert isinstance(column, Column), f"{curr_func()} can only be applied to Columns"
    assert isinstance(pattern, str), "pattern should be a string"

    return column._ops.LIKE(column, column._ops.Value(pattern))


def not_like(column, pattern):
    assert isinstance(column, Column), f"{curr_func()} can only be applied to Columns"
    assert isinstance(pattern, str), "pattern should be a string"

    return column._ops.NOT_LIKE(column, column._ops.Value(pattern))


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

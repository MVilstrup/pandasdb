def mean(ops, *columns):
    if not columns:
        raise ValueError("mean() requires at least one test_column")
    if len(columns) == 1:
        return ops.AVG(columns[0])

    return [ops.AVG(column) for column in columns]


def avg(ops, *columns):
    return mean(ops, *columns)


def max(ops, *columns):
    if not columns:
        raise ValueError("max() requires at least one test_column")

    if len(columns) == 1:
        return ops.MAX(columns[0])

    return [ops.MAX(column) for column in columns]


def min(ops, *columns):
    if not columns:
        raise ValueError("min() requires at least one test_column")

    if len(columns) == 1:
        return ops.MIN(columns[0])

    return [ops.MIN(column) for column in columns]


def sum(ops, *columns):
    if not columns:
        raise ValueError("sum() requires at least one test_column")

    if len(columns) == 1:
        return ops.SUM(columns[0])

    return [ops.SUM(column) for column in columns]


def count(ops, *columns):
    if not columns:
        raise ValueError("count() requires at least one test_column")

    if len(columns) == 1:
        return ops.COUNT(columns[0])

    return [ops.COUNT(column) for column in columns]

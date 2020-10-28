from pandasdb.table import Table
from pandasdb.column import Column


def latest_occurrence(table: Table, distinct_col: Column, time_col: Column):
    # @no:format
    return (table.distinct_on(distinct_col)
                 .select(*table.Columns)
                 .order_by(distinct_col,
                           time_col,
                           ascending=False))
    # @do:format


def first_occurrence(table: Table, distinct_col: Column, time_col: Column):
    # @no:format
    return (table.distinct_on(distinct_col)
                 .select(*table.Columns)
                 .order_by(distinct_col,
                           time_col,
                           ascending=True))
    # @do:format
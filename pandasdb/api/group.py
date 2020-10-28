import pandasdb.functions as func
from pandasdb.operators import GROUP_BY


class GroupedData:
    def __init__(self, table, *columns):
        self.table = table
        self.ops = self.table.ops
        self.columns = list(columns)

    @staticmethod
    def _ensure_list(columns):
        if not isinstance(columns, list):
            return [columns]
        return columns

    def apply(self):
        raise NotImplementedError("apply(*columns) is not implemented yet")

    def agg(self, *expressions):
        selected_columns = self.columns + self._ensure_list(expressions)
        table = self.table.select(*selected_columns)
        table.groups += [self.ops.GROUP_BY(*self.columns)]
        return table

    def mean(self, *columns):
        selected_columns = self.columns + self._ensure_list(func.avg(*columns))
        table = self.table.select(*selected_columns)
        table.groups += [self.ops.GROUP_BY(*self.columns)]
        return table

    def avg(self, *columns):
        return self.mean(*columns)

    def max(self, *columns):
        selected_columns = self.columns + self._ensure_list(func.max(*columns))
        table = self.table.select(*selected_columns)
        table.groups += [self.ops.GROUP_BY(*self.columns)]
        return table

    def min(self, *columns):
        selected_columns = self.columns + self._ensure_list(func.min(*columns))
        table = self.table.select(*selected_columns)
        table.groups += [self.ops.GROUP_BY(*self.columns)]
        return table

    def sum(self, *columns):
        selected_columns = self.columns + self._ensure_list(func.sum(*columns))
        table = self.table.select(*selected_columns)
        table.groups += [self.ops.GROUP_BY(*self.columns)]
        return table

    def count(self, *columns):
        selected_columns = self.columns + self._ensure_list(func.count(*columns))
        table = self.table.select(*selected_columns)
        table.groups += [self.ops.GROUP_BY(*self.columns)]
        return table

from ibis.expr.api import GroupedTableExpr
from pandasdb.sql.table import Table


class GroupedData:

    def __init__(self, ibis_grouped_data: GroupedTableExpr):
        self._group = ibis_grouped_data

    def aggr(self, **kwargs):
        return Table(self._group.aggregate(**kwargs))

    def count(self):
        return Table(self._group.count())

    def having(self, expr):
        return GroupedData(self._group.having(expr))

    def order_by(self, expr):
        return GroupedData(self._group.order_by(expr))

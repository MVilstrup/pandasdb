from ibis.expr.api import GroupedTableExpr
from pandasdb.sql.table import Table


class GroupedData:

    def __init__(self, ibis_grouped_data: GroupedTableExpr, table_name, connection):
        self._group = ibis_grouped_data
        self._table_name = table_name
        self._connection = connection

    def aggr(self, **kwargs):
        return Table(self._table_name, self._group.aggregate(**kwargs), self._connection)

    def count(self):
        return Table(self._table_name, self._group.count(), self._connection)

    def having(self, expr):
        return GroupedData(self._group.having(expr))

    def order_by(self, expr):
        return GroupedData(self._group.order_by(expr))

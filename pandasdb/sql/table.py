from functools import lru_cache

from ibis.expr.api import TableExpr
from pandasdb.sql.column import Column
import sqlparse

from pandasdb.sql.plot.graph import draw_graph
from pandasdb.sql.stream import Stream
from pandasdb.sql.utils import AutoComplete
from pandasdb.sql.utils.table_graph import recursive_copy
import networkx as nx


class Table:

    def __init__(self, name, ibis_table, connection):
        self.table_name = name
        self._connection = connection
        self._table: TableExpr = ibis_table
        self._limit_execute = None
        self._orderings = []

        columns = {column_name: Column(self._table.get_column(column_name), self) for column_name in
                   self._table.columns}
        self.columns = AutoComplete("columns", columns)

        for name, column in columns.items():
            setattr(self, name, column)

    @property
    def sql(self):
        return sqlparse.format(str(self._table.compile()), reindent=True, keyword_case='upper')

    def update(self, **kwargs):
        """
        Convenience function for table projections involving adding columns
        :param kwargs:
        :return:
        """
        return Table(self.table_name, self._table.mutate(**kwargs), self._connection)

    @property
    def schema(self):
        return self._table.schema()

    def _execute(self):
        return self._table.execute(self._limit_execute)

    def filter(self, predicts):
        return Table(self.table_name, self._table.filter(predicts), self._connection)

    def where(self, predicts):
        return self.filter(predicts)

    def rename(self, columns):
        return Table(self.table_name, self._table.relabel(columns), self._connection)

    def count(self):
        return self._table.count()._execute()

    def order_by(self, column_direction_pairs):
        sort_pairs = []
        for column, sort in column_direction_pairs:
            if isinstance(sort, str):
                direction = "a" in sort
                sort_pairs.append((column, direction))
            else:
                sort_pairs.append((column, bool(sort)))

        return Table(self.table_name, self._table.sort_by(sort_pairs), self._connection)

    def __len__(self):
        return self.count()

    def distinct(self):
        return self._table.distinct()

    def limit(self, n, offset=0):
        return Table(self.table_name, self._table.limit(n, offset), self._connection)

    def head(self, n=5):
        return self.limit(n).df()

    def groupby(self, *columns):
        from pandasdb.sql.grouped_data import GroupedData
        return GroupedData(self._table.group_by(*columns))

    def group_by(self, *columns):
        return self.groupby(*columns)

    def stream(self):
        return Stream.from_df(self.df())

    def df(self):
        return self._execute()

    def __getitem__(self, item):
        if isinstance(item, list) or isinstance(item, tuple):
            return Table(self.table_name, self._table[list(map(str, item))], self._connection)

        if isinstance(item, slice):
            return Table(self._table[item])

        return Column(self._table[str(item)], self)

    def __hash__(self):
        return hash(self.table_name)

    def __eq__(self, other):
        return self.table_name == other.table_name

    def graph(self, degree=1, width=16, height=8, draw=True):
        G = self._get_graph(degree)

        if draw:
            draw_graph(G, (width, height))
        else:
            return G

    @lru_cache
    def _get_graph(self, degree):
        G = nx.DiGraph()
        recursive_copy(from_graph=self._connection.graph(show=False),
                       to_graph=G,
                       node=self.table_name,
                       d=degree)
        return G

    def __setitem__(self, name, expr):
        self._table = self._table.mutate(**{name: expr})

    def _repr_html_(self):
        return self.head().to_html()

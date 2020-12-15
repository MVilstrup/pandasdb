from datetime import datetime
from functools import lru_cache
from types import FunctionType

import ibis
from ibis.expr.api import TableExpr
from pandas.api.types import infer_dtype

from pandasdb.sql.column import Column
import sqlparse
import pandas as pd

from pandasdb.sql.history import History
from pandasdb.sql.plot.graph import draw_graph
from pandasdb.sql.record import Record
from pandasdb.sql.stream import Stream
from pandasdb.sql.utils import AutoComplete, json
from pandasdb.sql.utils.table_graph import recursive_copy
import networkx as nx
from pandas.io.json import build_table_schema
from ibis import schema


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

    def select(self, *args):
        return self[args]

    def drop(self, *args):
        to_select = []

        for column in self.columns:
            if not any([str(arg) == str(column) for arg in args]):
                to_select.append(column)

        return self.select(*to_select)

    @property
    def schema(self):
        return self._table.schema()

    def _execute(self):
        try:
            return self._table.execute(self._limit_execute)
        except ValueError:
            return self._table.execute()

    def filter(self, predicts):
        if isinstance(predicts, FunctionType):
            resulting_expr = predicts(self)
            return Table(self.table_name, self._table.filter(resulting_expr), self._connection)

        return Table(self.table_name, self._table.filter(predicts), self._connection)

    def join_df(self, right, on, right_on=None, how="inner"):
        from pandasdb import Async

        if right_on is None:
            right_on = on

        left_df = Async.execute(self.df)
        if not isinstance(right, pd.DataFrame):
            right_df = Async.execute(right.df).result()
        else:
            right_df = right_on
        left_df = left_df.result()

        full_df = pd.merge(left_df, right_df, left_on=on, right_on=right_on, how=how)

        return Table.from_df(full_df)

    def left_join_df(self, right, on, right_on=None):
        if right_on is None:
            right_on = on

        return self.join_df(right, on, right_on, how="left")

    def inner_join_df(self, right, on, right_on=None):
        if right_on is None:
            right_on = on

        return self.join_df(right, on, right_on, how="inner")

    def right_join_df(self, right, on, right_on=None):
        if right_on is None:
            right_on = on

        return self.join_df(right, on, right_on, how="right")

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
        return self.limit(n).df(parse_json=True)

    def groupby(self, *columns):
        from pandasdb.sql.grouped_data import GroupedData
        return GroupedData(self._table.group_by(*columns), self.table_name, self._connection)

    def group_by(self, *columns):
        return self.groupby(*columns)

    def stream(self):
        return Stream(self)

    def changes(self, on_column, freq="1d", as_table=False):
        return History(self.df(), on_column=on_column, freq=freq, as_table=as_table)

    def df(self, parse_json=True):
        if not parse_json:
            return self._execute()

        else:
            df = self._execute()
            for string_column in df.select_dtypes(include='object').columns:
                df[string_column] = df[string_column].apply(lambda s: json.loads(s))

            return df

    def compute(self):
        return Table.from_df(self.df())

    def latest(self, group_column, date_column):
        df = self.df()
        return Table.from_df(df.loc[df.groupby(group_column)[date_column].idxmax()])

    def first(self, group_column, date_column):
        df = self.df()
        return Table.from_df(df.loc[df.groupby(group_column)[date_column].idxmin()])

    def interpolate(self, col_name, freq, start_col, end_col, NaT_val=datetime.now()):
        """

        :param col_name:
        :param freq:
        :param start_col:
        :param end_col:
        :param NaT_val:
        :return:
        """

        def _interpolate(row):
            _rows = []

            start, end = row[start_col], row[end_col]

            if pd.isna(start):
                start = NaT_val
            if pd.isna(end):
                end = NaT_val

            dates = pd.date_range(pd.to_datetime(start, utc=True), pd.to_datetime(end, utc=True), freq=freq)
            for new_time in dates:
                new_row = row.copy()

                if new_time < pd.to_datetime(start, utc=True):
                    new_time = pd.to_datetime(start, utc=True)
                elif new_time > pd.to_datetime(end, utc=True) or new_time == dates[-1]:
                    new_time = pd.to_datetime(end, utc=True)

                new_row[col_name] = new_time
                _rows.append(new_row)

            return _rows

        all_rows = []
        for interpolated_rows in map(_interpolate, self.df().to_dict(orient="records")):
            all_rows += interpolated_rows

        new_df = pd.DataFrame(all_rows)
        new_df = new_df.sort_values([col_name])
        return Table.from_df(new_df)

    @staticmethod
    def from_df(df):
        for column in df.columns:
            if infer_dtype(df[column]) == "mixed-integer":
                df[column] = df[column].map(str)

            col = df[df[column].notnull()].values
            if len(col) > 0 and (isinstance(col[0], dict) or isinstance(col[0], list)):
                df[column] = df[column].apply(lambda x: json.dumps(x) if x else x)

        type_dict = {
            "integer": "int64",
            "number": "float",
            "datetime": "timestamp"
        }
        names, types = [], []
        for column in build_table_schema(df)["fields"]:
            names.append(column["name"])
            types.append(type_dict.get(column["type"], column["type"]))

        name = "MOCK"
        conn = None
        return Table(name, ibis.pandas.connect({'df': df}).table("df", schema=schema(names=names, types=types)), conn)

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

    def __iter__(self):
        return iter([Record(**row) for row in self.df().to_dict(orient="records")])

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

    def __str__(self):
        return self.table_name

    def _repr_html_(self):
        return self.head().to_html()

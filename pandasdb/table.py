from collections import defaultdict

from pandasdb.connections.query import Query
from pandasdb.stream import Stream
from pandasdb.utils import string_to_python_attr, iterable, AutoComplete
from pandasdb.group import GroupedData
import pandas as pd
from pandasdb.column import Column
import networkx as nx
from pandasdb.plot.graph import draw_graph
from pandasdb.utils.table_graph import recursive_copy


class Table:

    def __init__(self, name, schema, connection, encapsulate_name, columns):
        """
        :param name:
        :param connection:
        :param columns:
        """
        self._name = name
        self.schema = schema
        self.encapsulate_name = encapsulate_name
        self.connection = connection
        self.ops = self.connection.ops

        self.query: Query = self.connection.QueryClass(ops=self.ops,
                                                       action=self.ops.SELECT(),
                                                       columns=[self.ops.ALL()],
                                                       table=self.full_name)

        # Add the columns as attibutes to the table to make them easier to access
        self._columns = []
        for column in columns:
            if column.table is None:
                column.table = self

            self._columns.append(column)

        self.Columns = AutoComplete("Columns", {string_to_python_attr(col.name): col for col in self._columns})

        dtypes = defaultdict(list)
        for col in self._columns:
            dtypes[str(col.dtype.__name__).capitalize()].append(col)

        for dtype, columns in dtypes.items():
            setattr(self, f"{dtype}Columns",
                    AutoComplete(f"{dtype}Columns", {string_to_python_attr(col.name): col for col in columns}))

    @property
    def full_name(self):
        name = f"{self.schema}.{self._name}"
        return name if not self.encapsulate_name else f'"{name}"'

    @property
    def name(self):
        name = f"{self._name}"
        return name if not self.encapsulate_name else f'"{name}"'

    @classmethod
    def _copy(cls, name, schema, connections, encapsulate_name, columns, query):
        """
        :param name:
        :param connection:
        :param columns:
        :param action:
        :param target_columns:
        :param joins:
        :param groups:
        :param where:
        :param having:
        :param meta:
        :return:
        """

        table = Table(name, schema, connections, encapsulate_name, columns)
        table.query = query.copy()
        return table

    def copy(self):
        """

        :return:
        """
        return Table._copy(self._name, self.schema, self.connection,
                           self.encapsulate_name, self._columns, self.query)

    @property
    def columns(self):
        """

        :return:
        """
        return list(map(lambda d: d.name, self._columns))

    @property
    def dtypes(self):
        """

        :return:
        """
        return list(map(lambda d: d.dtype, self._columns))

    @property
    def length(self):
        """

        :return:
        """
        new = self.copy()
        new.query.select(self.ops.COUNT(self.ops.ALL()))
        return new.df()

    @property
    def sql(self):
        """

        :return:
        """
        return str(self.query)

    def _has_column(self, column):
        """

        :param column:
        :return:
        """
        if isinstance(column, Column) and not hasattr(self.Columns, string_to_python_attr(column.name)):
            return False
        elif isinstance(column, str) and not any([column == c.name for c in self._columns]):
            return False
        return True

    def _execute(self):
        """

        :return:
        """
        return self.connection.execute(self.sql)

    def _select(self, *columns):
        """

        :param columns:
        :return:
        """
        new = self.copy()
        new.query.select(*columns)
        return new

    def select(self, *columns: str):
        """

        :param columns:
        :return:
        """
        approved_columns = []

        all_columns = []
        for col in columns:
            if isinstance(col, list) or isinstance(col, tuple):
                all_columns += list(col)
            else:
                all_columns.append(col)

        for column in all_columns:
            if self._has_column(column) or isinstance(column, Column):
                approved_columns.append(column)
            else:
                raise ValueError("table: {} do not have a test_column named: {}".format(self.name, column))

        return self._select(*approved_columns)

    def select_expr(self, *expressions: str):
        """

        :param expressions: Either columns or SQL expressions
        :return: pandasdb.Table
        """
        approved_expressions = []
        for expr in expressions:
            if issubclass(type(expr), self.ops.Operator):
                if self._has_column(expr) or isinstance(expr, Column):
                    approved_expressions.append(expr)
                else:
                    raise ValueError("table: {} do not have a test_column named: {}".format(self.name, expr))
            elif isinstance(expr, str):
                approved_expressions.append(expr)
            else:
                raise ValueError("The expressions should either be columns or strings")

        return self._select(*expressions)

    def take(self, amount, offset):
        new = self.copy()
        new.query.limit(amount)
        new.query.offset(offset)
        return new

    def where(self, condition):
        return self.filter(condition)

    def filter(self, condition):
        """

        :param condition:
        :return:
        """
        new = self.copy()
        new.query.where(condition)
        return new

    def head(self, n=5):
        """

        :param n:
        :return:
        """
        new = self.copy()
        new.query.limit(n)
        return new.df()

    def order_by(self, column, ascending=True):
        """

        :param column:
        :param ascending:
        :return:
        """
        return self.sort(column, ascending)

    def sort(self, *columns, ascending=True):
        """

        :param column:
        :param ascending:
        :return:
        """
        for column in columns:
            if not self._has_column(column):
                raise ValueError("table: {} do not have a test_column named: {}".format(self.name, column))

        new = self.copy()
        new.query.order_by(*columns, ascending=ascending)
        return new

    # def group_by(self, *columns):
    #     return GroupedData(self, *columns)

    def join(self, on_table, on_column=None, from_column="id", kind="LEFT"):
        """

        :param on_table:
        :param with_column:
        :param on_column:
        :param kind:
        :return:
        """
        if isinstance(on_table, str):
            try:
                on_table = getattr(self.connection.Tables, string_to_python_attr(on_table))
            except AttributeError:
                raise ValueError(f"{on_table} not found in {self.schema}")

        if isinstance(on_column, str):
            try:
                on_column = getattr(on_table.Columns, string_to_python_attr(on_column))
            except AttributeError:
                raise ValueError(f"{on_column} not found in table {on_table.name}")
        elif isinstance(on_column, None):
            try:
                graph = self.graph(degree=1, draw=False)
                edge = graph.edges[on_table.name, self.name]
                return self.join(on_table, on_column, from_column=edge["from"], kind=kind)
            except:
                raise ValueError(f"Please could not automatically detect 'on_column'")

        if isinstance(from_column, str):
            try:
                from_column = getattr(self.Columns, string_to_python_attr(from_column))
            except AttributeError:
                raise ValueError(f"{from_column} not found in table {self.name}")

        new = self.copy()
        new._columns += on_table._columns
        new.Columns = AutoComplete("Columns", {string_to_python_attr(col.name): col for col in new._columns})
        new.query.join(self._ops.JOIN(kind=kind,
                                      table_a=self,
                                      column_a=from_column,
                                      table_b=on_table,
                                      column_b=on_column))
        return new

    def df(self):
        """

        :return:
        """
        df = self._execute()

        # If there is only one result in the dataframe, it is returned as a constant
        if len(df.columns) == 1 and len(df.values) == 1:
            constant = df.values[0]
            if iterable(constant):
                return constant[0]
            return constant

        if df.values.shape[1] == 1:
            return pd.Series(map(lambda el: el[0], df.values), name=df.columns[0])

        # In all other cases it is returned as a dataframe
        return df

    def stream(self):
        return Stream(self.connection, str(self.query), self.length)

    def __str__(self):
        return self.sql

    def graph(self, degree=1, width=16, height=8, draw=True):
        G = nx.DiGraph()
        recursive_copy(from_graph=self.connection.graph(show=False),
                       to_graph=G,
                       node=self.name,
                       d=degree)
        if draw:
            draw_graph(G, (width, height))
        else:
            return G

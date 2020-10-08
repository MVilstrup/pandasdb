from pandasdb.utils import string_to_python_attr, type_check, iterable, AutoComplete
from pandasdb.group import GroupedData
from copy import deepcopy
import pandas as pd
from pandasdb.column import Column
import pandasdb.operators  as ops


class Table:

    def __init__(self, name, schema, get_connection, encapsulate_name, *columns):
        """

        :param name:
        :param connection:
        :param columns:
        """
        self._name = name
        self.schema = schema
        self.encapsulate_name = encapsulate_name
        self._get_connection = get_connection
        self._ops = self._get_connection().ops

        # Add the columns as attibutes to the table to make them easier to access
        self._columns = []
        for column in columns:
            if column.table is None:
                column.table = self

            self._columns.append(column)

        COLs = AutoComplete("Columns", {string_to_python_attr(col.name): col for col in self._columns})
        setattr(self, "COL", COLs)

        self.db = None
        self._query = {
            "action": self._ops.SELECT(),
            "columns": self._ops.ALL(),
            "where": None,
            "having": None,
            "joins": [],
            "groups": [],
            "meta": []
        }

    @property
    def connection(self):
        return self._get_connection()

    @property
    def full_name(self):
        name = f"{self.schema}.{self._name}"
        return name if not self.encapsulate_name else f'"{name}"'

    @property
    def name(self):
        name = f"{self._name}"
        return name if not self.encapsulate_name else f'"{name}"'

    @classmethod
    def _copy(cls, name, schema, get_connection, encapsulate_name, columns, query):
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

        table = Table(name, schema, get_connection, encapsulate_name, *columns)
        table._query = deepcopy(query)
        return table

    def copy(self):
        """

        :return:
        """
        return Table._copy(self._name, self.schema, self._get_connection,
                           self.encapsulate_name, self._columns, self._query)

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

    # @property
    # def length(self):
    #     """
    #
    #     :return:
    #     """
    #     new = self.copy()
    #     new.target_columns = [self._ops.COUNT(self._ops.ALL())]
    #     return new.df()["COUNT(*)"].values[0]

    @property
    def query(self):
        """

        :return:
        """
        query = deepcopy(self._query)

        where = self._query["where"]
        query["where"] = self._ops.WHERE(where) if where else ""

        having = self._query["having"]
        query["having"] = self._ops.HAVING(having) if having else ""

        query["table"] = self.full_name

        return self.connection.query(**query)

    def _has_column(self, column):
        """

        :param column:
        :return:
        """
        if isinstance(column, Column) and not hasattr(self.COL, string_to_python_attr(column.name)):
            return False
        elif isinstance(column, str) and not any([column == c.name for c in self._columns]):
            return False
        return True

    def _add_db(self, db):
        """

        :param db:
        :return:
        """
        self.db = db

    def _execute(self):
        """

        :return:
        """
        return self.connection.execute(self.query)

    def _select(self, *columns):
        """

        :param columns:
        :return:
        """
        new = self.copy()
        new._query["columns"] = columns
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
            if self._has_column(column):
                approved_columns.append(column)
            else:
                raise ValueError("table: {} do not have a test_column named: {}".format(self.name, column))

        return self._select(*approved_columns)

    # def select_expr(self, *expressions: str):
    #     """
    #
    #     :param expressions: Either columns or SQL expressions
    #     :return: pandasdb.Table
    #     """
    #     approved_expressions = []
    #     for expr in expressions:
    #         if issubclass(type(expr), self._get_connection().ops.Operator):
    #             if self._has_column(expr):
    #                 approved_expressions.append(expr)
    #             else:
    #                 raise ValueError("table: {} do not have a test_column named: {}".format(self.name, expr))
    #         elif isinstance(expr, str):
    #             approved_expressions.append(expr)
    #         else:
    #             raise ValueError("The expressions should either be columns or strings")
    #
    #     return self._select(*expressions)

    def take(self, amount, offset):
        new = self.copy()
        new._query["meta"] += [self._ops.LIMIT(amount), self._ops.OFFSET(offset)]
        return new

    def where(self, condition):
        return self.filter(condition)

    def filter(self, condition):
        """

        :param condition:
        :return:
        """
        new = self.copy()

        having_ops = [ops.AVG, ops.COUNT, ops.MIN, ops.MAX, ops.SUM]
        if any([condition.includes(ops) for ops in having_ops]):
            if len(condition.children > 2):
                raise ValueError(
                    "PandasDB cannot handle a mix of having and where filters. Use multiple filters to seperate them")
            else:
                if new._query["having"] is not None:
                    new._query["having"] = self._ops.AND(self._query["having"], condition)
                else:
                    new._query["having"] = condition

        if new._query["where"] is not None:
            new._query["where"] = self._ops.AND(self._query["where"], condition)
        else:
            new._query["where"] = condition

        return new

    def head(self, n=5):
        """

        :param n:
        :return:
        """
        new = self.copy()
        new._query["meta"] += [self._ops.LIMIT(n)]
        return new

    def order_by(self, column, ascending=True):
        """

        :param column:
        :param ascending:
        :return:
        """
        return self.sort(column, ascending)

    def sort(self, column, ascending=True):
        """

        :param column:
        :param ascending:
        :return:
        """
        if not self._has_column(column):
            raise ValueError("table: {} do not have a test_column named: {}".format(self.name, column))

        new = self.copy()
        order = self._ops.ASC() if ascending else self._ops.DESC()
        new._query["meta"] += [self._ops.ORDER_BY(column), order]

        return new

    # def group_by(self, *columns):
    #     return GroupedData(self, *columns)

    # def join(self, on_table, with_column, on_column, kind="LEFT"):
    #     """
    #
    #     :param on_table:
    #     :param with_column:
    #     :param on_column:
    #     :param kind:
    #     :return:
    #     """
    #     # Check the database has both the tables
    #     if not self.db._has_table(on_table):
    #         raise ValueError("Database do not have a table named: {}".format(on_table))
    #
    #     # Check the table has the test_column
    #     if not self._has_column(with_column):
    #         raise ValueError("table: {} do not have a test_column named: {}".format(self.name, with_column))
    #
    #     if isinstance(on_table, str):
    #         on_table = getattr(self.db, string_to_python_attr(on_table))
    #
    #     new = self.copy()
    #
    #     if new._columns == new.target_columns:
    #         new._query["columns"] += on_table._columns
    #
    #     new._columns += on_table._columns
    #
    #     new._query["joins"] += [self._ops.JOIN(kind=kind,
    #                                            table_a=self.name,
    #                                            column_a=with_column,
    #                                            table_b=on_table.name,
    #                                            column_b=on_column)]
    #     return new

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

    def __str__(self):
        return self.query

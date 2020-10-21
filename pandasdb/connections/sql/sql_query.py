import re

import sqlparse

from pandasdb.connections.query import Query
from pandasdb.utils.misc import string_if


class SQLQuery(Query):

    def select(self, *columns):
        self._columns = columns

    def where(self, condition):
        if self._where:
            self._where.right = self.ops.AND(self._where.right, condition)
        else:
            self._where = self.ops.WHERE(condition)

    def join(self, statement):
        self._joins.append(statement)

    def group_by(self, groups):
        self._groups.append(groups)

    def order_by(self, *columns, ascending=True):
        order = self.ops.ASC() if ascending else self.ops.DESC()
        self._order_by = [self.ops.ORDER_BY(*columns), order]

    def limit(self, amount: int):
        self._limit = self.ops.LIMIT(amount)

    def offset(self, amount: int):
        self._offset = self.ops.OFFSET(amount)

    def optimize(self):
        pass

    def __str__(self):
        self.optimize()

        sql = [
            string_if(self._action),
            string_if(self._columns, formatter=", "),
            "FROM",
            string_if(self._table),
            string_if(self._joins, formatter=" "),
            string_if(self._where),
            string_if(self._groups, formatter=" "),
            string_if(self._order_by, formatter=" "),
            string_if(self._offset),
            string_if(self._limit),
        ]

        formatted_sql = re.sub(r"\s+", " ", " ".join(sql)).strip()
        return sqlparse.format(formatted_sql, reindent=True, keyword_case='upper')

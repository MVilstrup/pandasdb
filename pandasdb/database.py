from pandasdb.utils import string_to_python_attr
from pandasdb.table import Table


class DataBase:

    def __init__(self, connection):
        """

        :param connection:
        """
        self._tables = connection.tables

        for table in self._tables:
            setattr(self, string_to_python_attr(table.name), table)
            table.db = self

    @property
    def tables(self):
        """

        :return:
        """
        return sorted(list(map(lambda d: string_to_python_attr(d.name), self._tables)))

    def _has_table(self, table):
        """

        :param table:
        :return:
        """
        if isinstance(table, Table) and not hasattr(self, string_to_python_attr(table.name)):
            return False
        elif isinstance(table, str) and not any([table == c.name for c in self._tables]):
            return False

        return True

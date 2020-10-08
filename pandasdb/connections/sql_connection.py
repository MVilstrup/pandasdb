from pandasdb.connections.connection import Connection
import pandasdb.operators as ops
import re
import sqlparse


class SQLConnection(Connection):

    def optimize(self, action, target_columns, table_name, joins, where, groups, having, meta):
        return action, target_columns, table_name, joins, where, groups, having, meta

    def handle_reserved(self, columns):

        def check(column):
            try:
                if column.name.upper() in self.reserved_words:
                    return "`{}`".format(column.name)
                else:
                    return str(column)
            except:
                return "`{}`".format(column) if str(column) in self.reserved_words else str(column)

        if not isinstance(columns, list):
            return check(columns)

        return list(map(check, columns))

    def query(self, action, columns, table, joins, where, groups, having, meta):
        def to_string(formatter, arr):
            return formatter.join(list(map(str, arr)))

        # Run all arguments through an optimization loop, before creating the sql
        args = action, columns, table, joins, where, groups, having, meta
        action, target_columns, table, joins, where, groups, having, meta = self.optimize(*args)

        sql = " ".join([
            str(action),
            to_string(", ", self.handle_reserved(target_columns)),
            "FROM",
            f"{table}",
            to_string(" ", joins),
            f"{where}" if where else "",
            to_string(" ", groups),
            str(having) if having else "",
            to_string(" ", meta)
        ])

        return sqlparse.format(re.sub(r"\s+", " ", sql).strip(), reindent=True, keyword_case='upper')

    @staticmethod
    def enclose_string_value(value):
        if value is None:
            return ""
        elif isinstance(value, str):
            return "'{}'".format(value)
        else:
            return str(value)

    @staticmethod
    def dict_representation(operation, left=None, right=None, arguments=None):
        return None

    @staticmethod
    def binary_operation(operation, left=None, right=None, arguments=[], enclose_string=True):
        if enclose_string:
            left = SQLConnection.enclose_string_value(left)
            right = SQLConnection.enclose_string_value(right)

        if operation in ["AND", "OR"]:
            return f"({left} {operation} {right})"
        else:
            return f"{left} {operation} {right}"

    @staticmethod
    def unary_operation(operation, right=None, arguments=[], enclose_string=True):
        if enclose_string:
            right = SQLConnection.enclose_string_value(right)

        return f"{operation} {right}"

    @staticmethod
    def functional_operation(operation=None, left=None, right=None, arguments=None, enclose_string=True):
        if enclose_string:
            arguments = list(map(SQLConnection.enclose_string_value, arguments))
        return "{ops}({arguments})".format(ops=operation, arguments=", ".join(arguments))

    @staticmethod
    def multi_arg_operation(operation=None, left=None, right=None, arguments=None, enclose_string=False):
        if enclose_string:
            arguments = list(map(SQLConnection.enclose_string_value, arguments))
        else:
            arguments = list(map(str, arguments))

        return "{ops} {arguments}".format(ops=operation, arguments=", ".join(arguments))

    @staticmethod
    def command_operation(operation=None, left=None, right=None, arguments=None, enclose_string=False):
        if enclose_string:
            arguments = list(map(SQLConnection.enclose_string_value, arguments))
        else:
            arguments = list(map(str, arguments))

        return "{ops} {arguments}".format(ops=operation, arguments=" ".join(arguments))

    def get_tables(self):
        raise NotImplementedError("get_tables() should be implemented by all children")

    def get_columns(self, table):
        raise NotImplementedError("get_columns(table) should be implemented by all children")

    def connect(self):
        raise NotImplementedError("connect() should be implemented by all children")

    def accepted_types(self, operator):
        raise NotImplementedError("accepted_types(operator) should be implemented by all children")

    def execute(self, action, target_columns, table_name, joins, where, groups, having, meta):
        raise NotImplementedError(
            "execute( action, target_columns, table_name, joins, where, groups, having, meta) should be implemented by all children")

from pandasdb.operators.operator import Operator


class JoinOperator(Operator):
    def __init__(self, kind, table_a, column_a, table_b, column_b, supported_ops, symbol, format=lambda x: x):
        Operator.__init__(self, supported_ops, symbol, format)
        self.kind = kind
        self.table_a = table_a
        self.table_b = table_b
        self.column_a = column_a
        self.column_b = column_b



    @property
    def children(self):
        return []

    def __str__(self):
        column_a = "{}.{}".format(self.table_a, self.column_a) if str(self.table_a) not in str(
            self.column_a) else self.column_a
        column_b = "{}.{}".format(self.table_b, self.column_b) if str(self.table_b) not in str(
            self.column_b) else self.column_b
        return self.format("{kind} {operator} {table_b} ON {column_a} = {column_b}".format(kind=self.kind,
                                                                                           operator=self.symbol,
                                                                                           column_a=column_a,
                                                                                           table_b=self.table_b,
                                                                                           column_b=column_b))

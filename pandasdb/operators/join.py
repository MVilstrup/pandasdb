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
        return self.format(f"{self.kind} {self.symbol} {self.table_b.full_name} ON {self.column_a} = {self.column_b}")

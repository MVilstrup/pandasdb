import networkx as nx
from pandasdb.libraries.overrides.lazy import LazyLoader
from pandasdb.services.database.table import Table


class Schema(LazyLoader):
    def __init__(self, name, database):
        super().__init__()
        self.name = name
        self.database = database
        self.tables = []

        self.graph = None
        self.__has_setup__ = False

    def __setup__(self):
        graph_future = self.database.inspect(lambda inspect: inspect.get_sorted_table_and_fkc_names(schema=self.name),
                                             asyncronous=True)
        tables_future = self.database.inspect(lambda inspect: inspect.get_table_names(schema=self.name),
                                              asyncronous=True)
        views_future = self.database.inspect(lambda inspect: inspect.get_view_names(schema=self.name), asyncronous=True)

        for name in tables_future.result():
            setattr(self, name, Table(name=name, schema=self, is_view=False))
            self.tables.append(name)

        for name in views_future.result():
            setattr(self, name, Table(name=name, schema=self, is_view=True))
            self.tables.append(name)

        self.tables = sorted(self.tables)
        self.graph = self._generate_graph(graph_future)

    def _generate_graph(self, schema_info):
        graph = nx.DiGraph()
        return schema_info


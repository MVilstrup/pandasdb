from functools import lru_cache

import ibis
from dbflow.configuration import Configuration

from pandasdb.io.connection.connection_wrapper import ConnectionWrapper
from pandasdb.sql.plot.graph import draw_graph
from pandasdb.sql.table import Table
from pandasdb.sql.utils.table_graph import generate_graph

ibis.options.interactive = True


class Connection:
    __connections__ = {}

    def __init__(self, connection_func, configuration: Configuration):
        from pandasdb import Async

        self._connection = connection_func
        self.configuration = configuration
        self._loaded = False

        # @no:format
        table_names = self.conn.list_tables(schema=self.configuration.schema)
        self._tables = {name: table for name, table in zip(table_names, Async.map_wait(self._create_table, table_names))}
        # @do:format

        for name, table in self._tables.items():
            setattr(self, name, table)

    @property
    def _conn_key(self):
        return "_".join(map(str, [
            self.configuration.host,
            self.configuration.username,
            self.configuration.password,
            self.configuration.port,
            self.configuration.database,
            self.configuration.tunnel
        ]))

    @property
    def conn(self):
        if self._conn_key not in self.__connections__:
            self.__connections__[self._conn_key] = ConnectionWrapper(self._connection,
                                                                     self.configuration.type,
                                                                     host=self.configuration.host,
                                                                     username=self.configuration.username,
                                                                     password=self.configuration.password,
                                                                     port=self.configuration.port,
                                                                     database=self.configuration.database,
                                                                     tunnel=self.configuration.tunnel,
                                                                     ssh_username=self.configuration.ssh_username,
                                                                     ssh_key=self.configuration.ssh_key)
        return self.__connections__[self._conn_key]

    @property
    def tables(self):
        return list(self._tables.keys())

    def refresh(self):
        if self._conn_key in self.__connections__:
            self.__connections__.pop(self._conn_key)

        return type(self)(self.configuration)

    def _create_table(self, name):
        try:
            return Table(name, self.conn.table(name, self.configuration.database, self.configuration.schema), self)
        except:
            import warnings
            warnings.warn(f"Could not parse table: {self.configuration.database}.{self.configuration.schema}.{name}", ResourceWarning)
            return None

    def graph(self, show=True, width=32, height=16, tables=None, save_to=None):
        graph = None
        try:
            graph = self._graph(tables)
        except:
            graph = self._graph_from_column_names(tables)
        finally:
            if show:
                draw_graph(graph, (width, height), save_to)
            else:
                return graph

    @lru_cache
    def neighbours(self, table):
        G = self.graph(show=False)

        nbrs = []
        for name in G.neighbors(table.table_name):
            nbrs.append(self._tables[name])

        return nbrs

    def __getitem__(self, item):
        if item in self.tables:
            return getattr(self, item)
        raise TypeError(f"{item} not in database")

    def connect(self):
        return self.conn.connect()

    def engine(self):
        return self.conn.engine()

    @classmethod
    def from_connection(cls, connection):
        return cls(configuration=connection._configuration)

    def _graph_from_column_names(self, tables):
        if tables:
            return generate_graph({name: table for name, table in self._tables.items() if name in tables})

        return generate_graph(self._tables)

    def _graph(self, tables):
        raise NotImplementedError("Children should implement graph function if possible")



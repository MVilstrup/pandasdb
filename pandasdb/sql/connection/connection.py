from functools import lru_cache

import ibis

from pandasdb.sql.connection.connection_wrapper import ConnectionWrapper
from pandasdb.sql.plot.graph import draw_graph
from pandasdb.sql.table import Table
from pandasdb.sql.utils.table_graph import generate_graph

ibis.options.interactive = True


class Connection:
    __connections__ = {}

    def __init__(self, connection_func, name="", host="", schema="public", username="", password="", port=-1,
                 database="", tunnel=None,
                 ssh_username=None, ssh_key=None, type=""):
        from pandasdb import Async

        self.name = name
        self.type = type
        self.schema = schema

        self._connection = connection_func
        self._conn = None
        self._host = host
        self._username = username
        self._password = password
        self._port = port
        self._database = database
        self._tunnel = tunnel
        self._ssh_user_name = ssh_username
        self._ssh_key = ssh_key
        self._loaded = False

        # @no:format
        table_names = self.conn.list_tables(schema=self.schema)
        self._tables = {name: table for name, table in zip(table_names, Async.map_wait(self._create_table, table_names))}
        # @do:format

        for name, table in self._tables.items():
            setattr(self, name, table)

    @property
    def conn(self):
        conn_key = f"{self._host}_{self._username}_{self._password}_{self._port}_{self._database}_{self._tunnel}"
        if conn_key not in self.__connections__:
            self.__connections__[conn_key] = ConnectionWrapper(self._connection,
                                                               self.type,
                                                               host=self._host,
                                                               username=self._username,
                                                               password=self._password,
                                                               port=self._port,
                                                               database=self._database,
                                                               tunnel=self._tunnel,
                                                               ssh_username=self._ssh_user_name,
                                                               ssh_key=self._ssh_key)
        return self.__connections__[conn_key]

    @property
    def tables(self):
        return list(self._tables.keys())

    def refresh(self):
        conn_key = f"{self._host}_{self._username}_{self._password}_{self._port}_{self._database}_{self._tunnel}"
        if conn_key in self.__connections__:
            self.__connections__.pop(conn_key)

        return type(self)(name=self.name, host=self._host, schema=self.schema, username=self._username,
                          password=self._password, port=self._port,
                          database=self._database, tunnel=self._tunnel, ssh_username=self._ssh_user_name,
                          ssh_key=self._ssh_key, type=self.type)

    def _create_table(self, name):
        return Table(name, self.conn.table(name, self._database, self.schema), self)

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

    def connect(self):
        return self.conn.connect()

    def engine(self):
        return self.conn.engine()

    def _graph_from_column_names(self, tables):
        if tables:
            return generate_graph({name: table for name, table in self._tables.items() if name in tables})

        return generate_graph(self._tables)

    def _graph(self, tables):
        raise NotImplementedError("Children should implement graph function if possible")

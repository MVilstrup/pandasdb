import sqlalchemy
from concurrent.futures import ThreadPoolExecutor
from pandasdb.libraries.configuration.src.base import BaseConfiguration


class AlchemyConnection:
    __pool__ = ThreadPoolExecutor()

    def __init__(self, configuration: BaseConfiguration):
        self.configuration = configuration
        self._engine: sqlalchemy.engine.Engine = None

    def __initialize__(self):
        return sqlalchemy.create_engine(self.configuration.key(),
                                        pool_size=20,
                                        max_overflow=2,
                                        pool_recycle=300,
                                        pool_pre_ping=True,
                                        pool_use_lifo=True)

    def valid(self):
        return all([self._engine is not None, self.configuration.valid])

    def __restart__(self):
        self.configuration = self.configuration.restart()

        if self._engine is not None:
            self._engine.dispose()

        self._engine = self.__initialize__()

    def inspect(self, callback: callable, asyncronous=False, timeout=None):
        def inspector(connection):
            return callback(sqlalchemy.inspect(connection.engine))

        return self.do(inspector, asyncronous, timeout)

    def do(self, callback: callable, asyncronous=False, timeout=None):
        def execute():
            # Ensure it is possible to start a connection
            if not self.valid():
                self.__restart__()

            with self._engine.begin() as connection:
                return callback(connection)

        if not asyncronous:
            return self.__pool__.submit(execute).result(timeout=timeout)
        else:
            return self.__pool__.submit(execute)

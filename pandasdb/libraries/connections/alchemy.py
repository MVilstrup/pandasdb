import sqlalchemy
from concurrent.futures import ThreadPoolExecutor
import warnings
from pandasdb.libraries.configuration.base import BaseConfiguration


class AlchemyConnection:
    __pool__ = ThreadPoolExecutor()

    def __init__(self, configuration: BaseConfiguration):
        self.configuration = configuration
        self._engine = None

    def __initialize__(self):
        return sqlalchemy.create_engine(self.configuration.key,
                                        pool_size=50,
                                        max_overflow=2,
                                        pool_recycle=300,
                                        pool_pre_ping=True,
                                        pool_use_lifo=True)

    def __restart__(self):
        self.configuration = self.configuration.restart()

        if self._engine is not None:
            self._engine.dispose()

        self._engine = self.__initialize__()

    def inspect(self, callback: callable, asyncronous=False):
        def inspector(connection):
            return callback(sqlalchemy.inspect(connection.engine))

        return self.do(inspector, asyncronous)

    def do(self, callback: callable, asyncronous=False):
        def _do(func):
            # Ensure it is possible to start a connection
            try:
                with warnings.catch_warnings():
                    self._engine.begin()
            except:
                self.__restart__()

            with self._engine.begin() as connection:
                return callback(connection)

        if not asyncronous:
            return _do(callback)
        else:
            return self.__pool__.submit(_do, callback)

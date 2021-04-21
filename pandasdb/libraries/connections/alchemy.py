from threading import Lock

import sqlalchemy
from concurrent.futures import ThreadPoolExecutor
from pandasdb.libraries.configuration.src.base import BaseConfiguration


class AlchemyConnection:
    __pool__ = ThreadPoolExecutor()

    def __init__(self, configuration: BaseConfiguration):
        self.configuration = configuration
        self._restart_lock = Lock()
        self._engine: sqlalchemy.engine.Engine = None

    def __initialize__(self):
        return sqlalchemy.create_engine(self.configuration.key(),
                                        pool_size=20,
                                        max_overflow=2,
                                        pool_recycle=300,
                                        pool_pre_ping=True,
                                        pool_use_lifo=True)

    @property
    def valid(self):
        return all([self._engine is not None, self.configuration.valid])

    def __restart__(self):
        self.configuration = self.configuration.restart()

        if self._engine is not None:
            self._engine.dispose()

        self._engine = self.__initialize__()

    def inspect(self, callback: callable, asynchronous=False, timeout=None):
        def inspector(connection):
            return callback(sqlalchemy.inspect(connection.engine))

        return self.do(inspector, asynchronous, timeout)

    def do(self, callback: callable, asynchronous=False, timeout=None, max_attempts=3):
        def execute(attempt=1):
            # Ensure it is possible to start a connection
            with self._restart_lock:
                if not self.valid:
                    self.__restart__()

            with self._engine.begin() as connection:
                try:
                    return callback(connection)
                except Exception as exc:
                    if attempt <= max_attempts:
                        return execute(attempt + 1)
                    else:
                        raise exc

        if not asynchronous:
            return self.__pool__.submit(execute).result(timeout=timeout)
        else:
            return self.__pool__.submit(execute)

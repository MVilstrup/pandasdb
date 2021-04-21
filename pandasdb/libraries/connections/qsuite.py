from threading import Lock
import gspread
from concurrent.futures import ThreadPoolExecutor

from pandasdb.libraries.configuration.src.oauth_config import OAuthConfig


class GSuiteConnection:
    __pool__ = ThreadPoolExecutor()

    def __init__(self, configuration: OAuthConfig):
        self.configuration = configuration
        self._restart_lock = Lock()
        self._engine: gspread.Client = None

    def __initialize__(self):
        return gspread.authorize(self.configuration.key)

    @property
    def valid(self):
        return self._engine is not None

    def __restart__(self):
        self._engine = self.__initialize__()

    def do(self, callback: callable, asynchronous=False, timeout=None, max_attempts=3):
        def execute(attempt=1):
            # Ensure it is possible to start a connection
            with self._restart_lock:
                if not self.valid:
                    self.__restart__()

            try:
                return callback(self._engine)
            except Exception as exc:
                if attempt <= max_attempts:
                    return execute(attempt + 1)
                else:
                    raise exc

        if not asynchronous:
            return self.__pool__.submit(execute).result(timeout=timeout)
        else:
            return self.__pool__.submit(execute)

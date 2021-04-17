from pandasdb.libraries.configuration.src.base import BaseConfiguration
from urllib.parse import quote_plus as urlquote


class DatabaseConfiguration(BaseConfiguration):

    def __init__(self, provider, database, host, password, port, schema, username, **kwargs):
        self.provider = provider
        self.database = database
        self.password = password
        self.schema = schema
        self.username = username
        self.db_host = host
        self.db_port = port

    def port(self, port=None):
        return self.db_port if not port else port

    def host(self, host=None):
        return self.db_host if not host else host

    def key(self, host=None, port=None):
        host, port = self.host(host), self.port(port)
        username = urlquote(self.username)
        password = urlquote(self.password)
        return f"{self.provider}://{username}:{password}@{host}:{port}/{self.database}"

    @property
    def valid(self):
        return True

    def restart(self):
        return self

    def __repr__(self):
        return f"DatabaseConfiguration(database={self.database}, user={self.username})"




from pandasdb.libraries.configuration.base import BaseConfiguration
from sshtunnel import SSHTunnelForwarder
import socket
from contextlib import closing
from urllib.parse import quote_plus as urlquote


class DatabaseConfiguration(BaseConfiguration):

    def __init__(self, provider, database, host, password, port, schema, username, tunnel=None, ssh_username=None,
                 ssh_key=None,
                 **kwargs):

        self.provider = provider
        self.database = database
        self.password = password
        self.schema = schema
        self.username = username

        self.tunnel = tunnel
        self.ssh_username = ssh_username
        self.ssh_key = ssh_key

        self._orig_host = host
        self._orig_port = port
        self._forwarded_host = None
        self._forwarded_port = None
        self._forwarder = None

    def __initialize__(self):
        tunnel_ip, tunnel_port = self.tunnel
        self._forwarder = SSHTunnelForwarder((tunnel_ip, int(tunnel_port)),
                                             ssh_private_key=self.ssh_key,
                                             ssh_username=self.ssh_username,
                                             remote_bind_address=(self._orig_host, int(self._orig_port)),
                                             local_bind_address=("127.0.0.1", int(self.free_port)))

        self._forwarder.daemon_forward_servers = True
        self._forwarder.daemon_transport = True
        self._forwarder.start()
        self._forwarded_host = "localhost"
        self._forwarded_port = self._forwarder.local_bind_port

    @property
    def port(self):
        if self.tunnel is None:
            return self._orig_port
        else:
            if self._forwarder is None:
                self.__initialize__()

            return self._forwarded_port

    @property
    def host(self):
        if self.tunnel is None:
            return self._orig_host
        else:
            if self._forwarder is None:
                self.__initialize__()

            return self._forwarded_host

    @property
    def free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    @property
    def key(self):
        username = urlquote(self.username)
        password = urlquote(self.password)
        return f"{self.provider}://{username}:{password}@{self.host}:{self.port}/{self.database}"

    def restart(self):
        if self._forwarder is not None:
            self._forwarder.close()

        return DatabaseConfiguration(provider=self.provider,
                                     database=self.database,
                                     host=self._orig_host,
                                     password=self.password,
                                     port=self._orig_port,
                                     schema=self.schema,
                                     username=self.username,
                                     tunnel=self.tunnel,
                                     ssh_username=self.ssh_username,
                                     ssh_key=self.ssh_key)

    def __repr__(self):
        return f"DatabaseConfiguration(database={self.database}, user={self.username})"

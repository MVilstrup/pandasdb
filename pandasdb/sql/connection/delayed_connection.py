from sshtunnel import SSHTunnelForwarder
from threading import Lock
import socket
from contextlib import closing


class DelayedConnection:
    lock = Lock()

    def __init__(self, connection_func, host="", username="", password="", port=None, database="", tunnel=None,
                 ssh_username=None, ssh_key=None):

        self._kwargs = {
            "connection_func": connection_func,
            "host": host,
            "username": username,
            "password": password,
            "port": port,
            "database": database,
            "tunnel": tunnel,
            "ssh_username": ssh_username,
            "ssh_key": ssh_key
        }

        self._conn = None
        self._forwarder = None

    @property
    def next_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def _setup_connection(self):
        connection_func = self._kwargs["connection_func"]
        host = self._kwargs["host"]
        port = self._kwargs["port"]
        forwarder = None

        with DelayedConnection.lock:
            if self._kwargs["tunnel"]:
                tunnel_ip, tunnel_port = self._kwargs["tunnel"]
                forwarder = SSHTunnelForwarder((tunnel_ip, int(tunnel_port)),
                                               ssh_private_key=self._kwargs["ssh_key"],
                                               ssh_username=self._kwargs["ssh_username"],
                                               remote_bind_address=(host, int(port)),
                                               local_bind_address=("127.0.0.1", int(self.next_port)))

                forwarder.daemon_forward_servers = True
                forwarder.daemon_transport = True
                forwarder.start()

                host = "localhost"
                port = forwarder.local_bind_port

            conn = connection_func(user=self._kwargs["username"],
                                   password=self._kwargs["password"],
                                   host=host,
                                   port=port,
                                   database=self._kwargs["database"])

        return forwarder, conn

    def _restart_connection(self):
        self._forwarder.stop()
        self.conn.close()
        self._forwarder, self._conn = self._setup_connection()
        return self._conn

    @property
    def conn(self):
        if self._conn is None:
            self._forwarder, self._conn = self._setup_connection()

        return self._conn

    def connect(self):
        return self.engine().connect()

    def engine(self):
        return self.conn.con

    def __getattr__(self, name):
        try:
            return getattr(self.conn, name)
        except AttributeError:
            return self.__getattribute__(name)

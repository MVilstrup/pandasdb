from sshtunnel import SSHTunnelForwarder


class Tunnel:
    def __init__(self, connection_func, host="", username="", password="", port=-1,
                 database="", tunnel=None, ssh_username=None, ssh_key=None):

        self._username = username
        self._password = password
        self._database = database
        self._tunnel = tunnel
        if self._tunnel:
            IP, PORT = self._tunnel
            self._forwarder = SSHTunnelForwarder((IP, int(PORT)),
                                                 ssh_private_key=ssh_key,
                                                 ssh_username=ssh_username,
                                                 remote_bind_address=(host, int(port)))
            self._forwarder.daemon_forward_servers = True
            self._host = "localhost"
        else:
            self._host = host
            self._port = port

        self._establish_connection = connection_func

    def __enter__(self):
        if self._tunnel:
            self._forwarder.start()
            self._port = self._forwarder.local_bind_port

        self._conn = self._establish_connection(user=self._username,
                                                password=self._password,
                                                host=self._host,
                                                port=self._port,
                                                database=self._database)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._tunnel:
            self._forwarder.stop()

    def __getattr__(self, item):
        if item.startswith("_"):
            return self.__getattribute__(item)

        return getattr(self._conn, item)

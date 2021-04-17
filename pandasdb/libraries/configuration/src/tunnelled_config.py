from pandasdb.libraries.configuration.src.base import BaseConfiguration
from sshtunnel import SSHTunnelForwarder
import socket
from contextlib import closing


class TunnelledConfiguration(BaseConfiguration):

    def __init__(self, configuration: BaseConfiguration, tunnel=None, ssh_username=None, ssh_key=None, **kwargs):
        self.configuration = configuration
        self.tunnel = tunnel
        self.ssh_username = ssh_username
        self.ssh_key = ssh_key

        self.forwarded_host = None
        self.forwarded_port = None
        self.forwarder: SSHTunnelForwarder = None

    def __initialize__(self):
        # @no:format
        tunnel_ip, tunnel_port = self.tunnel
        self.forwarder = SSHTunnelForwarder((tunnel_ip, int(tunnel_port)),
                                            ssh_private_key=self.ssh_key,
                                            ssh_username=self.ssh_username,
                                            remote_bind_address=(self.configuration.host(), int(self.configuration.port())),
                                            local_bind_address=("127.0.0.1", int(self.free_port)))

        self.forwarder.daemon_forward_servers = True
        self.forwarder.daemon_transport = True
        self.forwarder.start()
        self.forwarded_host = "localhost"
        self.forwarded_port = self.forwarder.local_bind_port
        # @do:format

    @property
    def valid(self):
        if self.forwarder is None:
            self.__initialize__()
            return self.valid

        return all([
            self.forwarder.tunnel_is_up,
            self.forwarder.is_alive,
            self.forwarder.is_active
        ])

    def port(self, port=None):
        if not self.valid:
            self.restart()
            return self.port(port)

        if port is not None:
            return port

        return self.forwarded_port

    def host(self, host=None):
        if not self.valid:
            self.restart()
            return self.host(host)

        if host is not None:
            return host

        return self.forwarded_host

    @property
    def free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def key(self, host=None, port=None):
        host, port = self.host(host), self.port(port)
        return self.configuration.key(host, port)

    def restart(self):
        if self.forwarder is None:
            self.__initialize__()
        else:
            self.forwarder.restart()

        return self

    def __repr__(self):
        return repr(self.configuration)

    def __getattr__(self, item):
        try:
            return self.__getattribute__(item)
        except:
            return getattr(self.configuration, item)

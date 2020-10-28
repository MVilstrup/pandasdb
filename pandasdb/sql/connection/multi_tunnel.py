from sshtunnel import SSHTunnelForwarder


class MultiTunnel:
    tunnels = {}

    @staticmethod
    def establish_connection(connection_func, host="", username="", password="", port=None,
                             database="", tunnel=None, ssh_username=None, ssh_key=None, schema="public"):
        if tunnel:
            tunnel_ip, tunnel_port = tunnel
            tunnel_key = f"{tunnel_ip}_{tunnel_port}_{ssh_username}_{ssh_key}_{host}_{port}"
            if tunnel_key not in MultiTunnel.tunnels:
                forwarder = SSHTunnelForwarder((tunnel_ip, int(tunnel_port)),
                                               ssh_private_key=ssh_key,
                                               ssh_username=ssh_username,
                                               remote_bind_address=(host, int(port)))
                forwarder.daemon_forward_servers = True
                forwarder.start()

                host = "localhost"
                port = forwarder.local_bind_port

                MultiTunnel.tunnels[tunnel_key] = {
                    "forwarder": forwarder,
                    "host": host,
                    "port": port
                }
            else:
                host = MultiTunnel.tunnels[tunnel_key]["host"]
                port = MultiTunnel.tunnels[tunnel_key]["port"]

        return connection_func(user=username,
                               password=password,
                               host=host,
                               port=port,
                               database=database)

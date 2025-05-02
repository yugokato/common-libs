import socket

from filelock import FileLock


def is_port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """Check if a TCP port is currently in use on the given host.

    :param port: The TCP port number to check
    :param host: Host address to check
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        try:
            s.bind((host, port))
            return False
        except OSError:
            return True


def find_open_port(start_port: int = 1024, end_port: int = 65535, host: str = "127.0.0.1") -> int:
    """Find an open port

    :param start_port: The fist port number range to check
    :param start_port: The last port number range to check
    :param host: The host address to check
    """
    with FileLock("find_open_port"):
        for port in range(start_port, end_port + 1):
            if not is_port_in_use(port, host=host):
                return port
        raise RuntimeError("Unable to find an open TCP port")

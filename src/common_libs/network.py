import socket
from collections.abc import Sequence

from .lock import Lock


def is_port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """Check if a TCP port is currently in use on the given host.

    :param port: The TCP port number to check
    :param host: Host address to check
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        try:
            s.connect((host, port))
            return True
        except (ConnectionRefusedError, OSError):
            return False


def find_open_port(
    start_port: int = 1024, end_port: int = 65535, host: str = "127.0.0.1", exclude: Sequence[int] | None = None
) -> int:
    """Find an open port

    :param start_port: The fist port number range to check
    :param end_port: The last port number range to check
    :param host: The host address to check
    :param exclude: Ports to exclude from checking
    """
    if exclude is None:
        exclude = []
    with Lock("find_open_port"):
        for port in range(start_port, end_port + 1):
            if port in exclude:
                continue
            if not is_port_in_use(port, host=host):
                return port
        raise RuntimeError("Unable to find an open TCP port")

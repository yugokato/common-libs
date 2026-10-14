"""A minimal OAuth2 userinfo endpoint, which the mail server asks to validate the access tokens it receives

The script is copied into the mail server container and runs there with the Python of the image, so it must only use
the standard library.

Usage: `python3 fake_idp.py <access_token> <email> <port>`
"""

import json
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class UserInfoHandler(BaseHTTPRequestHandler):
    """Answer `GET /userinfo` with the email of the user if the bearer token is the accepted one, or with 401"""

    access_token = ""
    email = ""

    def do_GET(self) -> None:
        """Handle the token validation request of the mail server"""
        if self.path == "/userinfo" and self.headers.get("Authorization") == f"Bearer {self.access_token}":
            status, body = HTTPStatus.OK, json.dumps({"email": self.email}).encode()
        else:
            status, body = HTTPStatus.UNAUTHORIZED, b""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        """Keep the output of the container quiet"""


def main(access_token: str, email: str, port: int) -> None:
    """Serve the endpoint on the loopback interface until the process is killed

    :param access_token: The only access token to accept
    :param email: The email address to return for the accepted token
    :param port: The port to listen on
    """
    UserInfoHandler.access_token = access_token
    UserInfoHandler.email = email
    ThreadingHTTPServer(("127.0.0.1", port), UserInfoHandler).serve_forever()


if __name__ == "__main__":
    access_token, email, port = sys.argv[1:]
    main(access_token, email, int(port))

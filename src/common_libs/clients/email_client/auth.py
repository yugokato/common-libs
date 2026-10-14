from __future__ import annotations

import imaplib
import re
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeAlias

_CONTROL_CHARACTERS_RE = re.compile(r"[\x00-\x1f\x7f]")

LoginStep: TypeAlias = Callable[[imaplib.IMAP4], object]


class EmailAuth(ABC):
    """How the client logs in to the IMAP server. Subclass it to support another mechanism"""

    @abstractmethod
    def prepare_login(self, username: str) -> LoginStep:
        """Collect the credentials before connecting and return the step that logs in on the new connection

        Anything raised here reaches the caller as is. The step may raise `imaplib.IMAP4.error` or `OSError`, which the
        client reports as a failed login.

        :param username: The login name
        """


@dataclass(frozen=True)
class PasswordAuth(EmailAuth):
    """Log in with a password

    :param password: The password (for Gmail, an app password)
    """

    password: str = field(repr=False)

    def prepare_login(self, username: str) -> LoginStep:
        """Return the step that logs in with the login name and the password

        :param username: The login name
        """
        return lambda conn: conn.login(username, self.password)


@dataclass(frozen=True)
class OAuth2Auth(EmailAuth):
    """Log in with an OAuth2 access token, using the XOAUTH2 mechanism of Gmail and Microsoft 365

    :param token: An access token, or a function that returns one. The function is called before every connection,
                  which means before every check of the mailbox while waiting for an email. It should return the same
                  token until it expires, and it must be thread-safe because one client can be shared by threads. An
                  error it raises ends the call right away
    """

    token: str | Callable[[], str] = field(repr=False)

    def __post_init__(self) -> None:
        if isinstance(self.token, str):
            _validate_token(self.token)

    def prepare_login(self, username: str) -> LoginStep:
        """Get the token and return the step that logs in with it

        :param username: The login name
        """
        token = self.token if isinstance(self.token, str) else self.token()
        _validate_token(token)
        if _CONTROL_CHARACTERS_RE.search(username):
            raise ValueError("username must not contain control characters for OAuth2")
        responses = iter([f"user={username}\x01auth=Bearer {token}\x01\x01".encode()])
        # A rejected token is answered with an error challenge, which has to be acknowledged with an empty response
        # before the server sends the final failure
        return lambda conn: conn.authenticate("XOAUTH2", lambda _: next(responses, b""))


def _validate_token(token: object) -> None:
    if not isinstance(token, str) or not token.strip() or _CONTROL_CHARACTERS_RE.search(token):
        raise ValueError("The OAuth2 token must be a string that is not blank and has no control characters")

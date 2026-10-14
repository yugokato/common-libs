from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from email import message_from_bytes, policy
from email.message import EmailMessage
from email.utils import getaddresses
from typing import Self, cast

from bs4 import BeautifulSoup

from .exceptions import EmailClientError, LinkNotFoundError


@dataclass(frozen=True)
class Link:
    """A hyperlink in the HTML body

    :param text: The visible text with whitespace collapsed
    :param url: The target URL
    """

    text: str
    url: str


@dataclass(frozen=True)
class Email:
    """A parsed email

    :param uid: The UID in the mailbox. None if the email was not fetched from a mailbox
    :param subject: The decoded subject
    :param sender: The From header
    :param to: The addresses in the To header, without display names
    :param date: The Date header. None if missing or invalid
    :param text: The plain text body. Empty if the email has none
    :param html: The HTML body. Empty if the email has none
    :param links: The links in the HTML body, in document order
    :param message: The underlying message, for anything not covered by the other fields
    """

    uid: int | None
    subject: str
    sender: str
    to: tuple[str, ...]
    date: datetime | None
    text: str = field(repr=False)
    html: str = field(repr=False)
    links: tuple[Link, ...] = field(repr=False)
    message: EmailMessage = field(repr=False, compare=False)

    @classmethod
    def from_bytes(cls, raw: bytes, uid: int | None = None) -> Self:
        """Parse a raw RFC 822 email

        :param raw: The raw email
        :param uid: The UID in the mailbox
        """
        try:
            msg = message_from_bytes(raw, policy=policy.default)
            html = _get_body(msg, "html")
            date = getattr(msg["Date"], "datetime", None)
            return cls(
                uid=uid,
                subject=str(msg["Subject"] or ""),
                sender=str(msg["From"] or ""),
                to=tuple(addr for _, addr in getaddresses([str(h) for h in msg.get_all("To", [])]) if addr),
                date=date,
                text=_get_body(msg, "plain"),
                html=html,
                links=_extract_links(html),
                message=msg,
            )
        except Exception as e:
            # The standard library parsers raise assorted exception types on malformed input
            name = "the email" if uid is None else f"email {uid}"
            raise EmailClientError(f"Unable to parse {name}: {e!r}") from e

    def get_link(self, text: str | re.Pattern[str]) -> str:
        """Return the URL of the link with the given text, e.g. the main button of the email

        Links that point to the same URL are treated as one link.

        :param text: The visible text of the link, ignoring whitespace differences. It must not be blank. A compiled
                     pattern is searched in the text instead
        """
        if isinstance(text, str) and not _normalize(text):
            raise ValueError("text must not be blank")
        matches = [link for link in self.links if _text_matches(text, link.text)]
        if not matches:
            available = ", ".join(repr(link.text) for link in self.links if link.text) or "none"
            raise LinkNotFoundError(f"No link with text {text!r} in the email. Links with text: {available}")
        if len({link.url for link in matches}) > 1:
            raise EmailClientError(f"Multiple different links match {text!r}: {[link.text for link in matches]}")
        return matches[0].url


def _get_body(msg: EmailMessage, subtype: str) -> str:
    part = msg.get_body((subtype,))
    if part is None:
        return ""
    try:
        return part.get_content()
    except (LookupError, UnicodeError):
        return cast(bytes, part.get_payload(decode=True)).decode(errors="replace")


def _extract_links(html: str) -> tuple[Link, ...]:
    links = []
    for a in BeautifulSoup(html, "lxml").find_all("a"):
        href = a.get("href")
        if isinstance(href, str) and href.strip():
            links.append(Link(text=_normalize(a.get_text()), url=href.strip()))
    return tuple(links)


def _text_matches(expected: str | re.Pattern[str], actual: str) -> bool:
    if isinstance(expected, re.Pattern):
        return expected.search(actual) is not None
    return _normalize(expected) == actual


def _normalize(text: str) -> str:
    return " ".join(text.split())

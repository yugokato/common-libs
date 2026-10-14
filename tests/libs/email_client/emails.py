from dataclasses import dataclass
from datetime import datetime
from email import message_from_bytes, policy
from pathlib import Path
from typing import IO, Any, Self

import pytest
from pytest_data_loader import parametrize_dir

# Absolute paths let the data stay here, since the data loader would otherwise search upward for a `data` directory
DATA_DIR = Path(__file__).parent / "data"
TEMPLATES_DIR = DATA_DIR / "templates"
SPECS_DIR = DATA_DIR / "email_specs"
TEMPLATE_READ_OPTIONS = {"mode": "rb"}
PASSWORD_RESET = "password_reset"
WORKSPACE_INVITATION = "workspace_invitation"
SENDER_ADDRESS = "noreply@mail.quillnest.example.net"
SENDER = f"Quillnest <{SENDER_ADDRESS}>"


@dataclass(frozen=True)
class EmailSpec:
    """What a template email is expected to contain, written by hand

    :param name: The name of the template email, without the extension
    :param subject: The decoded subject
    :param sender: The sender, with the display name
    :param to: The addresses of the recipients, without display names
    :param date: The timezone-aware date the email was sent
    :param text_snippet: A part of the decoded plain text body
    :param html_snippet: A part of the decoded HTML body
    :param link_texts: The visible text of every link in the HTML body in document order. An image-only link has none
    :param button_text: The visible text of the main button, which is a link styled as a button
    :param url: The URL of the link behind the main button
    :param logo_url: The URL of the image-only link, which is the logo
    """

    name: str
    subject: str
    sender: str
    to: tuple[str, ...]
    date: datetime
    text_snippet: str
    html_snippet: str
    link_texts: tuple[str, ...]
    button_text: str
    url: str
    logo_url: str

    @classmethod
    def from_json(cls, path: Path, data: dict[str, Any]) -> Self:
        """Build the spec from the content of a JSON file

        :param path: The path of the JSON file. Its stem is the name of the template email
        :param data: The parsed content of the JSON file
        """
        return cls(
            **data
            | {
                "name": path.stem,
                "to": tuple(data["to"]),
                "date": datetime.fromisoformat(data["date"]),
                "link_texts": tuple(data["link_texts"]),
            }
        )

    @property
    def template_path(self) -> Path:
        """The absolute path of the template email file"""
        return get_template_path(self.name)

    @property
    def named_link_texts(self) -> tuple[str, ...]:
        """The visible text of the links that have one"""
        return tuple(text for text in self.link_texts if text)


def get_template_path(name: str) -> Path:
    """Get the absolute path of a template email file

    :param name: The name of the template email, without the extension
    """
    return TEMPLATES_DIR / f"{name}.eml"


def get_spec_path(name: str) -> Path:
    """Get the absolute path of an email spec file

    :param name: The name of the template email, without the extension
    """
    return SPECS_DIR / f"{name}.json"


def read_template(file: IO[bytes]) -> bytes:
    """Read a template email file as bytes, as sent over the wire. The data loader reads `.eml` files as text by default

    :param file: The template email file opened with `TEMPLATE_READ_OPTIONS`
    """
    return file.read()


# Run a test once per email spec file, passing the parsed spec as the `spec` argument
parametrize_email_specs = parametrize_dir("spec", SPECS_DIR, processor=EmailSpec.from_json, ids=lambda path: path.stem)


def build_email_with_unparsable_header(header: str, address: str = "qa@example.com") -> bytes:
    """Build a raw email whose address header ends with an unterminated quote, which the standard library fails on

    :param header: The address header that is malformed, `To` or `From`
    :param address: The address in the To header. The malformed header lists it too
    """
    headers = {"To": address, "From": "noreply@example.com"} | {header: f'{address}, "'}
    lines = [f"{name}: {value}" for name, value in headers.items()]
    return ("\r\n".join(lines) + "\r\nContent-Type: text/plain\r\n\r\nHello").encode()


def _parser_fails_on_unparsable_header(header: str) -> bool:
    msg = message_from_bytes(build_email_with_unparsable_header(header), policy=policy.default)
    try:
        list(map(str, msg.get_all(header, [])))
    except Exception:
        return True
    return False


UNPARSABLE_HEADERS = [
    pytest.param(
        header,
        marks=pytest.mark.skipif(
            not _parser_fails_on_unparsable_header(header),
            reason="The email parser of this Python version accepts the header",
        ),
        id=header,
    )
    for header in ("To", "From")
]

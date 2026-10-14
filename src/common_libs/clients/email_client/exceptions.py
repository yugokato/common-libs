class EmailClientError(Exception):
    """Base class for all errors raised by the email client"""


class EmailNotFoundError(EmailClientError):
    """No matching email arrived before the timeout"""


class LinkNotFoundError(EmailClientError):
    """The email has no link with the requested text"""

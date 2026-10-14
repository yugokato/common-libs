import uuid

MAILBOX_ADDRESS = "qa@example.com"


def generate_email_address(tag: str | None = None) -> str:
    """Return a unique address that delivers to the test mailbox, e.g. `qa+signup-3f9c1a2b7d4e@example.com`

    :param tag: Optional readable label to make the address easier to recognize
    """
    local, _, domain = MAILBOX_ADDRESS.partition("@")
    unique = "-".join(filter(None, [tag, uuid.uuid4().hex[:12]]))
    return f"{local}+{unique}@{domain}"

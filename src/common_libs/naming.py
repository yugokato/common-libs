import keyword
import re


def clean_obj_name(name: str) -> str:
    """Convert the name to a legal Python object name

    - Illegal values will be converted to "_" (multiple illegal values in a row will be converted to single "_")
    - If the name starts with a number, "_" will be added at the beginning

    :param name: The original value
    """
    pattern_illegal_chars = r"\W+|^(?=\d)"
    has_illegal_chars = re.search(pattern_illegal_chars, name)
    is_reserved_name = keyword.iskeyword(name)
    if has_illegal_chars:
        name = re.sub(pattern_illegal_chars, "_", name)
    elif is_reserved_name:
        name = f"_{name}"

    return name


def camel_to_snake(camel_case_str: str) -> str:
    """Convert camel format to snake format

    :param camel_case_str: Camel case string value
    """
    snake_str = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", camel_case_str)
    return snake_str.lower()


def to_class_name(base_name: str, prefix: str | None = None, suffix: str | None = None) -> str:
    """Generate a PascalCase class name.

    Example:
        "my class" -> "MyClass"
        "_private_name" -> "_PrivateName"
    """
    snake = camel_to_snake(base_name)
    matched = re.match(r"^_+", snake)
    leading_underscores = matched.group(0) if matched else ""
    body = snake[len(leading_underscores) :]
    parts = re.split(r"[^a-zA-Z0-9]+", body)
    class_name = "".join(part.capitalize() for part in parts if part)

    if prefix:
        class_name = prefix + class_name
    if suffix:
        class_name += suffix

    return clean_obj_name(leading_underscores + class_name)

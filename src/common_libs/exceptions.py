from typing import Any


class NotFound(Exception): ...


class CommandError(Exception):
    def __init__(self, message: Any, exit_code: int | None = None):
        super().__init__(message)
        self.exit_code = exit_code

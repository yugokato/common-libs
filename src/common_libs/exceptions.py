class NotFound(Exception): ...


class CommandError(Exception):
    def __init__(self, message, exit_code: int = None):
        super().__init__(message)
        self.exit_code = exit_code

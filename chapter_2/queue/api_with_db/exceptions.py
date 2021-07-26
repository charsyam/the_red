from fastapi.responses import JSONResponse


class UnicornException(Exception):
    def __init__(self, status: int, code: int, message: str):
        self.status = status
        self.code = code
        self.message = message

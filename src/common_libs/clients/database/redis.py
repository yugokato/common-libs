from __future__ import annotations

import redis

from common_libs.decorators import singleton
from common_libs.logging import get_logger

logger = get_logger(__name__)


@singleton
class RedisClient:
    """Redis client"""

    def __init__(self, *, host: str = "localhost", port: int = 6379, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self._db: redis.Redis | None = None

        self.connect()

    @property
    def db(self) -> redis.Redis | None:
        return self._db

    def connect(self) -> None:
        """Connect to Redis"""
        if self.db is None:
            logger.info(f"Connecting to {self.host}:{self.port}")
            self._db = redis.Redis(
                host=self.host, port=self.port, username=self.user, password=self.password, decode_responses=True
            )

    def scan_keys(self, pattern: str) -> list[str]:
        """Returns a list of all the keys matching a given pattern"""
        count = 10000
        result = []

        def _scan(cur: int = 0) -> int:
            assert self._db is not None
            next_cur, keys = self._db.scan(cursor=cur, match=pattern, count=count)
            result.extend(keys)
            return next_cur

        next_cur = _scan()
        while next_cur:
            next_cur = _scan(cur=next_cur)

        return result

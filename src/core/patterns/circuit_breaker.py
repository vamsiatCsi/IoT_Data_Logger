from __future__ import annotations
import asyncio, time, logging
from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")

@dataclass
class BreakerConfig:
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout: float = 60.0                 # seconds

class BreakerState(Enum):
    CLOSED = "closed"
    OPEN   = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, cfg: BreakerConfig | None = None):
        self.cfg  = cfg or BreakerConfig()
        self.log  = logging.getLogger(self.__class__.__name__)
        self.state= BreakerState.CLOSED
        self.fail = 0
        self.ok   = 0
        self.last_fail_ts = 0.0

    async def __call__(self, fn: Callable[..., Awaitable[T]], *a, **kw) -> T:
        if self.state == BreakerState.OPEN:
            if time.time() - self.last_fail_ts > self.cfg.timeout:
                self.state, self.ok = BreakerState.HALF_OPEN, 0
            else:
                raise RuntimeError("circuit-breaker: OPEN")
        try:
            res = await fn(*a, **kw)
            await self._on_success()
            return res
        except Exception:
            await self._on_fail()
            raise

    async def _on_success(self):
        if self.state == BreakerState.HALF_OPEN:
            self.ok += 1
            if self.ok >= self.cfg.success_threshold:
                self.state, self.fail = BreakerState.CLOSED, 0
                self.log.info("circuit closed")
        else:
            self.fail = 0

    async def _on_fail(self):
        self.fail, self.last_fail_ts = self.fail + 1, time.time()
        self.log.warning("circuit fail %d/%d", self.fail, self.cfg.failure_threshold)
        if self.fail >= self.cfg.failure_threshold:
            self.state = BreakerState.OPEN
            self.log.error("circuit opened")

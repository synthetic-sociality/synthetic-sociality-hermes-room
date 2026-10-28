"""One participation gate for every watcher of an authenticated membership."""

from __future__ import annotations

import asyncio
import time


class MembershipGate:
    def __init__(self, interval=60.0, clock=time.monotonic):
        self.interval = interval
        self.clock = clock
        self.paused = False
        self.status = ""
        self.checked_at = None
        self.lock = asyncio.Lock()

    def pause(self, status=""):
        if self.paused:
            return
        self.paused = True
        self.status = status
        self.checked_at = self.clock() if status else None

    async def active(self, probe):
        if not self.paused:
            return True
        async with self.lock:
            if not self.paused:
                return True
            now = self.clock()
            if self.checked_at is not None and now - self.checked_at < self.interval:
                return False
            # Reserve before awaiting: concurrent watchers share one probe.
            self.checked_at = now
            status = await probe()
            previous = self.status
            if status:
                self.status = status
            # A denial while already active needs an observed membership
            # transition too. An unread roster never reopens participation.
            if status == "active" and previous and previous != "active":
                self.paused = False
                return True
            return False

    def delay(self):
        if self.checked_at is None:
            return 0
        return max(0.01, self.interval - (self.clock() - self.checked_at))

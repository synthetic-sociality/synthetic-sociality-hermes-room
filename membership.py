"""Shared roster evidence; only background contacts wait for participation."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar
from functools import wraps
import time


TURN_CONTACT = ContextVar("room_turn_contact", default=False)


def turn_contacts(operation):
    """Nested status/ack helpers remain fail-fast when called by a reply."""
    @wraps(operation)
    async def call(*args, **kwargs):
        token = TURN_CONTACT.set(True)
        try:
            return await operation(*args, **kwargs)
        finally:
            TURN_CONTACT.reset(token)
    return call


class MembershipGate:
    def __init__(self, interval=60.0, clock=time.monotonic):
        self.interval = interval
        self.clock = clock
        self.paused = False
        self.status = ""
        self.checked_at = None
        self.revision = 0
        self.lock = asyncio.Lock()
        self.resumed = asyncio.Event()
        self.resumed.set()

    def pause(self, status):
        if not status or status == "active":
            return
        self.resumed.clear()
        self.paused = True
        self.status = status
        self.checked_at = self.clock()

    async def active(self, probe, *, refresh=False, force=False):
        if not self.paused and not refresh:
            return True
        revision = self.revision
        async with self.lock:
            # Concurrent denials/offers share the completed probe, even when forced.
            if revision != self.revision:
                return not self.paused
            now = self.clock()
            if not force and self.checked_at is not None and now - self.checked_at < self.interval:
                return not self.paused
            self.checked_at = now
            try:
                status = await probe()
                if status:
                    self.status = status
                    self.paused = status != "active"
                    if not self.paused:
                        self.resumed.set()
                    else:
                        self.resumed.clear()
                # Unread evidence neither starts a pause nor reopens one.
                return not self.paused
            finally:
                self.revision += 1

    def delay(self):
        if self.checked_at is None:
            return 0
        return max(0.01, self.interval - (self.clock() - self.checked_at))

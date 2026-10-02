"""Contract P2c-4: machine-readable local status for the host watchdog.

One ``status.json`` per installation, next to ``state.json``, mode 0600,
replaced atomically. The schema is the delivery contract appendix "Local status
file (P2c-4)", identical in OpenClaw. It never holds a credential, renewal
secret, message body, reply text or prompt: every value written here is a
fixed code, an identifier, a number or a time.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = "synthetic-sociality-room-status-v1"
CONNECTION_STATES = frozenset({"connecting", "connected", "reconnecting", "stopped"})
_CODE_LIMIT = 64

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _code(value: Any) -> str:
    text = str(value or "").strip()
    if not text or len(text) > _CODE_LIMIT or not all(ch.isalnum() or ch in "_-.:" for ch in text):
        return "unknown_error" if text else ""
    return text


class LocalStatus:
    """Process-local status, written whole on every change."""

    def __init__(self, path: Path, *, plugin: str, version: str, host_label: str) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._failed = False
        self._data: dict[str, Any] = {
            "schema": SCHEMA,
            "plugin": plugin,
            "version": version,
            "pluginLoaded": True,
            "hostLabel": host_label,
            "pid": os.getpid(),
            "startedAt": _now(),
            "updatedAt": _now(),
            "secretStore": "isolated",
            "rooms": {},
        }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._data))

    def set_secret_store(self, value: str) -> None:
        with self._lock:
            self._data["secretStore"] = "reachable" if value == "reachable" else "isolated"
        self.write()

    def set_loaded(self, loaded: bool) -> None:
        with self._lock:
            self._data["pluginLoaded"] = bool(loaded)
        self.write()

    def room(self, room_id: str, membership_id: str = "", *, connection: str | None = None,
             error_code: str | None = None, connected: bool = False,
             gate_wait_seconds: float | None = None) -> None:
        """Update one Room entry; write only when something changed."""
        if not room_id:
            return
        with self._lock:
            rooms = self._data["rooms"]
            entry = rooms.setdefault(str(room_id), {
                "membershipId": str(membership_id or ""),
                "connection": "connecting",
                "lastConnectedAt": None,
                "lastErrorCode": "",
                "lastErrorAt": None,
                "lastGateWaitSeconds": 0,
                "lastGateWaitAt": None,
            })
            before = json.dumps(entry, sort_keys=True)
            if membership_id:
                entry["membershipId"] = str(membership_id)
            if connection in CONNECTION_STATES:
                entry["connection"] = connection
            if connected:
                entry["connection"] = "connected"
                entry["lastConnectedAt"] = _now()
            if error_code is not None:
                code = _code(error_code)
                entry["lastErrorCode"] = code
                entry["lastErrorAt"] = _now() if code else entry["lastErrorAt"]
            if gate_wait_seconds is not None:
                entry["lastGateWaitSeconds"] = round(max(0.0, float(gate_wait_seconds)), 1)
                entry["lastGateWaitAt"] = _now()
            changed = json.dumps(entry, sort_keys=True) != before
        if changed:
            self.write()

    def write(self) -> bool:
        with self._lock:
            self._data["updatedAt"] = _now()
            payload = json.dumps(self._data, indent=2, sort_keys=True) + "\n"
        directory = self.path.parent
        try:
            if not directory.is_dir():
                return False
            fd, temporary = tempfile.mkstemp(prefix=".status-", suffix=".tmp", dir=directory)
            try:
                os.fchmod(fd, 0o600)
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self.path)
            except BaseException:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass
                raise
        except OSError as error:
            if not self._failed:
                self._failed = True
                logger.warning("Room local status could not be written: %s", type(error).__name__)
            return False
        self._failed = False
        return True


def read(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


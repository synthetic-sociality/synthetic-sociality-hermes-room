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
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = "synthetic-sociality-room-status-v1"
CONNECTION_STATES = frozenset({"connecting", "connected", "reconnecting", "inactive", "stopped"})
# Contract P2c-6: the web reading values, and the tool names that count.
WEB_READ_VALUES = frozenset({"available", "unavailable", "unknown"})
WEB_READ_TOOL_NAMES = ("browser", "browser_navigate", "web_extract", "web_fetch", "web_search")
# Contract P2c-4 (ISSUE-023): why a dispatch waited on the host.
GATE_WAIT_REASONS = frozenset({"", "host_run_busy", "host_session_unobservable"})
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
            "secretStoreReasons": [],
            "secretIsolationEnforced": False,
            "webRead": "unknown",
            "webReadTools": [],
            "rooms": {},
        }
        # Contract P2c-3a: when the current outage of a Room began (monotonic
        # process memory only; never written).
        self._outage_started: dict[str, float] = {}
        # Contract P2c-3b: Rooms whose membership is inactive (muted), and each
        # Room's own connection state under that mark (P2c-4).
        self._inactive: set[str] = set()
        self._connection: dict[str, str] = {}

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._data))

    def set_secret_store(self, value: str, reasons: list[str] | tuple[str, ...] = (), *, enforced: bool = False) -> None:
        reachable = value == "reachable"
        with self._lock:
            self._data["secretStore"] = "reachable" if reachable else "isolated"
            self._data["secretStoreReasons"] = [_code(reason) for reason in reasons if _code(reason)] if reachable else []
            self._data["secretIsolationEnforced"] = bool(enforced)
        self.write()

    def set_web_read(self, tool_names: set[str] | list[str] | tuple[str, ...] | None) -> None:
        """Contract P2c-6: web reading from the host's own selection; None is unknown."""
        if tool_names is None:
            value, found = "unknown", []
        else:
            found = sorted(name for name in WEB_READ_TOOL_NAMES if name in set(tool_names))
            value = "available" if found else "unavailable"
        with self._lock:
            changed = (self._data.get("webRead"), self._data.get("webReadTools")) != (value, found)
            self._data["webRead"], self._data["webReadTools"] = value, found
        if changed:
            self.write()

    def set_loaded(self, loaded: bool) -> None:
        with self._lock:
            self._data["pluginLoaded"] = bool(loaded)
        self.write()

    def room(self, room_id: str, membership_id: str = "", *, connection: str | None = None,
             error_code: str | None = None, connected: bool = False, registered: bool = False,
             inactive: bool | None = None, gate_wait_seconds: float | None = None,
             gate_wait_reason: str | None = None, ledger: dict[str, Any] | None = None) -> None:
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
                "lastGateWaitReason": "",
                "lastReconnectedAt": None,
                "lastOutageSeconds": 0,
                "acknowledgedSeq": 0,
                "holeSeqs": [],
                "quarantinedSeqs": [],
                "ledgerCode": "",
            })
            before = json.dumps(entry, sort_keys=True)
            if membership_id:
                entry["membershipId"] = str(membership_id)
            key = str(room_id)
            # Contract P2c-4: `connection` is the connection's own state, with
            # `inactive` shown over `connected` only while the membership is
            # muted (P2c-3b); `stopped` and `reconnecting` win over it. A stop
            # or a new registration (a new runtime, an unmute) ends the mark.
            if inactive is True:
                self._inactive.add(key)
            elif inactive is False or connection == "stopped" or (connected and registered):
                self._inactive.discard(key)
            state = self._connection.get(key, entry["connection"])
            if connection == "inactive":
                state = "connected"
            elif connection in CONNECTION_STATES:
                state = connection
            if connection == "reconnecting":
                # Contract P2c-3a: the first failure of an outage starts it.
                self._outage_started.setdefault(key, time.monotonic())
            elif connection in {"stopped", "inactive"}:
                self._outage_started.pop(key, None)
            if connected:
                state = "connected"
                entry["lastConnectedAt"] = _now()
                started = self._outage_started.pop(key, None)
                if started is not None and registered:
                    # Re-established by an accepted registration after a
                    # failure; a heartbeat that survived ends the outage only.
                    entry["lastReconnectedAt"] = entry["lastConnectedAt"]
                    entry["lastOutageSeconds"] = round(max(0.0, time.monotonic() - started), 1)
            self._connection[key] = state
            entry["connection"] = "inactive" if state == "connected" and key in self._inactive else state
            if error_code is not None:
                code = _code(error_code)
                entry["lastErrorCode"] = code
                entry["lastErrorAt"] = _now() if code else entry["lastErrorAt"]
            if ledger is not None:
                # Contract P3-3: holes and quarantines as codes and seqs only.
                entry["acknowledgedSeq"] = int(ledger.get("acknowledgedSeq") or 0)
                entry["holeSeqs"] = [int(seq) for seq in ledger.get("holeSeqs") or []][:32]
                entry["quarantinedSeqs"] = [int(seq) for seq in ledger.get("quarantinedSeqs") or []][:32]
                entry["ledgerCode"] = _code(ledger.get("ledgerCode") or "")
            if gate_wait_seconds is not None:
                entry["lastGateWaitSeconds"] = round(max(0.0, float(gate_wait_seconds)), 1)
                entry["lastGateWaitAt"] = _now()
                entry["lastGateWaitReason"] = gate_wait_reason if gate_wait_reason in GATE_WAIT_REASONS else ""
            changed = json.dumps(entry, sort_keys=True) != before
        if changed:
            self.write()

    LINK_STATES = frozenset({"not_linked", "pending", "connecting", "connected", "reconnecting", "stopped"})

    def set_link(self, link: dict[str, Any]) -> None:
        """Contract L1-11: the device link, as codes, an identifier, numbers and times."""
        state = str(link.get("state") or "")
        limits = str(link.get("limits") or "")
        entry = {
            "state": state if state in self.LINK_STATES else "stopped",
            "code": _code(link.get("code") or ""),
            "deviceId": _code(link.get("deviceId") or ""),
            "channels": max(0, int(link.get("channels") or 0)),
            "limits": limits if limits in {"advertised", "fallback"} else "fallback",
            "lastConnectedAt": link.get("lastConnectedAt") if isinstance(link.get("lastConnectedAt"), str) else None,
            "lastChannelErrorCode": _code(link.get("lastChannelErrorCode") or ""),
            "updatedAt": _now(),
        }
        with self._lock:
            self._data["link"] = entry
        self.write()

    def write(self) -> bool:
        with self._lock:
            self._data["updatedAt"] = _now()
            payload = json.dumps(self._data, indent=2, sort_keys=True) + "\n"
        directory = self.path.parent
        try:
            if not directory.is_dir():
                # Contract P3-5: create the state's directory 0700, never a
                # filesystem root or top-level directory (P2a-5).
                from .state import assert_state_directory

                assert_state_directory(directory)
                directory.mkdir(mode=0o700, parents=True, exist_ok=True)
                os.chmod(directory, 0o700)
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
        except (OSError, ValueError) as error:
            if not self._failed:
                self._failed = True
                logger.warning("Room local status could not be written: %s", type(error).__name__)
            return False
        self._failed = False
        return True


def read(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


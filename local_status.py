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
# Contract P2c-4 (tkw-049): the `rooms` summary takes the membership whose
# connection is worst in this order, ties by the smallest membership id.
CONNECTION_ORDER = ("stopped", "reconnecting", "connecting", "inactive", "connected")
# Contract P2c-6: the web reading values, and the tool names that count.
WEB_READ_VALUES = frozenset({"available", "unavailable", "unknown"})
WEB_READ_TOOL_NAMES = ("browser", "browser_navigate", "web_extract", "web_fetch", "web_search")
# Contract P2c-4 (ISSUE-023): why a dispatch waited on the host.
GATE_WAIT_REASONS = frozenset({"", "host_run_busy", "host_session_unobservable", "held_by_moderator"})
_CODE_LIMIT = 64
# Contract P8-5: a change of `lastConnectedAt` alone is written at most this
# often per installation; every other change is written at once.
CONNECTED_AT_WRITE_SECONDS = 60.0

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _timer(delay: float, callback: Any) -> Any:
    """Run ``callback`` once after ``delay`` seconds; returns a cancel function."""
    timer = threading.Timer(max(0.0, delay), callback)
    timer.daemon = True
    timer.start()
    return timer.cancel


def _code(value: Any) -> str:
    text = str(value or "").strip()
    if not text or len(text) > _CODE_LIMIT or not all(ch.isalnum() or ch in "_-.:" for ch in text):
        return "unknown_error" if text else ""
    return text


class LocalStatus:
    """Process-local status, written whole on every change."""

    def __init__(self, path: Path, *, plugin: str, version: str, host_label: str,
                 clock: Any = None, schedule: Any = None) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._failed = False
        # Contract P8-5: when the file was last written (monotonic), and the
        # pending write of a `lastConnectedAt`-only change (a cancel function).
        self._clock = clock or time.monotonic
        self._schedule = schedule or _timer
        self._written_at: float | None = None
        self._pending_cancel: Any = None
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
            "documentTool": "unknown",
            "rooms": {},
            "memberships": {},
        }
        # Contract P2c-4 (tkw-049): one entry per (Room, membership), keyed by
        # membership id; an entry whose membership is not known yet is kept
        # under "room:<roomId>" and never written to `memberships`.
        self._entries: dict[str, dict[str, Any]] = {}
        # Contract P2c-3a: when the current outage of a membership began
        # (monotonic process memory only; never written).
        self._outage_started: dict[str, float] = {}
        # Contract P2c-3b: memberships that are inactive (muted), and each
        # membership's own connection state under that mark (P2c-4).
        self._inactive: set[str] = set()
        self._connection: dict[str, str] = {}
        # Contract P17-6/P17-7: an owner control receipt survives the process
        # that wrote it, so the restarted process can report and never repeat.
        receipt = _prior_connector_control(self.path)
        if receipt is not None:
            self._data["connectorControl"] = receipt

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

    def set_document_tool(self, value: str) -> None:
        """Contract P7d-6: whether the document tool is offered in a Room turn."""
        value = value if value in ("available", "unavailable") else "unknown"
        with self._lock:
            changed = self._data.get("documentTool") != value
            self._data["documentTool"] = value
        if changed:
            self.write()

    def set_loaded(self, loaded: bool) -> None:
        with self._lock:
            self._data["pluginLoaded"] = bool(loaded)
        self.write()

    def _adopt(self, old: str, new: str) -> None:
        """Move the entry and marks of ``old`` to ``new`` (callers hold the lock)."""
        entry = self._entries.pop(old)
        self._data["memberships"].pop(old, None)
        entry["membershipId"] = new
        self._entries[new] = entry
        self._data["memberships"][new] = entry
        for marks in (self._outage_started, self._connection):
            if old in marks:
                marks[new] = marks.pop(old)
        if old in self._inactive:
            self._inactive.discard(old)
            self._inactive.add(new)

    def _entry_key(self, room_id: str, membership_id: str) -> str | None:
        """The internal key of a (Room, membership); None when it is not attributable."""
        if membership_id:
            provisional = f"room:{room_id}"
            if provisional in self._entries and membership_id not in self._entries:
                # The provisional entry becomes the membership's own.
                self._adopt(provisional, membership_id)
            return membership_id
        # A value without a membership id belongs to the Room's only
        # membership; with several it is not attributed.
        known = [key for key, entry in self._entries.items() if entry["roomId"] == room_id]
        if len(known) > 1:
            return None
        return known[0] if known else f"room:{room_id}"

    def _summarize(self, room_id: str) -> None:
        """Contract P2c-4 (tkw-049): the `rooms` entry, derived from that Room's
        membership entries alone, never from the order of writes."""
        entries = sorted(
            (entry for entry in self._entries.values() if entry["roomId"] == room_id),
            key=lambda entry: (CONNECTION_ORDER.index(entry["connection"]), entry["membershipId"]),
        )
        if not entries:
            return
        fields = {key: value for key, value in json.loads(json.dumps(entries[0])).items() if key != "roomId"}
        fields["membershipIds"] = sorted(entry["membershipId"] for entry in entries if entry["membershipId"])
        self._data["rooms"][room_id] = fields

    def forget(self, room_id: str, membership_id: str) -> None:
        """Contract P2c-4: a binding replaced by another membership (a re-admission
        under a new membership id) or removed drops its entry and marks; the Room
        summary is recomputed from the remaining entries, or dropped with the last."""
        room_key = str(room_id or "")
        key = str(membership_id or "")
        with self._lock:
            entry = self._entries.get(key)
            if not room_key or not key or entry is None or entry["roomId"] != room_key:
                return
            del self._entries[key]
            self._data["memberships"].pop(key, None)
            self._outage_started.pop(key, None)
            self._connection.pop(key, None)
            self._inactive.discard(key)
            self._data["rooms"].pop(room_key, None)
            self._summarize(room_key)
        self.write()

    def replace_membership(self, room_id: str, old: str, new: str) -> None:
        """A Room's one binding now holds another membership (a re-admission):
        the old membership's entry is dropped and the new one starts its own."""
        if not old or old == new:
            return
        self.forget(room_id, old)

    def room(self, room_id: str, membership_id: str = "", *, connection: str | None = None,
             error_code: str | None = None, connected: bool = False, registered: bool = False,
             inactive: bool | None = None, gate_wait_seconds: float | None = None,
             gate_wait_reason: str | None = None, ledger: dict[str, Any] | None = None) -> None:
        """Update one membership's entry and its Room summary; write only when something changed."""
        if not room_id:
            return
        room_key = str(room_id)
        with self._lock:
            key = self._entry_key(room_key, str(membership_id or ""))
            if key is None:
                return
            entry = self._entries.get(key)
            if entry is None:
                entry = self._entries[key] = {
                    "roomId": room_key,
                    "membershipId": "" if key.startswith("room:") else key,
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
                }
                if entry["membershipId"]:
                    self._data["memberships"][key] = entry

            def unstamped() -> str:
                summary = self._data["rooms"].get(room_key) or {}
                return json.dumps([{**entry, "lastConnectedAt": None}, {**summary, "lastConnectedAt": None}],
                                  sort_keys=True)

            before = unstamped()
            connected_before = entry["lastConnectedAt"]
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
            self._summarize(room_key)
            changed = unstamped() != before
            connected_changed = entry["lastConnectedAt"] != connected_before
        if changed:
            self.write()
        elif connected_changed:
            self._write_connected_at()

    def _write_connected_at(self) -> None:
        """Contract P8-5: a `lastConnectedAt`-only change, at most once per 60 s."""
        with self._lock:
            if self._pending_cancel is not None:
                return  # already due; that write carries the newest value
            written_at = self._written_at
            wait = 0.0 if written_at is None else CONNECTED_AT_WRITE_SECONDS - (self._clock() - written_at)
            if wait > 0:
                self._pending_cancel = self._schedule(wait, self._write_pending)
                return
        self.write()

    def _write_pending(self) -> None:
        with self._lock:
            if self._pending_cancel is None:
                return  # written meanwhile
            self._pending_cancel = None
        if not self.path.parent.is_dir():
            return  # a deferred time alone never creates the directory again
        self.write()

    def connector_control(self) -> dict[str, Any] | None:
        """Contract P17: the owner control receipt, or None."""
        with self._lock:
            receipt = self._data.get("connectorControl")
            return json.loads(json.dumps(receipt)) if isinstance(receipt, dict) else None

    def set_connector_control(self, receipt: dict[str, Any]) -> bool:
        """Contract P17-6: store the owner control receipt and write it at once.

        Codes, identifiers, counts, times and the non-secret binding inventory
        only; True when the file was written.
        """
        with self._lock:
            self._data["connectorControl"] = json.loads(json.dumps(receipt))
        return self.write()

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
            # Contract P8-5: this write carries every pending value.
            self._written_at = self._clock()
            cancel, self._pending_cancel = self._pending_cancel, None
        if cancel is not None:
            try:
                cancel()
            except Exception:  # noqa: BLE001 - a timer that already ran
                pass
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


def _prior_connector_control(path: Path) -> dict[str, Any] | None:
    """The receipt a previous process left in ``status.json``, or None."""
    try:
        if path.is_symlink() or not path.is_file():
            return None
        receipt = json.loads(path.read_text(encoding="utf-8")).get("connectorControl")
    except (OSError, ValueError, AttributeError):
        return None
    if not isinstance(receipt, dict) or not isinstance(receipt.get("commandId"), str):
        return None
    return receipt


def read(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


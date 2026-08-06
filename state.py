"""Profile-private durable state for the Synthetic Sociality Hermes plugin."""

from __future__ import annotations

import json
import os
import secrets
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


STATE_VERSION = 1


def state_root() -> Path:
    hermes_home = os.environ.get("HERMES_HOME", "").strip()
    root = Path(hermes_home).expanduser() if hermes_home else Path.home() / ".hermes"
    return root / "synthetic-sociality-room"


def state_path() -> Path:
    return state_root() / "state.json"


@dataclass
class RoomBinding:
    base_url: str
    room_id: str
    membership_id: str
    credential: str
    credential_expires_at: str = ""
    display_name: str = ""
    identity_version: int = 0
    installation_id: str = ""
    connector_session_id: str = ""
    cursor: int = 0
    acknowledged_cursor: int = 0
    # Durable receive ledger. A sequence is acknowledged only after Hermes
    # has completed processing (or the event was intentionally ignored).
    inbox: dict[str, str] = field(default_factory=dict)
    # Unix timestamps for pending receive-ledger entries.  This lets a
    # crashed or permanently blocked model turn expire without pinning the
    # canonical acknowledgement cursor forever.
    pending_since: dict[str, float] = field(default_factory=dict)
    # Exact observedSeq used for an accepted idempotent turn request. Persist
    # before every attempt so crash replay reproduces the server request hash.
    turn_observed: dict[str, int] = field(default_factory=dict)
    # Durable source-to-sequence association for provenance cleanup even when
    # process-local event indexes are empty after restart.
    turn_sequences: dict[str, int] = field(default_factory=dict)
    # Exact hash-affecting post/finish arguments, persisted before I/O. A
    # retry after an ambiguous commit or process restart must reproduce the
    # server's accepted idempotency request rather than use a newer room head
    # or regenerated model output.
    delivery_intents: dict[str, dict[str, Any]] = field(default_factory=dict)
    enabled: bool = True
    revoked: bool = False
    transport: str = "long_poll"

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RoomBinding":
        allowed = cls.__dataclass_fields__.keys()
        normalized = {key: value[key] for key in allowed if key in value}
        if "acknowledged_cursor" not in normalized:
            normalized["acknowledged_cursor"] = int(normalized.get("cursor") or 0)
        return cls(**normalized)


@dataclass
class PluginState:
    version: int = STATE_VERSION
    bindings: list[RoomBinding] = field(default_factory=list)

    def binding(self, room_id: str) -> RoomBinding | None:
        return next((item for item in self.bindings if item.room_id == room_id), None)


def load(path: Path | None = None) -> PluginState:
    target = path or state_path()
    if not target.exists():
        return PluginState()
    if target.is_symlink() or not target.is_file() or target.parent.is_symlink():
        raise ValueError("Synthetic Sociality state must be a regular file in a regular directory")
    os.chmod(target.parent, 0o700)
    os.chmod(target, 0o600)
    payload = json.loads(target.read_text(encoding="utf-8"))
    if payload.get("version") != STATE_VERSION:
        raise ValueError(f"unsupported Synthetic Sociality state version: {payload.get('version')}")
    return PluginState(
        version=STATE_VERSION,
        bindings=[RoomBinding.from_dict(item) for item in payload.get("bindings", [])],
    )


def save(state: PluginState, path: Path | None = None) -> None:
    target = path or state_path()
    if target.is_symlink() or target.parent.is_symlink():
        raise ValueError("refusing to write Synthetic Sociality state through a symlink")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(target.parent, 0o700)
    encoded = json.dumps(asdict(state), indent=2, sort_keys=True) + "\n"
    fd, temporary = tempfile.mkstemp(prefix=".state-", dir=target.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def upsert(state: PluginState, binding: RoomBinding) -> None:
    if not binding.installation_id:
        binding.installation_id = "hermes-" + secrets.token_hex(16)
    for index, current in enumerate(state.bindings):
        if current.room_id == binding.room_id:
            state.bindings[index] = binding
            return
    state.bindings.append(binding)


def remove(state: PluginState, room_id: str) -> bool:
    before = len(state.bindings)
    state.bindings = [item for item in state.bindings if item.room_id != room_id]
    return len(state.bindings) != before

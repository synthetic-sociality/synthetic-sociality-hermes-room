"""Profile-private durable state for the Synthetic Sociality Hermes plugin."""

from __future__ import annotations

import json
import os
import secrets
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar

import fcntl


STATE_VERSION = 1
T = TypeVar("T")


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
    # Message payload support is discovered with the public read-only status
    # endpoint before this binding performs a connector write. It is scoped to
    # the binding because one Hermes process may serve old and new servers.
    message_payload_dialect: str = "v1"
    message_payload_capabilities: list[str] = field(default_factory=list)
    cursor: int = 0
    acknowledged_cursor: int = 0
    # Durable receive ledger. A sequence is acknowledged only after explicit
    # posted/skipped/cancelled/superseded/ignored terminal evidence.
    inbox: dict[str, str] = field(default_factory=dict)
    # Unix timestamps for pending receive-ledger entries.  This lets a
    # crashed or blocked model turn become retryable without pretending that
    # it reached a terminal delivery boundary.
    pending_since: dict[str, float] = field(default_factory=dict)
    # Bounded retry counters for pending receive-ledger entries. Exhaustion is
    # represented by the non-terminal ``quarantined`` inbox state, which keeps
    # the canonical acknowledgement cursor fail-closed for operator review.
    pending_retries: dict[str, int] = field(default_factory=dict)
    # Durable proof for every terminal inbox state. Keys are canonical source
    # sequences; posted evidence also carries the confirmed canonical event
    # ID. This is deliberately separate from Hermes' model/runtime outcome.
    terminal_evidence: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Audited recovery fence (operator-set). When > 0, canonical sequences in
    # (acknowledged_cursor, recovery_fence_cutoff] are marked terminally
    # `ignored` (reason `recovery_fence`) through the normal connector ack
    # path on consumption — never via a direct cursor write. This is the
    # authorized audited-skip mechanism for a stale backlog. 0 = no fence.
    recovery_fence_cutoff: int = 0
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
    # Durable ownership of model work by the server-issued attempt identity.
    # Different canonical source events may describe the same attempt; only
    # the recorded source is allowed to dispatch it to Hermes.
    cycle_attempt_owners: dict[str, str] = field(default_factory=dict)
    # Secret-free forensic receipts for explicitly recovered delivery intents
    # whose canonical source was already acknowledged and whose server-owned
    # discussion cycle had terminally accepted a pass with zero bytes. The
    # frozen response body is never copied here; only its SHA-256 is retained.
    abandoned_delivery_intents: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Crash-safe credential-renewal journal. It is profile-private state and
    # deliberately contains the exact grant, replacement, and previous
    # credential material needed to resume an ambiguous redeem/swap/confirm
    # boundary. The CLI mutates it only under the same 0600 atomic state lock.
    credential_rotation: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    revoked: bool = False
    transport: str = "long_poll"

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RoomBinding":
        allowed = cls.__dataclass_fields__.keys()
        normalized = {key: value[key] for key in allowed if key in value}
        if "acknowledged_cursor" not in normalized:
            normalized["acknowledged_cursor"] = int(normalized.get("cursor") or 0)
        binding = cls(**normalized)
        if binding.message_payload_dialect not in {"v1", "v2"}:
            raise ValueError("unsupported Room message payload dialect")
        return binding


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
        # fsyncing the temporary file protects its contents; fsyncing the
        # directory makes the atomic rename durable across a host crash.
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
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


@contextmanager
def _exclusive_state_lock(path: Path | None = None) -> Iterator[Path]:
    """Serialize a complete load/mutate/save transaction across processes."""
    target = path or state_path()
    if target.is_symlink() or target.parent.is_symlink():
        raise ValueError("refusing to lock Synthetic Sociality state through a symlink")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(target.parent, 0o700)
    lock_path = target.parent / ".state.lock"
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(lock_path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield target
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def update(mutator: Callable[[PluginState], T], path: Path | None = None) -> T:
    """Apply one state mutation without a cross-process lost-update window."""
    with _exclusive_state_lock(path) as target:
        current = load(target)
        result = mutator(current)
        save(current, target)
        return result


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

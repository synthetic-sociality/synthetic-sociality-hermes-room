"""Profile-private durable state for the Synthetic Sociality Hermes plugin."""

from __future__ import annotations

import json
import os
import re
import secrets
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar

import fcntl


STATE_VERSION = 1
T = TypeVar("T")


def _valid_canonical_timestamp(value: Any) -> bool:
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})",
        value,
    ) if isinstance(value, str) else None
    if match is None:
        return False
    fraction = match.group(2)
    parse_value = match.group(1)
    if fraction:
        parse_value += "." + fraction[:6].ljust(6, "0")
    parse_value += match.group(3)
    try:
        parsed = datetime.fromisoformat(parse_value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


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
    # Canonical delivery receipts and unfinished post-delivery lifecycle work.
    # This journal is intentionally separate: terminal source acknowledgement
    # may remove the frozen delivery intent while cycle/turn completion remains
    # safely retryable without another post or model invocation.
    delivery_lifecycle: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Operator-audited closure of non-retryable lifecycle work after canonical
    # delivery and authoritative terminal-cycle verification.
    resolved_delivery_lifecycle: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Durable ownership of model work by the server-issued attempt identity.
    # Different canonical source events may describe the same attempt; only
    # the recorded source is allowed to dispatch it to Hermes.
    cycle_attempt_owners: dict[str, str] = field(default_factory=dict)
    # Durable stale-delivery fence keyed by exact authority dimensions.
    delivery_authority: dict[str, dict[str, Any]] = field(default_factory=dict)
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
    # Upgrade boundary for epoch-scoped Hermes transcripts. Existing bindings
    # keep their current epoch on the legacy route; later epochs rotate. An
    # operator-authorized current reset sets rotate_current_epoch_session before
    # restart, causing initialization to omit the legacy baseline.
    epoch_session_routing_initialized: bool = False
    legacy_session_epoch_id: str = ""
    rotate_current_epoch_session: bool = False
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
        if type(binding.epoch_session_routing_initialized) is not bool:
            raise ValueError("Room epoch session routing marker must be boolean")
        if not isinstance(binding.legacy_session_epoch_id, str):
            raise ValueError("Room legacy session epoch must be a string")
        if type(binding.rotate_current_epoch_session) is not bool:
            raise ValueError("Room current epoch rotation marker must be boolean")
        if binding.message_payload_dialect not in {"v1", "v2"}:
            raise ValueError("unsupported Room message payload dialect")
        if not isinstance(binding.delivery_lifecycle, dict):
            raise ValueError("Room delivery lifecycle journal must be an object")
        if not isinstance(binding.resolved_delivery_lifecycle, dict):
            raise TypeError("Room resolved lifecycle audit must be an object")
        if not isinstance(binding.delivery_authority, dict):
            raise ValueError("Room delivery authority fence must be an object")
        normalized_authority: dict[str, dict[str, Any]] = {}
        for key, record in binding.delivery_authority.items():
            if not isinstance(key, str) or not isinstance(record, dict):
                raise ValueError("Room delivery authority entry is invalid")
            canonical_key = json.dumps([
                record.get("room_id"), record.get("source_event_id"),
                record.get("membership_id"), record.get("cycle_id"),
                record.get("attempt_id"), record.get("generation"),
            ], separators=(",", ":"), ensure_ascii=False)
            legacy_key = json.dumps([
                record.get("room_id"), record.get("source_event_id"),
                record.get("membership_id"), record.get("attempt_id"),
                record.get("generation"),
            ], separators=(",", ":"), ensure_ascii=False)
            if key == legacy_key and key != canonical_key:
                # A legacy active key did not bind cycle_id and therefore cannot
                # safely authorize work after upgrade. Preserve only its
                # denial-only superseded state under the cycle-bound key.
                if record.get("state") != "superseded":
                    continue
                key = canonical_key
            previous = normalized_authority.get(key)
            if not isinstance(previous, dict) or previous.get("state") != "superseded":
                normalized_authority[key] = record
        binding.delivery_authority = normalized_authority
        for key, record in binding.delivery_authority.items():
            expected_key = json.dumps([
                record.get("room_id"), record.get("source_event_id"),
                record.get("membership_id"), record.get("cycle_id"),
                record.get("attempt_id"), record.get("generation"),
            ], separators=(",", ":"), ensure_ascii=False)
            if (
                key != expected_key
                or record.get("room_id") != binding.room_id
                or record.get("membership_id") != binding.membership_id
                or not isinstance(record.get("source_event_id"), str)
                or not record["source_event_id"]
                or not isinstance(record.get("cycle_id"), str)
                or not record["cycle_id"]
                or not isinstance(record.get("attempt_id"), str)
                or not record["attempt_id"]
                or type(record.get("generation")) is not int
                or record["generation"] < 0
                or not _valid_canonical_timestamp(record.get("lease_expires_at"))
                or record.get("state") not in {"active", "superseded"}
            ):
                raise ValueError("Room delivery authority evidence is invalid")
        for source_id, record in binding.delivery_lifecycle.items():
            if not isinstance(source_id, str) or not source_id or not isinstance(record, dict):
                raise ValueError("Room delivery lifecycle journal entry is invalid")
            receipt = record.get("receipt")
            completion = record.get("completion")
            lifecycle_state = record.get("lifecycle_state")
            attempts = record.get("attempts")
            expected_binding = {
                "membership_id": binding.membership_id,
                "installation_id": binding.installation_id,
                "identity_version": binding.identity_version,
            }
            if (
                record.get("delivery_state") != "posted"
                or lifecycle_state not in {"pending", "blocked", "not_required"}
                or not isinstance(receipt, dict)
                or not isinstance(receipt.get("source_event_id"), str)
                or receipt.get("source_event_id") != source_id
                or type(receipt.get("source_seq")) is not int
                or receipt["source_seq"] <= 0
                or not isinstance(receipt.get("canonical_event_id"), str)
                or not receipt["canonical_event_id"]
                or type(receipt.get("canonical_seq")) is not int
                or receipt["canonical_seq"] <= 0
                or not _valid_canonical_timestamp(receipt.get("canonical_ts"))
                or not isinstance(completion, dict)
                or not isinstance(attempts, int)
                or isinstance(attempts, bool)
                or attempts < 0
                or attempts > 3
                or record.get("binding") != expected_binding
            ):
                raise ValueError("Room delivery lifecycle evidence is invalid")
            expected_state = {
                "pending": "lifecycle_pending",
                "blocked": "lifecycle_blocked",
                "not_required": "posted",
            }[lifecycle_state]
            expected_automatic_retry = lifecycle_state == "pending"
            if record.get("state") != expected_state:
                raise ValueError("Room lifecycle state label is inconsistent")
            if record.get("automatic_retry") is not expected_automatic_retry:
                raise ValueError("Room lifecycle automatic-retry state is inconsistent")
            if lifecycle_state == "blocked" and (
                not isinstance(record.get("last_error_code"), str)
                or not isinstance(record.get("last_error"), str)
            ):
                raise ValueError("Room lifecycle error evidence is invalid")
            if lifecycle_state == "not_required":
                if completion:
                    raise ValueError("Room lifecycle marked not-required has a completion request")
                continue
            kind = completion.get("kind")
            canonical_event_id = receipt["canonical_event_id"]
            if kind == "cycle":
                payload = completion.get("payload")
                if (
                    not isinstance(completion.get("cycle_id"), str)
                    or not completion["cycle_id"]
                    or not isinstance(completion.get("attempt_id"), str)
                    or not completion["attempt_id"]
                    or not isinstance(payload, dict)
                    or type(payload.get("generation")) is not int
                    or payload["generation"] < 0
                    or payload.get("action") != "contribute"
                    or not isinstance(payload.get("eventId"), str)
                    or payload["eventId"] != canonical_event_id
                ):
                    raise ValueError("Room cycle lifecycle completion is invalid")
            elif kind == "turn":
                if (
                    not isinstance(completion.get("turn_id"), str)
                    or not completion["turn_id"]
                    or type(completion.get("observed_seq")) is not int
                    or completion["observed_seq"] <= 0
                    or not isinstance(completion.get("source_event_id"), str)
                    or completion["source_event_id"] != source_id
                    or not isinstance(completion.get("idempotency_key"), str)
                    or not completion["idempotency_key"]
                ):
                    raise ValueError("Room turn lifecycle completion is invalid")
            else:
                raise ValueError("Room lifecycle completion kind is invalid")
        for source_id, record in binding.resolved_delivery_lifecycle.items():
            receipt = record.get("receipt") if isinstance(record, dict) else None
            completion = record.get("original_completion") if isinstance(record, dict) else None
            audit_binding = record.get("binding") if isinstance(record, dict) else None
            payload = completion.get("payload") if isinstance(completion, dict) else None
            allowed_audit_fields = {
                "version", "reason", "source_event_id", "source_seq",
                "canonical_event_id", "cycle_id", "terminal_state", "receipt",
                "original_completion", "binding", "original_error_code",
                "original_error", "recorded_at",
            }
            if (
                not isinstance(source_id, str) or not source_id
                or not isinstance(record, dict)
                or set(record) != allowed_audit_fields
                or type(record.get("version")) is not int
                or record["version"] != 1
                or record.get("reason") != "authoritative_terminal_cycle_after_canonical_delivery"
                or record.get("source_event_id") != source_id
                or type(record.get("source_seq")) is not int
                or record["source_seq"] <= 0
                or record.get("terminal_state") not in {"completed", "interrupted"}
                or not isinstance(record.get("cycle_id"), str) or not record["cycle_id"]
                or not isinstance(record.get("canonical_event_id"), str)
                or not record["canonical_event_id"]
                or not _valid_canonical_timestamp(record.get("recorded_at"))
                or not isinstance(receipt, dict)
                or receipt.get("source_event_id") != source_id
                or receipt.get("source_seq") != record["source_seq"]
                or receipt.get("canonical_event_id") != record["canonical_event_id"]
                or type(receipt.get("canonical_seq")) is not int
                or receipt["canonical_seq"] <= 0
                or not _valid_canonical_timestamp(receipt.get("canonical_ts"))
                or not isinstance(completion, dict)
                or completion.get("kind") != "cycle"
                or completion.get("cycle_id") != record["cycle_id"]
                or not isinstance(completion.get("attempt_id"), str)
                or not completion["attempt_id"]
                or not isinstance(payload, dict)
                or type(payload.get("generation")) is not int
                or payload["generation"] < 0
                or payload.get("action") != "contribute"
                or payload.get("eventId") != record["canonical_event_id"]
                or audit_binding != {
                    "membership_id": binding.membership_id,
                    "installation_id": binding.installation_id,
                    "identity_version": binding.identity_version,
                }
                or record.get("original_error_code") != "cycle_conflict"
                or not isinstance(record.get("original_error"), str)
                or binding.cursor < record["source_seq"]
                or binding.acknowledged_cursor < record["source_seq"]
            ):
                raise ValueError("Room resolved lifecycle audit evidence is invalid")
            if source_id in binding.delivery_lifecycle or source_id in binding.delivery_intents:
                raise ValueError("Room resolved lifecycle audit overlaps live work")
        for source_id, intent in binding.delivery_intents.items():
            if not isinstance(source_id, str) or not source_id or not isinstance(intent, dict):
                raise ValueError("Room delivery intent is invalid")
            if intent.get("delivery_state") != "posted":
                continue
            canonical = intent.get("canonical_event")
            if (
                not isinstance(canonical, dict)
                or not isinstance(canonical.get("id"), str)
                or not canonical["id"]
                or type(canonical.get("seq")) is not int
                or canonical["seq"] <= 0
                or not _valid_canonical_timestamp(canonical.get("ts"))
            ):
                raise ValueError("posted Room delivery intent requires a complete canonical receipt")
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

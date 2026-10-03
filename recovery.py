"""Contract P3: ledger report, terminal-intent retention and audited recovery.

Shared with OpenClaw (src/recovery.js): the same verbs, reasons, server proof,
audit record keys, confirmation token and record digest. Nothing here writes a
cursor, an acknowledgement, an intent or a quarantine. A closure is an
append-only audit record; the running connector applies it through its normal
acknowledgement path after re-checking the Room's proof.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

REASONS = ("cycle_terminal", "cycle_superseded", "epoch_closed")
TERMINAL_CYCLE_STATES = frozenset({"completed", "interrupted", "timed_out", "failed"})
TERMINAL_INTENT_RETENTION = 32
REPORT_LIMIT = 32
CLOSURE_EVIDENCE_PREFIX = "recovery_closure:"
_TOKEN_EXCLUDED = ("operator", "confirmToken", "recordedAt", "recordDigest")


class RecoveryRefused(ValueError):
    """A closure the Room does not prove, or that the state does not allow."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# --- ledger -------------------------------------------------------------------


def _ledger_keys(binding: Any) -> set[int]:
    keys: set[int] = set()
    for mapping in (binding.inbox, binding.terminal_evidence):
        for key in mapping:
            try:
                keys.add(int(key))
            except (TypeError, ValueError):
                continue
    return keys


def ledger_report(binding: Any) -> dict[str, Any]:
    """Holes and quarantines above the acknowledgement (contract P3-3)."""
    ack = int(binding.acknowledged_cursor)
    keys = _ledger_keys(binding)
    delivered = max([ack, *keys])
    holes = [seq for seq in range(ack + 1, delivered + 1) if seq not in keys]
    quarantined = sorted(
        int(key) for key, status in binding.inbox.items()
        if status == "quarantined" and int(key) > ack
    )
    code = "ack_hole" if holes else ("quarantine_held" if quarantined and quarantined[0] == ack + 1 else "")
    return {
        "acknowledgedSeq": ack,
        "deliveredSeq": delivered,
        "holeSeqs": holes[:REPORT_LIMIT],
        "quarantinedSeqs": quarantined[:REPORT_LIMIT],
        "ledgerCode": code,
    }


def intent_source_seq(intent: dict[str, Any]) -> int:
    selected = intent.get("selected") if isinstance(intent.get("selected"), dict) else {}
    try:
        return int((selected or {}).get("source_seq") or 0)
    except (TypeError, ValueError):
        return 0


def is_terminal_intent(intent: Any) -> bool:
    """Posted with its lifecycle done, superseded or skipped (contract P3-2)."""
    if not isinstance(intent, dict):
        return False
    delivery = str(intent.get("delivery_state") or "")
    state = str(intent.get("state") or "")
    lifecycle = str(intent.get("lifecycle_state") or "")
    if "quarantined" in {delivery, state}:
        return False
    if delivery == "posted" and lifecycle in {"complete", "not_required"} and state in {"posted", ""}:
        return True
    return delivery in {"superseded", "skipped"} and state in {"superseded", "skipped", ""}


def prune_terminal_intents(binding: Any, keep: int = TERMINAL_INTENT_RETENTION) -> list[str]:
    """Drop terminal intents beyond the newest ``keep`` (insertion order)."""
    terminal = [key for key, intent in binding.delivery_intents.items() if is_terminal_intent(intent)]
    pruned = terminal[:-keep] if keep > 0 else terminal
    for key in pruned:
        binding.delivery_intents.pop(key, None)
    return pruned


def stuck_intents(binding: Any) -> list[dict[str, Any]]:
    """Intents that cannot progress by themselves: quarantined, or open with an acknowledged source."""
    stuck: list[dict[str, Any]] = []
    ack = int(binding.acknowledged_cursor)
    for key, intent in binding.delivery_intents.items():
        if not isinstance(intent, dict) or is_terminal_intent(intent):
            continue
        seq = intent_source_seq(intent)
        quarantined = "quarantined" in {intent.get("delivery_state"), intent.get("state")}
        if quarantined or (seq and seq <= ack):
            stuck.append({
                "intentKey": key, "sourceSeq": seq,
                "state": str(intent.get("state") or intent.get("delivery_state") or ""),
                "lastErrorCode": str(intent.get("last_error_code") or ""),
            })
    return stuck


def prior_state(binding: Any, seq: int) -> dict[str, Any]:
    """What a closure would close: a hole or a quarantine; else refused."""
    ack = int(binding.acknowledged_cursor)
    if seq <= ack:
        raise RecoveryRefused("already_acknowledged", f"seq {seq} is at or below the acknowledgement {ack}")
    key = str(seq)
    status = str(binding.inbox.get(key) or "")
    if status == "quarantined":
        intent_key, code = "", ""
        for candidate, intent in binding.delivery_intents.items():
            if isinstance(intent, dict) and intent_source_seq(intent) == seq:
                intent_key, code = candidate, str(intent.get("last_error_code") or "")
        return {"kind": "quarantine", "intentKey": intent_key, "lastErrorCode": code}
    keys = _ledger_keys(binding)
    if seq not in keys and keys and seq < max(keys):
        return {"kind": "hole", "intentKey": "", "lastErrorCode": ""}
    raise RecoveryRefused("not_closable", f"seq {seq} is neither a hole nor a quarantine (ledger {status or 'none'})")


# --- server proof (read-only, the member's own credential) -------------------


def server_proof(api: Any, room_id: str, membership_id: str, seq: int, reason: str,
                 evidence_ref: str) -> dict[str, Any]:
    """The Room's proof that seq is terminal for this member, or RecoveryRefused."""
    if reason not in REASONS:
        raise RecoveryRefused("unknown_reason", f"reason must be one of {', '.join(REASONS)}")
    listing = api.epochs(room_id) or {}
    epochs = sorted(
        (item for item in (listing.get("epochs") if isinstance(listing, dict) else listing) or []
         if isinstance(item, dict) and isinstance(item.get("startsAtSeq"), int)),
        key=lambda item: item["startsAtSeq"],
    )
    containing = [item for item in epochs if item["startsAtSeq"] <= seq]
    if not containing:
        raise RecoveryRefused("event_not_found", f"no discussion epoch contains seq {seq}")
    epoch = containing[-1]
    epoch_id = str(epoch.get("id") or "")
    # An epoch-filtered read never moves the server's delivered seq.
    page = api.events_in_epoch(room_id, seq - 1, epoch_id) or {}
    event = next((item for item in page.get("events") or [] if int(item.get("seq") or 0) == seq), None)
    if not isinstance(event, dict):
        raise RecoveryRefused("event_not_found", f"the Room returned no event at seq {seq}")
    state = api.room_state(room_id) or {}
    active_epoch_id = str((state.get("activeEpoch") or {}).get("id") or "")
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    proof: dict[str, Any] = {
        "activeEpochId": active_epoch_id,
        "eventEpochStatus": str(epoch.get("status") or ""),
        "cycleId": "", "cycleState": "", "cycleGeneration": 0, "eventGeneration": 0,
        "deliveredSeq": int(page.get("deliveredSeq") or 0),
        "fetchedAt": _now(),
    }
    facts = {"eventId": str(event.get("id") or ""), "eventType": str(event.get("type") or ""), "eventEpochId": epoch_id}
    if reason == "epoch_closed":
        if evidence_ref != f"epoch:{epoch_id}":
            raise RecoveryRefused("evidence_mismatch", f"evidence must be epoch:{epoch_id}")
        if not epoch_id or epoch_id == active_epoch_id or proof["eventEpochStatus"] == "active":
            raise RecoveryRefused("not_proven", f"epoch {epoch_id or '?'} is still active")
        return {**facts, "proof": proof}
    cycle_id = str(payload.get("cycleId") or "")
    if not cycle_id:
        raise RecoveryRefused("not_proven", f"seq {seq} ({facts['eventType']}) names no cycle")
    if evidence_ref != f"cycle:{cycle_id}":
        raise RecoveryRefused("evidence_mismatch", f"evidence must be cycle:{cycle_id}")
    target = str(payload.get("membershipId") or "")
    if facts["eventType"] == "discussion.cycle_attempt_ready" and target and target != membership_id:
        raise RecoveryRefused("not_for_member", f"seq {seq} is another member's attempt")
    cycle = api.get_discussion_cycle(room_id, cycle_id) or {}
    cycle = cycle.get("cycle") if isinstance(cycle.get("cycle"), dict) else cycle
    proof.update({
        "cycleId": cycle_id,
        "cycleState": str(cycle.get("state") or ""),
        "cycleGeneration": int(cycle.get("generation") or 0),
        "eventGeneration": int(payload.get("generation") or 0),
    })
    if reason == "cycle_terminal" and proof["cycleState"] not in TERMINAL_CYCLE_STATES:
        raise RecoveryRefused("not_proven", f"cycle {cycle_id} is {proof['cycleState'] or 'unknown'}, not terminal")
    if reason == "cycle_superseded" and not (proof["eventGeneration"] and proof["cycleGeneration"] > proof["eventGeneration"]):
        raise RecoveryRefused("not_proven", f"cycle {cycle_id} has no newer generation than the event")
    return {**facts, "proof": proof}


# --- audit record ---------------------------------------------------------------


def confirm_token(record: dict[str, Any]) -> str:
    stable = {key: value for key, value in record.items() if key not in _TOKEN_EXCLUDED}
    stable["proof"] = {key: value for key, value in (record.get("proof") or {}).items() if key != "fetchedAt"}
    return hashlib.sha256(canonical(stable).encode()).hexdigest()[:16]


def record_digest(record: dict[str, Any]) -> str:
    return hashlib.sha256(canonical({key: value for key, value in record.items() if key != "recordDigest"}).encode()).hexdigest()


def build_record(*, connector: str, room_id: str, membership_id: str, client_instance_id: str, seq: int,
                 reason: str, evidence_ref: str, proven: dict[str, Any], prior: dict[str, Any],
                 operator: str) -> dict[str, Any]:
    record = {
        "version": 1, "verb": "close", "connector": connector,
        "room": room_id, "membership": membership_id, "clientInstanceId": client_instance_id,
        "seq": seq, "eventId": proven["eventId"], "eventType": proven["eventType"],
        "eventEpochId": proven["eventEpochId"], "reason": reason, "evidenceRef": evidence_ref,
        "proof": dict(proven["proof"]), "priorState": dict(prior),
        "operator": operator, "confirmToken": "", "recordedAt": "", "recordDigest": "",
    }
    record["confirmToken"] = confirm_token(record)
    return record


def seal(record: dict[str, Any]) -> dict[str, Any]:
    sealed = dict(record, recordedAt=_now())
    sealed["recordDigest"] = record_digest(sealed)
    return sealed


def journal_path(state_file: Path) -> Path:
    state_file = Path(state_file)
    return state_file.with_name(state_file.name + ".recovery-audit.jsonl")


def append_record(path: Path, record: dict[str, Any]) -> None:
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        os.write(fd, (canonical(record) + "\n").encode())
        os.fsync(fd)
    finally:
        os.close(fd)


def read_records(path: Path) -> list[dict[str, Any]]:
    """Valid sealed records; a malformed or altered line is skipped."""
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []
    records: list[dict[str, Any]] = []
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (isinstance(record, dict) and record.get("version") == 1 and record.get("verb") == "close"
                and record.get("recordDigest") == record_digest(record)
                and record.get("confirmToken") == confirm_token(record)):
            records.append(record)
    return records


def closures_for(records: Iterable[dict[str, Any]], room_id: str, membership_id: str,
                 client_instance_id: str) -> dict[int, dict[str, Any]]:
    """The latest sealed closure per seq for exactly this binding."""
    found: dict[int, dict[str, Any]] = {}
    for record in records:
        if (record.get("room") == room_id and record.get("membership") == membership_id
                and record.get("clientInstanceId") == client_instance_id and isinstance(record.get("seq"), int)):
            found[record["seq"]] = record
    return found


def render_record(record: dict[str, Any]) -> str:
    return json.dumps(record, indent=2, sort_keys=True)


def describe(report: dict[str, Any], stuck: list[dict[str, Any]], write: Callable[[str], None]) -> None:
    write(f"  acknowledged {report['acknowledgedSeq']}, highest read {report['deliveredSeq']}, code {report['ledgerCode'] or '-'}")
    write(f"  holes: {', '.join(map(str, report['holeSeqs'])) or 'none'}")
    write(f"  quarantines: {', '.join(map(str, report['quarantinedSeqs'])) or 'none'}")
    if stuck:
        for item in stuck:
            write(f"  stuck intent {item['intentKey']}: source seq {item['sourceSeq'] or '?'}, "
                  f"{item['state'] or '-'}, {item['lastErrorCode'] or '-'}")
    else:
        write("  stuck intents: none")

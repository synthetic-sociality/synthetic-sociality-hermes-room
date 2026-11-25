"""Contract P3: ledger report, terminal-intent retention and audited recovery.

Shared with OpenClaw (src/recovery.js): the same verbs, reasons, server proof,
audit record keys, confirmation token and record digest. Nothing here writes a
cursor, an acknowledgement, an intent or a quarantine. A closure is an
append-only audit record; the running connector applies it through its normal
acknowledgement path after re-checking the Room's proof.

The record digest is an unkeyed sha256 and the operator name is not
authenticated: anyone who can write the journal can write a well-formed record.
Safety rests on the Room's proof being checked again when the record is
applied (contract P3-4) and on the store fence that keeps agents out of it.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

REASONS = ("cycle_terminal", "cycle_superseded", "epoch_closed")
# Contract P2c-3h: the one terminal-state set of this connector. Every terminal
# check (frontier walk, retention, renewal quiescence, owner-control fence)
# uses it; ``stuck`` is terminal evidence written by the stuck rule.
STUCK_STATE = "stuck"
TERMINAL_EVENT_STATES = frozenset({"posted", "skipped", "failed", "cancelled", "superseded", "ignored", STUCK_STATE})
STUCK_TRIGGERS = ("epoch_closed", "cycle_terminal", "cycle_superseded", "age", "hole_epoch_closed")
STUCK_REASON = re.compile(r"^stuck:(epoch_closed|cycle_terminal|cycle_superseded|age|hole_epoch_closed):([a-z0-9_]{1,100})$")
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
    return (delivery in {"superseded", "skipped"} & TERMINAL_EVENT_STATES
            and state in {"superseded", "skipped", ""})


def prune_terminal_intents(binding: Any, keep: int = TERMINAL_INTENT_RETENTION) -> list[str]:
    """Drop terminal intents beyond the newest ``keep`` (insertion order)."""
    terminal = [key for key, intent in binding.delivery_intents.items() if is_terminal_intent(intent)]
    pruned = terminal[:-keep] if keep > 0 else terminal
    for key in pruned:
        binding.delivery_intents.pop(key, None)
    return pruned


# Contract P2c-3i: the Room is the authority for the acknowledged frontier.
ROOM_ACK_AHEAD = "room_ack_ahead"
ACK_FRONTIER_MISMATCH = "ack_frontier_mismatch"


class AckFrontierConflict(ValueError):
    """The Room's acknowledgement is behind the local one, or not an integer."""

    code = ACK_FRONTIER_MISMATCH


def reconcile_room_ack(binding: Any, room_ack: Any, *, frontier: int | None = None,
                       delivered: Any = None, conflict: str = "") -> dict[str, Any]:
    """Contract P2c-3i: the one place a Room acknowledgement meets a local cursor.

    ``frontier`` is the local value compared (default: the acknowledged
    cursor; the live acknowledgement passes its walked frontier). Equal: no
    change. Room ahead: adopted as a fence; every seq up to it without
    terminal evidence becomes ``ignored`` with reason room_ack_ahead (existing
    evidence is kept and the adopted range keeps its records until the next
    frontier advance prunes them), the cursors move and the binding's maps are
    pruned. Room behind, a non-integer answer, or a ``delivered`` below the
    Room's acknowledgement or the local read cursor: AckFrontierConflict, and
    nothing changes. The caller persists and logs (``room_ack_ahead_message``).
    """
    local = int(binding.acknowledged_cursor if frontier is None else frontier)
    message = conflict or "Room acknowledgement did not confirm the locally proven contiguous frontier"
    if type(room_ack) is not int or room_ack < local or room_ack < int(binding.acknowledged_cursor):
        raise AckFrontierConflict(message)
    if delivered is not None and (type(delivered) is not int or delivered < room_ack
                                  or delivered < int(binding.cursor)):
        raise AckFrontierConflict(message)
    if room_ack == local:
        return {"adopted": False, "frontier": local, "acknowledged": room_ack, "seqs": []}
    previous = int(binding.acknowledged_cursor)
    marked: list[int] = []
    for seq in range(previous + 1, room_ack + 1):
        key = str(seq)
        status = binding.inbox.get(key)
        evidence = binding.terminal_evidence.get(key)
        if status in TERMINAL_EVENT_STATES and isinstance(evidence, dict) and evidence.get("status") == status:
            continue
        source_id = next((source for source, source_seq in binding.turn_sequences.items() if source_seq == seq), "")
        binding.inbox[key] = "ignored"
        binding.terminal_evidence[key] = {
            "status": "ignored", "sourceEventId": source_id, "canonicalEventId": "",
            "canonicalSeq": 0, "canonicalTs": "", "reason": ROOM_ACK_AHEAD,
        }
        marked.append(seq)
    binding.acknowledged_cursor = room_ack
    binding.cursor = max(int(binding.cursor), room_ack)
    binding.inbox = {key: value for key, value in binding.inbox.items() if int(key) > previous}
    binding.terminal_evidence = {key: value for key, value in binding.terminal_evidence.items() if int(key) > previous}
    binding.pending_since = {key: value for key, value in binding.pending_since.items()
                             if int(key) > room_ack and binding.inbox.get(key) in {"pending", "quarantined"}}
    binding.pending_retries = {key: value for key, value in binding.pending_retries.items() if int(key) > room_ack}
    for source_id in [source for source, source_seq in binding.turn_sequences.items() if source_seq <= room_ack]:
        binding.turn_sequences.pop(source_id, None)
        binding.turn_observed.pop(source_id, None)
    prune_terminal_intents(binding)
    return {"adopted": True, "frontier": local, "acknowledged": room_ack, "seqs": marked}


def room_ack_ahead_message(room_id: str, result: dict[str, Any], where: str) -> str:
    """The one warning line per adoption (contract P2c-3i)."""
    return (f"Room {room_id} acknowledgement ahead of the local cursor adopted ({where}): frontier "
            f"{result['frontier']}, Room {result['acknowledged']}, {len(result['seqs'])} seqs ignored with reason "
            f"{ROOM_ACK_AHEAD} where no terminal evidence existed")


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


def _read_event(api: Any, room_id: str, seq: int) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    """The epoch holding seq, the event, its epoch page and the Room state (read-only)."""
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
    return epoch, event, page, state


def server_proof(api: Any, room_id: str, membership_id: str, seq: int, reason: str,
                 evidence_ref: str) -> dict[str, Any]:
    """The Room's proof that seq is terminal for this member, or RecoveryRefused."""
    if reason not in REASONS:
        raise RecoveryRefused("unknown_reason", f"reason must be one of {', '.join(REASONS)}")
    epoch, event, page, state = _read_event(api, room_id, seq)
    epoch_id = str(epoch.get("id") or "")
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


# --- stuck events (contract P2c-3h) ----------------------------------------------


def stuck_code(code: Any) -> str:
    """The original code in a stuck reason: lower-case [a-z0-9_], else the literal none."""
    text = re.sub(r"[^a-z0-9_]+", "_", str(code or "").strip().lower()).strip("_")[:100].strip("_")
    return text or "none"


def stuck_reason(trigger: str, original_code: Any) -> str:
    """stuck:<trigger>:<original code|none>; a hole never carries a code."""
    if trigger not in STUCK_TRIGGERS:
        raise ValueError(f"stuck trigger must be one of {', '.join(STUCK_TRIGGERS)}")
    return f"stuck:{trigger}:{'none' if trigger == 'hole_epoch_closed' else stuck_code(original_code)}"


def valid_stuck_reason(reason: Any) -> bool:
    return isinstance(reason, str) and STUCK_REASON.fullmatch(reason) is not None


def stuck_proof(api: Any, room_id: str, membership_id: str, seq: int) -> dict[str, Any]:
    """What the Room proves about seq for the stuck rule, in one read set.

    ``trigger`` is epoch_closed, cycle_terminal, cycle_superseded or "" (not
    proven). ``proofBytes`` is the canonical JSON of the Room's own objects the
    proof rests on (the event's epoch, the active epoch, the cycle when read).
    Raises RecoveryRefused when the Room has no such event.
    """
    epoch, event, page, state = _read_event(api, room_id, seq)
    epoch_id = str(epoch.get("id") or "")
    active = state.get("activeEpoch") if isinstance(state.get("activeEpoch"), dict) else {}
    active_epoch_id = str(active.get("id") or "")
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    proof: dict[str, Any] = {
        "activeEpochId": active_epoch_id,
        "eventEpochStatus": str(epoch.get("status") or ""),
        "cycleId": "", "cycleState": "", "cycleGeneration": 0, "eventGeneration": 0,
        "deliveredSeq": int(page.get("deliveredSeq") or 0),
        "fetchedAt": _now(),
    }
    facts = {"eventId": str(event.get("id") or ""), "eventType": str(event.get("type") or ""), "eventEpochId": epoch_id}
    raw: dict[str, Any] = {"event": {"id": facts["eventId"], "seq": seq, "type": facts["eventType"]},
                           "epoch": epoch, "activeEpoch": active}
    trigger, evidence_ref = "", ""
    if epoch_id and epoch_id != active_epoch_id and proof["eventEpochStatus"] != "active":
        trigger, evidence_ref = "epoch_closed", f"epoch:{epoch_id}"
    else:
        cycle_id = str(payload.get("cycleId") or "")
        target = str(payload.get("membershipId") or "")
        other_member = facts["eventType"] == "discussion.cycle_attempt_ready" and target and target != membership_id
        if cycle_id and not other_member:
            cycle = api.get_discussion_cycle(room_id, cycle_id) or {}
            cycle = cycle.get("cycle") if isinstance(cycle.get("cycle"), dict) else cycle
            raw["cycle"] = cycle
            proof.update({
                "cycleId": cycle_id,
                "cycleState": str(cycle.get("state") or ""),
                "cycleGeneration": int(cycle.get("generation") or 0),
                "eventGeneration": int(payload.get("generation") or 0),
            })
            if proof["cycleState"] in TERMINAL_CYCLE_STATES:
                trigger, evidence_ref = "cycle_terminal", f"cycle:{cycle_id}"
            elif proof["eventGeneration"] and proof["cycleGeneration"] > proof["eventGeneration"]:
                trigger, evidence_ref = "cycle_superseded", f"cycle:{cycle_id}"
    return {**facts, "trigger": trigger, "evidenceRef": evidence_ref, "proof": proof, "proofBytes": canonical(raw)}


def stuck_summary(binding: Any) -> dict[str, Any]:
    """Local status fields for the stuck events this binding recorded (P2c-3h)."""
    reasons = {
        int(key): str(mark.get("reason") or "")
        for key, mark in binding.recovery_closures.items()
        if str(key).isdigit() and isinstance(mark, dict) and valid_stuck_reason(mark.get("reason"))
    }
    seqs = sorted(reasons)
    counts: dict[str, int] = {}
    for reason in reasons.values():
        trigger = reason.split(":")[1]
        counts[trigger] = counts.get(trigger, 0) + 1
    recent = seqs[-REPORT_LIMIT:]
    return {
        "stuckSeqs": recent,
        "stuckReasons": {str(seq): reasons[seq] for seq in recent},
        "stuckCount": len(seqs),
        "stuckCounts": counts,
        "lastStuckSeq": seqs[-1] if seqs else 0,
    }


# --- audit record ---------------------------------------------------------------


def confirm_token(record: dict[str, Any]) -> str:
    stable = {key: value for key, value in record.items() if key not in _TOKEN_EXCLUDED}
    proof = record.get("proof")
    if not isinstance(proof, dict):
        raise ValueError("audit record proof must be an object")
    stable["proof"] = {key: value for key, value in proof.items() if key != "fetchedAt"}
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


def build_stuck_record(*, connector: str, room_id: str, membership_id: str, client_instance_id: str, seq: int,
                       trigger: str, original_code: Any, facts: dict[str, Any], evidence_ref: str,
                       proof: dict[str, Any], proof_bytes: str, prior: dict[str, Any]) -> dict[str, Any]:
    """Contract P2c-3h: the connector's own audit record for a stuck event (verb ``stuck``).

    Same envelope as a closure record; it is never read as an operator closure.
    """
    record = {
        "version": 1, "verb": STUCK_STATE, "connector": connector,
        "room": room_id, "membership": membership_id, "clientInstanceId": client_instance_id,
        "seq": seq, "eventId": str(facts.get("eventId") or ""), "eventType": str(facts.get("eventType") or ""),
        "eventEpochId": str(facts.get("eventEpochId") or ""), "reason": stuck_reason(trigger, original_code),
        "evidenceRef": evidence_ref, "proof": dict(proof), "priorState": dict(prior),
        "proofBytes": proof_bytes, "proofSha256": hashlib.sha256(proof_bytes.encode("utf-8")).hexdigest(),
        "operator": "connector", "confirmToken": "", "recordedAt": "", "recordDigest": "",
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


def read_records(path: Path, verb: str = "close") -> list[dict[str, Any]]:
    """Valid sealed records; a malformed, undecodable or altered line is skipped.

    Contract P3-4: an unreadable journal line or record is never fatal.
    """
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except FileNotFoundError:
        return []
    records: list[dict[str, Any]] = []
    for line in lines:
        try:
            record = json.loads(line)
            if (isinstance(record, dict) and record.get("version") == 1 and record.get("verb") == verb
                    and isinstance(record.get("proof"), dict) and isinstance(record.get("priorState"), dict)
                    and record.get("recordDigest") == record_digest(record)
                    and record.get("confirmToken") == confirm_token(record)):
                records.append(record)
        except Exception:  # noqa: BLE001 - one bad line never hides the others
            continue
    return records


def read_stuck_records(path: Path) -> list[dict[str, Any]]:
    """Contract P2c-3h: the connector's sealed stuck records (never closures)."""
    return [record for record in read_records(path, STUCK_STATE) if valid_stuck_reason(record.get("reason"))]


def _journal_identity(path: Path) -> tuple[int, ...] | None:
    try:
        info = os.stat(path)
    except FileNotFoundError:
        return None
    return (info.st_size, info.st_mtime_ns, info.st_ino, info.st_dev)


_cached_records: dict[str, tuple[tuple[int, ...], list[dict[str, Any]]]] = {}


def read_records_cached(path: Path) -> list[dict[str, Any]]:
    """Contract P8-6: ``read_records`` without reading a journal that has not changed.

    An append by the recovery tool changes its size and modification time. The
    records are shared and must not be changed.
    """
    key = str(path)
    identity = _journal_identity(path)
    if identity is None:
        _cached_records.pop(key, None)
        return []
    cached = _cached_records.get(key)
    if cached is not None and cached[0] == identity:
        return cached[1]
    records = read_records(path)
    if _journal_identity(path) == identity:
        _cached_records[key] = (identity, records)
    else:
        _cached_records.pop(key, None)
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

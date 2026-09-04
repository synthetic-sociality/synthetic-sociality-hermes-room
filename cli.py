"""`hermes room` onboarding and lifecycle commands."""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import platform
import re
import secrets
import sys
from copy import deepcopy
from datetime import datetime, timezone
from urllib.parse import urlsplit

from .protocol import ProtocolError, RoomProtocol, parse_invitation_url, stable_key
from .state import RoomBinding, load, remove, update, upsert


LEGACY_IDEMPOTENCY_MISMATCH_ERROR = "idempotency key payload mismatch"


def setup_cli(parser: argparse.ArgumentParser) -> None:
    commands = parser.add_subparsers(dest="room_command")
    join = commands.add_parser("join", help="Review and accept a Room invitation")
    join.add_argument("--base-url", help="Room origin or API URL (required when entering a short code)")
    join.add_argument("--name", help="Fallback identity for legacy invitations or short pairing codes")
    join.add_argument("--yes", action="store_true", help="Accept after displaying the review (non-interactive)")
    commands.add_parser("status", help="Show Room memberships and connector state")
    leave = commands.add_parser("leave", help="Disable and remove one local Room membership")
    leave.add_argument("room_id")
    toggle = commands.add_parser("enable", help="Enable one local Room membership")
    toggle.add_argument("room_id")
    toggle = commands.add_parser("disable", help="Disable one local Room membership")
    toggle.add_argument("room_id")
    rotate_session = commands.add_parser(
        "rotate-current-epoch-session",
        help="Authorize a fresh Hermes transcript for one Room's current epoch",
    )
    rotate_session.add_argument("room_id")
    rotate_session.add_argument(
        "--yes", action="store_true",
        help="Confirm the one-binding current-epoch transcript rotation (required)",
    )
    fence = commands.add_parser(
        "fence",
        help="Set or clear the audited recovery fence for one Room membership",
    )
    fence.add_argument("room_id")
    fence.add_argument(
        "--until", type=int, default=0,
        help=("Exact canonical sequence to fence through (inclusive). "
              "Sequences in (acknowledged_cursor, until] are marked terminally "
              "ignored via the normal connector ack path on consumption; beyond "
              "is processed normally. 0 clears the fence."),
    )
    collision = commands.add_parser(
        "recover-idempotency-collision",
        help="Audit and re-key one proven foreign-actor legacy collision",
    )
    collision.add_argument("room_id")
    collision.add_argument("source_seq", type=int)
    collision.add_argument("source_event_id")
    collision.add_argument("colliding_event_id")
    collision.add_argument("colliding_actor_membership_id")
    collision.add_argument(
        "--yes", action="store_true",
        help="Confirm the exact audited recovery (required)",
    )
    orphan = commands.add_parser(
        "recover-orphaned-intent",
        help="Close one proven post intent left behind by a terminal zero-byte cycle pass",
    )
    orphan.add_argument("room_id")
    orphan.add_argument("source_seq", type=int)
    orphan.add_argument("source_event_id")
    orphan.add_argument("cycle_started_event_id")
    orphan.add_argument("cycle_terminal_event_id")
    orphan.add_argument("body_sha256")
    orphan.add_argument(
        "--yes", action="store_true",
        help="Confirm the exact audited non-replayable closure (required)",
    )
    stale_context = commands.add_parser(
        "recover-stale-context-idempotency",
        help="Close one proven stale-context rekey failure after its canonical cycle contribution",
    )
    stale_context.add_argument("room_id")
    stale_context.add_argument("source_seq", type=int)
    stale_context.add_argument("source_event_id")
    stale_context.add_argument("canonical_event_id")
    stale_context.add_argument("cycle_id")
    stale_context.add_argument("attempt_id")
    stale_context.add_argument("local_body_sha256")
    stale_context.add_argument("canonical_body_sha256")
    stale_context.add_argument(
        "--yes", action="store_true",
        help="Confirm the exact audited non-replayable closure (required)",
    )
    lifecycle = commands.add_parser(
        "reconcile-terminal-lifecycle",
        help="Close one canonically posted lifecycle after authoritative terminal-cycle proof",
    )
    lifecycle.add_argument("room_id")
    lifecycle.add_argument("source_seq", type=int)
    lifecycle.add_argument("source_event_id")
    lifecycle.add_argument("canonical_event_id")
    lifecycle.add_argument("cycle_id")
    lifecycle.add_argument("terminal_state", choices=("completed", "interrupted"))
    lifecycle.add_argument(
        "--yes", action="store_true",
        help="Confirm the exact audited non-replayable lifecycle closure (required)",
    )
    renew = commands.add_parser(
        "renew",
        help="Resume an owner-authorized identity-preserving credential renewal",
    )
    renew.add_argument("room_id")
    renew.add_argument(
        "--request-owner", action="store_true",
        help="Create a secret-opaque owner approval request instead of entering a grant",
    )


def dispatch(args: argparse.Namespace) -> int:
    command = getattr(args, "room_command", None) or "status"
    try:
        if command == "join":
            return _join(args)
        if command == "status":
            return _status()
        if command == "leave":
            return _leave(args.room_id)
        if command in {"enable", "disable"}:
            return _toggle(args.room_id, command == "enable")
        if command == "rotate-current-epoch-session":
            return _rotate_current_epoch_session(args.room_id, confirmed=bool(args.yes))
        if command == "fence":
            return _fence(args.room_id, args.until)
        if command == "recover-idempotency-collision":
            return _recover_idempotency_collision(args)
        if command == "recover-orphaned-intent":
            return _recover_orphaned_intent(args)
        if command == "recover-stale-context-idempotency":
            return _recover_stale_context_idempotency(args)
        if command == "reconcile-terminal-lifecycle":
            return _reconcile_terminal_lifecycle(args)
        if command == "renew":
            return _renew(args.room_id, request_owner=bool(args.request_owner))
    except (ValueError, ProtocolError, OSError, json.JSONDecodeError) as error:
        print(f"Room command failed: {error}", file=sys.stderr)
        return 1
    print(f"Unknown room command: {command}", file=sys.stderr)
    return 2


def _join(args: argparse.Namespace) -> int:
    supplied = str(getattr(args, "code", "") or "").strip()
    if supplied:
        raise ValueError("invitation links and device codes are secrets; run `hermes room join` and enter them at the hidden prompt")
    descriptor = f"Hermes Agent profile on {platform.system()} ({platform.machine()})"
    review = None
    raw = getpass.getpass("Invitation link or device code (hidden): ").strip()
    if re.fullmatch(r"[A-HJ-NP-Z2-7]{8}", raw.upper()):
        base_url = _api_url(args.base_url or "")
        protocol = RoomProtocol(base_url)
        proposed = args.name or os.environ.get("HERMES_PROFILE", "Hermes Agent")
        print("A short pairing code is one-use and cannot be previewed by itself.")
        if not args.yes and input("Accept this Room invitation? [y/N] ").strip().lower() not in {"y", "yes"}:
            print("Invitation left untouched.")
            return 0
        exchange = protocol.redeem_pairing(raw, proposed, descriptor)
        del raw
    else:
        capability = parse_invitation_url(raw)
        protocol = RoomProtocol(capability.base_url)
        review = protocol.review(capability.invitation_id)
        _print_review(review)
        # A link invitation is authoritative: the Room owner assigned both the
        # formal identity and its room name before sending the capability.
        proposed = review.get("formalAgentName") or args.name or review.get("proposedAgentName") or os.environ.get("HERMES_PROFILE", "Hermes Agent")
        if not args.yes and input(f"Join as {proposed!r}? [y/N] ").strip().lower() not in {"y", "yes"}:
            print("Invitation left untouched.")
            return 0
        exchange = protocol.redeem(capability.invitation_id, capability.secret, proposed, descriptor)
        # The fragment-held secret exists only in this stack frame and is never
        # written to state, logs, config, or environment.
        del raw, capability
    head_seq = int(exchange.get("headSeq") or 0)
    binding = RoomBinding(
        base_url=protocol.base_url,
        room_id=exchange["roomId"],
        membership_id=exchange["membershipId"],
        credential=exchange["credential"],
        credential_expires_at=exchange.get("credentialExpiresAt", ""),
        display_name=proposed,
        identity_version=int(exchange.get("identityVersion") or 0),
        cursor=head_seq,
        acknowledged_cursor=head_seq,
    )
    update(lambda current: upsert(current, binding))
    print(f"Joined {review.get('roomTitle') if review else binding.room_id} as {proposed}.")
    print("Restart the Hermes gateway to activate the Room connector.")
    return 0


def _print_review(review: dict) -> None:
    print()
    print(f"Room:        {review.get('roomTitle') or review.get('roomId')}")
    if review.get("purpose"):
        print(f"Purpose:     {review['purpose']}")
    if review.get("inviterName"):
        print(f"Invited by:  {review['inviterName']}")
    identity = review.get("formalAgentName") or review.get("proposedAgentName")
    if identity:
        print(f"Identity:    {identity}")
    room_name = review.get("proposedAgentName")
    if room_name and room_name != identity:
        print(f"In room:     {room_name}")
    print(f"Role:        {review.get('role', 'participant_agent')}")
    print(f"Expires:     {review.get('expiresAt', 'unknown')}")
    print(f"Available:   {'yes' if review.get('consumable') else 'no'}")
    print()


def _status() -> int:
    current = load()
    if not current.bindings:
        print("No Synthetic Sociality Room memberships configured.")
        return 0
    for binding in current.bindings:
        state = "revoked" if binding.revoked else ("enabled" if binding.enabled else "disabled")
        expiry = _expiry_status(binding.credential_expires_at)
        quarantined = sorted(int(seq) for seq, status in binding.inbox.items() if status == "quarantined")
        failed = sorted(int(seq) for seq, status in binding.inbox.items() if status == "failed-retryable")
        recovery = ""
        if binding.recovery_fence_cutoff > 0:
            recovery += f"  FENCE until={binding.recovery_fence_cutoff}"
        if quarantined:
            recovery += "  QUARANTINED seq=" + ",".join(str(seq) for seq in quarantined)
        if failed:
            recovery += "  retryable seq=" + ",".join(str(seq) for seq in failed)
        selected = [
            intent.get("selected") or {}
            for intent in binding.delivery_intents.values()
            if isinstance(intent, dict) and isinstance(intent.get("selected"), dict)
        ]
        overtaken = [
            intent for intent in selected
            if 0 < int(intent.get("source_seq") or 0) <= binding.acknowledged_cursor
        ]
        if selected:
            oldest = min(float(intent.get("selected_at") or 0.0) for intent in selected)
            age = max(0, int(datetime.now(timezone.utc).timestamp() - oldest)) if oldest else 0
            recovery += f"  INTENTS selected={len(selected)} oldest={age}s"
        if overtaken:
            recovery += f"  ORPHANED={len(overtaken)}"
        print(f"{binding.room_id}  {binding.display_name or binding.membership_id}  {state}  {binding.transport}{expiry}{recovery}")
    return 0


def _leave(room_id: str) -> int:
    binding = load().binding(room_id)
    if binding and binding.connector_session_id and not binding.revoked:
        try:
            RoomProtocol(binding.base_url, binding.credential).disconnect(room_id, binding.connector_session_id, False)
        except ProtocolError:
            pass
    if not update(lambda current: remove(current, room_id)):
        raise ValueError(f"room {room_id!r} is not configured")
    print(f"Removed local membership state for {room_id}. The Room owner controls server-side revocation.")
    return 0


def _toggle(room_id: str, enabled: bool) -> int:
    binding = load().binding(room_id)
    if binding is None:
        raise ValueError(f"room {room_id!r} is not configured")
    binding.enabled = enabled
    if not enabled and binding.connector_session_id and not binding.revoked:
        try:
            RoomProtocol(binding.base_url, binding.credential).disconnect(
                room_id, binding.connector_session_id, False
            )
        except ProtocolError:
            pass
    def set_enabled(current):
        latest = current.binding(room_id)
        if latest is None:
            raise ValueError(f"room {room_id!r} is not configured")
        latest.enabled = enabled

    update(set_enabled)
    if enabled:
        print(f"{room_id} enabled; restart the Hermes gateway to activate it.")
    else:
        print(f"{room_id} disabled; the running connector will stop without reconnecting.")
    return 0


def _rotate_current_epoch_session(room_id: str, *, confirmed: bool) -> int:
    """Authorize one binding to leave its legacy baseline on restart."""
    if not confirmed:
        raise ValueError("--yes is required for current-epoch session rotation")
    if load().binding(room_id) is None:
        raise ValueError(f"room {room_id!r} is not configured")

    def authorize(current):
        latest = current.binding(room_id)
        if latest is None:
            raise ValueError(f"room {room_id!r} is not configured")
        latest.epoch_session_routing_initialized = False
        latest.legacy_session_epoch_id = ""
        latest.rotate_current_epoch_session = True

    update(authorize)
    print(
        f"{room_id} current-epoch transcript rotation authorized. "
        "Restart the Hermes gateway to apply it to this binding only."
    )
    return 0


def _fence(room_id: str, until: int) -> int:
    """Set or clear the audited recovery fence for one Room membership.

    ``until`` is the exact inclusive canonical sequence to fence through.
    Sequences in ``(acknowledged_cursor, until]`` are marked terminally
    ``ignored`` (reason ``recovery_fence``) through the normal connector ack
    path on consumption — never via a direct cursor/DB write. Sequences beyond
    ``until`` are processed normally. ``until=0`` clears the fence.
    """
    if not load().binding(room_id):
        raise ValueError(f"room {room_id!r} is not configured")

    def set_fence(current):
        latest = current.binding(room_id)
        if latest is None:
            raise ValueError(f"room {room_id!r} is not configured")
        latest.recovery_fence_cutoff = int(until)

    update(set_fence)
    if until > 0:
        print(
            f"{room_id} recovery fence set through seq {until}. "
            f"Sequences in (acknowledged_cursor, {until}] are marked ignored via "
            "the normal ack path on consumption; beyond is processed normally."
        )
    else:
        print(f"{room_id} recovery fence cleared.")
    return 0


def _recover_idempotency_collision(args: argparse.Namespace) -> int:
    """Re-key one locally quarantined, independently identified collision.

    This is deliberately not a generic 409 bypass. It accepts only the exact
    pre-v2 deterministic message key, proves the named source and foreign
    colliding event against the canonical read API, and atomically records the
    provenance before making that one sequence retryable. Cursor and ack are
    never edited.
    """
    if not args.yes:
        raise ValueError("--yes is required for an audited collision recovery")
    source_seq = int(args.source_seq)
    if source_seq < 1:
        raise ValueError("source_seq must be positive")
    binding = load().binding(args.room_id)
    if binding is None:
        raise ValueError(f"room {args.room_id!r} is not configured")
    if args.colliding_actor_membership_id == binding.membership_id:
        raise ValueError("collision recovery requires a different canonical actor")
    page = RoomProtocol(binding.base_url, binding.credential).events(
        # This proof belongs to the active discussion epoch. Using the
        # all-epochs delivery lane would truthfully advance the membership's
        # server-side delivered high-water even though this operator command
        # does not hand those newer events to the connector runtime.
        binding.room_id, source_seq - 1, 0, all_epochs=False,
    )
    canonical = {str(event.get("id") or ""): event for event in page.get("events") or []}
    source = canonical.get(args.source_event_id)
    collision = canonical.get(args.colliding_event_id)
    if int((source or {}).get("seq") or 0) != source_seq:
        raise ValueError("canonical source event does not match source_seq")
    if str((collision or {}).get("actorId") or "") != args.colliding_actor_membership_id:
        raise ValueError("canonical colliding event does not match the named foreign actor")
    if str((collision or {}).get("type") or "") != "message.posted":
        raise ValueError("canonical colliding event is not a message.posted operation")
    collision_payload = (collision or {}).get("payload") or {}
    refs = {str(value) for value in ((collision or {}).get("refs") or [])}
    responds_to = {
        str(value) for value in (
            collision_payload.get("respondsTo") or collision_payload.get("responds_to") or []
        )
    } if isinstance(collision_payload, dict) else set()
    if args.source_event_id not in refs | responds_to:
        raise ValueError("canonical colliding message is not causally linked to the source event")

    legacy_key = stable_key("message", args.source_event_id)
    v2_key = stable_key(
        "message", args.source_event_id,
        room_id=binding.room_id, membership_id=binding.membership_id,
    )

    def recover(current):
        latest = current.binding(args.room_id)
        if latest is None or latest.membership_id != binding.membership_id:
            raise ValueError("Room membership changed during collision recovery")
        if latest.inbox.get(str(source_seq)) != "quarantined":
            raise ValueError("source sequence is not quarantined")
        if int(latest.turn_sequences.get(args.source_event_id) or 0) != source_seq:
            raise ValueError("local source-to-sequence provenance does not match")
        intent = latest.delivery_intents.get(args.source_event_id)
        if not isinstance(intent, dict):
            raise ValueError("quarantined source has no durable delivery intent")
        mismatch_evidence = _local_idempotency_mismatch_evidence(intent)
        if not mismatch_evidence:
            raise ValueError("quarantined source is not an exact idempotency mismatch")
        selected = intent.get("selected")
        post = intent.get("post")
        if not isinstance(selected, dict) or not isinstance(post, dict):
            raise ValueError("quarantined source has no frozen selected post")
        current_keys = {
            str(selected.get("message_idempotency_key") or legacy_key),
            str(post.get("idempotency_key") or legacy_key),
        }
        if current_keys != {legacy_key}:
            raise ValueError("intent is not an exact legacy message-key collision")
        selected["message_idempotency_key"] = v2_key
        post["idempotency_key"] = v2_key
        # Connector 1.0.20 persisted the exact public error message but did
        # not yet persist its structured API code. Normalize only that one
        # historical representation after every other local and canonical
        # collision proof has succeeded. Similar text and conflicting codes
        # remain fail-closed.
        intent["last_error_code"] = "idempotency_mismatch"
        intent["collision_recovery"] = {
            "version": 1,
            "sourceSeq": source_seq,
            "sourceEventId": args.source_event_id,
            "legacyKey": legacy_key,
            "v2Key": v2_key,
            "collidingEventId": args.colliding_event_id,
            "collidingActorMembershipId": args.colliding_actor_membership_id,
            "localMismatchEvidence": mismatch_evidence,
            "reason": "verified_foreign_actor_legacy_key_collision",
            "recordedAt": datetime.now(timezone.utc).isoformat(),
        }
        intent["state"] = "recovery-ready"
        latest.inbox[str(source_seq)] = "failed-retryable"
        latest.pending_since.pop(str(source_seq), None)
        latest.pending_retries[str(source_seq)] = 0

    update(recover)
    print(
        f"{args.room_id} sequence {source_seq} is recovery-ready under an actor-scoped v2 key; "
        "cursor and acknowledgement were not changed. Restart the connector for canonical replay."
    )
    return 0


def _local_idempotency_mismatch_evidence(intent: dict[str, object]) -> str:
    """Recognize only structured or one exact historical mismatch proof."""
    code = str(intent.get("last_error_code") or "")
    if code:
        return "structured_code" if code == "idempotency_mismatch" else ""
    if intent.get("last_error") == LEGACY_IDEMPOTENCY_MISMATCH_ERROR:
        return "legacy_1.0.20_exact_text"
    return ""


def _recover_orphaned_intent(args: argparse.Namespace) -> int:
    """Close one exact selected post after a canonical zero-byte cycle pass.

    This is deliberately not a generic intent deletion command. The local
    receive ledger must already be past the exact source without retaining any
    pending/terminal entry, the frozen post must never have reached the network
    boundary, and the canonical server transcript must prove that this actor's
    exact source cycle ended as an all-agent pass with zero accepted bytes and
    no message from this membership causally linked to the source.
    """
    if not args.yes:
        raise ValueError("--yes is required for an audited orphan-intent recovery")
    source_seq = int(args.source_seq)
    if source_seq < 1:
        raise ValueError("source_seq must be positive")
    expected_body_sha = str(args.body_sha256 or "").lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_body_sha):
        raise ValueError("body_sha256 must be exactly 64 lowercase hexadecimal characters")
    binding = load().binding(args.room_id)
    if binding is None:
        raise ValueError(f"room {args.room_id!r} is not configured")

    page = RoomProtocol(binding.base_url, binding.credential).events(
        # Keep this operator proof on the active-epoch read lane. The
        # all-epochs delivery lane advances the server delivered high-water,
        # although the connector runtime has not consumed those later events.
        binding.room_id, source_seq - 1, 0, all_epochs=False,
    )
    canonical = {str(event.get("id") or ""): event for event in page.get("events") or []}
    source = canonical.get(args.source_event_id) or {}
    started = canonical.get(args.cycle_started_event_id) or {}
    terminal = canonical.get(args.cycle_terminal_event_id) or {}
    if int(source.get("seq") or 0) != source_seq or str(source.get("type") or "") != "message.posted":
        raise ValueError("canonical source is not the exact message.posted sequence")
    if str(source.get("actorId") or "") == binding.membership_id:
        raise ValueError("orphan recovery source cannot be this membership's own message")
    started_payload = started.get("payload") or {}
    started_refs = {str(value) for value in (started.get("refs") or [])}
    if (
        str(started.get("type") or "") != "discussion.cycle_started"
        or str(started.get("actorId") or "") != binding.membership_id
        or str(started_payload.get("sourceEventId") or "") != args.source_event_id
        or args.source_event_id not in started_refs
    ):
        raise ValueError("canonical cycle start does not prove this membership and source")
    cycle_id = str(started_payload.get("cycleId") or "")
    terminal_payload = terminal.get("payload") or {}
    terminal_refs = {str(value) for value in (terminal.get("refs") or [])}
    if (
        not cycle_id
        or str(terminal.get("type") or "") != "discussion.cycle_terminal"
        or str(terminal_payload.get("cycleId") or "") != cycle_id
        or str(terminal_payload.get("state") or "") != "completed"
        or str(terminal_payload.get("reason") or "") != "all_available_agents_passed"
        or int(terminal_payload.get("acceptedBytes") or 0) != 0
        or int(terminal_payload.get("acceptedTurns") or 0) < 1
        or args.source_event_id not in terminal_refs
    ):
        raise ValueError("canonical cycle terminal is not an exact zero-byte all-agent pass")
    for event in canonical.values():
        if str(event.get("type") or "") != "message.posted" or str(event.get("actorId") or "") != binding.membership_id:
            continue
        payload = event.get("payload") or {}
        causal = {str(value) for value in (event.get("refs") or [])}
        responds = payload.get("respondsTo") or payload.get("responds_to") or []
        if isinstance(responds, list):
            causal.update(str(value) for value in responds)
        if args.source_event_id in causal or str(payload.get("cycleId") or "") == cycle_id:
            raise ValueError("a canonical message already exists for this source or cycle")

    audit = {
        "version": 1,
        "sourceSeq": source_seq,
        "sourceEventId": args.source_event_id,
        "cycleStartedEventId": args.cycle_started_event_id,
        "cycleTerminalEventId": args.cycle_terminal_event_id,
        "cycleId": cycle_id,
        "bodySha256": expected_body_sha,
        "reason": "canonical_zero_byte_cycle_pass_after_source_ack",
    }

    def recover(current):
        latest = current.binding(args.room_id)
        if latest is None or latest.membership_id != binding.membership_id:
            raise ValueError("Room membership changed during orphan-intent recovery")
        existing_audit = latest.abandoned_delivery_intents.get(args.source_event_id)
        if existing_audit:
            comparable = {key: existing_audit.get(key) for key in audit}
            if comparable != audit:
                raise ValueError("orphan-intent recovery audit conflicts with this request")
            return
        if latest.cursor < source_seq or latest.acknowledged_cursor < source_seq:
            raise ValueError("source sequence has not been canonically acknowledged")
        seq_key = str(source_seq)
        if (
            seq_key in latest.inbox or seq_key in latest.pending_since
            or seq_key in latest.pending_retries or seq_key in latest.terminal_evidence
            or args.source_event_id in latest.turn_sequences
            or args.source_event_id in latest.turn_observed
        ):
            raise ValueError("source still has live receive-ledger or turn evidence")
        intent = latest.delivery_intents.get(args.source_event_id)
        if not isinstance(intent, dict):
            raise ValueError("source has no exact durable delivery intent")
        selected = intent.get("selected")
        if not isinstance(selected, dict) or selected.get("action") != "post":
            raise ValueError("source has no selected post intent")
        if intent.get("post"):
            raise ValueError("intent crossed the durable network-post boundary")
        if (
            str(selected.get("source_event_id") or "") != args.source_event_id
            or int(selected.get("source_seq") or 0) != source_seq
        ):
            raise ValueError("selected intent source provenance does not match")
        generation = selected.get("binding") or {}
        if generation and generation != {
            "membership_id": latest.membership_id,
            "installation_id": latest.installation_id,
            "identity_version": latest.identity_version,
        }:
            raise ValueError("selected intent belongs to a different binding generation")
        body = str(selected.get("body") or "")
        if hashlib.sha256(body.encode("utf-8")).hexdigest() != expected_body_sha:
            raise ValueError("selected intent body fingerprint does not match")
        receipt = dict(audit)
        receipt["recordedAt"] = datetime.now(timezone.utc).isoformat()
        latest.abandoned_delivery_intents[args.source_event_id] = receipt
        latest.delivery_intents.pop(args.source_event_id, None)

    update(recover)
    print(
        f"{args.room_id} source sequence {source_seq} is closed as a canonical zero-byte cycle pass; "
        "the frozen post cannot replay and cursor/ack were not changed."
    )
    return 0


def _recover_stale_context_idempotency(args: argparse.Namespace) -> int:
    """Close one overtaken intent whose contribution already committed.

    The command recognizes only the 1.0.51 stale-context signature: the
    connector changed ``observed_seq`` while retaining one idempotency key,
    the server rejected the later request as an idempotency mismatch, and the
    same membership already has one canonical contribution for the exact
    cycle.  No message is replayed and cursor/acknowledgement are unchanged.
    """
    if not args.yes:
        raise ValueError("--yes is required for an audited stale-context recovery")
    source_seq = int(args.source_seq)
    if source_seq < 1:
        raise ValueError("source_seq must be positive")
    local_body_sha = str(args.local_body_sha256 or "").lower()
    canonical_body_sha = str(args.canonical_body_sha256 or "").lower()
    for label, digest in (
        ("local_body_sha256", local_body_sha),
        ("canonical_body_sha256", canonical_body_sha),
    ):
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError(f"{label} must be exactly 64 lowercase hexadecimal characters")

    binding = load().binding(args.room_id)
    if binding is None:
        raise ValueError(f"room {args.room_id!r} is not configured")
    page = RoomProtocol(binding.base_url, binding.credential).events(
        binding.room_id, source_seq - 1, 0, all_epochs=False,
    )
    canonical = {str(event.get("id") or ""): event for event in page.get("events") or []}
    source = canonical.get(args.source_event_id) or {}
    result = canonical.get(args.canonical_event_id) or {}
    source_payload = source.get("payload") or {}
    result_payload = result.get("payload") or {}
    if (
        int(source.get("seq") or 0) != source_seq
        or str(source.get("type") or "") != "discussion.cycle_attempt_ready"
        or str(source_payload.get("cycleId") or "") != args.cycle_id
        or str(source_payload.get("membershipId") or "") != binding.membership_id
    ):
        raise ValueError("canonical source is not this membership's exact cycle attempt")
    canonical_body = str(result_payload.get("body") or "")
    if (
        str(result.get("type") or "") != "message.posted"
        or str(result.get("actorId") or "") != binding.membership_id
        or str(result_payload.get("cycleId") or "") != args.cycle_id
        or str(result_payload.get("attemptId") or "") != args.attempt_id
        or hashlib.sha256(canonical_body.encode("utf-8")).hexdigest() != canonical_body_sha
    ):
        raise ValueError("canonical result is not the exact contributed cycle message")

    cycle = RoomProtocol(binding.base_url, binding.credential).get_discussion_cycle(
        binding.room_id, args.cycle_id,
    )
    matching = [
        item for item in (cycle.get("contributions") or [])
        if isinstance(item, dict)
        and item.get("membershipId") == binding.membership_id
        and item.get("eventId") == args.canonical_event_id
    ]
    if (
        str(cycle.get("id") or cycle.get("cycleId") or "") != args.cycle_id
        or str(cycle.get("state") or "") not in {"completed", "interrupted", "timed_out"}
        or len(matching) != 1
    ):
        raise ValueError("authoritative cycle does not prove one terminal canonical contribution")

    audit_base = {
        "version": 1,
        "reason": "stale_context_observed_seq_changed_under_same_idempotency_key",
        "sourceSeq": source_seq,
        "sourceEventId": args.source_event_id,
        "canonicalEventId": args.canonical_event_id,
        "cycleId": args.cycle_id,
        "attemptId": args.attempt_id,
        "localBodySha256": local_body_sha,
        "canonicalBodySha256": canonical_body_sha,
        "cycleState": str(cycle.get("state") or ""),
    }

    def recover(current):
        latest = current.binding(args.room_id)
        if latest is None or latest.membership_id != binding.membership_id:
            raise ValueError("Room membership changed during stale-context recovery")
        existing_audit = latest.abandoned_delivery_intents.get(args.source_event_id)
        if existing_audit:
            comparable = {key: existing_audit.get(key) for key in audit_base}
            if comparable != audit_base:
                raise ValueError("stale-context recovery audit conflicts with this request")
            return
        if latest.cursor < source_seq or latest.acknowledged_cursor < source_seq:
            raise ValueError("source sequence has not been canonically acknowledged")
        seq_key = str(source_seq)
        if any(
            seq_key in ledger for ledger in (
                latest.inbox, latest.pending_since, latest.pending_retries,
                latest.terminal_evidence,
            )
        ):
            raise ValueError("source still has live receive-ledger evidence")
        if args.source_event_id in latest.delivery_lifecycle:
            raise ValueError("source unexpectedly has a delivery lifecycle journal")
        intent = latest.delivery_intents.get(args.source_event_id)
        if not isinstance(intent, dict):
            raise ValueError("source has no exact durable delivery intent")
        selected = intent.get("selected")
        post = intent.get("post")
        if not isinstance(selected, dict) or not isinstance(post, dict):
            raise ValueError("source lacks selected and posted request evidence")
        expected_binding = {
            "membership_id": latest.membership_id,
            "installation_id": latest.installation_id,
            "identity_version": latest.identity_version,
        }
        selected_observed = int(selected.get("observed_seq") or 0)
        post_observed = int(post.get("observed_seq") or 0)
        local_body = str(selected.get("body") or "")
        authority_keys = sorted(
            key for key, record in latest.delivery_authority.items()
            if isinstance(record, dict)
            and record.get("source_event_id") == args.source_event_id
            and record.get("room_id") == args.room_id
            and record.get("membership_id") == latest.membership_id
            and record.get("cycle_id") == args.cycle_id
            and record.get("attempt_id") == args.attempt_id
        )
        embedded_cycle = post.get("cycle") or {}
        embedded_cycle_matches = (
            isinstance(embedded_cycle, dict)
            and str(embedded_cycle.get("cycle_id") or "") == args.cycle_id
            and str(embedded_cycle.get("attempt_id") or "") == args.attempt_id
        )
        # Connector 1.0.51 persisted the exact attempt in delivery_authority,
        # but its post snapshot could contain an empty cycle object. Accept
        # that historical shape only when one exact authority record supplies
        # the missing binding; never infer it merely from the operator args.
        cycle_binding_proven = embedded_cycle_matches or len(authority_keys) == 1
        if (
            selected.get("action") != "post"
            or str(selected.get("source_event_id") or "") != args.source_event_id
            or int(selected.get("source_seq") or 0) != source_seq
            or selected.get("binding") != expected_binding
            or post.get("binding") != expected_binding
            or str(post.get("body") or "") != local_body
            or hashlib.sha256(local_body.encode("utf-8")).hexdigest() != local_body_sha
            or selected_observed < 1
            or post_observed <= selected_observed
            or str(selected.get("message_idempotency_key") or "")
               != str(post.get("idempotency_key") or "")
            or not cycle_binding_proven
            or intent.get("delivery_state") != "quarantined"
            or intent.get("state") != "quarantined"
            or intent.get("last_error_code") != "idempotency_mismatch"
            or (intent.get("canonical_event") or {}).get("id")
        ):
            raise ValueError("local intent is not the exact stale-context idempotency signature")
        receipt = dict(audit_base)
        receipt.update({
            "selectedObservedSeq": selected_observed,
            "postObservedSeq": post_observed,
            "authorityKeys": authority_keys,
            "recordedAt": datetime.now(timezone.utc).isoformat(),
        })
        latest.abandoned_delivery_intents[args.source_event_id] = receipt
        latest.delivery_intents.pop(args.source_event_id, None)
        latest.turn_sequences.pop(args.source_event_id, None)
        latest.turn_observed.pop(args.source_event_id, None)
        for key in authority_keys:
            latest.delivery_authority.pop(key, None)

    update(recover)
    print(
        f"{args.room_id} source sequence {source_seq} closed after its canonical cycle contribution; "
        "the quarantined request cannot replay and cursor/ack were not changed."
    )
    return 0


def _reconcile_terminal_lifecycle(args: argparse.Namespace) -> int:
    """Archive one blocked lifecycle only after authoritative terminal proof."""
    if not args.yes:
        raise ValueError("--yes is required for an audited terminal lifecycle reconciliation")
    source_seq = int(args.source_seq)
    if source_seq < 1:
        raise ValueError("source_seq must be positive")
    terminal_state = str(args.terminal_state)
    if terminal_state not in {"completed", "interrupted"}:
        raise ValueError("terminal_state must be completed or interrupted")
    binding = load().binding(args.room_id)
    if binding is None:
        raise ValueError(f"room {args.room_id!r} is not configured")

    expected_identity = {
        "source_event_id": args.source_event_id,
        "source_seq": source_seq,
        "canonical_event_id": args.canonical_event_id,
        "cycle_id": args.cycle_id,
        "terminal_state": terminal_state,
    }
    existing = binding.resolved_delivery_lifecycle.get(args.source_event_id)
    if existing:
        if any(existing.get(key) != value for key, value in expected_identity.items()):
            raise ValueError("resolved lifecycle audit conflicts with this request")
        print(f"{args.room_id} source sequence {source_seq} lifecycle was already reconciled.")
        return 0

    journal = binding.delivery_lifecycle.get(args.source_event_id)
    if not isinstance(journal, dict):
        raise TypeError("source has no blocked lifecycle journal")
    journal = deepcopy(journal)
    receipt = journal.get("receipt") or {}
    completion = journal.get("completion") or {}
    payload = completion.get("payload") or {}
    if (
        journal.get("state") != "lifecycle_blocked"
        or journal.get("delivery_state") != "posted"
        or journal.get("lifecycle_state") != "blocked"
        or journal.get("automatic_retry") is not False
        or not isinstance(journal.get("last_error_code"), str)
        or not isinstance(journal.get("last_error"), str)
        or journal.get("last_error_code") != "cycle_conflict"
        or receipt.get("source_event_id") != args.source_event_id
        or receipt.get("source_seq") != source_seq
        or receipt.get("canonical_event_id") != args.canonical_event_id
        or completion.get("kind") != "cycle"
        or completion.get("cycle_id") != args.cycle_id
        or payload.get("action") != "contribute"
        or payload.get("eventId") != args.canonical_event_id
        or binding.cursor < source_seq
        or binding.acknowledged_cursor < source_seq
        or args.source_event_id in binding.delivery_intents
    ):
        raise ValueError("blocked lifecycle does not match the exact reconciliation contract")

    cycle = RoomProtocol(binding.base_url, binding.credential).get_discussion_cycle(
        binding.room_id, args.cycle_id,
    )
    contributions = cycle.get("contributions")
    matching_contribution = any(
        isinstance(item, dict)
        and item.get("eventId") == args.canonical_event_id
        and item.get("membershipId") == binding.membership_id
        for item in contributions
    ) if isinstance(contributions, list) else False
    if (
        str(cycle.get("id") or cycle.get("cycleId") or "") != args.cycle_id
        or str(cycle.get("state") or "") != terminal_state
        or not matching_contribution
    ):
        raise ValueError("authoritative cycle does not match the requested terminal proof")

    audit = {
        "version": 1,
        "reason": "authoritative_terminal_cycle_after_canonical_delivery",
        **expected_identity,
        "receipt": deepcopy(receipt),
        "original_completion": deepcopy(completion),
        "binding": deepcopy(journal.get("binding")),
        "original_error_code": journal["last_error_code"],
        "original_error": journal["last_error"],
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    binding_identity = {
        "room_id": binding.room_id,
        "base_url": binding.base_url,
        "credential": binding.credential,
        "membership_id": binding.membership_id,
        "installation_id": binding.installation_id,
        "identity_version": binding.identity_version,
    }

    def reconcile(current):
        latest = current.binding(args.room_id)
        latest_identity = {
            "room_id": getattr(latest, "room_id", ""),
            "base_url": getattr(latest, "base_url", ""),
            "credential": getattr(latest, "credential", ""),
            "membership_id": getattr(latest, "membership_id", ""),
            "installation_id": getattr(latest, "installation_id", ""),
            "identity_version": getattr(latest, "identity_version", -1),
        }
        if latest is None or latest_identity != binding_identity:
            raise ValueError("Room binding changed during lifecycle reconciliation")
        if latest.cursor < source_seq or latest.acknowledged_cursor < source_seq:
            raise ValueError("Room acknowledgement proof changed during lifecycle reconciliation")
        if latest.delivery_lifecycle.get(args.source_event_id) != journal:
            raise ValueError("blocked lifecycle changed during reconciliation")
        if args.source_event_id in latest.delivery_intents:
            raise ValueError("source regained a live delivery intent")
        concurrent_audit = latest.resolved_delivery_lifecycle.get(args.source_event_id)
        if concurrent_audit is not None:
            if concurrent_audit == audit and args.source_event_id not in latest.delivery_lifecycle:
                return
            raise ValueError("resolved lifecycle audit changed during reconciliation")
        latest.resolved_delivery_lifecycle[args.source_event_id] = deepcopy(audit)
        latest.delivery_lifecycle.pop(args.source_event_id, None)

    update(reconcile)
    print(
        f"{args.room_id} source sequence {source_seq} lifecycle closed as {terminal_state}; "
        "canonical receipt preserved and cursor/ack unchanged."
    )
    return 0


def _renew(room_id: str, *, request_owner: bool = False) -> int:
    """Crash-safely rotate one room-bound credential without rejoining.

    The owner creates a grant for the exact existing membership. The grant ID
    and secret are entered only through hidden prompts on the first run; the
    replacement is generated locally. Before the first network request all
    three recovery secrets and the original binding invariants are persisted
    in the profile-private 0600 state file. Every later run resumes that exact
    journal and is therefore safe across ambiguous HTTP responses or crashes.
    """
    initial = load().binding(room_id)
    if initial is None:
        raise ValueError(f"room {room_id!r} is not configured")
    confirmed_at_entry = str(initial.credential_rotation.get("phase") or "") == "confirmed"
    false_revoked_recovery = bool(initial.credential_rotation.get("recover_false_revoked"))
    if initial.revoked and not (false_revoked_recovery or (request_owner and not initial.credential_rotation)):
        raise ValueError("a revoked membership cannot renew its credential")
    if not initial.credential_rotation:
        grant_id = ""
        request_id = ""
        if request_owner:
            request_id = _new_renewal_secret()
            grant_secret = _new_renewal_secret()
        else:
            grant_id = getpass.getpass("Renewal grant ID (hidden): ").strip()
            grant_secret = getpass.getpass("Renewal grant secret (hidden): ").strip()
            if not grant_id:
                raise ValueError("renewal grant ID is required")
            if not _canonical_renewal_secret(grant_secret):
                raise ValueError("renewal grant secret must encode exactly 32 random bytes")
        replacement = _new_renewal_secret()

        def prepare(current):
            binding = current.binding(room_id)
            if binding is None:
                raise ValueError(f"room {room_id!r} is not configured")
            if binding.credential_rotation:
                return
            binding.credential_rotation = {
                "phase": "requesting_owner" if request_owner else "prepared",
                "grant_id": grant_id,
                "grant_secret": grant_secret,
                "replacement_credential": replacement,
                "base_credential": binding.credential,
                "base_credential_expires_at": binding.credential_expires_at,
                "expected": _renewal_identity_snapshot(binding),
                "prepared_at": datetime.now(timezone.utc).isoformat(),
            }
            if binding.revoked:
                binding.credential_rotation["recover_false_revoked"] = True
            if request_id:
                binding.credential_rotation["request_id"] = request_id

        update(prepare)
        del grant_secret, replacement

    binding = load().binding(room_id)
    if binding is None:
        raise ValueError(f"room {room_id!r} is not configured")
    journal = deepcopy(binding.credential_rotation)
    phase = str(journal.get("phase") or "")
    if phase not in {"requesting_owner", "prepared", "redeemed", "swapped", "confirmed"}:
        raise ValueError("credential-renewal journal has an unsupported phase")
    required = ["grant_secret", "replacement_credential", "base_credential", "expected"]
    required.append("request_id" if phase == "requesting_owner" else "grant_id")
    for field in required:
        if not journal.get(field):
            raise ValueError("credential-renewal journal is incomplete")
    if not _canonical_renewal_secret(str(journal["grant_secret"])) or not _canonical_renewal_secret(str(journal["replacement_credential"])):
        raise ValueError("credential-renewal journal contains malformed secret material")

    protocol = RoomProtocol(binding.base_url, str(journal["base_credential"]))
    if phase in {"requesting_owner", "prepared"} and not journal.get("server_delivery_snapshot"):
        # The connector's local replay cursor and the server's delivered
        # high-water are distinct. An audited evidence read may legitimately
        # make delivered greater than the still-unprocessed local cursor. Bind
        # the renewal protocol to the server snapshot without changing the
        # local cursor or acknowledgement, so the gap is replayed normally
        # after the replacement credential becomes active.
        server_state = protocol.room_state(room_id)
        journal = _record_renewal_delivery_snapshot(room_id, journal, server_state)
    if phase == "requesting_owner":
        request_id = str(journal["request_id"])
        status = protocol.request_credential_renewal(
            room_id, request_id,
            hashlib.sha256(str(journal["grant_secret"]).encode("utf-8")).hexdigest(),
        )
        journal = _record_owner_identity_status(room_id, journal, status)
        state = str(status.get("state") or "")
        if state == "requested":
            print(
                f"{room_id} renewal approval requested for the existing membership; "
                "the room owner must approve it in Room settings. Rerun this command afterwards."
            )
            return 0
        if state == "expired":
            raise ValueError("credential-renewal owner request expired; retain the journal for operator review")
        if state == "rejected":
            raise ValueError("credential-renewal owner request was rejected; retain the journal for operator review")
        if state != "issued" or not status.get("grantId"):
            status = protocol.credential_renewal_request(room_id, request_id)
            journal = _record_owner_identity_status(room_id, journal, status)
            state = str(status.get("state") or "")
            if state == "requested":
                print(
                    f"{room_id} renewal is still awaiting owner approval; "
                    "rerun this command after approval."
                )
                return 0
            if state != "issued" or not status.get("grantId"):
                raise ValueError(f"credential-renewal owner request is not issuable (state={state or 'unknown'})")

        def mark_prepared(current):
            latest = _same_rotation(current, room_id, journal)
            latest.credential_rotation["phase"] = "prepared"
            latest.credential_rotation["grant_id"] = str(status["grantId"])
            latest.credential_rotation["approved_at"] = str(status.get("approvedAt") or "")
            latest.credential_rotation["grant_expires_at"] = str(status.get("expiresAt") or "")

        update(mark_prepared)
        binding = load().binding(room_id)
        journal = deepcopy(binding.credential_rotation)
        phase = "prepared"

    if phase == "prepared":
        request_id = str(journal.get("request_id") or "")
        if request_id:
            status = protocol.credential_renewal_request(room_id, request_id)
            journal = _record_owner_identity_status(room_id, journal, status)
            request_state = str(status.get("state") or "")
            if request_state in {"expired", "requested"}:
                def await_reapproval(current):
                    latest = _same_rotation(current, room_id, journal)
                    latest.credential_rotation["phase"] = "requesting_owner"
                    latest.credential_rotation.pop("grant_id", None)
                    latest.credential_rotation.pop("approved_at", None)
                    latest.credential_rotation.pop("grant_expires_at", None)

                update(await_reapproval)
                print(
                    f"{room_id} renewal approval expired before activation; "
                    "the exact journaled request will be presented to the room owner again. Rerun this command."
                )
                return 0
            if request_state == "rejected":
                raise ValueError("credential-renewal owner request was rejected; retain the journal for operator review")
            if request_state not in {"issued", "redeemed"}:
                raise ValueError(f"credential-renewal owner request cannot be redeemed (state={request_state or 'unknown'})")
        redemption = protocol.redeem_credential_renewal(
            room_id, str(journal["grant_id"]), str(journal["grant_secret"]),
            str(journal["replacement_credential"]),
        )
        _validate_renewal_identity(redemption, journal["expected"])

        def mark_redeemed(current):
            latest = _same_rotation(current, room_id, journal)
            latest.credential_rotation["phase"] = "redeemed"
            latest.credential_rotation["confirm_by"] = str(redemption.get("confirmBy") or "")
            latest.credential_rotation["pending_expires_at"] = str(redemption.get("credentialExpiresAt") or "")

        update(mark_redeemed)
        phase = "redeemed"

    if phase == "redeemed":
        binding = load().binding(room_id)
        journal = deepcopy(binding.credential_rotation)
        verification = RoomProtocol(binding.base_url).verify_credential_renewal(
            room_id, str(journal["grant_id"]), str(journal["replacement_credential"]),
        )
        _validate_renewal_identity(verification, journal["expected"])

        def swap(current):
            latest = _same_rotation(current, room_id, journal)
            before = _binding_without_rotation_fields(latest)
            latest.credential = str(journal["replacement_credential"])
            latest.credential_expires_at = str(verification.get("credentialExpiresAt") or "")
            latest.credential_rotation["phase"] = "swapped"
            after = _binding_without_rotation_fields(latest)
            allowed = {"credential", "credential_expires_at"}
            drift = {key for key in before if before[key] != after[key]}
            if not drift.issubset(allowed):
                raise ValueError(f"credential swap would alter protected binding fields: {sorted(drift)}")

        update(swap)
        phase = "swapped"

    if phase == "swapped":
        binding = load().binding(room_id)
        journal = deepcopy(binding.credential_rotation)
        if binding.credential != journal["replacement_credential"]:
            raise ValueError("local credential swap does not match the durable renewal journal")
        event = RoomProtocol(binding.base_url).confirm_credential_renewal(
            room_id, str(journal["grant_id"]), str(journal["replacement_credential"]),
        )
        if str(event.get("type") or "") != "credential.renewed":
            raise ValueError("renewal confirmation did not return credential.renewed")
        payload = event.get("payload") or {}
        if str(payload.get("membershipId") or "") != binding.membership_id:
            raise ValueError("renewal confirmation returned a different membership")
        final_expiry = str(payload.get("credentialExpiresAt") or "")
        if not final_expiry:
            raise ValueError("renewal confirmation omitted the final credential expiry")

        def mark_confirmed(current):
            latest = _same_rotation(current, room_id, journal)
            if latest.credential != journal["replacement_credential"]:
                raise ValueError("local credential swap changed before confirmation was recorded")
            latest.credential_expires_at = final_expiry
            latest.credential_rotation["phase"] = "confirmed"
            latest.credential_rotation["confirmation_event_id"] = str(event.get("id") or "")
            latest.credential_rotation["confirmed_at"] = datetime.now(timezone.utc).isoformat()

        update(mark_confirmed)
        phase = "confirmed"

    if phase == "confirmed":
        binding = load().binding(room_id)
        journal = deepcopy(binding.credential_rotation)
        # A lost Confirm response is retried above while phase=swapped. Once
        # recorded, prove the normal active-credential state route and exact
        # identity/cursors before declaring the local operation complete.
        state = RoomProtocol(binding.base_url, binding.credential).room_state(room_id)
        _validate_active_room_state(
            state, binding, journal["expected"], allow_monotonic=confirmed_at_entry,
        )

        def finish_renewal(current):
            latest = _same_rotation(current, room_id, journal)
            if latest.credential != journal["replacement_credential"]:
                raise ValueError("renewed credential changed before local recovery completed")
            before = _binding_without_rotation_fields(latest)
            allowed = set()
            reconciliation = journal.get("identity_reconciliation") or {}
            if reconciliation:
                latest.display_name = str(reconciliation["authoritative_display_name"])
                latest.identity_version = int(reconciliation["authoritative_identity_version"])
                allowed.update({"display_name", "identity_version"})
            if journal.get("recover_false_revoked"):
                latest.revoked = False
                latest.enabled = True
                allowed.update({"revoked", "enabled"})
            after = _binding_without_rotation_fields(latest)
            drift = {key for key in before if before[key] != after[key]}
            if not drift.issubset(allowed):
                raise ValueError(f"credential renewal would alter protected binding fields: {sorted(drift)}")
            # Confirm plus the exact authenticated state proof is the final
            # recovery boundary. Raw base/grant/replacement secrets are no
            # longer needed and must not remain in durable state. The
            # canonical credential.renewed event is the secret-free audit.
            latest.credential_rotation = {}

        update(finish_renewal)
        if confirmed_at_entry and request_owner:
            # An explicit new owner request after a previously completed
            # rotation may start immediately against the now-active base.
            return _renew(room_id, request_owner=True)
        print(
            f"{room_id} credential renewed for the existing membership; "
            "canonical identity and cursors are verified. Restart the Hermes gateway."
        )
        return 0
    raise ValueError("credential renewal did not reach a terminal local phase")


def _new_renewal_secret() -> str:
    import base64
    return base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode("ascii")


def _canonical_renewal_secret(value: str) -> bool:
    import base64
    try:
        raw = base64.urlsafe_b64decode(value + "=" * ((4 - len(value) % 4) % 4))
    except (ValueError, TypeError):
        return False
    return len(raw) == 32 and base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii") == value


def _renewal_identity_snapshot(binding: RoomBinding) -> dict[str, object]:
    return {
        "room_id": binding.room_id,
        "membership_id": binding.membership_id,
        "display_name": binding.display_name,
        "identity_version": binding.identity_version,
        "cursor": binding.cursor,
        "acknowledged_cursor": binding.acknowledged_cursor,
        "installation_id": binding.installation_id,
    }


def _owner_identity_reconciliation(binding: RoomBinding, journal: dict, response: dict) -> dict[str, object]:
    authoritative = {
        "request_id": str(response.get("requestId") or ""),
        "grant_id": str(response.get("grantId") or ""),
        "room_id": str(response.get("roomId") or ""),
        "membership_id": str(response.get("membershipId") or ""),
        "base_credential_generation": int(response.get("baseCredentialGeneration") or 0),
        "authoritative_display_name": str(response.get("displayName") or ""),
        "authoritative_identity_version": int(response.get("identityVersion") or 0),
    }
    if authoritative["request_id"] != str(journal.get("request_id") or ""):
        raise ValueError("credential-renewal owner status changed request")
    if authoritative["room_id"] != binding.room_id or authoritative["membership_id"] != binding.membership_id:
        raise ValueError("credential-renewal owner status changed room or membership")
    if not authoritative["grant_id"] or authoritative["base_credential_generation"] < 1:
        raise ValueError("credential-renewal owner status omitted grant or base generation")
    if not authoritative["authoritative_display_name"] or authoritative["authoritative_identity_version"] < 1:
        raise ValueError("credential-renewal owner status omitted canonical identity")
    return {
        **authoritative,
        "local_display_name": binding.display_name,
        "local_identity_version": binding.identity_version,
    }


def _record_owner_identity_status(room_id: str, journal: dict, response: dict) -> dict:
    def persist(latest_state):
        latest = latest_state.binding(room_id)
        if latest is None:
            raise ValueError(f"room {room_id!r} is not configured")
        for field in ("request_id", "grant_secret", "replacement_credential", "base_credential"):
            if latest.credential_rotation.get(field) != journal.get(field):
                raise ValueError("credential-renewal journal changed concurrently")
        frozen = _owner_identity_reconciliation(latest, journal, response)
        existing = latest.credential_rotation.get("identity_reconciliation")
        if existing and existing != frozen:
            raise ValueError("credential-renewal canonical identity snapshot changed")
        latest.credential_rotation["identity_reconciliation"] = frozen
        latest.credential_rotation["grant_id"] = frozen["grant_id"]
        latest.credential_rotation["base_credential_generation"] = frozen["base_credential_generation"]
        latest.credential_rotation["expected"]["display_name"] = frozen["authoritative_display_name"]
        latest.credential_rotation["expected"]["identity_version"] = frozen["authoritative_identity_version"]

    update(persist)
    refreshed = load().binding(room_id)
    if refreshed is None:
        raise ValueError(f"room {room_id!r} is not configured")
    return deepcopy(refreshed.credential_rotation)


def _record_renewal_delivery_snapshot(room_id: str, journal: dict, response: dict) -> dict:
    def persist(latest_state):
        latest = _same_rotation(latest_state, room_id, journal)
        if str(response.get("roomId") or "") != latest.room_id:
            raise ValueError("credential-renewal state preflight returned a different room")
        roster = response.get("roster") or []
        member = next(
            (item for item in roster if str(item.get("membershipId") or "") == latest.membership_id),
            None,
        )
        if member is None:
            raise ValueError("credential-renewal state preflight omitted the membership")
        expected = latest.credential_rotation.get("expected") or {}
        if not latest.credential_rotation.get("recover_false_revoked"):
            expected_name = str(expected.get("display_name") or "")
            if expected_name and str(member.get("displayName") or "") != expected_name:
                raise ValueError("credential-renewal state preflight changed display name")
            expected_version = int(expected.get("identity_version") or 0)
            if expected_version and int(member.get("identityVersion") or 0) != expected_version:
                raise ValueError("credential-renewal state preflight changed identity version")
        delivered = int(member.get("deliveredSeq") or 0)
        acknowledged = int(member.get("acknowledgedSeq") or 0)
        if delivered < latest.cursor or acknowledged != latest.acknowledged_cursor or delivered < acknowledged:
            raise ValueError("credential-renewal state preflight conflicts with local replay cursors")
        frozen = {"delivered_seq": delivered, "acknowledged_seq": acknowledged}
        existing = latest.credential_rotation.get("server_delivery_snapshot")
        if existing and existing != frozen:
            raise ValueError("credential-renewal server delivery snapshot changed")
        latest.credential_rotation["server_delivery_snapshot"] = frozen
        latest.credential_rotation["expected"]["cursor"] = delivered
        latest.credential_rotation["expected"]["acknowledged_cursor"] = acknowledged

    update(persist)
    refreshed = load().binding(room_id)
    if refreshed is None:
        raise ValueError(f"room {room_id!r} is not configured")
    return deepcopy(refreshed.credential_rotation)


def _validate_renewal_identity(response: dict, expected: dict) -> None:
    comparisons = {
        "room_id": str(response.get("roomId") or ""),
        "membership_id": str(response.get("membershipId") or ""),
        "display_name": str(response.get("displayName") or ""),
        "identity_version": int(response.get("identityVersion") or 0),
        "cursor": int(response.get("deliveredSeq") or 0),
        "acknowledged_cursor": int(response.get("acknowledgedSeq") or 0),
    }
    for field, actual in comparisons.items():
        wanted = expected.get(field)
        # Legacy local bindings may not have cached descriptive identity, but
        # room/membership and both cursor invariants are always exact.
        if field in {"display_name", "identity_version"} and not wanted:
            continue
        if actual != wanted:
            raise ValueError(f"renewal verification changed {field}")


def _validate_active_room_state(
    response: dict, binding: RoomBinding, expected: dict, *, allow_monotonic: bool = False,
) -> None:
    if str(response.get("roomId") or "") != binding.room_id:
        raise ValueError("active state verification returned a different room")
    roster = response.get("roster") or []
    member = next((item for item in roster if str(item.get("membershipId") or "") == binding.membership_id), None)
    if member is None:
        raise ValueError("active state verification omitted the renewed membership")
    expected_name = str(expected.get("display_name") or "")
    if expected_name and member.get("displayName") is not None and str(member.get("displayName")) != expected_name:
        raise ValueError("active state verification changed display name")
    expected_version = int(expected.get("identity_version") or 0)
    if expected_version and int(member.get("identityVersion") or 0) != expected_version:
        raise ValueError("active state verification changed identity version")
    delivered = int(member.get("deliveredSeq") or 0)
    acknowledged = int(member.get("acknowledgedSeq") or 0)
    if allow_monotonic:
        if (
            delivered < int(binding.cursor)
            or acknowledged != int(binding.acknowledged_cursor)
            or delivered < int(expected["cursor"])
            or acknowledged < int(expected["acknowledged_cursor"])
        ):
            raise ValueError("active state verification does not match monotonic local cursors")
    elif delivered != int(expected["cursor"]) or acknowledged != int(expected["acknowledged_cursor"]):
        raise ValueError("active state verification changed delivery or acknowledgement cursor")


def _same_rotation(current, room_id: str, journal: dict) -> RoomBinding:
    latest = current.binding(room_id)
    if latest is None:
        raise ValueError(f"room {room_id!r} is not configured")
    for field in ("grant_id", "grant_secret", "replacement_credential", "base_credential"):
        if latest.credential_rotation.get(field) != journal.get(field):
            raise ValueError("credential-renewal journal changed concurrently")
    return latest


def _binding_without_rotation_fields(binding: RoomBinding) -> dict[str, object]:
    from dataclasses import asdict
    result = asdict(binding)
    result.pop("credential_rotation", None)
    return result


def _api_url(raw: str) -> str:
    parsed = urlsplit(raw.strip())
    if parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}):
        raise ValueError("--base-url must be HTTPS (or loopback HTTP)")
    path = parsed.path.rstrip("/")
    if not path.endswith("/api"):
        path += "/api"
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def _expiry_status(raw: str) -> str:
    if not raw:
        return ""
    try:
        expires = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        remaining = (expires - datetime.now(timezone.utc)).total_seconds()
    except ValueError:
        return "  credential expiry unknown"
    if remaining <= 0:
        return "  credential expired; owner-authorized renewal required"
    if remaining <= 7 * 86400:
        return f"  credential expires in {max(1, int(remaining // 3600))}h; renew this membership"
    return f"  credential expires {expires.date().isoformat()}"

"""`hermes room` onboarding and lifecycle commands."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import platform
import re
import sys
from datetime import datetime, timezone
from urllib.parse import urlsplit

from .protocol import ProtocolError, RoomProtocol, parse_invitation_url
from .state import RoomBinding, load, remove, save, upsert


def setup_cli(parser: argparse.ArgumentParser) -> None:
    commands = parser.add_subparsers(dest="room_command")
    join = commands.add_parser("join", help="Review and accept a Room invitation")
    join.add_argument("--base-url", help="Room origin or API URL (required when entering a short code)")
    join.add_argument("--name", help="Agent display name; defaults to the invitation proposal or Hermes profile")
    join.add_argument("--yes", action="store_true", help="Accept after displaying the review (non-interactive)")
    commands.add_parser("status", help="Show Room memberships and connector state")
    leave = commands.add_parser("leave", help="Disable and remove one local Room membership")
    leave.add_argument("room_id")
    toggle = commands.add_parser("enable", help="Enable one local Room membership")
    toggle.add_argument("room_id")
    toggle = commands.add_parser("disable", help="Disable one local Room membership")
    toggle.add_argument("room_id")


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
        proposed = args.name or review.get("proposedAgentName") or os.environ.get("HERMES_PROFILE", "Hermes Agent")
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
    current = load()
    upsert(current, binding)
    save(current)
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
        print(f"{binding.room_id}  {binding.display_name or binding.membership_id}  {state}  {binding.transport}{expiry}")
    return 0


def _leave(room_id: str) -> int:
    current = load()
    binding = current.binding(room_id)
    if binding and binding.connector_session_id and not binding.revoked:
        try:
            RoomProtocol(binding.base_url, binding.credential).disconnect(room_id, binding.connector_session_id, False)
        except ProtocolError:
            pass
    if not remove(current, room_id):
        raise ValueError(f"room {room_id!r} is not configured")
    save(current)
    print(f"Removed local membership state for {room_id}. The Room owner controls server-side revocation.")
    return 0


def _toggle(room_id: str, enabled: bool) -> int:
    current = load()
    binding = current.binding(room_id)
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
    save(current)
    if enabled:
        print(f"{room_id} enabled; restart the Hermes gateway to activate it.")
    else:
        print(f"{room_id} disabled; the running connector will stop without reconnecting.")
    return 0


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
        return "  credential expired; rejoin required"
    if remaining <= 7 * 86400:
        return f"  credential expires in {max(1, int(remaining // 3600))}h; renew or rejoin"
    return f"  credential expires {expires.date().isoformat()}"

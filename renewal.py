"""Model-independent, installation-bound maintenance for owner renewals.

The adapter drains one binding before calling this engine. The same journaled
CLI engine and per-binding file lock cover crash recovery and concurrent CLI
invocations. No process environment, active profile, model or sibling changes.
"""
import time
from datetime import datetime
from pathlib import Path

from .protocol import RoomProtocol
from .recovery import TERMINAL_EVENT_STATES, is_terminal_intent
from .state import RoomBinding, load, state_path_scope


class RenewalClosed(ValueError):
    """Contract P2c-3i (ACK-AHEAD-07): the journal was closed without a credential.

    The owner request expired before it was issued; nothing was swapped. The
    binding records the reason and a new owner Renew is needed.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(f"owner renewal journal closed: {reason}; a new owner Renew is needed")
        self.reason = reason


def eligible(binding: RoomBinding) -> bool:
    return bool(
        binding.installation_id and not binding.revoked
        and (binding.enabled or binding.disabled_reason == "credential_expired")
        and (not binding.credential_rotation or binding.credential_rotation.get("owner_initiated"))
    )


def _passed(raw, now: float | None) -> bool:
    try:
        expires = datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return False
    return expires <= (time.time() if now is None else now)


def base_expired(binding: RoomBinding, now: float | None = None) -> bool:
    """Contract P2c-3j: the base credential has expired: its stored expiry passed, or
    the Room refused it (disabled credential_expired), or the renewal journal in
    progress began on an expired base (after the swap the binding carries the
    replacement's expiry)."""
    rotation = binding.credential_rotation or {}
    return (binding.disabled_reason == "credential_expired" or _passed(binding.credential_expires_at, now)
            or bool(rotation.get("resume_after_expiry"))
            or (bool(rotation) and _passed(rotation.get("base_credential_expires_at"), now)))


def quiescent(binding: RoomBinding, *, for_renewal: bool = False) -> bool:
    # Contract P2c-3j: for a renewal, an expired base never waits for work that
    # needs a valid credential to finish; the work is kept unchanged and resumes
    # after the renewal. Without for_renewal (the owner-control fence) and with a
    # valid base the rule below is unchanged.
    if for_renewal and base_expired(binding):
        return True
    # Quarantine is inert evidence, not an in-flight credential user. Keep it
    # byte-for-byte; renewal must neither replay it nor acknowledge its gap.
    # Unfinished outbound/lifecycle work still has to drain under the adapter's
    # receive/dispatch fence before exchanging credentials. Retained terminal
    # intents (contract P3-2) are finished history, not unfinished work. The
    # inbox uses the one terminal-state set (contract P2c-3h), stuck included.
    return not (
        any(not is_terminal_intent(intent)
            and not (intent.get("state") == "quarantined"
                     and intent.get("delivery_state", "quarantined") == "quarantined"
                     and intent.get("lifecycle_state", "not_started") == "not_started")
            for intent in binding.delivery_intents.values())
        or binding.delivery_lifecycle
        or any(binding.inbox.get(key) != "quarantined" for key in binding.pending_since)
        or any(value not in TERMINAL_EVENT_STATES and value != "quarantined"
               for value in binding.inbox.values())
    )


def discover(binding: RoomBinding) -> dict | None:
    return RoomProtocol(binding.base_url, binding.credential).credential_renewal_intent(
        binding.room_id, binding.installation_id,
    )


def resume(path: Path, room_id: str, intent: dict | None) -> None:
    from .cli import _renew

    with state_path_scope(path):
        binding = load().binding(room_id)
        if binding is None or not eligible(binding) or not quiescent(binding, for_renewal=True):
            raise ValueError("renewal requires the exact eligible quiescent binding")
        if intent is None and not binding.credential_rotation.get("owner_initiated"):
            raise ValueError("no owner-initiated renewal to resume")
        _renew(room_id, request_owner=bool(intent), owner_intent=intent, report=lambda _text: None)

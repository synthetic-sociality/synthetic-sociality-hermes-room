"""Model-independent, installation-bound maintenance for owner renewals.

The adapter drains one binding before calling this engine. The same journaled
CLI engine and per-binding file lock cover crash recovery and concurrent CLI
invocations. No process environment, active profile, model or sibling changes.
"""
from pathlib import Path

from .protocol import RoomProtocol
from .state import RoomBinding, load, state_path_scope


def eligible(binding: RoomBinding) -> bool:
    return bool(
        binding.installation_id and not binding.revoked
        and (binding.enabled or binding.disabled_reason == "credential_expired")
        and (not binding.credential_rotation or binding.credential_rotation.get("owner_initiated"))
    )


def quiescent(binding: RoomBinding) -> bool:
    # Renewal is not a repair for pending or quarantined canonical work.
    return not (
        binding.delivery_intents or binding.delivery_lifecycle
        or binding.pending_since
        or any(value not in {"posted", "skipped", "failed", "cancelled", "superseded", "ignored"}
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
        if binding is None or not eligible(binding) or not quiescent(binding):
            raise ValueError("renewal requires the exact eligible quiescent binding")
        if intent is None and not binding.credential_rotation.get("owner_initiated"):
            raise ValueError("no owner-initiated renewal to resume")
        _renew(room_id, request_owner=bool(intent), owner_intent=intent, report=lambda _text: None)

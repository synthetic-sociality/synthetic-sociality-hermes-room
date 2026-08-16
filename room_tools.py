"""Hermes operational surface for universal connector runtime actions."""

from __future__ import annotations

import hashlib
import json
import os
import fcntl
import tempfile
import time
from pathlib import Path
from typing import Any

from .context import canonical_room_context, recent_room_messages
from .protocol import (
    MESSAGE_LOGICAL_CONTRIBUTION_CAPABILITY,
    ProtocolError,
    RoomProtocol,
    stable_key,
)
from .state import RoomBinding, load, state_root


ROOM_CONTEXT_SCHEMA = {
    "description": "Read named canonical context from a Room configured for this Hermes profile.",
    "parameters": {
        "type": "object",
        "properties": {
            "room": {
                "type": "string",
                "description": "Optional configured room ID or exact room title. Omit when only one room is configured.",
            },
        },
        "additionalProperties": False,
    },
}

ROOM_POST_SCHEMA = {
    "description": "Post one explicit user-approved contribution through the configured Room conversation policy.",
    "parameters": {
        "type": "object",
        "properties": {
            "room": {
                "type": "string",
                "description": "Configured room ID or exact room title. Required when more than one room is configured.",
            },
            "body": {
                "type": "string",
                "description": "The exact user-approved contribution to post as this Room membership.",
            },
            "requestId": {
                "type": "string",
                "description": "Optional stable external request ID. Reuse it for retries; change it only for a separately approved identical post.",
            },
        },
        "required": ["body"],
        "additionalProperties": False,
    },
}


def _result(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _enabled_bindings() -> list[RoomBinding]:
    return [binding for binding in load().bindings if binding.enabled and not binding.revoked]


def _select_room(requested: str) -> tuple[RoomBinding | None, dict[str, Any] | None, list[dict[str, str]]]:
    bindings = _enabled_bindings()
    if not bindings:
        return None, None, []
    if requested:
        exact = next((binding for binding in bindings if requested == binding.room_id), None)
        if exact is not None:
            state = RoomProtocol(exact.base_url, exact.credential).room_state(exact.room_id)
            return exact, state, []
    candidates: list[tuple[RoomBinding, dict[str, Any]]] = []
    for binding in bindings:
        try:
            state = RoomProtocol(binding.base_url, binding.credential).room_state(binding.room_id)
        except (ProtocolError, OSError, ValueError):
            continue
        title = str(state.get("title") or "").strip()
        if not requested or title.casefold() == requested.casefold():
            candidates.append((binding, state))
    choices = [
        {"roomId": binding.room_id, "title": str(state.get("title") or binding.room_id)}
        for binding, state in candidates
    ]
    if len(candidates) == 1:
        return candidates[0][0], candidates[0][1], choices
    return None, None, choices


def room_context(args: dict[str, Any], **_kwargs: Any) -> str:
    """Return named canonical context without mutating Room state."""
    requested = str((args or {}).get("room") or "").strip()
    try:
        binding, state, choices = _select_room(requested)
        if binding is None or state is None:
            if choices:
                return _result({"success": False, "selectionRequired": True, "rooms": choices})
            return _result({"success": False, "error": "That room is not configured for this Hermes profile."})
        api = RoomProtocol(binding.base_url, binding.credential)
        head = int(state.get("headSeq") or 0)
        active_epoch = state.get("activeEpoch") or {}
        context_events = recent_room_messages(
            lambda after: api.events(binding.room_id, after, 0),
            before_seq=head + 1,
            active_epoch_starts_at=int(active_epoch.get("startsAtSeq") or 1),
        )
    except (ProtocolError, OSError, ValueError) as error:
        return _result({"success": False, "error": f"Room context is temporarily unavailable: {error}"})
    return _result({
        "success": True,
        "roomId": binding.room_id,
        "title": str(state.get("title") or binding.room_id),
        "headSeq": int(state.get("headSeq") or 0),
        "context": canonical_room_context(state, context_events),
    })


def _origin_id(room_id: str, body: str, metadata: dict[str, Any]) -> str:
    request_id = str(metadata.get("request_id") or "")
    origin = ({
        "roomId": room_id,
        "membershipId": str(metadata.get("membership_id") or ""),
        "requestId": request_id,
    } if request_id else {
        "roomId": room_id,
        "membershipId": str(metadata.get("membership_id") or ""),
        "body": body,
        "sessionId": str(metadata.get("session_id") or ""),
        "taskId": str(metadata.get("task_id") or ""),
        "userTask": str(metadata.get("user_task") or ""),
    })
    digest = hashlib.sha256(json.dumps(origin, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return "external-channel:" + digest


def _action_path(source_id: str) -> Path:
    digest = hashlib.sha256(source_id.encode("utf-8")).hexdigest()
    return state_root() / "external-actions" / f"{digest}.json"


def _read_action(source_id: str) -> dict[str, Any] | None:
    path = _action_path(source_id)
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file() or path.parent.is_symlink():
        raise ValueError("external action journal must be a regular private file")
    return json.loads(path.read_text(encoding="utf-8"))


def _write_action(source_id: str, action: dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    path = _action_path(source_id)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    encoded = json.dumps(action, sort_keys=True, separators=(",", ":")) + "\n"
    if create:
        lock_path = path.with_suffix(".lock")
        with lock_path.open("a+b") as lock:
            os.chmod(lock_path, 0o600)
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            existing = _read_action(source_id)
            if existing is not None:
                return existing
            # Publish the initial journal with the same temp+replace discipline
            # as every update. Readers can observe no file or a complete JSON
            # document, never a partially written accepted request body.
            return _write_action(source_id, action, create=False)
    fd, temporary = tempfile.mkstemp(prefix=".external-action-", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
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
    return action


def _retryable_call(operation: Any, *, attempts: int = 3) -> Any:
    """Replay one idempotent protocol operation after ambiguous transport failure."""
    for attempt in range(attempts):
        try:
            return operation()
        except ProtocolError as error:
            if not error.retryable or attempt + 1 >= attempts:
                raise
            time.sleep(min(max(error.retry_after, 0.05), 1.0))
    raise AssertionError("unreachable")


def _with_logical_contribution_id(api: Any, logical_contribution_id: str) -> Any:
    binder = getattr(api, "with_logical_contribution_id", None)
    return binder(logical_contribution_id) if callable(binder) else api


def _with_message_payload_dialect(api: Any, payload_dialect: str) -> Any:
    binder = getattr(api, "with_message_payload_dialect", None)
    return binder(payload_dialect) if callable(binder) else api


def _read_payload_dialect(api: RoomProtocol) -> str:
    """Choose a dialect from the explicit read-only server status response."""
    capabilities = api.status().get("protocolCapabilities")
    if capabilities is None:
        return "v1"
    if not isinstance(capabilities, list) or not all(isinstance(value, str) for value in capabilities):
        raise ProtocolError(
            "Room status returned malformed protocol capabilities",
            code="message_contract_unavailable",
            retryable=True,
        )
    return "v2" if MESSAGE_LOGICAL_CONTRIBUTION_CAPABILITY in capabilities else "v1"


def _request_turn(api: RoomProtocol, binding: RoomBinding, state: dict[str, Any], source_id: str) -> tuple[dict[str, Any], int]:
    observed = int(state.get("headSeq") or 0)
    while True:
        try:
            turn = _retryable_call(lambda: api.request_turn(binding.room_id, observed, source_id))
            return turn, observed
        except ProtocolError as error:
            if error.code != "stale_context":
                raise
            state = api.room_state(binding.room_id)
            observed = max(observed, int(state.get("headSeq") or 0))


def _wait_for_turn(api: RoomProtocol, binding: RoomBinding, turn: dict[str, Any], observed: int, source_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + 120.0
    while str(turn.get("state") or "") not in {"granted", "finished"}:
        if str(turn.get("state") or "") in {"expired", "revoked", "cancelled"}:
            raise ProtocolError("Room turn is no longer active", code="turn_not_active")
        if time.monotonic() >= deadline:
            raise ProtocolError("Timed out waiting for the Room turn", code="turn_wait_timeout", retryable=True)
        time.sleep(0.5)
        turn = _retryable_call(lambda: api.request_turn(binding.room_id, observed, source_id))
    return turn


def _finish_action(
    api: RoomProtocol, binding: RoomBinding, action: dict[str, Any], source_id: str, observed: int,
) -> None:
    observed = int(action.get("finishObserved") or observed)
    while True:
        action["finishObserved"] = observed
        _write_action(source_id, action)
        try:
            _retryable_call(lambda: api.finish_turn(
                binding.room_id, str(action.get("turnId") or ""), observed, source_id,
            ))
            return
        except ProtocolError as error:
            if error.code in {"turn_not_active", "turn_expired"}:
                return
            if error.code != "stale_context":
                raise
            fresh = api.room_state(binding.room_id)
            observed = max(observed, int(fresh.get("headSeq") or 0))


def room_post(args: dict[str, Any], **metadata: Any) -> str:
    """Post one explicit external-channel contribution through Room policy."""
    requested = str((args or {}).get("room") or "").strip()
    body = str((args or {}).get("body") or "").strip()
    request_id = str((args or {}).get("requestId") or "").strip()
    if not body:
        return _result({"success": False, "error": "body is required"})
    if len(body.encode("utf-8")) > 16 * 1024:
        return _result({"success": False, "error": "body exceeds the 16 KiB connector limit"})
    event: dict[str, Any] | None = None
    try:
        binding, state, choices = _select_room(requested)
        if binding is None or state is None:
            if choices:
                return _result({"success": False, "selectionRequired": True, "rooms": choices})
            return _result({"success": False, "error": "That room is not configured for this Hermes profile."})
        api = RoomProtocol(binding.base_url, binding.credential)
        policy_reader = getattr(api, "room_policy", None)
        policy_response = policy_reader(binding.room_id) if callable(policy_reader) else {"coordinationMode": "coordinated"}
        policy = policy_response.get("policy") if isinstance(policy_response.get("policy"), dict) else policy_response
        coordination_mode = str(policy.get("coordinationMode") or "coordinated")
        source_id = _origin_id(binding.room_id, body, {
            **metadata, "request_id": request_id, "membership_id": binding.membership_id,
        })
        action = _read_action(source_id)
        if action is None:
            payload_dialect = _read_payload_dialect(api)
            action = _write_action(source_id, {
                "version": 1,
                "sourceId": source_id,
                "roomId": binding.room_id,
                "body": body,
                "coordinationMode": coordination_mode,
                "messagePayloadDialect": payload_dialect,
                "turnObserved": int(state.get("headSeq") or 0),
            }, create=True)
        payload_dialect = str(action.get("messagePayloadDialect") or "")
        if payload_dialect not in {"v1", "v2"}:
            raise ProtocolError(
                "existing external action has no frozen message payload dialect",
                code="message_contract_unavailable",
                retryable=False,
            )
        if action.get("roomId") != binding.room_id or action.get("body") != body:
            raise ProtocolError("external action origin was reused with different content")
        action_mode = str(action.get("coordinationMode") or "coordinated")
        if action_mode != coordination_mode and not action.get("canonicalEventId"):
            raise ProtocolError(
                "Room conversation policy changed before delivery; retry with a new requestId",
                code="stale_context",
                retryable=False,
            )
        if action.get("superseded"):
            if not action.get("finished") and action.get("turnId"):
                _finish_action(
                    api, binding, action, source_id,
                    int(action.get("finishObserved") or action.get("postObserved") or action.get("turnObserved") or 0),
                )
                action["finished"] = True
                _write_action(source_id, action)
            return _result({
                "success": False,
                "error": "The Room epoch changed before delivery. The granted turn was released; send again as a new external request.",
                "retryable": False,
            })

        observed = int(action.get("turnObserved") or 0)
        if action.get("canonicalEventId"):
            event = {
                "id": str(action["canonicalEventId"]),
                "seq": int(action.get("canonicalEventSeq") or 0),
            }
            if not action.get("finished") and action.get("turnId"):
                try:
                    _finish_action(api, binding, action, source_id, int(event["seq"] or observed))
                    action["finished"] = True
                    _write_action(source_id, action)
                except ProtocolError as error:
                    if error.code in {"turn_not_active", "turn_expired"}:
                        action["finished"] = True
                        _write_action(source_id, action)
                    else:
                        raise
            return _result({
                "success": True, "roomId": binding.room_id,
                "canonicalEventId": str(event["id"]), "seq": int(event["seq"]),
                "replayed": True,
            })

        if coordination_mode == "open":
            # The operational Hermes instance follows the same free-room path
            # as automatic replies: no cycle and no speaking-turn lease. The
            # exact observed head plus source-derived idempotency key provide
            # canonical serialization and restart-safe replay.
            if "postObserved" not in action:
                fresh = api.room_state(binding.room_id)
                action["postObserved"] = int(fresh.get("headSeq") or 0)
                action["epochId"] = str((fresh.get("activeEpoch") or {}).get("id") or "")
                _write_action(source_id, action)
            observed = int(action["postObserved"])
            epoch_id = str(action.get("epochId") or "")
            while True:
                try:
                    event = _retryable_call(lambda: _with_message_payload_dialect(
                        _with_logical_contribution_id(api, stable_key(
                            "logical-contribution", source_id,
                            room_id=binding.room_id, membership_id=binding.membership_id,
                        )), payload_dialect,
                    ).post_message(
                        binding.room_id, "", observed, source_id, body,
                        epoch_id, standalone=True,
                    ))
                    break
                except ProtocolError as error:
                    if error.code == "stale_epoch":
                        action["superseded"] = True
                        _write_action(source_id, action)
                        return _result({
                            "success": False,
                            "error": "The Room epoch changed before delivery; send again as a new external request.",
                            "retryable": False,
                        })
                    if error.code != "stale_context":
                        raise
                    fresh = api.room_state(binding.room_id)
                    observed = max(observed, int(fresh.get("headSeq") or 0))
                    epoch_id = str((fresh.get("activeEpoch") or {}).get("id") or epoch_id)
                    action["postObserved"] = observed
                    action["epochId"] = epoch_id
                    _write_action(source_id, action)
            action["canonicalEventId"] = str(event.get("id") or "")
            action["canonicalEventSeq"] = int(event.get("seq") or 0)
            action["finished"] = True
            _write_action(source_id, action)
            return _result({
                "success": True, "roomId": binding.room_id,
                "canonicalEventId": str(event.get("id") or ""),
                "seq": int(event.get("seq") or 0),
            })

        while True:
            try:
                turn = _retryable_call(lambda: api.request_turn(binding.room_id, observed, source_id))
                break
            except ProtocolError as error:
                if error.code != "stale_context":
                    raise
                fresh = api.room_state(binding.room_id)
                observed = max(observed, int(fresh.get("headSeq") or 0))
                action["turnObserved"] = observed
                _write_action(source_id, action)
        action["turnId"] = str(turn.get("turnId") or action.get("turnId") or "")
        _write_action(source_id, action)
        turn = _wait_for_turn(api, binding, turn, observed, source_id)
        action["turnId"] = str(turn.get("turnId") or action.get("turnId") or "")
        if "postObserved" not in action:
            fresh = api.room_state(binding.room_id)
            action["postObserved"] = max(observed, int(fresh.get("headSeq") or 0))
            action["epochId"] = str((fresh.get("activeEpoch") or {}).get("id") or "")
            _write_action(source_id, action)
        observed = int(action["postObserved"])
        epoch_id = str(action.get("epochId") or "")
        def post() -> dict[str, Any]:
            return _with_message_payload_dialect(
                _with_logical_contribution_id(api, stable_key(
                    "logical-contribution", source_id,
                    room_id=binding.room_id, membership_id=binding.membership_id,
                )), payload_dialect,
            ).post_message(
                binding.room_id, str(action.get("turnId") or ""), observed, source_id, body,
                epoch_id, standalone=True,
            )

        try:
            event = _retryable_call(post)
        except ProtocolError as error:
            if error.code == "stale_epoch":
                fresh = api.room_state(binding.room_id)
                action["superseded"] = True
                _finish_action(
                    api, binding, action, source_id,
                    max(observed, int(fresh.get("headSeq") or 0)),
                )
                action["finished"] = True
                _write_action(source_id, action)
                return _result({
                    "success": False,
                    "error": "The Room epoch changed before delivery. The granted turn was released; send again as a new external request.",
                    "retryable": False,
                })
            if error.code != "stale_context":
                raise
            fresh = api.room_state(binding.room_id)
            observed = max(observed, int(fresh.get("headSeq") or 0))
            epoch_id = str((fresh.get("activeEpoch") or {}).get("id") or epoch_id)
            action["postObserved"] = observed
            action["epochId"] = epoch_id
            _write_action(source_id, action)
            event = _retryable_call(post)
        action["canonicalEventId"] = str(event.get("id") or "")
        action["canonicalEventSeq"] = int(event.get("seq") or 0)
        _write_action(source_id, action)
        try:
            _finish_action(api, binding, action, source_id, int(event.get("seq") or observed))
            action["finished"] = True
            _write_action(source_id, action)
        except ProtocolError as error:
            if error.code in {"turn_not_active", "turn_expired"}:
                action["finished"] = True
                _write_action(source_id, action)
            else:
                raise
        return _result({
            "success": True, "roomId": binding.room_id,
            "canonicalEventId": str(event.get("id") or ""), "seq": int(event.get("seq") or 0),
        })
    except (ProtocolError, OSError, ValueError) as error:
        if event is not None and isinstance(error, ProtocolError) and error.code in {"turn_not_active", "turn_expired"}:
            return _result({"success": True, "canonicalEventId": str(event.get("id") or ""), "seq": int(event.get("seq") or 0)})
        return _result({"success": False, "error": str(error), "retryable": bool(getattr(error, "retryable", False))})

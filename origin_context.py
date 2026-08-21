"""Process-local provenance fence for Room-origin Hermes turns."""

from __future__ import annotations

import threading
from typing import Any

ROOM_PLATFORM_NAME = "synthetic_sociality"

_LOCK = threading.Lock()
_SESSION_IDS: set[str] = set()
_TURN_IDS: set[str] = set()


def _safe_id(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _platform_value(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().lower()


def remember_room_context(session_id: Any, turn_id: Any = None) -> None:
    safe_session_id = _safe_id(session_id)
    safe_turn_id = _safe_id(turn_id)
    with _LOCK:
        if safe_session_id:
            _SESSION_IDS.add(safe_session_id)
        if safe_turn_id:
            _TURN_IDS.add(safe_turn_id)


def forget_room_context(
    session_id: Any, turn_id: Any = None, *, forget_session: bool = False,
) -> None:
    safe_session_id = _safe_id(session_id)
    safe_turn_id = _safe_id(turn_id)
    with _LOCK:
        if safe_turn_id:
            _TURN_IDS.discard(safe_turn_id)
        if forget_session and safe_session_id:
            _SESSION_IDS.discard(safe_session_id)


def is_room_context(**metadata: Any) -> bool:
    if _platform_value(metadata.get("platform")) == ROOM_PLATFORM_NAME:
        remember_room_context(metadata.get("session_id"), metadata.get("turn_id"))
        return True
    safe_session_id = _safe_id(metadata.get("session_id"))
    safe_turn_id = _safe_id(metadata.get("turn_id"))
    with _LOCK:
        return bool(
            (safe_turn_id and safe_turn_id in _TURN_IDS)
            or (safe_session_id and safe_session_id in _SESSION_IDS)
        )

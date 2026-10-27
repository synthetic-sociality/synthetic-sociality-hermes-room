"""Process-local provenance fence for Room-origin Hermes turns."""

from __future__ import annotations

import threading
from typing import Any

ROOM_PLATFORM_NAME = "synthetic_sociality"


def _process_state(name, factory):
    """One state object per process, whatever module copy asks: a core that evicts and reloads
    plugin modules would otherwise give the Room adapter and the share tool separate copies."""
    import sys as _sys, types as _types
    holder = _sys.modules.get('_synthetic_sociality_room_process_state')
    if holder is None:
        holder = _sys.modules.setdefault('_synthetic_sociality_room_process_state', _types.ModuleType('_synthetic_sociality_room_process_state'))
    if not hasattr(holder, name):
        setattr(holder, name, factory())
    return getattr(holder, name)

_LOCK = _process_state('origin_lock', threading.Lock)
_SESSION_IDS: set[str] = _process_state('origin_session_ids', set)
_TURN_IDS: set[str] = _process_state('origin_turn_ids', set)


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


# Contract P7d-5: the host session a Room turn runs in, as core reports it to
# the pre_llm_call hook (``sender_id`` is the Room id the adapter sets as the
# turn's user). Process memory only, at most ROOM_SESSION_LIMIT sessions.
ROOM_SESSION_LIMIT = 256
_ROOM_BY_SESSION: dict[str, str] = _process_state('origin_room_by_session', dict)
_SESSION_BY_ROOM: dict[str, str] = _process_state('origin_session_by_room', dict)


def remember_room_session(session_id: Any, room_id: Any) -> None:
    safe_session_id, safe_room_id = _safe_id(session_id), _safe_id(room_id)
    if not safe_session_id or not safe_room_id:
        return
    with _LOCK:
        _ROOM_BY_SESSION.pop(safe_session_id, None)
        _ROOM_BY_SESSION[safe_session_id] = safe_room_id
        _SESSION_BY_ROOM[safe_room_id] = safe_session_id
        while len(_ROOM_BY_SESSION) > ROOM_SESSION_LIMIT:
            _ROOM_BY_SESSION.pop(next(iter(_ROOM_BY_SESSION)))
        while len(_SESSION_BY_ROOM) > ROOM_SESSION_LIMIT:
            _SESSION_BY_ROOM.pop(next(iter(_SESSION_BY_ROOM)))


def room_for_session(session_id: Any) -> str:
    with _LOCK:
        return _ROOM_BY_SESSION.get(_safe_id(session_id), "")


def host_session_for_room(room_id: Any) -> str:
    with _LOCK:
        return _SESSION_BY_ROOM.get(_safe_id(room_id), "")

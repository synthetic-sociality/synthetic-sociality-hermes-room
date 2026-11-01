"""The Hermes profile this copy of the plugin serves.

Under ``gateway.multiplex_profiles`` one gateway process serves every profile. ``os.environ`` then holds the host
(default) profile's values; a served profile exists only as Hermes' context-local home and its terminal scope. Hermes
imports a directory plugin once per home, inside that home's scope, so what a copy sees at import is its own home for
as long as it lives. That holds on threads and executors, which do not inherit the context.

A process that imports the plugin outside any scope (a CLI on an older core, the test suite) follows Hermes' own
resolution at each call, as it always did.
"""
from __future__ import annotations

import os
import sys
import types
from pathlib import Path
from typing import Any, Callable, Mapping


def _home_override() -> str | None:
    try:
        from hermes_constants import get_hermes_home_override
    except ImportError:  # a core without context-local homes, or no core at all
        return None
    return get_hermes_home_override()


_LOADED_HOME = _home_override()
# Values of the host profile that say nothing about a served profile.
_HOST_ONLY_NAMES = ("HERMES_PROFILE", "HERMES_MODEL", "HERMES_PROVIDER")
# These too, on a core that keeps a terminal scope per profile. An older core's terminal tool reads the process's.
_TERMINAL_PREFIX = "TERMINAL_"
_TERMINAL_NAMES = ("MESSAGING_CWD",)


def home() -> Path:
    """This profile's Hermes home."""
    if _LOADED_HOME:
        return Path(_LOADED_HOME).expanduser()
    try:
        from hermes_constants import get_hermes_home
    except ImportError:
        configured = os.environ.get("HERMES_HOME", "").strip()
        return Path(configured).expanduser() if configured else Path.home() / ".hermes"
    return get_hermes_home()


def _served_by_a_host() -> bool:
    """True when this copy belongs to another profile than the one the process environment describes."""
    if not _LOADED_HOME:
        return False
    process_home = os.environ.get("HERMES_HOME", "").strip() or str(Path.home() / ".hermes")
    return os.path.realpath(os.path.expanduser(process_home)) != os.path.realpath(home())


def environ() -> Mapping[str, str]:
    """The process environment as this profile would have it in a gateway of its own."""
    # Read, never imported: if the core runs terminal scopes its module is already loaded.
    reader = getattr(sys.modules.get("tools.terminal_scope"), "get_terminal_scope", None)
    scoped_core = callable(reader)
    scope = reader() if scoped_core else None
    served = _served_by_a_host()
    if not scope and not served:
        return os.environ
    merged = dict(os.environ)
    if served:
        for name in list(merged):
            if name in _HOST_ONLY_NAMES or (scoped_core and (name.startswith(_TERMINAL_PREFIX) or name in _TERMINAL_NAMES)):
                del merged[name]
        merged["HERMES_HOME"] = str(home())
    merged.update(scope or {})
    return merged


def process_state(name: str, factory: Callable[[], Any]) -> Any:
    """One state object per profile in this process, whatever module copy asks: a core that evicts and reloads plugin
    modules would otherwise give the Room adapter and the share tool separate copies, and two profiles in one Room
    must not read each other's sessions."""
    holder = sys.modules.get("_synthetic_sociality_room_process_state")
    if holder is None:
        holder = sys.modules.setdefault(
            "_synthetic_sociality_room_process_state", types.ModuleType("_synthetic_sociality_room_process_state"),
        )
    key = f"{name}@{_LOADED_HOME}" if _LOADED_HOME else name
    if not hasattr(holder, key):
        setattr(holder, key, factory())
    return getattr(holder, key)

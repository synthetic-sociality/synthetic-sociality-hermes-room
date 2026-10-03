"""Contract P2c-1: the Room credential store is out of the agent's tools' reach.

Hermes runs its file, shell and code tools (read_file, search_files, patch,
terminal, process, execute_code) through one terminal backend. On the ``local``
or ``ssh`` backend they run as the gateway's own OS user on a machine that
holds the store, so nothing in this process can make the store unreadable to
them. On a sandbox backend they run where the store is not mounted. The
connector therefore:

1. reports whether the store is reachable (``store_reachability``): a host
   backend, or a sandbox mount that contains the store; it refuses to start
   while the store is reachable only when the installation enables
   ``enforceSecretIsolation`` (platforms.synthetic_sociality.extra);
2. blocks, in every session, any tool call whose path arguments resolve into
   the store (or, for a search, contain it), and any shell or code call that
   names it (``tool_call_block``); a hook error blocks;
3. removes the exact credential and renewal secret values from every tool
   result before it reaches the model (``scrub``).

Before enforcement is switched on, the host sets ``terminal.backend`` to a
sandbox and mounts nothing that contains HERMES_HOME. Reason codes never contain a path or a secret.
"""

from __future__ import annotations

import json
import os
import re
import threading
from pathlib import Path
from typing import Any, Iterable, Mapping

ISOLATING_BACKENDS = frozenset({
    "docker", "singularity", "modal", "managed_modal", "daytona", "vercel_sandbox",
})
SHELL_TOOLS = frozenset({"terminal", "process", "process_manage", "execute_code"})
# Contract P3-7: the tools that reach the host filesystem. With core's own
# tool selection none of the shell/code tools may remain for the denied route
# (process is process_manage on newer cores); computer use runs on the host
# desktop in either route.
HOST_SHELL_TOOL_NAMES = frozenset({"terminal", "process", "process_manage", "execute_code"})
HOST_DESKTOP_TOOL_NAMES = frozenset({"computer_use"})
# Contract P3-6: path arguments always resolved, whatever their shape.
KNOWN_PATH_KEYS = frozenset({"path", "file_path", "file", "cwd", "dir", "directory", "root", "workdir"})
SEARCH_TOOLS = frozenset({"search_files"})
STORE_DIRECTORY_NAME = "synthetic-sociality-room"
REFUSAL_CODE = "secret_store_reachable"
# Contract L1-15: the core's tool selection could not be read; unknown counts
# as reaching the store.
TOOL_SELECTION_UNAVAILABLE = "tool_selection_unavailable"
# The per-installation switch: off reports and keeps running, on refuses.
ENFORCEMENT_SETTING = "enforceSecretIsolation"
BLOCK_MESSAGE = (
    "Blocked by the Synthetic Sociality Room connector: its private state holds the Room "
    "credential and is not available to agent tools."
)
REDACTED = "[redacted Room credential]"
_PATCH_FILE_LINE = re.compile(r"^\*\*\* (?:Update|Add|Delete) File: (.+)$|^\*\*\* Move to: (.+)$", re.MULTILINE)
_SECRET_KEY = re.compile(r"credential|secret|token", re.IGNORECASE)
_MIN_SECRET_LENGTH = 16

_lock = threading.Lock()
_secret_values: set[str] = set()


def _resolved(value: str | os.PathLike[str], base: str = "") -> Path:
    path = Path(os.path.expanduser(str(value)))
    if not path.is_absolute():
        path = Path(base or os.getcwd()) / path
    return Path(os.path.realpath(path))


def _within(child: Path, parent: Path) -> bool:
    return child == parent or parent in child.parents


def _overlaps(left: Path, right: Path) -> bool:
    return _within(left, right) or _within(right, left)


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _sandbox_mount_sources() -> list[str]:
    """Host paths Hermes itself mounts or syncs into every sandbox."""
    try:
        import tools.credential_files as credential_files  # Hermes core
    except Exception:
        return []
    sources: list[str] = []
    for getter in ("get_credential_file_mounts", "get_skills_directory_mount", "get_cache_directory_mounts"):
        try:
            for entry in getattr(credential_files, getter)() or []:
                host_path = str((entry or {}).get("host_path") or "")
                if host_path:
                    sources.append(host_path)
        except Exception:
            continue
    return sources


def shell_tools_denied(tool_names: Iterable[str] | None) -> bool:
    """Contract P3-7: core's own tool selection holds no shell or code tool.

    ``tool_names`` is the set core selects for the profile (None when core
    cannot report it, which denies nothing).
    """
    return tool_names is not None and not (HOST_SHELL_TOOL_NAMES & set(tool_names))


def store_reachability(store_dir: str | os.PathLike[str], environ: Mapping[str, str] | None = None,
                       mount_sources: Iterable[str] | None = None,
                       tool_names: Iterable[str] | None = None,
                       mcp_servers: Iterable[str] = ()) -> list[str]:
    """Reason codes for which the agent's tools can reach the store; [] if none."""
    environ = os.environ if environ is None else environ
    names = None if tool_names is None else set(tool_names)
    reasons = _shell_route_reasons(store_dir, environ, mount_sources, names)
    # Host-side tools outside both routes (P3-7).
    if names is not None and HOST_DESKTOP_TOOL_NAMES & names:
        reasons.append("computer_use_tool")
    if list(mcp_servers):
        reasons.append("mcp_server_enabled")
    return reasons


def _shell_route_reasons(store_dir: str | os.PathLike[str], environ: Mapping[str, str],
                         mount_sources: Iterable[str] | None, names: set[str] | None) -> list[str]:
    store = _resolved(store_dir)
    reasons: list[str] = []
    backend = (environ.get("TERMINAL_ENV") or "local").strip().lower() or "local"
    if shell_tools_denied(names):
        # No shell or code tool exists for the agent; the file tools are fenced
        # by the pre_tool_call hook (P2c-1, P3-7).
        return reasons
    if backend not in ISOLATING_BACKENDS:
        reasons.append("host_shell_backend")
        return reasons
    if backend == "docker":
        try:
            volumes = json.loads(environ.get("TERMINAL_DOCKER_VOLUMES") or "[]")
        except json.JSONDecodeError:
            volumes = None
        if not isinstance(volumes, list):
            reasons.append("sandbox_volumes_unreadable")
        else:
            for volume in volumes:
                host = str(volume).split(":", 1)[0].strip()
                if host and (host.startswith(("/", "~"))) and _overlaps(_resolved(host), store):
                    reasons.append("sandbox_volume_covers_store")
                    break
        if _truthy(environ.get("TERMINAL_DOCKER_MOUNT_CWD_TO_WORKSPACE", "")):
            cwd = (environ.get("TERMINAL_CWD") or environ.get("MESSAGING_CWD") or "").strip() or os.getcwd()
            if _overlaps(_resolved(cwd), store):
                reasons.append("sandbox_cwd_covers_store")
    sources = _sandbox_mount_sources() if mount_sources is None else list(mount_sources)
    if any(_overlaps(_resolved(source), store) for source in sources if source):
        reasons.append("sandbox_mount_covers_store")
    return reasons


def _string_leaves(value: Any, depth: int = 0) -> Iterable[str]:
    if depth > 6:
        return
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _string_leaves(item, depth + 1)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _string_leaves(item, depth + 1)


def _path_candidates(tool_name: str, args: Mapping[str, Any]) -> Iterable[str]:
    # Contract P3-6: a known path argument is a path whatever its shape
    # ("synthetic-sociality-room", "state.json").
    for key in KNOWN_PATH_KEYS:
        value = args.get(key) if isinstance(args, Mapping) else None
        for item in (value if isinstance(value, (list, tuple)) else [value]):
            if isinstance(item, str) and item.strip() and len(item) < 4096 and "\n" not in item:
                yield item.strip()
    for text in _string_leaves(args):
        candidate = text.strip()
        if candidate and len(candidate) < 4096 and "\n" not in candidate and (
            candidate.startswith(("/", "~", "."))
            or ("/" in candidate and not any(ch.isspace() for ch in candidate))
        ):
            yield candidate
        if tool_name == "patch":
            for match in _PATCH_FILE_LINE.finditer(text):
                yield (match.group(1) or match.group(2) or "").strip()


def store_spellings(store_dir: str | os.PathLike[str], environ: Mapping[str, str] | None = None) -> set[str]:
    """The ways a command can name the store itself: its absolute path, and the
    same path written from ~, $HOME or $HERMES_HOME. The bare directory name is
    not one: a plugin directory of the same name (~/.hermes/plugins/...) or a
    command argument naming the plugin is not the store."""
    environ = os.environ if environ is None else environ
    paths = {str(Path(os.path.expanduser(str(store_dir)))), str(_resolved(store_dir))}
    spellings = set(paths)
    home = os.path.expanduser("~")
    hermes_home = (environ.get("HERMES_HOME") or "").strip() or os.path.join(home, ".hermes")
    prefixes = [(home, ("~", "$HOME", "${HOME}"))]
    for root in {os.path.expanduser(hermes_home), str(_resolved(hermes_home))}:
        prefixes.append((root, ("$HERMES_HOME", "${HERMES_HOME}")))
    for path in paths:
        for root, names in prefixes:
            root = root.rstrip("/")
            if root and path.startswith(root + "/"):
                spellings.update(name + path[len(root):] for name in names)
    return {spelling for spelling in spellings if spelling and spelling != "/"}


def _tool_base(cwd: str, environ: Mapping[str, str]) -> str:
    """The directory a relative tool path is resolved against: the tool's own
    cwd, else the terminal cwd Hermes configures (TERMINAL_CWD, MESSAGING_CWD,
    else the home directory)."""
    for value in (cwd, environ.get("TERMINAL_CWD"), environ.get("MESSAGING_CWD")):
        if value and str(value).strip():
            return os.path.expanduser(str(value).strip())
    return os.path.expanduser("~")


def tool_call_block(tool_name: str, args: Any, store_dir: str | os.PathLike[str],
                    environ: Mapping[str, str] | None = None, *, cwd: str = "") -> str | None:
    """The block message for a tool call that would reach the store, else None."""
    environ = os.environ if environ is None else environ
    store = _resolved(store_dir)
    mapping = args if isinstance(args, Mapping) else {"value": args}
    if tool_name in SHELL_TOOLS:
        # Backstop only: a command string cannot be fenced reliably. The start
        # gate keeps shell tools on a backend where the store does not exist.
        names = store_spellings(store_dir, environ)
        if any(name in text for text in _string_leaves(mapping) for name in names):
            return BLOCK_MESSAGE
    base = _tool_base(cwd, environ)
    if tool_name in SEARCH_TOOLS and not str(mapping.get("path") or "").strip():
        # A search without a path searches its cwd.
        if _within(store, _resolved(base)):
            return BLOCK_MESSAGE
    for candidate in _path_candidates(tool_name, mapping):
        if not candidate:
            continue
        resolved = _resolved(candidate, base)
        if _within(resolved, store) or (tool_name in SEARCH_TOOLS and _within(store, resolved)):
            return BLOCK_MESSAGE
    return None


def remember_secrets(bindings: Iterable[Any]) -> None:
    """Record the exact secret values of these bindings for result scrubbing."""
    values: set[str] = set()
    for binding in bindings:
        credential = str(getattr(binding, "credential", "") or "")
        if len(credential) >= _MIN_SECRET_LENGTH:
            values.add(credential)
        values.update(_journal_secrets(getattr(binding, "credential_rotation", {}) or {}))
    with _lock:
        _secret_values.update(values)


def remember_values(values: Iterable[str]) -> None:
    """Record further exact secret values (contract L1-10: link key, pairing
    secret, local Gateway credential) for result scrubbing."""
    found = {value for value in values if isinstance(value, str) and len(value) >= _MIN_SECRET_LENGTH}
    if found:
        with _lock:
            _secret_values.update(found)


def _journal_secrets(value: Any, key: str = "", depth: int = 0) -> set[str]:
    found: set[str] = set()
    if depth > 6:
        return found
    if isinstance(value, Mapping):
        for name, item in value.items():
            found |= _journal_secrets(item, str(name), depth + 1)
    elif isinstance(value, (list, tuple)):
        for item in value:
            found |= _journal_secrets(item, key, depth + 1)
    elif isinstance(value, str) and _SECRET_KEY.search(key) and len(value) >= _MIN_SECRET_LENGTH:
        found.add(value)
    return found


def scrub(text: Any) -> Any:
    """Replace every remembered secret value in a string; other values unchanged."""
    if not isinstance(text, str) or not text:
        return text
    with _lock:
        values = sorted(_secret_values, key=len, reverse=True)
    for value in values:
        if value in text:
            text = text.replace(value, REDACTED)
    return text


def forget_secrets() -> None:
    with _lock:
        _secret_values.clear()

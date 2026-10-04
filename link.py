"""Contract L1: device link (Hermes translation L3).

A user links this Hermes profile to their Harness as a local device. The wire
is fixed by SPEC-20261003-hermes-link-L1.md and its G2/G8/G10 addendum, pinned
in the contract section "L1 — Device link". This module holds:

- the token, URL, label and signature rules (L1-1 … L1-3), pure functions;
- the link record in the fenced credential store (L1-9, L1-10);
- the pairing confirmation (L1-2), run by ``hermes room link`` and, while the
  link is pending, by the gateway;
- the gateway-side link service (L1-4 … L1-8): one outbound WebSocket per
  profile, only while linked, each channel bridged to one local Gateway
  WebSocket (G23: the loopback dashboard's session token).

Nothing here logs, prints or stores the token, the pairing secret, the private
key or the local Gateway credential outside the fenced store; every value that
reaches a status, log or exception is a fixed code or an identifier.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import fcntl
import hashlib
import json
import logging
import os
import platform
import random
import re
import secrets
import stat
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterator
from urllib.parse import quote, urlsplit, urlunsplit

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from . import secret_isolation
from .protocol import RECONNECT_CAP_SECONDS, RECONNECT_FIRST_SECONDS, reconnect_delay
from .state import assert_state_directory, state_path

logger = logging.getLogger(__name__)


class _ScrubFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 - leave malformed records to logging
            return True
        scrubbed = secret_isolation.scrub(message)
        if scrubbed != message:
            record.msg, record.args = scrubbed, None
        return True


logger.addFilter(_ScrubFilter())
# Contract L1-10: the WebSocket library logs request lines (the local Gateway
# URL carries the dashboard token) at DEBUG. Its records for our sockets go
# through this logger, which never emits DEBUG and scrubs the rest.
_socket_logger = logging.getLogger(__name__ + ".socket")
_socket_logger.setLevel(logging.INFO)
_socket_logger.addFilter(_ScrubFilter())

TOKEN_PREFIX = "twh1."
MAX_TOKEN_LENGTH = 4096
EXPIRY_SKEW_SECONDS = 60
LABEL_LIMIT = 120
CONFIRMATION_BODY_LIMIT = 4096
CONNECT_SUFFIX = "/local-devices/connect"
CONFIRMATION_SUFFIX = "/local-devices/pairing-confirmations"
LINK_FILE = "link.json"
# Contract L1-9: a (re-)link waits here until its 201; link.json changes only then.
LINK_PENDING_FILE = "link-pending.json"
LINK_LOCK_FILE = "link.lock"
LINK_SCHEMA = "synthetic-sociality-room-link-v1"
SPKI_PREFIX = bytes.fromhex("302a300506032b6570032100")
# Contract L1-5: fallbacks when authentication-complete carries no (valid) limits.
FALLBACK_LIMITS = {
    "maxFrameBytes": 6_291_456,
    "maxDataBytes": 3_145_728,
    "openTimeoutMs": 15_000,
    "maxChannels": 4,
    "keepaliveMs": 25_000,
}
LIMIT_RANGES = {
    "maxFrameBytes": (1024, 8_388_608),
    "maxDataBytes": (1024, 8_388_608),
    "openTimeoutMs": (1000, 60_000),
    "maxChannels": (1, 16),
    "keepaliveMs": (1000, 300_000),
}
# Server messages up to the schema maximum carrier plus 1 KiB (>= 6 MiB + 1 KiB).
CLIENT_MAX_MESSAGE_BYTES = 8_388_608 + 1024
HANDSHAKE_TIMEOUT_SECONDS = 15.0
# Contract L1-8: the reconnect schedule of P2c-3a.
BACKOFF_INITIAL_SECONDS = RECONNECT_FIRST_SECONDS
BACKOFF_MAX_SECONDS = RECONNECT_CAP_SECONDS
RECORD_POLL_SECONDS = 2.0
DEFAULT_LOCAL_GATEWAY_URL = "http://127.0.0.1:9119"
# Literal loopback addresses only: a name ("localhost") is resolved per request.
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1"})
_UUID4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_PAIRING_SECRET = re.compile(r"^[A-Za-z0-9_-]{43}$")
_BASE64URL = re.compile(r"^[A-Za-z0-9_-]+$")
_ISO_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$")
_SESSION_TOKEN_JS = re.compile(r'window\.__HERMES_SESSION_TOKEN__\s*=\s*"([A-Za-z0-9_-]{1,512})"')
_LOCAL_TOKEN = re.compile(r"^[A-Za-z0-9_-]{1,512}$")
# Contract L1-1: plain http/ws only to a loopback host, and only in a test build.
_ALLOW_LOOPBACK_HTTP = False

# Contract L1-8: close code and reason → (status code, action).
STOP, RECONNECT = "stop", "reconnect"


class LinkError(ValueError):
    """A refusal with a fixed code; the message never holds a secret."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class LocalGatewayError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return (value or _now()).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    if not isinstance(value, str) or not _ISO_UTC.match(value):
        raise ValueError("not an ISO-8601 UTC time")
    head, _, rest = value[:-1].partition(".")
    fraction = (rest[:6].ljust(6, "0")) if rest else "000000"
    return datetime.fromisoformat(f"{head}.{fraction}+00:00")


# --- L1-1: token and URLs -------------------------------------------------------


def parse_token(raw: str, *, now: datetime | None = None) -> dict[str, str]:
    """The four fields of a v1 link token; LinkError otherwise (L1-1)."""
    text = (raw or "").strip()
    if not text:
        raise LinkError("link_token_invalid", "no link token was entered")
    if len(text) > MAX_TOKEN_LENGTH:
        raise LinkError("link_token_invalid", "the link token is too long")
    if not text.startswith(TOKEN_PREFIX):
        raise LinkError("link_token_invalid", "this is not a Hermes link token (it must start with twh1.)")
    body = text[len(TOKEN_PREFIX):]
    if not body or not _BASE64URL.match(body):
        raise LinkError("link_token_invalid", "the link token is not valid base64url")
    try:
        decoded = base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))
        payload = json.loads(decoded.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise LinkError("link_token_invalid", "the link token cannot be decoded") from None
    if not isinstance(payload, dict):
        raise LinkError("link_token_invalid", "the link token cannot be decoded")
    version = payload.get("v")
    if type(version) is not int or version != 1:
        raise LinkError("link_token_unsupported", "this link token version is not supported; update the plugin")
    fields: dict[str, str] = {}
    for name in ("audience", "deviceId", "pairingSecret", "expiresAt"):
        value = payload.get(name)
        if not isinstance(value, str) or not value:
            raise LinkError("link_token_invalid", f"the link token has no {name}")
        fields[name] = value
    if not _UUID4.match(fields["deviceId"]):
        raise LinkError("link_token_invalid", "the link token has an invalid deviceId")
    if not _PAIRING_SECRET.match(fields["pairingSecret"]):
        raise LinkError("link_token_invalid", "the link token has an invalid pairing secret")
    try:
        expires = _parse_time(fields["expiresAt"])
    except ValueError:
        raise LinkError("link_token_invalid", "the link token has an invalid expiry") from None
    derive_urls(fields["audience"], fields["deviceId"])
    if (expires - (now or _now())).total_seconds() < -EXPIRY_SKEW_SECONDS:
        raise LinkError("link_pairing_expired", "the link token has expired; create a new one in the Harness")
    return fields


def derive_urls(audience: str, device_id: str) -> tuple[str, str]:
    """(connect URL, confirmation URL) from the token audience (L1-1)."""
    if (not isinstance(audience, str) or len(audience) > 200 or audience != audience.strip()
            or any(ord(ch) < 0x21 or ord(ch) == 0x7F for ch in audience)):
        raise LinkError("link_token_invalid", "the link token has an invalid audience")
    parts = urlsplit(audience)
    host = (parts.hostname or "").lower()
    if parts.scheme == "https":
        socket_scheme = "wss"
    elif parts.scheme == "http" and _ALLOW_LOOPBACK_HTTP and host in LOOPBACK_HOSTS:
        socket_scheme = "ws"
    else:
        raise LinkError("link_token_invalid", "the link token's server address must use https")
    if (not host or parts.query or parts.fragment or "?" in audience or "#" in audience
            or parts.username or parts.password or not parts.path.endswith(CONNECT_SUFFIX)):
        raise LinkError("link_token_invalid", "the link token has an invalid audience")
    if not _UUID4.match(device_id or ""):
        raise LinkError("link_token_invalid", "the link token has an invalid deviceId")
    base_path = parts.path[: -len(CONNECT_SUFFIX)]
    connect = urlunsplit((socket_scheme, parts.netloc, parts.path, f"deviceId={device_id}", ""))
    confirmation = urlunsplit((parts.scheme, parts.netloc, base_path + CONFIRMATION_SUFFIX, "", ""))
    return connect, confirmation


# --- L1-2/L1-3: label, keys, signature input ---------------------------------------


def _utf16_length(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def device_label(node: str | None = None, profile: str | None = None) -> str:
    """Machine name and Hermes profile, trimmed, within the spec limits (G4)."""
    if node is None:
        node = platform.node()
    if profile is None:
        try:
            from hermes_cli.profiles import get_active_profile_name  # Hermes core

            profile = get_active_profile_name()
        except Exception:  # noqa: BLE001 - a core without profiles names none
            profile = ""
    raw = f"{node or ''} · Hermes {profile or 'default'}"
    cleaned = "".join(" " if (ord(ch) < 0x20 or ord(ch) == 0x7F) else ch for ch in raw)
    label = " ".join(cleaned.split())
    while label and _utf16_length(label) > LABEL_LIMIT:
        label = label[:-1].rstrip()
    return label or "Hermes"


def json_bytes(value: dict[str, Any]) -> bytes:
    """ECMAScript JSON.stringify bytes for an object built in key order (L1-3)."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def pairing_proof_bytes(device_id: str, pairing_secret: str, label: str, spki_base64: str) -> bytes:
    return json_bytes({
        "version": 1,
        "deviceId": device_id,
        "pairingSecret": pairing_secret,
        "deviceLabel": label,
        "publicKeySpkiBase64": spki_base64,
    })


def handshake_proof_bytes(challenge: dict[str, Any]) -> bytes:
    return json_bytes({
        "version": challenge["version"],
        "handshakeId": challenge["handshakeId"],
        "connectionId": challenge["connectionId"],
        "deviceId": challenge["deviceId"],
        "audience": challenge["audience"],
        "nonce": challenge["nonce"],
        "expiresAt": challenge["expiresAt"],
    })


def new_private_seed() -> bytes:
    return secrets.token_bytes(32)


def _private_key(seed: bytes) -> Ed25519PrivateKey:
    return Ed25519PrivateKey.from_private_bytes(seed)


def public_spki_der(seed: bytes) -> bytes:
    return _private_key(seed).public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def public_spki_base64(seed: bytes) -> str:
    return base64.b64encode(public_spki_der(seed)).decode("ascii")


def fingerprint(seed: bytes) -> str:
    return hashlib.sha256(public_spki_der(seed)).hexdigest()


def sign(seed: bytes, data: bytes) -> str:
    return base64.b64encode(_private_key(seed).sign(data)).decode("ascii")


# --- L1-9/L1-10: the link record in the fenced store -------------------------------


def link_path() -> Path:
    """Next to the state file in use, inside the fenced store (L1-10)."""
    return state_path().parent / LINK_FILE


def _seed(record: dict[str, Any]) -> bytes:
    seed = base64.b64decode(str(record.get("privateKeyBase64") or ""), validate=True)
    if len(seed) != 32:
        raise ValueError("invalid link key")
    return seed


def _remember(record: dict[str, Any] | None) -> None:
    if record:
        secret_isolation.remember_values(
            str(record.get(key) or "") for key in ("privateKeyBase64", "pairingSecret")
        )


def pending_path(path: Path | None = None) -> Path:
    return (path or link_path()).parent / LINK_PENDING_FILE


_RECORD_STATES = frozenset({"active", "pending", "refused", "expired"})


def _validate_record(record: Any) -> dict[str, Any]:
    """Every field the link code reads, checked on load (OP-12 P4 r1 H6)."""
    if not isinstance(record, dict) or record.get("schema") != LINK_SCHEMA:
        raise ValueError("unsupported link record")
    state = record.get("state")
    if not isinstance(state, str) or state not in _RECORD_STATES:
        raise ValueError("link record state is invalid")
    for key in ("deviceId", "audience", "deviceLabel", "privateKeyBase64", "publicKeySpkiBase64", "fingerprintSha256"):
        if not isinstance(record.get(key), str) or not record[key]:
            raise ValueError(f"link record lacks {key}")
    if not _UUID4.match(record["deviceId"]) or not re.fullmatch(r"[0-9a-f]{64}", record["fingerprintSha256"]):
        raise ValueError("link record identity is invalid")
    _seed(record)
    if state == "pending":
        if not isinstance(record.get("pairingSecret"), str) or not _PAIRING_SECRET.match(record["pairingSecret"]):
            raise ValueError("pending link record lacks its pairing secret")
        _parse_time(str(record.get("expiresAt") or ""))
    elif "pairingSecret" in record:
        raise ValueError("only a pending link record holds a pairing secret")
    return record


def read_link(path: Path | None = None) -> dict[str, Any] | None:
    """The link record, or None when not linked. Raises on an unreadable record."""
    target = path or link_path()
    if not target.exists() and not target.is_symlink():
        return None
    if target.is_symlink() or not target.is_file() or target.parent.is_symlink():
        raise ValueError("the link record must be a regular file in a regular directory")
    assert_state_directory(target.parent)
    os.chmod(target, 0o600)
    record = json.loads(target.read_text(encoding="utf-8"))
    if isinstance(record, dict):
        _remember(record)
    return _validate_record(record)


def record_digest(record: dict[str, Any] | None) -> str:
    if record is None:
        return ""
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode("utf-8")).hexdigest()


def write_link(record: dict[str, Any], path: Path | None = None) -> Path:
    """Write the record 0600 inside the fenced store, atomically (L1-10)."""
    target = path or link_path()
    directory = assert_state_directory(target.parent)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(directory, 0o700)
    _remember(record)
    fd, temporary = tempfile.mkstemp(prefix=".link-", suffix=".tmp", dir=directory)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise
    return target


def delete_link(path: Path | None = None) -> dict[str, Any] | None:
    """Delete the local key, record and any pending re-link (L1-9); the deleted record's public view."""
    target = path or link_path()
    pending = pending_path(target)
    pending_view: dict[str, Any] = {}
    if pending.exists() or pending.is_symlink():
        try:
            pending_view = public_view(read_link(pending) or {})
        except Exception:  # noqa: BLE001 - OP-12 P4 r3 finding 3: no shape blocks unlink
            pending_view = {}
        if pending.is_dir() and not pending.is_symlink():
            # OP-12 P5 review item 3: a hand-made directory in its place does
            # not block unlink. An empty one is removed; one with content is
            # left as it is (never a recursive delete) and reported.
            try:
                pending.rmdir()
            except OSError:
                pending_view = {"pendingNotRemoved": True}
        else:
            pending.unlink()
        pending_view["pendingOnly"] = True
    try:
        record = read_link(target)
    except Exception:  # noqa: BLE001 - an unreadable record is still deleted
        record = {}
    if not target.exists() and not target.is_symlink():
        # Only a pending (never confirmed) link was there (r2 item 10).
        return pending_view or None
    target.unlink()
    view = public_view(record or {})
    if pending_view.get("pendingNotRemoved"):
        view["pendingNotRemoved"] = True
    return view


# OP-12 P4 r3 finding 5: the digest of an unreadable pending file reads at
# most this many bytes and never follows a symlink.
BAD_FILE_DIGEST_BYTES = 64 * 1024


def _bad_file_digest(path: Path) -> str:
    """A change marker for an unreadable file: bounded, no symlink followed."""
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    except OSError as error:
        return hashlib.sha256(f"unopenable:{error.errno}".encode()).hexdigest()
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            return hashlib.sha256(f"irregular:{stat.S_IFMT(info.st_mode)}".encode()).hexdigest()
        digest = hashlib.sha256(f"{info.st_size}:".encode())
        digest.update(os.read(fd, BAD_FILE_DIGEST_BYTES))
        return digest.hexdigest()
    except OSError as error:
        return hashlib.sha256(f"unreadable:{error.errno}".encode()).hexdigest()
    finally:
        os.close(fd)


def public_view(record: dict[str, Any]) -> dict[str, Any]:
    """The record without its secrets, for display."""
    return {key: record.get(key) for key in (
        "state", "deviceId", "deviceLabel", "fingerprintSha256", "audience", "createdAt", "confirmedAt", "expiresAt",
    ) if record.get(key)}


@contextlib.contextmanager
def link_lock(*, blocking: bool = True, path: Path | None = None) -> Iterator[bool]:
    """Serialize confirmation between the CLI and the gateway."""
    target = (path or link_path()).parent / LINK_LOCK_FILE
    directory = assert_state_directory(target.parent)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(target, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def pending_record(token: dict[str, str], *, label: str | None = None, seed: bytes | None = None) -> dict[str, Any]:
    seed = seed or new_private_seed()
    return {
        "schema": LINK_SCHEMA,
        "state": "pending",
        "deviceId": token["deviceId"],
        "audience": token["audience"],
        "deviceLabel": label if label is not None else device_label(),
        "privateKeyBase64": base64.b64encode(seed).decode("ascii"),
        "publicKeySpkiBase64": public_spki_base64(seed),
        "fingerprintSha256": fingerprint(seed),
        "pairingSecret": token["pairingSecret"],
        "expiresAt": token["expiresAt"],
        "createdAt": _iso(),
    }


# --- L1-2: pairing confirmation ------------------------------------------------------


class ConfirmationRefused(Exception):
    """404: the token is spent, expired or invalid; never retried."""


class ConfirmationRetry(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def confirmation_body(record: dict[str, Any]) -> bytes:
    seed = _seed(record)
    proof = pairing_proof_bytes(
        record["deviceId"], record["pairingSecret"], record["deviceLabel"], record["publicKeySpkiBase64"],
    )
    body = json_bytes({
        "version": 1,
        "deviceId": record["deviceId"],
        "pairingSecret": record["pairingSecret"],
        "deviceLabel": record["deviceLabel"],
        "publicKeySpkiBase64": record["publicKeySpkiBase64"],
        "signatureBase64": sign(seed, proof),
    })
    if len(body) > CONFIRMATION_BODY_LIMIT:
        raise LinkError("link_pairing_refused", "the pairing confirmation would exceed 4096 bytes")
    return body


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """OP-12 P4 r1 H3: a redirect is answered as the 3xx it is, never followed."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401, ANN001
        return None


def _no_redirect_opener() -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(_NoRedirect())


def _no_proxy_opener() -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())


def _connect_without_redirects():
    """websockets' connect, refusing every redirect (OP-12 P4 r1 H3)."""
    from websockets.asyncio.client import connect

    class _Connect(connect):
        def process_redirect(self, exc: Exception) -> Exception:
            return exc

    return _Connect


def send_confirmation(record: dict[str, Any], *, timeout: float = 15.0,
                      opener: urllib.request.OpenerDirector | None = None) -> dict[str, Any]:
    """POST the signed confirmation once. 201 → the server's view."""
    _, url = derive_urls(record["audience"], record["deviceId"])
    request = urllib.request.Request(
        url, data=confirmation_body(record), method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with (opener or _no_redirect_opener()).open(request, timeout=timeout) as response:
            status = response.status
            payload = response.read(65_536)
    except urllib.error.HTTPError as error:
        status = error.code
        payload = b""
        with contextlib.suppress(Exception):
            payload = error.read(65_536)
    except (urllib.error.URLError, OSError, TimeoutError):
        raise ConfirmationRetry("link_connection_lost") from None
    if status == 404:
        raise ConfirmationRefused()
    if 300 <= status < 400:
        raise ConfirmationRetry("link_route_refused")
    if status != 201:
        dispatcher = ""
        with contextlib.suppress(Exception):
            dispatcher = str(json.loads(payload.decode("utf-8")).get("error") or "")
        if status in {400, 401, 403} and dispatcher in {"device_id_required", "bad_path", "no_route", "sign_in_required"}:
            raise ConfirmationRetry("link_route_refused")
        raise ConfirmationRetry("link_pairing_unavailable")
    try:
        view = json.loads(payload.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise ConfirmationRetry("link_pairing_unavailable") from None
    if (not isinstance(view, dict) or view.get("deviceId") != record["deviceId"] or view.get("state") != "active"
            or view.get("fingerprintSha256") != record["fingerprintSha256"]):
        raise ConfirmationRefused()
    return view


def confirm_pending(*, now: datetime | None = None, path: Path | None = None,
                    sender: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
                    blocking: bool = True) -> tuple[str, str]:
    """One confirmation attempt for the pending (re-)link: (state, code).

    state is ``active``, ``pending`` (retry later), ``refused`` or ``expired``;
    ``none`` when nothing is pending. The pending record lives in its own
    file; link.json is replaced only after the 201, so a failed re-link
    leaves a working link as it was (L1-9).
    """
    target = path or link_path()
    pending_file = pending_path(target)
    with link_lock(blocking=blocking, path=target) as locked:
        if not locked:
            return "pending", "link_pending"
        record = read_link(pending_file)
        if record is None:
            return "none", ""
        expires = _parse_time(str(record["expiresAt"]))
        if (now or _now()) >= expires:
            _retire(record, "expired", target)
            return "expired", "link_pairing_expired"
        try:
            view = (sender or send_confirmation)(record)
        except ConfirmationRefused:
            _retire(record, "refused", target)
            return "refused", "link_pairing_refused"
        except ConfirmationRetry as retry:
            return "pending", retry.code
        active = {key: value for key, value in record.items() if key not in {"pairingSecret", "expiresAt"}}
        active["state"] = "active"
        active["confirmedAt"] = str(view.get("confirmedAt") or _iso())
        write_link(active, target)
        pending_file.unlink()
        return "active", ""


def discard_pending(path: Path | None = None) -> None:
    """Drop a pending re-link (its pairing secret leaves the store)."""
    target = path or link_path()
    with link_lock(path=target):
        pending_file = pending_path(target)
        if pending_file.exists() or pending_file.is_symlink():
            pending_file.unlink()


def _retire(record: dict[str, Any], state: str, target: Path) -> None:
    # The spent pairing secret leaves the store. A working link stays as it
    # was; without one, the refused or expired first link is recorded.
    pending_path(target).unlink()
    try:
        current = read_link(target)
    except (ValueError, OSError, json.JSONDecodeError):
        # An unreadable link.json is left as it is (OP-12 P4 r2 item 9).
        return
    if current is not None and current.get("state") == "active":
        return
    retired = {key: value for key, value in record.items() if key not in {"pairingSecret", "expiresAt"}}
    retired["state"] = state
    write_link(retired, target)


# --- L1-7: the local Gateway (G23) ---------------------------------------------------


def _loopback_url_host(host: str) -> str:
    return f"[{host}]" if ":" in host else host


def resolve_local_gateway(configured_url: str = "", *,
                          opener: urllib.request.OpenerDirector | None = None) -> tuple[str, int, str]:
    """(host, port, session token) of the loopback dashboard Gateway (G23).

    1. The core's host rendezvous record (cores that have one): a loopback
       ``serve`` or ``desktop-serve`` owner and its 0600 token file.
    2. The configured dashboard URL (default http://127.0.0.1:9119): the token
       its loopback page injects.
    """
    try:
        from gateway import host_rendezvous as rendezvous  # Hermes core, newer cores only
    except Exception:  # noqa: BLE001 - absent on older cores
        rendezvous = None
    if rendezvous is not None:
        for role in ("ROLE_SERVE", "ROLE_DESKTOP_SERVE"):
            try:
                name = getattr(rendezvous, role)
                record = rendezvous.read_record(name)
                if record is None or not record.port or str(record.host).lower() not in LOOPBACK_HOSTS:
                    continue
                consistent = getattr(rendezvous, "record_token_is_consistent", None)
                if consistent is not None and not consistent(record):
                    continue
                token = rendezvous.read_token(name)
            except Exception:  # noqa: BLE001 - a broken record is not a dashboard
                continue
            if token and _LOCAL_TOKEN.match(token):
                secret_isolation.remember_values([token])
                return str(record.host), int(record.port), token
    url = (configured_url or DEFAULT_LOCAL_GATEWAY_URL).strip()
    try:
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
    except ValueError:
        # OP-12 P5 review note 4: a malformed address (``http://[::1``) is a
        # coded refusal, as an unusable dashboard URL.
        raise LocalGatewayError("local_gateway_auth_unavailable") from None
    if parts.scheme != "http" or host not in LOOPBACK_HOSTS or parts.username is not None or parts.password is not None:
        # Contract L1-7: only a loopback dashboard; a gated one has no
        # non-interactive credential. No userinfo (OP-12 P4 r2 item 8).
        raise LocalGatewayError("local_gateway_auth_unavailable")
    try:
        port = parts.port or 80
    except ValueError:
        # OP-12 P4 r3 finding 7: a port out of range is a coded refusal.
        raise LocalGatewayError("local_gateway_auth_unavailable") from None
    request = urllib.request.Request(urlunsplit(("http", f"{_loopback_url_host(host)}:{port}", "/", "", "")), method="GET")
    try:
        with (opener or _no_proxy_opener()).open(request, timeout=3.0) as response:
            page = response.read(4 * 1024 * 1024).decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        # A redirect is not followed (H3): the dashboard refused this request.
        code = "local_gateway_refused" if 300 <= error.code < 400 else "local_gateway_auth_unavailable"
        raise LocalGatewayError(code) from None
    except (urllib.error.URLError, OSError, TimeoutError):
        raise LocalGatewayError("local_gateway_unavailable") from None
    match = _SESSION_TOKEN_JS.search(page)
    if match is None:
        # A gated dashboard withholds its token: no non-interactive credential.
        raise LocalGatewayError("local_gateway_auth_unavailable")
    token = match.group(1)
    secret_isolation.remember_values([token])
    return host, port, token


async def open_local_gateway(configured_url: str, max_size: int,
                             resolver: Callable[[str], tuple[str, int, str]] = resolve_local_gateway):
    """One new Gateway WebSocket for one channel (L1-6, L1-7)."""
    from websockets.exceptions import InvalidStatus, InvalidHandshake

    connect = _connect_without_redirects()
    host, port, token = await asyncio.to_thread(resolver, configured_url)
    url = f"ws://{_loopback_url_host(host)}:{port}/api/ws?token={quote(token, safe='')}"
    try:
        return await connect(
            url, origin=None, compression=None, proxy=None, ping_interval=None, ping_timeout=None,
            max_size=max_size, open_timeout=None, close_timeout=2, user_agent_header=None,
            logger=_socket_logger,
        )
    except InvalidStatus:
        # The dashboard refuses before accept (4401 bad token, 4403 Host,
        # Origin or chat disabled), which reaches a client as HTTP 403.
        raise LocalGatewayError("local_gateway_refused") from None
    except (InvalidHandshake, OSError, TimeoutError):
        raise LocalGatewayError("local_gateway_unavailable") from None
    finally:
        del token, url


# --- L1-5/L1-8: limits and the close table -------------------------------------------


def read_limits(complete: dict[str, Any]) -> tuple[dict[str, int], str]:
    """Limits from authentication-complete, each falling back on its own (L1-5)."""
    advertised = complete.get("limits") if isinstance(complete, dict) else None
    limits = dict(FALLBACK_LIMITS)
    used = False
    if isinstance(advertised, dict):
        for name, (low, high) in LIMIT_RANGES.items():
            value = advertised.get(name)
            if type(value) is int and low <= value <= high:
                limits[name] = value
                used = True
    return limits, ("advertised" if used else "fallback")


def close_outcome(code: int | None, reason: str = "") -> tuple[str, str]:
    """The fixed close table of contract L1-8: (status code, action)."""
    if code == 1008:
        if reason in {"revoked", "authentication-failed"}:
            return "link_revoked", STOP
        if reason == "replaced":
            return "link_replaced", STOP
        return "link_protocol_error", RECONNECT
    if code == 1009:
        return "link_frame_too_large", RECONNECT
    return "link_connection_lost", RECONNECT


def backoff_delay(attempt: int, rng: Callable[[float, float], float] = random.uniform) -> float:
    """Contract L1-8: the P2c-3a schedule, in [0.8 d, d] with d ≤ 15 s."""
    return reconnect_delay(attempt, rng=rng)


# --- L1-4 … L1-6: the gateway-side link service -----------------------------------------


def _frame(kind: str, channel_id: str, data: str | None = None) -> dict[str, Any]:
    frame: dict[str, Any] = {"version": 1, "type": kind, "channelId": channel_id}
    if data is not None:
        frame["data"] = data
    return frame


class _ProtocolError(Exception):
    pass


class _Channel:
    def __init__(self, bridge: "_Bridge", channel_id: str) -> None:
        self.bridge = bridge
        self.id = channel_id
        self.local: Any = None
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.queued_bytes = 0
        self.ended = False
        self.task: asyncio.Task | None = None

    def enqueue(self, data: str) -> bool:
        size = len(data.encode("utf-8"))
        if self.queued_bytes + size > 4 * self.bridge.limits["maxDataBytes"]:
            return False
        self.queued_bytes += size
        self.queue.put_nowait(data)
        return True

    async def run(self) -> None:
        limits = self.bridge.limits
        try:
            try:
                self.local = await asyncio.wait_for(
                    self.bridge.local_opener(limits["maxDataBytes"]), limits["openTimeoutMs"] / 1000,
                )
            except LocalGatewayError as error:
                self.bridge.channel_error(error.code)
                await self.bridge.end(self, "hermes-open-failed")
                return
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - timeout, network or an unexpected opener failure
                self.bridge.channel_error("local_gateway_unavailable")
                await self.bridge.end(self, "hermes-open-failed")
                return
            if self.ended:
                return
            # L1-6: hermes-opened as soon as the local Gateway is open; the
            # greeting (gateway.ready) is forwarded like any other message.
            await self.bridge.send(_frame("hermes-opened", self.id))
            writer = asyncio.create_task(self._write())
            try:
                await self._read()
            finally:
                writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)
            await self.bridge.end(self, "hermes-close")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - one channel never ends the link
            await self.bridge.end(self, "hermes-close")
        finally:
            await self.close_local()

    async def _read(self) -> None:
        from websockets.exceptions import ConnectionClosed

        try:
            async for message in self.local:
                if self.ended:
                    return
                if not isinstance(message, str):
                    self.bridge.channel_error("channel_binary")
                    return
                if len(message.encode("utf-8")) > self.bridge.limits["maxDataBytes"]:
                    self.bridge.channel_error("channel_data_too_large")
                    return
                if not await self.bridge.send(_frame("hermes-frame", self.id, message), channel=True):
                    self.bridge.channel_error("channel_data_too_large")
                    return
        except ConnectionClosed as closed:
            if closed.rcvd is not None and closed.rcvd.code == 1009 or closed.sent is not None and closed.sent.code == 1009:
                self.bridge.channel_error("channel_data_too_large")

    async def _write(self) -> None:
        while True:
            data = await self.queue.get()
            self.queued_bytes -= len(data.encode("utf-8"))
            await self.local.send(data)

    async def close_local(self) -> None:
        local, self.local = self.local, None
        if local is not None:
            with contextlib.suppress(Exception):
                await local.close()


class _Bridge:
    """The channels of one authenticated device socket (L1-6)."""

    def __init__(self, socket: Any, limits: dict[str, int],
                 local_opener: Callable[[int], Awaitable[Any]], on_change: Callable[[], None],
                 on_channel_error: Callable[[str], None]) -> None:
        self.socket = socket
        self.limits = limits
        self.local_opener = local_opener
        self.channels: dict[str, _Channel] = {}
        self._on_change = on_change
        self._on_channel_error = on_channel_error

    def channel_error(self, code: str) -> None:
        self._on_channel_error(code)

    async def send(self, frame: dict[str, Any], *, channel: bool = False) -> bool:
        text = json.dumps(frame, separators=(",", ":"), ensure_ascii=False)
        if len(text.encode("utf-8")) > self.limits["maxFrameBytes"]:
            if channel:
                return False
            raise _ProtocolError("oversize control frame")
        await self.socket.send(text)
        return True

    async def end(self, channel: _Channel, kind: str) -> None:
        """End a channel from this side, telling the server once."""
        if channel.ended:
            return
        channel.ended = True
        if self.channels.get(channel.id) is channel:
            del self.channels[channel.id]
            self._on_change()
        with contextlib.suppress(Exception):
            await self.send(_frame(kind, channel.id))

    async def handle(self, message: Any) -> None:
        if not isinstance(message, str):
            raise _ProtocolError("binary frame")
        try:
            frame = json.loads(message)
        except ValueError:
            raise _ProtocolError("not JSON") from None
        if not isinstance(frame, dict):
            raise _ProtocolError("not an object")
        version, kind, channel_id = frame.get("version"), frame.get("type"), frame.get("channelId")
        if type(version) is not int or version != 1:
            raise _ProtocolError("version")
        if kind not in {"hermes-open", "hermes-frame", "hermes-close"}:
            raise _ProtocolError("type")
        if not isinstance(channel_id, str) or not _UUID4.match(channel_id):
            raise _ProtocolError("channelId")
        channel = self.channels.get(channel_id)
        if kind == "hermes-open":
            if channel is not None:
                # G9: a duplicate open ends that channel.
                self.channel_error("channel_duplicate")
                await self.end(channel, "hermes-close")
                if channel.task is not None:
                    channel.task.cancel()
                return
            if len(self.channels) >= self.limits["maxChannels"]:
                self.channel_error("channel_cap")
                await self.send(_frame("hermes-open-failed", channel_id))
                return
            channel = _Channel(self, channel_id)
            self.channels[channel_id] = channel
            self._on_change()
            channel.task = asyncio.create_task(channel.run())
            return
        if kind == "hermes-frame":
            data = frame.get("data")
            if not isinstance(data, str):
                raise _ProtocolError("data")
            if channel is None:
                # G9: an unknown channel is ended.
                self.channel_error("channel_unknown")
                await self.send(_frame("hermes-close", channel_id))
                return
            if len(data.encode("utf-8")) > self.limits["maxDataBytes"]:
                self.channel_error("channel_data_too_large")
                await self.end(channel, "hermes-close")
                return
            if not channel.enqueue(data):
                self.channel_error("channel_buffer_overflow")
                await self.end(channel, "hermes-close")
            return
        # hermes-close: the server ended the channel; no echo.
        if channel is not None:
            channel.ended = True
            del self.channels[channel_id]
            self._on_change()
            if channel.task is not None:
                channel.task.cancel()
            await channel.close_local()

    async def close_all(self) -> None:
        channels = list(self.channels.values())
        self.channels.clear()
        for channel in channels:
            channel.ended = True
            if channel.task is not None:
                channel.task.cancel()
        await asyncio.gather(*(channel.task for channel in channels if channel.task is not None),
                             return_exceptions=True)
        for channel in channels:
            await channel.close_local()
        self._on_change()


class LinkService:
    """One outbound device socket for this profile while it is linked (L1-4)."""

    def __init__(self, *, plugin_version: str, status: Any = None, local_gateway_url: str = "",
                 record_path: Path | None = None,
                 local_opener: Callable[[int], Awaitable[Any]] | None = None,
                 sleep: Callable[[float], Awaitable[None]] | None = None,
                 rng: Callable[[float, float], float] = random.uniform) -> None:
        self.plugin_version = plugin_version
        self.status = status
        self.local_gateway_url = local_gateway_url
        self.record_path = record_path
        self.local_opener = local_opener or (lambda max_size: open_local_gateway(self.local_gateway_url, max_size))
        self._sleep = sleep or asyncio.sleep
        self._rng = rng
        self._attempt = 0
        self._link: dict[str, Any] = {
            "state": "not_linked", "code": "", "deviceId": "", "channels": 0, "limits": "fallback",
            "lastConnectedAt": None, "lastChannelErrorCode": "",
        }
        self.bridge: _Bridge | None = None
        self.delays: list[float] = []

    # status (L1-11): codes only
    def _set(self, **fields: Any) -> None:
        changed = any(self._link.get(key) != value for key, value in fields.items())
        self._link.update(fields)
        if changed and self.status is not None:
            try:
                self.status.set_link(dict(self._link))
            except Exception:  # noqa: BLE001 - status is diagnostics only
                logger.debug("Room link status update failed")

    def snapshot(self) -> dict[str, Any]:
        return dict(self._link)

    def _read(self) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
        """(link record, pending record, pending digest). A corrupt or invalid
        pending file never hides a valid link.json (OP-12 P4 r2 item 2): it
        reads as None with its own digest, so a change is still noticed."""
        record = read_link(self.record_path)
        pending_file = pending_path(self.record_path)
        try:
            pending = read_link(pending_file)
            return record, pending, record_digest(pending)
        except Exception:  # noqa: BLE001 - OP-12 P4 r3 finding 3: any shape
            return record, None, "bad:" + _bad_file_digest(pending_file)

    async def _current(self) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
        try:
            record, pending, pending_digest = await asyncio.to_thread(self._read)
        except (ValueError, OSError, json.JSONDecodeError):
            return None, None, "unreadable"
        return record, pending, f"{record_digest(record)}:{pending_digest}"

    async def _link_digest(self) -> str:
        """The active record alone: a pending re-link does not touch the socket."""
        try:
            return record_digest(await asyncio.to_thread(read_link, self.record_path))
        except (ValueError, OSError, json.JSONDecodeError):
            return "unreadable"

    async def _wait_for_change(self, digest: str, seconds: float | None = None) -> None:
        """Sleep until the records change, or for ``seconds`` when given."""
        waited = 0.0
        while seconds is None or waited < seconds:
            step = RECORD_POLL_SECONDS if seconds is None else min(RECORD_POLL_SECONDS, seconds - waited)
            await self._sleep(step)
            waited += step
            if (await self._current())[2] != digest:
                return

    async def _backoff(self, digest: str) -> None:
        delay = backoff_delay(self._attempt, self._rng)
        self._attempt += 1
        self.delays.append(delay)
        await self._wait_for_change(digest, delay)

    async def run(self) -> None:
        while True:
            try:
                await self._step()
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - OP-12 P4 r1 H6: never end silently
                logger.warning("Room link service failed (%s); retrying", type(error).__name__)
                self._set(state="reconnecting", code="link_service_error", channels=0)
                delay = backoff_delay(self._attempt, self._rng)
                self._attempt += 1
                self.delays.append(delay)
                await self._sleep(delay)

    async def _step(self) -> None:
        record, pending, digest = await self._current()
        if digest == "unreadable":
            self._set(state="stopped", code="link_store_unreadable", channels=0)
            await self._wait_for_change(digest)
            return
        if record is not None and record.get("state") == "active":
            # A pending re-link is confirmed by the CLI; this link serves on.
            self._set(deviceId=str(record["deviceId"]))
            await self._serve_record(record)
            return
        if pending is None and ":bad:" in digest:
            # No working link, and the pending first link cannot be read.
            self._set(state="stopped", code="link_store_unreadable", channels=0)
            await self._wait_for_change(digest)
            return
        if pending is not None:
            # A first link the CLI could not confirm in time: retried here
            # until its token expires (G6).
            self._set(state="pending", code="link_pending", deviceId=str(pending["deviceId"]))
            result, code = await asyncio.to_thread(
                confirm_pending, path=self.record_path, blocking=False,
            )
            if result == "active":
                self._attempt = 0
                return
            if result == "pending":
                self._set(code=code or "link_pending")
                await self._backoff((await self._current())[2])
            return
        if record is None:
            self._attempt = 0
            self._set(state="not_linked", code="", deviceId="", channels=0)
            await self._wait_for_change(digest)
            return
        self._set(state="stopped", deviceId=str(record["deviceId"]),
                  code="link_pairing_expired" if record["state"] == "expired" else "link_pairing_refused")
        await self._wait_for_change(digest)

    async def _serve_record(self, record: dict[str, Any]) -> None:
        digest = record_digest(record)
        code, action = await self.connect_once(record, digest)
        if action == STOP:
            self._set(state="stopped", code=code, channels=0)
            logger.warning("Room link stopped: %s", code)
            await self._wait_for_link_change(digest)
            self._attempt = 0
            return
        if code == "link_record_changed":
            self._attempt = 0
            return
        self._set(state="reconnecting", code=code, channels=0)
        delay = backoff_delay(self._attempt, self._rng)
        self._attempt += 1
        self.delays.append(delay)
        await self._wait_for_link_change(digest, delay)

    async def _wait_for_link_change(self, digest: str, seconds: float | None = None) -> None:
        waited = 0.0
        while seconds is None or waited < seconds:
            step = RECORD_POLL_SECONDS if seconds is None else min(RECORD_POLL_SECONDS, seconds - waited)
            await self._sleep(step)
            waited += step
            if await self._link_digest() != digest:
                return

    async def connect_once(self, record: dict[str, Any], digest: str) -> tuple[str, str]:
        """One device socket, from upgrade to close: (status code, action)."""
        from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

        connect = _connect_without_redirects()
        self._set(state="connecting")
        try:
            url, _ = derive_urls(record["audience"], record["deviceId"])
            seed = _seed(record)
        except (LinkError, ValueError, KeyError):
            return "link_store_unreadable", STOP
        try:
            socket = await connect(
                url, compression=None, ping_interval=None, ping_timeout=None,
                max_size=CLIENT_MAX_MESSAGE_BYTES, open_timeout=HANDSHAKE_TIMEOUT_SECONDS, close_timeout=5,
                user_agent_header=f"synthetic-sociality-room-hermes/{self.plugin_version}",
                logger=_socket_logger,
            )
        except InvalidStatus as error:
            status = getattr(getattr(error, "response", None), "status_code", 0)
            refused = status in {400, 401, 403, 404} or 300 <= status < 400  # a redirect is never followed
            return ("link_route_refused" if refused else "link_connection_lost"), RECONNECT
        except (InvalidHandshake, OSError, TimeoutError, asyncio.TimeoutError):
            return "link_connection_lost", RECONNECT
        try:
            return await self._session(socket, record, seed, digest)
        except ConnectionClosed as closed:
            frame = closed.rcvd
            return close_outcome(frame.code if frame is not None else 1006, frame.reason if frame is not None else "")
        finally:
            if self.bridge is not None:
                await self.bridge.close_all()
                self.bridge = None
            with contextlib.suppress(Exception):
                await socket.close()
            self._set(channels=0)

    async def _session(self, socket: Any, record: dict[str, Any], seed: bytes, digest: str) -> tuple[str, str]:
        # L1-4: one proof per socket, echoing the challenge.
        try:
            challenge = json.loads(await asyncio.wait_for(socket.recv(), HANDSHAKE_TIMEOUT_SECONDS))
        except asyncio.TimeoutError:
            return "link_handshake_timeout", RECONNECT
        except (ValueError, TypeError):
            await socket.close(1008, "protocol-error")
            return "link_protocol_error", RECONNECT
        fields = ("handshakeId", "connectionId", "deviceId", "audience", "nonce", "expiresAt")
        if (not isinstance(challenge, dict) or challenge.get("type") != "authentication-challenge"
                or type(challenge.get("version")) is not int or challenge.get("version") != 1
                or not all(isinstance(challenge.get(name), str) for name in fields)):
            await socket.close(1008, "protocol-error")
            return "link_protocol_error", RECONNECT
        if challenge["audience"] != record["audience"] or challenge["deviceId"] != record["deviceId"]:
            await socket.close(1000)
            return "link_audience_mismatch", RECONNECT
        proof: dict[str, Any] = {"version": 1}
        for name in fields:
            proof[name] = challenge[name]
        proof["signatureBase64"] = sign(seed, handshake_proof_bytes(challenge))
        proof["pluginVersion"] = self.plugin_version
        await socket.send(json.dumps(proof, separators=(",", ":"), ensure_ascii=False))
        try:
            complete = json.loads(await asyncio.wait_for(socket.recv(), HANDSHAKE_TIMEOUT_SECONDS))
        except asyncio.TimeoutError:
            return "link_handshake_timeout", RECONNECT
        except (ValueError, TypeError):
            await socket.close(1008, "protocol-error")
            return "link_protocol_error", RECONNECT
        if (not isinstance(complete, dict) or complete.get("type") != "authentication-complete"
                or type(complete.get("version")) is not int or complete.get("version") != 1):
            await socket.close(1008, "protocol-error")
            return "link_protocol_error", RECONNECT
        limits, source = read_limits(complete)
        self._attempt = 0
        self._set(state="connected", code="", limits=source, lastConnectedAt=_iso(), channels=0)
        self.bridge = _Bridge(
            socket, limits, self.local_opener,
            on_change=lambda: self._set(channels=len(self.bridge.channels) if self.bridge else 0),
            on_channel_error=lambda code: self._set(lastChannelErrorCode=code),
        )
        return await self._serve(socket, limits, digest)

    async def _serve(self, socket: Any, limits: dict[str, int], digest: str) -> tuple[str, str]:
        from websockets.exceptions import ConnectionClosed

        keepalive = limits["keepaliveMs"] / 1000
        last_inbound = [time.monotonic()]
        outcome: list[tuple[str, str]] = []

        async def receive() -> None:
            while True:
                message = await socket.recv()
                last_inbound[0] = time.monotonic()
                try:
                    await self.bridge.handle(message)
                except _ProtocolError:
                    # G9: an unknown type or version closes the socket.
                    outcome.append(("link_protocol_error", RECONNECT))
                    await socket.close(1008, "protocol-error")
                    return

        async def keep_alive() -> None:
            # L1-5: the device's own ping; idle after twice the keepalive.
            while True:
                await asyncio.sleep(keepalive)
                if time.monotonic() - last_inbound[0] > 2 * keepalive:
                    # A silent peer gets no close handshake: drop the socket.
                    outcome.append(("link_keepalive_timeout", RECONNECT))
                    socket.transport.abort()
                    return
                try:
                    waiter = await socket.ping()
                    await asyncio.wait_for(waiter, keepalive)
                    last_inbound[0] = time.monotonic()
                except (asyncio.TimeoutError, ConnectionClosed):
                    continue

        async def watch_record() -> None:
            while True:
                await asyncio.sleep(RECORD_POLL_SECONDS)
                if await self._link_digest() != digest:
                    outcome.append(("link_record_changed", RECONNECT))
                    await socket.close(1000)
                    return

        tasks = [asyncio.create_task(receive()), asyncio.create_task(keep_alive()), asyncio.create_task(watch_record())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        if outcome:
            return outcome[0]
        for task in done:
            error = task.exception()
            if isinstance(error, ConnectionClosed):
                frame = error.rcvd
                return close_outcome(frame.code if frame is not None else 1006, frame.reason if frame is not None else "")
            if error is not None:
                logger.warning("Room link socket failed (%s)", type(error).__name__)
        return "link_connection_lost", RECONNECT

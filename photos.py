"""P9 photo hand-over: verified Room bytes to the host's own media store."""
from __future__ import annotations

import hashlib
import re
from typing import Any, Callable

from .documents import DOCUMENT_CONTEXT_HEADER, DOCUMENT_CONTEXT_FOOTER, discussion_documents, document_key, effective_given

PHOTO_TYPES = frozenset({"image/png", "image/jpeg", "image/webp"})
PHOTO_MAX_BYTES = 1_900_000
PHOTO_TURN_BYTES = 6_000_000
PHOTO_TURN_COUNT = 4


def source_photos(event: dict, events: list, cycle_source: dict | None = None) -> list[dict]:
    """The P5 source attachments, with P7d-1 filtering before photo selection."""
    source = event if event.get("type") == "message.posted" else None
    source_id = (event.get("payload") or {}).get("sourceEventId")
    if source_id:
        source = next((item for item in [event, *(events or [])] if item.get("id") == source_id), None)
    entries = []
    for item in [cycle_source, source]:
        if isinstance(item, dict):
            entries.extend((item.get("payload") or {}).get("attachments") or [])
    return discussion_documents({"activeTopic": {"documents": entries}}) or []


def effective_photos(memory: Any, epoch: str, session: str) -> list[str]:
    """P9 uses the same reset and key rules as P7d."""
    if not isinstance(memory, dict):
        return []
    return effective_given({**memory, "given": memory.get("photos", [])}, epoch, session)


def safe_photo_name(name: Any, mime: str) -> str:
    """A bounded ASCII basename; the verified MIME type owns the extension."""
    stem = str(name or "photo").replace("\\", "/").rsplit("/", 1)[-1].rsplit(".", 1)[0]
    stem = re.sub(r"[^A-Za-z0-9_-]", "_", stem)[:80] or "photo"
    return stem + {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp"}[mime]


def host_image_store() -> Callable | None:
    """Use only Hermes' own image cache; an older host stays text-only."""
    try:
        from gateway.platforms.base import cache_image_from_bytes
    except ImportError:
        return None
    # Hermes sniffs the bytes and generates its own cache basename.
    return lambda data, mime, name: cache_image_from_bytes(data)


def photo_context(entries: list[dict], given: list[str], membership: str,
                  artifact: Callable, download: Callable, store: Callable | None) -> tuple[str, list[str], list[dict]]:
    """Verify and store unseen photos newest first. No binary cache or prompt bytes.

    Returns status text, successful version keys, and host media facts. Callback
    failures are rendered with fixed reasons, never exception text or paths.
    """
    photos = [(index, entry) for index, entry in enumerate(entries) if entry.get("mimeType") in PHOTO_TYPES]
    photos.sort(key=lambda pair: (pair[1].get("sourceSeq") or 0, -pair[0]), reverse=True)
    offered, media, lines = [], [], []
    used = 0
    for _, entry in photos:
        aid, vid = entry["artifactId"], entry["versionId"]
        key = document_key(aid, vid)
        mime = entry["mimeType"]
        reason = ""
        if key in given:
            reason = "given earlier in this discussion; reference only"
        elif entry.get("classification") not in ("room_shared", "restricted"):
            reason = "sharing"
        elif type(entry.get("size")) is int and entry["size"] > PHOTO_MAX_BYTES:
            reason = "size cap"
        elif type(entry.get("size")) is not int or entry["size"] < 0:
            reason = "mismatch"
        elif len(media) >= PHOTO_TURN_COUNT or used + entry["size"] > PHOTO_TURN_BYTES:
            reason = "per-turn cap"
        elif store is None:
            reason = "host has no image path"
        else:
            try:
                # Current metadata can revoke an old message manifest. Never
                # use cached document text as authorization to fetch bytes.
                current = artifact(aid)
                visibility = current.get("visibility")
                audience = current.get("allowedMembershipIds") or []
                if visibility not in ("room_shared", "restricted") or (visibility == "restricted" and membership not in audience):
                    reason = "sharing"
                elif entry.get("classification") == "restricted" and membership not in (entry.get("allowedMembershipIds") or audience):
                    reason = "sharing"
                else:
                    data, content_type = download(aid, vid, mime)
                    if len(data) > PHOTO_MAX_BYTES:
                        reason = "size cap"
                    elif (len(data) != entry["size"] or hashlib.sha256(data).hexdigest() != entry.get("sha256")
                          or content_type.split(";", 1)[0].strip().lower() != mime):
                        reason = "mismatch"
                    else:
                        try:
                            path = store(data, mime, safe_photo_name(entry.get("name"), mime))
                        except Exception:
                            path = None
                        if not isinstance(path, str) or not path:
                            reason = "host has no image path"
                        else:
                            media.append({"path": path, "contentType": mime, "kind": "image"})
                            offered.append(key)
                            used += len(data)
            except Exception as error:
                reason = "size cap" if getattr(error, "code", "") == "photo_size_cap" else "fetch error"
        name = " ".join(str(entry.get("name") or "photo").split())[:500]
        lines.append(f"Document: {name} · artifact/version {aid} / {vid}\nPhoto given to the model: "
                     + (f"no: {reason}" if reason else "yes"))
    return "\n\n".join(lines), offered, media


def append_photo_context(context: str, statuses: str) -> str:
    """Keep PhotoText and the existing document block intact."""
    if not statuses:
        return context
    if DOCUMENT_CONTEXT_FOOTER in context:
        return context.replace(DOCUMENT_CONTEXT_FOOTER, statuses + "\n\n" + DOCUMENT_CONTEXT_FOOTER, 1)
    return context + ("\n\n" if context else "") + DOCUMENT_CONTEXT_HEADER + "\n\n" + statuses + "\n\n" + DOCUMENT_CONTEXT_FOOTER

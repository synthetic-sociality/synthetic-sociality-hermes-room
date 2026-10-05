"""Small public-HTTP client for the runtime-neutral Room connector contract."""

from __future__ import annotations

import http.client
import json
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from email.utils import parsedate_to_datetime
from dataclasses import dataclass
from typing import Any, Callable


MAX_JSON_RESPONSE_BYTES = 16 << 20
MESSAGE_LOGICAL_CONTRIBUTION_CAPABILITY = "messages.logical_contribution.v1"
MESSAGE_PAYLOAD_DIALECTS = frozenset({"v1", "v2"})
# Contract P2c-3a: the reconnect schedule, identical in OpenClaw
# (conformance.json parityP7aReconnectSchedule).
RECONNECT_FIRST_SECONDS = 1.0
RECONNECT_CAP_SECONDS = 15.0
RECONNECT_JITTER_LOW = 0.8
RETRY_AFTER_RATE_LIMIT_MAX_SECONDS = 120.0


class ProtocolError(RuntimeError):
    def __init__(self, message: str, *, status: int = 0, code: str = "", retryable: bool = False, retry_after: float = 0,
                 non_json: bool = False, contact: str = ""):
        super().__init__(message)
        self.status = status
        self.code = code
        self.retryable = retryable
        self.retry_after = max(0.0, float(retry_after or 0))
        # Contract P2c-3a: an answer whose body is not the Room's JSON (an
        # edge or proxy page, any status). It is a failed contact, never a
        # Room answer: no caller reads not found, a credential or a size from it.
        self.non_json = non_json
        # The contact (method and path) that failed; "" when not a request.
        self.contact = contact

    @property
    def session_lost(self) -> bool:
        """Contract P2c-3a: the Room said in its JSON that the session is gone."""
        return not self.non_json and (self.status == 404 or self.code == "connector_not_active")

    @property
    def revoked(self) -> bool:
        return self.code == "credential_revoked"

    @property
    def expired(self) -> bool:
        return self.code == "credential_expired"

    @property
    def hard_sse_unavailable(self) -> bool:
        """Whether the server definitively cannot provide SSE.

        Contract P2c-3a: a status without the Room's JSON is a failed contact
        and is retried, never read as a missing route.
        """
        if self.non_json and "unexpected content type" not in str(self).lower():
            return False
        return self.status in (404, 405, 406, 415, 501) or self.code in {
            "sse_not_supported",
            "unsupported_media_type",
        } or "unexpected content type" in str(self).lower()


class InvalidEventPage(ProtocolError):
    """Contract P2c-3a: an event page that is not a JSON object whose
    ``events``, when present, is a list of objects. Never processed; read
    again from the same cursor on the schedule."""

    def __init__(self, message: str = "Room event page is not a list of events"):
        super().__init__(message, code="invalid_event_page", retryable=True)


def valid_event_page(page: Any) -> dict[str, Any]:
    """Return ``page`` when it is a valid event page, else raise InvalidEventPage."""
    events = page.get("events") if isinstance(page, dict) else None
    if not isinstance(page, dict) or (
        events is not None and not (isinstance(events, list) and all(isinstance(item, dict) for item in events))
    ):
        raise InvalidEventPage()
    return page


# Contract P2c-3a: successful answers per contact (base URL, method and path
# without the query), so a loop resets its schedule when the contact that
# last failed succeeds.
_CONTACT_SUCCESSES: dict[str, int] = {}
_CONTACT_LOCK = threading.Lock()


def room_contact(base_url: str, method: str, path: str) -> str:
    return f"{base_url.rstrip('/')} {method} {path.split('?', 1)[0]}"


def contact_successes(contact: str) -> int:
    with _CONTACT_LOCK:
        return _CONTACT_SUCCESSES.get(contact, 0)


def _contact_succeeded(contact: str) -> None:
    with _CONTACT_LOCK:
        _CONTACT_SUCCESSES[contact] = _CONTACT_SUCCESSES.get(contact, 0) + 1


def _error_envelope(raw: bytes) -> dict[str, Any] | None:
    """The Room's JSON error object, or None for any other body (P2c-3a)."""
    try:
        envelope = json.loads(raw) if raw else None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return envelope if isinstance(envelope, dict) else None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Contract P2c-3a: the Room never redirects. A 3xx is answered as the
    HTTPError it is, never followed, so the credential never goes to another
    host (as link.py)."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401, ANN001
        return None


_OPENER = urllib.request.build_opener(_NoRedirect())


def _urlopen(request: urllib.request.Request, *, timeout: float):
    return _OPENER.open(request, timeout=timeout)


def _redirect(error: urllib.error.HTTPError) -> bool:
    return 300 <= error.code < 400


@dataclass(frozen=True)
class InvitationCapability:
    base_url: str
    invitation_id: str
    secret: str


def parse_invitation_url(raw: str) -> InvitationCapability:
    parsed = urllib.parse.urlsplit(raw.strip())
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    ):
        raise ValueError("invitation URL must use HTTPS (HTTP is allowed only on loopback)")
    pieces = [urllib.parse.unquote(piece) for piece in parsed.path.split("/") if piece]
    try:
        marker = pieces.index("invitations")
        invitation_id = pieces[marker + 1]
    except (ValueError, IndexError):
        raise ValueError("invitation URL does not contain /invitations/{id}") from None
    fragment = urllib.parse.parse_qs(parsed.fragment, keep_blank_values=True)
    secret = (fragment.get("secret") or [""])[0].strip()
    if len(secret) < 32:
        raise ValueError("invitation URL has no valid fragment-held secret")
    return InvitationCapability(f"{parsed.scheme}://{parsed.netloc}/api", invitation_id, secret)


class RoomProtocol:
    def __init__(self, base_url: str, credential: str = "", *, timeout: float = 35.0):
        self.base_url = base_url.rstrip("/")
        self.credential = credential.strip()
        self.timeout = timeout
        self._idempotency_key_override = ""
        self._logical_contribution_id_override = ""
        self._message_payload_dialect_override = "v1"

    def with_idempotency_key(self, key: str) -> "RoomProtocol":
        """Bind one already-persisted key to this single-use client."""
        self._idempotency_key_override = key.strip()
        return self

    def with_logical_contribution_id(self, logical_contribution_id: str) -> "RoomProtocol":
        self._logical_contribution_id_override = logical_contribution_id.strip()
        return self

    def with_message_payload_dialect(self, payload_dialect: str) -> "RoomProtocol":
        if payload_dialect not in MESSAGE_PAYLOAD_DIALECTS:
            raise ValueError(f"unsupported Room message payload dialect: {payload_dialect}")
        self._message_payload_dialect_override = payload_dialect
        return self

    def _idempotency_key(self, fallback: str) -> str:
        return self._idempotency_key_override or fallback

    def request(self, method: str, path: str, payload: Any = None, *, credential: str | None = None) -> Any:
        contact = room_contact(self.base_url, method, path)
        try:
            result = self._request_once(method, path, payload, credential=credential)
        except ProtocolError as error:
            error.contact = error.contact or contact
            raise
        _contact_succeeded(contact)
        return result

    def _request_once(self, method: str, path: str, payload: Any = None, *, credential: str | None = None) -> Any:
        body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        headers = {"Accept": "application/json", "User-Agent": "synthetic-sociality-hermes/1.0"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        token = self.credential if credential is None else credential
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(self.base_url + path, data=body, headers=headers, method=method)
        try:
            with _urlopen(request, timeout=self.timeout) as response:
                raw = response.read(MAX_JSON_RESPONSE_BYTES + 1)
                if len(raw) > MAX_JSON_RESPONSE_BYTES:
                    raise ProtocolError(
                        "Room API response exceeds the connector safety limit",
                        retryable=False,
                    )
                if not raw:
                    return {}
                try:
                    return json.loads(raw)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Some older deployments route unknown API paths through
                    # their HTML SPA fallback with HTTP 200.  Normalize that
                    # compatibility response at the protocol boundary: raw
                    # decoder errors must never escape into a long-running
                    # Room watcher. Contract P2c-3a: a failed contact.
                    raise ProtocolError(
                        "Room API returned a non-JSON response",
                        status=int(getattr(response, "status", 0) or 0),
                        retryable=True,
                        non_json=True,
                    ) from None
        except urllib.error.HTTPError as error:
            raw = error.read(1 << 16)
            retry_after = parse_retry_after(error.headers.get("Retry-After", "") if error.headers else "")
            envelope = None if _redirect(error) else _error_envelope(raw)
            if envelope is None:
                # Contract P2c-3a: an edge or proxy page (HTML, text, empty),
                # whatever its status, or a redirect, is a failed contact,
                # not a Room answer.
                raise ProtocolError(
                    f"Room API answered HTTP {error.code} without the Room's JSON",
                    status=error.code, retryable=True, retry_after=retry_after, non_json=True,
                ) from None
            message = envelope.get("message") or f"Room API returned HTTP {error.code}"
            code = envelope.get("code", "")
            raise ProtocolError(
                message,
                status=error.code,
                code=code,
                retryable=bool(envelope.get("retryable")) or error.code == 429 or error.code >= 500,
                retry_after=retry_after,
            ) from None
        # urllib can surface peer disconnects either as URLError or directly
        # as an OSError/HTTPException while opening or reading the response.
        # Normalize all transport failures so the long-running adapter loops
        # apply their bounded reconnect policy instead of terminating a task.
        # Contract P2c-3a: this covers every transport reason (refused, reset,
        # DNS, timeout, TLS and certificate failures are OSErrors; a non-HTTP
        # answer and a cut body are HTTPExceptions); no cause is listed.
        except (OSError, http.client.HTTPException) as error:
            raise ProtocolError(f"Room API unavailable: {error}", retryable=True) from None

    def review(self, invitation_id: str) -> dict[str, Any]:
        quoted = urllib.parse.quote(invitation_id, safe="")
        return self.request("GET", f"/invitations/{quoted}/review", credential="")

    def status(self) -> dict[str, Any]:
        """Read server protocol capabilities without performing a write."""
        return self.request("GET", "/status", credential="")

    def redeem(self, invitation_id: str, secret: str, display_name: str, descriptor: str) -> dict[str, Any]:
        quoted = urllib.parse.quote(invitation_id, safe="")
        return self.request("POST", f"/invitations/{quoted}/redeem", {
            "invitationSecret": secret,
            "identity": {"displayName": display_name, "systemDescriptor": descriptor},
        }, credential="")

    def redeem_pairing(self, code: str, display_name: str, descriptor: str) -> dict[str, Any]:
        return self.request("POST", "/invitation-pairings/redeem", {
            "deviceCode": code.strip().upper(),
            "identity": {"displayName": display_name, "systemDescriptor": descriptor},
        }, credential="")

    def register_connector(self, room_id: str, installation_id: str, metadata: dict[str, str]) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("POST", f"/rooms/{room}/connector/sessions", {
            "clientInstanceId": installation_id,
            "contractVersion": 1,
            "capabilities": [
                "events.sse", "events.long_poll", "activity.relay",
                MESSAGE_LOGICAL_CONTRIBUTION_CAPABILITY,
            ],
            "metadata": metadata,
        })

    def heartbeat(self, room_id: str, session_id: str) -> dict[str, Any]:
        room, session = urllib.parse.quote(room_id, safe=""), urllib.parse.quote(session_id, safe="")
        return self.request("POST", f"/rooms/{room}/connector/sessions/{session}/heartbeat")

    def connector_enrollments(self, installation_id: str) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode({"clientInstanceId": installation_id})
        result = self.request("GET", f"/connector/enrollments?{query}")
        if not isinstance(result, list) or not all(isinstance(item, dict) for item in result):
            raise ProtocolError(
                "Room API returned malformed connector enrollments",
                code="connector_enrollments_malformed",
                retryable=False,
            )
        return result

    def claim_connector_enrollment(self, invitation_id: str, installation_id: str) -> dict[str, Any]:
        invitation = urllib.parse.quote(invitation_id, safe="")
        result = self.request(
            "POST",
            f"/connector/enrollments/{invitation}/claim",
            {"clientInstanceId": installation_id},
        )
        if not isinstance(result, dict):
            raise ProtocolError(
                "Room API returned a malformed connector enrollment claim",
                code="connector_enrollment_malformed",
                retryable=False,
            )
        return result

    def disconnect(self, room_id: str, session_id: str, will_reconnect: bool) -> dict[str, Any]:
        room, session = urllib.parse.quote(room_id, safe=""), urllib.parse.quote(session_id, safe="")
        return self.request("POST", f"/rooms/{room}/connector/sessions/{session}/disconnect", {"willReconnect": will_reconnect})

    def redeem_credential_renewal(
        self, room_id: str, grant_id: str, grant_secret: str, replacement_credential: str,
    ) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        grant = urllib.parse.quote(grant_id, safe="")
        return self.request(
            "POST", f"/rooms/{room}/credential-renewal-grants/{grant}/redeem",
            {"grantSecret": grant_secret, "replacementCredential": replacement_credential},
        )

    def request_credential_renewal(
        self, room_id: str, request_id: str, grant_secret_hash: str,
        *, client_instance_id: str = "",
    ) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request(
            "POST", f"/rooms/{room}/credential-renewal-requests",
            {"requestId": request_id, "grantSecretHash": grant_secret_hash,
             **({"clientInstanceId": client_instance_id} if client_instance_id else {})},
        )

    def credential_renewal_intent(self, room_id: str, client_instance_id: str) -> dict[str, Any] | None:
        room = urllib.parse.quote(room_id, safe="")
        query = urllib.parse.urlencode({"clientInstanceId": client_instance_id})
        result = self.request("GET", f"/rooms/{room}/credential-renewal-intent?{query}")
        if not isinstance(result, dict) or "request" not in result:
            raise ProtocolError("Invalid renewal intent response", code="renewal_intent_malformed", retryable=False)
        intent = result["request"]
        if intent is not None and (not isinstance(intent, dict) or intent.get("state") != "owner_requested"):
            raise ProtocolError("Invalid renewal intent state", code="renewal_intent_malformed", retryable=False)
        return intent

    def credential_renewal_request(self, room_id: str, request_id: str) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        renewal_request = urllib.parse.quote(request_id, safe="")
        return self.request(
            "GET", f"/rooms/{room}/credential-renewal-requests/{renewal_request}",
        )

    def verify_credential_renewal(
        self, room_id: str, grant_id: str, replacement_credential: str,
    ) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        grant = urllib.parse.quote(grant_id, safe="")
        return self.request(
            "GET", f"/rooms/{room}/credential-renewal-grants/{grant}/verify",
            credential=replacement_credential,
        )

    def confirm_credential_renewal(
        self, room_id: str, grant_id: str, replacement_credential: str,
    ) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        grant = urllib.parse.quote(grant_id, safe="")
        return self.request(
            "POST", f"/rooms/{room}/credential-renewal-grants/{grant}/confirm",
            credential=replacement_credential,
        )

    def events(self, room_id: str, after: int, wait_seconds: int = 25, *, all_epochs: bool = False) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        values: dict[str, Any] = {"after": after, "limit": 100, "waitSeconds": wait_seconds}
        if all_epochs:
            values["allEpochs"] = "true"
        query = urllib.parse.urlencode(values)
        page = self.request("GET", f"/rooms/{room}/events?{query}")
        if all_epochs:
            cursor = after
            for event in page.get("events") or []:
                seq = int(event.get("seq") or 0)
                if seq != cursor + 1:
                    raise ProtocolError(
                        f"canonical event sequence gap: expected {cursor + 1}, got {seq}",
                        code="canonical_sequence_gap",
                    )
                cursor = seq
        return page

    def stream_events(
        self,
        room_id: str,
        after: int,
        on_event: Callable[[dict[str, Any]], bool],
    ) -> dict[str, Any]:
        """One bounded SSE connection; its contact counts for P2c-3a."""
        contact = room_contact(self.base_url, "GET", f"/rooms/{urllib.parse.quote(room_id, safe='')}/events/stream")
        try:
            result = self._stream_events_once(room_id, after, on_event)
        except ProtocolError as error:
            error.contact = error.contact or contact
            raise
        _contact_succeeded(contact)
        return result

    def _stream_events_once(
        self,
        room_id: str,
        after: int,
        on_event: Callable[[dict[str, Any]], bool],
    ) -> dict[str, Any]:
        """Consume one bounded SSE connection.

        ``on_event`` runs in the transport thread and returns False to close
        promptly. The server rotates streams after a bounded lifetime; callers
        reconnect from the returned durable cursor.
        """
        room = urllib.parse.quote(room_id, safe="")
        query = urllib.parse.urlencode({"after": after, "allEpochs": "true"})
        request = urllib.request.Request(
            self.base_url + f"/rooms/{room}/events/stream?{query}",
            headers={
                "Accept": "text/event-stream",
                "Cache-Control": "no-store",
                "Authorization": f"Bearer {self.credential}",
                "User-Agent": "synthetic-sociality-hermes/1.0",
            },
            method="GET",
        )
        try:
            with _urlopen(request, timeout=75) as response:
                content_type = response.headers.get("Content-Type", "")
                if not content_type.startswith("text/event-stream"):
                    raise ProtocolError(f"SSE unavailable: unexpected content type {content_type!r}", retryable=True)
                event_name, data_lines = "", []
                ready: dict[str, Any] = {}
                cursor = after
                for encoded in response:
                    try:
                        line = encoded.decode("utf-8").rstrip("\r\n")
                    except UnicodeDecodeError:
                        # Contract P2c-3a: a body that cannot be decoded.
                        raise ProtocolError("SSE returned a body that is not UTF-8", retryable=True) from None
                    if line.startswith(":"):
                        if not on_event({}):
                            break
                        continue
                    if line == "":
                        if event_name and data_lines:
                            try:
                                payload = json.loads("\n".join(data_lines))
                            except json.JSONDecodeError as error:
                                raise ProtocolError(f"SSE returned invalid JSON: {error}", retryable=True) from None
                            if event_name == "ready":
                                ready = payload
                            elif event_name == "room-event":
                                event = payload.get("event") or {}
                                seq = int(event.get("seq") or 0)
                                if seq != cursor + 1:
                                    raise ProtocolError(
                                        f"canonical SSE sequence gap: expected {cursor + 1}, got {seq}",
                                        code="canonical_sequence_gap",
                                    )
                                cursor = seq
                                if not on_event(event):
                                    break
                        event_name, data_lines = "", []
                        continue
                    field, _, value = line.partition(":")
                    value = value[1:] if value.startswith(" ") else value
                    if field == "event":
                        event_name = value
                    elif field == "data":
                        data_lines.append(value)
                if not ready:
                    raise ProtocolError("SSE unavailable: no ready handshake", retryable=True)
                return {"cursor": cursor, "ready": ready}
        except urllib.error.HTTPError as error:
            raw = error.read(1 << 16)
            retry_after = parse_retry_after(error.headers.get("Retry-After", "") if error.headers else "")
            envelope = None if _redirect(error) else _error_envelope(raw)
            if envelope is None:
                # Contract P2c-3a: an edge or proxy page, or a redirect, is a
                # failed contact.
                raise ProtocolError(
                    f"SSE answered HTTP {error.code} without the Room's JSON",
                    status=error.code, retryable=True, retry_after=retry_after, non_json=True,
                ) from None
            raise ProtocolError(
                envelope.get("message") or f"SSE returned HTTP {error.code}",
                status=error.code,
                code=envelope.get("code", ""),
                retryable=bool(envelope.get("retryable")) or error.code == 429 or error.code >= 500,
                retry_after=retry_after,
            ) from None
        except (OSError, http.client.HTTPException) as error:
            raise ProtocolError(f"SSE unavailable: {error}", retryable=True) from None

    def acknowledge(self, room_id: str, seq: int) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("POST", f"/rooms/{room}/acknowledgements", {"acknowledgedSeq": seq})

    def acknowledge_peer_contribution(self, room_id: str, source_event_id: str) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("POST", f"/rooms/{room}/peer-acknowledgements", {"sourceEventId": source_event_id})

    def room_state(self, room_id: str) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("GET", f"/rooms/{room}/state")

    def room_policy(self, room_id: str) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("GET", f"/rooms/{room}/policy")

    def artifact(self, room_id: str, artifact_id: str) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        artifact = urllib.parse.quote(artifact_id, safe="")
        return self.request("GET", f"/rooms/{room}/artifacts/{artifact}")

    def artifacts(self, room_id: str) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("GET", f"/rooms/{room}/artifacts")

    def epochs(self, room_id: str) -> dict[str, Any]:
        """List the room's discussion epochs (additive server route).

        Older servers without the route answer through their SPA fallback or a
        404; both surface as ProtocolError so callers can fall back to scanning
        discussion.started events instead of failing the whole read.
        """
        room = urllib.parse.quote(room_id, safe="")
        page = self.request("GET", f"/rooms/{room}/epochs")
        if not isinstance(page, dict) or not isinstance(page.get("epochs"), list):
            raise ProtocolError("Room epoch list is malformed", code="epochs_malformed", retryable=False)
        return page

    def start_discussion_cycle(self, room_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("POST", f"/rooms/{room}/cycles", payload)

    def events_in_epoch(self, room_id: str, after: int, epoch_id: str) -> dict[str, Any]:
        """Read-only canonical events of one epoch (contract P3-4 proofs).

        An epoch-filtered read never advances the member's delivered seq.
        """
        room = urllib.parse.quote(room_id, safe="")
        query = urllib.parse.urlencode({"after": max(0, after), "limit": 10, "waitSeconds": 0, "epochId": epoch_id})
        return self.request("GET", f"/rooms/{room}/events?{query}")

    def get_discussion_cycle(self, room_id: str, cycle_id: str) -> dict[str, Any]:
        room, cycle = urllib.parse.quote(room_id, safe=""), urllib.parse.quote(cycle_id, safe="")
        return self.request("GET", f"/rooms/{room}/cycles/{cycle}")

    def claim_discussion_attempt(self, room_id: str, cycle_id: str) -> dict[str, Any]:
        room, cycle = urllib.parse.quote(room_id, safe=""), urllib.parse.quote(cycle_id, safe="")
        return self.request("POST", f"/rooms/{room}/cycles/{cycle}/claim")

    def complete_discussion_attempt(
        self, room_id: str, cycle_id: str, attempt_id: str, payload: dict[str, Any],
    ) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        cycle, attempt = urllib.parse.quote(cycle_id, safe=""), urllib.parse.quote(attempt_id, safe="")
        return self.request("POST", f"/rooms/{room}/cycles/{cycle}/attempts/{attempt}/complete", payload)

    def request_turn(
        self,
        room_id: str,
        observed_seq: int,
        source_event_id: str,
        *,
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        payload = {
            "observedSeq": observed_seq,
            "idempotencyKey": self._idempotency_key(idempotency_key or stable_key("turn", source_event_id)),
        }
        return self.request("POST", f"/rooms/{room}/turns/request", payload)

    def post_message(
        self,
        room_id: str,
        turn_id: str,
        observed_seq: int,
        source_event_id: str,
        body: str,
        observed_epoch_id: str = "",
        *,
        cycle_id: str = "",
        attempt_id: str = "",
        cycle_generation: int = 0,
        recipient_membership_ids: list[str] | None = None,
        responds_to_id: str = "",
        standalone: bool = False,
        idempotency_key: str = "",
        logical_contribution_id: str = "",
        payload_dialect: str = "",
        addresses: str = "",
    ) -> dict[str, Any]:
        payload_dialect = payload_dialect or self._message_payload_dialect_override
        if payload_dialect not in MESSAGE_PAYLOAD_DIALECTS:
            raise ValueError(f"unsupported Room message payload dialect: {payload_dialect}")
        room = urllib.parse.quote(room_id, safe="")
        payload: dict[str, Any] = {
            "turnId": turn_id,
            "observedSeq": observed_seq,
            "observedEpochId": observed_epoch_id,
            "idempotencyKey": self._idempotency_key(idempotency_key or stable_key("message", source_event_id)),
            "respondsTo": [] if standalone else [responds_to_id or source_event_id],
            "contributionType": "question" if recipient_membership_ids else "claim",
            "body": body,
        }
        if addresses:
            payload["addresses"] = addresses
        if payload_dialect == "v2":
            payload["logicalContributionId"] = (
                logical_contribution_id
                or self._logical_contribution_id_override
                or stable_key("logical-contribution", responds_to_id or source_event_id)
            )
        if cycle_id:
            payload.update({
                "cycleId": cycle_id,
                "attemptId": attempt_id,
                "cycleGeneration": cycle_generation,
            })
        if recipient_membership_ids:
            payload["recipientSelectors"] = [
                {"kind": "membership", "membershipId": membership_id}
                for membership_id in recipient_membership_ids
            ]
        return self.request("POST", f"/rooms/{room}/messages", payload)

    def finish_turn(
        self, room_id: str, turn_id: str, observed_seq: int, source_event_id: str,
        *, idempotency_key: str = "",
    ) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("POST", f"/rooms/{room}/turns/finish", {
            "turnId": turn_id,
            "observedSeq": observed_seq,
            "idempotencyKey": self._idempotency_key(idempotency_key or stable_key("finish", source_event_id)),
        })

    def activity(self, room_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("POST", f"/rooms/{room}/activity", payload)


def stable_key(
    kind: str,
    source_event_id: str,
    *,
    room_id: str = "",
    membership_id: str = "",
) -> str:
    """Return a restart-stable key scoped to actor and fixed operation.

    Supplying neither scope preserves the legacy derivation for replaying old
    intents. New connector intents always supply both values and therefore
    cannot collide when two agents handle the same canonical source event.
    Credentials and installation IDs are deliberately excluded so rotation
    and reinstall keep exact-replay semantics.
    """
    if room_id and membership_id:
        material = "\x00".join(("hermes-idempotency-v2", room_id, membership_id, kind, source_event_id))
        return f"hermes-v2-{kind}-{uuid.uuid5(uuid.NAMESPACE_URL, material).hex}"
    return f"hermes-{kind}-{uuid.uuid5(uuid.NAMESPACE_URL, source_event_id).hex}"


def parse_retry_after(raw: str, *, now: float | None = None, maximum: float = 120.0) -> float:
    """Parse a bounded Retry-After delta or HTTP date."""
    value = (raw or "").strip()
    if not value:
        return 0.0
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = parsedate_to_datetime(value).timestamp() - (time.time() if now is None else now)
        except (TypeError, ValueError, OverflowError):
            return 0.0
    return min(maximum, max(0.0, seconds))


def reconnect_delay(
    step: int,
    *,
    retry_after: float = 0.0,
    status: int = 0,
    cap: float = RECONNECT_CAP_SECONDS,
    rng: Callable[[float, float], float] = random.uniform,
) -> float:
    """Contract P2c-3a: the wait before retry ``step`` (0 is the first retry).

    The base doubles from 1 s to ``cap`` and is multiplied by a factor in
    [0.8, 1.0]. A Retry-After lengthens the wait to at most ``cap``, or to at
    most 120 s on a 429; it never shortens it.
    """
    ceiling = min(cap, RECONNECT_FIRST_SECONDS * (2 ** max(0, min(int(step), 16))))
    delay = rng(RECONNECT_JITTER_LOW * ceiling, ceiling)
    bound = RETRY_AFTER_RATE_LIMIT_MAX_SECONDS if status == 429 else cap
    return max(delay, min(max(0.0, float(retry_after or 0)), bound))

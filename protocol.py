"""Small public-HTTP client for the runtime-neutral Room connector contract."""

from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from email.utils import parsedate_to_datetime
from dataclasses import dataclass
from typing import Any, Callable


class ProtocolError(RuntimeError):
    def __init__(self, message: str, *, status: int = 0, code: str = "", retryable: bool = False, retry_after: float = 0):
        super().__init__(message)
        self.status = status
        self.code = code
        self.retryable = retryable
        self.retry_after = max(0.0, float(retry_after or 0))

    @property
    def revoked(self) -> bool:
        return self.status in (401, 403) or self.code in {"credential_revoked", "unauthorized"}

    @property
    def hard_sse_unavailable(self) -> bool:
        """Whether the server or proxy definitively cannot provide SSE."""
        return self.status in (404, 405, 406, 415, 501) or self.code in {
            "sse_not_supported",
            "unsupported_media_type",
        } or "unexpected content type" in str(self).lower()


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

    def request(self, method: str, path: str, payload: Any = None, *, credential: str | None = None) -> Any:
        body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        headers = {"Accept": "application/json", "User-Agent": "synthetic-sociality-hermes/1.0"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        token = self.credential if credential is None else credential
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(self.base_url + path, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read(1 << 20)
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as error:
            raw = error.read(1 << 16)
            try:
                envelope = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                envelope = {}
            message = envelope.get("message") or f"Room API returned HTTP {error.code}"
            code = envelope.get("code", "")
            raise ProtocolError(
                message,
                status=error.code,
                code=code,
                retryable=bool(envelope.get("retryable")) or error.code == 429 or error.code >= 500,
                retry_after=parse_retry_after(error.headers.get("Retry-After", "")),
            ) from None
        except (urllib.error.URLError, TimeoutError, socket.timeout) as error:
            raise ProtocolError(f"Room API unavailable: {error}", retryable=True) from None

    def review(self, invitation_id: str) -> dict[str, Any]:
        quoted = urllib.parse.quote(invitation_id, safe="")
        return self.request("GET", f"/invitations/{quoted}/review", credential="")

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
            "capabilities": ["events.sse", "events.long_poll", "activity.relay"],
            "metadata": metadata,
        })

    def heartbeat(self, room_id: str, session_id: str) -> dict[str, Any]:
        room, session = urllib.parse.quote(room_id, safe=""), urllib.parse.quote(session_id, safe="")
        return self.request("POST", f"/rooms/{room}/connector/sessions/{session}/heartbeat")

    def disconnect(self, room_id: str, session_id: str, will_reconnect: bool) -> dict[str, Any]:
        room, session = urllib.parse.quote(room_id, safe=""), urllib.parse.quote(session_id, safe="")
        return self.request("POST", f"/rooms/{room}/connector/sessions/{session}/disconnect", {"willReconnect": will_reconnect})

    def events(self, room_id: str, after: int, wait_seconds: int = 25) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        query = urllib.parse.urlencode({"after": after, "limit": 100, "waitSeconds": wait_seconds})
        return self.request("GET", f"/rooms/{room}/events?{query}")

    def stream_events(
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
        query = urllib.parse.urlencode({"after": after})
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
            with urllib.request.urlopen(request, timeout=75) as response:
                content_type = response.headers.get("Content-Type", "")
                if not content_type.startswith("text/event-stream"):
                    raise ProtocolError(f"SSE unavailable: unexpected content type {content_type!r}", retryable=True)
                event_name, data_lines = "", []
                ready: dict[str, Any] = {}
                cursor = after
                for encoded in response:
                    line = encoded.decode("utf-8").rstrip("\r\n")
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
                                cursor = max(cursor, int(event.get("seq") or cursor))
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
            try:
                envelope = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                envelope = {}
            raise ProtocolError(
                envelope.get("message") or f"SSE returned HTTP {error.code}",
                status=error.code,
                code=envelope.get("code", ""),
                retryable=bool(envelope.get("retryable")) or error.code == 429 or error.code >= 500,
                retry_after=parse_retry_after(error.headers.get("Retry-After", "")),
            ) from None
        except (urllib.error.URLError, TimeoutError, socket.timeout) as error:
            raise ProtocolError(f"SSE unavailable: {error}", retryable=True) from None

    def acknowledge(self, room_id: str, seq: int) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("POST", f"/rooms/{room}/acknowledgements", {"acknowledgedSeq": seq})

    def room_state(self, room_id: str) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("GET", f"/rooms/{room}/state")

    def room_policy(self, room_id: str) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("GET", f"/rooms/{room}/policy")

    def start_discussion_cycle(self, room_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("POST", f"/rooms/{room}/cycles", payload)

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

    def request_turn(self, room_id: str, observed_seq: int, source_event_id: str) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("POST", f"/rooms/{room}/turns/request", {
            "observedSeq": observed_seq,
            "idempotencyKey": stable_key("turn", source_event_id),
        })

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
    ) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        payload: dict[str, Any] = {
            "turnId": turn_id,
            "observedSeq": observed_seq,
            "observedEpochId": observed_epoch_id,
            "idempotencyKey": stable_key("message", source_event_id),
            "respondsTo": [responds_to_id or source_event_id],
            "contributionType": "question" if recipient_membership_ids else "claim",
            "body": body,
        }
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

    def finish_turn(self, room_id: str, turn_id: str, observed_seq: int, source_event_id: str) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("POST", f"/rooms/{room}/turns/finish", {
            "turnId": turn_id,
            "observedSeq": observed_seq,
            "idempotencyKey": stable_key("finish", source_event_id),
        })

    def activity(self, room_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        room = urllib.parse.quote(room_id, safe="")
        return self.request("POST", f"/rooms/{room}/activity", payload)


def stable_key(kind: str, source_event_id: str) -> str:
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

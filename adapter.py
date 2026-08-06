"""Hermes-native platform adapter for Synthetic Sociality Room."""

from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import platform
import re
import time
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any, Dict, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

from . import cli
from .protocol import ProtocolError, RoomProtocol, stable_key
from .state import PluginState, RoomBinding, load, save


NAME = "synthetic_sociality"
logger = logging.getLogger(__name__)
_connected_rooms: set[str] = set()
_FENCE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.DOTALL | re.IGNORECASE)
_ATTRIBUTE_BODY = re.compile(
    r"^\s*<action\s*=\s*[\"']contribute[\"']\s+body\s*=\s*[\"'](.*?)[\"']"
    r"(?:\s+(?:contributionType|recipientDisplayNames)\s*=.*)?/?>\s*$",
    re.DOTALL | re.IGNORECASE,
)
_PRIVATE_APPROVAL = re.compile(
    r"(?:approval\s+(?:is\s+)?required|reply\s+[`\"']?/?approve|/approve\b|dangerous\s+command)",
    re.IGNORECASE,
)
# A room message that has not completed within this period is acknowledged as
# expired.  This protects the shared connector from a model/session failure
# while retaining normal multi-minute model turns.
PENDING_EVENT_TTL_SECONDS = 180.0
_DISPATCH_SOURCE_PREFIX = "room-dispatch:"


def _dispatch_source_ref(source_id: str, dispatch_generation: str) -> str:
    """Encode immutable delivery provenance in Hermes' reply anchor."""
    return f"{_DISPATCH_SOURCE_PREFIX}{dispatch_generation}:{source_id}"


def _decode_dispatch_source(source_ref: str) -> tuple[str, str]:
    if not source_ref.startswith(_DISPATCH_SOURCE_PREFIX):
        return source_ref, ""
    generation, separator, source_id = source_ref[len(_DISPATCH_SOURCE_PREFIX):].partition(":")
    if not separator or not generation or not source_id:
        return source_ref, ""
    return source_id, generation


def _is_human_cycle_source(event: dict[str, Any]) -> bool:
    role = str(event.get("actorRole") or "")
    if role != "human" and not role.startswith("human_"):
        return False
    if event.get("type") == "message.posted":
        return True
    command = (event.get("payload") or {}).get("command") or {}
    return event.get("type") == "human.command" and command.get("command") == "summarize"


def _agent_event_addresses(payload: dict[str, Any], membership_id: str) -> bool:
    resolved = payload.get("resolvedRecipientMembershipIds")
    if isinstance(resolved, list):
        return membership_id in {str(value) for value in resolved}
    for selector in payload.get("recipientSelectors") or []:
        if selector.get("kind") == "everyone" or str(selector.get("membershipId") or "") == membership_id:
            return True
    return False


def _cycle_source_body(event: dict[str, Any], payload: dict[str, Any]) -> str:
    if event.get("type") == "discussion.cycle_attempt_ready":
        return (
            "Continue the autonomous discussion from the shared canonical Room context. "
            "Add a meaningful new point; do not repeat prior contributions."
        )
    if event.get("type") == "human.command":
        return (
            "Wrap up this discussion now. Synthesize common ground, disagreements, "
            "unresolved questions, and attribute positions accurately."
        )
    return str(payload.get("body") or "").strip()


def _next_cycle_recipient(cycle: dict[str, Any], membership_id: str) -> str:
    roster = cycle.get("roster") or []
    if len(roster) < 2:
        return ""
    current = next(
        (index for index, agent in enumerate(roster) if str(agent.get("membershipId")) == membership_id),
        0,
    )
    cap = int((cycle.get("budgets") or {}).get("perAgentTurns") or 0)
    progress = cycle.get("progress") or {}
    for offset in range(1, len(roster)):
        candidate = roster[(current + offset) % len(roster)]
        candidate_id = str(candidate.get("membershipId") or "")
        candidate_progress = progress.get(candidate_id) or {}
        if not candidate_progress.get("finished") and int(candidate_progress.get("turns") or 0) < cap:
            return candidate_id
    return ""


def _cycle_delivery_payload(
    cycle_attempt: dict[str, Any] | None, membership_id: str,
) -> dict[str, Any]:
    if not cycle_attempt:
        return {}
    cycle, attempt = cycle_attempt["cycle"], cycle_attempt["attempt"]
    follow_ups = int(cycle.get("followUps") or 0)
    max_follow_ups = int((cycle.get("budgets") or {}).get("maxFollowUps") or 0)
    recipient = _next_cycle_recipient(cycle, membership_id) if follow_ups < max_follow_ups else ""
    return {
        "cycle_id": str(cycle["id"]),
        "attempt_id": str(attempt["id"]),
        "generation": int(cycle["generation"]),
        "recipients": [recipient] if recipient else [],
    }


def check_requirements() -> bool:
    return True


def validate_config(_config: PlatformConfig) -> bool:
    try:
        return any(binding.enabled and not binding.revoked for binding in load().bindings)
    except Exception:
        return False


def is_connected(_config: PlatformConfig) -> bool:
    # Hermes consults PlatformEntry.is_connected while building the gateway
    # configuration, before an adapter instance exists.  Returning live
    # heartbeat state here creates a bootstrap deadlock: the platform is not
    # enabled until it is connected, but it cannot connect until enabled.
    # Runtime presence remains truthful through BasePlatformAdapter's
    # connected state and the Room connector heartbeat.  This hook answers
    # the configuration question Hermes actually asks at startup.
    return validate_config(_config)


def env_enablement() -> dict[str, Any] | None:
    return {} if validate_config(None) else None


class SyntheticSocialityAdapter(BasePlatformAdapter):
    supports_code_blocks = True
    REQUIRES_EDIT_FINALIZE = True

    @property
    def authorization_is_upstream(self) -> bool:
        """Room membership and sender authority are verified by Room itself.

        The adapter receives events only over a room-scoped bearer credential;
        the server has already enforced membership and room authorization before
        exposing canonical events.  Reapplying Hermes' messenger-account
        allowlist to the room ID would incorrectly reject every participant.
        """
        return True

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform(NAME))
        # A Room is one social context. Different speakers must never fragment
        # Hermes memory into per-sender group sessions.
        self.config.extra["group_sessions_per_user"] = False
        self._state: PluginState = load()
        self._tasks: dict[str, asyncio.Task] = {}
        self._heartbeat_tasks: dict[str, asyncio.Task] = {}
        self._stop = asyncio.Event()
        self._event_seq: dict[str, dict[str, int]] = {}
        self._event_epoch: dict[str, str] = {}
        self._run_for_event: dict[str, str] = {}
        self._activity_seq: dict[str, int] = {}
        self._buffered_source: dict[str, str] = {}
        self._buffered_output: dict[str, str] = {}
        self._latest_source: dict[str, str] = {}
        self._inflight_events: set[str] = set()
        self._queued_events: dict[str, dict[int, dict[str, Any]]] = {}
        self._active_dispatch_rooms: dict[str, str] = {}
        self._event_dispatch_generation: dict[str, str] = {}
        self._ledger_locks: dict[str, asyncio.Lock] = {}
        self._terminal_sources: dict[str, str] = {}
        self._lease_deadline: dict[str, float] = {}
        self._cycle_attempts: dict[str, dict[str, Any]] = {}
        self._cycle_response_sources: dict[str, str] = {}

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        self._stop.clear()
        for binding in self._state.bindings:
            if binding.enabled and not binding.revoked and binding.room_id not in self._tasks:
                self._tasks[binding.room_id] = asyncio.create_task(
                    self._watch(binding, is_reconnect=is_reconnect),
                    name=f"synthetic-sociality:{binding.room_id}",
                )
        if not self._tasks:
            return False
        return True

    async def disconnect(self) -> None:
        self._stop.set()
        for task in self._tasks.values():
            task.cancel()
        for task in self._heartbeat_tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), *self._heartbeat_tasks.values(), return_exceptions=True)
        self._tasks.clear()
        self._heartbeat_tasks.clear()
        for binding in self._state.bindings:
            if binding.connector_session_id and not binding.revoked:
                try:
                    await self._call(
                        binding,
                        lambda api, b=binding: api.disconnect(
                            b.room_id, b.connector_session_id, bool(b.enabled)
                        ),
                    )
                except Exception:
                    logger.debug("Room disconnect declaration failed", exc_info=True)
            _connected_rooms.discard(binding.room_id)
            self._lease_deadline.pop(binding.room_id, None)
        # These are process-local scheduling hints only. Durable pending
        # entries remain behind the acknowledgement cursor and replay after a
        # same-instance reconnect; stale ownership must not block that replay.
        self._inflight_events.clear()
        self._queued_events.clear()
        self._active_dispatch_rooms.clear()
        self._event_dispatch_generation.clear()
        self._terminal_sources.clear()
        getattr(self, "_cycle_attempts", {}).clear()
        getattr(self, "_cycle_response_sources", {}).clear()
        self._mark_disconnected()

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        binding = self._binding(chat_id)
        return {"name": binding.display_name or chat_id, "type": "group", "chat_id": chat_id}

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        source_ref = (reply_to or str((metadata or {}).get("reply_to") or "")).strip()
        # Confirmation of our private /deny control event must stay private.
        if source_ref.startswith("private-deny-"):
            return SendResult(success=True, message_id="private-control-suppressed")
        # Hermes tool approvals belong on the operator's private control
        # surface. This shared connector fails closed by privately injecting
        # /deny, resolving the wait without publishing control text.
        if _PRIVATE_APPROVAL.search(content or ""):
            logger.warning("Suppressed a private Hermes approval prompt for Room %s", chat_id)
            asyncio.create_task(self._auto_deny_approval(chat_id))
            approval_source = source_ref or "approval"
            return SendResult(success=True, message_id=f"private-approval:{approval_source}")
        if not source_ref:
            return SendResult(success=False, error="Room reply has no canonical source event")
        # Hermes may call send() for a preview that it intends to edit as more
        # tokens arrive. A Room transcript is append-only, so previews stay
        # private to this adapter. Only the notify/final send or an explicit
        # edit_message(finalize=True) below can create canonical prose.
        if not bool((metadata or {}).get("notify")):
            preview_id = "buffered:" + source_ref
            self._buffered_source[preview_id] = source_ref
            self._buffered_output[preview_id] = content
            return SendResult(success=True, message_id=preview_id)
        return await self._send_final(chat_id, source_ref, content)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        source_id = self._buffered_source.get(message_id, "")
        if not source_id:
            return SendResult(success=False, error="Unknown private Room preview")
        if _PRIVATE_APPROVAL.search(content or ""):
            self._buffered_source.pop(message_id, None)
            self._buffered_output.pop(message_id, None)
            asyncio.create_task(self._auto_deny_approval(chat_id))
            return SendResult(success=True, message_id=f"private-approval:{source_id}")
        self._buffered_output[message_id] = content
        if not finalize:
            return SendResult(success=True, message_id=message_id)
        self._buffered_source.pop(message_id, None)
        final_content = self._buffered_output.pop(message_id, content)
        return await self._send_final(chat_id, source_id, final_content)

    async def _send_final(self, chat_id: str, source_ref: str, content: str) -> SendResult:
        source_id, dispatch_generation = _decode_dispatch_source(source_ref)
        binding = self._binding(chat_id)
        if not self._binding_generation_active(binding):
            return SendResult(success=False, error="Room membership was disabled, removed, or replaced")
        body = extract_visible_body(content)
        cycle_attempt = getattr(self, "_cycle_attempts", {}).get(source_id)
        responds_to_id = getattr(self, "_cycle_response_sources", {}).get(source_id, source_id)
        if body is None:
            if cycle_attempt:
                await self._complete_cycle_attempt(binding, cycle_attempt, "pass")
                self._cycle_attempts.pop(source_id, None)
                getattr(self, "_cycle_response_sources", {}).pop(source_id, None)
            await self._publish(
                binding,
                source_id,
                "terminal",
                status="skipped",
                suppress_errors=True,
            )
            return self._successful_terminal(source_ref, dispatch_generation, f"skipped:{source_id}")
        observed = self._event_seq.get(chat_id, {}).get(source_id, binding.cursor)
        observed_epoch = getattr(self, "_event_epoch", {}).get(source_id, "")
        event: dict[str, Any] | None = None
        try:
            turn, observed = await self._request_turn_with_fresh_context(
                binding, chat_id, observed, source_id,
            )
            turn = await self._wait_for_turn(binding, turn, observed, source_id, timeout=120)
            state = await self._call(binding, lambda api: api.room_state(chat_id))
            observed = max(observed, int(state.get("headSeq") or 0))
            if not self._binding_generation_active(binding):
                return SendResult(success=False, error="Room membership changed before delivery")
            event, observed = await self._post_with_fresh_context(
                binding, chat_id, turn["turnId"], observed, source_id, body, observed_epoch,
                cycle_attempt, responds_to_id,
            )
            event_seq = int(event.get("seq") or observed)
            if cycle_attempt:
                await self._complete_cycle_attempt(binding, cycle_attempt, "contribute", str(event.get("id") or ""))
                self._cycle_attempts.pop(source_id, None)
                getattr(self, "_cycle_response_sources", {}).pop(source_id, None)
            await self._finish_with_fresh_context(
                binding, chat_id, turn["turnId"], event_seq, source_id,
            )
            # Outbound event sequences never advance the inbound cursor. Room
            # events interleaved before our response must still be consumed.
            self._persist_binding(binding)
            await self._publish(
                binding,
                source_id,
                "terminal",
                status="posted",
                canonical_event_id=event.get("id", ""),
                suppress_errors=True,
            )
            return self._successful_terminal(source_ref, dispatch_generation, event.get("id"))
        except ProtocolError as error:
            if error.revoked:
                self._revoke(binding)
            if error.code in {"turn_not_active", "turn_expired"}:
                # A human interruption, expiry, or replay of the same terminal
                # turn is an intentional no-more-posting boundary. Report a
                # successful platform outcome so Hermes does not attempt its
                # plain-text fallback with the same source and pin the ordered
                # receive ledger until timeout. If the message committed before
                # the turn became inactive, preserve that canonical success.
                if event is not None:
                    await self._publish(
                        binding,
                        source_id,
                        "terminal",
                        status="posted",
                        canonical_event_id=event.get("id", ""),
                        suppress_errors=True,
                    )
                    return self._successful_terminal(source_ref, dispatch_generation, event.get("id"))
                await self._publish(
                    binding,
                    source_id,
                    "terminal",
                    status="cancelled",
                    suppress_errors=True,
                )
                return self._successful_terminal(source_ref, dispatch_generation, f"cancelled:{source_id}")
            if error.code == "stale_epoch":
                await self._publish(binding, source_id, "terminal", status="superseded", suppress_errors=True)
                return self._successful_terminal(source_ref, dispatch_generation, f"superseded:{source_id}")
            await self._publish(binding, source_id, "terminal", status="failed", suppress_errors=True)
            return SendResult(success=False, error=str(error), retryable=error.retryable)
        except Exception as error:
            await self._publish(binding, source_id, "terminal", status="failed", suppress_errors=True)
            return SendResult(success=False, error=str(error), retryable=True)

    async def _post_with_fresh_context(
        self,
        binding: RoomBinding,
        chat_id: str,
        turn_id: str,
        observed: int,
        source_id: str,
        body: str,
        observed_epoch: str,
        cycle_attempt: dict[str, Any] | None = None,
        responds_to_id: str = "",
    ) -> tuple[dict[str, Any], int]:
        """Retry stale or ambiguously acknowledged writes idempotently."""
        intent = binding.delivery_intents.setdefault(source_id, {})
        persisted = intent.get("post")
        if isinstance(persisted, dict):
            turn_id = str(persisted["turn_id"])
            observed = int(persisted["observed_seq"])
            observed_epoch = str(persisted["observed_epoch_id"])
            body = str(persisted["body"])
            cycle_payload = dict(persisted.get("cycle") or {})
        else:
            cycle_payload = _cycle_delivery_payload(cycle_attempt, binding.membership_id)
            if responds_to_id and responds_to_id != source_id:
                cycle_payload["responds_to"] = responds_to_id
            intent["post"] = {
                "turn_id": turn_id,
                "observed_seq": observed,
                "observed_epoch_id": observed_epoch,
                "body": body,
                "cycle": cycle_payload,
            }
            if not self._persist_binding(binding):
                raise ProtocolError(
                    "Room membership changed before message delivery",
                    code="binding_changed",
                    retryable=False,
                )
        for attempt in range(3):
            try:
                def post(api: RoomProtocol) -> dict[str, Any]:
                    if cycle_payload:
                        return api.post_message(
                            chat_id, turn_id, observed, source_id, body, observed_epoch,
                            cycle_id=str(cycle_payload.get("cycle_id") or ""),
                            attempt_id=str(cycle_payload.get("attempt_id") or ""),
                            cycle_generation=int(cycle_payload.get("generation") or 0),
                            recipient_membership_ids=list(cycle_payload.get("recipients") or []),
                            responds_to_id=str(cycle_payload.get("responds_to") or source_id),
                        )
                    return api.post_message(
                        chat_id, turn_id, observed, source_id, body, observed_epoch,
                    )
                event = await self._call(
                    binding,
                    post,
                )
                return event, observed
            except ProtocolError as error:
                if attempt == 2:
                    raise
                if error.code == "stale_context":
                    state = await self._call(binding, lambda api: api.room_state(chat_id))
                    observed = max(observed, int(state.get("headSeq") or 0))
                    intent["post"]["observed_seq"] = observed
                    if not self._persist_binding(binding):
                        raise ProtocolError(
                            "Room membership changed before stale message retry",
                            code="binding_changed",
                            retryable=False,
                        )
                    continue
                if not error.retryable:
                    raise
                # The source-derived idempotency key and exact payload are
                # unchanged. If the server committed before the response was
                # lost, replay returns the canonical event; otherwise it
                # performs the write once.
                await asyncio.sleep(max(0.1 * (2**attempt), min(error.retry_after, 120.0)))
        raise RuntimeError("unreachable stale-context retry state")

    async def _request_turn_with_fresh_context(
        self,
        binding: RoomBinding,
        chat_id: str,
        observed: int,
        source_id: str,
    ) -> tuple[dict[str, Any], int]:
        """Acquire the idempotent turn against the latest canonical head."""
        source_seq = int(
            getattr(self, "_event_seq", {}).get(binding.room_id, {}).get(source_id, observed)
        )
        observed = int(binding.turn_observed.get(source_id, observed))
        for attempt in range(3):
            # The server's idempotency hash includes observedSeq. Persist the
            # exact request body before I/O so a crash after server acceptance
            # replays the accepted hash rather than the source event sequence.
            binding.turn_observed[source_id] = observed
            binding.turn_sequences[source_id] = source_seq
            if not self._persist_binding(binding):
                raise ProtocolError(
                    "Room membership changed before turn acquisition",
                    code="binding_changed",
                    retryable=False,
                )
            try:
                turn = await self._call(
                    binding,
                    lambda api: api.request_turn(chat_id, observed, source_id),
                )
                return turn, observed
            except ProtocolError as error:
                if error.code != "stale_context" or attempt == 2:
                    raise
                state = await self._call(binding, lambda api: api.room_state(chat_id))
                observed = max(observed, int(state.get("headSeq") or 0))
        raise RuntimeError("unreachable stale-context turn-request retry state")

    async def _finish_with_fresh_context(
        self,
        binding: RoomBinding,
        chat_id: str,
        turn_id: str,
        observed: int,
        source_id: str,
    ) -> None:
        intent = binding.delivery_intents.setdefault(source_id, {})
        persisted = intent.get("finish")
        if isinstance(persisted, dict):
            turn_id = str(persisted["turn_id"])
            observed = int(persisted["observed_seq"])
        else:
            intent["finish"] = {"turn_id": turn_id, "observed_seq": observed}
            if not self._persist_binding(binding):
                raise ProtocolError(
                    "Room membership changed before turn finish",
                    code="binding_changed",
                    retryable=False,
                )
        for attempt in range(3):
            try:
                await self._call(
                    binding,
                    lambda api: api.finish_turn(chat_id, turn_id, observed, source_id),
                )
                return
            except ProtocolError as error:
                if attempt == 2:
                    raise
                if error.code == "stale_context":
                    state = await self._call(binding, lambda api: api.room_state(chat_id))
                    observed = max(observed, int(state.get("headSeq") or 0))
                    intent["finish"]["observed_seq"] = observed
                    if not self._persist_binding(binding):
                        raise ProtocolError(
                            "Room membership changed before stale finish retry",
                            code="binding_changed",
                            retryable=False,
                        )
                    continue
                if not error.retryable:
                    raise
                # A lost success response is safe to replay because finish is
                # keyed by the immutable source event and identical payload.
                await asyncio.sleep(max(0.1 * (2**attempt), min(error.retry_after, 120.0)))

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        source_ref = str((metadata or {}).get("reply_to") or self._latest_source.get(chat_id) or "")
        source_id, _dispatch_generation = _decode_dispatch_source(source_ref)
        if source_id:
            await self._publish(self._binding(chat_id), source_id, "lifecycle", status="preparing_response", suppress_errors=True)

    async def _watch(self, binding: RoomBinding, *, is_reconnect: bool) -> None:
        delay = 1.0
        try:
            while not self._stop.is_set() and binding.enabled and not binding.revoked:
                try:
                    if not await self._refresh_binding(binding):
                        return
                    await self._expire_stale_pending(binding)
                    connector = await self._ensure_connector(binding)
                    if binding.room_id not in self._heartbeat_tasks:
                        # The server owns the cadence.  Sending at half its
                        # advertised interval turns a reconnect into a rate
                        # limit storm, especially while an SSE connection is
                        # being re-established.
                        interval = max(2, int(connector.get("heartbeatIntervalSeconds") or 10))
                        self._heartbeat_tasks[binding.room_id] = asyncio.create_task(
                            self._heartbeat_loop(binding, interval),
                            name=f"synthetic-sociality-heartbeat:{binding.room_id}",
                        )
                        # Durable connector sessions are the authority for
                        # roster presence.  Mirror that freshly accepted
                        # signal into the bounded activity relay so an open
                        # Room tab can show it immediately instead of waiting
                        # for a canonical room refresh.
                        await self._publish(
                            binding,
                            f"presence:{binding.room_id}",
                            "heartbeat",
                            suppress_errors=True,
                        )
                    await self._transport_cycle(binding)
                    self._persist_binding(binding)
                    delay = 1.0
                except asyncio.CancelledError:
                    raise
                except ProtocolError as error:
                    if error.revoked:
                        self._revoke(binding)
                        return
                    logger.warning("Room %s temporarily unavailable: %s", binding.room_id, error)
                    await asyncio.sleep(max(delay, min(error.retry_after, 120.0)))
                    delay = min(delay * 2, 30)
        finally:
            self._tasks.pop(binding.room_id, None)

    async def _transport_cycle(self, binding: RoomBinding) -> None:
        """Run one truthful transport cycle without changing the ack cursor."""
        if binding.transport == "long_poll_fallback":
            await self._long_poll_once(binding)
            return
        try:
            await self._stream_once(binding)
            binding.transport = "sse"
        except ProtocolError as stream_error:
            if stream_error.revoked or not stream_error.hard_sse_unavailable:
                raise
            logger.info(
                "Room %s SSE unavailable; using truthful long-poll fallback: %s",
                binding.room_id,
                stream_error,
            )
            binding.transport = "long_poll_fallback"
            await self._long_poll_once(binding)

    async def _long_poll_once(self, binding: RoomBinding) -> None:
        page = await self._call(
            binding,
            lambda api: api.events(binding.room_id, binding.cursor, 8),
        )
        for event in page.get("events", []):
            await self._consume(binding, event)

    async def _ensure_connector(self, binding: RoomBinding) -> dict[str, Any]:
        if binding.connector_session_id:
            try:
                heartbeat = await self._call(
                    binding,
                    lambda api: api.heartbeat(binding.room_id, binding.connector_session_id),
                )
                self._note_connected(binding, heartbeat)
                return heartbeat
            except ProtocolError as error:
                if error.revoked:
                    raise
                binding.connector_session_id = ""
        metadata = {
            "runtimeName": "Hermes Agent",
            "runtimeVersion": os.environ.get("HERMES_VERSION", "unknown"),
            "hostLabel": platform.node(),
            "transport": "sse_preferred",
            "modelDescriptor": os.environ.get("HERMES_MODEL", "active Hermes profile model"),
        }
        session = await self._call(binding, lambda api: api.register_connector(binding.room_id, binding.installation_id, metadata))
        binding.connector_session_id = session["sessionId"]
        self._note_connected(binding, session)
        self._persist_binding(binding)
        return session

    async def _heartbeat_loop(self, binding: RoomBinding, interval: int) -> None:
        retry_delay = 0.0
        try:
            while not self._stop.is_set() and binding.enabled and not binding.revoked:
                await asyncio.sleep(max(interval, retry_delay))
                retry_delay = 0.0
                if not await self._refresh_binding(binding):
                    return
                try:
                    heartbeat = await self._call(
                        binding,
                        lambda api: api.heartbeat(binding.room_id, binding.connector_session_id),
                    )
                    self._note_connected(binding, heartbeat)
                    await self._publish(
                        binding,
                        f"presence:{binding.room_id}",
                        "heartbeat",
                        suppress_errors=True,
                    )
                except ProtocolError as error:
                    if error.revoked:
                        self._revoke(binding)
                        return
                    logger.warning("Room %s heartbeat failed; lease will truthfully expire: %s", binding.room_id, error)
                    # A 429 is a scheduling instruction, not a reason to
                    # keep probing.  Honour Retry-After before the next
                    # heartbeat; the room will truthfully show unavailable if
                    # its lease expires meanwhile.
                    retry_delay = max(interval, min(error.retry_after, 120.0))
                    deadline = self._lease_deadline.get(binding.room_id, 0)
                    if deadline and asyncio.get_running_loop().time() >= deadline:
                        _connected_rooms.discard(binding.room_id)
                        if not _connected_rooms:
                            self._mark_disconnected()
        finally:
            self._heartbeat_tasks.pop(binding.room_id, None)

    async def _stream_once(self, binding: RoomBinding) -> None:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

        def on_event(event: dict[str, Any]) -> bool:
            if self._stop.is_set():
                return False
            if event:
                loop.call_soon_threadsafe(queue.put_nowait, event)
            return True

        future = asyncio.create_task(self._call(
            binding,
            lambda api: api.stream_events(binding.room_id, binding.cursor, on_event),
        ))
        try:
            while not future.done():
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1)
                except asyncio.TimeoutError:
                    continue
                await self._consume(binding, event)
            result = await future
            while not queue.empty():
                await self._consume(binding, queue.get_nowait())
            # The transport cursor is not the durable acknowledgement cursor.
            # on_processing_complete advances the latter after Hermes finishes.
        finally:
            if not future.done():
                future.cancel()

    async def _consume(self, binding: RoomBinding, event: dict[str, Any]) -> None:
        seq = int(event.get("seq") or 0)
        if seq <= binding.acknowledged_cursor:
            return
        key = str(seq)
        if key not in binding.inbox:
            binding.inbox[key] = "pending"
            binding.pending_since[key] = time.time()
        elif binding.inbox[key] == "pending" and key not in binding.pending_since:
            # Pre-TTL state has no timestamp.  It is necessarily historical,
            # so allow the sweep to release it rather than replaying it
            # forever after an upgrade.
            binding.pending_since[key] = 0.0
        if not self._persist_binding(binding):
            return
        if binding.inbox.get(str(seq)) == "complete":
            # Crash/timeout after durable completion: retry only the idempotent
            # acknowledgement, never the Hermes model run.
            await self._complete_event(binding, seq)
            return
        if event.get("actorId") == binding.membership_id:
            await self._complete_event(binding, seq)
            return
        payload = event.get("payload") or {}
        event_type = str(event.get("type") or "")
        legacy_untyped_message = event_type == "message.posted" and not str(event.get("actorRole") or "")
        human_source = _is_human_cycle_source(event)
        ready_for_self = (
            event_type == "discussion.cycle_attempt_ready"
            and str(payload.get("membershipId") or "") == binding.membership_id
        )
        if event_type != "message.posted" and not human_source and not ready_for_self:
            await self._complete_event(binding, seq)
            return
        if event_type == "message.posted" and not human_source and not legacy_untyped_message and not _agent_event_addresses(payload, binding.membership_id):
            await self._complete_event(binding, seq)
            return
        cycle_attempt: dict[str, Any] | None = None
        if human_source:
            cycle = await self._ensure_discussion_cycle(binding, event)
            if cycle is None:
                await self._complete_event(binding, seq)
                return
            cycle_attempt = await self._claim_discussion_attempt(binding, str(cycle.get("id") or ""))
        elif payload.get("cycleId"):
            cycle_attempt = await self._claim_discussion_attempt(binding, str(payload.get("cycleId")))
        if (human_source or ready_for_self or payload.get("cycleId")) and not cycle_attempt:
            await self._complete_event(binding, seq)
            return
        body = _cycle_source_body(event, payload)
        event_id = str(event.get("id") or "")
        if not body or not event_id:
            await self._complete_event(binding, seq)
            return
        if event_id in self._inflight_events:
            return
        if cycle_attempt:
            cycle_attempts = getattr(self, "_cycle_attempts", None)
            if cycle_attempts is None:
                cycle_attempts = {}
                self._cycle_attempts = cycle_attempts
            cycle_attempts[event_id] = cycle_attempt
        if ready_for_self and payload.get("sourceEventId"):
            response_sources = getattr(self, "_cycle_response_sources", None)
            if response_sources is None:
                response_sources = {}
                self._cycle_response_sources = response_sources
            response_sources[event_id] = str(payload.get("sourceEventId"))
        queued_events = getattr(self, "_queued_events", None)
        if queued_events is None:
            queued_events = {}
            self._queued_events = queued_events
        queued_events.setdefault(binding.room_id, {})[seq] = event
        await self._dispatch_next_queued(binding)

    async def _ensure_discussion_cycle(
        self, binding: RoomBinding, event: dict[str, Any],
    ) -> dict[str, Any] | None:
        state = await self._call(binding, lambda api: api.room_state(binding.room_id))
        active_epoch = state.get("activeEpoch") or {}
        if not active_epoch.get("id"):
            return None
        roster = [
            member for member in state.get("roster", [])
            if member.get("status") == "active"
            and member.get("role") in {"participant_agent", "room_master"}
        ]
        payload = event.get("payload") or {}
        resolved = payload.get("resolvedRecipientMembershipIds")
        if isinstance(resolved, list):
            addressed = {str(value) for value in resolved}
            roster = [member for member in roster if str(member.get("membershipId")) in addressed]
        if event.get("type") == "human.command":
            policy = await self._call(binding, lambda api: api.room_policy(binding.room_id))
            coordinator = str(policy.get("summaryCoordinatorMembershipId") or "")
            roster = [
                member for index, member in enumerate(roster)
                if str(member.get("membershipId")) == coordinator or (not coordinator and index == 0)
            ]
        if not any(str(member.get("membershipId")) == binding.membership_id for member in roster):
            return None
        request = {
            "epochId": str(active_epoch["id"]),
            "sourceEventId": str(event.get("id") or ""),
            "policyDigest": "0" * 64,
            "researchDigest": "0" * 64,
            "roster": [
                {
                    "membershipId": str(member.get("membershipId")),
                    "displayName": str(member.get("displayName") or "Agent"),
                }
                for member in roster
            ],
            "budgets": {
                "totalTurns": 1, "perAgentTurns": 1, "maxContributionBytes": 1,
                "maxCycleBytes": 1, "maxDuration": 1, "maxFollowUps": 0,
            },
            "idempotencyKey": stable_key("cycle", str(event.get("id") or "")),
        }
        return await self._call(
            binding,
            lambda api: api.start_discussion_cycle(binding.room_id, request),
        )

    async def _claim_discussion_attempt(
        self, binding: RoomBinding, cycle_id: str,
    ) -> dict[str, Any] | None:
        try:
            return await self._call(
                binding,
                lambda api: api.claim_discussion_attempt(binding.room_id, cycle_id),
            )
        except ProtocolError as error:
            if error.code in {"cycle_no_attempt", "cycle_superseded"}:
                return None
            raise

    async def _complete_cycle_attempt(
        self,
        binding: RoomBinding,
        cycle_attempt: dict[str, Any],
        action: str,
        event_id: str = "",
    ) -> None:
        cycle, attempt = cycle_attempt["cycle"], cycle_attempt["attempt"]
        payload: dict[str, Any] = {
            "generation": int(cycle["generation"]),
            "action": action,
        }
        if event_id:
            payload["eventId"] = event_id
        await self._call(
            binding,
            lambda api: api.complete_discussion_attempt(
                binding.room_id, str(cycle["id"]), str(attempt["id"]), payload,
            ),
        )

    async def _dispatch_room_event(
        self,
        binding: RoomBinding,
        event: dict[str, Any],
        dispatch_generation: str,
    ) -> bool:
        seq = int(event.get("seq") or 0)
        payload = event.get("payload") or {}
        body = str(payload.get("body") or "").strip()
        event_id = str(event.get("id") or "")
        self._inflight_events.add(event_id)
        self._event_seq.setdefault(binding.room_id, {})[event_id] = seq
        state = await self._call(binding, lambda api: api.room_state(binding.room_id))
        active_epoch = state.get("activeEpoch") or {}
        starts_at = int(active_epoch.get("startsAtSeq") or 0)
        if starts_at and seq < starts_at:
            self._inflight_events.discard(event_id)
            await self._complete_event(binding, seq)
            return False
        self._event_epoch[event_id] = str(active_epoch.get("id") or "")
        self._latest_source[binding.room_id] = event_id
        run_id = "hermes:" + uuid.uuid4().hex
        self._run_for_event[event_id] = run_id
        self._activity_seq[event_id] = 0
        await self._publish(
            binding,
            event_id,
            "context_acknowledged",
            source_seq=seq,
            suppress_errors=True,
        )
        await self._publish(
            binding,
            event_id,
            "lifecycle",
            status="reading_shared_room",
            suppress_errors=True,
        )
        actor = payload.get("actorDisplayName") or event.get("actorRole") or "Room participant"
        cycle_attempt = getattr(self, "_cycle_attempts", {}).get(event_id)
        cycle_context = ""
        if cycle_attempt:
            cycle, attempt = cycle_attempt["cycle"], cycle_attempt["attempt"]
            turn_number = int(cycle.get("totalTurns") or 0) + 1
            total_turns = int((cycle.get("budgets") or {}).get("totalTurns") or 0)
            if turn_number >= total_turns:
                cycle_context = (
                    f"\n\n[Autonomous discussion round {attempt.get('round')}, final turn {turn_number}/{total_turns}] "
                    "Briefly synthesize common ground, differences, and unresolved questions before concluding."
                )
            else:
                cycle_context = (
                    f"\n\n[Autonomous discussion round {attempt.get('round')}, turn {turn_number}/{total_turns}] "
                    "Advance the discussion and directly engage the other participants when useful."
                )
        prompt = (
            f"[Synthetic Sociality Room event {event_id}, canonical sequence {seq}]\n"
            f"{actor}: {body}{cycle_context}\n\n"
            "Respond as your own Hermes identity and character. Address the human by name only when natural, "
            "never claim consensus or speak for another participant. Use the shared context but do not force the room theme. "
            "If no response from you is appropriate, return exactly {\"action\":\"skip\"}. Otherwise answer naturally; "
            "a structured envelope may use {\"action\":\"contribute\",\"body\":\"...\"}."
        )
        dispatch_source = _dispatch_source_ref(event_id, dispatch_generation)
        source = self.build_source(
            chat_id=binding.room_id,
            chat_name="Synthetic Sociality Room",
            chat_type="group",
            # Stable room-scoped identity preserves one shared Hermes context
            # even if a host ignores group_sessions_per_user.
            user_id=binding.room_id,
            user_name=str(actor),
            message_id=dispatch_source,
        )
        await self.handle_message(MessageEvent(
            text=prompt,
            message_type=MessageType.TEXT,
            source=source,
            message_id=dispatch_source,
            raw_message={**event, "_dispatchGeneration": dispatch_generation},
        ))
        return True

    async def on_processing_complete(self, event: MessageEvent, outcome: Any) -> None:
        source_ref = str(getattr(event, "message_id", "") or "")
        terminal_sources = getattr(self, "_terminal_sources", {})
        raw = getattr(event, "raw_message", {}) or {}
        decoded_source, decoded_generation = _decode_dispatch_source(source_ref)
        event_id = str(raw.get("id") or decoded_source)
        dispatch_generation = str(raw.get("_dispatchGeneration") or decoded_generation)
        terminal_delivery = (
            source_ref in terminal_sources
            and terminal_sources.get(source_ref) == dispatch_generation
        )
        if terminal_delivery:
            terminal_sources.pop(source_ref, None)
        seq = int(raw.get("seq") or 0)
        source = getattr(event, "source", None)
        room_id = str(getattr(source, "chat_id", "") or "")
        binding = self._state.binding(room_id)
        if binding is None or not seq:
            event_generations = getattr(self, "_event_dispatch_generation", {})
            if event_generations.get(event_id) == dispatch_generation:
                self._inflight_events.discard(event_id)
                event_generations.pop(event_id, None)
            active_rooms = getattr(self, "_active_dispatch_rooms", {})
            if active_rooms.get(room_id) == dispatch_generation:
                active_rooms.pop(room_id, None)
            return
        completion_error: Exception | None = None
        try:
            if terminal_delivery or str(getattr(outcome, "name", outcome)).lower() == "success":
                await self._complete_event(binding, seq)
            elif cycle_attempt := getattr(self, "_cycle_attempts", {}).get(event_id):
                try:
                    await self._complete_cycle_attempt(binding, cycle_attempt, "pass")
                except ProtocolError as error:
                    if error.code != "cycle_superseded":
                        raise
                getattr(self, "_cycle_attempts", {}).pop(event_id, None)
                getattr(self, "_cycle_response_sources", {}).pop(event_id, None)
                await self._complete_event(binding, seq)
        except Exception as error:
            completion_error = error
        finally:
            active_rooms = getattr(self, "_active_dispatch_rooms", {})
            if active_rooms.get(room_id) == dispatch_generation:
                active_rooms.pop(room_id, None)
            event_generations = getattr(self, "_event_dispatch_generation", {})
            if event_generations.get(event_id) == dispatch_generation:
                self._inflight_events.discard(event_id)
                event_generations.pop(event_id, None)
            await self._dispatch_next_queued(binding)
        if completion_error is not None:
            raise completion_error

    async def _dispatch_next_queued(self, binding: RoomBinding) -> None:
        queued_events = getattr(self, "_queued_events", None)
        if not queued_events:
            return
        active_rooms = getattr(self, "_active_dispatch_rooms", None)
        if active_rooms is None:
            active_rooms = {}
            self._active_dispatch_rooms = active_rooms
        if binding.room_id in active_rooms:
            return
        queued = queued_events.get(binding.room_id, {})
        while queued:
            seq = min(queued)
            event = queued[seq]
            if seq <= binding.acknowledged_cursor or binding.inbox.get(str(seq)) == "complete":
                queued.pop(seq)
                continue
            event_id = str(event.get("id") or "")
            dispatch_generation = uuid.uuid4().hex
            active_rooms[binding.room_id] = dispatch_generation
            event_generations = getattr(self, "_event_dispatch_generation", None)
            if event_generations is None:
                event_generations = {}
                self._event_dispatch_generation = event_generations
            event_generations[event_id] = dispatch_generation
            try:
                dispatched = await self._dispatch_room_event(binding, event, dispatch_generation)
            except Exception:
                self._inflight_events.discard(event_id)
                if active_rooms.get(binding.room_id) == dispatch_generation:
                    active_rooms.pop(binding.room_id, None)
                if event_generations.get(event_id) == dispatch_generation:
                    event_generations.pop(event_id, None)
                raise
            queued.pop(seq, None)
            if dispatched:
                return
            if active_rooms.get(binding.room_id) == dispatch_generation:
                active_rooms.pop(binding.room_id, None)
            if event_generations.get(event_id) == dispatch_generation:
                event_generations.pop(event_id, None)
        queued_events.pop(binding.room_id, None)

    def _successful_terminal(
        self,
        source_ref: str,
        dispatch_generation: str,
        message_id: str | None,
    ) -> SendResult:
        """Remember that Room delivery reached a truthful terminal boundary.

        Hermes can report a non-success processing outcome after an intentional
        interruption even though the adapter correctly cancelled delivery. The
        receive ledger follows the adapter's terminal delivery result, while
        ordinary model or delivery failures remain pending for recovery.
        """
        terminal_sources = getattr(self, "_terminal_sources", None)
        if terminal_sources is None:
            terminal_sources = {}
            self._terminal_sources = terminal_sources
        terminal_sources[source_ref] = dispatch_generation
        return SendResult(success=True, message_id=message_id)

    async def _complete_event(self, binding: RoomBinding, seq: int) -> None:
        ledger_locks = getattr(self, "_ledger_locks", None)
        if ledger_locks is None:
            ledger_locks = {}
            self._ledger_locks = ledger_locks
        lock = ledger_locks.setdefault(binding.room_id, asyncio.Lock())
        async with lock:
            await self._complete_event_locked(binding, seq)

    async def _complete_event_locked(self, binding: RoomBinding, seq: int) -> None:
        if seq <= binding.acknowledged_cursor:
            binding.inbox = {
                key: status for key, status in binding.inbox.items()
                if int(key) > binding.acknowledged_cursor
            }
            binding.pending_since = {
                key: started for key, started in binding.pending_since.items()
                if int(key) > binding.acknowledged_cursor and binding.inbox.get(key) == "pending"
            }
            completed_sources = [
                source_id for source_id, source_seq in binding.turn_sequences.items()
                if source_seq <= binding.acknowledged_cursor
            ]
            for source_id in completed_sources:
                binding.turn_sequences.pop(source_id, None)
                binding.turn_observed.pop(source_id, None)
                binding.delivery_intents.pop(source_id, None)
            self._persist_binding(binding)
            return
        binding.inbox[str(seq)] = "complete"
        binding.pending_since.pop(str(seq), None)
        completed_sources = [
            source_id for source_id, source_seq in binding.turn_sequences.items()
            if source_seq == seq
        ]
        for source_id in completed_sources:
            binding.turn_sequences.pop(source_id, None)
            binding.turn_observed.pop(source_id, None)
            binding.delivery_intents.pop(source_id, None)
        # Persist completion evidence before the network acknowledgement. An
        # ambiguous timeout can then retry the idempotent ack without rerunning
        # Hermes or losing the durable completion marker.
        if not self._persist_binding(binding):
            return
        cursor = binding.acknowledged_cursor
        while binding.inbox.get(str(cursor + 1)) == "complete":
            cursor += 1
        if cursor == binding.acknowledged_cursor:
            return
        response = await self._call(binding, lambda api: api.acknowledge(binding.room_id, cursor))
        authoritative = int((response or {}).get("acknowledgedSeq") or 0)
        cursor = max(binding.acknowledged_cursor, cursor, authoritative)
        binding.acknowledged_cursor = cursor
        binding.cursor = max(binding.cursor, cursor)
        binding.inbox = {
            key: status for key, status in binding.inbox.items()
            if int(key) > cursor
        }
        binding.pending_since = {
            key: started for key, started in binding.pending_since.items()
            if int(key) > cursor and binding.inbox.get(key) == "pending"
        }
        completed_sources = [
            source_id for source_id, source_seq in binding.turn_sequences.items()
            if source_seq <= cursor
        ]
        for source_id in completed_sources:
            binding.turn_sequences.pop(source_id, None)
            binding.turn_observed.pop(source_id, None)
            binding.delivery_intents.pop(source_id, None)
        self._persist_binding(binding)

    async def _expire_stale_pending(self, binding: RoomBinding) -> None:
        """Release receive-ledger entries that can no longer complete.

        The cursor is intentionally ordered, so one abandoned model turn used
        to retain every later event indefinitely.  Expiry is explicit in the
        connector ledger and never fabricates a room message; it only advances
        this connector's acknowledgement cursor.
        """
        now = time.time()
        stale = [
            int(key) for key, status in binding.inbox.items()
            if status == "pending" and now - float(binding.pending_since.get(key, 0.0)) >= PENDING_EVENT_TTL_SECONDS
        ]
        for seq in sorted(stale):
            logger.warning("Room %s expiring unprocessed event sequence %s after %.0fs", binding.room_id, seq, PENDING_EVENT_TTL_SECONDS)
            event_id = next((event_id for event_id, event_seq in self._event_seq.get(binding.room_id, {}).items() if event_seq == seq), "")
            self._inflight_events.discard(event_id)
            active_rooms = getattr(self, "_active_dispatch_rooms", {})
            generation = getattr(self, "_event_dispatch_generation", {}).get(event_id, "")
            if active_rooms.get(binding.room_id) == generation:
                active_rooms.pop(binding.room_id, None)
            if getattr(self, "_event_dispatch_generation", {}).get(event_id) == generation:
                getattr(self, "_event_dispatch_generation", {}).pop(event_id, None)
            await self._complete_event(binding, seq)
            await self._dispatch_next_queued(binding)

    async def _auto_deny_approval(self, chat_id: str) -> None:
        """Resolve a Hermes approval wait privately and fail closed."""
        await asyncio.sleep(0)
        message_id = "private-deny-" + uuid.uuid4().hex
        source = self.build_source(
            chat_id=chat_id,
            chat_name="Synthetic Sociality Room private control",
            chat_type="group",
            user_id=chat_id,
            user_name="Room connector policy",
            message_id=message_id,
        )
        await self.handle_message(MessageEvent(
            text="/deny",
            message_type=MessageType.TEXT,
            source=source,
            message_id=message_id,
            raw_message={"privateControl": True},
        ))

    async def _refresh_binding(self, binding: RoomBinding) -> bool:
        """Observe CLI lifecycle changes made by another process."""
        latest = load().binding(binding.room_id)
        if latest is not None and latest.enabled and not latest.revoked and self._same_generation(latest, binding):
            return True
        binding.enabled = False
        if binding.connector_session_id and not binding.revoked:
            try:
                await self._call(
                    binding,
                    lambda api: api.disconnect(binding.room_id, binding.connector_session_id, False),
                )
            except Exception:
                logger.debug("Room intentional disconnect failed", exc_info=True)
        binding.connector_session_id = ""
        _connected_rooms.discard(binding.room_id)
        self._lease_deadline.pop(binding.room_id, None)
        return False

    def _note_connected(self, binding: RoomBinding, evidence: dict[str, Any]) -> None:
        remaining = 45.0  # server contract fallback when old servers omit expiry
        raw_expiry = str(evidence.get("leaseExpiresAt") or "")
        if raw_expiry:
            try:
                expires = datetime.fromisoformat(raw_expiry.replace("Z", "+00:00"))
                remaining = max(0.0, expires.timestamp() - time.time())
            except ValueError:
                logger.warning("Room %s returned an invalid connector lease expiry", binding.room_id)
        self._lease_deadline[binding.room_id] = asyncio.get_running_loop().time() + remaining
        _connected_rooms.add(binding.room_id)
        self._mark_connected()

    @staticmethod
    def _same_generation(left: RoomBinding, right: RoomBinding) -> bool:
        return (
            left.membership_id == right.membership_id
            and left.credential == right.credential
            and left.installation_id == right.installation_id
        )

    def _binding_generation_active(self, binding: RoomBinding) -> bool:
        latest = load().binding(binding.room_id)
        return bool(
            latest is not None
            and latest.enabled
            and not latest.revoked
            and self._same_generation(latest, binding)
        )

    def _persist_binding(self, binding: RoomBinding) -> bool:
        """Persist runtime fields without overriding CLI lifecycle authority."""
        latest = load()
        existing = latest.binding(binding.room_id)
        if existing is None:
            return False
        same_generation = self._same_generation(existing, binding)
        if not same_generation or not existing.enabled or existing.revoked:
            binding.enabled = existing.enabled
            binding.revoked = existing.revoked
            return False
        existing.connector_session_id = binding.connector_session_id
        existing.cursor = binding.cursor
        existing.acknowledged_cursor = binding.acknowledged_cursor
        existing.inbox = dict(binding.inbox)
        existing.pending_since = dict(binding.pending_since)
        existing.turn_observed = dict(binding.turn_observed)
        existing.turn_sequences = dict(binding.turn_sequences)
        existing.delivery_intents = {
            source_id: dict(intent) for source_id, intent in binding.delivery_intents.items()
        }
        existing.transport = binding.transport
        save(latest)
        return True

    async def _wait_for_turn(
        self,
        binding: RoomBinding,
        turn: dict[str, Any],
        observed_seq: int,
        source_id: str,
        timeout: float,
    ) -> dict[str, Any]:
        # A replay after an ambiguous finish may return the same idempotent
        # turn already in its terminal state. Continue through the idempotent
        # message/finish calls so the original canonical event is recovered.
        if turn.get("state") in {"granted", "finished"}:
            return turn
        if turn.get("state") in {"revoked", "expired"}:
            raise ProtocolError(
                "Room turn is no longer active",
                status=409,
                code="turn_not_active",
                retryable=False,
            )
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.75)
            # Replaying the same idempotent request returns this turn's durable
            # record. Room state only exposes the current grant, so it cannot
            # distinguish a queued turn that was revoked, expired, or replaced.
            current = await self._call(
                binding,
                lambda api: api.request_turn(binding.room_id, observed_seq, source_id),
            )
            if current.get("turnId") != turn.get("turnId"):
                raise ProtocolError(
                    "Idempotent turn replay returned a different turn",
                    status=409,
                    code="idempotency_mismatch",
                    retryable=False,
                )
            if current.get("state") in {"granted", "finished"}:
                return current
            if current.get("state") in {"revoked", "expired"}:
                raise ProtocolError(
                    "Room turn is no longer active",
                    status=409,
                    code="turn_not_active",
                    retryable=False,
                )
        raise ProtocolError("Room turn remained queued until timeout", retryable=True)

    async def _publish(
        self,
        binding: RoomBinding,
        source_id: str,
        kind: str,
        *,
        status: str = "",
        source_seq: int = 0,
        canonical_event_id: str = "",
        suppress_errors: bool = False,
    ) -> None:
        run_id = self._run_for_event.get(source_id) or ("hermes:" + uuid.uuid4().hex)
        self._run_for_event[source_id] = run_id
        stream_seq = self._activity_seq.get(source_id, 0) + 1
        self._activity_seq[source_id] = stream_seq
        payload: dict[str, Any] = {"version": 1, "kind": kind, "runId": run_id, "streamSeq": stream_seq}
        if kind != "heartbeat":
            payload["sourceEventId"] = source_id
        if kind == "context_acknowledged":
            payload["sourceSeq"] = source_seq
        if kind in {"lifecycle", "terminal"}:
            payload["status"] = status
        if canonical_event_id:
            payload["canonicalEventId"] = canonical_event_id
        try:
            await self._call(binding, lambda api: api.activity(binding.room_id, payload))
        except Exception:
            if not suppress_errors:
                raise

    async def _call(self, binding: RoomBinding, operation: Callable[[RoomProtocol], Any]) -> Any:
        return await asyncio.to_thread(operation, RoomProtocol(binding.base_url, binding.credential))

    def _binding(self, room_id: str) -> RoomBinding:
        binding = self._state.binding(room_id)
        if binding is None:
            raise ValueError(f"Room {room_id!r} is not configured for this Hermes profile")
        return binding

    def _revoke(self, binding: RoomBinding) -> None:
        binding.revoked = True
        binding.enabled = False
        binding.connector_session_id = ""
        _connected_rooms.discard(binding.room_id)
        latest = load()
        existing = latest.binding(binding.room_id)
        if existing is not None and existing.membership_id == binding.membership_id:
            existing.revoked = True
            existing.enabled = False
            existing.connector_session_id = ""
            save(latest)
            self._state = latest


def extract_visible_body(content: str) -> str | None:
    """Return only user-facing prose from plain or fenced structured output."""
    value = (content or "").strip()
    fenced = _FENCE.match(value)
    candidate = fenced.group(1).strip() if fenced else value
    if candidate.startswith("{"):
        try:
            envelope, offset = json.JSONDecoder().raw_decode(candidate)
            # A few models append an extra closing brace after an otherwise
            # valid envelope. Accept only that narrow harmless remainder;
            # arbitrary trailing prose remains visible instead of being
            # silently discarded.
            remainder = candidate[offset:].strip()
            if remainder and set(remainder) != {"}"}:
                envelope = None
        except json.JSONDecodeError:
            envelope = None
        if isinstance(envelope, dict):
            action = str(envelope.get("action") or "").lower()
            if action in {"skip", "no_response", "none"}:
                return None
            body = envelope.get("body")
            if isinstance(body, str) and body.strip():
                return body.strip()
        # Envelope-looking output is metadata, never transcript prose.
        return None
    tagged = _ATTRIBUTE_BODY.match(candidate)
    if tagged:
        return html.unescape(tagged.group(1)).strip() or None
    if candidate.lower().startswith("<action"):
        return None
    return value or None


def register(ctx) -> None:
    ctx.register_platform(
        name=NAME,
        label="Synthetic Sociality Room",
        adapter_factory=lambda config: SyntheticSocialityAdapter(config),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        env_enablement_fn=env_enablement,
        required_env=[],
        install_hint="Run `hermes room join` and paste the invitation link at the hidden prompt.",
        emoji="◌",
        max_message_length=0,
        allow_update_command=False,
        pii_safe=False,
        platform_hint="Shared multi-agent Room; speak only as your configured Hermes identity.",
    )
    ctx.register_cli_command(
        name="room",
        help="Join and manage Synthetic Sociality rooms",
        description="Identity-preserving Synthetic Sociality Room connector",
        setup_fn=cli.setup_cli,
        handler_fn=cli.dispatch,
    )

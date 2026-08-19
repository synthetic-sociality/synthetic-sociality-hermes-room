"""Hermes-native platform adapter for Synthetic Sociality Room."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import html
import json
import logging
import os
import platform
import re
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

from . import cli
from .context import canonical_room_context as _canonical_room_context
from .context import recent_room_messages as _recent_room_messages
from .context import room_actor_name as _room_actor_name
from .origin_context import forget_room_context, is_room_context
from .protocol import (
    MESSAGE_LOGICAL_CONTRIBUTION_CAPABILITY,
    ProtocolError,
    RoomProtocol,
    stable_key,
)
from .room_tools import ROOM_CONTEXT_SCHEMA, ROOM_POST_SCHEMA, room_context, room_post
from .state import PluginState, RoomBinding, load, update


def _epoch_thread_id(epoch_id: str) -> str:
    """Return the Hermes thread boundary for one authenticated Room epoch."""
    if not isinstance(epoch_id, str) or not epoch_id.strip():
        raise ValueError("Room dispatch has no active epoch")
    digest = hashlib.sha256(epoch_id.encode("utf-8")).hexdigest()
    return f"room-epoch-v1:{digest}"


def _session_thread_for_epoch(binding: RoomBinding, epoch_id: str) -> str | None:
    if not binding.epoch_session_routing_initialized:
        raise ValueError("Room epoch session routing is not initialized")
    if not isinstance(epoch_id, str) or not epoch_id.strip():
        raise ValueError("Room dispatch has no active epoch")
    if epoch_id == binding.legacy_session_epoch_id:
        return None
    return _epoch_thread_id(epoch_id)


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
# A room message that has not completed within this period is retried. Time is
# never terminal evidence and therefore can never advance acknowledgement.
PENDING_EVENT_TTL_SECONDS = 180.0
PENDING_EVENT_MAX_RETRIES = 2
TERMINAL_EVENT_STATES = frozenset({"posted", "skipped", "cancelled", "superseded", "ignored"})
_DISPATCH_SOURCE_PREFIX = "room-dispatch:"


def _on_pre_llm_call(**kwargs: Any) -> None:
    is_room_context(**kwargs)


def _on_pre_tool_call(
    tool_name: str = "", **kwargs: Any,
) -> dict[str, str] | None:
    if tool_name != "synthetic_sociality_room_post" or not is_room_context(**kwargs):
        return None
    return {
        "action": "block",
        "message": (
            "This is already a Synthetic Sociality Room-origin turn. The platform adapter owns "
            "its single canonical delivery path. Do not call synthetic_sociality_room_post, do "
            "not retry or describe this block, and return the contribution directly as the final "
            "response so the adapter can post it exactly once."
        ),
    }


def _on_session_end(**kwargs: Any) -> None:
    forget_room_context(kwargs.get("session_id"), kwargs.get("turn_id"))


def _on_session_finalize(**kwargs: Any) -> None:
    forget_room_context(
        kwargs.get("session_id"), kwargs.get("turn_id"), forget_session=True,
    )


def _valid_canonical_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})",
        value,
    ):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _with_idempotency_key(api: Any, key: str) -> Any:
    """Apply a persisted key when the concrete protocol supports it.

    Small test doubles and older embedded protocol shims remain compatible;
    the production RoomProtocol always supports the exact-key override.
    """
    binder = getattr(api, "with_idempotency_key", None)
    return binder(key) if callable(binder) else api


def _with_logical_contribution_id(api: Any, logical_contribution_id: str) -> Any:
    """Bind logical identity when supported without breaking older shims."""
    binder = getattr(api, "with_logical_contribution_id", None)
    return binder(logical_contribution_id) if callable(binder) else api


def _with_message_payload_dialect(api: Any, payload_dialect: str) -> Any:
    """Apply a frozen dialect while retaining older test/protocol shims."""
    binder = getattr(api, "with_message_payload_dialect", None)
    return binder(payload_dialect) if callable(binder) else api


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
    # agent_owner is the second non-admin human membership (MMB/Yubao-shaped
    # humans), not an external agent (SS-134 / P0-F). Both human_owner and
    # agent_owner are human conversation sources.
    if role not in {"human", "human_owner", "agent_owner"}:
        return False
    if event.get("type") == "message.posted":
        return True
    if event.get("type") == "discussion.started":
        return True
    command = (event.get("payload") or {}).get("command") or {}
    return event.get("type") == "human.command" and command.get("command") in {"ask", "summarize"}


def _is_agent_cycle_seed(event: dict[str, Any]) -> bool:
    """Return true only for an eligible canonical agent contribution.

    A message already carrying cycleId belongs to an active persisted cycle
    and must never recursively create another one. Explicit server-resolved
    recipients are required; transcript names and @ prose are irrelevant.
    """
    if event.get("type") != "message.posted" or str(event.get("actorRole") or "") not in {"participant_agent", "room_master"}:
        return False
    payload = event.get("payload") or {}
    resolved = payload.get("resolvedRecipientMembershipIds")
    return not str(payload.get("cycleId") or "").strip() and isinstance(resolved, list) and bool(resolved)


def _is_peer_contribution(event: dict[str, Any], membership_id: str) -> bool:
    return (
        event.get("type") == "message.posted"
        and str(event.get("actorRole") or "") in {"participant_agent", "room_master"}
        and str(event.get("actorId") or "") != membership_id
    )


def _agent_event_addresses(payload: dict[str, Any], membership_id: str) -> bool:
    resolved = payload.get("resolvedRecipientMembershipIds")
    if isinstance(resolved, list):
        return membership_id in {str(value) for value in resolved}
    for selector in payload.get("recipientSelectors") or []:
        if selector.get("kind") == "everyone" or str(selector.get("membershipId") or "") == membership_id:
            return True
    return False


def _policy_view(response: dict[str, Any]) -> dict[str, Any]:
    """Normalize direct and enveloped policy responses across server versions."""
    nested = response.get("policy")
    return nested if isinstance(nested, dict) else response


def _room_policy(api: Any, room_id: str) -> dict[str, Any]:
    reader = getattr(api, "room_policy", None)
    return reader(room_id) if callable(reader) else {"coordinationMode": "coordinated"}


def _acknowledge_peer(api: Any, room_id: str, source_event_id: str) -> dict[str, Any]:
    reader = getattr(api, "acknowledge_peer_contribution", None)
    return reader(room_id, source_event_id) if callable(reader) else {}


def _cycle_source_body(event: dict[str, Any], payload: dict[str, Any]) -> str:
    if event.get("type") == "discussion.cycle_attempt_ready":
        return (
            "Continue the autonomous discussion from the shared canonical Room context. "
            "Add a meaningful new point; do not repeat prior contributions."
        )
    if event.get("type") == "human.command":
        command = payload.get("command") or {}
        if str(command.get("command") or "") == "summarize":
            return (
                "Wrap up this discussion now. Synthesize common ground, disagreements, "
                "unresolved questions, and attribute positions accurately."
            )
        if str(command.get("command") or "") == "ask":
            return str((command.get("arguments") or {}).get("instruction") or "").strip()
        return str(payload.get("visibleText") or "").strip()
    if event.get("type") == "discussion.started":
        topic = ((payload.get("epoch") or {}).get("topic") or {})
        title = str(topic.get("title") or "").strip()
        description = str(topic.get("description") or "").strip()
        return "\n\n".join(value for value in (title, description) if value)
    if event.get("type") == "discussion.cycle_terminal":
        return (
            "The bounded discussion is complete. Read every canonical contribution in the shared "
            "Room context and provide one concise overall synthesis. Attribute positions accurately, "
            "separate common ground from disagreement, and name unresolved questions."
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


def _cycle_phase_instruction(attempt: dict[str, Any], cycle: dict[str, Any], payload: dict[str, Any]) -> tuple[str, str]:
    round_number = int(attempt.get("round") or payload.get("round") or 1)
    command = payload.get("command") or {}
    summary_requested = isinstance(command, dict) and str(command.get("command") or "") == "summarize"
    initial_greeting = isinstance(command, dict) and str(command.get("idempotencyKey") or "").startswith("room-initial-greeting:v1:")
    if initial_greeting:
        phase = "initial_greeting"
    elif summary_requested:
        phase = "summary"
    elif round_number == 1:
        phase = "opening"
    else:
        phase = "follow_up"
    instructions = {
        "initial_greeting": "Greet the named human participants once, briefly and naturally. Speak only as yourself, acknowledge every named person, and do not begin a wider exchange.",
        "opening": "Respond naturally to the source message from your own perspective. A brief acknowledgement is enough for a greeting. Do not manufacture a debate, mandate, or task that the message did not request.",
        "follow_up": "Add a response only if it contributes a meaningful new point, answers an explicit question, or resolves a useful disagreement. Look for genuine common ground or synthesis where the claims support it, but never force consensus; justified disagreement may remain. Otherwise pass. The remaining turn budget is a safety ceiling, not a target to exhaust.",
        "summary": "Synthesize only the discussion that actually occurred: common ground, disagreements, unresolved questions, and model-attributed positions. Do not invent consensus or unrelated recommendations.",
    }
    return phase, instructions[phase]


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


def _cycle_attempt_owner_key(
    cycle_attempt: dict[str, Any], membership_id: str,
) -> str:
    cycle = cycle_attempt.get("cycle") or {}
    attempt = cycle_attempt.get("attempt") or {}
    return "|".join((
        str(cycle.get("id") or ""),
        str(int(cycle.get("generation") or 0)),
        str(attempt.get("id") or ""),
        str(membership_id or attempt.get("membershipId") or ""),
    ))


def _release_cycle_attempt_owner(binding: RoomBinding, source_id: str) -> None:
    if not source_id:
        return
    binding.cycle_attempt_owners = {
        key: owner for key, owner in binding.cycle_attempt_owners.items()
        if owner != source_id
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
        self._receive_locks: dict[str, asyncio.Lock] = {}
        self._terminal_sources: dict[str, str] = {}
        self._terminal_results: dict[str, dict[str, str]] = {}
        self._submission_tasks: dict[str, asyncio.Task] = {}
        self._lease_deadline: dict[str, float] = {}
        self._cycle_attempts: dict[str, dict[str, Any]] = {}
        self._cycle_response_sources: dict[str, str] = {}
        self._attempt_renewal_tasks: dict[str, asyncio.Task] = {}
        self._superseded_sources: set[str] = set()
        self._source_coordination_modes: dict[str, str] = {}
        self._open_reply_recipients: dict[str, list[str]] = {}
        self._message_contract_checked: set[str] = set()

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
        for task in getattr(self, "_attempt_renewal_tasks", {}).values():
            task.cancel()
        for task in getattr(self, "_submission_tasks", {}).values():
            task.cancel()
        await asyncio.gather(
            *self._tasks.values(),
            *self._heartbeat_tasks.values(),
            *getattr(self, "_attempt_renewal_tasks", {}).values(),
            *getattr(self, "_submission_tasks", {}).values(),
            return_exceptions=True,
        )
        self._tasks.clear()
        self._heartbeat_tasks.clear()
        getattr(self, "_attempt_renewal_tasks", {}).clear()
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
        getattr(self, "_receive_locks", {}).clear()
        self._terminal_sources.clear()
        getattr(self, "_terminal_results", {}).clear()
        getattr(self, "_submission_tasks", {}).clear()
        getattr(self, "_cycle_attempts", {}).clear()
        getattr(self, "_cycle_response_sources", {}).clear()
        getattr(self, "_source_coordination_modes", {}).clear()
        getattr(self, "_open_reply_recipients", {}).clear()
        getattr(self, "_superseded_sources", set()).clear()
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

    async def _send_with_retry(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Any = None,
        max_retries: int = 2,
        base_delay: float = 2.0,
    ) -> SendResult:
        """Let the Room delivery ledger own every retry.

        ``send`` converges callbacks on one durable intent and the protocol
        layer replays only its frozen request. Hermes' generic fallback would
        otherwise open a second semantic send call with changed prose after a
        deterministic schema error. Room failures therefore return directly
        to the ledger for quarantine/fail-closed handling.
        """
        del max_retries, base_delay
        return await self.send(
            chat_id=chat_id, content=content, reply_to=reply_to, metadata=metadata,
        )

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
        # Keep the last complete buffer visible until the shared submission
        # owner returns. A concurrent processing-complete callback then joins
        # the same task instead of either losing the final text or posting it
        # twice.
        final_content = self._buffered_output.get(message_id, content)
        return await self._send_final(chat_id, source_id, final_content)

    async def _send_final(self, chat_id: str, source_ref: str, content: str) -> SendResult:
        """Converge final send/edit/completion callbacks on one submission."""
        source_id, dispatch_generation = _decode_dispatch_source(source_ref)
        cached = getattr(self, "_terminal_results", {}).get(source_ref)
        if cached and cached.get("generation") == dispatch_generation:
            await self._complete_terminal_send(chat_id, source_id, cached)
            return SendResult(success=True, message_id=cached.get("message_id") or None)
        tasks = getattr(self, "_submission_tasks", None)
        if tasks is None:
            tasks = {}
            self._submission_tasks = tasks
        task = tasks.get(source_ref)
        if task is None:
            task = asyncio.create_task(
                self._send_final_owned(chat_id, source_ref, content),
                name=f"synthetic-sociality:submit:{chat_id}:{source_id}",
            )
            tasks[source_ref] = task
        try:
            result = await task
            if result.success:
                terminal = getattr(self, "_terminal_results", {}).get(source_ref) or {}
                if terminal.get("generation") == dispatch_generation:
                    await self._complete_terminal_send(chat_id, source_id, terminal)
                buffered_sources = getattr(self, "_buffered_source", {})
                buffered_output = getattr(self, "_buffered_output", {})
                for message_id in [
                    key for key, value in buffered_sources.items() if value == source_ref
                ]:
                    buffered_sources.pop(message_id, None)
                    buffered_output.pop(message_id, None)
            return result
        finally:
            if tasks.get(source_ref) is task and task.done():
                tasks.pop(source_ref, None)

    async def _complete_terminal_send(
        self, chat_id: str, source_id: str, terminal: dict[str, Any],
    ) -> None:
        """Persist terminal delivery regardless of Hermes callback ordering.

        Some runtimes report processing completion before their final callback.
        The final callback is therefore also an acknowledgement owner. This is
        idempotent with ``on_processing_complete`` and prevents a late selected
        intent from surviving after the canonical source cursor has advanced.
        """
        status = str(terminal.get("status") or "")
        if status not in TERMINAL_EVENT_STATES:
            return
        state = getattr(self, "_state", None)
        if state is None:
            # Lightweight unit harnesses may exercise output mapping without
            # constructing connector state. Production adapters always own a
            # PluginState and therefore always take the durable completion.
            return
        binding = state.binding(chat_id)
        if binding is None:
            return
        intent = binding.delivery_intents.get(source_id) or {}
        selected = intent.get("selected") or {}
        seq = int(
            selected.get("source_seq") or binding.turn_sequences.get(source_id)
            or getattr(self, "_event_seq", {}).get(chat_id, {}).get(source_id, 0)
        )
        if seq < 1:
            return
        await self._complete_event(
            binding,
            seq,
            terminal_status=status,
            source_id=source_id,
            canonical_event_id=str(terminal.get("canonical_event_id") or ""),
            reason=str(terminal.get("reason") or ""),
        )

    @staticmethod
    def _intent_binding(binding: RoomBinding) -> dict[str, Any]:
        return {
            "membership_id": binding.membership_id,
            "installation_id": binding.installation_id,
            "identity_version": binding.identity_version,
        }

    @classmethod
    def _intent_matches_binding(cls, binding: RoomBinding, intent: dict[str, Any]) -> bool:
        generation = intent.get("binding")
        if not isinstance(generation, dict):
            return True  # pre-1.0.18 intent; runtime generation checks still apply
        return generation == cls._intent_binding(binding)

    @staticmethod
    def _recoverable_selection(intent: dict[str, Any]) -> dict[str, Any] | None:
        selected = intent.get("selected")
        if isinstance(selected, dict) and selected.get("action") in {"post", "skip"}:
            return selected
        post = intent.get("post")
        if isinstance(post, dict) and "body" in post:
            cycle = dict(post.get("cycle") or {})
            recipients = list(post.get("recipient_membership_ids") or cycle.get("recipients") or [])
            return {
                "action": "post",
                "body": str(post.get("body") or ""),
                "responds_to": str(post.get("responds_to") or cycle.get("responds_to") or ""),
                "recipient_membership_ids": recipients,
                "coordination_mode": str(post.get("coordination_mode") or "coordinated"),
                "observed_seq": int(post.get("observed_seq") or 0),
                "observed_epoch_id": str(post.get("observed_epoch_id") or ""),
                "message_payload_dialect": SyntheticSocialityAdapter._persisted_payload_dialect(post),
                "cycle": cycle,
                "binding": post.get("binding"),
            }
        return None

    @classmethod
    def _activate_legacy_canonical_receipt(
        cls,
        binding: RoomBinding,
        source_id: str,
        seq: int,
    ) -> bool:
        """Promote only a complete legacy receipt tied to the active binding.

        The canonical event ID, positive sequence, and timestamp prove that
        delivery is already complete. Promotion therefore enables lifecycle-only
        recovery and can never reopen message submission. Incomplete, malformed,
        foreign, or already migrated state remains untouched.
        """
        key = str(seq)
        intent = binding.delivery_intents.get(source_id) or {}
        selected = intent.get("selected")
        post = intent.get("post")
        canonical = intent.get("canonical_event")
        expected_binding = cls._intent_binding(binding)
        canonical_seq = canonical.get("seq") if isinstance(canonical, dict) else None
        cycle = post.get("cycle") if isinstance(post, dict) else None
        has_cycle = (
            isinstance(cycle, dict)
            and isinstance(cycle.get("cycle_id"), str) and bool(cycle["cycle_id"])
            and isinstance(cycle.get("attempt_id"), str) and bool(cycle["attempt_id"])
            and type(cycle.get("generation")) is int and cycle["generation"] >= 0
        )
        has_turn = (
            isinstance(post, dict)
            and isinstance(post.get("turn_id"), str)
            and bool(post["turn_id"])
        )
        exact = (
            type(seq) is int and seq > 0
            and seq > binding.acknowledged_cursor
            and isinstance(selected, dict)
            and selected.get("action") == "post"
            and selected.get("source_event_id") == source_id
            and type(selected.get("source_seq")) is int
            and selected["source_seq"] == seq
            and isinstance(selected.get("body"), str)
            and bool(selected["body"])
            and selected.get("binding") == expected_binding
            and isinstance(post, dict)
            and post.get("body") == selected.get("body")
            and post.get("binding") == expected_binding
            and isinstance(canonical, dict)
            and isinstance(canonical.get("id"), str)
            and bool(canonical["id"])
            and type(canonical_seq) is int
            and canonical_seq > 0
            and _valid_canonical_timestamp(canonical.get("ts"))
            and "delivery_state" not in intent
            and not intent.get("migration")
            and not (has_cycle and has_turn)
        )
        if not exact:
            return False
        completion_pending = has_cycle or has_turn
        completion: dict[str, Any] | None = None
        if has_cycle:
            completion = {
                "kind": "cycle",
                "cycle_id": str(cycle["cycle_id"]),
                "attempt_id": str(cycle["attempt_id"]),
                "payload": {
                    "generation": int(cycle["generation"]),
                    "action": "contribute",
                    "eventId": str(canonical["id"]),
                },
            }
        elif has_turn:
            completion = {
                "kind": "turn",
                "turn_id": str(post["turn_id"]),
                "observed_seq": canonical_seq,
                "source_event_id": source_id,
                "idempotency_key": str(
                    selected.get("finish_idempotency_key")
                    or stable_key("finish", source_id, room_id=binding.room_id, membership_id=binding.membership_id)
                ),
            }
        intent["delivery_state"] = "posted"
        intent["lifecycle_state"] = "pending" if completion_pending else "not_required"
        intent["state"] = "lifecycle_pending" if completion_pending else "posted"
        intent["migration"] = "legacy-complete-canonical-receipt"
        binding.delivery_lifecycle[source_id] = {
            "state": "lifecycle_pending" if completion_pending else "posted",
            "delivery_state": "posted",
            "lifecycle_state": "pending" if completion_pending else "not_required",
            "receipt": {
                "source_event_id": source_id,
                "source_seq": seq,
                "canonical_event_id": canonical["id"],
                "canonical_seq": canonical_seq,
                "canonical_ts": canonical["ts"],
            },
            "completion": copy.deepcopy(completion or {}),
            "attempts": 0,
            "automatic_retry": completion_pending,
            "binding": expected_binding,
        }
        binding.inbox[key] = "pending"
        binding.pending_since[key] = time.time()
        binding.pending_retries[key] = 0
        return True

    @classmethod
    def _activate_legacy_post_commit_recovery(
        cls,
        binding: RoomBinding,
        source_id: str,
        seq: int,
    ) -> bool:
        """Recognise only the proven 1.0.35 post-commit quarantine signature.

        The legacy state lacks a canonical event receipt, so it cannot be
        relabelled ``posted`` locally. It is instead reopened for one exact,
        idempotent replay of the frozen post; the server response reconstructs
        the receipt without a model run. Every other quarantine remains closed.
        """
        key = str(seq)
        intent = binding.delivery_intents.get(source_id) or {}
        selected = intent.get("selected")
        post = intent.get("post")
        selected_cycle = dict((selected or {}).get("cycle") or {}) if isinstance(selected, dict) else {}
        post_cycle = dict((post or {}).get("cycle") or {}) if isinstance(post, dict) else {}
        expected_binding = cls._intent_binding(binding)
        expected_message_key = stable_key(
            "message", source_id, room_id=binding.room_id, membership_id=binding.membership_id,
        )
        cycle_attempt = {
            "cycle": {
                "id": str(post_cycle.get("cycle_id") or ""),
                "generation": int(post_cycle.get("generation") or 0),
            },
            "attempt": {"id": str(post_cycle.get("attempt_id") or "")},
        }
        expected_owner_key = _cycle_attempt_owner_key(cycle_attempt, binding.membership_id)
        exact = (
            seq > binding.acknowledged_cursor
            and binding.inbox.get(key) == "quarantined"
            and key not in binding.terminal_evidence
            and isinstance(selected, dict)
            and selected.get("action") == "post"
            and selected.get("source_event_id") == source_id
            and int(selected.get("source_seq") or 0) == seq
            and int(selected.get("observed_seq") or 0) == seq
            and str(selected.get("body") or "") != ""
            and selected.get("binding") == expected_binding
            and str(selected.get("message_idempotency_key") or "") == expected_message_key
            and isinstance(post, dict)
            and str(post.get("body") or "") == str(selected.get("body") or "")
            and int(post.get("observed_seq") or 0) == int(selected.get("observed_seq") or 0)
            and post.get("binding") == expected_binding
            and str(post.get("idempotency_key") or "") == expected_message_key
            and bool(str(post_cycle.get("cycle_id") or ""))
            and bool(str(post_cycle.get("attempt_id") or ""))
            and post_cycle.get("cycle_id") == selected_cycle.get("cycle_id")
            and post_cycle.get("attempt_id") == selected_cycle.get("attempt_id")
            and int(post_cycle.get("generation") or 0) == int(selected_cycle.get("generation") or 0)
            and binding.cycle_attempt_owners.get(expected_owner_key) == source_id
            and intent.get("state") == "quarantined"
            and "delivery_state" not in intent
            and intent.get("last_error_code") == "cycle_conflict"
            and not (intent.get("canonical_event") or {}).get("id")
        )
        if not exact:
            return False
        intent["legacy_post_commit_error"] = {
            "code": str(intent.get("last_error_code") or ""),
            "error": str(intent.get("last_error") or "")[:1000],
            "failed_at": intent.get("failed_at"),
        }
        intent["delivery_state"] = "delivery_pending"
        intent["lifecycle_state"] = "not_started"
        intent["state"] = "delivery_pending"
        intent["migration"] = "hermes-1.0.35-post-commit-cycle-conflict"
        binding.inbox[key] = "pending"
        binding.pending_since[key] = time.time()
        binding.pending_retries[key] = 0
        return True

    @staticmethod
    def _persisted_payload_dialect(value: dict[str, Any]) -> str:
        """Recover old frozen intent shapes without changing their request.

        Hermes 1.0.34 introduced ``logical_contribution_id`` in the durable
        intent at the same time that its protocol began sending the v2 field.
        Older shapes lack both. This is migration of recorded request history,
        not capability detection and never depends on a failed write.
        """
        dialect = str(value.get("message_payload_dialect") or "")
        if dialect in {"v1", "v2"}:
            return dialect
        return "v2" if "logical_contribution_id" in value else "v1"

    def _select_delivery_intent(
        self,
        binding: RoomBinding,
        source_id: str,
        body: str | None,
        responds_to_id: str,
        recipient_membership_ids: list[str],
        coordination_mode: str,
        observed_seq: int,
        observed_epoch_id: str,
        cycle_attempt: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Durably freeze model output and all stable delivery inputs."""
        intent = binding.delivery_intents.setdefault(source_id, {})
        selected = intent.get("selected")
        if isinstance(selected, dict):
            if not self._intent_matches_binding(binding, selected):
                raise ProtocolError(
                    "Room delivery intent belongs to a replaced binding",
                    code="binding_changed",
                    retryable=False,
                )
            if "message_payload_dialect" not in selected:
                selected["message_payload_dialect"] = self._persisted_payload_dialect(selected)
                if not self._persist_binding(binding):
                    raise ProtocolError(
                        "Room membership changed before delivery dialect migration was saved",
                        code="binding_changed",
                        retryable=False,
                    )
            return selected

        persisted_post = intent.get("post")
        cycle_payload = (
            dict(persisted_post.get("cycle") or {})
            if isinstance(persisted_post, dict)
            else _cycle_delivery_payload(cycle_attempt, binding.membership_id)
        )
        if responds_to_id and responds_to_id != source_id:
            cycle_payload["responds_to"] = responds_to_id
        if recipient_membership_ids:
            cycle_payload["recipients"] = list(recipient_membership_ids)
        recipients = list(cycle_payload.get("recipients") or [])
        responds_to = str(cycle_payload.get("responds_to") or responds_to_id or source_id)
        if isinstance(persisted_post, dict):
            body = str(persisted_post.get("body") or "")
            coordination_mode = str(persisted_post.get("coordination_mode") or coordination_mode)
            observed_seq = int(persisted_post.get("observed_seq") or observed_seq)
            observed_epoch_id = str(persisted_post.get("observed_epoch_id") or observed_epoch_id)
        selected = {
            "state": "selected",
            "action": "post" if body is not None else "skip",
            "source_event_id": source_id,
            "source_seq": int(
                getattr(self, "_event_seq", {}).get(binding.room_id, {}).get(source_id, 0)
            ),
            "body": str(body or ""),
            "responds_to": responds_to,
            "recipient_membership_ids": recipients,
            "contribution_type": "question" if recipients else "claim",
            "coordination_mode": coordination_mode,
            "observed_seq": int(observed_seq),
            "observed_epoch_id": observed_epoch_id,
            "message_idempotency_key": stable_key(
                "message", source_id, room_id=binding.room_id, membership_id=binding.membership_id,
            ),
            "logical_contribution_id": stable_key(
                "logical-contribution", responds_to,
                room_id=binding.room_id, membership_id=binding.membership_id,
            ),
            "message_payload_dialect": binding.message_payload_dialect,
            "turn_idempotency_key": stable_key(
                "turn", source_id, room_id=binding.room_id, membership_id=binding.membership_id,
            ),
            "finish_idempotency_key": stable_key(
                "finish", source_id, room_id=binding.room_id, membership_id=binding.membership_id,
            ),
            "cycle": cycle_payload,
            "binding": self._intent_binding(binding),
            "selected_at": time.time(),
        }
        intent["selected"] = selected
        intent["delivery_state"] = "selected"
        intent["lifecycle_state"] = "not_started"
        intent["state"] = "selected"
        if selected["source_seq"]:
            binding.turn_sequences[source_id] = int(selected["source_seq"])
        if not self._persist_binding(binding):
            raise ProtocolError(
                "Room membership changed before delivery selection was saved",
                code="binding_changed",
                retryable=False,
            )
        return selected

    def _record_canonical_delivery(
        self,
        binding: RoomBinding,
        source_id: str,
        event: dict[str, Any],
        *,
        completion: dict[str, Any] | None,
    ) -> None:
        """Persist the canonical delivery boundary before lifecycle finalisation."""
        raw_event_id = event.get("id")
        raw_sequence = event.get("seq")
        raw_timestamp = event.get("ts")
        if (
            not isinstance(raw_event_id, str) or not raw_event_id
            or type(raw_sequence) is not int or raw_sequence <= 0
            or not _valid_canonical_timestamp(raw_timestamp)
        ):
            raise ValueError("canonical Room delivery requires event ID, sequence, and timestamp")
        canonical_event_id = raw_event_id
        canonical_seq = raw_sequence
        canonical_ts = raw_timestamp
        canonical_event = {
            "id": canonical_event_id,
            "seq": canonical_seq,
            "ts": canonical_ts,
        }
        previous_intent = copy.deepcopy(binding.delivery_intents.get(source_id))
        previous_journal = copy.deepcopy(binding.delivery_lifecycle.get(source_id))
        intent = binding.delivery_intents.setdefault(source_id, {})
        intent["delivery_state"] = "posted"
        intent["canonical_event"] = canonical_event
        intent["lifecycle_state"] = "pending" if completion else "not_required"
        intent["state"] = "lifecycle_pending" if completion else "posted"
        binding.delivery_lifecycle[source_id] = {
            "state": "lifecycle_pending" if completion else "posted",
            "delivery_state": "posted",
            "lifecycle_state": "pending" if completion else "not_required",
            "receipt": {
                "source_event_id": source_id,
                "source_seq": int((intent.get("selected") or {}).get("source_seq") or 0),
                "canonical_event_id": canonical_event_id,
                "canonical_seq": canonical_event["seq"],
                "canonical_ts": canonical_event["ts"],
            },
            "completion": dict(completion or {}),
            "attempts": 0,
            "automatic_retry": bool(completion),
            "binding": self._intent_binding(binding),
        }
        for key in ("last_error", "last_error_code", "failed_at"):
            intent.pop(key, None)
        def rollback() -> None:
            if previous_intent is None:
                binding.delivery_intents.pop(source_id, None)
            else:
                binding.delivery_intents[source_id] = previous_intent
            if previous_journal is None:
                binding.delivery_lifecycle.pop(source_id, None)
            else:
                binding.delivery_lifecycle[source_id] = previous_journal

        try:
            persisted = self._persist_binding(binding)
        except Exception:
            rollback()
            raise
        if not persisted:
            rollback()
            raise ProtocolError(
                "Canonical Room receipt could not be persisted",
                code="receipt_persist_failed",
                retryable=True,
            )

    def _record_lifecycle_attempt(self, binding: RoomBinding, source_id: str) -> None:
        journal = binding.delivery_lifecycle.get(source_id) or {}
        if journal.get("delivery_state") != "posted":
            raise ValueError("lifecycle attempt requires canonical delivery evidence")
        attempts = int(journal.get("attempts") or 0) + 1
        if attempts > 3:
            raise ValueError("automatic lifecycle attempt budget exhausted")
        journal["attempts"] = attempts
        journal["last_attempt_at"] = time.time()
        binding.delivery_lifecycle[source_id] = journal
        if not self._persist_binding(binding):
            raise ProtocolError("Room membership changed before lifecycle attempt", code="binding_changed", retryable=False)

    def _record_lifecycle_complete(self, binding: RoomBinding, source_id: str) -> None:
        previous_intents = copy.deepcopy(binding.delivery_intents)
        previous_lifecycle = copy.deepcopy(binding.delivery_lifecycle)
        previous_owners = copy.deepcopy(binding.cycle_attempt_owners)
        try:
            intent = binding.delivery_intents.get(source_id) or {}
            if intent.get("delivery_state") == "posted":
                intent["lifecycle_state"] = "complete"
                intent["state"] = "posted"
                intent.pop("lifecycle_error", None)
                intent.pop("lifecycle_error_code", None)
                intent.pop("lifecycle_failed_at", None)
            binding.delivery_lifecycle.pop(source_id, None)
            if not self._persist_binding(binding):
                raise ProtocolError(
                    "Room lifecycle completion could not be persisted",
                    code="lifecycle_persist_failed", retryable=True,
                )
        except Exception:
            binding.delivery_intents = previous_intents
            binding.delivery_lifecycle = previous_lifecycle
            binding.cycle_attempt_owners = previous_owners
            raise

    def _record_lifecycle_pending(
        self,
        binding: RoomBinding,
        source_id: str,
        error: Exception,
        error_code: str = "",
    ) -> None:
        previous_intents = copy.deepcopy(binding.delivery_intents)
        previous_lifecycle = copy.deepcopy(binding.delivery_lifecycle)
        previous_owners = copy.deepcopy(binding.cycle_attempt_owners)
        try:
            intent = binding.delivery_intents.get(source_id) or {}
            journal = binding.delivery_lifecycle.get(source_id) or {}
            receipt = journal.get("receipt") or {}
            if journal.get("delivery_state") != "posted" or not receipt.get("canonical_event_id"):
                raise ValueError("lifecycle failure requires canonical delivery evidence")
            if intent and intent.get("delivery_state") != "posted":
                raise ValueError("lifecycle failure cannot downgrade an unposted delivery")
            attempts = int(journal.get("attempts") or 0)
            retryable = bool(getattr(error, "retryable", False))
            automatic_retry = retryable and attempts < 3
            lifecycle_state = "pending" if automatic_retry else "blocked"
            state = "lifecycle_pending" if automatic_retry else "lifecycle_blocked"
            intent["lifecycle_state"] = lifecycle_state
            intent["state"] = state
            intent["lifecycle_automatic_retry"] = automatic_retry
            intent["lifecycle_error"] = str(error)[:1000]
            intent["lifecycle_error_code"] = str(error_code or "")[:100]
            intent["lifecycle_failed_at"] = time.time()
            journal["state"] = state
            journal["delivery_state"] = "posted"
            journal["lifecycle_state"] = lifecycle_state
            journal["automatic_retry"] = automatic_retry
            journal["last_error"] = str(error)[:1000]
            journal["last_error_code"] = str(error_code or "")[:100]
            journal["failed_at"] = time.time()
            binding.delivery_lifecycle[source_id] = journal
            _release_cycle_attempt_owner(binding, source_id)
            if not self._persist_binding(binding):
                raise ProtocolError(
                    "Room lifecycle debt could not be persisted",
                    code="lifecycle_persist_failed", retryable=True,
                )
        except Exception:
            binding.delivery_intents = previous_intents
            binding.delivery_lifecycle = previous_lifecycle
            binding.cycle_attempt_owners = previous_owners
            raise

    def _record_lifecycle_failure_after_receipt(
        self,
        binding: RoomBinding,
        source_id: str,
        error: Exception,
        *,
        error_code: str,
    ) -> None:
        """Best-effort lifecycle debt recording after durable delivery.

        Storage can fail after the canonical receipt has already committed. That
        failure may lose lifecycle retry metadata, but it can never turn the
        visible delivery into a failed send or trigger a fallback/repost.
        """
        try:
            self._record_lifecycle_pending(binding, source_id, error, error_code=error_code)
        except Exception:
            intent = binding.delivery_intents.get(source_id)
            if isinstance(intent, dict):
                intent["delivery_state"] = "posted"
                intent.setdefault("canonical_event", {})
                intent["lifecycle_state"] = "blocked"
                intent["state"] = "lifecycle_blocked"
                intent["lifecycle_automatic_retry"] = False
            journal = binding.delivery_lifecycle.get(source_id)
            if isinstance(journal, dict) and journal.get("delivery_state") == "posted":
                journal["lifecycle_state"] = "blocked"
                journal["state"] = "lifecycle_blocked"
                journal["automatic_retry"] = False
            logger.exception(
                "Room %s could not persist lifecycle debt for already-posted source %s; delivery remains posted",
                binding.room_id, source_id,
            )

    async def _send_final_owned(self, chat_id: str, source_ref: str, content: str) -> SendResult:
        source_id, dispatch_generation = _decode_dispatch_source(source_ref)
        binding = self._binding(chat_id)
        if not self._binding_generation_active(binding):
            return SendResult(success=False, error="Room membership was disabled, removed, or replaced")
        body = extract_visible_body(content)
        cycle_attempt = getattr(self, "_cycle_attempts", {}).get(source_id)
        responds_to_id = getattr(self, "_cycle_response_sources", {}).get(source_id, source_id)
        coordination_mode = getattr(self, "_source_coordination_modes", {}).get(source_id, "coordinated")
        open_recipients = getattr(self, "_open_reply_recipients", {}).get(source_id, [])
        observed = getattr(self, "_event_seq", {}).get(chat_id, {}).get(source_id, binding.cursor)
        observed_epoch = getattr(self, "_event_epoch", {}).get(source_id, "")
        existing_intent = binding.delivery_intents.get(source_id) or {}
        recovered = self._recoverable_selection(existing_intent)
        if recovered is not None:
            observed = int(recovered.get("source_seq") or observed)
            observed_epoch = str(recovered.get("observed_epoch_id") or observed_epoch)
        canonical = existing_intent.get("canonical_event")
        durably_posted = (
            existing_intent.get("delivery_state") == "posted"
            and isinstance(canonical, dict)
            and bool(str(canonical.get("id") or ""))
        )
        if not durably_posted:
            try:
                current_state = await self._call(binding, lambda api: api.room_state(chat_id))
            except ProtocolError as error:
                return SendResult(success=False, error=str(error), retryable=error.retryable)
            active_epoch = current_state.get("activeEpoch") or {}
            active_epoch_id = active_epoch.get("id")
            starts_at = int(active_epoch.get("startsAtSeq") or 0)
            if (
                not isinstance(active_epoch_id, str)
                or not active_epoch_id.strip()
                or not observed_epoch
                or observed_epoch != active_epoch_id
                or (starts_at and observed < starts_at)
            ):
                await self._publish(
                    binding, source_id, "terminal", status="superseded", suppress_errors=True,
                )
                return self._successful_terminal(
                    source_ref, dispatch_generation, f"superseded:{source_id}",
                    terminal_status="superseded", reason="stale_epoch",
                )
        try:
            selected = self._select_delivery_intent(
                binding, source_id, body, responds_to_id, open_recipients,
                coordination_mode, observed, observed_epoch, cycle_attempt,
            )
        except ProtocolError as error:
            return SendResult(success=False, error=str(error), retryable=error.retryable)
        body = str(selected.get("body") or "") if selected.get("action") == "post" else None
        responds_to_id = str(selected.get("responds_to") or source_id)
        open_recipients = list(selected.get("recipient_membership_ids") or [])
        coordination_mode = str(selected.get("coordination_mode") or "coordinated")
        observed = int(selected.get("observed_seq") or observed)
        observed_epoch = str(selected.get("observed_epoch_id") or observed_epoch)
        selected_cycle = dict(selected.get("cycle") or {})
        if cycle_attempt is None and selected_cycle.get("cycle_id") and selected_cycle.get("attempt_id"):
            cycle_attempt = {
                "cycle": {
                    "id": str(selected_cycle["cycle_id"]),
                    "generation": int(selected_cycle.get("generation") or 0),
                },
                "attempt": {"id": str(selected_cycle["attempt_id"])},
            }
        if body is None:
            if cycle_attempt:
                await self._complete_cycle_attempt(binding, cycle_attempt, "pass")
                await self._stop_attempt_renewal(source_id)
                self._cycle_attempts.pop(source_id, None)
                getattr(self, "_cycle_response_sources", {}).pop(source_id, None)
            await self._publish(
                binding,
                source_id,
                "terminal",
                status="skipped",
                suppress_errors=True,
            )
            getattr(self, "_source_coordination_modes", {}).pop(source_id, None)
            getattr(self, "_open_reply_recipients", {}).pop(source_id, None)
            return self._successful_terminal(
                source_ref, dispatch_generation, f"skipped:{source_id}",
                terminal_status="skipped", reason="model_skip",
            )
        event: dict[str, Any] | None = None
        canonical_recorded = False
        try:
            intent = binding.delivery_intents.get(source_id) or {}
            if intent.get("delivery_state") == "quarantined" or intent.get("state") == "quarantined":
                return SendResult(
                    success=False,
                    error="Room delivery is quarantined for operator recovery",
                    retryable=False,
                )
            persisted_post = intent.get("post")
            persisted_mode = str((persisted_post or {}).get("coordination_mode") or "") if isinstance(persisted_post, dict) else ""
            if persisted_mode:
                current_policy = await self._call(binding, lambda api: _room_policy(api, chat_id))
                coordination_mode = str(_policy_view(current_policy).get("coordinationMode") or "coordinated")
                getattr(self, "_source_coordination_modes", {})[source_id] = coordination_mode
            if persisted_mode:
                # A canonical receipt is the immutable delivery boundary. After
                # restart, retry only unfinished lifecycle work; never post or
                # regenerate model output again. Without a receipt, replay the
                # exact frozen request under its persisted idempotency key.
                canonical = intent.get("canonical_event") if isinstance(intent.get("canonical_event"), dict) else {}
                existing_receipt = intent.get("delivery_state") == "posted" and bool(str(canonical.get("id") or ""))
                if existing_receipt:
                    event = dict(canonical)
                    canonical_recorded = True
                else:
                    try:
                        event, _ = await self._post_with_fresh_context(
                            binding, chat_id, "", observed, source_id, body, observed_epoch,
                            None, responds_to_id, open_recipients,
                            coordination_mode=persisted_mode,
                        )
                    except ProtocolError as replay_error:
                        mode_changed = persisted_mode != coordination_mode
                        if not mode_changed or replay_error.code not in {
                            "turn_required", "turns_not_used", "invalid_contribution",
                            "cycle_superseded", "cycle_no_attempt",
                        }:
                            raise
                        old_turn = str(persisted_post.get("turn_id") or "")
                        if old_turn:
                            try:
                                await self._finish_with_fresh_context(
                                    binding, chat_id, old_turn,
                                    int(persisted_post.get("observed_seq") or observed), source_id,
                                )
                            except ProtocolError as finish_error:
                                if finish_error.code not in {"turn_not_active", "turn_expired", "turns_not_used"}:
                                    raise
                        intent["delivery_superseded"] = True
                        self._persist_binding(binding)
                        await self._stop_attempt_renewal(source_id)
                        getattr(self, "_cycle_attempts", {}).pop(source_id, None)
                        await self._publish(binding, source_id, "terminal", status="superseded", suppress_errors=True)
                        return self._successful_terminal(
                            source_ref, dispatch_generation, f"superseded:{source_id}",
                            terminal_status="superseded", reason="coordination_mode_changed",
                        )
                cycle_payload = dict(persisted_post.get("cycle") or {})
                completion: dict[str, Any] | None = None
                if existing_receipt:
                    journal = binding.delivery_lifecycle.get(source_id) or {}
                    persisted_completion = journal.get("completion")
                    if journal.get("delivery_state") != "posted" or not isinstance(persisted_completion, dict):
                        raise ValueError("canonical delivery is missing its persisted lifecycle request")
                    completion = copy.deepcopy(persisted_completion) or None
                elif cycle_payload.get("cycle_id") and cycle_payload.get("attempt_id"):
                    completion = {
                        "kind": "cycle",
                        "cycle_id": str(cycle_payload["cycle_id"]),
                        "attempt_id": str(cycle_payload["attempt_id"]),
                        "payload": {
                            "generation": int(cycle_payload.get("generation") or 0),
                            "action": "contribute",
                            "eventId": str(event.get("id") or ""),
                        },
                    }
                elif str(persisted_post.get("turn_id") or ""):
                    completion = {
                        "kind": "turn",
                        "turn_id": str(persisted_post["turn_id"]),
                        "observed_seq": int(event.get("seq") or persisted_post.get("observed_seq") or observed),
                        "source_event_id": source_id,
                        "idempotency_key": str(
                            ((intent.get("selected") or {}).get("finish_idempotency_key"))
                            or stable_key("finish", source_id, room_id=binding.room_id, membership_id=binding.membership_id)
                        ),
                    }
                if not existing_receipt:
                    self._record_canonical_delivery(
                        binding, source_id, event, completion=completion,
                    )
                    canonical_recorded = True
                if completion:
                    self._record_lifecycle_attempt(binding, source_id)
                if completion and completion.get("kind") == "cycle":
                    payload = dict(completion.get("payload") or {})
                    replay_attempt = {
                        "cycle": {
                            "id": str(completion.get("cycle_id") or ""),
                            "generation": int(payload.get("generation") or 0),
                        },
                        "attempt": {"id": str(completion.get("attempt_id") or "")},
                    }
                    try:
                        await self._complete_cycle_attempt(
                            binding, replay_attempt, "contribute", str(event.get("id") or ""),
                        )
                    except ProtocolError as completion_error:
                        if completion_error.code != "cycle_superseded":
                            raise
                elif completion and completion.get("kind") == "turn":
                    try:
                        await self._finish_with_fresh_context(
                            binding, chat_id, str(completion.get("turn_id") or ""),
                            int(completion.get("observed_seq") or 0),
                            str(completion.get("source_event_id") or source_id),
                        )
                    except ProtocolError as finish_error:
                        if finish_error.code not in {"turn_not_active", "turn_expired", "turns_not_used"}:
                            raise
                self._record_lifecycle_complete(binding, source_id)
                self._persist_binding(binding)
                await self._publish(
                    binding, source_id, "terminal", status="posted",
                    canonical_event_id=event.get("id", ""), suppress_errors=True,
                )
                return self._successful_terminal(
                    source_ref, dispatch_generation, event.get("id"),
                    terminal_status="posted", canonical_event_id=str(event.get("id") or ""),
                )
            if coordination_mode == "open" and not cycle_attempt:
                # Restore the proven pre-cycle connector path: the model
                # decides whether to contribute first, then its exact-current
                # canonical post is serialized by observedSeq. No discussion
                # cycle or speaking-turn lease exists in an open room.
                state = await self._call(binding, lambda api: api.room_state(chat_id))
                observed = max(observed, int(state.get("headSeq") or 0))
                event, _ = await self._post_with_fresh_context(
                    binding, chat_id, "", observed, source_id, body, observed_epoch,
                    None, responds_to_id, open_recipients, coordination_mode="open",
                )
                self._record_canonical_delivery(
                    binding, source_id, event, completion=None,
                )
                canonical_recorded = True
                getattr(self, "_source_coordination_modes", {}).pop(source_id, None)
                getattr(self, "_open_reply_recipients", {}).pop(source_id, None)
                self._persist_binding(binding)
                await self._publish(
                    binding,
                    source_id,
                    "terminal",
                    status="posted",
                    canonical_event_id=event.get("id", ""),
                    suppress_errors=True,
                )
                return self._successful_terminal(
                    source_ref, dispatch_generation, event.get("id"),
                    terminal_status="posted", canonical_event_id=str(event.get("id") or ""),
                )
            if source_id in getattr(self, "_superseded_sources", set()):
                await self._stop_attempt_renewal(source_id)
                getattr(self, "_superseded_sources", set()).discard(source_id)
                await self._publish(binding, source_id, "terminal", status="superseded", suppress_errors=True)
                return self._successful_terminal(
                    source_ref, dispatch_generation, f"superseded:{source_id}",
                    terminal_status="superseded", reason="attempt_lease_lost",
                )
            if cycle_attempt:
                cycle_id = str((cycle_attempt.get("cycle") or {}).get("id") or "")
                refreshed = await self._claim_discussion_attempt(binding, cycle_id)
                original_attempt_id = str((cycle_attempt.get("attempt") or {}).get("id") or "")
                refreshed_attempt_id = str(((refreshed or {}).get("attempt") or {}).get("id") or "")
                if not refreshed or refreshed_attempt_id != original_attempt_id:
                    # Model execution may outlive its discussion lease. Fence
                    # the result before acquiring a canonical speaking turn;
                    # otherwise a rejected late post can leave that turn
                    # granted until timeout and cascade into later attempts.
                    self._cycle_attempts.pop(source_id, None)
                    getattr(self, "_cycle_response_sources", {}).pop(source_id, None)
                    await self._stop_attempt_renewal(source_id)
                    await self._publish(
                        binding,
                        source_id,
                        "terminal",
                        status="superseded",
                        suppress_errors=True,
                    )
                    return self._successful_terminal(
                        source_ref, dispatch_generation, f"superseded:{source_id}",
                        terminal_status="superseded", reason="attempt_superseded",
                    )
                cycle_attempt = refreshed
                self._cycle_attempts[source_id] = refreshed
            turn: dict[str, Any] | None = None
            if not cycle_attempt:
                # Backward-compatible path for a legacy untyped/direct event
                # that is not part of an automatic discussion cycle.
                turn, observed = await self._request_turn_with_fresh_context(
                    binding, chat_id, observed, source_id,
                )
                turn = await self._wait_for_turn(binding, turn, observed, source_id, timeout=120)
            state = await self._call(binding, lambda api: api.room_state(chat_id))
            observed = max(observed, int(state.get("headSeq") or 0))
            if not self._binding_generation_active(binding):
                return SendResult(success=False, error="Room membership changed before delivery")
            event, observed = await self._post_with_fresh_context(
                binding, chat_id, str((turn or {}).get("turnId") or ""), observed, source_id, body, observed_epoch,
                cycle_attempt, responds_to_id, coordination_mode="coordinated",
            )
            completion = None
            if cycle_attempt:
                cycle_view, attempt_view = cycle_attempt["cycle"], cycle_attempt["attempt"]
                completion = {
                    "kind": "cycle",
                    "cycle_id": str(cycle_view["id"]),
                    "attempt_id": str(attempt_view["id"]),
                    "payload": {
                        "generation": int(cycle_view["generation"]),
                        "action": "contribute",
                        "eventId": str(event.get("id") or ""),
                    },
                }
            elif turn:
                completion = {
                    "kind": "turn",
                    "turn_id": str(turn.get("turnId") or ""),
                    "observed_seq": int(event.get("seq") or observed),
                    "source_event_id": source_id,
                    "idempotency_key": str(
                        ((binding.delivery_intents.get(source_id) or {}).get("selected") or {}).get("finish_idempotency_key")
                        or stable_key("finish", source_id, room_id=binding.room_id, membership_id=binding.membership_id)
                    ),
                }
            self._record_canonical_delivery(
                binding, source_id, event, completion=completion,
            )
            canonical_recorded = True
            if completion:
                self._record_lifecycle_attempt(binding, source_id)
            if cycle_attempt:
                await self._complete_cycle_attempt(binding, cycle_attempt, "contribute", str(event.get("id") or ""))
                await self._stop_attempt_renewal(source_id)
                self._cycle_attempts.pop(source_id, None)
                getattr(self, "_cycle_response_sources", {}).pop(source_id, None)
            elif turn:
                await self._finish_with_fresh_context(
                    binding, chat_id, str(turn.get("turnId") or ""), int(event.get("seq") or observed), source_id,
                )
            self._record_lifecycle_complete(binding, source_id)
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
            return self._successful_terminal(
                source_ref, dispatch_generation, event.get("id"),
                terminal_status="posted", canonical_event_id=str(event.get("id") or ""),
            )
        except ProtocolError as error:
            if error.revoked:
                self._revoke(binding)
            elif error.expired:
                self._expire(binding)
            if canonical_recorded:
                intent = binding.delivery_intents.get(source_id) or {}
                if intent.get("lifecycle_state") != "complete":
                    self._record_lifecycle_failure_after_receipt(
                        binding, source_id, error, error_code=error.code,
                    )
                try:
                    await self._stop_attempt_renewal(source_id)
                except Exception:
                    logger.exception("Room %s could not stop renewal after durable delivery", binding.room_id)
                getattr(self, "_cycle_attempts", {}).pop(source_id, None)
                getattr(self, "_cycle_response_sources", {}).pop(source_id, None)
                await self._publish(
                    binding,
                    source_id,
                    "terminal",
                    status="posted",
                    canonical_event_id=event.get("id", ""),
                    suppress_errors=True,
                )
                return self._successful_terminal(
                    source_ref, dispatch_generation, event.get("id"),
                    terminal_status="posted", canonical_event_id=str(event.get("id") or ""),
                )
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
                    return self._successful_terminal(
                        source_ref, dispatch_generation, event.get("id"),
                        terminal_status="posted", canonical_event_id=str(event.get("id") or ""),
                    )
                await self._publish(
                    binding,
                    source_id,
                    "terminal",
                    status="cancelled",
                    suppress_errors=True,
                )
                return self._successful_terminal(
                    source_ref, dispatch_generation, f"cancelled:{source_id}",
                    terminal_status="cancelled", reason=error.code,
                )
            if error.code == "stale_epoch":
                await self._publish(binding, source_id, "terminal", status="superseded", suppress_errors=True)
                return self._successful_terminal(
                    source_ref, dispatch_generation, f"superseded:{source_id}",
                    terminal_status="superseded", reason="stale_epoch",
                )
            self._record_delivery_failure(
                binding, source_id, str(error), error.retryable, error_code=error.code,
            )
            await self._publish(binding, source_id, "terminal", status="failed", suppress_errors=True)
            return SendResult(success=False, error=str(error), retryable=error.retryable)
        except Exception as error:
            if canonical_recorded and event is not None:
                lifecycle_error = ProtocolError(
                    str(error), code="lifecycle_error", retryable=True,
                )
                self._record_lifecycle_failure_after_receipt(
                    binding, source_id, lifecycle_error, error_code="lifecycle_error",
                )
                try:
                    await self._stop_attempt_renewal(source_id)
                except Exception:
                    logger.exception("Room %s could not stop renewal after durable delivery", binding.room_id)
                getattr(self, "_cycle_attempts", {}).pop(source_id, None)
                getattr(self, "_cycle_response_sources", {}).pop(source_id, None)
                await self._publish(
                    binding, source_id, "terminal", status="posted",
                    canonical_event_id=event.get("id", ""), suppress_errors=True,
                )
                return self._successful_terminal(
                    source_ref, dispatch_generation, event.get("id"),
                    terminal_status="posted", canonical_event_id=event["id"],
                )
            self._record_delivery_failure(binding, source_id, str(error), True)
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
        recipient_membership_ids: list[str] | None = None,
        *,
        coordination_mode: str = "coordinated",
    ) -> tuple[dict[str, Any], int]:
        """Retry stale or ambiguously acknowledged writes idempotently."""
        intent = binding.delivery_intents.setdefault(source_id, {})
        selected_value = intent.get("selected")
        selected = selected_value if isinstance(selected_value, dict) else {}
        persisted = intent.get("post")
        if isinstance(persisted, dict):
            turn_id = str(persisted["turn_id"])
            observed = int(persisted["observed_seq"])
            observed_epoch = str(persisted["observed_epoch_id"])
            body = str(persisted["body"])
            cycle_payload = dict(persisted.get("cycle") or {})
            payload_dialect = self._persisted_payload_dialect(persisted)
            if "message_payload_dialect" not in persisted:
                persisted["message_payload_dialect"] = payload_dialect
                if not self._persist_binding(binding):
                    raise ProtocolError(
                        "Room membership changed before post dialect migration was saved",
                        code="binding_changed",
                        retryable=False,
                    )
        else:
            if isinstance(selected_value, dict):
                if not self._intent_matches_binding(binding, selected):
                    raise ProtocolError(
                        "Room delivery intent belongs to a replaced binding",
                        code="binding_changed",
                        retryable=False,
                    )
                coordination_mode = str(selected.get("coordination_mode") or coordination_mode)
                observed_epoch = str(selected.get("observed_epoch_id") or observed_epoch)
                body = str(selected.get("body") or body)
                responds_to_id = str(selected.get("responds_to") or responds_to_id or source_id)
                recipient_membership_ids = list(
                    selected.get("recipient_membership_ids") or recipient_membership_ids or []
                )
                cycle_payload = dict(selected.get("cycle") or {})
            else:
                cycle_payload = _cycle_delivery_payload(cycle_attempt, binding.membership_id)
            payload_dialect = str(selected.get("message_payload_dialect") or binding.message_payload_dialect)
            if payload_dialect not in {"v1", "v2"}:
                raise ProtocolError(
                    "Room message payload dialect was not negotiated",
                    code="message_contract_unavailable",
                    retryable=True,
                )
            if responds_to_id and responds_to_id != source_id:
                cycle_payload["responds_to"] = responds_to_id
            if recipient_membership_ids:
                cycle_payload["recipients"] = list(recipient_membership_ids)
            recipients = list(cycle_payload.get("recipients") or [])
            intent["post"] = {
                "coordination_mode": coordination_mode,
                "turn_id": turn_id,
                "observed_seq": observed,
                "observed_epoch_id": observed_epoch,
                "body": body,
                "responds_to": str(cycle_payload.get("responds_to") or source_id),
                "recipient_membership_ids": recipients,
                "contribution_type": "question" if recipients else "claim",
                "idempotency_key": str((selected or {}).get("message_idempotency_key") or stable_key(
                    "message", source_id, room_id=binding.room_id, membership_id=binding.membership_id,
                )),
                "logical_contribution_id": str((selected or {}).get("logical_contribution_id") or stable_key(
                    "logical-contribution", str(cycle_payload.get("responds_to") or source_id),
                    room_id=binding.room_id, membership_id=binding.membership_id,
                )),
                "message_payload_dialect": payload_dialect,
                "cycle": cycle_payload,
                "binding": self._intent_binding(binding),
            }
            if not self._persist_binding(binding):
                raise ProtocolError(
                    "Room membership changed before message delivery",
                    code="binding_changed",
                    retryable=False,
                )
        # Pre-1.0.20 durable post intents did not always carry their key.
        # Reconstruct only the exact legacy derivation for those old shapes;
        # every newly selected intent above persists its v2 actor-scoped key.
        post_key = str(intent["post"].get("idempotency_key") or stable_key("message", source_id))
        logical_contribution_id = str(
            intent["post"].get("logical_contribution_id")
            or selected.get("logical_contribution_id")
            or stable_key(
                "logical-contribution", responds_to_id,
                room_id=binding.room_id, membership_id=binding.membership_id,
            )
        )
        payload_dialect = self._persisted_payload_dialect(intent["post"])
        for attempt in range(3):
            try:
                def post(api: RoomProtocol) -> dict[str, Any]:
                    if cycle_payload:
                        return _with_message_payload_dialect(
                            _with_logical_contribution_id(
                                _with_idempotency_key(api, post_key), logical_contribution_id,
                            ), payload_dialect,
                        ).post_message(
                            chat_id, turn_id, observed, source_id, body, observed_epoch,
                            cycle_id=str(cycle_payload.get("cycle_id") or ""),
                            attempt_id=str(cycle_payload.get("attempt_id") or ""),
                            cycle_generation=int(cycle_payload.get("generation") or 0),
                            recipient_membership_ids=list(cycle_payload.get("recipients") or []),
                            responds_to_id=str(cycle_payload.get("responds_to") or source_id),
                        )
                    return _with_message_payload_dialect(
                        _with_logical_contribution_id(
                            _with_idempotency_key(api, post_key), logical_contribution_id,
                        ), payload_dialect,
                    ).post_message(
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
        *,
        cycle_attempt: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], int]:
        """Acquire the idempotent turn against the latest canonical head."""
        source_seq = int(
            getattr(self, "_event_seq", {}).get(binding.room_id, {}).get(source_id, observed)
        )
        observed = int(binding.turn_observed.get(source_id, observed))
        intent = binding.delivery_intents.setdefault(source_id, {})
        selected = intent.get("selected") if isinstance(intent.get("selected"), dict) else {}
        turn_key = str(selected.get("turn_idempotency_key") or stable_key(
            "turn", source_id, room_id=binding.room_id, membership_id=binding.membership_id,
        ))
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
                    lambda api: _with_idempotency_key(api, turn_key).request_turn(
                        chat_id, observed, source_id,
                    ),
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
            intent["finish"] = {
                "turn_id": turn_id,
                "observed_seq": observed,
                "idempotency_key": str(((intent.get("selected") or {}).get("finish_idempotency_key")) or stable_key(
                    "finish", source_id, room_id=binding.room_id, membership_id=binding.membership_id,
                )),
                "binding": self._intent_binding(binding),
            }
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
                    lambda api: _with_idempotency_key(
                        api, str(intent["finish"]["idempotency_key"]),
                    ).finish_turn(
                        chat_id, turn_id, observed, source_id,
                    ),
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

    async def _repair_pending_lifecycles(self, binding: RoomBinding) -> None:
        """Retry only validated, persisted lifecycle work within its durable budget."""
        for source_id, journal in list(binding.delivery_lifecycle.items()):
            if (
                not isinstance(journal, dict)
                or journal.get("delivery_state") != "posted"
                or journal.get("lifecycle_state") != "pending"
                or not str((journal.get("receipt") or {}).get("canonical_event_id") or "")
            ):
                continue
            attempts = int(journal.get("attempts") or 0)
            if journal.get("automatic_retry") is False or attempts >= 3:
                journal["state"] = "lifecycle_blocked"
                journal["lifecycle_state"] = "blocked"
                journal["automatic_retry"] = False
                binding.delivery_lifecycle[source_id] = journal
                self._persist_binding(binding)
                continue
            completion = journal.get("completion") or {}
            kind = str(completion.get("kind") or "")
            if kind not in {"cycle", "turn"}:
                self._record_lifecycle_pending(
                    binding, source_id, ValueError("invalid persisted lifecycle completion kind"),
                    error_code="invalid_lifecycle_state",
                )
                continue
            try:
                # Persist the increment before authenticated I/O so a crash or
                # ambiguous response cannot reset the automatic retry budget.
                self._record_lifecycle_attempt(binding, source_id)
                if kind == "cycle":
                    await self._call(
                        binding,
                        lambda api, request=completion: api.complete_discussion_attempt(
                            binding.room_id,
                            str(request["cycle_id"]),
                            str(request["attempt_id"]),
                            dict(request["payload"]),
                        ),
                    )
                else:
                    await self._call(
                        binding,
                        lambda api, request=completion: _with_idempotency_key(
                            api, str(request["idempotency_key"]),
                        ).finish_turn(
                            binding.room_id,
                            str(request["turn_id"]),
                            int(request["observed_seq"]),
                            str(request["source_event_id"]),
                        ),
                    )
            except ProtocolError as error:
                if error.code in {"cycle_superseded", "turn_not_active", "turn_expired", "turns_not_used"}:
                    self._record_lifecycle_complete(binding, source_id)
                    continue
                self._record_lifecycle_pending(
                    binding, source_id, error, error_code=error.code,
                )
                continue
            except Exception as error:
                # Persist malformed/in-process failures as blocked work instead
                # of crashing the watch task into an unbounded restart loop.
                self._record_lifecycle_pending(
                    binding, source_id, error, error_code="lifecycle_repair_failed",
                )
                continue
            self._record_lifecycle_complete(binding, source_id)

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
                    orphaned = self._overtaken_delivery_intents(binding)
                    if orphaned:
                        raise ProtocolError(
                            "Room has selected delivery intents older than its canonical acknowledgement; "
                            "operator recovery is required before reconnect",
                            code="orphaned_delivery_intent",
                            retryable=False,
                        )
                    connector = await self._ensure_connector(binding)
                    await self._ensure_epoch_session_routing(binding)
                    await self._repair_pending_lifecycles(binding)
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
                    if error.expired:
                        self._expire(binding)
                        return
                    if error.code == "orphaned_delivery_intent":
                        logger.error(
                            "Room %s connector stopped on overtaken delivery intent; run audited recovery before restart",
                            binding.room_id,
                        )
                        return
                    logger.warning("Room %s temporarily unavailable: %s", binding.room_id, error)
                    await asyncio.sleep(max(delay, min(error.retry_after, 120.0)))
                    delay = min(delay * 2, 30)
        finally:
            self._tasks.pop(binding.room_id, None)

    @staticmethod
    def _overtaken_delivery_intents(binding: RoomBinding) -> list[str]:
        """Return selected intents whose source is already acknowledged."""
        return sorted(
            source_id
            for source_id, intent in binding.delivery_intents.items()
            if isinstance(intent, dict)
            and not (
                isinstance(binding.delivery_lifecycle.get(source_id), dict)
                and str(((binding.delivery_lifecycle.get(source_id) or {}).get("receipt") or {}).get("canonical_event_id") or "")
            )
            and isinstance(intent.get("selected"), dict)
            and int((intent.get("selected") or {}).get("source_seq") or 0) > 0
            and int((intent.get("selected") or {}).get("source_seq") or 0)
            <= binding.acknowledged_cursor
        )

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
            lambda api: api.events(binding.room_id, binding.cursor, 8, all_epochs=True),
        )
        for event in page.get("events", []):
            await self._consume(binding, event)

    async def _ensure_connector(self, binding: RoomBinding) -> dict[str, Any]:
        await self._ensure_message_payload_contract(binding)
        if binding.connector_session_id:
            try:
                heartbeat = await self._call(
                    binding,
                    lambda api: api.heartbeat(binding.room_id, binding.connector_session_id),
                )
                self._note_connected(binding, heartbeat)
                return heartbeat
            except ProtocolError as error:
                if error.revoked or error.expired:
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

    async def _ensure_epoch_session_routing(self, binding: RoomBinding) -> None:
        if binding.epoch_session_routing_initialized:
            return
        state = await self._call(binding, lambda api: api.room_state(binding.room_id))
        active_epoch_id = (state.get("activeEpoch") or {}).get("id")
        if not isinstance(active_epoch_id, str) or not active_epoch_id.strip():
            raise ProtocolError(
                "Room state has no active epoch for session routing",
                code="active_epoch_unavailable",
                retryable=True,
            )
        previous = (
            binding.epoch_session_routing_initialized,
            binding.legacy_session_epoch_id,
            binding.rotate_current_epoch_session,
        )
        binding.legacy_session_epoch_id = (
            "" if binding.rotate_current_epoch_session else active_epoch_id
        )
        binding.epoch_session_routing_initialized = True
        binding.rotate_current_epoch_session = False
        binding._consuming_epoch_session_rotation = previous[2]
        try:
            persisted = self._persist_binding(binding)
        except Exception:
            (
                binding.epoch_session_routing_initialized,
                binding.legacy_session_epoch_id,
                binding.rotate_current_epoch_session,
            ) = previous
            raise
        finally:
            del binding._consuming_epoch_session_rotation
        if not persisted:
            (
                binding.epoch_session_routing_initialized,
                binding.legacy_session_epoch_id,
                binding.rotate_current_epoch_session,
            ) = previous
            raise ProtocolError(
                "Room membership changed before epoch session routing was saved",
                code="binding_changed",
                retryable=False,
            )

    async def _ensure_message_payload_contract(self, binding: RoomBinding) -> None:
        """Negotiate this binding's write dialect through one read-only GET."""
        if binding.room_id in self._message_contract_checked:
            return
        report = await self._call(binding, lambda api: api.status())
        raw_capabilities = report.get("protocolCapabilities")
        if raw_capabilities is None:
            capabilities: list[str] = []  # legacy server: explicit safe v1
        elif isinstance(raw_capabilities, list) and all(
            isinstance(value, str) for value in raw_capabilities
        ):
            capabilities = list(dict.fromkeys(raw_capabilities))
        else:
            raise ProtocolError(
                "Room status returned malformed protocol capabilities",
                code="message_contract_unavailable",
                retryable=True,
            )
        binding.message_payload_capabilities = capabilities
        binding.message_payload_dialect = (
            "v2" if MESSAGE_LOGICAL_CONTRIBUTION_CAPABILITY in capabilities else "v1"
        )
        if not self._persist_binding(binding):
            raise ProtocolError(
                "Room membership changed before message capability negotiation was saved",
                code="binding_changed",
                retryable=False,
            )
        self._message_contract_checked.add(binding.room_id)

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
                    if error.expired:
                        self._expire(binding)
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
        receive_locks = getattr(self, "_receive_locks", None)
        if receive_locks is None:
            receive_locks = {}
            self._receive_locks = receive_locks
        lock = receive_locks.setdefault(binding.room_id, asyncio.Lock())
        async with lock:
            await self._consume_locked(binding, event)

    async def _consume_locked(self, binding: RoomBinding, event: dict[str, Any]) -> None:
        seq = int(event.get("seq") or 0)
        if seq <= binding.acknowledged_cursor:
            return
        key = str(seq)
        event_id = str(event.get("id") or "")
        # Audited recovery fence: an operator-set cutoff (recovery_fence_cutoff
        # > 0, exact, explicit) marks the stale backlog through the normal
        # connector acknowledgement path — terminally `ignored` with the
        # recovery_fence reason — so the durable chain-walk advances the cursor
        # without any direct cursor/DB write. The fence is idempotent and
        # replay-safe: it only ever acts on (acknowledged_cursor, cutoff], and
        # sequences beyond the cutoff are processed normally. This is the
        # authorized audited-skip mechanism; it never produces a post.
        if binding.recovery_fence_cutoff > 0 and seq <= binding.recovery_fence_cutoff:
            status = str(binding.inbox.get(key) or "")
            if status not in TERMINAL_EVENT_STATES:
                binding.inbox[key] = "ignored"
                binding.pending_since.pop(key, None)
                binding.pending_retries.pop(key, None)
                binding.terminal_evidence[key] = {
                    "status": "ignored",
                    "sourceEventId": str(event.get("id") or ""),
                    "canonicalEventId": str(event.get("id") or ""),
                    "reason": "recovery_fence",
                }
                self._persist_binding(binding)
                logger.info(
                    "Room %s recovery fence marked sequence %s ignored (cutoff=%s)",
                    binding.room_id, seq, binding.recovery_fence_cutoff,
                )
            await self._complete_event(binding, seq, terminal_status="ignored",
                                       source_id=str(event.get("id") or ""),
                                       reason="recovery_fence")
            return
        if key not in binding.inbox:
            binding.inbox[key] = "pending"
            binding.pending_since[key] = time.time()
            binding.pending_retries.setdefault(key, 0)
        elif binding.inbox[key] in {"retryable", "failed-retryable"}:
            binding.inbox[key] = "pending"
            binding.pending_since[key] = time.time()
        elif binding.inbox[key] == "pending" and key not in binding.pending_since:
            # Pre-TTL state has no timestamp.  It is necessarily historical,
            # so allow the sweep to release it rather than replaying it
            # forever after an upgrade.
            binding.pending_since[key] = 0.0
        elif binding.inbox[key] == "complete":
            # 1.0.16 used an unclassified `complete` marker. It cannot prove
            # posted/skip/cancel/supersede/ignore after upgrade, so preserve
            # the cursor and intent for operator review.
            binding.inbox[key] = "quarantined"
            logger.error(
                "Room %s quarantined legacy unclassified completion at sequence %s",
                binding.room_id, seq,
            )
        if self._activate_legacy_canonical_receipt(binding, event_id, seq):
            logger.warning(
                "Room %s promoted a complete legacy canonical receipt for sequence %s; delivery will not be replayed",
                binding.room_id, seq,
            )
        elif self._activate_legacy_post_commit_recovery(binding, event_id, seq):
            logger.warning(
                "Room %s activated exact idempotent post-commit recovery for sequence %s",
                binding.room_id, seq,
            )
        if not self._persist_binding(binding):
            return
        status = str(binding.inbox.get(key) or "")
        if status in TERMINAL_EVENT_STATES:
            # Crash/timeout after durable completion: retry only the idempotent
            # acknowledgement, never the Hermes model run.
            await self._complete_event(binding, seq)
            return
        if status == "quarantined":
            return
        if event_id in self._inflight_events:
            return
        intent = binding.delivery_intents.get(event_id) or {}
        selected = self._recoverable_selection(intent)
        if selected is not None:
            if not self._intent_matches_binding(binding, selected):
                binding.inbox[key] = "quarantined"
                binding.pending_since.pop(key, None)
                self._persist_binding(binding)
                logger.error(
                    "Room %s quarantined delivery intent %s from a replaced binding generation",
                    binding.room_id, event_id,
                )
                return
            event_seq = getattr(self, "_event_seq", None)
            if event_seq is None:
                event_seq = {}
                self._event_seq = event_seq
            event_seq.setdefault(binding.room_id, {})[event_id] = seq
            event_epoch = getattr(self, "_event_epoch", None)
            if event_epoch is None:
                event_epoch = {}
                self._event_epoch = event_epoch
            event_epoch[event_id] = str(selected.get("observed_epoch_id") or "")
            modes = getattr(self, "_source_coordination_modes", None)
            if modes is None:
                modes = {}
                self._source_coordination_modes = modes
            modes[event_id] = str(
                selected.get("coordination_mode") or "coordinated"
            )
            recipients = list(selected.get("recipient_membership_ids") or [])
            if recipients:
                open_recipients = getattr(self, "_open_reply_recipients", None)
                if open_recipients is None:
                    open_recipients = {}
                    self._open_reply_recipients = open_recipients
                open_recipients[event_id] = recipients
            responds_to = str(selected.get("responds_to") or "")
            if responds_to and responds_to != event_id:
                response_sources = getattr(self, "_cycle_response_sources", None)
                if response_sources is None:
                    response_sources = {}
                    self._cycle_response_sources = response_sources
                response_sources[event_id] = responds_to
            queued_events = getattr(self, "_queued_events", None)
            if queued_events is None:
                queued_events = {}
                self._queued_events = queued_events
            queued_events.setdefault(binding.room_id, {})[seq] = event
            await self._dispatch_next_queued(binding)
            return
        if event.get("actorId") == binding.membership_id:
            await self._complete_event(
                binding, seq, terminal_status="ignored", source_id=event_id,
                reason="self_event",
            )
            return
        payload = event.get("payload") or {}
        event_type = str(event.get("type") or "")
        legacy_untyped_message = event_type == "message.posted" and not str(event.get("actorRole") or "")
        human_source = _is_human_cycle_source(event)
        agent_seed = _is_agent_cycle_seed(event)
        ready_for_self = (
            event_type == "discussion.cycle_attempt_ready"
            and str(payload.get("membershipId") or "") == binding.membership_id
        )
        summary_for_self = False
        if event_type == "discussion.cycle_terminal" and str(payload.get("state") or "") == "completed":
            policy_response = await self._call(
                binding, lambda api: _room_policy(api, binding.room_id),
            )
            policy_view = _policy_view(policy_response)
            coordinator = str(policy_view.get("summaryCoordinatorMembershipId") or "")
            summary_for_self = (
                str(policy_view.get("summaryBehavior") or "") == "on_cycle_complete"
                and coordinator == binding.membership_id
            )
        if event_type != "message.posted" and not human_source and not ready_for_self and not summary_for_self:
            await self._complete_event(
                binding, seq, terminal_status="ignored", source_id=event_id,
                reason="ineligible_event_type",
            )
            return
        if event_type == "message.posted" and not human_source and not legacy_untyped_message and not _agent_event_addresses(payload, binding.membership_id):
            await self._complete_event(
                binding, seq, terminal_status="ignored", source_id=event_id,
                reason="not_addressed_to_membership",
            )
            return
        if _is_peer_contribution(event, binding.membership_id):
            await self._call(
                binding,
                lambda api: _acknowledge_peer(api, binding.room_id, event_id),
            )
        if event_type == "message.posted" and payload.get("cycleId"):
            # A cycle-bound post is canonical model context. The server-owned
            # attempt-ready event is the sole model trigger, so these adjacent
            # sources cannot launch the same attempt twice.
            await self._complete_event(
                binding, seq, terminal_status="ignored", source_id=event_id,
                reason="cycle_context_only",
            )
            return
        if event_id and event_id not in self._run_for_event:
            self._run_for_event[event_id] = "hermes:" + uuid.uuid4().hex
            self._activity_seq[event_id] = 0
        coordination_mode = "coordinated"
        policy_view: dict[str, Any] = {}
        if event_type in {"message.posted", "human.command", "discussion.started", "discussion.cycle_attempt_ready", "discussion.cycle_terminal"}:
            policy_response = await self._call(
                binding, lambda api: _room_policy(api, binding.room_id),
            )
            policy_view = _policy_view(policy_response)
            coordination_mode = str(policy_view.get("coordinationMode") or "coordinated")
            modes = getattr(self, "_source_coordination_modes", None)
            if modes is None:
                modes = {}
                self._source_coordination_modes = modes
            modes[event_id] = coordination_mode

        if coordination_mode == "open" and event_type == "human.command":
            # Structured bounded-discussion commands belong to coordinated
            # rooms. Ordinary open-room prose follows the earlier free path.
            await self._complete_event(
                binding, seq, terminal_status="ignored", source_id=event_id,
                reason="coordinated_command_in_open_room",
            )
            return

        if agent_seed and (
            coordination_mode != "open"
            or not bool(policy_view.get("agentFollowUpEnabled", True))
        ):
            await self._complete_event(
                binding, seq, terminal_status="ignored", source_id=event_id,
                reason="agent_cycle_policy_ineligible",
            )
            return

        if coordination_mode == "open" and event_type == "message.posted":
            role = str(event.get("actorRole") or "")
            if role == "human" or role.startswith("human_"):
                resolved = payload.get("resolvedRecipientMembershipIds")
                if isinstance(resolved, list) and resolved and binding.membership_id not in {str(value) for value in resolved}:
                    await self._complete_event(
                        binding, seq, terminal_status="ignored", source_id=event_id,
                        reason="resolved_recipients_exclude_membership",
                    )
                    return
                # A normal human contribution seeds the bounded server-owned
                # open-exchange cycle. No transcript parsing or visible slash
                # command is involved; Room policy/guidance controls behavior.
            else:
                # The one-hop cap protects only casual peer replies. A
                # canonical cycleId is server-owned routing and remains
                # bounded by the persisted cycle budget, so it must reach
                # Claim even at deeper follow-up depths.
                if not payload.get("cycleId"):
                    if not agent_seed and (
                        not bool(policy_view.get("agentFollowUpEnabled", True))
                        or int(payload.get("followUpDepth") or 0) >= 1
                    ):
                        await self._complete_event(
                            binding, seq, terminal_status="ignored", source_id=event_id,
                            reason="agent_follow_up_policy_ineligible",
                        )
                        return
                    actor_id = str(event.get("actorId") or "")
                    if actor_id:
                        recipients = getattr(self, "_open_reply_recipients", None)
                        if recipients is None:
                            recipients = {}
                            self._open_reply_recipients = recipients
                        recipients[event_id] = [actor_id]
        cycle_attempt: dict[str, Any] | None = None
        if human_source or agent_seed:
            state = await self._call(binding, lambda api: api.room_state(binding.room_id))
            active_epoch = state.get("activeEpoch") or {}
            starts_at = int(active_epoch.get("startsAtSeq") or 0)
            if starts_at and seq < starts_at:
                # Full-chain agent transport deliberately replays historical
                # epochs.  Those events are canonical transport work, but they
                # are not fresh conversation triggers in the active epoch.
                # Fence them before cycle creation so restart recovery cannot
                # duplicate turns or model work.
                await self._complete_event(
                    binding, seq, terminal_status="ignored", source_id=event_id,
                    reason="historical_epoch",
                )
                return
            cycle = await self._ensure_discussion_cycle(binding, event, state=state)
            if cycle is None:
                await self._complete_event(
                    binding, seq, terminal_status="ignored", source_id=event_id,
                    reason="no_eligible_discussion_cycle",
                )
                return
            # Starting the cycle emits discussion.cycle_attempt_ready. Treat
            # this source as context and let that authoritative event own the
            # model run.
            getattr(self, "_open_reply_recipients", {}).pop(event_id, None)
            getattr(self, "_source_coordination_modes", {}).pop(event_id, None)
            await self._complete_event(
                binding, seq, terminal_status="ignored", source_id=event_id,
                reason="cycle_source_context_only",
            )
            return
        elif payload.get("cycleId") and not summary_for_self:
            cycle_attempt = await self._claim_discussion_attempt(binding, str(payload.get("cycleId")))
        if (human_source or agent_seed or ready_for_self or (payload.get("cycleId") and not summary_for_self)) and not cycle_attempt:
            await self._complete_event(
                binding, seq, terminal_status="superseded", source_id=event_id,
                reason="discussion_attempt_unavailable",
            )
            return
        body = _cycle_source_body(event, payload)
        if not body or not event_id:
            await self._complete_event(
                binding, seq, terminal_status="ignored", source_id=event_id,
                reason="missing_dispatchable_body_or_event_id",
            )
            return
        if cycle_attempt:
            attempt_key = _cycle_attempt_owner_key(cycle_attempt, binding.membership_id)
            owner = str(binding.cycle_attempt_owners.get(attempt_key) or "")
            if owner and owner != event_id:
                await self._complete_event(
                    binding, seq, terminal_status="superseded", source_id=event_id,
                    reason="duplicate_attempt_source",
                )
                return
            binding.cycle_attempt_owners[attempt_key] = event_id
            if not self._persist_binding(binding):
                return
            cycle_attempts = getattr(self, "_cycle_attempts", None)
            if cycle_attempts is None:
                cycle_attempts = {}
                self._cycle_attempts = cycle_attempts
            cycle_attempts[event_id] = cycle_attempt
            self._start_attempt_renewal(binding, event_id, cycle_attempt)
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
        self, binding: RoomBinding, event: dict[str, Any], *, state: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if state is None:
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
        # An empty resolved list is the canonical broadcast form for an
        # ordinary human Room message. Only a non-empty list narrows the
        # server-verified roster.
        if isinstance(resolved, list) and resolved:
            addressed = {str(value) for value in resolved}
            if _is_agent_cycle_seed(event):
                addressed.add(str(event.get("actorId") or ""))
            roster = [member for member in roster if str(member.get("membershipId")) in addressed]
        if event.get("type") == "human.command":
            command = payload.get("command") or {}
            command_name = str(command.get("command") or "")
            targets = {
                str(value) for value in command.get("resolvedTargetMembershipIds") or []
            }
            if command_name == "ask" and targets:
                roster = [
                    member for member in roster
                    if str(member.get("membershipId")) in targets
                ]
            elif command_name == "summarize":
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
            "idempotencyKey": stable_key(
                "cycle", str(event.get("id") or ""),
                room_id=binding.room_id, membership_id=binding.membership_id,
            ),
        }
        try:
            return await self._call(
                binding,
                lambda api: api.start_discussion_cycle(binding.room_id, request),
            )
        except ProtocolError as error:
            if error.code != "cycle_conflict":
                raise
            # The epoch may have advanced after the preflight state read.  A
            # source that is now historical is safely complete transport work;
            # a same-epoch conflict remains a real server-owned invariant and
            # must stay visible rather than being silently retried.
            fresh = await self._call(binding, lambda api: api.room_state(binding.room_id))
            fresh_epoch = fresh.get("activeEpoch") or {}
            starts_at = int(fresh_epoch.get("startsAtSeq") or 0)
            if starts_at and int(event.get("seq") or 0) < starts_at:
                return None
            raise

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

    def _start_attempt_renewal(
        self,
        binding: RoomBinding,
        source_event_id: str,
        cycle_attempt: dict[str, Any],
    ) -> None:
        tasks = getattr(self, "_attempt_renewal_tasks", None)
        if tasks is None:
            tasks = {}
            self._attempt_renewal_tasks = tasks
        existing = tasks.get(source_event_id)
        if existing and not existing.done():
            return
        cycle_id = str((cycle_attempt.get("cycle") or {}).get("id") or "")
        attempt_id = str((cycle_attempt.get("attempt") or {}).get("id") or "")
        if not cycle_id or not attempt_id:
            return

        async def renew() -> None:
            cycle_attempts = getattr(self, "_cycle_attempts", None)
            if cycle_attempts is None:
                cycle_attempts = {}
                self._cycle_attempts = cycle_attempts
            try:
                while not self._stop.is_set():
                    attempt_view = (cycle_attempts.get(source_event_id) or cycle_attempt).get("attempt") or {}
                    raw_expiry = str(attempt_view.get("leaseExpiresAt") or "")
                    delay = 30.0
                    if raw_expiry:
                        try:
                            expires = datetime.fromisoformat(raw_expiry.replace("Z", "+00:00"))
                            remaining = (expires - datetime.now(timezone.utc)).total_seconds()
                            delay = max(1.0, min(30.0, remaining / 3.0))
                        except ValueError:
                            pass
                    await asyncio.sleep(delay)
                    refreshed = await self._claim_discussion_attempt(binding, cycle_id)
                    refreshed_attempt = str(((refreshed or {}).get("attempt") or {}).get("id") or "")
                    if not refreshed or refreshed_attempt != attempt_id:
                        cycle_attempts.pop(source_event_id, None)
                        superseded = getattr(self, "_superseded_sources", None)
                        if superseded is None:
                            superseded = set()
                            self._superseded_sources = superseded
                        superseded.add(source_event_id)
                        return
                    cycle_attempts[source_event_id] = refreshed
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Room %s discussion attempt renewal failed for source %s",
                    binding.room_id,
                    source_event_id,
                    exc_info=True,
                )
                cycle_attempts.pop(source_event_id, None)
                superseded = getattr(self, "_superseded_sources", None)
                if superseded is None:
                    superseded = set()
                    self._superseded_sources = superseded
                superseded.add(source_event_id)
            finally:
                current = tasks.get(source_event_id)
                if current is asyncio.current_task():
                    tasks.pop(source_event_id, None)

        tasks[source_event_id] = asyncio.create_task(
            renew(), name=f"synthetic-sociality:attempt-renew:{binding.room_id}:{source_event_id}",
        )

    async def _stop_attempt_renewal(self, source_event_id: str) -> None:
        task = getattr(self, "_attempt_renewal_tasks", {}).pop(source_event_id, None)
        if task is None or task is asyncio.current_task():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _dispatch_room_event(
        self,
        binding: RoomBinding,
        event: dict[str, Any],
        dispatch_generation: str,
    ) -> bool:
        seq = int(event.get("seq") or 0)
        payload = event.get("payload") or {}
        body = _cycle_source_body(event, payload)
        event_id = str(event.get("id") or "")
        self._inflight_events.add(event_id)
        self._event_seq.setdefault(binding.room_id, {})[event_id] = seq
        # The authenticated current-epoch fence precedes recovery as well as
        # new model dispatch. A frozen historical result must never regain an
        # outbound I/O path merely because the process restarted.
        state = await self._call(binding, lambda api: api.room_state(binding.room_id))
        active_epoch = state.get("activeEpoch") or {}
        starts_at = int(active_epoch.get("startsAtSeq") or 0)
        if starts_at and seq < starts_at:
            self._inflight_events.discard(event_id)
            await self._complete_event(
                binding, seq, terminal_status="ignored", source_id=event_id,
                reason="historical_epoch_before_dispatch",
            )
            return False
        active_epoch_id = active_epoch.get("id")
        if not isinstance(active_epoch_id, str) or not active_epoch_id.strip():
            self._inflight_events.discard(event_id)
            raise ProtocolError(
                "Room state has no active epoch for dispatch",
                code="active_epoch_unavailable",
                retryable=True,
            )
        self._event_epoch[event_id] = active_epoch_id
        selected = self._recoverable_selection(binding.delivery_intents.get(event_id) or {})
        if selected is not None:
            # Recovery owns the durable model result. Never invoke Hermes a
            # second time merely because process-local buffers disappeared.
            source_ref = _dispatch_source_ref(event_id, dispatch_generation)
            content = (
                str(selected.get("body") or "")
                if selected.get("action") == "post"
                else '{"action":"skip"}'
            )
            result = await self._send_final(binding.room_id, source_ref, content)
            terminal = getattr(self, "_terminal_results", {}).get(source_ref) or {}
            terminal_delivery = (
                bool(getattr(result, "success", False))
                and terminal.get("generation") == dispatch_generation
                and terminal.get("status") in TERMINAL_EVENT_STATES
            )
            if terminal_delivery:
                await self._complete_event(
                    binding,
                    seq,
                    terminal_status=str(terminal["status"]),
                    source_id=event_id,
                    canonical_event_id=str(terminal.get("canonical_event_id") or ""),
                    reason=str(terminal.get("reason") or ""),
                )
            self._inflight_events.discard(event_id)
            return False
        self._latest_source[binding.room_id] = event_id
        run_id = self._run_for_event.get(event_id) or ("hermes:" + uuid.uuid4().hex)
        self._run_for_event[event_id] = run_id
        await self._publish(
            binding,
            event_id,
            "lifecycle",
            status="reading_shared_room",
            suppress_errors=True,
        )
        actor = _room_actor_name(state, event)
        shared_context = ""
        context_policy: dict[str, Any] = {}
        try:
            context_policy = await self._call(
                binding, lambda api: _room_policy(api, binding.room_id),
            )
        except Exception as error:
            logger.warning("Room %s guidance refresh unavailable: %s", binding.room_id, error)
        try:
            context_events = await self._call(
                binding,
                lambda api: _recent_room_messages(
                    lambda after: api.events(binding.room_id, after, 0),
                    before_seq=seq,
                    active_epoch_starts_at=starts_at,
                ),
            )
            shared_context = _canonical_room_context(
                state, context_events, event_id, _policy_view(context_policy),
            )
        except Exception as error:
            # Canonical delivery remains available if the bounded enrichment
            # read fails; the next Room event gets another chance to refresh.
            logger.warning("Room %s context refresh unavailable: %s", binding.room_id, error)
        cycle_attempt = getattr(self, "_cycle_attempts", {}).get(event_id)
        cycle_context = ""
        if cycle_attempt:
            cycle, attempt = cycle_attempt["cycle"], cycle_attempt["attempt"]
            turn_number = int(cycle.get("totalTurns") or 0) + 1
            total_turns = int((cycle.get("budgets") or {}).get("totalTurns") or 0)
            phase, instruction = _cycle_phase_instruction(attempt, cycle, payload)
            cycle_context = f"\n\n[Autonomous discussion phase {phase}, round {attempt.get('round')}, turn {turn_number}/{total_turns}] {instruction}"
        prompt = (
            f"[Synthetic Sociality Room event {event_id}, canonical sequence {seq}]\n"
            f"{shared_context}\n\n{actor}: {body}{cycle_context}\n\n"
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
            # Preserve one shared context inside an epoch while ensuring a new
            # discussion cannot inherit prior task authority or unfinished turns.
            thread_id=_session_thread_for_epoch(
                binding, active_epoch_id,
            ),
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
        terminal_completion = seq <= binding.acknowledged_cursor
        try:
            outcome_name = str(getattr(outcome, "name", outcome)).lower()
            terminal_result = getattr(self, "_terminal_results", {}).get(source_ref) or {}
            terminal_delivery = (
                terminal_sources.get(source_ref) == dispatch_generation
                and terminal_result.get("generation") == dispatch_generation
                and terminal_result.get("status") in TERMINAL_EVENT_STATES
            )
            if not terminal_delivery and outcome_name == "success":
                # Some Hermes streaming paths emit only private preview/edit
                # callbacks and then report processing completion. The last
                # complete buffer is finalised here through the same shared
                # submission task as an explicit final callback.
                buffered_id = next((
                    message_id for message_id, buffered_source in
                    getattr(self, "_buffered_source", {}).items()
                    if buffered_source == source_ref
                ), "")
                submission = getattr(self, "_submission_tasks", {}).get(source_ref)
                if submission is not None:
                    await submission
                elif buffered_id:
                    buffered_output = getattr(self, "_buffered_output", {}).get(buffered_id, "")
                    if buffered_output:
                        await self._send_final(room_id, source_ref, buffered_output)
                terminal_result = getattr(self, "_terminal_results", {}).get(source_ref) or {}
                terminal_delivery = (
                    terminal_sources.get(source_ref) == dispatch_generation
                    and terminal_result.get("generation") == dispatch_generation
                    and terminal_result.get("status") in TERMINAL_EVENT_STATES
                )
            if terminal_delivery:
                terminal_sources.pop(source_ref, None)
                pending = getattr(self, "_context_activity_pending", None)
                if pending is None:
                    pending = {}
                    self._context_activity_pending = pending
                pending.setdefault(binding.room_id, {})[seq] = event_id
                terminal_completion = True
                await self._complete_event(
                    binding,
                    seq,
                    terminal_status=str(terminal_result["status"]),
                    source_id=event_id,
                    canonical_event_id=str(terminal_result.get("canonical_event_id") or ""),
                    reason=str(terminal_result.get("reason") or ""),
                )
            elif cycle_attempt := getattr(self, "_cycle_attempts", {}).get(event_id):
                try:
                    await self._complete_cycle_attempt(binding, cycle_attempt, "pass")
                except ProtocolError as error:
                    if error.code != "cycle_superseded":
                        raise
                getattr(self, "_cycle_attempts", {}).pop(event_id, None)
                getattr(self, "_cycle_response_sources", {}).pop(event_id, None)
                pending = getattr(self, "_context_activity_pending", None)
                if pending is None:
                    pending = {}
                    self._context_activity_pending = pending
                pending.setdefault(binding.room_id, {})[seq] = event_id
                await self._publish(
                    binding, event_id, "terminal", status="skipped", suppress_errors=True,
                )
                terminal_completion = True
                await self._complete_event(
                    binding,
                    seq,
                    terminal_status="skipped",
                    source_id=event_id,
                    reason="processing_failure_cycle_pass",
                )
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
            if terminal_completion:
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
            if seq <= binding.acknowledged_cursor or binding.inbox.get(str(seq)) in TERMINAL_EVENT_STATES:
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
        *,
        terminal_status: str,
        canonical_event_id: str = "",
        reason: str = "",
    ) -> SendResult:
        """Remember that Room delivery reached a truthful terminal boundary.

        Hermes can report a non-success processing outcome after an intentional
        interruption even though the adapter correctly cancelled delivery. The
        receive ledger follows the adapter's terminal delivery result, while
        ordinary model or delivery failures remain pending for recovery.
        """
        if terminal_status not in TERMINAL_EVENT_STATES:
            raise ValueError(f"unsupported Room terminal status: {terminal_status}")
        if terminal_status == "posted" and not canonical_event_id:
            raise ValueError("posted Room terminal evidence requires a canonical event ID")
        if terminal_status != "posted" and not reason:
            raise ValueError(f"{terminal_status} Room terminal evidence requires a reason")
        terminal_sources = getattr(self, "_terminal_sources", None)
        if terminal_sources is None:
            terminal_sources = {}
            self._terminal_sources = terminal_sources
        terminal_sources[source_ref] = dispatch_generation
        terminal_results = getattr(self, "_terminal_results", None)
        if terminal_results is None:
            terminal_results = {}
            self._terminal_results = terminal_results
        terminal_results[source_ref] = {
            "generation": dispatch_generation,
            "status": terminal_status,
            "message_id": str(message_id or ""),
            "canonical_event_id": canonical_event_id,
            "reason": reason,
        }
        return SendResult(success=True, message_id=message_id)

    async def _complete_event(
        self,
        binding: RoomBinding,
        seq: int,
        *,
        terminal_status: str | None = None,
        source_id: str = "",
        canonical_event_id: str = "",
        reason: str = "",
    ) -> None:
        ledger_locks = getattr(self, "_ledger_locks", None)
        if ledger_locks is None:
            ledger_locks = {}
            self._ledger_locks = ledger_locks
        lock = ledger_locks.setdefault(binding.room_id, asyncio.Lock())
        async with lock:
            await self._complete_event_locked(
                binding,
                seq,
                terminal_status=terminal_status,
                source_id=source_id,
                canonical_event_id=canonical_event_id,
                reason=reason,
            )

    @staticmethod
    def _terminal_evidence_valid(status: str, evidence: dict[str, Any]) -> bool:
        if status not in TERMINAL_EVENT_STATES or evidence.get("status") != status:
            return False
        if status == "posted":
            return (
                isinstance(evidence.get("canonicalEventId"), str)
                and bool(evidence["canonicalEventId"])
                and type(evidence.get("canonicalSeq")) is int
                and evidence["canonicalSeq"] > 0
                and _valid_canonical_timestamp(evidence.get("canonicalTs"))
            )
        return bool(str(evidence.get("reason") or ""))

    async def _complete_event_locked(
        self,
        binding: RoomBinding,
        seq: int,
        *,
        terminal_status: str | None,
        source_id: str,
        canonical_event_id: str,
        reason: str,
    ) -> None:
        key = str(seq)
        canonical_seq = 0
        canonical_ts = ""
        if terminal_status == "posted":
            journal_receipt = (binding.delivery_lifecycle.get(source_id) or {}).get("receipt") or {}
            intent_receipt = (binding.delivery_intents.get(source_id) or {}).get("canonical_event") or {}
            receipt_id = journal_receipt.get("canonical_event_id") or intent_receipt.get("id")
            receipt_seq = journal_receipt.get("canonical_seq") if "canonical_seq" in journal_receipt else intent_receipt.get("seq")
            receipt_ts = journal_receipt.get("canonical_ts") if "canonical_ts" in journal_receipt else intent_receipt.get("ts")
            if (
                not isinstance(receipt_id, str) or not receipt_id
                or receipt_id != canonical_event_id
                or type(receipt_seq) is not int or receipt_seq <= 0
                or not _valid_canonical_timestamp(receipt_ts)
            ):
                raise ValueError(f"Room sequence {seq} has no complete canonical receipt")
            canonical_seq = receipt_seq
            canonical_ts = receipt_ts
        if seq <= binding.acknowledged_cursor:
            binding.inbox = {
                key: status for key, status in binding.inbox.items()
                if int(key) > binding.acknowledged_cursor
            }
            binding.pending_since = {
                key: started for key, started in binding.pending_since.items()
                if int(key) > binding.acknowledged_cursor and binding.inbox.get(key) == "pending"
            }
            binding.pending_retries = {
                key: retries for key, retries in binding.pending_retries.items()
                if int(key) > binding.acknowledged_cursor
            }
            binding.terminal_evidence = {
                key: evidence for key, evidence in binding.terminal_evidence.items()
                if int(key) > binding.acknowledged_cursor
            }
            completed_sources = [
                completed_source_id
                for completed_source_id, source_seq in binding.turn_sequences.items()
                if source_seq <= binding.acknowledged_cursor
            ]
            for completed_source_id in completed_sources:
                binding.turn_sequences.pop(completed_source_id, None)
                binding.turn_observed.pop(completed_source_id, None)
                # Never infer delivery from cursor position. A legacy false
                # acknowledgement may have overtaken an uncommitted intent;
                # only explicit terminal evidence is allowed to remove it.
            if terminal_status is not None:
                evidence = {
                    "status": terminal_status,
                    "sourceEventId": source_id,
                    "canonicalEventId": canonical_event_id,
                    "canonicalSeq": canonical_seq,
                    "canonicalTs": canonical_ts,
                    "reason": reason,
                }
                if not self._terminal_evidence_valid(terminal_status, evidence):
                    raise ValueError(f"Room sequence {seq} has no valid terminal evidence")
                # A late final callback is explicit terminal evidence. It may
                # arrive after another callback already advanced the source
                # cursor; in that exact case the selected intent must be
                # removed rather than preserved as replayable work.
                binding.turn_sequences.pop(source_id, None)
                binding.turn_observed.pop(source_id, None)
                binding.delivery_intents.pop(source_id, None)
                journal = binding.delivery_lifecycle.get(source_id) or {}
                if journal.get("lifecycle_state") not in {"pending", "blocked"}:
                    binding.delivery_lifecycle.pop(source_id, None)
                _release_cycle_attempt_owner(binding, source_id)
            self._persist_binding(binding)
            return
        status = terminal_status or str(binding.inbox.get(key) or "")
        evidence = dict(binding.terminal_evidence.get(key) or {})
        if terminal_status is not None:
            evidence = {
                "status": terminal_status,
                "sourceEventId": source_id,
                "canonicalEventId": canonical_event_id,
                "canonicalSeq": canonical_seq,
                "canonicalTs": canonical_ts,
                "reason": reason,
            }
            _release_cycle_attempt_owner(binding, source_id)
        if not self._terminal_evidence_valid(status, evidence):
            raise ValueError(f"Room sequence {seq} has no valid terminal evidence")
        binding.inbox[key] = status
        binding.terminal_evidence[key] = evidence
        binding.pending_since.pop(key, None)
        binding.pending_retries.pop(key, None)
        completed_sources = [
            current_source for current_source, source_seq in binding.turn_sequences.items()
            if source_seq == seq
        ]
        if source_id and source_id not in completed_sources:
            completed_sources.append(source_id)
        for source_id in completed_sources:
            binding.turn_sequences.pop(source_id, None)
            binding.turn_observed.pop(source_id, None)
            # This removal is justified by the terminal evidence persisted in
            # the same state snapshot, never by cursor advancement alone.
            binding.delivery_intents.pop(source_id, None)
            journal = binding.delivery_lifecycle.get(source_id) or {}
            if journal.get("lifecycle_state") not in {"pending", "blocked"}:
                binding.delivery_lifecycle.pop(source_id, None)
        # Persist completion evidence before the network acknowledgement. An
        # ambiguous timeout can then retry the idempotent ack without rerunning
        # Hermes or losing the durable completion marker.
        if not self._persist_binding(binding):
            return
        cursor = binding.acknowledged_cursor
        while True:
            next_key = str(cursor + 1)
            next_status = str(binding.inbox.get(next_key) or "")
            next_evidence = dict(binding.terminal_evidence.get(next_key) or {})
            if not self._terminal_evidence_valid(next_status, next_evidence):
                break
            cursor += 1
        if cursor == binding.acknowledged_cursor:
            return
        response = await self._call(binding, lambda api: api.acknowledge(binding.room_id, cursor))
        authoritative = (response or {}).get("acknowledgedSeq")
        if type(authoritative) is not int or authoritative != cursor:
            raise ProtocolError(
                "Room acknowledgement did not confirm the locally proven contiguous frontier",
                code="ack_frontier_mismatch", retryable=True,
            )
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
        binding.pending_retries = {
            key: retries for key, retries in binding.pending_retries.items()
            if int(key) > cursor
        }
        binding.terminal_evidence = {
            key: evidence for key, evidence in binding.terminal_evidence.items()
            if int(key) > cursor
        }
        completed_sources = [
            source_id for source_id, source_seq in binding.turn_sequences.items()
            if source_seq <= cursor
        ]
        for source_id in completed_sources:
            binding.turn_sequences.pop(source_id, None)
            binding.turn_observed.pop(source_id, None)
        self._persist_binding(binding)
        pending_by_room = getattr(self, "_context_activity_pending", {})
        pending = pending_by_room.get(binding.room_id, {})
        for source_seq in sorted(seq for seq in pending if seq <= cursor):
            source_id = pending.pop(source_seq)
            await self._publish(
                binding, source_id, "context_acknowledged",
                source_seq=source_seq, suppress_errors=True,
            )
        if not pending:
            pending_by_room.pop(binding.room_id, None)

    async def _expire_stale_pending(self, binding: RoomBinding) -> None:
        """Retry stale work and quarantine it fail-closed after exhaustion."""
        now = time.time()
        stale = [
            int(key) for key, status in binding.inbox.items()
            if status == "pending" and now - float(binding.pending_since.get(key, 0.0)) >= PENDING_EVENT_TTL_SECONDS
        ]
        for seq in sorted(stale):
            key = str(seq)
            retries = int(binding.pending_retries.get(key, 0)) + 1
            binding.pending_retries[key] = retries
            binding.pending_since.pop(key, None)
            if retries <= PENDING_EVENT_MAX_RETRIES:
                binding.inbox[key] = "failed-retryable"
                logger.warning(
                    "Room %s retrying unprocessed event sequence %s after %.0fs (attempt %s/%s)",
                    binding.room_id, seq, PENDING_EVENT_TTL_SECONDS,
                    retries, PENDING_EVENT_MAX_RETRIES,
                )
            else:
                binding.inbox[key] = "quarantined"
                logger.error(
                    "Room %s quarantined unprocessed event sequence %s after %s bounded retries; acknowledgement remains at %s",
                    binding.room_id, seq, PENDING_EVENT_MAX_RETRIES,
                    binding.acknowledged_cursor,
                )
            event_id = next((event_id for event_id, event_seq in self._event_seq.get(binding.room_id, {}).items() if event_seq == seq), "")
            self._inflight_events.discard(event_id)
            active_rooms = getattr(self, "_active_dispatch_rooms", {})
            generation = getattr(self, "_event_dispatch_generation", {}).get(event_id, "")
            if active_rooms.get(binding.room_id) == generation:
                active_rooms.pop(binding.room_id, None)
            if getattr(self, "_event_dispatch_generation", {}).get(event_id) == generation:
                getattr(self, "_event_dispatch_generation", {}).pop(event_id, None)
            self._persist_binding(binding)

    def _record_delivery_failure(
        self,
        binding: RoomBinding,
        source_id: str,
        error: str,
        retryable: bool,
        error_code: str = "",
    ) -> None:
        """Persist a failed delivery boundary without advancing acknowledgement."""
        intent = binding.delivery_intents.setdefault(source_id, {})
        if intent.get("delivery_state") == "posted" and (intent.get("canonical_event") or {}).get("id"):
            raise ValueError("canonical delivery cannot be downgraded to delivery failure")
        intent["delivery_state"] = "delivery_pending" if retryable else "quarantined"
        intent["lifecycle_state"] = "not_started"
        intent["state"] = "failed-retryable" if retryable else "quarantined"
        intent["last_error"] = error[:1000]
        intent["last_error_code"] = str(error_code or "")[:100]
        intent["failed_at"] = time.time()
        seq = int(
            binding.turn_sequences.get(source_id)
            or getattr(self, "_event_seq", {}).get(binding.room_id, {}).get(source_id, 0)
        )
        if seq > binding.acknowledged_cursor:
            key = str(seq)
            retries = int(binding.pending_retries.get(key, 0)) + 1
            binding.pending_retries[key] = retries
            binding.pending_since.pop(key, None)
            if retryable and retries <= PENDING_EVENT_MAX_RETRIES:
                binding.inbox[key] = "failed-retryable"
            else:
                binding.inbox[key] = "quarantined"
                logger.error(
                    "Room %s quarantined failed delivery for sequence %s; acknowledgement remains at %s",
                    binding.room_id, seq, binding.acknowledged_cursor,
                )
        self._persist_binding(binding)

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
        """Persist runtime fields without a cross-process lost-update window."""
        def merge(latest: PluginState) -> bool:
            existing = latest.binding(binding.room_id)
            if existing is None:
                return False
            same_generation = self._same_generation(existing, binding)
            if not same_generation or not existing.enabled or existing.revoked:
                binding.enabled = existing.enabled
                binding.revoked = existing.revoked
                return False
            existing.connector_session_id = binding.connector_session_id
            existing.message_payload_dialect = binding.message_payload_dialect
            existing.message_payload_capabilities = list(binding.message_payload_capabilities)
            existing.cursor = binding.cursor
            existing.acknowledged_cursor = binding.acknowledged_cursor
            existing.inbox = dict(binding.inbox)
            existing.pending_since = dict(binding.pending_since)
            existing.pending_retries = dict(binding.pending_retries)
            existing.terminal_evidence = {
                key: dict(evidence) for key, evidence in binding.terminal_evidence.items()
            }
            existing.turn_observed = dict(binding.turn_observed)
            existing.turn_sequences = dict(binding.turn_sequences)
            existing.delivery_intents = {
                source_id: dict(intent) for source_id, intent in binding.delivery_intents.items()
            }
            existing.delivery_lifecycle = copy.deepcopy(binding.delivery_lifecycle)
            existing.cycle_attempt_owners = dict(binding.cycle_attempt_owners)
            existing.abandoned_delivery_intents = {
                source_id: dict(receipt)
                for source_id, receipt in binding.abandoned_delivery_intents.items()
            }
            if (
                not existing.rotate_current_epoch_session
                or getattr(binding, "_consuming_epoch_session_rotation", False)
            ):
                existing.epoch_session_routing_initialized = binding.epoch_session_routing_initialized
                existing.legacy_session_epoch_id = binding.legacy_session_epoch_id
                existing.rotate_current_epoch_session = binding.rotate_current_epoch_session
            existing.transport = binding.transport
            return True

        return update(merge)

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
        run_for_event = getattr(self, "_run_for_event", None)
        if run_for_event is None:
            run_for_event = {}
            self._run_for_event = run_for_event
        activity_seq = getattr(self, "_activity_seq", None)
        if activity_seq is None:
            activity_seq = {}
            self._activity_seq = activity_seq
        run_id = run_for_event.get(source_id) or ("hermes:" + uuid.uuid4().hex)
        run_for_event[source_id] = run_id
        stream_seq = activity_seq.get(source_id, 0) + 1
        activity_seq[source_id] = stream_seq
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
        def revoke_generation(latest: PluginState) -> PluginState:
            existing = latest.binding(binding.room_id)
            if existing is not None and self._same_generation(existing, binding):
                existing.revoked = True
                existing.enabled = False
                existing.connector_session_id = ""
            return latest

        self._state = update(revoke_generation)

    def _expire(self, binding: RoomBinding) -> None:
        """Stop an expired binding without turning expiry into revocation."""
        binding.enabled = False
        binding.connector_session_id = ""
        _connected_rooms.discard(binding.room_id)

        def expire_generation(latest: PluginState) -> PluginState:
            existing = latest.binding(binding.room_id)
            if existing is not None and self._same_generation(existing, binding):
                # A concurrent task may already have persisted an explicit
                # credential_revoked result for this generation. Expiry is
                # weaker evidence and must never clear that terminal marker.
                binding.revoked = existing.revoked
                existing.enabled = False
                existing.connector_session_id = ""
            return latest

        self._state = update(expire_generation)


def _restore_escaped_json_layout(candidate: str) -> str:
    """Restore JSON whitespace escaped by a model outside string values."""
    restored: list[str] = []
    in_string = False
    escaped = False
    index = 0
    whitespace = {"n": "\n", "r": "\r", "t": "\t"}
    while index < len(candidate):
        char = candidate[index]
        if in_string:
            restored.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
            restored.append(char)
            index += 1
            continue
        if char == "\\" and index + 1 < len(candidate) and candidate[index + 1] in whitespace:
            restored.append(whitespace[candidate[index + 1]])
            index += 2
            continue
        restored.append(char)
        index += 1
    return "".join(restored)


def extract_visible_body(content: str) -> str | None:
    """Return only user-facing prose from plain or fenced structured output."""
    value = (content or "").strip()
    fenced = _FENCE.match(value)
    candidate = fenced.group(1).strip() if fenced else value
    if candidate.startswith("{"):
        envelope = None
        for encoded in (candidate, _restore_escaped_json_layout(candidate)):
            try:
                envelope, offset = json.JSONDecoder().raw_decode(encoded)
                # A few models append an extra closing brace after an otherwise
                # valid envelope. Accept only that narrow harmless remainder;
                # arbitrary trailing prose remains visible instead of being
                # silently discarded.
                remainder = encoded[offset:].strip()
                if remainder and set(remainder) != {"}"}:
                    envelope = None
                if envelope is not None:
                    break
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
        platform_hint=(
            "Shared multi-agent Room; speak only as your configured Hermes identity. For an "
            "inbound Room turn, return the contribution directly. Never call "
            "synthetic_sociality_room_post from inside a Room turn because this adapter already "
            "owns canonical delivery."
        ),
    )
    ctx.register_hook("pre_llm_call", _on_pre_llm_call)
    ctx.register_hook("pre_tool_call", _on_pre_tool_call)
    ctx.register_hook("on_session_end", _on_session_end)
    ctx.register_hook("on_session_finalize", _on_session_finalize)
    ctx.register_cli_command(
        name="room",
        help="Join and manage Synthetic Sociality rooms",
        description="Identity-preserving Synthetic Sociality Room connector",
        setup_fn=cli.setup_cli,
        handler_fn=cli.dispatch,
    )
    ctx.register_tool(
        name="synthetic_sociality_room_context",
        toolset="synthetic_sociality_room",
        schema=ROOM_CONTEXT_SCHEMA,
        handler=room_context,
        description=(
            "Read the bounded canonical transcript and named roster context of a Synthetic "
            "Sociality Room configured for this Hermes profile. Use when a user asks from "
            "Telegram or another Hermes channel what happened in the Room. This tool is read-only."
        ),
        emoji="◌",
    )
    ctx.register_tool(
        name="synthetic_sociality_room_post",
        toolset="synthetic_sociality_room",
        schema=ROOM_POST_SCHEMA,
        handler=room_post,
        description=(
            "Post one explicit user-approved contribution from Telegram or another Hermes channel "
            "to a Room configured for this profile. Uses the universal Room turn and idempotency "
            "contract. Never call during an inbound Synthetic Sociality Room turn; return that "
            "turn's contribution directly because the platform adapter owns delivery. Never call "
            "merely to inspect context or continue a conversation autonomously."
        ),
        emoji="◌",
    )

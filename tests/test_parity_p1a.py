"""Parity release P1a: connectors carry no behavioural rules.

Each ``test_p1a_*`` case carries the shared conformance name from
``conformance.json`` ``parityP1aCases``; OpenClaw implements the same names.
Owner decision 2026-10-01: how to deliberate, whether to agree or to agree to
disagree, is the model's decision and the Room's policy, never the connector's.
Every prompt below is built by the real ``_dispatch_room_event``.
"""
from __future__ import annotations

import asyncio
import json
import re
import types
import unittest
from pathlib import Path

from test_delivery_lifecycle import ROOT, adapter

# Shared deny-list (PROMPTS-20261001-parity-P1a.md, case P1a-01).
DENY = ("consensus", "common ground", "synthes", "otherwise pass", "only if",
        "meaningful", "manufacture", "do not repeat", "force", "agree")
DENY_PATTERN = re.compile("|".join(re.escape(word) for word in DENY), re.IGNORECASE)

EPOCH = {"id": "epoch-2", "ordinal": 2, "startsAtSeq": 41, "status": "active",
         "topic": {"title": "Patient and doctor", "epochId": "epoch-2"}}
START = {"id": "start-2", "seq": 41, "type": "discussion.started", "actorRole": "human_owner",
         "actorId": "owner-1", "payload": {"epoch": EPOCH}}
HUMAN = {"id": "m-42", "seq": 42, "type": "message.posted", "actorRole": "human_owner",
         "actorId": "owner-1", "payload": {"body": "Should the doctor disclose the diagnosis?"}}
PEER = {"id": "m-43", "seq": 43, "type": "message.posted", "actorRole": "participant_agent",
        "actorId": "peer-1", "payload": {"body": "Disclosure first, then support.",
                                         "resolvedRecipientMembershipIds": ["member-1"]}}


def attempt_ready(seq, round_number, source="m-42", **extra):
    return {"id": f"ready-{seq}", "seq": seq, "type": "discussion.cycle_attempt_ready",
            "actorRole": "system", "actorId": "room_coordinator",
            "payload": {"cycleId": "cycle-1", "epochId": "epoch-2", "generation": 1,
                        "membershipId": "member-1", "round": round_number,
                        "sourceEventId": source, **extra}}


def command(seq, name, **extra):
    return {"id": f"cmd-{seq}", "seq": seq, "type": "human.command", "actorRole": "human_owner",
            "actorId": "owner-1", "payload": {"command": {"command": name, **extra}, "visibleText": f"/{name}"}}


def cycle(total_turns=6, used=1, **budgets):
    return {"cycle": {"id": "cycle-1", "generation": 1, "totalTurns": used,
                      "budgets": {"totalTurns": total_turns, "perAgentTurns": 3, **budgets}},
            "attempt": {"id": "attempt-1", "round": 1}}


def build_prompt(event, *, attempt=None, policy=None, rules=None, context=(START, HUMAN, PEER),
                 epoch_id="epoch-2", starts_at=41, topic=None):
    """Return the exact prompt text the adapter hands to Hermes for ``event``."""
    captured = []
    instance = object.__new__(adapter.SyntheticSocialityAdapter)
    instance.platform = adapter.Platform(adapter.NAME)
    instance._inflight_events, instance._event_seq, instance._event_epoch = set(), {}, {}
    instance._latest_source, instance._run_for_event, instance._epoch_handovers = {}, {}, {}
    instance._cycle_attempts = {event["id"]: attempt} if attempt else {}
    state = {
        "title": "Showroom", "purpose": "Patient and doctor use case", "headSeq": 60,
        "activeEpoch": {"id": epoch_id, "startsAtSeq": starts_at}, "rules": list(rules or []),
        **({"activeTopic": topic} if topic else {}),
        "roster": [{"membershipId": "owner-1", "displayName": "Owner"},
                   {"membershipId": "peer-1", "displayName": "Peer"},
                   {"membershipId": "member-1", "displayName": "Agent"}],
    }
    api = types.SimpleNamespace(
        room_state=lambda _room: state,
        room_policy=lambda _room: {"policy": dict(policy or {})},
        events=lambda _room, after, _wait: {"events": [item for item in context if item["seq"] > after]},
        artifacts=lambda _room: {"items": []},
        artifact=lambda _room, _artifact: {},
    )
    instance._call = lambda _binding, operation: asyncio.sleep(0, result=operation(api))
    instance._publish = lambda *_args, **_kwargs: asyncio.sleep(0)
    instance.build_source = types.MethodType(
        lambda self, **kwargs: types.SimpleNamespace(platform=self.platform, **kwargs), instance,
    )
    instance.handle_message = lambda message: asyncio.sleep(0, result=captured.append(message))
    binding = adapter.RoomBinding("https://room.example/api", "room-1", "member-1", "credential",
                                  installation_id="inst-1", display_name="Agent")
    binding.epoch_session_routing_initialized = True
    binding.legacy_session_epoch_id = "epoch-1"
    instance._event_epoch[event["id"]] = epoch_id
    assert asyncio.run(instance._dispatch_room_event(binding, event, "generation"))
    return captured[0].text


def every_prompt(**options):
    """Every prompt shape the Hermes connector can build."""
    return {
        "opening": build_prompt(attempt_ready(50, 1), attempt=cycle(), **options),
        "follow_up": build_prompt(attempt_ready(51, 2, source="m-43"),
                                  attempt={**cycle(), "attempt": {"id": "attempt-2", "round": 2}}, **options),
        "summary": build_prompt(command(52, "summarize"), attempt=cycle(), **options),
        "wrap_up": build_prompt(command(53, "summarize"), **options),
        "autonomous_fallback": build_prompt(attempt_ready(54, 2, source="not-in-context"), attempt=cycle(), **options),
        "initial_greeting": build_prompt(
            command(55, "ask", idempotencyKey="room-initial-greeting:v1:x", arguments={"instruction": "Hello"}),
            attempt=cycle(), **options),
        "human_reply": build_prompt({**HUMAN, "id": "m-56", "seq": 56}, **options),
        "summary_for_self": build_prompt(
            {"id": "term-57", "seq": 57, "type": "discussion.cycle_terminal", "actorRole": "system",
             "actorId": "room_coordinator", "payload": {"cycleId": "cycle-1", "state": "completed",
                                                         "epochId": "epoch-2"}}, **options),
    }


def tool_texts(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "description" and isinstance(child, str):
                yield child
            else:
                yield from tool_texts(child)
    elif isinstance(value, list):
        for child in value:
            yield from tool_texts(child)


class ParityP1aTests(unittest.TestCase):
    def assert_no_behavioural_wording(self, name, text):
        hits = sorted({match.group(0).lower() for match in DENY_PATTERN.finditer(text)})
        self.assertEqual(hits, [], f"{name}: connector-authored behavioural wording {hits}")

    def test_p1a_01_no_connector_authored_behavioural_wording_in_any_prompt(self):
        for name, prompt in every_prompt().items():
            with self.subTest(prompt=name):
                self.assert_no_behavioural_wording(name, prompt)
        # Cross-channel and tool framing: every tool description the model sees.
        tools = importlib_room_tools()
        for schema_name in ("ROOM_CONTEXT_SCHEMA", "ROOM_POST_SCHEMA", "ROOM_HISTORY_SCHEMA"):
            for text in tool_texts(getattr(tools, schema_name)):
                with self.subTest(tool=schema_name):
                    self.assert_no_behavioural_wording(schema_name, text)
        # Allowed only inside Room-supplied text: the same scan passes once the
        # Room's own words are taken out, and those words arrive intact.
        room_text = "Seek consensus only if the claims support it; agreeing to disagree is fine."
        prompts = every_prompt(rules=[{"enforcement": "guidance", "text": room_text}])
        for name, prompt in prompts.items():
            with self.subTest(room_text=name):
                self.assertIn(room_text, prompt)
                self.assert_no_behavioural_wording(name, prompt.replace(room_text, ""))

    def test_p1a_02_room_phase_instruction_and_policy_guidance_passed_verbatim_with_source_label(self):
        phase_text = "Room phase: weigh both positions and say where you differ. Agreement is optional."
        rule_text = "Owner rule: cite the patient's own words when you rely on them."
        prompt = build_prompt(
            attempt_ready(50, 2, phase="debate", phaseInstruction=phase_text),
            attempt=cycle(), rules=[{"enforcement": "guidance", "text": rule_text}],
            policy={"topicDrift": "allowed"},
        )
        self.assertIn("[Autonomous discussion phase debate, round 1, turn 2/6]", prompt)
        self.assertIn(f"[Room phase instruction — server-provided, verbatim]\n{phase_text}\n[/Room phase instruction]", prompt)
        self.assertIn("[Active Room guidance — source: the Room's rules and policy, owner-controlled; "
                      "passed verbatim by the connector]", prompt)
        self.assertIn(f"- {rule_text}", prompt)
        self.assertIn("Topic drift policy: allowed.", prompt)
        # The Open Exchange preamble is the Room's text, recognised by digest only.
        context = importlib_context()
        self.assertFalse(hasattr(context, "OPEN_EXCHANGE_PREAMBLE"))
        preamble = (ROOT / "tests" / "fixtures" / "open-exchange-preamble-v1.txt").read_text().rstrip("\n")
        import hashlib
        self.assertEqual(hashlib.sha256(preamble.encode()).hexdigest(), context.OPEN_EXCHANGE_PREAMBLE_SHA256)
        with_preamble = build_prompt(attempt_ready(50, 1), attempt=cycle(),
                                     rules=[{"enforcement": "guidance", "text": preamble}])
        self.assertIn(f"[{context.OPEN_EXCHANGE_PREAMBLE_VERSION}; sha256={context.OPEN_EXCHANGE_PREAMBLE_SHA256}]\n{preamble}\n", with_preamble)

    def test_p1a_03_without_room_instructions_only_procedural_facts_are_present(self):
        for name, prompt in every_prompt().items():
            with self.subTest(prompt=name):
                self.assertNotIn("[Room phase instruction", prompt)
                self.assertNotIn("[Active Room guidance", prompt)
                tail = (f"{adapter.ROOM_HISTORY_INSTRUCTION} {adapter.ROOM_IDENTITY_FACT} "
                        f"{adapter.ROOM_RESPONSE_INSTRUCTION}")
                self.assertTrue(prompt.endswith(tail), name)
        prompts = every_prompt()
        # Phase name and budget as numbers, nothing after them.
        self.assertIn("[Autonomous discussion phase opening, round 1, turn 2/6]\n\n", prompts["opening"])
        # A peer's directly addressed message adds only the trust-boundary fact.
        self.assertIn("[Autonomous discussion phase follow_up, round 2, turn 2/6]"
                      + adapter._direct_peer_turn_instruction(PEER, PEER["payload"], "member-1") + "\n\n",
                      prompts["follow_up"])
        self.assertIn("[Autonomous discussion phase summary, round 1, turn 2/6]\n\n", prompts["summary"])
        self.assertIn("[Autonomous discussion phase initial_greeting, round 1, turn 2/6]\n\n", prompts["initial_greeting"])
        self.assertIn("Owner: /summarize\n", prompts["wrap_up"])
        self.assertIn("\n\n[Room scheduler: your attempt in the current discussion cycle is ready; "
                      "its source event not-in-context is not available to the connector]\n\n",
                      prompts["autonomous_fallback"])
        for name in ("autonomous_fallback", "summary_for_self"):
            self.assertNotIn("Room participant:", prompts[name])
        self.assertIn("[Room policy summaryBehavior=on_cycle_complete names you summary coordinator; "
                      "discussion cycle cycle-1 ended: completed]", prompts["summary_for_self"])

    def test_p1a_04_pass_envelope_documented_technically_without_usage_advice(self):
        instruction = adapter.ROOM_RESPONSE_INSTRUCTION
        self.assertEqual(instruction, (
            'Reply format: to pass, return exactly {"action":"skip"}. Any other reply is posted '
            "to the Room as your contribution, as plain natural text, not wrapped in JSON, XML or a code fence."
        ))
        for advice in ("if ", "when", "appropriate", "otherwise", "only", "should", "nothing to add"):
            self.assertNotIn(advice, instruction.lower())

    def test_p1a_05_policy_word_limit_stated_as_number_on_every_turn_when_set_in_open_and_coordinated_modes(self):
        open_turns = [build_prompt({**HUMAN, "id": f"m-{seq}", "seq": seq}, policy={
            "coordinationMode": "open", "maxContributionWords": 120}) for seq in (56, 57)]
        for prompt in open_turns:
            self.assertIn("Word limit per response: 120 words (Room policy).", prompt)
        coordinated = build_prompt(attempt_ready(50, 1), attempt=cycle(maxContributionWords=80),
                                   policy={"coordinationMode": "coordinated", "maxContributionWords": 120})
        self.assertIn("Word limit per response: 80 words (cycle budget).", coordinated)
        self.assertNotIn("120 words", coordinated)
        coordinated_policy_only = build_prompt(attempt_ready(50, 1), attempt=cycle(),
                                               policy={"coordinationMode": "coordinated", "maxContributionWords": 120})
        self.assertIn("Word limit per response: 120 words (Room policy).", coordinated_policy_only)
        for unset in ({}, {"maxContributionWords": 0}, {"maxContributionWords": True}, {"maxContributionWords": "90"}):
            with self.subTest(policy=unset):
                self.assertNotIn("Word limit", build_prompt({**HUMAN, "id": "m-58", "seq": 58}, policy=unset))


class DiscussionStartTests(unittest.TestCase):
    def test_p1a_06_discussion_start_title_and_description_presented_verbatim_and_attributed_to_the_starting_human(self):
        fixture = json.loads((ROOT / "tests" / "fixtures" / "showroom-discussion-start" / "seq-12-14.canonical.json").read_text())
        start, _cycle_started, ready = fixture["events"]
        topic = start["payload"]["epoch"]["topic"]
        attempt = {"cycle": {"id": "cycle-show", "generation": 1, "totalTurns": 0,
                             "budgets": {"totalTurns": 6, "perAgentTurns": 3}},
                   "attempt": {"id": "attempt-14", "round": 1}}
        prompt = build_prompt(ready, attempt=attempt, context=tuple(fixture["events"][:2]),
                              epoch_id="epoch-show", starts_at=12,
                              topic={"id": topic["id"], "title": topic["title"], "description": topic["description"]})
        # Current-turn input: the topic itself, verbatim, from the starting human.
        self.assertIn(f"\n\n{fixture['starterDisplayName']}: {topic['title']}\n\n{topic['description']}\n\n[Autonomous discussion phase opening",
                      prompt)
        for invented in ("Room participant", "system:", "Room scheduler", "Continue the autonomous"):
            self.assertNotIn(invented, prompt)
        # Context: the description under "Current discussion".
        self.assertIn(f"Current discussion: {topic['title']}\n{topic['description']}\n", prompt)


def core_status_sender():
    """Hermes core's own ``_send_or_update_status_coro`` (gateway/run.py), compiled
    from its source: the module itself needs dependencies this venv lacks."""
    import ast
    import inspect
    from test_delivery_lifecycle import hermes_build_session_key
    path = Path(inspect.getfile(hermes_build_session_key)).with_name("run.py")
    tree = ast.parse(path.read_text())
    node = next(item for item in tree.body
                if isinstance(item, ast.AsyncFunctionDef) and item.name == "_send_or_update_status_coro")
    namespace: dict = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["_send_or_update_status_coro"]


class HostNoticeTests(unittest.TestCase):
    def test_p1a_07_host_fallback_notice_is_never_posted_the_model_reply_is(self):
        from test_parity_p1 import cycle_attempt, room_instance, source_ref
        fixture = json.loads((ROOT / "tests" / "fixtures" / "host-fallback-notice" / "notice-then-model.json").read_text())
        notice, model = fixture["payloads"]
        send_status = core_status_sender()

        async def run():
            instance, binding, api, _published, _dispatched = room_instance(cycle_attempt())
            bodies, post = [], api.post_message

            def recording_post(room_id, turn_id, observed, source_id, body, *args, **kwargs):
                bodies.append(body)
                return post(room_id, turn_id, observed, source_id, body, *args, **kwargs)

            api.post_message = recording_post
            # The Room adapter has no send_or_update_status, so core falls back to send().
            self.assertFalse(callable(getattr(instance, "send_or_update_status", None)))
            metadata = {key: (source_ref() if value == "SOURCE_REF" else value)
                        for key, value in notice["metadata"].items()}
            notice_result = await send_status(instance, "room-1", notice["statusKey"], notice["content"], metadata)
            after_notice = (list(api.posts), dict(instance._buffered_output), list(api.completions))
            model_result = await instance.send(
                "room-1", model["content"], reply_to=source_ref(), metadata=model["metadata"],
            )
            return instance, binding, api, notice_result, after_notice, model_result, bodies

        instance, binding, api, notice_result, after_notice, model_result, bodies = asyncio.run(run())
        # The notice reaches the adapter without a canonical source: refused,
        # neither posted nor buffered as a preview that could be finalised.
        self.assertFalse(notice_result.success)
        self.assertEqual(after_notice, ([], {}, []))
        # The model's reply is the one contribution, posted once.
        self.assertTrue(model_result.success)
        self.assertEqual(len(api.posts), 1)
        self.assertEqual(bodies, ["TJE — the unresolved thread that sticks with me…"])  # marker removed
        self.assertEqual([payload["action"] for _id, payload in api.completions], ["contribute"])
        self.assertEqual(instance._terminal_results[source_ref()]["status"], "posted")
        self.assertNotIn("Model Fallback", json.dumps(binding.delivery_intents, ensure_ascii=False))


def importlib_room_tools():
    import sys
    return sys.modules[adapter.__name__.rsplit(".", 1)[0] + ".room_tools"]


def importlib_context():
    import sys
    return sys.modules[adapter.__name__.rsplit(".", 1)[0] + ".context"]


if __name__ == "__main__":
    unittest.main()

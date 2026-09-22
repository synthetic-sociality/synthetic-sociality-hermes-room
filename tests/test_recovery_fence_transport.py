"""Recovery uses the transport and normal receipt/ack path, never model replay."""
import asyncio
import copy
import unittest
from unittest.mock import AsyncMock
from test_delivery_lifecycle import load_adapter


class FenceTransportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.module = load_adapter()
        self.adapter = object.__new__(self.module.SyntheticSocialityAdapter)
        self.binding = self.module.RoomBinding(
            'https://room.example/api', 'room', 'member', 'credential',
            cursor=360, acknowledged_cursor=360, recovery_fence_cutoff=381)
        self.binding.inbox = {str(n): 'ignored' for n in range(361, 383)}
        self.binding.terminal_evidence = {
            str(n): {'status': 'ignored', 'reason': 'technical'} for n in range(361, 383)}
        for n in (361, 381):
            self.binding.inbox[str(n)] = 'quarantined'
            self.binding.terminal_evidence.pop(str(n))
        self.snapshots = []
        def persist(binding):
            self.snapshots.append(copy.deepcopy(binding))
            return True
        self.adapter._persist_binding = persist
        self.adapter.handle_message = AsyncMock(side_effect=AssertionError('model dispatch'))
        self.adapter._stop = asyncio.Event()
        self.reads, self.acks = [], []
        self.fail_ack = False
        case = self
        class API:
            def events(self, room, after, wait, **kwargs):
                case.reads.append(after)
                return {'events': [{'id': f'e{n}', 'seq': n} for n in range(after+1, min(after+8, 382)+1)]}
            def acknowledge(self, room, cursor):
                case.acks.append(cursor)
                if case.fail_ack:
                    raise OSError('lost acknowledgement')
                return {'acknowledgedSeq': cursor}
            def stream_events(self, room, after, receive):
                for event in self.events(room, after, 0)['events']:
                    receive(event)
        self.api = API()
        async def call(binding, operation):
            return operation(self.api)
        self.adapter._call = call
        self.adapter._stream_call = call

    async def test_paginated_fence_closes_holes_and_preserves_sibling(self):
        sibling = copy.deepcopy(self.binding)
        sibling.room_id = 'sibling'
        before = copy.deepcopy(sibling)
        for _ in range(4):
            await self.adapter._long_poll_once(self.binding)
        self.assertEqual(self.reads[0], 360)
        self.assertGreaterEqual(self.binding.acknowledged_cursor, 381)
        for n in (361, 381):
            self.assertTrue(any(s.terminal_evidence.get(str(n), {}).get('reason') == 'recovery_fence' for s in self.snapshots))
        self.assertEqual(sibling, before)
        self.adapter.handle_message.assert_not_called()

    async def test_stream_uses_same_frontier(self):
        await self.adapter._stream_once(self.binding)
        self.assertEqual(self.reads, [360])
        self.assertGreaterEqual(self.binding.acknowledged_cursor, 380)
        self.adapter.handle_message.assert_not_called()

    async def test_pending_retryable_and_advanced_transport_cursor_cannot_bypass_fence(self):
        for status in ('quarantined', 'pending', 'retryable', 'failed-retryable'):
            self.binding.inbox['361'] = status
            self.binding.cursor = 382
            self.assertEqual(self.adapter._read_frontier(self.binding), 360)

    async def test_no_fence_and_quarantine_outside_fence_keep_isolation(self):
        self.binding.recovery_fence_cutoff = 0
        self.assertEqual(self.adapter._read_frontier(self.binding), 382)
        self.binding.recovery_fence_cutoff = 360
        self.assertEqual(self.adapter._read_frontier(self.binding), 382)

    async def test_valid_posted_receipt_is_not_rewritten_by_fence(self):
        evidence = {'status': 'posted', 'canonicalEventId': 'post', 'canonicalSeq': 400,
                    'canonicalTs': '2026-09-22T10:00:00Z'}
        self.binding.inbox['362'] = 'posted'
        self.binding.terminal_evidence['362'] = evidence.copy()
        # Earlier hole keeps this receipt retained for inspection.
        await self.adapter._consume(self.binding, {'id': 'e362', 'seq': 362})
        self.assertEqual(self.binding.terminal_evidence['362'], evidence)
        self.assertEqual(self.binding.inbox['362'], 'posted')

    async def test_lost_ack_restart_retries_only_evidence_and_is_idempotent(self):
        self.fail_ack = True
        with self.assertRaises(OSError):
            await self.adapter._long_poll_once(self.binding)
        self.binding = copy.deepcopy(self.snapshots[-1])
        self.assertEqual(self.binding.acknowledged_cursor, 360)
        self.fail_ack = False
        for _ in range(4):
            await self.adapter._long_poll_once(self.binding)
        before = self.binding.acknowledged_cursor
        await self.adapter._long_poll_once(self.binding)
        self.assertEqual(self.binding.acknowledged_cursor, before)
        self.adapter.handle_message.assert_not_called()

    async def test_fence_refuses_selected_uncertain_and_active_work_without_mutation(self):
        cases = (
            ("delivery_intents", {"e361": {"state": "selected"}}),
            ("delivery_intents", {"e361": {"state": "quarantined", "last_error_code": "unknown_transport_outcome"}}),
            ("delivery_lifecycle", {"e361": {"lifecycle_state": "pending"}}),
            ("cycle_attempt_owners", {"cycle:attempt": "e361"}),
        )
        for attribute, value in cases:
            with self.subTest(attribute=attribute, value=value):
                setattr(self.binding, attribute, value)
                before = copy.deepcopy(self.binding)
                with self.assertRaises(self.module.ProtocolError) as caught:
                    await self.adapter._consume(self.binding, {"id": "e361", "seq": 361})
                self.assertEqual(caught.exception.code, "recovery_fence_unresolved_work")
                self.assertEqual(self.binding, before)
                self.assertEqual(self.acks, [])
                setattr(self.binding, attribute, {})

    async def test_quarantine_after_active_cutoff_is_preserved(self):
        self.binding.inbox['383'] = 'quarantined'
        for _ in range(3):
            await self.adapter._long_poll_once(self.binding)
        self.assertEqual(self.binding.acknowledged_cursor, 382)
        self.assertEqual(self.binding.inbox['383'], 'quarantined')
        self.assertEqual(self.adapter._read_frontier(self.binding), 383)

    async def test_sse_reconnect_drains_fence_through_same_ack_path(self):
        for _ in range(3):
            await self.adapter._stream_once(self.binding)
        self.assertGreaterEqual(self.binding.acknowledged_cursor, 381)
        self.assertEqual(self.reads[:2], [360, 380])
        self.adapter.handle_message.assert_not_called()

    async def test_real_state_transaction_preserves_sibling_and_cli_fence_on_restart(self):
        import importlib
        import tempfile
        from pathlib import Path
        state = importlib.import_module(self.module.__package__ + '.state')
        cli = importlib.import_module(self.module.__package__ + '.cli')
        sibling = copy.deepcopy(self.binding)
        sibling.room_id = 'sibling'
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            state.save(state.PluginState(bindings=[self.binding, sibling]), path)
            with state.state_path_scope(path):
                cli._fence('room', 381)
                self.binding = state.load().binding('room')
                del self.adapter._persist_binding  # real atomic merge, not a stub
                for _ in range(4):
                    await self.adapter._long_poll_once(self.binding)
                saved = state.load()
                self.assertGreaterEqual(saved.binding('room').acknowledged_cursor, 381)
                self.assertEqual(saved.binding('room').recovery_fence_cutoff, 381)
                self.assertEqual(saved.binding('sibling'), sibling)

    async def test_revoked_or_replaced_binding_cannot_persist_or_ack_fence(self):
        import importlib
        import tempfile
        from pathlib import Path
        state = importlib.import_module(self.module.__package__ + '.state')
        for changes in ({'revoked': True}, {'credential': 'replacement'}):
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'state.json'
                latest = copy.deepcopy(self.binding)
                for key, value in changes.items():
                    setattr(latest, key, value)
                state.save(state.PluginState(bindings=[latest]), path)
                before = path.read_bytes()
                self.adapter._persist_binding = self.module.SyntheticSocialityAdapter._persist_binding.__get__(self.adapter)
                with state.state_path_scope(path):
                    await self.adapter._consume(self.binding, {'id': 'e361', 'seq': 361})
                self.assertEqual(path.read_bytes(), before)
                self.assertEqual(self.acks, [])

    async def test_missing_evidence_or_ack_response_never_advances_ack(self):
        self.binding.inbox.pop('370')
        self.binding.terminal_evidence.pop('370')
        await self.adapter._consume(self.binding, {'id': 'e381', 'seq': 381})
        self.assertEqual(self.binding.acknowledged_cursor, 360)
        await self.adapter._consume(self.binding, {'id': 'e361', 'seq': 361})
        self.assertEqual(self.binding.acknowledged_cursor, 369)

    async def test_fence_refuses_inconsistent_legacy_alias_in_completion_scope(self):
        self.binding.turn_sequences['legacy-alias'] = 361
        self.binding.delivery_intents['legacy-alias'] = {'state': 'selected'}
        before = copy.deepcopy(self.binding)
        with self.assertRaises(self.module.ProtocolError):
            await self.adapter._consume(self.binding, {'id': 'e361', 'seq': 361})
        self.assertEqual(self.binding, before)
        self.assertEqual(self.acks, [])

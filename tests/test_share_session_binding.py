"""Active dispatch authority, using core-shaped kwargs and core record seams.

The repair corpus omits hermes_state/SessionDB and core dependencies. The DB
below models its read APIs; source tests execute the supplied core dispatcher
and the adapter's actual begin call without importing the incomplete runtime.
"""
from __future__ import annotations

import ast
import copy
from contextlib import nullcontext
import json
import re
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock

from support import ROOT, load_module

share = load_module('share_file')
origin = load_module('origin_context')
CORE = ROOT.parent / 'hermes-core-7cadfaa6'

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text(encoding="utf-8"))
CONTRACT = (ROOT / "docs/delivery-lifecycle-contract-v1.md").read_text(encoding="utf-8")
CASES = [
    'P12-01 own session shares without a hook or matching turn identifier',
    'P12-02 host-reported continuation of a bound live session can share',
    'P12-03 routing provenance or historical Room mapping grants no authority',
    'P12-04 host-reported descendant chain shares and reuses the receipt',
    'P12-05 turn end refuses bound sessions and late continuations',
    'P12-06 historical continuations cannot join a successor turn',
    'P12-07 missing identities and tool-supplied or unrecorded lineage are refused',
    'P12-08 foreign lineage cannot move authority between Rooms',
    'P12-09 unresolved dispatch without fresh core evidence and historical parent grant no authority',
    'P12-10 canonical post sealing refuses sharing and new lineage',
]


class ParityP12ConformanceTests(unittest.TestCase):
    def test_conformance_names_the_clause_and_the_shared_cases(self):
        self.assertEqual(CONFORMANCE["parityP12Cases"], CASES)
        document, anchor = CONFORMANCE["parityP12Contract"].split("#", 1)
        self.assertEqual(document, "docs/delivery-lifecycle-contract-v1.md")
        self.assertEqual(anchor, "p12--share-authority-follows-the-hosts-own-session-continuation-2026-10-26")
        headings = [line[3:] for line in CONTRACT.split("\n") if line.startswith("## ")]
        anchors = {re.sub(r"[^\w\- ]", "", title.strip().lower()).replace(" ", "-")
                   for title in headings}
        self.assertIn(anchor, anchors)
        self.assertEqual(CONTRACT.count("## P12 — Share authority follows the host's own session continuation (2026-10-26)"), 1)
        self.assertIn("\nP12-1 Host-owned continuation authority.", CONTRACT)
        self.assertIn("\nP12-2 Shared conformance.", CONTRACT)
        section = CONTRACT.split("## P12 — Share authority follows the host's own session continuation (2026-10-26)", 1)[1].split("\n## ", 1)[0]
        self.assertEqual([line[2:] for line in section.splitlines() if line.startswith("- ")], CASES)


class CoreRecords:
    """Synthetic core read APIs; canonical tips are independent of parent rows."""
    def __init__(self):
        self.rows, self.tips = {}, {}

    def get_session(self, session_id):
        return copy.deepcopy(self.rows.get(session_id))

    def get_compression_tip(self, session_id):
        return self.tips.get(session_id, session_id)

    def row(self, session):
        self.rows[session] = dict(id=session, started_at=time.time(),
            ended_at=None, end_reason=None, parent_session_id=None,
            session_key='lineage-key', model_config=None)

    def rotate(self, parent, child, reason='compression'):
        self.rows[parent].update(ended_at=time.time(), end_reason=reason)
        self.row(child)
        self.rows[child]['parent_session_id'] = parent
        if reason == 'compression':
            for ancestor, tip in list(self.tips.items()):
                if tip == parent:
                    self.tips[ancestor] = child
            self.tips[parent] = child
        else:
            self.rows[child]['model_config'] = json.dumps({'_reset_from': parent})


def source_function(path, name, namespace):
    tree = ast.parse(path.read_text())
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)
    node.decorator_list = []
    module = ast.Module(body=[ast.ImportFrom(module='__future__',
        names=[ast.alias(name='annotations')], level=0), node], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), 'exec'), namespace)
    return namespace[name]


class ShareSessionBindingTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        (self.root / 'draft.md').write_text('Synthetic share.\n')
        self.created = []
        self.db = CoreRecords()
        self.db.row('lineage-session')
        self.turn = self.begin('lineage-room', 'lineage-session')

    def begin(self, room, session, *, db=None):
        turn = share.ShareTurn({}, self.root, [], lambda: True,
            lambda *_: self.created.append(room) or
            {'artifactId': 'synthetic-artifact', 'versionId': 'synthetic-version'})
        share.begin(room, turn, session, session_db=db or self.db, session_key='lineage-key')
        self.addCleanup(share.end, room, turn)
        return turn

    def call(self, session_id=None, **extra):
        # Exact shape emitted for a normal tool by model_tools._execute_tool.
        return json.loads(share.room_share_file({'path': 'draft.md'},
            task_id='synthetic-task', session_id=session_id, user_task='Share draft.md', **extra))

    def refused(self, session_id=None, **extra):
        self.assertEqual(self.call(session_id, **extra), share.refused('no-room-turn'))

    def test_own_session_without_hook_or_turn_id(self):
        self.assertTrue(self.call('lineage-session')['success'])
        self.assertEqual(self.created, ['lineage-room'])

    def test_compression_mid_turn_then_share_without_hook(self):
        self.db.rotate('lineage-session', 'lineage-compressed')
        self.assertEqual(origin.room_for_session('lineage-compressed'), '')
        self.assertTrue(self.call('lineage-compressed')['success'])
        self.assertIn('lineage-compressed', self.turn.host_sessions)
        self.assertEqual(origin.host_session_for_room('lineage-room'), 'lineage-compressed')
        self.assertEqual(self.created, ['lineage-room'])

    def test_multiple_compressions_before_first_share(self):
        self.db.rotate('lineage-session', 'lineage-mid')
        self.db.rotate('lineage-mid', 'lineage-tip')
        self.assertTrue(self.call('lineage-tip')['success'])
        self.assertTrue(self.call('lineage-tip')['success'])
        self.assertEqual(self.created, ['lineage-room'])  # receipt is reused

    def test_rotation_before_pre_llm_hook_uses_records_without_parent_metadata(self):
        self.db.rotate('lineage-session', 'lineage-hook-current')
        self.assertTrue(share.note_host_turn('lineage-hook-current', 'hook-turn', 'lineage-room'))
        self.assertIn('lineage-hook-current', self.turn.host_sessions)
        self.assertEqual(origin.host_session_for_room('lineage-room'), 'lineage-hook-current')
        self.assertTrue(self.call('lineage-hook-current')['success'])

    def test_gateway_reset_between_peek_and_handle_message(self):
        for reason in ('session_reset', 'suspended'):
            with self.subTest(reason=reason):
                self.db.rotate('lineage-session', 'lineage-reset-' + reason, reason)
                self.assertTrue(self.call('lineage-reset-' + reason)['success'])
        self.assertEqual(self.created, ['lineage-room'])

    def test_compression_then_reset_chain(self):
        self.db.rotate('lineage-session', 'lineage-mid-reset')
        self.db.rotate('lineage-mid-reset', 'lineage-reset-tip', 'session_reset')
        self.assertTrue(self.call('lineage-reset-tip')['success'])

    def test_compression_after_end_is_refused(self):
        share.end('lineage-room', self.turn)
        self.db.rotate('lineage-session', 'lineage-late')
        self.refused('lineage-late')
        self.refused('lineage-session')
        self.assertEqual(self.created, [])

    def test_previous_turn_continuations_cannot_join_successor(self):
        self.db.rotate('lineage-session', 'lineage-old-child')
        share.end('lineage-room', self.turn)
        self.db.row('lineage-successor')
        current = self.begin('lineage-room', 'lineage-successor')
        self.db.rotate('lineage-old-child', 'lineage-old-grandchild')
        for session in ('lineage-session', 'lineage-old-child', 'lineage-old-grandchild'):
            self.refused(session)
        self.assertEqual(current.host_sessions, {'lineage-successor'})
        self.assertEqual(self.created, [])

    def test_historical_child_of_bound_parent_is_refused(self):
        self.db.rotate('lineage-session', 'lineage-historical-child')
        self.db.rows['lineage-historical-child']['started_at'] = self.turn.started_at - 1
        self.refused('lineage-historical-child')
        self.assertEqual(self.created, [])

    def test_foreign_room_continuation_cannot_join_this_turn(self):
        other_db = CoreRecords()
        other_db.row('lineage-foreign-parent')
        other = self.begin('lineage-other-room', 'lineage-foreign-parent', db=other_db)
        # Conflicting same-store link to this turn is not permission to move a Room.
        self.db.rotate('lineage-session', 'lineage-foreign-child')
        origin.remember_room_session('lineage-foreign-child', 'lineage-other-room')
        self.refused('lineage-foreign-child')
        self.assertEqual(self.turn.host_sessions, {'lineage-session'})
        self.assertEqual(other.host_sessions, {'lineage-foreign-parent'})
        self.assertEqual(self.created, [])

    def test_foreign_active_ancestor_is_refused(self):
        self.db.row('lineage-other-root')
        self.begin('lineage-second-room', 'lineage-other-root')
        self.db.rotate('lineage-session', 'lineage-other-root')
        self.db.rotate('lineage-other-root', 'lineage-foreign-tip')
        self.assertTrue(self.call('lineage-foreign-tip')['success'])
        self.assertNotIn('lineage-foreign-tip', self.turn.host_sessions)
        self.assertEqual(self.created, ['lineage-second-room'])

    def test_forged_parent_metadata_is_not_authority(self):
        self.refused('lineage-forged', parent_session_id='lineage-session')
        self.assertEqual(self.created, [])
        self.assertNotIn('lineage-forged', self.turn.host_sessions)

    def test_parent_row_without_canonical_compression_is_refused(self):
        self.db.rotate('lineage-session', 'lineage-real-child')
        self.db.row('lineage-fork')
        self.db.rows['lineage-fork']['parent_session_id'] = 'lineage-session'
        self.refused('lineage-fork')  # a parent link alone is not compression
        self.assertEqual(self.created, [])

    def test_reset_requires_core_reason_key_and_reset_marker(self):
        self.db.rotate('lineage-session', 'lineage-bad-reset', 'session_reset')
        valid = copy.deepcopy(self.db.rows)
        for target, field, value in (
            ('lineage-session', 'end_reason', 'agent_close'),
            ('lineage-bad-reset', 'model_config', {}),
            ('lineage-bad-reset', 'model_config', '{bad json'),
            ('lineage-bad-reset', 'session_key', 'foreign-key'),
            ('lineage-session', 'session_key', 'foreign-key'),
        ):
            with self.subTest(target=target, field=field):
                self.db.rows = copy.deepcopy(valid)
                self.db.rows[target][field] = value
                self.refused('lineage-bad-reset')
        self.assertEqual(self.created, [])

    def test_invalid_missing_and_historical_sessions_are_refused(self):
        origin.remember_room_session('lineage-historical', 'lineage-room')
        for session in (None, 7, '', ' ', 'lineage-unknown', 'lineage-historical'):
            self.refused(session)
        self.assertEqual(self.created, [])

    def test_missing_api_or_db_failure_refuses_continuation_only(self):
        self.db.rotate('lineage-session', 'lineage-unreadable')
        for db in (None, types.SimpleNamespace(),
                   types.SimpleNamespace(get_session=mock.Mock(side_effect=OSError('unreadable'))),
                   types.SimpleNamespace(get_session=self.db.get_session)):
            self.turn.session_db = db
            self.refused('lineage-unreadable')
        self.assertTrue(self.call('lineage-session')['success'])

    def test_invalid_core_timestamps_and_ended_child_fail_closed(self):
        self.db.rotate('lineage-session', 'lineage-invalid')
        for value in (None, '', True, float('nan'), float('inf'), self.turn.started_at - 1):
            self.db.rows['lineage-invalid']['started_at'] = value
            self.refused('lineage-invalid')
        self.db.rows['lineage-invalid'].update(started_at=time.time(),
            parent_session_id='lineage-invalid', ended_at=time.time(), end_reason='compression')
        self.refused('lineage-invalid')
        self.assertEqual(self.created, [])

    def test_cycle_in_ancestor_records_fails_closed(self):
        self.db.row('lineage-cycle-a')
        self.db.row('lineage-cycle-b')
        self.db.rotate('lineage-cycle-a', 'lineage-cycle-b')
        self.db.rotate('lineage-cycle-b', 'lineage-cycle-a')
        self.db.rotate('lineage-cycle-a', 'lineage-cycle-target')
        self.db.rows['lineage-cycle-a']['parent_session_id'] = 'lineage-cycle-b'
        self.refused('lineage-cycle-target')
        self.assertEqual(self.created, [])

    def test_core_db_resolution_never_falls_back_to_ambient_home(self):
        self.assertIsNone(share.core_session_db(None, 'lineage-key'))
        self.assertIsNone(share.core_session_db(types.SimpleNamespace(_db=self.db), 'lineage-key'))
        store = types.SimpleNamespace(_db_for_key=mock.Mock(side_effect=OSError('unavailable')),
                                      _db=self.db)
        self.assertIsNone(share.core_session_db(store, 'lineage-key'))
        store._db_for_key.assert_called_once_with('lineage-key')
        self.assertIsNone(share.core_session_db(store, ''))

    def test_unresolved_turn_cannot_use_historical_parent(self):
        self.db.row('lineage-unresolved-parent')
        origin.remember_room_session('lineage-unresolved-parent', 'lineage-unresolved-room')
        unresolved = self.begin('lineage-unresolved-room', '')
        self.db.rotate('lineage-unresolved-parent', 'lineage-unresolved-child')
        self.db.rows['lineage-unresolved-child']['started_at'] = unresolved.started_at - 1
        self.refused('lineage-unresolved-child')
        self.assertEqual(unresolved.host_sessions, set())

    def unresolved(self):
        share.end('lineage-room', self.turn)
        self.turn = self.begin('lineage-room', '')
        return self.turn

    def test_fresh_session_binds_at_first_tool_call_or_hook(self):
        for entry in ('tool', 'hook'):
            with self.subTest(entry=entry):
                turn = self.unresolved()
                session = 'lineage-fresh-' + entry
                self.db.row(session)
                # Equality is intentional: core timestamps can be identical.
                self.db.rows[session]['started_at'] = turn.started_at
                if entry == 'hook':
                    self.assertTrue(share.note_host_turn(session, '', 'lineage-room'))
                    self.assertEqual(turn.host_sessions, {session})
                self.assertTrue(self.call(session)['success'])
                self.assertEqual(turn.host_sessions, {session})
                self.assertEqual(origin.host_session_for_room('lineage-room'), session)

    def test_fresh_session_needs_no_compression_api(self):
        turn = self.unresolved()
        self.db.row('lineage-fresh-reader-only')
        turn.session_db = types.SimpleNamespace(get_session=self.db.get_session)
        self.assertTrue(self.call('lineage-fresh-reader-only')['success'])
        self.assertEqual(turn.host_sessions, {'lineage-fresh-reader-only'})

    def test_fresh_session_requires_live_exact_key_row_and_finite_dispatch_timestamp(self):
        turn = self.unresolved()
        self.db.row('lineage-fresh-invalid')
        valid = copy.deepcopy(self.db.rows['lineage-fresh-invalid'])
        for field, value in (
            ('session_key', 'foreign-key'), ('session_key', 'lineage-key '),
            ('session_key', None), ('id', 'another-session'),
            ('ended_at', time.time()),
            *[('started_at', value) for value in
              (None, '', True, float('nan'), float('inf'), float('-inf'), turn.started_at - 1)],
        ):
            with self.subTest(field=field, value=value):
                self.db.rows['lineage-fresh-invalid'] = {**valid, field: value}
                self.assertFalse(share.note_host_turn('lineage-fresh-invalid', '', 'lineage-room'))
                self.refused('lineage-fresh-invalid')
                self.assertEqual(turn.host_sessions, set())
        self.assertEqual(self.created, [])

    def test_fresh_session_missing_evidence_fails_closed(self):
        turn = self.unresolved()
        self.db.row('lineage-fresh-missing')
        for db in (None, types.SimpleNamespace(),
                   types.SimpleNamespace(get_session=mock.Mock(side_effect=OSError('unreadable'))),
                   types.SimpleNamespace(get_session=lambda _: None),
                   types.SimpleNamespace(get_session=lambda _: {})):
            with self.subTest(db=db):
                turn.session_db = db
                self.assertFalse(share.note_host_turn('lineage-fresh-missing', '', 'lineage-room'))
                self.refused('lineage-fresh-missing')
                self.assertEqual(turn.host_sessions, set())
        turn.session_db, turn.session_key = self.db, ''
        self.assertFalse(share.note_host_turn('lineage-fresh-missing', '', 'lineage-room'))
        self.refused('lineage-fresh-missing')
        self.assertEqual(turn.host_sessions, set())
        self.assertEqual(self.created, [])

    def test_second_fresh_session_requires_existing_continuation_rules(self):
        turn = self.unresolved()
        self.db.row('lineage-fresh-first')
        self.assertTrue(self.call('lineage-fresh-first')['success'])
        self.db.row('lineage-fresh-second')
        self.assertFalse(share.note_host_turn('lineage-fresh-second', '', 'lineage-room'))
        self.refused('lineage-fresh-second')
        self.assertEqual(turn.host_sessions, {'lineage-fresh-first'})
        self.db.rotate('lineage-fresh-first', 'lineage-fresh-compressed')
        self.assertTrue(self.call('lineage-fresh-compressed')['success'])
        self.db.rotate('lineage-fresh-compressed', 'lineage-fresh-reset', 'session_reset')
        self.assertTrue(share.note_host_turn('lineage-fresh-reset', '', 'lineage-room'))
        self.assertTrue(self.call('lineage-fresh-reset')['success'])
        self.assertEqual(self.created, ['lineage-room'])  # one turn, one receipt

    def test_fresh_session_conflicting_with_another_room_is_refused(self):
        turn = self.unresolved()
        self.db.row('lineage-fresh-conflict')
        # The other active binding is sufficient even without historical mapping.
        other = self.begin('lineage-other-room', 'lineage-fresh-conflict')
        with mock.patch.object(origin, 'room_for_session', return_value=''):
            self.assertFalse(share.note_host_turn('lineage-fresh-conflict', '', 'lineage-room'))
            self.assertFalse(share._continuation('lineage-room', turn, 'lineage-fresh-conflict'))
            other.sealed = lambda: True
            self.refused('lineage-fresh-conflict')
        self.assertEqual(turn.host_sessions, set())
        self.assertEqual(other.host_sessions, {'lineage-fresh-conflict'})
        # Also refuse foreign provenance when there is no competing active run.
        share.end('lineage-other-room', other)
        self.refused('lineage-fresh-conflict')
        self.assertFalse(share.note_host_turn('lineage-fresh-conflict', '', 'lineage-room'))
        self.assertEqual(turn.host_sessions, set())
        self.assertEqual(self.created, [])

    def test_fresh_session_with_ambiguous_dispatches_binds_neither(self):
        turn = self.unresolved()
        other = self.begin('lineage-other-room', '')
        self.db.row('lineage-fresh-ambiguous')
        self.refused('lineage-fresh-ambiguous')
        self.assertEqual(turn.host_sessions, set())
        self.assertEqual(other.host_sessions, set())

    def test_seal_still_refuses_upload(self):
        self.db.rotate('lineage-session', 'lineage-sealed')
        self.turn.sealed = lambda: True
        self.refused('lineage-sealed')
        self.refused('lineage-session')
        self.assertEqual(self.created, [])
        self.assertEqual(self.turn.records, {})

    def test_end_after_binding_before_share_is_rechecked(self):
        self.db.rotate('lineage-session', 'lineage-racing')
        original = self.turn.share
        def after_binding(args):
            share.end('lineage-room', self.turn)
            return original(args)
        self.turn.share = after_binding
        self.refused('lineage-racing')
        self.assertEqual(self.created, [])

    def test_core_hook_delegation_remains_scoped_to_active_parent(self):
        self.assertTrue(share.note_host_turn('lineage-delegate', '', 'lineage-room',
                                           parent_session_id='lineage-session'))
        self.assertTrue(self.call('lineage-delegate')['success'])
        self.assertFalse(share.note_host_turn('lineage-unknown', '', 'lineage-room'))

    def test_adapter_uses_owning_core_db_before_handle_message(self):
        # Evaluate the exact begin call from _dispatch_room_event, retaining
        # the real adapter wiring while its unavailable gateway is excluded.
        tree = ast.parse((ROOT / 'adapter.py').read_text())
        method = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)
                      and n.name == '_dispatch_room_event')
        begin = next(n for n in ast.walk(method) if isinstance(n, ast.Call)
                     and isinstance(n.func, ast.Attribute) and n.func.attr == 'begin'
                     and isinstance(n.func.value, ast.Name) and n.func.value.id == 'share_file')
        handle = next(n for n in ast.walk(method) if isinstance(n, ast.Call)
                      and isinstance(n.func, ast.Attribute) and n.func.attr == 'handle_message')
        self.assertLess(begin.lineno, handle.lineno)
        store = types.SimpleNamespace(_db_for_key=mock.Mock(return_value=self.db))
        turn = share.ShareTurn({}, self.root, [], lambda: True,
            lambda *_: self.created.append('adapter') or {'artifactId': 'a', 'versionId': 'v'})
        eval(compile(ast.Expression(begin), str(ROOT / 'adapter.py'), 'eval'), dict(
            share_file=share, binding=types.SimpleNamespace(room_id='lineage-adapter-room'),
            turn=turn, host_session='lineage-peeked', session_key='lineage-key',
            self=types.SimpleNamespace(_session_store=store)))
        self.addCleanup(share.end, 'lineage-adapter-room', turn)
        store._db_for_key.assert_called_once_with('lineage-key')
        self.assertIs(turn.session_db, self.db)
        store._db_for_key.return_value = CoreRecords()  # later ambient scope is irrelevant
        self.db.row('lineage-peeked')
        self.db.rotate('lineage-peeked', 'lineage-actual-run', 'session_reset')
        self.assertTrue(self.call('lineage-actual-run')['success'])
        self.assertEqual(self.created, ['adapter'])

    @unittest.skipUnless(CORE.is_dir(), 'Requires the optional local 7cadfaa6 source corpus')
    def test_supplied_core_dispatcher_emits_real_kwargs_after_compression(self):
        seen = []
        def dispatch(name, args, **kwargs):
            self.assertEqual(name, share.TOOL_NAME)
            seen.append(kwargs)
            return share.room_share_file(args, **kwargs)
        execute = source_function(CORE / 'model_tools.py', '_execute_tool', {
            'registry': types.SimpleNamespace(dispatch=dispatch),
            '_approval_observability': lambda _ids: nullcontext(),
        })
        connectors = types.ModuleType('tools.connectors')
        connectors.is_connector_name = lambda _name: False
        connectors.dispatch_connector_call = lambda *_args: self.fail('unexpected connector')
        self.db.rotate('lineage-session', 'lineage-dispatched')
        with mock.patch.dict('sys.modules', {'tools.connectors': connectors}):
            result = execute(share.TOOL_NAME, {'path': 'draft.md'}, {},
                types.SimpleNamespace(task_id='real-task', session_id='lineage-dispatched'),
                user_task='Share draft.md', enabled_tools=None, skip_tool_execution_middleware=True)
        self.assertEqual(seen, [dict(task_id='real-task', session_id='lineage-dispatched', user_task='Share draft.md')])
        self.assertTrue(json.loads(result)['success'])
        self.assertEqual(self.created, ['lineage-room'])


if __name__ == '__main__':
    unittest.main()

"""Exercise the installed core's real hook dispatch and tool executor offline.

Run in a child process: the other suites deliberately install gateway stubs.
HERMES_CORE_PATH may select another locally available supported core checkout.
No model, gateway, credential, or Room service is used.
"""
from __future__ import annotations

import ast
from importlib.machinery import PathFinder
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


def probe():
    from types import SimpleNamespace
    from unittest import mock
    import model_tools
    from agent import tool_executor
    from hermes_cli import lifecycle, plugins
    from tools.registry import registry
    from support import load_module

    adapter = load_module('adapter')
    share = load_module('share_file')
    core = Path(model_tools.__file__).parent
    # Execute the actual hook call expression from this core, retaining its
    # kwargs verbatim, through the real lifecycle/PluginManager dispatcher.
    tree = ast.parse((core / 'agent/turn_context.py').read_text())
    hook = next(node for node in ast.walk(tree) if isinstance(node, ast.Call)
                and node.args and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == 'pre_llm_call')
    manager = plugins.PluginManager()
    manager._discovered = True
    seen_hooks, seen_tools, creates = [], [], []
    def on_hook(**kwargs):
        seen_hooks.append(kwargs)
        adapter._on_pre_llm_call(**kwargs)
    manager._hooks['pre_llm_call'] = [on_hook]
    def handler(args, **metadata):
        seen_tools.append(metadata)
        return share.room_share_file(args, **metadata)
    registry.register(name=share.TOOL_NAME, toolset='issue116b',
                      schema=share.SCHEMA, handler=handler)
    agent = mock.MagicMock()
    agent.session_id, agent._current_turn_id = 'synthetic-session', 'synthetic-hook-turn'
    agent.platform, agent._user_id, agent.model = adapter.NAME, 'synthetic-room', 'offline'
    agent._parent_session_id = ''
    agent._context_engine = agent._memory_manager = None
    agent._context_engine_tool_names = set()
    agent._interrupt_requested = agent._incremental_persistence_failed = False
    agent.quiet_mode, agent.verbose_logging = True, False
    agent.valid_tool_names = [share.TOOL_NAME]
    agent._current_api_request_id = ''
    agent._should_emit_quiet_tool_messages.return_value = False
    agent._subdirectory_hints.check_tool_call.return_value = ''
    agent._append_guardrail_observation.side_effect = lambda _name, _args, result, **_kw: result
    agent._tool_result_content_for_active_model.side_effect = lambda _name, result: result
    # Replace only unrelated middleware, persistence and UI infrastructure;
    # execute_tool_calls_sequential -> handle_function_call -> registry.dispatch
    # -> plugin handler all run the selected core's actual implementations.
    def middleware(_agent, **kw):
        return tool_executor._ManagedToolResult(kw['execute'](kw['function_args']),
                                               kw['function_args'], [], False, True)
    with tempfile.TemporaryDirectory() as directory, mock.patch.object(
        plugins, 'get_plugin_manager', return_value=manager
    ), mock.patch('hermes_cli.observability.observe_lifecycle'), mock.patch.object(
        tool_executor, '_run_sequential_tool_execution_middleware', side_effect=middleware, create=True
    ), mock.patch.object(
        tool_executor, '_run_agent_tool_execution_middleware', side_effect=middleware
    ), mock.patch.object(tool_executor, '_emit_terminal_post_tool_call'), mock.patch.object(
        tool_executor, '_flush_session_db_after_tool_progress', return_value=True
    ), mock.patch.object(tool_executor, 'get_active_env', return_value=None):
        root = Path(directory)
        (root / 'draft.md').write_text('Synthetic shared file.\n')
        turn = share.ShareTurn({}, root, [], lambda: True,
            lambda *_: creates.append(True) or {'artifactId': 'synthetic-artifact', 'versionId': 'synthetic-version'})
        # Dispatch resolves the session before hooks; hooks cannot discover it.
        share.begin('synthetic-room', turn, agent.session_id)
        try:
            eval(compile(ast.Expression(hook), str(core / 'agent/turn_context.py'), 'eval'), {
                '_invoke_hook': lifecycle.invoke_hook, 'agent': agent,
                'effective_task_id': 'synthetic-task', 'turn_id': agent._current_turn_id,
                'original_user_message': 'Share draft.md', 'messages': [], 'conversation_history': [],
            })
            messages = []
            call = SimpleNamespace(id='synthetic-call', function=SimpleNamespace(
                name=share.TOOL_NAME, arguments=json.dumps({'path': 'draft.md'})))
            tool_executor.execute_tool_calls_sequential(agent, SimpleNamespace(tool_calls=[call]),
                                                       messages, 'synthetic-task', finalize=False)
            result = json.loads(messages[-1]['content'])
            print(json.dumps({'hook_keys': sorted(seen_hooks[0]),
                'hook_has_turn': bool(seen_hooks[0].get('turn_id')),
                'tool_keys': sorted(seen_tools[0]),
                'tool_has_turn': bool(seen_tools[0].get('turn_id')),
                'result': result, 'creates': len(creates)}, sort_keys=True))
            assert result.get('success') is True, result
            assert len(creates) == 1
        finally:
            share.end('synthetic-room', turn)


class Issue116bHostTests(unittest.TestCase):
    def test_real_core_hook_and_tool_dispatch_share_a_room_file(self):
        spec = PathFinder.find_spec('model_tools', sys.path)
        self.assertIsNotNone(spec, 'Put a supported Hermes core on PYTHONPATH')
        core = Path(os.environ.get('HERMES_CORE_PATH', Path(spec.origin).parent))
        self.assertTrue((core / 'agent/tool_executor.py').is_file(), 'Select a supported core with HERMES_CORE_PATH')
        with tempfile.TemporaryDirectory() as home:
            env = dict(os.environ, HERMES_HOME=home, PYTHONDONTWRITEBYTECODE='1',
                       PYTHONPATH=os.pathsep.join([str(core), str(Path(__file__).parent)]))
            result = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--probe'],
                                    env=env, text=True, capture_output=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    if '--probe' in sys.argv:
        probe()
    else:
        unittest.main()

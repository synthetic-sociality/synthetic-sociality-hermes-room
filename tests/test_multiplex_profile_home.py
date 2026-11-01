"""The plugin under Hermes' one gateway for all profiles (``gateway.multiplex_profiles``).

One process serves every profile. ``os.environ`` holds the host (default) profile's values; a served profile exists
only as Hermes' context-local home, its secret scope and its terminal scope. Seen live on the Mac 2024: the plugin read
``HERMES_HOME`` from the environment, found no Room binding in the host's home, was never enabled, and the gateway ran
Telegram alone.

These run in a child process, because the other suites install gateway stubs: Hermes' plugin manager, config loader and
profile scope are real there, and the Room is a loopback server with the answers of the suite's FakeRoom.
"""
from __future__ import annotations

import asyncio
import contextlib
import http.server
from importlib.machinery import PathFinder
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
REPORT = "MULTIPLEX-REPORT "
STORE = "synthetic-sociality-room"
PLUGIN_FILES = (
    "__init__.py", "adapter.py", "artcraft.py", "artcraft-tools.json", "cli.py", "conformance.json", "context.py",
    "documents.py", "photos.py", "link.py", "local_status.py", "membership.py", "origin_context.py", "plugin.yaml",
    "profile_home.py", "protocol.py", "recovery.py", "renewal.py", "room_tools.py", "share_file.py",
    "secret_isolation.py", "state.py",
)
LEASE = "2099-08-29T00:00:00Z"


def write_binding(home, room_id, credential, base_url="http://127.0.0.1:9/api"):
    store = Path(home) / STORE
    store.mkdir(parents=True, mode=0o700)
    (store / "state.json").write_text(json.dumps({"version": 1, "bindings": [{
        "base_url": base_url, "room_id": room_id, "membership_id": "member-1", "credential": credential,
        "credential_expires_at": "2099-01-01T00:00:00Z", "installation_id": f"inst-{room_id}", "enabled": True,
    }]}))
    (store / "state.json").chmod(0o600)


def load_plugin_copy(name):
    """The plugin as Hermes imports a directory plugin: one package per module name."""
    spec = importlib.util.spec_from_file_location(name, ROOT / "__init__.py", submodule_search_locations=[str(ROOT)])
    module = importlib.util.module_from_spec(spec)
    module.__package__ = name
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def submodule(package, name):
    return sys.modules[f"{package}.{name}"]


def probe_copies(root, report):
    """Each copy keeps the home it was imported under, on threads too, whatever the environment says."""
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    homes = {name: Path(root) / "profiles" / name for name in ("berlin", "second")}
    write_binding(homes["berlin"], "roomberlin00000", "fake-berlin")
    homes["second"].mkdir(parents=True)
    packages = {}
    for name, home in homes.items():
        token = set_hermes_home_override(str(home))
        try:
            load_plugin_copy(f"multiplex_lab_{name}")
        finally:
            reset_hermes_home_override(token)
        packages[name] = f"multiplex_lab_{name}"
    # The next import of the same home, as a core that evicts and reloads plugin modules makes it.
    token = set_hermes_home_override(str(homes["berlin"]))
    try:
        load_plugin_copy("multiplex_lab_berlin_again")
    finally:
        reset_hermes_home_override(token)

    report["env_home_is_host"] = os.environ["HERMES_HOME"] == str(root)
    on_thread = {}
    worker = threading.Thread(target=lambda: on_thread.update(
        {name: str(submodule(package, "state").state_root()) for name, package in packages.items()}
    ))
    worker.start()
    worker.join()
    report["on_thread"] = on_thread
    report["roots"] = {name: str(submodule(package, "state").state_root()) for name, package in packages.items()}
    report["enablement"] = {name: submodule(package, "adapter").env_enablement() for name, package in packages.items()}
    binding = submodule(packages["berlin"], "state").load().bindings[0]
    os.environ.update(HERMES_MODEL="host-model", HERMES_PROFILE="host")
    report["facts"] = submodule(packages["berlin"], "adapter")._runtime_capability_context(binding, {}, transport="sse")
    del os.environ["HERMES_MODEL"], os.environ["HERMES_PROFILE"]
    report["credentials"] = [binding.credential for binding in submodule(packages["berlin"], "state").load().bindings]
    origin = {name: submodule(package, "origin_context") for name, package in packages.items()}
    again = submodule("multiplex_lab_berlin_again", "origin_context")
    report["process_state_shared_between_profiles"] = origin["berlin"]._ROOM_BY_SESSION is origin["second"]._ROOM_BY_SESSION
    report["process_state_shared_within_profile"] = origin["berlin"]._ROOM_BY_SESSION is again._ROOM_BY_SESSION

    berlin_isolation = submodule(packages["berlin"], "secret_isolation")
    store = homes["berlin"] / STORE
    report["spellings"] = sorted(berlin_isolation.store_spellings(store))
    report["blocked_by_env_spelling"] = bool(berlin_isolation.tool_call_block(
        "terminal", {"command": f"cat $HERMES_HOME/{STORE}/state.json"}, store,
    ))


def probe_unscoped(root, report):
    """Imported outside any scope (a CLI on an older core), it follows Hermes' resolution at each call."""
    load_plugin_copy("multiplex_lab_plain")
    state = submodule("multiplex_lab_plain", "state")
    first = str(state.state_root())
    os.environ["HERMES_HOME"] = str(Path(root) / "elsewhere")
    report["roots"] = [first, str(state.state_root())]


def probe_own_gateway(root, report):
    """A profile's own gateway (or CLI) imports under its own home, however that path is spelled: its environment is
    its own and stays what it is."""
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    real = Path(root) / "profiles" / "real"
    real.mkdir(parents=True)
    alias = Path(root) / "alias"
    alias.symlink_to(real)
    os.environ.update(HERMES_HOME=str(alias), TERMINAL_ENV="docker", TERMINAL_CWD="/work", HERMES_MODEL="own-model")
    token = set_hermes_home_override(str(real))
    try:
        load_plugin_copy("multiplex_lab_own")
    finally:
        reset_hermes_home_override(token)
    environment = submodule("multiplex_lab_own", "profile_home").environ()
    report["same_mapping"] = environment is os.environ
    report["terminal"] = environment.get("TERMINAL_ENV")
    report["model"] = environment.get("HERMES_MODEL")


class _Room(http.server.BaseHTTPRequestHandler):
    """The answers of FakeRoom (tests/test_parity_p7a.py), over HTTP, recording each request's bearer."""

    protocol_version = "HTTP/1.1"
    seen: list = []
    lock = threading.Lock()

    def log_message(self, *_args):
        pass

    def _note(self):
        with self.lock:
            self.seen.append((self.command, self.path.split("?")[0], (self.headers.get("Authorization") or "")[7:]))

    def _json(self, value):
        body = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 - http.server API
        self._note()
        path = self.path.split("?")[0]
        if path.endswith("/connector/enrollments"):
            return self._json([])
        if path.endswith("/state"):
            return self._json({"roster": [{"membershipId": "member-1", "status": "active"}],
                               "activeEpoch": {"id": "epoch-1", "startsAtSeq": 1}})
        if path.endswith("/events/stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            with contextlib.suppress(OSError):
                self.wfile.write(b'event: ready\ndata: {"ok": true}\n\n')
                self.wfile.flush()
                for _ in range(20):
                    time.sleep(0.25)
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
            return None
        return self._json({"protocolCapabilities": []} if path.endswith("/status") else {})

    def do_POST(self):  # noqa: N802 - http.server API
        self._note()
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.path.split("?")[0].endswith("/connector/sessions"):
            return self._json({"sessionId": "session-1", "leaseExpiresAt": LEASE, "heartbeatIntervalSeconds": 30})
        return self._json({"leaseExpiresAt": LEASE})


def install(home, base_url=None, room_id="", credential="", terminal=""):
    """A profile as the installer leaves it: the plugin, enabled, and its Room binding when it has one."""
    target = Path(home) / "plugins" / "synthetic-sociality-room"
    target.mkdir(parents=True)
    for name in PLUGIN_FILES:
        shutil.copy(ROOT / name, target / name)
    (Path(home) / "config.yaml").write_text(
        "plugins:\n  enabled:\n    - synthetic-sociality-room\n" + terminal
    )
    if room_id:
        write_binding(home, room_id, credential, base_url)


def probe_gateway(root, report):
    """Hermes' own plugin manager, config loader and scope, as a multiplexed host runs them for one served profile."""
    import gateway.run as runner_module
    from gateway.config import Platform, load_gateway_config
    from gateway.platform_registry import platform_registry
    from hermes_cli.plugins import discover_plugins

    scope = runner_module._profile_runtime_scope
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Room)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base_url = f"http://127.0.0.1:{server.server_port}/api"
    (Path(root) / "config.yaml").write_text("gateway:\n  multiplex_profiles: true\n")
    homes = {name: Path(root) / "profiles" / name for name in ("berlin", "second", "unbound")}
    install(homes["berlin"], base_url, "roomberlin00000", "fake-berlin", "terminal:\n  backend: local\n  cwd: /tmp/berlin-cwd\n")
    install(homes["second"], base_url, "roomsecond00000", "fake-second")
    install(homes["unbound"])
    name = "synthetic_sociality"
    os.environ["TERMINAL_ENV"] = "docker"  # what a host profile's config bridges into the process

    async def main():
        adapters, enabled = {}, {}
        for profile, home in homes.items():
            with scope(home):
                discover_plugins()
                config = load_gateway_config()
                platform_config = config.platforms.get(Platform(name))
                enabled[profile] = bool(platform_config and platform_config.enabled)
                if enabled[profile]:
                    adapter = platform_registry.create_adapter(name, platform_config)
                    adapters[profile] = adapter
                    report.setdefault("state_files", {})[profile] = str(adapter._state_file)
                    report.setdefault("modules", {})[profile] = type(adapter).__module__
                    report.setdefault("own_terminal", {})[profile] = sys.modules[
                        type(adapter).__module__.rsplit(".", 1)[0] + ".profile_home"
                    ].environ().get("TERMINAL_ENV")
                    report.setdefault("connected", {})[profile] = await adapter.connect()
        report["enabled"] = enabled
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            with _Room.lock:
                registered = {seen[2] for seen in _Room.seen if seen[1].endswith("/connector/sessions")}
            if {"fake-berlin", "fake-second"} <= registered:
                break
            await asyncio.sleep(0.05)
        with _Room.lock:
            report["requests"] = sorted({(seen[0], seen[1], seen[2]) for seen in _Room.seen})
        for adapter in adapters.values():
            await adapter.disconnect()
        for profile, home in homes.items():
            status = home / STORE / "status.json"
            report.setdefault("status_written", {})[profile] = status.is_file()
            if status.is_file():
                report.setdefault("secret_store", {})[profile] = json.loads(status.read_text()).get("secretStore")

    asyncio.run(main())
    report["host_store_exists"] = (Path(root) / STORE).exists()
    report["core_has_terminal_scopes"] = "tools.terminal_scope" in sys.modules


PROBES = {"copies": probe_copies, "unscoped": probe_unscoped, "own": probe_own_gateway, "gateway": probe_gateway}


def probe(case):
    root = os.environ["HERMES_HOME"]
    report = {}
    PROBES[case](root, report)
    sys.stdout.flush()
    print(REPORT + json.dumps(report, sort_keys=True), flush=True)
    os._exit(0)  # a served adapter's daemon threads have no part in this answer


class PluginServesItsOwnProfile(unittest.TestCase):
    def run_probe(self, case):
        spec = PathFinder.find_spec("model_tools", sys.path)
        self.assertIsNotNone(spec, "Put a supported Hermes core on PYTHONPATH")
        core = Path(os.environ.get("HERMES_CORE_PATH", Path(spec.origin).parent))
        # The parent's own path follows: on the managed runtime the core's packages may arrive that way.
        path = [str(core), *filter(None, os.environ.get("PYTHONPATH", "").split(os.pathsep))]
        with tempfile.TemporaryDirectory() as root:
            root = os.path.realpath(root)
            env = {key: value for key, value in os.environ.items() if not key.startswith(("TERMINAL_", "GATEWAY_"))}
            env.update(HERMES_HOME=root, HOME=root, PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=os.pathsep.join(path))
            result = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--probe", case],
                env=env, text=True, capture_output=True, timeout=180,
            )
            self.assertEqual(result.returncode, 0, result.stdout[-2000:] + result.stderr[-4000:])
            line = next(item for item in result.stdout.splitlines() if item.startswith(REPORT))
            return root, json.loads(line[len(REPORT):])

    def test_each_copy_keeps_the_home_it_was_imported_under(self):
        root, report = self.run_probe("copies")
        self.assertTrue(report["env_home_is_host"], report)
        expected = {name: f"{root}/profiles/{name}/{STORE}" for name in ("berlin", "second")}
        self.assertEqual(report["roots"], expected)
        self.assertEqual(report["on_thread"], expected)
        self.assertEqual(report["credentials"], ["fake-berlin"])

    def test_a_binding_in_the_profile_enables_the_platform_there_and_nowhere_else(self):
        _root, report = self.run_probe("copies")
        self.assertEqual(report["enablement"], {"berlin": {}, "second": None})

    def test_process_state_is_per_profile_and_survives_a_reload(self):
        _root, report = self.run_probe("copies")
        self.assertIs(report["process_state_shared_between_profiles"], False)
        self.assertIs(report["process_state_shared_within_profile"], True)

    def test_the_store_fence_knows_the_profile_home_as_home(self):
        _root, report = self.run_probe("copies")
        self.assertIn(f"$HERMES_HOME/{STORE}", report["spellings"])
        self.assertTrue(report["blocked_by_env_spelling"], report)

    def test_the_turn_facts_name_the_served_profile_not_the_hosts(self):
        _root, report = self.run_probe("copies")
        self.assertIn('profile="berlin"', report["facts"])
        self.assertNotIn("host-model", report["facts"])

    def test_a_profiles_own_gateway_keeps_its_environment(self):
        _root, report = self.run_probe("own")
        self.assertEqual(report, {"same_mapping": True, "terminal": "docker", "model": "own-model"})

    def test_without_a_scope_at_import_the_plugin_follows_hermes_home(self):
        root, report = self.run_probe("unscoped")
        self.assertEqual(report["roots"], [f"{root}/{STORE}", f"{root}/elsewhere/{STORE}"])

    def test_served_profiles_connect_with_their_own_binding_under_hermes_scope(self):
        root, report = self.run_probe("gateway")
        self.assertEqual(report["enabled"], {"berlin": True, "second": True, "unbound": False})
        self.assertEqual(report["connected"], {"berlin": True, "second": True})
        self.assertEqual(report["state_files"], {
            "berlin": f"{root}/profiles/berlin/{STORE}/state.json",
            "second": f"{root}/profiles/second/{STORE}/state.json",
        })
        self.assertNotEqual(report["modules"]["berlin"], report["modules"]["second"])
        registrations = {tuple(item[1:]) for item in report["requests"] if item[1].endswith("/connector/sessions")}
        self.assertEqual(registrations, {
            ("/api/rooms/roomberlin00000/connector/sessions", "fake-berlin"),
            ("/api/rooms/roomsecond00000/connector/sessions", "fake-second"),
        })
        self.assertEqual(report["status_written"], {"berlin": True, "second": True, "unbound": False})
        self.assertIs(report["host_store_exists"], False)

    def test_a_served_profile_reads_its_own_terminal_policy_not_the_hosts(self):
        _root, report = self.run_probe("gateway")
        if not report["core_has_terminal_scopes"]:
            # An older core's terminal tool reads the process environment, for every profile.
            self.assertEqual(report["own_terminal"]["berlin"], "docker", report)
            return
        # berlin's config says local; the process environment carries the host's docker. Read the host's and the
        # credential store would pass as out of the agent's reach while berlin's shell runs on the host.
        self.assertEqual(report["own_terminal"]["berlin"], "local", report)
        self.assertEqual(report["secret_store"]["berlin"], "reachable", report)


if __name__ == "__main__":
    if "--probe" in sys.argv:
        probe(sys.argv[sys.argv.index("--probe") + 1])
    else:
        unittest.main()

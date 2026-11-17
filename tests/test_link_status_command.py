"""Hermes 1.0.112: contract L1-11a, the link status command (`hermes room link status`).

The universal rule is the contract paragraph L1-11a, byte-identical in OpenClaw;
the shared case names are ``conformance.json`` ``parityL1StatusCommandCases``
(LSC-01 to LSC-06), the same six strings in both connectors. Hermes' gateway
writes ``status.json`` next to the state file in use, and no host
configuration decides that path. Every test runs the command through
``cli.dispatch`` in a throwaway home; no real token, key or credential is used.
"""

from __future__ import annotations

import argparse
import base64
import builtins
import contextlib
import hashlib
import io
import json
import os
import stat
import sys
import unittest
from pathlib import Path
from unittest import mock

from test_parity_l1 import DEVICE, FAKE_PAIRING_SECRET, ROOT, SEED, LinkTestCase, _iso, adapter, cli, link, local_status

CONFORMANCE = json.loads((ROOT / "conformance.json").read_text(encoding="utf-8"))
CONTRACT = (ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_text(encoding="utf-8")
state_path_scope = sys.modules[cli.state_path.__module__].state_path_scope

STATUS_COMMAND_CASES = [
    "LSC-01 the status file the gateway writes is found by the gateway's own path rule, also when it is not beside the link record",
    "LSC-02 the link record without its secrets and the link object are shown as the gateway reports them",
    "LSC-03 no status file names the link: a plain statement that the gateway has not reported it",
    "LSC-04 a missing or unreadable status file or host configuration never fails the command",
    "LSC-05 a host configuration is read the way the host reads it; without such a dependency none is read",
    "LSC-06 the command writes nothing and prints no credential, configuration or account content",
]
PARAGRAPH = """\
L1-11a Status command (2026-11-17). Each connector gives the person one command that shows the link on that host
(Hermes: `hermes room link status`; OpenClaw: `openclaw-room-link status`). It shows the link record without its
secrets (L1-10) and the `link` object of L1-11 as the running gateway reports it. The command reads that object from
the status file the gateway actually writes (P3-5), found by the gateway's own path rule and never by a second rule of
the command's own; this holds for every layout the connector supports, also when the status file is not in the
directory of the link record. Where that path depends on the host's configuration, the command reads the configuration
the way the host reads it; where it does not, the command reads no host configuration. When no status file names the
link, the command says in plain words that the gateway has not reported the link. A status file or a host
configuration that is missing or cannot be read never fails the command: it then shows the link record and that plain
statement. The command writes nothing, and prints nothing of a credential, of the host configuration or of an account
or state file beyond the link record's public fields and the `link` object.
"""
PARAGRAPH_SHA256 = "55509458bcb69d5a1627233b64ced87b6dd83e63f957f315eec6233a90f62681"
NO_STATUS = "Gateway:     no status (the Hermes gateway has not reported the link)"
AUDIENCE = "https://rooms.example/tokenwerk/room/v1/local-devices/connect"
OTHER_DEVICE = "7a6b5c4d-3e2f-4a1b-9c8d-0e1f2a3b4c5d"
OTHER_SEED = bytes(range(32, 64))
# Planted where the command must never look, or must never print from.
FAKE_ROOM_CREDENTIAL = "room-credential-for-tests-only-0123456789abcdef"
FAKE_CONFIG_SECRET = "config-api-key-for-tests-only-0123456789abcdef"
FAKE_STATUS_EXTRA = "status-field-outside-link-for-tests-only"


class LinkStatusCommandConformanceTests(unittest.TestCase):
    def test_the_paragraph_follows_l1_11_byte_identical(self):
        self.assertEqual(hashlib.sha256(PARAGRAPH.encode("utf-8")).hexdigest(), PARAGRAPH_SHA256)
        lines = CONTRACT.split("\n")
        start = next(index for index, line in enumerate(lines) if line.startswith("L1-11 Status. "))
        end = next(index for index in range(start, len(lines)) if not lines[index])
        self.assertEqual(lines[end - 1], "`channel_binary`, or empty) and `updatedAt`.")
        block = PARAGRAPH.split("\n")[:-1]
        self.assertEqual(lines[end + 1:end + 1 + len(block)], block)
        self.assertEqual(lines[end + 1 + len(block)], "")
        self.assertTrue(lines[end + 2 + len(block)].startswith("The following clauses were added to P4 after"))
        self.assertEqual(CONTRACT.count("L1-11a Status command"), 1)

    def test_the_case_names_are_the_shared_ones_and_each_has_a_test(self):
        self.assertEqual(CONFORMANCE["parityL1StatusCommandCases"], STATUS_COMMAND_CASES)
        source = Path(__file__).read_text(encoding="utf-8")
        for index, name in enumerate(STATUS_COMMAND_CASES, start=1):
            self.assertTrue(name.startswith(f"LSC-{index:02d} "), name)
            self.assertGreaterEqual(source.count(f"    def test_lsc_{index:02d}_"), 1, name)

    def test_the_contract_hash_is_the_pinned_one(self):
        digest = hashlib.sha256((ROOT / "docs" / "delivery-lifecycle-contract-v1.md").read_bytes()).hexdigest()
        self.assertEqual(CONFORMANCE["contractSha256"], digest)


def tree(root: Path) -> dict[str, tuple]:
    """Every entry under ``root``: kind, mode, size, mtime and content digest."""
    found: dict[str, tuple] = {}
    for path in sorted(root.rglob("*")):
        info = path.lstat()
        if stat.S_ISREG(info.st_mode):
            try:
                content = hashlib.sha256(path.read_bytes()).hexdigest()
            except OSError:
                content = "unreadable"
        elif stat.S_ISLNK(info.st_mode):
            content = os.readlink(path)
        else:
            content = ""
        size = info.st_size if stat.S_ISREG(info.st_mode) else 0
        found[str(path.relative_to(root))] = (stat.S_IFMT(info.st_mode), stat.S_IMODE(info.st_mode), size,
                                              info.st_mtime_ns, content)
    return found


class LinkStatusCommandTests(LinkTestCase):
    def setUp(self) -> None:
        super().setUp()
        # A second installation, not the profile's default store.
        self.other_state = self.scratch / "elsewhere" / "installation-b" / "custom-state.json"
        self.other_store = self.other_state.parent
        self.home.mkdir(parents=True, exist_ok=True)

    def record(self, store: Path, *, device: str = DEVICE, seed: bytes = SEED, state: str = "active",
               label: str = "Test · Hermes test") -> dict:
        record = link.pending_record({"deviceId": device, "audience": AUDIENCE, "pairingSecret": FAKE_PAIRING_SECRET,
                                      "expiresAt": _iso(300)}, label=label, seed=seed)
        if state == "pending":
            link.write_link(record, store / link.LINK_PENDING_FILE)
            return record
        record = {key: value for key, value in record.items() if key not in {"pairingSecret", "expiresAt"}}
        record["state"] = state
        link.write_link(record, store / link.LINK_FILE)
        return record

    def gateway_status(self, store: Path, entry: dict | None) -> Path:
        """The file as the gateway writes it (``LocalStatus``), with or without a link."""
        status = local_status.LocalStatus(store / "status.json", plugin="hermes", version=adapter.CONNECTOR_VERSION,
                                          host_label=FAKE_STATUS_EXTRA)
        if entry is not None:
            status.set_link(entry)
        self.assertTrue(status.write())
        return status.path

    def status(self, state_file: Path | None = None) -> str:
        """Run `hermes room link status`; exit 0 and an empty stderr are part of every case."""
        out, err = io.StringIO(), io.StringIO()
        with contextlib.ExitStack() as stack:
            if state_file is not None:
                stack.enter_context(state_path_scope(state_file))
            stack.enter_context(contextlib.redirect_stdout(out))
            stack.enter_context(contextlib.redirect_stderr(err))
            code = cli.dispatch(argparse.Namespace(room_command="link", action="status", yes=False))
        self.assertEqual((code, err.getvalue()), (0, ""), out.getvalue())
        return out.getvalue()

    def record_lines(self, record: dict, state: str = "active") -> list[str]:
        return [
            f"Link:        {state}",
            f"Device:      {record['deviceId']}",
            f"Label:       {record['deviceLabel']}",
            f"Fingerprint: {record['fingerprintSha256']}",
            "Server:      rooms.example",
        ]

    # --- LSC-01 ---------------------------------------------------------------------

    def test_lsc_01_the_status_file_next_to_the_state_file_in_use_is_read_not_the_default_one(self):
        record = self.record(self.other_store, device=OTHER_DEVICE, seed=OTHER_SEED)
        self.gateway_status(self.other_store, {"state": "reconnecting", "code": "link_keepalive_timeout",
                                               "deviceId": OTHER_DEVICE, "channels": 2, "limits": "advertised"})
        # The default place holds another link and another gateway report: neither is read.
        self.record(self.store)
        self.gateway_status(self.store, {"state": "connected", "deviceId": DEVICE, "channels": 7,
                                         "limits": "advertised", "lastChannelErrorCode": "channel_cap"})
        self.assertEqual(self.status(self.other_state).splitlines(), self.record_lines(record) + [
            "Gateway:     reconnecting link_keepalive_timeout",
            "Channels:    2",
        ])
        # Without a status file next to the state file in use, the default one is still not read.
        (self.other_store / "status.json").unlink()
        self.assertEqual(self.status(self.other_state).splitlines(), self.record_lines(record) + [NO_STATUS])
        # And the default installation reads its own.
        self.assertEqual(self.status().splitlines()[5:], ["Gateway:     connected", "Channels:    7",
                                                           "Last local:  channel_cap"])

    def test_lsc_01_the_command_reads_the_file_at_the_gateways_own_path(self):
        record = self.record(self.other_store)
        with state_path_scope(self.other_state), mock.patch.object(adapter, "load", lambda *_: adapter.PluginState()):
            gateway = adapter.SyntheticSocialityAdapter(adapter.PlatformConfig())
        # The gateway's rule (P3-5): next to the state file in use.
        self.assertEqual(gateway._status.path, self.other_store / "status.json")
        gateway._status.set_link({"state": "connected", "deviceId": DEVICE, "channels": 1, "limits": "fallback"})
        self.assertTrue(gateway._status.path.is_file())
        self.assertEqual(self.status(self.other_state).splitlines(), self.record_lines(record) + [
            "Gateway:     connected",
            "Channels:    1",
        ])
        self.assertFalse(self.status_path.exists())

    # --- LSC-02 ---------------------------------------------------------------------

    def test_lsc_02_the_public_fields_and_the_gateways_state_code_and_channels_are_printed_as_reported(self):
        record = self.record(self.other_store)
        reports = (
            ({"state": "connected", "channels": 3}, ["Gateway:     connected", "Channels:    3"]),
            ({"state": "connecting"}, ["Gateway:     connecting", "Channels:    0"]),
            ({"state": "reconnecting", "code": "link_handshake_timeout", "channels": 0},
             ["Gateway:     reconnecting link_handshake_timeout", "Channels:    0"]),
            ({"state": "stopped", "code": "link_revoked", "channels": 0, "lastChannelErrorCode": "local_gateway_refused"},
             ["Gateway:     stopped link_revoked", "Channels:    0", "Last local:  local_gateway_refused"]),
            ({"state": "connected", "channels": 1, "lastChannelErrorCode": "channel_data_too_large"},
             ["Gateway:     connected", "Channels:    1", "Last local:  channel_data_too_large"]),
        )
        for entry, expected in reports:
            with self.subTest(entry=entry):
                path = self.gateway_status(self.other_store, dict(entry, deviceId=DEVICE, limits="advertised"))
                written = json.loads(path.read_text(encoding="utf-8"))["link"]
                self.assertEqual((written["state"], written["code"], written["channels"]),
                                 (entry["state"], entry.get("code", ""), entry.get("channels", 0)))
                self.assertEqual(self.status(self.other_state).splitlines(), self.record_lines(record) + expected)

    def test_lsc_02_a_pending_link_is_shown_with_its_public_fields(self):
        record = self.record(self.other_store, state="pending")
        self.gateway_status(self.other_store, {"state": "pending", "code": "link_pending", "deviceId": DEVICE})
        self.assertEqual(self.status(self.other_state).splitlines(), self.record_lines(record, "pending") + [
            "Gateway:     pending link_pending",
            "Channels:    0",
        ])

    # --- LSC-03 ---------------------------------------------------------------------

    def test_lsc_03_without_a_link_object_the_output_has_the_plain_no_status_sentence(self):
        record = self.record(self.other_store)
        expected = self.record_lines(record) + [NO_STATUS]
        # The gateway's file before the link service has reported.
        path = self.gateway_status(self.other_store, None)
        self.assertNotIn("link", json.loads(path.read_text(encoding="utf-8")))
        self.assertEqual(self.status(self.other_state).splitlines(), expected)
        whole = json.loads(path.read_text(encoding="utf-8"))
        for value in (None, {}):
            with self.subTest(link=value):
                path.write_text(json.dumps(dict(whole, link=value)), encoding="utf-8")
                self.assertEqual(self.status(self.other_state).splitlines(), expected)

    # --- LSC-04 ---------------------------------------------------------------------

    def test_lsc_04_a_missing_unreadable_or_non_json_status_file_gives_exit_0_and_that_sentence(self):
        record = self.record(self.other_store)
        expected = self.record_lines(record) + [NO_STATUS]
        path = self.other_store / "status.json"

        def clear() -> None:
            if path.is_symlink() or path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()

        def unreadable() -> None:
            path.write_text('{"link": {"state": "connected", "channels": 1}}', encoding="utf-8")
            os.chmod(path, 0)

        cases = {
            "missing": lambda: None,
            "no permission": unreadable,
            "empty": lambda: path.write_bytes(b""),
            "not JSON": lambda: path.write_text("link: connected\n", encoding="utf-8"),
            "cut short": lambda: path.write_text('{"link": {"state": "conn', encoding="utf-8"),
            "not UTF-8": lambda: path.write_bytes(b'{"link": "\xff\xfe"}'),
            "a directory": path.mkdir,
            "a dangling symlink": lambda: path.symlink_to(self.other_store / "nowhere.json"),
        }
        self.assertNotEqual(os.geteuid(), 0, "the suite never runs as root: a mode 0 file would be readable")
        for name, arrange in cases.items():
            with self.subTest(status_file=name):
                clear()
                arrange()
                try:
                    self.assertEqual(self.status(self.other_state).splitlines(), expected)
                finally:
                    clear()

    def test_lsc_04_a_status_file_that_is_json_but_no_object_or_whose_link_is_no_object_never_fails_the_command(self):
        record = self.record(self.other_store)
        expected = self.record_lines(record) + [NO_STATUS]
        path = self.other_store / "status.json"
        for content in ("[]", '"connected"', "null", "7", "true", '{"link": "connected"}', '{"link": ["connected"]}',
                        '{"link": 1}', '{"link": true}'):
            with self.subTest(status_file=content):
                path.write_text(content, encoding="utf-8")
                self.assertEqual(self.status(self.other_state).splitlines(), expected)

    def test_lsc_04_a_deeply_nested_status_file_never_fails_the_command(self):
        """LSC-04 deeply nested: 100000 opening brackets, also with their closing ones (OP-12 F1)."""
        record = self.record(self.other_store)
        expected = self.record_lines(record) + [NO_STATUS]
        path = self.other_store / "status.json"
        contents = {
            "deeply nested, opening brackets only": "[" * 100000,
            "deeply nested, opening and closing brackets": "[" * 100000 + "]" * 100000,
        }
        for name, content in contents.items():
            with self.subTest(status_file=name):
                path.write_text(content, encoding="utf-8")
                out, err = io.StringIO(), io.StringIO()
                with state_path_scope(self.other_state), contextlib.redirect_stdout(out), \
                        contextlib.redirect_stderr(err):
                    try:
                        code = cli.dispatch(argparse.Namespace(room_command="link", action="status", yes=False))
                    except RecursionError as error:
                        self.fail(f"the command ended with a traceback: {type(error).__name__}")
                self.assertEqual((code, err.getvalue()), (0, ""))
                self.assertNotIn("Traceback", out.getvalue())
                self.assertNotIn("RecursionError", out.getvalue())
                self.assertEqual(out.getvalue().splitlines(), expected)

    def test_lsc_04_a_missing_or_unreadable_host_configuration_never_fails_the_command(self):
        record = self.record(self.other_store)
        self.gateway_status(self.other_store, {"state": "connected", "deviceId": DEVICE, "channels": 1})
        expected = self.record_lines(record) + ["Gateway:     connected", "Channels:    1"]
        config = self.home / "config.yaml"
        self.assertFalse(config.exists())
        self.assertEqual(self.status(self.other_state).splitlines(), expected)
        config.write_text("gateway: [unclosed\n", encoding="utf-8")
        os.chmod(config, 0)
        self.addCleanup(os.chmod, config, 0o600)
        self.assertEqual(self.status(self.other_state).splitlines(), expected)

    # --- LSC-05 ---------------------------------------------------------------------

    def test_lsc_05_the_command_reads_no_host_configuration(self):
        record = self.record(self.store)
        self.gateway_status(self.store, {"state": "connected", "deviceId": DEVICE, "channels": 2})
        expected = self.record_lines(record) + ["Gateway:     connected", "Channels:    2"]
        self.assertEqual(self.status().splitlines(), expected)
        config = self.home / "config.yaml"
        elsewhere = self.gateway_status(self.other_store, {"state": "stopped", "code": "link_revoked", "deviceId": DEVICE})

        def clear() -> None:
            if config.is_dir():
                config.rmdir()
            elif config.exists():
                os.chmod(config, 0o600)
                config.unlink()

        def no_permission() -> None:
            config.write_text("model: x\n", encoding="utf-8")
            os.chmod(config, 0)

        configurations = {
            "cannot be parsed": lambda: config.write_bytes(b"gateway: [unclosed\n\x00\xff: {"),
            "cannot be read": no_permission,
            "a directory": config.mkdir,
            # A configuration that names another state directory and status file decides nothing.
            "names another place": lambda: config.write_text(
                f"synthetic_sociality:\n  state_dir: {self.other_store}\n  status_file: {elsewhere}\n"
                f"api_key: {FAKE_CONFIG_SECRET}\n", encoding="utf-8"),
        }
        opened: list[str] = []
        real_open, real_os_open = io.open, os.open

        def watching_open(file, *args, **kwargs):
            opened.append(os.fspath(file) if not isinstance(file, int) else f"fd:{file}")
            return real_open(file, *args, **kwargs)

        def watching_os_open(path, *args, **kwargs):
            opened.append(os.fspath(path))
            return real_os_open(path, *args, **kwargs)

        for name, arrange in configurations.items():
            with self.subTest(configuration=name):
                clear()
                arrange()
                del opened[:]
                try:
                    with mock.patch.object(io, "open", watching_open), \
                            mock.patch.object(builtins, "open", watching_open), \
                            mock.patch.object(os, "open", watching_os_open):
                        output = self.status()
                finally:
                    clear()
                self.assertEqual(output.splitlines(), expected)
                self.assertNotIn(FAKE_CONFIG_SECRET, output)
                # Only the link record and the gateway's status file are opened.
                self.assertEqual(sorted(set(opened)), sorted(str(path) for path in (
                    self.store / link.LINK_FILE, self.store / "status.json")))
        try:
            import hermes_cli.config as host_config  # Hermes core
        except ImportError:
            return
        refuse = mock.Mock(side_effect=AssertionError("the status command read the host configuration"))
        with contextlib.ExitStack() as stack:
            for loader in ("load_config", "load_config_readonly", "read_raw_config"):
                if hasattr(host_config, loader):
                    stack.enter_context(mock.patch.object(host_config, loader, refuse))
            self.assertEqual(self.status().splitlines(), expected)
        refuse.assert_not_called()

    # --- LSC-06 ---------------------------------------------------------------------

    def secrets_of(self, record: dict, seed: bytes) -> list[str]:
        return [record["privateKeyBase64"], base64.b64encode(seed).decode("ascii"), seed.hex(),
                base64.urlsafe_b64encode(seed).decode("ascii").rstrip("="), FAKE_PAIRING_SECRET,
                FAKE_ROOM_CREDENTIAL, FAKE_CONFIG_SECRET, FAKE_STATUS_EXTRA]

    def plant(self, store: Path) -> None:
        """A state file with a Room credential and a host configuration with a key: neither is printed."""
        (store / "custom-state.json").write_text(json.dumps({"bindings": [{
            "base_url": "https://room.example/api", "room_id": "room-1", "membership_id": "member-1",
            "credential": FAKE_ROOM_CREDENTIAL,
        }]}), encoding="utf-8")
        os.chmod(store / "custom-state.json", 0o600)
        (self.home / "config.yaml").write_text(f"api_key: {FAKE_CONFIG_SECRET}\n", encoding="utf-8")

    def test_lsc_06_nothing_is_written_and_no_private_key_pairing_secret_or_credential_is_printed(self):
        arrangements = {
            "active, the gateway reports": ("active", lambda: self.gateway_status(
                self.other_store, {"state": "connected", "deviceId": DEVICE, "channels": 1, "limits": "advertised"})),
            "active, no status file": ("active", lambda: None),
            "active, a status file that is not JSON": ("active", lambda: (self.other_store / "status.json").write_text(
                f"not json {FAKE_STATUS_EXTRA}", encoding="utf-8")),
            "pending, the gateway reports": ("pending", lambda: self.gateway_status(
                self.other_store, {"state": "pending", "code": "link_pending", "deviceId": DEVICE})),
        }
        for name, (state, arrange) in arrangements.items():
            with self.subTest(arrangement=name):
                for path in sorted(self.other_store.glob("*")) if self.other_store.is_dir() else ():
                    path.unlink()
                record = self.record(self.other_store, state=state)
                self.plant(self.other_store)
                arrange()
                before = tree(self.scratch)
                self.assertIn(f"elsewhere/installation-b/{link.LINK_PENDING_FILE if state == 'pending' else link.LINK_FILE}",
                              before)
                output = self.status(self.other_state)
                self.assertEqual(tree(self.scratch), before)
                self.assertEqual(output.splitlines()[:5], self.record_lines(record, state))
                for secret in self.secrets_of(record, SEED):
                    self.assertNotIn(secret, output)
                for word in ("privateKey", "pairingSecret", "credential", "api_key", "hostLabel"):
                    self.assertNotIn(word, output)

    def test_lsc_06_not_linked_writes_nothing_and_creates_no_directory(self):
        self.gateway_status(self.store, {"state": "not_linked", "deviceId": ""})
        before = tree(self.scratch)
        self.assertEqual(self.status(), "Not linked.\n")
        self.assertEqual(self.status(self.other_state), "Not linked.\n")
        self.assertEqual(tree(self.scratch), before)
        self.assertFalse(self.other_store.exists())


if __name__ == "__main__":
    unittest.main()

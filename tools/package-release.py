#!/usr/bin/env python3
"""Build a deterministic standalone Hermes Room release archive."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile


PAYLOAD = (
    "README.md",
    "__init__.py",
    "adapter.py",
    "cli.py",
    "conformance.json",
    "context.py",
    "documents.py",
    "photos.py",
    "docs/delivery-lifecycle-contract-v1.md",
    "install.sh",
    "link.py",
    "local_status.py",
    "origin_context.py",
    "plugin.yaml",
    "protocol.py",
    "recovery.py",
    "renewal.py",
    "room_tools.py",
    "secret_isolation.py",
    "state.py",
    "share_file.py",
    "tests/fixtures/agent-files.json",
    "tests/test_issue116_delivery.py",
    "tests/test_issue116b_host.py",
    "tests/test_parity_p10.py",
    "tests/test_share_session_binding.py",
    "tests/test_parity_p11b.py",
    "tests/__init__.py",
    "tests/fixtures/berlin-huawei-malformed-redacted.json",
    "tests/fixtures/berlin-seq120-redacted-offline.json",
    "tests/fixtures/joined-coding-epoch-terminal/seq-838-840.canonical.json",
    "tests/fixtures/open-exchange-preamble-v1.txt",
    "tests/fixtures/plugin-1.0.79.yaml",
    "tests/fixtures/host-fallback-notice/notice-then-model.json",
    "tests/fixtures/showroom-discussion-start/seq-12-14.canonical.json",
    "tests/fixtures/zurie-epoch-boundary/seq-132.canonical.json",
    "tests/fixtures/zurie-epoch-boundary/seq-133.canonical.json",
    "tests/fixtures/zurie-epoch-boundary/seq-134.canonical.json",
    "tests/fixtures/issue-106.json",
    "tests/fixtures/photos-to-model.json",
    "tests/test_issue106.py",
    "tests/test_parity_p13.py",
    "tests/test_issue106_host.py",
    "tests/support.py",
    "tests/test_context.py",
    "tests/test_credential_renewal.py",
    "tests/test_delivery_lifecycle.py",
    "tests/test_epoch_handover_dispatch.py",
    "tests/test_parity_l1.py",
    "tests/test_parity_l1_room.py",
    "tests/test_parity_p1.py",
    "tests/test_parity_p1a.py",
    "tests/test_parity_p2a.py",
    "tests/test_parity_p2c.py",
    "tests/test_parity_p3.py",
    "tests/test_parity_p5.py",
    "tests/test_parity_p6.py",
    "tests/test_parity_p7a.py",
    "tests/test_parity_p7c.py",
    "tests/test_parity_p7d.py",
    "tests/test_parity_p8.py",
    "tests/test_parity_p9.py",
    "tests/test_prior_epoch_system_terminal.py",
    "tests/test_protocol_artifacts.py",
    "tests/test_recovery_fence_transport.py",
    "tests/test_reply_marker.py",
    "tests/test_room_history_tool.py",
    "tests/test_stale_dispatch_recovery.py",
    # Shipped so the extracted archive can verify its own payload (G36).
    "tools/package-release.py",
)


def git(repo: Path, *arguments: str, binary: bool = False) -> str | bytes:
    result = subprocess.check_output(
        ["git", "-C", str(repo), *arguments],
        stderr=subprocess.DEVNULL,
    )
    return result if binary else result.decode("utf-8").strip()


def committed_bytes(repo: Path, commit: str, name: str) -> bytes:
    return git(repo, "show", f"{commit}:{name}", binary=True)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    if git(repo, "status", "--porcelain"):
        raise SystemExit("release source is dirty; commit and review it before packaging")

    commit = str(git(repo, "rev-parse", "HEAD"))
    commit_epoch = int(str(git(repo, "show", "-s", "--format=%ct", commit)))
    tracked = set(str(git(repo, "ls-tree", "-r", "--name-only", commit)).splitlines())
    missing = sorted(set(PAYLOAD) - tracked)
    if missing:
        raise SystemExit(f"release payload is missing tracked files: {missing}")

    contents = {name: committed_bytes(repo, commit, name) for name in PAYLOAD}
    plugin = contents["plugin.yaml"].decode("utf-8")
    match = re.search(r"^version:\s*([^\s]+)\s*$", plugin, re.MULTILINE)
    if not match:
        raise SystemExit("plugin.yaml has no version")
    version = match.group(1)
    conformance = json.loads(contents["conformance.json"])
    if conformance.get("adapterVersion") != version:
        raise SystemExit("plugin.yaml and conformance.json versions differ")

    basename = f"synthetic-sociality-hermes-room-{version}"
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    archive = output / f"{basename}.tar.gz"

    directories = {PurePosixPath(basename)}
    for name in PAYLOAD:
        parent = PurePosixPath(basename, name).parent
        while parent != PurePosixPath("."):
            directories.add(parent)
            if parent == PurePosixPath(basename):
                break
            parent = parent.parent

    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w", format=tarfile.PAX_FORMAT) as bundle:
        for directory in sorted(directories, key=lambda value: (len(value.parts), str(value))):
            info = tarfile.TarInfo(f"{directory}/")
            info.type = tarfile.DIRTYPE
            info.size = 0
            info.mtime = commit_epoch
            info.uid = info.gid = 0
            info.uname = info.gname = "root"
            info.mode = 0o775
            bundle.addfile(info)

        for name in PAYLOAD:
            data = contents[name]
            info = tarfile.TarInfo(f"{basename}/{name}")
            info.size = len(data)
            info.mtime = commit_epoch
            info.uid = info.gid = 0
            info.uname = info.gname = "root"
            info.mode = 0o775 if name == "install.sh" else 0o664
            bundle.addfile(info, io.BytesIO(data))

    with archive.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            compressed.write(tar_buffer.getvalue())

    archive_digest = sha256(archive.read_bytes())
    manifest = {
        "archive": archive.name,
        "archiveSha256": archive_digest,
        "artifact": "synthetic-sociality-hermes-room",
        "contractSha256": sha256(contents["docs/delivery-lifecycle-contract-v1.md"]),
        "files": {name: sha256(contents[name]) for name in PAYLOAD},
        "schemaVersion": 1,
        "signature": None,
        "sourceCommit": commit,
        "version": version,
    }
    manifest_path = output / f"{basename}.manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "archive": str(archive),
        "manifest": str(manifest_path),
        "sha256": archive_digest,
        "sourceCommit": commit,
        "version": version,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

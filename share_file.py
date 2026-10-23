"""P10: explicit, source-bound file sharing; no automatic retry of create."""
from __future__ import annotations
import asyncio
import hashlib
import json
import os
import stat
import threading
from pathlib import Path

TOOL_NAME = 'synthetic_sociality_room_share_file'
SHARE_INSTRUCTION = 'Share a file you want the Room to see with synthetic_sociality_room_share_file, never by naming a local path or pasting its contents.'
PARAMETERS = {'type': 'object', 'properties': {'path': {'type': 'string'}, 'title': {'type': 'string'}}, 'required': ['path'], 'additionalProperties': False}
DESCRIPTION = 'Share a file from your configured workspace with the Room during this turn; stages it on the single canonical post.'
SCHEMA = {'description': DESCRIPTION, 'parameters': PARAMETERS}
LIMIT = 2_031_616
TYPES = {'.pdf': 'application/pdf', '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation', '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.csv': 'text/csv', '.json': 'application/json', '.md': 'text/markdown', '.txt': 'text/plain', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp'}

def configured_root(extra):
    configured = extra.get('shareRoot') or os.environ.get('TERMINAL_CWD') or os.environ.get('MESSAGING_CWD')
    if configured:
        return configured
    try:
        # Ask Hermes for the same local workspace its file/terminal tools use.
        # A remote/container cwd is not a host file path: no guessed mapping.
        from tools.terminal_tool import _get_env_config
        config = _get_env_config()
        if config.get('env_type') == 'local':
            return str(config.get('cwd') or '')
    except Exception:
        pass
    return ''

def refused(code):
    return dict(success=False, code=code, **({'limitBytes': LIMIT} if code == 'size' else {}))

def valid_refs(value):
    return isinstance(value, list) and len(value) <= 8 and all(
        isinstance(ref, dict) and set(ref) == {'artifactId', 'versionId'} and
        all(isinstance(ref[k], str) and 0 < len(ref[k]) <= 128 for k in ref) for ref in value)

def valid_uploads(value):
    return isinstance(value, dict) and all(
        isinstance(source, str) and source and isinstance(records, dict) and len(records) <= 8 and all(
            isinstance(key, str) and len(key) == 64 and all(c in '0123456789abcdef' for c in key) and
            isinstance(record, dict) and record.get('state') in ('creating', 'created', 'refused') and
            (record.get('state') != 'created' or (
                isinstance(record.get('result'), dict) and record['result'].get('success') is True and
                valid_refs([{'artifactId': record['result'].get('artifactId'), 'versionId': record['result'].get('versionId')}])
            )) for key, record in records.items()) for source, records in value.items())

def refs(records):
    return [{'artifactId': r['result']['artifactId'], 'versionId': r['result']['versionId']} for r in records.values() if r.get('result', {}).get('success') is True]

def receipt(value):
    # The create response is the artifact DTO (as with GET), not a manifest.
    artifact = value.get('artifact', value) if isinstance(value, dict) else {}
    version = artifact.get('currentVersion') or artifact.get('latestVersion') or value.get('version') or {}
    aid = artifact.get('artifactId') or artifact.get('id')
    vid = value.get('versionId') or artifact.get('currentVersionId') or artifact.get('latestVersionId') or version.get('versionId') or version.get('id')
    if not all(isinstance(v, str) and 0 < len(v) <= 128 for v in (aid, vid)):
        raise ValueError('Invalid artifact receipt')
    return aid, vid

class ShareTurn:
    def __init__(self, records, root, excluded, persist, create, sealed=lambda: False):
        self.records, self.root, self.excluded = records, root, excluded
        self.persist, self.create, self.sealed = persist, create, sealed
        self.active = True
        self.lock = threading.Lock()

    def save(self):
        if self.persist() is False:
            raise OSError('Share intent not persisted')

    def share(self, args):
        with self.lock:
            return self._share(args)

    def _share(self, args):
        if not self.active or self.sealed(): return refused('no Room turn')
        if not isinstance(args, dict) or set(args) - {'path', 'title'} or not isinstance(args.get('path'), str) or not args['path'].strip() or ('title' in args and not isinstance(args['title'], str)):
            return refused('invalid_arguments')
        try:
            if not self.root: return refused('outside-share-root')
            root = Path(self.root).expanduser().resolve(strict=True)
            path = Path(args['path']).expanduser()
            path = (path if path.is_absolute() else root / path).resolve(strict=True)
            if not path.is_relative_to(root) or any(path.is_relative_to(Path(p).expanduser().resolve()) for p in self.excluded):
                return refused('outside-share-root')
            title = args.get('title') or path.name
            key = hashlib.sha256(json.dumps([str(path), title], ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
            previous = self.records.get(key)
            if previous:
                self.save()  # A receipt whose previous write failed must become durable.
                return previous.get('result') or refused('upload-uncertain')
            if len(self.records) >= 8: return refused('per-turn cap')
            mime = TYPES.get(path.suffix.lower())
            if mime is None: return refused('type')
            before = path.stat()
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1: return refused('file')
            if before.st_size > LIMIT: return refused('size')
            # Walk resolved components with no-follow descriptors, including parents.
            # This keeps a renamed/symlink-swapped directory from opening another tree.
            directory = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
            try:
                for part in path.parts[1:-1]:
                    child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                    os.close(directory)
                    directory = child
                fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            finally:
                os.close(directory)
            with os.fdopen(fd, 'rb') as stream:
                opened = os.fstat(stream.fileno())
                if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino) or opened.st_nlink != 1: return refused('file')
                data = stream.read(LIMIT + 1)
            if len(data) > LIMIT: return refused('size')
        except (OSError, ValueError, RuntimeError):
            return refused('file')
        record = {'state': 'creating'}
        self.records[key] = record
        try:
            self.save()  # Never issue create before its uncertainty marker is durable.
            value = self.create(data, path.name, mime, title)
            aid, vid = receipt(value)
            record['result'] = dict(success=True, name=path.name, size=len(data), mimeType=mime, artifactId=aid, versionId=vid)
            record['state'] = 'created'
            self.save()  # Before returning to the model and before the canonical post.
            return dict(record['result'])
        except Exception as error:
            status, code = getattr(error, 'status', 0), getattr(error, 'code', '')
            if 400 <= status < 500 and code:
                record['result'] = refused('size' if status == 413 else 'type' if status == 400 and code == 'invalid_enum' else 'upload')
                record['state'] = 'refused'
                try: self.save()
                except Exception: return refused('upload-uncertain')
                return record['result']
            return refused('upload-uncertain')

# A host run registers its exact Room; the hook binds the host session to it.
_ACTIVE = {}
_ACTIVE_LOCK = threading.Lock()

def begin(room_id, turn):
    with _ACTIVE_LOCK:
        if room_id in _ACTIVE: raise RuntimeError('Room share run already active')
        _ACTIVE[room_id] = turn

def end(room_id, turn):
    with _ACTIVE_LOCK:
        turn.active = False
        if _ACTIVE.get(room_id) is turn: _ACTIVE.pop(room_id)

def finish_dispatch(room_id, turn, owner):
    if isinstance(owner, asyncio.Future) and not owner.done():
        owner.add_done_callback(lambda _task: end(room_id, turn))
    else:
        end(room_id, turn)

def note_host_turn(session_id, turn_id, room_id):
    with _ACTIVE_LOCK:
        turn = _ACTIVE.get(room_id)
        if turn is not None:
            turn.host_session = str(session_id or '')
            turn.host_turn = str(turn_id or '')

def room_share_file(args, **metadata):
    from .origin_context import room_for_session
    room = room_for_session(metadata.get('session_id'))
    with _ACTIVE_LOCK:
        turn = _ACTIVE.get(room)
    if turn is not None and getattr(turn, 'host_turn', '') and str(metadata.get('turn_id') or '') != turn.host_turn:
        turn = None
    return json.dumps(turn.share(args) if turn else refused('no Room turn'), ensure_ascii=False, separators=(',', ':'))

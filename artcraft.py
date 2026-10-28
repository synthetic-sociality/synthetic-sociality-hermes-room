"""P14: Room-bound ArtCraft tools. No local app execution or file access."""
from __future__ import annotations
import concurrent.futures
import json
import math
from pathlib import Path
import re
import threading
import time
from urllib.parse import quote, urlencode
import uuid
from . import share_file

TOOLS = json.loads(Path(__file__).with_name('artcraft-tools.json').read_text())
ERROR_CODES = ('artcraft_off', 'app_not_allowed', 'not_member', 'agent_not_granted', 'device_offline', 'busy', 'invalid_request', 'stale_base', 'quota', 'not_found', 'upstream')
FIELDS = {
    'artcraft_capabilities': ('available', 'reason', 'apps', 'device', 'schema'),
    'artcraft_create': ('jobId', 'status'), 'artcraft_run': ('jobId', 'status'),
    'artcraft_job': ('jobId', 'status', 'error', 'artifact'),
    'artcraft_artifact': ('artifactId', 'app', 'name', 'revisions', 'cardUrl'),
}
MAX_REQUEST_BYTES = 256 * 1024
CARD_URL = re.compile(r'^/\?roomId=[a-z0-9]{15}#artcraft-[a-z0-9]{15}-[a-z0-9]{15}$')
UNSAFE = re.compile(r'(?:^|[\s\x22\x27(:=])/(?:root|home|Users|var|etc|opt|tmp|private|srv|usr|run)(?=[/\s\x22\x27():,;=\]}]|$)|\bfile://|\bBearer\s+\S+', re.I)
PRIVATE = re.compile(r'^(?:credentials?|(?:access|refresh|membership|auth)?[_-]?token|secret(?:[_-]?key)?|password|authorization|bearer|(?:host|local)[_-]?paths?)$', re.I)


def error(code, message):
    return {'error': {'code': code, 'message': message}}


def clean(value, credential='', key=''):
    if key == 'cardUrl' and (not isinstance(value, str) or not (
            CARD_URL.fullmatch(value) or re.fullmatch(r'https?://[^\s]+', value))):
        raise ValueError('Invalid card URL')
    if isinstance(value, str):
        if UNSAFE.search(value) or (credential and credential in value):
            raise ValueError('Unsafe result')
        return value
    if isinstance(value, list):
        return [clean(v, credential) for v in value]
    if isinstance(value, dict):
        return {k: clean(v, credential, k) for k, v in value.items() if not PRIVATE.search(k)}
    if value is None or isinstance(value, (bool, int, float)):
        return value
    raise ValueError('Invalid result')


def valid(args, spec):
    if not isinstance(args, dict) or set(args) - set(spec['properties']) or any(k not in args for k in spec['required']):
        return False
    for k, v in args.items():
        rule = spec['properties'][k]
        if rule['type'] == 'string':
            if not isinstance(v, str) or not rule.get('minLength', 0) <= len(v) <= rule.get('maxLength', math.inf): return False
            if 'pattern' in rule and not re.fullmatch(rule['pattern'], v): return False
        elif rule['type'] == 'array':
            if not isinstance(v, list) or not rule.get('minItems', 0) <= len(v) <= rule.get('maxItems', math.inf): return False
            if k == 'calls' and not all(isinstance(c, dict) for c in v): return False
        elif rule['type'] == 'number':
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or not rule['minimum'] <= v <= rule['maximum']: return False
    if 'oneOf' in spec and not (('calls' in args and 'operation' not in args and 'inputs' not in args) or ('calls' not in args and 'operation' in args and 'inputs' in args)): return False
    return all(args.get(k) not in ('.', '..') for k in ('file_name', 'job_id', 'artifact_id'))


def valid_result(name, value):
    if name == 'artcraft_capabilities':
        return isinstance(value.get('available'), bool) and isinstance(value.get('apps'), list) and value.get('device') in ('ready', 'offline', 'busy')
    if name == 'artcraft_artifact':
        return all(isinstance(value.get(k), str) and value[k] for k in ('artifactId', 'app', 'name', 'cardUrl')) and isinstance(value.get('revisions'), list)
    return isinstance(value.get('jobId'), str) and bool(value['jobId']) and value.get('status') in (('queued', 'running', 'done', 'failed', 'cancelled') if name == 'artcraft_job' else ('queued', 'running'))


class ArtCraftTurn:
    def __init__(self, room_id, request, active, credential='', *, clock=time.monotonic, sleep=time.sleep):
        self.prefix = '/rooms/' + quote(room_id, safe='') + '/artcraft/agent'
        self.request, self.active, self.credential = request, active, credential
        self.clock, self.sleep = clock, sleep

    def call(self, method, suffix, body=None, timeout=20):
        if not self.active(): return error('not_member', 'ArtCraft requires an active Room turn.')
        if method == 'POST':
            try:
                if len(json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode('utf-8')) > MAX_REQUEST_BYTES:
                    return error('invalid_request', 'ArtCraft request exceeds 256 KiB.')
            except (TypeError, ValueError, UnicodeError):
                return error('invalid_request', 'Invalid ArtCraft request body.')
        try:
            value = self.request(method, self.prefix + suffix, body, timeout)
        except Exception as exc:
            value = getattr(exc, 'artcraft_error', None)
            if not value: return error('upstream', 'ArtCraft request failed.')
        if not self.active(): return error('not_member', 'ArtCraft requires an active Room turn.')
        if not isinstance(value, dict): return error('upstream', 'Invalid ArtCraft response.')
        if 'error' in value and 'jobId' not in value:
            e = value['error']
            if not isinstance(e, dict) or e.get('code') not in ERROR_CODES or not isinstance(e.get('message'), str): return error('upstream', 'Invalid ArtCraft response.')
            return clean(error(e['code'], e['message']), self.credential)
        return value

    def execute(self, name, args):
        if not self.active(): return error('not_member', 'ArtCraft requires an active Room turn.')
        if name not in TOOLS or not valid(args, TOOLS[name]['parameters']): return error('invalid_request', 'Invalid ArtCraft arguments.')
        try:
            result = self._execute(name, args)
            if not self.active(): return error('not_member', 'ArtCraft requires an active Room turn.')
            if 'error' not in result or 'jobId' in result:
                if not valid_result(name, result): return error('upstream', 'Invalid ArtCraft response.')
                result = {k: result[k] for k in FIELDS[name] if k in result}
            return clean(result, self.credential)
        except Exception:
            return error('upstream', 'ArtCraft request failed.')

    def _execute(self, name, args):
        if name == 'artcraft_capabilities':
            return self.call('GET', '/capabilities' + ('?' + urlencode({'app': args['app']}) if 'app' in args else ''))
        if name == 'artcraft_artifact': return self.call('GET', '/artifacts/' + quote(args['artifact_id'], safe=''))
        if name == 'artcraft_job': return self.poll(args)
        body = {'idempotencyKey': args.get('idempotency_key', str(uuid.uuid4())), 'kind': 'create' if name == 'artcraft_create' else 'run'}
        if name == 'artcraft_create': body.update(app=args['app'], fileName=args['file_name'])
        else:
            artifact = self.call('GET', '/artifacts/' + quote(args['artifact_id'], safe=''))
            if 'error' in artifact: return artifact
            app, filename = artifact.get('app'), artifact.get('name')
            if not isinstance(app, str) or not app or not valid({'app': app, 'file_name': filename, 'calls': [{}]}, TOOLS['artcraft_create']['parameters']):
                return error('upstream', 'Invalid ArtCraft artifact metadata.')
            body.update(app=app, fileName=filename, artifactId=args['artifact_id'], baseRevision=args['base_revision'])
        for k in ('calls', 'operation', 'inputs'):
            if k in args: body[k] = args[k]
        return self.call('POST', '/jobs', body)

    def poll(self, args):
        # A daemon reader plus Future timeout bounds even a trickling HTTP body.
        budget = args.get('wait_seconds', 0)
        deadline = self.clock() + (budget or 20)
        last = None
        while self.active():
            remaining = deadline - self.clock()
            if remaining <= 0: return last or error('upstream', 'ArtCraft request timed out.')
            future = concurrent.futures.Future()
            def read(future=future, remaining=remaining):
                try: future.set_result(self.call('GET', '/jobs/' + quote(args['job_id'], safe=''), timeout=remaining))
                except Exception: future.set_result(error('upstream', 'ArtCraft request failed.'))
            threading.Thread(target=read, daemon=True).start()
            try: last = future.result(timeout=remaining)
            except concurrent.futures.TimeoutError: return last or error('upstream', 'ArtCraft request timed out.')
            if 'error' in last or last.get('status') not in ('queued', 'running') or not budget: return last
            remaining = deadline - self.clock()
            if remaining <= 0: return last
            self.sleep(min(0.25, remaining))
        return error('not_member', 'ArtCraft requires an active Room turn.')


def room_tool(name, args, **metadata):
    _, turn = share_file.active_turn(refusal_message='ArtCraft request refused: not in a Room turn', **metadata)
    bound = getattr(turn, 'artcraft', None)
    result = bound.execute(name, args) if bound else error('not_member', 'ArtCraft requires an active Room turn.')
    return json.dumps(result, ensure_ascii=False, separators=(',', ':'))


def register(ctx):
    for name, spec in TOOLS.items():
        def handler(args, _name=name, **metadata):
            return room_tool(_name, args, **metadata)
        ctx.register_tool(name=name, toolset='synthetic_sociality_room', schema=spec,
                          handler=handler, description=spec['description'], emoji='◌')

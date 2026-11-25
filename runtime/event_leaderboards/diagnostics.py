"""Bounded, allowlisted scan telemetry. Never accept provider payloads or URLs."""
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
from functools import wraps
import json
import math
import re
import sys
import threading
import time
import uuid

PREFIX = '[event-scan-diagnostic] '
MAX_LINE_BYTES = 16384
CURRENT = ContextVar('event_scan_diagnostic', default=None)
PHASES = frozenset({'startup', 'event_setup', 'head', 'boundary', 'checkpoint', 'overlap',
                    'forward_logs', 'forward_verify', 'receipt', 'commit', 'publish'})
ACTIONS = frozenset({'eth_blockNumber', 'eth_getBlockByNumber', 'eth_getTransactionReceipt',
                     'getblocknobytime', 'getLogs', 'unknown'})
FAILURES = frozenset({'provider_timeout', 'provider_stale_connection', 'provider_tls_verification_failure', 'provider_tls_failure',
                      'provider_dns_failure', 'provider_invalid_json', 'provider_invalid_request',
                      'provider_connection_failure', 'provider_invalid_response', 'provider_query_limit',
                      'provider_rejected_request', 'provider_rate_limit', 'provider_quota_exhausted',
                      'provider_missing_result', 'receipt_unavailable', 'provider_http_error', 'unclassified'})
METRICS = ('attempts', 'retries', 'timeouts', 'errors', 'request_ms', 'max_request_ms',
           'pacing_wait_ms', 'retry_wait_ms')
DETAILS = frozenset({'from_block', 'through_block', 'hint_block', 'resolved_block', 'head_block',
                     'verified_checkouts', 'inserted', 'new_checkouts', 'chunks', 'block_reads',
                     'events', 'logs', 'known_checkouts', 'receipt_reused', 'source_index'})


def _metrics():
    return {name: 0 for name in METRICS}


def _failure_allowed(code):
    return code in FAILURES or re.fullmatch(r'provider_http_[1-5][0-9]{2}', code) is not None


def _safe_numbers(value, allowed):
    if not isinstance(value, dict) or set(value) - set(allowed):
        raise ValueError('invalid diagnostic metrics')
    if any(type(n) not in (int, float) or not math.isfinite(n) or not 0 <= n <= 10**15
           for n in value.values()):
        raise ValueError('invalid diagnostic number')
    return dict(value)


def validated_line(line):
    """Rebuild a child record from a closed schema before forwarding it to logs."""
    if not line.startswith(PREFIX) or len(line.encode()) > MAX_LINE_BYTES:
        return None
    try:
        data = json.loads(line[len(PREFIX):])
        allowed = {'version', 'kind', 'run_id', 'event_ref', 'phase', 'status', 'at', 'elapsed_ms',
                   'totals', 'actions', 'phases', 'phase_actions', 'failures', 'details'}
        if not isinstance(data, dict) or set(data) - allowed or data.get('version') != 1:
            return None
        if data.get('kind') not in {'started', 'phase_started', 'phase_finished', 'heartbeat', 'finished'}:
            return None
        if not re.fullmatch(r'[a-f0-9]{32}', data.get('run_id', '')):
            return None
        if 'event_ref' in data and not re.fullmatch(r'[a-f0-9]{16}', data['event_ref']):
            return None
        if 'phase' in data and data['phase'] not in PHASES:
            return None
        if 'status' in data and data['status'] not in {'ok', 'error', 'cancelled'}:
            return None
        if not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z', data.get('at', '')):
            return None
        _safe_numbers({'elapsed_ms': data['elapsed_ms']}, {'elapsed_ms'})
        if 'totals' in data:
            _safe_numbers(data['totals'], METRICS)
        if 'details' in data:
            _safe_numbers(data['details'], DETAILS)
        for name, allowed_keys, value_keys in [('actions', ACTIONS, METRICS),
                                                ('phases', PHASES, (*METRICS, 'elapsed_ms', 'calls'))]:
            values = data.get(name, {})
            if not isinstance(values, dict) or set(values) - allowed_keys:
                return None
            for values_for_key in values.values():
                _safe_numbers(values_for_key, value_keys)
        phase_actions = data.get('phase_actions', {})
        if not isinstance(phase_actions, dict) or set(phase_actions) - PHASES:
            return None
        for counts in phase_actions.values():
            _safe_numbers(counts, ACTIONS)
        failures = data.get('failures', {})
        if not isinstance(failures, dict) or any(not _failure_allowed(code) for code in failures):
            return None
        _safe_numbers(failures, set(failures))
        return PREFIX + json.dumps(data, sort_keys=True, separators=(',', ':'), allow_nan=False)
    except (ValueError, TypeError, KeyError, OverflowError):
        return None


def emit_stderr(line):
    print(line, file=sys.stderr, flush=True)


def synchronized(method):
    @wraps(method)
    def locked(self, *args, **kwargs):
        with self.lock:
            return method(self, *args, **kwargs)
    return locked


class ScanDiagnostics:
    """One event/job scope. Phase durations include children; counters do not."""
    def __init__(self, *, sink=emit_stderr, run_id=None, event_id=None):
        self.sink = sink
        self.lock = threading.RLock()
        self.run_id = run_id or uuid.uuid4().hex
        self.event_ref = hashlib.sha256(event_id.encode()).hexdigest()[:16] if event_id is not None else None
        self.started = time.monotonic()
        self.last_emit = self.started
        self.totals, self.actions, self.phases, self.phase_actions = _metrics(), {}, {}, {}
        self.failures = {}
        self.current_phase = None
        self.outcome = 'ok'

    def __enter__(self):
        self.parent = CURRENT.get()
        self.token = CURRENT.set(self)
        self.emit('started')
        return self

    def __exit__(self, error_type, error, traceback):
        try:
            failed = error_type is not None and not (error_type is SystemExit and error.code in (None, 0))
            self.emit('finished', status='error' if failed else self.outcome, **self.summary())
        finally:
            CURRENT.reset(self.token)
            if self.parent is not None:
                self.parent.merge(self)

    @synchronized
    def merge(self, child):
        for target, source in [(self.totals, child.totals),
                               *((self.actions.setdefault(k, _metrics()), v) for k, v in child.actions.items()),
                               *((self.phases.setdefault(k, {**_metrics(), 'elapsed_ms': 0, 'calls': 0}), v)
                                 for k, v in child.phases.items())]:
            for key, value in source.items():
                target[key] = max(target[key], value) if key == 'max_request_ms' else target[key] + value
        for name, actions in child.phase_actions.items():
            target = self.phase_actions.setdefault(name, {})
            for action, count in actions.items():
                target[action] = target.get(action, 0) + count
        for code, count in child.failures.items():
            self.failures[code] = self.failures.get(code, 0) + count

    @synchronized
    def emit(self, kind, **values):
        record = {'version': 1, 'kind': kind, 'run_id': self.run_id,
                  'at': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                  'elapsed_ms': max(0, round((time.monotonic() - self.started) * 1000)), **values}
        if self.event_ref is not None:
            record['event_ref'] = self.event_ref
        line = validated_line(PREFIX + json.dumps(record, allow_nan=False))
        if line is None:
            raise ValueError('invalid diagnostic record')
        if self.sink is not None:
            self.sink(line)
        self.last_emit = time.monotonic()

    @synchronized
    def summary(self):
        return {'totals': dict(self.totals), 'actions': {k: dict(v) for k, v in self.actions.items()},
                'phases': {k: dict(v) for k, v in self.phases.items()},
                'phase_actions': {k: dict(v) for k, v in self.phase_actions.items()}, 'failures': dict(self.failures)}

    @synchronized
    def add(self, action, **metrics):
        action = action if action in ACTIONS else 'unknown'
        phase = self.current_phase
        targets = [self.totals, self.actions.setdefault(action, _metrics())]
        if phase is not None:
            targets.append(self.phases[phase])
            if metrics.get('attempts', 0):
                counts = self.phase_actions.setdefault(phase, {})
                counts[action] = counts.get(action, 0) + metrics['attempts']
        for target in targets:
            for key, value in metrics.items():
                if key == 'max_request_ms':
                    target[key] = max(target[key], value)
                else:
                    target[key] += value
        if time.monotonic() - self.last_emit >= 30:
            self.emit('heartbeat', **({'phase': phase} if phase else {}), **self.summary())

    @contextmanager
    def phase(self, name, **details):
        if name not in PHASES:
            raise ValueError('unknown diagnostic phase')
        _safe_numbers(details, DETAILS)
        started, previous = time.monotonic(), self.current_phase
        self.current_phase = name
        stat = self.phases.setdefault(name, {**_metrics(), 'elapsed_ms': 0, 'calls': 0})
        stat['calls'] += 1
        # Range/boundary records are needed for every source. Per-transaction
        # receipt/commit phases remain aggregated to keep high-volume logs bounded.
        emit_phase = stat['calls'] == 1 or name in {'boundary', 'overlap', 'forward_logs', 'publish'}
        if emit_phase:
            self.emit('phase_started', phase=name, details=details)
        status = 'ok'
        try:
            yield details
        except BaseException:
            status = 'error'
            raise
        finally:
            stat['elapsed_ms'] += max(0, round((time.monotonic() - started) * 1000))
            self.current_phase = previous
            if emit_phase or status == 'error':
                self.emit('phase_finished', phase=name, status=status, details=details, **self.summary())


@contextmanager
def phase(name, **details):
    diagnostic = CURRENT.get()
    if diagnostic is None:
        yield details
    else:
        with diagnostic.phase(name, **details) as result:
            yield result


def record_request(action, *, elapsed_ms=0, error=False, timeout=False, retry=False,
                   pacing_wait_ms=0, retry_wait_ms=0, attempted=False, failure_code=None):
    diagnostic = CURRENT.get()
    if diagnostic is not None:
        with diagnostic.lock:
            if failure_code is not None:
                code = failure_code if _failure_allowed(failure_code) else 'unclassified'
                diagnostic.failures[code] = diagnostic.failures.get(code, 0) + 1
            diagnostic.add(action, attempts=int(attempted), errors=int(error), timeouts=int(timeout),
                           retries=int(retry), request_ms=elapsed_ms, max_request_ms=elapsed_ms,
                           pacing_wait_ms=pacing_wait_ms, retry_wait_ms=retry_wait_ms)

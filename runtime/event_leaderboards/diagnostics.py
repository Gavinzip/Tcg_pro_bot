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
from .response_evidence import valid_observation
from .overlap_evidence import valid_overlap_comparison

PREFIX = '[event-scan-diagnostic] '
MAX_LINE_BYTES = 16384
CURRENT = ContextVar('event_scan_diagnostic', default=None)
PHASES = frozenset({'startup', 'event_setup', 'head', 'boundary', 'checkpoint', 'overlap',
                    'forward_logs', 'forward_verify', 'receipt', 'canonical_block', 'commit', 'publish'})
ACTIONS = frozenset({'eth_blockNumber', 'eth_getBlockByNumber', 'eth_getTransactionReceipt',
                     'eth_getStorageAt', 'eth_getCode', 'getblocknobytime', 'getLogs', 'unknown'})
FAILURES = frozenset({'provider_timeout', 'provider_stale_connection', 'provider_tls_verification_failure', 'provider_tls_failure',
                      'provider_dns_failure', 'provider_invalid_json', 'provider_invalid_request',
                      'provider_connection_failure', 'provider_invalid_response', 'provider_query_limit',
                      'provider_rejected_request', 'provider_rate_limit', 'provider_quota_exhausted',
                      'provider_missing_result', 'provider_historical_state_unavailable', 'provider_invalid_boundary_hint', 'receipt_unavailable', 'provider_http_error', 'unclassified'})
# Closed codes, never arbitrary exception strings or upstream payloads. Request
# failures and terminal scan failures are separate so counters keep their meaning.
TERMINAL_FAILURES = frozenset({
    'invalid_integer', 'negative_integer', 'invalid_address', 'invalid_hash',
    'invalid_abi_data', 'invalid_abi_length', 'invalid_indexed_address', 'invalid_block',
    'boundary_above_head', 'invalid_logs_page', 'provider_repeated_log_page',
    'provider_single_block_result_limit', 'log_outside_requested_range',
    'unexpected_checkout_log', 'unexpected_checkout_builder', 'conflicting_duplicate_log',
    'removed_checkout_log', 'invalid_checkout_topics', 'invalid_v3_checkout_data',
    'unsupported_checkout_currency', 'invalid_v3_checkout_ids', 'invalid_legacy_checkout_data',
    'unsuccessful_or_wrong_receipt', 'receipt_reorg_mismatch', 'receipt_missing_logs',
    'inconsistent_receipt_log', 'duplicate_receipt_log', 'invalid_usdt_transfer',
    'duplicate_checkout_id', 'invalid_legacy_batch_topics', 'invalid_legacy_batch_data',
    'legacy_batch_members_mismatch', 'legacy_batch_dust_payment',
    'checkout_payment_amount_mismatch', 'indexed_log_receipt_mismatch',
    'unsupported_chain_or_decimals', 'audited_schema_configuration_mismatch',
    'unaudited_pack_contract', 'insufficient_confirmations',
    'checkpoint_missing_reorg_anchor', 'checkpoint_reorg_mismatch',
    'confirmed_head_below_checkpoint', 'overlap_missing_known_checkout',
    'checkout_block_mismatch',
    'event_outside_anchored_range',
    'event_timestamp_outside_anchors',
    'invalid_audited_v3_checkout',
    'invalid_event_block_number', 'invalid_event_timestamp',
    'event_block_anchor_mismatch', 'inconsistent_event_block',
    'invalid_chain_request_rate',
    'invalid_event_block_hash',
    'missing_chain_api_key',
    'missing_event_timestamp',
    'missing_verified_event_block',
    'nonmonotonic_event_timestamp',
    'transaction_block_mismatch',
    'unaudited_event_proof_contract',
    'v3_implementation_code_mismatch',
    'v3_implementation_mismatch',
    'v3_invalid_implementation_code',
    'v3_invalid_implementation_slot',
    'v3_unaudited_block_range',
    'v3_unguarded_checkout',
    'v3_upgrade_in_scan_range',
})
METRICS = ('attempts', 'retries', 'timeouts', 'errors', 'request_ms', 'max_request_ms',
           'pacing_wait_ms', 'retry_wait_ms')
DETAILS = frozenset({'from_block', 'through_block', 'hint_block', 'resolved_block', 'head_block',
                     'verified_checkouts', 'inserted', 'new_checkouts', 'chunks', 'block_reads',
                     'events', 'logs', 'known_checkouts', 'receipt_reused', 'source_index',
                     'target_timestamp', 'hint_invalid'})


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
                   'totals', 'actions', 'phases', 'phase_actions', 'failures', 'terminal_failures', 'details',
                   'observation', 'failure_code', 'comparison'}
        if not isinstance(data, dict) or set(data) - allowed or data.get('version') != 1:
            return None
        if data.get('kind') not in {'started', 'phase_started', 'phase_finished', 'heartbeat', 'finished', 'provider_response', 'overlap_comparison'}:
            return None
        if data['kind'] == 'overlap_comparison':
            if not valid_overlap_comparison(data.get('comparison')):
                return None
        elif 'comparison' in data:
            return None
        if data['kind'] == 'provider_response':
            code = data.get('failure_code')
            if (not valid_observation(data.get('observation')) or not isinstance(code, str)
                    or not (code == 'ok' or _failure_allowed(code) or code in TERMINAL_FAILURES)):
                return None
        elif 'observation' in data or 'failure_code' in data:
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
        terminal = data.get('terminal_failures', {})
        if not isinstance(terminal, dict) or any(not (_failure_allowed(code) or code in TERMINAL_FAILURES) for code in terminal):
            return None
        _safe_numbers(terminal, set(terminal))
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
        self.terminal_failures = {}
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
        for code, count in child.terminal_failures.items():
            self.terminal_failures[code] = self.terminal_failures.get(code, 0) + count

    @synchronized
    def terminal_failure(self, code):
        safe = code if isinstance(code, str) and (_failure_allowed(code) or code in TERMINAL_FAILURES) else 'unclassified'
        self.terminal_failures[safe] = self.terminal_failures.get(safe, 0) + 1

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
                'phase_actions': {k: dict(v) for k, v in self.phase_actions.items()}, 'failures': dict(self.failures),
                'terminal_failures': dict(self.terminal_failures)}

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


def record_provider_response(observation, code):
    diagnostic = CURRENT.get()
    if diagnostic is not None:
        safe = code if isinstance(code, str) and (code == 'ok' or _failure_allowed(code) or code in TERMINAL_FAILURES) else 'unclassified'
        diagnostic.emit('provider_response', observation=observation, failure_code=safe)

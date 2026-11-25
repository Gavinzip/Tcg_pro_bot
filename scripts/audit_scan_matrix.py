#!/usr/bin/env python3
"""Bounded real-chain matrix; a frozen confirmed upper bound, never fake responses.

Credentials arrive only via an inherited RAM pipe (fd 3). Evidence stays in the
ignored audit directory. No production database or deployment is modified.
"""
from __future__ import annotations

import argparse
from collections import OrderedDict
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from runtime.event_leaderboards.config import EventDefinition, source_key, timestamp
from runtime.event_leaderboards.diagnostics import ScanDiagnostics
from runtime.event_leaderboards.scanner import EtherscanProvider, EventLeaderboardScanner, SourceIncomplete
from runtime.event_leaderboards.store import EventLeaderboardStore


class Budget:
    def __init__(self, root, maximum=600, already_used=2):
        self.path = root / 'budget.json'
        self.lock = threading.Lock()
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {
            'maximum': maximum, 'requests': already_used, 'actions': {'quota': already_used}}

    def reserve(self, action):
        with self.lock:
            if self.state['requests'] >= self.state['maximum']:
                raise SourceIncomplete('audit_request_budget_exhausted')
            self.state['requests'] += 1
            actions = self.state['actions']
            actions[action] = actions.get(action, 0) + 1
            self.path.write_text(json.dumps(self.state, indent=2) + '\n')


class AuditProvider(EtherscanProvider):
    def __init__(self, key, budget, capture, *, bound, rps, **options):
        super().__init__({'EVENT_LEADERBOARD_API_KEY': key,
                          'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': str(rps), **options})
        self.budget, self.capture, self.bound = budget, capture, bound
        self.capture_lock = threading.Lock()

    def head(self):
        actual = super().head()
        if actual < self.bound + 64:
            raise SourceIncomplete('audit_bound_not_confirmed')
        return self.bound + 64

    def _read_response(self, module, action, params):
        self.budget.reserve(action)
        result = super()._read_response(module, action, params)
        with self.capture_lock, self.capture.open('a') as stream:
            stream.write(json.dumps({'module': module, 'action': action, 'params': params,
                                     'result': result}, separators=(',', ':')) + '\n')
        return result


class ReplayProvider(AuditProvider):
    def __init__(self, *args, replay_capture, **kwargs):
        super().__init__(*args, **kwargs)
        self.responses = {}
        self.logical_requests = 0
        self.spacing = 0
        for line in Path(replay_capture).read_text().splitlines():
            row = json.loads(line)
            self.responses[self._key(row['module'], row['action'], row['params'])] = json.dumps(row['result'])

    @staticmethod
    def _key(module, action, params):
        return module, action, json.dumps(params, sort_keys=True)

    def _read_response(self, module, action, params):
        self.logical_requests += 1
        key = self._key(module, action, params)
        if key not in self.responses:
            raise SourceIncomplete('offline_capture_missing_read')
        return json.loads(self.responses[key])


def windows(source):
    with sqlite3.connect('file:' + str(source) + '?mode=ro', uri=True) as db:
        rows = sorted((json.loads(r[0]) for r in db.execute('SELECT payload FROM ledger')),
                      key=lambda r: (r['block_number'], r['log_index']))
    groups = OrderedDict()
    for row in rows:
        groups.setdefault(row['block_number'], []).append(row)
    groups = list(groups.items())
    result = [(0, rows[0]['block_number'] - 1, rows[0]['block_number'] - 1, rows[0]['timestamp'] - 1)]
    for count in (1, 5, 10, 20):
        found = None
        for i in range(len(groups)):
            txs = set()
            for j in range(i, len(groups)):
                txs.update(r['tx_hash'] for r in groups[j][1])
                if len(txs) == count:
                    found = (count, groups[i][0], groups[j][0], groups[i][1][0]['timestamp'])
                    break
                if len(txs) > count:
                    break
            if found:
                break
        if not found:
            raise ValueError('no_complete_block_window_for_count')
        result.append(found)
    return result


def fingerprint(rows):
    fields = ('tx_hash', 'log_index', 'paid_amount_raw', 'draw_count', 'block_hash', 'wallet')
    content = sorted(tuple(str(r[k]) for k in fields) for r in rows)
    return hashlib.sha256(json.dumps(content, separators=(',', ':')).encode()).hexdigest()


def measure(store, provider, definition, workers):
    before = provider.budget.state['requests']
    logical_before = getattr(provider, 'logical_requests', 0)
    started, cpu = time.perf_counter(), time.process_time()
    options = {} if workers == 1 else {'verification_workers': workers}
    with ScanDiagnostics(sink=None) as diagnostic:
        result = EventLeaderboardScanner(store, provider, **options).sync(definition.event_id)
    elapsed, cpu_elapsed = time.perf_counter() - started, time.process_time() - cpu
    read_started = time.perf_counter()
    meta, rows, _ = store.read_checkout_snapshot(definition.event_id)
    encoded = json.dumps(rows, separators=(',', ':')).encode()
    return {'state': result['state'], 'error': result.get('error'),
            'seconds': round(elapsed, 5), 'process_cpu_seconds': round(cpu_elapsed, 5),
            'requests': provider.budget.state['requests'] - before,
            'offline_logical_reads': getattr(provider, 'logical_requests', 0) - logical_before,
            'transactions': len({r['tx_hash'] for r in rows}), 'checkouts': len(rows),
            'draws': sum(r['draw_count'] for r in rows), 'checksum': fingerprint(rows),
            'read_encode_seconds': round(time.perf_counter() - read_started, 5),
            'encoded_bytes': len(encoded), 'snapshot_available': meta is not None,
            'diagnostics': diagnostic.summary()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--label', required=True)
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--rps', type=float, default=2)
    parser.add_argument('--counts', default='0,1,5,10,20')
    parser.add_argument('--pooled', action='store_true')
    parser.add_argument('--offline-capture')
    parser.add_argument('--resume-until', help='Resume a backup of the saved cold prefix to this UTC boundary')
    args = parser.parse_args()
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = root / args.label
    target.mkdir(mode=0o700)
    key = 'offline-fixture' if args.offline_capture else os.fdopen(3).read().strip()
    budget = Budget(root)
    source = Path('.scratch/whole-cost-live-20261125')
    config = json.loads((source / 'source-events.json').read_text())['events'][0]
    definition = EventDefinition('matrix', timestamp(config['start_at']), None,
                                 tuple(config['contracts']), builder=config['builder'])
    report = {'mode': 'OFFLINE_CAPTURE_CPU_ONLY' if args.offline_capture else 'REAL_NETWORK_FROZEN_CONFIRMED_BOUND', 'label': args.label,
              'workers': args.workers, 'rps': args.rps, 'cases': [], 'production_writes': 0}
    def save():
        report['budget'] = dict(budget.state)
        (target / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    try:
        if args.resume_until:
            provider = AuditProvider(key, budget, target / 'capture.jsonl', bound=0, rps=args.rps,
                                     **({'EVENT_LEADERBOARD_HTTP_POOL': '1'} if args.pooled else {}))
            actual_head = EtherscanProvider.head(provider)
            provider.bound = provider.first_block_at(timestamp(args.resume_until), actual_head) - 1
            with sqlite3.connect('file:' + str(source / 'bnb-renaiss-2026.sqlite3') + '?mode=ro', uri=True) as original:
                with sqlite3.connect(str(target / 'resumed.sqlite3')) as backup:
                    original.backup(backup)
            store = EventLeaderboardStore(str(target / 'resumed.sqlite3'), update_interval_seconds=60, stale_after_seconds=180)
            event = store.get_event(config['event_id'])
            checkpoint = store.get_checkpoint(event.event_id, source_key(event, event.contracts[0]))
            report.update(resume_until=args.resume_until, original_next_block=checkpoint['next_block'],
                          through_block=provider.bound)
            save()
            report['cold_resume'] = measure(store, provider, event, args.workers)
            save()
            if report['cold_resume']['state'] != 'active':
                raise SourceIncomplete(report['cold_resume']['error'])
            report['cold_steady'] = measure(store, provider, event, args.workers)
            provider.close()
            return 0
        for count, start, end, start_ts in windows(source / 'bnb-renaiss-2026.sqlite3'):
            if count not in {int(n) for n in args.counts.split(',')}:
                continue
            options = {'EVENT_LEADERBOARD_HTTP_POOL': '1'} if args.pooled else {}
            provider_class = ReplayProvider if args.offline_capture else AuditProvider
            if args.offline_capture:
                options['replay_capture'] = args.offline_capture
            provider = provider_class(key, budget, target / 'capture.jsonl', bound=end, rps=args.rps, **options)
            event = replace(definition, event_id='matrix-' + str(count), start_ts=start_ts)
            store = EventLeaderboardStore(str(target / (event.event_id + '.sqlite3')),
                                           update_interval_seconds=60, stale_after_seconds=180)
            store.register_event(event)
            before = budget.state['requests']
            block = provider.block(start - 1)
            store.commit_chunk(event.event_id, source_key(event, event.contracts[0]), None,
                               start, [], block_hash=block['hash'])
            store.publish_snapshot(event.event_id, start - 1, block['timestamp'])
            case = {'requested_transactions': count, 'from_block': start, 'through_block': end,
                    'preparation_requests': budget.state['requests'] - before}
            report['cases'].append(case)
            for kind in ('new_transactions', 'steady_no_new_transactions'):
                case[kind] = measure(store, provider, event, args.workers)
                save()
                print(json.dumps({'label': args.label, 'count': count, 'kind': kind,
                                  **{k: case[kind][k] for k in ('state', 'transactions', 'seconds', 'requests')},
                                  'total_requests': budget.state['requests']}), flush=True)
                if case[kind]['state'] != 'active' or case[kind]['transactions'] != count:
                    raise SourceIncomplete(case[kind]['error'] or 'audit_unexpected_transaction_count')
            if hasattr(provider, 'close'):
                provider.close()
    except Exception as error:
        report['incomplete'] = str(error) if isinstance(error, SourceIncomplete) else type(error).__name__
        print(json.dumps({'stopped': report['incomplete']}), flush=True)
        return 1
    finally:
        save()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Offline A/B of the full scanner and SQLite, never a live-network benchmark.

100 TX: captured real logs/receipts/headers from the prior bounded live audit.
1000 TX: explicitly synthetic scale-out of those shapes and payment amounts.
Both use modeled historical storage/upgrade replies for the new version guard;
the pinned runtime bytes were separately captured from the public official RPC.
No network is available through this provider. Missing captured headers fail.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import types

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.diagnostics import ScanDiagnostics
from runtime.event_leaderboards.scanner import EtherscanProvider, EventLeaderboardScanner, SourceIncomplete, number
from runtime.event_leaderboards.store import EventLeaderboardStore
from runtime.event_leaderboards.v3_evidence import AUDITED_IMPLEMENTATION, UPGRADED

BASELINE = 'f8c4990685a23e294088e6b4fd6247ee34c4f713'
CAPTURE = Path('.scratch/scan-matrix-20261125/cold-resumed-to-1440/capture.jsonl')
PREFIX = Path('.scratch/whole-cost-live-20261125/bnb-renaiss-2026.sqlite3')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def load_capture(capture_path=CAPTURE, prefix_path=PREFIX):
    capture = [json.loads(line) for line in capture_path.read_text().splitlines()]
    headers, receipts, logs, hints = {}, {}, {}, {}
    for row in capture:
        action, params, result = row['action'], row['params'], row['result']
        if action == 'eth_getBlockByNumber': headers[number(result['number'])] = result
        elif action == 'eth_getTransactionReceipt': receipts[params['txhash']] = result
        elif action == 'getblocknobytime': hints[params['timestamp']] = result
        elif action == 'getLogs':
            for event in result: logs[event['transactionHash'], event['logIndex']] = event
    with sqlite3.connect('file:' + str(prefix_path.resolve()) + '?mode=ro', uri=True) as db:
        prefix = [json.loads(r[0]) for r in db.execute('SELECT payload FROM ledger')]
        next_block = db.execute('SELECT next_block FROM checkpoints').fetchone()[0]
    groups = defaultdict(set)
    for tx, receipt in receipts.items(): groups[number(receipt['blockNumber'])].add(tx)
    selected = set()
    for block, transactions in sorted(groups.items()):
        selected.update(transactions)
        if len(selected) >= 100:
            if len(selected) != 100: raise ValueError('No exact complete-block 100-TX prefix')
            bound = block
            break
    else: raise ValueError('Insufficient complete captured transactions')
    return dict(headers=headers, receipts={k: v for k, v in receipts.items() if k in selected},
                logs=sorted((r for r in logs.values() if number(r['blockNumber']) <= bound),
                            key=lambda r: (number(r['blockNumber']), number(r['logIndex']))),
                hints=hints, prefix=prefix, next_block=next_block, bound=bound)


def scaled_capture(real):
    value = deepcopy(real)
    templates = sorted(real['receipts'].values(), key=lambda r: (number(r['blockNumber']), r['transactionHash']))
    heights = {b: i for i, b in enumerate(sorted({number(r['blockNumber']) for r in templates}))}
    previous = real['next_block'] - 1
    stamp = number(real['headers'][previous]['timestamp'])
    scoped = defaultdict(list)
    for row in real['logs']:
        if row['transactionHash'] in real['receipts']: scoped[row['transactionHash']].append(row)
    value['logs'] = [r for r in value['logs'] if number(r['blockNumber']) <= previous]
    value['receipts'] = {}
    for cycle in range(10):
        for i, template in enumerate(templates):
            block = previous + 1 + cycle * len(heights) + heights[number(template['blockNumber'])]
            tx = '0x' + digest(['synthetic-1000', cycle, i])
            block_hash = '0x' + digest(['synthetic-block', block])
            timestamp = stamp + 1 + (block - previous - 1) // 2
            def transform(original):
                row = deepcopy(original)
                row.update(transactionHash=tx, blockNumber=hex(block), blockHash=block_hash)
                if 'timeStamp' in row: row['timeStamp'] = hex(timestamp)
                if row.get('topics', [None])[0] == real['logs'][-1]['topics'][0]:
                    words = [int(row['data'][j:j + 64], 16) for j in range(2, len(row['data']), 64)]
                    words[4:] = [n + cycle * 10**9 for n in words[4:]]
                    row['data'] = '0x' + ''.join(f'{n:064x}' for n in words)
                return row
            receipt = transform(template)
            receipt['logs'] = [transform(r) for r in template['logs']]
            value['receipts'][tx] = receipt
            value['logs'].extend(transform(r) for r in scoped[template['transactionHash']])
            value['headers'][block] = dict(number=hex(block), hash=block_hash, timestamp=hex(timestamp))
    value['bound'] = max(number(r['blockNumber']) for r in value['receipts'].values())
    return value


class OfflineProvider(EtherscanProvider):
    def __init__(self, capture, runtime):
        super().__init__({'EVENT_LEADERBOARD_API_KEY': 'offline-no-network',
                          'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': '4'})
        self.capture, self.runtime = capture, runtime
        self.spacing = 0  # CPU/SQLite only. Pacing estimate is reported separately.

    def _read_response(self, module, action, params):
        c = self.capture
        if action == 'eth_blockNumber': return hex(c['bound'] + 64)
        if action == 'eth_getBlockByNumber':
            block = number(params['tag'])
            if block not in c['headers']: raise SourceIncomplete('offline_missing_captured_header')
            return deepcopy(c['headers'][block])
        if action == 'getblocknobytime':
            if params['timestamp'] not in c['hints']: raise SourceIncomplete('offline_missing_captured_time_hint')
            return c['hints'][params['timestamp']]
        if action == 'eth_getTransactionReceipt': return deepcopy(c['receipts'][params['txhash']])
        # These are explicitly modeled historical guard inputs, NOT a claim
        # that an archive-capable Etherscan request has been exercised live.
        if action == 'eth_getStorageAt': return '0x' + '0' * 24 + AUDITED_IMPLEMENTATION[2:]
        if action == 'eth_getCode': return self.runtime
        if action == 'getLogs':
            if params['topic0'] == UPGRADED: return []
            low, high = params['fromBlock'], params['toBlock']
            rows = [r for r in c['logs'] if low <= number(r['blockNumber']) <= high
                    and r['address'] == params['address'] and r['topics'][0] == params['topic0']
                    and ('topic3' not in params or r['topics'][3] == params['topic3'])]
            rows.sort(key=lambda r: (number(r['blockNumber']), number(r['logIndex'])))
            offset = (params['page'] - 1) * params['offset']
            return deepcopy(rows[offset:offset + params['offset']])
        raise AssertionError('No live HTTP path: unexpected action ' + action)


def original_scanner(commit=BASELINE):
    source = subprocess.check_output(['git', 'show', commit + ':runtime/event_leaderboards/scanner.py'], text=True)
    module = types.ModuleType('runtime.event_leaderboards._receipt_baseline')
    module.__package__ = 'runtime.event_leaderboards'
    sys.modules[module.__name__] = module
    exec(compile(source, '<committed-receipt-baseline>', 'exec'), module.__dict__)
    return module.EventLeaderboardScanner


def previous_local_scanner(directory):
    """Load saved pre-edit code, including its header prefetch and proof guard."""
    modules = {}
    for name in ('verification', 'v3_evidence', 'scanner'):
        path = Path(directory) / (name + '-before.py')
        source = path.read_text()
        if name == 'scanner':
            source = source.replace('from .verification import ReceiptPrefetch, BlockPrefetch',
                                    'from ._before_log_verification import ReceiptPrefetch, BlockPrefetch')
        module = types.ModuleType('runtime.event_leaderboards._before_log_' + name)
        module.__package__ = 'runtime.event_leaderboards'
        sys.modules[module.__name__] = module
        exec(compile(source, str(path), 'exec'), module.__dict__)
        modules[name] = module
    modules['scanner'].V3EventEvidence = modules['v3_evidence'].V3EventEvidence
    return modules['scanner'].EventLeaderboardScanner


def run_case(root, name, capture, runtime, scanner_type, options):
    path = root / (name + '.sqlite3')
    if path.exists(): raise ValueError('Refusing to overwrite a test database')
    store = EventLeaderboardStore(str(path))
    prefix = capture['prefix']
    definition = store.register_event(EventDefinition('benchmark', min(r['timestamp'] for r in prefix), None,
        tuple({r['pack_contract'] for r in prefix}), builder=prefix[0]['builder']))
    anchor = capture['headers'][capture['next_block'] - 1]
    store.commit_chunk('benchmark', source_key(definition, definition.contracts[0]), None,
                       capture['next_block'], prefix, block_hash=anchor['hash'])
    provider = OfflineProvider(capture, runtime)
    scanner = scanner_type(store, provider, verification_workers=3, **options)
    before, cpu = time.perf_counter(), time.process_time()
    with ScanDiagnostics(sink=None) as diagnostic:
        result = scanner.sync('benchmark', now=number(capture['headers'][capture['bound']]['timestamp']) + 100)
    elapsed, cpu = time.perf_counter() - before, time.process_time() - cpu
    summary = diagnostic.summary()
    metadata, rows, _ = store.read_checkout_snapshot('benchmark')
    if result['state'] != 'active': raise AssertionError((name, result))
    expected = len(prefix) + len({r['transactionHash'] for r in capture['receipts'].values()})
    if len(rows) != expected: raise AssertionError((name, len(rows), expected))
    return dict(state=result['state'], new_transactions=len(capture['receipts']),
                new_distinct_blocks=len({r['blockNumber'] for r in capture['receipts'].values()}),
                checkpoint=metadata['through_block'], checkout_count=len(rows), draws=sum(r['draw_count'] for r in rows),
                evidence_checksum=digest(rows), local_wall_seconds=round(elapsed, 6), cpu_seconds=round(cpu, 6),
                ranking_checksum=digest(store.read_snapshot('benchmark')[1]),
                logical_api_calls=summary['totals']['attempts'],
                actions={k: v['attempts'] for k, v in summary['actions'].items()},
                pacing_only_seconds_at_4rps=summary['totals']['attempts'] / 4,
                diagnostics=summary, database=str(path))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-directory', required=True)
    parser.add_argument('--public-runtime', required=True)
    parser.add_argument('--capture', type=Path, default=CAPTURE)
    parser.add_argument('--prefix-database', type=Path, default=PREFIX)
    parser.add_argument('--baseline', default=BASELINE)
    parser.add_argument('--previous-local-directory', help='Saved scanner/v3_evidence/verification before this edit')
    args = parser.parse_args()
    root = Path(args.output_directory); root.mkdir(parents=True, exist_ok=False)
    runtime = json.loads(Path(args.public_runtime).read_text())['code']
    real = load_capture(args.capture, args.prefix_database)
    modes = [('before', original_scanner(args.baseline), {}),
             ('combine_only', EventLeaderboardScanner, {'combine_log_queries': True}),
             ('event_and_combine', EventLeaderboardScanner, {'v3_event_proof': True, 'combine_log_queries': True})]
    if args.previous_local_directory:
        modes.insert(1, ('receipt_free_with_headers', previous_local_scanner(args.previous_local_directory),
                         {'v3_event_proof': True, 'combine_log_queries': True}))
    report = dict(mode='OFFLINE_FULL_SCANNER_AND_SQLITE_NOT_LIVE_NETWORK', baseline_commit=args.baseline,
                  paid_api_requests=0, modeled_historical_guard_responses=True,
                  input_capture_sha256=hashlib.sha256(args.capture.read_bytes()).hexdigest(),
                  prefix_database_opened_read_only=True, cases={})
    if args.previous_local_directory:
        report['previous_local_source_sha256'] = {name: hashlib.sha256(
            (Path(args.previous_local_directory) / (name + '-before.py')).read_bytes()).hexdigest()
            for name in ('scanner', 'v3_evidence', 'verification')}
    for count, capture in [(100, real), (1000, scaled_capture(real))]:
        group = dict(input='CAPTURED_REAL_CHECKOUT_RECEIPT_HEADER_DATA' if count == 100 else 'SYNTHETIC_SCALE_10X', variants={})
        for mode, scanner, options in modes:
            result = run_case(root, f'{count}-{mode}', capture, runtime, scanner, options)
            group['variants'][mode] = result
            print(json.dumps({'N': count, 'mode': mode, **{k: result[k] for k in
                  ('logical_api_calls', 'actions', 'local_wall_seconds', 'cpu_seconds')} }), flush=True)
        for field in ('evidence_checksum', 'ranking_checksum', 'checkout_count', 'draws', 'checkpoint'):
            assert len({r[field] for r in group['variants'].values()}) == 1, 'Changed ' + field
        group['identical_evidence_across_all_variants'] = True
        report['cases'][str(count)] = group
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print('Complete: evidence matches; pacing-only numbers exclude network latency/retries and are not an SLA.')


if __name__ == '__main__':
    main()

"""Offline full-scanner A/B: real HTTP adapter + SQLite, synthetic chain responses.

No provider traffic or production writes. A virtual clock models 0.65-second
responses and the unchanged 0.5 requests/second pacing. This is not a live SLA.
"""
import argparse
import ast
import io
import json
from pathlib import Path
import subprocess
from types import MethodType
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from runtime.event_leaderboards.config import EventDefinition
from runtime.event_leaderboards.diagnostics import ScanDiagnostics, phase
from runtime.event_leaderboards.scanner import EtherscanProvider, EventLeaderboardScanner, number, SourceIncomplete
from runtime.event_leaderboards.store import EventLeaderboardStore
from tests.test_event_leaderboard_scanner import checkout, payment, h, VRF_V3_CONTRACT
from tests.test_event_boundary_search import measure_boundary


BASELINE = '169e5f89d85585fd3df4cd05a9b871e513654a76'


def baseline_method(commit=BASELINE):
    source = subprocess.check_output(['git', 'show', commit + ':runtime/event_leaderboards/scanner.py'], text=True)
    tree = ast.parse(source)
    provider = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'EtherscanProvider')
    method = next(node for node in provider.body if isinstance(node, ast.FunctionDef) and node.name == 'first_block_at')
    namespace = {'number': number, 'SourceIncomplete': SourceIncomplete, 'phase': phase}
    if commit != BASELINE:
        boundary_source = subprocess.check_output(['git', 'show', commit + ':runtime/event_leaderboards/boundary.py'], text=True)
        exec(compile(boundary_source, '<committed-search-baseline>', 'exec'), namespace)
    exec(compile(ast.Module(body=[method], type_ignores=[]), '<committed-boundary-baseline>', 'exec'), namespace)
    return namespace['first_block_at']


def full_scan(path, method=None, *, new_transactions=2, response_seconds=.65, timeout_once=False):
    assert 0 <= new_transactions <= 20
    clock, heights, actions, lines = [0.], [2200], [], []
    new_blocks = [2200, 2210] + [b for b in range(2202, 2240, 2) if b != 2210]
    rows = [checkout(block=b, tx=b) for b in [470, 480, *new_blocks[:new_transactions]]]
    injected, warmup = [False], [True]
    for row in rows:
        row['topics'][3] = h(7)
    receipts = {r['transactionHash']: {'transactionHash': r['transactionHash'], 'status': '0x1',
                'blockNumber': r['blockNumber'], 'blockHash': r['blockHash'],
                'logs': [payment(r), r]} for r in rows}
    def sleep(delay):
        clock[0] += delay
    def respond(url, timeout):
        query = parse_qs(urlsplit(url).query)
        action = query['action'][0]
        actions.append(action)
        if timeout_once and not warmup[0] and not injected[0] and action == 'getLogs':
            injected[0] = True
            clock[0] += 15
            raise TimeoutError('synthetic-timeout-without-real-provider-traffic')
        clock[0] += response_seconds
        if action == 'eth_blockNumber':
            value = hex(heights[0])
        elif action == 'getblocknobytime':
            value = str(max(0, int(query['timestamp'][0]) * 4 - 300))
        elif action == 'eth_getBlockByNumber':
            block = int(query['tag'][0], 16)
            assert 0 <= block <= heights[0]
            value = {'number': hex(block), 'hash': h(block), 'timestamp': hex(block // 4)}
        elif action == 'eth_getTransactionReceipt':
            value = receipts[query['txhash'][0]]
        elif action == 'getLogs':
            start, end = int(query['fromBlock'][0]), int(query['toBlock'][0])
            value = [r for r in rows if start <= int(r['blockNumber'], 16) <= end]
        else:
            raise AssertionError('unexpected provider action')
        return io.BytesIO(json.dumps({'status': '1', 'result': value}).encode())
    env = {'EVENT_LEADERBOARD_API_KEY': 'synthetic-key-not-a-real-key',
           'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': '0.5'}
    store = EventLeaderboardStore(str(path), update_interval_seconds=60, stale_after_seconds=180)
    store.register_event(EventDefinition('benchmark', 100, None, (VRF_V3_CONTRACT,), builder=h(7)))
    with patch('time.monotonic', side_effect=lambda: clock[0]), patch('time.sleep', side_effect=sleep), \
         patch('urllib.request.urlopen', side_effect=respond):
        warm = EventLeaderboardScanner(store, EtherscanProvider(env)).sync('benchmark', now=1000)
        assert warm['state'] == 'active', warm
        # Each production round starts a new process/provider; retain the ledger.
        heights[0], clock[0] = 2320, 0.
        warmup[0] = False
        actions.clear()
        provider = EtherscanProvider(env)
        if method is not None:
            provider.first_block_at = MethodType(method, provider)
        scanner = EventLeaderboardScanner(store, provider)
        with ScanDiagnostics(sink=lines.append) as diagnostic:
            result = scanner.sync('benchmark', now=1100)
        assert result['state'] == 'active', result
        elapsed, report = round(clock[0], 3), diagnostic.summary()
        assert sum(stat['attempts'] for stat in report['phases'].values()) == report['totals']['attempts']
        count_before = store.read_checkout_snapshot('benchmark')[1]
        first_snapshot = result['snapshot']['snapshot_id']
        # Repeat the same confirmed range: no duplicate paid checkout or extra tickets.
        again = EventLeaderboardScanner(store, EtherscanProvider(env)).sync('benchmark', now=1100)
        count_after = store.read_checkout_snapshot('benchmark')[1]
        pinned = store.read_checkout_snapshot('benchmark', first_snapshot)[1]
        identities = lambda values: sorted((r['tx_hash'], r['log_index']) for r in values)
        assert identities(count_before) == identities(count_after) == identities(pinned)
        assert len(count_before) == 2 + new_transactions
        assert again['state'] == 'active'
    return {'requests': report['totals']['attempts'], 'modeled_elapsed_seconds': elapsed,
            'checkouts': len(count_before), 'draws': sum(r['draw_count'] for r in count_before),
            'no_duplicates_after_replay': True, 'pinned_snapshot_unchanged': True,
            'diagnostics': report, 'diagnostic_records': len(lines)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database-root', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    root = Path(args.database_root)
    root.mkdir(parents=True, exist_ok=False)
    baseline = baseline_method()
    report = {'evidence': 'OFFLINE_SYNTHETIC_NOT_HISTORICAL_REPLAY', 'baseline_commit': BASELINE,
              'response_seconds': .65, 'requests_per_second': .5,
              'boundary_only': {'before': measure_boundary(baseline),
                                'first_optimization': measure_boundary(baseline_method('b101c54')),
                                'after': measure_boundary(),
                                'after_with_verified_anchor': measure_boundary(cached_heights=(2800,))},
              'full_scan': {'before': full_scan(root / 'before.sqlite3', baseline),
                            'first_optimization': full_scan(root / 'first.sqlite3', baseline_method('b101c54')),
                            'after': full_scan(root / 'after.sqlite3')}}
    assert report['full_scan']['before']['checkouts'] == report['full_scan']['after']['checkouts']
    assert report['full_scan']['before']['draws'] == report['full_scan']['after']['draws']
    scenarios = []
    for response_seconds in (.65, 3.):
        for transactions in (0, 2, 5, 10, 20):
            case = full_scan(root / f'load-{response_seconds}-{transactions}.sqlite3',
                             new_transactions=transactions, response_seconds=response_seconds)
            scenarios.append({'new_transactions': transactions, 'response_seconds': response_seconds,
                              'requests': case['requests'], 'modeled_elapsed_seconds': case['modeled_elapsed_seconds']})
    timeout = full_scan(root / 'one-timeout.sqlite3', timeout_once=True)
    report['load_scenarios'] = scenarios
    report['one_timeout'] = {k: timeout[k] for k in ('requests', 'modeled_elapsed_seconds', 'diagnostics')}
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k: {version: {field: value for field, value in result.items() if field != 'diagnostics'}
                         for version, result in report[k].items()} for k in ('boundary_only', 'full_scan')}, indent=2))
    print(json.dumps({'load_scenarios': scenarios, 'one_timeout_seconds': timeout['modeled_elapsed_seconds']}, indent=2))


if __name__ == '__main__':
    main()

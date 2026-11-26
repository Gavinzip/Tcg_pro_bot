"""Compare successful provider response decoding on identical offline traces.

This measures CPU/response-processing cost, not production network/storage time.
It never calls a chain API and never changes the checked-out source version.
"""
import io
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runtime.event_leaderboards.diagnostics import ScanDiagnostics


def reader(ref):
    source = subprocess.check_output(['git', 'show', ref + ':runtime/event_leaderboards/scanner.py'], cwd=ROOT, text=True)
    scope = {'__name__': 'runtime.event_leaderboards._offline_cost_' + ref,
             '__package__': 'runtime.event_leaderboards'}
    exec(compile(source, '<git-response-reader-in-memory>', 'exec'), scope)
    return scope['EtherscanProvider']


def trace(logs):
    def payload(value, proxy=False):
        return json.dumps({'jsonrpc': '2.0', 'id': 1, 'result': value} if proxy else
                          {'status': '1', 'message': 'OK', 'result': value}).encode()
    receipt = {'status': '0x1', 'logs': [{'data': '0x' + '0' * 64, 'topics': ['0x' + 'a' * 64]} for _ in range(logs)]}
    return [('proxy', 'eth_blockNumber', {}, payload('0x817c000', True))] + \
        [('proxy', 'eth_getBlockByNumber', {'tag': '0x817c000'}, payload({'number': '0x817c000', 'timestamp': '0x6ab00000'}, True))] * 62 + \
        [('proxy', 'eth_getTransactionReceipt', {}, payload(receipt, True))] * 61 + \
        [('logs', 'getLogs', {'fromBlock': 135000000, 'toBlock': 135000200}, payload([]))] * 2 + \
        [('block', 'getblocknobytime', {'timestamp': 1795664180, 'closest': 'before'}, payload('135775822'))]


def measure(factory, cases):
    samples, checksum = [], None
    provider = factory({'EVENT_LEADERBOARD_API_KEY': 'offline-fixture-no-real-key'})
    def open_response(url, timeout):
        return io.BytesIO(current[0])
    current = [b'']
    for _ in range(7):
        started = time.perf_counter()
        with patch('urllib.request.urlopen', side_effect=open_response), ScanDiagnostics(sink=None):
            for _ in range(30):
                results = []
                for module, action, params, body in cases:
                    current[0] = body
                    results.append(provider._read_response(module, action, params))
                checksum = json.dumps(results, sort_keys=True)
        samples.append((time.perf_counter() - started) * 1000 / 30)
    provider.close()
    return {'millisecondsPer127Responses': round(statistics.median(samples), 3)}, checksum


if __name__ == '__main__':
    report = {'externalApiCalls': 0, 'scope': 'offline response processing only', 'cases': []}
    old, new = reader('67d9283'), reader('f8c4990')
    for size in (10, 100):
        cases = trace(size)
        before, a = measure(old, cases)
        after, b = measure(new, cases)
        assert a == b
        report['cases'].append({'receiptLogCount': size, 'responses': len(cases), 'before': before, 'after': after,
                                'differenceMs': round(after['millisecondsPer127Responses'] - before['millisecondsPer127Responses'], 3),
                                'identicalResults': True})
    print(json.dumps(report, indent=2))

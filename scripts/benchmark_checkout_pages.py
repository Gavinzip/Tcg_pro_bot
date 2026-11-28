"""Offline real HTTP/SQLite A/B. No production, wallet or chain traffic.

Run prepare once, then baseline and fixed in separate processes. The equal
sample is bounded; fixed additionally downloads both complete pinned snapshots.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import threading
import time
import types
import urllib.parse
import urllib.request
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.store import EventLeaderboardStore
from runtime.event_leaderboards.http import EventLeaderboardHTTP
from runtime.event_leaderboards import store as current_store

BASE = 'be5f4b742090b5737f0165b9552808606f6ea74c'
CONTRACT, BUILDER = '0x' + 'a' * 40, '0x' + 'b' * 64
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('mode', choices=['prepare', 'baseline', 'fixed'])
parser.add_argument('--rows', type=int, default=100000)
parser.add_argument('--players', type=int, default=11150)
parser.add_argument('--sample-pages', type=int, default=6)
parser.add_argument('--out', default='.scratch/payment-sync-20261129')
parser.add_argument('--serve', action='store_true', help='Serve the synthetic fixture until stdin receives a line')
args = parser.parse_args()
out = (ROOT / args.out).resolve()
if not out.is_relative_to(ROOT / '.scratch'):
    raise ValueError('Benchmark data must remain in this checkout .scratch directory')
os.umask(0o077)
out.mkdir(parents=True, exist_ok=True)
database = out / 'fixture.sqlite3'

if args.mode == 'prepare':
    if database.exists():
        raise ValueError('Preserving an existing benchmark database; choose a fresh --out')
    store = EventLeaderboardStore(str(database))
    definition = store.register_event(EventDefinition('paging', 100, None, (CONTRACT,),
        builder=BUILDER, contract_schemas={CONTRACT: 'v3'}))
    rows = [dict(wallet='0x' + f'{i % args.players + 1:040x}', tx_hash='0x' + f'{i + 1:064x}',
        log_index=0, pack_contract=CONTRACT, pack_id='0x' + 'c' * 64, builder=BUILDER,
        draw_count=1, paid_amount_raw='28000000000000000000', block_number=10,
        block_hash='0x' + 'd' * 64, timestamp=110) for i in range(args.rows)]
    store.commit_chunk('paging', source_key(definition, CONTRACT), None, 20, rows, block_hash='0xanchor')
    snapshots = [store.publish_snapshot('paging', 19, 120), store.publish_snapshot('paging', 19, 120)]
    (out / 'fixture.json').write_text(json.dumps({'rows':args.rows,'players':args.players,'snapshots':snapshots}))
    print(json.dumps({'prepared':args.rows,'players':args.players,'databaseBytes':database.stat().st_size}))
    sys.exit(0)

module = current_store
if args.mode == 'baseline':
    def old_module(name):
        m = types.ModuleType('runtime.event_leaderboards._benchmark_' + name)
        m.__package__ = 'runtime.event_leaderboards'
        sys.modules[m.__name__] = m
        code = subprocess.check_output(['git','show',f'{BASE}:runtime/event_leaderboards/{name}.py'],cwd=ROOT)
        exec(compile(code, 'baseline/' + name + '.py', 'exec'), m.__dict__)
        return m
    old_cache, module = old_module('checkout_pages'), old_module('store')
    module.CheckoutPages = old_cache.CheckoutPages

f = json.loads((out / 'fixture.json').read_text())
store = module.EventLeaderboardStore(str(database))
if args.serve:
    if args.mode != 'fixed':
        raise ValueError('The integration server must use the current source')
    store.publish_snapshot('paging', 19, 120)
http = EventLeaderboardHTTP(store)
hydrated, snapshot_reads = 0, 0
snapshot_ms, controller_ms, write_ms = [], [], []
original_payloads, original_snapshot, original_dispatch = module.checkout_payloads, store.read_snapshot, http.dispatch

def counted(*a, **kw):
    global hydrated
    for row in original_payloads(*a, **kw):
        hydrated += 1
        yield row

def read_snapshot(*a, **kw):
    global snapshot_reads
    began = time.perf_counter()
    result = original_snapshot(*a, **kw)
    snapshot_ms.append((time.perf_counter() - began) * 1000)
    if kw.get('include_rows', True): snapshot_reads += 1
    return result

def dispatch(*a, **kw):
    began = time.perf_counter()
    result = original_dispatch(*a, **kw)
    controller_ms.append((time.perf_counter() - began) * 1000)
    return result

module.checkout_payloads = counted
store.read_snapshot, http.dispatch = read_snapshot, dispatch
import profile_api

class Handler(profile_api.ProfileApiHandler):
    def log_message(self, *a): pass
    def _send_json(self, status, payload):
        began = time.perf_counter()
        super()._send_json(status, payload)
        write_ms.append((time.perf_counter() - began) * 1000)

def digest_rows(rows, hasher):
    for row in rows:
        hasher.update(json.dumps(row, sort_keys=True, separators=(',', ':')).encode())

with patch.object(profile_api, '_EVENT_CONTROLLER', http), \
     patch.object(profile_api, 'API_TOKEN', 'synthetic-local-internal'), \
     patch.object(profile_api, 'LEADERBOARD_CLIENT_TOKENS', {'site':'synthetic-local-read'}), \
     patch.dict(os.environ, {'EVENT_LEADERBOARD_API_ENABLED':'1'}):
    server = profile_api.ProfileApiServer(('127.0.0.1',0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    cursors = [None, None]
    hashes = [hashlib.sha256(), hashlib.sha256()]
    totals, amounts, times = [0,0], [0,0], []
    counts, first_digest = [0,0], None
    requests = 0
    try:
        if args.serve:
            print(json.dumps({'port': server.server_address[1]}), flush=True)
            sys.stdin.readline()
            (out / 'integration-source-report.json').write_text(json.dumps({
                'hydratedRows': hydrated, 'identityReads': snapshot_reads,
                'httpRequests': len(controller_ms), 'cacheStats': store._checkout_pages.stats,
                'chainApiCalls': 0}, indent=2))
            sys.exit(0)
        def page(i):
            global requests
            query = {'snapshot_id':f['snapshots'][i]['snapshot_id'],'limit':'1000','builder':BUILDER}
            if cursors[i]: query['cursor'] = cursors[i]
            url = f'http://127.0.0.1:{server.server_address[1]}/v1/leaderboards/paging/checkouts?' + urllib.parse.urlencode(query)
            began = time.perf_counter()
            request = urllib.request.Request(url, headers={'Authorization':'Bearer synthetic-local-read'})
            with urllib.request.urlopen(request, timeout=30) as r:
                assert r.status == 200
                result = json.load(r)
            times.append((time.perf_counter() - began) * 1000)
            assert result['snapshot_id'] == f['snapshots'][i]['snapshot_id']
            assert result['total_checkouts'] == f['rows']
            digest_rows(result['rows'], hashes[i])
            totals[i] += sum(r['draw_count'] for r in result['rows'])
            amounts[i] += sum(int(r['paid_amount_raw']) for r in result['rows'])
            counts[i] += len(result['rows'])
            cursors[i] = result['next_cursor']
            requests += 1

        began = time.perf_counter()
        for _ in range(args.sample_pages):
            for i in range(2): page(i)
        sample = {'httpRequests':requests,'elapsedMs':round((time.perf_counter()-began)*1000,2),
            'hydratedRows':hydrated,'identityReads':snapshot_reads,
            'rowDigests':[h.hexdigest() for h in hashes],'records':counts.copy(),
            'drawTotals':totals.copy(),'paidRawTotals':[str(n) for n in amounts]}
        if args.mode == 'fixed':
            while any(cursors):
                for i in range(2):
                    if cursors[i]: page(i)
            assert counts == [f['rows'],f['rows']]
            assert totals == [f['rows'],f['rows']]
            assert amounts == [f['rows']*28*10**18]*2
            assert hashes[0].digest() == hashes[1].digest()
            assert hydrated == f['rows'] * 2
        elapsed = round((time.perf_counter()-began)*1000,2)
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024**2 if sys.platform=='darwin' else 1024)
        ordered = sorted(times)
        report = {'mode':args.mode,'rows':f['rows'],'players':f['players'],'equalSample':sample,
            'totalHttpRequests':requests,'elapsedMs':elapsed,'hydratedRows':hydrated,'identityReads':snapshot_reads,
            'httpMedianMs':round(ordered[len(ordered)//2],2),'httpMaxMs':round(max(times),2),
            'controllerTotalMs':round(sum(controller_ms),2),'snapshotReadTotalMs':round(sum(snapshot_ms),2),
            'jsonWriteTotalMs':round(sum(write_ms),2),'peakRssMiB':round(peak,2),
            'finalRowDigests':[h.hexdigest() for h in hashes],'records':counts,'drawTotals':totals,
            'paidRawTotals':[str(n) for n in amounts],'chainApiCalls':0,
            'cacheDiskBytes':getattr(store._checkout_pages,'_disk_bytes',0),'cacheStats':getattr(store._checkout_pages,'stats',None)}
        (out / (args.mode + '-report.json')).write_text(json.dumps(report,indent=2))
        print(json.dumps(report))
    finally:
        server.shutdown(); server.server_close(); thread.join()
        close = getattr(store._checkout_pages,'close',None)
        if close: close()

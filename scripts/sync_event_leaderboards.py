#!/usr/bin/env python3
"""Run one bounded, read-only-on-chain activity indexer synchronization."""
from __future__ import annotations

from dataclasses import replace
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

# Support the normal `python scripts/sync_event_leaderboards.py` entry point.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from runtime.event_leaderboards.config import DEFAULT_CONFIG_PATH, load_event_definitions
from runtime.event_leaderboards.scanner import EtherscanProvider, EventLeaderboardScanner, SourceIncomplete, address, hash32, number
from runtime.event_leaderboards.store import EventLeaderboardStore
from runtime.wallet_migration import wallet_migration_map_path
from runtime.wallet_usernames import load_wallet_username_map
from runtime.event_leaderboards.diagnostics import ScanDiagnostics, phase


MIGRATION_HELPER = "0x2e737d552b3c601ada4fcd167bfbd8d4e1043b2c"
MIGRATION_TOPIC = "0xa8842895c03659cf75d4e5e2202ace2ef1981a77551fc02961bc661af0950830"


def verified_file_aliases(path=None):
    """Accept only provenance-bearing output from the existing helper log indexer.

    Missing optional map means no known migrations. Nonempty malformed maps and
    legacy heuristic/flat maps are errors, rather than silently merging wallets.
    The application owns this protected local file; its hash freezes provenance.
    """
    resolved = Path(path or wallet_migration_map_path())
    if not resolved.exists():
        return []
    try:
        raw = resolved.read_bytes()
        payload = json.loads(raw)
        if (not isinstance(payload, dict) or payload.get("source") != "bsc_logs:helper_migration_event"
                or payload.get("helper_contract") != MIGRATION_HELPER
                or payload.get("helper_event_topic") != MIGRATION_TOPIC
                or not isinstance(payload.get("mappings"), list)):
            raise SourceIncomplete("unverified_migration_map")
        maximum = number(payload.get("last_scanned_block"))
        fingerprint = hashlib.sha256(raw).hexdigest()
        aliases = []
        seen = {}
        for row in payload["mappings"]:
            if not isinstance(row, dict) or row.get("source") not in {"helper_event_only", "helper_event_with_sbt13_delta_same_tx"}:
                raise SourceIncomplete("unverified_migration_row")
            old, new = address(row.get("old")), address(row.get("new"))
            block, tx = number(row.get("block")), hash32(row.get("tx"))
            if old == new or not 0 < block <= maximum or (old in seen and seen[old] != new):
                raise SourceIncomplete("invalid_migration_pair")
            seen[old] = new
            aliases.append(dict(old=old, new=new, verified=True,
                evidence=f"helper:{MIGRATION_HELPER}:tx:{tx}:block:{block}:map_sha256:{fingerprint}"))
        return aliases
    except (OSError, ValueError, TypeError):
        raise SourceIncomplete("invalid_migration_map") from None


def _main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", help="Synchronize this configured event only")
    parser.add_argument("--config", help="Activity definitions JSON path")
    parser.add_argument("--database", help="Persistent activity ledger SQLite path")
    parser.add_argument("--chain-env", help="Read a local secrets JSON into process environment only")
    parser.add_argument("--confirmations", type=int, default=int(os.getenv("EVENT_LEADERBOARD_CONFIRMATIONS", "64")))
    parser.add_argument("--chunk-blocks", type=int, default=int(os.getenv("EVENT_LEADERBOARD_CHUNK_BLOCKS", "2000")))
    parser.add_argument("--max-chunks", type=int, default=int(os.getenv("EVENT_LEADERBOARD_MAX_CHUNKS", "100")))
    parser.add_argument("--replay-seconds", type=int, default=int(os.getenv("EVENT_LEADERBOARD_REPLAY_SECONDS", "300")),
                        help="Recheck this many seconds before each persisted confirmed cursor (default: 300)")
    parser.add_argument("--verification-workers", type=int,
                        default=int(os.getenv("EVENT_LEADERBOARD_VERIFICATION_WORKERS", "1")),
                        help="Bounded parallel receipt readers; shared request pacing still applies (default: 1)")
    args = parser.parse_args(argv)
    store = None
    selected = []
    provider = None
    try:
        if args.chain_env:
            with open(args.chain_env, encoding="utf-8") as stream:
                secrets = json.load(stream)
            if not isinstance(secrets, dict) or any(not isinstance(v, str) for v in secrets.values()):
                raise ValueError("invalid secret environment")
            os.environ.update(secrets)
        with phase('startup'):
            store = EventLeaderboardStore(args.database)
            explicit_config = args.config or os.getenv("EVENT_LEADERBOARD_CONFIG_PATH")
            existing = store.list_events()
            definitions = (existing if existing and not explicit_config and not Path(DEFAULT_CONFIG_PATH).exists()
                           else load_event_definitions(args.config))
            extra = os.getenv("EVENT_LEADERBOARD_CONTRACT_SCHEMAS")
            extra_schemas = json.loads(extra) if extra else {}
            events = [store.register_event(replace(row, checkout_schema_version=2, contract_schemas={**row.contract_schemas,
                **{k.lower(): v for k, v in extra_schemas.items() if k.lower() in row.contracts}})) for row in definitions]
            selected = [row for row in events if args.event is None or row.event_id == args.event]
            if not selected:
                raise ValueError("event not configured")
            try:
                aliases = verified_file_aliases()
            except SourceIncomplete as error:
                for event in selected:
                    store.record_failure(event.event_id, str(error), progress={"state": "stale", "error": str(error)})
                raise
            names = load_wallet_username_map()
            for event in selected:
                store.set_aliases(event.event_id, aliases)
            provider = EtherscanProvider()
            scanner = EventLeaderboardScanner(store, provider, confirmations=args.confirmations,
                chunk_blocks=args.chunk_blocks, max_chunks=args.max_chunks, replay_seconds=args.replay_seconds,
                verification_workers=args.verification_workers)
        results = [scanner.sync(event.event_id, usernames=names) for event in selected]
        print(json.dumps({"events": results, "identity_sources": {"username_entries": len(names),
            "verified_migration_pairs": len(aliases)}}, sort_keys=True))
        return int(any(row["state"] == "stale" for row in results))
    except Exception as error:
        # Never print request URLs, keys, environment values or raw provider errors.
        code = str(error) if isinstance(error, SourceIncomplete) else type(error).__name__
        if store is not None:
            affected = selected or [row for row in store.list_events() if args.event is None or row.event_id == args.event]
            for event in affected:
                store.record_failure(event.event_id, code, progress={"state": "stale", "error": code})
        print(json.dumps({"error": code}), file=sys.stderr)
        return 1
    finally:
        if provider is not None:
            provider.close()


def main(argv=None):
    with ScanDiagnostics() as diagnostic:
        code = _main(argv)
        diagnostic.outcome = 'error' if code else 'ok'
        return code


if __name__ == "__main__":
    raise SystemExit(main())

"""Durable paid-checkout ledger and immutable published activity snapshots."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

from .config import DEFAULT_DB_PATH, EventDefinition, builder_code, iso_time, timestamp, wallet, source_key
from .aggregation import build_rankings
from .evidence_history import initialize_evidence_history, insertion_watermark, checkout_payloads


class LedgerConflict(ValueError):
    """A checkpoint race, inconsistent duplicate, migration, or reorg needs inspection."""


class BuilderFilterUnavailable(ValueError):
    """A historical snapshot has no complete builder evidence."""

class CheckoutEvidenceUnavailable(ValueError):
    """The selected snapshot predates frozen per-checkout evidence."""


class EventLeaderboardStore:
    def __init__(self, path: str | None = None, *, update_interval_seconds=None, stale_after_seconds=None):
        self.path = str(path or os.getenv("EVENT_LEADERBOARD_DB_PATH") or DEFAULT_DB_PATH)
        self.update_interval_seconds = max(60, int(update_interval_seconds if update_interval_seconds is not None
            else os.getenv("EVENT_LEADERBOARD_SYNC_INTERVAL_SECONDS", "600")))
        default_stale = max(2 * self.update_interval_seconds, self.update_interval_seconds + 120)
        self.stale_after_seconds = int(stale_after_seconds if stale_after_seconds is not None
            else os.getenv("EVENT_LEADERBOARD_STALE_AFTER_SECONDS", str(default_stale)))
        if self.stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS event_definitions (
                    event_id TEXT NOT NULL, revision INTEGER NOT NULL, config_hash TEXT NOT NULL,
                    definition TEXT NOT NULL, PRIMARY KEY(event_id,revision));
                CREATE TABLE IF NOT EXISTS event_current (event_id TEXT PRIMARY KEY, revision INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS checkpoints (
                    event_id TEXT, revision INTEGER, source_key TEXT, next_block INTEGER NOT NULL, block_hash TEXT,
                    PRIMARY KEY(event_id,revision,source_key));
                CREATE TABLE IF NOT EXISTS ledger (
                    event_id TEXT, revision INTEGER, chain_id INTEGER, tx_hash TEXT, log_index INTEGER,
                    payload TEXT NOT NULL, PRIMARY KEY(event_id,revision,chain_id,tx_hash,log_index));
                CREATE TABLE IF NOT EXISTS aliases (
                    event_id TEXT, revision INTEGER, old_wallet TEXT, new_wallet TEXT, evidence TEXT,
                    PRIMARY KEY(event_id,revision,old_wallet));
                CREATE TABLE IF NOT EXISTS event_state (
                    event_id TEXT, revision INTEGER, last_error TEXT, progress TEXT,
                    PRIMARY KEY(event_id,revision));
                CREATE TABLE IF NOT EXISTS snapshots (
                    snapshot_id TEXT PRIMARY KEY, event_id TEXT, revision INTEGER,
                    created_seq INTEGER UNIQUE, metadata TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS snapshot_event ON snapshots(event_id,created_seq);
                CREATE TABLE IF NOT EXISTS snapshot_rows (
                    snapshot_id TEXT, wallet TEXT, payload TEXT NOT NULL,
                    PRIMARY KEY(snapshot_id,wallet));
                CREATE TABLE IF NOT EXISTS snapshot_builder_rows (
                    snapshot_id TEXT, builder TEXT, wallet TEXT, payload TEXT NOT NULL,
                    PRIMARY KEY(snapshot_id,builder,wallet));
                CREATE TABLE IF NOT EXISTS snapshot_checkout_rows (
                    snapshot_id TEXT, tx_hash TEXT, log_index INTEGER, payload TEXT NOT NULL,
                    PRIMARY KEY(snapshot_id,tx_hash,log_index));
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(checkpoints)")}
            if "block_hash" not in columns:
                db.execute("ALTER TABLE checkpoints ADD COLUMN block_hash TEXT")

        with self._connection(write=True) as db:
            initialize_evidence_history(db)

    @contextmanager
    def _connection(self, write=False):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("PRAGMA synchronous=FULL")
        try:
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _event(db, event_id):
        row = db.execute("SELECT d.* FROM event_current c JOIN event_definitions d "
                         "ON c.event_id=d.event_id AND c.revision=d.revision WHERE c.event_id=?", (event_id,)).fetchone()
        if row is None:
            raise KeyError(event_id)
        return replace(EventDefinition.from_dict(json.loads(row["definition"]), stored=True), revision=row["revision"])

    def register_event(self, definition: EventDefinition) -> EventDefinition:
        with self._connection(write=True) as db:
            try:
                current = self._event(db, definition.event_id)
            except KeyError:
                current = None
            if current and current.config_hash == definition.config_hash:
                return current
            revision = current.revision + 1 if current else 1
            definition = replace(definition, revision=revision)
            db.execute("INSERT INTO event_definitions VALUES (?,?,?,?)", (definition.event_id, revision,
                       definition.config_hash, json.dumps(definition.payload(), sort_keys=True)))
            db.execute("INSERT INTO event_current VALUES (?,?) ON CONFLICT(event_id) DO UPDATE SET revision=excluded.revision",
                       (definition.event_id, revision))
            return definition

    def get_event(self, event_id):
        with self._connection() as db:
            return self._event(db, event_id)

    def list_events(self):
        with self._connection() as db:
            return [self._event(db, row[0]) for row in db.execute("SELECT event_id FROM event_current ORDER BY event_id")]

    def get_checkpoint(self, event_id, source_key):
        with self._connection() as db:
            definition = self._event(db, event_id)
            row = db.execute("SELECT next_block,block_hash FROM checkpoints WHERE event_id=? AND revision=? AND source_key=?",
                             (event_id, definition.revision, source_key)).fetchone()
            return {"next_block": row[0], "block_hash": row[1], "revision": definition.revision} if row else None

    @staticmethod
    def _put_aliases(db, definition, aliases):
        for entry in aliases:
            old, new = wallet(entry["old"]), wallet(entry["new"])
            if entry.get("verified") is not True or not entry.get("evidence") or old == new:
                raise ValueError("aliases require verified evidence and distinct wallets")
            existing = db.execute("SELECT new_wallet FROM aliases WHERE event_id=? AND revision=? AND old_wallet=?",
                                  (definition.event_id, definition.revision, old)).fetchone()
            if existing and existing[0] != new:
                raise LedgerConflict("conflicting verified migration")
            db.execute("INSERT OR IGNORE INTO aliases VALUES (?,?,?,?,?)", (definition.event_id, definition.revision,
                       old, new, str(entry["evidence"])))
        mapping = dict(db.execute("SELECT old_wallet,new_wallet FROM aliases WHERE event_id=? AND revision=?",
                                 (definition.event_id, definition.revision)))
        for address in mapping:
            seen = set()
            while address in mapping:
                if address in seen:
                    raise LedgerConflict("migration cycle")
                seen.add(address)
                address = mapping[address]

    def set_aliases(self, event_id, aliases):
        with self._connection(write=True) as db:
            self._put_aliases(db, self._event(db, event_id), aliases)

    @staticmethod
    def _put_checkouts(db, definition, checkouts, lower_block, upper_block, *, contract=None):
        inserted = 0
        for original in checkouts:
            row = dict(original)
            for key in ("draw_count", "block_number", "log_index", "timestamp"):
                if isinstance(row.get(key), bool) or not isinstance(row.get(key), int):
                    raise ValueError("invalid checkout integer: " + key)
            row["wallet"] = wallet(row["wallet"])
            row["pack_contract"] = wallet(row["pack_contract"])
            row["chain_id"] = int(row.get("chain_id", definition.chain_id))
            raw = str(row["paid_amount_raw"])
            if not raw.isdigit() or int(raw) <= 0 or row["draw_count"] <= 0 or row["log_index"] < 0:
                raise ValueError("only positive actually-paid successful checkouts count")
            row["paid_amount_raw"] = str(int(raw))
            row["tx_hash"] = str(row["tx_hash"]).lower()
            row["block_hash"] = str(row["block_hash"]).lower()
            row["pack_id"] = str(row["pack_id"])
            if definition.checkout_schema_version >= 2:
                if definition.contract_schemas.get(row["pack_contract"]) == "v3":
                    row["builder"] = builder_code(row.get("builder"))
                elif row.get("builder") is not None:
                    raise ValueError("legacy checkout cannot claim a builder")
                else:
                    row["builder"] = None
            if (not row["tx_hash"] or not row["block_hash"] or row["chain_id"] != definition.chain_id
                    or row["pack_contract"] not in definition.contracts
                    or (contract is not None and row["pack_contract"] != contract)
                    or (definition.builder is not None and row.get("builder") != definition.builder)
                    or row["timestamp"] < definition.start_ts
                    or (definition.end_ts is not None and row["timestamp"] >= definition.end_ts)
                    or row["block_number"] < 0 or row["block_number"] >= upper_block
                    or (lower_block is not None and row["block_number"] < lower_block)):
                raise ValueError("checkout outside validated event/chunk scope")
            payload = json.dumps(row, sort_keys=True, separators=(",", ":"))
            identity = (definition.event_id, definition.revision, row["chain_id"], row["tx_hash"], row["log_index"])
            existing = db.execute("SELECT payload FROM ledger WHERE event_id=? AND revision=? AND chain_id=? AND tx_hash=? AND log_index=?", identity).fetchone()
            if existing and existing[0] != payload:
                raise LedgerConflict("checkout identity changed (possible reorg)")
            inserted += db.execute("INSERT OR IGNORE INTO ledger VALUES (?,?,?,?,?,?)", (*identity, payload)).rowcount
        return inserted

    def commit_chunk(self, event_id, source_key, expected_next_block, next_block, checkouts, aliases=(), block_hash=None, expected_revision=None):
        """Only complete verified chunks enter the ledger; all validation rolls back checkpoint too."""
        if not source_key or not isinstance(next_block, int) or next_block < 0:
            raise ValueError("invalid chunk bounds")
        if expected_next_block is not None and next_block <= expected_next_block:
            raise ValueError("chunk must advance")
        with self._connection(write=True) as db:
            definition = self._event(db, event_id)
            if expected_revision is not None and definition.revision != expected_revision:
                raise LedgerConflict("event definition changed during scan")
            scope = (event_id, definition.revision, source_key)
            previous = db.execute("SELECT next_block FROM checkpoints WHERE event_id=? AND revision=? AND source_key=?", scope).fetchone()
            if (previous[0] if previous else None) != expected_next_block:
                raise LedgerConflict("checkpoint changed; rescan current checkpoint")
            self._put_checkouts(db, definition, checkouts, expected_next_block, next_block)
            self._put_aliases(db, definition, aliases)
            db.execute("INSERT INTO checkpoints VALUES (?,?,?,?,?) ON CONFLICT(event_id,revision,source_key) "
                       "DO UPDATE SET next_block=excluded.next_block,block_hash=excluded.block_hash", (*scope, next_block, block_hash))

    def checkouts_in_range(self, event_id, contract, start_block, end_block, *, expected_revision):
        with self._connection() as db:
            definition = self._event(db, event_id)
            if definition.revision != expected_revision or contract not in definition.contracts:
                raise LedgerConflict("event definition changed during overlap scan")
            return self._range_checkouts(db, definition, contract, start_block, end_block)

    @staticmethod
    def _range_checkouts(db, definition, contract, start_block, end_block):
        return [json.loads(r[0]) for r in db.execute(
            "SELECT payload FROM ledger WHERE event_id=? AND revision=? "
            "AND json_extract(payload,'$.pack_contract')=? "
            "AND json_extract(payload,'$.block_number') BETWEEN ? AND ?",
            (definition.event_id, definition.revision, contract, start_block, end_block))]

    def reconcile_confirmed_range(self, event_id, contract, checkpoint, start_block, end_block, checkouts):
        """Append verified late records atomically without rewinding/advancing a cursor.

        A successful response that now omits already-known records is an error,
        not a request to delete evidence. Conflicting identities roll back too.
        """
        if (type(start_block) is not int or type(end_block) is not int
                or not 0 <= start_block <= end_block < checkpoint['next_block']):
            raise ValueError("invalid confirmed overlap bounds")
        with self._connection(write=True) as db:
            definition = self._event(db, event_id)
            if definition.revision != checkpoint['revision']:
                raise LedgerConflict("event definition changed during overlap scan")
            current = db.execute("SELECT next_block,block_hash FROM checkpoints "
                "WHERE event_id=? AND revision=? AND source_key=?",
                (event_id, definition.revision, source_key(definition, contract))).fetchone()
            if (current is None or current[0] != checkpoint['next_block']
                    or current[1] != checkpoint['block_hash']):
                raise LedgerConflict("checkpoint changed during overlap scan")
            known = self._range_checkouts(db, definition, contract, start_block, end_block)
            key = lambda r: (r['chain_id'], r['tx_hash'].lower(), r['log_index'])
            if not {key(r) for r in known}.issubset({key(r) for r in checkouts}):
                raise LedgerConflict("overlap response omitted known paid checkout")
            return self._put_checkouts(db, definition, checkouts, start_block, end_block + 1, contract=contract)

    def set_progress(self, event_id, progress):
        with self._connection(write=True) as db:
            definition = self._event(db, event_id)
            db.execute("INSERT INTO event_state VALUES (?,?,NULL,?) ON CONFLICT(event_id,revision) "
                       "DO UPDATE SET progress=excluded.progress", (event_id, definition.revision, json.dumps(progress)))

    def record_failure(self, event_id, error, progress=None):
        with self._connection(write=True) as db:
            definition = self._event(db, event_id)
            db.execute("INSERT INTO event_state VALUES (?,?,?,?) ON CONFLICT(event_id,revision) "
                       "DO UPDATE SET last_error=excluded.last_error,progress=COALESCE(excluded.progress,event_state.progress)",
                       (event_id, definition.revision, str(error), json.dumps(progress) if progress is not None else None))

    @staticmethod
    def _amount(raw, decimals):
        if not decimals:
            return str(raw)
        digits = str(raw).zfill(decimals + 1)
        return digits[:-decimals] + "." + digits[-decimals:]

    def publish_snapshot(self, event_id, through_block, through_at, finalized=False, usernames=None, expected_revision=None):
        """Publish only when every audited contract checkpoint covers this confirmed bound."""
        through_at = timestamp(through_at)
        if not isinstance(through_block, int) or through_block < 0:
            raise ValueError("invalid through_block")
        with self._connection(write=True) as db:
            definition = self._event(db, event_id)
            if expected_revision is not None and definition.revision != expected_revision:
                raise LedgerConflict("event definition changed before publication")
            for contract in definition.contracts:
                checkpoint = db.execute("SELECT next_block,block_hash FROM checkpoints "
                    "WHERE event_id=? AND revision=? AND source_key=?",
                    (event_id, definition.revision, source_key(definition, contract))).fetchone()
                if checkpoint is None or checkpoint[0] <= through_block or not checkpoint[1]:
                    raise LedgerConflict("publication requires complete anchored checkpoints for every contract")
            if finalized and (definition.end_ts is None or through_at < definition.end_ts):
                raise ValueError("cannot finalize before the event end is confirmed")
            aliases = dict(db.execute("SELECT old_wallet,new_wallet FROM aliases WHERE event_id=? AND revision=?",
                                      (event_id, definition.revision)))
            checkouts = [json.loads(record[0]) for record in db.execute(
                "SELECT payload FROM ledger WHERE event_id=? AND revision=?", (event_id, definition.revision))]
            rankings = build_rankings(checkouts,
                definition=definition, aliases=aliases, usernames=usernames, through_block=through_block,
                through_at=through_at, format_amount=self._amount)
            generated = int(time.time())
            snapshot_id = uuid.uuid4().hex
            metadata = {"event_id": event_id, "revision": definition.revision, "config_hash": definition.config_hash,
                        "start_at": iso_time(definition.start_ts), "end_at": iso_time(definition.end_ts) if definition.end_ts is not None else None,
                        "snapshot_id": snapshot_id, "generated_at": iso_time(generated),
                        "data_through_at": iso_time(through_at), "through_block": through_block,
                        "total_participants": len(rankings[None]), "status": "final" if finalized else ("settling" if definition.end_ts is not None and generated >= definition.end_ts else "live"),
                        "counting_rule": "confirmed_paid_checkout", "contracts": list(definition.contracts),
                        "chain_id": definition.chain_id, "token_decimals": definition.token_decimals,
                        "ranking_rule": "competition_draw_count_desc_wallet_asc",
                        "builder_filter_supported": definition.checkout_schema_version >= 2, "builder": None,
                        "checkout_evidence_supported": True}
            if definition.end_ts is None:
                # Pin insertion order as well as block/time and identities.
                # A later repair of an older block must not alter this evidence.
                metadata["checkout_evidence_storage"] = "ledger_range_v2"
                metadata["ledger_insertion_watermark"] = insertion_watermark(db)
            if definition.builder is not None:
                metadata["indexed_builder"] = definition.builder
            seq = db.execute("SELECT COALESCE(MAX(created_seq),0)+1 FROM snapshots").fetchone()[0]
            db.execute("INSERT INTO snapshots VALUES (?,?,?,?,?)", (snapshot_id, event_id, definition.revision, seq, json.dumps(metadata)))
            for code, rows in rankings.items():
                for row in rows:
                    payload = json.dumps(row, ensure_ascii=False)
                    if code is None:
                        db.execute("INSERT INTO snapshot_rows VALUES (?,?,?)", (snapshot_id, row["wallet"], payload))
                    else:
                        db.execute("INSERT INTO snapshot_builder_rows VALUES (?,?,?,?)",
                                   (snapshot_id, code, row["wallet"], payload))
            identities = {alias: row for row in rankings[None] for alias in row['aliases']}
            if definition.end_ts is not None:
                for checkout in checkouts:
                    identity = identities[checkout['wallet']]
                    frozen = {**checkout, 'source_wallet': checkout['wallet'], 'wallet': identity['wallet'],
                              'aliases': identity['aliases'], 'username': identity['username'],
                              'pack_id': None if checkout['pack_id'] in (None,'None') else checkout['pack_id']}
                    db.execute('INSERT INTO snapshot_checkout_rows VALUES(?,?,?,?)',
                               (snapshot_id,checkout['tx_hash'],checkout['log_index'],json.dumps(frozen,ensure_ascii=False)))
            db.execute("INSERT INTO event_state VALUES (?,?,NULL,NULL) ON CONFLICT(event_id,revision) DO UPDATE SET last_error=NULL",
                       (event_id, definition.revision))
            return metadata

    def read_checkout_snapshot(self, event_id, snapshot_id=None, *, builder=None):
        if builder is not None:
            builder = builder_code(builder)
        metadata, identities, health = self.read_snapshot(event_id,snapshot_id,builder=builder)
        if metadata is None:
            return None, [], health
        builder = metadata["builder"]
        if not metadata.get('checkout_evidence_supported'):
            raise CheckoutEvidenceUnavailable('Snapshot has no frozen checkout evidence; wait for a new complete snapshot.')
        with self._connection() as db:
            storage = metadata.get('checkout_evidence_storage')
            if storage in {'ledger_range_v1', 'ledger_range_v2'}:
                cutoff = metadata.get('ledger_insertion_watermark')
                if storage == 'ledger_range_v1':
                    record = db.execute('SELECT insertion_id FROM snapshot_ledger_watermarks WHERE snapshot_id=?',
                                        (metadata['snapshot_id'],)).fetchone()
                    cutoff = record[0] if record else None
                if type(cutoff) is not int or cutoff < 0:
                    raise CheckoutEvidenceUnavailable('Snapshot has no frozen insertion watermark.')
                frozen_identities = {alias: row for row in identities for alias in row['aliases']}
                rows = []
                for raw in checkout_payloads(db, event_id, metadata['revision'], cutoff):
                    checkout = json.loads(raw[0])
                    if (checkout['block_number'] > metadata['through_block']
                            or checkout['timestamp'] > timestamp(metadata['data_through_at'])
                            or (builder is not None and checkout.get('builder') != builder)):
                        continue
                    identity = frozen_identities.get(checkout['wallet'])
                    if identity is None:
                        raise LedgerConflict('frozen checkout identity missing')
                    rows.append({**checkout,'source_wallet':checkout['wallet'],'wallet':identity['wallet'],
                        'aliases':identity['aliases'],'username':identity['username'],
                        'pack_id':None if checkout['pack_id'] in (None,'None') else checkout['pack_id']})
            elif storage is None:
                rows = [json.loads(r[0]) for r in db.execute(
                    'SELECT payload FROM snapshot_checkout_rows WHERE snapshot_id=? ORDER BY tx_hash,log_index',
                    (metadata['snapshot_id'],))]
            else:
                raise CheckoutEvidenceUnavailable('Unsupported checkout snapshot storage version.')
        if builder is not None:
            rows = [r for r in rows if r.get('builder') == builder]
        return metadata, rows, health

    def read_snapshot(self, event_id, snapshot_id=None, *, builder=None):
        if builder is not None:
            builder = builder_code(builder)
        with self._connection() as db:
            definition = self._event(db, event_id)
            if definition.builder is not None:
                if builder is not None and builder != definition.builder:
                    raise ValueError("builder outside indexed activity scope")
                builder = definition.builder
            latest = db.execute("SELECT * FROM snapshots WHERE event_id=? AND revision=? ORDER BY created_seq DESC LIMIT 1",
                                (event_id, definition.revision)).fetchone()
            if snapshot_id:
                record = db.execute("SELECT * FROM snapshots WHERE event_id=? AND snapshot_id=?", (event_id, snapshot_id)).fetchone()
                if record is None:
                    raise ValueError("snapshot does not belong to this event")
            else:
                record = latest
            state = db.execute("SELECT last_error,progress FROM event_state WHERE event_id=? AND revision=?",
                               (event_id, definition.revision)).fetchone()
            current_metadata = json.loads(latest["metadata"]) if latest else None
            overdue = bool(current_metadata and current_metadata["status"] != "final"
                and time.time() - timestamp(current_metadata["generated_at"]) > self.stale_after_seconds)
            reason = ("last_error" if state and state[0] else "update_overdue" if overdue
                      else "no_current_snapshot" if record and latest is None else None)
            health = {"stale": reason is not None, "stale_reason": reason,
                      "last_error": state[0] if state else None,
                      "progress": json.loads(state[1]) if state and state[1] else None,
                      "update_interval_seconds": self.update_interval_seconds,
                      "stale_after_seconds": self.stale_after_seconds}
            if record is None:
                return None, [], health
            metadata = json.loads(record["metadata"])
            if metadata.get("indexed_builder") is not None and metadata["indexed_builder"] != builder:
                raise ValueError("snapshot indexed for another builder")
            metadata.setdefault("builder_filter_supported", False)
            metadata["builder"] = builder
            if builder is not None:
                if not metadata["builder_filter_supported"]:
                    raise BuilderFilterUnavailable("Snapshot has no complete builder evidence; refresh after the versioned rescan.")
                rows = [json.loads(row[0]) for row in db.execute(
                    "SELECT payload FROM snapshot_builder_rows WHERE snapshot_id=? AND builder=?",
                    (record["snapshot_id"], builder))]
                metadata["total_participants"] = len(rows)
            else:
                rows = [json.loads(row[0]) for row in db.execute("SELECT payload FROM snapshot_rows WHERE snapshot_id=?", (record["snapshot_id"],))]
            return metadata, rows, health

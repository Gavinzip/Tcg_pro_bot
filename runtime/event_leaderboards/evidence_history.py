"""Stable insertion watermarks keep old checkout snapshots immutable after repairs.

Legacy range snapshots acquire a sidecar watermark before the first late insert.
Their metadata and public IDs are not rewritten. Explicit integer IDs survive
SQLite VACUUM; rowid itself must never be used as a persistent watermark.
"""
import json


def initialize_evidence_history(db):
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ledger_insertions'").fetchone():
        return
    db.execute("CREATE TABLE ledger_insertions (insertion_id INTEGER PRIMARY KEY AUTOINCREMENT,"
               "event_id TEXT NOT NULL, revision INTEGER NOT NULL, chain_id INTEGER NOT NULL,"
               "tx_hash TEXT NOT NULL, log_index INTEGER NOT NULL,"
               "UNIQUE(event_id,revision,chain_id,tx_hash,log_index))")
    db.execute("CREATE TABLE snapshot_ledger_watermarks (snapshot_id TEXT PRIMARY KEY, insertion_id INTEGER NOT NULL)")
    db.execute("INSERT INTO ledger_insertions(event_id,revision,chain_id,tx_hash,log_index) "
               "SELECT event_id,revision,chain_id,tx_hash,log_index FROM ledger ORDER BY rowid")
    cutoff = insertion_watermark(db)
    for record in db.execute("SELECT snapshot_id,metadata FROM snapshots"):
        if json.loads(record[1]).get('checkout_evidence_storage') == 'ledger_range_v1':
            db.execute("INSERT INTO snapshot_ledger_watermarks VALUES (?,?)", (record[0], cutoff))
    # Covers ordinary chunks AND existing prefix-seed insertions in the same transaction.
    db.execute("CREATE TRIGGER record_ledger_insertion AFTER INSERT ON ledger BEGIN "
               "INSERT INTO ledger_insertions(event_id,revision,chain_id,tx_hash,log_index) "
               "VALUES (NEW.event_id,NEW.revision,NEW.chain_id,NEW.tx_hash,NEW.log_index); END")


def insertion_watermark(db):
    return db.execute("SELECT COALESCE(MAX(insertion_id),0) FROM ledger_insertions").fetchone()[0]


def checkout_payloads(db, event_id, revision, cutoff):
    return db.execute("SELECT l.payload FROM ledger l JOIN ledger_insertions i "
                      "ON l.event_id=i.event_id AND l.revision=i.revision AND l.chain_id=i.chain_id "
                      "AND l.tx_hash=i.tx_hash AND l.log_index=i.log_index "
                      "WHERE l.event_id=? AND l.revision=? AND i.insertion_id<=? "
                      "ORDER BY l.tx_hash,l.log_index", (event_id, revision, cutoff))

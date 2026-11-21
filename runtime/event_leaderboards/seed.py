"""Explicit, audited reuse of a completely confirmed prefix for a new activity.

This does not rescope the archived activity or trust an aggregate as checkout proof.
The scanner still verifies every copied cursor's reorg anchor before continuing.
"""
import json
import time

from .config import source_key, timestamp
from .store import LedgerConflict


def seed_from_final_snapshot(store, source_event_id, target_event_id):
    if source_event_id == target_event_id:
        raise ValueError("source and target activities must differ")
    with store._connection(write=True) as db:
        source = store._event(db, source_event_id)
        target = store._event(db, target_event_id)
        if (source.start_ts != target.start_ts or source.end_ts is None
                or (target.end_ts is not None and target.end_ts < source.end_ts)
                or source.chain_id != target.chain_id or source.token_decimals != target.token_decimals
                or (source.builder is not None and source.builder != target.builder)
                or source.checkout_schema_version != 2 or target.checkout_schema_version != 2
                or not set(target.contracts).issubset(source.contracts)
                or any(source.contract_schemas[c] != target.contract_schemas[c] for c in target.contracts)):
            raise ValueError("source is not an identical, completely confirmed prefix")
        record = db.execute("SELECT * FROM snapshots WHERE event_id=? AND revision=? ORDER BY created_seq DESC LIMIT 1",
                            (source_event_id, source.revision)).fetchone()
        if record is None:
            raise LedgerConflict("source has no published snapshot")
        metadata = json.loads(record["metadata"])
        if (metadata["status"] != "final" or not metadata.get("checkout_evidence_supported")
                or timestamp(metadata["data_through_at"]) < source.end_ts):
            raise LedgerConflict("source activity has not completed checkout verification")
        state = db.execute("SELECT last_error FROM event_state WHERE event_id=? AND revision=?",
                           (source_event_id, source.revision)).fetchone()
        if state and state[0]:
            raise LedgerConflict("source verification has an unresolved error")
        through = metadata["through_block"]
        cursors = {}
        for contract in source.contracts:
            row = db.execute("SELECT next_block,block_hash FROM checkpoints WHERE event_id=? AND revision=? AND source_key=?",
                             (source_event_id, source.revision, source_key(source, contract))).fetchone()
            if row is None or row[0] != through + 1 or not row[1]:
                raise LedgerConflict("source snapshot and complete anchored cursors differ")
            cursors[contract] = row
        if len({r[1] for r in cursors.values()}) != 1:
            raise LedgerConflict("source cursor anchors disagree")
        db.execute("CREATE TABLE IF NOT EXISTS event_prefix_seeds (event_id TEXT,revision INTEGER,report TEXT NOT NULL,PRIMARY KEY(event_id,revision))")
        prior = db.execute("SELECT report FROM event_prefix_seeds WHERE event_id=? AND revision=?",
                           (target_event_id, target.revision)).fetchone()
        if prior:
            report = json.loads(prior[0])
            if report["source_snapshot_id"] != metadata["snapshot_id"]:
                raise LedgerConflict("activity was seeded from another snapshot")
            return {**report, "already_seeded": True}
        for table in ("ledger", "checkpoints", "aliases", "snapshots"):
            if db.execute(f"SELECT 1 FROM {table} WHERE event_id=? AND revision=? LIMIT 1",
                          (target_event_id, target.revision)).fetchone():
                raise LedgerConflict("only an empty target revision can accept a confirmed prefix")
        ledger = db.execute("SELECT chain_id,tx_hash,log_index,payload FROM ledger WHERE event_id=? AND revision=?",
                            (source_event_id, source.revision)).fetchall()
        frozen = {(row[0], row[1]): json.loads(row[2]) for row in db.execute(
            "SELECT tx_hash,log_index,payload FROM snapshot_checkout_rows WHERE snapshot_id=?", (metadata["snapshot_id"],))}
        if len(ledger) != len(frozen):
            raise LedgerConflict("source ledger and frozen checkout evidence differ")
        copied = 0
        for row in ledger:
            payload = json.loads(row[3])
            proof = frozen.get((row[1], row[2]))
            if (proof is None or proof["source_wallet"] != payload["wallet"]
                    or proof["pack_id"] != (None if payload["pack_id"] in (None, "None") else payload["pack_id"])
                    or any(proof[k] != payload[k] for k in ("pack_contract", "paid_amount_raw", "draw_count",
                        "block_number", "block_hash", "timestamp", "builder"))):
                raise LedgerConflict("source frozen evidence does not match the verified ledger")
            if (payload["pack_contract"] not in target.contracts
                    or (target.builder is not None and payload.get("builder") != target.builder)):
                continue
            if payload["block_number"] > through or not source.start_ts <= payload["timestamp"] < source.end_ts:
                raise LedgerConflict("ledger exceeds the published verified prefix")
            db.execute("INSERT INTO ledger VALUES (?,?,?,?,?,?)",
                       (target_event_id, target.revision, row[0], row[1], row[2], row[3]))
            copied += 1
        db.execute("INSERT INTO aliases SELECT ?,?,old_wallet,new_wallet,evidence FROM aliases WHERE event_id=? AND revision=?",
                   (target_event_id, target.revision, source_event_id, source.revision))
        for contract in target.contracts:
            cursor = cursors[contract]
            db.execute("INSERT INTO checkpoints VALUES (?,?,?,?,?)",
                       (target_event_id, target.revision, source_key(target, contract), cursor[0], cursor[1]))
        report = dict(source_event_id=source_event_id, source_revision=source.revision,
                      source_snapshot_id=metadata["snapshot_id"], target_event_id=target_event_id,
                      target_revision=target.revision, checkouts=copied, through_block=through,
                      anchor_block_hash=next(iter(cursors.values()))[1],
                      data_through_at=metadata["data_through_at"], created_at=int(time.time()))
        db.execute("INSERT INTO event_prefix_seeds VALUES (?,?,?)", (target_event_id, target.revision, json.dumps(report)))
        return report


def publish_confirmed_prefix(store, provider, source_event_id, target_event_id, *, usernames=None):
    report = seed_from_final_snapshot(store, source_event_id, target_event_id)
    current, _, _ = store.read_snapshot(target_event_id)
    if current:
        return {**report, "snapshot": current}
    if provider.block(report["through_block"])["hash"] != report["anchor_block_hash"]:
        store.record_failure(target_event_id, "checkpoint_reorg_mismatch")
        raise LedgerConflict("confirmed prefix anchor changed on chain")
    snapshot = store.publish_snapshot(target_event_id, report["through_block"], report["data_through_at"],
                                      usernames=usernames, expected_revision=report["target_revision"])
    return {**report, "snapshot": snapshot}

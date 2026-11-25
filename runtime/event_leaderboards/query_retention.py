"""Bound disposable live-query versions without touching evidence or final results."""
import json
import os
from dataclasses import dataclass
from .config import timestamp


class SnapshotExpired(ValueError):
    pass


@dataclass(frozen=True)
class QueryRetention:
    events: frozenset
    seconds: int = 3600
    max_versions: int = 120

    @classmethod
    def configured(cls, events=None):
        selected = frozenset(events if events is not None else filter(None, (
            part.strip() for part in os.getenv('EVENT_LEADERBOARD_QUERY_RETENTION_EVENTS', 'bnb-renaiss-2026').split(','))))
        seconds = int(os.getenv('EVENT_LEADERBOARD_QUERY_RETENTION_SECONDS', '3600'))
        versions = int(os.getenv('EVENT_LEADERBOARD_QUERY_MAX_VERSIONS', '120'))
        if seconds < 60 or versions < 2:
            raise ValueError('Invalid live query retention policy')
        return cls(selected, seconds, versions)

    @staticmethod
    def initialize(db):
        db.execute('CREATE TABLE IF NOT EXISTS event_query_versions ('
                   'snapshot_id TEXT PRIMARY KEY, created_at INTEGER NOT NULL)')

    def record(self, db, event_id, metadata, now):
        if event_id not in self.events:
            return
        if metadata['status'] != 'final':
            db.execute('INSERT INTO event_query_versions VALUES (?,?)', (metadata['snapshot_id'], now))
        rows = db.execute('SELECT s.snapshot_id,v.created_at FROM snapshots s JOIN event_query_versions v '
                          'ON v.snapshot_id=s.snapshot_id WHERE s.event_id=? ORDER BY s.created_seq DESC',
                          (event_id,)).fetchall()
        expired = [(row[0],) for index, row in enumerate(rows)
                   if row[0] != metadata['snapshot_id'] and
                   (index >= self.max_versions or row[1] <= now - self.seconds)]
        # Every dependent query table is removed in the publisher's transaction.
        # ledger, ledger_insertions, aliases, definitions and final snapshots stay intact.
        for table in ('snapshot_rows', 'snapshot_builder_rows', 'snapshot_checkout_rows',
                      'snapshot_ledger_watermarks', 'event_query_versions', 'snapshots'):
            db.executemany('DELETE FROM ' + table + ' WHERE snapshot_id=?', expired)

    def check_pin(self, event_id, record, latest, now):
        if event_id not in self.events:
            return
        if record is None:
            raise SnapshotExpired('Query version unavailable; fetch the latest leaderboard without a cursor.')
        metadata = json.loads(record['metadata'])
        if metadata.get('status') == 'final' or (latest and record['snapshot_id'] == latest['snapshot_id']):
            return
        if timestamp(metadata['generated_at']) <= now - self.seconds:
            raise SnapshotExpired('Query version expired; fetch the latest leaderboard without a cursor.')

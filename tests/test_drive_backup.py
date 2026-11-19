"""Recovery tests use a real local encrypted restic repository, never production."""
import json
import os
from pathlib import Path
import sqlite3
import unittest

from runtime.backup_snapshot import SnapshotError, capture, validate_bulk, verify_restored
from runtime.drive_backup import DriveBackup, DriveBackupError

BASE = Path(os.environ['TCG_BACKUP_TEST_ROOT'])
RESTIC = os.environ['TCG_BACKUP_TEST_RESTIC']


class BackupTests(unittest.TestCase):
    def fixture(self, name):
        base = BASE / name
        root = base / 'source'
        root.mkdir(parents=True)
        (root / 'user_settings.json').write_text('{"wallet":"example"}')
        (root / 'market_cache').mkdir()
        (root / 'market_cache' / 'card.png').write_bytes(b'\x89PNG\r\n' + bytes(range(256)) * 1024)
        old = root / 'renaiss_sync' / 'rankings'
        old.mkdir(parents=True)
        (old / 'old.json').write_text('{"obsolete":true}')
        git = root / 'renaiss_sync' / 'backup_repo'
        git.mkdir()
        (git / 'private-git-copy').write_text('must not leave the original volume')
        return base, root

    def test_sqlite_wal_is_captured_as_consistent_database(self):
        base, root = self.fixture('sqlite')
        db = sqlite3.connect(root / 'activity.sqlite3')
        try:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE events(id INTEGER PRIMARY KEY, amount INTEGER)')
            db.execute('INSERT INTO events VALUES (1, 25)')
            db.commit()
            stage = root / '.tcg-drive-backup' / 'staging' / 'test'
            stage.mkdir(parents=True)
            manifest = capture(root, stage)
            self.assertFalse(any(f['path'].endswith(('-wal', '-shm')) for f in manifest['files']))
            self.assertFalse(any('rankings/' in f['path'] or 'backup_repo/' in f['path'] for f in manifest['files']))
            with sqlite3.connect(stage / 'data' / 'activity.sqlite3') as copy:
                self.assertEqual(copy.execute('SELECT amount FROM events').fetchall(), [(25,)])
            self.assertTrue(any(f['kind'] == 'sqlite-online' for f in manifest['files']))
        finally:
            db.close()

    def test_changed_media_is_not_reported_as_success(self):
        base, root = self.fixture('changed-media')
        stage = root / '.tcg-drive-backup' / 'staging' / 'test'
        stage.mkdir(parents=True)
        manifest = capture(root, stage)
        (root / 'market_cache' / 'card.png').write_bytes(b'changed')
        with self.assertRaisesRegex(SnapshotError, 'media_changed'):
            validate_bulk(manifest)

    def test_symlink_fails_explicitly(self):
        base, root = self.fixture('symlink')
        (root / 'external').symlink_to(base / 'outside')
        with self.assertRaisesRegex(SnapshotError, 'symlink'):
            capture(root, root / '.tcg-drive-backup' / 'staging' / 'test')

    def test_real_encrypted_roundtrip_and_corruption_detection(self):
        base, root = self.fixture('roundtrip')
        env = {'TCG_BACKUP_SOURCE': str(root), 'TCG_DRIVE_BACKUP_REPOSITORY': str(base / 'repository'),
               'TCG_DRIVE_BACKUP_PASSWORD': 'synthetic-test-password-not-a-real-secret',
               'TCG_BACKUP_RESTIC_BIN': RESTIC, 'PATH': os.environ['PATH']}
        backup = DriveBackup(env, allow_local_repository=True)
        backup.initialize()
        result = backup.backup()
        self.assertEqual(result['phase'], 'complete')
        self.assertFalse(result['fullRestoreVerified'])
        restored = base / 'restore'
        restored.mkdir()
        verified = backup.verify(result['snapshotId'], result['manifestPath'], restored)
        self.assertTrue(verified['fullRestoreVerified'])
        self.assertEqual(verified['files'], 2)
        manifest = json.loads(backup.run(['dump', result['snapshotId'], result['manifestPath']]))
        user_file = restored / Path(manifest['stageRoot']).relative_to('/') / 'data' / 'user_settings.json'
        user_file.write_text('tampered')
        with self.assertRaisesRegex(SnapshotError, 'restored_file_mismatch'):
            verify_restored(manifest, restored)
        with self.assertRaisesRegex(DriveBackupError, 'outside_production'):
            backup.verify(result['snapshotId'], result['manifestPath'], root / 'unsafe-restore')
        with self.assertRaisesRegex(DriveBackupError, 'already_running'):
            with backup.locked():
                with backup.locked():
                    pass


if __name__ == '__main__':
    unittest.main()

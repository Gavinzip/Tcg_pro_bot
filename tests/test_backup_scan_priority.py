"""Actual supervisor/backup subprocess coordination, without cloud calls."""
import asyncio
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys
import sqlite3
import threading
import unittest
from unittest.mock import patch
import uuid

from runtime import production_jobs
from runtime.job_priority import JobPriority
from runtime.backup_snapshot import sqlite_copy


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / '.scratch' / 'backup-priority-tests' / uuid.uuid4().hex


class BackupScanPriorityTests(unittest.IsolatedAsyncioTestCase):
    def fixture(self):
        root = ROOT / self._testMethodName
        scripts = root / 'scripts'
        scripts.mkdir(parents=True)
        worker = root / 'restic-fixture'
        worker.write_text('#!' + sys.executable + '\n' + '''
import json,os,time
from pathlib import Path
r=Path(os.environ['TCG_BACKUP_SOURCE']); n=0; child=os.fork()
name='counter-child' if child==0 else 'counter'
if child: (r/'worker-pids.json').write_text(json.dumps([os.getpid(),child]))
while not (r/'stop').exists():
 n+=1; (r/(name+'.tmp')).write_text(str(n)); (r/(name+'.tmp')).replace(r/name)
 if child and (r/'counter-child').exists(): (r/'ready').touch()
 time.sleep(.01)
if child==0: os._exit(0)
os.waitpid(child,0)
print(json.dumps({'finished':True}))
''')
        worker.chmod(0o700)
        (scripts / 'backup.py').write_text('import sys\nsys.path.insert(0,' + repr(str(REPO)) + ')\n' + '''
from runtime.drive_backup import DriveBackup
b=DriveBackup(allow_local_repository=True)
import os
if os.getenv('FIXTURE_TIMEOUT'): b.timeout=float(os.environ['FIXTURE_TIMEOUT'])
b.run(['backup'])
''')
        (scripts / 'scan.py').write_text('''
import json,os,time
from pathlib import Path
r=Path(os.environ['TCG_BACKUP_SOURCE']); before=[int((r/n).read_text()) for n in ['counter','counter-child']]
time.sleep(.25)
after=[int((r/n).read_text()) for n in ['counter','counter-child']]
(r/'scan-result.json').write_text(json.dumps({'before':before,'after':after}))
''')
        env = {'TCG_BACKUP_SOURCE': str(root), 'TCG_DRIVE_BACKUP_ENABLED': '1',
               'TCG_DRIVE_BACKUP_REPOSITORY': str(root / 'repository'),
               'TCG_DRIVE_BACKUP_PASSWORD': 'fixture-password-no-real-secret',
               'TCG_BACKUP_RESTIC_BIN': str(worker)}
        return root, env

    async def ready(self, root):
        for _ in range(200):
            if (root / 'ready').exists():
                return
            await asyncio.sleep(.01)
        self.fail('backup fixture did not start')

    async def test_real_backup_uploader_yields_to_scan_and_then_finishes(self):
        root, env = self.fixture()
        with patch.dict(os.environ, env), patch.object(production_jobs, 'ROOT', root):
            backup = asyncio.create_task(production_jobs.run_command('drive-backup', 'backup.py', [], 5))
            try:
                await self.ready(root)
                self.assertEqual(await production_jobs.run_command('event-leaderboards', 'scan.py', [], 3), 0)
                result = json.loads((root / 'scan-result.json').read_text())
                self.assertEqual(result['before'], result['after'], 'backup kept consuming resources during scan')
            finally:
                (root / 'stop').touch()
                self.assertEqual(await asyncio.wait_for(backup, 5), 0)

    async def test_backup_does_not_launch_uploader_while_scan_already_owns_priority(self):
        root, env = self.fixture()
        priority = JobPriority(root)
        with patch.dict(os.environ, env), patch.object(production_jobs, 'ROOT', root):
            async with priority.scan():
                backup = asyncio.create_task(production_jobs.run_command('drive-backup', 'backup.py', [], 5))
                await asyncio.sleep(.2)
                self.assertFalse((root / 'ready').exists())
            try:
                await self.ready(root)
            finally:
                (root / 'stop').touch()
                self.assertEqual(await asyncio.wait_for(backup, 5), 0)

    async def test_scanner_cancellation_releases_intent_and_work_locks(self):
        root, _ = self.fixture()
        priority = JobPriority(root)
        entered = asyncio.Event()
        async def scan():
            async with priority.scan():
                entered.set()
                await asyncio.Event().wait()
        task = asyncio.create_task(scan())
        await asyncio.wait_for(entered.wait(), 2)
        self.assertTrue(priority.scan_pending())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(priority.scan_pending())
        with priority.backup_chunk():
            pass

    async def test_scan_admission_timeout_releases_intent_without_starting_child(self):
        root, env = self.fixture()
        priority = JobPriority(root)
        with patch.dict(os.environ, env), patch.object(production_jobs, 'ROOT', root), \
             patch.object(production_jobs.asyncio, 'create_subprocess_exec') as spawn:
            with priority.backup_chunk():
                with self.assertRaises(asyncio.TimeoutError):
                    await production_jobs.run_command('event-leaderboards', 'scan.py', [], .05)
                self.assertFalse(priority.scan_pending())
                spawn.assert_not_called()

    async def test_paused_upload_keeps_original_timeout_and_cleans_up_group(self):
        root, env = self.fixture()
        env['FIXTURE_TIMEOUT'] = '.4'
        with patch.dict(os.environ, env), patch.object(production_jobs, 'ROOT', root):
            backup = asyncio.create_task(production_jobs.run_command('drive-backup', 'backup.py', [], 5))
            try:
                await self.ready(root)
                async with JobPriority(root).scan():
                    self.assertEqual(await asyncio.wait_for(backup, 2), 1)
                    values = [(root/n).read_text() for n in ('counter','counter-child')]
                    await asyncio.sleep(.1)
                    self.assertEqual(values, [(root/n).read_text() for n in ('counter','counter-child')])
            finally:
                (root / 'stop').touch()
                if not backup.done():
                    backup.cancel()
                    await asyncio.gather(backup, return_exceptions=True)

    async def test_cancelled_backup_stops_its_separate_uploader_process_group(self):
        root, env = self.fixture()
        with patch.dict(os.environ, env), patch.object(production_jobs, 'ROOT', root):
            backup = asyncio.create_task(production_jobs.run_command('drive-backup', 'backup.py', [], 5))
            try:
                await self.ready(root)
                async with JobPriority(root).scan():
                    before = [(root/n).read_text() for n in ('counter','counter-child')]
                    backup.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(backup, 3)
                    await asyncio.sleep(.1)
                    self.assertEqual(before, [(root/n).read_text() for n in ('counter','counter-child')])
            finally:
                (root / 'stop').touch()
                if not backup.done():
                    backup.cancel()
                    await asyncio.gather(backup, return_exceptions=True)

    async def test_live_wal_capture_yields_after_page_step_and_keeps_one_consistent_view(self):
        root, _ = self.fixture()
        source, target = root/'live.sqlite3', root/'copy.sqlite3'
        writer = sqlite3.connect(source)
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('CREATE TABLE records(id INTEGER PRIMARY KEY, payload TEXT)')
        writer.executemany('INSERT INTO records VALUES (?,?)', ((i,'a'*2048) for i in range(3000)))
        writer.commit()
        copying, allow_checkpoint = threading.Event(), threading.Event()
        priority = JobPriority(root)
        class WatchedPriority:
            @contextmanager
            def backup_chunk(self):
                with priority.backup_chunk() as lease:
                    class Boundary:
                        first = True
                        def checkpoint(self):
                            if self.first:
                                self.first = False
                                copying.set()
                                if not allow_checkpoint.wait(3):
                                    raise TimeoutError('test foreground was not scheduled')
                            lease.checkpoint()
                    yield Boundary()
        backup = asyncio.create_task(asyncio.to_thread(sqlite_copy, source, target, priority=WatchedPriority()))
        try:
            self.assertTrue(await asyncio.to_thread(copying.wait, 2))
            async def scan():
                async with priority.scan():
                    writer.execute('INSERT INTO records VALUES (?,?)', (3000,'new verified checkout'))
                    writer.commit()
                    self.assertFalse(backup.done())
            scanning = asyncio.create_task(scan())
            for _ in range(100):
                if priority.scan_pending():
                    break
                await asyncio.sleep(.01)
            self.assertTrue(priority.scan_pending())
            allow_checkpoint.set()
            await asyncio.wait_for(scanning, 2)
            await asyncio.wait_for(backup, 3)
            with sqlite3.connect(target) as copied:
                self.assertEqual(copied.execute('PRAGMA quick_check').fetchone()[0], 'ok')
                self.assertEqual(copied.execute('SELECT count(*) FROM records').fetchone()[0], 3000)
            self.assertEqual(writer.execute('SELECT count(*) FROM records').fetchone()[0], 3001)
        finally:
            allow_checkpoint.set()
            writer.close()
            await asyncio.gather(backup, return_exceptions=True)


if __name__ == '__main__':
    unittest.main()

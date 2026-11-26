"""Exercise scheduler deadlines with a clock; never spawn subprocesses or create files."""
import asyncio
import json
import os
from pathlib import Path
import uuid
import unittest
from unittest.mock import patch

from runtime import production_jobs


class ProductionJobScheduleTests(unittest.IsolatedAsyncioTestCase):
    async def exercise_backup(self, *, now, status, update_during_first_sleep=None, fail_first=False):
        clock = [now]
        runs, sleeps = [], []
        interval = 86400

        async def sleep(delay):
            sleeps.append(delay)
            if len(sleeps) == 1 and update_during_first_sleep is not None:
                # Another process completes a manual backup halfway through the wait.
                status.update(phase='complete', updatedAt=clock[0] + update_during_first_sleep)
            clock[0] += delay

        async def run(*args):
            runs.append(clock[0])
            if fail_first and len(runs) == 1:
                raise RuntimeError('synthetic child failure')
            raise asyncio.CancelledError()

        with patch.object(production_jobs.time, 'time', side_effect=lambda: clock[0]), \
             patch.object(production_jobs.Path, 'read_text', side_effect=lambda: json.dumps(status)), \
             patch.object(production_jobs.asyncio, 'sleep', side_effect=sleep), \
             patch.object(production_jobs, 'run_command', side_effect=run):
            with self.assertRaises(asyncio.CancelledError):
                await production_jobs.periodic('drive-backup', 'backup_tcg_drive.py', ['backup'], interval, 100, True)
        return runs, sleeps

    async def test_restart_waits_only_until_existing_success_expires(self):
        now = 200000
        last_success = now - 23 * 3600
        runs, sleeps = await self.exercise_backup(now=now, status={'phase': 'complete', 'updatedAt': last_success})
        self.assertEqual(sleeps, [3600])
        self.assertEqual(runs, [last_success + 86400])

    async def test_manual_success_during_wait_reanchors_next_deadline(self):
        now = 200000
        runs, sleeps = await self.exercise_backup(now=now,
            status={'phase': 'complete', 'updatedAt': now - 23 * 3600}, update_during_first_sleep=1800)
        manual_success = now + 1800
        self.assertEqual(runs, [manual_success + 86400])
        self.assertEqual(sleeps, [3600, 86400 - 1800])

    async def test_recent_failure_is_due_and_child_failure_retries_next_interval(self):
        now = 200000
        runs, sleeps = await self.exercise_backup(now=now,
            status={'phase': 'failed', 'updatedAt': now - 1}, fail_first=True)
        self.assertEqual(runs, [now, now + 86400])
        self.assertEqual(sleeps, [86400])

    async def test_event_job_keeps_immediate_start_and_its_own_interval(self):
        clock = [200000]
        runs, sleeps = [], []

        async def sleep(delay):
            sleeps.append(delay)
            clock[0] += delay

        async def run(*args):
            runs.append(clock[0])
            if len(runs) == 2:
                raise asyncio.CancelledError()
            return 0

        with patch.object(production_jobs.Path, 'read_text', side_effect=AssertionError('event job must not read backup state')) as read, \
             patch.object(production_jobs.time, 'monotonic', side_effect=lambda: clock[0]), \
             patch.object(production_jobs.asyncio, 'sleep', side_effect=sleep), \
             patch.object(production_jobs, 'run_command', side_effect=run):
            with self.assertRaises(asyncio.CancelledError):
                await production_jobs.periodic('event-leaderboards', 'sync_event_leaderboards.py', [], 60, 540)
        read.assert_not_called()
        self.assertEqual(runs, [200000, 200060])
        self.assertEqual(sleeps, [60])

    async def exercise_scans(self, durations, *, outcomes=None):
        clock, starts, sleeps = [1000.], [], []
        async def run(*args):
            index = len(starts)
            starts.append(clock[0])
            if index == len(durations):
                raise asyncio.CancelledError()
            clock[0] += durations[index]
            if outcomes and outcomes[index] is not None:
                raise outcomes[index]
            return 0
        async def sleep(delay):
            self.assertGreaterEqual(delay, 0)
            sleeps.append(delay)
            clock[0] += delay
        with patch.object(production_jobs.time, 'monotonic', side_effect=lambda: clock[0]), \
             patch.object(production_jobs.time, 'time', side_effect=AssertionError('cadence must use monotonic time')), \
             patch.object(production_jobs, 'run_command', side_effect=run), \
             patch.object(production_jobs.asyncio, 'sleep', side_effect=sleep):
            with self.assertRaises(asyncio.CancelledError):
                await production_jobs.periodic('event-leaderboards', 'unused.py', [], 60, 1800)
        return [v - 1000 for v in starts], sleeps

    async def test_work_duration_is_included_in_sixty_second_start_interval(self):
        starts, sleeps = await self.exercise_scans([6, 12, 0, 59])
        self.assertEqual(starts, [0, 60, 120, 180, 240])
        self.assertEqual(sleeps, [54, 48, 60, 1])

    async def test_full_day_has_1440_start_slots_with_variable_short_scans(self):
        measured = [11.534, 5.952, 5.615, 5.265, 5.716, 6.643]
        starts, sleeps = await self.exercise_scans(measured * 240)
        self.assertEqual(starts, list(range(0, 86401, 60)))
        self.assertEqual(len(sleeps), 1440)

    async def test_overrun_runs_one_successor_without_parallel_or_catchup_burst(self):
        starts, sleeps = await self.exercise_scans([85, 6, 6])
        self.assertEqual(starts, [0, 85, 145, 205])
        self.assertEqual(sleeps, [0, 54, 54])

    async def test_long_overrun_and_exact_deadline_do_not_accumulate_missed_rounds(self):
        starts, sleeps = await self.exercise_scans([605, 60, 6])
        self.assertEqual(starts, [0, 605, 665, 725])
        self.assertEqual(sleeps, [0, 0, 54])

    async def test_failed_rounds_use_same_interval_and_cancellation_stops_rescheduling(self):
        starts, sleeps = await self.exercise_scans([2, 90, 6],
            outcomes=[RuntimeError('synthetic'), asyncio.TimeoutError(), None])
        self.assertEqual(starts, [0, 60, 150, 210])
        self.assertEqual(sleeps, [58, 0, 54])

    async def test_non_scanner_retains_wait_after_completion(self):
        clock, starts = [0.], []
        async def run(*args):
            starts.append(clock[0])
            if len(starts) == 2:
                raise asyncio.CancelledError()
            clock[0] += 7
        async def sleep(delay):
            clock[0] += delay
        with patch.object(production_jobs, 'run_command', side_effect=run), \
             patch.object(production_jobs.asyncio, 'sleep', side_effect=sleep):
            with self.assertRaises(asyncio.CancelledError):
                await production_jobs.periodic('drive-backup', 'unused.py', [], 86400, 1800)
        self.assertEqual(starts, [0, 86407])

    async def test_duplicate_start_calls_create_only_one_scanner_task(self):
        entered = asyncio.Event()
        async def run(*args):
            entered.set()
            await asyncio.Event().wait()
        env = {'EVENT_LEADERBOARD_SYNC_ENABLED': '1', 'TCG_DRIVE_BACKUP_ENABLED': '0',
               'EVENT_LEADERBOARD_SYNC_INTERVAL_SECONDS': '60'}
        with patch.dict(os.environ, env), patch.object(production_jobs, '_TASKS', {}), \
             patch.object(production_jobs, 'run_command', side_effect=run) as command:
            production_jobs.start_product_background_jobs()
            task = production_jobs._TASKS['event-leaderboards']
            try:
                production_jobs.start_product_background_jobs()
                await asyncio.wait_for(entered.wait(), 2)
                self.assertIs(production_jobs._TASKS['event-leaderboards'], task)
                self.assertEqual(command.await_count, 1)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_real_slow_child_processes_do_not_overlap(self):
        root = Path(__file__).resolve().parents[1] / '.scratch' / 'scanner-cadence-tests' / uuid.uuid4().hex
        scripts = root / 'scripts'
        scripts.mkdir(parents=True)
        events = root / 'events.jsonl'
        (scripts / 'cadence.py').write_text(
            'import json,os,time\n'
            'from pathlib import Path\n'
            'p=Path(' + repr(str(events)) + ')\n'
            'def record(kind):\n'
            ' with p.open("a") as f: f.write(json.dumps({"kind":kind,"pid":os.getpid(),"at":time.monotonic()})+"\\n")\n'
            'fd=os.open(str(p)+".active",os.O_CREAT|os.O_EXCL|os.O_WRONLY)\n'
            'try:\n record("start")\n time.sleep(.08)\n record("end")\n'
            'finally:\n os.close(fd)\n Path(str(p)+".active").unlink()\n')
        original = production_jobs.run_command
        rounds, parent_windows = [], []
        async def run(*args):
            started = production_jobs.time.monotonic()
            code = await original(*args)
            parent_windows.append((started, production_jobs.time.monotonic()))
            rounds.append(code)
            if len(rounds) == 3:
                raise asyncio.CancelledError()
            return code
        with patch.dict(os.environ, {'TCG_DRIVE_BACKUP_ENABLED': '0'}), \
             patch.object(production_jobs, 'ROOT', root), \
             patch.object(production_jobs, 'run_command', side_effect=run):
            with self.assertRaises(asyncio.CancelledError):
                await production_jobs.periodic('event-leaderboards', 'cadence.py', [], .04, 2)
        rows = [json.loads(line) for line in events.read_text().splitlines()]
        self.assertEqual(rounds, [0, 0, 0])
        self.assertEqual([r['kind'] for r in rows], ['start', 'end'] * 3)
        self.assertEqual(len({r['pid'] for r in rows}), 3)
        for start, end in zip(rows[::2], rows[1::2]):
            self.assertEqual(start['pid'], end['pid'])
            self.assertGreaterEqual(end['at'] - start['at'], .08)
        # On macOS Python 3.9 monotonic origins differ by process. Compare
        # parent windows and each child's own duration, never cross-PID clocks.
        for previous, following in zip(parent_windows, parent_windows[1:]):
            self.assertGreaterEqual(following[0], previous[1])
        self.assertFalse(Path(str(events) + '.active').exists())

    def test_status_without_valid_success_remains_due(self):
        for raw in ['{}', '[]', '{bad', '{"phase":"complete","updatedAt":"NaN"}']:
            with self.subTest(raw=raw), patch.object(production_jobs.Path, 'read_text', return_value=raw):
                self.assertTrue(production_jobs.backup_due(86400))


if __name__ == '__main__':
    unittest.main()

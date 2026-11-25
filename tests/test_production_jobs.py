"""Exercise scheduler deadlines with a clock; never spawn subprocesses or create files."""
import asyncio
import json
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
             patch.object(production_jobs.asyncio, 'sleep', side_effect=sleep), \
             patch.object(production_jobs, 'run_command', side_effect=run):
            with self.assertRaises(asyncio.CancelledError):
                await production_jobs.periodic('event-leaderboards', 'sync_event_leaderboards.py', [], 60, 540)
        read.assert_not_called()
        self.assertEqual(runs, [200000, 200060])
        self.assertEqual(sleeps, [60])

    async def test_one_minute_wait_still_follows_completion_even_for_a_slow_scan(self):
        clock, starts = [0.], []
        async def run(*args):
            starts.append(clock[0])
            if len(starts) == 2:
                raise asyncio.CancelledError()
            clock[0] += 605
        async def sleep(delay):
            self.assertEqual(delay, 60)
            clock[0] += delay
        with patch.object(production_jobs, 'run_command', side_effect=run), \
             patch.object(production_jobs.asyncio, 'sleep', side_effect=sleep):
            with self.assertRaises(asyncio.CancelledError):
                await production_jobs.periodic('event-leaderboards', 'unused.py', [], 60, 1800)
        self.assertEqual(starts, [0., 665.])

    def test_status_without_valid_success_remains_due(self):
        for raw in ['{}', '[]', '{bad', '{"phase":"complete","updatedAt":"NaN"}']:
            with self.subTest(raw=raw), patch.object(production_jobs.Path, 'read_text', return_value=raw):
                self.assertTrue(production_jobs.backup_due(86400))


if __name__ == '__main__':
    unittest.main()

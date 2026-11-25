"""Safe diagnostics at the real provider and supervisor boundaries; no network."""
import asyncio
import io
import json
import unittest
import signal
from unittest.mock import AsyncMock, patch

from runtime import production_jobs
from runtime.event_leaderboards.scanner import EtherscanProvider


class DiagnosticTests(unittest.IsolatedAsyncioTestCase):
    async def test_supervisor_preserves_safe_child_diagnostics_without_raw_output(self):
        # Import inside the test so the initial red result identifies the missing feature.
        from runtime.event_leaderboards.diagnostics import PREFIX, ScanDiagnostics
        lines = []
        with ScanDiagnostics(sink=lines.append) as diagnostic:
            with diagnostic.phase("head"):
                pass
        stderr = asyncio.StreamReader()
        stderr.feed_data(("private-key-on-stderr\n" + "\n".join(lines) + "\n").encode())
        stderr.feed_eof()
        stdout = asyncio.StreamReader()
        stdout.feed_data(b'private-key-on-stdout\n')
        stdout.feed_eof()
        class Child:
            returncode = 0
            pid = 12345
            async def wait(self):
                return 0
            async def communicate(self):
                return await stdout.read(), await stderr.read()
        child = Child()
        child.stdout, child.stderr = stdout, stderr
        captured = io.StringIO()
        with patch.object(production_jobs.asyncio, "create_subprocess_exec", new=AsyncMock(return_value=child)), \
             patch("sys.stdout", captured):
            self.assertEqual(await production_jobs.run_command("event-leaderboards", "unused.py", [], 10), 0)
        self.assertIn(PREFIX, captured.getvalue())
        self.assertNotIn("private-key", captured.getvalue())
        self.assertIn("duration_seconds=", captured.getvalue())

    async def test_successful_retry_is_counted_not_hidden(self):
        from runtime.event_leaderboards.diagnostics import ScanDiagnostics
        clock = [0.]
        outcomes = [TimeoutError("do-not-log-key"), io.BytesIO(b'{"result":"0x10"}')]
        def open_request(*args, **kwargs):
            value = outcomes.pop(0)
            if isinstance(value, Exception):
                clock[0] += 15
                raise value
            clock[0] += .5
            return value
        provider = EtherscanProvider({"EVENT_LEADERBOARD_API_KEY": "do-not-log-key",
                                      "EVENT_LEADERBOARD_REQUESTS_PER_SECOND": "0.5"})
        def sleep(seconds):
            clock[0] += seconds
        with patch("time.monotonic", side_effect=lambda: clock[0]), patch("time.sleep", side_effect=sleep), \
             patch("urllib.request.urlopen", side_effect=open_request), ScanDiagnostics(sink=None) as diagnostic:
            with diagnostic.phase("head"):
                self.assertEqual(provider.head(), 16)
            report = diagnostic.summary()
        self.assertEqual(report["totals"]["attempts"], 2)
        self.assertEqual(report["totals"]["retries"], 1)
        self.assertEqual(report["totals"]["timeouts"], 1)
        self.assertEqual(report["totals"]["retry_wait_ms"], 2000)
        self.assertNotIn("do-not-log-key", json.dumps(report))
        self.assertEqual(report['phase_actions']['head']['eth_blockNumber'], 2)
        self.assertEqual(report['failures'], {'provider_timeout': 1})

    async def test_child_events_merge_into_job_with_one_run_id_and_private_event_ref(self):
        from runtime.event_leaderboards.diagnostics import ScanDiagnostics, record_request, PREFIX
        lines = []
        with ScanDiagnostics(sink=lines.append) as job:
            with ScanDiagnostics(sink=lines.append, run_id=job.run_id, event_id='secret-looking-event') as event:
                with event.phase('overlap'):
                    with event.phase('boundary'):
                        record_request('eth_getBlockByNumber', attempted=True, elapsed_ms=5)
                    record_request('getLogs', attempted=True, elapsed_ms=6)
            report = job.summary()
        self.assertEqual(report['totals']['attempts'], 2)
        self.assertEqual(sum(x['attempts'] for x in report['phases'].values()), 2)
        self.assertEqual(report['phase_actions'], {'boundary': {'eth_getBlockByNumber': 1}, 'overlap': {'getLogs': 1}})
        records = [json.loads(x[len(PREFIX):]) for x in lines]
        self.assertEqual({r['run_id'] for r in records}, {job.run_id})
        self.assertNotIn('secret-looking-event', '\n'.join(lines))

    async def test_schema_rejects_secret_strings_unknown_fields_and_nonfinite_values(self):
        from runtime.event_leaderboards.diagnostics import ScanDiagnostics, PREFIX, validated_line
        lines = []
        with ScanDiagnostics(sink=lines.append):
            pass
        base = json.loads(lines[-1][len(PREFIX):])
        for changes in ({'url': 'https://private-key'}, {'details': {'secret': 1}},
                        {'actions': {'private-key': {}}}, {'phase': 'private-key'},
                        {'phase_actions': {'head': {'private-key': 1}}},
                        {'failures': {'private-key': 1}},
                        {'elapsed_ms': float('nan')}, {'totals': {'attempts': 'private-key'}}):
            with self.subTest(changes=changes):
                self.assertIsNone(validated_line(PREFIX + json.dumps({**base, **changes})))

    async def test_oversized_line_does_not_hide_the_next_valid_diagnostic(self):
        from runtime.event_leaderboards.diagnostics import ScanDiagnostics, MAX_LINE_BYTES
        lines = []
        with ScanDiagnostics(sink=lines.append):
            pass
        reader = asyncio.StreamReader()
        reader.feed_data(b'x' * (MAX_LINE_BYTES + 8000) + b'\n' + lines[-1].encode() + b'\n')
        reader.feed_eof()
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            await production_jobs._forward_diagnostics(reader, True)
        self.assertEqual(captured.getvalue().strip(), lines[-1])

    async def test_timeout_keeps_partial_diagnostics_and_kills_the_child(self):
        from runtime.event_leaderboards.diagnostics import ScanDiagnostics, PREFIX
        lines = []
        with ScanDiagnostics(sink=lines.append):
            pass
        stdout, stderr = asyncio.StreamReader(), asyncio.StreamReader()
        stdout.feed_eof()
        stderr.feed_data(lines[0].encode() + b'\n')
        # No EOF: simulates an in-flight process that reaches its deadline.
        class Child:
            pid, returncode = 12345, None
            async def wait(self):
                if self.returncode is not None:
                    return self.returncode
                await asyncio.Future()
        child = Child()
        child.stdout, child.stderr = stdout, stderr
        def kill(pid, sig):
            child.returncode = -sig
            stderr.feed_eof()
        captured = io.StringIO()
        with patch.object(production_jobs.asyncio, 'create_subprocess_exec', new=AsyncMock(return_value=child)), \
             patch.object(production_jobs.os, 'killpg', side_effect=kill) as killed, patch('sys.stdout', captured):
            with self.assertRaises(asyncio.TimeoutError):
                await production_jobs.run_command('event-leaderboards', 'unused.py', [], .01)
        killed.assert_called_once_with(12345, signal.SIGTERM)
        self.assertIn(PREFIX, captured.getvalue())
        self.assertIn('stopped reason=timeout', captured.getvalue())

    async def test_repeated_receipt_phases_are_aggregated_without_per_transaction_log_flood(self):
        from runtime.event_leaderboards.diagnostics import ScanDiagnostics, record_request
        lines = []
        with ScanDiagnostics(sink=lines.append) as diagnostic:
            for _ in range(300):
                with diagnostic.phase('receipt'):
                    record_request('eth_getTransactionReceipt', attempted=True, elapsed_ms=1)
        self.assertEqual(diagnostic.summary()['phases']['receipt']['calls'], 300)
        self.assertEqual(diagnostic.summary()['totals']['attempts'], 300)
        self.assertLessEqual(len(lines), 5)

    async def test_real_cli_help_keeps_a_successful_exit_status_in_diagnostics(self):
        from runtime.event_leaderboards.diagnostics import PREFIX
        from scripts.sync_event_leaderboards import main
        stderr, stdout = io.StringIO(), io.StringIO()
        with patch('sys.stderr', stderr), patch('sys.stdout', stdout):
            with self.assertRaises(SystemExit) as exit:
                main(['--help'])
        self.assertEqual(exit.exception.code, 0)
        records = [json.loads(line[len(PREFIX):]) for line in stderr.getvalue().splitlines() if line.startswith(PREFIX)]
        self.assertEqual(records[-1]['status'], 'ok')


if __name__ == "__main__":
    unittest.main()

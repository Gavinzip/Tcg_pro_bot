"""Owned background jobs for finite activities and encrypted Drive recovery."""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
import json
import math
import os
from pathlib import Path
import signal
import sys
import time

from runtime.event_leaderboards.diagnostics import MAX_LINE_BYTES, validated_line

_TASKS: dict[str, asyncio.Task] = {}
ROOT = Path(__file__).resolve().parents[1]


def enabled(name: str, default=False):
    return os.getenv(name, '1' if default else '0').strip().lower() in {'1', 'true', 'yes', 'on'}


def backup_remaining(interval):
    """Seconds until the last successful backup expires; failed/incomplete backups remain due."""
    path = Path(os.getenv('TCG_BACKUP_SOURCE', '/data')) / '.tcg-drive-backup' / 'status.json'
    try:
        status = json.loads(path.read_text())
        if not isinstance(status, dict) or status.get('phase') != 'complete':
            return 0
        completed_at = float(status['updatedAt'])
        if not math.isfinite(completed_at):
            raise ValueError('invalid backup completion time')
        return max(0, interval - (time.time() - completed_at))
    except (OSError, ValueError, KeyError, TypeError):
        return 0


def backup_due(interval):
    return backup_remaining(interval) == 0


async def _discard_output(stream):
    while await stream.read(8192):
        pass


async def _forward_diagnostics(stream, enabled):
    pending, oversized = bytearray(), False
    while True:
        chunk = await stream.read(8192)
        if not chunk:
            break
        for piece in chunk.splitlines(keepends=True):
            complete = piece.endswith(b'\n')
            if not oversized:
                pending.extend(piece)
                if len(pending) > MAX_LINE_BYTES:
                    pending.clear()
                    oversized = True
            if complete:
                if enabled and not oversized:
                    line = validated_line(pending.decode('utf-8', errors='replace').rstrip('\r\n'))
                    if line is not None:
                        print(line, flush=True)
                pending.clear()
                oversized = False


async def run_command(name, script, args, timeout):
    if name == 'event-leaderboards' and enabled('TCG_DRIVE_BACKUP_ENABLED', False):
        from runtime.job_priority import JobPriority
        deadline = time.monotonic() + timeout
        async with AsyncExitStack() as stack:
            await asyncio.wait_for(stack.enter_async_context(
                JobPriority(os.getenv('TCG_BACKUP_SOURCE', '/data')).scan()), timeout=timeout)
            return await _run_command(name, script, args, max(.001, deadline - time.monotonic()))
    return await _run_command(name, script, args, timeout)


async def _run_command(name, script, args, timeout):
    started = time.monotonic()
    print(f'[production-job] {name} started', flush=True)
    child = await asyncio.create_subprocess_exec(
        sys.executable, str(ROOT / 'scripts' / script), *args,
        cwd=ROOT, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    readers = [asyncio.create_task(_discard_output(child.stdout)),
               asyncio.create_task(_forward_diagnostics(child.stderr, name == 'event-leaderboards'))]
    completion = asyncio.gather(child.wait(), *readers)
    try:
        await asyncio.wait_for(completion, timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError) as error:
        if child.returncode is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(child.wait(), timeout=5)
            except asyncio.TimeoutError:
                os.killpg(child.pid, signal.SIGKILL)
                await child.wait()
        reason = 'cancelled' if isinstance(error, asyncio.CancelledError) else 'timeout'
        print(f'[production-job] {name} stopped reason={reason} duration_seconds={time.monotonic() - started:.3f}', flush=True)
        raise
    finally:
        for reader in readers:
            reader.cancel()
        await asyncio.gather(completion, *readers, return_exceptions=True)
    elapsed = time.monotonic() - started
    print(f'[production-job] {name} finished returncode={child.returncode} duration_seconds={elapsed:.3f}', flush=True)
    return child.returncode


async def periodic(name, script, args, interval, timeout, skip_until_due=False):
    while True:
        try:
            if skip_until_due:
                remaining = backup_remaining(interval)
                if remaining > 0:
                    await asyncio.sleep(remaining)
                    # A manual backup may have succeeded while this job was waiting.
                    continue
            await run_command(name, script, args, timeout)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Never print raw provider URLs, env values or child stderr.
            print(f'[production-job] {name} failed type={type(exc).__name__}', flush=True)
        await asyncio.sleep(interval)


def start_product_background_jobs():
    specs = []
    if enabled('EVENT_LEADERBOARD_SYNC_ENABLED', False):
        specs.append(('event-leaderboards', 'sync_event_leaderboards.py', [],
                      max(60, int(os.getenv('EVENT_LEADERBOARD_SYNC_INTERVAL_SECONDS', '600'))),
                      max(30, int(os.getenv('EVENT_LEADERBOARD_SYNC_TIMEOUT_SECONDS', '540'))), False))
    if enabled('TCG_DRIVE_BACKUP_ENABLED', False):
        specs.append(('drive-backup', 'backup_tcg_drive.py', ['backup'],
                      max(3600, int(os.getenv('TCG_DRIVE_BACKUP_INTERVAL_SECONDS', '86400'))),
                      max(60, int(os.getenv('TCG_BACKUP_TIMEOUT_SECONDS', '3600'))) + 60, True))
    for name, script, args, interval, timeout, due in specs:
        previous = _TASKS.get(name)
        if previous is None or previous.done():
            _TASKS[name] = asyncio.create_task(periodic(name, script, args, interval, timeout, due), name=name)

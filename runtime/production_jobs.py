"""Owned background jobs for finite activities and encrypted Drive recovery."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import signal
import sys
import time

_TASKS: dict[str, asyncio.Task] = {}
ROOT = Path(__file__).resolve().parents[1]


def enabled(name: str, default=False):
    return os.getenv(name, '1' if default else '0').strip().lower() in {'1', 'true', 'yes', 'on'}


def backup_due(interval):
    path = Path(os.getenv('TCG_BACKUP_SOURCE', '/data')) / '.tcg-drive-backup' / 'status.json'
    try:
        status = json.loads(path.read_text())
        return status.get('phase') != 'complete' or time.time() - float(status['updatedAt']) >= interval
    except (OSError, ValueError, KeyError, TypeError):
        return True


async def run_command(name, script, args, timeout):
    child = await asyncio.create_subprocess_exec(
        sys.executable, str(ROOT / 'scripts' / script), *args,
        cwd=ROOT, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        await asyncio.wait_for(child.communicate(), timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        if child.returncode is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(child.wait(), timeout=5)
            except asyncio.TimeoutError:
                os.killpg(child.pid, signal.SIGKILL)
                await child.wait()
        raise
    print(f'[production-job] {name} finished returncode={child.returncode}', flush=True)
    return child.returncode


async def periodic(name, script, args, interval, timeout, skip_until_due=False):
    while True:
        try:
            if not skip_until_due or backup_due(interval):
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

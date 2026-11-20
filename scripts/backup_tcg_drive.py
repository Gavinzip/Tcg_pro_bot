#!/usr/bin/env python3
"""Manual or scheduled TCG backup; never runs Git or automatic pruning."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runtime.drive_backup import DriveBackup, DriveBackupError


def main():
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['init', 'backup', 'verify', 'status'])
    p.add_argument('--snapshot')
    p.add_argument('--manifest-path')
    p.add_argument('--restore-target', type=Path)
    a = p.parse_args()
    backup = DriveBackup()
    try:
        if a.action == 'init': result = backup.initialize()
        elif a.action == 'backup': result = backup.backup()
        elif a.action == 'status': result = json.loads(backup.status_path.read_text())
        else:
            if not (a.snapshot and a.manifest_path and a.restore_target):
                p.error('verify requires --snapshot, --manifest-path and --restore-target')
            result = backup.verify(a.snapshot, a.manifest_path, a.restore_target)
        print(json.dumps(result))
    except Exception as e:
        print(json.dumps({'phase': 'failed', 'error': str(e) if isinstance(e, DriveBackupError) else 'backup_failed'}))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

"""Encrypted TCG filesystem backups through standard restic and rclone."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time
import uuid

from runtime.backup_snapshot import capture, validate_bulk, verify_restored


class DriveBackupError(RuntimeError):
    pass


class DriveBackup:
    def __init__(self, env=None, *, allow_local_repository=False):
        self.env = dict(os.environ if env is None else env)
        self.root = Path(self.env.get('TCG_BACKUP_SOURCE', '/data')).resolve()
        self.work = self.root / '.tcg-drive-backup'
        self.work.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.work.is_symlink():
            raise DriveBackupError('backup_work_directory_symlink')
        os.chmod(self.work, 0o700)
        self.repository = self.env.get('TCG_DRIVE_BACKUP_REPOSITORY', '')
        if not self.repository or (not allow_local_repository and not self.repository.startswith('rclone:gdrive:')):
            raise DriveBackupError('drive_repository_not_configured')
        if not self.env.get('TCG_DRIVE_BACKUP_PASSWORD'):
            raise DriveBackupError('backup_password_not_configured')
        self.env.update(RESTIC_REPOSITORY=self.repository,
                        RESTIC_PASSWORD=self.env['TCG_DRIVE_BACKUP_PASSWORD'],
                        RESTIC_CACHE_DIR=str(self.work / 'restic-cache'),
                        RCLONE_CONFIG=str(self.work / 'rclone.conf'))
        private_config = Path(self.env['RCLONE_CONFIG'])
        if not private_config.exists():
            private_config.touch(mode=0o600)
        os.chmod(private_config, 0o600)
        self.restic = self.env.get('TCG_BACKUP_RESTIC_BIN', 'restic')
        self.rclone = self.env.get('TCG_BACKUP_RCLONE_BIN', 'rclone')
        self.env['PATH'] = str(Path(self.rclone).resolve().parent) + os.pathsep + self.env.get('PATH', '') if '/' in self.rclone else self.env.get('PATH', '')
        self.status_path = self.work / 'status.json'
        self.timeout = max(30, int(self.env.get('TCG_BACKUP_TIMEOUT_SECONDS', '3600')))

    def run(self, args):
        try:
            child = subprocess.Popen([self.restic, *args], env=self.env,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     text=True, start_new_session=True)
            try:
                stdout, _stderr = child.communicate(timeout=self.timeout)
            except subprocess.TimeoutExpired as exc:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.communicate()
                raise DriveBackupError('restic_execution_timeout') from exc
        except OSError as exc:
            raise DriveBackupError('restic_execution_failed') from exc
        if child.returncode != 0:
            # stderr can contain credentials or private filenames. Never log it.
            raise DriveBackupError('restic_incomplete' if child.returncode == 3 else 'restic_failed')
        return stdout

    @contextmanager
    def locked(self):
        with (self.work / 'job.lock').open('a') as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise DriveBackupError('backup_already_running') from exc
            yield

    def status(self, value):
        value = {'updatedAt': time.time(), **value}
        temporary = self.status_path.with_suffix('.tmp')
        temporary.write_text(json.dumps(value), encoding='utf-8')
        os.chmod(temporary, 0o600)
        temporary.replace(self.status_path)
        return value

    def initialize(self):
        """Explicit new repository creation; never invoked after a backup error."""
        with self.locked():
            if self.repository.startswith('rclone:'):
                remote = self.repository.removeprefix('rclone:')
                # A Drive folder must already be created and independently verified.
                try:
                    result = subprocess.run([self.rclone, 'lsf', remote], env=self.env,
                                            text=True, capture_output=True, timeout=60)
                except (OSError, subprocess.TimeoutExpired) as exc:
                    raise DriveBackupError('drive_folder_check_failed') from exc
                if result.returncode or result.stdout.strip():
                    raise DriveBackupError('repository_not_verified_empty')
            elif Path(self.repository).exists() and any(Path(self.repository).iterdir()):
                raise DriveBackupError('repository_not_verified_empty')
            self.run(['init'])
            return self.status({'phase': 'initialized'})

    def cache_bulk_hashes(self, manifest):
        validate_bulk(manifest)
        signatures = {row['path']: row['signature'] for row in manifest['bulkSignatures']}
        cache = {row['path']: {'signature': signatures[row['path']], 'sha256': row['sha256']}
                 for row in manifest['files'] if row['kind'] == 'bulk'}
        temporary = self.work / 'bulk-hashes.tmp'
        temporary.write_text(json.dumps(cache), encoding='utf-8')
        os.chmod(temporary, 0o600)
        temporary.replace(self.work / 'bulk-hashes.json')

    def backup(self):
        with self.locked():
            stage = self.work / 'staging' / uuid.uuid4().hex
            stage.mkdir(parents=True, mode=0o700)
            self.status({'phase': 'capturing'})
            try:
                manifest = capture(self.root, stage)
                paths = [stage / 'manifest.json', stage / 'data']
                paths.extend(self.root / f['path'] for f in manifest['files'] if f['kind'] == 'bulk')
                filelist = stage / 'restic-files'
                filelist.write_bytes(b''.join(os.fsencode(p) + b'\0' for p in paths))
                self.status({'phase': 'uploading', 'files': len(manifest['files'])})
                output = self.run(['backup', '--json', '--host', 'tcg-pro', '--tag', 'tcg-drive-v1',
                                   '--group-by', 'host,tags', '--read-concurrency', '2', '--files-from-raw', str(filelist)])
                validate_bulk(manifest)
                summaries = [json.loads(line) for line in output.splitlines() if line.strip().startswith('{')]
                summary = next((row for row in reversed(summaries) if row.get('message_type') == 'summary'), None)
                if not summary or not summary.get('snapshot_id'):
                    raise DriveBackupError('snapshot_id_missing')
                # Independent remote readback of the encrypted snapshot manifest.
                readback = json.loads(self.run(['dump', summary['snapshot_id'], str(stage / 'manifest.json')]))
                if readback != manifest:
                    raise DriveBackupError('snapshot_manifest_readback_mismatch')
                self.cache_bulk_hashes(manifest)
                return self.status({'phase': 'complete', 'snapshotId': summary['snapshot_id'],
                                    'manifestPath': str(stage / 'manifest.json'),
                                    'files': len(manifest['files']),
                                    'bytes': sum(f['bytes'] for f in manifest['files']),
                                    'manifestReadback': True, 'fullRestoreVerified': False})
            except Exception as exc:
                self.status({'phase': 'failed', 'error': str(exc) if type(exc) in {DriveBackupError} else 'backup_failed'})
                raise
            finally:
                shutil.rmtree(stage)  # Only this invocation's announced scratch.

    def verify(self, snapshot_id, manifest_path, restore_target: Path):
        """Full initial acceptance, intentionally separate from the daily job."""
        with self.locked():
            restore_target = restore_target.resolve()
            if restore_target == self.root or self.root in restore_target.parents:
                raise DriveBackupError('restore_target_must_be_outside_production')
            if not restore_target.is_dir() or any(restore_target.iterdir()):
                raise DriveBackupError('restore_target_must_be_empty')
            manifest = json.loads(self.run(['dump', snapshot_id, manifest_path]))
            self.run(['restore', snapshot_id, '--target', str(restore_target)])
            result = verify_restored(manifest, restore_target)
            # Read all encrypted repository data during first acceptance, not daily.
            self.run(['check', '--read-data'])
            return {'snapshotId': snapshot_id, **result, 'repositoryReadData': True,
                    'fullRestoreVerified': True}

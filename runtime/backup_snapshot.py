"""Capture mutable files and live SQLite databases for an isolated restic backup."""
from __future__ import annotations

from contextlib import closing, nullcontext
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import sqlite3
import stat
import time
from urllib.parse import quote

# The product intentionally stores downloaded market images as hash.img.
BULK_SUFFIXES = {'.img', '.jpg', '.jpeg', '.png', '.webp', '.gif', '.mp4', '.mov', '.webm', '.woff', '.woff2'}
EXCLUDED = ('.tcg-drive-backup', 'lost+found', 'renaiss_sync/backup_repo', 'renaiss_sync/rankings')


class SnapshotError(RuntimeError):
    pass


class _Unrestricted:
    def checkpoint(self):
        pass


def _io(priority):
    return priority.backup_chunk() if priority is not None else nullcontext(_Unrestricted())


def sha256(path: Path, *, priority=None) -> str:
    h = hashlib.sha256()
    with _io(priority) as lease, path.open('rb') as stream:
        while True:
            lease.checkpoint()
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def signature(path: Path) -> tuple[int, int, int]:
    s = path.stat()
    return s.st_size, s.st_mtime_ns, s.st_ino


def excluded(relative: str) -> bool:
    return any(relative == p or relative.startswith(p + '/') for p in EXCLUDED)


def inventory(root: Path, *, priority=None) -> list[Path]:
    paths = []
    with _io(priority) as lease:
        for directory, dirs, files in os.walk(root, followlinks=False):
            lease.checkpoint()
            current = Path(directory)
            dirs[:] = sorted(d for d in dirs if not excluded((current / d).relative_to(root).as_posix()))
            for index, name in enumerate(dirs + sorted(files)):
                if index % 32 == 0:
                    lease.checkpoint()
                path = current / name
                relative = path.relative_to(root).as_posix()
                if excluded(relative):
                    continue
                if path.is_symlink():
                    raise SnapshotError('source_symlink_requires_review')
                if path.is_file():
                    paths.append(path)
                elif not path.is_dir():
                    raise SnapshotError('source_special_file_requires_review')
    return sorted(paths)


def is_sqlite(path: Path, *, priority=None) -> bool:
    with _io(priority), path.open('rb') as stream:
        return stream.read(16) == b'SQLite format 3\x00'


def sqlite_copy(source: Path, target: Path, *, priority=None) -> None:
    uri = 'file:' + quote(str(source), safe='/') + '?mode=ro'
    with _io(priority) as lease, closing(sqlite3.connect(uri, uri=True, timeout=10)) as original:
        with closing(sqlite3.connect(target)) as copy:
            # Pin one WAL read view across cooperative pauses. Otherwise each
            # foreground write can restart online backup from page one forever.
            # Rollback-journal sources keep their original short read steps.
            if priority is not None and original.execute('PRAGMA journal_mode').fetchone()[0] == 'wal':
                original.execute('BEGIN')
                original.execute('SELECT count(*) FROM sqlite_master').fetchone()
            # sqlite3_backup_step has returned before progress runs: source
            # read locks are released. Never suspend a live DB mid-step.
            original.backup(copy, pages=256, sleep=0.01,
                            progress=lambda *_: lease.checkpoint())
            copy.set_progress_handler(lambda: (lease.checkpoint() or 0), 1000)
            try:
                if copy.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                    raise SnapshotError('sqlite_snapshot_integrity_failed')
            finally:
                copy.set_progress_handler(None, 0)


def copy_file(source, target, *, priority=None):
    with _io(priority) as lease, source.open('rb') as reader, target.open('wb') as writer:
        while True:
            lease.checkpoint()
            block = reader.read(1024 * 1024)
            if not block:
                break
            writer.write(block)


def capture(root: Path, stage: Path, *, attempts: int = 3, priority=None) -> dict:
    """No production file is changed. Each SQLite database is an online snapshot.

    Other mutable files are copied, with a bounded retry if source files changed
    during capture. Bulk media are backed up in place and checked again after
    restic finishes. This is not a global transaction across independent stores.
    """
    root, stage = root.resolve(), stage.resolve()
    cache_path = root / '.tcg-drive-backup' / 'bulk-hashes.json'
    cache = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    if not isinstance(cache, dict):
        raise SnapshotError('invalid_bulk_hash_cache')
    data = stage / 'data'
    for attempt in range(attempts):
        if data.exists():
            shutil.rmtree(data)  # Only this invocation's announced scratch.
        data.mkdir(parents=True, mode=0o700)
        paths = inventory(root, priority=priority)
        scratch_bytes = sum(p.stat().st_size for p in paths if p.suffix.lower() not in BULK_SUFFIXES)
        if shutil.disk_usage(root).free < scratch_bytes + 512 * 1024 * 1024:
            raise SnapshotError('insufficient_backup_scratch_space')
        sqlite_paths = {p for p in paths if p.suffix.lower() in {'.db', '.sqlite', '.sqlite3'} and is_sqlite(p, priority=priority)}
        sidecars = {Path(str(p) + suffix) for p in sqlite_paths for suffix in ('-wal', '-shm', '-journal')}
        files, stable, bulk = [], {}, []
        for source in paths:
            if source in sidecars:
                continue
            relative = source.relative_to(root).as_posix()
            before = signature(source)
            mode = stat.S_IMODE(source.stat().st_mode)
            if source in sqlite_paths:
                target = data / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                sqlite_copy(source, target, priority=priority)
                kind = 'sqlite-online'
            elif source.suffix.lower() in BULK_SUFFIXES:
                target, kind = source, 'bulk'
                bulk.append({'path': relative, 'signature': list(before)})
            else:
                target = data / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                copy_file(source, target, priority=priority)
                kind = 'copied'
            if kind != 'bulk':
                os.chmod(target, mode)
            if kind != 'sqlite-online':
                stable[source] = before
            cached = cache.get(relative, {}) if kind == 'bulk' else {}
            digest = cached.get('sha256') if cached.get('signature') == list(before) else None
            if digest is not None and (not isinstance(digest, str) or not re.fullmatch('[0-9a-f]{64}', digest)):
                raise SnapshotError('invalid_bulk_hash_cache')
            files.append({'path': relative, 'kind': kind, 'bytes': target.stat().st_size,
                          'sha256': digest if digest is not None else sha256(target, priority=priority), 'mode': mode})
        try:
            unchanged = all(signature(p) == value for p, value in stable.items())
            after_paths = [p for p in inventory(root, priority=priority) if p not in sidecars]
            unchanged = unchanged and set(after_paths) == {root / f['path'] for f in files}
        except OSError:
            unchanged = False
        if unchanged:
            manifest = {'version': 1, 'capturedAt': time.time(), 'sourceRoot': str(root),
                        'stageRoot': str(stage), 'files': files, 'bulkSignatures': bulk,
                        'excluded': list(EXCLUDED),
                        'consistency': 'online SQLite per database; copied mutable files; stable media; no cross-store transaction'}
            (stage / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False), encoding='utf-8')
            return manifest
        if attempt + 1 == attempts:
            raise SnapshotError('source_changed_during_capture')
    raise SnapshotError('snapshot_not_created')


def validate_bulk(manifest: dict, *, priority=None) -> None:
    root = Path(manifest['sourceRoot'])
    try:
        with _io(priority) as lease:
            for row in manifest['bulkSignatures']:
                lease.checkpoint()
                if list(signature(root / row['path'])) != row['signature']:
                    raise SnapshotError('media_changed_during_backup')
    except OSError as exc:
        raise SnapshotError('media_changed_during_backup') from exc


def verify_restored(manifest: dict, restored: Path) -> dict:
    """Verify an isolated restore; never copy or overwrite production data."""
    total = 0
    for entry in manifest['files']:
        relative = Path(entry['path'])
        if relative.is_absolute() or '..' in relative.parts:
            raise SnapshotError('invalid_manifest_path')
        original_root = Path(manifest['sourceRoot']) if entry['kind'] == 'bulk' else Path(manifest['stageRoot']) / 'data'
        path = restored / original_root.relative_to('/') / relative
        if not path.is_file() or path.is_symlink() or path.stat().st_size != entry['bytes'] or sha256(path) != entry['sha256']:
            raise SnapshotError('restored_file_mismatch')
        if entry['kind'] == 'sqlite-online':
            with closing(sqlite3.connect('file:' + quote(str(path), safe='/') + '?mode=ro', uri=True)) as db:
                if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                    raise SnapshotError('restored_sqlite_integrity_failed')
        total += entry['bytes']
    return {'files': len(manifest['files']), 'bytes': total, 'allHashesMatch': True}

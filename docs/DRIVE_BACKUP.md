# TCG Drive recovery

Runtime data is backed up by restic 0.19.1 through rclone 1.74.2 to a dedicated
`tcg-pro` Drive repository. It never performs Git operations, fallback storage,
automatic expiry, or pruning. `scripts/install_backup_tools.py` pins official
Linux/amd64 archive checksums. Add `RUN python scripts/install_backup_tools.py`
after copying the source in the final Docker image. No Python dependency is added.
The container uses `tini` as PID 1 to reap orphaned rclone transport processes;
daily jobs must not accumulate zombie processes in the application supervisor.

## Scope and consistency

- Captures user preferences, runtime wallet/pack maps, market indexes and media,
  and the new activity state/snapshots.
- Excludes `renaiss_sync/backup_repo` (the retained Git clone), the retired
  `renaiss_sync/rankings` tree, `lost+found`, and `.tcg-drive-backup` work files.
  Exclusions do not delete anything from the production volume.
- Unchanged media reuse a private SHA-256 cache only when size, nanosecond mtime
  and inode all match a previously successful capture; media signatures are
  checked again after upload. This avoids re-reading all cached images daily.
  Production market images use `.img`; this format is explicitly classified as
  bulk media so the daily staging area does not duplicate the image collection.
- Mutable files are copied first and a changed source inventory aborts/retries
  capture. Media are read in place; changes during upload fail the job.
- `.db`, `.sqlite` and `.sqlite3` SQLite databases are captured with SQLite's
  online backup API and integrity checked. Their WAL/journal sidecars are not
  backed up as a competing database version.
- This is a consistent snapshot per database, not a global transaction spanning
  unrelated application stores. JSON writers must use atomic replacement.
- Filesystem recovery is distinct from source/deployment credentials. Keep the
  code revision and Railway configuration recoverable separately. Drive OAuth
  and the sole restic decryption password must never be inside this repository.

## Runtime configuration

Private server-only environment variables:

- `TCG_DRIVE_BACKUP_REPOSITORY`: dedicated `rclone:gdrive:.../tcg-pro` repository.
- `TCG_DRIVE_BACKUP_PASSWORD`: unique password, independently escrowed in the
  local ignored `.secrets/restic-password` (directory 0700, file 0600).
- `RCLONE_CONFIG_GDRIVE_*`: existing authorized Drive OAuth configuration.
- `TCG_BACKUP_SOURCE`: `/data` by default.
- `TCG_BACKUP_TIMEOUT_SECONDS`: 3600 by default; jobs fail visibly on timeout.
- Optional `TCG_BACKUP_RESTIC_BIN` / `TCG_BACKUP_RCLONE_BIN`: installed binaries.

Schedule `python scripts/backup_tcg_drive.py backup` once daily (86400 seconds),
inside the Bot service owning the volume. Do not add a second scheduler. A
filesystem `flock` excludes overlapping runs. Keep interactive query work on its
own workers. `status` reports capturing/uploading/complete/failed and the last
snapshot; a complete upload is not a full restore acceptance.
After a restart, the scheduler waits only the remaining time since the last
success. It rechecks the deadline after waiting, so a manual successful backup
does not cause another full day to be skipped. Failed jobs retain the configured
full-interval retry policy.

Explicit initial setup: create and independently verify the new empty Drive
folder, escrow its unique password, then run the `init` action once. Authentication
or repository errors never trigger automatic initialization.

Each job owns only `/data/.tcg-drive-backup/staging/<job-id>/`; its copied scratch
is removed after that job, while status and restic cache remain private. It never
removes original source files or existing Drive snapshots.

## Acceptance and restore

The backup manifest records paths, sizes, SHA-256 and source root mappings. After
initial upload, run `verify --snapshot <id> --manifest-path <path-from-status>
--restore-target <empty-isolated-directory>`. Verification reads the snapshot
manifest from Drive, restores all data to that isolated directory, checks every
captured hash and SQLite integrity, then runs `restic check --read-data`. It
refuses a production restore target. It does not overwrite the live volume.

To recover manually, use the encrypted manifest's `kind` mapping: `bulk` entries
come from `sourceRoot/<path>` in the isolated restore; other files come from
`stageRoot/data/<path>`. Review the restore before copying to a replacement
volume. Never replay already completed activity work blindly.

Acceptance evidence contains only counts, hashes, tool versions and snapshot
IDs, not credentials or user payloads. No `forget`/`prune` belongs in a normal job.

Official sources: https://restic.readthedocs.io/en/stable/040_backup.html and
https://rclone.org/drive/ . Restic exit 3 is an incomplete backup and is treated
as failure here.

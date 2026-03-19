#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

MORALIS_OWNERS_URL_TMPL = "https://deep-index.moralis.io/api/v2.2/nft/{contract}/{token_id}/owners"


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _now_tpe() -> datetime:
    return datetime.now(ZoneInfo("Asia/Taipei"))


def _run(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=False, text=True, capture_output=True)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def _json_load(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def _holder_key(row: dict[str, Any]) -> str:
    owner = str(row.get("owner_of") or "").strip().lower()
    token_id = str(row.get("token_id") or "").strip()
    block_number = str(row.get("block_number") or "").strip()
    amount = str(row.get("amount") or "").strip()
    return f"{owner}|{token_id}|{block_number}|{amount}"


@dataclass
class SyncConfig:
    app_env: str
    test_mode: bool
    data_dir: Path
    moralis_api_key: str
    moralis_chain: str
    contract: str
    token_id: str
    page_limit: int
    api_max_retries: int
    api_backoff_sec: float
    bootstrap_from_git: bool
    backup_git_enabled: bool
    backup_git_repo: str
    backup_git_branch: str
    backup_git_dir: Path
    webhook_url: str
    base_dir: Path

    @property
    def snapshot_latest_path(self) -> Path:
        return self.data_dir / "snapshots" / f"nft_{self.token_id}_holders.latest.json"

    @property
    def state_path(self) -> Path:
        return self.data_dir / "state" / f"nft_{self.token_id}_state.json"

    @property
    def history_path(self) -> Path:
        date_key = _now_tpe().strftime("%Y-%m-%d")
        return self.data_dir / "snapshots" / "history" / f"{date_key}.json"

    @property
    def status_path(self) -> Path:
        return self.data_dir / "state" / f"nft_{self.token_id}_status.json"

    @property
    def baseline_path(self) -> Path:
        return self.base_dir / f"nft_{self.token_id}_holders.json"

    @property
    def repo_dataset_dir(self) -> Path:
        return self.backup_git_dir / f"nft_{self.token_id}"


def load_config() -> SyncConfig:
    load_dotenv()
    base_dir = Path(__file__).resolve().parents[1]
    app_env = str(os.getenv("APP_ENV", "local")).strip().lower() or "local"
    default_data_dir = "/data/renaiss_sync" if app_env == "server" else "./data/renaiss_sync"
    data_dir = Path(os.getenv("SYNC_DATA_DIR", default_data_dir)).expanduser().resolve()

    test_mode = _env_bool("SYNC_TEST_MODE", False)
    backup_git_enabled = _env_bool("BACKUP_GIT_ENABLED", False)
    bootstrap_from_git = _env_bool("BOOTSTRAP_FROM_GIT", app_env == "server")

    cfg = SyncConfig(
        app_env=app_env,
        test_mode=test_mode,
        data_dir=data_dir,
        moralis_api_key=str(os.getenv("MORALIS_API_KEY", "")).strip(),
        moralis_chain=str(os.getenv("MORALIS_CHAIN", "bsc")).strip() or "bsc",
        contract=str(
            os.getenv("NFT_CONTRACT", "0x7d1b7db704d722295fbaa284008f526634673dbf")
        ).strip().lower(),
        token_id=str(os.getenv("NFT_TOKEN_ID", "13")).strip(),
        page_limit=max(1, min(100, int(os.getenv("MORALIS_PAGE_LIMIT", "20")))),
        api_max_retries=max(1, int(os.getenv("PROFILE_API_MAX_RETRIES", "4"))),
        api_backoff_sec=max(0.2, float(os.getenv("PROFILE_API_RETRY_BACKOFF_SEC", "0.8"))),
        bootstrap_from_git=bootstrap_from_git,
        backup_git_enabled=backup_git_enabled,
        backup_git_repo=str(os.getenv("BACKUP_GIT_REPO", "")).strip(),
        backup_git_branch=str(os.getenv("BACKUP_GIT_BRANCH", "main")).strip() or "main",
        backup_git_dir=Path(
            os.getenv("BACKUP_GIT_DIR", str(data_dir / "backup_repo"))
        ).expanduser().resolve(),
        webhook_url=str(
            os.getenv("SYNC_WEBHOOK_URL")
            or os.getenv("DISCORD_SYNC_WEBHOOK_URL")
            or os.getenv("DISCORD_WEBHOOK_URL")
            or ""
        ).strip(),
        base_dir=base_dir,
    )
    return cfg


def validate_config(cfg: SyncConfig, require_api_key: bool = True) -> None:
    if not cfg.contract.startswith("0x") or len(cfg.contract) != 42:
        raise RuntimeError("NFT_CONTRACT invalid")
    if require_api_key and not cfg.moralis_api_key:
        raise RuntimeError("MORALIS_API_KEY is required")
    if cfg.backup_git_enabled and not cfg.backup_git_repo:
        raise RuntimeError("BACKUP_GIT_REPO is required when BACKUP_GIT_ENABLED=1")


def send_webhook(cfg: SyncConfig, message: str, success: bool = True) -> None:
    if not cfg.webhook_url:
        return
    title = "NFT13 Sync Success" if success else "NFT13 Sync Failed"
    color = 0x2ECC71 if success else 0xE74C3C
    if cfg.test_mode:
        title = f"[TEST] {title}"
    payload = {
        "embeds": [
            {
                "title": title,
                "description": message[:4000],
                "color": color,
                "timestamp": _now_tpe().isoformat(),
            }
        ]
    }
    try:
        requests.post(cfg.webhook_url, json=payload, timeout=12)
    except Exception:
        pass


def write_status(
    cfg: SyncConfig,
    *,
    success: bool,
    trigger: str,
    message: str,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "updated_at": _now_tpe().isoformat(),
        "success": bool(success),
        "trigger": trigger,
        "message": message,
        "app_env": cfg.app_env,
        "test_mode": cfg.test_mode,
        "chain": cfg.moralis_chain,
        "token_id": cfg.token_id,
    }
    if extra:
        payload["extra"] = extra
    _atomic_write_json(cfg.status_path, payload)


def ensure_repo(cfg: SyncConfig) -> Path:
    repo_dir = cfg.backup_git_dir
    if (repo_dir / ".git").exists():
        return repo_dir
    repo_dir.parent.mkdir(parents=True, exist_ok=True)
    res = _run(["git", "clone", "--branch", cfg.backup_git_branch, cfg.backup_git_repo, str(repo_dir)])
    if res.returncode != 0:
        raise RuntimeError(f"git clone failed: {res.stderr.strip() or res.stdout.strip()}")
    return repo_dir


def git_pull(cfg: SyncConfig, repo_dir: Path) -> None:
    _run(["git", "fetch", "--all"], cwd=repo_dir)
    res = _run(["git", "pull", "--rebase", "origin", cfg.backup_git_branch], cwd=repo_dir)
    if res.returncode != 0:
        raise RuntimeError(f"git pull failed: {res.stderr.strip() or res.stdout.strip()}")


def git_push_snapshots(cfg: SyncConfig, commit_message: str) -> str:
    if cfg.test_mode:
        return "test-mode-skip-push"
    repo_dir = ensure_repo(cfg)
    git_pull(cfg, repo_dir)

    dataset_dir = cfg.repo_dataset_dir
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "history").mkdir(parents=True, exist_ok=True)
    (dataset_dir / "state").mkdir(parents=True, exist_ok=True)

    shutil.copy2(cfg.snapshot_latest_path, dataset_dir / cfg.snapshot_latest_path.name)
    shutil.copy2(cfg.history_path, dataset_dir / "history" / cfg.history_path.name)
    shutil.copy2(cfg.state_path, dataset_dir / "state" / cfg.state_path.name)

    _run(["git", "config", "user.name", os.getenv("BACKUP_GIT_USER_NAME", "tcg-pro-bot")], cwd=repo_dir)
    _run(["git", "config", "user.email", os.getenv("BACKUP_GIT_USER_EMAIL", "tcg-pro-bot@example.com")], cwd=repo_dir)
    _run(["git", "add", "."], cwd=repo_dir)
    status = _run(["git", "status", "--porcelain"], cwd=repo_dir)
    if status.returncode != 0:
        raise RuntimeError(f"git status failed: {status.stderr.strip() or status.stdout.strip()}")
    if not status.stdout.strip():
        head = _run(["git", "rev-parse", "--short", "HEAD"], cwd=repo_dir)
        return head.stdout.strip() or "no-change"

    commit = _run(["git", "commit", "-m", commit_message], cwd=repo_dir)
    if commit.returncode != 0:
        raise RuntimeError(f"git commit failed: {commit.stderr.strip() or commit.stdout.strip()}")
    push = _run(["git", "push", "origin", cfg.backup_git_branch], cwd=repo_dir)
    if push.returncode != 0:
        raise RuntimeError(f"git push failed: {push.stderr.strip() or push.stdout.strip()}")
    head = _run(["git", "rev-parse", "--short", "HEAD"], cwd=repo_dir)
    return head.stdout.strip() or "unknown"


def bootstrap_from_git(cfg: SyncConfig) -> bool:
    if not cfg.backup_git_enabled:
        return False
    repo_dir = ensure_repo(cfg)
    if not cfg.test_mode:
        git_pull(cfg, repo_dir)
    repo_latest = cfg.repo_dataset_dir / cfg.snapshot_latest_path.name
    if not repo_latest.exists():
        return False

    cfg.snapshot_latest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(repo_latest, cfg.snapshot_latest_path)

    repo_history_dir = cfg.repo_dataset_dir / "history"
    if repo_history_dir.exists():
        target_history_dir = cfg.history_path.parent
        target_history_dir.mkdir(parents=True, exist_ok=True)
        for p in repo_history_dir.glob("*.json"):
            shutil.copy2(p, target_history_dir / p.name)

    repo_state = cfg.repo_dataset_dir / "state" / cfg.state_path.name
    if repo_state.exists():
        cfg.state_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repo_state, cfg.state_path)
    return True


def initialize_from_baseline_if_needed(cfg: SyncConfig) -> bool:
    if cfg.snapshot_latest_path.exists():
        return False
    if not cfg.baseline_path.exists():
        return False
    data = _json_load(cfg.baseline_path)
    _atomic_write_json(cfg.snapshot_latest_path, data)
    _atomic_write_json(cfg.history_path, data)
    state = {"last_init_at": _now_tpe().isoformat(), "source": "baseline"}
    _atomic_write_json(cfg.state_path, state)
    return True


def fetch_moralis_page(cfg: SyncConfig, cursor: str | None = None) -> dict[str, Any]:
    url = MORALIS_OWNERS_URL_TMPL.format(contract=cfg.contract, token_id=cfg.token_id)
    params: dict[str, Any] = {"chain": cfg.moralis_chain, "limit": cfg.page_limit}
    if cursor:
        params["cursor"] = cursor
    headers = {"X-API-Key": cfg.moralis_api_key}

    last_err: Exception | None = None
    for attempt in range(1, cfg.api_max_retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            status = int(resp.status_code or 0)
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError("Moralis response is not object")
            return data
        except Exception as e:  # noqa: BLE001
            last_err = e
            status = None
            if isinstance(e, requests.RequestException) and getattr(e, "response", None) is not None:
                status = int(e.response.status_code or 0)
            retryable = status in (408, 409, 425, 429) or (status is not None and status >= 500) or status is None
            if retryable and attempt < cfg.api_max_retries:
                time.sleep(cfg.api_backoff_sec * (2 ** (attempt - 1)))
                continue
            break
    raise RuntimeError(f"Moralis owners request failed: {last_err}")


def incremental_sync(cfg: SyncConfig) -> dict[str, Any]:
    if not cfg.snapshot_latest_path.exists():
        raise RuntimeError(f"Latest snapshot missing: {cfg.snapshot_latest_path}")

    current = _json_load(cfg.snapshot_latest_path)
    old_holders = current.get("holders") or current.get("result") or []
    if not isinstance(old_holders, list):
        old_holders = []

    old_keys = {_holder_key(x) for x in old_holders if isinstance(x, dict)}
    new_rows: list[dict[str, Any]] = []
    seen_new_keys: set[str] = set()
    seen_cursors: set[str] = set()

    stop_on_duplicate = False
    pages = 0
    cursor: str | None = None
    last_page_data: dict[str, Any] = {}

    while True:
        page_data = fetch_moralis_page(cfg, cursor=cursor)
        pages += 1
        last_page_data = page_data

        rows = page_data.get("result") or []
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if not isinstance(row, dict):
                continue
            key = _holder_key(row)
            if not key:
                continue
            if key in old_keys:
                stop_on_duplicate = True
                break
            if key in seen_new_keys:
                continue
            seen_new_keys.add(key)
            new_rows.append(row)

        if stop_on_duplicate:
            break

        next_cursor = page_data.get("cursor")
        if not next_cursor:
            break
        next_cursor = str(next_cursor)
        if next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    merged_holders = new_rows + old_holders
    unique_holders: list[dict[str, Any]] = []
    seen_merged: set[str] = set()
    for row in merged_holders:
        if not isinstance(row, dict):
            continue
        key = _holder_key(row)
        if not key or key in seen_merged:
            continue
        seen_merged.add(key)
        unique_holders.append(row)

    total_records = int(last_page_data.get("total") or current.get("total_records") or len(unique_holders))
    total_records = max(total_records, len(unique_holders))

    out_payload = {
        "contract": cfg.contract,
        "token_id": cfg.token_id,
        "chain": cfg.moralis_chain,
        "total_records": total_records,
        "holders": unique_holders,
        "meta": {
            "updated_at": _now_tpe().isoformat(),
            "new_rows": len(new_rows),
            "pages_fetched": pages,
            "stop_on_duplicate": stop_on_duplicate,
            "page_limit": cfg.page_limit,
        },
    }
    _atomic_write_json(cfg.snapshot_latest_path, out_payload)
    _atomic_write_json(cfg.history_path, out_payload)
    _atomic_write_json(
        cfg.state_path,
        {
            "updated_at": _now_tpe().isoformat(),
            "new_rows": len(new_rows),
            "pages_fetched": pages,
            "stop_on_duplicate": stop_on_duplicate,
            "holder_count": len(unique_holders),
        },
    )

    return {
        "new_rows": len(new_rows),
        "pages_fetched": pages,
        "stop_on_duplicate": stop_on_duplicate,
        "holder_count": len(unique_holders),
        "total_records": total_records,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync NFT token holders incrementally and backup to git")
    p.add_argument("--trigger", default="manual")
    p.add_argument("--bootstrap-only", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config()
    validate_config(cfg, require_api_key=not args.bootstrap_only)

    cfg.data_dir.mkdir(parents=True, exist_ok=True)

    # 1) server startup: pull latest from git if enabled
    bootstrapped = False
    if cfg.bootstrap_from_git and cfg.backup_git_enabled:
        try:
            bootstrapped = bootstrap_from_git(cfg)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] bootstrap_from_git failed: {e}")

    # 2) if no snapshot yet, initialize from baseline
    initialized = initialize_from_baseline_if_needed(cfg)

    if args.bootstrap_only:
        bootstrap_commit = "skip"
        if initialized and cfg.backup_git_enabled:
            commit_message = (
                f"bootstrap nft_{cfg.token_id} {datetime.now(ZoneInfo('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            bootstrap_commit = git_push_snapshots(cfg, commit_message=commit_message)
        msg = (
            f"trigger={args.trigger} bootstrap_only=1 bootstrapped={bootstrapped} "
            f"initialized={initialized} latest={cfg.snapshot_latest_path} commit={bootstrap_commit}"
        )
        print(f"[OK] {msg}")
        write_status(
            cfg,
            success=True,
            trigger=args.trigger,
            message=msg,
            extra={
                "bootstrap_only": True,
                "bootstrapped": bootstrapped,
                "initialized": initialized,
                "latest_path": str(cfg.snapshot_latest_path),
                "commit": bootstrap_commit,
            },
        )
        send_webhook(cfg, msg, success=True)
        return 0

    result = incremental_sync(cfg)
    new_rows = int(result["new_rows"])

    commit_hash = "git-disabled"
    if cfg.backup_git_enabled and (new_rows > 0 or initialized):
        commit_message = (
            f"sync nft_{cfg.token_id} {datetime.now(ZoneInfo('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S')} "
            f"+{new_rows} trigger={args.trigger}"
        )
        commit_hash = git_push_snapshots(cfg, commit_message=commit_message)

    msg = (
        f"trigger={args.trigger} chain={cfg.moralis_chain} token_id={cfg.token_id} "
        f"new_rows={new_rows} pages={result['pages_fetched']} stop_on_duplicate={result['stop_on_duplicate']} "
        f"holders={result['holder_count']} total_records={result['total_records']} commit={commit_hash}"
    )
    print(f"[OK] {msg}")
    write_status(
        cfg,
        success=True,
        trigger=args.trigger,
        message=msg,
        extra={
            "new_rows": new_rows,
            "pages_fetched": result["pages_fetched"],
            "stop_on_duplicate": result["stop_on_duplicate"],
            "holder_count": result["holder_count"],
            "total_records": result["total_records"],
            "commit": commit_hash,
        },
    )
    send_webhook(cfg, msg, success=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:  # noqa: BLE001
        cfg = None
        try:
            cfg = load_config()
        except Exception:
            cfg = None
        err = f"{type(e).__name__}: {e}"
        print(f"[ERROR] {err}")
        if cfg is not None:
            trig = "unknown"
            try:
                trig = str(parse_args().trigger)
            except Exception:
                trig = "unknown"
            try:
                write_status(cfg, success=False, trigger=trig, message=err, extra=None)
            except Exception:
                pass
            send_webhook(cfg, err, success=False)
        raise

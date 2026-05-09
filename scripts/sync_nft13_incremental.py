#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

import requests
from dotenv import load_dotenv

TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TRANSFER_BATCH_TOPIC = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _safe_tzinfo(name: str):
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    # Fallback when container does not ship tzdata.
    return timezone(timedelta(hours=8))


TPE_TZ = _safe_tzinfo("Asia/Taipei")


def _now_tpe() -> datetime:
    return datetime.now(TPE_TZ)


def _now_tpe_str() -> str:
    return _now_tpe().strftime("%Y-%m-%d %H:%M:%S")


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
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def _parse_int(value: Any, default: int = 0) -> int:
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return default
    try:
        if text.lower().startswith("0x"):
            return int(text, 16)
        return int(text)
    except Exception:
        return default


def _clean_hex(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    return text


def _topic_to_address(topic: Any) -> str:
    text = _clean_hex(topic)
    if len(text) < 40:
        return ""
    return "0x" + text[-40:]


def _word_to_int(word: str) -> int:
    word = _clean_hex(word)
    if not word:
        return 0
    return int(word[-64:].rjust(64, "0"), 16)


def _split_words(data: Any) -> list[str]:
    text = _clean_hex(data)
    if not text:
        return []
    if len(text) % 64 != 0:
        text = text.rjust(((len(text) + 63) // 64) * 64, "0")
    return [text[i : i + 64] for i in range(0, len(text), 64)]


def _decode_transfer_single(data: Any) -> tuple[int, int] | None:
    words = _split_words(data)
    if len(words) < 2:
        return None
    return _word_to_int(words[0]), _word_to_int(words[1])


def _decode_dynamic_uint_array(words: list[str], offset_word: str) -> list[int]:
    offset_bytes = _word_to_int(offset_word)
    start = offset_bytes // 32
    if start < 0 or start >= len(words):
        return []
    length = _word_to_int(words[start])
    out: list[int] = []
    for i in range(length):
        idx = start + 1 + i
        if idx >= len(words):
            break
        out.append(_word_to_int(words[idx]))
    return out


def _decode_transfer_batch(data: Any) -> tuple[list[int], list[int]] | None:
    words = _split_words(data)
    if len(words) < 4:
        return None
    ids = _decode_dynamic_uint_array(words, words[0])
    values = _decode_dynamic_uint_array(words, words[1])
    if not ids or len(ids) != len(values):
        return None
    return ids, values


def _event_sort_key(row: dict[str, Any]) -> tuple[int, int, int]:
    return (
        _parse_int(row.get("blockNumber")),
        _parse_int(row.get("transactionIndex")),
        _parse_int(row.get("logIndex")),
    )


@dataclass
class SyncConfig:
    app_env: str
    test_mode: bool
    data_dir: Path
    bsc_api_url: str
    bsc_chain_id: int
    bsc_api_key: str
    contract: str
    token_id: str
    start_block: int
    block_chunk_size: int
    log_page_limit: int
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
    def token_id_int(self) -> int:
        return _parse_int(self.token_id)

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
    base_dir = Path(__file__).resolve().parents[1]
    load_dotenv(dotenv_path=base_dir / ".env")
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
        bsc_api_url=str(os.getenv("BSCSCAN_API_URL", "https://api.etherscan.io/v2/api")).strip()
        or "https://api.etherscan.io/v2/api",
        bsc_chain_id=max(1, int(os.getenv("BSCSCAN_CHAIN_ID", "56"))),
        bsc_api_key=str(os.getenv("BSCSCAN_API_KEY", "")).strip(),
        contract=str(
            os.getenv("NFT_CONTRACT", "0x7d1b7db704d722295fbaa284008f526634673dbf")
        ).strip().lower(),
        token_id=str(os.getenv("NFT_TOKEN_ID", "13")).strip(),
        start_block=max(0, int(os.getenv("NFT_SYNC_START_BLOCK", "72800000"))),
        block_chunk_size=max(100, int(os.getenv("NFT_SYNC_BLOCK_CHUNK", "200000"))),
        log_page_limit=max(1, min(1000, int(os.getenv("NFT_SYNC_LOG_PAGE_LIMIT", "1000")))),
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
    if _parse_int(cfg.token_id, -1) < 0:
        raise RuntimeError("NFT_TOKEN_ID invalid")
    if require_api_key and not cfg.bsc_api_key:
        raise RuntimeError("BSCSCAN_API_KEY is required")
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
        "provider": "bscscan_logs",
        "chain_id": cfg.bsc_chain_id,
        "contract": cfg.contract,
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


def _bsc_get(cfg: SyncConfig, params: dict[str, Any]) -> dict[str, Any]:
    query = dict(params)
    query.setdefault("chainid", cfg.bsc_chain_id)
    query["apikey"] = cfg.bsc_api_key
    last_err: Exception | None = None
    for attempt in range(1, cfg.api_max_retries + 1):
        try:
            resp = requests.get(cfg.bsc_api_url, params=query, timeout=30)
            status_code = int(resp.status_code or 0)
            if status_code == 429 or status_code >= 500:
                raise requests.HTTPError(f"HTTP {status_code}", response=resp)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError("BSC API response is not object")
            result = data.get("result")
            message = str(data.get("message") or "").lower()
            status = str(data.get("status") or "").strip()
            if status == "0" and isinstance(result, str):
                lowered = result.lower()
                if "no records" in lowered or "no transactions" in lowered:
                    return data
                if "rate limit" in lowered or "timeout" in lowered or "busy" in lowered:
                    raise RuntimeError(result)
                raise RuntimeError(result)
            if status == "0" and isinstance(result, list) and not result and "no records" not in message:
                raise RuntimeError(str(data.get("message") or "BSC API returned status 0"))
            return data
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < cfg.api_max_retries:
                time.sleep(cfg.api_backoff_sec * (2 ** (attempt - 1)))
                continue
            break
    raise RuntimeError(f"BSC API request failed: {last_err}")


def fetch_latest_block(cfg: SyncConfig) -> int:
    data = _bsc_get(cfg, {"module": "proxy", "action": "eth_blockNumber"})
    result = str(data.get("result") or "").strip()
    if not result:
        raise RuntimeError("eth_blockNumber missing result")
    return _parse_int(result)


def _fetch_logs_page(
    cfg: SyncConfig,
    *,
    from_block: int,
    to_block: int,
    topic0: str,
    page: int,
) -> list[dict[str, Any]]:
    data = _bsc_get(
        cfg,
        {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": int(from_block),
            "toBlock": int(to_block),
            "address": cfg.contract,
            "topic0": topic0,
            "page": int(page),
            "offset": int(cfg.log_page_limit),
        },
    )
    result = data.get("result")
    if isinstance(result, list):
        return [x for x in result if isinstance(x, dict)]
    if isinstance(result, str) and "no records" in result.lower():
        return []
    raise RuntimeError(f"getLogs invalid result: {result}")


def _fetch_logs_range_adaptive(
    cfg: SyncConfig,
    *,
    from_block: int,
    to_block: int,
    topic0: str,
) -> list[dict[str, Any]]:
    try:
        out: list[dict[str, Any]] = []
        page = 1
        while True:
            rows = _fetch_logs_page(
                cfg,
                from_block=from_block,
                to_block=to_block,
                topic0=topic0,
                page=page,
            )
            if not rows:
                break
            out.extend(rows)
            if len(rows) < cfg.log_page_limit:
                break
            page += 1
        return out
    except Exception:
        if from_block >= to_block:
            raise
        mid = (from_block + to_block) // 2
        return _fetch_logs_range_adaptive(
            cfg,
            from_block=from_block,
            to_block=mid,
            topic0=topic0,
        ) + _fetch_logs_range_adaptive(
            cfg,
            from_block=mid + 1,
            to_block=to_block,
            topic0=topic0,
        )


def _load_snapshot_balances(path: Path, token_id: str) -> tuple[dict[str, int], dict[str, int], int]:
    data = _json_load(path)
    rows = data.get("holders") or data.get("result") or data.get("rows") or []
    if not isinstance(rows, list):
        rows = []
    balances: dict[str, int] = {}
    holder_blocks: dict[str, int] = {}
    max_block = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_token = str(row.get("token_id") or row.get("tokenID") or row.get("tokenId") or token_id).strip()
        if row_token and row_token != str(token_id):
            continue
        owner = str(row.get("owner_of") or row.get("ownerAddress") or row.get("owner_address") or row.get("address") or row.get("wallet") or "").strip().lower()
        if not owner.startswith("0x") or len(owner) != 42:
            continue
        amount = _parse_int(row.get("amount"), 1)
        if amount <= 0:
            continue
        block_number = _parse_int(row.get("block_number") or row.get("blockNumber"))
        balances[owner] = balances.get(owner, 0) + amount
        if block_number > 0:
            holder_blocks[owner] = max(holder_blocks.get(owner, 0), block_number)
            max_block = max(max_block, block_number)
    return balances, holder_blocks, max_block


def _load_last_scanned_block(cfg: SyncConfig, snapshot_max_block: int) -> int:
    state = _json_load(cfg.state_path)
    for key in ("last_scanned_block", "end_block"):
        value = _parse_int(state.get(key))
        if value > 0:
            return value
    snapshot = _json_load(cfg.snapshot_latest_path)
    meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}
    for key in ("last_scanned_block", "end_block"):
        value = _parse_int(meta.get(key))
        if value > 0:
            return value
    return snapshot_max_block


def _apply_delta(balances: dict[str, int], holder_blocks: dict[str, int], owner: str, delta: int, block_number: int) -> None:
    owner = str(owner or "").strip().lower()
    if not owner.startswith("0x") or len(owner) != 42 or owner == ZERO_ADDRESS or delta == 0:
        return
    new_balance = int(balances.get(owner, 0)) + int(delta)
    if new_balance <= 0:
        balances.pop(owner, None)
        holder_blocks.pop(owner, None)
        return
    balances[owner] = new_balance
    if block_number > 0:
        holder_blocks[owner] = block_number


def _apply_transfer_event(
    cfg: SyncConfig,
    row: dict[str, Any],
    balances: dict[str, int],
    holder_blocks: dict[str, int],
) -> int:
    topics = row.get("topics") if isinstance(row.get("topics"), list) else []
    if len(topics) < 4:
        return 0
    topic0 = str(topics[0] or "").strip().lower()
    from_addr = _topic_to_address(topics[2])
    to_addr = _topic_to_address(topics[3])
    block_number = _parse_int(row.get("blockNumber"))
    matched = 0

    if topic0 == TRANSFER_SINGLE_TOPIC:
        decoded = _decode_transfer_single(row.get("data"))
        if decoded is None:
            return 0
        event_token_id, amount = decoded
        if event_token_id != cfg.token_id_int or amount <= 0:
            return 0
        _apply_delta(balances, holder_blocks, from_addr, -amount, block_number)
        _apply_delta(balances, holder_blocks, to_addr, amount, block_number)
        return 1

    if topic0 == TRANSFER_BATCH_TOPIC:
        decoded_batch = _decode_transfer_batch(row.get("data"))
        if decoded_batch is None:
            return 0
        ids, values = decoded_batch
        for event_token_id, amount in zip(ids, values):
            if event_token_id != cfg.token_id_int or amount <= 0:
                continue
            _apply_delta(balances, holder_blocks, from_addr, -amount, block_number)
            _apply_delta(balances, holder_blocks, to_addr, amount, block_number)
            matched += 1
    return matched


def _scan_and_apply_logs(
    cfg: SyncConfig,
    *,
    start_block: int,
    end_block: int,
    balances: dict[str, int],
    holder_blocks: dict[str, int],
) -> dict[str, int]:
    if start_block > end_block:
        return {"ranges": 0, "events": 0, "matched_events": 0}
    ranges = 0
    events = 0
    matched_events = 0
    cursor = max(0, start_block)
    while cursor <= end_block:
        chunk_end = min(end_block, cursor + cfg.block_chunk_size - 1)
        chunk_logs: list[dict[str, Any]] = []
        for topic in (TRANSFER_SINGLE_TOPIC, TRANSFER_BATCH_TOPIC):
            rows = _fetch_logs_range_adaptive(cfg, from_block=cursor, to_block=chunk_end, topic0=topic)
            chunk_logs.extend(rows)
        chunk_logs.sort(key=_event_sort_key)
        for row in chunk_logs:
            matched_events += _apply_transfer_event(cfg, row, balances, holder_blocks)
        ranges += 1
        events += len(chunk_logs)
        print(
            f"[PROGRESS] bsc_logs range={cursor}-{chunk_end} logs={len(chunk_logs)} matched={matched_events}",
            flush=True,
        )
        cursor = chunk_end + 1
    return {"ranges": ranges, "events": events, "matched_events": matched_events}


def _holders_payload(
    cfg: SyncConfig,
    *,
    balances: dict[str, int],
    holder_blocks: dict[str, int],
    start_block: int,
    end_block: int,
    latest_block: int,
    scan_stats: dict[str, int],
    full_rebuild: bool,
) -> dict[str, Any]:
    holders: list[dict[str, Any]] = []
    for owner in sorted(balances):
        amount = int(balances.get(owner, 0))
        if amount <= 0:
            continue
        holders.append(
            {
                "amount": str(amount),
                "token_id": str(cfg.token_id),
                "token_address": cfg.contract,
                "contract_type": "ERC1155",
                "owner_of": owner,
                "block_number": str(holder_blocks.get(owner, end_block if end_block > 0 else latest_block)),
                "name": None,
                "symbol": None,
                "metadata": None,
            }
        )
    return {
        "contract": cfg.contract,
        "token_id": cfg.token_id,
        "chain": "bsc",
        "provider": "bscscan_logs",
        "total_records": len(holders),
        "holders": holders,
        "meta": {
            "updated_at": _now_tpe().isoformat(),
            "full_rebuild": bool(full_rebuild),
            "start_block": int(start_block),
            "end_block": int(end_block),
            "latest_block": int(latest_block),
            "last_scanned_block": int(end_block),
            "block_chunk_size": int(cfg.block_chunk_size),
            "log_page_limit": int(cfg.log_page_limit),
            **scan_stats,
        },
    }


def sync_from_bsc_logs(cfg: SyncConfig, *, full_rebuild: bool = False) -> dict[str, Any]:
    latest_block = fetch_latest_block(cfg)
    if full_rebuild or not cfg.snapshot_latest_path.exists():
        balances: dict[str, int] = {}
        holder_blocks: dict[str, int] = {}
        start_block = cfg.start_block
    else:
        balances, holder_blocks, snapshot_max_block = _load_snapshot_balances(cfg.snapshot_latest_path, cfg.token_id)
        last_scanned = _load_last_scanned_block(cfg, snapshot_max_block)
        start_block = max(cfg.start_block, last_scanned + 1)

    end_block = latest_block
    scan_stats = _scan_and_apply_logs(
        cfg,
        start_block=start_block,
        end_block=end_block,
        balances=balances,
        holder_blocks=holder_blocks,
    )
    payload = _holders_payload(
        cfg,
        balances=balances,
        holder_blocks=holder_blocks,
        start_block=start_block,
        end_block=end_block,
        latest_block=latest_block,
        scan_stats=scan_stats,
        full_rebuild=full_rebuild,
    )
    _atomic_write_json(cfg.snapshot_latest_path, payload)
    _atomic_write_json(cfg.history_path, payload)
    state = {
        "updated_at": _now_tpe().isoformat(),
        "provider": "bscscan_logs",
        "holder_count": len(payload["holders"]),
        "total_records": int(payload["total_records"]),
        "start_block": int(start_block),
        "end_block": int(end_block),
        "latest_block": int(latest_block),
        "last_scanned_block": int(end_block),
        "full_rebuild": bool(full_rebuild),
        **scan_stats,
    }
    _atomic_write_json(cfg.state_path, state)
    return state


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync NFT token holders from BSC ERC1155 logs and backup to git")
    p.add_argument("--trigger", default="manual")
    p.add_argument("--bootstrap-only", action="store_true")
    p.add_argument("--full-rebuild", action="store_true", help="Replay ERC1155 logs from NFT_SYNC_START_BLOCK")
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
            commit_message = f"bootstrap nft_{cfg.token_id} {_now_tpe_str()}"
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

    result = sync_from_bsc_logs(cfg, full_rebuild=bool(args.full_rebuild))
    matched_events = int(result.get("matched_events") or 0)
    holder_count = int(result.get("holder_count") or 0)

    commit_hash = "git-disabled"
    if cfg.backup_git_enabled and (matched_events > 0 or initialized or args.full_rebuild):
        commit_message = (
            f"sync nft_{cfg.token_id} {_now_tpe_str()} "
            f"matched={matched_events} trigger={args.trigger}"
        )
        commit_hash = git_push_snapshots(cfg, commit_message=commit_message)

    msg = (
        f"trigger={args.trigger} provider=bscscan_logs chain_id={cfg.bsc_chain_id} token_id={cfg.token_id} "
        f"blocks={result['start_block']}-{result['end_block']} logs={result['events']} "
        f"matched={matched_events} holders={holder_count} total_records={result['total_records']} "
        f"commit={commit_hash}"
    )
    print(f"[OK] {msg}")
    write_status(
        cfg,
        success=True,
        trigger=args.trigger,
        message=msg,
        extra={**result, "commit": commit_hash},
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

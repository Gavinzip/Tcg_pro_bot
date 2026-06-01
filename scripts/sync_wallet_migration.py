#!/usr/bin/env python3
"""Sync old-wallet -> new-wallet mappings from verified migration transactions.

This intentionally stays independent from profile/ranking logic.  pack_rank reads
the produced map and merges counts; if the map is missing or empty, pack_rank
continues with raw wallet counts.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from runtime.wallet_migration import load_wallet_migration_map
from sync_nft13_incremental import (  # type: ignore
    TRANSFER_BATCH_TOPIC,
    TRANSFER_SINGLE_TOPIC,
    ZERO_ADDRESS,
    _atomic_write_json,
    _decode_transfer_batch,
    _decode_transfer_single,
    _event_sort_key,
    _fetch_logs_range_adaptive,
    _parse_int,
    _safe_tzinfo,
    _topic_to_address,
    fetch_latest_block,
)

ERC721_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
DEFAULT_START_BLOCK = 99465073
DEFAULT_HELPER_CONTRACT = "0x2e737d552b3c601ada4fcd167bfbd8d4e1043b2c"
DEFAULT_HELPER_MIGRATION_TOPIC = "0xa8842895c03659cf75d4e5e2202ace2ef1981a77551fc02961bc661af0950830"


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _run(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=False, text=True, capture_output=True)


def _json_load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _normalize_address(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x") and len(text) == 42:
        return text
    return ""


def _now_tpe(tzinfo) -> datetime:
    return datetime.now(tzinfo)


@dataclass
class LogConfig:
    bsc_api_url: str
    bsc_chain_id: int
    bsc_api_key: str
    contract: str
    log_page_limit: int
    api_max_retries: int
    api_backoff_sec: float


@dataclass
class WalletMigrationConfig:
    trigger: str
    data_dir: Path
    map_path: Path
    status_path: Path
    tz_name: str
    tzinfo: Any
    bsc_api_url: str
    bsc_chain_id: int
    bsc_api_key: str
    sbt_contract: str
    sbt_token_id: int
    card_contract: str
    helper_contract: str
    helper_event_topic: str
    include_card_evidence: bool
    start_block: int
    block_chunk_size: int
    log_page_limit: int
    api_max_retries: int
    api_backoff_sec: float
    backup_git_enabled: bool
    backup_git_repo: str
    backup_git_branch: str
    backup_git_dir: Path
    bootstrap_from_git: bool

    @property
    def repo_state_dir(self) -> Path:
        return self.backup_git_dir / "state"

    @property
    def repo_map_path(self) -> Path:
        return self.repo_state_dir / "wallet_migration_map.json"

    @property
    def repo_status_path(self) -> Path:
        return self.repo_state_dir / "wallet_migration_status.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync wallet migration old->new map")
    parser.add_argument("--trigger", default="manual")
    parser.add_argument("--data-dir", default="")
    parser.add_argument("--bootstrap-only", action="store_true")
    parser.add_argument("--full-rebuild", action="store_true")
    return parser.parse_args()


def load_config(args: argparse.Namespace) -> WalletMigrationConfig:
    app_env = str(os.getenv("APP_ENV", "local")).strip().lower() or "local"
    default_data_dir = "/data/renaiss_sync" if app_env == "server" else "./data/renaiss_sync"
    data_dir_raw = str(args.data_dir or "").strip() or str(os.getenv("SYNC_DATA_DIR", default_data_dir)).strip()
    data_dir = Path(data_dir_raw).expanduser().resolve()
    tz_name = str(os.getenv("WALLET_MIGRATION_SYNC_TZ", os.getenv("RANK_SYNC_TZ", "Asia/Taipei"))).strip() or "Asia/Taipei"
    tzinfo = _safe_tzinfo(tz_name)
    explicit_map_path = str(os.getenv("WALLET_MIGRATION_MAP_PATH", "")).strip()
    map_path = Path(explicit_map_path).expanduser() if explicit_map_path else data_dir / "state" / "wallet_migration_map.json"
    if not map_path.is_absolute():
        map_path = (PROJECT_ROOT / map_path).resolve()
    else:
        map_path = map_path.resolve()

    bsc_api_url = str(
        os.getenv("BSCSCAN_API_URL", os.getenv("ONCHAIN_API_URL", "https://api.etherscan.io/v2/api"))
    ).strip() or "https://api.etherscan.io/v2/api"
    backup_git_enabled = _env_bool("BACKUP_GIT_ENABLED", False)
    bootstrap_from_git = _env_bool("BOOTSTRAP_FROM_GIT", app_env == "server")
    backup_git_dir = Path(os.getenv("BACKUP_GIT_DIR", str(data_dir / "backup_repo"))).expanduser().resolve()

    return WalletMigrationConfig(
        trigger=str(args.trigger or "manual"),
        data_dir=data_dir,
        map_path=map_path,
        status_path=data_dir / "state" / "wallet_migration_status.json",
        tz_name=tz_name,
        tzinfo=tzinfo,
        bsc_api_url=bsc_api_url,
        bsc_chain_id=max(1, int(os.getenv("BSCSCAN_CHAIN_ID", os.getenv("ONCHAIN_CHAIN_ID", "56")))),
        bsc_api_key=str(os.getenv("BSCSCAN_API_KEY", "")).strip(),
        sbt_contract=str(
            os.getenv("WALLET_MIGRATION_SBT_CONTRACT", os.getenv("NFT_CONTRACT", "0x7d1b7db704d722295fbaa284008f526634673dbf"))
        ).strip().lower(),
        sbt_token_id=max(0, int(os.getenv("WALLET_MIGRATION_SBT_TOKEN_ID", os.getenv("NFT_TOKEN_ID", "13")))),
        card_contract=str(os.getenv("WALLET_MIGRATION_CARD_CONTRACT", "0xf8646a3ca093e97bb404c3b25e675c0394dd5b30")).strip().lower(),
        helper_contract=str(os.getenv("WALLET_MIGRATION_HELPER_CONTRACT", DEFAULT_HELPER_CONTRACT)).strip().lower(),
        helper_event_topic=str(os.getenv("WALLET_MIGRATION_HELPER_TOPIC", DEFAULT_HELPER_MIGRATION_TOPIC)).strip().lower(),
        include_card_evidence=_env_bool("WALLET_MIGRATION_INCLUDE_CARD_EVIDENCE", False),
        start_block=max(0, int(os.getenv("WALLET_MIGRATION_START_BLOCK", str(DEFAULT_START_BLOCK)))),
        block_chunk_size=max(100, int(os.getenv("WALLET_MIGRATION_BLOCK_CHUNK", "200000"))),
        log_page_limit=max(1, min(1000, int(os.getenv("WALLET_MIGRATION_LOG_PAGE_LIMIT", "1000")))),
        api_max_retries=max(1, int(os.getenv("PROFILE_API_MAX_RETRIES", "4"))),
        api_backoff_sec=max(0.2, float(os.getenv("PROFILE_API_RETRY_BACKOFF_SEC", "0.8"))),
        backup_git_enabled=backup_git_enabled,
        backup_git_repo=str(os.getenv("BACKUP_GIT_REPO", "")).strip(),
        backup_git_branch=str(os.getenv("BACKUP_GIT_BRANCH", "main")).strip() or "main",
        backup_git_dir=backup_git_dir,
        bootstrap_from_git=bootstrap_from_git,
    )


def validate_config(cfg: WalletMigrationConfig, *, require_api_key: bool = True) -> None:
    if require_api_key and not cfg.bsc_api_key:
        raise RuntimeError("BSCSCAN_API_KEY is required for wallet migration sync")
    for name, addr in (
        ("sbt_contract", cfg.sbt_contract),
        ("card_contract", cfg.card_contract),
        ("helper_contract", cfg.helper_contract),
    ):
        if not addr.startswith("0x") or len(addr) != 42:
            raise RuntimeError(f"{name} invalid")
    if not cfg.helper_event_topic.startswith("0x") or len(cfg.helper_event_topic) != 66:
        raise RuntimeError("helper_event_topic invalid")
    if cfg.backup_git_enabled and not cfg.backup_git_repo:
        raise RuntimeError("BACKUP_GIT_REPO is required when BACKUP_GIT_ENABLED=1")


def _log_cfg(cfg: WalletMigrationConfig, contract: str) -> LogConfig:
    return LogConfig(
        bsc_api_url=cfg.bsc_api_url,
        bsc_chain_id=cfg.bsc_chain_id,
        bsc_api_key=cfg.bsc_api_key,
        contract=contract,
        log_page_limit=cfg.log_page_limit,
        api_max_retries=cfg.api_max_retries,
        api_backoff_sec=cfg.api_backoff_sec,
    )


def _bsc_get_logs(cfg: WalletMigrationConfig, contract: str, from_block: int, to_block: int, topic0: str) -> list[dict[str, Any]]:
    log_cfg = _log_cfg(cfg, contract)
    return _fetch_logs_range_adaptive(log_cfg, from_block=from_block, to_block=to_block, topic0=topic0)


def _is_target_sbt_mint(cfg: WalletMigrationConfig, row: dict[str, Any]) -> tuple[bool, str]:
    topics = row.get("topics") if isinstance(row.get("topics"), list) else []
    if len(topics) < 4:
        return False, ""
    from_addr = _topic_to_address(topics[2]).lower()
    to_addr = _topic_to_address(topics[3]).lower()
    if from_addr != ZERO_ADDRESS or not _normalize_address(to_addr):
        return False, ""
    topic0 = str(topics[0] or "").strip().lower()
    if topic0 == TRANSFER_SINGLE_TOPIC:
        decoded = _decode_transfer_single(row.get("data"))
        if decoded and decoded[0] == cfg.sbt_token_id and decoded[1] > 0:
            return True, to_addr
        return False, ""
    if topic0 == TRANSFER_BATCH_TOPIC:
        decoded_batch = _decode_transfer_batch(row.get("data"))
        if decoded_batch is None:
            return False, ""
        ids, values = decoded_batch
        if any(token_id == cfg.sbt_token_id and amount > 0 for token_id, amount in zip(ids, values)):
            return True, to_addr
    return False, ""


def _target_sbt_transfers(cfg: WalletMigrationConfig, row: dict[str, Any]) -> list[tuple[str, str, int]]:
    topics = row.get("topics") if isinstance(row.get("topics"), list) else []
    if len(topics) < 4:
        return []
    topic0 = str(topics[0] or "").strip().lower()
    from_addr = _topic_to_address(topics[2]).lower()
    to_addr = _topic_to_address(topics[3]).lower()
    if not _normalize_address(from_addr) or not _normalize_address(to_addr):
        return []

    if topic0 == TRANSFER_SINGLE_TOPIC:
        decoded = _decode_transfer_single(row.get("data"))
        if decoded is None:
            return []
        token_id, amount = decoded
        if token_id == cfg.sbt_token_id and amount > 0:
            return [(from_addr, to_addr, int(amount))]
        return []

    if topic0 == TRANSFER_BATCH_TOPIC:
        decoded_batch = _decode_transfer_batch(row.get("data"))
        if decoded_batch is None:
            return []
        ids, values = decoded_batch
        out: list[tuple[str, str, int]] = []
        for token_id, amount in zip(ids, values):
            if token_id == cfg.sbt_token_id and amount > 0:
                out.append((from_addr, to_addr, int(amount)))
        return out

    return []


def _topic_address(topic: Any) -> str:
    text = str(topic or "").strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    if len(text) < 40:
        return ""
    return _normalize_address("0x" + text[-40:])


def _helper_migration_pair(row: dict[str, Any]) -> tuple[str, str] | None:
    topics = row.get("topics") if isinstance(row.get("topics"), list) else []
    if len(topics) < 4:
        return None
    old_wallet = _topic_address(topics[2])
    new_wallet = _topic_address(topics[3])
    if not old_wallet or not new_wallet:
        return None
    if old_wallet == ZERO_ADDRESS or new_wallet == ZERO_ADDRESS or old_wallet == new_wallet:
        return None
    return old_wallet, new_wallet


def _scan_confirmed_migrations(cfg: WalletMigrationConfig, *, from_block: int, to_block: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if from_block > to_block:
        return [], {
            "ranges": 0,
            "sbt_logs": 0,
            "sbt_token_events": 0,
            "sbt_mints": 0,
            "helper_logs": 0,
            "helper_candidates": 0,
            "helper_confirmed_pairs": 0,
            "card_logs": 0,
            "card_confirmed_pairs": 0,
            "confirmed_pairs": 0,
        }

    sbt_logs: list[dict[str, Any]] = []
    helper_logs: list[dict[str, Any]] = []
    cursor = from_block
    ranges = 0
    while cursor <= to_block:
        chunk_end = min(to_block, cursor + cfg.block_chunk_size - 1)
        for topic in (TRANSFER_SINGLE_TOPIC, TRANSFER_BATCH_TOPIC):
            sbt_logs.extend(_bsc_get_logs(cfg, cfg.sbt_contract, cursor, chunk_end, topic))
        helper_logs.extend(_bsc_get_logs(cfg, cfg.helper_contract, cursor, chunk_end, cfg.helper_event_topic))
        ranges += 1
        cursor = chunk_end + 1
    sbt_logs.sort(key=_event_sort_key)
    helper_logs.sort(key=_event_sort_key)

    migration_txs: set[str] = set()
    new_by_tx: dict[str, set[str]] = defaultdict(set)
    mint_blocks: dict[tuple[str, str], int] = {}
    sbt_deltas_by_tx: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    sbt_token_events = 0
    for row in sbt_logs:
        tx_hash = str(row.get("transactionHash") or "").strip().lower()
        if not tx_hash:
            continue
        block_number = _parse_int(row.get("blockNumber"))
        for from_addr, to_addr, amount in _target_sbt_transfers(cfg, row):
            sbt_token_events += 1
            if from_addr != ZERO_ADDRESS:
                sbt_deltas_by_tx[tx_hash][from_addr] -= int(amount)
            if to_addr != ZERO_ADDRESS:
                sbt_deltas_by_tx[tx_hash][to_addr] += int(amount)

        ok, new_wallet = _is_target_sbt_mint(cfg, row)
        if not ok:
            continue
        migration_txs.add(tx_hash)
        new_by_tx[tx_hash].add(new_wallet)
        mint_blocks[(tx_hash, new_wallet)] = block_number

    rows_by_old: dict[str, dict[str, Any]] = {}

    def _candidate_priority(row: dict[str, Any]) -> tuple[int, int]:
        source = str(row.get("source") or "")
        priority = 2 if source.startswith("sbt13_helper") else 1
        return _parse_int(row.get("block")), priority

    def _add_candidate(row: dict[str, Any]) -> None:
        old_addr = _normalize_address(row.get("old"))
        new_addr = _normalize_address(row.get("new"))
        if not old_addr or not new_addr or old_addr == new_addr:
            return
        prev = rows_by_old.get(old_addr)
        if prev is None or _candidate_priority(row) >= _candidate_priority(prev):
            rows_by_old[old_addr] = row

    helper_candidates = 0
    for row in helper_logs:
        tx_hash = str(row.get("transactionHash") or "").strip().lower()
        if not tx_hash:
            continue
        pair = _helper_migration_pair(row)
        if pair is None:
            continue
        helper_candidates += 1
        old_wallet, new_wallet = pair
        deltas = sbt_deltas_by_tx.get(tx_hash) or {}
        if int(deltas.get(new_wallet, 0)) <= 0:
            continue
        migration_txs.add(tx_hash)
        new_by_tx[tx_hash].add(new_wallet)
        block_number = _parse_int(row.get("blockNumber"))
        mint_blocks.setdefault((tx_hash, new_wallet), block_number)
        _add_candidate(
            {
                "old": old_wallet,
                "new": new_wallet,
                "tx": tx_hash,
                "block": block_number,
                "cards": 0,
                "sbt_old_delta": int(deltas.get(old_wallet, 0)),
                "sbt_new_delta": int(deltas.get(new_wallet, 0)),
                "source": "sbt13_helper_event_and_sbt_mint_same_tx",
            }
        )

    card_logs: list[dict[str, Any]] = []
    if cfg.include_card_evidence and migration_txs:
        cursor = from_block
        while cursor <= to_block:
            chunk_end = min(to_block, cursor + cfg.block_chunk_size - 1)
            card_logs.extend(_bsc_get_logs(cfg, cfg.card_contract, cursor, chunk_end, ERC721_TRANSFER_TOPIC))
            cursor = chunk_end + 1
    card_logs.sort(key=_event_sort_key)

    card_pairs_by_tx: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for row in card_logs:
        tx_hash = str(row.get("transactionHash") or "").strip().lower()
        if tx_hash not in migration_txs:
            continue
        topics = row.get("topics") if isinstance(row.get("topics"), list) else []
        if len(topics) < 3:
            continue
        old_wallet = _topic_address(topics[1])
        new_wallet = _topic_address(topics[2])
        if not old_wallet or not new_wallet or old_wallet == ZERO_ADDRESS:
            continue
        if new_wallet not in new_by_tx.get(tx_hash, set()):
            continue
        card_pairs_by_tx[tx_hash].append((old_wallet, new_wallet))

    for tx_hash, pairs in card_pairs_by_tx.items():
        if not pairs:
            continue
        pair, count = Counter(pairs).most_common(1)[0]
        old_wallet, new_wallet = pair
        _add_candidate(
            {
                "old": old_wallet,
                "new": new_wallet,
                "tx": tx_hash,
                "block": int(mint_blocks.get((tx_hash, new_wallet), 0)),
                "cards": int(count),
                "source": "sbt13_mint_and_card_transfer_same_tx",
            }
        )

    rows = sorted(rows_by_old.values(), key=lambda x: (int(x.get("block") or 0), str(x.get("old") or ""), str(x.get("new") or "")))
    stats = {
        "ranges": int(ranges),
        "sbt_logs": len(sbt_logs),
        "sbt_token_events": int(sbt_token_events),
        "sbt_mints": sum(len(v) for v in new_by_tx.values()),
        "helper_logs": len(helper_logs),
        "helper_candidates": int(helper_candidates),
        "helper_confirmed_pairs": sum(1 for x in rows if str(x.get("source") or "").startswith("sbt13_helper")),
        "card_logs": len(card_logs),
        "card_confirmed_pairs": len(card_pairs_by_tx),
        "confirmed_pairs": len(rows),
    }
    return rows, stats


def _merge_mapping_payload(cfg: WalletMigrationConfig, new_rows: list[dict[str, Any]], *, last_scanned_block: int) -> dict[str, Any]:
    existing_payload = _json_load(cfg.map_path)
    existing_map = load_wallet_migration_map(str(cfg.map_path))
    rows_by_old: dict[str, dict[str, Any]] = {}

    if isinstance(existing_payload.get("mappings"), list):
        for row in existing_payload.get("mappings") or []:
            if not isinstance(row, dict):
                continue
            old_addr = _normalize_address(row.get("old") or row.get("old_wallet") or row.get("from"))
            new_addr = _normalize_address(row.get("new") or row.get("new_wallet") or row.get("to"))
            if not old_addr or not new_addr or old_addr == new_addr:
                continue
            rows_by_old[old_addr] = {
                "old": old_addr,
                "new": new_addr,
                "block": _parse_int(row.get("block")),
                "tx": str(row.get("tx") or "").strip().lower(),
                "cards": _parse_int(row.get("cards")),
                "source": str(row.get("source") or "existing"),
                **(
                    {"sbt_old_delta": _parse_int(row.get("sbt_old_delta"))}
                    if row.get("sbt_old_delta") is not None
                    else {}
                ),
                **(
                    {"sbt_new_delta": _parse_int(row.get("sbt_new_delta"))}
                    if row.get("sbt_new_delta") is not None
                    else {}
                ),
            }
    else:
        for old_addr, new_addr in existing_map.items():
            rows_by_old[old_addr] = {
                "old": old_addr,
                "new": new_addr,
                "block": 0,
                "tx": "",
                "cards": 0,
                "source": "existing_flat_map",
            }

    for row in new_rows:
        old_addr = _normalize_address(row.get("old"))
        new_addr = _normalize_address(row.get("new"))
        if not old_addr or not new_addr or old_addr == new_addr:
            continue
        prev = rows_by_old.get(old_addr)
        row_block = _parse_int(row.get("block"))
        if prev is None or row_block >= _parse_int(prev.get("block")):
            normalized_row = {
                "old": old_addr,
                "new": new_addr,
                "block": row_block,
                "tx": str(row.get("tx") or "").strip().lower(),
                "cards": _parse_int(row.get("cards")),
                "source": str(row.get("source") or "sbt13_mint_and_card_transfer_same_tx"),
            }
            if row.get("sbt_old_delta") is not None:
                normalized_row["sbt_old_delta"] = _parse_int(row.get("sbt_old_delta"))
            if row.get("sbt_new_delta") is not None:
                normalized_row["sbt_new_delta"] = _parse_int(row.get("sbt_new_delta"))
            rows_by_old[old_addr] = normalized_row

    mappings = sorted(rows_by_old.values(), key=lambda x: (str(x.get("old") or ""), str(x.get("new") or "")))
    now = _now_tpe(cfg.tzinfo).isoformat()
    return {
        "version": 1,
        "updated_at": now,
        "source": "bsc_logs:sbt13_helper_event_plus_sbt_mint",
        "sbt_contract": cfg.sbt_contract,
        "sbt_token_id": str(cfg.sbt_token_id),
        "card_contract": cfg.card_contract,
        "helper_contract": cfg.helper_contract,
        "helper_event_topic": cfg.helper_event_topic,
        "include_card_evidence": bool(cfg.include_card_evidence),
        "last_scanned_block": int(last_scanned_block),
        "mappings": mappings,
    }


def _status_payload(cfg: WalletMigrationConfig, *, success: bool, message: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "updated_at": _now_tpe(cfg.tzinfo).isoformat(),
        "success": bool(success),
        "trigger": cfg.trigger,
        "message": message,
        "map_path": str(cfg.map_path),
        "sbt_contract": cfg.sbt_contract,
        "sbt_token_id": str(cfg.sbt_token_id),
        "card_contract": cfg.card_contract,
        "helper_contract": cfg.helper_contract,
        "helper_event_topic": cfg.helper_event_topic,
        "include_card_evidence": bool(cfg.include_card_evidence),
    }
    if extra:
        payload["extra"] = extra
    return payload


def write_status(cfg: WalletMigrationConfig, *, success: bool, message: str, extra: dict[str, Any] | None = None) -> None:
    _atomic_write_json(cfg.status_path, _status_payload(cfg, success=success, message=message, extra=extra))


def ensure_repo(cfg: WalletMigrationConfig) -> Path:
    repo_dir = cfg.backup_git_dir
    if (repo_dir / ".git").exists():
        return repo_dir
    repo_dir.parent.mkdir(parents=True, exist_ok=True)
    res = _run(["git", "clone", "--branch", cfg.backup_git_branch, cfg.backup_git_repo, str(repo_dir)])
    if res.returncode != 0:
        raise RuntimeError(f"git clone failed: {res.stderr.strip() or res.stdout.strip()}")
    return repo_dir


def git_pull(cfg: WalletMigrationConfig, repo_dir: Path) -> None:
    _run(["git", "fetch", "--all"], cwd=repo_dir)
    res = _run(["git", "pull", "--rebase", "origin", cfg.backup_git_branch], cwd=repo_dir)
    if res.returncode != 0:
        raise RuntimeError(f"git pull failed: {res.stderr.strip() or res.stdout.strip()}")


def bootstrap_from_git(cfg: WalletMigrationConfig) -> bool:
    if not cfg.backup_git_enabled:
        return False
    repo_dir = ensure_repo(cfg)
    if not _env_bool("SYNC_TEST_MODE", False):
        git_pull(cfg, repo_dir)
    copied = False
    if cfg.repo_map_path.exists():
        cfg.map_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cfg.repo_map_path, cfg.map_path)
        copied = True
    if cfg.repo_status_path.exists():
        cfg.status_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cfg.repo_status_path, cfg.status_path)
    return copied


def git_push_map(cfg: WalletMigrationConfig, commit_message: str) -> str:
    if not cfg.backup_git_enabled:
        return "git-disabled"
    if _env_bool("SYNC_TEST_MODE", False):
        return "test-mode-skip-push"
    repo_dir = ensure_repo(cfg)
    git_pull(cfg, repo_dir)
    cfg.repo_state_dir.mkdir(parents=True, exist_ok=True)
    if cfg.map_path.exists():
        shutil.copy2(cfg.map_path, cfg.repo_map_path)
    if cfg.status_path.exists():
        shutil.copy2(cfg.status_path, cfg.repo_status_path)
    _run(["git", "config", "user.name", os.getenv("BACKUP_GIT_USER_NAME", "tcg-pro-bot")], cwd=repo_dir)
    _run(["git", "config", "user.email", os.getenv("BACKUP_GIT_USER_EMAIL", "tcg-pro-bot@example.com")], cwd=repo_dir)
    _run(["git", "add", str(cfg.repo_map_path.relative_to(repo_dir)), str(cfg.repo_status_path.relative_to(repo_dir))], cwd=repo_dir)
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


def _last_scanned_block(cfg: WalletMigrationConfig) -> int:
    payload = _json_load(cfg.map_path)
    block = _parse_int(payload.get("last_scanned_block"))
    return block if block > 0 else 0


def run_sync(cfg: WalletMigrationConfig, *, full_rebuild: bool = False) -> dict[str, Any]:
    latest_block = fetch_latest_block(_log_cfg(cfg, cfg.sbt_contract))
    prev_scanned = 0 if full_rebuild else _last_scanned_block(cfg)
    from_block = max(cfg.start_block, prev_scanned + 1 if prev_scanned > 0 else cfg.start_block)
    to_block = int(latest_block)
    rows, stats = _scan_confirmed_migrations(cfg, from_block=from_block, to_block=to_block)
    payload = _merge_mapping_payload(cfg, rows, last_scanned_block=to_block)
    _atomic_write_json(cfg.map_path, payload)
    return {
        **stats,
        "from_block": from_block,
        "to_block": to_block,
        "latest_block": latest_block,
        "mapping_count": len(payload.get("mappings") or []),
        "new_pairs": len(rows),
        "map_path": str(cfg.map_path),
    }


def main() -> int:
    load_dotenv()
    args = parse_args()
    cfg = load_config(args)
    validate_config(cfg, require_api_key=not args.bootstrap_only)
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    cfg.map_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.status_path.parent.mkdir(parents=True, exist_ok=True)

    bootstrapped = False
    if cfg.bootstrap_from_git and cfg.backup_git_enabled:
        try:
            bootstrapped = bootstrap_from_git(cfg)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] bootstrap_from_git failed: {e}", file=sys.stderr)

    if args.bootstrap_only:
        msg = f"trigger={cfg.trigger} bootstrap_only=1 bootstrapped={1 if bootstrapped else 0} map={cfg.map_path}"
        write_status(cfg, success=True, message=msg, extra={"bootstrap_only": True, "bootstrapped": bootstrapped})
        print(f"[OK] {msg}")
        return 0

    result = run_sync(cfg, full_rebuild=bool(args.full_rebuild))
    msg = (
        f"trigger={cfg.trigger} blocks={result['from_block']}-{result['to_block']} "
        f"sbt_mints={result['sbt_mints']} helper_confirmed={result['helper_confirmed_pairs']} "
        f"confirmed={result['confirmed_pairs']} "
        f"mappings={result['mapping_count']} map={cfg.map_path}"
    )
    write_status(cfg, success=True, message=msg, extra=result)
    commit = "git-disabled"
    if cfg.backup_git_enabled:
        commit = git_push_map(
            cfg,
            commit_message=f"sync wallet migration {_now_tpe(cfg.tzinfo):%Y-%m-%d %H:%M:%S}",
        )
    print(f"[OK] {msg} commit={commit}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:  # noqa: BLE001
        load_dotenv()
        args = parse_args()
        cfg = load_config(args)
        err = f"{type(e).__name__}: {e}"
        print(f"[ERROR] {err}", file=sys.stderr)
        try:
            write_status(cfg, success=False, message=err, extra=None)
        except Exception:
            pass
        raise

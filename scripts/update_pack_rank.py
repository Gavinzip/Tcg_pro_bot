#!/usr/bin/env python3
"""Standalone monthly pack-rank sync (wallet-time on-chain scan)."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from onchain_metrics import OnchainConfig, scan_pack_open_counts_incremental
from runtime.pack_contracts import DEFAULT_PACK_CONTRACTS
from runtime.wallet_migration import apply_wallet_migration_counts, load_wallet_migration_map, wallet_migration_map_path
from runtime.wallet_usernames import (
    load_wallet_username_map,
    wallet_username_map_path as default_wallet_username_map_path,
)

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


DEFAULT_MONTHLY_PACK_LAUNCH_START = "2026-05-01T00:00:00+08:00"
DEFAULT_PACK_RANK_CUTOVER_START = ""
DEFAULT_PACK_RANK_CONTRACTS = DEFAULT_PACK_CONTRACTS


def _safe_tzinfo(name: str):
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    return timezone(timedelta(hours=8))


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _json_load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _parse_iso_dt(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _parse_address_csv(raw: str | None, *, default_values: tuple[str, ...] = ()) -> tuple[str, ...]:
    source = raw if raw is not None else ",".join(default_values)
    out: list[str] = []
    seen: set[str] = set()
    for token in str(source or "").replace(";", ",").split(","):
        address = token.strip().lower()
        if not address:
            continue
        if not address.startswith("0x") or len(address) != 42:
            continue
        if address in seen:
            continue
        seen.add(address)
        out.append(address)
    return tuple(out)


def _merge_address_tuples(*groups: tuple[str, ...]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for addr in group:
            a = str(addr or "").strip().lower()
            if not a.startswith("0x") or len(a) != 42:
                continue
            if a in seen:
                continue
            seen.add(a)
            out.append(a)
    return tuple(out)


def _month_start_local(now_dt: datetime) -> datetime:
    return now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _pack_rank_cutover_start(tzinfo) -> datetime | None:
    raw = str(os.getenv("PACK_RANK_CUTOVER_START", DEFAULT_PACK_RANK_CUTOVER_START)).strip()
    if not raw:
        return None
    dt = _parse_iso_dt(raw)
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tzinfo)
    return dt.astimezone(tzinfo)


def _monthly_pack_rank_window(now_dt: datetime, tzinfo) -> tuple[datetime, datetime]:
    month_start = _month_start_local(now_dt)
    launch_raw = str(os.getenv("PACK_RANK_LAUNCH_START", DEFAULT_MONTHLY_PACK_LAUNCH_START)).strip()
    launch_dt = _parse_iso_dt(launch_raw)
    if launch_dt is not None:
        if launch_dt.tzinfo is None:
            launch_dt = launch_dt.replace(tzinfo=tzinfo)
        else:
            launch_dt = launch_dt.astimezone(tzinfo)
        if launch_dt.year == now_dt.year and launch_dt.month == now_dt.month and launch_dt > month_start:
            month_start = launch_dt

    cutover_dt = _pack_rank_cutover_start(tzinfo)
    if (
        cutover_dt is not None
        and cutover_dt.year == now_dt.year
        and cutover_dt.month == now_dt.month
        and cutover_dt > month_start
    ):
        month_start = cutover_dt

    return month_start, now_dt


def _gacha_level(rank_value: int) -> str:
    rv = int(rank_value or 0)
    if rv <= 0:
        return "none"
    if rv <= 10:
        return "master"
    if rv <= 50:
        return "hunter"
    if rv <= 200:
        return "seeker"
    return "none"


def _pack_rank_contracts(prev_state: dict[str, Any] | None = None) -> tuple[str, ...]:
    state_contracts: tuple[str, ...] = ()
    if isinstance(prev_state, dict):
        raw_contracts = prev_state.get("contracts")
        if isinstance(raw_contracts, list):
            state_contracts = tuple(str(x or "").strip().lower() for x in raw_contracts)
    return _merge_address_tuples(
        DEFAULT_PACK_RANK_CONTRACTS,
        state_contracts,
        _parse_address_csv(os.getenv("PACK_RANK_CONTRACTS"), default_values=()),
        _parse_address_csv(os.getenv("PACK_RANK_CONTRACTS_EXTRA"), default_values=()),
    )


def _username_map_from_pack_rank_latest(latest_path: Path) -> dict[str, str]:
    payload = _json_load(latest_path)
    rows = payload.get("wallets") if isinstance(payload.get("wallets"), list) else []
    usernames: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        addr = str(row.get("address") or "").strip().lower()
        if not addr.startswith("0x") or len(addr) != 42:
            continue
        username = str(row.get("username") or "").strip()
        if username:
            usernames[addr] = username
    return usernames


@dataclass
class PackRankConfig:
    trigger: str
    data_dir: Path
    tz_name: str
    tzinfo: Any
    onchain_api_url: str
    onchain_chain_id: int
    onchain_api_key: str
    onchain_usdt_contract: str
    onchain_pack_contracts: tuple[str, ...]
    onchain_marketplace_contract: str
    onchain_page_size: int
    wallet_migration_map_path: str
    wallet_username_map_path: str

    @property
    def latest_path(self) -> Path:
        return self.data_dir / "pack_rank_latest.json"

    @property
    def status_path(self) -> Path:
        return self.data_dir / "state" / "pack_rank_status.json"

    @property
    def state_path(self) -> Path:
        return self.data_dir / "state" / "pack_rank_state.json"

    @property
    def history_path(self) -> Path:
        key = datetime.now(tz=self.tzinfo).strftime("%Y-%m-%d_%H")
        return self.data_dir / "history" / f"pack_rank_{key}.json"

    @property
    def pre_cutover_snapshot_path(self) -> Path:
        return self.data_dir / "state" / "pack_rank_pre_cutover_snapshot.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Standalone monthly pack-rank sync (wallet-time)")
    parser.add_argument("--trigger", default="manual")
    parser.add_argument("--data-dir", default="")
    parser.add_argument("--full-rebuild", action="store_true", help="Ignore pack_rank_state and rescan this month")
    return parser.parse_args()


def load_config(args: argparse.Namespace) -> PackRankConfig:
    app_env = str(os.getenv("APP_ENV", "local")).strip().lower() or "local"
    default_data_dir = "/data/renaiss_sync/rankings" if app_env == "server" else "./data/renaiss_sync/rankings"
    data_dir_raw = str(args.data_dir or "").strip() or str(
        os.getenv("RANK_SYNC_DATA_DIR", os.getenv("RANKING_DATA_DIR", default_data_dir))
    ).strip()
    tz_name = str(os.getenv("RANK_SYNC_TZ", "Asia/Taipei")).strip() or "Asia/Taipei"
    tzinfo = _safe_tzinfo(tz_name)

    onchain_api_url = str(
        os.getenv("BSCSCAN_API_URL", os.getenv("ONCHAIN_API_URL", "https://api.etherscan.io/v2/api"))
    ).strip() or "https://api.etherscan.io/v2/api"
    onchain_chain_id = _to_int(os.getenv("BSCSCAN_CHAIN_ID", os.getenv("ONCHAIN_CHAIN_ID", "56")), 56)
    onchain_api_key = str(os.getenv("BSCSCAN_API_KEY", "")).strip()
    onchain_usdt_contract = str(
        os.getenv("ONCHAIN_USDT_CONTRACT", "0x55d398326f99059ff775485246999027b3197955")
    ).strip().lower()
    onchain_pack_contracts: tuple[str, ...] = ()
    onchain_marketplace_contract = str(
        os.getenv("ONCHAIN_MARKETPLACE_CONTRACT", "0xae3e7268ef5a062946216a44f58a8f685ffd11d0")
    ).strip().lower()
    onchain_page_size = max(100, min(10000, _to_int(os.getenv("ONCHAIN_PAGE_SIZE", "10000"), 10000)))
    wallet_migration_map_path = str(os.getenv("WALLET_MIGRATION_MAP_PATH", "")).strip()
    wallet_username_map_path = str(os.getenv("WALLET_USERNAME_MAP_PATH", "")).strip() or default_wallet_username_map_path()

    return PackRankConfig(
        trigger=str(args.trigger or "manual"),
        data_dir=Path(data_dir_raw).expanduser().resolve(),
        tz_name=tz_name,
        tzinfo=tzinfo,
        onchain_api_url=onchain_api_url,
        onchain_chain_id=onchain_chain_id,
        onchain_api_key=onchain_api_key,
        onchain_usdt_contract=onchain_usdt_contract,
        onchain_pack_contracts=onchain_pack_contracts,
        onchain_marketplace_contract=onchain_marketplace_contract,
        onchain_page_size=onchain_page_size,
        wallet_migration_map_path=wallet_migration_map_path,
        wallet_username_map_path=wallet_username_map_path,
    )


def validate_config(cfg: PackRankConfig) -> None:
    if not cfg.onchain_api_key:
        raise RuntimeError("BSCSCAN_API_KEY is required for pack_rank sync")


def _build_onchain_cfg(cfg: PackRankConfig) -> OnchainConfig:
    return OnchainConfig(
        api_url=cfg.onchain_api_url,
        chain_id=cfg.onchain_chain_id,
        api_key=cfg.onchain_api_key,
        usdt_contract=cfg.onchain_usdt_contract,
        pack_contracts=(),
        marketplace_contract=cfg.onchain_marketplace_contract,
        page_size=cfg.onchain_page_size,
        retries=max(1, _to_int(os.getenv("API_MAX_RETRIES", "3"), 3)),
        backoff_sec=max(0.2, float(os.getenv("API_RETRY_BACKOFF_SEC", "0.5"))),
        card_contract=str(
            os.getenv(
                "PROFILE_CHAIN_CARD_CONTRACT",
                os.getenv("WALLET_MIGRATION_CARD_CONTRACT", "0xf8646a3ca093e97bb404c3b25e675c0394dd5b30"),
            )
        ).strip().lower(),
    )


def write_status(cfg: PackRankConfig, *, success: bool, message: str, extra: dict[str, Any] | None) -> None:
    payload: dict[str, Any] = {
        "updated_at": datetime.now(tz=cfg.tzinfo).isoformat(),
        "success": bool(success),
        "trigger": cfg.trigger,
        "message": message,
    }
    if isinstance(extra, dict):
        payload["extra"] = extra
    _atomic_write_json(cfg.status_path, payload)


def _maybe_snapshot_pre_cutover_pack_rank(cfg: PackRankConfig, now_dt: datetime) -> None:
    cutover_dt = _pack_rank_cutover_start(cfg.tzinfo)
    if cutover_dt is None or now_dt < cutover_dt:
        return
    if cfg.pre_cutover_snapshot_path.exists():
        return

    latest_payload = _json_load(cfg.latest_path)
    if not latest_payload:
        return
    meta = latest_payload.get("meta") if isinstance(latest_payload.get("meta"), dict) else {}
    wallets = latest_payload.get("wallets") if isinstance(latest_payload.get("wallets"), list) else []
    if not wallets:
        return

    # Snapshot only when latest file is still from the pre-cutover window.
    latest_window_end = _parse_iso_dt(str(meta.get("monthly_gacha_window_end") or "").strip())
    if latest_window_end is None:
        latest_window_end = _parse_iso_dt(str(meta.get("updated_at") or "").strip())
    if latest_window_end is None or latest_window_end > cutover_dt:
        return

    snapshot_payload = {
        "meta": {
            "captured_at": now_dt.isoformat(),
            "cutover_start": cutover_dt.isoformat(),
            "source_latest_path": str(cfg.latest_path),
            "source_updated_at": str(meta.get("updated_at") or ""),
            "source_window_start": str(meta.get("monthly_gacha_window_start") or ""),
            "source_window_end": str(meta.get("monthly_gacha_window_end") or ""),
            "wallet_count": len(wallets),
        },
        "wallets": wallets,
        "top": latest_payload.get("top") if isinstance(latest_payload.get("top"), dict) else {},
    }
    _atomic_write_json(cfg.pre_cutover_snapshot_path, snapshot_payload)


def run_sync(cfg: PackRankConfig, *, full_rebuild: bool = False) -> dict[str, Any]:
    started_at = datetime.now(tz=cfg.tzinfo)
    _maybe_snapshot_pre_cutover_pack_rank(cfg, started_at)
    window_start, window_end = _monthly_pack_rank_window(started_at, cfg.tzinfo)
    window_start_ts = int(window_start.astimezone(timezone.utc).timestamp())
    window_end_ts = int(window_end.astimezone(timezone.utc).timestamp())

    prev_state = {} if full_rebuild else _json_load(cfg.state_path)
    onchain_cfg = _build_onchain_cfg(cfg)
    pack_contracts = _pack_rank_contracts(prev_state)
    if not pack_contracts:
        raise RuntimeError("PACK_RANK_CONTRACTS is empty")
    previous_username_map = _username_map_from_pack_rank_latest(cfg.latest_path)
    wallet_username_map = load_wallet_username_map(cfg.wallet_username_map_path)
    if wallet_username_map:
        print(
            f"[INFO] wallet username map loaded wallets={len(wallet_username_map)} path={cfg.wallet_username_map_path}",
            flush=True,
        )
    else:
        print(
            f"[WARN] wallet username map unavailable; pack_rank usernames will use previous latest only path={cfg.wallet_username_map_path}",
            flush=True,
        )
    scan = scan_pack_open_counts_incremental(
        onchain_cfg,
        pack_contracts=pack_contracts,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
        prev_state=prev_state,
    )

    wallet_counts_raw = scan.get("wallet_counts") if isinstance(scan.get("wallet_counts"), dict) else {}
    migration_map = load_wallet_migration_map(cfg.wallet_migration_map_path)
    wallet_counts = apply_wallet_migration_counts(wallet_counts_raw, migration_map)
    stats = scan.get("stats") if isinstance(scan.get("stats"), dict) else {}
    next_state = scan.get("state") if isinstance(scan.get("state"), dict) else {}

    rows: list[dict[str, Any]] = []
    for address, raw_count in wallet_counts.items():
        addr = str(address or "").strip().lower()
        if not addr.startswith("0x") or len(addr) != 42:
            continue
        opens = max(0, _to_int(raw_count, 0))
        if opens <= 0:
            continue
        username = wallet_username_map.get(addr) or previous_username_map.get(addr)
        if not username and migration_map:
            # If canonical(new) has no username yet, borrow old wallet name from latest map.
            for old_addr, new_addr in migration_map.items():
                if str(new_addr or "").strip().lower() != addr:
                    continue
                old_key = str(old_addr or "").strip().lower()
                old_name = wallet_username_map.get(old_key) or previous_username_map.get(old_key)
                if old_name:
                    username = old_name
                    break
        rows.append(
            {
                "address": addr,
                "username": username or None,
                "monthly_gacha_open_count": int(opens),
            }
        )

    rows.sort(key=lambda x: (int(x.get("monthly_gacha_open_count") or 0), str(x.get("address") or "")), reverse=True)
    for idx, row in enumerate(rows, start=1):
        row["monthly_gacha_open_rank"] = idx
        row["monthly_gacha_level"] = _gacha_level(idx)

    finished_at = datetime.now(tz=cfg.tzinfo)
    meta = {
        "timezone": cfg.tz_name,
        "started_at": started_at.isoformat(),
        "updated_at": finished_at.isoformat(),
        "version": 1,
        "trigger": cfg.trigger,
        "full_rebuild": bool(full_rebuild),
        "monthly_gacha_window_start": window_start.isoformat(),
        "monthly_gacha_window_end": window_end.isoformat(),
        "monthly_gacha_level_rules": {
            "master": "top10",
            "hunter": "top50",
            "seeker": "top200",
        },
        "monthly_gacha_scan_mode": str(stats.get("scan_mode") or "contract_center_incremental"),
        "monthly_gacha_scan_api_calls": int(_to_int(stats.get("api_calls"), 0)),
        "monthly_gacha_receipt_api_calls": int(_to_int(stats.get("receipt_api_calls"), 0)),
        "monthly_gacha_card_transfer_log_api_calls": int(_to_int(stats.get("card_transfer_log_api_calls"), 0)),
        "monthly_gacha_scan_rows_scanned": int(_to_int(stats.get("rows_scanned"), 0)),
        "monthly_gacha_card_transfer_log_rows_scanned": int(_to_int(stats.get("card_transfer_log_rows_scanned"), 0)),
        "monthly_gacha_receipt_failed": int(_to_int(stats.get("receipt_failed"), 0)),
        "monthly_gacha_multi_open_txs": int(_to_int(stats.get("multi_open_txs"), 0)),
        "monthly_gacha_fallback_open_txs": int(_to_int(stats.get("fallback_open_txs"), 0)),
        "monthly_gacha_scan_reset": bool(stats.get("reset_applied")),
        "pack_contracts": list(pack_contracts),
        "pack_contract_count": len(pack_contracts),
        "cutover_start": (_pack_rank_cutover_start(cfg.tzinfo).isoformat() if _pack_rank_cutover_start(cfg.tzinfo) else ""),
        "wallet_migration_map_path": str(cfg.wallet_migration_map_path or wallet_migration_map_path()),
        "wallet_migration_pairs": len(migration_map),
        "wallet_username_map_path": str(cfg.wallet_username_map_path),
        "wallet_username_map_wallets": len(wallet_username_map),
        "wallet_count": len(rows),
        "total_monthly_opens": int(sum(int(r.get("monthly_gacha_open_count") or 0) for r in rows)),
    }
    payload = {
        "meta": meta,
        "top": {
            "monthly_gacha": rows[:300],
        },
        "wallets": rows,
    }

    if next_state:
        _atomic_write_json(cfg.state_path, next_state)
    _atomic_write_json(cfg.latest_path, payload)
    _atomic_write_json(cfg.history_path, payload)

    duration_sec = max(0.0, (finished_at - started_at).total_seconds())
    return {
        "wallet_count": len(rows),
        "duration_sec": duration_sec,
        "api_calls": int(_to_int(stats.get("api_calls"), 0)),
        "receipt_api_calls": int(_to_int(stats.get("receipt_api_calls"), 0)),
        "card_transfer_log_api_calls": int(_to_int(stats.get("card_transfer_log_api_calls"), 0)),
        "rows_scanned": int(_to_int(stats.get("rows_scanned"), 0)),
        "card_transfer_log_rows_scanned": int(_to_int(stats.get("card_transfer_log_rows_scanned"), 0)),
        "receipt_failed": int(_to_int(stats.get("receipt_failed"), 0)),
        "multi_open_txs": int(_to_int(stats.get("multi_open_txs"), 0)),
        "fallback_open_txs": int(_to_int(stats.get("fallback_open_txs"), 0)),
        "scan_mode": str(stats.get("scan_mode") or "contract_center_incremental"),
        "reset_applied": bool(stats.get("reset_applied")),
        "latest_path": str(cfg.latest_path),
        "history_path": str(cfg.history_path),
        "state_path": str(cfg.state_path),
        "pre_cutover_snapshot_path": str(cfg.pre_cutover_snapshot_path),
        "full_rebuild": bool(full_rebuild),
        "wallet_username_map_path": str(cfg.wallet_username_map_path),
        "wallet_username_map_wallets": len(wallet_username_map),
    }


def main() -> int:
    load_dotenv()
    args = parse_args()
    cfg = load_config(args)
    validate_config(cfg)

    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    (cfg.data_dir / "history").mkdir(parents=True, exist_ok=True)
    (cfg.data_dir / "state").mkdir(parents=True, exist_ok=True)

    result = run_sync(cfg, full_rebuild=bool(args.full_rebuild))
    msg = (
        f"trigger={cfg.trigger} scan_mode={result['scan_mode']} wallets={result['wallet_count']} "
        f"api_calls={result['api_calls']} receipt_calls={result['receipt_api_calls']} "
        f"card_log_calls={result['card_transfer_log_api_calls']} rows_scanned={result['rows_scanned']} "
        f"multi_open_txs={result['multi_open_txs']} "
        f"receipt_failed={result['receipt_failed']} "
        f"reset={1 if result['reset_applied'] else 0} full_rebuild={1 if result['full_rebuild'] else 0} "
        f"duration_sec={result['duration_sec']:.2f}"
    )
    print(f"[OK] {msg}")
    write_status(
        cfg,
        success=True,
        message=msg,
        extra={
            "wallet_count": result["wallet_count"],
            "scan_mode": result["scan_mode"],
            "api_calls": result["api_calls"],
            "receipt_api_calls": result["receipt_api_calls"],
            "card_transfer_log_api_calls": result["card_transfer_log_api_calls"],
            "rows_scanned": result["rows_scanned"],
            "card_transfer_log_rows_scanned": result["card_transfer_log_rows_scanned"],
            "receipt_failed": result["receipt_failed"],
            "multi_open_txs": result["multi_open_txs"],
            "fallback_open_txs": result["fallback_open_txs"],
            "reset_applied": result["reset_applied"],
            "duration_sec": result["duration_sec"],
            "latest_path": result["latest_path"],
            "history_path": result["history_path"],
            "state_path": result["state_path"],
            "pre_cutover_snapshot_path": result["pre_cutover_snapshot_path"],
            "full_rebuild": result["full_rebuild"],
            "wallet_username_map_path": result["wallet_username_map_path"],
            "wallet_username_map_wallets": result["wallet_username_map_wallets"],
        },
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:  # noqa: BLE001
        load_dotenv()
        args = parse_args()
        cfg = load_config(args)
        err = f"{type(e).__name__}: {e}"
        print(f"[ERROR] {err}")
        try:
            write_status(cfg, success=False, message=err, extra=None)
        except Exception:
            pass
        raise

#!/usr/bin/env python3
"""Standalone monthly pack-rank sync (wallet-time on-chain scan)."""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from onchain_metrics import OnchainConfig, scan_wallet_open_counts_by_time_incremental

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


DEFAULT_MONTHLY_PACK_LAUNCH_START = "2026-05-01T00:00:00+08:00"


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


def _build_wallet_index(rank_data_dir: Path) -> tuple[list[str], dict[str, str]]:
    latest_path = rank_data_dir / "latest.json"
    payload = _json_load(latest_path)
    rows = payload.get("wallets") if isinstance(payload.get("wallets"), list) else []
    usernames: dict[str, str] = {}
    wallets: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        addr = str(row.get("address") or "").strip().lower()
        if not addr.startswith("0x") or len(addr) != 42:
            continue
        if addr not in seen:
            seen.add(addr)
            wallets.append(addr)
        username = str(row.get("username") or "").strip()
        if username:
            usernames[addr] = username
    return wallets, usernames


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Standalone monthly pack-rank sync (wallet-time)")
    parser.add_argument("--trigger", default="manual")
    parser.add_argument("--data-dir", default="")
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
    # Pack rank no longer uses a fixed pack-contract allowlist. It counts wallet
    # USDT-out transactions that also receive NFTs in the same tx within the
    # monthly time window.
    onchain_pack_contracts: tuple[str, ...] = ()
    onchain_marketplace_contract = str(
        os.getenv("ONCHAIN_MARKETPLACE_CONTRACT", "0xae3e7268ef5a062946216a44f58a8f685ffd11d0")
    ).strip().lower()
    onchain_page_size = max(100, min(10000, _to_int(os.getenv("ONCHAIN_PAGE_SIZE", "10000"), 10000)))

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


def run_sync(cfg: PackRankConfig) -> dict[str, Any]:
    started_at = datetime.now(tz=cfg.tzinfo)
    window_start, window_end = _monthly_pack_rank_window(started_at, cfg.tzinfo)
    window_start_ts = int(window_start.astimezone(timezone.utc).timestamp())
    window_end_ts = int(window_end.astimezone(timezone.utc).timestamp())

    prev_state = _json_load(cfg.state_path)
    onchain_cfg = _build_onchain_cfg(cfg)
    wallet_universe, username_map = _build_wallet_index(cfg.data_dir)
    scan = scan_wallet_open_counts_by_time_incremental(
        onchain_cfg,
        wallets=wallet_universe,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
        prev_state=prev_state,
    )

    wallet_counts_raw = scan.get("wallet_counts") if isinstance(scan.get("wallet_counts"), dict) else {}
    stats = scan.get("stats") if isinstance(scan.get("stats"), dict) else {}
    next_state = scan.get("state") if isinstance(scan.get("state"), dict) else {}

    rows: list[dict[str, Any]] = []
    for address, raw_count in wallet_counts_raw.items():
        addr = str(address or "").strip().lower()
        if not addr.startswith("0x") or len(addr) != 42:
            continue
        opens = max(0, _to_int(raw_count, 0))
        if opens <= 0:
            continue
        rows.append(
            {
                "address": addr,
                "username": username_map.get(addr) or None,
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
        "monthly_gacha_window_start": window_start.isoformat(),
        "monthly_gacha_window_end": window_end.isoformat(),
        "monthly_gacha_level_rules": {
            "master": "top10",
            "hunter": "top50",
            "seeker": "top200",
        },
        "monthly_gacha_scan_mode": str(stats.get("scan_mode") or "wallet_time_incremental"),
        "monthly_gacha_scan_api_calls": int(_to_int(stats.get("api_calls"), 0)),
        "monthly_gacha_scan_rows_scanned": int(_to_int(stats.get("rows_scanned"), 0)),
        "monthly_gacha_scan_reset": bool(stats.get("reset_applied")),
        "wallet_universe_count": len(wallet_universe),
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
        "rows_scanned": int(_to_int(stats.get("rows_scanned"), 0)),
        "scan_mode": str(stats.get("scan_mode") or "wallet_time_incremental"),
        "reset_applied": bool(stats.get("reset_applied")),
        "latest_path": str(cfg.latest_path),
        "history_path": str(cfg.history_path),
        "state_path": str(cfg.state_path),
    }


def main() -> int:
    load_dotenv()
    args = parse_args()
    cfg = load_config(args)
    validate_config(cfg)

    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    (cfg.data_dir / "history").mkdir(parents=True, exist_ok=True)
    (cfg.data_dir / "state").mkdir(parents=True, exist_ok=True)

    result = run_sync(cfg)
    msg = (
        f"trigger={cfg.trigger} scan_mode={result['scan_mode']} wallets={result['wallet_count']} "
        f"api_calls={result['api_calls']} rows_scanned={result['rows_scanned']} "
        f"reset={1 if result['reset_applied'] else 0} duration_sec={result['duration_sec']:.2f}"
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
            "rows_scanned": result["rows_scanned"],
            "reset_applied": result["reset_applied"],
            "duration_sec": result["duration_sec"],
            "latest_path": result["latest_path"],
            "history_path": result["history_path"],
            "state_path": result["state_path"],
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

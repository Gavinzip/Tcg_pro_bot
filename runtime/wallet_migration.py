from __future__ import annotations

import json
import os
from typing import Any


def _normalize_wallet_address(address: str | None) -> str:
    text = str(address or "").strip().lower()
    if text.startswith("0x") and len(text) == 42:
        return text
    return ""


def _sync_data_dir() -> str:
    app_env = str(os.getenv("APP_ENV", "local")).strip().lower() or "local"
    default_dir = "/data/renaiss_sync" if app_env == "server" else "./data/renaiss_sync"
    return str(os.getenv("SYNC_DATA_DIR", default_dir)).strip() or default_dir


def wallet_migration_map_path() -> str:
    explicit = str(os.getenv("WALLET_MIGRATION_MAP_PATH", "")).strip()
    if explicit:
        return explicit
    return os.path.join(_sync_data_dir(), "state", "wallet_migration_map.json")


def _load_json(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def load_wallet_migration_map(path: str | None = None) -> dict[str, str]:
    map_path = str(path or wallet_migration_map_path()).strip()
    if not map_path:
        return {}
    payload = _load_json(map_path)
    if not payload:
        return {}

    # Supported formats:
    # 1) {"0xold": "0xnew", ...}
    # 2) {"mappings":[{"old":"0x..","new":"0x.."}, ...]}
    # 3) {"pairs":[{"old":"0x..","new":"0x.."}, ...]}
    out: dict[str, str] = {}

    def _put_pair(old_addr: str | None, new_addr: str | None) -> None:
        old_norm = _normalize_wallet_address(old_addr)
        new_norm = _normalize_wallet_address(new_addr)
        if not old_norm or not new_norm:
            return
        if old_norm == new_norm:
            return
        out[old_norm] = new_norm

    for k, v in payload.items():
        if isinstance(v, str):
            _put_pair(str(k), v)

    for key in ("mappings", "pairs", "items"):
        rows = payload.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            _put_pair(
                row.get("old") or row.get("old_wallet") or row.get("from"),
                row.get("new") or row.get("new_wallet") or row.get("to"),
            )

    return out


def apply_wallet_migration_counts(wallet_counts: dict[str, int], old_to_new: dict[str, str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for raw_addr, raw_count in (wallet_counts or {}).items():
        addr = _normalize_wallet_address(raw_addr)
        if not addr:
            continue
        canonical = _normalize_wallet_address(old_to_new.get(addr) or "") or addr
        out[canonical] = int(out.get(canonical, 0)) + int(raw_count or 0)
    return out

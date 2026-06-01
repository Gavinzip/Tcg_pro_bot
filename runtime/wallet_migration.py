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


def wallet_migration_status_path() -> str:
    return os.path.join(_sync_data_dir(), "state", "wallet_migration_status.json")


def _load_json(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def load_wallet_migration_payload(path: str | None = None) -> dict[str, Any]:
    map_path = str(path or wallet_migration_map_path()).strip()
    if not map_path:
        return {}
    return _load_json(map_path)


def load_wallet_migration_status(path: str | None = None) -> dict[str, Any]:
    status_path = str(path or wallet_migration_status_path()).strip()
    if not status_path:
        return {}
    return _load_json(status_path)


def _wallet_migration_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []

    rows: list[dict[str, Any]] = []

    def _put_row(old_addr: str | None, new_addr: str | None, source_row: dict[str, Any] | None = None) -> None:
        old_norm = _normalize_wallet_address(old_addr)
        new_norm = _normalize_wallet_address(new_addr)
        if not old_norm or not new_norm or old_norm == new_norm:
            return
        row: dict[str, Any] = {
            "old": old_norm,
            "new": new_norm,
        }
        if isinstance(source_row, dict):
            for key in ("tx", "block", "cards", "source"):
                if key in source_row:
                    row[key] = source_row.get(key)
        rows.append(row)

    for k, v in payload.items():
        if isinstance(v, str):
            _put_row(str(k), v, {"source": "flat"})

    for key in ("mappings", "pairs", "items"):
        items = payload.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            _put_row(
                item.get("old") or item.get("old_wallet") or item.get("from"),
                item.get("new") or item.get("new_wallet") or item.get("to"),
                item,
            )

    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        old_addr = str(row.get("old") or "")
        if old_addr:
            deduped[old_addr] = row
    return sorted(deduped.values(), key=lambda x: (str(x.get("old") or ""), str(x.get("new") or "")))


def wallet_migration_api_payload(
    map_path: str | None = None,
    status_path: str | None = None,
) -> dict[str, Any]:
    resolved_map_path = str(map_path or wallet_migration_map_path()).strip()
    resolved_status_path = str(status_path or wallet_migration_status_path()).strip()
    payload = load_wallet_migration_payload(resolved_map_path)
    status = load_wallet_migration_status(resolved_status_path)
    mappings = _wallet_migration_rows(payload)
    old_set = {str(row.get("old") or "") for row in mappings if row.get("old")}
    new_set = {str(row.get("new") or "") for row in mappings if row.get("new")}
    old_to_new = {str(row["old"]): str(row["new"]) for row in mappings if row.get("old") and row.get("new")}

    return {
        "success": bool(payload),
        "version": payload.get("version") if isinstance(payload, dict) else None,
        "updated_at": payload.get("updated_at") if isinstance(payload, dict) else None,
        "source": payload.get("source") if isinstance(payload, dict) else None,
        "sbt_contract": payload.get("sbt_contract") if isinstance(payload, dict) else None,
        "sbt_token_id": payload.get("sbt_token_id") if isinstance(payload, dict) else None,
        "card_contract": payload.get("card_contract") if isinstance(payload, dict) else None,
        "last_scanned_block": payload.get("last_scanned_block") if isinstance(payload, dict) else None,
        "summary": {
            "migration_pairs": len(mappings),
            "unique_old_wallets": len(old_set),
            "unique_new_wallets": len(new_set),
            "unique_all_addresses": len(old_set | new_set),
        },
        "mappings": mappings,
        "old_to_new": old_to_new,
        "status": status,
    }


def load_wallet_migration_map(path: str | None = None) -> dict[str, str]:
    map_path = str(path or wallet_migration_map_path()).strip()
    if not map_path:
        return {}
    payload = load_wallet_migration_payload(map_path)
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

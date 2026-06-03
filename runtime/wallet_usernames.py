from __future__ import annotations

import csv
import gzip
import json
import os
from pathlib import Path
from typing import Any


def _normalize_wallet_address(address: str | None) -> str:
    text = str(address or "").strip().lower()
    if text.startswith("0x") and len(text) == 42:
        return text
    return ""


def bundled_wallet_username_map_path() -> str:
    return str(Path(__file__).resolve().parent / "resources" / "wallet_username_map.json.gz")


def wallet_username_map_path() -> str:
    explicit = str(os.getenv("WALLET_USERNAME_MAP_PATH", "")).strip()
    if explicit:
        return explicit
    return bundled_wallet_username_map_path()


def _load_json_payload(path: Path) -> Any:
    if path.suffix == ".gz" or str(path).endswith(".json.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _put_username(out: dict[str, str], address: str | None, username: str | None) -> None:
    addr = _normalize_wallet_address(address)
    name = str(username or "").strip()
    if not addr or not name:
        return
    out[addr] = name


def _load_csv_map(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not isinstance(row, dict):
                continue
            username = str(row.get("username") or row.get("name") or "").strip()
            if not username:
                continue
            for key in ("old_wallet", "new_wallet", "wallet", "address", "wallet_address"):
                _put_username(out, row.get(key), username)
    return out


def _load_payload_map(payload: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    if isinstance(payload, dict):
        rows = payload.get("wallets") or payload.get("usernames")
        if isinstance(rows, dict):
            for addr, username in rows.items():
                _put_username(out, str(addr), str(username or ""))
            return out
        if all(isinstance(k, str) and k.startswith("0x") for k in payload.keys()):
            for addr, username in payload.items():
                _put_username(out, str(addr), str(username or ""))
            return out
        for key in ("rows", "items", "mappings"):
            rows = payload.get(key)
            if isinstance(rows, list):
                payload = rows
                break

    if isinstance(payload, list):
        for row in payload:
            if not isinstance(row, dict):
                continue
            username = str(row.get("username") or row.get("name") or "").strip()
            if not username:
                continue
            for key in ("old_wallet", "new_wallet", "wallet", "address", "wallet_address"):
                _put_username(out, row.get(key), username)
    return out


_WALLET_USERNAME_CACHE: dict[str, Any] = {
    "path": "",
    "mtime": -1.0,
    "data": {},
}


def load_wallet_username_map(path: str | None = None) -> dict[str, str]:
    resolved = str(path or wallet_username_map_path()).strip()
    if not resolved:
        return {}
    p = Path(resolved).expanduser()
    try:
        mtime = p.stat().st_mtime
    except OSError:
        return {}

    if (
        _WALLET_USERNAME_CACHE.get("path") == str(p)
        and _WALLET_USERNAME_CACHE.get("mtime") == mtime
        and isinstance(_WALLET_USERNAME_CACHE.get("data"), dict)
    ):
        return _WALLET_USERNAME_CACHE["data"]  # type: ignore[return-value]

    try:
        if p.suffix.lower() == ".csv":
            data = _load_csv_map(p)
        else:
            data = _load_payload_map(_load_json_payload(p))
    except Exception:
        data = {}

    _WALLET_USERNAME_CACHE["path"] = str(p)
    _WALLET_USERNAME_CACHE["mtime"] = mtime
    _WALLET_USERNAME_CACHE["data"] = data
    return data


def username_for_wallet(address: str | None, username_map: dict[str, str] | None = None) -> str | None:
    addr = _normalize_wallet_address(address)
    if not addr:
        return None
    data = username_map if username_map is not None else load_wallet_username_map()
    username = str((data or {}).get(addr) or "").strip()
    return username or None

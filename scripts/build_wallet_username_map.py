#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import gzip
import json
from pathlib import Path
from typing import Any


def _normalize_wallet_address(address: str | None) -> str:
    text = str(address or "").strip().lower()
    if text.startswith("0x") and len(text) == 42:
        return text
    return ""


def _choose_username(counter: Counter[str], first_seen: dict[str, int]) -> str:
    return sorted(counter.items(), key=lambda item: (-item[1], first_seen.get(item[0], 10**12), item[0]))[0][0]


def build_wallet_username_payload(source_csv: Path) -> dict[str, Any]:
    address_names: dict[str, Counter[str]] = defaultdict(Counter)
    first_seen: dict[tuple[str, str], int] = {}
    row_count = 0
    with source_csv.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row_idx, row in enumerate(reader):
            row_count += 1
            username = str(row.get("username") or row.get("name") or "").strip()
            if not username:
                continue
            for key in ("old_wallet", "new_wallet", "wallet", "address", "wallet_address"):
                addr = _normalize_wallet_address(row.get(key))
                if not addr:
                    continue
                address_names[addr][username] += 1
                first_seen.setdefault((addr, username), row_idx)

    wallets: dict[str, str] = {}
    conflicts: dict[str, list[dict[str, Any]]] = {}
    for addr, counter in address_names.items():
        wallets[addr] = _choose_username(counter, {name: first_seen[(addr, name)] for name in counter})
        if len(counter) > 1:
            conflicts[addr] = [
                {"username": name, "count": count, "first_seen": first_seen[(addr, name)]}
                for name, count in sorted(counter.items(), key=lambda item: (-item[1], first_seen[(addr, item[0])]))
            ]

    return {
        "version": 1,
        "source": {
            "filename": source_csv.name,
            "row_count": row_count,
            "address_count": len(wallets),
            "conflict_count": len(conflicts),
            "format": "old_wallet,new_wallet,username",
            "conflict_resolution": "highest_count_then_first_seen",
        },
        "wallets": dict(sorted(wallets.items())),
        "conflicts": conflicts,
    }


def write_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if path.suffix == ".gz" or str(path).endswith(".json.gz"):
        path.write_bytes(gzip.compress(body, compresslevel=9, mtime=0))
    else:
        path.write_bytes(body)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build compact wallet username map from Renaiss user wallet CSV")
    parser.add_argument("source_csv", help="CSV with old_wallet,new_wallet,username columns")
    parser.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parents[1] / "runtime" / "resources" / "wallet_username_map.json.gz"),
        help="Output .json or .json.gz path",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_csv = Path(args.source_csv).expanduser()
    out_path = Path(args.out).expanduser()
    payload = build_wallet_username_payload(source_csv)
    write_payload(out_path, payload)
    source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
    print(
        "[OK] wallet username map built "
        f"rows={source.get('row_count')} addresses={source.get('address_count')} "
        f"conflicts={source.get('conflict_count')} out={out_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

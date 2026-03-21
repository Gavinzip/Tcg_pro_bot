#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

import update_rankings as ur


def _default_data_dir() -> Path:
    app_env = str(os.getenv("APP_ENV", "local")).strip().lower() or "local"
    default_dir = "/data/renaiss_sync/rankings" if app_env == "server" else "./data/renaiss_sync/rankings"
    raw = str(os.getenv("RANK_SYNC_DATA_DIR", os.getenv("RANKING_DATA_DIR", default_dir))).strip() or default_dir
    return Path(raw).expanduser().resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill ranking withdraw-card values for all wallets")
    parser.add_argument("--data-dir", default="")
    parser.add_argument("--workers", type=int, default=max(1, int(os.getenv("RANKING_WORKERS", "8"))))
    parser.add_argument(
        "--activity-page-limit",
        type=int,
        default=max(1, int(os.getenv("RANKING_ACTIVITY_PAGE_LIMIT", os.getenv("PROFILE_ACTIVITY_PAGE_LIMIT", "50")))),
    )
    parser.add_argument(
        "--activity-max-pages",
        type=int,
        default=max(1, int(os.getenv("RANKING_ACTIVITY_MAX_PAGES", os.getenv("PROFILE_ACTIVITY_MAX_PAGES", "120")))),
    )
    parser.add_argument("--max-wallets", type=int, default=None)
    parser.add_argument("--write-history", action="store_true")
    parser.add_argument("--retry-rounds", type=int, default=max(0, int(os.getenv("RANK_BACKFILL_RETRY_ROUNDS", "2"))))
    parser.add_argument("--retry-sleep-sec", type=float, default=max(0.5, float(os.getenv("RANK_BACKFILL_RETRY_SLEEP_SEC", "2"))))
    return parser.parse_args()


def _compute_withdraw_total_for_wallet(address: str, page_limit: int, max_pages: int) -> Decimal:
    wallet_norm = str(address or "").strip().lower()
    if not wallet_norm:
        return Decimal("0")

    cursor: str | None = None
    seen_cursors: set[str] = set()
    seen_withdraw_events: set[str] = set()
    withdraw_token_ids: set[str] = set()
    token_latest_values: dict[str, tuple[int, Decimal]] = {}

    def _remember_token_value(token_id: str, value: Decimal, ts_value: int) -> None:
        tid = str(token_id or "").strip()
        if not tid or value <= 0:
            return
        prev = token_latest_values.get(tid)
        if prev is None or ts_value >= prev[0]:
            token_latest_values[tid] = (ts_value, value)

    for _ in range(max_pages):
        page = ur._trpc_user_activities(wallet_norm, cursor=cursor, limit=page_limit)
        rows = page.get("activities") or []
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if not isinstance(row, dict):
                continue
            row_type = str(row.get("__typename") or "").strip()
            ts = ur._parse_int(row.get("timestamp")) or 0
            row_item = row.get("item") if isinstance(row.get("item"), dict) else {}
            token_hint = str(row.get("nftTokenId") or row.get("tokenId") or row_item.get("tokenId") or "").strip()

            if row_type in ("PerpetualBuybackActivity", "BuybackActivity"):
                buyback_price = ur._wei_to_usdt(row.get("priceInUsdt"))
                if buyback_price <= 0:
                    buyback_price = ur._wei_to_usdt(row.get("amount"))
                fmv_hint = ur._card_price_to_usd(row.get("fmvPriceInUsd"))
                _remember_token_value(token_hint, fmv_hint if fmv_hint > 0 else buyback_price, ts)

            elif row_type == "TransferActivity":
                target = str(row.get("to") or "").strip().lower()
                if target != ur.PROFILE_CARD_WITHDRAW_ADDRESS:
                    continue
                token_id = str(row.get("tokenId") or row_item.get("tokenId") or "").strip()
                tx_hash = str(row.get("txHash") or "").strip().lower()
                if not tx_hash and not token_id:
                    continue
                event_key = f"{tx_hash}:{token_id}"
                if event_key in seen_withdraw_events:
                    continue
                seen_withdraw_events.add(event_key)
                if token_id:
                    withdraw_token_ids.add(token_id)

        next_cursor = page.get("nextCursor")
        if not next_cursor:
            break
        next_cursor = str(next_cursor)
        if next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    card_withdraw_total = Decimal("0")
    unresolved_tokens: list[str] = []
    for token_id in withdraw_token_ids:
        hinted_value = ur._to_decimal((token_latest_values.get(token_id) or (0, Decimal("0")))[1])
        if hinted_value > 0:
            card_withdraw_total += hinted_value
        else:
            unresolved_tokens.append(token_id)

    for token_id in unresolved_tokens:
        fallback_value = ur._to_decimal(ur._fetch_card_withdraw_value_by_token_id(token_id))
        if fallback_value > 0:
            card_withdraw_total += fallback_value

    return card_withdraw_total


def _build_payload_from_rows(
    rows: list[dict[str, Any]],
    prev_payload: dict[str, Any],
    started_at: datetime,
    finished_at: datetime,
    changed_wallets: int,
) -> dict[str, Any]:
    records = []
    for row in rows:
        rec = ur._from_wallet_row(row)
        if rec is None:
            continue
        records.append(rec)

    prev_meta = prev_payload.get("meta") if isinstance(prev_payload.get("meta"), dict) else {}
    collectible_pages = ur._parse_int(prev_meta.get("collectible_pages")) or 0
    wallet_source = str(prev_meta.get("wallet_source") or "holders_file").strip() or "holders_file"
    holders_file_raw = str(prev_meta.get("holders_file") or "").strip()
    holders_file = Path(holders_file_raw) if holders_file_raw else None
    if holders_file is not None and not holders_file.exists():
        holders_file = None

    out = ur.build_payload(
        records,
        started_at,
        finished_at,
        collectible_pages=collectible_pages,
        full_rebuild=False,
        full_rebuild_reason="withdraw-backfill",
        refreshed_wallets=len(records),
        changed_wallets=changed_wallets,
        removed_wallets=0,
        wallet_source=wallet_source,
        holders_file=holders_file,
    )
    out_meta = out.get("meta") if isinstance(out.get("meta"), dict) else {}
    out_meta["trigger"] = "withdraw_backfill"
    out_meta["version"] = ur._parse_int(out_meta.get("version")) or 4
    out_meta["withdraw_backfill_changed_wallets"] = changed_wallets
    out["meta"] = out_meta
    return out


def main() -> int:
    load_dotenv()
    args = parse_args()

    data_dir = Path(args.data_dir).expanduser().resolve() if str(args.data_dir).strip() else _default_data_dir()
    latest_path = data_dir / "latest.json"
    if not latest_path.exists():
        print(f"[ERROR] latest.json not found: {latest_path}", flush=True)
        return 2

    payload = ur._json_load(latest_path)
    rows_raw = payload.get("wallets") if isinstance(payload.get("wallets"), list) else []
    rows = [r for r in rows_raw if isinstance(r, dict)]
    if not rows:
        print("[ERROR] no wallet rows in latest.json", flush=True)
        return 2

    if args.max_wallets is not None and args.max_wallets > 0:
        rows = rows[: int(args.max_wallets)]

    started_at = datetime.now(tz=ur._safe_tzinfo(str(os.getenv("RANK_SYNC_TZ", "Asia/Taipei")).strip() or "Asia/Taipei"))
    total = len(rows)
    workers = max(1, int(args.workers))
    page_limit = max(1, int(args.activity_page_limit))
    max_pages = max(1, int(args.activity_max_pages))

    print(
        f"[INFO] withdraw backfill start wallets={total} workers={workers} page_limit={page_limit} max_pages={max_pages}",
        flush=True,
    )

    row_map = {}
    for row in rows:
        addr = str(row.get("address") or "").strip().lower()
        if not addr:
            continue
        row_map[addr] = row

    changed_wallets = 0
    pending_addrs = list(row_map.keys())
    failed_wallets = 0
    retry_rounds = max(0, int(args.retry_rounds))
    retry_sleep_sec = max(0.5, float(args.retry_sleep_sec))

    for round_idx in range(0, retry_rounds + 1):
        if not pending_addrs:
            break
        stage = "withdraw_backfill" if round_idx == 0 else f"withdraw_backfill_retry{round_idx}"
        round_total = len(pending_addrs)
        round_done = 0
        round_failed: list[str] = []
        print(f"[INFO] {stage} start wallets={round_total}", flush=True)

        with ThreadPoolExecutor(max_workers=min(workers, max(1, round_total))) as pool:
            future_map = {
                pool.submit(_compute_withdraw_total_for_wallet, addr, page_limit, max_pages): addr
                for addr in pending_addrs
            }
            for future in as_completed(future_map):
                addr = future_map[future]
                row = row_map.get(addr)
                if row is None:
                    round_done += 1
                    ur._maybe_print_progress(stage, round_done, round_total, ur._progress_every())
                    continue
                try:
                    new_withdraw = ur._to_decimal(future.result())
                except Exception as e:  # noqa: BLE001
                    round_failed.append(addr)
                    print(f"[WARN] {stage} failed for {addr}: {e}", flush=True)
                    round_done += 1
                    ur._maybe_print_progress(stage, round_done, round_total, ur._progress_every())
                    continue

                old_withdraw = ur._to_decimal(row.get("card_withdraw_total_usdt"))
                if ur._quantize_2(new_withdraw) != ur._quantize_2(old_withdraw):
                    row["card_withdraw_total_usdt"] = ur._decimal_to_str(new_withdraw)
                    total_spent = ur._to_decimal(row.get("total_spent_usdt"))
                    total_earned = ur._to_decimal(row.get("total_earned_usdt"))
                    holdings_value = ur._to_decimal(row.get("holdings_value_usdt"))
                    cash_net = total_earned - total_spent + new_withdraw
                    total_pnl = cash_net + holdings_value
                    row["cash_net_usdt"] = ur._decimal_to_str(cash_net)
                    row["total_pnl_usdt"] = ur._decimal_to_str(total_pnl)
                    changed_wallets += 1

                round_done += 1
                ur._maybe_print_progress(stage, round_done, round_total, ur._progress_every())

        if not round_failed:
            pending_addrs = []
            break
        pending_addrs = round_failed
        if round_idx < retry_rounds:
            print(f"[INFO] {stage} failed={len(round_failed)} -> retry in {retry_sleep_sec}s", flush=True)
            time.sleep(retry_sleep_sec)

    failed_wallets = len(pending_addrs)

    finished_at = datetime.now(tz=started_at.tzinfo)
    new_payload = _build_payload_from_rows(rows, payload, started_at, finished_at, changed_wallets)
    ur._atomic_write_json(latest_path, new_payload)

    if args.write_history:
        history_path = data_dir / "history" / f"{finished_at.strftime('%Y-%m-%d_%H')}.json"
        ur._atomic_write_json(history_path, new_payload)

    status_payload = {
        "updated_at": finished_at.isoformat(),
        "success": failed_wallets == 0,
        "trigger": "withdraw_backfill",
        "message": (
            f"withdraw backfill done changed_wallets={changed_wallets} failed_wallets={failed_wallets} wallets={total}"
        ),
        "extra": {
            "wallet_count": total,
            "changed_wallets": changed_wallets,
            "failed_wallets": failed_wallets,
            "duration_sec": round((finished_at - started_at).total_seconds(), 2),
        },
    }
    ur._atomic_write_json(data_dir / "state" / "ranking_status.json", status_payload)

    print(
        (
            f"[OK] withdraw backfill done wallets={total} changed_wallets={changed_wallets} "
            f"failed_wallets={failed_wallets} duration_sec={status_payload['extra']['duration_sec']}"
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

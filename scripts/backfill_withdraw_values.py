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
from runtime.wallet_migration import (
    build_wallet_migration_source_groups,
    canonical_wallet_address,
    wallet_migration_cycle_sources,
)


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

    withdraw_targets = ur._withdraw_target_set()
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

    for page_index in range(max_pages):
        page = ur._trpc_user_activities(wallet_norm, cursor=cursor, limit=page_limit)
        if not isinstance(page, dict):
            raise RuntimeError(f"activity API returned invalid page for {wallet_norm}")
        rows = page.get("activities")
        if not isinstance(rows, list):
            raise RuntimeError(f"activity API returned invalid activities for {wallet_norm}")

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
                if target not in withdraw_targets:
                    continue
                token_id = str(row.get("tokenId") or row_item.get("tokenId") or "").strip()
                tx_hash = str(row.get("txHash") or "").strip().lower()
                if not token_id:
                    raise RuntimeError(
                        f"withdraw transfer is missing tokenId for {wallet_norm}; tx={tx_hash or '-'}"
                    )
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
            raise RuntimeError(f"activity API repeated cursor for {wallet_norm}: {next_cursor}")
        if page_index + 1 >= max_pages:
            raise RuntimeError(
                f"activity history exceeded max_pages={max_pages} for {wallet_norm}; "
                "increase --activity-max-pages"
            )
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
        if fallback_value <= 0:
            raise RuntimeError(f"withdraw token {token_id} has no resolvable value for {wallet_norm}")
        card_withdraw_total += fallback_value

    return card_withdraw_total


def _compute_withdraw_total_for_sources(
    source_addresses: tuple[str, ...],
    page_limit: int,
    max_pages: int,
) -> Decimal:
    return sum(
        (
            _compute_withdraw_total_for_wallet(address, page_limit, max_pages)
            for address in source_addresses
        ),
        Decimal("0"),
    )


def _canonicalize_rows(
    rows: list[dict[str, Any]],
    old_to_new: dict[str, str],
) -> dict[str, dict[str, Any]]:
    row_map: dict[str, dict[str, Any]] = {}
    for source_row in rows:
        source_address = ur._normalize_wallet_address(str(source_row.get("address") or ""))
        if not source_address:
            continue
        canonical = canonical_wallet_address(source_address, old_to_new) or source_address
        if canonical != source_address:
            raise ValueError(
                f"non-canonical ranking row {source_address} resolves to {canonical}; "
                "run a full ranking rebuild first"
            )
        if canonical in row_map:
            raise ValueError(
                f"duplicate ranking rows resolve to canonical wallet {canonical}; run a full ranking rebuild first"
            )
        row = dict(source_row)
        row["address"] = canonical
        row_map[canonical] = row
    return row_map


def _build_payload_from_rows(
    rows: list[dict[str, Any]],
    prev_payload: dict[str, Any],
    started_at: datetime,
    finished_at: datetime,
    changed_wallets: int,
    monthly_pack_window_start: datetime,
    monthly_pack_window_end: datetime,
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
        collection_fallback_reason=str(prev_meta.get("collection_snapshot_fallback_reason") or ""),
        full_rebuild=False,
        full_rebuild_reason="withdraw-backfill",
        refreshed_wallets=len(records),
        changed_wallets=changed_wallets,
        removed_wallets=0,
        wallet_source=wallet_source,
        holders_file=holders_file,
        monthly_pack_window_start=monthly_pack_window_start,
        monthly_pack_window_end=monthly_pack_window_end,
        monthly_pack_scan_stats={
            "scan_mode": prev_meta.get("monthly_gacha_scan_mode"),
            "api_calls": prev_meta.get("monthly_gacha_scan_api_calls"),
            "rows_scanned": prev_meta.get("monthly_gacha_scan_rows_scanned"),
            "reset_applied": prev_meta.get("monthly_gacha_scan_reset"),
        },
        sbt_scan_stats={
            "source": prev_meta.get("sbt_source"),
            "scan_mode": prev_meta.get("sbt_scan_mode"),
            "api_calls": prev_meta.get("sbt_scan_api_calls"),
            "rows_scanned": prev_meta.get("sbt_scan_rows_scanned"),
            "reset_applied": prev_meta.get("sbt_scan_reset"),
            "refresh_wallets": prev_meta.get("sbt_refresh_wallets"),
            "fallback_wallets": prev_meta.get("sbt_fallback_wallets"),
            "failed_wallets": prev_meta.get("sbt_failed_wallets"),
        },
    )
    out_meta = out.get("meta") if isinstance(out.get("meta"), dict) else {}
    for key in (
        "wallet_migration_map_path",
        "wallet_migration_pairs",
        "source_wallet_count",
        "canonical_wallet_count",
        "migrated_source_wallets",
        "canonical_wallets_from_multiple_sources",
        "source_wallet_state_path",
        "source_wallet_state_loaded",
    ):
        if key in prev_meta:
            out_meta[key] = prev_meta[key]
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

    prev_meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    monthly_pack_window_start = ur._parse_iso_dt(prev_meta.get("monthly_gacha_window_start"))
    monthly_pack_window_end = ur._parse_iso_dt(prev_meta.get("monthly_gacha_window_end"))
    if monthly_pack_window_start is None or monthly_pack_window_end is None:
        print(
            "[ERROR] ranking snapshot is missing monthly window metadata; run a full ranking rebuild first",
            flush=True,
        )
        return 2

    migration_map_path = str(
        prev_meta.get("wallet_migration_map_path") or ur.wallet_migration_map_path()
    ).strip()
    old_to_new = ur.load_wallet_migration_map(migration_map_path)
    if "wallet_migration_pairs" not in prev_meta:
        print(
            "[ERROR] ranking snapshot is missing migration metadata; run a full ranking rebuild first",
            flush=True,
        )
        return 2
    expected_migration_pairs = ur._parse_int(prev_meta.get("wallet_migration_pairs")) or 0
    if expected_migration_pairs != len(old_to_new):
        print(
            (
                "[ERROR] wallet migration map does not match ranking snapshot; "
                f"expected_pairs={expected_migration_pairs} loaded_pairs={len(old_to_new)} "
                f"path={migration_map_path}"
            ),
            flush=True,
        )
        return 2
    cycle_sources = wallet_migration_cycle_sources(old_to_new)
    if cycle_sources:
        print(
            (
                "[ERROR] wallet migration map contains a cycle; "
                f"sources={','.join(cycle_sources)} path={migration_map_path}"
            ),
            flush=True,
        )
        return 2
    prev_meta["wallet_migration_map_path"] = migration_map_path
    prev_meta["wallet_migration_pairs"] = len(old_to_new)

    try:
        row_map = _canonicalize_rows(rows, old_to_new)
    except ValueError as exc:
        print(f"[ERROR] {exc}", flush=True)
        return 2
    rows = list(row_map.values())

    source_groups = build_wallet_migration_source_groups(list(row_map), old_to_new)
    source_wallet_count = len({source for sources in source_groups.values() for source in sources})
    merged_wallet_count = sum(1 for sources in source_groups.values() if len(sources) > 1)

    print(
        (
            f"[INFO] withdraw backfill start wallets={total} source_wallets={source_wallet_count} "
            f"merged_wallets={merged_wallet_count} workers={workers} "
            f"page_limit={page_limit} max_pages={max_pages}"
        ),
        flush=True,
    )

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
                pool.submit(
                    _compute_withdraw_total_for_sources,
                    source_groups.get(addr, (addr,)),
                    page_limit,
                    max_pages,
                ): addr
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
    if failed_wallets:
        status_payload = {
            "updated_at": finished_at.isoformat(),
            "success": False,
            "trigger": "withdraw_backfill",
            "message": (
                f"withdraw backfill aborted failed_wallets={failed_wallets} wallets={total}; "
                "latest snapshot was not modified"
            ),
            "extra": {
                "wallet_count": total,
                "source_wallet_count": source_wallet_count,
                "merged_wallet_count": merged_wallet_count,
                "changed_wallets": 0,
                "failed_wallets": failed_wallets,
                "duration_sec": round((finished_at - started_at).total_seconds(), 2),
            },
        }
        ur._atomic_write_json(data_dir / "state" / "ranking_status.json", status_payload)
        print(f"[ERROR] {status_payload['message']}", flush=True)
        return 1

    new_payload = _build_payload_from_rows(
        rows,
        payload,
        started_at,
        finished_at,
        changed_wallets,
        monthly_pack_window_start,
        monthly_pack_window_end,
    )
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
            "source_wallet_count": source_wallet_count,
            "merged_wallet_count": merged_wallet_count,
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

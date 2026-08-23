from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import os
import time
from typing import Any, Callable

from runtime.profile_source_cache import load_profile_source


SCHEMA_VERSION = "1.3"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
SUPPORTED_POSTER_KINDS = {"collection", "history", "extremes"}


class ProfileDataError(RuntimeError):
    pass


def normalize_wallet_address(value: str | None) -> str:
    wallet = str(value or "").strip().lower()
    if len(wallet) != 42 or not wallet.startswith("0x"):
        return ""
    try:
        int(wallet[2:], 16)
    except ValueError:
        return ""
    return wallet


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    text = str(value or "").replace(",", "").strip()
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _money(value: Any) -> float:
    return float(_decimal(value).quantize(Decimal("0.01")))


def _fmv_cents(value: Any) -> int:
    try:
        return max(0, int(_decimal(value)))
    except (TypeError, ValueError):
        return 0


def _timed(call: Callable[[], Any]) -> tuple[Any, float]:
    started = time.perf_counter()
    return call(), round(time.perf_counter() - started, 3)


def _collection_row(row: dict[str, Any]) -> dict[str, Any]:
    fmv_cents = _fmv_cents(row.get("fmvPriceInUSD"))
    image_url = str(
        row.get("frontWithoutStandImageUrl")
        or row.get("frontImageUrl")
        or row.get("imageUrl")
        or row.get("collectibleImageUrl")
        or ""
    ).strip()
    token_id = str(row.get("tokenId") or "").strip()
    return {
        "token_id": token_id,
        "name": str(row.get("name") or "Unknown Collectible").strip(),
        "set_name": str(row.get("setName") or "").strip(),
        "fmv_usd": float((Decimal(fmv_cents) / Decimal("100")).quantize(Decimal("0.01"))),
        "image_url": image_url,
        "detail_url": f"https://www.renaiss.xyz/card/{token_id}" if token_id else "",
    }


def _sbt_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "token_id": str(row.get("sbt_id") or "").strip(),
        "name": str(row.get("name") or "Unknown SBT").strip(),
        "amount": max(0, int(_decimal(row.get("balance")))),
        "image_url": str(row.get("image_url") or "").strip(),
    }


def _fetch_sbt_snapshot(core: Any, wallet: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read one ERC-1155 transfer stream and derive both balances and zero-balance evidence."""
    from scripts.onchain_metrics import fetch_all_erc1155_transfers

    cfg = core._build_profile_onchain_cfg(force=True)
    contract = str(core.ONCHAIN_SBT_CONTRACT or "").strip().lower()
    if cfg is None or len(contract) != 42 or not contract.startswith("0x"):
        raise ProfileDataError("sbt_source_unavailable")

    transfers = fetch_all_erc1155_transfers(cfg, wallet, contract)
    balances: dict[str, int] = {}
    transfer_count = 0
    incoming_transfers = 0
    outgoing_transfers = 0
    mint_transfers = 0
    burn_transfers = 0
    received_total = 0
    sent_total = 0
    minted_total = 0
    burned_total = 0

    for row in transfers:
        if not isinstance(row, dict):
            continue
        token_id = str(row.get("tokenID") or row.get("tokenId") or "").strip()
        amount = max(0, int(_decimal(row.get("tokenValue"))))
        if not token_id or amount <= 0:
            continue
        transfer_count += 1
        from_address = str(row.get("from") or "").strip().lower()
        to_address = str(row.get("to") or "").strip().lower()
        if to_address == wallet:
            balances[token_id] = balances.get(token_id, 0) + amount
            incoming_transfers += 1
            received_total += amount
            if from_address == ZERO_ADDRESS:
                mint_transfers += 1
                minted_total += amount
        if from_address == wallet:
            balances[token_id] = balances.get(token_id, 0) - amount
            outgoing_transfers += 1
            sent_total += amount
            if to_address == ZERO_ADDRESS:
                burn_transfers += 1
                burned_total += amount

    current_balances = {token_id: amount for token_id, amount in balances.items() if amount > 0}
    metadata = core._fetch_sbt_metadata_map_onchain(tuple(current_balances))
    badges: list[dict[str, Any]] = []
    for token_id, amount in current_balances.items():
        meta = metadata.get(token_id) if isinstance(metadata, dict) else {}
        meta = meta if isinstance(meta, dict) else {}
        badges.append(
            {
                "name": str(meta.get("name") or f"SBT #{token_id}").strip(),
                "balance": int(amount),
                "is_owned": True,
                "sbt_id": token_id,
                "image_url": str(meta.get("image_url") or "").strip(),
            }
        )
    badges.sort(key=lambda row: (int(row.get("balance") or 0), str(row.get("sbt_id") or "")), reverse=True)

    current_total = sum(current_balances.values())
    if current_total > 0:
        status = "current_balance"
    elif incoming_transfers == 0:
        status = "no_sbt_transfer_history"
    elif minted_total > 0 and burned_total >= minted_total and sent_total >= received_total:
        status = "all_minted_sbt_burned"
    elif sent_total >= received_total:
        status = "all_sbt_transferred_or_burned"
    else:
        status = "no_current_balance"

    return badges, {
        "source": "bsc_erc1155_transfers",
        "status": status,
        "current_total": current_total,
        "transfer_count": transfer_count,
        "incoming_transfers": incoming_transfers,
        "outgoing_transfers": outgoing_transfers,
        "mint_transfers": mint_transfers,
        "burn_transfers": burn_transfers,
        "received_total": received_total,
        "sent_total": sent_total,
        "minted_total": minted_total,
        "burned_total": burned_total,
    }


def _ranking_snapshot(core: Any, wallet: str) -> dict[str, Any]:
    wallet_map = core._load_rankings_wallet_map()
    row = wallet_map.get(wallet) if isinstance(wallet_map, dict) else None
    row = row if isinstance(row, dict) else {}
    snapshot_updated_at: str | None = None
    try:
        latest_path = os.path.join(core._rank_sync_data_dir(), "latest.json")
        with open(latest_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        meta = payload.get("meta") if isinstance(payload, dict) else {}
        if isinstance(meta, dict):
            snapshot_updated_at = str(meta.get("updated_at") or "").strip() or None
    except (OSError, ValueError, TypeError):
        snapshot_updated_at = None

    def rank(key: str) -> dict[str, Any]:
        raw = row.get(key)
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 0
        chip = core._rank_chip_payload(value)
        return {
            "rank": value if value > 0 else None,
            "tier": str(chip.get("tier") or "none"),
        }

    return {
        "source": "tcg_pro_ranking_snapshot",
        "snapshot_updated_at": snapshot_updated_at,
        "holders_total": len(wallet_map) if isinstance(wallet_map, dict) else 0,
        "wallet_in_snapshot": bool(row),
        "total_spent": rank("total_spent_rank"),
        "holdings": rank("holdings_rank"),
        "pnl": rank("pnl_rank"),
        "trade_volume": rank("volume_rank"),
        "active_days": rank("participation_days_rank"),
        "sbt": rank("sbt_rank"),
    }


def _extreme_row(
    row: dict[str, Any] | None,
    kind: str,
    *,
    collectible_lookup: Callable[[str], dict] | None = None,
    warnings: list[dict[str, str]] | None = None,
    allow_empty: bool = False,
) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    token_id = str(row.get("token_id") or "").strip()
    name = str(row.get("name") or "").strip()
    image_url = str(row.get("image") or "").strip()
    if " / " in name:
        name = name.split(" / ", 1)[1].strip()
    generic_name = name.lower() in {"renaiss", "unknown collectible", ""}
    image_is_prepared = image_url.startswith("data:image/") or "nft_image_standalone" in image_url
    if token_id and collectible_lookup and (generic_name or not image_is_prepared):
        try:
            collectible = collectible_lookup(token_id)
            resolved_name = str(
                (collectible or {}).get("name")
                or (collectible or {}).get("collectibleName")
                or ""
            ).strip()
            if resolved_name and name.lower() in {"renaiss", "unknown collectible", ""}:
                name = resolved_name
            standalone_image = str((collectible or {}).get("frontWithoutStandImageUrl") or "").strip()
            if standalone_image:
                image_url = standalone_image
        except Exception as exc:
            if warnings is not None:
                warnings.append(
                    {
                        "code": "extreme_name_unavailable",
                        "source": "renaiss_collectible_api",
                        "message": f"Official extreme collectible name was unavailable: {type(exc).__name__}",
                    }
                )
    value = _money(row.get("value"))
    if value <= 0 and not allow_empty:
        return None
    return {
        "kind": kind,
        "token_id": token_id,
        "name": name or "Unknown Collectible",
        "value_usd": value,
        "image_url": image_url,
    }


def build_wallet_profile_data(
    wallet_address: str,
    *,
    language: str = "zh-Hant",
    include_extremes: bool = True,
    include_posters: bool = False,
    poster_kind: str | None = None,
) -> dict[str, Any]:
    wallet = normalize_wallet_address(wallet_address)
    if not wallet:
        raise ProfileDataError("valid wallet address is required")
    requested_poster = str(poster_kind or "").strip().lower() or None
    if requested_poster is not None and requested_poster not in SUPPORTED_POSTER_KINDS:
        raise ProfileDataError("unsupported poster")

    # Import lazily so the lightweight API health endpoint can start without
    # initializing Discord or the rendering runtime.
    from runtime import core

    profile_name = str(core._username_from_rankings_wallet(wallet) or "").strip()
    short_wallet = f"{wallet[:6]}...{wallet[-4:]}"

    needs_history = requested_poster in {None, "history", "extremes"}
    needs_collection = requested_poster in {None, "collection", "history", "extremes"}
    needs_sbt = requested_poster in {None, "collection", "history", "extremes"}
    needs_rankings = requested_poster in {None, "history"}
    needs_extremes = requested_poster == "extremes" or (requested_poster is None and include_extremes)

    history: dict[str, Any] = {}
    collection: list[dict[str, Any]] = []
    sbt_badges: list[dict[str, Any]] = []
    sbt_diagnostics: dict[str, Any] | None = None
    history_seconds = 0.0
    collection_seconds = 0.0
    sbt_seconds = 0.0
    source_calls: dict[str, Callable[[], tuple[Any, float, str]]] = {}
    source_cache: dict[str, str] = {}

    def cached_source(
        source: str,
        cache_language: str,
        loader: Callable[[], Any],
    ) -> tuple[Any, float, str]:
        started = time.perf_counter()
        value, cache_status = load_profile_source(
            source,
            wallet,
            cache_language,
            loader,
        )
        return value, round(time.perf_counter() - started, 3), cache_status

    if needs_history:
        source_calls["history"] = lambda: cached_source(
            "history",
            language,
            lambda: core._build_wallet_activity_history_for_profile(wallet, profile_lang=language),
        )
    if needs_collection:
        source_calls["collection"] = lambda: cached_source(
            "collection",
            "",
            lambda: core._fetch_wallet_collection_chain(wallet),
        )
    if needs_sbt:
        source_calls["sbt"] = lambda: cached_source(
            "sbt",
            "",
            lambda: _fetch_sbt_snapshot(core, wallet),
        )

    with ThreadPoolExecutor(max_workers=max(1, len(source_calls)), thread_name_prefix="profile-data") as pool:
        futures = {name: pool.submit(call) for name, call in source_calls.items()}
        try:
            if "history" in futures:
                history, history_seconds, source_cache["history"] = futures["history"].result()
            if "collection" in futures:
                collection, collection_seconds, source_cache["collection"] = futures["collection"].result()
            if "sbt" in futures:
                sbt_snapshot, sbt_seconds, source_cache["sbt"] = futures["sbt"].result()
                sbt_badges, sbt_diagnostics = sbt_snapshot
        except Exception as exc:
            raise ProfileDataError("profile_lookup_failed") from exc

    collection_rows = [_collection_row(row) for row in collection if isinstance(row, dict)]
    collection_rows.sort(key=lambda row: (float(row.get("fmv_usd") or 0), str(row.get("token_id") or "")), reverse=True)
    sbt_rows = [_sbt_row(row) for row in sbt_badges if isinstance(row, dict)]
    sbt_rows = [row for row in sbt_rows if int(row.get("amount") or 0) > 0]
    sbt_rows.sort(key=lambda row: (int(row.get("amount") or 0), str(row.get("token_id") or "")), reverse=True)

    holdings_value = sum((_decimal(row.get("fmv_usd")) for row in collection_rows), Decimal("0"))
    cash_net = _decimal(history.get("net_total"))
    warnings = [dict(row) for row in (history.get("warnings") or []) if isinstance(row, dict)]
    rankings = _ranking_snapshot(core, wallet) if needs_rankings else {
        "source": "not_requested",
        "snapshot_updated_at": None,
        "holders_total": 0,
        "wallet_in_snapshot": False,
        **{key: {"rank": None, "tier": "none"} for key in ("total_spent", "holdings", "pnl", "trade_volume", "active_days", "sbt")},
    }
    if needs_rankings and not rankings.get("wallet_in_snapshot"):
        warnings.append(
            {
                "code": "ranking_wallet_unavailable",
                "source": "tcg_pro_ranking_snapshot",
                "message": "This wallet is not present in the current ranking snapshot; rank chips are shown as unavailable.",
            }
        )

    extremes: dict[str, Any] = {"highest": None, "lowest": None}
    extremes_seconds = 0.0
    if needs_extremes:
        parsed_collection = [
            {
                "raw": row,
                "fmv": _fmv_cents(row.get("fmvPriceInUSD")),
                "set": str(row.get("setName") or "").strip(),
            }
            for row in collection
            if isinstance(row, dict)
        ]
        parsed_collection.sort(key=lambda row: int(row.get("fmv") or 0), reverse=True)
        try:
            extreme_context, extremes_seconds = _timed(
                lambda: core._build_wallet_extremes_template_context(
                    history_data=history,
                    parsed_sorted=parsed_collection,
                    profile_name=profile_name or short_wallet,
                    short_wallet=short_wallet,
                    profile_lang=language,
                    prepare_images=False,
                )
            )
            items = extreme_context.get("items") if isinstance(extreme_context, dict) else []
            extremes = {
                "highest": _extreme_row(
                    items[0] if isinstance(items, list) and len(items) > 0 else None,
                    "highest",
                    collectible_lookup=lambda token_id: core._collectible_by_token_cached(
                        token_id,
                        max_retries=1,
                    ),
                    warnings=warnings,
                    allow_empty=include_posters and requested_poster == "extremes",
                ),
                "lowest": _extreme_row(
                    items[1] if isinstance(items, list) and len(items) > 1 else None,
                    "lowest",
                    collectible_lookup=lambda token_id: core._collectible_by_token_cached(
                        token_id,
                        max_retries=1,
                    ),
                    warnings=warnings,
                    allow_empty=include_posters and requested_poster == "extremes",
                ),
            }
            if include_posters:
                unprepared = [
                    row
                    for row in extremes.values()
                    if isinstance(row, dict)
                    and not (
                        str(row.get("image_url") or "").startswith("data:image/")
                        or "nft_image_standalone" in str(row.get("image_url") or "")
                    )
                ]
                if unprepared:
                    raise ProfileDataError("profile_extreme_image_prepare_failed")
        except Exception as exc:
            if include_posters:
                raise ProfileDataError("profile_extreme_image_prepare_failed") from exc
            warnings.append(
                {
                    "code": "extremes_unavailable",
                    "source": "profile_extremes",
                    "message": f"Heaven and Hell data could not be calculated: {type(exc).__name__}",
                }
            )

    payload = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "language": language,
        "wallet": wallet,
        "requested_poster": requested_poster,
        "loaded_sources": [
            source
            for source, loaded in (
                ("profile_identity", True),
                ("history", needs_history),
                ("collection", needs_collection),
                ("sbt", needs_sbt),
                ("rankings", needs_rankings),
                ("extremes", needs_extremes),
            )
            if loaded
        ],
        "profile_name": profile_name or short_wallet,
        "sources": {
            "profile": "tcg_pro",
            **({"financials": "bsc_onchain"} if needs_history else {}),
            **({"collection": "renaiss_card_api_v1"} if needs_collection else {}),
            **({"sbt": "bsc_erc1155_onchain"} if needs_sbt else {}),
        },
        "metrics": {
            "opened_packs": int(history.get("opened_packs_count") or 0),
            "pack_spent_usd": _money(history.get("pack_spent_total")),
            "market_bought_usd": _money(history.get("market_buy_total")),
            "buyback_usd": _money(history.get("buyback_total")),
            "market_sold_usd": _money(history.get("market_sell_total")),
            "card_withdraw_usd": _money(history.get("card_withdraw_total")),
            "total_spent_usd": _money(history.get("total_spent")),
            "total_earned_usd": _money(history.get("total_earned")),
            "cash_net_usd": _money(cash_net),
            "holdings_value_usd": _money(holdings_value),
            "net_with_holdings_usd": _money(cash_net + holdings_value),
            "active_days": int(history.get("active_days_count") or 0),
            "activity_count": int(history.get("activity_total_count") or 0),
            "collection_count": len(collection_rows),
            "sbt_total": sum(int(row.get("amount") or 0) for row in sbt_rows),
            "sbt_badge_count": len(sbt_rows),
        },
        "history": {
            "range": str(history.get("history_range") or "-"),
            "pack_rows": [dict(row) for row in (history.get("contract_rows") or []) if isinstance(row, dict)],
            "activity_rows": [dict(row) for row in (history.get("activity_rows") or []) if isinstance(row, dict)],
        },
        "collection": collection_rows,
        "sbt": sbt_rows,
        "sbt_diagnostics": sbt_diagnostics,
        "rankings": rankings,
        "extremes": extremes,
        "warnings": warnings,
        "source_cache": source_cache,
        "timings": {
            "history_seconds": history_seconds,
            "collection_seconds": collection_seconds,
            "sbt_seconds": sbt_seconds,
            "extremes_seconds": extremes_seconds,
        },
    }
    if include_posters:
        try:
            from runtime.profile_posters import build_wallet_profile_poster_documents

            posters = build_wallet_profile_poster_documents(payload, language, poster_kind=requested_poster)
            if requested_poster and not posters.get("documents", {}).get(requested_poster):
                raise ProfileDataError("profile_poster_unavailable")
            payload["posters"] = posters
        except Exception as exc:
            raise ProfileDataError("profile_poster_render_failed") from exc
    return payload

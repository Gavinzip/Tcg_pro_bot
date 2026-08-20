from __future__ import annotations

from datetime import datetime
from html import escape
from typing import Any


POSTER_SCHEMA_VERSION = "1.0"
POSTER_WIDTH = 1200
POSTER_HEIGHT = 900


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _integer(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _text(value: Any) -> str:
    return escape(str(value or "").strip(), quote=True)


def _money_value(value: Any) -> str:
    return f"{_number(value):,.2f}"


def _collection_value(value: Any) -> str:
    number = _number(value)
    if number.is_integer():
        return f"{number:,.0f}"
    return f"{number:,.2f}"


def _signed_money(value: Any) -> str:
    number = _number(value)
    if number > 0:
        return f"+${number:,.2f}"
    if number < 0:
        return f"-${abs(number):,.2f}"
    return "$0.00"


def _generated_date(profile: dict[str, Any]) -> str:
    raw = str(profile.get("generated_at") or "").strip()
    if raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return datetime.now().strftime("%Y-%m-%d")


def _short_wallet(wallet: str) -> str:
    value = str(wallet or "").strip()
    return f"{value[:6]}...{value[-4:]}" if len(value) == 42 else value


def _rank(rankings: dict[str, Any], key: str) -> tuple[str, str]:
    row = rankings.get(key) if isinstance(rankings, dict) else None
    row = row if isinstance(row, dict) else {}
    rank_value = row.get("rank")
    try:
        rank_text = str(int(rank_value)) if int(rank_value) > 0 else "—"
    except (TypeError, ValueError):
        rank_text = "—"
    tier = str(row.get("tier") or "none").strip().lower()
    if tier not in {"gold", "silver", "bronze"}:
        tier = "none"
    return rank_text, tier


def _compact_badge_label(value: Any, max_len: int = 24) -> str:
    label = str(value or "").strip() or "SBT"
    for separator in ("—", "–", "-", "|"):
        if separator in label:
            tail = label.split(separator)[-1].strip()
            if tail and len(tail) < len(label):
                label = tail
                break
    return label if len(label) <= max_len else f"{label[: max_len - 1].rstrip()}…"


def build_wallet_profile_poster_documents(
    profile: dict[str, Any],
    language: str,
    *,
    poster_kind: str | None = None,
) -> dict[str, Any]:
    """Adapt one JSON Profile result into the exact HTML templates used by `/profile`."""
    from runtime import core

    collection = [row for row in (profile.get("collection") or []) if isinstance(row, dict)]
    sbt_rows = [row for row in (profile.get("sbt") or []) if isinstance(row, dict)]
    history = profile.get("history") if isinstance(profile.get("history"), dict) else {}
    metrics = profile.get("metrics") if isinstance(profile.get("metrics"), dict) else {}
    rankings = profile.get("rankings") if isinstance(profile.get("rankings"), dict) else {}
    extremes = profile.get("extremes") if isinstance(profile.get("extremes"), dict) else {}
    wallet = str(profile.get("wallet") or "").strip()
    wallet_short = _short_wallet(wallet)
    profile_name_raw = str(profile.get("profile_name") or wallet_short or "Unknown User").strip()
    update_date = _generated_date(profile)

    item_count = min(10, len(collection))
    contract = core.wallet_profile_poster_contract(language, item_count=max(1, item_count))
    ui = contract.get("ui") if isinstance(contract.get("ui"), dict) else {}
    labels = contract.get("history") if isinstance(contract.get("history"), dict) else {}
    brand_name = _text(ui.get("brand_name") or "Renaiss")
    brand_site = _text(ui.get("brand_site") or "renaiss.xyz")
    sbt_total = _integer(metrics.get("sbt_total"))
    badges = [
        {
            "name": _text(row.get("name") or "SBT"),
            "label": _text(_compact_badge_label(row.get("name"))),
            "balance": _integer(row.get("amount")),
            "image": _text(row.get("image_url")),
        }
        for row in sbt_rows
    ]

    documents: dict[str, str] = {}
    order: list[str] = []
    requested = {poster_kind} if poster_kind else {"collection", "history", "extremes"}

    collection_items = [
        {
            "name": _text(row.get("name") or "Unknown Collectible"),
            "image": _text(row.get("image_url")),
            "image_mode": "standalone",
            "value": _collection_value(row.get("fmv_usd")),
        }
        for row in collection[:10]
    ]
    if "collection" in requested:
        collection_total = sum(_number(row.get("fmv_usd")) for row in collection[:10])
        collection_context = {
            "collection_name": _text(f"{profile_name_raw} Collection"),
            "sbt_total": sbt_total,
            "sbt_badges_display": badges[:7],
            "items": collection_items,
            "assets_count": _integer(metrics.get("collection_count")) or len(collection),
            "total_value": _collection_value(collection_total),
            "total_value_label": _text(contract.get("top_value_label")),
            "items_count_label": _text(ui.get("items_count_label") or "Items Count"),
            "assets_unit": _text(ui.get("assets_unit") or "Assets"),
            "sbt_badges_label": _text(ui.get("sbt_badges_label") or "SBT Badges"),
            "no_sbt_label": _text(ui.get("no_sbt_label") or "No SBT"),
            "owned_prefix": _text(ui.get("owned_prefix") or "owned "),
            "brand_name": brand_name,
            "brand_site": brand_site,
            "update_date": update_date,
            "enable_tilt": False,
            "background_key": "classic",
            "background_image": "",
        }
        documents["collection"] = core.render_wallet_profile_template_document("collection", collection_context, embed_logo=False)
        order.append("collection")

    total_spent_rank, total_spent_tier = _rank(rankings, "total_spent")
    holdings_rank, holdings_tier = _rank(rankings, "holdings")
    pnl_rank, pnl_tier = _rank(rankings, "pnl")
    volume_rank, volume_tier = _rank(rankings, "trade_volume")
    active_days_rank, active_days_tier = _rank(rankings, "active_days")
    sbt_rank, sbt_tier = _rank(rankings, "sbt")

    contract_rows = []
    for row in history.get("pack_rows") or []:
        if not isinstance(row, dict):
            continue
        contract_rows.append(
            {
                "pack_name": _text(row.get("pack_name") or "Unknown Pack"),
                "contract": _text(row.get("contract")),
                "contract_short": _text(row.get("contract_short") or "—"),
                "open_count": _integer(row.get("open_count")),
                "unit_price": _text(row.get("unit_price") or "0.00"),
                "spent_total": _text(row.get("spent_total") or "0.00"),
            }
        )
    activity_rows = []
    for row in history.get("activity_rows") or []:
        if not isinstance(row, dict):
            continue
        activity_rows.append(
            {
                "name": _text(row.get("name") or "Activity"),
                "count": _integer(row.get("count")),
                "highlight": bool(row.get("highlight")),
            }
        )

    history_context = {
        "collection_name": _text(f"{profile_name_raw} Collection"),
        "brand_name": brand_name,
        "brand_site": brand_site,
        "wallet_short": _text(wallet_short),
        "update_date": update_date,
        "history_title": _text(labels.get("title") or "Collection History"),
        "history_subtitle": _text(labels.get("subtitle") or "Pack and trade history overview"),
        "history_range": _text(history.get("range") or "—"),
        "section_contract": _text(labels.get("section_contract") or "Contract / Pack Breakdown"),
        "section_activity": _text(labels.get("section_activity") or "Activity Counts"),
        "head_pack": _text(labels.get("head_pack") or "Pack"),
        "head_contract": _text(labels.get("head_contract") or "Contract"),
        "head_open_count": _text(labels.get("head_open_count") or "Opened"),
        "head_unit_price": _text(labels.get("head_unit_price") or "Unit (USDT)"),
        "head_spent_total": _text(labels.get("head_spent_total") or "Total (USDT)"),
        "empty_contract": _text(labels.get("empty_contract") or "No pack-open data available"),
        "metric_pack_spent_label": _text(labels.get("kpi_pack_spent") or "Pack Spend"),
        "metric_pack_spent_value": _money_value(metrics.get("pack_spent_usd")),
        "metric_total_spent_label": _text(labels.get("kpi_total_spent") or "Total Spent"),
        "metric_total_spent_note": _text(labels.get("kpi_total_spent_note")),
        "metric_total_spent_value": _money_value(metrics.get("total_spent_usd")),
        "metric_total_spent_rank": total_spent_rank,
        "metric_total_spent_rank_tier": total_spent_tier,
        "metric_total_earned_label": _text(labels.get("kpi_total_earned") or "Total Earned"),
        "metric_total_earned_note": _text(labels.get("kpi_total_earned_note")),
        "metric_total_earned_value": _money_value(metrics.get("total_earned_usd")),
        "metric_net_label": _text(labels.get("kpi_net") or "Net PnL"),
        "metric_net_note": _text(labels.get("kpi_net_note")),
        "metric_net_value": _signed_money(metrics.get("net_with_holdings_usd")),
        "metric_net_rank": pnl_rank,
        "metric_net_rank_tier": pnl_tier,
        "metric_trade_volume_label": _text(labels.get("kpi_trade_volume") or "Trade Volume"),
        "metric_trade_volume_value": _money_value(_number(metrics.get("market_bought_usd")) + _number(metrics.get("market_sold_usd"))),
        "metric_trade_volume_rank": volume_rank,
        "metric_trade_volume_rank_tier": volume_tier,
        "metric_assets_value_label": _text(labels.get("kpi_assets_value") or "Holdings Value"),
        "metric_assets_value_value": _money_value(metrics.get("holdings_value_usd")),
        "metric_assets_value_rank": holdings_rank,
        "metric_assets_value_rank_tier": holdings_tier,
        "metric_sbt_label": "SBT",
        "metric_sbt_value": f"{sbt_total:,}",
        "metric_sbt_rank": sbt_rank,
        "metric_sbt_rank_tier": sbt_tier,
        "metric_buyback_label": _text(labels.get("kpi_buyback") or "Buyback Total"),
        "metric_buyback_value": _money_value(metrics.get("buyback_usd")),
        "metric_market_buy_label": _text(labels.get("kpi_market_buy") or "Market Buy Total"),
        "metric_market_buy_value": _money_value(metrics.get("market_bought_usd")),
        "metric_market_sell_label": _text(labels.get("kpi_market_sell") or "Market Sell Total"),
        "metric_market_sell_value": _money_value(metrics.get("market_sold_usd")),
        "metric_card_withdraw_label": _text(labels.get("kpi_card_withdraw") or "Card Withdrawal Value"),
        "metric_card_withdraw_value": _money_value(metrics.get("card_withdraw_usd")),
        "active_days_label": _text(labels.get("kpi_active_days") or "Active Days"),
        "active_days_value": f"{_integer(metrics.get('active_days')):,}",
        "active_days_rank": active_days_rank,
        "active_days_rank_tier": active_days_tier,
        "contract_rows": contract_rows,
        "activity_rows": activity_rows,
    }
    if "history" in requested:
        documents["history"] = core.render_wallet_profile_template_document("history", history_context, embed_logo=False)
        order.append("history")

    extreme_items = []
    for kind in ("highest", "lowest"):
        row = extremes.get(kind) if isinstance(extremes, dict) else None
        if not isinstance(row, dict):
            continue
        extreme_items.append(
            {
                "name": _text(row.get("name") or "Unknown Collectible"),
                "image": _text(row.get("image_url")),
                "value": _collection_value(row.get("value_usd")),
            }
        )
    if extreme_items and "extremes" in requested:
        extreme_context = {
            "collection_name": _text(f"{profile_name_raw} Collection"),
            "sbt_total": sbt_total,
            "sbt_badges_display": badges[:7],
            "items": extreme_items,
            "assets_count": _integer(metrics.get("collection_count")),
            "total_value": _collection_value(sum(_number(row.get("value_usd")) for row in extremes.values() if isinstance(row, dict))),
            "items_count_label": _text(ui.get("items_count_label") or "Items Count"),
            "assets_unit": _text(ui.get("assets_unit") or "Assets"),
            "sbt_badges_label": _text(ui.get("sbt_badges_label") or "SBT Badges"),
            "no_sbt_label": _text(ui.get("no_sbt_label") or "No SBT"),
            "owned_prefix": _text(ui.get("owned_prefix") or "owned "),
            "brand_name": brand_name,
            "brand_site": brand_site,
            "update_date": update_date,
            "enable_tilt": False,
            "background_key": "classic",
            "background_image": "",
            "extreme_mode": True,
            "hide_footer": True,
        }
        documents["extremes"] = core.render_wallet_profile_template_document("extremes", extreme_context, embed_logo=False)
        order.append("extremes")

    return {
        "schema_version": POSTER_SCHEMA_VERSION,
        "source": "tcg_pro_canonical_html_templates",
        "width": POSTER_WIDTH,
        "height": POSTER_HEIGHT,
        "order": order,
        "documents": documents,
    }

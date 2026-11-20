"""Freeze rankings from checkout evidence, before any wallet-level aggregation."""
from __future__ import annotations

from .config import builder_code, wallet


def build_rankings(checkouts, *, definition, aliases, usernames, through_block, through_at, format_amount):
    groups = {None: {}}

    def canonical(address):
        seen = set()
        while address in aliases:
            if address in seen:
                raise ValueError("migration cycle")
            seen.add(address)
            address = aliases[address]
        return address

    def add(totals, address, entry):
        total = totals.setdefault(address, {"draw_count": 0, "paid_amount_raw": 0, "aliases": set()})
        total["draw_count"] += entry["draw_count"]
        total["paid_amount_raw"] += int(entry["paid_amount_raw"])
        total["aliases"].add(entry["wallet"])

    for entry in checkouts:
        if entry["block_number"] > through_block or entry["timestamp"] > through_at:
            raise ValueError("snapshot bound does not cover the committed ledger")
        code = entry.get("builder")
        if definition.checkout_schema_version >= 2:
            if definition.contract_schemas[entry["pack_contract"]] == "v3":
                code = builder_code(code)
            elif code is not None:
                raise ValueError("legacy checkout cannot claim a builder")
        address = canonical(entry["wallet"])
        add(groups[None], address, entry)
        if code is not None:
            add(groups.setdefault(code, {}), address, entry)

    # Identity is shared across scopes; payment quantities never are.
    all_totals = groups[None]
    for old in aliases:
        address = canonical(old)
        if address in all_totals:
            all_totals[address]["aliases"].add(old)
    names = {wallet(key): str(value) for key, value in (usernames or {}).items() if value is not None}
    ranked = {}
    for code, totals in groups.items():
        rows, previous_count, rank = [], None, 0
        for position, (address, total) in enumerate(sorted(totals.items(), key=lambda item: (-item[1]["draw_count"], item[0])), 1):
            if total["draw_count"] != previous_count:
                rank, previous_count = position, total["draw_count"]
            frozen_aliases = sorted(all_totals[address]["aliases"] | {address})
            username = names.get(address) or next((names[item] for item in frozen_aliases if names.get(item)), None)
            rows.append({"rank": rank, "wallet": address, "username": username, "aliases": frozen_aliases,
                         "draw_count": total["draw_count"], "paid_amount_raw": str(total["paid_amount_raw"]),
                         "paid_amount": format_amount(total["paid_amount_raw"], definition.token_decimals)})
        ranked[code] = rows
    return ranked

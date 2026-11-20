"""Complete block-range log queries that adapt only to provider dataset limits."""
from __future__ import annotations

from scripts.onchain_metrics import _fetch_etherscan_json


class LogResultWindowFull(RuntimeError):
    """Pagination reached the provider cap without proving the range complete."""


def block_at_or_before(cfg, timestamp):
    """Resolve a numeric cutoff using the existing shared request pacer.

    A malformed/error response never becomes block zero or an empty history.
    """
    response = _fetch_etherscan_json(cfg, {
        "chainid": cfg.chain_id, "module": "block", "action": "getblocknobytime",
        "timestamp": int(timestamp), "closest": "before", "apikey": cfg.api_key,
    })
    if (not isinstance(response, dict) or response.get("status") != "1"
            or not isinstance(response.get("result"), str) or not response["result"].isdigit()):
        raise RuntimeError("metrics_cutoff_block_unavailable")
    return int(response["result"])


def complete_bounded_logs(fetch_page, start, end, page_size, *, result_limit=10_000):
    """Return a complete inclusive range, splitting timed-out/capped datasets.

    Pages are buffered before return. If a later page times out, its entire range
    is queried again as disjoint children; earlier partial pages are discarded.
    Network/rate/auth/data errors propagate unchanged. A single block that cannot
    be exhausted also fails rather than returning a partial result.
    """
    if not isinstance(start, int) or not isinstance(end, int) or start < 0 or end < 0:
        raise ValueError("invalid log block bounds")
    if start > end:
        return []
    page_size = max(1, min(1000, int(page_size)))
    pages = result_limit // page_size
    if pages < 1:
        raise ValueError("invalid log pagination limit")
    try:
        rows = []
        for page in range(1, pages + 1):
            batch = fetch_page(start, end, page)
            if not isinstance(batch, list) or len(batch) > page_size or any(not isinstance(row, dict) for row in batch):
                raise RuntimeError("metrics_invalid_logs_page")
            rows.extend(batch)
            if len(batch) < page_size:
                return rows
        raise LogResultWindowFull("metrics_provider_page_limit")
    except RuntimeError as error:
        dataset_limit = isinstance(error, LogResultWindowFull) or "query timeout" in str(error).lower()
        if not dataset_limit or start == end:
            raise
    middle = (start + end) // 2
    return (complete_bounded_logs(fetch_page, start, middle, page_size, result_limit=result_limit)
            + complete_bounded_logs(fetch_page, middle + 1, end, page_size, result_limit=result_limit))

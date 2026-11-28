"""Read-only HTTP controller; the hosting Profile API retains authentication."""
from __future__ import annotations

import base64
import json
from urllib.parse import parse_qs, unquote, urlsplit

from .config import builder_code, load_event_definitions
from .store import BuilderFilterUnavailable, CheckoutEvidenceUnavailable, EventLeaderboardStore
from .query_retention import SnapshotExpired
from .checkout_pages import CheckoutCacheUnavailable

SORT_FIELDS = {"rank", "draw_count", "wallet", "username", "paid_amount"}


def _one(query, name, default=None):
    value = query.get(name, default)
    if isinstance(value, (list, tuple)):
        if len(value) != 1:
            raise ValueError("parameter must occur exactly once: " + name)
        return value[0]
    return value


def _cursor(payload):
    return base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode().rstrip("=")


def _decode_cursor(raw):
    if len(raw) > 4096:
        raise ValueError("invalid cursor")
    try:
        value = json.loads(base64.b64decode(raw + "=" * (-len(raw) % 4), altchars=b"-_", validate=True))
        required = {"snapshot_id", "sort", "order", "offset", "q"}
        if (not isinstance(value, dict) or set(value) not in (required, required | {"builder"})
                or not isinstance(value["offset"], int) or isinstance(value["offset"], bool) or value["offset"] < 0
                or not isinstance(value["snapshot_id"], str) or not value["snapshot_id"]):
            raise ValueError()
        value.setdefault("builder", None)
        if value["builder"] is not None:
            value["builder"] = builder_code(value["builder"])
        return value
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ValueError("invalid cursor") from exc


class EventLeaderboardHTTP:
    def __init__(self, store=None):
        self.store = store or EventLeaderboardStore()
        if not self.store.list_events():
            for definition in load_event_definitions():
                self.store.register_event(definition)

    def handle_request(self, path, query=None):
        return self.dispatch(path, query)

    def dispatch(self, path, query=None):
        parsed = urlsplit(path)
        if not parsed.path.startswith("/v1/leaderboards/"):
            return None
        pieces = parsed.path.removeprefix("/v1/leaderboards/").split("/")
        if len(pieces) not in (1, 2) or (len(pieces) == 2 and pieces[1] not in {"search", "checkouts"}) or not pieces[0]:
            return 404, {"error": "not_found"}
        event_id = unquote(pieces[0])
        query = parse_qs(parsed.query, keep_blank_values=True) if query is None else query
        search = len(pieces) == 2 and pieces[1] == 'search'
        if len(pieces) == 2 and pieces[1] == 'checkouts':
            return self.checkouts(event_id, query)
        try:
            self.store.get_event(event_id)
            snapshot_id = _one(query, "snapshot_id")
            raw_builder = _one(query, "builder")
            builder = builder_code(raw_builder) if raw_builder is not None else None
            sort = _one(query, "sort", "rank")
            order = _one(query, "order", "asc")
            if sort not in SORT_FIELDS or order not in {"asc", "desc"}:
                raise ValueError("invalid sort or order")
            limit = int(_one(query, "limit", "100"))
            if not 1 <= limit <= 1000:
                raise ValueError("limit must be between 1 and 1000")
            all_value = _one(query, "all", "false")
            if all_value not in {"true", "false"}:
                raise ValueError("all must be true or false")
            all_rows = all_value == "true"
            q = _one(query, "q", "").strip() if search else ""
            if search and (not q or len(q) > 200):
                raise ValueError("search requires q between 1 and 200 characters")
            raw_cursor = _one(query, "cursor")
            offset = 0
            if raw_cursor:
                if all_rows:
                    raise ValueError("all=true cannot be combined with cursor")
                cursor = _decode_cursor(raw_cursor)
                if raw_builder is not None and builder != cursor["builder"]:
                    raise ValueError("cursor does not match builder")
                if (snapshot_id and snapshot_id != cursor["snapshot_id"]) or any(
                    cursor[key] != value for key, value in (("sort", sort), ("order", order), ("q", q.casefold()))):
                    raise ValueError("cursor does not match snapshot or query")
                snapshot_id, offset = cursor["snapshot_id"], cursor["offset"]
                builder = cursor["builder"]
            if search and not snapshot_id:
                raise ValueError("search requires snapshot_id from the leaderboard response")
            metadata, rows, health = self.store.read_snapshot(event_id, snapshot_id, builder=builder)
            if metadata is None:
                return (503 if health["last_error"] else 202), {
                    "event_id": event_id, "snapshot_id": None, "builder": builder, "status": "initializing", **health,
                    "message": "No complete confirmed scan has been published yet."}
            builder = metadata["builder"]
            if search:
                needle = q.casefold()
                rows = [row for row in rows if needle in (row["username"] or "").casefold()
                        or any(needle in address for address in row["aliases"])]
            # Stable wallet tie order is independent of client sort direction.
            rows.sort(key=lambda row: row["wallet"])
            def sort_key(row):
                if sort == "paid_amount":
                    return int(row["paid_amount_raw"])
                if sort == "username":
                    return (row["username"] or "").casefold()
                return row[sort]
            rows.sort(key=sort_key, reverse=order == "desc")
            if offset > len(rows):
                raise ValueError("cursor offset outside snapshot")
            selected = rows if all_rows else rows[offset:offset + limit]
            next_offset = offset + len(selected)
            next_cursor = None if all_rows or next_offset >= len(rows) else _cursor({
                "snapshot_id": metadata["snapshot_id"], "sort": sort, "order": order,
                "offset": next_offset, "q": q.casefold(), "builder": builder})
            return 200, {**metadata, **health, "sort": sort, "order": order, "rows": selected,
                         "total_matches": len(rows), "next_cursor": next_cursor}
        except SnapshotExpired as exc:
            return 409, {"error": "snapshot_unavailable", "message": str(exc), "event_id": event_id}
        except BuilderFilterUnavailable as exc:
            return 409, {"error": "builder_filter_unavailable", "message": str(exc),
                         "event_id": event_id, "snapshot_id": snapshot_id, "builder": builder}
        except KeyError:
            return 404, {"error": "unknown_event", "event_id": event_id}
        except (ValueError, TypeError) as exc:
            return 400, {"error": "invalid_request", "message": str(exc)}

    def checkouts(self, event_id, query):
        snapshot_id = None
        builder = None
        try:
            if set(query) - {"snapshot_id", "builder", "limit", "cursor"}:
                raise ValueError("unsupported checkout query")
            snapshot_id = _one(query, "snapshot_id")
            raw_builder = _one(query, "builder")
            builder = builder_code(raw_builder) if raw_builder is not None else None
            limit = int(_one(query, "limit", "1000"))
            if not 1 <= limit <= 1000:
                raise ValueError("limit must be between 1 and 1000")
            offset = 0
            raw_cursor = _one(query, "cursor")
            if raw_cursor:
                cursor = _decode_cursor(raw_cursor)
                if (cursor["sort"] != "checkout" or cursor["order"] != "asc" or cursor["q"] != ""
                        or (snapshot_id and snapshot_id != cursor["snapshot_id"])
                        or (raw_builder is not None and builder != cursor["builder"])):
                    raise ValueError("cursor does not match checkout scope")
                snapshot_id, builder, offset = cursor["snapshot_id"], cursor["builder"], cursor["offset"]
            metadata, selected, health, total = self.store.read_checkout_page(
                event_id, snapshot_id, builder=builder, offset=offset, limit=limit)
            if metadata is None:
                return (503 if health["last_error"] else 202), {
                    "event_id": event_id, "snapshot_id": None, "builder": builder,
                    "status": "initializing", **health}
            builder = metadata["builder"]
            following = offset + len(selected)
            cursor = None if following >= total else _cursor({
                "snapshot_id": metadata["snapshot_id"], "builder": builder, "offset": following,
                "sort": "checkout", "order": "asc", "q": ""})
            identities = {}
            if offset == 0:
                _, global_rows, _ = self.store.read_snapshot(event_id, metadata["snapshot_id"])
                identities = {"identities": [
                    {key: row[key] for key in ("wallet", "aliases", "username")} for row in global_rows]}
            return 200, {**metadata, **health, **identities, "evidence_type": "confirmed_paid_checkout",
                         "rows": selected, "total_checkouts": total, "next_cursor": cursor}
        except SnapshotExpired as exc:
            return 409, {"error": "snapshot_unavailable", "message": str(exc), "event_id": event_id}
        except CheckoutCacheUnavailable:
            return 503, {"error": "checkout_cache_unavailable", "message": "Checkout evidence pagination is temporarily unavailable.", "event_id": event_id}
        except (BuilderFilterUnavailable, CheckoutEvidenceUnavailable) as exc:
            return 409, {"error": "checkout_evidence_unavailable", "message": str(exc),
                         "event_id": event_id, "snapshot_id": snapshot_id, "builder": builder}
        except KeyError:
            return 404, {"error": "unknown_event", "event_id": event_id}
        except (ValueError, TypeError) as exc:
            return 400, {"error": "invalid_request", "message": str(exc)}


def handle_request(path, query=None, store=None):
    return EventLeaderboardHTTP(store).dispatch(path, query)

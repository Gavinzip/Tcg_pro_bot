#!/usr/bin/env python3
from __future__ import annotations

from collections import OrderedDict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import time
from typing import Any
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

from runtime.profile_data import normalize_wallet_address
from runtime.profile_job_lock import ProfileJobLockTimeout, profile_job_lock
from runtime.profile_lookup_worker import (
    ProfileLookupWorker,
    ProfileLookupWorkerError,
    ProfileLookupWorkerTimeout,
)


load_dotenv()

HOST = str(os.getenv("PROFILE_API_HOST", "0.0.0.0") or "0.0.0.0").strip()
PORT = max(1, int(os.getenv("PROFILE_API_PORT", os.getenv("PORT", "8091")) or "8091"))
API_TOKEN = str(os.getenv("PROFILE_API_TOKEN", "") or "").strip()
CACHE_TTL_SECONDS = max(0, int(os.getenv("PROFILE_API_CACHE_TTL_SECONDS", "300") or "300"))
CACHE_MAX_ENTRIES = max(1, int(os.getenv("PROFILE_API_CACHE_MAX_ENTRIES", "256") or "256"))
MAX_CONCURRENT_LOOKUPS = max(1, int(os.getenv("PROFILE_API_MAX_CONCURRENT_LOOKUPS", "2") or "2"))
LOOKUP_WAIT_SECONDS = max(0.0, float(os.getenv("PROFILE_API_LOOKUP_WAIT_SECONDS", "3") or "3"))
TOTAL_TIMEOUT_SECONDS = max(5.0, float(os.getenv("PROFILE_API_TOTAL_TIMEOUT_SECONDS", "285") or "285"))
SINGLE_FLIGHT_WAIT_SECONDS = max(
    TOTAL_TIMEOUT_SECONDS + 5.0,
    float(os.getenv("PROFILE_API_SINGLE_FLIGHT_WAIT_SECONDS", "125") or "125"),
)
SUPPORTED_LANGUAGES = {"zh-Hant", "zh-Hans", "en", "ko"}
LANGUAGE_ALIASES = {"zh": "zh-Hant", "zhs": "zh-Hans"}
SUPPORTED_POSTERS = {"collection", "history", "extremes"}

_LOOKUP_SEMAPHORE = threading.BoundedSemaphore(MAX_CONCURRENT_LOOKUPS)
_CACHE_LOCK = threading.Lock()
_CACHE: OrderedDict[str, tuple[float, dict]] = OrderedDict()
_INFLIGHT_LOCK = threading.Lock()
_INFLIGHT: dict[str, dict[str, Any]] = {}
_PROFILE_WORKER = ProfileLookupWorker()


def _env_true(value: str | None, default: bool = False) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def _publish_inflight(cache_key: str, inflight: dict[str, Any]) -> None:
    with _INFLIGHT_LOCK:
        if _INFLIGHT.get(cache_key) is inflight:
            _INFLIGHT.pop(cache_key, None)
    inflight["event"].set()


class ProfileApiHandler(BaseHTTPRequestHandler):
    server_version = "RenaissProfileAPI/1.0"

    def _send_json(self, status: HTTPStatus, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            return

    def _authorized(self) -> bool:
        if not API_TOKEN:
            return True
        return str(self.headers.get("Authorization") or "").strip() == f"Bearer {API_TOKEN}"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path in {"/", "/healthz"}:
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "service": "tcg-profile-api",
                    "max_concurrent_lookups": MAX_CONCURRENT_LOOKUPS,
                    "total_timeout_seconds": TOTAL_TIMEOUT_SECONDS,
                },
            )
            return
        if path != "/v1/profile":
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not found"})
            return
        if not self._authorized():
            self._send_json(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "unauthorized"})
            return

        query = parse_qs(parsed.query, keep_blank_values=False)
        wallet = normalize_wallet_address(str((query.get("wallet") or query.get("address") or [""])[0]))
        if not wallet:
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "valid wallet address is required"})
            return
        requested_language = str((query.get("lang") or ["zh-Hant"])[0] or "zh-Hant").strip()
        language = LANGUAGE_ALIASES.get(requested_language, requested_language)
        if language not in SUPPORTED_LANGUAGES:
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "unsupported language"})
            return
        poster = str((query.get("poster") or [""])[0] or "").strip().lower() or None
        if poster is not None and poster not in SUPPORTED_POSTERS:
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "unsupported poster"})
            return
        include_extremes = _env_true(str((query.get("include_extremes") or ["1"])[0]), True)
        include_posters = _env_true(str((query.get("include_posters") or ["0"])[0]), False)
        cache_key = f"{wallet}:{language}:{poster or 'all'}:{1 if include_extremes else 0}:{1 if include_posters else 0}"
        now = time.time()
        with _CACHE_LOCK:
            expired_keys = [
                key for key, (cached_at, _) in _CACHE.items()
                if CACHE_TTL_SECONDS <= 0 or now - cached_at > CACHE_TTL_SECONDS
            ]
            for key in expired_keys:
                _CACHE.pop(key, None)
            cached = _CACHE.get(cache_key)
            if cached:
                _CACHE.move_to_end(cache_key)
        if cached and CACHE_TTL_SECONDS > 0 and now - cached[0] <= CACHE_TTL_SECONDS:
            payload = dict(cached[1])
            payload["cache"] = "hit"
            self._send_json(HTTPStatus.OK, payload)
            return

        with _INFLIGHT_LOCK:
            inflight = _INFLIGHT.get(cache_key)
            is_leader = inflight is None
            if inflight is None:
                inflight = {
                    "event": threading.Event(),
                    "payload": None,
                    "status": HTTPStatus.BAD_GATEWAY,
                    "error": "profile_lookup_failed",
                }
                _INFLIGHT[cache_key] = inflight

        if not is_leader:
            if not inflight["event"].wait(timeout=SINGLE_FLIGHT_WAIT_SECONDS):
                self._send_json(
                    HTTPStatus.TOO_MANY_REQUESTS,
                    {"ok": False, "error": "profile lookup capacity reached; retry shortly"},
                )
                return
            shared_payload = inflight.get("payload")
            if isinstance(shared_payload, dict):
                payload = dict(shared_payload)
                payload["cache"] = "hit"
                self._send_json(HTTPStatus.OK, payload)
            else:
                self._send_json(
                    inflight.get("status", HTTPStatus.BAD_GATEWAY),
                    {"ok": False, "error": str(inflight.get("error") or "profile_lookup_failed")},
                )
            return

        try:
            if not _LOOKUP_SEMAPHORE.acquire(timeout=LOOKUP_WAIT_SECONDS):
                inflight["status"] = HTTPStatus.TOO_MANY_REQUESTS
                inflight["error"] = "profile lookup capacity reached; retry shortly"
                _publish_inflight(cache_key, inflight)
                self._send_json(
                    HTTPStatus.TOO_MANY_REQUESTS,
                    {"ok": False, "error": "profile lookup capacity reached; retry shortly"},
                )
                return
            started = time.perf_counter()
            deadline = time.monotonic() + TOTAL_TIMEOUT_SECONDS
            try:
                lock_wait_seconds = max(0.0, deadline - time.monotonic())
                with profile_job_lock(wait_seconds=lock_wait_seconds):
                    remaining_seconds = max(0.1, deadline - time.monotonic())
                    payload = _PROFILE_WORKER.run(
                        wallet=wallet,
                        language=language,
                        include_extremes=include_extremes,
                        include_posters=include_posters,
                        poster_kind=poster,
                        timeout_seconds=remaining_seconds,
                    )
            except ProfileJobLockTimeout:
                inflight["status"] = HTTPStatus.TOO_MANY_REQUESTS
                inflight["error"] = "profile job queue wait timed out; retry shortly"
                _publish_inflight(cache_key, inflight)
                self._send_json(
                    HTTPStatus.TOO_MANY_REQUESTS,
                    {"ok": False, "error": "profile job queue wait timed out; retry shortly"},
                )
                return
            except ProfileLookupWorkerTimeout:
                inflight["status"] = HTTPStatus.GATEWAY_TIMEOUT
                inflight["error"] = "profile_lookup_timeout"
                _publish_inflight(cache_key, inflight)
                self._send_json(
                    HTTPStatus.GATEWAY_TIMEOUT,
                    {"ok": False, "error": "profile_lookup_timeout"},
                )
                return
            except ProfileLookupWorkerError as exc:
                print(f"[profile-api] lookup failed poster={poster or 'all'} cause={exc.cause_name}", flush=True)
                _publish_inflight(cache_key, inflight)
                self._send_json(HTTPStatus.BAD_GATEWAY, {"ok": False, "error": "profile_lookup_failed"})
                return
            except Exception as exc:
                inflight["status"] = HTTPStatus.INTERNAL_SERVER_ERROR
                print(f"[profile-api] unexpected lookup failure wallet={wallet} cause={type(exc).__name__}")
                _publish_inflight(cache_key, inflight)
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"ok": False, "error": "profile_lookup_failed"},
                )
                return
            finally:
                _LOOKUP_SEMAPHORE.release()

            payload["cache"] = "miss"
            payload.setdefault("timings", {})["total_seconds"] = round(time.perf_counter() - started, 3)
            stage_timings = payload.get("timings") if isinstance(payload.get("timings"), dict) else {}
            source_cache = payload.get("source_cache") if isinstance(payload.get("source_cache"), dict) else {}
            print(
                "[profile-api] lookup complete "
                f"poster={poster or 'all'} total={stage_timings.get('total_seconds', 0)}s "
                f"history={stage_timings.get('history_seconds', 0)}s "
                f"collection={stage_timings.get('collection_seconds', 0)}s "
                f"sbt={stage_timings.get('sbt_seconds', 0)}s "
                f"extremes={stage_timings.get('extremes_seconds', 0)}s "
                f"source_cache={source_cache}",
                flush=True,
            )
            inflight["payload"] = dict(payload)
            with _CACHE_LOCK:
                _CACHE[cache_key] = (time.time(), dict(payload))
                _CACHE.move_to_end(cache_key)
                while len(_CACHE) > CACHE_MAX_ENTRIES:
                    _CACHE.popitem(last=False)
            _publish_inflight(cache_key, inflight)
            self._send_json(HTTPStatus.OK, payload)
        finally:
            _publish_inflight(cache_key, inflight)

    def log_message(self, format: str, *args) -> None:
        return


class ProfileApiServer(ThreadingHTTPServer):
    daemon_threads = True


def main() -> int:
    _PROFILE_WORKER.start()
    server = ProfileApiServer((HOST, PORT), ProfileApiHandler)
    print(
        f"[profile-api] listening on {HOST}:{PORT}; "
        f"concurrency={MAX_CONCURRENT_LOOKUPS}; cache_ttl={CACHE_TTL_SECONDS}s; "
        f"cache_max_entries={CACHE_MAX_ENTRIES}; total_timeout={TOTAL_TIMEOUT_SECONDS}s"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        _PROFILE_WORKER.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

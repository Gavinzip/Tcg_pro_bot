from __future__ import annotations

import threading
import time
from collections.abc import Callable

DEFAULT_RENAISS_CARD_API_BASE_URL = "https://api.renaiss.xyz/v1/cards"
_CARD_API_REQUEST_LOCK = threading.Lock()
_CARD_API_LAST_REQUEST_AT = 0.0


class _RetryableHttpStatus(RuntimeError):
    def __init__(self, status_code: int, response) -> None:
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code
        self.response = response


def fetch_card_api_collectible(
    token_id: str,
    *,
    http_get: Callable,
    base_url: str = DEFAULT_RENAISS_CARD_API_BASE_URL,
    timeout: float = 25,
    max_retries: int = 4,
    retry_backoff_sec: float = 0.8,
    min_request_interval_sec: float = 0.25,
) -> dict:
    """Fetch one collectible from the official Renaiss v1 card endpoint."""
    global _CARD_API_LAST_REQUEST_AT

    token = str(token_id or "").strip()
    if not token:
        return {}

    url = f"{str(base_url or DEFAULT_RENAISS_CARD_API_BASE_URL).rstrip('/')}/{token}"
    last_err: Exception | None = None
    retries = max(1, int(max_retries or 1))

    attempts_made = 0
    for attempt in range(1, retries + 1):
        attempts_made = attempt
        try:
            # Coordinate request start times without holding the lock during HTTP I/O.
            # Profile collection workers can therefore overlap the slower v1 calls.
            with _CARD_API_REQUEST_LOCK:
                min_interval = max(0.0, float(min_request_interval_sec))
                wait_for_slot = min_interval - (time.monotonic() - _CARD_API_LAST_REQUEST_AT)
                if wait_for_slot > 0:
                    time.sleep(wait_for_slot)
                _CARD_API_LAST_REQUEST_AT = time.monotonic()

            response = http_get(
                url,
                params={
                    "includeActivities": "true",
                    "verbosePrice": "true",
                },
                headers={
                    "Accept": "application/json",
                    "User-Agent": "renaiss-cli-compatible/1.0",
                },
                timeout=timeout,
            )
            status = int(response.status_code or 0)
            if status == 404:
                return {}
            if status == 429 or status >= 500:
                raise _RetryableHttpStatus(status, response)
            response.raise_for_status()

            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError("Renaiss card API returned a non-object payload")
            collectible = payload.get("collectible")
            if not isinstance(collectible, dict):
                raise RuntimeError("Renaiss card API response is missing collectible")
            return collectible
        except Exception as exc:
            last_err = exc
            status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
            response = getattr(exc, "response", None)
            if status is None and response is not None:
                status = int(response.status_code or 0)
            retryable = (
                status in (408, 409, 425, 429)
                or (status is not None and status >= 500)
                or status is None
            )
            if retryable and attempt < retries:
                wait_sec = max(0.0, float(retry_backoff_sec)) * (2 ** (attempt - 1))
                retry_after = None
                headers = getattr(response, "headers", None)
                if headers is not None:
                    try:
                        retry_after = float(headers.get("Retry-After") or 0)
                    except (TypeError, ValueError):
                        retry_after = None
                time.sleep(max(wait_sec, retry_after or 0.0))
                continue
            break

    raise RuntimeError(f"Renaiss card API request failed after {attempts_made} attempts: {last_err}")

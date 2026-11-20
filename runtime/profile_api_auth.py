"""Separate revocable website credentials from the internal Profile API token."""
from __future__ import annotations

import hmac
import json
import re
from collections.abc import Mapping


CLIENT_TOKEN_ENV = "EVENT_LEADERBOARD_CLIENT_TOKENS_JSON"


def load_leaderboard_client_tokens(raw: str | None, *, profile_token: str = "") -> dict[str, str]:
    """Reject invalid explicit configuration without including credential values."""
    if raw is None or not raw.strip():
        return {}

    def unique_object(pairs):
        result = {}
        for name, value in pairs:
            if name in result:
                raise ValueError()
            result[name] = value
        return result

    try:
        clients = json.loads(raw, object_pairs_hook=unique_object)
        if not isinstance(clients, dict) or not clients:
            raise ValueError()
        for name, token in clients.items():
            if (not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", name)
                    or not isinstance(token, str) or not re.fullmatch(r"[!-~]{32,256}", token)
                    or token == profile_token):
                raise ValueError()
        if len(set(clients.values())) != len(clients):
            raise ValueError()
        return clients
    except (ValueError, TypeError, RecursionError):
        raise ValueError(f"{CLIENT_TOKEN_ENV} must map unique client IDs to distinct "
                         "32-256 character ASCII tokens, separate from PROFILE_API_TOKEN; "
                         "credential values withheld") from None


def authorized(authorization: str | None, *, profile_token: str,
               leaderboard_clients: Mapping[str, str], leaderboard: bool = False) -> bool:
    # Preserve the existing unconfigured local mode. Configuring even website-only
    # credentials closes anonymous access to both protected route families.
    if not profile_token and not leaderboard_clients:
        return True
    header = str(authorization or "").strip()
    if not header.startswith("Bearer "):
        return False
    supplied = header.removeprefix("Bearer ").encode("utf-8")
    accepted = False
    candidates = ([profile_token] if profile_token else [])
    if leaderboard:
        candidates += list(leaderboard_clients.values())
    for token in candidates:
        accepted |= hmac.compare_digest(supplied, token.encode("utf-8"))
    return accepted

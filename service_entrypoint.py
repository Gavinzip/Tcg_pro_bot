#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
import sys

from runtime.service_supervisor import ServiceProcess, run_supervised_processes


SERVICE_TARGETS = {
    "bot": "bot.py",
    "profile-api": "profile_api.py",
    "profile_api": "profile_api.py",
}


def _env_true(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _service_script(name: str) -> Path:
    script = Path(__file__).resolve().with_name(name)
    if not script.is_file():
        raise FileNotFoundError(f"Service entrypoint is missing: {script}")
    return script


def _run_bot_with_profile_api() -> int:
    shared_env = dict(os.environ)
    profile_env = dict(shared_env)
    # PORT belongs to the bot health server in combined mode.
    profile_env.setdefault("PROFILE_API_HOST", "0.0.0.0")
    profile_env.setdefault("PROFILE_API_PORT", "8091")
    if str(profile_env["PROFILE_API_PORT"]).strip() == "8080":
        raise SystemExit(
            "PROFILE_API_PORT must differ from the bot health port 8080 in combined mode"
        )

    return run_supervised_processes(
        [
            ServiceProcess("profile-api", _service_script("profile_api.py"), profile_env),
            ServiceProcess("bot", _service_script("bot.py"), shared_env),
        ]
    )


def main() -> int:
    mode = str(os.getenv("TCG_SERVICE_MODE", "bot") or "bot").strip().lower()
    if mode == "bot" and _env_true(os.getenv("PROFILE_API_ENABLED")):
        return _run_bot_with_profile_api()
    target = SERVICE_TARGETS.get(mode)
    if not target:
        supported = ", ".join(sorted(SERVICE_TARGETS))
        raise SystemExit(f"Unsupported TCG_SERVICE_MODE={mode!r}; expected one of: {supported}")
    script = _service_script(target)
    os.execv(sys.executable, [sys.executable, str(script)])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

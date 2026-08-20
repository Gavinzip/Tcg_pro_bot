#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
import sys


SERVICE_TARGETS = {
    "bot": "bot.py",
    "profile-api": "profile_api.py",
    "profile_api": "profile_api.py",
}


def main() -> int:
    mode = str(os.getenv("TCG_SERVICE_MODE", "bot") or "bot").strip().lower()
    target = SERVICE_TARGETS.get(mode)
    if not target:
        supported = ", ".join(sorted(SERVICE_TARGETS))
        raise SystemExit(f"Unsupported TCG_SERVICE_MODE={mode!r}; expected one of: {supported}")
    script = Path(__file__).resolve().with_name(target)
    if not script.is_file():
        raise FileNotFoundError(f"Service entrypoint is missing: {script}")
    os.execv(sys.executable, [sys.executable, str(script)])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

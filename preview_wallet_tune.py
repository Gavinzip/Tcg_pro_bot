#!/usr/bin/env python3
import argparse
import asyncio
import os
import re
import tempfile

import bot


def _set_css_var(content: str, var_name: str, value: str) -> str:
    pattern = rf"({re.escape(var_name)}\s*:\s*)([^;]+)(;)"
    updated, n = re.subn(pattern, rf"\g<1>{value}\3", content, count=1)
    if n == 0:
        raise ValueError(f"CSS variable not found in template: {var_name}")
    return updated


def _pct(v: float) -> str:
    s = f"{v:.3f}".rstrip("0").rstrip(".")
    return f"{s}%"


def parse_args():
    p = argparse.ArgumentParser(description="Quick wallet poster tuning preview")
    p.add_argument("--wallet", default="0x00f82d2f05280a7888a39d724486fcab808d17b2")
    p.add_argument("--out", default="debug_wallet_profile_latest")
    p.add_argument("--name", default="wallet_profile_tune_cli")
    p.add_argument("--template", default=None, help="Optional template path (e.g. wallet_profile＿beta.html)")

    p.add_argument("--scale", type=float, default=None, help="e.g. 1.15")
    p.add_argument("--shift-x", type=float, default=None, help="percent, e.g. -1.2")
    p.add_argument("--shift-y", type=float, default=None, help="percent, e.g. 3.5")
    p.add_argument("--pos-x", type=float, default=None, help="percent, e.g. 50")
    p.add_argument("--pos-y", type=float, default=None, help="percent, e.g. 50")
    p.add_argument("--crop-top", type=float, default=None, help="percent")
    p.add_argument("--crop-right", type=float, default=None, help="percent")
    p.add_argument("--crop-bottom", type=float, default=None, help="percent")
    p.add_argument("--crop-left", type=float, default=None, help="percent")
    p.add_argument("--frame-gap", type=float, default=None, help="percent")
    return p.parse_args()


async def main():
    args = parse_args()
    wallet = bot._normalize_wallet_address(args.wallet)
    if not wallet:
        raise SystemExit("Invalid wallet format")

    template_path = os.path.abspath(args.template) if args.template else bot.PROFILE_TEMPLATE_PATH
    if not os.path.exists(template_path):
        raise SystemExit(f"Template not found: {template_path}")

    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()

    mapping = [
        ("--card-scale", None, args.scale),
        ("--card-shift-x", _pct, args.shift_x),
        ("--card-shift-y", _pct, args.shift_y),
        ("--card-pos-x", _pct, args.pos_x),
        ("--card-pos-y", _pct, args.pos_y),
        ("--card-crop-top", _pct, args.crop_top),
        ("--card-crop-right", _pct, args.crop_right),
        ("--card-crop-bottom", _pct, args.crop_bottom),
        ("--card-crop-left", _pct, args.crop_left),
        ("--frame-side-gap", _pct, args.frame_gap),
    ]
    for var, formatter, raw in mapping:
        if raw is None:
            continue
        val = f"{raw:.4f}".rstrip("0").rstrip(".") if formatter is None else formatter(raw)
        template = _set_css_var(template, var, val)

    tmp = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8")
    tmp.write(template)
    tmp.close()

    context = bot._build_wallet_profile_context(wallet)
    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)

    original_template = bot.PROFILE_TEMPLATE_PATH
    try:
        bot.PROFILE_TEMPLATE_PATH = tmp.name
        out_path = await bot._render_wallet_profile_poster(context, out_dir, safe_name=args.name)
    finally:
        bot.PROFILE_TEMPLATE_PATH = original_template
        try:
            os.unlink(tmp.name)
        except OSError:
            pass

    print(out_path)


if __name__ == "__main__":
    asyncio.run(main())

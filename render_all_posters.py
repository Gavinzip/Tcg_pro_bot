#!/usr/bin/env python3
"""Render all 3 wallet profile posters for a given wallet address."""
import asyncio
import os
import sys
import traceback

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bot

WALLET = sys.argv[1] if len(sys.argv) > 1 else "0x642fb63947a957a029dcdF82Aa114216E4367561"
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug_wallet_profile_latest")


async def main():
    wallet = bot._normalize_wallet_address(WALLET)
    if not wallet:
        print(f"❌ Invalid wallet: {WALLET}")
        return

    print(f"🔍 Building profile context for {wallet}...", flush=True)
    loop = asyncio.get_running_loop()
    try:
        profile_ctx = await loop.run_in_executor(None, bot._build_wallet_profile_context, wallet)
    except Exception as e:
        print(f"❌ Failed to build profile context: {e}", flush=True)
        traceback.print_exc()
        return

    if not profile_ctx:
        print("❌ profile_ctx is None/empty", flush=True)
        return

    print(f"✅ Context built. profile_poster_enabled={profile_ctx.get('profile_poster_enabled')}, count={profile_ctx.get('count')}", flush=True)

    os.makedirs(OUT_DIR, exist_ok=True)
    safe_name = f"wallet_{wallet[-6:]}"

    print("🖼️  Rendering poster 1 (collection)...", flush=True)
    try:
        p1 = await bot._render_wallet_profile_poster(profile_ctx, OUT_DIR, safe_name=safe_name)
        print(f"  ✅ {p1}", flush=True)
    except Exception as e:
        print(f"  ❌ Poster 1 failed: {e}", flush=True)
        traceback.print_exc()
        p1 = None

    print("🖼️  Rendering poster 2 (history)...", flush=True)
    try:
        p2 = await bot._render_wallet_profile_history_poster(profile_ctx, OUT_DIR, safe_name=safe_name)
        print(f"  ✅ {p2}", flush=True)
    except Exception as e:
        print(f"  ❌ Poster 2 failed: {e}", flush=True)
        traceback.print_exc()
        p2 = None

    print("🖼️  Rendering poster 3 (extremes)...", flush=True)
    try:
        p3 = await bot._render_wallet_profile_extreme_poster(profile_ctx, OUT_DIR, safe_name=safe_name)
        print(f"  ✅ {p3}", flush=True)
    except Exception as e:
        print(f"  ❌ Poster 3 failed: {e}", flush=True)
        traceback.print_exc()
        p3 = None

    print("\n✅ Done! Output files:", flush=True)
    for p in [p1, p2, p3]:
        if p:
            print(f"   {p}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())

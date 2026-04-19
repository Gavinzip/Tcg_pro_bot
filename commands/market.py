from __future__ import annotations

import asyncio
import os
import sys
import traceback
from datetime import datetime, timezone

import discord


async def handle_market(
    interaction: discord.Interaction,
    *,
    deps: dict,
) -> None:
    language_select_view_cls = deps["LanguageSelectView"]
    translate = deps["translate"]
    lang_flags = deps["LANG_FLAGS"]
    lang_labels = deps["LANG_LABELS"]
    market_recent_limit = deps["MARKET_RECENT_LIMIT"]
    market_source_channel_id = deps["MARKET_SOURCE_CHANNEL_ID"]
    market_browser_view_cls = deps["MarketBrowserView"]
    market_load_cached_index = deps["market_load_cached_index"]
    market_collect_listings = deps["market_collect_listings"]
    market_live_sync_refresh = deps["market_live_sync_refresh"]
    market_index_path = deps["market_index_path"]

    thread: discord.Thread | None = None
    lang = "zh"
    try:
        lang_view = language_select_view_cls(interaction.user.id, timeout_seconds=20)
        await interaction.response.send_message(
            "🌐 請先選擇語言 / Please choose language",
            view=lang_view,
            ephemeral=False,
        )
        starter = await interaction.original_response()
        picked_lang, selected = await lang_view.wait_for_choice()
        lang = picked_lang if selected and picked_lang else "zh"
        lang_view._disable_all()
        try:
            if selected:
                await starter.edit(
                    content=translate(
                        lang,
                        f"✅ 語言已選擇：{lang_flags.get(lang, '🌐')} **{lang_labels.get(lang, '繁體中文')}**\n📦 正在載入 Market Browser...",
                        f"✅ Language selected: {lang_flags.get(lang, '🌐')} **{lang_labels.get(lang, 'English')}**\n📦 Loading Market Browser...",
                        f"✅ 언어 선택 완료: {lang_flags.get(lang, '🌐')} **{lang_labels.get(lang, '한국어')}**\n📦 Market Browser를 불러오는 중...",
                        f"✅ 语言已选择：{lang_flags.get(lang, '🌐')} **{lang_labels.get(lang, '简体中文')}**\n📦 正在载入 Market Browser...",
                    ),
                    view=lang_view,
                )
            else:
                await starter.edit(
                    content=translate(
                        lang,
                        "⏱️ 20 秒未選擇，已使用預設語言：🇹🇼 **繁體中文**。\n📦 正在載入 Market Browser...",
                        "⏱️ No selection in 20s. Default language set to 🇹🇼 **Traditional Chinese**.\n📦 Loading Market Browser...",
                        "⏱️ 20초 내 미선택으로 기본 언어 🇹🇼 **번체 중국어**를 사용합니다.\n📦 Market Browser를 불러오는 중...",
                        "⏱️ 20 秒未选择，已使用默认语言：🇹🇼 **繁体中文**。\n📦 正在载入 Market Browser...",
                    ),
                    view=lang_view,
                )
        except Exception:
            pass

        if isinstance(interaction.channel, discord.Thread):
            thread = interaction.channel
        else:
            channel = interaction.channel
            if channel is None:
                print("❌ /market 互動已失效且找不到可用頻道。", file=sys.stderr)
                return

            thread_name = f"market-{datetime.now().strftime('%m%d-%H%M')}"
            try:
                thread = await starter.create_thread(name=thread_name, auto_archive_duration=60)
            except Exception as e:
                print(f"❌ /market 建立討論串失敗: {e}", file=sys.stderr)
                await starter.reply(f"❌ 建立討論串失敗：{e}")
                return
            try:
                await thread.add_user(interaction.user)
            except Exception:
                pass

        if thread is None:
            raise RuntimeError("market thread is not available")

        cached_items, cached_meta = market_load_cached_index(limit=market_recent_limit)
        if bool(cached_meta.get("from_cache")) and (cached_items or os.path.exists(market_index_path())):
            listings = list(cached_items or [])
            meta = dict(cached_meta or {})
            error = str(meta.get("error") or "").strip()
            asyncio.create_task(market_live_sync_refresh("market_open"))
        else:
            listings, meta = await market_collect_listings()
            error = str(meta.get("error") or "").strip()

        if error:
            if bool(meta.get("used_cache_fallback")):
                updated_at = str(meta.get("updated_at") or "").strip() or "-"
                await thread.send(
                    translate(
                        lang,
                        "⚠️ 讀取 market 來源失敗，已改用本地快取。\n"
                        f"Error: `{error}`\n"
                        f"Cache Updated: `{updated_at}`\n"
                        f"Cache Path: `{meta.get('index_path') or market_index_path()}`",
                        "⚠️ Failed to read market source. Using local cache.\n"
                        f"Error: `{error}`\n"
                        f"Cache Updated: `{updated_at}`\n"
                        f"Cache Path: `{meta.get('index_path') or market_index_path()}`",
                        "⚠️ 마켓 소스 읽기에 실패하여 로컬 캐시를 사용합니다.\n"
                        f"Error: `{error}`\n"
                        f"Cache Updated: `{updated_at}`\n"
                        f"Cache Path: `{meta.get('index_path') or market_index_path()}`",
                        "⚠️ 读取 market 来源失败，已改用本地缓存。\n"
                        f"Error: `{error}`\n"
                        f"Cache Updated: `{updated_at}`\n"
                        f"Cache Path: `{meta.get('index_path') or market_index_path()}`",
                    )
                )
            else:
                await thread.send(
                    translate(
                        lang,
                        f"⚠️ 讀取 market 來源時發生問題：`{error}`",
                        f"⚠️ Error reading market source: `{error}`",
                        f"⚠️ 마켓 소스 읽기 오류: `{error}`",
                        f"⚠️ 读取 market 来源时发生问题：`{error}`",
                    )
                )

        view = market_browser_view_cls(interaction.user.id, listings, meta=meta, lang=lang)
        panel_msg = await thread.send(view.render_message(), embeds=view.render_embeds(), view=view)
        view.bind_message(panel_msg)
    except Exception as e:
        print(f"❌ /market 失敗: {e}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        err_text = translate(
            lang,
            f"❌ `/market` 執行失敗：{e}",
            f"❌ `/market` failed: {e}",
            f"❌ `/market` 실행 실패: {e}",
            f"❌ `/market` 执行失败：{e}",
        )
        if thread is not None:
            try:
                await thread.send(err_text)
                return
            except Exception:
                pass
        try:
            if interaction.response.is_done():
                await interaction.followup.send(err_text, ephemeral=True)
            else:
                await interaction.response.send_message(err_text, ephemeral=True)
            return
        except Exception:
            pass
        channel = interaction.channel
        if channel is not None:
            try:
                await channel.send(err_text)
            except Exception:
                pass


async def handle_market_bootstrap(
    interaction: discord.Interaction,
    force: bool,
    push_backup: bool,
    *,
    deps: dict,
) -> None:
    market_collect_listings = deps["market_collect_listings"]
    market_write_index_file = deps["market_write_index_file"]
    market_source_channel_id = deps["MARKET_SOURCE_CHANNEL_ID"]
    market_index_path = deps["market_index_path"]
    market_cache_images_dir = deps["market_cache_images_dir"]
    market_save_bootstrap_state = deps["market_save_bootstrap_state"]
    run_ranking_sync_script = deps["run_ranking_sync_script"]

    await interaction.response.send_message(
        "⏳ 正在執行 market 全掃（每次都完整重掃，含封存討論串），完成後會回報索引路徑...",
        ephemeral=True,
    )
    started_at = datetime.now(timezone.utc)
    listings, meta = await market_collect_listings(
        include_archived=True,
        limit=None,
        archived_limit=None,
    )
    error = str(meta.get("error") or "").strip()
    if error:
        await interaction.followup.send(
            f"❌ `market_bootstrap` 失敗：`{error}`",
            ephemeral=True,
        )
        return

    index_write_ok, index_write_error, forced_index_path, forced_updated_at = market_write_index_file(
        listings=listings,
        source_channel_id=int(meta.get("source_channel_id") or market_source_channel_id),
        include_archived=True,
        scanned_thread_count=int(meta.get("scanned_thread_count") or 0),
        active_thread_count=int(meta.get("active_thread_count") or 0),
        recent_limit="all",
    )
    if not index_write_ok:
        await interaction.followup.send(
            f"❌ `market_bootstrap` 失敗：索引寫入失敗 `{index_write_error}`\nPath: `{forced_index_path}`",
            ephemeral=True,
        )
        return

    state_payload: dict[str, object] = {
        "done": True,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at.isoformat(),
        "forced": bool(force),
        "mode": "always_rescan",
        "source_channel_id": int(meta.get("source_channel_id") or market_source_channel_id),
        "include_archived": True,
        "scanned_thread_count": int(meta.get("scanned_thread_count") or 0),
        "listing_count": len(listings),
        "index_path": str(forced_index_path or meta.get("index_path") or market_index_path()),
        "images_dir": str(meta.get("images_dir") or market_cache_images_dir()),
        "index_updated_at": str(forced_updated_at or meta.get("updated_at") or ""),
    }
    market_save_bootstrap_state(state_payload)

    summary_msg = (
        "✅ `market_bootstrap` 完成（本指令每次都會完整重掃）。\n"
        f"Scanned Threads: `{state_payload['scanned_thread_count']}`\n"
        f"Matched Listings: `{state_payload['listing_count']}`\n"
        f"Index: `{state_payload['index_path']}`\n"
        f"Images: `{state_payload['images_dir']}`"
    )
    if not push_backup:
        await interaction.followup.send(summary_msg, ephemeral=True)
        return

    await interaction.followup.send(summary_msg + "\n⏳ 正在觸發備份推送...", ephemeral=True)
    backup_ok = await run_ranking_sync_script(
        "market_bootstrap",
        bootstrap_only=False,
        full_rebuild=False,
        push_only=True,
        market_only=True,
    )
    await interaction.followup.send(
        (
            "✅ 備份推送流程已完成（請到資料 repo 檢查最新 commit）。"
            if backup_ok
            else "⚠️ 備份推送失敗，請檢查 BACKUP_GIT_ENABLED / BACKUP_GIT_REPO 與伺服器網路。"
        ),
        ephemeral=True,
    )


async def handle_market_push_backup(
    interaction: discord.Interaction,
    *,
    deps: dict,
) -> None:
    run_ranking_sync_script = deps["run_ranking_sync_script"]

    await interaction.response.send_message(
        "⏳ 正在執行 market 備份推送（push-only）...",
        ephemeral=True,
    )
    backup_ok = await run_ranking_sync_script(
        "market_manual_push",
        bootstrap_only=False,
        full_rebuild=False,
        push_only=True,
        market_only=True,
    )
    await interaction.followup.send(
        (
            "✅ market 備份推送完成。"
            if backup_ok
            else "⚠️ 備份推送失敗，請檢查 BACKUP_GIT_ENABLED / BACKUP_GIT_REPO 與伺服器網路。"
        ),
        ephemeral=True,
    )

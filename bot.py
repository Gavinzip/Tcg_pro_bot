#!/usr/bin/env python3
"""Bot entrypoint: command/event wiring only.

Core business logic lives in runtime/core.py.
"""

from runtime import core as _core
from commands import (
    handle_ask,
    handle_cardset,
    handle_flex_pack,
    handle_market,
    handle_market_bootstrap,
    handle_market_push_backup,
    handle_profile,
    handle_settings,
)

# Re-export core runtime symbols (including leading-underscore helpers)
# so existing scripts importing `bot` continue to work.
for _name, _value in vars(_core).items():
    if _name.startswith("__"):
        continue
    globals()[_name] = _value


@tree.command(name="manual_analyze", description="手動選擇版本生成報告與海報")
async def manual_analyze(interaction: discord.Interaction, image: discord.Attachment, lang: str = "zh"):
    if not any(image.filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.webp']):
        await interaction.response.send_message("❌ 請上傳有效的圖片", ephemeral=True)
        return
        
    await interaction.response.send_message(f"🔍 收到圖片，正在建立分析討論串...", ephemeral=False)
    resp = await interaction.original_response()
    thread_name = "卡片分析 (手動模式)" if lang == "zh" else "Card Analysis (Manual)"
    thread = await resp.create_thread(name=thread_name, auto_archive_duration=60)
    await thread.add_user(interaction.user)
    
    temp_dir = tempfile.gettempdir()
    img_path = os.path.join(temp_dir, f"manual_{image.id}_{image.filename}")
    await image.save(img_path)
    
    try:
        await thread.send(f"⏳ 正在辨識卡片影像中 (語言: {lang})...")
        api_key = os.getenv("MINIMAX_API_KEY")
        res = await market_report_vision.process_image_for_candidates(img_path, api_key, lang=lang)
        if not res or len(res) < 2:
            await thread.send(f"❌ 分析失敗: {res[1] if res else '未知錯誤'}")
            return
            
        card_info, candidates = res
        pc_candidates = candidates.get("pc", [])
        snkr_candidates = candidates.get("snkr", [])
        
        if not pc_candidates and not snkr_candidates:
            await thread.send("❌ 找不到任何符合的卡片候補。")
            return
            
        # --- 抓取預覽圖 ---
        await thread.send("🖼️ 正在抓取候選版本預覽圖，請稍候...")
        loop = asyncio.get_running_loop()
        embeds = []
        
        # 1. PC 預覽 (Top 5)
        for i, url in enumerate(pc_candidates[:5], start=1):
            try:
                _re, _url, thumb_url = await loop.run_in_executor(None, lambda: market_report_vision._fetch_pc_prices_from_url(url, skip_hi_res=True))
                slug = url.split('/')[-1]
                embed = discord.Embed(title=f"PriceCharting 候選 #{i}", description=f"Slug: `{slug}`", url=url, color=0x3498db)
                if thumb_url: embed.set_thumbnail(url=thumb_url)
                embeds.append(embed)
            except: pass
            
        # 2. SNKRDUNK 預覽 (Top 5)
        for i, cand in enumerate(snkr_candidates[:5], start=1):
            try:
                url = cand.split(" — ")[0]
                title = cand.split(" — ")[1] if " — " in cand else "SNKRDUNK Item"
                _re, thumb_url = await loop.run_in_executor(None, market_report_vision._fetch_snkr_prices_from_url_direct, url)
                embed = discord.Embed(title=f"SNKRDUNK 候選 #{i}", description=title, url=url, color=0xe67e22)
                if thumb_url: embed.set_thumbnail(url=thumb_url)
                embeds.append(embed)
            except: pass

        # 分批發送 Embeds
        for i in range(0, len(embeds), 5):
            await thread.send(embeds=embeds[i:i+5])
            
        view = ManualCandidateView(card_info, pc_candidates, snkr_candidates, lang=lang)
        info_text = f"**手動模式：候選卡片選擇**\n"
        info_text += f"> **名稱:** {card_info.get('name', '')} ({card_info.get('jp_name', '')})\n"
        info_text += f"> **編號:** {card_info.get('number', '')}\n"
        info_text += "請根據上方圖片，選擇正確版本後按下確認："
        
        view_msg = await thread.send(content=info_text, view=view)
        view.original_message = view_msg
        
    except Exception as e:
        error_trace = traceback.format_exc()
        await thread.send(f"❌ 發生異常：\n```python\n{error_trace[-1900:]}\n```")
    finally:
        if os.path.exists(img_path):
            os.remove(img_path)


_COMMAND_HANDLER_DEPS = {
    # ask/settings
    "ASK_KIND_LABELS": _ASK_KIND_LABELS,
    "ASK_FEEDBACK_SAVE_ENABLED": ASK_FEEDBACK_SAVE_ENABLED,
    "ASK_FEEDBACK_CHANNEL_ID": ASK_FEEDBACK_CHANNEL_ID,
    "append_ask_feedback": _append_ask_feedback,
    "forward_ask_feedback": _forward_ask_feedback,
    "get_user_default_wallet": _get_user_default_wallet,
    "normalize_wallet_address": _normalize_wallet_address,
    "save_user_settings": _save_user_settings,
    # market
    "LanguageSelectView": LanguageSelectView,
    "translate": _t,
    "LANG_FLAGS": LANG_FLAGS,
    "LANG_LABELS": LANG_LABELS,
    "MARKET_RECENT_LIMIT": MARKET_RECENT_LIMIT,
    "MARKET_SOURCE_CHANNEL_ID": MARKET_SOURCE_CHANNEL_ID,
    "MarketBrowserView": MarketBrowserView,
    "market_load_cached_index": _market_load_cached_index,
    "market_collect_listings": _market_collect_listings,
    "market_live_sync_refresh": _market_live_sync_refresh,
    "market_index_path": _market_index_path,
    "market_write_index_file": _market_write_index_file,
    "market_cache_images_dir": _market_cache_images_dir,
    "market_save_bootstrap_state": _market_save_bootstrap_state,
    "run_ranking_sync_script": _run_ranking_sync_script,
    # cardset
    "normalize_series_code_input": _normalize_series_code_input,
    "SERIES_CODE_RE": _SERIES_CODE_RE,
    "profile_wizard_texts": _profile_wizard_texts,
    "infer_cardset_category_with_minimax": _infer_cardset_category_with_minimax,
    "sanitize_cardset_category": _sanitize_cardset_category,
    "REPORT_SEMAPHORE": REPORT_SEMAPHORE,
    "process_single_image_compat": _process_single_image_compat,
    "smart_split": smart_split,
    "POSTER_SEMAPHORE": POSTER_SEMAPHORE,
    "market_report_vision": market_report_vision,
    # profile/flexpack
    "build_wallet_flex_pack_picker_data": _build_wallet_flex_pack_picker_data,
    "FlexPackConfigView": FlexPackConfigView,
    "build_wallet_profile_picker_data": _build_wallet_profile_picker_data,
    "ProfileConfigView": ProfileConfigView,
}


@tree.command(name="flex_pack", description="指定卡包生成 Flex 海報（剛抽排版 / 天堂地獄）")
@app_commands.describe(address="錢包地址（可留空使用已儲存的預設地址）")
async def flex_pack(interaction: discord.Interaction, address: str = None):
    await handle_flex_pack(
        interaction,
        address,
        beta_mode=False,
        deps=_COMMAND_HANDLER_DEPS,
    )


@tree.command(name="flex_pack_beta", description="Flex 海報 Beta（天堂地獄改用純背景，不疊雙色遮罩）")
@app_commands.describe(address="錢包地址（可留空使用已儲存的預設地址）")
async def flex_pack_beta(interaction: discord.Interaction, address: str = None):
    await handle_flex_pack(
        interaction,
        address,
        beta_mode=True,
        deps=_COMMAND_HANDLER_DEPS,
    )


@tree.command(name="cardset", description="輸入系列代號（如 op15），直接生成卡盒 Top10 報告與海報")
@app_commands.describe(series_code="系列代號，例如 op15 / m4 / loch")
async def cardset(interaction: discord.Interaction, series_code: str):
    await handle_cardset(interaction, series_code, deps=_COMMAND_HANDLER_DEPS)


@tree.command(name="market", description="瀏覽市場討論串（WTS / WTB）")
async def market(interaction: discord.Interaction):
    await handle_market(interaction, deps=_COMMAND_HANDLER_DEPS)


@tree.command(name="market_bootstrap", description="一次性全頻道掃描 market thread（含封存）")
@app_commands.describe(
    force="保留參數（目前每次都會完整重掃，可忽略）",
    push_backup="完成後是否立即觸發一次備份推送到資料 Git（預設 true）",
)
async def market_bootstrap(
    interaction: discord.Interaction,
    force: bool = False,
    push_backup: bool = True,
):
    await handle_market_bootstrap(
        interaction,
        force,
        push_backup,
        deps=_COMMAND_HANDLER_DEPS,
    )


@tree.command(name="market_push_backup", description="僅推送 market 快取到資料 Git（不重掃 market）")
async def market_push_backup(interaction: discord.Interaction):
    await handle_market_push_backup(interaction, deps=_COMMAND_HANDLER_DEPS)


@tree.command(name="ask", description="提交意見回饋（Bug / 新功能）")
@app_commands.describe(
    kind="回饋類型",
    content="請輸入 bug 描述或功能需求（最多 1200 字）",
    screenshot="可選：附上圖片截圖",
)
@app_commands.choices(
    kind=[
        app_commands.Choice(name="🐞 Bug 回報", value="bug"),
        app_commands.Choice(name="✨ 新功能建議", value="feature"),
        app_commands.Choice(name="💬 其他意見", value="other"),
    ]
)
async def ask(
    interaction: discord.Interaction,
    kind: app_commands.Choice[str],
    content: str,
    screenshot: discord.Attachment | None = None,
):
    await handle_ask(
        interaction,
        kind,
        content,
        screenshot,
        deps=_COMMAND_HANDLER_DEPS,
    )


@tree.command(name="settings", description="設定你的預設錢包地址")
@app_commands.describe(wallet="錢包地址（0x 開頭），留空則顯示目前設定")
async def settings(interaction: discord.Interaction, wallet: str = None):
    await handle_settings(interaction, wallet, deps=_COMMAND_HANDLER_DEPS)


@tree.command(name="profile", description="輸入錢包地址，開啟互動式海報設定面板")
@app_commands.describe(address="錢包地址（可留空使用已儲存的預設地址）")
async def profile(interaction: discord.Interaction, address: str = None):
    await handle_profile(interaction, address, deps=_COMMAND_HANDLER_DEPS)


async def _global_interaction_usage_check(interaction: discord.Interaction) -> bool:
    # Keep as a permissive global check.
    # Usage counting is handled in on_interaction for reliability.
    return True


# discord.py 2.7+: interaction_check is an overridable coroutine method (not a decorator).
# Bind our permissive check explicitly to avoid un-awaited coroutine warnings on import.
tree.interaction_check = _global_interaction_usage_check  # type: ignore[assignment]


@tree.command(name="sync_status", description="查看 NFT holders 同步狀態")
async def sync_status(interaction: discord.Interaction):
    status_path = _nft_sync_status_path()
    latest_path = os.path.join(
        _nft_sync_data_dir(),
        "snapshots",
        f"nft_{str(os.getenv('NFT_TOKEN_ID', '13')).strip() or '13'}_holders.latest.json",
    )
    if not os.path.exists(status_path):
        await interaction.response.send_message(
            f"⚠️ 尚無同步狀態檔：`{status_path}`\n請先等待啟動 bootstrap 或每日同步執行。",
            ephemeral=True,
        )
        return

    try:
        with open(status_path, "r", encoding="utf-8") as f:
            status = json.load(f)
        if not isinstance(status, dict):
            raise RuntimeError("status json invalid")
    except Exception as e:
        await interaction.response.send_message(
            f"❌ 讀取同步狀態失敗：{e}",
            ephemeral=True,
        )
        return

    success = bool(status.get("success"))
    trigger = str(status.get("trigger") or "unknown")
    updated_at = str(status.get("updated_at") or "unknown")
    message = str(status.get("message") or "").strip()
    extra = status.get("extra") if isinstance(status.get("extra"), dict) else {}
    new_rows = extra.get("new_rows", "n/a")
    pages = extra.get("pages_fetched", "n/a")
    holder_count = extra.get("holder_count", "n/a")
    stop_on_dup = extra.get("stop_on_duplicate", "n/a")
    commit = extra.get("commit", "n/a")

    txt = (
        f"{'✅' if success else '❌'} 同步狀態：**{'成功' if success else '失敗'}**\n"
        f"Trigger: `{trigger}`\n"
        f"Updated At: `{updated_at}`\n"
        f"New Rows: `{new_rows}` | Pages: `{pages}` | Holders: `{holder_count}`\n"
        f"Stop on First Duplicate: `{stop_on_dup}`\n"
        f"Commit: `{commit}`\n"
        f"Status File: `{status_path}`\n"
        f"Latest Snapshot: `{latest_path}`\n"
    )
    if message:
        txt += f"\nMessage: `{message[:1200]}`"
    await interaction.response.send_message(txt, ephemeral=True)


@tree.command(name="ranking_sync_status", description="查看 rankings 同步狀態")
async def ranking_sync_status(interaction: discord.Interaction):
    status_path = _rank_sync_status_path()
    latest_path = os.path.join(_rank_sync_data_dir(), "latest.json")
    if not os.path.exists(status_path):
        await interaction.response.send_message(
            f"⚠️ 尚無 rankings 同步狀態檔：`{status_path}`\n請先等待啟動 bootstrap / compare 或整點同步執行。",
            ephemeral=True,
        )
        return

    try:
        with open(status_path, "r", encoding="utf-8") as f:
            status = json.load(f)
        if not isinstance(status, dict):
            raise RuntimeError("status json invalid")
    except Exception as e:
        await interaction.response.send_message(
            f"❌ 讀取 rankings 同步狀態失敗：{e}",
            ephemeral=True,
        )
        return

    success = bool(status.get("success"))
    trigger = str(status.get("trigger") or "unknown")
    updated_at = str(status.get("updated_at") or "unknown")
    message = str(status.get("message") or "").strip()
    extra = status.get("extra") if isinstance(status.get("extra"), dict) else {}
    wallet_count = extra.get("wallet_count", "n/a")
    pages = extra.get("collectible_pages", "n/a")
    changed_wallets = extra.get("changed_wallets", "n/a")
    refreshed_wallets = extra.get("refreshed_wallets", "n/a")
    full_rebuild = extra.get("full_rebuild", "n/a")
    backup_status = extra.get("backup_status", "n/a")
    commit = extra.get("commit", "n/a")
    duration_sec = extra.get("duration_sec", "n/a")

    txt = (
        f"{'✅' if success else '❌'} Rankings 同步狀態：**{'成功' if success else '失敗'}**\n"
        f"Trigger: `{trigger}`\n"
        f"Updated At: `{updated_at}`\n"
        f"Wallets: `{wallet_count}` | Pages: `{pages}`\n"
        f"Changed: `{changed_wallets}` | Refreshed: `{refreshed_wallets}` | Full Rebuild: `{full_rebuild}`\n"
        f"Backup: `{backup_status}` | Commit: `{commit}` | Duration: `{duration_sec}`\n"
        f"Status File: `{status_path}`\n"
        f"Latest Snapshot: `{latest_path}`\n"
    )
    if message:
        txt += f"\nMessage: `{message[:1200]}`"
    await interaction.response.send_message(txt, ephemeral=True)


@tree.command(name="ranking", description="查看各項排名 Top 10（文字版）")
async def ranking(interaction: discord.Interaction):
    latest_path = os.path.join(_rank_sync_data_dir(), "latest.json")
    if not os.path.exists(latest_path):
        await interaction.response.send_message(
            f"⚠️ 尚無排名資料：`{latest_path}`\n請先執行一次 ranking 同步。",
            ephemeral=True,
        )
        return

    try:
        with open(latest_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            raise RuntimeError("latest.json 格式錯誤")
    except Exception as e:
        await interaction.response.send_message(f"❌ 讀取排名資料失敗：{e}", ephemeral=True)
        return

    wallets_raw = payload.get("wallets")
    wallets = wallets_raw if isinstance(wallets_raw, list) else []
    top_map = payload.get("top") if isinstance(payload.get("top"), dict) else {}

    def _label(row: dict) -> str:
        addr = _short_hex(str(row.get("address") or ""))
        username = str(row.get("username") or "").strip()
        if not username:
            return addr or "unknown"
        username = username[:28]
        return f"{username} · {addr}" if addr else username

    def _money_text(row: dict, field: str, signed: bool = False) -> str:
        return _format_usdt_currency(_to_decimal(row.get(field)), signed=signed)

    def _int_text(row: dict, field: str) -> str:
        return _format_number(_parse_int(row.get(field)) or 0)

    def _rank_text(row: dict, field: str, fallback_idx: int | None = None) -> str:
        if not field:
            return str(fallback_idx) if fallback_idx is not None else "-"
        val = _parse_int(row.get(field))
        return str(val) if (val is not None and val > 0) else "-"

    def _fallback_top(
        metric_field: str,
        *,
        numeric_type: str,
        descending: bool = True,
        require_negative: bool = False,
    ) -> list[dict]:
        rows: list[dict] = []
        for w in wallets:
            if isinstance(w, dict):
                if require_negative:
                    if numeric_type == "int":
                        if (_parse_int(w.get(metric_field)) or 0) >= 0:
                            continue
                    else:
                        if _to_decimal(w.get(metric_field)) >= 0:
                            continue
                rows.append(w)
        if numeric_type == "int":
            rows.sort(
                key=lambda x: (_parse_int(x.get(metric_field)) or 0, str(x.get("address") or "").lower()),
                reverse=descending,
            )
        else:
            rows.sort(
                key=lambda x: (_to_decimal(x.get(metric_field)), str(x.get("address") or "").lower()),
                reverse=descending,
            )
        return rows[:10]

    sections = [
        ("volume", "交易量", "trade_volume_usdt", "volume_rank", "money", True, False),
        ("pack_spent", "抽卡花費", "pack_spent_usdt", "", "money", True, False),
        ("total_spent", "總花費", "total_spent_usdt", "total_spent_rank", "money", True, False),
        ("holdings", "持有價值", "holdings_value_usdt", "holdings_rank", "money", True, False),
        ("pnl", "總盈虧", "total_pnl_usdt", "pnl_rank", "money_signed", True, False),
        ("loss", "總虧損", "total_pnl_usdt", "", "money_signed", False, True),
        ("participation_days", "參與天數", "participation_days_count", "participation_days_rank", "int", True, False),
        ("sbt", "SBT", "sbt_owned_total", "sbt_rank", "int", True, False),
    ]

    blocks: list[str] = []
    for top_key, title, value_field, rank_field, value_type, descending, require_negative in sections:
        top_rows_raw = top_map.get(top_key) if isinstance(top_map, dict) else None
        if isinstance(top_rows_raw, list) and top_rows_raw:
            rows = [x for x in top_rows_raw if isinstance(x, dict)][:10]
        else:
            rows = _fallback_top(
                value_field,
                numeric_type=("int" if value_type == "int" else "decimal"),
                descending=descending,
                require_negative=require_negative,
            )

        section_lines: list[str] = [f"**{title} Top 10**"]
        if not rows:
            section_lines.append("無資料")
            blocks.append("\n".join(section_lines))
            continue

        for idx, row in enumerate(rows, start=1):
            if value_type == "money":
                value_txt = _money_text(row, value_field, signed=False)
            elif value_type == "money_signed":
                value_txt = _money_text(row, value_field, signed=True)
            else:
                value_txt = _int_text(row, value_field)
            section_lines.append(f"{idx}. `{_rank_text(row, rank_field, idx)}` {_label(row)} | {value_txt}")
        blocks.append("\n".join(section_lines))

    updated_at = "-"
    meta = payload.get("meta")
    if isinstance(meta, dict):
        updated_at = str(meta.get("updated_at") or "-")
    blocks.append(f"Updated: `{updated_at}`")

    if not blocks:
        blocks = ["無資料"]

    # Keep each outgoing message safely below Discord message limit.
    normalized_blocks: list[str] = []
    for block in blocks:
        text = str(block or "").strip()
        if not text:
            continue
        if len(text) <= 1900:
            normalized_blocks.append(text)
            continue
        start = 0
        while start < len(text):
            normalized_blocks.append(text[start : start + 1800])
            start += 1800
    if not normalized_blocks:
        normalized_blocks = ["無資料"]

    if isinstance(interaction.channel, discord.Thread):
        await interaction.response.send_message(normalized_blocks[0], ephemeral=False)
        for block in normalized_blocks[1:]:
            await interaction.followup.send(block, ephemeral=False)
        return

    try:
        await interaction.response.send_message("📊 正在建立 ranking 討論串...", ephemeral=False)
        starter = await interaction.original_response()
    except discord.NotFound:
        channel = interaction.channel
        if channel is None:
            print("❌ /ranking 互動已失效且找不到可用頻道。", file=sys.stderr)
            return
        starter = await channel.send("📊 正在建立 ranking 討論串...")

    thread_name = f"ranking-{datetime.now().strftime('%m%d-%H%M')}"
    try:
        thread = await starter.create_thread(name=thread_name, auto_archive_duration=60)
    except Exception as e:
        print(f"❌ /ranking 建立討論串失敗: {e}", file=sys.stderr)
        await starter.reply(f"❌ 建立討論串失敗：{e}")
        return

    try:
        await thread.add_user(interaction.user)
    except Exception:
        pass
    for block in normalized_blocks:
        await thread.send(block)


@tree.command(name="bot_usage", description="查看機器人使用次數（總次數 + 各指令）")
async def bot_usage(interaction: discord.Interaction):
    path = _bot_usage_stats_path()
    data = _load_bot_usage_stats()
    if not data:
        app_env = str(os.getenv("APP_ENV", "local")).strip() or "local"
        sync_data_dir = str(os.getenv("SYNC_DATA_DIR", "")).strip() or "(unset)"
        rank_data_dir = str(os.getenv("RANK_SYNC_DATA_DIR", os.getenv("RANKING_DATA_DIR", ""))).strip() or "(unset)"
        fallback_path = os.path.join(_rank_sync_data_dir(), "state", "bot_usage_stats.json")
        await interaction.response.send_message(
            "⚠️ 尚無使用統計資料。\n"
            f"usage path: `{path}`\n"
            f"fallback path: `{fallback_path}`\n"
            f"APP_ENV: `{app_env}`\n"
            f"SYNC_DATA_DIR: `{sync_data_dir}`\n"
            f"RANK_SYNC_DATA_DIR: `{rank_data_dir}`\n"
            "先執行幾次 `/profile`、`/ranking`、`/bot_usage` 後再查看。",
            ephemeral=True,
        )
        return

    total = _parse_int(data.get("total")) or 0
    updated_at = str(data.get("updated_at") or "-")
    commands_raw = data.get("commands")
    commands = commands_raw if isinstance(commands_raw, dict) else {}
    items: list[tuple[str, int]] = []
    for name, count_raw in commands.items():
        name_s = str(name or "").strip()
        if not name_s:
            continue
        count = _parse_int(count_raw) or 0
        items.append((name_s, count))
    items.sort(key=lambda x: (-x[1], x[0].lower()))

    lines = [
        f"**總使用次數**：`{_format_number(total)}`",
        "",
        "**各指令使用次數**",
    ]
    if not items:
        lines.append("無資料")
    else:
        max_len = 1850
        rendered = 0
        for idx, (name, count) in enumerate(items, start=1):
            line = f"{idx}. `{name}`: `{_format_number(count)}`"
            draft = "\n".join(lines + [line])
            if len(draft) > max_len:
                remain = len(items) - rendered
                lines.append(f"... 其餘 `{_format_number(remain)}` 個指令略過")
                break
            lines.append(line)
            rendered += 1
    lines.append("")
    lines.append(f"Updated: `{updated_at}`")
    lines.append(f"Path: `{path}`")
    await interaction.response.send_message("\n".join(lines), ephemeral=True)


@tree.command(name="clear_stale_commands", description="[Owner] 清除所有全域斜線指令並重置 (建議改用 !sync)")
async def clear_stale(interaction: discord.Interaction):
    await interaction.response.send_message("⚙️ 正在清除全域指令並重新同步，請稍候...", ephemeral=True)
    # Note: Global sync can be slow. 
    await tree.sync()
    await interaction.followup.send("✅ 已發送同步請求。若指令未更新，請嘗試使用文字指令 `!sync` (需標註機器人)。", ephemeral=True)


@client.event
async def on_interaction(interaction: discord.Interaction):
    try:
        if getattr(interaction, "type", None) == discord.InteractionType.application_command:
            raw = interaction.data if isinstance(interaction.data, dict) else {}
            cmd_name = str(raw.get("name") or "").strip()
            if not cmd_name:
                cmd = getattr(interaction, "command", None)
                cmd_name = str(
                    getattr(cmd, "qualified_name", None) or getattr(cmd, "name", None) or "unknown"
                ).strip()
            _record_bot_usage(
                str(getattr(interaction.user, "id", "unknown")),
                f"/{cmd_name}",
                guild_id=getattr(interaction, "guild_id", None),
                channel_id=getattr(interaction, "channel_id", None),
            )
    except Exception as e:
        print(f"⚠️ usage stats record failed in on_interaction: {e}", file=sys.stderr)


@client.event
async def on_thread_create(thread: discord.Thread):
    try:
        parent_id = int(getattr(thread, "parent_id", 0) or 0)
        if parent_id != MARKET_SOURCE_CHANNEL_ID:
            return
        if bool(getattr(thread, "archived", False)):
            return
        asyncio.create_task(_market_live_sync_refresh("thread_create"))
    except Exception as e:
        print(f"⚠️ market thread_create hook failed: {e}", file=sys.stderr)


@client.event
async def on_thread_update(before: discord.Thread, after: discord.Thread):
    try:
        parent_id = int(getattr(after, "parent_id", 0) or 0)
        if parent_id != MARKET_SOURCE_CHANNEL_ID:
            return
        if bool(getattr(before, "archived", False)) != bool(getattr(after, "archived", False)):
            asyncio.create_task(_market_live_sync_refresh("thread_archive_state_changed"))
    except Exception as e:
        print(f"⚠️ market thread_update hook failed: {e}", file=sys.stderr)


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    try:
        ch = message.channel
        if isinstance(ch, discord.Thread):
            parent_id = int(getattr(ch, "parent_id", 0) or 0)
            if parent_id == MARKET_SOURCE_CHANNEL_ID:
                author_id = int(getattr(message.author, "id", 0) or 0)
                if MARKET_SUMMARY_BOT_ID <= 0 or author_id == MARKET_SUMMARY_BOT_ID:
                    side, _title = _market_parse_summary(str(getattr(message, "content", "") or ""))
                    if side in ("WTS", "WTB"):
                        asyncio.create_task(_market_live_sync_refresh("summary_message"))
    except Exception as e:
        print(f"⚠️ market summary hook failed: {e}", file=sys.stderr)

    bot_mentioned = client.user in message.mentions
    content_lower = message.content.lower().strip()

    if bot_mentioned and "!sync" in content_lower:
        _record_bot_usage(
            str(getattr(message.author, "id", "unknown")),
            "!sync",
            guild_id=getattr(message.guild, "id", None),
            channel_id=getattr(message.channel, "id", None),
        )
        try:
            # 1. 先同步到當前伺服器 (Guild Sync - 幾乎即時生效)
            print(f"⚙️ 正在同步指令到伺服器: {message.guild.id}")
            tree.copy_global_to(guild=message.guild)
            await tree.sync(guild=message.guild)

            # 2. 同步到全域 (Global Sync - 需 1 小時)
            await tree.sync()

            await message.reply("✅ **指令同步成功！**\n1. 伺服器專屬指令已更新 (應可立即使用)。\n2. 全域更新已送出 (可能需 1 小時)。\n\n*注意：若仍未看到指令，請重啟 Discord APP。*")
            return
        except Exception as e:
            await message.reply(f"❌ 同步失敗: {e}")
            return

    auto_monitor_handled = False
    if (
        AUTO_IMAGE_THREAD_MONITOR_ENABLED
        and AUTO_IMAGE_THREAD_CHANNEL_IDS
        and not isinstance(message.channel, discord.Thread)
        and message.channel.id in AUTO_IMAGE_THREAD_CHANNEL_IDS
        and message.attachments
    ):
        valid_attachments = [a for a in message.attachments if _is_image_attachment(a)]
        if valid_attachments:
            for _ in valid_attachments:
                _record_bot_usage(
                    str(getattr(message.author, "id", "unknown")),
                    "image:auto_monitor",
                    guild_id=getattr(message.guild, "id", None),
                    channel_id=getattr(message.channel, "id", None),
                )
            auto_monitor_handled = True
            asyncio.create_task(handle_images_from_monitored_channel(message, valid_attachments))

    if bot_mentioned:
        # 原本的圖片處理邏輯
        if message.attachments and not auto_monitor_handled:
            content_lower = message.content.lower()
            valid_attachments = [
                a for a in message.attachments
                if _is_image_attachment(a)
            ]
            if not valid_attachments:
                return

            for _ in valid_attachments:
                _record_bot_usage(
                    str(getattr(message.author, "id", "unknown")),
                    "image:mention",
                    guild_id=getattr(message.guild, "id", None),
                    channel_id=getattr(message.channel, "id", None),
                )

            # 可用 !zh / !zhs(!cn) / !en / !ko 強制指定；未指定時用按鈕詢問，5 秒逾時預設繁中。
            lang_override = _parse_lang_override(content_lower)
            if lang_override:
                lang = lang_override
            else:
                lang = await choose_language_for_message(message)

            for attachment in valid_attachments:
                # 每張圖建立獨立 Task；並發上限由 REPORT/POSTER 雙 Semaphore 控制
                asyncio.create_task(handle_image(attachment, message, lang=lang))


if __name__ == "__main__":
    if not TOKEN:
        print("❌ 錯誤：找不到 DISCORD_BOT_TOKEN。")
    else:
        threading.Thread(target=run_health_server, daemon=True).start()
        print("啟動中...")
        client.run(TOKEN)

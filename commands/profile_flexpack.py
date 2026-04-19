from __future__ import annotations

import asyncio
import sys
import traceback

import discord


async def handle_flex_pack(
    interaction: discord.Interaction,
    address: str | None,
    *,
    beta_mode: bool,
    deps: dict,
) -> None:
    get_user_default_wallet = deps["get_user_default_wallet"]
    normalize_wallet_address = deps["normalize_wallet_address"]
    profile_wizard_texts = deps["profile_wizard_texts"]
    language_select_view_cls = deps["LanguageSelectView"]
    build_wallet_flex_pack_picker_data = deps["build_wallet_flex_pack_picker_data"]
    flex_pack_config_view_cls = deps["FlexPackConfigView"]

    cmd_name = "/flex_pack_beta" if beta_mode else "/flex_pack"
    if not address:
        address = get_user_default_wallet(str(interaction.user.id))
        if not address:
            await interaction.response.send_message(
                "❌ 請輸入錢包地址，或先使用 `/settings wallet:0x...` 設定預設地址。",
                ephemeral=True,
            )
            return

    wallet = normalize_wallet_address(address)
    if not wallet:
        await interaction.response.send_message(
            "❌ 錢包地址格式錯誤，請輸入 `0x` 開頭且長度 42 的地址。",
            ephemeral=True,
        )
        return

    try:
        opening_text = (
            "🎛️ 正在建立卡包 Flex 海報設定（Beta）討論串..."
            if beta_mode
            else "🎛️ 正在建立卡包 Flex 海報設定討論串..."
        )
        await interaction.response.send_message(opening_text, ephemeral=False)
        resp = await interaction.original_response()
    except discord.NotFound:
        channel = interaction.channel
        if channel is None:
            print(f"❌ {cmd_name} 互動已失效且找不到可用頻道。", file=sys.stderr)
            return
        opening_text = (
            "🎛️ 正在建立卡包 Flex 海報設定（Beta）討論串..."
            if beta_mode
            else "🎛️ 正在建立卡包 Flex 海報設定討論串..."
        )
        resp = await channel.send(opening_text)

    thread_name = "卡包 Flex 海報設定 Beta" if beta_mode else "卡包 Flex 海報設定"
    thread = await resp.create_thread(name=thread_name, auto_archive_duration=60)
    await thread.add_user(interaction.user)

    default_lang = "zh"
    lang_view = language_select_view_cls(interaction.user.id, timeout_seconds=20)
    lang_msg = await thread.send(profile_wizard_texts(default_lang)["lang_prompt"], view=lang_view)
    picked_lang, selected = await lang_view.wait_for_choice()
    profile_lang = picked_lang if selected and picked_lang else "zh"
    if not selected:
        lang_view._disable_all()
        try:
            await lang_msg.edit(content=profile_wizard_texts(profile_lang)["lang_timeout"], view=lang_view)
        except Exception:
            pass

    loop = asyncio.get_running_loop()
    try:
        picker_data = await loop.run_in_executor(
            None,
            build_wallet_flex_pack_picker_data,
            wallet,
            profile_lang,
        )
        if not (picker_data.get("pack_options") or []):
            await thread.send("⚠️ 這個地址目前找不到可用的卡包抽卡紀錄。")
            return
        view = flex_pack_config_view_cls(interaction.user.id, wallet, picker_data, profile_lang, beta_mode=beta_mode)
        msg = await thread.send(view.render_message(), view=view)
        view.bind_message(msg)
    except Exception as e:
        print(f"❌ {cmd_name} 失敗: {e}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        await thread.send(f"❌ 生成失敗：{e}")


async def handle_profile(
    interaction: discord.Interaction,
    address: str | None,
    *,
    deps: dict,
) -> None:
    get_user_default_wallet = deps["get_user_default_wallet"]
    normalize_wallet_address = deps["normalize_wallet_address"]
    profile_wizard_texts = deps["profile_wizard_texts"]
    language_select_view_cls = deps["LanguageSelectView"]
    build_wallet_profile_picker_data = deps["build_wallet_profile_picker_data"]
    profile_config_view_cls = deps["ProfileConfigView"]

    if not address:
        address = get_user_default_wallet(str(interaction.user.id))
        if not address:
            await interaction.response.send_message(
                "❌ 請輸入錢包地址，或先使用 `/settings wallet:0x...` 設定預設地址。",
                ephemeral=True,
            )
            return

    wallet = normalize_wallet_address(address)
    if not wallet:
        await interaction.response.send_message(
            "❌ 錢包地址格式錯誤，請輸入 `0x` 開頭且長度 42 的地址。",
            ephemeral=True,
        )
        return

    try:
        await interaction.response.send_message("🎛️ 正在建立收藏海報設定討論串...", ephemeral=False)
        resp = await interaction.original_response()
    except discord.NotFound:
        channel = interaction.channel
        if channel is None:
            print("❌ /profile 互動已失效且找不到可用頻道。", file=sys.stderr)
            return
        resp = await channel.send("🎛️ 正在建立收藏海報設定討論串...")
    thread = await resp.create_thread(name="收藏海報設定", auto_archive_duration=60)
    await thread.add_user(interaction.user)

    default_lang = "zh"
    default_texts = profile_wizard_texts(default_lang)

    lang_view = language_select_view_cls(interaction.user.id, timeout_seconds=20)
    lang_msg = await thread.send(default_texts["lang_prompt"], view=lang_view)
    picked_lang, selected = await lang_view.wait_for_choice()
    profile_lang = picked_lang if selected and picked_lang else "zh"
    if not selected:
        lang_view._disable_all()
        try:
            await lang_msg.edit(content=profile_wizard_texts(profile_lang)["lang_timeout"], view=lang_view)
        except Exception:
            pass

    loop = asyncio.get_running_loop()
    try:
        picker_data = await loop.run_in_executor(
            None,
            build_wallet_profile_picker_data,
            wallet,
        )
        view = profile_config_view_cls(interaction.user.id, wallet, picker_data, profile_lang)
        msg = await thread.send(view.render_message(), view=view)
        view.bind_message(msg)
    except Exception as e:
        print(f"❌ /profile 失敗: {e}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        await thread.send(f"❌ 生成失敗：{e}")

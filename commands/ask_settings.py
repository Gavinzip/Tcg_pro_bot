from __future__ import annotations

from datetime import datetime, timezone

import discord
from discord import app_commands


async def handle_ask(
    interaction: discord.Interaction,
    kind: app_commands.Choice[str],
    content: str,
    screenshot: discord.Attachment | None,
    *,
    deps: dict,
) -> None:
    ask_kind_labels = deps["ASK_KIND_LABELS"]
    ask_feedback_save_enabled = deps["ASK_FEEDBACK_SAVE_ENABLED"]
    ask_feedback_channel_id = deps["ASK_FEEDBACK_CHANNEL_ID"]
    append_ask_feedback = deps["append_ask_feedback"]
    forward_ask_feedback = deps["forward_ask_feedback"]

    feedback_text = str(content or "").strip()
    if len(feedback_text) < 3:
        await interaction.response.send_message(
            "❌ 內容太短，請至少輸入 3 個字。",
            ephemeral=True,
        )
        return
    if len(feedback_text) > 1200:
        await interaction.response.send_message(
            "❌ 內容太長，請控制在 1200 字以內。",
            ephemeral=True,
        )
        return

    screenshot_url = ""
    if screenshot is not None:
        ctype = str(getattr(screenshot, "content_type", "") or "").strip().lower()
        if ctype and not ctype.startswith("image/"):
            await interaction.response.send_message(
                "❌ `screenshot` 只支援圖片檔案。",
                ephemeral=True,
            )
            return
        screenshot_url = str(getattr(screenshot, "url", "") or "").strip()

    kind_value = str(getattr(kind, "value", "other") or "other").strip().lower()
    if kind_value not in ask_kind_labels:
        kind_value = "other"
    kind_label = ask_kind_labels.get(kind_value, ask_kind_labels["other"])

    now = datetime.now(timezone.utc)
    feedback_id = f"ask_{now.strftime('%Y%m%d%H%M%S')}_{int(interaction.id)}"
    guild_id = getattr(interaction, "guild_id", None)
    channel_id = getattr(interaction, "channel_id", None)
    jump_url = ""
    if guild_id and channel_id:
        jump_url = f"https://discord.com/channels/{guild_id}/{channel_id}"

    entry: dict[str, object] = {
        "feedback_id": feedback_id,
        "created_at": now.isoformat(),
        "kind": kind_value,
        "content": feedback_text,
        "screenshot_url": screenshot_url,
        "user_id": str(getattr(interaction.user, "id", "unknown")),
        "username": str(getattr(interaction.user, "name", "") or ""),
        "display_name": str(getattr(interaction.user, "display_name", "") or ""),
        "guild_id": str(guild_id) if guild_id is not None else "",
        "channel_id": str(channel_id) if channel_id is not None else "",
        "jump_url": jump_url,
    }

    if ask_feedback_save_enabled:
        save_ok, save_info = append_ask_feedback(entry)
        if not save_ok:
            await interaction.response.send_message(
                f"❌ 儲存回饋失敗：`{save_info}`",
                ephemeral=True,
            )
            return

    forward_ok, forward_err = await forward_ask_feedback(entry)
    if (
        not forward_ok
        and int(ask_feedback_channel_id or 0) > 0
        and not ask_feedback_save_enabled
    ):
        await interaction.response.send_message(
            f"❌ 回饋送出失敗：`{str(forward_err)[:180]}`",
            ephemeral=True,
        )
        return

    lines = [
        "✅ 已收到你的回饋，感謝幫忙改善 tcg value。",
        f"ID: `{feedback_id}`",
        f"類型: `{kind_label}`",
        "儲存: `已關閉`" if not ask_feedback_save_enabled else f"儲存: `{save_info}`",
    ]
    if int(ask_feedback_channel_id or 0) > 0:
        if not forward_ok:
            if ask_feedback_save_enabled:
                lines.append(f"⚠️ 已儲存成功，但推送頻道失敗：`{str(forward_err)[:180]}`")
            else:
                lines.append(f"⚠️ 推送頻道失敗：`{str(forward_err)[:180]}`")

    await interaction.response.send_message("\n".join(lines), ephemeral=True)


async def handle_settings(
    interaction: discord.Interaction,
    wallet: str | None,
    *,
    deps: dict,
) -> None:
    get_user_default_wallet = deps["get_user_default_wallet"]
    normalize_wallet_address = deps["normalize_wallet_address"]
    save_user_settings = deps["save_user_settings"]

    user_id = str(interaction.user.id)

    if wallet is None:
        current = get_user_default_wallet(user_id)
        if current:
            await interaction.response.send_message(
                f"✅ 你的預設錢包地址：\n`{current}`\n\n要更新請重新輸入 `/settings wallet:0x...`",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "📝 你尚未設定預設錢包地址。\n\n請使用 `/settings wallet:0x...` 來設定。",
                ephemeral=True,
            )
        return

    addr = normalize_wallet_address(wallet)
    if not addr:
        await interaction.response.send_message(
            "❌ 錢包地址格式錯誤，請輸入 `0x` 開頭且長度 42 的地址。",
            ephemeral=True,
        )
        return

    if save_user_settings(user_id, addr):
        await interaction.response.send_message(
            f"✅ 已儲存你的預設錢包地址：\n`{addr}`",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            "❌ 儲存失敗，請稍後再試。",
            ephemeral=True,
        )

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import traceback

import discord


async def handle_cardset(
    interaction: discord.Interaction,
    series_code: str,
    *,
    deps: dict,
) -> None:
    normalize_series_code_input = deps["normalize_series_code_input"]
    series_code_re = deps["SERIES_CODE_RE"]
    language_select_view_cls = deps["LanguageSelectView"]
    translate = deps["translate"]
    lang_flags = deps["LANG_FLAGS"]
    lang_labels = deps["LANG_LABELS"]
    profile_wizard_texts = deps["profile_wizard_texts"]
    infer_cardset_category_with_minimax = deps["infer_cardset_category_with_minimax"]
    sanitize_cardset_category = deps["sanitize_cardset_category"]
    report_semaphore = deps["REPORT_SEMAPHORE"]
    process_single_image_compat = deps["process_single_image_compat"]
    smart_split = deps["smart_split"]
    poster_semaphore = deps["POSTER_SEMAPHORE"]
    market_report_vision = deps["market_report_vision"]

    series = normalize_series_code_input(series_code)
    if not series_code_re.fullmatch(series):
        await interaction.response.send_message(
            "❌ 系列代號格式錯誤。請輸入英數代號（例如 `op15` / `m4` / `loch`）。",
            ephemeral=True,
        )
        return

    lang_view = language_select_view_cls(interaction.user.id, timeout_seconds=20)
    await interaction.response.send_message("🌐 請先選擇語言 / Please choose language", view=lang_view, ephemeral=False)
    resp = await interaction.original_response()
    picked_lang, selected = await lang_view.wait_for_choice()
    lang = picked_lang if selected and picked_lang else "zh"
    lang_view._disable_all()
    try:
        if selected:
            await resp.edit(
                content=translate(
                    lang,
                    f"✅ 語言已選擇：{lang_flags.get(lang, '🌐')} **{lang_labels.get(lang, '繁體中文')}**\n"
                    f"📦 series_code: `{series}`，正在建立卡盒分析討論串...",
                    f"✅ Language selected: {lang_flags.get(lang, '🌐')} **{lang_labels.get(lang, 'English')}**\n"
                    f"📦 series_code: `{series}`, creating analysis thread...",
                    f"✅ 언어 선택 완료: {lang_flags.get(lang, '🌐')} **{lang_labels.get(lang, '한국어')}**\n"
                    f"📦 series_code: `{series}`, 분석 스레드를 생성합니다...",
                    f"✅ 语言已选择：{lang_flags.get(lang, '🌐')} **{lang_labels.get(lang, '简体中文')}**\n"
                    f"📦 series_code: `{series}`，正在创建分析讨论串...",
                ),
                view=lang_view,
            )
        else:
            await resp.edit(content=profile_wizard_texts(lang)["lang_timeout"], view=lang_view)
    except Exception:
        pass

    thread_name = translate(
        lang,
        f"卡盒分析 {series.upper()}",
        f"Box Analysis {series.upper()}",
        f"박스 분석 {series.upper()}",
        f"卡盒分析 {series.upper()}",
    )
    thread = await resp.create_thread(name=thread_name, auto_archive_duration=60)
    try:
        await thread.add_user(interaction.user)
    except Exception:
        pass

    card_out_dir = tempfile.mkdtemp(prefix=f"tcg_cardset_{interaction.id}_")
    try:
        infer = await infer_cardset_category_with_minimax(series, lang=lang)
        category = sanitize_cardset_category(infer.get("category"))

        external_card_info = {
            "name": f"{series.upper()} Booster Box",
            "set_code": series.upper(),
            "number": series.upper(),
            "grade": "Ungraded",
            "jp_name": "",
            "c_name": "",
            "category": category,
            "release_info": f"{series.upper()} Series Box",
            "illustrator": "Unknown",
            "market_heat": "N/A",
            "features": "Series box direct lookup",
            "collection_value": "N/A",
            "competitive_freq": "N/A",
            "is_alt_art": False,
            "language": "Unknown",
            "item_type": "series_box",
            "series_code": series,
            "poster_version": "v3",
        }

        report_text = None
        poster_data = None
        async with report_semaphore:
            market_report_vision.REPORT_ONLY = True
            result = await process_single_image_compat(
                image_path=None,
                api_key=(os.getenv("MINIMAX_API_KEY") or "").strip(),
                out_dir=card_out_dir,
                stream_mode=True,
                lang=lang,
                external_card_info=external_card_info,
            )
            for status in market_report_vision.get_and_clear_notify_msgs():
                await thread.send(status)

            if isinstance(result, tuple):
                report_text, poster_data = result
            else:
                report_text = result

        if report_text:
            for chunk in smart_split(str(report_text)):
                await thread.send(chunk)
        else:
            await thread.send(
                translate(
                    lang,
                    "❌ 生成失敗：未取得卡盒報告內容。",
                    "❌ Failed: no box report content was returned.",
                    "❌ 생성 실패: 박스 리포트를 받지 못했습니다.",
                    "❌ 生成失败：未取得卡盒报告内容。",
                )
            )
            return

        if poster_data:
            await thread.send(
                translate(
                    lang,
                    "🖼️ 正在生成卡盒 Top10 海報...",
                    "🖼️ Generating box Top10 poster...",
                    "🖼️ 박스 Top10 포스터 생성 중...",
                    "🖼️ 正在生成卡盒 Top10 海报...",
                )
            )
            async with poster_semaphore:
                out_paths = await market_report_vision.generate_posters(poster_data)
            for path in out_paths or []:
                if path and os.path.exists(path):
                    await thread.send(file=discord.File(path))
        else:
            await thread.send(
                translate(
                    lang,
                    "⚠️ 已生成文字報告，但未取得海報資料。",
                    "⚠️ Text report generated, but no poster data returned.",
                    "⚠️ 텍스트 리포트는 생성되었지만 포스터 데이터가 없습니다.",
                    "⚠️ 已生成文字报告，但未取得海报数据。",
                )
            )
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"❌ /cardset 失敗: {e}", file=sys.stderr)
        print(error_trace, file=sys.stderr)
        await thread.send(f"❌ 發生異常：\n```python\n{error_trace[-1900:]}\n```")
    finally:
        shutil.rmtree(card_out_dir, ignore_errors=True)

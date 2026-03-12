#!/usr/bin/env python3
import discord
from discord import app_commands
import os
import shutil
import tempfile
import threading
import asyncio
import traceback
import sys
import market_report_vision
from dotenv import load_dotenv
from http.server import BaseHTTPRequestHandler, HTTPServer

# ============================================================
# ⚠️ JINA AI RATE LIMITER 說明（重要！請勿刪除此說明）
# ============================================================
# market_report_vision.py 內部的 fetch_jina_markdown() 函數使用了一個
# 全域的 sliding window rate limiter：
#   - _jina_requests_queue: 記錄最近60秒內的請求時間戳
#   - _jina_lock: threading.Lock，確保多執行緒安全
#   - 限制：18 requests / 60 seconds（留兩次緩衝給 Jina 每分鐘20次的限額）
#
# 在併發情境下（多個用戶同時送圖），rate limiter 依然有效，因為：
# 1. Python module 是 singleton，所有 task 共用同一份 _jina_requests_queue
# 2. _jina_lock 是 threading.Lock，在 tasks 跑的 executor threads 中也是 thread-safe 的
# 3. 超過限額的 task 會自動 sleep 等待，不會炸掉 Jina
#
# 每次分析一張卡約會用掉 4~8 次 Jina 請求（PC: 2~3次, SNKRDUNK: 2~5次）
# ============================================================

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass  # 安靜模式

def run_health_server():
    server = HTTPServer(('0.0.0.0', 8080), HealthCheckHandler)
    server.serve_forever()

load_dotenv()
TOKEN = os.getenv('DISCORD_BOT_TOKEN')
MAX_CONCURRENT_REPORTS = max(
    1, int(os.getenv("MAX_CONCURRENT_REPORTS", os.getenv("MAX_CONCURRENT_IMAGES", "6")))
)
MAX_CONCURRENT_POSTERS = max(1, int(os.getenv("MAX_CONCURRENT_POSTERS", "3")))

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)
REPORT_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REPORTS)
POSTER_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_POSTERS)


def smart_split(text, limit=1900):
    chunks = []
    current_chunk = ""
    for line in text.split('\n'):
        if len(current_chunk) + len(line) + 1 > limit:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = line + "\n"
        else:
            current_chunk += line + "\n"
    if current_chunk.strip():
        chunks.append(current_chunk.strip())
    return chunks


# LangSelectView removed (defaulting to zh or command-based en)

class VersionSelectView(discord.ui.View):
    """
    版本選擇按鈕 View (航海王專用)。
    """
    def __init__(self, candidates):
        super().__init__(timeout=180)  # 3 分鐘超時
        self.chosen_url = None
        self._event = asyncio.Event()
        self.candidates = candidates
        
        # 動態建立按鈕
        for i, url in enumerate(candidates, start=1):
            btn = discord.ui.Button(label=f"選擇版本 {i}", style=discord.ButtonStyle.primary, custom_id=f"ver_{i}")
            btn.callback = self.make_callback(url, i)
            self.add_item(btn)

    def make_callback(self, url, idx):
        async def callback(interaction: discord.Interaction):
            self.chosen_url = url
            self._event.set()
            await interaction.response.edit_message(content=f"✅ 已選擇 **第 {idx} 個版本**，繼續生成報告...", view=None)
        return callback

    async def wait_for_choice(self) -> str | None:
        try:
            await asyncio.wait_for(self._event.wait(), timeout=180)
            return self.chosen_url
        except asyncio.TimeoutError:
            return None


async def _handle_image_impl(attachment, message, lang="zh"):
    """
    ** 並發核心函數（stream 模式）**

    流程：
    1. 建立討論串並加入使用者
    2. 下載圖片
    3. AI 分析 + 爬蟲 → 立即傳送文字報告
    4. （非同步）生成海報 → 生成完成後補傳
    """
    # 根據語言設定討論串名稱
    thread_name = "卡片分析報表" if lang == "zh" else "Card Analysis Report"
    
    # 1. 建立討論串並加入使用者
    # 先發送一個初始訊息作為討論串的起點
    init_msg = await message.reply(f"🃏 收到圖片，分析語言：**{'中文' if lang == 'zh' else 'English'}**...")
    
    thread = await init_msg.create_thread(name=thread_name, auto_archive_duration=60)
    
    # 主動把使用者加入討論串，確保他會收到通知並看到視窗
    await thread.add_user(message.author)

    # 立即傳送第一則訊息，提供即時回饋
    analyzing_msg = "🔍 Analyzing image, please wait..." if lang == "en" else "🔍 正在分析圖片中，請稍候..."
    await thread.send(analyzing_msg)

    # 3. 建立暫存資料夾（海報存這裡）
    card_out_dir = tempfile.mkdtemp(prefix=f"tcg_bot_{message.id}_")
    img_path = os.path.join(card_out_dir, attachment.filename)
    await attachment.save(img_path)

    try:
        print(f"⚙️ [並發] 開始分析: {attachment.filename} (lang={lang}, 來自 {message.author})")
        poster_data = None

        async with REPORT_SEMAPHORE:
            market_report_vision.REPORT_ONLY = True
            api_key = os.getenv("MINIMAX_API_KEY")

            result = await market_report_vision.process_single_image(
                img_path, api_key, out_dir=card_out_dir, stream_mode=True, lang=lang
            )

            # 傳送 AI 模型切換通知（如 Minimax → GPT-4o-mini 備援）
            for _status in market_report_vision.get_and_clear_notify_msgs():
                await thread.send(_status)

            # 處理「需要版本選擇」的狀態 (航海王)
            if isinstance(result, dict) and result.get("status") == "need_selection":
                candidates = result["candidates"]
                # 去重並保留順序
                candidates = list(dict.fromkeys(candidates))
                
                await thread.send(f"⚠️ 偵測到**航海王**有多個候選版本，請根據下方預覽圖選擇正確的版本：")
                
                # 抓取每個候選版本的縮圖並以 Embed 呈現
                loading_msg = await thread.send("🖼️ 正在抓取版本預覽中...")
                loop = asyncio.get_running_loop()
                
                for i, url in enumerate(candidates, start=1):
                    # 這裡改為順序執行並加上 skip_hi_res=True 以加快速度
                    print(f"DEBUG: Fetching thumbnail for candidate {i}: {url}")
                    _re, _url, thumb_url = await loop.run_in_executor(None, lambda: market_report_vision._fetch_pc_prices_from_url(url, skip_hi_res=True))
                    slug = url.split('/')[-1]
                    
                    embed = discord.Embed(title=f"版本 #{i}", description=f"Slug: `{slug}`", url=url, color=0x3498db)
                    if thumb_url:
                        embed.set_thumbnail(url=thumb_url)
                    else:
                        embed.description += "\n*(無法取得預覽圖)*"
                        print(f"DEBUG: Failed to find thumbnail for {url}")
                    await thread.send(embed=embed)

                await loading_msg.delete()

                ver_view = VersionSelectView(candidates)
                await thread.send("請點選下方按鈕進行選擇：", view=ver_view)
                selected_url = await ver_view.wait_for_choice()

                if not selected_url:
                    await thread.send("⏰ 選擇逾時，已中止。")
                    return

                # 使用選擇的 URL 重新抓取並完成報告
                final_pc_res = await loop.run_in_executor(None, market_report_vision._fetch_pc_prices_from_url, selected_url)
                pc_records, pc_url, pc_img_url = final_pc_res
                
                snkr_result = result["snkr_result"]
                snkr_records, final_img_url, snkr_url = snkr_result if snkr_result else (None, None, None)
                if not final_img_url and pc_img_url:
                    final_img_url = pc_img_url
                
                jpy_rate = market_report_vision.get_exchange_rate()
                # 呼叫 helper 完成剩餘流程
                result = await market_report_vision.finish_report_after_selection(
                    result["card_info"],
                    pc_records,
                    pc_url,
                    pc_img_url,
                    snkr_records,
                    final_img_url,
                    snkr_url,
                    jpy_rate,
                    result["out_dir"],
                    result.get("poster_version", "v3"),
                    result["lang"],
                    stream_mode=True,
                )

            if isinstance(result, tuple):
                report_text, poster_data = result
            else:
                report_text = result
                poster_data = None

            # 4. 立即傳送文字報告
            if report_text:
                if report_text.startswith("❌"):
                    await thread.send(report_text)
                else:
                    for chunk in smart_split(report_text):
                        await thread.send(chunk)
            else:
                err_msg = "❌ Analysis failed: No card info found or unknown error." if lang == "en" else "❌ 分析失敗：未發現卡片資訊或發生未知錯誤。"
                await thread.send(err_msg)
                return

        # 5. 生成海報
        if poster_data:
            wait_msg = "🖼️ Generating poster, please wait..." if lang == "en" else "🖼️ 海報生成中，請稍候..."
            await thread.send(wait_msg)
            try:
                async with POSTER_SEMAPHORE:
                    out_paths = await market_report_vision.generate_posters(poster_data)
                if out_paths:
                    for path in out_paths:
                        if os.path.exists(path):
                            await thread.send(file=discord.File(path))
                else:
                    fail_msg = "⚠️ Poster generation failed, but the text report is complete." if lang == "en" else "⚠️ 海報生成失敗，但文字報告已完成。"
                    await thread.send(fail_msg)
            except Exception as poster_err:
                err_msg = f"⚠️ Poster generation error: {poster_err}" if lang == "en" else f"⚠️ 海報生成時發生錯誤：{poster_err}"
                await thread.send(err_msg)

    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"❌ 分析失敗 ({attachment.filename}): {e}", file=sys.stderr)
        await thread.send(
            f"❌ System error:\n```python\n{error_trace[-1900:]}\n```"
        )

    finally:
        shutil.rmtree(card_out_dir, ignore_errors=True)
        print(f"✅ [並發] 完成並清理: {attachment.filename}")


async def handle_image(attachment, message, lang="zh"):
    await _handle_image_impl(attachment, message, lang=lang)


@client.event
async def on_ready():
    # Attempt to sync global commands
    try:
        await tree.sync()
        print(f'✅ 機器人已成功登入為 {client.user}')
        print(f"✅ 全域 Slash Commands 同步嘗試完成 (全球更新可能需 1 小時)")
    except Exception as e:
        print(f"⚠️ 全域同步失敗: {e}")

class PCSelect(discord.ui.Select):
    def __init__(self, candidates):
        options = []
        for i, c in enumerate(candidates[:25], start=1):
            prefix = f"#{i} — "
            slug = c.split('/')[-1][:100 - len(prefix)]
            label = prefix + slug
            options.append(discord.SelectOption(label=label, value=c[:100], description=c[:100]))
        super().__init__(placeholder="請選擇 PriceCharting 的正確版本...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_pc = self.values[0]
        for orig in self.view.pc_candidates:
            if orig.startswith(self.values[0]):
                self.view.selected_pc = orig
                break
        await interaction.response.defer()

class SnkrSelect(discord.ui.Select):
    def __init__(self, candidates):
        options = []
        for i, c in enumerate(candidates[:25], start=1):
            prefix = f"#{i} — "
            raw_text = c.split(" — ")[1] if " — " in c else c.split("/")[-1]
            label_text = raw_text[:100 - len(prefix)]
            label = prefix + label_text
            val = c.split(" — ")[0][:100]
            options.append(discord.SelectOption(label=label, value=val))
        super().__init__(placeholder="請選擇 SNKRDUNK 的正確版本...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_snkr = self.values[0]
        await interaction.response.defer()

class ManualCandidateView(discord.ui.View):
    def __init__(self, card_info, pc_candidates, snkr_candidates, lang="zh"):
        super().__init__(timeout=600)
        self.card_info = card_info
        self.pc_candidates = pc_candidates
        self.snkr_candidates = snkr_candidates
        self.lang = lang
        self.selected_pc = None
        self.selected_snkr = None
        self.original_message = None
        
        if pc_candidates:
            self.add_item(PCSelect(pc_candidates))
        if snkr_candidates:
            self.add_item(SnkrSelect(snkr_candidates))
            
    @discord.ui.button(label="確認選擇並生成報告", style=discord.ButtonStyle.primary, row=2)
    async def submit(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("🔍 正在生成報告與海報，請稍候...", ephemeral=False)
        self.stop()
        for child in self.children:
            child.disabled = True
        if self.original_message:
            await self.original_message.edit(view=self)
        asyncio.create_task(self.generate_report(interaction))
        
    async def generate_report(self, interaction: discord.Interaction):
        thread = interaction.channel # In this version, we start in the thread
        card_out_dir = tempfile.mkdtemp(prefix=f"tcg_manual_{id(self)}_")
        try:
            # 2. Generate text report and poster_data
            async with REPORT_SEMAPHORE:
                result = await market_report_vision.generate_report_from_selected(
                    self.card_info, self.selected_pc, self.selected_snkr, out_dir=card_out_dir, lang=self.lang
                )
            
            report_text, poster_data = result if isinstance(result, tuple) else (result, None)
            
            if report_text:
                for chunk in smart_split(report_text):
                    await thread.send(chunk)
            
            # 3. Generate and send posters
            if poster_data:
                wait_msg = "🖼️ Generating poster..." if self.lang == "en" else "🖼️ 海報生成中..."
                await thread.send(wait_msg)
                async with POSTER_SEMAPHORE:
                    out_paths = await market_report_vision.generate_posters(poster_data)
                if out_paths:
                    for path in out_paths:
                        if os.path.exists(path):
                            await thread.send(file=discord.File(path))
        except Exception as e:
            error_trace = traceback.format_exc()
            await thread.send(f"❌ 執行異常：\n```python\n{error_trace[-1900:]}\n```")
        finally:
            shutil.rmtree(card_out_dir, ignore_errors=True)

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

@tree.command(name="clear_stale_commands", description="[Owner] 清除所有全域斜線指令並重置 (建議改用 !sync)")
async def clear_stale(interaction: discord.Interaction):
    await interaction.response.send_message("⚙️ 正在清除全域指令並重新同步，請稍候...", ephemeral=True)
    # Note: Global sync can be slow. 
    await tree.sync()
    await interaction.followup.send("✅ 已發送同步請求。若指令未更新，請嘗試使用文字指令 `!sync` (需標註機器人)。", ephemeral=True)

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if client.user in message.mentions:
        content_lower = message.content.lower().strip()
        
        # --- 強制同步指令 (文字備援方案) ---
        if "!sync" in content_lower:
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

        # 原本的圖片處理邏輯
        if message.attachments:
            # 偵測語言指令
            content_lower = message.content.lower()
            if "!en" in content_lower or " en" in content_lower or content_lower.endswith("en"):
                lang = "en"
            else:
                lang = "zh"

            valid_attachments = [
                a for a in message.attachments
                if any(a.filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.webp'])
            ]
            if not valid_attachments:
                return

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

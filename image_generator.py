import os
import urllib.request
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
import base64
import io
from collections import deque
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from datetime import datetime
from playwright.async_api import async_playwright
import re
import asyncio

# Font loading for different environments
font_path_mac = '/System/Library/Fonts/Supplemental/Arial Unicode.ttf'
font_path_local = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fonts', 'NotoSansCJK-Bold.ttc')

if os.path.exists(font_path_local):
    fm.fontManager.addfont(font_path_local)
    font_prop = fm.FontProperties(fname=font_path_local)
    plt.rcParams['font.family'] = font_prop.get_name()
    print(f"✅ 使用本地字體: {font_path_local}")
elif os.path.exists(font_path_mac):
    fm.fontManager.addfont(font_path_mac)
    plt.rcParams['font.family'] = 'Arial Unicode MS'
    print("✅ 使用系統字體: Arial Unicode MS")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Global semaphore to prevent OOM when multiple people send images simultaneously
# 2GB RAM can safe handle 3-4 simultaneous browser tabs while the bot is running
RENDER_SEMAPHORE = asyncio.Semaphore(3)

def _resolve_template_bundle(template_version):
    version = str(template_version or "v3").strip().lower().replace(" ", "")
    if version in {"1", "v1"}:
        template_dir = os.path.join(BASE_DIR, "templates", "v1")
        profile_tpl = os.path.join(template_dir, "report_template_1.html")
        market_tpl = os.path.join(template_dir, "report_template_2.html")
        return "v1", template_dir, profile_tpl, market_tpl

    # Treat "b3" as v3 alias to tolerate typo/input variants.
    if version in {"3", "v3", "b3"}:
        template_dir = os.path.join(BASE_DIR, "templates", "v3")
        profile_tpl = os.path.join(template_dir, "ai_studio_code (1).html")
        market_tpl = os.path.join(template_dir, "ai_studio_code.html")
        return "v3", template_dir, profile_tpl, market_tpl

    # Fallback to v3 by default.
    template_dir = os.path.join(BASE_DIR, "templates", "v3")
    profile_tpl = os.path.join(template_dir, "ai_studio_code (1).html")
    market_tpl = os.path.join(template_dir, "ai_studio_code.html")
    return "v3", template_dir, profile_tpl, market_tpl

class AsyncBrowserManager:
    _instance = None
    _browser = None
    _playwright = None
    _lock = asyncio.Lock()

    @classmethod
    async def get_browser(cls):
        async with cls._lock:
            if cls._browser is None:
                cls._playwright = await async_playwright().start()
                cls._browser = await cls._playwright.chromium.launch(headless=True)
            return cls._browser

    @classmethod
    async def close(cls):
        async with cls._lock:
            if cls._browser:
                await cls._browser.close()
                cls._browser = None
            if cls._playwright:
                await cls._playwright.stop()
                cls._playwright = None

def _candidate_image_urls(url):
    if not url:
        return []
    src = str(url).strip()
    candidates = []
    try:
        parsed = urlsplit(src)
        host = parsed.netloc.lower()
        query_dict = dict(parse_qsl(parsed.query, keep_blank_values=True))

        # Prefer higher-resolution image variants for SNKRDUNK.
        if "snkrdunk.com" in host and "size" in query_dict:
            q_no_size = dict(query_dict)
            q_no_size.pop("size", None)
            candidates.append(urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(q_no_size), parsed.fragment)))

            q_large = dict(query_dict)
            q_large["size"] = "l"
            candidates.append(urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(q_large), parsed.fragment)))
    except Exception:
        pass

    candidates.append(src)
    seen = set()
    deduped = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            deduped.append(c)
    return deduped


def get_image_base64_from_url(url):
    if not url:
        return ""

    for candidate in _candidate_image_urls(url):
        try:
            req = urllib.request.Request(
                candidate,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                },
            )
            with urllib.request.urlopen(req, timeout=20) as response:
                img_data = response.read()
                b64 = base64.b64encode(img_data).decode("utf-8")
                mime = response.headers.get_content_type() or "image/png"
                if mime == "application/octet-stream":
                    lower_url = candidate.lower()
                    if lower_url.endswith(".jpg") or lower_url.endswith(".jpeg"):
                        mime = "image/jpeg"
                    elif lower_url.endswith(".webp"):
                        mime = "image/webp"
                    else:
                        mime = "image/png"
                return f"data:{mime};base64,{b64}"
        except Exception:
            continue

    print(f"Failed to fetch image from {url}")
    return ""


def _strip_white_border_background_png(logo_bytes):
    try:
        import numpy as np
        import matplotlib.image as mpimg
    except Exception:
        return logo_bytes

    try:
        arr = mpimg.imread(io.BytesIO(logo_bytes), format="png")
        if arr is None or arr.ndim != 3:
            return logo_bytes

        if arr.dtype != np.float32 and arr.dtype != np.float64:
            arr = arr.astype(np.float32) / 255.0

        if arr.shape[2] == 3:
            alpha = np.ones((arr.shape[0], arr.shape[1], 1), dtype=arr.dtype)
            arr = np.concatenate([arr, alpha], axis=2)

        rgb = arr[:, :, :3]
        alpha = arr[:, :, 3]

        # Already transparent: keep original bytes to avoid unnecessary re-encoding.
        if (alpha < 0.01).any():
            return logo_bytes

        h, w = alpha.shape
        edge_samples = np.vstack([rgb[0, :, :], rgb[h - 1, :, :], rgb[:, 0, :], rgb[:, w - 1, :]])
        bg_color = np.median(edge_samples, axis=0)

        dist = np.sqrt(np.sum((rgb - bg_color) ** 2, axis=2))
        bg_mask = (dist < 0.20) & (alpha > 0.75)

        # Also capture near-white backgrounds.
        near_white = (
            (rgb[:, :, 0] > 0.93)
            & (rgb[:, :, 1] > 0.93)
            & (rgb[:, :, 2] > 0.93)
            & (alpha > 0.75)
        )
        bg_mask = bg_mask | near_white

        visited = np.zeros((h, w), dtype=bool)
        q = deque()

        def push(y, x):
            if 0 <= y < h and 0 <= x < w and bg_mask[y, x] and not visited[y, x]:
                visited[y, x] = True
                q.append((y, x))

        for x in range(w):
            push(0, x)
            push(h - 1, x)
        for y in range(h):
            push(y, 0)
            push(y, w - 1)

        while q:
            y, x = q.popleft()
            push(y - 1, x)
            push(y + 1, x)
            push(y, x - 1)
            push(y, x + 1)

        if not visited.any():
            return logo_bytes

        arr[:, :, 3] = np.where(visited, 0.0, alpha)

        out = io.BytesIO()
        plt.imsave(out, arr, format="png")
        return out.getvalue()
    except Exception as e:
        print(f"⚠️ Logo transparency processing failed: {e}")
        return logo_bytes


async def _screenshot_poster_root(page, out_path):
    try:
        await page.evaluate(
            """async () => {
                if (document.fonts && document.fonts.ready) {
                    await document.fonts.ready;
                }
            }"""
        )
    except Exception:
        pass
    await page.wait_for_timeout(300)

    locator = page.locator('[data-poster-root="true"], [data-poster-root]').first
    if await locator.count() > 0:
        await locator.screenshot(path=out_path, type="png", animations="disabled")
        return

    await page.screenshot(path=out_path, type="png", full_page=False, animations="disabled")

def parse_level_and_desc(text):
    text = str(text).strip()
    match = re.match(r'^([A-Za-z]+)[。，,：:\s]+(.*)', text)
    if match:
        level = match.group(1).capitalize()
        desc = match.group(2).strip().lstrip('\\').lstrip(':').lstrip(' ').strip()
        return level, desc
    return "Medium", text.lstrip('\\').lstrip(':').lstrip(' ').strip()

def get_width_from_level(level):
    l = level.lower()
    if 'high' in l or 'outstanding' in l: return 90
    if 'medium' in l: return 60
    if 'low' in l: return 30
    return 50

def generate_features_html(features_text, theme="dark"):
    lines = [L.strip().lstrip('•').strip() for L in str(features_text).split('\n') if L.strip()]
    icons = ['verified', 'hotel_class', 'bolt', 'star', 'diamond']
    html = ""
    is_light = (theme == "light")
    panel_overlay = "bg-slate-900/[0.03] group-hover:bg-slate-900/[0.06]" if is_light else "bg-primary/5 group-hover:bg-primary/10"
    icon_class = "text-amber-600" if is_light else "text-primary-light drop-shadow-[0_0_5px_rgba(212,175,55,1)]"
    title_class = "text-slate-800" if is_light else "text-white"
    desc_class = "text-slate-600" if is_light else "text-slate-100"
    for i, line in enumerate(lines[:2]):
        title = line
        desc = ""
        if '：' in line:
            parts = line.split('：', 1)
            title = parts[0]
            desc = parts[1]
        elif len(line) > 15:
            title = "Special Feature"
            desc = line
        else:
            title = line
            desc = ""
            
        icon = icons[i % len(icons)]
        col_span = " md:col-span-2" if len(lines) == 3 and i == 2 else ""
        
        desc_html = f'<p class="{desc_class} text-[14px] mt-1.5 leading-relaxed">{desc}</p>' if desc else ''
        
        html += f"""
<div class="glass-panel p-5 rounded-xl flex items-start gap-4{col_span} relative overflow-hidden group">
<div class="absolute inset-0 {panel_overlay} transition-colors pointer-events-none"></div>
<span class="material-symbols-outlined {icon_class} mt-0.5 text-[26px]">{icon}</span>
<div class="relative z-10 flex flex-col justify-center">
<h4 class="{title_class} font-bold text-[16px] tracking-wide">{title}</h4>
{desc_html}
</div>
</div>
"""
    return html

def generate_table_rows(records, is_jpy=False, target_grade=None, theme="dark"):
    is_light = (theme == "light")
    if is_light:
        empty_cls = "text-slate-500"
        row_hover = "hover:bg-slate-900/[0.04]"
        date_cls = "text-slate-700"
        grade_cls = "text-slate-500"
        price_cls = "text-sky-700"
    else:
        empty_cls = "text-slate-300"
        row_hover = "hover:bg-primary/5"
        date_cls = "text-slate-300"
        grade_cls = "text-slate-400"
        price_cls = "text-primary"

    if not records:
        return f'<tr><td colspan="3" class="p-3 pl-4 {empty_cls} text-center">No transactions found</td></tr>'
        
    filtered_records = []
    if target_grade:
        for r in records:
            if is_jpy:
                filtered_records.append(r)
            else:
                if r.get('grade') == target_grade:
                    filtered_records.append(r)
        
        if not filtered_records:
            filtered_records = records
    else:
        filtered_records = records

    html = ""
    for r in filtered_records[:10]:
        date = r['date']
        grade = r.get('grade', 'Ungraded')
        if is_jpy:
            jpy = int(r['price'])
            usd = int(jpy / 150) # Rough exchange reference
            price_str = f"¥{jpy:,} (~${usd})"
        else:
            price_str = f"${float(r['price']):.2f}"
            
        html += f"""
<tr class="{row_hover} transition-colors">
<td class="p-4 pl-4 {date_cls} text-base">{date}</td>
<td class="p-4 {grade_cls} text-base">{grade}</td>
<td class="p-4 pr-4 text-right font-medium {price_cls} text-base">{price_str}</td>
</tr>
"""
    return html

def get_badge_html(grade):
    grade_upper = grade.upper()
    if 'PSA' in grade_upper:
        company = 'PSA'
        num = grade_upper.replace('PSA', '').strip()
        label = 'GEM MT' if num == '10' else ('MINT' if num == '9' else 'NM-MT')
        return f"""
<div class="w-24 h-24 bg-gradient-to-br from-primary via-[#e6c21f] to-[#b39616] rounded-full flex items-center justify-center shadow-[0_4px_20px_rgba(0,0,0,0.5)] z-30 border-4 border-[#4a4220]">
<div class="text-center">
<span class="block text-[#4a4220] font-black text-xs tracking-wider">{label}</span>
<span class="block text-[#221f10] font-bold text-3xl leading-none">{num}</span>
<span class="block text-[#4a4220] font-bold text-[10px] tracking-widest mt-0.5">{company}</span>
</div>
</div>"""
    elif 'BGS' in grade_upper:
        return f"""
<div class="w-24 h-24 bg-gradient-to-br from-slate-300 via-slate-100 to-slate-400 rounded-lg flex items-center justify-center shadow-[0_4px_20px_rgba(0,0,0,0.5)] z-30 border-4 border-slate-500 transform rotate-3">
<div class="text-center">
<span class="block text-slate-800 font-bold text-3xl leading-none">{grade_upper.replace('BGS','').strip()}</span>
<span class="block text-slate-600 font-bold text-[10px] tracking-widest mt-0.5">BGS</span>
</div>
</div>"""
    else:
        return f"""
<div class="badge-ungraded px-4 py-2 rounded-full shadow-xl z-30">
<span class="text-slate-200 font-bold text-sm">{grade}</span>
</div>"""


def create_premium_matplotlib_chart_b64(records, color_line='#f4d125', target_grade="PSA 10", is_jpy=False, theme="dark"):
    import re
    from datetime import datetime, timedelta
    import matplotlib.dates as mdates
    from collections import defaultdict
    import matplotlib.pyplot as plt
    import io, base64

    if records is None: records = []

    def parse_d(d_str):
        try:
            if '日前' in d_str: return datetime.now() - timedelta(days=int(re.search(r'\d+', d_str).group()))
            if '小時前' in d_str or '時間前' in d_str: return datetime.now() - timedelta(hours=int(re.search(r'\d+', d_str).group()))
            if '分前' in d_str: return datetime.now() - timedelta(minutes=int(re.search(r'\d+', d_str).group()))
            if '-' in d_str: return datetime.strptime(d_str.strip(), '%Y-%m-%d')
            if '/' in d_str: return datetime.strptime(d_str.strip(), '%Y/%m/%d')
            if ',' in d_str: return datetime.strptime(d_str.strip(), '%b %d, %Y')
        except: pass
        return datetime.now()

    if is_jpy:
        if '10' in str(target_grade) or str(target_grade).upper() == 'S': valid_grades = ['S', 'PSA10', 'PSA 10']
        elif str(target_grade).lower() in ['ungraded', 'a']: valid_grades = ['A']
        else: valid_grades = [target_grade, target_grade.replace(' ', '')]

    else:
        if '10' in str(target_grade): valid_grades = ['PSA 10']
        else: valid_grades = None  # None means: show all non-PSA10 records


    if valid_grades is None:
        # Show all non-PSA10 records (PSA 9, Raw, Ungraded, etc.)
        filt = [r for r in records if r.get('grade', 'Ungraded') != 'PSA 10']
    else:
        filt = [r for r in records if r.get('grade', 'Ungraded') in valid_grades]

    
    date_to_prices = defaultdict(list)
    for r in filt:
        d = parse_d(r['date']).date() 
        price_val = float(r['price'])
        if is_jpy:
            price_val = price_val / 150.0
        date_to_prices[d].append(price_val)
        
    sorted_dates = sorted(list(date_to_prices.keys()))

    # Trim leading gap: if consecutive data points have a gap >= 60 days (2 months),
    # only show data from after the last such gap (avoids ugly blank stretches)
    if len(sorted_dates) > 1:
        cutoff_idx = 0
        for i in range(1, len(sorted_dates)):
            if (sorted_dates[i] - sorted_dates[i - 1]).days >= 60:
                cutoff_idx = i
        if cutoff_idx > 0:
            sorted_dates = sorted_dates[cutoff_idx:]

    if theme == "light":
        axis_text = '#28425c'
        axis_text_2 = '#4f6d89'
        grid_color = '#8aa6bf'
        spine_color = '#8aa6bf'
        legend_color = '#28425c'
        bar_color = '#aec7db'
        point_edge = '#ffffff'
        bar_alpha = 0.65
        line_width = 3.8
        point_size = 62
        point_edge_width = 1.8
        y_grid_alpha = 0.35
        x_grid_alpha = 0.22
    else:
        axis_text = '#cbc190'
        axis_text_2 = '#a1a1aa'
        grid_color = '#f4d125'
        spine_color = '#685f31'
        legend_color = 'white'
        bar_color = '#fed7aa'
        point_edge = '#ffffff'
        bar_alpha = 0.85
        line_width = 3.0
        point_size = 50
        point_edge_width = 1.5
        y_grid_alpha = 0.2
        x_grid_alpha = 0.1

    # Use a wider/taller canvas ratio to better fill the poster chart slot.
    fig, ax1 = plt.subplots(figsize=(8.0, 3.6), facecolor='none')
    ax1.set_facecolor('none')

    if not sorted_dates:
        ax1.axis('off')
        buf = io.BytesIO()
        plt.savefig(buf, format='png', transparent=True)
        buf.seek(0)
        plt.close(fig)
        return f"data:image/png;base64,{base64.b64encode(buf.getvalue()).decode('utf-8')}"
        
    prices = [sum(date_to_prices[d])/len(date_to_prices[d]) for d in sorted_dates]
    volumes = [len(date_to_prices[d]) for d in sorted_dates]
    
    # Legend labels
    price_label = "Price (Daily Avg)" if not is_jpy else "Price (Daily Avg, USD)"
    vol_label = "Quantity"

    if len(sorted_dates) == 1:
        sorted_dates = [sorted_dates[0] - timedelta(days=1), sorted_dates[0], sorted_dates[0] + timedelta(days=1)]
        prices = [prices[0], prices[0], prices[0]]
        volumes = [0, volumes[0], 0]

    ax2 = ax1.twinx()
    
    # 1. Bar Chart (Volume / Quantity) on Right Axis
    ax2.bar(sorted_dates, volumes, color=bar_color, alpha=bar_alpha, width=0.7, zorder=1, label="Quantity")
    
    # 2. Line Chart (Price) on Left Axis
    ax1.plot(sorted_dates, prices, color=color_line, linewidth=line_width, zorder=4, label=price_label)
    ax1.scatter(sorted_dates, prices, color=color_line, s=point_size, edgecolors=point_edge, linewidths=point_edge_width, zorder=5)

    # Keep headroom so top-left legend does not collide with high points.
    p_min = min(prices) if prices else 0
    p_max = max(prices) if prices else 1
    p_span = max(p_max - p_min, 1.0)
    y_bottom = max(0.0, p_min - p_span * 0.12)
    y_top = p_max + p_span * 0.38
    ax1.set_ylim(y_bottom, y_top)

    # Styles
    for ax in [ax1, ax2]:
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
    ax1.spines['bottom'].set_color(spine_color)
    ax2.spines['bottom'].set_visible(False)

    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    # --- Intelligent Labeling Fix ---
    # Use AutoDateLocator to prevent crowded labels
    locator = mdates.AutoDateLocator(minticks=3, maxticks=7)
    ax1.xaxis.set_major_locator(locator)
    
    ax1.tick_params(axis='x', colors=axis_text, labelsize=10, rotation=16)
    ax1.tick_params(axis='y', colors=axis_text, labelsize=12)
    ax2.tick_params(axis='y', colors=axis_text_2, labelsize=12)
    
    # Bold labels
    for t in ax1.get_xticklabels() + ax1.get_yticklabels():
        t.set_fontweight('bold')
    for t in ax2.get_yticklabels():
        t.set_fontweight('bold')

    # Reduce outer padding while keeping axis labels readable.
    fig.subplots_adjust(left=0.055, right=0.985, top=0.96, bottom=0.17)
    plt.margins(x=0.01)

    ax1.yaxis.grid(color=grid_color, linestyle=':', linewidth=1, alpha=y_grid_alpha)
    ax1.xaxis.grid(color=grid_color, linestyle=':', linewidth=1, alpha=x_grid_alpha)

    # Ensure Line draws over Bars
    ax1.set_zorder(ax2.get_zorder()+1)
    ax1.patch.set_visible(False)
    
    # Scale ax2 so bars only occupy bottom half
    ax2.set_ylim(0, max(volumes) * 2.2)

    # Legend (same anchor region, with safer overlap due added Y headroom).
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(
        lines_1 + lines_2,
        [price_label, vol_label],
        loc='upper left',
        bbox_to_anchor=(0.01, 0.995),
        prop={'size': 11},
        frameon=False,
        labelcolor=legend_color,
    )

    buf = io.BytesIO()
    # Preserve predictable aspect ratio for poster slots while keeping transparent bg.
    plt.savefig(buf, format='png', transparent=True, dpi=220, pad_inches=0)
    buf.seek(0)
    plt.close(fig)
    return f"data:image/png;base64,{base64.b64encode(buf.getvalue()).decode('utf-8')}"

def calculate_arbitrage_stats(pc_records, snkr_records):
    pc_safe = pc_records or []
    snkr_safe = snkr_records or []
    
    # Calculate stats for the bottom section
    prices_10 = [float(r['price']) for r in pc_safe if '10' in str(r.get('grade', ''))]
    prices_raw = [float(r['price']) for r in pc_safe if 'Ungraded' in str(r.get('grade', ''))]
    prices_9 = [float(r['price']) for r in pc_safe if '9' in str(r.get('grade', ''))]
    
    avg_10 = sum(prices_10)/len(prices_10) if len(prices_10) > 0 else 0
    max_10 = max(prices_10) if len(prices_10) > 0 else 0
    avg_raw = sum(prices_raw)/len(prices_raw) if len(prices_raw) > 0 else 0
    avg_9 = sum(prices_9)/len(prices_9) if len(prices_9) > 0 else 0
    
    snkr_10 = [float(r['price']) for r in snkr_safe if r.get('grade', '') in ['S', 'PSA10', 'PSA 10']]
    snkr_raw = [float(r['price']) for r in snkr_safe if 'A' in str(r.get('grade', ''))]
    
    # Arbitrage Profit estimation for Raw -> PSA 10 (Targeting Max Price)
    # Grading fee estimated around 1100 TWD (~$35 USD) + 10% value upcharge
    profit = 0
    if max_10 > 0 and avg_raw > 0:
        grading_cost = 35.0 + (max_10 * 0.10)
        profit = max_10 - (avg_raw + grading_cost)
        
    return avg_10, avg_9, avg_raw, profit, max_10

async def generate_report(card_data, snkr_records, pc_records, out_dir=None, template_version="v3"):
    if not out_dir:
        out_dir = BASE_DIR

    selected_version, template_dir, template1_path, template2_path = _resolve_template_bundle(template_version)
    print(f"🖼️ Poster template version: {selected_version} | profile={os.path.basename(template1_path)} | market={os.path.basename(template2_path)}")
    # v3 currently uses a dark profile poster + light market-data poster.
    # Keep text/chart palette aligned to each poster surface.
    if selected_version == "v3":
        profile_theme = "light"
        market_theme = "light"
    else:
        profile_theme = "dark"
        market_theme = "dark"
    chart_line_color = "#1f6f8b" if market_theme == "light" else "#f4d125"
    chart_img_class = "block w-full h-full object-fill" if market_theme == "light" else "block w-full h-full object-fill mix-blend-screen"
    chart_img_class_raw = "block w-full h-full object-fill" if market_theme == "light" else "block w-full h-full object-fill mix-blend-screen opacity-90"

    with open(template1_path, 'r', encoding='utf-8') as f:
        html1 = f.read()
    with open(template2_path, 'r', encoding='utf-8') as f:
        html2 = f.read()

    # Inline logo image when templates reference local "logo.png".
    logo_path = os.path.join(template_dir, "logo.png")
    if os.path.exists(logo_path):
        try:
            with open(logo_path, "rb") as logo_f:
                logo_bytes = logo_f.read()
            # If logo has white border/background, remove edge-connected white area.
            logo_bytes = _strip_white_border_background_png(logo_bytes)
            logo_b64 = base64.b64encode(logo_bytes).decode("utf-8")
            logo_src = f"data:image/png;base64,{logo_b64}"
            html1 = html1.replace('src="logo.png"', f'src="{logo_src}"').replace("src='logo.png'", f"src='{logo_src}'")
            html2 = html2.replace('src="logo.png"', f'src="{logo_src}"').replace("src='logo.png'", f"src='{logo_src}'")
        except Exception as e:
            print(f"⚠️ Logo inline failed: {e}")

    name = card_data.get('c_name') or card_data.get('jp_name') or card_data.get('name', 'Unknown Trading Card')
    safe_name = name.replace(' ', '_').replace('/', '_')
    
    mh_level, mh_desc = parse_level_and_desc(card_data.get('market_heat', 'Medium'))
    cv_level, cv_desc = parse_level_and_desc(card_data.get('collection_value', 'Medium'))
    cf_level, cf_desc = parse_level_and_desc(card_data.get('competitive_freq', 'Low'))
    
    card_img_b64 = get_image_base64_from_url(card_data.get('img_url', ''))
    
    p_prices = [r['price'] for r in pc_records] if pc_records else [0]
    total_entries = (len(snkr_records) if snkr_records else 0) + (len(pc_records) if pc_records else 0)
    
    avg_10, avg_9, avg_raw, profit, max_10 = calculate_arbitrage_stats(pc_records, snkr_records) if pc_records else (0,0,0,0,0)
    
    market_grade = str(card_data.get('grade', 'Ungraded')).upper()
    if market_grade in ['UNGRADED', 'A']:
        badge_mode = 'ungraded'
    elif 'PSA' in market_grade or 'BGS' in market_grade:
        badge_mode = 'psa10'
    else:
        badge_mode = 'both'
        
    from datetime import datetime, timedelta
    import re
    
    def parse_d(d_str):
        try:
            if '日前' in d_str: return datetime.now() - timedelta(days=int(re.search(r'\d+', d_str).group()))
            if '小時前' in d_str or '時間前' in d_str: return datetime.now() - timedelta(hours=int(re.search(r'\d+', d_str).group()))
            if '分前' in d_str: return datetime.now() - timedelta(minutes=int(re.search(r'\d+', d_str).group()))
            if '-' in d_str: return datetime.strptime(d_str.strip(), '%Y-%m-%d')
            if '/' in d_str: return datetime.strptime(d_str.strip(), '%Y/%m/%d')
            if ',' in d_str: return datetime.strptime(d_str.strip(), '%b %d, %Y')
        except: pass
        return datetime.now()

    def count_30_days(records_list, tgt_grade):
        cutoff = datetime.now() - timedelta(days=30)
        return len([r for r in (records_list or []) if r.get('grade') == tgt_grade and parse_d(r['date']) > cutoff])

    target_grade_1 = card_data.get('grade', 'Ungraded')
    recent_prices = []
    sixty_days_ago = datetime.now() - timedelta(days=60)
    
    if pc_records:
        recent_prices.extend([float(r['price']) for r in pc_records if r.get('grade') == target_grade_1 and parse_d(r['date']) >= sixty_days_ago])
        
    if snkr_records:
        if '10' in target_grade_1:
            valid_snkr_grades = ['S', 'PSA10', 'PSA 10']
        elif target_grade_1.lower() == 'ungraded':
            valid_snkr_grades = ['A']
        else:
            valid_snkr_grades = [target_grade_1, target_grade_1.replace(' ', '')]
            
        recent_prices.extend([float(r['price']) / 150.0 for r in snkr_records if r.get('grade') in valid_snkr_grades and parse_d(r['date']) >= sixty_days_ago])
        
    recent_avg = sum(recent_prices) / len(recent_prices) if recent_prices else 0
    recent_avg_str = f"${recent_avg:.2f}" if recent_avg > 0 else "N/A"

    replacements_1 = {
        "{{ card_name }}": name,
        "{{ card_number }}": card_data.get('number', 'Unknown'),
        "{{ card_set }}": card_data.get('set_code', 'Unknown Set'),
        "{{ grade }}": card_data.get('grade', 'Ungraded'),
        "{{ badge_mode }}": badge_mode,
        "{{ category }}": card_data.get('category', 'PROMO'),
        "{{ market_heat_level }}": mh_level,
        "{{ market_heat_desc }}": mh_desc,
        "{{ market_heat_width }}": str(get_width_from_level(mh_level)),
        "{{ collection_value_level }}": cv_level,
        "{{ collection_value_desc }}": cv_desc,
        "{{ collection_value_width }}": str(get_width_from_level(cv_level)),
        "{{ competitive_freq_level }}": cf_level,
        "{{ competitive_freq_desc }}": cf_desc,
        "{{ competitive_freq_width }}": str(get_width_from_level(cf_level)),
        "{{ features_html }}": generate_features_html(card_data.get('features', ''), theme=profile_theme),
        "{{ illustrator }}": card_data.get('illustrator', 'Unknown'),
        "{{ release_info }}": card_data.get('release_info', 'Unknown'),
        "{{ card_image }}": card_img_b64,
        "{{ badge_html }}": get_badge_html(card_data.get('grade', 'Ungraded')),
        "{{ recent_avg_price }}": recent_avg_str,
        "{{ target_grade }}": target_grade_1
    }
    
    import re
    for k, v in replacements_1.items():
        # Convert "{{ key }}" to pattern "\{\{\s*key\s*\}\}"
        core_key = k.replace('{{ ', '').replace(' }}', '').replace('{{', '').replace('}}', '').strip()
        pattern = r'\{\{\s*' + re.escape(core_key) + r'\s*\}\}'
        html1 = re.sub(pattern, str(v).replace('\\', r'\\') if v is not None else "", html1)
        

    # --- Dynamic Charts and Stats Construction ---
    target_grade = card_data.get('grade', 'Ungraded')

    # Calculate time span for Total Entries
    all_dates = []
    for r in (pc_records or []) + (snkr_records or []):
        all_dates.append(parse_d(r['date']))
        
    days_span = ""
    if all_dates:
        min_date = min(all_dates)
        delta_days = (datetime.now() - min_date).days
        if delta_days == 0:
            days_span = " (24h內)"
        elif delta_days < 30:
            days_span = f" (近{delta_days}天)"
        elif delta_days <= 60:
            days_span = f" (近1個月)" # roughly
        else:
            months = round(delta_days / 30)
            days_span = f" (近{months}個月)"

    is_raw = target_grade in ['Ungraded', 'A']

    if is_raw:
        # Generate 4 Charts (2 per column) with 30-day volume metrics overlaid
        c_pc_10 = create_premium_matplotlib_chart_b64(pc_records, color_line=chart_line_color, target_grade='PSA 10', is_jpy=False, theme=market_theme)
        c_pc_raw = create_premium_matplotlib_chart_b64(pc_records, color_line=chart_line_color, target_grade='Ungraded', is_jpy=False, theme=market_theme)
        c_sk_10 = create_premium_matplotlib_chart_b64(snkr_records, color_line=chart_line_color, target_grade='S', is_jpy=True, theme=market_theme)
        c_sk_raw = create_premium_matplotlib_chart_b64(snkr_records, color_line=chart_line_color, target_grade='A', is_jpy=True, theme=market_theme)
        
        v_pc_10 = count_30_days(pc_records, 'PSA 10')
        v_pc_raw_cutoff = datetime.now() - timedelta(days=30)
        v_pc_raw = len([r for r in (pc_records or []) if r.get('grade') != 'PSA 10' and parse_d(r['date']) > v_pc_raw_cutoff])
        
        # SNKRDUNK volume metrics (Synced with chart filters)
        v_sk_10_cutoff = datetime.now() - timedelta(days=30)
        v_sk_10 = len([r for r in (snkr_records or []) if r.get('grade') in ['S', 'PSA10', 'PSA 10'] and parse_d(r['date']) > v_sk_10_cutoff])
        v_sk_raw = count_30_days(snkr_records, 'A')

        pc_charts_html = f"""
        <div class="w-full flex flex-col gap-6 mb-2 mt-4">
            <div class="relative glass-panel rounded-xl border border-green-500/40 p-2 h-[220px] overflow-hidden shadow-[0_0_20px_rgba(34,197,94,0.15)]">
                <span class="absolute top-[-14px] left-4 text-[10px] font-bold text-white tracking-widest bg-black border border-green-500/50 px-3 py-1 rounded-full z-20 shadow-lg">PSA 10 Trend</span>
                <span class="absolute top-[-14px] right-4 text-[10px] font-bold text-white bg-black/90 px-3 py-1 rounded-full border border-green-500/50 z-20 shadow-lg">30d Vol: {v_pc_10} Set</span>
                <img src="{c_pc_10}" class="{chart_img_class_raw}" />
            </div>
            <div class="relative glass-panel rounded-xl border border-red-500/40 p-2 h-[220px] overflow-hidden shadow-[0_0_20px_rgba(239,68,68,0.15)]">
                <span class="absolute top-[-14px] left-4 text-[10px] font-bold text-white tracking-widest bg-black border border-red-500/50 px-3 py-1 rounded-full z-20 shadow-lg">Ungraded Trend</span>
                <span class="absolute top-[-14px] right-4 text-[10px] font-bold text-white bg-black/90 px-3 py-1 rounded-full border border-red-500/50 z-20 shadow-lg">30d Vol: {v_pc_raw} Set</span>
                <img src="{c_pc_raw}" class="{chart_img_class_raw}" />
            </div>
        </div>"""
        
        snkr_charts_html = f"""
        <div class="w-full flex flex-col gap-6 mb-2 mt-4">
            <div class="relative glass-panel rounded-xl border border-green-500/40 p-2 h-[220px] overflow-hidden shadow-[0_0_20px_rgba(34,197,94,0.15)]">
                <span class="absolute top-[-14px] left-4 text-[10px] font-bold text-white tracking-widest bg-black border border-green-500/50 px-3 py-1 rounded-full z-20 shadow-lg">PSA 10 Trend</span>
                <span class="absolute top-[-14px] right-4 text-[10px] font-bold text-white bg-black/90 px-3 py-1 rounded-full border border-green-500/50 z-20 shadow-lg">30d Vol: {v_sk_10} Set</span>
                <img src="{c_sk_10}" class="{chart_img_class_raw}" />
            </div>
            <div class="relative glass-panel rounded-xl border border-red-500/40 p-2 h-[220px] overflow-hidden shadow-[0_0_20px_rgba(239,68,68,0.15)]">
                <span class="absolute top-[-14px] left-4 text-[10px] font-bold text-white tracking-widest bg-black border border-red-500/50 px-3 py-1 rounded-full z-20 shadow-lg">Ungraded Trend</span>
                <span class="absolute top-[-14px] right-4 text-[10px] font-bold text-white bg-black/90 px-3 py-1 rounded-full border border-red-500/50 z-20 shadow-lg">30d Vol: {v_sk_raw} Set</span>
                <img src="{c_sk_raw}" class="{chart_img_class_raw}" />
            </div>
        </div>"""

        stat_1_t, stat_1_v = "PSA 10 Avg (完整品)", f"${avg_10:.2f}" if avg_10 > 0 else "N/A"
        stat_2_t, stat_2_v = "Ungraded Avg (裸卡)", f"${avg_raw:.2f}" if avg_raw > 0 else "N/A"
        stat_3_t, stat_3_v = "PSA 10 Max (最高成交價)", f"${max_10:.2f}" if max_10 > 0 else "N/A"
        stat_4_t, stat_4_v = f"Total Entries{days_span}", str(total_entries)
        
        pc_table_html = ""
        snkr_table_html = ""
        
    else:
        # Standard 2 Charts (For Graded Cards)
        if market_theme == "light":
            table_outer_border = "border-slate-300/70"
            table_head_border = "border-slate-300/70"
            table_head_text = "text-slate-600"
            table_body_divider = "divide-slate-200/80"
        else:
            table_outer_border = "border-border-gold/30"
            table_head_border = "border-border-gold/20"
            table_head_text = "text-primary-dark"
            table_body_divider = "divide-border-gold/10"

        if snkr_records:
            if '10' in target_grade:
                valid_snkr_grades = ['S', 'PSA10', 'PSA 10']
            elif target_grade.lower() == 'ungraded':
                valid_snkr_grades = ['A']
            else:
                valid_snkr_grades = [target_grade, target_grade.replace(' ', '')]
            
            snkr_target_records = [r for r in snkr_records if r['grade'] in valid_snkr_grades]
        else:
            snkr_target_records = []

        c_pc = create_premium_matplotlib_chart_b64(pc_records, color_line=chart_line_color, target_grade=target_grade, is_jpy=False, theme=market_theme)
        c_sk = create_premium_matplotlib_chart_b64(snkr_target_records, color_line=chart_line_color, target_grade=target_grade, is_jpy=True, theme=market_theme)
        
        pc_charts_html = f"""
        <div class="w-full h-[220px] mt-2 mb-1 flex items-end justify-center relative overflow-hidden">
            <img src="{c_pc}" class="{chart_img_class}" />
        </div>"""
        
        pc_table_html = f"""
                <div class="flex-1 glass-panel rounded-xl overflow-hidden p-3 border {table_outer_border}">
                    <table class="w-full text-left border-collapse">
                        <thead>
                            <tr class="border-b {table_head_border} text-[10px] font-black uppercase tracking-widest {table_head_text}">
                                <th class="p-3">Date (日期)</th>
                                <th class="p-3">Grade (狀態)</th>
                                <th class="p-3 text-right">Price (金額)</th>
                            </tr>
                        </thead>
                        <tbody class="text-sm divide-y {table_body_divider}">
                            {generate_table_rows(pc_records, is_jpy=False, target_grade=card_data.get('grade', ''), theme=market_theme)}
                        </tbody>
                    </table>
                </div>"""
        
        if snkr_target_records:
            snkr_charts_html = f"""
        <div class="w-full h-[220px] mt-2 mb-1 flex items-end justify-center overflow-hidden">
            <img src="{c_sk}" class="{chart_img_class}" />
        </div>"""
            snkr_table_html = f"""
                <div class="flex-1 glass-panel rounded-xl overflow-hidden p-3 border {table_outer_border}">
                    <table class="w-full text-left border-collapse">
                        <thead>
                            <tr class="border-b {table_head_border} text-[10px] font-black uppercase tracking-widest {table_head_text}">
                                <th class="p-3">Time (時間)</th>
                                <th class="p-3">Grade (狀態)</th>
                                <th class="p-3 text-right">Price (金額)</th>
                            </tr>
                        </thead>
                        <tbody class="text-sm divide-y {table_body_divider}">
                            {generate_table_rows(snkr_target_records, is_jpy=True, theme=market_theme)}
                        </tbody>
                    </table>
                </div>"""
        else:
            snkr_charts_html = """
        <div class="w-full h-[220px] mt-2 mb-1 glass-panel rounded-xl border border-slate-300/70 flex items-center justify-center">
            <p class="text-slate-500 text-sm font-semibold tracking-wide">No SNKRDUNK trend data for this grade</p>
        </div>"""
            snkr_table_html = f"""
                <div class="glass-panel rounded-xl p-6 border {table_outer_border} text-center">
                    <p class="text-slate-500 text-sm font-semibold">No SNKRDUNK transactions found for {target_grade}</p>
                </div>"""
                
        tgt_prices = []
        if pc_records:
            tgt_prices.extend([float(r['price']) for r in pc_records if r.get('grade') == target_grade])
        if snkr_target_records:
            tgt_prices.extend([float(r['price']) / 150.0 for r in snkr_target_records])
            
        avg_tgt = sum(tgt_prices)/len(tgt_prices) if tgt_prices else 0
        stat_1_t, stat_1_v = f"{target_grade} Avg (均價)", f"${avg_tgt:.2f}" if avg_tgt > 0 else "N/A"
        # SAFETY CHECK for empty sequences
        stat_2_t, stat_2_v = f"{target_grade} Min (最低)", f"${min(tgt_prices):.2f}" if tgt_prices else "N/A"
        stat_3_t, stat_3_v = f"{target_grade} Max (最高)", f"${max(tgt_prices):.2f}" if tgt_prices else "N/A"
        
        stat_4_t, stat_4_v = f"Total Entries{days_span}", str(total_entries)

    replacements_2 = {
        "{{ card_name }}": name,
        "{{ card_set }}": card_data.get('set_code', ''),
        "{{ grade }}": card_data.get('grade', ''),
        "{{ stat_1_title }}": stat_1_t,
        "{{ stat_1_val }}": stat_1_v,
        "{{ stat_2_title }}": stat_2_t,
        "{{ stat_2_val }}": stat_2_v,
        "{{ stat_3_title }}": stat_3_t,
        "{{ stat_3_val }}": stat_3_v,
        "{{ stat_4_title }}": stat_4_t,
        "{{ stat_4_val }}": stat_4_v,
        "{{ pc_charts_html }}": pc_charts_html,
        "{{ pc_table_html }}": pc_table_html,
        "{{ snkr_charts_html }}": snkr_charts_html,
        "{{ snkr_table_html }}": snkr_table_html
    }
    
    for k, v in replacements_2.items():
        core_key = k.replace('{{ ', '').replace(' }}', '').replace('{{', '').replace('}}', '').strip()
        pattern = r'\{\{\s*' + re.escape(core_key) + r'\s*\}\}'
        html2 = re.sub(pattern, str(v).replace('\\', r'\\') if v is not None else "", html2)

    out_path_1 = os.path.join(out_dir, f"report_{safe_name}_profile.png")
    out_path_2 = os.path.join(out_dir, f"report_{safe_name}_data.png")

    async with RENDER_SEMAPHORE:
        browser = await AsyncBrowserManager.get_browser()
        
        # We create a fresh context per request but reuse the browser instance
        # This is very memory efficient and fast
        context = await browser.new_context(
            viewport={"width": 1280, "height": 1000},
            device_scale_factor=2,
        )
        
        try:
            page1 = await context.new_page()
            await page1.set_content(html1, wait_until="networkidle")
            await _screenshot_poster_root(page1, out_path_1)
            await page1.close()
            
            page2 = await context.new_page()
            await page2.set_content(html2, wait_until="networkidle")
            await _screenshot_poster_root(page2, out_path_2)
            await page2.close()
        finally:
            await context.close()

    return [out_path_1, out_path_2]

if __name__ == "__main__":
    import random
    from datetime import timedelta
    test_data = {
        'c_name': '皮卡丘 V (Pikachu V)',
        'set_code': '25th Anniversary Golden Box',
        'number': '005/015',
        'category': 'Promo',
        'grade': 'PSA 10',
        'release_info': '2021 Pokemon Japanese',
        'illustrator': 'Ryota Murayama',
        'img_url': 'https://s3.ap-northeast-1.amazonaws.com/image.snkrdunk.com/trading-cards/products/202501/7/8ce38fc1-f761-4606-aec0-d3e9c5edc507.jpg',
        'market_heat': 'High，此卡來自於全球熱搶的 25 週年黃金紀念箱，皮卡丘作為招牌角色，其限定版本在二手中市場具有極高的流動性與熱度。',
        'collection_value': 'High，黃金盒限定卡片且具備 PSA 10 的滿分等級，在收藏市場中屬於頂級配置，具備非常穩定的長期持有價值。',
        'competitive_freq': 'Low，雖然可以在官方賽事中使用，但這張卡片主要被視為收藏品，在主流競技套牌中的出現頻率較低。',
        'features': '• 25 週年紀念限定版本\n• 卡面印有 25th Anniversary 專屬標誌\n• 全圖閃卡工藝配合生動的電擊特效背景'
    }
    
    snkr_test = []
    base_date = datetime(2025, 2, 8)
    current_price = 150000
    for i in range(10):
        d = (base_date - timedelta(days=i)).strftime('%Y/%m/%d')
        current_price = current_price + random.randint(-5000, 6000)
        snkr_test.append({'date': d, 'price': current_price, 'grade': 'PSA 10'})
            
    pc_test = []
    current_usd = 1100
    for i in range(10):
        d = (base_date - timedelta(days=i*1.2)).strftime('%Y-%m-%d')
        current_usd = current_usd + random.randint(-20, 25)
        pc_test.append({'date': d, 'price': current_usd, 'grade': 'PSA 10'})
    
    print("Generating HTML/Playwright Two-Poster Report...")
    out_imgs = generate_report(test_data, snkr_test, pc_test)
    print(f"Posters saved to {out_imgs}")

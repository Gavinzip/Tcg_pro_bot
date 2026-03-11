#!/usr/bin/env python3
"""
模擬 bot.py 的並發處理：同時送三張圖，看是否真的並發執行
"""
import asyncio
import time
import os
import market_report_vision

market_report_vision.REPORT_ONLY = False  # 只要文字報告，不跑 Playwright

API_KEY = os.getenv("MINIMAX_API_KEY")
if not API_KEY:
    from dotenv import load_dotenv
    load_dotenv()
    API_KEY = os.getenv("MINIMAX_API_KEY")

IMAGES = [
    "../test/CleanShot_2026-02-19_at_00.07.082x.PNG",
    "../test/CleanShot_2026-02-19_at_15.46.122x.PNG",
    "../test/CleanShot_2026-02-19_at_18.36.232x.PNG",
]

async def analyze_one(img_path, task_id):
    start = time.time()
    print(f"[Task {task_id}] ▶️  開始: {os.path.basename(img_path)}")
    result = await market_report_vision.process_single_image(img_path, API_KEY)
    elapsed = time.time() - start
    
    report = result[0] if isinstance(result, tuple) else result
    first_line = (report or "").split('\n')[2] if report else "❌ 失敗"
    print(f"[Task {task_id}] ✅ 完成 ({elapsed:.1f}s): {first_line.strip()}")
    return task_id, elapsed

async def main():
    print("=" * 60)
    print(f"🚀 同時啟動 {len(IMAGES)} 個分析任務...")
    wall_start = time.time()
    
    # 模擬 bot.py 的 create_task：同時建立所有任務
    tasks = [asyncio.create_task(analyze_one(img, i+1)) for i, img in enumerate(IMAGES)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    wall_elapsed = time.time() - wall_start
    print("=" * 60)
    print(f"🏁 全部完成！總花費: {wall_elapsed:.1f}s")
    for r in results:
        if isinstance(r, Exception):
            print(f"  ❌ 錯誤: {r}")
        else:
            task_id, elapsed = r
            print(f"  Task {task_id}: {elapsed:.1f}s")
    
    # 若完全序列跑，時間應該是各任務時間總和
    # 若並發跑，總時間應該接近最慢那個任務的時間
    print(f"\n💡 若序列執行，預計需要 {sum(r[1] for r in results if not isinstance(r, Exception)):.0f}s")
    print(f"   實際並發完成: {wall_elapsed:.1f}s")

if __name__ == "__main__":
    asyncio.run(main())

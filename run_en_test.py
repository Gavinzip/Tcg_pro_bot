import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import market_report_vision

market_report_vision.REPORT_ONLY = True

IMAGES = [
    "../test/IMG_3459.jpg",
    "../test/IMG_3473.jpg",
    "../test/IMG_3490.jpg",
    "../test/IMG_5931.PNG",
    "../test/IMG_7193.JPG",
]
OUT_DIR = "../report"

async def main():
    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.getenv("MINIMAX_API_KEY")
    os.makedirs(OUT_DIR, exist_ok=True)

    tasks = [
        asyncio.create_task(
            market_report_vision.process_single_image(img, api_key, out_dir=OUT_DIR, lang="en")
        )
        for img in IMAGES
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    print("\n=== DONE ===")
    for img, res in zip(IMAGES, results):
        if isinstance(res, Exception):
            print(f"❌ {os.path.basename(img)}: {res}")
        else:
            print(f"✅ {os.path.basename(img)}")

asyncio.run(main())

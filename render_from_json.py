import os
import json
import image_generator

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def main():
    json_path = os.path.join(BASE_DIR, "report_data.json")
    if not os.path.exists(json_path):
        print("❌ report_data.json not found! Run market_report_vision.py --report_only first to generate the dataset.")
        return
        
    print(f"⏳ Loading cached data from {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    card_info = data.get('card_info', {})
    snkr_records = data.get('snkr_records', [])
    pc_records = data.get('pc_records', [])
    
    print(f"✅ Data loaded for: {card_info.get('name')} #{card_info.get('number')} ({card_info.get('grade')})")
    print(f"   - SNKRDUNK records: {len(snkr_records)}")
    print(f"   - PriceCharting records: {len(pc_records)}")
    
    print("\n🎨 Generating premium high-res Evilcharts HTML posters...")
    out_paths = image_generator.generate_report(card_info, snkr_records, pc_records, out_dir=BASE_DIR)
    
    print("\n✅ Render Complete!")
    for p in out_paths:
        print(f"   -> {p}")

if __name__ == "__main__":
    main()

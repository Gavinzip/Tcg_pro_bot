import os
import image_generator
from datetime import datetime
from PIL import Image
import random
from datetime import timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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
current_usd = 110
for i in range(10):
    d = (base_date - timedelta(days=i*1.2)).strftime('%Y-%m-%d')
    current_usd = current_usd + random.randint(-5, 8)
    pc_test.append({'date': d, 'price': current_usd, 'grade': 'PSA 10'})

template1_path = os.path.join(BASE_DIR, "templates", "report_template_1.html")
template2_path = os.path.join(BASE_DIR, "templates", "report_template_2.html")

with open(template1_path, 'r', encoding='utf-8') as f:
    html1 = f.read()
with open(template2_path, 'r', encoding='utf-8') as f:
    html2 = f.read()

name = test_data['c_name']
mh_level, mh_desc = image_generator.parse_level_and_desc(test_data['market_heat'])
cv_level, cv_desc = image_generator.parse_level_and_desc(test_data['collection_value'])
cf_level, cf_desc = image_generator.parse_level_and_desc(test_data['competitive_freq'])

card_img_b64 = image_generator.get_image_base64_from_url(test_data['img_url'])
p_prices = [r['price'] for r in pc_test]
total_entries = len(snkr_test) + len(pc_test)

replacements_1 = {
    "{{ card_name }}": name,
    "{{ card_number }}": test_data['number'],
    "{{ card_set }}": test_data['set_code'],
    "{{ grade }}": test_data['grade'],
    "{{ category }}": test_data['category'],
    "{{ market_heat_level }}": mh_level,
    "{{ market_heat_desc }}": mh_desc,
    "{{ market_heat_width }}": str(image_generator.get_width_from_level(mh_level)),
    "{{ collection_value_level }}": cv_level,
    "{{ collection_value_desc }}": cv_desc,
    "{{ collection_value_width }}": str(image_generator.get_width_from_level(cv_level)),
    "{{ competitive_freq_level }}": cf_level,
    "{{ competitive_freq_desc }}": cf_desc,
    "{{ competitive_freq_width }}": str(image_generator.get_width_from_level(cf_level)),
    "{{ features_html }}": image_generator.generate_features_html(test_data['features']),
    "{{ illustrator }}": test_data['illustrator'],
    "{{ release_info }}": test_data['release_info'],
    "{{ card_image }}": card_img_b64,
    "{{ badge_html }}": image_generator.get_badge_html(test_data['grade'])
}

for k, v in replacements_1.items():
    html1 = html1.replace(k, str(v) if v is not None else "")

replacements_2 = {
    "{{ card_name }}": name,
    "{{ card_set }}": test_data['set_code'],
    "{{ grade }}": test_data['grade'],
    "{{ pc_rows }}": image_generator.generate_table_rows(pc_test, is_jpy=False),
    "{{ snkr_rows }}": image_generator.generate_table_rows(snkr_test, is_jpy=True),
    "{{ stat_1_title }}": "PSA 10 Avg (完整品)",
    "{{ stat_1_val }}": "$85.00",
    "{{ stat_2_title }}": "Ungraded Avg (裸卡)",
    "{{ stat_2_val }}": "$36.00",
    "{{ stat_3_title }}": "Est Arb Profit (預估純利)",
    "{{ stat_3_val }}": "$14.00",
    "{{ stat_4_title }}": "Total Entries (累積筆數)",
    "{{ stat_4_val }}": "24",
    "{{ pc_chart_b64 }}": image_generator.create_premium_matplotlib_chart_b64(pc_test),
    "{{ snkr_chart_b64 }}": image_generator.create_premium_matplotlib_chart_b64(snkr_test)
}

for k, v in replacements_2.items():
    html2 = html2.replace(k, str(v) if v is not None else "")

out_html1 = os.path.join(BASE_DIR, "preview_1.html")
out_html2 = os.path.join(BASE_DIR, "preview_2.html")

with open(out_html1, 'w', encoding='utf-8') as f:
    f.write(html1)
with open(out_html2, 'w', encoding='utf-8') as f:
    f.write(html2)

print(f"✅ Generated static previews at:")
print(f"   {out_html1}")
print(f"   {out_html2}")

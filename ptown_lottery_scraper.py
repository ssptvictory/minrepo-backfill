"""
DMMぱちタウン(https://p-town.dmm.com/)のホールページから、入場ルール
(抽選方式・抽選時間)をテキストで取得する。パチ7(pachi7_lottery_scraper.py)
だけでは網羅率が約30%と低く、抽選情報は店長ごとの裁量で公開状況が
バラバラという構造的な制約があるため、2つのサイトを合算(OR)して
カバー率を上げる狙い。

robots.txt確認済み: AIクローラーの個別拒否なし(Baidu/Yandex等の
海外検索エンジンのみ拒否)。/shops/{pref}/{id} 形式のホール詳細
ページは問題なくアクセス可能。

発見方法: サイトマップインデックス(https://cdn.p-town.dmm.com/sitemap/sitemap.xml)
が示す個別ファイルは実際には同じcdn.p-town.dmm.comドメイン配下
(sitemap/ディレクトリ)にあり、https://cdn.p-town.dmm.com/sitemap/shops.xml
(全国分、1ファイル)から/shops/{saitama|tokyo}/{ID}のURLを抜き出せる。

出力: data/ptown_hall_rules.parquet
  列: shop_id, hall_name, prefecture, address, nearest_station,
      access, business_hours, entry_rule, rule_detail, numbered_ticket,
      last_checked_date
"""
import random
import re
import time
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_PATH = SCRIPT_DIR / "data" / "ptown_hall_rules.parquet"
IDS_PATH = SCRIPT_DIR / "data" / "ptown_hall_ids.json"
SHOPS_SITEMAP_URL = "https://cdn.p-town.dmm.com/sitemap/shops.xml"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

PREFECTURE_SLUGS = {"saitama": "埼玉県", "tokyo": "東京都"}
TIME_BUDGET_SECONDS = 1500

_session = requests.Session()
_start_time = time.time()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - _start_time)


def fetch(url: str, retries: int = 3) -> requests.Response:
    last_exc = None
    for attempt in range(retries):
        try:
            resp = _session.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp
        except requests.exceptions.RequestException as e:
            last_exc = e
            time.sleep(3 + attempt * 3 + random.random() * 2)
    raise last_exc


def discover_hall_ids():
    import json

    if IDS_PATH.exists():
        with open(IDS_PATH, encoding="utf-8") as f:
            return json.load(f)  # {slug: [id, id, ...]}

    resp = fetch(SHOPS_SITEMAP_URL)
    result = {}
    for slug in PREFECTURE_SLUGS:
        ids = sorted(set(re.findall(
            rf"https://p-town\.dmm\.com/shops/{slug}/(\d+)</loc>", resp.text
        )), key=int)
        result[slug] = ids
        print(f"{slug}: {len(ids)}件")

    IDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(IDS_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f)
    return result


def parse_hall_detail(html: str, slug: str):
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    hall_name = h1.get_text(strip=True) if h1 else ""

    info = {}
    for tr in soup.select("tr.tr"):
        th = tr.find("th")
        td = tr.find("td")
        if th and td:
            info[th.get_text(strip=True)] = td.get_text(" ", strip=True)

    return {
        "hall_name": hall_name,
        "prefecture": PREFECTURE_SLUGS.get(slug, slug),
        "address": info.get("住所", ""),
        "nearest_station": info.get("最寄り駅・沿線", ""),
        "access": info.get("アクセス", ""),
        "business_hours": info.get("営業時間", ""),
        "entry_rule": info.get("入場ルール", ""),
        "rule_detail": info.get("ルール詳細", ""),
        "numbered_ticket": info.get("整理券", ""),
    }


def run():
    today = date.today()
    ids_by_pref = discover_hall_ids()
    all_targets = [(slug, hid) for slug, ids in ids_by_pref.items() for hid in ids]
    print(f"\n対象ホール数: {len(all_targets)}")

    rows = []
    for i, (slug, hid) in enumerate(all_targets):
        if time_left() <= 0:
            print(f"時間切れ、{i}/{len(all_targets)}件まで処理")
            break
        try:
            resp = fetch(f"https://p-town.dmm.com/shops/{slug}/{hid}")
            data = parse_hall_detail(resp.text, slug)
            data["shop_id"] = hid
            data["last_checked_date"] = today
            rows.append(data)
            if (i + 1) % 50 == 0:
                print(f"  [{i+1}/{len(all_targets)}] 処理済み")
        except Exception as e:
            print(f"  [{slug}/{hid}] 失敗: {e}")
        time.sleep(1.0 + random.random())

    if not rows:
        print("新規データなし")
        return

    new_df = pd.DataFrame(rows)
    if OUT_PATH.exists():
        existing = pq.read_table(OUT_PATH).to_pandas()
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["shop_id"], keep="last")
    else:
        combined = new_df

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(combined, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"\n保存: {OUT_PATH.name} ({len(combined)}行)")


if __name__ == "__main__":
    run()

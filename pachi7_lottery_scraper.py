"""
パチ7(https://pachiseven.jp/)のホール詳細ページから、入場ルール
(抽選時間・再整列・入場開始時刻など)をテキストのまま取得する。

P-WORLD等は抽選情報が画像で提供されることが多いが、パチ7のホール
詳細ページ(/halls/detail/{ID})は<table class="table" summary="ホール情報">
の中にテキストで入っており、OCR等が不要で確実に取得できる。

robots.txt確認済み: ClaudeBot等のAIクローラーも含め全面的に許可
(/users/ /kanri/ 等の管理系パスのみ除外)。

発見方法:
  1. /halls/select_city.html?pref={都道府県}&area=kanto で市区町村と
     件数一覧を取得
  2. 各市区町村について /halls/list_area_result.html?pref=...&city=...
     (+ /page:N) を件数に応じてページ送りし、/halls/detail/{ID}の
     リンクを重複排除して収集
  3. 収集したID毎に詳細ページを取得し、店名(titleタグから抽出)・
     都道府県(住所の先頭から抽出)・入場ルール関連の項目を保存

抽選方法はそうそう変わらない情報のため、1〜2週間に1回程度の再取得で
十分という想定。初回は埼玉県・東京都の約900店舗を総当たりするため
時間がかかるが、一度終われば次回以降は同じID群を再チェックするだけ
でよい。

出力: data/pachi7_hall_rules.parquet
  列: hall_id, hall_name, prefecture, address, nearest_station,
      business_hours, entry_rule, rule_detail, last_checked_date
"""
import random
import re
import time
from datetime import date
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_PATH = SCRIPT_DIR / "data" / "pachi7_hall_rules.parquet"
IDS_PATH = SCRIPT_DIR / "data" / "pachi7_hall_ids.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

PREFECTURES = ["埼玉県", "東京都"]
TIME_BUDGET_SECONDS = 1500  # 1回の実行でこの秒数を超えたら中断して次回に持ち越す

PREF_NAMES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

_session = requests.Session()
_start_time = time.time()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - _start_time)


def fetch(url: str, retries: int = 3) -> requests.Response:
    """一時的なDNS失敗/接続エラーに対応するため、間隔を空けてリトライする
    (実測: 連続アクセス中にDNS解決が一時的に失敗するケースがあった)。"""
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
    """未発見なら埼玉県・東京都の全ホールIDを市区町村経由で収集する"""
    import json

    if IDS_PATH.exists():
        with open(IDS_PATH, encoding="utf-8") as f:
            return set(json.load(f))

    all_ids = set()
    for pref in PREFECTURES:
        url = f"https://pachiseven.jp/halls/select_city.html?pref={quote(pref)}&area=kanto"
        resp = fetch(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        city_links = [a for a in soup.find_all("a", href=True) if "list_area_result" in a["href"]]
        print(f"=== {pref}: 市区町村数 {len(city_links)} ===")

        for a in city_links:
            m = re.search(r"\((\d+)\)", a.get_text())
            count = int(m.group(1)) if m else 10
            n_pages = max(1, -(-count // 10))  # 切り上げ
            city_name_m = re.search(r"city=([^&]+)", a["href"])
            city_q = city_name_m.group(1) if city_name_m else ""

            for page in range(1, n_pages + 1):
                if page == 1:
                    list_url = f"https://pachiseven.jp/halls/list_area_result.html?pref={quote(pref)}&city={city_q}&area=kanto"
                else:
                    list_url = f"https://pachiseven.jp/halls/list_area_result/page:{page}?pref={quote(pref)}&city={city_q}&area=kanto"
                try:
                    r = fetch(list_url)
                    s = BeautifulSoup(r.text, "html.parser")
                    found = set()
                    for link in s.select('a[href*="/halls/detail/"]'):
                        hid_m = re.search(r"/halls/detail/(\d+)", link["href"])
                        if hid_m:
                            found.add(hid_m.group(1))
                    all_ids |= found
                except Exception as e:
                    print(f"  失敗 {list_url}: {e}")
                time.sleep(1.2 + random.random())
        print(f"  {pref} までの累計ID数: {len(all_ids)}")

    IDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    import json
    with open(IDS_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted(all_ids), f)
    return all_ids


def parse_hall_detail(html: str):
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text() if soup.title else ""
    hall_name = title.split(" - ")[0].strip() if " - " in title else title.strip()

    table = soup.find("table", class_="table", summary="ホール情報")
    info = {}
    if table:
        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                info[th.get_text(strip=True)] = td.get_text(" ", strip=True)

    address = info.get("住所", "")
    prefecture = ""
    for p in PREF_NAMES:
        if p in address:
            prefecture = p
            break

    return {
        "hall_name": hall_name,
        "prefecture": prefecture,
        "address": address,
        "nearest_station": info.get("最寄り駅", ""),
        "access": info.get("アクセス", ""),
        "business_hours": info.get("営業時間", ""),
        "entry_rule": info.get("入場ルール", ""),
        "rule_detail": info.get("ルール詳細", ""),
    }


def run():
    today = date.today()
    hall_ids = discover_hall_ids()
    print(f"\n対象ホールID数: {len(hall_ids)}")

    rows = []
    for i, hid in enumerate(sorted(hall_ids, key=int)):
        if time_left() <= 0:
            print(f"時間切れ、{i}/{len(hall_ids)}件まで処理")
            break
        try:
            resp = fetch(f"https://pachiseven.jp/halls/detail/{hid}")
            data = parse_hall_detail(resp.text)
            data["hall_id"] = hid
            data["last_checked_date"] = today
            rows.append(data)
            if (i + 1) % 50 == 0:
                print(f"  [{i+1}/{len(hall_ids)}] 処理済み")
        except Exception as e:
            print(f"  [{hid}] 失敗: {e}")
        time.sleep(1.0 + random.random())

    if not rows:
        print("新規データなし")
        return

    new_df = pd.DataFrame(rows)
    if OUT_PATH.exists():
        existing = pq.read_table(OUT_PATH).to_pandas()
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["hall_id"], keep="last")
    else:
        combined = new_df

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(combined, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"\n保存: {OUT_PATH.name} ({len(combined)}行)")


if __name__ == "__main__":
    run()

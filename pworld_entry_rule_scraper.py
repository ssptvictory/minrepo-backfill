"""
P-WORLD(https://www.p-world.co.jp/)のホール個別ページ(例:
https://www.p-world.co.jp/tokyo/das-higashijuujou.htm)から、
「朝の入場」欄のテキスト(抽選受付締切・番号公開・来店意思表明締切・
最終番号決定・整列などの時刻、WEB抽選システム名)を取得する。

このページは当初「画像のみで情報がない」と判断していたが、実際には
EUC-JP(charset=x-euc-jp)でエンコードされており、UTF-8前提で解析して
いたために本文が読めていなかっただけだった。正しくデコードすると、
<td bgcolor="#6699FF">朝の入場</td>の隣のセルに具体的な時刻が
テキストで入っている。パチ7・p-townより網羅的(P-WORLD自体が全国の
ホール台帳そのもの)なため、この3サイト合算で高いカバー率を狙う。

発見方法: /{pref}/cities/{city_code}/halls のページから各市区町村の
ホール個別ページ(/{pref}/{slug}.htm)のURLを収集する(パチ7の
市区町村巡回と同じ構造)。

出力: data/pworld_hall_rules.parquet
  列: hall_name, hall_url, morning_entry, last_checked_date
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
OUT_PATH = SCRIPT_DIR / "data" / "pworld_hall_rules.parquet"
URLS_PATH = SCRIPT_DIR / "data" / "pworld_hall_urls.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

PREFECTURE_SLUGS = ["saitama", "tokyo"]
TIME_BUDGET_SECONDS = 1500

_session = requests.Session()
_start_time = time.time()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - _start_time)


def fetch_raw(url: str, retries: int = 3) -> bytes:
    last_exc = None
    for attempt in range(retries):
        try:
            resp = _session.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.RequestException as e:
            last_exc = e
            time.sleep(3 + attempt * 3 + random.random() * 2)
    raise last_exc


def fetch_text(url: str) -> str:
    """P-WORLDはEUC-JP(charset=x-euc-jp)でエンコードされているため、
    決め打ちでeuc_jpとしてデコードする(文字化けの原因だった箇所)。"""
    raw = fetch_raw(url)
    return raw.decode("euc_jp", errors="replace")


def discover_hall_urls():
    import json

    if URLS_PATH.exists():
        with open(URLS_PATH, encoding="utf-8") as f:
            return json.load(f)

    all_urls = set()
    for pref in PREFECTURE_SLUGS:
        pref_html = fetch_text(f"https://www.p-world.co.jp/{pref}/")
        city_codes = sorted(set(re.findall(rf"/{pref}/cities/(\d+)/halls", pref_html)))
        print(f"=== {pref}: 市区町村数 {len(city_codes)} ===")
        time.sleep(1.5)

        for code in city_codes:
            try:
                html = fetch_text(f"https://www.p-world.co.jp/{pref}/cities/{code}/halls")
                urls = re.findall(rf'href="(https://www\.p-world\.co\.jp/{pref}/[a-zA-Z0-9_-]+\.htm)"', html)
                all_urls |= set(urls)
            except Exception as e:
                print(f"  失敗 {pref}/{code}: {e}")
            time.sleep(1.2 + random.random())
        print(f"  {pref} までの累計URL数: {len(all_urls)}")

    URLS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(URLS_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted(all_urls), f)
    return sorted(all_urls)


def parse_hall_page(html: str):
    soup = BeautifulSoup(html, "html.parser")
    # <title>は店名そのもの(例:"北越谷ＵＮＯ店")が入っており信頼できる。
    # <h1>は多くのページで存在しないが、一部チェーン(ニラク等)のページ
    # ではh1が複数存在し、最初のh1が「最新情報」等の無関係な見出しに
    # なっているため、h1を優先すると誤った店名を取得してしまう。
    title = soup.title.get_text(strip=True) if soup.title else ""
    hall_name = title.split("｜")[0].strip() if title and title != "P-WORLD Not Found" else ""
    if not hall_name:
        h1 = soup.find("h1")
        hall_name = h1.get_text(strip=True) if h1 else ""

    morning_entry = ""
    trs = soup.find_all("tr")
    for i, tr in enumerate(trs):
        th_like = tr.find("td", attrs={"bgcolor": "#6699FF"})
        if th_like and "入場" in th_like.get_text():
            tds = tr.find_all("td")
            if len(tds) > 1:
                morning_entry = tds[1].get_text(" ", strip=True)
            break

    return hall_name, morning_entry


def run():
    today = date.today()
    hall_urls = discover_hall_urls()
    print(f"\n対象URL数: {len(hall_urls)}")

    rows = []
    for i, url in enumerate(hall_urls):
        if time_left() <= 0:
            print(f"時間切れ、{i}/{len(hall_urls)}件まで処理")
            break
        try:
            html = fetch_text(url)
            hall_name, morning_entry = parse_hall_page(html)
            rows.append({
                "hall_name": hall_name,
                "hall_url": url,
                "morning_entry": morning_entry,
                "last_checked_date": today,
            })
            if (i + 1) % 50 == 0:
                print(f"  [{i+1}/{len(hall_urls)}] 処理済み")
        except Exception as e:
            print(f"  [{url}] 失敗: {e}")
        time.sleep(1.0 + random.random())

    if not rows:
        print("新規データなし")
        return

    new_df = pd.DataFrame(rows)
    if OUT_PATH.exists():
        existing = pq.read_table(OUT_PATH).to_pandas()
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["hall_url"], keep="last")
    else:
        combined = new_df

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(combined, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"\n保存: {OUT_PATH.name} ({len(combined)}行)")


if __name__ == "__main__":
    run()

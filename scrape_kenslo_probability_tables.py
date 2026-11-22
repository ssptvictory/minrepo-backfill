"""
kenslo65536.com(設定判別ツールサイト)から、機種ごとの設定別確率表
(BIG/REG/合算/ブドウ、設定1〜6)をスクレイピングする。

P-TOWNの同種データは画像(bonus.jpg)埋め込みでOCRが必要だったが、
このサイトは<table class="kaiseki-table">内に<td id="big_rate1">等の
安定したid付きHTMLとして確率値を持っており、OCR無しで直接パース
できる。sitemap.xmlから/hanbetsu/配下の機種ページURL一覧を取得し
(-list/-detail/-ocr/-allocation等のサブページは除外)、220機種分を
順にスクレイピングする。

出力: data/kenslo_probability_tables.parquet
  列: url, machine_name, setting(1-6), big_rate, reg_rate, br_rate, koyaku_rate
  (各rateは分母の数値。例: "1/273.1" -> 273.1)
"""
import json
import random
import re
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
SITEMAP_URL = "https://kenslo65536.com/sitemap.xml"
OUT_PATH = SCRIPT_DIR / "data" / "kenslo_probability_tables.parquet"
PROGRESS_PATH = SCRIPT_DIR / "data" / "kenslo_probability_progress.json"
REQUEST_DELAY = 2.5
EXCLUDE_SUFFIXES = ("-list", "-detail", "-ocr", "-allocation")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


def get_machine_urls():
    resp = requests.get(SITEMAP_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    urls = re.findall(r"<loc>([^<]*?/hanbetsu/[^<]*?)</loc>", resp.text)
    return sorted(set(
        u for u in urls
        if not any(u.replace(".html", "").endswith(suf) for suf in EXCLUDE_SUFFIXES)
        and u.rstrip("/").endswith(".html")
        and "/hanbetsu/sp/" not in u
    ))


def parse_rate(text: str):
    m = re.search(r"1/(\d+\.?\d*)", text)
    return float(m.group(1)) if m else None


def fetch_and_parse(url: str):
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    h1 = soup.find("h1")
    if not h1:
        return None
    machine_name = h1.get_text(strip=True).replace("設定判別ツール", "").strip().strip("　")

    table = soup.find("table", class_="kaiseki-table")
    if not table:
        return []

    rows = []
    for setting in range(1, 7):
        big = table.find("td", id=f"big_rate{setting}")
        reg = table.find("td", id=f"reg_rate{setting}")
        br = table.find("td", id=f"br_rate{setting}")
        koyaku = table.find("td", id=f"koyaku_rate{setting}")
        if big is None and reg is None:
            continue
        rows.append({
            "url": url,
            "machine_name": machine_name,
            "setting": setting,
            "big_rate": parse_rate(big.get_text()) if big else None,
            "reg_rate": parse_rate(reg.get_text()) if reg else None,
            "br_rate": parse_rate(br.get_text()) if br else None,
            "koyaku_rate": parse_rate(koyaku.get_text()) if koyaku else None,
        })
    return rows


def run():
    print("sitemapから機種ページURL一覧取得中...")
    urls = get_machine_urls()
    print(f"対象URL数: {len(urls)}")

    progress = json.load(open(PROGRESS_PATH)) if PROGRESS_PATH.exists() else {"done": {}}
    done = progress["done"]
    remaining = [u for u in urls if u not in done]
    print(f"未処理: {len(remaining)}")

    for i, url in enumerate(remaining):
        try:
            rows = fetch_and_parse(url)
            done[url] = {"ok": True, "rows": rows}
            print(f"  [{i+1}/{len(remaining)}] {url} -> {len(rows) if rows else 0}行")
        except Exception as e:
            done[url] = {"ok": False, "error": str(e)}
            print(f"  [{i+1}/{len(remaining)}] {url} 失敗: {e}")
        if (i + 1) % 20 == 0:
            json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        time.sleep(REQUEST_DELAY + random.random())

    json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    all_rows = []
    for url, info in done.items():
        if info.get("ok") and info.get("rows"):
            all_rows.extend(info["rows"])
    df = pd.DataFrame(all_rows)
    print(f"\n総行数: {len(df)}")
    print(f"機種数: {df['machine_name'].nunique() if len(df) else 0}")

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

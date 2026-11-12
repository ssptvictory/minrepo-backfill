"""
ryomemoblog.com(個人ブログ)の「{店名}のイベント日と並び人数と朝の抽選時間
について」という統一フォーマットの記事から、店ごとに一般化された
並び人数の目安(イベント日は何人〜何人、通常日は何人〜何人等)を取得する。

対象記事はsitemapから「並び人数」を含むURLとして発見する(659件、
2024-05〜2026-11、全国)。各記事の本文(entry-content)内には
「状況/入場方法/並び人数（目安）/特徴・状況」という統一された
テーブルが1つあり、そこから状況ごとの人数レンジを抽出する。

このブログは個人運営(または類似の生成コンテンツ)で、掲載されている
数字は一次情報源のない「目安」であり、信頼度は高くない前提で扱う
(ユーザー了承済み: 強い信頼度は不要、参考程度でよい)。

出力: data/ryome_crowd_sizes.parquet
  列: hall_name, situation, entry_method, crowd_text, crowd_min, crowd_max,
      note, source_url, in_core_392(halls_export.csvと一致するか)
"""
import glob
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
OUT_PATH = SCRIPT_DIR / "data" / "ryome_crowd_sizes.parquet"
PROGRESS_PATH = SCRIPT_DIR / "data" / "ryome_crowd_sizes_progress.json"
URL_LIST_PATH = SCRIPT_DIR / "data" / "ryome_article_urls.json"
REQUEST_DELAY = 3.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


def load_article_urls():
    if URL_LIST_PATH.exists():
        return json.load(open(URL_LIST_PATH, encoding="utf-8"))

    sitemap_dir = SCRIPT_DIR / "data" / "_ryome_sitemaps"
    sitemap_dir.mkdir(parents=True, exist_ok=True)
    index_resp = requests.get("https://ryomemoblog.com/sitemap.xml", headers=HEADERS, timeout=15)
    month_sitemaps = re.findall(r"<loc>(https://ryomemoblog\.com/sitemap-posttype-post\.\d+\.xml)</loc>", index_resp.text)

    urls = []
    for sm_url in month_sitemaps:
        resp = requests.get(sm_url, headers=HEADERS, timeout=15)
        urls.extend(re.findall(r"<loc>(https://ryomemoblog\.com/[^<]+)</loc>", resp.text))
        time.sleep(1.5)

    import urllib.parse
    target = [u for u in urls if "並び人数" in urllib.parse.unquote(u)]
    json.dump(target, open(URL_LIST_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return target


def parse_crowd_range(text: str):
    text = text.replace(",", "")
    m = re.search(r"(\d+)\s*[〜～\-]\s*(\d+)", text)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"(\d+)\s*[人名]?以下", text)
    if m:
        return 0, int(m.group(1))
    m = re.search(r"(\d+)\s*[人名]?(?:超|以上)", text)
    if m:
        return int(m.group(1)), None
    m = re.search(r"(\d+)\s*[人名]", text)
    if m:
        return int(m.group(1)), int(m.group(1))
    return None, None


def extract_rows(html: str):
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else ""
    hall_name = re.split(r"のイベント日|で出玉の信頼度", title)[0].strip()
    hall_name = hall_name.replace(" | メモブログ", "").strip()

    content = soup.find("div", class_="entry-content")
    if not content:
        return hall_name, []

    rows = []
    for table in content.find_all("table"):
        trs = table.find_all("tr")
        if not trs:
            continue
        header = [c.get_text(strip=True) for c in trs[0].find_all(["th", "td"])]
        if "並び人数" not in "".join(header) and "人数" not in "".join(header):
            continue
        for tr in trs[1:]:
            cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
            if len(cells) < 3:
                continue
            situation, entry_method, crowd_text = cells[0], cells[1], cells[2]
            note = cells[3] if len(cells) > 3 else ""
            cmin, cmax = parse_crowd_range(crowd_text)
            rows.append({
                "situation": situation,
                "entry_method": entry_method,
                "crowd_text": crowd_text,
                "crowd_min": cmin,
                "crowd_max": cmax,
                "note": note,
            })
    return hall_name, rows


def run():
    print("対象記事URL取得中...")
    urls = load_article_urls()
    print(f"対象記事数: {len(urls)}")

    progress = json.load(open(PROGRESS_PATH, encoding="utf-8")) if PROGRESS_PATH.exists() else {"done": {}}
    done = progress["done"]
    remaining = [u for u in urls if u not in done]
    print(f"未処理: {len(remaining)}")

    for i, url in enumerate(remaining):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.encoding = "utf-8"
            if resp.status_code != 200 or len(resp.text) < 2000:
                done[url] = {"found": False, "reason": f"status={resp.status_code} len={len(resp.text)}"}
            else:
                hall_name, rows = extract_rows(resp.text)
                done[url] = {"found": bool(rows), "hall_name": hall_name, "rows": rows}
            if (i + 1) % 15 == 0:
                print(f"  [{i+1}/{len(remaining)}] 処理済み")
                json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        except Exception as e:
            print(f"  [{url}] 失敗: {e}")
        time.sleep(REQUEST_DELAY + random.random() * 1.5)

    json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    halls_csv = pd.read_csv(SCRIPT_DIR / "halls_export.csv")
    core_names = set(halls_csv["name"]) | set(halls_csv["minrepo_name"].dropna())

    out_rows = []
    for url, info in done.items():
        if not info.get("found"):
            continue
        hall_name = info["hall_name"]
        for r in info["rows"]:
            out_rows.append({
                "hall_name": hall_name,
                "in_core_392": hall_name in core_names,
                "source_url": url,
                **r,
            })

    df = pd.DataFrame(out_rows)
    print(f"\n総行数: {len(df)}")
    if len(df):
        print(f"ユニーク店舗数: {df['hall_name'].nunique()}")
        print(f"コア392店一致: {df[df['in_core_392']]['hall_name'].nunique()}店")

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

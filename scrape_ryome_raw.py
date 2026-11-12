"""
ryomemoblog.comの「{店名}のイベント日と並び人数と朝の抽選時間について」系
記事(659件)から、生データをまず全部保存する(フェーズ1: 取得)。

正規化(並び人数のmin/max抽出等)のロジックは今後も見直しが必要になる
可能性が高い(実際、表の見出しが「並び人数」でなく「抽選・並び」だったり、
人数情報が別の列に埋め込まれているパターンが複数見つかった)。ロジックの
不具合を直すたびに全記事を再取得するのは時間とサイトへの負荷の両面で
無駄なので、各記事の関連テーブルを生の形(セルのテキストそのまま)で
保存しておき、そこから正規化する処理(normalize_ryome_crowd_sizes.py)は
完全にオフラインで何度でもやり直せるようにする。

出力: data/ryome_raw.parquet
  列: hall_name, source_url, article_text(entry-content全文),
      tables_json(関連テーブルのセルをそのまま入れ子リストで保存したJSON)
"""
import json
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
OUT_PATH = SCRIPT_DIR / "data" / "ryome_raw.parquet"
PROGRESS_PATH = SCRIPT_DIR / "data" / "ryome_raw_progress.json"
URL_LIST_PATH = SCRIPT_DIR / "data" / "ryome_article_urls.json"
REQUEST_DELAY = 3.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

# 本文テーブルのうち、関連しそうなものを広く拾うためのヒント
TABLE_HINT_RE = re.compile(r"並び|人数|抽選|規模")


def load_article_urls():
    if URL_LIST_PATH.exists():
        return json.load(open(URL_LIST_PATH, encoding="utf-8"))
    raise FileNotFoundError(f"{URL_LIST_PATH} が無い。先にsitemapからURL一覧を作る必要がある")


def extract_raw(html: str):
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else ""
    hall_name = re.split(r"のイベント日|で出玉の信頼度", title)[0].strip()
    hall_name = hall_name.replace(" | メモブログ", "").strip()

    content = soup.find("div", class_="entry-content")
    if not content:
        return hall_name, "", []

    article_text = content.get_text("\n", strip=True)

    tables = []
    for table in content.find_all("table"):
        trs = table.find_all("tr")
        if not trs:
            continue
        table_rows = [[c.get_text(strip=True) for c in tr.find_all(["th", "td"])] for tr in trs]
        flat = " ".join(cell for row in table_rows for cell in row)
        if TABLE_HINT_RE.search(flat):
            tables.append(table_rows)

    return hall_name, article_text, tables


def run():
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
                done[url] = {"ok": False, "reason": f"status={resp.status_code} len={len(resp.text)}"}
            else:
                hall_name, article_text, tables = extract_raw(resp.text)
                done[url] = {
                    "ok": True,
                    "hall_name": hall_name,
                    "article_text": article_text,
                    "tables": tables,
                }
            if (i + 1) % 15 == 0:
                print(f"  [{i+1}/{len(remaining)}] 処理済み")
                json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        except Exception as e:
            print(f"  [{url}] 失敗: {e}")
        time.sleep(REQUEST_DELAY + random.random() * 1.5)

    json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    rows = []
    for url, info in done.items():
        if not info.get("ok"):
            continue
        rows.append({
            "hall_name": info["hall_name"],
            "source_url": url,
            "article_text": info["article_text"],
            "tables_json": json.dumps(info["tables"], ensure_ascii=False),
            "checked_date": date.today(),
        })
    df = pd.DataFrame(rows)
    print(f"\n保存件数: {len(df)}")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

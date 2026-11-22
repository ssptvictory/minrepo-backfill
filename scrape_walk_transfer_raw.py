"""
wadattsu261.comの乗換ルート詳細ページから、生データ(タイトル全文+
本文全文)をまず保存する(フェーズ1: 取得)。

正規化ロジック(駅名・分数の抽出パターン)は複数の表記バリエーションが
あることが分かっており(「乗り換えにかかる時間は」と「ホームまでの
所要時間はおよそ」等)、今後も見直しが必要になる可能性が高い。そのため
生データを保存しておき、正規化(normalize_walk_transfer.py)は完全に
オフラインでやり直せるようにする。

このサイトには少なくとも2種類のページがある:
  - 【A・B駅】C線からD線への乗り換えルート: 駅名が異なる徒歩乗換
    (南越谷⇔新越谷のようなケース)
  - 【A駅】C線からD線への乗り換えルート: 同一駅内の路線間乗り換え
    (渋谷・新宿等の主要ターミナル駅のホーム間移動、64駅の個別調査に
    使える)

出力: data/walk_transfer_raw.parquet
  列: detail_url, title, article_text
"""
import random
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
LIST_PATH = SCRIPT_DIR / "data" / "walk_transfer_list_raw.parquet"
OUT_PATH = SCRIPT_DIR / "data" / "walk_transfer_raw.parquet"
PROGRESS_PATH = SCRIPT_DIR / "data" / "walk_transfer_raw_progress.json"
REQUEST_DELAY = 2.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


def fetch_and_extract(url):
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else ""
    if "お探しのページは見つかりませんでした" in title:
        return None
    content = soup.find("article") or soup.find("div", class_="entry-content") or soup.body
    text = content.get_text("\n", strip=True) if content else ""
    return title, text


def run():
    import json
    list_df = pd.read_parquet(LIST_PATH)
    detail_urls = sorted(set(u for u in list_df["detail_url"] if u))
    print(f"対象ユニークURL数: {len(detail_urls)}")

    progress = json.load(open(PROGRESS_PATH)) if PROGRESS_PATH.exists() else {"done": {}}
    done = progress["done"]
    remaining = [u for u in detail_urls if u not in done]
    print(f"未処理: {len(remaining)}")

    for i, url in enumerate(remaining):
        try:
            result = fetch_and_extract(url)
            if result is None:
                done[url] = {"ok": False, "reason": "not_found"}
            else:
                title, text = result
                done[url] = {"ok": True, "title": title, "article_text": text}
        except Exception as e:
            print(f"  [{url}] 失敗: {e}")
        if (i + 1) % 20 == 0:
            print(f"  [{i+1}/{len(remaining)}] 処理済み")
            json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        time.sleep(REQUEST_DELAY + random.random())

    json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    rows = []
    for url, info in done.items():
        if not info.get("ok"):
            continue
        rows.append({"detail_url": url, "title": info["title"], "article_text": info["article_text"]})
    df = pd.DataFrame(rows)
    print(f"\n保存件数: {len(df)}")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

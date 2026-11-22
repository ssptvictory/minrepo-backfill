"""
walk_transfer_raw.parquet(タイトル全文+本文全文)から、駅名ペア(または
同一駅内の路線ペア)と徒歩乗換分数を抽出する(フェーズ2: 正規化)。
ネットワークアクセスは行わないので、ロジックの不具合が見つかっても
何度でも無料でやり直せる。

タイトルには2パターンある:
  - 【A・B駅】...乗り換えルート/方法: 駅名が異なる徒歩乗換
    (南越谷⇔新越谷のようなケース)
  - 【A駅】...乗り換えルート/方法: 同一駅内の路線間乗り換え
    (主要ターミナル駅のホーム間移動)

本文中の「X分程度」という表現は、ページ冒頭の要約文だけでなく、個別の
手順説明(「改札を出て〜まで３分程度」等)にも何度も登場する。調査した
範囲では要約の分数が本文の中で最初に登場する(詳細な手順説明より前に
書かれている)ため、最初に出現したものを採用する。範囲表記(「2〜3分
程度」)にも対応する。

出力: data/walk_transfer_final.parquet
  列: detail_url, station_a, station_b, is_same_station,
      walk_minutes_min, walk_minutes_max, raw_match
"""
import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
RAW_PATH = SCRIPT_DIR / "data" / "walk_transfer_raw.parquet"
OUT_PATH = SCRIPT_DIR / "data" / "walk_transfer_final.parquet"

CROSS_STATION_RE = re.compile(r"【(.+?)・(.+?)駅】")
SAME_STATION_RE = re.compile(r"【(.+?)駅】")
MINUTES_RE = re.compile(r"(\d+)\s*[～〜\-]?\s*(\d+)?\s*分程度")


def parse_title(title: str):
    m = CROSS_STATION_RE.search(title)
    if m:
        return m.group(1), m.group(2), False
    m = SAME_STATION_RE.search(title)
    if m:
        return m.group(1), m.group(1), True
    return None, None, None


def parse_minutes(text: str):
    m = MINUTES_RE.search(text)
    if not m:
        return None, None
    lo = int(m.group(1))
    hi = int(m.group(2)) if m.group(2) else lo
    return lo, hi


def run():
    raw = pd.read_parquet(RAW_PATH)
    print(f"生データ記事数: {len(raw)}")

    rows = []
    no_title_match = 0
    no_minutes_match = 0
    for _, article in raw.iterrows():
        station_a, station_b, is_same = parse_title(article["title"])
        if station_a is None:
            no_title_match += 1
            continue
        lo, hi = parse_minutes(article["article_text"])
        if lo is None:
            no_minutes_match += 1
        rows.append({
            "detail_url": article["detail_url"],
            "station_a": station_a,
            "station_b": station_b,
            "is_same_station": is_same,
            "walk_minutes_min": lo,
            "walk_minutes_max": hi,
        })

    df = pd.DataFrame(rows)
    print(f"\n総行数: {len(df)}")
    print(f"タイトルから駅名を抽出できなかった件数: {no_title_match}")
    print(f"分数を抽出できなかった件数: {no_minutes_match}")
    print(f"同一駅内(路線間): {df['is_same_station'].sum()}件")
    print(f"別駅(徒歩乗換): {(~df['is_same_station']).sum()}件")
    print()
    print("=== サンプル ===")
    print(df.dropna(subset=["walk_minutes_min"]).head(10).to_string())

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"\n保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

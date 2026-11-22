"""
anoslot.moeの /queue/{YYYY-MM-DD} ページから、埼玉県・東京都の店舗の
並び人数(実測値)を取得する。1日1リクエストで全国分がまとめて
表示されるページから、都道府県見出し「東京都29店」「埼玉県17店」
などを使って対象県のテーブルだけを抽出する。

毎日の自動収集ではなく、過去分を一括で遡る一回限りのバックフィル。
2026年1月1日〜今日まで試す(初期は投稿が無い日も多いため、データが
無い日はスキップする)。

出力: data/anoslot_queue.parquet
  列: date, hall_name, prefecture, queue_count, total_diff, win_rate_text,
      in_core_392, has_interview(その日のinterview_matched.parquetとの突き合わせ)
"""
import random
import re
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_PATH = SCRIPT_DIR / "data" / "anoslot_queue.parquet"
PROGRESS_PATH = SCRIPT_DIR / "data" / "anoslot_queue_progress.json"
START_DATE = date(2026, 1, 1)
TARGET_PREFECTURES = ["東京都", "埼玉県"]
REQUEST_DELAY = 2.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


def parse_queue_page(html: str, target_date: str):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for table in soup.find_all("table"):
        heading = table.find_previous(["h1", "h2", "h3", "h4"])
        if not heading:
            continue
        heading_text = heading.get_text(strip=True)
        pref = next((p for p in TARGET_PREFECTURES if heading_text.startswith(p)), None)
        if not pref:
            continue
        for tr in table.find_all("tr")[1:]:
            cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
            if len(cells) < 4:
                continue
            hall_name, queue_text, diff_text, winrate_text = cells[:4]
            m = re.search(r"([\d,]+)", queue_text)
            queue_count = int(m.group(1).replace(",", "")) if m else None
            rows.append({
                "date": target_date,
                "hall_name": hall_name,
                "prefecture": pref,
                "queue_count": queue_count,
                "total_diff": diff_text,
                "win_rate_text": winrate_text,
            })
    return rows


def run():
    import json
    progress = json.load(open(PROGRESS_PATH)) if PROGRESS_PATH.exists() else {"done": []}
    done = set(progress["done"])

    all_dates = []
    d = START_DATE
    today = date.today()
    while d <= today:
        all_dates.append(d.isoformat())
        d += timedelta(days=1)
    remaining = [d for d in all_dates if d not in done]
    print(f"対象日数: {len(all_dates)}, 未処理: {len(remaining)}")

    all_rows = []
    for i, d_str in enumerate(remaining):
        try:
            resp = requests.get(f"https://anoslot.moe/queue/{d_str}", headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                rows = parse_queue_page(resp.text, d_str)
                all_rows.extend(rows)
                if rows:
                    print(f"  [{i+1}/{len(remaining)}] {d_str}: {len(rows)}行")
            done.add(d_str)
            if (i + 1) % 20 == 0:
                progress["done"] = sorted(done)
                json.dump(progress, open(PROGRESS_PATH, "w"), ensure_ascii=False, indent=1)
                _flush(all_rows)
                all_rows = []
        except Exception as e:
            print(f"  [{d_str}] 失敗: {e}")
        time.sleep(REQUEST_DELAY + random.random())

    progress["done"] = sorted(done)
    json.dump(progress, open(PROGRESS_PATH, "w"), ensure_ascii=False, indent=1)
    _flush(all_rows)

    _finalize()


def _flush(rows):
    if not rows:
        return
    new_df = pd.DataFrame(rows)
    if OUT_PATH.exists():
        existing = pq.read_table(OUT_PATH).to_pandas()
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "hall_name"], keep="last")
    else:
        combined = new_df
    table = pa.Table.from_pandas(combined, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"  保存: {len(combined)}行")


def _finalize():
    """コア392店フラグと取材予定フラグを付けて保存し直す"""
    if not OUT_PATH.exists():
        print("データなし")
        return
    df = pq.read_table(OUT_PATH).to_pandas()

    halls_csv = pd.read_csv(SCRIPT_DIR / "halls_export.csv")
    core_names = set(halls_csv["name"]) | set(halls_csv["minrepo_name"].dropna())
    df["in_core_392"] = df["hall_name"].isin(core_names)

    interview_path = SCRIPT_DIR / "data" / "interview_matched.parquet"
    if interview_path.exists():
        interview = pd.read_parquet(interview_path)
        interview_keys = set(zip(interview["hall_name"], interview["event_date"].astype(str)))
        df["has_interview"] = [
            (h, d) in interview_keys for h, d in zip(df["hall_name"], df["date"])
        ]
    else:
        df["has_interview"] = False

    print(f"\n総行数: {len(df)}")
    print(f"ユニーク店舗数: {df['hall_name'].nunique()}")
    print(f"コア392店一致: {df[df['in_core_392']]['hall_name'].nunique()}店")
    print(f"取材予定と重なった行数: {df['has_interview'].sum()}")

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

"""
スロパイ(https://slotana777.com/)の日付ページ(https://slotana777.com/YYYY-MM-DD)
から、その日に予定・実施されている取材・来店イベントを全国分まとめて取得する。

当初はホール単位のページ(/halls/{id})から「過去の取材履歴」「今後の
取材予定」を取ろうとしたが、「今後の取材予定」は媒体別の実績カードに
日付情報が紛れ込む形でレンダリングされており、単純なHTMLパースでは
日付→媒体の対応を安定して取り出せなかった。

一方、日付ページ(/YYYY-MM-DD)は1日1ページで全国の取材・来店が
シンプルなボタン一覧(都道府県・ホール名・タイトル)としてまとまって
おり、しかも未来の日付(今後の予定)・過去の日付(実施済み)の両方で
同じ構造が使える。ホール単位のID探索も不要になり、実装・運用コストが
大幅に下がった。

実測(2026-11-05時点):
  - 過去(11/4): 205件
  - 近い未来(11/7): 129件、(11/20): 21件
  - 遠い未来(2027-03-03, 118日後): 0件 -> このサイトの表示範囲は
    おおよそ90日程度までと見られる(どのみちP-WORLDの約25日より
    十分長いので実用上問題ない)

robots.txt確認済み: このパス(/YYYY-MM-DD)は/api/・/admin/・
/halls/*/20のいずれにも該当せず、User-Agent: *の一般許可(Allow: /)
の範囲内。

出力: data/slotana_schedule.parquet
  列: hall_name, prefecture, event_date, title, first_seen_date, last_seen_date
"""
import random
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_PATH = SCRIPT_DIR / "data" / "slotana_schedule.parquet"
PROGRESS_PATH = SCRIPT_DIR / "data" / "slotana_progress.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

FUTURE_DAYS = 30  # 今日から何日先まで取得するか(P-WORLDの約25日より少し長め)
BACKFILL_DAYS = 400  # 過去何日分まで遡って取得するか(他データと同じ約1年)
TIME_BUDGET_SECONDS = 900  # 1回の実行でこの秒数を超えたら中断して次回に持ち越す
KEY_COLS = ["hall_name", "event_date", "title"]

_session = requests.Session()
_start_time = time.time()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - _start_time)


def fetch(url: str) -> requests.Response:
    resp = _session.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp


def parse_date_page(html: str):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for btn in soup.select("button.w-full"):
        divs = btn.select("div.min-w-0.flex-1 > div")
        pref_el = btn.select_one("div.text-\\[10px\\]")
        name_el = btn.select_one("div.text-sm.font-bold")
        title_el = btn.select_one("div.text-xs")
        if not name_el:
            continue
        rows.append({
            "hall_name": name_el.get_text(strip=True),
            "prefecture": pref_el.get_text(strip=True) if pref_el else "",
            "title": title_el.get_text(strip=True) if title_el else "",
        })
    return rows


def load_progress():
    import json
    if not PROGRESS_PATH.exists():
        return {"backfilled_through": None, "future_fetched_through": None}
    with open(PROGRESS_PATH, encoding="utf-8") as f:
        return json.load(f)


def save_progress(progress: dict):
    import json
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=1)


def merge_and_save(new_rows: list, today: date):
    if not new_rows:
        print("新規データなし")
        return

    new_df = pd.DataFrame(new_rows)
    new_df["first_seen_date"] = today
    new_df["last_seen_date"] = today

    if OUT_PATH.exists():
        existing = pq.read_table(OUT_PATH).to_pandas()
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined["first_seen_date"] = combined.groupby(KEY_COLS)["first_seen_date"].transform("min")
        combined["last_seen_date"] = combined.groupby(KEY_COLS)["last_seen_date"].transform("max")
        combined = combined.drop_duplicates(subset=KEY_COLS, keep="last")
    else:
        combined = new_df

    combined = combined.sort_values(["event_date", "hall_name"])
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(combined, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name} ({len(combined)}行, {OUT_PATH.stat().st_size/1024:.1f} KB)")


def fetch_one_day(d: date):
    url = f"https://slotana777.com/{d.isoformat()}"
    resp = fetch(url)
    rows = parse_date_page(resp.text)
    for r in rows:
        r["event_date"] = d
    return rows


def run():
    today = date.today()
    progress = load_progress()
    all_rows = []

    # === フェーズ1: 未来方向(今後の予定) ===
    # 毎日新しく増える1〜2日分だけでなく、毎回FUTURE_DAYS日分を取り直すことで、
    # 後から追加/修正された予告も自然に拾える(差分管理をしない、常に全量再取得)。
    print(f"=== 未来方向: 今日から{FUTURE_DAYS}日分 ===")
    for i in range(FUTURE_DAYS + 1):
        if time_left() <= 0:
            break
        d = today + timedelta(days=i)
        try:
            rows = fetch_one_day(d)
            print(f"  {d}: {len(rows)}件")
            all_rows.extend(rows)
        except Exception as e:
            print(f"  {d}: 失敗 {e}")
        time.sleep(1.5 + random.random() * 1.5)

    # === フェーズ2: 過去方向(履歴バックフィル、backfilled_throughから続ける) ===
    backfilled_through = progress.get("backfilled_through")
    start_offset = 1
    if backfilled_through:
        last_done = date.fromisoformat(backfilled_through)
        start_offset = (today - last_done).days + 1

    print(f"=== 過去方向: バックフィル(オフセット{start_offset}から) ===")
    offset = start_offset
    while offset <= BACKFILL_DAYS:
        if time_left() <= 0:
            break
        d = today - timedelta(days=offset)
        try:
            rows = fetch_one_day(d)
            print(f"  {d}: {len(rows)}件")
            all_rows.extend(rows)
            progress["backfilled_through"] = d.isoformat()
        except Exception as e:
            print(f"  {d}: 失敗 {e}")
        offset += 1
        time.sleep(1.5 + random.random() * 1.5)

    print(f"\n今回取得した総件数: {len(all_rows)}")
    merge_and_save(all_rows, today)
    save_progress(progress)


if __name__ == "__main__":
    run()

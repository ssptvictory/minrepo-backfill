"""
P-WORLD(https://www.p-world.co.jp/)の「取材・来店情報」ページから、
都道府県単位で取材・来店の予告(スケジュール)を取得する。

min-repo.comと違い、ボット対策のチャレンジは無く、都道府県単位の
一覧ページ(ページ送り)だけで対象ホール分の予告がまとめて取れるため、
ホール単位でのアクセスは不要(リクエスト数が少なく済む=サイトへの
負荷が小さい)。

robots.txt確認済み: ClaudeBot/Claude-Userは/job/と/hall/floor_maps/
のみ拒否されており、このスクレイパーが使う/hall/interviews/配下は
許可されている。

このサイトの予告は最大で3〜4週間先まで掲載される(実測: 2026-11-05
時点で埼玉県は11/30まで、218件/5ページ)。一方でPR表記(ホールが
費用を払って掲載する広告枠)であることに注意が必要で、「取材予告が
ある=必ず好配置」という前提にはならない。is_pr列として残しておき、
分析側で特徴量として扱う。

出力: data/pworld_interviews.parquet
  列: hall_name, hall_url, event_date, interview_type, title, is_pr,
      first_seen_date, last_seen_date
  (first_seen_dateは「このスクレイパーが初めてこの予告を観測した日」
   であり、サイト側の実際の掲載開始日より遅れる可能性がある。毎日
   実行し続けることで、この遅れを数日以内に抑える想定)
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
OUT_PATH = SCRIPT_DIR / "data" / "pworld_interviews.parquet"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

# halls_export.csvのprefecture列の表記 -> P-WORLD側のURLスラッグ
PREFECTURE_SLUGS = {
    "埼玉県": "saitama",
    "東京都": "tokyo",
}

MAX_PAGES_PER_PREF = 20  # 保険の上限(実際は5ページ前後で終わる想定)
KEY_COLS = ["hall_url", "event_date", "interview_type", "title"]

_session = requests.Session()


def fetch(url: str) -> requests.Response:
    resp = _session.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp


def parse_event_date(date_text: str, today: date):
    """「11/05(木)10:00〜22:45」や「11/05(木)」から日付を取り出す。
    P-WORLDの予告には年が表記されないため、今年の日付として解釈し、
    それが今日より前(年末→年始の跨ぎ)になる場合だけ来年とみなす。"""
    m = re.match(r"(\d{1,2})/(\d{1,2})", date_text)
    if not m:
        return None
    month, day = int(m.group(1)), int(m.group(2))
    try:
        candidate = date(today.year, month, day)
    except ValueError:
        return None
    if candidate < today:
        candidate = date(today.year + 1, month, day)
    return candidate


def parse_prefecture_page(html: str, today: date):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for item in soup.select(".hallList-item"):
        name_el = item.select_one(".hallList-item-name-link") or item.select_one(".hallList-item-name")
        if not name_el:
            continue
        hall_name = name_el.get_text(strip=True)
        hall_url = name_el.get("href") or ""

        for interview in item.select(".hallList-interview"):
            date_el = interview.select_one(".hallList-interview-date")
            type_el = interview.select_one(".hallList-interview-type")
            title_el = interview.select_one(".hallList-interview-title")
            pr_el = interview.select_one(".hallList-interview-prBadge")

            if not date_el:
                continue
            event_date = parse_event_date(date_el.get_text(strip=True), today)
            if event_date is None:
                continue

            rows.append({
                "hall_name": hall_name,
                "hall_url": hall_url,
                "event_date": event_date,
                "interview_type": type_el.get_text(strip=True) if type_el else "",
                "title": title_el.get_text(strip=True) if title_el else "",
                "is_pr": bool(pr_el and pr_el.get_text(strip=True)),
            })
    return rows


def scrape_prefecture(pref_slug: str, today: date):
    all_rows = []
    for page in range(1, MAX_PAGES_PER_PREF + 1):
        url = f"https://www.p-world.co.jp/hall/interviews/prefs/{pref_slug}"
        if page > 1:
            url += f"?page={page}"
        resp = fetch(url)
        rows = parse_prefecture_page(resp.text, today)
        print(f"  [{pref_slug}] page {page}: {len(rows)}件")
        if not rows:
            break
        all_rows.extend(rows)
        time.sleep(2 + random.random() * 2)
    return all_rows


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
        # 同じ予告が既にあれば、first_seen_dateは古い方(初めて観測した日)を残し、
        # last_seen_dateは新しい方(今回観測できた=まだ掲載が続いている)で更新する
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


def run():
    today = date.today()
    all_rows = []
    for pref_name, pref_slug in PREFECTURE_SLUGS.items():
        print(f"=== {pref_name}({pref_slug}) ===")
        rows = scrape_prefecture(pref_slug, today)
        all_rows.extend(rows)

    print(f"\n今回取得した総件数: {len(all_rows)}")
    merge_and_save(all_rows, today)


if __name__ == "__main__":
    run()

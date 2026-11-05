"""
緊急対応: パーラースーパーセブン新中野店の取り漏れ分を、タグページ内の
「過去の結果」一覧テーブル(日付+URLが多数まとめて載っている)から直接
取得する。既存のbackfill_minrepo_full.pyの fetch()/challenge解決ロジックを
再利用し、1日ずつ「前日」リンクを辿る代わりに、一覧から得たURL群を
直接1件ずつ叩いて?kishu=allページを取得する。

このやり方なら、サイト側の記事欠番(今回発覚した6/25〜6/29の欠番など)で
「前日リンクが見つからない」と誤判定して停止する問題を回避できる。
"""
import re
import time
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bs4 import BeautifulSoup

import backfill_minrepo_full as b

HALL_NAME = "パーラースーパーセブン新中野店"
SCRIPT_DIR = Path(__file__).resolve().parent
UNIT_DATA_DIR = SCRIPT_DIR / "data" / "daily_unit_data"


def get_date_url_list(hall_name: str):
    """タグページの「過去の結果」テーブルから (日付文字列, URL) の一覧を取得する"""
    tag_url = f"https://min-repo.com/tag/{quote(hall_name, safe='')}/"
    resp = b.fetch(tag_url)
    soup = BeautifulSoup(resp.text, "html.parser")

    pairs = []
    for table in soup.select(".table_wrap table"):
        if "日付" not in table.get_text():
            continue
        for tr in table.select("tr"):
            a = tr.find("a", href=True)
            if not a:
                continue
            date_text = a.get_text(strip=True)
            href = a["href"]
            pairs.append((date_text, href))
    return pairs


def parse_date_from_label(date_text: str, today):
    """一覧の日付ラベルから日付を確定する。
    「10/31(土)」のように年が無い場合は今年とみなす。
    「2025/12/31(水)」のように年が明記されている場合は必ずそれを使う
    (記事タイトルからの推測に頼ると、1年以上遡った日付を誤って
    今年だと推測してしまうバグがあったため、ここでは一覧側の
    明示的な年号を信頼する)。
    """
    import re as _re
    from datetime import date as _date
    m = _re.match(r"(?:(\d{4})/)?(\d{1,2})/(\d{1,2})", date_text)
    if not m:
        return None
    year_str, month_str, day_str = m.groups()
    year = int(year_str) if year_str else today.year
    try:
        candidate = _date(year, int(month_str), int(day_str))
    except ValueError:
        return None
    if not year_str and candidate > today:
        candidate = _date(year - 1, int(month_str), int(day_str))
    return candidate


def main():
    from datetime import date as _date
    today = _date.today()

    print(f"=== {HALL_NAME}: 過去の結果一覧を取得 ===")
    pairs = get_date_url_list(HALL_NAME)
    print(f"取得できた日付×URL件数: {len(pairs)}")
    for d, u in pairs[:5]:
        print(f"  {d}: {u}")
    print("  ...")
    for d, u in pairs[-5:]:
        print(f"  {d}: {u}")

    collected = []
    for i, (date_text, url) in enumerate(pairs):
        full_url = url if url.startswith("http") else f"https://min-repo.com{url}"
        try:
            resp = b.fetch(full_url)
            html = resp.text
            if len(html) < 2000:
                print(f"  [{i+1}/{len(pairs)}] {date_text}: ベースページ異常に短い、スキップ")
                continue
            play_date = parse_date_from_label(date_text, today)
            all_url = f"{full_url.split('?')[0]}?kishu=all"
            all_resp = b.fetch(all_url, referer=full_url)
            if len(all_resp.text) < 2000:
                print(f"  [{i+1}/{len(pairs)}] {date_text}: 全台データ異常に短い、スキップ")
                continue
            unit_rows = b.parse_all_units_table(all_resp.text)
            if play_date and unit_rows:
                for r in unit_rows:
                    collected.append({
                        "hall_name": HALL_NAME,
                        "play_date": play_date,
                        "machine_name": r["machine_name"],
                        "unit_number": r["unit_number"],
                        "diff": r["diff"],
                        "games": r["games"],
                    })
                print(f"  [{i+1}/{len(pairs)}] {date_text} -> {play_date}: {len(unit_rows)}台分")
            time.sleep(1.5)
        except Exception as e:
            print(f"  [{i+1}/{len(pairs)}] {date_text}: 失敗 {e}")
            time.sleep(3)

    print(f"\n合計収集行数: {len(collected)}")
    if not collected:
        print("収集データなし、終了")
        return

    new_df = pd.DataFrame(collected)
    new_df["play_date"] = pd.to_datetime(new_df["play_date"]).dt.date
    new_df["year_month"] = new_df["play_date"].apply(lambda d: f"{d.year:04d}-{d.month:02d}")

    key_cols = ["hall_name", "play_date", "machine_name", "unit_number"]
    UNIT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    for ym, group in new_df.groupby("year_month"):
        group = group.drop(columns="year_month")
        out_path = UNIT_DATA_DIR / f"{ym}.parquet"
        if out_path.exists():
            existing = pq.read_table(out_path).to_pandas()
            existing["play_date"] = pd.to_datetime(existing["play_date"]).dt.date
            combined = pd.concat([existing, group], ignore_index=True)
        else:
            combined = group
        combined = combined.drop_duplicates(subset=key_cols, keep="last")
        combined = combined.sort_values(["hall_name", "play_date", "unit_number"])
        table = pa.Table.from_pandas(combined, preserve_index=False)
        pq.write_table(table, out_path, compression="zstd", compression_level=15)
        print(f"  {out_path.name}: {len(combined)}行 ({out_path.stat().st_size/1024:.1f} KB)")


if __name__ == "__main__":
    main()

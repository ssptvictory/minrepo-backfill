"""
min-repo.comの個別台ページ(?num=台番号)から、Aタイプ+ART機種
(machine_type_lookup.parquetでcategory=bonus/artの機種)に該当する
全台のBB/REG実績(当日+過去10日分)を取得する。

目的: kenslo65536.comから取得済みの設定別確率表(BIG/REG確率)と
照らし合わせ、実際の出目からどの設定だった可能性が高いかを尤度推定
するため。

対象は約4万台(376ホール)あり、1回の起動では終わらないため、
backfill_minrepo_full.pyと同じ「ホール単位で進捗保存、TIME_BUDGET_
SECONDS超過で中断・次回再開」方式にする。各台ページには過去10日分の
履歴が載っているため、1台につき週1回程度の頻度での再取得で十分
(日次更新は不要)。

処理の流れ:
  1. 対象ホールごとに、タグページ(get_date_url_list)で最新記事URLを
     解決する(ホールごとに1回、進捗に保存してキャッシュする)
  2. 最新記事URL + "?num=<台番号>" で個別台ページを取得し、
     当日表+過去10日間表のBB/RB/合算/差枚/G数をパースする

出力:
  data/unit_bonus_shards/shard_<run_id>.parquet
    列: hall_name, machine_name, unit_number, play_date, diff, games,
        bb, rb, out_rate
  data/unit_bonus_progress.json
    ホールごとの最新記事URL解決状況、台ごとの最終取得日時
"""
import glob
import json
import os
import random
import re
import time
from datetime import date
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bs4 import BeautifulSoup

# min-repo.com向けの、セッション維持+Bot対策JSチャレンジ解決込みのfetch()を
# backfill_minrepo_full.pyから再利用する(同じサイト向けに既に動作確認済みの
# ロジックのため、重複実装しない)。
from backfill_minrepo_full import fetch

SCRIPT_DIR = Path(__file__).resolve().parent
SHARDS_DIR = SCRIPT_DIR / "data" / "unit_bonus_shards"
PROGRESS_PATH = SCRIPT_DIR / "data" / "unit_bonus_progress.json"
RUN_ID = os.environ.get("GITHUB_RUN_ID") or f"local{int(time.time())}"

TIME_BUDGET_SECONDS = int(os.environ.get("UNIT_BONUS_TIME_BUDGET", "600"))
REFRESH_INTERVAL_DAYS = 7  # 1台あたりこの日数以上経っていたら再取得対象にする
REQUEST_DELAY = 3.0
MAX_UNITS_PER_HALL_PER_RUN = 3  # 1回の実行で1ホールにつき処理する台数の上限
MAX_CONSECUTIVE_EMPTY = 2  # 空レスポンスがこの回数連続したらそのホールを切り上げる

_start_time = time.time()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - _start_time)


def get_latest_article_url(hall_name: str):
    tag_url = f"https://min-repo.com/tag/{quote(hall_name, safe='')}/"
    resp = fetch(tag_url)
    if len(resp.text) < 2000:
        return False
    if resp.url.rstrip("/") == "https://min-repo.com":
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    for table in soup.select(".table_wrap table"):
        if "日付" not in table.get_text():
            continue
        a = table.select_one("tr a[href]")
        if a:
            href = a["href"]
            return href if href.startswith("http") else f"https://min-repo.com{href}"
    return None


def parse_date_label(label: str):
    """「11/16」や「11/15(日)」のようなラベルを実際の日付に変換する。
    年の明記はないため今年とみなし、未来日になる場合は去年とみなす
    (backfill_minrepo_full.pyのparse_date_from_labelと同じ考え方)。
    """
    m = re.match(r"(\d{1,2})/(\d{1,2})", label)
    if not m:
        return None
    today = date.today()
    month, day = int(m.group(1)), int(m.group(2))
    try:
        candidate = date(today.year, month, day)
    except ValueError:
        return None
    if candidate > today:
        candidate = date(today.year - 1, month, day)
    return candidate.isoformat()


def parse_number(text: str):
    text = text.strip().replace(",", "")
    if text in ("", "-", "ー"):
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_unit_page(html: str):
    """個別台ページ(?num=台番号)から当日+過去10日分の実績を取り出す。

    実際のテーブル構成(4つ):
      1. class="kishu": 機種/差枚/G数/出率(当日、1行)
      2. (無印): BB/RB/合成/BB率/RB率(当日、ヘッダ行+値1行、日付列なし)
      3. (無印): 日付/差枚/G数/出率/BB/RB/合成/BB率/RB率(過去10日間、
         当日を含まない。1行目が「昨日」)
      4. class="halllist": 無関係(他店舗の並び情報)なので無視

    当日の日付は<title>タグの "11/16(月) ホール名 | ..." から取る。
    """
    soup = BeautifulSoup(html, "html.parser")
    rows_out = []

    title = soup.find("title")
    today_label = None
    if title:
        m = re.search(r"(\d{1,2}/\d{1,2})\([日月火水木金土]\)", title.get_text())
        if m:
            today_label = m.group(1)

    today_diff = today_games = today_out = None
    kishu_table = soup.select_one("table.kishu")
    if kishu_table:
        tds = kishu_table.select("tr td")
        if len(tds) >= 4:
            # tds[0]は機種名、[1]差枚、[2]G数、[3]出率
            today_diff = parse_number(tds[1].get_text(strip=True))
            today_games = parse_number(tds[2].get_text(strip=True))

    today_bb = today_rb = None
    history_rows = []
    for table in soup.select("table"):
        if table.get("class") == ["kishu"] or table.get("class") == ["halllist"]:
            continue
        header_cells = [th.get_text(strip=True) for th in table.select("tr th")]
        if "BB" not in header_cells:
            continue
        is_history = "日付" in header_cells
        for tr in table.select("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            if is_history:
                if len(tds) < 6:
                    continue
                date_text = tds[0].get_text(strip=True)
                diff_text, games_text = tds[1].get_text(strip=True), tds[2].get_text(strip=True)
                bb_text, rb_text = tds[4].get_text(strip=True), tds[5].get_text(strip=True)
                history_rows.append({
                    "date_label": date_text,
                    "diff": parse_number(diff_text),
                    "games": parse_number(games_text),
                    "bb": parse_number(bb_text),
                    "rb": parse_number(rb_text),
                })
            else:
                if len(tds) < 2:
                    continue
                today_bb = parse_number(tds[0].get_text(strip=True))
                today_rb = parse_number(tds[1].get_text(strip=True))

    if today_label and (today_diff is not None or today_bb is not None):
        rows_out.append({
            "date_label": today_label, "diff": today_diff, "games": today_games,
            "bb": today_bb, "rb": today_rb,
        })
    rows_out.extend(history_rows)
    return rows_out


def load_progress():
    if PROGRESS_PATH.exists():
        return json.load(open(PROGRESS_PATH))
    return {"hall_article_url": {}, "unit_last_fetched": {}}


def save_progress(progress):
    json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


def load_target_units():
    lookup_path = SCRIPT_DIR / "data" / "machine_type_lookup.parquet"
    if not lookup_path.exists():
        raise SystemExit("data/machine_type_lookup.parquet が見つかりません。先に配置してください。")
    lookup = pd.read_parquet(lookup_path)
    target_names = set(lookup[lookup["category"].isin(["bonus", "art"])]["machine_name"])

    dfs = [pd.read_parquet(f) for f in glob.glob(str(SCRIPT_DIR / "data" / "daily_unit_data" / "2026-1*.parquet"))]
    df = pd.concat(dfs, ignore_index=True)
    df = df[df["machine_name"].isin(target_names)]
    return df[["hall_name", "machine_name", "unit_number"]].drop_duplicates()


FLUSH_INTERVAL_SECONDS = 60


def flush(progress, hall_article_url, unit_last_fetched, collected_rows, shard_path):
    """長時間(ローカルで数時間)実行する場合、途中でクラッシュ/中断しても
    その時点までの成果が失われないよう、定期的に進捗とデータを書き出す。"""
    progress["hall_article_url"] = hall_article_url
    progress["unit_last_fetched"] = unit_last_fetched
    save_progress(progress)
    if collected_rows:
        df_out = pd.DataFrame(collected_rows)
        table = pa.Table.from_pandas(df_out, preserve_index=False)
        pq.write_table(table, shard_path, compression="zstd", compression_level=15)


def run():
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)
    print("対象(ホール,機種,台番号)読み込み中...")
    targets = load_target_units()
    print(f"対象件数: {len(targets)}")

    progress = load_progress()
    hall_article_url = progress["hall_article_url"]
    unit_last_fetched = progress["unit_last_fetched"]

    today_str = date.today().isoformat()
    collected_rows = []
    shard_path = SHARDS_DIR / f"shard_{RUN_ID}.parquet"
    last_flush = time.time()

    halls = targets["hall_name"].unique().tolist()
    random.shuffle(halls)  # 特定ホールに偏らないようにする

    for hall_name in halls:
        if time_left() <= 0:
            break
        hall_units = targets[targets["hall_name"] == hall_name]

        article_url = hall_article_url.get(hall_name)
        if article_url is None:
            try:
                resolved = get_latest_article_url(hall_name)
            except Exception as e:
                print(f"! [{hall_name}] タグページ取得失敗: {e}")
                continue
            if resolved is False:
                continue  # 一時的失敗、次回再試行
            if resolved is None:
                hall_article_url[hall_name] = "NOT_FOUND"
                continue
            article_url = resolved
            hall_article_url[hall_name] = article_url
            time.sleep(REQUEST_DELAY + random.random())
        if article_url == "NOT_FOUND":
            continue

        units_to_fetch = []
        for _, row in hall_units.iterrows():
            key = f"{hall_name}|{row['unit_number']}"
            last_fetched = unit_last_fetched.get(key)
            if last_fetched:
                days_since = (date.today() - date.fromisoformat(last_fetched)).days
                if days_since < REFRESH_INTERVAL_DAYS:
                    continue
            units_to_fetch.append(row)
        if not units_to_fetch:
            continue
        # 1ホールへの連続アクセスが続くと、このサイトのレート制限は数分単位で
        # 回復しないことが分かっている(backfill_minrepo_full.pyの既知の挙動と
        # 同じ)。そのため1回の巡回では少数の台だけ処理し、残りは次回以降の
        # 実行(GitHub Actionsは10分おきなのでその頃には回復している想定)に
        # 回す。これにより多くのホールを薄く回すことになる。
        units_to_fetch = units_to_fetch[:MAX_UNITS_PER_HALL_PER_RUN]

        # ?num=ページはベース記事ページのBot対策チャレンジを解決済みでないと
        # 空レスポンスになるため、このホールの台を処理する前に一度ウォームアップする
        try:
            fetch(article_url)
        except Exception as e:
            print(f"! [{hall_name}] ベースページ取得失敗: {e}")
            continue
        time.sleep(REQUEST_DELAY + random.random())

        consecutive_empty = 0
        for row in units_to_fetch:
            if time_left() <= 0:
                break
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                print(f"  [{hall_name}] 空レスポンスが連続({consecutive_empty}回)のため、このホールを切り上げ次へ")
                break
            key = f"{hall_name}|{row['unit_number']}"
            url = f"{article_url.split('?')[0]}?num={row['unit_number']}"
            try:
                resp = fetch(url, referer=article_url)
                parsed = parse_unit_page(resp.text)
                consecutive_empty = 0 if parsed else consecutive_empty + 1
                for p in parsed:
                    play_date = parse_date_label(p.pop("date_label"))
                    if play_date is None:
                        continue
                    collected_rows.append({
                        "hall_name": hall_name,
                        "machine_name": row["machine_name"],
                        "unit_number": row["unit_number"],
                        "play_date": play_date,
                        **p,
                    })
                if parsed:
                    unit_last_fetched[key] = today_str
                else:
                    # 空レスポンス(レート制限の疑い)はdone扱いにせず、次回すぐ再試行できるようにする
                    unit_last_fetched.pop(key, None)
                print(f"  [{hall_name}] 台{row['unit_number']} {row['machine_name'][:20]} -> {len(parsed)}日分")
            except Exception as e:
                print(f"  ! [{hall_name}] 台{row['unit_number']} 失敗: {e}")
            time.sleep(REQUEST_DELAY + random.random())

            if time.time() - last_flush >= FLUSH_INTERVAL_SECONDS:
                flush(progress, hall_article_url, unit_last_fetched, collected_rows, shard_path)
                last_flush = time.time()
                print(f"  [定期保存] 累計{len(collected_rows)}行, 台{len(unit_last_fetched)}件処理済み")

    flush(progress, hall_article_url, unit_last_fetched, collected_rows, shard_path)
    print(f"\n保存: {shard_path.name} ({len(collected_rows)}行)" if collected_rows else "\n今回収集した行なし")
    print(f"累計取得済み台数: {len(unit_last_fetched)} / {len(targets)}")


if __name__ == "__main__":
    run()

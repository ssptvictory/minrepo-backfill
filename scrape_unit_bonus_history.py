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

処理の流れ(2026-11-28 改修、詳細は run() の上のコメント):
  1. 店ごとの最新記事URLは backfill_minrepo_full.py が backfill_progress.json に残したものを使う
     (以前は最初に見つけた記事をずっと使っていたため、新しい日が取れなくなっていた)
  2. 最新記事URL + "?num=<台番号>" で個別台ページを取得し、
     当日表+過去10日間表のBB/RB/合算/差枚/G数をパースする。1台あたり約9日に1回
  - backfill.yml の中で backfill_minrepo_full.py の後に続けて動く(同時に2本アクセスしないため)

出力:
  data/unit_bonus_shards/shard_<run_id>.parquet
    列: hall_name, machine_name, unit_number, play_date, diff, games,
        bb, rb, slump_graph(当日分のみ、JSON文字列化した日中差枚推移の
        配列。過去日分はNone。末尾の値がdiffとほぼ一致することを確認済み)
  data/unit_bonus_progress.json
    ホールごとの最新記事URL解決状況、台ごとの最終取得日時
"""
import glob
import json
import os
import random
import re
import time
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bs4 import BeautifulSoup

# min-repo.com向けの、セッション維持+Bot対策JSチャレンジ解決込みのfetch()を
# backfill_minrepo_full.pyから再利用する(同じサイト向けに既に動作確認済みの
# ロジックのため、重複実装しない)。
from backfill_minrepo_full import SiteUnavailable, fetch

SCRIPT_DIR = Path(__file__).resolve().parent
SHARDS_DIR = SCRIPT_DIR / "data" / "unit_bonus_shards"
PROGRESS_PATH = SCRIPT_DIR / "data" / "unit_bonus_progress.json"
RUN_ID = os.environ.get("GITHUB_RUN_ID") or f"local{int(time.time())}"

TIME_BUDGET_SECONDS = int(os.environ.get("UNIT_BONUS_TIME_BUDGET", "600"))
UNIT_REQUEST_DELAY = 0.5  # 同一ホール内で台を連続取得する間の間隔(短め。検証の結果、
# ウォームアップ直後や長い間隔を挟むと空レスポンスになる現象を確認したため)
MAX_CONSECUTIVE_EMPTY = 2  # 空レスポンスがこの回数連続したらそのホールを切り上げる(保険)

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

    # スランプグラフ(Chart.jsのdata配列)は当日の日中の差枚推移そのもの
    # (末尾の値が当日diffとほぼ一致することを確認済み)。過去日分は
    # このページには無く、当日分のみ取得できる。
    slump_graph = None
    m = re.search(r"data:\s*\[([^\]]+)\]\s*,\s*fill", html)
    if m:
        try:
            slump_graph = [int(x.strip()) for x in m.group(1).split(",") if x.strip()]
        except ValueError:
            slump_graph = None

    if today_label and (today_diff is not None or today_bb is not None):
        rows_out.append({
            "date_label": today_label, "diff": today_diff, "games": today_games,
            "bb": today_bb, "rb": today_rb,
            "slump_graph": json.dumps(slump_graph) if slump_graph else None,
        })
    for h in history_rows:
        h["slump_graph"] = None
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
    # 台入替えがあった台は(ホール,機種,台番号)で重複除去すると旧機種・新機種の
    # 2行になり、同じ台日を別の機種名で二重保存していた(2026-11-24判明、
    # BB/RB 333,061行中114,445行)。台ごとに最新日の機種だけを対象にする。
    df = df.sort_values("play_date").drop_duplicates(["hall_name", "unit_number"], keep="last")
    df = df[df["machine_name"].isin(target_names)]
    df = df.merge(lookup[["machine_name", "category"]].drop_duplicates("machine_name"), on="machine_name", how="left")
    return df[["hall_name", "machine_name", "unit_number", "category"]]


# スランプグラフはAT機(category="at")の設定判別用にだけ使う(2026-11-24決定)。
# BB/RBで判別できるAタイプ・ART機では保存しない。
SLUMP_CATEGORIES = {"at"}


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


# --- 2026-11-28 改修: みんレポの負担を減らす ---
# 以前は店ごとに最初に見つけた記事をずっと使い(新しい日が取れない)、0.5秒間隔で全台を取っていた
# (11/26〜27の2日で約2万7千ページ)。今は:
#   - 店ごとの最新の記事を使う(backfill_minrepo_full.py が一覧ページを見たときに backfill_progress.json に残す)。
#     まだ最新の記事が分からない店は飛ばす(一覧ページをこちらで取り直さない)
#   - 1台のページには記事の日+過去10日分が載っているので、前回取った記事の日から REFRESH_DAYS 以上
#     新しい記事が出ている台だけ取る(=1台あたり約9日に1回)
#   - 1つの店で一度に取るのは MAX_UNITS_PER_VISIT 台まで、店と店の間は HALL_PAUSE 秒以上空ける
#   - 本文の無いページが続く店が続いたら、その回はやめる(fetch側でも空ページ6回連続でサイト全体を休止)
#   - アクセス数の上限(1日/1時間)は fetch() 側で全スクリプト合計で数える
REFRESH_DAYS = 9
MAX_UNITS_PER_VISIT = 30
HALL_PAUSE = 15.0
MAX_DEAD_HALLS = 4  # 本文の無いページで打ち切った店がこの数続いたら、その回はやめる
BACKFILL_PROGRESS_PATH = SCRIPT_DIR / "data" / "backfill_progress.json"


def latest_articles():
    """backfill_minrepo_full.py が記録した店ごとの最新記事 {店名: (日付, URL)}"""
    try:
        bp = json.load(open(BACKFILL_PROGRESS_PATH, encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    out = {}
    for hall, s in bp.items():
        a = (s or {}).get("latest_article") if isinstance(s, dict) else None
        if a and a.get("url") and a.get("date"):
            out[hall] = (date.fromisoformat(a["date"]), a["url"])
    return out


def run():
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)
    print("対象(ホール,機種,台番号)読み込み中...")
    targets = load_target_units()
    print(f"対象件数: {len(targets)}")

    progress = load_progress()
    hall_article_url = progress["hall_article_url"]
    unit_last_fetched = progress["unit_last_fetched"]
    # 値の意味を「取った日」から「取れたデータの一番新しい日」に変えた(2026-11-28)。
    # 古い値は取った日で、古い記事を見ていた台は実際のデータがもっと前で止まっているため、保存済みのBB/RBから求め直す
    if not progress.get("values_are_article_dates"):
        newest = {}
        for f in glob.glob(str(SHARDS_DIR / "*.parquet")):
            d = pd.read_parquet(f, columns=["hall_name", "unit_number", "play_date"])
            for (h, n), v in d.groupby(["hall_name", "unit_number"]).play_date.max().items():
                k = f"{h}|{n}"
                newest[k] = max(newest.get(k, ""), str(v)[:10])
        unit_last_fetched.clear()
        unit_last_fetched.update(newest)
        progress["values_are_article_dates"] = True
        print(f"台ごとの取得済みの日を保存済みデータから求め直した: {len(newest)}台")

    articles = latest_articles()
    collected_rows = []
    shard_path = SHARDS_DIR / f"shard_{RUN_ID}.parquet"
    last_flush = time.time()
    n_pages = n_units_ok = 0
    no_article = 0
    dead_halls = 0

    # データが一番古い台を持つ店から取る(上限の中で取りきれない日があっても、抜けができにくいように)
    oldest = {}
    for h, n in zip(targets["hall_name"], targets["unit_number"]):
        oldest[h] = min(oldest.get(h, "9999"), unit_last_fetched.get(f"{h}|{n}", "0000"))
    halls = sorted(oldest, key=lambda h: (oldest[h], random.random()))

    try:
        for hall_name in halls:
            if time_left() <= 0:
                break
            if hall_name not in articles:
                no_article += 1
                continue
            art_date, article_url = articles[hall_name]
            hall_units = targets[targets["hall_name"] == hall_name]

            units_to_fetch = []
            for _, row in hall_units.iterrows():
                covered = unit_last_fetched.get(f"{hall_name}|{row['unit_number']}")
                if covered and (art_date - date.fromisoformat(covered)).days < REFRESH_DAYS:
                    continue
                units_to_fetch.append(row)
            if not units_to_fetch:
                continue
            units_to_fetch = units_to_fetch[:MAX_UNITS_PER_VISIT]
            hall_article_url[hall_name] = article_url

            time.sleep(HALL_PAUSE + random.random() * 5)
            # ウォームアップ(記事ページ)の直後は間を空けずに台ページを取る(2026-11-17の検証。空けると空ページになる)
            try:
                fetch(article_url)
                n_pages += 1
            except Exception as e:
                print(f"! [{hall_name}] 記事ページ取得失敗: {e}")
                continue

            consecutive_empty = got = 0
            for row in units_to_fetch:
                if time_left() <= 0:
                    break
                if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                    print(f"  [{hall_name}] 中身の無いページが{consecutive_empty}回続いたので、この店は次回に回す")
                    break
                key = f"{hall_name}|{row['unit_number']}"
                url = f"{article_url.split('?')[0]}?num={row['unit_number']}"
                try:
                    resp = fetch(url, referer=article_url, retries=1)  # 空でも取り直さない
                    n_pages += 1
                    parsed = parse_unit_page(resp.text)
                except Exception as e:
                    print(f"  ! [{hall_name}] 台{row['unit_number']} 失敗: {e}")
                    parsed = []
                consecutive_empty = 0 if parsed else consecutive_empty + 1
                for p in parsed:
                    play_date = parse_date_label(p.pop("date_label"))
                    if play_date is None:
                        continue
                    if row.get("category") not in SLUMP_CATEGORIES:
                        p["slump_graph"] = None
                    collected_rows.append({"hall_name": hall_name, "machine_name": row["machine_name"],
                                           "unit_number": row["unit_number"], "play_date": play_date, **p})
                if parsed:
                    unit_last_fetched[key] = art_date.isoformat()
                    got += 1
                time.sleep(UNIT_REQUEST_DELAY)

                if time.time() - last_flush >= FLUSH_INTERVAL_SECONDS:
                    flush(progress, hall_article_url, unit_last_fetched, collected_rows, shard_path)
                    last_flush = time.time()
            n_units_ok += got
            print(f"  [{hall_name}] 記事{art_date}: {got}/{len(units_to_fetch)}台")
            dead_halls = dead_halls + 1 if got == 0 else 0
            if dead_halls >= MAX_DEAD_HALLS:
                print(f"! 中身の無いページで打ち切った店が{dead_halls}店続いたため、今回はここまで")
                break
    except SiteUnavailable as e:
        print(f"! {e}")

    flush(progress, hall_article_url, unit_last_fetched, collected_rows, shard_path)
    print(f"\n今回: {n_pages}ページ取得、{n_units_ok}台分 / {len(collected_rows)}行"
          f" / 最新記事がまだ分からず飛ばした店 {no_article}")
    print(f"累計取得済み台数: {len(unit_last_fetched)} / {len(targets)}")


if __name__ == "__main__":
    run()

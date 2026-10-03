"""
本番用: halls_export.csv に載っている全店舗(392店舗)について、
過去1年分の台番号別データ(機種名・差枚・G数)をmin-repo.comから取得し、
リポジトリ内のファイル(data/配下)に保存する。

外部DB(Supabase/Turso等)には依存せず、進捗もデータもすべてこのリポジトリに
コミットする形で永続化する。GitHub Actions側がこのスクリプト実行後に
data/配下の変更をコミット・プッシュする想定(ワークフローYAML参照)。

出力ファイル:
  data/backfill_progress.json          店舗ごとの進捗状態
  data/daily_unit_data/YYYY-MM.parquet 月ごとの台データ(zstd圧縮、列指向)
    列: hall_name, play_date, machine_name, unit_number, diff, games
    (出率は games*3 と diff から復元できるため保存しない。実データで
     誤差ゼロを確認済み: out_rate = round((games*3+diff)/(games*3)*100, 1))

パイロット検証(pilot_backfill_minrepo.py)で確認した以下の対策を組み込んでいる:
  - Bot対策のJSチャレンジ2種への対応
    (admin-ajax.phpにnonce POSTするタイプ / レスポンス中のJSに直接
     $.cookie('_d2', ...) と書かれているタイプ)
  - ?kishu=allページ(全台データ一覧)を使い、1日1店舗あたり2リクエストに抑える
  - レート制限(空レスポンス)を検知したらセッションを作り直してリトライする

392店舗×365日は現在のペース(丁寧な間隔)だと合計で数ヶ月かかる規模のため、
1回の起動ではTIME_BUDGET_SECONDS秒だけ処理して進捗をdata/backfill_progress.json
に保存し、続きは次回の起動時に自動で再開する。
GitHub Actionsで繰り返し起動し、全店舗が完了するまで少しずつ進める想定。

実行方法:
    python3 backfill_minrepo_full.py

二重起動防止は、GitHub Actions側のconcurrency設定(ワークフローYAML)で
行う想定のため、このスクリプト自体にはロック機構を持たせていない。
"""
import csv
import json
import random
import re
import time
from collections import defaultdict
from datetime import date
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
HALLS_CSV = SCRIPT_DIR / "halls_export.csv"
DATA_DIR = SCRIPT_DIR / "data"
UNIT_DATA_DIR = DATA_DIR / "daily_unit_data"
PROGRESS_PATH = DATA_DIR / "backfill_progress.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

DAYS_BACK = 365
TIME_BUDGET_SECONDS = 600  # 1回の起動でこの秒数を超えたら中断して次回に持ち越す
HALL_TIME_BUDGET_SECONDS = 30  # 1店舗にこの秒数以上かけたら見切りをつけて次の店舗に進む
# (実測で判明した問題への対策: ?kishu=allページは1IPあたり非常に厳しいレート制限が
#  あり、数日〜数十日分連続で失敗することがある。この上限がないと、リストの手前に
#  ある詰まった店舗がTIME_BUDGET_SECONDS全体を使い切ってしまい、後続の店舗が
#  何時間経っても一度も処理されない事態が実際に発生した。)

_session = requests.Session()
_start_time = time.time()

# このプロセス内で新たに取得した台データを溜めておき、最後にまとめて
# Parquetファイルへマージ書き込みする(1行ごとのI/Oを避けるため)。
_collected_rows = []


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - _start_time)


def _ensure_challenge_solved(resp: requests.Response):
    """チャレンジページ(JSリダイレクト)が返ってきていたら突破してCookieを取得する。
    このサイトのチャレンジには2パターンある:
      1. admin-ajax.phpにnonce付きPOSTしてCookieを発行してもらうタイプ
         (記事ページ等で発生)
      2. レスポンス中のJSに直接 $.cookie('_d2', '値', ...) と書かれていて、
         それをそのままセットしてリロードさせるタイプ(タグ一覧ページ等で発生)
    """
    m = re.search(r"action=w_scd_n&_ajax_nonce=([a-f0-9]+)", resp.text)
    if m:
        _session.post(
            "https://min-repo.com/wp-admin/admin-ajax.php",
            headers=HEADERS,
            data={"action": "w_scd_n", "_ajax_nonce": m.group(1)},
            timeout=15,
        )
        return True

    m = re.search(r"\$\.cookie\(\s*'_d2'\s*,\s*'([^']+)'", resp.text)
    if m:
        _session.cookies.set("_d2", m.group(1), domain=".min-repo.com", path="/")
        return True

    return False


def fetch(url: str, referer: str = None, retries: int = 3) -> requests.Response:
    """Bot対策のJSチャレンジを解決しつつページを取得する。

    このサイトは時間経過で回復するタイプのレート制限があるらしく、
    短時間にアクセスしすぎると本文の代わりに空レスポンス(200, 0バイト)が
    返ってくることがある。その場合はセッション(Cookie)を作り直して
    間隔を空けてリトライする。

    retriesは以前6だったが、実測で「?kishu=allは同一IPから連続アクセスすると
    数分単位では回復しない」ケースが確認できたため、深追いせず早めに見切りを
    つけて呼び出し元(process_hall)に判断を戻す値に減らしてある。
    """
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer
    resp = None
    for attempt in range(retries):
        resp = _session.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        if _ensure_challenge_solved(resp):
            resp = _session.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            resp.encoding = "utf-8"
        if len(resp.text) >= 2000:
            return resp
        _session.cookies.clear()
        time.sleep(6 + attempt * 6 + random.random() * 3)
    return resp


def load_halls():
    with open(HALLS_CSV, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return [(r["minrepo_name"].strip() or r["name"].strip()) for r in rows]


def load_progress():
    """data/backfill_progress.json から全店舗分の進捗を読み込む"""
    if not PROGRESS_PATH.exists():
        return {}
    with open(PROGRESS_PATH, encoding="utf-8") as f:
        return json.load(f)


def save_progress(progress: dict):
    """進捗全体をdata/backfill_progress.jsonへ書き出す(実行の最後に1回呼ぶ)"""
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=1, sort_keys=True)


def queue_unit_data(hall_name, play_date, unit_rows):
    """取得した台データをメモリ上に溜める(実際のファイル書き込みはflush_unit_dataで行う)"""
    for r in unit_rows:
        _collected_rows.append({
            "hall_name": hall_name,
            "play_date": play_date,
            "machine_name": r["machine_name"],
            "unit_number": r["unit_number"],
            "diff": r["diff"],
            "games": r["games"],
        })


def flush_unit_data():
    """_collected_rows を月ごとのParquetファイルへマージ書き込みする。
    既存ファイルがあれば読み込んで結合し、(hall_name, play_date, machine_name,
    unit_number)で重複排除してから書き戻す(再実行しても安全なようにupsert的に扱う)。
    """
    if not _collected_rows:
        print("新規データなし(flush_unit_data: スキップ)")
        return

    UNIT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(_collected_rows)
    new_df["play_date"] = pd.to_datetime(new_df["play_date"]).dt.date
    new_df["year_month"] = new_df["play_date"].apply(lambda d: f"{d.year:04d}-{d.month:02d}")

    key_cols = ["hall_name", "play_date", "machine_name", "unit_number"]
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


def get_latest_post_url(hall_name: str):
    """タグページから最新の投稿URLを取得"""
    tag_url = f"https://min-repo.com/tag/{quote(hall_name, safe='')}/"
    resp = fetch(tag_url)
    soup = BeautifulSoup(resp.text, "html.parser")

    first = soup.select_one(".ichiran_title a") or soup.select_one("article h1 a")
    if first and first.get("href"):
        return first["href"]

    for a in soup.select("a[href]"):
        href = a["href"]
        if re.match(r"https://min-repo\.com/\d+/?$", href):
            return href
    return None


def parse_date_from_title(html: str):
    """h1見出し(例: "9/18(金) キューデン・アネックス")から日付を推定する。
    年をまたぐ可能性があるため、現在日付を基準に妥当な年を選ぶ。
    """
    m = re.search(r"(\d{1,2})/(\d{1,2})\(", html)
    if not m:
        return None
    month, day = int(m.group(1)), int(m.group(2))
    today = date.today()
    year = today.year
    try:
        candidate = date(year, month, day)
        if candidate > today:
            candidate = date(year - 1, month, day)
        return candidate
    except ValueError:
        return None


def get_prev_day_url(html: str):
    """「前日」リンクのURLを取得する。
    リンクは <a>前日 <i class="..."></i></a> のように子要素にアイコンタグを
    含むため、string=検索(直接の文字列子のみに一致)ではなくget_text()で判定する。
    """
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select(".prev_next_link a"):
        if "前日" in a.get_text() and a.get("href"):
            href = a["href"]
            return href if href.startswith("http") else f"https://min-repo.com{href}"
    return None


def parse_all_units_table(html: str):
    """?kishu=allページの「全台データ一覧」テーブル(機種・台番・差枚・G数・出率)をパースする"""
    soup = BeautifulSoup(html, "html.parser")
    rows_out = []

    table = None
    for t in soup.select(".table_wrap table"):
        header_text = t.get_text()
        if "機種" in header_text and "台番" in header_text:
            table = t
            break
    if table is None:
        return rows_out

    for tr in table.select("tr"):
        cells = tr.find_all("td")
        if len(cells) < 5:
            continue

        machine_name = cells[0].get_text(strip=True)
        unit_number_text = cells[1].get_text(strip=True)
        if not machine_name or not unit_number_text.isdigit():
            continue
        diff_text = cells[2].get_text(strip=True).replace(",", "")
        games_text = cells[3].get_text(strip=True).replace(",", "")

        try:
            diff = int(diff_text) if diff_text not in ("", "-") else None
        except ValueError:
            diff = None
        try:
            games = int(games_text) if games_text not in ("", "-") else None
        except ValueError:
            games = None

        rows_out.append({
            "machine_name": machine_name,
            "unit_number": int(unit_number_text),
            "diff": diff,
            "games": games,
        })

    return rows_out


def process_hall(hall_name: str, state: dict):
    if state.get("status") in ("done", "not_found"):
        return

    hall_start = time.time()

    def hall_time_left():
        return HALL_TIME_BUDGET_SECONDS - (time.time() - hall_start)

    current_url = state.get("next_url")
    days_done = state.get("days_done", 0)

    if current_url is None:
        current_url = get_latest_post_url(hall_name)
        if not current_url:
            print(f"  ! {hall_name}: 最新記事が見つかりませんでした")
            state["status"] = "not_found"
            return
        state["next_url"] = current_url
        state["status"] = "in_progress"

    while days_done < DAYS_BACK:
        if time_left() <= 0:
            state["next_url"] = current_url
            state["days_done"] = days_done
            state["status"] = "in_progress"
            return
        if hall_time_left() <= 0:
            # この店舗に持ち時間を使い切った。詰まっている店舗が全体の処理を
            # ブロックしないよう、ここで見切りをつけて次の店舗に進む。
            state["next_url"] = current_url
            state["days_done"] = days_done
            state["status"] = "in_progress"
            print(f"  [{hall_name}] 持ち時間({HALL_TIME_BUDGET_SECONDS}秒)を使い切ったため次の店舗へ")
            return

        print(f"  [{hall_name}] [{days_done + 1}/{DAYS_BACK}] {current_url}")

        def give_up_for_now(reason):
            # レート制限等でこの日のデータを取得できなかった場合、
            # days_done/next_urlを進めずに終了する。「前日」リンクが
            # 見つからない=データ終端、と誤判定して打ち切らないため、
            # また特定の日を「0台分」のまま永久にスキップしないための対策。
            state["next_url"] = current_url
            state["days_done"] = days_done
            state["status"] = "in_progress"
            print(f"  [{hall_name}] {reason}のため中断(同じ日から次回再開)")

        try:
            resp = fetch(current_url)
        except Exception as e:
            print(f"    ! ベースページ取得失敗: {e}")
            give_up_for_now("ベースページ取得失敗")
            return
        html = resp.text
        # 正常なページは数万バイト規模。チャレンジ用スタブ等の異常な
        # レスポンスはごく短いことが多いため、閾値を下回る場合も
        # ブロックされたとみなして安全側に倒す。
        if len(html) < 2000:
            give_up_for_now(f"ベースページが異常に短い({len(html)}バイト、レート制限の可能性)")
            return
        play_date = parse_date_from_title(html)

        all_url = f"{current_url.split('?')[0]}?kishu=all"
        try:
            all_resp = fetch(all_url, referer=current_url)
        except Exception as e:
            print(f"    ! 全台データ取得失敗: {e}")
            give_up_for_now("全台データ取得失敗")
            return
        if len(all_resp.text) < 2000:
            give_up_for_now(f"全台データが異常に短い({len(all_resp.text)}バイト、レート制限の可能性)")
            return

        unit_rows = parse_all_units_table(all_resp.text)
        if play_date:
            queue_unit_data(hall_name, play_date, unit_rows)
        print(f"    {play_date}: {len(unit_rows)}台分")

        time.sleep(2 + random.random() * 2)

        prev_url = get_prev_day_url(html)
        days_done += 1
        state["days_done"] = days_done

        if not prev_url:
            state["status"] = "done"
            state["next_url"] = None
            print(f"  [{hall_name}] 「前日」リンクが見つからず終了({days_done}日分取得)")
            return

        current_url = prev_url
        state["next_url"] = current_url
        time.sleep(2 + random.random() * 2)

    state["status"] = "done"


def run():
    halls = load_halls()
    progress = load_progress()

    try:
        for hall_name in halls:
            if time_left() <= 0:
                break
            state = progress.setdefault(
                hall_name, {"status": "pending", "days_done": 0, "next_url": None}
            )
            try:
                process_hall(hall_name, state)
            except Exception as e:
                print(f"! {hall_name} で失敗: {e}")
    finally:
        # 時間切れ・例外のいずれでも、ここまでに取得できた分は必ず保存する
        print("=== 取得データをParquetへ保存 ===")
        flush_unit_data()
        save_progress(progress)

    done = sum(1 for s in progress.values() if s.get("status") == "done")
    not_found = sum(1 for s in progress.values() if s.get("status") == "not_found")
    print(
        f"\n=== 今回の実行終了: 完了{done}/{len(halls)}店舗 "
        f"(未検出{not_found}店舗) ==="
    )


if __name__ == "__main__":
    run()

"""
unit_bonus_history(BB/REG)の過去データを遡って取得する。

これまでのscrape_unit_bonus_history.pyは「最新記事の個別台ページ(?num=)」
だけを見ていたため、各台につき当日+過去10日分しか蓄積できず、収集開始
(2026-10-02)以降の分しか積み上がらなかった。しかし実際には、min-repo.comの
古い記事(?num=台番号ページ)にもその記事の日付時点のBB/RB実績が載っている
ことが確認できた(backfill_minrepo_full.pyが使っている「過去の結果」一覧
=get_date_url_listで、数ヶ月前の記事に遡れることは既に実証済み)。

本スクリプトは、backfill_minrepo_full.pyと同じ「ホール単位でoldest_processed_
dateを進捗管理しながら過去の記事を遡る」方式で、各過去記事の対象台個別
ページを取得し、BB/RB等をshardとして保存する。

注意: ?kishu=all(全台一覧)ページにはBB/RB列が無い(機種/台番/差枚/G数/
出率のみ)ことを確認済みのため、diff/games backfillのような1ホール1
リクエストでの効率化はできず、台ごとに個別リクエストが必要(既存の
scrape_unit_bonus_history.pyと同じ制約)。そのため遡る日数は365日ではなく
DAYS_BACKで控えめに設定し、GitHub Actionsでの繰り返し実行に乗せて
じわじわ進める想定。

出力:
  data/unit_bonus_shards/backfill_shard_<run_id>.parquet (既存と同じ列構成)
  data/unit_bonus_backfill_progress.json
    列: ホールごとの{status, oldest_processed_date, days_done}
"""
import glob
import json
import os
import random
import time
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from backfill_minrepo_full import fetch, get_date_url_list, parse_date_from_label
from scrape_unit_bonus_history import parse_unit_page, parse_number

SCRIPT_DIR = Path(__file__).resolve().parent
SHARDS_DIR = SCRIPT_DIR / "data" / "unit_bonus_shards"
PROGRESS_PATH = SCRIPT_DIR / "data" / "unit_bonus_backfill_progress.json"
RUN_ID = os.environ.get("GITHUB_RUN_ID") or f"local{int(time.time())}"

TIME_BUDGET_SECONDS = int(os.environ.get("UNIT_BONUS_BACKFILL_TIME_BUDGET", "600"))
HALL_TIME_BUDGET_SECONDS = 90  # 1ホールにこの秒数以上かけたら見切りをつけて次のホールへ
DAYS_BACK = 180  # 1ホールにつき最大これだけ遡る(全台個別リクエストが必要なため365日は非現実的)
UNIT_REQUEST_DELAY = 0.5
MAX_CONSECUTIVE_EMPTY = 2

_start_time = time.time()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - _start_time)


def load_progress():
    if PROGRESS_PATH.exists():
        return json.load(open(PROGRESS_PATH))
    return {}


def save_progress(progress):
    json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


def load_target_units():
    lookup_path = SCRIPT_DIR / "data" / "machine_type_lookup.parquet"
    lookup = pd.read_parquet(lookup_path)
    target_names = set(lookup[lookup["category"].isin(["bonus", "art"])]["machine_name"])

    dfs = [pd.read_parquet(f) for f in glob.glob(str(SCRIPT_DIR / "data" / "daily_unit_data" / "2026-1*.parquet"))]
    df = pd.concat(dfs, ignore_index=True)
    df = df[df["machine_name"].isin(target_names)]
    return df[["hall_name", "machine_name", "unit_number"]].drop_duplicates()


FLUSH_INTERVAL_SECONDS = 60


def flush(progress, collected_rows, shard_path):
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
    today = date.today()
    collected_rows = []
    shard_path = SHARDS_DIR / f"backfill_shard_{RUN_ID}.parquet"
    last_flush = time.time()

    halls = targets["hall_name"].unique().tolist()
    random.shuffle(halls)

    for hall_name in halls:
        if time_left() <= 0:
            break
        state = progress.setdefault(hall_name, {"status": "pending", "days_done": 0, "oldest_processed_date": None})
        if state["status"] in ("done", "not_found"):
            continue

        hall_start = time.time()

        def hall_time_left():
            return HALL_TIME_BUDGET_SECONDS - (time.time() - hall_start)

        pairs = get_date_url_list(hall_name)
        if pairs is False:
            print(f"  ! [{hall_name}] タグページ取得が一時的に失敗。次回再試行")
            continue
        if pairs is None or not pairs:
            state["status"] = "not_found"
            print(f"  ! [{hall_name}] タグページ/一覧が見つかりません")
            continue

        oldest_processed = state.get("oldest_processed_date")
        oldest_processed_date = date.fromisoformat(oldest_processed) if oldest_processed else None

        dated_pairs = []
        for date_text, href in pairs:
            d = parse_date_from_label(date_text, today)
            if d is None:
                continue
            if oldest_processed_date is not None and d >= oldest_processed_date:
                continue
            dated_pairs.append((d, href))
        dated_pairs.sort(key=lambda x: x[0], reverse=True)

        if not dated_pairs:
            state["status"] = "done"
            print(f"  [{hall_name}] 一覧を使い切ったため終了({state['days_done']}日分)")
            continue

        hall_units = targets[targets["hall_name"] == hall_name]
        time.sleep(1.0 + random.random())

        for d, href in dated_pairs:
            if state["days_done"] >= DAYS_BACK:
                state["status"] = "done"
                break
            if time_left() <= 0 or hall_time_left() <= 0:
                break

            article_url = href if href.startswith("http") else f"https://min-repo.com{href}"
            try:
                fetch(article_url)  # ウォームアップ(sleepを挟まず直後に本番取得する既存の作法に合わせる)
            except Exception as e:
                print(f"  ! [{hall_name}] {d} ベースページ取得失敗: {e}")
                continue

            consecutive_empty = 0
            day_rows = 0
            for _, row in hall_units.iterrows():
                if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                    print(f"    [{hall_name}] {d} 空レスポンス連続のためこの日は切り上げ")
                    break
                url = f"{article_url.split('?')[0]}?num={row['unit_number']}"
                try:
                    resp = fetch(url, referer=article_url)
                    parsed = parse_unit_page(resp.text)
                    consecutive_empty = 0 if parsed else consecutive_empty + 1
                    # 遡っている記事自体の日付(d)に対応する行だけを採用する
                    # (当日欄のみ。過去10日欄が付いていても二重取得を避けるため無視)
                    for p in parsed[:1]:
                        collected_rows.append({
                            "hall_name": hall_name,
                            "machine_name": row["machine_name"],
                            "unit_number": row["unit_number"],
                            "play_date": d.isoformat(),
                            "diff": p.get("diff"),
                            "games": p.get("games"),
                            "bb": p.get("bb"),
                            "rb": p.get("rb"),
                            "slump_graph": None,
                        })
                        day_rows += 1
                except Exception:
                    pass
                time.sleep(UNIT_REQUEST_DELAY)

                if time.time() - last_flush >= FLUSH_INTERVAL_SECONDS:
                    flush(progress, collected_rows, shard_path)
                    last_flush = time.time()

            print(f"  [{hall_name}] {d}: {day_rows}/{len(hall_units)}台")
            state["days_done"] = state.get("days_done", 0) + 1
            state["oldest_processed_date"] = d.isoformat()
            state["status"] = "in_progress"

    flush(progress, collected_rows, shard_path)
    print(f"\n保存: {shard_path.name} ({len(collected_rows)}行)" if collected_rows else "\n今回収集した行なし")


if __name__ == "__main__":
    run()

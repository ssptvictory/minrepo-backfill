"""BB/RB(個別台ページ ?num=)の過去分を、全ホール横並びで遡って取得する。

方針(2026-11-24):
- 全ホールを「いちばん浅いホールから1記事ずつ」進める(幅優先)。
  先に全ホールの直近1か月がそろい、その後さらに古い日付へ進む。
- 既存のunit_bonus_shardsで既にカバーされている日は取らない。
  各ホールの「ここより新しい日はカバー済み」という境界(covered_oldest)を
  初回に既存データから決め、そこから過去へ進む。
- 個別台ページには、その記事の日付+過去10日分(BB/RB付き)の表がある。
  古い記事のページにも過去10日表が付いていれば、1記事で11日分取れるので
  11日おきに記事を選ぶ。付いていなければ1日ずつ進む(実行時に自動判定)。
- 取得する台は、その記事の日付〜10日前の全台一覧(daily_unit_data)で
  Aタイプ・ART(category bonus/art)だった台番号。機種名はその時点の
  一覧のものを入れる(分析側でも全台一覧の機種名に付け替える)。
- サイトへの配慮: 取得は1本ずつ(並列にしない)、リクエスト間隔は1秒以上。
  空レスポンス(レート制限)が続いたら2分待ち、それでも続けば今回の実行を
  打ち切り、2時間の休止期限(data/unit_bonus_backfill_cooldown.json)を
  記録する。期限内にcron-job.orgから起動されても何もせず終了する。
- スランプグラフはAT機(category="at")専用なので、ここでは保存しない。

出力:
  data/unit_bonus_shards/backfill_<run_id>_<chunk>.parquet (既存と同じ列構成)
  data/unit_bonus_backfill_progress.json
    ホールごとの {status, covered_oldest, articles_done}
"""
import glob
import heapq
import json
import os
import random
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from backfill_minrepo_full import fetch, get_date_url_list, parse_date_from_label
from scrape_unit_bonus_history import parse_unit_page

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
SHARDS_DIR = DATA_DIR / "unit_bonus_shards"
PROGRESS_PATH = DATA_DIR / "unit_bonus_backfill_progress.json"
RATE_LIMIT_MARKER = DATA_DIR / ".unit_bonus_rate_limited"  # ワークフローのチャンクループを止める合図(commitしない)
COOLDOWN_PATH = DATA_DIR / "unit_bonus_backfill_cooldown.json"  # レート制限後の休止期限(commitする)
COOLDOWN_HOURS = 2
RUN_ID = os.environ.get("GITHUB_RUN_ID") or f"local{int(time.time())}"
CHUNK = os.environ.get("UNIT_BONUS_BACKFILL_CHUNK", "0")

TIME_BUDGET_SECONDS = int(os.environ.get("UNIT_BONUS_BACKFILL_TIME_BUDGET", "1800"))
DAYS_BACK = int(os.environ.get("UNIT_BONUS_BACKFILL_DAYS_BACK", "365"))
TARGET_CATEGORIES = {"bonus", "art"}
HISTORY_DAYS = 10
REQUEST_DELAY = 1.0          # 秒。これに0〜0.5秒のゆらぎを足す
MAX_CONSECUTIVE_EMPTY = 3    # 空レスポンス(レート制限の疑い)の連続回数
NO_TABLE_LIMIT = 5           # 1記事の最初からBB/RB表の無い台がこの数続いたら、その記事(店舗)を飛ばす
MAX_CONSECUTIVE_DEAD_HALLS = 3  # 既存データの無い店舗で空が続くのは店舗側の事情とみなすが、3店続いたらレート制限扱い
RATE_LIMIT_PAUSE = 120
COVERED_SHARE = 0.8          # 既存データで台の8割以上が取れている日はカバー済みとみなす
FLUSH_INTERVAL_SECONDS = 60

_start = time.time()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - _start)


def polite_sleep():
    time.sleep(REQUEST_DELAY + random.random() * 0.5)


def load_lineup():
    """(hall, play_date) -> その日のbonus/art台の {unit_number: machine_name}"""
    lookup = pd.read_parquet(DATA_DIR / "machine_type_lookup.parquet")
    names = set(lookup[lookup["category"].isin(TARGET_CATEGORIES)]["machine_name"])
    since = (date.today() - timedelta(days=DAYS_BACK + HISTORY_DAYS + 5)).strftime("%Y-%m")
    dfs = []
    for f in sorted(glob.glob(str(DATA_DIR / "daily_unit_data" / "*.parquet"))):
        if Path(f).stem < since:
            continue
        d = pd.read_parquet(f, columns=["hall_name", "play_date", "machine_name", "unit_number"])
        dfs.append(d[d["machine_name"].isin(names)])
    df = pd.concat(dfs, ignore_index=True)
    df["play_date"] = pd.to_datetime(df["play_date"]).dt.date
    return df


def covered_dates(lineup, existing):
    """{hall: 既存データで台の8割以上がそろっている日の集合}"""
    have = existing.groupby(["hall_name", "play_date"]).size().rename("have")
    need = lineup.groupby(["hall_name", "play_date"]).size().rename("need")
    cov = pd.concat([need, have], axis=1).fillna(0)
    cov = cov[cov["have"] >= cov["need"] * COVERED_SHARE].reset_index()
    return {h: set(g["play_date"]) for h, g in cov.groupby("hall_name")}


def initial_covered_oldest(lineup, covered):
    """直近の未カバー日(毎日の収集scrape_unit_bonus_history.pyが7日ごとの
    取り直しで埋める分)は飛ばし、既存データの連続したカバー期間の最古日を
    境界にする。既存データが無いホールは今日。"""
    out = {}
    for hall in lineup["hall_name"].unique():
        days = sorted(covered.get(hall, ()), reverse=True)
        if not days:
            out[hall] = date.today().isoformat()
            continue
        oldest = days[0]
        hall_days = sorted({d for d in lineup.loc[lineup["hall_name"] == hall, "play_date"] if d <= days[0]},
                           reverse=True)
        for d in hall_days:
            if d not in covered[hall]:
                break
            oldest = d
        out[hall] = oldest.isoformat()
    return out


def load_existing_keys():
    files = glob.glob(str(DATA_DIR / "unit_bonus_shards" / "*.parquet"))
    dfs = [pd.read_parquet(f, columns=["hall_name", "unit_number", "play_date"]) for f in files]
    if not dfs:
        return pd.DataFrame({"hall_name": [], "unit_number": [], "play_date": []})
    df = pd.concat(dfs, ignore_index=True).drop_duplicates()
    df["play_date"] = pd.to_datetime(df["play_date"]).dt.date
    return df


def load_progress():
    if PROGRESS_PATH.exists():
        return json.load(open(PROGRESS_PATH, encoding="utf-8"))
    return {}


def save(progress, rows, shard_path):
    json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    if rows:
        pq.write_table(pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False),
                       shard_path, compression="zstd", compression_level=15)


def in_cooldown():
    if not COOLDOWN_PATH.exists():
        return False
    until = datetime.fromisoformat(json.load(open(COOLDOWN_PATH))["until"])
    return datetime.now(timezone.utc) < until


def start_cooldown():
    until = datetime.now(timezone.utc) + timedelta(hours=COOLDOWN_HOURS)
    json.dump({"until": until.isoformat()}, open(COOLDOWN_PATH, "w"))
    RATE_LIMIT_MARKER.touch()


def run():
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)
    RATE_LIMIT_MARKER.unlink(missing_ok=True)
    if in_cooldown():
        # cron-job.orgは数十分おきに起動してくるので、レート制限後はここで休む
        print("レート制限後の休止期間中のため、何もせず終了します")
        RATE_LIMIT_MARKER.touch()
        return
    today = date.today()
    limit_date = today - timedelta(days=DAYS_BACK)

    print("全台一覧(bonus/art)読み込み中...")
    lineup = load_lineup()
    lineup_idx = {k: dict(zip(g["unit_number"], g["machine_name"]))
                  for k, g in lineup.groupby(["hall_name", "play_date"])}
    progress = load_progress()
    print("既存データのカバー状況を算出中...")
    covered_map = covered_dates(lineup, load_existing_keys())
    new_halls = [h for h in lineup["hall_name"].unique() if h not in progress]
    if new_halls:
        print(f"初回のカバー境界を既存データから算出中({len(new_halls)}ホール)...")
        init = initial_covered_oldest(lineup[lineup["hall_name"].isin(new_halls)], covered_map)
        for h in new_halls:
            progress[h] = {"status": "in_progress", "covered_oldest": init.get(h, today.isoformat()),
                           "articles_done": 0}

    # 幅優先: covered_oldestが新しい(=まだ浅い)ホールから1記事ずつ
    heap = [(-date.fromisoformat(s["covered_oldest"]).toordinal(), h)
            for h, s in progress.items() if s["status"] == "in_progress"]
    heapq.heapify(heap)
    print(f"対象ホール {len(heap)} / 完了 {sum(s['status'] == 'done' for s in progress.values())}")

    shard_path = SHARDS_DIR / f"backfill_{RUN_ID}_{CHUNK}.parquet"
    rows, pairs_cache = [], {}
    consecutive_dead = 0
    last_flush = time.time()
    n_requests = 0

    while heap and time_left() > 0:
        _, hall = heapq.heappop(heap)
        state = progress[hall]
        covered = date.fromisoformat(state["covered_oldest"])
        if covered <= limit_date:
            state["status"] = "done"
            continue

        if hall not in pairs_cache:
            polite_sleep()
            pairs = get_date_url_list(hall)
            n_requests += 1
            if pairs is False:
                print(f"  ! [{hall}] 記事一覧の取得に一時失敗。次回再試行")
                continue
            if not pairs:
                state["status"] = "not_found"
                continue
            dated = [(parse_date_from_label(t, today), h) for t, h in pairs]
            pairs_cache[hall] = sorted([(d, h) for d, h in dated if d], reverse=True)
        # 境界より古く、まだカバーされていない日の記事のうち最新のもの
        already = covered_map.get(hall, set())
        older = [(d, h) for d, h in pairs_cache[hall] if d < covered and d not in already]
        if not older:
            state["status"] = "done"
            print(f"  [{hall}] 記事一覧を使い切ったので完了")
            continue

        art_date, href = older[0]
        article_url = href if href.startswith("http") else f"https://min-repo.com{href}"
        window = [art_date - timedelta(days=i) for i in range(HISTORY_DAYS + 1)]
        units = {}
        for d in reversed(window):  # 新しい日の機種名で上書き
            units.update(lineup_idx.get((hall, d), {}))
        if not units:
            state["covered_oldest"] = art_date.isoformat()
            heapq.heappush(heap, (-art_date.toordinal(), hall))
            continue

        try:
            polite_sleep()
            fetch(article_url)
            n_requests += 1
        except Exception as e:
            print(f"  ! [{hall}] {art_date} 記事取得失敗: {e}")
            continue

        print(f"  [{hall}] 記事{art_date}を取得中({len(units)}台)")
        empty, pages, no_table, hist_dates = 0, 0, 0, {}
        aborted = no_bbrb = dead_hall = False
        for unit, machine in units.items():
            if time_left() <= 0:
                aborted = True
                break
            polite_sleep()
            try:
                resp = fetch(f"{article_url.split('?')[0]}?num={unit}", referer=article_url)
                n_requests += 1
                blocked = len(resp.text) < 2000  # 0バイト等=レート制限の疑い
                parsed = [] if blocked else parse_unit_page(resp.text)
            except Exception as e:
                print(f"    ! 取得エラー: {e}")
                blocked, parsed = True, []
            if not blocked and not parsed:
                # ページは正常だがBB/RBの表が無い(BB/RBを掲載していない店舗など)
                empty = 0
                no_table += 1
                if pages == 0 and no_table >= NO_TABLE_LIMIT:
                    no_bbrb = True
                    break
                continue
            if blocked:
                empty += 1
                if empty == MAX_CONSECUTIVE_EMPTY and pages == 0 and not covered_map.get(hall):
                    # 既存データが1件も無い店舗で最初から空が続く=店舗側の事情
                    # (毎日の収集でも取れていない)。レート制限とはみなさず対象外にする。
                    dead_hall = True
                    break
                if empty == MAX_CONSECUTIVE_EMPTY:
                    print(f"  ! 空レスポンスが{empty}回連続。{RATE_LIMIT_PAUSE}秒待機")
                    time.sleep(RATE_LIMIT_PAUSE)
                elif empty >= MAX_CONSECUTIVE_EMPTY * 2:
                    print(f"  ! 待機後も空レスポンスが続くため打ち切り、{COOLDOWN_HOURS}時間休止します")
                    start_cooldown()
                    aborted = True
                    break
                continue
            empty = 0
            pages += 1
            for p in parsed:
                # 日付は記事の日付を基準に解決する(年をまたぐ過去記事でも正しくなる)
                d = parse_date_from_label(p["date_label"], art_date)
                if d is None or d > art_date or d >= covered or d < limit_date:
                    continue
                if d < art_date:
                    hist_dates[d] = hist_dates.get(d, 0) + 1
                rows.append({"hall_name": hall, "machine_name": machine, "unit_number": int(unit),
                             "play_date": d.isoformat(), "diff": p.get("diff"), "games": p.get("games"),
                             "bb": p.get("bb"), "rb": p.get("rb"), "slump_graph": None})
            if time.time() - last_flush >= FLUSH_INTERVAL_SECONDS:
                save(progress, rows, shard_path)
                last_flush = time.time()

        if aborted:
            break  # この記事は未完了扱い(covered_oldestを進めない)。次回やり直す
        if dead_hall:
            consecutive_dead += 1
            if consecutive_dead >= MAX_CONSECUTIVE_DEAD_HALLS:
                # 店舗をまたいで空が続く=本当にレート制限の可能性が高い
                print(f"  ! 空レスポンスの店舗が{consecutive_dead}店続いたため打ち切り、{COOLDOWN_HOURS}時間休止します")
                start_cooldown()
                break
            state["status"] = "no_data"
            print(f"  [{hall}] 個別台ページが空で、既存データも無いため対象外にします")
            continue
        consecutive_dead = 0
        if no_bbrb:
            if not covered_map.get(hall):
                # 毎日の収集でも1件も取れていない=この店舗はBB/RBを掲載していない
                state["status"] = "no_bbrb"
                print(f"  [{hall}] BB/RBの表が無い台が{no_table}台続き、既存データも無いため対象外にします")
            else:
                # 既存データはあるので、この記事だけ表が無かったとみなして次の記事へ
                state["covered_oldest"] = art_date.isoformat()
                print(f"  [{hall}] 記事{art_date}はBB/RBの表が無いため飛ばします")
                heapq.heappush(heap, (-art_date.toordinal(), hall))
            continue

        # 過去10日表が半数以上の台に付いていた最古の日まで進める。無ければ記事の日まで。
        common = [d for d, c in hist_dates.items() if c >= max(1, pages // 2)]
        new_covered = min(common) if common else art_date
        state["covered_oldest"] = new_covered.isoformat()
        state["articles_done"] = state.get("articles_done", 0) + 1
        state["last_step_days"] = (covered - new_covered).days
        print(f"  [{hall}] 記事{art_date}: {pages}/{len(units)}台, 過去表{'あり' if common else 'なし'} "
              f"→ {new_covered}まで取得済み")
        heapq.heappush(heap, (-new_covered.toordinal(), hall))

    save(progress, rows, shard_path)
    done = sum(s["status"] != "in_progress" for s in progress.values())
    print(f"\n今回: {n_requests}リクエスト, {len(rows)}行 / ホール完了 {done}/{len(progress)}")
    depth = pd.Series({h: (today - date.fromisoformat(s["covered_oldest"])).days for h, s in progress.items()})
    print(f"遡った日数(ホール別): 最小{depth.min()}日 / 中央値{int(depth.median())}日 / 最大{depth.max()}日")


if __name__ == "__main__":
    run()

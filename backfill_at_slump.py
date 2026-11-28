"""AT機(machine_type_lookup.category="at")のスランプグラフを、機種別ページ
(記事URL?kishu=機種名 の「グラフ一覧」)から取得する。

- 機種別ページには、その日のその機種の全台について 台番号・差枚・G数・合成・
  スランプ(約100Gごとの差枚推移)が載っている。1店舗×1機種×1日で1リクエスト
  (台ごとの個別台ページより約1/3のアクセス)。
- 個別台ページと違い過去10日表のような複数日の情報は無いので、日付ごとに取る。
- 進め方(全ホール横並び):
  1) まず各ホールの「まだ取っていない新しい日」を古い順に追いつかせる(毎日の分)
  2) 追いついたホールから、浅い順に1記事ずつ過去へ遡る(DAYS_BACK日前まで)
- 機種別ページにグラフ一覧が無い機種(1台だけの機種など)は、その台の
  個別台ページ(?num=)から当日のスランプを取る。
- サイトへの配慮・レート制限・休止期限はBB/RB過去分(backfill_unit_bonus_history.py)
  と共通(同じ休止期限ファイルを使う)。

出力:
  data/at_slump_shards/at_<run_id>_<chunk>.parquet
    列: hall_name, play_date, machine_name, unit_number, diff, games,
        combined_rate(合成の分母。1/60なら60), slump_graph(JSON文字列)
  data/at_slump_progress.json
    ホールごとの {status, newest_done, oldest_done, articles_done}
"""
import glob
import heapq
import json
import os
import re
import time
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from backfill_minrepo_full import SiteUnavailable, fetch, get_date_url_list, parse_date_from_label
from backfill_unit_bonus_history import (RATE_LIMIT_MARKER, MAX_CONSECUTIVE_EMPTY, RATE_LIMIT_PAUSE,
                                         COOLDOWN_HOURS, in_cooldown, start_cooldown, polite_sleep)
from scrape_unit_bonus_history import parse_number, parse_unit_page

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
SHARDS_DIR = DATA_DIR / "at_slump_shards"
PROGRESS_PATH = DATA_DIR / "at_slump_progress.json"
RUN_ID = os.environ.get("GITHUB_RUN_ID") or f"local{int(time.time())}"
CHUNK = os.environ.get("AT_SLUMP_CHUNK", "0")

TIME_BUDGET_SECONDS = int(os.environ.get("AT_SLUMP_TIME_BUDGET", "2700"))
DAYS_BACK = int(os.environ.get("AT_SLUMP_DAYS_BACK", "365"))
FRESH_DAYS = 2   # 直近この日数以内で全台一覧がまだ無い日は「まだ準備できていない」とみなす
FLUSH_INTERVAL_SECONDS = 60

_start = time.time()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - _start)


def parse_slump_list(html: str):
    """機種別ページの「グラフ一覧」から台ごとの数値を取り出す。"""
    i = html.find('class="slump_list"')
    if i < 0:
        return []
    j = html.find("</ul>", i)
    seg = html[i: j if j > 0 else len(html)]
    out = []
    for item in re.split(r"<li[\s>]", seg)[1:]:
        m = re.search(r"\?num=(\d+)", item)
        if not m:
            continue
        tds = re.findall(r"<td[^>]*>([^<]*)</td>", item)
        diff = parse_number(tds[0]) if len(tds) > 0 else None
        games = parse_number(tds[1]) if len(tds) > 1 else None
        combined = None
        if len(tds) > 2:
            c = re.match(r"\s*1/([\d,.]+)", tds[2])
            combined = parse_number(c.group(1)) if c else None
        d = re.search(r"datasets:\s*\[\s*\{.*?data:\s*\[([^\]]*)\]", item, re.S)
        slump = None
        if d:
            try:
                slump = [int(x.strip()) for x in d.group(1).split(",") if x.strip()]
            except ValueError:
                slump = None
        out.append({"unit_number": int(m.group(1)), "diff": diff, "games": games,
                    "combined_rate": combined, "slump_graph": json.dumps(slump) if slump else None})
    return out


def load_lineup():
    """{(hall, date): {machine: [unit_number, ...]}} (AT機のみ)"""
    lookup = pd.read_parquet(DATA_DIR / "machine_type_lookup.parquet")
    names = set(lookup[lookup["category"] == "at"]["machine_name"])
    since = (date.today() - timedelta(days=DAYS_BACK + 5)).strftime("%Y-%m")
    dfs = []
    for f in sorted(glob.glob(str(DATA_DIR / "daily_unit_data" / "*.parquet"))):
        if Path(f).stem < since:
            continue
        d = pd.read_parquet(f, columns=["hall_name", "play_date", "machine_name", "unit_number"])
        dfs.append(d[d["machine_name"].isin(names)])
    df = pd.concat(dfs, ignore_index=True)
    df["play_date"] = pd.to_datetime(df["play_date"]).dt.date
    idx = {}
    for (h, d, m), g in df.groupby(["hall_name", "play_date", "machine_name"]):
        idx.setdefault((h, d), {})[m] = g["unit_number"].astype(int).tolist()
    return idx, sorted(df["hall_name"].unique())


ARTICLE_CACHE_PATH = DATA_DIR / ".at_slump_article_cache.json"  # commitしない(同じジョブ内の区切り間で使い回す)
ARTICLE_CACHE_HOURS = 6


def load_article_cache():
    try:
        raw = json.load(open(ARTICLE_CACHE_PATH, encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    cutoff = time.time() - ARTICLE_CACHE_HOURS * 3600
    return {h: [(date.fromisoformat(d), u) for d, u in v["pairs"]]
            for h, v in raw.items() if v.get("fetched", 0) >= cutoff}


def save_article_cache(cache, fetched_at):
    json.dump({h: {"fetched": fetched_at.get(h, time.time()), "pairs": [[d.isoformat(), u] for d, u in v]}
               for h, v in cache.items()}, open(ARTICLE_CACHE_PATH, "w", encoding="utf-8"), ensure_ascii=False)


def load_progress():
    if PROGRESS_PATH.exists():
        return json.load(open(PROGRESS_PATH, encoding="utf-8"))
    return {}


def save(progress, rows, shard_path):
    json.dump(progress, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    if rows:
        pq.write_table(pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False),
                       shard_path, compression="zstd", compression_level=15)


def next_task(state, pairs, today, limit_date):
    """(優先度, 並び順キー, 記事日付, href) を返す。無ければNone。
    優先度0=新しい日への追いつき(古い順)、1=過去への遡り(浅いホールから)。"""
    newest = date.fromisoformat(state["newest_done"]) if state.get("newest_done") else None
    oldest = date.fromisoformat(state["oldest_done"]) if state.get("oldest_done") else None
    if newest is None:
        if pairs:
            d, h = pairs[0]  # 最新の記事から始める
            return (0, d.toordinal(), d, h)
        return None
    forward = [(d, h) for d, h in pairs if d > newest]
    if forward:
        d, h = min(forward)
        return (0, d.toordinal(), d, h)
    backward = [(d, h) for d, h in pairs if d < oldest and d >= limit_date]
    if backward:
        d, h = max(backward)
        return (1, (today - oldest).days, d, h)
    return None


def run():
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)
    RATE_LIMIT_MARKER.unlink(missing_ok=True)
    if in_cooldown():
        print("レート制限後の休止期間中のため、何もせず終了します")
        RATE_LIMIT_MARKER.touch()
        return
    today = date.today()
    limit_date = today - timedelta(days=DAYS_BACK)

    print("全台一覧(AT機)読み込み中...")
    lineup, halls = load_lineup()
    progress = load_progress()
    for h in halls:
        progress.setdefault(h, {"status": "in_progress", "newest_done": None, "oldest_done": None,
                                "articles_done": 0})

    shard_path = SHARDS_DIR / f"at_{RUN_ID}_{CHUNK}.parquet"
    rows = []
    pairs_cache = load_article_cache()
    fetched_at = {h: time.time() for h in pairs_cache}  # 読み込んだ分は期限内なので保存時刻を据え置き相当に扱う
    try:
        raw = json.load(open(ARTICLE_CACHE_PATH, encoding="utf-8"))
        fetched_at.update({h: v["fetched"] for h, v in raw.items() if h in pairs_cache})
    except (OSError, ValueError):
        pass
    last_flush = time.time()
    n_requests = 0
    consecutive_dead = 0

    # 記事一覧が手元にあるホールは次の作業で並べ、無いホールは一覧取得を先に行う
    heap = []
    for h, s in progress.items():
        if s["status"] != "in_progress":
            continue
        if h in pairs_cache:
            task = next_task(s, pairs_cache[h], today, limit_date)
            if task:
                heap.append((task[0], task[1], h))
        else:
            depth = (today - date.fromisoformat(s["oldest_done"])).days if s.get("oldest_done") else -1
            heap.append((0, depth, h))
    heapq.heapify(heap)
    print(f"対象ホール {len(heap)}")

    try:
        while heap and time_left() > 0:
            _, _, hall = heapq.heappop(heap)
            state = progress[hall]
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
                fetched_at[hall] = time.time()
                task = next_task(state, pairs_cache[hall], today, limit_date)
                if task:
                    heapq.heappush(heap, (task[0], task[1], hall))
                continue  # 一覧を取ったら優先度を付け直して並べ直す

            task = next_task(state, pairs_cache[hall], today, limit_date)
            if task is None:
                continue  # 遡り切っても「完了」にはしない(毎日の新しい分を取り続けるため)
            _, _, art_date, href = task
            article_url = href if href.startswith("http") else f"https://min-repo.com{href}"
            machines = lineup.get((hall, art_date))
            if not machines:
                if (today - art_date).days <= FRESH_DAYS:
                    continue  # 全台一覧がまだ無い新しい日。次回以降に回す
                machines = {}

            if machines:
                print(f"  [{hall}] 記事{art_date}を取得中({len(machines)}機種)")
                try:
                    polite_sleep()
                    fetch(article_url)
                    n_requests += 1
                except Exception as e:
                    print(f"  ! [{hall}] {art_date} 記事取得失敗: {e}")
                    continue

            empty, got_units, dead_hall, aborted = 0, 0, False, False
            base = article_url.split("?")[0]
            for machine, units in machines.items():
                if time_left() <= 0:
                    aborted = True
                    break
                polite_sleep()
                try:
                    resp = fetch(f"{base}?kishu={quote_plus(machine)}", referer=article_url)
                    n_requests += 1
                    blocked = len(resp.text) < 2000
                    items = [] if blocked else parse_slump_list(resp.text)
                except Exception as e:
                    print(f"    ! 取得エラー: {e}")
                    blocked, items = True, []

                if blocked:
                    empty += 1
                    if empty == MAX_CONSECUTIVE_EMPTY and got_units == 0 and state.get("articles_done", 0) == 0:
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

                if not items:
                    # グラフ一覧が無い機種(1台のみ等)は個別台ページから当日のスランプを取る
                    for u in units:
                        polite_sleep()
                        try:
                            r2 = fetch(f"{base}?num={u}", referer=article_url)
                            n_requests += 1
                            parsed = [] if len(r2.text) < 2000 else parse_unit_page(r2.text)
                        except Exception:
                            parsed = []
                        for p in parsed:
                            if parse_date_from_label(p["date_label"], art_date) == art_date:
                                items.append({"unit_number": int(u), "diff": p.get("diff"), "games": p.get("games"),
                                              "combined_rate": None, "slump_graph": p.get("slump_graph")})
                                break
                for it in items:
                    rows.append({"hall_name": hall, "play_date": art_date.isoformat(), "machine_name": machine, **it})
                got_units += len(items)
                if time.time() - last_flush >= FLUSH_INTERVAL_SECONDS:
                    save(progress, rows, shard_path)
                    last_flush = time.time()

            if aborted:
                break
            if dead_hall:
                consecutive_dead += 1
                if consecutive_dead >= 3:
                    print(f"  ! 空レスポンスの店舗が{consecutive_dead}店続いたため打ち切り、{COOLDOWN_HOURS}時間休止します")
                    start_cooldown()
                    break
                state["status"] = "no_data"
                print(f"  [{hall}] 機種別ページが空のため対象外にします")
                continue
            consecutive_dead = 0

            newest = date.fromisoformat(state["newest_done"]) if state.get("newest_done") else None
            oldest = date.fromisoformat(state["oldest_done"]) if state.get("oldest_done") else None
            state["newest_done"] = max(art_date, newest).isoformat() if newest else art_date.isoformat()
            state["oldest_done"] = min(art_date, oldest).isoformat() if oldest else art_date.isoformat()
            state["articles_done"] = state.get("articles_done", 0) + 1
            if machines:
                print(f"  [{hall}] 記事{art_date}: {got_units}台分")
            task = next_task(state, pairs_cache[hall], today, limit_date)
            if task:
                heapq.heappush(heap, (task[0], task[1], hall))

    except SiteUnavailable as e:
        # みんレポがエラーを返し続けている(落ちている/断られている)。ここまでの分を保存して、ワークフローのチャンクも止める
        print(f"! {e}")
        RATE_LIMIT_MARKER.touch()

    save(progress, rows, shard_path)
    save_article_cache(pairs_cache, fetched_at)
    print(f"\n今回: {n_requests}リクエスト, {len(rows)}台日")
    depth = [(today - date.fromisoformat(s["oldest_done"])).days for s in progress.values() if s.get("oldest_done")]
    if depth:
        s = pd.Series(depth)
        print(f"遡った日数(着手済み{len(s)}ホール): 最小{s.min()}日 / 中央値{int(s.median())}日 / 最大{s.max()}日")


if __name__ == "__main__":
    run()

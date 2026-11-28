"""推薦する地域の店(data/priority_halls.json)だけ、AT系機種のスランプグラフを毎日取る。

- 機種別ページ(記事URL?kishu=機種名 の「グラフ一覧」)は、その日のその機種の全台の
  台番号・差枚・G数・合成・スランプ(約100Gごとの差枚推移)を1ページで返す。1店×1機種×1日で1回。
- 対象: その日の全台一覧で2台以上ある、BB/RBで判別する機種(Aタイプ)以外の機種。1台だけの機種は取らない
  (台ごとのページを取ることになり回数が増えるため)。
- 取る日: 毎日の収集(backfill_minrepo_full.py)が記録した各店の最新記事の日だけ。記事一覧は取り直さない。
  取りこぼした日は取りに戻らない(過去分を遡らない)。
- 回数の上限: このスクリプトだけで1日 AREA_SLUMP_DAILY_CAP 回(既定1,500、日本時間)。
  全体の上限(1日6,000回・1時間400回)と休止(5xx/空ページ続き)は fetch() 側で共通に効く。
- 取り方: 記事ページ(ウォームアップ)の直後に機種ページを0.5秒間隔で続けて取る(台ページと同じ。間を空けると空になる)。
  空ページが2回続いたらその店はやめる。店と店の間は15〜20秒空ける。
- AREA_SLUMP_ENABLED=1 のときだけ動く(みんレポへのアクセスが安定してから有効にする)。

出力: data/area_slump_shards/slump_<run_id>.parquet
        列: hall_name, play_date, machine_name, unit_number, diff, games, combined_rate, slump_graph(JSON文字列)
      data/area_slump_progress.json  {"halls": {店: 取った最新の日}, "requests": {"date": 日本時間の日付, "n": 回数}}
"""
import json
import os
import random
import time
from datetime import date
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from backfill_minrepo_full import SiteUnavailable, fetch, jst_now
from backfill_at_slump import parse_slump_list

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
SHARDS_DIR = DATA_DIR / "area_slump_shards"
PROGRESS_PATH = DATA_DIR / "area_slump_progress.json"
RUN_ID = os.environ.get("GITHUB_RUN_ID") or f"local{int(time.time())}"
ENABLED = os.environ.get("AREA_SLUMP_ENABLED", "0") == "1"
TIME_BUDGET = int(os.environ.get("AREA_SLUMP_TIME_BUDGET", "240"))
DAILY_CAP = int(os.environ.get("AREA_SLUMP_DAILY_CAP", "1500"))
PAGE_DELAY = 0.5
HALL_PAUSE = 15.0
MAX_EMPTY = 2
_start = time.time()


def time_left():
    return TIME_BUDGET - (time.time() - _start)


def latest_articles():
    try:
        bp = json.load(open(DATA_DIR / "backfill_progress.json", encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    out = {}
    for hall, s in bp.items():
        a = s.get("latest_article") if isinstance(s, dict) else None
        if a and a.get("url") and a.get("date"):
            out[hall] = (date.fromisoformat(a["date"]), a["url"])
    return out


def machines_for(hall, day):
    """その日の全台一覧で2台以上ある、Aタイプ(bonus)以外の機種。"""
    f = DATA_DIR / "daily_unit_data" / f"{day:%Y-%m}.parquet"
    if not f.exists():
        return []
    d = pd.read_parquet(f, columns=["hall_name", "play_date", "machine_name", "unit_number"],
                        filters=[("hall_name", "==", hall)])
    d = d[pd.to_datetime(d.play_date).dt.date == day]
    lookup = pd.read_parquet(DATA_DIR / "machine_type_lookup.parquet")
    bonus = set(lookup.loc[lookup.category == "bonus", "machine_name"])
    n = d[~d.machine_name.isin(bonus)].groupby("machine_name").unit_number.nunique()
    return sorted(n[n >= 2].index)


def run():
    if not ENABLED:
        print("AREA_SLUMP_ENABLED が 1 ではないので何もしません")
        return
    halls = json.load(open(DATA_DIR / "priority_halls.json", encoding="utf-8"))["halls"]
    prog = json.load(open(PROGRESS_PATH, encoding="utf-8")) if PROGRESS_PATH.exists() else {}
    done = prog.setdefault("halls", {})
    today_jst = jst_now().date().isoformat()
    req = prog.get("requests") or {}
    if req.get("date") != today_jst:
        req = {"date": today_jst, "n": 0}
    prog["requests"] = req
    arts = latest_articles()

    # まだ取っていない新しい日がある店を、取った日が古い順に
    todo = [(done.get(h, ""), random.random(), h) for h in halls
            if h in arts and arts[h][0].isoformat() > done.get(h, "")]
    todo.sort()
    print(f"対象 {len(halls)}店のうち、新しい日がある店 {len(todo)} / 今日すでに {req['n']}回(上限 {DAILY_CAP})")
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)
    rows, skipped_gap = [], 0
    try:
        for _, _, hall in todo:
            if time_left() <= 0 or req["n"] >= DAILY_CAP:
                break
            day, url = arts[hall]
            if done.get(hall) and (day - date.fromisoformat(done[hall])).days > 1:
                skipped_gap += 1
            machines = machines_for(hall, day)
            if not machines:
                continue  # 全台一覧がまだ無い(毎日の収集がこの日を取り終えていない)
            if req["n"] + 1 + len(machines) > DAILY_CAP:
                break
            time.sleep(HALL_PAUSE + random.random() * 5)
            fetch(url)
            req["n"] += 1
            base = url.split("?")[0]
            empty = got = 0
            for m in machines:
                if time_left() <= 0 or empty >= MAX_EMPTY:
                    break
                time.sleep(PAGE_DELAY)
                resp = fetch(f"{base}?kishu={quote_plus(m)}", referer=url, retries=1)
                req["n"] += 1
                items = parse_slump_list(resp.text) if len(resp.text) >= 2000 else []
                empty = 0 if items else empty + 1
                for it in items:
                    rows.append({"hall_name": hall, "play_date": day.isoformat(), "machine_name": m, **it})
                got += len(items) > 0
            print(f"  [{hall}] {day}: {got}/{len(machines)}機種")
            if got:
                done[hall] = day.isoformat()
    except SiteUnavailable as e:
        print(f"! {e}")
    finally:
        if rows:
            pq.write_table(pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False),
                           SHARDS_DIR / f"slump_{RUN_ID}.parquet", compression="zstd", compression_level=15)
        json.dump(prog, open(PROGRESS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"今回: {len(rows)}台日 / 今日の回数 {req['n']} / 間の日を取りこぼした店 {skipped_gap}")


if __name__ == "__main__":
    run()

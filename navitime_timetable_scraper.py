"""
NAVITIME(https://www.navitime.co.jp/)の時刻表ページから、埼玉・東京を
走る鉄道各社の駅ごとの発車時刻(平日/土曜/休日、両方向)を取得する。

robots.txt確認済み: ClaudeBotはCrawl-delay:10のみでアクセス自体は
明示的に許可されている。10秒間隔を守って巡回する。

このページは一見JavaScriptで時刻を描画しているように見えるが、実際は
平日/土曜/休日×上下2方向、計6パターン分の時刻データが最初のHTML
レスポンスに<div class="time-frame" data-hour=".." data-min="..">の
ような形で全て埋め込まれており、表示切り替えはCSSクラスの付け替え
だけで実現されている。そのためJS実行(Playwright等)は不要で、
requests+BeautifulSoupで完全に取得できる。

ただし方向(updown)はURLのクエリパラメータで指定する必要があり、
1回のリクエストでは指定した方向のコンテナしか埋まらない(反対方向の
コンテナは空のまま)。そのため1駅1路線につき2回(上り/下り)取得する。

発見方法:
  1. /diagram/company/{company_id}/ で会社ごとの路線一覧(lineId)を取得
  2. /diagram/stationList?lineId={line_id} で路線ごとの駅の並び順
     (隣接関係)と各駅のnode IDを取得
  3. /diagram/timetable?node={node_id}&lineId={line_id}&updown={0|1}
     で駅ごとの発車時刻を取得

駅間の所要時間(今回は取得しない)は、隣接駅同士の発車時刻を列車種別・
順序で突き合わせることで別途算出する想定(後続の分析スクリプトで行う)。
乗換時間(ホーム間の移動時間)はこのデータには含まれないため別途検討。

出力:
  data/navitime_lines.json          {line_id: {name, company}}
  data/navitime_stations.json       {line_id: [{node_id, name}, ...]} (駅順)
  data/navitime_shards/shard_{run_id}.parquet  このrunで取得した時刻表行
  data/navitime_shards/shard_{run_id}.json     このrunで完了したキー一覧

このデータはshardに分けて保存する(1本の時刻表parquetに毎回上書き
保存するのではない)。理由: GitHub Actionsのschedule実行は15分おき
だが、別の頻度の高いワークフロー(backfill.yml、20分おきに自データを
コミット)と同じリポジトリにコミットするため、`git pull --rebase`が
頻繁に衝突する。parquetはバイナリファイルでgitが3-way mergeできない
ため、衝突時は「このrunのコミットを丸ごと破棄してorigin側に合わせる」
という設計にしていた結果、1回目のローカルpush分(129件)以降は実質
何も進んでいなかった(5回の"成功"run全てが自分の成果を破棄していた)。
各runが自分のrun_idだけのユニークなファイル名に書き込むようにすれば、
他runのコミットと絶対に衝突しない(ファイルパス自体が別なので)。
後で読む側(セグメント所要時間算出・NetworkXグラフ構築)は
data/navitime_shards/*.parquetを全部concatして使う想定。
"""
import os
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
LINES_PATH = SCRIPT_DIR / "data" / "navitime_lines.json"
STATIONS_PATH = SCRIPT_DIR / "data" / "navitime_stations.json"
SHARDS_DIR = SCRIPT_DIR / "data" / "navitime_shards"

# GitHub Actions実行ごとにユニークなGITHUB_RUN_IDが振られる。
# これをshardファイル名に使うことで他runのコミットと絶対に衝突しない。
RUN_ID = os.environ.get("GITHUB_RUN_ID") or f"local{int(time.time())}"
SHARD_TIMETABLE_PATH = SHARDS_DIR / f"shard_{RUN_ID}.parquet"
SHARD_PROGRESS_PATH = SHARDS_DIR / f"shard_{RUN_ID}.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

# 埼玉・東京を走る鉄道会社のNAVITIME company_id
COMPANIES = {
    "00000004": "JR東日本",
    "00000035": "京王電鉄",
    "00000059": "埼玉新都市交通",
    "00000097": "相模鉄道",
    "00000092": "西武鉄道",
    "00000120": "東急電鉄",
    "00000121": "東京都交通局",
    "00000122": "東京臨海高速鉄道",
    "00000123": "東武鉄道",
    "00000113": "東京地下鉄(メトロ)",
    "00000173": "首都圏新都市鉄道",
}

# JR東日本は全国の路線を含むため、埼玉・東京周辺を走る路線名のみに絞る
JR_LINE_FILTER = [
    "山手線", "京浜東北線", "埼京線", "川越線", "中央線", "中央本線", "総武線",
    "高崎線", "東北本線", "宇都宮線", "常磐線", "京葉線", "南武線", "横浜線",
    "武蔵野線", "五日市線", "八高線", "青梅線", "成田線", "湘南新宿ライン",
    "横須賀線", "上野東京ライン",
]

# クロール間隔(robots.txtのCrawl-delay:10を守る)
CRAWL_DELAY = 10.5
TIME_BUDGET_SECONDS = 1500

_session = requests.Session()
_start_time = time.time()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - _start_time)


def fetch(url: str, retries: int = 3) -> requests.Response:
    last_exc = None
    for attempt in range(retries):
        try:
            resp = _session.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp
        except requests.exceptions.RequestException as e:
            last_exc = e
            time.sleep(5 + attempt * 5)
    raise last_exc


def load_json(path, default):
    import json
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, data):
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)


def discover_lines():
    lines = load_json(LINES_PATH, {})
    if lines:
        return lines

    for company_id, company_name in COMPANIES.items():
        resp = fetch(f"https://www.navitime.co.jp/diagram/company/{company_id}/")
        soup = BeautifulSoup(resp.text, "html.parser")
        count = 0
        for a in soup.select('a[href*="stationList?lineId="]'):
            m = re.search(r"lineId=(\d+)", a["href"])
            if not m:
                continue
            line_name = a.get_text(strip=True)
            if company_name == "JR東日本" and not any(f in line_name for f in JR_LINE_FILTER):
                continue
            lines[m.group(1)] = {"name": line_name, "company": company_name}
            count += 1
        print(f"{company_name}: {count}路線")
        time.sleep(CRAWL_DELAY)

    save_json(LINES_PATH, lines)
    return lines


def discover_stations(lines: dict):
    stations_by_line = load_json(STATIONS_PATH, {})
    remaining = [lid for lid in lines if lid not in stations_by_line]
    print(f"駅順未取得の路線: {len(remaining)}/{len(lines)}")

    for i, line_id in enumerate(remaining):
        if time_left() <= 0:
            print(f"時間切れ、駅順取得は{i}/{len(remaining)}路線まで")
            break
        try:
            resp = fetch(f"https://www.navitime.co.jp/diagram/stationList?lineId={line_id}")
            soup = BeautifulSoup(resp.text, "html.parser")
            stations = []
            seen_nodes = set()
            for a in soup.select(f'a[href*="lineId={line_id}"]'):
                m = re.search(r"node=(\d+)", a["href"])
                if not m or m.group(1) in seen_nodes:
                    continue
                seen_nodes.add(m.group(1))
                stations.append({"node_id": m.group(1), "name": a.get_text(strip=True)})
            stations_by_line[line_id] = stations
            print(f"  [{i+1}/{len(remaining)}] {lines[line_id]['name']}: {len(stations)}駅")
        except Exception as e:
            print(f"  [{i+1}/{len(remaining)}] {line_id} 失敗: {e}")
        time.sleep(CRAWL_DELAY)

    save_json(STATIONS_PATH, stations_by_line)
    return stations_by_line


def parse_timetable(html: str, node_id: str, line_id: str, updown: str):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for day_type in ["weekday", "saturday", "holiday"]:
        frame = soup.find(id=f"{day_type}-{updown}")
        if not frame:
            continue
        for tf in frame.select(".time-frame"):
            d = tf.attrs
            rows.append({
                "station_node_id": node_id,
                "line_id": line_id,
                "updown": updown,
                "day_type": day_type,
                "hour": d.get("data-hour", ""),
                "minute": d.get("data-min", ""),
                "train_type": d.get("data-name", ""),
                "dest_name": d.get("data-dest", ""),
                "dest_node_id": d.get("data-arv-node", ""),
                "is_first": d.get("data-first", "") == "first",
            })
    return rows


def load_all_done() -> set:
    """全shard(過去の全run分)から完了済みキーを集める。"""
    done = set()
    if SHARDS_DIR.exists():
        for p in SHARDS_DIR.glob("shard_*.json"):
            done.update(load_json(p, {"done": []}).get("done", []))
    return done


def scrape_timetables(lines: dict, stations_by_line: dict):
    done = load_all_done()

    targets = []
    for line_id, stations in stations_by_line.items():
        for st in stations:
            for updown in ["0", "1"]:
                key = f"{st['node_id']}:{line_id}:{updown}"
                if key not in done:
                    targets.append((st["node_id"], line_id, updown, key))

    print(f"対象(駅×路線×方向): {len(targets)}件(うち完了済み{len(done)}件)")

    all_rows = []
    shard_done = []
    for i, (node_id, line_id, updown, key) in enumerate(targets):
        if time_left() <= 0:
            print(f"時間切れ、{i}/{len(targets)}件まで処理")
            break
        try:
            resp = fetch(f"https://www.navitime.co.jp/diagram/timetable?node={node_id}&lineId={line_id}&updown={updown}")
            rows = parse_timetable(resp.text, node_id, line_id, updown)
            all_rows.extend(rows)
            shard_done.append(key)
            if (i + 1) % 20 == 0:
                print(f"  [{i+1}/{len(targets)}] 処理済み(直近: {len(rows)}件)")
        except Exception as e:
            print(f"  [{i+1}/{len(targets)}] {key} 失敗: {e}")
        time.sleep(CRAWL_DELAY)

        # 100件ごとに中間保存(途中で止まっても進捗とデータが残るように)
        if (i + 1) % 100 == 0:
            _save_shard(all_rows, shard_done)
            all_rows = []

    _save_shard(all_rows, shard_done)
    print(f"\nこのrunで完了: {len(shard_done)}件 / 全体完了: {len(done) + len(shard_done)}件")


def _save_shard(rows, shard_done):
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)
    if rows:
        new_df = pd.DataFrame(rows)
        if SHARD_TIMETABLE_PATH.exists():
            existing = pq.read_table(SHARD_TIMETABLE_PATH).to_pandas()
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        table = pa.Table.from_pandas(combined, preserve_index=False)
        pq.write_table(table, SHARD_TIMETABLE_PATH, compression="zstd", compression_level=15)
        print(f"  保存: {SHARD_TIMETABLE_PATH.name} ({len(combined)}行)")
    save_json(SHARD_PROGRESS_PATH, {"done": shard_done})


def run():
    print("=== フェーズ1: 路線一覧の発見 ===")
    lines = discover_lines()
    print(f"対象路線数: {len(lines)}")

    print("\n=== フェーズ2: 駅順の発見 ===")
    stations_by_line = discover_stations(lines)

    print("\n=== フェーズ3: 発車時刻の取得 ===")
    scrape_timetables(lines, stations_by_line)


if __name__ == "__main__":
    run()

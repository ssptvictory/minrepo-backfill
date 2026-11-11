"""
NAVITIMEから取得した時刻表データ(data/navitime_shards/*.parquet)から、
隣接駅同士の所要時間(駅間の走行時分)を算出する。

ダイヤの発車時刻そのものは改正ごとに変わるため長持ちしないが、駅間の
走行時分は線路の物理的な距離・速度制限に由来するためほぼ変わらない。
そのため全データを常に最新に保つ必要はなく、今回のスナップショットから
一度だけ算出した所要時間を使い続ける想定。

算出方法: 同じ路線・方向・曜日種別で隣接する駅Aの各発車時刻に対し、
駅Bの「直後(window分以内)の最も近い発車時刻」を対応させ、その差分の
最頻値(mode)を所要時間とする(駅間で列車の始発・終着があり本数が
一致しないケースに対応するため、位置ベースの単純な対応付けはしない)。

注意点: 快速/各駅停車が混在する駅間では、通過駅により列車の対応が
ズレて誤った差分が混ざることがある(検証時に南浦和→蕨で実際3分程度の
区間が1分と算出される例を確認)。mode_confidence(最頻値が全サンプル
中どれだけの割合を占めるか)が低い区間は信頼度が低いとみなすこと。

出力: data/navitime_segment_times.parquet
  列: line_id, line_name, updown, day_type,
      station_a_node, station_a_name, station_b_node, station_b_name,
      segment_minutes, sample_count, mode_confidence
"""
import glob
import json
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
SHARDS_GLOB = str(SCRIPT_DIR / "data" / "navitime_shards" / "shard_*.parquet")
LINES_PATH = SCRIPT_DIR / "data" / "navitime_lines.json"
STATIONS_PATH = SCRIPT_DIR / "data" / "navitime_stations.json"
OUT_PATH = SCRIPT_DIR / "data" / "navitime_segment_times.parquet"

MATCH_WINDOW_MIN = 10
DAY_TYPES = ["weekday", "saturday", "holiday"]


def load_timetable() -> pd.DataFrame:
    dfs = [pd.read_parquet(f) for f in glob.glob(SHARDS_GLOB)]
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(
        subset=["station_node_id", "line_id", "updown", "day_type", "hour", "minute", "dest_node_id"]
    )
    df["minutes"] = df["hour"].astype(int) * 60 + df["minute"].astype(int)
    return df


def derive_segment(times_a, times_b, window=MATCH_WINDOW_MIN):
    """駅Aの各発車時刻に対し、駅Bの直後(window分以内)の最も近い発車時刻との
    差分を集め、最頻値と信頼度(最頻値の占める割合)を返す。"""
    times_b_sorted = sorted(times_b)
    diffs = []
    for ta in times_a:
        cands = [tb for tb in times_b_sorted if ta < tb <= ta + window]
        if cands:
            diffs.append(min(cands) - ta)
    if not diffs:
        return None, 0, 0.0
    counts = Counter(diffs)
    minutes, mode_count = counts.most_common(1)[0]
    return minutes, len(diffs), mode_count / len(diffs)


def run():
    print("時刻表データ読み込み中...")
    df = load_timetable()
    print(f"読み込み行数: {len(df)}")

    lines = json.load(open(LINES_PATH, encoding="utf-8"))
    stations_by_line = json.load(open(STATIONS_PATH, encoding="utf-8"))

    # (station_node_id, line_id, updown, day_type) -> list[minutes]
    grouped = df.groupby(["station_node_id", "line_id", "updown", "day_type"])["minutes"].apply(list)

    rows = []
    skipped_no_data = 0
    for line_id, stations in stations_by_line.items():
        line_name = lines.get(line_id, {}).get("name", line_id)
        for i in range(len(stations) - 1):
            st_a, st_b = stations[i], stations[i + 1]
            for updown in ["0", "1"]:
                for day_type in DAY_TYPES:
                    key_a = (st_a["node_id"], line_id, updown, day_type)
                    key_b = (st_b["node_id"], line_id, updown, day_type)
                    if key_a not in grouped.index or key_b not in grouped.index:
                        skipped_no_data += 1
                        continue
                    minutes, n, confidence = derive_segment(grouped[key_a], grouped[key_b])
                    if minutes is None:
                        skipped_no_data += 1
                        continue
                    rows.append({
                        "line_id": line_id,
                        "line_name": line_name,
                        "updown": updown,
                        "day_type": day_type,
                        "station_a_node": st_a["node_id"],
                        "station_a_name": st_a["name"],
                        "station_b_node": st_b["node_id"],
                        "station_b_name": st_b["name"],
                        "segment_minutes": minutes,
                        "sample_count": n,
                        "mode_confidence": round(confidence, 3),
                    })

    out = pd.DataFrame(rows)
    print(f"\n算出できたセグメント数: {len(out)}")
    print(f"データ不足でスキップ: {skipped_no_data}")
    if len(out):
        print(f"信頼度(mode_confidence)分布:\n{out['mode_confidence'].describe()}")
        low_conf = out[out["mode_confidence"] < 0.5]
        print(f"信頼度50%未満の区間: {len(low_conf)}件")

    table = pa.Table.from_pandas(out, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

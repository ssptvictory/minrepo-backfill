"""
NAVITIMEから取得した時刻表データ(data/navitime_shards/*.parquet)から、
駅・路線・方向・曜日種別・時間帯ごとの運行間隔(本数の間隔)を算出する。

駅間の所要時間(derive_segment_times.py)だけでは「乗ってからどれだけ
かかるか」しかわからず、「駅に着いてから電車が来るまでどれだけ待つか」
が欠けている。実際の移動時間は 待ち時間(間隔の半分程度を期待値とする
のが一般的) + 乗車時間 + 乗換待ち なので、間隔データも必要。

時間帯は通勤ラッシュ等で本数が大きく変わるため、1日平均ではなく
時間帯別に分けて算出する。

出力: data/navitime_train_frequency.parquet
  列: station_node_id, station_name, line_id, line_name, updown, day_type,
      time_band, avg_interval_minutes, median_interval_minutes, sample_count
"""
import glob
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
SHARDS_GLOB = str(SCRIPT_DIR / "data" / "navitime_shards" / "shard_*.parquet")
LINES_PATH = SCRIPT_DIR / "data" / "navitime_lines.json"
STATIONS_PATH = SCRIPT_DIR / "data" / "navitime_stations.json"
OUT_PATH = SCRIPT_DIR / "data" / "navitime_train_frequency.parquet"

# (開始時, 終了時, 名前) 終了時は含まない
TIME_BANDS = [
    (0, 7, "早朝深夜"),
    (7, 9, "朝ラッシュ"),
    (9, 17, "日中"),
    (17, 19, "夕ラッシュ"),
    (19, 24, "夜間"),
]


def load_timetable() -> pd.DataFrame:
    dfs = [pd.read_parquet(f) for f in glob.glob(SHARDS_GLOB)]
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(
        subset=["station_node_id", "line_id", "updown", "day_type", "hour", "minute", "dest_node_id"]
    )
    df["minutes"] = df["hour"].astype(int) * 60 + df["minute"].astype(int)
    return df


def band_for(hour: int) -> str:
    for start, end, name in TIME_BANDS:
        if start <= hour < end:
            return name
    return "夜間"


def run():
    print("時刻表データ読み込み中...")
    df = load_timetable()
    df["time_band"] = df["hour"].astype(int).apply(band_for)

    stations_by_line = json.load(open(STATIONS_PATH, encoding="utf-8"))
    lines = json.load(open(LINES_PATH, encoding="utf-8"))
    name_by_node = {}
    for line_id, stations in stations_by_line.items():
        for s in stations:
            name_by_node[(s["node_id"], line_id)] = s["name"]

    rows = []
    group_cols = ["station_node_id", "line_id", "updown", "day_type", "time_band"]
    for (node, line_id, updown, day_type, time_band), g in df.groupby(group_cols):
        times = sorted(g["minutes"].tolist())
        if len(times) < 2:
            continue
        intervals = [b - a for a, b in zip(times[:-1], times[1:])]
        rows.append({
            "station_node_id": node,
            "station_name": name_by_node.get((node, line_id), ""),
            "line_id": line_id,
            "line_name": lines.get(line_id, {}).get("name", line_id),
            "updown": updown,
            "day_type": day_type,
            "time_band": time_band,
            "avg_interval_minutes": round(sum(intervals) / len(intervals), 1),
            "median_interval_minutes": sorted(intervals)[len(intervals) // 2],
            "sample_count": len(intervals),
        })

    out = pd.DataFrame(rows)
    print(f"算出できた(駅×路線×方向×曜日×時間帯)の組み合わせ数: {len(out)}")
    if len(out):
        print(out["avg_interval_minutes"].describe())

    table = pa.Table.from_pandas(out, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

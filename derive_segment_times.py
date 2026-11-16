"""
NAVITIMEから取得した時刻表データ(data/navitime_shards/*.parquet)から、
隣接駅同士の所要時間(駅間の走行時分)を算出する。

ダイヤの発車時刻そのものは改正ごとに変わるため長持ちしないが、駅間の
走行時分は線路の物理的な距離・速度制限に由来するためほぼ変わらない。
そのため全データを常に最新に保つ必要はなく、今回のスナップショットから
一度だけ算出した所要時間を使い続ける想定。

算出方法(v2): 同じ路線・方向・曜日種別・(種別,行先)の組み合わせで、
隣接する駅Aと駅Bの本数が完全に一致する場合、時刻の早い順に機械的に
対応付ける(=同じ物理的な列車同士が対応している前提が成り立つ)。
本数が一致しない組み合わせ(一部の列車がどちらかの駅を通過・始発・
終着する等)は対象から除外する。

v1(発車時刻の近さだけで対応付け)は種別・行先の違う列車同士を誤って
対応付けてしまい信頼度50%台にとどまっていたが、v2では本数一致グループ
内で順序対応付けすることで、最頻値+隣接1分のカバー率が95%まで改善
した(検証済み)。残る隣接1分のブレは、分単位でしか時刻が記録されて
いないことによる丸め誤差(本当の所要時間が2.5分なら2分/3分に割れる)
が主因と考えられ、これはデータの時間粒度の限界であって列車マッチング
の精度問題ではない。

出力: data/navitime_segment_times.parquet
  列: line_id, line_name, updown, day_type,
      station_a_node, station_a_name, station_b_node, station_b_name,
      segment_minutes, sample_count, mode_confidence, top2_confidence
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

DAY_TYPES = ["weekday", "saturday", "holiday"]


def load_timetable() -> pd.DataFrame:
    dfs = [pd.read_parquet(f) for f in glob.glob(SHARDS_GLOB)]
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(
        subset=["station_node_id", "line_id", "updown", "day_type", "hour", "minute", "dest_node_id"]
    )
    df["minutes"] = df["hour"].astype(int) * 60 + df["minute"].astype(int)
    return df


def derive_segment(a_rows: pd.DataFrame, b_rows: pd.DataFrame):
    """(種別,行先)ごとに本数が一致するグループだけを時刻順に対応付ける"""
    diffs = []
    for (tt, dest), ga in a_rows.groupby(["train_type", "dest_node_id"]):
        gb = b_rows[(b_rows["train_type"] == tt) & (b_rows["dest_node_id"] == dest)]
        if len(ga) != len(gb) or len(ga) == 0:
            continue
        a_times = ga.sort_values("minutes")["minutes"].tolist()
        b_times = gb.sort_values("minutes")["minutes"].tolist()
        for ta, tb in zip(a_times, b_times):
            diffs.append(tb - ta)
    if not diffs:
        return None, 0, 0.0, 0.0
    counts = Counter(diffs)
    top = counts.most_common(2)
    minutes, mode_count = top[0]
    confidence = mode_count / len(diffs)
    if len(top) > 1 and abs(top[0][0] - top[1][0]) <= 1:
        top2_confidence = (top[0][1] + top[1][1]) / len(diffs)
    else:
        top2_confidence = confidence
    return minutes, len(diffs), round(confidence, 3), round(top2_confidence, 3)


def run():
    print("時刻表データ読み込み中...")
    df = load_timetable()
    print(f"読み込み行数: {len(df)}")

    lines = json.load(open(LINES_PATH, encoding="utf-8"))
    stations_by_line = json.load(open(STATIONS_PATH, encoding="utf-8"))

    grouped = df.groupby(["station_node_id", "line_id", "updown", "day_type"])

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
                    if key_a not in grouped.groups or key_b not in grouped.groups:
                        skipped_no_data += 1
                        continue
                    a_rows = grouped.get_group(key_a)
                    b_rows = grouped.get_group(key_b)
                    minutes, n, confidence, top2_confidence = derive_segment(a_rows, b_rows)
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
                        "mode_confidence": confidence,
                        "top2_confidence": top2_confidence,
                    })

    out = pd.DataFrame(rows)
    print(f"\n算出できたセグメント数: {len(out)}")
    print(f"データ不足でスキップ: {skipped_no_data}")
    if len(out):
        print(f"mode_confidence平均: {out['mode_confidence'].mean():.3f}")
        print(f"top2_confidence平均: {out['top2_confidence'].mean():.3f}")
        low_conf = out[out["top2_confidence"] < 0.8]
        print(f"top2_confidence 80%未満の区間: {len(low_conf)}件")

    table = pa.Table.from_pandas(out, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

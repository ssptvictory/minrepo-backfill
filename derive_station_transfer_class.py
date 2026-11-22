"""
各駅の乗り入れ線路数・運営会社の組み合わせから、乗換時間の扱いを分類する。

- 1線のみ: 乗換の概念が発生しない(transfer_na)。改札→ホームの徒歩時間は
  別の指標として扱う(今回は対象外)。
- 2線、同一会社: 改札を出ずに済むことが多く、短め(1〜2分)と仮定できる。
- 2線、会社をまたぐ: 改札の出入りが必要なことが多く、長め(3〜5分)と
  仮定できる。
- 3線以上: 構造が複雑でこの単純な前提では扱えないため、個別にデータ収集
  が必要な駅として明示する(本スクリプトでは分類のみ行い、実際の時間は
  別途調査する)。

出力: data/station_transfer_class.parquet
  列: station_node, station_name, line_count, company_count, classification,
      assumed_minutes_min, assumed_minutes_max
"""
import json
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
STATIONS_PATH = SCRIPT_DIR / "data" / "navitime_stations.json"
LINES_PATH = SCRIPT_DIR / "data" / "navitime_lines.json"
OUT_PATH = SCRIPT_DIR / "data" / "station_transfer_class.parquet"

ASSUMED_MINUTES = {
    "transfer_na": (None, None),
    "same_company": (1, 2),
    "cross_company": (3, 5),
    "needs_data": (None, None),
}


def classify(line_ids, lines):
    n = len(line_ids)
    companies = {lines.get(lid, {}).get("company") for lid in line_ids}
    if n <= 1:
        return "transfer_na"
    if n == 2:
        return "same_company" if len(companies) == 1 else "cross_company"
    return "needs_data"


def run():
    stations = json.load(open(STATIONS_PATH, encoding="utf-8"))
    lines = json.load(open(LINES_PATH, encoding="utf-8"))

    line_ids_by_node = defaultdict(set)
    name_by_node = {}
    for line_id, sts in stations.items():
        for s in sts:
            line_ids_by_node[s["node_id"]].add(line_id)
            name_by_node[s["node_id"]] = s["name"]

    rows = []
    for node, line_ids in line_ids_by_node.items():
        cls = classify(line_ids, lines)
        lo, hi = ASSUMED_MINUTES[cls]
        companies = {lines.get(lid, {}).get("company") for lid in line_ids}
        rows.append({
            "station_node": node,
            "station_name": name_by_node[node],
            "line_count": len(line_ids),
            "company_count": len(companies),
            "classification": cls,
            "assumed_minutes_min": lo,
            "assumed_minutes_max": hi,
        })

    df = pd.DataFrame(rows).sort_values(["classification", "line_count"], ascending=[True, False])
    print(df["classification"].value_counts())
    print()
    print("=== needs_data(3線以上、個別調査が必要) ===")
    print(df[df["classification"] == "needs_data"][["station_name", "line_count"]].to_string(index=False))

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"\n保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

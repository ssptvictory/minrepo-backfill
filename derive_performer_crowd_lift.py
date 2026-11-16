"""
演者/タグごとに、並び人数(anoslot.moeの実測queue_count)がどれくらい
変動するかを算出する。「イベント日/取材日」で一括りにすると、有名演者と
無名演者が混ざって平均化されてしまうため、performer_tag_lift.py(mean_z
対象)と同じ手法を、並び人数に対しても適用する。

各ホールの「通常の並び人数」を基準にして、taggedな日の並び人数が
その基準からどれくらい(比率で)増えるかをlift値として算出し、サンプル
数が少ないタグはベイズ的縮小で0(=変化なし)方向に寄せる。

出力: data/performer_crowd_lift.parquet
  列: entity, raw_lift_ratio, shrunk_lift_ratio, n_samples
  (lift_ratioは「平常時の並び人数に対する倍率」。1.0なら変化なし、
   1.5なら平常の1.5倍、という意味)
"""
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
QUEUE_PATH = SCRIPT_DIR / "data" / "anoslot_queue.parquet"
HALLS_CSV = SCRIPT_DIR / "halls_export.csv"
OUT_PATH = SCRIPT_DIR / "data" / "performer_crowd_lift.parquet"

SHRINKAGE_K = 20
MIN_SAMPLES = 5


def load_name_map():
    halls = pd.read_csv(HALLS_CSV)
    name_map = {n: n for n in halls["name"]}
    name_map.update({m: n for m, n in zip(halls["minrepo_name"], halls["name"])})
    return name_map


def extract_entity(title: str):
    m = re.search(r"^(.*?)(来店取材|来店情報|来店＆取材|来店|取材)$", str(title))
    return m.group(1).strip() if m else None


def load_entity_events(name_map):
    pworld = pd.read_parquet(SCRIPT_DIR / "data" / "pworld_interviews.parquet")[["hall_name", "event_date", "title"]]
    slotana = pd.read_parquet(SCRIPT_DIR / "data" / "slotana_schedule.parquet")[["hall_name", "event_date", "title"]]
    combo = pd.concat([pworld, slotana], ignore_index=True)
    combo["hall_name"] = combo["hall_name"].map(name_map).fillna(combo["hall_name"])
    combo["event_date"] = pd.to_datetime(combo["event_date"]).dt.strftime("%Y-%m-%d")
    combo["entity"] = combo["title"].apply(extract_entity)
    combo = combo.dropna(subset=["entity"])
    combo = combo[combo["entity"].str.len() > 0]
    return combo.drop_duplicates(subset=["hall_name", "event_date", "entity"])


def run():
    print("並び人数データ読み込み中...")
    queue = pd.read_parquet(QUEUE_PATH)

    # ホールごとの「平常時」の並び人数基準値(中央値。平均は極端な日に引っ張られやすいため)
    hall_baseline = queue.groupby("hall_name")["queue_count"].median().rename("baseline_queue")
    queue = queue.merge(hall_baseline, on="hall_name", how="left")
    queue = queue[queue["baseline_queue"] > 0]
    queue["ratio"] = queue["queue_count"] / queue["baseline_queue"]

    print("取材予定からタグ抽出中...")
    name_map = load_name_map()
    events = load_entity_events(name_map)

    merged = events.merge(queue, left_on=["hall_name", "event_date"], right_on=["hall_name", "date"], how="inner")
    print(f"マッチした(タグ,並び人数)件数: {len(merged)}")

    stats = merged.groupby("entity")["ratio"].agg(raw_lift_ratio="mean", n_samples="count").reset_index()
    stats = stats[stats["n_samples"] >= MIN_SAMPLES]
    # 1.0(変化なし)を基準に、サンプルが少ないタグは1.0方向へ縮小する
    stats["shrunk_lift_ratio"] = 1 + (stats["raw_lift_ratio"] - 1) * stats["n_samples"] / (stats["n_samples"] + SHRINKAGE_K)
    stats = stats.sort_values("shrunk_lift_ratio", ascending=False)

    print(f"\n対象タグ数(サンプル{MIN_SAMPLES}以上): {len(stats)}")
    print("=== shrunk_lift_ratio上位10(並びが増えるタグ) ===")
    print(stats.head(10).to_string())
    print("=== shrunk_lift_ratio下位10(並びが減るタグ) ===")
    print(stats.tail(10).to_string())

    table = pa.Table.from_pandas(stats, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"\n保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

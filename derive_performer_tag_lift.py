"""
P-WORLD/スロパイの取材予定タイトルから演者/企画タグを抽出し、各タグが
実際にどれくらい実績(mean_z)を押し上げているか(lift)を算出する。

「取材予定の有無」という単純なフラグは、良い演者と悪い演者/企画が
混ざって平均化され、ほぼ無相関に見えることをEDAで確認済み(詳細は
eda_hall_strength.py)。個別のタグに分解すると+0.09〜-0.06まで
大きくばらつくことも確認済み。

サンプル数が少ないタグの生のlift値は信頼できないため、ベイズ的縮小
(James-Stein型、n/(n+k)で0に向けて縮小)を適用する。kは「このくらいの
サンプル数があれば半分信頼する」という値で、ここでは20とした。

出力: data/performer_tag_lift.parquet
  列: entity, raw_lift, shrunk_lift, n_samples
"""
import glob
import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
DAILY_DIR = SCRIPT_DIR / "data" / "daily_unit_data"
HALLS_CSV = SCRIPT_DIR / "halls_export.csv"
OUT_PATH = SCRIPT_DIR / "data" / "performer_tag_lift.parquet"

SHRINKAGE_K = 20
MIN_SAMPLES = 5
TRACKED_SINCE = "2026-02-02"  # スロパイの実質的な追跡開始日(EDAで確認済み)


def load_name_map():
    halls = pd.read_csv(HALLS_CSV)
    name_map = {n: n for n in halls["name"]}
    name_map.update({m: n for m, n in zip(halls["minrepo_name"], halls["name"])})
    return name_map


def load_hall_day_z():
    name_map = load_name_map()
    dfs = [pd.read_parquet(f) for f in glob.glob(str(DAILY_DIR / "*.parquet"))]
    df = pd.concat(dfs, ignore_index=True)
    df["hall_name"] = df["hall_name"].map(name_map).fillna(df["hall_name"])
    df["play_date"] = pd.to_datetime(df["play_date"])

    grp = df.groupby(["hall_name", "machine_name", "unit_number"])["diff"]
    mean = grp.transform("mean")
    std = grp.transform("std").replace(0, pd.NA)
    df["z"] = (df["diff"] - mean) / std

    hall_day = df.groupby(["hall_name", "play_date"])["z"].mean().reset_index()
    hall_day = hall_day.rename(columns={"z": "mean_z"})
    return hall_day[hall_day["play_date"] >= TRACKED_SINCE]


def extract_entity(title: str):
    m = re.search(r"^(.*?)(来店取材|来店情報|来店＆取材|来店|取材)$", str(title))
    return m.group(1).strip() if m else None


def load_entity_events(name_map):
    pworld = pd.read_parquet(SCRIPT_DIR / "data" / "pworld_interviews.parquet")[["hall_name", "event_date", "title"]]
    slotana = pd.read_parquet(SCRIPT_DIR / "data" / "slotana_schedule.parquet")[["hall_name", "event_date", "title"]]
    combo = pd.concat([pworld, slotana], ignore_index=True)
    combo["hall_name"] = combo["hall_name"].map(name_map).fillna(combo["hall_name"])
    combo["event_date"] = pd.to_datetime(combo["event_date"])
    combo["entity"] = combo["title"].apply(extract_entity)
    combo = combo.dropna(subset=["entity"])
    combo = combo[combo["entity"].str.len() > 0]
    return combo.drop_duplicates(subset=["hall_name", "event_date", "entity"])


def run():
    print("ホール×日付のmean_z算出中...")
    name_map = load_name_map()
    hall_day = load_hall_day_z()
    print(f"対象ホール×日付: {len(hall_day)}")

    print("取材予定からタグ抽出中...")
    events = load_entity_events(name_map)

    merged = events.merge(hall_day, left_on=["hall_name", "event_date"], right_on=["hall_name", "play_date"], how="inner")
    print(f"マッチした(タグ,実績)件数: {len(merged)}")

    stats = merged.groupby("entity")["mean_z"].agg(raw_lift="mean", n_samples="count").reset_index()
    stats = stats[stats["n_samples"] >= MIN_SAMPLES]
    stats["shrunk_lift"] = stats["raw_lift"] * stats["n_samples"] / (stats["n_samples"] + SHRINKAGE_K)
    stats = stats.sort_values("shrunk_lift", ascending=False)

    print(f"\n対象タグ数(サンプル{MIN_SAMPLES}以上): {len(stats)}")
    print("=== shrunk_lift上位10 ===")
    print(stats.head(10).to_string())
    print("=== shrunk_lift下位10 ===")
    print(stats.tail(10).to_string())

    table = pa.Table.from_pandas(stats, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"\n保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

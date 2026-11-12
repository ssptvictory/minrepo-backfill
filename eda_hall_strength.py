"""
ホール強さスコアの特徴量候補について、EDA(簡単な集計)で効果があるか
確認する。本格的な学習(Lasso等)に進む前に、まず各特徴量について
「その特徴がある日は翌日/当日の実績がどれくらい違うか」を単純比較する。

出力(当日の実績との比較。翌日予測ではなく、まず当日の相関があるかを
先に見る):
  - 機種別z-score(台ごとの長期平均・標準偏差に対する当日のdiffの位置)
  - ホール全体のbreadth(その日、好調だった台の割合)
  - 取材予定(interview_matched.parquet)の有無・confidence tier別の比較
  - 曜日・ゾロ目日の比較
"""
import glob
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
DAILY_DIR = SCRIPT_DIR / "data" / "daily_unit_data"
HALLS_CSV = SCRIPT_DIR / "halls_export.csv"
INTERVIEW_PATH = SCRIPT_DIR / "data" / "interview_matched.parquet"


def load_name_map():
    halls = pd.read_csv(HALLS_CSV)
    name_map = {n: n for n in halls["name"]}
    name_map.update({m: n for m, n in zip(halls["minrepo_name"], halls["name"])})
    return name_map


def load_daily_unit_data(name_map):
    dfs = []
    for f in sorted(glob.glob(str(DAILY_DIR / "*.parquet"))):
        dfs.append(pd.read_parquet(f))
    df = pd.concat(dfs, ignore_index=True)
    df["hall_name"] = df["hall_name"].map(name_map).fillna(df["hall_name"])
    df["play_date"] = pd.to_datetime(df["play_date"])
    return df


def compute_zscore(df):
    # 台ごと(ホール×機種×台番号)の長期平均・標準偏差に対するdiffのz-score
    grp = df.groupby(["hall_name", "machine_name", "unit_number"])["diff"]
    mean = grp.transform("mean")
    std = grp.transform("std").replace(0, pd.NA)
    df["z"] = (df["diff"] - mean) / std
    return df


def to_hall_day(df):
    # ホール×日付に集約: 平均z、breadth(z>0の台の割合)
    g = df.groupby(["hall_name", "play_date"])["z"]
    out = g.agg(mean_z="mean", breadth=lambda s: (s > 0).mean(), n_units="count").reset_index()
    return out


def run():
    print("名寄せ台帳読み込み中...")
    name_map = load_name_map()

    print("daily_unit_data読み込み中(時間がかかります)...")
    df = load_daily_unit_data(name_map)
    print(f"行数: {len(df)}, 期間: {df['play_date'].min()} 〜 {df['play_date'].max()}")

    df = compute_zscore(df)
    hall_day = to_hall_day(df)
    print(f"ホール×日付の件数: {len(hall_day)}")

    print("\n取材予定データ読み込み中...")
    interview = pd.read_parquet(INTERVIEW_PATH)
    interview["event_date"] = pd.to_datetime(interview["event_date"])
    interview_keys = set(zip(interview["hall_name"], interview["event_date"]))
    week_before_keys = set(zip(
        interview[interview["confidence_week_before"]]["hall_name"],
        interview[interview["confidence_week_before"]]["event_date"],
    ))
    two_days_keys = set(zip(
        interview[interview["confidence_2days_before"]]["hall_name"],
        interview[interview["confidence_2days_before"]]["event_date"],
    ))

    hall_day["has_interview"] = [
        (h, d) in interview_keys for h, d in zip(hall_day["hall_name"], hall_day["play_date"])
    ]
    hall_day["interview_week_before"] = [
        (h, d) in week_before_keys for h, d in zip(hall_day["hall_name"], hall_day["play_date"])
    ]
    hall_day["interview_2days_before"] = [
        (h, d) in two_days_keys for h, d in zip(hall_day["hall_name"], hall_day["play_date"])
    ]

    print("\n=== 取材予定の有無での比較 ===")
    g = hall_day.groupby("has_interview")[["mean_z", "breadth"]].agg(["mean", "count"])
    print(g)

    print("\n=== confidence_week_before での比較(取材予定ありの中で) ===")
    sub = hall_day[hall_day["has_interview"]]
    g2 = sub.groupby("interview_week_before")[["mean_z", "breadth"]].agg(["mean", "count"])
    print(g2)

    print("\n=== confidence_2days_before での比較(取材予定ありの中で) ===")
    g3 = sub.groupby("interview_2days_before")[["mean_z", "breadth"]].agg(["mean", "count"])
    print(g3)

    print("\n=== 曜日別の比較 ===")
    hall_day["dow"] = hall_day["play_date"].dt.day_name()
    print(hall_day.groupby("dow")[["mean_z", "breadth"]].mean())

    print("\n=== ゾロ目日(5の倍数日)での比較 ===")
    hall_day["is_zorome"] = hall_day["play_date"].dt.day % 5 == 0
    g4 = hall_day.groupby("is_zorome")[["mean_z", "breadth"]].agg(["mean", "count"])
    print(g4)

    hall_day.to_parquet(SCRIPT_DIR / "data" / "eda_hall_day_features.parquet", compression="zstd")
    print("\n保存: data/eda_hall_day_features.parquet")


if __name__ == "__main__":
    run()

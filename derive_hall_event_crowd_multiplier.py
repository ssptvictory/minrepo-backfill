"""
ryome_crowd_sizes.parquet(ryomemoblogの記事から抽出した、ホールごとの
「通常日」「特定日」の並び人数レンジ)から、ホール単位の「特定日は
通常の何倍混むか」という乗数を算出する。

anoslot_queueの実測並び人数と違い日次データではなく、演者/タグにも
紐づけられないため、performer_crowd_lift.parquetの代わりにはならない。
ただしanoslot_queueのカバーが及ばないホールでも、「このホールは特定日に
どれくらい混みやすいか」という粗い目安として、hall_event_days.parquet
の特定日フラグと組み合わせて使う(演者別liftが使えない時のフォール
バック)。

出力: data/hall_event_crowd_multiplier.parquet
  列: hall_name, normal_mid, special_mid, multiplier, n_normal, n_special
"""
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
RYOME_CROWD_PATH = SCRIPT_DIR / "data" / "ryome_crowd_sizes.parquet"
OUT_PATH = SCRIPT_DIR / "data" / "hall_event_crowd_multiplier.parquet"


def run():
    df = pd.read_parquet(RYOME_CROWD_PATH)
    df["mid"] = (df["crowd_min"] + df["crowd_max"]) / 2
    df = df.dropna(subset=["mid"])

    is_normal = df["situation"].str.contains("通常", na=False)
    normal = df[is_normal].groupby("hall_name")["mid"].agg(normal_mid="mean", n_normal="size")
    special = df[~is_normal].groupby("hall_name")["mid"].agg(special_mid="mean", n_special="size")

    stats = normal.join(special, how="inner").reset_index()
    stats["multiplier"] = stats["special_mid"] / stats["normal_mid"]

    print(f"対象ホール数: {len(stats)}")
    print(stats["multiplier"].describe())

    table = pa.Table.from_pandas(stats, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

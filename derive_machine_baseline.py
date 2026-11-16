"""
daily_unit_data全体から機種ごとの全国統計(平均差枚・標準偏差・サンプル数・
導入店舗数)を算出し、軽い派生テーブルとして保存する。

この値は店舗横断で見た機種そのものの「荒さ」(コイン単価相当の volatility)
と「全国的な平均の出方」を表す。各店舗の強さを判断する際、店舗側の
観測値をこの値で標準化(t値化)するためのベースラインとして使う。

回転数(games)についても同じ理由で機種ごとの平均・標準偏差を持たせる。
機種間で最大回転数(ゲーム性による速度差)が全く異なるため生の回転数は
比較できないが、diffと同じく「その機種自身の平均/標準偏差」でz化すれば
機種差が吸収される。読みやすい機種で早期に見切られて回転数が落ちる
ケースも、ノイズではなく「その日その台の調子が悪かった」という正しい
信号としてz値にそのまま反映されるため、別途補正は不要
(diff_zとgames_zの相関係数を検証済み: 0.24で正の相関はあるが1.0には
遠く、回転数はdiffとは別の情報を持つ特徴量として使える)。

生データ(daily_unit_data、3000万行超)を分析の都度スキャンするのは
計算コストが大きいため、ここで一度計算して軽いテーブルとして保存し、
週1回程度の頻度で再計算し直す想定(機種の全国的な傾向が1日で大きく
変わることはないため)。

出力: data/machine_baseline_stats.parquet
  列: machine_name, mean_diff, std_diff, mean_games, std_games,
      n_samples, n_halls, updated_date
"""
import glob
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_PATH = SCRIPT_DIR / "data" / "machine_baseline_stats.parquet"

# 台番号単位のデータに混ざっている、実機種ではないプレースホルダー行
EXCLUDE_MACHINE_NAMES = {
    "S新台（検）", "サンプルS", "スロットその他機種", "スロット空台", "停止台パチスロ",
}


def run():
    print("daily_unit_data読み込み中...")
    dfs = [pd.read_parquet(f) for f in glob.glob(str(SCRIPT_DIR / "data" / "daily_unit_data" / "*.parquet"))]
    df = pd.concat(dfs, ignore_index=True)
    print(f"読み込み行数: {len(df)}")

    df = df[~df["machine_name"].isin(EXCLUDE_MACHINE_NAMES)]

    stats = df.groupby("machine_name").agg(
        mean_diff=("diff", "mean"),
        std_diff=("diff", "std"),
        mean_games=("games", "mean"),
        std_games=("games", "std"),
        n_samples=("diff", "size"),
        n_halls=("hall_name", "nunique"),
    ).reset_index()
    stats = stats[(stats["std_diff"] > 0) & (stats["std_games"] > 0)]
    stats["updated_date"] = date.today()

    stats = stats.sort_values("n_samples", ascending=False)
    print(f"\n対象機種数: {len(stats)}")
    print(stats.head(10))

    table = pa.Table.from_pandas(stats, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

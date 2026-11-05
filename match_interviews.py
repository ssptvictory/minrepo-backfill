"""
P-WORLD(pworld_interviews.parquet)とスロパイ(slotana_schedule.parquet)の
取材・来店データを、halls_export.csvの392店と名寄せして1つのデータセットに
まとめる。

表記ゆれ(店/号店/スロット館などの接尾辞、全角/半角、中点の有無)を
吸収するため正規化キーで突き合わせる。正規化後に複数の自店舗が同じ
キーになる場合は紐付けを曖昧とみなし突き合わせない(例: 「新館」
「本館」は別店舗なので接尾辞として削らない)。

分析側で使う「確度」の考え方:
  - 1週間前パス: event_dateの7日前までにfirst_seen_dateが来ている記録
  - 前々日パス: event_dateの2日前までにfirst_seen_dateが来ている記録
  (前々日パスの方が、直近の媒体の自然な取材情報も追加で拾えていたり
   複数ソースで裏が取れていたりするため、同じ予定でも確度が上がる
   という設計。P-WORLDの長期PR予告だけでなく、スロパイの短期の
   編集系取材も合わさることで、前々日時点の確度がより高くなる)

出力: data/interview_matched.parquet
  列: hall_name(自分のcsv表記), event_date, sources(カンマ区切り),
      titles(カンマ区切り), is_pr(いずれかのソースでPRならTrue),
      first_seen_date(全ソース中最速), confidence_week_before,
      confidence_2days_before
"""
import re
import unicodedata
import csv
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
HALLS_CSV = SCRIPT_DIR / "halls_export.csv"
PWORLD_PATH = SCRIPT_DIR / "data" / "pworld_interviews.parquet"
SLOTANA_PATH = SCRIPT_DIR / "data" / "slotana_schedule.parquet"
OUT_PATH = SCRIPT_DIR / "data" / "interview_matched.parquet"


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = s.strip()
    s = re.sub(r"\s+", "", s)
    s = s.lower()
    for suffix in ["スロット館", "号店", "店"]:
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    s = s.replace("・", "").replace("･", "").replace("-", "").replace("ー", "")
    return s


def load_our_halls():
    with open(HALLS_CSV, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    norm_to_names = {}
    for r in rows:
        norm_to_names.setdefault(normalize(r["name"]), []).append(r["name"])
    # 正規化後に複数の自店舗が同じキーになる場合は曖昧なので突き合わせない
    return {k: v[0] for k, v in norm_to_names.items() if len(v) == 1}


def load_source(path: Path, source_name: str):
    df = pd.read_parquet(path)
    df = df.copy()
    df["norm"] = df["hall_name"].apply(normalize)
    df["source"] = source_name
    if "title" not in df.columns:
        df["title"] = ""
    if "is_pr" not in df.columns:
        df["is_pr"] = False
    return df[["norm", "event_date", "title", "is_pr", "first_seen_date", "source"]]


def run():
    norm_to_our_name = load_our_halls()
    print(f"自店舗(正規化後、曖昧でないもの): {len(norm_to_our_name)}店")

    frames = []
    if PWORLD_PATH.exists():
        frames.append(load_source(PWORLD_PATH, "pworld"))
    if SLOTANA_PATH.exists():
        frames.append(load_source(SLOTANA_PATH, "slotana"))
    if not frames:
        print("ソースデータが見つかりません")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[combined["norm"].isin(norm_to_our_name)]
    combined["hall_name"] = combined["norm"].map(norm_to_our_name)

    matched_halls = combined["hall_name"].nunique()
    print(f"名寄せでマッチした自店舗: {matched_halls}店")
    print(f"マッチした延べ件数: {len(combined)}")

    grouped = combined.groupby(["hall_name", "event_date"]).agg(
        sources=("source", lambda x: ",".join(sorted(set(x)))),
        titles=("title", lambda x: ",".join(sorted(set(t for t in x if t)))),
        is_pr=("is_pr", "any"),
        first_seen_date=("first_seen_date", "min"),
    ).reset_index()

    grouped["event_date"] = pd.to_datetime(grouped["event_date"])
    grouped["first_seen_date"] = pd.to_datetime(grouped["first_seen_date"])
    lead_days = (grouped["event_date"] - grouped["first_seen_date"]).dt.days
    grouped["confidence_week_before"] = lead_days >= 7
    grouped["confidence_2days_before"] = lead_days >= 2

    grouped = grouped.sort_values(["event_date", "hall_name"])
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    grouped.to_parquet(OUT_PATH, compression="zstd")
    print(f"\n保存: {OUT_PATH.name} ({len(grouped)}行)")

    print("\n=== サンプル(直近の予定) ===")
    today = pd.Timestamp.now().normalize()
    upcoming = grouped[grouped["event_date"] >= today].head(10)
    print(upcoming[["hall_name", "event_date", "sources", "titles", "confidence_week_before", "confidence_2days_before"]])


if __name__ == "__main__":
    run()

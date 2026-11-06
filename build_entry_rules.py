"""
パチ7(pachi7_hall_rules.parquet)とDMMぱちタウン(ptown_hall_rules.parquet)の
入場ルール・抽選情報を、halls_export.csvの392店と名寄せして1つに統合する。

単独では情報が見つかる店舗が少なく(パチ7単独で約80店、p-town単独で
約300店程度)、店長の裁量で公開状況がバラバラという構造的な制約がある
ため、2サイトを合算(OR)して最大限のカバー率を得る。

名寄せが失敗した店舗(表記ゆれが大きい、または1施設が複数名義で
掲載されている等)は、無理に自動で紐付けず「未確認」として残す
(過去に発生した「別ホールのデータを誤って紐付ける」事故を避けるため、
衝突する可能性がある正規化結果は自動マッチ対象から除外している)。

出力: data/entry_rules.parquet
  列: hall_name, source(pachi7/ptown/both), entry_rule, rule_detail,
      numbered_ticket, address, nearest_station, access, business_hours,
      last_checked_date, found
"""
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from match_interviews import normalize, load_our_halls

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_PATH = SCRIPT_DIR / "data" / "entry_rules.parquet"


def run():
    norm_map = load_our_halls()

    pachi7 = pd.read_parquet(SCRIPT_DIR / "data" / "pachi7_hall_rules.parquet")
    ptown = pd.read_parquet(SCRIPT_DIR / "data" / "ptown_hall_rules.parquet")
    pachi7["norm"] = pachi7["hall_name"].apply(normalize)
    ptown["norm"] = ptown["hall_name"].apply(normalize)
    pachi7_m = pachi7[pachi7["norm"].isin(norm_map)].copy()
    ptown_m = ptown[ptown["norm"].isin(norm_map)].copy()
    pachi7_m["our_name"] = pachi7_m["norm"].map(norm_map)
    ptown_m["our_name"] = ptown_m["norm"].map(norm_map)

    rows = {}
    for _, r in pachi7_m.iterrows():
        has_info = bool(r["entry_rule"]) or bool(r["rule_detail"])
        rows[r["our_name"]] = {
            "hall_name": r["our_name"],
            "source": "pachi7",
            "entry_rule": r["entry_rule"],
            "rule_detail": r["rule_detail"],
            "numbered_ticket": "",
            "address": r["address"],
            "nearest_station": r["nearest_station"],
            "access": "",
            "business_hours": r["business_hours"],
            "last_checked_date": r["last_checked_date"],
            "found": has_info,
        }

    for _, r in ptown_m.iterrows():
        has_info = bool(r["entry_rule"]) or bool(r["rule_detail"])
        existing = rows.get(r["our_name"])
        if existing and existing["found"] and has_info:
            existing["source"] = "both"
            continue
        if existing and existing["found"] and not has_info:
            continue
        rows[r["our_name"]] = {
            "hall_name": r["our_name"],
            "source": "ptown" if not (existing and existing["found"]) else existing["source"],
            "entry_rule": r["entry_rule"],
            "rule_detail": r["rule_detail"],
            "numbered_ticket": r["numbered_ticket"],
            "address": r["address"],
            "nearest_station": r["nearest_station"],
            "access": r["access"],
            "business_hours": r["business_hours"],
            "last_checked_date": r["last_checked_date"],
            "found": has_info,
        }

    all_our_names = set(norm_map.values())
    checked_date = pd.Timestamp.now().date()
    for name in all_our_names:
        if name not in rows:
            rows[name] = {
                "hall_name": name,
                "source": "",
                "entry_rule": "",
                "rule_detail": "",
                "numbered_ticket": "",
                "address": "",
                "nearest_station": "",
                "access": "",
                "business_hours": "",
                "last_checked_date": checked_date,
                "found": False,
            }

    df = pd.DataFrame(rows.values()).sort_values("hall_name")
    found_count = df["found"].sum()
    print(f"情報あり: {found_count}/{len(df)}店 ({found_count/len(df)*100:.1f}%)")

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

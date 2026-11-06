"""
パチ7(pachi7_hall_rules.parquet)・DMMぱちタウン(ptown_hall_rules.parquet)・
P-WORLD(pworld_hall_rules.parquet)の入場ルール・抽選情報を、
halls_export.csvの392店と名寄せして1つに統合する。

単独では情報が見つかる店舗が少なく、店長の裁量で公開状況がバラバラ
という構造的な制約があるため、3サイトを合算(OR)して最大限のカバー
率を得る。P-WORLD側は「朝の入場」欄がテキストの代わりに「入場方法を
画像で確認する」という案内文のみのケースがあり、これは実質情報なし
として扱う。

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
    pworld = pd.read_parquet(SCRIPT_DIR / "data" / "pworld_hall_rules.parquet")

    def has_info_pachi7(r):
        return bool(r["entry_rule"]) or bool(r["rule_detail"])

    def has_info_ptown(r):
        return bool(r["entry_rule"]) or bool(r["rule_detail"])

    def has_info_pworld(r):
        text = r["morning_entry"]
        return bool(text) and "画像で確認" not in text

    def to_fields_pachi7(r):
        return {
            "entry_rule": r["entry_rule"], "rule_detail": r["rule_detail"],
            "numbered_ticket": "", "address": r["address"],
            "nearest_station": r["nearest_station"], "access": "",
            "business_hours": r["business_hours"],
        }

    def to_fields_ptown(r):
        return {
            "entry_rule": r["entry_rule"], "rule_detail": r["rule_detail"],
            "numbered_ticket": r["numbered_ticket"], "address": r["address"],
            "nearest_station": r["nearest_station"], "access": r["access"],
            "business_hours": r["business_hours"],
        }

    def to_fields_pworld(r):
        return {
            "entry_rule": r["morning_entry"], "rule_detail": "",
            "numbered_ticket": "", "address": "",
            "nearest_station": "", "access": "",
            "business_hours": "",
        }

    sources = [
        ("pachi7", pachi7, has_info_pachi7, to_fields_pachi7),
        ("ptown", ptown, has_info_ptown, to_fields_ptown),
        ("pworld", pworld, has_info_pworld, to_fields_pworld),
    ]

    rows = {}
    for source_name, df, has_info_fn, to_fields_fn in sources:
        df = df.copy()
        df["norm"] = df["hall_name"].apply(normalize)
        df_m = df[df["norm"].isin(norm_map)].copy()
        df_m["our_name"] = df_m["norm"].map(norm_map)

        for _, r in df_m.iterrows():
            has_info = has_info_fn(r)
            existing = rows.get(r["our_name"])
            if existing and existing["found"]:
                if has_info:
                    existing["source"] = existing["source"] + "+" + source_name
                continue
            fields = to_fields_fn(r)
            rows[r["our_name"]] = {
                "hall_name": r["our_name"],
                "source": source_name,
                "last_checked_date": r["last_checked_date"],
                "found": has_info,
                **fields,
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

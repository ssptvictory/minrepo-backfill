"""
ryome_raw.parquet(フェーズ1で取得した生テーブルデータ)から、並び人数の
目安を正規化して抽出する(フェーズ2: 正規化)。ネットワークアクセスは
一切行わないので、ロジックの不具合が見つかっても何度でも無料で
やり直せる。

表の列構成は記事によってばらつきがあり、以下のパターンを確認済み:
  - 状況 / 入場方法 / 並び人数（目安） / 特徴・状況   (4列、標準形)
  - 状況 / 抽選・並び人数（目安） / 特徴・状況        (3列、入場方法と
    人数が1列に統合)
  - 状況 / 抽選予約・並び人数（目安） / 特徴・状況    (同上の別表記)
  - 状況 / 抽選方法（モバイル） / 特徴・状況          (人数が「抽選方法」
    列の中に埋め込まれ、専用の人数列が無い)
そのため列の位置には依存せず、各行のセルを順に見て「数量っぽい表現を
含むセル」を人数セルとして特定する方式にしている。

人数表現のパターン:
  - "150〜300人" のような範囲
  - "50人以下" / "500人超" / "300人以上"
  - "1,500人〜打ち切り" (下限のみ明示、上限は変動)
  - "数百人" / "数千人" (漢数字による概数表現)
"""
import json
import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
RAW_PATH = SCRIPT_DIR / "data" / "ryome_raw.parquet"
OUT_PATH = SCRIPT_DIR / "data" / "ryome_crowd_sizes.parquet"

KANJI_APPROX = {
    "数十": (20, 90), "数百": (200, 900), "数千": (2000, 9000), "数万": (20000, 90000),
}
# セルの中からどれが「人数セル」かを特定するための判定。
# 「9:10〜9:30」のような時刻表記は数字同士がハイフン/チルダで繋がるため、
# 単純な「digit-digit」パターンだけで判定すると誤検知する。そのため
# ここでは「人/名」または概数表現(数百等)が実際に含まれることを必須とする。
CROWD_HINT_RE = re.compile(r"\d+\s*[人名]|数十|数百|数千|数万")


def parse_crowd_range(text: str):
    text = text.replace(",", "")
    # 「〜200人前後」のように開始側の数字が無い(上限のみの概算)パターン
    m = re.search(r"^[^\d]{0,3}[〜～]\s*(\d+)\s*[人名]", text)
    if m:
        return 0, int(m.group(1))
    m = re.search(r"(\d+)\s*[人名]?\s*[〜～\-]\s*打ち切り", text)
    if m:
        return int(m.group(1)), None
    # 両側に人/名が付くケース(50人 〜 100人)も、片側だけのケース(50〜100人)も拾う
    m = re.search(r"(\d+)\s*[人名]?\s*[〜～\-]\s*(\d+)\s*[人名]?", text)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"(\d+)\s*[人名]?以下", text)
    if m:
        return 0, int(m.group(1))
    m = re.search(r"(\d+)\s*[人名]?(?:超|以上)", text)
    if m:
        return int(m.group(1)), None
    m = re.search(r"(\d+)\s*[人名]", text)
    if m:
        return int(m.group(1)), int(m.group(1))
    for kanji, (lo, hi) in KANJI_APPROX.items():
        if kanji in text:
            return lo, hi
    return None, None


def extract_rows_from_tables(tables):
    rows = []
    for table in tables:
        if len(table) < 2:
            continue
        header_joined = "".join(table[0])
        data_rows = table[1:]

        for cells in data_rows:
            if len(cells) < 2:
                continue
            situation = cells[0]
            rest = cells[1:]

            crowd_idx = next((i for i, c in enumerate(rest) if CROWD_HINT_RE.search(c)), None)
            if crowd_idx is None:
                continue
            crowd_text = rest[crowd_idx]
            other_cells = [c for i, c in enumerate(rest) if i != crowd_idx]
            entry_method = next(
                (c for c in other_cells if re.search(r"抽選|モバイル|整理券|入場|並び", c)), ""
            )
            note = next((c for c in other_cells if c != entry_method), "")

            cmin, cmax = parse_crowd_range(crowd_text)
            if cmin is not None and cmax is not None and cmin > cmax:
                cmin, cmax = cmax, cmin
            rows.append({
                "situation": situation,
                "entry_method": entry_method,
                "crowd_text": crowd_text,
                "crowd_min": cmin,
                "crowd_max": cmax,
                "note": note,
            })
    return rows


def run():
    raw = pd.read_parquet(RAW_PATH)
    print(f"生データ記事数: {len(raw)}")

    halls_csv = pd.read_csv(SCRIPT_DIR / "halls_export.csv")
    core_names = set(halls_csv["name"]) | set(halls_csv["minrepo_name"].dropna())

    out_rows = []
    no_table_halls = []
    for _, article in raw.iterrows():
        tables = json.loads(article["tables_json"])
        rows = extract_rows_from_tables(tables)
        if not rows:
            no_table_halls.append(article["hall_name"])
            continue
        for r in rows:
            out_rows.append({
                "hall_name": article["hall_name"],
                "in_core_392": article["hall_name"] in core_names,
                "source_url": article["source_url"],
                **r,
            })

    df = pd.DataFrame(out_rows)
    print(f"\n総行数: {len(df)}")
    print(f"ユニーク店舗数: {df['hall_name'].nunique()}")
    print(f"コア392店一致: {df[df['in_core_392']]['hall_name'].nunique()}店")
    print(f"crowd_min抽出できなかった行数: {df['crowd_min'].isna().sum()} / {len(df)}")
    print(f"テーブルから何も抽出できなかった記事数: {len(no_table_halls)}")

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

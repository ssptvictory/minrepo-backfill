"""
みんパチ(https://minpachi.com/)の検索機能を使って、パチ7・p-town・
P-WORLDの3ソースで見つからなかったホールの入場方法(整理券の有無・
配布方法・配布時間・注意点)を調べる。

みんパチは「入場方法」セクションがth/td形式で明確に構造化されており、
他サイトより詳細なことが多い(777コンパス/モバイル抽選等のシステム名、
締切・認証・整列・入場の各時刻、特定日限定の注意書きなど)。

発見方法: https://minpachi.com/?s={店名} で検索し、結果一覧のうち
正規化後の文字列が一致するものだけを採用する(誤った別ホールへの
紐付けを避けるため、曖昧な部分一致は採用しない)。

結果はdata/minpachi_hall_rules.parquetに独立して保存する
(build_entry_rules.pyが週次でpachi7/p-town/P-WORLDから
entry_rules.parquetを再生成する際、この結果も4つ目のソースとして
取り込む設計。entry_rules.parquet側に直接書き込むと週次の再生成で
上書き・消失してしまうため)。

このスクレイパーは「他3ソースでまだ見つかっていないホール」だけを
対象に実行する(entry_rules.parquetのfound=Falseの行)。
"""
import time
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
import minpachi_search as m
from match_interviews import normalize

SCRIPT_DIR = Path(__file__).resolve().parent
ENTRY_RULES_PATH = SCRIPT_DIR / "data" / "entry_rules.parquet"
OUT_PATH = SCRIPT_DIR / "data" / "minpachi_hall_rules.parquet"
TIME_BUDGET_SECONDS = 1200

_start_time = time.time()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - _start_time)


def run():
    df = pd.read_parquet(ENTRY_RULES_PATH)
    targets = sorted(df[~df["found"]]["hall_name"])
    print(f"対象(未確認)ホール数: {len(targets)}")

    today = date.today()
    rows = []
    for i, name in enumerate(targets):
        if time_left() <= 0:
            print(f"時間切れ、{i}/{len(targets)}件まで処理")
            break

        norm_target = normalize(name)
        found_url, found_text = None, None
        try:
            search_results = m.search_hall(name)
        except Exception as e:
            print(f"[{i+1}/{len(targets)}] {name}: 検索失敗 {e}")
            time.sleep(2)
            continue

        for text, url in search_results:
            norm_cand = normalize(text)
            if norm_target and (norm_target in norm_cand or norm_cand in norm_target):
                found_url, found_text = url, text
                break

        if not found_url:
            print(f"[{i+1}/{len(targets)}] {name}: 見つからず")
            time.sleep(1.3)
            continue

        try:
            time.sleep(1.2)
            resp = m.fetch(found_url)
            info = m.parse_entry_method(resp.text)
        except Exception as e:
            print(f"[{i+1}/{len(targets)}] {name} -> {found_text} だが詳細取得失敗: {e}")
            time.sleep(1.3)
            continue

        if not info:
            print(f"[{i+1}/{len(targets)}] {name} -> {found_text}: 入場方法欄なし")
            time.sleep(1.3)
            continue

        rows.append({
            "hall_name": name,
            "matched_text": found_text,
            "url": found_url,
            "numbered_ticket": info.get("整理券の有無", ""),
            "method": info.get("整理券の配布方法", ""),
            "distribution_time": info.get("配布時間", ""),
            "notes": info.get("注意点", ""),
            "last_checked_date": today,
        })
        print(f"[{i+1}/{len(targets)}] {name} -> {found_text} | {info}")
        time.sleep(1.3)

    if not rows:
        print("新規データなし")
        return

    new_df = pd.DataFrame(rows)
    if OUT_PATH.exists():
        existing = pq.read_table(OUT_PATH).to_pandas()
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["hall_name"], keep="last")
    else:
        combined = new_df

    table = pa.Table.from_pandas(combined, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"\n保存: {OUT_PATH.name} ({len(combined)}行)")


if __name__ == "__main__":
    run()

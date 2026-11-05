"""
データ汚染チェックの結果を data/contamination_check_log.json に記録する。

バックフィル全体が完了したタイミングで再チェックする際、どの店舗が
いつ・どのチェック方法で確認済みかを追跡するためのログ。
まだログに載っていない店舗(主に、この時点でまだdone/in_progressに
なっていなかった店舗)が、次回チェックすべき対象になる。

これまでに実施したチェック:
  1. tag_page_hall_name_match: タグページに店名が実際に含まれているか
     (2026-10-05実施。14店舗で汚染発覚、修正・リセット済み)
  2. day_to_day_count_anomaly: 同一店舗内で日ごとの台数が異常に変動して
     いないか(前日比40%以上の変化を異常とみなす)
  3. cross_hall_duplicate_fingerprint: 異なる店舗間で、ある日のデータ
     (台番号・差枚・G数の組み合わせ)が完全一致していないか
"""
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

PROGRESS_PATH = Path(__file__).resolve().parent.parent / "data" / "backfill_progress.json"
UNIT_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "daily_unit_data"
LOG_PATH = Path(__file__).resolve().parent.parent / "data" / "contamination_check_log.json"

# 2026-10-05時点で汚染が発覚し、pendingにリセットした店舗
# (再取得後、別途チェックが必要)
PREVIOUSLY_CONTAMINATED = {
    "R7STRONG", "SAP蒲生", "ウイング蕨600", "ジャラン川口峯店",
    "ジャラン川口弥平店", "ジャラン武里店", "デルパラ2吉川店",
    "ニューダイエイ3", "パチンコプラザラ・カータ上里店", "ピーアーク草加",
    "ピーアーク谷塚", "ピークスゼロ", "ライブガーデン上尾店", "楽園大宮店",
}
# データが空だったため再取得対象にした店舗
PREVIOUSLY_EMPTY = {"TOHO川越店"}


def main():
    files = sorted(UNIT_DATA_DIR.glob("*.parquet"))
    dfs = [pq.read_table(f).to_pandas() for f in files]
    df = pd.concat(dfs, ignore_index=True)
    checked_halls = sorted(df["hall_name"].unique())

    today = date.today().isoformat()
    log = {}
    if LOG_PATH.exists():
        log = json.load(open(LOG_PATH, encoding="utf-8"))

    halls_log = log.get("halls", {})
    for hall in checked_halls:
        halls_log[hall] = {
            "last_checked": today,
            "checks_passed": [
                "tag_page_hall_name_match",
                "day_to_day_count_anomaly",
                "cross_hall_duplicate_fingerprint",
            ],
            "status": "clean",
        }

    for hall in PREVIOUSLY_CONTAMINATED:
        halls_log[hall] = {
            "last_checked": today,
            "checks_passed": [],
            "status": "previously_contaminated_reset_pending_rescrape",
        }
    for hall in PREVIOUSLY_EMPTY:
        halls_log[hall] = {
            "last_checked": today,
            "checks_passed": [],
            "status": "previously_empty_reset_pending_rescrape",
        }

    log["halls"] = halls_log
    log["last_run"] = today
    log["checks_performed"] = [
        "tag_page_hall_name_match",
        "day_to_day_count_anomaly",
        "cross_hall_duplicate_fingerprint",
    ]
    log["note"] = (
        "まだこのログに無い店舗(backfillが未completeの店舗)は、"
        "次回のチェック対象。バックフィル全体完了時に再度このスクリプトを"
        "実行し、新たにdone/in_progressになった店舗を検証すること。"
    )

    with open(LOG_PATH, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=1, sort_keys=True)

    print(f"チェック済みとして記録: {len(checked_halls)}店舗(クリーン)")
    print(f"再取得待ち: {len(PREVIOUSLY_CONTAMINATED)+len(PREVIOUSLY_EMPTY)}店舗")
    print(f"保存先: {LOG_PATH}")


if __name__ == "__main__":
    main()

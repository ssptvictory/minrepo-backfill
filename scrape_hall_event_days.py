"""
min-repo.comの各店舗ページに記載されている「旧イベント日」を取得する。

「旧イベント日」は店舗ごとに静的な情報で、各日の個別記事ページ
(https://min-repo.com/{数値ID}/)の決まった位置に毎回同じ内容で
表示される。記事を1店舗につき1回だけ取得すれば済む。

店舗ごとの記事URL発見には、当初/tag/{店名}/ページ(「過去の結果」
一覧テーブル)を使う予定だったが、このエンドポイントへの連続アクセスで
レート制限に遭遇し、本文(テーブル部分)だけが欠落した応答が返ってくる
ようになった(タイトル・見出しは正常、2000文字チェックも通過するため
既存の検知ロジックでは気づけない)。/category/{都道府県}/ページと
個別記事ページ自体は問題なくアクセスできたため、カテゴリ一覧ページを
ページネーションで辿って(1店舗1記事あれば十分なので、直近の数十
ページで事実上の全店舗をカバーできる)店舗名と記事URLのペアを収集する
方式に変更した。

出力: data/hall_event_days.parquet
  列: hall_name, found, raw_text, days(JSON), patterns(JSON),
      source_url, checked_date
"""
import json
import random
import re
import sys
import time
from datetime import date
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
import backfill_minrepo_full as bf

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_PATH = SCRIPT_DIR / "data" / "hall_event_days.parquet"
PROGRESS_PATH = SCRIPT_DIR / "data" / "hall_event_days_progress.json"
DISCOVERY_PATH = SCRIPT_DIR / "data" / "hall_article_urls.json"
REQUEST_DELAY = 3.0
PREFECTURES = ["埼玉県", "東京都"]
MAX_PAGES_PER_PREF = 60


def load_targets():
    halls = pd.read_csv(SCRIPT_DIR / "halls_export.csv")
    targets = list(halls["minrepo_name"].fillna(halls["name"]))

    entry_rules_path = SCRIPT_DIR / "data" / "entry_rules.parquet"
    if entry_rules_path.exists():
        er = pd.read_parquet(entry_rules_path)
        extra = er[~er["in_core_392"]]["hall_name"].unique().tolist()
        targets.extend(extra)

    seen = set()
    uniq = []
    for t in targets:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq


def discover_article_urls(targets_remaining: set) -> dict:
    """カテゴリページを辿り、未発見の店舗の記事URLを見つける。
    1店舗1URL見つかれば十分なので、見つかった時点でtargets_remainingから外す。
    """
    found = {}
    for pref in PREFECTURES:
        if not targets_remaining:
            break
        for page in range(1, MAX_PAGES_PER_PREF + 1):
            if not targets_remaining:
                break
            url = (
                f"https://min-repo.com/category/{quote(pref, safe='')}/"
                if page == 1
                else f"https://min-repo.com/category/{quote(pref, safe='')}/page/{page}/"
            )
            try:
                resp = bf.fetch(url)
                if len(resp.text) < 3000:
                    print(f"  {pref} page{page}: 応答が短い、スキップ")
                    time.sleep(REQUEST_DELAY)
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                hits_this_page = 0
                for a in soup.find_all("a", href=re.compile(r"https://min-repo\.com/\d+/$")):
                    text = a.get_text(strip=True)
                    m = re.match(r"\d{1,2}/\d{1,2}\([^)]+\)\s*(.+)$", text)
                    if not m:
                        continue
                    hall_name = m.group(1).strip()
                    if hall_name in targets_remaining and hall_name not in found:
                        found[hall_name] = a["href"]
                        targets_remaining.discard(hall_name)
                        hits_this_page += 1
                print(f"  {pref} page{page}: 新規発見{hits_this_page}件、残り{len(targets_remaining)}件")
            except Exception as e:
                print(f"  {pref} page{page} 失敗: {e}")
            time.sleep(REQUEST_DELAY + random.random())
    return found


def parse_event_text(raw: str):
    days = []
    patterns = []
    for part in re.split(r"[、,]", raw):
        part = part.strip()
        if not part:
            continue
        m = re.fullmatch(r"(\d{1,2})日", part)
        if m:
            days.append(int(m.group(1)))
            continue
        patterns.append(part)
    return days, patterns


def extract_event_days(html: str):
    soup = BeautifulSoup(html, "html.parser")
    th = soup.find("th", string=re.compile("旧イベント日"))
    if not th:
        return None
    td = th.find_next_sibling("td")
    if not td:
        return None
    spans = td.find_all("span")
    if spans:
        raw = "、".join(s.get_text(strip=True) for s in spans)
    else:
        raw = td.get_text(strip=True)
    raw = re.sub(r"※追加・修正.*$", "", raw).strip()
    return raw


def run():
    targets = load_targets()
    print(f"対象店舗数: {len(targets)}")

    progress = json.load(open(PROGRESS_PATH)) if PROGRESS_PATH.exists() else {"done": {}}
    done = progress["done"]

    article_urls = json.load(open(DISCOVERY_PATH)) if DISCOVERY_PATH.exists() else {}

    remaining_for_discovery = {t for t in targets if t not in done and t not in article_urls}
    if remaining_for_discovery:
        print(f"\n=== 記事URL発見フェーズ(未発見{len(remaining_for_discovery)}件) ===")
        newly_found = discover_article_urls(remaining_for_discovery)
        article_urls.update(newly_found)
        json.dump(article_urls, open(DISCOVERY_PATH, "w"), ensure_ascii=False, indent=1)
        print(f"発見できた記事URL: {len(newly_found)}件")

    remaining = [t for t in targets if t not in done and t in article_urls]
    print(f"\n=== 旧イベント日取得フェーズ(対象{len(remaining)}件) ===")

    for i, hall_name in enumerate(remaining):
        try:
            resp = bf.fetch(article_urls[hall_name])
            raw = extract_event_days(resp.text)
            if raw is None:
                done[hall_name] = {"found": False, "reason": "旧イベント日欄なし"}
            else:
                days, patterns = parse_event_text(raw)
                done[hall_name] = {
                    "found": True,
                    "raw_text": raw,
                    "days": days,
                    "patterns": patterns,
                    "source_url": article_urls[hall_name],
                }
            if (i + 1) % 20 == 0:
                print(f"  [{i+1}/{len(remaining)}] 処理済み")
                json.dump(progress, open(PROGRESS_PATH, "w"), ensure_ascii=False, indent=1)
        except Exception as e:
            print(f"  [{hall_name}] 失敗: {e}")
        time.sleep(REQUEST_DELAY + random.random())

    json.dump(progress, open(PROGRESS_PATH, "w"), ensure_ascii=False, indent=1)

    rows = []
    for hall_name, info in done.items():
        rows.append({
            "hall_name": hall_name,
            "found": info.get("found", False),
            "raw_text": info.get("raw_text", ""),
            "days": json.dumps(info.get("days", []), ensure_ascii=False),
            "patterns": json.dumps(info.get("patterns", []), ensure_ascii=False),
            "source_url": info.get("source_url", ""),
            "checked_date": date.today(),
        })
    df = pd.DataFrame(rows)
    print(f"\n発見: {df['found'].sum() if len(df) else 0}/{len(df)}店")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUT_PATH, compression="zstd", compression_level=15)
    print(f"保存: {OUT_PATH.name}")


if __name__ == "__main__":
    run()

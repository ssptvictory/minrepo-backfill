"""
みんパチ(https://minpachi.com/)の検索機能を使って、残りの未確認ホールの
入場方法(整理券の有無・配布方法・配布時間・注意点)を調べる。

パチ7・p-town・P-WORLDの3ソースで情報が見つからなかった店舗向けの
補完調査。みんパチは「入場方法」セクションが明確なtable行(整理券の
有無/配布方法/配布時間/注意点)でテキスト化されており、他サイトより
詳細なことが多い。

robots.txt確認済み: AIクローラーの個別拒否なし。

発見方法: https://minpachi.com/?s={店名} で検索し、結果一覧から
最も一致するホールへのリンクを辿る。都道府県別カテゴリページは
件数が少なく網羅的でないため使わない。
"""
import random
import time
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

_session = requests.Session()


def fetch(url: str, retries: int = 3) -> requests.Response:
    last_exc = None
    for attempt in range(retries):
        try:
            resp = _session.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp
        except requests.exceptions.RequestException as e:
            last_exc = e
            time.sleep(3 + attempt * 3)
    raise last_exc


NAV_SLUGS = {
    "hajimetenokata", "ranking", "userinfo", "besthallranking",
    "search", "category", "feed", "wp-login", "privacy",
}


def search_hall(query: str):
    resp = fetch(f"https://minpachi.com/?s={quote(query)}")
    soup = BeautifulSoup(resp.text, "html.parser")
    results = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "?" in href or not href.startswith("https://minpachi.com/"):
            continue
        parts = href[len("https://minpachi.com/"):].strip("/").split("/")
        if len(parts) != 1 or not parts[0]:
            continue
        slug = parts[0]
        if slug in NAV_SLUGS:
            continue
        text = a.get_text(strip=True)
        if text and href not in seen:
            seen.add(href)
            results.append((text, href))
    return results


def parse_entry_method(html: str):
    soup = BeautifulSoup(html, "html.parser")
    info = {}
    capture = False
    for tr in soup.find_all("tr"):
        th = tr.find("th")
        if not th:
            continue
        label = th.get_text(strip=True)
        if label == "入場方法":
            capture = True
            continue
        if label == "遊技料金":
            capture = False
        if capture:
            td = tr.find("td")
            if td:
                info[label] = td.get_text(" ", strip=True)
    return info

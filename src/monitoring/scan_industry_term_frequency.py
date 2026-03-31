from __future__ import annotations

import argparse
import csv
import json
import re
import time
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote_plus
from urllib.request import Request, build_opener

try:
    import requests
except ImportError:
    requests = None


DEFAULT_A_SHARE_TAXONOMY_FILE = "taxonomy/a_share_sw_taxonomy.json"
DEFAULT_US_TAXONOMY_FILE = "taxonomy/gics_us_taxonomy.json"
DEFAULT_TAXONOMY: list[dict[str, Any]] = []

DEFAULT_A_SHARE_QUERIES = [
    "中国 产业 政策",
    "中国 行业 新闻",
    "A股 行业",
    "宏观 经济 产业链",
]

DEFAULT_US_QUERIES = [
    "US stock sector news",
    "S&P 500 industry news",
    "Wall Street sector rotation",
]

PUBLIC_MEDIA_RSS_FEEDS: list[dict[str, str]] = [
    {"name": "sky_home", "url": "https://feeds.skynews.com/feeds/rss/home.xml"},
    {"name": "sky_world", "url": "https://feeds.skynews.com/feeds/rss/world.xml"},
    {"name": "sky_business", "url": "https://feeds.skynews.com/feeds/rss/business.xml"},
    {"name": "wapo_world", "url": "https://feeds.washingtonpost.com/rss/world"},
    {"name": "wapo_business", "url": "https://feeds.washingtonpost.com/rss/business"},
    {"name": "abc_topstories", "url": "https://feeds.abcnews.com/abcnews/topstories"},
    {"name": "abc_international", "url": "https://feeds.abcnews.com/abcnews/internationalheadlines"},
    {"name": "fox_latest", "url": "https://moxie.foxnews.com/google-publisher/latest.xml"},
    {"name": "fox_business", "url": "https://moxie.foxnews.com/google-publisher/business.xml"},
    {"name": "guardian_world", "url": "https://www.theguardian.com/world/rss"},
    {"name": "guardian_business", "url": "https://www.theguardian.com/business/rss"},
    {"name": "guardian_technology", "url": "https://www.theguardian.com/technology/rss"},
    {"name": "dw_top", "url": "http://rss.dw.com/rdf/rss-en-top"},
    {"name": "dw_business", "url": "http://rss.dw.com/rdf/rss-en-bus"},
    {"name": "lemonde_international", "url": "https://www.lemonde.fr/en/international/rss_full.xml"},
]

BAIDU_RSS_FEEDS: list[dict[str, str]] = [
    {"name": "baidu_finannews", "url": "https://news.baidu.com/n?cmd=4&class=finannews&tn=rss"},
    {"name": "baidu_technnews", "url": "https://news.baidu.com/n?cmd=4&class=technnews&tn=rss"},
    {"name": "baidu_civilnews", "url": "https://news.baidu.com/n?cmd=4&class=civilnews&tn=rss"},
    {"name": "baidu_enternews", "url": "https://news.baidu.com/n?cmd=4&class=enternews&tn=rss"},
]


@dataclass
class NewsItem:
    title: str
    summary: str
    published_at: datetime
    source_query: str
    source_name: str


class RetryClient:
    def __init__(self, max_retry: int, timeout_sec: int = 30) -> None:
        self.max_retry = max_retry
        self.timeout_sec = timeout_sec
        self.user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
        self.session = None
        self.opener = None
        if requests is not None:
            self.session = requests.Session()
            self.session.trust_env = True
            self.session.headers.update({"User-Agent": self.user_agent})
        else:
            self.opener = build_opener()

    @staticmethod
    def _decode_bytes(payload: bytes, charset: str | None) -> str:
        encodings = [charset, "utf-8", "utf-8-sig", "gb18030", "latin-1"]
        for encoding in encodings:
            if not encoding:
                continue
            try:
                return payload.decode(encoding)
            except UnicodeDecodeError:
                continue
        return payload.decode("utf-8", errors="replace")

    def get_text(self, url: str) -> str:
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retry + 1):
            try:
                if self.session is not None:
                    resp = self.session.get(url, timeout=self.timeout_sec)
                    resp.raise_for_status()
                    resp.encoding = resp.apparent_encoding or resp.encoding
                    return resp.text

                request = Request(url, headers={"User-Agent": self.user_agent})
                with self.opener.open(request, timeout=self.timeout_sec) as resp:
                    charset = resp.headers.get_content_charset()
                    return self._decode_bytes(resp.read(), charset)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt >= self.max_retry:
                    break
                time.sleep(2 ** attempt)
        raise RuntimeError(f"Request failed after retries: {url}") from last_exc

    def get_json(self, url: str) -> dict[str, Any]:
        payload = self.get_text(url)
        parsed = json.loads(payload)
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Unexpected JSON payload type from: {url}")
        return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按行业词汇统计公开新闻近N天词频")
    parser.add_argument("--lookback-days", type=int, default=3, help="抓取时间范围（天），默认3")
    parser.add_argument("--top-level1", type=int, default=3, help="输出一级行业TopN，默认3")
    parser.add_argument("--top-level2", type=int, default=5, help="输出二级行业TopN，默认5")
    parser.add_argument("--top-level3", type=int, default=10, help="输出三级行业TopN，默认10")
    parser.add_argument("--top-level4", type=int, default=15, help="输出四级行业TopN，默认15")
    parser.add_argument("--target-news-count", type=int, default=1000, help="单个市场累计去重新闻达到该条数后提前结束，默认1000")
    parser.add_argument("--output-path", default="", help="输出CSV路径")
    parser.add_argument("--max-retry", type=int, default=3, help="网络重试次数，默认3")
    parser.add_argument("--max-items-per-query", type=int, default=120, help="每个查询最多解析条目数")
    parser.add_argument("--max-items-per-feed", type=int, default=80, help="每个公开RSS最多解析条目数")
    parser.add_argument(
        "--sources",
        default="baidu,bing,google",
        help="新闻源，逗号分隔：baidu,bing,google",
    )
    parser.add_argument(
        "--query",
        action="append",
        default=[],
        help="可重复传入公开搜索词。为空时使用内置行业查询。",
    )
    parser.add_argument(
        "--taxonomy-path",
        default="",
        help=f"可选A股行业词表JSON路径。为空时默认使用 src/{DEFAULT_A_SHARE_TAXONOMY_FILE}。",
    )
    parser.add_argument(
        "--us-taxonomy-path",
        default="",
        help=f"美股GICS词表JSON路径。为空时默认使用 src/{DEFAULT_US_TAXONOMY_FILE}。",
    )
    parser.add_argument("--debug", action="store_true", help="输出抓取诊断信息")
    return parser.parse_args()


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def parse_datetime(raw: Any, fallback: datetime) -> datetime:
    if raw is None:
        return fallback
    text = str(raw).strip()
    if not text:
        return fallback

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y%m%d%H%M%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    try:
        parsed = parsedate_to_datetime(text)
        if parsed.tzinfo is not None:
            return parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:  # noqa: BLE001
        return fallback


def parse_rss_items(
    xml_text: str,
    query: str,
    source_name: str,
    now: datetime,
    max_items: int,
) -> list[NewsItem]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    nodes = root.findall(".//item")
    if not nodes:
        nodes = root.findall(".//{http://www.w3.org/2005/Atom}entry")
    if not nodes:
        nodes = root.findall(".//entry")

    items: list[NewsItem] = []
    for node in nodes[:max_items]:
        title = (
            node.findtext("title")
            or node.findtext("{http://www.w3.org/2005/Atom}title")
            or ""
        ).strip()
        summary = (
            node.findtext("description")
            or node.findtext("summary")
            or node.findtext("{http://www.w3.org/2005/Atom}summary")
            or ""
        ).strip()
        published_raw = (
            node.findtext("pubDate")
            or node.findtext("published")
            or node.findtext("updated")
            or node.findtext("{http://www.w3.org/2005/Atom}published")
            or node.findtext("{http://www.w3.org/2005/Atom}updated")
            or ""
        )
        published_at = parse_datetime(published_raw, fallback=now)
        items.append(
            NewsItem(
                title=title,
                summary=summary,
                published_at=published_at,
                source_query=query,
                source_name=source_name,
            )
        )
    return items


def fetch_bing_news_rss(
    client: RetryClient,
    query: str,
    lookback_days: int,
    max_items_per_query: int,
    now: datetime,
    set_lang: str = "zh-Hans",
    market_code: str = "",
) -> list[NewsItem]:
    url = f"https://www.bing.com/news/search?q={quote_plus(query)}&format=rss&setlang={quote_plus(set_lang)}"
    if market_code:
        url += f"&mkt={quote_plus(market_code)}"
    xml_text = client.get_text(url)
    parsed = parse_rss_items(xml_text, query=query, source_name="bing", now=now, max_items=max_items_per_query)
    cutoff = now - timedelta(days=max(1, lookback_days))
    return [item for item in parsed if item.published_at >= cutoff]


def fetch_msn_edge_news_rss(
    client: RetryClient,
    query: str,
    lookback_days: int,
    max_items_per_query: int,
    now: datetime,
) -> list[NewsItem]:
    # Edge/MSN news traffic commonly originates from Bing News endpoints.
    url = (
        "https://www.bing.com/news/search"
        f"?q={quote_plus(query)}&format=rss&setlang=en-US&mkt=en-US&form=PTFTNR"
    )
    xml_text = client.get_text(url)
    parsed = parse_rss_items(xml_text, query=query, source_name="msn_edge", now=now, max_items=max_items_per_query)
    cutoff = now - timedelta(days=max(1, lookback_days))
    return [item for item in parsed if item.published_at >= cutoff]


def fetch_google_news_rss(
    client: RetryClient,
    query: str,
    lookback_days: int,
    max_items_per_query: int,
    now: datetime,
    source_name: str,
    hl: str,
    gl: str,
    ceid: str,
) -> list[NewsItem]:
    query_text = f"{query} when:{max(1, lookback_days)}d"
    url = (
        "https://news.google.com/rss/search"
        f"?q={quote_plus(query_text)}&hl={hl}&gl={gl}&ceid={ceid}"
    )
    xml_text = client.get_text(url)
    parsed = parse_rss_items(
        xml_text,
        query=query,
        source_name=source_name,
        now=now,
        max_items=max_items_per_query,
    )
    cutoff = now - timedelta(days=max(1, lookback_days))
    return [item for item in parsed if item.published_at >= cutoff]


def fetch_gdelt_items(
    client: RetryClient,
    query: str,
    lookback_days: int,
    max_items_per_query: int,
    now: datetime,
) -> list[NewsItem]:
    url = (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={quote_plus(query)}&mode=ArtList&maxrecords={max(10, max_items_per_query)}"
        f"&format=json&timespan={max(1, lookback_days)}d"
    )
    payload = client.get_json(url)
    articles = payload.get("articles") or []
    if not isinstance(articles, list):
        return []
    cutoff = now - timedelta(days=max(1, lookback_days))
    out: list[NewsItem] = []
    for item in articles:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        summary = str(item.get("seendate") or item.get("domain") or "")
        date_raw = item.get("seendate") or item.get("date") or ""
        published_at = parse_datetime(date_raw, fallback=now)
        if published_at < cutoff:
            continue
        out.append(
            NewsItem(
                title=title,
                summary=summary,
                published_at=published_at,
                source_query=query,
                source_name="gdelt",
            )
        )
    return out


def normalize_leaf(raw: Any, fallback_name: str = "") -> dict[str, Any] | None:
    if isinstance(raw, str):
        name = raw.strip()
        if not name:
            return None
        return {"name": name, "aliases": [name]}

    if not isinstance(raw, dict):
        return None
    name = str(raw.get("name") or raw.get("level4") or fallback_name).strip()
    if not name:
        return None
    aliases_raw = raw.get("aliases") or raw.get("keywords") or raw.get("level4_keywords") or [name]
    if not isinstance(aliases_raw, list):
        aliases_raw = [name]
    aliases = [str(item).strip() for item in aliases_raw if str(item).strip()]
    if not aliases:
        aliases = [name]
    return {"name": name, "aliases": aliases}


def normalize_term(raw: Any, fallback_name: str = "") -> dict[str, Any] | None:
    if isinstance(raw, str):
        name = raw.strip()
        if not name:
            return None
        return {"name": name, "aliases": [name], "level3": [], "level4": []}

    if not isinstance(raw, dict):
        return None
    name = str(raw.get("name") or raw.get("level2") or raw.get("level3") or fallback_name).strip()
    if not name:
        return None
    aliases_raw = (
        raw.get("aliases")
        or raw.get("keywords")
        or raw.get("level2_keywords")
        or raw.get("level3_keywords")
        or [name]
    )
    if not isinstance(aliases_raw, list):
        aliases_raw = [name]
    aliases = [str(item).strip() for item in aliases_raw if str(item).strip()]
    if not aliases:
        aliases = [name]

    level3_raw = raw.get("level3") or []
    level3: list[dict[str, Any]] = []
    if isinstance(level3_raw, list):
        for node in level3_raw:
            normalized = normalize_term(node)
            if normalized:
                level3.append(normalized)

    level4_raw = raw.get("level4") or []
    level4: list[dict[str, Any]] = []
    if isinstance(level4_raw, list):
        for node in level4_raw:
            normalized = normalize_leaf(node)
            if normalized:
                level4.append(normalized)

    return {"name": name, "aliases": aliases, "level3": level3, "level4": level4}


def normalize_taxonomy(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        level1 = str(item.get("level1") or "").strip()
        if not level1:
            continue
        aliases_raw = item.get("aliases") or item.get("level1_keywords") or [level1]
        if not isinstance(aliases_raw, list):
            aliases_raw = [level1]
        aliases = [str(node).strip() for node in aliases_raw if str(node).strip()]
        if not aliases:
            aliases = [level1]

        level2_raw = item.get("level2")
        if not isinstance(level2_raw, list):
            old = item.get("level2_keywords") or []
            level2_raw = [{"name": str(node), "aliases": [str(node)], "level3": []} for node in old]

        level2_terms: list[dict[str, Any]] = []
        for node in level2_raw:
            normalized = normalize_term(node)
            if normalized:
                level2_terms.append(normalized)
        out.append({"level1": level1, "aliases": aliases, "level2": level2_terms})
    return out


def load_taxonomy(
    taxonomy_path: Path | None,
    default_taxonomy: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if taxonomy_path and taxonomy_path.exists():
        payload = taxonomy_path.read_text(encoding="utf-8")
        loaded = json.loads(payload)
        normalized = normalize_taxonomy(loaded)
        if normalized:
            return normalized
    return normalize_taxonomy(default_taxonomy)


def resolve_default_taxonomy_path(script_dir: Path, default_rel_path: str, legacy_file_name: str) -> Path:
    src_dir = script_dir.parent if script_dir.name == "monitoring" else script_dir
    default_path = src_dir / default_rel_path
    if default_path.exists():
        return default_path
    legacy_path = src_dir / legacy_file_name
    if legacy_path.exists():
        return legacy_path
    return default_path


def build_queries(
    taxonomy: list[dict[str, Any]],
    custom_queries: list[str],
    default_queries: list[str],
    query_builder: Any,
) -> list[str]:
    queries = [q.strip() for q in custom_queries if q.strip()]
    if queries:
        return list(dict.fromkeys(queries))

    auto_queries = list(default_queries)
    for item in taxonomy:
        level1 = str(item.get("level1") or "").strip()
        if not level1:
            continue
        auto_queries.extend(query_builder(level1))
    return list(dict.fromkeys(auto_queries))


def contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text))


def count_alias_occurrences(text_lower: str, alias: str) -> int:
    token = alias.strip().lower()
    if not token:
        return 0
    if contains_cjk(token):
        return text_lower.count(token)
    pattern = rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])"
    return len(re.findall(pattern, text_lower))


def count_term_hits(text_lower: str, aliases: list[str]) -> int:
    unique_aliases = list(dict.fromkeys([a.strip() for a in aliases if a.strip()]))
    return sum(count_alias_occurrences(text_lower, alias) for alias in unique_aliases)


def aggregate_industry_frequency(
    items: list[NewsItem],
    taxonomy: list[dict[str, Any]],
) -> dict[str, Counter[Any]]:
    hot_level1: Counter[str] = Counter()
    hot_level2: Counter[tuple[str, str]] = Counter()
    hot_level3: Counter[tuple[str, str, str]] = Counter()
    hot_level4: Counter[tuple[str, str, str, str]] = Counter()

    for item in items:
        text = f"{item.title} {item.summary}".strip().lower()
        if not text:
            continue

        for sector in taxonomy:
            level1_name = str(sector.get("level1") or "").strip()
            if not level1_name:
                continue
            level1_aliases = sector.get("aliases") or [level1_name]
            l1_hits = count_term_hits(text, level1_aliases)

            level2_total = 0
            level3_total = 0
            level4_total = 0
            for level2 in sector.get("level2") or []:
                l2_name = str(level2.get("name") or "").strip()
                if not l2_name:
                    continue
                l2_aliases = level2.get("aliases") or [l2_name]
                l2_hits = count_term_hits(text, l2_aliases)
                if l2_hits > 0:
                    hot_level2[(level1_name, l2_name)] += l2_hits
                    level2_total += l2_hits

                for level3 in level2.get("level3") or []:
                    l3_name = str(level3.get("name") or "").strip()
                    if not l3_name:
                        continue
                    l3_aliases = level3.get("aliases") or [l3_name]
                    l3_hits = count_term_hits(text, l3_aliases)
                    if l3_hits > 0:
                        hot_level3[(level1_name, l2_name, l3_name)] += l3_hits
                        level3_total += l3_hits

                    level4_terms = level3.get("level4") or []
                    if not isinstance(level4_terms, list):
                        level4_terms = []
                    # A股默认三级词表无level4时，回退到将三级作为四级，以支持同层级top对比。
                    if not level4_terms:
                        level4_terms = [{"name": l3_name, "aliases": l3_aliases}]
                    for level4 in level4_terms:
                        l4_name = str(level4.get("name") or "").strip()
                        if not l4_name:
                            continue
                        l4_aliases = level4.get("aliases") or [l4_name]
                        l4_hits = count_term_hits(text, l4_aliases)
                        if l4_hits > 0:
                            hot_level4[(level1_name, l2_name, l3_name, l4_name)] += l4_hits
                            level4_total += l4_hits

            total_hits = l1_hits + level2_total + level3_total + level4_total
            if total_hits > 0:
                hot_level1[level1_name] += total_hits

    return {
        "level1": hot_level1,
        "level2": hot_level2,
        "level3": hot_level3,
        "level4": hot_level4,
    }


def write_blank_row(writer: csv.DictWriter) -> None:
    writer.writerow(
        {
            "market": "",
            "level": "",
            "rank": "",
            "level1_industry": "",
            "level2_industry": "",
            "level3_industry": "",
            "level4_industry": "",
            "frequency": "",
            "lookback_days": "",
            "start_date": "",
            "end_date": "",
            "news_count": "",
        }
    )


def write_rows_for_level(
    writer: csv.DictWriter,
    market: str,
    level: str,
    rows: Iterable[tuple[Any, int]],
    lookback_days: int,
    start_date: str,
    end_date: str,
    news_count: int,
) -> None:
    for rank, (keys, freq) in enumerate(rows, start=1):
        level1 = ""
        level2 = ""
        level3 = ""
        level4 = ""
        if level == "level1":
            level1 = str(keys)
        elif level == "level2":
            level1, level2 = keys
        elif level == "level3":
            level1, level2, level3 = keys
        elif level == "level4":
            level1, level2, level3, level4 = keys

        writer.writerow(
            {
                "market": market,
                "level": level,
                "rank": rank,
                "level1_industry": level1,
                "level2_industry": level2,
                "level3_industry": level3,
                "level4_industry": level4,
                "frequency": freq,
                "lookback_days": lookback_days,
                "start_date": start_date,
                "end_date": end_date,
                "news_count": news_count,
            }
        )


def write_result_csv(
    output_path: Path,
    market_rows: list[dict[str, Any]],
    lookback_days: int,
    start_date: str,
    end_date: str,
    news_count: int,
) -> None:
    ensure_parent_dir(output_path)
    headers = [
        "market",
        "level",
        "rank",
        "level1_industry",
        "level2_industry",
        "level3_industry",
        "level4_industry",
        "frequency",
        "lookback_days",
        "start_date",
        "end_date",
        "news_count",
    ]
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

        for idx, block in enumerate(market_rows):
            market = str(block["market"])
            market_news_count = int(block.get("news_count", news_count))
            write_rows_for_level(
                writer,
                market=market,
                level="level1",
                rows=block.get("level1_rows", []),
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
                news_count=market_news_count,
            )
            write_blank_row(writer)
            write_rows_for_level(
                writer,
                market=market,
                level="level2",
                rows=block.get("level2_rows", []),
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
                news_count=market_news_count,
            )
            write_blank_row(writer)
            write_rows_for_level(
                writer,
                market=market,
                level="level3",
                rows=block.get("level3_rows", []),
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
                news_count=market_news_count,
            )
            write_blank_row(writer)
            write_rows_for_level(
                writer,
                market=market,
                level="level4",
                rows=block.get("level4_rows", []),
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
                news_count=market_news_count,
            )
            if idx < len(market_rows) - 1:
                write_blank_row(writer)
                write_blank_row(writer)


def fetch_public_media_rss(
    client: RetryClient,
    lookback_days: int,
    max_items_per_feed: int,
    now: datetime,
) -> tuple[list[NewsItem], list[dict[str, str]]]:
    cutoff = now - timedelta(days=max(1, lookback_days))
    collected: list[NewsItem] = []
    errors: list[dict[str, str]] = []
    for feed in PUBLIC_MEDIA_RSS_FEEDS:
        feed_name = str(feed.get("name") or "").strip()
        feed_url = str(feed.get("url") or "").strip()
        if not feed_name or not feed_url:
            continue
        try:
            xml_text = client.get_text(feed_url)
            parsed = parse_rss_items(
                xml_text=xml_text,
                query=feed_url,
                source_name=f"public_rss:{feed_name}",
                now=now,
                max_items=max_items_per_feed,
            )
            for item in parsed:
                if item.published_at >= cutoff:
                    collected.append(item)
        except Exception as exc:  # noqa: BLE001
            errors.append({"source": "public_rss", "query": feed_name, "error": str(exc)})
    return collected, errors


def fetch_baidu_rss(
    client: RetryClient,
    lookback_days: int,
    max_items_per_feed: int,
    now: datetime,
) -> tuple[list[NewsItem], list[dict[str, str]]]:
    cutoff = now - timedelta(days=max(1, lookback_days))
    collected: list[NewsItem] = []
    errors: list[dict[str, str]] = []
    for feed in BAIDU_RSS_FEEDS:
        feed_name = str(feed.get("name") or "").strip()
        feed_url = str(feed.get("url") or "").strip()
        if not feed_name or not feed_url:
            continue
        try:
            xml_text = client.get_text(feed_url)
            parsed = parse_rss_items(
                xml_text=xml_text,
                query=feed_url,
                source_name=f"baidu:{feed_name}",
                now=now,
                max_items=max_items_per_feed,
            )
            for item in parsed:
                if item.published_at >= cutoff:
                    collected.append(item)
        except Exception as exc:  # noqa: BLE001
            errors.append({"source": "baidu", "query": feed_name, "error": str(exc)})
    return collected, errors


def fetch_by_source(
    source: str,
    client: RetryClient,
    query: str,
    lookback_days: int,
    max_items_per_query: int,
    now: datetime,
    market: str,
) -> list[NewsItem]:
    if source == "bing":
        if market == "us_gics":
            return fetch_bing_news_rss(
                client,
                query,
                lookback_days,
                max_items_per_query,
                now,
                set_lang="en-US",
                market_code="en-US",
            )
        return fetch_bing_news_rss(client, query, lookback_days, max_items_per_query, now)
    if source == "google":
        if market == "us_gics":
            return fetch_google_news_rss(
                client,
                query,
                lookback_days,
                max_items_per_query,
                now,
                source_name="google",
                hl="en-US",
                gl="US",
                ceid="US:en",
            )
        return fetch_google_news_rss(
            client,
            query,
            lookback_days,
            max_items_per_query,
            now,
            source_name="google",
            hl="zh-CN",
            gl="CN",
            ceid="CN:zh-Hans",
        )
    if source == "baidu":
        return []
    raise ValueError(f"unsupported source: {source}")


def get_taxonomy_level_counts(taxonomy: list[dict[str, Any]]) -> dict[str, int]:
    level1 = len(taxonomy)
    level2 = sum(len(item.get("level2") or []) for item in taxonomy)
    level3 = sum(len(level2_item.get("level3") or []) for item in taxonomy for level2_item in item.get("level2") or [])
    level4 = sum(
        len(level3_item.get("level4") or [])
        for item in taxonomy
        for level2_item in item.get("level2") or []
        for level3_item in level2_item.get("level3") or []
    )
    # A股默认三级词表无level4时，按三级数量回填四级统计，方便与美股同层级对齐。
    if level4 == 0:
        level4 = level3
    return {"level1": level1, "level2": level2, "level3": level3, "level4": level4}


def build_universe_keys(taxonomy: list[dict[str, Any]]) -> dict[str, list[Any]]:
    keys_level1: list[str] = []
    keys_level2: list[tuple[str, str]] = []
    keys_level3: list[tuple[str, str, str]] = []
    keys_level4: list[tuple[str, str, str, str]] = []
    for sector in taxonomy:
        level1_name = str(sector.get("level1") or "").strip()
        if not level1_name:
            continue
        keys_level1.append(level1_name)
        for level2 in sector.get("level2") or []:
            level2_name = str(level2.get("name") or "").strip()
            if not level2_name:
                continue
            keys_level2.append((level1_name, level2_name))
            for level3 in level2.get("level3") or []:
                level3_name = str(level3.get("name") or "").strip()
                if not level3_name:
                    continue
                keys_level3.append((level1_name, level2_name, level3_name))
                level4_items = level3.get("level4") or []
                if not level4_items:
                    keys_level4.append((level1_name, level2_name, level3_name, level3_name))
                    continue
                for level4 in level4_items:
                    level4_name = str(level4.get("name") or "").strip()
                    if not level4_name:
                        continue
                    keys_level4.append((level1_name, level2_name, level3_name, level4_name))
    return {
        "level1": keys_level1,
        "level2": keys_level2,
        "level3": keys_level3,
        "level4": keys_level4,
    }


def pick_top_rows(counter: Counter[Any], universe_keys: list[Any], top_n: int) -> list[tuple[Any, int]]:
    wanted = max(1, top_n)
    picked = counter.most_common(wanted)
    seen = {item[0] for item in picked}
    if len(picked) >= wanted:
        return picked
    for key in universe_keys:
        if key in seen:
            continue
        picked.append((key, 0))
        seen.add(key)
        if len(picked) >= wanted:
            break
    return picked


def format_counter_rows(counter_rows: list[tuple[Any, int]]) -> list[dict[str, Any]]:
    out_rows: list[dict[str, Any]] = []
    for key, frequency in counter_rows:
        if isinstance(key, tuple):
            out_rows.append({"path": list(key), "frequency": frequency})
        else:
            out_rows.append({"path": [str(key)], "frequency": frequency})
    return out_rows


def append_unique_items(
    all_items: list[NewsItem],
    dedup_keys: set[str],
    new_items: Iterable[NewsItem],
    target_news_count: int,
) -> bool:
    for item in new_items:
        key = f"{item.title}|{item.published_at.strftime('%Y-%m-%d %H')}"
        if key in dedup_keys:
            continue
        dedup_keys.add(key)
        all_items.append(item)
        if target_news_count > 0 and len(all_items) >= target_news_count:
            return True
    return False


def resolve_market_sources(requested_sources: list[str], market: str) -> list[str]:
    allowed_sources = {"baidu", "bing", "google"}
    filtered = [source for source in requested_sources if source in allowed_sources]
    if market == "us_gics":
        filtered = [source for source in filtered if source != "baidu"]
    return list(dict.fromkeys(filtered))


def collect_market_news(
    market: str,
    queries: list[str],
    sources: list[str],
    client: RetryClient,
    lookback_days: int,
    max_items_per_query: int,
    max_items_per_feed: int,
    target_news_count: int,
    now: datetime,
    debug: bool,
) -> dict[str, Any]:
    all_items: list[NewsItem] = []
    dedup_keys: set[str] = set()
    failed_queries: list[dict[str, str]] = []
    source_item_counts: Counter[str] = Counter()
    source_success_queries: Counter[str] = Counter()
    target_reached = False

    if market == "a_share" and "baidu" in sources:
        baidu_items, baidu_errors = fetch_baidu_rss(
            client=client,
            lookback_days=lookback_days,
            max_items_per_feed=max(10, max_items_per_feed),
            now=now,
        )
        source_item_counts["baidu"] += len(baidu_items)
        if baidu_items:
            source_success_queries["baidu"] += 1
        failed_queries.extend(baidu_errors)
        target_reached = append_unique_items(all_items, dedup_keys, baidu_items, target_news_count)

    query_sources = [source for source in sources if source != "baidu"]
    for query in queries:
        if target_reached:
            break
        query_has_result = False
        for source in query_sources:
            if target_reached:
                break
            try:
                items = fetch_by_source(
                    source=source,
                    client=client,
                    query=query,
                    lookback_days=lookback_days,
                    max_items_per_query=max(10, max_items_per_query),
                    now=now,
                    market=market,
                )
            except Exception as exc:  # noqa: BLE001
                failed_queries.append({"query": query, "source": source, "error": str(exc)})
                continue

            if items:
                query_has_result = True
                source_success_queries[source] += 1
                source_item_counts[source] += len(items)
                target_reached = append_unique_items(all_items, dedup_keys, items, target_news_count)

        if not query_has_result and debug:
            failed_queries.append({"query": query, "source": ",".join(sources), "error": "empty result"})

    return {
        "market": market,
        "items": all_items,
        "sources": sources,
        "target_reached": target_reached,
        "failed_queries": failed_queries,
        "source_item_counts": dict(source_item_counts),
        "source_success_queries": dict(source_success_queries),
        "news_count": len(all_items),
        "query_count": len(queries),
    }


def main() -> None:
    args = parse_args()
    now = datetime.now()
    lookback_days = max(1, args.lookback_days)
    top_level1 = max(1, args.top_level1)
    top_level2 = max(1, args.top_level2)
    top_level3 = max(1, args.top_level3)
    top_level4 = max(1, args.top_level4)
    target_news_count = max(0, args.target_news_count)

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parents[1]
    output_path = (
        Path(args.output_path)
        if args.output_path
        else repo_root / "output" / f"industry_term_frequency_{now.strftime('%Y%m%d')}.csv"
    )
    a_taxonomy_path = (
        Path(args.taxonomy_path)
        if args.taxonomy_path
        else resolve_default_taxonomy_path(
            script_dir,
            default_rel_path=DEFAULT_A_SHARE_TAXONOMY_FILE,
            legacy_file_name="a_share_sw_taxonomy.json",
        )
    )
    us_taxonomy_path = (
        Path(args.us_taxonomy_path)
        if args.us_taxonomy_path
        else resolve_default_taxonomy_path(
            script_dir,
            default_rel_path=DEFAULT_US_TAXONOMY_FILE,
            legacy_file_name="gics_us_taxonomy.json",
        )
    )

    a_taxonomy = load_taxonomy(a_taxonomy_path, default_taxonomy=DEFAULT_TAXONOMY)
    us_taxonomy = load_taxonomy(us_taxonomy_path, default_taxonomy=[])
    if not a_taxonomy:
        raise ValueError(
            f"A股行业词表为空，无法执行。请检查 --taxonomy-path 或 src/{DEFAULT_A_SHARE_TAXONOMY_FILE}。"
        )
    if not us_taxonomy:
        raise ValueError(f"美股GICS词表为空，无法执行。请检查 --us-taxonomy-path 或 src/{DEFAULT_US_TAXONOMY_FILE}。")

    a_queries = build_queries(
        a_taxonomy,
        args.query,
        DEFAULT_A_SHARE_QUERIES,
        lambda level1: [f"{level1} 行业 政策", f"{level1} 行业 新闻"],
    )
    us_queries = build_queries(
        us_taxonomy,
        args.query,
        DEFAULT_US_QUERIES,
        lambda level1: [f"{level1} sector news"],
    )
    requested_sources = [s.strip().lower() for s in str(args.sources).split(",") if s.strip()]
    if not requested_sources:
        requested_sources = ["baidu", "bing", "google"]
    a_sources = resolve_market_sources(requested_sources, "a_share")
    us_sources = resolve_market_sources(requested_sources, "us_gics")

    client = RetryClient(max_retry=max(1, args.max_retry), timeout_sec=30)
    a_result = collect_market_news(
        market="a_share",
        queries=a_queries,
        sources=a_sources,
        client=client,
        lookback_days=lookback_days,
        max_items_per_query=args.max_items_per_query,
        max_items_per_feed=args.max_items_per_feed,
        target_news_count=target_news_count,
        now=now,
        debug=args.debug,
    )
    us_result = collect_market_news(
        market="us_gics",
        queries=us_queries,
        sources=us_sources,
        client=client,
        lookback_days=lookback_days,
        max_items_per_query=args.max_items_per_query,
        max_items_per_feed=args.max_items_per_feed,
        target_news_count=target_news_count,
        now=now,
        debug=args.debug,
    )

    a_items = a_result["items"]
    us_items = us_result["items"]
    total_news_count = len(a_items) + len(us_items)

    a_stats = aggregate_industry_frequency(a_items, a_taxonomy)
    us_stats = aggregate_industry_frequency(us_items, us_taxonomy)
    a_universe = build_universe_keys(a_taxonomy)
    us_universe = build_universe_keys(us_taxonomy)

    a_hot_l1 = pick_top_rows(a_stats["level1"], a_universe["level1"], top_level1)
    a_hot_l2 = pick_top_rows(a_stats["level2"], a_universe["level2"], top_level2)
    a_hot_l3 = pick_top_rows(a_stats["level3"], a_universe["level3"], top_level3)
    a_hot_l4 = pick_top_rows(a_stats["level4"], a_universe["level4"], top_level4)

    us_hot_l1 = pick_top_rows(us_stats["level1"], us_universe["level1"], top_level1)
    us_hot_l2 = pick_top_rows(us_stats["level2"], us_universe["level2"], top_level2)
    us_hot_l3 = pick_top_rows(us_stats["level3"], us_universe["level3"], top_level3)
    us_hot_l4 = pick_top_rows(us_stats["level4"], us_universe["level4"], top_level4)

    market_rows = [
        {
            "market": "a_share",
            "level1_rows": a_hot_l1,
            "level2_rows": a_hot_l2,
            "level3_rows": a_hot_l3,
            "level4_rows": a_hot_l4,
            "news_count": len(a_items),
        },
        {
            "market": "us_gics",
            "level1_rows": us_hot_l1,
            "level2_rows": us_hot_l2,
            "level3_rows": us_hot_l3,
            "level4_rows": us_hot_l4,
            "news_count": len(us_items),
        },
    ]

    start_date = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")
    write_result_csv(
        output_path=output_path,
        market_rows=market_rows,
        lookback_days=lookback_days,
        start_date=start_date,
        end_date=end_date,
        news_count=total_news_count,
    )

    print(
        json.dumps(
            {
                "success": True,
                "output_path": str(output_path.resolve()),
                "lookback_days": lookback_days,
                "requested_sources": requested_sources,
                "news_count": total_news_count,
                "target_news_count": target_news_count,
                "market_collection": {
                    "a_share": {
                        "sources": a_sources,
                        "query_count": a_result["query_count"],
                        "news_count": a_result["news_count"],
                        "target_reached": a_result["target_reached"],
                        "source_item_counts": a_result["source_item_counts"],
                        "source_success_queries": a_result["source_success_queries"],
                        "failed_query_count": len(a_result["failed_queries"]),
                        "failed_queries_sample": a_result["failed_queries"][:20],
                    },
                    "us_gics": {
                        "sources": us_sources,
                        "query_count": us_result["query_count"],
                        "news_count": us_result["news_count"],
                        "target_reached": us_result["target_reached"],
                        "source_item_counts": us_result["source_item_counts"],
                        "source_success_queries": us_result["source_success_queries"],
                        "failed_query_count": len(us_result["failed_queries"]),
                        "failed_queries_sample": us_result["failed_queries"][:20],
                    },
                },
                "top_n": {
                    "level1": top_level1,
                    "level2": top_level2,
                    "level3": top_level3,
                    "level4": top_level4,
                },
                "taxonomy_counts": {
                    "a_share": get_taxonomy_level_counts(a_taxonomy),
                    "us_gics": get_taxonomy_level_counts(us_taxonomy),
                },
                "market_results": {
                    "a_share": {
                        "level1": format_counter_rows(a_hot_l1),
                        "level2": format_counter_rows(a_hot_l2),
                        "level3": format_counter_rows(a_hot_l3),
                        "level4": format_counter_rows(a_hot_l4),
                    },
                    "us_gics": {
                        "level1": format_counter_rows(us_hot_l1),
                        "level2": format_counter_rows(us_hot_l2),
                        "level3": format_counter_rows(us_hot_l3),
                        "level4": format_counter_rows(us_hot_l4),
                    },
                },
                "warning": (
                    "no news collected; check failed_queries_sample and network/proxy settings"
                    if total_news_count == 0
                    else ""
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()


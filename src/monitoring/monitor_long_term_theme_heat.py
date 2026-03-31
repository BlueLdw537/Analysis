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
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus
from urllib.request import Request, build_opener


DEFAULT_QUERIES = [
    "中国 产业 政策",
    "中国 行业 新闻",
    "A股 行业",
    "宏观 经济 产业链",
]


@dataclass
class NewsItem:
    title: str
    summary: str
    published_at: datetime
    source_name: str
    source_query: str


@dataclass
class IndustryTerm:
    level: str
    name: str
    aliases: list[str]
    code: str
    level1: str
    level2: str


class RetryClient:
    def __init__(self, max_retry: int, timeout_sec: int = 30) -> None:
        self.max_retry = max_retry
        self.timeout_sec = timeout_sec
        self.opener = build_opener()
        self.user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )

    def get_text(self, url: str) -> str:
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retry + 1):
            try:
                request = Request(url, headers={"User-Agent": self.user_agent})
                with self.opener.open(request, timeout=self.timeout_sec) as resp:
                    raw = resp.read()
                    charset = resp.headers.get_content_charset() or "utf-8"
                    try:
                        return raw.decode(charset, errors="replace")
                    except LookupError:
                        return raw.decode("utf-8", errors="replace")
            except (HTTPError, URLError, TimeoutError, OSError) as exc:
                last_exc = exc
                if attempt >= self.max_retry:
                    break
                time.sleep(2**attempt)
        raise RuntimeError(f"Request failed after retries: {url}") from last_exc

    def get_json(self, url: str) -> dict[str, Any]:
        payload = json.loads(self.get_text(url))
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected JSON payload type from: {url}")
        return payload


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    src_dir = script_dir.parent if script_dir.name == "monitoring" else script_dir
    parser = argparse.ArgumentParser(description="按申万行业词表统计公开讨论命中次数并输出Top行业")
    parser.add_argument("--taxonomy-path", default=str(src_dir / "taxonomy" / "a_share_sw_taxonomy.json"), help="行业词表 JSON 路径")
    parser.add_argument("--lookback-days", type=int, default=10, help="抓取时间范围（天），默认10")
    parser.add_argument("--top-n", type=int, default=10, help="输出热度最高的行业数量，默认10")
    parser.add_argument("--output-path", default="", help="输出CSV路径")
    parser.add_argument("--max-retry", type=int, default=3, help="网络重试次数，默认3")
    parser.add_argument("--max-items-per-query", type=int, default=500, help="每个查询最多解析条目数，默认500")
    parser.add_argument(
        "--sources",
        default="bing",
        help="新闻源，逗号分隔：bing,google,google_en,gdelt；默认 bing",
    )
    parser.add_argument(
        "--query",
        action="append",
        default=[],
        help="可重复传入公开搜索词；不传则自动拼接行业查询",
    )
    parser.add_argument("--debug", action="store_true", help="输出抓取诊断信息")
    return parser.parse_args()


def parse_datetime(raw: Any, fallback: datetime) -> datetime:
    if raw is None:
        return fallback
    text = str(raw).strip()
    if not text:
        return fallback
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y%m%d%H%M%S",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        parsed = parsedate_to_datetime(text)
        return parsed.astimezone().replace(tzinfo=None) if parsed.tzinfo else parsed
    except Exception:  # noqa: BLE001
        return fallback


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def parse_rss_items(xml_text: str, query: str, source_name: str, now: datetime, max_items: int) -> list[NewsItem]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    nodes = root.findall(".//item")
    if not nodes:
        nodes = root.findall(".//{http://www.w3.org/2005/Atom}entry")
    if not nodes:
        nodes = root.findall(".//entry")

    out: list[NewsItem] = []
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
        raw_date = (
            node.findtext("pubDate")
            or node.findtext("published")
            or node.findtext("updated")
            or node.findtext("{http://www.w3.org/2005/Atom}published")
            or node.findtext("{http://www.w3.org/2005/Atom}updated")
            or ""
        ).strip()
        out.append(
            NewsItem(
                title=title,
                summary=summary,
                published_at=parse_datetime(raw_date, fallback=now),
                source_name=source_name,
                source_query=query,
            )
        )
    return out


def fetch_bing_news_rss(client: RetryClient, query: str, lookback_days: int, max_items: int, now: datetime) -> list[NewsItem]:
    url = f"https://www.bing.com/news/search?q={quote_plus(query)}&format=rss&setlang=zh-Hans"
    items = parse_rss_items(client.get_text(url), query, "bing", now, max_items)
    cutoff = now - timedelta(days=lookback_days)
    return [item for item in items if item.published_at >= cutoff]


def fetch_google_news_rss(
    client: RetryClient,
    query: str,
    lookback_days: int,
    max_items: int,
    now: datetime,
    source_name: str,
    hl: str,
    gl: str,
    ceid: str,
) -> list[NewsItem]:
    query_text = f"{query} when:{lookback_days}d"
    url = f"https://news.google.com/rss/search?q={quote_plus(query_text)}&hl={hl}&gl={gl}&ceid={ceid}"
    items = parse_rss_items(client.get_text(url), query, source_name, now, max_items)
    cutoff = now - timedelta(days=lookback_days)
    return [item for item in items if item.published_at >= cutoff]


def fetch_gdelt_items(client: RetryClient, query: str, lookback_days: int, max_items: int, now: datetime) -> list[NewsItem]:
    url = (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={quote_plus(query)}&mode=ArtList&maxrecords={max(10, max_items)}"
        f"&format=json&timespan={lookback_days}d"
    )
    payload = client.get_json(url)
    articles = payload.get("articles") or []
    if not isinstance(articles, list):
        return []
    cutoff = now - timedelta(days=lookback_days)
    out: list[NewsItem] = []
    for article in articles:
        if not isinstance(article, dict):
            continue
        raw_date = article.get("seendate") or article.get("date") or ""
        published_at = parse_datetime(raw_date, fallback=now)
        if published_at < cutoff:
            continue
        out.append(
            NewsItem(
                title=str(article.get("title") or "").strip(),
                summary=str(article.get("domain") or article.get("seendate") or "").strip(),
                published_at=published_at,
                source_name="gdelt",
                source_query=query,
            )
        )
    return out


def fetch_by_source(
    source: str,
    client: RetryClient,
    query: str,
    lookback_days: int,
    max_items: int,
    now: datetime,
) -> list[NewsItem]:
    if source == "bing":
        return fetch_bing_news_rss(client, query, lookback_days, max_items, now)
    if source == "google":
        return fetch_google_news_rss(client, query, lookback_days, max_items, now, "google", "zh-CN", "CN", "CN:zh-Hans")
    if source == "google_en":
        return fetch_google_news_rss(client, query, lookback_days, max_items, now, "google_en", "en-US", "US", "US:en")
    if source == "gdelt":
        return fetch_gdelt_items(client, query, lookback_days, max_items, now)
    raise ValueError(f"unsupported source: {source}")


def load_taxonomy(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("taxonomy 文件必须是 JSON 数组。")
    return payload


def flatten_industries(nodes: list[dict[str, Any]]) -> list[IndustryTerm]:
    out: list[IndustryTerm] = []
    for level1 in nodes:
        level1_name = str(level1.get("level1") or "").strip()
        if not level1_name:
            continue
        aliases = level1.get("aliases") or [level1_name]
        out.append(
            IndustryTerm(
                level="level1",
                name=level1_name,
                aliases=[str(item).strip() for item in aliases if str(item).strip()],
                code=str(level1.get("code") or ""),
                level1=level1_name,
                level2="",
            )
        )
        for level2 in level1.get("level2") or []:
            level2_name = str(level2.get("name") or "").strip()
            if not level2_name:
                continue
            aliases2 = level2.get("aliases") or [level2_name]
            out.append(
                IndustryTerm(
                    level="level2",
                    name=level2_name,
                    aliases=[str(item).strip() for item in aliases2 if str(item).strip()],
                    code=str(level2.get("code") or ""),
                    level1=level1_name,
                    level2=level2_name,
                )
            )
            for level3 in level2.get("level3") or []:
                level3_name = str(level3.get("name") or "").strip()
                if not level3_name:
                    continue
                aliases3 = level3.get("aliases") or [level3_name]
                out.append(
                    IndustryTerm(
                        level="level3",
                        name=level3_name,
                        aliases=[str(item).strip() for item in aliases3 if str(item).strip()],
                        code=str(level3.get("code") or ""),
                        level1=level1_name,
                        level2=level2_name,
                    )
                )
    return out


def build_queries(custom_queries: list[str], taxonomy: list[dict[str, Any]]) -> list[str]:
    cleaned = [item.strip() for item in custom_queries if item.strip()]
    if cleaned:
        return list(dict.fromkeys(cleaned))
    queries = list(DEFAULT_QUERIES)
    for node in taxonomy:
        level1_name = str(node.get("level1") or "").strip()
        if level1_name:
            queries.append(f"{level1_name} 行业 新闻")
            queries.append(f"{level1_name} 行业 政策")
    return list(dict.fromkeys(queries))


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
    unique_aliases = list(dict.fromkeys([alias.strip() for alias in aliases if alias.strip()]))
    return sum(count_alias_occurrences(text_lower, alias) for alias in unique_aliases)


def aggregate_hits(items: list[NewsItem], industries: list[IndustryTerm]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for item in items:
        text = f"{item.title} {item.summary}".strip().lower()
        if not text:
            continue
        for industry in industries:
            hits = count_term_hits(text, industry.aliases)
            if hits > 0:
                counter[industry.code or industry.name] += hits
    return counter


def write_result_csv(output_path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_parent_dir(output_path)
    headers = ["rank", "level", "industry_name", "industry_code", "level1", "level2", "hit_count"]
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


def main() -> None:
    args = parse_args()
    now = datetime.now()
    lookback_days = max(1, args.lookback_days)
    top_n = max(1, args.top_n)
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parents[1]
    taxonomy_path = Path(args.taxonomy_path)
    output_path = Path(args.output_path) if args.output_path else repo_root / "output" / f"industry_top_hits_{now.strftime('%Y%m%d')}.csv"

    taxonomy = load_taxonomy(taxonomy_path)
    industries = flatten_industries(taxonomy)
    if not industries:
        raise ValueError("taxonomy 中未解析到任何行业。")

    queries = build_queries(args.query, taxonomy)
    sources = [item.strip().lower() for item in str(args.sources).split(",") if item.strip()]
    client = RetryClient(max_retry=max(1, args.max_retry), timeout_sec=30)

    all_items: list[NewsItem] = []
    dedup_keys: set[str] = set()
    failed_queries: list[dict[str, str]] = []
    source_item_counts: Counter[str] = Counter()
    for query in queries:
        for source in sources:
            try:
                items = fetch_by_source(source, client, query, lookback_days, max(10, args.max_items_per_query), now)
            except Exception as exc:  # noqa: BLE001
                if args.debug:
                    failed_queries.append({"query": query, "source": source, "error": str(exc)})
                continue
            source_item_counts[source] += len(items)
            for item in items:
                key = f"{item.title}|{item.published_at.strftime('%Y-%m-%d %H')}"
                if key in dedup_keys:
                    continue
                dedup_keys.add(key)
                all_items.append(item)

    hit_counter = aggregate_hits(all_items, industries)
    industry_map = {term.code or term.name: term for term in industries}
    top_codes = hit_counter.most_common(top_n)
    rows: list[dict[str, Any]] = []
    for idx, (key, hit_count) in enumerate(top_codes, start=1):
        term = industry_map[key]
        rows.append(
            {
                "rank": idx,
                "level": term.level,
                "industry_name": term.name,
                "industry_code": term.code,
                "level1": term.level1,
                "level2": term.level2,
                "hit_count": hit_count,
            }
        )

    write_result_csv(output_path, rows)
    print(
        json.dumps(
            {
                "success": True,
                "taxonomy_path": str(taxonomy_path.resolve()),
                "output_path": str(output_path.resolve()),
                "lookback_days": lookback_days,
                "query_count": len(queries),
                "industry_count": len(industries),
                "news_count": len(all_items),
                "sources": sources,
                "source_item_counts": dict(source_item_counts),
                "top_n": top_n,
                "top_industries": rows,
                "failed_queries_sample": failed_queries[:20],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

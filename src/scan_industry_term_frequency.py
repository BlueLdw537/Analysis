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
from urllib.parse import quote_plus

import requests


DEFAULT_TAXONOMY: list[dict[str, Any]] = [
    {
        "level1": "信息技术",
        "aliases": ["信息技术", "数字经济", "科技"],
        "level2": [
            {
                "name": "人工智能",
                "aliases": ["人工智能", "AI", "AIGC", "生成式AI", "大模型"],
                "level3": [
                    {"name": "大模型", "aliases": ["大模型", "LLM", "Foundation Model"]},
                    {"name": "智能体", "aliases": ["智能体", "AI Agent", "Agent"]},
                    {"name": "算力", "aliases": ["算力", "GPU", "AI芯片", "加速卡"]},
                ],
            },
            {
                "name": "半导体",
                "aliases": ["半导体", "芯片", "集成电路", "IC"],
                "level3": [
                    {"name": "半导体设备", "aliases": ["半导体设备", "刻蚀机", "光刻机"]},
                    {"name": "半导体材料", "aliases": ["半导体材料", "硅片", "光刻胶"]},
                    {"name": "封测", "aliases": ["封测", "先进封装"]},
                ],
            },
            {
                "name": "云计算",
                "aliases": ["云计算", "数据中心", "IDC", "云服务"],
                "level3": [
                    {"name": "数据中心", "aliases": ["数据中心", "IDC机房"]},
                    {"name": "服务器", "aliases": ["服务器", "云服务器"]},
                    {"name": "网络安全", "aliases": ["网络安全", "零信任", "数据安全"]},
                ],
            },
        ],
    },
    {
        "level1": "医药生物",
        "aliases": ["医药生物", "生物医药", "医疗健康"],
        "level2": [
            {
                "name": "创新药",
                "aliases": ["创新药", "新药研发", "创新疗法"],
                "level3": [
                    {"name": "ADC", "aliases": ["ADC", "抗体偶联药物"]},
                    {"name": "GLP-1", "aliases": ["GLP-1", "减重药"]},
                    {"name": "PD-1", "aliases": ["PD-1", "免疫治疗"]},
                ],
            },
            {
                "name": "医疗器械",
                "aliases": ["医疗器械", "医械", "医疗设备"],
                "level3": [
                    {"name": "体外诊断", "aliases": ["体外诊断", "IVD"]},
                    {"name": "影像设备", "aliases": ["影像设备", "CT", "MRI"]},
                    {"name": "手术机器人", "aliases": ["手术机器人", "达芬奇"]},
                ],
            },
            {
                "name": "CXO",
                "aliases": ["CXO", "CRO", "CDMO", "医药外包"],
                "level3": [
                    {"name": "临床CRO", "aliases": ["临床CRO", "临床外包"]},
                    {"name": "CDMO", "aliases": ["CDMO", "工艺开发"]},
                    {"name": "原料药", "aliases": ["原料药", "API"]},
                ],
            },
        ],
    },
    {
        "level1": "新能源",
        "aliases": ["新能源", "绿色能源", "新型能源"],
        "level2": [
            {
                "name": "新能源汽车",
                "aliases": ["新能源汽车", "电动车", "EV", "智能汽车"],
                "level3": [
                    {"name": "智能驾驶", "aliases": ["智能驾驶", "自动驾驶", "NOA"]},
                    {"name": "热管理", "aliases": ["热管理", "热泵"]},
                    {"name": "车载芯片", "aliases": ["车载芯片", "智能座舱芯片"]},
                ],
            },
            {
                "name": "储能",
                "aliases": ["储能", "储能电站", "电化学储能"],
                "level3": [
                    {"name": "锂电池", "aliases": ["锂电池", "动力电池", "电池"]},
                    {"name": "逆变器", "aliases": ["逆变器", "PCS"]},
                    {"name": "BMS", "aliases": ["BMS", "电池管理系统"]},
                ],
            },
            {
                "name": "光伏风电",
                "aliases": ["光伏", "风电", "新能源发电"],
                "level3": [
                    {"name": "光伏组件", "aliases": ["光伏组件", "电池片"]},
                    {"name": "海上风电", "aliases": ["海上风电", "海风"]},
                    {"name": "硅料", "aliases": ["硅料", "硅片"]},
                ],
            },
        ],
    },
    {
        "level1": "消费服务",
        "aliases": ["消费服务", "消费", "内需"],
        "level2": [
            {
                "name": "消费电子",
                "aliases": ["消费电子", "智能手机", "可穿戴"],
                "level3": [
                    {"name": "ARVR", "aliases": ["AR", "VR", "MR"]},
                    {"name": "折叠屏", "aliases": ["折叠屏"]},
                    {"name": "声学", "aliases": ["声学器件", "TWS"]},
                ],
            },
            {
                "name": "食品饮料",
                "aliases": ["食品饮料", "白酒", "乳制品", "饮料"],
                "level3": [
                    {"name": "白酒", "aliases": ["白酒"]},
                    {"name": "啤酒", "aliases": ["啤酒"]},
                    {"name": "休闲食品", "aliases": ["休闲食品", "零食"]},
                ],
            },
            {
                "name": "旅游酒店",
                "aliases": ["旅游", "酒店", "出行", "文旅"],
                "level3": [
                    {"name": "免税", "aliases": ["免税"]},
                    {"name": "景区", "aliases": ["景区", "景点"]},
                    {"name": "航空出行", "aliases": ["航空出行", "机票"]},
                ],
            },
        ],
    },
    {
        "level1": "资源周期",
        "aliases": ["资源周期", "周期行业", "大宗商品"],
        "level2": [
            {
                "name": "有色金属",
                "aliases": ["有色金属", "铜", "铝", "锂", "稀土"],
                "level3": [
                    {"name": "铜", "aliases": ["铜"]},
                    {"name": "铝", "aliases": ["铝"]},
                    {"name": "稀土", "aliases": ["稀土"]},
                ],
            },
            {
                "name": "油气化工",
                "aliases": ["石油", "天然气", "化工", "炼化", "油气"],
                "level3": [
                    {"name": "原油", "aliases": ["原油", "油价"]},
                    {"name": "天然气", "aliases": ["天然气", "LNG"]},
                    {"name": "煤化工", "aliases": ["煤化工"]},
                ],
            },
            {
                "name": "煤炭钢铁",
                "aliases": ["煤炭", "钢铁", "焦煤", "焦炭"],
                "level3": [
                    {"name": "动力煤", "aliases": ["动力煤"]},
                    {"name": "焦煤焦炭", "aliases": ["焦煤", "焦炭"]},
                    {"name": "螺纹钢", "aliases": ["螺纹钢"]},
                ],
            },
        ],
    },
    {
        "level1": "金融地产",
        "aliases": ["金融地产", "地产金融", "金融"],
        "level2": [
            {
                "name": "银行保险",
                "aliases": ["银行", "保险"],
                "level3": [
                    {"name": "城商行", "aliases": ["城商行"]},
                    {"name": "寿险", "aliases": ["寿险"]},
                    {"name": "财险", "aliases": ["财险"]},
                ],
            },
            {
                "name": "券商",
                "aliases": ["券商", "证券公司", "投行"],
                "level3": [
                    {"name": "经纪业务", "aliases": ["经纪业务"]},
                    {"name": "自营业务", "aliases": ["自营业务"]},
                    {"name": "投行业务", "aliases": ["投行业务", "IPO"]},
                ],
            },
            {
                "name": "房地产",
                "aliases": ["房地产", "物业", "保交楼", "保障房"],
                "level3": [
                    {"name": "物业服务", "aliases": ["物业服务", "物业管理"]},
                    {"name": "保障房", "aliases": ["保障房"]},
                    {"name": "城中村改造", "aliases": ["城中村改造"]},
                ],
            },
        ],
    },
]

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
    source_query: str
    source_name: str


class RetryClient:
    def __init__(self, max_retry: int, timeout_sec: int = 30) -> None:
        self.max_retry = max_retry
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        self.session.trust_env = True
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            }
        )

    def get_text(self, url: str) -> str:
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retry + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout_sec)
                resp.raise_for_status()
                resp.encoding = resp.apparent_encoding or resp.encoding
                return resp.text
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
    parser.add_argument("--lookback-days", type=int, default=10, help="抓取时间范围（天），默认10")
    parser.add_argument("--top-level1", type=int, default=3, help="输出一级行业TopN，默认3")
    parser.add_argument("--top-level2", type=int, default=10, help="输出二级行业TopN，默认10")
    parser.add_argument("--top-level3", type=int, default=10, help="输出三级行业TopN，默认10")
    parser.add_argument("--output-path", default="", help="输出CSV路径")
    parser.add_argument("--max-retry", type=int, default=3, help="网络重试次数，默认3")
    parser.add_argument("--max-items-per-query", type=int, default=120, help="每个查询最多解析条目数")
    parser.add_argument(
        "--sources",
        default="bing,google,google_en,gdelt",
        help="新闻源，逗号分隔：bing,google,google_en,gdelt",
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
        help="可选行业词表JSON路径。为空时使用内置词表。",
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
) -> list[NewsItem]:
    url = f"https://www.bing.com/news/search?q={quote_plus(query)}&format=rss&setlang=zh-Hans"
    xml_text = client.get_text(url)
    parsed = parse_rss_items(xml_text, query=query, source_name="bing", now=now, max_items=max_items_per_query)
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


def normalize_term(raw: Any, fallback_name: str = "") -> dict[str, Any] | None:
    if isinstance(raw, str):
        name = raw.strip()
        if not name:
            return None
        return {"name": name, "aliases": [name], "level3": []}

    if not isinstance(raw, dict):
        return None
    name = str(raw.get("name") or raw.get("level2") or fallback_name).strip()
    if not name:
        return None
    aliases_raw = raw.get("aliases") or raw.get("keywords") or raw.get("level2_keywords") or [name]
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
                normalized["level3"] = []
                level3.append(normalized)

    return {"name": name, "aliases": aliases, "level3": level3}


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


def load_taxonomy(taxonomy_path: Path | None) -> list[dict[str, Any]]:
    if taxonomy_path and taxonomy_path.exists():
        payload = taxonomy_path.read_text(encoding="utf-8")
        loaded = json.loads(payload)
        normalized = normalize_taxonomy(loaded)
        if normalized:
            return normalized
    return normalize_taxonomy(DEFAULT_TAXONOMY)


def build_queries(taxonomy: list[dict[str, Any]], custom_queries: list[str]) -> list[str]:
    queries = [q.strip() for q in custom_queries if q.strip()]
    if queries:
        return list(dict.fromkeys(queries))

    auto_queries = list(DEFAULT_QUERIES)
    for item in taxonomy:
        level1 = str(item.get("level1") or "").strip()
        if not level1:
            continue
        auto_queries.append(f"{level1} 行业 政策")
        auto_queries.append(f"{level1} 行业 新闻")
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
) -> tuple[Counter[str], Counter[tuple[str, str]], Counter[tuple[str, str, str]]]:
    level1_counter: Counter[str] = Counter()
    level2_counter: Counter[tuple[str, str]] = Counter()
    level3_counter: Counter[tuple[str, str, str]] = Counter()

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
            for level2 in sector.get("level2") or []:
                l2_name = str(level2.get("name") or "").strip()
                if not l2_name:
                    continue
                l2_aliases = level2.get("aliases") or [l2_name]
                l2_hits = count_term_hits(text, l2_aliases)
                if l2_hits > 0:
                    level2_counter[(level1_name, l2_name)] += l2_hits
                    level2_total += l2_hits

                for level3 in level2.get("level3") or []:
                    l3_name = str(level3.get("name") or "").strip()
                    if not l3_name:
                        continue
                    l3_aliases = level3.get("aliases") or [l3_name]
                    l3_hits = count_term_hits(text, l3_aliases)
                    if l3_hits > 0:
                        level3_counter[(level1_name, l2_name, l3_name)] += l3_hits
                        level3_total += l3_hits

            total_hits = l1_hits + level2_total + level3_total
            if total_hits > 0:
                level1_counter[level1_name] += total_hits

    return level1_counter, level2_counter, level3_counter


def write_blank_row(writer: csv.DictWriter) -> None:
    writer.writerow(
        {
            "level": "",
            "rank": "",
            "level1_industry": "",
            "level2_industry": "",
            "level3_industry": "",
            "frequency": "",
            "lookback_days": "",
            "start_date": "",
            "end_date": "",
            "news_count": "",
        }
    )


def write_result_csv(
    output_path: Path,
    top_level1_rows: list[tuple[str, int]],
    top_level2_rows: list[tuple[tuple[str, str], int]],
    top_level3_rows: list[tuple[tuple[str, str, str], int]],
    lookback_days: int,
    start_date: str,
    end_date: str,
    news_count: int,
) -> None:
    ensure_parent_dir(output_path)
    headers = [
        "level",
        "rank",
        "level1_industry",
        "level2_industry",
        "level3_industry",
        "frequency",
        "lookback_days",
        "start_date",
        "end_date",
        "news_count",
    ]
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

        for rank, (level1, freq) in enumerate(top_level1_rows, start=1):
            writer.writerow(
                {
                    "level": "level1",
                    "rank": rank,
                    "level1_industry": level1,
                    "level2_industry": "",
                    "level3_industry": "",
                    "frequency": freq,
                    "lookback_days": lookback_days,
                    "start_date": start_date,
                    "end_date": end_date,
                    "news_count": news_count,
                }
            )

        write_blank_row(writer)

        for rank, ((level1, level2), freq) in enumerate(top_level2_rows, start=1):
            writer.writerow(
                {
                    "level": "level2",
                    "rank": rank,
                    "level1_industry": level1,
                    "level2_industry": level2,
                    "level3_industry": "",
                    "frequency": freq,
                    "lookback_days": lookback_days,
                    "start_date": start_date,
                    "end_date": end_date,
                    "news_count": news_count,
                }
            )

        write_blank_row(writer)

        for rank, ((level1, level2, level3), freq) in enumerate(top_level3_rows, start=1):
            writer.writerow(
                {
                    "level": "level3",
                    "rank": rank,
                    "level1_industry": level1,
                    "level2_industry": level2,
                    "level3_industry": level3,
                    "frequency": freq,
                    "lookback_days": lookback_days,
                    "start_date": start_date,
                    "end_date": end_date,
                    "news_count": news_count,
                }
            )


def fetch_by_source(
    source: str,
    client: RetryClient,
    query: str,
    lookback_days: int,
    max_items_per_query: int,
    now: datetime,
) -> list[NewsItem]:
    if source == "bing":
        return fetch_bing_news_rss(client, query, lookback_days, max_items_per_query, now)
    if source == "google":
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
    if source == "google_en":
        return fetch_google_news_rss(
            client,
            query,
            lookback_days,
            max_items_per_query,
            now,
            source_name="google_en",
            hl="en-US",
            gl="US",
            ceid="US:en",
        )
    if source == "gdelt":
        return fetch_gdelt_items(client, query, lookback_days, max_items_per_query, now)
    raise ValueError(f"unsupported source: {source}")


def main() -> None:
    args = parse_args()
    now = datetime.now()
    lookback_days = max(1, args.lookback_days)
    top_level1 = max(1, args.top_level1)
    top_level2 = max(1, args.top_level2)
    top_level3 = max(1, args.top_level3)

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    output_path = (
        Path(args.output_path)
        if args.output_path
        else repo_root / "output" / f"industry_term_frequency_{now.strftime('%Y%m%d')}.csv"
    )
    taxonomy_path = Path(args.taxonomy_path) if args.taxonomy_path else None

    taxonomy = load_taxonomy(taxonomy_path)
    if not taxonomy:
        raise ValueError("行业词表为空，无法执行。")

    queries = build_queries(taxonomy, args.query)
    sources = [s.strip().lower() for s in str(args.sources).split(",") if s.strip()]
    if not sources:
        sources = ["bing", "google", "google_en", "gdelt"]

    client = RetryClient(max_retry=max(1, args.max_retry), timeout_sec=30)
    all_items: list[NewsItem] = []
    dedup_keys: set[str] = set()
    failed_queries: list[dict[str, str]] = []
    source_item_counts: Counter[str] = Counter()
    source_success_queries: Counter[str] = Counter()

    for query in queries:
        per_query_items: list[NewsItem] = []
        query_has_result = False
        for source in sources:
            try:
                items = fetch_by_source(
                    source=source,
                    client=client,
                    query=query,
                    lookback_days=lookback_days,
                    max_items_per_query=max(10, args.max_items_per_query),
                    now=now,
                )
            except Exception as exc:  # noqa: BLE001
                failed_queries.append({"query": query, "source": source, "error": str(exc)})
                continue

            if items:
                query_has_result = True
                source_success_queries[source] += 1
                source_item_counts[source] += len(items)
                per_query_items.extend(items)

        if not query_has_result and args.debug:
            failed_queries.append({"query": query, "source": ",".join(sources), "error": "empty result"})

        for item in per_query_items:
            key = f"{item.title}|{item.published_at.strftime('%Y-%m-%d %H')}"
            if key in dedup_keys:
                continue
            dedup_keys.add(key)
            all_items.append(item)

    level1_counter, level2_counter, level3_counter = aggregate_industry_frequency(all_items, taxonomy)
    top_level1_rows = level1_counter.most_common(top_level1)
    top_level2_rows = level2_counter.most_common(top_level2)
    top_level3_rows = level3_counter.most_common(top_level3)

    start_date = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")
    write_result_csv(
        output_path=output_path,
        top_level1_rows=top_level1_rows,
        top_level2_rows=top_level2_rows,
        top_level3_rows=top_level3_rows,
        lookback_days=lookback_days,
        start_date=start_date,
        end_date=end_date,
        news_count=len(all_items),
    )

    print(
        json.dumps(
            {
                "success": True,
                "output_path": str(output_path.resolve()),
                "lookback_days": lookback_days,
                "sources": sources,
                "query_count": len(queries),
                "news_count": len(all_items),
                "source_item_counts": dict(source_item_counts),
                "source_success_queries": dict(source_success_queries),
                "failed_query_count": len(failed_queries),
                "failed_queries_sample": failed_queries[:20],
                "top_level1_count": len(top_level1_rows),
                "top_level2_count": len(top_level2_rows),
                "top_level3_count": len(top_level3_rows),
                "top_level1": [{"industry": k, "frequency": v} for k, v in top_level1_rows],
                "top_level2": [{"level1": k[0], "industry": k[1], "frequency": v} for k, v in top_level2_rows],
                "top_level3": [
                    {"level1": k[0], "level2": k[1], "industry": k[2], "frequency": v}
                    for k, v in top_level3_rows
                ],
                "warning": (
                    "no news collected; check failed_queries_sample and network/proxy settings"
                    if len(all_items) == 0
                    else ""
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

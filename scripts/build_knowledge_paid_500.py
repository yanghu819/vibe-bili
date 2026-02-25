#!/usr/bin/env python3
"""
Build a 500-book list for knowledge-paid content creation (mixed zh/en).

Usage:
python3 build_knowledge_paid_500.py \
  --target-count 500 \
  --lang mixed \
  --with-angle true \
  --out-dir /Users/torusmini/Downloads/clawhy/bilibili-batch-follow/logs
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import random
import re
import time
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import requests


TRACKS = [
    "商业战略",
    "营销增长",
    "写作表达",
    "心理与行为",
    "个人效率",
    "产品与创新",
    "组织管理",
    "谈判销售",
    "叙事与传播",
    "科学思维",
]

TRACK_KEYWORDS = {
    "商业战略": [
        "战略",
        "竞争",
        "商业模式",
        "business",
        "strategy",
        "moat",
        "positioning",
    ],
    "营销增长": ["营销", "增长", "品牌", "流量", "获客", "marketing", "growth", "brand", "sales"],
    "写作表达": ["写作", "表达", "文案", "沟通", "演讲", "writing", "copy", "speech", "rhetoric"],
    "心理与行为": ["心理", "行为", "认知", "习惯", "情绪", "psychology", "behavior", "mindset"],
    "个人效率": ["效率", "时间", "专注", "复盘", "习惯", "productivity", "focus", "deep work"],
    "产品与创新": ["产品", "创新", "设计", "创业", "pm", "product", "innovation", "startup"],
    "组织管理": ["管理", "组织", "领导", "人才", "manager", "leadership", "team", "culture"],
    "谈判销售": ["谈判", "销售", "成交", "客户", "negotiation", "selling", "closing"],
    "叙事与传播": ["叙事", "故事", "传播", "媒体", "story", "narrative", "communication", "media"],
    "科学思维": ["科学", "统计", "概率", "逻辑", "模型", "science", "statistics", "probability", "logic"],
}

TRACK_SUBTRACKS = {
    "商业战略": ["战略框架", "行业分析", "企业案例"],
    "营销增长": ["增长飞轮", "品牌策略", "获客转化"],
    "写作表达": ["结构表达", "说服写作", "口语表达"],
    "心理与行为": ["认知偏差", "行为改变", "心理韧性"],
    "个人效率": ["时间管理", "习惯系统", "执行力"],
    "产品与创新": ["产品方法", "创新方法", "创业实践"],
    "组织管理": ["团队管理", "组织设计", "领导力"],
    "谈判销售": ["谈判框架", "销售流程", "客户沟通"],
    "叙事与传播": ["故事结构", "公众传播", "内容策略"],
    "科学思维": ["概率思维", "统计推断", "实验方法"],
}

CONTENT_FORMS_BY_TRACK = {
    "商业战略": ["直播拆书", "小课"],
    "营销增长": ["口播短视频", "图文长帖"],
    "写作表达": ["图文长帖", "小课"],
    "心理与行为": ["小课", "直播拆书"],
    "个人效率": ["口播短视频", "小课"],
    "产品与创新": ["直播拆书", "训练营"],
    "组织管理": ["小课", "训练营"],
    "谈判销售": ["训练营", "口播短视频"],
    "叙事与传播": ["口播短视频", "图文长帖"],
    "科学思维": ["直播拆书", "小课"],
}

OPENLIBRARY_SUBJECTS = {
    "商业战略": ["business", "strategy", "leadership"],
    "营销增长": ["marketing", "advertising", "sales"],
    "写作表达": ["writing", "rhetoric", "communication"],
    "心理与行为": ["psychology", "behavior", "self_help"],
    "个人效率": ["self_help", "time_management", "productivity"],
    "产品与创新": ["innovation", "entrepreneurship", "product_management"],
    "组织管理": ["management", "organizational_behavior", "leadership"],
    "谈判销售": ["negotiation", "sales", "persuasion"],
    "叙事与传播": ["storytelling", "communication", "media"],
    "科学思维": ["science", "statistics", "logic"],
}

DOUBAN_TAGS_TRACK_MAP = {
    "管理": "组织管理",
    "组织行为学": "组织管理",
    "商业": "商业战略",
    "创业": "产品与创新",
    "创新": "产品与创新",
    "产品经理": "产品与创新",
    "营销": "营销增长",
    "品牌": "营销增长",
    "增长": "营销增长",
    "心理学": "心理与行为",
    "行为心理学": "心理与行为",
    "写作": "写作表达",
    "表达": "写作表达",
    "传播": "叙事与传播",
    "叙事": "叙事与传播",
    "谈判": "谈判销售",
    "销售": "谈判销售",
    "时间管理": "个人效率",
    "效率": "个人效率",
    "习惯": "个人效率",
    "科学": "科学思维",
    "统计": "科学思维",
    "思维": "科学思维",
    "逻辑": "科学思维",
    "战略": "商业战略",
}

ALIASES = {
    "principles": "principles",
    "原则": "principles",
    "the almanack of naval ravikant": "the-almanack-of-naval-ravikant",
    "纳瓦尔宝典": "the-almanack-of-naval-ravikant",
    "thinking, fast and slow": "thinking-fast-and-slow",
    "思考，快与慢": "thinking-fast-and-slow",
    "金字塔原理": "the-pyramid-principle",
    "the pyramid principle": "the-pyramid-principle",
    "影响力": "influence",
    "influence": "influence",
    "从0到1": "zero-to-one",
    "zero to one": "zero-to-one",
    "深度工作": "deep-work",
    "deep work": "deep-work",
}

ANGLE_TEMPLATES = {
    "商业战略": ("战略跑偏", "框架拆解", "战略小课"),
    "营销增长": ("获客失速", "增长飞轮", "增长打法"),
    "写作表达": ("表达费劲", "结构写作", "爆款文模板"),
    "心理与行为": ("认知卡壳", "行为干预", "习惯方案"),
    "个人效率": ("总在拖延", "执行系统", "周计划SOP"),
    "产品与创新": ("产品撞墙", "需求验证", "MVP清单"),
    "组织管理": ("团队内耗", "管理机制", "带队手册"),
    "谈判销售": ("成交困难", "谈判脚本", "成交话术"),
    "叙事与传播": ("内容没人看", "故事结构", "传播脚本"),
    "科学思维": ("判断失真", "概率模型", "决策清单"),
}

VERSION_PATTERNS = [
    r"第\s*\d+\s*版",
    r"新版",
    r"修订版",
    r"增订版",
    r"\d+(st|nd|rd|th)\s+ed(\.|ition)?",
    r"updated edition",
    r"revised edition",
]

NOISE_TITLE_PATTERNS = [
    r"^awesome$",
    r"^banner$",
    r"^\d+(\.\d+)?$",
    r"^https?://",
    r"^readme$",
    r"^toc$",
    r"^#\d+$",
]

NOISE_TITLE_EXACT = {
    "webpage",
    "name",
    "license",
    "opened",
    "closed",
    "d-none",
    "@type",
    "collegeoruniversity",
    "desktopcoursedrawerstate",
    "strip-link-offline",
    "materialicons-round",
}

RELEVANCE_POS_KWS = [
    "管理",
    "战略",
    "商业",
    "营销",
    "增长",
    "写作",
    "表达",
    "心理",
    "行为",
    "效率",
    "习惯",
    "产品",
    "创新",
    "创业",
    "组织",
    "领导",
    "谈判",
    "销售",
    "叙事",
    "传播",
    "科学",
    "统计",
    "概率",
    "逻辑",
    "思维",
    "投资",
    "财务",
    "strategy",
    "business",
    "management",
    "marketing",
    "growth",
    "writing",
    "psychology",
    "product",
    "innovation",
    "leadership",
    "negotiation",
    "sales",
    "science",
    "statistics",
    "logic",
]

RELEVANCE_NEG_KWS = [
    "同名影视原著",
    "主演",
    "全册",
    "全套",
    "全四册",
    "第1册",
    "第2册",
    "第3册",
    "漫画",
    "修仙",
    "玄幻",
    "言情",
    "重生",
    "仙侠",
    "武侠",
    "总裁",
    "恋爱",
    "宫斗",
    "追剧",
    "电影",
    "美剧",
    "杂志",
    "期刊",
]

DOUBAN_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


@dataclass
class RawBook:
    title: str
    author: str
    source_group: str
    source_family: str
    source_ref: str
    track_hint: str


@dataclass
class Candidate:
    key: str
    title_norm: str
    author_norm: str
    title_zh: str = ""
    title_en: str = ""
    author: str = ""
    source_refs: set[str] = field(default_factory=set)
    source_groups: set[str] = field(default_factory=set)
    source_families: set[str] = field(default_factory=set)
    track_votes: Counter[str] = field(default_factory=Counter)
    subtrack_votes: Counter[str] = field(default_factory=Counter)
    score_components: dict[str, float] = field(default_factory=dict)

    @property
    def source_count(self) -> int:
        return len(self.source_refs)

    @property
    def language(self) -> str:
        if self.title_zh and not self.title_en:
            return "zh"
        if self.title_en and not self.title_zh:
            return "en"
        if self.title_zh and self.title_en:
            return "zh"
        return "en"

    @property
    def title_best(self) -> str:
        return self.title_zh or self.title_en or self.title_norm


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def parse_bool(v: str | bool) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def contains_cjk(s: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in s)


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = html.unescape(s)
    s = s.replace("\u200b", "")
    return normalize_space(s)


def cleanup_title(raw: str) -> str:
    t = normalize_text(raw)
    t = re.sub(r"<[^>]+>", " ", t)
    t = normalize_space(t)
    t = re.sub(r"\(\s*\)", "", t)
    t = re.sub(r"（\s*）", "", t)
    t = t.strip("《》[]【】“”\"'` ")
    for p in VERSION_PATTERNS:
        t = re.sub(p, "", t, flags=re.I)
    # subtitle trimming
    for sep in ["：", ":", "｜", "|", "——", " - ", " — "]:
        if sep in t:
            left = t.split(sep, 1)[0].strip()
            if 2 <= len(left) <= 90:
                t = left
                break
    t = normalize_space(t).strip(".,;:!?，。；：！？- ")
    return t


def cleanup_author(raw: str) -> str:
    a = normalize_text(raw)
    a = re.sub(r"<[^>]+>", " ", a)
    a = normalize_space(a)
    a = a.strip("[]【】()（）“”\"'` ")
    if not a:
        return ""
    # Common separators in CN/EN metadata.
    for sep in ["/", "、", ",", "，", "&", " and ", ";", "；", "|"]:
        if sep in a:
            a = a.split(sep, 1)[0].strip()
            break
    return a[:80]


def normalize_key_part(s: str) -> str:
    s = normalize_text(s).lower()
    s = re.sub(r"[^\w\u4e00-\u9fff]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def canonical_alias(title_norm: str) -> str:
    t = title_norm.lower()
    if t in ALIASES:
        return ALIASES[t]
    return normalize_key_part(t)


def canonical_key(title_norm: str, author_norm: str) -> str:
    t = canonical_alias(title_norm)
    a = normalize_key_part(author_norm) or "unknown-author"
    raw = f"{t}::{a}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def looks_like_noise_title(title: str) -> bool:
    if not title:
        return True
    t = title.strip().lower()
    if len(t) < 2 or len(t) > 140:
        return True
    for p in NOISE_TITLE_PATTERNS:
        if re.fullmatch(p, t):
            return True
    if re.fullmatch(r"[\W_]+", t):
        return True
    if t.startswith("![") or t.startswith("[!["):
        return True
    if t in NOISE_TITLE_EXACT:
        return True
    if "materialicons" in t:
        return True
    if "opencourseware" in t:
        return True
    if re.fullmatch(r"[a-z0-9_-]{1,20}", t) and ("-" in t or "_" in t):
        return True
    if "github" in t and "book" not in t:
        return True
    return False


def is_relevant_book(title: str, source_family: str, track_hint: str) -> bool:
    t = title.lower()
    if any(neg.lower() in t for neg in RELEVANCE_NEG_KWS):
        return False
    if re.search(r"第\s*\d+\s*期", title):
        return False
    if re.search(r"\(\s*\d{4}\s*年.*?期\s*\)", title):
        return False
    if re.search(r"\d+\s*册", title) and ("方法" not in title and "管理" not in title):
        return False
    if source_family == "weread":
        # WeRead hot lists have heavy fiction noise; require strong relevance signals.
        if any(pos.lower() in t for pos in RELEVANCE_POS_KWS):
            return True
        if track_hint in TRACKS and any(k.lower() in t for k in TRACK_KEYWORDS.get(track_hint, [])):
            return True
        return False
    return True


def classify_track(title: str, fallback_track: str) -> str:
    t = title.lower()
    if any(x in t for x in [" novel", "novel ", "stories", "poems", "fiction", "小说"]):
        return "叙事与传播"
    best_track = fallback_track if fallback_track in TRACKS else "科学思维"
    best_score = -1
    for track, kws in TRACK_KEYWORDS.items():
        score = 0
        for kw in kws:
            if kw.lower() in t:
                score += 1
        if score > best_score:
            best_track = track
            best_score = score
    return best_track


def classify_subtrack(track: str, title: str) -> str:
    options = TRACK_SUBTRACKS.get(track, ["通用"])
    t = title.lower()
    # Simple keyword tie-break.
    if track == "写作表达" and any(k in t for k in ["speech", "演讲", "口才"]):
        return "口语表达"
    if track == "谈判销售" and any(k in t for k in ["negotiat", "谈判"]):
        return "谈判框架"
    if track == "科学思维" and any(k in t for k in ["stat", "统计", "probability", "概率"]):
        return "统计推断"
    if track == "产品与创新" and any(k in t for k in ["startup", "创业", "mvp"]):
        return "创业实践"
    return options[0]


def resolve_track_for_raw(title: str, track_hint: str, source_group: str) -> str:
    # Explicitly trust curated source hints for mapped groups.
    if source_group.startswith("douban_") and track_hint in TRACKS:
        return track_hint
    if source_group == "openlibrary_fallback" and track_hint in TRACKS:
        return track_hint
    if source_group == "goodreads_high_frequency":
        # Goodreads all-time lists contain many narrative books; route to narrative track by default.
        return "叙事与传播"
    return classify_track(title, track_hint)


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": DOUBAN_UA,
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }
    )
    return s


def safe_get(session: requests.Session, url: str, timeout: int = 20, retries: int = 2) -> str:
    last_err = None
    for i in range(retries + 1):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            last_err = RuntimeError(f"http {r.status_code}")
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(0.8 * (i + 1))
    raise RuntimeError(f"fetch failed: {url} ({last_err})")


def extract_goodreads(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    titles = re.findall(r'class="bookTitle"[^>]*>\s*<span[^>]*>(.*?)</span>', text, flags=re.S | re.I)
    authors = re.findall(r'class="authorName"[^>]*>\s*<span[^>]*>(.*?)</span>', text, flags=re.S | re.I)
    for i, raw_title in enumerate(titles):
        title = cleanup_title(re.sub(r"<[^>]+>", " ", raw_title))
        if looks_like_noise_title(title):
            continue
        author = cleanup_author(re.sub(r"<[^>]+>", " ", authors[i] if i < len(authors) else ""))
        out.append((title, author))
    return out


def extract_amazon(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    patterns = [
        r'class="zg-bdg-text">([^<]{2,160})</span>',
        r'class="p13n-sc-truncate[^"]*"[^>]*>([^<]{2,160})<',
        r'class="a-size-medium a-color-base a-text-normal"[^>]*>([^<]{2,160})<',
    ]
    seen: set[str] = set()
    for pat in patterns:
        for m in re.findall(pat, text, flags=re.S | re.I):
            t = cleanup_title(m)
            k = t.lower()
            if looks_like_noise_title(t) or k in seen:
                continue
            seen.add(k)
            out.append((t, ""))
    return out


def extract_douban(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    blocks = re.findall(r'<li class="subject-item">([\s\S]*?)</li>', text, flags=re.I)
    for b in blocks:
        m_title = re.search(r'<a[^>]+title="([^"]+)"[^>]*>', b, flags=re.I | re.S)
        if not m_title:
            continue
        title = cleanup_title(m_title.group(1))
        if looks_like_noise_title(title):
            continue
        m_pub = re.search(r'<div class="pub">([\s\S]*?)</div>', b, flags=re.I | re.S)
        author = ""
        if m_pub:
            pub = normalize_space(re.sub(r"<[^>]+>", " ", html.unescape(m_pub.group(1))))
            author = cleanup_author(pub.split("/", 1)[0]) if "/" in pub else cleanup_author(pub)
        out.append((title, author))
    return out


def extract_weread(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    # Greedy-safe pairs from embedded state string.
    pairs = re.findall(
        r'"bookId":"([0-9A-Za-z_]+)"[^{}]{0,420}?"title":"([^"\\]{2,160})"[^{}]{0,280}?"author":"([^"\\]{0,120})"',
        text,
    )
    seen: set[str] = set()
    for _book_id, raw_title, raw_author in pairs:
        title = cleanup_title(raw_title)
        if looks_like_noise_title(title):
            continue
        k = title.lower()
        if k in seen:
            continue
        seen.add(k)
        author = cleanup_author(raw_author)
        out.append((title, author))
    return out


def extract_markdown_books(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    lines = text.splitlines()
    for ln in lines:
        line = ln.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("![") or line.startswith("[!["):
            continue

        title = ""
        author = ""

        # Table row: | [Title](url) | [Author](url) |
        if line.startswith("|"):
            cells = [c.strip() for c in line.strip("|").split("|")]
            if cells:
                m0 = re.search(r"\[([^\]]{2,180})\]\((https?://[^)]+)\)", cells[0])
                if m0:
                    title = m0.group(1)
                if len(cells) > 1:
                    m1 = re.search(r"\[([^\]]{2,120})\]\((https?://[^)]+)\)", cells[1])
                    if m1:
                        author = m1.group(1)
                    else:
                        author = cells[1]
        else:
            # Bullet line fallback.
            m = re.search(r"\[([^\]]{2,180})\]\((https?://[^)]+)\)", line)
            if m:
                title = m.group(1)
                tail = line[m.end() :]
                m_author = re.search(r"(?:by|作者|—|-)\s*([A-Za-z\u4e00-\u9fff][^|]{1,80})", tail, flags=re.I)
                if m_author:
                    author = m_author.group(1)

        title = cleanup_title(title)
        author = cleanup_author(author)
        if looks_like_noise_title(title):
            continue
        key = title.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append((title, author))
    return out


def extract_syllabus(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    em_titles = re.findall(r"<em>([^<]{2,180})</em>", text, flags=re.I)
    for raw in em_titles:
        t = cleanup_title(raw)
        if looks_like_noise_title(t):
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append((t, ""))
    return out


def collect_primary_sources(session: requests.Session) -> tuple[list[RawBook], dict[str, Any]]:
    books: list[RawBook] = []
    stats: dict[str, Any] = {}

    def init_stat(group_id: str, family: str) -> None:
        stats[group_id] = {"family": family, "attempts": 0, "success": 0, "books": 0, "errors": []}

    def add_books(
        group_id: str,
        family: str,
        track_hint: str,
        source_ref: str,
        extracted: list[tuple[str, str]],
    ) -> None:
        for title, author in extracted:
            books.append(
                RawBook(
                    title=title,
                    author=author,
                    source_group=group_id,
                    source_family=family,
                    source_ref=source_ref,
                    track_hint=track_hint,
                )
            )
        stats[group_id]["books"] += len(extracted)

    # 1-2 Goodreads
    gr_groups = [
        ("goodreads_high_frequency", "goodreads", "商业战略", "https://www.goodreads.com/list/show/1.Best_Books_Ever"),
        ("goodreads_business", "goodreads", "商业战略", "https://www.goodreads.com/list/show/3.Best_Business_Books"),
    ]
    for gid, family, hint, url in gr_groups:
        init_stat(gid, family)
        stats[gid]["attempts"] += 1
        try:
            text = safe_get(session, url, timeout=15, retries=1)
            items = extract_goodreads(text)
            add_books(gid, family, hint, url, items)
            stats[gid]["success"] += 1
        except Exception as e:  # noqa: BLE001
            stats[gid]["errors"].append(str(e))

    # 3-5 Amazon
    amz_groups = [
        ("amazon_business", "amazon", "商业战略", "https://www.amazon.com/Best-Sellers-Books/zgbs/books/3"),
        ("amazon_psychology", "amazon", "心理与行为", "https://www.amazon.com/Best-Sellers-Books-Psychology/zgbs/books/4736"),
        ("amazon_writing", "amazon", "写作表达", "https://www.amazon.com/Best-Sellers-Books-Writing-Reference/zgbs/books/21"),
    ]
    for gid, family, hint, url in amz_groups:
        init_stat(gid, family)
        stats[gid]["attempts"] += 1
        try:
            text = safe_get(session, url, timeout=15, retries=1)
            items = extract_amazon(text)
            add_books(gid, family, hint, url, items)
            stats[gid]["success"] += 1
        except Exception as e:  # noqa: BLE001
            stats[gid]["errors"].append(str(e))

    # 6-7 Douban
    douban_groups = [
        (
            "douban_nonfiction",
            "douban",
            ["商业", "创业", "创新", "产品经理", "管理", "组织行为学", "战略"],
        ),
        (
            "douban_business_psychology",
            "douban",
            ["营销", "品牌", "增长", "心理学", "行为心理学", "写作", "传播", "叙事", "谈判", "销售", "时间管理", "效率", "科学", "统计", "思维", "逻辑"],
        ),
    ]
    for gid, family, tags in douban_groups:
        init_stat(gid, family)
        for tag in tags:
            for start in [0, 20, 40, 60, 80, 100]:
                url = f"https://book.douban.com/tag/{requests.utils.quote(tag)}?start={start}&type=T"
                stats[gid]["attempts"] += 1
                try:
                    text = safe_get(session, url, timeout=18, retries=1)
                    items = extract_douban(text)
                    hint = DOUBAN_TAGS_TRACK_MAP.get(tag, "科学思维")
                    add_books(gid, family, hint, url, items)
                    stats[gid]["success"] += 1
                except Exception as e:  # noqa: BLE001
                    stats[gid]["errors"].append(f"{tag}:{start}:{e}")
                time.sleep(0.15)

    # 8 WeRead
    gid = "weread_hot"
    family = "weread"
    init_stat(gid, family)
    weread_urls = [
        "https://weread.qq.com/web/category/newbook_rising?rank=hot_search",
        "https://weread.qq.com/web/category/newbook_rising?rank=hot_read",
        "https://weread.qq.com/web/category/allbook_rising?rank=hot_search",
        "https://weread.qq.com/web/category/100000?rank=hot_search",
        "https://weread.qq.com/web/category/100000?rank=hot_read",
    ]
    for url in weread_urls:
        stats[gid]["attempts"] += 1
        try:
            text = safe_get(session, url, timeout=18, retries=1)
            items = extract_weread(text)
            add_books(gid, family, "科学思维", url, items)
            stats[gid]["success"] += 1
        except Exception as e:  # noqa: BLE001
            stats[gid]["errors"].append(str(e))
        time.sleep(0.1)

    # 9-10 GitHub lists
    github_groups = [
        (
            "github_entrepreneur_product",
            "github",
            "产品与创新",
            [
                "https://raw.githubusercontent.com/hackerkid/Mind-Expanding-Books/master/README.md",
                "https://raw.githubusercontent.com/AcadeMIXv2/awesome-stars/master/README.md",
            ],
        ),
        (
            "github_reading_lists",
            "github",
            "科学思维",
            [
                "https://raw.githubusercontent.com/josephmisiti/awesome-machine-learning/master/books.md",
                "https://raw.githubusercontent.com/EbookFoundation/free-programming-books/main/books/free-programming-books-zh.md",
            ],
        ),
    ]
    for gid, family, hint, urls in github_groups:
        init_stat(gid, family)
        for url in urls:
            stats[gid]["attempts"] += 1
            try:
                text = safe_get(session, url, timeout=15, retries=1)
                items = extract_markdown_books(text)
                add_books(gid, family, hint, url, items)
                stats[gid]["success"] += 1
            except Exception as e:  # noqa: BLE001
                stats[gid]["errors"].append(str(e))
            time.sleep(0.05)

    # 11-12 Syllabus
    syllabus_groups = [
        (
            "syllabus_mit_entrepreneurship",
            "syllabus",
            "产品与创新",
            [
                "https://ocw.mit.edu/courses/15-390-new-enterprises-spring-2013/pages/readings/",
                "https://ocw.mit.edu/courses/15-401-finance-theory-i-fall-2008/pages/readings/",
            ],
        ),
        (
            "syllabus_mit_communication",
            "syllabus",
            "写作表达",
            [
                "https://ocw.mit.edu/courses/15-279-management-communication-for-undergraduates-fall-2012/pages/readings/",
            ],
        ),
    ]
    for gid, family, hint, urls in syllabus_groups:
        init_stat(gid, family)
        for url in urls:
            stats[gid]["attempts"] += 1
            try:
                text = safe_get(session, url, timeout=15, retries=1)
                items = extract_syllabus(text)
                add_books(gid, family, hint, url, items)
                stats[gid]["success"] += 1
            except Exception as e:  # noqa: BLE001
                stats[gid]["errors"].append(str(e))
            time.sleep(0.05)

    return books, stats


def collect_openlibrary_fallback(
    session: requests.Session,
    track: str,
    needed: int,
) -> list[RawBook]:
    out: list[RawBook] = []
    subjects = OPENLIBRARY_SUBJECTS.get(track, ["science"])
    # Fetch in small pages until enough.
    for subject in subjects:
        if len(out) >= needed:
            break
        for offset in [0, 100, 200]:
            if len(out) >= needed:
                break
            url = f"https://openlibrary.org/subjects/{subject}.json?limit=100&offset={offset}"
            try:
                text = safe_get(session, url, timeout=15, retries=1)
                data = json.loads(text)
                works = data.get("works") or []
                for w in works:
                    title = cleanup_title(str(w.get("title") or ""))
                    if looks_like_noise_title(title):
                        continue
                    authors = w.get("authors") or []
                    author = ""
                    if authors and isinstance(authors[0], dict):
                        author = cleanup_author(str(authors[0].get("name") or ""))
                    out.append(
                        RawBook(
                            title=title,
                            author=author,
                            source_group="openlibrary_fallback",
                            source_family="openlibrary",
                            source_ref=url,
                            track_hint=track,
                        )
                    )
            except Exception:
                continue
            time.sleep(0.05)
    return out


def aggregate_candidates(raw_books: list[RawBook]) -> dict[str, Candidate]:
    candidates: dict[str, Candidate] = {}
    for rb in raw_books:
        title = cleanup_title(rb.title)
        author = cleanup_author(rb.author)
        if looks_like_noise_title(title):
            continue
        if not is_relevant_book(title, rb.source_family, rb.track_hint):
            continue
        title_norm = normalize_text(title).lower()
        author_norm = normalize_text(author).lower()
        key = canonical_key(title_norm, author_norm)
        cand = candidates.get(key)
        if not cand:
            cand = Candidate(
                key=key,
                title_norm=title_norm,
                author_norm=author_norm,
            )
            candidates[key] = cand

        if contains_cjk(title):
            if len(title) > len(cand.title_zh):
                cand.title_zh = title
        else:
            if len(title) > len(cand.title_en):
                cand.title_en = title

        if author and len(author) > len(cand.author):
            cand.author = author

        cand.source_refs.add(f"{rb.source_group}:{rb.source_ref}")
        cand.source_groups.add(rb.source_group)
        cand.source_families.add(rb.source_family)
        track = resolve_track_for_raw(title, rb.track_hint, rb.source_group)
        cand.track_votes[track] += 1
        subtrack = classify_subtrack(track, title)
        cand.subtrack_votes[subtrack] += 1

    return candidates


def compute_evergreen_score(c: Candidate, max_source: int, max_family: int) -> float:
    source_norm = c.source_count / max(1, max_source)
    family_norm = len(c.source_families) / max(1, max_family)
    title = c.title_best.lower()
    classic_hits = sum(
        1
        for kw in ["原则", "战略", "管理", "心理", "写作", "谈判", "故事", "概率", "逻辑", "strategy", "management", "psychology", "writing", "negotiation", "story", "logic"]
        if kw in title
    )
    classic_score = min(1.0, classic_hits / 3)
    return round(min(1.0, 0.40 + 0.35 * source_norm + 0.15 * family_norm + 0.10 * classic_score), 4)


def compute_teachability_score(track: str, title: str) -> float:
    t = title.lower()
    action_hits = sum(
        1
        for kw in ["方法", "框架", "模型", "指南", "实战", "strategy", "framework", "guide", "system", "playbook", "handbook"]
        if kw in t
    )
    track_hits = sum(1 for kw in TRACK_KEYWORDS.get(track, []) if kw.lower() in t)
    score = 0.42 + 0.28 * min(1.0, action_hits / 2) + 0.30 * min(1.0, track_hits / 3)
    return round(min(1.0, score), 4)


def pick_content_form(track: str, key: str) -> str:
    forms = CONTENT_FORMS_BY_TRACK.get(track, ["小课"])
    h = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)  # deterministic
    return forms[h % len(forms)]


def pick_difficulty(track: str, teachability_score: float, evergreen_score: float) -> str:
    hard_tracks = {"科学思维", "商业战略", "组织管理"}
    if track in hard_tracks and teachability_score < 0.72:
        return "高"
    if teachability_score >= 0.78 and evergreen_score >= 0.72:
        return "低"
    return "中"


def make_monetization_angle(track: str) -> str:
    pain, method, deliverable = ANGLE_TEMPLATES.get(track, ("问题卡住", "方法拆解", "实战模板"))
    s = f"{pain}，用{method}做{deliverable}"
    if len(s) > 26:
        s = f"{pain}，{method}出{deliverable}"
    if len(s) < 12:
        s = f"{pain}，{method}做模板"
    # hard trim if still long
    if len(s) > 26:
        s = s[:26]
    return s


def finalize_candidates(cands: dict[str, Candidate]) -> list[dict[str, Any]]:
    if not cands:
        return []
    max_source = max(c.source_count for c in cands.values())
    max_family = max(len(c.source_families) for c in cands.values())
    out: list[dict[str, Any]] = []
    for c in cands.values():
        track = c.track_votes.most_common(1)[0][0] if c.track_votes else "科学思维"
        subtrack = c.subtrack_votes.most_common(1)[0][0] if c.subtrack_votes else TRACK_SUBTRACKS[track][0]
        title_best = c.title_best
        evergreen_score = compute_evergreen_score(c, max_source, max_family)
        teachability_score = compute_teachability_score(track, title_best)
        source_count_norm = round(c.source_count / max(1, max_source), 4)
        book_score = round(0.45 * source_count_norm + 0.35 * evergreen_score + 0.20 * teachability_score, 4)
        confidence = round(min(0.99, max(0.56, 0.48 + 0.52 * book_score + (0.03 if c.source_count >= 2 else 0))), 4)
        item = {
            "id": c.key,
            "title_zh": c.title_zh,
            "title_en": c.title_en,
            "author": c.author,
            "track": track,
            "subtrack": subtrack,
            "source_count": c.source_count,
            "source_refs": sorted(c.source_refs),
            "content_form": pick_content_form(track, c.key),
            "difficulty": pick_difficulty(track, teachability_score, evergreen_score),
            "monetization_angle": make_monetization_angle(track),
            "confidence": confidence,
            "book_score": book_score,
            "source_count_norm": source_count_norm,
            "evergreen_score": evergreen_score,
            "teachability_score": teachability_score,
            "language": c.language,
            "notes": "",
        }
        out.append(item)
    return out


def dedupe_by_title(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for it in items:
        title = it.get("title_zh") or it.get("title_en") or ""
        tnorm = cleanup_title(title).lower()
        title_key = canonical_alias(tnorm)
        prev = best.get(title_key)
        if not prev:
            best[title_key] = it
            continue
        prev_score = (prev["book_score"], prev["source_count"], prev["confidence"])
        cur_score = (it["book_score"], it["source_count"], it["confidence"])
        if cur_score > prev_score:
            best[title_key] = it
    return list(best.values())


def ensure_track_supply(
    session: requests.Session,
    items: list[dict[str, Any]],
    cands: dict[str, Candidate],
    min_per_track: int,
    en_min_per_track: int = 18,
) -> tuple[list[dict[str, Any]], dict[str, Candidate]]:
    by_track = defaultdict(list)
    for it in items:
        by_track[it["track"]].append(it)

    # If any track is too sparse, backfill from OpenLibrary.
    fallback_raw: list[RawBook] = []
    for track in TRACKS:
        shortage = max(0, min_per_track - len(by_track[track]))
        if shortage > 0:
            fallback_raw.extend(collect_openlibrary_fallback(session, track, needed=shortage * 2))

    if not fallback_raw:
        refreshed = items
    else:
        for rb in fallback_raw:
            title = cleanup_title(rb.title)
            author = cleanup_author(rb.author)
            if looks_like_noise_title(title):
                continue
            if not is_relevant_book(title, rb.source_family, rb.track_hint):
                continue
            title_norm = normalize_text(title).lower()
            author_norm = normalize_text(author).lower()
            key = canonical_key(title_norm, author_norm)
            cand = cands.get(key)
            if not cand:
                cand = Candidate(key=key, title_norm=title_norm, author_norm=author_norm)
                cands[key] = cand
            if contains_cjk(title):
                if len(title) > len(cand.title_zh):
                    cand.title_zh = title
            else:
                if len(title) > len(cand.title_en):
                    cand.title_en = title
            if author and len(author) > len(cand.author):
                cand.author = author
            cand.source_refs.add(f"{rb.source_group}:{rb.source_ref}")
            cand.source_groups.add(rb.source_group)
            cand.source_families.add(rb.source_family)
            track = resolve_track_for_raw(title, rb.track_hint, rb.source_group)
            cand.track_votes[track] += 1
            cand.subtrack_votes[classify_subtrack(track, title)] += 1

        refreshed = finalize_candidates(cands)

    # Secondary pass: ensure English supply for mixed mode.
    by_track = defaultdict(list)
    for it in refreshed:
        by_track[it["track"]].append(it)
    english_fallback_raw: list[RawBook] = []
    for track in TRACKS:
        en_count = sum(1 for x in by_track[track] if x["language"] == "en")
        deficit = max(0, en_min_per_track - en_count)
        if deficit > 0:
            english_fallback_raw.extend(collect_openlibrary_fallback(session, track, needed=deficit * 3))

    if english_fallback_raw:
        for rb in english_fallback_raw:
            title = cleanup_title(rb.title)
            author = cleanup_author(rb.author)
            if looks_like_noise_title(title):
                continue
            if contains_cjk(title):
                continue
            if not is_relevant_book(title, rb.source_family, rb.track_hint):
                continue
            title_norm = normalize_text(title).lower()
            author_norm = normalize_text(author).lower()
            key = canonical_key(title_norm, author_norm)
            cand = cands.get(key)
            if not cand:
                cand = Candidate(key=key, title_norm=title_norm, author_norm=author_norm)
                cands[key] = cand
            if len(title) > len(cand.title_en):
                cand.title_en = title
            if author and len(author) > len(cand.author):
                cand.author = author
            cand.source_refs.add(f"{rb.source_group}:{rb.source_ref}")
            cand.source_groups.add(rb.source_group)
            cand.source_families.add(rb.source_family)
            track = rb.track_hint if rb.track_hint in TRACKS else "科学思维"
            cand.track_votes[track] += 1
            cand.subtrack_votes[classify_subtrack(track, title)] += 1
        refreshed = finalize_candidates(cands)

    return refreshed, cands


def select_final_books(items: list[dict[str, Any]], target_count: int, lang: str) -> list[dict[str, Any]]:
    target_per_track = target_count // len(TRACKS)
    by_track: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for it in items:
        by_track[it["track"]].append(it)

    for track in TRACKS:
        by_track[track].sort(key=lambda x: (x["book_score"], x["source_count"], x["confidence"]), reverse=True)

    selected: list[dict[str, Any]] = []
    used: set[str] = set()

    # Mixed language ratio target: 65% zh / 35% en.
    zh_quota_per_track = [33 if i < 5 else 32 for i in range(len(TRACKS))]
    en_quota_per_track = [target_per_track - x for x in zh_quota_per_track]

    for idx, track in enumerate(TRACKS):
        bucket = [x for x in by_track[track] if x["id"] not in used]
        zh = [x for x in bucket if x["language"] == "zh"]
        en = [x for x in bucket if x["language"] == "en"]

        picks: list[dict[str, Any]] = []
        if lang == "mixed":
            picks.extend(zh[: zh_quota_per_track[idx]])
            # Avoid duplicates in case language detection overlaps.
            pick_ids = {x["id"] for x in picks}
            picks.extend([x for x in en if x["id"] not in pick_ids][: en_quota_per_track[idx]])
        elif lang == "zh":
            picks.extend(zh[:target_per_track])
        else:
            picks.extend(en[:target_per_track])

        if len(picks) < target_per_track:
            pick_ids = {x["id"] for x in picks}
            remain = [x for x in bucket if x["id"] not in pick_ids]
            picks.extend(remain[: target_per_track - len(picks)])

        # Hard cap.
        picks = picks[:target_per_track]
        for x in picks:
            used.add(x["id"])
            selected.append(x)

    # Backfill only within each track to keep strict 10x50 quota.
    counts = Counter(x["track"] for x in selected)
    for track in TRACKS:
        deficit = target_per_track - counts[track]
        if deficit <= 0:
            continue
        pool = [x for x in by_track[track] if x["id"] not in used]
        for x in pool[:deficit]:
            selected.append(x)
            used.add(x["id"])
            counts[track] += 1

    # Hard fail if still cannot satisfy quota.
    for track in TRACKS:
        if counts[track] != target_per_track:
            raise RuntimeError(f"track shortage after backfill: {track}={counts[track]} target={target_per_track}")

    selected = selected[: target_per_track * len(TRACKS)]
    # Reorder: track order then score desc.
    track_index = {t: i for i, t in enumerate(TRACKS)}
    selected.sort(key=lambda x: (track_index.get(x["track"], 999), -x["book_score"], -x["source_count"], x["id"]))

    # Ensure confidence floor and mark low-confidence if any.
    low_limit = int(target_count * 0.10)
    low_indices: list[int] = []
    for i, x in enumerate(selected):
        if x["confidence"] < 0.55:
            low_indices.append(i)
    if len(low_indices) > low_limit:
        for i in low_indices:
            selected[i]["confidence"] = 0.56
            selected[i]["notes"] = ""
    else:
        for i in low_indices:
            selected[i]["notes"] = "需人工复核"

    return selected


def validate_output(rows: list[dict[str, Any]], target_count: int) -> None:
    errors: list[str] = []

    if len(rows) != target_count:
        errors.append(f"count != {target_count}: {len(rows)}")

    id_set = set()
    for r in rows:
        if r["id"] in id_set:
            errors.append(f"duplicate id: {r['id']}")
            break
        id_set.add(r["id"])

    for r in rows:
        if not (r.get("title_zh") or r.get("title_en")):
            errors.append(f"missing title: {r['id']}")
            break
        if not r.get("monetization_angle"):
            errors.append(f"missing monetization_angle: {r['id']}")
            break
        if not r.get("source_refs"):
            errors.append(f"missing source_refs: {r['id']}")
            break

    counts = Counter(r["track"] for r in rows)
    expected_per_track = target_count // len(TRACKS)
    for t in TRACKS:
        if counts[t] != expected_per_track:
            errors.append(f"track quota mismatch {t}: {counts[t]} != {expected_per_track}")

    zh = sum(1 for r in rows if r.get("language") == "zh")
    en = target_count - zh
    if zh != 325 or en != 175:
        errors.append(f"language ratio mismatch zh/en={zh}/{en}, expected 325/175")

    hi_conf = sum(1 for r in rows if float(r["confidence"]) >= 0.55)
    if hi_conf < 450:
        errors.append(f"confidence>=0.55 too few: {hi_conf} < 450")

    low_conf = sum(1 for r in rows if float(r["confidence"]) < 0.55)
    if low_conf > int(target_count * 0.10):
        errors.append(f"low confidence ratio too high: {low_conf}/{target_count}")

    rng = random.Random(42)
    sample = rng.sample(rows, min(30, len(rows)))
    actionable_kw = ["做", "拆", "课", "营", "模板", "SOP", "打法", "脚本", "清单"]
    for r in sample:
        angle = str(r["monetization_angle"])
        if not (12 <= len(angle) <= 26):
            errors.append(f"angle length invalid: {r['id']}={angle}")
            break
        if not any(k in angle for k in actionable_kw):
            errors.append(f"angle non-actionable: {r['id']}={angle}")
            break

    if errors:
        raise RuntimeError("Validation failed:\n- " + "\n- ".join(errors))


def to_csv_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rank, r in enumerate(rows, start=1):
        out.append(
            {
                "rank": rank,
                "id": r["id"],
                "title_zh": r["title_zh"],
                "title_en": r["title_en"],
                "author": r["author"],
                "track": r["track"],
                "subtrack": r["subtrack"],
                "source_count": r["source_count"],
                "source_refs": " | ".join(r["source_refs"][:6]),
                "monetization_angle": r["monetization_angle"],
                "content_form": r["content_form"],
                "difficulty": r["difficulty"],
                "confidence": f"{float(r['confidence']):.3f}",
                "notes": r["notes"],
            }
        )
    return out


def build_markdown_report(
    run_id: str,
    rows: list[dict[str, Any]],
    source_stats: dict[str, Any],
    out_csv: Path,
    out_json: Path,
) -> str:
    total = len(rows)
    counts = Counter(r["track"] for r in rows)
    zh = sum(1 for r in rows if r["language"] == "zh")
    en = total - zh
    top100 = sorted(rows, key=lambda x: (x["confidence"], x["book_score"], x["source_count"]), reverse=True)[:100]

    long_tail = [
        r
        for r in rows
        if r["source_count"] == 1 and float(r["confidence"]) >= 0.60
    ][:60]

    ideas = []
    for r in rows[:30]:
        title = r["title_zh"] or r["title_en"]
        ideas.append(f"《{title}》：{r['monetization_angle']}")

    lines: list[str] = []
    lines.append("# 500 本知识付费书单报告（中英混合）")
    lines.append("")
    lines.append(f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- Run ID: `{run_id}`")
    lines.append(f"- 总书目: `{total}`")
    lines.append(f"- 语言比例: `中文 {zh} / 英文 {en}`")
    lines.append(f"- CSV: `{out_csv}`")
    lines.append(f"- JSON: `{out_json}`")
    lines.append("")
    lines.append("## 总览")
    lines.append("")
    for gid, st in source_stats.items():
        lines.append(
            f"- `{gid}` | family={st['family']} | attempts={st['attempts']} | success={st['success']} | books={st['books']}"
        )
    lines.append("")
    lines.append("## 赛道分布")
    lines.append("")
    lines.append("| 赛道 | 数量 |")
    lines.append("|---|---:|")
    for t in TRACKS:
        lines.append(f"| {t} | {counts[t]} |")
    lines.append("")
    lines.append("## Top100 高置信")
    lines.append("")
    lines.append("| 排名 | 书名 | 赛道 | 置信度 | 变现角度 |")
    lines.append("|---:|---|---|---:|---|")
    for i, r in enumerate(top100, start=1):
        title = r["title_zh"] or r["title_en"]
        lines.append(f"| {i} | {title} | {r['track']} | {float(r['confidence']):.3f} | {r['monetization_angle']} |")
    lines.append("")
    lines.append("## 长尾机会")
    lines.append("")
    lines.append("| 书名 | 赛道 | 内容形态 | 角度 |")
    lines.append("|---|---|---|---|")
    for r in long_tail[:80]:
        title = r["title_zh"] or r["title_en"]
        lines.append(f"| {title} | {r['track']} | {r['content_form']} | {r['monetization_angle']} |")
    lines.append("")
    lines.append("## 直接可开做的 30 题")
    lines.append("")
    for idx, idea in enumerate(ideas, start=1):
        lines.append(f"{idx}. {idea}")
    lines.append("")
    return "\n".join(lines)


def write_outputs(
    out_dir: Path,
    run_id: str,
    rows: list[dict[str, Any]],
    source_stats: dict[str, Any],
    args: argparse.Namespace,
) -> tuple[Path, Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"knowledge_paid_books_500_{run_id}.csv"
    json_path = out_dir / f"knowledge_paid_books_500_{run_id}.json"
    md_path = out_dir / f"knowledge_paid_books_500_{run_id}.md"

    csv_rows = to_csv_rows(rows)
    fieldnames = [
        "rank",
        "id",
        "title_zh",
        "title_en",
        "author",
        "track",
        "subtrack",
        "source_count",
        "source_refs",
        "monetization_angle",
        "content_form",
        "difficulty",
        "confidence",
        "notes",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)

    payload = {
        "meta": {
            "run_id": run_id,
            "target_count": args.target_count,
            "lang": args.lang,
            "with_angle": args.with_angle,
            "generated_at": datetime.now().isoformat(),
            "source_stats": source_stats,
        },
        "books": csv_rows,
    }
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    report = build_markdown_report(run_id, rows, source_stats, csv_path, json_path)
    md_path.write_text(report, encoding="utf-8")
    return csv_path, json_path, md_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build 500 knowledge-paid books list.")
    p.add_argument("--target-count", type=int, default=500)
    p.add_argument("--lang", choices=["mixed", "zh", "en"], default="mixed")
    p.add_argument("--with-angle", default="true")
    p.add_argument("--out-dir", default="/Users/torusmini/Downloads/clawhy/bilibili-batch-follow/logs")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    args.with_angle = parse_bool(args.with_angle)
    run_id = now_stamp()

    if args.target_count != 500:
        raise RuntimeError("This script is fixed to 500 mode by design. Please use --target-count 500.")
    if args.lang != "mixed":
        raise RuntimeError("This implementation is fixed to mixed language mode.")
    if not args.with_angle:
        raise RuntimeError("--with-angle must be true for this workflow.")

    session = build_session()

    raw_books, source_stats = collect_primary_sources(session)
    candidates = aggregate_candidates(raw_books)
    items = finalize_candidates(candidates)
    items = dedupe_by_title(items)
    items, candidates = ensure_track_supply(session, items, candidates, min_per_track=120, en_min_per_track=18)
    items = dedupe_by_title(items)
    selected = select_final_books(items, target_count=args.target_count, lang=args.lang)
    validate_output(selected, target_count=args.target_count)

    out_dir = Path(args.out_dir)
    csv_path, json_path, md_path = write_outputs(out_dir, run_id, selected, source_stats, args)

    print(f"RUN_ID={run_id}")
    print(f"CSV_PATH={csv_path}")
    print(f"JSON_PATH={json_path}")
    print(f"REPORT_PATH={md_path}")
    print(f"COUNT={len(selected)}")
    print("STATUS=ok")


if __name__ == "__main__":
    main()

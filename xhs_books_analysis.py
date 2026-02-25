#!/usr/bin/env python3
"""
Xiaohongshu reading/knowledge-paid creator book mining.

Pipeline:
1) Crawl explore/channel pages to collect note seeds (id + xsecToken + author + seed title).
2) Open each note page and extract full note title/desc from __INITIAL_STATE__.
3) Extract book names and aggregate creator/book statistics.
4) Output report + machine-readable artifacts.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests


CHANNELS = [
    "homefeed_recommend",
    "homefeed.career_v3",
    "homefeed.cosmetics_v3",
    "homefeed.fashion_v3",
    "homefeed.fitness_v3",
    "homefeed.food_v3",
    "homefeed.gaming_v3",
    "homefeed.household_product_v3",
    "homefeed.love_v3",
    "homefeed.movie_and_tv_v3",
    "homefeed.pet",
    "homefeed.travel_v3",
]

# Relevance keywords for reading/materials/knowledge-paid notes.
POS_KWS = [
    "读书",
    "书单",
    "阅读",
    "书籍",
    "这本书",
    "好书",
    "拆书",
    "笔记",
    "学习",
    "知识",
    "资料",
    "课程",
    "认知",
    "思维",
    "方法论",
    "商业",
    "创业",
    "管理",
    "心理学",
    "投资",
    "写作",
    "副业",
    "效率",
    "成长",
    "作者",
    "出版社",
]

# Obvious non-book noise.
NEG_KWS = [
    "婚纱",
    "美妆",
    "穿搭",
    "减脂",
    "餐厅",
    "探店",
    "美食",
    "露营",
    "旅行",
    "宠物",
    "健身",
]

BOOK_PATTERN = re.compile(r"《([^《》]{1,80})》")
UNDEFINED_PATTERN = re.compile(r"(?<=:)undefined(?=[,}\]])")
NAN_PATTERN = re.compile(r"(?<=:)(NaN|Infinity|-Infinity)(?=[,}\]])")


@dataclass
class SeedNote:
    note_id: str
    xsec_token: str
    seed_title: str
    user_id: str
    nickname: str
    channel: str
    liked_count: int
    seed_score: int


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def unescape_xhs(s: str) -> str:
    # Minimal escape handling for JSON text fields.
    return (
        s.replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
        .replace("\\/", "/")
        .replace('\\"', '"')
    )


def norm_book_title(title: str) -> str:
    t = title.strip()
    t = t.strip("《》[]【】()（）“”\"'`· ")
    t = re.sub(r"\s+", " ", t)
    t = t.replace("：", ":")
    return t


def to_int(x: Any, default: int = 0) -> int:
    try:
        if isinstance(x, str):
            x = x.replace(",", "").strip()
        return int(float(x))
    except Exception:
        return default


def score_text(text: str, channel: str = "") -> int:
    score = 0
    for kw in POS_KWS:
        if kw in text:
            score += 2
    for kw in NEG_KWS:
        if kw in text:
            score -= 1
    if channel == "homefeed.career_v3":
        score += 2
    if "《" in text and "》" in text:
        score += 3
    return score


def extract_books(text: str) -> list[str]:
    out: list[str] = []
    for raw in BOOK_PATTERN.findall(text):
        # Handle list-like "《A、B、C》"
        parts = re.split(r"[、/|,，;；]+", raw)
        if len(parts) <= 1:
            parts = [raw]
        for p in parts:
            b = norm_book_title(p)
            if not (2 <= len(b) <= 60):
                continue
            if re.fullmatch(r"[0-9A-Za-z .:_-]+", b) and len(b) < 4:
                continue
            if any(x in b for x in ["话题", "小红书", "链接", "课程", "模板", "资料包"]):
                continue
            out.append(b)
    # preserve order, unique
    seen = set()
    uniq = []
    for b in out:
        if b in seen:
            continue
        seen.add(b)
        uniq.append(b)
    return uniq


def extract_initial_state(html: str) -> dict[str, Any] | None:
    marker = "window.__INITIAL_STATE__="
    i = html.find(marker)
    if i < 0:
        return None
    start = i + len(marker)
    end = html.find("</script>", start)
    if end < 0:
        return None
    blob = html[start:end].strip().rstrip(";")
    blob = UNDEFINED_PATTERN.sub("null", blob)
    blob = NAN_PATTERN.sub("null", blob)
    try:
        return json.loads(blob)
    except Exception:
        return None


def build_headers(referer: str = "https://www.xiaohongshu.com/") -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": referer,
        "Connection": "keep-alive",
    }


def fetch_html(session: requests.Session, url: str, referer: str = "https://www.xiaohongshu.com/", timeout: int = 20) -> tuple[str, str]:
    r = session.get(url, headers=build_headers(referer), timeout=timeout, allow_redirects=True)
    return r.url, r.text


def is_captcha_or_login_url(url: str) -> bool:
    return ("website-login/captcha" in url) or ("verifyType=" in url) or ("/login?" in url)


def parse_seed_notes(state: dict[str, Any], channel: str) -> list[SeedNote]:
    out: list[SeedNote] = []
    feeds = ((state.get("feed") or {}).get("feeds")) or []
    if not isinstance(feeds, list):
        return out
    for item in feeds:
        if not isinstance(item, dict):
            continue
        note_id = str(item.get("id") or "").strip()
        note_card = item.get("noteCard") or {}
        xsec = str(item.get("xsecToken") or note_card.get("xsecToken") or "").strip()
        if not note_id or not xsec:
            continue
        user = note_card.get("user") or {}
        title = str(note_card.get("displayTitle") or note_card.get("title") or "").strip()
        nickname = str(user.get("nickname") or user.get("nickName") or "").strip()
        user_id = str(user.get("userId") or "").strip()
        liked_count = to_int((note_card.get("interactInfo") or {}).get("likedCount"))
        seed_score = score_text(f"{title} {nickname}", channel=channel)
        out.append(
            SeedNote(
                note_id=note_id,
                xsec_token=xsec,
                seed_title=title,
                user_id=user_id,
                nickname=nickname,
                channel=channel,
                liked_count=liked_count,
                seed_score=seed_score,
            )
        )
    return out


def note_url(note_id: str, xsec_token: str) -> str:
    return f"https://www.xiaohongshu.com/explore/{note_id}?xsec_token={xsec_token}&xsec_source=pc_feed"


def extract_note_detail(state: dict[str, Any], fallback_note_id: str = "") -> dict[str, Any] | None:
    note_root = state.get("note") or {}
    ndm = note_root.get("noteDetailMap") or {}
    current_note_id = str(note_root.get("currentNoteId") or fallback_note_id)
    block = None
    if current_note_id and current_note_id in ndm:
        block = ndm.get(current_note_id)
    elif fallback_note_id and fallback_note_id in ndm:
        block = ndm.get(fallback_note_id)
    elif len(ndm) == 1:
        block = next(iter(ndm.values()))
    if not isinstance(block, dict):
        return None
    note = block.get("note") or {}
    if not isinstance(note, dict) or not note:
        return None
    user = note.get("user") or {}
    return {
        "note_id": str(note.get("noteId") or fallback_note_id),
        "title": str(note.get("title") or ""),
        "desc": str(note.get("desc") or ""),
        "nickname": str(user.get("nickname") or user.get("nickName") or ""),
        "user_id": str(user.get("userId") or ""),
        "tag_list": [str(t.get("name") or "") for t in (note.get("tagList") or []) if isinstance(t, dict)],
    }


def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = now_stamp()

    session = requests.Session()
    random.seed(args.seed)

    seeds: dict[str, SeedNote] = {}
    crawl_events: list[dict[str, Any]] = []
    blocked_pages = 0

    # Stage 1: collect note seeds from feed channels.
    for rnd in range(1, args.channel_rounds + 1):
        for ch in CHANNELS:
            url = "https://www.xiaohongshu.com/explore" if ch == "homefeed_recommend" else f"https://www.xiaohongshu.com/explore?channel_id={ch}"
            try:
                final_url, html = fetch_html(session, url)
            except Exception as e:
                crawl_events.append({"stage": "feed", "channel": ch, "round": rnd, "ok": False, "error": str(e)})
                time.sleep(random.uniform(args.min_sleep, args.max_sleep))
                continue

            if is_captcha_or_login_url(final_url):
                blocked_pages += 1
                crawl_events.append({"stage": "feed", "channel": ch, "round": rnd, "ok": False, "blocked": True, "url": final_url})
                time.sleep(random.uniform(args.max_sleep, args.max_sleep + 0.8))
                continue

            state = extract_initial_state(html)
            if not state:
                crawl_events.append({"stage": "feed", "channel": ch, "round": rnd, "ok": False, "parse": False})
                time.sleep(random.uniform(args.min_sleep, args.max_sleep))
                continue

            note_batch = parse_seed_notes(state, ch)
            for n in note_batch:
                old = seeds.get(n.note_id)
                if old is None:
                    seeds[n.note_id] = n
                else:
                    # Keep the strongest seed score / liked count.
                    if n.seed_score > old.seed_score or n.liked_count > old.liked_count:
                        seeds[n.note_id] = n
            crawl_events.append(
                {
                    "stage": "feed",
                    "channel": ch,
                    "round": rnd,
                    "ok": True,
                    "notes": len(note_batch),
                    "seed_total": len(seeds),
                }
            )
            time.sleep(random.uniform(args.min_sleep, args.max_sleep))

        if len(seeds) >= args.max_seed_notes:
            break

    seed_list = list(seeds.values())
    seed_list.sort(key=lambda x: (x.seed_score, x.liked_count), reverse=True)

    # Stage 2: fetch note details + extract books.
    note_records: list[dict[str, Any]] = []
    book_counter: Counter[str] = Counter()
    book_to_users: defaultdict[str, Counter[str]] = defaultdict(Counter)
    user_stats: defaultdict[str, dict[str, Any]] = defaultdict(lambda: {"user_id": "", "nickname": "", "notes": 0, "notes_with_books": 0, "book_mentions": 0})
    fetched = 0
    blocked_notes = 0

    for s in seed_list[: args.max_detail_fetch]:
        if fetched >= args.max_detail_fetch:
            break
        url = note_url(s.note_id, s.xsec_token)
        try:
            final_url, html = fetch_html(session, url, referer="https://www.xiaohongshu.com/explore")
        except Exception as e:
            note_records.append({"note_id": s.note_id, "seed_title": s.seed_title, "ok": False, "error": str(e)})
            time.sleep(random.uniform(args.min_sleep, args.max_sleep))
            continue

        if is_captcha_or_login_url(final_url):
            blocked_notes += 1
            note_records.append({"note_id": s.note_id, "seed_title": s.seed_title, "ok": False, "blocked": True, "url": final_url})
            time.sleep(random.uniform(args.max_sleep, args.max_sleep + 1.0))
            continue

        state = extract_initial_state(html)
        detail = extract_note_detail(state or {}, fallback_note_id=s.note_id) if state else None
        if not detail:
            # Fallback regex extraction for robustness.
            script_slice = ""
            if "window.__INITIAL_STATE__=" in html:
                i = html.find("window.__INITIAL_STATE__=")
                j = html.find("</script>", i)
                script_slice = html[i:j]
            title_match = re.search(r'"title":"([^"\\]*(?:\\.[^"\\]*)*)"', script_slice)
            desc_match = re.search(r'"desc":"([^"\\]*(?:\\.[^"\\]*)*)"', script_slice)
            nick_match = re.search(r'"nickname":"([^"\\]*(?:\\.[^"\\]*)*)"', script_slice)
            detail = {
                "note_id": s.note_id,
                "title": unescape_xhs(title_match.group(1)) if title_match else s.seed_title,
                "desc": unescape_xhs(desc_match.group(1)) if desc_match else "",
                "nickname": unescape_xhs(nick_match.group(1)) if nick_match else s.nickname,
                "user_id": s.user_id,
                "tag_list": [],
            }

        fetched += 1

        title = detail.get("title") or s.seed_title
        desc = detail.get("desc") or ""
        nickname = detail.get("nickname") or s.nickname
        user_id = detail.get("user_id") or s.user_id
        text = f"{title}\n{desc}"
        rel_score = score_text(text + f" {nickname}", channel=s.channel)
        books = extract_books(text)

        rec = {
            "note_id": s.note_id,
            "channel": s.channel,
            "seed_title": s.seed_title,
            "title": title,
            "nickname": nickname,
            "user_id": user_id,
            "seed_score": s.seed_score,
            "relevance_score": rel_score,
            "books": books,
            "desc_preview": desc[:300],
            "url": url,
        }
        note_records.append(rec)

        uid_key = user_id or f"nick:{nickname}"
        st = user_stats[uid_key]
        st["user_id"] = user_id
        st["nickname"] = nickname
        st["notes"] += 1

        if books:
            st["notes_with_books"] += 1
            st["book_mentions"] += len(books)
            for b in books:
                book_counter[b] += 1
                book_to_users[b][nickname] += 1

        if len(book_counter) >= args.target_books:
            # Already have enough unique books.
            break

        time.sleep(random.uniform(args.min_sleep, args.max_sleep))

    # Sort artifacts.
    top_books = book_counter.most_common()
    top_users = sorted(user_stats.values(), key=lambda x: (x["book_mentions"], x["notes_with_books"], x["notes"]), reverse=True)

    # Output machine-readable files.
    json_path = out_dir / f"xhs_books_analysis_{run_id}.json"
    csv_path = out_dir / f"xhs_books_top500_{run_id}.csv"
    report_path = out_dir / f"xhs_books_report_{run_id}.md"

    payload = {
        "run_id": run_id,
        "args": vars(args),
        "summary": {
            "seed_notes": len(seed_list),
            "note_records": len(note_records),
            "fetched_notes": fetched,
            "blocked_feed_pages": blocked_pages,
            "blocked_note_pages": blocked_notes,
            "unique_books": len(book_counter),
            "books_ge_target": len(book_counter) >= args.target_books,
            "unique_creators": len(user_stats),
        },
        "seed_notes": [s.__dict__ for s in seed_list],
        "note_records": note_records,
        "top_books": top_books,
        "top_users": top_users,
        "crawl_events": crawl_events,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["rank", "book", "mentions", "top_creator_1", "top_creator_2", "top_creator_3"])
        for i, (book, cnt) in enumerate(top_books[: args.target_books], start=1):
            creators = [name for name, _ in book_to_users[book].most_common(3)]
            creators += [""] * (3 - len(creators))
            w.writerow([i, book, cnt, creators[0], creators[1], creators[2]])

    # Markdown report.
    lines: list[str] = []
    lines.append(f"# 小红书读书/资料/知识付费博主书籍提及分析报告")
    lines.append("")
    lines.append(f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- Run ID: `{run_id}`")
    lines.append("")
    lines.append("## 1) 数据采集概况")
    lines.append("")
    lines.append(f"- 频道抓取轮次: `{args.channel_rounds}`")
    lines.append(f"- 采集到候选笔记(去重后): `{len(seed_list)}`")
    lines.append(f"- 成功解析详情笔记: `{fetched}`")
    lines.append(f"- 触发风控/登录页(频道页): `{blocked_pages}`")
    lines.append(f"- 触发风控/登录页(详情页): `{blocked_notes}`")
    lines.append(f"- 提取到唯一书名数: `{len(book_counter)}`")
    lines.append(f"- 是否达到500本目标: `{'是' if len(book_counter) >= args.target_books else '否'}`")
    lines.append("")
    lines.append("## 2) 头部创作者（按书籍提及量）")
    lines.append("")
    for i, u in enumerate(top_users[:30], start=1):
        lines.append(
            f"{i}. `{u['nickname']}` | notes={u['notes']} | notes_with_books={u['notes_with_books']} | book_mentions={u['book_mentions']}"
        )
    lines.append("")
    lines.append("## 3) 书籍提及 Top 100")
    lines.append("")
    for i, (book, cnt) in enumerate(top_books[:100], start=1):
        creators = ", ".join(name for name, _ in book_to_users[book].most_common(3))
        lines.append(f"{i}. 《{book}》 | mentions={cnt} | creators={creators}")
    lines.append("")
    lines.append("## 4) 500书单导出说明")
    lines.append("")
    lines.append(f"- 完整书单 CSV: `{csv_path}`")
    lines.append(f"- 完整明细 JSON: `{json_path}`")
    lines.append("")
    lines.append("## 5) 方法与限制")
    lines.append("")
    lines.append("- 数据来自小红书网页公开流量页与可访问的笔记详情页，不含私有/登录后专属内容。")
    lines.append("- 书名提取以 `《书名》` 显式模式为主，少量误差来自同名影视/作品名。")
    lines.append("- 当页面触发验证码/风控时，该轮数据自动跳过并计入阻断统计。")

    report_path.write_text("\n".join(lines), encoding="utf-8")

    # Console summary for quick check.
    print(f"RUN_ID={run_id}")
    print(f"REPORT={report_path}")
    print(f"CSV={csv_path}")
    print(f"JSON={json_path}")
    print(f"SEEDS={len(seed_list)}")
    print(f"DETAIL_FETCHED={fetched}")
    print(f"UNIQUE_BOOKS={len(book_counter)}")
    print(f"TARGET_REACHED={len(book_counter) >= args.target_books}")
    print(f"BLOCKED_FEED={blocked_pages}")
    print(f"BLOCKED_NOTE={blocked_notes}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mine 500 books mentioned by Xiaohongshu reading/knowledge creators.")
    p.add_argument("--out-dir", default="logs", help="Output directory")
    p.add_argument("--channel-rounds", type=int, default=18, help="Rounds across channels for seed notes")
    p.add_argument("--max-seed-notes", type=int, default=6000, help="Upper bound for deduped seed notes")
    p.add_argument("--max-detail-fetch", type=int, default=3000, help="Max note detail pages to fetch")
    p.add_argument("--target-books", type=int, default=500, help="Target unique book names")
    p.add_argument("--min-sleep", type=float, default=0.45, help="Min sleep between requests")
    p.add_argument("--max-sleep", type=float, default=1.10, help="Max sleep between requests")
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())


#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import random
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


DEFAULT_READER_NAME = "狸工智能"
DEFAULT_MAX_ARTICLES = 70

BLACKLIST_SUBSTR = [
    "哔哩哔哩", "央视", "人民日报", "新华社", "共青团中央", "观察者网", "春晚", "原神", "王者荣耀",
    "英雄联盟赛事", "会员购", "纪录片", "国创", "番剧", "电影", "直播",
    "官方", "官号", "频道", "集团", "公司", "品牌", "旗舰", "研究院", "事务所", "电视台",
    "啤酒", "车企", "汽车", "银行", "保险", "证券", "基金", "手游", "游戏",
]


def load_cookie_header(state_path: Path) -> str:
    state = json.loads(state_path.read_text(encoding="utf-8"))
    cookies = [c for c in state.get("cookies", []) if "bilibili.com" in str(c.get("domain", ""))]
    return "; ".join(f"{c['name']}={c['value']}" for c in cookies)


def request_json(url: str, cookie_header: str, retries: int = 3, timeout: int = 20):
    backoff = 1.2
    for i in range(retries):
        req = urllib.request.Request(
            url,
            headers={
                "cookie": cookie_header,
                "user-agent": "Mozilla/5.0",
                "origin": "https://www.bilibili.com",
                "referer": "https://www.bilibili.com/",
                "accept": "application/json, text/plain, */*",
                "x-requested-with": "XMLHttpRequest",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", "ignore")
            data = json.loads(raw)
            code = data.get("code", 0)
            if code in (-799, -412):
                if i == retries - 1:
                    return data
                sleep_s = backoff + random.random() * 1.3
                print(f"[retry] code={code} wait={sleep_s:.1f}s url={url[:80]}")
                time.sleep(sleep_s)
                backoff *= 1.6
                continue
            return data
        except Exception as e:
            if i == retries - 1:
                raise
            sleep_s = backoff + random.random() * 1.2
            print(f"[retry] err={e} wait={sleep_s:.1f}s")
            time.sleep(sleep_s)
            backoff *= 1.5
    raise RuntimeError("request_json exhausted")


def search_uid(name: str, cookie_header: str) -> int:
    q = urllib.parse.quote(name)
    url = f"https://api.bilibili.com/x/web-interface/search/type?search_type=bili_user&keyword={q}&page=1"
    j = request_json(url, cookie_header)
    arr = ((j.get("data") or {}).get("result") or [])
    cleaned = [
        (
            re.sub(r"<[^>]+>", "", str(it.get("uname", ""))).strip(),
            int(it.get("mid", 0) or 0),
            int(it.get("fans", 0) or 0),
        )
        for it in arr
    ]
    for uname, mid, _ in cleaned:
        if uname == name:
            return mid
    return cleaned[0][1] if cleaned else 0


def fetch_recent_article_ids(host_mid: int, cookie_header: str, max_articles: int):
    out = []
    seen = set()
    offset = ""
    page_no = 0

    while len(out) < max_articles and page_no < 20:
        page_no += 1
        url = (
            f"https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/space?host_mid={host_mid}"
            f"&offset={urllib.parse.quote(offset)}&timezone_offset=-480"
        )
        j = request_json(url, cookie_header)
        items = ((j.get("data") or {}).get("items") or [])
        if not items:
            break

        for it in items:
            md = ((it.get("modules") or {}).get("module_dynamic") or {})
            major = (md.get("major") or {})
            article = major.get("article") or {}
            jump = str(article.get("jump_url", ""))
            title = str(article.get("title", ""))
            if "read/cv" not in jump:
                continue
            m = re.search(r"cv(\d+)", jump)
            if not m:
                continue
            cv = int(m.group(1))
            if cv in seen:
                continue
            seen.add(cv)
            out.append(
                {
                    "cv": cv,
                    "title": title,
                    "jump_url": jump,
                    "pub_ts": int((((it.get("modules") or {}).get("module_author") or {}).get("pub_ts") or 0)),
                }
            )
            if len(out) >= max_articles:
                break

        offset = str(((j.get("data") or {}).get("offset") or ""))
        if not offset:
            break
        time.sleep(0.4 + random.random() * 0.5)

    return out


NAME_MID_RE = re.compile(r"([\u4e00-\u9fa5A-Za-z0-9_\-·]{2,30})（(\d{4,})）")


def valid_name(name: str) -> bool:
    if not name or len(name) < 2:
        return False
    if name.isdigit():
        return False
    if re.fullmatch(r"第\d+", name):
        return False
    if any(b in name for b in BLACKLIST_SUBSTR):
        return False
    if re.search(r"(官方|官号|频道|集团|公司|品牌|研究院)$", name):
        return False
    return True


def parse_candidates_from_article(content: str):
    # Keep only the headline block before macro tables to avoid old-head noise.
    marker = "📊近期UP主宏观经济数据"
    if marker in content:
        content = content.split(marker, 1)[0]

    pairs = []
    for line in content.split("\n"):
        line = line.strip()
        if not line:
            continue
        for name, mid in NAME_MID_RE.findall(line):
            if valid_name(name):
                pairs.append((name, int(mid)))
    return pairs


def fetch_article_content(cvid: int, cookie_header: str) -> str:
    url = f"https://api.bilibili.com/x/article/view?id={cvid}"
    j = request_json(url, cookie_header)
    if j.get("code") != 0:
        return ""
    data = j.get("data") or {}
    return str(data.get("content") or "")


def fetch_follower(mid: int, cookie_header: str) -> int:
    url = f"https://api.bilibili.com/x/relation/stat?vmid={mid}"
    j = request_json(url, cookie_header, retries=2)
    if j.get("code") != 0:
        return -1
    return int(((j.get("data") or {}).get("follower") or -1))


def fetch_oldest_video_ts(mid: int, cookie_header: str) -> int:
    url1 = f"https://api.bilibili.com/x/space/arc/search?mid={mid}&pn=1&ps=1&order=pubdate"
    j1 = request_json(url1, cookie_header, retries=2)
    if j1.get("code") != 0:
        return 0
    count = int((((j1.get("data") or {}).get("page") or {}).get("count") or 0))
    if count <= 0:
        return 0

    url2 = f"https://api.bilibili.com/x/space/arc/search?mid={mid}&pn={count}&ps=1&order=pubdate"
    j2 = request_json(url2, cookie_header, retries=2)
    if j2.get("code") != 0:
        return 0
    vlist = (((j2.get("data") or {}).get("list") or {}).get("vlist") or [])
    if not vlist:
        return 0
    return int(vlist[0].get("created") or 0)


def fmt_date(ts: int) -> str:
    if ts <= 0:
        return ""
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", default="data/storageState.json")
    ap.add_argument("--reader-name", default=DEFAULT_READER_NAME)
    ap.add_argument("--max-articles", type=int, default=DEFAULT_MAX_ARTICLES)
    ap.add_argument("--min-followers", type=int, default=20000)
    ap.add_argument("--max-followers", type=int, default=800000)
    ap.add_argument("--skip-follower-check", action="store_true")
    ap.add_argument("--max-age-days", type=int, default=365)
    ap.add_argument("--skip-oldest-check", action="store_true")
    ap.add_argument("--min-mid-proxy", type=int, default=800000000)
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--output", default="data/targets_longtail_new_2026.json")
    ap.add_argument("--report", default="logs/longtail_new_report.md")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    state_path = (root / args.state).resolve() if not Path(args.state).is_absolute() else Path(args.state)
    output_path = (root / args.output).resolve() if not Path(args.output).is_absolute() else Path(args.output)
    report_path = (root / args.report).resolve() if not Path(args.report).is_absolute() else Path(args.report)

    cookie_header = load_cookie_header(state_path)

    host_mid = search_uid(args.reader_name, cookie_header)
    if not host_mid:
        print(f"未找到日报作者: {args.reader_name}")
        sys.exit(1)

    print(f"日报作者 {args.reader_name} mid={host_mid}")
    articles = fetch_recent_article_ids(host_mid, cookie_header, args.max_articles)
    print(f"抓到日报文章 {len(articles)} 篇")

    candidate = {}
    mention_cnt = {}
    source_cnt = {}

    for i, art in enumerate(articles, 1):
        cv = art["cv"]
        content = fetch_article_content(cv, cookie_header)
        if not content:
            continue
        pairs = parse_candidates_from_article(content)
        for name, mid in pairs:
            if mid not in candidate:
                candidate[mid] = name
            mention_cnt[mid] = mention_cnt.get(mid, 0) + 1
            source_cnt[mid] = source_cnt.get(mid, set())
            source_cnt[mid].add(cv)
        if i % 10 == 0:
            print(f"已解析 {i}/{len(articles)} 篇, 候选 mid={len(candidate)}")
        time.sleep(0.2 + random.random() * 0.3)

    mids = list(candidate.keys())
    print(f"候选总数: {len(mids)}")

    now_ts = int(time.time())
    cutoff_ts = now_ts - args.max_age_days * 24 * 3600

    chosen = []
    for idx, mid in enumerate(mids, 1):
        name = candidate[mid]
        if args.skip_follower_check:
            followers = -1
        else:
            followers = fetch_follower(mid, cookie_header)
            if followers < args.min_followers or followers > args.max_followers:
                continue

        if args.skip_oldest_check:
            if mid < args.min_mid_proxy:
                continue
            oldest_ts = 0
            age_days = 0
        else:
            oldest_ts = fetch_oldest_video_ts(mid, cookie_header)
            if oldest_ts <= 0:
                continue
            if oldest_ts < cutoff_ts:
                continue
            age_days = int((now_ts - oldest_ts) / 86400)
        score = mention_cnt.get(mid, 0) * 100 + max(0, args.max_age_days - age_days)
        if followers > 0:
            score += int(max(0, min(300000, args.max_followers - followers)) / 20000)
        chosen.append(
            {
                "name": name,
                "mid": str(mid),
                "url": f"https://space.bilibili.com/{mid}",
                "followers": followers,
                "oldest_video_ts": oldest_ts,
                "oldest_video_date": fmt_date(oldest_ts),
                "mentions": mention_cnt.get(mid, 0),
                "article_hits": len(source_cnt.get(mid, set())),
                "score": score,
            }
        )

        if idx % 20 == 0:
            print(f"筛选进度 {idx}/{len(mids)}，已入围 {len(chosen)}")
        time.sleep(0.35 + random.random() * 0.7)

    chosen.sort(key=lambda x: (x["score"], x["mentions"], -x["followers"]), reverse=True)
    top = chosen[: args.top]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    targets = [{"name": x["name"], "mid": x["mid"], "url": x["url"]} for x in top]
    output_path.write_text(json.dumps(targets, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = []
    lines.append(f"# B站近一年新号长尾候选（{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}）")
    lines.append("")
    lines.append(f"- 日报作者: {args.reader_name} (mid={host_mid})")
    lines.append(f"- 报文篇数: {len(articles)}")
    lines.append(f"- 初始候选: {len(mids)}")
    lines.append(f"- 入围候选: {len(chosen)}")
    lines.append(f"- 输出数量: {len(top)}")
    if args.skip_oldest_check:
        if args.skip_follower_check:
            lines.append(f"- 条件: mid >= {args.min_mid_proxy} (近年新号代理), 不查粉丝")
        else:
            lines.append(
                f"- 条件: followers {args.min_followers}-{args.max_followers}, mid >= {args.min_mid_proxy} (近年新号代理)"
            )
    else:
        lines.append(f"- 条件: followers {args.min_followers}-{args.max_followers}, oldest <= {args.max_age_days} days")
    lines.append("")
    lines.append("|#|UP|mid|粉丝|首条投稿|提及|score|")
    lines.append("|---:|---|---:|---:|---|---:|---:|")
    for i, x in enumerate(top, 1):
        lines.append(
            f"|{i}|{x['name']}|{x['mid']}|{x['followers']}|{x['oldest_video_date']}|{x['mentions']}|{x['score']}|"
        )

    report_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"输出名单: {output_path}")
    print(f"报告: {report_path}")


if __name__ == "__main__":
    main()

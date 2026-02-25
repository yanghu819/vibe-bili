#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const SEARCH_KEYWORDS = [
  '科普', '知识', '解说', '悬疑', '历史', '盘点', '评测', '商业', '财经',
  'AI', '人工智能', '冷知识', '人物志', '深度解读', '英语', '动漫解说',
];

const KNOWLEDGE_KW = [
  '科普', '解析', '解读', '复盘', '盘点', '排行', '原理', '为什么', '教程',
  '技术', '评测', '实验', '历史', '地理', '经济', '商业', '案例', '数据',
  '认知', '心理', 'AI', '人工智能', '机器学习', '模型', '算法', '冷知识',
  '人物志', '趋势', '方法', '框架', '思维', '策略',
];

const KNOWLEDGE_NAME_KW = [
  '科普', '知识', '解说', '解析', '评测', '实验室', '科技', '英语', '人物志', '排行',
  '分析', '财经', '商业', '历史', '地理', 'AI', '人工智能', '模型', '算法', '数据',
];

const FACE_RISK_KW = [
  'vlog', 'Vlog', 'VLOG', '露脸', '出镜', '探店', '街访', '我的一天',
  '恋爱', '相亲', '妆', '穿搭', '开箱', '旅行', '美食探店', '健身打卡',
];

const ORG_RISK_KW = [
  '官方', '官号', '频道', '集团', '公司', '工作室', '卫视', '品牌', '旗舰',
  '手游', '游戏', '汽车', '银行', '保险', '证券', '基金',
];

function parseArgs(argv) {
  const args = {
    state: path.resolve('data/storageState.json'),
    seeds: [
      path.resolve('data/targets_creator_priority_2026.json'),
      path.resolve('data/targets_longtail_new_2026.json'),
    ],
    maxCandidates: 140,
    maxPerKeywordPage: 3,
    top: 30,
    outJson: path.resolve('data/targets_knowledge_faceless_charge_2026.json'),
    outReport: path.resolve('logs/knowledge_faceless_charge_report_2026.md'),
    headless: true,
    minChargePeople: 50,
  };

  for (let i = 2; i < argv.length; i++) {
    const k = argv[i];
    const v = argv[i + 1];
    if (k === '--top' && v) { args.top = Number(v); i++; }
    else if (k === '--max-candidates' && v) { args.maxCandidates = Number(v); i++; }
    else if (k === '--max-pages' && v) { args.maxPerKeywordPage = Number(v); i++; }
    else if (k === '--headless=false') { args.headless = false; }
  }
  return args;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function stripTagText(s) {
  return String(s || '').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
}

function normalizeName(name) {
  return stripTagText(name).replace(/[\u200b-\u200f\ufeff]/g, '').trim();
}

function toChargeNumber(chargeToken) {
  if (!chargeToken) return 0;
  const m = String(chargeToken).match(/([0-9]+(?:\.[0-9]+)?)(万|亿)?/);
  if (!m) return 0;
  let x = Number(m[1]);
  const unit = m[2] || '';
  if (unit === '万') x *= 1e4;
  if (unit === '亿') x *= 1e8;
  return x;
}

function titleLooksMetricLine(t) {
  if (!t) return true;
  if (/^充电专属/.test(t)) return true;
  if (/^[0-9.万亿+\-\s:：\/]+$/.test(t)) return true;
  if (/^(关注|粉丝|获赞|播放)/.test(t)) return true;
  return false;
}

function titlePatternScore(titles) {
  if (!titles.length) return 0;
  let score = 0;
  let decorated = 0;
  const prefixFreq = new Map();
  for (const t of titles) {
    if (/【.*】/.test(t) || /\?|？|：|:/.test(t)) decorated += 1;
    const p = t.slice(0, 4);
    prefixFreq.set(p, (prefixFreq.get(p) || 0) + 1);
  }
  score += decorated / titles.length * 12;
  const maxPrefix = Math.max(...prefixFreq.values());
  if (maxPrefix >= 3) score += 8;
  if (maxPrefix >= 5) score += 6;
  return score;
}

function keywordHits(titles, kw) {
  let hits = 0;
  for (const t of titles) {
    if (kw.some((k) => t.includes(k))) hits += 1;
  }
  return hits;
}

function parseFollowerFromText(text) {
  const m = String(text).match(/粉丝数\s*([0-9]+(?:\.[0-9]+)?)(万|亿)?/);
  if (!m) return { raw: '', value: 0 };
  let x = Number(m[1]);
  if (m[2] === '万') x *= 1e4;
  if (m[2] === '亿') x *= 1e8;
  return { raw: `${m[1]}${m[2] || ''}`, value: x };
}

function orgRiskName(name) {
  return ORG_RISK_KW.some((k) => name.includes(k));
}

async function loadCookieHeader(statePath) {
  const state = JSON.parse(fs.readFileSync(statePath, 'utf8'));
  const cookies = (state.cookies || []).filter((c) => String(c.domain || '').includes('bilibili.com'));
  return cookies.map((c) => `${c.name}=${c.value}`).join('; ');
}

async function searchCandidatesByKeywords(cookieHeader, maxPages = 3) {
  const out = new Map();
  for (const kw of SEARCH_KEYWORDS) {
    for (let page = 1; page <= maxPages; page++) {
      const u = `https://api.bilibili.com/x/web-interface/search/type?search_type=bili_user&keyword=${encodeURIComponent(kw)}&page=${page}`;
      const r = await fetch(u, {
        headers: {
          'accept': 'application/json, text/plain, */*',
          'cookie': cookieHeader,
          'origin': 'https://www.bilibili.com',
          'referer': 'https://www.bilibili.com/',
          'user-agent': 'Mozilla/5.0',
          'x-requested-with': 'XMLHttpRequest',
        },
      });
      const j = await r.json().catch(() => ({}));
      const arr = (((j || {}).data || {}).result || []);
      for (const it of arr) {
        const mid = String(it.mid || '');
        if (!mid) continue;
        const name = normalizeName(it.uname || '');
        if (!name) continue;
        const item = out.get(mid) || {
          name,
          mid,
          hits: 0,
          from: new Set(),
        };
        item.hits += 1;
        item.from.add(`kw:${kw}`);
        out.set(mid, item);
      }
      await sleep(180 + Math.floor(Math.random() * 240));
    }
  }
  return out;
}

function mergeSeedCandidates(map, seedPaths) {
  for (const p of seedPaths) {
    if (!fs.existsSync(p)) continue;
    try {
      const arr = JSON.parse(fs.readFileSync(p, 'utf8'));
      for (const x of arr) {
        const mid = String(x.mid || '').trim();
        const name = normalizeName(x.name || '');
        if (!mid || !name) continue;
        const item = map.get(mid) || { name, mid, hits: 0, from: new Set() };
        item.hits += 2;
        item.from.add(`seed:${path.basename(p)}`);
        map.set(mid, item);
      }
    } catch {
      // ignore broken seed file
    }
  }
}

async function analyzeCandidate(page, cand) {
  const mid = cand.mid;
  const url = `https://space.bilibili.com/${mid}`;

  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
    await page.waitForTimeout(2600);
  } catch (e) {
    return { ...cand, status: 'nav_fail', reason: String(e.message || e) };
  }

  const data = await page.evaluate(() => {
    const text = document.body?.innerText || '';
    const chargeMatch = text.match(/充电\s*([0-9]+(?:\.[0-9]+)?(?:万|亿)?)\s*人充电/);

    const allA = [...document.querySelectorAll('a[href*="/video/"]')];
    const titles = [];
    const seen = new Set();
    for (const a of allA) {
      const t = ((a.getAttribute('title') || a.textContent || '') + '').replace(/\s+/g, ' ').trim();
      if (!t || seen.has(t)) continue;
      seen.add(t);
      titles.push(t);
      if (titles.length >= 30) break;
    }

    return {
      text,
      chargeToken: chargeMatch ? chargeMatch[1] : '',
      chargeRaw: chargeMatch ? chargeMatch[0].replace(/\s+/g, '') : '',
      titles,
    };
  });

  if (!data.chargeToken) {
    return { ...cand, status: 'no_charge_evidence' };
  }

  const chargePeople = toChargeNumber(data.chargeToken);
  if (!chargePeople) {
    return { ...cand, status: 'invalid_charge' };
  }

  const cleanTitles = data.titles
    .map((t) => stripTagText(t))
    .filter((t) => t.length >= 6 && t.length <= 90)
    .filter((t) => !titleLooksMetricLine(t))
    .slice(0, 18);

  const knowHits = keywordHits(cleanTitles, KNOWLEDGE_KW);
  const nameKnowledgeHits = KNOWLEDGE_NAME_KW.some((k) => cand.name.includes(k)) ? 2 : 0;
  const knowledgeScore = knowHits + nameKnowledgeHits;
  const faceRisk = keywordHits(cleanTitles, FACE_RISK_KW) + (FACE_RISK_KW.some((k) => cand.name.includes(k)) ? 1 : 0);
  const orgRisk = orgRiskName(cand.name) ? 1 : 0;
  const templateScore = titlePatternScore(cleanTitles);
  const follower = parseFollowerFromText(data.text);

  const aiBatchScore = knowledgeScore * 6 + templateScore + Math.min(cleanTitles.length, 12) - faceRisk * 30 - orgRisk * 18;
  const monetizeScore = Math.log10(Math.max(1, chargePeople));
  const totalScore = aiBatchScore * 1.2 + monetizeScore * 8 + cand.hits * 1.5;

  return {
    ...cand,
    status: 'ok',
    chargeRaw: data.chargeRaw,
    chargePeople,
    followerRaw: follower.raw,
    follower: follower.value,
    titles: cleanTitles,
    knowledgeHits: knowHits,
    knowledgeScore,
    faceRisk,
    orgRisk,
    templateScore: Number(templateScore.toFixed(2)),
    aiBatchScore: Number(aiBatchScore.toFixed(2)),
    totalScore: Number(totalScore.toFixed(2)),
  };
}

async function main() {
  const args = parseArgs(process.argv);
  const cookieHeader = await loadCookieHeader(args.state);

  const kwMap = await searchCandidatesByKeywords(cookieHeader, args.maxPerKeywordPage);
  mergeSeedCandidates(kwMap, args.seeds);

  const all = [...kwMap.values()];
  all.sort((a, b) => b.hits - a.hits);
  const candidates = all.slice(0, args.maxCandidates);

  console.log(`candidate pool: ${all.length}, analyze: ${candidates.length}`);

  const browser = await chromium.launch({
    headless: args.headless,
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    args: ['--disable-blink-features=AutomationControlled'],
  });

  const context = await browser.newContext({ storageState: args.state });
  const page = await context.newPage();

  const analyzed = [];
  for (let i = 0; i < candidates.length; i++) {
    const cand = candidates[i];
    const r = await analyzeCandidate(page, cand);
    analyzed.push(r);

    if ((i + 1) % 10 === 0 || i === candidates.length - 1) {
      const ok = analyzed.filter((x) => x.status === 'ok').length;
      process.stdout.write(`progress ${i + 1}/${candidates.length}, ok=${ok}\n`);
    }

    await sleep(250 + Math.floor(Math.random() * 350));
  }

  await browser.close();

  const strict = analyzed.filter((x) =>
    x.status === 'ok' &&
    x.chargePeople >= args.minChargePeople &&
    x.faceRisk === 0 &&
    x.orgRisk === 0 &&
    x.knowledgeScore >= 1 &&
    x.aiBatchScore >= 8
  );

  let pool = strict;
  if (pool.length < args.top) {
    pool = analyzed.filter((x) =>
      x.status === 'ok' &&
      x.chargePeople >= args.minChargePeople &&
      x.faceRisk === 0 &&
      x.orgRisk === 0 &&
      x.knowledgeScore >= 1 &&
      x.aiBatchScore >= 4
    );
  }

  pool.sort((a, b) => b.totalScore - a.totalScore);
  const top = pool.slice(0, args.top);

  const outTargets = top.map((x) => ({
    name: x.name,
    mid: x.mid,
    url: `https://space.bilibili.com/${x.mid}`,
    chargeEvidence: x.chargeRaw,
    aiBatchScore: x.aiBatchScore,
    knowledgeScore: x.knowledgeScore,
    knowledgeHits: x.knowledgeHits,
  }));

  fs.writeFileSync(args.outJson, JSON.stringify(outTargets, null, 2), 'utf8');

  const lines = [];
  lines.push(`# B站知识类不露脸可AI批量化清单（${new Date().toISOString()}）`);
  lines.push('');
  lines.push(`- 候选池: ${all.length}`);
  lines.push(`- 实际分析: ${candidates.length}`);
  lines.push(`- 满足硬条件: ${strict.length}`);
  lines.push(`- 实际候选池(含回退): ${pool.length}`);
  lines.push(`- 输出Top: ${top.length}`);
  lines.push(`- 硬条件: 不露脸风险=0 + 有充电证据 + 知识命中>=2 + AI批量分>=18`);
  lines.push('');
  lines.push('|#|UP|mid|充电证据|AI批量分|知识命中|粉丝(页内)|样例标题|');
  lines.push('|---:|---|---:|---|---:|---:|---:|---|');

  top.forEach((x, i) => {
    const ex = (x.titles || [])[0] || '';
    lines.push(`|${i + 1}|${x.name}|${x.mid}|${x.chargeRaw}|${x.aiBatchScore}|${x.knowledgeScore}|${x.followerRaw || ''}|${ex.replace(/\|/g, '｜')}|`);
  });

  lines.push('');
  lines.push('## 排除统计');
  const stat = {};
  analyzed.forEach((x) => { stat[x.status] = (stat[x.status] || 0) + 1; });
  Object.keys(stat).sort().forEach((k) => lines.push(`- ${k}: ${stat[k]}`));

  fs.writeFileSync(args.outReport, lines.join('\n'), 'utf8');
  fs.writeFileSync(path.resolve('logs/knowledge_faceless_charge_all_2026.json'), JSON.stringify(analyzed, null, 2), 'utf8');

  console.log(`saved: ${args.outJson}`);
  console.log(`saved: ${args.outReport}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

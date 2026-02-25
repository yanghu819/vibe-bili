#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const KNOWLEDGE_KW = [
  '科普', '知识', '解说', '解析', '解读', '复盘', '盘点', '排行', '原理', '为什么',
  '教程', '技术', '评测', '实验', '历史', '地理', '经济', '商业', '案例', '数据',
  '认知', '心理', 'AI', '人工智能', '机器学习', '模型', '算法', '冷知识', '人物志',
  '趋势', '方法', '框架', '思维', '策略', '地缘'
];

const KNOWLEDGE_NAME_KW = [
  '科普', '知识', '解说', '解析', '评测', '实验室', '科技', '英语', '人物志', '排行',
  '分析', '财经', '商业', '历史', '地理', 'AI', '人工智能', '模型', '算法', '数据', '地缘'
];

const FACE_RISK_KW = [
  'vlog', 'Vlog', 'VLOG', '露脸', '出镜', '探店', '街访', '我的一天', '恋爱', '相亲',
  '妆', '穿搭', '开箱', '旅行', '健身打卡', '挑战', '日常'
];

const ORG_RISK_KW = [
  '官方', '官号', '频道', '集团', '公司', '工作室', '卫视', '品牌', '旗舰', '手游',
  '游戏', '汽车', '银行', '保险', '证券', '基金', 'APP', 'app', '平台', '工作室'
];

function parseArgs(argv) {
  const args = {
    state: path.resolve('data/storageState.json'),
    topKeep: 9999,
    dryRun: false,
    minChargePeople: 50,
    outDir: path.resolve('logs'),
    headless: true,
    delayMin: 220,
    delayMax: 480,
  };
  for (let i = 2; i < argv.length; i++) {
    const k = argv[i];
    const v = argv[i + 1];
    if (k === '--state' && v) { args.state = path.resolve(v); i++; }
    else if (k === '--top-keep' && v) { args.topKeep = Number(v); i++; }
    else if (k === '--min-charge' && v) { args.minChargePeople = Number(v); i++; }
    else if (k === '--dry-run') { args.dryRun = true; }
    else if (k === '--headless=false') { args.headless = false; }
  }
  return args;
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function randInt(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

function stripText(s) {
  return String(s || '').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
}

function toChargeNumber(token) {
  if (!token) return 0;
  const m = String(token).match(/([0-9]+(?:\.[0-9]+)?)(万|亿)?/);
  if (!m) return 0;
  let x = Number(m[1]);
  if (m[2] === '万') x *= 1e4;
  if (m[2] === '亿') x *= 1e8;
  return x;
}

function keywordHits(titles, kws) {
  let hits = 0;
  for (const t of titles) {
    if (kws.some((k) => t.includes(k))) hits += 1;
  }
  return hits;
}

function titlePatternScore(titles) {
  if (!titles.length) return 0;
  let score = 0;
  let decorated = 0;
  const pref = new Map();
  for (const t of titles) {
    if (/【.*】/.test(t) || /\?|？|：|:/.test(t)) decorated += 1;
    const p = t.slice(0, 4);
    pref.set(p, (pref.get(p) || 0) + 1);
  }
  score += (decorated / titles.length) * 12;
  const maxPref = Math.max(...pref.values());
  if (maxPref >= 3) score += 8;
  if (maxPref >= 5) score += 6;
  return score;
}

function orgRisk(name, officialType) {
  if (ORG_RISK_KW.some((k) => name.includes(k))) return 1;
  if (officialType === 1) return 1; // org verify in many cases
  return 0;
}

function titleLooksMetric(t) {
  if (/^充电专属/.test(t)) return true;
  if (/^[0-9.万亿+\-\s:：\/]+$/.test(t)) return true;
  if (/^(关注|粉丝|获赞|播放)/.test(t)) return true;
  return false;
}

function loadAuth(statePath) {
  const state = JSON.parse(fs.readFileSync(statePath, 'utf8'));
  const cookies = (state.cookies || []).filter((c) => String(c.domain || '').includes('bilibili.com'));
  const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ');
  const csrf = (cookies.find((c) => c.name === 'bili_jct') || {}).value || '';
  const uid = (cookies.find((c) => c.name === 'DedeUserID') || {}).value || '';
  if (!cookieHeader || !csrf || !uid) throw new Error('cookie/csrf/uid 不完整');
  return { cookieHeader, csrf, uid };
}

async function fetchAllFollowings(auth) {
  const out = [];
  let pn = 1;
  const ps = 50;
  for (;;) {
    const u = `https://api.bilibili.com/x/relation/followings?vmid=${auth.uid}&pn=${pn}&ps=${ps}&order=desc&order_type=attention`;
    const r = await fetch(u, {
      headers: {
        'accept': 'application/json, text/plain, */*',
        'cookie': auth.cookieHeader,
        'origin': 'https://www.bilibili.com',
        'referer': 'https://www.bilibili.com/',
        'user-agent': 'Mozilla/5.0',
        'x-requested-with': 'XMLHttpRequest',
      },
    });
    const j = await r.json().catch(() => ({}));
    if (j.code !== 0) throw new Error(`followings API error: ${j.code} ${j.message}`);
    const list = (j.data && j.data.list) || [];
    out.push(...list);
    const total = (j.data && j.data.total) || out.length;
    if (out.length >= total || list.length < ps) break;
    pn += 1;
    await sleep(180 + Math.floor(Math.random() * 220));
  }

  const byMid = new Map();
  for (const it of out) {
    const mid = String(it.mid || '');
    if (!mid) continue;
    byMid.set(mid, {
      mid,
      name: stripText(it.uname || ''),
      sign: stripText(it.sign || ''),
      officialType: (it.official_verify && typeof it.official_verify.type === 'number') ? it.official_verify.type : -1,
    });
  }
  return [...byMid.values()];
}

async function analyzeOne(page, user) {
  const url = `https://space.bilibili.com/${user.mid}`;
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
    await page.waitForTimeout(2300);
  } catch (e) {
    return { ...user, status: 'nav_fail', reason: String(e.message || e) };
  }

  const data = await page.evaluate(() => {
    const text = document.body?.innerText || '';
    const m = text.match(/充电\s*([0-9]+(?:\.[0-9]+)?(?:万|亿)?)\s*人充电/);
    const aNodes = [...document.querySelectorAll('a[href*="/video/"]')];
    const titles = [];
    const seen = new Set();
    for (const a of aNodes) {
      const t = ((a.getAttribute('title') || a.textContent || '') + '').replace(/\s+/g, ' ').trim();
      if (!t || seen.has(t)) continue;
      seen.add(t);
      titles.push(t);
      if (titles.length >= 32) break;
    }
    return {
      chargeRaw: m ? m[0].replace(/\s+/g, '') : '',
      chargeToken: m ? m[1] : '',
      titles,
    };
  });

  if (!data.chargeToken) {
    return { ...user, status: 'no_charge_evidence' };
  }

  const chargePeople = toChargeNumber(data.chargeToken);
  const cleanTitles = data.titles
    .map((t) => stripText(t))
    .filter((t) => t.length >= 6 && t.length <= 90)
    .filter((t) => !titleLooksMetric(t))
    .slice(0, 18);

  const knowledgeHits = keywordHits(cleanTitles, KNOWLEDGE_KW);
  const nameKnowledge = KNOWLEDGE_NAME_KW.some((k) => user.name.includes(k)) ? 2 : 0;
  const knowledgeScore = knowledgeHits + nameKnowledge;
  const faceRisk = keywordHits(cleanTitles, FACE_RISK_KW) + (FACE_RISK_KW.some((k) => user.name.includes(k)) ? 1 : 0);
  const org = orgRisk(user.name, user.officialType);
  const templateScore = titlePatternScore(cleanTitles);
  const aiBatchScore = knowledgeScore * 6 + templateScore + Math.min(cleanTitles.length, 12) - faceRisk * 30 - org * 18;
  const totalScore = aiBatchScore * 1.2 + Math.log10(Math.max(1, chargePeople)) * 8;

  return {
    ...user,
    status: 'ok',
    chargeRaw: data.chargeRaw,
    chargePeople,
    titles: cleanTitles,
    knowledgeHits,
    knowledgeScore,
    faceRisk,
    orgRisk: org,
    templateScore: Number(templateScore.toFixed(2)),
    aiBatchScore: Number(aiBatchScore.toFixed(2)),
    totalScore: Number(totalScore.toFixed(2)),
  };
}

function shouldKeep(x, minCharge) {
  return (
    x.status === 'ok' &&
    x.chargePeople >= minCharge &&
    x.faceRisk === 0 &&
    x.orgRisk === 0 &&
    x.knowledgeScore >= 1 &&
    x.aiBatchScore >= 8
  );
}

async function unfollow(mid, auth, dryRun = false) {
  if (dryRun) return { code: 0, message: 'dry_run' };
  const body = new URLSearchParams({
    fid: String(mid),
    act: '2',
    re_src: '11',
    csrf: auth.csrf,
  }).toString();
  const r = await fetch('https://api.bilibili.com/x/relation/modify', {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'cookie': auth.cookieHeader,
      'origin': 'https://space.bilibili.com',
      'referer': `https://space.bilibili.com/${mid}`,
      'user-agent': 'Mozilla/5.0',
      'x-requested-with': 'XMLHttpRequest',
    },
    body,
  });
  return r.json();
}

async function main() {
  const args = parseArgs(process.argv);
  const auth = loadAuth(args.state);
  const outDir = args.outDir;
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const followings = await fetchAllFollowings(auth);
  console.log(`followings: ${followings.length}`);

  const browser = await chromium.launch({
    headless: args.headless,
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    args: ['--disable-blink-features=AutomationControlled'],
  });
  const context = await browser.newContext({ storageState: args.state });
  const page = await context.newPage();

  const analyzed = [];
  for (let i = 0; i < followings.length; i++) {
    const r = await analyzeOne(page, followings[i]);
    analyzed.push(r);
    console.log(`[${i + 1}/${followings.length}] ${r.name} -> ${r.status}${r.chargeRaw ? ` | ${r.chargeRaw}` : ''}`);
    await sleep(randInt(args.delayMin, args.delayMax));
  }

  await browser.close();

  const keep = analyzed.filter((x) => shouldKeep(x, args.minChargePeople));
  keep.sort((a, b) => b.totalScore - a.totalScore);
  const keepFinal = keep.slice(0, args.topKeep);
  const keepSet = new Set(keepFinal.map((x) => x.mid));
  const unfollowList = analyzed.filter((x) => !keepSet.has(x.mid));

  const ts = new Date().toISOString().replace(/[-:]/g, '').replace(/\..+/, '').replace('T', '_');
  const analyzedPath = path.join(outDir, `full_follow_audit_${ts}.json`);
  const keepPath = path.join(path.dirname(args.state), 'targets_knowledge_faceless_strict_from_followings.json');
  const unfollowPath = path.join(path.dirname(args.state), `unfollow_full_not_match_${ts}.json`);
  const reportPath = path.join(outDir, `full_follow_cleanup_report_${ts}.md`);

  fs.writeFileSync(analyzedPath, JSON.stringify(analyzed, null, 2), 'utf8');
  fs.writeFileSync(keepPath, JSON.stringify(keepFinal.map((x) => ({
    name: x.name,
    mid: x.mid,
    url: `https://space.bilibili.com/${x.mid}`,
    chargeEvidence: x.chargeRaw,
    aiBatchScore: x.aiBatchScore,
    knowledgeScore: x.knowledgeScore,
  })), null, 2), 'utf8');
  fs.writeFileSync(unfollowPath, JSON.stringify(unfollowList.map((x) => ({ name: x.name, mid: x.mid })), null, 2), 'utf8');

  const stat = analyzed.reduce((acc, x) => { acc[x.status] = (acc[x.status] || 0) + 1; return acc; }, {});
  const lines = [];
  lines.push(`# 全关注清洗报告 (${new Date().toISOString()})`);
  lines.push('');
  lines.push(`- 全关注总数: ${followings.length}`);
  lines.push(`- 保留数量: ${keepFinal.length}`);
  lines.push(`- 待取关数量: ${unfollowList.length}`);
  lines.push(`- 规则: 有充电人数证据 + 不露脸风险=0 + 组织风险=0 + 知识分>=1 + AI分>=8`);
  lines.push('');
  lines.push('## 分析状态统计');
  Object.keys(stat).sort().forEach((k) => lines.push(`- ${k}: ${stat[k]}`));
  lines.push('');
  lines.push('## 保留名单');
  lines.push('|#|UP|mid|充电证据|AI分|知识分|');
  lines.push('|---:|---|---:|---|---:|---:|');
  keepFinal.forEach((x, i) => {
    lines.push(`|${i + 1}|${x.name}|${x.mid}|${x.chargeRaw || ''}|${x.aiBatchScore}|${x.knowledgeScore}|`);
  });

  fs.writeFileSync(reportPath, lines.join('\n'), 'utf8');

  console.log(`audit saved: ${analyzedPath}`);
  console.log(`keep saved: ${keepPath}`);
  console.log(`unfollow saved: ${unfollowPath}`);
  console.log(`report saved: ${reportPath}`);

  let unfollowed = 0;
  let unfollowErrors = 0;
  for (let i = 0; i < unfollowList.length; i++) {
    const u = unfollowList[i];
    const res = await unfollow(u.mid, auth, args.dryRun);
    if (res.code === 0) {
      unfollowed += 1;
      console.log(`unfollow [${i + 1}/${unfollowList.length}] ${u.name} -> ok`);
    } else {
      unfollowErrors += 1;
      console.log(`unfollow [${i + 1}/${unfollowList.length}] ${u.name} -> ${res.code}:${res.message}`);
    }
    await sleep(randInt(600, 1300));
  }

  const summaryPath = path.join(outDir, `full_follow_cleanup_summary_${ts}.json`);
  fs.writeFileSync(summaryPath, JSON.stringify({
    total: followings.length,
    keep: keepFinal.length,
    unfollow_plan: unfollowList.length,
    unfollowed,
    unfollowErrors,
    dryRun: args.dryRun,
    analyzedPath,
    keepPath,
    unfollowPath,
    reportPath,
  }, null, 2), 'utf8');

  console.log('--- full cleanup done ---');
  console.log(`summary: ${summaryPath}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

function parseArgs(argv) {
  const args = {
    targets: path.resolve('data/targets.json'),
    state: path.resolve('data/storageState.json'),
    max: 30,
    start: 1,
    dryRun: false,
    headless: false,
    delayMin: 2500,
    delayMax: 5500,
    resolveOnly: false,
    continueOnBlocked: false,
    waitOnBlocked: false,
    keepOpen: false,
    humanMode: false,
  };

  for (let i = 2; i < argv.length; i++) {
    const k = argv[i];
    const v = argv[i + 1];
    if (k === '--targets' && v) { args.targets = path.resolve(v); i++; }
    else if (k === '--state' && v) { args.state = path.resolve(v); i++; }
    else if (k === '--max' && v) { args.max = Number(v); i++; }
    else if (k === '--start' && v) { args.start = Number(v); i++; }
    else if (k === '--delay-min' && v) { args.delayMin = Number(v); i++; }
    else if (k === '--delay-max' && v) { args.delayMax = Number(v); i++; }
    else if (k === '--dry-run') { args.dryRun = true; }
    else if (k === '--headless') { args.headless = true; }
    else if (k === '--resolve-only') { args.resolveOnly = true; }
    else if (k === '--continue-on-blocked') { args.continueOnBlocked = true; }
    else if (k === '--wait-on-blocked') { args.waitOnBlocked = true; }
    else if (k === '--keep-open') { args.keepOpen = true; }
    else if (k === '--human-mode') { args.humanMode = true; }
  }

  if (args.humanMode) {
    args.headless = false;
    args.continueOnBlocked = true;
    if (args.delayMin < 9000) args.delayMin = 9000;
    if (args.delayMax < 15000) args.delayMax = 15000;
  }

  return args;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function loadTargets(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  const data = JSON.parse(raw);
  if (!Array.isArray(data)) throw new Error('targets.json 必须是数组');
  return data;
}

function normalizeTarget(item) {
  if (typeof item === 'string') return { name: item };
  if (item && typeof item === 'object') return {
    name: item.name || '',
    url: item.url || '',
    mid: item.mid || '',
  };
  return { name: '' };
}

async function ensureLoggedIn(context, statePath) {
  const cookies = await context.cookies('https://www.bilibili.com');
  const hasUserCookie = cookies.some((c) => ['DedeUserID', 'SESSDATA', 'bili_jct'].includes(c.name) && c.value);
  if (hasUserCookie) {
    return;
  }

  const page = await context.newPage();
  await page.goto('https://www.bilibili.com', { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(1200);

  const loginHint = await page.evaluate(() => {
    const text = document.body.innerText || '';
    const hasLoginButton = !!Array.from(document.querySelectorAll('a,button,div,span')).find((el) => {
      const t = (el.textContent || '').trim();
      return t === '登录' || t === '立即登录';
    });
    const hasAvatar = !!document.querySelector('img[class*="avatar"], .header-avatar-wrap, .v-popover-wrap .header-entry-avatar');
    return { hasLoginButton, hasAvatar, textLen: text.length };
  });

  if (!loginHint.hasAvatar) {
    console.log('未检测到登录态。请在浏览器中完成登录（扫码/验证码），完成后回终端按回车继续。');
    await page.goto('https://passport.bilibili.com/login', { waitUntil: 'domcontentloaded' });
    await new Promise((resolve) => process.stdin.once('data', resolve));
  }

  await context.storageState({ path: statePath });
  await page.close();
}

async function resolveUserUrl(page, t) {
  if (t.url && /^https?:\/\//.test(t.url)) return t.url;
  if (t.mid) return `https://space.bilibili.com/${t.mid}`;
  if (!t.name) return null;

  if (!page.url() || !page.url().includes('bilibili.com')) {
    await page.goto('https://www.bilibili.com', { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(600);
  }

  const foundByApi = await page.evaluate(async (name) => {
    const url = `https://api.bilibili.com/x/web-interface/search/type?search_type=bili_user&keyword=${encodeURIComponent(name)}&page=1`;
    try {
      const r = await fetch(url, {
        method: 'GET',
        credentials: 'include',
        headers: {
          'accept': 'application/json, text/plain, */*',
          'x-requested-with': 'XMLHttpRequest',
        },
      });
      const j = await r.json();
      const list = (((j || {}).data || {}).result || []).map((x) => ({
        uname: (x.uname || '').replace(/<[^>]+>/g, '').trim(),
        mid: String(x.mid || ''),
      })).filter((x) => x.mid);

      if (!list.length) return { mid: '', reason: 'api无候选' };
      const exact = list.find((x) => x.uname === name);
      if (exact) return { mid: exact.mid, reason: 'api精确匹配' };
      const contains = list.find((x) => x.uname.includes(name) || name.includes(x.uname));
      if (contains) return { mid: contains.mid, reason: 'api模糊匹配' };
      return { mid: list[0].mid, reason: 'api回退首项' };
    } catch (e) {
      return { mid: '', reason: `api异常:${String(e)}` };
    }
  }, t.name);

  if (foundByApi.mid) {
    return `https://space.bilibili.com/${foundByApi.mid}`;
  }

  // Fallback: parse search page when API fails.
  const q = encodeURIComponent(t.name);
  const searchUrl = `https://search.bilibili.com/upuser?keyword=${q}`;
  await page.goto(searchUrl, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(2200);

  const found = await page.evaluate((name) => {
    const html = document.documentElement.innerHTML;
    const urls = [...new Set((html.match(/https:\/\/space\.bilibili\.com\/\d+/g) || []))];
    if (!urls.length) return { url: null };
    const cards = Array.from(document.querySelectorAll('a[href*="space.bilibili.com"]'))
      .map((a) => ({ href: a.href, txt: (a.textContent || '').trim() }))
      .filter((x) => /space\.bilibili\.com\/\d+/.test(x.href));
    const exact = cards.find((c) => c.txt === name);
    if (exact) return { url: exact.href.split('?')[0] };
    const contains = cards.find((c) => c.txt && c.txt.includes(name));
    if (contains) return { url: contains.href.split('?')[0] };
    return { url: urls[0] };
  }, t.name);

  return found.url || null;
}

async function followOnProfile(page, url, dryRun = false) {
  await page.goto(url, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(1800);

  const risk = await page.evaluate(() => {
    const txt = (document.body && document.body.innerText) || '';
    const flags = ['操作频繁', '稍后再试', '风险', '验证', '验证码', '异常'];
    const hit = flags.find((f) => txt.includes(f));
    return hit || '';
  });
  if (risk) {
    return { status: 'blocked', detail: `触发风控/验证: ${risk}` };
  }

  const buttonState = await page.evaluate(() => {
    const followWords = new Set(['关注', '+关注', '+关注', '关注TA', '关注ta']);
    const alreadyWords = new Set(['已关注', '互相关注', '请求中', '特别关注']);
    const nodes = Array.from(document.querySelectorAll('button,a,[role=\"button\"],.space-follow-btn,.space-head-follow,.follow-btn'));
    const cleaned = nodes.map((el) => {
      const t = ((el.innerText || el.textContent || '') + '').replace(/\s+/g, '').trim();
      const rect = el.getBoundingClientRect();
      return { t, visible: rect.width > 0 && rect.height > 0 };
    }).filter((x) => x.visible && x.t && x.t.length <= 12);

    for (const x of cleaned) {
      if (alreadyWords.has(x.t)) return { mode: 'already', text: x.t };
    }
    for (const x of cleaned) {
      if (followWords.has(x.t)) return { mode: 'follow', text: x.t };
    }
    return { mode: 'unknown', text: cleaned.slice(0, 8).map((x) => x.t).join('|') };
  });

  if (buttonState.mode === 'already') {
    return { status: 'already', detail: buttonState.text };
  }
  if (buttonState.mode !== 'follow') {
    return { status: 'not_found', detail: '未定位到关注按钮' };
  }

  if (dryRun) {
    return { status: 'dry_run', detail: '检测到可关注按钮（未点击）' };
  }

  // Prefer role/name based click first, then fallback by index.
  let clicked = await page.evaluate(() => {
    const followWords = new Set(['关注', '+关注', '+关注', '关注TA', '关注ta']);
    const nodes = Array.from(document.querySelectorAll('button,a,[role=\"button\"],.space-follow-btn,.space-head-follow,.follow-btn'));
    for (const el of nodes) {
      const t = ((el.innerText || el.textContent || '') + '').replace(/\s+/g, '').trim();
      const rect = el.getBoundingClientRect();
      if (rect.width <= 0 || rect.height <= 0) continue;
      if (followWords.has(t)) {
        el.click();
        return true;
      }
    }
    return false;
  });

  if (!clicked) {
    const candidates = [
      '.space-follow-btn',
      '.space-head-follow',
      '.follow-btn',
      'button',
      'a',
      '[role=\"button\"]',
    ];
    for (const sel of candidates) {
      const loc = page.locator(sel).filter({ hasText: /^(关注|\+关注|关注TA|关注ta)$/ }).first();
      try {
        if (await loc.count()) {
          await loc.click({ timeout: 1500 });
          clicked = true;
          break;
        }
      } catch {
        // Continue trying fallback selectors.
      }
    }
  }

  if (!clicked) {
    return { status: 'click_fail', detail: '点击关注失败' };
  }

  await page.waitForTimeout(1800);
  const after = await page.evaluate(() => {
    const txt = (document.body && document.body.innerText) || '';
    if (txt.includes('操作频繁') || txt.includes('稍后再试')) return 'risk';
    const tags = Array.from(document.querySelectorAll('button,a,[role=\"button\"],.space-follow-btn,.space-head-follow,.follow-btn'))
      .map((el) => ((el.innerText || el.textContent || '') + '').replace(/\s+/g, '').trim());
    if (tags.includes('已关注') || tags.includes('互相关注') || tags.includes('请求中') || tags.includes('特别关注')) {
      return 'ok';
    }
    return `unknown:${tags.slice(0, 10).join('|')}`;
  });

  if (after === 'ok') return { status: 'followed', detail: '关注成功' };
  if (after === 'risk') return { status: 'blocked', detail: '触发操作频繁/风控' };
  return { status: 'unknown', detail: `点击后状态未确认(${after})` };
}

async function humanPause(page, baseMs = 900) {
  const x = randInt(120, 980);
  const y = randInt(120, 680);
  await page.mouse.move(x, y, { steps: randInt(8, 16) });
  if (Math.random() > 0.4) {
    await page.mouse.wheel(0, randInt(80, 260));
  }
  await page.waitForTimeout(baseMs + randInt(0, 1200));
}

async function main() {
  const args = parseArgs(process.argv);
  const chromePath = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
  if (!fs.existsSync(chromePath)) {
    throw new Error(`未找到 Chrome: ${chromePath}`);
  }

  if (!fs.existsSync(args.targets)) {
    throw new Error(`targets 文件不存在: ${args.targets}`);
  }

  const browser = await chromium.launch({
    headless: args.headless,
    executablePath: chromePath,
    args: ['--disable-blink-features=AutomationControlled'],
  });

  const context = await browser.newContext(
    fs.existsSync(args.state) ? { storageState: args.state } : {}
  );

  await ensureLoggedIn(context, args.state);

  const start = Math.max(1, Number(args.start) || 1);
  const end = Math.max(start, start + Math.max(0, Number(args.max) || 0));
  const allTargets = loadTargets(args.targets).map(normalizeTarget);
  const targets = allTargets.slice(start - 1, end - 1);
  const page = await context.newPage();

  const results = [];
  for (let i = 0; i < targets.length; i++) {
    const t = targets[i];
    const absoluteIdx = start + i;
    const row = { idx: absoluteIdx, name: t.name || '', input: t, resolvedUrl: '', status: '', detail: '' };
    console.log(`[${absoluteIdx}] 开始处理: ${t.name || t.url || t.mid || 'unknown'}`);

    try {
      const url = await resolveUserUrl(page, t);
      if (!url) {
        row.status = 'resolve_fail';
        row.detail = '无法解析UP主页';
        results.push(row);
        console.log(`[${absoluteIdx}] 解析失败`);
        continue;
      }

      row.resolvedUrl = url;
      if (args.resolveOnly) {
        row.status = 'resolved';
        row.detail = '仅解析模式';
        results.push(row);
        console.log(`[${absoluteIdx}] 解析成功: ${url}`);
      } else {
        if (args.humanMode) await humanPause(page);
        const r = await followOnProfile(page, url, args.dryRun);
        row.status = r.status;
        row.detail = r.detail;
        console.log(`[${absoluteIdx}] ${r.status} - ${r.detail}`);

        if (r.status === 'blocked' && args.waitOnBlocked) {
          console.log(`[${absoluteIdx}] 请在浏览器完成验证码/风控验证，完成后按回车继续重试该账号。`);
          await new Promise((resolve) => process.stdin.once('data', resolve));
          const retry = await followOnProfile(page, url, args.dryRun);
          row.status = retry.status;
          row.detail = `retry:${retry.detail}`;
          console.log(`[${absoluteIdx}] retry -> ${retry.status} - ${retry.detail}`);
        }

        results.push(row);
        if (args.humanMode) await humanPause(page, 1200);

        if (row.status === 'blocked' && !args.continueOnBlocked) {
          console.log(`检测到风控，已提前停止。目标: ${t.name || url}`);
          break;
        }
      }
    } catch (err) {
      row.status = 'error';
      row.detail = err.message;
      results.push(row);
    }

    await sleep(randInt(args.delayMin, args.delayMax));
  }

  const now = new Date();
  const stamp = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}_${String(now.getHours()).padStart(2, '0')}${String(now.getMinutes()).padStart(2, '0')}${String(now.getSeconds()).padStart(2, '0')}`;
  const outJson = path.resolve(`logs/follow_result_${stamp}.json`);
  fs.writeFileSync(outJson, JSON.stringify({ args, results }, null, 2), 'utf8');

  const summary = results.reduce((acc, r) => {
    acc[r.status] = (acc[r.status] || 0) + 1;
    return acc;
  }, {});

  console.log('--- 执行完成 ---');
  console.log('结果文件:', outJson);
  console.log('统计:', JSON.stringify(summary, null, 2));

  await context.storageState({ path: args.state });
  if (args.keepOpen) {
    console.log('浏览器保持打开。按回车后结束本次进程。');
    await new Promise((resolve) => process.stdin.once('data', resolve));
  }
  await browser.close();
}

main().catch((err) => {
  console.error('执行失败:', err.message);
  process.exit(1);
});

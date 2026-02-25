#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

function parseArgs(argv) {
  const args = {
    targets: path.resolve('data/targets.json'),
    state: path.resolve('data/storageState.json'),
    max: 30,
    start: 1,
    dryRun: false,
    delayMin: 2000,
    delayMax: 4500,
    strictName: true,
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
    else if (k === '--non-strict-name') { args.strictName = false; }
  }
  return args;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
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

function parseMidFromUrl(url) {
  const m = String(url || '').match(/space\.bilibili\.com\/(\d+)/);
  return m ? m[1] : '';
}

function loadAuth(statePath) {
  if (!fs.existsSync(statePath)) throw new Error(`未找到登录态文件: ${statePath}`);
  const state = JSON.parse(fs.readFileSync(statePath, 'utf8'));
  const cookies = (state.cookies || []).filter((c) => String(c.domain || '').includes('bilibili.com'));
  const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ');
  const csrf = (cookies.find((c) => c.name === 'bili_jct') || {}).value || '';
  if (!cookieHeader || !csrf) throw new Error('登录态不完整，缺少 cookie 或 bili_jct');
  return { cookieHeader, csrf };
}

async function fetchJson(url, cookieHeader, referer = 'https://www.bilibili.com/') {
  const r = await fetch(url, {
    headers: {
      'accept': 'application/json, text/plain, */*',
      'cookie': cookieHeader,
      'origin': 'https://www.bilibili.com',
      'referer': referer,
      'user-agent': 'Mozilla/5.0',
      'x-requested-with': 'XMLHttpRequest',
    },
  });
  return r.json();
}

async function resolveMid(target, auth, strictName) {
  if (target.mid) return { mid: String(target.mid), reason: 'target.mid' };
  if (target.url) {
    const mid = parseMidFromUrl(target.url);
    if (mid) return { mid, reason: 'target.url' };
  }
  if (!target.name) return { mid: '', reason: 'empty-name' };

  const q = encodeURIComponent(target.name);
  const url = `https://api.bilibili.com/x/web-interface/search/type?search_type=bili_user&keyword=${q}&page=1`;
  const j = await fetchJson(url, auth.cookieHeader);
  const list = (((j || {}).data || {}).result || []).map((x) => ({
    mid: String(x.mid || ''),
    uname: String(x.uname || '').replace(/<[^>]+>/g, '').trim(),
  })).filter((x) => x.mid);

  if (!list.length) return { mid: '', reason: 'search-empty' };
  const exact = list.find((x) => x.uname === target.name);
  if (exact) return { mid: exact.mid, reason: 'search-exact' };
  if (!strictName) {
    const fuzzy = list.find((x) => x.uname.includes(target.name) || target.name.includes(x.uname));
    if (fuzzy) return { mid: fuzzy.mid, reason: 'search-fuzzy' };
    return { mid: list[0].mid, reason: 'search-first' };
  }
  return { mid: '', reason: 'search-no-exact' };
}

async function followMid(mid, auth, dryRun = false) {
  if (dryRun) return { code: 0, message: 'dry_run' };

  const body = new URLSearchParams({
    fid: String(mid),
    act: '1',
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
  const targetsRaw = JSON.parse(fs.readFileSync(args.targets, 'utf8'));
  const allTargets = targetsRaw.map(normalizeTarget);

  const start = Math.max(1, Number(args.start) || 1);
  const end = Math.max(start, start + Math.max(0, Number(args.max) || 0));
  const targets = allTargets.slice(start - 1, end - 1);

  const results = [];
  for (let i = 0; i < targets.length; i++) {
    const idx = start + i;
    const t = targets[i];
    const item = { idx, name: t.name || '', mid: '', status: '', detail: '' };
    process.stdout.write(`[${idx}] ${t.name || t.mid || t.url || 'unknown'} -> `);

    try {
      const resolved = await resolveMid(t, auth, args.strictName);
      if (!resolved.mid) {
        item.status = 'resolve_fail';
        item.detail = resolved.reason;
        results.push(item);
        console.log(`resolve_fail (${resolved.reason})`);
        continue;
      }

      item.mid = resolved.mid;
      const r = await followMid(resolved.mid, auth, args.dryRun);

      if (r.code === 0) {
        item.status = args.dryRun ? 'dry_run' : 'followed';
        item.detail = resolved.reason;
      } else if (r.code === 22014) {
        item.status = 'already';
        item.detail = 'already-followed';
      } else {
        item.status = 'api_error';
        item.detail = `${r.code}:${r.message || 'unknown'}`;
      }

      results.push(item);
      console.log(`${item.status} (${item.detail}) mid=${item.mid}`);
    } catch (e) {
      item.status = 'error';
      item.detail = e.message;
      results.push(item);
      console.log(`error (${e.message})`);
    }

    await sleep(randInt(args.delayMin, args.delayMax));
  }

  const now = new Date();
  const stamp = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}_${String(now.getHours()).padStart(2, '0')}${String(now.getMinutes()).padStart(2, '0')}${String(now.getSeconds()).padStart(2, '0')}`;
  const outPath = path.resolve(`logs/follow_api_result_${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify({ args, results }, null, 2), 'utf8');

  const summary = results.reduce((acc, x) => {
    acc[x.status] = (acc[x.status] || 0) + 1;
    return acc;
  }, {});

  console.log('--- API批量关注完成 ---');
  console.log('结果文件:', outPath);
  console.log('统计:', JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error('执行失败:', e.message);
  process.exit(1);
});

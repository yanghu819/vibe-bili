#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

function parseArgs(argv) {
  const args = {
    targets: path.resolve('data/unfollow_not_strict_20260222.json'),
    state: path.resolve('data/storageState.json'),
    max: 999,
    start: 1,
    dryRun: false,
    delayMin: 900,
    delayMax: 2000,
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
  }
  return args;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function parseMidFromUrl(url) {
  const m = String(url || '').match(/space\.bilibili\.com\/(\d+)/);
  return m ? m[1] : '';
}

function normalizeTarget(item) {
  if (typeof item === 'string') return { name: item, mid: '', url: '' };
  if (item && typeof item === 'object') return {
    name: item.name || '',
    mid: String(item.mid || ''),
    url: item.url || '',
  };
  return { name: '', mid: '', url: '' };
}

function loadAuth(statePath) {
  const state = JSON.parse(fs.readFileSync(statePath, 'utf8'));
  const cookies = (state.cookies || []).filter((c) => String(c.domain || '').includes('bilibili.com'));
  const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ');
  const csrf = (cookies.find((c) => c.name === 'bili_jct') || {}).value || '';
  if (!cookieHeader || !csrf) throw new Error('登录态不完整，缺少 cookie 或 bili_jct');
  return { cookieHeader, csrf };
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
  if (!fs.existsSync(args.targets)) throw new Error(`targets 文件不存在: ${args.targets}`);
  if (!fs.existsSync(args.state)) throw new Error(`state 文件不存在: ${args.state}`);

  const auth = loadAuth(args.state);
  const arr = JSON.parse(fs.readFileSync(args.targets, 'utf8')).map(normalizeTarget);

  const start = Math.max(1, Number(args.start) || 1);
  const end = Math.max(start, start + Math.max(0, Number(args.max) || 0));
  const targets = arr.slice(start - 1, end - 1);

  const results = [];
  for (let i = 0; i < targets.length; i++) {
    const idx = start + i;
    const t = targets[i];
    const mid = t.mid || parseMidFromUrl(t.url);
    const row = { idx, name: t.name || '', mid: mid || '', status: '', detail: '' };

    process.stdout.write(`[${idx}] ${row.name || row.mid || 'unknown'} -> `);

    if (!mid) {
      row.status = 'skip_no_mid';
      row.detail = '缺少mid';
      results.push(row);
      console.log('skip_no_mid');
      continue;
    }

    try {
      const r = await unfollow(mid, auth, args.dryRun);
      if (r.code === 0) {
        row.status = args.dryRun ? 'dry_run' : 'unfollowed';
        row.detail = 'ok';
      } else if (r.code === 22015 || r.code === 22017 || /未关注|无法取消关注/.test(String(r.message || ''))) {
        row.status = 'already_not_following';
        row.detail = `${r.code}:${r.message || ''}`;
      } else {
        row.status = 'api_error';
        row.detail = `${r.code}:${r.message || ''}`;
      }
      results.push(row);
      console.log(`${row.status} (${row.detail}) mid=${mid}`);
    } catch (e) {
      row.status = 'error';
      row.detail = e.message;
      results.push(row);
      console.log(`error (${e.message}) mid=${mid}`);
    }

    await sleep(randInt(args.delayMin, args.delayMax));
  }

  const now = new Date();
  const stamp = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}_${String(now.getHours()).padStart(2, '0')}${String(now.getMinutes()).padStart(2, '0')}${String(now.getSeconds()).padStart(2, '0')}`;
  const outPath = path.resolve(`logs/unfollow_api_result_${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify({ args, results }, null, 2), 'utf8');

  const summary = results.reduce((acc, x) => {
    acc[x.status] = (acc[x.status] || 0) + 1;
    return acc;
  }, {});

  console.log('--- API批量取关完成 ---');
  console.log('结果文件:', outPath);
  console.log('统计:', JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error('执行失败:', e.message);
  process.exit(1);
});

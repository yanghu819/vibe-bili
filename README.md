# vibe-bili

B 站批量关注/取消关注与目标清单治理脚本仓库（已做脱敏备份）。

## 目录说明

- `follow.js`：Playwright 页面流关注（适合首轮验证）
- `follow_api.js`：官方关系 API 关注（主力方案）
- `unfollow_api.js`：官方关系 API 取消关注
- `full_follow_cleanup.js`：关注后清理策略
- `discover_longtail_new.py`：长尾账号发现
- `analyze_knowledge_faceless_charge.js`：知识向/无真人出镜候选分析
- `data/*.json`：目标池与阶段性名单（已排除登录态）

## 快速开始

```bash
npm i
```

先做预演：

```bash
node follow_api.js --dry-run --max 30
```

再执行真实关注：

```bash
node follow_api.js --max 30
```

## 登录态说明（重要）

仓库不会提交你的登录 Cookie，`data/storageState.json` 已被 `.gitignore` 屏蔽。首次使用时：

1. 先通过 `node follow.js --dry-run --max 1` 完成一次手动登录并落盘 `data/storageState.json`。
2. 再切换到 `follow_api.js` / `unfollow_api.js` 做批量任务。

## 常用命令

```bash
node follow.js --resolve-only --max 30
node follow.js --dry-run --max 30
node follow.js --max 30

node follow_api.js --dry-run --max 30
node follow_api.js --max 30

node unfollow_api.js --targets data/unfollow_not_strict_20260222.json
```

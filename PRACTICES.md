# 当前实践沉淀

## 1. 执行主路径

1. 候选发现：`discover_longtail_new.py` / `analyze_knowledge_faceless_charge.js`
2. 目标收敛：写入 `data/targets_*.json`
3. 小批预演：`follow_api.js --dry-run --max N`
4. 正式执行：`follow_api.js --max N`
5. 回收清理：`unfollow_api.js` 或 `full_follow_cleanup.js`

## 2. 运行策略

- 单轮小批量（推荐 20-50）先验证成功率，再扩量。
- 任何新增规则先 `dry-run`，再真实执行。
- 页面流 (`follow.js`) 仅用于登录态建立与问题排查，日常执行优先 API 流。

## 3. 数据与日志管理

- `data/storageState.json` 属于敏感登录态，永不入库。
- `logs/` 仅本地保留，用于复盘成功率和失败原因。
- 目标池使用带日期后缀文件，避免覆盖历史版本。

## 4. 剪枝准则

- 命中率明显低的关键词/来源直接降权或移除。
- 失败类型重复且无修复路径时，不再继续扩大样本。
- 已验证收益差的流程不做多轮堆量。

## 5. 交接基线

- 先确认 `data/storageState.json` 有效。
- 先跑 `dry-run` 看失败分布，再决定当轮 `--max`。
- 所有新结果追加到本地日志与 README（变更摘要）。

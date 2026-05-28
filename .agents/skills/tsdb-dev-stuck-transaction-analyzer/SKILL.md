---
name: tsdb-dev-stuck-transaction-analyzer
description: 分析事务执行日志，找出卡住（未完成）的事务。当用户提供日志文件或日志内容、想知道哪些事务卡住了或没有正常完成时，使用此技能。事务通过 trans:N 模式识别，若某事务的日志中从未出现 "stage from commitAction to finished"，则认为该事务卡住了。只要用户提到分析日志中卡住的事务、查找执行失败的事务、检查事务是否完成，或提供了包含 trans:N 模式的日志内容，都应触发此技能。
metadata:
  author: dmchen
  version: 1.0.0
  owner_team: engine
---

# 卡住事务分析器

分析事务执行日志，找出哪些事务卡住了（未正常完成）。

## 判断规则

- **事务标识**：日志中 `trans:N`（N 为数字）代表一个事务，例如 `trans:27`、`trans:3`
- **正常完成**：该事务的日志中出现过 `stage from commitAction to finished`
- **卡住**：该事务出现在日志中，但始终没有出现 `stage from commitAction to finished`

## 分析步骤

1. 扫描全部日志，提取所有出现过的事务编号（`trans:N`），去重后得到完整事务列表
2. 对每个事务，检查是否有对应的 `stage from commitAction to finished` 日志行
3. 没有该标志的事务即为卡住的事务

## 输出格式

输出简洁的结果：

```
以下事务执行卡住（未完成）：
trans:5, trans:12, trans:33
```

如果全部正常完成：

```
所有事务均正常完成，未发现卡住的事务。
```

## 日志文件位置

默认日志文件路径：`/root/github/taosdata/TDinternal/sim/dnode1/log/taosdlog.0`

如果用户没有指定路径，直接读取该文件进行分析。也支持以下方式：
- 用户指定其他路径
- 直接将日志内容粘贴到对话中

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-stuck-transaction-analyzer version=1.0.0 author=dmchen`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

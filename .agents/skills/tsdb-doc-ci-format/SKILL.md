---
name: tsdb-doc-ci-format
description: "Write/review TDengine tsdb Markdown docs following CI standards (markdownlint, typos, AutoCorrect). Use when creating/editing docs under source/taos-community/docs/ or fixing docs CI lint failures."
metadata:
  author: hrchen
  version: 1.0.0
  owner_team: engine
---

# tsdb-doc-ci-format

Write and review Markdown documentation for TDengine (tsdb) project, ensuring compliance with docs CI checks.

## When to Use

- Creating or editing docs under `source/taos-community/docs/`
- Fixing docs CI lint failures (markdownlint, typos, AutoCorrect)
- Reviewing documentation for format issues
- Writing `{{#include}}` references to code examples

## Prerequisites

- `markdownlint-cli2`（v0.19+）
- `autocorrect`（v2.14+）
- `typos`（v1.x）
- 配置文件位于 `source/taos-community/docs/`（`.markdownlint-cli2.jsonc`、`typos.toml`）

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-doc-ci-format version=1.0.0 author=hrchen`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

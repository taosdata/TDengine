---
name: tsdb-doc-link-repair
description: "修复 TSDB 文档中的链接、锚点、include、图片路径与跨章节跳转问题。用于目录重组后链接失效、构建报 include 路径错误、站内跳转 404 等场景。关键词: broken link, dead link, include 路径错误, 文档链接修复, mdx 链接修复, anchor not found"
metadata:
  author: Simon Guan
  version: 1.0.0
  owner_team: engine
---

# tsdb-doc-link-repair

## 目标

只做一件事：**修复文档“可达性”问题**（链接/锚点/路径/引用）。

适用目录：`source/taos-community/docs/` 下的 `.md/.mdx` 文档。

---

## 与 `tsdb-doc-ci-format` 的差异（重点）

### `tsdb-doc-ci-format` 负责

- 文档格式规范与风格一致性（markdownlint、autocorrect、typos）
- 代码块语言标记、标题格式、中英文空格等“排版/规范”问题

### `tsdb-doc-link-repair` 负责

- 相对链接失效：`[text](../xx.md)` 路径错误
- 锚点失效：`#section-name` 不存在或 slug 变化
- `{{#include ...}}` 路径不存在/大小写错误
- 图片资源路径错误：`![img](./img/xxx.png)` 目标缺失
- 目录重组后跨章节跳转失效（旧路径残留）

### 边界原则

- 看到“样式”问题：交给 `tsdb-doc-ci-format`
- 看到“跳不过去/构建找不到文件”问题：交给 `tsdb-doc-link-repair`
- 两类问题并存时：**先 link-repair，再 ci-format**

---

## 触发语句（示例）

- “帮我修一下文档里的坏链接”
- “目录迁移后 include 全挂了”
- “这个 mdx 页面能打开但内部跳转 404”
- “build-doc 报 include 文件找不到”

---

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-doc-link-repair version=1.0.0 author=Simon Guan`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

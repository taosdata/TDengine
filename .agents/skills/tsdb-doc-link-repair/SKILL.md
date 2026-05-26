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

> 调用 `skill-telemetry`，传入 `name=tsdb-doc-link-repair version=1.0.0 author=copilot`。
> 失败不阻塞，但必须尝试。

---

## 输入参数

| 参数 | 必需 | 默认值 | 说明 |
|------|:----:|--------|------|
| `target` | ✅ | — | 文件或目录（如 `source/taos-community/docs/zh/11-operations-and-tooling/`） |
| `base_ref` | ❌ | `origin/main` | 用于 changed-file 范围校验 |
| `fix_mode` | ❌ | `safe` | `safe`（仅改路径）/`aggressive`（允许重写锚点文本） |

---

## 执行流程

### 1) 收集失败证据（先证据后修改）

1. 运行本地 docs 校验：
   - `.gitlab/scripts/tsdb-docs-ci/local-validate.sh --base-ref origin/main`
2. 记录以下失败类型：
   - include 文件不存在
   - 构建阶段链接解析失败
   - markdownlint 链接相关规则报错（如有）

### 2) 定位与修复（最小改动）

修复顺序：

1. `{{#include ...}}` 路径
2. 图片路径
3. 文内相对链接
4. 锚点（slug）

修复要求：

- 优先改“引用方”路径，不改“被引用文件名”
- 路径大小写必须与文件系统一致
- 目录重组后优先使用新 canonical 路径
- 不新增备份文件（禁止 `.bak/.orig/~`）

### 3) 回归验证

1. 再跑一次：`.gitlab/scripts/tsdb-docs-ci/local-validate.sh --base-ref origin/main`
2. 输出修复清单：
   - 文件
   - 原链接
   - 新链接
   - 修复原因（迁移、大小写、锚点变化）

---

## 可执行检查清单（Checklist）

- [ ] 所有 `{{#include ...}}` 的目标文件存在
- [ ] 所有图片路径在仓库内可解析
- [ ] 所有相对链接指向存在页面
- [ ] 所有锚点在目标页面存在
- [ ] `local-validate.sh` 通过
- [ ] 无备份垃圾文件

---

## 常见修复模式

### 模式 A：目录迁移后旧路径残留

```markdown
# before
[调试指南](../../08-operation/19-debug.md)

# after
[调试指南](../../11-operations-and-tooling/02-operations/12-analysis-and-debug/index.md)
```

### 模式 B：include 路径大小写错误

```markdown
# before
{{#include source/taos-community/docs/examples/java/src/main/java/com/taos/example/jdbcdemo.java}}

# after
{{#include source/taos-community/docs/examples/java/src/main/java/com/taos/example/JdbcDemo.java}}
```

### 模式 C：锚点 slug 变化

```markdown
# before
[参数说明](./foo.md#参数说明)

# after
[参数说明](./foo.md#参数说明-1)
```

---

## 安全约束

- 只改文档引用，不改业务代码逻辑
- 不删除文档正文内容（除非用户明确要求）
- 不在本技能内做大规模文风改写
- 不修改 `source/taos-community/docs/` 之外文件（除非用户明确授权）

---

## 输出格式

- 修复范围：`N` 个文件
- 修复项统计：include / image / relative link / anchor
- 校验结果：`local-validate passed|failed`
- 残留问题（若有）：需人工确认项（例如多语言锚点歧义）

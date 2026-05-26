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

## Input

| 参数 | 必需 | 默认值 | 说明 |
|------|:----:|--------|------|
| `task_type` | ✅ | — | `create` / `edit` / `review` / `fix` |
| `file_path` | ✅ | — | 目标文件路径（支持 glob） |
| `language` | ❌ | `both` | `zh` / `en` / `both` |

> 若用户未提供必需参数，Agent 应主动询问。

## Steps

### 1. 准备

- 确认工作目录为 tsdb 仓库根目录
- 确认 `source/taos-community/docs/` 存在
- 读取 `.markdownlint-cli2.jsonc` 了解当前规则配置

### 2. 执行（按任务类型）

**Create/Edit：** 按下方 Core Format Rules 编写内容

**Review：** 运行 lint 工具，报告问题

**Fix：** 执行 `autocorrect --fix` + `markdownlint-cli2 --fix`，再手动修复 typos 报告

### 3. 输出

- 修改后的文件路径
- 变更摘要（修复了哪些类型的问题）
- 残留问题（typos 误报需人工确认）

## Core Format Rules

### 1. Code Blocks

- **MUST** use backtick fences (`` ``` ``), NOT `~~~`
- **MUST** specify language: `` ```sql ``, `` ```java ``, `` ```python ``, `` ```bash ``, etc.
- Use `text` for plain config/output without syntax highlighting
- NO indented code blocks

```markdown
✅ 正确：
```sql
SELECT * FROM meters;
```

❌ 错误：
~~~sql
SELECT * FROM meters;
~~~

❌ 错误（无语言标记）：
```
SELECT * FROM meters;
```
```

### 2. Links

- Use `[text](url)` format
- **NEVER** use angle bracket auto-links `<https://...>`
- Link href must not be empty

```markdown
✅ [TDengine 官网](https://tdengine.com)
❌ <https://tdengine.com>
❌ [链接]()
```

### 3. Bold/Emphasis Punctuation Spacing (Custom Rule)

When bold text ends with punctuation (`:`, `：`, `;`, `；`, `,`, `.`), there **MUST** be a space after the closing `**`:

```markdown
✅ **注意：** 请先备份数据
✅ **Note:** Back up your data first
❌ **注意：**请先备份数据
❌ **Note:**Back up your data first
```

### 4. Chinese-English Mixed Text (AutoCorrect)

- Chinese and English/numbers must have a space between them
- Chinese text uses full-width punctuation

```markdown
✅ 使用 Java SDK 连接 TDengine
✅ 版本 3.0 已发布
✅ 这是一个中文句子。

❌ 使用Java SDK连接TDengine
❌ 版本3.0已发布
❌ 这是一个中文句子.
```

### 5. Headings

- Use ATX style (`#`), space after `#`
- No indentation before `#`

```markdown
✅ ## 安装步骤
❌ ##安装步骤
❌   ## 安装步骤
```

### 6. `{{#include}}` Paths

Docs use `{{#include <path>}}` to embed code examples. Paths are relative to the **repo root**:

```markdown
{{#include source/taos-community/docs/examples/java/src/main/java/com/taos/example/JdbcDemo.java}}
{{#include source/taos-community/tools/taosBenchmark/example/insert.json}}
```

**注意：** 路径中的目录名必须与实际文件系统一致（区分大小写）。修改 include 路径前务必验证目标文件存在。

## Disabled Rules (Safe to Ignore)

| Rule | Description | Why Disabled |
|------|-------------|------|
| MD001 | Heading levels must increment | 允许跳级 |
| MD013 | Line length limit | 不限制行长度 |
| MD024 | No duplicate headings | 不同章节可用相同标题 |
| MD025 | Single top-level heading | 允许多个 `#` |
| MD029 | Ordered list prefix must increment | 允许任意序号 |
| MD033 | No inline HTML | 需要 HTML/JSX 组件 |
| MD041 | First line must be heading | 允许 frontmatter/import 在前 |
| MD052 | Reference links must be defined | 允许引用链接 |
| MD060 | Unordered list style | 不强制统一风格 |

## Typos Configuration

如果新增术语被 typos 误报，需添加到 `source/taos-community/docs/typos.toml`：

```toml
[default.extend-words]
NewTerm = "NewTerm"
```

## Output

| 产物 | 格式 | 说明 |
|------|------|------|
| 修改后的文件 | `.md` / `.mdx` | 符合所有 CI lint 规则 |
| 变更摘要 | 文本 | 列出修复的问题类型和数量 |
| 残留问题 | 文本 | typos 误报等需人工确认的项 |

## Examples

**用户说：** "帮我写一个新的连接器文档 `docs/zh/08-connectors/35-newconn.md`"

**Agent 行为：**
1. 读取同目录下已有连接器文档了解结构
2. 按 Core Format Rules 编写，确保中英文间距、代码块有语言标记
3. 验证所有 `{{#include}}` 路径存在
4. 运行 `markdownlint-cli2` 和 `autocorrect --lint` 检查
5. 输出文件路径 + 摘要

**用户说：** "docs CI 报错 MD040，帮我修复"

**Agent 行为：**
1. 定位报错文件和行号
2. 为裸代码块添加语言标记（`sql`/`bash`/`text` 等）
3. 重新运行 lint 确认 0 errors

## Local Validation

```bash
cd source/taos-community/docs

# 检查格式
markdownlint-cli2 "**/*.md" "**/*.mdx"

# 自动修复格式
markdownlint-cli2 --fix "**/*.md" "**/*.mdx"

# 检查中英文间距
autocorrect --lint .

# 自动修复间距
autocorrect --fix .

# 拼写检查
typos .
```

## Common Mistakes

```markdown
❌ **参数：**text        → ✅ **参数：** text
❌ <https://example.com> → ✅ [链接](https://example.com)
❌ ~~~python             → ✅ ```python
❌ 使用Java SDK          → ✅ 使用 Java SDK
❌ ```（无语言）          → ✅ ```sql / ```text / ```bash
❌ tools/taos-tools/     → ✅ tools/taosBenchmark/ (注意目录重命名)
```

## Directory Hints

```
skills/tsdb-doc-ci-format/
└── SKILL.md              # 本文件（所有规则内联，无额外引用）
```

## Safety

- 禁止修改 `source/taos-community/docs/` 以外的文件（除非用户明确要求）
- 禁止删除已有文档文件
- `autocorrect --fix` 可能误改代码文件中的字符串——仅对 `.md`/`.mdx` 运行
- 修改 `{{#include}}` 路径前必须验证目标文件存在

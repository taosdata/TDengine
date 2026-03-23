---
name: doc-writing
description: "Write and review TDengine Markdown documentation following project CI standards (markdownlint, typos, AutoCorrect). Use when creating/editing docs or fixing format issues."
---

# doc-writing

Write and review high-quality Markdown documentation for TDengine project, ensuring compliance with CI checks.

## When to use

- Creating new Markdown documentation
- Editing existing documentation
- Reviewing documentation for format/quality issues
- Fixing CI check failures (markdownlint, typos, AutoCorrect)

## Input

Ask the user for:
- Task type: create/edit/review
- File path (for edit/review) or target directory (for create)
- Content requirements (for create)

## Core Format Rules (MUST follow)

### 1. Code Blocks
- Use ``` backticks (NOT ~~~)
- MUST specify language: ```java, ```python, ```bash, etc.
- Use `text` for plain text / directory trees
- Use fenced blocks, NOT indented blocks

### 2. Links
- Use `[text](url)` format
- NEVER use angle brackets: `<url>` (will fail CI)
- Links must not be empty

### 3. Bold Punctuation (Custom Rule)
- `**Bold:**` MUST have space after: `**Bold:** text` (correct)
- `**Bold:**text` (will fail CI)

### 4. Headings
- Use ATX style (#), NOT underline style
- Space after #: `# Title` (correct), `#Title` (wrong)
- No indentation before #

### 5. Chinese-English Mixed Text
- Add space between Chinese and English: `使用 Java SDK` (correct)
- Add space between Chinese and numbers: `版本 3.0` (correct)
- Use full-width punctuation in Chinese: `这是句子。` (correct)

## Disabled Rules (can ignore)

- MD001: Heading levels can skip
- MD013: No line length limit
- MD024: Duplicate headings allowed
- MD025: Multiple level-1 headings allowed
- MD029: Ordered list numbers can be arbitrary
- MD033: HTML tags allowed
- MD041: First line doesn't need to be heading
- MD052: Reference links allowed
- MD060: Unordered list style not enforced

## Directory Structure

- Chinese docs: `docs/zh/`
- English docs: `docs/en/`
- Root-level docs: `./*.md` (README, INSTALL, etc.)
- Project sub-docs: `tools/tdgpt/**/*.md`, etc.
- Internal docs (CI excluded): `.claude/` - Documentation here is excluded from all CI checks

## Workflow

### Create New Document
1. Read similar existing docs to understand style
2. Create document following format rules
3. Ensure code examples are runnable
4. Output: file path + brief summary

### Edit Document
1. Use Read tool to load document
2. Use Edit tool for precise changes
3. Maintain consistent style
4. Output: file path + changes made

### Review Document
1. Check format: code blocks, links, bold punctuation
2. Check content: technical accuracy, completeness
3. List issues with priority
4. Output: review report

## Quality Check Tools

Project uses these tools in CI (`.github/workflows/tdengine-docs-ci.yml`):

**Format & Style Checks:**
- **markdownlint-cli2** - Markdown format validation (config: `docs/.markdownlint-cli2.jsonc`)
  - Runs on ALL changed `**/*.md` files
- **typos** - Spell checking (config: `docs/typos.toml`)
  - Runs on `docs/zh/`, `docs/en/`, and `./*.md` (root markdown files only)
- **AutoCorrect** - Chinese formatting
  - Runs on `docs/zh/*` and `docs/en/*` only

**CI Exclusions:**
- markdownlint ignores `node_modules/**` and `dist/**`
- typos only checks `docs/zh/`, `docs/en/`, `./*.md`
- AutoCorrect only checks `docs/zh/*`, `docs/en/*`
- `.claude/` directory is excluded from all checks

**Build Validation:**
- Doc build happens in separate repos (docs.taosdata.com, docs.tdengine.com)
- Changes to `docs/` directory trigger build verification

Local check script: `.claude/skills/doc-writing/scripts/check-local.sh`

## Image File Validation (CRITICAL)

When adding or modifying image references in documentation, ALWAYS verify the image file exists.

### Image Reference Rules

**MUST check before adding:**
1. Verify image file exists at the referenced path
2. Check both Chinese (`docs/zh/`) and English (`docs/en/`) versions if applicable
3. Ensure image path is relative to the markdown file location

**Validation steps:**
1. Extract image path from markdown: `![text](path)`
2. Resolve relative path based on markdown file location
3. Use Read tool to verify file exists
4. If missing, warn user: "Image file not found: {path}. Please add the image or remove the reference."

### When to skip image checks

- Image URLs (starting with `http://` or `https://`)
- Placeholder images marked with comments: `<!-- ![Placeholder](./image.png) -->`
- Images in code blocks or inline code

## Common Errors

```markdown
**Param:**text          -> **Param:** text
<https://example.com>   -> [link](https://example.com)
~~~python               -> ```python
使用Java SDK            -> 使用 Java SDK
```

## Output Format

After completing task, briefly state:
- File path
- Main content or changes
- Items needing human review (if any)

## Safety

- Do NOT modify files outside docs directories without confirmation
- Do NOT commit changes automatically
- Do NOT add sensitive information (credentials, tokens) to docs
- Warn user if detecting potential security issues in examples

## References

See `CHEATSHEET.md` for quick reference of common scenarios.

---

## Detailed Rules Reference

### 标题规范（markdownlint）

### 1.1 使用 ATX 样式（# 号）
项目要求使用 `#` 号标题，不能使用下划线样式。

```markdown
正确：
### 三级标题

错误（会报错）：
三级标题
--------
```

### 1.2 标题前后空格
```markdown
正确：
# 文档标题
## 二级标题

错误（MD018/MD019）：
#文档标题      <- # 后缺少空格
##  二级标题    <- # 后多个空格
```

### 1.3 标题不能缩进
```markdown
正确：
## 二级标题

错误（MD023）：
  ## 二级标题    <- 前面有空格
```

---

## 二、代码块规范（markdownlint）

### 2.1 使用围栏代码块（反引号）
```markdown
正确：
```java
System.out.println("Hello");
```

错误（MD046）：
    System.out.println("Hello");  <- 使用缩进代码块
```

### 2.2 使用反引号而非波浪号
```markdown
正确（MD048）：
```bash
echo "Hello"
```

错误：
~~~bash
echo "Hello"
~~~
```

### 2.3 必须指定语言
代码块必须指定语言以通过 MD040 检查：

```markdown
推荐语言标识：
```java
```python
```bash
```json
```yaml
```sql
```text     <- 用于目录树、纯文本
```

---

## 三、链接规范（markdownlint）

### 3.1 链接不能为空
```markdown
正确：
[链接文本](https://example.com)
[锚点链接](#section-id)

错误（MD042）：
[链接文本]()  <- 地址为空
```

### 3.2 不要使用尖括号包裹 URL（自定义规则）
项目自定义规则禁止尖括号包裹 URL：

```markdown
正确：
访问 https://example.com 获取更多信息
[点击这里](https://example.com)

错误（no-angle-bracket-url）：
访问 <https://example.com> 获取更多信息
```

### 3.3 避免使用 javascript: 链接
```markdown
错误：
[点击](javascript:void(0))
```

---

## 四、中文排版规范（AutoCorrect）

### 4.1 中英文之间加空格
这是 AutoCorrect 的核心规则，会被检查：

```markdown
正确：
使用 Java SDK 进行开发
TDengine 是一个时序数据库
支持 REST API 和 WebSocket

错误（会被 AutoCorrect 标记）：
使用Java SDK进行开发
TDengine是一个时序数据库
支持REST API和WebSocket
```

### 4.2 中文与数字之间加空格
```markdown
正确：
版本 3.0 已发布
有 100 个连接

错误：
版本3.0已发布
有100个连接
```

### 4.3 全角标点符号
```markdown
正确：
这是一个中文句子。
请在"设置"中配置参数。
选择"文件"->"新建"。

错误：
这是一个中文句子.
请在"设置"中配置参数.
```

### 4.4 强调符号后加空格（自定义规则）
项目自定义规则要求 `**` 后加空格：

```markdown
正确（space-after-punctuation-in-emphasis）：
**重要**：这是提示内容。
**注意** - 请先备份数据。

错误：
**重要**:这是提示内容。
**注意**-请先备份数据。
```

---

## 五、列表规范

### 5.1 有序列表（项目已禁用 MD029 限制）
```markdown
都可以：
1. 第一项
2. 第二项
3. 第三项

或：
1. 第一项
1. 第二项
1. 第三项
```

### 5.2 无序列表（项目已禁用 MD060 统一风格）
```markdown
都可以（但建议统一）：
- 项目 1
- 项目 2

或：
* 项目 1
* 项目 2
```

---

## 六、命名规范

### 6.1 文件名规范
```text
正确：
01-overview.md
get-started-docker.md
api-reference.md

错误：
01 overview.md          <- 包含空格
GetStartedDocker.md     <- 驼峰命名
GET-STARTED.MD          <- 大写扩展名
```

### 6.2 目录命名
```text
正确：
01-intro/
02-get-started/
api-reference/

错误：
01_intro/
Get Started/
```

---

## 七、处理自定义术语（typos）

如果某些专业术语被 typos 误报为拼写错误，添加到 `docs/typos.toml`：

```toml
[default.extend-identifiers]
TDengine = "TDengine"

[default.extend-words]
API = "API"
SDK = "SDK"
```

---

## 八、已禁用的规则（无需担心）

以下规则已在配置中禁用，你可以放心使用：

| 规则 | 说明 | 禁用原因 |
|------|------|---------|
| MD001 | 标题必须逐级递增 | 允许跳级 |
| MD013 | 行长度限制 | 不限制行长度 |
| MD024 | 禁止重复标题 | 不同章节可用相同标题 |
| MD025 | 只能有一个一级标题 | 允许多个一级标题 |
| MD029 | 有序列表序号必须递增 | 允许任意序号 |
| MD033 | 禁止行内 HTML | 允许 HTML 标签 |
| MD041 | 第一行必须是标题 | 允许导入语句在前 |
| MD052 | 引用链接必须存在 | 允许使用引用链接 |
| MD060 | 无序列表风格统一 | 不强制统一 |

---

## 九、提交前自查清单

在提交 PR 前，确认以下事项：

- [ ] 标题使用 `#` 号，后面有空格
- [ ] 代码块使用反引号，并指定了语言
- [ ] 中英文之间有空格
- [ ] 中文使用全角标点
- [ ] 链接地址不为空
- [ ] 没有使用尖括号包裹 URL
- [ ] 文件名使用小写和连字符
- [ ] **强调符号后有空格**（重要！自定义规则）
- [ ] **图片文件已验证存在**（所有 `![](path)` 引用的图片文件都已检查）

---

## 参考文件

| 文件 | 用途 |
|------|------|
| `.github/workflows/tdengine-docs-ci.yml` | CI 格式检查和构建工作流 |
| `docs/.markdownlint-cli2.jsonc` | markdownlint 配置 |
| `docs/custom-rules/no-angle-bracket-url.js` | 自定义规则：禁止尖括号 URL |
| `docs/custom-rules/space-after-punctuation-in-emphasis.js` | 自定义规则：强调符号后加空格 |
| `docs/typos.toml` | 拼写检查白名单 |

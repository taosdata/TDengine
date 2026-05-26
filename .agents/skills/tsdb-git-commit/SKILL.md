---
name: tsdb-git-commit
description: tsdb commit 规范，TSDB 仓库 commit 提交、git commit 飞书链接、Git commit message 规范 skill，基于 Conventional Commits 标准。在编写 git commit message、检查提交规范、生成 CHANGELOG 时触发。
metadata:
  author: Simon Guan
  version: 1.0.0
  owner_team: engine
---

# tsdb-git-commit

## When to Use

- 带 tsdb / TDengine 关键字的生成 git commit 要求
- 带 tsdb / TDengine 和飞书链接生成 git commit 要求
- 在 tsdb 仓库目录下生成 git commit 要求
- 在 tsdb 仓库目录下，带飞书链接生成 git commit 要求

# Git Commit 提交规范

当用户需要提交代码时，**必须严格遵循**以下规范生成 commit message，不得自行发挥格式。

## 提交格式

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

---

## 1. Type（类型）— 必填，只能选以下之一

| 类型               | 说明                                  |
| ------------------ | ------------------------------------- |
| `feat`             | 新功能（feature）                     |
| `enh`              | 已有功能优化（enhancement）           |
| `fix`              | 修复 bug                              |
| `docs`             | 文档变更                              |
| `ci`               | CI 相关变更                           |
| `refactor` / `ref` | 代码重构                              |
| `perf`             | 性能优化                              |
| `test`             | 测试相关                              |
| `release`          | 发布相关                              |
| `chore`            | 其他杂项变更（构建、辅助工具等）      |

---

## 2. Scope（范围）— 可选

**核心模块：**
`parser` | `planner` | `executor` | `qworker` | `catalog` | `function` | `scalar` | `qcom` | `stream` | `udf` | `vnode` | `dnode` | `mnode` | `client` | `common`

**仓库子系统：**
`adapter` | `connector` | `docs` | `test` | `ci` | `cmake` | `tools` | `packaging` | `build` | `scripts`

---

## 3. Description（描述）— 必填规则

- **必须以动词开头，使用现在时**（add / fix / update / remove / refactor，不用 added / fixed）
- 首字母小写（专有名词除外）
- 末尾不加句号
- 标题行控制在 **72 个字符以内**

---

## 4. Footer（页脚）

- **用户提供了任务链接时**，必须在 footer 中使用 `Closes` 关联，格式为 `[ID](url)`，ID 取链接末尾数字：
  ```
  Closes [6857598725](https://project.feishu.cn/taosdata_td/sub_task1/detail/6857598725)
  ```
- 多个链接使用列表格式：
  ```
  Closes
  - [6857598725](https://project.feishu.cn/taosdata_td/sub_task1/detail/6857598725)
  - [9999999999](https://project.feishu.cn/taosdata_td/sub_task1/detail/9999999999)
  ```
- **用户未提供任何链接时，省略 footer，不得自行补充或虚构链接。**
- Breaking Changes（如有）：`BREAKING CHANGE: <说明>`

---

## 示例

```
feat(executor): add hash join fallback path

Improve robustness when build-side hash table exceeds threshold.

Closes [6857598725](https://project.feishu.cn/taosdata_td/sub_task1/detail/6857598725)
```

```
fix(parser): fix AST node leak in nested query branch

Closes [6857598725](https://project.feishu.cn/taosdata_td/sub_task1/detail/6857598725)
```

```
docs(docs): update stream external_window usage guide

（无任务链接，省略 footer）
```

---

## 提交前检查清单

在生成或审查 commit message 时，逐项确认：

- [ ] Type 是否来自允许列表？
- [ ] Scope 是否准确（如适用）？
- [ ] Description 是否以动词开头且使用现在时？
- [ ] 标题行是否在 72 字符以内？
- [ ] 是否关联了任务链接（有链接时必须 Closes，无链接则省略 footer）？
- [ ] 是否清晰表达了变更意图？

---

## Input（输入）

| 参数 | 必填 | 说明 |
| ---- | ---- | ---- |
| 变更描述 / diff | 是 | 用户描述本次改动，或提供 `git diff` / 暂存区内容 |
| 任务链接 | 否 | 飞书任务 URL；有则必须写入 footer，无则省略 |
| Scope | 否 | 若用户未指定，根据 tsdb 仓库改动文件自动推断；无法推断则省略 |

若变更描述不足以确定 type 或 scope，向用户追问一次，不得自行猜测。

---

## Output（输出）

输出一段完整的 commit message，格式固定为：

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

- 直接输出可复制的文本块，无额外解释。
- 若用户要求审查已有 commit message，逐项列出不符合规范之处并给出修改建议。

---

## Safety（安全）

- 本 skill 仅生成文本，不执行任何 git 命令，不写入文件。
- **禁止**虚构任务链接或 issue 编号；无链接时必须省略 footer。
- **禁止**在 description 中暴露密钥、密码、内部 IP 等敏感信息。

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-git-commit version=0.2.0 author=Simon Guan`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->


# 输出与发布格式

## 终端输出

终端报告固定包含三段：

1. `Review summary`
2. `Final findings`
3. `Publish preview`

`Review summary` 必须先明确当前 target：

- `github_pull_request`：显示 PR 编号、标题、URL
- `gitlab_merge_request`：显示 MR IID、标题、URL
- `branch_diff`：显示 `base_ref..HEAD` 与当前分支名（若可得）

每条 final finding 至少包含：

- Category
- 简短标题
- 为什么这是问题
- 证据位置
- 修复方向
- 是否适合发布为 inline comment

最小终端模板：

```text
Review summary
- Status: completed | skipped | failed
- Target: github_pull_request #<number> <title> <url> | gitlab_merge_request !<iid> <title> <url> | branch_diff <base_ref>..HEAD [branch: <name>]
- Coverage: <completed reviewers and any degraded coverage>

Final findings
1. [<severity>/<confidence>] <category>: <title>
   - Problem: <why this is a real issue>
   - Evidence: <file:line or concrete diff/context>
   - why_target_related: <why this target introduced or amplified it>
   - Fix direction: <actionable repair guidance>
   - Publishability: summary | inline | not publishable

Publish preview
- Summary comment: <will publish | disabled | not requested>
- Inline comments: <count and target locations, none, or inline skipped with reason>
```

## 发布总则

- 只有 `github_pull_request` 或 `gitlab_merge_request` target 且用户显式要求时才发布。
- `branch_diff` target 一律不发布。
- 平台发布内容是已验证 findings 按 `publishability` 与目标平台能力过滤后的子集（可与终端结果集合相等）。
- `publishability` 仅表示已验证 finding 是否进入平台发布集合；进入该集合的 finding 先进入 summary comment，只有局部且可精确定位的问题才进一步进入 inline comments。
- GitHub PR 发布使用 `gh`。
- GitLab MR 发布使用 `glab`。
- 如果发布模式下没有问题，也要发布一条简短的 `No issues found` summary comment。

## Publish preview

- `github_pull_request`：预览发布模式下将生成的 summary comment 与 inline comment 候选；若没有问题，则预览 `No issues found` summary comment。
- `gitlab_merge_request`：预览发布模式下将生成的 summary comment、inline comment 候选、inline 能力探测结果；无法发布 inline 时写明 `inline skipped` 与原因。
- `branch_diff`：固定写明“发布已禁用：branch diff target 不支持发布”。

## 评论类型关系

- summary comment 是独立评论类型。
- inline comment 是另一种评论类型。
- Committable Suggestion 是 inline comment 的特例，不是第三种平级 comment。

## github_pull_request 发布内容

### Summary Comment

- 说明发现了多少个问题。
- 按优先级排序或分组。
- 保持简洁。
- 跨文件问题、高层设计问题、边界问题留在 summary comment。

创建方式：

- summary comment：`gh pr comment "$PR" --body-file /tmp/code-review-summary.md`

### Inline Comments

只有同时满足以下条件时才发布：

- 能精确映射到具体文件和行范围。
- 问题在局部上下文里就能理解。
- 作者可以直接据此行动。

创建方式：

- inline comment：`gh api --method POST repos/{owner}/{repo}/pulls/{pull_number}/comments`

inline comment 必须能从当前 PR diff 映射出以下字段：

- `body`：评论正文
- `commit_id`：PR head commit SHA
- `path`：目标文件路径
- `line`：diff 中目标行号
- `side`：通常为 `RIGHT`；只有评论 base 侧删除行时才用 `LEFT`
- `start_line` 与 `start_side`：仅用于多行评论；单行评论不要伪造多行字段

无法可靠映射 `commit_id`、`path`、`line`、`side` 时，不得创建 inline comment；把该 finding 留在 summary comment。

### Committable Suggestion

只有 suggestion patch 足以完整修复问题时才允许使用。
如果应用 suggestion 后还需要额外修改，禁止把它伪装成 committable suggestion。

## gitlab_merge_request 发布内容

### 能力探测

发布前必须做能力探测：

- `glab mr note create --help` 能执行。
- help 输出包含 `--message` 或 `-m`，或当前版本明确支持从 stdin 读取正文。
- 若要发布 inline，help 输出必须包含 `--file`，且包含 `--line` 或 `--old-line`。

### Summary Comment

发布模式下始终尝试发布一条 summary comment：

- 有 findings 时，按优先级列出可发布问题。
- 没有 findings 时，发布简短的 `No issues found` summary comment。
- comment body 必须包含本 skill 固定 marker，用于后续重复发布检测。

创建方式：

- summary comment：`glab mr note create "$MR" < /tmp/code-review-summary.md`
- 若目标 `glab` 版本不支持从 stdin 读取正文，再使用 `--message`，但不得用会破坏大段 Markdown 的临时拼接方式。

### Inline Comments

只有同时满足以下条件时才发布：

- 用户显式要求发布。
- finding 的 `publishability` 为 `inline`。
- finding 能精确映射到 MR diff 中的文件和行。
- 问题在局部上下文中可理解，作者可以直接行动。
- `glab mr note create` 支持 diff comment 参数。

创建方式：

- new-side inline comment：`glab mr note create "$MR" --file "$PATH" --line "$LINE" --message "$BODY"`
- old-side inline comment：`glab mr note create "$MR" --file "$PATH" --old-line "$OLD_LINE" --message "$BODY"`

多行 inline comment 只有在 finding 能可靠映射完整范围时才允许使用 `--line start:end`。否则使用单行评论或降级到 summary comment。

无法可靠映射目标文件和行号，或当前 `glab` 不支持 inline 参数时，不得创建 inline comment；把该 finding 留在 summary comment，并在 `Publish preview` 写明 `inline skipped` 与原因。

## branch_diff 不支持发布

- 不创建 summary comment
- 不创建 inline comment
- 不创建 committable suggestion
- 如果用户显式要求发布 branch diff 结果，run 直接返回 `failed`
- 失败前不得执行任何平台写入命令

## 发布失败

- `github_pull_request` 或 `gitlab_merge_request` review 已完成但发布失败时，保留终端结果
- 单独报告发布失败
- 不得把发布失败误报成 `No issues found`
- `branch_diff` target 不应进入“发布失败”分支，因为发布路径应在更早阶段被拒绝

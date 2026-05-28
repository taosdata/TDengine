# 输出与发布格式

## 终端输出

终端报告固定包含三段：

1. `Review summary`
2. `Final findings`
3. `Publish preview`

`Review summary` 必须先明确当前 target，显示 MR 编号、标题、URL。

每条 final finding 至少包含：

- Category（问题类别）
- 简短标题
- 为什么这是问题
- 证据位置（文件路径 + 行号）
- 修复方向
- 是否适合发布为评论

最小终端模板：

```text
Review summary
- Status: completed | skipped | failed
- Target: GitLab MR !<number> <title> <url>
- Coverage: <completed reviewers and any degraded coverage>

Final findings
1. [<severity>/<confidence>] <category>: <title>
   - Problem: <why this is a real issue>
   - Evidence: <file:line or concrete diff/context>
   - why_target_related: <why this MR introduced or amplified it>
   - Fix direction: <actionable repair guidance>
   - Publishability: summary | inline | not publishable

Publish preview
- Summary comment: <will publish | disabled | not requested>
- Inline comments: <count and target locations, or none>
```

## GitLab 发布总则

- 只有用户明确确认后才发布到 GitLab（review 完成后主动提示，用户确认方可发布）
- GitHub 发布路径不存在；本 skill 只向 GitLab 发评论
- GitLab 发布内容是已验证 findings 按 `publishability` 过滤后的子集
- `publishability` 仅表示已验证 finding 是否进入 GitLab 发布集合；进入该集合的 finding 先进入 summary comment，只有局部且可精确定位的问题才进一步进入 inline comments
- 进入发布模式时，始终发布一条 summary comment
- 进入发布模式时，只有局部且可精确定位的问题才发布 inline comments
- 如果在发布模式下没有问题，也要发布一条简短的 `No issues found` summary comment
- summary comment 必须包含以下 marker，用于防止重复发布检测：

  ```
  <!-- tsdb-review-doc:summary -->
  ```

## Publish preview

预览发布模式下将生成的 summary comment 与 inline comment 候选；若没有问题，则预览 `No issues found` summary comment。

## 评论类型关系

- summary comment 是独立评论类型
- inline comment 是 MR diff 的行级评论（note）
- Committable Suggestion 不适用于文档 review（文档 patch 由作者决定，不提供一键提交建议）

## GitLab 发布命令

```bash
# 发布 summary comment（通用 MR 评论）
# 方式 1：内容较短时直接使用 -m
GITLAB_HOST=<host> glab mr note <id> --repo <namespace> \
  -m "<!-- tsdb-review-doc:summary -->
**Doc Review Summary**

<review summary in markdown>"

# 方式 2：内容较长时，先写入临时文件再读取（推荐，避免 shell 引号转义问题）
cat > /tmp/tsdb-review-doc-comment.md << 'COMMENT_EOF'
<!-- tsdb-review-doc:summary -->
**Doc Review Summary**

<review summary in markdown>
COMMENT_EOF

GITLAB_HOST=<host> glab mr note <id> --repo <namespace> \
  -m "$(cat /tmp/tsdb-review-doc-comment.md)"

# 注意：glab mr note 不支持 -F <file> 标志，必须用 -m 传入内容字符串
```

```bash
# 发布 inline comment（行级 note，使用 GitLab API）
# 通过 glab api 调用 GitLab REST API：
GITLAB_HOST=<host> glab api \
  "projects/<url-encoded-namespace>/merge_requests/<id>/discussions" \
  -X POST \
  -F "body=<comment>" \
  -F "position[position_type]=text" \
  -F "position[base_sha]=<base_sha>" \
  -F "position[head_sha]=<head_sha>" \
  -F "position[start_sha]=<start_sha>" \
  -F "position[new_path]=<file_path>" \
  -F "position[new_line]=<line_number>"
# 注意：glab api 的 -F 是表单字段，与 glab mr note 的用法不同
```

inline comment 必须能从当前 MR diff 映射出以下字段：

- `body`：评论正文
- `position[base_sha]`：MR base commit SHA（从 `glab mr view -F json` 获取）
- `position[head_sha]`：MR head commit SHA
- `position[start_sha]`：通常与 `base_sha` 相同
- `position[new_path]`：目标文件路径
- `position[new_line]`：diff 中目标行号

无法可靠映射上述字段时，不得创建 inline comment；把该 finding 留在 summary comment。

## 发布失败

- review 已完成但发布失败时，保留终端结果
- 单独报告发布失败
- 不得把发布失败误报成 `No issues found`

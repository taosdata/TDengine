---
name: tsdb-review-code
description: Use when reviewing a GitHub Pull Request, GitLab Merge Request, or explicit base..HEAD branch diff, especially when the user asks for a terminal review report or platform comments.
metadata:
  author: Jinqing Kuang
  version: 1.0.0
  owner_team: engine
---

# TSDB Code Review

## 概览

对代码 review target 做高信噪比审查。支持 GitHub Pull Request、GitLab Merge Request 和显式指定 base 的 branch diff。先由 `Change Summarizer` 建立共享上下文，再做广覆盖、规则、性能与可维护性审查，最后只保留经过验证的问题。

## 前置环境检查：GitLab CLI

在解析 review target、读取 MR 信息或生成 diff 之前，必须先确认 `glab` 可用且已登录：

1. 执行 `command -v glab`。
   - 若未安装，当前 run 直接返回 `failed`，不要继续 review。
   - 告知用户需要先安装 GitLab CLI 并登录；安装是系统级变更，除非用户明确要求并确认，不得自动安装。
   - 可给出安装入口：macOS 使用 `brew install glab`；其他系统参考 `https://gitlab.com/gitlab-org/cli#installation`。
2. 执行 `glab auth status --hostname <host>`。
   - `<host>` 来自 MR URL；没有 MR URL 时默认 `gitlab.com`。
   - 若未登录，要求用户执行 `glab auth login --hostname <host> --web`，或在用户明确同意后协助运行交互式登录。
   - 登录流程必须使用 `glab` 的浏览器/OAuth 流程；不得要求用户粘贴 token、密码或其他凭证。
3. 登录后再次执行 `glab auth status --hostname <host>`。
   - 仍失败时，当前 run 返回 `failed`，并说明 `glab` 认证未通过。
   - 只有 `command -v glab` 和 `glab auth status --hostname <host>` 都通过后，才允许继续后续 workflow。

## 触发示例

- “审查这个 GitHub PR，先别发评论。”
- “帮我 review PR #123，并把可发布的问题发到 GitHub。”
- “审查这个 GitLab MR，先别发评论。”
- “帮我 review GitLab MR !123，并把可发布的问题发到 GitLab。”
- “基于 main review 当前分支的 branch diff，只要终端结果。”
- “基于 main 看这个 branch diff 有没有性能和可维护性风险。”

## 先读哪些文件

1. 先读 `references/workflow.md`
2. 读取 `references/output-format.md`（终端报告必需；若用户显式要求发布，再使用其中发布部分）
3. 在结束前对照 `references/manual-validation.md`

## 硬约束

- 审查目标只能是 `GitHub Pull Request`、`GitLab Merge Request` 或 `显式提供 base 的 branch diff`
- `branch diff` 的 `head` 固定为当前 `HEAD`
- `branch diff` 禁止发布
- GitHub PR 读取和写入使用 `gh`
- GitLab MR 读取和写入使用 `glab`
- 只提供 URL 时必须按 path pattern 区分 GitHub PR 与 GitLab MR，不得靠命令试错猜平台
- 默认只输出到终端
- 未验证的 `candidate issue` 不得进入最终 findings
- 终端输出必须区分 `skipped`、`failed`、`completed`
- 不运行 CI、build、lint、test
- 固定 reviewer 只能是 `Change Summarizer`、`Broad Scanner`、`Performance Reviewer`、`Maintainability Reviewer`、`Rule Reviewer`
- 规则类问题必须验证作用域是否真的命中当前路径
- 最终问题必须明确与当前 review target 有关

## 执行骨架

- 解析 review target，并按目标类型执行 eligibility gate；细则见 `references/workflow.md`
- 组装共享上下文；先产出 change summary，再并行运行固定 reviewer
- 只把带完整字段的 `candidate issues` 送入验证；验证失败的项直接丢弃
- 按 `references/output-format.md` 生成终端报告；只有 GitHub PR / GitLab MR target 且用户显式要求时才发布
- 终止、降级、大型 target 处理均以 `references/workflow.md` 为准

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-review-code version=1.0.0 author=Jinqing Kuang`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

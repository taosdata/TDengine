---
name: tsdb-review-doc
description: >
  对 TDengine TSDB 文档 MR 进行高信噪比审查，仅支持 GitLab MR 链接（如 https://git.tdengine.net/rd-public/tsdb/-/merge_requests/151）。
  检查文档准确性、完整性、清晰度、语法、代码示例、链接有效性及格式规范。
  Trigger keywords: tsdb doc review, tsdb 文档审查, 文档 MR review, gitlab doc review, review doc MR, 审查文档 MR.
metadata:
  author: wpan
  version: 1.0.0
  owner_team: engine
---

# TSDB Doc Review

## 概览

对 TSDB GitLab MR 中的文档变更做高信噪比审查。只接受 GitLab MR 链接作为输入，覆盖准确性、完整性、清晰度、代码示例、链接、格式规范等维度，最终只保留经过验证的问题。

## 前置环境检查：GitLab CLI

在读取 MR 信息或生成 diff 之前，必须先确认 `glab` 可用且已登录：

1. 执行 `command -v glab`。
   - 若未安装，当前 run 直接返回 `failed`，不要继续 review。
   - 告知用户需要先安装 GitLab CLI；安装是系统级变更，除非用户明确要求并确认，不得自动安装。
   - 安装参考：`https://gitlab.com/gitlab-org/cli/-/releases`
2. 从 MR URL 解析出 `<host>`（默认 `git.tdengine.net`），执行：
   ```bash
   GITLAB_HOST=<host> glab auth status
   ```
   - 若未登录，要求用户执行 `GITLAB_HOST=<host> glab auth login`。
   - 登录流程必须使用 `glab` 的浏览器或 token 流程；不得要求用户粘贴密码或私钥。
3. 登录后再次执行 `GITLAB_HOST=<host> glab auth status`。
   - 仍失败时，当前 run 返回 `failed`，并说明 `glab` 认证未通过。
   - 只有两项检查都通过后，才允许继续后续 workflow。

## 触发示例

- "帮我 review 这个文档 MR：https://git.tdengine.net/rd-public/tsdb/-/merge_requests/151"
- "审查一下这个 MR 的文档变更。"
- "这个 MR 的文档写得准确吗？"
- "tsdb doc review https://git.tdengine.net/rd-public/tsdb/-/merge_requests/151"

## 先读哪些文件

1. 先读 `references/workflow.md`
2. 读取 `references/output-format.md`（终端报告必需；若用户显式要求发布，再使用其中发布部分）
3. 读取 `references/templates.md`（Template Compliance Reviewer 必需；识别文档类型后加载对应必需章节列表）
4. 在结束前对照 `references/manual-validation.md`

## 硬约束

 `GitLab MR URL`；不支持分支 diff、本地改动或 GitHub PR
- 所有 GitLab 读取和写入都使用 `glab`
- 默认只输出到终端；review 完成后主动提示用户是否发布，未经确认不得向 GitLab MR 发评论
- 未验证的 `candidate issue` 不得进入最终 findings
- 终端输出必须区分 `skipped`、`failed`、`completed`
- 不运行 CI、build、lint、test
- 固定 reviewer 只能是 `Change Summarizer`、`Accuracy Reviewer`、`Completeness Reviewer`、`Clarity Reviewer`、`Format Reviewer`、`Template Compliance Reviewer`、`Cross-Doc Consistency Reviewer`
- 最终问题必须明确与当前 MR 的文档变更有关

## 执行骨架

- 解析 MR URL，提取 host、namespace、MR 编号；细则见 `references/workflow.md`
- 执行 eligibility gate；详见 `references/workflow.md`
- 组装共享上下文；先产出 change summary，再并行运行固定 reviewer
- 只把带完整字段的 `candidate issues` 送入验证；验证失败的项直接丢弃
- 按 `references/output-format.md` 生成终端报告；review 完成后主动询问用户是否发布到 GitLab（见 `references/workflow.md §10`）；未经用户明确确认不得发布
- 终止、降级、大型 target 处理均以 `references/workflow.md` 为准

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-review-doc version=1.0.0 author=wpan`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

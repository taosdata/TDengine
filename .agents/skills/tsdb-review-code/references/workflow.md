# Review Target 审查工作流

本文档定义 `github_pull_request`、`gitlab_merge_request` 与 `branch_diff` 三类 review target 的主流程、固定 reviewer 分工、candidate issue schema、验证规则、状态流转与排序过滤策略。

## 目录

1. [目标解析](#1-目标解析)
2. [Eligibility Gate](#2-eligibility-gate)
3. [共享审查上下文](#3-共享审查上下文)
4. [固定 reviewer 角色](#4-固定-reviewer-角色)
5. [candidate issues](#5-candidate-issues)
6. [验证与 finding](#6-验证与-finding)
7. [状态与降级](#7-状态与降级)
8. [排序与过滤](#8-排序与过滤)
9. [大型 review target](#9-大型-review-target)

## 1. 目标解析

### URL 平台判定

当用户只提供一个 URL 时，先按 URL path pattern 判定平台，不靠命令试错：

- GitHub PR URL 必须匹配 `/<owner>/<repo>/pull/<number>`，例如 `https://github.com/org/repo/pull/123`。
- GitLab MR URL 必须匹配 `/<group>/<project>/-/merge_requests/<iid>`，并支持嵌套 group，例如 `https://gitlab.com/group/subgroup/repo/-/merge_requests/123`。
- 自建域名也按 path pattern 判定，例如 `https://git.company.com/a/b/-/merge_requests/12` 判定为 GitLab MR，`https://github.company.com/a/b/pull/12` 判定为 GitHub PR。
- URL 不匹配上述明确模式时，当前 run 返回 `failed`，终端说明无法从 URL 判断是 GitHub PR 还是 GitLab MR。

不得因为 URL 域名包含 `git`、`gitlab` 或 `github` 就推断平台；不得同时尝试 `gh` 和 `glab` 来猜目标类型。

当用户只提供裸编号时，必须由语义明确平台：

- `GitHub PR #123` 判定为 `github_pull_request`。
- `GitLab MR !123` 判定为 `gitlab_merge_request`。
- 只有 `123` 或 `#123` 且没有平台语义时，当前 run 返回 `failed`，要求用户补充平台或链接。

### github_pull_request

- 优先接受用户提供的 GitHub PR URL 或明确 GitHub 语义的 PR 编号。
- 若用户未提供，使用 `gh pr view --json number,title,body,state,isDraft,url` 解析当前 PR。
- 无法解析 PR，或 `gh` 不可用时，当前 run 直接返回 `failed`。

### gitlab_merge_request

- 优先接受用户提供的 GitLab MR URL 或明确 GitLab 语义的 MR 编号。
- 若用户未提供，允许 `glab` 默认解析当前分支 MR。
- 使用 `glab mr view [<id | branch>] --output json` 读取 MR 元数据。
- 使用 `glab mr diff [<id | branch>] --color=never` 或 `--raw` 读取 MR diff。
- 使用 `glab mr note list [<id | branch>] --output json` 读取现有 notes / discussions。
- 无法解析 MR、`glab` 不可用、`glab` 未认证、或无法读取可信 diff 时，当前 run 直接返回 `failed`。

### branch_diff

- 只有用户明确要求 review branch diff 时，才进入此路径。
- 用户必须显式提供 `base branch` 或 `base commit`。
- `head` 固定为当前 `HEAD`。
- 使用 `base..HEAD` 作为审查范围。
- 缺少 `base`、`base` 无法解析、或无法生成可信的 `base..HEAD` diff 时，当前 run 直接返回 `failed`。

## 2. Eligibility Gate

### github_pull_request

满足以下任一条件时，当前 run 返回 `skipped`：

- PR 已关闭。
- PR 仍是 draft。
- 改动过于 trivial，仅机械性变化，几乎没有行为风险。
- 用户要求发布到 GitHub，且本 skill 已经对同一 PR 发布过评论。

`trivial` 仅指极小的机械性变化，例如纯重命名、纯格式化、注释改写或无语义变更的生成物更新。只要改动涉及控制流、数据结构、接口契约、配置语义或错误处理，就不得按 `trivial` 跳过。

“本 skill 已经对同一 PR 发布过评论”只按本 skill 之前发布的 summary comment 固定 marker 识别；没有该 marker，不得判定为重复发布。

说明：即使之前已经发布过评论，仍允许只在终端重新运行 GitHub PR review（不发布）。

### gitlab_merge_request

满足以下任一条件时，当前 run 返回 `skipped`：

- MR 已关闭或已合并。
- MR 仍是 draft / WIP。
- 改动过于 trivial，仅机械性变化，几乎没有行为风险。
- 用户要求发布到 GitLab，且本 skill 已经对同一 MR 发布过评论。

`trivial` 与 GitHub PR gate 使用同一标准：仅限极小的机械性变化，不得覆盖涉及控制流、数据结构、接口契约、配置语义或错误处理的改动。

“本 skill 已经对同一 MR 发布过评论”只按本 skill 之前发布的 summary comment 固定 marker 识别；没有该 marker，不得判定为重复发布。

说明：即使之前已经发布过评论，仍允许只在终端重新运行 GitLab MR review（不发布）。

### branch_diff

满足以下条件时，当前 run 返回 `failed` 或 `skipped`：

- 未显式提供 `base`：`failed`
- `base` 无法解析：`failed`
- `base..HEAD` 没有可审查改动：`skipped`
- `base..HEAD` 只有极小机械性改动，几乎没有行为风险：`skipped`

这里的 `trivial` 与 GitHub PR / GitLab MR gate 使用同一标准：仅限极小的机械性变化，不得覆盖涉及控制流、数据结构、接口契约、配置语义或错误处理的改动。

以下 PR / MR 专属条件不适用于 `branch_diff`：

- `closed`
- `merged`
- `draft`
- `WIP`
- 重复发布

## 3. 共享审查上下文

所有 reviewer 共享以下上下文：

- `target_type`
- `target_display`
- diff 与变更文件列表
- 命中路径作用域的 `AGENTS.md`、`CLAUDE.md` 及仓库显式约定
- 为性能或可维护性分析所需的额外仓库上下文

按目标类型补充：

### github_pull_request

- PR `title` 与 `description`
- PR URL / 编号
- 现有 PR comments（PR discussion comments、review comments，以及由本 skill 之前发布的 summary comments）

### gitlab_merge_request

- MR `title` 与 `description`
- MR URL / IID
- source branch 与 target branch
- state、draft / WIP 状态
- 现有 MR notes / discussions（以及由本 skill 之前发布的 summary comments）

### branch_diff

- `base_ref`
- `head_ref=HEAD`
- 当前分支名（若可得）
- `base..HEAD` 间的 commit messages

边界约束：

- 不运行 CI、build、lint、test。

## 4. 固定 reviewer 角色

### Change Summarizer

- 总结改动意图。
- 标记主要影响区域。
- 标记风险热点。

输入按目标类型切换：

- GitHub PR 模式：读取 PR title、description、现有 PR comments 与 diff。
- GitLab MR 模式：读取 MR title、description、source / target branch、现有 MR notes / discussions 与 diff。
- branch diff 模式：读取 `base_ref`、`HEAD`、当前分支名、commit messages 与 diff。

`Change Summarizer` 是全局前置步骤，失败时整个 run 标记为 `failed`。

### Broad Scanner

- 浅层广覆盖扫描 correctness bug。
- 检查安全风险。
- 检查测试缺口与回归风险。
- 检查文档、注释和 API 可理解性。

### Performance Reviewer

- 深挖复杂度退化。
- 深挖重复或不必要的 I/O。
- 深挖缓存失效。
- 深挖 N+1 行为。
- 深挖资源泄漏。
- 深挖无界增长。
- 深挖并发或协作问题。

### Maintainability Reviewer

- 深挖职责漂移。
- 深挖耦合上升。
- 深挖重复逻辑。
- 深挖异常流程越来越难跟踪。
- 深挖接口复杂化。
- 深挖未来维护成本上升。

### Rule Reviewer

- 检查仓库规则/规范是否被遵循。
- 只检查对当前文件路径真实生效的仓库规则。
- 没有作用域命中的规则，不得硬套。

## 5. candidate issues

每条 `candidate issue` 至少包含以下字段：

- `category`
- `claim`
- `evidence`
- `scope`
- `why_target_related`

缺少任一字段的 `candidate issue` 在验证前直接丢弃。

`why_target_related` 必须按当前 review target 解释：

- `github_pull_request`：说明为什么问题由当前 GitHub PR 引入或放大
- `gitlab_merge_request`：说明为什么问题由当前 GitLab MR 引入或放大
- `branch_diff`：说明为什么问题由 `base..HEAD` 这段差异引入或放大

## 6. 验证与 finding

每条候选问题都必须执行以下验证：

- 真实性（是否客观成立）。
- 当前 review target 归因（是否由当前 target 引入或放大）。
- 若 issue 引用了规则，则验证规则适用性（被引用规则是否在该路径真实生效）。

验证通过后，补充 finding 元数据：

- `confidence`
- `severity`
- `publishability`

`publishability` 仅用于区分“已验证 finding 是否适合进入平台发布候选”。它不决定一个 finding 是否属于已验证 finding。
终端结果集合与平台可发布结果集合不是同一个集合：前者包含全部已验证 findings，后者是按 `publishability` 与目标平台能力过滤后的子集。

验证未通过的问题不得进入最终 findings。

## 7. 状态与降级

- review 成功完成并产出可信终端结果：run 状态为 `completed`。
- 命中 Eligibility Gate 的有意提前退出条件：run 状态为 `skipped`。
- `Change Summarizer` 失败：整个 run 标记为 `failed`。
- 其他 reviewer 失败：继续执行，但必须在终端明确说明覆盖范围下降。
- `candidate issue` 验证失败：不得进入最终 findings。
- 用户要求发布 branch diff 结果：直接返回 `failed`。
- GitLab MR inline 能力不可用或定位不可靠：review 继续；可发布 finding 留在 summary comment，并在终端报告 inline skipped 原因。
- review 已完成且终端结果可信，但 GitHub/GitLab 发布失败：run 状态仍为 `completed`，同时保留终端结果并单独报告发布失败。

## 8. 排序与过滤

最终 findings 按以下优先级排序：

1. `severity`（高到低）
2. `confidence`（高到低）
3. 可行动性（越能直接指导修复越靠前）

必须过滤：

- 与当前 review target 没有实质关系的历史遗留问题。
- 纯主观偏好。
- 缺乏证据的问题。
- 无法明确关联到当前 target 的担忧。

只有当不存在“通过验证的问题”时，才输出 `No issues found`；是否可发布由 `publishability` 单独控制。

## 9. 大型 review target

- 大型 review target 不自动终止。
- 必须提示覆盖率可能下降。
- 必须提示结果不确定性可能更高。
- 必须提示拆小 PR / MR 或缩小 diff 范围会得到更好的 review 质量。
- 优先审查入口点、接口契约、错误处理、数据迁移、权限/安全、并发、资源生命周期和高 churn 文件。
- 对无法穷尽的低风险机械性改动允许抽样，但必须在 `Review summary` 说明抽样范围与未覆盖范围。
- 不得因为 target 很大而省略归因验证；归因或真实性不确定的问题不得进入 final findings。

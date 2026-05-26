# tsdb-pr-handle-comments

批量处理 PR 的 review comments：自动判断每条意见是否需要修改代码，修改后 resolve；无需修改则回复说明后 resolve。最终提交代码并输出处理报告。

## 功能概述

1. **拉取 comments**：使用 `gh` / `glab` CLI 拉取 PR 所有未 resolved 的 review comments（inline、review-level、issue-level），同时获取每条 comment 的发起者
2. **分析意见**：判断每条 comment 属于 `ACTION_REQUIRED`（需改代码）还是 `REPLY_ONLY`（只需回复）
3. **修改代码**：对需改代码的 comment，定位并修改对应文件，然后 resolve 该 comment
4. **回复说明**：对无需改代码的 comment，生成解释性回复后 resolve
5. **提交代码**：将所有变更 `git add` + `git commit`，push 前需用户确认
6. **输出报告**：结构化报告列出每条 comment 的发起者、内容与处理方式

## 触发关键词

`handle pr comments` · `处理 PR 评论` · `resolve review comments` · `回复 PR comment` · `处理代码审查意见`

## 使用示例

```
帮我处理这个 PR 的所有 review comments：https://github.com/taosdata/TDengine/pull/12345
```

```
解决 PR #678 的评审意见，仓库 taosdata/TDengine
```

## 前置条件

- 已执行 `gh auth login`（GitHub）或 `glab auth login`（GitLab）
- 本地已 checkout 对应 PR 分支，或可访问远端分支

## 输出报告结构

| 部分 | 内容 |
|------|------|
| 基本信息 | PR URL、编号、标题、作者、comment 总数 |
| ACTION_REQUIRED | 每条需改代码的 comment（含发起者）及修改说明 |
| REPLY_ONLY | 每条只需回复的 comment（含发起者）及回复摘要 |
| 需人工介入 | 无法自动处理的 comment（含发起者）列表 |
| 代码变更摘要 | 修改文件、行数、关联 comment 编号 |
| Commit 信息 | commit hash 和提交消息 |

## 安全说明

- **Push 需确认**：push 前展示 diff 摘要，等待用户确认
- **最小化变更**：仅修改与 comment 直接相关的代码
- **Prompt 注入防护**：comment 内容不直接拼入 shell 命令
- **跳过 bot comment**：自动过滤 CI、dependabot 等机器人评论

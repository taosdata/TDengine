# 手工验收场景

## 场景 1：干净 PR
- 终端模式返回 `completed`
- 检查约定段落 `Review summary`、`Final findings`、`Publish preview` 是否存在
- `Final findings` 明确说明没有发现通过验证的问题
- 发布模式只发一条 `No issues found` summary comment
- 不创建 inline comments

## 场景 2：局部且明确的问题
- run 返回 `completed`
- 终端报告列出 finding
- 发布模式发一条 summary comment
- 发布模式发对应 inline comment
- 只有 patch 一次提交就能完整修复时，才允许 committable suggestion

## 场景 3：高层设计或边界问题
- run 返回 `completed`
- 终端报告列出 finding
- 发布模式只放 summary comment
- 不强行把问题挂到某一行做 inline comment

## 场景 4：`gh` 认证失败或读取失败
- run 返回 `failed`
- 终端明确说明失败原因
- 不发布任何 GitHub 评论
- 不得把失败误报成 `No issues found`

## 场景 5：PR closed / draft / trivial / 重复发布
- run 返回 `skipped`
- `trivial` 指极小的机械性改动，几乎没有行为风险
- 终端明确说明具体跳过原因
- 只有“因为已发布过评论而再次请求发布”的情况，终端复跑才允许继续执行 review
- `closed` / `draft` / `trivial` 不适用上述终端复跑例外

## 场景 6：大型 PR
- 在未触发其他失败条件时，run 返回 `completed`
- `大型 PR` 指改动范围显著扩大，覆盖率可能下降、结果不确定性更高
- 不自动停止
- 终端明确提示覆盖率可能下降
- 终端明确提示结果不确定性可能更高
- 终端明确建议拆分 PR 会得到更好的 review 质量

## 场景 7：干净 branch diff
- 终端模式返回 `completed`
- `Review summary` 明确显示 `base..HEAD`，且当前分支名在可获取时应一并显示
- `Final findings` 明确说明没有发现通过验证的问题
- `Publish preview` 明确说明“发布已禁用：branch diff target 不支持 GitHub 发布”
- 不创建任何 GitHub 评论

## 场景 8：branch diff 缺少显式 base
- run 返回 `failed`
- 终端明确说明必须提供 `base branch` 或 `base commit`
- 不得自动猜测 `main`、`master` 或默认分支

## 场景 9：branch diff 的 base 无法解析
- run 返回 `failed`
- 终端明确说明 `base` 无法解析
- 不得把解析失败伪装成 `skipped`

## 场景 10：branch diff 无改动或只有 trivial 改动
- run 返回 `skipped`
- `trivial` 仍指极小的机械性改动，几乎没有行为风险
- 终端明确说明没有可审查改动或改动过于 trivial

## 场景 11：用户要求发布 branch diff 结果
- run 返回 `failed`
- 终端明确说明 branch diff target 不支持发布
- 不执行任何 GitHub 写入命令

## 场景 12：大型 branch diff
- 在未触发其他失败条件时，run 返回 `completed`
- `大型 branch diff` 指改动范围显著扩大，覆盖率可能下降、结果不确定性更高
- 不自动停止
- 终端明确提示覆盖率可能下降
- 终端明确提示结果不确定性可能更高
- 终端明确建议缩小 diff 范围

## 场景 13：branch diff 有实际 finding
- run 返回 `completed`
- `Review summary` 明确显示 `base..HEAD`，且当前分支名在可获取时应一并显示
- `Final findings` 列出通过验证的问题
- `Publish preview` 仍明确说明“发布已禁用：branch diff target 不支持 GitHub 发布”
- 不创建任何 GitHub 评论或写入操作

## 场景 14：干净 GitLab MR
- 终端模式返回 `completed`
- `Review summary` 明确显示 `gitlab_merge_request !<iid>`、标题和 URL
- `Final findings` 明确说明没有发现通过验证的问题
- 发布模式发一条 `No issues found` summary comment
- inline comment 数量为 0

## 场景 15：GitLab MR 有局部且明确的问题
- run 返回 `completed`
- 终端报告列出 finding
- 发布模式发一条 summary comment
- 当 `glab mr note create` 支持 inline 参数且定位可靠时，发布对应 inline comment
- 不能可靠定位时，finding 留在 summary comment

## 场景 16：GitLab MR 高层设计或边界问题
- run 返回 `completed`
- 终端报告列出 finding
- 发布模式只放 summary comment
- 不强行把问题挂到某一行做 inline comment

## 场景 17：`glab` 不存在、认证失败或读取失败
- run 返回 `failed`
- 终端明确说明失败原因
- 不发布任何 GitLab 评论
- 不得把失败误报成 `No issues found`

## 场景 18：GitLab MR closed / merged / draft / WIP / trivial / 重复发布
- run 返回 `skipped`
- `trivial` 指极小的机械性改动，几乎没有行为风险
- 终端明确说明具体跳过原因
- 只有“因为已发布过评论而再次请求发布”的情况，终端复跑才允许继续执行 review
- `closed` / `merged` / `draft` / `WIP` / `trivial` 不适用上述终端复跑例外

## 场景 19：GitLab MR inline 能力不可用
- run 返回 `completed`
- summary comment 仍可发布
- 不创建 inline comments
- `Publish preview` 写明 `inline skipped` 和能力探测失败原因

## 场景 20：GitLab MR inline 定位不可靠
- run 返回 `completed`
- summary comment 仍可发布
- 不创建 inline comments
- `Publish preview` 写明 `inline skipped` 和定位不可靠原因

## 场景 21：GitLab 发布失败
- run 保持 `completed`
- 终端保留 findings
- 单独报告 GitLab 发布失败原因
- 不得把发布失败误报成 `No issues found`

## 场景 22：大型 GitLab MR
- 在未触发其他失败条件时，run 返回 `completed`
- 不自动停止
- 终端明确提示覆盖率可能下降
- 终端明确提示结果不确定性可能更高
- 终端明确建议拆分 MR 会得到更好的 review 质量

## 场景 23：只提供可识别平台的 URL
- `https://github.com/org/repo/pull/123` 判定为 `github_pull_request`
- `https://gitlab.com/group/subgroup/repo/-/merge_requests/123` 判定为 `gitlab_merge_request`
- 自建域名按 path pattern 判定，不按域名猜平台

## 场景 24：URL 或裸编号无法判断平台
- run 返回 `failed`
- 终端明确说明无法从 URL 判断是 GitHub PR 还是 GitLab MR，或裸编号缺少平台语义
- 不得同时尝试 `gh` 和 `glab` 猜目标类型
- 不发布任何评论

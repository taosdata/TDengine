# 手工验收场景

## 场景 1：干净 MR（文档无问题）

- 终端模式返回 `completed`
- 检查约定段落 `Review summary`、`Final findings`、`Publish preview` 是否存在
- `Final findings` 明确说明没有发现通过验证的问题
- 发布模式只发一条 `No issues found` summary comment（含 marker `<!-- tsdb-review-doc:summary -->`）
- 不创建 inline comments

## 场景 2：局部且明确的文档问题

- run 返回 `completed`
- 终端报告列出 finding
- 发布模式发一条 summary comment
- 若问题可精确定位到文件行，发布模式发对应 inline comment
- 不为文档 finding 生成 committable suggestion

## 场景 3：全局性文档问题（无法精确定位到单行）

- run 返回 `completed`
- 终端报告列出 finding
- 发布模式只放 summary comment
- 不强行把问题挂到某一行做 inline comment

## 场景 4：`glab` 认证失败或读取失败

- run 返回 `failed`
- 终端明确说明失败原因
- 不发布任何 GitLab 评论
- 不得把失败误报成 `No issues found`

## 场景 5：MR closed / no doc changes / trivial

- run 返回 `skipped`
- 终端明确说明具体跳过原因：
  - `closed`：MR 已关闭
  - `no doc changes`：diff 中不含文档文件变更
  - `trivial`：变更极小，仅机械性格式调整
- `closed` / `no doc changes` / `trivial` 不适用终端复跑例外
- 只有"因为已发布过评论而再次请求发布"的情况，终端复跑才允许继续执行 review

## 场景 6：用户提供非 GitLab MR URL（如 GitHub PR URL）

- run 返回 `failed`
- 终端明确说明本 skill 只支持 GitLab MR URL
- 提示正确的 URL 格式示例：`https://git.tdengine.net/<namespace>/-/merge_requests/<id>`
- 不执行任何 review 逻辑

## 场景 7：大型 MR（文档改动量很大）

- 在未触发其他失败条件时，run 返回 `completed`
- 不自动停止
- 终端明确提示覆盖率可能下降
- 终端明确提示结果不确定性可能更高
- 终端明确建议拆小 MR 或将文档改动分批提交

## 场景 8：MR URL 格式无法解析

- run 返回 `failed`
- 终端明确说明 URL 解析失败，给出期望格式
- 不得自动猜测或补全 URL

## 场景 9：MR diff 包含文档文件和代码文件

- run 正常进入 review 流程（不因包含代码文件而拒绝）
- 只审查 diff 中的文档文件（`.md`、`.rst`、`.txt`、`.adoc`，以及 `docs/`、`documentation/`、`doc/` 路径下的文件）
- 可在必要时参考代码文件来验证文档准确性，但不对代码本身做代码 review
- `Review summary` 说明审查范围仅限文档文件，并列出被跳过的非文档文件数量

## 场景 10：发布模式下 MR 已有相同 marker 的 summary comment

- run 返回 `skipped`（仅针对发布操作，review 本身仍可在终端执行）
- 终端明确说明已发布过，并提示可在不发布模式下重新 review
- 不重复发布 summary comment

## 场景 11：inline comment 字段无法映射

- 不创建该条 inline comment
- 把对应 finding 保留在 summary comment 中
- 在终端说明该 finding 无法作为 inline comment 发布的原因
- run 状态不因此变为 `failed`

## 场景 12：review 完成但发布失败

- run 状态仍为 `completed`
- 保留终端结果
- 单独报告发布失败原因
- 不得把发布失败误报成 `No issues found`

## 场景 13：MR 中已有 unresolved 讨论覆盖了部分 finding

- run 返回 `completed`
- 被已有讨论覆盖的 finding 在终端和发布 comment 中均以 `[已有讨论 @<author>, 行 <line>]` 标签呈现
- 该标签的作用是告知其他 reviewer 该问题已有人跟进，而非重复提问
- 所有已验证 finding（含 `[已有讨论]`）均可进入发布候选，不再静默排除

## 场景 14：review 完成后提示发布

- run 返回 `completed` 后，agent 主动展示提示："Review 已完成，共发现 N 个问题。是否需要将结果发布为 MR comment？"
- 用户确认后才执行发布；用户拒绝则结束，不再追问
- run 状态为 `skipped` 或 `failed` 时不展示此提示

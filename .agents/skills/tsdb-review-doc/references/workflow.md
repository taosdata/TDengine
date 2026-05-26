# TSDB Doc Review 审查工作流

本文档定义 `tsdb-review-doc` 的主流程、固定 reviewer 分工、candidate issue schema、验证规则、状态流转与排序过滤策略。

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
10. [发布提示](#10-发布提示)

## 1. 目标解析

### GitLab MR URL

- 接受格式：`https://<host>/<namespace>/-/merge_requests/<id>`
  - 示例：`https://git.tdengine.net/rd-public/tsdb/-/merge_requests/151`
- 解析步骤：
  1. 提取 `host`（如 `git.tdengine.net`）
  2. 提取 `namespace`（如 `rd-public/tsdb`）
  3. 提取 MR 编号（如 `151`）
- 解析后获取 MR 元信息：

  ```bash
  GITLAB_HOST=<host> glab mr view <id> --repo <namespace> -F json
  ```

- 获取 MR diff：

  ```bash
  GITLAB_HOST=<host> glab mr diff <id> --repo <namespace> --color=never
  ```

- 无法解析 MR URL，或 `glab` 不可用时，当前 run 直接返回 `failed`。
- 仅支持 GitLab MR URL；若用户提供 GitHub PR URL 或本地路径，当前 run 直接返回 `failed` 并说明原因。

## 2. Eligibility Gate

满足以下任一条件时，当前 run 返回 `skipped`：

- MR 已关闭（`state: closed`）。
- MR diff 中不含任何文档文件变更（文档文件指 `.md`、`.rst`、`.txt`、`.adoc`，以及 `docs/`、`documentation/`、`doc/` 路径下的所有文件）。
- 变更过于 trivial：仅机械性格式调整（纯空白/缩进/换行），几乎没有内容风险。
- 用户要求发布到 GitLab，且本 skill 已经对同一 MR 发布过 summary 评论。

`trivial` 仅指极小的机械性变化，例如纯格式化、纯空白调整、无语义变更的自动生成文件更新。只要改动涉及新增或修改文本内容、代码示例、链接、章节结构，就不得按 `trivial` 跳过。

**重复发布检测（进入发布模式前必须执行）**：

```bash
# 获取全部 MR comments（不限 state，用于检测 marker）
GITLAB_HOST=<host> glab mr note list <id> --repo <namespace> -F json
```

在返回结果中搜索 `<!-- tsdb-review-doc:summary -->` marker。只有找到该 marker 时，才判定为已发布过；否则不得判定为重复发布。

即使之前已发布过评论，仍允许只在终端重新运行 review（不发布）。

## 3. 共享审查上下文

所有 reviewer 共享以下上下文：

- `target_type = gitlab_mr`
- `target_display`：MR 编号、标题、URL
- 文档 diff 与变更文件列表（仅文档文件）
- MR `title` 与 `description`
- MR 现有未解决讨论（**用于去重，见 §6**）：

  ```bash
  GITLAB_HOST=<host> glab mr note list <id> --repo <namespace> --state unresolved -F json
  ```

  解析后提取每条 note 的 `body`、`author.username`、`position.new_line`（如有），构建 `existing_discussions` 集合。

- 必要时读取仓库中相关文档文件的原始内容，以判断与现有文档的一致性

边界约束：

- 不运行 CI、build、lint、test。
- 不读取代码文件（除非需要验证文档中代码示例的准确性）。

## 4. 固定 reviewer 角色

### Change Summarizer

- 总结文档变更意图。
- 标记主要变更区域（新增章节、修改示例、更新配置说明等）。
- 标记风险热点（可能存在信息错误、覆盖不足、与代码不一致的区域）。

`Change Summarizer` 是全局前置步骤，失败时整个 run 标记为 `failed`。

### Accuracy Reviewer

**通用检查：**

- 技术描述是否与实际行为一致（必要时对照关联代码或已有文档）。
- 配置项、参数、返回值的描述是否准确，边界条件是否说明。
- 代码示例是否语法正确、逻辑合理、可以实际运行。
- 版本信息、API 版本是否与实际相符。

**FS（概要设计）专项：**

- §4 必须明确本功能适用版本（社区版 / 企业版 / 两者），不得模糊表述；若仅企业版支持，必须显式标注。
- §4 SQL 示例必须使用 TSDB 标准智能电表数据（`meters`/`readings` 等），不得使用自造数据。
- §4 API 接口若引入了新的接口或参数变更，必须有准确的参数说明。
- §4 新引入的配置参数必须说明：有效范围、默认值、适用范围（适用于哪些部署模式/平台）、是否支持动态修改（无需重启即可生效）。
- §3 新引入的概念或术语必须有明确定义。
- §5 性能影响须量化说明（即使是"negligible"也要给出理由），不接受无理由的"无"。

**DS（详细设计）专项：**

- §6 关键数据结构必须列出并描述字段含义；若有图表（消息序列图、状态转换图），应与实际设计一致。
- §7 接口规范中的 API 必须提供调用示例。

**TS（功能测试报告）专项：**

- §4 测试结论必须包含量化数据或明确通过/失败的说明，不得只写"已测试"。
- §6 用例表格中的测试结果须为真实结论，不得全部填写"通过"而无验证依据。
- §6 每条测试用例的"测试描述"必须清晰说明：测试的具体输入/操作、预期结果、判断依据；描述模糊（如"测试 XX 功能"）视为不合格。
- §9 性能测试场景描述必须明确：测试数据规模（表数、行数、并发数等）、测试方法、基准对比（与上一版本或预期目标的对比）、环境配置；缺少任一项均为 finding。
- §5 测试环境中列出的环境必须是实际执行测试的环境。

### Completeness Reviewer

**通用检查：**

- 新增功能是否遗漏重要使用场景或边界条件。
- 必要的前置条件、依赖项是否有说明。
- 错误情况、异常行为是否有覆盖。
- 与该功能相关的其他文档是否需要同步更新。

**FS（概要设计）专项：**

- §7 兼容性：若有 breaking change，必须明确列出并给出**必须这样做的理由**；即使"无"也要确认而非假设。
- §9 使用场景：use case 是否尽量穷举？典型场景、边界场景、异常场景是否覆盖？
- §4 若有 API 代码示例，文档中是否提及该示例需加入 CI 作为测试用例。
- §11 复杂功能必须有错误排查说明；简单功能写"无"时须简短说明理由。
- §14 是否明确说明需要同步更新企业版/官网文档。
- 全文是否在显著位置（标题、背景或行为说明开头）声明本功能适用于社区版还是企业版；若 RS §2.4 已有版本要求，FS 与之是否一致。

**DS（详细设计）专项：**

- §6 关键数据结构是否完整列出？是否有遗漏的核心模块？
- §8/§9 若标注"无"或"N/A"，必须有一句理由说明为何不适用。

**TS（功能测试报告）专项：**

- §6 每个功能点必须同时覆盖正向用例和负向用例（边界值、异常输入）。
- §11 兼容性测试：至少覆盖升级后旧数据可用、升级后可降级两个场景（或说明为何不适用）。

### Clarity Reviewer

**通用检查：**

- 语言是否清晰易懂，是否存在歧义或模糊表述。
- 段落结构、列表、标题层级是否有助于读者理解。
- 语法错误、拼写错误、断句问题（中英文混排时注意标点规范）。

**术语一致性（重点）：**

- 同一概念在文档中是否使用统一术语（如不得在同一文档中混用"写入/insert/写"来指代同一操作）。
- 新引入的术语是否与 TSDB 已有文档（其他 FS/DS/用户手册）保持一致；若需重新定义，必须在 §3（定义）中明确声明。
- 中英文术语混用时，首次出现应给出对照（如：超级表（STable））。

**内容与标题一致性：**

- 各节内容是否与其标题相符，是否有偏题内容放错了章节。

### Template Compliance Reviewer

**前置步骤**：读取 `references/templates.md`，识别文档类型并加载对应的必需章节列表。

重点检查：

- 根据文件名识别文档类型（RS / FS / DS / TS）；无法识别时在 finding 中说明并跳过本项检查。
- 逐一核对必需章节是否全部存在（标题编号与模板一致）。
- `§1 修订记录` 表格是否存在且有有效数据行（非全 `XXX` 占位符）。
- 必需章节（非可选）是否有实质性内容（不可只有标题而无正文）；可选章节（如 TS §7/§8/§12）标题必须存在，内容允许为"无"但不得整节缺失。
- 发现章节缺失时，列出所有缺失章节，统一作为一条 `category: template` finding 汇报。

`Template Compliance Reviewer` 失败不影响整体 run 状态，但缺失章节属于 `severity: high` finding。

### Format Reviewer

重点检查：

- Markdown 语法是否正确（标题、列表、代码块、链接格式）。
- 链接是否有效（相对链接路径是否正确、锚点是否存在）。
- 代码块是否指定了语言（如 ` ```sql`、` ```bash`）。
- 文件命名是否符合规范：建议格式为 `NN-feature-name-{rs|fs|ds|ts}.md`（`NN` 为两位数字序号）；不符合时作为 `severity: low` finding。
- 标题编号是否连续且与模板一致（不得跳号或重号）。
- 表格是否完整（列数一致、无缺失分隔符）。
- 格式风格是否与仓库中其他文档保持一致。

### Cross-Doc Consistency Reviewer

检查文档与其关联文档（RS/FS/DS/TS）之间的一致性：

- **引用完整性**：
  - FS §15 参考文档是否引用了对应 RS；DS §12 是否引用了对应 FS；TS §3 是否引用了对应 FS。
  - 引用路径/链接是否可达（相对路径是否正确）。
- **版本与 Edition 一致性**：
  - 若 RS 指定了版本要求（开源/企业版、发版版本号），FS 的版本信息是否与之一致。
- **MR 标题与文档标题**：
  - MR title 与文档 H1 标题是否一致或合理对应（不一致时作为 `severity: low` finding 提示）。
- **术语与定义一致性**：
  - 若 FS 中定义了新术语（§3 定义），DS 中使用该术语时是否与 FS 定义一致。

`Cross-Doc Consistency Reviewer` 只检查当前 MR 包含的文档与可获取的关联文档之间的一致性；若关联文档不在同一 MR 中且无法通过 `glab` 获取，则跳过对应检查项并在 finding 中注明。

## 5. candidate issues

每条 `candidate issue` 至少包含以下字段：

- `category`：问题类别（accuracy / completeness / clarity / format / template / cross-doc）
- `claim`：问题陈述
- `evidence`：具体证据（文件路径、行号、引用原文）
- `scope`：问题影响范围
- `why_target_related`：说明为什么问题由当前 MR 的文档变更引入或放大

缺少任一字段的 `candidate issue` 在验证前直接丢弃。

## 6. 验证与 finding

每条候选问题都必须执行以下验证：

- 真实性：问题是否客观成立（不是个人偏好）。
- 当前 MR 归因：是否由当前 MR 变更引入或放大（非历史遗留且与本次变更无关）。
- 若 issue 引用技术事实，则验证该事实是否可通过代码或已有文档佐证。

**已有讨论去重标记（必须执行）**：

在通过上述验证后，将每条 finding 与 `existing_discussions` 集合进行比对：

- 若 existing_discussions 中已有针对**同一问题点**（相同文件位置或相同核心论点）的 unresolved 讨论，则为该 finding 打上标签 `[已有讨论 @<author>, 行 <line>]`，并在 finding 描述中补充该标注（如 `[已有讨论 @wpan, 行 92]`）。
- `[已有讨论]` finding **仍进入发布候选**（`publishability` 保持 `summary`）；标记的目的是让其他 reviewer 一眼看出该问题已有人跟进，而非重复提问。
- 未被任何 existing discussion 覆盖的 finding 正常流转，不附加标签。

验证通过后，补充 finding 元数据：

- `confidence`：高 / 中 / 低
- `severity`：critical / high / medium / low（文档维度）
- `publishability`：summary | inline | not publishable

终端结果集合和 GitLab 可发布集合均包含全部已验证 findings（含 `[已有讨论]` 标记项）；`not publishable` 仅用于验证未通过或其他明确不适合发布的情况。

验证未通过的问题不得进入最终 findings。

## 7. 状态与降级

- review 成功完成并产出可信终端结果：run 状态为 `completed`。
- 命中 Eligibility Gate 的有意提前退出条件：run 状态为 `skipped`。
- `Change Summarizer` 失败：整个 run 标记为 `failed`。
- 其他 reviewer 失败：继续执行，但必须在终端明确说明覆盖范围下降。
- `candidate issue` 验证失败：不得进入最终 findings。
- review 已完成且终端结果可信，但发布失败：run 状态仍为 `completed`，同时保留终端结果并单独报告发布失败。

## 8. 排序与过滤

最终 findings 按以下优先级排序：

1. `severity`（高到低）
2. `confidence`（高到低）
3. 可行动性（越能直接指导修复越靠前）

必须过滤：

- 与当前 MR 文档变更没有实质关系的历史遗留问题。
- 纯主观偏好（例如"我更喜欢这种写法"）。
- 缺乏证据的问题。
- 无法明确关联到当前 MR 变更的担忧。

只有当不存在"通过验证的问题"时，才输出 `No issues found`。

## 9. 大型 review target

- 大型 review target 不自动终止。
- 必须提示覆盖率可能下降。
- 必须提示结果不确定性可能更高。
- 必须提示拆小 MR 或缩小 diff 范围会得到更好的 review 质量。
- 优先审查入口文档、API 说明、关键配置说明、代码示例、以及高 churn 文件。
- 对无法穷尽的低风险机械性改动允许抽样，但必须在 `Review summary` 说明抽样范围与未覆盖范围。
- 不得因为 target 很大而省略归因验证；归因或真实性不确定的问题不得进入 final findings。

## 10. 发布提示

review 完成（run 状态为 `completed`）并向用户展示终端报告后，**必须主动询问**：

> "Review 已完成，共发现 N 个问题。是否需要将结果发布为 MR comment？"

- 若用户确认发布：执行重复发布检测（§2），再按 `references/output-format.md` 的发布流程发布 summary comment。
- 若用户拒绝或无响应：保留终端结果，不发布，不再追问。
- 若 run 状态为 `skipped` 或 `failed`：不展示此提示。

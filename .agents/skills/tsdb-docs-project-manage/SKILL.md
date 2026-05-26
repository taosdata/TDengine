# Skill: tsdb-docs-project-manage

## 触发条件

当用户需要更新 TSDB 项目管理文档时使用，包括：
- 更新项目进度跟踪表
- 更新项目变更跟踪表
- 编写项目进度跟踪会议记录
- 编写项目变更评审记录
- 基于 Excel 导出数据更新工作列表

触发关键词：项目进度, 项目变更, 进度跟踪, 变更跟踪, 项目管理文档, update progress, update changes

## 输入文件

### Excel 数据源

1. **engine.xlsx** — 引擎侧工作项导出
   - 来源：飞书项目 Feature 导出
   - 关键列：工作项id, 名称, 状态, 当前负责人, 分类标签, 优先级, Reporter, Feature链接
   - 分类标签筛选规则：
     - `TSDB-FromTX` → 更新 "3.2 业务" 章节
     - `TSDB-FromIDMP` → 更新 "3.3 IDMP" 章节
     - `TSDB-Plan` → 更新 "3.4 规划" 章节
     - `TSDB-FromNA` → 更新 "3.5 海外" 章节
   - 注意：标签可能有多个（如 `TSDB-FromTX；TSDB-KeyTask`），匹配时用 `in` 判断

2. **platform.xlsx** — 平台侧工作项导出
   - 来源：飞书项目 Feature 导出
   - 关键列同上（无 Feature链接 列单独，link 在第 12 列即 index 11）
   - 全部用于更新 "3.6 平台" 章节

### 项目文档

- `docs/releases/TSDB-v{version}/01-项目管理/TSDB v{version} 项目进度跟踪表.md`
- `docs/releases/TSDB-v{version}/01-项目管理/TSDB v{version} 项目变更跟踪表.md`

### 参考模版（会议记录）

- `docs/releases/TSDB-v{prev_version}/09-会议纪要和评审记录/01-项目管理/YYYYMMDD 项目变更评审记录.md`
- `docs/releases/TSDB-v{prev_version}/09-会议纪要和评审记录/01-项目管理/YYYYMMDD 项目进度跟踪会议记录.md`

## 工作流程

### Step 1: 读取 Excel 数据

```python
import openpyxl

wb = openpyxl.load_workbook('engine.xlsx')
ws = wb.active
# Headers: 工作项id(0), 名称(1), 状态(2), 当前负责人(3), 分类标签(4), 优先级(5), 
#          到期日(6), 迭代周期(7), 迭代周期ID(8), Owner(9), Reporter(10), 实际发版日(11), Feature链接(12)
```

### Step 2: 分类与对比

1. 从现有进度跟踪表中提取每个章节的已有工作项 ID 集合
2. 对比 Excel 中的最新 ID 集合：
   - **新增**：Excel 中存在但进度跟踪表中不存在的 ID
   - **移出**：进度跟踪表中存在但 Excel 中不存在的 ID
   - **更新**：两边都有但状态可能已变化的 ID（取 Excel 最新状态）

### Step 3: 更新项目进度跟踪表

1. **修订记录**：新增一行，日期为当天
2. **项目进度概览**：
   - 统计各模块 total/done/remaining
   - 统计各模块 added/removed
   - 主要风险保持不变（除非用户指示更新）
3. **3.1 亮点功能**：不更新（除非用户指示）
4. **3.2~3.6 工作列表**：
   - 按 Excel 数据完全替换表格内容
   - 新增项"说明"列标注"新增"
   - 移出项追加到表格末尾，"说明"列标注"删除"，**必须保留完整信息**（名称、优先级、报告人、链接），从 git 历史或上次跟踪数据中获取原始值，不可用占位符（如 `（已移出）| - | - | - |`）
   - 若移出原因为分类调整（如从规划移至IDMP），在"说明"列标注"删除，移至IDMP"
5. **风险管理表**：不更新
6. **月度总结**：填写最近一个空白月度总结

### Step 4: 更新项目变更跟踪表

1. **修订记录**：新增一行
2. 新增一个 `## N. YYYYMMDD 工作范围变更` 章节，包含：
   - 变更描述（原因、类型）
   - 变更内容（新增工作项列表、移出工作项列表，按模块分组）
   - 移出工作项列表中**必须列出实际名称**，格式为 `名称（工作项ID）`，不可只写 ID
   - 若移出原因为分类调整，在名称后标注（如 `移至IDMP`）
   - 变更影响分析

### Step 5: 编写会议记录

1. **项目变更评审记录**：
   - 评审文档链接指向变更跟踪表（相对路径）
   - 评审记录简述变更原因和影响
   - 评审结论（通常为"通过"）
   - 后续行动项

2. **项目进度跟踪会议记录**：
   - 会议议定事项（新增/移除功能、重点关注事项）
   - 会议未定事项

## 表格格式规范

```markdown
| 工作项ID | 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- | --- |
| 6925549512 | [售前][中石油] 支持不限制国产操作系统和 CPU 的社区版 | P3 | Guang Li | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6925549512?node=27777982) |
```

- **工作项ID**：飞书项目的工作项 ID
- **名称**：直接使用 Excel 中的名称
- **优先级**：P1/P2/P3/P4/P5
- **报告人**：使用 Excel 中的 Reporter 列
- **状态**：New/Processing/Reviewing/Testing/Releasing/Verifying/Done/Canceled/Blocked/Backlog
- **说明**：新增项标注"新增"，移出项标注"删除"，其他为空
- **链接**：`[链接](Feature链接URL)`

## 状态统计规则

- **已完成**：status == 'Done'
- **未完成**：total - done
- **完成百分比**：显示未完成占比 = (total - done) / total * 100%

## 文件路径约定

- 进度跟踪表：`docs/releases/TSDB-v{version}/01-项目管理/TSDB v{version} 项目进度跟踪表.md`
- 变更跟踪表：`docs/releases/TSDB-v{version}/01-项目管理/TSDB v{version} 项目变更跟踪表.md`
- 评审记录：`docs/releases/TSDB-v{version}/09-会议纪要和评审记录/01-项目管理/YYYYMMDD 项目变更评审记录.md`
- 会议记录：`docs/releases/TSDB-v{version}/09-会议纪要和评审记录/01-项目管理/YYYYMMDD 项目进度跟踪会议记录.md`

## 注意事项

1. 建议使用 Python 脚本 + openpyxl 读取 Excel，避免在命令行中处理 f-string 转义问题
2. 先生成中间文件（_gen_*.md），再组装最终文档，便于调试
3. 超链接使用相对路径（`../../01-项目管理/...`）指向同版本目录下的文档
4. 月度总结中的"项目主要成果"需要根据各模块状态变化（与上次跟踪对比）据实描述
5. 清理临时生成文件（_gen_*.md, _gen_changes.json, _*.py）
6. **移出项必须保留完整信息**：通过 `git show <commit>:<file>` 从 git 历史获取移出项的原始名称、优先级、报告人和链接，不可使用 `（已移出）` 等占位符
7. 获取 git 历史中文内容时需先设置 `[Console]::OutputEncoding = [System.Text.Encoding]::UTF8`，否则中文显示为乱码

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-git-commit version=0.2.0 author=Simon Guan`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

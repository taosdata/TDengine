# tsdb-dev-external-window

围绕 TDengine `external_window` 的语义、规划链路与执行器实现，提供面向问题定位、代码修改与回归补测的分析指引。

## 概述

本技能面向 TDengine 内核开发、问题排查与回归修复场景，核心目标是帮助使用者快速判断 `external_window` 问题属于语义约束、解析器、规划器、执行器还是测试预期层，并给出稳定的排查顺序与关键代码落点。

它特别适用于以下几类工作：

- 理解 `external_window(...)` 的实际语义与限制
- 排查结果错行、漏行、重复行、分组串扰、窗口边界错误、排序异常
- 修改 parser / planner / executor 中与 `external_window` 相关的实现
- 为已有缺陷补充最小回归用例，或分析现有回归失败原因

## 触发场景

- 用户要求解释 `external_window` 的语义、约束条件或调用链
- 用户反馈 `external_window` 查询结果不正确，涉及 `_wstart`、`_wend`、`w.xxx` 等窗口伴生列
- 用户需要分析 `PARTITION BY`、`GROUP BY`、`INTERVAL` 与 `external_window` 组合后的行为
- 用户需要定位 `calcWithPartition`、`extWinSplit`、多分组输出、嵌套 external window、动态窗口等实现问题
- 用户要修改 `externalwindowoperator.c` 或相关 planner / parser 文件，并补充回归覆盖

## 分析结构（5 个阶段）

1. **语义约束确认**
   - 先核对文档、语法与语义限制，判断 SQL 是否满足 `external_window` 前提条件。
2. **解析层检查**
   - 检查 `sql.y` 与 `parTranslater.c`，确认子查询 schema、窗口语法、非法组合是否被正确拦截。
3. **规划层检查**
   - 检查逻辑计划、物理计划与 split 策略，重点确认 `calcWithPartition`、`extWinSplit`、排序与分组语义是否丢失。
4. **执行层检查**
   - 从 `externalwindowoperator.c` 主路径出发，区分 scalar / agg / indefinite rows 三种执行模式，分析窗口构造、分组切换、窗口匹配与结果输出。
5. **回归验证**
   - 回到 `test_external.py` 与对应 `.ans` 文件，判断是实现回归还是预期结果需要更新，并补齐最接近问题的用例簇。

## 核心关注点

- **固定排查顺序**：必须按“文档/语义 -> parser -> planner -> executor -> regression”推进，避免把上游约束问题误判为执行器缺陷。
- **分区问题优先看 group id 归一化**：出现分区丢失、分区串扰、嵌套 external window 分区异常时，应优先检查 `baseGId/groupId` 相关归一化逻辑。
- **边界与空窗口问题优先看窗口构造与输出补齐**：半开区间、空窗口补 `NULL`、过滤后窗口索引重建，都是常见错误来源。
- **不要只盯 `externalwindowoperator.c`**：很多问题根因在 parser 或 planner，尤其是语义约束、排序保证与 split 策略。

## 关键代码位置

以下路径默认相对于 TDinternal 仓库中的 `community/` 子目录；该目录承载 TDengine 主体代码，因此下面不再重复书写 `community/` 前缀。

- 语法入口：`source/libs/parser/inc/sql.y`
- 语义校验：`source/libs/parser/src/parTranslater.c`
- 逻辑规划：`source/libs/planner/src/planLogicCreater.c`
- 物理规划：`source/libs/planner/src/planPhysiCreater.c`
- 拆分策略：`source/libs/planner/src/planSpliter.c`
- 执行主路径：`source/libs/executor/src/externalwindowoperator.c`
- 流式执行：`source/libs/executor/src/streamexternalwindowoperator.c`
- 用户文档：`docs/zh/05-basic/03-query.md`
- 回归入口：`test/cases/13-TimeSeriesExt/08-ExternalWindow/test_external.py`

## 输出预期

使用本技能时，理想输出应至少覆盖以下内容：

1. 对 SQL 与语义限制的判断
2. 问题更可能属于 parser / planner / executor / regression 的哪一层
3. 最小可疑代码落点与建议修改点
4. 需要补充或更新的回归用例位置
5. 如用户要求实现分析，应给出完整调用链与关键状态流转说明

## 作者

- Author: xsren
- Team: taosd
- Version: 0.1.0
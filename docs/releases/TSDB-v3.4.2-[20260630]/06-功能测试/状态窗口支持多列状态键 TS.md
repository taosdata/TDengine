# 状态窗口支持多列状态键 - TS

# 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-24 | 2026-04-24 | 1.0 | Tony Zhang | 初始版本 |

# 2. 测试目标

本文档用于定义“状态窗口支持多列状态键”在当前分支上的测试范围、测试入口和验收标准。

- 验证 `STATE_WINDOW` 从单列扩展到多列状态键后的查询、计划和执行语义正确性。
- 验证单列历史写法与新子句写法的语法兼容性，确保不引入 parser/planner 回归。
- 验证多列状态键在 `NULL`、`EXTEND(...)`、`ZEROTH_STATE(...)`、表达式键、`PARTITION BY tbname`、虚拟表查询中的行为。
- 验证流式计算中多列状态键触发、通知载荷和历史回放语义一致。
- 验证跨版本升级/兼容场景下，旧格式 stream 与新实现共存时的兼容性边界。

# 3. 参考文档

- 功能规格文档：`tsdb/docs/releases/TSDB-v3.4.2-[20260630]/05-设计文档/state_window_supports_multi_columns-FS.md`
- 相关实现与测试入口：
  - `source/libs/parser/test/parSelectTest.cpp`
  - `source/libs/parser/test/parStreamTest.cpp`
  - `source/libs/planner/test/planStateTest.cpp`
  - `test/cases/13-TimeSeriesExt/04-StateWindow/test_state_window_multi_col.py`
  - `test/cases/13-TimeSeriesExt/04-StateWindow/test_state_window_null_regression.py`
  - `test/cases/18-StreamProcessing/05-Notify/test_stream_notify_state.py`
  - `test/cases/18-StreamProcessing/23-Compatibility/test_compatibility_cross_version.py`
  - `test/cases/18-StreamProcessing/23-Compatibility/test_new_stream_compatibility.py`
  - `test/cases/05-VirtualTables/test_vtable_plan_window_optimize.py`
  - `test/ci/cases.task`

# 4. 测试结论

基于当前分支的代码与测试变更，可以确认该需求已形成较完整的自动化测试闭环，测试面覆盖语法解析、计划生成、查询执行器、流式触发、状态通知、虚拟表和兼容性六个层次。

本次文档生成过程中未重新执行整套回归，因此本文结论是“测试设计与自动化覆盖已就绪”，不是一次新的测试执行报告。对于已落地的自动化用例，本文统一以“单元测试覆盖”“自动化覆盖”“已接入持续集成”或“非默认持续集成自动化覆盖”描述其状态。

# 5. 测试环境

- 操作系统：Linux
- 运行入口：
  - C++ 单元测试：语法解析、计划生成、执行器目录下测试二进制
  - Python/pytest：`test/ci/pytest.sh pytest ...`

# 6. 功能测试

## 6.1 SQL 语法与兼容性

### 6.1.1 测试要点

- 单列历史写法继续支持：`STATE_WINDOW(c1, 1)`、`STATE_WINDOW(c1, 1, 0)`。
- 单列关键字写法支持：`EXTEND(...)`、`ZEROTH_STATE(...)`、`NO_ZEROTH`。
- 多列状态键支持列引用与表达式键混合。
- 多列场景禁止把扩展模式或零状态位置参数继续塞回 `STATE_WINDOW(...)` 圆括号内。
- 重复列、非法 tag 列、常量表达式、混用关键字/旧写法等错误路径必须报错。

### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 单列关键字写法解析 | `parStreamTest.stateWindowKeywordSyntax` 校验 `EXTEND` / `ZEROTH_STATE` / `NO_ZEROTH` 解析成功 | 单元测试覆盖 |
| 2 | 单列历史写法兼容 | `parStreamTest.stateWindowKeywordSyntax` 校验 `STATE_WINDOW(c1, 1)` 与 `STATE_WINDOW(c1, 1, 0)` 继续可用 | 单元测试覆盖 |
| 3 | 关键字与旧写法混用报错 | `state_window(c1, 1) extend(1)` 返回 `TSDB_CODE_PAR_INVALID_STATE_WIN_COL` | 单元测试覆盖 |
| 4 | 多列状态键查询语法 | `parSelectTest.selectFunc` 校验 `STATE_WINDOW(c1, CASE WHEN ... END)` 等多列表达式语法 | 单元测试覆盖 |
| 5 | 常量、标签列、重复列非法输入 | `parSelectTest.selectFunc` 校验常量、标签列、重复列、歧义写法报错 | 单元测试覆盖 |

## 6.2 计划生成与计划重写

### 6.2.1 测试要点

- 多列表达式键可进入逻辑计划与物理计划。
- `SELECT state_key_cols, agg(*) ... STATE_WINDOW(...)` 的 `GROUP_KEY` 重写正确。
- stable/ntb 上的 state window 计划构造与历史行为兼容。
- 虚拟表场景下，state window 相关优化、列替换和结果基线同步更新。

### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 多列表达式键计划构造 | `planStateTest.stateExpr` 覆盖 `CASE WHEN + 列`、`表达式 + 表达式` 等组合 | 单元测试覆盖 |
| 2 | 单列兼容计划 | `planStateTest.basic` 覆盖 `STATE_WINDOW(c1)`、`STATE_WINDOW(c1,1)`、`STATE_WINDOW(c1,1,0)` | 单元测试覆盖 |
| 3 | 聚合与状态窗口联合计划 | `planStateTest.selectFunc` 校验聚合列与状态列共存时的计划生成 | 单元测试覆盖 |
| 4 | 虚拟表窗口计划基线 | `test_vtable_plan_window_optimize.py` 及 `05-VirtualTables/in|ans/test_*_window_state*` 基线同步更新 | 基线覆盖 |
| 5 | 虚拟表相关替换/裁剪优化 | `test_vtable_plan_eliminate_virtual_scan`、`test_vtable_plan_replace_tbname` 的 state window 基线回归 | 基线覆盖 |

## 6.3 多列状态窗口查询能力

### 6.3.1 测试要点

- 支持两列和三列状态键。
- 支持 `int + bool`、`int + varchar`、`int + bool + varchar` 等多类型组合。
- 支持 `_wstart/_wend/_wduration`、`count/sum/avg/min/max/first/last` 等聚合与伪列组合。
- 支持 `PARTITION BY tbname` 与 `GROUP_KEY` 改写后的返回列一致性。
- 支持表达式键参与查询与流式计算。

### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 基础双列切窗 | `test_state_window_multi_col.py` 中“基础双列查询”场景校验 `STATE_WINDOW(c_int, c_bool)` 的连续片段划分 | 自动化覆盖，已接入持续集成 |
| 2 | 多类型组合 | “整数加字符串双列组合”和“三列状态键组合”场景校验 `int + varchar`、三列状态键切窗结果 | 自动化覆盖，已接入持续集成 |
| 3 | 聚合与伪列 | “多列聚合函数”和“窗口伪列输出”场景校验窗口聚合、`_wstart/_wend/_wduration` 输出 | 自动化覆盖，已接入持续集成 |
| 4 | 分区与列重写 | “按表名分区”和“分组键重写”场景校验 `PARTITION BY tbname`、`GROUP_KEY` 改写 | 自动化覆盖，已接入持续集成 |
| 5 | 表达式状态键 | “表达式加普通列”“双表达式状态键”“普通列加表达式”场景校验表达式键行为 | 自动化覆盖，已接入持续集成 |
| 6 | 单列回归兼容 | “单列状态窗口向后兼容”场景校验历史单列路径不回归 | 自动化覆盖，已接入持续集成 |

## 6.4 空值、边界扩展与零状态语义

### 6.4.1 测试要点

- 多列场景按列独立维护“上一有效状态键”，部分 `NULL` 与全 `NULL` 路径分离。
- 候选段在 `EXTEND(0/1/2)` 下的前兼容、后兼容、独立成窗决策正确。
- `ZEROTH_STATE(...)` 与 `NO_ZEROTH` 过滤逻辑正确，未定义列不得误判为零状态。
- 查询、实时流、历史回放三条路径对同一批数据得出一致窗口边界。

### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 多列空值基线 | `test_state_window_multi_col.py` 中“多列空值处理”场景覆盖单列为空、双列全空场景 | 自动化覆盖，已接入持续集成 |
| 2 | 三种边界扩展模式 | “边界扩展模式 0、1、2”场景覆盖 `EXTEND(0/1/2)` 下的归属差异 | 自动化覆盖，已接入持续集成 |
| 3 | 多列零状态过滤 | “所有列都配置零状态值”场景校验过滤行为 | 自动化覆盖，已接入持续集成 |
| 4 | 部分空值回归基线 | `test_state_window_null_regression.py` 的 `do_query_partial_null_baseline` 覆盖按列独立比较 | 自动化覆盖，已接入持续集成 |
| 5 | 三类历史回归问题 | `test_state_window_null_regression.py` 覆盖旧状态污染、未定义列误判零状态、待定状态污染等历史问题 | 自动化覆盖，已接入持续集成 |
| 6 | 查询、实时、历史三路径一致性 | 同一文件中查询、实时流、历史回放三组场景交叉验证 `EXTEND` 与延迟部分空值语义 | 自动化覆盖，已接入持续集成 |
| 7 | 前后兼容边界候选段 | 仅前兼容、双侧兼容、内部全空等边界候选段单独覆盖 | 自动化覆盖，已接入持续集成 |

## 6.5 流式触发与状态通知

### 6.5.1 测试要点

- 多列状态键可用于流式触发，并正确产出窗口结果。
- 实时与 `fill_history` 历史补算路径在多列状态键下保持一致。
- 状态通知中的 `prevState` / `curState` / `nextState` 载荷从单值升级为 JSON 数组，宽度与状态键列数一致。
- 首个窗口打开时 `prevState` 为 JSON null，而不是空数组或脏数据。

### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 多列表达式流式触发与历史回放 | `test_state_window_multi_col.py` 中“流式表达式状态键触发”和“多列状态键历史触发”场景覆盖表达式键与历史场景 | 自动化覆盖，已接入持续集成 |
| 2 | 多列状态通知载荷 | `test_stream_notify_state.py` 的 `verify_multi_state_events` 校验 `WINDOW_OPEN/CLOSE` 事件载荷为二维状态数组 | 自动化覆盖，非默认持续集成 |
| 3 | 单列状态通知兼容 | 同文件 `verify_single_state_events` 校验单列路径仍返回长度为 1 的数组 | 自动化覆盖，非默认持续集成 |
| 4 | JSON null 语义 | 首次 `WINDOW_OPEN` 的 `prevState` 必须是 JSON null | 自动化覆盖，非默认 CI |
| 5 | 事件序列正确性 | 校验 `[1, True] -> [1, False] -> [2, False] -> [2, True]` 等窗口开闭事件链 | 自动化覆盖，非默认 CI |

## 6.6 虚拟表与查询回归

### 6.6.1 测试要点

- 虚拟普通表、虚拟子表、虚拟超级表下的 state window 查询结果与计划输出正确。
- 新增 state window 多列语义后，不影响既有虚拟表优化路径。
- `.in/.ans` 基线覆盖三种模式输出，防止计划生成或执行器细节改动造成回归。

### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | vtable state window 结果基线 | `05-VirtualTables/ans/test_vtable_select_test_state_mode_{0,1,2}.ans` 对应输入文件基线更新 | 基线覆盖 |
| 2 | vstable state window 结果基线 | `test_vstable_select_test_state_mode_{0,1,2}` 校验超级表路径 | 基线覆盖 |
| 3 | vctable state window 结果基线 | `test_vctable_select_test_state*` 校验子表/组合路径 | 基线覆盖 |
| 4 | 窗口计划基线 | `test_vtable_plan_test_window_state`、`test_vstable_plan_test_window_state`、`test_vctable_plan_test_window_state` | 基线覆盖 |

# 7. 长期稳定性测试（可选）

当前分支未看到专门的 soak / chaos / 长稳脚本，本次不单列长期稳定性结论。若后续需要发版级别验证，建议补充以下观察项：

- 多列状态键流式任务在长时间写入下的状态通知积压、内存占用与窗口闭合延迟。
- `fill_history` 与实时混合写入时的结果一致性。
- 高频部分 `NULL` 候选段输入下的窗口数量与输出行数稳定性。

# 8. 性能测试

本次提交范围未包含专门的性能 benchmark 或性能门禁。当前 TS 仅要求确认多列状态键不会导致明显功能退化，性能专项不在本轮验收范围内。

# 9. 安全测试

本需求不引入新的权限模型、认证路径或敏感信息持久化逻辑。安全相关关注点主要是：

- 状态通知载荷结构变化不能破坏已有消费方 JSON 解析约定；
- 非法 SQL 组合必须在 parser 阶段稳定报错，避免进入执行层产生未定义行为。

# 10. 兼容性测试

### 10.1 测试要点

- 新版本对旧版本流式元数据、旧格式状态窗口任务和企业版升级路径的兼容边界清晰。
- 升级后旧任务可继续校验结果，或按预期识别为不兼容并阻止启动。
- 3.3.x 与 3.4.1.0 等基线版本上的流式任务与状态窗口能力能在当前版本完成验证。

### 10.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 升级与回退流式兼容 | `test_new_stream_compatibility.py` 覆盖 `3.3.7.9`、`3.3.8.5`、`3.3.8.6`、`3.4.1.0` 到当前版本的升级验证 | 自动化覆盖，非默认持续集成 |
| 2 | 跨版本流式任务与 TSMA 兼容 | `test_compatibility_cross_version.py` 覆盖 `3.3.3.0`~`3.3.6.0` 的旧格式流式任务与 TSMA 行为 | 自动化覆盖，非默认持续集成 |
| 3 | 持续集成注册情况 | `test/ci/cases.task` 已挂载 `test_state_window_multi_col.py`、`test_state_window_null_regression.py`；兼容性与状态通知用例保留为非默认持续集成入口 | 已接入 |

# 11. 已知问题和限制（可选）

- 本文档基于代码与测试变更生成，未在生成过程中重新跑完整自动化回归，因此不包含执行时长、通过率和失败归因。
- 状态通知与兼容性用例依赖外部环境（状态通知服务、企业版安装包、旧版本二进制），默认不适合在轻量持续集成中强制执行。
- 性能与长期稳定性尚未形成专项门禁，如需发版风险收敛，建议单独补性能和 soak 测试报告。
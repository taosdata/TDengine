# Session-Level Timezone 变更审查报告

## Review summary

- Status: completed (fixes applied)
- Target: working_tree [unstaged]
- Coverage: Change Summarizer ✓, Broad Scanner ✓, Security Reviewer ✓, Performance Reviewer ✓, Maintainability Reviewer ✓, Rule Reviewer ✓

### Change Summary

本次变更为 TDengine 引入 session-level timezone 和 firstDayOfWeek 机制，涉及 66 个文件、约 1800 行改动。核心意图：

1. 新 SQL：SET TIMEZONE '<tz>'、SET FIRST_DAY_OF_WEEK <0..6>，client-local 执行。
2. L2 session timezone：连接创建时从 tsTimezoneStr (L3) 快照，后续 ALTER LOCAL 仅改 L3，L2 独立。
3. IANA timezone 全链路：timezone IANA 名从 client 到 planner 到 physi plan 到 executor 到 scalar，替代此前仅固定偏移的做法，支持 DST-aware 计算。
4. TIMETRUNCATE 扩展：支持 1n/1q/1y 自然日历单位截断；1w 按 firstDayOfWeek 对齐而非固定周四。
5. TO_CHAR/TO_ISO8601 扩展：可接受 IANA timezone 参数，支持 DST-aware 输出。
6. TIMEZONE() 函数：文档新增 TIMEZONE(1) 语义。
7. Shell 改造：timestamp 展示使用连接级 timezone。

## Final findings

1. [P1/High] 正确性/并发: taos_get_conn_tz 返回的 timezone_t 在 releaseTscObj 后存在生命周期约束
- Category: correctness, concurrency
- Problem: taos_get_conn_tz 获取 pObj->optionInfo.timezone 后立即 releaseTscObj。若并发执行 SET TIMEZONE，调用方缓存该指针可能读取到过期语义。
- Evidence: [source/client/src/clientMain.c](source/client/src/clientMain.c#L309)
- why_change_related: 本次变更新增 taos_get_conn_tz API。
- Fix direction: 在 taos.h 增加更严格的生命周期约束文档，或改为受控句柄/拷贝语义，避免跨命令边界缓存。
- Publishability: summary
- Confidence: Medium

2. [P1/High] 可维护性: normalizeOffsetTz 与 normalizeOffsetTzCommon 逻辑重复
- Category: maintainability
- Problem: clientMain.c 与 ttime.c 各自维护一份固定偏移时区归一化逻辑，后续演进易出现不一致。
- Evidence: [source/client/src/clientMain.c](source/client/src/clientMain.c#L123), [source/common/src/ttime.c](source/common/src/ttime.c#L192)
- why_change_related: 本次变更新增两处几乎等价实现。
- Fix direction: 抽象单一实现并复用，避免双点维护。
- Publishability: inline
- Confidence: High

3. [P1/High] 可维护性/一致性: setConnectionTz 与 taosValidateTimezone 验证规则重复
- Category: maintainability, correctness
- Problem: 两处独立实现了相似的时区合法性规则（歧义缩写、IANA/offset 约束），未来一处变更易导致行为分叉。
- Evidence: [source/client/src/clientMain.c](source/client/src/clientMain.c#L190), [source/common/src/ttime.c](source/common/src/ttime.c#L240)
- why_change_related: 本次变更在两处分别加入了完整校验流程。
- Fix direction: 统一以 taosValidateTimezone 为入口，setConnectionTz 只负责缓存与映射。
- Publishability: inline
- Confidence: High

4. [P1/High] 架构韧性: TIMETRUNCATE 参数布局依赖 inputNum 的隐式约定
- Category: correctness, maintainability
- Problem: timeTruncateFunction 用 inputNum 和 pInput[2] 类型推导参数布局；若 translator 注入顺序变化，索引逻辑容易失效。
- Evidence: [source/libs/scalar/src/sclfunc.c](source/libs/scalar/src/sclfunc.c#L4431)
- why_change_related: 本次变更把 TIMETRUNCATE 注入参数扩展到 precision/tz/fdow/unitCh 四项。
- Fix direction: 引入显式布局标识参数，或统一尾部固定参数顺序并在 scalar 层做边界断言。
- Publishability: summary
- Confidence: High

5. [P2/Medium] 可维护性: timeTruncateFunction 复杂度显著上升
- Category: maintainability
- Problem: timeTruncateFunction 引入多分支路径（calendar/explicit tz/connection tz/week/day/fallback），函数体过长且维护成本高。
- Evidence: [source/libs/scalar/src/sclfunc.c](source/libs/scalar/src/sclfunc.c#L4418)
- why_change_related: 本次变更把 calendar 截断、DST 感知和 firstDayOfWeek 逻辑集中在一个函数内。
- Fix direction: 拆分为若干 helper（calendar truncation、week truncation、day truncation、offset truncation），主函数仅保留分发。
- Publishability: summary
- Confidence: High

6. [P2/Medium] 防御性不足: timeZoneIdx 默认 -1 缺少保护性校验
- Category: code-quality
- Problem: timeZoneIdx 初始化为 -1，当前路径虽不触发越界，但未来分支调整后存在误用风险。
- Evidence: [source/libs/scalar/src/sclfunc.c](source/libs/scalar/src/sclfunc.c#L4431), [source/libs/scalar/src/sclfunc.c](source/libs/scalar/src/sclfunc.c#L4476)
- why_change_related: 本次变更引入了多布局索引访问。
- Fix direction: 在 extractTimezoneParamString 调用前增加 idx>=0 断言或显式错误返回。
- Publishability: inline
- Confidence: Medium

7. [P2/Medium] 资源清理一致性: early return 与统一 cleanup 风格混用
- Category: reliability
- Problem: timeTruncateFunction 在部分分支直接 return，部分分支走 _return 清理路径；当前未见确定泄漏，但模式不统一增加后续回归概率。
- Evidence: [source/libs/scalar/src/sclfunc.c](source/libs/scalar/src/sclfunc.c#L4448), [source/libs/scalar/src/sclfunc.c](source/libs/scalar/src/sclfunc.c#L4695)
- why_change_related: 本次变更新增 explicitTz/fallbackTz 等资源管理。
- Fix direction: 将错误出口统一改为 goto _return，确保后续维护不会遗漏释放。
- Publishability: inline
- Confidence: Medium

8. [P2/Medium] 语义清晰度: TO_ISO8601 对 IANA 与固定偏移路径行为差异需显式说明
- Category: docs-consistency
- Problem: IANA 路径 DST-aware，固定偏移路径不感知 DST；行为合理但易引发用户误解。
- Evidence: [source/libs/scalar/src/sclfunc.c](source/libs/scalar/src/sclfunc.c#L4010), [source/libs/scalar/src/sclfunc.c](source/libs/scalar/src/sclfunc.c#L4088)
- why_change_related: 本次变更把默认注入改为 IANA 名，并保留用户显式 offset 路径。
- Fix direction: 在函数文档与测试中补充“显式 offset 与 IANA 时区语义差异”说明。
- Publishability: summary
- Confidence: Medium

9. [P1/High] 实现与文档一致性风险: 文档宣称 TIMEZONE([0|1])，实现侧参数签名需复核
- Category: correctness, docs-consistency
- Problem: 文档更新为 TIMEZONE([0|1]) 语义；需确认 builtins 与执行层已完全支持参数化分支，避免文档先行导致行为不一致。
- Evidence: [docs/zh/14-reference/03-taos-sql/22-function.md](docs/zh/14-reference/03-taos-sql/22-function.md#L2149), [source/libs/function/src/builtins.c](source/libs/function/src/builtins.c#L4805)
- why_change_related: 本次变更同时触达文档与函数定义。
- Fix direction: 对 TIMEZONE(0/1) 补充 parser+function+scalar+回归测试的端到端用例，确保文档与实现完全一致。
- Publishability: inline
- Confidence: Medium

## Publish preview

- Summary comment: disabled
- Inline comments: disabled

## 建议优先级

1. ~~优先去重时区解析与校验逻辑（Finding 2 + 3）。~~ ✅ 已修复
2. ~~复核并锁定 TIMEZONE([0|1]) 的实现闭环（Finding 9）。~~ ✅ 文档已回退至实现（TIMEZONE() 无参数）
3. ~~拆分 timeTruncateFunction 降低后续回归风险（Finding 4+5+6+7）。~~ ✅ 已拆分为 4 个 helper

## 修复记录

| Finding | 状态 | 修复方式 |
|---------|------|----------|
| 1 | Dismissed | timezone_t 由 pTimezoneMap 缓存，进程生命周期内不释放，无竞态 |
| 2+3 | ✅ Fixed | clientMain.c: 移除 ~80 行重复 normalizeOffsetTz + inline 校验；setConnectionTz 现调用 taosValidateTimezone 统一入口 |
| 4 | ✅ Fixed | 参数布局增加 timeZoneIdx >= 0 防御校验 |
| 5 | ✅ Fixed | 提取 timeTruncTicksToSec / truncateCalendarUnit / truncateWeekUnit / truncateDayUnit 四个 helper |
| 6 | ✅ Fixed | extractTimezoneParamString 前增加 idx < 0 错误返回 |
| 7 | ✅ Fixed | 所有 early return 统一改为 goto _return |
| 8 | ✅ Fixed | toISO8601Function 顶部注释补充 IANA vs fixed-offset 语义差异说明 |
| 9 | ✅ Fixed | 中英文文档回退为 TIMEZONE()（无参数），与 builtins.c maxParamNum=0 一致 |

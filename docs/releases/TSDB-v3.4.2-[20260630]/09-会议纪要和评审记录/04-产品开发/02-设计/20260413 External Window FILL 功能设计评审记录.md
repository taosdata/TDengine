# 20260413 External Window FILL 设计评审记录

## 1. 评审信息

1. 评审目的：评估 "External Window FILL FS" 设计的合理性、安全性、性能、兼容性及可维护性
2. 评审文档：[External Window FILL FS](../../../05-设计文档/External%20Window%20FILL%20FS.md)
3. 会议主持：关胜亮
4. 会议人员：关胜亮、霍琳贺、潘魏、任新胜
5. 会议时间：2026-04-13 16:30 - 16:40
6. 会议形式：线下
7. 会议地点：taosX
8. 会议记录人：关胜亮

## 2. 评审记录

评审团队对设计文档（External Window FILL FS）进行了全面审查，认为整体设计贴合 external_window 与 interval fill 窗口级语义对齐需求、逻辑严谨、可落地性强，具体评审意见如下：
1. 设计目标清晰精准，核心痛点定位明确，紧扣当前 external_window 在聚合模式下已具备空窗口计算能力但 FILL 能力分别在语法层、语义层和计划层被阻断、默认行为等价于 FILL(NONE) 的实际痛点，明确核心目标为让 external_window 与 interval fill 在窗口级语义上具备可比较、可演进的能力，支持范围（NONE/NULL/NULL_F/VALUE/VALUE_F/PREV/NEXT）和不支持范围（LINEAR/NEAR/SURROUND）界定清晰合理，LINEAR 不支持的原因（external_window 窗口不等宽不等距，依赖规则时间轴的 LINEAR 语义不适用）论证充分。
2. 功能设计全面细致，可落地性强，覆盖核心业务场景：各 FILL 模式行为定义完整，通过与 INTERVAL FILL 的系统化对比表明确语义对齐关系和关键差异；forced/non-forced 语义仅在"当前窗口集合所有窗口均空"时产生差异的规则定义精准，避免了边界模糊；伪列处理（_wstart/_wend/_wduration）和窗口属性列（w.xxx）在填充行上的取值来源明确不受 FILL 模式影响；PARTITION BY 交互规则清晰（partition 内独立执行、PREV/NEXT 不跨 partition 边界、完全缺席分组不额外物化）；边界场景覆盖全面（全空/部分空/全有数据/首末空窗口/连续空窗口/非聚合报错等 11 种场景）；各层改动清单（Parser/Translator/Planner/Executor/Test）任务拆分到位，PREV/NEXT 实现策略分析（两遍扫描 vs 延迟输出）有理有据，推荐方案利用已预构建的窗口数组，成本可控。
3. 设计文档结构规范，版本与修订记录清晰：明确修订记录（0.1 版本 xsren），术语定义简洁准确（空窗口、external_window FILL、forced/non-forced），16 个章节从背景、定义、行为说明、性能、兼容性到使用场景、约束限制、常见错误排查、可观测性、附录（当前现状、实现路径分析、测试覆盖规划、开发任务拆分、风险与回滚策略、测试矩阵与验收标准）分层呈现，特别是附录中对当前三层禁用方式的代码定位（sql.y L2751、parTranslater.c L9669、planLogicCreater.c L2947）精确到行号，便于开发快速定位改动点，符合 TDengine 设计文档规范要求。
4. 安全性、兼容性与性能考虑周全，风险可控：安全不涉及额外开发，因功能仅影响查询结果不落盘无数据格式升级风险；兼容性重点关注空窗口"参与计算但默认不输出"行为变更后老查询结果集行数可能变化的风险，明确默认路径（不开启 FILL）行为不变、历史用例 100% 通过的兼容性目标，并设计了 A/B 比较差异审计；性能设计推荐在 ExternalWindowOperator 内部实现填充逻辑而非复用独立 FillOperator，避免 taosFillResultDataBlock 等宽 interval 假设的适配成本，NEXT 两遍扫描利用已预构建的内存窗口数组额外成本可控；回滚策略分快速回滚（恢复语义层拒绝）和执行层回滚两级，风险可控。

## 3. 评审结论

设计文档整体设计合理、逻辑清晰，功能覆盖全面，FILL 模式语义定义、forced/non-forced 差异规则、PARTITION BY 交互、伪列与窗口属性处理、边界场景覆盖、各层改动清单与实现策略等核心设计规范严谨，测试矩阵和验收标准完备，性能、兼容性设计符合系统规范，精准解决了 external_window FILL 能力在语法层、语义层和计划层被阻断、无法与 interval fill 在窗口级语义上对齐的核心痛点。

## 4. 后续行动项

无

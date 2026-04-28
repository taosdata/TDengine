# 20260424 External Window FILL 测试报告评审记录

## 1. 评审信息

1. 评审目的：评估 "External Window FILL 功能支持测试报告" 的合理性、安全性、性能、兼容性及可维护性
2. 评审文档：[External Window FILL TS](../../../06-功能测试/External%20Window%20FILL%20TS.md)
3. 会议主持：关胜亮
4. 会议人员：关胜亮、霍琳贺、潘魏、任新胜
5. 会议时间：2026-04-13 16:40 - 16:50
6. 会议形式：线下
7. 会议地点：taosX
8. 会议记录人：关胜亮

## 2. 评审记录

评审团队对测试文档（External Window FILL 功能支持测试相关）进行了全面审查，认为所有设计合理、内容详实、流程规范，具体评审意见如下：
1. 测试目标明确：针对 external_window FILL 功能开展专项测试，目标聚焦 NONE/NULL/NULL_F/VALUE/VALUE_F/PREV/NEXT 七种 FILL 模式在不同数据分布、分组、HAVING/ORDER BY 和多 vgroup 场景下的行为正确性验证，同时覆盖 forced/non-forced 差异、伪列正确性、不支持模式负例报错及 fill-value-to-column 错位回归，定位清晰、重点突出。
2. 测试用例设计全面：覆盖基础功能验证（七种 FILL 模式 + PARTITION BY + w.mark + 负例，14 个 helper）、HAVING 与 ORDER BY 交互（prev/value/null + 不同条件组合，5 个 helper）、扩展覆盖（多聚合列、连续空窗口 PREV/NEXT 传播、_wstart 正确性，6 个 helper）、边界场景（数据仅在末窗口的 group key 补丁路径、_wend 正确性、首/末空窗口 PREV/NEXT→NULL，5 个 helper）、多 vgroup merge aligned 路径（4 vgroup + 3 子表 + value/null/prev 三种模式）、fill-value 错位回归（HAVING/ORDER BY 引用 SELECT 外聚合函数，3 个 helper），6 个 test method 共 34+ 个 helper，用例设计科学合理、覆盖全面。
3. 测试覆盖维度完整：涵盖功能测试、兼容性测试两大核心维度，功能测试细分六个子模块层层递进，兼容性测试确认不带 FILL 时行为不变、历史用例通过、功能只影响查询结果不落盘无数据格式升级风险；空窗口行为矩阵以表格形式系统对照七种模式在空窗口和非空窗口的预期行为，已知限制明确记录（LINEAR/NEAR/SURROUND 不支持、缺席分组不物化、_wend 空窗口值差异），测试严谨性强。
4. 测试方法规范：明确各功能模块测试要点，详细列出测试用例、测试描述及测试结果，清晰区分正常场景与异常场景的验证重点；测试过程中主动发现并修正了 fill-value-to-column 错位问题，给出了修正方案（planLogicCreater.c 构建 pFillExprs、planPhysiCreater.c 通过 nodesEqualNode 匹配 slot）并新增回归 case，体现了测试驱动缺陷发现的价值。
5. 测试结论数据充分：6 个 test method 全部通过，测试入口命令和验证结果清晰记录，已发现问题已修正并有回归覆盖，结论客观真实，具备参考价值。
6. 文档信息完整：包含修订记录、测试目标、参考文档、测试环境、测试结论、测试用例总览、已知问题和限制等关键信息，修订记录清晰记录从初稿到评审发布的版本迭代，逻辑连贯、格式规范，便于后续查阅与维护。

## 3. 评审结论

测试文档整体合格，符合测试文档规范要求，同意归档。

## 4. 后续行动项

无

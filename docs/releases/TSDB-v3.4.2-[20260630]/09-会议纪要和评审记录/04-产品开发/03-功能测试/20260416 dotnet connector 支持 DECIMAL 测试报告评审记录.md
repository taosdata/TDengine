# 20260424 dotnet connector 支持 DECIMAL 测试报告评审记录

## 1. 评审信息

1. 评审目的：评估 "dotnet connector 支持 DECIMAL 测试报告" 的合理性、安全性、性能、兼容性及可维护性
2. 评审文档：[dotnet connector 支持 DECIMAL TS](../../../06-功能测试/dotnet%20connector%20支持%20DECIMAL%20TS.md)
3. 会议主持：关胜亮
4. 会议人员：关胜亮、霍琳贺、王旭、佘彦杰、裴亚明
5. 会议时间：2026-04-16 17:00 - 17:10
6. 会议形式：线下
7. 会议地点：taosX
8. 会议记录人：关胜亮

## 2. 评审记录

评审团队对测试文档（dotnet 连接器 decimal 类型支持功能测试相关）进行了全面审查，认为所有设计合理、内容详实、流程规范，具体评审意见如下：
1. 测试目标明确：针对 dotnet 连接器（taos-connector-dotnet）新增 decimal 数据类型（含 DECIMAL 128 位和 DECIMAL64 64 位）支持开展专项测试，目标聚焦 WebSocket 连接方式下 decimal 类型的 stmt2 协议处理、C# decimal 原生类型绑定、SQL 读写、stmt2 参数化查询、BlockReader 解析、类型校验及元数据识别七大核心场景，定位清晰、重点突出。
2. 测试用例设计全面：覆盖 stmt2 协议中 decimal 按变长字符串序列化（IsStmtVarDataType 与 IsVarDataType 区分）、C# decimal 原生类型 BindRow/BindColumn 绑定（含 decimal?[] null 处理）、SQL 正数/负数/null 写入查询、stmt2 参数化 SELECT WHERE 条件绑定、BlockReader 固定长度 ConvertDecimal64/ConvertDecimal 解析、类型校验拒绝 DateTime/bool/数值类型接受 string/decimal、元数据 GetFieldTypeName 返回等完整功能链路，DECIMAL 和 DECIMAL64 两种精度均有对称覆盖，用例设计科学合理、覆盖全面。
3. 测试覆盖维度完整：涵盖功能测试、兼容性测试两大核心维度，功能测试细分七个子模块，兼容性测试覆盖 .NET 5/6/7/8/9 多框架验证、已有类型不受影响回归验证（16 个 StmtTest 含 decimal 列全部通过）、3.3.6.0 旧版本兼容（TD_3360_TEST 环境变量控制 decimal 测试自动跳过），已知限制（decimal 不支持 tag 列、stmt2 仅接受字符串类型、精度上限 DECIMAL64=18/DECIMAL=38）明确记录，测试严谨性强。
4. 测试方法规范：明确各功能模块测试要点，详细列出测试用例、测试描述及测试结果，清晰区分正常场景与异常场景（非法类型抛出 ArgumentException）的验证重点，集成测试 WebSocketDecimalTest 和 WebSocketStmtTest 全部通过，单元测试 BlockReaderTest.TestDecimal 通过，测试流程规范，结果可验证、可追溯。
5. 测试结论数据充分：明确列出修改源文件 4 个、测试文件 3 个的变更范围，集成测试和单元测试通过情况清晰，已知限制均为服务端约束而非连接器缺陷，结论客观真实，具备参考价值。
6. 文档信息完整：包含修订记录、测试目标、参考文档、测试环境、测试结论、已知问题和限制等关键信息，修订记录清晰，逻辑连贯、格式规范，便于后续查阅与维护。

## 3. 评审结论

测试文档整体合格，符合测试文档规范要求，同意归档。

## 4. 后续行动项

无

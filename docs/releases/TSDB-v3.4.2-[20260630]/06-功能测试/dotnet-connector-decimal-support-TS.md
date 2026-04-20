# 功能测试报告（Test Spec）: dotnet 连接器支持 decimal 类型

# 修改记录

| 编写日期 | 发布日期 | 版本 | 修改人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-16 | 2026-04-16 | 1.0 | 裴亚明 | 新增 dotnet 连接器 decimal 类型支持功能测试 |

# 测试目标

TDengine 数据库已支持 decimal 数据类型（包括 DECIMAL 128位 和 DECIMAL64 64位），本需求为 dotnet 连接器（taos-connector-dotnet）添加 decimal 类型的完整支持，通过 WebSocket 连接方式实现。主要目标包括：

- 支持 decimal 类型数据的 SQL 写入与查询
- 支持 decimal 类型数据的 stmt2 参数绑定写入（数据列，字符串绑定）
- 支持 stmt2 参数化查询中 decimal 列的条件绑定
- 支持 C# `string` 和 C# `decimal` 两种原生类型绑定 decimal 列
- 支持 decimal 类型在 BlockReader 中的正确解析
- 支持 decimal 类型的元数据识别（类型名称、C# 类型映射）
- 支持 decimal 类型在 stmt BindRow / BindColumn 中的类型校验

# 参考文档

- TDengine 源码 taos.h：`TSDB_DATA_TYPE_DECIMAL = 17`（128位，16字节），`TSDB_DATA_TYPE_DECIMAL64 = 21`（64位，8字节）

# 测试结论

本 decimal 类型支持功能已完成开发和测试，WebSocket 连接方式可正常工作。

**关键数据：**
- 修改源文件：4 个（AbstractStmtBindColumn.cs、AbstractStmtBindrow.cs、AbstractStmtExec.cs、TDengineConstant.cs）
- 修改测试文件：3 个（Client.cs、WS.cs、Failover.cs）
- 集成测试：WebSocketDecimalTest、WebSocketStmtTest 全部通过
- 单元测试：BlockReaderTest.TestDecimal 通过

**已知限制（TDengine 服务端）：**
- decimal 类型 **不支持** 作为 tag 列
- decimal DDL 语法示例：`decimal(20,4)` 表示 precision=20, scale=4；当 precision ≤ 18 时内部使用 DECIMAL64（8字节），否则使用 DECIMAL（16字节）
- stmt2 参数绑定 decimal 列 **仅接受字符串类型** 数据（服务端通过 `decimal64FromStr`/`decimal128FromStr` 转换）
- DECIMAL64 最大精度 18，DECIMAL 最大精度 38

# 测试环境

- OS: Windows
- .NET SDK: 10.0，测试目标框架 net8
- TDengine Server: 3.4.1.3 Enterprise
- 测试框架: xUnit 2.4.3
- 连接方式: WebSocket（通过 taosAdapter）

# 功能测试

## decimal 类型 stmt2 协议处理

### 测试要点

验证 decimal 类型在 stmt2 参数绑定协议中按变长字符串方式序列化：
- 新增 `IsStmtVarDataType` 方法，将 DECIMAL/DECIMAL64 归入变长数据类型（仅用于 stmt2 序列化路径）
- `AbstractStmtExec` 中列数据序列化路径使用 `IsStmtVarDataType` 替代 `IsVarDataType`，避免影响 BlockReader 的固定长度 decimal 读取语义
- Tag 序列化路径保持使用 `IsVarDataType`（decimal 不支持作为 tag）
- `TypeLengthMap` 不包含 DECIMAL/DECIMAL64，通过 `IsStmtVarDataType` 检查确保走变长路径

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | WebSocketDecimalTest - stmt BindRow 字符串写入 | WebSocket 方式 stmt 使用 `string` 绑定 decimal(20,4) 和 decimal64(8,4) 列，验证写入成功 | Pass |
| 2 | WebSocketDecimalTest - stmt BindColumn 字符串写入 | WebSocket 方式 stmt 使用 `string[]` 绑定 decimal 列，验证写入成功 | Pass |

## decimal 类型 C# decimal 原生类型绑定

### 测试要点

验证除字符串外，C# 原生 `decimal` 类型也可用于绑定 decimal 列：
- `BindRow` 支持 C# `decimal` 类型值，内部通过 `InvariantCulture.ToString()` 转为字符串
- `BindColumn` 支持 `decimal[]` 和 `decimal?[]` 类型数组
- `decimal?[]` 中的 `null` 元素正确处理为 NULL 值

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | WebSocketDecimalTest - BindRow C# decimal | WebSocket 方式 BindRow 绑定 `88.1234m` 到 decimal(20,4) 列，查询验证值正确 | Pass |
| 2 | WebSocketDecimalTest - BindColumn decimal[] | WebSocket 方式 BindColumn 绑定 `decimal[] { 99.0001m }` 到 decimal 列，查询验证正确 | Pass |
| 3 | WebSocketDecimalTest - BindColumn decimal?[] null | WebSocket 方式 BindColumn 绑定 `decimal?[] { null }` 到 decimal 列，查询验证 IsDBNull 返回 true | Pass |
| 4 | WebSocketDecimalTest - decimal64 BindRow C# decimal | WebSocket 方式 BindRow 绑定 `55.6789m` 到 decimal64(8,4) 列，查询验证值正确 | Pass |

## decimal 类型 SQL 写入与查询

### 测试要点

验证通过 SQL 语句写入和查询 decimal 数据的正确性：
- 文本 decimal 数据写入与读取（`12345.6789`、`-9999.9999`）
- null decimal 值写入与读取
- `GetDecimal` 返回正确的 `decimal` 值
- `GetString` 返回正确的格式化字符串
- `IsDBNull` 正确识别 null decimal

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | WebSocketDecimalTest - SQL 正数写入查询 | WebSocket 方式 SQL 插入 `0.9999`，查询验证 `GetDecimal` 返回 `0.9999m` | Pass |
| 2 | WebSocketDecimalTest - SQL null 写入 | WebSocket 方式 SQL 插入 null decimal，查询验证 `IsDBNull` 返回 `true` | Pass |
| 3 | WebSocketDecimalTest - SQL 负数写入查询 | WebSocket 方式 stmt BindColumn 绑定 `"-9999.9999"` 写入，SQL 查询验证 `GetDecimal` 返回 `-9999.9999m` | Pass |

## decimal 类型 stmt2 参数化查询

### 测试要点

验证通过 stmt2 参数化查询（SELECT ... WHERE c1 = ?）查询 decimal 数据：
- `Prepare` 设置带参数的 SELECT 语句
- `BindRow` 绑定 decimal 字符串查询条件
- `Result()` 返回正确的查询结果
- 分别覆盖 decimal(20,4) 和 decimal64(8,4)

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | WebSocketDecimalTest - stmt2 查询 decimal | WebSocket 方式 `select * from table where c1 = ?` 绑定 `"12345.6789"` 查询 decimal(20,4) 列，验证 Result 返回正确行 | Pass |
| 2 | WebSocketDecimalTest - stmt2 查询 decimal64 | WebSocket 方式 `select * from table where c1 = ?` 绑定 `"1234.5678"` 查询 decimal64(8,4) 列，验证 Result 返回正确行 | Pass |

## decimal 类型 BlockReader 解析

### 测试要点

验证 decimal 数据在查询结果块中的正确解析：
- DECIMAL64（8字节固定长度）通过 `ConvertDecimal64` 解析
- DECIMAL（16字节固定长度）通过 `ConvertDecimal` 解析
- 精度和小数位信息从列元数据中正确提取
- null 位图正确处理

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | TestDecimal | 构造含 decimal64 和 decimal128 数据的模拟 block，验证 `ConvertDecimal64` 和 `ConvertDecimal` 正确解析，`GetValues` 和 `GetDecimal` 返回正确结果 | Pass |

## decimal 类型 stmt BindRow / BindColumn 类型校验

### 测试要点

验证 decimal 类型在 stmt 参数绑定时的类型校验逻辑：
- `BindRow`：decimal 列接受 `string` 和 C# `decimal` 类型，拒绝 `DateTime`、`bool`、数值类型等
- `BindColumn`：decimal 列接受 `string[]`、`decimal[]`、`decimal?[]` 类型，拒绝其他数组类型
- 绑定值使用合法 decimal 字符串 `"123.4500"`（而非 `"abc"`）

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | DoStmtTest - decimal 拒绝 DateTime | BindRow 绑定 `DateTime` 到 decimal 列，抛出 `ArgumentException` | Pass |
| 2 | DoStmtTest - decimal 拒绝 bool | BindRow 绑定 `bool` 到 decimal 列，抛出 `ArgumentException` | Pass |
| 3 | DoStmtTest - decimal 拒绝数值类型 | BindRow/BindColumn 绑定 `sbyte`、`short`、`int`、`long`、`float`、`double` 等到 decimal 列，均抛出 `ArgumentException` | Pass |
| 4 | DoStmtTest - decimal 接受 string | BindRow 绑定 `"123.4500"` 到 decimal 列成功；BindColumn 绑定 `string[]` 成功 | Pass |
| 5 | DoStmtTest - decimal64 拒绝非法类型 | 同上测试逻辑应用于 decimal64 列，所有非法类型均被拒绝 | Pass |
| 6 | DoStmtTest - decimal64 接受 string | BindRow 绑定合法 decimal 字符串到 decimal64 列成功 | Pass |

## decimal 类型元数据识别

### 测试要点

验证 decimal 类型在连接器类型系统中的正确注册：
- `GetFieldTypeName` 对 DECIMAL 和 DECIMAL64 返回 `"DECIMAL"`
- `ScanType` 映射为 `typeof(decimal)`
- `IsStmtVarDataType` 包含 DECIMAL 和 DECIMAL64
- `IsVarDataType` 不包含 DECIMAL 和 DECIMAL64（避免影响 BlockReader 固定长度读取）

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | GetColFields 返回 decimal 类型 | stmt Prepare 后 GetColFields 返回 decimal 字段，type 正确为 DECIMAL 或 DECIMAL64 | Pass |

## 易用性测试

不涉及。

## 长期稳定性测试

无。

## 性能测试

无。

## 安全性测试

无。

# 兼容性测试

| # | 测试场景 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | net 框架兼容性 | 在 .NET 5/6/7/8/9 框架下运行全部 decimal 测试（单元 + 集成），验证兼容性 | Pass |
| 2 | 已有类型不受影响 | 运行全部 StmtTest（16 个，含 decimal 列），验证新增 decimal 支持不影响已有类型 | Pass |
| 3 | 3.3.6.0 版本兼容 | 当 `TD_3360_TEST=true` 时，DecimalTest 和 StmtTest 中 decimal 相关测试自动跳过，不影响旧版本测试 | Pass |

# 已知问题和限制

- decimal 类型 **不支持** 作为 tag 列，这是 TDengine 服务端的限制（`"Decimal type is not allowed for tag"`）。
- stmt2 参数绑定 decimal 列 **仅接受字符串类型** 数据（`string`/`string[]` 或 C# `decimal`/`decimal[]` 自动转换为字符串）。不接受 C# 数值类型如 `int`、`double` 等直接绑定。
- DECIMAL64 最大精度为 18，DECIMAL 最大精度为 38。超出精度范围的值由服务端返回 `TSDB_CODE_DECIMAL_OVERFLOW` 错误。
- WebSocket 方式下，stmt2 绑定数据通过二进制协议直接传递给 C 库（taosAdapter 不解析绑定数据）。

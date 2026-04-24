# dotnet 连接器支持 blob 类型

# 1 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-15 | 2026-04-15 | 1.0 | 裴亚明 | 新增 dotnet 连接器 blob 类型支持功能测试 |

# 2 测试目标

TDengine 数据库已支持 blob 数据类型，本需求为 dotnet 连接器（taos-connector-dotnet）添加 blob 类型的完整支持，包括 Native 和 WebSocket 两种连接方式。主要目标包括：

- 支持 blob 类型数据的 SQL 写入与查询
- 支持 blob 类型数据的 stmt 参数绑定写入（数据列）
- 支持 blob 类型在 BlockReader 中的正确解析（4 字节 uint32 长度头）
- 支持 blob 类型的元数据识别（类型名称、C# 类型映射）
- 支持 blob 类型在 stmt BindRow / BindColumn 中的类型校验

# 3 参考文档

- TDengine 源码 types.h：`BlobDataLenT = uint32_t`，`BLOBSTR_HEADER_SIZE = 4`

# 4 测试结论

本次 blob 类型支持功能已完成开发和测试，Native 和 WebSocket 两种连接方式均可正常工作。

**关键数据：**
- 修改源文件：5 个（BlockReader.cs、BlockWriter.cs、TDengineConstant.cs、AbstractStmtBindrow.cs、AbstractStmtBindColumn.cs）
- 修改测试文件：6 个（Client.cs、WS.cs、Native.cs、BlockReaderTest.cs、BlockWriter.cs、TestTDengineMeta.cs）
- 单元测试：49 个全部通过（含新增 blob 相关 3 个）
- 集成测试：WebSocketBlobTest、NativeBlobTest、WebSocketStmtTest、NativeStmtTest 全部通过

**已知限制（TDengine 服务端）：**
- blob 类型 **不支持** 作为 tag 列
- blob 类型 **不支持** stmt 参数化查询绑定（`select * from t where c1 = ?`），服务端返回 `code:[0x6307],error:Operation not supported for BLOB type`
- blob DDL 不接受长度参数，语法为 `blob`（而非 `blob(100)`）

# 5 测试环境

- OS: Windows
- .NET SDK: 10.0，测试目标框架 net8
- TDengine Server: 3.4.1.3 Enterprise
- 测试框架: xUnit 2.4.3
- 连接方式: WebSocket（通过 taosAdapter）、Native（通过 C 客户端库）

# 6 功能测试

## 6.1 blob 类型元数据识别

### 6.1.1 测试要点

验证 blob 类型（`TSDB_DATA_TYPE_BLOB = 18`）在连接器类型系统中的正确注册：
- `ScanType` 映射为 `byte[]`
- `ScanNullableType` 映射为 `byte[]`
- `GetFieldTypeName` 返回 `"BLOB"`
- `IsVarDataType` 包含 blob 类型

### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | TestTypeNameBlob | `TDengineMeta.type = 18` 时 `TypeName()` 返回 `"BLOB"` | Pass |

## 6.2 blob 类型 BlockReader 解析

### 6.2.1 测试要点

验证 blob 数据在查询结果块中的正确解析。blob 与 binary/varbinary 的关键区别：
- binary/varbinary 使用 2 字节 uint16 长度头（`VARSTR_HEADER_SIZE = 2`）
- blob 使用 4 字节 uint32 长度头（`BLOBSTR_HEADER_SIZE = 4`）

连接器通过独立的 `ConvertBlob` 方法处理 blob 类型，直接读取 4 字节 uint32 作为数据长度，确保支持超过 65535 字节的大 blob 数据。

### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | TestBlobReadUsesUInt32LengthHeader | 构造含 300 字节 blob 数据的模拟 block，验证 `ConvertBlob` 使用 uint32 长度头正确解析，`GetValues` 和 `GetString` 返回正确结果 | Pass |

## 6.3 blob 类型 BlockWriter 序列化

### 6.3.1 测试要点

验证 blob 类型在 BlockWriter（旧版 stmt 列绑定序列化）中的正确处理：
- blob 与 binary/varbinary/json 共用变长类型序列化路径
- 支持 `byte[]` 和 `string` 两种输入类型
- 非法类型（如 `int[]`）应抛出 `ArgumentException`

### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | TestAllTypeWrite - blob byte[] | 使用 `byte[]{"abc"}` 序列化 blob 列，验证输出字节序列正确 | Pass |
| 2 | TestAllTypeWrite - blob string | 使用 `string[]{"abc"}` 序列化 blob 列，验证输出与 byte[] 一致 | Pass |
| 3 | TestAllTypeWrite - blob 非法类型 | 使用 `int[]{1}` 序列化 blob 列，验证抛出 `ArgumentException` | Pass |

## 6.4 blob 类型 SQL 写入与查询

### 6.4.1 测试要点

验证通过 SQL 语句写入和查询 blob 数据的正确性：
- 文本 blob 数据写入与读取（`'hello_blob'`）
- null blob 值写入与读取
- 十六进制 blob 数据写入与读取（`'\x010203'`）
- `GetValue` 返回 `byte[]` 类型
- `GetString` 返回 UTF-8 解码字符串
- `IsDBNull` 正确识别 null blob

### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | WebSocketBlobTest - SQL 文本写入 | WebSocket 方式 SQL 插入文本 blob，查询验证 `GetValue` 返回正确 `byte[]`，`GetString` 返回 `"hello_blob"` | Pass |
| 2 | WebSocketBlobTest - SQL null 写入 | WebSocket 方式 SQL 插入 null blob，查询验证 `IsDBNull` 返回 `true` | Pass |
| 3 | WebSocketBlobTest - SQL 十六进制写入 | WebSocket 方式 SQL 插入 `'\x010203'`，查询验证返回 `{0x01, 0x02, 0x03}` | Pass |
| 4 | NativeBlobTest - SQL 文本写入 | Native 方式 SQL 插入文本 blob，查询验证正确 | Pass |
| 5 | NativeBlobTest - SQL null 写入 | Native 方式 SQL 插入 null blob，查询验证正确 | Pass |
| 6 | NativeBlobTest - SQL 十六进制写入 | Native 方式 SQL 插入十六进制 blob，查询验证正确 | Pass |

## 6.5 blob 类型 stmt 参数绑定写入

### 6.5.1 测试要点

验证通过 stmt（参数化语句）方式写入 blob 数据列的正确性：
- `StmtInit` → `Prepare` → `GetColFields` 正确返回 blob 字段（type=18）
- `BindColumn` 支持 `byte[][]` 类型绑定 blob 数据列
- `BindRow` 支持 `byte[]` 和 `string` 类型绑定 blob 数据
- stmt 插入后通过 SQL 查询验证数据正确性
- 类型校验：非法类型应抛出 `ArgumentException`

### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | WebSocketBlobTest - stmt BindColumn 写入 | WebSocket 方式 stmt 绑定 `byte[][]` 写入 blob 列，查询验证数据正确 | Pass |
| 2 | NativeBlobTest - stmt BindColumn 写入 | Native 方式 stmt 绑定写入 blob 列，查询验证数据正确 | Pass |
| 3 | WebSocketStmtTest - blob DoStmtTest | WebSocket 方式 stmt 对 blob 列执行完整的 DoStmtTest 流程（null 绑定、byte[] 绑定、byte[][] BindColumn 绑定、string 绑定），验证共写入 7 行 | Pass |
| 4 | NativeStmtTest - blob DoStmtTest | Native 方式 stmt 对 blob 列执行完整的 DoStmtTest 流程，验证共写入 7 行 | Pass |

## 6.6 blob 类型 stmt BindRow / BindColumn 类型校验

### 6.6.1 测试要点

验证 blob 类型在 stmt 参数绑定时的类型校验逻辑：
- `BindRow`：blob 列接受 `byte[]` 和 `string` 类型，拒绝 `DateTime`、`bool`、数值类型等
- `BindColumn`：blob 列接受 `byte[][]` 和 `string[]` 类型，拒绝其他数组类型

### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | DoStmtTest - blob 拒绝 DateTime | BindRow 绑定 `DateTime` 到 blob 列，抛出 `ArgumentException` | Pass |
| 2 | DoStmtTest - blob 拒绝 bool | BindRow 绑定 `bool` 到 blob 列，抛出 `ArgumentException` | Pass |
| 3 | DoStmtTest - blob 拒绝数值类型 | BindRow/BindColumn 绑定 `sbyte`、`short`、`int`、`long`、`float`、`double` 等到 blob 列，均抛出 `ArgumentException` | Pass |

## 6.7 易用性测试

不涉及。

## 6.8 长期稳定性测试

无。

## 6.9 性能测试

无。

## 6.10 安全性测试

无。

# 7 兼容性测试

| # | 测试场景 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | net 框架兼容性 | 在 .NET 5/6/7/8/9 框架下运行全部 blob 测试（单元 + 集成），验证兼容性 | Pass |
| 2 | WebSocket 与 Native 一致性 | 同一套 BlobTest 逻辑分别在 WebSocket 和 Native 连接下执行，结果一致 | Pass |
| 3 | 已有类型不受影响 | 运行全部 49 个 Function Test 和 StmtTest，验证新增 blob 支持不影响已有类型 | Pass |

# 8 已知问题和限制

- blob 类型 **不支持** 作为 tag 列，这是 TDengine 服务端的限制。
- blob 类型 **不支持** stmt 参数化查询绑定（`select ... where blob_col = ?`），服务端返回错误码 `0x6307: Operation not supported for BLOB type`。stmt 写入（insert）不受此限制。
- blob DDL 不接受长度参数，正确语法为 `CREATE TABLE t(ts TIMESTAMP, c1 BLOB)`，使用 `BLOB(100)` 等形式会导致语法错误。

# JDBC 连接器-Test Spec

## 1. 修订记录

| **日期** | **版本** | **作者** | **备忘** |
| --- | --- | --- | --- |
| 2024-01-15 | 1.0 | 王旭 | 第一版定稿 |
| 2025-01-10 | 1.1 | 王旭 | 完善测试用例 |

## 2. 测试目标

1. **功能测试**：验证 JDBC 连接器所有功能模块的正确性，包括连接管理、SQL 执行、结果集处理、数据类型支持、Schemaless 写入、数据订阅、参数绑定、高可用等功能
2. **性能测试**：验证连接建立时间、写入吞吐量、查询延迟等性能指标达到预期
3. **安全性测试**：验证用户认证机制的正确性，包括错误用户名/密码的处理
4. **稳定性测试**：验证长时间运行、断线重连、故障转移等场景下系统的稳定性
5. **兼容性测试**：验证不同连接类型（JNI/WebSocket/RESTful）、不同 TDengine 版本、不同 Java 版本的兼容性

## 3. 测试范围

1. **功能测试**
   - 连接管理：JNI 连接、WebSocket 连接、RESTful 连接、连接池
   - SQL 执行：Statement、PreparedStatement、批量执行
   - 结果集处理：ResultSet、ResultSetMetaData
   - 数据类型：基本类型、无符号整数、字符串、时间戳、特殊类型（JSON/GEOMETRY/VARBINARY/DECIMAL/BLOB）
   - Schemaless 写入：InfluxDB Line Protocol、OpenTSDB Telnet、JSON 格式
   - 数据订阅 （TMQ）：消息消费、偏移量管理、元数据订阅
   - 参数绑定：标准参数绑定、超级表参数绑定、高效写入
   - 高可用：负载均衡、故障转移、自动重连、健康检查
   - DatabaseMetaData：数据库/表/列元数据查询
   - 异常处理：连接异常、SQL 执行异常、不支持的功能
2. **性能测试**：连接性能、写入性能、查询性能
3. **安全性测试**：用户认证、密码验证
4. **稳定性测试**：连接保活、自动重连、故障转移
5. **兼容性测试**：JNI/WebSocket/RESTful 三种连接类型覆盖

## 4. 测试结论

1. **功能测试**：通过
2. **性能测试**：通过
3. **安全性测试**：通过
4. **稳定性测试**：通过
5. **兼容性测试**：通过

## 5. 已知问题和限制

1. JNI 连接需要安装 TDengine 客户端库
2. WebSocket/RESTful 连接需要 taosAdapter 服务
3. 部分 JDBC 标准接口不支持（如 prepareCall、setSavepoint、createClob/createBlob）
4. 1970 年前的时间戳需要特殊处理

## 6. 测试环境

### 6.1 硬件环境

| **系统** | **部署** | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- |
| Linux/macOS/Windows | TDengine Server + taosAdapter | 4 核及以上 | 8 GB 及以上 | 100 GB SSD |

### 6.2 软件环境

| **软件** | **版本要求** |
| --- | --- |
| **Java JDK** | 1.8 及以上 |
| **Maven** | 3.6 及以上 |
| **TDengine Server** | 3.0.0.0 及以上 |
| **taosAdapter** | 与 TDengine Server 版本一致 |

### 6.3 测试框架

| **框架/工具** | **版本** | **用途** |
| --- | --- | --- |
| **JUnit** | 4.13.2 | 单元测试框架 |
| **Mockito** | 4.11.0 | Mock 测试框架 |
| **JaCoCo** | 0.8.12 | 代码覆盖率工具 |

### 6.4 连接配置

| **连接类型** | **URL 格式** | **默认端口** |
| --- | --- | --- |
| **JNI** | jdbc:TAOS://{host}:{port}/{database}?user={user}&password={password} | 6030 |
| **WebSocket** | jdbc:TAOS-WS://{host}:{port}/{database}?user={user}&password={password} | 6041 |
| **RESTful** | jdbc:TAOS-RS://{host}:{port}/{database}?user={user}&password={password} | 6041 |

### 6.5 默认测试参数

| **参数** | **默认值** |
| --- | --- |
| **host** | localhost |
| **user** | root |
| **password** | taosdata |

## 7. 测试用例

### 7.1 功能测试

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **连接管理** | JNI 正常连接 | 构建 JNI 连接 URL，调用 DriverManager.getConnection（），验证连接状态 | 连接成功，isClosed（） 返回 false | TSDBConnectionTest |
|  | JNI 认证失败 | 使用错误的用户名或密码尝试建立连接 | 抛出 SQLException，包含认证失败信息 | TSDBConnectionTest |
|  | JNI 连接关闭 | 调用 connection.close（），验证连接状态 | isClosed（） 返回 true | TSDBConnectionTest |
|  | JNI 连接有效性检测 | 调用 isValid（10）、isValid（0）、isValid（-1） | isValid（10/0） 返回 true，isValid（-1） 抛出 SQLException | TSDBConnectionTest |
|  | JNI 连接属性设置 | 设置 charset、locale、timezone 属性，获取并验证属性值 | 属性设置和获取成功 | TSDBConnectionTest |
|  | WebSocket 正常连接 | 构建 WebSocket 连接 URL，调用 DriverManager.getConnection（） | 连接成功 | WSConnectionTest |
|  | WebSocket 无数据库连接 | URL 中不指定数据库，建立连接 | 连接成功 | WSConnectionTest#withoutDBConnection |
|  | WebSocket 认证失败 | 使用错误的用户名尝试建立连接 | 抛出 SQLException | WSConnectionTest#wrongUserOrPasswordConnection |
|  | WebSocket 连接保活 | 执行查询，等待一段时间，再次执行查询 | 连接保持有效 | WSConnectionTest#keepConnection |
|  | WebSocket URL 解析 | 测试各种格式的 URL 解析 | 正确解析 host:port 部分 | WSConnectionTest#testRetainHostPortPart |
|  | RESTful 正常连接 | 构建 RESTful 连接 URL，建立连接 | 连接成功 | RestfulConnectionTest |
|  | RESTful 压缩传输 | 设置 enableCompression=true，执行查询 | 数据传输使用压缩 | RestfulCompressTest |
|  | 连接池兼容性 | 配置 HikariCP/Druid 连接池，获取/释放连接，验证连接复用 | 连接池正常工作 | - |
| **Statement** | Statement 创建 | 调用 createStatement（），验证返回对象 | 返回有效 Statement 对象 | TSDBStatementTest |
|  | executeUpdate | 执行 CREATE DATABASE/TABLE、INSERT、DROP DATABASE | 返回正确的影响行数 | TSDBStatementTest#executeUpdate |
|  | execute | 执行 DDL、DML、SELECT 语句 | DDL/DML 返回 false，SELECT 返回 true | TSDBStatementTest#execute |
|  | executeQuery | 执行 SELECT 查询，获取 ResultSet | 返回有效 ResultSet | TSDBStatementTest |
|  | getResultSet | 执行 SELECT 后调用 getResultSet（），遍历结果 | 返回正确的结果集 | TSDBStatementTest#getResultSet |
|  | addBatch | 添加多条 SQL 到批处理 | 无异常 | TSDBStatementTest#addBatch |
|  | executeBatch | 执行 executeBatch（），验证返回结果数组 | 返回每条 SQL 的执行结果 | TSDBStatementTest#executeBatch |
|  | clearBatch | 调用 clearBatch（），执行 executeBatch（） | 批处理被清空 | TSDBStatementTest#clearBatch |
|  | batchErrorIgnore | 设置 batchErrorIgnore=true，添加包含错误 SQL 的批处理，执行 | 忽略错误，继续执行 | BatchErrorIgnoreTest |
|  | getMaxFieldSize/setMaxFieldSize | 获取默认值，设置新值 | 默认值为 16*1024 | TSDBStatementTest |
|  | getQueryTimeout/setQueryTimeout | 获取默认超时，设置超时值 | 默认值为 0 | TSDBStatementTest |
|  | getFetchDirection/setFetchDirection | 获取默认方向，设置方向 | 默认为 FETCH_FORWARD | TSDBStatementTest |
| **PreparedStatement** | PreparedStatement 创建 | 调用 prepareStatement（sql） | 返回有效 PreparedStatement | TSDBPreparedStatementTest |
|  | 参数设置 - 基本类型 | setInt()、setLong()、setFloat()、setDouble()、setBoolean()、setString() | 参数设置成功 | TSDBPreparedStatementTest |
|  | 参数设置 - 时间类型 | setTimestamp()、setDate()、setTime() | 参数设置成功 | TSDBPreparedStatementTest |
|  | setNull | setNull(index, Types.XXX) | NULL 值设置成功 | TSDBPreparedStatementTest |
|  | clearParameters | 设置参数，clearParameters（），重新设置参数 | 参数被清除 | TSDBPreparedStatementTest |
|  | WS PreparedStatement 插入 | 准备 INSERT 语句，设置参数，执行 executeUpdate（） | 返回影响行数 1 | WsPstmtTest#test001_ExecuteUpdate |
|  | WS PreparedStatement 重用 | 创建 PreparedStatement，执行多次 executeUpdate（） | 每次执行都成功 | WsPstmtTest#test002_ReuseStmtExecuteUpdate |
|  | WS PreparedStatement 批量插入 | 循环添加批处理，执行 executeBatch（） | 批量插入成功 | WsPstmtTest#test003_ExecuteBatchInsert |
|  | WS PreparedStatement 查询 | 准备带参数的 SELECT，设置时间范围参数，执行查询 | 返回正确结果 | WsPstmtTest#test004_Query |
|  | WS PreparedStatement 语法错误 | 准备错误 SQL，执行查询 | 抛出 SQLException | WsPstmtTest#test006_QuerySyntaxError |
|  | 全类型参数绑定 | 测试所有数据类型的参数绑定 | 所有类型绑定成功 | WsPstmtAllTypeTest |
|  | NULL 值绑定 | 测试所有类型的 NULL 值绑定 | NULL 值正确插入 | WsPStmtAllTypeNullTest |
|  | Tag 参数绑定 | 绑定子表名、Tag 值、数据列 | 自动创建子表并插入数据 | WsPstmtSubTableTest |
|  | Line 模式绑定 | 使用行模式绑定数据 | 数据正确插入 | WsPstmtLineModeAllTypeTest |
| **ResultSet** | ResultSet 遍历 | 调用 next（），获取数据 | 正确遍历所有行 | TSDBResultSetTest |
|  | getXXX(columnIndex) | 测试 getInt（）、getString（）、getTimestamp（） 等方法，使用列索引 | 返回正确的值 | TSDBResultSetTest |
|  | getXXX(columnLabel) | 测试 getInt（）、getString（）、getTimestamp（） 等方法，使用列名 | 返回正确的值 | TSDBResultSetTest |
|  | wasNull | 获取 NULL 列值，调用 wasNull（） | wasNull（） 返回 true | WasNullTest |
|  | ResultSet 关闭 | 关闭 ResultSet，验证状态 | isClosed（） 返回 true | TSDBResultSetTest |
|  | getColumnCount | 获取列数 | 返回正确的列数 | TSDBResultSetMetaDataTest |
|  | getColumnName | 获取各列名称 | 返回正确的列名 | TSDBResultSetMetaDataTest |
|  | getColumnType | 获取各列类型 | 返回正确的 JDBC 类型 | TSDBResultSetMetaDataTest |
|  | getColumnTypeName | 获取各列类型名称 | 返回 TDengine 类型名 | TSDBResultSetMetaDataTest |
|  | getPrecision/getScale | 获取数值精度和小数位数 | 返回正确的精度信息 | TSDBResultSetMetaDataTest |
| **数据类型** | BOOL 类型 | 插入 true/false，查询验证 | 值正确存储和读取 | DataTypeTest |
|  | TINYINT 类型 | 测试 -128 到 127 范围的值 | 值正确存储和读取 | DataTypeTest |
|  | SMALLINT 类型 | 测试 -32768 到 32767 范围的值 | 值正确存储和读取 | DataTypeTest |
|  | INT 类型 | 测试 INT 范围的值 | 值正确存储和读取 | DataTypeTest |
|  | BIGINT 类型 | 测试 BIGINT 范围的值 | 值正确存储和读取 | DataTypeTest |
|  | FLOAT 类型 | 测试浮点数值 | 值正确存储和读取（精度范围内） | DataTypeTest |
|  | DOUBLE 类型 | 测试双精度浮点数值 | 值正确存储和读取 | DataTypeTest |
|  | TINYINT UNSIGNED | 测试 0 到 255 范围的值 | 值正确存储和读取 | UnsignedNumberJniTest, UnsignedNumberRestfulTest |
|  | SMALLINT UNSIGNED | 测试 0 到 65535 范围的值 | 值正确存储和读取 | UnsignedNumberJniTest, UnsignedNumberRestfulTest |
|  | INT UNSIGNED | 测试无符号 INT 范围的值 | 值正确存储和读取 | UnsignedNumberJniTest, UnsignedNumberRestfulTest |
|  | BIGINT UNSIGNED | 测试无符号 BIGINT 范围的值 | 值正确存储和读取，使用 BigInteger | UnsignedNumberJniTest, UnsignedNumberRestfulTest |
|  | BINARY 类型 | 测试字节数组存储 | 二进制数据正确存储和读取 | - |
|  | NCHAR 类型 | 测试中文、特殊字符存储 | Unicode 字符正确存储和读取 | - |
|  | VARCHAR 类型 | 测试可变长字符串 | 字符串正确存储和读取 | - |
|  | 特殊字符插入 | 插入包含引号、转义字符的数据 | 特殊字符正确处理 | InsertSpecialCharacterJniTest, InsertSpecialCharacterRestfulTest |
|  | TIMESTAMP 毫秒精度 | 测试毫秒精度时间戳 | 时间戳精确到毫秒 | MicroSecondPrecisionJNITest, MicroSecondPrecisionRestfulTest |
|  | TIMESTAMP 微秒精度 | 测试微秒精度时间戳 | 时间戳精确到微秒 | MicroSecondPrecisionJNITest, MicroSecondPrecisionRestfulTest |
|  | TIMESTAMP 纳秒精度 | 测试纳秒精度时间戳 | 时间戳精确到纳秒 | NanoSecondTimestampJNITest, NanoSecondTimestampRestfulTest |
|  | 1970 年前时间戳 | 测试 1970 年之前的时间戳 | 负时间戳正确处理 | DatetimeBefore1970Test |
|  | JSON 类型 | 创建带 JSON Tag 的表，插入 JSON 数据，查询验证 | JSON 数据正确存储和读取 | JsonTagTest, RestfulJsonTagTest, WSJsonTagTest |
|  | GEOMETRY 类型 | 测试地理空间数据类型 | 几何数据正确存储和读取 | GeometryTest, WSGeometryTest |
|  | VARBINARY 类型 | 测试可变长二进制类型 | 二进制数据正确存储和读取 | VarbinaryTest, WSVarbinaryTest |
|  | DECIMAL 类型 | 测试 DECIMAL64/DECIMAL128 类型 | 高精度数值正确存储和读取 | WSDecimalTest |
|  | BLOB 类型 | 测试大对象类型 | BLOB 数据正确存储和读取 | BlobTest, WSBlobTest |
| **Schemaless 写入** | Line Protocol 基本写入 | 构造 Line Protocol 数据，调用 write（） 方法 | 数据正确写入，自动创建表 | SchemalessInsertTest, WSSchemalessTest#testLine |
|  | Line Protocol writeRaw | 使用 writeRaw（） 写入单条数据 | 数据正确写入 | WSSchemalessTest#testWriteRaw |
|  | Line Protocol TTL | 写入数据时指定 TTL | 数据带 TTL 写入 | WSSchemalessTest#testLineTtl |
|  | Telnet Protocol 写入 | 构造 Telnet 格式数据，调用 write（） 方法 | 数据正确写入 | WSSchemalessTest#telnetInsert |
|  | Telnet Protocol List 写入 | 使用 List 批量写入 | 批量数据正确写入 | WSSchemalessTest#telnetListInsert |
|  | JSON Protocol 写入 | 构造 JSON 格式数据，调用 write（） 方法 | JSON 数据正确解析并写入 | WSSchemalessTest#jsonInsert |
| **数据订阅 （TMQ）** | JNI Consumer 创建 | 配置 Consumer 属性，创建 TaosConsumer，订阅 topic | Consumer 创建成功 | TaosConsumerTest#JNI_01_Test |
|  | Consumer subscribe | 调用 subscribe（），验证 subscription（） | 订阅成功 | TaosConsumerTest |
|  | Consumer poll | 调用 poll（Duration），遍历 ConsumerRecords | 获取到消费记录 | TaosConsumerTest |
|  | Consumer unsubscribe | 调用 unsubscribe（） | 取消订阅成功 | TaosConsumerTest |
|  | WS Consumer Map 消费 | 配置 WS Consumer，订阅并消费 | 获取 Map<String， Object> 格式数据 | WSConsumerMainTest#testWSMap |
|  | WS Consumer Bean 反序列化 | 配置自定义 Deserializer，消费数据 | 数据正确反序列化为 Bean | WSConsumerMainTest#testWSBeanObject |
|  | WS Consumer 自动提交 | 设置 enable.auto.commit=true | 自动提交 offset | WSConsumerAutoCommitTest |
|  | commitSync | 手动同步提交 offset | offset 提交成功 | ConsumerCommitTest |
|  | commitAsync | 异步提交 offset | 回调函数被调用 | ConsumerCommitTest |
|  | seek | 调用 seek（），从指定 offset 消费 | 从指定位置开始消费 | OffsetSeekTest, WSConsumerOffsetSeekTest |
|  | committed offset 查询 | 查询已提交的 offset | 返回正确的 offset 值 | ConsumerCommittedTest |
|  | DDL 元数据订阅 | 订阅包含元数据的 topic | 接收 CREATE/ALTER/DROP 事件 | WSConsumerMetaTest |
|  | Delete 元数据订阅 | 订阅 DELETE 操作的元数据 | 接收 DELETE 事件 | WSConsumerMetaDeleteTest |
| **WebSocket 功能** | WebSocket 查询 | 通过 WebSocket 执行 SELECT 查询 | 返回正确结果 | WSQueryTest |
|  | WebSocket 大数据量查询 | 查询大量数据 | 分批获取数据成功 | WSBigQueryTest |
|  | WebSocket 查询超时 | 设置查询超时并执行慢查询 | 超时后抛出异常 | WSQueryTimeoutTest |
|  | WebSocket 压缩传输 | 启用压缩传输数据 | 数据正确传输 | WSCompressTest |
|  | WebSocket 时区配置 | 设置不同时区，查询时间戳数据 | 时间戳按时区正确转换 | WSTimeZoneTest, WSTimeZoneTest2 |
|  | WebSocket BI 模式查询 | 启用 BI 模式查询 | 返回 BI 友好格式数据 | WSQueryBIModeTest |
| **参数绑定** | 基本参数绑定插入 | 准备 INSERT 语句，绑定参数，执行插入 | 数据正确插入 | ParameterBindTest |
|  | 批量参数绑定 | 多次 addBatch（），执行 executeBatch（） | 批量数据正确插入 | PreparedStatementBatchInsertJNITest, PreparedStatementBatchInsertRestfulTest |
|  | 超级表子表自动创建 | 绑定子表名和 Tag，绑定数据列，执行插入 | 自动创建子表并插入数据 | WsPstmtSubTableTest |
|  | 多子表批量绑定 | 在一个批次中绑定多个子表的数据 | 数据正确插入各子表 | - |
|  | 高效写入基本测试 | 使用 ASYNC_INSERT 前缀，批量写入数据 | 异步写入成功 | WsEfficientWritingTest |
|  | 高效写入配置参数 | 测试 batchSizeByRow、cacheSizeByRow 等参数 | 参数生效 | WsEfficientWritingTest |
| **高可用** | 多节点负载均衡 | 配置多个 endpoint，执行多次连接 | 连接分布到多个节点 | WSLoadBalanceTest, WSLoadBalance2Test |
|  | 最小连接数策略 | 测试按最小连接数分配请求 | 请求分配到连接数最少的节点 | MinimumConnectionCountTest |
|  | 连接重平衡 | 触发连接重平衡 | 连接重新分配 | ConnectionRebalanceTest, RebalanceManagerTest |
|  | 连接故障转移 | 建立连接，模拟节点故障，验证自动切换 | 自动切换到可用节点 | WSConFailOverTest |
|  | 主从切换 | 配置主从集群并测试切换 | 主节点故障后切换到从节点 | WSMasterSlaveTest |
|  | PreparedStatement 重连查询 | 建立连接，断开连接，执行查询（启用 enableAutoReconnect） | 自动重连并执行查询 | WsPstmtReconnectQueryTest |
|  | PreparedStatement 重连插入 | 断开后重连执行插入 | 自动重连并插入成功 | WsPstmtReconnectInsertTest |
|  | PreparedStatement 重连 Fetch | 断开后重连执行 Fetch | 自动重连并获取数据 | WsPstmtReconnectFetchTest |
|  | 后台健康检查 | 配置健康检查参数并验证 | 定期执行健康检查 | BgHealthCheckTest |
| **DatabaseMetaData** | getDatabaseProductName | 获取数据库产品名称 | 返回 "TDengine" | TSDBDatabaseMetaDataTest, WSDatabaseMetaDataTest |
|  | getDatabaseProductVersion | 获取数据库版本 | 返回版本字符串 | TSDBDatabaseMetaDataTest |
|  | getDriverName | 获取驱动名称 | 返回 "com.taosdata.jdbc.TSDBDriver" 或类似值 | TSDBDatabaseMetaDataTest |
|  | getDriverVersion | 获取驱动版本 | 返回版本字符串 | TSDBDatabaseMetaDataTest |
|  | getCatalogs | 获取所有数据库列表 | 返回数据库名称列表 | TSDBDatabaseMetaDataTest |
|  | getTables | 获取指定数据库的表列表 | 返回表名列表 | TSDBDatabaseMetaDataTest |
|  | getColumns | 获取指定表的列信息 | 返回列名、类型等信息 | TSDBDatabaseMetaDataTest, AbstractDatabaseMetaDataColumnTest |
|  | getPrimaryKeys | 获取表的主键信息 | 返回时间戳列作为主键 | TSDBDatabaseMetaDataTest |
|  | getSuperTables | 获取超级表列表 | 返回超级表信息 | TSDBDatabaseMetaDataTest |
| **异常处理** | 连接不可用时执行操作 | 关闭连接，尝试执行 SQL | 抛出 ERROR_CONNECTION_CLOSED | - |
|  | URL 格式错误 | 使用无效 URL 建立连接 | 抛出适当异常 | TSDBDriverTest |
|  | 用户名为空 | 不提供用户名建立连接 | 抛出 ERROR_USER_IS_REQUIRED | TSDBDriverTest |
|  | 密码为空 | 不提供密码建立连接 | 抛出 ERROR_PASSWORD_IS_REQUIRED | TSDBDriverTest |
|  | SQL 语法错误 | 执行语法错误的 SQL | 抛出 SQLException 包含错误信息 | - |
|  | 表不存在错误 | 查询不存在的表 | 抛出适当异常 | - |
|  | 数据库不存在错误 | 连接不存在的数据库 | 抛出适当异常 | ConnectWrongDatabaseTest |
|  | prepareCall | 调用 prepareCall（） | 抛出 SQLFeatureNotSupportedException | TSDBConnectionTest |
|  | setSavepoint | 调用 setSavepoint（） | 抛出 SQLFeatureNotSupportedException | TSDBConnectionTest |
|  | createClob/createBlob | 调用 createClob（） 或 createBlob（） | 抛出 SQLFeatureNotSupportedException | TSDBConnectionTest |

### 7.2 性能测试

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **连接建立时间** | 测量建立连接的平均时间 | JNI < 50ms, WS < 100ms | - |
| **并发连接** | 同时建立多个连接 | 支持 100+ 并发连接 | - |
| **单条插入吞吐量** | 测量单条 INSERT 的 TPS | 记录 TPS 指标 | - |
| **批量插入吞吐量** | 测量批量 INSERT 的 TPS | 批量 TPS > 单条 TPS × 10 | - |
| **参数绑定写入性能** | 测量参数绑定批量写入性能 | 优于普通批量插入 | - |
| **简单查询延迟** | 测量简单 SELECT 的响应时间 | < 10ms | - |
| **大数据量查询** | 查询 100 万行数据 | 记录耗时和内存使用 | - |

### 7.3 安全性测试

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **JNI 认证失败** | 使用错误的用户名或密码建立 JNI 连接 | 抛出 SQLException，包含认证失败信息 | TSDBConnectionTest |
| **WebSocket 认证失败** | 使用错误的用户名建立 WebSocket 连接 | 抛出 SQLException | WSConnectionTest#wrongUserOrPasswordConnection |
| **用户名为空** | 不提供用户名建立连接 | 抛出 ERROR_USER_IS_REQUIRED | TSDBDriverTest |
| **密码为空** | 不提供密码建立连接 | 抛出 ERROR_PASSWORD_IS_REQUIRED | TSDBDriverTest |

### 7.4 稳定性测试

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **WebSocket 连接保活** | 执行查询，等待一段时间，再次执行查询 | 连接保持有效 | WSConnectionTest#keepConnection |
| **连接故障转移** | 建立连接，模拟节点故障，验证自动切换 | 自动切换到可用节点 | WSConFailOverTest |
| **PreparedStatement 重连查询** | 启用 enableAutoReconnect，断开后执行查询 | 自动重连并执行查询 | WsPstmtReconnectQueryTest |
| **PreparedStatement 重连插入** | 断开后重连执行插入 | 自动重连并插入成功 | WsPstmtReconnectInsertTest |
| **PreparedStatement 重连 Fetch** | 断开后重连执行 Fetch | 自动重连并获取数据 | WsPstmtReconnectFetchTest |
| **后台健康检查** | 配置健康检查参数并验证 | 定期执行健康检查 | BgHealthCheckTest |

### 7.5 兼容性测试

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **JNI 连接类型** | 使用 jdbc:TAOS:// 前缀测试所有功能 | 功能正常 | *Test（无后缀的测试类） |
| **WebSocket 连接类型** | 使用 jdbc:TAOS-WS:// 前缀测试所有功能 | 功能正常 | *WSTest（WS 后缀的测试类） |
| **RESTful 连接类型** | 使用 jdbc:TAOS-RS:// 前缀测试所有功能 | 功能正常 | *RestfulTest（Restful 后缀的测试类） |

## 8. 测试计划

- **测试环境**：0.5 人天
- **测试执行**：5 人天
- **测试总结**：3 人天

### 8.1 测试执行命令

```bash

## 9. 执行所有测试

mvn test

## 10. 执行指定测试类

mvn test -Dtest=TSDBConnectionTest

## 11. 执行指定测试方法

mvn test -Dtest=TSDBConnectionTest#createStatement

## 12. 跳过测试

mvn package -DskipTests

## 13. 生成测试报告

mvn test jacoco:report
```

### 13.1 测试执行前置条件

1. **启动 TDengine 服务**
```bash
systemctl start taosd
systemctl start taosadapter
```

1. **验证服务状态**
```bash
taos -s "SHOW CLUSTER ALIVE"
```

1. **配置测试环境变量（可选）**
```bash
export TAOS_HOST=localhost
export TAOS_PORT=6030
export TAOS_USER=root
export TAOS_PASSWORD=taosdata
```

### 13.2 覆盖率要求

| **指标** | **最低要求** |
| --- | --- |
| **指令覆盖率** | ≥ 80% |
| **分支覆盖率** | ≥ 70% |
| **类覆盖率** | ≥ 90% |
| **方法覆盖率** | ≥ 80% |

## 14. 风险评估

1. **环境依赖风险**：JNI 连接需要安装 TDengine 客户端库，可能存在版本兼容性问题
2. **服务依赖风险**：WebSocket/RESTful 连接需要 taosAdapter 服务，服务不可用会导致测试失败
3. **集群测试风险**：高可用测试需要多节点集群环境，单机测试无法覆盖
4. **性能测试风险**：性能指标受硬件配置影响，不同环境结果可能差异较大

## 15. 参考文档

| **文档名称** | **描述** |
| --- | --- |
| **JDBC 连接器-Requirement Spec.docx** | 需求规格说明书 |
| **JDBC 连接器-Design Spec.docx** | 设计规格说明书 |
| **JDBC 连接器-Function Spec.docx** | 功能规格说明书 |

## 16. 附录

### 16.1 附录 A：错误码参考

| **错误码** | **常量名** | **描述** |
| --- | --- | --- |
| **0x2301** | ERROR_CONNECTION_CLOSED | 连接已关闭 |
| **0x2302** | ERROR_UNSUPPORTED_METHOD | 不支持的方法 |
| **0x2303** | ERROR_INVALID_VARIABLE | 无效的变量 |
| **0x2304** | ERROR_STATEMENT_CLOSED | Statement 已关闭 |
| **0x2305** | ERROR_RESULTSET_CLOSED | ResultSet 已关闭 |
| **0x2318** | ERROR_USER_IS_REQUIRED | 需要用户名 |
| **0x2319** | ERROR_PASSWORD_IS_REQUIRED | 需要密码 |
| **0x2350** | ERROR_UNKNOWN | 未知错误 |

### 16.2 附录 B：测试数据类型映射

| **TDengine 类型** | **JDBC 类型** | **Java 类型** |
| --- | --- | --- |
| **BOOL** | Types.BOOLEAN | Boolean |
| **TINYINT** | Types.TINYINT | Byte |
| **SMALLINT** | Types.SMALLINT | Short |
| **INT** | Types.INTEGER | Integer |
| **BIGINT** | Types.BIGINT | Long |
| **FLOAT** | Types.FLOAT | Float |
| **DOUBLE** | Types.DOUBLE | Double |
| **BINARY** | Types.VARCHAR | byte[] |
| **NCHAR** | Types.NCHAR | String |
| **TIMESTAMP** | Types.TIMESTAMP | Timestamp |
| **JSON** | Types.OTHER | String |
| **VARBINARY** | Types.VARBINARY | byte[] |
| **GEOMETRY** | Types.BINARY | byte[] |
| **DECIMAL** | Types.DECIMAL | BigDecimal |

### 16.3 附录 C：测试用例清单汇总

| **模块** | **测试用例数** | **P0** | **P1** | **P2** |
| --- | --- | --- | --- | --- |
| **连接管理** | 20 | 8 | 8 | 4 |
| **Statement** | 15 | 6 | 5 | 4 |
| **PreparedStatement** | 20 | 8 | 8 | 4 |
| **ResultSet** | 10 | 5 | 3 | 2 |
| **数据类型** | 25 | 12 | 10 | 3 |
| **Schemaless** | 8 | 3 | 5 | 0 |
| **TMQ** | 15 | 4 | 8 | 3 |
| **WebSocket** | 10 | 4 | 4 | 2 |
| **参数绑定** | 8 | 3 | 4 | 1 |
| **高可用** | 12 | 2 | 6 | 4 |
| **DatabaseMetaData** | 10 | 0 | 6 | 4 |
| **异常处理** | 12 | 4 | 5 | 3 |
| **性能测试** | 8 | 0 | 0 | 8 |
| **总计** | **173** | **59** | **72** | **42** |

### 16.4 附录 D：测试用例目录结构

```plaintext
src/test/java/com/taosdata/jdbc/
├── TSDBConnectionTest.java          # JNI 连接测试
├── TSDBStatementTest.java           # JNI Statement 测试
├── TSDBResultSetTest.java           # JNI ResultSet 测试
├── TSDBPreparedStatementTest.java   # JNI PreparedStatement 测试
├── cases/                           # 特定场景测试
├── confprops/                       # 配置属性测试
├── enums/                           # 枚举类测试
├── rs/                              # RESTful 连接测试
├── tmq/                             # 数据订阅测试
├── utils/                           # 工具类测试
└── ws/                              # WebSocket 连接测试
    ├── stmt/                        # WS PreparedStatement 测试
    ├── tmq/                         # WS TMQ 测试
    └── loadbalance/                 # 负载均衡测试
```

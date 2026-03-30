# C# 连接器-Function Spec

## 1. 修订记录

| 修改日期 | 发布日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-11 | 2025-01-11 | 1.0 | 谭雪峰 |  |
| 2025-12-09 | 2025-12-09 | 2.0 | 谭雪峰 | 添加查询结果新方法 |
| 2026-01-26 | 2026-01-27 | 2.1 | 霍琳贺 | 更新安全部分 |

## 2. 背景

taos-connector-dotnet 项目旨在为 C# 开发者提供一套功能完备的官方连接器。该连接器遵循 .NET 数据库访问的最佳实践，支持 TDengine 的核心功能，包括连接、SQL 执行、参数绑定等。对于 TDengine 特有的功能（如无模式写入和数据订阅），将通过扩展接口实现。

## 3. 定义

1. **ADO.NET****： **ADO.NET 是 .NET 框架中用于访问和操作数据的一组类库，提供了一套标准接口（如 `IDbConnection`、`IDbCommand`、`IDataReader` 等）来连接和操作各种数据源。
2. **DSN (Data Source Name)： **数据源名称，用于指定连接到数据库所需的信息。
3. **WebSocket： **一种在单个 TCP 连接上进行全双工通信的协议。
4. **TMQ： **TDengine 消息队列，支持订阅数据。
5. **无模式写入： **一种数据写入方式，允许在不预先定义表结构的情况下直接将数据写入数据库。

## 4. 行为说明

C# 连接器提供 ADO.NET 接口（命名空间 TDengine.Data.Client ）、查询写入接口（命名空间 TDengine.Driver.Client）与 TMQ 接口（命名空间 TDengine.TMQ）

### 4.1 数据类型对应

| TDengine 类型 | C# 类型 |
| --- | --- |
| TIMESTAMP | DateTime |
| TINYINT | sbyte |
| SMALLINT | short |
| INT | int |
| BIGINT | long |
| TINYINT UNSIGNED | byte |
| SMALLINT UNSIGNED | ushort |
| INT UNSIGNED | uint |
| BIGINT UNSIGNED | ulong |
| FLOAT | float |
| DOUBLE | double |
| BOOL | bool |
| BINARY | byte[] |
| NCHAR | string |
| JSON | byte[] |
| VARBINARY | byte[] |
| GEOMETRY | byte[] |
| DECIMAL | decimal |

### 4.2 ADO.NET 接口

#### 4.2.1 连接参数

`TDengineConnectionStringBuilder` 是 `taos-connector-dotnet` 中用于构建和管理 TDengine 数据库连接字符串的类。它继承自 `System.Data.Common.DbConnectionStringBuilder`。
`TDengineConnectionStringBuilder` 使用 key-value 对方式设置连接参数，key 为参数名，value 为参数值，不同参数之间使用分号 `;` 分割。
例如：
```csharp
"protocol=WebSocket;host=127.0.0.1;port=6041;useSSL=false"
```

支持的参数如下：
- `host`：TDengine 运行实例的地址。
- `port`：TDengine 运行实例的端口。
- `username`：连接的用户名。
- `password`：连接的密码。
- `protocol`：连接的协议，可选值为 Native 或 WebSocket，默认为 Native。
- `db`：连接的数据库。
- `timezone`：时区，默认为本地时区，尽在查询结果解析使用。
- `connTimeout`：连接超时时间，默认为 1 分钟。
- `connectionTimezone`：连接时区，仅支持 .NET 6 及以上版本，只支持 IANA 时区格式，与 `timezone` 不可同时设置
WebSocket 连接额外支持以下参数：
- `readTimeout`：读取超时时间，默认为 5 分钟。
- `writeTimeout`：发送超时时间，默认为 10 秒。
- `token`：连接 TDengine cloud 的 token。
- `useSSL`：是否使用 SSL 连接，默认为 false。
- `enableCompression`：是否启用 WebSocket 压缩，默认为 false。
- `autoReconnect`：是否自动重连，默认为 false。
- `reconnectRetryCount`：重连次数，默认为 3。
- `reconnectIntervalMs`：重连间隔毫秒时间，默认为 2000。

#### 4.2.2 数据库连接

`TDengineConnection` 是 `taos-connector-dotnet` 中用于管理与 TDengine 数据库连接的核心类。它实现了 `System.Data.Common.DbConnection` 接口，提供了连接管理、命令创建等功能。
1. **连接管理**
  - 提供 `Open()` 和 `Close()` 方法，用于建立和关闭与 TDengine 数据库的连接。
1. **连接状态管理**
  - 通过 `State` 属性获取当前连接的状态（如 `Open`、`Closed`）。
  - 提供 `StateChange` 事件，用于监听连接状态的变化。
1. **命令创建**
  - 提供 `CreateCommand()` 方法，用于创建与当前连接关联的 `TDengineCommand` 对象，以执行 SQL 查询或操作。
1. **连接字符串管理**
  - 通过 `ConnectionString` 属性设置或获取连接字符串。
  - 支持使用 `TDengineConnectionStringBuilder` 构建连接字符串。
原生链接样例
```csharp {wrap}
static void Main(String[] args)
{
    var connectionString = "host=127.0.0.1;port=6030;username=root;password=taosdata";
    try
    {
        // Connect to TDengine server using Native
        var builder = new ConnectionStringBuilder(connectionString);
        // Open connection with using block, it will close the connection automatically
        using (var client = DbDriver.Open(builder))
        {
            Console.WriteLine("Connected to " + connectionString + " successfully.");
        }
    }
    catch (TDengineError e)
    {
        // handle TDengine error
        Console.WriteLine("Failed to connect to " + connectionString + "; ErrCode:" + e.Code + "; ErrMessage: " + e.Error);
        throw;
    }
    catch (Exception e)
    {
        // handle other exceptions
        Console.WriteLine("Failed to connect to " + connectionString + "; Err:" + e.Message);
        throw;
    }
}
```

WebSocket 连接样例
1. ws 连接
```csharp {wrap}
static void Main(string[] args)
{
    var connectionString =
        "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata";
    try
    {
        // Connect to TDengine server using WebSocket
        var builder = new ConnectionStringBuilder(connectionString);
        // Open connection with using block, it will close the connection automatically
        using (var client = DbDriver.Open(builder))
        {
            Console.WriteLine("Connected to " + connectionString + " successfully.");
        }
    }
    catch (TDengineError e)
    {
        // handle TDengine error
        Console.WriteLine("Failed to connect to " + connectionString + "; ErrCode:" + e.Code +
                          "; ErrMessage: " + e.Error);
        throw;
    }
    catch (Exception e)
    {
        // handle other exceptions
        Console.WriteLine("Failed to connect to " + connectionString + "; Err:" + e.Message);
        throw;
    }
}
```

1. wss 连接
```csharp {wrap}
static void Main(string[] args)
{
    var connectionString =
        "protocol=WebSocket;host=localhost;port=6041;useSSL=true;username=root;password=taosdata";
    try
    {
        // Connect to TDengine server using WebSocket
        var builder = new ConnectionStringBuilder(connectionString);
        // Open connection with using block, it will close the connection automatically
        using (var client = DbDriver.Open(builder))
        {
            Console.WriteLine("Connected to " + connectionString + " successfully.");
        }
    }
    catch (TDengineError e)
    {
        // handle TDengine error
        Console.WriteLine("Failed to connect to " + connectionString + "; ErrCode:" + e.Code +
                          "; ErrMessage: " + e.Error);
        throw;
    }
    catch (Exception e)
    {
        // handle other exceptions
        Console.WriteLine("Failed to connect to " + connectionString + "; Err:" + e.Message);
        throw;
    }
}
```

1. 云服务连接
```csharp {wrap}
static void Main(string[] args)
{
    var connectionString = $"protocol=WebSocket;host=gw.cloud.taosdata.com;port=443;useSSL=true;token=xxxx;";
    // Connect to TDengine server using WebSocket
    var builder = new ConnectionStringBuilder(connectionString);
    try
    {
        // Open connection with using block, it will close the connection automatically
        using (var client = DbDriver.Open(builder))
        {
            Console.WriteLine("Connected to " + builder.ToString() + " successfully.");
        }
    }
    catch (TDengineError e)
    {
        // handle TDengine error
        Console.WriteLine("Failed to connect to " + builder.ToString() + "; ErrCode:" + e.Code +
                          "; ErrMessage: " + e.Error);
        throw;
    }
    catch (Exception e)
    {
        // handle other exceptions
        Console.WriteLine("Failed to connect to " + builder.ToString() + "; Err:" + e.Message);
        throw;
    }
}
```

#### 4.2.3 执行命令

`TDengineCommand` 是 `taos-connector-dotnet` 中用于执行 SQL 语句或存储过程的核心类。它实现了 `System.Data.Common.DbCommand` 接口，提供了查询执行、参数绑定等功能。
1. **SQL 执行**
  - 提供 `ExecuteNonQuery()` 方法，用于执行不返回结果集的 SQL 语句（如 `INSERT`、`UPDATE`、`DELETE`）。
  - 提供 `ExecuteDbDataReader()` 方法，用于执行查询并返回 `TDengineDataReader` 对象，以读取查询结果。
  - 提供 `ExecuteScalar()` 方法，用于执行查询并返回结果集中的第一行第一列的值。
1. **参数绑定**
  - 支持通过 `Parameters` 属性添加和管理 SQL 参数，避免 SQL 注入风险并提高查询性能。

#### 4.2.4 SQL 参数集合

`TDengineParameterCollection` 是 `taos-connector-dotnet` 中用于管理 SQL 参数集合的类。它实现了 `System.Data.Common.DbParameterCollection` 接口，提供了参数的添加、删除、访问和管理功能。
- `public int Add(object value)`
  - **接口说明**：添加参数。
  - **参数说明**：
    - `value`：参数值。
  - **返回值**：参数索引。
- `public void Clear()`
  - **接口说明**：清空参数。
- `public bool Contains(object value)`
  - **接口说明**：是否包含参数。
  - **参数说明**：
    - `value`：参数值。
  - **返回值**：是否包含参数。
- `public int IndexOf(object value)`
  - **接口说明**：获取参数索引。
  - **参数说明**：
    - `value`：参数值。
  - **返回值**：参数索引。
- `public void Insert(int index, object value)`
  - **接口说明**：插入参数。
  - **参数说明**：
    - `index`：索引。
    - `value`：参数值。
- `public void Remove(object value)`
  - **接口说明**：移除参数。
  - **参数说明**：
    - `value`：参数值。
- `public void RemoveAt(int index)`
  - **接口说明**：移除参数。
  - **参数说明**：
    - `index`：索引。
- `public void RemoveAt(string parameterName)`
  - **接口说明**：移除参数。
  - **参数说明**：
    - `parameterName`：参数名。
- `public int Count`
  - **接口说明**：获取参数数量。
  - **返回值**：参数数量。
- `public int IndexOf(string parameterName)`
  - **接口说明**：获取参数索引。
  - **参数说明**：
    - `parameterName`：参数名。
  - **返回值**：参数索引。
- `public bool Contains(string value)`
  - **接口说明**：是否包含参数。
  - **参数说明**：
    - `value`：参数名。
  - **返回值**：是否包含参数。
- `public void CopyTo(Array array, int index)`
  - **接口说明**：复制参数。
  - **参数说明**：
    - `array`：目标数组。
    - `index`：索引。
- `public IEnumerator GetEnumerator()`
  - **接口说明**：获取枚举器。
  - **返回值**：枚举器。
- `public void AddRange(Array values)`
  - **接口说明**：添加参数。
  - **参数说明**：
    - `values`：参数数组。
`TDengineParameter` 继承了 `DbParameter` 接口，支持以下功能：
- `public TDengineParameter(string name, object value)`
  - **接口说明**：TDengineParameter 构造函数。
  - **参数说明**：
    - `name`：参数名，需要以 @ 开头，如 @0、@1、@2 等。
    - `value`：参数值，需要 C# 列类型与 TDengine 列类型一一对应。
- `public string ParameterName`
  - **接口说明**：获取或设置参数名。
  - **返回值**：参数名。
- `public object Value`
  - **接口说明**：获取或设置参数值。
  - **返回值**：参数值。

#### 4.2.5 SQL 参数

`TDengineParameter` 是 `taos-connector-dotnet` 中用于表示 SQL 参数的类。它实现了 `System.Data.Common.DbParameter` 接口，提供了参数的名称、值等属性的管理功能。
构造方法
`public TDengineParameter(string name, object value)`
**接口说明**：TDengineParameter 构造函数。
**参数说明**：
`name`：参数名，需要以 @ 开头，如 @0、@1、@2 等。
`value`：参数值，需要 C# 列类型与 TDengine 列类型一一对应。

#### 4.2.6 查询结果

`TDengineDataReader` 是 `taos-connector-dotnet` 中用于读取查询结果的类。它实现了 `System.Data.Common.DbDataReader` 接口，提供了逐行读取数据、访问列值、获取元数据等功能。
- `public bool GetBoolean(int ordinal)`
  - **接口说明**：获取指定列的布尔值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：布尔值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public byte GetByte(int ordinal)`
  - **接口说明**：获取指定列的字节值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：字节值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)`
  - **接口说明**：获取指定列的字节值。
  - **参数说明**：
    - `ordinal`：列索引。
    - `dataOffset`：数据偏移量。
    - `buffer`：缓冲区。
    - `bufferOffset`：缓冲区偏移量。
    - `length`：长度。
  - **返回值**：字节值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public char GetChar(int ordinal)`
  - **接口说明**：获取指定列的字符值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：字符值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)`
  - **接口说明**：获取指定列的字符值。
  - **参数说明**：
    - `ordinal`：列索引。
    - `dataOffset`：数据偏移量。
    - `buffer`：缓冲区。
    - `bufferOffset`：缓冲区偏移量。
    - `length`：长度。
  - **返回值**：字符值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public DateTime GetDateTime(int ordinal)`
  - **接口说明**：获取指定列的日期时间值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：日期时间值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public double GetDouble(int ordinal)`
  - **接口说明**：获取指定列的双精度浮点数值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：双精度浮点数值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public float GetFloat(int ordinal)`
  - **接口说明**：获取指定列的单精度浮点数值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：单精度浮点数值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public short GetInt16(int ordinal)`
  - **接口说明**：获取指定列的 16 位整数值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：16 位整数值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public int GetInt32(int ordinal)`
  - **接口说明**：获取指定列的 32 位整数值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：32 位整数值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public long GetInt64(int ordinal)`
  - **接口说明**：获取指定列的 64 位整数值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：64 位整数值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public string GetString(int ordinal)`
  - **接口说明**：获取指定列的字符串值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：字符串值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public object GetValue(int ordinal)`
  - **接口说明**：获取指定列的值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： 结果对象。
- `public int GetValues(object[] values)`
  - **接口说明**：获取所有列的值。
  - **参数说明**：
    - `values`：值数组。
  - **返回值**：值数量。
- `public bool IsDBNull(int ordinal)`
  - **接口说明**：判断指定列是否为 NULL。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：是否为 NULL。
- `public int RecordsAffected`
  - **接口说明**：获取受影响的行数。
  - **返回值**：受影响的行数。
- `public bool HasRows`
  - **接口说明**：结果是否有行数据。
  - **返回值**：结果是否有行数据。
- `public bool Read()`
  - **接口说明**：读取下一行。
  - **返回值**：是否读取成功。
- `public IEnumerator GetEnumerator()`
  - **接口说明**：获取枚举器。
  - **返回值**：枚举器。
- `public void Close()`
  - **接口说明**：关闭结果集。

#### 4.2.7 样例

1. 创建数据库 power。
2. 切换数据库 power。
3. 创建表 meters。
4. 自动建表写入 d1001。
5. 绑定参数执行写入。
6. 执行查询 `SELECT * FROM meters`。
7. 读取结果。
原生连接样例：
```csharp {wrap}
using System;
using TDengine.Data.Client;

namespace NativeADO
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            const string connectionString = "host=localhost;port=6030;username=root;password=taosdata";
            using (var connection = new TDengineConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    using (var command = new TDengineCommand(connection))
                    {
                        command.CommandText = "create database power";
                        command.ExecuteNonQuery();
                        connection.ChangeDatabase("power");
                        command.CommandText =
                            "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))";
                        command.ExecuteNonQuery();
                        command.CommandText = "INSERT INTO " +
                                              "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
                                              "VALUES " +
                                              "(?,?,?,?)";
                        var parameters = command.Parameters;
                        parameters.Add(new TDengineParameter("@0", new DateTime(2023,10,03,14,38,05,000)));
                        parameters.Add(new TDengineParameter("@1", (float)10.30000));
                        parameters.Add(new TDengineParameter("@2", (int)219));
                        parameters.Add(new TDengineParameter("@3", (float)0.31000));
                        command.ExecuteNonQuery();
                        command.Parameters.Clear();
                        command.CommandText = "SELECT * FROM meters";
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine(
                                    $"{((DateTime) reader.GetValue(0)):yyyy-MM-dd HH:mm:ss.fff}, {reader.GetValue(1)}, {reader.GetValue(2)}, {reader.GetValue(3)}, {reader.GetValue(4)}, {System.Text.Encoding.UTF8.GetString((byte[]) reader.GetValue(5))}");
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }
    }
}
```

WS 连接样例：
```csharp {wrap}
using System;
using TDengine.Data.Client;

namespace WSADO
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            const string connectionString = "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata";
            using (var connection = new TDengineConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    using (var command = new TDengineCommand(connection))
                    {
                        command.CommandText = "create database power";
                        command.ExecuteNonQuery();
                        connection.ChangeDatabase("power");
                        command.CommandText =
                            "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))";
                        command.ExecuteNonQuery();
                        command.CommandText = "INSERT INTO " +
                                              "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
                                              "VALUES " +
                                              "(?,?,?,?)";
                        var parameters = command.Parameters;
                        parameters.Add(new TDengineParameter("@0", new DateTime(2023,10,03,14,38,05,000)));
                        parameters.Add(new TDengineParameter("@1", (float)10.30000));
                        parameters.Add(new TDengineParameter("@2", (int)219));
                        parameters.Add(new TDengineParameter("@3", (float)0.31000));
                        command.ExecuteNonQuery();
                        command.Parameters.Clear();
                        command.CommandText = "SELECT * FROM meters";
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine(
                                    $"{((DateTime) reader.GetValue(0)):yyyy-MM-dd HH:mm:ss.fff}, {reader.GetValue(1)}, {reader.GetValue(2)}, {reader.GetValue(3)}, {reader.GetValue(4)}, {System.Text.Encoding.UTF8.GetString((byte[]) reader.GetValue(5))}");
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }
    }
}
```

### 4.3 客户端接口

除了 ADO.NET 接口外`taos-connector-dotnet` 还提供了包括 schemaless、多行参数绑定等功能的全功能接口（命名空间 `TDengine.Driver.Client`）。此接口支持原生与 WebSocket 两种连接方式。

#### 4.3.1 TDengineClient

```csharp {wrap}
public interface ITDengineClient : IDisposable
{
    // stmt
    IStmt StmtInit();
    IStmt StmtInit(long reqId);
    // 执行查询
    IRows Query(string query);
    IRows Query(string query, long reqId);
    // 执行非查询接口
    long Exec(string query);
    long Exec(string query, long reqId);
    // schemaless 写入
    void SchemalessInsert(string[] lines, TDengineSchemalessProtocol protocol,
        TDengineSchemalessPrecision precision, int ttl, long reqId);
    // 连接状态是否正常
    bool ConnectionAvailable();
}

public enum TDengineSchemalessProtocol
{
    // 非法协议
    TSDB_SML_UNKNOWN_PROTOCOL = 0,
    // InfluxDB 行协议
    TSDB_SML_LINE_PROTOCOL = 1,
    // OpenTSDB 行协议
    TSDB_SML_TELNET_PROTOCOL = 2,
    // OpenTSDB JSON 协议
    TSDB_SML_JSON_PROTOCOL = 3
}

// schemaless 写入数据时间精度
public enum TDengineSchemalessPrecision
{
    // 未设置
    TSDB_SML_TIMESTAMP_NOT_CONFIGURED = 0,
    // 小时
    TSDB_SML_TIMESTAMP_HOURS = 1,
    // 分钟
    TSDB_SML_TIMESTAMP_MINUTES = 2,
    // 秒
    TSDB_SML_TIMESTAMP_SECONDS = 3,
    // 毫秒
    TSDB_SML_TIMESTAMP_MILLI_SECONDS = 4,
    // 微秒
    TSDB_SML_TIMESTAMP_MICRO_SECONDS = 5,
    // 纳秒
    TSDB_SML_TIMESTAMP_NANO_SECONDS = 6
}
```

#### 4.3.2 IStmt

```csharp {wrap}
public interface IStmt : IDisposable
{
    
    void Prepare(string query);
    bool IsInsert();
    void SetTableName(string tableName);
    void SetTags(object[] tags);
    TaosFieldE[] GetTagFields();
    TaosFieldE[] GetColFields();
    void BindRow(object[] row);
    void BindColumn( TaosFieldE[] fields,params Array[] arrays);
    void AddBatch();
    void Exec();
    long Affected();
    IRows Result();
}

public struct TaosFieldE
{
    [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 65)]
    public string name;

    public sbyte type;
    public byte precision;
    public byte scale;
    public int bytes;
}
```

- `IStmt StmtInit(long reqId)`
  - **接口说明**：初始化 statement 对象。
  - **参数说明**：
    - `reqId`：请求 ID。
  - **返回值**：实现 IStmt 接口的对象。
`IStmt` 接口提供了扩展的参数绑定接口。
- `void Prepare(string query)`
  - **接口说明**：准备 statement。
  - **参数说明**：
    - `query`：查询语句。
- `bool IsInsert()`
  - **接口说明**：判断是否为插入语句。
  - **返回值**：是否为插入语句。
- `void SetTableName(string tableName)`
  - **接口说明**：设置表名。
  - **参数说明**：
    - `tableName`：表名。
- `void SetTags(object[] tags)`
  - **接口说明**：设置标签。
  - **参数说明**：
    - `tags`：标签数组。
- `TaosFieldE[] GetTagFields()`
  - **接口说明**：获取标签属性。
  - **返回值**：标签属性数组。
- `TaosFieldE[] GetColFields()`
  - **接口说明**：获取列属性。
  - **返回值**：列属性数组。
- `void BindRow(object[] row)`
  - **接口说明**：绑定行。
  - **参数说明**：
    - `row`：行数据数组。
- `void BindColumn( TaosFieldE[] fields,params Array[] arrays)`
  - **接口说明**：绑定全部列。
  - **参数说明**：
    - `fields`：字段属性数组。
    - `arrays`：多列数据数组。
- `void AddBatch()`
  - **接口说明**：添加批处理。
- `void Exec()`
  - **接口说明**：执行参数绑定。
- `long Affected()`
  - **接口说明**：获取受影响的行数。
  - **返回值**：受影响的行数。
- `IRows Result()`
  - **接口说明**：获取结果。
  - **返回值**：结果对象。

#### 4.3.3 IRows

```csharp {wrap}
public interface IRows : IDisposable
{
    bool HasRows { get; }
    int AffectRows { get; }
    int FieldCount { get; }
    long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length);
    char GetChar(int ordinal);
    long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length);
    string GetDataTypeName(int ordinal);
    object GetValue(int ordinal);
    Type GetFieldType(int ordinal);
    int GetFieldSize(int ordinal);
    string GetName(int ordinal);
    int GetOrdinal(string name);
    bool Read();
    int GetFieldPrecision(int ordinal);
    int GetFieldScale(int ordinal);
    bool IsDBNull(int ordinal);
    byte GetByte(int ordinal);
    short GetInt16(int ordinal);
    int GetInt32(int ordinal);
    long GetInt64(int ordinal);
    bool GetBoolean(int ordinal);
    DateTime GetDateTime(int ordinal);
    decimal GetDecimal(int ordinal);
    double GetDouble(int ordinal);
    float GetFloat(int ordinal);
    string GetString(int ordinal);
    int GetValues(object[] values);
    DateTimeOffset GetDateTimeOffset(int ordinal);
}
```

- `public bool HasRows`
  - **接口说明**：结果是否有行数据。
  - **返回值**：结果是否有行数据。
- `public bool AffectRows`
  - **接口说明**：受影响行数。
  - **返回值**：受影响行数。
- `public long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)`
  - **接口说明**：获取指定列的字节值。
  - **参数说明**：
    - `ordinal`：列索引。
    - `dataOffset`：数据偏移量。
    - `buffer`：缓冲区。
    - `bufferOffset`：缓冲区偏移量。
    - `length`：长度。
  - **返回值**：字节值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public char GetChar(int ordinal)`
  - **接口说明**：获取指定列的字符值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：字符值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)`
  - **接口说明**：获取指定列的字符值。
  - **参数说明**：
    - `ordinal`：列索引。
    - `dataOffset`：数据偏移量。
    - `buffer`：缓冲区。
    - `bufferOffset`：缓冲区偏移量。
    - `length`：长度。
  - **返回值**：字符值。
  - **异常**：类型不对应抛出 `InvalidCastException` 异常。
- `public string GetDataTypeName(int ordinal)`
  - **接口说明**：获取指定列的数据类型名称。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：数据类型名称。
- `public object GetValue(int ordinal)`
  - **接口说明**：获取指定列的值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： 结果对象。
- `public Type GetFieldType(int ordinal)`
  - **接口说明**：获取指定列的数据类型。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：数据类型。
- `public int GetFieldSize(int ordinal)`
  - **接口说明**：获取指定列的大小。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：列大小。
- `public string GetName(int ordinal)`
  - **接口说明**：获取指定列的名称。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**：列名称。
- `public int GetOrdinal(string name)`
  - **接口说明**：获取指定列的索引。
  - **参数说明**：
    - `name`：列名称。
  - **返回值**：列索引。
- `public bool Read()`
  - **接口说明**：读取下一行。
  - **返回值**：是否读取成功。
- `public int GetFieldPrecision(int ordinal)`
  - **接口说明**：获取指定列的精度（decimal 类型使用）。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： 精度。
- `public int GetFieldScale(int ordinal)`
  - **接口说明**：获取指定列的小数位数（decimal 类型使用）。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： 小数位数。
- `public bool IsDBNull(int ordinal)`
  - **接口说明**：获取指定列的值是否为 null。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： 是否为 null。
- `public byte GetByte(int ordinal)`
  - **接口说明**：获取指定列的 byte 值(uint8)。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： byte 值。
- `public short GetInt16(int ordinal)`
  - **接口说明**：获取指定列的 int16 值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： int16 值。
- `public int GetInt32(int ordinal)`
  - **接口说明**：获取指定列的 int32 值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： int32 值。
- `public int GetInt64(int ordinal)`
  - **接口说明**：获取指定列的 int64 值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： int64 值。
- `public bool GetBoolean(int ordinal)`
  - **接口说明**：获取指定列的 bool 值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： bool 值。
- `public DateTime GetDateTime(int ordinal)`
  - **接口说明**：获取指定列的 DateTime 值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： DateTime 值。
- `public decimal GetDecimal(int ordinal)`
  - **接口说明**：获取指定列的 Decimal 值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： Decimal 值。
- `double GetDouble(int ordinal)`
  - **接口说明**：获取指定列的 Decimal 值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： Decimal 值。
- `float GetFloat(int ordinal)`
  - **接口说明**：获取指定列的 float 值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： float 值。
- `string GetString(int ordinal)`
  - **接口说明**：获取指定列的字符串值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： 字符串值。
- `int GetValues(object[] values)`
  - **接口说明**：获取当前行的值。
  - **参数说明**：
    - `values`：要填充的对象数组。
  - **返回值**： 填充个数。
- `DateTimeOffset GetDateTimeOffset(int ordinal)`
  - **接口说明**：获取指定列的 DateTimeOffset 值。
  - **参数说明**：
    - `ordinal`：列索引。
  - **返回值**： DateTimeOffset 值。

#### 4.3.4 样例

1. 执行 SQL
   - 原生连接
    ```csharp {wrap}
    public static void Main(string[] args)
    {
        var builder = new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
        using (var client = DbDriver.Open(builder))
        {
            try
            {
                client.Exec($"create database power");
                client.Exec($"use power");
                client.Exec(
                    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                string insertQuery =
                    "INSERT INTO " +
                    "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
                    "VALUES " +
                    "('2023-10-03 14:38:05.000', 10.30000, 219, 0.31000) " +
                    "('2023-10-03 14:38:15.000', 12.60000, 218, 0.33000) " +
                    "('2023-10-03 14:38:16.800', 12.30000, 221, 0.31000) " +
                    "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
                    "VALUES " +
                    "('2023-10-03 14:38:16.650', 10.30000, 218, 0.25000) " +
                    "power.d1003 USING power.meters TAGS(2,'California.LosAngeles') " +
                    "VALUES " +
                    "('2023-10-03 14:38:05.500', 11.80000, 221, 0.28000) " +
                    "('2023-10-03 14:38:16.600', 13.40000, 223, 0.29000) " +
                    "power.d1004 USING power.meters TAGS(3,'California.LosAngeles') " +
                    "VALUES " +
                    "('2023-10-03 14:38:05.000', 10.80000, 223, 0.29000) " +
                    "('2023-10-03 14:38:06.500', 11.50000, 221, 0.35000)";
                client.Exec(insertQuery);
                string query = "SELECT * FROM meters";
                using (var rows = client.Query(query))
                {
                    while (rows.Read())
                    {
                        Console.WriteLine(
                            $"{((DateTime)rows.GetValue(0)):yyyy-MM-dd HH:mm:ss.fff}, {rows.GetValue(1)}, {rows.GetValue(2)}, {rows.GetValue(3)}, {rows.GetValue(4)}, {Encoding.UTF8.GetString((byte[])rows.GetValue(5))}");
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                throw;
            }
        }
    }
    ```

   - WebSocket 连接
    ```csharp {wrap}
    public static void Main(string[] args)
    {
        var builder = new ConnectionStringBuilder("protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
        using (var client = DbDriver.Open(builder))
        {
            try
            {
                client.Exec($"create database power");
                client.Exec($"use power");
                client.Exec(
                    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                string insertQuery =
                    "INSERT INTO " +
                    "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
                    "VALUES " +
                    "('2023-10-03 14:38:05.000', 10.30000, 219, 0.31000) " +
                    "('2023-10-03 14:38:15.000', 12.60000, 218, 0.33000) " +
                    "('2023-10-03 14:38:16.800', 12.30000, 221, 0.31000) " +
                    "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
                    "VALUES " +
                    "('2023-10-03 14:38:16.650', 10.30000, 218, 0.25000) " +
                    "power.d1003 USING power.meters TAGS(2,'California.LosAngeles') " +
                    "VALUES " +
                    "('2023-10-03 14:38:05.500', 11.80000, 221, 0.28000) " +
                    "('2023-10-03 14:38:16.600', 13.40000, 223, 0.29000) " +
                    "power.d1004 USING power.meters TAGS(3,'California.LosAngeles') " +
                    "VALUES " +
                    "('2023-10-03 14:38:05.000', 10.80000, 223, 0.29000) " +
                    "('2023-10-03 14:38:06.500', 11.50000, 221, 0.35000)";
                client.Exec(insertQuery);
                string query = "SELECT * FROM meters";
                using (var rows = client.Query(query))
                {
                    while (rows.Read())
                    {
                        Console.WriteLine(
                            $"{((DateTime)rows.GetValue(0)):yyyy-MM-dd HH:mm:ss.fff}, {rows.GetValue(1)}, {rows.GetValue(2)}, {rows.GetValue(3)}, {rows.GetValue(4)}, {Encoding.UTF8.GetString((byte[])rows.GetValue(5))}");
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                throw;
            }
        }
    }
    ```

1. 参数绑定
   - 原生连接
    ```csharp {wrap}
    public static void Main(string[] args)
    {
        var builder = new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
        using (var client = DbDriver.Open(builder))
        {
            try
            {
                client.Exec($"create database power");
                client.Exec(
                    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                using (var stmt = client.StmtInit())
                {
                    stmt.Prepare(
                        "Insert into power.d1001 using power.meters tags(2,'California.SanFrancisco') values(?,?,?,?)");
                    var ts = new DateTime(2023, 10, 03, 14, 38, 05, 000);
                    stmt.BindRow(new object[] { ts, (float)10.30000, (int)219, (float)0.31000 });
                    stmt.AddBatch();
                    stmt.Exec();
                    var affected = stmt.Affected();
                    Console.WriteLine($"affected rows: {affected}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
    ```

   - WebSocket 连接
    ```csharp {wrap}
    public static void Main(string[] args)
    {
        var builder =
            new ConnectionStringBuilder(
                "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
        using (var client = DbDriver.Open(builder))
        {
            try
            {
                client.Exec($"create database power");
                client.Exec(
                    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                using (var stmt = client.StmtInit())
                {
                    stmt.Prepare(
                        "Insert into power.d1001 using power.meters tags(2,'California.SanFrancisco') values(?,?,?,?)");
                    var ts = new DateTime(2023, 10, 03, 14, 38, 05, 000);
                    stmt.BindRow(new object[] { ts, (float)10.30000, (int)219, (float)0.31000 });
                    stmt.AddBatch();
                    stmt.Exec();
                    var affected = stmt.Affected();
                    Console.WriteLine($"affected rows: {affected}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
    ```

1. 无模式写入
   - 原生连接
    ```csharp {wrap}
    public static void Main(string[] args)
    {
        var builder =
            new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
        using (var client = DbDriver.Open(builder))
        {
            client.Exec("create database sml");
            client.Exec("use sml");
            var influxDBData =
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000";
            client.SchemalessInsert(new string[] { influxDBData },
                TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL,
                TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NANO_SECONDS, 0, ReqId.GetReqId());
            var telnetData = "stb0_0 1626006833 4 host=host0 interface=eth0";
            client.SchemalessInsert(new string[] { telnetData },
                TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS, 0, ReqId.GetReqId());
            var jsonData =
                "{\"metric\": \"meter_current\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}";
            client.SchemalessInsert(new string[] { jsonData }, TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL,
                TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS, 0, ReqId.GetReqId());
        }
    }
    ```

   - WebSocket 连接
    ```csharp {wrap}
    public static void Main(string[] args)
    {
        var builder =
            new ConnectionStringBuilder("protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
        using (var client = DbDriver.Open(builder))
        {
            client.Exec("create database sml");
            client.Exec("use sml");
            var influxDBData =
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000";
            client.SchemalessInsert(new string[] { influxDBData },
                TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL,
                TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NANO_SECONDS, 0, ReqId.GetReqId());
            var telnetData = "stb0_0 1626006833 4 host=host0 interface=eth0";
            client.SchemalessInsert(new string[] { telnetData },
                TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS, 0, ReqId.GetReqId());
            var jsonData =
                "{\"metric\": \"meter_current\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}";
            client.SchemalessInsert(new string[] { jsonData }, TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL,
                TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS, 0, ReqId.GetReqId());
        }
    }
    ```

### 4.4 TMQ订阅

`ConsumerBuilder` 类提供了消费者构建相关接口，`ConsumeResult` 类提供了消费结果相关接口，`TopicPartitionOffset` 类提供了分区偏移量相关接口。`ReferenceDeserializer` 和 `DictionaryDeserializer` 提供了反序列化的支持。

#### 4.4.1 消费者

`public ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)`
**接口说明**：ConsumerBuilder 构造函数。
**参数说明**：
`config`：消费配置。
创建消费者支持属性列表：
- `useSSL`：是否使用 SSL 连接，默认为 false。
- `token`：连接 TDengine cloud 的 token。
- `ws.message.enableCompression`：是否启用 WebSocket 压缩，默认为 false。
- `ws.autoReconnect`：是否自动重连，默认为 false。
- `ws.reconnect.retry.count`：重连次数，默认为 3。
- `ws.reconnect.interval.ms`：重连间隔毫秒时间，默认为 2000。
- `connectionTimezone`:连接时区
- 其他参数同 TDengine TMQ 订阅
- `public ConsumerBuilder<TValue> SetValueDeserializer(IDeserializer<TValue> deserializer)`
  - **接口说明**：ConsumerBuilder 设置反序列化器。
- `public IConsumer<TValue> Build()`
  - **接口说明**：构建消费者。
  - **返回值**：消费者对象。
`IConsumer` 接口提供了消费者相关 API：
- `ConsumeResult<TValue> Consume(int millisecondsTimeout)`
  - **接口说明**：消费消息。
  - **参数说明**：
    - `millisecondsTimeout`：毫秒超时时间。
  - **返回值**：消费结果。
- `List<TopicPartition> Assignment { get; }`
  - **接口说明**：获取分配信息。
  - **返回值**：分配信息。
- `List<string> Subscription()`
  - **接口说明**：获取订阅的主题。
  - **返回值**：主题列表。
- `void Subscribe(IEnumerable<string> topic)`
  - **接口说明**：订阅主题列表。
  - **参数说明**：
    - `topic`：主题列表。
- `void Subscribe(string topic)`
  - **接口说明**：订阅单个主题。
  - **参数说明**：
    - `topic`：主题。
- `void Unsubscribe()`
  - **接口说明**：取消订阅。
- `void Commit(ConsumeResult<TValue> consumerResult)`
  - **接口说明**：提交消费结果。
  - **参数说明**：
    - `consumerResult`：消费结果。
- `List<TopicPartitionOffset> Commit()`
  - **接口说明**：提交全部消费结果。
  - **返回值**：分区偏移量。
- `void Commit(IEnumerable<TopicPartitionOffset> offsets)`
  - **接口说明**：提交消费结果。
  - **参数说明**：
    - `offsets`：分区偏移量。
- `void Seek(TopicPartitionOffset tpo)`
  - **接口说明**：跳转到分区偏移量。
  - **参数说明**：
    - `tpo`：分区偏移量。
- `List<TopicPartitionOffset> Committed(TimeSpan timeout)`
  - **接口说明**：获取分区偏移量。
  - **参数说明**：
    - `timeout`：超时时间(未使用)。
  - **返回值**：分区偏移量。
- `List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)`
  - **接口说明**：获取指定分区偏移量。
  - **参数说明**：
    - `partitions`：分区列表。
    - `timeout`：超时时间(未使用)。
  - **返回值**：分区偏移量。
- `Offset Position(TopicPartition partition)`
  - **接口说明**：获取消费位置。
  - **参数说明**：
    - `partition`：分区。
  - **返回值**：偏移量。
- `void Close()`
  - **接口说明**：关闭消费者。

#### 4.4.2 消费记录

`ConsumeResult` 类提供了消费结果相关接口：
- `public List<TmqMessage<TValue>> Message`
  - **接口说明**：获取消息列表。
  - **返回值**：消息列表。
`TmqMessage` 类提供了消息具体内容：
```csharp
    public class TmqMessage<TValue>
    {
        public string TableName { get; set; }
        public TValue Value { get; set; }
    }
```

- `TableName`：表名
- `Value`：消息内容

#### 4.4.3 分区信息

从 `ConsumeResult` 获取 `TopicPartitionOffset`：
```csharp
public TopicPartitionOffset TopicPartitionOffset
```

`TopicPartitionOffset` 类提供了获取分区信息的接口：
- `public string Topic { get; }`
  - **接口说明**：获取主题。
  - **返回值**：主题。
- `public Partition Partition { get; }`
  - **接口说明**：获取分区。
  - **返回值**：分区。
- `public Offset Offset { get; }`
  - **接口说明**：获取偏移量。
  - **返回值**：偏移量。
- `public TopicPartition TopicPartition`
  - **接口说明**：获取主题分区。
  - **返回值**：主题分区。
- `public string ToString()`
  - **接口说明**：转换为字符串。
  - **返回值**：字符串信息。

#### 4.4.4 偏移量元数据

`Offset` 类提供了偏移量相关接口：
- `public long Value`
  - **接口说明**：获取偏移量值。
  - **返回值**：偏移量值。

#### 4.4.5 反序列化器

C# 驱动提供了两个反序列化类：`ReferenceDeserializer` 和 `DictionaryDeserializer`。它们都实现了 `IDeserializer` 接口，如果要实现自定义反序列化方法需要实现 `IDeserializer` 接口并在 `ConsumerBuilder` 调用 `SetValueDeserializer` 设置 TMQ 使用的解析器。
1. ReferenceDeserializer 用来将消费到的一条记录反序列化为一个对象，需要保证对象类的属性名与消费到的数据的列名能够对应，且类型能够匹配。
2. `DictionaryDeserializer`则会将消费到的一行数据反序列化为一个 `Dictionary<string, object>` 对象，其 key 为列名，值为对象。
```csharp {wrap}
public interface IDeserializer<T>
{
    T Deserialize(ITMQRows data, bool isNull, SerializationContext context);
}
```

#### 4.4.6 样例

1. 原生连接 
  ```csharp {wrap}
  using System;
  using System.Collections.Generic;
  using System.Threading.Tasks;
  using TDengine.Driver;
  using TDengine.Driver.Client;
  using TDengine.TMQ;
  
  namespace TMQExample
  {
      internal class SubscribeDemo
      {
          private static string _host = "";
          private static string _groupId = "";
          private static string _clientId = "";
          private static string _topic = "";
  
          public static void Main(string[] args)
          {
              try
              {
                  var builder = new ConnectionStringBuilder("host=127.0.0.1;port=6030;username=root;password=taosdata");
                  using (var client = DbDriver.Open(builder))
                  {
                      client.Exec("CREATE DATABASE IF NOT EXISTS power");
                      client.Exec("USE power");
                      client.Exec(
                          "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                      client.Exec("CREATE TOPIC IF NOT EXISTS topic_meters as SELECT * from power.meters");
                      var consumer = CreateConsumer();
                      // insert data
                      Task.Run(InsertData);
                      // consume message
                      Consume(consumer);
                      // seek
                      Seek(consumer);
                      // commit
                      CommitOffset(consumer);
                      // close
                      Close(consumer);
                      Console.WriteLine("Done");
                  }
              }
              catch (TDengineError e)
              {
                  // handle TDengine error
                  Console.WriteLine(e.Message);
                  throw;
              }
              catch (Exception e)
              {
                  // handle other exceptions
                  Console.WriteLine(e.Message);
                  throw;
              }
          }
  
          static void InsertData()
          {
              var builder = new ConnectionStringBuilder("host=127.0.0.1;port=6030;username=root;password=taosdata");
              using (var client = DbDriver.Open(builder))
              {
                  while (true)
                  {
                      client.Exec(
                          "INSERT into power.d1001 using power.meters tags(2,'California.SanFrancisco') values(now,11.5,219,0.30)");
                      Task.Delay(1000).Wait();
                  }
              }
          }
  
          static IConsumer<Dictionary<string, object>> CreateConsumer()
          {
              // consumer config
              _host = "127.0.0.1";
              _groupId = "group1";
              _clientId = "client1";
              var cfg = new Dictionary<string, string>()
              {
                  { "td.connect.port", "6030" },
                  { "auto.offset.reset", "latest" },
                  { "msg.with.table.name", "true" },
                  { "enable.auto.commit", "true" },
                  { "auto.commit.interval.ms", "1000" },
                  { "group.id", _groupId },
                  { "client.id", _clientId },
                  { "td.connect.ip", _host },
                  { "td.connect.user", "root" },
                  { "td.connect.pass", "taosdata" },
              };
              IConsumer<Dictionary<string, object>> consumer = null!;
              try
              {
                  // create consumer
                  consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                  Console.WriteLine(
                      $"Create consumer successfully, " +
                      $"host: {_host}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}");
              }
              catch (TDengineError e)
              {
                  // handle TDengine error
                  Console.WriteLine(
                      $"Failed to create native consumer, " +
                      $"host: {_host}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"ErrCode: {e.Code}, " +
                      $"ErrMessage: {e.Error}");
                  throw;
              }
              catch (Exception e)
              {
                  // handle other exceptions
                  Console.WriteLine(
                      $"Failed to create native consumer, " +
                      $"host: {_host}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"ErrMessage: {e.Message}");
                  throw;
              }
  
              return consumer;
          }
  
          static void Consume(IConsumer<Dictionary<string, object>> consumer)
          {
              _topic = "topic_meters";
              try
              {
                  // subscribe
                  consumer.Subscribe(new List<string>() { _topic });
                  Console.WriteLine("Subscribe topics successfully");
                  for (int i = 0; i < 50; i++)
                  {
                      // consume message with using block to ensure the result is disposed
                      using (var cr = consumer.Consume(100))
                      {
                          if (cr == null) continue;
                          foreach (var message in cr.Message)
                          {
                              // handle message
                              Console.WriteLine(
                                  $"data: {{{((DateTime)message.Value["ts"]).ToString("yyyy-MM-dd HH:mm:ss.fff")}, " +
                                  $"{message.Value["current"]}, {message.Value["voltage"]}, {message.Value["phase"]}}}");
                          }
                      }
                  }
              }
              catch (TDengineError e)
              {
                  // handle TDengine error
                  Console.WriteLine(
                      $"Failed to poll data, " +
                      $"topic: {_topic}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"ErrCode: {e.Code}, " +
                      $"ErrMessage: {e.Error}");
                  throw;
              }
              catch (Exception e)
              {
                  // handle other exceptions
                  Console.WriteLine(
                      $"Failed to poll data, " +
                      $"topic: {_topic}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"ErrMessage: {e.Message}");
                  throw;
              }
          }
  
          static void Seek(IConsumer<Dictionary<string, object>> consumer)
          {
              try
              {
                  // get assignment
                  var assignment = consumer.Assignment;
                  Console.WriteLine($"Now assignment: {assignment}");
                  // seek to the beginning
                  foreach (var topicPartition in assignment)
                  {
                      consumer.Seek(new TopicPartitionOffset(topicPartition.Topic, topicPartition.Partition, 0));
                  }
  
                  Console.WriteLine("Assignment seek to beginning successfully");
              }
              catch (TDengineError e)
              {
                  // handle TDengine error
                  Console.WriteLine(
                      $"Failed to seek offset, " +
                      $"topic: {_topic}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"offset: 0, " +
                      $"ErrCode: {e.Code}, " +
                      $"ErrMessage: {e.Error}");
                  throw;
              }
              catch (Exception e)
              {
                  // handle other exceptions
                  Console.WriteLine(
                      $"Failed to seek offset, " +
                      $"topic: {_topic}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"offset: 0, " +
                      $"ErrMessage: {e.Message}");
                  throw;
              }
          }
  
          static void CommitOffset(IConsumer<Dictionary<string, object>> consumer)
          {
              for (int i = 0; i < 5; i++)
              {
                  TopicPartitionOffset topicPartitionOffset = null;
                  try
                  {
                      // consume message with using block to ensure the result is disposed
                      using (var cr = consumer.Consume(100))
                      {
                          if (cr == null) continue;
                          // commit offset
                          topicPartitionOffset = cr.TopicPartitionOffset;
                          consumer.Commit(new List<TopicPartitionOffset>
                          {
                              topicPartitionOffset,
                          });
                          Console.WriteLine("Commit offset manually successfully.");
                      }
                  }
                  catch (TDengineError e)
                  {
                      // handle TDengine error
                      Console.WriteLine(
                          $"Failed to commit offset, " +
                          $"topic: {_topic}, " +
                          $"groupId: {_groupId}, " +
                          $"clientId: {_clientId}, " +
                          $"offset: {topicPartitionOffset}, " +
                          $"ErrCode: {e.Code}, " +
                          $"ErrMessage: {e.Error}");
                      throw;
                  }
                  catch (Exception e)
                  {
                      // handle other exceptions
                      Console.WriteLine(
                          $"Failed to commit offset, " +
                          $"topic: {_topic}, " +
                          $"groupId: {_groupId}, " +
                          $"clientId: {_clientId}, " +
                          $"offset: {topicPartitionOffset}, " +
                          $"ErrMessage: {e.Message}");
                      throw;
                  }
              }
          }
  
          static void Close(IConsumer<Dictionary<string, object>> consumer)
          {
              try
              {
                  // unsubscribe
                  consumer.Unsubscribe();
              }
              catch (TDengineError e)
              {
                  // handle TDengine error
                  Console.WriteLine(
                      $"Failed to unsubscribe consumer, " +
                      $"topic: {_topic}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"ErrCode: {e.Code}, " +
                      $"ErrMessage: {e.Error}");
                  throw;
              }
              catch (Exception e)
              {
                  // handle other exceptions
                  Console.WriteLine(
                      $"Failed to execute commit example, " +
                      $"topic: {_topic}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"ErrMessage: {e.Message}");
                  throw;
              }
              finally
              {
                  // close consumer
                  consumer.Close();
                  Console.WriteLine("Consumer closed successfully.");
              }
          }
      }
  }
  ```

1. WebSocket 连接
  ```csharp {wrap}
  using System;
  using System.Collections.Generic;
  using System.Threading.Tasks;
  using TDengine.Driver;
  using TDengine.Driver.Client;
  using TDengine.TMQ;
  
  namespace TMQExample
  {
      internal class SubscribeDemo
      {
          private static string _host = "";
          private static string _groupId = "";
          private static string _clientId = "";
          private static string _topic = "";
  
          public static void Main(string[] args)
          {
              try
              {
                  var builder =
                      new ConnectionStringBuilder(
                          "protocol=WebSocket;host=127.0.0.1;port=6041;username=root;password=taosdata");
                  using (var client = DbDriver.Open(builder))
                  {
                      client.Exec("CREATE DATABASE IF NOT EXISTS power");
                      client.Exec("USE power");
                      client.Exec(
                          "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                      client.Exec("CREATE TOPIC IF NOT EXISTS topic_meters as SELECT * from power.meters");
                      var consumer = CreateConsumer();
                      // insert data
                      Task.Run(InsertData);
                      // consume message
                      Consume(consumer);
                      // seek
                      Seek(consumer);
                      // commit
                      CommitOffset(consumer);
                      // close
                      Close(consumer);
                      Console.WriteLine("Done");
                  }
              }
              catch (TDengineError e)
              {
                  // handle TDengine error
                  Console.WriteLine(e.Message);
                  throw;
              }
              catch (Exception e)
              {
                  // handle other exceptions
                  Console.WriteLine(e.Message);
                  throw;
              }
          }
  
          static void InsertData()
          {
              var builder =
                  new ConnectionStringBuilder(
                      "protocol=WebSocket;host=127.0.0.1;port=6041;username=root;password=taosdata");
              using (var client = DbDriver.Open(builder))
              {
                  while (true)
                  {
                      client.Exec(
                          "INSERT into power.d1001 using power.meters tags(2,'California.SanFrancisco') values(now,11.5,219,0.30)");
                      Task.Delay(1000).Wait();
                  }
              }
          }
  
          static IConsumer<Dictionary<string, object>> CreateConsumer()
          {
              // consumer config
              _host = "127.0.0.1";
              _groupId = "group1";
              _clientId = "client1";
              var cfg = new Dictionary<string, string>()
              {
                  { "td.connect.type", "WebSocket" },
                  { "td.connect.port", "6041" },
                  { "auto.offset.reset", "latest" },
                  { "msg.with.table.name", "true" },
                  { "enable.auto.commit", "true" },
                  { "auto.commit.interval.ms", "1000" },
                  { "group.id", _groupId },
                  { "client.id", _clientId },
                  { "td.connect.ip", _host },
                  { "td.connect.user", "root" },
                  { "td.connect.pass", "taosdata" },
              };
              IConsumer<Dictionary<string, object>> consumer = null!;
              try
              {
                  // create consumer
                  consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                  Console.WriteLine(
                      $"Create consumer successfully, " +
                      $"host: {_host}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}");
              }
              catch (TDengineError e)
              {
                  // handle TDengine error
                  Console.WriteLine(
                      $"Failed to create native consumer, " +
                      $"host: {_host}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"ErrCode: {e.Code}, " +
                      $"ErrMessage: {e.Error}");
                  throw;
              }
              catch (Exception e)
              {
                  // handle other exceptions
                  Console.WriteLine(
                      $"Failed to create native consumer, " +
                      $"host: {_host}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"ErrMessage: {e.Message}");
                  throw;
              }
  
              return consumer;
          }
  
          static void Consume(IConsumer<Dictionary<string, object>> consumer)
          {
              _topic = "topic_meters";
              try
              {
                  // subscribe
                  consumer.Subscribe(new List<string>() { _topic });
                  Console.WriteLine("Subscribe topics successfully");
                  for (int i = 0; i < 50; i++)
                  {
                      // consume message with using block to ensure the result is disposed
                      using (var cr = consumer.Consume(100))
                      {
                          if (cr == null) continue;
                          foreach (var message in cr.Message)
                          {
                              // handle message
                              Console.WriteLine(
                                  $"data: {{{((DateTime)message.Value["ts"]).ToString("yyyy-MM-dd HH:mm:ss.fff")}, " +
                                  $"{message.Value["current"]}, {message.Value["voltage"]}, {message.Value["phase"]}}}");
                          }
                      }
                  }
              }
              catch (TDengineError e)
              {
                  // handle TDengine error
                  Console.WriteLine(
                      $"Failed to poll data, " +
                      $"topic: {_topic}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"ErrCode: {e.Code}, " +
                      $"ErrMessage: {e.Error}");
                  throw;
              }
              catch (Exception e)
              {
                  // handle other exceptions
                  Console.WriteLine($"Failed to poll data, " +
                                    $"topic: {_topic}, " +
                                    $"groupId: {_groupId}, " +
                                    $"clientId: {_clientId}, " +
                                    $"ErrMessage: {e.Message}");
                  throw;
              }
          }
  
          static void Seek(IConsumer<Dictionary<string, object>> consumer)
          {
              try
              {
                  // get assignment
                  var assignment = consumer.Assignment;
                  Console.WriteLine($"Now assignment: {assignment}");
                  // seek to the beginning
                  foreach (var topicPartition in assignment)
                  {
                      consumer.Seek(new TopicPartitionOffset(topicPartition.Topic, topicPartition.Partition, 0));
                  }
  
                  Console.WriteLine("Assignment seek to beginning successfully");
              }
              catch (TDengineError e)
              {
                  // handle TDengine error
                  Console.WriteLine(
                      $"Failed to seek offset, " +
                      $"topic: {_topic}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"offset: 0, " +
                      $"ErrCode: {e.Code}, " +
                      $"ErrMessage: {e.Error}");
                  throw;
              }
              catch (Exception e)
              {
                  // handle other exceptions
                  Console.WriteLine(
                      $"Failed to seek offset, " +
                      $"topic: {_topic}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"offset: 0, " +
                      $"ErrMessage: {e.Message}");
                  throw;
              }
          }
  
          static void CommitOffset(IConsumer<Dictionary<string, object>> consumer)
          {
              for (int i = 0; i < 5; i++)
              {
                  TopicPartitionOffset topicPartitionOffset = null;
                  try
                  {
                      // consume message with using block to ensure the result is disposed
                      using (var cr = consumer.Consume(100))
                      {
                          if (cr == null) continue;
                          // commit offset
                          topicPartitionOffset = cr.TopicPartitionOffset;
                          consumer.Commit(new List<TopicPartitionOffset>
                          {
                              topicPartitionOffset,
                          });
                          Console.WriteLine("Commit offset manually successfully.");
                      }
                  }
                  catch (TDengineError e)
                  {
                      // handle TDengine error
                      Console.WriteLine(
                          $"Failed to commit offset, " +
                          $"topic: {_topic}, " +
                          $"groupId: {_groupId}, " +
                          $"clientId: {_clientId}, " +
                          $"offset: {topicPartitionOffset}, " +
                          $"ErrCode: {e.Code}, " +
                          $"ErrMessage: {e.Error}");
                      throw;
                  }
                  catch (Exception e)
                  {
                      // handle other exceptions
                      Console.WriteLine(
                          $"Failed to commit offset, " +
                          $"topic: {_topic}, " +
                          $"groupId: {_groupId}, " +
                          $"clientId: {_clientId}, " +
                          $"offset: {topicPartitionOffset}, " +
                          $"ErrMessage: {e.Message}");
                      throw;
                  }
              }
          }
  
          static void Close(IConsumer<Dictionary<string, object>> consumer)
          {
              try
              {
                  // unsubscribe
                  consumer.Unsubscribe();
              }
              catch (TDengineError e)
              {
                  // handle TDengine error
                  Console.WriteLine(
                      $"Failed to unsubscribe consumer, " +
                      $"topic: {_topic}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"ErrCode: {e.Code}, " +
                      $"ErrMessage: {e.Error}");
                  throw;
              }
              catch (Exception e)
              {
                  // handle other exceptions
                  Console.WriteLine(
                      $"Failed to execute commit example, " +
                      $"topic: {_topic}, " +
                      $"groupId: {_groupId}, " +
                      $"clientId: {_clientId}, " +
                      $"ErrMessage: {e.Message}");
                  throw;
              }
              finally
              {
                  // close consumer
                  consumer.Close();
                  Console.WriteLine("Consumer closed successfully.");
              }
          }
      }
  }
  ```

## 5. 安全特性

### 5.1 身份认证

1. **用户名密码认证**：通过 `username` 和 `password` 参数进行身份验证，适用于原生和 WebSocket 连接。
2. **Token 认证**：通过 `token` 参数进行 TDengine 云服务认证，仅适用于 WebSocket 连接。
3. **凭证安全存储**：
  - **不建议**：在代码中硬编码密码或 Token。
  - **建议**：从环境变量、加密配置文件或密钥管理服务读取凭证。
    示例：
  ```csharp
  // 不安全：硬编码密码
  var connStr = "host=localhost;username=root;password=taosdata";
  
  // 安全：从环境变量读取
  var password = Environment.GetEnvironmentVariable("TDENGINE_PASSWORD");
  var connStr = $"host=localhost;username=root;password={password}";
  ```

### 5.2 传输安全

1. **WSS 加密连接**：WebSocket 达连支持 WSS 协议，通过设置 `useSSL=true` 启用。当启用 SSL 时，默认端口为 443。
  示例：
  ```csharp
  var builder = new ConnectionStringBuilder(
      "protocol=WebSocket;host=myserver.com;useSSL=true;username=root;password=xxx");
  ```

1. **数据压缩**：启用 `enableCompression=true` 可对 WebSocket 传输数据进行压缩，减少传输带宽消耗。
  **注意**：数据压缩仅在 .NET 6.0 及以上版本中支持。
1. **证书验证**：使用 WSS 连接时，.NET 框架默认启用服务端证书验证。如需使用自签名证书，需在应用程序层面配置证书验证回调。

### 5.3 SQL 注入防护

1. **参数绑定**：使用 `TDengineParameter` 进行参数绑定，避免 SQL 注入风险。
  不安全示例（字符串拼接）：
  ```csharp
  // ✗ 不安全：字符串拼接 SQL
  string userInput = GetUserInput();
  string sql = $"SELECT * FROM meters WHERE location = '{userInput}'";
  command.CommandText = sql;
  command.ExecuteNonQuery();
  ```

  安全示例（参数绑定）：
  ```csharp
  // ✓ 安全：使用参数绑定
  string userInput = GetUserInput();
  command.CommandText = "SELECT * FROM meters WHERE location = @0";
  command.Parameters.Add(new TDengineParameter("@0", userInput));
  var reader = command.ExecuteReader();
  ```

1. **参数名称验证**：参数名称必须以特定前缀开头：
  - `@` - 数据列参数
  - `$` - 标签参数
  - `#` - 表名参数
    违反规则将抛出 `ArgumentException` 异常，防止参数混淆。

### 5.4 资源管理

1. **自动资源释放**：所有主要类（`TDengineConnection`、`TDengineCommand`、`TDengineDataReader`、`IStmt`、`IRows`）均实现 `IDisposable` 接口，支持 `using` 语句自动资源释放。
  示例：
  ```csharp
  using (var connection = new TDengineConnection(connStr))
  {
      connection.Open();
      using (var command = connection.CreateCommand())
      {
          command.CommandText = "SELECT * FROM meters";
          using (var reader = command.ExecuteReader())
          {
              while (reader.Read())
              {
                  // 处理数据
              }
          } // reader 自动释放
      } // command 自动释放
  } // connection 自动关闭和释放
  ```

1. **超时控制**：
  - 连接超时：`connTimeout` 防止连接建立过程挂起（默认 1 分钟）
  - 读取超时：`readTimeout` 防止读操作挂起（WebSocket，默认 5 分钟）
  - 写入超时：`writeTimeout` 防止写操作挂起（WebSocket，默认 10 秒）
1. **重连安全**：WebSocket 自动重连机制使用锁保护，防止并发重连导致的状态混乱。重连后会重新进行身份认证。

### 5.5 错误处理

1. **异常信息脱敏**：`TDengineError` 异常不应包含密码、Token 等敏感信息。
2. **错误码传递**：所有 TDengine 错误通过 `TDengineError` 异常抛出，包含错误码（`Code`）和错误消息（`Error`），便于故障排查。
3. **连接状态检查**：`ITDengineClient.ConnectionAvailable()` 方法用于检查连接状态，防止在断开连接上执行操作。

### 5.6 链路追踪

1. **请求 ID 支持**：所有主要接口均支持传递 `reqId` 参数：
  - `Query(string query, long reqId)`
  - `Exec(string query, long reqId)`
  - `StmtInit(long reqId)`
  - `SchemalessInsert(..., long reqId)`
1. **请求 ID 生成器**：`ReqId.GetReqId()` 提供唯一请求 ID 生成，用于分布式追踪和审计。

## 6. 性能

1. 以二进制数据块的方式与 TDengine 交互，提高传输性能。
2. 提供多行数据绑定，提升参数绑定性能。
3. 支持 WebSocket 数据压缩，优化公网数据传输性能。

## 7. 兼容性

| **Connector 版本** | **TDengine 版本** |
| --- | --- |
| 3.1.9 | 3.3.6.0及以上 |
| 3.1.8 | 3.3.6.0及以上 |
| 3.1.7 | 3.3.6.0及以上 |
| 3.1.6 | 3.3.2.0及以上 |
| 3.1.5 | 3.3.2.0及以上 |
| 3.1.4 | 3.3.2.0及以上 |
| 3.1.3 | 3.2.1.0及以上 |
| 3.1.2 | 3.2.1.0及以上 |
| 3.1.1 | 3.2.1.0及以上 |
| 3.1.0 | 3.2.1.0及以上 |

## 8. 运维

无

## 9. 使用场景

- 在 C# 应用中连接 TDengine 数据库。
- 通过原生连接和 WebSocket 连接执行 SQL、参数绑定、无模式写入 和数据订阅。

## 10. 约束和限制

1. 支持 .NET Framework 4.6 及以上版本。.NET 5.0 及以上版本。
2. 原生连接方式，必须保证 taosc 驱动与 TDengine 版本一致性。
3. 不支持针对单条数据记录的删除操作。
4. 不支持事务操作。

## 11. 常见错误和排查

1. 使用原生连接报找不到动态库
   - **原因**：没有安装 TDengine 客户端。
   - **解决方法**：安装与服务端版本对应的 TDengine 客户端。
2. 原生连接 TDengine 失败
   - **原因**：TDengine 没有启动成功或客户端没有设置 FQDN。
   - **解决方法**：确认 TDengine 启动成功，修改客户端 hosts 将 TDengine 集群的每台机器的 fqdn 配置好解析。
3. WebSocket 连接失败或超时
   - **原因**：taosAdapter 没有启动或端口没有开放。
   - **解决方法**：确认 taosAdapter 启动成功，taosAdapter 配置端口（默认 6041）客户端可以访问。

## 12. 可观测性

支持传递请求 id 的接口可以通过请求 id 进行链路追踪，通过请求 id 可以在后续模块日志进行分析。

## 13. 安装和卸载

可以在当前 .NET 项目的路径下，通过 dotnet CLI 添加 Nuget package `TDengine.Connector` 到当前项目。
```bash
dotnet add package TDengine.Connector
```

也可以修改当前项目的 `.csproj` 文件，添加如下 ItemGroup。
```xml
  <ItemGroup><PackageReference Include="TDengine.Connector" Version="3.1.*" /></ItemGroup>
```

## 14. 文档

需要在官方文档中添加章节【TDengine C# Connector】。

## 15. 参考文档

1. [C/C++ 连接器-Function Spec](https://taosdata.feishu.cn/wiki/Hk2Swj9bdipmZCkK0NEcZCKankd) 4. 行为说明
2. [taosAdapter-Function Spec](https://taosdata.feishu.cn/wiki/Xf3zweDQRiFhwNkBSWScVj01nVc) 4. 行为说明
3. .NET Framework 文档：https://learn.microsoft.com/zh-cn/dotnet/framework/
4. ADO.NET 文档：https://learn.microsoft.com/zh-cn/dotnet/framework/data/adonet/

## 16. 附录

无。

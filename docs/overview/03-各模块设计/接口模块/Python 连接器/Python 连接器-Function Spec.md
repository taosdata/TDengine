# Python 连接器-Function Spec

## 1. 修订记录

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/1/13 | 1.0 | 门世斌 | 创建 |
| 2025/12/8 | 1.1 | 郭振伟 | 更新到 TDengine 3.4.0.0 变更 |

## 2. 背景

在物联网和工业互联网快速发展中，时序数据的存储与分析愈发重要。TDengine 是一款针对物联网场景优化的开源时序数据库，我们需要一个遵循 Python 规范的连接器来支持应用开发。
该连接器支持 TDengine 的核心功能，包括执行 SQL、参数绑定、无模式写入和数据订阅。我们的目标是开发一个功能齐全、性能优越、易于使用的 Python 连接器，同时提供详细文档和示例，确保可靠性与可维护性。

## 3. 定义

**无模式写入**：是指在写入数据时无需预先定义表结构，系统会根据数据自动创建或调整表结构的特性。
**数据订阅**：允许客户端实时接收数据库中的数据变化，适用于实时监控场景。
**参数绑定**：是在 SQL 语句中使用占位符替代具体值的技术，可以防止 SQL 注入并提高性能。
**WebSocket**： 是一种基于 TCP 的全双工通信协议，支持服务器和客户端之间的实时数据传输。
**FQDN**：全限定域名是互联网域名系统（DNS）中的一种完整的、唯一的域名表示形式，用于明确标识互联网上的特定主机或服务器。
**RFC3339**：RFC 3339 是一种时间和日期格式的标准，旨在规范化地表示时间和日期，以便在计算机系统之间传递和处理。

## 4. 行为说明

### 4.1 支持 Python 版本

支持 Python 3.0 及以上版本。

### 4.2 数据类型映射

TDengine 目前支持时间戳、数字、字符、布尔类型，与 Python 对应类型转换如下：
| TDengine DataType | Python DataType |
| --- | --- |
| TIMESTAMP | datetime/int |
| INT | int |
| BIGINT | int |
| FLOAT | float |
| DOUBLE | int |
| SMALLINT | int |
| TINYINT | int |
| BOOL | bool |
| BINARY | str |
| NCHAR | str |
| JSON | str |
| GEOMETRY | bytearray |
| VARBINARY | bytearray |

**注意**：JSON 类型仅在 tag 中支持。

### 4.3 连接方式

- **原生连接**，Python 连接器加载 TDengine 客户端驱动程序（libtaos.so/taos.dll），直接连接 TDengine 实例，特点性能高，速度快。功能上支持数据写入、查询、数据订阅、schemaless 接口和参数绑定接口等功能。对应 `taospy` 包的 `taos` 模块。
- **WebSocket 连接**，Python 连接器通过 `taosAdapter` 提供的 WebSocket 接口连接 TDengine 实例，特点是兼具前两种连接的优势， 即性能高又依赖小。功能上 WebSocket 连接实现功能集合和原生连接有少量不同。对应 `taos-ws-py` 包，可以选装。

### 4.4 WebSocket 连接

#### 4.4.1 URL 规范

```plaintext
[+<protocol>]://[<username>:<password>@][<host1>:<port1>[,...<hostN>:<portN>]][/<database>][?<key1>=<value1>[&...<keyN>=<valueN>]]
|-----------|---|----------|-----------|-------------------------------------|------------|--------------------------------------|
|  protocol |   | username | password  |  addresses                          |   database |   params                             |
```

- **protocol**：指定使用的协议，例如 `ws://localhost:6041` 表示通过 WebSocket 协议建立连接。
  - **ws**：通过 WebSocket 协议建立连接。
  - **wss**：通过 WebSocket 协议并启用 SSL/TLS 加密建立连接。
- **username/password**：数据库的用户名和密码。
- **addresses**：指定创建连接的服务器地址，多个地址间用英文逗号分隔。对于 IPv6 地址，必须使用中括号括起来（如 `[::1]` 或 `[2001:db8:1234:5678::1]`），以避免端口号解析冲突。
  - 示例：`ws://host1:6041,host2:6041` 或 `ws://`（等同于 `ws://localhost:6041`）。
- **database**：数据库名称。
- **params**：
  - `token`：用于 TDengine TSDB 云服务的身份验证。
  - `timezone`：时区，IANA 格式（如 `Asia/Shanghai`），默认为本地时区。
  - `compression`：是否启用数据压缩，默认为 `false`。
  - `conn_retries`：连接失败时的最大重试次数，默认为 5。
  - `retry_backoff_ms`：连接失败时的初始等待时间（毫秒），默认为 200。此值会随着连续失败而指数增长，直到达到最大等待时间。
  - `retry_backoff_max_ms`：连接失败时的最大等待时间（毫秒），默认为 2000。
  - `read_timeout`：WebSocket 连接的响应超时时间（秒），不含数据订阅，默认为 300（5 分钟）。

#### 4.4.2 建立连接

`fn connect(dsn: Option<&str>, args: Option<&PyDict>) -> PyResult<Connection>` {folded="true"}
**接口说明**：建立 taosAdapter 连接。
**参数说明**：
`dsn`： 类型 `Option<&str>` 可选，数据源名称（DSN），用于指定要连接的数据库的位置和认证信息。
`args`： 类型 `Option<&PyDict>` 可选，以 Python 字典的形式提供， 可用于设置
`user`： 数据库的用户名
`password`： 数据库的密码。
`host`： 主机地址
`port`： 端口号
`database`： 数据库名称
**返回值**：连接对象。
**异常**：操作失败抛出 `ConnectionError` 异常。
`fn cursor(&self) -> PyResult<Cursor>` {folded="true"}
**接口说明**：创建一个新的数据库游标对象，用于执行 SQL 命令和查询。
**返回值**：数据库游标对象。
**异常**：操作失败抛出 `ConnectionError` 异常。
连接示例：
```python
conn = None
host = "localhost"
port = 6041
try:
    conn = taosws.connect(
        user="root",
        password="taosdata",
        host=host,
        port=port,
    )
    print(f"Connected to {host}:{port} successfully.")
except Exception as err:
    print(f"Failed to connect to {host}:{port} , ErrMessage:{err}")
    raise err
```

SSL 连接示例：
```python
try:
    url = 'wss://root:taosdata@localhost:6041'
    conn = taosws.connect(url)
    print(f"Connected to {url} successfully.")
except Exception as err:
    print(f"Failed to connect to {url} , ErrMessage:{err}")
    raise err
```

云服务实例：
```python
try:
    url = 'wss://gw.cloud.taosdata.com?token=215f1e77f81abxssssxxx9'
    conn = taosws.connect(url)
    print(f"Connected to {url} successfully.")
except Exception as err:
    print(f"Failed to connect to {url} , ErrMessage:{err}")
    raise err
```

#### 4.4.3 执行 SQL

`fn execute(&self, sql: &str) -> PyResult<i32>`
**接口说明**：执行 sql 语句。
**参数说明**：
`sql`：待执行的 sql 语句。
**返回值**：影响的条数。
**异常**：操作失败抛出 `QueryError` 异常。
`fn execute_with_req_id(&self, sql: &str, req_id: u64) -> PyResult<i32>`
**接口说明**：执行带有 req_id 的 sql 语句。
**参数说明**：
`sql`：待执行的 sql 语句。
`reqId`： 用于问题追踪。
**返回值**：影响的条数。
**异常**：操作失败抛出 `QueryError` 异常。
`fn query(&self, sql: &str) -> PyResult<TaosResult>`
**接口说明**：查询数据。
**参数说明**：
`sql`：待执行的 sql 语句。
**返回值**：`TaosResult` 数据集对象。
**异常**：操作失败抛出 `QueryError` 异常。
`fn query_with_req_id(&self, sql: &str, req_id: u64) -> PyResult<TaosResult>`
**接口说明**：查询带有 req_id 的 sql 语句。
**参数说明**：
`sql`：待执行的 sql 语句。
`reqId`： 用于问题追踪。
**返回值**：`TaosResult` 数据集对象。
**异常**：操作失败抛出 `QueryError` 异常。

#### 4.4.4 数据集

TaosResult 对象可以通过循环遍历获取查询到的数据。
`fn fields(&self) -> Vec<TaosField>`
**接口说明**：获取查询数据的字段信息， 包括：名称，类型及字段长度。
**返回值**：`Vec<TaosField>` 字段信息数组。
`fn field_count(&self) -> i32`
**接口说明**：获取查询到的记录条数。
**返回值**：`i32` 查询到的记录条数。
**完整示例**
```python
import taosws

conn = None
host="localhost"
port=6041
try:
    conn = taosws.connect(user="root",
                          password="taosdata",
                          host=host,
                          port=port)

    # create database
    rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS power")
    print(f"Create database power successfully, rowsAffected: {rowsAffected}");
    
    # create super table
    rowsAffected = conn.execute(
        "CREATE TABLE IF NOT EXISTS power.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )
    print(f"Create stable power.meters successfully, rowsAffected: {rowsAffected}");

    sql = """
        INSERT INTO 
        power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
            VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
            (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
        power.d1002 USING power.meters (groupid, location) TAGS(3, 'California.SanFrancisco') 
            VALUES (NOW + 1a, 10.30000, 218, 0.25000)
        """
    affectedRows = conn.execute(sql)
    print(f"Successfully inserted {affectedRows} rows to power.meters.")
    
    sql = "SELECT ts, current, location FROM power.meters limit 100"
    result = conn.query(sql)
    for row in result:
        print(f"ts: {row[0]}, current: {row[1]}, location:  {row[2]}")

except Exception as err:
    print(f"Failed to execute sql, sql: {sql}, ErrMessage: {err}.")
    raise err
finally:
    if conn:
        conn.close()
```

#### 4.4.5 无模式写入

```python {wrap}
fn schemaless_insert(&self, lines: Vec<String>, protocol: PySchemalessProtocol,         precision: PySchemalessPrecision, ttl: i32, req_id: u64) -> PyResult<()>
```

**接口说明**：无模式写入。
**参数说明**：
`lines`：待写入的数据数组，无模式具体的数据格式可参考 `Schemaless 写入`。
`protocol`： 协议类型
`PySchemalessProtocol::Line`： InfluxDB 行协议（Line Protocol）。
`PySchemalessProtocol::Telnet`：OpenTSDB 文本行协议。
`PySchemalessProtocol::Json`： JSON 协议格式
`precision`： 时间精度
`PySchemalessPrecision::Hour`： 小时
`PySchemalessPrecision::Minute`：分钟
`PySchemalessPrecision::Second` 秒
`PySchemalessPrecision::Millisecond`：毫秒
`PySchemalessPrecision::Microsecond`：微秒
`PySchemalessPrecision::Nanosecond`： 纳秒
`ttl`：表过期时间，单位天。
`reqId`： 用于问题追踪。
**异常**：操作失败抛出 `DataError` 或 `OperationalError` 异常。
完整示例：
```python
import taosws

host = "localhost"
port = 6041
def prepare():
    conn = None
    try:
        conn = taosws.connect(user="root",
                            password="taosdata",
                            host=host,
                            port=port)

        # create database
        rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS power")
        assert rowsAffected == 0

    except Exception as err:
        print(f"Failed to create db and table, db addrr:{host}:{port} ; ErrMessage:{err}")
        raise err
    finally:
        if conn:
            conn.close()

def schemaless_insert():

    conn = None

    lineDemo = [
        "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639"
    ]

    telnetDemo = ["metric_telnet 1707095283260 4 host=host0 interface=eth0"]

    jsonDemo = [
        '{"metric": "metric_json","timestamp": 1626846400,"value": 10.3, "tags": {"groupid": 2, "location": "California.SanFrancisco", "id": "d1001"}}'
    ]

    try:
        conn = taosws.connect(user="root",
                              password="taosdata",
                              host=host,
                              port=port,
                              database='power')

        conn.schemaless_insert(
            lines = lineDemo,
            protocol = taosws.PySchemalessProtocol.Line,
            precision = taosws.PySchemalessPrecision.Millisecond,
            ttl=1,
            req_id=1,
        )

        conn.schemaless_insert(
            lines=telnetDemo,
            protocol=taosws.PySchemalessProtocol.Telnet,
            precision=taosws.PySchemalessPrecision.Microsecond,
            ttl=1,
            req_id=2,
        )

        conn.schemaless_insert(
            lines=jsonDemo,
            protocol=taosws.PySchemalessProtocol.Json,
            precision=taosws.PySchemalessPrecision.Millisecond,
            ttl=1,
            req_id=3,
        )
        print("Inserted data with schemaless successfully.");
    except Exception as err:
        print(f"Failed to insert data with schemaless, ErrMessage:{err}")
        raise err
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    prepare()
    schemaless_insert()
  
```

#### 4.4.6 参数绑定

`fn stmt2_statement(&self) -> PyResult<TaosStmt2>`
**接口说明**：使用 连接 对象创建 stmt2 对象。
**返回值**：stmt2 对象。
**异常**：操作失败抛出 `ConnectionError` 异常。
`fn prepare(&mut self, sql: &str) -> PyResult<()>`
**接口说明**：绑定预编译 sql 语句。
**参数说明**：
`sql`：预编译的 SQL 语句。
**异常**：操作失败抛出 `ProgrammingError` 异常。
`fn bind(&mut self, params: Vec<PyStmt2BindParam>) -> PyResult<()>`
**接口说明**：绑定数据。
**参数说明**：
`params`：绑定数据。
**异常**：操作失败抛出 `ProgrammingError` 异常。
`fn execute(&mut self) -> PyResult<usize>`
**接口说明**：执行将绑定的数据全部写入。
**返回值**：写入条数。
**异常**：操作失败抛出 `QueryError` 异常。
`fn result_set(&mut self) -> PyResult<TaosResult>`
**接口说明**：获取绑定查询结果集。
**返回值**：查询结果集。
**异常**：操作失败抛出 `QueryError` 异常。
`fn affect_rows(&mut self) -> PyResult<usize>`
**接口说明**：获取写入条数。
**返回值**：写入条数。
`fn close(&self) -> PyResult<()>`
**接口说明**：关闭 stmt2 对象。
`fn statement(&self) -> PyResult<TaosStmt>`
**接口说明**：使用 连接 对象创建 stmt 对象。
**返回值**：stmt 对象。
**异常**：操作失败抛出 `ConnectionError` 异常。
`fn prepare(&mut self, sql: &str) -> PyResult<()>`
**接口说明**：绑定预编译 sql 语句。
**参数说明**：
`sql`： 预编译的 SQL 语句。
**异常**：操作失败抛出 `ProgrammingError` 异常。
`fn set_tbname(&mut self, table_name: &str) -> PyResult<()>`
**接口说明**：设置将要写入数据的表名。
**参数说明**：
`tableName`： 表名，如果需要指定数据库， 例如： `db_name.table_name` 即可。
**异常**：操作失败抛出 `ProgrammingError` 异常。
`fn set_tags(&mut self, tags: Vec<PyTagView>) -> PyResult<()>`
**接口说明**：设置表 Tags 数据， 用于自动建表。
**参数说明**：
`paramsArray`： Tags 数据。
**异常**：操作失败抛出 `ProgrammingError` 异常。
`fn bind_param(&mut self, params: Vec<PyColumnView>) -> PyResult<()>`
**接口说明**：绑定数据。
**参数说明**：
`paramsArray`： 绑定数据。
**异常**：操作失败抛出 `ProgrammingError` 异常。
`fn add_batch(&mut self) -> PyResult<()>`
**接口说明**：提交绑定数据。
**异常**：操作失败抛出 `ProgrammingError` 异常。
`fn execute(&mut self) -> PyResult<usize>`
**接口说明**：执行将绑定的数据全部写入。
**返回值**：写入条数。
**异常**：操作失败抛出 `QueryError` 异常。
`fn affect_rows(&mut self) -> PyResult<usize>`
**接口说明**： 获取写入条数。
**返回值**：写入条数。
`fn close(&self) -> PyResult<()>`
**接口说明**： 关闭 stmt 对象。
STMT2 示例：
```python {wrap}
from datetime import datetime
import random
import taosws

numOfSubTable = 10

numOfRow = 10

conn = None
stmt2 = None
host="localhost"
port=6041
try:
    conn = taosws.connect(user="root",
                          password="taosdata",
                          host=host,
                          port=port)

    conn.execute("CREATE DATABASE IF NOT EXISTS power")
    conn.execute("USE power")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )

    sql = "INSERT INTO ? USING meters (groupid, location) TAGS(?,?) VALUES (?,?,?,?)"
    stmt2 = conn.stmt2_statement()
    stmt2.prepare(sql)

    for i in range(numOfSubTable):        
        current = int(datetime.now().timestamp() * 1000)
        timestamps = []
        currents = []
        voltages = []
        phases = []
        for j in range (numOfRow):
            timestamps.append(current + j)
            currents.append(random.random() * 30)
            voltages.append(random.randint(100, 300))
            phases.append(random.random())

        pyStmt2Param = taosws.stmt2_bind_param_view(
            table_name=f"d_bind_{i}", 
            tags=[taosws.int_to_tag(i),
                  taosws.varchar_to_tag(f"location_{i}")
            ], 
            columns=[  
                taosws.millis_timestamps_to_column(timestamps),
                taosws.floats_to_column(currents),
                taosws.ints_to_column(voltages),
                taosws.floats_to_column(phases)
            ]
        )    
        
        stmt2.bind([pyStmt2Param])
        rows = stmt2.execute()
        print(f"Successfully inserted to power.meters.")
        
except Exception as err:
    print(f"Failed to insert to table meters using stmt, ErrMessage:{err}") 
    raise err
finally:
    if stmt2:
        stmt2.close()
    if conn:    
        conn.close()
```

**STMT 示例：**
```python
from datetime import datetime
import random
import taosws

numOfSubTable = 10

numOfRow = 10

conn = None
stmt = None
host="localhost"
port=6041
try:
    conn = taosws.connect(user="root",
                          password="taosdata",
                          host=host,
                          port=port)

    conn.execute("CREATE DATABASE IF NOT EXISTS power")
    conn.execute("USE power")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )

    # ANCHOR: stmt
    sql = "INSERT INTO ? USING meters (groupid, location) TAGS(?,?) VALUES (?,?,?,?)"
    stmt = conn.statement()
    stmt.prepare(sql)

    for i in range(numOfSubTable):
        tbname = f"d_bind_{i}"

        tags = [
            taosws.int_to_tag(i),
            taosws.varchar_to_tag(f"location_{i}"),
        ]
        stmt.set_tbname_tags(tbname, tags)
        current = int(datetime.now().timestamp() * 1000)
        timestamps = []
        currents = []
        voltages = []
        phases = []
        for j in range (numOfRow):
            timestamps.append(current + i)
            currents.append(random.random() * 30)
            voltages.append(random.randint(100, 300))
            phases.append(random.random())

        stmt.bind_param(
            [
                taosws.millis_timestamps_to_column(timestamps),
                taosws.floats_to_column(currents),
                taosws.ints_to_column(voltages),
                taosws.floats_to_column(phases),
            ]
        )

        stmt.add_batch()
        stmt.execute()
        
        print(f"Successfully inserted to power.meters.")
        
except Exception as err:
    print(f"Failed to insert to table meters using stmt, ErrMessage:{err}") 
    raise err
finally:
    if stmt:
        stmt.close()
    if conn:    
        conn.close()

```

#### 4.4.7 数据订阅

创建消费者支持属性列表：
```python
host：主机地址。
port：端口号。
group.id：所在的 group。
client.id：客户端id。
td.connect.user: 数据库用户名。
td.connect.pass: 数据库密码。
td.connect.token：数据库的连接token。
auto.offset.reset：来确定消费位置为最新数据（latest）还是包含旧数据（earliest）。
enable.auto.commit：是否允许自动提交。
auto.commit.interval.ms：自动提交间隔
```

`fn Consumer(conf: Option<&PyDict>, dsn: Option<&str>) -> PyResult<Self>`
**接口说明** 消费者构造函数。
`conf`： 类型 `Option<&PyDict>` 可选，以 Python 字典的形式提供， 具体配置参见属性列表。
`dsn`： 类型 `Option<&str>` 可选，数据源名称（DSN），用于指定要连接的数据库的位置和认证信息。
**返回值**：Consumer 消费者对象。
**异常**：操作失败抛出 `ConsumerException` 异常。
`fn subscribe(&mut self, topics: &PyList) -> PyResult<()>`
**接口说明** 订阅一组主题。
**参数说明**：
`topics`： 订阅的主题列表。
**异常**：操作失败抛出 `ConsumerException` 异常。
`fn unsubscribe(&mut self)`
**接口说明** 取消订阅。
**异常**：操作失败抛出 `ConsumerException` 异常。
`fn poll(&mut self, timeout: Option<f64>) -> PyResult<Option<Message>>`
**接口说明** 轮询消息。
**参数说明**：
`timeout`： 表示轮询的超时时间，单位毫秒。
**返回值**：`Message` 每个主题对应的数据。
**异常**：操作失败抛出 `ConsumerException` 异常。
`fn commit(&mut self, message: &mut Message) -> PyResult<()>`
**接口说明** 提交当前处理的消息的偏移量。
**参数说明**：
`message`： 类型 `Message`， 当前处理的消息的偏移量。
**异常**：操作失败抛出 `ConsumerException` 异常。
`fn assignment(&mut self) -> PyResult<Option<Vec<TopicAssignment>>>`
**接口说明**：获取消费者当前分配的指定的分区或所有分区。
**返回值**：返回值类型为 `Vec<TopicAssignment>`，即消费者当前分配的所有分区。
**异常**：操作失败抛出 ConsumerException 异常。
`fn seek(&mut self, topic: &str, vg_id: i32, offset: i64) -> PyResult<()>`
**接口说明**：将给定分区的偏移量设置到指定的位置。
**参数说明**：
`topic`： 订阅的主题。
`vg_id`: vgroupid. 
`offset`：需要设置的偏移量。
**异常**：操作失败抛出 ConsumerException 异常。
`fn committed(&mut self, topic: &str, vg_id: i32) -> PyResult<i64>`
**接口说明**：获取订阅主题的 vgroupid 分区最后提交的偏移量。
**参数说明**：
`topic`： 订阅的主题。
`vg_id`: vgroupid. 
**返回值**：`i64`，分区最后提交的偏移量。
**异常**：操作失败抛出 ConsumerException 异常。
`fn position(&mut self, topic: &str, vg_id: i32) -> PyResult<i64>`
**接口说明**：获取给定分区当前的偏移量。
**参数说明**：
`topic`： 订阅的主题。
`vg_id`: vgroupid. 
**返回值**：`i64`，分区最后提交的偏移量。
**异常**：操作失败抛出 ConsumerException 异常。
`fn close(&mut self)`
**接口说明**：关闭 tmq 连接。
**异常**：操作失败抛出 ConsumerException 异常。
**完整示例**
```python
#!/usr/bin/python3
import taosws

db              = "power"
topic           = "topic_meters"
user            = "root"
password        = "taosdata"
host            = "localhost"
port            = 6041
groupId         = "group1"
clientId        = "1"  
tdConnWsScheme  = "ws"
autoOffsetReset = "latest"
autoCommitState = "true"
autoCommitIntv  = "1000"


def prepareMeta():
    conn = None

    try:
        conn = taosws.connect(user=user, password=password, host=host, port=port)

        # create database
        rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        assert rowsAffected == 0

        # change database.
        rowsAffected = conn.execute(f"USE {db}")
        assert rowsAffected == 0

        # create super table
        rowsAffected = conn.execute(
            "CREATE TABLE IF NOT EXISTS `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(64))"
        )
        assert rowsAffected == 0

        # create table
        rowsAffected = conn.execute(
            "CREATE TABLE IF NOT EXISTS `d0` USING `meters` (groupid, location) TAGS(0, 'Los Angles')")
        assert rowsAffected == 0

        # ANCHOR: create_topic
        # create topic
        conn.execute(
            f"CREATE TOPIC IF NOT EXISTS {topic} AS SELECT ts, current, voltage, phase, groupid, location FROM meters"
        )
        # ANCHOR_END: create_topic

        sql = """
            INSERT INTO 
            power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
                VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
                (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
            power.d1002 USING power.meters (groupid, location) TAGS(3, 'California.SanFrancisco') 
                VALUES (NOW + 1a, 10.30000, 218, 0.25000)
            """
        affectedRows = conn.execute(sql)
        print(f"Inserted into {affectedRows} rows to power.meters successfully.")

    except Exception as err:
        print(f"Failed to prepareMeta, host: {host}:{port}, db: {db}, topic: {topic}, ErrMessage:{err}.")
        raise err
    finally:
        if conn:
            conn.close()


def create_consumer():  
    try:
        consumer = taosws.Consumer(conf={
            "td.connect.websocket.scheme": tdConnWsScheme,
            "group.id": groupId,
            "client.id": clientId,
            "auto.offset.reset": autoOffsetReset,
            "td.connect.ip": host,
            "td.connect.port": port,
            "enable.auto.commit": autoCommitState,
            "auto.commit.interval.ms": autoCommitIntv,
        })
        print(f"Create consumer successfully, host: {host}:{port}, groupId: {groupId}, clientId: {clientId}.");
        return consumer;
    except Exception as err:
        print(f"Failed to create websocket consumer, host: {host}:{port}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.");
        raise err


def seek_offset(consumer):

    try:
        assignments = consumer.assignment()
        for assignment in assignments:
            topic = assignment.topic()
            print(f"topic: {topic}")
            for assign in assignment.assignments():
                print(
                    f"vg_id: {assign.vg_id()}, offset: {assign.offset()}, begin: {assign.begin()}, end: {assign.end()}")
                consumer.seek(topic, assign.vg_id(), assign.begin())
                print("Assignment seek to beginning successfully.")

    except Exception as err:
        print(f"Failed to seek offset, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err

def subscribe(consumer):
    try:
        consumer.subscribe([topic])
        print("Subscribe topics successfully")
        for i in range(50):
            records = consumer.poll(timeout=1.0)
            if records:
                for block in records:
                    for row in block:
                        print(f"data: {row}")

    except Exception as err:
        print(f"Failed to poll data, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err

def commit_offset(consumer):
    try:
        for i in range(50):
            records = consumer.poll(timeout=1.0)
            if records:
                for block in records:
                    for row in block:
                        print(f"data: {row}")
                        
                #  after processing the data, commit the offset manually        
                consumer.commit(records)
                print("Commit offset manually successfully.")

    except Exception as err:
        print(f"Failed to commit offset, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err

def unsubscribe(consumer):
  
    try:
        consumer.unsubscribe()
        print("Consumer unsubscribed successfully.");
    except Exception as err:
        print(f"Failed to unsubscribe consumer. topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err
    finally:
        if consumer:
            consumer.close()
            print("Consumer closed successfully."); 

if __name__ == "__main__":
    consumer = None
    try:
        prepareMeta()
        consumer = create_consumer()
        subscribe(consumer)
        seek_offset(consumer)
        commit_offset(consumer)      
    finally:
        if consumer:
            unsubscribe(consumer)
```

### 4.5 Native 连接

#### 4.5.1 建立连接

- `def connect(*args, **kwargs):`
  - **接口说明**：建立 taosAdapter 连接。
  - **参数说明**：
    - `kwargs`： 以 Python 字典的形式提供， 可用于设置
      - `user`： 数据库的用户名
      - `password`： 数据库的密码。
      - `host`： 主机地址
      - `port`： 端口号
      - `database`： 数据库名称
      - `timezone`： 时区
  - **返回值**：`TaosConnection` 连接对象。
  - **异常**：操作失败抛出 `AttributeError` 或 `ConnectionError` 异常。
- `def cursor(self)`
  - **接口说明**：创建一个新的数据库游标对象，用于执行 SQL 命令和查询。
  - **返回值**：数据库游标对象。
**完整示例**
```python
import taos

def create_connection():
    # all parameters are optional.
    conn = None
    host = "localhost"
    port = 6030
    try:
        conn = taos.connect(
            user="root",
            password="taosdata",
            host=host,
            port=port,
        )
        print(f"Connected to {host}:{port} successfully.");
    except Exception as err:
        print(f"Failed to connect to {host}:{port} , ErrMessage:{err}")
        raise err
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    create_connection()
```

#### 4.5.2 执行 SQL

- `def execute(self, operation, req_id: Optional[int] = None)`
  - **接口说明**：执行 sql 语句。
  - **参数说明**：
    - `operation`：待执行的 sql 语句。
    - `reqId`： 用于问题追踪。
  - **返回值**：影响的条数。
  - **异常**：操作失败抛出 `ProgrammingError` 异常。
- `def query(self, sql: str, req_id: Optional[int] = None) -> TaosResult`
  - **接口说明**：查询数据。
  - **参数说明**：
    - `sql`：待执行的 sql 语句。
    - `reqId`： 用于问题追踪。
  - **返回值**：`TaosResult` 数据集对象。
  - **异常**：操作失败抛出 `ProgrammingError` 异常。

#### 4.5.3 数据集

TaosResult 对象可以通过循环遍历获取查询到的数据。
- `def fields(&self)`
  - **接口说明**：获取查询数据的字段信息， 包括：名称，类型及字段长度。
  - **返回值**：`TaosFields` 字段信息 list。
- `def field_count(&self)`
  - **接口说明**：获取查询到的记录条数。
  - **返回值**：查询到的记录条数。
- `def fetch_all_into_dict(self)`
  - **接口说明**：将所有的记录转换为字典。
  - **返回值**：返回字典列表。
**完整示例**
```python
import taos

conn = None
host = "localhost"
port = 6030
try:
    conn = taos.connect(host=host,
                        port=port,
                        user="root",
                        password="taosdata")

    # create database
    rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS power")
    print(f"Create database power successfully, rowsAffected: {rowsAffected}");
    
    # create super table
    rowsAffected = conn.execute(
        "CREATE TABLE IF NOT EXISTS power.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )
    print(f"Create stable power.meters successfully, rowsAffected: {rowsAffected}");

    sql = """
        INSERT INTO 
        power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
            VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
            (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
        power.d1002 USING power.meters (groupid, location)  TAGS(3, 'California.SanFrancisco') 
            VALUES (NOW + 1a, 10.30000, 218, 0.25000)
        """
    affectedRows = conn.execute(sql)
    print(f"Successfully inserted {affectedRows} rows to power.meters.")

    sql = "SELECT ts, current, location FROM power.meters limit 100"
    result = conn.query(sql)
    for row in result:
        print(f"ts: {row[0]}, current: {row[1]}, location:  {row[2]}")
        
except Exception as err:
    print(f"Failed to execute sql, sql: {sql}, ErrMessage: {err}.")
    raise err
finally:
    if conn:
        conn.close() 
```

#### 4.5.4 无模式写入

- `def schemaless_insert(&self, lines: List[str], protocol: SmlProtocol, precision: SmlPrecision, req_id: Optional[int] = None, ttl: Optional[int] = None) -> int:`
  - **接口说明**：无模式写入。
  - **参数说明**：
    - `lines`：待写入的数据数组，无模式具体的数据格式可参考 `Schemaless 写入`。
    - `protocol`： 协议类型
      - `SmlProtocol.LINE_PROTOCOL`： InfluxDB 行协议（Line Protocol）。
      - `SmlProtocol.TELNET_PROTOCOL`：OpenTSDB 文本行协议。
      - `SmlProtocol.JSON_PROTOCOL`： JSON 协议格式
    - `precision`： 时间精度
      - `SmlPrecision.Hour`： 小时
      - `SmlPrecision.Minute`：分钟
      - `SmlPrecision.Second` 秒
      - `SmlPrecision.Millisecond`：毫秒
      - `SmlPrecision.Microsecond`：微秒
      - `SmlPrecision.Nanosecond`： 纳秒
    - `ttl`：表过期时间，单位天。
    - `reqId`： 用于问题追踪。
  - **返回值**：影响的条数。
  - **异常**：操作失败抛出 `SchemalessError` 异常。
**完整示例**
```python
import taos

lineDemo = [
    "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639"
]

telnetDemo = ["metric_telnet 1707095283260 4 host=host0 interface=eth0"]

jsonDemo = [
    '{"metric": "metric_json","timestamp": 1626846400,"value": 10.3, "tags": {"groupid": 2, "location": "California.SanFrancisco", "id": "d1001"}}'
]
host = "localhost"
port = 6030
try:
    conn = taos.connect(
        user="root",
        password="taosdata",
        host=host,
        port=port
    )

    conn.execute("CREATE DATABASE IF NOT EXISTS power")
    # change database. same as execute "USE db"
    conn.select_db("power")

    conn.schemaless_insert(
        lineDemo, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.MILLI_SECONDS
    )
    conn.schemaless_insert(
        telnetDemo, taos.SmlProtocol.TELNET_PROTOCOL, taos.SmlPrecision.MICRO_SECONDS
    )
    conn.schemaless_insert(
        jsonDemo, taos.SmlProtocol.JSON_PROTOCOL, taos.SmlPrecision.MILLI_SECONDS
    )
    print("Inserted data with schemaless successfully.");
except Exception as err:
    print(f"Failed to insert data with schemaless, ErrMessage:{err}")
    raise err
finally:
    if conn:
        conn.close()
```

#### 4.5.5 参数绑定

- `def statement2(self, sql=None, option=None)`
  - **接口说明**：使用连接对象创建 stmt2 对象
  - **参数说明**
    - `sql`： 绑定的 SQL 语句，如果不为空会调用`prepare`函数
    - `option` 传入 TaosStmt2Option 类实例选项
  - **返回值**：stmt2 对象。
  - **异常**：操作失败抛出 `ConnectionError` 异常。
- `def prepare(self, sql)`
  - **接口说明**：绑定预编译 sql 语句
  - **参数说明**：
    - `sql`： 绑定的 SQL 语句
  - **异常**：操作失败抛出 `StatementError` 异常。
- `def bind_param(self, tbnames, tags, datas)`
  - **接口说明**：以独立数组方式绑定数据
  - **参数说明**：
    - `tbnames`： 绑定表名数组，数据类型为 list
    - `tags`： 绑定 tag 列值数组，数据类型为 list
    - `tags`： 绑定普通列值数组，数据类型为 list
  - **异常**：操作失败抛出 `StatementError` 异常
- `def bind_param_with_tables(self, tables)`
  - **接口说明**：以独立表方式绑定数据，独立表是以表为组织单位，每张表中有表名，TAG 值及普通列数值属性
  - **参数说明**：
    - `tables`： `BindTable` 独立表对象数组
  - **异常**：操作失败抛出 `StatementError` 异常。
- `def execute(self) -> int:`
  - **接口说明**：执行将绑定数据全部写入
  - **返回值**：影响行数
  - **异常**：操作失败抛出 `QueryError` 异常。
- `def result(self)`
  - **接口说明**： 获取参数绑定查询结果集
  - **返回值**：返回 TaosResult 对象
- `def close(self)`
  - **接口说明**： 关闭 stmt2 对象
**完整示例**
```python
import taos
from datetime import datetime
import random

numOfSubTable = 10
numOfRow = 10

conn = None
stmt2 = None
host="localhost"        
port=6030
try:
    # 1 connect
    conn = taos.connect(
        user="root",
        password="taosdata",
        host=host,        
        port=port,
    )

    # 2 create db and table
    conn.execute("CREATE DATABASE IF NOT EXISTS power")
    conn.execute("USE power")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
    )

    # 3 prepare
    sql = "INSERT INTO ? USING meters (groupid, location) TAGS(?,?) VALUES (?,?,?,?)"
    stmt2 = conn.statement2(sql)

    tbnames = []
    tags    = []
    datas   = []
    
    for i in range(numOfSubTable):
        # tbnames
        tbnames.append(f"d_bind_{i}")
        # tags
        tags.append([i, f"location_{i}"])
        # datas
        current = int(datetime.now().timestamp() * 1000)
        timestamps = []
        currents = []
        voltages = []
        phases = []
        for j in range (numOfRow):
            timestamps.append(current + i*1000 + j)
            currents.append(float(random.random() * 30))
            voltages.append(random.randint(100, 300))
            phases.append(float(random.random()))
        data = [timestamps, currents, voltages, phases]
        datas.append(data)

    # 4 bind param
    stmt2.bind_param(tbnames, tags, datas)

    # 5 execute
    stmt2.execute()

    # show 
    print(f"Successfully inserted with stmt2 to power.meters. child={numOfSubTable} rows={numOfRow} \n")

except Exception as err:
    print(f"Failed to insert to table meters using stmt2, ErrMessage:{err}") 
    raise err
finally:
    if stmt2:
        stmt2.close()
    if conn:    
        conn.close()

```

#### 4.5.6 数据订阅

**创建消费者支持属性列表**：
```python
td.connect.ip：主机地址。
td.connect.port：端口号。
group.id：所在的 group。
client.id：客户端id。
td.connect.user: 数据库用户名。
td.connect.pass: 数据库密码。
td.connect.token：数据库的连接token。
auto.offset.reset：来确定消费位置为最新数据（latest）还是包含旧数据（earliest）。
enable.auto.commit：是否允许自动提交。
auto.commit.interval.ms：自动提交间隔
```

- `def Consumer(configs)`
  - **接口说明** 消费者构造函数。
    - `configs`： Python 字典的形式提供， 具体配置参见属性列表。
  - **返回值**：Consumer 消费者对象。
  - **异常**：操作失败抛出 `TmqError` 异常。
- `def subscribe(self, topics)`
  - **接口说明** 订阅一组主题。
  - **参数说明**：
    - `topics`： 订阅的主题列表。
  - **异常**：操作失败抛出 `TmqError` 异常。
- `def unsubscribe(self)`
  - **接口说明** 取消订阅。
  - **异常**：操作失败抛出 `TmqError` 异常。
- `def poll(self, timeout: float = 1.0)`
  - **接口说明** 轮询消息。
  - **参数说明**：
    - `timeout`： 表示轮询的超时时间，单位毫秒。
  - **返回值**：`Message` 每个主题对应的数据。
  - **异常**：操作失败抛出 `TmqError` 异常。
- `def commit(self, message: Message = None, offsets: [TopicPartition] = None)`
  - **接口说明** 提交当前处理的消息的偏移量。
  - **参数说明**：
    - `message`： 类型 `Message`， 当前处理的消息的偏移量。
    - `offsets`： 类型 `[TopicPartition]`， 提交一批消息的偏移量。
  - **异常**：操作失败抛出 `TmqError` 异常。
- `def assignment(self)`
  - **接口说明**：获取消费者当前分配的指定的分区或所有分区。
  - **返回值**：返回值类型为 `[TopicPartition]`，即消费者当前分配的所有分区。
  - **异常**：操作失败抛出 TmqError 异常。
- `def seek(self, partition)`
  - **接口说明**：将给定分区的偏移量设置到指定的位置。
  - **参数说明**：
    - `partition`： 需要设置的偏移量。
      - `topic`： 订阅的主题
      - `partition`： 分区
      - `offset`： 偏移量
  - **异常**：操作失败抛出 `TmqError` 异常。
- `def committed(self, partitions)`
  - **接口说明**：获取订阅主题的分区最后提交的偏移量。
  - **参数说明**：
    - `partition`： 需要设置的偏移量。
      - `topic`： 订阅的主题
      - `partition`： 分区
  - **返回值**：`partition`，分区最后提交的偏移量。
  - **异常**：操作失败抛出 `TmqError` 异常。
- `def position(self, partitions)`
  - **接口说明**：获取给定分区当前的偏移量。
  - **参数说明**：
    - `partition`： 需要设置的偏移量。
      - `topic`： 订阅的主题
      - `partition`： 分区
  - **返回值**：`partition`，分区最后提交的偏移量。
  - **异常**：操作失败抛出 TmqError 异常。
- `def close(self)`
  - **接口说明**：关闭 tmq 连接。
  - **异常**：操作失败抛出 TmqError 异常。
**完整示例**
```python
#!/usr/bin/python3
import taos

db              = "power"
topic           = "topic_meters"
user            = "root"
password        = "taosdata"
host            = "localhost"
port            = 6030
groupId         = "group1"
clientId        = "1"  
tdConnWsScheme  = "ws"
autoOffsetReset = "latest"
autoCommitState = "true"
autoCommitIntv  = "1000"


def prepareMeta():
    conn = None
    try:
        conn = taos.connect(host=host, user=user, password=password, port=port)
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {db}")

        # change database. same as execute "USE db"
        conn.select_db(db)

        # create super table
        conn.execute(
            "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
        )

        # ANCHOR: create_topic
        # create topic
        conn.execute(
            f"CREATE TOPIC IF NOT EXISTS {topic} AS SELECT ts, current, voltage, phase, groupid, location FROM meters"
        )
        # ANCHOR_END: create_topic
        sql = """
            INSERT INTO 
            power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
                VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
                (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
            power.d1002 USING power.meters (groupid, location) TAGS(3, 'California.SanFrancisco') 
                VALUES (NOW + 1a, 10.30000, 218, 0.25000)
            """
        affectedRows = conn.execute(sql)
        print(f"Inserted into {affectedRows} rows to power.meters successfully.")
    except Exception as err:
        print(f"Failed to prepareMeta, host: {host}:{port}, db: {db}, topic: {topic}, ErrMessage:{err}.")
        raise err
    finally:
        if conn:
            conn.close()

from taos.tmq import Consumer

def create_consumer():
    try:
        consumer = Consumer(
            {
                "group.id": groupId,
                "client.id": clientId,
                "td.connect.user": user,
                "td.connect.pass": password,
                "enable.auto.commit": autoCommitState,
                "auto.commit.interval.ms": autoCommitIntv,
                "auto.offset.reset": autoOffsetReset,
                "td.connect.ip": host,
                "td.connect.port": str(port),
            }
        )
        print(f"Create consumer successfully, host: {host}:{port}, groupId: {groupId}, clientId: {clientId}")
        return consumer
    except Exception as err:
        print(f"Failed to create native consumer, host: {host}:{port}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err

def subscribe(consumer):
    try:
        # subscribe to the topics
        consumer.subscribe(["topic_meters"])
        print("Subscribe topics successfully")
        for i in range(50):
            records = consumer.poll(1)
            if records:
                err = records.error()
                if err is not None:
                    print(f"Poll data error, {err}")
                    raise err

                val = records.value()
                if val:
                    for block in val:
                        data = block.fetchall()
                        print(f"data: {data}")

    except Exception as err:
        print(f"Failed to poll data, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err

def commit_offset(consumer):
    try:
        for i in range(50):
            records = consumer.poll(1)
            if records:
                err = records.error()
                if err is not None:
                    print(f"Poll data error, {err}")
                    raise err

                val = records.value()
                if val:
                    for block in val:
                        print(block.fetchall())

                # after processing the data, commit the offset manually
                consumer.commit(records)
                print("Commit offset manually successfully.")

    except Exception as err:
        print(f"Failed to commit offset, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err

def seek_offset(consumer):
    try:
        assignments = consumer.assignment()
        if assignments:
            for partition in assignments:
                partition.offset = 0
                consumer.seek(partition)
                print(f"Assignment seek to beginning successfully.")
    except Exception as err:
        print(f"Failed to seek offset, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err

def unsubscribe(consumer): 
    try:
        consumer.unsubscribe()
        print("Consumer unsubscribed successfully.")
    except Exception as err:
        print(f"Failed to unsubscribe consumer. topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err
    finally:
        if consumer:
            consumer.close()
            print("Consumer closed successfully.")       

if __name__ == "__main__":
    consumer = None
    try:
        prepareMeta()
        consumer = create_consumer()
        subscribe(consumer)
        seek_offset(consumer)
        commit_offset(consumer)
    finally:
        if consumer:
            unsubscribe(consumer)
```

## 5. 性能

1. 以二进制数据块的方式与 TDengine 交互，提高传输性能。
2. 提供多行数据绑定，提升参数绑定性能。

## 6. 安全

- 凭据保护（T-PYCONN-01/02/03）：禁止在代码中硬编码用户名、密码或 Token，推荐通过环境变量、密钥管理服务或加密配置文件读取凭据。连接器日志输出默认对密码、Token 等敏感信息进行脱敏处理，日志中不记录完整凭据或连接字符串。生产环境下，凭据不应以明文形式存储或长时间保留在内存中，示例代码和文档均展示安全凭据管理方式。
- 传输加固（T-PYCONN-05/06/07）：WebSocket 连接默认使用 wss://，原生连接加密能力在文档中明确说明。所有加密连接默认启用 SSL/TLS 证书验证，禁用验证时需显示配置并警告风险。数据传输通过加密通道保障机密性与完整性。
- 输入校验与注入防护（T-PYCONN-04）：所有 SQL 执行接口均支持参数绑定（Prepared Statement），防止 SQL 注入。文档和示例代码强调参数绑定为推荐实践；对于不支持参数绑定的场景，提供输入校验和转义指导，拒绝危险输入。
- 日志脱敏与最小化（T-PYCONN-03/09）：日志输出默认脱敏密码、Token、个人身份信息，SQL 语句日志输出可配置开关。生产环境错误信息简化，避免暴露内部实现细节，详细错误仅记录到安全日志系统。日志文件权限建议为 0600，防止未授权访问。
- 审计与可追溯（T-PYCONN-15）：连接器支持可选审计日志功能，记录关键操作（如连接建立/断开、SQL 执行、数据修改），日志包含时间戳、用户标识、操作类型等，便于安全事件追溯。审计日志不包含敏感数据内容。
- 资源与速率保护（T-PYCONN-10/11/12）：支持连接池配置，包括最大连接数、超时、空闲回收等，防止连接耗尽。查询接口支持超时配置，防止慢查询占用资源。文档建议应用层实现写入速率限制，参数绑定和无模式写入接口对单次数据量设有合理上限。
- 依赖与供应链安全（T-PYCONN-13/14）：客户端库文件通过官方 PyPI 仓库分发，提供哈希值或数字签名供用户校验完整性。CI/CD 流程集成依赖安全扫描工具，定期检查并修复第三方依赖漏洞。
- 合规与隐私（T-PYCONN-08）：遵循 GDPR、CCPA 等数据保护法规，文档提供数据最小化和匿名化指导。开发与实现参考 OWASP 安全编码标准，适用场景下参考 PCI DSS 等行业标准。
- 错误处理（T-PYCONN-09）：生产环境下错误信息简化，避免返回堆栈、SQL 语句等内部细节，详细错误仅记录于安全日志。提供调试模式开关，便于开发环境排查问题。
- 文档与示例安全（全局）：所有用户文档和示例代码均展示安全最佳实践，包括凭据管理、加密连接、参数绑定等，避免误导用户采用不安全用法。

## 7. 兼容性

1. 对于原生连接，客户端驱动要求与 TDengine 版本一致。
2. 对于 WebSocket 接口：
   - 已发布的连接器，可以正常连接其支持版本之后的新版本 TDengine。
   - 新发布的连接器，可以要求必须至少工作的 TDengine 版本。

## 8. 运维

无

## 9. 使用场景

python 开发人员可以通过 websocket 或 native 与 taosadapter 建立连接，实现数据的读写和订阅。

## 10. 约束和限制

1. 仅支持  Python 3.0 及以上版本开发的 python 应用。
2. 原生连接方式，必须保证 taosc 驱动与 TDengine 版本一致性。
3. 不支持针对单条数据记录的删除操作。
4. 不支持事务操作。

## 11. 常见错误和排查

| Error Type | Description | Suggested Actions |
| --- | --- | --- |
| InterfaceError | taosc 版本太低，不支持所使用的接口 | 请检查 TDengine 客户端版本 |
| ConnectionError | 数据库链接错误 | 请检查 TDengine 服务端状态和连接参数 |
| DatabaseError | 数据库错误 | 请检查 TDengine 服务端版本，并将 Python 连接器升级到最新版 |
| OperationalError | 操作错误 | API 使用错误，请检查代码 |
| ProgrammingError | 接口调用错误 | 请检查提交的数据是否正确 |
| StatementError | stmt 相关异常 | 请检查绑定参数与 sql 是否匹配 |
| ResultError | 操作数据错误 | 请检查操作的数据与数据库中的数据类型是否匹配 |
| SchemalessError | schemaless 相关异常 | 请检查数据格式及对应的协议类型是否正确 |
| TmqError | tmq 相关异常 | 请检查 Topic 及 consumer 配置是否正确 |

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

如果应用使用扩展接口传递 reqId，则可以在后续模块如 taosc、taosAdapter 等日志中进行分析。

## 13. 安装和卸载

安装
```python
安装连接器命令如下：

## 14. 原生连接

pip3 install taospy

## 15. WebSocket 连接，可选装

pip3 install taos-ws-py
```

卸载
```python
pip3 uninstall taospy

pip3 uninstall taos-ws-py
```

## 16. 文档

需要修改官网文档。

## 17. 参考文档

1. [taosAdapter 参考手册](https://docs.taosdata.com/reference/components/taosadapter/)

## 18. 附录

无

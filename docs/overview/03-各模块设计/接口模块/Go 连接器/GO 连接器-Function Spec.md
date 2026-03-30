# GO 连接器-Function Spec

## 1. 变更历史

| 编写日期 | 发布日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-09 | 2025-01-09 | 1.0 | 谭雪峰 | 编写文档 |
| 2025-12-20 | 2025-12-20 | 2.0 | 谭雪峰 | 更新 STMT2 和数据类型 |

## 2. 背景

随着大数据和物联网技术的不断发展，时序数据库在数据存储和分析方面的优势日益凸显。TDengine作为一款高性能、可扩展的时序数据库，已经在众多领域得到了广泛应用。然而，对于Go语言开发者来说，直接操作TDengine数据库存在一定的门槛，需要熟悉 TDengine 的接口和通信协议。
为了满足 Go 语言开发者对 TDengine 数据库的需求，taosdata/driver-go 项目应运而生。该项目通过封装TDengine 数据库的底层操作，提供了简洁易用的 API 接口，使得 Go 语言开发者能够轻松实现数据库的连接、查询、插入等操作。同时，该项目还支持多种连接方式，包括原生、HTTP 和 WebSocket，可以根据具体的应用场景选择合适的连接方式。

## 3. 定义

1. **driver-go：** 是 TDengine 的 Go 语言驱动库，它实现了 Go 语言中的 database/sql 标准接口，使得开发者可以使用 Go 语言轻松连接和操作 TDengine 数据库。
2. **DSN (Data Source Name)：** 数据源名称，用于指定连接到数据库所需的信息。在 driver-go 中，DSN 的格式依赖于所使用的连接接口（如 RESTful、WebSocket 等），但通常包括用户名、密码、协议、主机地址、端口号和数据库名等信息。
3. **CGO：** CGO 是 Go 语言的一个工具，它允许 Go 程序调用 C 语言代码。在 driver-go 项目中，cgo 可能被用于实现与 TDengine C 客户端库的接口。
4. **Restful：** 一种网络应用程序的设计风格和开发方式，基于 HTTP 协议，可以使用 XML 或 JSON 格式传输数据。driver-go 项目中的 Restful 接口允许通过 HTTP 请求与 TDengine 数据库进行交互。
5. **WebSocket：** 一种在单个 TCP 连接上进行全双工通讯的协议。driver-go 项目中的 WebSocket 接口允许通过 WebSocket 协议与 TDengine 数据库进行实时数据交互。
6. **tmq 订阅：** TDengine 消息队列，支持订阅写入的数据。driver-go 项目中的 tmq 接口允许通过 WebSocket 和原生接口与进行订阅。

## 4. 行为说明

### 4.1 数据类型对应

TDengine 数据类型与 Go 类型对应关系见下表

| TDengine 数据类型 | Go 类型 |
| --- | --- |
| TIMESTAMP | time.Time |
| TINYINT | int8 |
| SMALLINT | int16 |
| INT | int32 |
| BIGINT | int64 |
| TINYINT UNSIGNED | uint8 |
| SMALLINT UNSIGNED | uint16 |
| INT UNSIGNED | uint32 |
| BIGINT UNSIGNED | uint64 |
| FLOAT | float32 |
| DOUBLE | float64 |
| BOOL | bool |
| BINARY | string |
| NCHAR | string |
| JSON | []byte |
| GEOMETRY | []byte |
| VARBINARY | []byte |
| DECIMAL | string |
| BLOB | []byte |

扫描反射类型对应关系如下
```go {wrap}
var (
    NullInt8    = reflect.TypeOf(types.NullInt8{})
    NullInt16   = reflect.TypeOf(types.NullInt16{})
    NullInt32   = reflect.TypeOf(types.NullInt32{})
    NullInt64   = reflect.TypeOf(types.NullInt64{})
    NullUInt8   = reflect.TypeOf(types.NullUInt8{})
    NullUInt16  = reflect.TypeOf(types.NullUInt16{})
    NullUInt32  = reflect.TypeOf(types.NullUInt32{})
    NullUInt64  = reflect.TypeOf(types.NullUInt64{})
    NullFloat32 = reflect.TypeOf(types.NullFloat32{})
    NullFloat64 = reflect.TypeOf(types.NullFloat64{})
    NullTime    = reflect.TypeOf(types.NullTime{})
    NullBool    = reflect.TypeOf(types.NullBool{})
    NullString  = reflect.TypeOf(types.NullString{})
    Bytes       = reflect.TypeOf([]byte{})
    NullJson    = reflect.TypeOf(types.NullJson{})
    UnknownType = reflect.TypeOf(new(interface{})).Elem()
)

var ColumnTypeMap = map[int]reflect.Type{
    TSDB_DATA_TYPE_BOOL:      NullBool,
    TSDB_DATA_TYPE_TINYINT:   NullInt8,
    TSDB_DATA_TYPE_SMALLINT:  NullInt16,
    TSDB_DATA_TYPE_INT:       NullInt32,
    TSDB_DATA_TYPE_BIGINT:    NullInt64,
    TSDB_DATA_TYPE_UTINYINT:  NullUInt8,
    TSDB_DATA_TYPE_USMALLINT: NullUInt16,
    TSDB_DATA_TYPE_UINT:      NullUInt32,
    TSDB_DATA_TYPE_UBIGINT:   NullUInt64,
    TSDB_DATA_TYPE_FLOAT:     NullFloat32,
    TSDB_DATA_TYPE_DOUBLE:    NullFloat64,
    TSDB_DATA_TYPE_BINARY:    NullString,
    TSDB_DATA_TYPE_NCHAR:     NullString,
    TSDB_DATA_TYPE_TIMESTAMP: NullTime,
    TSDB_DATA_TYPE_JSON:      NullJson,
    TSDB_DATA_TYPE_VARBINARY: Bytes,
    TSDB_DATA_TYPE_GEOMETRY:  Bytes,
    TSDB_DATA_TYPE_DECIMAL64: NullString,
    TSDB_DATA_TYPE_BLOB:      Bytes,
}
```

### 4.2 database/sql 驱动

`database/sql` 是 Go 语言标准库中提供的数据库操作包，它定义了通用的数据库操作接口，通过驱动注册机制来支持不同的数据库类型。
`database/sql` 定义了一组统一的编程接口供用户使用，如 `Prepare`、`Exec` 和 `Query` 等方法，用于准备 SQL 语句、执行 SQL 语句和执行查询等操作。这些方法会接收参数并调用底层驱动的相应方法来执行实际的数据库操作。
通过以上的机制，`database/sql` 包能够实现对不同数据库驱动的统一封装和调用。用户可以使用相同的编程接口来进行数据库操作，无需关心底层驱动的具体细节。这种设计使得代码更具可移植性和灵活性，方便切换和适配不同的数据库。

#### 4.2.1 原生连接

原生连接通过 TDengine 客户端（libtaos.so、taos.dll）与 TDengine 进行交互，并且实现了`database/sql/driver` 规定的以下接口：

| 接口名称 | 作用概述 |
| --- | --- |
| `Driver` | 数据库驱动必须实现的接口，定义了一个`Open`方法，该方法返回一个连接（`Conn`）和可能的错误。是数据库驱动与`database/sql`包进行交互的基础。 |
| `Connector` | 表示能够创建数据库连接的固定配置的驱动。定义了一个`Connect`方法，该方法根据提供的上下文（`context.Context`）返回一个连接（`Conn`）和可能的错误。 |
| `Pinger` | 可选接口，可能由`Conn`实现。定义了一个`Ping`方法，用于检查数据库连接是否仍然有效。如果连接无效，`Ping`方法可能会返回一个错误。 |
| `Execer` | 可选接口，定义了一个`Exec`方法，用于执行一个SQL命令（通常是INSERT、UPDATE或DELETE语句），并返回执行结果和可能的错误。 |
| `ExecerContext` | 与`Execer`类似，但添加了上下文（`context.Context`）支持。允许在执行SQL命令时传递上下文信息，以便进行超时控制、取消操作等。 |
| `Queryer` | 可选接口，定义了一个`Query`方法，用于执行一个SQL查询语句，并返回一个`Rows`结果集和可能的错误。 |
| `QueryerContext` | 与`Queryer`类似，但添加了上下文（`context.Context`）支持。允许在执行SQL查询时传递上下文信息。 |
| `Conn` | 表示一个数据库连接。它定义了多个方法，用于执行SQL命令、查询、管理事务等。 |
| `Stmt` | 表示一个预处理语句（prepared statement）。它定义了执行语句、查询、关闭等方法。 |
| `Rows` | 表示一个SQL查询的结果集。它定义了多个方法，用于遍历结果集中的行、获取列值等 |
| `RowsColumnTypeScanType` | 可选接口，它定义了一个`ColumnTypeScanType`方法，用于获取指定列的扫描类型（scan type） |
| `RowsColumnTypeDatabaseTypeName` | 可选接口，它定义了一个 `ColumnTypeDatabaseTypeName`方法，用于获取指定列的数据库类型名称 |
| `RowsColumnTypeLength` | 可选接口，它定义了一个 `RowsColumnTypeLength` 方法，用于获取指定列的类型长度 |
| `NamedValueChecker` | 可选接口，它定义了一个`CheckNamedValue`方法，用于检查命名参数的有效性 |

引入` ``github.com/taosdata/driver-go/v3/taosSql` 驱动后，应用可以通过 `database/sql` 提供的接口进行数据写入和查询操作。

##### 4.2.1.1 DSN

1. DSN 规范
数据源名称具有通用格式（方括号表示可选）：
`[username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]`
完整形式的 DSN：
`username:password@protocol(address)/dbname?param=value`
导入驱动：
```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v3/taosSql"
)
```

使用 `taosSql` 作为 `driverName` 并且使用一个正确的 DSN 作为 `dataSourceName` 如下：
```go
var taosUri = "root:taosdata@tcp(localhost:6030)/"
taos, err := sql.Open("taosSql", taosUri)
```

支持的 DSN 参数：
- `interpolateParams` 启用客户端占位符替换。
- `cgoThread` 指定 cgo 同时执行的数量，默认为系统核数。
- `cgoAsyncHandlerPoolSize` 指定异步函数的 handle 大小，默认为 10000。
- `loc`时区，目前没有使用。
- `timezone` 连接上使用的时区，影响 sql 解析与查询结果解析
- 其他参数，当做 TDengine 客户端参数设置（调用 C 接口 `taos_set_config`）。
支持的 protocol 参数：
- 当指定 protocol 为 `cfg` 时`address` 为 TDengine 配置文件路径（taos.cfg 路径），例如
  ```go {wrap}
  root:taosdata@cfg(/home/taos)/db
  ```

  设置 TDengine 配置文件路径为 `/home/taos`
- 当为其他值时使用`address`连接 TDengine
  - address 格式：`host:port`
    - host 为 TDengine 部署机器的 FQDN。
    - port 为 TDengine 开放的端口（默认6030）。

##### 4.2.1.2 创建连接

1. 方法签名：
  - `func Open(driverName, dataSourceName string) (*DB, error)`
    - **接口说明**：(`database/sql`)连接数据库
    - **参数说明**：
      - `driverName`：驱动名称。
      - `dataSourceName`：连接参数 DSN。
    - **返回值**：连接对象，错误信息。
1. 引入驱动
   - 驱动名称：`taosSql`。
   - 引入驱动包 `_ "``github.com/taosdata/driver-go/v3/taosSql``"`。
2. 样例
  ```go {wrap}
  package main
  
  import (
          "database/sql"
          "fmt"
          "log"
  
          _ "github.com/taosdata/driver-go/v3/taosSql"
  )
  
  func main() {
          var taosDSN = "root:taosdata@tcp(localhost:6030)/"
          taos, err := sql.Open("taosSql", taosDSN)
          if err != nil {
                  log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
          }
          fmt.Println("Connected to " + taosDSN + " successfully.")
          defer taos.Close()
  }
  
  ```

##### 4.2.1.3 写入

1. 常用接口描述
  - `func (db *DB) Exec(query string, args ...any) (Result, error)`
    - **接口说明**：执行查询但不返回任何行。
    - **参数说明**：
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：Result 对象（只有影响行数），错误信息。
  - `func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (Result, error)`
    - **接口说明**：执行查询但不返回任何行。
    - **参数说明**：
      - `ctx`：上下文，使用 Value 传递请求 id 进行链路追踪，key 为 `taos_req_id` value 为 int64 类型值。
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：结果 Result 对象（只有影响行数），错误信息。
1. 写入样例
   - 创建数据库 power。
   - 创建表 meters。
   - 使用自动建表写入数据。
  ```go {wrap}
  package main
  
  import (
      "database/sql"
      "fmt"
      "log"
  
      _ "github.com/taosdata/driver-go/v3/taosSql"
  )
  
  func main() {
      var taosDSN = "root:taosdata@tcp(localhost:6030)/"
      db, err := sql.Open("taosSql", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to " + taosDSN + " successfully.")
      defer db.Close()
      res, err := db.Exec("CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      rowsAffected, err := res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create database power successfully, rowsAffected: ", rowsAffected)
      res, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
      if err != nil {
         log.Fatalln("Failed to create stable meters, ErrMessage: " + err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create stable rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create stable power.meters successfully, rowsAffected:", rowsAffected)
      insertQuery := "INSERT INTO " +
         "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
         "VALUES " +
         "(NOW + 1a, 10.30000, 219, 0.31000) " +
         "(NOW + 2a, 12.60000, 218, 0.33000) " +
         "(NOW + 3a, 12.30000, 221, 0.31000) " +
         "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
         "VALUES " +
         "(NOW + 1a, 10.30000, 218, 0.25000) "
      res, err = db.Exec(insertQuery)
      if err != nil {
         log.Fatalf("Failed to insert data to power.meters, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalf("Failed to get insert rowsAffected, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
      }
      fmt.Printf("Successfully inserted %d rows to power.meters.\n", rowsAffected)
  }
  ```

1. 带 req_id 的写入样例
   - 使用 common.GetReqID() 生成 req_id。
   - 使用 `db.ExecContext` 传入 req_id 并执行 sql。
  ```go {wrap}
  package main
  
  import (
      "context"
      "database/sql"
      "fmt"
      "log"
  
      "github.com/taosdata/driver-go/v3/common"
      _ "github.com/taosdata/driver-go/v3/taosSql"
  )
  
  func main() {
      var taosDSN = "root:taosdata@tcp(localhost:6030)/"
      db, err := sql.Open("taosSql", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to " + taosDSN + " successfully.")
      defer db.Close()
      reqId := common.GetReqID()
      log.Println("Request ID: ", reqId)
      ctx := context.WithValue(context.Background(), "taos_req_id", reqId)
      res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      rowsAffected, err := res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create database power successfully, rowsAffected: ", rowsAffected)
  }
  ```

##### 4.2.1.4 查询

1. 常用接口描述
  - `func (db *DB) Query(query string, args ...any) (*Rows, error)`
    - **接口说明**：执行查询并返回行的结果。
    - **参数说明**：
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：Rows 对象，错误信息。
  - `func (rs *Rows) Next() bool`
    - **接口说明**：准备下一行数据。
    - **返回值**：是否有下一行数据。
  - `func (rs *Rows) Columns() ([]string, error)`
    - **接口说明**：返回列名。
    - **返回值**：列名，错误信息。
  `func (rs *Rows) Scan(dest ...any) error`
  **接口说明**：将当前行的列值复制到 dest 指向的值中。
  **参数说明**：
  `dest`：目标值。
  **返回值**：错误信息。
  - `func (rs *Rows) Close() error`
    - **接口说明**：关闭行（如果使用 Next 获取完全部数据则不需要调用 Close）。
    - **返回值**：错误信息。
  - `func (r *Row) Scan(dest ...any) error`
    - **接口说明**：将当前行的列值复制到 dest 指向的值中。
    - **参数说明**：
      - `dest`：目标值。
    - **返回值**：错误信息。
  - `func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error)`
    - **接口说明**：执行查询并返回行结果。
    - **参数说明**：
      - `ctx`：上下文，使用 Value 传递请求 id 进行链路追踪，key 为 `taos_req_id` value 为 int64 类型值。
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：结果集 Rows 对象，错误信息。
1. 查询样例
   - 执行 SQL `SELECT ts, current, location FROM power.meters limit 100`。
   - 获取结果。
  ```go {wrap}
  package main
  
  import (
      "database/sql"
      "fmt"
      "log"
      "time"
  
      _ "github.com/taosdata/driver-go/v3/taosSql"
  )
  
  func main() {
      var taosDSN = "root:taosdata@tcp(localhost:6030)/"
      db, err := sql.Open("taosSql", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to " + taosDSN + " successfully.")
      defer db.Close()
      querySql := "SELECT ts, current, location FROM power.meters limit 100"
      rows, err := db.Query(querySql)
      if err != nil {
         log.Fatalf("Failed to query data from power.meters, sql: %s, ErrMessage: %s\n", querySql, err.Error())
      }
      columns,err := rows.Columns()
      if err != nil {
         log.Fatalf("Failed to get columns, sql: %s, ErrMessage: %s\n", querySql, err.Error())
      }
      fmt.Println("Columns: ", columns)
      for rows.Next() {
         // Add your data processing logic here
         var (
            ts       time.Time
            current  float32
            location string
         )
         err = rows.Scan(&ts, &current, &location)
         if err != nil {
            log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", querySql, err)
         }
         fmt.Printf("ts: %s, current: %f, location: %s\n", ts, current, location)
      }
  }
  ```

1. 带 req_id 的查询样例
   - 使用 common.GetReqID() 生成 req_id。
   - 使用 `db.QueryContext`传入 req_id 并执行 sql `SELECT ts, current, location FROM power.meters limit 100`。
   - 获取结果。
  ```go {wrap}
  package main
  
  import (
      "context"
      "database/sql"
      "fmt"
      "log"
      "time"
  
      "github.com/taosdata/driver-go/v3/common"
      _ "github.com/taosdata/driver-go/v3/taosSql"
  )
  
  func main() {
      var taosDSN = "root:taosdata@tcp(localhost:6030)/"
      db, err := sql.Open("taosSql", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to " + taosDSN + " successfully.")
      defer db.Close()
      querySql := "SELECT ts, current, location FROM power.meters limit 100"
      reqId := common.GetReqID()
      fmt.Println("Request ID: ", reqId)
      ctx := context.WithValue(context.Background(), "taos_req_id", reqId)
      rows, err := db.QueryContext(ctx, querySql)
      if err != nil {
         log.Fatalf("Failed to query data from power.meters, sql: %s, ErrMessage: %s\n", querySql, err.Error())
      }
      columns, err := rows.Columns()
      if err != nil {
         log.Fatalf("Failed to get columns, sql: %s, ErrMessage: %s\n", querySql, err.Error())
      }
      fmt.Println("Columns: ", columns)
      for rows.Next() {
         // Add your data processing logic here
         var (
            ts       time.Time
            current  float32
            location string
         )
         err = rows.Scan(&ts, &current, &location)
         if err != nil {
            log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", querySql, err)
         }
         fmt.Printf("ts: %s, current: %f, location: %s\n", ts, current, location)
      }
  }
  ```

##### 4.2.1.5 数据绑定

参数绑定只支持写入语句绑定列，查询语句绑定查询参数，不支持写入语句绑定 table_name 和 tag
1. 常用接口描述
  - `Prepare(query string) (Stmt, error)`
    - **接口说明**：准备返回一个与此连接绑定的准备好的语句(statement)。
    - **参数说明**：
      - `query`：要进行参数绑定的语句。
    - **返回值**：Stmt 对象，错误信息。
  - `func (s *Stmt) Exec(args ...any) (Result, error)`
    - **接口说明**：使用给定的参数执行准备好的语句并返回总结该语句效果的结果（只可以绑定列值，不支持绑定表名和 tag）。
    - **参数说明**：
      - `args`：命令参数，Go 原始类型会自动转换数据库类型，类型不匹配可能会丢精度，建议使用与数据库相同的类型，时间类型使用 int64 或 `RFC3339Nano` 格式化后的字符串。
    - **返回值**：结果 Result 对象（只有影响行数），错误信息。
  - `func (s *Stmt) Query(args ...any) (*Rows, error)`
    - **接口说明**：使用给定的参数执行准备好的语句并返回行的结果。
    - **参数说明**：
      - `args`：命令参数，Go 原始类型会自动转换数据库类型，类型不匹配可能会丢精度，建议使用与数据库相同的类型，时间类型使用 int64 或 `RFC3339Nano` 格式化后的字符串。
    - **返回值**：结果集 Rows 对象，错误信息。
  - `func (s *Stmt) Close() error`
    - **接口说明**：关闭语句。
    - **返回值**：错误信息。
1. 写入和查询样例
   - 创建数据库 power 超级表 meters 普通表 d1001。
   - 创建 stmt 实例准备语句 `INSERT INTO power.d1001 VALUES (?, ?, ?, ?)`。
   - 绑定参数并执行 now, 10.30000, 219, 0.31000。
   - 创建 stmt 准备查询语句 `SELECT * FROM power.d1001 WHERE ts = ?`。
   - 绑定参数并执行 now。
   - 获取查询结果。
  ```go {wrap}
  package main
  
  import (
      "database/sql"
      "fmt"
      "log"
      "time"
  
      _ "github.com/taosdata/driver-go/v3/taosSql"
  )
  
  func main() {
      var taosDSN = "root:taosdata@tcp(localhost:6030)/"
      db, err := sql.Open("taosSql", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to " + taosDSN + " successfully.")
      defer db.Close()
      // 创建数据库，超级表和普通表
      res, err := db.Exec("CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      rowsAffected, err := res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create database power successfully, rowsAffected: ", rowsAffected)
      res, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
      if err != nil {
         log.Fatalln("Failed to create stable meters, ErrMessage: " + err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create stable rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create stable power.meters successfully, rowsAffected:", rowsAffected)
      res, err = db.Exec("CREATE TABLE IF NOT EXISTS power.d1001 USING power.meters TAGS(2,'California.SanFrancisco')")
      if err != nil {
         log.Fatalln("Failed to create table d1001, ErrMessage: " + err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create table rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create table power.d1001 successfully, rowsAffected:", rowsAffected)
  
      // 插入数据
      stmt, err := db.Prepare("INSERT INTO power.d1001 VALUES (?, ?, ?, ?)")
      if err != nil {
         log.Fatalln("Failed to prepare insert statement, ErrMessage: " + err.Error())
      }
      defer stmt.Close()
      now := time.Now()
      res, err = stmt.Exec(now, 10.30000, 219, 0.31000)
      if err != nil {
         log.Fatalln("Failed to insert data, ErrMessage: " + err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get insert data rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Insert data successfully, rowsAffected:", rowsAffected)
      // 查询数据
      queryStmt, err := db.Prepare("SELECT * FROM power.d1001 WHERE ts = ?")
      if err != nil {
         log.Fatalln("Failed to prepare query statement, ErrMessage: " + err.Error())
      }
      defer queryStmt.Close()
      rows, err := queryStmt.Query(now)
      if err != nil {
         log.Fatalln("Failed to query data, ErrMessage: " + err.Error())
      }
      defer rows.Close()
      for rows.Next() {
         var (
            ts      time.Time
            current float64
            voltage int
            phase   float64
         )
         err = rows.Scan(&ts, &current, &voltage, &phase)
         if err != nil {
            log.Fatalln("Failed to scan data, ErrMessage: " + err.Error())
         }
         fmt.Printf("ts: %v, current: %v, voltage: %v, phase: %v\n", ts, current, voltage, phase)
      }
  } 
  ```

#### 4.2.2 WebSocket连接

WebSocket 连接通过 WebSocket 协议与 taosAdapter 进行交互，并且实现了`database/sql/driver` 规定的以下接口：

| 接口名称 | 作用概述 |
| --- | --- |
| `Driver` | 数据库驱动必须实现的接口，定义了一个`Open`方法，该方法返回一个连接（`Conn`）和可能的错误。是数据库驱动与`database/sql`包进行交互的基础。 |
| `Connector` | 表示能够创建数据库连接的固定配置的驱动。定义了一个`Connect`方法，该方法根据提供的上下文（`context.Context`）返回一个连接（`Conn`）和可能的错误。 |
| `Pinger` | 可选接口，可能由`Conn`实现。定义了一个`Ping`方法，用于检查数据库连接是否仍然有效。如果连接无效，`Ping`方法可能会返回一个错误。 |
| `Execer` | 可选接口，可定义了一个`Exec`方法，用于执行一个SQL命令（通常是INSERT、UPDATE或DELETE语句），并返回执行结果和可能的错误。 |
| `ExecerContext` | 与`Execer`类似，但添加了上下文（`context.Context`）支持。允许在执行SQL命令时传递上下文信息，以便进行超时控制、取消操作等。 |
| `Queryer` | 可选接口，定义了一个`Query`方法，用于执行一个SQL查询语句，并返回一个`Rows`结果集和可能的错误。 |
| `QueryerContext` | 与`Queryer`类似，但添加了上下文（`context.Context`）支持。允许在执行SQL查询时传递上下文信息。 |
| `Conn` | 表示一个数据库连接。它定义了多个方法，用于执行SQL命令、查询、管理事务等。 |
| `Stmt` | 表示一个预处理语句（prepared statement）。它定义了执行语句、查询、关闭等方法。 |
| `Rows` | 表示一个SQL查询的结果集。它定义了多个方法，用于遍历结果集中的行、获取列值等 |
| `RowsColumnTypeDatabaseTypeName` | 可选接口，它定义了一个 `ColumnTypeDatabaseTypeName`方法，用于获取指定列的数据库类型名称 |
| `RowsColumnTypeLength` | 可选接口，它定义了一个 `RowsColumnTypeLength` 方法，用于获取指定列的类型长度 |
| `RowsColumnTypeScanType` | 可选接口，它定义了一个`ColumnTypeScanType`方法，用于获取指定列的扫描类型（scan type） |
| `NamedValueChecker` | 可选接口，它定义了一个`CheckNamedValue`方法，用于检查命名参数的有效性 |

##### 4.2.2.1 DSN

1. DSN 规范
数据源名称具有通用格式（方括号表示可选）：
`[username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]`
完整形式的 DSN：
`username:password@protocol(address)/dbname?param=value`
导入驱动：
```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v3/taosWS"
)
```

使用 `taosWS` 作为 `driverName` 并且使用一个正确的 DSN 作为 `dataSourceName` 如下：
```go
var taosUri = "root:taosdata@ws(localhost:6041)/"
taos, err := sql.Open("taosWS", taosUri)
```

支持的 DSN 参数：
- `interpolateParams` 启用客户端占位符替换。
- `enableCompression` 是否发送压缩数据，默认为 false 不发送压缩数据，如果传输数据使用压缩设置为 true。
- `token` 连接云服务使用的验证信息。
- `readTimeout` 读取数据的超时时间，默认为 5m。
- `writeTimeout` 写入数据的超时时间，默认为 10s。
- `timezone` 连接上使用的时区，影响 sql 解析与查询结果解析
支持的 protocol 参数：
- `ws` 连接使用 ws 协议
- `wss` 连接使用 wss 协议
address 格式：
`host:port`
- Host 为 taosAdapter 部署机器的域名或 ip ，如果配置负载均衡则为负载均衡的域名或 ip
- port 为 taosAdapter 开放的端口，如果配置负载均衡则为负载均衡的端口

##### 4.2.2.2 创建连接

1. 方法签名：
  - `func Open(driverName, dataSourceName string) (*DB, error)`
    - **接口说明**：(`database/sql`)连接数据库
    - **参数说明**：
      - `driverName`：驱动名称。
      - `dataSourceName`：连接参数 DSN。
    - **返回值**：连接对象，错误信息。
1. 引入驱动
   - 驱动名称：`taosWS`。
   - 引入驱动包 `_ "``github.com/taosdata/driver-go/v3/``taosWS"`。
2. 样例
   - ws 连接
  ```go {wrap}
  package main
  
  import (
          "database/sql"
          "fmt"
          "log"
  
          _ "github.com/taosdata/driver-go/v3/taosWS"
  )
  
  func main() {
          var taosDSN = "root:taosdata@ws(localhost:6041)/"
          taos, err := sql.Open("taosWS", taosDSN)
          if err != nil {
                  log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
          }
          fmt.Println("Connected to " + taosDSN + " successfully.")
          defer taos.Close()
  }
  ```

   - wss 连接
  ```go {wrap}
  package main
  
  import (
          "database/sql"
          "fmt"
          "log"
  
          _ "github.com/taosdata/driver-go/v3/taosWS"
  )
  
  func main() {
          var taosDSN = "root:taosdata@wss(localhost:6041)/"
          taos, err := sql.Open("taosWS", taosDSN)
          if err != nil {
                  log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
          }
          fmt.Println("Connected to " + taosDSN + " successfully.")
          defer taos.Close()
  }
  ```

   - 连接云服务
  ```go {wrap}
  package main
  
  import (
          "database/sql"
          "fmt"
          "log"
  
          _ "github.com/taosdata/driver-go/v3/taosWS"
  )
  
  func main() {
          var taosDSN = "wss(gw.cloud.taosdata.com:443)/?token=xxxx"
          taos, err := sql.Open("taosWS", taosDSN)
          if err != nil {
                  log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
          }
          fmt.Println("Connected to " + taosDSN + " successfully.")
          defer taos.Close()
  }
  ```

##### 4.2.2.3 写入

1. 常用接口描述
  - `func (db *DB) Exec(query string, args ...any) (Result, error)`
    - **接口说明**：执行查询但不返回任何行。
    - **参数说明**：
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：Result 对象（只有影响行数），错误信息。
  - `func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (Result, error)`
    - **接口说明**：执行查询但不返回任何行。
    - **参数说明**：
      - `ctx`：上下文，暂不支持 req_id。
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：结果 Result 对象（只有影响行数），错误信息。
1. 写入样例
   - 创建数据库 power。
   - 创建表 meters。
   - 使用自动建表写入数据。
  ```go {wrap}
  package main
  
  import (
      "database/sql"
      "fmt"
      "log"
  
      _ "github.com/taosdata/driver-go/v3/taosWS"
  )
  
  func main() {
      var taosDSN = "root:taosdata@ws(localhost:6041)/"
      db, err := sql.Open("taosWS", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to " + taosDSN + " successfully.")
      defer db.Close()
      res, err := db.Exec("CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      rowsAffected, err := res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create database power successfully, rowsAffected: ", rowsAffected)
      res, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
      if err != nil {
         log.Fatalln("Failed to create stable meters, ErrMessage: " + err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create stable rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create stable power.meters successfully, rowsAffected:", rowsAffected)
      insertQuery := "INSERT INTO " +
         "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
         "VALUES " +
         "(NOW + 1a, 10.30000, 219, 0.31000) " +
         "(NOW + 2a, 12.60000, 218, 0.33000) " +
         "(NOW + 3a, 12.30000, 221, 0.31000) " +
         "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
         "VALUES " +
         "(NOW + 1a, 10.30000, 218, 0.25000) "
      res, err = db.Exec(insertQuery)
      if err != nil {
         log.Fatalf("Failed to insert data to power.meters, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalf("Failed to get insert rowsAffected, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
      }
      fmt.Printf("Successfully inserted %d rows to power.meters.\n", rowsAffected)
  }
  ```

##### 4.2.2.4 查询

1. 常用接口描述
  - `func (db *DB) Query(query string, args ...any) (*Rows, error)`
    - **接口说明**：执行查询并返回行的结果。
    - **参数说明**：
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：Rows 对象，错误信息。
  - `func (rs *Rows) Next() bool`
    - **接口说明**：准备下一行数据。
    - **返回值**：是否有下一行数据。
  - `func (rs *Rows) Columns() ([]string, error)`
    - **接口说明**：返回列名。
    - **返回值**：列名，错误信息。
  - `func (rs *Rows) Scan(dest ...any) error`
    - **接口说明**：将当前行的列值复制到 dest 指向的值中。
    - **参数说明**：
      - `dest`：目标值。
    - **返回值**：错误信息。
  - `func (rs *Rows) Close() error`
    - **接口说明**：关闭行（如果使用 Next 获取完全部数据则不需要调用 Close）。
    - **返回值**：错误信息。
  - `func (r *Row) Scan(dest ...any) error`
    - **接口说明**：将当前行的列值复制到 dest 指向的值中。
    - **参数说明**：
      - `dest`：目标值。
    - **返回值**：错误信息。
  - `func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error)`
    - **接口说明**：执行查询并返回行结果。
    - **参数说明**：
      - `ctx`：上下文，暂不支持 req_id。
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：结果集 Rows 对象，错误信息。
1. 查询样例
   - 执行 SQL `SELECT ts, current, location FROM power.meters limit 100`。
   - 获取结果。
  ```go {wrap}
  package main
  
  import (
      "database/sql"
      "fmt"
      "log"
      "time"
  
      _ "github.com/taosdata/driver-go/v3/taosWS"
  )
  
  func main() {
      var taosDSN = "root:taosdata@ws(localhost:6041)/"
      db, err := sql.Open("taosWS", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to " + taosDSN + " successfully.")
      defer db.Close()
      querySql := "SELECT ts, current, location FROM power.meters limit 100"
      rows, err := db.Query(querySql)
      if err != nil {
         log.Fatalf("Failed to query data from power.meters, sql: %s, ErrMessage: %s\n", querySql, err.Error())
      }
      columns, err := rows.Columns()
      if err != nil {
         log.Fatalf("Failed to get columns, sql: %s, ErrMessage: %s\n", querySql, err.Error())
      }
      fmt.Println("Columns: ", columns)
      for rows.Next() {
         // Add your data processing logic here
         var (
            ts       time.Time
            current  float32
            location string
         )
         err = rows.Scan(&ts, &current, &location)
         if err != nil {
            log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", querySql, err)
         }
         fmt.Printf("ts: %s, current: %f, location: %s\n", ts, current, location)
      }
  }
  ```

##### 4.2.2.5 数据绑定

参数绑定只支持写入语句绑定列，查询语句绑定查询参数，不支持写入语句绑定 table_name 和 tag
1. 常用接口描述
  - `Prepare(query string) (Stmt, error)`
    - **接口说明**：准备返回一个与此连接绑定的准备好的语句(statement)。
    - **参数说明**：
      - `query`：要进行参数绑定的语句。
    - **返回值**：Stmt 对象，错误信息。
  - `func (s *Stmt) Exec(args ...any) (Result, error)`
    - **接口说明**：使用给定的参数执行准备好的语句并返回总结该语句效果的结果（只可以绑定列值，不支持绑定表名和 tag）。
    - **参数说明**：
      - `args`：命令参数，Go 原始类型会自动转换数据库类型，类型不匹配可能会丢精度，建议使用与数据库相同的类型，时间类型使用 int64 或 `RFC3339Nano` 格式化后的字符串。
    - **返回值**：结果 Result 对象（只有影响行数），错误信息。
  - `func (s *Stmt) Query(args ...any) (*Rows, error)`
    - **接口说明**：使用给定的参数执行准备好的语句并返回行的结果。
    - **参数说明**：
      - `args`：命令参数，Go 原始类型会自动转换数据库类型，类型不匹配可能会丢精度，建议使用与数据库相同的类型，时间类型使用 int64 或 `RFC3339Nano` 格式化后的字符串。
    - **返回值**：结果集 Rows 对象，错误信息。
  - `func (s *Stmt) Close() error`
    - **接口说明**：关闭语句。
    - **返回值**：错误信息。
1. 写入和查询样例
   - 创建数据库 power 超级表 meters 普通表 d1001。
   - 创建 stmt 实例准备语句 `INSERT INTO power.d1001 VALUES (?, ?, ?, ?)`。
   - 绑定参数并执行 now, 10.30000, 219, 0.31000。
   - 创建 stmt 准备查询语句 `SELECT * FROM power.d1001 WHERE ts = ?`。
   - 绑定参数并执行 now。
   - 获取查询结果。
  ```go {wrap}
  package main
  
  import (
      "database/sql"
      "fmt"
      "log"
      "time"
  
      _ "github.com/taosdata/driver-go/v3/taosWS"
  )
  
  func main() {
      var taosDSN = "root:taosdata@ws(localhost:6041)/"
      db, err := sql.Open("taosWS", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to " + taosDSN + " successfully.")
      defer db.Close()
      // 创建数据库，超级表和普通表
      res, err := db.Exec("CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      rowsAffected, err := res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create database power successfully, rowsAffected: ", rowsAffected)
      res, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
      if err != nil {
         log.Fatalln("Failed to create stable meters, ErrMessage: " + err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create stable rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create stable power.meters successfully, rowsAffected:", rowsAffected)
      res, err = db.Exec("CREATE TABLE IF NOT EXISTS power.d1001 USING power.meters TAGS(2,'California.SanFrancisco')")
      if err != nil {
         log.Fatalln("Failed to create table d1001, ErrMessage: " + err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create table rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create table power.d1001 successfully, rowsAffected:", rowsAffected)
  
      // 插入数据
      stmt, err := db.Prepare("INSERT INTO power.d1001 VALUES (?, ?, ?, ?)")
      if err != nil {
         log.Fatalln("Failed to prepare insert statement, ErrMessage: " + err.Error())
      }
      defer stmt.Close()
      now := time.Now()
      res, err = stmt.Exec(now, 10.30000, 219, 0.31000)
      if err != nil {
         log.Fatalln("Failed to insert data, ErrMessage: " + err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get insert data rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Insert data successfully, rowsAffected:", rowsAffected)
      // 查询数据
      queryStmt, err := db.Prepare("SELECT * FROM power.d1001 WHERE ts = ?")
      if err != nil {
         log.Fatalln("Failed to prepare query statement, ErrMessage: " + err.Error())
      }
      defer queryStmt.Close()
      rows, err := queryStmt.Query(now)
      if err != nil {
         log.Fatalln("Failed to query data, ErrMessage: " + err.Error())
      }
      defer rows.Close()
      for rows.Next() {
         var (
            ts      time.Time
            current float64
            voltage int
            phase   float64
         )
         err = rows.Scan(&ts, &current, &voltage, &phase)
         if err != nil {
            log.Fatalln("Failed to scan data, ErrMessage: " + err.Error())
         }
         fmt.Printf("ts: %v, current: %v, voltage: %v, phase: %v\n", ts, current, voltage, phase)
      }
  }
  ```

#### 4.2.3 RESTful 连接

RESTful 连接通过 http 协议与 taosAdapter 进行交互，并且实现了`database/sql/driver` 规定的以下接口：

| 接口名称 | 作用概述 |
| --- | --- |
| `Driver` | 数据库驱动必须实现的接口，定义了一个`Open`方法，该方法返回一个连接（`Conn`）和可能的错误。是数据库驱动与`database/sql`包进行交互的基础。 |
| `Connector` | 表示能够创建数据库连接的固定配置的驱动。定义了一个`Connect`方法，该方法根据提供的上下文（`context.Context`）返回一个连接（`Conn`）和可能的错误。 |
| `Pinger` | 可选接口，可能由`Conn`实现。定义了一个`Ping`方法，用于检查数据库连接是否仍然有效。如果连接无效，`Ping`方法可能会返回一个错误。 |
| `Execer` | 可选接口，可定义了一个`Exec`方法，用于执行一个SQL命令（通常是INSERT、UPDATE或DELETE语句），并返回执行结果和可能的错误。 |
| `ExecerContext` | 与`Execer`类似，但添加了上下文（`context.Context`）支持。允许在执行SQL命令时传递上下文信息，以便进行超时控制、取消操作等。 |
| `Queryer` | 可选接口，定义了一个`Query`方法，用于执行一个SQL查询语句，并返回一个`Rows`结果集和可能的错误。 |
| `QueryerContext` | 与`Queryer`类似，但添加了上下文（`context.Context`）支持。允许在执行SQL查询时传递上下文信息。 |
| `Conn` | 表示一个数据库连接。它定义了多个方法，用于执行SQL命令、查询、管理事务等。 |
| `Rows` | 表示一个SQL查询的结果集。它定义了多个方法，用于遍历结果集中的行、获取列值等 |
| `RowsColumnTypeDatabaseTypeName` | 可选接口，它定义了一个 `ColumnTypeDatabaseTypeName`方法，用于获取指定列的数据库类型名称 |
| `RowsColumnTypeLength` | 可选接口，它定义了一个 `RowsColumnTypeLength` 方法，用于获取指定列的类型长度 |
| `RowsColumnTypeScanType` | 可选接口，它定义了一个`ColumnTypeScanType`方法，用于获取指定列的扫描类型（scan type） |

RESTful 仅支持 SQL 的插入和查询，无参数绑定功能。

##### 4.2.3.1 DSN

1. DSN 规范
数据源名称具有通用格式（方括号表示可选）：
`[username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]`
完整形式的 DSN：
`username:password@protocol(address)/dbname?param=value`
导入驱动：
```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v3/taosRestful"
)
```

使用 `taosRestful` 作为 `driverName` 并且使用一个正确的 DSN 作为 `dataSourceName` 如下：
```go
var taosUri = "root:taosdata@http(localhost:6041)/"
taos, err := sql.Open("taosRestful", taosUri)
```

支持的 DSN 参数：
- `interpolateParams` 启用客户端占位符替换。
- `disableCompression` 是否接受压缩数据，默认为 true 不接受压缩数据，如果传输数据使用 gzip 压缩设置为 false。
- `readBufferSize` 读取数据的缓存区大小默认为 4K（4096），当查询结果数据量多时可以适当调大该值。
- `token` 连接云服务时使用的 token。
- `skipVerify` 是否跳过证书验证，默认为 false 不跳过证书验证，如果连接的是不安全的服务设置为 true。
- `timezone` 连接上使用的时区，影响 sql 解析与查询结果解析
支持的 protocol 参数：
- `http` 连接使用 http 协议。
- `https`连接使用 http 协议。
address 格式：
`host:port`
- `host` 为 taosAdapter 部署机器的域名或 ip ，如果配置负载均衡则为负载均衡的域名或 ip。
- `port`为 taosAdapter 开放的端口，如果配置负载均衡则为负载均衡的端口。

##### 4.2.3.2 创建连接

1. 方法签名：
  - `func Open(driverName, dataSourceName string) (*DB, error)`
    - **接口说明**：(`database/sql`)连接数据库
    - **参数说明**：
      - `driverName`：驱动名称。
      - `dataSourceName`：连接参数 DSN。
    - **返回值**：连接对象，错误信息。
1. 引入驱动
   - 驱动名称：`taosRestful`。
   - 引入驱动包 `_ "``github.com/taosdata/driver-go/v3/taos``Restful"`。
2. 样例
   - 普通连接
    ```go {wrap}
    package main
    
    import (
            "database/sql"
            "fmt"
            "log"
    
            _ "github.com/taosdata/driver-go/v3/taosRestful"
    )
    
    func main() {
            var taosDSN = "root:taosdata@http(localhost:6041)/"
            taos, err := sql.Open("taosRestful", taosDSN)
            if err != nil {
                    log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
            }
            fmt.Println("Connected to " + taosDSN + " successfully.")
            defer taos.Close()
    }
    ```

   - https 连接
    ```go {wrap}
    package main
    
    import (
            "database/sql"
            "fmt"
            "log"
    
            _ "github.com/taosdata/driver-go/v3/taosRestful"
    )
    
    func main() {
            var taosDSN = "root:taosdata@https(localhost:6041)/"
            taos, err := sql.Open("taosRestful", taosDSN)
            if err != nil {
                    log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
            }
            fmt.Println("Connected to " + taosDSN + " successfully.")
            defer taos.Close()
    }
    ```

   - 连接云服务
    ```go {wrap}
    package main
    
    import (
            "database/sql"
            "fmt"
            "log"
    
            _ "github.com/taosdata/driver-go/v3/taosRestful"
    )
    
    func main() {
            var taosDSN = "https(gw.cloud.taosdata.com:443)/?token=xxxx"
            taos, err := sql.Open("taosRestful", taosDSN)
            if err != nil {
                    log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
            }
            fmt.Println("Connected to " + taosDSN + " successfully.")
            defer taos.Close()
    }
    ```

##### 4.2.3.3 写入

1. 常用接口描述
  - `func (db *DB) Exec(query string, args ...any) (Result, error)`
    - **接口说明**：执行查询但不返回任何行。
    - **参数说明**：
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：Result 对象（只有影响行数），错误信息。
  - `func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (Result, error)`
    - **接口说明**：执行查询但不返回任何行。
    - **参数说明**：
      - `ctx`：上下文，暂不支持 req_id。
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：结果 Result 对象（只有影响行数），错误信息。
1. 写入样例
   - 创建数据库 power。
   - 创建表 meters。
   - 使用自动建表写入数据。
  ```go {wrap}
  package main
  
  import (
      "database/sql"
      "fmt"
      "log"
  
      _ "github.com/taosdata/driver-go/v3/taosRestful"
  )
  
  func main() {
      var taosDSN = "root:taosdata@http(localhost:6041)/"
      db, err := sql.Open("taosRestful", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to " + taosDSN + " successfully.")
      defer db.Close()
      res, err := db.Exec("CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      rowsAffected, err := res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create database power successfully, rowsAffected: ", rowsAffected)
      res, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
      if err != nil {
         log.Fatalln("Failed to create stable meters, ErrMessage: " + err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create stable rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create stable power.meters successfully, rowsAffected:", rowsAffected)
      insertQuery := "INSERT INTO " +
         "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
         "VALUES " +
         "(NOW + 1a, 10.30000, 219, 0.31000) " +
         "(NOW + 2a, 12.60000, 218, 0.33000) " +
         "(NOW + 3a, 12.30000, 221, 0.31000) " +
         "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
         "VALUES " +
         "(NOW + 1a, 10.30000, 218, 0.25000) "
      res, err = db.Exec(insertQuery)
      if err != nil {
         log.Fatalf("Failed to insert data to power.meters, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
      }
      rowsAffected, err = res.RowsAffected()
      if err != nil {
         log.Fatalf("Failed to get insert rowsAffected, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
      }
      fmt.Printf("Successfully inserted %d rows to power.meters.\n", rowsAffected)
  }
  ```

##### 4.2.3.4 查询

1. 常用接口描述
  - `func (db *DB) Query(query string, args ...any) (*Rows, error)`
    - **接口说明**：执行查询并返回行的结果。
    - **参数说明**：
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：Rows 对象，错误信息。
  - `func (rs *Rows) Next() bool`
    - **接口说明**：准备下一行数据。
    - **返回值**：是否有下一行数据。
  - `func (rs *Rows) Columns() ([]string, error)`
    - **接口说明**：返回列名。
    - **返回值**：列名，错误信息。
  - `func (rs *Rows) Scan(dest ...any) error`
    - **接口说明**：将当前行的列值复制到 dest 指向的值中。
    - **参数说明**：
      - `dest`：目标值。
    - **返回值**：错误信息。
  - `func (rs *Rows) Close() error`
    - **接口说明**：关闭行（如果使用 Next 获取完全部数据则不需要调用 Close）。
    - **返回值**：错误信息。
  - `func (r *Row) Scan(dest ...any) error`
    - **接口说明**：将当前行的列值复制到 dest 指向的值中。
    - **参数说明**：
      - `dest`：目标值。
    - **返回值**：错误信息。
  - `func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error)`
    - **接口说明**：执行查询并返回行结果。
    - **参数说明**：
      - `ctx`：上下文，暂不支持 req_id。
      - `query`：要执行的命令。
      - `args`：命令参数。
    - **返回值**：结果集 Rows 对象，错误信息。
1. 查询样例
   - 执行 SQL `SELECT ts, current, location FROM power.meters limit 100`。
   - 获取结果
  ```go {wrap}
  package main
  
  import (
      "database/sql"
      "fmt"
      "log"
      "time"
  
      _ "github.com/taosdata/driver-go/v3/taosRestful"
  )
  
  func main() {
      var taosDSN = "root:taosdata@http(localhost:6041)/"
      db, err := sql.Open("taosRestful", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to " + taosDSN + " successfully.")
      defer db.Close()
      querySql := "SELECT ts, current, location FROM power.meters limit 100"
      rows, err := db.Query(querySql)
      if err != nil {
         log.Fatalf("Failed to query data from power.meters, sql: %s, ErrMessage: %s\n", querySql, err.Error())
      }
      columns, err := rows.Columns()
      if err != nil {
         log.Fatalf("Failed to get columns, sql: %s, ErrMessage: %s\n", querySql, err.Error())
      }
      fmt.Println("Columns: ", columns)
      for rows.Next() {
         // Add your data processing logic here
         var (
            ts       time.Time
            current  float32
            location string
         )
         err = rows.Scan(&ts, &current, &location)
         if err != nil {
            log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", querySql, err)
         }
         fmt.Printf("ts: %s, current: %f, location: %s\n", ts, current, location)
      }
  }
  ```

### 4.3 原生连接高级功能

`database/sql` 驱动为开发者提供了至关重要的便捷工具，使数据写入和查询操作变得简单而直观，极大地促进了与数据库的交互效率。然而，TDengine 数据库在此基础上更进一步，不仅兼容 `database/sql` 驱动，更在 `af` 包中封装了丰富的原生高级功能。

#### 4.3.1 创建连接

1. 接口描述
  - `func Open(host, user, pass, db string, port int) (*Connector, error)`
    - **接口说明**：连接数据库。
    - **参数说明**：
      - `host`：主机地址。
      - `user`：用户名，默认 `root`。
      - `pass`：密码，默认 `taosdata`。
      - `db`：数据库名称。
      - `port`：端口号。
    - **返回值**：连接对象，错误信息。
1. 实例
  ```go {wrap}
  package main
  
  import (
      "fmt"
      "github.com/taosdata/driver-go/v3/af"
      "log"
  )
  
  func main() {
      conn, err := af.Open("", "root", "taosdata", "", 0)
      if err != nil {
         log.Panicf("Open failed: %s", err)
      }
      defer conn.Close()
      fmt.Printf("Open success\n")
  }
  ```

#### 4.3.2 写入

1. 接口描述
  - `func (conn *Connector) Exec(query string, args ...driver.Value) (driver.Result, error)`
    - 接口说明：执行数据库写入操作。
    - 参数说明：
      - `query`：要执行的SQL查询语句。
      - `args`：查询参数，可变参数列表，类型为`driver.Value`。
    - 返回值：
      - `driver.Result`：执行结果对象。
      - `error`：如果执行过程中发生错误，则返回相应的错误信息。
  - `func (conn *Connector) ExecWithReqID(query string, reqID int64, args ...driver.Value) (driver.Result, error)`
    - **接口说明**：执行带有请求ID的数据库写入操作。
    - **参数说明**：
      - `query`：要执行的SQL查询语句。
      - `reqID`：请求的唯一标识符，类型为`int64`。
      - `args`：查询参数，可变参数列表，类型为`driver.Value`。
    - **返回值**：
      - `driver.Result`：执行结果对象。
      - `error`：如果执行过程中发生错误，则返回相应的错误信息。
1. 写入样例
   - 创建数据库 power。
   - 创建表 meters。 
   - 使用自动建表写入数据。
  ```go {wrap}
  package main
  
  import (
      "fmt"
      "github.com/taosdata/driver-go/v3/af"
      "log"
  
      "github.com/taosdata/driver-go/v3/common"
  )
  
  func main() {
      db, err := af.Open("localhost", "root", "taosdata", "", 6030)
      if err != nil {
         log.Fatalln("Failed to connect to localhost; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to localhost successfully.")
      reqId := common.GetReqID()
      log.Printf("Request ID:0x%x", reqId)
      res, err := db.ExecWithReqID("CREATE DATABASE IF NOT EXISTS power", reqId)
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      rowsAffected, err := res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create database power successfully, rowsAffected: ", rowsAffected)
  }
  ```

1. 带 req_id 的写入样例
   - 使用 common.GetReqID() 生成 req_id。
   - 使用 `db.ExecContext` 传入 req_id 并执行 sql。
  ```go {wrap}
  package main
  
  import (
      "context"
      "database/sql"
      "fmt"
      "log"
  
      "github.com/taosdata/driver-go/v3/common"
      _ "github.com/taosdata/driver-go/v3/taosSql"
  )
  
  func main() {
      var taosDSN = "root:taosdata@tcp(localhost:6030)/"
      db, err := sql.Open("taosSql", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to " + taosDSN + " successfully.")
      defer db.Close()
      reqId := common.GetReqID()
      log.Println("Request ID: ", reqId)
      ctx := context.WithValue(context.Background(), "taos_req_id", reqId)
      res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      rowsAffected, err := res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
      }
      fmt.Println("Create database power successfully, rowsAffected: ", rowsAffected)
  }
  ```

#### 4.3.3 查询

1. 接口描述
  - `func (conn *Connector) Query(query string, args ...driver.Value) (driver.Rows, error)`
    - **接口说明**：执行数据库查询操作，并返回查询结果集。
    - **参数说明**：
      - `query`：要执行的SQL查询语句。
      - `args`：查询参数，可变参数列表，类型为`driver.Value`。
    - **返回值**：
      - `driver.Rows`：查询结果集对象，用于逐行遍历查询结果。
      - `error`：如果执行过程中发生错误，则返回相应的错误信息。
  - `func (conn *Connector) QueryWithReqID(query string, reqID int64, args ...driver.Value) (driver.Rows, error)`
    - **接口说明**：执行带有请求ID的数据库查询操作，并返回查询结果集。
    - **参数说明**：
      - `query`：要执行的SQL查询语句。
      - `reqID`：请求的唯一标识符，类型为`int64`。这个标识符可以用于跟踪或日志记录等目的。
      - `args`：查询参数，可变参数列表，类型为`driver.Value`。
    - **返回值**：
      - `driver.Rows`：查询结果集对象，用于逐行遍历查询结果。
      - `error`：如果执行过程中发生错误，则返回相应的错误信息。
1. 获取结果接口
  - `func (rs *rows) Columns() []string`
    - **接口说明**：获取结果集中每一列的列名。
    - **参数说明**：无。
    - **返回值**：
      - `[]string`：一个字符串切片，包含结果集中每一列的列名。
  - `func (rs *rows) ColumnTypeDatabaseTypeName(i int) string`
    - **接口说明**：获取结果集中指定列的数据库特定类型名称。
    - **参数说明**：
      - `i`：列的索引，从0开始。表示要获取其数据库特定类型名称的列的索引位置。
    - **返回值**：
      - `string`：指定列的数据库特定类型名称。
  - `func (rs *rows) ColumnTypeScanType(i int) reflect.Type`
    - **接口说明**：获取结果集中指定列在扫描时应使用的Go类型。
    - **参数说明**：
      - `i`：列的索引，从0开始。表示要获取其扫描类型的列的索引位置。
    - **返回值**：
      - `reflect.Type`：指定列在扫描到Go变量时应使用的类型。
  - `func (rs *rows) Next(dest []driver.Value) error`
    - **接口说明**：前进到结果集的下一行，并将数据填充到提供的切片中。
    - **参数说明**：
      - `dest`：一个 `driver.Value` 类型的切片，用于接收当前行的数据。该切片的长度应至少与结果集中的列数相匹配。
    - **返回值**：
      - `error`：如果发生错误（例如，尝试读取超出结果集末尾的行），则返回相应的错误信息；如果成功读取到下一行数据，则返回 `nil`，如果读取完成返回 io.EOF。
  - `func (rs *rows) Close() error`
    - **接口说明**：关闭结果集，释放相关资源。
    - **参数说明**：无。
    - **返回值**：
      - `error`：目前只返回 `nil`。
1. 查询样例
   - 执行 SQL `SELECT ts, current, location FROM power.meters limit 100`。
   - 获取结果。
  ```go {wrap}
  package main
  
  import (
      "database/sql/driver"
      "fmt"
      "github.com/taosdata/driver-go/v3/af"
      "io"
      "log"
  )
  
  func main() {
      db, err := af.Open("localhost", "root", "taosdata", "", 6030)
      if err != nil {
         log.Fatalln("Failed to connect to localhost; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to localhost successfully.")
      defer db.Close()
      querySql := "SELECT ts, current, location FROM power.meters limit 100"
      rows, err := db.Query(querySql)
      if err != nil {
         log.Fatalf("Failed to query data from power.meters, sql: %s, ErrMessage: %s\n", querySql, err.Error())
      }
      columns := rows.Columns()
      fmt.Println("Columns: ", columns)
      values := make([]driver.Value, len(columns))
      for {
         err = rows.Next(values)
         if err != nil {
            if err == io.EOF {
               break
            }
            rows.Close()
            log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", querySql, err)
         }
         fmt.Printf("ts: %s, current: %f, location: %s\n", values[0], values[1], values[2])
      }
  }
  ```

1. 带 req_id 的查询样例
   - 使用 common.GetReqID() 生成 req_id。
   - 使用 `db.QueryWithReqID`传入 req_id 并执行 sql `SELECT ts, current, location FROM power.meters limit 100`。
   - 获取结果。
  ```go {wrap}
  package main
  
  import (
      "database/sql/driver"
      "fmt"
      "github.com/taosdata/driver-go/v3/af"
      "github.com/taosdata/driver-go/v3/common"
      "io"
      "log"
  )
  
  func main() {
      db, err := af.Open("localhost", "root", "taosdata", "", 6030)
      if err != nil {
         log.Fatalln("Failed to connect to localhost; ErrMessage: " + err.Error())
      }
      fmt.Println("Connected to localhost successfully.")
      defer db.Close()
      querySql := "SELECT ts, current, location FROM power.meters limit 100"
      redId := common.GetReqID()
      rows, err := db.QueryWithReqID(querySql, redId)
      if err != nil {
         log.Fatalf("Failed to query data from power.meters, sql: %s, ErrMessage: %s\n", querySql, err.Error())
      }
      columns := rows.Columns()
      fmt.Println("Columns: ", columns)
      values := make([]driver.Value, len(columns))
      for {
         err = rows.Next(values)
         if err != nil {
            if err == io.EOF {
               break
            }
            rows.Close()
            log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", querySql, err)
         }
         fmt.Printf("ts: %s, current: %f, location: %s\n", values[0], values[1], values[2])
      }
  }
  ```

#### 4.3.4 schemaless 写入 

1. 接口描述
  - `func (conn *Connector) InfluxDBInsertLines(lines []string, precision string) error`
    - **接口说明**：该方法用于通过 InfluxDB 行协议向数据库中批量插入数据点，数据以文本行的形式提供。
    - **参数说明**：
      - `lines`：一个字符串切片，其中每个元素都代表了一条 InfluxDB 行协议的数据。这些行应符合InfluxDB的LINE协议格式。
      - `precision`：一个字符串，用于指定时间戳的精度。InfluxDB支持的时间精度包括纳秒（ns）、微秒（u）、毫秒（ms）、秒（s）、分钟（m）和小时（h）。
    - **返回值**：
      - `error`：数据插入过程中发生任何错误
  - `func (conn *Connector) OpenTSDBInsertTelnetLines(lines []string) error`
    - **接口说明**：该方法用于通过 OpenTSDB telnet 协议向数据库中批量插入数据点，数据以文本行的形式提供。
    - **参数说明**：
      - `lines`：一个字符串切片，其中每个元素都代表了一条 OpenTSDB telnet 协议数据行。
    - **返回值**：
      - `error`：数据插入过程中发生任何错误
  - `func (conn *Connector) OpenTSDBInsertJsonPayload(payload string) error`
    - **接口说明**：该方法用于通过 OpenTSDB JSON 协议向数据库中插入数据，数据以 JSON 格式的字符串提供。
    - **参数说明**：
      - `payload`：一个字符串，OpenTSDB JSON格式数据。
    - **返回值**：
      - `error`：数据插入过程中发生任何错误。
1. 样例
  ```go {wrap}
  package main
  
  import (
      "fmt"
      "log"
  
      "github.com/taosdata/driver-go/v3/af"
  )
  
  func main() {
      host := "127.0.0.1"
      lineDemo := "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639"
      telnetDemo := "metric_telnet 1707095283260 4 host=host0 interface=eth0"
      jsonDemo := "{\"metric\": \"metric_json\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}"
  
      conn, err := af.Open(host, "root", "taosdata", "", 0)
      if err != nil {
         log.Fatalln("Failed to connect to host: " + host + "; ErrMessage: " + err.Error())
      }
      defer conn.Close()
      _, err = conn.Exec("CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      _, err = conn.Exec("USE power")
      if err != nil {
         log.Fatalln("Failed to use database power, ErrMessage: " + err.Error())
      }
      // insert influxdb line protocol
      err = conn.InfluxDBInsertLines([]string{lineDemo}, "ms")
      if err != nil {
         log.Fatalln("Failed to insert data with schemaless, data:" + lineDemo + ", ErrMessage: " + err.Error())
      }
      // insert opentsdb telnet protocol
      err = conn.OpenTSDBInsertTelnetLines([]string{telnetDemo})
      if err != nil {
         log.Fatalln("Failed to insert data with schemaless, data:" + telnetDemo + ", ErrMessage: " + err.Error())
      }
      // insert opentsdb json protocol
      err = conn.OpenTSDBInsertJsonPayload(jsonDemo)
      if err != nil {
         log.Fatalln("Failed to insert data with schemaless, data:" + jsonDemo + ", ErrMessage: " + err.Error())
      }
      fmt.Println("Inserted data with schemaless successfully.")
  }
  ```

#### 4.3.5 数据绑定

1. 支持查询和写入的 stmt，绑定数据仅支持单行绑定
  - `func (conn *Connector) Stmt() *Stmt`
    - **接口说明**：该方法用于从`Connector`实例中获取一个`Stmt`对象。
    - **参数说明**：无。
    - **返回值**：
      - `*Stmt`：返回一个指向`Stmt`类型的指针。
  - `func (s *Stmt) Prepare(sql string) error`
    - **接口说明**：该方法用于准备一个SQL语句，以便后续执行。
    - **参数说明**：
      - `sql`：一个字符串，包含了要准备的SQL语句。
    - **返回值**：
      - `error`：如果准备过程中发生错误则返回相应的错误信息。如果SQL语句成功准备，则返回 `nil`。
  - `func (s *Stmt) NumParams() (int, error)`
    - **接口说明**：该方法用于获取预处理语句（`Stmt`）中参数的数量。
    - **参数说明**：无。
    - **返回值**：
      - `int`：返回预处理语句中参数的数量。
      - `error`：如果发生错误，则返回相应的错误信息。如果成功获取到参数数量，则返回 `nil`。
  - `func (s *Stmt) SetTableNameWithTags(tableName string, tags *param.Param) error`
    - **接口说明**：该方法用于设置预处理语句（`Stmt`）中的表名以及与之相关的标签（tags）。
    - **参数说明**：
      - `tableName`：一个字符串，指定了要设置的表名。
      - `tags`：一个指向`param.Param`类型的指针，包含了与表名相关联的标签信息。
    - **返回值**：
      - `error`：如果设置过程中发生错误，则返回相应的错误信息。如果表名和标签成功设置，则返回`nil`。
  - `func (s *Stmt) SetTableName(tableName string) error`
    - **接口说明**：该方法用于设置预处理语句（`Stmt`）中的表名。
    - **参数说明**：
      - `tableName`：一个字符串，指定了要设置的表名。
    - **返回值**：
      - `error`：如果设置过程中发生错误，则返回相应的错误信息。如果表名成功设置，则返回`nil`。
  - `func (s *Stmt) BindRow(row *param.Param) error`
    - **接口说明**：该方法用于将一行数据（`row`）绑定到预处理语句（`Stmt`）上。
    - **参数说明**：
      - `row`：一个指向`param.Param`类型的指针，包含了要绑定到预处理语句上的一行数据。
    - **返回值**：
      - `error`：如果绑定过程中发生错误（，则返回相应的错误信息。如果数据成功绑定，则返回`nil`。
  - `func (s *Stmt) AddBatch() error`
    - **接口说明**：该方法用于将当前的预处理语句（`Stmt`）添加到批处理中。
    - **返回值**：
      - `error`：如果添加过程中发生错误则返回相应的错误信息。如果预处理语句成功添加到批处理中，则返回`nil`。
  - `func (s *Stmt) Execute() error`
    - **接口说明**：该方法用于执行预处理语句（`Stmt`）。
    - **返回值**：
      - `error`：如果执行过程中发生错误，则返回相应的错误信息。如果预处理语句成功执行，则返回`nil`。
  - `func (s *Stmt) GetAffectedRows() int`
    - **接口说明**：该方法用于获取上一次执行预处理语句（`Stmt`）所影响的行数。
    - **返回值**：
      - `int`：返回上一次执行预处理语句所影响的行数。
  - `func (s *Stmt) UseResult() (driver.Rows, error)`
    - **接口说明**：该方法用于获取并处理预处理语句（`Stmt`）的查询结果。
    - **返回值**：
      - `driver.Rows`：返回一个实现了`driver.Rows`接口的对象，该对象可用于迭代查询结果集。
      - `error`：如果获取或处理查询结果过程中发生错误则返回相应的错误信息。如果查询结果成功获取，则返回`nil`。
  - `func (s *Stmt) Close() error`
    - **接口说明**：该方法用于关闭预处理语句（`Stmt`）并释放与之相关的资源。
    - **返回值**：
      - `error`：如果关闭过程中发生错误则返回相应的错误信息。如果预处理语句成功关闭且资源正确释放，则返回`nil`。
1. 仅支持写入的 stmt，绑定数据支持多行绑定
  - `func (conn *Connector) InsertStmt() *insertstmt.InsertStmt`
    - **接口说明**：该方法用于创建一个新的插入语句对象（`InsertStmt`），该对象可用于构建和执行插入（INSERT）操作。
    - **返回值**：
      - `*insertstmt.InsertStmt`：返回一个指向`insertstmt.InsertStmt`类型的指针，该对象提供了构建和执行插入操作的方法和属性。
  - `func (conn *Connector) InsertStmtWithReqID(reqID int64) *insertstmt.InsertStmt`
    - **接口说明**：该方法用于创建一个新的插入语句对象（`InsertStmt`），并附加一个请求ID（`reqID`）以便于跟踪。
    - **参数说明**：
      - `reqID`：一个`int64`类型的值，表示请求的唯一标识符，用于跟踪。
    - **返回值**：
      - `*insertstmt.InsertStmt`：返回一个指向`insertstmt.InsertStmt`类型的指针，该对象提供了构建和执行插入操作的方法和属性。
  - `func (s *InsertStmt) Prepare(sql string) error`
    - **接口说明**：该方法用于准备一个SQL语句，以便后续执行。
    - **参数说明**：
      - `sql`：一个字符串，包含了要准备的SQL语句。
    - **返回值**：
      - `error`：如果准备过程中发生错误则返回相应的错误信息。如果SQL语句成功准备，则返回 `nil`。
  - `func (stmt *InsertStmt) SetTableName(name string) error`
    - **接口说明**：该方法用于设置插入语句的表名。
    - **参数说明**：
      - `name`：一个字符串，表示要插入数据的数据库表的名称。
    - **返回值**：
      - `error`：如果设置过程中发生错误，则返回相应的错误信息。如果表名成功设置，则返回`nil`。
  - `func (stmt *InsertStmt) SetSubTableName(name string) error`
    - **接口说明**：该方法用于设置插入语句的表名,与`SetTableName` 功能相同。
  - `func (stmt *InsertStmt) SetTableNameWithTags(tableName string, tags *param.Param) error`
    - **接口说明**：该方法用于设置预处理语句（`InsertStmt`）中的表名以及与之相关的标签（tags）。
    - **参数说明**：
      - `tableName`：一个字符串，指定了要设置的表名。
      - `tags`：一个指向`param.Param`类型的指针，包含了与表名相关联的标签信息。
    - **返回值**：
      - `error`：如果设置过程中发生错误则返回相应的错误信息。如果表名和标签成功设置，则返回`nil`。
  - `func (stmt *InsertStmt) BindParam(params []*param.Param, bindType *param.ColumnType) error`
    - **接口说明**：该方法用于将多行参数绑定到插入语句中，并指定这些参数的数据类型。
    - **参数说明**：
      - `params`：一个`*param.Param`类型的切片，包含了要绑定到插入语句中的参数集合。每个`Param`对象为一行数据。
      - `bindType`：一个指向`param.ColumnType`类型的指针，用于指定`params`中参数的数据类型。
    - **返回值**：
      - `error`：如果绑定过程中发生错误则返回相应的错误信息。如果参数成功绑定，则返回`nil`。
  - `func (stmt *InsertStmt) AddBatch() error`
    - **接口说明**：该方法用于将当前的插入语句添加到一个批处理操作中。
    - **返回值**：
      - `error`：如果添加过程中发生错误则返回相应的错误信息。如果插入语句成功添加到批处理中，则返回`nil`。
  - `func (stmt *InsertStmt) Execute() error`
    - **接口说明**：该方法用于执行预处理语句 。
    - **返回值**：
      - `error`：如果执行过程中发生错误则返回相应的错误信息。如果插入语句成功执行，则返回`nil`。
  - `func (stmt *InsertStmt) GetAffectedRows() int`
    - **接口说明**：该方法用于获取上一次执行预处理语句所影响的行数。
    - **返回值**：
      - `int`：返回上一次执行预处理语句所影响的行数。
  - `func (stmt *InsertStmt) Close() error`
    **接口说明**：该方法用于关闭预处理语句并释放与之相关的资源。
    **返回值**：
    `error`：如果关闭过程中发生错误则返回相应的错误信息。如果预处理语句成功关闭且资源正确释放，则返回`nil`。
1. 直接执行 stmt 单行写入
  - `func (conn *Connector) StmtExecute(sql string, params *param.Param) (res driver.Result, err error)`
    - **接口说明**：该方法用于在指定的数据库连接（`Connector`）上执行预处理语句（由`sql`字符串和`params`参数构成）。
    - **参数说明**：
      - `sql`：一个字符串，包含了要执行的SQL语句。这个SQL语句是预处理语句的形式。
      - `params`：一个指向`param.Param`类型的指针，包含了要绑定到SQL语句中的单行参数值。
    - **返回值**：
      - `res`：返回一个实现了`driver.Result`接口的对象，该对象可用于获取执行结果（影响的行数）。
      - `err`：如果执行过程中发生错误，则返回相应的错误信息。如果SQL语句成功执行，则返回`nil`。
  - `func (conn *Connector) StmtExecuteWithReqID(sql string, params *param.Param, reqID int64) (res driver.Result, err error)`
    - **接口说明**：该方法用于在指定的数据库连接（`Connector`）上执行预处理语句，并附加一个请求ID（`reqID`）以便于跟踪。
    - **参数说明**：
      - `sql`：一个字符串，包含了要执行的SQL语句。这个SQL语句是预处理语句的形式，。
      - `params`：一个指向`param.Param`类型的指针，包含了要绑定到SQL语句中的参数值。
      - `reqID`：一个`int64`类型的值，表示请求的唯一标识符，用于跟踪。
    - **返回值**：
      - `res`：返回一个实现了`driver.Result`接口的对象，该对象可用于获取执行结果（影响的行数）。
      - `err`：如果执行过程中发生错误则返回相应的错误信息。如果SQL语句成功执行，则返回`nil`。
1. 单行绑定查询与写入样例
   - 创建数据库 power 超级表 meters。
   - 创建 stmt 写入实例，准备自动建表语句。
   - 对 10 张子表循环绑定数据添加批量，执行写入，获取受影响行数。
   - 创建 stmt 查询实例，准备查询语句。
   - 绑定查询条件。
   - 添加批量，执行查询，获取结果。
   - 遍历结果打印查询数据。
  ```go {wrap}
  package main
  
  import (
      "database/sql/driver"
      "fmt"
      "io"
      "log"
      "math/rand"
      "time"
  
      "github.com/taosdata/driver-go/v3/af"
      "github.com/taosdata/driver-go/v3/common"
      "github.com/taosdata/driver-go/v3/common/param"
  )
  
  func main() {
      host := "127.0.0.1"
      numOfSubTable := 10
      numOfRow := 10
      db, err := af.Open(host, "root", "taosdata", "", 0)
      if err != nil {
         log.Fatalln("Failed to connect to " + host + "; ErrMessage: " + err.Error())
      }
      defer db.Close()
      // prepare database and table
      _, err = db.Exec("CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      _, err = db.Exec("USE power")
      if err != nil {
         log.Fatalln("Failed to use database power, ErrMessage: " + err.Error())
      }
      _, err = db.Exec("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
      if err != nil {
         log.Fatalln("Failed to create stable meters, ErrMessage: " + err.Error())
      }
      // prepare statement
      sql := "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)"
      stmt := db.Stmt()
      err = stmt.Prepare(sql)
      if err != nil {
         log.Fatalln("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
      }
      for i := 1; i <= numOfSubTable; i++ {
         tableName := fmt.Sprintf("d_bind_%d", i)
         tags := param.NewParam(2).AddInt(i).AddBinary([]byte(fmt.Sprintf("location_%d", i)))
         // set tableName and tags
         err = stmt.SetTableNameWithTags(tableName, tags)
         if err != nil {
            log.Fatalln("Failed to set table name and tags, tableName: " + tableName + "; ErrMessage: " + err.Error())
         }
         // bind column data
         current := time.Now()
         for j := 0; j < numOfRow; j++ {
            row := param.NewParam(4).
               AddTimestamp(current.Add(time.Millisecond*time.Duration(j)), common.PrecisionMilliSecond).
               AddFloat(rand.Float32() * 30).
               AddInt(rand.Intn(300)).
               AddFloat(rand.Float32())
            err = stmt.BindRow(row)
            if err != nil {
               log.Fatalln("Failed to bind params, ErrMessage: " + err.Error())
            }
         }
         // add batch
         err = stmt.AddBatch()
         if err != nil {
            log.Fatalln("Failed to add batch, ErrMessage: " + err.Error())
         }
         // execute batch
         err = stmt.Execute()
         if err != nil {
            log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
         }
         // get affected rows
         affected := stmt.GetAffectedRows()
         // you can check exeResult here
         fmt.Printf("Successfully inserted %d rows to %s.\n", affected, tableName)
      }
      err = stmt.Close()
      if err != nil {
         log.Fatal("failed to close statement, err:", err)
      }
  
      // prepare query statement
      sql = "SELECT * FROM d_bind_1 WHERE ts < ?"
      stmt = db.Stmt()
      err = stmt.Prepare(sql)
      if err != nil {
         log.Fatalln("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
      }
      err = stmt.BindRow(param.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMilliSecond))
      if err != nil {
         log.Fatalln("Failed to bind params, ErrMessage: " + err.Error())
      }
      err = stmt.AddBatch()
      if err != nil {
         log.Fatalln("Failed to add batch, ErrMessage: " + err.Error())
      }
      err = stmt.Execute()
      if err != nil {
         log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
      }
      rows, err := stmt.UseResult()
      if err != nil {
         log.Fatalln("Failed to use result, ErrMessage: " + err.Error())
      }
      fmt.Println("Columns:", rows.Columns())
      values := make([]driver.Value, len(rows.Columns()))
      for {
         err = rows.Next(values)
         if err != nil {
            if err == io.EOF {
               break
            }
            rows.Close()
            log.Fatalln("Failed to get next row, ErrMessage: " + err.Error())
         }
         for i := 0; i < len(rows.Columns()); i++ {
            fmt.Printf("%s:%v ", rows.Columns()[i], values[i])
         }
         fmt.Println()
      }
      err = stmt.Close()
      if err != nil {
         log.Fatal("failed to close statement, err:", err)
      }
  }
  ```

1. 多行绑定写入样例 
  ```go {wrap}
  package main
  
  import (
      "fmt"
      "log"
      "math/rand"
      "time"
  
      "github.com/taosdata/driver-go/v3/af"
      "github.com/taosdata/driver-go/v3/common"
      "github.com/taosdata/driver-go/v3/common/param"
  )
  
  func main() {
      host := "127.0.0.1"
      numOfSubTable := 10
      numOfRow := 10
      db, err := af.Open(host, "root", "taosdata", "", 0)
      if err != nil {
         log.Fatalln("Failed to connect to " + host + "; ErrMessage: " + err.Error())
      }
      defer db.Close()
      // prepare database and table
      _, err = db.Exec("CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      _, err = db.Exec("USE power")
      if err != nil {
         log.Fatalln("Failed to use database power, ErrMessage: " + err.Error())
      }
      _, err = db.Exec("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
      if err != nil {
         log.Fatalln("Failed to create stable meters, ErrMessage: " + err.Error())
      }
      // prepare statement
      sql := "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)"
      stmt := db.InsertStmt()
      err = stmt.Prepare(sql)
      if err != nil {
         log.Fatalln("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
      }
      for i := 1; i <= numOfSubTable; i++ {
         tableName := fmt.Sprintf("d_bind_%d", i)
         tags := param.NewParam(2).AddInt(i).AddBinary([]byte(fmt.Sprintf("location_%d", i)))
         // set tableName and tags
         err = stmt.SetTableNameWithTags(tableName, tags)
         if err != nil {
            log.Fatalln("Failed to set table name and tags, tableName: " + tableName + "; ErrMessage: " + err.Error())
         }
         // bind column data
         bindType := param.NewColumnType(4).AddTimestamp().AddFloat().AddInt().AddFloat()
         cols := make([]*param.Param, 4)
         current := time.Now()
         cols[0] = param.NewParam(numOfRow)
         cols[1] = param.NewParam(numOfRow)
         cols[2] = param.NewParam(numOfRow)
         cols[3] = param.NewParam(numOfRow)
         for j := 0; j < numOfRow; j++ {
            cols[0].AddTimestamp(current.Add(time.Millisecond*time.Duration(j)), common.PrecisionMilliSecond)
            cols[1].AddFloat(rand.Float32() * 30)
            cols[2].AddInt(rand.Intn(300))
            cols[3].AddFloat(rand.Float32())
         }
         err = stmt.BindParam(cols, bindType)
         if err != nil {
            log.Fatalln("Failed to bind params, ErrMessage: " + err.Error())
         }
         // add batch
         err = stmt.AddBatch()
         if err != nil {
            log.Fatalln("Failed to add batch, ErrMessage: " + err.Error())
         }
         // execute batch
         err = stmt.Execute()
         if err != nil {
            log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
         }
         // get affected rows
         affected := stmt.GetAffectedRows()
         // you can check exeResult here
         fmt.Printf("Successfully inserted %d rows to %s.\n", affected, tableName)
      }
      err = stmt.Close()
      if err != nil {
         log.Fatal("failed to close statement, err:", err)
      }
  }
  ```

1. 直接执行单行写入样例
  ```go {wrap}
  package main
  
  import (
      "fmt"
      "log"
      "time"
  
      "github.com/taosdata/driver-go/v3/af"
      "github.com/taosdata/driver-go/v3/common"
      "github.com/taosdata/driver-go/v3/common/param"
  )
  
  func main() {
      host := "127.0.0.1"
      db, err := af.Open(host, "root", "taosdata", "", 0)
      if err != nil {
         log.Fatalln("Failed to connect to " + host + "; ErrMessage: " + err.Error())
      }
      defer db.Close()
      // prepare database and table
      _, err = db.Exec("CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
      }
      _, err = db.Exec("USE power")
      if err != nil {
         log.Fatalln("Failed to use database power, ErrMessage: " + err.Error())
      }
      _, err = db.Exec("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
      if err != nil {
         log.Fatalln("Failed to create stable meters, ErrMessage: " + err.Error())
      }
  
      sql := "INSERT INTO d_bind_1 USING meters TAGS(1,'location_1') VALUES (?,?,?,?)"
      now := time.Now()
      values := param.NewParam(4).AddTimestamp(now, common.PrecisionMilliSecond).AddFloat(10.0).AddInt(220).AddFloat(0.5)
      res, err := db.StmtExecute(sql, values)
      if err != nil {
         log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
      }
      affected, err := res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get affected rows, ErrMessage: " + err.Error())
      }
      fmt.Printf("Successfully inserted %d rows to d_bind_1.\n", affected)
      values = param.NewParam(4).AddTimestamp(now.Add(time.Millisecond), common.PrecisionMilliSecond).AddFloat(10.0).AddInt(220).AddFloat(0.5)
      reqID := common.GetReqID()
      res, err = db.StmtExecuteWithReqID(sql, values, reqID)
      if err != nil {
         log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
      }
      affected, err = res.RowsAffected()
      if err != nil {
         log.Fatalln("Failed to get affected rows, ErrMessage: " + err.Error())
      }
      fmt.Printf("Successfully inserted %d rows to d_bind_1.\n", affected)
  }
  ```

#### 4.3.6 STMT2 参数绑定

- `func (conn *Connector) Stmt2(reqID int64, singleTableBindOnce bool) *Stmt2`
  - **接口说明**：该方法用于从`Connector`实例中获取一个`Stmt`2对象。
  - **参数说明**：
    - `reqID`：请求 ID 用于上下文追踪。
    - `singleTableBindOnce`：单个子表只绑定一次。
  - **返回值**：
    - `*Stmt2`：返回一个指向`Stmt2`类型的指针。
- `func (s *Stmt2) SetTimezone(tz *time.Location)`
  - **接口说明**：设置当前 STMT2 查询结果解析使用的时区信息。
  - **参数说明**：
    - `tz`：时区。
- `func (s *Stmt2) Prepare(sql string) error`
  - **接口说明**：该方法用于准备一个SQL语句，以便后续执行。
  - **参数说明**：
    - `sql`：一个字符串，包含了要准备的SQL语句。
  - **返回值**：
    - `error`：如果准备过程中发生错误则返回相应的错误信息。如果SQL语句成功准备，则返回 `nil`。
- `func (s *Stmt2) Bind(params []*stmt.TaosStmt2BindData) error`
  - **接口说明**：该方法用于绑定数据。
  - **参数说明**：
    - `params`：绑定数据的数组，每个元素代表一个表的数据。每个表数据包含表名，以行形式组织的标签值，以列组织的列值。如果查询语句只需要 cols
    ```go
    type TaosStmt2BindData struct {
        TableName string
        Tags      []driver.Value   // row format
        Cols      [][]driver.Value // column format
    }
    ```

  - **返回值**：
    - `error`：绑定数据成功返回 nil 失败返回对应错误。
- `func (s *Stmt2) Execute() error`
  - **接口说明**：该方法用于执行查询或写入。
  - **返回值**：
    - `error`：执行成功返回 nil 失败返回对应错误。
- `func (s *Stmt2) GetAffectedRows() int`
  - **接口说明**：该方法用于获取影响行数。
  - **返回值**：
    - `int`：写入成功返回的行数。
- `func (s *Stmt2) UseResult() (driver.Rows, error)`
  - **接口说明**：该方法用于获取查询结果。
  - **返回值**：
    - `driver.Rows`：查询结果。
    - `error`：成功返回 nil，失败返回对应错误。
- `func (s *Stmt2) Close() error`
  - **接口说明**：该方法用于关闭 stmt2 实例。
  - **返回值**：
    - `error`：成功返回 nil，失败返回对应错误。

#### 4.3.7 订阅

1. 接口描述
  - `func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error)`
    - **功能**：创建一个新的 `Consumer` 实例，使用提供的 `tmq.ConfigMap` 配置。
    - **参数**：
      - `conf`：`*tmq.ConfigMap` 类型，指向包含消费者配置信息的映射。
    - **返回值**：
      - `*Consumer`：指向新创建的 `Consumer` 实例的指针。
      - `error`：如果创建过程中发生错误，则返回一个错误对象；否则返回 `nil`。
  - `func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error`
    - **功能**：订阅指定的主题。
    - **参数**：
      - `topic`：`string` 类型，要订阅的主题名称。
      - `rebalanceCb`：`RebalanceCb` 类型，无效果。
    - **返回值**：
      - `error`：如果订阅过程中发生错误，则返回一个错误对象；否则返回 `nil`。
  - `func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error`
    - **功能**：订阅多个主题。
    - **参数**：
      - `topics`：要订阅的主题名称列表。
      - `rebalanceCb`：无效参数。
    - **返回值**：返回一个 `error` 类型，如果订阅成功则返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Unsubscribe() error`
    - **功能**：取消订阅。
    - **返回值**：返回一个 `error` 类型，如果取消订阅成功则返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Poll(timeoutMs int) tmq.Event`
    - **功能**：轮询消息，等待消息的到达或超时。
    - **参数**：
      - `timeoutMs`：轮询的超时时间（毫秒）。
    - **返回值**：返回一个 `tmq.Event` 类型，包含接收到的消息或错误信息。
  - `func (c *Consumer) Commit() ([]tmq.TopicPartition, error)`
    - **功能**：提交当前已消费的偏移量。
    - **返回值**：返回一个 `[]tmq.TopicPartition` 和一个 `error` 类型，`[]tmq.TopicPartition` 包含已提交的分区信息，如果提交成功 `error` 返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Assignment() (partitions []tmq.TopicPartition, err error)`
    - **功能**：获取当前分配给消费者的分区信息。
    - **返回值**：返回一个 `[]tmq.TopicPartition` 和一个 `error` 类型，`[]tmq.TopicPartition` 包含分配的分区信息，如果获取成功 `error` 返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Seek(partition tmq.TopicPartition, ignoredTimeoutMs int) error`
    - **功能**：将指定分区的消费者偏移量设置到指定的位置。
    - **参数**：
      - `partition`：要设置的分区信息，类型为 `tmq.TopicPartition`。
      - `ignoredTimeoutMs`：此参数在当前实现中被忽略，保留用于接口一致性。
    - **返回值**：返回一个 `error` 类型，如果设置成功则返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Committed(partitions []tmq.TopicPartition, timeoutMs int) (offsets []tmq.TopicPartition, err error)`
    - **功能**：获取指定分区的已提交偏移量。
    - **参数**：
      - `partitions`：要查询的分区信息列表。
      - `timeoutMs`：查询的超时时间（毫秒）此参数在当前实现中被忽略，保留用于接口一致性。
    - **返回值**：返回一个 `[]tmq.TopicPartition` 和一个 `error` 类型，`[]tmq.TopicPartition` 包含已提交的偏移量信息，如果查询成功`error` 返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) CommitOffsets(offsets []tmq.TopicPartition) ([]tmq.TopicPartition, error)`
    - **功能**：提交指定的偏移量到服务器。
    - **参数**：
      - `offsets`：要提交的偏移量信息列表。
    - **返回值**：返回一个 `[]tmq.TopicPartition` 和一个 `error` 类型，`[]tmq.TopicPartition` 包含已提交的分区信息，如果提交成功`error` 返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Position(partitions []tmq.TopicPartition) (offsets []tmq.TopicPartition, err error)`
    - **功能**：获取指定分区的当前位置（即下一个要消费的偏移量）。
    - **参数**：
      - `partitions`：要查询的分区信息列表。
    - **返回值**：返回一个 `[]tmq.TopicPartition` 和一个 `error` 类型，`[]tmq.TopicPartition` 包含当前位置信息，如果查询成功`error` 返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Close() error`
    - **功能**：关闭消费者，释放相关资源。
    - **返回值**：返回一个 `error` 类型，如果关闭成功则返回 `nil`，否则返回错误信息。
1. 样例
   - 准备数据库、超级表。
   - 循环10次每次等待1秒之后向表 power.d1001 写入一条数据。
   - 创建消费者。
   - 订阅主题。
   - 获取数据超时 100 毫秒。
   - 提交偏移量。
   - 获取分配信息。
   - 将所有分区偏移量设置到 0。
   - 取消订阅。
   - 关闭消费者。
  ```go {wrap}
  package main
  
  import (
      "database/sql"
      "fmt"
      "log"
      "time"
  
      "github.com/taosdata/driver-go/v3/af/tmq"
      tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
      _ "github.com/taosdata/driver-go/v3/taosSql"
  )
  
  var done = make(chan struct{})
  var groupID string
  var clientID string
  var host string
  var topic string
  
  func main() {
      // init env
      taosDSN := "root:taosdata@tcp(127.0.0.1:6030)/"
      conn, err := sql.Open("taosSql", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + ", ErrMessage: " + err.Error())
      }
      defer func() {
         conn.Close()
      }()
      initEnv(conn)
      // create consumer
      groupID = "group1"
      clientID = "client1"
      host = "127.0.0.1"
      consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
         "td.connect.user":         "root",
         "td.connect.pass":         "taosdata",
         "auto.offset.reset":       "latest",
         "msg.with.table.name":     "true",
         "enable.auto.commit":      "true",
         "auto.commit.interval.ms": "1000",
         "group.id":                groupID,
         "client.id":               clientID,
      })
      if err != nil {
         log.Fatalf(
            "Failed to create native consumer, host: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
            host,
            groupID,
            clientID,
            err.Error(),
         )
      }
      log.Printf("Create consumer successfully, host: %s, groupId: %s, clientId: %s\n", host, groupID, clientID)
  
      topic = "topic_meters"
      err = consumer.Subscribe(topic, nil)
      if err != nil {
         log.Fatalf(
            "Failed to subscribe topic_meters, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
            topic,
            groupID,
            clientID,
            err.Error(),
         )
      }
      log.Println("Subscribe topics successfully")
      for i := 0; i < 50; i++ {
         ev := consumer.Poll(100)
         if ev != nil {
            switch e := ev.(type) {
            case *tmqcommon.DataMessage:
               // process your data here
               fmt.Printf("data:%v\n", e)
               // commit offset
               _, err = consumer.CommitOffsets([]tmqcommon.TopicPartition{e.TopicPartition})
               if err != nil {
                  log.Fatalf(
                     "Failed to commit offset, topic: %s, groupId: %s, clientId: %s, offset %s, ErrMessage: %s\n",
                     topic,
                     groupID,
                     clientID,
                     e.TopicPartition,
                     err.Error(),
                  )
               }
               log.Println("Commit offset manually successfully.")
            case tmqcommon.Error:
               log.Fatalf("Failed to poll data, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n", topic, groupID, clientID, e.Error())
            }
         }
      }
      // get assignment
      partitions, err := consumer.Assignment()
      if err != nil {
         log.Fatalf("Failed to get assignment, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n", topic, groupID, clientID, err.Error())
      }
      fmt.Println("Now assignment:", partitions)
      for i := 0; i < len(partitions); i++ {
         // seek to the beginning
         err = consumer.Seek(tmqcommon.TopicPartition{
            Topic:     partitions[i].Topic,
            Partition: partitions[i].Partition,
            Offset:    0,
         }, 0)
         if err != nil {
            log.Fatalf(
               "Failed to execute seek offset, topic: %s, groupId: %s, clientId: %s, partition: %d, offset: %d, ErrMessage: %s\n",
               topic,
               groupID,
               clientID,
               partitions[i].Partition,
               0,
               err.Error(),
            )
         }
      }
      fmt.Println("Assignment seek to beginning successfully")
      // unsubscribe
      err = consumer.Unsubscribe()
      if err != nil {
         log.Fatalf(
            "Failed to unsubscribe consumer, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
            topic,
            groupID,
            clientID,
            err.Error(),
         )
      }
      fmt.Println("Consumer unsubscribed successfully.")
      // close consumer
      err = consumer.Close()
      if err != nil {
         log.Fatalf(
            "Failed to close consumer, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
            topic,
            groupID,
            clientID,
            err.Error(),
         )
      }
      fmt.Println("Consumer closed successfully.")
      <-done
  }
  
  func initEnv(conn *sql.DB) {
      _, err := conn.Exec("CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatal("Failed to create database, ErrMessage: " + err.Error())
      }
      _, err = conn.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
      if err != nil {
         log.Fatal("Failed to create stable, ErrMessage: " + err.Error())
      }
      _, err = conn.Exec("CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM power.meters")
      if err != nil {
         log.Fatal("Failed to create topic, ErrMessage: " + err.Error())
      }
      go func() {
         for i := 0; i < 10; i++ {
            time.Sleep(time.Second)
            _, err = conn.Exec("INSERT INTO power.d1001 USING power.meters TAGS (2, 'California.SanFrancisco') VALUES (NOW , 10.2, 219, 0.32)")
            if err != nil {
               log.Fatal("Failed to insert data, ErrMessage: " + err.Error())
            }
         }
         done <- struct{}{}
      }()
  }
  ```

### 4.4 WebSocket高级功能

WebSocket 同样提供了 schemaless 写入，数据绑定和订阅功能。

#### 4.4.1 schemaless 写入 

包名 `github.com/taosdata/driver-go/v3/ws/schemaless`
1. 设置WebSocket连接
  - `func NewConfig(url string, chanLength uint, opts ...func(*Config)) *Config`
    - **功能**：创建一个新的 `Config` 实例，并应用一系列可选的配置项。
    - **参数**：
      - `url`：数据库连接 URL，类型为 `string`。
      - `chanLength`：请求管道长度，类型为 `uint`。
      - `opts`：可变参数列表，包含零个或多个配置函数，每个函数接受一个指向 `Config` 实例的指针。
    - **返回值**：返回一个新的 `Config` 实例的指针。
  - `func SetUser(user string) func(*Config)`
    - **功能**：返回一个配置函数，该函数用于设置 `Config` 实例中的用户。
    - **参数**：
      - `user`：要设置的用户名，类型为 `string`。
  - `func SetPassword(password string) func(*Config)`
    - **功能**：返回一个配置函数，该函数用于设置 `Config` 实例中的密码。
    - **参数**：
      - `password`：要设置的密码，类型为 `string`。
  - `func SetDb(db string) func(*Config)`
    - **功能**：返回一个配置函数，该函数用于设置 `Config` 实例中要连接的数据库名。
    - **参数**：
      - `db`：要设置的数据库名，类型为 `string`。
  - `func SetReadTimeout(readTimeout time.Duration) func(*Config)`
    - **功能**：返回一个配置函数，该函数用于设置 `Config` 实例中的读超时时间。
    - **参数**：
      - `readTimeout`：要设置的读超时时间，类型为 `time.Duration`。
  - `func SetWriteTimeout(writeTimeout time.Duration) func(*Config)`
    - **功能**：返回一个配置函数，该函数用于设置 `Config` 实例中的写超时时间。
    - **参数**：
      - `writeTimeout`：要设置的写超时时间，类型为 `time.Duration`。
  - `func SetErrorHandler(errorHandler func(error)) func(*Config)`
    - **功能**：返回一个配置函数，该函数用于设置 `Config` 实例中的错误处理函数。
    - **参数**：
      - `errorHandler`：要设置的错误处理函数，该函数接受一个 `error` 类型的参数。
  - `func SetEnableCompression(enableCompression bool) func(*Config)`
    **功能**：返回一个配置函数，该函数用于设置 `Config` 实例中是否启用压缩。
    **参数**：
    `enableCompression`：一个布尔值，指示是否启用压缩。
1. schemaless 写入
  - `func NewSchemaless(config *Config) (*Schemaless, error)`
    - **功能**：根据提供的配置创建一个新的 `Schemaless` 实例。
    - **参数**：
      - `config`：指向 `Config` 实例的指针，包含了创建 `Schemaless` 实例所需的配置信息。
    - **返回值**：
      - 成功时返回一个指向新创建的 `Schemaless` 实例的指针和一个 `nil` 错误。
      - 失败时返回一个 `nil` 的 `Schemaless` 实例指针和一个描述错误的 `error` 类型值。
  - `func (s *Schemaless) Insert(lines string, protocol int, precision string, ttl int, reqID int64) error`
    - **功能**：向 `Schemaless` 实例中插入数据。
    - **参数**：
      - `lines`：要插入的数据行，类型为 `string`。
      - `protocol`：指定使用的协议版本 influxdb 1，OpenTSDB telnet 协议 2，OpenTSDB JSON 协议 3
      - `precision`：时间戳的精度，类型为 `string`，纳秒（ns）、微秒（u）、毫秒（ms）、秒（s）、分钟（m）和小时（h）。
      - `ttl`：数据表存活时间（单位 天）。
      - `reqID`：请求的唯一标识符，类型为 `int64`。
    - **返回值**：如果插入成功，返回 `nil`；如果插入失败，返回一个描述错误的 `error` 类型值。
1. 样例
  ```go {wrap}
  package main
  
  import (
          "database/sql"
          "fmt"
          "log"
          "time"
  
          "github.com/taosdata/driver-go/v3/common"
          _ "github.com/taosdata/driver-go/v3/taosWS"
          "github.com/taosdata/driver-go/v3/ws/schemaless"
  )
  
  func main() {
          host := "127.0.0.1"
          lineDemo := "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639"
          telnetDemo := "metric_telnet 1707095283260 4 host=host0 interface=eth0"
          jsonDemo := "{\"metric\": \"metric_json\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}"
  
          taosDSN := fmt.Sprintf("root:taosdata@ws(%s:6041)/", host)
          db, err := sql.Open("taosWS", taosDSN)
          if err != nil {
                  log.Fatalln("Failed to connect to host: " + host + "; ErrMessage: " + err.Error())
          }
          defer db.Close()
          _, err = db.Exec("CREATE DATABASE IF NOT EXISTS power")
          if err != nil {
                  log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
          }
          s, err := schemaless.NewSchemaless(schemaless.NewConfig("ws://localhost:6041", 1,
                  schemaless.SetDb("power"),
                  schemaless.SetReadTimeout(10*time.Second),
                  schemaless.SetWriteTimeout(10*time.Second),
                  schemaless.SetUser("root"),
                  schemaless.SetPassword("taosdata"),
          ))
          if err != nil {
                  log.Fatalln("Failed to connect to host: " + host + "; ErrMessage: " + err.Error())
          }
          // insert influxdb line protocol
          err = s.Insert(lineDemo, schemaless.InfluxDBLineProtocol, "ms", 0, common.GetReqID())
          if err != nil {
                  log.Fatalln("Failed to insert data with schemaless, data:" + lineDemo + ", ErrMessage: " + err.Error())
          }
          // insert opentsdb telnet line protocol
          err = s.Insert(telnetDemo, schemaless.OpenTSDBTelnetLineProtocol, "ms", 0, common.GetReqID())
          if err != nil {
                  log.Fatalln("Failed to insert data with schemaless, data: " + telnetDemo + ", ErrMessage: " + err.Error())
          }
          // insert opentsdb json format protocol
          err = s.Insert(jsonDemo, schemaless.OpenTSDBJsonFormatProtocol, "s", 0, common.GetReqID())
          if err != nil {
                  log.Fatalln("Failed to insert data with schemaless, data: " + jsonDemo + ", ErrMessage: " + err.Error())
          }
          fmt.Println("Inserted data with schemaless successfully.")
  }
  
  ```

#### 4.4.2 数据绑定

包名 `github.com/taosdata/driver-go/v3/ws/stmt`
1. 设置WebSocket连接
  - `func (c *Config) SetConnectUser(user string) error`
    - **功能**：设置数据库连接的用户名。
    - **参数**：
      - `user`：要设置的用户名，类型为 `string`。
    - **返回值**：如果设置成功，返回 `nil`；如果设置失败（例如由于内部错误），返回一个描述错误的 `error` 类型值。
  - `func (c *Config) SetConnectPass(pass string) error`
    - **功能**：设置数据库连接的密码。
    - **参数**：
      - `pass`：要设置的密码，类型为 `string`。
    - **返回值**：如果设置成功，返回 `nil`；如果设置失败，返回一个描述错误的 `error` 类型值。
  - `func (c *Config) SetConnectDB(db string) error`
    - **功能**：设置要连接的数据库名。
    - **参数**：
      - `db`：要连接的数据库名，类型为 `string`。
    - **返回值**：如果设置成功，返回 `nil`；如果设置失败，返回一个描述错误的 `error` 类型值。
  - `func (c *Config) SetMessageTimeout(timeout time.Duration) error`
    - **功能**：设置消息超时时间。
    - **参数**：
      - `timeout`：要设置的超时时间，类型为 `time.Duration`。
    - **返回值**：如果设置成功，返回 `nil`；如果设置失败，返回一个描述错误的 `error` 类型值。
  - `func (c *Config) SetWriteWait(writeWait time.Duration) error`
    - **功能**：设置写等待时间。
    - **参数**：
      - `writeWait`：要设置的写等待时间，类型为 `time.Duration`。
    - **返回值**：如果设置成功，返回 `nil`；如果设置失败，返回一个描述错误的 `error` 类型值。
  - `func (c *Config) SetErrorHandler(f func(connector *Connector, err error))`
    - **功能**：设置错误处理函数。
    - **参数**：
      - `f`：错误处理函数，该函数接受一个指向 `Connector` 实例的指针和一个 `error` 类型的参数。
  - `func (c *Config) SetCloseHandler(f func())`
    - **功能**：设置关闭处理函数。
    - **参数**：
      - `f`：关闭处理函数，该函数不接受任何参数也不返回任何值。
  - `func (c *Config) SetEnableCompression(enableCompression bool)`
    - **功能**：设置是否启用压缩。
    - **参数**：
      - `enableCompression`：一个布尔值，指示是否启用压缩。
1. 连接
  - `func NewConnector(config *Config) (*Connector, error)`
    - **功能**：根据提供的配置创建一个新的 `Connector` 实例。
    - **参数**：
      - `config`：指向 `Config` 实例的指针，该配置实例包含了创建 `Connector` 所需的所有配置信息。
    - **返回值**：
      - 成功时，返回一个指向新创建的 `Connector` 实例的指针和一个 `nil` 错误值。
      - 失败时，返回一个 `nil` 的 `Connector` 指针和一个描述错误的 `error` 类型值。
1. 参数绑定
  - `func (c *Connector) Init() (*Stmt, error)`
    - **功能**：初始化 `Connector` 实例，并准备一条或多条语句。
    - **返回值**：
      - 成功时，返回一个指向初始化过程中准备的 `Stmt`（语句）实例的指针，以及一个 `nil` 错误值。`Stmt` 通常代表了一个预编译的或可执行的数据库语句。
      - 失败时，返回一个 `nil` 的 `Stmt` 指针和一个描述错误的 `error` 类型值。
  - `func (s *Stmt) Prepare(sql string) error`
    - **功能**：准备一条 SQL 语句，以便之后执行。
    - **参数**：
      - `sql`：要准备的 SQL 语句，类型为 `string`。
    - **返回值**：
      - 如果成功，返回 `nil` 错误值，如果准备失败返回错误。
  - `func (s *Stmt) SetTableName(name string) error`
    - **功能**：表名。
    - **参数**：
      - `name`：要设置的表名，类型为 `string`。
    - **返回值**：
      - 如果成功，返回 `nil` 错误值，如果准备失败返回错误。
  - `func (s *Stmt) SetTags(tags *param.Param, bindType *param.ColumnType) error`
    - **功能**：为 SQL 语句设置标签。
    - **参数**：
      - `tags`：指向 `param.Param` 实例的指针，包含标签信息。
      - `bindType`：指向 `param.ColumnType` 的指针，指定标签的绑定类型。
    - **返回值**：
      - 如果成功，返回 `nil` 错误值，如果准备失败返回错误。
  - `func (s *Stmt) BindParam(params []*param.Param, bindType *param.ColumnType) error`
    - **功能**：为 SQL 语句绑定参数。
    - **参数**：
      - `params`：指向 `param.Param` 实例的切片，包含要绑定的参数。
      - `bindType`：指向 `param.ColumnType` 的指针，指定参数的绑定类型。
    - **返回值**：
      - 如果成功，返回 `nil` 错误值，如果准备失败返回错误。
  - `func (s *Stmt) AddBatch() error`
    - **功能**：添加到批。
    - **返回值**：
      - 如果成功，返回 `nil` 错误值，如果准备失败返回错误。
  - `func (s *Stmt) Exec() error`
    - **功能**：执行。
    - **返回值**：
      - 如果成功，返回 `nil` 错误值，如果准备失败返回错误。
  - `func (s *Stmt) GetAffectedRows() int`
    - **功能**：获取最近一次执行 SQL 语句时受影响的行数。
    - **返回值**：
      - 返回一个整数，表示受影响的行数。
  - `func (s *Stmt) Close() error`
    - **功能**：关闭 `Stmt` 实例，释放与之相关的所有资源。
    - **返回值**：
      - 如果成功，返回 `nil` 错误值，如果准备失败返回错误。
1. 样例
  ```go {wrap}
  package main
  
  import (
          "database/sql"
          "fmt"
          "log"
          "math/rand"
          "time"
  
          "github.com/taosdata/driver-go/v3/common"
          "github.com/taosdata/driver-go/v3/common/param"
          _ "github.com/taosdata/driver-go/v3/taosRestful"
          "github.com/taosdata/driver-go/v3/ws/stmt"
  )
  
  func main() {
          host := "127.0.0.1"
          numOfSubTable := 10
          numOfRow := 10
  
          taosDSN := fmt.Sprintf("root:taosdata@http(%s:6041)/", host)
          db, err := sql.Open("taosRestful", taosDSN)
          if err != nil {
                  log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
          }
          defer db.Close()
          // prepare database and table
          _, err = db.Exec("CREATE DATABASE IF NOT EXISTS power")
          if err != nil {
                  log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
          }
          _, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
          if err != nil {
                  log.Fatalln("Failed to create stable power.meters, ErrMessage: " + err.Error())
          }
  
          config := stmt.NewConfig(fmt.Sprintf("ws://%s:6041", host), 0)
          config.SetConnectUser("root")
          config.SetConnectPass("taosdata")
          config.SetConnectDB("power")
          config.SetMessageTimeout(common.DefaultMessageTimeout)
          config.SetWriteWait(common.DefaultWriteWait)
  
          connector, err := stmt.NewConnector(config)
          if err != nil {
                  log.Fatalln("Failed to create stmt connector,url: " + taosDSN + "; ErrMessage: " + err.Error())
          }
          // prepare statement
          sql := "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)"
          stmt, err := connector.Init()
          if err != nil {
                  log.Fatalln("Failed to init stmt, sql: " + sql + ", ErrMessage: " + err.Error())
          }
          err = stmt.Prepare(sql)
          if err != nil {
                  log.Fatal("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
          }
          for i := 1; i <= numOfSubTable; i++ {
                  tableName := fmt.Sprintf("d_bind_%d", i)
                  tags := param.NewParam(2).AddInt(i).AddBinary([]byte(fmt.Sprintf("location_%d", i)))
                  tagsType := param.NewColumnType(2).AddInt().AddBinary(24)
                  columnType := param.NewColumnType(4).AddTimestamp().AddFloat().AddInt().AddFloat()
                  // set tableName
                  err = stmt.SetTableName(tableName)
                  if err != nil {
                          log.Fatal("Failed to set table name, tableName: " + tableName + "; ErrMessage: " + err.Error())
                  }
                  // set tags
                  err = stmt.SetTags(tags, tagsType)
                  if err != nil {
                          log.Fatal("Failed to set tags, ErrMessage: " + err.Error())
                  }
                  // bind column data
                  current := time.Now()
                  for j := 0; j < numOfRow; j++ {
                          columnData := make([]*param.Param, 4)
                          columnData[0] = param.NewParam(1).AddTimestamp(current.Add(time.Millisecond*time.Duration(j)), common.PrecisionMilliSecond)
                          columnData[1] = param.NewParam(1).AddFloat(rand.Float32() * 30)
                          columnData[2] = param.NewParam(1).AddInt(rand.Intn(300))
                          columnData[3] = param.NewParam(1).AddFloat(rand.Float32())
                          err = stmt.BindParam(columnData, columnType)
                          if err != nil {
                                  log.Fatal("Failed to bind params, ErrMessage: " + err.Error())
                          }
                  }
                  // add batch
                  err = stmt.AddBatch()
                  if err != nil {
                          log.Fatal("Failed to add batch, ErrMessage: " + err.Error())
                  }
                  // execute batch
                  err = stmt.Exec()
                  if err != nil {
                          log.Fatal("Failed to exec, ErrMessage: " + err.Error())
                  }
                  // get affected rows
                  affected := stmt.GetAffectedRows()
                  // you can check exeResult here
                  fmt.Printf("Successfully inserted %d rows to %s.\n", affected, tableName)
          }
          err = stmt.Close()
          if err != nil {
                  log.Fatal("Failed to close stmt, ErrMessage: " + err.Error())
          }
  }
  
  ```

#### 4.4.3 订阅

包名 `github.com/taosdata/driver-go/v3/ws/tmq`
1. 接口描述
  - `func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error)`
    - **功能**：创建一个新的 `Consumer` 实例，使用提供的 `tmq.ConfigMap` 配置。
    - **参数**：
      - `conf`：`*tmq.ConfigMap` 类型，指向包含消费者配置信息的映射。
    - **返回值**：
      - `*Consumer`：指向新创建的 `Consumer` 实例的指针。
      - `error`：如果创建过程中发生错误，则返回一个错误对象；否则返回 `nil`。
  - `func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error`
    - **功能**：订阅指定的主题。
    - **参数**：
      - `topic`：`string` 类型，要订阅的主题名称。
      - `rebalanceCb`：`RebalanceCb` 类型，无效果。
    - **返回值**：
      - `error`：如果订阅过程中发生错误，则返回一个错误对象；否则返回 `nil`。
  - `func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error`
    - **功能**：订阅多个主题。
    - **参数**：
      - `topics`：要订阅的主题名称列表。
      - `rebalanceCb`：无效参数。
    - **返回值**：返回一个 `error` 类型，如果订阅成功则返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Unsubscribe() error`
    - **功能**：取消订阅。
    - **返回值**：返回一个 `error` 类型，如果取消订阅成功则返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Poll(timeoutMs int) tmq.Event`
    - **功能**：轮询消息，等待消息的到达或超时。
    - **参数**：
      - `timeoutMs`：轮询的超时时间（毫秒）。
    - **返回值**：返回一个 `tmq.Event` 类型，包含接收到的消息或错误信息。
  - `func (c *Consumer) Commit() ([]tmq.TopicPartition, error)`
    - **功能**：提交当前已消费的偏移量。
    - **返回值**：返回一个 `[]tmq.TopicPartition` 和一个 `error` 类型，`[]tmq.TopicPartition` 包含已提交的分区信息，如果提交成功 `error` 返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Assignment() (partitions []tmq.TopicPartition, err error)`
    - **功能**：获取当前分配给消费者的分区信息。
    - **返回值**：返回一个 `[]tmq.TopicPartition` 和一个 `error` 类型，`[]tmq.TopicPartition` 包含分配的分区信息，如果获取成功 `error` 返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Seek(partition tmq.TopicPartition, ignoredTimeoutMs int) error`
    - **功能**：将指定分区的消费者偏移量设置到指定的位置。
    - **参数**：
      - `partition`：要设置的分区信息，类型为 `tmq.TopicPartition`。
      - `ignoredTimeoutMs`：此参数在当前实现中被忽略，保留用于接口一致性。
    - **返回值**：返回一个 `error` 类型，如果设置成功则返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Committed(partitions []tmq.TopicPartition, timeoutMs int) (offsets []tmq.TopicPartition, err error)`
    - **功能**：获取指定分区的已提交偏移量。
    - **参数**：
      - `partitions`：要查询的分区信息列表。
      - `timeoutMs`：查询的超时时间（毫秒）。
    - **返回值**：返回一个 `[]tmq.TopicPartition` 和一个 `error` 类型，`[]tmq.TopicPartition` 包含已提交的偏移量信息，如果查询成功`error` 返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) CommitOffsets(offsets []tmq.TopicPartition) ([]tmq.TopicPartition, error)`
    - **功能**：提交指定的偏移量到服务器。
    - **参数**：
      - `offsets`：要提交的偏移量信息列表。
    - **返回值**：返回一个 `[]tmq.TopicPartition` 和一个 `error` 类型，`[]tmq.TopicPartition` 包含已提交的分区信息，如果提交成功`error` 返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Position(partitions []tmq.TopicPartition) (offsets []tmq.TopicPartition, err error)`
    - **功能**：获取指定分区的当前位置（即下一个要消费的偏移量）。
    - **参数**：
      - `partitions`：要查询的分区信息列表。
    - **返回值**：返回一个 `[]tmq.TopicPartition` 和一个 `error` 类型，`[]tmq.TopicPartition` 包含当前位置信息，如果查询成功`error` 返回 `nil`，否则返回错误信息。
  - `func (c *Consumer) Close() error`
    **功能**：关闭消费者，释放相关资源。
    **返回值**：返回一个 `error` 类型，如果关闭成功则返回 `nil`，否则返回错误信息。
1. 样例
  ```go {wrap}
  package main
  
  import (
      "database/sql"
      "fmt"
      "log"
      "time"
  
      "github.com/taosdata/driver-go/v3/common"
      tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
      _ "github.com/taosdata/driver-go/v3/taosWS"
      "github.com/taosdata/driver-go/v3/ws/tmq"
  )
  
  var done = make(chan struct{})
  var groupID string
  var clientID string
  var host string
  var topic string
  
  func main() {
      // init env
      taosDSN := "root:taosdata@ws(127.0.0.1:6041)/"
      conn, err := sql.Open("taosWS", taosDSN)
      if err != nil {
         log.Fatalln("Failed to connect to " + taosDSN + ", ErrMessage: " + err.Error())
      }
      defer func() {
         conn.Close()
      }()
      initEnv(conn)
      // ANCHOR: create_consumer
      // create consumer
      wsUrl := "ws://127.0.0.1:6041"
      groupID = "group1"
      clientID = "client1"
      host = "127.0.0.1"
      consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
         "ws.url":                  wsUrl,
         "ws.message.channelLen":   uint(0),
         "ws.message.timeout":      common.DefaultMessageTimeout,
         "ws.message.writeWait":    common.DefaultWriteWait,
         "td.connect.user":         "root",
         "td.connect.pass":         "taosdata",
         "auto.offset.reset":       "latest",
         "msg.with.table.name":     "true",
         "enable.auto.commit":      "true",
         "auto.commit.interval.ms": "1000",
         "group.id":                groupID,
         "client.id":               clientID,
      })
      if err != nil {
         log.Fatalf(
            "Failed to create websocket consumer, host: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
            host,
            groupID,
            clientID,
            err.Error(),
         )
      }
      log.Printf("Create consumer successfully, host: %s, groupId: %s, clientId: %s\n", host, groupID, clientID)
  
      // ANCHOR: subscribe
      topic = "topic_meters"
      err = consumer.Subscribe(topic, nil)
      if err != nil {
         log.Fatalf(
            "Failed to subscribe topic_meters, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
            topic,
            groupID,
            clientID,
            err.Error(),
         )
      }
      log.Println("Subscribe topics successfully")
      for i := 0; i < 50; i++ {
         ev := consumer.Poll(100)
         if ev != nil {
            switch e := ev.(type) {
            case *tmqcommon.DataMessage:
               // process your data here
               fmt.Printf("data:%v\n", e)
               // ANCHOR: commit_offset
               // commit offset
               _, err = consumer.CommitOffsets([]tmqcommon.TopicPartition{e.TopicPartition})
               if err != nil {
                  log.Fatalf(
                     "Failed to commit offset, topic: %s, groupId: %s, clientId: %s, offset %s, ErrMessage: %s\n",
                     topic,
                     groupID,
                     clientID,
                     e.TopicPartition,
                     err.Error(),
                  )
               }
               log.Println("Commit offset manually successfully.")
            case tmqcommon.Error:
               log.Fatalf(
                  "Failed to poll data, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
                  topic,
                  groupID,
                  clientID,
                  e.Error(),
               )
            }
         }
      }
      // ANCHOR: seek
      // get assignment
      partitions, err := consumer.Assignment()
      if err != nil {
         log.Fatalf(
            "Failed to get assignment, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
            topic,
            groupID,
            clientID,
            err.Error(),
         )
      }
      fmt.Println("Now assignment:", partitions)
      for i := 0; i < len(partitions); i++ {
         // seek to the beginning
         err = consumer.Seek(tmqcommon.TopicPartition{
            Topic:     partitions[i].Topic,
            Partition: partitions[i].Partition,
            Offset:    0,
         }, 0)
         if err != nil {
            log.Fatalf(
               "Failed to seek offset, topic: %s, groupId: %s, clientId: %s, partition: %d, offset: %d, ErrMessage: %s\n",
               topic,
               groupID,
               clientID,
               partitions[i].Partition,
               0,
               err.Error(),
            )
         }
      }
      fmt.Println("Assignment seek to beginning successfully")
      // ANCHOR: close
      // unsubscribe
      err = consumer.Unsubscribe()
      if err != nil {
         log.Fatalf(
            "Failed to unsubscribe consumer, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
            topic,
            groupID,
            clientID,
            err.Error(),
         )
      }
      fmt.Println("Consumer unsubscribed successfully.")
      // close consumer
      err = consumer.Close()
      if err != nil {
         log.Fatalf(
            "Failed to close consumer, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
            topic,
            groupID,
            clientID,
            err.Error(),
         )
      }
      fmt.Println("Consumer closed successfully.")
      <-done
  }
  
  func initEnv(conn *sql.DB) {
      _, err := conn.Exec("CREATE DATABASE IF NOT EXISTS power")
      if err != nil {
         log.Fatal("Failed to create database, ErrMessage: " + err.Error())
      }
      _, err = conn.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
      if err != nil {
         log.Fatal("Failed to create stable, ErrMessage: " + err.Error())
      }
      _, err = conn.Exec("CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM power.meters")
      if err != nil {
         log.Fatal("Failed to create topic, ErrMessage: " + err.Error())
      }
      go func() {
         for i := 0; i < 10; i++ {
            time.Sleep(time.Second)
            _, err = conn.Exec("INSERT INTO power.d1001 USING power.meters TAGS (2, 'California.SanFrancisco') VALUES (NOW , 10.2, 219, 0.32)")
            if err != nil {
               log.Fatal("Failed to insert data, ErrMessage: " + err.Error())
            }
         }
         done <- struct{}{}
      }()
  }
  ```

### 4.5 参数绑定参数和类型

1. 绑定参数
  - `func NewParam(size int) *Param`
    - **接口说明**：该函数用于创建一个新的`Param`类型的实例，并返回一个指向该实例的指针。
    - **参数说明**：
      - `size`：一个整型（`int`）参数，指定新创建的`Param`实例的大小
    - **返回值**：
      - `*Param`：返回一个指向新创建的`Param`实例的指针。
  - `func NewParamsWithRowValue(value []driver.Value) []*Param`
    - **功能**：根据提供的 `driver.Value` 切片创建一个 `Param` 实例的切片，每个 `Param` 只有一个值且与`value` 对应，此接口内部用于 `database/sql` 驱动绑定单行。
    - **参数**：
      - `value`：一个 `driver.Value` 类型的切片，包含用于初始化 `Param` 实例的数据。
    - **返回值**：
      - `[]*Param`：返回一个指向 `Param` 实例的指针切片。
  - `func (p *Param) SetBool(offset int, value bool)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置布尔值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：布尔型，要设置的值。
  - `func (p *Param) SetNull(offset int)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置空值。
    - **参数**：
      - `offset`：整型，指定要设置空值的偏移量。
  - `func (p *Param) SetTinyint(offset int, value int)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 TINYINT 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：整型，要设置的 TINYINT 值。
  - `func (p *Param) SetSmallint(offset int, value int)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 SMALLINT 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：整型，要设置的 SMALLINT 值。
  - `func (p *Param) SetInt(offset int, value int)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 INT 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：整型，要设置的 INT 值。
  - `func (p *Param) SetBigint(offset int, value int)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 BIGINT 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：整型，要设置的 BIGINT 值。
  - `func (p *Param) SetUTinyint(offset int, value uint)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 UNSIGNED TINYINT 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：无符号整型，要设置的 UNSIGNED TINYINT 值。
  - `func (p *Param) SetUSmallint(offset int, value uint)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 UNSIGNED SMALLINT 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：无符号整型，要设置的 UNSIGNED SMALLINT 值。
  - `func (p *Param) SetUInt(offset int, value uint)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 UNSIGNED INT 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：无符号整型，要设置的 UNSIGNED INT 值。
  - `func (p *Param) SetUBigint(offset int, value uint)`
    - **功能**（假设有效）：在 `Param` 对象的指定偏移量位置设置 UNSIGNED BIGINT 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：无符号整型，要设置的 UNSIGNED BIGINT 值。
  - `func (p *Param) SetFloat(offset int, value float32)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 FLOAT 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：32位浮点型，要设置的 FLOAT 值。
  - `func (p *Param) SetDouble(offset int, value float64)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 DOUBLE 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：64位浮点型，要设置的 DOUBLE 值。
  - `func (p *Param) SetBinary(offset int, value []byte)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 BINARY 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：字节切片，要设置的 BINARY 值。
  - `func (p *Param) SetVarBinary(offset int, value []byte)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 VARBINARY 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：字节切片，要设置的 VARBINARY 值。
  - `func (p *Param) SetNchar(offset int, value string)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 NCHAR 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：字符串，要设置的 NCHAR 值。
  - `func (p *Param) SetTimestamp(offset int, value time.Time, precision int)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 TIMESTAMP 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：时间类型，要设置的 TIMESTAMP 值。
      - `precision`：整型，时间戳的小数秒精度。
  - `func (p *Param) SetJson(offset int, value []byte)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 JSON 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：字节切片，要设置的 JSON 值。
  - `func (p *Param) SetGeometry(offset int, value []byte)`
    - **功能**：在 `Param` 对象的指定偏移量位置设置 GEOMETRY 值。
    - **参数**：
      - `offset`：整型，指定要设置的值的偏移量。
      - `value`：字节切片，要设置的 GEOMETRY 值。
  - `func (p *Param) AddBool(value bool) *Param`
    - **功能**：向 `Param` 对象添加一个布尔值。
    - **参数**：
      - `value`：要添加的布尔值。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddNull() *Param`
    - **功能**：向 `Param` 对象添加一个空值。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddTinyint(value int) *Param`
    - **功能**：向 `Param` 对象添加一个 TINYINT 值。
    - **参数**：
      - `value`：要添加的 TINYINT 值（整型）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddSmallint(value int) *Param`
    - **功能**：向 `Param` 对象添加一个 SMALLINT 值。
    - **参数**：
      - `value`：要添加的 SMALLINT 值（整型）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddInt(value int) *Param`
    - **功能**：向 `Param` 对象添加一个 INT 值。
    - **参数**：
      - `value`：要添加的 INT 值（整型）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddBigint(value int64) *Param`
    - **功能**：向 `Param` 对象添加一个 BIGINT 值。
    - **参数**：
      - `value`：要添加的 BIGINT 值（`int64` 型）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddUTinyint(value uint8) *Param`
    - **功能**：向 `Param` 对象添加一个 UNSIGNED TINYINT 值。
    - **参数**：
      - `value`：要添加的 UNSIGNED TINYINT 值（无符号 8 位整型）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddUSmallint(value uint16) *Param`
    - **功能**：向 `Param` 对象添加一个 UNSIGNED SMALLINT 值。
    - **参数**：
      - `value`：要添加的 UNSIGNED SMALLINT 值（无符号 16 位整型）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddUInt(value uint32) *Param`
    - **功能**：向 `Param` 对象添加一个 UNSIGNED INT 值。
    - **参数**：
      - `value`：要添加的 UNSIGNED INT 值（无符号 32 位整型）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddUBigint(value uint64) *Param`
    - **功能**：向 `Param` 对象添加一个 UNSIGNED BIGINT 值。
    - **参数**：
      - `value`：要添加的 UNSIGNED BIGINT 值（无符号 64 位整型）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddFloat(value float32) *Param`
    - **功能**：向 `Param` 对象添加一个 FLOAT 值。
    - **参数**：
      - `value`：要添加的 FLOAT 值（32 位浮点型）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddDouble(value float64) *Param`
    - **功能**：向 `Param` 对象添加一个 DOUBLE 值。
    - **参数**：
      - `value`：要添加的 DOUBLE 值（64 位浮点型）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddBinary(value []byte) *Param`
    - **功能**：向 `Param` 对象添加一个 BINARY 值。
    - **参数**：
      - `value`：要添加的 BINARY 值（字节切片）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddVarBinary(value []byte) *Param`
    - **功能**：向 `Param` 对象添加一个 VARBINARY 值。
    - **参数**：
      - `value`：要添加的 VARBINARY 值（字节切片）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddNchar(value string) *Param`
    - **功能**：向 `Param` 对象添加一个 NCHAR 值。
    - **参数**：
      - `value`：要添加的 NCHAR 值（字符串）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddTimestamp(value time.Time, precision int) *Param`
    - **功能**：向 `Param` 对象添加一个 TIMESTAMP 值。
    - **参数**：
      - `value`：要添加的 TIMESTAMP 值（时间类型）。
      - `precision`：时间戳的小数秒精度。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddJson(value []byte) *Param`
    - **功能**：向 `Param` 对象添加一个 JSON 值。
    - **参数**：
      - `value`：要添加的 JSON 值（字节切片）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) AddGeometry(value []byte) *Param`
    - **功能**：向 `Param` 对象添加一个 GEOMETRY 值。
    - **参数**：
      - `value`：要添加的 GEOMETRY 值（字节切片）。
    - **返回值**：返回 `Param` 对象的指针，允许链式调用。
  - `func (p *Param) GetValues() []driver.Value`
    - **功能**：从 `Param` 实例中检索并返回所有已设置的参数值。
    - **返回值**：一个 `[]driver.Value` 类型的切片，包含所有参数值。
  - `func (p *Param) AddValue(value interface{}) *Param`
    **功能**：向 `Param` 实例中添加一个新的参数值。
    **参数**：
    `value`：要添加的参数值，类型为 `interface{}`，表示可以是任何类型。
    **返回值**：返回当前 `Param` 实例的指针，支持链式调用。
1. 参数绑定类型
  - `func NewColumnType(size int) *ColumnType`
    - **功能**：创建并初始化一个新的 `ColumnType` 结构体实例。
    - **参数**：
      - `size`：整数型，指定列的大小或长度。
    - **返回值**：指向 `ColumnType` 结构体的指针
  - `func NewColumnTypeWithValue(value []*types.ColumnType) *ColumnType`
    - **功能**：根据提供的 `types.ColumnType` 切片创建一个新的 `ColumnType` 实例
    - **参数**：
      - `value`：`[]*types.ColumnType` 类型，用于初始化。
    - **返回值**：返回一个新的 `ColumnType` 结构体的指针。
  - `func (c *ColumnType) AddBool() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个布尔类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针，支持链式调用。
  - `func (c *ColumnType) AddTinyint() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 TINYINT 类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddSmallint() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 SMALLINT 类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddInt() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 INT 类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddBigint() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 BIGINT 类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddUTinyint() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 UNSIGNED TINYINT 类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddUSmallint() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 UNSIGNED SMALLINT 类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddUInt() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 UNSIGNED INT 类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddUBigint() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 UNSIGNED BIGINT 类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddFloat() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 FLOAT 类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddDouble() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 DOUBLE 类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddBinary(strMaxLen int) *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 BINARY 类型的列信息，并指定最大长度。
    - **参数**：`strMaxLen` 指定 BINARY 列的最大长度。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddVarBinary(strMaxLen int) *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 VARBINARY 类型的列信息，并指定最大长度。
    - **参数**：`strMaxLen` 指定 VARBINARY 列的最大长度。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddNchar(strMaxLen int) *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 NCHAR 类型的列信息，并指定最大长度。
    - **参数**：`strMaxLen` 指定 NCHAR 列的最大长度。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddTimestamp() *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 TIMESTAMP 类型的列信息。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddJson(strMaxLen int) *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 JSON 类型的列信息，并指定最大长度。
    - **参数**：`strMaxLen` 指定 JSON 列的最大长度（如果适用）。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) AddGeometry(strMaxLen int) *ColumnType`
    - **功能**：向 `ColumnType` 实例中添加一个 GEOMETRY 类型的列信息，并指定最大长度。
    - **参数**：`strMaxLen` 指定 GEOMETRY 列的最大长度或相关参数。
    - **返回值**：返回当前 `ColumnType` 实例的指针。
  - `func (c *ColumnType) GetValue() ([]*types.ColumnType, error)`
    **功能**：获取当前 `ColumnType` 实例中已添加的列信息列表。
    **返回值**：返回 `types.ColumnType` 指针的切片，以及可能发生的错误。

### 4.6 订阅参数和结果

1. 订阅参数
  ```go {wrap}
  type ConfigValue interface{}
  type ConfigMap map[string]ConfigValue
  ```

  创建消费者支持属性：
  - `ws.url`：WebSocket 连接地址。
  - `ws.message.channelLen`：WebSocket 消息通道缓存长度，默认 0。
  - `ws.message.timeout`：WebSocket 消息超时时间，默认 5m。
  - `ws.message.writeWait`：WebSocket 写入消息超时时间，默认 10s。
  - `ws.message.enableCompression`：WebSocket 是否启用压缩，默认 false。
  - 其它参数与 TDengine 相同。
1. 订阅结果
  ```go {wrap}
  type Event interface {
      String() string
  }
  ```

   - 数据消息
    ```go {wrap}
    type DataMessage struct {
        TopicPartition TopicPartition
        dbName         string
        topic          string
        data           []*Data
        offset         Offset
    }
    
    // 获取主题
    func (m *DataMessage) Topic() string {
        return m.topic
    }
    // 获取数据库
    func (m *DataMessage) DBName() string {
        return m.dbName
    }
    
    // 获取值
    func (m *DataMessage) Value() interface{} {
        return m.data
    }
    
    // 获取偏移
    func (m *DataMessage) Offset() Offset {
        return m.offset
    }
    
    // 分区信息
    type TopicPartition struct {
        Topic     *string
        Partition int32
        Offset    Offset
        Metadata  *string
        Error     error
    }
    
    // 数据
    type Data struct {
        TableName string
        Data      [][]driver.Value
    }
    
    // 偏移量
    type Offset int64
    
    // 未设置的偏移量
    const OffsetInvalid = Offset(-2147467247)
    ```

   - 元数据消息
    ```go {wrap}
    type MetaMessage struct {
        TopicPartition TopicPartition
        dbName         string
        topic          string
        offset         Offset
        meta           *Meta
    }
    
    func (m *MetaMessage) Topic() string {
        return m.topic
    }
    
    func (m *MetaMessage) DBName() string {
        return m.dbName
    }
    
    // 获取 meta 信息
    func (m *MetaMessage) Value() interface{} {
        return m.meta
    }
    
    // meta 内容
    type Meta struct {
        Type          string        `json:"type"`
        TableName     string        `json:"tableName"`
        TableType     string        `json:"tableType"`
        CreateList    []*CreateItem `json:"createList"`
        Columns       []*Column     `json:"columns"`
        Using         string        `json:"using"`
        TagNum        int           `json:"tagNum"`
        Tags          []*Tag        `json:"tags"`
        TableNameList []string      `json:"tableNameList"`
        AlterType     int           `json:"alterType"`
        ColName       string        `json:"colName"`
        ColNewName    string        `json:"colNewName"`
        ColType       int           `json:"colType"`
        ColLength     int           `json:"colLength"`
        ColValue      string        `json:"colValue"`
        ColValueNull  bool          `json:"colValueNull"`
    }
    
    type Tag struct {
        Name  string      `json:"name"`
        Type  int         `json:"type"`
        Value interface{} `json:"value"`
    }
    
    type Column struct {
        Name   string `json:"name"`
        Type   int    `json:"type"`
        Length int    `json:"length"`
    }
    ```

   - 数据和元数据消息
    ```go {wrap}
    type MetaDataMessage struct {
        TopicPartition TopicPartition
        dbName         string
        topic          string
        offset         Offset
        metaData       *MetaData
    }
    
    type MetaData struct {
        Meta *Meta
        Data []*Data
    }
    
    func (m *MetaDataMessage) Topic() string {
        return m.topic
    }
    
    func (m *MetaDataMessage) DBName() string {
        return m.dbName
    }
    
    func (m *MetaDataMessage) Value() interface{} {
        return m.metaData
    }
    ```

   - 错误
    ```go {wrap}
    type Error struct {
        code int
        str  string
    }
    
    func (e Error) String() string {
        return fmt.Sprintf("[0x%x] %s", e.code, e.str)
    }
    
    func (e Error) Error() string {
        return e.String()
    }
    
    func (e Error) Code() int {
        return e.code
    }
    ```

## 5. 安全特性

### 5.1 认证机制

#### 5.1.1 基本身份认证

Go 连接器支持多种身份认证机制，根据不同的连接类型和使用场景选择合适的认证方式。

##### 5.1.1.1 用户名密码认证

所有连接类型（原生、WebSocket、RESTful）都支持基本的用户名密码认证。在 DSN 中通过以下格式配置：
```plaintext
username:password@protocol(address)/dbname
```

密码会通过 URL 编码进行处理，防止特殊字符导致的解析错误。
**原生连接示例：**
```go
var taosDSN = "root:taosdata@tcp(localhost:6030)/"
db, err := sql.Open("taosSql", taosDSN)
```

**WebSocket 连接示例：**
```go
var taosDSN = "root:taosdata@ws(localhost:6041)/"
db, err := sql.Open("taosWS", taosDSN)
```

**RESTful 连接示例：**
```go
var taosDSN = "root:taosdata@http(localhost:6041)/"
db, err := sql.Open("taosRestful", taosDSN)
```

#### 5.1.2 云服务认证

##### 5.1.2.1 Token 认证

WebSocket 和 RESTful 连接支持使用 Token 连接云服务实例。Token 认证优先级最高，当 DSN 中同时包含用户名密码和 Token 时，优先使用 Token。
**WebSocket Token 认证示例：**
```go
var taosDSN = "wss(gw.cloud.taosdata.com:443)/?token=your_token"
db, err := sql.Open("taosWS", taosDSN)
```

**RESTful Token 认证示例：**
```go
var taosDSN = "https(gw.cloud.taosdata.com:443)/?token=your_token"
db, err := sql.Open("taosRestful", taosDSN)
```

##### 5.1.2.2 Bearer Token 认证

WebSocket 和 RESTful 连接还支持 Bearer Token 认证机制，用于 TSDB 企业版鉴权。Bearer Token 会被添加到 HTTP 请求的 Authorization 头中。
**WebSocket Bearer Token 示例：**
```go
var taosDSN = "ws(localhost:6041)/?bearerToken=your_bearer_token"
db, err := sql.Open("taosWS", taosDSN)
```

**RESTful Bearer Token 示例：**
```go
var taosDSN = "http(localhost:6041)/?bearerToken=your_bearer_token"
db, err := sql.Open("taosRestful", taosDSN)
```

#### 5.1.3 双因子认证

##### 5.1.3.1 TOTP 认证

WebSocket 连接支持基于时间的一次性密码（TOTP）双因子认证机制，提供额外的安全层。TOTP 代码通过 `totpCode` 参数传递。
**WebSocket TOTP 认证示例：**
```go
var taosDSN = "ws(localhost:6041)/?totpCode=123456"
db, err := sql.Open("taosWS", taosDSN)
```

driver-go 提供了 TOTP 相关的工具函数：
```go
import "github.com/taosdata/driver-go/v3/common"

// 生成 TOTP 密钥
secret := common.GenerateTOTPSecret(seed)
// 将密钥转为 Base32 编码字符串
secretStr := common.TOTPSecretStr(secret)
// 生成 TOTP 代码
code := common.GenerateTOTPCode(secret, counter, 6)
```

### 5.2 传输安全

#### 5.2.1 TLS/SSL 加密

##### 5.2.1.1 WebSocket Secure (WSS)

WebSocket 连接支持 WSS 协议，提供传输层加密。只需将协议由 `ws` 改为 `wss` 即可启用。
```go
// 使用 WSS 协议
var taosDSN = "root:taosdata@wss(localhost:6041)/"
db, err := sql.Open("taosWS", taosDSN)
```

##### 5.2.1.2 HTTPS 协议

RESTful 连接支持 HTTPS 协议，提供传输层加密。将协议由 `http` 改为 `https` 即可启用。
```go
// 使用 HTTPS 协议
var taosDSN = "root:taosdata@https(localhost:6041)/"
db, err := sql.Open("taosRestful", taosDSN)
```

#### 5.2.2 TLS 证书验证

RESTful 连接默认启用 TLS 证书验证。如果需要连接不安全的服务或跳过证书验证（仅用于测试环境），可以设置 `skipVerify` 参数。
```go
// 跳过证书验证（不推荐用于生产环境）
var taosDSN = "root:taosdata@https(localhost:6041)/?skipVerify=true"
db, err := sql.Open("taosRestful", taosDSN)
```

❗ **安全警告**：在生产环境中应始终启用证书验证，跳过证书验证可能会造成中间人攻击。

#### 5.2.3 数据压缩

##### 5.2.3.1 WebSocket 压缩

WebSocket 连接支持数据压缩，可以减少网络传输数据量。通过 `enableCompression` 参数启用。
```go
var taosDSN = "root:taosdata@ws(localhost:6041)/?enableCompression=true"
db, err := sql.Open("taosWS", taosDSN)
```

##### 5.2.3.2 RESTful 压缩

RESTful 连接默认不启用压缩。如需启用 gzip 压缩，可设置 `disableCompression` 为 false。
```go
var taosDSN = "root:taosdata@http(localhost:6041)/?disableCompression=false"
db, err := sql.Open("taosRestful", taosDSN)
```

### 5.3 连接安全

#### 5.3.1 超时控制

##### 5.3.1.1 WebSocket 超时配置

WebSocket 连接支持读写超时配置，防止连接挂起：
- `readTimeout`：读取数据的超时时间，默认 5 分钟
- `writeTimeout`：写入数据的超时时间，默认 10 秒
```go
var taosDSN = "root:taosdata@ws(localhost:6041)/?readTimeout=10m&writeTimeout=30s"
db, err := sql.Open("taosWS", taosDSN)
```

##### 5.3.1.2 RESTful 超时配置

RESTful 连接使用 Go 标准库的 HTTP 客户端，自动配置了合理的超时参数：
- TLS Handshake 超时：10 秒
- Idle 连接超时：90 秒

#### 5.3.2 请求追踪

##### 5.3.2.1 请求 ID (req_id)

原生连接和 WebSocket 连接支持请求 ID 机制，用于链路追踪和审计日志。
**原生连接使用请求 ID：**
```go
import (
    "context"
    "github.com/taosdata/driver-go/v3/common"
)

// 生成请求 ID
reqID := common.GetReqID()
ctx := context.WithValue(context.Background(), "taos_req_id", reqID)

// 在查询中使用
rows, err := db.QueryContext(ctx, "SELECT * FROM meters")
```

**af 包使用请求 ID：**
```go
import "github.com/taosdata/driver-go/v3/af"

conn, _ := af.Open("localhost", "root", "taosdata", "", 6030)
reqID := common.GetReqID()
res, err := conn.ExecWithReqID("CREATE DATABASE test", reqID)
```

#### 5.3.3 连接管理

##### 5.3.3.1 连接池管理

`database/sql` 包提供了内置的连接池管理功能，可以通过以下方法配置：
```go
db, err := sql.Open("taosSql", taosDSN)
// 设置最大打开连接数
db.SetMaxOpenConns(100)
// 设置最大空闲连接数
db.SetMaxIdleConns(10)
// 设置连接最大生命周期
db.SetConnMaxLifetime(time.Hour)
```

##### 5.3.3.2 原生连接并发控制

原生连接通过 `cgoThread` 参数控制 CGO 调用的并发度，防止创建过多线程：
```go
var taosDSN = "root:taosdata@tcp(localhost:6030)/?cgoThread=10"
db, err := sql.Open("taosSql", taosDSN)
```

### 5.4 SQL 注入防护

#### 5.4.1 参数化查询

Go 连接器支持参数化查询（PreparedStatement），防止 SQL 注入攻击。

##### 5.4.1.1 database/sql 参数绑定

```go
// 准备语句
stmt, err := db.Prepare("INSERT INTO meters VALUES (?, ?, ?, ?)")
defer stmt.Close()

// 绑定参数执行
_, err = stmt.Exec(time.Now(), 10.3, 219, 0.31)
```

##### 5.4.1.2 客户端占位符替换

所有连接类型默认启用 `interpolateParams` 功能，在客户端进行参数替换：
```go
// 自动在客户端进行参数替换
rows, err := db.Query("SELECT * FROM meters WHERE ts > ?", time.Now())
```

如需禁用，可设置：
```go
var taosDSN = "root:taosdata@ws(localhost:6041)/?interpolateParams=false"
```

#### 5.4.2 输入验证

DSN 解析器对所有参数进行严格验证：
- 端口号必须为有效数字
- 布尔类型参数必须为 true/false
- 数值类型参数必须为有效数字
- 时区参数必须为有效的时区标识符
非法参数会导致连接失败，防止配置错误导致的安全问题。

### 5.5 安全最佳实践

#### 5.5.1 使用加密连接

在生产环境中，应始终使用 WSS 或 HTTPS 协议：
```go
// 推荐：使用 WSS
var taosDSN = "root:taosdata@wss(server.example.com:6041)/"

// 或使用 HTTPS
var taosDSN = "root:taosdata@https(server.example.com:6041)/"
```

#### 5.5.2 保护凭证信息

不要在代码中硬编码凭证信息，应使用环境变量或配置文件：
```go
import "os"

username := os.Getenv("TDENGINE_USER")
password := os.Getenv("TDENGINE_PASSWORD")
taosDSN := fmt.Sprintf("%s:%s@wss(server:6041)/", username, password)
```

#### 5.5.3 启用请求追踪

在所有数据库操作中使用请求 ID，便于问题追踪和审计：
```go
reqID := common.GetReqID()
ctx := context.WithValue(context.Background(), "taos_req_id", reqID)
log.Printf("Executing query with req_id: 0x%x", reqID)
_, err := db.ExecContext(ctx, sql)
```

#### 5.5.4 使用参数化查询

始终使用参数化查询防止 SQL 注入：
```go
// 推荐：使用参数绑定
stmt, _ := db.Prepare("SELECT * FROM meters WHERE location = ?")
rows, _ := stmt.Query(userInput)

// 不推荐：字符串拼接
sql := fmt.Sprintf("SELECT * FROM meters WHERE location = '%s'", userInput)
```

#### 5.5.5 合理配置超时

根据应用场景配置合理的超时时间，防止连接挂起：
```go
var taosDSN = "root:taosdata@ws(localhost:6041)/?readTimeout=5m&writeTimeout=30s"
```

#### 5.5.6 定期轮转凭证

对于长期运行的应用，应定期轮转数据库凭证，降低凭证泄露风险。

#### 5.5.7 监控连接状态

定期检查数据库连接状态：
```go
// 定期 Ping 检查
if err := db.PingContext(ctx); err != nil {
    log.Printf("Database connection lost: %v", err)
    // 重连逻辑
}
```

## 6. 性能

1. 以二进制数据块的方式与 taosadapter 交互，提高传输性能。
2. 提供多行数据绑定，提升参数绑定性能。
3. 支持 WebSocket 数据压缩，优化公网数据传输性能。

## 7. 兼容性

| v3.5.5 | 3.2.3.0及以上 |
| --- | --- |
| v3.5.4 | 3.2.3.0及以上 |
| v3.5.3 | 3.2.3.0及以上 |
| v3.5.2 | 3.2.3.0及以上 |
| v3.5.1 | 3.2.1.0及以上 |
| v3.5.0 | 3.0.5.0及以上 |
| v3.3.1 | 3.0.4.1及以上 |
| v3.1.0 | 3.0.2.2及以上 |
| v3.0.4 | 3.0.2.2及以上 |
| v3.0.3 | 3.0.1.5及以上 |
| v3.0.2 | 3.0.1.5及以上 |
| v3.0.1 | 3.0.0.0及以上 |
| v3.0.0 | 3.0.0.0及以上 |

## 8. 运维

无。

## 9. 使用场景

### 9.1 **Go 语言开发的应用程序**

- **场景描述**：如果你正在使用 Go 语言开发应用程序，并且需要与 TDengine 数据库进行交互，那么 `taosdata/driver-go` 是必不可少的工具。
- **使用场景**：
  - 在 Go 应用中连接 TDengine 数据库。
  - 执行 SQL 查询、插入、更新等操作。
  - 处理 TDengine 返回的数据。
- **示例**：
  - 一个用 Go 编写的物联网平台，需要将传感器数据存储到 TDengine 中。
  - 一个用 Go 开发的监控系统，需要从 TDengine 中查询监控数据并展示。

### 9.2 **数据迁移和同步**

- **场景描述**：需要将数据从其他数据库迁移到 TDengine，或者在不同数据库之间同步数据。
- **使用场景**：
  - 使用 Go 编写数据迁移工具，通过 `taosdata/driver-go` 将数据写入 TDengine。
  - 实现实时数据同步。
- **示例**：
  - 将 MySQL 或 PostgreSQL 中的数据迁移到 TDengine。
  - 将 Kafka 中的实时数据写入 TDengine。

### 9.3 **自定义数据处理**

- **场景描述**：需要对 TDengine 中的数据进行自定义处理，例如数据清洗、转换、聚合等。
- **使用场景**：
  - 使用 Go 编写数据处理程序，通过 `taosdata/driver-go` 查询 TDengine 中的数据并进行处理。
- **示例**：
  - 对传感器数据进行清洗和格式化。

## 10. 约束和限制

1. 支持 Go 版本 1.14 及以上版本。
2. 原生连接方式，必须保证 taosc 驱动与 TDengine 版本一致性。
3. 不支持针对单条数据记录的删除操作。
4. 不支持事务操作。

## 11. 常见错误和排查

1. 使用原生连接编译失败
   - **原因**：没有安装 TDengine 客户端或没有允许 cgo。
   - **解决方法**：安装 TDengine 客户端，Go 设置 CGO_ENABLED=1
2. 原生连接 TDengine 失败
   - **原因**：TDengine 没有启动成功或客户端没有设置 FQDN。
   - **解决方法**：确认 TDengine 启动成功，修改客户端 hosts 将 TDengine 集群的每台机器的 fqdn 配置好解析
3. RESTful 连接或 WebSocket 连接失败
   - **原因**：taosAdapter 没有启动或端口没有开放。
   - **解决方法**：确认 taosAdapter 启动成功，taosAdapter 配置端口（默认 6041）客户端可以访问

## 12. 可观测性

支持传递请求 id 的接口可以通过请求 id 进行链路追踪，通过请求 id 可以在后续模块日志进行分析。

## 13. 安装和卸载

编辑 `go.mod` 添加 `driver-go` 依赖即可。
```plaintext
module goexample

require github.com/taosdata/driver-go/v3 v3.7.6
```

## 14. 文档

需要在官方文档中添加章节【TDengine Go Connector】。

## 15. 参考文档

1. [C/C++ 连接器-Function Spec](https://taosdata.feishu.cn/wiki/Hk2Swj9bdipmZCkK0NEcZCKankd) 4. 行为说明
2. [taosAdapter-Function Spec](https://taosdata.feishu.cn/wiki/Xf3zweDQRiFhwNkBSWScVj01nVc) 4. 行为说明

## 16. 附录

无。

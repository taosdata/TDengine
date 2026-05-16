# Go 连接器支持 stmt2

## 1. 背景

[stmt2 功能规格](https://taosdata.feishu.cn/wiki/OHn7wgE38i6LiCkKFQec3wyKnOf)按照以上文档开发 go 连接器的 stmt2 接口

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/09/14 | 0.1 | @谭雪峰 | 编写文档 |
| 2024/09/14 | 0.2 | @谭雪峰 | 删除 GetFields 相关接口，删除 IsInsert 接口 |
| 2024/09/19 | 0.3 | @谭雪峰 | 删除 singleStbInsert 参数 |
|  |  |  |  |

## 3. 定义

stmt2 API: 预处理语句版本 2 API。

## 4. 行为说明

### 4.1 stmt2 接口

```go
func (conn *Connector) Stmt2(reqID int64, singleTableBindOnce bool) *Stmt2
func (s *Stmt2) Prepare(sql string) error
func (s *Stmt2) Bind(params []*stmt.TaosStmt2BindData) error 
func (s *Stmt2) Execute() error
func (s *Stmt2) GetAffectedRows() int
func (s *Stmt2) UseResult() (driver.Rows, error)
func (s *Stmt2) Close() error
```

- `func (conn *Connector) Stmt2(reqID int64, singleTableBindOnce bool) *Stmt2`
  - **接口说明**：从连接创建 stmt2。
  - **参数说明：**
    - `reqID` 请求 ID。
    - `singleTableBindOnce` 单个子表在单次执行中只有一次数据绑定。
  - **返回值**：stmt2 对象。
- `func (s *Stmt2) Prepare(sql string) error`
  - **接口说明**：绑定 sql 语句。
  - **参数说明：**
    - `sql` 要绑定的 sql 语句。
  - **返回值**：错误信息。
- `func (s *Stmt2) Bind(params []*stmt.TaosStmt2BindData) error`
  - **接口说明**：绑定数据。
  - **参数说明：**
    - `params`要绑定的数据。
  - **返回值**：错误信息。
- `func (s *Stmt2) Execute() error`
  - **接口说明**：执行语句。
  - **返回值**：错误信息。
- `func (s *Stmt2) GetAffectedRows() int`
  - **接口说明**：获取受影响行数（只在插入语句有效）。
  - **返回值**：受影响行数。
- `func (s *Stmt2) UseResult() (driver.Rows, error)`
  - **接口说明**：获取结果集（只在查询语句有效）。
  - **返回值**：结果集 Rows 对象，错误信息。
- `func (s *Stmt2) Close() error`
  - **接口说明**：关闭stmt2。
  - **返回值**：错误信息。

### 4.2 sql 限制

由于绑定参数不要求传入类型，因此在 prepare 时要求 sql 语句中必须存在表名，如：
1. 绑定普通表数据
`insert into common_table values(?,?)`
1. 自动建表
`insert into common_table using super_table tags (?,?) values (?,?)`
1. 查询语句
`select * from common_table where ts = ? and v = ?`
**不支持以下语句：**
1. 绑定普通表数据 需要绑定表名
`insert into ? values(?,?)`
1. 自动建表需要绑定表名，目前无法从超级表获取到绑定信息，待引擎支持后可以支持此种情况
`insert into ? using super_table tags(?,?) values(?,?)`

### 4.3 参数绑定说明

`stmt.TaosStmt2BindData` 结构如下
```go
type TaosStmt2BindData struct {
    TableName string
    Tags      []driver.Value   // row format
    Cols      [][]driver.Value // column format
}
```

- TableName 为表名
- Tags 为要绑定的标签信息，一维数组以行的形式组织，只在查询时设置
- Cols 为要绑定的列信息，二维数组，每一为一列数据，查询和写入设置，查询时每列只能绑定一行数据

### 4.4 数据库类型与 Go 类型对应

绑定 Tags 和 Cols 需要数据类型与数据库类型匹配，写入 null 时绑定 nil， 以下为对应关系

| 数据库类型 | Go 类型 |
| --- | --- |
| BOOL | bool |
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
| TIMESTAMP | time.Time |
| BINARY | []byte |
| NCHAR | string/[]byte |
| VARBINARY | []byte |
| GEOMETRY | []byte |
| JSON | []byte |

样例：
```go
params := []*stmt.TaosStmt2BindData{
    {
       Tags: []driver.Value{
          // TIMESTAMP
          now,
          // BOOL
          true,
          // TINYINT
          int8(1),
          // SMALLINT
          int16(1),
          // INT
          int32(1),
          // BIGINT
          int64(1),
          // UTINYINT
          uint8(1),
          // USMALLINT
          uint16(1),
          // UINT
          uint32(1),
          // UBIGINT
          uint64(1),
          // FLOAT
          float32(1.2),
          // DOUBLE
          float64(1.2),
          // BINARY
          []byte("binary"),
          // VARBINARY
          []byte("varbinary"),
          // GEOMETRY
          []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
          // NCHAR
          "nchar",
       },
       Cols: [][]driver.Value{
          {
             // TIMESTAMP
             now,
             now.Add(time.Second),
             now.Add(time.Second * 2),
          },
          {
             // BOOL
             true,
             nil,
             false,
          },
          {
             // TINYINT
             int8(11),
             nil,
             int8(12),
          },
          {
             // SMALLINT
             int16(11),
             nil,
             int16(12),
          },
          {
             // INT
             int32(11),
             nil,
             int32(12),
          },
          {
             // BIGINT
             int64(11),
             nil,
             int64(12),
          },
          {
             // TINYINT UNSIGNED
             uint8(11),
             nil,
             uint8(12),
          },
          {
             // SMALLINT UNSIGNED
             uint16(11),
             nil,
             uint16(12),
          },
          {
             // INT UNSIGNED
             uint32(11),
             nil,
             uint32(12),
          },
          {
             // BIGINT UNSIGNED
             uint64(11),
             nil,
             uint64(12),
          },
          {
             // FLOAT
             float32(11.2),
             nil,
             float32(12.2),
          },
          {
             // DOUBLE
             float64(11.2),
             nil,
             float64(12.2),
          },
          {
             // BINARY
             []byte("binary1"),
             nil,
             []byte("binary2"),
          },
          {
             // VARBINARY
             []byte("varbinary1"),
             nil,
             []byte("varbinary2"),
          },
          {
             // GEOMETRY `point(100 100)`
             []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
             nil,
             []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
          },
          {
             // NCHAR
             "nchar1",
             nil,
             "nchar2",
          },
       },
    },
}
```

## 5. 性能

无，新加接口

## 6. 兼容性

新增 API，不影响原有 API 的兼容性。

## 7. 运维

无。

## 8. 使用场景

主要应用在避免重复解析 SQL 语句的场景

## 9. 约束和限制

见 4.2、4.3 和 4.4 章节

## 10. 常见错误和排查

无

## 11. 可观测性

对 taos shell, taos Explorer， TDinsight 等基于 UI 或用户交互界面的产品组件无影响。

## 12. 安装和卸载

对安装和卸载脚本无要求。

## 13. 文档

不需要修改企业版文档
暂不需要修改官网文档（需要等 API 稳定后再公开到官网文档）

## 14. 参考文档

[stmt2 功能规格](https://taosdata.feishu.cn/wiki/OHn7wgE38i6LiCkKFQec3wyKnOf)
[Python 连接器 Native 支持 stmt2](https://taosdata.feishu.cn/wiki/HHcdwlfTpimKIukGBMWcOS2hnRg)
[参数绑定模块设计总结](https://taosdata.feishu.cn/docx/ULoZdtUsZokmryxoRT2cYHOynye)

## 15. 附录

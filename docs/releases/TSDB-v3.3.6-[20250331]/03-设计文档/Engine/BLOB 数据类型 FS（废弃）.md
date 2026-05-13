# BLOB 数据类型 FS（废弃）

## 1. 背景

BLOB 对象，通常是长度很大的二进制类型的数据，比如超过1MB。在数据库中写入、查询、存储管理大量的 BLOB 对象，会对数据库操作效率等带来全新的挑战，包括内存池、写放大、和读放大等等显著问题。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/03/01 | 0.1 | 赵本光 | 初稿 |
| 2024/03/01 | 0.2 | 赵本光 | 基于 Wade 的意见修改 |
| 2024/03/06 | 1.0 | 赵本光 | 根据评审意见修改 |

## 3. 定义

| 名词 | 解释 |
| --- | --- |
| OID | Object ID |

## 4. 行为说明

### 4.1 建表

支持超级表、普通表的数据列字段定义为 BLOB 类型，通过 COMPRESS 指定压缩方式，以及 LEVEL 指定压缩级别。如果 COMPRESS 未指定时，默认为不压缩。
例如：
```sql
CREATE TABLE t (
    ts TIMESTAMP, 
    data BLOB COMPRESS 'lz4' [LEVEL 'low'],
);
```

**约束****：**
1. TAG 列不能定义为 BLOB 数据类型。
2. BLOB 字段的压缩方式，目前暂只支持 LZ4 和 disabled 两种方式；默认为 disabled。
3. 表中的 BLOB 字段的数目不做限制。
4. 单条 INSERT 语句中，BLOB 字段数据的总长度，设置上限为 2MB。

### 4.2 写入

#### 4.2.1 SQL 写入

支持 SQL 语句方式写入
- 以 "\x 开头的字符串，为十六进制表示的数据，即HEX字符串，如 VALUES (now, "\x393866343633") 。
- 不以 "\x  开头的字符串，表示原始字符串，如 VALUES (now, "98f46e") 
- 其它方式报错
**注意**：
1. SQL语句长度限制，保持 1MB 不变。
2. 支持 BLOB 字段为 NULL。
例如：
```sql
taos> insert into t values(now, "98f463");
Insert OK, 1 row(s) affected (0.002910s)

taos> select * from t;
           ts            |        v         |
=============================================
 2024-01-10 10:18:25.630 | \x393866343633   |
Query OK, 1 row(s) in set (0.003333s)

taos> insert into t values(now, "\x393866343633");
Insert OK, 1 row(s) affected (0.001338s)

taos> select * from t;
           ts            |        v         |
=============================================
 2024-01-10 10:18:25.630 | \x393866343633   |
 2024-01-10 10:19:04.236 | \x393866343633   |
Query OK, 2 row(s) in set (0.005155s)
```

#### 4.2.2 STMT

支持 STMT 方式写入。
在 TAOS_STMT 结构中，BLOB 字段以二进制形式表示。

#### 4.2.3 Schemaless

暂不支持。

#### 4.2.4 文件方式

支持通过文件写入 BLOB 类型数据（在 REST/Websocket 中不支持）。例如
```sql
taos> insert into t values(now, load_file("/path/to/your/file"));
```

**注意**：
1. LOAD_FILE 中路径，需要为绝对路径方式；路径格式，支持操作系统 Linux 和 Windows 两种。
2. 被加载的文件，被看做是二进制字符串格式。
3. 单条语句中，文件总大小超过 BLOB 类型总长度上限时（目前为 2MB）, 直接返回并报错，例如：超过 BLOB 类型长度限制。
4. taosAdapter 通过内部控制参数 tsUseAdapter 关闭文件方式写入 BLOB 类型数据功能。

#### 4.2.5 查询写入

支持 insert into select 方式，把类型 VARCHAR，VARBINARY，BLOB 数据写入 BLOB 字段中，此种写入方式受 SQL 上限为1M的约束。

### 4.3 BLOB 时序存储

BLOB 数据文件，按照时间范围的文件组分组，并且可只追加写。
BLOB 数据文件，包括存储 BLOB 数据的 blobs 文件，及其索引的 idx 文件类型。

### 4.4 查询

#### 4.4.1 SQL

##### 4.4.1.1 投影

查询时，BLOB 字段在shell中显示为以'\x'开头的大写HEX字符串格式，不管原始数据的写入方式是什么。 例如
```sql
taos> select * from t;
           ts            |        v         |
=============================================
 2024-01-10 10:18:25.630 | \x393866343633   |
Query OK, 1 row(s) in set (0.003333s)
```

##### 4.4.1.2 运算符

BLOB 字段支持如下运算符。
**集合运算符**：UNION ALL，（暂不支持 UNION）
**比较运算符**：IS [NOT] NULL

##### 4.4.1.3 函数

BLOB 字段支持如下函数。
**字符串函数**：LENGTH，SUBSTR
**转换函数**：CAST （只支持 BLOB -> VARCHAR, BLOB -> VARBINARY）
**聚合函数**：COUNT
**选择函数**：FIRST，LAST，LAST_ROW，TAIL，SAMPLE，（暂不支持 MODE 和 UNIQUE）
**注意**：
1. BLOB 类型的字符串函数 SUBSTR，把 BLOB 类型当做二进制字符串数据处理，其返回也是 BLOB 类型。
2. CAST 类型转换 BLOB -> VARBINARY 时，如果结果超过 VARBINARY 长度限制时，后面部分将被截断。
3. CAST 类型转换 BLOB -> VARCHAR 时，当遇到'\0'字符或超出 VARCHAR 长度限制时，后面部分将被截断。 

#### 4.4.2 STMT

查询时，BLOB 在 TAOS_FIELD 结构中，以二进制形式表示。
通过函数 taos_print_row 函数输出的 BLOB 字段，显示为以'\x'开头的大写HEX字符串格式。

#### 4.4.3 查询表结构

通过describe查询表结构，可正确显示 BLOB 字段类型。

### 4.5 多副本部署

支持多副本部署。
BLOB 对象在多个副本之间保持一致性，与其它数据类型一样。

### 4.6 BLOB 数据压缩和解压缩

#### 4.6.1 处理阶段

BLOB 数据的压缩和解压缩，均在客户端或查询引擎中完成，存储引擎不进行 BLOB 数据压缩和解压缩处理。
**注意**：BLOB 数据压缩和解压缩，会使用客户端或查询引擎所在节点 CPU 资源。

#### 4.6.2 算法变更

支持 BLOB 字段压缩算法变更。
例如：
```sql
ALTER STABLE stb_name MODIFY COLUMN col_name BLOB [compress 'lz4'];
```

压缩算法变更后，只对后续写入的数据生效；已经写入的 BLOB 数据压缩方式则保持不变。

### 4.7 Compact

时序数据重整，即 compact 操作，不处理 BLOB 数据。这意味，BLOB 对象，如果存在更新、删除等操作，不能通过紧缩过程，释放磁盘空间。

### 4.8 Retention

根据数据库 KEEP 策略，同常规数据相同策略进行删除和移动。

### 4.9 其它功能

1. 订阅支持消费 BLOB 类型数据。
2. taosX 支持写入 BLOB 类型数据。（暂不支持transformer）。
3. taosBenchmark 支持生成 BLOB 类型数据。

## 5. 性能

1. 当 BLOB 数据长度，低于 64K 时；性能相当于 VARCHAR。
2. 当 BLOB 数据长度，不低于 64K 时，性能指标以测试结果为准。

## 6. 兼容性

1. 支持版本升级。
2. 当创建含  BLOB 字段的普通表或超级表后，不支持版本回退。

## 7. 运维

暂无

## 8. 使用场景

与提供 CDN 等网站访问的 BLOB 存储系统访问方式不同，BLOB 时序存储，面向用户需要按照事件发生的时间顺序写入，以及按照时间范围进行过滤批量查询读取的使用场景。这些应用场景，包括与事件发生时间强相关而采集的图片、音频、视频、以及高频传感器采集数据等高速存储，按照时间范围过滤查询高速读取等场景，例如车联网等业务场景。
在适用场景下，高速的存取性能，是 BLOB 时序存储系统相对于竞争方案而言，提供的核心价值。

## 9. 约束和限制

1. 当 BLOB 数据存在更新、删除时，BLOB 数据的磁盘占用，暂不能通过 Compact 方式释放存储空间。
2. BLOB 字 段大小上限，暂定 20 MB。

## 10. 常见错误和排查

暂无

## 11. 参考文档

- [BLOB时序存储：文件及格式概要](https://taosdata.feishu.cn/wiki/NC5pw3cVhizpTVkAQAscKm2enoe)
- [BLOB存储调研](https://taosdata.feishu.cn/wiki/Ij9owXeTXiRRi4kX0Rockvq4npw)
- [需求说明：BLOB](https://taosdata.feishu.cn/wiki/Kv3KwukozixkIZk5wT8ctiTlnbf)
- [TDengine 压缩增强](https://taosdata.feishu.cn/wiki/St4WwSX5Ei3VfMk3yMUcv2DMnMh)
- [VARBINARY 数据类型](https://taosdata.feishu.cn/wiki/WWJkwPD6LiKKTxkfihjcclXjnSe)

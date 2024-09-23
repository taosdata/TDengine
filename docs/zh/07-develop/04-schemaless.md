---
title: 无模式写入
sidebar_label: 无模式写入
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

在物联网应用中，为了实现自动化管理、业务分析和设备监控等多种功能，通常需要采集大量的数据项。然而，由于应用逻辑的版本升级和设备自身的硬件调整等原因，数据采集项可能会频繁发生变化。为了应对这种挑战，TDengine 提供了无模式（schemaless）写入方式，旨在简化数据记录过程。

采用无模式写入方式，用户无须预先创建超级表或子表，因为 TDengine 会根据实际写入的数据自动创建相应的存储结构。此外，在必要时，无模式写入方式还能自动添加必要的数据列或标签列，确保用户写入的数据能够被正确存储。

值得注意的是，通过无模式写入方式创建的超级表及其对应的子表与通过 SQL 直接创建的超级表和子表在功能上没有区别，用户仍然可以使用 SQL 直接向其中写入数据。然而，由于无模式写入方式生成的表名是基于标签值按照固定的映射规则生成的，因此这些表名可能缺乏可读性，不易于理解。

**采用无模式写入方式时会自动创建表，无须手动创建表。**

## 无模式写入行协议 

TDengine 的无模式写入行协议兼容 InfluxDB 的行协议、OpenTSDB 的 telnet 行协议和 OpenTSDB 的 JSON 格式协议。InfluxDB、OpenTSDB 的标准写入协议请参考各自的官方文档。

下面首先以 InfluxDB 的行协议为基础，介绍 TDengine 扩展的协议内容。该协议允许用户采用更加精细的方式控制（超级表）模式。采用一个字符串来表达一个数据行，可以向写入 API 中一次传入多行字符串来实现多个数据行的批量写入，其格式约定如下。

```text
measurement,tag_set field_set timestamp
```

各参数说明如下。
- measurement 为数据表名，与 tag_set 之间使用一个英文逗号来分隔。
- tag_set 格式形如 `<tag_key>=<tag_value>, <tag_key>=<tag_value>`，表示标签列数据，使用英文逗号分隔，与 field_set 之间使用一个半角空格分隔。
- field_set 格式形如 `<field_key>=<field_value>, <field_key>=<field_value>`，表示普通列，同样使用英文逗号来分隔，与 timestamp 之间使用一个半角空格分隔。
- timestamp 为本行数据对应的主键时间戳。

tag_set 中的所有的数据自动转化为 nchar 数据类型，并不需要使用双引号。
在无模式写入数据行协议中，field_set 中的每个数据项都需要对自身的数据类型进行描述，具体要求如下。
- 如果两边有英文双引号，表示 varchar 类型，例如 "abc"。
- 如果两边有英文双引号而且带有 L 或 l 前缀，表示 nchar 类型，例如 L" 报错信息 "。
- 如果两边有英文双引号而且带有 G 或 g 前缀， 表 示 geometry 类型， 例 如G"Point(4.343 89.342)"。
- 如果两边有英文双引号而且带有 B 或 b 前缀，表示 varbinary 类型，双引号内可以为 \x 开头的十六进制或者字符串，例如 B"\x98f46e" 和 B"hello"。
- 对于空格、等号（=）、逗号（,）、双引号（"）、反斜杠（\），前面需要使用反斜杠（\）进行转义（均为英文半角符号）。无模式写入协议的域转义规则如下表所示。

| **序号** | **域**   | **需转义字符**   |
| -------- | -------- | ---------------- |
| 1        | 超级表名 | 逗号，空格       |
| 2        | 标签名   | 逗号，等号，空格 |
| 3        | 标签值   | 逗号，等号，空格 |
| 4        | 列名     | 逗号，等号，空格 |
| 5        | 列值     | 双引号，反斜杠   |

如果使用两个连续的反斜杠，则第1个反斜杠作为转义符，当只有一个反斜杠时则无须转义。无模式写入协议的反斜杠转义规则如下表所示。

| **序号** | **反斜杠**   | **转义为** |
| -------- | ------------ | ---------- |
| 1        | \            | \          |
| 2        | \\\\         | \          |
| 3        | \\\\\\       | \\\\       |
| 4        | \\\\\\\\     | \\\\       |
| 5        | \\\\\\\\\\   | \\\\\\     |
| 6        | \\\\\\\\\\\\ | \\\\\\     |

数值类型将通过后缀来区分数据类型。无模式写入协议的数值类型转义规则如下表所示。

| **序号** | **后缀**    | **映射类型**                  | **大小(字节)** |
| -------- | ----------- | ----------------------------- | -------------- |
| 1        | 无或 f64    | double                        | 8              |
| 2        | f32         | float                         | 4              |
| 3        | i8/u8       | TinyInt/UTinyInt              | 1              |
| 4        | i16/u16     | SmallInt/USmallInt            | 2              |
| 5        | i32/u32     | Int/UInt                      | 4              |
| 6        | i64/i/u64/u | BigInt/BigInt/UBigInt/UBigInt | 8              |

- t, T, true, True, TRUE, f, F, false, False 将直接作为 BOOL 型来处理。

例如如下数据行表示：向名为 st 的超级表下的 t1 标签为 "3"（NCHAR）、t2 标签为 "4"（NCHAR）、t3
标签为 "t3"（NCHAR）的数据子表，写入 c1 列为 3（BIGINT）、c2 列为 false（BOOL）、c3
列为 "passit"（BINARY）、c4 列为 4（DOUBLE）、主键时间戳为 1626006833639000000 的一行数据。

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

需要注意的是，如果描述数据类型后缀时出现大小写错误，或者为数据指定的数据类型有误，均可能引发报错提示而导致数据写入失败。

TDengine提供数据写入的幂等性保证，即用户可以反复调用API进行出错数据的写入操作。无模式写入TDengine的主要处理逻辑请参考TDengine的官方网站，此处不赘述。

## 时间分辨率识别

无模式写入支持3个指定的模式，如下表所示：

| **序号** | **值**              | **说明**                        |
| -------- | ------------------- | ------------------------------- |
| 1        | SML_LINE_PROTOCOL   | InfluxDB 行协议（Line Protocol) |
| 2        | SML_TELNET_PROTOCOL | OpenTSDB 文本行协议             |
| 3        | SML_JSON_PROTOCOL   | JSON 协议格式                   |

在 SML_LINE_PROTOCOL 解析模式下，需要用户指定输入的时间戳的时间分辨率。可用的时间分辨率如下表所示：

| **序号** | **时间分辨率定义**                | **含义**       |
| -------- | --------------------------------- | -------------- |
| 1        | TSDB_SML_TIMESTAMP_NOT_CONFIGURED | 未定义（无效） |
| 2        | TSDB_SML_TIMESTAMP_HOURS          | 小时           |
| 3        | TSDB_SML_TIMESTAMP_MINUTES        | 分钟           |
| 4        | TSDB_SML_TIMESTAMP_SECONDS        | 秒             |
| 5        | TSDB_SML_TIMESTAMP_MILLI_SECONDS  | 毫秒           |
| 6        | TSDB_SML_TIMESTAMP_MICRO_SECONDS  | 微秒           |
| 7        | TSDB_SML_TIMESTAMP_NANO_SECONDS   | 纳秒           |

在 SML_TELNET_PROTOCOL 和 SML_JSON_PROTOCOL 模式下，根据时间戳的长度来确定时间精度（与 OpenTSDB 标准操作方式相同），此时会忽略用户指定的时间分辨率。

## 数据模式映射规则

InﬂuxDB行协议的数据将被映射成具有模式的数据，其中，measurement映射为超级表名称，tag_set中的标签名称映射为数据模式中的标签名，field_set中的名称映射为列名称。例如下面的数据。

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

该行数据映射生成一个超级表： st， 其包含了 3 个类型为 nchar 的标签，分别是：t1, t2, t3。五个数据列，分别是 ts（timestamp），c1 (bigint），c3(binary)，c2 (bool), c4 (bigint）。映射成为如下 SQL 语句：

```json
create stable st (_ts timestamp, c1 bigint, c2 bool, c3 binary(6), c4 bigint) tags(t1 nchar(1), t2 nchar(1), t3 nchar(2))
```

## 数据模式变更处理

本节将说明不同行数据写入情况下，对于数据模式的影响。

在使用行协议写入一个明确的标识的字段类型的时候，后续更改该字段的类型定义，会出现明确的数据模式错误，即会触发写入 API 报告错误。如下所示，

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4    1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4i   1626006833640000000
```

第一行的数据类型映射将 c4 列定义为 Double， 但是第二行的数据又通过数值后缀方式声明该列为 BigInt， 由此会触发无模式写入的解析错误。

如果列前面的行协议将数据列声明为了 binary， 后续的要求长度更长的 binary 长度，此时会触发超级表模式的变更。

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c5="pass"     1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c5="passit"   1626006833640000000
```

第一行中行协议解析会声明 c5 列是一个 binary(4)的字段，第二次行数据写入会提取列 c5 仍然是 binary 列，但是其宽度为 6，此时需要将 binary 的宽度增加到能够容纳 新字符串的宽度。

```json
st,t1=3,t2=4,t3=t3 c1=3i64               1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c6="passit"   1626006833640000000
```

第二行数据相对于第一行来说增加了一个列 c6，类型为 binary(6)。那么此时会自动增加一个列 c6， 类型为 binary(6)。

## 无模式写入示例
下面以智能电表为例，介绍各语言连接器使用无模式写入接口写入数据的代码样例，包含了三种协议： InfluxDB 的行协议、OpenTSDB 的 TELNET 行协议和 OpenTSDB 的 JSON 格式协议。  

:::note
- 因为无模式写入自动建表规则与之前执行 SQL 样例中不同，因此运行代码样例前请确保 `meters`、`metric_telnet` 和 `metric_json` 表不存在。 
- OpenTSDB 的 TELNET 行协议和 OpenTSDB 的 JSON 格式协议只支持一个数据列，因此我们采用了其他示例。   

:::

### Websocket 连接

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/SchemalessWsTest.java:schemaless}}
```


执行带有 reqId 的无模式写入，最后一个参数 reqId 可用于请求链路追踪。

```java
writer.write(lineDemo, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS, 1L);
```

</TabItem>
<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/schemaless_ws.py}}
```
</TabItem>
<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/schemaless/ws/main.go}}
```
</TabItem>
<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/restexample/examples/schemaless.rs}}
```

</TabItem>
<TabItem label="Node.js" value="node">
```js
{{#include docs/examples/node/websocketexample/line_example.js}}
```
</TabItem>
<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/wssml/Program.cs:main}}
```
</TabItem>
<TabItem label="C" value="c">
不支持
</TabItem>
<TabItem label="REST API" value="rest">
不支持
</TabItem>   
</Tabs>

### 原生连接
<Tabs defaultValue="java" groupId="lang">
    <TabItem label="Java" value="java">
```java
{{#include docs/examples/java/src/main/java/com/taos/example/SchemalessJniTest.java:schemaless}}
```

执行带有 reqId 的无模式写入，最后一个参数 reqId 可用于请求链路追踪。

```java
writer.write(lineDemo, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS, 1L);
```

</TabItem>
<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/schemaless_native.py}}
```
</TabItem>
<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/schemaless/native/main.go}}
```
</TabItem>
<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/nativeexample/examples/schemaless.rs}}
```

</TabItem>
<TabItem label="Node.js" value="node">
不支持
</TabItem>
<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/nativesml/Program.cs:main}}
```
</TabItem>
<TabItem label="C" value="c">
```c
{{#include docs/examples/c/sml_insert_demo.c:schemaless}}
```
</TabItem>
<TabItem label="REST API" value="rest">
不支持
</TabItem>   
</Tabs>

## 查询写入的数据

运行上节的样例代码，会在 power 数据库中自动建表，我们可以通过 taos shell 或者应用程序来查询数据。下面给出用 taos shell 查询超级表和 meters 表数据的样例。

```shell
taos> show power.stables;
          stable_name           |
=================================
 meter_current                  |
 stb0_0                         |
 meters                         |
Query OK, 3 row(s) in set (0.002527s)



taos> select * from power.meters limit 1 \G;
*************************** 1.row ***************************
     _ts: 2021-07-11 20:33:53.639
 current: 10.300000199999999
 voltage: 219
   phase: 0.310000000000000
 groupid: 2
location: California.SanFrancisco
Query OK, 1 row(s) in set (0.004501s)
```

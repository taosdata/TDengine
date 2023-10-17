---
sidebar_label: InfluxDB 行协议
title: InfluxDB 行协议
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import JavaLine from "./_java_line.mdx";
import PyLine from "./_py_line.mdx";
import GoLine from "./_go_line.mdx";
import RustLine from "./_rust_line.mdx";
import NodeLine from "./_js_line.mdx";
import CsLine from "./_cs_line.mdx";
import CLine from "./_c_line.mdx";

## 协议介绍

InfluxDB Line 协议采用一行字符串来表示一行数据。分为四部分：

```
measurement,tag_set field_set timestamp
```

- measurement 将作为超级表名。它与 tag_set 之间使用一个英文逗号来分隔。
- tag_set 将作为标签数据，其格式形如 `<tag_key>=<tag_value>,<tag_key>=<tag_value>`，也即可以使用英文逗号来分隔多个标签数据。它与 field_set 之间使用一个半角空格来分隔。
- field_set 将作为普通列数据，其格式形如 `<field_key>=<field_value>,<field_key>=<field_value>`，同样是使用英文逗号来分隔多个普通列的数据。它与 timestamp 之间使用一个半角空格来分隔。
- timestamp 即本行数据对应的主键时间戳。

例如：

```
meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611249500
```

:::note

- tag_set 中的所有的数据自动转化为 NCHAR 数据类型
- field_set 中的每个数据项都需要对自身的数据类型进行描述, 比如 1.2f32 代表 FLOAT 类型的数值 1.2, 如果不带类型后缀会被当作 DOUBLE 处理
- timestamp 支持多种时间精度。写入数据的时候需要用参数指定时间精度，支持从小时到纳秒的 6 种时间精度
- 为了提高写入的效率，默认假设同一个超级表中 field_set 的顺序是一样的（第一条数据包含所有的 field，后面的数据按照这个顺序），如果顺序不一样，需要配置参数 smlDataFormat 为 false，否则，数据写入按照相同顺序写入，库中数据会异常。（3.0.1.3 之后的版本 smlDataFormat 默认为 false，从3.0.3.0开始，该配置废弃） [TDengine 无模式写入参考指南](/reference/schemaless/#无模式写入行协议)
- 子表名生成规则
  - 默认产生的子表名是根据规则生成的唯一 ID 值。
  - 用户也可以通过在client端的 taos.cfg 里配置 smlAutoChildTableNameDelimiter 参数来指定连接标签之间的分隔符，连接起来后作为子表名。举例如下：配置 smlAutoChildTableNameDelimiter=-, 插入数据为 st,t0=cpu1,t1=4 c1=3 1626006833639000000 则创建的子表名为 cpu1-4。
  - 用户也可以通过在client端的 taos.cfg 里配置 smlChildTableName 参数来指定某个标签值作为子表名。该标签值应该具有全局唯一性。举例如下：假设有个标签名为tname, 配置 smlChildTableName=tname, 插入数据为 st,tname=cpu1,t1=4 c1=3 1626006833639000000 则创建的子表名为 cpu1。注意如果多行数据 tname 相同，但是后面的 tag_set 不同，则使用第一行自动建表时指定的 tag_set，其他的行会忽略）。[TDengine 无模式写入参考指南](/reference/schemaless/#无模式写入行协议)

:::

要了解更多可参考：[InfluxDB Line 协议官方文档](https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/) 和 [TDengine 无模式写入参考指南](/reference/schemaless/#无模式写入行协议)

## 示例代码

<Tabs defaultValue="java" groupId="lang">
  <TabItem label="Java" value="java">
    <JavaLine />
  </TabItem>
  <TabItem label="Python" value="Python">
    <PyLine />
  </TabItem>
  <TabItem label="Go" value="go">
    <GoLine />
  </TabItem>
  <TabItem label="Node.js" value="nodejs">
    <NodeLine />
  </TabItem>
  <TabItem label="C#" value="csharp">
    <CsLine />
  </TabItem>
  <TabItem label="C" value="c">
    <CLine />
  </TabItem>
</Tabs>

## SQL 查询示例

`meters` 是插入数据的超级表名。

可以通过超级表的 TAG 来过滤数据，比如查询 `location=California.LosAngeles,groupid=2` 可以通过如下 SQL：

```sql
SELECT * FROM meters WHERE location = "California.LosAngeles" AND groupid = 2;
```

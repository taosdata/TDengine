---
title: 执行 SQL
sidebar_label: 执行 SQL
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

上一节我们介绍了如何建立连接，本节以 WebSocket 连接为例，使用各种语言连接器执行 SQL 完成写入。

## 建库和表

<Tabs defaultValue="java" groupId="create">
<TabItem value="java" label="Java">

```java
// create statement
Statement stmt = conn.createStatement();
// create database
stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS power");
// use database
stmt.executeUpdate("USE power");
// create table
stmt.executeUpdate("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
```
</TabItem>
</Tabs>

## 插入数据

<Tabs defaultValue="java" groupId="insert">
<TabItem value="java" label="Java">

```java
// insert data
String insertQuery = "INSERT INTO " +
    "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
    "VALUES " +
    "(NOW + 1a, 10.30000, 219, 0.31000) " +
    "(NOW + 2a, 12.60000, 218, 0.33000) " +
    "(NOW + 3a, 12.30000, 221, 0.31000) " +
    "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
    "VALUES " +
    "(NOW + 1a, 10.30000, 218, 0.25000) ";
int affectedRows = stmt.executeUpdate(insertQuery);
System.out.println("insert " + affectedRows + " rows.");
```
</TabItem>
</Tabs>

**Note**
NOW 为系统内部函数，默认为客户端所在计算机当前时间。 NOW + 1s 代表客户端当前时间往后加 1 秒，数字后面代表时间单位：a（毫秒），s（秒），m（分），h（小时），d（天），w（周），n（月），y（年）。

## 查询数据

<Tabs defaultValue="java" groupId="query">
<TabItem value="java" label="Java">

```java
// query data
ResultSet resultSet = stmt.executeQuery("SELECT * FROM meters");

Timestamp ts;
float current;
String location;
while(resultSet.next()) {
    ts = resultSet.getTimestamp(1);
    current = resultSet.getFloat(2);
    location = resultSet.getString("location");
    System.out.printf("%s, %f, %s\n", ts, current, location);
}
```
</TabItem>
</Tabs>

**Note** 查询和操作关系型数据库一致，使用下标获取返回字段内容时从 1 开始，建议使用字段名称获取。

## 执行带有 reqId 的 SQL

reqId 可用于请求链路追踪，reqId 就像分布式系统中的 traceId 作用一样。一个请求可能需要经过多个服务或者模块才能完成。reqId 用于标识和关联这个请求的所有相关操作，以便于我们可以追踪和分析请求的完整路径。

使用 reqId 有下面好处：
- 请求追踪：通过将同一个 reqId 关联到一个请求的所有相关操作，可以追踪请求在系统中的完整路径
- 性能分析：通过分析一个请求的 reqId，可以了解请求在各个服务和模块中的处理时间，从而找出性能瓶颈
- 故障诊断：当一个请求失败时，可以通过查看与该请求关联的 reqId 来找出问题发生的位置

如果用户不设置 reqId，连接器会在内部随机生成一个，但建议由显式用户设置以以更好地跟用户请求关联起来。

<Tabs defaultValue="java" groupId="query">
<TabItem value="java" label="Java">

```java
AbstractStatement aStmt = (AbstractStatement) connection.createStatement();
aStmt.execute("CREATE DATABASE IF NOT EXISTS power", 1L);
aStmt.executeUpdate("USE power", 2L);
try (ResultSet rs = aStmt.executeQuery("SELECT * FROM meters limit 1", 3L)) {
    while(rs.next()){
        Timestamp timestamp = rs.getTimestamp(1);
        System.out.println("timestamp = " + timestamp);
    }
}
aStmt.close();
```
</TabItem>
</Tabs>
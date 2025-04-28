---
sidebar_label: Spark
title: 与 Spark 集成
---

Apache Spark 是开源的大规模数据处理引擎。它基于内存计算，速度快。支持 Java、Scala、Python 和 R 等语言，提供丰富 API。其核心组件包括 Spark Core、Spark SQL、Spark Streaming、MLlib 和 GraphX，可用于批处理、流处理、机器学习、图计算等多种场景，能与 Hadoop、Kafka 等多种大数据技术集成，应用广泛。

通过使用 `TDengine Java connector` 连接器，Spark 可以快速访问 TDengine 数据, 实现数据写入、查询及分析等功能。

## 前置条件 

准备以下环境：

- TDengine 3.3.6.0 以上版本集群已部署并正常运行（企业及社区版均可）。
- taosAdapter 能够正常运行，详细参考 [taosAdapter 参考手册](../../../reference/components/taosadapter)。
- Spark 安装（如未安装，请下载并安装 [Spark 下载](https://spark.apache.org/downloads.html)）。
- 安装 JDBC 驱动。从 `maven.org` 下载 `TDengine JDBC` 连接器文件 `taos-jdbcdriver-3.6.2-dist.jar` 或以上版本。

## 配置数据源

下面以 JAVA 语言编写任务程序，通过 `spark-submit` 向集群提交任务为例，介绍连接过程：

**第 1 步**，注册 TDengine 语法方言， 详见示例 registerDialect()。

**第 2 步**，创建 Spark 通信会话实例，详见示例 createSpark（）。

**第 3 步**，建立 JDBC WebSocket 连接，创建数据库及示例表，并写入示例数据，详见示例 prepareDemoData()。

**第 4 步**，验证从数据源正确获取数据，详见 readTable()。


## 数据分析

### 数据准备

以上节 `配置数据源` 中写入的智能电表数据为样例

### 分析电压周变化率
我们选择了一个无法在 TDengine 中完成，只能通过 spark 完成的数据分析示例，意在展示对接 spark 是对 TDengine 能力的扩展。
LAG() 函数是 Spark 支持的获取当前行前指定行数据，此函数在 TDengine 上不支持，示例使用此函数分析电压每周变化率。
**第 1 步**，通过 TDengine SQL 获取数据并创建 Spark View, 详见 createSparkView()
``` sql
select tbname,* from test.meters where tbname='d0'
```

**第 2 步**，从创建出的 Spark View 上使用 LAG 函数计算电压周变化率，查询 SQL 如下：
``` sql
SELECT tbname, ts, voltage, 
      (LAG(voltage, 7) OVER (ORDER BY tbname)) AS voltage_last_week, 
      CONCAT(ROUND(((voltage - voltage_last_week) / voltage_last_week * 100), 1),'%') AS weekly_growth_rate 
      FROM viewMeters
```

**第 3 步**，输出分析结果，如图：
![spark-result](./spark-result.png)

## 示例源码
示例为 JAVA 语言编写，编译运行参考示例源码目录下 README。
 (示例源码)(https://github.com/taosdata/tdengine-eco)


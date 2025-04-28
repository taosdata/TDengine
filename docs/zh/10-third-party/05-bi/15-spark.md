---
sidebar_label: Spark
title: 与 Spark 集成
---

Apache Spark 是开源大数据处理引擎，它基于内存计算，核心组件包括 Spark Core、Spark SQL、Spark Streaming、MLlib 和 GraphX，可用于批、流处理、机器学习、图计算等多种场景，支持 MapReduce 计算模型及丰富计算操作符、函数等，在大超大规模数据上具有强大的分布式处理计算能力。

通过 `TDengine Java connector` 连接器，Spark 可快速获取 TDengine 数据， 利用 Spark 强大引擎，扩展 TDengine 数据处理计算能力。

## 前置条件 

准备以下环境：

- TDengine 3.3.6.0 及以上版本集群已部署并正常运行（企业及社区版均可）。
- taosAdapter 能够正常运行，详细参考 [taosAdapter 参考手册](../../../reference/components/taosadapter)。
- Spark 3.4.4 及以上版本（ [Spark 下载](https://spark.apache.org/downloads.html)）。
- 安装 JDBC 驱动。从 `maven.org` 下载 `TDengine JDBC` 连接器文件 `taos-jdbcdriver-3.6.2-dist.jar` 或以上版本。

## 配置数据源

下面以 JAVA 语言编写 Spark 任务，通过 `spark-submit` 提交任务执行为例，介绍对接过程，后面附完整示例代码。

**第 1 步**，注册 TDengine 语法方言， 详见示例 registerDialect()。

**第 2 步**，创建 Spark 会话实例，详见示例 createSpark（）。

**第 3 步**，建立 JDBC WebSocket 连接，创建演示数据库及表，并写入演示数据，详见示例 prepareDemoData()。

**第 4 步**，验证从数据源可正确获取数据，详见示例 readTable()。


## 数据分析

### 数据准备

以上节 `配置数据源` 中写入智能电表数据为样例

### 分析电压周变化率
我们选择展示了一个在 TDengine 中不支持，但 spark 能够支持的数据处理，意在表明对接 spark 后对 TDengine 能力的扩展。
LAG() 函数是 Spark 支持获取当前行之前某行数据，此函数在 TDengine 上并不支持，示例使用此函数进行了电压周变化率的分析。

**第 1 步**，通过 TDengine SQL 获取数据并创建 Spark View, 详见 createSparkView()
``` sql
select tbname,* from test.meters where tbname='d0'
```

**第 2 步**，从创建出的 Spark View 上使用 LAG 函数计算电压周变化率，查询 SQL 如下：
``` sql
SELECT tbname, ts, voltage,
      (LAG(voltage, 7) OVER (ORDER BY tbname)) AS voltage_last_week, 
      CONCAT(ROUND(((voltage - voltage_last_week)/voltage_last_week * 100), 1),'%') AS weekly_growth_rate
      FROM viewMeters
```

**第 3 步**，输出分析结果，如图：

![spark-result](./spark-result.png)

## 示例源码
示例为 JAVA 语言编写，编译运行参考示例源码目录下 README。

[完整示例源码](https://github.com/taosdata/tdengine-eco/tree/main/spark)

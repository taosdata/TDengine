---
sidebar_label: Spark
title: 与 Spark 集成
toc_max_heading_level: 5
---

[Apache Spark](https://spark.apache.org/) 是开源大数据处理引擎，它基于内存计算，可用于批、流处理、机器学习、图计算等多种场景，支持 MapReduce 计算模型及丰富计算操作符、函数等，在大超大规模数据上具有强大的分布式处理计算能力。

通过 `TDengine Java connector` 连接器，Spark 可快速读取 TDengine 数据， 利用 Spark 强大引擎，扩展 TDengine 数据处理计算能力，同时通过 `TDengine Java connector` 连接器， Spark 亦可把数据写入 TDengine 及从 TDengine 订阅数据。

## 前置条件 

准备以下环境：

- TDengine 3.3.6.0 及以上版本集群已部署并正常运行（企业及社区版均可）。
- taosAdapter 能够正常运行，详细参考 [taosAdapter 参考手册](../../../reference/components/taosadapter)。
- Spark 3.4.0 及以上版本（ [Spark 下载](https://spark.apache.org/downloads.html)）。
- 安装 JDBC 驱动。从 `maven.org` 下载 `TDengine JDBC` 连接器文件 `taos-jdbcdriver-3.6.2-dist.jar` 或以上版本。

## 配置数据源
使用 JDBC WebSocket 连接至 TDengine 数据源，连接 URL 格式为：
``` sql
jdbc:TAOS-WS://[host_name]:[port]/[database_name]?[user={user}|&password={password}]
```
详细参数介绍见：[URL 参数介绍](../../../reference/connector/java/#url-规范)
driverClass 指定为“com.taosdata.jdbc.ws.WebSocketDriver”

以下示例创建 Spark 实例并连接到本机 TDengine 服务：
``` java
    // create spark instance
    SparkSession spark = SparkSession.builder()
		.appName("appSparkTest")
		.master("local[*]")
		.getOrCreate();
  
    // connect TDengine and create reader
    String url      = "jdbc:TAOS-WS://localhost:6041/test?user=root&password=taosdata";
    String driver   = "com.taosdata.jdbc.ws.WebSocketDriver";
    DataFrameReader dataFrameReader = spark.read()
               .format("jdbc")
               .option("url", url)
               .option("driver", driver);

```


## 数据接入
数据接入需注册 TDengine 方言，方言中主要处理反引号，数据类型映射与 JDBC 相同, 无需额外处理，参见：[JDBC 数据类型映射](../../../reference/connector/java/#数据类型映射)

下面以 JAVA 语言编写 Spark 任务，通过 `spark-submit` 提交任务执行为例，介绍数据接入，后附完整示例代码。

### 数据写入
数据写入使用参数绑定完成，核心代码如下，详细参见示例： writeToTDengine
``` java
  String sql = String.format("INSERT INTO test.d%d using test.meters tags(%d,'location%d') VALUES (?,?,?,?) ", i, i, i);
  PreparedStatement preparedStatement = connection.prepareStatement(sql);
```

### 数据读取
数据读取通过表映射方式读取，详细参见示例： readFromTDengine
``` java
  Dataset<Row> df = reader.option("dbtable", dbtable).load();
  df.show();
```

### 数据订阅
数据订阅使用 JDBC 标准数据订阅方法，详细参见示例： subscribeFromTDengine
``` java
   List<String> topics = Collections.singletonList("topic_meters");
   consumer.subscribe(topics);
```


## 数据分析

### 数据准备

以智能电表数据为样例，生成一个超级表，两个子表，每个子表写入 20 条数据，电压数据生成在 100 ~ 120 范围内随机数

### 分析电压周变化率
LAG() 函数是 Spark 提供获取当前行之前某行数据的函数，示例使用此函数进行电压周变化率分析。

**第 1 步**，通过 TDengine SQL 获取数据并创建 Spark View, 详见 createSparkView()。
``` sql
select tbname,* from test.meters where tbname='d0'
```

**第 2 步**，使用 Spark SQL 查询 Spark View 数据, 计算电压周变化率，SQL 如下：
``` sql
SELECT tbname, ts, voltage,
      (LAG(voltage, 7) OVER (ORDER BY tbname)) AS voltage_last_week, 
      CONCAT(ROUND(((voltage - voltage_last_week)/voltage_last_week * 100), 1),'%') AS weekly_growth_rate
      FROM sparkMeters
```

**第 3 步**，输出分析结果，如图：

![spark-result](./spark-result.png)


### 更多场景
 Spark 接 TDengine 数据源后，可扩展 TDengine 更多数据处理能力：

- 跨数据库数据分析

- 数据集交集、并集、差集运算

- Where 带子查询进行过滤

- 支持普通列 JOIN 操作等等


## 示例源码
示例为 JAVA 语言编写，编译运行参考示例源码目录下 README。    
[完整示例源码](https://github.com/taosdata/tdengine-eco/tree/main/spark)

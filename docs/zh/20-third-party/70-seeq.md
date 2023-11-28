---
sidebar_label: Seeq
title: Seeq
description: 如何使用 Seeq 和 TDengine 进行时序数据分析
---

# 如何使用 Seeq 和 TDengine 进行时序数据分析

## 方案介绍

Seeq 是制造业和工业互联网（IIOT）高级分析软件。Seeq 支持在工艺制造组织中使用机器学习创新的新功能。这些功能使组织能够将自己或第三方机器学习算法部署到前线流程工程师和主题专家使用的高级分析应用程序，从而使单个数据科学家的努力扩展到许多前线员工。

通过 TDengine Java connector， Seeq 可以轻松支持查询 TDengine 提供的时序数据，并提供数据展现、分析、预测等功能。

### Seeq 安装方法

从 [Seeq 官网](https://www.seeq.com/customer-download)下载相关软件，例如 Seeq Server 和 Seeq Data Lab 等。Seeq Data Lab 需要安装在和 Seeq Server 不同的服务器上，并通过配置和 Seeq Server 互联。详细安装配置指令参见[Seeq 知识库]( https://support.seeq.com/kb/latest/cloud/)。

## TDengine 本地实例安装方法

请参考[官网文档](../../get-started)。 

## TDengine Cloud 访问方法
如果使用 Seeq 连接 TDengine Cloud，请在 https://cloud.taosdata.com 申请帐号并登录查看如何访问 TDengine Cloud。

## 如何配置 Seeq 访问 TDengine

1. 查看 data 存储位置

```
sudo seeq config get Folders/Data
```

2. 从 maven.org 下载 TDengine Java connector 包，目前最新版本为[3.2.5](https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/3.2.5/taos-jdbcdriver-3.2.5-dist.jar)，并拷贝至 data 存储位置的 plugins\lib 中。

3. 重新启动 seeq server

```
sudo seeq restart
```

4. 输入 License

使用浏览器访问 ip:34216 并按照说明输入 license。

## 使用 Seeq 分析 TDengine 时序数据

本章节演示如何使用 Seeq 软件配合 TDengine 进行时序数据分析。

### 场景介绍

示例场景为一个电力系统，用户每天从电站仪表收集用电量数据，并将其存储在 TDengine 集群中。现在用户想要预测电力消耗将会如何发展，并购买更多设备来支持它。用户电力消耗随着每月订单变化而不同，另外考虑到季节变化，电力消耗量会有所不同。这个城市位于北半球，所以在夏天会使用更多的电力。我们模拟数据来反映这些假定。

### 数据 Schema

```
CREATE STABLE meters (ts TIMESTAMP, num INT, temperature FLOAT, goods INT) TAGS (device NCHAR(20));
CREATE TABLE goods (ts1 TIMESTAMP, ts2 TIMESTAMP, goods FLOAT);
```

![Seeq demo schema](./seeq/seeq-demo-schema.webp)

### 构造数据方法

```
python mockdata.py
taos -s "insert into power.goods select _wstart, _wstart + 10d, avg(goods) from power.meters interval(10d);"
```

源代码托管在[GitHub 仓库](https://github.com/sangshuduo/td-forecasting)。

### 使用 Seeq 进行数据分析

#### 配置数据源（Data Source）

使用 Seeq 管理员角色的帐号登录，并新建数据源。

- Power

```
{
    "QueryDefinitions": [
        {
            "Name": "PowerNum",
            "Type": "SIGNAL",
            "Sql": "SELECT  ts, num FROM meters",
            "Enabled": true,
            "TestMode": false,
            "TestQueriesDuringSync": true,
            "InProgressCapsulesEnabled": false,
            "Variables": null,
            "Properties": [
                {
                    "Name": "Name",
                    "Value": "Num",
                    "Sql": null,
                    "Uom": "string"
                },
                {
                    "Name": "Interpolation Method",
                    "Value": "linear",
                    "Sql": null,
                    "Uom": "string"
                },
                {
                    "Name": "Maximum Interpolation",
                    "Value": "2day",
                    "Sql": null,
                    "Uom": "string"
                }
            ],
            "CapsuleProperties": null
        }
    ],
    "Type": "GENERIC",
    "Hostname": null,
    "Port": 0,
    "DatabaseName": null,
    "Username": "root",
    "Password": "taosdata",
    "InitialSql": null,
    "TimeZone": null,
    "PrintRows": false,
    "UseWindowsAuth": false,
    "SqlFetchBatchSize": 100000,
    "UseSSL": false,
    "JdbcProperties": null,
    "GenericDatabaseConfig": {
        "DatabaseJdbcUrl": "jdbc:TAOS-RS://127.0.0.1:6041/power?user=root&password=taosdata",
        "SqlDriverClassName": "com.taosdata.jdbc.rs.RestfulDriver",
        "ResolutionInNanoseconds": 1000,
        "ZonedColumnTypes": []
    }
}
```

- Goods

```
{
    "QueryDefinitions": [
        {
            "Name": "PowerGoods",
            "Type": "CONDITION",
            "Sql": "SELECT ts1, ts2, goods FROM power.goods",
            "Enabled": true,
            "TestMode": false,
            "TestQueriesDuringSync": true,
            "InProgressCapsulesEnabled": false,
            "Variables": null,
            "Properties": [
                {
                    "Name": "Name",
                    "Value": "Goods",
                    "Sql": null,
                    "Uom": "string"
                },
                {
                    "Name": "Maximum Duration",
                    "Value": "10days",
                    "Sql": null,
                    "Uom": "string"
                }
            ],
            "CapsuleProperties": [
                {
                    "Name": "goods",
                    "Value": "${columnResult}",
                    "Column": "goods",
                    "Uom": "string"
                }
            ]
        }
    ],
    "Type": "GENERIC",
    "Hostname": null,
    "Port": 0,
    "DatabaseName": null,
    "Username": "root",
    "Password": "taosdata",
    "InitialSql": null,
    "TimeZone": null,
    "PrintRows": false,
    "UseWindowsAuth": false,
    "SqlFetchBatchSize": 100000,
    "UseSSL": false,
    "JdbcProperties": null,
    "GenericDatabaseConfig": {
        "DatabaseJdbcUrl": "jdbc:TAOS-RS://127.0.0.1:6041/power?user=root&password=taosdata",
        "SqlDriverClassName": "com.taosdata.jdbc.rs.RestfulDriver",
        "ResolutionInNanoseconds": 1000,
        "ZonedColumnTypes": []
    }
}
```

- Temperature

```
{
    "QueryDefinitions": [
        {
            "Name": "PowerNum",
            "Type": "SIGNAL",
            "Sql": "SELECT  ts, temperature FROM meters",
            "Enabled": true,
            "TestMode": false,
            "TestQueriesDuringSync": true,
            "InProgressCapsulesEnabled": false,
            "Variables": null,
            "Properties": [
                {
                    "Name": "Name",
                    "Value": "Temperature",
                    "Sql": null,
                    "Uom": "string"
                },
                {
                    "Name": "Interpolation Method",
                    "Value": "linear",
                    "Sql": null,
                    "Uom": "string"
                },
                {
                    "Name": "Maximum Interpolation",
                    "Value": "2day",
                    "Sql": null,
                    "Uom": "string"
                }
            ],
            "CapsuleProperties": null
        }
    ],
    "Type": "GENERIC",
    "Hostname": null,
    "Port": 0,
    "DatabaseName": null,
    "Username": "root",
    "Password": "taosdata",
    "InitialSql": null,
    "TimeZone": null,
    "PrintRows": false,
    "UseWindowsAuth": false,
    "SqlFetchBatchSize": 100000,
    "UseSSL": false,
    "JdbcProperties": null,
    "GenericDatabaseConfig": {
        "DatabaseJdbcUrl": "jdbc:TAOS-RS://127.0.0.1:6041/power?user=root&password=taosdata",
        "SqlDriverClassName": "com.taosdata.jdbc.rs.RestfulDriver",
        "ResolutionInNanoseconds": 1000,
        "ZonedColumnTypes": []
    }
}
```

#### 使用 Seeq Workbench

登录 Seeq 服务页面并新建 Seeq Workbench，通过选择数据源搜索结果和根据需要选择不同的工具，可以进行数据展现或预测，详细使用方法参见[官方知识库](https://support.seeq.com/space/KB/146440193/Seeq+Workbench)。

![Seeq Workbench](./seeq/seeq-demo-workbench.webp)

#### 用 Seeq Data Lab Server 进行进一步的数据分析

登录 Seeq 服务页面并新建 Seeq Data Lab，可以进一步使用 Python 编程或其他机器学习工具进行更复杂的数据挖掘功能。

```Python
from seeq import spy
spy.options.compatibility = 189
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import mlforecast
import lightgbm as lgb
from mlforecast.target_transforms import Differences
from sklearn.linear_model import LinearRegression

ds = spy.search({'ID': "8C91A9C7-B6C2-4E18-AAAF-XXXXXXXXX"})
print(ds)

sig = ds.loc[ds['Name'].isin(['Num'])]
print(sig)

data = spy.pull(sig, start='2015-01-01', end='2022-12-31', grid=None)
print("data.info()")
data.info()
print(data)
#data.plot()

print("data[Num].info()")
data['Num'].info()
da = data['Num'].index.tolist()
#print(da)

li = data['Num'].tolist()
#print(li)

data2 = pd.DataFrame()
data2['ds'] = da
print('1st data2 ds info()')
data2['ds'].info()

#data2['ds'] = pd.to_datetime(data2['ds']).to_timestamp()
data2['ds'] = pd.to_datetime(data2['ds']).astype('int64')
data2['y'] = li
print('2nd data2 ds info()')
data2['ds'].info()
print(data2)

data2.insert(0, column = "unique_id", value="unique_id")

print("Forecasting ...")

forecast = mlforecast.MLForecast(
    models = lgb.LGBMRegressor(),
    freq = 1,
    lags=[365],
    target_transforms=[Differences([365])],
)

forecast.fit(data2)
predicts = forecast.predict(365)

pd.concat([data2, predicts]).set_index("ds").plot(title = "current data with forecast")
plt.show()
```

运行程序输出结果：

![Seeq forecast result](./seeq/seeq-forecast-result.webp)

### 配置 Seeq 数据源连接 TDengine Cloud

配置 Seeq 数据源连接 TDengine Cloud 和连接 TDengine 本地安装实例没有本质的不同，只要登录 TDengine Cloud 后选择“编程 - Java”并拷贝带 token 字符串的 JDBC 填写为 Seeq Data Source 的 DatabaseJdbcUrl 值。
注意使用 TDengine Cloud 时 SQL 命令中需要指定数据库名称。

#### 用 TDengine Cloud 作为数据源的配置内容示例：

```
{
    "QueryDefinitions": [
        {
            "Name": "CloudVoltage",
            "Type": "SIGNAL",
            "Sql": "SELECT  ts, voltage FROM test.meters",
            "Enabled": true,
            "TestMode": false,
            "TestQueriesDuringSync": true,
            "InProgressCapsulesEnabled": false,
            "Variables": null,
            "Properties": [
                {
                    "Name": "Name",
                    "Value": "Voltage",
                    "Sql": null,
                    "Uom": "string"
                },
                {
                    "Name": "Interpolation Method",
                    "Value": "linear",
                    "Sql": null,
                    "Uom": "string"
                },
                {
                    "Name": "Maximum Interpolation",
                    "Value": "2day",
                    "Sql": null,
                    "Uom": "string"
                }
            ],
            "CapsuleProperties": null
        }
    ],
    "Type": "GENERIC",
    "Hostname": null,
    "Port": 0,
    "DatabaseName": null,
    "Username": "root",
    "Password": "taosdata",
    "InitialSql": null,
    "TimeZone": null,
    "PrintRows": false,
    "UseWindowsAuth": false,
    "SqlFetchBatchSize": 100000,
    "UseSSL": false,
    "JdbcProperties": null,
    "GenericDatabaseConfig": {
        "DatabaseJdbcUrl": "jdbc:TAOS-RS://gw.cloud.taosdata.com?useSSL=true&token=41ac9d61d641b6b334e8b76f45f5a8XXXXXXXXXX",
        "SqlDriverClassName": "com.taosdata.jdbc.rs.RestfulDriver",
        "ResolutionInNanoseconds": 1000,
        "ZonedColumnTypes": []
    }
}
```

#### TDengine Cloud 作为数据源的 Seeq Workbench 界面示例

![Seeq workbench with TDengine cloud](./seeq/seeq-workbench-with-tdengine-cloud.webp)

## 方案总结

通过集成Seeq和TDengine，可以充分利用TDengine高效的存储和查询性能，同时也可以受益于Seeq提供给用户的强大数据可视化和分析功能。

这种集成使用户能够充分利用TDengine的高性能时序数据存储和检索，确保高效处理大量数据。同时，Seeq提供高级分析功能，如数据可视化、异常检测、相关性分析和预测建模，使用户能够获得有价值的洞察并基于数据进行决策。

综合来看，Seeq和TDengine共同为制造业、工业物联网和电力系统等各行各业的时序数据分析提供了综合解决方案。高效数据存储和先进的分析相结合，赋予用户充分发挥时序数据潜力的能力，推动运营改进，并支持预测和规划分析应用。

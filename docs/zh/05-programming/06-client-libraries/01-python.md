---
sidebar_label: Python
title: TDengine Python 连接器
description: "taospy 是 TDengine 的官方 Python 连接器。taospy 提供了丰富的 API，使得 Python 应用可以很方便地使用 TDengine。tasopy 对 TDengine 的原生接口和 REST 接口都进行了封装，分别对应 tasopy 的两个子模块：taos 和 taosrest。除了对原生接口和 REST 接口的封装，taospy 还提供了符合 Python 数据访问规范 (PEP 249) 的编程接口。这使得 taospy 和很多第三方工具集成变得简单，比如 SQLAlchemy 和 pandas"
---

`taospy` 是 TDengine 的官方 Python 连接器。`taospy` 提供了丰富的 API，使得 Python 应用可以很方便地使用 TDengine。
`taospy` 对 REST 接口进行了封装，还提供了符合 [Python 数据访问规范 (PEP 249)](https://peps.python.org/pep-0249/) 的编程接口。这使得 `taospy` 和很多第三方工具集成变得简单，比如 [SQLAlchemy](https://www.sqlalchemy.org/) 和 [pandas](https://pandas.pydata.org/)。

Python 连接器的源码托管在 [GitHub](https://github.com/taosdata/taos-connector-python)。

## 连接方式

`taospy`主要提供三种形式的连接器。TDengine Cloud 支持 REST 连接和 WebSocket 连接两种方式

* 原生连接，对应 taospy 包的 taos 模块。通过 TDengine 客户端驱动程序（taosc）原生连接 TDengine 实例，支持数据写入、查询、数据订阅、schemaless 接口和参数绑定接口等功能。
* REST 连接，对应 taospy 包的 taosrest 模块。通过 taosAdapter 提供的 HTTP 接口连接 TDengine 实例，不支持 schemaless 和数据订阅等特性。
* WebSocket 连接，对应 taos-ws-py 包，可以选装。通过 taosAdapter 提供的 WebSocket 接口连接 TDengine 实例，WebSocket 连接实现的功能集合和原生连接有少量不同。

:::note IMPORTANT

1. 使用客户端驱动提供的原生接口直接与服务端建立的连接，下文中称为“原生连接”.

2. 使用 taosAdapter 提供的 REST 接口或 WebSocket 接口与服务端建立的连接的方式下文中称为“REST 连接”或“WebSocket 连接”。
:::

关于如何建立连接的详细介绍请参考：[开发指南 - 建立连接-Python](../01-connect/01-python.md)

## 示例程序

下面以智能电表为例，展示如何使用 Python 连接器在名为 power 的数据库中，创建一个名为 meters 的超级表（STABLE），插入并查询数据。meters 表结构包含时间戳、电流、电压、相位等列，以及分组 ID 和位置作为标签。

:::note IMPORTANT

1. 在执行下面样例代码的之前，您必须先在 [TDengine Cloud - 数据浏览器](https://cloud.taosdata.com/explorer) 页面创建一个名为 power 的数据库
2. 如何在代码中建立和 TDengine Cloud 的连接，请参考 [开发指南 - 建立连接](../../connect/)。
:::

### 使用 TaosRestConnection 类

```python
{{#include docs/examples/python/reference_connection.py:example}}
```

### 使用 TaosRestCursor 类

`TaosRestCursor` 类是对 PEP249 Cursor 接口的实现。

```python
{{#include docs/examples/python/reference_cursor.py:basic}}
```

* `cursor.execute` ：用来执行任意 SQL 语句。
* `cursor.rowcount`：对于写入操作返回写入成功记录数。对于查询操作，返回结果集行数。
* `cursor.description` ：返回字段的描述信息。关于描述信息的具体格式请参考[TaosRestCursor](https://docs.taosdata.com/api/taospy/taosrest/cursor.html)。

#### 使用 RestClient 类

`RestClient` 类是对于 [REST API](../rest-api) 的直接封装。它只包含一个 `sql()` 方法用于执行任意 SQL 语句，并返回执行结果。

```python
{{#include docs/examples/python/reference_rest_client.py}}
```

对于 `sql()` 方法更详细的介绍，请参考 [RestClient](https://docs.taosdata.com/api/taospy/taosrest/restclient.html)。

## 其它说明

### 异常处理

所有数据库操作如果出现异常，都会直接抛出来。由应用程序负责异常处理。比如：

```python
{{#include docs/examples/python/handle_exception.py}}
```

### 关于纳秒 (nanosecond)

由于目前 Python 对 nanosecond 支持的不完善 (见下面的链接)，目前的实现方式是在 nanosecond 精度时返回整数，而不是 ms 和 us 返回的 datetime 类型，应用开发者需要自行处理，建议使用 pandas 的 to_datetime()。未来如果 Python 正式完整支持了纳秒，Python 连接器可能会修改相关接口。

1. [parsing-datetime-strings-containing-nanoseconds](https://stackoverflow.com/questions/10611328/parsing-datetime-strings-containing-nanoseconds)
2. [PEP 564 -- Support for the Python nanosecond timestamp](https://www.python.org/dev/peps/pep-0564/)

## 重要更新

[**Release Notes**](https://github.com/taosdata/taos-connector-python/releases)

## API 参考

* [连接器-Python-Api 参考](https://docs.taosdata.com/reference/connector/python/#api-参考)
* [taos](https://docs.taosdata.com/api/taospy/taos/)
* [taosrest](https://docs.taosdata.com/api/taospy/taosrest)

更多关于 Python 连接器的详细介绍请参考[连接器-Python](https://docs.taosdata.com/reference/connector/python)

## 常见问题

欢迎[提问或报告问题](https://github.com/taosdata/taos-connector-python/issues)。

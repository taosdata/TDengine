---
sidebar_label: Python
title: TDengine Python 连接器
description: "taospy 是 TDengine 的官方 Python 连接器。taospy 提供了丰富的 API， 使得 Python 应用可以很方便地使用 TDengine。tasopy 对 TDengine 的原生接口和 REST 接口都进行了封装， 分别对应 tasopy 的两个子模块：taos 和 taosrest。除了对原生接口和 REST 接口的封装，taospy 还提供了符合 Python 数据访问规范(PEP 249)的编程接口。这使得 taospy 和很多第三方工具集成变得简单，比如 SQLAlchemy 和 pandas"
---

`taospy` 是 TDengine 的官方 Python 连接器。`taospy` 提供了丰富的 API， 使得 Python 应用可以很方便地使用 TDengine。`taospy` 对 REST 接口都进行了封装。`taospy` 还提供了符合 [Python 数据访问规范(PEP 249)](https://peps.python.org/pep-0249/) 的编程接口。这使得 `taospy` 和很多第三方工具集成变得简单，比如 [SQLAlchemy](https://www.sqlalchemy.org/) 和 [pandas](https://pandas.pydata.org/)。

Python 连接器的源码托管在 [GitHub](https://github.com/taosdata/taos-connector-python)。

## 安装

### 准备

1. 安装 Python。新近版本 taospy 包要求 Python 3.6+。早期版本 taospy 包要求 Python 3.7+。taos-ws-py 包要求 Python 3.7+。如果系统上还没有 Python 可参考 [Python BeginnersGuide](https://wiki.python.org/moin/BeginnersGuide/Download) 安装。
2. 安装 [pip](https://pypi.org/project/pip/)。大部分情况下 Python 的安装包都自带了 pip 工具， 如果没有请参考 [pip documentation](https://pip.pypa.io/en/stable/installation/) 安装。

### 使用 pip 安装

```bash
pip3 install -U taospy[ws]
```

### 使用 conda 安装

```bash
conda install -c conda-forge taospy taospyws
```

### 安装验证

只需验证是否能成功导入 `taosrest` 模块。可在 Python 交互式 Shell 中输入：

```python
import taosrest
```

## 建立连接

```python
{{#include docs/examples/python/reference_connection.py:connect}}
```

`connect()` 函数的所有参数都是可选的关键字参数。下面是连接参数的具体说明：

- `url`： TDengine Cloud 的URL。
- `token`: TDengine Cloud 的令牌.
- `timeout`: HTTP 请求超时时间。单位为秒。默认为 `socket._GLOBAL_DEFAULT_TIMEOUT`。 一般无需配置。

## 示例程序

### 使用 TaosRestConnection 类

```python
{{#include docs/examples/python/reference_connection.py:example}}
```

### 使用 TaosRestCursor 类

`TaosRestCursor` 类是对 PEP249 Cursor 接口的实现。

```python
{{#include docs/examples/python/reference_cursor.py:basic}}
```

- `cursor.execute` ： 用来执行任意 SQL 语句。
- `cursor.rowcount`： 对于写入操作返回写入成功记录数。对于查询操作，返回结果集行数。
- `cursor.description` ： 返回字段的描述信息。关于描述信息的具体格式请参考[TaosRestCursor](https://docs.taosdata.com/api/taospy/taosrest/cursor.html)。

##### RestClient 类的使用

`RestClient` 类是对于 [REST API](../rest-api) 的直接封装。它只包含一个 `sql()` 方法用于执行任意 SQL 语句， 并返回执行结果。

```python
{{#include docs/examples/python/reference_rest_client.py}}
```

对于 `sql()` 方法更详细的介绍， 请参考 [RestClient](https://docs.taosdata.com/api/taospy/taosrest/restclient.html)。

## 其它说明

### 异常处理

所有数据库操作如果出现异常，都会直接抛出来。由应用程序负责异常处理。比如：

```python
{{#include docs/examples/python/handle_exception.py}}
```

### 关于纳秒 (nanosecond)

由于目前 Python 对 nanosecond 支持的不完善(见下面的链接)，目前的实现方式是在 nanosecond 精度时返回整数，而不是 ms 和 us 返回的 datetime 类型，应用开发者需要自行处理，建议使用 pandas 的 to_datetime()。未来如果 Python 正式完整支持了纳秒，Python 连接器可能会修改相关接口。

1. https://stackoverflow.com/questions/10611328/parsing-datetime-strings-containing-nanoseconds
2. https://www.python.org/dev/peps/pep-0564/

## 重要更新

[**Release Notes**](https://github.com/taosdata/taos-connector-python/releases)

## API 参考

- [taos](https://docs.taosdata.com/api/taospy/taos/)
- [taosrest](https://docs.taosdata.com/api/taospy/taosrest)

## 常见问题

欢迎[提问或报告问题](https://github.com/taosdata/taos-connector-python/issues)。

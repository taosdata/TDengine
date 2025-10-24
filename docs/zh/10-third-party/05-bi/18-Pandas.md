---
sidebar_label: Pandas
title: 与 Pandas 集成
---

Pandas 是 Python 编程语言中最为流行的数据处理和分析库，自2008年由 Wes McKinney 创建以来，已成为数据科学领域不可或缺的核心工具。它专门为解决现实世界中的数据分析任务而设计，使得在 Python 中处理结构化数据变得异常简单。无论是处理商业报表、科学研究数据，还是进行金融分析，Pandas都能提供专业的解决方案。通过简洁的API和丰富的功能，Pandas极大地降低了数据处理的技术门槛，让使用者能够更专注于数据本身的价值挖掘，而非陷入繁琐的技术细节之中。

通过 TDengine TSDB 的 Python 连接器，Pandas 可支持 TDengine TSDB 数据源并提供数据展现、分析等功能。

## 前置条件

准备以下环境：

- TDengine TSDB 3.3.7.0 或以上版本已安装（企业及社区版均可）。
- SQLAlchemy v2.0.0 或以上版本已安装，安装参考 [官方文档](https://www.sqlalchemy.org/)。
- pandas v2.1.0 或以上版本已安装，安装参考 [官方文档](https://pandas.pydata.org/)。
- Python 连接器 taospy 2.8.6 或以上版本已 [安装](https://pypi.org/project/taospy/)。

## 配置数据源

Pandas 使用 SQLAlchemy 连接到 TDengine TSDB 数据源，连接 URL 格式为：

``` sql
taos://[username]:[password]@[<host1>:<port1>]/[database_name]
```

建立连接

```python
{{#include docs/examples/python/conn_native_pandas.py:connect}}
```

## 数据交互

下面介绍了如何通过调用 Pandas 接口，并结合 SQLAlchemy 与 TDengine TSDB 数据库进行写入和查询操作。
关于 Pandas 接口的详细说明请参照 [Pandas Api](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html#pandas.read_sql)。

### 数据类型映射

TDengine TSDB 目前支持时间戳、数字、字符、布尔类型，与 sqlalchemy.types 对应类型转换如下：

|  Sqlalchemy Types       |  TDengine TSDB DataType |       
| ------------------------|-------------------------|
| sqltypes.Boolean        | BOOL                    |
| sqltypes.TIMESTAMP      | TIMESTAMP               |
| sqltypes.Integer        | INT                     |
| sqltypes.Integer        | INT UNSIGNED            |
| sqltypes.BigInteger     | BIGINT                  |
| sqltypes.BigInteger     | BIGINT UNSIGNED         |
| sqltypes.FLOAT          | FLOAT                   |
| sqltypes.FLOAT          | DOUBLE                  |
| sqltypes.SmallInteger   | TINYINT                 |
| sqltypes.SmallInteger   | TINYINT UNSIGNED        |
| sqltypes.SmallInteger   | SMALLINT                |
| sqltypes.SmallInteger   | SMALLINT UNSIGNED       |
| sqltypes.String         | BINARY                  |
| sqltypes.String         | VARCHAR                 |
| sqltypes.BINARY         | VARBINARY               |
| sqltypes.Unicode        | NCHAR                   |
| sqltypes.JSON           | JSON                    |
| sqltypes.BLOB           | BLOB                    |
| sqltypes.BINARY         | GEOMETRY                |

### 数据写入

使用 Pandas 的 to_sql 方式写入数据：

```python
{{#include docs/examples/python/conn_native_pandas.py:pandas_to_sql_example}}
```

### 数据查询

使用 Pandas 的 read_sql 方式进行查询：

```python
{{#include docs/examples/python/conn_native_pandas.py:pandas_read_sql_example}}
```

使用 Pandas 的 read_sql_table 方式读取表数据：

```python
{{#include docs/examples/python/conn_native_pandas.py:pandas_read_sql_table_example}}
```

## 参考文档

- [Sqlalchemy](https://docs.sqlalchemy.org/en/20/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Connecting Pandas to a Database with SQLAlchemy](https://hackersandslackers.com/connecting-pandas-to-a-sql-database-with-sqlalchemy/)


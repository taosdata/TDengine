---
sidebar_label: Pandas
title: Integrate with Pandas
toc_max_heading_level: 5
---

Pandas is the most popular data processing and analysis library in the Python programming language. Since its creation in 2008 by Wes McKinney, it has become an indispensable core tool in the field of data science. Designed specifically to address real-world data analysis tasks, Pandas makes handling structured data in Python exceptionally straightforward. Whether dealing with business reports, scientific research data, or conducting financial analysis, Pandas provides professional solutions. With its intuitive API and extensive functionality, Pandas significantly lowers the technical barrier to data processing, enabling users to focus more on uncovering the value within the data rather than getting bogged down in technical intricacies.

Through the Python connector of TDengine TSDB, Pandas supports TDengine TSDB data sources and provides capabilities for data presentation and analysis.

## Prerequisites

Prepare the following environment:

- TDengine TSDB 3.3.7.0 and above version is installed and running normally (both Enterprise and Community versions are available).
- SQLAlchemy version 2.0.0 or above is already installed, refre to [Install reference link](https://www.sqlalchemy.org/).
- pandas version v2.1.0 or above is already installed, refre to [Install reference link](https://pandas.pydata.org/)
- The Python connector taospy version 2.8.6 or higher is installed, [Install reference link](https://pypi.org/project/taospy/).

## Configure Data Source

Pandas uses SQLAlchemy to connect to TDengine TSDB data sources, with the connection URL formatted as:

``` sql
taos://[username]:[password]@[<host1>:<port1>]/[database_name]
```

Establishing Connection

```python
{{#include docs/examples/python/conn_native_pandas.py:connect}}
```

## Data Interaction

The following describes how to perform write and query operations with the TDengine TSDB database by invoking Pandas interfaces in combination with SQLAlchemy.
For detailed specifications of the Pandas interfaces, please refer to [Pandas Api](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html#pandas.read_sql).

### Data type mapping

TDengine currently supports timestamp, number, character, and boolean types, and the corresponding type conversions with Fsqlalchemy.types are as follows:

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

### Data Writing

Writing data using Pandas' to_sql method:

```python
{{#include docs/examples/python/conn_native_pandas.py:pandas_to_sql_example}}
```

### Data Reading

Querying using Pandas' read_sql method:

```python
{{#include docs/examples/python/conn_native_pandas.py:pandas_read_sql_example}}
```

Reading table data using Pandas' read_sql_table method:

```python
{{#include docs/examples/python/conn_native_pandas.py:pandas_read_sql_table_example}}
```

## References

- [Sqlalchemy](https://docs.sqlalchemy.org/en/20/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Connecting Pandas to a Database with SQLAlchemy](https://hackersandslackers.com/connecting-pandas-to-a-sql-database-with-sqlalchemy/)

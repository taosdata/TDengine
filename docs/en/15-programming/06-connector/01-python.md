---
sidebar_label: Python
title: TDengine Python Connector
description: Detailed guide for Python Connector
---

`taospy` is the official Python connector for TDengine. `taospy` wraps the  [REST interface](/reference/rest-api) of TDengine. Additionally `taospy`  provides a set of programming interfaces that conforms to the [Python Data Access Specification (PEP 249)](https://peps.python.org/pep-0249/). It is easy to integrate `taospy` with many third-party tools, such as [SQLAlchemy](https://www.sqlalchemy.org/) and [pandas](https://pandas.pydata.org/).

The source code for the Python connector is hosted on [GitHub](https://github.com/taosdata/taos-connector-python).

## Installation

### Preparation

1. Install Python. Python >= 3.6 is recommended. If Python is not available on your system, refer to the [Python BeginnersGuide](https://wiki.python.org/moin/BeginnersGuide/Download) to install it.
2. Install [pip](https://pypi.org/project/pip/). In most cases, the Python installer comes with the pip utility. If not, please refer to [pip documentation](https://pip.pypa.io/en/stable/installation/) to install it.

### Install via pip

```
pip3 install -U taospy
```
### Install vial conda

```
conda install -c conda-forge taospy
```

### Installation verification

Verifying that the `taosrest` module can be imported successfully. This can be done in the Python Interactive Shell by typing.

```python
import taosrest
```

## Establish connection

```python
{{#include docs/examples/python/reference_connection.py:connect}}
```

All arguments to the `connect()` function are optional keyword arguments. The following are the connection parameters specified.

- `url`： The cloud URL.
- `token`: The cloud token.
- `timeout`: HTTP request timeout in seconds. The default is `socket._GLOBAL_DEFAULT_TIMEOUT`. Usually, no configuration is needed.

## Sample program

### Use of TaosRestConnection Class

```python
{{#include docs/examples/python/reference_connection.py:example}}
```

### Use of TaosRestCursor Class

The `TaosRestCursor` class is an implementation of the PEP249 Cursor interface.

```python
{{#include docs/examples/python/reference_cursor.py:basic}}
```
- `cursor.execute` : Used to execute arbitrary SQL statements.
- `cursor.rowcount` : For write operations, returns the number of successful rows written. For query operations, returns the number of rows in the result set.
- `cursor.description` : Returns the description of the field. Please refer to [TaosRestCursor](https://docs.taosdata.com/api/taospy/taosrest/cursor.html) for the specific format of the description information.

### Use of the RestClient class

The `RestClient` class is a direct wrapper for the [REST API](/reference/rest-api). It contains only a `sql()` method for executing arbitrary SQL statements and returning the result.

```python
{{#include docs/examples/python/reference_rest_client.py}}
```

For a more detailed description of the `sql()` method, please refer to [RestClient](https://docs.taosdata.com/api/taospy/taosrest/restclient.html).

## Important Update

| Connector version | Important Update                          | Release date |
| ----------------- | ----------------------------------------- | ------------ |
| 2.6.2             | fix ci script                             | 2022-08-18   |
| 2.5.2             | fix taos-ws-py python version dependency  | 2022-08-12   |
| 2.5.1             | (rest): add timezone option               | 2022-08-11   |
| 2.5.0             | add taosws module                         | 2022-08-10   |
| 2.4.0             | add execute method to TaosRestConnection  | 2022-07-18   |
| 2.3.3             | support connect to TDengine Cloud Service | 2022-06-06   |

[**Release Notes**](https://github.com/taosdata/taos-connector-python/releases)

## API Reference

- [taos](https://docs.taosdata.com/api/taospy/taos/)
- [taosrest](https://docs.taosdata.com/api/taospy/taosrest)

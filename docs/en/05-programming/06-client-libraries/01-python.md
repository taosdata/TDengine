---
sidebar_label: Python
title: TDengine Python Client Library
description: This document describes the TDengine Python client library.
---

`taospy` is the official Python client library for TDengine. `taospy` wraps the  [REST interface](/tdengine-reference/client-libraries/rest-api/) of TDengine. Additionally `taospy`  provides a set of programming interfaces that conforms to the [Python Data Access Specification (PEP 249)](https://peps.python.org/pep-0249/). It is easy to integrate `taospy` with many third-party tools, such as [SQLAlchemy](https://www.sqlalchemy.org/) and [pandas](https://pandas.pydata.org/).

The source code for the Python client library is hosted on [GitHub](https://github.com/taosdata/taos-connector-python).

## Connection types

`taospy` mainly provides 3 connection types. TDengine Cloud can be accessed by both REST and WebSocket connections.

* Native connection, which correspond to the taos modules of the taospy package, connects to TDengine instances natively through the TDengine client driver (taosc), supporting data writing, querying, subscriptions, schemaless writing, and bind interface.
* REST connection, which correspond to the taosrest modules of the taospy package, which is implemented through taosAdapter. Some features like schemaless and subscriptions are not supported.
* Websocket connection taos-ws-py is an optional package to enable using WebSocket to connect TDengine, which is implemented through taosAdapter. The set of features implemented by the WebSocket connection differs slightly from those implemented by the native connection.

:::note IMPORTANT

1. The direct connection to the server using the native interface provided by the client driver is referred to hereinafter as a "native connection".
2. The connection to the server using the REST or WebSocket interface provided by taosAdapter is referred to hereinafter as a "REST connection" or "WebSocket connection".
:::

For detailed information on how to establish a connection, please refer to: [Programming - Connect - Python](../01-connect/01-python.md).

## Installation

### Preparation

1. Install Python. Python >= 3.6 is recommended. If Python is not available on your system, refer to the [Python BeginnersGuide](https://wiki.python.org/moin/BeginnersGuide/Download) to install it.
2. Install [pip](https://pypi.org/project/pip/). In most cases, the Python installer comes with the pip utility. If not, please refer to [pip documentation](https://pip.pypa.io/en/stable/installation/) to install it.

### Install via pip

```bash
pip3 install -U taospy[ws]
```

### Install vial conda

```bash
conda install -c conda-forge taospy taospyws
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

* `url`： The cloud URL.
* `token`: The cloud token.
* `timeout`: HTTP request timeout in seconds. The default is `socket._GLOBAL_DEFAULT_TIMEOUT`. Usually, no configuration is needed.

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

* `cursor.execute` : Used to execute arbitrary SQL statements.
* `cursor.rowcount` : For write operations, returns the number of successful rows written. For query operations, returns the number of rows in the result set.
* `cursor.description` : Returns the description of the field. Please refer to [TaosRestCursor](https://docs.taosdata.com/api/taospy/taosrest/cursor.html) for the specific format of the description information.

### Use of the RestClient class

The `RestClient` class is a direct wrapper for the [REST API](/tdengine-reference/client-libraries/rest-api/). It contains only a `sql()` method for executing arbitrary SQL statements and returning the result.

```python
{{#include docs/examples/python/reference_rest_client.py}}
```

For a more detailed description of the `sql()` method, please refer to [RestClient](https://docs.taosdata.com/api/taospy/taosrest/restclient.html).

## Other notes

### Exception handling

All errors from database operations are thrown directly as exceptions and the error message from the database is passed up the exception stack. The application is responsible for exception handling. For example:

```python
import taos

try:
    conn = taos.connect()
    conn.execute("CREATE TABLE 123")  # wrong sql
except taos.Error as e:
    print(e)
    print("exception class: ", e.__class__.__name__)
    print("error number:", e.errno)
    print("error message:", e.msg)
except BaseException as other:
    print("exception occur")
    print(other)

# output:
# [0x0216]: syntax error near 'Incomplete SQL statement'
# exception class:  ProgrammingError
# error number: -2147483114
# error message: syntax error near 'Incomplete SQL statement'

```

[view source code](https://github.com/taosdata/TDengine/blob/3.0/docs/examples/python/handle_exception.py)

### About nanoseconds

Due to the current imperfection of Python's nanosecond support (see link below), the current implementation returns integers at nanosecond precision instead of the `datetime` type produced by `ms` and `us`, which application developers will need to handle on their own. And it is recommended to use pandas' to_datetime(). The Python client library may modify the interface in the future if Python officially supports nanoseconds in full.

1. [https://stackoverflow.com/questions/10611328/parsing-datetime-strings-containing-nanoseconds](https://stackoverflow.com/questions/10611328/parsing-datetime-strings-containing-nanoseconds)
2. [https://www.python.org/dev/peps/pep-0564/](https://www.python.org/dev/peps/pep-0564/)

## Important Update

[**Release Notes**](https://github.com/taosdata/taos-connector-python/releases)

## API Reference

* [taos](https://docs.taosdata.com/api/taospy/taos/)
* [taosrest](https://docs.taosdata.com/api/taospy/taosrest)

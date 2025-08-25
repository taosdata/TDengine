---
sidebar_label: PHP
title: PHP Client Library
slug: /tdengine-reference/client-libraries/php
---

import CommunityLibrary from '../../assets/resources/_community-library.mdx';

<CommunityLibrary/>

`php-tdengine` is a PHP connector extension contributed by the community, which also specifically supports Swoole coroutine.

The PHP connector depends on the TDengine client driver.

Project address: [https://github.com/Yurunsoft/php-tdengine](https://github.com/Yurunsoft/php-tdengine)

After installing the TDengine server or client, `taos.h` is located at:

- Linux: `/usr/local/taos/include`
- Windows: `C:\TDengine\include`
- macOS: `/usr/local/include`

The dynamic library of the TDengine client driver is located at:

- Linux: `/usr/local/taos/driver/libtaos.so`
- Windows: `C:\TDengine\taos.dll`
- macOS: `/usr/local/lib/libtaos.dylib`

## Supported Platforms

- Windows, Linux, MacOS

- PHP >= 7.4

- TDengine >= 2.0

- Swoole >= 4.8 (optional)

## Supported Versions

The version number of the TDengine client driver is strongly correlated with the version number of the TDengine server. It is recommended to use the client driver that is exactly the same as the TDengine server. Although a lower version of the client driver can be compatible with a higher version of the server if the first three segments of the version number match (only the fourth segment is different), this is not recommended. It is strongly discouraged to use a higher version of the client driver to access a lower version of the server.

## Installation Steps

### Install TDengine Client Driver

Please refer to the [Installation Guide](../#installation-steps) for the installation of the TDengine client driver.

### Compile and install php-tdengine

**Download and unzip the code:**

```shell
curl -L -o php-tdengine.tar.gz https://github.com/Yurunsoft/php-tdengine/archive/refs/tags/v1.0.2.tar.gz \
&& mkdir php-tdengine \
&& tar -xzf php-tdengine.tar.gz -C php-tdengine --strip-components=1
```

> Version `v1.0.2` can be replaced with any newer version, available at [TDengine PHP Connector Release History](https://github.com/Yurunsoft/php-tdengine/releases).

**Non-Swoole environment:**

```shell
phpize && ./configure && make -j && make install
```

**Manually specify the tdengine directory:**

```shell
phpize && ./configure --with-tdengine-dir=/usr/local/Cellar/tdengine/3.0.0.0 && make -j && make install
```

> Follow `--with-tdengine-dir=` with the tdengine directory.
> Suitable for cases where it cannot be found by default, or for MacOS users.

**Swoole environment:**

```shell
phpize && ./configure --enable-swoole && make -j && make install
```

**Enable the extension:**

Method one: Add `extension=tdengine` in `php.ini`

Method two: Run with the parameter `php -dextension=tdengine test.php`

## Example Program

This section shows example code for common ways to access the TDengine cluster using the client driver.

> All errors will throw an exception: `TDengine\Exception\TDengineException`

### Establish Connection

<details>
<summary>Establish Connection</summary>

```c
{{#include docs/examples/php/connect.php}}
```

</details>

### Insert Data

<details>
<summary>Insert Data</summary>

```c
{{#include docs/examples/php/insert.php}}
```

</details>

### Synchronous Query

<details>
<summary>Synchronous Query</summary>

```c
{{#include docs/examples/php/query.php}}
```

</details>

### Parameter Binding

<details>
<summary>Parameter Binding</summary>

```c
{{#include docs/examples/php/insert_stmt.php}}
```

</details>

## Constants

| Constant                            | Description |
| ----------------------------------- | ----------- |
| `TDengine\TSDB_DATA_TYPE_NULL`      | null        |
| `TDengine\TSDB_DATA_TYPE_BOOL`      | bool        |
| `TDengine\TSDB_DATA_TYPE_TINYINT`   | tinyint     |
| `TDengine\TSDB_DATA_TYPE_SMALLINT`  | smallint    |
| `TDengine\TSDB_DATA_TYPE_INT`       | int         |
| `TDengine\TSDB_DATA_TYPE_BIGINT`    | bigint      |
| `TDengine\TSDB_DATA_TYPE_FLOAT`     | float       |
| `TDengine\TSDB_DATA_TYPE_DOUBLE`    | double      |
| `TDengine\TSDB_DATA_TYPE_BINARY`    | binary      |
| `TDengine\TSDB_DATA_TYPE_VARBINARY` | varbinary   |
| `TDengine\TSDB_DATA_TYPE_TIMESTAMP` | timestamp   |
| `TDengine\TSDB_DATA_TYPE_NCHAR`     | nchar       |
| `TDengine\TSDB_DATA_TYPE_UTINYINT`  | utinyint    |
| `TDengine\TSDB_DATA_TYPE_USMALLINT` | usmallint   |
| `TDengine\TSDB_DATA_TYPE_UINT`      | uint        |
| `TDengine\TSDB_DATA_TYPE_UBIGINT`   | ubigint     |

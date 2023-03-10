---
title: REST API
sidebar_label: REST API
description: 详细介绍 TDengine 提供的 RESTful API.
---

为支持各种不同类型平台的开发，TDengine 提供符合 RESTful 设计标准的 API，即 REST API。为最大程度降低学习成本，不同于其他数据库 REST API 的设计方法，TDengine 直接通过 HTTP POST 请求 BODY 中包含的 SQL 语句来操作数据库，仅需要一个 URL。

:::note
与原生连接器的一个区别是，RESTful 接口是无状态的，因此 `USE db_name` 指令没有效果，所有对表名、超级表名的引用都需要指定数据库名前缀。支持在 RESTful URL 中指定 db_name，这时如果 SQL 语句中没有指定数据库名前缀的话，会使用 URL 中指定的这个 db_name。
:::

## 安装

RESTful 接口不依赖于任何 TDengine 的库，因此客户端不需要安装任何 TDengine 的库，只要客户端的开发语言支持 HTTP 协议即可。

## 验证

在已经安装 TDengine 服务器端的情况下，可以按照如下方式进行验证。

下面以 Ubuntu 环境中使用 `curl` 工具（请确认已经安装）来验证 RESTful 接口是否工作正常，验证前请确认 taosAdapter 服务已开启，在 Linux 系统上此服务默认由 systemd 管理，使用命令 `systemctl start taosadapter` 启动。

下面示例是列出所有的数据库都在 `TDengine Cloud URL` 主机上面。如果您正在访问 TDengine Cloud ，您必须使用云服务的令牌。

```bash
curl -L \
  -d "select name, ntables, status from information_schema.ins_databases;" \
  <TDengine Cloud URL>/rest/sql?token=<TDengine Cloud Token>
```

返回值结果如下表示验证通过：

```json
{
    "code": 0,
    "column_meta": [
        [
            "name",
            "VARCHAR",
            64
        ],
        [
            "ntables",
            "BIGINT",
            8
        ],
        [
            "status",
            "VARCHAR",
            10
        ]
    ],
    "data": [
        [
            "information_schema",
            16,
            "ready"
        ],
        [
            "performance_schema",
            9,
            "ready"
        ]
    ],
    "rows": 2
}
```

## HTTP 请求格式

```text
https://<TDENGINE_CLOUD_URL>/rest/sql/[db_name]?token=TDENGINE_CLOUD_TOKEN
```

参数说明：

- TDENGINE_CLOUD_URL: TDengine Cloud 的地址。
- db_name: 可选参数，指定本次所执行的 SQL 语句的默认数据库库名。
- token: 用来访问 TDengine Cloud 。

例如：`https://gw.cloud.taosdata.com/rest/sql/test?token=xxxxxxxxx` 是指向地址为 `gw-aws.cloud.tdengine:80` 的 URL，并将默认使用的数据库库名设置为 `test`。

HTTP 请求的 BODY 里就是一个完整的 SQL 语句，SQL 语句中的数据表应提供数据库前缀，例如 db_name.tb_name。如果表名不带数据库前缀，又没有在 URL 中指定数据库名的话，系统会返回错误。因为 HTTP 模块只是一个简单的转发，没有当前 DB 的概念。

使用 `curl` 通过自定义身份认证方式来发起一个 HTTP Request，语法如下：

```bash
curl -L -d "<SQL>" <TDENGINE_CLOUD_URL>/rest/sql/[db_name]?token=TDENGINE_CLOUD_TOKEN
```

## HTTP 返回格式

### HTTP 响应码

| **response code** | **说明**         |
|-------------------|----------------|
| 200               | 正确返回和 C 接口错误返回 |
| 400               | 参数错误返回         |
| 401               | 鉴权失败           |
| 404               | 接口不存在          |
| 500               | 内部错误           |
| 503               | 系统资源不足         |

### HTTP body 结构

#### 正确执行插入

样例：

```json
{
  "code": 0,
  "column_meta": [["affected_rows", "INT", 4]],
  "data": [[0]],
  "rows": 1
}
```

说明：

- code：（`int`）0 代表成功。
- column_meta：（`[1][3]any`）只返回 `[["affected_rows", "INT", 4]]`。
- rows：（`int`）只返回 `1`。
- data：（`[][]any`）返回受影响行数。

#### 正确执行查询

样例：

```json
{
  "code": 0,
  "column_meta": [
    ["ts", "TIMESTAMP", 8],
    ["count", "BIGINT", 8],
    ["endpoint", "VARCHAR", 45],
    ["status_code", "INT", 4],
    ["client_ip", "VARCHAR", 40],
    ["request_method", "VARCHAR", 15],
    ["request_uri", "VARCHAR", 128]
  ],
  "data": [
    [
      "2022-06-29T05:50:55.401Z",
      2,
      "LAPTOP-NNKFTLTG:6041",
      200,
      "172.23.208.1",
      "POST",
      "/rest/sql"
    ],
    [
      "2022-06-29T05:52:16.603Z",
      1,
      "LAPTOP-NNKFTLTG:6041",
      200,
      "172.23.208.1",
      "POST",
      "/rest/sql"
    ],
    [
      "2022-06-29T06:28:14.118Z",
      1,
      "LAPTOP-NNKFTLTG:6041",
      200,
      "172.23.208.1",
      "POST",
      "/rest/sql"
    ],
    [
      "2022-06-29T05:52:16.603Z",
      2,
      "LAPTOP-NNKFTLTG:6041",
      401,
      "172.23.208.1",
      "POST",
      "/rest/sql"
    ]
  ],
  "rows": 4
}
```

说明：

- code：（`int`）0 代表成功。
- column_meta：（`[][3]any`） 列信息，每个列会用三个值来说明，分别为：列名（string）、列类型（string）、类型长度（int）。
- rows：（`int`）数据返回行数。
- data：（`[][]any`）具体数据内容（时间格式仅支持 RFC3339，结果集为 0 时区）。

列类型使用如下字符串：

- "NULL"
- "BOOL"
- "TINYINT"
- "SMALLINT"
- "INT"
- "BIGINT"
- "FLOAT"
- "DOUBLE"
- "VARCHAR"
- "TIMESTAMP"
- "NCHAR"
- "TINYINT UNSIGNED"
- "SMALLINT UNSIGNED"
- "INT UNSIGNED"
- "BIGINT UNSIGNED"
- "JSON"

#### 错误

样例：

```json
{
  "code": 9728,
  "desc": "syntax error near \"1\""
}
```

说明：

- code：（`int`）错误码。
- desc：（`string`）错误描述。

## 使用示例

- 在 demo 库里查询表 d1001 的所有记录：

  ```bash
  export TDENGINE_CLOUD_URL=https://gw.cloud.taosdata.com
  export TDENGINE_CLOUD_TOKEN=<actual token string>
  curl -L -d "select * from demo.d1001" $TDENGINE_CLOUD_URL/rest/sql?token=$TDENGINE_CLOUD_TOKEN
  ```

  返回值：

  ```json
  {
      "code": 0,
      "column_meta": [
          [
              "ts",
              "TIMESTAMP",
              8
          ],
          [
              "current",
              "FLOAT",
              4
          ],
          [
              "voltage",
              "INT",
              4
          ],
          [
              "phase",
              "FLOAT",
              4
          ]
      ],
      "data": [
          [
              "2022-07-30T06:44:40.32Z",
              10.3,
              219,
              0.31
          ],
          [
              "2022-07-30T06:44:41.32Z",
              12.6,
              218,
              0.33
          ]
      ],
      "rows": 2
  }
  ```

- 创建库 demo：

  ```bash
  curl -L -d "create database demo" $TDENGINE_CLOUD_URL/rest/sql?token=$TDENGINE_CLOUD_TOKEN
  ```

  返回值：

  ```json
  {
      "code": 0,
      "column_meta": [
          [
              "affected_rows",
              "INT",
              4
          ]
      ],
      "data": [
          [
              0
          ]
      ],
      "rows": 1
  }
  ```

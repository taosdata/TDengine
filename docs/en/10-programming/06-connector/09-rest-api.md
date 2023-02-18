---
sidebar_label: REST API
title: REST API
description: Detailed guide for REST API
---

To support the development of various types of applications and platforms, TDengine provides an API that conforms to REST principles; namely REST API. To minimize the learning cost, unlike REST APIs for other database engines, TDengine allows insertion of SQL commands in the BODY of an HTTP POST request, to operate the database. 

:::note
One difference from the native connector is that the REST interface is stateless and so the `USE db_name` command has no effect. All references to table names and super table names need to specify the database name in the prefix. TDengine supports specification of the db_name in RESTful URL. If the database name prefix is not specified in the SQL command, the `db_name` specified in the URL will be used.
:::

## Installation

The REST interface does not rely on any TDengine native library, so the client application does not need to install any TDengine libraries. The client application's development language only needs to support the HTTP protocol.

## Verification

To verify accessing the TDengine Cloud service, it can be as follows:

The following example is in an Ubuntu environment and uses the `curl` tool to verify that the REST interface is working. Note that the `curl` tool may need to be installed in your environment.

The following example lists all databases on the `TDengine Cloud URL` host. If you are accessing TDengine Cloud, you need to use the given token.

```bash
curl -L \
  -d "select name, ntables, status from information_schema.ins_databases;" \
  <TDengine Cloud URL>/rest/sql?token=<TDengine Cloud Token>
```

The following return value results indicate that the verification passed.

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

## HTTP request URL format

```text
https://<TDENGINE_CLOUD_URL>/rest/sql/[db_name]?token=TDENGINE_CLOUD_TOKEN
```

Parameter Description:

- TDENGINE_CLOUD_URL: TDengine cloud service's address.
- db_name: Optional parameter specifies the default database name for the executed SQL command.
- token: used to access TDengine cloud service.

For example, `https://gw-aws.cloud.tdengine.com:80/rest/sql/test?token=xxxxxxxxx` is a URL to `gw-aws.cloud.tdengine:80` and sets the default database name to `test`.

The HTTP request's BODY is a complete SQL command, and the data table in the SQL statement should be provided with a database prefix, e.g., `db_name.tb_name`. If the table name does not have a database prefix and the database name is not specified in the URL, the system will respond with an error because the HTTP module is a simple forwarder and has no awareness of the current DB.

Use `curl` to initiate an HTTP request with a custom authentication method, with the following syntax.

```bash
curl -L -d "<SQL>" <TDENGINE_CLOUD_URL>/rest/sql/[db_name]?token=TDENGINE_CLOUD_TOKEN
```

## HTTP Return Format

### HTTP Response Code

| **Response Code** | **Description**         |
|-------------------|----------------|
| 200               | Success. (Also used for C interface errors.) |
| 400               | Parameter error         |
| 401               | Authentication failure           |
| 404               | Interface not found          |
| 500               | Internal error           |
| 503               | Insufficient system resources         |

### HTTP body structure

#### Successful Operation

Example:

```json
{
  "code": 0,
  "column_meta": [["affected_rows", "INT", 4]],
  "data": [[0]],
  "rows": 1
}
```

Description:

- code: (`int`) 0 indicates success.
- column_meta: (`[1][3]any`) Only returns `[["affected_rows", "INT", 4]]`.
- rows: (`int`) Only returns `1`.
- data: (`[][]any`) Returns the number of rows affected.

#### Successful Query

Example:

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

Description:

- code: `int` 0 indicates success.
- column_meta: (`[][3]any`) Column information. Each column is described with three values: column name (string), column type (string), and type length (int).
- rows: (`int`) The number of rows returned.
- data: (`[][]any`)

The following types may be returned:

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

#### Errors

Example:

```json
{
  "code": 9728,
  "desc": "syntax error near \"1\""
}
```

Description:

- code: (`int`) Error code.
- desc: (`string`): Error code description.

## Usage examples

- query all records from table d1001 of the database demo

  ```bash
  export TDENGINE_CLOUD_URL=https://gw-aws.cloud.tdengine.com:80
  export TDENGINE_CLOUD_TOKEN=<actual token string>

  curl -L -d "select * from demo.d1001" $TDENGINE_CLOUD_URL/rest/sql?token=$TDENGINE_CLOUD_TOKEN
  ```

  Response body:

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

- Create database demo:

  ```bash
  curl -L -d "create database demo" $TDENGINE_CLOUD_URL/rest/sql?token=$TDENGINE_CLOUD_TOKEN
  ```

  Response body:

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


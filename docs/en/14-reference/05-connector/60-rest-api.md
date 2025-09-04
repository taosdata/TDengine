---
title: REST API
slug: /tdengine-reference/client-libraries/rest-api
---

To support development on various types of platforms, TDengine offers an API that adheres to RESTful design standards, namely the REST API. To minimize the learning curve, unlike other databases' REST API design methods, TDengine operates the database directly through SQL statements contained in the BODY of an HTTP POST request, requiring only one URL.

:::note
One difference from native connectors is that the RESTful interface is stateless, so the `USE db_name` command has no effect, and all references to table names and supertable names need to specify the database name prefix. It supports specifying db_name in the RESTful URL, in which case if the SQL statement does not specify a database name prefix, the db_name specified in the URL will be used.
:::

## Installation

The RESTful interface does not depend on any TDengine libraries, so the client does not need to install any TDengine libraries as long as the client's development language supports the HTTP protocol. TDengine's RESTful API is provided by [taosAdapter](../../components/taosadapter/), and `taosAdapter` must be running before using the RESTful API.

## Verification

If the TDengine server side is already installed, you can verify it in the following way.

Below is an example using the `curl` tool in an Ubuntu environment (please confirm it is installed) to verify if the RESTful interface is working properly. Before verifying, please ensure that the taosAdapter service is started, which is managed by systemd on Linux systems by default, using the command `systemctl start taosadapter`.

The following example lists all databases, please replace `h1.tdengine.com` and 6041 (default value) with the actual running TDengine service FQDN and port number:

```shell
curl -L -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" \
  -d "select name, ntables, status from information_schema.ins_databases;" \
  h1.tdengine.com:6041/rest/sql
```

If the return value is as follows, it indicates verification passed:

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

## HTTP Request Format

```text
http://<fqdn>:<port>/rest/sql/[db_name][?tz=timezone[&req_id=req_id][&row_with_meta=true]]
```

Parameter explanation:

- fqdn: Any host FQDN or IP address in the cluster.
- port: Configured in the httpPort configuration item, default is 6041.
- db_name: Optional parameter, specifies the default database name for the SQL statement being executed.
- tz: Optional parameter, specifies the time zone for the returned time, following the IANA Time Zone rules, such as `America/New_York`.
- req_id: Optional parameter, specifies the request id, which can be used for tracing.
- row_with_meta: Optional parameter, specifies whether each row of data carries column names, default is false. (Supported from version 3.3.2.0)

For example: `http://h1.taos.com:6041/rest/sql/test` points to the URL at `h1.taos.com:6041` and sets the default database name to `test`.

The HTTP request header must contain authentication information. TDengine supports two authentication mechanisms: Basic authentication and custom authentication. Future versions will provide a standard secure digital signature mechanism for identity verification.

- Custom authentication information is as follows:

  ```text
  Authorization: Taosd <TOKEN>
  ```

- Basic authentication information is as follows:

  ```text
  Authorization: Basic <TOKEN>
  ```

The BODY of the HTTP request contains a complete SQL statement. The data table in the SQL statement should provide a database prefix, such as db_name.tb_name. If the table name does not include a database prefix and no database name is specified in the URL, the system will return an error. This is because the HTTP module is just a simple forwarder and does not have the concept of the current DB.

Use `curl` to initiate an HTTP Request with custom authentication as follows:

```shell
curl -L -H "Authorization: Basic <TOKEN>" -d "<SQL>" <ip>:<PORT>/rest/sql/[db_name][?tz=timezone[&req_id=req_id][&row_with_meta=true]]
```

Or,

```shell
curl -L -u username:password -d "<SQL>" <ip>:<PORT>/rest/sql/[db_name][?tz=timezone[&req_id=req_id][&row_with_meta=true]]
```

Here, `TOKEN` is the string `{username}:{password}` after Base64 encoding, for example, `root:taosdata` encoded as `cm9vdDp0YW9zZGF0YQ==`.

## HTTP Response Format

### HTTP Response Codes

By default, `taosAdapter` returns a 200 response code for most C interface call errors, but the HTTP body contains error information. Starting from `TDengine 3.0.3.0`, `taosAdapter` provides a configuration parameter `httpCodeServerError` to set whether to return a non-200 HTTP response code when the C interface returns an error. Regardless of whether this parameter is set, the response body contains detailed error codes and error information, please refer to [Errors](../rest-api/).

**When httpCodeServerError is false:**

| **Description**             |**HTTP Response Code** |
|--------------------|-------------------------------|
| C interface call successful  | 200                           |
| C interface call error, not an authentication error | 200                   |
| HTTP request URL parameter error               | 400    |
| C interface call authentication error               | 401                           |
| Interface does not exist              | 404                           |
| Insufficient system resources             | 503                          |

**When httpCodeServerError is true:**

| **Description**             |  **HTTP Response Code**          |
|--------------------|-------------------------------|
| C interface call successful  |  200                                   |
| HTTP request URL parameter error and C interface call parameter parsing error    | 400  |
| C interface call authentication error                 |  401          |
| Interface does not exist              | 404             |
| C interface call network unavailable error            | 502            |
| Insufficient system resources             |503                |
| Other C interface call errors | 500                |

C interface parameter parsing related error codes:

- TSDB_CODE_TSC_SQL_SYNTAX_ERROR (0x0216)
- TSDB_CODE_TSC_LINE_SYNTAX_ERROR (0x021B)
- TSDB_CODE_PAR_SYNTAX_ERROR (0x2600)
- TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE (0x060B)
- TSDB_CODE_TSC_VALUE_OUT_OF_RANGE (0x0224)
- TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE (0x263B)

C interface authentication related error codes:

- TSDB_CODE_MND_USER_ALREADY_EXIST (0x0350)
- TSDB_CODE_MND_USER_NOT_EXIST (0x0351)
- TSDB_CODE_MND_INVALID_USER_FORMAT (0x0352)
- TSDB_CODE_MND_INVALID_PASS_FORMAT (0x0353)
- TSDB_CODE_MND_NO_USER_FROM_CONN (0x0354)
- TSDB_CODE_MND_TOO_MANY_USERS (0x0355)
- TSDB_CODE_MND_INVALID_ALTER_OPER (0x0356)
- TSDB_CODE_MND_AUTH_FAILURE (0x0357)

C interface network unavailability related error codes:

- TSDB_CODE_RPC_NETWORK_UNAVAIL (0x000B)

For error codes and descriptions, please refer to [Error Codes](../../error-codes/)

### HTTP body structure

#### Successful insertion execution

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

- code: (`int`) 0 represents success.
- column_meta: (`[1][3]any`) only returns `[["affected_rows", "INT", 4]]`.
- rows: (`int`) only returns `1`.
- data: (`[][]any`) returns the number of affected rows.

#### Successful query execution

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

- code: (`int`) 0 represents success.
- column_meta: (`[][3]any`) Column information, each column is described by three values: column name (string), column type (string), and type length (int).
- rows: (`int`) Number of data return rows.
- data: (`[][]any`) Specific data content (time format only supports RFC3339, result set for timezone 0, when specifying tz, the corresponding time zone is returned).

Column types use the following strings:

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
- "VARBINARY"
- "GEOMETRY"
- "DECIMAL(precision, scale)"
- "BLOB"

For the `DECIMAL` data type, `precision` refers to the maximum number of significant digits supported, and `scale` refers to the maximum number of decimal places. For example, `DECIMAL(8, 4)` represents a range of `[-9999.9999, 9999.9999]`.

`VARBINARY`, `GEOMETRY` and `BLOB` types return data as Hex strings, example:

Prepare data

```shell
create database demo;
create table demo.t(ts timestamp,c1 varbinary(20),c2 geometry(100),c3 blob);
insert into demo.t values(now,'\x7f8290','point(100 100)','\x010203ddff');
```

Execute query

```shell
curl --location 'http://<fqdn>:<port>/rest/sql' \
--header 'Content-Type: text/plain' \
--header 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' \
--data 'select * from demo.t'
```

Return result

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
            "c1",
            "VARBINARY",
            20
        ],
        [
            "c2",
            "GEOMETRY",
            100
        ],
        [
            "c3",
            "BLOB",
            4194304
        ]
    ],
    "data": [
        [
            "2025-07-22T05:58:41.798Z",
            "7f8290",
            "010100000000000000000059400000000000005940",
            "010203ddff"
        ]
    ],
    "rows": 1
}
```

- `010100000000000000000059400000000000005940` is the [Well-Known Binary (WKB)](https://libgeos.org/specifications/wkb/) format for `point(100 100)`

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
- desc: (`string`) Error description.

For error codes and descriptions, please refer to [Error Codes](../../error-codes/)

#### Return data in key-value format

When the URL parameter `row_with_meta=true` is specified, the data in the returned data changes from an array format to an object format, where the object's key is the column name and the value is the data, as shown below:

Insert data return example

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
        {
            "affected_rows": 1
        }
    ],
    "rows": 1
}
```

Data Query Return Example

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
        ],
        [
            "groupid",
            "INT",
            4
        ],
        [
            "location",
            "VARCHAR",
            24
        ]
    ],
    "data": [
        {
            "ts": "2017-07-14T02:40:00.000Z",
            "current": -2.498076,
            "voltage": 0,
            "phase": -0.846025,
            "groupid": 8,
            "location": "California.Sunnyvale"
        }
    ],
    "rows": 1
}
```

## Custom Authorization Code

HTTP requests need to include an authorization code `<TOKEN>`, used for identity verification. The authorization code is usually provided by the administrator and can be simply obtained by sending an `HTTP GET` request as follows:

```shell
curl http://<fqnd>:<port>/rest/login/<username>/<password>
```

Here, `fqdn` is the FQDN or IP address of the TDengine database, `port` is the port number of the TDengine service, `username` is the database username, and `password` is the database password. The return is in JSON format, with the fields meaning as follows:

- code: Return code.
- desc: Authorization code.

Example of obtaining an authorization code:

```shell
curl http://192.168.0.1:6041/rest/login/root/taosdata
```

Return value:

```json
{
  "code": 0,
  "desc": "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04"
}
```

## Usage Example

- Query all records of table d1001 in the demo database:

  ```shell
  curl -L -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" -d "select * from demo.d1001" 192.168.0.1:6041/rest/sql
  curl -L -H "Authorization: Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04" -d "select * from demo.d1001" 192.168.0.1:6041/rest/sql
  ```

  Return value:

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

  ```shell
  curl -L -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" -d "create database demo" 192.168.0.1:6041/rest/sql
  curl -L -H "Authorization: Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04" -d "create database demo" 192.168.0.1:6041/rest/sql
  ```

  Return value:

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

## Differences between REST API in TDengine 2.x and 3.0

### URI

| URI                  | TDengine 2.x         | TDengine 3.0                                         |
| :--------------------| :------------------: | :--------------------------------------------------: |
| /rest/sql            | Supported            | Supported (Different response codes and message bodies) |
| /rest/sqlt           | Supported            | No longer supported                                  |
| /rest/sqlutc         | Supported            | No longer supported                                  |

### HTTP code

| HTTP code            | TDengine 2.x         | TDengine 3.0 | Remarks                              |
| :--------------------| :------------------: | :----------: | :-----------------------------------: |
| 200                  | Supported            | Supported    | Correct return and taosc interface error return |
| 400                  | Not supported        | Supported    | Parameter error return               |
| 401                  | Not supported        | Supported    | Authentication failure               |
| 404                  | Supported            | Supported    | Interface does not exist             |
| 500                  | Not supported        | Supported    | Internal error                       |
| 503                  | Supported            | Supported    | Insufficient system resources        |

### Response codes and message bodies

#### TDengine 2.x response codes and message bodies

```json
{
  "status": "succ",
  "head": [
    "name",
    "created_time",
    "ntables",
    "vgroups",
    "replica",
    "quorum",
    "days",
    "keep1,keep2,keep(D)",
    "cache(MB)",
    "blocks",
    "minrows",
    "maxrows",
    "wallevel",
    "fsync",
    "comp",
    "precision",
    "status"
  ],
  "data": [
    [
      "log",
      "2020-09-02 17:23:00.039",
      4,
      1,
      1,
      1,
      10,
      "30,30,30",
      1,
      3,
      100,
      4096,
      1,
      3000,
      2,
      "us",
      "ready"
    ]
  ],
  "rows": 1
}
```

```json
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
```

#### TDengine 3.0 Response Codes and Message Body

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

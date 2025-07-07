---
title: Usage of Special Characters in Passwords
description: Usage of special characters in user passwords in TDengine
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine user passwords must meet the following rules:

1. The username must not exceed 23 bytes.
2. The password length must be between 8 and 255 characters.
3. The range of password characters:
    1. Uppercase letters: `A-Z`
    2. Lowercase letters: `a-z`
    3. Numbers: `0-9`
    4. Special characters: `! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`
4. When strong password is enabled (EnableStrongPassword 1, enabled by default), the password must contain at least three of the following categories: uppercase letters, lowercase letters, numbers, and special characters. When not enabled, there are no restrictions on character types.

## Usage Guide for Special Characters in Different Components

Take the username `user1` and password `Ab1!@#$%^&*()-_+=[]{}` as an example.

```sql
CREATE USER user1 PASS 'Ab1!@#$%^&*()-_+=[]{}';
```

<Tabs defaultValue="shell" groupId="component">
<TabItem label="CLI" value="shell">

In the [TDengine Command Line Interface (CLI)](../../tdengine-reference/tools/tdengine-cli/), note the following:

- If the `-p` parameter is used without a password, you will be prompted to enter a password, and any acceptable characters can be entered.
- If the `-p` parameter is used with a password, and the password contains special characters, single quotes must be used.

Login with user `user1`:

```shell
taos -u user1 -p'Ab1!@#$%^&*()-_+=[]{}'
taos -u user1 -pAb1\!\@\#\$\%\^\&\*\(\)\-\_\+\=\[\]\{\}
```

</TabItem>
<TabItem label="taosdump" value="taosdump">

In [taosdump](../../tdengine-reference/tools/taosdump/), note the following:

- If the `-p` parameter is used without a password, you will be prompted to enter a password, and any acceptable characters can be entered.
- If the `-p` parameter is used with a password, and the password contains special characters, single quotes or escaping must be used.

Backup database `test` with user `user1`:

```shell
taosdump -u user1 -p'Ab1!@#$%^&*()-_+=[]{}' -D test
taosdump -u user1 -pAb1\!\@\#\$\%\^\&\*\(\)\-\_\+\=\[\]\{\} -D test
```

</TabItem>
<TabItem label="Benchmark" value="benchmark">

In [taosBenchmark](../../tdengine-reference/tools/taosbenchmark/), note the following:

- If the `-p` parameter is used without a password, you will be prompted to enter a password, and any acceptable characters can be entered.
- If the `-p` parameter is used with a password, and the password contains special characters, single quotes or escaping must be used.

Example of data write test with user `user1`:

```shell
taosBenchmark -u user1 -p'Ab1!@#$%^&*()-_+=[]{}' -d test -y
```

When using `taosBenchmark -f <JSON>`, there are no restrictions on the password in the JSON file.

</TabItem>
<TabItem label="taosX" value="taosx">

[taosX](../../tdengine-reference/components/taosx/) uses DSN to represent TDengine connections, in the format: `(taos|tmq)[+ws]://<user>:<pass>@<ip>:<port>`, where `<pass>` can contain special characters, such as: `taos+ws://user1:Ab1!@#$%^&*()-_+=[]{}@192.168.10.10:6041`.

Example of exporting data with user `user1`:

```shell
taosx -f 'taos://user1:Ab1!@#$%^&*()-_+=[]{}@localhost:6030?query=select * from test.t1' \
  -t 'csv:./test.csv'
```

Note that if the password can be URL decoded, the URL decoded result will be used as the password. For example: `taos+ws://user1:Ab1%21%40%23%24%25%5E%26%2A%28%29-_%2B%3D%5B%5D%7B%7D@localhost:6041` is equivalent to `taos+ws://user1:Ab1!@#$%^&*()-_+=[]{}@localhost:6041`.

No special handling is required in [Explorer](../../tdengine-reference/components/taosexplorer/), just use it directly.

</TabItem>

<TabItem label="Java" value="java">

When using special character passwords in JDBC, the password needs to be URL encoded, as shown below:

```java
package com.taosdata.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import com.taosdata.jdbc.TSDBDriver;

public class JdbcPassDemo {
 public static void main(String[] args) throws Exception {
  String password = "Ab1!@#$%^&*()-_+=[]{}";
  String encodedPassword = URLEncoder.encode(password, StandardCharsets.UTF_8.toString());
  String jdbcUrl = "jdbc:TAOS-WS://localhost:6041";
  Properties connProps = new Properties();
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_USER, "user1");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, encodedPassword);
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

  try (Connection conn = DriverManager.getConnection(jdbcUrl, connProps)) {
   System.out.println("Connected to " + jdbcUrl + " successfully.");

   // you can use the connection for execute SQL here

  } catch (Exception ex) {
   // please refer to the JDBC specifications for detailed exceptions info
   System.out.printf("Failed to connect to %s, %sErrMessage: %s%n",
     jdbcUrl,
     ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
     ex.getMessage());
   // Print stack trace for context in examples. Use logging in production.
   ex.printStackTrace();
   throw ex;
  }
 }
}
```

</TabItem>
<TabItem label="Python" value="python">

No special handling is required for special character passwords in Python, as shown below:

```python
import taos
import taosws


def create_connection():
    host = "localhost"
    port = 6030
    return taos.connect(
        user="user1",
        password="Ab1!@#$%^&*()-_+=[]{}",
        host=host,
        port=port,
    )

def create_ws_connection():
    host = "localhost"
    port = 6041
    return taosws.connect(
        user="user1",
        password="Ab1!@#$%^&*()-_+=[]{}",
        host=host,
        port=port,
    )


def show_databases(conn):
    cursor = conn.cursor()
    cursor.execute("show databases")
    print(cursor.fetchall())
    cursor.close()


if __name__ == "__main__":
    print("Connect with native protocol")
    conn = create_connection()
    show_databases(conn)
    print("Connect with websocket protocol")
    conn = create_ws_connection()
    show_databases(conn)

```

</TabItem>

<TabItem label="Go" value="go">

Starting from version 3.6.0, Go supports passwords containing special characters, which need to be encoded using encodeURIComponent.

```go
package main

import (
 "database/sql"
 "fmt"
 "log"
 "net/url"

 _ "github.com/taosdata/driver-go/v3/taosWS"
)

func main() {
 var user = "user1"
 var password = "Ab1!@#$%^&*()-_+=[]{}"
 var encodedPassword = url.QueryEscape(password)
 var taosDSN = user + ":" + encodedPassword + "@ws(localhost:6041)/"
 taos, err := sql.Open("taosWS", taosDSN)
 if err != nil {
  log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
 }
 fmt.Println("Connected to " + taosDSN + " successfully.")
 defer taos.Close()
}
```

</TabItem>

<TabItem label="Rust" value="rust">

In Rust, DSN is used to represent TDengine connections, in the format: `(taos|tmq)[+ws]://<user>:<pass>@<ip>:<port>`, where `<pass>` can contain special characters, such as: `taos+ws://user1:Ab1!@#$%^&*()-_+=[]{}@192.168.10.10:6041`.

```rust
let dsn = "taos+ws://user1:Ab1!@#$%^&*()-_+=[]{}@localhost:6041";
let connection = TaosBuilder::from_dsn(&dsn)?.build().await?;
```

</TabItem>
<TabItem label="Node.js" value="node">

Starting from version 3.1.5, the Node.js connector supports passwords containing all valid characters.

```js
const taos = require("@tdengine/websocket");

let dsn = 'ws://localhost:6041';
async function createConnect() {
    try {
        let conf = new taos.WSConfig(dsn);
        conf.setUser('user1');
        conf.setPwd('Ab1!@#$%^&*()-_+=[]{}');
        conf.setDb('test');
        conn = await taos.sqlConnect(conf);
        console.log("Connected to " + dsn + " successfully.");
        return conn;
    } catch (err) {
        console.log("Connection failed with code: " + err.code + ", message: " + err.message);
        throw err;
    }
}

createConnect()
```

</TabItem>
<TabItem label="C#" value="csharp">

When using passwords in C#, note that connection strings do not support semicolons (as semicolons are delimiters). In this case, you can construct the `ConnectionStringBuilder` without a password, and then set the username and password.

As shown below:

```csharp
var builder = new ConnectionStringBuilder("host=localhost;port=6030");
builder.Username = "user1";
builder.Password = "Ab1!@#$%^&*()-_+=[]{}";
using (var client = DbDriver.Open(builder)){}
```

</TabItem>
<TabItem label="C" value="c">

There are no restrictions on passwords in C.

```c
TAOS *taos = taos_connect("localhost", "user1", "Ab1!@#$%^&*()-_+=[]{}", NULL, 6030);
```

</TabItem>
<TabItem label="REST" value="rest">

When using passwords in REST API, note the following:

- Passwords use Basic Auth, in the format `Authorization: Basic base64(<user>:<pass>)`.
- Passwords containing colons `:` are not supported.

The following two methods are equivalent:

```shell
curl -u'user1:Ab1!@#$%^&*()-_+=[]{}' \
  -d 'show databases' http://localhost:6041/rest/sql
curl -H 'Authorization: Basic dXNlcjE6QWIxIUAjJCVeJiooKS1fKz1bXXt9' \
  -d 'show databases' http://localhost:6041/rest/sql
```

</TabItem>
<TabItem label="ODBC" value="odbc">

No special handling is required for special character passwords in ODBC connector, as shown below:

```c
#include <stdio.h>
#include <stdlib.h>
#include <sql.h>
#include <sqlext.h>

int test_user_connect(const char *dsn, const char *uid, const char *pwd) {
  SQLHENV   env   = SQL_NULL_HENV;
  SQLHDBC   dbc   = SQL_NULL_HDBC;
  SQLHSTMT  stmt  = SQL_NULL_HSTMT;
  SQLRETURN sr    = SQL_SUCCESS;

  sr = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

  sr = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

  sr = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

  sr = SQLConnect(dbc, (SQLCHAR *)dsn, SQL_NTS, (SQLCHAR *)uid, SQL_NTS, (SQLCHAR *)pwd, SQL_NTS);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

  sr = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

  sr = SQLExecDirect(stmt, (SQLCHAR *)"show databases", SQL_NTS);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO)
    goto end;

end:
  if (stmt != SQL_NULL_HSTMT) {
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    stmt = SQL_NULL_HSTMT;
  }

  if (dbc != SQL_NULL_HDBC) {
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    dbc = SQL_NULL_HDBC;
  }

  if (env != SQL_NULL_HENV) {
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    env = SQL_NULL_HENV;
  }

  return (sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO) ? 0 : -1;
}

int main() {
  int result = test_user_connect("TAOS_ODBC_WS_DSN", "user1", "Ab1!@#$%^&*()-_+=[]{}");
  printf("test case test_user_connect %s\n", !result ? "pass" : "failed");
  return 0;
}
```

</TabItem>
</Tabs>

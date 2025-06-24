---
title: 密码中特殊字符的使用
description: TDengine 用户密码中特殊字符的使用
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine 用户密码需满足以下规则：

1. 用户名最长不超过 23 个字节。
2. 密码长度必须为 8 到 255 位。
3. 密码字符的取值范围
    1. 大写字母：`A-Z`
    2. 小写字母：`a-z`
    3. 数字：`0-9`
    4. 特殊字符： `! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`
4. 强密码启用（EnableStrongPassword 1，默认开启）时，至少包含大写字母、小写字母、数字、特殊字符中的三类，不启用时，字符种类不做约束。

## 各组件特殊字符使用指南

以用户名 `user1`，密码 `Ab1!@#$%^&*()-_+=[]{}` 为例。

```sql
CREATE USER user1 PASS 'Ab1!@#$%^&*()-_+=[]{}';
```

<Tabs defaultValue="shell" groupId="component">
<TabItem label="CLI" value="shell">

在 [TDengine 命令行客户端（CLI）](../../reference/tools/taos-cli/) 中使用需要注意以下几点：

- 使用参数 `-p` 后不带密码，会提示输入密码，可输入任意可接收字符。
- 使用参数 `-p` 后带密码，如果密码中包含特殊字符，需使用单引号。

使用用户 `user1` 登录：

```shell
taos -u user1 -p'Ab1!@#$%^&*()-_+=[]{}'
taos -u user1 -pAb1\!\@\#\$\%\^\&\*\(\)\-\_\+\=\[\]\{\}
```

</TabItem>
<TabItem label="taosdump" value="taosdump">

在 [taosdump](../../reference/tools/taosdump/) 中使用需要注意以下几点：

- 使用参数 `-p` 后不带密码，会提示输入密码，可输入任意可接收字符。
- 使用参数 `-p` 后带密码，如果密码中包含特殊字符，需使用单引号或进行转义。

使用用户 `user1` 备份数据库 `test`：

```shell
taosdump -u user1 -p'Ab1!@#$%^&*()-_+=[]{}' -D test
taosdump -u user1 -pAb1\!\@\#\$\%\^\&\*\(\)\-\_\+\=\[\]\{\} -D test
```

</TabItem>
<TabItem label="Benchmark" value="benchmark">

在 [taosBenchmark](../../reference/tools/taosbenchmark/) 中使用需要注意以下几点：

- 使用参数 `-p` 后不带密码，会提示输入密码，可输入任意可接收字符。
- 使用参数 `-p` 后带密码，如果密码中包含特殊字符，需使用单引号或进行转义。

使用用户 `user1` 进行数据写入测试示例如下：

```shell
taosBenchmark -u user1 -p'Ab1!@#$%^&*()-_+=[]{}' -d test -y
```

使用 `taosBenchmark -f <JSON>` 方式时，JSON 文件中密码使用无限制。

</TabItem>
<TabItem label="taosX" value="taosx">

[taosX](../../reference/components/taosx/) 使用 DSN 表示 TDengine 连接，使用如下格式：`(taos|tmq)[+ws]://<user>:<pass>@<ip>:<port>`，其中 `<pass>` 可以包含特殊字符，如：`taos+ws://user1:Ab1!@#$%^&*()-_+=[]{}@192.168.10.10:6041`。

使用用户 `user1` 导出数据示例如下：

```shell
taosx -f 'taos://user1:Ab1!@#$%^&*()-_+=[]{}@localhost:6030?query=select * from test.t1' \
  -t 'csv:./test.csv'
```

需要注意的是，如果密码可被 URL decode，则会使用 URL decoded 结果作为密码。如：`taos+ws://user1:Ab1%21%40%23%24%25%5E%26%2A%28%29-_%2B%3D%5B%5D%7B%7D@localhost:6041` 与 `taos+ws://user1:Ab1!@#$%^&*()-_+=[]{}@localhost:6041` 是等价的。

在 [Explorer](../../reference/components/explorer/) 中无需特殊处理，直接使用即可。

</TabItem>

<TabItem label="Java" value="java">

在 JDBC 中使用特殊字符密码时，密码需要通过 URL 编码，示例如下：

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

在 Python 中使用特殊字符密码无需特殊处理，示例如下：

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

从 3.6.0 版本开始，Go 语言中支持密码中包含特殊字符，使用时需要 encodeURIComponent 编码。

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

Rust 中使用 DSN 表示 TDengine 连接，使用如下格式：`(taos|tmq)[+ws]://<user>:<pass>@<ip>:<port>`，其中 `<pass>` 可以包含特殊字符，如：`taos+ws://user1:Ab1!@#$%^&*()-_+=[]{}@192.168.10.10:6041`。

```rust
let dsn = "taos+ws://user1:Ab1!@#$%^&*()-_+=[]{}@localhost:6041";
let connection = TaosBuilder::from_dsn(&dsn)?.build().await?;
```

</TabItem>
<TabItem label="Node.js" value="node">

从 3.1.5 版本开始，Node.js 连接器支持密码中包含特殊字符无需特殊处理。

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

在 C# 中使用密码时，需要注意：使用连接字符串时，不支持分号（因分号为分隔符）；此时可使用不带密码的字符串构建 `ConnectionStringBuilder`，之后再设置用户名和密码。

示例如下：

```csharp
var builder = new ConnectionStringBuilder("host=localhost;port=6030");
builder.Username = "user1";
builder.Password = "Ab1!@#$%^&*()-_+=[]{}";
using (var client = DbDriver.Open(builder)){}
```

</TabItem>
<TabItem label="C" value="c">

C 语言中使用密码无限制。

```c
TAOS *taos = taos_connect("localhost", "user1", "Ab1!@#$%^&*()-_+=[]{}", NULL, 6030);
```

</TabItem>
<TabItem label="REST" value="rest">

REST API 中使用密码时，需要注意以下几点：

- 密码使用 Basic Auth，格式为 `Authorization: Basic base64(<user>:<pass>)`。
- 不支持密码中包含冒号 `:`。

以下两种方式等价：

```shell
curl -u'user1:Ab1!@#$%^&*()-_+=[]{}' \
  -d 'show databases' http://localhost:6041/rest/sql
curl -H 'Authorization: Basic dXNlcjE6QWIxIUAjJCVeJiooKS1fKz1bXXt9' \
  -d 'show databases' http://localhost:6041/rest/sql
```

</TabItem>
<TabItem label="ODBC" value="odbc">

ODBC 连接器支持密码中包含特殊字符，无需特殊处理。示例如下：

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

# C/C++ 连接器-Function Spec

## 1. 变更历史

| 编写日期 | 发布日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-05 | 2025-01-05 | 1.0 | 任新胜 | 第一次安可送审 |
| 2026-01-10 | 2025-01-10 | 1.1 | 廖浩均 | 重构文档 |

## 2. 背景

本文的目标是设计并在此基础上实现高效、易用的 C/C++ 连接器，使得开发者可以通过该连接器与 TDengine 数据库进行高效的读写操作。连接器应当提供基本的连接管理、查询执行、错误处理等功能，并支持高并发的时序数据操作。

## 3. 定义

### 3.1 基本概念

**原生连接（native connect）：**通过客户端驱动程序 （libtaos.so）直接与服务端程序 taosd 建立连接
**REST连接**：通过 taosAdapter 组件提供的 REST API 建立与 taosd 的连接
**WebSocket 连接**：通过 taosAdapter 组件提供的 WebSocket API 建立与 taosd 的连接
**taosAdapter: **接收用户应用的各种请求，并通过标准化接口将用户应用与数据库系统解耦合的无状态服务节点，在获得查询结果后将其转换为 json 或 二进制结果返回给应用。从而实现用户应用于数据库系统的解耦合。
**taosc: **taos client 的简写，指的是 TDengine 的 C/C++ 连接器。
**参数绑定：**指通过 sql 写入数据时，sql 中具体值的位置用特殊符号代替，提前完成语法解析，后续可以在此基础上重复使用相同格式的 sql , 给占位符号赋予具体值，反复执行写入操作。可以通过参数绑定减少语法解析过程耗时，大幅提高写入速度。
**无模式写入 ：**用户无须预先创建超级表或子表，根据实际写入的数据自动创建相应的存储结构。
**数据订阅 ：**允许客户端实时接收数据库中的数据变化，适用于实时监控场景。

### 3.2 数据定义

**连接句柄：**void * 类型的结构体指针，连接函数 `taos_connect` 的返回值，当前连接标识符，用于后续查询。
**结果集句柄：**void * 类型的结构体指针，查询执行函数`taos_query`返回的结果指针，可进一步通过该指针获取查询结果。
**行数据：**TAOS_ROW 命名的 void **类型，可由 taos_fetch_row 函数在结果集上获取到，根据列描述能在行数据上获取每一列的具体值。
**结果集列描述:  **查询结果的每一列数据定义，可由 taos_fetch_fields 函数在结果集上获取到，根据列描述可以在行数据上获取每一列的具体值。列描述定义如下：
```c
typedef struct taosField {
  char    name[65];
  int8_t  type;
  int32_t bytes;
} TAOS_FIELD;
```

**STMT句柄：**用于参数绑定方式写入的对象句柄，该句柄在执行过程中关联执行过程的上下文。

## 4. 行为说明

### 4.1 环境准备

#### 4.1.1 安装

如果应用程序需要客户端驱动（libtaos.so/taosc）才能与服务端进行连接，如果安装了服务端，默认会同时安装客户端驱动。如果在未部署服务器的节点上运行应用程序，需要单独安装客户端驱动以便于应用程序能够正常调用服务`API`，需要注意的是为了避免客户端驱动和服务端不兼容，需要使用一致的版本。

#### 4.1.2 配置

编辑 `taos.cfg` 文件（默认路径/etc/taos/taos.cfg），将 `firstEP` 修改为 TDengine 服务器的 End Point，例如：`h1.tdengine.com:6030`

#### 4.1.3 检查

在 `shell` 下直接执行 `taos` 连接到 TDengine 服务，进入到 TDengine CLI 界面，示例如下：
```plaintext
$ taos
taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 db                             |
Query OK, 3 rows in database (0.019154s)
taos>
```

### 4.2 连接管理

#### 4.2.1 初始化与退出

1. taos_init
**函数原型：**`int  taos_init(void);`
**函数说明：**初始化连接器运行需要的基本资源和配置，进程运行期间只需调用一次，重复调用不会有实际意义。taos_connect 时默认调用了`taos_init`，因此可以省略该函数的调用。
1. taos_cleanup
**函数原型：**`void taos_cleanup(void);`
**函数说明：**释放`taos_init` 申请的资源，进程不再使用 taos c/c++ 连接器时使用，以释放连接器占用的资源。

#### 4.2.2 连接与释放

1. taos_connect
**函数原型：**`TAOS *taos_connect(const char *host, const char *user, const char *pass, const char *db, uint16_t port);`
**函数说明：**用于建立与 TDengine 数据库的连接。其参数详细说明如下：
- `host`：要连接的数据库服务器的主机名或IP地址。如果是本地数据库，可以使用 `"``localhost``"`。
- `user`：用于登录数据库的用户名。
- `passwd`：与用户名对应的密码。
- `db`：连接时默认选择的数据库名。如果不指定数据库，可以传递 `NULL` 或空字符串。
- `port`：数据库服务器监听的端口号。默认的端口号是 `6030`。
1. taos_close
**函数原型：**`void       taos_close(TAOS *taos);`
**函数说明：**断开与 TDengine 数据库的连接，并释放相应资源。

#### 4.2.3 连接示例代码

```c
// compile with
// gcc connect_example.c -o connect_example -ltaos
#include <stdio.h>
#include <stdlib.h>
#include "taos.h"

int main() {
  const char *host = "localhost";
  const char *user = "root";
  const char *passwd = "taosdata";
  const char *db = NULL;      // if don't want to connect to a default db, set it to NULL or ""
  uint16_t    port = 6030;    // 0 means use the default port
  TAOS       *taos = taos_connect(host, user, passwd, db, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL), taos_errstr(NULL));
    taos_cleanup();
    return -1;
  }
  fprintf(stdout, "Connected to %s:%hu successfully.\n", host, port);
  
  /* put your code here for read and write */

  // close & clean
  taos_close(taos);
  taos_cleanup();
}

```

### 4.3 配置管理

taos_options
**函数原型：**`taos_options(TSDB_OPTION option, const void *arg, ...)`
**函数说明：**配置客户端连接时使用的参数，`TSDB_OPTION` 为整型枚举值，表示配置的类型，支持下边几个参数。其中 TSDB_OPTION_CONFIGDIR 参数管理配置文件路径，而配置文件可以修改所有可配置参数。

| 配置项 | 枚举值 | 说明 | 取值范围 |
| --- | --- | --- | --- |
| locale | 0 | 系统区位信息及编码格式，缺省从系统中获取 | N/A |
| TSDB_OPTION_CONFIGDIR | 3 | 默认配置文件 taos.cfg 的路径 | 文件路径 |
| shellActivityTimer | 4 | 客户端向 mnode 发送心跳的时长，单位为秒 | 取值范围 1-120，默认值 3 |

### 4.4 数据类型

TDengine 支持常用的数据类型，并提供 taos_data_type 函数将 TDengine 的数据类型值转为可以阅读的类型描述。
**函数原型：**`const char *taos_data_type(int type)`
类型定义如下：
```c
#define TSDB_DATA_TYPE_NULL       0   // 1 bytes
#define TSDB_DATA_TYPE_BOOL       1   // 1 bytes
#define TSDB_DATA_TYPE_TINYINT    2   // 1 byte
#define TSDB_DATA_TYPE_SMALLINT   3   // 2 bytes
#define TSDB_DATA_TYPE_INT        4   // 4 bytes
#define TSDB_DATA_TYPE_BIGINT     5   // 8 bytes
#define TSDB_DATA_TYPE_FLOAT      6   // 4 bytes
#define TSDB_DATA_TYPE_DOUBLE     7   // 8 bytes
#define TSDB_DATA_TYPE_VARCHAR    8   // string, alias for varchar
#define TSDB_DATA_TYPE_TIMESTAMP  9   // 8 bytes
#define TSDB_DATA_TYPE_NCHAR      10  // unicode string
#define TSDB_DATA_TYPE_UTINYINT   11  // 1 byte
#define TSDB_DATA_TYPE_USMALLINT  12  // 2 bytes
#define TSDB_DATA_TYPE_UINT       13  // 4 bytes
#define TSDB_DATA_TYPE_UBIGINT    14  // 8 bytes
#define TSDB_DATA_TYPE_JSON       15  // json string
#define TSDB_DATA_TYPE_VARBINARY  16  // binary
#define TSDB_DATA_TYPE_DECIMAL    17  // decimal
#define TSDB_DATA_TYPE_BLOB       18  // binary
#define TSDB_DATA_TYPE_MEDIUMBLOB 19
#define TSDB_DATA_TYPE_BINARY     TSDB_DATA_TYPE_VARCHAR  // string
#define TSDB_DATA_TYPE_GEOMETRY   20                      // geometry
```

### 4.5 sql 执行与结果获取（同步）

#### 4.5.1 sql 执行

1. taos_query
**函数原型：**`TAOS_RES *taos_query(TAOS *taos, const char *sql);`
**函数说明：**在一个 TDengine 的连接上同步方式执行一条 sql 语句并返回结果句柄。

#### 4.5.2 结果获取

1. taos_fetch_row
**函数原型：**`TAOS_ROW taos_fetch_row(TAOS_RES *res);`
**函数说明：**在一个查询结果集上获取下一个有效的结果行，一般循环调用。当返回为 NULL 时，表示已经遍历获取了所有的结果行。返回的结果 TAOS_ROW 类型是一个指针数组，其中每个元素指向对应行的一个列的结果指针。和 taos_fetch_fields 配合使用可以获取结果的每行每列具体值。
1. taos_fetch_fields
**函数原型：**`TAOS_FIELD *taos_fetch_fields(TAOS_RES *res);`
**函数说明：**获取结果集中每个数据列的定义，根据列描述可以在行数据上获取每一列的具体值。列描述定义如下：
```c
typedef struct taosField {
  char    name[65];
  int8_t  type;
  int32_t bytes;
} TAOS_FIELD;
```

1. taos_field_count
**函数原型：**`int  taos_field_count(TAOS_RES *res);`
**函数说明：**获取结果集返回数据的列数。
1. taos_affected_rows64
**函数原型：**`int  taos_affected_rows64(TAOS_RES *res);`
**函数说明：**返回 sql 执行后影响的数据行数。注意，影响的数据函数不是返回数据的行数。
1. taos_result_precision
**函数原型：**`int  taos_result_precision(TAOS_RES *res);`
**函数说明：**获取返回结果中时间列的精度。具体值如下：

| 精度定义 | 枚举值 | 说明 |
| --- | --- | --- |
| TSDB_TIME_PRECISION_MILLI | 0 | ms |
| TSDB_TIME_PRECISION_MICRO | 1 | us |
| TSDB_TIME_PRECISION_NANO | 2 | ns |

#### 4.5.3 释放资源

1. taos_free_result
sql 语句执行完成后，需要调用 `taos_free_result` 释放执行 SQL语句占用的资源，否则应用程序会一直占有该资源。
**函数原型：**`void     taos_free_result(TAOS_RES *res);`

#### 4.5.4 同步执行示例

##### 4.5.4.1 同步查询

以下代码是同步查询的示例代码，执行完 `taos_query` 后，使用 `taos_fetch_row` 遍历查询结果的每一行，并对每一行结果中的每一列数据，根据每列的类型进行处理。列数和每列类型是由 `taos_field_count` 和`taos_fetch_fields`获取。
```c
const char *host = "localhost";
const char *user = "root";
const char *password = "taosdata";
uint16_t    port = 6030;
int         code = 0;

// connect
TAOS *taos = taos_connect(host, user, password, NULL, port);
if (taos == NULL) {
  fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
         taos_errstr(NULL));
  taos_cleanup();
  return -1;
}

// query data, please make sure the database and table are already created
const char *sql = "SELECT ts, current, location FROM power.meters limit 100";
TAOS_RES   *result = taos_query(taos, sql);
code = taos_errno(result);
if (code != 0) {
  fprintf(stderr, "Failed to query data from power.meters, sql: %s, ErrCode: 0x%x, ErrMessage: %s\n.", sql, code,
         taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}

TAOS_ROW    row = NULL;
int         rows = 0;
int         num_fields = taos_field_count(result);
TAOS_FIELD *fields = taos_fetch_fields(result);

fprintf(stdout, "query successfully, got %d fields, the sql is: %s.\n", num_fields, sql);

while ((row = taos_fetch_row(result))) {
    printf("taos_fetch_row success!\n");

    numOfRows++;
    temp[0] = 0;
    for (int i = 0; i < num_fields; i++) {
        switch (fields[i].type) {
            case TSDB_DATA_TYPE_TINYINT:
                sprintf(temp + strlen(temp), "%d ", *((char *) row[i]));
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                sprintf(temp + strlen(temp), "%d ", *((short *) row[i]));
                break;
            case TSDB_DATA_TYPE_INT:
                sprintf(temp + strlen(temp), "%d ", *((int *) row[i]));
                break;
            case TSDB_DATA_TYPE_BIGINT:
                sprintf(temp + strlen(temp), "%ld ", *((long *) row[i]));
                break;
            case TSDB_DATA_TYPE_FLOAT:
                sprintf(temp + strlen(temp), "%f ", *((float *) row[i]));
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                sprintf(temp + strlen(temp), "%lf ", *((double *) row[i]));
                break;
            case TSDB_DATA_TYPE_BINARY:
                sprintf(temp + strlen(temp), "%s ", (char *) row[i]);
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
                sprintf(temp + strlen(temp), "%ld ", *((long *) row[i]));
                break;
            default:
                break;
        }
    }
fprintf(stdout, "total rows: %d\n", rows);
taos_free_result(result);

// close & clean
taos_close(taos);
taos_cleanup();
return 0;
```

##### 4.5.4.2 同步写入

写入 sql 执行结束后，可以使用 `taos_errno` 检查是否正确执行，也可以通过 `taos_affected_rows` 查看写入的行数。
```c
const char *host = "localhost";
const char *user = "root";
const char *password = "taosdata";
uint16_t    port = 6030;
int         code = 0;

// connect
TAOS *taos = taos_connect(host, user, password, NULL, port);
if (taos == NULL) {
  fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
         taos_errstr(NULL));
  taos_cleanup();
  return -1;
}

// insert data, please make sure the database and table are already created
const char *sql =
    "INSERT INTO "
    "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') "
    "VALUES "
    "(NOW + 1a, 10.30000, 219, 0.31000) "
    "(NOW + 2a, 12.60000, 218, 0.33000) "
    "(NOW + 3a, 12.30000, 221, 0.31000) "
    "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') "
    "VALUES "
    "(NOW + 1a, 10.30000, 218, 0.25000) ";
TAOS_RES *result = taos_query(taos, sql);
code = taos_errno(result);
if (code != 0) {
  fprintf(stderr, "Failed to insert data to power.meters, sql: %s, ErrCode: 0x%x, ErrMessage: %s\n.", sql, code, taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}
// you can check affectedRows here
int rows = taos_affected_rows(result);
fprintf(stdout, "Successfully inserted %d rows into power.meters.\n", rows);

taos_free_result(result);

// close & clean
taos_close(taos);
taos_cleanup();
return 0;
```

### 4.6 异步执行查询

为了更好的利用系统资源，提高查询效率，还提供高并发执行的异步接口，应用程序可以非阻塞的方式并发执行 sql 语句并获取执行结果。
对应同步方式的 `taos_query` 和 `taos_fetch_rows` 接口，异步接口分别是: `taos_query_a` 和 `taos_fetch_rows_a`，两个接口的详细说明如下：

#### 4.6.1 执行 SQL 语句

**函数原型：**`taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param);`
**函数说明：**在一个 TDengine 的连接上异步方式执行一条 sql 语句。fp 为查询结果的回调函数，param 传递给回调函数的自定义参数指针，可用于传递自定义数据。

#### 4.6.2 获取结果

异步执行时，获取结果的过程可以在 `taos_query_a` 的 fp 参数指定的函数过程中实现，在回调函数中通过调用 `taos_fetch_rows_a` 可以获取每行结果，同样，在 `taos_fetch_rows_a` 的回调中获取每列数据
**函数原型：**`taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param);`
**函数说明：**`taos_query_a` 调用后，在 fp 回调函数中获取每行结果，并在 `taos_fetch_rows_a` 的回调中继续获取每列数据。

### 4.7 参数绑定写入

#### 4.7.1 基本介绍

通过参数绑定方式写入数据时，能避免SQL语法解析的资源消耗，从而显著提升写入性能。参数绑定能提高写入效率的原因主要有以下几点：
- 减少解析时间：通过参数绑定，SQL 语句的结构在第一次执行时就已经确定，后续的执行只需要替换参数值，这样可以避免每次执行时都进行语法解析，从而减少解析时间。
- 预编译：当使用参数绑定时，SQL 语句可以被预编译并缓存，后续使用不同的参数值执行时，可以直接使用预编译的版本，提高执行效率。
- 减少网络开销：参数绑定还可以减少发送到数据库的数据量，因为只需要发送参数值而不是完整的 SQL 语句，特别是在执行大量相似的插入或更新操作时，这种差异尤为明显。

#### 4.7.2 接口说明

`TAOS_STMT` 系列函数用于处理 TAOS 数据库的预编译 SQL 语句。以下是这些函数的详细说明：

##### 4.7.2.1 初始化和关闭

1. `TAOS_STMT *taos_stmt_init(TAOS *taos)` 初始化一个新的 `TAOS_STMT` 对象。
2. `TAOS_STMT *taos_stmt_init_with_reqid(TAOS *taos, int64_t reqid)` 使用指定的请求 ID 初始化一个新的 `TAOS_STMT` 对象。
3. `TAOS_STMT *taos_stmt_init_with_options(TAOS *taos, TAOS_STMT_OPTIONS *options)` 使用指定的选项初始化一个新的 `TAOS_STMT` 对象。
4. `int taos_stmt_close(TAOS_STMT *stmt)` 关闭并释放 `TAOS_STMT` 对象。

##### 4.7.2.2 预编译和设置

1. `int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length)` 预编译 SQL 语句。
2. `int taos_stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags)` 设置表名和标签。
3. `int taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name)` 设置表名。
4. `int taos_stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags)` 设置标签。
5. `int taos_stmt_set_sub_tbname(TAOS_STMT *stmt, const char *name)` 设置子表名。

##### 4.7.2.3 获取字段信息

1. `int taos_stmt_get_tag_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields)` 获取标签字段信息。
2. `int taos_stmt_get_col_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields)` 获取列字段信息。
3. `void taos_stmt_reclaim_fields(TAOS_STMT *stmt, TAOS_FIELD_E *fields)` 回收由 `taos_stmt_get_tag_fields` 或 `taos_stmt_get_col_fields` 分配的字段。

##### 4.7.2.4 参数绑定

1. `int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) `绑定参数。
2. `int taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind)` 批量绑定参数。
3. `int taos_stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx)` 批量绑定单个参数。

##### 4.7.2.5 执行和结果

1. `int taos_stmt_add_batch(TAOS_STMT *stmt)` 添加批处理。
2. `int taos_stmt_execute(TAOS_STMT *stmt)` 执行预编译语句。
3. `TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt)` 获取执行结果。

##### 4.7.2.6 错误处理

1. `char *taos_stmt_errstr(TAOS_STMT *stmt)` 获取错误字符串。

##### 4.7.2.7 其他

1. `int taos_stmt_is_insert(TAOS_STMT *stmt, int *insert)` 检查是否为插入语句。
2. `int taos_stmt_num_params(TAOS_STMT *stmt, int *nums)` 获取参数数量。
3. `int taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes)` 获取参数信息。
4. `int taos_stmt_affected_rows(TAOS_STMT *stmt)` 获取受影响的行数。
5. `int taos_stmt_affected_rows_once(TAOS_STMT *stmt)` 获取一次操作中受影响的行数。
6. 这些函数提供了丰富的接口，用于处理 TAOS 数据库的预编译 SQL 语句，支持参数绑定、批处理、执行和结果获取等操作。

#### 4.7.3 参数绑定写入示例

```c
/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

// TAOS standard API example. The same syntax as MySQL, but only a subset
// to compile: gcc -o stmt_insert_demo stmt_insert_demo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "taos.h"

/**
 * @brief execute sql only.
 *
 * @param taos
 * @param sql
 */
void executeSQL(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int       code = taos_errno(res);
  if (code != 0) {
    fprintf(stderr, "%s\n", taos_errstr(res));
    taos_free_result(res);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }
  taos_free_result(res);
}

/**
 * @brief check return status and exit program when error occur.
 *
 * @param stmt
 * @param code
 * @param msg
 */
void checkErrorCode(TAOS_STMT *stmt, int code, const char *msg) {
  if (code != 0) {
    fprintf(stderr, "%s. code: %d, error: %s\n", msg,code,taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }
}

typedef struct {
  int64_t ts;
  float   current;
  int     voltage;
  float   phase;
} Row;

int num_of_sub_table = 10;
int num_of_row = 10;
int total_affected = 0;
/**
 * @brief insert data using stmt API
 *
 * @param taos
 */
void insertData(TAOS *taos) {
  // init
  TAOS_STMT *stmt = taos_stmt_init(taos);
  if (stmt == NULL) {
      fprintf(stderr, "Failed to init taos_stmt, error: %s\n", taos_stmt_errstr(NULL));
      exit(EXIT_FAILURE);
  }
  // prepare
  const char *sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)";
  int         code = taos_stmt_prepare(stmt, sql, 0);
  checkErrorCode(stmt, code, "Failed to execute taos_stmt_prepare");
  for (int i = 1; i <= num_of_sub_table; i++) {
    char table_name[20];
    sprintf(table_name, "d_bind_%d", i);
    char location[20];
    sprintf(location, "location_%d", i);

    // set table name and tags
    TAOS_MULTI_BIND tags[2];
    // groupId
    tags[0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[0].buffer_length = sizeof(int);
    tags[0].length = (int32_t *)&tags[0].buffer_length;
    tags[0].buffer = &i;
    tags[0].is_null = NULL;
    tags[0].num = 1;
    // location
    tags[1].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[1].buffer_length = strlen(location);
    tags[1].length =(int32_t *) &tags[1].buffer_length;
    tags[1].buffer = location;
    tags[1].is_null = NULL;
    tags[1].num = 1;
    code = taos_stmt_set_tbname_tags(stmt, table_name, tags);
    checkErrorCode(stmt, code, "Failed to set table name and tags\n");

    // insert rows
    TAOS_MULTI_BIND params[4];
    // ts
    params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[0].buffer_length = sizeof(int64_t);
    params[0].length = (int32_t *)&params[0].buffer_length;
    params[0].is_null = NULL;
    params[0].num = 1;
    // current
    params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[1].buffer_length = sizeof(float);
    params[1].length = (int32_t *)&params[1].buffer_length;
    params[1].is_null = NULL;
    params[1].num = 1;
    // voltage
    params[2].buffer_type = TSDB_DATA_TYPE_INT;
    params[2].buffer_length = sizeof(int);
    params[2].length = (int32_t *)&params[2].buffer_length;
    params[2].is_null = NULL;
    params[2].num = 1;
    // phase
    params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[3].buffer_length = sizeof(float);
    params[3].length = (int32_t *)&params[3].buffer_length;
    params[3].is_null = NULL;
    params[3].num = 1;

    for (int j = 0; j < num_of_row; j++) {
      struct timeval tv;
      gettimeofday(&tv, NULL);
      long long milliseconds = tv.tv_sec * 1000LL + tv.tv_usec / 1000;  // current timestamp in milliseconds
      int64_t   ts = milliseconds + j;
      float     current = (float)rand() / RAND_MAX * 30;
      int       voltage = rand() % 300;
      float     phase = (float)rand() / RAND_MAX;
      params[0].buffer = &ts;
      params[1].buffer = &current;
      params[2].buffer = &voltage;
      params[3].buffer = &phase;
      // bind param
      code = taos_stmt_bind_param(stmt, params);
      checkErrorCode(stmt, code, "Failed to bind param");
    }
    // add batch
    code = taos_stmt_add_batch(stmt);
    checkErrorCode(stmt, code, "Failed to add batch");
    // execute batch
    code = taos_stmt_execute(stmt);
    checkErrorCode(stmt, code, "Failed to exec stmt");
    // get affected rows
    int affected = taos_stmt_affected_rows_once(stmt);
    total_affected += affected;
  }
  fprintf(stdout, "Successfully inserted %d rows to power.meters.\n", total_affected);
  taos_stmt_close(stmt);
}

int main() {
  const char *host      = "localhost";
  const char *user      = "root";
  const char *password  = "taosdata";
  uint16_t    port      = 6030;
  TAOS *taos = taos_connect(host, user, password, NULL, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL), taos_errstr(NULL));
    taos_cleanup();
    exit(EXIT_FAILURE);
  }
  // create database and table
  executeSQL(taos, "CREATE DATABASE IF NOT EXISTS power");
  executeSQL(taos, "USE power");
  executeSQL(taos,
             "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
             "(groupId INT, location BINARY(24))");
  insertData(taos);
  taos_close(taos);
  taos_cleanup();
}
```

### 4.8 无模式写入

#### 4.8.1 基本介绍

在物联网应用中，为了实现自动化管理、业务分析和设备监控等多种功能，通常需要采集大量的数据项。然而，由于应用逻辑的版本升级和设备自身的硬件调整等原因，数据采集项可能会频繁发生变化。提供无模式（schemaless）写入方式，旨在简化数据记录过程。
采用无模式写入方式，用户无须预先创建超级表或子表，因为 TDengine 会根据实际写入的数据自动创建相应的存储结构。此外，在必要时，无模式写入方式还能自动添加必要的数据列或标签列，确保用户写入的数据能够被正确存储。
值得注意的是，通过无模式写入方式创建的超级表及其对应的子表与通过 SQL 直接创建的超级表和子表在功能上没有区别，用户仍然可以使用 SQL 直接向其中写入数据。无模式写入方式生成的表名是基于标签值按照固定的映射规则生成的，因此表名缺乏可读性，不易于理解和记忆。

#### 4.8.2 协议介绍

无模式写入行协议兼容 InfluxDB 的行协议、OpenTSDB 的 telnet 行协议和 OpenTSDB 的 JSON 格式协议。InfluxDB、OpenTSDB 的标准写入协议请参考各自的官方文档。
下面首先以 InfluxDB 的行协议为基础，介绍 TDengine 扩展的协议内容。该协议允许用户采用更加精细的方式控制（超级表）模式。采用一个字符串来表达一个数据行，可以向写入 API 中一次传入多行字符串来实现多个数据行的批量写入，其格式约定如下。
```plaintext
measurement,tag_set field_set timestamp
```

各参数说明如下。
1. measurement 为数据表名，与 tag_set 之间使用一个英文逗号来分隔。
2. tag_set 格式形如 `<tag_key>=<tag_value>, <tag_key>=<tag_value>`，表示标签列数据，使用英文逗号分隔，与 field_set 之间使用一个半角空格分隔。
3. field_set 格式形如 `<field_key>=<field_value>, <field_key>=<field_value>`，表示普通列，同样使用英文逗号来分隔，与 timestamp 之间使用一个半角空格分隔。
4. timestamp 为本行数据对应的主键时间戳。
5. 无模式写入不支持含第二主键列的表的数据写入。
tag_set 中的所有的数据自动转化为 nchar 数据类型，并不需要使用双引号。 在无模式写入数据行协议中，field_set 中的每个数据项都需要对自身的数据类型进行描述，具体要求如下。
1. 如果两边有英文双引号，表示 varchar 类型，例如 "abc"。
2. 如果两边有英文双引号而且带有 L 或 l 前缀，表示 nchar 类型，例如 L" 报错信息 "。
3. 如果两边有英文双引号而且带有 G 或 g 前缀， 表 示 geometry 类型， 例 如G"Point(4.343 89.342)"。
4. 如果两边有英文双引号而且带有 B 或 b 前缀，表示 varbinary 类型，双引号内可以为 \x 开头的十六进制或者字符串，例如 B"\x98f46e" 和 B"hello"。
5. 对于空格、等号（=）、逗号（,）、双引号（"）、反斜杠（\），前面需要使用反斜杠（\）进行转义（均为英文半角符号）。无模式写入协议的域转义规则如下表所示。
  | 序号 | 域 | 需转义字符 |
| --- | --- | --- |
| 1 | 超级表名 | 逗号，空格 |
| 2 | 标签名 | 逗号，等号，空格 |
| 3 | 标签值 | 逗号，等号，空格 |
| 4 | 列名 | 逗号，等号，空格 |
| 5 | 列值 | 双引号，反斜杠 |

如果使用两个连续的反斜杠，则第1个反斜杠作为转义符，当只有一个反斜杠时则无须转义。无模式写入协议的反斜杠转义规则如下表所示。
| 序号 | 反斜杠 | 转义为 |
| --- | --- | --- |
| 1 | \ | \ |
| 2 | \\ | \ |
| 3 | \\\ | \\ |
| 4 | \\\\ | \\ |
| 5 | \\\\\ | \\\ |
| 6 | \\\\\\ | \\\ |

数值类型将通过后缀来区分数据类型。无模式写入协议的数值类型转义规则如下表所示。
| 序号 | 后缀 | 映射类型 | 大小(字节) |
| --- | --- | --- | --- |
| 1 | 无或 f64 | double | 8 |
| 2 | f32 | float | 4 |
| 3 | i8/u8 | TinyInt/UTinyInt | 1 |
| 4 | i16/u16 | SmallInt/USmallInt | 2 |
| 5 | i32/u32 | Int/UInt | 4 |
| 6 | i64/i/u64/u | BigInt/BigInt/UBigInt/UBigInt | 8 |

- t, T, true, True, TRUE, f, F, false, False 将直接作为 BOOL 型来处理。
例如如下数据行表示：向名为 st 的超级表下的 t1 标签为 "3"（NCHAR）、t2 标签为 "4"（NCHAR）、t3 标签为 "t3"（NCHAR）的数据子表，写入 c1 列为 3（BIGINT）、c2 列为 false（BOOL）、c3 列为 "passit"（BINARY）、c4 列为 4（DOUBLE）、主键时间戳为 1626006833639000000 的一行数据。
```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

需要注意的是，如果描述数据类型后缀时出现大小写错误，或者为数据指定的数据类型有误，均可能引发报错提示而导致数据写入失败。
TDengine 提供数据写入的幂等性保证，即您可以反复调用 API 进行出错数据的写入操作。但是不提供多行数据写入的原子性保证。即在多行数据一批次写入过程中，会出现部分数据写入成功，部分数据写入失败的情况。

#### 4.8.3 无模式写入处理规则

无模式写入按照如下原则来处理行数据：
1. 将使用如下规则来生成子表名：首先将 measurement 的名称和标签的 key 和 value 组合成为如下的字符串
```json
"measurement,tag_key1=tag_value1,tag_key2=tag_value2"
```

1. 如果解析行协议获得的超级表不存在，则会创建这个超级表（不建议手动创建超级表，不然插入数据可能异常）。
2. 如果解析行协议获得子表不存在，则 Schemaless 会按照步骤 1 或 2 确定的子表名来创建子表。
3. 如果数据行中指定的标签列或普通列不存在，则在超级表中增加对应的标签列或普通列（只增不减）。
4. 如果超级表中存在一些标签列或普通列未在一个数据行中被指定取值，那么这些列的值在这一行中会被置为 NULL。
5. 对 BINARY 或 NCHAR 列，如果数据行中所提供值的长度超出了列类型的限制，自动增加该列允许存储的字符长度上限（只增不减），以保证数据的完整保存。
6. 整个处理过程中遇到的错误会中断写入过程，并返回错误代码。
7. 为了提高写入的效率，默认假设同一个超级表中 field_set 的顺序是一样的（第一条数据包含所有的 field，后面的数据按照这个顺序），如果顺序不一样，需要配置参数 smlDataFormat 为 false，否则，数据写入按照相同顺序写入，库中数据会异常，从3.0.3.0开始，自动检测顺序是否一致，该配置废弃。
8. 由于sql建表表名不支持点号（.），所以schemaless也对点号（.）做了处理，如果schemaless自动建表的表名如果有点号（.），会自动替换为下划线（_）。如果手动指定子表名的话，子表名里有点号（.），同样转化为下划线（_）。
9. taos.cfg 增加 smlTsDefaultName 配置（值为字符串），只在client端起作用，配置后，schemaless自动建表的时间列名字可以通过该配置设置。不配置的话，默认为 _ts。
10. 无模式写入的数据超级表或子表名区分大小写。
11. 无模式写入仍然遵循 TDengine 对数据结构的底层限制，例如每行数据的总长度不能超过 48KB（从 3.0.5.0 版本开始为 64KB），标签值的总长度不超过16KB。

#### 4.8.4 时间分辨率识别

无模式写入支持3个指定的模式，如下表所示：
| 序号 | 值 | 说明 |
| --- | --- | --- |
| 1 | SML_LINE_PROTOCOL | InfluxDB 行协议（Line Protocol) |
| 2 | SML_TELNET_PROTOCOL | OpenTSDB 文本行协议 |
| 3 | SML_JSON_PROTOCOL | JSON 协议格式 |

在 SML_LINE_PROTOCOL 解析模式下，需要用户指定输入的时间戳的时间分辨率。可用的时间分辨率如下表所示：
| 序号 | 时间分辨率定义 | 含义 |
| --- | --- | --- |
| 1 | TSDB_SML_TIMESTAMP_NOT_CONFIGURED | 未定义（无效） |
| 2 | TSDB_SML_TIMESTAMP_HOURS | 小时 |
| 3 | TSDB_SML_TIMESTAMP_MINUTES | 分钟 |
| 4 | TSDB_SML_TIMESTAMP_SECONDS | 秒 |
| 5 | TSDB_SML_TIMESTAMP_MILLI_SECONDS | 毫秒 |
| 6 | TSDB_SML_TIMESTAMP_MICRO_SECONDS | 微秒 |
| 7 | TSDB_SML_TIMESTAMP_NANO_SECONDS | 纳秒 |

在 SML_TELNET_PROTOCOL 和 SML_JSON_PROTOCOL 模式下，根据时间戳的长度来确定时间精度（与 OpenTSDB 标准操作方式相同），此时会忽略用户指定的时间分辨率。

#### 4.8.5 数据模式映射规则

InﬂuxDB行协议的数据将被映射成具有模式的数据，其中，measurement映射为超级表名称，tag_set中的标签名称映射为数据模式中的标签名，field_set中的名称映射为列名称。例如下面的数据。
```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

该行数据映射生成一个超级表： st， 其包含了 3 个类型为 nchar 的标签，分别是：t1, t2, t3。五个数据列，分别是 ts（timestamp），c1 (bigint），c3(binary)，c2 (bool), c4 (bigint）。映射成为如下 SQL 语句：
```json
create stable st (_ts timestamp, c1 bigint, c2 bool, c3 binary(6), c4 bigint) tags(t1 nchar(1), t2 nchar(1), t3 nchar(2))
```

#### 4.8.6 数据模式变更处理

本节将说明不同行数据写入情况下，对于数据模式的影响。
在使用行协议写入一个明确的标识的字段类型的时候，后续更改该字段的类型定义，会出现明确的数据模式错误，即会触发写入 API 报告错误。如下所示，
```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4    1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4i   1626006833640000000
```

第一行的数据类型映射将 c4 列定义为 Double， 但是第二行的数据又通过数值后缀方式声明该列为 BigInt， 由此会触发无模式写入的解析错误。
如果列前面的行协议将数据列声明为了 binary， 后续的要求长度更长的 binary 长度，此时会触发超级表模式的变更。
```json
st,t1=3,t2=4,t3=t3 c1=3i64,c5="pass"     1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c5="passit"   1626006833640000000
```

第一行中行协议解析会声明 c5 列是一个 binary(4)的字段，第二次行数据写入会提取列 c5 仍然是 binary 列，但是其宽度为 6，此时需要将 binary 的宽度增加到能够容纳 新字符串的宽度。
```json
st,t1=3,t2=4,t3=t3 c1=3i64               1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c6="passit"   1626006833640000000
```

第二行数据相对于第一行来说增加了一个列 c6，类型为 binary(6)。那么此时会自动增加一个列 c6， 类型为 binary(6)。

#### 4.8.7 无模式写入示例

下面以智能电表为例，介绍各语言连接器使用无模式写入接口写入数据的代码样例，包含了三种协议： InfluxDB 的行协议、OpenTSDB 的 TELNET 行协议和 OpenTSDB 的 JSON 格式协议。
```c
const char *host = "localhost";
const char *user = "root";
const char *password = "taosdata";
uint16_t    port = 6030;
int         code = 0;

// connect
TAOS *taos = taos_connect(host, user, password, NULL, port);
if (taos == NULL) {
  fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
         taos_errstr(NULL));
  taos_cleanup();
  return -1;
}

// create database
TAOS_RES *result = taos_query(taos, "CREATE DATABASE IF NOT EXISTS power");
code = taos_errno(result);
if (code != 0) {
  fprintf(stderr, "Failed to create database power, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}
taos_free_result(result);

// use database
result = taos_query(taos, "USE power");
code = taos_errno(result);
if (code != 0) {
  fprintf(stderr, "Failed to execute use power, ErrCode: 0x%x, ErrMessage: %s\n.", code, taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}
taos_free_result(result);

// schemaless demo data
char *line_demo =
    "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 "
    "1626006833639";
char *telnet_demo = "metric_telnet 1707095283260 4 host=host0 interface=eth0";
char *json_demo =
    "{\"metric\": \"metric_json\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, "
    "\"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}";

// influxdb line protocol
char *lines[] = {line_demo};
result = taos_schemaless_insert(taos, lines, 1, TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
code = taos_errno(result);
if (code != 0) {
  fprintf(stderr, "Failed to insert schemaless line data, data: %s, ErrCode: 0x%x, ErrMessage: %s\n.", line_demo, code,
         taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}

int rows = taos_affected_rows(result);
fprintf(stdout, "Insert %d rows of schemaless line data successfully.\n", rows);
taos_free_result(result);

// opentsdb telnet protocol
char *telnets[] = {telnet_demo};
result = taos_schemaless_insert(taos, telnets, 1, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
code = taos_errno(result);
if (code != 0) {
  fprintf(stderr, "Failed to insert schemaless telnet data, data: %s, ErrCode: 0x%x, ErrMessage: %s\n.", telnet_demo, code,
         taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}

rows = taos_affected_rows(result);
fprintf(stdout, "Insert %d rows of schemaless telnet data successfully.\n", rows);
taos_free_result(result);

// opentsdb json protocol
char *jsons[1] = {0};
// allocate memory for json data. can not use static memory.
size_t size = 1024;
jsons[0] = malloc(size);
if (jsons[0] == NULL) {
  fprintf(stderr, "Failed to allocate memory: %zu bytes.\n", size);
  taos_close(taos);
  taos_cleanup();
  return -1;
}
(void)strncpy(jsons[0], json_demo, 1023);
result = taos_schemaless_insert(taos, jsons, 1, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
code = taos_errno(result);
if (code != 0) {
  free(jsons[0]);
  fprintf(stderr, "Failed to insert schemaless json data, Server: %s, ErrCode: 0x%x, ErrMessage: %s\n.", json_demo, code,
         taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}
free(jsons[0]);

rows = taos_affected_rows(result);
fprintf(stdout, "Insert %d rows of schemaless json data successfully.\n", rows);
taos_free_result(result);

// close & clean
taos_close(taos);
taos_cleanup();
return 0;
```

### 4.9 数据订阅

TDengine  C/C++ 也提供了订阅功能，具体描述见《数据订阅 - Requirement》文档。

### 4.10 其他功能

除了以上业务功能之外，还提供了比如集群管理、授权管理、流计算等能力，这些都可以调用 sql 执行的接口来完成，详细的介绍有各个功能的独立文档，不在这里重复介绍。 

## 5. 性能

顺序写入性能：qps达到100w/s以上，无数据缺失
乱序写入性能：qps达到10w/s以上，无数据缺失
查询性能：       qps达到1000/s以上，无请求超时

## 6. 安全

数据库连接器的安全旨在确保数据在客户端和服务端之间传输、访问和存储过程中的机密性、完整性、可用性以及操作的可审计性。核心标准与要求可归纳为以下几个方面：
1. 数据传输安全：在数据传输过程中保证数据的机密性与完整性。使用SSL、TLS等通信加密协议，通过第三方可信机构签发的证书对数据库服务器进行身份认证，有效防范中间人攻击与身份伪造风险。具体内容参见《通信 - Requirement》文档。
2. 审计与可追溯性：连接器需要与数据库系统的审计功能适配。记录用户对数据库的所有操作，包括通过API 调用触发的关键事件（如启动、停止）和具体的用户操作行为。记录的所有日志需要在服务端进行保存，后续审计员可以利用这些日志监控数据库中的各种行为，追踪非法存取数据的人员和时间，实现追溯和责任认定。

## 7. 兼容性

1. C/C++ 连接器版本原则上与 taosd 一致。
2. 内部实现改变不影响接口定义，应用程序不应受连接器版本影响。
3. 新需求或者变化需要更新接口，增加新接口，不修改原有接口行为。

## 8. 运维

C/C++ 连接器默认使用 taos.cfg 作为配置文件，和 taos shell 使用相同的默认文件路径。

## 9. 使用场景

使用 C/C++ 开发语言，通过 taosc 连接和请求 taosd 服务器。其他开发语言也可以使用 taosc， 建议优先使用 TDengine 提供的各语言连接器。

## 10. 约束和限制

1. 使用和 taosd 相同的发行版本。
2. 不能指定默认的 taos.cfg 文件路径。

## 11. 常见错误和排查

下表是常见的 taosc 错误码，服务端的错误也会由 taosc 接口返回，可参考相关文档。
| 错误码 | 错误描述 | 可能的出错场景或者可能的原因 | 建议用户采取的措施 |
| --- | --- | --- | --- |
| 0x80000207 | Invalid user name | 数据库用户名不合法 | 检查数据库用户名是否正确 |
| 0x80000208 | Invalid password | 数据库密码不合法 | 检查数据库密码是否正确 |
| 0x80000209 | Database name too long | 数据库名称不合法 | 检查数据库名称是否正确 |
| 0x8000020A | Table name too long | 表名不合法 | 检查表名是否正确 |
| 0x8000020F | Query terminated | 查询被中止 | 检查是否有用户中止了查询 |
| 0x80000213 | Disconnected from server | 连接已中断 | 检查连接是否被人为中断或客户端正在退出 |
| 0x80000216 | Syntax error in SQL | SQL语法错误 | 检查SQL语句并修正错误 |
| 0x80000219 | SQL statement too long | SQL长度超出限制 | 检查SQL语句并修正错误 |
| 0x8000021A | File is empty | 文件内容为空 | 检查输入文件内容 |
| 0x8000021F | Invalid column length | 列长度错误 | 保留现场和日志，github上报issue |
| 0x80000222 | Invalid JSON data type | JSON数据类型错误 | 检查输入JSON内容 |
| 0x80000224 | Value out of range | 数据大小超过类型范围 | 检查输入的数据值 |
| 0x80000229 | Invalid tsc input | API输入错误 | 检查应用调用API时传递的参数 |
| 0x8000022A | Stmt API usage error | STMT API使用错误 | 检查STMT API调用的顺序、适用场景、错误处理 |
| 0x8000022B | Stmt table name not set | STMT未正确设置table name | 检查是否调用了设置table name接口 |
| 0x8000022D | Query killed | 查询被中止 | 检查是否有用户中止了查询 |
| 0x8000022E | No available execution node | 没有可用的查询执行节点 | 检查当前query policy配置，如果需要有Qnode参与确保系统中存在可用的Qnode节点 |
| 0x8000022F | Table is not a super table | 当前语句中的表名不是超级表 | 检查当前语句中所用表名是否是超级表 |
| 0x80000230 | Stmt cache error | STMT内部缓存出错 | 保留现场和日志，github上报issue |
| 0x80000231 | Tsc internal error | TSC内部错误 | 保留现场和日志，github上报issue |

## 12. 可观测性

无

## 13. 安装和卸载

无

## 14. 文档

需要在官方文档【参考手册】【连接器】增加章节【C/C++】
需要在官方文档【开发指南】部分，增加 C/C++ 连接器相关的描述和示例

## 15. 参考文档

无

## 16. 附录

无

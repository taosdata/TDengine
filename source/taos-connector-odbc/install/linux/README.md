# TDengine ODBC Connector - Linux 安装与测试

本目录提供 Linux 环境下 TDengine ODBC 连接器的安装脚本和功能测试程序。

## 目录结构

```
install/linux/
├── README.md              # 本文档
├── install_odbc.sh        # ODBC 连接器安装脚本
└── example/               # 测试程序源码目录
    └── test_odbc.c        # 综合功能测试程序
```

## 前置条件

目标服务器需满足以下条件：

1. **TDengine 客户端已安装**（`libtaos.so` 可用），且 TDengine 服务端已部署并正在运行
2. **unixODBC 已安装**
3. **GCC 编译器已安装**（用于编译测试程序）
4. **libtaosws.so 已安装**（可选，如果需要 WebSocket 连接模式）

### 安装依赖（如尚未安装）

```bash
# Ubuntu / Debian
sudo apt-get update
sudo apt-get install -y unixodbc unixodbc-dev gcc

# CentOS / RHEL
sudo yum install -y unixODBC unixODBC-devel gcc
```

## 使用步骤

### 第一步：准备驱动库文件

在编译服务器上，创建部署包目录并拷贝驱动库：

```bash
mkdir -p taos_odbc_deploy/lib

# 从构建目录拷贝驱动库
cp debug-others/build/taos-connector-odbc/build/src/libtaos_odbc.so.0.1 taos_odbc_deploy/lib/
cd taos_odbc_deploy/lib && ln -sf libtaos_odbc.so.0.1 libtaos_odbc.so && cd ../..

# 拷贝安装脚本和测试程序
cp source/taos-connector-odbc/install/linux/install_odbc.sh taos_odbc_deploy/
cp -r source/taos-connector-odbc/install/linux/example taos_odbc_deploy/
```

打包传输到目标服务器：

```bash
tar czf taos_odbc_deploy.tar.gz taos_odbc_deploy/
scp taos_odbc_deploy.tar.gz user@target-server:/path/to/
```

### 第二步：在目标服务器上安装 ODBC 连接器

```bash
tar xzf taos_odbc_deploy.tar.gz
cd taos_odbc_deploy

# 安装 ODBC 驱动和 DSN（需要 root 权限）
sudo ./install_odbc.sh
```

**自定义服务器地址**（如 TDengine 服务不在本机）：

```bash
sudo ./install_odbc.sh --server 192.168.1.100:6030 --ws-url http://192.168.1.100:6041
```

安装脚本会执行以下操作：
- 将 `libtaos_odbc.so` 安装到 `/usr/local/lib/`
- 在 `/etc/odbcinst.ini` 注册 ODBC 驱动（`TDengine` / `TAOS_ODBC_DRIVER`）
- 在 `/etc/odbc.ini` 和 `~/.odbc.ini` 配置 DSN 数据源（`TAOS_ODBC_DSN` / `TAOS_ODBC_WS_DSN`）

### 第三步：验证安装

```bash
# 查看已注册的驱动
odbcinst -q -d

# 查看已配置的 DSN
odbcinst -q -s -l
odbcinst -q -s -h

# 快速连接测试
isql -v TAOS_ODBC_DSN root taosdata
```

### 第四步：编译测试程序

```bash
gcc -o example/test_odbc example/test_odbc.c -lodbc -Wall -Wextra
```

### 第五步：运行测试

```bash
# 使用默认 DSN（TAOS_ODBC_DSN，Native 连接），默认用户名 root / 密码 taosdata
./example/test_odbc

# 指定用户名和密码
./example/test_odbc -u root -p taosdata

# 使用 WebSocket DSN，并指定用户名和密码
./example/test_odbc -u myuser -p mypassword TAOS_ODBC_WS_DSN

# 使用连接字符串直连
ODBC_CONN_STR="Driver=TAOS_ODBC_DRIVER;SERVER=localhost:6030;UID=root;PWD=taosdata" ./example/test_odbc

# 查看帮助
./example/test_odbc -h
```

## 测试覆盖的 ODBC 功能

测试程序分 10 个阶段，覆盖以下标准 ODBC API：

| 阶段 | 说明 | 测试的 ODBC API |
|------|------|-----------------|
| 1 | 环境与连接管理 | `SQLAllocHandle`, `SQLSetEnvAttr`, `SQLGetEnvAttr`, `SQLSetConnectAttr`, `SQLConnect`, `SQLDriverConnect`, `SQLGetInfo`, `SQLGetFunctions` |
| 2 | DDL 操作 | `SQLExecDirect`, `SQLRowCount` |
| 3 | 数据写入（参数绑定） | `SQLPrepare`, `SQLBindParameter`, `SQLExecute`, `SQLNumParams`, `SQLDescribeParam`, `SQLRowCount` |
| 4 | 数据读取（SQLGetData） | `SQLExecDirect`, `SQLNumResultCols`, `SQLDescribeCol`, `SQLColAttribute`, `SQLFetch`, `SQLGetData` |
| 5 | 数据读取（SQLBindCol） | `SQLBindCol`, `SQLFetch` |
| 6 | 元数据查询 | `SQLTables`, `SQLColumns`, `SQLPrimaryKeys`, `SQLGetTypeInfo`, `SQLCloseCursor` |
| 7 | 语句与游标管理 | `SQLSetStmtAttr`, `SQLGetStmtAttr`, `SQLCloseCursor`, `SQLFreeStmt`, `SQLMoreResults`, `SQLFetchScroll` |
| 8 | 诊断信息 | `SQLGetDiagRec`, `SQLGetDiagField` |
| 9 | 描述符操作 | `SQLGetStmtAttr`(获取描述符句柄), `SQLAllocHandle`/`SQLFreeHandle`(DESC) |
| 10 | 事务与清理 | `SQLEndTran`, `SQLDisconnect`, `SQLFreeHandle` |

**总计覆盖约 35+ 个已实现的标准 ODBC API 函数。**

### 已知驱动未实现的功能（测试中标记为 SKIP）

以下 ODBC API 在驱动中导出但未实现（返回 SQL_ERROR），测试中标记为 SKIP（不计入失败）：

| 函数 | 说明 |
|------|------|
| `SQLStatistics` | 统计信息查询 |
| `SQLSpecialColumns` | 特殊列查询 |
| `SQLProcedures` / `SQLProcedureColumns` | 存储过程查询 |
| `SQLGetDescField` / `SQLSetDescField` | 描述符字段操作 |
| `SQLGetDescRec` / `SQLSetDescRec` | 描述符记录操作 |
| `SQLCopyDesc` | 描述符复制 |
| `SQLSetCursorName` / `SQLGetCursorName` | 游标名称管理 |
| `SQLBulkOperations` / `SQLSetPos` | 批量操作 |
| `SQLCancel` | 取消执行 |
| `SQLNativeSql` | 原生 SQL 转换 |
| `SQLBrowseConnect` | 浏览连接 |

## 期望输出

测试程序运行后，将逐阶段输出每项测试的 PASS/FAIL/SKIP 状态，最终汇总结果：

```
============================================================
  TDengine ODBC Connector - Comprehensive Test Suite
============================================================
  DSN: TAOS_ODBC_DSN
  UID: root
============================================================

========== Phase: 1 - Environment & Connection Management ==========
  [PASS] SQLAllocHandle(ENV)
  [PASS] SQLSetEnvAttr(ODBC_VERSION=3)
  [PASS] SQLConnect(DSN, root, taosdata)
  [PASS] SQLGetInfo(DBMS_NAME)
  ...

========== Phase: 3 - Data Write (INSERT & Parameter Binding) ==========
  [PASS] SQLPrepare(INSERT with params)
  [PASS] SQLBindParameter(4: BOOL via SBIGINT+TINYINT)
  [PASS] SQLExecute(param row 1)
  ...

========== Phase: 9 - Descriptor Operations ==========
  [PASS] SQLAllocHandle(DESC)
  [SKIP] SQLGetDescField (not implemented by driver (stub))
  ...

============================================================
  Test Summary
============================================================
  Total:   95
  Passed:  86
  Failed:  0
  Skipped: 9 (driver limitation)
============================================================
  RESULT: ALL TESTS PASSED
  (Skipped items are known driver limitations,
   not test failures.)
============================================================
```

> **说明：** Skipped 的测试项是驱动已知未实现的功能，不计入失败。所有已实现的功能均应 PASS。

## 卸载

```bash
sudo ./install_odbc.sh --uninstall
```

## DSN 配置参考

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `SERVER` | TDengine 原生连接地址 | `localhost:6030` |
| `URL` | WebSocket 连接地址 | `http://localhost:6041` |
| `DB` | 默认数据库 | 空 |
| `UID` | 用户名 | `root` |
| `PWD` | 密码 | `taosdata` |
| `UNSIGNED_PROMOTION` | 无符号整数提升 | 空（禁用） |
| `TIMESTAMP_AS_IS` | 时间戳原样返回 | 空（禁用） |
| `CHARSET_ENCODER_FOR_PARAM_BIND` | 参数绑定字符编码 | 空 |
| `CHARSET_ENCODER_FOR_COL_BIND` | 列绑定字符编码 | 空 |
| `CONN_MODE` | 连接模式（WebSocket DSN 专用） | 空 |

## 故障排查

1. **连接失败**：确认 TDengine 服务正在运行，检查 `SERVER` 地址和端口
2. **驱动加载失败**：运行 `ldd /usr/local/lib/libtaos_odbc.so` 检查依赖库
3. **DSN 未找到**：运行 `odbcinst -q -s` 确认 DSN 已配置
4. **启用调试日志**：
   ```bash
   export TAOS_ODBC_LOG_LEVEL=DEBUG
   export TAOS_ODBC_LOGGER=stderr
   ./example/test_odbc
   ```

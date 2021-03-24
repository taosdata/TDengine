
# ODBC 驱动 #

- **TAOS ODBC驱动持续更新中**

- **目前导出的ODBC函数包括(注: 有些不常用的函数只是导出，但并未实现)**:
SQLAllocEnv
SQLFreeEnv
SQLAllocConnect
SQLFreeConnect
SQLGetEnvAttr
SQLSetEnvAttr
SQLGetConnectAttr
SQLGetConnectOption
SQLGetInfo
SQLConnect
SQLDisconnect
SQLAllocStmt
SQLAllocHandle
SQLFreeHandle
SQLFreeStmt
SQLExecDirect
SQLNumResultCols
SQLRowCount
SQLColAttribute
SQLGetData
SQLFetch
SQLPrepare
SQLExecute
SQLParamData
SQLPutData
SQLGetDiagRec
SQLBindParameter
SQLDescribeParam
SQLDriverConnect
SQLSetConnectAttr
SQLDescribeCol
SQLBindCol
SQLNumParams
SQLSetStmtAttr
SQLBindParam
SQLCancel
SQLCancelHandle
SQLCloseCursor
SQLColumns
SQLCopyDesc
SQLDataSources
SQLEndTran
SQLFetchScroll
SQLGetCursorName
SQLGetDescField
SQLGetDescRec
SQLGetStmtAttr
SQLGetStmtOption
SQLGetTypeInfo
SQLSetConnectOption
SQLSetCursorName
SQLSetDescField
SQLSetDescRec
SQLSetParam
SQLSetStmtOption
SQLSpecialColumns
SQLStatistics
SQLTables
SQLTransact

`

- **国际化。可以通过在ODBC连接串中指定针对SQLCHAR/SQLWCHAR/taos_charset/system-locale的字符集来解决常见的环境匹配问题**.

- **现有的ODBC客户端工具可以籍此驱动同TAOS数据源互联，包括主流linux/macosx/windows平台**

- **现有的支持ODBC的编程语言可以籍此驱动同TAOS数据源互联, 例如: c/nodejs/python/rust/go已经在上述三个主流平台测试通过, 熟悉其他语言的同学可以发现这基本上是开箱即用**

- **持续更新中**...

# 编译和测试使用
**Note**: 下述所有步骤都在TDengine项目的根目录下进行
**Note**: 请先确保src/connector/odbc如下所示，被包含在src/CMakeLists.txt源文件中
```
...
ADD_SUBDIRECTORY(dnode)
ADD_SUBDIRECTORY(connector/odbc)
ADD_SUBDIRECTORY(connector/jdbc)
```

# Linux下的编译, 以Ubuntu为例
```
sudo apt install unixodbc unixodbc-dev flex
rm -rf debug && cmake -B debug && cmake --build debug && cmake --install debug && echo yes
```
# MacOSX下的编译, 以Catalina为例，依赖homebrew进行第三方工具安装[https://brew.sh/]
```
brew install unixodbc
rm -rf debug && cmake -B debug && cmake --build debug && cmake --install debug && echo yes
```
# Windows下的编译, 以Windows 10为例
- 安装windows的`flex`工具. 目前我们使用[https://github.com/lexxmark/winflexbison](url). 安装完成后请确保win_flex.exe所在目录记录于`PATH`环境变量中.
- 安装Microsoft Visual Studio工具, 以VS2019为例
- `"C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvars64.bat"`
- `rmdir /s /q debug`
- `cmake -G "NMake Makefiles" -B debug`
- `cmake --build debug`
- `cmake --install debug`
- 以管理员身份打开`命令提示符`
- 安装ODBC驱动: 在上述打开的提示符下执行 `odbcconf /A {INSTALLDRIVER "TAOS | Driver=C:/TDengine/driver/todbc.dll | ConnectFunctions=YYN | DriverODBCVer=03.00"}`
- 新增一个数据源DSN: 执行 `odbcconf /A {CONFIGDSN "TAOS" "DSN=TAOS_DSN | Server=<host>:<port>"}`
上述步骤出现失败的话，可以参看这些链接:
1. win flex的安装: https://github.com/lexxmark/winflexbison/releases
2. PATH环境变量: https://jingyan.baidu.com/article/8ebacdf02d3c2949f65cd5d0.html
3. 管理员身份: https://blog.csdn.net/weixin_41083002/article/details/81019893
4. 安装odbc驱动/数据源: https://docs.microsoft.com/en-us/sql/odbc/odbcconf-exe?view=sql-server-ver15

# 测试使用
强烈建议您在linux上编译运行taosd服务端，因为当前TAOS还没有windows侧的服务端移植.
**Note1**: <>符号所括起的内容请按您当前的系统填写
**Note2**: `.stmts` 文件存放的是测试用sql语句, 注意其格式为`UTF-8`(不带BOM导引头)
## 按官方文档在linux侧启动taosd,确保选用'UTF-8'作为其字符集
## 在linux下创建数据
```
./debug/build/bin/tcodbc --dsn TAOS_DSN --uid <uid> --pwd <pwd> --sts ./src/connector/odbc/samples/create_data.stmts
--<或指定特殊的ODBC连接字符串 -->
./debug/build/bin/tcodbc -C 'DSN=TAOS_DSN;UID=<uid>;PWD=<pwd>;Server=<fqdn>:<port>' --sts ./src/connector/odbc/samples/create_data.stmts
```
## 在windows下检索数据
```
.\debug\build\bin\tcodbc -C "DSN=TAOS_DSN;UID=<uid>;PWD=<pwd>;Server=<fqdn>:<port>;enc_char=UTF-8" --sts .\src\connector\odbc\samples\query_data.stmts
```
## 在MacOSX下检索数据
```
./debug/build/bin/tcodbc -C "DSN=TAOS_DSN;UID=<uid>;PWD=<pwd>;Server=<fqdn>:<port>" --sts ./src/connector/odbc/samples/query_data.stmts
```

## 代码示例
- src/connector/odbc/examples/c
- src/connector/odbc/examples/js
- src/connector/odbc/examples/py
- src/connector/odbc/examples/rust
- src/connector/odbc/examples/go

在linux或MacOSX上, 可以通过修改运行如下脚本来尝试各种测试:
**Note**: 不要忘记替换<host>:<port>
**Note**: 你需要在你的平台上安装nodejs/python/rust/go
**Note**: 你还需要安装对应语言的ODBC包:
-- node-odbc for nodejs: https://www.npmjs.com/package/odbc
-- pyodbc for python:    https://pypi.org/project/pyodbc/
-- rust-odbc for rust:   https://docs.rs/odbc/0.17.0/odbc/
-- go-odbc for go:       https://github.com/alexbrainman/odbc

```
echo c &&
./debug/build/bin/tcodbc -C "DSN=TAOS_DSN;Server=<host>:<port>" --sts src/connector/odbc/samples/create_data.stmts &&
echo nodejs &&
./src/connector/odbc/examples/js/odbc.js -C 'DSN=TAOS_DSN;Server=<host>:<port>' &&
echo python &&
python3 src/connector/odbc/examples/py/odbc.py -C 'DSN=TAOS_DSN;Server=<host>:<port>' &&
echo rust &&
pushd src/connector/odbc/examples/rust/main && DSN='DSN=TAOS_DSN;Server=<host>:<port>' cargo run && popd &&
echo go &&
DSN='DSN=TAOS_DSN;Server=<host>:<port>' go run src/connector/odbc/examples/go/odbc.go &&
```

## 您可以对比测试一下prepared-batch-insert是否会带来速度的提升:
**注** src/connector/odbc/examples/c/main.c是tcodbc的源代码
```
./debug/build/bin/tcodbc -C "DSN=TAOS_DSN;Server=<host>:<port>" --insert --batch_size 200 --batchs 10000
```



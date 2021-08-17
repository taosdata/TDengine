
# ODBC Driver #

- **on-going implementation of ODBC driver for TAOS**

- **currently exported ODBC functions are**:
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

- **internationalized, you can specify charset for SQLCHAR/SQLWCHAR/taos_charset/system-locale to coordinate with the environment**.

- **enable ODBC-aware software to communicate with TAOS, no matter what platform it's running on, currently we support linux/macosx/windows**

- **enable any language with ODBC-bindings/ODBC-plugings to communicate with TAOS, currently c/nodejs/python/rust/go are all passed in our test environment, we believe other languages with ODBC-bindings/plugins are available-out-of-box**

- **still going on**...

# Building and Testing
**Note**: all `work` is done in TDengine's project directory
**Note**: please make sure src/connector/odbc is included in src/CMakeLists.txt
```
...
ADD_SUBDIRECTORY(dnode)
ADD_SUBDIRECTORY(connector/odbc)
ADD_SUBDIRECTORY(connector/jdbc)
```

# Building under Linux, use Ubuntu as example
```
sudo apt install unixodbc unixodbc-dev flex
rm -rf debug && cmake -B debug && cmake --build debug && cmake --install debug && echo yes
```
# Building under Windows, use Windows 10 as example
- install windows `flex` port. We use [https://github.com/lexxmark/winflexbison](url) at the moment. Please be noted to append `<path_to_win_flex.exe>` to your `PATH`.
- install Microsoft Visual Studio, take VS2019 as example here
- `"C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvars64.bat"`
- `rmdir /s /q debug`
- `cmake -G "NMake Makefiles" -B debug`
- `cmake --build debug`
- `cmake --install debug`
- open your `Command Prompt` with Administrator's priviledge
- install TAOS ODBC driver that was just built: run `odbcconf /A {INSTALLDRIVER "TAOS | Driver=C:/TDengine/driver/todbc.dll | ConnectFunctions=YYN | DriverODBCVer=03.00"}`
- add a new user dsn: run `odbcconf /A {CONFIGDSN "TAOS" "DSN=TAOS_DSN | Server=host:port"}`

# Test
we highly suggest that you build both in linux(ubuntu) and windows(windows 10) platform, because currently TAOS has not server-side port on windows platform.
**Note1**: content within <> shall be modified to match your environment
**Note2**: `.stmts` source files are all encoded in `UTF-8`
## start taosd in linux, suppose charset is `UTF-8` as default, please follow TAOS doc for starting up
## create data in linux
```
./debug/build/bin/tcodbc --dsn TAOS_DSN --uid <uid> --pwd <pwd> --sts ./src/connector/odbc/samples/create_data.stmts
--<or with driver connection string -->
./debug/build/bin/tcodbc -C 'DSN=TAOS_DSN;UID=<uid>;PWD=<pwd>;Server=<fqdn>:<port>' --sts ./src/connector/odbc/samples/create_data.stmts
```
## query data in windows
```
.\debug\build\bin\tcodbc -C "DSN=TAOS_DSN;UID=<uid>;PWD=<pwd>;Server=<fqdn>:<port>;enc_char=UTF-8" --sts .\src\connector\odbc\samples\query_data.stmts
```
## query data in MacOSX
```
./debug/build/bin/tcodbc -C "DSN=TAOS_DSN;UID=<uid>;PWD=<pwd>;Server=<fqdn>:<port>" --sts ./src/connector/odbc/samples/query_data.stmts
```

## code examples
- src/connector/odbc/examples/c
- src/connector/odbc/examples/js
- src/connector/odbc/examples/py
- src/connector/odbc/examples/rust
- src/connector/odbc/examples/go

on linux/MacOSX, here after are script-snippet for you to play with:
**Note**: don't forget to replace <host>:<port> with whatever on your environment
**Note**: you need to install node/python3/rust/go on you machine
**Note**: you also need to install odbc-bindings/odbc-pluggins on those language platform, such as:
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

## see how fast prepared-statment could bring up with:
```
./debug/build/bin/tcodbc -C "DSN=TAOS_DSN;Server=<host>:<port>" --insert --batch_size 200 --batchs 10000
```



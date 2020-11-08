
# ODBC Driver #

- **very initial implementation of ODBC driver for TAOS

- **currently partially supported ODBC functions are: `
SQLAllocEnv
SQLFreeEnv
SQLAllocConnect
SQLFreeConnect
SQLConnect
SQLDisconnect
SQLAllocStmt
SQLAllocHandle
SQLFreeStmt
SQLExecDirect
SQLExecDirectW
SQLNumResultCols
SQLRowCount
SQLColAttribute
SQLGetData
SQLFetch
SQLPrepare
SQLExecute
SQLGetDiagField
SQLGetDiagRec
SQLBindParameter
SQLDriverConnect
SQLSetConnectAttr
SQLDescribeCol
SQLNumParams
SQLSetStmtAttr
ConfigDSN
`

- **internationalized, you can specify different charset/code page for easy going. eg.: insert `utf-8.zh_cn` characters into database located in linux machine, while query them out in `gb2312/gb18030/...` code page in your chinese windows machine, or vice-versa. and much fun, insert `gb2312/gb18030/...` characters into database located in linux box from
your japanese windows box, and query them out in your local chinese  windows machine.

- **enable ODBC-aware software to communicate with TAOS.

- **enable any language with ODBC-bindings/ODBC-plugings to communicate with TAOS

- **still going on...

# Building and Testing
**Note**: all `work` is done in TDengine's project directory


# Building under Linux, use Ubuntu as example
```
sudo apt install unixodbc unixodbc-dev flex
rm -rf debug && cmake -B debug && cmake --build debug && cmake --install debug && echo yes
```
# Building under Windows, use Windows 10 as example
- install windows `flex` port. We use [https://github.com/lexxmark/winflexbison](url) at the moment. Please be noted to append `<path_to_win_flex.exe>` to your `PATH`.
- install Microsoft Visual Studio, take VS2015 as example here
- `"C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" amd64`
- `rmdir /s /q debug`
- `cmake -G "NMake Makefiles" -B debug`
- `cmake --build debug`
- `cmake --install debug`
- open your `Command Prompt` with Administrator's priviledge
- remove previously installed TAOS ODBC driver: run `C:\TDengine\todbcinst -u -f -n TAOS`
- install TAOS ODBC driver that was just built: run `C:\TDengine\todbcinst -i -n TAOS -p C:\TDengine\driver`
- add a new user dsn: run `odbcconf CONFIGDSN TAOS "DSN=TAOS_DSN|Server=<fqdn>:<port>`

# Test
we highly suggest that you build both in linux(ubuntu) and windows(windows 10) platform, because currently TAOS only has it's server-side port on linux platform.
**Note1**: content within <> shall be modified to match your environment
**Note2**: `.stmts` source files are all encoded in `UTF-8`
## start taosd in linux, suppose charset is `UTF-8` as default
```
taosd -c ./debug/test/cfg
```
## create data in linux
```
./debug/build/bin/tcodbc --dsn TAOS_DSN --uid <uid> --pwd <pwd> --sts ./src/connector/odbc/tests/create_data.stmts
--<or with driver connection string -->
./debug/build/bin/tcodbc --dcs 'Driver=TAOS;UID=<uid>;PWD=<pwd>;Server=<fqdn>:<port>;client_enc=UTF-8' ./src/connector/odbc/tests/create_data.stmts
```
## query data in windows
```
.\debug\build\bin\tcodbc --dsn TAOS_DSN --uid <uid> --pwd <pwd> --sts .\src\connector\odbc\tests\query_data.stmts
--<or with driver connection string -->
.\debug\build\bin\tcodbc --dcs "Driver=TAOS;UID=<uid>;PWD=<pwd>;Server=<fqdn>:<port>;client_enc=UTF-8" .\src\connector\odbc\tests\query_data.stmts
```



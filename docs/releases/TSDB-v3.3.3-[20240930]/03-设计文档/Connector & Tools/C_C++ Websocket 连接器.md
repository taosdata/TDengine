# C/C++ Websocket 连接器

## 1. 背景

需求连接 
TS-5060

目前使用原生连接的缺点是服务端升级时，因为我们版本匹配要求，客户端也必须升级 taosc 库。而 Websocket 连接方式可以避免上述缺点，同时提供与原生连接相近的性能。
目前没有 C/C++ 语言的 Websocket 连接器供客户使用，因此需要支持。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/7/04 | 0.1 | 佘彦杰 | 创建 |
| 2024/7/12 | 0.2 | 佘彦杰 | 增加 api 定义 |
| 2024/7/15 | 0.3 | 佘彦杰 | 基于 Wade Review 意见，优化对返回值的说明，去除所有 void 返回值 |
| 2024/9/18 | 1.0 | 佘彦杰 | 根据实际情况，调整函数名，有些不实现的，删除。 |

## 3. 定义

连接器：连接数据库的客户端驱动，此处指连接 TDengine 的客户端程序库。
Native 连接：通过客户端驱动程序 taosc 直接与服务端程序 taosd 建立连接
Websocket 连接：通过 taosAdapter 组件提供的 Websocket API 建立与 taosd 的连接

## 4. 行为说明

### 4.1 使用方式

#### 4.1.1 包含头文件

包含头文件` #include <taosws.h>`
TDengine 服务端或客户端安装后，`taosws.h` 位于：
- Linux: `/usr/local/taos/include`
- Windows: `C:\TDengine\include`
- macOS: `/usr/local/include`

#### 4.1.2 链接动态库

TDengine 客户端驱动的动态库位于：
- Linux: `/usr/local/taos/driver/libtaosws.so`
- Windows: `C:\TDengine\libtaosws.dll`
- macOS: `/usr/local/lib/libtaosws.dylib`

### 4.2 API 定义

API 与原生连接的对应关系和差异。对于 Adapter 不支持的接口，一般建议不放入 0831 版本。对于异步接口，一般建议不支持。

| Native API | Websocket API | 备注 | 结论 |
| --- | --- | --- | --- |
| void taos_cleanup(void); |  | Websocket 没有对应实现 | 不实现 |
| int taos_options(TSDB_OPTION option, const void *arg, ...); |  | 设置客户端选项，目前支持区域、字符集、时区、配置文件路径等 Websocket 没有对应实现 | 不实现 |
| setConfRet taos_set_config(const char *config); |  | 空函数 | 不实现 |
| int taos_init(void); |  | Websocket 没有对应实现 | 不实现 |
| TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port); | WS_TAOS *ws_connect(const char *dsn); | Dsn 更灵活 char* dsn = "taos://localhost:6041"; WS_TAOS* taos = ws_connect(dsn); | 支持，保持现状 |
| TAOS *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port); |  | 需 Adapter 支持 | 不放入 0831 版本 |
| void taos_close(TAOS *taos); | int32_t ws_close(WS_TAOS *taos); |  | 支持 |
| const char *taos_data_type(int type); |  | 返回类型字符串描述信息 | 支持 |
| const char *taos_get_server_info(TAOS *taos); | const char *ws_get_server_info(WS_TAOS *taos); |  | 支持 |
| const char *taos_get_client_info(); | const char *ws_get_client_info(void); |  | 支持 |
| int taos_get_current_db(TAOS *taos, char *database, int len, int *required); | int32_t ws_get_current_db(WS_TAOS *taos, char *database, int len, int *required); |  | 支持 |
| const char *taos_errstr(TAOS_RES *res); | const char *ws_errstr(WS_RES *res); |  | 支持 |
| int taos_errno(TAOS_RES *res); | int32_t ws_errno(WS_RES *res); |  | 支持 |
|  |  |  |  |
| TAOS_RES *taos_query(TAOS *taos, const char *sql); | WS_RES *ws_query(WS_TAOS *taos, const char *sql); |  | 支持 |
| TAOS_RES *taos_query_with_reqid(TAOS *taos, const char *sql, int64_t reqId); | WS_RES *ws_query_with_reqid(WS_TAOS *taos, const char *sql, uint64_t req_id); | 可以支持 | 支持 |
| TAOS_ROW taos_fetch_row(TAOS_RES *res); | WS_ROW ws_fetch_row(WS_RES *res); |  | 支持 |
| int taos_result_precision(TAOS_RES *res); // get the time precision of result | int32_t ws_result_precision(const WS_RES *res); |  | 支持 |
| void taos_free_result(TAOS_RES *res); | int32_t ws_free_result(WS_RES *res); |  | 支持 |
| void taos_kill_query(TAOS *taos); |  | 3.0 无法支持 | 不支持 |
| int taos_field_count(TAOS_RES *res); | int32_t ws_field_count(const WS_RES *res); |  | 支持 |
| int taos_num_fields(TAOS_RES *res); | int32_t ws_num_fields(const WS_RES *res); |  | 支持 |
| int taos_affected_rows(TAOS_RES *res); | int32_t ws_affected_rows(const WS_RES *res); |  | 支持 |
| int64_t taos_affected_rows64(TAOS_RES *res); | int64_t ws_affected_rows64(const WS_RES *res); |  | 支持 |
| void taos_stop_query(TAOS_RES *res); | int32_t ws_stop_query(WS_RES *res); |  | 支持 |
| TAOS_FIELD *taos_fetch_fields(TAOS_RES *res); | const struct WS_FIELD *ws_fetch_fields(WS_RES *res); |  | 支持 |
| int taos_select_db(TAOS *taos, const char *db); | int32_t ws_select_db(WS_TAOS *taos, const char *db); |  | 支持 |
| int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields); |  | 可以支持，接口要改，没有长度不安全 | 支持 |
| bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col); | bool ws_is_null(const WS_RES *rs, int32_t row, int32_t col); |  | 支持 |
| bool taos_is_update_query(TAOS_RES *res); | bool ws_is_update_query(const WS_RES *res); |  | 支持 |
| int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows); |  | 仅支持获取 raw block | 不支持 |
| int taos_fetch_block_s(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows); |  | 仅支持获取 raw block | 不支持 |
| int taos_fetch_raw_block(TAOS_RES *res, int *numOfRows, void **pData); | int ws_fetch_raw_block(WS_RES *res, int *numOfRows, void **pData); | 获取 raw block 块 | 支持 |
| void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param); |  | 异步接口，暂不支持 | 不支持 |
| int *taos_get_column_data_offset(TAOS_RES *res, int columnIndex); |  | 解析 block 用 | 不支持 |
| int taos_validate_sql(TAOS *taos, const char *sql); |  | Adapter 不支持 | 后续支持 不放入 0831 版本 |
| void taos_reset_current_db(TAOS *taos); |  | 给 TaosAdapter 重置连接池使用，应用不需要 | 不支持 |
| int *taos_fetch_lengths(TAOS_RES *res); |  | 也是为了解析 block 的 | 不支持 |
| TAOS_ROW *taos_result_block(TAOS_RES *res); |  | 也是为了解析 block 的 | 不支持 |
|  |  |  |  |
| TAOS_STMT *taos_stmt_init(TAOS *taos); | WS_STMT *ws_stmt_init(const WS_TAOS *taos); |  | 支持 |
| TAOS_STMT *taos_stmt_init_with_reqid(TAOS *taos, int64_t reqid); |  |  | 支持 |
| TAOS_STMT *taos_stmt_init_with_options(TAOS *taos, TAOS_STMT_OPTIONS* options); |  | 后加，没看到说明，下去确认 | 不放入 0831 版本 |
| int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length); | int ws_stmt_prepare(WS_STMT *stmt, const char *sql, unsigned long len); |  | 支持 |
| int taos_stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags); | int ws_stmt_set_tbname_tags(WS_STMT *stmt, const char *name, const WS_MULTI_BIND *bind, uint32_t len); |  | 支持 |
| int taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name); | int ws_stmt_set_tbname(WS_STMT *stmt, const char *name); |  | 支持 |
| int taos_stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags); | int ws_stmt_set_tags(WS_STMT *stmt, const WS_MULTI_BIND *bind, uint32_t len); | 已实现，参数不同。 | 支持，保持现状 |
| int taos_stmt_set_sub_tbname(TAOS_STMT *stmt, const char *name); | int ws_stmt_set_sub_tbname(WS_STMT *stmt, const char *name); |  | 支持 |
| int taos_stmt_get_tag_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields); | int ws_stmt_get_tag_fields(WS_STMT *stmt, int *fieldNum, struct StmtField **fields,); | 已实现，参数顺序不同 | 修改 |
| int taos_stmt_get_col_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields); | int ws_stmt_get_col_fields(WS_STMT *stmt, int *fieldNum, struct StmtField **fields); | 已实现，参数顺序不同 | 修改 |
| void taos_stmt_reclaim_fields(TAOS_STMT *stmt, TAOS_FIELD_E *fields); | int ws_stmt_reclaim_fields(WS_STMT *stmt, struct StmtField **fields, int fieldNum); | 已实现，参数不同， 因为 rust 必须根据数组长度来恢复切片，所以与Native 不同 | 支持 |
|  |  |  |  |
| int taos_stmt_is_insert(TAOS_STMT *stmt, int *insert); | int ws_stmt_is_insert(WS_STMT *stmt, int *insert); |  | 支持 |
| int taos_stmt_num_params(TAOS_STMT *stmt, int *nums); | int ws_stmt_num_params(WS_STMT *stmt, int *nums); |  | 支持 |
| int taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes); | int ws_stmt_get_param(WS_STMT *stmt, int idx, int *type, int *bytes); |  | 支持 |
| int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind); |  | 单行接口，优先级不高 | 不放入 0831 版本 |
| int taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind); | int ws_stmt_bind_param_batch(WS_STMT *stmt, const WS_MULTI_BIND *bind, uint32_t len); | 已实现，参数不同 | 支持，保持现状 |
| int taos_stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx); |  | 绑定单列接口 | 不放入 0831 版本 |
| int taos_stmt_add_batch(TAOS_STMT *stmt); | int ws_stmt_add_batch(WS_STMT *stmt); |  | 支持 |
| int taos_stmt_execute(TAOS_STMT *stmt); | int ws_stmt_execute(WS_STMT *stmt, int32_t *affected_rows); | 已实现，参数不同 | 支持，保持现状 |
| TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt); |  | 用于 stmt 查询，需要支持，优先级低 | 不放入 0831 版本 |
| int taos_stmt_close(TAOS_STMT *stmt); | int32_t ws_stmt_close(WS_STMT *stmt); | 已实现，返回值不同， Native 返回值代表成功失败。 | 修改为一致 |
| char *taos_stmt_errstr(TAOS_STMT *stmt); | const char *ws_stmt_errstr(WS_STMT *stmt); |  | 支持 |
| int taos_stmt_affected_rows(TAOS_STMT *stmt); | int ws_stmt_affected_rows(WS_STMT *stmt); |  | 支持 |
| int taos_stmt_affected_rows_once(TAOS_STMT *stmt); | int ws_stmt_affected_rows_once(WS_STMT *stmt); |  | 支持 |
|  |  |  |  |
| void taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param); |  | 异步接口，暂不支持 | 不支持 |
| void taos_query_a_with_reqid(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param, int64_t reqid); |  | 异步接口，暂不支持 | 不支持 |
| void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param); |  | 异步接口，暂不支持 | 不支持 |
| const void *taos_get_raw_block(TAOS_RES *res); |  | 后加，没看到说明 | 不支持 |
|  |  |  |  |
| int taos_get_db_route_info(TAOS *taos, const char *db, TAOS_DB_ROUTE_INFO *dbInfo); |  | 高效写入，Adapter 还不支持 | 不放入 0831 版本 |
| int taos_get_table_vgId(TAOS *taos, const char *db, const char *table, int *vgId); |  | 高效写入，Adapter 还不支持 | 不放入 0831 版本 |
| int taos_get_tables_vgId(TAOS *taos, const char *db, const char *table[], int tableNum, int *vgId); |  | 高效写入，Adapter 还不支持 | 不放入 0831 版本 |
|  |  |  |  |
| int taos_load_table_info(TAOS *taos, const char *tableNameList); |  | 加载表信息 | 支持，不放入 0831 版本 |
| void taos_set_hb_quit(int8_t quitByKill); |  | 后加，没看到说明 | 不支持 |
| int taos_set_notify_cb(TAOS *taos, __taos_notify_fn_t fp, void *param, int type); |  | 后加，没看到说明 | 不支持 |
| void taos_fetch_whitelist_a(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param); |  | 白名单 | 不支持 |
| int taos_set_conn_mode(TAOS* taos, int mode, int value); |  | DSN 支持，无需支持 | 通过 dsn 支持，保持现状 |
|  |  |  |  |
| TAOS_RES *taos_schemaless_insert(TAOS *taos, char *lines[], int numLines, int protocol, int precision); |  | 不带 raw 的有 bug， 无需支持 | 不支持 |
| TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int64_t reqid); |  | 不带 raw 的有 bug， 无需支持 | 不支持 |
| TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl); |  | 不带 raw 的有 bug， 无需支持 | 不支持 |
| TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl, int64_t reqid); |  | 不带 raw 的有 bug， 无需支持 | 不支持 |
| TAOS_RES *taos_schemaless_insert_raw(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision); |  | 需要支持，返回值只有成功失败 字符集建议用 utf8 | 支持 |
| TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int64_t reqid); |  | 需要支持，返回值只有成功失败 | 支持 |
| TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl); |  | 需要支持，返回值只有成功失败 | 支持 |
| TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl, int64_t reqid); |  | 需要支持，返回值只有成功失败 | 支持 |
|  |  |  |  |
| tmq_conf_t *tmq_conf_new(); |  | 需要支持 | 支持 |
| tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value); |  | 需要支持 | 支持 |
| void tmq_conf_destroy(tmq_conf_t *conf); |  | 需要支持 | 支持 |
| void tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param); |  | 异步接口 | 可以支持，低优先级 |
|  |  |  |  |
| tmq_list_t *tmq_list_new(); |  | 需要支持 | 支持 |
| int32_t tmq_list_append(tmq_list_t *, const char *); |  | 需要支持 | 支持 |
| void tmq_list_destroy(tmq_list_t *); |  | 需要支持 | 支持 |
| int32_t tmq_list_get_size(const tmq_list_t *); |  | 需要支持 | 支持 |
| char **tmq_list_to_c_array(const tmq_list_t *); |  | 需要支持 | 支持 |
|  |  |  |  |
| tmq_t *tmq_consumer_new(tmq_conf_t *conf, char *errstr, int32_t errstrLen); |  | 需要支持 | 支持 |
| int32_t tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list); |  | 需要支持 | 支持 |
| int32_t tmq_unsubscribe(tmq_t *tmq); |  | 需要支持 | 支持 |
| int32_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics); |  | 不支持 | 不支持 |
| TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout); |  | 需要支持 | 支持 |
| int32_t tmq_consumer_close(tmq_t *tmq); |  | 需要支持 | 支持 |
| int32_t tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg); //Commit the msg’s offset + 1 |  | 需要支持 | 支持 |
| void tmq_commit_async(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param); |  | 异步接口，暂不支持 | 不支持 |
| int32_t tmq_commit_offset_sync(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset); |  | 需要支持 | 支持 |
| void tmq_commit_offset_async(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb, void *param); |  | 异步接口，暂不支持 | 不支持 |
| int32_t tmq_get_topic_assignment(tmq_t *tmq, const char *pTopicName, tmq_topic_assignment **assignment,int32_t *numOfAssignment); |  | 需要支持 | 支持 |
| void tmq_free_assignment(tmq_topic_assignment* pAssignment); |  | 需要支持 | 支持 |
| int32_t tmq_offset_seek(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset); |  | 需要支持 | 支持 |
| int64_t tmq_position(tmq_t *tmq, const char *pTopicName, int32_t vgId); // The current offset is the offset of the last consumed message + 1 |  | 需要支持 | 支持 |
| int64_t tmq_committed(tmq_t *tmq, const char *pTopicName, int32_t vgId); |  | 需要支持 | 支持 |
|  |  |  |  |
| TAOS *tmq_get_connect(tmq_t *tmq); |  | Adapter 专用 | 不支持 |
| const char *tmq_get_table_name(TAOS_RES *res); |  | 需要支持 | 支持 |
| tmq_res_t tmq_get_res_type(TAOS_RES *res); |  | 需要支持 | 支持 |
| const char *tmq_get_topic_name(TAOS_RES *res); |  | 需要支持 | 支持 |
| const char *tmq_get_db_name(TAOS_RES *res); |  | 需要支持 | 支持 |
| int32_t tmq_get_vgroup_id(TAOS_RES *res); |  | 需要支持 | 支持 |
| int64_t tmq_get_vgroup_offset(TAOS_RES* res); |  | 需要支持 | 支持 |
| const char *tmq_err2str(int32_t code); |  | 因 Websocket 实现无法获取 Native 的错误码表，修改函数接口为： - `const char *ws_tmq_errstr(ws_tmq_t *tmq)` | 支持 |
|  |  |  |  |
| TSDB_SERVER_STATUS taos_check_server_status(const char *fqdn, int port, char *details, int maxlen); |  | Adapter 还不支持 | 不放入 0831 版本 |
| char* getBuildInfo(); |  |  | 不放入 0831 版本 |

## 5. 接口说明（0831版本包含部分）

### 5.1 基础 API

基础 API 用于完成创建数据库连接等工作，为其它 API 的执行提供运行时环境。
- `char *ws_get_client_info()`
  - 功能说明：获取客户端版本信息。
  - 返回值：客户端版本信息字符串, 如 "0.12.4"， NULL 表示失败。
- `WS_TAOS *ws_connect(const char *dsn)`
  - 功能说明：创建数据库连接，初始化连接上下文。
  - 参数说明：
    dsn 描述字符串基本结构如下：
    ```plaintext
    <driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
    |------|------------|---|-----------|-----------|------|------|------------|-----------------------|
    |driver|   protocol |   | username  | password  | host | port |  database  |  params               |
    ```

    各部分意义如下：
    - **driver**: 必须指定驱动名以便连接器选择何种方式创建连接，支持如下驱动名：
      - **taos**: 使用 TDengine 连接器驱动，支持查询和写入。
      - **tmq**: 使用 TMQ 订阅数据。
      - **http/ws**: 使用 Websocket 创建连接。
      - **https/wss**: 在 Websocket 连接方式下显示启用 SSL/TLS 连接。
    - **protocol**: 显示指定以何种方式建立连接，例如：`taos+ws://localhost:6041` 指定以 Websocket 方式建立连接。
    - **username/password**: 用于创建连接的用户名及密码。
    - **host/port**: 指定创建连接的服务器及端口，当不指定服务器地址及端口时（`taos+ws://`），Websocket 连接默认为 `localhost:6041` 。
    - **database**: 指定默认连接的数据库名，可选参数。
    - **params**：其他可选参数。
    一个完整的 DSN 描述字符串示例如下：`taos+ws://``localhost:6041/test`
    表示使用 Websocket（`ws`）方式通过 `6041` 端口连接服务器 `localhost`，并指定默认数据库为 `test`。
  - 返回值：为空表示失败。应用程序需要保存返回的参数，以便后续使用。连接使用完毕后必须调用 `ws_close` 关闭，以释放资源。
- `char *ws_get_server_info(WS_TAOS *taos)`
  - 功能说明：获取服务端版本信息。
  - 参数说明：
    - taos：连接句柄，为 `ws_connect()` 创建连接时返回。
  - 返回值：服务端版本信息字符串，NULL 表示失败
- `int32_t ws_select_db(WS_TAOS *taos, const char *db)`
  - 功能说明：将当前的缺省数据库设置为 `db`。
  - 参数说明：
    - taos：连接句柄
    -  db：要设置的数据库名称
  - 返回值： 0 成功，其他失败
- `int32_t ws_get_current_db(WS_TAOS *taos, char *database, int len, int *required)`
  - 功能说明：获取当前选择的数据库名
  - 参数说明：
    - taos：连接句柄
    - database：用来存储数据库名的指针
    - len：为用户在外面申请的 database 空间字节数，内部会把当前 db 赋值到 database 里。
    - required：存储 db 需要的空间
  - 返回值： 0 成功，其他失败。
    - 只要是没有正常把 db 名赋值到 database 中（包括截断），返回错误，返回值为 -1，然后用户可以通过 taos_errstr（NULL） 来获取错误提示。
    - 如果，database == NULL 或者 len <= 0 返回错误，required 里保存存储 db 需要的空间（包含最后的'\0'）
    - 如果，len 小于 存储 db 需要的空间（包含最后的'\0'），返回错误，database 里赋值截断的数据，以'\0'结尾。
    - 如果，len 大于等于 存储 db 需要的空间（包含最后的'\0'），返回正常 0，database 里赋值以'\0‘结尾的 db 名。
- `int32_t ws_close(WS_TAOS *taos)`
  - 功能说明：关闭连接
  - 参数说明：
    - taos：连接句柄，为 `ws_connect()` 创建连接时返回。
  - 返回值： 0 成功，其他失败。

### 5.2 同步查询 API

本小节介绍 API 均属于同步接口。应用调用后，会阻塞等待响应，直到获得返回结果或错误信息。
- `WS_RES *ws_query(WS_TAOS *taos, const char *sql)`
  - 功能说明：执行 SQL 语句，可以是 DQL、DML 或 DDL 语句。 
  - 参数说明：
    - taos：连接句柄，为 `taos_connect()` 创建连接时返回。
    - sql：sql 语句字符串
  - 返回值：结果集句柄，不能仅仅通过返回值是否是 `NULL` 来判断执行结果是否失败，而是需要用 `taos_errno()` 函数解析结果集中的错误代码来进行判断。注意后面必须调用 `taos_free_result` 释放结果集相关资源。
- `WS_RES *ws_query_with_reqid(WS_TAOS *taos, const char *sql, uint64_t req_id)`
  - 功能说明：增加 `req_id`，其余同 `WS_RES *ws_query(WS_TAOS *taos, const char *sql)` 
  - 参数说明：
    - `req_id` ：`req_id` 可用于请求链路追踪，`req_id` 就像分布式系统中的 traceId 作用一样。一个请求可能需要经过多个服务或者模块才能完成。`req_id` 用于标识和关联这个请求的所有相关操作，以便于我们可以追踪和分析请求的完整路径。使用 `req_id` 有下面好处：
      - 请求追踪：通过将同一个 `req_id` 关联到一个请求的所有相关操作，可以追踪请求在系统中的完整路径
      - 性能分析：通过分析一个请求的 `req_id`，可以了解请求在各个服务和模块中的处理时间，从而找出性能瓶颈
      - 故障诊断：当一个请求失败时，可以通过查看与该请求关联的 reqId 来找出问题发生的位置
      如果用户不设置 `req_id`，连接器也会内部随机生成一个，但是还是建议用户设置，可以更好的跟用户请求关联起来。
  - 返回值：结果集句柄，不能仅仅通过返回值是否是 `NULL` 来判断执行结果是否失败，而是需要用 `taos_errno()` 函数解析结果集中的错误代码来进行判断。注意后面必须调用 `taos_free_result` 释放结果集相关资源。
- `int32_t ws_result_precision(const WS_RES *res)`
  - 功能说明：返回结果集时间戳字段的精度
  - 参数说明：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
  - 返回值：0 代表毫秒，1 代表微秒，2 代表纳秒。-1 表示失败。
- `WS_ROW ws_fetch_row(WS_RES *res)`
  - 功能说明：按行获取查询结果集中的数据
  - 参数说明：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
  - 返回值：
    - 返回一个指针数组，数组的元素个数为列数，通过 `ws_num_fields` 获取，元素的类型和长度通过 `ws_fetch_fields` 来获取。当返回 NULL 时，需要调用 `ws_errno` 来判断成功失败。
- `int ws_fetch_raw_block(WS_RES *res, void **pData, int *numOfRows )`
  - 功能说明：批量获取查询结果集中的数据，一般用来跳过部分结果行
  - 参数说明：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
    - numOfRows：用来设置块中包含的行数
    - pData：块数据
  - 返回值：0 为成功，其他失败。失败信息可以通过 `ws_errno` 和 `ws_errstr` 获取
- `int ws_num_fields(WS_RES *res)`
  - 功能说明：用于获取查询结果集中的列数
  - 参数说明：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
  - 返回值：结果集中的列数。-1 表示失败。
- `int ws_field_count(WS_RES *res)`
  - 同 `ws_num_fields`
- `int ws_affected_rows(WS_RES *res)`
  - 功能说明：获取被所执行的 SQL 语句影响的行数。
  - 参数说明：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
  - 返回值：被所执行的 SQL 语句影响的行数，-1 表示 失败。
- ` int64_t ws_affected_rows64(const WS_RES *res)`
  - 功能说明：同 `ws_affected_rows`，仅返回值类型不同
- `WS_FIELD *Ws_fetch_fields(WS_RES *res)`
  - 功能说明：获取查询结果集每列数据的属性（列的名称、列的数据类型、列的长度），与 `ws_num_fields()` 配合使用，可用来解析 `ws_fetch_row()` 返回的一个元组（一行）的数据。 
  - 参数说明：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
  - 返回值：`WS_FIELD` 类型指针。NULL 表示 失败，结构体其定义如下：
    ```c
     typedef struct WS_FIELD {
       char name[65];
       uint8_t type;
       uint32_t bytes;
     } WS_FIELD;
    ```

- `bool ws_is_null(const WS_RES *rs, int32_t row, int32_t col)`
  - 功能说明：判断结果集中，第 row 行 col 列 是否为空
  - 参数说明：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
    - row：行数
    - col：列数
  - 返回值：true 代表 第 row 行 col 列 为空，false 表示不为空。注意对于超过范围的行和列，返回 true。
- `bool ws_is_update_query(const WS_RES *res)`
  - 功能说明：判断执行的 sql 是否为更新语句
  - 参数：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
  - 返回值：true 表示执行的 sql 是更新语句，false 表示不是。
- `int ``ws_stop_query(WS_RES *res)`
  - 功能说明：停止当前查询的执行。
  - 参数说明：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
  - 返回值：0 为成功，其他失败。失败信息可以通过 `ws_errno` 和 `ws_errstr` 获取
- `int`` ws_free_result(WS_RES *res)`
  - 功能说明：释放查询结果集以及相关的资源。查询完成后，务必调用该 API 释放资源，否则可能导致应用内存泄露。但也需注意，释放资源后，切勿再调用以此查询结果集句柄为参数的方法，否则将导致应用崩溃。
  - 参数说明：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
  - 0 为成功，其他失败。失败信息可以通过 `ws_errno` 和 `ws_errstr` 获取
- `char *ws_errstr(WS_RES *res)`
  - 功能说明：获取最近一次查询结果集相关 API 调用失败的原因
  - 参数说明：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
  - 返回值：为字符串标识的错误提示信息。NULL 表示无错误。
- `int ws_errno(WS_RES *res)`
  - 功能说明：获取最近一次 API 调用的错误码。
  - 参数说明：
    - res：调用 `ws_query` 或 `ws_query_with_reqid` 返回的结果集句柄。
  - 返回值：0 为 无错误，其他为错误码。
**NOTE**
推荐数据库应用的每个线程都建立一个独立的连接，或基于线程建立连接池。而不推荐在应用中将该连接 （`WS_TAOS*`） 结构体传递到不同的线程共享使用。同一个连接上只能串行调用接口（`ws_stop_query` 除外）。“USE DB” 等状态量有可能在线程之间相互干扰。建议只有在程序最后退出的时候才调用 `ws_close()` 关闭连接。 另一个需要注意的是，在上述同步 API 执行过程中，不能调用类似 pthread_cancel 之类的 API 来强制结束线程，如果强制结束线程有可能造成资源泄漏。

### 5.3 参数绑定 API

#### 5.3.1 参数绑定过程说明

除了直接调用 `ws_query()` 进行查询，TDengine 也提供了支持参数绑定的 Prepare API，风格与 MySQL 类似，目前也仅支持用问号 `?` 来代表待绑定的参数。
通过参数绑定接口写入数据时，就避免了 SQL 语法解析的资源消耗，从而在绝大多数情况下显著提升写入性能。此时的典型操作步骤如下：
1. 调用 `ws_stmt_init()` 创建参数绑定对象；
2. 调用 `ws_stmt_prepare()` 解析 INSERT 语句；
3. 设置表名和 TAGS：
   - 如果 INSERT 语句中预留了表名但没有预留 TAGS，那么调用 `ws_stmt_set_tbname()` 来设置表名；
   - 如果 INSERT 语句中既预留了表名又预留了 TAGS（例如 INSERT 语句采取的是自动建表的方式），那么调用 `ws_stmt_set_tbname_tags()` 来设置表名和 TAGS 的值；
4. 调用 `ws_stmt_bind_param_batch()` 以多行的方式设置 VALUES 的值，或者调用 `ws_stmt_bind_param()` 以单行的方式设置 VALUES 的值；
5. 调用 `ws_stmt_add_batch()` 把当前绑定的参数加入批处理；可以重复第 3 ～ 5 步，为批处理加入更多的数据行；
6. 调用 `ws_stmt_execute()` 执行已经准备好的批处理指令；
7. 执行完毕，调用 `ws_stmt_close()` 释放所有资源。
说明：如果 `ws_stmt_execute()` 执行成功，假如不需要改变 SQL 语句的话，那么是可以复用 `ws_stmt_prepare()` 的解析结果，直接进行第 3 ～ 5 步绑定新数据的。但如果执行出错，那么并不建议继续在当前的环境上下文下继续工作，而是建议释放资源，然后从 `ws_stmt_init()` 步骤重新开始。

#### 5.3.2 参数绑定接口说明

接口相关的具体函数如下（也可以参考 [prepare.c](https://github.com/taosdata/TDengine/blob/develop/examples/c/prepare.c) 文件中使用对应函数的方式）：
- `WS_STMT *ws_stmt_init_with_reqid(const WS_TAOS *taos, uint64_t req_id)`
  - 功能说明：创建一个 `WS_TAOS` 对象用于后续调用，注意最后不需要再执行 stmt 调用时，要调用 `ws_stmt_close` 来释放资源。
  - 参数说明：
    - taos：连接句柄，为 `taos_connect()` 创建连接时返回。
    - req_id: 用于请求链路追踪
  - 返回值：一个 `WS_STMT` 对象的句柄，NULL 表示失败。
- `WS_STMT *ws_stmt_init(const WS_TAOS *taos)`
  - 功能说明：同 `taos_stmt_init_with_reqid`， 缺少 req_id 参数
- `int ws_stmt_prepare(WS_STMT *stmt, const char *sql, unsigned long len)`
  - 功能说明：解析一条 SQL 语句，将解析结果和参数信息绑定到 stmt 上
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
    - sql：要绑定的 sql
    - len：sql 语句的长度，如果等于 0，将自动判断 SQL 语句的长度。
  - 返回值：0 绑定成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_bind_param_batch(WS_STMT *stmt, const WS_MULTI_BIND *bind, uint32_t len))`
  - 功能说明：以多列的方式传递待绑定的数据，需要保证这里传递的数据列的顺序、列的数量与 SQL 语句中的 VALUES 参数完全一致。
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
    - bind：为一个数组，每个元素为一列数据，以多列的方式传递待绑定的数据，WS_MULTI_BIND 的具体定义如下：
      ```c
      typedef struct TaosMultiBind {
          int buffer_type;  // column type
          const void *buffer; // column data
          uintptr_t buffer_length; // element capacity byte size
          const int32_t *length; // an array which element is real byte size
          const char *is_null; // an array which element value is 1 indicating a null value
          int num; // rows
      } TaosMultiBind;
      typedef struct TaosMultiBind WS_MULTI_BIND;
      ```

    - len：列数
  - 返回值： 0 绑定数据成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_set_tbname(WS_STMT *stmt, const char *name)`
  - 功能说明：当 SQL 语句中的超级表名使用了 `?` 占位时，可以使用此函数绑定一个具体的表名。
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
    - name：表名字符串
  - 返回值：0 表示成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_set_sub_tbname(WS_STMT *stmt, const char *name)`
  - 功能说明：当 SQL 语句中的子表名使用了 `?` 占位时，可以使用此函数绑定一个具体的表名。
  - 参数说明：参考 `ws_stmt_set_tbname`
  - 返回值：参考 `ws_stmt_set_tbname`
- `int ws_stmt_set_tbname_tags(WS_STMT *stmt, const char *name, const WS_MULTI_BIND *bind, uint32_t len)`
  - 功能说明：当 SQL 语句中的表名和 TAGS 都使用了 `?` 占位时，可以使用此函数绑定具体的表名和具体的 TAGS 取值。最典型的使用场景是使用了自动建表功能的 INSERT 语句（目前版本不支持指定具体的 TAGS 列）。TAGS 参数中的列数量需要与 SQL 语句中要求的 TAGS 数量完全一致。
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
    - name：表名字符串
    - bind：要绑定的 TAGS 数据，结构可以参考 `ws_stmt_bind_param_batch` 的说明。
    - len：要绑定的 tag 数量。
  - 返回值：0 表示成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_set_tags(WS_STMT *stmt, const WS_MULTI_BIND *bind, uint32_t len)`
  - 功能说明：当 SQL 语句中的 TAGS 使用了 `?` 占位时，可以使用此函数绑定具体的 TAGS 取值。TAGS 参数中的列数量需要与 SQL 语句中要求的 TAGS 数量完全一致。
  - 参数：参考 `ws_stmt_set_tbname_tags`
  - 返回值：参考 `ws_stmt_set_tbname_tags`
- `int ws_stmt_get_tag_fields(WS_STMT *stmt, int *fieldNum, struct StmtField **fields)`
  - 功能说明：获取要绑定的 TAGS 的类型和长度等 meta 信息。
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
    - fieldNum：tag 数量，等于 `ws_stmt_prepare` 调用时 sql 中 TAG 部分 ？ 的数量。
    - fields：tag 信息，`StmtField` 类型的指针数组，`StmtField` 类型定义如下：
      ```c
      typedef struct StmtField {
        char name[65];
        int8_t type;
        uint8_t precision;
        uint8_t scale;
        int32_t bytes;
      } StmtField;
      ```

  - 返回值：0 表示成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_get_col_fields(WS_STMT *stmt, int *fieldNum, struct StmtField **fields)`
  - 功能说明：获取要绑定的 列 的类型和长度等 meta 信息。
  - 参数说明：参考 `ws_stmt_get_tag_fields`
  - 返回值：参考 `ws_stmt_get_tag_fields`
- `int ws_stmt_reclaim_fields(WS_STMT *stmt, struct StmtField **fields, int fieldNum);`
  - 功能说明：用来释放之前调用 `ws_stmt_get_tag_fields` 或 `ws_stmt_get_col_fields` 申请的资源。
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
    - fields：之前通过调用 `ws_stmt_get_tag_fields` 或 `ws_stmt_get_col_fields` 返回的句柄。
    - fieldNum：fields 数组长度
  - 返回值：0 表示成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_is_insert(WS_STMT *stmt, int *insert)`
  - 功能说明：判断 stmt 是否为插入
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
    - insert：整型指针，如果是 insert 会赋值 为 1，否则为 0
  - 返回值：0 表示成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_num_params(WS_STMT *stmt, int *nums)`
  - 功能说明：获取 stmt 中占位符的数量
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
    - nums：整型指针，会被赋值为 stmt 占位符数量
  - 返回值：0 表示成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_get_param(WS_STMT *stmt, int idx, int *type, int *bytes)`
  - 功能说明：获取 stmt 中某列的类型和长度
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
    - idx：第几列
    - type：整型指针，用来返回此列类型
    - bytes：整型指针，用来返回此列长度
  - 返回值：0 表示成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_add_batch(WS_STMT *stmt)`
  - 功能说明：将当前绑定的参数加入批处理中，调用此函数后，可以再次调用 `taos_stmt_bind_param()` 或 `taos_stmt_bind_param_batch()` 绑定新的参数。需要注意，此函数仅支持 INSERT/IMPORT 语句，如果是 SELECT 等其他 SQL 语句，将返回错误。
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
  - 返回值：0 表示成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_execute(WS_STMT *stmt, int32_t *affected_rows)`
  - 功能说明：执行准备好的语句。目前，一条语句只能执行一次。
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
    - affected_rows：整型指针，用来返回影响行数
  - 返回值：0 表示成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_affected_rows(WS_STMT *stmt)`
  - 功能说明：获取执行多次绑定语句影响的行数。
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
  - 返回值：返回影响的行数，负数表示失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `int ws_stmt_affected_rows_once(WS_STMT *stmt)`
  - 功能说明：获取执行一次绑定语句影响的行数。
  - 参数说明：参考 `ws_stmt_affected_rows`
  - 返回值：参考 `ws_stmt_affected_rows`
- `int ws_stmt_close(WS_STMT *stmt)`
  - 功能说明；执行完毕，释放所有资源
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
  - 返回值：0 表示成功，其他失败。失败后可以通过 `ws_stmt_errstr` 获取详细错误原因。
- `char * ws_stmt_errstr(WS_STMT *stmt)`
  - 功能说明；用于在其他 STMT API 返回错误（返回错误码或空指针）时获取错误信息。
  - 参数说明：
    - stmt：通过 `ws_stmt_init` 创建的 `WS_STMT` 对象的句柄
  - 返回值：详细错误原因字符串。NULL 表示无错误。

### 5.4 无模式（schemaless）写入 API

除了使用 SQL 方式或者使用参数绑定 API 写入数据外，还可以使用 Schemaless 的方式完成写入。Schemaless 可以免于预先创建超级表/数据子表的数据结构，而是可以直接写入数据，TDengine 系统会根据写入的数据内容自动创建和维护所需要的表结构。Schemaless 的使用方式详见 [Schemaless 写入](https://docs.taosdata.com/reference/schemaless/) 章节，这里介绍与之配套使用的 C/C++ API。
- `WS_RES *ws_schemaless_insert_raw_ttl_with_reqid(WS_TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl, int64_t reqid)`
- 功能说明
  - 该接口将行协议的文本数据写入到 TDengine 中。
- 参数说明
  - taos: 数据库连接，通过 `ws_connect()` 函数建立的数据库连接。
  - lines：文本数据。满足解析格式要求的无模式文本字符串，支持多行，用换行符隔开即可。
  - len: lines 字符串的长度 。
  - totalRows：整型指针，用来返回插入多少行。
  - protocol: 行协议类型，用于标识文本数据格式。
    ```c
    TSDB_SML_LINE_PROTOCOL = 1, //InfluxDB 行协议（Line Protocol)
    TSDB_SML_TELNET_PROTOCOL,   //OpenTSDB Telnet 文本行协议
    TSDB_SML_JSON_PROTOCOL,     //OpenTSDB Json 协议格式
    ```

  - precision：文本数据中的时间戳精度字符串。需要注意的是，时间戳分辨率参数只在协议类型为 TSDB_SML_LINE_PROTOCOL 的时候生效。 对于 OpenTSDB 的文本协议，时间戳的解析遵循其官方解析规则 — 按照时间戳包含的字符的数量来确认时间精度。
    ```c
    TSDB_SML_TIMESTAMP_NOT_CONFIGURED = 0,
    TSDB_SML_TIMESTAMP_HOURS,
    TSDB_SML_TIMESTAMP_MINUTES,
    TSDB_SML_TIMESTAMP_SECONDS,
    TSDB_SML_TIMESTAMP_MILLI_SECONDS,
    TSDB_SML_TIMESTAMP_MICRO_SECONDS,
    TSDB_SML_TIMESTAMP_NANO_SECONDS
    ```

  - ttl：传递 ttl 参数来控制建表的 ttl 到期时间。
  - reqid：通过传递 reqid 参数来追踪整个的调用链。
- 返回值
  - WS_RES 结构体，应用可以通过使用 `taos_errstr()` 获得错误信息，也可以使用 `taos_errno()` 获得错误码。 在某些情况下，返回的 TAOS_RES 为 `NULL`，此时仍然可以调用 `taos_errno()` 来安全地获得错误码信息。 返回的 TAOS_RES 需要调用方来负责释放，否则会出现内存泄漏。
- schemaless 其他相关的接口，与 `ws_schemaless_insert_raw_ttl_with_reqid` 相比只是少一些参数，同名参数的含义完全相同，返回值定义也相同
  - `WS_RES *ws_schemaless_insert_raw(WS_TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision)`
  - `WS_RES *ws_schemaless_insert_raw_with_reqid(WS_TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int64_t reqid)`
  - `WS_RES *ws_schemaless_insert_raw_ttl(WS_TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl)`

### 5.5 数据订阅 API

#### 5.5.1 订阅配置参数

- `ws_tmq_conf_t *ws_tmq_conf_new()`
  - 功能说明：创建一个 `ws_tmq_conf_t` 结构体，用于配置消费参数。
  - 参数说明：无。
  - 返回值：配置 `ws_tmq_conf_t` 类型指针，NULL 表示失败。
- `ws_tmq_conf_res_t ws_tmq_conf_set(ws_tmq_conf_t *conf, const char *key, const char *value)`
  - 功能说明：设置消费参数。
  - 参数说明：
    - conf：tmq_conf_t 结构体指针。
    - key：参数名。
    - value：参数值。
  - 返回值：结果 `ws_tmq_conf_res_t` 枚举类型，`WS_TMQ_CONF_OK` 表示成功，其他值表示失败。
    ```c
    typedef enum ws_tmq_conf_res_t {
      WS_TMQ_CONF_UNKNOWN = -2,
      WS_TMQ_CONF_INVALID = -1,
      WS_TMQ_CONF_OK = 0,
    } ws_tmq_conf_res_t;
    ```

- `int ws_tmq_conf_destroy(ws_tmq_conf_t *conf)`
  - 功能说明：销毁 `ws_tmq_conf_t` 结构体。
  - 参数说明：
    - conf：`tmq_conf_t` 结构体指针
  - 返回值：成功时返回 0，失败时返回非 0 值。可通过 `ws_tmq_errstr` 函数获取错误信息。

#### 5.5.2 订阅主题

- `ws_tmq_list_t *ws_tmq_list_new()`
  - 功能说明：创建一个 `ws_tmq_list_t` 结构体，用于存储订阅的 topic。
  - 返回值：成功时返回 `ws_tmq_list_t` 结构体的指针，失败时返回 NULL。
- `int32_t ws_tmq_list_append(ws_tmq_list_t * list, const char * src)`
  - 功能说明：向 ws_tmq_list_t 结构体中添加一个 topic。
  - 参数说明：
    - list: `ws_tmq_list_t` 结构体的指针，为调用 `ws_tmq_list_new` 创建。
    - src: 要添加的 topic 的字符串指针。
  - 返回值：成功时返回 0，失败时返回非 0 值。可通过 `char *ws_tmq_errstr(int32_t code)` 函数获取错误信息。
- `int ``ws_tmq_list_destroy(ws_tmq_list_t * list)`
  - 功能说明：销毁 ws_tmq_list_t 结构体，释放相关资源。ws_tmq_list_new 创建的结果需要通过该接口销毁。
  - 参数说明：
    - list: ws_tmq_list_t 结构体的指针，为调用 `ws_tmq_list_new` 创建。
  - 成功时返回 0，失败时返回非 0 值。可通过 `ws_tmq_errstr` 函数获取错误信息。
- `int32_t ws_tmq_list_get_size(const ws_tmq_list_t * list)`
  - 功能说明：获取 ws_tmq_list_t 结构体中 topic 的个数。
  - 参数说明：
    - list: `ws_tmq_list_t` 结构体的指针，为调用 `ws_tmq_list_new` 创建。
  - 返回值：成功时返回 topic 的个数，失败时返回 -1。
- `char **ws_tmq_list_to_c_array(const ws_tmq_list_t * list)`
  - 功能说明：将 `ws_tmq_list_t` 结构体转换为 C 数组，数组每个元素为字符串指针，数组长度通过 `ws_tmq_list_get_size` 获得。
  - 参数说明：
    - list: `ws_tmq_list_t` 结构体的指针，为调用 `ws_tmq_list_new` 创建。
  - 返回值：成功时返回 C 数组的指针，每个元素是字符串指针，失败时返回 NULL。

#### 5.5.3 消费者和订阅

- `ws_tmq_t *ws_tmq_consumer_new(ws_tmq_conf_t *conf, char *errstr, int32_t errstrLen)`
  - 功能说明：创建一个 `ws_tmq_t` 结构体，用于消费数据。消费完数据后需调用 `ws_tmq_consumer_close` 关闭消费者。
  - 参数说明：
    - conf：配置消费参数的 `ws_tmq_conf_t` 结构体指针，调用 `ws_tmq_conf_new` 所返回 。
    - errstr：错误信息存储在这个字符串中，需自行分配内存，释放内存由调用者负责。
    - errstrLen：errstr 字符串的长度。
  - 返回值：成功时返回 `ws_tmq_t` 类型句柄，失败返回 NULL。
- `int32_t ws_tmq_subscribe(ws_tmq_t *tmq, const ws_tmq_list_t *topic_list)`
  - 功能说明：订阅 topic 列表。消费完数据后，需调用 `ws_tmq_unsubscribe` 取消订阅。
  - 参数说明：
    - tmq：`ws_tmq_consumer_new` 函数返回的句柄。
    - topic_list：要订阅的 topic 列表，`ws_tmq_list_t` 结构体指针。
  - 返回值：成功时返回 0，失败时返回非 0 值。可通过 `ws_tmq_errstr` 函数获取错误信息。
- `int32_t ws_tmq_unsubscribe(ws_tmq_t *tmq)`
  - 功能说明：取消订阅的 topic 列表。需与 `ws_tmq_subscribe` 配合使用。
  - 参数说明：
    - tmq：`ws_tmq_consumer_new` 函数返回的句柄。
  - 返回值：成功时返回 0，失败时返回非 0 值。可通过 `ws_tmq_errstr` 函数获取错误信息。
- `WS_RES *ws_tmq_consumer_poll(ws_tmq_t *tmq, int64_t timeout)`
  - 功能说明：轮询消费数据。每一个消费者，只能单线程调用该接口。
  - 参数说明：
    - tmq：`ws_tmq_consumer_new` 函数返回的句柄。
    - timeout：超时时间，单位为毫秒。表示多久没数据的话自动返回 NULL，负数的话默认超时 1 秒。
  - 返回值：有数据时返回 WS_RES 类型句柄，NULL 表示没有数据，非 NULL 表示有数据，WS_RES 结果集句柄和 `ws_taos_query` 返回结果一致，可通过查询的各种接口获取 WS_RES 里的信息，比如 schema 等。
- `int32_t ws_tmq_consumer_close(ws_tmq_t *tmq)`
  - 功能说明：关闭消费者。需与 `ws_tmq_consumer_new` 配合使用。
  -  参数说明：
    - tmq：`ws_tmq_consumer_new` 函数返回的句柄。
  - 返回值：成功时返回 0，失败时返回非 0 值。可通过 `ws_tmq_errstr` 函数获取错误信息。
- `const char *ws_tmq_get_table_name(WS_RES *res)`
  - 功能说明：获取返回结果所属的的表名。
  - 参数说明：
    - res：`ws_tmq_consumer_poll` 返回的结果集句柄。
  - 返回值：返回值为消费到的数据所属的表名，非 NULL 正常，NULL 失败。
- `ws_tmq_res_t ws_tmq_get_res_type(WS_RES *res)`
  - 功能说明：获取返回结果的类型。
  - 参数说明：
    - res：`ws_tmq_consumer_poll` 返回的结果集句柄。
  - 返回值：返回值为消费到的数据所属的类型，具体见下面 `ws_tmq_res_t` 的注释说明。目前仅支持 `TMQ_RES_DATA` 类型
    ```c
    typedef enum ws_tmq_res_t {
      TMQ_RES_INVALID = -1,   // invalid
      TMQ_RES_DATA = 1,       // 数据
      TMQ_RES_TABLE_META = 2, // 元数据
      TMQ_RES_METADATA = 3    // 既有元数据又有数据，即自动建表
    } tmq_res_t;
    ```

- `const char *ws_tmq_get_topic_name(WS_RES *res)`
  - 功能说明：获取返回结果所属的 topic 名。
  - 参数说明：
    - res：`ws_tmq_consumer_poll` 返回的结果集句柄。
  - 返回值：返回值为消费到的数据所属的 topic 名，非 NULL 正常，NULL 失败。
- `const char *ws_tmq_get_db_name(WS_RES *res)`
  - 功能说明：获取返回结果所属的数据库名。
  - 参数说明：
    - res：`ws_tmq_consumer_poll` 返回的结果集句柄。
  - 返回值：返回值为消费到的数据所属的数据库名，非 NULL 正常，NULL 失败。

#### 5.5.4 消费进度 API

- `int32_t ws_tmq_get_topic_assignment(ws_tmq_t *tmq, const char *pTopicName, ws_tmq_topic_assignment **assignment, int32_t *numOfAssignment)`
  - 功能说明：接口返回当前 consumer 分配的 vgroup 的信息，每个 vgroup 的信息包括 vgId，wal 的最大最小 offset，以及当前消费到的 offset。
  - 参数说明：
    - tmq：`ws_tmq_consumer_new` 函数返回的句柄。
    - pTopicName：主题名称。
    - assignment：分配的信息，数据大小为 numOfAssignment，需要通过 `ws_tmq_free_assignment` 接口释放。
    - numOfAssignment：分配给该 consumer 有效的 vgroup 个数。
  - 返回值：0 成功，非 0 失败，可通过 `ws_tmq_errstr` 函数获取错误信息。
- `int32_t`` ws_tmq_free_assignment(ws_tmq_topic_assignment* pAssignment, int32_t numOfAssignment)`
  - 功能说明：释放 `ws_mq_topic_assignment` 结构体资源。
  - 参数说明：
    - pAssignment：要释放的 `ws_tmq_topic_assignment` 结构体指针。结构体定义如下：
      ```c
      typedef struct ws_tmq_topic_assignment {
        int32_t vgId;
        int64_t currentOffset;
        int64_t begin;
        int64_t end;
      } ws_tmq_topic_assignment;
      ```

    - 返回值：错误码，0 成功，非 0 失败，可通过 `ws_tmq_errstr` 函数获取错误信息。
- `int64_t ws_tmq_committed(ws_tmq_t *tmq, const char *pTopicName, int32_t vgId)`
  - 功能说明：获取当前 consumer 在某个 topic 和 vgroup 上的 commit 位置。
  - 参数说明：
    - tmq：`ws_tmq_consumer_new` 函数返回的句柄。
    - pTopicName：主题名称。
    - vgId：vgroup 的 ID。
  - 返回值：当前 commit 的位置，-2147467247 表示没有消费进度，其他小于 0 的值表示失败，错误码就是返回值。
- `int32_t ws_tmq_commit_sync(ws_tmq_t *tmq, const WS_RES *msg)`
  - 功能说明：根据消息提交，提交消息里的进度，如果消息传 NULL，提交当前 consumer 所有消费的 vgroup 的当前进度。
  - 参数说明：
    - tmq：`ws_tmq_consumer_new` 函数返回的句柄。
    - msg：消费到的消息结构，如果 msg 传 NULL，提交当前 consumer 所有消费的 vgroup 的当前进度。
  - 返回值：错误码，0 成功，非 0 失败，可通过 `ws_tmq_errstr` 函数获取错误信息。
- `int32_t ws_tmq_commit_offset_sync(ws_tmq_t *tmq, const char *pTopicName, int32_t vgId, ws_int64_t offset)`
  - 功能说明：根据某个 topic 的某个 vgroup 的 offset 提交。
  - 参数说明：
    - tmq：`ws_tmq_consumer_new` 函数返回的句柄。
    - pTopicName：主题名称。
    - vgId：vgroup 的 ID。
    - offset：要提交的 offset。
  - 返回值：错误码，0 成功，非 0 失败，可通过 `ws_tmq_errstr` 函数获取错误信息。
- `int64_t ws_tmq_position(ws_tmq_t *tmq, const char *pTopicName, int32_t vgId)`
  - 功能说明：获取当前消费位置，为消费到的数据位置的下一个位置。
  - 参数说明：
    - tmq：`ws_tmq_consumer_new` 函数返回的句柄。
    - pTopicName：主题名称。
    - vgId：vgroup 的 ID。
  - 返回值：消费位置，负数失败，可通过 `ws_tmq_errstr` 函数获取错误信息。
- `int32_t ws_tmq_offset_seek(ws_tmq_t *tmq, const char *pTopicName, int32_t vgId, ws_int64_t offset)`
  - 功能说明：设置 consumer 在某个 topic 的某个 vgroup 的 offset 位置，开始消费。
  - 参数说明：
    - tmq：`ws_tmq_consumer_new` 函数返回的句柄。
    - pTopicName：主题名称。
    - vgId：vgroup 的 ID。
    - offset：要设置的 offset。
  - 返回值：错误码，0 成功，非 0 失败，可通过 `ws_tmq_errstr` 函数获取错误信息。
- `int64_t ws_tmq_get_vgroup_offset(WS_RES* res)`
  - 功能说明：获取 poll 消费到的数据的起始 offset。
  - 参数说明：
    - res：`ws_tmq_consumer_poll` 返回的结果集句柄。
  - 返回值：消费到的 offset，负数失败，可通过 `ws_tmq_errstr` 函数获取错误信息。
- `int32_t ws_tmq_get_vgroup_id(WS_RES *res)`
  - 功能说明：获取 poll 消费到的数据的所属的 vgroup id。
  - 参数说明：
    - res：`ws_tmq_consumer_poll` 返回的结果集句柄。
  - 返回值：消费到的数据所属的 vgroup id，负数失败，可通过 `ws_tmq_errstr` 函数获取错误信息。

#### 5.5.5 错误信息 API

- `const char *ws_tmq_errstr(ws_tmq_t *tmq)`
- 功能说明
  - 该接口用于获取上次数据订阅调用出错的错误信息
- 参数说明
  - tmq：`ws_tmq_consumer_new` 函数返回的句柄。
- 返回值
  - 非 NULL，返回错误信息，错误信息可能为空字符串

## 6. 性能

SQL 写入和查询目标达到原生连接的 80%以上。
STMT 接口引擎组正在优化，旧接口对 Websocket 性能支持不佳，后续随 STMT 新接口优化。
TMQ 订阅性能目标达到原生连接 80% 以上（自己写测试代码单线程验证，与 Native 结果对比）。

## 7. 兼容性

涉及少量已有接口调整，会影响使用 libtaosws.so 的其他组件如 ODBC， taos shell 等。必须一起修改。@裴亚明 @段宽军 @潘魏
建议服务端使用 3.3.2.0 及以上版本， 更早版本理论上也支持，但是性能不是最优。

## 8. 运维

客户端库，不涉及运维。

## 9. 使用场景

客户应用开发依赖 C/C++ Websocket 库，可以正常对 TDengine 进行读写，以及数据订阅。

## 10. 约束和限制

支持的平台请参考：https://docs.taosdata.com/connector/#%E6%94%AF%E6%8C%81%E7%9A%84%E5%B9%B3%E5%8F%B0 C/C++ 部分

## 11. 常见错误和排查

暂无

## 12. 可观测性

可以设置连接器的日志配置，来将日志打印到控制台。
日志级别支持："error", "warn", "info", "debug" 和 "trace"。
打开日志方式：
1. 程序中调用 int ws_enable_log（const char* log_level)， 进行日志初始化，设置日志级别，日志会打印到控制台
     log_level 是日志级别， 返回值 0 表示成功，其他失败
1. 环境变量方式设置日志级别，优先级高于接口调用。如：LIBTAOSWS_LOG_LEVEL=trace

## 13. 安装和卸载

随 TDengine 客户端一起安装和卸载

## 14. 文档

1. 需要修改官网文档
2. 不需要修改企业版文档

## 15. 参考文档

https://docs.taosdata.com/connector/cpp/

## 16. 附录

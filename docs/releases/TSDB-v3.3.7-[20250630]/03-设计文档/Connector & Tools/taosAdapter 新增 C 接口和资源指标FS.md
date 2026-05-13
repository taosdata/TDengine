# taosAdapter 新增 C 接口和资源指标FS

## 1. 背景

TS-6567

增加 taosAdaper 调用 C 接口次数和 WebSocket 占用 C 资源指标。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/05/24 | 0.1 | 谭雪峰 | 编写文档 |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

WebSocket 占用 C 资源：各个 WebSocket 接口持有的 sql 查询结果，stmt 和 stmt2 句柄。

## 4. 行为说明

参考 [TDengine 监测](https://taosdata.feishu.cn/wiki/B1W1wfUu8iSefQktLI3cRfeHntd) 5.5.1.2 接口协议章节格式编码
1. 新增以下 C 接口调用指标，通过 taosKeeper 写入到 adapter_c_interface 数据表，tag : `endpoint`

| 指标 | 描述 |
| --- | --- |
| taos_connect_total | 尝试建立连接的总次数 |
| taos_connect_success | 成功建立连接的次数 |
| taos_connect_fail | 建立连接失败的次数 |
| taos_close_total | 尝试关闭连接的总次数 |
| taos_close_success | 成功关闭连接的次数 |
| taos_schemaless_insert_total | schemaless 插入操作的总次数 |
| taos_schemaless_insert_success | schemaless 插入成功的次数 |
| taos_schemaless_insert_fail | schemaless 插入失败的次数 |
| taos_schemaless_free_result_total | schemaless 释放结果集的总次数 |
| taos_schemaless_free_result_success | schemaless 成功释放结果集的次数 |
| taos_query_total | 执行同步 SQL 的总次数 |
| taos_query_success | 执行同步 SQL 成功的次数 |
| taos_query_fail | 执行同步 SQL 失败的次数 |
| taos_query_free_result_total | 释放同步 SQL 结果集的总次数 |
| taos_query_free_result_success | 成功释放同步 SQL 结果集的次数 |
| taos_query_a_with_reqid_total | 带请求 ID 的异步 SQL 总次数 |
| taos_query_a_with_reqid_success | 带请求 ID 的异步 SQL 成功次数 |
| taos_query_a_with_reqid_callback_total | 带请求 ID 的异步 SQL 回调总次数 |
| taos_query_a_with_reqid_callback_success | 带请求 ID 的异步 SQL 回调成功次数 |
| taos_query_a_with_reqid_callback_fail | 带请求 ID 的异步 SQL 回调失败次数 |
| taos_query_a_free_result_total | 异步 SQL 释放结果集的总次数 |
| taos_query_a_free_result_success | 异步 SQL 成功释放结果集的次数 |
| tmq_consumer_poll_result_total | 消费者 poll 有数据的总次数 |
| tmq_free_result_total | 释放 TMQ 数据的总次数 |
| tmq_free_result_success | 成功释放 TMQ 数据的次数 |
| taos_stmt2_init_total | stmt2 初始化的总次数 |
| taos_stmt2_init_success | stmt2 初始化成功的次数 |
| taos_stmt2_init_fail | stmt2 初始化失败的次数 |
| taos_stmt2_close_total | stmt2 关闭的总次数 |
| taos_stmt2_close_success | stmt2 关闭成功的次数 |
| taos_stmt2_close_fail | stmt2 关闭失败的次数 |
| taos_stmt2_get_fields_total | stmt2 获取字段的总次数 |
| taos_stmt2_get_fields_success | stmt2 成功获取字段的次数 |
| taos_stmt2_get_fields_fail | stmt2 获取字段失败的次数 |
| taos_stmt2_free_fields_total | stmt2 释放字段的总次数 |
| taos_stmt2_free_fields_success | stmt2 成功释放字段的次数 |
| taos_stmt_init_with_reqid_total | 带请求 ID 的 stmt 初始化总次数 |
| taos_stmt_init_with_reqid_success | 带请求 ID 的 stmt 初始化成功次数 |
| taos_stmt_init_with_reqid_fail | 带请求 ID 的 stmt 初始化失败次数 |
| taos_stmt_close_total | stmt 关闭的总次数 |
| taos_stmt_close_success | stmt 关闭成功的次数 |
| taos_stmt_close_fail | stmt 关闭失败的次数 |
| taos_stmt_get_tag_fields_total | stmt 获取 tag 字段的总次数 |
| taos_stmt_get_tag_fields_success | stmt 成功获取 tag 字段的次数 |
| taos_stmt_get_tag_fields_fail | stmt 获取 tag 字段失败的次数 |
| taos_stmt_get_col_fields_total | stmt 获取列字段的总次数 |
| taos_stmt_get_col_fields_success | stmt 成功获取列字段的次数 |
| taos_stmt_get_col_fields_fail | stmt 获取列字段失败的次数 |
| taos_stmt_reclaim_fields_total | stmt 释放字段的总次数 |
| taos_stmt_reclaim_fields_success | stmt 成功释放字段的次数 |
| tmq_get_json_meta_total | tmq 获取 JSON 元数据的总次数 |
| tmq_get_json_meta_success | tmq 成功获取 JSON元数据的次数 |
| tmq_free_json_meta_total | tmq 释放 JSON 元数据的总次数 |
| tmq_free_json_meta_success | tmq 成功释放 JSON 元数据的次数 |
| taos_fetch_whitelist_a_total | 异步获取白名单的总次数 |
| taos_fetch_whitelist_a_success | 异步成功获取白名单的次数 |
| taos_fetch_whitelist_a_callback_total | 异步获取白名单回调总次数 |
| taos_fetch_whitelist_a_callback_success | 异步成功获取白名单回调次数 |
| taos_fetch_whitelist_a_callback_fail | 异步获取白名单回调失败次数 |
| taos_fetch_rows_a_total | 异步获取行的总次数 |
| taos_fetch_rows_a_success | 异步成功获取行的次数 |
| taos_fetch_rows_a_callback_total | 异步获取行回调总次数 |
| taos_fetch_rows_a_callback_success | 异步成功获取行回调次数 |
| taos_fetch_rows_a_callback_fail | 异步获取行回调失败次数 |
| taos_fetch_raw_block_a_total | 异步获取原始块的总次数 |
| taos_fetch_raw_block_a_success | 异步成功获取原始块的次数 |
| taos_fetch_raw_block_a_callback_total | 异步获取原始块回调总次数 |
| taos_fetch_raw_block_a_callback_success | 异步成功获取原始块回调次数 |
| taos_fetch_raw_block_a_callback_fail | 异步获取原始块回调失败次数 |
| tmq_get_raw_total | 获取原始数据的总次数 |
| tmq_get_raw_success | 成功获取原始数据的次数 |
| tmq_get_raw_fail | 获取原始数据失败的次数 |
| tmq_free_raw_total | 释放原始数据的总次数 |
| tmq_free_raw_success | 成功释放原始数据的次数 |
| tmq_consumer_new_total | 创建新消费者的总次数 |
| tmq_consumer_new_success | 成功创建新消费者的次数 |
| tmq_consumer_new_fail | 创建新消费者失败的次数 |
| tmq_consumer_close_total | 关闭消费者的总次数 |
| tmq_consumer_close_success | 成功关闭消费者的次数 |
| tmq_consumer_close_fail | 关闭消费者失败的次数 |
| tmq_subscribe_total | 订阅主题的总次数 |
| tmq_subscribe_success | 成功订阅主题的次数 |
| tmq_subscribe_fail | 订阅主题失败的次数 |
| tmq_unsubscribe_total | 取消订阅的总次数 |
| tmq_unsubscribe_success | 成功取消订阅的次数 |
| tmq_unsubscribe_fail | 取消订阅失败的次数 |
| tmq_list_new_total | 创建新主题列表的总次数 |
| tmq_list_new_success | 成功创建新主题列表的次数 |
| tmq_list_new_fail | 创建新主题列表失败的次数 |
| tmq_list_destroy_total | 销毁主题列表的总次数 |
| tmq_list_destroy_success | 成功销毁主题列表的次数 |
| tmq_conf_new_total | tmq 创建新配置的总次数 |
| tmq_conf_new_success | tmq 成功创建新配置的次数 |
| tmq_conf_new_fail | tmq 创建新配置失败的次数 |
| tmq_conf_destroy_total | tmq 销毁配置的总次数 |
| tmq_conf_destroy_success | tmq 成功销毁配置的次数 |
| taos_stmt2_prepare_total | stmt2 准备的总次数 |
| taos_stmt2_prepare_success | stmt2 准备成功的次数 |
| taos_stmt2_prepare_fail | stmt2 准备失败的次数 |
| taos_stmt2_is_insert_total | 检查是否为插入的总次数 |
| taos_stmt2_is_insert_success | 成功检查是否为插入的次数 |
| taos_stmt2_is_insert_fail | 检查是否为插入失败的次数 |
| taos_stmt2_bind_param_total | stmt2 绑定参数的总次数 |
| taos_stmt2_bind_param_success | stmt2 成功绑定参数的次数 |
| taos_stmt2_bind_param_fail | stmt2 绑定参数失败的次数 |
| taos_stmt2_exec_total | stmt2 执行的总次数 |
| taos_stmt2_exec_success | stmt2 执行成功的次数 |
| taos_stmt2_exec_fail | stmt2 执行失败的次数 |
| taos_stmt2_error_total | stmt2 错误检查的总次数 |
| taos_stmt2_error_success | stmt2 成功检查错误的次数 |
| taos_fetch_row_total | 同步获取行的总次数 |
| taos_fetch_row_success | 成功同步获取行的次数 |
| taos_is_update_query_total | 检查是否为更新语句的总次数 |
| taos_is_update_query_success | 成功检查是否为更新语句的次数 |
| taos_affected_rows_total | SQL 获取影响行数的总次数 |
| taos_affected_rows_success | SQL 成功获取影响行数的次数 |
| taos_num_fields_total | 获取字段数量的总次数 |
| taos_num_fields_success | 成功获取字段数量的次数 |
| taos_fetch_fields_e_total | 获取字段信息的扩展总次数 |
| taos_fetch_fields_e_success | 成功获取字段信息的扩展次数 |
| taos_fetch_fields_e_fail | 获取字段信息的扩展失败次数 |
| taos_result_precision_total | 获取结果精度的总次数 |
| taos_result_precision_success | 成功获取结果精度的次数 |
| taos_get_raw_block_total | 获取原始块的总次数 |
| taos_get_raw_block_success | 成功获取原始块的次数 |
| taos_fetch_raw_block_total | 拉取原始块的总次数 |
| taos_fetch_raw_block_success | 成功拉取原始块的次数 |
| taos_fetch_raw_block_fail | 拉取原始块失败的次数 |
| taos_fetch_lengths_total | 获取字段长度的总次数 |
| taos_fetch_lengths_success | 成功获取字段长度的次数 |
| taos_write_raw_block_with_reqid_total | 带请求 ID 写入原始块的总次数 |
| taos_write_raw_block_with_reqid_success | 带请求 ID 成功写入原始块的次数 |
| taos_write_raw_block_with_reqid_fail | 带请求 ID 写入原始块失败的次数 |
| taos_write_raw_block_with_fields_with_reqid_total | 带请求 ID 和字段写入原始块的总次数 |
| taos_write_raw_block_with_fields_with_reqid_success | 带请求 ID 和字段成功写入原始块的次数 |
| taos_write_raw_block_with_fields_with_reqid_fail | 带请求 ID 和字段写入原始块失败的次数 |
| tmq_write_raw_total | 写入原始数据的 TMQ 总次数 |
| tmq_write_raw_success | 成功写入原始数据的 TMQ 次数 |
| tmq_write_raw_fail | 写入原始数据的 TMQ 失败次数 |
| taos_stmt_prepare_total | stmt 准备的总次数 |
| taos_stmt_prepare_success | stmt 准备成功的次数 |
| taos_stmt_prepare_fail | stmt 准备失败的次数 |
| taos_stmt_is_insert_total | 检查 stmt 是否为插入的总次数 |
| taos_stmt_is_insert_success | 成功检查 stmt 是否为插入的次数 |
| taos_stmt_is_insert_fail | 检查 stmt 是否为插入失败的次数 |
| taos_stmt_set_tbname_total | stmt 设置表名的总次数 |
| taos_stmt_set_tbname_success | stmt 成功设置表名的次数 |
| taos_stmt_set_tbname_fail | stmt 设置表名失败的次数 |
| taos_stmt_set_tags_total | stmt 设置tag的总次数 |
| taos_stmt_set_tags_success | stmt 成功设置tag的次数 |
| taos_stmt_set_tags_fail | stmt 设置tag失败的次数 |
| taos_stmt_bind_param_batch_total | stmt 批量绑定参数的总次数 |
| taos_stmt_bind_param_batch_success | stmt 成功批量绑定参数的次数 |
| taos_stmt_bind_param_batch_fail | stmt 批量绑定参数失败的次数 |
| taos_stmt_add_batch_total | stmt 添加批处理的总次数 |
| taos_stmt_add_batch_success | stmt 成功添加批处理的次数 |
| taos_stmt_add_batch_fail | stmt 添加批处理失败的次数 |
| taos_stmt_execute_total | stmt 执行的总次数 |
| taos_stmt_execute_success | stmt 执行成功的次数 |
| taos_stmt_execute_fail | stmt 执行失败的次数 |
| taos_stmt_num_params_total | stmt 获取参数数量的总次数 |
| taos_stmt_num_params_success | stmt 成功获取参数数量的次数 |
| taos_stmt_num_params_fail | stmt 获取参数数量失败的次数 |
| taos_stmt_get_param_total | stmt 获取参数的总次数 |
| taos_stmt_get_param_success | stmt 成功获取参数的次数 |
| taos_stmt_get_param_fail | stmt 获取参数失败的次数 |
| taos_stmt_errstr_total | stmt 获取 stmt 错误信息的总次数 |
| taos_stmt_errstr_success | stmt 成功获取 stmt 错误信息的次数 |
| taos_stmt_affected_rows_once_total | stmt 获取单次影响行数的总次数 |
| taos_stmt_affected_rows_once_success | stmt 成功获取单次影响行数的次数 |
| taos_stmt_use_result_total | stmt 使用结果集的总次数 |
| taos_stmt_use_result_success | stmt 成功使用结果集的次数 |
| taos_stmt_use_result_fail | stmt 使用结果集失败的次数 |
| taos_select_db_total | 选择数据库的总次数 |
| taos_select_db_success | 成功选择数据库的次数 |
| taos_select_db_fail | 选择数据库失败的次数 |
| taos_get_tables_vgId_total | 获取表 vgroup ID 的总次数 |
| taos_get_tables_vgId_success | 成功获取表 vgroup ID 的次数 |
| taos_get_tables_vgId_fail | 获取表 vgroup ID 失败的次数 |
| taos_options_connection_total | 设置连接选项的总次数 |
| taos_options_connection_success | 成功设置连接选项的次数 |
| taos_options_connection_fail | 设置连接选项失败的次数 |
| taos_validate_sql_total | 验证SQL的总次数 |
| taos_validate_sql_success | 成功验证SQL的次数 |
| taos_validate_sql_fail | 验证SQL失败的次数 |
| taos_check_server_status_total | 检查服务器状态的总次数 |
| taos_check_server_status_success | 成功检查服务器状态的次数 |
| taos_get_current_db_total | 获取当前数据库的总次数 |
| taos_get_current_db_success | 成功获取当前数据库的次数 |
| taos_get_current_db_fail | 获取当前数据库失败的次数 |
| taos_get_server_info_total | 获取服务器信息的总次数 |
| taos_get_server_info_success | 成功获取服务器信息的次数 |
| taos_options_total | 设置选项的总次数 |
| taos_options_success | 成功设置选项的次数 |
| taos_options_fail | 设置选项失败的次数 |
| taos_set_conn_mode_total | 设置连接模式的总次数 |
| taos_set_conn_mode_success | 成功设置连接模式的次数 |
| taos_set_conn_mode_fail | 设置连接模式失败的次数 |
| taos_reset_current_db_total | 重置当前数据库的总次数 |
| taos_reset_current_db_success | 成功重置当前数据库的次数 |
| taos_set_notify_cb_total | 设置通知回调的总次数 |
| taos_set_notify_cb_success | 成功设置通知回调的次数 |
| taos_set_notify_cb_fail | 设置通知回调失败的次数 |
| taos_errno_total | 获取错误码的总次数 |
| taos_errno_success | 成功获取错误码的次数 |
| taos_errstr_total | 获取错误信息的总次数 |
| taos_errstr_success | 成功获取错误信息的次数 |
| tmq_consumer_poll_total | tmq 消费者 poll 的总次数 |
| tmq_consumer_poll_success | tmq 消费者 poll 成功的次数 |
| tmq_consumer_poll_fail | tmq 消费者 poll 失败的次数 |
| tmq_subscription_total | tmq 获取订阅信息的总次数 |
| tmq_subscription_success | tmq 成功获取订阅信息的次数 |
| tmq_subscription_fail | tmq 获取订阅信息失败的次数 |
| tmq_list_append_total | tmq 列表追加的总次数 |
| tmq_list_append_success | tmq 成功列表追加的次数 |
| tmq_list_append_fail | tmq 列表追加失败的次数 |
| tmq_list_get_size_total | tmq 获取列表大小的总次数 |
| tmq_list_get_size_success | tmq 成功获取列表大小的次数 |
| tmq_err2str_total | tmq 错误码转字符串的总次数 |
| tmq_err2str_success | tmq 成功将错误码转为字符串的次数 |
| tmq_conf_set_total | tmq 设置配置的总次数 |
| tmq_conf_set_success | tmq 成功设置配置的次数 |
| tmq_conf_set_fail | tmq 设置配置失败的次数 |
| tmq_get_res_type_total | tmq 获取资源类型的总次数 |
| tmq_get_res_type_success | tmq 成功获取资源类型的次数 |
| tmq_get_topic_name_total | tmq 获取主题名称的总次数 |
| tmq_get_topic_name_success | tmq 成功获取主题名称的次数 |
| tmq_get_vgroup_id_total | tmq 获取 vgroup ID 的总次数 |
| tmq_get_vgroup_id_success | tmq 成功获取 vgroup ID 的次数 |
| tmq_get_vgroup_offset_total | tmq 获取 vgroup 偏移量的总次数 |
| tmq_get_vgroup_offset_success | tmq 成功获取 vgroup 偏移量的次数 |
| tmq_get_db_name_total | tmq 获取数据库名称的总次数 |
| tmq_get_db_name_success | tmq 成功获取数据库名称的次数 |
| tmq_get_table_name_total | tmq 获取表名称的总次数 |
| tmq_get_table_name_success | tmq 成功获取表名称的次数 |
| tmq_get_connect_total | tmq 获取连接的总次数 |
| tmq_get_connect_success | tmq 成功获取连接的次数 |
| tmq_commit_sync_total | tmq 同步提交的总次数 |
| tmq_commit_sync_success | tmq 同步提交成功的次数 |
| tmq_commit_sync_fail | tmq 同步提交失败的次数 |
| tmq_fetch_raw_block_total | tmq 获取原始块的总次数 |
| tmq_fetch_raw_block_success | tmq 成功获取原始块的次数 |
| tmq_fetch_raw_block_fail | tmq 获取原始块失败的次数 |
| tmq_get_topic_assignment_total | tmq 获取主题分配的总次数 |
| tmq_get_topic_assignment_success | tmq 成功获取主题分配的次数 |
| tmq_get_topic_assignment_fail | tmq 获取主题分配失败的次数 |
| tmq_offset_seek_total | tmq 偏移量定位的总次数 |
| tmq_offset_seek_success | tmq 成功偏移量定位的次数 |
| tmq_offset_seek_fail | tmq 偏移量定位失败的次数 |
| tmq_committed_total | tmq 获取已提交偏移量的总次数 |
| tmq_committed_success | tmq 成功获取已提交偏移量的次数 |
| tmq_commit_offset_sync_fail | tmq 同步提交偏移量失败的次数 |
| tmq_position_total | tmq 获取当前位置的总次数 |
| tmq_position_success | tmq 成功获取当前位置的次数 |
| tmq_commit_offset_sync_total | tmq 同步提交偏移量的总次数 |
| tmq_commit_offset_sync_success | tmq 同步提交偏移量成功的次数 |

1. adapter_status 表新增以下指标

| 指标 | 描述 |
| --- | --- |
| ws_query_conn_inc | /rest/ws 接口新增连接 |
| ws_query_conn_dec | /rest/ws 接口减少连接 |
| ws_stmt_conn_inc | /rest/stmt 接口新增连接 |
| ws_stmt_conn_dec | /rest/stmt 接口减少连接 |
| ws_sml_conn_inc | /rest/schemaless 接口新增连接 |
| ws_sml_conn_dec | /rest/schemaless 接口减少连接 |
| ws_ws_conn_inc | /ws 接口新增连接 |
| ws_ws_conn_dec | /ws 接口减少连接 |
| ws_tmq_conn_inc | /rest/tmq 接口新增连接 |
| ws_tmq_conn_dec | /rest/tmq 接口减少连接 |
| ws_query_sql_result_count | /rest/ws 接口当前持有 SQL 查询结果数量 |
| ws_stmt_stmt_count | /rest/stmt 接口当前持有 stmt 数量 |
| ws_ws_sql_result_count | /ws 接口当前持有 SQL 查询结果数量 |
| ws_ws_stmt_count | /ws 接口当前持有 stmt 数量 |
| ws_ws_stmt2_count | /ws 接口当前持有 stmt2 数量 |

## 5. 性能

增加日志和和指标统计预计会降低性能。

## 6. 兼容性

taosAdapter 保持对旧版本 taosKeeper 兼容

## 7. 运维

无。

## 8. 使用场景

taosAdapter 监控。

## 9. 约束和限制

无。

## 10. 常见错误和排查

无。

## 11. 可观测性

1. 通过 log 库可查询对应指标

## 12. 安装和卸载

无。

## 13. 文档

## 14. 参考文档

[taosAdapter连接池监控fs](https://taosdata.feishu.cn/wiki/VAJDwgRJ5iUhzBkm6Y1cFConnec)

## 15. 附录

无。

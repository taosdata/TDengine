# API-数据写入重构接口

现有的数据写入任务状态获取采用 http 接口轮询的方式，任务多的时候请求非常频繁，对页面性能也有一定影响。先考虑优化为 ws 接口，具体定义如下：

#### /datain/task/activity

1. B to S 上行消息

| 消息 | 示例 | 说明 |
| --- | --- | --- |
| 订阅 | [{"id": 7, action: "verbose"}, ...] | 对任务${id} 进行 ${action} 操作； Id 为 data in 任务 id; action 可选值有： verbose - 订阅详细信息，可获取 activity 列表 brief - 获取任务状态信息 none - 取消订阅 |


1. S to B 下行消息

| 消息 | 示例 | 说明 |
| --- | --- | --- |
| 任务事件 | [{ "id": 7, status: "error", activities: [{...}] }] | 对任务${id} 进行 ${action} 操作； Id 为 data in 任务 id; status 运行状态，可选值有： error - 异常 warn - 警告 healthy - 正常 activities：只有订阅模式在 verbose 时才返回此列表。只推送最新的 activity 列表。 |

#### 涉及 dsn 参数的接口

1. 新增/编辑数据源  /tasks/{id}
2. 连通性检查 /ds/in/validate
3. Opc csv 文件校验 /ds/in/point/file/is_valid
4. 获取数据源列表 /tasks/{id}
5. Csv 文件解析接口 /filemeta
目前配置参数是以 dsn 字符串的方式传递，将 dsn 字符串修改为 json 的方式传递数据，其余参数格式不变
```json
{
    "name": "",
    "type": "mqtt",
    "targetDB": "",
    "agent": "",
    "data": {
        "connection_options": {// 连接配置
            "host": "",
            "port": "",
            "protocol": ""
        },
        "authentication": {// 认证配置
            "plain": {
                "username": "",
                "password": ""
            },
            "currentTab": "plain"
        },
        "groups_before": {// 连通性需要配置组
            "ssl": {
                "ca": "",
                "cert": "",
                "cert_key": "",
                "isEnable": false
            },
            "collect": {
                "version": "3.1",
                "client_id": "",
                "keep_alive": 60,
                "clean_session": true,
                "topics": "",
                "compression": "none",
                "char_encoding": "UTF_8"
            }
        },
        "checkConnectivity": "",
        "groups_after": { // 在连通性检查之后的配置
            "mode":{
                "collect_mode": "subscribe"
                "interval": 10
                "request_timeout": 10
                "update_interval": 600
             }
        },
        "datasets": { // 数据点位
            "csv_config_file": "",
            "select_all_points": {
                "root": "",
                "namespaces": "",
                "node_id_pattern": "",
                "browse_name_pattern": "",
                "super_table_expression": "opc_{type}",
                "child_table_expression": "t_{ns}_{id}",
                "table_primary_key": "original_ts",
                "table_primary_key_alias": "ts"
            },
            "currentTab": "csv_config_file"
        },
        "advanced_options": {// 高级选项
            "unprocessed_messages_buffer_size": 50000,
            "maximum_processing_batch": 100,
            "batch_size": 1000,
            "batch_timeout": 500,
            "keep_raw_data": false,
            "keep_raw_data_days": 1,
            "keep_raw_data_dir": "",
            "health_check_window_in_second_type": "s",
            "busy_threshold": 100,
            "busy_threshold_type": "%",
            "max_queue_length": 1000,
            "max_errors_in_window": 10
        },
        "write_config": {// 写入配置
            "primary_timestamp_overflow": "archive",
            "primary_timestamp_null": "archive",
            "table_name_length_overflow": "archive",
            "table_name_contains_illegal_char": "",
            "table_name_contains_illegal_char_type": "replace_to",
            "variable_not_exist_in_table_name_template": "",
            "variable_not_exist_in_table_name_template_type": "replace_to",
            "field_name_length_overflow": "archive"
        }
    }
}
```

#### 数据源完整 json

##### 3.1  MQTT

```json
// mqtt 数据源完整参数
{
    "name": "",
    "type": "mqtt",
    "targetDB": "",
    "agent": "",
    "data": {
        "connection_options": {
            "host": "",
            "port": ""
        },
        "authentication": {
            "plain": {
                "username": "",
                "password": ""
            },
            "currentTab": "plain"
        },
        "groups_before": {
            "ssl": {
                "ca": "",
                "cert": "",
                "cert_key": "",
                "isEnable": true
            },
            "collect": {
                "version": "3.1",
                "client_id": "",
                "keep_alive": 60,
                "clean_session": true,
                "topics": "",
                "topic_pattern": "",
                "compression": "none",
                "char_encoding": "UTF_8"
            }
        },
        "checkConnectivity": "",
        "groups_after": "",
        "parser": {
            "parse": {
                "payload": {
                    "json": [],
                    "keep": true
                }
            },
            "model": {
                "name": "",
                "using": "",
                "columns": [
                    "ts"
                ],
                "tags": []
            }
        },
        "advanced_options": {
            "unprocessed_messages_buffer_size": 50000,
            "maximum_processing_batch": 100,
            "batch_size": 1000,
            "batch_timeout": 500,
            "keep_raw_data": false,
            "keep_raw_data_days": 1,
            "keep_raw_data_dir": "",
            "health_check_window_in_second_type": "s",
            "busy_threshold": 100,
            "busy_threshold_type": "%",
            "max_queue_length": 1000,
            "max_errors_in_window": 10
        },
        "write_config": {
            "primary_timestamp_overflow": "archive",
            "primary_timestamp_null": "archive",
            "table_name_length_overflow": "archive",
            "table_name_contains_illegal_char": "",
            "table_name_contains_illegal_char_type": "replace_to",
            "variable_not_exist_in_table_name_template": "",
            "variable_not_exist_in_table_name_template_type": "replace_to",
            "field_name_length_overflow": "archive"
        }
    }
}
```

##### 3.2 CSV

```json
{
    "name": "",
    "type": "csv",
    "targetDB": "",
    "agent": "",
    "data": {
        "groups_before": "",
        "groups_after": {
            "0d14aa37-292f-4d91-89a5-7f9f90bfe72a": {
                "has_header": false,
                "skip": 0,
                "delimiter": ",",
                "quote": "\"",
                "comment": "#"
            }
        },
        "csvData": {
            "upload_csv_file": {
                "keep_processed_files": false,
                "path": ""
            },
            "monitor_file_directory": {
                "path": "./files/1736822656521/数据点位.csv",
                "file_pattern": "",
                "new_file_notify": true,
                "notify_interval": 30,
                "sort": "1"
            },
            "currentTab": "monitor_file_directory"
        },
        "advanced_options": {
            "read_concurrency": 0,
            "batch_size": 1000,
            "health_check_window_in_second_type": "s",
            "busy_threshold": 100,
            "busy_threshold_type": "%",
            "max_queue_length": 1000,
            "max_errors_in_window": 10
        },
        "write_config": {
            "primary_timestamp_overflow": "archive",
            "primary_timestamp_null": "archive",
            "table_name_length_overflow": "archive",
            "table_name_contains_illegal_char": "",
            "table_name_contains_illegal_char_type": "replace_to",
            "variable_not_exist_in_table_name_template": "",
            "variable_not_exist_in_table_name_template_type": "replace_to",
            "field_name_length_overflow": "archive"
        }
    }
}
```

#####

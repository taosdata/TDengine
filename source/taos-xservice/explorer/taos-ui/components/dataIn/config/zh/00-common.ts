export default {
  exceptionStrategy: `{
      "label": "异常处理策略",
      "field": "write_config",
      "description": "对写入策略配置参数进行调整，可修改以下选项。",
      "type": "collapse",
      "defaultValue": true,
      "collapsible": "one",
      "children": [
        {
          "label": "目标库连接超时",
          "field": "database_connection_error",
          "description": "目标库连接超时的操作，可选：归档、丢弃、报错、缓存。默认：缓存。",
          "defaultValue": "cache",
          "required": false,
          "type": "select",
          "options": [
            {
              "value": "cache",
              "label": "缓存"
            },
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            }
          ]
        },
        {
          "label": "目标库不存在",
          "field": "database_not_exist",
          "description": "目标库不存在的操作，可选：归档、丢弃、报错。默认：报错。",
          "defaultValue": "break",
          "required": false,
          "type": "select",
          "options": [
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            }
          ]
        },
        {
          "label": "表不存在",
          "field": "table_not_exist",
          "description": "表不存在的操作，可选：归档、丢弃、报错、重试。默认：自动建表并重试。",
          "defaultValue": "retry",
          "required": false,
          "type": "select",
          "options": [
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            },
            {
              "value": "retry",
              "label": "自动建表并重试"
            }
          ]
        },
        {
          "label": "主键时间戳溢出",
          "field": "primary_timestamp_overflow",
          "description": "表示时间戳溢出时的操作，可选：归档、丢弃、报错。默认：归档。",
          "defaultValue": "archive",
          "required": false,
          "type": "select",
          "options": [
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            }
          ]
        },
        {
          "label": "主键时间戳空",
          "field": "primary_timestamp_null",
          "description": "表示时间戳为空时的操作，可选：使用当前时间、归档、丢弃、报错。默认：归档。",
          "defaultValue": "archive",
          "required": false,
          "type": "select",
          "options": [
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            },
            {
              "value": "use_current_time",
              "label": "使用当前时间"
            }
          ]
        },
        {
          "field": "primary_key_null",
          "label": "复合主键空",
          "type": "select",
          "options": [
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            }
          ],
          "description": "表示复合主键列为空时的操作，可选：归档、丢弃、报错。默认：归档。",
          "defaultValue": "archive"
        },
        {
          "label": "表名长度溢出",
          "field": "table_name_length_overflow",
          "description": "表示当表名长度溢出时的操作，当前支持 归档、丢弃、截断、截断及归档、报错。默认：归档。",
          "defaultValue": "archive",
          "required": false,
          "type": "select",
          "options": [
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            },
            {
              "value": "truncate",
              "label": "截断"
            },
            {
              "value": "truncate_and_archive",
              "label": "截断且归档"
            }
          ]
        },
        {
          "label": "表名非法字符",
          "field": "table_name_contains_illegal_char",
          "description":
            "表示当表名包含非法字符时（如 . ）的处置策略，可选：替换为指定字符或字符串、丢弃、归档、报错。默认：替换为 _。",
          "defaultValue": "",
          "required": false,
          "unit_value": "replace_to",
          "disabledValues": ["archive", "skip", "break"],
          "type": "compose",
          "options": [
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            },
            {
              "value": "replace_to",
              "label": "非法字符替换为指定字符串"
            }
          ]
        },
        {
          "label": "表名模板变量空值",
          "field": "variable_not_exist_in_table_name_template",
          "description":
            "表示当表名模板中变量为空时的处置策略，可选：替换为指定字符串、留空、丢弃整行。 默认：替换为 NULL。",
          "defaultValue": "",
          "required": false,
          "unit_value": "replace_to",
          "disabledValues": ["leave_blank", "skip"],
          "type": "compose",
          "options": [
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "leave_blank",
              "label": "留空"
            },
            {
              "value": "replace_to",
              "label": "变量替换为指定字符串"
            }
          ]
        },
        {
          "field": "field_name_not_found",
          "label": "列名不存在",
          "type": "select",
          "options": [
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            },
            {
              "value": "add_field",
              "label": "自动增加缺失列"
            }
          ],
          "description": "表示列名不存在的操作，可选：使用当前时间、归档、丢弃、报错、自动增加缺失列。默认：归档。",
          "defaultValue": "add_field"
        },
        {
          "label": "列名长度溢出",
          "field": "field_name_length_overflow",
          "description": "表示列名长度溢出的操作，可选：使用当前时间、归档、丢弃、报错、截断、截断且归档。默认：归档。",
          "defaultValue": "archive",
          "required": false,
          "type": "select",
          "options": [
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            }
          ]
        },
        {
          "field": "field_length_extend",
          "label": "列自动扩容",
          "type": "switch",
          "defaultValue": true,
          "description": "启用时，VARCHAR/VARBINARY/NCHAR 列自动扩容到可入库的长度。默认为 true 。",
          "value": true
        },
        {
          "field": "field_length_overflow",
          "label": "列长度溢出",
          "type": "select",
          "options": [
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            },
            {
              "value": "truncate",
              "label": "截断"
            },
            {
              "value": "truncate_and_archive",
              "label": "截断且归档"
            }
          ],
          "description": "表示列长度溢出的操作，可选：归档、丢弃、报错、截断、截断且归档。默认：归档。",
          "defaultValue": "archive"
        },
        {
          "field": "ingesting_error",
          "label": "数据异常",
          "type": "select",
          "options": [
            {
              "value": "archive",
              "label": "归档"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错"
            }
          ],
          "description": "因数据本身无法入库导致失败时的数据行为，当前支持 归档 、丢弃、报错 三种。默认：归档。",
          "defaultValue": "archive"
        },
        {
          "field": "connection_timeout_in_second",
          "label": "连接超时",
          "type": "composeAppend",
          "options": [
            {
              "value": "s",
              "label": "秒"
            }
          ],
          "min": 1,
          "max": 600,
          "description": "目标数据库连接超时，默认为 30s。",
          "required": false,
          "placeholder": "输入范围为[1,600]整数",
          "defaultValue": "30s"
        },
        {
          "field": "cache.keep_days",
          "label": "临时存储保留天数",
          "type": "composeAppend",
          "options": [
            {
              "value": "d",
              "label": "天"
            }
          ],
          "min": 0,
          "max": 65535,
          "description": "配置以上操作配置为 缓存 时，缓存文件的最大保留时长。默认 30 天。配置为 0 表示默认值。",
          "required": false,
          "placeholder": "输入非负整数，0 表示默认值30d",
          "defaultValue": "30d"
        },
        {
          "field": "cache.max_size",
          "label": "临时存储文件大小",
          "type": "composeAppend",
          "options": [
            {
              "value": "MB",
              "label": "MB"
            },
            {
              "value": "GB",
              "label": "GB"
            }
          ],
          "min": 0,
          "max": 65535,
          "description":
            "单个缓存文件的大小，默认为 1G，最大为 65535G，配置为 0 表示使用默认值。默认路径是 ： $DATA_DIR/tasks/:id/cache",
          "required": false,
          "placeholder": "输入范围为[0,65535]整数",
          "defaultValue": "1GB"
        },
        {
          "field": "cache.rotate_count",
          "label": "临时存储文件个数",
          "type": "number",
          "min": 0,
          "max": 65535,
          "description": "临时存储文件的个数，默认值为 100。配置为 0 表示使用默认值。",
          "required": false,
          "defaultValue": 100
        },
        {
          "field": "cache.location",
          "label": "临时存储文件位置",
          "type": "input",
          "description": "表示临时存储文件位置，默认 $DATA_DIR/tasks/:id/cache ",
          "value": "cache",
          "placeholder": "$DATA_DIR/tasks/:id/cache"
        },
        {
          "field": "cache.on_fail",
          "label": "临时存储失败处理策略",
          "type": "select",
          "options": [
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错并停止任务"
            }
          ],
          "description": "表示临时存储失败处理策略的操作，可选有丢弃、报错并停止任务，默认：丢弃。",
          "defaultValue": "skip"
        },
        {
          "field": "archive.keep_days",
          "label": "归档数据保留天数",
          "type": "composeAppend",
          "options": [
            {
              "value": "d",
              "label": "天"
            }
          ],
          "min": 0,
          "max": 65535,
          "description": "配置以上操作配置为 归档 时，归档文件的最大保留时长。默认 30 天。配置为 0 表示使用默认值。",
          "required": false,
          "placeholder": "输入非负整数，0 表示默认值30d",
          "defaultValue": "30d"
        },
        {
          "field": "archive.max_size",
          "label": "归档数据文件大小",
          "type": "composeAppend",
          "options": [
            {
              "value": "MB",
              "label": "MB"
            },
            {
              "value": "GB",
              "label": "GB"
            }
          ],
          "min": 0,
          "max": 65535,
          "description":
            "单个归档文件的大小，默认为 1G，最大为 65535G，配置为 0 表示使用默认值。默认路径：$DATA_DIR/tasks/:id/archived",
          "required": false,
          "placeholder": "输入范围为[0,65535]整数",
          "defaultValue": "1GB"
        },
        {
          "field": "archive.rotate_count",
          "label": "归档数据文件个数",
          "type": "number",
          "min": 0,
          "max": 65535,
          "description": "归档文件的个数，默认值为 100。配置为 0 表示使用默认值。",
          "required": false,
          "defaultValue": 100
        },
        {
          "field": "archive.location",
          "label": "归档数据文件位置",
          "type": "input",
          "description": "表示归档数据文件位置，默认：$DATA_DIR/tasks/:id/archived",
          "value": "archived",
          "placeholder": "$DATA_DIR/tasks/:id/archived"
        },
        {
          "field": "archive.on_fail",
          "label": "归档数据失败处理策略",
          "type": "select",
          "options": [
            {
              "value": "rotate",
              "label": "删除旧文件"
            },
            {
              "value": "skip",
              "label": "丢弃"
            },
            {
              "value": "break",
              "label": "报错并停止任务"
            }
          ],
          "description": "删除旧文件、报错或丢弃。",
          "defaultValue": "rotate"
        }
      ]
    }`
};

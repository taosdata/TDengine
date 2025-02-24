export default {
  name: 'CSV',
  id: 'csv',
  type: 'path',
  description: '导入一个或多个 CSV 文件数据到 TDengine。\n',
  strict: true,
  config: [
    {
      label: 'Groups-before',
      field: 'groups_before',
      hide: true,
      children: []
    },
    {
      label: 'Groups-after',
      field: 'groups_after',
      hide: true,
      children: [
        {
          label: 'CSV 选项',
          field: '0d14aa37-292f-4d91-89a5-7f9f90bfe72a',
          description: 'CSV 读取选项',
          children: [
            {
              label: '包含表头',
              description: '如果包含表头，则第一行将被视为列信息。\n',
              field: 'has_header',
              placeholder: '',
              defaultValue: false,
              pattern: null,
              grid_two: false,
              type: 'switch'
            },
            {
              label: '忽略前 N 行',
              description: '忽略 CSV 文件的前 N 行。',
              field: 'skip',
              placeholder: '',
              defaultValue: 0,
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 0
              // "max": null
            },
            {
              label: '字段分隔符',
              description: 'CSV 字段之间的分隔符。',
              field: 'delimiter',
              placeholder: '',
              defaultValue: ',',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: ',',
                  value: ','
                },
                {
                  label: ';',
                  value: ';'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: '字段引用符',
              description: '当 CSV 字段中包含分隔符或换行符时，用于包围字段内容，以确保整个字段被正确识别。',
              field: 'quote',
              placeholder: '',
              defaultValue: '"',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: '"',
                  value: '"'
                },
                {
                  label: "'",
                  value: "'"
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: '注释前缀符',
              description: '当 CSV 文件中某行以此处指定的字符开头，则忽略该行。',
              field: 'comment',
              placeholder: '',
              defaultValue: '#',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: '#',
                  value: '#'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            }
          ],
          hide: false
        }
      ]
    },
    {
      label: '',
      field: 'csvData',
      type: 'csvData',
      children: [{ field: 'upload_csv_file' }, { field: 'monitor_file_directory', children: [] }],
      defaultValue: 'upload_csv_file',
      valueField: 'currentTab'
    },
    {
      label: '高级选项',
      field: 'advanced_options',
      description: '对数据源性能、日志等其他参数进行调整，可修改以下选项。\n',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
        {
          label: '最大读取并发数',
          field: 'read_concurrency',
          description: '数据源连接数或读取线程数限制，当默认参数不满足需要或需要调整资源使用量时修改此参数。\n',
          defaultValue: 0,
          required: false,
          hint: {
            type: 'integer',
            min: 0,
            max: 1000
          },
          type: 'number',
          min: 0,
          max: 1000
        },
        {
          label: '批次大小',
          field: 'batch_size',
          description: '单次发送的最大消息数或行数。\n',
          defaultValue: 1000,
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 100000
          },
          type: 'number',
          min: 1,
          max: 100000
        },
        {
          label: '写入并发数量',
          field: 'written_concurrent',
          description: '同时写入 TDengine 的并发任务数量。\n',
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 100
          },
          type: 'number',
          min: 1,
          max: 100
        },
        {
          label: '健康监测时段',
          field: 'health_check_window_in_second',
          description: '表示对最近多长时间的任务状态进行统计。通常为分钟级，此时段对健康状态各种模式统一生效。\n',
          defaultValue: '',
          placeholder: '输入范围为[0,60000]整数',
          required: false,
          hint: {
            type: 'duration',
            choices: [
              {
                value: 's',
                label: '秒'
              }
            ],
            min: 0,
            max: 60000
          },
          unit_value: 's',
          type: 'composeAppend',
          options: [
            {
              value: 's',
              label: '秒'
            }
          ],
          min: 0,
          max: 60000
        },
        {
          label: 'Busy 状态阈值',
          field: 'busy_threshold',
          description: '百分比，表示写入队列中入队元素数量与队列长度之比，默认 100%。\n',
          defaultValue: 100,
          required: false,
          hint: {
            type: 'duration',
            choices: [
              {
                label: '%',
                value: '%'
              }
            ],
            min: 0,
            max: 100
          },
          unit_value: '%',
          type: 'composeAppend',
          options: [
            {
              label: '%',
              value: '%'
            }
          ],
          min: 0,
          max: 100
        },
        {
          label: '写入队列长度',
          field: 'max_queue_length',
          description: '表示一个 IPC 连接对应的写入队列长度最大值。',
          defaultValue: '1000',
          required: false,
          hint: {
            type: 'integer',
            min: 0,
            max: 10000
          },
          type: 'number',
          min: 0,
          max: 10000
        },
        {
          label: '写入错误阈值',
          field: 'max_errors_in_window',
          description: '表示健康监测时段中允许写入错误的数量。超出阈值，则发送 Fatal 警告。',
          defaultValue: 10,
          required: false,
          hint: {
            type: 'integer',
            min: 0,
            max: 10000
          },
          type: 'number',
          min: 0,
          max: 10000
        }
      ]
    },
    {
      label: '异常处理策略',
      field: 'write_config',
      description: '对写入策略配置参数进行调整，可修改以下选项。\n',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
        {
          label: '主键时间戳溢出',
          field: 'primary_timestamp_overflow',
          description: '表示时间戳溢出时的操作，可选：归档、丢弃、报错。默认：归档。\n',
          defaultValue: 'archive',
          required: false,
          type: 'select',
          options: [
            {
              value: 'archive',
              label: '归档'
            },
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'break',
              label: '报错'
            }
          ]
        },
        {
          label: '主键时间戳空',
          field: 'primary_timestamp_null',
          description: '表示时间戳为空时的操作，可选：使用当前时间、归档、丢弃、报错。默认：归档。\n',
          defaultValue: 'archive',
          required: false,
          type: 'select',
          options: [
            {
              value: 'archive',
              label: '归档'
            },
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'break',
              label: '报错'
            },
            {
              value: 'use_current_time',
              label: '使用当前时间'
            }
          ]
        },
        {
          field: 'primary_key_null',
          label: '复合主键空',
          type: 'select',
          options: [
            {
              value: 'archive',
              label: '归档'
            },
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'break',
              label: '报错'
            }
          ],
          description: '表示复合主键列为空时的操作，可选：归档、丢弃、报错。默认：归档。\n',
          defaultValue: 'archive'
        },
        {
          label: '表名长度溢出',
          field: 'table_name_length_overflow',
          description: '表示当表名长度溢出时的操作，当前支持 归档、丢弃、截断、截断及归档、报错。默认：归档。\n',
          defaultValue: 'archive',
          required: false,
          type: 'select',
          options: [
            {
              value: 'archive',
              label: '归档'
            },
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'break',
              label: '报错'
            },
            {
              value: 'truncate',
              label: '截断'
            },
            {
              value: 'truncate_and_archive',
              label: '截断且归档'
            }
          ]
        },
        {
          label: '表名非法字符',
          field: 'table_name_contains_illegal_char',
          description:
            '表示当表名包含非法字符时（如 . ）的处置策略，可选：替换为指定字符或字符串、丢弃、归档、报错。默认：替换为 _。\n',
          defaultValue: '',
          required: false,
          unit_value: 'replace_to',
          disabledValues: ['archive', 'skip', 'break'],
          type: 'compose',
          options: [
            {
              value: 'archive',
              label: '归档'
            },
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'break',
              label: '报错'
            },
            {
              value: 'replace_to',
              label: '非法字符替换为指定字符串'
            }
          ]
        },
        {
          label: '表名模板变量空值',
          field: 'variable_not_exist_in_table_name_template',
          description:
            '表示当表名模板中变量为空时的处置策略，可选：替换为指定字符串、留空、丢弃整行。 默认：替换为 NULL。\n',
          defaultValue: '',
          required: false,
          unit_value: 'replace_to',
          disabledValues: ['leave_blank', 'skip'],
          type: 'compose',
          options: [
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'leave_blank',
              label: '留空'
            },
            {
              value: 'replace_to',
              label: '变量替换为指定字符串'
            }
          ]
        },
        {
          field: 'field_name_not_found',
          label: '列名不存在',
          type: 'select',
          options: [
            {
              value: 'archive',
              label: '归档'
            },
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'break',
              label: '报错'
            },
            {
              value: 'add_field',
              label: '自动增加缺失列'
            }
          ],
          description: '表示列名不存在的操作，可选：使用当前时间、归档、丢弃、报错、自动增加缺失列。默认：归档。\n\n',
          defaultValue: 'add_field'
        },
        {
          label: '列名长度溢出',
          field: 'field_name_length_overflow',
          description: '表示列名长度溢出的操作，可选：使用当前时间、归档、丢弃、报错、截断、截断且归档。默认：归档。\n',
          defaultValue: 'archive',
          required: false,
          type: 'select',
          options: [
            {
              value: 'archive',
              label: '归档'
            },
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'break',
              label: '报错'
            }
          ]
        },
        {
          field: 'field_length_extend',
          label: '列自动扩容',
          type: 'switch',
          defaultValue: true,
          description: '启用时，VARCHAR/VARBINARY/NCHAR 列自动扩容到可入库的长度。默认为 true 。\n',
          value: true
        },
        {
          field: 'field_length_overflow',
          label: '列长度溢出',
          type: 'select',
          options: [
            {
              value: 'archive',
              label: '归档'
            },
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'break',
              label: '报错'
            },
            {
              value: 'truncate',
              label: '截断'
            },
            {
              value: 'truncate_and_archive',
              label: '截断且归档'
            }
          ],
          description: '表示列长度溢出的操作，可选：归档、丢弃、报错、截断、截断且归档。默认：归档。\n',
          defaultValue: 'archive'
        },
        {
          field: 'ingesting_error',
          label: '数据异常',
          type: 'select',
          options: [
            {
              value: 'archive',
              label: '归档'
            },
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'break',
              label: '报错'
            }
          ],
          description: '因数据本身无法入库导致失败时的数据行为，当前支持 归档 、丢弃、报错 三种。默认：归档。\n',
          defaultValue: 'archive'
        },
        {
          field: 'connection_timeout_in_second',
          label: '连接超时',
          type: 'composeAppend',
          options: [
            {
              value: 's',
              label: '秒'
            }
          ],
          min: 1,
          max: 600,
          description: '目标数据库连接超时，默认为 30s。\n',
          required: false,
          placeholder: '输入范围为[1,600]整数',
          value: 30,
          unit_value: 's'
        },
        {
          field: 'cache.max_size',
          label: '临时存储可用空间',
          type: 'composeAppend',
          options: [
            {
              value: 'MB',
              label: 'MB'
            },
            {
              value: 'GB',
              label: 'GB'
            }
          ],
          min: 0,
          max: 65535,
          description:
            '启用时，需配置允许占用的磁盘空间，最小为 1G，最大为 65535 G，配置为 0 表示无限制。默认无限制。默认路径是 ： $DATA_DIR/tasks/:id/cache\n',
          required: false,
          placeholder: '输入范围为[1,65535]整数',
          value: 0,
          unit_value: 'GB'
        },
        {
          field: 'cache.location',
          label: '临时存储文件位置',
          type: 'input',
          description: '表示临时存储文件位置，默认 $DATA_DIR/tasks/:id/cache \n',
          value: 'cache',
          placeholder: '$DATA_DIR/tasks/:id/cache'
        },
        {
          field: 'cache.on_fail',
          label: '临时存储失败处理策略',
          type: 'select',
          options: [
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'break',
              label: '报错并停止任务'
            }
          ],
          description: '表示临时存储失败处理策略的操作，可选有丢弃、报错并停止任务，默认：丢弃。\n',
          defaultValue: 'skip'
        },
        {
          field: 'archive.keep_days',
          label: '归档数据保留天数',
          type: 'composeAppend',
          options: [
            {
              value: 'd',
              label: '天'
            }
          ],
          min: 0,
          max: 65535,
          description: '配置以上操作配置为 归档 时，归档文件的最大保留时长。默认 30 天。配置为 0 表示无限制。\n',
          required: false,
          placeholder: '输入非负整数，0 表示无限制',
          value: 30,
          unit_value: 'd'
        },
        {
          field: 'archive.max_size',
          label: '归档数据可用空间',
          type: 'composeAppend',
          options: [
            {
              value: 'MB',
              label: 'MB'
            },
            {
              value: 'GB',
              label: 'GB'
            }
          ],
          min: 0,
          max: 65535,
          description:
            '归档文件的最大可用磁盘空间，最小为 1G，最大为 65535G，配置为 0 表示无限制。默认无限制。默认路径：$DATA_DIR/tasks/:id/archived\n',
          required: false,
          placeholder: '输入范围为[1,65535]整数',
          value: 0,
          unit_value: 'GB'
        },
        {
          field: 'archive.location',
          label: '归档数据文件位置',
          type: 'input',
          description: '表示归档数据文件位置，默认：$DATA_DIR/tasks/:id/archived\n',
          value: 'archived',
          placeholder: '$DATA_DIR/tasks/:id/archived'
        },
        {
          field: 'archive.on_fail',
          label: '归档数据失败处理策略',
          type: 'select',
          options: [
            {
              value: 'rotate',
              label: '删除旧文件'
            },
            {
              value: 'skip',
              label: '丢弃'
            },
            {
              value: 'break',
              label: '报错并停止任务'
            }
          ],
          description: '删除旧文件、报错或丢弃。\n',
          defaultValue: 'rotate'
        }
      ]
    }
  ]
};

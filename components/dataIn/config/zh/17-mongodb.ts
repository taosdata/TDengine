export default {
  name: 'MongoDB',
  id: 'mongodb',
  type: 'uri',
  description:
    'MongoDB 是一个介于关系型数据库与非关系型数据库之间的产品，被广泛应用于内容管理系统、移动应用与物联网等众多领域。\n\nTDengine 可以高效地从 MongoDB 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: '服务地址',
          description: 'MongoDB 的服务器地址',
          field: 'host',
          required: true,
          placeholder: '127.0.0.1',
          pattern: null,
          defaultValue: '',
          display_order: 1,
          type: 'input'
        },
        {
          label: '服务端口',
          description: 'MongoDB 的端口',
          field: 'port',
          required: true,
          placeholder: '27017',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: '端口号的范围是 0-65535',
          defaultValue: '',
          type: 'input'
        }
      ]
    },
    {
      label: '认证',
      description: '使用用户名和密码访问 MongoDB 数据库',
      field: 'authentication',
      type: 'tabs',
      valueField: 'isEnable',
      defaultValue: 'plain',
      multiple: false,
      children: [
        {
          label: '用户名密码访问',
          name: 'plain',
          field: 'plain',
          children: [
            {
              label: '用户',
              placeholder: '请输入用户名',
              required: true,
              field: 'username',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'input'
            },
            {
              label: '密码',
              placeholder: '请输入密码',
              required: true,
              field: 'password',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'password'
            },
            {
              label: '认证数据库',
              description: 'MongoDB 中存储用户信息的数据库，默认为 admin。\n',
              placeholder: '认证数据库',
              required: false,
              field: 'source',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'input'
            }
          ]
        }
      ]
    },
    {
      label: 'Groups-before',
      field: 'groups_before',
      hide: true,
      children: [
        {
          label: '连接选项',
          field: 'eac6ca16-1b3d-4155-b71c-dc9fd7b57d1f',
          description: '其他数据库连接选项。',
          children: [
            {
              label: '应用名称',
              description: '用于标识客户端。',
              field: 'app_name',
              placeholder: '示例: TDengine',
              pattern: null,
              grid_two: false,
              type: 'input'
            }
          ],
          hide: false
        },
        {
          label: 'SSL 证书',
          field: 'ssl',
          description: '使用证书和私钥建立连接以启用 SSL。',
          hide: false,
          type: 'switch',
          defaultValue: false,
          valueField: 'isEnable',
          hasValue: true,
          children: [
            {
              label: 'CA 文件',
              description: 'CA 证书文件',
              field: 'ca_file_path',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'file',
              templateUrl: '',
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/ssl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
              }
            },
            {
              label: '证书文件',
              description: '.cert 文件',
              field: 'cert_key_file_path',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'file',
              templateUrl: '',
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/ssl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
              }
            }
          ]
        }
      ]
    },
    {
      field: 'checkConnectivity',
      type: 'checkConnectivity',
      children: []
    },
    {
      label: 'Groups-after',
      field: 'groups_after',
      hide: true,
      children: [
        {
          label: '数据查询',
          field: '82736fff-424f-40d8-8f48-03c4c9197cfc',
          description: '数据采集相关配置项。',
          children: [
            {
              label: '数据库',
              description:
                'MongoDB 中源数据库，可以使用占位符进行动态配置，可用占位符列表：\n<ul><li>${Y} 完整的公历年表示，零填充的 4 位整数</li><li>${y} 公历年除以 100，零填充的 2 位整数</li><li>${M} 整数月份（1 - 12）</li><li>${m} 整数月份（01 - 12）</li><li>${B} 月份英文全拼</li><li>${b} 月份英文的缩写（3 个字母）</li><li>${D} 日期的数字表示（1 - 31）</li><li>${d} 日期的数字表示（01 - 31）</li><li>${J} 一年中的第几天（1 - 366）</li><li>${j} 一年中的第几天（001 - 366）</li><li>${F} 相当于 ${Y}-${m}-${d}</li></ul>\n',
              field: 'database',
              required: true,
              placeholder: 'database_${Y}',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: '集合',
              description:
                'MongoDB 中集合，可以使用占位符进行动态配置，可用占位符列表：\n<ul><li>${Y} 完整的公历年表示，零填充的 4 位整数</li><li>${y} 公历年除以 100，零填充的 2 位整数</li><li>${M} 整数月份（1 - 12）</li><li>${m} 整数月份（01 - 12）</li><li>${B} 月份英文全拼</li><li>${b} 月份英文的缩写（3 个字母）</li><li>${D} 日期的数字表示（1 - 31）</li><li>${d} 日期的数字表示（01 - 31）</li><li>${J} 一年中的第几天（1 - 366）</li><li>${j} 一年中的第几天（001 - 366）</li><li>${F} 相当于 ${Y}-${m}-${d}</li></ul>',
              field: 'collection',
              required: true,
              placeholder: 'collection_${md}',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: '子表字段',
              description: '用于拆分子表的字段。',
              field: 'subtable_fields',
              placeholder: 'col_name1,col_name2,...',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: '查询模板',
              description:
                '用于查询数据的查询语句，JSON格式，语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现（至少一个闭区间）。\n使用不同的占位符表示不同的时间格式要求，具体有以下占位符格式：\n1. `${start_datetime}`、`${end_datetime}`：对应后端 datetime 类型字段的筛选，如：{"ddate":{"$gte":${start_datetime},"$lt":${end_datetime}}} 将被转换为 {"ddate":{"$gte":{"$date":"2024-06-01T00:00:00+00:00"},"$lt":{"$date":"2024-07-01T00:00:00+00:00"}}}\n2. `${start_timestamp}`、`${end_timestamp}`：对应后端 timestamp 类型字段的筛选，如：{"ttime":{"$gte":${start_timestamp},"$lt":${end_timestamp}}} 将被转换为 {"ttime":{"$gte":{"$timestamp":{"t":123,"i":456}},"$lt":{"$timestamp":{"t":123,"i":456}}}}\n\n如果使用子表字段，需要在语句中拼接字段占位符。\n\n示例：`{"ddate":{"$gte":${start_datetime},"$lt":${end_datetime}},${col_name1},${col_name2}}`',
              field: 'sql',
              required: true,
              placeholder: '{"ddate":{"$gte":${start_datetime},"$lt":${end_datetime}},${col_name1},${col_name2}}',
              pattern: null,
              grid_two: true,
              type: 'input'
            },
            {
              label: '查询排序',
              description:
                '执行查询时的排序条件。\n\n1.`{"createtime":1}`：MongoDB 查询结果按 `createtime` 正序返回。\n\n2.`{"createdate":1, "createtime":1}`：MongoDB 查询结果按 `createdate` 正序、`createtime` 正序返回。',
              field: 'sort',
              placeholder: '{"createtime":1}',
              pattern: null,
              grid_two: false,
              validator: 'checkJson',
              type: 'input'
            },
            {
              label: '起始时间',
              description: '迁移数据的起始时间。\n',
              field: 'start',
              required: true,
              placeholder: '如：2023-01-01 00:00:00',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: '结束时间',
              description:
                '迁移数据的结束时间，可留空。如果设置，则迁移任务执行到结束时间后，任务完成自动停止；如果留空，则持续同步实时数据，任务不会自动停止。\n',
              field: 'end',
              placeholder: '如：2024-01-01 00:00:00',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: '查询间隔',
              description:
                '分段查询数据的时间间隔，默认1天。为了避免查询数据量过大，一次数据同步子任务会使用查询间隔分时间段查询数据。\n',
              field: 'interval',
              placeholder: '输入范围为[0,600]整数',
              pattern: null,
              patternMsg: '只能输入正整数或者0',
              grid_two: false,
              unit_value: 'd',
              type: 'composeAppend',
              options: [
                {
                  value: 'd',
                  label: '天'
                },
                {
                  value: 'h',
                  label: '小时'
                }
              ],
              min: 0,
              max: 600
            },
            {
              label: '延迟时长',
              description: '实时同步数据场景中，为了避免延迟写入的数据丢失，每次同步任务会读取延迟时长之前的数据。\n',
              field: 'delay',
              placeholder: '输入范围为[0,60000]整数',
              pattern: null,
              patternMsg: '只能输入正整数或者0',
              grid_two: false,
              unit_value: 's',
              type: 'composeAppend',
              options: [
                {
                  value: 'm',
                  label: '分钟'
                },
                {
                  value: 's',
                  label: '秒'
                }
              ],
              min: 0,
              max: 60000
            }
          ],
          hide: false
        }
      ]
    },
    {
      label: 'Payload 转换',
      description: '',
      field: 'parser',
      type: 'parser',
      fields: [
        {
          name: 'value',
          description: '消息体。',
          type: 'varchar'
        }
      ],
      defaultValue: {
        parse: {
          payload: {
            json: [],
            keep: true
          }
        },
        model: {
          name: '',
          using: '',
          columns: ['ts'],
          tags: []
        }
      },
      children: []
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
          defaultValue: 10000,
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
          defaultValue: 1000,
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
  ],
  parser: {
    display: 'Payload 转换',
    required: true,
    description:
      'Kafka 连接器会上传以下六列到服务端：<br>\n\n- **ts**: 采集时间戳。<br>\n- **topic**: 订阅主题名。<br>\n- **partition**: 当前消息所在的分区 ID。<br>\n- **offset**: 当前消息的偏移量。<br>\n- **key**: 当前消息的 Key。<br>\n- **value**: 当前消息的数据内容。<br>\n\ntaosX 可以使用 JSON 提取器解析数据，并允许用户在数据库中指定数据模型，<br>\n包括，指定表名称和超级表名，设置普通列和标签列等。\n',
    fields: [
      {
        name: 'value',
        description: '消息体。',
        type: 'varchar'
      }
    ]
  }
};

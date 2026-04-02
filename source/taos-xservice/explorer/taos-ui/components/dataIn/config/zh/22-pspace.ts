export default {
  name: 'pSpace',
  id: 'pspace',
  type: 'uri',
  description:
    'pSpace 是一个时序数据库。 TDengine TSDB 封装了 pSpace 的 SDK，支持历史数据迁移、实时数据同步、持续查询同步。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: 'Server 地址',
          description: 'pSpace Server 的 IP 地址或域名',
          field: 'host',
          required: true,
          placeholder: '127.0.0.1',
          pattern: null,
          defaultValue: '',
          display_order: 1,
          type: 'input'
        },
        {
          label: 'Server 端口',
          description: 'pSpace Server 的端口',
          field: 'port',
          placeholder: '8889',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: '端口号的范围是 0-65535',
          defaultValue: '',
          type: 'input'
        },
        {
          label: '连接超时',
          field: 'connect_timeout',
          description: '连接超时，单位是秒。默认 30 秒。最小值为 1 秒，最大值为 300 秒。\n',
          required: false,
          defaultValue: '30s',
          hint: {
            type: 'duration',
            choices: [
              {
                value: 's',
                label: '秒'
              }
            ],
            min: 1,
            max: 300
          },
          type: 'composeAppend',
          options: [
            {
              value: 's',
              label: '秒'
            }
          ],
          min: 1,
          max: 300
        }
      ]
    },
    {
      label: '认证',
      field: 'authentication',
      children: [
        {
          label: '用户',
          description: '访问 pSpace Server 的用户名',
          required: true,
          field: 'username',
          defaultValue: '',
          type: 'input'
        },
        {
          label: '密码',
          description: '访问 pSpace Server 的密码',
          required: true,
          field: 'password',
          defaultValue: '',
          type: 'password'
        }
      ]
    },
    {
      label: 'Groups-before',
      field: 'groups_before',
      hide: true,
      children: []
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
      children: []
    },
    {
      label: 'pSpace 数据点配置',
      description: 'pSpace 数据点和 TSDB 表的映射规则。',
      field: 'tag_datasets',
      name: 'datasets',
      type: 'tabs',
      multiple: false,
      valueField: 'currentTab',
      defaultValue: 'select_all_points',
      children: [
        {
          label: '选择数据点位',
          name: 'select_all_points',
          labelShow: false,
          labelWidth: '0px',
          category: 'select_all_points',
          field: 'select_all_points',
          type: 'dataset',
          placeholder: '设置过滤条件，选择 pSpace Server 满足指定条件的数据点位。\n',
          required: true,
          multiple: true,
          editable: true,
          selectable: true,
          children: [
            {
              name: 'root',
              display: '根节点',
              description:
                '从该节点开始遍历所有数据点, 例如：`\\北京\\朝阳`，表示从 `\\北京\\朝阳` 节点开始，向下遍历。默认从根节点开始遍历。\n',
              placeholder: '根节点',
              label: '根节点',
              field: 'root',
              defaultValue: '',
              multiple: false,
              type: 'lazyTreeSelect',
              clearable: true,
              rootLabel: '根节点'
            },
            {
              name: 'point_name_pattern',
              display: '数据点名称',
              description:
                '支持根据数据点的 LongName 过滤。例如：`\\北京\\朝阳\\*气温*`，表示查询 `\\北京\\朝阳`下，所有名称包含`气温`的数据点。\n',
              placeholder: '例如：\\北京\\朝阳\\*气温*',
              label: '数据点名称',
              field: 'point_name_pattern',
              defaultValue: '',
              multiple: false,
              type: 'pattern',
              viewText: '查看数据点列表'
            },
            {
              name: 'super_table_expression',
              display: '超级表名称',
              description:
                '支持 `<super table prefix>_{type}` 格式，`{type}` 表示数据点的数据类型。例如：数据点的数据类型为 `int`，则 `pspace_{type}` 表示使用 `pspace_int` 作为超级表名。\n',
              required: true,
              value: 'pspace_{type}',
              label: '超级表名称',
              field: 'super_table_expression',
              defaultValue: 'pspace_{type}',
              multiple: false,
              type: 'input'
            },
            {
              name: 'child_table_expression',
              display: '表名称',
              description:
                '支持 `<child table prefix>_{point_id}` 格式，`{point_id}` 为数据点ID。例如：数据点ID为 `150017`，则 `t_{point_id}` 表示使用 `t_150017` 作为表名。\n',
              required: true,
              value: 't_{point_id}',
              label: '表名称',
              field: 'child_table_expression',
              defaultValue: 't_{point_id}',
              multiple: false,
              type: 'input'
            },
            {
              name: 'table_primary_key',
              display: '时间戳列',
              description:
                '在目标数据表中作为时间戳列使用。`original_ts` 表示使用数据点的原始时间戳；`request_ts` 表示查询请求的发起时间；`received_ts` 表示查询请求接收到数据的时间。\n',
              required: false,
              value: 'original_ts',
              label: '时间戳列',
              field: 'table_primary_key',
              defaultValue: 'original_ts',
              multiple: false,
              type: 'select',
              options: [
                {
                  label: 'original_ts',
                  value: 'original_ts'
                },
                {
                  label: 'request_ts',
                  value: 'request_ts'
                },
                {
                  label: 'received_ts',
                  value: 'received_ts'
                }
              ]
            },
            {
              name: 'table_primary_key_alias',
              display: '时间戳列名称',
              description: '在目标数据表中，时间戳列的名称。\n',
              required: false,
              value: 'ts',
              label: '时间戳列名称',
              field: 'table_primary_key_alias',
              defaultValue: 'ts',
              multiple: false,
              type: 'input'
            },
            {
              name: 'value_col',
              display: '值列名称',
              description: '指定目标 TSDB 表中值列的名称。例如 `value_col=val` 表示将值列名称设置为 `val`。\n',
              required: false,
              label: '值列名称',
              field: 'value_col',
              defaultValue: 'val',
              multiple: false,
              type: 'input'
            },
            {
              name: 'value_transform',
              display: '值变换',
              description:
                '对写入 TSDB 的 `value` 进行变换表达式。例如：`value_transform=(val-32)/1.8` 表示按表达式计算。\n',
              required: false,
              label: '值变换',
              field: 'value_transform',
              defaultValue: '',
              multiple: false,
              type: 'input'
            },
            {
              name: 'quality_col',
              display: '数据质量列名称',
              description:
                '指定目标 TSDB 表中数据质量列的名称。例如 `quality_col=quality` 表示将数据质量列名称设置为 `quality`。\n',
              required: false,
              label: '数据质量列名称',
              field: 'quality_col',
              defaultValue: 'quality',
              multiple: false,
              type: 'input'
            },
            {
              name: 'custom_tags',
              display: '自定义标签',
              description:
                '可以配置多个自定义标签，使用逗号分隔。支持静态值和 pSpace 数据点属性提取的动态值。例如：`{LongName}` 替换为该点位的实际 LongName 属性。\n',
              required: false,
              label: '自定义标签',
              field: 'custom_tags',
              defaultValue:
                'VARCHAR(1024)::name::{Name};VARCHAR(1024)::LongName::{LongName};VARCHAR(1024)::Description::{Description}',
              multiple: false,
              type: 'input'
            }
          ],
          defaultValue: ''
        },
        {
          label: '上传 CSV 配置文件',
          name: 'csv_config_file',
          field: 'csv_config_file',
          labelShow: false,
          labelWidth: '0px',
          description:
            "使用 csv 文件定义每个数据点到表的映射规则：\n\n(1) point_id：必填，数据点在 pSpace 上的 ID；\n\n(2) stable：必填，映射的 TSDB 超级表；\n\n(3) tbname：必填，映射的 TSDB 子表；\n\n(4) enable：可选，默认值 '1'，指定是否采集该数据点数据。0-不采集并且删除对应子表，1-采集数据点数据，没有子表时创建子表；\n\n(5) value_col：可选，默认值 'val'。采集值在 TSDB 中对应的列名；\n\n(6) value_transform：可选，采集值执行的变换函数，目前仅支持数值计算表达式，详见 transform 文档的 expr 表达式说明；\n\n(7) type：可选，默认值取源数据类型。采集值的数据类型，可用于替换超级表名称中的占位符 {type}；\n\n(8) quality_col：可选，采集值质量在 TSDB 中对应的列名；\n\n(9) ts_col/request_ts_col/received_ts_col：必填，TSDB 时间戳主键定义；可只保留其中一列作为主键，也可填写多列，居前的时间戳列作为主键；\n\n(10) ts_transform/request_ts_transform/received_ts_transform：可选，时间戳变换表达式，支持`+ - * /`操作和括号；\n\n(11) tag::VARCHAR(200)::name：可选/可配置多个 tag 列；表示在 TSDB 中对应的 Tag 列；其中 tag 为保留关键字，VARCHAR(200) 表示该 tag 的类型；name 是该 tag 的列名。\n",
          category: 'csv_config_file',
          radio: false,
          type: 'dataset',
          accept: '.csv',
          templateUrl: 'template-zh.csv',
          required: true,
          requiredDependsOn: ['tag_datasets/currentTab'],
          requiredDependsOnValues: {
            currentTab: ['csv_config_file']
          },
          multiple: true,
          editable: true,
          selectable: true,
          defaultValue: '',
          info2: true
        }
      ]
    },
    {
      label: '采集配置',
      field: 'collect_options',
      description: '数据采集相关配置项。',
      children: [
        {
          label: '任务模式',
          name: 'pspace_task_mode',
          field: 'pspace_task_mode',
          description: '选择数据采集的任务模式。',
          required: true,
          hide: false,
          placeholder: 'query',
          defaultValue: 'query',
          pattern: null,
          grid_two: false,
          type: 'select',
          options: [
            {
              label: '历史查询',
              value: 'query'
            },
            {
              label: '实时订阅',
              value: 'subscribe'
            },
            {
              label: '持续查询',
              value: 'query_sync'
            }
          ]
        },
        {
          label: '开始时间',
          field: 'start_time',
          description: '查询的开始时间。\n',
          required: true,
          display_order: 1,
          type: 'time',
          valueFormat: 'yyyy-MM-dd HH:mm:ss',
          dateType: 'datetime',
          requiredDependsOn: ['collect_options/pspace_task_mode'],
          requiredDependsOnValues: {
            pspace_task_mode: ['query', 'query_sync']
          },
          displayDependsOn: ['collect_options/pspace_task_mode'],
          displayDependsOnValues: {
            pspace_task_mode: ['query', 'query_sync']
          }
        },
        {
          label: '结束时间',
          field: 'end_time',
          description: '查询的结束时间，默认当前时间。',
          required: false,
          display_order: 2,
          type: 'time',
          valueFormat: 'yyyy-MM-dd HH:mm:ss',
          dateType: 'datetime',
          displayDependsOn: ['collect_options/pspace_task_mode'],
          displayDependsOnValues: {
            pspace_task_mode: ['query']
          }
        },
        {
          label: '查询窗口',
          field: 'time_window',
          description: '每次查询的时间窗口，默认 1 天。',
          required: false,
          display_order: 3,
          defaultValue: '1d',
          type: 'composeAppend',
          options: [
            {
              value: 'd',
              label: '天'
            },
            {
              value: 'h',
              label: '小时'
            },
            {
              value: 'm',
              label: '分钟'
            },
            {
              value: 's',
              label: '秒'
            }
          ],
          displayDependsOn: ['collect_options/pspace_task_mode'],
          displayDependsOnValues: {
            pspace_task_mode: ['query', 'query_sync']
          }
        },
        {
          label: '乱序',
          field: 'time_excursion',
          description: 'QuerySync 模式持续同步阶段容忍乱序的时长，默认 0 秒。',
          defaultValue: '0s',
          required: false,
          type: 'composeAppend',
          options: [
            {
              value: 'd',
              label: '天'
            },
            {
              value: 'h',
              label: '小时'
            },
            {
              value: 'm',
              label: '分钟'
            },
            {
              value: 's',
              label: '秒'
            }
          ],
          displayDependsOn: ['collect_options/pspace_task_mode'],
          displayDependsOnValues: {
            pspace_task_mode: ['query_sync']
          }
        },
        {
          label: '查询间隔',
          field: 'query_interval',
          description: '持续查询模式下，查询的间隔时间。',
          defaultValue: '10s',
          required: false,
          type: 'composeAppend',
          options: [
            {
              value: 'd',
              label: '天'
            },
            {
              value: 'h',
              label: '小时'
            },
            {
              value: 'm',
              label: '分钟'
            },
            {
              value: 's',
              label: '秒'
            }
          ],
          displayDependsOn: ['collect_options/pspace_task_mode'],
          displayDependsOnValues: {
            pspace_task_mode: ['query_sync']
          }
        }
      ]
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
          label: '日志级别',
          field: 'log_level',
          description: '根据需要调整数据源的日志级别，此参数不总是生效。',
          defaultValue: 'info',
          required: false,
          hint: {
            type: 'str',
            choices: ['error', 'warn', 'info', 'debug', 'trace']
          },
          type: 'select',
          options: [
            {
              label: 'error',
              value: 'error'
            },
            {
              label: 'warn',
              value: 'warn'
            },
            {
              label: 'info',
              value: 'info'
            },
            {
              label: 'debug',
              value: 'debug'
            },
            {
              label: 'trace',
              value: 'trace'
            }
          ]
        },
        {
          label: '保存原始数据',
          field: 'keep_raw_data',
          description: '是否保存原始数据？\n',
          defaultValue: false,
          required: false,
          hint: {
            type: 'bool'
          },
          type: 'switch'
        },
        {
          label: '最大保留天数',
          field: 'keep_raw_data_days',
          description: '原始数据最大保存天数，默认 1 天。\n',
          defaultValue: 1,
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 365
          },
          type: 'number',
          min: 1,
          max: 365,
          displayDependsOn: ['advanced_options/keep_raw_data'],
          displayDependsOnValues: {
            keep_raw_data: [true]
          }
        },
        {
          label: '原始数据存储目录',
          field: 'keep_raw_data_dir',
          description: '自定义原始数据存储目录，默认存储到系统数据目录下。\n',
          placeholder: '$DATA_DIR/tasks/:id/rawdata/',
          required: false,
          hint: {
            type: 'str'
          },
          type: 'input',
          displayDependsOn: ['advanced_options/keep_raw_data'],
          displayDependsOnValues: {
            keep_raw_data: [true]
          }
        },
        {
          label: '并发数',
          field: 'concurrency',
          description: '最大并发数限制，当默认参数性能不足时，可增大此参数。\n',
          defaultValue: '0',
          required: false,
          hint: {
            type: 'integer',
            min: 0,
            max: 128
          },
          type: 'number',
          min: 0,
          max: 128
        },
        {
          label: '批次大小',
          field: 'batch_size',
          description: '单次发送的最大消息数或行数。\n',
          defaultValue: '1000',
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 10000
          },
          type: 'number',
          min: 1,
          max: 10000
        },
        {
          label: '批次延时',
          field: 'batch_timeout',
          description: '发送批次数据前的最大等待时间（单位：秒）。默认值为 1s。当数据源响应较慢时，可适当增大此值。\n',
          defaultValue: 1,
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 60
          },
          type: 'number',
          min: 1,
          max: 60
        },
        {
          label: '健康监测时段',
          field: 'health_check_window_in_second',
          description: '表示对最近多长时间的任务状态进行统计。通常为分钟级，此时段对健康状态各种模式统一生效。\n',
          defaultValue: '0s',
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
          defaultValue: '100%',
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
          defaultValue: '10',
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
    }
  ]
};

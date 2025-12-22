import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
  name: 'AVEVA Historian',
  id: 'avevaHistorian',
  type: 'uri',
  description:
    'AVEVA Historian 是一款工业大数据分析软件，前身为 Wonderware。可以捕获并存储高保真工业大数据，释放受制约的潜力，从而改善运营。\nTDengine 可以高效地从 AVEVA Historian 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: 'Server 地址',
          description: 'AVEVA Historian SQL Server 的 IP 地址或域名',
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
          description: 'AVEVA Historian SQL Server 的端口',
          field: 'port',
          placeholder: '1433',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: '端口号的范围是 0-65535',
          defaultValue: '',
          type: 'input'
        }
      ]
    },
    {
      label: '认证',
      description: '使用用户名和密码访问 AVEVA Historian SQL Server',
      field: 'authentication',
      type: 'tabs',
      valueField: 'dea7d812-3c76-40a5-bb8a-1048945f79cb',
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
              required: true,
              field: 'username',
              defaultValue: '',
              type: 'input'
            },
            {
              label: '密码',
              required: true,
              field: 'password',
              defaultValue: '',
              type: 'password'
            },
            {
              label: '加密级别',
              description: '设置连接的加密级别',
              field: 'encryption',
              defaultValue: 'Off',
              type: 'select',
              options: [
                {
                  label: 'Off',
                  value: 'Off'
                },
                {
                  label: 'On',
                  value: 'On'
                },
                {
                  label: 'NotSupported',
                  value: 'NotSupported'
                },
                {
                  label: 'Required',
                  value: 'Required'
                }
              ]
            }
          ]
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
      children: [
        {
          label: '采集配置',
          field: 'collect_options',
          description: '数据采集相关配置项。',
          children: [
            {
              label: '采集模式',
              description: '采集模式，可选值为 `synchronize` 和 `migrate`。\n',
              field: 'mode',
              placeholder: 'synchronize',
              defaultValue: 'synchronize',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'synchronize',
                  value: 'synchronize'
                },
                {
                  label: 'migrate',
                  value: 'migrate'
                }
              ]
            },
            {
              label: '表',
              description:
                '检索 historian 中的数据库表，历史数据在 Runtime.dbo.History 中，实时数据在 Runtime.dbo.Live 中。\n',
              field: 'table',
              required: true,
              placeholder: 'Runtime.dbo.History',
              defaultValue: 'Runtime.dbo.History',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'Runtime.dbo.History',
                  value: 'Runtime.dbo.History'
                },
                {
                  label: 'Runtime.dbo.Live',
                  value: 'Runtime.dbo.Live'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              },
              displayDependsOn: ['groups_after/collect_options/mode'],
              displayDependsOnValues: {
                mode: ['synchronize']
              }
            },
            {
              label: '标签',
              description: '需要迁移/同步的tag，`*`代表除了Sys开头以外的全部tag。\n',
              field: 'tags',
              placeholder: '*',
              defaultValue: '*',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: '标签组大小',
              description:
                '当 `table` 为 `Runtime.dbo.History` 且 `tags` 中的 TagName 超过 `tagListSize` 时，tags 被按照每组 tagListSize 个进行划分。 使用 `tagListSize` 划分 TagName 是为了提高数据迁移/同步时的查询效率。`tagListSize` 默认值为 10。\n',
              field: 'tagListSize',
              placeholder: '10',
              defaultValue: '10',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1,
              max: 1000,
              displayConditions: 'some',
              displayDependsOn: ['groups_after/collect_options/table'],
              displayDependsOnValues: {
                table: ['Runtime.dbo.History', '']
              }
            },
            {
              label: '任务开始时间',
              description: '任务的开始时间，rfc3339格式的日期时间。\n',
              field: 'beginDateTime',
              placeholder: '如：2023-01-01T00:00:00+08:00',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime',
              requiredConditions: 'some',
              requiredDependsOn: ['groups_after/collect_options/mode', 'groups_after/collect_options/table'],
              requiredDependsOnValues: {
                mode: ['migrate'],
                table: ['Runtime.dbo.History']
              },
              displayConditions: 'some',
              displayDependsOn: ['groups_after/collect_options/table'],
              displayDependsOnValues: {
                table: ['Runtime.dbo.History', '']
              }
            },
            {
              label: '任务结束时间',
              description: '任务的结束时间，rfc3339格式的日期时间。\n',
              field: 'endDateTime',
              placeholder: '如：2023-01-01T00:00:00+08:00',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime',
              displayDependsOn: ['groups_after/collect_options/mode'],
              displayDependsOnValues: {
                mode: ['migrate']
              },
              requiredDependsOn: ['groups_after/collect_options/mode'],
              requiredDependsOnValues: {
                mode: ['migrate']
              }
            },
            {
              label: '查询的时间窗口',
              description: '历史数据迁移时，每次查询的时间窗口。\n',
              field: 'timeWindow',
              placeholder: '输入范围为[0,60000]整数',
              pattern: null,
              patternMsg: '只能输入正整数或者0',
              grid_two: false,
              defaultValue: '1d',
              type: 'composeAppend',
              options: [
                {
                  value: 'y',
                  label: '年'
                },
                {
                  value: 'mo',
                  label: '月'
                },
                {
                  value: 'd',
                  label: '天'
                },
                {
                  value: 'w',
                  label: '周'
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
                },
                {
                  value: 'ms',
                  label: '毫秒'
                },
                {
                  value: 'u',
                  label: '微秒'
                },
                {
                  value: 'ns',
                  label: '纳秒'
                }
              ],
              min: 0,
              max: 60000,
              displayConditions: 'some',
              displayDependsOn: ['groups_after/collect_options/table'],
              displayDependsOnValues: {
                table: ['Runtime.dbo.History', '']
              }
            },
            {
              label: '实时同步的时间间隔',
              description: '实时数据同步时，每次查询的时间间隔。\n',
              field: 'retrieveInterval',
              placeholder: '输入范围为[0,60000]整数',
              pattern: null,
              patternMsg: '只能输入正整数或者0',
              grid_two: false,
              defaultValue: '10s',
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
                },
                {
                  value: 'ms',
                  label: '毫秒'
                }
              ],
              min: 0,
              max: 60000,
              displayDependsOn: ['groups_after/collect_options/mode'],
              displayDependsOnValues: {
                mode: ['synchronize']
              }
            },
            {
              label: '乱序时间上限',
              description: '容忍乱序数据延迟到达的时间上限。\n',
              field: 'tolerance',
              placeholder: '输入范围为[0,60000]整数',
              pattern: null,
              patternMsg: '只能输入正整数或者0',
              grid_two: false,
              defaultValue: '0ms',
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
                },
                {
                  value: 'ms',
                  label: '毫秒'
                }
              ],
              min: 0,
              max: 60000,
              displayDependsOn: ['groups_after/collect_options/mode', 'groups_after/collect_options/table'],
              displayDependsOnValues: {
                mode: ['synchronize'],
                table: ['Runtime.dbo.History', '']
              }
            }
          ],
          hide: false
        }
      ]
    },
    {
      label: 'Payload 转换',
      description: 'taosX 允许用户在数据库中指定数据模型，包括：指定表名称和超级表名，设置普通列和标签列等\n',
      field: 'parser',
      type: 'parser',
      fields: [
        {
          name: 'DateTime',
          description: '值对应的时间戳。',
          type: 'timestamp'
        },
        {
          name: 'TagName',
          description: '测点名称。',
          type: 'varchar'
        },
        {
          name: 'Value',
          description: '标记在时间戳处的值。对于字符串tag，该值始终为NULL。',
          type: 'double'
        },
        {
          name: 'vValue',
          description: '字符串形式的值，在查询中使用此列允许您使用混合数据类型的值。',
          type: 'varchar'
        },
        {
          name: 'Quality',
          description: '与数据值相关联的基本数据质量指标。',
          type: 'int'
        },
        {
          name: 'QualityDetail',
          description: '数据质量的内部表示。',
          type: 'int'
        },
        {
          name: 'OPCQuality',
          description: '从数据源接收到的质量值。',
          type: 'int'
        },
        {
          name: 'wwTagKey',
          description: '单个AVEVA历史记录中tag的唯一数字标识符。',
          type: 'int'
        },
        {
          name: 'wwResolution',
          description: '在循环模式下检索数据的采样率，以毫秒为单位。',
          type: 'int'
        },
        {
          name: 'StartDateTime',
          description: '返回该行的检索周期的开始时间。',
          type: 'timestamp'
        },
        {
          name: 'SourceTag',
          description: '在存储该点时复制标记的源标记的名称。',
          type: 'varchar'
        },
        {
          name: 'SourceServer',
          description: '在存储该点时复制标记的服务器的名称。',
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
          defaultValue: '0',
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
          defaultValue: '10000',
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
          defaultValue: '1',
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 365
          },
          type: 'number',
          min: 1,
          max: 365
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
          type: 'input'
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
    exceptionStrategy
  ],
  parser: {
    display: 'Payload 转换',
    required: true,
    editableSample: true,
    description: 'taosX 允许用户在数据库中指定数据模型，包括：指定表名称和超级表名，设置普通列和标签列等\n',
    fields: [
      {
        name: 'DateTime',
        description: '值对应的时间戳。',
        type: 'timestamp'
      },
      {
        name: 'TagName',
        description: '测点名称。',
        type: 'varchar'
      },
      {
        name: 'Value',
        description: '标记在时间戳处的值。对于字符串tag，该值始终为NULL。',
        type: 'double'
      },
      {
        name: 'vValue',
        description: '字符串形式的值，在查询中使用此列允许您使用混合数据类型的值。',
        type: 'varchar'
      },
      {
        name: 'Quality',
        description: '与数据值相关联的基本数据质量指标。',
        type: 'int'
      },
      {
        name: 'QualityDetail',
        description: '数据质量的内部表示。',
        type: 'int'
      },
      {
        name: 'OPCQuality',
        description: '从数据源接收到的质量值。',
        type: 'int'
      },
      {
        name: 'wwTagKey',
        description: '单个AVEVA历史记录中tag的唯一数字标识符。',
        type: 'int'
      },
      {
        name: 'wwResolution',
        description: '在循环模式下检索数据的采样率，以毫秒为单位。',
        type: 'int'
      },
      {
        name: 'StartDateTime',
        description: '返回该行的检索周期的开始时间。',
        type: 'timestamp'
      },
      {
        name: 'SourceTag',
        description: '在存储该点时复制标记的源标记的名称。',
        type: 'varchar'
      },
      {
        name: 'SourceServer',
        description: '在存储该点时复制标记的服务器的名称。',
        type: 'varchar'
      }
    ]
  }
};

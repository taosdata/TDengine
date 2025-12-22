export default {
  name: 'OpenTSDB',
  id: 'opentsdb',
  type: 'uri',
  description:
    'OpenTSDB 是一个架构在 HBase 系统之上的实时监控信息收集和展示平台。\n\nTDengine 可以通过 OpenTSDB 连接器高效地读取 OpenTSDB 中的数据，并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: '连接协议',
          description: 'OpenTSDB 数据库的连接协议，请按实际情况选择，否则无法正常运行任务。',
          field: 'protocol',
          type: 'select',
          display_order: 0,
          defaultValue: 'http',
          required: true,
          options: [
            {
              label: 'HTTP 协议',
              value: 'http'
            },
            {
              label: 'HTTPS 协议',
              value: 'https'
            }
          ]
        },
        {
          label: '服务器地址',
          description:
            'OpenTSDB 数据库的 IP 地址或域名。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。',
          field: 'host',
          placeholder: '127.0.0.1',
          pattern: null,
          defaultValue: '',
          required: true,
          display_order: 1,
          type: 'input'
        },
        {
          label: '端口',
          description: 'OpenTSDB 数据库的服务端口。',
          field: 'port',
          placeholder: '4242',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: '端口号的范围是 0-65535',
          defaultValue: '',
          required: true,
          type: 'input'
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
          label: 'task',
          field: '5bb74215-3753-4f1b-a617-d727ab4786fc',
          description: '配置同步任务的数据集、时间范围与性能参数等内容。',
          children: [
            {
              label: '物理量 Metrics',
              description: 'OpenTSDB 中的物理量，可以指定多个需要同步的 Metrics，未指定则同步数据库中的全部数据。',
              field: 'metrics',
              placeholder: '请选择 Metrics',
              multiple: true,
              pattern: null,
              grid_two: false,
              type: 'bucket',
              options: []
            },
            {
              label: '起始时间',
              description: '数据的起始时间，同步任务仅读取该指定时间及之后的数据。',
              field: 'beginTime',
              required: true,
              placeholder: 'YYYY-MM-DD HH:mm:ss',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: '结束时间',
              description:
                '数据的截止时间，同步任务仅读取该指定时间及之前的数据，如果指定未来时间，任务将持续进行直至到达截止时间，如果未指定，任务将持续进行直至人为结束。',
              field: 'endTime',
              placeholder: 'YYYY-MM-DD HH:mm:ss',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: '每次读取的时间范围（分钟）',
              description: '每次从 OpenTSDB 读取数据时，最大的时间范围。',
              field: 'readWindow',
              placeholder: '请输入读取时间范围',
              defaultValue: '60',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1,
              max: 6000
            },
            {
              label: '延迟（秒）',
              description: '为了消除乱序数据的影响，TDengine 总是等待这里指定的时长，然后才读取数据。',
              field: 'delay',
              placeholder: '请输入延迟时长',
              defaultValue: '10',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1,
              max: 30
            },
            {
              label: '重命名时间戳字段',
              description: '重命名从 OpenTSDB 写入 TDengine 的时间戳字段，默认是 "timestamp"。',
              field: 'timestampFieldName',
              placeholder: '默认: timestamp',
              pattern: null,
              grid_two: false,
              type: 'input',
              min: 1,
              max: 30
            },
            {
              label: '重命名值字段',
              description: '重命名从 OpenTSDB 写入 TDengine 的值字段，默认是 "value"。',
              field: 'valueFieldName',
              placeholder: '默认: value',
              pattern: null,
              grid_two: false,
              type: 'input',
              min: 1,
              max: 30
            },
            {
              label: '子表名表达式',
              description:
                '自定义子表名的表达式。例如：tb_${tag1}_${tag2}，表示子表名由 tag1 和 tag2 两个标签值组成，未指定则使用默认的子表命名规则。',
              field: 'tableNamePattern',
              placeholder: '请输入子表名表达式',
              pattern: null,
              grid_two: false,
              type: 'input',
              min: 1,
              max: 200
            }
          ],
          hide: false
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
          label: '最大读取并发数',
          field: 'read_concurrency',
          description: '数据源连接数或读取线程数限制，当默认参数不满足需要或需要调整资源使用量时修改此参数。\n',
          defaultValue: '50',
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
          label: '最大写入并发数',
          field: 'write_concurrency',
          description: '写入 taosX 的最大并发数限制，当默认参数性能不足时，可增大此参数。\n',
          defaultValue: '50',
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 500
          },
          type: 'number',
          min: 1,
          max: 500
        },
        {
          label: '批次大小',
          field: 'batch_size',
          description: '单次发送的最大消息数或行数。\n',
          defaultValue: '5000',
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
          description: '单次读取最大延时（单位为秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n',
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

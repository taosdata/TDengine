import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
  name: 'Oracle',
  id: 'oracle',
  type: 'uri',
  description:
    'Oracle 数据库系统是世界上流行的关系数据库管理系统，系统可移植性好、使用方便、功能强，适用于各类大、中、小微机环境。它是一种高效率的、可靠性好的、适应高吞吐量的数据库方案。\nTDengine 可以高效地从 Oracle 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: '服务地址',
          description: 'Oracle 的服务器地址',
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
          description: 'Oracle 的端口',
          field: 'port',
          required: true,
          placeholder: '1521',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: '端口号的范围是 0-65535',
          defaultValue: '',
          type: 'input'
        },
        {
          label: '数据库',
          description: 'Oracle 数据库名称',
          field: 'subject',
          required: true,
          placeholder: '示例: db1',
          pattern: null,
          defaultValue: '',
          type: 'input'
        },
        {
          label: '最小连接数',
          description: '连接池中最小的连接数，默认为 5',
          field: 'min_connections',
          required: false,
          type: 'number',
          min: 1,
          max: 10000
        },
        {
          label: '最大连接数',
          description: '连接池中最大的连接数，默认为 20',
          field: 'max_connections',
          required: false,
          type: 'number',
          min: 1,
          max: 10000
        },
        {
          label: '连接超时',
          description: '连接池中连接的超时时间，单位为秒，默认为 20 秒',
          field: 'connection_timeout',
          required: false,
          type: 'number',
          min: 1,
          max: 2000
        }
      ]
    },
    {
      label: '认证',
      description: '使用用户名和密码访问 Oracle 数据库',
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
          label: 'SQL 查询',
          field: 'd4a7c949-da32-47e0-8f55-b949d1dbaf3b',
          description: '数据采集相关配置项。',
          children: [
            {
              label: '子表字段',
              description: '用于拆分子表的字段。',
              field: 'subtable_fields',
              placeholder: 'select distinct col_name1,col_name2 from table',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: 'SQL 模板',
              description:
                '用于查询的 SQL 语句，SQL 语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现（至少一个闭区间）。\nSQL使用不同的占位符表示不同的时间格式要求，具体有以下占位符格式：\n1. `${start}`、`${end}`：表示 RFC3339 格式时间戳，如：2024-03-14T08:00:00+0800\n2. `${start_no_tz}`、`${end_no_tz}`：表示不带时区的 RFC3339 字符串：2024-03-14T08:00:00\n3. `${start_date}`、`${end_date}`：表示仅日期，但 Oracle 中没有纯日期类型，所以它会带零时零分零秒，如：2024-03-14 00:00:00，所以使用 date <= `${end_date}` 时需要注意，它不能包含 2024-03-14 当天数据。\n\n如果使用子表字段，需要在语句中拼接字段占位符 `and ${col_name1} and ${col_name2}`，请注意，字段占位符大小写敏感，需要与数据库中字段保持一致。如果要按指定字段排序（建议按时间正序），需要在语句中拼接 `ORDER BY time`。\n\n示例：`SELECT * FROM table WHERE time >= ${start} AND time < ${end} and ${col_name1} and ${col_name2} ORDER BY time`',
              field: 'sql',
              required: true,
              placeholder: '完整示例请在描述中查看',
              pattern: null,
              grid_two: true,
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
              defaultValue: '0s',
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
      label: '数据映射',
      description: 'taosX 允许用户在数据库中指定数据模型，包括：指定表名称和超级表名，设置普通列和标签列等\n',
      field: 'parser',
      type: 'parser',
      fields: [
        {
          name: 'DateTime',
          description: '值对应的时间戳。',
          type: 'timestamp'
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
    display: '数据映射',
    required: true,
    description: 'taosX 允许用户在数据库中指定数据模型，包括：指定表名称和超级表名，设置普通列和标签列等\n',
    fields: [
      {
        name: 'DateTime',
        description: '值对应的时间戳。',
        type: 'timestamp'
      }
    ]
  }
};

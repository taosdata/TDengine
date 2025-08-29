export default {
  name: 'InfluxDB',
  id: 'influxdb',
  type: 'uri',
  description:
    'InfluxDB 是一种流行的开源时间序列数据库，它针对处理大量时间序列数据进行了优化。\n\nTDengine 可以通过 InfluxDB 连接器高效地读取 InfluxDB 中的数据，并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: '连接协议',
          description: 'InfluxDB 数据库的连接协议，请按实际情况选择，否则无法正常运行任务。',
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
            'InfluxDB 数据库的 IP 地址或域名。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。',
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
          description: 'InfluxDB 数据库的服务端口。',
          field: 'port',
          placeholder: '8086',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: '端口号的范围是 0-65535',
          defaultValue: '',
          required: true,
          type: 'input'
        }
      ]
    },
    {
      label: '认证',
      description: 'InfluxDB 的鉴权认证。',
      field: 'authentication',
      type: 'tabs',
      valueField: 'only-choose-one$',
      defaultValue: '2~x',
      multiple: false,
      children: [
        {
          label: '1.x 版本',
          name: '1~x',
          children: [
            {
              label: '版本',
              description: 'InfluxDB 数据库的版本，由于版本之间存在接口差异，所以请按实际情况选择。',
              placeholder: '请选择 InfluxDB 版本',
              required: true,
              field: 'version',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'select',
              options: [
                {
                  label: '1.8',
                  value: '1.8'
                },
                {
                  label: '1.7',
                  value: '1.7'
                }
              ]
            },
            {
              label: '用户',
              description: 'InfluxDB 数据库的用户，该用户必须在该组织中拥有读取权限。',
              placeholder: '请输入 InfluxDB 用户',
              required: true,
              field: 'username',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'input'
            },
            {
              label: '密码',
              description: 'InfluxDB 数据库中用户的登陆密码。',
              placeholder: '请输入登陆密码',
              required: true,
              field: 'password',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'password'
            }
          ]
        },
        {
          label: '版本 2.x',
          name: '2~x',
          children: [
            {
              label: '版本',
              description: 'InfluxDB 数据库的版本，由于版本之间存在接口差异，所以请按实际情况选择。',
              placeholder: '请选择 InfluxDB 版本',
              required: true,
              field: 'version',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'select',
              options: [
                {
                  label: '2.7',
                  value: '2.7'
                },
                {
                  label: '2.6',
                  value: '2.6'
                },
                {
                  label: '2.5',
                  value: '2.5'
                },
                {
                  label: '2.4',
                  value: '2.4'
                },
                {
                  label: '2.3',
                  value: '2.3'
                },
                {
                  label: '2.2',
                  value: '2.2'
                },
                {
                  label: '2.1',
                  value: '2.1'
                },
                {
                  label: '2.0',
                  value: '2.0'
                }
              ]
            },
            {
              label: '组织 ID',
              description:
                'InfluxDB 数据库的组织 ID, 它是一个由十六进制字符组成的字符串，而不是组织名称，可以从 InfluxDB 控制台的Organization -> About页面获取。',
              placeholder: '请输入 InfluxDB 组织 ID',
              required: true,
              field: 'orgId',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              pattern: {},
              patternMsg: '请输入十六进制字符',
              type: 'input'
            },
            {
              label: '令牌 Token',
              description: 'InfluxDB 数据库的访问令牌，该令牌必须在该组织中对要迁移的 bucket 拥有读取权限。',
              placeholder: '请输入 InfluxDB 令牌',
              required: true,
              field: 'token',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'input'
            },
            {
              label: '添加数据库保留策略',
              description:
                'InfluxQL 需要数据库与保留策略（DBRP）的组合才能查询数据，InfluxDB 的 Cloud 版本及某些 2.x 版本需要人工添加这个映射关系，打开这个开关，连接器可以在执行任务时自动添加。',
              required: false,
              field: 'addDbrp',
              defaultValue: false,
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'switch'
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
          label: 'task',
          field: 'task',
          description: '配置同步任务的数据集、时间范围与性能参数等内容。',
          children: [
            {
              label: '桶 Bucket',
              description: 'InfluxDB 数据库中的 Bucket，是存储数据的一个命名空间，每个任务需要指定一个 Bucket。',
              field: 'bucket',
              required: true,
              placeholder: '请选择 Bucket',
              pattern: null,
              grid_two: false,
              type: 'bucket',
              options: []
            },
            {
              label: '测量值 Measurements',
              description:
                'Bucket 中的测量值，可以指定多个需要同步的 Measurements，未指定则同步该 Bucket 中的全部数据。',
              field: 'measurements',
              placeholder: '请选择 Measurements',
              multiple: true,
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [],
              meta: {
                allowCreate: true,
                filterable: true
              }
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
              description: '每次从 InfluxDB 读取数据时，最大的时间范围。',
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
          label: '数据源并发读取方式',
          field: 'read_concurrency_type',
          description: 'measurement 的并行读取方式。queue: 多线程同时读取一个 measurement，完成后读取下一个。average: 平均方式，多个 measurement 同时被不同线程读取。sequence: 每个 measurement 同时只有一个线程读取。\n',
          defaultValue: 'sequence',
          required: false,
          hint: {
            type: 'str',
            choices: ['queue', 'average', 'sequence']
          },
          type: 'select',
          options: [
            {
              label: 'queue',
              value: 'queue'
            },
            {
              label: 'average',
              value: 'average'
            },
            {
              label: 'sequence',
              value: 'sequence'
            }
          ]
        },
        {
          label: '最大读取并发数',
          field: 'read_concurrency',
          description: '数据源连接数或读取线程数限制，当默认参数不满足需要或需要调整资源使用量时修改此参数。\n',
          defaultValue: 50,
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
          label: '每次读取行数',
          field: 'rows_per_read',
          description: '每次从 InfluxDB 读取数据时的行数。\n',
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
          label: '缓存队列大小',
          field: 'cache_queue_size',
          description: '从 InfluxDB 读取数据后放入的缓存列队的大小。\n',
          defaultValue: 200000,
          required: false,
          hint: {
            type: 'integer',
            min: 200000,
            max: 10000000
          },
          type: 'number',
          min: 200000,
          max: 10000000
        },
        {
          label: '批次大小',
          field: 'batch_size',
          description: '单次发送的最大消息数或行数。\n',
          defaultValue: 5000,
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 1000000
          },
          type: 'number',
          min: 1,
          max: 1000000
        },
        {
          label: 'JVM 参数',
          description:
            '控制 JVM 内存参数, GC类型等参数，比如：-Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2',
          field: 'jvm_opts',
          placeholder: '-Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2',
          pattern: null,
          defaultValue: '',
          required: false,
          display_order: 1,
          type: 'input'
        },
        {
          label: '批次延时',
          field: 'batch_timeout',
          description:
            '单次读取最大延时（单位为毫秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n',
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
    }
  ]
};

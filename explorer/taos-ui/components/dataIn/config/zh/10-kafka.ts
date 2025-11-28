import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
  name: 'Kafka',
  id: 'kafka',
  type: 'uri',
  description:
    'Apache Kafka 是一个用于流处理、实时数据管道和大规模数据集成的开源分布式流系统。\nTDengine 可以高效地从 Kafka 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据流入库。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: 'bootstrap-server',
          description:
            'Kafka Server 地址。\n<br/>如果配置多个，所有 Kafka Server 必须属于同一个集群，并使用逗号分割。\n<br/>如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。\n',
          field: 'endpoint',
          placeholder: 'ip:port,ip:port',
          pattern: null,
          defaultValue: '',
          required: true,
          display_order: 1,
          type: 'input'
        }
      ]
    },
    {
      label: 'SASL 认证',
      field: 'sasl',
      hide: true,
      children: [
        {
          label: '认证机制',
          description: 'SASL 的认证机制',
          field: 'sasl_mechanism',
          required: false,
          placeholder: '',
          defaultValue: '',
          pattern: null,
          grid_two: false,
          type: 'select',
          options: [
            {
              label: 'PLAIN',
              value: 'PLAIN'
            },
            {
              label: 'SCRAM-SHA-256',
              value: 'SCRAM-SHA-256'
            },
            {
              label: 'GSSAPI',
              value: 'GSSAPI'
            }
          ],
          meta: {
            allowCreate: true,
            filterable: true
          }
        },
        {
          label: '用户名',
          description: '用于 SASL 认证机制的用户名',
          field: 'sasl_username',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'input',
          displayDependsOn: ['sasl/sasl_mechanism'],
          displayDependsOnValues: {
            sasl_mechanism: ['PLAIN', 'SCRAM-SHA-256']
          }
        },
        {
          label: '密码',
          description: '用于 SASL 认证机制的密码',
          field: 'sasl_password',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'password',
          displayDependsOn: ['sasl/sasl_mechanism'],
          displayDependsOnValues: {
            sasl_mechanism: ['PLAIN', 'SCRAM-SHA-256']
          }
        },
        {
          label: 'Kerberos 服务名',
          description: ' 用于 GSSAPI 认证机制的 Kerberos 服务名',
          field: 'sasl_kerberos_service_name',
          required: true,
          placeholder: '示例：kafka',
          grid_two: false,
          type: 'input',
          displayDependsOn: ['sasl/sasl_mechanism'],
          displayDependsOnValues: {
            sasl_mechanism: ['GSSAPI']
          }
        },
        {
          label: 'Kerberos 主体',
          description: ' 用于 GSSAPI 认证机制的 Kerberos 主体',
          field: 'sasl_kerberos_principal',
          required: true,
          placeholder: '示例：kafkaclient',
          pattern: null,
          grid_two: false,
          type: 'input',
          displayDependsOn: ['sasl/sasl_mechanism'],
          displayDependsOnValues: {
            sasl_mechanism: ['GSSAPI']
          }
        },
        {
          label: 'Kerberos 初始化命令',
          description: '用于 GSSAPI 认证机制的 Kerberos 初始化命令',
          field: 'sasl_kerberos_kinit_cmd',
          required: false,
          placeholder: "示例：kinit -R -t '%{sasl.kerberos.keytab}' -k %{sasl.kerberos.principal}",
          pattern: null,
          grid_two: false,
          type: 'input',
          displayDependsOn: ['sasl/sasl_mechanism'],
          displayDependsOnValues: {
            sasl_mechanism: ['GSSAPI']
          }
        },
        {
          label: 'Kerberos 密钥表',
          description: '用于 GSSAPI 认证机制的 Kerberos 密钥表',
          field: 'sasl_kerberos_keytab',
          required: false,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: '',
          displayDependsOn: ['sasl/sasl_mechanism'],
          displayDependsOnValues: {
            sasl_mechanism: ['GSSAPI']
          }
        }
      ]
    },
    {
      label: 'SSL 认证',
      field: 'ssl',
      hide: true,
      children: [
        {
          label: 'CA',
          description: 'CA 证书文件(PEM格式), 用于验证 broker 的密钥。',
          field: 'ca',
          required: false,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: ''
        },
        {
          label: 'CA 密码',
          description: 'CA 私钥密码',
          field: 'ca_password',
          required: false,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'password'
        },
        {
          label: '客户端证书',
          description: '用于身份验证的客户端公钥文件(PEM格式)。',
          field: 'cert',
          required: false,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: ''
        },
        {
          label: '客户端私钥',
          description: '用于身份验证的客户端私钥文件(PEM格式)。',
          field: 'cert_key',
          required: false,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: ''
        }
      ]
    },
    {
      label: '采集配置',
      field: 'collect_options',
      description: '数据采集相关配置项。',
      children: [
        {
          label: '超时时间',
          description:
            '指定 Kafka Source 的超时时间，当从 Kafka 消费不到任何数据，超过 timeout 后，数据采集任务会退出。 默认值是 0 ms。 当 timeout 设置为 `0` 时，Kafka Source 会一直等待，直到有数据可用，或者发生错误。\n',
          field: 'timeout',
          placeholder: '输入范围为[0,60000]整数',
          pattern: null,
          patternMsg: '只能输入正整数或者0',
          grid_two: false,
          defaultValue: '0ms',
          type: 'composeAppend',
          options: [
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
          max: 60000
        },
        {
          label: '主题',
          description: '指定要消费的 Topic。可以配置多个 Topic，Topic 之间使用逗号分隔，例如：`tp1,tp2`。\n',
          field: 'topics',
          required: true,
          placeholder: 'tp1,tp2',
          pattern: null,
          grid_two: false,
          type: 'input'
        },
        {
          label: '客户端 ID',
          description: 'Kafka Broker 客户端 ID。',
          field: 'client_id',
          required: true,
          placeholder: '示例：client_id',
          pattern: null,
          grid_two: false,
          type: 'customId'
        },
        {
          label: '消费者组 ID',
          description: 'Kafka 消费者组 ID。',
          field: 'group',
          required: true,
          placeholder: '示例：group_id',
          pattern: null,
          grid_two: false,
          type: 'customId'
        },
        {
          label: 'Offset',
          description:
            'Fallback Offset 参数可以指定以下值：\n* `Earliest`：用于请求最早的 offset. \n* `Latest`：用于请求最晚的 offset. \n* 默认值为Earliest。',
          field: 'fallback_offset',
          placeholder: 'Earliest',
          defaultValue: 'Earliest',
          pattern: null,
          grid_two: false,
          type: 'select',
          options: [
            {
              label: 'Earliest',
              value: 'Earliest'
            },
            {
              label: 'Latest',
              value: 'Latest'
            }
          ],
          meta: {
            allowCreate: true,
            filterable: true
          }
        },
        {
          label: '字符编码',
          description: 'taosX 默认只接收 utf8 编码的字符串，如果发送端使用了非 utf8 编码，需要在这里指定。',
          field: 'char_encoding',
          placeholder: '',
          defaultValue: 'UTF_8',
          pattern: null,
          grid_two: false,
          type: 'select',
          options: [
            {
              label: 'UTF_8',
              value: 'UTF_8'
            },
            {
              label: 'GBK',
              value: 'GBK'
            },
            {
              label: 'GB18030',
              value: 'GB18030'
            },
            {
              label: 'BIG5',
              value: 'BIG5'
            }
          ],
          meta: {
            allowCreate: true,
            filterable: true
          }
        }
      ],
      hide: false
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
      label: 'Payload 转换',
      description: '',
      field: 'parser',
      type: 'parser',
      fields: [
        {
          name: 'ts',
          description: '时间戳。',
          type: 'timestamp'
        },
        {
          name: 'topic',
          description: '主题名。',
          type: 'varchar'
        },
        {
          name: 'partition',
          description: '分区 ID。',
          type: 'int'
        },
        {
          name: 'offset',
          description: '偏移。',
          type: 'bigint'
        },
        {
          name: 'key',
          description: '消息 Key。',
          type: 'varchar'
        },
        {
          name: 'value',
          description: '消息体。',
          type: 'varchar'
        }
      ],
      defaultValue: {
        parse: {},
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
    description:
      'Kafka 连接器会上传以下六列到服务端：<br>\n\n- **ts**: 采集时间戳。<br>\n- **topic**: 订阅主题名。<br>\n- **partition**: 当前消息所在的分区 ID。<br>\n- **offset**: 当前消息的偏移量。<br>\n- **key**: 当前消息的 Key。<br>\n- **value**: 当前消息的数据内容。<br>\n\ntaosX 可以使用 JSON 提取器解析数据，并允许用户在数据库中指定数据模型，<br>\n包括，指定表名称和超级表名，设置普通列和标签列等。\n',
    fields: [
      {
        name: 'ts',
        description: '时间戳。',
        type: 'timestamp'
      },
      {
        name: 'topic',
        description: '主题名。',
        type: 'varchar'
      },
      {
        name: 'partition',
        description: '分区 ID。',
        type: 'int'
      },
      {
        name: 'offset',
        description: '偏移。',
        type: 'bigint'
      },
      {
        name: 'key',
        description: '消息 Key。',
        type: 'varchar'
      },
      {
        name: 'value',
        description: '消息体。',
        type: 'varchar'
      }
    ]
  }
};

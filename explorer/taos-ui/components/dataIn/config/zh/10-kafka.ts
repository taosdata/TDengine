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
          host: {
            label: 'bootstrap-server',
            description:
              'Kafka Server 地址。\n<br/>如果配置多个，所有 Kafka Server 必须属于同一个集群。\n<br/>如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。\n',
            field: 'host_1',
            placeholder: '127.0.0.1',
            required: true,
            pattern: null,
            defaultValue: '',
            type: 'input'
          },
          port: {
            label: '服务端口',
            description: 'Kafka 的端口',
            field: 'port_1',
            placeholder: '9092',
            required: true,
            pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
            patternMsg: '端口号的范围是 0-65535',
            defaultValue: '',
            type: 'input'
          }
        }
      ],
      type: 'grouping'
    },
    {
      label: 'Groups-before',
      field: 'groups_before',
      hide: true,
      children: [
        {
          label: 'SASL 认证机制',
          field: 'sasl',
          description: '用来认证服务器与客户端的一种认证机制。',
          hide: false,
          type: 'switch',
          defaultValue: false,
          valueField: 'isEnable',
          hasValue: true,
          children: [
            {
              label: '认证机制',
              description: 'SASL 的认证机制',
              field: 'sasl_mechanism',
              required: true,
              placeholder: '',
              defaultValue: 'PLAIN',
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
              },
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/sasl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
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
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/sasl/isEnable', 'groups_before/sasl/sasl_mechanism'],
              displayDependsOnValues: {
                isEnable: [true],
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
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/sasl/isEnable', 'groups_before/sasl/sasl_mechanism'],
              displayDependsOnValues: {
                isEnable: [true],
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
              displayDependsOn: ['groups_before/sasl/sasl_mechanism'],
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
              displayDependsOn: ['groups_before/sasl/sasl_mechanism'],
              displayDependsOnValues: {
                sasl_mechanism: ['GSSAPI']
              }
            },
            {
              label: 'Kerberos 初始化命令',
              description: '用于 GSSAPI 认证机制的 Kerberos 初始化命令',
              field: 'sasl_kerberos_kinit_cmd',
              required: true,
              placeholder: "示例：kinit -R -t '%{sasl.kerberos.keytab}' -k %{sasl.kerberos.principal}",
              pattern: null,
              grid_two: false,
              type: 'input',
              displayDependsOn: ['groups_before/sasl/sasl_mechanism'],
              displayDependsOnValues: {
                sasl_mechanism: ['GSSAPI']
              }
            },
            {
              label: 'Kerberos 密钥表',
              description: '用于 GSSAPI 认证机制的 Kerberos 密钥表',
              field: 'sasl_kerberos_keytab',
              required: true,
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'file',
              templateUrl: '',
              displayDependsOn: ['groups_before/sasl/sasl_mechanism'],
              displayDependsOnValues: {
                sasl_mechanism: ['GSSAPI']
              }
            }
          ]
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
              label: 'CA',
              description: 'CA 证书文件(PEM格式), 用于验证 broker 的密钥。',
              field: 'ca',
              required: true,
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
              label: 'CA 密码',
              description: 'CA 私钥密码',
              field: 'ca_password',
              required: true,
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'password',
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/ssl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
              }
            },
            {
              label: '客户端证书',
              description: '用于身份验证的客户端公钥文件(PEM格式)。',
              field: 'cert',
              required: true,
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
              label: '客户端私钥',
              description: '用于身份验证的客户端私钥文件(PEM格式)。',
              field: 'cert_key',
              required: true,
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
              unit_value: 'ms',
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

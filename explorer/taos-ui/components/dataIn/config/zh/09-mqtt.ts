import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
  name: 'MQTT',
  id: 'mqtt',
  type: 'uri',
  description:
    'MQTT 表示 Message Queuing Telemetry Transport （消息队列遥测传输）。它是一种轻量级的消息协议，易于实现和使用。它非常适合连接资源有限的设备，例如电池供电的设备或带宽较低的设备。MQTT也是实时控制系统等延迟重要的应用程序的不错选择。\n\nMQTT 通过使用发布/订阅模型来工作。这意味着设备可以将消息发布到主题，其他设备可以订阅这些主题以接收消息。这使得轻松将设备解耦，并根据需要扩展应用程序。\n\nMQTT 是物联网应用程序的流行选择。它得到了广泛的设备和平台支持，并提供许多开源和商业实现。\n\ntaosX 可以通过连接器插件从 MQTT 代理订阅数据。请查看每个部分的帮助消息以了解详细信息。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: 'MQTT 地址',
          description:
            'MQTT 服务器地址。如: “127.0.0.1”\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。\n',
          field: 'host',
          placeholder: '127.0.0.1',
          pattern: null,
          defaultValue: '',
          required: true,
          display_order: 1,
          type: 'input'
        },
        {
          label: 'MQTT 端口',
          description: 'MQTT 服务器端口',
          field: 'port',
          placeholder: '1883',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: '端口号的范围是 0-65535',
          defaultValue: '1883',
          required: true,
          type: 'input'
        },
        {
          label: 'TLS 校验',
          description:
            '是否开启 TLS 校验。\n开启单向校验后，需要上传 CA 证书文件，用于校验 MQTT 服务器证书。\n开启双向校验后，需要上传 CA 证书文件、客户端证书和客户端私钥文件，用于校验 MQTT 服务器证书和客户端证书。\n',
          field: 'tsl_verify',
          placeholder: '',
          defaultValue: 'none',
          pattern: null,
          grid_two: false,
          type: 'select',
          options: [
            {
              label: '不开启',
              value: 'none'
            },
            {
              label: '单向校验',
              value: 'single'
            },
            {
              label: '双向校验',
              value: 'both'
            }
          ]
        },
        {
          label: 'CA',
          description: 'CA 证书文件，用于校验 MQTT 服务器证书。',
          field: 'ca',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: '',
          displayDependsOn: ['connection_options/tsl_verify'],
          displayDependsOnValues: {
            tsl_verify: ['single', 'both']
          }
        },
        {
          label: '客户端证书',
          description: '需要 .cert 文件。',
          field: 'cert',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: '',
          displayDependsOn: ['connection_options/tsl_verify'],
          displayDependsOnValues: {
            tsl_verify: ['both']
          }
        },
        {
          label: '客户端私钥',
          description: '私钥文件，和客户端证书必须同时上传。',
          field: 'cert_key',
          placeholder: '',
          required: true,
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: '',
          displayDependsOn: ['connection_options/tsl_verify'],
          displayDependsOnValues: {
            tsl_verify: ['both']
          }
        }
      ]
    },
    {
      label: '认证',
      description: '使用用户名和密码访问 MQTT Broker。',
      field: 'authentication',
      type: 'tabs',
      valueField: 'currentTab',
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
              field: 'username',
              defaultValue: '',
              type: 'input'
            },
            {
              label: '密码',
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
      children: [
        {
          label: '采集配置',
          field: 'collect',
          description: '采集任务配置',
          children: [
            {
              label: 'MQTT 协议',
              description: 'MQTT 协议版本。',
              field: 'version',
              required: true,
              placeholder: '',
              defaultValue: '3.1',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: '3.1',
                  value: '3.1'
                },
                {
                  label: '3.1.1',
                  value: '3.1.1'
                },
                {
                  label: '5.0',
                  value: '5.0'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: '客户端 ID',
              description: 'MQTT Broker 客户端 ID。',
              field: 'client_id',
              required: true,
              placeholder: '示例：client_id',
              pattern: null,
              grid_two: false,
              type: 'customId'
            },
            {
              label: 'Keep Alive',
              description:
                '如果代理在保持活动间隔内没有收到来自客户端的任何消息，它将假定客户端已断开连接，并关闭连接。\n\n保持活动间隔是指客户端和代理之间协商的时间间隔，用于检测客户端是否活动。如果客户端在保持活动间隔内没有向代理发送消息，则代理将断开连接。\n\n保持活动间隔的默认值为60秒，但可以通过在连接时设置 CONNECT 报文中的 keep alive 字段来更改它。\n',
              field: 'keep_alive',
              placeholder: '10',
              defaultValue: '60',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1
              // "max": null
            },
            {
              label: 'Clean Session',
              description:
                '如果clean session标志设置为True，则代理将忘记有关会话的所有信息，包括客户端的订阅。<br>\nclean session 标志的默认值为True。<br>\n如果设置为False，则代理将保留有关客户端的信息，包括其订阅。这意味着客户端在重新连接时可以恢复其以前的订阅。<br>\n',
              field: 'clean_session',
              placeholder: '',
              defaultValue: true,
              pattern: null,
              grid_two: false,
              type: 'switch'
            },
            {
              label: '订阅主题及 QoS 配置',
              description:
                '输入格式 `<topic name>::<QoS>`，其中QoS 只能输入0、1、2，订阅多个主题使用逗号分割，例如: `topic1::0,topic2::1`\n',
              field: 'topics',
              required: true,
              placeholder: 'topic1::0,topic2::1',
              pattern: '^(?:[^,\\s]+(?:\\s+[^,\\s]+)*::[0-2],)*[^,\\s]+(?:\\s+[^,\\s]+)*::[0-2]$',
              patternMsg:
                '输入格式有误，请按照格式 `<topic name>::<QoS>`，其中QoS 只能输入0、1、2，例如： `topic1::0,topic2::1`',
              grid_two: false,
              type: 'input'
            },
            {
              field: 'topic_pattern',
              label: '主题解析',
              type: 'input',
              short_description: '',
              description: '将订阅主题通配符内容解析为变量',
              required: false,
              placeholder: '_/_/site_controller_id/_/point_name/data_type'
            },
            {
              label: '数据压缩',
              description:
                '为了节省网络带宽，您可以将数据压缩后发送给 mqtt broker，这里配置同样的压缩算法，可实现解压缩',
              field: 'compression',
              placeholder: '',
              defaultValue: 'none',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'none',
                  value: 'none'
                },
                {
                  label: 'gzip',
                  value: 'gzip'
                },
                {
                  label: 'snappy',
                  value: 'snappy'
                },
                {
                  label: 'lz4',
                  value: 'lz4'
                },
                {
                  label: 'zstd',
                  value: 'zstd'
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
          description: '时间戳',
          type: 'timestamp'
        },
        {
          name: 'topic',
          description: '主题',
          type: 'varchar'
        },
        {
          name: 'qos',
          description: '质量',
          type: 'int'
        },
        {
          name: 'payload',
          description: '负载',
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
          label: '消息等待队列大小',
          field: 'unprocessed_messages_buffer_size',
          description:
            '缓存在队列中还没来得及处理的消息的最大数量，用于控制内存占用，当队列满时，新到达的数据会直接丢弃。可设置为 0，即不缓存。',
          defaultValue: '50000',
          required: false,
          type: 'number',
          min: 0,
          max: 100000
        },
        {
          label: '处理中批次上限',
          field: 'maximum_processing_batch',
          description:
            '允许在处理中还没有等到 ACK 回复的最大批次数量，没有到达此阈值时，会从缓存队列中取出一个批次进行处理；当到达最大数量后，缓存队列中的消息会开始积压。此配置用于背压机制防止对下游造成太大写入压力。',
          defaultValue: '100',
          required: false,
          type: 'number',
          min: 1,
          max: 1000
        },
        {
          label: '批次大小',
          field: 'batch_size',
          description: '单次发送的最大消息数或行数。\n',
          defaultValue: '1000',
          required: false,
          type: 'number',
          min: 1,
          max: 10000
        },
        {
          label: '批次延时',
          field: 'batch_timeout',
          description:
            '单次读取最大延时（单位为毫秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n',
          defaultValue: '500',
          required: false,
          type: 'number',
          min: 1,
          max: 60000
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
          label: '缓存实时数据',
          field: 'persist_data_enable',
          description:
            '开启后，当 taosX 由于性能不足或者下游 TDengine 写入慢时，会将实时数据暂存，等恢复时再将缓存数据重新写入下游 TDengine.\n',
          defaultValue: false,
          required: false,
          type: 'switch'
        },
        {
          label: '缓存数据存储目录',
          field: 'persist_data_dir',
          description: '自定义缓存数据存储目录，默认存储到系统数据目录下。\n',
          placeholder: '$DATA_DIR/tasks/:id/persist_queue/',
          required: false,
          type: 'input',
          displayDependsOn: ['advanced_options/persist_data_enable'],
          displayDependsOnValues: {
            persist_data_enable: [true]
          }
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
          type: 'input',
          displayDependsOn: ['advanced_options/keep_raw_data'],
          displayDependsOnValues: {
            keep_raw_data: [true]
          }
        },
        {
          label: '健康监测时段',
          field: 'health_check_window_in_second',
          description: '表示对最近多长时间的任务状态进行统计。通常为分钟级，此时段对健康状态各种模式统一生效。\n',
          defaultValue: '0s',
          placeholder: '输入范围为[0,60000]整数',
          required: false,
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
          type: 'number',
          min: 0,
          max: 10000
        }
      ]
    },
    exceptionStrategy
  ]
  // parser: {
  //   display: 'Payload 转换',
  //   required: true,
  //   description:
  //     'MQTT 连接器会上传以下四列到服务端：\n\n- **ts**: 采集时间戳。\n- **topic**: 订阅主题名。\n- **qos**: 采集点质量。\n- **payload**: 采集数据。\n\ntaosX 可以使用 JSON 提取器解析数据，并允许用户在数据库中指定数据模型，包括，指定表名称和超级表名，设置普通列和标签列等。\n',
  //   fields: [
  //     {
  //       name: 'ts',
  //       description: '时间戳',
  //       type: 'timestamp'
  //     },
  //     {
  //       name: 'topic',
  //       description: '主题',
  //       type: 'varchar'
  //     },
  //     {
  //       name: 'qos',
  //       description: '质量',
  //       type: 'int'
  //     },
  //     {
  //       name: 'payload',
  //       description: '负载',
  //       type: 'varchar'
  //     }
  //   ]
  // }
};

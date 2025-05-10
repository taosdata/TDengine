export default {
  name: 'TDengine 数据订阅',
  id: 'tmq',
  type: 'uri',
  description:
    '使用 TMQ 进行 TDengine 指定从数据库或超级表的订阅。\n\n支持使用原生连接或 WebSocket 连接（使用 HTTP 或 HTTPS 协议）。默认使用原生连接。\n\n使用 `database` 方式指定数据库名，或 `database.table` 方式指定订阅一个超级表或普通表。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: 'Topic DSN',
          description:
            '请登录 TDengine 云服务或打开企业版的 Explorer, 点击`数据订阅`，你将看到主题列表，复制主题对应的 DSN 到这里即可。\n',
          field: 'endpoint',
          placeholder: 'Topic 示例: tmq+ws://root:taosdata@localhost:6041/topic',
          pattern: null,
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
          label: '订阅设置',
          field: '1257130d-bd33-4400-b2b6-3f4f69b700dc',
          description: 'TDengine TMQ 订阅设置。',
          children: [
            {
              label: '订阅初始位置',
              description:
                '订阅初始位置定义了拉取数据范围。\n有以下可选项：\n- *earliest*: 相当于拉取全量数据，包括新增的数据；\n- *latest*: 从最新的数据开始订阅。\n',
              field: 'auto.offset.reset',
              placeholder: '',
              defaultValue: 'earliest',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'earliest',
                  value: 'earliest'
                },
                {
                  label: 'latest',
                  value: 'latest'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: '订阅组 ID',
              description:
                '订阅组 ID 是用于标识一个订阅组的字符串，最大长度为 192。同一个订阅组内的订阅者共享消费进度。不指定情况下将使用随机生成的 group ID。\n',
              field: 'group.id',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: '客户端 ID',
              description: '客户端 ID 是一个用于标识客户端的字符串，最大长度为 192。\n',
              field: 'client.id',
              placeholder: '客户端 ID 是一个用于标识客户端的字符串，最大长度为 192',
              grid_two: false,
              required: true,
              type: 'input'
            },
            {
              label: '超时',
              description:
                '超时时间范围内没有新增数据，同步任务将自动结束。\n可配置为：\n- `0`: 表示无超时时间，持续进行订阅。\n- 指定超时时间：`5s`, `1m` 等。\n',
              field: 'timeout',
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
              label: '同步已落盘数据',
              description:
                '如启用，可以同步已经落盘到 TSDB 时序数据存储文件中（即不在 WAL 中）的数据。如关闭，则只同步尚未落盘（即保存在 WAL 中）的数据。\n',
              field: 'experimental.snapshot.enable',
              placeholder: '',
              defaultValue: true,
              pattern: null,
              grid_two: false,
              type: 'switch'
            },
            {
              label: '同步删表操作',
              description: '如启用则会同步删表操作到目标数据库。\n',
              field: 'with.meta.drop',
              placeholder: '',
              defaultValue: true,
              pattern: null,
              grid_two: false,
              type: 'switch'
            },
            {
              label: '同步删数据操作',
              description: '如启用则会同步删数据操作到目标数据库。\n',
              field: 'with.meta.delete',
              placeholder: '',
              defaultValue: true,
              pattern: null,
              grid_two: false,
              type: 'switch'
            }
          ],
          hide: false
        }
      ]
    },
    {
      label: '高级选项',
      field: 'advanced_options',
      description: '调整与读并发、写并发和错误日志相关的参数。\n',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
        {
          label: '压缩',
          field: 'compression',
          description: '启用 WebSocket 压缩支持，以降低网络带宽占用。\n',
          defaultValue: false,
          required: false,
          type: 'switch'
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
        },
        {
          field: 'num.of.consumers',
          label: '消费者数量',
          hint: {
            type: 'integer',
            min: 0,
            max: 1000
          },
          type: 'number',
          description: '消费者数量',
          defaultValue: '0'
        },
        {
          field: 'num.of.writers',
          label: '写入线程数',
          hint: {
            type: 'integer',
            min: 0,
            max: 1000
          },
          type: 'number',
          description: '写入线程数',
          defaultValue: '0'
        },
        {
          field: 'prefer',
          label: '写入偏好',
          options: [
            {
              label: 'auto',
              value: 'auto'
            },
            {
              label: 'raw',
              value: 'raw'
            }
          ],
          type: 'select',
          description: '写入偏好，auto: 根据读取的数据程序自动选择',
          defaultValue: 'auto'
        },
        {
          field: 'commit.chunk.size',
          label: '缓冲区大小',
          hint: {
            type: 'integer',
            min: 0,
            max: 1000000000
          },
          type: 'number',
          description: '缓冲区大小',
          defaultValue: '0'
        },
        {
          field: 'commit.interval.ms',
          label: 'Commit 间隔(毫秒)',
          hint: {
            type: 'integer',
            min: 0,
            max: 1000000
          },
          type: 'number',
          description: 'Commit 间隔(毫秒)',
          defaultValue: '0'
        }
      ]
    }
  ]
};

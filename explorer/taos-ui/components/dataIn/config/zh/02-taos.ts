export default {
  name: 'TDengine 查询',
  id: 'taos',
  type: 'uri',
  description: '从旧版本 TDengine (2.4+, 3.0+) 迁移到当前集群。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: '连接协议',
          description: '选择使用何种方式连接到 TDengine 数据源。',
          field: 'protocol',
          type: 'select',
          display_order: 0,
          defaultValue: 'ws',
          required: true,
          options: [
            {
              label: 'WS',
              value: 'ws',
              description: '使用 HTTP 协议的 WebSocket 连接。'
            },
            {
              label: 'WSS',
              value: 'wss',
              description: '使用 HTTPS 协议的 WebSocket 连接。'
            }
          ]
        },
        {
          label: '服务器',
          description: 'TDengine REST API 服务地址。如果应用多节点，建议配合负载均衡器使用。',
          field: 'host',
          placeholder: 'taos-adapter-addr',
          pattern: null,
          defaultValue: '',
          required: true,
          display_order: 1,
          type: 'input'
        },
        {
          label: '端口',
          description: 'TDengine REST API 服务端口。',
          field: 'port',
          placeholder: '6041',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: '端口号的范围是 0-65535',
          defaultValue: '',
          required: true,
          type: 'input'
        },
        {
          label: '数据库',
          description: '数据库名称，支持特殊字符。',
          field: 'subject',
          placeholder: '示例: db1',
          pattern: null,
          defaultValue: '',
          required: true,
          type: 'input'
        }
      ]
    },
    {
      label: '认证',
      description: '使用用户名密码进行认证。',
      field: 'authentication',
      children: [
        {
          label: '用户名密码',
          name: '',
          field: 'plain',
          children: [
            {
              label: '用户名',
              description: 'TDengine 用户名，默认使用 `root`。',
              field: 'username',
              defaultValue: 'root',
              type: 'input'
            },
            {
              label: '密码',
              description: 'TDengine 密码，默认为 `taosdata`。',
              field: 'password',
              defaultValue: 'taosdata',
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
          label: '迁移模式',
          field: 'migrate_options',
          description: '支持迁移历史数据或近实时数据同步，也可设置是否总是重建表模型。',
          children: [
            {
              label: 'Mode',
              description: '迁移历史数据（`history`）或实时数据（`realtime`）或两者（`both`）。',
              field: 'mode',
              placeholder: '',
              defaultValue: 'history',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'history',
                  value: 'history'
                },
                {
                  label: 'realtime',
                  value: 'realtime'
                },
                {
                  label: 'all',
                  value: 'all'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: '表结构',
              description:
                '是否迁移表结构。\n\n- `only`: 仅迁移表结构，不迁移表数据。\n- `none`: 不迁移表结构，仅迁移表数据。\n- `always`: 始终迁移表结构和数据。\n',
              field: 'schema',
              placeholder: '',
              defaultValue: 'always',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'always',
                  value: 'always'
                },
                {
                  label: 'none',
                  value: 'none'
                },
                {
                  label: 'only',
                  value: 'only'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: '稀疏模式',
              description: '启用此模式以提升多表低频场景下的性能。',
              field: 'sparse',
              placeholder: '',
              defaultValue: false,
              pattern: null,
              grid_two: false,
              type: 'switch'
            },
            {
              label: '元数据轮询间隔',
              description: '元数据轮询间隔，用于同步过程中的元数据变更检测。',
              field: 'schema-polling-interval',
              placeholder: '输入范围为[0,60000]整数',
              defaultValue: '5s',
              pattern: null,
              patternMsg: '只能输入正整数或者0',
              grid_two: false,
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
        },
        {
          label: '表',
          field: 'what_to_migrate',
          description: '如果不是迁移全部数据，请配置需要迁移的表。',
          children: [
            {
              label: '超级表',
              description: '逗号分隔的一个或多个超级表。选择超级表会迁移超级表下的所有子表数据。',
              field: 'stables',
              placeholder: 'metrics',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: '表',
              description: '子表或普通表，支持 `tb1` 形式的表名或 `stable.table` 形式的子表名。\n',
              field: 'tables',
              placeholder: 'd0001',
              pattern: null,
              grid_two: false,
              type: 'input'
            }
          ],
          hide: false
        },
        {
          label: '时间范围',
          field: 'range',
          description: '迁移时间范围和查询单元。',
          children: [
            {
              label: '开始时间',
              description: '迁移数据开始时间。',
              field: 'start',
              placeholder: '2023-10-01T12:00:00.000+08:00',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime',
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'],
              displayDependsOnValues: {
                mode: ['history', 'all'],
                schema: ['always', 'none']
              }
            },
            {
              label: '结束时间',
              description: '迁移数据结束时间。',
              field: 'end',
              placeholder: '2023-10-02T12:00:00.000+08:00',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime',
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'],
              displayDependsOnValues: {
                mode: ['history', 'all'],
                schema: ['always', 'none']
              }
            },
            {
              label: '查询单元',
              description:
                '查询数据的基本单元，长时间范围的查询会以此为依据切割为多次查询。<br>\n支持使用数字加单位缩写，如"1ms"表示1毫秒，"1s"表示1秒，"1m"表示1分钟，"1h"表示1小时，"1d"表示1天，"1w"表示1周。<br>\n单独使用数字则默认认为是秒。<br>',
              field: 'unit',
              placeholder: '输入范围为[0,60000]整数',
              defaultValue: '1d',
              pattern: null,
              patternMsg: '只能输入正整数或者0',
              grid_two: false,
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
                }
              ],
              min: 0,
              max: 60000,
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'],
              displayDependsOnValues: {
                mode: ['history', 'all'],
                schema: ['always', 'none']
              }
            }
          ],
          hide: false
        },
        {
          label: '实时同步',
          field: 'realtime_settings',
          description: '以下参数仅在实时同步模式（`realtime`）下支持。',
          children: [
            {
              label: '回溯',
              description:
                '在实时同步前回溯一段时间内的数据写入目标库。<br>\n支持使用数字加单位缩写，如"1ms"表示1毫秒，"1s"表示1秒，"1m"表示1分钟，"1h"表示1小时，"1d"表示1天，"1w"表示1周。<br>\n单独使用数字则默认认为是秒。<br>',
              field: 'retro',
              placeholder: '输入范围为[0,60000]整数',
              defaultValue: '0s',
              pattern: null,
              patternMsg: '只能输入正整数或者0',
              grid_two: false,
              type: 'composeAppend',
              options: [
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
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'], // 代表层级
              displayDependsOnValues: {
                mode: ['realtime', 'all'],
                schema: ['always', 'none']
              }
            },
            {
              label: '间隔',
              description:
                '轮询查询的时间间隔。<br>\n支持使用数字加单位缩写，如"1ms"表示1毫秒，"1s"表示1秒，"1m"表示1分钟，"1h"表示1小时，"1d"表示1天，"1w"表示1周。<br>\n单独使用数字则默认认为是秒。<br>',
              field: 'interval',
              placeholder: '输入范围为[0,60000]整数',
              defaultValue: '1s',
              pattern: null,
              patternMsg: '只能输入正整数或者0',
              grid_two: false,
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
              max: 60000,
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'],
              displayDependsOnValues: {
                mode: ['realtime', 'all'],
                schema: ['always', 'none']
              }
            },
            {
              label: '乱序',
              description:
                '等待一段时间的乱序数据入库后再进行查询。<br>\n支持使用数字加单位缩写，如"1ms"表示1毫秒，"1s"表示1秒，"1m"表示1分钟，"1h"表示1小时，"1d"表示1天，"1w"表示1周。<br>\n单独使用数字则默认认为是秒。<br>',
              field: 'excursion',
              placeholder: '输入范围为[0,60000]整数',
              defaultValue: '500ms',
              pattern: null,
              patternMsg: '只能输入正整数或者0',
              grid_two: false,
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
              max: 60000,
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'],
              displayDependsOnValues: {
                mode: ['realtime', 'all'],
                schema: ['always', 'none']
              }
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
          label: '最大读并发数',
          field: 'workers',
          description: '并发查询的线程数，如果为 0 会自动设置为 CPU 核数。',
          defaultValue: '0',
          required: false,
          type: 'number',
          min: 0,
          max: 100
        },
        {
          label: '最大写并发数',
          field: 'write-concurrency',
          description: '写入目标数据库的整体最大并发数。不能小于读并发数，默认等于读并发数。\n',
          defaultValue: '1',
          required: false,
          type: 'number',
          min: 1,
          max: 100
        },
        {
          label: '错误记录文件',
          field: 'fails-to',
          description:
            'taosX 所处运行环境的一个绝对路径。 如有值，写入失败的数据及失败原因将被写入该文件，并不阻塞任务执行。如无值，写入失败会导致任务中断。\n',
          required: false,
          type: 'input'
        },
        {
          label: '压缩',
          field: 'compression',
          description: '启用 WebSocket 压缩支持，以降低网络带宽占用。\n',
          defaultValue: false,
          required: false,
          type: 'switch'
        }
      ]
    }
  ]
};

export default {
  name: 'OPC-DA',
  id: 'opcda',
  type: 'uri',
  description:
    'OPC是工业自动化领域和其他行业中安全可靠地交换数据的互操作标准之一。\n\nOPC DA（数据访问）是一种经典的基于COM的规范，仅适用于Windows。尽管OPC DA不是最新和最高效的数据通信规范，但它被广泛使用。这主要是因为一些旧设备只支持OPC DA。\n\nOPC UA是经典OPC规范的下一代标准，是一个平台无关的面向服务的架构规范，集成了现有OPC Classic规范的所有功能，提供了一条迁移到更安全和可扩展解决方案的路径。\n\n如果想了解更多关于OPC UA/DA的信息，可以阅读OPC Foundation网站和一些有用的博客，例如：\n1. [What is OPC](https://opcfoundation.org/about/what-is-opc/)\n2. [What is OPC DA](https://plcynergy.com/opc-da/)\n\ntaosX 使用 OPC 连接器从 OPC 服务器拉取或订阅数据。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: '服务地址',
          description:
            'OPC 服务器地址。如： `127.0.0.1<,localhost>/Matrikon.OPC.Simulation.1`。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址应是 taosX 服务器所在主机。\n',
          field: 'endpoint',
          required: true,
          placeholder: '127.0.0.1/Matrikon.OPC.Simulation.1',
          pattern: null,
          defaultValue: '',
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
      label: '点位集',
      description: 'OPC 采集数据点位列表。',
      field: 'datasets',
      type: 'tabs',
      multiple: false,
      name: 'datasets',
      valueField: 'currentTab',
      defaultValue: 'csv_config_file',
      children: [
        {
          label: '上传 CSV 配置文件',
          name: 'csv_config_file',
          labelShow: false,
          labelWidth: '0px',
          category: 'csv_config_file',
          radio: false,
          description:
            'OPC 数据写入使用 csv 文件定义每一个数据点位到 TDengine 数据子表的映射规则：\n\n(1) tag_name：必填，数据点位在 OPC DA 服务器上的 id；\n\n(2) stable：必填，数据点位对应的 TDengine 超级表；\n\n(3) tbname：必填，数据点位对应的 TDengine 子表；\n\n(4) enable：可选，默认值 \'1\'，指定是否采集该点位数据。0-不采集并且删除对应子表，1-采集点位数据，没有子表时创建子表；\n\n(5) value_col：可选，默认值 \'val\'。数据点位采集值在 TDengine 中对应的列名；\n\n(6) value_transform：可选，数据点位采集值在 taosX 中执行的变换函数，目前仅支持数值计算表达式，详见 transform 文档的 expr 表达式说明；\n\n(7) type：可选，默认值取源数据类型。数据点位采集值的数据类型，可用于替换超级表名称中的占位符 {type}；\n\n(8) quality_col：可选，数据点位采集值质量在 TDengine 中对应的列名；\n\n(9) ts_col/request_ts_col/received_ts_col：必填，TDengine 时间戳主键定义：可只保留其中一列，保留的时间戳列将作为主键；也可填写多列，居前的时间戳列作为主键；其中 ts_col 使用数据点位上报 opc server 时间，request_ts_col 使用 observe 采集模式下每次轮询的发起请求时间，received_ts_col 使用从 opc server 接收到数据的时间；\n\n(10) xx_ts_transform：可选，时间戳变换函数，参考 transform 数值计算表达式 expr 的说明；\n\n(11) tag::VARCHAR(200)::name：可选/可配置多个tag列；数据点位在 TDengine 中对应的 Tag 列；其中 tag 为保留关键字，表示该列为一个 tag 列；VARCHAR(200) 表示该 tag 的类型，也可以是其它合法的类型；name 是该 tag 的列名。\n\n更多填写规则请参考<a target="_blank" href="/docs/advanced/data-in/opcda/">企业版文档</a>。  \n',
          field: 'csv_config_file',
          type: 'dataset',
          accept: '.csv',
          templateUrl: 'template-zh.csv',
          required: true,
          requiredDependsOn: ['datasets/currentTab'],
          requiredDependsOnValues: {
            currentTab: ['csv_config_file']
          },
          multiple: true,
          editable: true,
          selectable: true,
          defaultValue: '',
          info2: true
        },
        {
          label: '选择数据点位',
          name: 'select_all_points',
          labelShow: false,
          labelWidth: '0px',
          category: 'select_all_points',
          radio: true,
          field: 'select_all_points',
          type: 'dataset',
          accept: '.csv',
          templateUrl: 'template-zh.csv',
          placeholder: '设置过滤条件，选择 OPC 服务器上满足指定条件的数据点位。\n',
          required: true,
          multiple: true,
          editable: true,
          selectable: true,
          children: [
            {
              name: 'root',
              display: '根节点',
              hint: {
                type: 'str'
              },
              description: '从该节点开始查询所有子节点, 多级父节点间用“.”相连接。\n',
              placeholder: '例如 root.parent',
              label: '根节点',
              field: 'root',
              defaultValue: '',
              multiple: false,
              type: 'input'
            },
            {
              name: 'node_id_pattern',
              display: '节点 ID',
              if: '!pattern',
              hint: {
                type: 'str'
              },
              description: '数据点位 id 需要满足设置的正则表达式。\n',
              label: '节点 ID',
              field: 'node_id_pattern',
              defaultValue: '',
              multiple: false,
              type: 'input'
            },
            {
              name: 'browse_name_pattern',
              display: '节点名称',
              hint: {
                type: 'str'
              },
              description: '数据点位 TagName 需要满足设置的正则表达式。\n',
              label: '节点名称',
              field: 'browse_name_pattern',
              defaultValue: '',
              multiple: false,
              type: 'pattern'
            },
            {
              name: 'super_table_expression',
              display: '超级表名称',
              hint: {
                type: 'str'
              },
              description: '支持 `<super table prefix>_{type}` 格式，`{type}` 表示点位的数据类型。\n',
              required: true,
              value: 'opc_{type}',
              label: '超级表名称',
              field: 'super_table_expression',
              defaultValue: 'opc_{type}',
              multiple: false,
              type: 'input'
            },
            {
              name: 'child_table_expression',
              display: '表名称',
              hint: {
                type: 'str'
              },
              description: '支持 `<child table prefix>_{tag_name}` 格式，`{tag_name}` 表示点位名称。\n',
              required: true,
              value: 't_{tag_name}',
              label: '表名称',
              field: 'child_table_expression',
              defaultValue: 't_{tag_name}',
              multiple: false,
              type: 'input'
            },
            {
              name: 'table_primary_key',
              display: '主键列',
              hint: {
                type: 'str',
                choices: ['original_ts', 'request_ts', 'received_ts']
              },
              description:
                '目标数据表主键将使用选择的值作为时间戳主键列，original_ts 表示使用数据点位上报 opc server 时间，request_ts 是 observe 采集模式下每次轮询的发起请求时间，received_ts 表示从 opc server 接收到数据的时间。\n',
              required: false,
              value: 'original_ts',
              label: '主键列',
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
              display: '主键别名',
              hint: {
                type: 'str'
              },
              description: '在目标数据表中的主键列名称。\n',
              required: false,
              value: 'ts',
              label: '主键别名',
              field: 'table_primary_key_alias',
              defaultValue: 'ts',
              multiple: false,
              type: 'input'
            }
          ],
          defaultValue: ''
        }
      ]
    },
    {
      label: 'Groups-after',
      field: 'groups_after',
      hide: true,
      children: [
        {
          label: '连接配置',
          field: '050884b0-d79b-4089-98e8-2e875b0ce968',
          description: 'OPC 连接相关配置',
          children: [
            {
              label: '连接超时',
              description: 'DA 连接超时间隔，单位为：秒 (s)。',
              field: 'connect_timeout',
              placeholder: '10',
              defaultValue: '10',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1
            },
            {
              label: '采集超时',
              description: 'DA 数据采集超时间隔，单位为：秒 (s)。',
              field: 'request_timeout',
              placeholder: '10',
              defaultValue: '10',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1
            }
          ],
          hide: false
        },
        {
          label: '采集配置',
          field: '39088942-8a97-43e0-a94c-6885a806a79f',
          description: '数据采集相关配置项。',
          children: [
            {
              label: '上报异常值',
              description: '是否上报异常值（Bad Quality）的数据，默认上报异常值数据。',
              field: 'contains_bad',
              type: 'switch',
              defaultValue: true
            },
            {
              label: '采集间隔',
              description: '数据点位采集间隔，单位为：秒。',
              field: 'interval',
              placeholder: '',
              defaultValue: '1',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1
            },
            {
              label: '点位更新模式',
              description:
                '点位更新模式，在使用“选择数据点位”时，可以开启动态点位更新。none：不开启动态点位更新；append：开启动态点位更新，但只追加；update：开启动态点位更新，追加或删除。\n',
              field: 'update_mode',
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
                  label: 'append',
                  value: 'append'
                },
                {
                  label: 'update',
                  value: 'update'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: '点位更新间隔',
              description: '动态点位更新间隔，在“点位更新模式”为 append 和 update 时生效，以秒为单位。\n',
              field: 'update_interval',
              placeholder: '',
              defaultValue: '600',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 60,
              max: 2147483647
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
          label: '最大写入并发数',
          field: 'write_concurrency',
          description: '写入 taosX 的最大并发数限制，当默认参数性能不足时，可增大此参数。\n',
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
          description: '单次读取最大延时（单位为秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n',
          defaultValue: '1',
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

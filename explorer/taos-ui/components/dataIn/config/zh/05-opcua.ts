export default {
  name: 'OPC-UA',
  id: 'opcua',
  type: 'uri',
  description:
    'OPC 是工业自动化领域和其他行业中安全可靠地交换数据的互操作标准之一。\n\nOPC UA 是经典 OPC 规范的下一代标准，是一个平台无关的面向服务的架构规范，集成了现有 OPC Classic 规范的所有功能，提供了一条迁移到更安全和可扩展解决方案的路径。\n\n如果想了解更多关于 OPC UA 的信息，可以阅读 OPC Foundation 网站和一些有用的博客，例如：\n1. [What is OPC](https://opcfoundation.org/about/what-is-opc/)\n2. [What is OPC UA](https://opcfoundation.org/about/opc-technologies/opc-ua/)\n\ntaosX 使用 OPC 连接器从 OPC 服务器拉取或订阅数据。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: '服务地址',
          description:
            'OPC UA 服务器端点，如：`127.0.0.1:6666/OPCUA/ServerPath`。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。\n',
          field: 'endpoint',
          required: true,
          placeholder: '127.0.0.1:6666/OPCUA/ServerPath',
          pattern: null,
          defaultValue: '',
          type: 'input'
        },
        {
          label: '安全模式',
          description:
            'Security mode（安全模式）是 OPC UA 协议中用于保护通信安全的一种机制。安全模式定义了如何加密和验证通信数据，以防止未经授权的访问和篡改。\n',
          field: 'security_mode',
          pattern: null,
          defaultValue: '',
          type: 'select',
          options: [
            {
              label: 'None',
              value: 'None'
            },
            {
              label: 'Sign',
              value: 'Sign'
            },
            {
              label: 'SignAndEncrypt',
              value: 'SignAndEncrypt'
            }
          ]
        },
        {
          label: '安全策略',
          description:
            'Security Policy（安全策略）是 OPC UA 协议中用于定义安全机制的一种机制。安全策略定义了如何实现安全模式中的加密和验证机制，包括使用的加密算法、密钥长度、数字证书等。\n',
          field: 'security_policy',
          pattern: null,
          defaultValue: '',
          type: 'select',
          options: [
            {
              label: 'None',
              value: 'None'
            },
            {
              label: 'Basic128Rsa15',
              value: 'Basic128Rsa15'
            },
            {
              label: 'Basic256',
              value: 'Basic256'
            },
            {
              label: 'Basic256Sha256',
              value: 'Basic256Sha256'
            },
            {
              label: 'Aes128_Sha256_RsaOaep',
              value: 'Aes128_Sha256_RsaOaep'
            },
            {
              label: 'Aes256_Sha256_RsaPss',
              value: 'Aes256_Sha256_RsaPss'
            }
          ],
          requiredDependsOn: ['connection_options/security_mode'],
          requiredDependsOnValues: {
            security_mode: ['Sign', 'SignAndEncrypt']
          },
          disabledDependsOn: ['connection_options/security_mode'],
          disabledDependsOnValues: {
            security_mode: ['None']
          },
          emptyDependsOn: ['connection_options/security_mode'],
          emptyDependsOnValues: {
            security_mode: ['None']
          }
        },
        {
          label: '安全通信证书',
          description:
            '建立连接时，发送给 OPC UA 服务器；如果未经 CA 认证，请在服务器端信任此证书后，再次发起连通性检查。',
          field: 'certificate',
          pattern: null,
          defaultValue: '',
          type: 'file',
          requiredDependsOn: ['connection_options/security_mode'],
          requiredDependsOnValues: {
            security_mode: ['Sign', 'SignAndEncrypt']
          }
        },
        {
          label: '安全通信私钥',
          description: '私钥文件，对服务器发送的消息做签名检查或者解密。',
          field: 'private_key',
          pattern: null,
          defaultValue: '',
          type: 'file',
          requiredDependsOn: ['connection_options/security_mode'],
          requiredDependsOnValues: {
            security_mode: ['Sign', 'SignAndEncrypt']
          }
        },
        {
          label: '连接超时',
          description: '连接超时间隔，单位为：秒 (s)。',
          field: 'connect_timeout',
          placeholder: '10',
          type: 'number',
          min: 1,
          defaultValue: 10
        },
        {
          label: '请求超时',
          description: '请求的超时间隔，单位为：秒 (s)。',
          field: 'request_timeout',
          placeholder: '10',
          type: 'number',
          min: 1,
          defaultValue: 10
        }
      ]
    },
    {
      label: '认证',
      description: 'OPC UA 可选择使用多种认证方式。',
      field: 'authentication',
      type: 'tabs',
      valueField: 'currentTab',
      defaultValue: 'anonymous',
      multiple: false,
      children: [
        {
          label: '匿名访问',
          name: 'anonymous',
          field: 'anonymous',
          children: []
        },
        {
          label: '用户名',
          name: 'plain',
          field: 'plain',
          children: [
            {
              label: '用户名',
              description: 'OPC UA 服务登录用户名。',
              required: true,
              field: 'username',
              defaultValue: '',
              type: 'input'
            },
            {
              label: '密码',
              description: 'OPC UA 服务登录密码.',
              required: true,
              field: 'password',
              defaultValue: '',
              type: 'password'
            }
          ]
        },
        {
          label: '证书访问',
          name: 'certificates',
          field: 'certificates',
          children: [
            {
              label: '认证证书文件',
              required: true,
              field: 'auth_certificate',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'file'
            },
            {
              label: '认证证书私钥',
              required: true,
              field: 'auth_private_key',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'file'
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
          description:
            'OPC 数据写入使用 csv 文件定义每一个数据点位到 TDengine 数据子表的映射规则：\n\n(1) point_id：必填，数据点位在 OPC UA 服务器上的 id；\n\n(2) stable：必填，数据点位对应的 TDengine 超级表；\n\n(3) tbname：必填，数据点位对应的 TDengine 子表；\n\n(4) enable：可选，默认值 \'1\'，指定是否采集该点位数据。0-不采集并且删除对应子表，1-采集点位数据，没有子表时创建子表；\n\n(5) value_col：可选，默认值 \'val\'。数据点位采集值在 TDengine 中对应的列名；\n\n(6) value_transform：可选，数据点位采集值在 taosX 中执行的变换函数，目前仅支持数值计算表达式，详见 transform 文档的 expr 表达式说明；\n\n(7) type：可选，默认值取源数据类型。数据点位采集值的数据类型，可用于替换超级表名称中的占位符 {type}；\n\n(8) quality_col：可选，数据点位采集值质量在 TDengine 中对应的列名；\n\n(9) ts_col/request_ts_col/received_ts_col：必填，TDengine 时间戳主键定义：可只保留其中一列，保留的时间戳列将作为主键；也可填写多列，居前的时间戳列作为主键；其中 ts_col 使用数据点位上报 opc server 时间，request_ts_col 使用 observe 采集模式下每次轮询的发起请求时间，received_ts_col 使用从 opc server 接收到数据的时间；\n\n(10) xx_ts_transform：可选，时间戳变换函数，参考 transform 数值计算表达式 expr 的说明；\n\n(11) tag::VARCHAR(200)::name：可选/可配置多个tag列；数据点位在 TDengine 中对应的 Tag 列；其中 tag 为保留关键字，表示该列为一个 tag 列；VARCHAR(200) 表示该 tag 的类型，也可以是其它合法的类型；name 是该 tag 的列名。\n\n更多填写规则请参考<a target="_blank" href="/docs/advanced/data-in/opcua/">企业版文档</a>。\n',
          field: 'csv_config_file',
          type: 'dataset',
          accept: '.csv',
          templateUrl: 'template-zh.csv',
          placeholder: '上传 CSV 配置文件，定义数据点位到 TDengine 数据子表的映射规则。\n',
          required: true,
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
          field: 'select_all_points',
          type: 'dataset',
          accept: '.csv',
          templateUrl: 'template-zh.csv',
          placeholder: '设置过滤条件，选择 OPC UA 服务器上满足指定条件的数据点位。\n',
          required: true,
          multiple: true,
          editable: true,
          selectable: true,
          children: [
            {
              name: 'root',
              display: '根节点 ID',
              hint: {
                type: 'str'
              },
              description: '从该节点开始遍历所有子节点。\n',
              placeholder: '例如 ns=3;i=1001',
              label: '根节点 ID',
              field: 'root',
              defaultValue: '',
              multiple: false,
              type: 'input'
            },
            {
              name: 'namespaces',
              display: '命名空间',
              description: '支持多选,只查询这些 namespace 下的数据点位。\n',
              multiple: true,
              placeholder: '连通性检查通过后，可选择，支持多选',
              label: '命名空间',
              field: 'namespaces',
              type: 'namespace'
            },
            {
              name: 'node_id_pattern',
              display: '节点 ID',
              // "if": "!pattern",
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
              // "if": "!pattern",
              hint: {
                type: 'str'
              },
              description: '数据点位名称需要满足设置的正则表达式。\n',
              label: '节点名称',
              field: 'browse_name_pattern',
              defaultValue: '',
              multiple: false,
              type: 'pattern'
            },
            // {
            //   "name": "pattern",
            //   "display": "正则匹配",
            //   "if": "pattern",
            //   "hint": {
            //     "type": "str"
            //   },
            //   "description": "数据点位名称或 id 需要满足设置的正则表达式。\n",
            //   "label": "正则匹配",
            //   "field": "pattern",
            //   "defaultValue": "",
            //   "multiple": false,
            //   "type": "pattern"
            // },
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
              description:
                '支持 `<child table prefix>_{ns}_{id}` 格式，`{ns}` 表示点位的namespace，`{id}` 为点位的 id。比如：点位的 point_id 为`ns=3;i=1001`，那么`{ns}`为3，`{id}`为1001。',
              required: true,
              value: 't_{ns}_{id}',
              label: '表名称',
              field: 'child_table_expression',
              defaultValue: 't_{ns}_{id}',
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
          label: '采集配置',
          field: 'collect_options',
          description: '数据采集相关配置项。',
          children: [
            {
              label: '采集模式',
              description:
                '`observe` 模式（读取点位最新值上报）或 `subscribe`（订阅模式，变更时上报）。默认为 `subscribe`。',
              field: 'collect_mode',
              placeholder: 'subscribe',
              defaultValue: 'subscribe',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'observe',
                  value: 'observe'
                },
                {
                  label: 'subscribe',
                  value: 'subscribe'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: '采集间隔',
              description: '数据点位采集间隔，单位为：秒。',
              field: 'interval',
              placeholder: '',
              defaultValue: 10,
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1,
              displayDependsOn: [
                // 'datasets/currentTab',
                'groups_after/collect_options/collect_mode'
              ], // 代表层级
              displayDependsOnValues: {
                // 'currentTab': ['select_all_points'],
                collect_mode: ['observe']
              }
            },
            {
              label: '采集超时',
              description: '数据采集请求超时间隔，单位为：秒 (s)。',
              field: 'request_timeout',
              placeholder: '10',
              defaultValue: 10,
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1,
              displayDependsOn: [
                // 'datasets/currentTab',
                'groups_after/collect_options/collect_mode'
              ], // 代表层级
              displayDependsOnValues: {
                // 'currentTab': ['select_all_points'],
                collect_mode: ['observe']
              }
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
              },
              displayDependsOn: ['datasets/currentTab'], // 代表层级
              displayDependsOnValues: {
                currentTab: ['select_all_points']
              }
            },
            {
              label: '点位更新间隔',
              description: '动态点位更新间隔，在“点位更新模式”为 append 和 update 时生效，以秒为单位。\n',
              field: 'update_interval',
              placeholder: '',
              defaultValue: 600,
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 60,
              max: 2147483647,
              displayDependsOn: ['datasets/currentTab'], // 代表层级
              displayDependsOnValues: {
                currentTab: ['select_all_points']
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
          defaultValue: 1000,
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
          defaultValue: 1,
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
        }
      ]
    }
  ]
};

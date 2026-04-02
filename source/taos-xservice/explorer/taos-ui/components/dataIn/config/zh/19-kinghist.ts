export default {
  name: 'KingHistorian',
  id: 'kinghist',
  type: 'uri',
  description: 'KingHistorian 是一个时序数据库。 taosX 封装了 KingHistorian 的 SDK，支持历史数据迁移和实时数据同步。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: 'Server 地址',
          description: 'KingHistorian Server 的 IP 地址或域名',
          field: 'host',
          required: true,
          placeholder: '127.0.0.1',
          pattern: null,
          defaultValue: '',
          display_order: 1,
          type: 'input'
        },
        {
          label: 'Server 端口',
          description: 'KingHistorian Server 的端口',
          field: 'port',
          placeholder: '5678',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: '端口号的范围是 0-65535',
          defaultValue: '',
          type: 'input'
        },
        {
          label: '连接超时',
          required: false,
          field: 'connect_timeout',
          defaultValue: '30',
          type: 'number'
        }
      ]
    },
    {
      label: '认证',
      description: '使用用户名和密码访问 KingHistorian',
      field: 'authentication',
      type: 'tabs',
      defaultValue: 'plain',
      valueField: 'currentTab',
      multiple: false,
      children: [
        {
          label: '用户名密码',
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
      label: 'Tag 配置',
      description: 'King Histotrian 的 Tag 和 TDengine 子表的映射规则。',
      field: 'tag_datasets',
      name: 'datasets',
      type: 'tabs',
      multiple: false,
      valueField: 'currentTab',
      defaultValue: 'csv_config_file',
      children: [
        {
          label: '上传 CSV 配置文件',
          name: 'csv_config_file',
          field: 'csv_config_file',
          labelShow: false,
          labelWidth: '0px',
          description:
            "使用 csv 文件定义每一个 Tag 到 TDengine 数据子表的映射规则：\n\n(1) tag_name：必填，数据点位在 KingHistorian 上的名称；\n\n(2) stable：必填，映射的 TDengine 超级表；\n\n(3) tbname：必填，映射的 TDengine 子表；\n\n(4) enable：可选，默认值 '1'，指定是否采集该点位数据。0-不采集并且删除对应子表，1-采集点位数据，没有子表时创建子表；\n\n(5) value_col：可选，默认值 'val'。采集值在 TDengine 中对应的列名；\n\n(6) value_transform：可选，采集值在 taosX 中执行的变换函数，目前仅支持数值计算表达式，详见 transform 文档的 expr 表达式说明；\n\n(7) type：可选，默认值取源数据类型。采集值的数据类型，可用于替换超级表名称中的占位符 {type}；\n\n(8) quality_col：可选，采集值质量在 TDengine 中对应的列名；\n\n(9) ts_col/request_ts_col/received_ts_col：必填，TDengine 时间戳主键定义；可只保留其中一列作为主键，也可填写多列，居前的时间戳列作为主键；\n\n(10) ts_transform/request_ts_transform/received_ts_transform：可选，时间戳变换表达式，支持`+ - * /`操作和括号；\n\n(11) tag::VARCHAR(200)::name：可选/可配置多个 tag 列；表示在 TDengine 中对应的 Tag 列；其中 tag 为保留关键字，VARCHAR(200) 表示该 tag 的类型；name 是该 tag 的列名。\n",
          category: 'csv_config_file',
          radio: false,
          type: 'dataset',
          accept: '.csv',
          templateUrl: 'template-zh.csv',
          required: true,
          requiredDependsOn: ['tag_datasets/currentTab'],
          requiredDependsOnValues: {
            currentTab: ['csv_config_file']
          },
          multiple: true,
          editable: true,
          selectable: true,
          defaultValue: '',
          info2: true
        }
      ]
    },
    {
      label: '采集配置',
      description: '数据采集相关配置项。',
      field: 'datasets',
      type: 'tabs',
      multiple: false,
      name: 'datasets',
      valueField: 'currentTab',
      defaultValue: 'history',
      children: [
        {
          label: '历史数据迁移',
          name: 'history',
          field: 'history',
          hide: false,
          children: [
            {
              label: '开始时间',
              field: 'start',
              description: '历史数据迁移的起始时间。',
              required: true,
              display_order: 1,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: '结束时间',
              field: 'end',
              description: '历史数据迁移的结束时间，默认当前时间。',
              required: false,
              display_order: 2,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: '查询窗口',
              field: 'step',
              description: '每次查询的时间窗口，默认 1 天。',
              required: false,
              display_order: 3,
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
                },
                {
                  value: 'm',
                  label: '分钟'
                },
                {
                  value: 's',
                  label: '秒'
                }
              ]
            },
            {
              label: '乱序',
              field: 'excursion',
              description: '容忍乱序的时长，默认 0 秒。',
              defaultValue: '0s',
              required: false,
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
                },
                {
                  value: 's',
                  label: '秒'
                }
              ]
            },
            {
              label: '查询间隔',
              field: 'interval',
              description: '每次查询的间隔时间，单位是秒，默认 10 秒。',
              defaultValue: '10',
              required: false,
              type: 'number'
            }
          ]
        },
        {
          label: '实时数据同步',
          name: 'realtime',
          field: 'realtime',
          hide: false,
          children: [
            {
              label: '最小间隔时间',
              description: '订阅的最小间隔时间，单位是毫秒。默认 1000 毫秒。',
              field: 'min_elapsed',
              defaultValue: '1000',
              required: false,
              type: 'number'
            }
          ]
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
          label: '并发数',
          field: 'concurrency',
          description: '最大并发数限制，当默认参数性能不足时，可增大此参数。\n',
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

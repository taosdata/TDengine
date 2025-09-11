export default {
  name: 'PI',
  id: 'pi',
  type: 'uri',
  description:
    'PI 系统是一套用于数据收集、查找、分析、传递和可视化的软件产品，可以作为管理实时数据和事件的企业级系统的基础架构。\n\nPI 系统这个术语通常用来指代PI服务器，但这两者并不相同。PI系统指的是所有 OSIsoft 软件产品，而 PI 服务器是 PI 系统的核心产品。数据可以自动从许多来源（控制系统、实验室设备、计算、手动输入或定制软件）收集。\n\ntaosX 可以通过 PI 连接器插件从 PI 系统中提取实时数据。\n',
  config: [
    {
      label: '连接配置',
      field: 'connection_options',
      children: [
        {
          label: 'PI 系统配置',
          field: 'system_configuration',
          required: true,
          defaultValue: 'PI Data Archive and Asset Framework (AF) Server',
          display_order: 0,
          type: 'select',
          options: [
            {
              label: 'PI Data Archive and Asset Framework (AF) Server',
              value: 'PI Data Archive and Asset Framework (AF) Server'
            },
            {
              label: 'PI Data Archive Only',
              value: 'PI Data Archive Only'
            }
          ]
        },
        {
          label: 'AF Server 名称',
          field: 'PISystemName',
          description: 'PI 系统(AF Server) 名称 (hostname).',
          required: true,
          placeholder: 'pi-af-server-name',
          display_order: 1,
          type: 'input',
          displayDependsOn: ['connection_options/system_configuration'],
          displayDependsOnValues: {
            system_configuration: ['PI Data Archive and Asset Framework (AF) Server']
          }
        },
        {
          label: 'PI服务名',
          description:
            'PI 服务器地址（通常使用主机名）。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。',
          field: 'host',
          required: true,
          placeholder: 'server',
          pattern: null,
          defaultValue: '',
          display_order: 1,
          type: 'input'
        },
        {
          label: 'AF Database Name',
          description: 'AF 数据库名',
          field: 'subject',
          required: true,
          placeholder: '如: Met1',
          pattern: null,
          defaultValue: '',
          type: 'input',
          displayDependsOn: ['connection_options/system_configuration'],
          displayDependsOnValues: {
            system_configuration: ['PI Data Archive and Asset Framework (AF) Server']
          }
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
      label: '数据模型配置',
      description:
        '使用默认配置，或者下载并修改后上传。配置入库的点位或者元素，入库的数据模型、数据过滤条件和变换规则。',
      field: 'datasets',
      type: 'tabs',
      multiple: false,
      name: 'datasets',
      valueField: 'only-choose-one$',
      defaultValue: 'single-column',
      children: [
        {
          label: '单列模式',
          name: 'single-column',
          short_description: '单列模式基于点位所属 UOM 建立超级表，每一个点位建立一个子表。',
          children: [
            {
              name: 'filter_value',
              display: '数据集过滤',
              placeholder: '通配符*匹配0或者多个字符，通配符?精确匹配一个字符',
              options: {
                'PI Data Archive Only': [
                  {
                    label: 'point',
                    value: 'point'
                  }
                ],
                'PI Data Archive and Asset Framework (AF) Server': [
                  {
                    value: 'template',
                    label: 'template'
                  }
                ]
              },
              action: 'Download',
              action_text: '下载默认配置',
              description:
                '可指定过滤条件，下载默认模板<br> - point: 使用点位名称过滤<br> - element: 使用AF element 名称过滤<br> - template: 使用AF template 名称过滤<br> 过滤条件可以使用通配符*匹配0或者多个字符，使用通配符?精确匹配一个字符',
              label: '数据集过滤',
              field: 'filter_value',
              defaultValue: '',
              type: 'compose',
              unit_value: 'template',
              optionsDependsOn: 'connection_options/system_configuration'
            },
            {
              name: 'transform_config_file',
              display: '点位配置文件',
              btnText: '上传配置文件',
              required: true,
              hint: {
                type: 'file'
              },
              description: '上传单列模式点位列表文件，文件格式为 CSV。',
              label: '点位配置文件',
              field: 'transform_config_file',
              defaultValue: '',
              multiple: false,
              type: 'file',
              accept: '.csv'
            }
          ],
        },
        {
          label: '多列模式',
          name: 'multi-column',
          short_description: '多列模式基于 AF Template 建立超级表，每一个 AF element建立一个子表。',
          selectable: false,
          children: [
            {
              name: 'filter_value',
              display: '数据集过滤',
              placeholder: '通配符*匹配0或者多个字符，通配符?精确匹配一个字符',
              options: {
                'PI Data Archive Only': [
                  {
                    label: 'point',
                    value: 'point'
                  }
                ],
                'PI Data Archive and Asset Framework (AF) Server': [
                  {
                    value: 'template',
                    label: 'template'
                  }
                ]
              },
              action: 'Download',
              action_text: '下载默认配置',
              description:
                '可指定过滤条件，下载默认模板<br> - point: 使用点位名称过滤<br> - element: 使用AF element 名称过滤<br> - template: 使用AF template 名称过滤<br> 过滤条件可以使用通配符*匹配0或者多个字符，使用通配符?精确匹配一个字符',
              label: '数据集过滤',
              field: 'filter_value',
              defaultValue: '',
              multiple: false,
              type: 'compose',
              unit_value: 'template',
              optionsDependsOn: 'connection_options/system_configuration'
            },
            {
              name: 'transform_config_file',
              display: '模型配置文件',
              required: true,
              btnText: '上传配置文件',
              hint: {
                type: 'file'
              },
              description: '上传单列模式点位列表文件，文件格式为 CSV。',
              label: '模型配置文件',
              field: 'transform_config_file',
              defaultValue: '',
              type: 'file',
              accept: '.csv'
            }
          ],
          disabledDependsOn: ['connection_options/system_configuration'],
          disabledDependsOnValues: {
            system_configuration: ['PI Data Archive Only']
          }
        }
      ]
    },
    {
      label: 'Groups-after',
      field: 'groups_after',
      hide: true,
      children: [
        {
          label: '自动回填',
          field: 'ddcef949-d140-40ad-b310-81d33b8510a8',
          description: '自动回填配置。',
          children: [
            {
              label: '重启补偿时间',
              description: '连接丢失或首次启动时自动回填的最长时间：`2d`、`3h`、`4m` 等。',
              field: 'MaxBackfillRangeDays',
              placeholder: '输入范围为[0,600]整数',
              defaultValue: '0m',
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
                },
                {
                  value: 's',
                  label: '秒'
                }
              ],
              min: 0,
              max: 600
            }
          ],
          hide: false
        }
      ]
    },
    {
      label: '高级选项',
      field: 'advanced_options',
      description: '对数据源性能、日志等其他参数进行调整，可修改以下选项',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
        {
          label: '同步新增的元素',
          field: 'sync_add_element',
          description: '监听配置的模板下新增的元素，无需重启任务，即可自动同步新增元素',
          defaultValue: true,
          required: false,
          type: 'switch'
        },
        {
          label: '同步静态属性的变化',
          field: 'sync_update_attribute',
          description: '同步所有静态属性（非 PI Point 属性）的变化',
          defaultValue: true,
          required: false,
          type: 'switch'
        },
        {
          label: '同步删除元素的操作',
          field: 'sync_delete_element',
          description: '监听配置的模板下删除元素的事件，并同步删除 TDengine 对应子表',
          defaultValue: true,
          required: false,
          type: 'switch'
        },
        {
          label: '同步删除 PI Point 历史数据',
          field: 'sync_delete_data',
          description:
            '对于某个元素的动态属性，如果在 PI 中某个时间的数据被删除了，TDengine 对应时间对应列的数据会被置空',
          defaultValue: true,
          required: false,
          type: 'switch'
        },
        {
          label: '同步修改 PI Point 历史数据',
          field: 'sync_update_data',
          description: '对于某个元素的动态属性，如果在 PI 中历史数据被修改了，TDengine 对应时间的数据也会更新',
          defaultValue: true,
          required: false,
          type: 'switch'
        },
        {
          label: '日志级别',
          field: 'log_level',
          description: '根据需要调整数据源的日志级别，此参数不总是生效。',
          defaultValue: 'info',
          required: false,
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
          description: '单次读取最大延时（单位为秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n',
          defaultValue: '1',
          required: false,
          type: 'number',
          min: 1,
          max: 60
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

import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
  name: 'CSV',
  id: 'csv',
  type: 'path',
  description: '导入一个或多个 CSV 文件数据到 TDengine。\n',
  strict: true,
  config: [
    {
      label: 'Groups-after',
      field: 'groups_after',
      hide: true,
      children: [
        {
          label: 'CSV 选项',
          field: '0d14aa37-292f-4d91-89a5-7f9f90bfe72a',
          description: 'CSV 读取选项',
          children: [
            {
              label: '包含表头',
              description: '如果包含表头，则第一行将被视为列信息。\n',
              field: 'has_header',
              placeholder: '',
              defaultValue: false,
              pattern: null,
              grid_two: false,
              type: 'switch'
            },
            {
              label: '忽略前 N 行',
              description: '忽略 CSV 文件的前 N 行。',
              field: 'skip',
              placeholder: '',
              defaultValue: 0,
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 0
              // "max": null
            },
            {
              label: '字段分隔符',
              description: 'CSV 字段之间的分隔符。',
              field: 'delimiter',
              placeholder: '',
              defaultValue: ',',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: ',',
                  value: ','
                },
                {
                  label: ';',
                  value: ';'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: '字段引用符',
              description: '当 CSV 字段中包含分隔符或换行符时，用于包围字段内容，以确保整个字段被正确识别。',
              field: 'quote',
              placeholder: '',
              defaultValue: '"',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: '"',
                  value: '"'
                },
                {
                  label: "'",
                  value: "'"
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: '注释前缀符',
              description: '当 CSV 文件中某行以此处指定的字符开头，则忽略该行。',
              field: 'comment',
              placeholder: '',
              defaultValue: '#',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: '#',
                  value: '#'
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
      label: 'Payload 转换',
      field: 'csvData',
      type: 'csvData',
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
  ]
};

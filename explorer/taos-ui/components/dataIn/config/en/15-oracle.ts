import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
  name: 'Oracle',
  id: 'oracle',
  type: 'uri',
  description:
    "Oracle is the world's most popular relational database management system. It has good portability, ease of use, and powerful functions, and is suitable for various large, medium, and small microcomputer environments. It is an efficient, reliable, and high-throughput database solution.\n\nTDengine can efficiently read the data in Oracle and write it to TDengine through the Oracle connector to achieve historical data migration or real-time data synchronization.\n",
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Host',
          description:
            'The access address of Oracle.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.',
          field: 'host',
          required: true,
          placeholder: '127.0.0.1',
          pattern: null,
          defaultValue: '',
          display_order: 1,
          type: 'input'
        },
        {
          label: 'Port',
          description: 'The port of Oracle.',
          field: 'port',
          required: true,
          placeholder: '1521',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: 'The port number ranges from 0 to 65535',
          defaultValue: '',
          type: 'input'
        },
        {
          label: 'Database',
          description: 'The name of the database to connect to.',
          field: 'subject',
          required: true,
          placeholder: 'for example: db1',
          pattern: null,
          defaultValue: '',
          type: 'input'
        },
        {
          label: 'minimum Connections',
          description: 'The minimum number of connections in the connection pool, default is 5.',
          field: 'min_connections',
          required: false,
          type: 'number',
          min: 1,
          max: 10000
        },
        {
          label: 'maximum Connections',
          description: 'The maximum number of connections in the connection pool, default is 20.',
          field: 'max_connections',
          required: false,
          type: 'number',
          min: 1,
          max: 10000
        },
        {
          label: 'Connection Timeout',
          description: 'The timeout for establishing a connection, default is 20 seconds.',
          field: 'connection_timeout',
          required: false,
          type: 'number',
          min: 1,
          max: 2000
        }
      ]
    },
    {
      label: 'Authentication',
      description: 'Authentication is the process of verifying the identity before granting access to Oracle.',
      field: 'authentication',
      type: 'tabs',
      valueField: 'a7dcf55a-a4ea-483b-8980-2db60cd2d8d6',
      defaultValue: 'plain',
      multiple: false,
      children: [
        {
          label: 'Username and Password',
          name: 'plain',
          field: 'plain',
          children: [
            {
              label: 'Username',
              required: true,
              field: 'username',
              defaultValue: '',
              type: 'input'
            },
            {
              label: 'Password',
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
      label: 'Groups-after',
      field: 'groups_after',
      hide: true,
      children: [
        {
          label: 'Data Collection',
          field: '9190e0a4-7a86-4d01-bb5b-540762d79460',
          description: 'Data collection related configuration items.',
          children: [
            {
              label: 'Subtable Fields',
              description: 'Fields and query statements used for splitting sub tables.',
              field: 'subtable_fields',
              placeholder: 'select distinct col_name1,col_name2 from table',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: 'SQL Template',
              description:
                "SQL statement used for querying. The SQL statement must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).\nSQL uses different placeholders to represent different time format requirements, specifically the following placeholder formats:\n1. `${start}`, `${end}`: Represents the RFC3339 format timestamp, such as: 2024-03-14T08:00:00+0800\n2. `${start_no_tz}`, `${end_no_tz}`: Represents the RFC3339 string without a time zone: 2024-03-14T08:00:00\n3. `${start_date}`, `${end_date}`: Represents only the date, but there's no pure date type in Oracle, so it will contain zero hours, zero minutes, and zero seconds, such as: 2024-03-14 00:00:00, Therefore, when using date<=`${end_date}`, it should be noted that it cannot contain the day of 2024-03-14\n\nIf you use subtable fields, you need to concatenate field placeholders `and ${col_name1} and ${col_name2}` in the statement.note that field placeholders are case sensitive and need to be consistent with the fields in the database. If you want to sort by a specific field (recommended in ascending time order), you need to concatenate `ORDER BY time` in the statement.\n\nExample:`SELECT * FROM table WHERE time >= ${start} AND time < ${end} and ${col_name1} and ${col_name2} ORDER BY time`",
              field: 'sql',
              required: true,
              placeholder: 'See the description for a complete example',
              pattern: null,
              grid_two: true,
              type: 'input'
            },
            {
              label: 'Start Time',
              description: 'Start time for migrating data.\n',
              field: 'start',
              required: true,
              placeholder: 'for example: 2023-01-01 00:00:00',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: 'End Time',
              description:
                'End time for migrating data, can be left blank. If set, the migration task will stop automatically after the task is executed to the end time; if left blank, the real-time data will be synchronized continuously, and the task will not stop automatically.\n',
              field: 'end',
              placeholder: 'for example: 2024-01-01 00:00:00',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: 'Query Interval',
              description:
                'The time interval for segmented queries. The default is 1 day. To avoid querying too much data, a data synchronization subtask will use the query interval to query data in time segments.\n',
              field: 'interval',
              placeholder: 'The value is an integer ranging [0,600]',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              defaultValue: '1d',
              type: 'composeAppend',
              options: [
                {
                  value: 'd',
                  label: 'Day'
                },
                {
                  value: 'h',
                  label: 'Hours'
                }
              ],
              min: 0,
              max: 600
            },
            {
              label: 'Delay',
              description:
                'In the real-time data synchronization scenario, to avoid the loss of delayed written data, each synchronization task will read data before the delay time.\n',
              field: 'delay',
              placeholder: 'The value is an integer ranging [0,60000]',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              defaultValue: '0s',
              type: 'composeAppend',
              options: [
                {
                  value: 'm',
                  label: 'Minute'
                },
                {
                  value: 's',
                  label: 'Second'
                }
              ],
              min: 0,
              max: 60000
            }
          ],
          hide: false
        }
      ]
    },
    {
      label: 'Data Mapping',
      description:
        'taosX could let users to specify the data model in the database, for example, the table name pattern and stable name pattern, field names as tags or field names as columns.\n',
      field: 'parser',
      type: 'parser',
      fields: [
        {
          name: 'DateTime',
          description: 'The timestamp of the returned value.',
          type: 'timestamp'
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
      label: 'Advanced Options',
      field: 'advanced_options',
      description:
        'Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave these options as default to use the recommended settings.\n',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
        {
          label: 'Read Concurrency',
          field: 'read_concurrency',
          description:
            'The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n',
          defaultValue: '0',
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
          label: 'Batch Size',
          field: 'batch_size',
          description:
            'The number of data points to be written in a single request. The default value is 10000. If the data source is slow to respond, you can reduce this value appropriately.\n',
          defaultValue: '10000',
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
          label: 'Health Check Duration',
          field: 'health_check_window_in_second',
          description:
            'Indicates the time duration for monitoring the task status. Typically in minutes, this duration applies uniformly to all health states.',
          placeholder: 'Enter an integer in the range [0, 60000]',
          required: false,
          hint: {
            type: 'duration',
            choices: [
              {
                value: 's',
                label: 'Seconds'
              }
            ],
            min: 0,
            max: 60000
          },
          defaultValue: '0s',
          type: 'composeAppend',
          options: [
            {
              value: 's',
              label: 'Seconds'
            }
          ],
          min: 0,
          max: 60000
        },
        {
          label: 'Busy State Threshold',
          field: 'busy_threshold',
          description:
            'Percentage indicating the ratio of the number of elements enqueued to the total queue length. Default is 100%.',
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
          label: 'Max Write Queue Length',
          field: 'max_queue_length',
          description: 'Indicates the maximum write queue length for a single IPC connection.',
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
          label: 'Write Error Threshold',
          field: 'max_errors_in_window',
          description:
            'Indicates the number of allowed write errors during the health check duration. Exceeding the threshold will trigger a Fatal alert.',
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
    },
    exceptionStrategy
  ],
  parser: {
    display: 'Data Mapping',
    required: true,
    description:
      'taosX could let users to specify the data model in the database, for example, the table name pattern and stable name pattern, field names as tags or field names as columns.\n',
    fields: [
      {
        name: 'DateTime',
        description: 'The timestamp of the returned value.',
        type: 'timestamp'
      }
    ]
  }
};

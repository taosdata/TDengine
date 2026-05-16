import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
  name: 'MongoDB',
  id: 'mongodb',
  type: 'uri',
  description:
    'MongoDB is a product between relational and non-relational databases, which is widely used in many fields such as content management systems, mobile applications, and the Internet of Things. \n\nTDengine efficiently reads data from MongoDB and writes it to TDengine for historical data migration or real-time data synchronization. \n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Host',
          description:
            'The access address of MongoDB. If using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.',
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
          description: 'The port of MongoDB',
          field: 'port',
          required: true,
          placeholder: '27017',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: 'The port number ranges from 0 to 65535',
          defaultValue: '',
          type: 'input'
        }
      ]
    },
    {
      label: 'Authentication',
      description: 'Authentication is the process of verifying the identity before granting access to MongoDB.',
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
              placeholder: 'Username',
              required: true,
              field: 'username',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'input'
            },
            {
              label: 'Password',
              placeholder: 'Password',
              required: true,
              field: 'password',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'password'
            },
            {
              label: 'Authenticate DB',
              description: 'The default database for storing user information in MongoDB is admin.\n',
              placeholder: 'Authenticate DB',
              required: false,
              field: 'source',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'input'
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
          label: 'Connection options',
          field: 'b2792b2a-b452-4266-b4ce-7efe2d33e615',
          description: 'Other connection options.',
          children: [
            {
              label: 'Application Name',
              description: 'Identifies a client.',
              field: 'app_name',
              placeholder: 'For example: TDengine',
              pattern: null,
              grid_two: false,
              type: 'input'
            }
          ],
          hide: false
        },
        {
          label: 'Enable SSL',
          field: 'ssl',
          description: 'Use self-signed certificate file and private key.',
          hide: false,
          type: 'switch',
          defaultValue: false,
          valueField: 'isEnable',
          hasValue: true,
          children: [
            {
              label: 'CA File',
              description: 'CA certificate file',
              field: 'ca_file_path',
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
              label: 'Cert File',
              description: '.cert file',
              field: 'cert_key_file_path',
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
      children: [
        {
          label: 'Data Collection',
          field: 'a621a3c7-9386-4837-9b3c-ec0de759ccc3',
          description: 'Data collection related configuration items.',
          children: [
            {
              label: 'Database',
              description:
                'Source database in MongoDB, can be dynamically configured using placeholders, available placeholder list: \n<ul><li>${Y} Full Gregorian year representation, zero-filled 4-digit integer </li><li>${y} Gregorian year divided by 100, Zero padding of two integers </li><li>${M} integer (1-12) month </li><li>${m} in integer (01-12) </li><li>${B} in English whole put together </li><li>${b} in English abbreviations (3 A letter) </li><li>${D} date Numbers (1-31) </li><li>${d} date Numbers (01-31) </li><li>${J} the first day of the year (1-366) </li><li>${j} the first day of the year (001 - 366) </li><li>${F} is equivalent to ${Y}-${m}-${d}</li></ul>\n',
              field: 'database',
              required: true,
              placeholder: 'database_${Y}',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: 'Collection',
              description:
                'Set in MongoDB, can be dynamically configured using placeholders, available placeholder list: \n<ul><li>${Y} Full Gregorian year representation, zero-filled 4-digit integer </li><li>${y} Gregorian year divided by 100, Zero padding of two integers </li><li>${M} integer (1-12) month </li><li>${m} in integer (01-12) </li><li>${B} in English whole put together </li><li>${b} in English abbreviations (3 A letter) </li><li>${D} date Numbers (1-31) </li><li>${d} date Numbers (01-31) </li><li>${J} the first day of the year (1-366) </li><li>${j} the first day of the year (001 - 366) </li><li>${F} is equivalent to ${Y}-${m}-${d}</li></ul>',
              field: 'collection',
              required: true,
              placeholder: 'collection_${md}',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: 'Subtable Fields',
              description: 'Fields and query statements used for splitting sub tables.',
              field: 'subtable_fields',
              placeholder: 'col_name1,col_name2,...',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: 'Query Template',
              description:
                'A query statement used to query data, in JSON format, must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).\nUse different placeholders to indicate different time format requirements, specifically the following placeholder formats:\n1. `${start_datetime}`、`${end_datetime}`:Filters corresponding to back-end datetime fields, for example:`{"ddate":{"$gte":${start_datetime},"$lt":${end_datetime}}}` will be converted to `{"ddate":{"$gte":{"$date":"2024-06-01T00:00:00+00:00"},"$lt":{"$date":"2024-07-01T00:00:00+00:00"}}}`\n2. `${start_timestamp}`、`${end_timestamp}`: indicates the filtering of back-end timestamp fields, for example:`{"ttime":{"$gte":${start_timestamp},"$lt":${end_timestamp}}}` will be converted to `{"ttime":{"$gte":{"$timestamp":{"t":123,"i":456}},"$lt":{"$timestamp":{"t":123,"i":456}}}}`\n\nIf you use subtable fields, you need to concatenate field placeholders in the statement.\n\nExample:`{"ddate":{"$gte":${start_datetime},"$lt":${end_datetime}},${col_name1},${col_name2}}`',
              field: 'sql',
              required: true,
              placeholder: '{"ddate":{"$gte":${start_datetime},"$lt":${end_datetime}},${col_name1},${col_name2}}',
              pattern: null,
              grid_two: true,
              type: 'input'
            },
            {
              label: 'Sort',
              description:
                'Sorting of query statements.\n\n\n1.`{"createtime":1}`:MongoDB query results are returned in `createtime` order.\n\n2.`{"createdate":1, "createtime":1}`:MongoDB query results are returned in `createdate` and `createtime` order.',
              field: 'sort',
              placeholder: '{"createtime":1}',
              pattern: null,
              grid_two: false,
              validator: 'checkJson',
              type: 'input'
            },
            {
              label: 'Start Time',
              description: 'Start time of data migration.\n',
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
                'The end time of data migration can be left blank. If this parameter is set, the migration task is automatically stopped when the end time expires. If left blank, real-time data is continuously synchronized and the task does not automatically stop.\n',
              field: 'end',
              placeholder: 'for example: 2024-01-01 00:00:00',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: 'Interval',
              description:
                'Interval for querying data in segments. The default value is 1 day. To avoid a large amount of query data, a data synchronization task queries data in time intervals.\n',
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
                  label: 'Days'
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
                'In the real-time data synchronization scenario, each synchronization task reads data before the delay to prevent data loss.\n',
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
      label: 'Payload Transformation',
      description: '',
      field: 'parser',
      type: 'parser',
      fields: [
        {
          name: 'value',
          description: 'Sample Message Body',
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
    display: 'Payload Transformation',
    required: true,
    description:
      'taosX could let users to specify the data model in the database, for example, the table name pattern and stable name pattern, field names as tags or field names as columns.\n',
    fields: [
      {
        name: 'value',
        description: 'Sample Message Body',
        type: 'varchar'
      }
    ]
  }
};

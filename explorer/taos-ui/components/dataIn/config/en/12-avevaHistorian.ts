import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
  name: 'AVEVA Historian',
  id: 'avevaHistorian',
  type: 'uri',
  description:
    'AVEVA Historian process database integrated with operations control enabling access to your process, alarm, and event history data. Wonderware Historian is now AVEVA Historian.\n\nTDengine efficiently reads data from the AVEVA Historian and writes it to TDengine for historical data migration or real-time data synchronization.\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Host',
          description: 'AVEVA Historian SQL Server IP address or host name',
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
          description: 'AVEVA Historian SQL Server port',
          field: 'port',
          placeholder: '1433',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: 'The port number ranges from 0 to 65535',
          defaultValue: '',
          type: 'input'
        }
      ]
    },
    {
      label: 'Authentication',
      description: 'Use username and password of AVEVA Historian SQL Server',
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
            },
            {
              label: 'Encryption Level',
              description: 'Set the encryption level for the connection',
              field: 'encryption',
              defaultValue: 'Off',
              type: 'select',
              options: [
                {
                  label: 'Off',
                  value: 'Off'
                },
                {
                  label: 'On',
                  value: 'On'
                },
                {
                  label: 'NotSupported',
                  value: 'NotSupported'
                },
                {
                  label: 'Required',
                  value: 'Required'
                }
              ]
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
          label: 'Collect',
          field: 'collect_options',
          description: 'Configure Data Collection Task',
          children: [
            {
              label: 'Collection Mode',
              description: 'Collection mode. The optional values are `synchronize` and `migrate`.\n',
              field: 'mode',
              required: true,
              placeholder: 'synchronize',
              defaultValue: 'synchronize',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'synchronize',
                  value: 'synchronize'
                },
                {
                  label: 'migrate',
                  value: 'migrate'
                }
              ]
            },
            {
              label: 'Table',
              description:
                'Retrieves database tables in historian, with historical data in Runtime.dbo.History and real-time data in Runtime.dbo.Live.\n',
              field: 'table',
              required: true,
              placeholder: 'Runtime.dbo.History',
              defaultValue: 'Runtime.dbo.History',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'Runtime.dbo.History',
                  value: 'Runtime.dbo.History'
                },
                {
                  label: 'Runtime.dbo.Live',
                  value: 'Runtime.dbo.Live'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              },
              displayDependsOn: ['groups_after/collect_options/mode'],
              displayDependsOnValues: {
                mode: ['synchronize']
              }
            },
            {
              label: 'Tags',
              description: 'tags to be migrated/synchronized. `*` indicates that all tags.\n',
              field: 'tags',
              placeholder: '*',
              defaultValue: '*',
              pattern: null,
              grid_two: false,
              type: 'input',
              displayDependsOn: ['groups_after/collect_options/table'],
              displayDependsOnValues: {
                table: ['Runtime.dbo.History', '']
              }
            },
            {
              label: 'Tag List Size',
              description:
                'When `table` is `Runtime.dbo.History` and TagName in `tags` exceeds the `tagListSize`, tags are divided according to each group of `tagListSize`. The `tagListSize` is used to partition TagName to improve query efficiency during data migration/synchronization.  The default value of `tagListSize` is 10.\n',
              field: 'tagListSize',
              placeholder: '10',
              defaultValue: '10',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1,
              max: 1000
            },
            {
              label: 'Begin Time',
              description: 'The start time of the task is in rfc3339 format.',
              field: 'beginDateTime',
              required: true,
              placeholder: 'e.g., 2023-01-01T00:00:00.000Z',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime',
              requiredConditions: 'some',
              requiredDependsOn: ['groups_after/collect_options/mode', 'groups_after/collect_options/table'],
              requiredDependsOnValues: {
                mode: ['migrate'],
                table: ['Runtime.dbo.History']
              },
              displayConditions: 'some',
              displayDependsOn: ['groups_after/collect_options/table'],
              displayDependsOnValues: {
                table: ['Runtime.dbo.History', '']
              }
            },
            {
              label: 'End Time',
              description: 'The end time of the task is in rfc3339 format.',
              field: 'endDateTime',
              placeholder: 'e.g., 2023-01-01T00:00:00.000Z',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime',
              displayDependsOn: ['groups_after/collect_options/mode'],
              displayDependsOnValues: {
                mode: ['migrate']
              },
              requiredDependsOn: ['groups_after/collect_options/mode'],
              requiredDependsOnValues: {
                mode: ['migrate']
              }
            },
            {
              label: 'Time Window',
              description: 'Time window for historical data migration.',
              field: 'timeWindow',
              placeholder: 'The value is an integer ranging [0,60000]',
              defaultValue: '1d',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              type: 'composeAppend',
              options: [
                {
                  value: 'y',
                  label: 'Year'
                },
                {
                  value: 'mo',
                  label: 'Month'
                },
                {
                  value: 'd',
                  label: 'Day'
                },
                {
                  value: 'w',
                  label: 'Week'
                },
                {
                  value: 'h',
                  label: 'Hours'
                },
                {
                  value: 'm',
                  label: 'Minute'
                },
                {
                  value: 's',
                  label: 'Second'
                },
                {
                  value: 'ms',
                  label: 'Millisecond'
                },
                {
                  value: 'u',
                  label: 'Microsecond'
                },
                {
                  value: 'ns',
                  label: 'Nanoseconds'
                }
              ],
              min: 0,
              max: 60000,
              displayConditions: 'some',
              displayDependsOn: ['groups_after/collect_options/table'],
              displayDependsOnValues: {
                table: ['Runtime.dbo.History', '']
              }
            },
            {
              label: 'Retrieve Interval',
              description: 'Pull interval for real-time data synchronization.',
              field: 'retrieveInterval',
              placeholder: 'The value is an integer ranging [0,60000]',
              defaultValue: '10s',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              type: 'composeAppend',
              options: [
                {
                  value: 'd',
                  label: 'Day'
                },
                {
                  value: 'h',
                  label: 'Hours'
                },
                {
                  value: 'm',
                  label: 'Mniute'
                },
                {
                  value: 's',
                  label: 'Second'
                },
                {
                  value: 'ms',
                  label: 'millisecond'
                }
              ],
              min: 0,
              max: 60000,
              displayDependsOn: ['groups_after/collect_options/mode'],
              displayDependsOnValues: {
                mode: ['synchronize']
              }
            },
            {
              label: 'Tolerance',
              description: 'The maximum time limit for tolerating out-of-order data delay.',
              field: 'tolerance',
              placeholder: 'The value is an integer ranging [0,60000]',
              defaultValue: '0ms',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              type: 'composeAppend',
              options: [
                {
                  value: 'd',
                  label: 'Day'
                },
                {
                  value: 'h',
                  label: 'Hours'
                },
                {
                  value: 'm',
                  label: 'Mniute'
                },
                {
                  value: 's',
                  label: 'Second'
                },
                {
                  value: 'ms',
                  label: 'millisecond'
                }
              ],
              min: 0,
              max: 60000,
              displayDependsOn: ['groups_after/collect_options/mode', 'groups_after/collect_options/table'],
              displayDependsOnValues: {
                mode: ['synchronize'],
                table: ['Runtime.dbo.History', '']
              }
            }
          ],
          hide: false
        }
      ]
    },
    {
      label: 'Payload Transformation',
      description:
        'taosX could let users to specify the data model in the database, for example, the table name pattern <br>\nand stable name pattern, field names as tags or field names as columns.\n',
      field: 'parser',
      type: 'parser',
      fields: [
        {
          name: 'DateTime',
          description: 'The timestamp of the returned value.',
          type: 'timestamp'
        },
        {
          name: 'TagName',
          description: 'The unique name of the tag.',
          type: 'varchar'
        },
        {
          name: 'Value',
          description: 'The value of the tag at the timestamp. The value is always NULL for string tags.',
          type: 'double'
        },
        {
          name: 'vValue',
          description: 'The value of the analog, discrete, or string tag stored as a sql_variant.',
          type: 'varchar'
        },
        {
          name: 'Quality',
          description: 'The basic data quality indicator associated with the data value.',
          type: 'int'
        },
        {
          name: 'QualityDetail',
          description: 'An internal representation of data quality.',
          type: 'int'
        },
        {
          name: 'OPCQuality',
          description: 'The quality value received from the data source.',
          type: 'int'
        },
        {
          name: 'wwTagKey',
          description: 'The unique numerical identifier of a tag.',
          type: 'int'
        },
        {
          name: 'wwResolution',
          description: 'The sampling rate, in milliseconds, for retrieving the data in cyclic mode.',
          type: 'int'
        },
        {
          name: 'StartDateTime',
          description: 'Start time of the retrieval cycle for which this row is returned.',
          type: 'timestamp'
        },
        {
          name: 'SourceTag',
          description: 'The name of the source tag for a replicated tag at the time this point was stored.',
          type: 'varchar'
        },
        {
          name: 'SourceServer',
          description: 'The name of the server for this replicated tag at the time this point was stored.',
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
        'Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n',
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
            'The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n',
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
          label: 'Keep Raw Data',
          field: 'keep_raw_data',
          description: 'Whether to keep the raw data. If enabled, the raw data will be stored.\n',
          defaultValue: false,
          required: false,
          hint: {
            type: 'bool'
          },
          type: 'switch'
        },
        {
          label: 'Max Keep Days',
          field: 'keep_raw_data_days',
          description: 'The number of days to keep the raw data. The default value is 1 day.\n',
          defaultValue: '1',
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 365
          },
          type: 'number',
          min: 1,
          max: 365
        },
        {
          label: 'Raw Data Directory',
          field: 'keep_raw_data_dir',
          description: 'The directory to store the raw data. The default value is `$DATA_DIR/tasks/:id/rawdata/`.\n',
          placeholder: '$DATA_DIR/tasks/:id/rawdata/',
          required: false,
          hint: {
            type: 'str'
          },
          type: 'input'
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
    editableSample: true,
    description:
      'taosX could let users to specify the data model in the database, for example, the table name pattern <br>\nand stable name pattern, field names as tags or field names as columns.\n',
    fields: [
      {
        name: 'DateTime',
        description: 'The timestamp of the returned value.',
        type: 'timestamp'
      },
      {
        name: 'TagName',
        description: 'The unique name of the tag.',
        type: 'varchar'
      },
      {
        name: 'Value',
        description: 'The value of the tag at the timestamp. The value is always NULL for string tags.',
        type: 'double'
      },
      {
        name: 'vValue',
        description: 'The value of the analog, discrete, or string tag stored as a sql_variant.',
        type: 'varchar'
      },
      {
        name: 'Quality',
        description: 'The basic data quality indicator associated with the data value.',
        type: 'int'
      },
      {
        name: 'QualityDetail',
        description: 'An internal representation of data quality.',
        type: 'int'
      },
      {
        name: 'OPCQuality',
        description: 'The quality value received from the data source.',
        type: 'int'
      },
      {
        name: 'wwTagKey',
        description: 'The unique numerical identifier of a tag.',
        type: 'int'
      },
      {
        name: 'wwResolution',
        description: 'The sampling rate, in milliseconds, for retrieving the data in cyclic mode.',
        type: 'int'
      },
      {
        name: 'StartDateTime',
        description: 'Start time of the retrieval cycle for which this row is returned.',
        type: 'timestamp'
      },
      {
        name: 'SourceTag',
        description: 'The name of the source tag for a replicated tag at the time this point was stored.',
        type: 'varchar'
      },
      {
        name: 'SourceServer',
        description: 'The name of the server for this replicated tag at the time this point was stored.',
        type: 'varchar'
      }
    ]
  }
};

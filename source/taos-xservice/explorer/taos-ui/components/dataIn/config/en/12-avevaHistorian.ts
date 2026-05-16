import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
  name: 'AVEVA Historian',
  id: 'avevaHistorian',
  type: 'uri',
  description:
    'AVEVA Historian is an industrial big data analysis software, formerly known as Wonderware. It can capture and store high-fidelity industrial big data, unlocking constrained potential to improve operations.\nTDengine can efficiently read data from AVEVA Historian and write it to TDengine for historical data migration or real-time data synchronization.\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Server Address',
          description: 'AVEVA Historian SQL Server IP address or domain name',
          field: 'host',
          required: true,
          placeholder: '127.0.0.1',
          pattern: null,
          defaultValue: '',
          display_order: 1,
          type: 'input'
        },
        {
          label: 'Server Port',
          description: 'AVEVA Historian SQL Server port',
          field: 'port',
          placeholder: '1433',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: 'The port number ranges from 0 to 65535',
          defaultValue: '',
          type: 'input'
        },
        {
          label: 'Connection Timeout (seconds)',
          description: 'Timeout for connecting to the Historian database, in seconds, default 120',
          field: 'connection_timeout',
          required: false,
          placeholder: '120',
          defaultValue: '120',
          type: 'number',
          min: 1
        },
        {
          label: 'Reconnect Attempts',
          description: 'Maximum number of retry attempts after the Historian database connection is lost, default 10',
          field: 'reconnect_times',
          required: false,
          placeholder: '10',
          defaultValue: '10',
          type: 'number',
          min: 1
        },
        {
          label: 'Reconnect Interval (seconds)',
          description: 'Retry interval after the Historian database connection is lost, in seconds, default 5',
          field: 'reconnect_interval',
          required: false,
          placeholder: '5',
          defaultValue: '5',
          type: 'number',
          min: 1
        }
      ]
    },
    {
      label: 'Authentication',
      field: 'authentication',
      children: [
        {
          label: 'Username',
          description: 'Username for accessing AVEVA Historian SQL Server',
          required: true,
          field: 'username',
          defaultValue: '',
          type: 'input'
        },
        {
          label: 'Password',
          description: 'Password for accessing AVEVA Historian SQL Server',
          required: true,
          field: 'password',
          defaultValue: '',
          type: 'password'
        },
        {
          label: 'Encryption Level',
          description: 'Set the encryption level for the connection',
          field: 'encryption',
          defaultValue: 'NotSupported',
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
      children: []
    },
    {
      label: 'Collection Configuration',
      field: 'collect_options',
      description: 'Data collection related configuration items.',
      children: [
        {
          label: 'Collection Mode',
          description: 'Collection mode. The optional values are `synchronize` and `migrate`.\n',
          field: 'mode',
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
          displayDependsOn: ['collect_options/mode'],
          displayDependsOnValues: {
            mode: ['synchronize']
          }
        },
        {
          label: 'Tags',
          description: 'Tags to be migrated/synchronized. `*` indicates all tags except those starting with Sys.\n',
          field: 'tags',
          placeholder: '*',
          defaultValue: '*',
          pattern: null,
          grid_two: false,
          type: 'input'
        },
        {
          label: 'Tag List Size',
          description:
            'When `table` is `Runtime.dbo.History` and TagName in `tags` exceeds the `tagListSize`, tags are divided into groups of `tagListSize`. Using `tagListSize` to partition TagName improves query efficiency during data migration/synchronization. The default value of `tagListSize` is 10.\n',
          field: 'tagListSize',
          placeholder: '10',
          defaultValue: '10',
          pattern: null,
          grid_two: false,
          type: 'number',
          min: 1,
          max: 1000,
          displayConditions: 'some',
          displayDependsOn: ['collect_options/table'],
          displayDependsOnValues: {
            table: ['Runtime.dbo.History', '']
          }
        },
        {
          label: 'Task Start Time',
          description: 'The start time of the task, in rfc3339 format.\n',
          field: 'beginDateTime',
          placeholder: 'e.g., 2023-01-01T00:00:00+08:00',
          pattern: null,
          grid_two: false,
          type: 'time',
          valueFormat: 'yyyy-MM-dd HH:mm:ss',
          dateType: 'datetime',
          requiredConditions: 'some',
          requiredDependsOn: ['collect_options/mode', 'collect_options/table'],
          requiredDependsOnValues: {
            mode: ['migrate'],
            table: ['Runtime.dbo.History']
          },
          displayConditions: 'some',
          displayDependsOn: ['collect_options/table'],
          displayDependsOnValues: {
            table: ['Runtime.dbo.History', '']
          }
        },
        {
          label: 'Task End Time',
          description: 'The end time of the task, in rfc3339 format.\n',
          field: 'endDateTime',
          placeholder: 'e.g., 2023-01-01T00:00:00+08:00',
          pattern: null,
          grid_two: false,
          type: 'time',
          valueFormat: 'yyyy-MM-dd HH:mm:ss',
          dateType: 'datetime',
          displayDependsOn: ['collect_options/mode'],
          displayDependsOnValues: {
            mode: ['migrate']
          },
          requiredDependsOn: ['collect_options/mode'],
          requiredDependsOnValues: {
            mode: ['migrate']
          }
        },
        {
          label: 'Query Time Window',
          description: 'Time window for each query during historical data migration.\n',
          field: 'timeWindow',
          placeholder: 'The value is an integer ranging [0,60000]',
          pattern: null,
          patternMsg: 'The value can only be a positive integer or 0',
          grid_two: false,
          defaultValue: '1d',
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
              label: 'Hour'
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
              label: 'Nanosecond'
            }
          ],
          min: 0,
          max: 60000,
          displayConditions: 'some',
          displayDependsOn: ['collect_options/table'],
          displayDependsOnValues: {
            table: ['Runtime.dbo.History', '']
          }
        },
        {
          label: 'Real-time Sync Interval',
          description: 'Query interval for real-time data synchronization.\n',
          field: 'retrieveInterval',
          placeholder: 'The value is an integer ranging [0,60000]',
          pattern: null,
          patternMsg: 'The value can only be a positive integer or 0',
          grid_two: false,
          defaultValue: '10s',
          type: 'composeAppend',
          options: [
            {
              value: 'd',
              label: 'Day'
            },
            {
              value: 'h',
              label: 'Hour'
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
            }
          ],
          min: 0,
          max: 60000,
          displayDependsOn: ['collect_options/mode'],
          displayDependsOnValues: {
            mode: ['synchronize']
          }
        },
        {
          label: 'Out-of-order Tolerance',
          description: 'The maximum time limit for tolerating out-of-order data arrival.\n',
          field: 'tolerance',
          placeholder: 'The value is an integer ranging [0,60000]',
          pattern: null,
          patternMsg: 'The value can only be a positive integer or 0',
          grid_two: false,
          defaultValue: '0ms',
          type: 'composeAppend',
          options: [
            {
              value: 'd',
              label: 'Day'
            },
            {
              value: 'h',
              label: 'Hour'
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
            }
          ],
          min: 0,
          max: 60000,
          displayDependsOn: ['collect_options/mode', 'collect_options/table'],
          displayDependsOnValues: {
            mode: ['synchronize'],
            table: ['Runtime.dbo.History', '']
          }
        }
      ],
      hide: false
    },
    {
      label: 'Payload Transformation',
      description: 'taosX allows users to specify the data model in the database, including: specifying table name and supertable name, setting normal columns and tag columns, etc.\n',
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
          description: 'The value as a string. Using this column in queries allows you to work with values of mixed data types.',
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
          description: 'The unique numerical identifier of a tag in a single AVEVA Historian.',
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
      description: 'Adjust data source performance, logging, and other parameters by modifying the following options.\n',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
        {
          label: 'Max Read Concurrency',
          field: 'read_concurrency',
          description: 'Data source connection count or read thread limit. Modify this parameter when the default is insufficient or resource usage needs adjustment.\n',
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
          description: 'Maximum number of messages or rows per batch send.\n',
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
          description: 'Whether to keep the raw data.\n',
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
          description: 'Maximum number of days to keep raw data, default is 1 day.\n',
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
          description: 'Custom directory for raw data storage, defaults to the system data directory.\n',
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
          description: 'Indicates the time duration for monitoring the task status. Typically in minutes, this duration applies uniformly to all health state modes.\n',
          defaultValue: '0s',
          placeholder: 'The value is an integer ranging [0,60000]',
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
          description: 'Percentage indicating the ratio of enqueued elements to the write queue length, default 100%.\n',
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
          label: 'Write Queue Length',
          field: 'max_queue_length',
          description: 'Maximum write queue length for a single IPC connection.',
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
          label: 'Write Error Threshold',
          field: 'max_errors_in_window',
          description: 'Number of allowed write errors during the health check duration. Exceeding the threshold will trigger a Fatal alert.',
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
  ],
  parser: {
    display: 'Payload Transformation',
    required: true,
    editableSample: true,
    description: 'taosX allows users to specify the data model in the database, including: specifying table name and supertable name, setting normal columns and tag columns, etc.\n',
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
        description: 'The value as a string. Using this column in queries allows you to work with values of mixed data types.',
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
        description: 'The unique numerical identifier of a tag in a single AVEVA Historian.',
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

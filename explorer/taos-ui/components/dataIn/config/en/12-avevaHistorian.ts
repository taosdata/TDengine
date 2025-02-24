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
              defaultValue: '1',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              unit_value: 'd',
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
              defaultValue: '10',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              unit_value: 's',
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
              defaultValue: '0',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              unit_value: 'ms',
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
          defaultValue: '',
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
          unit_value: 's',
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
          defaultValue: '100',
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
          unit_value: '%',
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
    {
      label: 'Exception handling strategy',
      field: 'write_config',
      description:
        'Adjust the configuration parameters for the write strategy. The following options can be modified.\n',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
        {
          label: 'Primary Timestamp Overflow',
          field: 'primary_timestamp_overflow',
          description:
            'Represents the operation when a timestamp overflow occurs. Options: Archive, Skip, Break. Default: Archive.\n',
          defaultValue: 'archive',
          required: false,
          hint: {
            type: 'select',
            choices: [
              {
                value: 'archive',
                label: 'Archive'
              },
              {
                value: 'skip',
                label: 'Skip'
              },
              {
                value: 'break',
                label: 'Break'
              }
            ]
          },
          disabledValues: [],
          type: 'select',
          options: [
            {
              value: 'archive',
              label: 'Archive'
            },
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'break',
              label: 'Break'
            }
          ]
        },
        {
          label: 'Primary Timestamp Null',
          field: 'primary_timestamp_null',
          description:
            'Represents the operation when a timestamp is null. Options: Use Current Time, Archive, Skip, Break. Default: Archive.\n',
          defaultValue: 'archive',
          required: false,
          disabledValues: [],
          type: 'select',
          options: [
            {
              value: 'archive',
              label: 'Archive'
            },
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'break',
              label: 'Break'
            },
            {
              value: 'use_current_time',
              label: 'Use Current Time'
            }
          ]
        },
        {
          field: 'primary_key_null',
          label: 'Primary Key Null',
          type: 'select',
          choices: [
            {
              value: 'archive',
              label: 'Archive'
            },
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'break',
              label: 'Break'
            }
          ],
          description:
            'Represents the operation when a composite primary key column is null. Options: Archive, Skip, Break. Default: Archive.\n',
          defaultValue: 'archive'
        },
        {
          label: 'Table Name Length Overflow',
          field: 'table_name_length_overflow',
          description:
            'Represents the operation when a table name length overflows. Currently supports Archive, Skip, Truncate, Truncate and Archive, and Break. Default: Archive.\n',
          defaultValue: 'archive',
          required: false,
          hint: {
            type: 'select',
            choices: [
              {
                value: 'archive',
                label: 'Archive'
              },
              {
                value: 'skip',
                label: 'Skip'
              },
              {
                value: 'break',
                label: 'Break'
              },
              {
                value: 'truncate',
                label: 'Truncate'
              },
              {
                value: 'truncate_and_archive',
                label: 'Truncate and Archive'
              }
            ]
          },
          disabledValues: [],
          type: 'select',
          options: [
            {
              value: 'archive',
              label: 'Archive'
            },
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'break',
              label: 'Break'
            },
            {
              value: 'truncate',
              label: 'Truncate'
            },
            {
              value: 'truncate_and_archive',
              label: 'Truncate and Archive'
            }
          ]
        },
        {
          label: 'Table Name Contains Illegal Char',
          field: 'table_name_contains_illegal_char',
          description:
            "Represents the strategy when a table name contains illegal characters (e.g., .). Options: Replace with a specified character or string, Skip, Archive, Break. Default: Replace with '_'.\n",
          defaultValue: '',
          required: false,
          hint: {
            type: 'compose',
            choices: [
              {
                value: 'archive',
                label: 'Archive'
              },
              {
                value: 'skip',
                label: 'Skip'
              },
              {
                value: 'break',
                label: 'Break'
              },
              {
                value: 'replace_to',
                label: 'Replace Illegal Character with Specified String'
              }
            ]
          },
          unit_value: 'replace_to',
          disabledValues: ['archive', 'skip', 'break'],
          type: 'compose',
          options: [
            {
              value: 'archive',
              label: 'Archive'
            },
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'break',
              label: 'Break'
            },
            {
              value: 'replace_to',
              label: 'Replace Illegal Character with Specified String'
            }
          ]
        },
        {
          label: 'Variable Not Exist in Table Name Template',
          field: 'variable_not_exist_in_table_name_template',
          description:
            'Represents the strategy when a variable in the table name template is empty. Options: Replace with a specified string, Leave blank, Skip the entire row. Default: Replace with NULL.\n',
          defaultValue: '',
          required: false,
          unit_value: 'replace_to',
          disabledValues: ['leave_blank', 'skip'],
          type: 'compose',
          options: [
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'leave_blank',
              label: 'Leave Blank'
            },
            {
              value: 'replace_to',
              label: 'Replace Variable with Specified String'
            }
          ]
        },
        {
          field: 'field_name_not_found',
          label: 'Field Name Not Found',
          type: 'select',
          options: [
            {
              value: 'archive',
              label: 'Archive'
            },
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'break',
              label: 'Break'
            },
            {
              value: 'add_field',
              label: 'Automatically Add Missing Field'
            }
          ],
          description:
            'Represents the action when a field name is not found. Options: Use current time, Archive, Skip, Break, Automatically add missing field. Default: Archive.',
          defaultValue: 'add_field'
        },
        {
          label: 'Field Name Length Overflow',
          field: 'field_name_length_overflow',
          description:
            'Represents the action when a field name length overflows. Options: Use current time, Archive, Skip, Break, Truncate, Truncate and Archive. Default: Archive.',
          defaultValue: 'archive',
          required: false,
          hint: {
            type: 'select',
            choices: [
              {
                value: 'archive',
                label: 'Archive'
              },
              {
                value: 'skip',
                label: 'Skip'
              },
              {
                value: 'break',
                label: 'Break'
              }
            ]
          },
          disabledValues: [],
          type: 'select',
          options: [
            {
              value: 'archive',
              label: 'Archive'
            },
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'break',
              label: 'Break'
            }
          ]
        },
        {
          name: 'field_length_extend',
          label: 'Field Length Extend',
          type: 'switch',
          defaultValue: true,
          description:
            'When enabled, VARCHAR/VARBINARY/NCHAR columns are automatically resized to the allowable length for storage. Default: true.',
          value: true
        },
        {
          field: 'field_length_overflow',
          label: 'Field Length Overflow',
          type: 'select',
          options: [
            {
              value: 'archive',
              label: 'Archive'
            },
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'break',
              label: 'Break'
            },
            {
              value: 'truncate',
              label: 'Truncate'
            },
            {
              value: 'truncate_and_archive',
              label: 'Truncate and Archive'
            }
          ],
          description:
            'Represents actions for column length overflow. Options: Archive, Skip, Break, Truncate, Truncate and Archive. Default: Archive.',
          defaultValue: 'archive'
        },
        {
          field: 'ingesting_error',
          label: 'Ingesting Error',
          type: 'select',
          options: [
            {
              value: 'archive',
              label: 'Archive'
            },
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'break',
              label: 'Break'
            }
          ],
          description:
            'Actions for data failure when data cannot be ingested into the database. Currently supports Archive, Skip, and Break. Default: Archive.',
          defaultValue: 'archive'
        },
        {
          field: 'connection_timeout_in_second',
          label: 'Connection Timeout',
          type: 'composeAppend',
          options: [
            {
              value: 's',
              label: 'Seconds'
            }
          ],
          min: 1,
          max: 600,
          description: 'Target database connection timeout, default is 30 seconds.',
          required: false,
          placeholder: 'Enter an integer between [1,600]',
          value: 30,
          unit_value: 's'
        },
        {
          field: 'cache.max_size',
          label: 'Cache Max Size ',
          type: 'composeAppend',
          options: [
            {
              value: 'MB',
              label: 'MB'
            },
            {
              value: 'GB',
              label: 'GB'
            }
          ],
          min: 0,
          max: 65535,
          description:
            'When enabled, configure the allowable disk space to be used. The minimum is 1GB, the maximum is 65535GB, and a value of 0 means no limit. Default is unlimited. Default path: $DATA_DIR/tasks/:id/cache\n',
          required: false,
          placeholder: 'Enter an integer in the range [1, 65535]',
          value: 0,
          unit_value: 'GB'
        },
        {
          field: 'cache.location',
          label: 'Cache Location',
          type: 'input',
          description: 'Indicates the location of the temporary storage file. Default: $DATA_DIR/tasks/:id/cache\n',
          value: 'cache',
          placeholder: '$DATA_DIR/tasks/:id/cache'
        },
        {
          field: 'cache.on_fail',
          label: 'Cache On Fail',
          type: 'select',
          options: [
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'break',
              label: 'Report Break and Stop Task'
            }
          ],
          description:
            'Defines the handling strategy for temporary storage failure. Options include Discard or Report Break and Stop Task. Default is Discard.\n',
          defaultValue: 'skip'
        },
        {
          field: 'archive.keep_days',
          label: 'Archive Keep Days',
          type: 'composeAppend',
          options: [
            {
              value: 'd',
              label: 'Days'
            }
          ],
          min: 0,
          max: 65535,
          description:
            'When the above operation is set to Archive, this configures the maximum retention period for archived files. Default is 30 days. Setting it to 0 means no limit.\n',
          required: false,
          placeholder: 'Enter a non-negative integer, 0 means unlimited',
          value: '30',
          unit_value: 'd'
        },
        {
          field: 'archive.max_size',
          label: 'Archive Max Size',
          type: 'composeAppend',
          options: [
            {
              value: 'MB',
              label: 'MB'
            },
            {
              value: 'GB',
              label: 'GB'
            }
          ],
          min: 0,
          max: 65535,
          description:
            'Maximum available disk space for archived files. Minimum is 1GB, maximum is 65535GB. Setting it to 0 means no limit. Default is unlimited. Default path: $DATA_DIR/tasks/:id/archived\n',
          required: false,
          placeholder: 'Enter an integer in the range [1, 65535]',
          value: '0',
          unit_value: 'GB'
        },
        {
          field: 'archive.location',
          label: 'Archive Location',
          type: 'input',
          description: 'Specifies the location for archived data files. Default is $DATA_DIR/tasks/:id/archived\n',
          value: 'archived',
          placeholder: '$DATA_DIR/tasks/:id/archived'
        },
        {
          field: 'archive.on_fail',
          label: 'Archive On Fail',
          type: 'select',
          options: [
            {
              value: 'rotate',
              label: 'Delete Old Files'
            },
            {
              value: 'skip',
              label: 'Skip'
            },
            {
              value: 'break',
              label: 'Report Break and Stop Task'
            }
          ],
          description: 'Delete old files, discard, or report break and stop the task.\n',
          defaultValue: 'rotate'
        }
      ]
    }
  ],
  parser: {
    display: 'Payload Transformation',
    required: true,
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

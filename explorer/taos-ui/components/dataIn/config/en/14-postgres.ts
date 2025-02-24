export default {
  name: 'PostgreSQL',
  id: 'postgres',
  type: 'uri',
  description:
    'PostgreSQL is a very powerful, open-source client/server relational database management system that has many features found in large commercial RDBMSs, including transactions, subselects, triggers, views, referential integrity, and sophisticated locking functionality.\nTDengine can efficiently read data from PostgreSQL and write it to TDengine to achieve historical data migration or real-time data synchronization.\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Host',
          description:
            'The access address of PostgreSQL.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.',
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
          description: 'The port of PostgreSQL.',
          field: 'port',
          required: true,
          placeholder: '5432',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: 'The port number ranges from 0 to 65535',
          defaultValue: '',
          type: 'input'
        },
        {
          label: 'Database',
          description: 'The name of the PostgreSQL database to connect to.',
          field: 'subject',
          required: true,
          placeholder: 'for example: db1',
          pattern: null,
          defaultValue: '',
          type: 'input'
        }
      ]
    },
    {
      label: 'Authentication',
      description: 'Authentication is the process of verifying the identity before granting access to PostgreSQL.',
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
      children: [
        {
          label: 'Connection options',
          field: 'f7f1c537-b629-484c-97d2-8bb97e2cb917',
          description: 'Other connection options.',
          children: [
            {
              label: 'Application Name',
              description: 'Set the application name to identify the connecting application.',
              field: 'application_name',
              placeholder: 'for example: TDengine',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: 'SSL Mode',
              description:
                'Set whether to negotiate a secure SSL TCP/IP connection with the server or the priority for negotiation.',
              field: 'ssl_mode',
              placeholder: 'Please select the SSL mode',
              defaultValue: 'PREFER',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'DISABLE',
                  value: 'DISABLE'
                },
                {
                  label: 'ALLOW',
                  value: 'ALLOW'
                },
                {
                  label: 'PREFER',
                  value: 'PREFER'
                },
                {
                  label: 'REQUIRE',
                  value: 'REQUIRE'
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
          field: '60866ce0-92b2-42b2-bfc4-73788db17d23',
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
                'SQL statement used for querying. The SQL statement must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).\nSQL uses different placeholders to represent different time format requirements, specifically the following placeholder formats:\n1. `${start}`, `${end}`: Represents the RFC3339 format timestamp, such as: 2024-03-14T08:00:00+0800\n2. `${start_no_tz}`, `${end_no_tz}`: Represents the RFC3339 string without a time zone: 2024-03-14T08:00:00\n3. `${start_date}`, `${end_date}`: Represents only the date, such as: 2024-03-14\n\nIf you use subtable fields, you need to concatenate field placeholders `and ${col_name1} and ${col_name2}` in the statement.note that field placeholders are case sensitive and need to be consistent with the fields in the database. If you want to sort by a specific field (recommended in ascending time order), you need to concatenate `ORDER BY time` in the statement.\n\nExample:`SELECT * FROM table WHERE time >= ${start} AND time < ${end} and ${col_name1} and ${col_name2} ORDER BY time`',
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
              label: 'Time Interval',
              description:
                'The time interval for segmented queries. The default is 1 day. To avoid querying too much data, a data synchronization subtask will use the query interval to query data in time segments.\n',
              field: 'interval',
              placeholder: 'The value is an integer ranging [0,600]',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              unit_value: 'd',
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
              unit_value: 's',
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

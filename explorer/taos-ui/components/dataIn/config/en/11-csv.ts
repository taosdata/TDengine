export default {
  name: 'CSV',
  id: 'csv',
  type: 'path',
  description: 'Import a file or a collection of files in CSV format to TDengine.\n',
  strict: true,
  config: [
    {
      label: 'Groups-before',
      field: 'groups_before',
      hide: true,
      children: []
    },
    {
      label: 'Groups-after',
      field: 'groups_after',
      hide: true,
      children: [
        {
          label: 'CSV Options',
          field: 'b8c8db31-00bf-46cd-a46c-80a680cbea67',
          description: 'CSV reading options',
          children: [
            {
              label: 'Include Header',
              description: 'If including header, the first row will be treated as column information.\n',
              field: 'has_header',
              placeholder: '',
              defaultValue: false,
              pattern: null,
              grid_two: false,
              type: 'switch'
            },
            {
              label: 'Skip the first N lines',
              description: 'Skip the first N lines for each CSV file.',
              field: 'skip',
              placeholder: '',
              defaultValue: '0',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 0,
              max: null
            },
            {
              label: 'Delimiter Char',
              description: 'The field separator in a CSV line.',
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
              label: 'Quote Char',
              description: 'The quote is used to enclose field values.',
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
              label: 'Comment Prefix',
              description:
                'If a line begins with the character given here, then that line will be ignored by the CSV parser.',
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
      label: '',
      field: 'csvData',
      type: 'csvData',
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
          defaultValue: '1000',
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
          label: 'Write Concurrency',
          field: 'written_concurrent',
          description: 'The max number of concurrent tasks writing to TDengine simultaneously.\n',
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
  ]
};

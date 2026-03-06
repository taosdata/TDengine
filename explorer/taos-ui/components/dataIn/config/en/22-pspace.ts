export default {
  // Synced with zh config except for language
  name: 'pSpace',
  id: 'pspace',
  type: 'uri',
  description:
    'pSpace is a time-series database. TDengine TSDB provides an SDK wrapper for pSpace and supports historical data migration, real-time data synchronization, and continuous query synchronization.\n',
  config: [
    {
      label: 'Connection',
      field: 'connection_options',
      children: [
        {
          label: 'Server Host',
          description: 'The IP address or domain name of the pSpace Server.',
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
          description: 'The port number of the pSpace Server.',
          field: 'port',
          placeholder: '8889',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: 'Port must be between 0 and 65535.',
          defaultValue: '',
          type: 'input'
        },
        {
          label: 'Connect Timeout',
          field: 'connect_timeout',
          description: 'Connection timeout in seconds. Default is 30 seconds. Min: 1 second, Max: 300 seconds.\n',
          required: false,
          defaultValue: '30s',
          hint: {
            type: 'duration',
            choices: [
              {
                value: 's',
                label: 'Second'
              }
            ],
            min: 1,
            max: 300
          },
          type: 'composeAppend',
          options: [
            {
              value: 's',
              label: 'Second'
            }
          ],
          min: 1,
          max: 300
        }
      ]
    },
    {
      label: 'Authentication',
      field: 'authentication',
      children: [
        {
          label: 'Username',
          description: 'Username to access pSpace Server.',
          required: true,
          field: 'username',
          defaultValue: '',
          type: 'input'
        },
        {
          label: 'Password',
          description: 'Password to access pSpace Server.',
          required: true,
          field: 'password',
          defaultValue: '',
          type: 'password'
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
      label: 'Tag Mapping',
      description: 'Mapping rules between pSpace data points and TSDB tables.',
      field: 'tag_datasets',
      name: 'datasets',
      type: 'tabs',
      multiple: false,
      valueField: 'currentTab',
      defaultValue: 'select_all_points',
      children: [
        {
          label: 'Select Points',
          name: 'select_all_points',
          labelShow: false,
          labelWidth: '0px',
          category: 'select_all_points',
          field: 'select_all_points',
          type: 'dataset',
          placeholder:
            'Set filter conditions and select data points on the pSpace Server that match the specified criteria.\n',
          required: true,
          multiple: true,
          editable: true,
          selectable: true,
          children: [
            {
              name: 'root',
              display: 'Root Node',
              description:
                'Traverse all data points starting from this node. For example, `\\Beijing\\Chaoyang` means starting from the `\\Beijing\\Chaoyang` node and traversing downward. By default, traversal starts from the root node.\n',
              placeholder: 'Root node',
              label: 'Root Node',
              field: 'root',
              defaultValue: '',
              multiple: false,
              type: 'lazyTreeSelect',
              clearable: true,
              rootLabel: 'Root Node'
            },
            {
              name: 'point_name_pattern',
              display: 'Point Name',
              description:
                'Filter by data point LongName. Example: \\Beijing\\Chaoyang\\temperature-* means all data points under "\\Beijing\\Chaoyang" whose names start with "temperature-".\n',
              placeholder: 'e.g., \\Beijing\\Chaoyang\\temperature-*',
              label: 'Point Name',
              field: 'point_name_pattern',
              defaultValue: '',
              multiple: false,
              type: 'pattern',
              viewText: 'View Point List'
            },
            {
              name: 'super_table_expression',
              display: 'Super Table Name',
              description:
                "Supports the `<super table prefix>_{type}` format, where `{type}` is the data point's data type. For example, if the data point type is `int`, then `pspace_{type}` means the super table name will be `pspace_int`.\n",
              required: true,
              value: 'pspace_{type}',
              label: 'Super Table Name',
              field: 'super_table_expression',
              defaultValue: 'pspace_{type}',
              multiple: false,
              type: 'input'
            },
            {
              name: 'child_table_expression',
              display: 'Table Name',
              description:
                'Supports the `<child table prefix>_{point_id}` format, where `{point_id}` is the data point ID. For example, if the data point ID is `150017`, then `t_{point_id}` means the table name will be `t_150017`.\n',
              required: true,
              value: 't_{point_id}',
              label: 'Table Name',
              field: 'child_table_expression',
              defaultValue: 't_{point_id}',
              multiple: false,
              type: 'input'
            },
            {
              name: 'table_primary_key',
              display: 'Timestamp Column',
              description:
                "Used as the timestamp column in the target table. `original_ts` uses the data point's original timestamp; `request_ts` is the time when the query request is sent; `received_ts` is the time when the query response is received.\n",
              required: false,
              value: 'original_ts',
              label: 'Timestamp Column',
              field: 'table_primary_key',
              defaultValue: 'original_ts',
              multiple: false,
              type: 'select',
              options: [
                {
                  label: 'original_ts',
                  value: 'original_ts'
                },
                {
                  label: 'request_ts',
                  value: 'request_ts'
                },
                {
                  label: 'received_ts',
                  value: 'received_ts'
                }
              ]
            },
            {
              name: 'table_primary_key_alias',
              display: 'Timestamp Column Name',
              description: 'The name of the timestamp column in the target table.\n',
              required: false,
              value: 'ts',
              label: 'Timestamp Column Name',
              field: 'table_primary_key_alias',
              defaultValue: 'ts',
              multiple: false,
              type: 'input'
            },
            {
              name: 'value_col',
              display: 'Value Column Name',
              description:
                'Specify the name of the value column in the target TSDB table. For example, `value_col=val` means the value column name is set to `val`.\n',
              required: false,
              label: 'Value Column Name',
              field: 'value_col',
              defaultValue: 'val',
              multiple: false,
              type: 'input'
            },
            {
              name: 'value_transform',
              display: 'Value Transform',
              description:
                'Transform expression applied to the `value` before writing to TSDB. For example, `value_transform=(val-32)/1.8` means the value is calculated using this expression.\n',
              required: false,
              label: 'Value Transform',
              field: 'value_transform',
              defaultValue: '',
              multiple: false,
              type: 'input'
            },
            {
              name: 'quality_col',
              display: 'Quality Column Name',
              description:
                'Specify the name of the quality column in the target TSDB table. For example, `quality_col=quality` means the quality column name is set to `quality`.\n',
              required: false,
              label: 'Quality Column Name',
              field: 'quality_col',
              defaultValue: 'quality',
              multiple: false,
              type: 'input'
            },
            {
              name: 'custom_tags',
              display: 'Custom Tags',
              description:
                'You can configure multiple custom tags, separated by commas. Supports static values and dynamic values extracted from pSpace data point attributes. For example, `{LongName}` will be replaced with the actual LongName attribute of the point.\n',
              required: false,
              label: 'Custom Tags',
              field: 'custom_tags',
              defaultValue:
                'VARCHAR(1024)::name::{Name};VARCHAR(1024)::LongName::{LongName};VARCHAR(1024)::Description::{Description}',
              multiple: false,
              type: 'input'
            }
          ],
          defaultValue: ''
        },
        {
          label: 'Upload CSV Mapping File',
          name: 'csv_config_file',
          field: 'csv_config_file',
          labelShow: false,
          labelWidth: '0px',
          description:
            "Use a CSV file to define the mapping from each data point to a table:\n\n(1) point_id: required. The data point ID in pSpace;\n\n(2) stable: required. The TSDB super table to map to;\n\n(3) tbname: required. The TSDB sub table to map to;\n\n(4) enable: optional. Default is '1'. Whether to collect this data point. 0 = do not collect and drop the corresponding sub table; 1 = collect data point data. If the sub table doesn't exist, it will be created;\n\n(5) value_col: optional. Default is 'val'. The column name for the collected value in TSDB;\n\n(6) value_transform: optional. The transform function applied to the collected value. Currently only numeric expression transforms are supported. See the transform docs for the expr expression syntax;\n\n(7) type: optional. Defaults to the source data type. The data type of the collected value; can be used to replace the {type} placeholder in the super table name;\n\n(8) quality_col: optional. The column name for quality in TSDB;\n\n(9) ts_col/request_ts_col/received_ts_col: required. TSDB timestamp primary key definition. You can keep only one as the primary key, or specify multiple; the first timestamp column will be used as the primary key;\n\n(10) ts_transform/request_ts_transform/received_ts_transform: optional. Timestamp transform expression; supports `+ - * /` and parentheses;\n\n(11) tag::VARCHAR(200)::name: optional; you can configure multiple tag columns. Represents a Tag column in TSDB. `tag` is a reserved keyword here. VARCHAR(200) is the tag type; name is the tag column name.\n",
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
      label: 'Collection',
      field: 'collect_options',
      description: 'Options related to data collection.',
      children: [
        {
          label: 'Task Mode',
          name: 'pspace_task_mode',
          field: 'pspace_task_mode',
          description: 'Select the task mode for data collection.',
          required: true,
          hide: false,
          placeholder: 'query',
          defaultValue: 'query',
          pattern: null,
          grid_two: false,
          type: 'select',
          options: [
            {
              label: 'Historical Query',
              value: 'query'
            },
            {
              label: 'Real-time Subscription',
              value: 'subscribe'
            },
            {
              label: 'Continuous Query',
              value: 'query_sync'
            }
          ]
        },
        {
          label: 'Start Time',
          field: 'start_time',
          description: 'Start time for historical query.',
          required: true,
          display_order: 1,
          type: 'time',
          valueFormat: 'yyyy-MM-dd HH:mm:ss',
          dateType: 'datetime',
          requiredDependsOn: ['collect_options/pspace_task_mode'],
          requiredDependsOnValues: {
            pspace_task_mode: ['query', 'query_sync']
          },
          displayDependsOn: ['collect_options/pspace_task_mode'],
          displayDependsOnValues: {
            pspace_task_mode: ['query', 'query_sync']
          }
        },
        {
          label: 'End Time',
          field: 'end_time',
          description: 'End time for historical query. Defaults to now.',
          required: false,
          display_order: 2,
          type: 'time',
          valueFormat: 'yyyy-MM-dd HH:mm:ss',
          dateType: 'datetime',
          displayDependsOn: ['collect_options/pspace_task_mode'],
          displayDependsOnValues: {
            pspace_task_mode: ['query']
          }
        },
        {
          label: 'Query Window',
          field: 'time_window',
          description: 'Time window for each query. Default is 1 day.',
          required: false,
          display_order: 3,
          defaultValue: '1d',
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
            }
          ],
          displayDependsOn: ['collect_options/pspace_task_mode'],
          displayDependsOnValues: {
            pspace_task_mode: ['query', 'query_sync']
          }
        },
        {
          label: 'Out-of-order Tolerance',
          field: 'time_excursion',
          description: 'Out-of-order tolerance for QuerySync continuous sync phase. Default is 0 seconds.',
          defaultValue: '0s',
          required: false,
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
            }
          ],
          displayDependsOn: ['collect_options/pspace_task_mode'],
          displayDependsOnValues: {
            pspace_task_mode: ['query_sync']
          }
        },
        {
          label: 'Query Interval',
          field: 'query_interval',
          description: 'Interval between queries in continuous query mode.',
          defaultValue: '10s',
          required: false,
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
            }
          ],
          displayDependsOn: ['collect_options/pspace_task_mode'],
          displayDependsOnValues: {
            pspace_task_mode: ['query_sync']
          }
        }
      ]
    },
    {
      label: 'Advanced Options',
      field: 'advanced_options',
      description: 'Tune performance, logging, and other parameters using the options below.\n',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
        {
          label: 'Log Level',
          field: 'log_level',
          description: 'Adjust the connector log level as needed. This setting may not always take effect.',
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
          label: 'Save Raw Data',
          field: 'keep_raw_data',
          description: 'Whether to save raw data?\n',
          defaultValue: false,
          required: false,
          hint: {
            type: 'bool'
          },
          type: 'switch'
        },
        {
          label: 'Max Retention Days',
          field: 'keep_raw_data_days',
          description: 'Maximum number of days to retain raw data. Default is 1 day.\n',
          defaultValue: 1,
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 365
          },
          type: 'number',
          min: 1,
          max: 365,
          displayDependsOn: ['advanced_options/keep_raw_data'],
          displayDependsOnValues: {
            keep_raw_data: [true]
          }
        },
        {
          label: 'Raw Data Directory',
          field: 'keep_raw_data_dir',
          description: 'Custom directory for storing raw data. Defaults to the system data directory.\n',
          placeholder: '$DATA_DIR/tasks/:id/rawdata/',
          required: false,
          hint: {
            type: 'str'
          },
          type: 'input',
          displayDependsOn: ['advanced_options/keep_raw_data'],
          displayDependsOnValues: {
            keep_raw_data: [true]
          }
        },
        {
          label: 'Concurrency',
          field: 'concurrency',
          description: 'Maximum concurrency. Increase this if the default performance is not sufficient.\n',
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
          label: 'Batch Size',
          field: 'batch_size',
          description: 'Maximum number of messages/rows to send per batch.\n',
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
          label: 'Batch Timeout',
          field: 'batch_timeout',
          description:
            'Maximum wait time before sending a batch (in seconds). Default is 1s. Increase this value when the data source responds slowly.\n',
          defaultValue: 1,
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 60
          },
          type: 'number',
          min: 1,
          max: 60
        },
        {
          label: 'Health Check Window',
          field: 'health_check_window_in_second',
          description:
            'How far back (time window) to aggregate task status for health checks. Usually in minutes. This window applies to all modes.\n',
          defaultValue: '0s',
          placeholder: 'Enter an integer in the range [0, 60000].',
          required: false,
          hint: {
            type: 'duration',
            choices: [
              {
                value: 's',
                label: 'Second'
              }
            ],
            min: 0,
            max: 60000
          },
          type: 'composeAppend',
          options: [
            {
              value: 's',
              label: 'Second'
            }
          ],
          min: 0,
          max: 60000
        },
        {
          label: 'Busy Threshold',
          field: 'busy_threshold',
          description: 'Percentage. The ratio of enqueued entries to queue length. Default is 100%.\n',
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
            'Number of allowed write errors within the health check window. If exceeded, a Fatal warning will be sent.',
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

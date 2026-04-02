export default {
  name: 'KingHistorian',
  id: 'kinghist',
  type: 'uri',
  description:
    'KingHistorian is a time-series database. taosX integrates the KingHistorian SDK and supports historical data migration and real-time synchronization.\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Host',
          description: 'KingHistorian server IP address or host name',
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
          description: 'KingHistorian server port',
          field: 'port',
          placeholder: '5678',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: 'The port number ranges from 0 to 65535',
          defaultValue: '',
          type: 'input'
        },
        {
          label: 'Connection Timeout (s)',
          required: false,
          field: 'connect_timeout',
          defaultValue: '30',
          type: 'number'
        }
      ]
    },
    {
      label: 'Authentication',
      description: 'Use username and password to access KingHistorian',
      field: 'authentication',
      type: 'tabs',
      defaultValue: 'plain',
      valueField: 'currentTab',
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
      label: 'Tag Configuration',
      description: 'Mapping rules between KingHistorian tags and TDengine sub-tables.',
      field: 'tag_datasets',
      name: 'datasets',
      type: 'tabs',
      multiple: false,
      valueField: 'currentTab',
      defaultValue: 'csv_config_file',
      children: [
        {
          label: 'Upload CSV Configuration File',
          name: 'csv_config_file',
          field: 'csv_config_file',
          labelShow: false,
          labelWidth: '0px',
          description:
            "Use a CSV file to define the mapping rules from each tag to a TDengine sub-table:\n\n(1) tag_name: Required. The tag name in KingHistorian.\n\n(2) stable: Required. The target TDengine super table.\n\n(3) tbname: Required. The target TDengine sub-table.\n\n(4) enable: Optional, default '1'. Whether to collect this tag. 0 - do not collect and delete the corresponding sub-table; 1 - collect the tag data and create the sub-table if it doesn't exist.\n\n(5) value_col: Optional, default 'val'. The column name for the collected value in TDengine.\n\n(6) value_transform: Optional. The transform expression executed in taosX for the collected value. Currently only arithmetic expressions are supported; see the transform documentation for expr details.\n\n(7) type: Optional, default is the source data type. The value type; can be used to replace the placeholder {type} in the super table name.\n\n(8) quality_col: Optional. The column name for the value quality in TDengine.\n\n(9) ts_col/request_ts_col/received_ts_col: Required. Define the TDengine timestamp primary key. You can keep only one of them as the primary key or specify multiple; the first timestamp column is used as the primary key.\n\n(10) ts_transform/request_ts_transform/received_ts_transform: Optional. Timestamp transform expression, supports `+ - * /` and parentheses.\n\n(11) tag::VARCHAR(200)::name: Optional/Multiple allowed. Defines a tag column in TDengine. 'tag' is a reserved keyword; VARCHAR(200) is the type; 'name' is the tag column name.\n",
          category: 'csv_config_file',
          radio: false,
          type: 'dataset',
          accept: '.csv',
          templateUrl: 'template-en.csv',
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
      label: 'Collection Settings',
      description: 'Configuration related to data collection.',
      field: 'datasets',
      type: 'tabs',
      multiple: false,
      name: 'datasets',
      valueField: 'currentTab',
      defaultValue: 'history',
      children: [
        {
          label: 'Historical Data Migration',
          name: 'history',
          field: 'history',
          hide: false,
          children: [
            {
              label: 'Start Time',
              field: 'start',
              description: 'The start time of historical data migration.',
              required: true,
              display_order: 1,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: 'End Time',
              field: 'end',
              description: 'The end time of historical data migration. Defaults to the current time.',
              required: false,
              display_order: 2,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: 'Time Window',
              field: 'step',
              description: 'The time window for each query. Default is 1 day.',
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
                  label: 'Hours'
                },
                {
                  value: 'm',
                  label: 'Minute'
                },
                {
                  value: 's',
                  label: 'Second'
                }
              ]
            },
            {
              label: 'Tolerance',
              field: 'excursion',
              description: 'The maximum time to tolerate out-of-order data. Default is 0 seconds.',
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
                  label: 'Hours'
                },
                {
                  value: 'm',
                  label: 'Minute'
                },
                {
                  value: 's',
                  label: 'Second'
                }
              ]
            },
            {
              label: 'Query Interval (s)',
              field: 'interval',
              description: 'The interval between each query, in seconds. Default is 10 seconds.',
              defaultValue: '10',
              required: false,
              type: 'number'
            }
          ]
        },
        {
          label: 'Real-time Synchronization',
          name: 'realtime',
          field: 'realtime',
          hide: false,
          children: [
            {
              label: 'Minimum Interval (ms)',
              description: 'The minimum subscription interval, in milliseconds. Default is 1000 ms.',
              field: 'min_elapsed',
              defaultValue: '1000',
              required: false,
              type: 'number'
            }
          ]
        }
      ]
    },
    {
      label: 'Advanced Options',
      field: 'advanced_options',
      description:
        'Adjust parameters related to data source performance, logging, and other options. You can modify the following settings.\n',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
        {
          label: 'Log Level',
          field: 'log_level',
          description: 'Adjust the log level for the data source as needed. This parameter may not always take effect.',
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
          label: 'Concurrency',
          field: 'concurrency',
          description: 'The maximum concurrency limit. If default performance is insufficient, increase this value.\n',
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
          description: 'The maximum number of messages or rows sent in a single batch.\n',
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
          description: 'Maximum number of days to keep raw data. Default is 1 day.\n',
          defaultValue: '1',
          required: false,
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
          description:
            'Customize the directory for storing raw data. By default, it is stored in the system data directory.\n',
          placeholder: '$DATA_DIR/tasks/:id/rawdata/',
          required: false,
          type: 'input',
          displayDependsOn: ['advanced_options/keep_raw_data'],
          displayDependsOnValues: {
            keep_raw_data: [true]
          }
        },
        {
          label: 'Health Check Duration',
          field: 'health_check_window_in_second',
          description:
            'Indicates the recent time duration over which the task status is monitored. Typically in minutes. This duration applies uniformly to all health status modes.\n',
          defaultValue: '0s',
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
            'Percentage indicating the ratio of the number of elements enqueued to the queue length. Default is 100%.\n',
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
          description: 'Indicates the maximum write queue length for an IPC connection.',
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
    }
  ]
};

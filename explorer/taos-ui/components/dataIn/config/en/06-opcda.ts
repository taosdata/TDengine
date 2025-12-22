export default {
  name: 'OPC-DA',
  id: 'opcda',
  type: 'uri',
  description:
    "OPC is one of interoperability standard for the secure and reliable exchange of data in the industrial automation space   and in other industries.\n\nOPC DA (Data Access) is a classic COM-based specification that works only on Windows.\nOPC DA is widely used even though it isn't the newest and most efficient data communication specification out there. This is mainly because of older devices that only support the OPC DA.\n\nFor more about OPC DA we introduce you to read the [OPC Foundation site](https://opcfoundation.org/), and some useful blogs, such as\n\n1. [What is OPC](https://opcfoundation.org/about/what-is-opc/)\n2. [What is OPC DA](https://plcynergy.com/opc-da/)\n\ntaosX could pull data from OPC server by a OPC connector plugin.\n\nCheck the help message in each part to see the details.\n",
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Server endpoint',
          description:
            'OPC server endpoint, such as `127.0.0.1<,localhost>/Matrikon.OPC.Simulation.1`.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be the taosX host.\n',
          field: 'endpoint',
          placeholder: '127.0.0.1/Matrikon.OPC.Simulation.1',
          pattern: null,
          defaultValue: '',
          type: 'input'
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
      label: 'Data Sets',
      description: 'Data points in OPC server to collect.',
      field: 'datasets',
      type: 'tabs',
      multiple: false,
      name: 'datasets',
      valueField: 'currentTab',
      defaultValue: 'csv_config_file',
      children: [
        {
          label: 'Upload CSV',
          name: 'csv_config_file',
          labelShow: false,
          labelWidth: '0px',
          category: 'csv_config_file',
          radio: false,
          description:
            'OPC DataIn task uses a csv file to define the mapping rules for each data point to the TDengine table:\n\n(1) tag_name: required, the id of the data point on the OPC DA server;\n\n(2) stable: required. TDengine super table corresponding to data points;\n\n(3) tbname: required. TDengine subtable corresponding to the data point;\n\n(4) enable: optional. The default value is \'1\', which specifies whether to collect data at this point. 0- Do not collect and delete the corresponding sub-table, 1- collect the point data, create a sub-table when there is no sub-table;\n\n(5) value_col: optional. The default value is val. The column name corresponding to the data point collection value in TDengine;\n\n(6) value_transform: optional, the transformation function executed in taosX for data point acquisition values. Currently, only numerical calculation expressions are supported. See expr expression description in transform document for details.\n\n(7) type: optional. The default value is the source data type. The data type of the data point collection value, which can be used to replace the placeholder {type} in the supertable name;\n\n(8) quality_col: optional, the column name corresponding to the quality of data point collection value in TDengine;\n\n(9) (9) ts_col/request_ts_col/received_ts_col: required. Definition of the TDengine timestamp primary key: You can keep only one of these columns, and the retained timestamp column will serve as the primary key. You can also fill in multiple columns, and the timestamp column at the front will be used as the primary key. Among them, ts_col uses the time when the data point is reported to the OPC server, request_ts_col uses the time when each polling request is initiated in the observe collection mode, and received_ts_col uses the time when the data is received from the OPC server;\n\n(10) xx_ts_transform: Optional. Timestamp transformation function. Refer to the description of the numerical calculation expression expr in the transform section;\n\n(11) tag::VARCHAR(200)::name: Multiple tag columns are optional or configurable. The Tag column corresponding to the data point in TDengine; tag is reserved keyword, indicating that the column is a tag column. VARCHAR(200) indicates the type of the tag, or any other valid type. name is the column name of the tag.\n\nFor more rules, please refer to the <a target="_blank" href="/docs/advanced/data-in/opcda/">enterprise version document</a>.\n',
          field: 'csv_config_file',
          type: 'dataset',
          accept: '.csv',
          templateUrl: 'template-en.csv',
          placeholder: 'OPC DA point configuration list.\n',
          required: true,
          requiredDependsOn: ['datasets/currentTab'],
          requiredDependsOnValues: {
            currentTab: ['csv_config_file']
          },
          multiple: true,
          editable: true,
          selectable: true,
          defaultValue: '',
          info2: true
        },
        {
          label: 'Data Points',
          name: 'select_all_points',
          labelShow: false,
          labelWidth: '0px',
          category: 'select_all_points',
          radio: true,
          description: 'OPC DA point configuration file.\n',
          field: 'select_all_points',
          type: 'dataset',
          accept: '.csv',
          templateUrl: 'template-en.csv',
          placeholder: 'Select data points that meet specified conditions on the OPC server.\n',
          required: true,
          multiple: true,
          editable: true,
          selectable: true,
          children: [
            {
              name: 'root',
              display: 'Root node',
              hint: {
                type: 'str'
              },
              description: 'Query all child nodes starting from this node.\n',
              placeholder: 'For example root.parent',
              label: 'Root node',
              field: 'root',
              defaultValue: '',
              multiple: false,
              type: 'input'
            },
            {
              name: 'node_id_pattern',
              display: 'Point ID',
              if: '!pattern',
              hint: {
                type: 'str'
              },
              description: 'Regex pattern match the data point id.\n',
              label: 'Point ID',
              field: 'node_id_pattern',
              defaultValue: '',
              multiple: false,
              type: 'input'
            },
            {
              name: 'browse_name_pattern',
              display: 'Point Name',
              if: '!pattern',
              hint: {
                type: 'str'
              },
              description: 'Regex pattern match the data point tag name.\n',
              label: 'Point Name',
              field: 'browse_name_pattern',
              defaultValue: '',
              multiple: false,
              type: 'pattern'
            },
            {
              name: 'pattern',
              display: 'Regex pattern',
              if: 'pattern',
              hint: {
                type: 'str'
              },
              description: 'Match the data point TagName or ID.\n',
              label: 'Regex pattern',
              field: 'pattern',
              defaultValue: '',
              multiple: false,
              type: 'pattern'
            },
            {
              name: 'super_table_expression',
              display: 'Super Table Name',
              hint: {
                type: 'str'
              },
              description:
                'Support `<super table prefix>_{type}` pattern, `{type}` is the data type of the OPC point.\n',
              required: true,
              value: 'opc_{type}',
              label: 'Super Table Name',
              field: 'super_table_expression',
              defaultValue: 'opc_{type}',
              multiple: false,
              type: 'input'
            },
            {
              name: 'child_table_expression',
              display: 'Table Name',
              hint: {
                type: 'str'
              },
              description:
                'Support `<child table prefix>_{tag_name}` pattern, `{tag_name}` is the name of the OPC point.\n',
              required: true,
              value: 't_{tag_name}',
              label: 'Table Name',
              field: 'child_table_expression',
              defaultValue: 't_{tag_name}',
              multiple: false,
              type: 'input'
            },
            {
              name: 'table_primary_key',
              display: 'Primary Key',
              hint: {
                type: 'str',
                choices: ['original_ts', 'request_ts', 'received_ts']
              },
              description:
                'The selected value will be the primary key of target table. original_ts represents the time when the data point is reported to the OPC server. request_ts is the time when each polling request is initiated in the observe collection mode. received_ts indicates the time when the data is received from the OPC server. \n',
              required: false,
              value: 'original_ts',
              label: 'Primary Key',
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
              display: 'Primary Key Name',
              hint: {
                type: 'str'
              },
              description: 'The primary key column name in the target table.\n',
              required: false,
              value: 'ts',
              label: 'Primary Key Name',
              field: 'table_primary_key_alias',
              defaultValue: 'ts',
              multiple: false,
              type: 'input'
            }
          ],
          defaultValue: ''
        }
      ]
    },
    {
      label: 'Groups-after',
      field: 'groups_after',
      hide: true,
      children: [
        {
          label: 'Connection',
          field: 'collect_options',
          description: 'Configuration used in OPC connection',
          children: [
            {
              label: 'Connect Timeout',
              description: 'Timeout for connect to endpoint in seconds',
              field: 'connect_timeout',
              placeholder: '10',
              defaultValue: '10',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1
            },
            {
              label: 'Request Timeout',
              description: 'Timeout for a request to endpoint in seconds',
              field: 'request_timeout',
              placeholder: '10',
              defaultValue: '10',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1
            }
          ],
          hide: false
        },
        {
          label: 'Collect',
          field: 'collect_options',
          description: 'Configurations for collecting data from OPC',
          children: [
            {
              label: 'Contains Bad',
              description: 'Whether to collect data with Bad Quality. Default is true.',
              field: 'contains_bad',
              type: 'switch',
              defaultValue: true
            },
            {
              label: 'Collect Interval',
              description: 'Collect data interval in second',
              field: 'interval',
              placeholder: '',
              defaultValue: '1',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1
            },
            {
              label: 'Point Update Mode',
              description:
                'Update the OPC data points. none: do not update points. append: append new points. update: append new points and delete off-line points.\n',
              field: 'update_mode',
              placeholder: '',
              defaultValue: 'none',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'none',
                  value: 'none'
                },
                {
                  label: 'append',
                  value: 'append'
                },
                {
                  label: 'update',
                  value: 'update'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: 'Point Update Interval',
              description: 'Update the OPC data points interval in seconds.\n',
              field: 'update_interval',
              placeholder: '',
              defaultValue: '600',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 60,
              max: 2147483647
            }
          ],
          hide: false
        }
      ]
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
          label: 'Log Level',
          field: 'log_level',
          description:
            'Adjust the log level of the data source as required. This parameter does not always take effect.',
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
          label: 'Write Concurrency',
          field: 'write_concurrency',
          description:
            'The number of concurrent write requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n',
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
          description:
            'The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n',
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
            'The maximum time(in seconds) to wait before sending a batch of data points. The default value is 1s. If the data source is slow to respond, you can increase this value appropriately.\n',
          defaultValue: '1',
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
          label: 'Cache Real-time Data',
          field: 'persist_data_enable',
          description:
            'After it is enabled, when taosX experiences performance issues or the downstream TDengine has slow write speeds, it will temporarily store the real-time data. Once the situation recovers, it will write the cached data back to the downstream TDengine.\n',
          defaultValue: false,
          required: false,
          hint: {
            type: 'bool'
          },
          type: 'switch'
        },
        {
          label: 'Cache Data Directory',
          field: 'persist_data_dir',
          description:
            'The directory to store the cached data. The default value is `$DATA_DIR/tasks/:id/persist_queue/`.\n',
          placeholder: '$DATA_DIR/tasks/:id/persist_queue/',
          required: false,
          type: 'input',
          displayDependsOn: ['advanced_options/persist_data_enable'],
          displayDependsOnValues: {
            persist_data_enable: [true]
          }
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
          max: 365,
          displayDependsOn: ['advanced_options/keep_raw_data'],
          displayDependsOnValues: {
            keep_raw_data: [true]
          }
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
    }
  ]
};

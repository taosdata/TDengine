export default {
  name: 'OPC-UA',
  id: 'opcua',
  type: 'uri',
  description:
    'OPC is one of interoperability standard for the secure and reliable exchange of data in the industrial automation space and in other industries.\n\nOPC UA is the next generation beyond the classic OPC specification, a platform-independent, service-oriented architecture specification that integrates all functionality from the existing OPC Classic specifications, providing a migration path to a more secure and scalable solution.\n\nTo learn more about OPC, OPC UA and OPC DA, please visit the following links on the [OPC Foundation site](https://opcfoundation.org/):\n\n1. [What is OPC](https://opcfoundation.org/about/what-is-opc/)\n2. [What is OPC UA](https://opcfoundation.org/about/opc-technologies/opc-ua/)\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Server endpoint',
          description:
            'OPC UA server endpoint, such as `127.0.0.1:6666/OPCUA/ServerPath`.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.\n',
          field: 'endpoint',
          required: true,
          placeholder: '127.0.0.1:6666/OPCUA/ServerPath',
          pattern: null,
          defaultValue: '',
          type: 'input'
        },
        {
          label: 'Security Mode',
          description: 'Available value is one of None / Sign / SignAndEncrypt.\n',
          field: 'security_mode',
          pattern: null,
          defaultValue: '',
          type: 'select',
          options: [
            {
              label: 'None',
              value: 'None'
            },
            {
              label: 'Sign',
              value: 'Sign'
            },
            {
              label: 'SignAndEncrypt',
              value: 'SignAndEncrypt'
            }
          ]
        },
        {
          label: 'Security Policy',
          description: 'Available value is one of None/Basic128Rsa15/Basic256/Basic256Sha256.\n',
          field: 'security_policy',
          pattern: null,
          defaultValue: '',
          type: 'select',
          options: [
            {
              label: 'None',
              value: 'None'
            },
            {
              label: 'Basic128Rsa15',
              value: 'Basic128Rsa15'
            },
            {
              label: 'Basic256',
              value: 'Basic256'
            },
            {
              label: 'Basic256Sha256',
              value: 'Basic256Sha256'
            },
            {
              label: 'Aes128_Sha256_RsaOaep',
              value: 'Aes128_Sha256_RsaOaep'
            },
            {
              label: 'Aes256_Sha256_RsaPss',
              value: 'Aes256_Sha256_RsaPss'
            }
          ],
          requiredDependsOn: ['connection_options/security_mode'],
          requiredDependsOnValues: {
            security_mode: ['Sign', 'SignAndEncrypt']
          },
          disabledDependsOn: ['connection_options/security_mode'],
          disabledDependsOnValues: {
            security_mode: ['None']
          },
          emptyDependsOn: ['connection_options/security_mode'],
          emptyDependsOnValues: {
            security_mode: ['None']
          }
        },
        {
          label: 'Secure Channel Certificate',
          description:
            'If the certificate is not authenticated by CA, please trust it on the server side and initiate a connectivity check again.',
          field: 'certificate',
          pattern: null,
          defaultValue: '',
          type: 'file',
          requiredDependsOn: ['connection_options/security_mode'],
          requiredDependsOnValues: {
            security_mode: ['Sign', 'SignAndEncrypt']
          }
        },
        {
          label: "Certificate's Private Key",
          description: 'The private key of the certificate.',
          field: 'private_key',
          pattern: null,
          defaultValue: '',
          type: 'file',
          requiredDependsOn: ['connection_options/security_mode'],
          requiredDependsOnValues: {
            security_mode: ['Sign', 'SignAndEncrypt']
          }
        },
        {
          label: 'Connect Timeout',
          description: 'Timeout for connect to endpoint in seconds',
          field: 'connect_timeout',
          placeholder: '10',
          type: 'number',
          min: 1,
          defaultValue: '10'
        },
        {
          label: 'Request Timeout',
          description: 'Timeout for a request to endpoint in seconds',
          field: 'request_timeout',
          placeholder: '10',
          type: 'number',
          min: 1,
          defaultValue: 10
        }
      ]
    },
    {
      label: 'Authentication',
      description: 'Use username/password plain authentication or with certificate files, or anonymous(default).',
      field: 'authentication',
      type: 'tabs',
      valueField: 'currentTab',
      defaultValue: 'anonymous',
      multiple: false,
      children: [
        {
          label: 'Anonymous',
          name: 'anonymous',
          field: 'anonymous',
          children: []
        },
        {
          label: 'Username',
          name: 'plain',
          field: 'plain',
          children: [
            {
              label: 'Username',
              description: 'OPC UA server username.',
              required: true,
              field: 'username',
              defaultValue: '',
              type: 'input'
            },
            {
              label: 'Password',
              description: 'OPC UA server password.',
              required: true,
              field: 'password',
              defaultValue: '',
              type: 'password'
            }
          ]
        },
        {
          label: 'Certificates',
          name: 'certificates',
          field: 'certificates',
          children: [
            {
              label: 'Authentication Certificate',
              required: true,
              field: 'auth_certificate',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'file'
            },
            {
              label: 'Private key of Certificate',
              required: true,
              field: 'auth_private_key',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'file'
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
            'OPC DataIn task uses a csv file to define the mapping rules for each data point to the TDengine table:\n\n(1) point_id: required, the id of the data point on the OPC UA server;\n\n(2) stable: required. TDengine super table corresponding to data points;\n\n(3) tbname: required. TDengine subtable corresponding to the data point;\n\n(4) enable: optional. The default value is \'1\', which specifies whether to collect data at this point. 0- Do not collect and delete the corresponding sub-table, 1- collect the point data, create a sub-table when there is no sub-table;\n\n(5) value_col: optional. The default value is val. The column name corresponding to the data point collection value in TDengine;\n\n(6) value_transform: optional, the transformation function executed in taosX for data point acquisition values. Currently, only numerical calculation expressions are supported. See expr expression description in transform document for details.\n\n(7) type: optional. The default value is the source data type. The data type of the data point collection value, which can be used to replace the placeholder {type} in the supertable name;\n\n(8) quality_col: optional, the column name corresponding to the quality of data point collection value in TDengine;\n\n(9) ts_col/request_ts_col/received_ts_col: required. Definition of the TDengine timestamp primary key: You can keep only one of these columns, and the retained timestamp column will serve as the primary key. You can also fill in multiple columns, and the timestamp column at the front will be used as the primary key. Among them, ts_col uses the time when the data point is reported to the OPC server, request_ts_col uses the time when each polling request is initiated in the observe collection mode, and received_ts_col uses the time when the data is received from the OPC server;\n\n(10) xx_ts_transform: Optional. Timestamp transformation function. Refer to the description of the numerical calculation expression expr in the transform section;\n\n(11) tag::VARCHAR(200)::name: Multiple tag columns are optional or configurable. The Tag column corresponding to the data point in TDengine; tag is reserved keyword, indicating that the column is a tag column. VARCHAR(200) indicates the type of the tag, or any other valid type. name is the column name of the tag.\n\nFor more rules, please refer to the <a target="_blank" href="/docs/advanced/data-in/opcua/">enterprise version document</a>.\n',
          field: 'csv_config_file',
          type: 'dataset',
          accept: '.csv',
          templateUrl: 'template-en.csv',
          placeholder: 'Upload a csv file to define the mapping rules for each data point to the TDengine table.\n',
          required: true,
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
          description: 'OPC UA point configuration file.\n',
          field: 'select_all_points',
          type: 'dataset',
          accept: '.csv',
          templateUrl: 'template-en.csv',
          placeholder: 'Select data points that meet specified conditions on the OPC UA server.\n',
          required: true,
          multiple: true,
          editable: true,
          selectable: true,
          children: [
            {
              name: 'root',
              display: 'Root node ID',
              hint: {
                type: 'str'
              },
              description: 'Query all child nodes starting from this node.\n',
              placeholder: 'For example ns=1;i=1001',
              label: 'Root node ID',
              field: 'root',
              defaultValue: '',
              multiple: false,
              type: 'input'
            },
            {
              name: 'namespaces',
              display: 'Namespaces of point',
              hint: {
                type: 'str',
                choices: ['--NONE--']
              },
              description: 'Support multiple selections, only query the data points under these namespaces.\n',
              multiple: true,
              placeholder: 'Please select after connection check successfully',
              label: 'Namespaces of point',
              field: 'namespaces',
              type: 'select'
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
              description: 'Regex pattern match the data point name.\n',
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
              description: 'Match the data point name or id',
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
                'Support `<child table prefix>_{ns}_{id}` pattern, `{ns}` is the namespace of the OPC point, and `{id}` is the id of the OPC point.for example: If the point_id is `ns=3;i=1001`, then the `{ns}` is 3 and the `{id}` is 1001.\n',
              required: true,
              value: 't_{ns}_{id}',
              label: 'Table Name',
              field: 'child_table_expression',
              defaultValue: 't_{ns}_{id}',
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
                'The selected value will be the primary key of target table. original_ts represents the time when the data point is reported to the OPC server. request_ts is the time when each polling request is initiated in the observe collection mode. received_ts indicates the time when the data is received from the OPC server.\n',
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
          label: 'Collect',
          field: 'collect_options',
          description: 'Configurations for collecting data from OPC UA server.',
          children: [
            {
              label: 'Collect Mode',
              description: 'observe or subscribe. default is subscribe',
              field: 'collect_mode',
              placeholder: 'subscribe',
              defaultValue: 'subscribe',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'observe',
                  value: 'observe'
                },
                {
                  label: 'subscribe',
                  value: 'subscribe'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: 'Collect Interval',
              description: 'Collect data interval in second',
              field: 'interval',
              placeholder: '',
              defaultValue: '10',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1,
              displayDependsOn: [
                // 'datasets/currentTab',
                'groups_after/collect_options/collect_mode'
              ], // 代表层级
              displayDependsOnValues: {
                // 'currentTab': ['select_all_points'],
                collect_mode: ['observe']
              }
            },
            {
              label: 'Request Timeout',
              description: 'Timeout for a request to endpoint in seconds',
              field: 'request_timeout',
              placeholder: '10',
              defaultValue: '1',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1,
              displayDependsOn: [
                // 'datasets/currentTab',
                'groups_after/collect_options/collect_mode'
              ], // 代表层级
              displayDependsOnValues: {
                // 'currentTab': ['select_all_points'],
                collect_mode: ['observe']
              }
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
              },
              displayDependsOn: ['datasets/currentTab'], // 代表层级
              displayDependsOnValues: {
                currentTab: ['select_all_points']
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
              max: 2147483647,
              displayDependsOn: ['datasets/currentTab'], // 代表层级
              displayDependsOnValues: {
                currentTab: ['select_all_points']
              }
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

export default {
  name: 'MQTT',
  id: 'mqtt',
  type: 'uri',
  description:
    'MQTT stands for Message Queuing Telemetry Transport. It is a lightweight messaging protocol that is easy to implement and use. It is ideal for connecting devices with limited resources, such as battery-powered devices or devices with low bandwidth. MQTT is also a good choice for applications where latency is important, such as real-time control systems.\n\nMQTT works by using a publish/subscribe model. This means that devices can publish messages to topics, and other devices can subscribe to those topics to receive the messages. This makes it easy to decouple devices from each other, and to scale up applications as needed.\n\nMQTT is a popular choice for IoT applications. It is supported by a wide range of devices and platforms, and there are many open source and commercial implementations available.\n\ntaosX could subscribe data from MQTT broker by a connector plugin.\n\nCheck the help message in each part to see the details.\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'MQTT Host',
          description:
            'MQTT server endpoint. e.g: 127.0.0.1\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.\n',
          field: 'host',
          required: true,
          placeholder: '127.0.0.1',
          pattern: null,
          defaultValue: '',
          display_order: 1,
          type: 'input'
        },
        {
          label: 'MQTT Port',
          description: 'MQTT server port',
          field: 'port',
          required: true,
          placeholder: '1883',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: 'The port number ranges from 0 to 65535',
          defaultValue: '',
          type: 'input'
        }
      ]
    },
    {
      label: 'Authentication',
      field: 'authentication',
      type: 'tabs',
      valueField: 'a7dcf55a-a4ea-483b-8980-2db60cd2d8d6',
      defaultValue: 'plain',
      multiple: false,
      children: [
        {
          label: 'Username Password',
          name: 'plain',
          field: 'plain',
          children: [
            {
              label: 'Username',
              field: 'username',
              defaultValue: '',
              type: 'input'
            },
            {
              label: 'Password',
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
          label: 'Enable SSL',
          field: 'isEnable',
          description: 'Use self-signed certificate file and private key.',
          children: [
            {
              label: 'CA',
              description: 'CA file.',
              field: 'ca',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'file',
              templateUrl: '',
              displayDependsOn: ['groups_before/ssl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
              }
            },
            {
              label: 'Client certificate file',
              description: 'Client certificate file.',
              field: 'cert',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'file',
              templateUrl: '',
              displayDependsOn: ['groups_before/ssl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
              }
            },
            {
              label: 'Client key file',
              description: 'Client key file.',
              field: 'cert_key',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'file',
              templateUrl: '',
              displayDependsOn: ['groups_before/ssl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
              }
            }
          ],
          hide: false,
          type: 'switch',
          defaultValue: false,
          valueField: 'a7dcf55a-a4ea-483b-8980-2db60cd2d8d6',
          hasValue: true
        },
        {
          label: 'Collect',
          field: 'f303fa89-1083-44a5-9dbd-2e5cdd9afb4d',
          description: 'Some configurations used in collection task.',
          children: [
            {
              label: 'MQTT protocol version',
              description: 'MQTT protocol version.',
              field: 'version',
              required: true,
              placeholder: '',
              defaultValue: '3.1',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: '3.1',
                  value: '3.1'
                },
                {
                  label: '3.1.1',
                  value: '3.1.1'
                },
                {
                  label: '5.0',
                  value: '5.0'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: 'Client ID',
              description: 'Client id used to connect to mqtt broker.',
              field: 'client_id',
              required: true,
              placeholder: 'for example: client_id',
              pattern: null,
              grid_two: false,
              type: 'customId'
            },
            {
              label: 'Keep Alive',
              description:
                'If the broker does not receive any messages from the<br>\nclient within the keep alive interval, it will assume<br>\nthat the client has disconnected and will close the<br>\nconnection.\n',
              field: 'keep_alive',
              placeholder: '10',
              defaultValue: '60',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1
              // max: null
            },
            {
              label: 'Clean Session',
              description:
                "True means that the server will forget all information<br>\nabout the session, including the client's subscriptions.<br>\nThe default value for the clean session flag is true.<br>\n",
              field: 'clean_session',
              placeholder: '',
              defaultValue: true,
              pattern: null,
              grid_two: false,
              type: 'switch'
            },
            {
              label: 'Topics Qos Config',
              description:
                'Input format: `<topic name>::<QoS>`, QoS can be 0/1/2, if subscribe multiple topics, use commas to separate them, e.g: topic1::0,topic2::1\n',
              field: 'topics',
              required: true,
              placeholder: 'topic1::0,topic2::1',
              pattern: '^(?:\\S+::[0-2],)*\\S+::[0-2]$',
              patternMsg:
                'Input format error, please refer to: `<topic name>::<QoS>`, QoS can be 0/1/2, e.g: `topic1::0,topic2::1`',
              grid_two: false,
              type: 'input'
            },
            {
              label: 'Topic Analysis',
              description: 'Resolves the subscription topic wildcard content into variables',
              field: 'topic_pattern',
              placeholder: '_/_/site_controller_id/_/point_name/data_type',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: 'Compression',
              description:
                'To save network bandwidth, you can compress the data and send it to MQTT broker. The same compression algorithm is configured here to achieve decompression.',
              field: 'compression',
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
                  label: 'gzip',
                  value: 'gzip'
                },
                {
                  label: 'snappy',
                  value: 'snappy'
                },
                {
                  label: 'lz4',
                  value: 'lz4'
                },
                {
                  label: 'zstd',
                  value: 'zstd'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: 'Char Encoding',
              description:
                'TaosX only accepts UTF8 encoded strings by default. If the sender uses non UTF8 encoding, it needs to be specified here.',
              field: 'char_encoding',
              placeholder: '',
              defaultValue: 'UTF_8',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'UTF_8',
                  value: 'UTF_8'
                },
                {
                  label: 'GBK',
                  value: 'GBK'
                },
                {
                  label: 'GB18030',
                  value: 'GB18030'
                },
                {
                  label: 'BIG5',
                  value: 'BIG5'
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
      children: []
    },
    {
      label: 'Payload Transformation',
      description: '',
      field: 'parser',
      type: 'parser',
      fields: [
        {
          name: 'ts',
          description: 'Timestamp.',
          type: 'timestamp'
        },
        {
          name: 'topic',
          description: 'Topic name.',
          type: 'varchar'
        },
        {
          name: 'qos',
          description: 'QoS, one of 0/1/2.',
          type: 'int'
        },
        {
          name: 'payload',
          description: 'Payload',
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
          label: 'Message Buffer Size',
          field: 'unprocessed_messages_buffer_size',
          description:
            'The maximum number of messages cached in the queue that have not been processed yet, used to control memory usage. When the queue is full, newly arrived data will be directly discarded. Can be set to 0, meaning not cached.',
          defaultValue: '50000',
          required: false,
          hint: {
            type: 'integer',
            min: 0,
            max: 100000
          },
          type: 'number',
          min: 0,
          max: 100000
        },
        {
          label: 'Maxmum Batch IN Processing',
          field: 'maximum_processing_batch',
          description:
            'The maximum number of batches that have not yet received an ACK response during processing. When this threshold is not reached, a batch will be retrieved from the cache queue for processing; When the maximum number is reached, the messages in the cache queue will begin to pile up. This configuration is used for backpressure mechanism to prevent excessive write pressure downstream.',
          defaultValue: '100',
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 1000
          },
          type: 'number',
          min: 1,
          max: 1000
        },
        {
          label: 'Batch Size',
          field: 'batch_size',
          description: 'The maximum number of messages or lines that can be sent at a time.',
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
            'The maximum time(in ms) to wait before sending a batch of data. If the data source is slow to respond, you can increase this value appropriately.\n',
          defaultValue: '500',
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 60000
          },
          type: 'number',
          min: 1,
          max: 60000
        },
        {
          label: 'writter concurrent',
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
      'MQTT will report exactly four fields of data for each data stream:<br>\n\n- **ts**: the collect timestamp.\n- **topic**: the topic name to subscribe.\n- **qos**: the QoS of the message, usually 0, 1, 2.\n- **payload**: the data payload of the message.\n\ntaosX could parse the payload with JSON extractor and let users to specify the data model in the database, for example, the table name pattern and stable name pattern, field names as tags or field names as columns.\n',
    fields: [
      {
        name: 'ts',
        description: 'Timestamp.',
        type: 'timestamp'
      },
      {
        name: 'topic',
        description: 'Topic name.',
        type: 'varchar'
      },
      {
        name: 'qos',
        description: 'QoS, one of 0/1/2.',
        type: 'int'
      },
      {
        name: 'payload',
        description: 'Payload',
        type: 'varchar'
      }
    ]
  }
};

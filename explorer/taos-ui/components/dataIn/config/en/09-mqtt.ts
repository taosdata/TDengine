import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

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
          defaultValue: '1883',
          type: 'input'
        },
        {
          label: 'TLS Verification',
          description:
            'Whether to enable TLS verification.\nAfter enabling one-way verification, you need to upload the CA certificate file to verify the MQTT server certificate.\nAfter enabling two-way verification, you need to upload the CA certificate file, the client certificate, and the client private key file to verify both the MQTT server certificate and the client certificate.',
          field: 'tsl_verify',
          placeholder: '',
          defaultValue: 'none',
          pattern: null,
          grid_two: false,
          type: 'select',
          options: [
            {
              label: 'Disable',
              value: 'none'
            },
            {
              label: 'Unidirectional',
              value: 'single'
            },
            {
              label: 'Bidirectional',
              value: 'both'
            }
          ]
        },
        {
          label: 'CA',
          description: 'CA certificate file, used to verify the MQTT server certificate.',
          field: 'ca',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: '',
          displayDependsOn: ['connection_options/tsl_verify'],
          displayDependsOnValues: {
            tsl_verify: ['single', 'both']
          }
        },
        {
          label: 'Client certificate file',
          description: 'A .cert file is required. ',
          field: 'cert',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: '',
          displayDependsOn: ['connection_options/tsl_verify'],
          displayDependsOnValues: {
            tsl_verify: ['both']
          }
        },
        {
          label: 'Client key file',
          description: 'Client key file',
          field: 'cert_key',
          placeholder: '',
          required: true,
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: '',
          displayDependsOn: ['connection_options/tsl_verify'],
          displayDependsOnValues: {
            tsl_verify: ['both']
          }
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
              label: 'Topics QoS Config',
              description:
                'Input format: `<topic name>::<QoS>`, QoS can be 0/1/2, if subscribe multiple topics, use commas to separate them, e.g: topic1::0,topic2::1\n',
              field: 'topics',
              required: true,
              placeholder: 'topic1::0,topic2::1',
              pattern: '^(?:[^,]+::[0-2],)*[^,]+::[0-2]$',
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
          label: 'Maximum Batch IN Processing',
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
          label: 'Written Concurrent',
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
    },
    exceptionStrategy
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

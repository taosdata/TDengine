import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
  name: 'Pulsar',
  id: 'pulsar',
  type: 'uri',
  description:
    'Apache Pulsar is an open-source distributed streaming system used for stream processing, real-time data pipelines, and data integration at scale.\nTDengine can efficiently read the data from Pulsar and write to TDengine to achieve historical data migration or real-time data streaming.\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Broker Server',
          description:
            'Pulsar Broker address.\n<br/>If using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.',
          field: 'endpoint',
          placeholder: 'broker.example.com:6650',
          pattern: null,
          defaultValue: '',
          required: true,
          display_order: 1,
          type: 'input'
        }
      ]
    },
    {
      label: 'Authentication',
      field: 'auth',
      hide: true,
      children: [
        {
          label: 'Mechanism',
          description: 'Pulsar Authentication mechanism.',
          field: 'auth_mechanism',
          placeholder: '',
          defaultValue: '',
          pattern: null,
          grid_two: false,
          type: 'select',
          options: [
            {
              label: 'Basic-Auth',
              value: 'Basic-Auth'
            },
            {
              label: 'JWT',
              value: 'JWT'
            },
            {
              label: 'mTLS',
              value: 'mTLS'
            },
            {
              label: 'Custom Authentication',
              value: 'Custom Authentication'
            }
          ],
          meta: {
            allowCreate: true,
            filterable: true
          }
        },
        {
          label: 'Username',
          description: 'The username for Basic Authentication.',
          field: 'ba_username',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'input',
          displayDependsOn: ['auth/auth_mechanism'],
          displayDependsOnValues: {
            auth_mechanism: ['Basic-Auth']
          }
        },
        {
          label: 'Password',
          description: 'The password for Basic Authentication.',
          field: 'ba_password',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'password',
          displayDependsOn: ['auth/auth_mechanism'],
          displayDependsOnValues: {
            auth_mechanism: ['Basic-Auth']
          }
        },
        {
          label: 'JWT token',
          description: 'The JWT token for JWT Authentication.',
          field: 'jwt_token',
          required: true,
          placeholder: 'Example: pulsar_jwt_token',
          grid_two: false,
          type: 'input',
          displayDependsOn: ['auth/auth_mechanism'],
          displayDependsOnValues: {
            auth_mechanism: ['JWT']
          }
        },
        {
          label: 'Client certificate',
          description: 'The public key file(PEM format) used for authentication.',
          field: 'cert',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: '',
          displayDependsOn: ['auth/auth_mechanism'],
          displayDependsOnValues: {
            auth_mechanism: ['mTLS']
          }
        },
        {
          label: 'Client key',
          description: 'The private key file(PEM format) used for authentication.',
          field: 'cert_key',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'file',
          templateUrl: '',
          displayDependsOn: ['auth/auth_mechanism'],
          displayDependsOnValues: {
            auth_mechanism: ['mTLS']
          }
        },
        {
          label: 'Custom auth name',
          description: 'The name of the custom authentication mechanism.',
          field: 'custom_auth_name',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'input',
          templateUrl: '',
          displayDependsOn: ['auth/auth_mechanism'],
          displayDependsOnValues: {
            auth_mechanism: ['Custom Authentication']
          }
        },
        {
          label: 'Custom auth data',
          description: 'The data used for custom authentication.',
          field: 'custom_auth_data',
          required: true,
          placeholder: '',
          pattern: null,
          grid_two: false,
          type: 'input',
          templateUrl: '',
          displayDependsOn: ['auth/auth_mechanism'],
          displayDependsOnValues: {
            auth_mechanism: ['Custom Authentication']
          }
        },
        {
          label: 'is use SSL',
          field: 'custom_is_ssl',
          description: 'is use SSL connection？\n',
          defaultValue: false,
          required: true,
          hint: {
            type: 'bool'
          },
          type: 'switch',
          displayDependsOn: ['auth/auth_mechanism'],
          displayDependsOnValues: {
            auth_mechanism: ['Custom Authentication']
          }
        }
      ]
    },
    {
      label: 'Collect',
      field: 'collect_options',
      description: 'Configurations for collecting data.',
      children: [
        {
          label: 'Timeout',
          description:
            'Specifies the timeout of the Pulsar Source. When no data is consumed from Pulsar, the data migration task will exit after timeout. The default value is 0 ms.\nWhen use `timeout=0`, it will wait for an usable message forever and never stop the subscription until any error caused.\n',
          field: 'timeout',
          placeholder: 'The value is an integer ranging [0,60000]',
          pattern: null,
          patternMsg: 'The value can only be a positive integer or 0',
          grid_two: false,
          defaultValue: '0ms',
          type: 'composeAppend',
          options: [
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
          max: 60000
        },
        {
          label: 'Topics',
          description: 'Specifies one topic or several topics to consume. e.g. topics=tp1,tp2\n',
          field: 'topics',
          required: true,
          placeholder: 'persistent://public/default/tp1,persistent://public/default/tp2',
          pattern: null,
          grid_two: false,
          type: 'input'
        },
        {
          label: 'Consumer name',
          description: 'Pulsar Consumer name.',
          field: 'consumer_name',
          required: true,
          placeholder: 'for example: consumer_name',
          pattern: null,
          grid_two: false,
          type: 'customId'
        },
        {
          label: 'Subscription name',
          description: 'Pulsar Subscription name.',
          field: 'subscription',
          required: true,
          placeholder: 'for example: subscription_test',
          pattern: null,
          grid_two: false,
          type: 'customId'
        },
        {
          label: 'Initial Position',
          description:
            "The position where the connector starts from.\n* `Earliest`: Receive the earliest available data record. \n* `Latest`: Receive the latest data record. \n* default is Earliest.",
          field: 'initial_position',
          placeholder: 'Earliest',
          defaultValue: 'Earliest',
          pattern: null,
          grid_two: false,
          type: 'select',
          options: [
            {
              label: 'Earliest',
              value: 'Earliest'
            },
            {
              label: 'Latest',
              value: 'Latest'
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
          name: 'partition',
          description: 'Topic partition.',
          type: 'int'
        },
        {
          name: 'offset',
          description: 'Message offset.',
          type: 'bigint'
        },
        {
          name: 'key',
          description: 'Message key.',
          type: 'varchar'
        },
        {
          name: 'value',
          description: 'Value',
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
  ]
  // parser: {
  //   display: 'Payload Transformation',
  //   required: true,
  //   description:
  //     'Pulsar will report exactly five fields of data for each data stream:<br>\n\n- **ts**: the collect timestamp.<br>\n- **topic**: the topic name to subscribe.<br>\n- **partition**: the topic partition.<br>\n- **offset**: the message offset in the topic.<br>\n- **key**: the message offset in the topic.<br>\n- **value**: the data payload of the message.<br>\n\ntaosX could parse the payload with JSON extractor and let users to specify the<br>\ndata model in the database, for example, the table name pattern and stable name<br>\npattern, field names as tags or field names as columns.\n',
  //   fields: [
  //     {
  //       name: 'ts',
  //       description: 'Timestamp.',
  //       type: 'timestamp'
  //     },
  //     {
  //       name: 'topic',
  //       description: 'Topic name.',
  //       type: 'varchar'
  //     },
  //     {
  //       name: 'partition',
  //       description: 'Topic partition.',
  //       type: 'int'
  //     },
  //     {
  //       name: 'offset',
  //       description: 'Message offset.',
  //       type: 'bigint'
  //     },
  //     {
  //       name: 'key',
  //       description: 'Message key.',
  //       type: 'varchar'
  //     },
  //     {
  //       name: 'value',
  //       description: 'Value',
  //       type: 'varchar'
  //     }
  //   ]
  // }
};

import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
  name: 'Kafka',
  id: 'kafka',
  type: 'uri',
  description:
    'Apache Kafka is an open-source distributed streaming system used for stream processing, real-time data pipelines, and data integration at scale.\nTDengine can efficiently read the data from Kafka and write to TDengine to achieve historical data migration or real-time data streaming.\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'bootstrap-server',
          description:
            'kafka bootstrap-server.\n<br/>If you configure multiple Kafka servers, all Kafka servers must belong to the same cluster.\n<br/>If using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.',
          field: 'endpoint',
          placeholder: 'ip:port,ip:port',
          pattern: null,
          defaultValue: '',
          required: true,
          display_order: 1,
          type: 'input'
        }
      ]
    },
    {
      label: 'Groups-before',
      field: 'groups_before',
      hide: true,
      children: [
        {
          label: 'SASL Authentication',
          field: 'sasl',
          description: 'Simple Authentication and Security Layer.',
          hide: false,
          type: 'switch',
          defaultValue: false,
          valueField: 'isEnable',
          hasValue: true,
          children: [
            {
              label: 'Mechanism',
              description: 'SASL authentication mechanism.',
              field: 'sasl_mechanism',
              placeholder: '',
              defaultValue: 'PLAIN',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'PLAIN',
                  value: 'PLAIN'
                },
                {
                  label: 'SCRAM-SHA-256',
                  value: 'SCRAM-SHA-256'
                },
                {
                  label: 'GSSAPI',
                  value: 'GSSAPI'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              },
              hasParentSwitch: true, //是否有父级开关
              displayDependsOn: ['groups_before/sasl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
              }
            },
            {
              label: 'Username',
              description: 'The username for SASL authentication mechanism.',
              field: 'sasl_username',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'input',
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/sasl/isEnable', 'groups_before/sasl/sasl_mechanism'],
              displayDependsOnValues: {
                isEnable: [true],
                sasl_mechanism: ['PLAIN', 'SCRAM-SHA-256']
              }
            },
            {
              label: 'Password',
              description: 'The password for SASL authentication mechanism.',
              field: 'sasl_password',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'password',
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/sasl/isEnable', 'groups_before/sasl/sasl_mechanism'],
              displayDependsOnValues: {
                isEnable: [true],
                sasl_mechanism: ['PLAIN', 'SCRAM-SHA-256']
              }
            },
            {
              label: 'Kerberos Service Name',
              description: 'The Kerberos service name for GSSAPI authentication mechanism.',
              field: 'sasl_kerberos_service_name',
              placeholder: 'for example: kafka',
              pattern: null,
              grid_two: false,
              type: 'input',
              displayDependsOn: ['groups_before/sasl/sasl_mechanism'],
              displayDependsOnValues: {
                sasl_mechanism: ['GSSAPI']
              }
            },
            {
              label: ' Kerberos Principal',
              description: 'The Kerberos principal for GSSAPI authentication mechanism.',
              field: 'sasl_kerberos_principal',
              placeholder: 'for example: kafkaclient',
              pattern: null,
              grid_two: false,
              type: 'input',
              displayDependsOn: ['groups_before/sasl/sasl_mechanism'],
              displayDependsOnValues: {
                sasl_mechanism: ['GSSAPI']
              }
            },
            {
              label: 'Kerberos Init Command',
              description: 'The Kerberos init command for GSSAPI authentication mechanism.',
              field: 'sasl_kerberos_kinit_cmd',
              placeholder: "for example: kinit -R -t '%{sasl.kerberos.keytab}' -k %{sasl.kerberos.principal}",
              pattern: null,
              grid_two: false,
              type: 'input',
              displayDependsOn: ['groups_before/sasl/sasl_mechanism'],
              displayDependsOnValues: {
                sasl_mechanism: ['GSSAPI']
              }
            },
            {
              label: 'Kerberos Keytab',
              description: 'The Kerberos keytab for GSSAPI authentication mechanism.',
              field: 'sasl_kerberos_keytab',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'file',
              templateUrl: '',
              displayDependsOn: ['groups_before/sasl/sasl_mechanism'],
              displayDependsOnValues: {
                sasl_mechanism: ['GSSAPI']
              }
            }
          ]
        },
        {
          label: 'Enable SSL',
          field: 'ssl',
          description: 'Use self-signed certificate file and private key.',
          hide: false,
          type: 'switch',
          defaultValue: false,
          valueField: 'isEnable',
          hasValue: true,
          children: [
            {
              label: 'CA',
              description: "CA certificate file(PEM format) for verifying the broker's key.",
              field: 'ca',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'file',
              templateUrl: '',
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/ssl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
              }
            },
            {
              label: 'CA Password',
              description: 'CA private key passphrase.',
              field: 'ca_password',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'password',
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/ssl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
              }
            },
            {
              label: 'Client certificate',
              description: "Client's public key file(PEM format) used for authentication.",
              field: 'cert',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'file',
              templateUrl: '',
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/ssl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
              }
            },
            {
              label: 'Client key',
              description: "Client's private key file(PEM format) used for authentication.",
              field: 'cert_key',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'file',
              templateUrl: '',
              hasParentSwitch: true,
              displayDependsOn: ['groups_before/ssl/isEnable'],
              displayDependsOnValues: {
                isEnable: [true]
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
                'Specifies the timeout of the Kafka Source. When no data is consumed from Kafka, the data migration task will exit after timeout. The default value is 0 ms.\nWhen use `timeout=0`, it will wait for an usable message forever and never stop the subscription until any error caused.\n',
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
              placeholder: 'tp1,tp2',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: 'Client ID',
              description: 'Client id used to connect to Kafka broker.',
              field: 'client_id',
              required: true,
              placeholder: 'for example: client_id',
              pattern: null,
              grid_two: false,
              type: 'customId'
            },
            {
              label: 'Group ID',
              description: 'Kafka Group ID。',
              field: 'group',
              required: true,
              placeholder: 'for example: group_id',
              pattern: null,
              grid_two: false,
              type: 'customId'
            },
            {
              label: 'Fallback Offset',
              description:
                "Possible values when querying a topic's offset.\n* `Earliest`: Receive the earliest available offset. \n* `Latest`: Receive the latest offset. \n* default is Earliest.",
              field: 'fallback_offset',
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
  ],
  parser: {
    display: 'Payload Transformation',
    required: true,
    description:
      'Kafka will report exactly five fields of data for each data stream:<br>\n\n- **ts**: the collect timestamp.<br>\n- **topic**: the topic name to subscribe.<br>\n- **partition**: the topic partition.<br>\n- **offset**: the message offset in the topic.<br>\n- **key**: the message offset in the topic.<br>\n- **value**: the data payload of the message.<br>\n\ntaosX could parse the payload with JSON extractor and let users to specify the<br>\ndata model in the database, for example, the table name pattern and stable name<br>\npattern, field names as tags or field names as columns.\n',
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
    ]
  }
};

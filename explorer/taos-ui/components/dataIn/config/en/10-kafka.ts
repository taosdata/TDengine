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
          host: {
            label: 'bootstrap-server',
            description:
              'kafka bootstrap-server.\n<br/>If you configure multiple Kafka servers, all Kafka servers must belong to the same cluster.\n<br/>If using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.',
            field: 'host_42248ddb-b1b0-4cb5-93c4-e7708e85664d',
            placeholder: '127.0.0.1',
            required: true,
            pattern: null,
            defaultValue: '',
            type: 'input'
          },
          port: {
            label: 'Port',
            description: 'Kafka Server Port',
            field: 'port_42248ddb-b1b0-4cb5-93c4-e7708e85664d',
            placeholder: '9092',
            required: true,
            pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
            patternMsg: 'The port number ranges from 0 to 65535',
            defaultValue: '',
            type: 'input'
          }
        }
      ],
      type: 'grouping'
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
              unit_value: 'ms',
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
                'Possible values when querying a topic’s offset.\n* `Earliest`: Receive the earliest available offset. \n* `Latest`: Receive the latest offset. \n* default is Earliest.',
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

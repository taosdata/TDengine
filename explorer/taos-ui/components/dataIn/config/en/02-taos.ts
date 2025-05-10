export default {
  name: 'TDengine Query',
  id: 'taos',
  type: 'uri',
  description:
    'The TDengine Query data source can be used to migrate data from previous version to current cluster.\n\n## Protocols\n\nThe supported protocols are:\n\n- ws: websocket protocol with plain HTTP connection.\n- wss: websocket protocol with TLS http connection.\n\nIf a protocol setting is not specified, a TDengine native connection will be used.\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Protocol',
          description: 'Choose a protocol scheme for websocket connection.',
          field: 'protocol',
          type: 'select',
          display_order: 0,
          defaultValue: 'ws',
          required: true,
          options: [
            {
              label: 'WS',
              value: 'ws',
              description: 'Use WebSocket with HTTP connection.'
            },
            {
              label: 'WSS',
              value: 'wss',
              description: 'Use WebSocket with HTTPS connection.'
            }
          ]
        },
        {
          label: 'Host',
          description:
            'Remote server REST API (taosAdapter) address. If you prefer to use multiple nodes, please consider to use a load-balancer.',
          field: 'host',
          required: true,
          placeholder: 'taos-adapter-addr',
          pattern: null,
          defaultValue: '',
          display_order: 1,
          type: 'input'
        },
        {
          label: 'Port',
          description: 'Remote server REST API (taosAdapter) port.',
          field: 'port',
          required: true,
          placeholder: '6041',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: 'The port number ranges from 0 to 65535',
          defaultValue: '',
          type: 'input'
        },
        {
          label: 'Database',
          description: 'Database name',
          field: 'subject',
          required: true,
          placeholder: 'Example: db1',
          pattern: null,
          defaultValue: '',
          type: 'input'
        }
      ]
    },
    {
      label: 'Authentication',
      description: 'Use username/password plain authentication.',
      field: 'authentication',
      type: 'tabs',
      valueField: 'currentTab',
      defaultValue: 'plain',
      multiple: false,
      children: [
        {
          label: 'Plain',
          name: 'plain',
          field: 'plain',
          children: [
            {
              label: 'Username',
              description: 'TDengine username. The default is root.',
              field: 'username',
              defaultValue: 'root',
              type: 'input'
            },
            {
              label: 'Password',
              description: 'TDengine password. The default is taosdata.',
              field: 'password',
              defaultValue: 'taosdata',
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
      label: 'Groups-after',
      field: 'groups_after',
      hide: true,
      children: [
        {
          label: 'Migrate Options',
          field: 'migrate_options',
          description: 'How to migrate.',
          children: [
            {
              label: 'Mode',
              description: 'Migrate history data or realtime or both.',
              field: 'mode',
              placeholder: '',
              defaultValue: 'history',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'history',
                  value: 'history'
                },
                {
                  label: 'realtime',
                  value: 'realtime'
                },
                {
                  label: 'all',
                  value: 'all'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: 'Schema',
              description:
                'Which kind of data to be migrated.\n\n- `only`: means only migrate schema into target.\n- `none`: means not migrate schema, but only data into target.\n- `always`: means migrate all stuff.\n',
              field: 'schema',
              placeholder: 'always',
              defaultValue: 'always',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'always',
                  value: 'always'
                },
                {
                  label: 'none',
                  value: 'none'
                },
                {
                  label: 'only',
                  value: 'only'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: 'Sparse',
              description:
                'Enable this mode to improve performance in case of high-cardinality and low data ingestion frequency.',
              field: 'sparse',
              placeholder: '',
              defaultValue: false,
              pattern: null,
              grid_two: false,
              type: 'switch'
            },
            {
              label: 'Schema Polling Interval',
              description: 'Polling interval to query schema.',
              field: 'schema-polling-interval',
              placeholder: 'The value is an integer ranging [0,60000]',
              defaultValue: '5s',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              type: 'composeAppend',
              options: [
                {
                  value: 'm',
                  label: 'Minute'
                },
                {
                  value: 's',
                  label: 'Second'
                }
              ],
              min: 0,
              max: 60000
            }
          ],
          hide: false
        },
        {
          label: 'What to migrate',
          field: 'what_to_migrate',
          description: 'Choose to migrate from stable or tables.',
          children: [
            {
              label: 'STables',
              description: 'Select some of stables from the database. Separated by `,`.',
              field: 'stables',
              placeholder: 'metrics',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: 'Tables',
              description: 'Select table names to be migrated.\n',
              field: 'tables',
              placeholder: 'd0001',
              pattern: null,
              grid_two: false,
              type: 'input'
            }
          ],
          hide: false
        },
        {
          field: 'range',
          description: 'Migration time range.',
          children: [
            {
              label: 'Start',
              description: 'Time range start.',
              field: 'start',
              placeholder: '2023-10-01T12:00:00.000+08:00',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime',
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'],
              displayDependsOnValues: {
                mode: ['history', 'all'],
                schema: ['always', 'none']
              }
            },
            {
              label: 'End',
              description: 'Time range end.',
              field: 'end',
              placeholder: '2023-10-02T12:00:00.000+08:00',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime',
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'],
              displayDependsOnValues: {
                mode: ['history', 'all'],
                schema: ['always', 'none']
              }
            },
            {
              label: 'Unit',
              description:
                'Time duration unit for query.<br>\nSupports abbreviations of numbers and units, such as "1ms" for 1 millisecond, "1s" for 1 seconds, "1m" for 1 minute, "1h" for 1 hour, "1d" for 1 day, and "1w" for 1 week.<br>\nOnly numbers default to seconds as unit.<br>',
              field: 'unit',
              placeholder: 'The value is an integer ranging [0,60000]',
              defaultValue: '1d',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
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
                }
              ],
              min: 0,
              max: 60000,
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'],
              displayDependsOnValues: {
                mode: ['history', 'all'],
                schema: ['always', 'none']
              }
            }
          ],
          hide: false
        },
        {
          label: 'Realtime Settings',
          field: 'realtime_settings',
          description: 'Only available in `realtime` mode.',
          children: [
            {
              label: 'Retrospection',
              description:
                'Retrospect data from some time ago into target before realtime data migrating.<br>\nSupports abbreviations of numbers and units, such as "1ms" for 1 millisecond, "1s" for 1 seconds, "1m" for 1 minute, "1h" for 1 hour, "1d" for 1 day, and "1w" for 1 week.<br>\nOnly numbers default to seconds as unit.<br>',
              field: 'retro',
              placeholder: 'The value is an integer ranging [0,60000]',
              defaultValue: '0s',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              type: 'composeAppend',
              options: [
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
                },
                {
                  value: 'ms',
                  label: 'millisecond'
                }
              ],
              min: 0,
              max: 60000,
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'], // 代表层级
              displayDependsOnValues: {
                mode: ['realtime', 'all'],
                schema: ['always', 'none']
              }
            },
            {
              label: 'Interval',
              description:
                'Polling interval to query realtime data.<br>\nSupports abbreviations of numbers and units, such as "1ms" for 1 millisecond, "1s" for 1 seconds, "1m" for 1 minute, "1h" for 1 hour, "1d" for 1 day, and "1w" for 1 week.<br>\nOnly numbers default to seconds as unit.<br>',
              field: 'interval',
              placeholder: 'The value is an integer ranging [0,60000]',
              defaultValue: '1s',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              type: 'composeAppend',
              options: [
                {
                  value: 'm',
                  label: 'Minute'
                },
                {
                  value: 's',
                  label: 'Second'
                }
              ],
              min: 0,
              max: 60000,
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'],
              displayDependsOnValues: {
                mode: ['realtime', 'all'],
                schema: ['always', 'none']
              }
            },
            {
              label: 'Excursion',
              description:
                'Wait for some period to querying random-order data.<br>\nSupports abbreviations of numbers and units, such as "1ms" for 1 millisecond, "1s" for 1 seconds, "1m" for 1 minute, "1h" for 1 hour, "1d" for 1 day, and "1w" for 1 week.<br>\nOnly numbers default to seconds as unit.<br>',
              field: 'excursion',
              placeholder: 'The value is an integer ranging [0,60000]',
              defaultValue: '500ms',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              type: 'composeAppend',
              options: [
                {
                  value: 'm',
                  label: 'Minute'
                },
                {
                  value: 's',
                  label: 'Second'
                }
              ],
              min: 0,
              max: 60000,
              displayDependsOn: ['groups_after/migrate_options/mode', 'groups_after/migrate_options/schema'],
              displayDependsOnValues: {
                mode: ['realtime', 'all'],
                schema: ['always', 'none']
              }
            }
          ],
          hide: true
        }
      ]
    },
    {
      label: 'Advanced Options',
      field: 'advanced_options',
      description:
        'Adjust the parameters related to concurrency setting for reading from data source and  writing into data sink, and error log.\n',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
        {
          label: 'Read Concurreny',
          field: 'workers',
          description:
            'The number of threads for reading data from the source. If not set, the default value is the number of CPU cores.',
          defaultValue: '0',
          required: false,
          hint: {
            type: 'integer',
            min: 0,
            max: 100
          },
          type: 'number',
          min: 0,
          max: 100
        },
        {
          label: 'Write Concurreny',
          field: 'write-concurrency',
          description:
            'The overall maximum concurrency for writing to the target database. It cannot be less than the read concurrency, and the default is equal to the read concurrency.\n',
          defaultValue: '1',
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
          label: 'File to write failed data',
          field: 'fails-to',
          description:
            'An absolute path of the environment where taosX is running. If set, the failed data and the reason for the failure will be written to the file and will not block task execution. If not set, a failed write will cause task interruption.\n',
          required: false,
          hint: {
            type: 'str'
          },
          type: 'input'
        },
        {
          label: 'Compression',
          field: 'compression',
          description: 'Enable WebSocket compression to reduce network bandwidth consumption.\n',
          defaultValue: false,
          required: false,
          hint: {
            type: 'bool'
          },
          type: 'switch'
        }
      ]
    }
  ]
};

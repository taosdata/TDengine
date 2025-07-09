export default {
  name: 'TDengine Data Subscription',
  id: 'tmq',
  type: 'uri',
  description:
    'TMQ data source is a read-only data source for TDengine.\n\n## Protocols\n\nThe following protocols are supported.\n\n- ws: websocket protocol with plain HTTP connection.\n- wss: websocket protocol with TLS http connection.\n\nWithout protocol settings, TMQ will use the TDengine native connection.\n\n## Subject\n\nA TMQ data source can subscribe to data from a database or a specified table. The table must be specified in the "database.tablename" format.\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Topic DSN',
          description:
            'Please login TDengine Cloud or TDengine enterprise, select "topics", under the list of topics, copy DSN and paste it here.\n',
          field: 'endpoint',
          required: true,
          placeholder: 'Topic example: ws://root:taosdata@127.0.0.1:6041/topic1',
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
      label: 'Groups-after',
      field: 'groups_after',
      hide: true,
      children: [
        {
          label: 'Subscribe Options',
          field: 'd5209d3d-4964-437b-8762-f76a279adbc6',
          description: 'Options for TMQ subscription.',
          children: [
            {
              label: 'Start From',
              description:
                'Data offset to start subscribing.\n- *earliest*: All the data in TDengine, include the new data,\n- *latest*: Subscribe from latest data.\n',
              field: 'auto.offset.reset',
              placeholder: '',
              defaultValue: 'earliest',
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [
                {
                  label: 'earliest',
                  value: 'earliest'
                },
                {
                  label: 'latest',
                  value: 'latest'
                }
              ],
              meta: {
                allowCreate: true,
                filterable: true
              }
            },
            {
              label: 'Group ID',
              description:
                'Group ID is a string used to identify a subscription group, with a maximum length of 192. Subscribers within the same subscription group share consumption progress. Randomly generated group ID will be used when not specified.      \n',
              field: 'group.id',
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: 'Client ID',
              description: 'Client ID is a string used to identify the client, with a maximum length of 192.\n',
              field: 'client.id',
              required: true,
              placeholder: '',
              pattern: null,
              grid_two: false,
              type: 'input'
            },
            {
              label: 'Timeout',
              description:
                'A timeout for polling data from the topic.\n\nThe input value should be one of:\n- `0`: means waiting for valid message without timeout.\n- A duration string like `5s`, `1m` etc.\n',
              field: 'timeout',
              placeholder: 'The value is an integer ranging [0,60000]',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              defaultValue: '0s',
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
              label: 'TSDB Data',
              description:
                '- If enabled, the data that has been persisted in time series data storage files will be replicated too; otherwise, only the data still in WAL (write ahead log) will be replicated.\n',
              field: 'experimental.snapshot.enable',
              placeholder: '',
              defaultValue: true,
              pattern: null,
              grid_two: false,
              type: 'switch'
            },
            {
              label: 'Table Deletions',
              description:
                'If enabled, the table deletion operations on the source side will be replayed on the sink side.\n',
              field: 'with.meta.drop',
              placeholder: '',
              defaultValue: true,
              pattern: null,
              grid_two: false,
              type: 'switch'
            },
            {
              label: 'Data Deletions',
              description:
                'If enabled, the data deletion operations on the source side will be replayed on the sink side.\n',
              field: 'with.meta.delete',
              placeholder: '',
              defaultValue: true,
              pattern: null,
              grid_two: false,
              type: 'switch'
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
        'Adjust the parameters related to concurrency setting for reading from data source and  writing into data sink, and error log.\n',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
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
        },
        {
          label: 'Health Check Duration',
          field: 'health_check_window_in_second',
          description:
            'Indicates the time duration for monitoring the task status. Typically in minutes, this duration applies uniformly to all health states.',
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
        },
        {
          label: 'Number of Consumers',
          field: 'num.of.consumers',
          description: 'Number of Consumers',
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
          label: 'Number of Writers',
          field: 'num.of.writers',
          description: 'Number of Writers',
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
          label: 'Prefer',
          field: 'prefer',
          description: 'Prefer',
          defaultValue: 'auto',
          required: false,
          hint: {
            type: 'str',
            choices: ['auto', 'raw']
          },
          type: 'select',
          options: [
            {
              label: 'auto',
              value: 'auto'
            },
            {
              label: 'raw',
              value: 'raw'
            }
          ]
        },
        {
          label: 'Commit Chunk Size',
          field: 'commit.chunk.size',
          description: 'Commit Chunk Size',
          defaultValue: '0',
          required: false,
          hint: {
            type: 'integer',
            min: 0,
            max: 1000000000
          },
          type: 'number',
          min: 0,
          max: 1000000000
        },
        {
          label: 'Commit Inerval(ms)',
          field: 'commit.interval.ms',
          description: 'Commit Inerval(ms)',
          defaultValue: '0',
          required: false,
          hint: {
            type: 'integer',
            min: 0,
            max: 1000000
          },
          type: 'number',
          min: 0,
          max: 1000000
        }
      ]
    }
  ]
};

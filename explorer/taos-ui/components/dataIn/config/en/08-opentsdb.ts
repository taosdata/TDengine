export default {
  name: 'OpenTSDB',
  id: 'opentsdb',
  type: 'uri',
  description:
    'OpenTSDB is a real-time monitoring information collection and display platform based on the HBase system.\n\nTDengine can efficiently read the data in OpenTSDB and write it to TDengine through the OpenTSDB connector to achieve historical data migration or real-time data synchronization.\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Protocol',
          description:
            'The protocol of the OpenTSDB connection, please choose according to the actual situation, otherwise the task cannot run normally.',
          field: 'protocol',
          type: 'select',
          display_order: 0,
          defaultValue: 'http',
          required: true,
          options: [
            {
              label: 'HTTP Protocol',
              value: 'http'
            },
            {
              label: 'HTTPS Protocol',
              value: 'https'
            }
          ]
        },
        {
          label: 'IP address',
          description:
            'The access address of OpenTSDB.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.',
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
          description: 'The port of OpenTSDB',
          field: 'port',
          required: true,
          placeholder: '4242',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: 'The port number ranges from 0 to 65535',
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
          label: 'task',
          field: '05ec1b4d-fed4-4b4f-aa38-2446d5dd4c67',
          description: 'Configure the data migration task',
          children: [
            {
              label: 'Metrics',
              description:
                'Metrics in OpenTSDB, select one or more specified metrics to migrate, if empty, migrate all.',
              field: 'metrics',
              placeholder: 'Please select the Metrics',
              multiple: true,
              pattern: null,
              grid_two: false,
              type: 'bucket',
              options: []
            },
            {
              label: 'Data Begin Time',
              description:
                'The starting time of the data, and the task only reads data from the specified time and after.',
              field: 'beginTime',
              required: true,
              placeholder: 'YYYY-MM-DD HH:mm:ss',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: 'Data End Time',
              description:
                'The stopping time of the data, and the task only reads the data at the specified time and before, If a future time is specified, the task will continue until the deadline is reached. If not specified, the task will continue until it is manually terminated.',
              field: 'endTime',
              placeholder: 'YYYY-MM-DD HH:mm:ss',
              pattern: null,
              grid_two: false,
              type: 'time',
              valueFormat: 'yyyy-MM-dd HH:mm:ss',
              dateType: 'datetime'
            },
            {
              label: 'Time range per read in minutes',
              description: 'The maximum time range every time when retrieving data from OpenTSDB.',
              field: 'readWindow',
              placeholder: 'Please input the time range',
              defaultValue: '60',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1,
              max: 6000
            },
            {
              label: 'Delay in seconds',
              description:
                'To migrate the out of order data, TDengine connector always waits for time specified here before reading them.',
              field: 'delay',
              placeholder: 'Please input the delay',
              defaultValue: '10',
              pattern: null,
              grid_two: false,
              type: 'number',
              min: 1,
              max: 30
            },
            {
              label: 'Rename Timestamp Field',
              description: 'Rename the timestamp field from OpenTSDB when writing to TDengine, default is "timestamp".',
              field: 'timestampFieldName',
              placeholder: 'Default: timestamp',
              pattern: null,
              grid_two: false,
              type: 'input',
              min: 1,
              max: 30
            },
            {
              label: 'Rename Value Field',
              description: 'Rename the value field from OpenTSDB when writing to TDengine, default is "value".',
              field: 'valueFieldName',
              placeholder: 'Default: value',
              pattern: null,
              grid_two: false,
              type: 'input',
              min: 1,
              max: 30
            },
            {
              label: 'Subtable Name Pattern',
              description:
                'The expression to generate subtable names in TDengine. E.g., "tb_${tag1}_${tag2}", means the subtable name is composed of the values of tag1 and tag2. If not specified, the default subtable naming convention is used.',
              field: 'tableNamePattern',
              placeholder: 'Please enter the subtable name pattern',
              pattern: null,
              grid_two: false,
              type: 'input',
              min: 1,
              max: 200
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
          label: 'Read Concurrency',
          field: 'read_concurrency',
          description:
            'The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n',
          defaultValue: '50',
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
          label: 'Write Concurrency',
          field: 'write_concurrency',
          description:
            'The number of concurrent write requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n',
          defaultValue: '50',
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 500
          },
          type: 'number',
          min: 1,
          max: 500
        },
        {
          label: 'Batch Size',
          field: 'batch_size',
          description:
            'The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n',
          defaultValue: '5000',
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
            'The maximum time(in milliseconds) to wait before sending a batch of data points. The default value is 1000ms. If the data source is slow to respond, you can increase this value appropriately.\n',
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

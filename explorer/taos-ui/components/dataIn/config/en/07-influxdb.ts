export default {
  name: 'InfluxDB',
  id: 'influxdb',
  type: 'uri',
  description:
    'InfluxDB is a popular open-source time-series database that is optimized for handling large volumes of timestamped data.\n\nTDengine can efficiently read the data in InfluxDB and write it to TDengine through the InfluxDB connector to achieve historical data migration or real-time data synchronization.\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'Protocol',
          description:
            'The protocol of the InfluxDB connection, please choose according to the actual situation, otherwise the task cannot run normally.',
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
            'The access address of InfluxDB.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.',
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
          description: 'The port of InfluxDB',
          field: 'port',
          required: true,
          placeholder: '8086',
          pattern: '^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$',
          patternMsg: 'The port number ranges from 0 to 65535',
          defaultValue: '',
          type: 'input'
        }
      ]
    },
    {
      label: 'Authentication',
      description: 'Authentication is the process of verifying the identity before granting access to InfluxDB.',
      field: 'authentication',
      type: 'tabs',
      valueField: 'only-choose-one$',
      defaultValue: '2~x',
      multiple: false,
      children: [
        {
          label: 'Version 1.x',
          name: '1~x',
          children: [
            {
              label: 'Version',
              description:
                'The version of InfluxDB, due to interface differences between versions, please choose according to the actual situation.',
              placeholder: 'Please select the version of InfluxDB',
              required: true,
              field: 'version',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'select',
              options: [
                {
                  label: '1.8',
                  value: '1.8'
                },
                {
                  label: '1.7',
                  value: '1.7'
                }
              ]
            },
            {
              label: 'Username',
              description: 'This user must have permission to read anything in this organization.',
              placeholder: 'Please input a username in the InfluxDB',
              required: true,
              field: 'username',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'input'
            },
            {
              label: 'Password',
              description: 'Verification password for the above user.',
              placeholder: 'Please input the password for the above user',
              required: true,
              field: 'password',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'password'
            }
          ]
        },
        {
          label: 'Version 2.x',
          name: '2~x',
          children: [
            {
              label: 'Version',
              description:
                'The version of InfluxDB, due to interface differences between versions, please choose according to the actual situation.',
              placeholder: 'Please select the version of InfluxDB',
              required: true,
              field: 'version',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'select',
              options: [
                {
                  label: '2.7',
                  value: '2.7'
                },
                {
                  label: '2.6',
                  value: '2.6'
                },
                {
                  label: '2.5',
                  value: '2.5'
                },
                {
                  label: '2.4',
                  value: '2.4'
                },
                {
                  label: '2.3',
                  value: '2.3'
                },
                {
                  label: '2.2',
                  value: '2.2'
                },
                {
                  label: '2.1',
                  value: '2.1'
                },
                {
                  label: '2.0',
                  value: '2.0'
                }
              ]
            },
            {
              label: 'Organization ID',
              description:
                "It's a hex number string generated by InfluxDB, not Organization name, please copy from InfluxDB organization->about page and paste it here.",
              placeholder: 'Please input your organization id in the InfluxDB',
              required: true,
              field: 'orgId',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              pattern: {},
              patternMsg: 'Please enter hexadecimal characters',
              type: 'input'
            },
            {
              label: 'Token',
              description: 'This token must have permission to read all buckets that you want to migrate.',
              placeholder: 'Please input your access token in the InfluxDB',
              required: true,
              field: 'token',
              defaultValue: '',
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'input'
            },
            {
              label: 'Add DBRP',
              description:
                'InfluxQL requires a database and retention policy (DBRP) combination in order to query data. In InfluxDB Cloud and some 2.x require manual addition of this mapping relationship. By turning on this switch, the connector can be automatically added during task execution.',
              required: false,
              field: 'addDbrp',
              defaultValue: false,
              accept: '.pem,.der,.cert,.key,.crt',
              type: 'switch'
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
          label: 'task',
          field: '03ec9fbf-5731-45db-b6eb-52f9b2e76906',
          description: 'Configure the data migration task',
          children: [
            {
              label: 'Bucket',
              description:
                'A bucket in the InfluxDB is a namespace for storing data, and each task needs to specify a bucket.',
              field: 'bucket',
              required: true,
              placeholder: 'Please select the bucket',
              pattern: null,
              grid_two: false,
              type: 'bucket',
              options: []
            },
            {
              label: 'Measurements',
              description:
                'Measurements in the above bucket, select one or more specified measurements to migrate, if empty, migrate all.',
              field: 'measurements',
              placeholder: 'Please select the measurements',
              multiple: true,
              pattern: null,
              grid_two: false,
              type: 'select',
              options: [],
              meta: {
                allowCreate: true,
                filterable: true
              }
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
              description: 'The maximum time range every time when retrieving data from InfluxDB.',
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
          label: 'Concurrent Reading Methods',
          field: 'read_concurrency_type',
          description: 'Concurrent reading methods for measurement. Queue: Multiple threads read one measurement at the same time, and then move on to the next one. Average: In an average manner, multiple measurements are read simultaneously by different threads. Sequence: Each measurement is read by only one thread at a time. \n',
          defaultValue: 'sequence',
          required: false,
          hint: {
            type: 'str',
            choices: ['queue', 'average', 'sequence']
          },
          type: 'select',
          options: [
            {
              label: 'queue',
              value: 'queue'
            },
            {
              label: 'average',
              value: 'average'
            },
            {
              label: 'sequence',
              value: 'sequence'
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
          label: 'Rows Per Read',
          field: 'rows_per_read',
          description: 'The number of rows read per query from InfluxDB. \n',
          defaultValue: 1000,
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
          label: 'Cache Queue Size',
          field: 'cache_queue_size',
          description: 'The size of the cache queue after data is read from InfluxDB. \n',
          defaultValue: 200000,
          required: false,
          hint: {
            type: 'integer',
            min: 200000,
            max: 10000000
          },
          type: 'number',
          min: 200000,
          max: 10000000
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
          label: 'JVM Options',
          description:
            'Control JVM memory parameters, GC types, etc. For example: -Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2',
          field: 'jvm_opts',
          placeholder: '-Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2',
          pattern: null,
          defaultValue: '',
          required: false,
          display_order: 1,
          type: 'input'
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

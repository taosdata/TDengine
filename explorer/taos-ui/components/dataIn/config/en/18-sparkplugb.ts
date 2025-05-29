import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
    name: 'SparkplugB',
    id: 'sparkplugb',
    type: 'uri',
    description:
        'Sparkplug B is an open message specification designed for Industrial Internet of Things (IIoT) applications, based on the MQTT protocol.',
    config: [
        {
            label: 'Connection Configuration',
            field: 'connection_options',
            children: [
                {
                    label: 'Brokers',
                    description: 'MQTT broker addresses',
                    field: 'endpoint',
                    placeholder: 'ip:port,ip:port',
                    pattern: null,
                    defaultValue: '',
                    required: true,
                    type: 'input'
                },
                {
                    label: 'MQTT Protocol Version',
                    description: 'MQTT Protocol Version',
                    field: 'version',
                    required: true,
                    placeholder: '',
                    defaultValue: '5.0',
                    pattern: null,
                    grid_two: false,
                    type: 'select',
                    options: [
                        {
                            label: '3.x',
                            value: '3.x'
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
                    description: 'MQTT Client ID',
                    field: 'client_id',
                    required: true,
                    placeholder: '示例：client_id',
                    pattern: null,
                    grid_two: false,
                    type: 'input'
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
                },
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
            label: 'Subscribe Configuration',
            field: 'subscribe_options',
            children: [
                {
                    label: 'Group ID',
                    description: 'SparkplugB group id',
                    field: 'group_id',
                    placeholder: '',
                    pattern: null,
                    defaultValue: '',
                    required: true,
                    display_order: 1,
                    type: 'input'
                },
                {
                    label: 'Node Device List',
                    description: 'SparkplugB node id and device id',
                    field: 'node_device_list',
                    placeholder: '',
                    pattern: null,
                    defaultValue: '',
                    required: true,
                    display_order: 2,
                    type: 'input'
                },
                {
                    label: 'Message Type',
                    description: 'SparkplugB message types',
                    field: 'message_types',
                    placeholder: '',
                    pattern: null,
                    defaultValue: '',
                    required: true,
                    display_order: 2,
                    type: 'input'
                },
                {
                    label: 'Rebirth CMD',
                    description: 'SparkplugB rebirth_cmd',
                    field: 'rebirth_cmd',
                    placeholder: '',
                    pattern: null,
                    defaultValue: false,
                    required: true,
                    display_order: 2,
                    type: 'switch'
                }
            ]
        },
        {
            label: 'Payload Transformation',
            description: '',
            field: 'parser',
            type: 'parser',
            fields: [
                {
                    name: 'namespace',
                    description: 'namespace',
                    type: 'varchar'
                },
                {
                    name: 'group_id',
                    description: 'group id',
                    type: 'varchar'
                },
                {
                    name: 'message_type',
                    description: 'message type',
                    type: 'varchar'
                },
                {
                    name: 'edge_node_id',
                    description: 'edge node id',
                    type: 'varchar'
                },
                {
                    name: 'device_id',
                    description: 'device id',
                    type: 'varchar'
                },
                {
                    name: 'payload_ts',
                    description: 'payload timestamp',
                    type: 'timestamp'
                },
                {
                    name: 'payload_seq',
                    description: 'payload seq number',
                    type: 'int'
                },
                {
                    name: 'payload_online',
                    description: 'payload online/offline',
                    type: 'bool'
                },
                {
                    name: 'name',
                    description: 'metric name',
                    type: 'varchar'
                },
                {
                    name: 'alias',
                    description: 'metric alias',
                    type: 'int'
                },
                {
                    name: 'timestamp',
                    description: 'metric timestamp',
                    type: 'timestamp'
                },
                {
                    name: 'datatype_str',
                    description: 'metric datatype string name',
                    type: 'varchar'
                },
                {
                    name: 'datatype',
                    description: 'metric datatype number',
                    type: 'int'
                },
                {
                    name: 'value',
                    description: 'metric value',
                    type: 'varchar'
                },
                {
                    name: 'is_historical',
                    description: 'metric is historical',
                    type: 'bool'
                },
                {
                    name: 'is_transient',
                    description: 'metric is transient',
                    type: 'bool'
                },
                {
                    name: 'is_null',
                    description: 'metric is null',
                    type: 'bool'
                },
                {
                    name: 'metadata',
                    description: 'metric metadata',
                    type: 'varchar'
                },
                {
                    name: 'properties',
                    description: 'metric properties',
                    type: 'varchar'
                }
            ],
            defaultValue: {
                parse: {}
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
};

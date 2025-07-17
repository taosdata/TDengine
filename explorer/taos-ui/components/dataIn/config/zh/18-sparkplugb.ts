import commonConfigs from './00-common';
const exceptionStrategy = JSON.parse(commonConfigs.exceptionStrategy);

export default {
    name: 'SparkplugB',
    id: 'sparkplugb',
    type: 'uri',
    description:
        'SparkplugB 是一种开放消息规范，专为工业物联网 (IIoT) 应用设计，基于 MQTT 协议。',
    config: [
        {
            label: '连接配置',
            field: 'connection_options',
            children: [
                {
                    label: 'Brokers',
                    description: 'MQTT broker 地址',
                    field: 'endpoint',
                    placeholder: 'ip:port,ip:port',
                    pattern: null,
                    defaultValue: '',
                    required: true,
                    type: 'input'
                },
                {
                    label: 'MQTT 协议',
                    description: 'MQTT 协议版本。',
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
                    label: '客户端 ID',
                    description: 'MQTT Broker 客户端 ID。',
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
                        '如果代理在保持活动间隔内没有收到来自客户端的任何消息，它将假定客户端已断开连接，并关闭连接。\n\n保持活动间隔是指客户端和代理之间协商的时间间隔，用于检测客户端是否活动。如果客户端在保持活动间隔内没有向代理发送消息，则代理将断开连接。\n\n保持活动间隔的默认值为60秒，但可以通过在连接时设置 CONNECT 报文中的 keep alive 字段来更改它。\n',
                    field: 'keep_alive',
                    placeholder: '10',
                    defaultValue: '60',
                    pattern: null,
                    grid_two: false,
                    type: 'number',
                    min: 1
                },
                {
                    label: '用户名密码访问',
                    name: 'plain',
                    field: 'plain',
                    children: [
                        {
                            label: '用户',
                            field: 'username',
                            defaultValue: '',
                            type: 'input'
                        },
                        {
                            label: '密码',
                            field: 'password',
                            defaultValue: '',
                            type: 'password'
                        }
                    ]
                },
                {
                    label: 'TLS 校验',
                    description:
                        '是否开启 TLS 校验。\n开启单向校验后，需要上传 CA 证书文件，用于校验 MQTT 服务器证书。\n开启双向校验后，需要上传 CA 证书文件、客户端证书和客户端私钥文件，用于校验 MQTT 服务器证书和客户端证书。\n',
                    field: 'tsl_verify',
                    placeholder: '',
                    defaultValue: 'none',
                    pattern: null,
                    grid_two: false,
                    type: 'select',
                    options: [
                        {
                            label: '不开启',
                            value: 'none'
                        },
                        {
                            label: '单向校验',
                            value: 'single'
                        },
                        {
                            label: '双向校验',
                            value: 'both'
                        }
                    ]
                },
                {
                    label: 'CA',
                    description: 'CA 证书文件，用于校验 MQTT 服务器证书。',
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
                    label: '客户端证书',
                    description: '需要 .cert 文件。',
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
                    label: '客户端私钥',
                    description: '私钥文件，和客户端证书必须同时上传。',
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
            label: '订阅配置',
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
                    label: '节点/设备列表',
                    description: 'SparkplugB 节点和设备列表，由逗号分隔。如：node1,node1/device1',
                    field: 'node_device_list',
                    placeholder: '',
                    pattern: null,
                    defaultValue: '',
                    required: true,
                    display_order: 2,
                    type: 'input'
                },
                {
                    label: '消息类型',
                    description: 'SparkplugB 消息类型，由逗号分隔。有 NBIRTH,NDEATH,NDATA,DBIRTH,DDEATH,DDATA,STATE 等。',
                    field: 'message_types',
                    placeholder: '',
                    pattern: null,
                    defaultValue: '',
                    required: true,
                    display_order: 2,
                    type: 'input'
                },
                {
                    label: '下发 REBIRTH 命令',
                    description: '如果消息体中使用 alias 别名，则必须开启此选项，通过下发 REBIRTH 命令来获取 alias 对应的 metric 名称。',
                    field: 'rebirth_cmd',
                    placeholder: '',
                    pattern: null,
                    defaultValue: false,
                    required: true,
                    type: 'switch'
                }
            ]
        },
        {
            label: 'Payload 转换',
            description: '',
            field: 'parser',
            type: 'parser',
            fields: [
                {
                    name: 'namespace',
                    description: '命名空间',
                    type: 'varchar'
                },
                {
                    name: 'group_id',
                    description: '组名',
                    type: 'varchar'
                },
                {
                    name: 'message_type',
                    description: '消息类型',
                    type: 'varchar'
                },
                {
                    name: 'edge_node_id',
                    description: '节点 ID',
                    type: 'varchar'
                },
                {
                    name: 'device_id',
                    description: '设备 ID',
                    type: 'varchar'
                },
                {
                    name: 'payload_ts',
                    description: 'payload 时间戳',
                    type: 'timestamp'
                },
                {
                    name: 'payload_seq',
                    description: '',
                    type: 'int'
                },
                {
                    name: 'payload_online',
                    description: '',
                    type: 'bool'
                },
                {
                    name: 'name',
                    description: '',
                    type: 'varchar'
                },
                {
                    name: 'alias',
                    description: '',
                    type: 'int'
                },
                {
                    name: 'timestamp',
                    description: '',
                    type: 'timestamp'
                },
                {
                    name: 'datatype_str',
                    description: '',
                    type: 'varchar'
                },
                {
                    name: 'datatype',
                    description: '',
                    type: 'int'
                },
                {
                    name: 'value',
                    description: '',
                    type: 'varchar'
                },
                {
                    name: 'is_historical',
                    description: '',
                    type: 'bool'
                },
                {
                    name: 'is_transient',
                    description: '',
                    type: 'bool'
                },
                {
                    name: 'is_null',
                    description: '',
                    type: 'bool'
                },
                {
                    name: 'metadata',
                    description: '',
                    type: 'varchar'
                },
                {
                    name: 'properties',
                    description: '',
                    type: 'varchar'
                }
            ],
            defaultValue: {
                parse: {}
            },
            children: []
        },
        {
            label: '高级选项',
            field: 'advanced_options',
            description: '对数据源性能、日志等其他参数进行调整，可修改以下选项。\n',
            type: 'collapse',
            defaultValue: true,
            collapsible: 'one',
            children: [
                {
                    label: '处理中批次上限',
                    field: 'maximum_processing_batch',
                    description:
                        '允许在处理中还没有等到 ACK 回复的最大批次数量，没有到达此阈值时，会从缓存队列中取出一个批次进行处理；当到达最大数量后，缓存队列中的消息会开始积压。此配置用于背压机制防止对下游造成太大写入压力。',
                    defaultValue: '100',
                    required: false,
                    type: 'number',
                    min: 1,
                    max: 1000
                },
                {
                    label: '批次大小',
                    field: 'batch_size',
                    description: '单次发送的最大消息数或行数。\n',
                    defaultValue: '1000',
                    required: false,
                    type: 'number',
                    min: 1,
                    max: 10000
                },
                {
                    label: '批次延时',
                    field: 'batch_timeout',
                    description:
                        '单次读取最大延时（单位为毫秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n',
                    defaultValue: '500',
                    required: false,
                    type: 'number',
                    min: 1,
                    max: 60000
                },
                {
                    label: '写入并发数量',
                    field: 'written_concurrent',
                    description: '同时写入 TDengine 的并发任务数量。\n',
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
                    label: '健康监测时段',
                    field: 'health_check_window_in_second',
                    description: '表示对最近多长时间的任务状态进行统计。通常为分钟级，此时段对健康状态各种模式统一生效。\n',
                    defaultValue: '0s',
                    placeholder: '输入范围为[0,60000]整数',
                    required: false,
                    type: 'composeAppend',
                    options: [
                        {
                            value: 's',
                            label: '秒'
                        }
                    ],
                    min: 0,
                    max: 60000
                },
                {
                    label: 'Busy 状态阈值',
                    field: 'busy_threshold',
                    description: '百分比，表示写入队列中入队元素数量与队列长度之比，默认 100%。\n',
                    defaultValue: '100%',
                    required: false,
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
                    label: '写入队列长度',
                    field: 'max_queue_length',
                    description: '表示一个 IPC 连接对应的写入队列长度最大值。',
                    defaultValue: '1000',
                    required: false,
                    type: 'number',
                    min: 0,
                    max: 10000
                },
                {
                    label: '写入错误阈值',
                    field: 'max_errors_in_window',
                    description: '表示健康监测时段中允许写入错误的数量。超出阈值，则发送 Fatal 警告。',
                    defaultValue: '10',
                    required: false,
                    type: 'number',
                    min: 0,
                    max: 10000
                }
            ]
        },
        exceptionStrategy
    ]
};

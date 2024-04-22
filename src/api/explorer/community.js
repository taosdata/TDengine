const datasources = [
    {
      "id": "tmq",
      "type": "uri",
      "name": "TDengine 3.x",
      "description": "使用 TMQ 进行 TDengine 指定从数据库或超级表的订阅。\n\n支持使用原生连接或 WebSocket 连接（使用 HTTP 或 HTTPS 协议）。默认使用原生连接。\n\n使用 `database` 方式指定数据库名，或 `database.table` 方式指定订阅一个超级表或普通表。\n",
      "options": {
        "endpoint": {
          "required": true,
          "display": "Topic DSN",
          "description": "请登录 TDengine 云服务或打开企业版的 Explorer, 点击`数据订阅`，你将看到主题列表，复制主题对应的 DSN 到这里即可。\n",
          "placeholder": "Topic 示例: tmq+ws://root:taosdata@localhost:6041/topic"
        }
      },
      "groups": [
        {
          "name": "订阅设置",
          "display_order": 2,
          "short_description": "TDengine TMQ 订阅设置。",
          "description": "TDengine TMQ 订阅设置。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "auto.offset.reset",
              "display": "订阅初始位置",
              "hint": {
                "type": "str",
                "choices": [
                  "earliest",
                  "latest"
                ]
              },
              "short_description": "订阅初始位置定义了拉取数据范围。",
              "description": "订阅初始位置定义了拉取数据范围。\n有以下可选项：\n- *earliest*: 相当于拉取全量数据，包括新增的数据；\n- *latest*: 从最新的数据开始订阅。\n",
              "value": "earliest"
            },
            {
              "name": "group.id",
              "display": "订阅组 ID",
              "hint": {
                "type": "str"
              },
              "short_description": "订阅组 ID 是用于标识一个订阅组的任意字符串，最大长度为 192。同一个订阅组内的订阅者共享消费进度。不指定情况下将使用随机生成的 group ID。",
              "description": "订阅组 ID 是用于标识一个订阅组的任意字符串，最大长度为 192。同一个订阅组内的订阅者共享消费进度。不指定情况下将使用随机生成的 group ID。\n"
            },
            {
              "name": "client.id",
              "display": "客户端 ID",
              "hint": {
                "type": "str"
              },
              "short_description": "客户端 ID 是一个用于标识客户端的任意字符串，最大长度为 192。",
              "description": "客户端 ID 是一个用于标识客户端的任意字符串，最大长度为 192。\n",
              "required": true
            },
            {
              "name": "timeout",
              "display": "超时",
              "hint": {
                "type": "timeout"
              },
              "short_description": "超时时间范围内没有新增数据，同步任务将自动结束。",
              "description": "超时时间范围内没有新增数据，同步任务将自动结束。\n可配置为：\n- `never`: 表示无超时时间，持续进行订阅。\n- 指定超时时间：`5s`, `1m` 等。\n",
              "placeholder": "5s"
            },
            {
              "name": "experimental.snapshot.enable",
              "display": "同步已落盘数据",
              "hint": {
                "type": "bool"
              },
              "short_description": "如启用，可以同步已经落盘到 TSDB 时序数据存储文件中（即不在 WAL 中）的数据。如关闭，则只同步尚未落盘（即保存在 WAL 中）的数据。",
              "description": "如启用，可以同步已经落盘到 TSDB 时序数据存储文件中（即不在 WAL 中）的数据。如关闭，则只同步尚未落盘（即保存在 WAL 中）的数据。\n",
              "value": "true"
            },
            {
              "name": "with.meta.drop",
              "display": "同步删表操作",
              "hint": {
                "type": "bool"
              },
              "short_description": "如启用则会同步删表操作到目标数据库。",
              "description": "如启用则会同步删表操作到目标数据库。\n",
              "value": "true"
            },
            {
              "name": "with.meta.delete",
              "display": "同步删数据操作",
              "hint": {
                "type": "bool"
              },
              "short_description": "如启用则会同步删数据操作到目标数据库。",
              "description": "如启用则会同步删数据操作到目标数据库。\n",
              "value": "true"
            }
          ]
        }
      ]
    },
    {
      "id": "taos",
      "type": "uri",
      "name": "TDengine 2.x",
      "description": "从旧版本 TDengine (2.x) 迁移到当前集群。\n",
      "options": {
        "host": {
          "required": true,
          "display": "服务器",
          "description": "TDengine REST API 服务地址。如果应用多节点，建议配合负载均衡器使用。",
          "placeholder": "taos-adapter-addr"
        },
        "port": {
          "required": true,
          "display": "端口",
          "description": "TDengine REST API 服务端口。",
          "placeholder": "6041"
        },
        "subject": {
          "required": true,
          "display": "数据库",
          "description": "数据库名称，支持特殊字符。",
          "placeholder": "示例: db1"
        }
      },
      "protocol": {
        "display": "连接协议",
        "description": "选择使用何种方式连接到 TDengine 数据源。",
        "choices": [
          {
            "name": "ws",
            "display": "WS",
            "description": "使用 HTTP 协议的 WebSocket 连接。"
          },
          {
            "name": "wss",
            "display": "WSS",
            "description": "使用 HTTPS 协议的 WebSocket 连接。"
          }
        ],
        "value": "ws"
      },
      "authentication": {
        "display": "认证",
        "description": "使用用户名密码进行认证。",
        "value": "plain",
        "alternatives": [
          {
            "name": "plain",
            "display": "用户名密码",
            "username": {
              "display": "用户名",
              "description": "TDengine 用户名，默认使用 `root`。",
              "placeholder": "root",
              "value": "root"
            },
            "password": {
              "display": "Password",
              "description": "TDengine 密码，默认为 `taosdata`。",
              "placeholder": "taosdata",
              "value": "taosdata"
            }
          }
        ]
      },
      "groups": [
        {
          "name": "迁移模式",
          "display_order": 1,
          "short_description": "支持迁移历史数据或近实时数据同步，也可设置是否总是重建表模型。",
          "description": "支持迁移历史数据或近实时数据同步，也可设置是否总是重建表模型。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "mode",
              "display": "Mode",
              "hint": {
                "type": "str",
                "choices": [
                  "history",
                  "realtime",
                  "all"
                ]
              },
              "short_description": "迁移历史数据（`history`）或实时数据（`realtime`）或两者（`both`）。",
              "description": "迁移历史数据（`history`）或实时数据（`realtime`）或两者（`both`）。",
              "value": "history"
            },
            {
              "name": "schema",
              "display": "表结构",
              "hint": {
                "type": "str",
                "choices": [
                  "always",
                  "none",
                  "only"
                ]
              },
              "short_description": "是否迁移表结构。",
              "description": "是否迁移表结构。\n\n- `only`: 仅迁移表结构，不迁移表数据。\n- `none`: 不迁移表结构，仅迁移表数据。\n- `always`: 始终迁移表结构和数据。\n",
              "value": "always"
            },
            {
              "name": "sparse",
              "display": "稀疏模式",
              "hint": {
                "type": "bool"
              },
              "short_description": "启用此模式以提升多表低频场景下的性能。",
              "description": "启用此模式以提升多表低频场景下的性能。",
              "value": "false"
            },
            {
              "name": "schema-polling-interval",
              "display": "元数据轮询间隔",
              "hint": {
                "type": "duration"
              },
              "short_description": "元数据轮询间隔，用于同步过程中的元数据变更检测。",
              "description": "元数据轮询间隔，用于同步过程中的元数据变更检测。",
              "placeholder": "时间: 5s",
              "value": "5s"
            }
          ]
        },
        {
          "name": "表",
          "display_order": 2,
          "short_description": "如果不是迁移全部数据，请配置需要迁移的表。",
          "description": "如果不是迁移全部数据，请配置需要迁移的表。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "stables",
              "display": "超级表",
              "hint": {
                "type": "str"
              },
              "short_description": "逗号分隔的一个或多个超级表。选择超级表会迁移超级表下的所有子表数据。",
              "description": "逗号分隔的一个或多个超级表。选择超级表会迁移超级表下的所有子表数据。",
              "placeholder": "metrics"
            },
            {
              "name": "tables",
              "display": "表",
              "hint": {
                "type": "str"
              },
              "short_description": "子表或普通表，支持 `tb1` 形式的表名或 `stable.table` 形式的子表名。",
              "description": "子表或普通表，支持 `tb1` 形式的表名或 `stable.table` 形式的子表名。\n",
              "placeholder": "d0001"
            }
          ]
        },
        {
          "name": "时间范围",
          "display_order": 3,
          "short_description": "迁移时间范围和查询单元。",
          "description": "迁移时间范围和查询单元。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "start",
              "display": "开始时间",
              "hint": {
                "type": "time"
              },
              "short_description": "迁移数据开始时间。",
              "description": "迁移数据开始时间。",
              "placeholder": "2023-10-01T12:00:00.000+08:00"
            },
            {
              "name": "end",
              "display": "结束时间",
              "hint": {
                "type": "time"
              },
              "short_description": "迁移数据结束时间。",
              "description": "迁移数据结束时间。",
              "placeholder": "2023-10-02T12:00:00.000+08:00"
            },
            {
              "name": "unit",
              "display": "查询单元",
              "hint": {
                "type": "duration"
              },
              "short_description": "查询数据的基本单元，长时间范围的查询会以此为依据切割为多次查询。",
              "description": "查询数据的基本单元，长时间范围的查询会以此为依据切割为多次查询。<br>\n支持使用数字加单位缩写，如\"1ms\"表示1毫秒，\"1s\"表示1秒，\"1m\"表示1分钟，\"1h\"表示1小时，\"1d\"表示1天，\"1w\"表示1周。<br>\n单独使用数字则默认认为是秒。<br>",
              "placeholder": "示例：1d",
              "value": "1d"
            }
          ]
        },
        {
          "name": "实时同步",
          "display_order": 4,
          "short_description": "以下参数仅在实时同步模式（`realtime`）下支持。",
          "description": "以下参数仅在实时同步模式（`realtime`）下支持。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "retro",
              "display": "回溯",
              "hint": {
                "type": "duration"
              },
              "short_description": "在实时同步前回溯一段时间内的数据写入目标库。",
              "description": "在实时同步前回溯一段时间内的数据写入目标库。<br>\n支持使用数字加单位缩写，如\"1ms\"表示1毫秒，\"1s\"表示1秒，\"1m\"表示1分钟，\"1h\"表示1小时，\"1d\"表示1天，\"1w\"表示1周。<br>\n单独使用数字则默认认为是秒。<br>",
              "placeholder": "示例：1s",
              "value": "0s"
            },
            {
              "name": "interval",
              "display": "间隔",
              "hint": {
                "type": "duration"
              },
              "short_description": "轮询查询的时间间隔。",
              "description": "轮询查询的时间间隔。<br>\n支持使用数字加单位缩写，如\"1ms\"表示1毫秒，\"1s\"表示1秒，\"1m\"表示1分钟，\"1h\"表示1小时，\"1d\"表示1天，\"1w\"表示1周。<br>\n单独使用数字则默认认为是秒。<br>",
              "placeholder": "示例：1s",
              "value": "1s"
            },
            {
              "name": "excursion",
              "display": "乱序",
              "hint": {
                "type": "duration"
              },
              "short_description": "等待一段时间的乱序数据入库后再进行查询。",
              "description": "等待一段时间的乱序数据入库后再进行查询。<br>\n支持使用数字加单位缩写，如\"1ms\"表示1毫秒，\"1s\"表示1秒，\"1m\"表示1分钟，\"1h\"表示1小时，\"1d\"表示1天，\"1w\"表示1周。<br>\n单独使用数字则默认认为是秒。<br>",
              "placeholder": "示例：5m",
              "value": "500ms"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "调整与读并发、写并发和错误日志相关的参数。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "workers",
            "display": "最大读并发数",
            "hint": {
              "type": "integer",
              "min": 0,
              "max": 100
            },
            "description": "并发查询的线程数，如果为 0 会自动设置为 CPU 核数。",
            "value": "0"
          },
          {
            "name": "write-concurrency",
            "display": "最大写并发数",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 100
            },
            "description": "写入目标数据库的整体最大并发数。不能小于读并发数，默认等于读并发数。\n",
            "value": "1"
          },
          {
            "name": "fails-to",
            "display": "错误记录文件",
            "hint": {
              "type": "str"
            },
            "description": "taosX 所处运行环境的一个绝对路径。 如有值，写入失败的数据及失败原因将被写入该文件，并不阻塞任务执行。如无值，写入失败会导致任务中断。\n"
          }
        ]
      }
    },
    {
      "id": "pi",
      "type": "uri",
      "name": "PI",
      "description": "PI 系统是一套用于数据收集、查找、分析、传递和可视化的软件产品，可以作为管理实时数据和事件的企业级系统的基础架构。\n\nPI 系统这个术语通常用来指代PI服务器，但这两者并不相同。PI系统指的是所有 OSIsoft 软件产品，而 PI 服务器是 PI 系统的核心产品。数据可以自动从许多来源（控制系统、实验室设备、计算、手动输入或定制软件）收集。\n\ntaosX 可以通过 PI 连接器插件从 PI 系统中提取实时数据。\n",
      "options": {
        "host": {
          "required": true,
          "display": "PI服务名",
          "description": "PI 服务器地址（通常使用主机名）。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。",
          "placeholder": "server"
        },
        "port": {},
        "subject": {
          "required": true,
          "display": "AF Database Name",
          "description": "AF 数据库名",
          "placeholder": "如: Met1"
        }
      },
      "groups": [
        {
          "name": "自动回填",
          "display_order": 1,
          "short_description": "自动回填配置。",
          "description": "自动回填配置。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "MaxBackfillRangeDays",
              "display": "重启补偿时间",
              "hint": {
                "type": "str"
              },
              "short_description": "连接丢失或首次启动时自动回填的最长时间：`2d`、`3h`、`4m` 等。",
              "description": "连接丢失或首次启动时自动回填的最长时间：`2d`、`3h`、`4m` 等。",
              "placeholder": "30m",
              "value": "30m"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "log_level",
            "display": "日志级别",
            "hint": {
              "type": "str",
              "choices": [
                "error",
                "warn",
                "info",
                "debug",
                "trace"
              ]
            },
            "description": "根据需要调整数据源的日志级别，此参数不总是生效。",
            "value": "info"
          },
          {
            "name": "batch_size",
            "display": "批次大小",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 10000
            },
            "description": "单次发送的最大消息数或行数。\n",
            "value": "1000"
          },
          {
            "name": "batch_timeout",
            "display": "批次延时",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 60
            },
            "description": "单次读取最大延时（单位为秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n",
            "value": "1"
          }
        ]
      },
      "params": [
        {
          "name": "system_configuration",
          "display": "PI 系统配置",
          "display_order": 0,
          "hint": {
            "type": "str",
            "choices": [
              "PI Data Archive and Asset Framework (AF) Server",
              "PI Data Archive Only"
            ]
          },
          "value": "PI Data Archive and Asset Framework (AF) Server"
        },
        {
          "name": "PISystemName",
          "display": "AF Server 名称",
          "display_order": 3,
          "hint": {
            "type": "str"
          },
          "description": "PI 系统(AF Server) 名称 (hostname).",
          "required": true,
          "placeholder": "pi-af-server-name"
        }
      ],
      "datasets": {
        "name": "Data Sets",
        "display": "监测点集",
        "description": "不同类型的点位配置文件，这将决定入库的数据模型。\n",
        "params": [
          {
            "name": "point_file",
            "display": "单列模式点位列表",
            "hint": {
              "type": "file"
            },
            "description": "一个单列点位名称列表文件。\n\n| |\n| ------------------- |\n| meter_10001_current |\n| meter_10001_voltage |\n"
          },
          {
            "name": "template_for_pi_point_file",
            "display": "单列模式 AF 模板列表",
            "hint": {
              "type": "file"
            },
            "description": "单列点位名称（AF 模板）列表文件。\n\n| |\n| ------------------- |\n| MeterTemplate  |\n| MeterTemplate1 |\n"
          },
          {
            "name": "template_for_af_element_file",
            "display": "AF 模式模板列表",
            "hint": {
              "type": "file"
            },
            "description": "单列模板名称列表文件。\n\n| |\n| ------------------- |\n| MeterTemplate  |\n| MeterTemplate1 |\n"
          }
        ]
      }
    },
    {
      "id": "pibackfill",
      "type": "uri",
      "name": "PI Backfill",
      "description": "PI 系统是一套用于数据收集、查找、分析、传递和可视化的软件产品，可以作为管理实时数据和事件的企业级系统的基础架构。\n\nPI 系统这个术语通常用来指代PI服务器，但这两者并不相同。PI系统指的是所有 OSIsoft 软件产品，而 PI 服务器是 PI 系统的核心产品。数据可以自动从许多来源（控制系统、实验室设备、计算、手动输入或定制软件）收集。\n\ntaosX 可以通过 PI BACKFILL 连接器插件从 PI 系统中提取历史数据。\n",
      "options": {
        "host": {
          "required": true,
          "display": "PI服务名",
          "description": "PI 服务器地址（通常使用主机名）。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。",
          "placeholder": "server"
        },
        "port": {},
        "subject": {
          "required": true,
          "display": "AFDatabaseName",
          "description": "AF 数据库名",
          "placeholder": "Example: Met1"
        }
      },
      "groups": [
        {
          "name": "历史填充（Backfill）",
          "display_order": 1,
          "short_description": "Backfill 参数设置",
          "description": "Backfill 参数设置",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "BackfillStartTime",
              "display": "Backfill 开始时间",
              "hint": [
                {
                  "selected": true,
                  "display": "请选择开始时间",
                  "type": "time",
                  "value": null,
                  "default": null
                },
                {
                  "selected": false,
                  "display": "从TDengine存储的记录的最晚时间戳开始",
                  "type": "constant",
                  "value": "auto"
                }
              ],
              "short_description": "从该时间开始导入历史数据，默认为当前时间 10 天之前。",
              "description": "从该时间开始导入历史数据，默认为当前时间 10 天之前。\n",
              "placeholder": "YYYY-MM-DD HH:mm:ss"
            },
            {
              "name": "BackfillEndTime",
              "display": "Backfill 结束时间",
              "hint": [
                {
                  "selected": true,
                  "display": "请选择开始时间",
                  "type": "time",
                  "value": null,
                  "default": null
                },
                {
                  "selected": false,
                  "display": "到TDengine存储的记录的最早的时间戳结束",
                  "type": "constant",
                  "value": "auto"
                }
              ],
              "short_description": "导入历史数据以该时间结束，默认是当前时间。",
              "description": "导入历史数据以该时间结束，默认是当前时间。\n",
              "placeholder": "YYYY-MM-DD HH:mm:ss",
              "conflicts_with": [
                {
                  "name": "BackfillStartTime",
                  "value": "auto",
                  "when": "auto"
                }
              ]
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "log_level",
            "display": "日志级别",
            "hint": {
              "type": "str",
              "choices": [
                "error",
                "warn",
                "info",
                "debug",
                "trace"
              ]
            },
            "description": "根据需要调整数据源的日志级别，此参数不总是生效。",
            "value": "info"
          },
          {
            "name": "batch_size",
            "display": "批次大小",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 10000
            },
            "description": "单次发送的最大消息数或行数。\n",
            "value": "1000"
          },
          {
            "name": "batch_timeout",
            "display": "批次延时",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 60
            },
            "description": "单次读取最大延时（单位为秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n",
            "value": "1"
          }
        ]
      },
      "params": [
        {
          "name": "system_configuration",
          "display": "PI 系统配置",
          "display_order": 0,
          "hint": {
            "type": "str",
            "choices": [
              "PI Data Archive and Asset Framework (AF) Server",
              "PI Data Archive Only"
            ]
          },
          "value": "PI Data Archive and Asset Framework (AF) Server"
        },
        {
          "name": "PISystemName",
          "display": "AF Server 名称",
          "display_order": 3,
          "hint": {
            "type": "str"
          },
          "description": "PI 系统(AF Server) 名称 (hostname).",
          "required": true,
          "placeholder": "pi-af-server-name"
        }
      ],
      "datasets": {
        "name": "Data Sets",
        "display": "监测点集",
        "description": "不同类型的点位配置文件，这将决定入库的数据模型。\n",
        "params": [
          {
            "name": "point_file",
            "display": "单列模式点位列表",
            "hint": {
              "type": "file"
            },
            "description": "一个单列点位名称列表文件。\n\n| |\n| ------------------- |\n| meter_10001_current |\n| meter_10001_voltage |\n"
          },
          {
            "name": "template_for_pi_point_file",
            "display": "单列模式 AF 模板列表",
            "hint": {
              "type": "file"
            },
            "description": "单列点位名称（AF 模板）列表文件。\n\n| |\n| ------------------- |\n| MeterTemplate  |\n| MeterTemplate1 |\n"
          },
          {
            "name": "template_for_af_element_file",
            "display": "AF 模式模板列表",
            "hint": {
              "type": "file"
            },
            "description": "单列模板名称列表文件。\n\n| |\n| ------------------- |\n| MeterTemplate  |\n| MeterTemplate1 |\n"
          }
        ]
      }
    },
    {
      "id": "opcua",
      "type": "uri",
      "name": "OPC-UA",
      "description": "OPC 是工业自动化领域和其他行业中安全可靠地交换数据的互操作标准之一。\n\nOPC UA 是经典 OPC 规范的下一代标准，是一个平台无关的面向服务的架构规范，集成了现有 OPC Classic 规范的所有功能，提供了一条迁移到更安全和可扩展解决方案的路径。\n\n如果想了解更多关于 OPC UA 的信息，可以阅读 OPC Foundation 网站和一些有用的博客，例如：\n1. [What is OPC](https://opcfoundation.org/about/what-is-opc/)\n2. [What is OPC UA](https://opcfoundation.org/about/opc-technologies/opc-ua/)\n\ntaosX 使用 OPC 连接器从 OPC 服务器拉取或订阅数据。\n",
      "options": {
        "endpoint": {
          "required": true,
          "display": "服务地址",
          "description": "OPC UA 服务器端点，如：`127.0.0.1:6666/OPCUA/ServerPath`。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。\n",
          "placeholder": "127.0.0.1:6666/OPCUA/ServerPath"
        },
        "security_mode": {
          "name": "security_mode",
          "display": "安全模式",
          "hint": {
            "type": "str",
            "choices": [
              "None",
              "Sign",
              "SignAndEncrypt"
            ]
          },
          "description": "Security mode（安全模式）是 OPC UA 协议中用于保护通信安全的一种机制。安全模式定义了如何加密和验证通信数据，以防止未经授权的访问和篡改。\n"
        },
        "security_policy": {
          "name": "security_policy",
          "display": "安全策略",
          "hint": {
            "type": "str",
            "choices": [
              "None",
              "Basic128Rsa15",
              "Basic256",
              "Basic256Sha256",
              "Aes128_Sha256_RsaOaep",
              "Aes256_Sha256_RsaPss"
            ]
          },
          "description": "Security Policy（安全策略）是 OPC UA 协议中用于定义安全机制的一种机制。安全策略定义了如何实现安全模式中的加密和验证机制，包括使用的加密算法、密钥长度、数字证书等。\n"
        },
        "certificate": {
          "name": "certificate",
          "display": "安全通信证书",
          "hint": {
            "type": "file"
          },
          "description": "建立连接时，发送给 OPC UA 服务器；如果未经 CA 认证，请在服务器端信任此证书后，再次发起连通性检查。"
        },
        "private_key": {
          "name": "private_key",
          "display": "安全通信私钥",
          "hint": {
            "type": "file"
          },
          "description": "私钥文件，对服务器发送的消息做签名检查或者解密。"
        },
        "connect_timeout": {
          "name": "connect_timeout",
          "display": "连接超时",
          "hint": {
            "type": "integer",
            "min": 1,
            "max": 60
          },
          "description": "连接超时间隔，单位为：秒 (s)。",
          "placeholder": "10",
          "value": "10"
        }
      },
      "authentication": {
        "display": "认证",
        "description": "OPC UA 可选择使用多种认证方式。",
        "value": "anonymous",
        "alternatives": [
          {
            "name": "anonymous",
            "display": "匿名访问"
          },
          {
            "name": "plain",
            "display": "用户名",
            "username": {
              "required": true,
              "display": "用户名",
              "description": "OPC UA 服务登录用户名。",
              "placeholder": "username"
            },
            "password": {
              "required": true,
              "display": "密码",
              "description": "OPC UA 服务登录密码.",
              "placeholder": "password"
            }
          },
          {
            "name": "certificates",
            "display": "证书访问",
            "params": [
              {
                "name": "auth_certificate",
                "display": "认证证书文件",
                "hint": {
                  "type": "file"
                }
              },
              {
                "name": "auth_private_key",
                "display": "认证证书私钥",
                "hint": {
                  "type": "file"
                }
              }
            ]
          }
        ]
      },
      "groups": [
        {
          "name": "采集配置",
          "display_order": 1,
          "short_description": "数据采集相关配置项。",
          "description": "数据采集相关配置项。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "interval",
              "display": "采集间隔",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 60
              },
              "short_description": "数据点位采集间隔，单位为：秒。",
              "description": "数据点位采集间隔，单位为：秒。",
              "value": "10"
            },
            {
              "name": "collect_mode",
              "display": "采集模式",
              "hint": {
                "type": "str",
                "choices": [
                  "observe",
                  "subscribe"
                ]
              },
              "short_description": "`observe` 模式（读取点位最新值上报）或 `subscribe`（订阅模式，变更时上报）。默认为 `subscribe`。",
              "description": "`observe` 模式（读取点位最新值上报）或 `subscribe`（订阅模式，变更时上报）。默认为 `subscribe`。",
              "placeholder": "subscribe",
              "value": "subscribe"
            },
            {
              "name": "request_timeout",
              "display": "采集超时",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 60
              },
              "short_description": "数据采集请求超时间隔，单位为：秒 (s)。",
              "description": "数据采集请求超时间隔，单位为：秒 (s)。",
              "placeholder": "10",
              "value": "10"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "log_level",
            "display": "日志级别",
            "hint": {
              "type": "str",
              "choices": [
                "error",
                "warn",
                "info",
                "debug",
                "trace"
              ]
            },
            "description": "根据需要调整数据源的日志级别，此参数不总是生效。",
            "value": "info"
          },
          {
            "name": "write_concurrency",
            "display": "最大写入并发数",
            "hint": {
              "type": "integer",
              "min": 0,
              "max": 128
            },
            "description": "写入 taosX 的最大并发数限制，当默认参数性能不足时，可增大此参数。\n",
            "value": "0"
          },
          {
            "name": "batch_size",
            "display": "批次大小",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 10000
            },
            "description": "单次发送的最大消息数或行数。\n",
            "value": "1000"
          },
          {
            "name": "batch_timeout",
            "display": "批次延时",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 60
            },
            "description": "单次读取最大延时（单位为秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n",
            "value": "1"
          },
          {
            "name": "keep_raw_data",
            "display": "保存原始数据",
            "hint": {
              "type": "bool"
            },
            "description": "是否保存原始数据？\n"
          },
          {
            "name": "keep_raw_data_days",
            "display": "最大保留天数",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 365
            },
            "description": "原始数据最大保存天数，默认 1 天。\n",
            "value": "1",
            "requires": "keep_raw_data"
          },
          {
            "name": "keep_raw_data_dir",
            "display": "原始数据存储目录",
            "hint": {
              "type": "str"
            },
            "description": "自定义原始数据存储目录，默认存储到系统数据目录下。\n",
            "placeholder": "$DATA_DIR/tasks/:id/rawdata/",
            "requires": "keep_raw_data"
          }
        ]
      },
      "datasets": {
        "name": "点位集",
        "description": "OPC 采集数据点位列表。",
        "value": "csv_config_file",
        "categories": [
          {
            "category": "csv_config_file",
            "display": "上传 CSV 配置文件",
            "description": "您可以下载 CSV 空模板并按模板配置点位信息，然后上传 CSV 配置文件来配置点位；或者根据所配置的筛选条件下载数据点位，并以 CSV 模板所制定的格式下载。\n\n通过 CSV 文件配置 OPC UA 点位的规则如下：\n\n1.文件编码\n\n请上传编码为 UTF-8 或 UTF-8 BOM 的 CSV 文件；\n\n2.Header 的规则\n\nCSV 文件的第一行为 Header，请按照如下规则配置 Header：\n\n(1) point_id：数据点位在 OPC UA 服务器上的 id，必填；\n\n(2) stable：数据点位在 TDengine 对应的超级表，必填；\n\n(3) tbname：数据点位在 TDengine 对应的子表，必填；\n\n(4) enable：是否采集该点位数据，可选，不配置 enable 列时，使用统一的默认值1作为 enable 的值；\n\n(5) value_col：数据点位采集值在 TDengine 中对应的列名，可选，不配置 value_col 列时，使用统一的默认值 val 作为 value_col 的值；\n\n(6) value_transform：数据点位采集值在 taosX 中执行的变换函数，可选，不配置 value_transform 列时，统一不进行采集值的 transform；\n\n(7) type：数据点位采集值的数据类型，可选，不配置 type 列时，统一使用采集值的原始类型作为 TDengine 中的数据类型；\n\n(8) quality_col：数据点位采集值质量在 TDengine 中对应的列名，可选，不配置 quality_col 时，统一不在 TDengine 添加 quality 列；\n\n(9) ts_col：数据点位的原始时间戳在 TDengine 中对应的时间戳列，可选，ts_col，received_ts_col 按顺序同时存在，使用 ts_col 作 TDengine 中的时间戳列；ts_col 存在，使用 ts_col 作 TDengine 中的时间戳列；\n\n(10) received_ts_col：接收到该点位采集值时的时间戳在 TDengine 中对应的时间戳列，可选，received_ts_col，ts_col 按顺序同时存在，使用 received_ts_col 作 TDengine 中的时间戳列；received_ts_col 存在，使用 received_ts_col 作 TDengine 中的时间戳列；\n\n(11) ts_col 和 received_ts_col 同时不存在，使用数据点位原始时间戳作 TDengine 中的时间戳列，且列名为默认值ts。\n\n(12) ts_transform：数据点位时间戳在 taosX 中执行的变换函数，可选，不配置 ts_transform 列时，统一不进行数据点位原始时间戳的 transform；\n\n(13) received_ts_transform：数据点位接收时间戳在 taosX 中执行的变换函数，可选，不配置 received_ts_transform 列时，统一不进行数据点位接收时间戳的 transform；\n\n(14) tag::VARCHAR(200)::name：数据点位在 TDengine 中对应的 Tag 列；其中 tag 为保留关键字，表示该列为一个 tag 列；VARCHAR(200) 表示该 tag 的类型，也可以是其它合法的类型；name 是该 tag 的实际名称。\n\n(15) tag 列是可选的，当 CSV 中配置 1 个以上的 tag 列，则使用配置的 tag 列；\n\n(16) 当没有配置任何 tag 列，且 stable 在 TDengine 中存在，使用 TDengine 中的 stable 的 tag；\n\n(17) 没有配置任何 tag 列，且 stable 在 TDengine 中不存在，则默认自动添加以下 2 个 tag 列：tag::VARCHAR(256)::point_id 和 tag::VARCHAR(256)::point_name\n\n(18) CSV Header 中，不能有重复的列；\n\n(19) CSV Header 中，类似 tag::VARCHAR(200)::name 这样的列可以配置多个，对应 TDengine 中的多个 Tag，但 Tag 的名称不能重复。\n\n(20) CSV Header 中，列的顺序不影响 CSV 文件校验规则；\n\n(21) CSV Header 中，可以配置不在上表中的列，例如：序号，这些列会被自动忽略。\n\n3.Row 的规则\n\nCSV 文件的第二行开始为数据行，每一行对应一个数据点位的配置信息。请按照下面的规则配置 Row。\n\n一个 Row 中，与 Header 列对应的关系如下：\n\n(1) point_id：类似ns=3;i=1005这样的字符串，必填；\n\n(2) stable：符合 TDengine 超级表命名规范的任何字符串；如果存在特殊字符.，使用下划线替换；如果存在{type}，则：CSV 文件的 type 不为空，使用 type 的值进行替换；CSV 文件的 type 为空，使用采集值的原始类型进行替换；\n\n(3) tbname：符合 TDengine 子表命名规范的任何字符串；如果存在特殊字符.，使用下划线替换；如果存在{ns}，使用 point_id 中的 ns 替换，如果存在{id}，使用 point_id 中的 id 替换；\n\n(4) enable：0，不采集该点位，且在 OPC DataIn 任务开始前，删除 TDengine 中点位对应的子表；1，采集该点位，在 OPC DataIn 任务开始前，不删除子表。\n\n(5) value_col：符合 TDengine 命名规范的列名\n\n(6) value_transform：符合 Rhai 引擎的计算表达式，例如：(val + 10) / 1000 * 2.0，log(val) + 10等；\n\n(7) type：支持类型包括：b/bool，i8/tinyint，i16/smallint，i32/int，i64/bigint，u8/tinyint unsigned，u16/smallint unsigned，u32/int unsigned，u64/bigint unsigned，f32/float，f64/double，timestamp/timestamp(ms)，timestamp(us)，timestamp(ns)，json\n\n(8) quality_col：符合 TDengine 命名规范的列名\n\n(9) ts_col：符合 TDengine 命名规范的列名\n\n(10) received_ts_col：符合 TDengine 命名规范的列名\n\n(11) ts_transform 和 received_ts_transform：支持 +、-、*、/、% 操作符，例如：ts / 1000 * 1000，将一个 ms 单位的时间戳的最后 3 位置为 0；ts + 8 * 3600 * 1000，将一个 ms 精度的时间戳，增加 8 小时；ts - 8 * 3600 * 1000，将一个 ms 精度的时间戳，减去 8 小时；\n\n(12) tag::VARCHAR(200)::name：tag 里的值，当 tag 的类型是 VARCHAR 时，可以是中文。\n\n同时，多个Row之间还需要满足：\n\n(13) point_id 在整个 DataIn 任务中是唯一的，即：在一个 OPC DataIn 任务中，一个数据点位只能被写入到 TDengine 的一张子表。如果需要将一个数据点位写入多张子表，需要建多个 OPC DataIn 任务；\n\n(14) 当 point_id 不同，但 tbname 相同时，value_col 必须不同。这种配置能够将不同数据类型的多个点位的数据写入同一张子表中不同的列。这种方式对应 “OPC 数据入 TDengine 宽表”的使用场景。\n\n4.其他规则\n\n(1) 如果 Header 和 Row 的列数不一致，校验失败，提示用户不满足要求的行号；\n\n(2) Header 在首行，且不能为空；\n\n(3) Row 为 1 行以上；\n",
            "target": {
              "name": "csv_config_file",
              "required": true,
              "multiple": true,
              "editable": true,
              "selectable": true
            }
          },
          {
            "category": "select_all_points",
            "display": "选择数据点位",
            "target": {
              "name": "select_all_points",
              "description": "设置过滤条件，选择 OPC UA 服务器上满足指定条件的数据点位。\n",
              "required": true,
              "multiple": true,
              "editable": true,
              "selectable": true
            },
            "params": [
              {
                "name": "root",
                "display": "根节点 ID",
                "hint": {
                  "type": "str"
                },
                "description": "从该节点开始遍历所有子节点。\n",
                "placeholder": "例如 ns=3;i=1001"
              },
              {
                "name": "namespaces",
                "display": "命名空间",
                "hint": {
                  "type": "str",
                  "choices": [
                    "--NONE--"
                  ]
                },
                "description": "支持多选,只查询这些 namespace 下的数据点位。\n",
                "multiple": true,
                "placeholder": "连通性检查通过后，可选择，支持多选"
              },
              {
                "name": "pattern",
                "display": "正则匹配",
                "hint": {
                  "type": "str"
                },
                "description": "数据点位名称或 id 需要满足设置的正则表达式。\n"
              },
              {
                "name": "table_primary_key",
                "display": "主键列",
                "hint": {
                  "type": "str",
                  "choices": [
                    "received_ts",
                    "original_ts"
                  ]
                },
                "description": "目标数据表主键将使用选择的值作为时间戳主键名称，original_ts 表示使用数据点位上报 OPC 服务时间，received_ts 表示 taosX 任务接收数据的时间。\n",
                "required": true,
                "value": "original_ts"
              },
              {
                "name": "child_table_expression",
                "display": "表名称",
                "hint": {
                  "type": "str"
                },
                "description": "支持 <child table prefix>_{ns}_{id} 格式，如果 NodeId 中不存在 ns 或 id 将置为空\n",
                "required": true,
                "value": "t_{ns}_{id}"
              }
            ]
          }
        ]
      }
    },
    {
      "id": "opcda",
      "type": "uri",
      "name": "OPC-DA",
      "description": "OPC是工业自动化领域和其他行业中安全可靠地交换数据的互操作标准之一。\n\nOPC DA（数据访问）是一种经典的基于COM的规范，仅适用于Windows。尽管OPC DA不是最新和最高效的数据通信规范，但它被广泛使用。这主要是因为一些旧设备只支持OPC DA。\n\nOPC UA是经典OPC规范的下一代标准，是一个平台无关的面向服务的架构规范，集成了现有OPC Classic规范的所有功能，提供了一条迁移到更安全和可扩展解决方案的路径。\n\n如果想了解更多关于OPC UA/DA的信息，可以阅读OPC Foundation网站和一些有用的博客，例如：\n1. [What is OPC](https://opcfoundation.org/about/what-is-opc/)\n2. [What is OPC DA](https://plcynergy.com/opc-da/)\n\ntaosX 使用 OPC 连接器从 OPC 服务器拉取或订阅数据。\n",
      "options": {
        "endpoint": {
          "required": true,
          "display": "服务地址",
          "description": "OPC 服务器地址。如： `127.0.0.1<,localhost>/Matrikon.OPC.Simulation.1`。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址应是 taosX 服务器所在主机。\n",
          "placeholder": "127.0.0.1/Matrikon.OPC.Simulation.1"
        }
      },
      "groups": [
        {
          "name": "连接配置",
          "display_order": 1,
          "short_description": "OPC 连接相关配置",
          "description": "OPC 连接相关配置",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "connect_timeout",
              "display": "连接超时",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 60
              },
              "short_description": "DA 连接超时间隔，单位为：秒 (s)。",
              "description": "DA 连接超时间隔，单位为：秒 (s)。",
              "placeholder": "10",
              "value": "10"
            },
            {
              "name": "request_timeout",
              "display": "采集超时",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 60
              },
              "short_description": "DA 数据采集超时间隔，单位为：秒 (s)。",
              "description": "DA 数据采集超时间隔，单位为：秒 (s)。",
              "placeholder": "10",
              "value": "10"
            }
          ]
        },
        {
          "name": "采集配置",
          "display_order": 2,
          "short_description": "数据采集相关配置项。",
          "description": "数据采集相关配置项。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "interval",
              "display": "采集间隔",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 60
              },
              "short_description": "数据点位采集间隔，单位为：秒。",
              "description": "数据点位采集间隔，单位为：秒。",
              "value": "1"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "log_level",
            "display": "日志级别",
            "hint": {
              "type": "str",
              "choices": [
                "error",
                "warn",
                "info",
                "debug",
                "trace"
              ]
            },
            "description": "根据需要调整数据源的日志级别，此参数不总是生效。",
            "value": "info"
          },
          {
            "name": "write_concurrency",
            "display": "最大写入并发数",
            "hint": {
              "type": "integer",
              "min": 0,
              "max": 128
            },
            "description": "写入 taosX 的最大并发数限制，当默认参数性能不足时，可增大此参数。\n",
            "value": "0"
          },
          {
            "name": "batch_size",
            "display": "批次大小",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 10000
            },
            "description": "单次发送的最大消息数或行数。\n",
            "value": "1000"
          },
          {
            "name": "batch_timeout",
            "display": "批次延时",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 60
            },
            "description": "单次读取最大延时（单位为秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n",
            "value": "1"
          },
          {
            "name": "keep_raw_data",
            "display": "保存原始数据",
            "hint": {
              "type": "bool"
            },
            "description": "是否保存原始数据？\n"
          },
          {
            "name": "keep_raw_data_days",
            "display": "最大保留天数",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 365
            },
            "description": "原始数据最大保存天数，默认 1 天。\n",
            "value": "1",
            "requires": "keep_raw_data"
          },
          {
            "name": "keep_raw_data_dir",
            "display": "原始数据存储目录",
            "hint": {
              "type": "str"
            },
            "description": "自定义原始数据存储目录，默认存储到系统数据目录下。\n",
            "placeholder": "$DATA_DIR/tasks/:id/rawdata/",
            "requires": "keep_raw_data"
          }
        ]
      },
      "datasets": {
        "name": "点位集",
        "description": "OPC 采集数据点位列表。",
        "value": "csv_config_file",
        "categories": [
          {
            "category": "csv_config_file",
            "display": "上传 CSV 配置文件",
            "description": "您可以下载 CSV 空模板并按模板配置点位信息，然后上传 CSV 配置文件来配置点位；或者根据所配置的筛选条件下载数据点位，并以 CSV 模板所制定的格式下载。\n\n通过 CSV 文件配置 OPC DA 点位的规则如下：\n\n1.文件编码\n\n请上传编码为 UTF-8 或 UTF-8 BOM 的 CSV 文件；\n\n2.Header 的规则\n\nCSV 文件的第一行为 Header，请按照如下规则配置 Header：\n\n(1) tag_name：数据点位在 OPC DA 服务器上的 id，必填；\n\n(2) stable：数据点位在 TDengine 对应的超级表，必填；\n\n(3) tbname：数据点位在 TDengine 对应的子表，必填；\n\n(4) enable：是否采集该点位数据，可选，不配置 enable 列时，使用统一的默认值1作为 enable 的值；\n\n(5) value_col：数据点位采集值在 TDengine 中对应的列名，可选，不配置 value_col 列时，使用统一的默认值 val 作为 value_col 的值；\n\n(6) value_transform：数据点位采集值在 taosX 中执行的变换函数，可选，不配置 value_transform 列时，统一不进行采集值的 transform；\n\n(7) type：数据点位采集值的数据类型，可选，不配置 type 列时，统一使用采集值的原始类型作为 TDengine 中的数据类型；\n\n(8) quality_col：数据点位采集值质量在 TDengine 中对应的列名，可选，不配置 quality_col 时，统一不在 TDengine 添加 quality 列；\n\n(9) ts_col：数据点位的原始时间戳在 TDengine 中对应的时间戳列，可选，ts_col，received_ts_col 按顺序同时存在，使用 ts_col 作 TDengine 中的时间戳列；ts_col 存在，使用 ts_col 作 TDengine 中的时间戳列；\n\n(10) received_ts_col：接收到该点位采集值时的时间戳在 TDengine 中对应的时间戳列，可选，received_ts_col，ts_col 按顺序同时存在，使用 received_ts_col 作 TDengine 中的时间戳列；received_ts_col 存在，使用 received_ts_col 作 TDengine 中的时间戳列；\n\n(11) ts_col 和 received_ts_col 同时不存在，使用数据点位原始时间戳作 TDengine 中的时间戳列，且列名为默认值ts。\n\n(12) ts_transform：数据点位时间戳在 taosX 中执行的变换函数，可选，不配置 ts_transform 列时，统一不进行数据点位原始时间戳的 transform；\n\n(13) received_ts_transform：数据点位接收时间戳在 taosX 中执行的变换函数，可选，不配置 received_ts_transform 列时，统一不进行数据点位接收时间戳的 transform；\n\n(14) tag::VARCHAR(200)::name：数据点位在 TDengine 中对应的 Tag 列；其中 tag 为保留关键字，表示该列为一个 tag 列；VARCHAR(200) 表示该 tag 的类型，也可以是其它合法的类型；name 是该 tag 的实际名称。\n\n(15) tag 列是可选的，当 CSV 中配置 1 个以上的 tag 列，则使用配置的 tag 列；\n\n(16) 当没有配置任何 tag 列，且 stable 在 TDengine 中存在，使用 TDengine 中的 stable 的 tag；\n\n(17) 没有配置任何 tag 列，且 stable 在 TDengine 中不存在，则默认自动添加以下 2 个 tag 列：tag::VARCHAR(256)::point_id 和 tag::VARCHAR(256)::point_name\n\n(18) CSV Header 中，不能有重复的列；\n\n(19) CSV Header 中，类似 tag::VARCHAR(200)::name 这样的列可以配置多个，对应 TDengine 中的多个 Tag，但 Tag 的名称不能重复。\n\n(20) CSV Header 中，列的顺序不影响 CSV 文件校验规则；\n\n(21) CSV Header 中，可以配置不在上表中的列，例如：序号，这些列会被自动忽略。\n\n3.Row 的规则\n\nCSV 文件的第二行开始为数据行，每一行对应一个数据点位的配置信息。请按照下面的规则配置 Row。\n\n一个 Row 中，与 Header 列对应的关系如下：\n\n(1) tag_name：类似`root.parent.temperature`这样的字符串，必填；\n\n(2) stable：符合 TDengine 超级表命名规范的任何字符串；如果存在特殊字符.，使用下划线替换；如果存在{type}，则：CSV 文件的 type 不为空，使用 type 的值进行替换；CSV 文件的 type 为空，使用采集值的原始类型进行替换；\n\n(3) tbname：符合 TDengine 子表命名规范的任何字符串；如果存在特殊字符.，使用下划线替换；如果存在{tag_name}，使用 tag_name 替换；\n\n(4) enable：0，不采集该点位，且在 OPC DataIn 任务开始前，删除 TDengine 中点位对应的子表；1，采集该点位，在 OPC DataIn 任务开始前，不删除子表。\n\n(5) value_col：符合 TDengine 命名规范的列名\n\n(6) value_transform：符合 Rhai 引擎的计算表达式，例如：(val + 10) / 1000 * 2.0，log(val) + 10等；\n\n(7) type：支持类型包括：b/bool，i8/tinyint，i16/smallint，i32/int，i64/bigint，u8/tinyint unsigned，u16/smallint unsigned，u32/int unsigned，u64/bigint unsigned，f32/float，f64/double，timestamp/timestamp(ms)，timestamp(us)，timestamp(ns)，json\n\n(8) quality_col：符合 TDengine 命名规范的列名\n\n(9) ts_col：符合 TDengine 命名规范的列名\n\n(10) received_ts_col：符合 TDengine 命名规范的列名\n\n(11) ts_transform 和 received_ts_transform：支持 +、-、*、/、% 操作符，例如：ts / 1000 * 1000，将一个 ms 单位的时间戳的最后 3 位置为 0；ts + 8 * 3600 * 1000，将一个 ms 精度的时间戳，增加 8 小时；ts - 8 * 3600 * 1000，将一个 ms 精度的时间戳，减去 8 小时；\n\n(12) tag::VARCHAR(200)::name：tag 里的值，当 tag 的类型是 VARCHAR 时，可以是中文。\n\n同时，多个Row之间还需要满足：\n\n(13) tag_name 在整个 DataIn 任务中是唯一的，即：在一个 OPC DataIn 任务中，一个数据点位只能被写入到 TDengine 的一张子表。如果需要将一个数据点位写入多张子表，需要建多个 OPC DataIn 任务；\n\n(14) 当 tag_name 不同，但 tbname 相同时，value_col 必须不同。这种配置能够将不同数据类型的多个点位的数据写入同一张子表中不同的列。这种方式对应 “OPC 数据入 TDengine 宽表”的使用场景。\n\n4.其他规则\n\n(1) 如果 Header 和 Row 的列数不一致，校验失败，提示用户不满足要求的行号；\n\n(2) Header 在首行，且不能为空；\n\n(3) Row 为 1 行以上；\n",
            "target": {
              "name": "csv_config_file",
              "required": true,
              "multiple": true,
              "editable": true,
              "selectable": true
            }
          },
          {
            "category": "select_all_points",
            "display": "选择数据点位",
            "target": {
              "name": "select_all_points",
              "description": "设置过滤条件，选择 OPC 服务器上满足指定条件的数据点位。\n",
              "required": true,
              "multiple": true,
              "editable": true,
              "selectable": true
            },
            "params": [
              {
                "name": "root",
                "display": "根节点",
                "hint": {
                  "type": "str"
                },
                "description": "从该节点开始查询所有子节点, 多级父节点间用“.”相连接。\n",
                "placeholder": "例如 root.parent"
              },
              {
                "name": "pattern",
                "display": "正则匹配",
                "hint": {
                  "type": "str"
                },
                "description": "数据点位 TagName 需要满足设置的正则表达式。\n"
              },
              {
                "name": "table_primary_key",
                "display": "主键列",
                "hint": {
                  "type": "str",
                  "choices": [
                    "received_ts",
                    "original_ts"
                  ]
                },
                "description": "目标数据表主键将使用选择的值作为时间戳主键名称，original_ts 表示使用数据点位上报 OPC 服务时间，received_ts 表示 taosX 任务接收数据的时间。\n",
                "required": true,
                "value": "original_ts"
              },
              {
                "name": "child_table_expression",
                "display": "表名称",
                "hint": {
                  "type": "str"
                },
                "description": "支持 <child table prefix>_{TagName} 的格式\n",
                "required": true,
                "value": "t_{TagName}"
              }
            ]
          }
        ]
      }
    },
    {
      "id": "influxdb",
      "type": "uri",
      "name": "InfluxDB",
      "description": "InfluxDB 是一种流行的开源时间序列数据库，它针对处理大量时间序列数据进行了优化。\n\nTDengine 可以通过 InfluxDB 连接器高效地读取 InfluxDB 中的数据，并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n",
      "options": {
        "host": {
          "required": true,
          "display": "服务器地址",
          "description": "InfluxDB 数据库的 IP 地址或域名。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。",
          "placeholder": "127.0.0.1"
        },
        "port": {
          "required": true,
          "display": "端口",
          "description": "InfluxDB 数据库的服务端口。",
          "placeholder": "8086"
        },
        "subject": {}
      },
      "protocol": {
        "display": "连接协议",
        "description": "InfluxDB 数据库的连接协议，请按实际情况选择，否则无法正常运行任务。",
        "choices": [
          {
            "name": "http",
            "display": "HTTP 协议"
          },
          {
            "name": "https",
            "display": "HTTPS 协议"
          }
        ],
        "value": "http"
      },
      "authentication": {
        "display": "认证",
        "description": "InfluxDB 的鉴权认证。",
        "value": "2.x",
        "alternatives": [
          {
            "name": "1.x",
            "display": "1.x 版本",
            "params": [
              {
                "name": "version",
                "display": "版本",
                "hint": {
                  "type": "str",
                  "choices": [
                    "1.8",
                    "1.7"
                  ]
                },
                "description": "InfluxDB 数据库的版本，由于版本之间存在接口差异，所以请按实际情况选择。",
                "required": true,
                "placeholder": "请选择 InfluxDB 版本"
              },
              {
                "name": "username",
                "display": "用户",
                "hint": "str",
                "description": "InfluxDB 数据库的用户，该用户必须在该组织中拥有读取权限。",
                "required": true,
                "placeholder": "请输入 InfluxDB 用户"
              },
              {
                "name": "password",
                "display": "密码",
                "hint": "str",
                "description": "InfluxDB 数据库中用户的登陆密码。",
                "required": true,
                "placeholder": "请输入登陆密码"
              }
            ]
          },
          {
            "name": "2.x",
            "display": "版本 2.x",
            "params": [
              {
                "name": "version",
                "display": "版本",
                "hint": {
                  "type": "str",
                  "choices": [
                    "2.7",
                    "2.6",
                    "2.5",
                    "2.4",
                    "2.3",
                    "2.2",
                    "2.1",
                    "2.0"
                  ]
                },
                "description": "InfluxDB 数据库的版本，由于版本之间存在接口差异，所以请按实际情况选择。",
                "required": true,
                "placeholder": "请选择 InfluxDB 版本"
              },
              {
                "name": "orgId",
                "display": "组织 ID",
                "hint": "str",
                "description": "InfluxDB 数据库的组织 ID, 它是一个由十六进制字符组成的字符串，而不是组织名称，可以从 InfluxDB 控制台的Organization -> About页面获取。",
                "required": true,
                "placeholder": "请输入 InfluxDB 组织 ID"
              },
              {
                "name": "token",
                "display": "令牌 Token",
                "hint": "str",
                "description": "InfluxDB 数据库的访问令牌，该令牌必须在该组织中拥有读取权限。",
                "required": true,
                "placeholder": "请输入 InfluxDB 令牌"
              },
              {
                "name": "addDbrp",
                "display": "添加数据库保留策略",
                "hint": {
                  "type": "bool"
                },
                "description": "InfluxQL 需要数据库与保留策略（DBRP）的组合才能查询数据，InfluxDB 的 Cloud 版本及某些 2.x 版本需要人工添加这个映射关系，打开这个开关，连接器可以在执行任务时自动添加。",
                "value": "false"
              }
            ]
          }
        ]
      },
      "groups": [
        {
          "name": "task",
          "display": "任务设置",
          "display_order": 1,
          "short_description": "配置同步任务的数据集、时间范围与性能参数等内容。",
          "description": "配置同步任务的数据集、时间范围与性能参数等内容。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "bucket",
              "display": "桶 Bucket",
              "hint": {
                "type": "str",
                "choices": [
                  "--NONE--"
                ]
              },
              "short_description": "InfluxDB 数据库中的 Bucket，是存储数据的一个命名空间，每个任务需要指定一个 Bucket。",
              "description": "InfluxDB 数据库中的 Bucket，是存储数据的一个命名空间，每个任务需要指定一个 Bucket。",
              "required": true,
              "placeholder": "请选择 Bucket"
            },
            {
              "name": "measurements",
              "display": "测量值 Measurements",
              "hint": {
                "type": "str",
                "choices": [
                  "--NONE--"
                ]
              },
              "short_description": "Bucket 中的测量值，可以指定多个需要同步的 Measurements，未指定则同步该 Bucket 中的全部数据。",
              "description": "Bucket 中的测量值，可以指定多个需要同步的 Measurements，未指定则同步该 Bucket 中的全部数据。",
              "multiple": true,
              "editable": true,
              "placeholder": "请选择 Measurements"
            },
            {
              "name": "beginTime",
              "display": "起始时间",
              "hint": "time",
              "short_description": "数据的起始时间，同步任务仅读取该指定时间及之后的数据。",
              "description": "数据的起始时间，同步任务仅读取该指定时间及之后的数据。",
              "required": true,
              "placeholder": "YYYY-MM-DD HH:mm:ss"
            },
            {
              "name": "endTime",
              "display": "结束时间",
              "hint": "time",
              "short_description": "数据的截止时间，同步任务仅读取该指定时间及之前的数据，如果指定未来时间，任务将持续进行直至到达截止时间，如果未指定，任务将持续进行直至人为结束。",
              "description": "数据的截止时间，同步任务仅读取该指定时间及之前的数据，如果指定未来时间，任务将持续进行直至到达截止时间，如果未指定，任务将持续进行直至人为结束。",
              "placeholder": "YYYY-MM-DD HH:mm:ss"
            },
            {
              "name": "readWindow",
              "display": "每次读取的时间范围（分钟）",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 6000
              },
              "short_description": "每次从 InfluxDB 读取数据时，最大的时间范围。",
              "description": "每次从 InfluxDB 读取数据时，最大的时间范围。",
              "placeholder": "请输入读取时间范围",
              "value": "60"
            },
            {
              "name": "delay",
              "display": "延迟（秒）",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 30
              },
              "short_description": "为了消除乱序数据的影响，TDengine 总是等待这里指定的时长，然后才读取数据。",
              "description": "为了消除乱序数据的影响，TDengine 总是等待这里指定的时长，然后才读取数据。",
              "placeholder": "请输入延迟时长",
              "value": "10"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "log_level",
            "display": "日志级别",
            "hint": {
              "type": "str",
              "choices": [
                "error",
                "warn",
                "info",
                "debug",
                "trace"
              ]
            },
            "description": "根据需要调整数据源的日志级别，此参数不总是生效。",
            "value": "info"
          },
          {
            "name": "read_concurrency",
            "display": "最大读取并发数",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 100
            },
            "description": "数据源连接数或读取线程数限制，当默认参数不满足需要或需要调整资源使用量时修改此参数。\n",
            "value": "50"
          },
          {
            "name": "write_concurrency",
            "display": "最大写入并发数",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 500
            },
            "description": "写入 taosX 的最大并发数限制，当默认参数性能不足时，可增大此参数。\n",
            "value": "50"
          },
          {
            "name": "batch_size",
            "display": "批次大小",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 10000
            },
            "description": "单次发送的最大消息数或行数。\n",
            "value": "5000"
          },
          {
            "name": "batch_timeout",
            "display": "批次延时",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 10000
            },
            "description": "单次读取最大延时（单位为毫秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n",
            "value": "1000",
            "hidden": true
          }
        ]
      }
    },
    {
      "id": "opentsdb",
      "type": "uri",
      "name": "OpenTSDB",
      "description": "OpenTSDB 是一个架构在 HBase 系统之上的实时监控信息收集和展示平台。\n\nTDengine 可以通过 OpenTSDB 连接器高效地读取 OpenTSDB 中的数据，并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n",
      "options": {
        "host": {
          "required": true,
          "display": "服务器地址",
          "description": "OpenTSDB 数据库的 IP 地址或域名。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。",
          "placeholder": "127.0.0.1"
        },
        "port": {
          "required": true,
          "display": "端口",
          "description": "OpenTSDB 数据库的服务端口。",
          "placeholder": "4242"
        },
        "subject": {}
      },
      "protocol": {
        "display": "连接协议",
        "description": "OpenTSDB 数据库的连接协议，请按实际情况选择，否则无法正常运行任务。",
        "choices": [
          {
            "name": "http",
            "display": "HTTP 协议"
          },
          {
            "name": "https",
            "display": "HTTPS 协议"
          }
        ],
        "value": "http"
      },
      "groups": [
        {
          "name": "task",
          "display": "任务设置",
          "display_order": 1,
          "short_description": "配置同步任务的数据集、时间范围与性能参数等内容。",
          "description": "配置同步任务的数据集、时间范围与性能参数等内容。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "metrics",
              "display": "物理量 Metrics",
              "hint": {
                "type": "str",
                "choices": [
                  "--NONE--"
                ]
              },
              "short_description": "OpenTSDB 中的物理量，可以指定多个需要同步的 Metrics，未指定则同步数据库中的全部数据。",
              "description": "OpenTSDB 中的物理量，可以指定多个需要同步的 Metrics，未指定则同步数据库中的全部数据。",
              "multiple": true,
              "editable": true,
              "placeholder": "请选择 Metrics"
            },
            {
              "name": "beginTime",
              "display": "起始时间",
              "hint": "time",
              "short_description": "数据的起始时间，同步任务仅读取该指定时间及之后的数据。",
              "description": "数据的起始时间，同步任务仅读取该指定时间及之后的数据。",
              "required": true,
              "placeholder": "YYYY-MM-DD HH:mm:ss"
            },
            {
              "name": "endTime",
              "display": "结束时间",
              "hint": "time",
              "short_description": "数据的截止时间，同步任务仅读取该指定时间及之前的数据，如果指定未来时间，任务将持续进行直至到达截止时间，如果未指定，任务将持续进行直至人为结束。",
              "description": "数据的截止时间，同步任务仅读取该指定时间及之前的数据，如果指定未来时间，任务将持续进行直至到达截止时间，如果未指定，任务将持续进行直至人为结束。",
              "placeholder": "YYYY-MM-DD HH:mm:ss"
            },
            {
              "name": "readWindow",
              "display": "每次读取的时间范围（分钟）",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 6000
              },
              "short_description": "每次从 OpenTSDB 读取数据时，最大的时间范围。",
              "description": "每次从 OpenTSDB 读取数据时，最大的时间范围。",
              "placeholder": "请输入读取时间范围",
              "value": "60"
            },
            {
              "name": "delay",
              "display": "延迟（秒）",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 30
              },
              "short_description": "为了消除乱序数据的影响，TDengine 总是等待这里指定的时长，然后才读取数据。",
              "description": "为了消除乱序数据的影响，TDengine 总是等待这里指定的时长，然后才读取数据。",
              "placeholder": "请输入延迟时长",
              "value": "10"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "log_level",
            "display": "日志级别",
            "hint": {
              "type": "str",
              "choices": [
                "error",
                "warn",
                "info",
                "debug",
                "trace"
              ]
            },
            "description": "根据需要调整数据源的日志级别，此参数不总是生效。",
            "value": "info"
          },
          {
            "name": "read_concurrency",
            "display": "最大读取并发数",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 100
            },
            "description": "数据源连接数或读取线程数限制，当默认参数不满足需要或需要调整资源使用量时修改此参数。\n",
            "value": "50"
          },
          {
            "name": "write_concurrency",
            "display": "最大写入并发数",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 500
            },
            "description": "写入 taosX 的最大并发数限制，当默认参数性能不足时，可增大此参数。\n",
            "value": "50"
          },
          {
            "name": "batch_size",
            "display": "批次大小",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 10000
            },
            "description": "单次发送的最大消息数或行数。\n",
            "value": "5000"
          },
          {
            "name": "batch_timeout",
            "display": "批次延时",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 10000
            },
            "description": "单次读取最大延时（单位为秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n",
            "value": "1000",
            "hidden": true
          }
        ]
      }
    },
    {
      "id": "mqtt",
      "type": "uri",
      "name": "MQTT",
      "description": "MQTT 表示 Message Queuing Telemetry Transport （消息队列遥测传输）。它是一种轻量级的消息协议，易于实现和使用。它非常适合连接资源有限的设备，例如电池供电的设备或带宽较低的设备。MQTT也是实时控制系统等延迟重要的应用程序的不错选择。\n\nMQTT 通过使用发布/订阅模型来工作。这意味着设备可以将消息发布到主题，其他设备可以订阅这些主题以接收消息。这使得轻松将设备解耦，并根据需要扩展应用程序。\n\nMQTT 是物联网应用程序的流行选择。它得到了广泛的设备和平台支持，并提供许多开源和商业实现。\n\ntaosX 可以通过连接器插件从 MQTT 代理订阅数据。请查看每个部分的帮助消息以了解详细信息。\n",
      "options": {
        "endpoint": {
          "required": true,
          "display": "MQTT 地址",
          "description": "MQTT 服务器地址。如: “127.0.0.1:1883”\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。\n",
          "placeholder": "127.0.0.1:1883",
          "pattern": "^[0-9A-Za-z.]+:(?:[0-9]{1,5})$",
          "patternMsg": "输入格式有误，请按照格式 `host:port`，port 范围为 1-65535。"
        }
      },
      "authentication": {
        "display": "认证",
        "description": "使用用户名和密码访问 MQTT Broker。",
        "value": "plain",
        "alternatives": [
          {
            "name": "plain",
            "display": "用户名密码访问",
            "username": {
              "display": "用户",
              "placeholder": "username"
            },
            "password": {
              "display": "密码",
              "placeholder": "password"
            }
          }
        ]
      },
      "groups": [
        {
          "name": "SSL 证书",
          "short_description": "使用证书和私钥建立连接以启用 SSL。",
          "description": "使用证书和私钥建立连接以启用 SSL。",
          "collapsible": true,
          "connection_option": true,
          "collapsed": false,
          "params": [
            {
              "name": "ca",
              "display": "CA",
              "hint": {
                "type": "file"
              },
              "short_description": "CA 证书文件",
              "description": "CA 证书文件",
              "required": true
            },
            {
              "name": "cert",
              "display": "客户端证书",
              "hint": {
                "type": "file"
              },
              "short_description": ".cert 文件",
              "description": ".cert 文件",
              "required": true
            },
            {
              "name": "cert_key",
              "display": "客户端私钥",
              "hint": {
                "type": "file"
              },
              "short_description": "私钥文件",
              "description": "私钥文件",
              "required": true
            }
          ]
        },
        {
          "name": "采集配置",
          "display_order": 1,
          "short_description": "采集任务配置",
          "description": "采集任务配置",
          "collapsible": false,
          "connection_option": true,
          "params": [
            {
              "name": "version",
              "display": "MQTT 协议",
              "hint": {
                "type": "str",
                "choices": [
                  "3.1",
                  "3.1.1",
                  "5.0"
                ]
              },
              "short_description": "MQTT 协议版本。",
              "description": "MQTT 协议版本。",
              "required": true,
              "value": "3.1"
            },
            {
              "name": "client_id",
              "display": "Client ID",
              "hint": {
                "type": "str"
              },
              "short_description": "MQTT Broker 客户端 ID。",
              "description": "MQTT Broker 客户端 ID。",
              "placeholder": "client_id"
            },
            {
              "name": "keep_alive",
              "display": "Keep Alive",
              "hint": {
                "type": "integer",
                "min": 1
              },
              "short_description": "如果代理在保持活动间隔内没有收到来自客户端的任何消息，它将假定客户端已断开连接，并关闭连接。",
              "description": "如果代理在保持活动间隔内没有收到来自客户端的任何消息，它将假定客户端已断开连接，并关闭连接。\n\n保持活动间隔是指客户端和代理之间协商的时间间隔，用于检测客户端是否活动。如果客户端在保持活动间隔内没有向代理发送消息，则代理将断开连接。\n\n保持活动间隔的默认值为60秒，但可以通过在连接时设置 CONNECT 报文中的 keep alive 字段来更改它。\n",
              "placeholder": "10",
              "value": "60"
            },
            {
              "name": "clean_session",
              "display": "Clean Session",
              "hint": {
                "type": "bool"
              },
              "short_description": "如果clean session标志设置为True，则代理将忘记有关会话的所有信息，包括客户端的订阅。",
              "description": "如果clean session标志设置为True，则代理将忘记有关会话的所有信息，包括客户端的订阅。<br>\nclean session 标志的默认值为True。<br>\n如果设置为False，则代理将保留有关客户端的信息，包括其订阅。这意味着客户端在重新连接时可以恢复其以前的订阅。<br>\n",
              "value": "true"
            },
            {
              "name": "topics",
              "display": "订阅主题及 QoS 配置",
              "hint": {
                "type": "str"
              },
              "short_description": "输入格式 `<topic name>::<QoS>`，其中QoS 只能输入0、1、2，订阅多个主题使用逗号分割，例如: `topic1::0,topic2::1`",
              "description": "输入格式 `<topic name>::<QoS>`，其中QoS 只能输入0、1、2，订阅多个主题使用逗号分割，例如: `topic1::0,topic2::1`\n",
              "required": true,
              "pattern": "^(?:\\S+::[0-2],)*\\S+::[0-2]$",
              "patternMsg": "输入格式有误，请按照格式 `<topic name>::<QoS>`，其中QoS 只能输入0、1、2，例如： `topic1::0,topic2::1`",
              "placeholder": "topic1::0,topic2::1"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "log_level",
            "display": "日志级别",
            "hint": {
              "type": "str",
              "choices": [
                "error",
                "warn",
                "info",
                "debug",
                "trace"
              ]
            },
            "description": "根据需要调整数据源的日志级别，此参数不总是生效。",
            "value": "info"
          },
          {
            "name": "read_concurrency",
            "display": "最大读取并发数",
            "hint": {
              "type": "integer",
              "min": 0,
              "max": 1000
            },
            "description": "数据源连接数或读取线程数限制，当默认参数不满足需要或需要调整资源使用量时修改此参数。\n",
            "value": "0",
            "hidden": true
          },
          {
            "name": "write_concurrency",
            "display": "最大写入并发数",
            "hint": {
              "type": "integer",
              "min": 0,
              "max": 1000
            },
            "description": "写入 taosX 的最大并发数限制，当默认参数性能不足时，可增大此参数。\n",
            "value": "0",
            "hidden": true
          },
          {
            "name": "batch_size",
            "display": "批次大小",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 10000
            },
            "description": "单次发送的最大消息数或行数。\n",
            "value": "1000",
            "hidden": true
          },
          {
            "name": "batch_timeout",
            "display": "批次延时",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 60
            },
            "description": "单次读取最大延时（单位为毫秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。\n",
            "value": "1",
            "hidden": true
          },
          {
            "name": "keep_raw_data",
            "display": "保存原始数据",
            "hint": {
              "type": "bool"
            },
            "description": "是否保存原始数据？\n"
          },
          {
            "name": "keep_raw_data_days",
            "display": "最大保留天数",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 365
            },
            "description": "原始数据最大保存天数，默认 1 天。\n",
            "value": "1",
            "requires": "keep_raw_data"
          },
          {
            "name": "keep_raw_data_dir",
            "display": "原始数据存储目录",
            "hint": {
              "type": "str"
            },
            "description": "自定义原始数据存储目录，默认存储到系统数据目录下。\n",
            "placeholder": "$DATA_DIR/tasks/:id/rawdata/",
            "requires": "keep_raw_data"
          }
        ]
      },
      "parser": {
        "display": "Payload 转换",
        "required": true,
        "description": "MQTT 连接器会上传以下四列到服务端：\n\n- **ts**: 采集时间戳。\n- **topic**: 订阅主题名。\n- **qos**: 采集点质量。\n- **payload**: 采集数据。\n\ntaosX 可以使用 JSON 提取器解析数据，并允许用户在数据库中指定数据模型，包括，指定表名称和超级表名，设置普通列和标签列等。\n",
        "fields": [
          {
            "name": "ts",
            "description": "时间戳",
            "type": "timestamp"
          },
          {
            "name": "topic",
            "description": "主题",
            "type": "varchar"
          },
          {
            "name": "qos",
            "description": "质量",
            "type": "int"
          },
          {
            "name": "payload",
            "description": "负载",
            "type": "varchar"
          }
        ]
      }
    },
    {
      "id": "kafka",
      "type": "uri",
      "name": "Kafka",
      "description": "Apache Kafka 是一个用于流处理、实时数据管道和大规模数据集成的开源分布式流系统。\nTDengine 可以高效地从 Kafka 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据流入库。\n",
      "options": {
        "endpoint": {
          "required": true,
          "display": "bootstrap-servers",
          "description": "Kafka Server 地址。\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。\n",
          "placeholder": "127.0.0.1:9092"
        }
      },
      "groups": [
        {
          "name": "SSL 证书",
          "display_order": 1,
          "short_description": "使用证书和私钥建立连接以启用 SSL。",
          "description": "使用证书和私钥建立连接以启用 SSL。",
          "collapsible": true,
          "connection_option": true,
          "collapsed": false,
          "params": [
            {
              "name": "ca",
              "display": "CA",
              "hint": {
                "type": "file"
              },
              "short_description": "CA 证书文件",
              "description": "CA 证书文件",
              "required": true
            },
            {
              "name": "cert",
              "display": "客户端证书",
              "hint": {
                "type": "file"
              },
              "short_description": ".cert 文件",
              "description": ".cert 文件",
              "required": true
            },
            {
              "name": "cert_key",
              "display": "客户端私钥",
              "hint": {
                "type": "file"
              },
              "short_description": "私钥文件",
              "description": "私钥文件",
              "required": true
            }
          ]
        },
        {
          "name": "采集配置",
          "display_order": 2,
          "short_description": "数据采集相关配置项。",
          "description": "数据采集相关配置项。",
          "collapsible": false,
          "connection_option": true,
          "params": [
            {
              "name": "timeout",
              "display": "超时时间",
              "hint": {
                "type": "str"
              },
              "short_description": "指定 Kafka Source 的超时时间，当从 Kafka 消费不到任何数据，超过 timeout 后，数据采集任务会退出。 默认值是 500 ms。 当 timeout 设置为 `never` 时，Kafka Source 会一直等待，直到有数据可用，或者发生错误。",
              "description": "指定 Kafka Source 的超时时间，当从 Kafka 消费不到任何数据，超过 timeout 后，数据采集任务会退出。 默认值是 500 ms。 当 timeout 设置为 `never` 时，Kafka Source 会一直等待，直到有数据可用，或者发生错误。\n",
              "required": false,
              "placeholder": "500"
            },
            {
              "name": "topics",
              "display": "主题",
              "hint": {
                "type": "str"
              },
              "short_description": "指定要消费的 Topic。可以配置多个 Topic，Topic 之间使用逗号分隔，例如：`tp1,tp2`。",
              "description": "指定要消费的 Topic。可以配置多个 Topic，Topic 之间使用逗号分隔，例如：`tp1,tp2`。\n",
              "required": true,
              "placeholder": "tp1,tp2"
            },
            {
              "name": "fallback_offset",
              "display": "Offset",
              "hint": {
                "type": "str",
                "choices": [
                  "Earliest",
                  "Latest",
                  "ByTime"
                ]
              },
              "short_description": "Fallback Offset 参数可以指定以下值：",
              "description": "Fallback Offset 参数可以指定以下值：\n* `Earliest`：用于请求最早的 offset. * `Latest`：用于请求最晚的 offset. * `ByTime`：用于请求在特定时间（毫秒）之前的所有消息; 时间戳为毫秒精度。\n默认值为Earliest。\n",
              "required": false,
              "placeholder": "Earliest",
              "value": "Earliest"
            },
            {
              "name": "fetch_max_wait_time",
              "display": "获取数据的最大时长",
              "hint": {
                "type": "str"
              },
              "short_description": "设置获取消息时等待数据不足的最长时间（以毫秒为单位），默认值为 100ms。",
              "description": "设置获取消息时等待数据不足的最长时间（以毫秒为单位），默认值为 100ms。\n",
              "required": false,
              "placeholder": "100ms"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "read_concurrency",
            "display": "最大读取并发数",
            "hint": {
              "type": "integer",
              "min": 0,
              "max": 1000
            },
            "description": "数据源连接数或读取线程数限制，当默认参数不满足需要或需要调整资源使用量时修改此参数。\n",
            "value": "0"
          }
        ]
      },
      "parser": {
        "display": "Payload 转换",
        "required": true,
        "description": "Kafka 连接器会上传以下六列到服务端：<br>\n\n- **ts**: 采集时间戳。<br>\n- **topic**: 订阅主题名。<br>\n- **partition**: 当前消息所在的分区 ID。<br>\n- **offset**: 当前消息的偏移量。<br>\n- **key**: 当前消息的 Key。<br>\n- **value**: 当前消息的数据内容。<br>\n\ntaosX 可以使用 JSON 提取器解析数据，并允许用户在数据库中指定数据模型，<br>\n包括，指定表名称和超级表名，设置普通列和标签列等。\n",
        "fields": [
          {
            "name": "ts",
            "description": "时间戳。",
            "type": "timestamp"
          },
          {
            "name": "topic",
            "description": "主题名。",
            "type": "varchar"
          },
          {
            "name": "partition",
            "description": "分区 ID。",
            "type": "int"
          },
          {
            "name": "offset",
            "description": "偏移。",
            "type": "bigint"
          },
          {
            "name": "key",
            "description": "消息 Key。",
            "type": "varchar"
          },
          {
            "name": "value",
            "description": "消息体。",
            "type": "varchar"
          }
        ]
      }
    },
    {
      "id": "csv",
      "type": "path",
      "name": "CSV",
      "description": "导入一个或多个 CSV 文件数据到 TDengine。\n",
      "strict": true,
      "options": {
        "path": {
          "required": true,
          "display": "Path",
          "description": "CSV 文件名或文件路径（处理该路径下的所有 CSV 文件）。",
          "placeholder": "示例: a.csv,b.csv"
        }
      },
      "groups": [
        {
          "name": "CSV 选项",
          "display_order": 1,
          "short_description": "CSV 读取选项",
          "description": "CSV 读取选项",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "has_header",
              "display": "包含表头",
              "hint": {
                "type": "bool"
              },
              "short_description": "如果包含表头，则第一行将被视为列信息。",
              "description": "如果包含表头，则第一行将被视为列信息。\n"
            },
            {
              "name": "skip",
              "display": "忽略前 N 行",
              "hint": {
                "type": "integer",
                "min": 0
              },
              "short_description": "忽略 CSV 文件的前 N 行。",
              "description": "忽略 CSV 文件的前 N 行。",
              "value": "0"
            },
            {
              "name": "delimiter",
              "display": "字段分隔符",
              "hint": {
                "type": "str",
                "choices": [
                  ",",
                  ";"
                ]
              },
              "short_description": "CSV 字段之间的分隔符。",
              "description": "CSV 字段之间的分隔符。",
              "editable": true,
              "value": ","
            },
            {
              "name": "quote",
              "display": "字段引用符",
              "hint": {
                "type": "str",
                "choices": [
                  "\"",
                  "'"
                ]
              },
              "short_description": "当 CSV 字段中包含分隔符或换行符时，用于包围字段内容，以确保整个字段被正确识别。",
              "description": "当 CSV 字段中包含分隔符或换行符时，用于包围字段内容，以确保整个字段被正确识别。",
              "editable": true,
              "value": "\""
            },
            {
              "name": "comment",
              "display": "注释前缀符",
              "hint": {
                "type": "str",
                "choices": [
                  "#"
                ]
              },
              "short_description": "当 CSV 文件中某行以此处指定的字符开头，则忽略该行。",
              "description": "当 CSV 文件中某行以此处指定的字符开头，则忽略该行。",
              "editable": true,
              "value": "#"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "read_concurrency",
            "display": "最大读取并发数",
            "hint": {
              "type": "integer",
              "min": 0,
              "max": 1000
            },
            "description": "数据源连接数或读取线程数限制，当默认参数不满足需要或需要调整资源使用量时修改此参数。\n",
            "value": "0"
          },
          {
            "name": "batch_size",
            "display": "批次大小",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 100000
            },
            "description": "单次发送的最大消息数或行数。\n",
            "value": "1000"
          }
        ]
      }
    },
    {
      "id": "avevaHistorian",
      "type": "uri",
      "name": "AVEVA Historian",
      "description": "AVEVA Historian 是一款工业大数据分析软件，前身为 Wonderware。可以捕获并存储高保真工业大数据，释放受制约的潜力，从而改善运营。\nTDengine 可以高效地从 AVEVA Historian 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n",
      "options": {
        "host": {
          "required": true,
          "display": "Server 地址",
          "description": "AVEVA Historian SQL Server 的 IP 地址或域名",
          "placeholder": "127.0.0.1"
        },
        "port": {
          "display": "Server 端口",
          "description": "AVEVA Historian SQL Server 的端口",
          "placeholder": "1433"
        },
        "subject": {}
      },
      "authentication": {
        "display": "认证",
        "description": "使用用户名和密码访问 AVEVA Historian SQL Server",
        "value": "plain",
        "alternatives": [
          {
            "name": "plain",
            "display": "用户名密码访问",
            "username": {
              "required": true,
              "display": "用户",
              "placeholder": "username"
            },
            "password": {
              "required": true,
              "display": "密码",
              "placeholder": "password"
            }
          }
        ]
      },
      "groups": [
        {
          "name": "采集配置",
          "display_order": 1,
          "short_description": "数据采集相关配置项。",
          "description": "数据采集相关配置项。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "mode",
              "display": "采集模式",
              "hint": {
                "type": "str",
                "choices": [
                  "synchronize",
                  "migrate"
                ]
              },
              "short_description": "采集模式，可选值为 `synchronize` 和 `migrate`。",
              "description": "采集模式，可选值为 `synchronize` 和 `migrate`。\n",
              "required": true,
              "placeholder": "synchronize",
              "value": "synchronize"
            },
            {
              "name": "table",
              "display": "表",
              "hint": {
                "type": "str",
                "choices": [
                  "Runtime.dbo.History",
                  "Runtime.dbo.Live"
                ]
              },
              "short_description": "检索 historian 中的数据库表，历史数据在 Runtime.dbo.History 中，实时数据在 Runtime.dbo.Live 中。",
              "description": "检索 historian 中的数据库表，历史数据在 Runtime.dbo.History 中，实时数据在 Runtime.dbo.Live 中。\n",
              "required": true,
              "placeholder": "Runtime.dbo.History"
            },
            {
              "name": "tags",
              "display": "标签",
              "hint": {
                "type": "str"
              },
              "short_description": "需要迁移/同步的tag，`*`代表除了Sys开头以外的全部tag。",
              "description": "需要迁移/同步的tag，`*`代表除了Sys开头以外的全部tag。\n",
              "required": false,
              "placeholder": "*",
              "value": "*"
            },
            {
              "name": "tagListSize",
              "display": "标签组大小",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 1000
              },
              "short_description": "当 `table` 为 `Runtime.dbo.History` 且 `tags` 中的 TagName 超过 `tagListSize` 时，tags 被按照每组 tagListSize 个进行划分。 使用 `tagListSize` 划分 TagName 是为了提高数据迁移/同步时的查询效率。`tagListSize` 默认值为 10。",
              "description": "当 `table` 为 `Runtime.dbo.History` 且 `tags` 中的 TagName 超过 `tagListSize` 时，tags 被按照每组 tagListSize 个进行划分。 使用 `tagListSize` 划分 TagName 是为了提高数据迁移/同步时的查询效率。`tagListSize` 默认值为 10。\n",
              "required": false,
              "placeholder": "10",
              "value": "10"
            },
            {
              "name": "beginDateTime",
              "display": "任务开始时间",
              "hint": {
                "type": "time"
              },
              "short_description": "任务的开始时间，rfc3339格式的日期时间。",
              "description": "任务的开始时间，rfc3339格式的日期时间。\n",
              "required": true,
              "placeholder": "如：2023-01-01T00:00:00+08:00"
            },
            {
              "name": "endDateTime",
              "display": "任务结束时间",
              "hint": {
                "type": "time"
              },
              "short_description": "任务的结束时间，rfc3339格式的日期时间。",
              "description": "任务的结束时间，rfc3339格式的日期时间。\n",
              "required": false,
              "placeholder": "如：2023-01-01T00:00:00+08:00"
            },
            {
              "name": "timeWindow",
              "display": "查询的时间窗口",
              "hint": {
                "type": "str"
              },
              "short_description": "历史数据迁移时，每次查询的时间窗口。",
              "description": "历史数据迁移时，每次查询的时间窗口。\n",
              "required": false,
              "placeholder": "1 day",
              "value": "1 day"
            },
            {
              "name": "retrieveInterval",
              "display": "实时同步的时间间隔",
              "hint": {
                "type": "str"
              },
              "short_description": "实时数据同步时，每次查询的时间间隔。",
              "description": "实时数据同步时，每次查询的时间间隔。\n",
              "required": false,
              "placeholder": "10s",
              "value": "10s"
            },
            {
              "name": "tolerance",
              "display": "乱序时间上限",
              "hint": {
                "type": "str"
              },
              "short_description": "容忍乱序数据延迟到达的时间上限。",
              "description": "容忍乱序数据延迟到达的时间上限。\n",
              "required": false,
              "placeholder": "0 ms",
              "value": "0 ms"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "read_concurrency",
            "display": "最大读取并发数",
            "hint": {
              "type": "integer",
              "min": 0,
              "max": 1000
            },
            "description": "数据源连接数或读取线程数限制，当默认参数不满足需要或需要调整资源使用量时修改此参数。\n",
            "value": "0"
          },
          {
            "name": "batch_size",
            "display": "批次大小",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 100000
            },
            "description": "单次发送的最大消息数或行数。\n",
            "value": "10000"
          },
          {
            "name": "keep_raw_data",
            "display": "保存原始数据",
            "hint": {
              "type": "bool"
            },
            "description": "是否保存原始数据？\n"
          },
          {
            "name": "keep_raw_data_days",
            "display": "最大保留天数",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 365
            },
            "description": "原始数据最大保存天数，默认 1 天。\n",
            "value": "1",
            "requires": "keep_raw_data"
          },
          {
            "name": "keep_raw_data_dir",
            "display": "原始数据存储目录",
            "hint": {
              "type": "str"
            },
            "description": "自定义原始数据存储目录，默认存储到系统数据目录下。\n",
            "placeholder": "$DATA_DIR/tasks/:id/rawdata/",
            "requires": "keep_raw_data"
          }
        ]
      },
      "parser": {
        "display": "数据映射",
        "required": true,
        "description": "taosX 允许用户在数据库中指定数据模型，包括：指定表名称和超级表名，设置普通列和标签列等\n",
        "fields": [
          {
            "name": "DateTime",
            "description": "值对应的时间戳。",
            "type": "timestamp"
          },
          {
            "name": "TagName",
            "description": "测点名称。",
            "type": "varchar"
          },
          {
            "name": "Value",
            "description": "标记在时间戳处的值。对于字符串tag，该值始终为NULL。",
            "type": "double"
          },
          {
            "name": "vValue",
            "description": "字符串形式的值，在查询中使用此列允许您使用混合数据类型的值。",
            "type": "varchar"
          },
          {
            "name": "Quality",
            "description": "与数据值相关联的基本数据质量指标。",
            "type": "int"
          },
          {
            "name": "QualityDetail",
            "description": "数据质量的内部表示。",
            "type": "int"
          },
          {
            "name": "OPCQuality",
            "description": "从数据源接收到的质量值。",
            "type": "int"
          },
          {
            "name": "wwTagKey",
            "description": "单个AVEVA历史记录中tag的唯一数字标识符。",
            "type": "int"
          },
          {
            "name": "wwResolution",
            "description": "在循环模式下检索数据的采样率，以毫秒为单位。",
            "type": "int"
          },
          {
            "name": "StartDateTime",
            "description": "返回该行的检索周期的开始时间。",
            "type": "timestamp"
          },
          {
            "name": "SourceTag",
            "description": "在存储该点时复制标记的源标记的名称。",
            "type": "varchar"
          },
          {
            "name": "SourceServer",
            "description": "在存储该点时复制标记的服务器的名称。",
            "type": "varchar"
          }
        ]
      }
    },
    {
      "id": "mysql",
      "type": "uri",
      "name": "MySQL",
      "description": "MySQL是最流行的关系型数据库管理系统之一，由于其体积小、速度快、总体拥有成本低，尤其是开放源码这一特点，一般中小型和大型网站的开发都选择 MySQL 作为网站数据库。\nTDengine 可以高效地从 MySQL 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n",
      "options": {
        "host": {
          "required": true,
          "display": "服务地址",
          "description": "MySQL 的服务器地址",
          "placeholder": "127.0.0.1"
        },
        "port": {
          "required": true,
          "display": "服务端口",
          "description": "MySQL 的端口",
          "placeholder": "3306"
        },
        "subject": {
          "required": true,
          "display": "数据库",
          "description": "MySQL 数据库名称",
          "placeholder": "示例: db1"
        }
      },
      "authentication": {
        "display": "认证",
        "description": "使用用户名和密码访问 MySQL 数据库",
        "value": "plain",
        "alternatives": [
          {
            "name": "plain",
            "display": "用户名密码访问",
            "username": {
              "required": true,
              "display": "用户",
              "placeholder": "username"
            },
            "password": {
              "required": true,
              "display": "密码",
              "placeholder": "password"
            }
          }
        ]
      },
      "groups": [
        {
          "name": "连接选项",
          "display_order": 1,
          "short_description": "其他数据库连接选项。",
          "description": "其他数据库连接选项。",
          "collapsible": false,
          "connection_option": true,
          "params": [
            {
              "name": "charset",
              "display": "字符集",
              "hint": {
                "type": "str",
                "choices": [
                  "utf8",
                  "utf8mb4",
                  "utf16",
                  "utf32",
                  "gbk",
                  "big5",
                  "latin1",
                  "ascii"
                ]
              },
              "short_description": "设置连接的字符集。默认字符集为 utf8mb4 。MySQL 5.5.3 支持此功能。如果需要连接到旧版本，建议改为 utf8 。",
              "description": "设置连接的字符集。默认字符集为 utf8mb4 。MySQL 5.5.3 支持此功能。如果需要连接到旧版本，建议改为 utf8 。",
              "placeholder": "请选择数据库字符集",
              "value": "utf8"
            },
            {
              "name": "ssl",
              "display": "SSL 模式",
              "hint": {
                "type": "str",
                "choices": [
                  "DISABLED",
                  "PREFERRED",
                  "REQUIRED"
                ]
              },
              "short_description": "设置是否与服务器协商安全 SSL TCP/IP 连接或以何种优先级进行协商。",
              "description": "设置是否与服务器协商安全 SSL TCP/IP 连接或以何种优先级进行协商。",
              "placeholder": "请选择 SSL 模式",
              "value": "PREFERRED"
            }
          ]
        },
        {
          "name": "SQL 查询",
          "display_order": 2,
          "short_description": "数据采集相关配置项。",
          "description": "数据采集相关配置项。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "sql",
              "display": "SQL 模板",
              "hint": {
                "type": "str"
              },
              "short_description": "用于查询的 SQL 语句。",
              "description": "用于查询的 SQL 语句。\n",
              "required": false,
              "placeholder": "SELECT * FROM table WHERE time >= $start AND time < $end"
            },
            {
              "name": "start",
              "display": "起始时间",
              "hint": {
                "type": "time"
              },
              "short_description": "应用于查询语句的起始时间。",
              "description": "应用于查询语句的起始时间。\n",
              "required": true,
              "placeholder": "如：2023-01-01 00:00:00"
            },
            {
              "name": "end",
              "display": "结束时间",
              "hint": {
                "type": "time"
              },
              "short_description": "应用于查询语句的结束时间。",
              "description": "应用于查询语句的结束时间。\n",
              "required": false,
              "placeholder": "如：2024-01-01 00:00:00"
            },
            {
              "name": "interval",
              "display": "查询间隔",
              "hint": {
                "type": "duration"
              },
              "short_description": "用于分段查询的时间间隔。",
              "description": "用于分段查询的时间间隔。\n",
              "required": false,
              "placeholder": "1h"
            },
            {
              "name": "delay",
              "display": "延迟时长",
              "hint": {
                "type": "duration"
              },
              "short_description": "用于同步未来时刻数据的等待时长。",
              "description": "用于同步未来时刻数据的等待时长。\n",
              "required": false,
              "placeholder": "0s"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "read_concurrency",
            "display": "最大读取并发数",
            "hint": {
              "type": "integer",
              "min": 0,
              "max": 1000
            },
            "description": "数据源连接数或读取线程数限制，当默认参数不满足需要或需要调整资源使用量时修改此参数。\n",
            "value": "0"
          },
          {
            "name": "batch_size",
            "display": "批次大小",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 100000
            },
            "description": "单次发送的最大消息数或行数。\n",
            "value": "10000"
          }
        ]
      },
      "parser": {
        "display": "数据映射",
        "required": true,
        "description": "taosX 允许用户在数据库中指定数据模型，包括：指定表名称和超级表名，设置普通列和标签列等\n",
        "fields": [
          {
            "name": "DateTime",
            "description": "值对应的时间戳。",
            "type": "timestamp"
          }
        ]
      }
    },
    {
      "id": "postgres",
      "type": "uri",
      "name": "PostgreSQL",
      "description": "PostgreSQL 是一个功能非常强大的、源代码开放的客户/服务器关系型数据库管理系统， 有很多在大型商业RDBMS中所具有的特性，包括事务、子选择、触发器、视图、外键引用完整性和复杂锁定功能。\nTDengine 可以高效地从 PostgreSQL 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n",
      "options": {
        "host": {
          "required": true,
          "display": "服务地址",
          "description": "PostgreSQL 的服务器地址",
          "placeholder": "127.0.0.1"
        },
        "port": {
          "required": true,
          "display": "服务端口",
          "description": "PostgreSQL 的端口",
          "placeholder": "5432"
        },
        "subject": {
          "required": true,
          "display": "数据库",
          "description": "PostgreSQL 数据库名称",
          "placeholder": "示例: db1"
        }
      },
      "authentication": {
        "display": "认证",
        "description": "使用用户名和密码访问 PostgreSQL 数据库",
        "value": "plain",
        "alternatives": [
          {
            "name": "plain",
            "display": "用户名密码访问",
            "username": {
              "required": true,
              "display": "用户",
              "placeholder": "username"
            },
            "password": {
              "required": true,
              "display": "密码",
              "placeholder": "password"
            }
          }
        ]
      },
      "groups": [
        {
          "name": "连接选项",
          "display_order": 1,
          "short_description": "其他数据库连接选项。",
          "description": "其他数据库连接选项。",
          "collapsible": false,
          "connection_option": true,
          "params": [
            {
              "name": "applicationName",
              "display": "应用名称",
              "hint": {
                "type": "str"
              },
              "short_description": "设置应用程序名称，用于标识连接的应用程序。",
              "description": "设置应用程序名称，用于标识连接的应用程序。",
              "placeholder": "示例: TDengine"
            },
            {
              "name": "ssl",
              "display": "SSL 模式",
              "hint": {
                "type": "str",
                "choices": [
                  "DISABLE",
                  "ALLOW",
                  "PREFER",
                  "REQUIRE"
                ]
              },
              "short_description": "设置是否与服务器协商安全 SSL TCP/IP 连接或以何种优先级进行协商。",
              "description": "设置是否与服务器协商安全 SSL TCP/IP 连接或以何种优先级进行协商。",
              "placeholder": "请选择 SSL 模式",
              "value": "PREFER"
            }
          ]
        },
        {
          "name": "SQL 查询",
          "display_order": 2,
          "short_description": "数据采集相关配置项。",
          "description": "数据采集相关配置项。",
          "collapsible": false,
          "connection_option": false,
          "params": [
            {
              "name": "sql",
              "display": "SQL 模板",
              "hint": {
                "type": "str"
              },
              "short_description": "用于查询的 SQL 语句。",
              "description": "用于查询的 SQL 语句。\n",
              "required": false,
              "placeholder": "SELECT * FROM schema.table WHERE time >= $start AND time < $end"
            },
            {
              "name": "start",
              "display": "起始时间",
              "hint": {
                "type": "time"
              },
              "short_description": "应用于查询语句的起始时间。",
              "description": "应用于查询语句的起始时间。\n",
              "required": true,
              "placeholder": "如：2023-01-01 00:00:00"
            },
            {
              "name": "end",
              "display": "结束时间",
              "hint": {
                "type": "time"
              },
              "short_description": "应用于查询语句的结束时间。",
              "description": "应用于查询语句的结束时间。\n",
              "required": false,
              "placeholder": "如：2024-01-01 00:00:00"
            },
            {
              "name": "interval",
              "display": "查询间隔",
              "hint": {
                "type": "duration"
              },
              "short_description": "用于分段查询的时间间隔。",
              "description": "用于分段查询的时间间隔。\n",
              "required": false,
              "placeholder": "1h"
            },
            {
              "name": "delay",
              "display": "延迟时长",
              "hint": {
                "type": "duration"
              },
              "short_description": "用于同步未来时刻数据的等待时长。",
              "description": "用于同步未来时刻数据的等待时长。\n",
              "required": false,
              "placeholder": "0s"
            }
          ]
        }
      ],
      "advanced": {
        "name": "高级选项",
        "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项。\n",
        "collapsible": true,
        "connection_option": false,
        "params": [
          {
            "name": "read_concurrency",
            "display": "最大读取并发数",
            "hint": {
              "type": "integer",
              "min": 0,
              "max": 1000
            },
            "description": "数据源连接数或读取线程数限制，当默认参数不满足需要或需要调整资源使用量时修改此参数。\n",
            "value": "0"
          },
          {
            "name": "batch_size",
            "display": "批次大小",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 100000
            },
            "description": "单次发送的最大消息数或行数。\n",
            "value": "10000"
          }
        ]
      },
      "parser": {
        "display": "数据映射",
        "required": true,
        "description": "taosX 允许用户在数据库中指定数据模型，包括：指定表名称和超级表名，设置普通列和标签列等\n",
        "fields": [
          {
            "name": "DateTime",
            "description": "值对应的时间戳。",
            "type": "timestamp"
          }
        ]
      }
    }
  ];

export function getDataSources() {
    return datasources;
}
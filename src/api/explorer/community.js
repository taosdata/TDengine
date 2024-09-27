export function getDataSource(lang, type) {
  let allDataSources = getDataSources(lang);

  for (let i = 0; i < allDataSources.length; i++) {
    if (allDataSources[i].id === type) {
      return JSON.parse(JSON.stringify(allDataSources[i], (k, v) => {
        if (v instanceof RegExp) {
          return v.toString()
        }
        return v
      }));
    }
  }

  return null;
}

export function getDataSources(lang) {
  if (lang && lang === 'en') {
    return [
      {
        "id": "tmq",
        "type": "uri",
        "name": "TDengine 3.x",
        "license_id": "td3.0",
        "description": "TMQ data source is a read-only data source for TDengine.\n\n## Protocols\n\nThe following protocols are supported.\n\n- ws: websocket protocol with plain HTTP connection.\n- wss: websocket protocol with TLS http connection.\n\nWithout protocol settings, TMQ will use the TDengine native connection.\n\n## Subject\n\nA TMQ data source can subscribe to data from a database or a specified table. The table must be specified in the \"database.tablename\" format.\n",
        "options": {
          "endpoint": {
            "required": true,
            "display": "Topic DSN",
            "description": "Please login TDengine Cloud or TDengine enterprise, select \"topics\", under the list of topics, copy DSN and paste it here.\n",
            "placeholder": "Topic example: ws://root:taosdata@127.0.0.1:6041/topic1"
          }
        },
        "groups": [
          {
            "name": "Subscribe Options",
            "display_order": 2,
            "short_description": "Options for TMQ subscription.",
            "description": "Options for TMQ subscription.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "auto.offset.reset",
                "display": "Start From",
                "hint": {
                  "type": "str",
                  "choices": [
                    "earliest",
                    "latest"
                  ]
                },
                "short_description": "Data offset to start subscribing.",
                "description": "Data offset to start subscribing.\n- *earliest*: All the data in TDengine, include the new data,\n- *latest*: Subscribe from latest data.\n",
                "value": "earliest",
                "edit_disabled": true,
              },
              {
                "name": "group.id",
                "display": "Group ID",
                "hint": {
                  "type": "str"
                },
                "short_description": "Group ID is a string used to identify a subscription group, with a maximum length of 192. Subscribers within the same subscription group share consumption progress. Randomly generated group ID will be used when not specified.      ",
                "description": "Group ID is a string used to identify a subscription group, with a maximum length of 192. Subscribers within the same subscription group share consumption progress. Randomly generated group ID will be used when not specified.      \n",
                "edit_disabled": true,
              },
              {
                "name": "client.id",
                "display": "Client ID",
                "hint": {
                  "type": "str"
                },
                "short_description": "Client ID is a string used to identify the client, with a maximum length of 192.",
                "description": "Client ID is a string used to identify the client, with a maximum length of 192.\n",
                "required": true,
                "edit_disabled": true,
              },
              {
                "name": "timeout",
                "display": "Timeout",
                "hint": {
                  "type": "timeout",
                  "choices": [
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                    {
                      "value": "ms",
                      "label": "Millisecond"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "A timeout for polling data from the topic.",
                "description": "A timeout for polling data from the topic.\n\nThe input value should be one of:\n- `0`: means waiting for valid message without timeout.\n- A duration string like `5s`, `1m` etc.\n",
                "placeholder": "The value is an integer ranging [0,60000]",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              },
              {
                "name": "experimental.snapshot.enable",
                "display": "TSDB Data",
                "hint": {
                  "type": "bool"
                },
                "short_description": "- If enabled, the data that has been persisted in time series data storage files will be replicated too; otherwise, only the data still in WAL (write ahead log) will be replicated.",
                "description": "- If enabled, the data that has been persisted in time series data storage files will be replicated too; otherwise, only the data still in WAL (write ahead log) will be replicated.\n",
                "value": "true"
              },
              {
                "name": "with.meta.drop",
                "display": "Table Deletions",
                "hint": {
                  "type": "bool"
                },
                "short_description": "If enabled, the table deletion operations on the source side will be replayed on the sink side.",
                "description": "If enabled, the table deletion operations on the source side will be replayed on the sink side.\n",
                "value": "true"
              },
              {
                "name": "with.meta.delete",
                "display": "Data Deletions",
                "hint": {
                  "type": "bool"
                },
                "short_description": "If enabled, the data deletion operations on the source side will be replayed on the sink side.",
                "description": "If enabled, the data deletion operations on the source side will be replayed on the sink side.\n",
                "value": "true"
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Adjust the parameters related to concurrency setting for reading from data source and  writing into data sink, and error log.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "compression",
              "display": "Compression",
              "hint": {
                "type": "bool"
              },
              "short_description": "Enable WebSocket compression to reduce network bandwidth consumption.",
              "description": "Enable WebSocket compression to reduce network bandwidth consumption.\n",
              "value": "false"
            }
          ]
        }
      },
      {
        "id": "taos",
        "type": "uri",
        "name": "TDengine 2.x",
        "license_id": "td2.6",
        "description": "The TDengine 2.x data source can be used to migrate data from previous version to current cluster.\n\n## Protocols\n\nThe supported protocols are:\n\n- ws: websocket protocol with plain HTTP connection.\n- wss: websocket protocol with TLS http connection.\n\nIf a protocol setting is not specified, a TDengine native connection will be used.\n",
        "options": {
          "host": {
            "required": true,
            "display": "Host",
            "description": "Remote server REST API (taosAdapter) address. If you prefer to use multiple nodes, please consider to use a load-balancer.",
            "placeholder": "taos-adapter-addr"
          },
          "port": {
            "required": true,
            "display": "Port",
            "description": "Remote server REST API (taosAdapter) port.",
            "placeholder": "6041",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "The port number ranges from 0 to 65535",
          },
          "subject": {
            "required": true,
            "display": "Database",
            "description": "Database name",
            "placeholder": "Example: db1"
          }
        },
        "protocol": {
          "display": "Protocol",
          "description": "Choose a protocol scheme for websocket connection.",
          "choices": [
            {
              "name": "ws",
              "display": "WS",
              "description": "Use WebSocket with HTTP connection."
            },
            {
              "name": "wss",
              "display": "WSS",
              "description": "Use WebSocket with HTTPS connection."
            }
          ],
          "value": "ws"
        },
        "authentication": {
          "display": "Authentication",
          "description": "Use username/password plain authentication.",
          "value": "plain",
          "alternatives": [
            {
              "name": "plain",
              "display": "Plain",
              "username": {
                "display": "Username",
                "description": "TDengine username. The default is root.",
                "placeholder": "root",
                "value": "root"
              },
              "password": {
                "display": "Password",
                "description": "TDengine password. The default is taosdata.",
                "placeholder": "taosdata",
                "value": "taosdata"
              }
            }
          ]
        },
        "groups": [
          {
            "display": "Migrate Options",
            "name": "migrate_options",
            "display_order": 1,
            "short_description": "How to migrate.",
            "description": "How to migrate.",
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
                "short_description": "Migrate history data or realtime or both.",
                "description": "Migrate history data or realtime or both.",
                "value": "history"
              },
              {
                "name": "schema",
                "display": "Schema",
                "hint": {
                  "type": "str",
                  "choices": [
                    "always",
                    "none",
                    "only"
                  ]
                },
                "short_description": "Which kind of data to be migrated.",
                "description": "Which kind of data to be migrated.\n\n- `only`: means only migrate schema into target.\n- `none`: means not migrate schema, but only data into target.\n- `always`: means migrate all stuff.\n",
                "placeholder": "always",
                "value": "always"
              },
              {
                "name": "sparse",
                "display": "Sparse",
                "hint": {
                  "type": "bool"
                },
                "short_description": "Enable this mode to improve performance in case of high-cardinality and low data ingestion frequency.",
                "description": "Enable this mode to improve performance in case of high-cardinality and low data ingestion frequency.",
                "value": "false"
              },
              {
                "name": "schema-polling-interval",
                "display": "Schema Polling Interval",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "Polling interval to query schema.",
                "description": "Polling interval to query schema.",
                "placeholder": "The value is an integer ranging [0,60000]",
                "value": "5",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              }
            ]
          },
          {
            "display": "What to migrate",
            "name": "what_to_migrate",
            "display_order": 2,
            "short_description": "Choose to migrate from stable or tables.",
            "description": "Choose to migrate from stable or tables.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "stables",
                "display": "STables",
                "hint": {
                  "type": "str"
                },
                "short_description": "Select some of stables from the database. Separated by `,`.",
                "description": "Select some of stables from the database. Separated by `,`.",
                "placeholder": "metrics"
              },
              {
                "name": "tables",
                "display": "Tables",
                "hint": {
                  "type": "str"
                },
                "short_description": "Select table names to be migrated.",
                "description": "Select table names to be migrated.\n",
                "placeholder": "d0001"
              }
            ]
          },
          {
            "diaplay": "Range",
            "name": "range",
            "display_order": 3,
            "short_description": "Migration time range.",
            "description": "Migration time range.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "start",
                "display": "Start",
                "hint": {
                  "type": "time"
                },
                "short_description": "Time range start.",
                "description": "Time range start.",
                "placeholder": "2023-10-01T12:00:00.000+08:00"
              },
              {
                "name": "end",
                "display": "End",
                "hint": {
                  "type": "time"
                },
                "short_description": "Time range end.",
                "description": "Time range end.",
                "placeholder": "2023-10-02T12:00:00.000+08:00"
              },
              {
                "name": "unit",
                "display": "Unit",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "Day"
                    },
                    {
                      "value": "h",
                      "label": "Hours"
                    },
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "Time duration unit for query.",
                "description": "Time duration unit for query.<br>\nSupports abbreviations of numbers and units, such as \"1ms\" for 1 millisecond, \"1s\" for 1 seconds, \"1m\" for 1 minute, \"1h\" for 1 hour, \"1d\" for 1 day, and \"1w\" for 1 week.<br>\nOnly numbers default to seconds as unit.<br>",
                "placeholder": "The value is an integer ranging [0,60000]",
                "value": "1d",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              }
            ]
          },
          {
            "display": "Realtime Settings",
            "name": "realtime_settings",
            "display_order": 4,
            "short_description": "Only available in `realtime` mode.",
            "description": "Only available in `realtime` mode.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "retro",
                "display": "Retrospection",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "h",
                      "label": "Hours"
                    },
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                    {
                      "value": "ms",
                      "label": "millisecond"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "Retrospect data from some time ago into target before realtime data migrating.",
                "description": "Retrospect data from some time ago into target before realtime data migrating.<br>\nSupports abbreviations of numbers and units, such as \"1ms\" for 1 millisecond, \"1s\" for 1 seconds, \"1m\" for 1 minute, \"1h\" for 1 hour, \"1d\" for 1 day, and \"1w\" for 1 week.<br>\nOnly numbers default to seconds as unit.<br>",
                "placeholder": "The value is an integer ranging [0,60000]",
                "value": "0",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              },
              {
                "name": "interval",
                "display": "Interval",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "Polling interval to query realtime data.",
                "description": "Polling interval to query realtime data.<br>\nSupports abbreviations of numbers and units, such as \"1ms\" for 1 millisecond, \"1s\" for 1 seconds, \"1m\" for 1 minute, \"1h\" for 1 hour, \"1d\" for 1 day, and \"1w\" for 1 week.<br>\nOnly numbers default to seconds as unit.<br>",
                "placeholder": "The value is an integer ranging [0,60000]",
                "value": "1",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              },
              {
                "name": "excursion",
                "display": "Excursion",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "Wait for some period to querying random-order data.",
                "description": "Wait for some period to querying random-order data.<br>\nSupports abbreviations of numbers and units, such as \"1ms\" for 1 millisecond, \"1s\" for 1 seconds, \"1m\" for 1 minute, \"1h\" for 1 hour, \"1d\" for 1 day, and \"1w\" for 1 week.<br>\nOnly numbers default to seconds as unit.<br>",
                "placeholder": "The value is an integer ranging [0,60000]",
                "value": "500ms",
                "type_value": "ms",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Adjust the parameters related to concurrency setting for reading from data source and  writing into data sink, and error log.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "workers",
              "display": "Read Concurreny",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 100
              },
              "description": "The number of threads for reading data from the source. If not set, the default value is the number of CPU cores.",
              "value": "0"
            },
            {
              "name": "write-concurrency",
              "display": "Write Concurreny",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 100
              },
              "description": "The overall maximum concurrency for writing to the target database. It cannot be less than the read concurrency, and the default is equal to the read concurrency.\n",
              "value": "1"
            },
            {
              "name": "fails-to",
              "display": "File to write failed data",
              "hint": {
                "type": "str"
              },
              "description": "An absolute path of the environment where taosX is running. If set, the failed data and the reason for the failure will be written to the file and will not block task execution. If not set, a failed write will cause task interruption.\n"
            },
            {
              "name": "compression",
              "display": "Compression",
              "hint": {
                "type": "bool"
              },
              "short_description": "Enable WebSocket compression to reduce network bandwidth consumption.",
              "description": "Enable WebSocket compression to reduce network bandwidth consumption.\n",
              "value": "false"
            }
          ]
        }
      },
      {
        "id": "pi",
        "type": "uri",
        "name": "PI",
        "license_id": "pi",
        "description": "The Aveva PI System is a suite of software products that are used for data collection, historicizing, finding, analyzing, delivering, and visualizing. It is marketed as an enterprise infrastructure for management of real-time data and events.\n\nThe term PI System is often used to refer to the PI Server but the two are not the same. The PI System refers to all Aveva PI software products whereas the PI Server is the core product of the PI System. Data can be automatically collected from many sources (control systems, lab equipment, calculations, manual entry or custom software).\n",
        "options": {
          "host": {
            "required": true,
            "display": "PI Data Archive Server",
            "description": "PI Data Archive Server (hostname).\n\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.",
            "placeholder": "pi data archive server"
          },
          "port": {},
          "subject": {
            "required": true,
            "display": "AF Database Name",
            "description": "AF database name",
            "placeholder": "Example: Met1"
          }
        },
        "groups": [
          {
            "name": "Auto Backfill",
            "display_order": 3,
            "short_description": "Auto-backfill configurations.",
            "description": "Auto-backfill configurations.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "MaxBackfillRangeDays",
                "display": "Max Backfill Range",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "Day"
                    },
                    {
                      "value": "h",
                      "label": "Hours"
                    },
                    {
                      "value": "m",
                      "label": "Mniute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "The maximum time for automatic backfilling upon connection loss or first startup: `2d`, `3h`, `4m`, etc.",
                "description": "The maximum time for automatic backfilling upon connection loss or first startup: `2d`, `3h`, `4m`, etc.\n",
                "placeholder": "The value is an integer ranging [0,600]",
                "value": "0",
                "type_value": "m",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "sync_add_element",
              "display": "Synchronize New Elements",
              "description": "Monitor the newly added elements under the configured templates, and synchronize the data of the newly added elements without restarting the task",
              "hint": {
                "type": "bool",
              },
              "value": "true",
            },
            {
              "name": "sync_update_attribute",
              "display": "Synchronize The Changes of Static Attribute",
              "description": "Synchronize the changes of all static attribute to TDengine",
              "hint": {
                "type": "bool",
              },
              "value": "true",
            },
            {
              "name": "sync_delete_element",
              "display": "Synchronize The Deletions of Elements",
              "description": "Monitor deleting elements under the configured templates, and correspondingly drop the corresponding child tables in TDengine",
              "hint": {
                "type": "bool",
              },
              "value": "true",
            },
            {
              "name": "sync_delete_data",
              "display": "Synchronize The Deletion of Point Data",
              "description": "For the dynamic attributes of an element, if the data for a certain period of time is deleted in PI, the corresponding data is set to null in TDengine",
              "hint": {
                "type": "bool",
              },
              "value": "true",
            },
            {
              "name": "sync_update_data",
              "display": "Synchronize The Changes of Point Data",
              "description": "For the dynamic attributes of an element, if the data for a certain time is modified in PI, the corresponding data is updated automatically too in TDengine",
              "hint": {
                "type": "bool",
              },
              "value": "true",
            },
            {
              "name": "log_level",
              "display": "Log Level",
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
              "description": "Adjust the log level of the data source as required. This parameter does not always take effect.",
              "value": "info"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 10000
              },
              "description": "The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "1000"
            },
            {
              "name": "batch_timeout",
              "display": "Batch Timeout",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 60
              },
              "description": "The maximum time(in seconds) to wait before sending a batch of data points. The default value is 1s. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "1"
            }
          ]
        },
        "params": [
          {
            "name": "system_configuration",
            "display": "System Configuration",
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
            "display": "AF Server Name",
            "display_order": 3,
            "hint": {
              "type": "str"
            },
            "description": "PI System(AF Server) name (hostname).",
            "required": true,
            "placeholder": "pi-af-server-name"
          }
        ],
        "datasets": {
          "name": "Data Model Configuration",
          "display": "Data Model",
          "description": "Use the default configuration, or download and modify it before uploading. Configure the entry points or elements, the data model for entry, data filtering conditions, and transformation rules.",
          "value": "single-column",
          "categories": [
            {
              "category": "single-column",
              "display": "Single column mode",
              "short_description": "The single column mode creates a super table based on the UOM of the point, with each point creating a sub table.",
              "target": {
                "name": "single-column",
              },
              "params": [{
                "name": "filter_value",
                "display": "Dataset filtering",
                "placeholder": "Wildcard * matches 0 or more characters, wildcard ? exactly match one character",
                "hint": {
                  "type": "compose",
                  "choices": [
                    "point",
                    "element",
                    "template"
                  ]
                },
                "action": "download",
                "action_text": "Download default configuration",
                "description": "Filter conditions can be specified, download default template<br>- point: filter using point names<br>- element: filter using AF element names<br>- template: filter using AF template names<br>Filter conditions can use wildcard * to match 0 or multiple characters, use wildcard? Exactly match one character",
              }, {
                "name": "transform_config_file",
                "display": "Point configuration file",
                "btnText": "Upload configuration file",
                "required": true,
                "hint": {
                  "type": "file"
                },
                "description": "Upload a single column mode point list file in CSV format.",
              }]
            },
            {
              "category": "multi-column",
              "display": "Multi column mode",
              "short_description": "The multi column pattern creates a super table based on the AF Template, with each AF element creating a sub table.",
              "target": {
                "name": "multi-column",
                "selectable": false
              },
              "params": [{
                "name": "filter_value",
                "display": "Dataset filtering",
                "placeholder": "Wildcard * matches 0 or more characters, wildcard ? exactly match one character",
                "hint": {
                  "type": "compose",
                  "choices": [
                    "point",
                    "element",
                    "template"
                  ]
                },
                "action": "download",
                "action_text": "Download default configuration",
                "description": "Filter conditions can be specified, download default template<br>- point: filter using point names<br>- element: filter using AF element names<br>- template: filter using AF template names<br>Filter conditions can use wildcard * to match 0 or multiple characters, use wildcard? Exactly match one character",
              }, {
                "name": "transform_config_file",
                "display": "Model configuration file",
                "required": true,
                "btnText": "Upload configuration file",
                "hint": {
                  "type": "file"
                },
                "description": "Upload a multi column pattern model configuration file in CSV format.",
              }]
            }
          ]
        }
      },
      {
        "id": "pibackfill",
        "type": "uri",
        "name": "PI Backfill",
        "license_id": "pi",
        "description": "The Aveva PI System is a suite of software products that are used for data collection, historicizing, finding, analyzing, delivering, and visualizing. It is marketed as an enterprise infrastructure for management of real-time data and events.\n\nThe term PI System is often used to refer to the PI Server but the two are not the same. The PI System refers to all Aveva PI software products whereas the PI Server is the core product of the PI System. Data can be automatically collected from many sources (control systems, lab equipment, calculations, manual entry or custom software).\n",
        "options": {
          "host": {
            "required": true,
            "display": "PI Data Archive Server",
            "description": "PI data archive server name (hostname).\n\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.",
            "placeholder": "server"
          },
          "port": {},
          "subject": {
            "required": true,
            "display": "AFDatabaseName",
            "description": "AF database name",
            "placeholder": "Example: Met1"
          }
        },
        "groups": [
          {
            "name": "Backfill",
            "display_order": 3,
            "short_description": "TDBackfill param set.",
            "description": "TDBackfill param set.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "BackfillStartTime",
                "display": "Backfill Start Time",
                "hint": {
                  "type": "time"
                },
                "required": true,
                "short_description": "The start time for backfilling data.",
                "description": "The start time for backfilling data.\n",
                "placeholder": "YYYY-MM-DD HH:mm:ss"
              },
              {
                "name": "BackfillEndTime",
                "display": "Backfill End Time",
                "hint": {
                  "type": "time"
                },
                "required": true,
                "short_description": "The end time for backfilling data.",
                "description": "The end time for backfilling data.Cannot be later than the now.\n",
                "placeholder": "YYYY-MM-DD HH:mm:ss",
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "log_level",
              "display": "Log Level",
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
              "description": "Adjust the log level of the data source as required. This parameter does not always take effect.",
              "value": "info"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 10000
              },
              "description": "The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "1000"
            },
            {
              "name": "batch_timeout",
              "display": "Batch Timeout",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 60
              },
              "description": "The maximum time(in seconds) to wait before sending a batch of data points. The default value is 1s. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "1"
            }
          ]
        },
        "params": [
          {
            "name": "system_configuration",
            "display": "System Configuration",
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
            "display": "AF Server Name",
            "display_order": 3,
            "hint": {
              "type": "str"
            },
            "description": "PI System(AF Server) name (hostname).",
            "required": true,
            "placeholder": "pi-af-server-name"
          }
        ],
        "datasets": {
          "name": "Data Model Configuration",
          "display": "Data Model",
          "description": "Use the default configuration, or download and modify it before uploading. Configure the entry points or elements, the data model for entry, data filtering conditions, and transformation rules.",
          "value": "single-column",
          "categories": [
            {
              "category": "single-column",
              "display": "Single column mode",
              "short_description": "The single column mode creates a super table based on the UOM of the point, with each point creating a sub table.",
              "target": {
                "name": "single-column",
              },
              "params": [{
                "name": "filter_value",
                "display": "Dataset filtering",
                "placeholder": "Wildcard * matches 0 or more characters, wildcard ? exactly match one character",
                "hint": {
                  "type": "compose",
                  "choices": [
                    "point",
                    "element",
                    "template"
                  ]
                },
                "action": "download",
                "action_text": "Download default configuration",
                "description": "Filter conditions can be specified, download default template<br>- point: filter using point names<br>- element: filter using AF element names<br>- template: filter using AF template names<br>Filter conditions can use wildcard * to match 0 or multiple characters, use wildcard? Exactly match one character",
              }, {
                "name": "transform_config_file",
                "display": "Point configuration file",
                "btnText": "Upload configuration file",
                "required": true,
                "hint": {
                  "type": "file"
                },
                "description": "Upload a single column mode point list file in CSV format.",
              }]
            },
            {
              "category": "multi-column",
              "display": "Multi column mode",
              "short_description": "The multi column pattern creates a super table based on the AF Template, with each AF element creating a sub table.",
              "target": {
                "name": "multi-column",
                "selectable": false
              },
              "params": [{
                "name": "filter_value",
                "display": "Dataset filtering",
                "placeholder": "Wildcard * matches 0 or more characters, wildcard ? exactly match one character",
                "hint": {
                  "type": "compose",
                  "choices": [
                    "point",
                    "element",
                    "template"
                  ]
                },
                "action": "download",
                "action_text": "Download default configuration",
                "description": "Filter conditions can be specified, download default template<br>- point: filter using point names<br>- element: filter using AF element names<br>- template: filter using AF template names<br>Filter conditions can use wildcard * to match 0 or multiple characters, use wildcard? Exactly match one character",
              }, {
                "name": "transform_config_file",
                "display": "Model configuration file",
                "required": true,
                "btnText": "Upload configuration file",
                "hint": {
                  "type": "file"
                },
                "description": "Upload a multi column pattern model configuration file in CSV format.",
              }]
            }
          ]
        }
      },
      {
        "id": "opcua",
        "type": "uri",
        "name": "OPC-UA",
        "license_id": "opc_ua",
        "description": "OPC is one of interoperability standard for the secure and reliable exchange of data in the industrial automation space and in other industries.\n\nOPC UA is the next generation beyond the classic OPC specification, a platform-independent, service-oriented architecture specification that integrates all functionality from the existing OPC Classic specifications, providing a migration path to a more secure and scalable solution.\n\nTo learn more about OPC, OPC UA and OPC DA, please visit the following links on the [OPC Foundation site](https://opcfoundation.org/):\n\n1. [What is OPC](https://opcfoundation.org/about/what-is-opc/)\n2. [What is OPC UA](https://opcfoundation.org/about/opc-technologies/opc-ua/)\n",
        "options": {
          "endpoint": {
            "required": true,
            "display": "Server endpoint",
            "description": "OPC UA server endpoint, such as `127.0.0.1:6666/OPCUA/ServerPath`.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.\n",
            "placeholder": "127.0.0.1:6666/OPCUA/ServerPath"
          },
          "security_mode": {
            "name": "security_mode",
            "display": "Security Mode",
            "hint": {
              "type": "str",
              "choices": [
                "None",
                "Sign",
                "SignAndEncrypt"
              ]
            },
            "description": "Available value is one of None / Sign / SignAndEncrypt.\n"
          },
          "security_policy": {
            "name": "security_policy",
            "display": "Security Policy",
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
            "description": "Available value is one of None/Basic128Rsa15/Basic256/Basic256Sha256.\n"
          },
          "certificate": {
            "name": "certificate",
            "display": "Secure Channel Certificate",
            "hint": {
              "type": "file"
            },
            "description": "If the certificate is not authenticated by CA, please trust it on the server side and initiate a connectivity check again."
          },
          "private_key": {
            "name": "private_key",
            "display": "Certificate's Private Key",
            "hint": {
              "type": "file"
            },
            "description": "The private key of the certificate."
          },
          "connect_timeout": {
            "name": "connect_timeout",
            "display": "Connect Timeout",
            "hint": {
              "type": "integer",
              "min": 1,
              "max": 60
            },
            "description": "Timeout for connect to endpoint in seconds",
            "placeholder": "10",
            "value": "10"
          }
        },
        "authentication": {
          "display": "Authentication",
          "description": "Use username/password plain authentication or with certificate files, or anonymous(default).",
          "value": "anonymous",
          "alternatives": [
            {
              "name": "anonymous",
              "display": "Anonymous"
            },
            {
              "name": "plain",
              "display": "Username",
              "username": {
                "required": true,
                "display": "Username",
                "description": "OPC UA server username.",
                "placeholder": "username"
              },
              "password": {
                "required": true,
                "display": "Password",
                "description": "OPC UA server password.",
                "placeholder": "password"
              }
            },
            {
              "name": "certificates",
              "display": "Certificates",
              "params": [
                {
                  "name": "auth_certificate",
                  "required": true,
                  "display": "Authentication Certificate",
                  "hint": {
                    "type": "file"
                  }
                },
                {
                  "name": "auth_private_key",
                  "required": true,
                  "display": "Private key of Certificate",
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
            "name": "Collect",
            "display_order": 1,
            "short_description": "Configurations for collecting data from OPC UA server.",
            "description": "Configurations for collecting data from OPC UA server.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "collect_mode",
                "display": "Collect Mode",
                "hint": {
                  "type": "str",
                  "choices": [
                    "observe",
                    "subscribe"
                  ]
                },
                "short_description": "observe or subscribe. default is subscribe",
                "description": "observe or subscribe. default is subscribe",
                "placeholder": "subscribe",
                "value": "subscribe"
              },
              {
                "name": "interval",
                "display": "Collect interval",
                "hint": {
                  "type": "integer",
                  "min": 1,
                  "max": 60
                },
                "short_description": "Collect data interval in second",
                "description": "Collect data interval in second",
                "value": "10"
              },
              {
                "name": "request_timeout",
                "display": "Request Timeout",
                "hint": {
                  "type": "integer",
                  "min": 1,
                  "max": 60
                },
                "short_description": "Timeout for a request to endpoint in seconds",
                "description": "Timeout for a request to endpoint in seconds",
                "placeholder": "10",
                "value": "1"
              },
              {
                "name": "update_mode",
                "display": "Point Update Mode",
                "hint": {
                  "type": "str",
                  "choices": [
                    "none",
                    "append",
                    "update"
                  ]
                },
                "short_description": "Update the OPC data points. none: do not update points. append: append new points. update: append new points and delete off-line points.",
                "description": "Update the OPC data points. none: do not update points. append: append new points. update: append new points and delete off-line points.\n",
                "value": "none"
              },
              {
                "name": "update_interval",
                "display": "Point Update Interval",
                "hint": {
                  "type": "integer",
                  "min": 60,
                  "max": 2147483647
                },
                "short_description": "Update the OPC data points interval in seconds.",
                "description": "Update the OPC data points interval in seconds.\n",
                "value": "600"
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "log_level",
              "display": "Log Level",
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
              "description": "Adjust the log level of the data source as required. This parameter does not always take effect.",
              "value": "info"
            },
            {
              "name": "write_concurrency",
              "display": "Write Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 128
              },
              "description": "The number of concurrent write requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 10000
              },
              "description": "The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "1000"
            },
            {
              "name": "batch_timeout",
              "display": "Batch Timeout",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 60
              },
              "description": "The maximum time(in seconds) to wait before sending a batch of data points. The default value is 1s. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "1"
            },
            {
              "name": "keep_raw_data",
              "display": "Keep Raw Data",
              "hint": {
                "type": "bool"
              },
              "description": "Whether to keep the raw data. If enabled, the raw data will be stored.\n"
            },
            {
              "name": "keep_raw_data_days",
              "display": "Max Keep Days",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 365
              },
              "description": "The number of days to keep the raw data. The default value is 1 day.\n",
              "value": "1",
              "requires": "keep_raw_data"
            },
            {
              "name": "keep_raw_data_dir",
              "display": "Raw Data Directory",
              "hint": {
                "type": "str"
              },
              "description": "The directory to store the raw data. The default value is `$DATA_DIR/tasks/:id/rawdata/`.\n",
              "placeholder": "$DATA_DIR/tasks/:id/rawdata/",
              "requires": "keep_raw_data"
            }
          ]
        },
        "datasets": {
          "name": "Data Sets",
          "description": "Data points in OPC server to collect.",
          "value": "csv_config_file",
          "categories": [
            {
              "category": "csv_config_file",
              "display": "Upload CSV",
              "description": "OPC DataIn task uses a csv file to define the mapping rules for each data point to the TDengine table:\n\n(1) point_id: required, the id of the data point on the OPC UA server;\n\n(2) stable: required. TDengine super table corresponding to data points;\n\n(3) tbname: required. TDengine subtable corresponding to the data point;\n\n(4) enable: optional. The default value is '1', which specifies whether to collect data at this point. 0- Do not collect and delete the corresponding sub-table, 1- collect the point data, create a sub-table when there is no sub-table;\n\n(5) value_col: optional. The default value is val. The column name corresponding to the data point collection value in TDengine;\n\n(6) value_transform: optional, the transformation function executed in taosX for data point acquisition values. Currently, only numerical calculation expressions are supported. See expr expression description in transform document for details.\n\n(7) type: optional. The default value is the source data type. The data type of the data point collection value, which can be used to replace the placeholder {type} in the supertable name;\n\n(8) quality_col: optional, the column name corresponding to the quality of data point collection value in TDengine;\n\n(9) ts_col/received_ts_col: required. TDengine timestamp primary key definition: If only ts_col exists, the original timestamp is used as the primary key. If only received_ts_col exists, the collection timestamp is used as the primary key. If both columns exist, the first timestamp column is used as the primary key.\n\n(10) ts_transform: optional, the original timestamp transform function, refer to the description of the transform numerical calculation expression expr;\n\n(11) received_ts_transform: optional, collect data timestamp transform function, refer to the description of Transform numerical calculation expression expr;\n\n(12) tag::VARCHAR(200)::name: Multiple tag columns are optional or configurable. The Tag column corresponding to the data point in TDengine; tag is reserved keyword, indicating that the column is a tag column. VARCHAR(200) indicates the type of the tag, or any other valid type. name is the column name of the tag.\n\nFor more rules, please refer to the <a target=\"_blank\" href=\"/docs-en/enterprise/datain/opcua\">enterprise version document</a>.\n",
              "target": {
                "name": "csv_config_file",
                "description": "Upload a csv file to define the mapping rules for each data point to the TDengine table.\n",
                "required": true,
                "multiple": true,
                "editable": true,
                "selectable": true
              }
            },
            {
              "category": "select_all_points",
              "display": "Data Points",
              "description": "OPC UA point configuration file.\n",
              "target": {
                "name": "select_all_points",
                "description": "Select data points that meet specified conditions on the OPC UA server.\n",
                "required": true,
                "multiple": true,
                "editable": true,
                "selectable": true
              },
              "params": [
                {
                  "name": "root",
                  "display": "Root node ID",
                  "hint": {
                    "type": "str"
                  },
                  "description": "Query all child nodes starting from this node.\n",
                  "placeholder": "For example ns=1;i=1001"
                },
                {
                  "name": "namespaces",
                  "display": "Namespaces of point",
                  "hint": {
                    "type": "str",
                    "choices": [
                      "--NONE--"
                    ]
                  },
                  "description": "Support multiple selections, only query the data points under these namespaces.\n",
                  "multiple": true,
                  "placeholder": "Please select after connection check successfully"
                },
                {
                  "name": "pattern",
                  "display": "Regex pattern",
                  "hint": {
                    "type": "str"
                  },
                  "description": "Match the data point name or id"
                },
                {
                  "name": "super_table_expression",
                  "display": "Super Table Name",
                  "hint": {
                    "type": "str"
                  },
                  "description": "Support <super table prefix>_{type} pattern, {type} is the data type of the OPC point.\n",
                  "required": true,
                  "value": "opc_{type}"
                },
                {
                  "name": "child_table_expression",
                  "display": "Table Name",
                  "hint": {
                    "type": "str"
                  },
                  "description": "Support <child table prefix>_{tagname} pattern, {tagname} is the name of the OPC point.\n",
                  "required": true,
                  "value": "t_{tagname}"
                },
                {
                  "name": "table_primary_key",
                  "display": "Primary Key",
                  "hint": {
                    "type": "str",
                    "choices": [
                      "original_ts",
                      "received_ts"
                    ]
                  },
                  "description": "The selected value will be the primary key of target table, original_ts represents the time when OPC service receive data from data points, and received_ts represents the time when the taosX task received data from OPC server.\n",
                  "required": false,
                  "value": "original_ts"
                },
                {
                  "name": "table_primary_key_alias",
                  "display": "Primary Key Name",
                  "hint": {
                    "type": "str"
                  },
                  "description": "The primary key column name in the target table.\n",
                  "required": false,
                  "value": "ts"
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
        "license_id": "opc_da",
        "description": "OPC is one of interoperability standard for the secure and reliable exchange of data in the industrial automation space   and in other industries.\n\nOPC DA (Data Access) is a classic COM-based specification that works only on Windows.\nOPC DA is widely used even though it isn't the newest and most efficient data communication specification out there. This is mainly because of older devices that only support the OPC DA.\n\nFor more about OPC DA we introduce you to read the [OPC Foundation site](https://opcfoundation.org/), and some useful blogs, such as\n\n1. [What is OPC](https://opcfoundation.org/about/what-is-opc/)\n2. [What is OPC DA](https://plcynergy.com/opc-da/)\n\ntaosX could pull data from OPC server by a OPC connector plugin.\n\nCheck the help message in each part to see the details.\n",
        "options": {
          "endpoint": {
            "required": true,
            "display": "Server endpoint",
            "description": "OPC server endpoint, such as `127.0.0.1<,localhost>/Matrikon.OPC.Simulation.1`.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be the taosX host.\n",
            "placeholder": "127.0.0.1/Matrikon.OPC.Simulation.1"
          }
        },
        "groups": [
          {
            "name": "Connection",
            "display_order": 1,
            "short_description": "Configuration used in OPC connection",
            "description": "Configuration used in OPC connection",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "connect_timeout",
                "display": "Connect Timeout",
                "hint": {
                  "type": "integer",
                  "min": 1,
                  "max": 60
                },
                "short_description": "Timeout for connect to endpoint in seconds",
                "description": "Timeout for connect to endpoint in seconds",
                "placeholder": "10",
                "value": "10"
              },
              {
                "name": "request_timeout",
                "display": "Request Timeout",
                "hint": {
                  "type": "integer",
                  "min": 1,
                  "max": 60
                },
                "short_description": "Timeout for a request to endpoint in seconds",
                "description": "Timeout for a request to endpoint in seconds",
                "placeholder": "10",
                "value": "10"
              }
            ]
          },
          {
            "name": "Collect",
            "display_order": 2,
            "short_description": "Configurations for collecting data from OPC",
            "description": "Configurations for collecting data from OPC",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "interval",
                "display": "Collect interval",
                "hint": {
                  "type": "integer",
                  "min": 1,
                  "max": 60
                },
                "short_description": "Collect data interval in second",
                "description": "Collect data interval in second",
                "value": "1"
              },
              {
                "name": "update_mode",
                "display": "Point Update Mode",
                "hint": {
                  "type": "str",
                  "choices": [
                    "none",
                    "append",
                    "update"
                  ]
                },
                "short_description": "Update the OPC data points. none: do not update points. append: append new points. update: append new points and delete off-line points.",
                "description": "Update the OPC data points. none: do not update points. append: append new points. update: append new points and delete off-line points.\n",
                "value": "none"
              },
              {
                "name": "update_interval",
                "display": "Point Update Interval",
                "hint": {
                  "type": "integer",
                  "min": 60,
                  "max": 2147483647
                },
                "short_description": "Update the OPC data points interval in seconds.",
                "description": "Update the OPC data points interval in seconds.\n",
                "value": "600"
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "log_level",
              "display": "Log Level",
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
              "description": "Adjust the log level of the data source as required. This parameter does not always take effect.",
              "value": "info"
            },
            {
              "name": "write_concurrency",
              "display": "Write Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 128
              },
              "description": "The number of concurrent write requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 10000
              },
              "description": "The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "1000"
            },
            {
              "name": "batch_timeout",
              "display": "Batch Timeout",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 60
              },
              "description": "The maximum time(in seconds) to wait before sending a batch of data points. The default value is 1s. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "1"
            },
            {
              "name": "keep_raw_data",
              "display": "Keep Raw Data",
              "hint": {
                "type": "bool"
              },
              "description": "Whether to keep the raw data. If enabled, the raw data will be stored.\n"
            },
            {
              "name": "keep_raw_data_days",
              "display": "Max Keep Days",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 365
              },
              "description": "The number of days to keep the raw data. The default value is 1 day.\n",
              "value": "1",
              "requires": "keep_raw_data"
            },
            {
              "name": "keep_raw_data_dir",
              "display": "Raw Data Directory",
              "hint": {
                "type": "str"
              },
              "description": "The directory to store the raw data. The default value is `$DATA_DIR/tasks/:id/rawdata/`.\n",
              "placeholder": "$DATA_DIR/tasks/:id/rawdata/",
              "requires": "keep_raw_data"
            }
          ]
        },
        "datasets": {
          "name": "Data Sets",
          "description": "Data points in OPC server to collect.",
          "value": "csv_config_file",
          "categories": [
            {
              "category": "csv_config_file",
              "display": "Upload CSV",
              "description": "OPC DataIn task uses a csv file to define the mapping rules for each data point to the TDengine table:\n\n(1) tag_name: required, the id of the data point on the OPC DA server;\n\n(2) stable: required. TDengine super table corresponding to data points;\n\n(3) tbname: required. TDengine subtable corresponding to the data point;\n\n(4) enable: optional. The default value is '1', which specifies whether to collect data at this point. 0- Do not collect and delete the corresponding sub-table, 1- collect the point data, create a sub-table when there is no sub-table;\n\n(5) value_col: optional. The default value is val. The column name corresponding to the data point collection value in TDengine;\n\n(6) value_transform: optional, the transformation function executed in taosX for data point acquisition values. Currently, only numerical calculation expressions are supported. See expr expression description in transform document for details.\n\n(7) type: optional. The default value is the source data type. The data type of the data point collection value, which can be used to replace the placeholder {type} in the supertable name;\n\n(8) quality_col: optional, the column name corresponding to the quality of data point collection value in TDengine;\n\n(9) ts_col/received_ts_col: required. TDengine timestamp primary key definition: If only ts_col exists, the original timestamp is used as the primary key. If only received_ts_col exists, the collection timestamp is used as the primary key. If both columns exist, the first timestamp column is used as the primary key.\n\n(10) ts_transform: optional, the original timestamp transform function, refer to the description of the transform numerical calculation expression expr;\n\n(11) received_ts_transform: optional, collect data timestamp transform function, refer to the description of Transform numerical calculation expression expr;\n\n(12) tag::VARCHAR(200)::name: Multiple tag columns are optional or configurable. The Tag column corresponding to the data point in TDengine; tag is reserved keyword, indicating that the column is a tag column. VARCHAR(200) indicates the type of the tag, or any other valid type. name is the column name of the tag.\n\nFor more rules, please refer to the <a target=\"_blank\" href=\"/docs-en/enterprise/datain/opcda\">enterprise version document</a>.\n",
              "target": {
                "name": "csv_config_file",
                "description": "OPC DA point configuration list.\n",
                "required": true,
                "multiple": true,
                "editable": true,
                "selectable": true
              }
            },
            {
              "category": "select_all_points",
              "display": "Data Points",
              "description": "OPC DA point configuration file.\n",
              "target": {
                "name": "select_all_points",
                "description": "Select data points that meet specified conditions on the OPC server.\n",
                "required": true,
                "multiple": true,
                "editable": true,
                "selectable": true
              },
              "params": [
                {
                  "name": "root",
                  "display": "Root node",
                  "hint": {
                    "type": "str"
                  },
                  "description": "Query all child nodes starting from this node.\n",
                  "placeholder": "For example root.parent"
                },
                {
                  "name": "pattern",
                  "display": "Regex pattern",
                  "hint": {
                    "type": "str"
                  },
                  "description": "Match the data point TagName.\n"
                },
                {
                  "name": "super_table_expression",
                  "display": "Super Table Name",
                  "hint": {
                    "type": "str"
                  },
                  "description": "Support <super table prefix>_{type} pattern, {type} is the data type of the OPC point.\n",
                  "required": true,
                  "value": "opc_{type}"
                },
                {
                  "name": "child_table_expression",
                  "display": "Table Name",
                  "hint": {
                    "type": "str"
                  },
                  "description": "Support <child table prefix>_{tag_name} pattern, {tag_name} is the name of the OPC point.\n",
                  "required": true,
                  "value": "t_{tag_name}"
                },
                {
                  "name": "table_primary_key",
                  "display": "Primary Key",
                  "hint": {
                    "type": "str",
                    "choices": [
                      "original_ts",
                      "received_ts"
                    ]
                  },
                  "description": "The selected value will be the primary key of target table, original_ts represents the time when OPC service receive data from data points, and received_ts represents the time when the taosX task received data from OPC server.\n",
                  "required": false,
                  "value": "original_ts"
                },
                {
                  "name": "table_primary_key_alias",
                  "display": "Primary Key Name",
                  "hint": {
                    "type": "str"
                  },
                  "description": "The primary key column name in the target table.\n",
                  "required": false,
                  "value": "ts"
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
        "license_id": "influxdb",
        "description": "InfluxDB is a popular open-source time-series database that is optimized for handling large volumes of timestamped data.\n\nTDengine can efficiently read the data in InfluxDB and write it to TDengine through the InfluxDB connector to achieve historical data migration or real-time data synchronization.\n",
        "options": {
          "host": {
            "required": true,
            "display": "IP address",
            "description": "The access address of InfluxDB.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "Port",
            "description": "The port of InfluxDB",
            "placeholder": "8086",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "The port number ranges from 0 to 65535",
          },
          "subject": {}
        },
        "protocol": {
          "display": "Protocol",
          "description": "The protocol of the InfluxDB connection, please choose according to the actual situation, otherwise the task cannot run normally.",
          "choices": [
            {
              "name": "http",
              "display": "HTTP Protocol"
            },
            {
              "name": "https",
              "display": "HTTPS Protocol"
            }
          ],
          "value": "http"
        },
        "authentication": {
          "display": "Authentication",
          "description": "Authentication is the process of verifying the identity before granting access to InfluxDB.",
          "value": "2.x",
          "alternatives": [
            {
              "name": "1.x",
              "display": "Version 1.x",
              "params": [
                {
                  "name": "version",
                  "display": "Version",
                  "hint": {
                    "type": "str",
                    "choices": [
                      "1.8",
                      "1.7"
                    ]
                  },
                  "description": "The version of InfluxDB, due to interface differences between versions, please choose according to the actual situation.",
                  "required": true,
                  "placeholder": "Please select the version of InfluxDB"
                },
                {
                  "name": "username",
                  "display": "Username",
                  "hint": "str",
                  "description": "This user must have permission to read anything in this organization.",
                  "required": true,
                  "placeholder": "Please input a username in the InfluxDB"
                },
                {
                  "name": "password",
                  "display": "Password",
                  "hint": "str",
                  "description": "Verification password for the above user.",
                  "required": true,
                  "placeholder": "Please input the password for the above user"
                }
              ]
            },
            {
              "name": "2.x",
              "display": "Version 2.x",
              "params": [
                {
                  "name": "version",
                  "display": "Version",
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
                  "description": "The version of InfluxDB, due to interface differences between versions, please choose according to the actual situation.",
                  "required": true,
                  "placeholder": "Please select the version of InfluxDB"
                },
                {
                  "name": "orgId",
                  "display": "Organization ID",
                  "hint": "str",
                  "description": "It's a hex number string generated by InfluxDB, not Organization name, please copy from InfluxDB organization->about page and paste it here.",
                  "required": true,
                  "placeholder": "Please input your organization id in the InfluxDB"
                },
                {
                  "name": "token",
                  "display": "Token",
                  "hint": "str",
                  "description": "This token must have permission to read anything in this organization.",
                  "required": true,
                  "placeholder": "Please input your access token in the InfluxDB"
                },
                {
                  "name": "addDbrp",
                  "display": "Add DBRP",
                  "hint": {
                    "type": "bool"
                  },
                  "description": "InfluxQL requires a database and retention policy (DBRP) combination in order to query data. In InfluxDB Cloud and some 2.x require manual addition of this mapping relationship. By turning on this switch, the connector can be automatically added during task execution.",
                  "value": "false"
                }
              ]
            }
          ]
        },
        "groups": [
          {
            "name": "task",
            "display": "Task",
            "display_order": 1,
            "short_description": "Configure the data migration task",
            "description": "Configure the data migration task",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "bucket",
                "display": "Bucket",
                "hint": {
                  "type": "str",
                  "choices": [
                    "--NONE--"
                  ]
                },
                "short_description": "A bucket in the InfluxDB is a namespace for storing data, and each task needs to specify a bucket.",
                "description": "A bucket in the InfluxDB is a namespace for storing data, and each task needs to specify a bucket.",
                "required": true,
                "placeholder": "Please select the bucket"
              },
              {
                "name": "measurements",
                "display": "Measurements",
                "hint": {
                  "type": "str",
                  "choices": [
                    "--NONE--"
                  ]
                },
                "short_description": "Measurements in the above bucket, select one or more specified measurements to migrate, if empty, migrate all.",
                "description": "Measurements in the above bucket, select one or more specified measurements to migrate, if empty, migrate all.",
                "multiple": true,
                "editable": true,
                "placeholder": "Please select the measurements"
              },
              {
                "name": "beginTime",
                "display": "Data Begin Time",
                "hint": "time",
                "short_description": "The starting time of the data, and the task only reads data from the specified time and after.",
                "description": "The starting time of the data, and the task only reads data from the specified time and after.",
                "required": true,
                "placeholder": "YYYY-MM-DD HH:mm:ss"
              },
              {
                "name": "endTime",
                "display": "Data End Time",
                "hint": "time",
                "short_description": "The stopping time of the data, and the task only reads the data at the specified time and before, If a future time is specified, the task will continue until the deadline is reached. If not specified, the task will continue until it is manually terminated.",
                "description": "The stopping time of the data, and the task only reads the data at the specified time and before, If a future time is specified, the task will continue until the deadline is reached. If not specified, the task will continue until it is manually terminated.",
                "placeholder": "YYYY-MM-DD HH:mm:ss"
              },
              {
                "name": "readWindow",
                "display": "Time range per read in minutes",
                "hint": {
                  "type": "integer",
                  "min": 1,
                  "max": 6000
                },
                "short_description": "The maximum time range every time when retrieving data from InfluxDB.",
                "description": "The maximum time range every time when retrieving data from InfluxDB.",
                "placeholder": "Please input the time range",
                "value": "60"
              },
              {
                "name": "delay",
                "display": "Delay in seconds",
                "hint": {
                  "type": "integer",
                  "min": 1,
                  "max": 30
                },
                "short_description": "To migrate the out of order data, TDengine connector always waits for time specified here before reading them.",
                "description": "To migrate the out of order data, TDengine connector always waits for time specified here before reading them.",
                "placeholder": "Please input the delay",
                "value": "10"
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "log_level",
              "display": "Log Level",
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
              "description": "Adjust the log level of the data source as required. This parameter does not always take effect.",
              "value": "info"
            },
            {
              "name": "read_concurrency",
              "display": "Read Concurrency",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 100
              },
              "description": "The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "50"
            },
            {
              "name": "write_concurrency",
              "display": "Write Concurrency",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 500
              },
              "description": "The number of concurrent write requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "50"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 10000
              },
              "description": "The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "5000"
            },
            {
              "name": "batch_timeout",
              "display": "Batch Timeout",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 10000
              },
              "description": "The maximum time(in milliseconds) to wait before sending a batch of data points. The default value is 1000ms. If the data source is slow to respond, you can increase this value appropriately.\n",
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
        "license_id": "opentsdb",
        "description": "OpenTSDB is a real-time monitoring information collection and display platform based on the HBase system.\n\nTDengine can efficiently read the data in OpenTSDB and write it to TDengine through the OpenTSDB connector to achieve historical data migration or real-time data synchronization.\n",
        "options": {
          "host": {
            "required": true,
            "display": "IP address",
            "description": "The access address of OpenTSDB.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "Port",
            "description": "The port of OpenTSDB",
            "placeholder": "4242",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "The port number ranges from 0 to 65535",
          },
          "subject": {}
        },
        "protocol": {
          "display": "Protocol",
          "description": "The protocol of the OpenTSDB connection, please choose according to the actual situation, otherwise the task cannot run normally.",
          "choices": [
            {
              "name": "http",
              "display": "HTTP Protocol"
            },
            {
              "name": "https",
              "display": "HTTPS Protocol"
            }
          ],
          "value": "http"
        },
        "groups": [
          {
            "name": "task",
            "display": "Task",
            "display_order": 1,
            "short_description": "Configure the data migration task",
            "description": "Configure the data migration task",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "metrics",
                "display": "Metrics",
                "hint": {
                  "type": "str",
                  "choices": [
                    "--NONE--"
                  ]
                },
                "short_description": "Metrics in OpenTSDB, select one or more specified metrics to migrate, if empty, migrate all.",
                "description": "Metrics in OpenTSDB, select one or more specified metrics to migrate, if empty, migrate all.",
                "multiple": true,
                "editable": true,
                "placeholder": "Please select the Metrics"
              },
              {
                "name": "beginTime",
                "display": "Data Begin Time",
                "hint": "time",
                "short_description": "The starting time of the data, and the task only reads data from the specified time and after.",
                "description": "The starting time of the data, and the task only reads data from the specified time and after.",
                "required": true,
                "placeholder": "YYYY-MM-DD HH:mm:ss"
              },
              {
                "name": "endTime",
                "display": "Data End Time",
                "hint": "time",
                "short_description": "The stopping time of the data, and the task only reads the data at the specified time and before, If a future time is specified, the task will continue until the deadline is reached. If not specified, the task will continue until it is manually terminated.",
                "description": "The stopping time of the data, and the task only reads the data at the specified time and before, If a future time is specified, the task will continue until the deadline is reached. If not specified, the task will continue until it is manually terminated.",
                "placeholder": "YYYY-MM-DD HH:mm:ss"
              },
              {
                "name": "readWindow",
                "display": "Time range per read in minutes",
                "hint": {
                  "type": "integer",
                  "min": 1,
                  "max": 6000
                },
                "short_description": "The maximum time range every time when retrieving data from OpenTSDB.",
                "description": "The maximum time range every time when retrieving data from OpenTSDB.",
                "placeholder": "Please input the time range",
                "value": "60"
              },
              {
                "name": "delay",
                "display": "Delay in seconds",
                "hint": {
                  "type": "integer",
                  "min": 1,
                  "max": 30
                },
                "short_description": "To migrate the out of order data, TDengine connector always waits for time specified here before reading them.",
                "description": "To migrate the out of order data, TDengine connector always waits for time specified here before reading them.",
                "placeholder": "Please input the delay",
                "value": "10"
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "log_level",
              "display": "Log Level",
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
              "description": "Adjust the log level of the data source as required. This parameter does not always take effect.",
              "value": "info"
            },
            {
              "name": "read_concurrency",
              "display": "Read Concurrency",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 100
              },
              "description": "The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "50"
            },
            {
              "name": "write_concurrency",
              "display": "Write Concurrency",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 500
              },
              "description": "The number of concurrent write requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "50"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 10000
              },
              "description": "The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "5000"
            },
            {
              "name": "batch_timeout",
              "display": "Batch Timeout",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 10000
              },
              "description": "The maximum time(in milliseconds) to wait before sending a batch of data points. The default value is 1000ms. If the data source is slow to respond, you can increase this value appropriately.\n",
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
        "license_id": "mqtt",
        "description": "MQTT stands for Message Queuing Telemetry Transport. It is a lightweight messaging protocol that is easy to implement and use. It is ideal for connecting devices with limited resources, such as battery-powered devices or devices with low bandwidth. MQTT is also a good choice for applications where latency is important, such as real-time control systems.\n\nMQTT works by using a publish/subscribe model. This means that devices can publish messages to topics, and other devices can subscribe to those topics to receive the messages. This makes it easy to decouple devices from each other, and to scale up applications as needed.\n\nMQTT is a popular choice for IoT applications. It is supported by a wide range of devices and platforms, and there are many open source and commercial implementations available.\n\ntaosX could subscribe data from MQTT broker by a connector plugin.\n\nCheck the help message in each part to see the details.\n",
        "options": {
          "host": {
            "required": true,
            "display": "MQTT Host",
            "description": "MQTT server endpoint. e.g: 127.0.0.1\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.\n",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "MQTT Port",
            "description": "MQTT server port",
            "placeholder": "1883",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "The port number ranges from 0 to 65535",
          }
        },
        "authentication": {
          "display": "Authentication",
          "value": "plain",
          "alternatives": [
            {
              "name": "plain",
              "display": "Username Password",
              "username": {
                "display": "Username",
                "placeholder": "username"
              },
              "password": {
                "display": "Password",
                "placeholder": "password"
              }
            }
          ]
        },
        "groups": [
          {
            "name": "Enable SSL",
            "display_order": 0,
            "short_description": "Use self-signed certificate file and private key.",
            "description": "Use self-signed certificate file and private key.",
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
                "short_description": "CA file.",
                "description": "CA file.",
                "required": true
              },
              {
                "name": "cert",
                "display": "Client certificate file",
                "hint": {
                  "type": "file"
                },
                "short_description": "Client certificate file.",
                "description": "Client certificate file.",
                "required": true
              },
              {
                "name": "cert_key",
                "display": "Client key file",
                "hint": {
                  "type": "file"
                },
                "short_description": "Client key file.",
                "description": "Client key file.",
                "required": true
              }
            ]
          },
          {
            "name": "Collect",
            "display_order": 1,
            "short_description": "Some configurations used in collection task.",
            "description": "Some configurations used in collection task.",
            "collapsible": false,
            "connection_option": true,
            "params": [
              {
                "name": "version",
                "display": "MQTT protocol version",
                "hint": {
                  "type": "str",
                  "choices": [
                    "3.1",
                    "3.1.1",
                    "5.0"
                  ]
                },
                "short_description": "MQTT protocol version.",
                "description": "MQTT protocol version.",
                "required": true,
                "value": "3.1"
              },
              {
                "name": "client_id",
                "display": "Client ID",
                "hint": {
                  "type": "str"
                },
                "required": true,
                "short_description": "Client id used to connect to mqtt broker.",
                "description": "Client id used to connect to mqtt broker.",
                "placeholder": "for example: client_id"
              },
              {
                "name": "keep_alive",
                "display": "Keep Alive",
                "hint": {
                  "type": "integer",
                  "min": 1
                },
                "short_description": "If the broker does not receive any messages from the",
                "description": "If the broker does not receive any messages from the<br>\nclient within the keep alive interval, it will assume<br>\nthat the client has disconnected and will close the<br>\nconnection.\n",
                "placeholder": "10",
                "value": "60"
              },
              {
                "name": "clean_session",
                "display": "Clean Session",
                "hint": {
                  "type": "bool"
                },
                "short_description": "True means that the server will forget all information",
                "description": "True means that the server will forget all information<br>\nabout the session, including the client's subscriptions.<br>\nThe default value for the clean session flag is true.<br>\n",
                "value": "true"
              },
              {
                "name": "topics",
                "display": "Topics Qos Config",
                "hint": {
                  "type": "str"
                },
                "short_description": "Input format: `<topic name>::<QoS>`, QoS can be 0/1/2, if subscribe multiple topics, use commas to separate them, e.g: topic1::0,topic2::1",
                "description": "Input format: `<topic name>::<QoS>`, QoS can be 0/1/2, if subscribe multiple topics, use commas to separate them, e.g: topic1::0,topic2::1\n",
                "required": true,
                "pattern": "^(?:\\S+::[0-2],)*\\S+::[0-2]$",
                "patternMsg": "Input format error, please refer to: `<topic name>::<QoS>`, QoS can be 0/1/2, e.g: `topic1::0,topic2::1`",
                "placeholder": "topic1::0,topic2::1",
                "edit_disabled": true,
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "log_level",
              "display": "Log Level",
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
              "description": "Adjust the log level of the data source as required. This parameter does not always take effect.",
              "value": "info"
            },
            {
              "name": "read_concurrency",
              "display": "Read Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 1000
              },
              "description": "The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0",
              "hidden": true
            },
            {
              "name": "write_concurrency",
              "display": "Write Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 1000
              },
              "description": "The number of concurrent write requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0",
              "hidden": true
            },
            {
              "name": "batch_timeout",
              "display": "Batch Timeout",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 60
              },
              "description": "The maximum time(in seconds) to wait before sending a batch of data points. The default value is 1s. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "1",
              "hidden": true
            },
            {
              "name": "keep_raw_data",
              "display": "Keep Raw Data",
              "hint": {
                "type": "bool"
              },
              "description": "Whether to keep the raw data. If enabled, the raw data will be stored.\n"
            },
            {
              "name": "keep_raw_data_days",
              "display": "Max Keep Days",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 365
              },
              "description": "The number of days to keep the raw data. The default value is 1 day.\n",
              "value": "1",
              "requires": "keep_raw_data"
            },
            {
              "name": "keep_raw_data_dir",
              "display": "Raw Data Directory",
              "hint": {
                "type": "str"
              },
              "description": "The directory to store the raw data. The default value is `$DATA_DIR/tasks/:id/rawdata/`.\n",
              "placeholder": "$DATA_DIR/tasks/:id/rawdata/",
              "requires": "keep_raw_data"
            }
          ]
        },
        "parser": {
          "display": "Payload Transformation",
          "required": true,
          "description": "MQTT will report exactly four fields of data for each data stream:<br>\n\n- **ts**: the collect timestamp.\n- **topic**: the topic name to subscribe.\n- **qos**: the QoS of the message, usually 0, 1, 2.\n- **payload**: the data payload of the message.\n\ntaosX could parse the payload with JSON extractor and let users to specify the data model in the database, for example, the table name pattern and stable name pattern, field names as tags or field names as columns.\n",
          "fields": [
            {
              "name": "ts",
              "description": "Timestamp.",
              "type": "timestamp"
            },
            {
              "name": "topic",
              "description": "Topic name.",
              "type": "varchar"
            },
            {
              "name": "qos",
              "description": "QoS, one of 0/1/2.",
              "type": "int"
            },
            {
              "name": "payload",
              "description": "Payload",
              "type": "varchar"
            }
          ]
        }
      },
      {
        "id": "kafka",
        "type": "uri",
        "name": "Kafka",
        "license_id": "kafka",
        "description": "Apache Kafka is an open-source distributed streaming system used for stream processing, real-time data pipelines, and data integration at scale.\nTDengine can efficiently read the data from Kafka and write to TDengine to achieve historical data migration or real-time data streaming.\n",
        "options": {
          "params": [
            {
              "host": {
                "name": "host",
                "required": true,
                "display": "bootstrap-server",
                "description": "kafka bootstrap-server.\n<br/>If you configure multiple Kafka servers, all Kafka servers must belong to the same cluster.\n<br/>If using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.",
                "placeholder": "127.0.0.1",
              },
              "port": {
                "name": "port",
                "required": true,
                "display": "Port",
                "description": "Kafka Server Port",
                "placeholder": "9092",
                "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
                "patternMsg": "The port number ranges from 0 to 65535",
              },
            }
          ]
        },
        "groups": [
          {
            "name": "SASL Authentication",
            "display_order": 1,
            "short_description": "Simple Authentication and Security Layer.",
            "description": "Simple Authentication and Security Layer.",
            "collapsible": true,
            "connection_option": true,
            "collapsed": false,
            "params": [
              {
                "name": "sasl_mechanism",
                "display": "Mechanism",
                "hint": {
                  "type": "str",
                  "choices": [
                    "PLAIN",
                    "SCRAM-SHA-256",
                    "GSSAPI"
                  ]
                },
                "short_description": "SASL authentication mechanism.",
                "description": "SASL authentication mechanism.",
                "required": true,
                "value": "PLAIN"
              },
              {
                "name": "sasl_username",
                "display": "Username",
                "hint": {
                  "type": "str"
                },
                "short_description": "The username for SASL authentication mechanism.",
                "description": "The username for SASL authentication mechanism.",
                "required": true
              },
              {
                "name": "sasl_password",
                "display": "Password",
                "hint": {
                  "type": "password"
                },
                "short_description": "The password for SASL authentication mechanism.",
                "description": "The password for SASL authentication mechanism.",
                "required": true
              },
              {
                "name": "sasl_kerberos_service_name",
                "display": "Kerberos Service Name",
                "description": "The Kerberos service name for GSSAPI authentication mechanism.",
                "placeholder": "for example: kafka",
                "required": true,
                "hint": {
                  "type": "str"
                }
              },
              {
                "name": "sasl_kerberos_principal",
                "display": " Kerberos Principal",
                "description": "The Kerberos principal for GSSAPI authentication mechanism.",
                "placeholder": "for example: kafkaclient",
                "required": true,
                "hint": {
                  "type": "str"
                }
              },
              {
                "name": "sasl_kerberos_kinit_cmd",
                "display": "Kerberos Init Command",
                "description": "The Kerberos init command for GSSAPI authentication mechanism.",
                "placeholder": "for example: kinit -R -t '%{sasl.kerberos.keytab}' -k %{sasl.kerberos.principal}",
                "required": false,
                "hint": {
                  "type": "str"
                }
              },
              {
                "name": "sasl_kerberos_keytab",
                "display": "Kerberos Keytab",
                "description": "The Kerberos keytab for GSSAPI authentication mechanism.",
                "required": true,
                "hint": {
                  "type": "file"
                }
              }
            ]
          },
          {
            "name": "Enable SSL",
            "display_order": 2,
            "short_description": "Use self-signed certificate file and private key.",
            "description": "Use self-signed certificate file and private key.",
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
                "short_description": "CA certificate file(PEM format) for verifying the broker's key.",
                "description": "CA certificate file(PEM format) for verifying the broker's key.",
                "required": true
              },
              {
                "name": "ca_password",
                "display": "CA Password",
                "hint": {
                  "type": "password"
                },
                "short_description": "CA private key passphrase.",
                "description": "CA private key passphrase.",
                "required": true
              },
              {
                "name": "cert",
                "display": "Client certificate",
                "hint": {
                  "type": "file"
                },
                "short_description": "Client's public key file(PEM format) used for authentication.",
                "description": "Client's public key file(PEM format) used for authentication.",
                "required": true
              },
              {
                "name": "cert_key",
                "display": "Client key",
                "hint": {
                  "type": "file"
                },
                "short_description": "Client's private key file(PEM format) used for authentication.",
                "description": "Client's private key file(PEM format) used for authentication.",
                "required": true
              }
            ]
          },
          {
            "name": "Collect",
            "display_order": 3,
            "short_description": "Configurations for collecting data.",
            "description": "Configurations for collecting data.",
            "collapsible": false,
            "connection_option": true,
            "params": [
              {
                "name": "timeout",
                "display": "Timeout",
                "hint": {
                  "type": "timeout",
                  "choices": [
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                    {
                      "value": "ms",
                      "label": "Millisecond"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "Specifies the timeout of the Kafka Source. When no data is consumed from Kafka, the data migration task will exit after timeout. The default value is 0 ms.",
                "description": "Specifies the timeout of the Kafka Source. When no data is consumed from Kafka, the data migration task will exit after timeout. The default value is 0 ms.\nWhen use `timeout=0`, it will wait for an usable message forever and never stop the subscription until any error caused.\n",
                "required": false,
                "placeholder": "The value is an integer ranging [0,60000]",
                "type_value": "ms",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              },
              {
                "name": "topics",
                "display": "Topics",
                "hint": {
                  "type": "str"
                },
                "short_description": "Specifies one topic or several topics to consume. e.g. topics=tp1,tp2",
                "description": "Specifies one topic or several topics to consume. e.g. topics=tp1,tp2\n",
                "required": true,
                "placeholder": "tp1,tp2",
                "edit_disabled": true,
              },
              {
                "name": "client_id",
                "display": "Client ID",
                "hint": {
                  "type": "str"
                },
                "required": true,
                "short_description": "Client id used to connect to Kafka broker.",
                "description": "Client id used to connect to Kafka broker.",
                "placeholder": "for example: client_id"
              },
              {
                "name": "group",
                "display": "Group ID",
                "hint": {
                  "type": "str"
                },
                "required": true,
                "short_description": "Kafka Group ID。",
                "description": "Kafka Group ID。",
                "placeholder": "for example: group_id"
              },
              {
                "name": "client_id",
                "display": "Client id",
                "hint": {
                  "type": "str"
                },
                "required": true,
                "short_description": "Client id used to connect to Kafka broker.",
                "description": "Client id used to connect to Kafka broker.",
                "placeholder": "client_id"
              },
              {
                "name": "group",
                "display": "Group ID",
                "hint": {
                  "type": "str"
                },
                "required": true,
                "short_description": "Kafka Group ID。",
                "description": "Kafka Group ID。",
                "placeholder": "group_id"
              },
              {
                "name": "fallback_offset",
                "display": "Fallback Offset",
                "hint": {
                  "type": "str",
                  "choices": [
                    "Earliest",
                    "Latest"
                  ]
                },
                "short_description": "Possible values when querying a topic’s offset.",
                "description": "Possible values when querying a topic’s offset.\n* `Earliest`: Receive the earliest available offset. \n* `Latest`: Receive the latest offset. \n* default is Earliest.",
                "required": false,
                "placeholder": "Earliest",
                "value": "Earliest"
              },
              // {
              //   "name": "fetch_max_wait_time",
              //   "display": "Waiting Timeout",
              //   "hint": {
              //     "type": "integer",
              //     "min": 0,
              //     "max": 300
              //   },
              //   "short_description": "A timeout for polling data from the topic.",
              //   "description": "A timeout for polling data from the topic.\n\nThe default value `0`: means waiting for valid message without timeout,the unit is s.\n",
              //   "required": false,
              //   "placeholder": "",
              //   "value": 0
              // }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "read_concurrency",
              "display": "Read Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 1000
              },
              "description": "The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 100000
              },
              "description": "The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "1000"
            }
          ]
        },
        "parser": {
          "display": "Payload Transformation",
          "required": true,
          "description": "Kafka will report exactly five fields of data for each data stream:<br>\n\n- **ts**: the collect timestamp.<br>\n- **topic**: the topic name to subscribe.<br>\n- **partition**: the topic partition.<br>\n- **offset**: the message offset in the topic.<br>\n- **key**: the message offset in the topic.<br>\n- **value**: the data payload of the message.<br>\n\ntaosX could parse the payload with JSON extractor and let users to specify the<br>\ndata model in the database, for example, the table name pattern and stable name<br>\npattern, field names as tags or field names as columns.\n",
          "fields": [
            {
              "name": "ts",
              "description": "Timestamp.",
              "type": "timestamp"
            },
            {
              "name": "topic",
              "description": "Topic name.",
              "type": "varchar"
            },
            {
              "name": "partition",
              "description": "Topic partition.",
              "type": "int"
            },
            {
              "name": "offset",
              "description": "Message offset.",
              "type": "bigint"
            },
            {
              "name": "key",
              "description": "Message key.",
              "type": "varchar"
            },
            {
              "name": "value",
              "description": "Value",
              "type": "varchar"
            }
          ]
        }
      },
      {
        "id": "csv",
        "type": "path",
        "name": "CSV",
        "license_id": "csv",
        "description": "Import a file or a collection of files in CSV format to TDengine.\n",
        "strict": true,
        "options": {
          "path": {
            "required": true,
            "display": "Path",
            "description": "CSV file path or directory.",
            "placeholder": "Example: a.csv,b.csv"
          }
        },
        "groups": [
          {
            "name": "CSV Options",
            "display_order": 1,
            "short_description": "CSV reading options",
            "description": "CSV reading options",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "has_header",
                "display": "Include Header",
                "hint": {
                  "type": "bool"
                },
                "short_description": "If including header, the first row will be treated as column information.",
                "description": "If including header, the first row will be treated as column information.\n"
              },
              {
                "name": "skip",
                "display": "Skip the first N lines",
                "hint": {
                  "type": "integer",
                  "min": 0
                },
                "short_description": "Skip the first N lines for each CSV file.",
                "description": "Skip the first N lines for each CSV file.",
                "value": "0"
              },
              {
                "name": "delimiter",
                "display": "Delimiter Char",
                "hint": {
                  "type": "str",
                  "choices": [
                    ",",
                    ";"
                  ]
                },
                "short_description": "The field separator in a CSV line.",
                "description": "The field separator in a CSV line.",
                "editable": true,
                "value": ","
              },
              {
                "name": "quote",
                "display": "Quote Char",
                "hint": {
                  "type": "str",
                  "choices": [
                    "\"",
                    "'"
                  ]
                },
                "short_description": "The quote is used to enclose field values.",
                "description": "The quote is used to enclose field values.",
                "editable": true,
                "value": "\""
              },
              {
                "name": "comment",
                "display": "Comment Prefix",
                "hint": {
                  "type": "str",
                  "choices": [
                    "#"
                  ]
                },
                "short_description": "If a line begins with the character given here, then that line will be ignored by the CSV parser.",
                "description": "If a line begins with the character given here, then that line will be ignored by the CSV parser.",
                "editable": true,
                "value": "#"
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "read_concurrency",
              "display": "Read Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 1000
              },
              "description": "The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0",
              "hidden": false
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 100000
              },
              "description": "The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "1000",
              "hidden": false
            }
          ]
        }
      },
      {
        "id": "avevaHistorian",
        "type": "uri",
        "name": "AVEVA Historian",
        "license_id": "avevahistorian",
        "description": "AVEVA Historian process database integrated with operations control enabling access to your process, alarm, and event history data. Wonderware Historian is now AVEVA Historian.\n\nTDengine efficiently reads data from the AVEVA Historian and writes it to TDengine for historical data migration or real-time data synchronization.\n",
        "options": {
          "host": {
            "required": true,
            "display": "Host",
            "description": "AVEVA Historian SQL Server IP address or host name",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "display": "Port",
            "description": "AVEVA Historian SQL Server port",
            "placeholder": "1433",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "The port number ranges from 0 to 65535",
          },
          "subject": {}
        },
        "authentication": {
          "display": "Authentication",
          "description": "Use username and password of AVEVA Historian SQL Server",
          "value": "plain",
          "alternatives": [
            {
              "name": "plain",
              "display": "Username and Password",
              "username": {
                "required": true,
                "display": "Username",
                "placeholder": "aaAdmin"
              },
              "password": {
                "required": true,
                "display": "Password",
                "placeholder": "password"
              }
            }
          ]
        },
        "groups": [
          {
            "name": "Collect",
            "display_order": 1,
            "short_description": "Configure Data Collection Task",
            "description": "Configure Data Collection Task",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "mode",
                "display": "Collection Mode",
                "hint": {
                  "type": "str",
                  "choices": [
                    "synchronize",
                    "migrate"
                  ]
                },
                "short_description": "Collection mode. The optional values are `synchronize` and `migrate`.",
                "description": "Collection mode. The optional values are `synchronize` and `migrate`.\n",
                "required": true,
                "placeholder": "synchronize",
                "value": "synchronize"
              },
              {
                "name": "table",
                "display": "Table",
                "hint": {
                  "type": "str",
                  "choices": [
                    "Runtime.dbo.History",
                    "Runtime.dbo.Live"
                  ]
                },
                "short_description": "Retrieves database tables in historian, with historical data in Runtime.dbo.History and real-time data in Runtime.dbo.Live.",
                "description": "Retrieves database tables in historian, with historical data in Runtime.dbo.History and real-time data in Runtime.dbo.Live.\n",
                "required": true,
                "placeholder": "Runtime.dbo.History"
              },
              {
                "name": "tags",
                "display": "Tags",
                "hint": {
                  "type": "str"
                },
                "short_description": "tags to be migrated/synchronized. `*` indicates that all tags.",
                "description": "tags to be migrated/synchronized. `*` indicates that all tags.\n",
                "required": false,
                "placeholder": "*",
                "value": "*"
              },
              {
                "name": "tagListSize",
                "display": "Tag List Size",
                "hint": {
                  "type": "integer",
                  "min": 1,
                  "max": 1000
                },
                "short_description": "When `table` is `Runtime.dbo.History` and TagName in `tags` exceeds the `tagListSize`, tags are divided according to each group of `tagListSize`. The `tagListSize` is used to partition TagName to improve query efficiency during data migration/synchronization.  The default value of `tagListSize` is 10.",
                "description": "When `table` is `Runtime.dbo.History` and TagName in `tags` exceeds the `tagListSize`, tags are divided according to each group of `tagListSize`. The `tagListSize` is used to partition TagName to improve query efficiency during data migration/synchronization.  The default value of `tagListSize` is 10.\n",
                "required": false,
                "placeholder": "10",
                "value": "10"
              },
              {
                "name": "beginDateTime",
                "display": "Begin Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "The start time of the task is in rfc3339 format.",
                "description": "The start time of the task is in rfc3339 format.",
                "required": true,
                "placeholder": "e.g., 2023-01-01T00:00:00.000Z"
              },
              {
                "name": "endDateTime",
                "display": "End Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "The end time of the task is in rfc3339 format.",
                "description": "The end time of the task is in rfc3339 format.",
                "required": false,
                "placeholder": "e.g., 2023-01-01T00:00:00.000Z"
              },
              {
                "name": "timeWindow",
                "display": "Time Window",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "y",
                      "label": "Year"
                    },
                    {
                      "value": "mo",
                      "label": "Month"
                    },
                    {
                      "value": "d",
                      "label": "Day"
                    },
                    {
                      "value": "w",
                      "label": "Week"
                    },
                    {
                      "value": "h",
                      "label": "Hours"
                    },
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                    {
                      "value": "ms",
                      "label": "Millisecond"
                    },
                    {
                      "value": "u",
                      "label": "Microsecond"
                    },
                    {
                      "value": "ns",
                      "label": "Nanoseconds"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "Time window for historical data migration.",
                "description": "Time window for historical data migration.",
                "required": false,
                "placeholder": "The value is an integer ranging [0,60000]",
                "value": "1",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              },
              {
                "name": "retrieveInterval",
                "display": "Retrieve Interval",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "Day"
                    },
                    {
                      "value": "h",
                      "label": "Hours"
                    },
                    {
                      "value": "m",
                      "label": "Mniute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                    {
                      "value": "ms",
                      "label": "millisecond"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "Pull interval for real-time data synchronization.",
                "description": "Pull interval for real-time data synchronization.",
                "required": false,
                "placeholder": "The value is an integer ranging [0,60000]",
                "value": "10",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              },
              {
                "name": "tolerance",
                "display": "Tolerance",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "Day"
                    },
                    {
                      "value": "h",
                      "label": "Hours"
                    },
                    {
                      "value": "m",
                      "label": "Mniute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                    {
                      "value": "ms",
                      "label": "millisecond"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "The maximum time limit for tolerating out-of-order data delay.",
                "description": "The maximum time limit for tolerating out-of-order data delay.",
                "required": false,
                "placeholder": "The value is an integer ranging [0,60000]",
                "value": "0",
                "type_value": "ms",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "read_concurrency",
              "display": "Read Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 1000
              },
              "description": "The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 100000
              },
              "description": "The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "10000"
            },
            {
              "name": "keep_raw_data",
              "display": "Keep Raw Data",
              "hint": {
                "type": "bool"
              },
              "description": "Whether to keep the raw data. If enabled, the raw data will be stored.\n"
            },
            {
              "name": "keep_raw_data_days",
              "display": "Max Keep Days",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 365
              },
              "description": "The number of days to keep the raw data. The default value is 1 day.\n",
              "value": "1",
              "requires": "keep_raw_data"
            },
            {
              "name": "keep_raw_data_dir",
              "display": "Raw Data Directory",
              "hint": {
                "type": "str"
              },
              "description": "The directory to store the raw data. The default value is `$DATA_DIR/tasks/:id/rawdata/`.\n",
              "placeholder": "$DATA_DIR/tasks/:id/rawdata/",
              "requires": "keep_raw_data"
            }
          ]
        },
        "parser": {
          "display": "Payload Transformation",
          "required": true,
          "description": "taosX could let users to specify the data model in the database, for example, the table name pattern <br>\nand stable name pattern, field names as tags or field names as columns.\n",
          "fields": [
            {
              "name": "DateTime",
              "description": "The timestamp of the returned value.",
              "type": "timestamp"
            },
            {
              "name": "TagName",
              "description": "The unique name of the tag.",
              "type": "varchar"
            },
            {
              "name": "Value",
              "description": "The value of the tag at the timestamp. The value is always NULL for string tags.",
              "type": "double"
            },
            {
              "name": "vValue",
              "description": "The value of the analog, discrete, or string tag stored as a sql_variant.",
              "type": "varchar"
            },
            {
              "name": "Quality",
              "description": "The basic data quality indicator associated with the data value.",
              "type": "int"
            },
            {
              "name": "QualityDetail",
              "description": "An internal representation of data quality.",
              "type": "int"
            },
            {
              "name": "OPCQuality",
              "description": "The quality value received from the data source.",
              "type": "int"
            },
            {
              "name": "wwTagKey",
              "description": "The unique numerical identifier of a tag.",
              "type": "int"
            },
            {
              "name": "wwResolution",
              "description": "The sampling rate, in milliseconds, for retrieving the data in cyclic mode.",
              "type": "int"
            },
            {
              "name": "StartDateTime",
              "description": "Start time of the retrieval cycle for which this row is returned.",
              "type": "timestamp"
            },
            {
              "name": "SourceTag",
              "description": "The name of the source tag for a replicated tag at the time this point was stored.",
              "type": "varchar"
            },
            {
              "name": "SourceServer",
              "description": "The name of the server for this replicated tag at the time this point was stored.",
              "type": "varchar"
            }
          ]
        }
      },
      {
        "id": "mysql",
        "type": "uri",
        "name": "MySQL",
        "license_id": "mysql",
        "description": "MySQL is one of the most popular relational database management systems. Due to its small size, fast speed, low overall cost of ownership, especially open source, MySQL is generally chosen as the website database for the development of small and large websites.\n\nTDengine can efficiently read the data in MySQL and write it to TDengine through the MySQL connector to achieve historical data migration or real-time data synchronization.\n",
        "options": {
          "host": {
            "required": true,
            "display": "Host",
            "description": "The access address of MySQL.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "Port",
            "description": "The port of MySQL.",
            "placeholder": "3306",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "The port number ranges from 0 to 65535",
          },
          "subject": {
            "required": true,
            "display": "Database",
            "description": "The name of the database to connect to.",
            "placeholder": "for example: db1"
          }
        },
        "authentication": {
          "display": "Authentication",
          "description": "Authentication is the process of verifying the identity before granting access to MySQL.",
          "value": "plain",
          "alternatives": [
            {
              "name": "plain",
              "display": "Username and Password",
              "username": {
                "required": true,
                "display": "Username",
                "placeholder": "username"
              },
              "password": {
                "required": true,
                "display": "Password",
                "placeholder": "password"
              }
            }
          ]
        },
        "groups": [
          {
            "name": "Connection options",
            "display_order": 1,
            "short_description": "Other connection options.",
            "description": "Other connection options.",
            "collapsible": false,
            "connection_option": true,
            "params": [
              {
                "name": "charset",
                "display": "Character Set",
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
                "short_description": "Set the character set for the connection. The default character set is utf8mb4. MySQL 5.5.3 supports this feature. If you need to connect to an older version, it is recommended to change to utf8.",
                "description": "Set the character set for the connection. The default character set is utf8mb4. MySQL 5.5.3 supports this feature. If you need to connect to an older version, it is recommended to change to utf8.",
                "placeholder": "Please select the database character set",
                "value": "utf8"
              },
              {
                "name": "ssl_mode",
                "display": "SSL Mode",
                "hint": {
                  "type": "str",
                  "choices": [
                    "DISABLED",
                    "PREFERRED",
                    "REQUIRED"
                  ]
                },
                "short_description": "Set whether to negotiate a secure SSL TCP/IP connection with the server or what priority to negotiate with.",
                "description": "Set whether to negotiate a secure SSL TCP/IP connection with the server or what priority to negotiate with.",
                "placeholder": "Please select the SSL mode",
                "value": "PREFERRED"
              }
            ]
          },
          {
            "name": "Data Collection",
            "display_order": 2,
            "short_description": "Data collection related configuration items.",
            "description": "Data collection related configuration items.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "subtable_fields",
                "display": "Subtable Fields",
                "hint": {
                  "type": "str"
                },
                "short_description": "Fields and query statements used for splitting sub tables.",
                "description": "Fields and query statements used for splitting sub tables.",
                "required": false,
                "placeholder": "select distinct col_name1,col_name2 from table",
              },
              {
                "name": "sql",
                "display": "SQL Template",
                "hint": {
                  "type": "str"
                },
                "short_description": "SQL statement used for querying. The SQL statement must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).",
                "description": "SQL statement used for querying. The SQL statement must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).\nSQL uses different placeholders to represent different time format requirements, specifically the following placeholder formats:\n1. `${start}`, `${end}`: Represents the RFC3339 format timestamp, such as: 2024-03-14T08:00:00+0800\n2. `${start_no_tz}`, `${end_no_tz}`: Represents the RFC3339 string without a time zone: 2024-03-14T08:00:00\n3. `${start_date}`, `${end_date}`: Represents only the date, such as: 2024-03-14\n\nIf you use subtable fields, you need to concatenate field placeholders \`and ${col_name1} and ${col_name2}\` in the statement,note that field placeholders are case sensitive and need to be consistent with the fields in the database. If you want to sort by a specific field (recommended in ascending time order), you need to concatenate \`ORDER BY time\` in the statement.\n\nExample:\`SELECT * FROM table WHERE time >= ${start} AND time < ${end} and ${col_name1} and ${col_name2} ORDER BY time\`",
                "required": true,
                "placeholder": "See the description for a complete example",
                "grid_two": true,
              },
              {
                "name": "start",
                "display": "Start Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "Start time for migrating data.",
                "description": "Start time for migrating data.\n",
                "required": true,
                "placeholder": "for example: 2023-01-01 00:00:00"
              },
              {
                "name": "end",
                "display": "End Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "End time for migrating data, can be left blank. If set, the migration task will stop automatically after the task is executed to the end time; if left blank, the real-time data will be synchronized continuously, and the task will not stop automatically.",
                "description": "End time for migrating data, can be left blank. If set, the migration task will stop automatically after the task is executed to the end time; if left blank, the real-time data will be synchronized continuously, and the task will not stop automatically.\n",
                "required": false,
                "placeholder": "for example: 2024-01-01 00:00:00"
              },
              {
                "name": "interval",
                "display": "Query Interval",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "Day"
                    },
                    {
                      "value": "h",
                      "label": "Hours"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "The time interval for segmented queries. The default is 1 day. To avoid querying too much data, a data synchronization subtask will use the query interval to query data in time segments.",
                "description": "The time interval for segmented queries. The default is 1 day. To avoid querying too much data, a data synchronization subtask will use the query interval to query data in time segments.\n",
                "required": false,
                "placeholder": "The value is an integer ranging [0,600]",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              },
              {
                "name": "delay",
                "display": "Delay",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "In the real-time data synchronization scenario, to avoid the loss of delayed written data, each synchronization task will read data before the delay time.",
                "description": "In the real-time data synchronization scenario, to avoid the loss of delayed written data, each synchronization task will read data before the delay time.\n",
                "required": false,
                "placeholder": "The value is an integer ranging [0,60000]",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave these options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "read_concurrency",
              "display": "Read Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 1000
              },
              "description": "The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 100000
              },
              "description": "The number of data points to be written in a single request. The default value is 10000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "10000"
            }
          ]
        },
        "parser": {
          "display": "Data Mapping",
          "required": true,
          "description": "taosX could let users to specify the data model in the database, for example, the table name pattern and stable name pattern, field names as tags or field names as columns.\n",
          "fields": [
            {
              "name": "DateTime",
              "description": "The timestamp of the returned value.",
              "type": "timestamp"
            }
          ]
        }
      },
      {
        "id": "postgres",
        "type": "uri",
        "name": "PostgreSQL",
        "license_id": "postgres",
        "description": "PostgreSQL is a very powerful, open-source client/server relational database management system that has many features found in large commercial RDBMSs, including transactions, subselects, triggers, views, referential integrity, and sophisticated locking functionality.\nTDengine can efficiently read data from PostgreSQL and write it to TDengine to achieve historical data migration or real-time data synchronization.\n",
        "options": {
          "host": {
            "required": true,
            "display": "Host",
            "description": "The access address of PostgreSQL.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "Port",
            "description": "The port of PostgreSQL.",
            "placeholder": "5432",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "The port number ranges from 0 to 65535",
          },
          "subject": {
            "required": true,
            "display": "Database",
            "description": "The name of the PostgreSQL database to connect to.",
            "placeholder": "for example: db1"
          }
        },
        "authentication": {
          "display": "Authentication",
          "description": "Authentication is the process of verifying the identity before granting access to PostgreSQL.",
          "value": "plain",
          "alternatives": [
            {
              "name": "plain",
              "display": "Username and Password",
              "username": {
                "required": true,
                "display": "Username",
                "placeholder": "username"
              },
              "password": {
                "required": true,
                "display": "Password",
                "placeholder": "password"
              }
            }
          ]
        },
        "groups": [
          {
            "name": "Connection options",
            "display_order": 1,
            "short_description": "Other connection options.",
            "description": "Other connection options.",
            "collapsible": false,
            "connection_option": true,
            "params": [
              {
                "name": "application_name",
                "display": "Application Name",
                "hint": {
                  "type": "str"
                },
                "short_description": "Set the application name to identify the connecting application.",
                "description": "Set the application name to identify the connecting application.",
                "placeholder": "for example: TDengine"
              },
              {
                "name": "ssl_mode",
                "display": "SSL Mode",
                "hint": {
                  "type": "str",
                  "choices": [
                    "DISABLE",
                    "ALLOW",
                    "PREFER",
                    "REQUIRE"
                  ]
                },
                "short_description": "Set whether to negotiate a secure SSL TCP/IP connection with the server or the priority for negotiation.",
                "description": "Set whether to negotiate a secure SSL TCP/IP connection with the server or the priority for negotiation.",
                "placeholder": "Please select the SSL mode",
                "value": "PREFER"
              }
            ]
          },
          {
            "name": "Data Collection",
            "display_order": 2,
            "short_description": "Data collection related configuration items.",
            "description": "Data collection related configuration items.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "subtable_fields",
                "display": "Subtable Fields",
                "hint": {
                  "type": "str"
                },
                "short_description": "Fields and query statements used for splitting sub tables.",
                "description": "Fields and query statements used for splitting sub tables.",
                "required": false,
                "placeholder": "select distinct col_name1,col_name2 from table",
              },
              {
                "name": "sql",
                "display": "SQL Template",
                "hint": {
                  "type": "str"
                },
                "short_description": "SQL statement used for querying. The SQL statement must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).",
                "description": "SQL statement used for querying. The SQL statement must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).\nSQL uses different placeholders to represent different time format requirements, specifically the following placeholder formats:\n1. `${start}`, `${end}`: Represents the RFC3339 format timestamp, such as: 2024-03-14T08:00:00+0800\n2. `${start_no_tz}`, `${end_no_tz}`: Represents the RFC3339 string without a time zone: 2024-03-14T08:00:00\n3. `${start_date}`, `${end_date}`: Represents only the date, such as: 2024-03-14\n\nIf you use subtable fields, you need to concatenate field placeholders \`and ${col_name1} and ${col_name2}\` in the statement.note that field placeholders are case sensitive and need to be consistent with the fields in the database. If you want to sort by a specific field (recommended in ascending time order), you need to concatenate \`ORDER BY time\` in the statement.\n\nExample:\`SELECT * FROM table WHERE time >= ${start} AND time < ${end} and ${col_name1} and ${col_name2} ORDER BY time\`",
                "required": true,
                "placeholder": "See the description for a complete example",
                "grid_two": true,
              },
              {
                "name": "start",
                "display": "Start Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "Start time for migrating data.",
                "description": "Start time for migrating data.\n",
                "required": true,
                "placeholder": "for example: 2023-01-01 00:00:00"
              },
              {
                "name": "end",
                "display": "End Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "End time for migrating data, can be left blank. If set, the migration task will stop automatically after the task is executed to the end time; if left blank, the real-time data will be synchronized continuously, and the task will not stop automatically.",
                "description": "End time for migrating data, can be left blank. If set, the migration task will stop automatically after the task is executed to the end time; if left blank, the real-time data will be synchronized continuously, and the task will not stop automatically.\n",
                "required": false,
                "placeholder": "for example: 2024-01-01 00:00:00"
              },
              {
                "name": "interval",
                "display": "Time Interval",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "Day"
                    },
                    {
                      "value": "h",
                      "label": "Hours"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "The time interval for segmented queries. The default is 1 day. To avoid querying too much data, a data synchronization subtask will use the query interval to query data in time segments.",
                "description": "The time interval for segmented queries. The default is 1 day. To avoid querying too much data, a data synchronization subtask will use the query interval to query data in time segments.\n",
                "required": false,
                "placeholder": "The value is an integer ranging [0,600]",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              },
              {
                "name": "delay",
                "display": "Delay",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "In the real-time data synchronization scenario, to avoid the loss of delayed written data, each synchronization task will read data before the delay time.",
                "description": "In the real-time data synchronization scenario, to avoid the loss of delayed written data, each synchronization task will read data before the delay time.\n",
                "required": false,
                "placeholder": "The value is an integer ranging [0,60000]",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave these options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "read_concurrency",
              "display": "Read Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 1000
              },
              "description": "The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 100000
              },
              "description": "The number of data points to be written in a single request. The default value is 10000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "10000"
            }
          ]
        },
        "parser": {
          "display": "Data Mapping",
          "required": true,
          "description": "taosX could let users to specify the data model in the database, for example, the table name pattern and stable name pattern, field names as tags or field names as columns.\n",
          "fields": [
            {
              "name": "DateTime",
              "description": "The timestamp of the returned value.",
              "type": "timestamp"
            }
          ]
        }
      },
      {
        "id": "oracle",
        "type": "uri",
        "name": "Oracle",
        "license_id": "oracle",
        "description": "Oracle is the world's most popular relational database management system. It has good portability, ease of use, and powerful functions, and is suitable for various large, medium, and small microcomputer environments. It is an efficient, reliable, and high-throughput database solution.\n\nTDengine can efficiently read the data in Oracle and write it to TDengine through the Oracle connector to achieve historical data migration or real-time data synchronization.\n",
        "options": {
          "host": {
            "required": true,
            "display": "Host",
            "description": "The access address of Oracle.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "Port",
            "description": "The port of Oracle.",
            "placeholder": "1521",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "The port number ranges from 0 to 65535",
          },
          "subject": {
            "required": true,
            "display": "Database",
            "description": "The name of the database to connect to.",
            "placeholder": "for example: db1"
          }
        },
        "authentication": {
          "display": "Authentication",
          "description": "Authentication is the process of verifying the identity before granting access to Oracle.",
          "value": "plain",
          "alternatives": [
            {
              "name": "plain",
              "display": "Username and Password",
              "username": {
                "required": true,
                "display": "Username",
                "placeholder": "username"
              },
              "password": {
                "required": true,
                "display": "Password",
                "placeholder": "password"
              }
            }
          ]
        },
        "groups": [
          {
            "name": "Data Collection",
            "display_order": 2,
            "short_description": "Data collection related configuration items.",
            "description": "Data collection related configuration items.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "subtable_fields",
                "display": "Subtable Fields",
                "hint": {
                  "type": "str"
                },
                "short_description": "Fields and query statements used for splitting sub tables.",
                "description": "Fields and query statements used for splitting sub tables.",
                "required": false,
                "placeholder": "select distinct col_name1,col_name2 from table",
              },
              {
                "name": "sql",
                "display": "SQL Template",
                "hint": {
                  "type": "str"
                },
                "short_description": "SQL statement used for querying. The SQL statement must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).",
                "description": "SQL statement used for querying. The SQL statement must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).\nSQL uses different placeholders to represent different time format requirements, specifically the following placeholder formats:\n1. `${start}`, `${end}`: Represents the RFC3339 format timestamp, such as: 2024-03-14T08:00:00+0800\n2. `${start_no_tz}`, `${end_no_tz}`: Represents the RFC3339 string without a time zone: 2024-03-14T08:00:00\n3. `${start_date}`, `${end_date}`: Represents only the date, but there's no pure date type in Oracle, so it will contain zero hours, zero minutes, and zero seconds, such as: 2024-03-14 00:00:00, Therefore, when using date<=`${end_date}`, it should be noted that it cannot contain the day of 2024-03-14\n\nIf you use subtable fields, you need to concatenate field placeholders \`and ${col_name1} and ${col_name2}\` in the statement.note that field placeholders are case sensitive and need to be consistent with the fields in the database. If you want to sort by a specific field (recommended in ascending time order), you need to concatenate \`ORDER BY time\` in the statement.\n\nExample:\`SELECT * FROM table WHERE time >= ${start} AND time < ${end} and ${col_name1} and ${col_name2} ORDER BY time\`",
                "required": true,
                "placeholder": "See the description for a complete example",
                "grid_two": true,
              },
              {
                "name": "start",
                "display": "Start Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "Start time for migrating data.",
                "description": "Start time for migrating data.\n",
                "required": true,
                "placeholder": "for example: 2023-01-01 00:00:00"
              },
              {
                "name": "end",
                "display": "End Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "End time for migrating data, can be left blank. If set, the migration task will stop automatically after the task is executed to the end time; if left blank, the real-time data will be synchronized continuously, and the task will not stop automatically.",
                "description": "End time for migrating data, can be left blank. If set, the migration task will stop automatically after the task is executed to the end time; if left blank, the real-time data will be synchronized continuously, and the task will not stop automatically.\n",
                "required": false,
                "placeholder": "for example: 2024-01-01 00:00:00"
              },
              {
                "name": "interval",
                "display": "Query Interval",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "Day"
                    },
                    {
                      "value": "h",
                      "label": "Hours"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "The time interval for segmented queries. The default is 1 day. To avoid querying too much data, a data synchronization subtask will use the query interval to query data in time segments.",
                "description": "The time interval for segmented queries. The default is 1 day. To avoid querying too much data, a data synchronization subtask will use the query interval to query data in time segments.\n",
                "required": false,
                "placeholder": "The value is an integer ranging [0,600]",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              },
              {
                "name": "delay",
                "display": "Delay",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "In the real-time data synchronization scenario, to avoid the loss of delayed written data, each synchronization task will read data before the delay time.",
                "description": "In the real-time data synchronization scenario, to avoid the loss of delayed written data, each synchronization task will read data before the delay time.\n",
                "required": false,
                "placeholder": "The value is an integer ranging [0,60000]",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave these options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "read_concurrency",
              "display": "Read Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 1000
              },
              "description": "The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 100000
              },
              "description": "The number of data points to be written in a single request. The default value is 10000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "10000"
            }
          ]
        },
        "parser": {
          "display": "Data Mapping",
          "required": true,
          "description": "taosX could let users to specify the data model in the database, for example, the table name pattern and stable name pattern, field names as tags or field names as columns.\n",
          "fields": [
            {
              "name": "DateTime",
              "description": "The timestamp of the returned value.",
              "type": "timestamp"
            }
          ]
        }
      },
      {
        "id": "mssql",
        "type": "uri",
        "name": "Microsoft SQL Server",
        "license_id": "mssql",
        "description": "Microsoft SQL Server is a relational database management system developed by Microsoft Corporation. It has the advantages of easy use, good scalability, and high integration with related software.\n\nTDengine can efficiently read data from Microsoft SQL Server and write it to TDengine to achieve historical data migration or real-time data synchronization.\n",
        "options": {
          "host": {
            "required": true,
            "display": "Host",
            "description": "The access address of SQL Server.\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "Port",
            "description": "The port of SQL Server.",
            "placeholder": "1433",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "The port number ranges from 0 to 65535",
          },
          "subject": {
            "required": true,
            "display": "Database",
            "description": "The name of the SQL Server database to connect to.",
            "placeholder": "for example: db1"
          }
        },
        "authentication": {
          "display": "Authentication",
          "description": "Authentication is the process of verifying the identity before granting access to SQL Server.",
          "value": "plain",
          "alternatives": [
            {
              "name": "plain",
              "display": "Username and Password",
              "username": {
                "required": true,
                "display": "Username",
                "placeholder": "username"
              },
              "password": {
                "required": true,
                "display": "Password",
                "placeholder": "password"
              }
            }
          ]
        },
        "groups": [
          {
            "name": "Connection options",
            "display_order": 1,
            "short_description": "Other connection options.",
            "description": "Other connection options.",
            "collapsible": false,
            "connection_option": true,
            "params": [
              {
                "name": "instance_name",
                "display": "Instance Name",
                "hint": {
                  "type": "str"
                },
                "short_description": "The name of the SQL Server instance.",
                "description": "The name of the SQL Server instance.",
                "placeholder": "for example: MSSQLSERVER"
              },
              {
                "name": "application_name",
                "display": "Application Name",
                "hint": {
                  "type": "str"
                },
                "short_description": "The name of the application.",
                "description": "The name of the application.",
                "placeholder": "for example: TDengine"
              },
              {
                "name": "encryption",
                "display": "Encryption",
                "hint": {
                  "type": "str",
                  "choices": [
                    "Off",
                    "On",
                    "NotSupported",
                    "Required"
                  ]
                },
                "short_description": "Set whether to encrypt the connection.",
                "description": "Set whether to encrypt the connection.",
                "placeholder": "Please select the type of encryption",
                "value": "Off"
              },
              {
                "name": "trust_cert",
                "display": "Trust Certificate",
                "hint": {
                  "type": "bool"
                },
                "short_description": "Set whether to trust the server certificate.",
                "description": "Set whether to trust the server certificate.",
                "placeholder": "Please select whether to trust the server certificate",
                "value": "true"
              },
              {
                "name": "trust_cert_ca",
                "display": "Trust Certificate CA",
                "hint": {
                  "type": "file"
                },
                "short_description": "The certificate of the CA if you trust the server certificate.",
                "description": "The certificate of the CA if you trust the server certificate.",
                "placeholder": "Please upload the certificate of the CA"
              }
            ]
          },
          {
            "name": "Data Collection",
            "display_order": 2,
            "short_description": "Data collection related configuration items.",
            "description": "Data collection related configuration items.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "subtable_fields",
                "display": "Subtable Fields",
                "hint": {
                  "type": "str"
                },
                "short_description": "Fields and query statements used for splitting sub tables.",
                "description": "Fields and query statements used for splitting sub tables.",
                "required": false,
                "placeholder": "select distinct col_name1,col_name2 from table",
              },
              {
                "name": "sql",
                "display": "SQL Template",
                "hint": {
                  "type": "str"
                },
                "short_description": "SQL statement used for querying. The SQL statement must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).",
                "description": "SQL statement used for querying. The SQL statement must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).\nSQL uses different placeholders to represent different time format requirements, specifically the following placeholder formats:\n1. `${start}`, `${end}`: Represents the RFC3339 format timestamp, such as: 2024-03-14T08:00:00+0800\n2. `${start_no_tz}`, `${end_no_tz}`: Represents the RFC3339 string without a time zone: 2024-03-14T08:00:00\n3. `${start_date}`, `${end_date}`: Represents only the date, such as: 2024-03-14\n\nIf you use subtable fields, you need to concatenate field placeholders \`and ${col_name1} and ${col_name2}\` in the statement.note that field placeholders are case sensitive and need to be consistent with the fields in the database. If you want to sort by a specific field (recommended in ascending time order), you need to concatenate \`ORDER BY time\` in the statement.\n\nExample:\`SELECT * FROM table WHERE time >= ${start} AND time < ${end} and ${col_name1} and ${col_name2} ORDER BY time\`",
                "required": true,
                "placeholder": "See the description for a complete example",
                "grid_two": true,
              },
              {
                "name": "start",
                "display": "Start Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "Start time for migrating data.",
                "description": "Start time for migrating data.\n",
                "required": true,
                "placeholder": "for example: 2023-01-01 00:00:00"
              },
              {
                "name": "end",
                "display": "End Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "End time for migrating data, can be left blank. If set, the migration task will stop automatically after the task is executed to the end time; if left blank, the real-time data will be synchronized continuously, and the task will not stop automatically.",
                "description": "End time for migrating data, can be left blank. If set, the migration task will stop automatically after the task is executed to the end time; if left blank, the real-time data will be synchronized continuously, and the task will not stop automatically.\n",
                "required": false,
                "placeholder": "for example: 2024-01-01 00:00:00"
              },
              {
                "name": "interval",
                "display": "Query Interval",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "Day"
                    },
                    {
                      "value": "h",
                      "label": "Hours"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "The time interval for segmented queries. The default is 1 day. To avoid querying too much data, a data synchronization subtask will use the query interval to query data in time segments.",
                "description": "The time interval for segmented queries. The default is 1 day. To avoid querying too much data, a data synchronization subtask will use the query interval to query data in time segments.\n",
                "required": false,
                "placeholder": "The value is an integer ranging [0,600]",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              },
              {
                "name": "delay",
                "display": "Delay",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "In the real-time data synchronization scenario, to avoid the loss of delayed written data, each synchronization task will read data before the delay time.",
                "description": "In the real-time data synchronization scenario, to avoid the loss of delayed written data, each synchronization task will read data before the delay time.\n",
                "required": false,
                "placeholder": "The value is an integer ranging [0,60000]",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave these options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "read_concurrency",
              "display": "Read Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 1000
              },
              "description": "The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 100000
              },
              "description": "The number of data points to be written in a single request. The default value is 10000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "10000"
            }
          ]
        },
        "parser": {
          "display": "Data Mapping",
          "required": true,
          "description": "taosX could let users to specify the data model in the database, for example, the table name pattern and stable name pattern, field names as tags or field names as columns.\n",
          "fields": [
            {
              "name": "DateTime",
              "description": "The timestamp of the returned value.",
              "type": "timestamp"
            }
          ]
        }
      },
      {
        "id": "mongodb",
        "type": "uri",
        "name": "MongoDB",
        "license_id": "mongodb",
        "description": "MongoDB is a product between relational and non-relational databases, which is widely used in many fields such as content management systems, mobile applications, and the Internet of Things. \n\nTDengine efficiently reads data from MongoDB and writes it to TDengine for historical data migration or real-time data synchronization. \n",
        "options": {
          "host": {
            "required": true,
            "display": "Host",
            "description": "The access address of MongoDB. If using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "Port",
            "description": "The port of MongoDB",
            "placeholder": "27017",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "The port number ranges from 0 to 65535",
          },
          // "load_balanced": {
          //   "required": false,
          //   "display": "Load Balanced",
          //   "description": "Whether to use load balancing to connect.\n- *true*:The host address is used as the load balancing address \n- *false*:The host address is used as the database address\n",
          //   "hint": {
          //     "type": "bool",
          //   }
          // },
          // "direct_connection": {
          //   "required": false,
          //   "display": "Direct Connection",
          //   "description": "Whether to connect directly to a single host or automatically discover all servers in the cluster.\n- *true*:host connects directly to host:port \n- *false*:host Discovers other servers in the cluster\n",
          //   "hint": {
          //     "type": "bool",
          //   },
          //   "value": "true"
          // },
          // "repl_set_name": {
          //   "required": false,
          //   "display": "Replica Name",
          //   "description": "The client connects to the cluster replica with the specified name. If a replica name is specified, only this replica server is connected.",
          //   "placeholder": "",
          // },
          // "local_threshold": {
          //   "required": false,
          //   "display": "Local Threshold",
          //   "description": "Used to determine how much the average round trip time between the client and the server is allowed to increase compared to the shortest round trip time among all servers. If the value is 0, it indicates that there is no delay window, so only the server with the lowest average round-trip time will be connected. The default is 15 ms.",
          //   "hint": {
          //     "type": "duration",
          //     "choices": [
          //       {
          //         "value": "m",
          //         "label": "Minute"
          //       },
          //       {
          //         "value": "s",
          //         "label": "Second"
          //       },
          //     ]
          //   },
          //   "placeholder": "15",
          //   "value": "15",
          //   "type_value": "s",
          //   "pattern": null,
          //   "patternMsg": "The value can only be a positive integer or 0",
          // }
        },
        "authentication": {
          "display": "Authentication",
          "description": "Authentication is the process of verifying the identity before granting access to MongoDB.",
          "value": "plain",
          "alternatives": [
            {
              "name": "plain",
              "display": "Username and Password",
              "params": [
                {
                  "name": "username",
                  "required": true,
                  "display": "Username",
                  "placeholder": "Username"
                },
                {
                  "name": "password",
                  "required": true,
                  "display": "Password",
                  "placeholder": "Password"
                },
                // {
                //   "name": "mechanism",
                //   "required": false,
                //   "display": "Authenticate Mechanism",
                //   "placeholder": "Select an authentication mechanism",
                //   "short_description": "The authentication mechanism to be used, if not provided, will be negotiated with the server.\n",
                //   "description": "The authentication mechanism to be used, if not provided, will be negotiated with the server.\n",
                //   "hint": {
                //     "type": "str",
                //     "choices": [
                //       "MongoDbCr",
                //       "ScramSha1",
                //       "ScramSha256",
                //       "MongoDbX509",
                //       "Gssapi",
                //       "Plain",
                //       "MongoDbAws",
                //       "MongoDbOidc",
                //     ]
                //   },
                // },
                {
                  "name": "source",
                  "required": false,
                  "display": "Authenticate DB",
                  "placeholder": "Authenticate DB",
                  "short_description": "The default database for storing user information in MongoDB is admin.\n",
                  "description": "The default database for storing user information in MongoDB is admin.\n",
                },
              ],
            }
          ]
        },
        "groups": [
          {
            "name": "Connection options",
            "display_order": 1,
            "short_description": "Other connection options.",
            "description": "Other connection options.",
            "collapsible": false,
            "connection_option": true,
            "params": [
              {
                "name": "app_name",
                "display": "Application Name",
                "hint": {
                  "type": "str",
                },
                "short_description": "Identifies a client.",
                "description": "Identifies a client.",
                "placeholder": "For example: TDengine",
              },
            ]
          },
          {
            "name": "Enable SSL",
            "short_description": "Use self-signed certificate file and private key.",
            "description": "Use self-signed certificate file and private key.",
            "collapsible": true,
            "connection_option": true,
            "collapsed": false,
            "params": [
              {
                "name": "ca_file_path",
                "display": "CA File",
                "hint": {
                  "type": "file"
                },
                "short_description": "CA certificate file",
                "description": "CA certificate file",
                "required": true
              },
              {
                "name": "cert_key_file_path",
                "display": "Cert File",
                "hint": {
                  "type": "file"
                },
                "short_description": ".cert file",
                "description": ".cert file",
                "required": true
              },
            ]
          },
          {
            "name": "Data Collection",
            "display_order": 2,
            "short_description": "Data collection related configuration items.",
            "description": "Data collection related configuration items.",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "database",
                "display": "Database",
                "hint": {
                  "type": "str"
                },
                "short_description": "The source database.",
                "description": "Source database in MongoDB, can be dynamically configured using placeholders, available placeholder list: \n<ul><li>${Y} Full Gregorian year representation, zero-filled 4-digit integer </li><li>${y} Gregorian year divided by 100, Zero padding of two integers </li><li>${M} integer (1-12) month </li><li>${m} in integer (01-12) </li><li>${B} in English whole put together </li><li>${b} in English abbreviations (3 A letter) </li><li>${D} date Numbers (1-31) </li><li>${d} date Numbers (01-31) </li><li>${J} the first day of the year (1-366) </li><li>${j} the first day of the year (001 - 366) </li><li>${F} is equivalent to ${Y}-${m}-${d}</li></ul>\n",
                "required": true,
                "placeholder": "database_${Y}",
              },
              {
                "name": "collection",
                "display": "Collection",
                "hint": {
                  "type": "str"
                },
                "short_description": "The source collection.",
                "description": "Set in MongoDB, can be dynamically configured using placeholders, available placeholder list: \n<ul><li>${Y} Full Gregorian year representation, zero-filled 4-digit integer </li><li>${y} Gregorian year divided by 100, Zero padding of two integers </li><li>${M} integer (1-12) month </li><li>${m} in integer (01-12) </li><li>${B} in English whole put together </li><li>${b} in English abbreviations (3 A letter) </li><li>${D} date Numbers (1-31) </li><li>${d} date Numbers (01-31) </li><li>${J} the first day of the year (1-366) </li><li>${j} the first day of the year (001 - 366) </li><li>${F} is equivalent to ${Y}-${m}-${d}</li></ul>",
                "required": true,
                "placeholder": "collection_${md}",
              },
              {
                "name": "subtable_fields",
                "display": "Subtable Fields",
                "hint": {
                  "type": "str"
                },
                "short_description": "Fields and query statements used for splitting sub tables.",
                "description": "Fields and query statements used for splitting sub tables.",
                "required": false,
                "placeholder": "col_name1,col_name2,...",
              },
              {
                "name": "sql",
                "display": "Query Template",
                "hint": {
                  "type": "str"
                },
                "short_description": "A query statement used to query data, in JSON format, must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).",
                "description": "A query statement used to query data, in JSON format, must contain a time range condition, and the start time and end time must appear in pairs(at least one closed interval).\nUse different placeholders to indicate different time format requirements, specifically the following placeholder formats:\n1. `${start_datetime}`、`${end_datetime}`:Filters corresponding to back-end datetime fields, for example:{\"ddate\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}} will be converted to {\"ddate\":{\"$gte\":{\"$date\":\"2024-06-01T00:00:00+00:00\"},\"$lt\":{\"$date\":\"2024-07-01T00:00:00+00:00\"}}}\n2. `${start_timestamp}`、`${end_timestamp}`: indicates the filtering of back-end timestamp fields, for example:{\"ttime\":{\"$gte\":${start_timestamp},\"$lt\":${end_timestamp}}} will be converted to {\"ttime\":{\"$gte\":{\"$timestamp\":{\"t\":123,\"i\":456}},\"$lt\":{\"$timestamp\":{\"t\":123,\"i\":456}}}}\n\nIf you use subtable fields, you need to concatenate field placeholders in the statement.\n\nExample:\`{\"ddate\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}},${col_name1},${col_name2}}\`",
                "required": true,
                "placeholder": "{\"ddate\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}},${col_name1},${col_name2}}",
                "grid_two": true,
              },
              {
                "name": "sort",
                "display": "Sort",
                "hint": {
                  "type": "str"
                },
                "short_description": "Sorting of query statements.",
                "description": "Sorting of query statements.\n\n\n1.`{\"createtime\":1}`:MongoDB query results are returned in `createtime` order.\n\n2.`{\"createdate\":1, \"createtime\":1}`:MongoDB query results are returned in `createdate` and `createtime` order.",
                "required": false,
                "placeholder": "{\"createtime\":1}",
                "validator": "checkJson"
              },
              {
                "name": "start",
                "display": "Start Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "Start time of data migration.",
                "description": "Start time of data migration.\n",
                "required": true,
                "placeholder": "如：2023-01-01 00:00:00"
              },
              {
                "name": "end",
                "display": "End Time",
                "hint": {
                  "type": "time"
                },
                "short_description": "The end time of data migration can be left blank. If this parameter is set, the migration task is automatically stopped when the end time expires. If left blank, real-time data is continuously synchronized and the task does not automatically stop.",
                "description": "The end time of data migration can be left blank. If this parameter is set, the migration task is automatically stopped when the end time expires. If left blank, real-time data is continuously synchronized and the task does not automatically stop.\n",
                "required": false,
                "placeholder": "如：2024-01-01 00:00:00"
              },
              {
                "name": "interval",
                "display": "Interval",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "Days"
                    },
                    {
                      "value": "h",
                      "label": "Hours"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "Interval for querying data in segments. The default value is 1 day. To avoid a large amount of query data, a data synchronization task queries data in time intervals.",
                "description": "Interval for querying data in segments. The default value is 1 day. To avoid a large amount of query data, a data synchronization task queries data in time intervals.\n",
                "required": false,
                "placeholder": "The value is an integer ranging [0,600]",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              },
              {
                "name": "delay",
                "display": "Delay",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "Minute"
                    },
                    {
                      "value": "s",
                      "label": "Second"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "In the real-time data synchronization scenario, each synchronization task reads data before the delay to prevent data loss.",
                "description": "In the real-time data synchronization scenario, each synchronization task reads data before the delay to prevent data loss.\n",
                "required": false,
                "placeholder": "The value is an integer ranging [0,60000]",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "The value can only be a positive integer or 0",
              }
            ]
          }
        ],
        "advanced": {
          "name": "Advanced Options",
          "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave these options as default to use the recommended settings.\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "read_concurrency",
              "display": "Read Concurrency",
              "hint": {
                "type": "integer",
                "min": 0,
                "max": 1000
              },
              "description": "The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.\n",
              "value": "0"
            },
            {
              "name": "batch_size",
              "display": "Batch Size",
              "hint": {
                "type": "integer",
                "min": 1,
                "max": 100000
              },
              "description": "The number of data points to be written in a single request. The default value is 10000. If the data source is slow to respond, you can reduce this value appropriately.\n",
              "value": "10000"
            }
          ]
        },
        "parser": {
          "display": "Payload Transformation",
          "required": true,
          "description": "taosX could let users to specify the data model in the database, for example, the table name pattern and stable name pattern, field names as tags or field names as columns.\n",
          "fields": [
            {
              "name": "value",
              "description": "Sample Message Body",
              "type": "varchar"
            }
          ]
        }
      },
    ]
  } else {
    return [
      {
        "id": "tmq",
        "type": "uri",
        "name": "TDengine 3.x",
        "license_id": "td3.0",
        "description": "使用 TMQ 进行 TDengine 指定从数据库或超级表的订阅。\n\n支持使用原生连接或 WebSocket 连接（使用 HTTP 或 HTTPS 协议）。默认使用原生连接。\n\n使用 `database` 方式指定数据库名，或 `database.table` 方式指定订阅一个超级表或普通表。\n",
        "options": {
          "endpoint": {
            "required": true,
            "display": "Topic DSN",
            "description": "请登录 TDengine 云服务或打开企业版的 Explorer, 点击`数据订阅`，你将看到主题列表，复制主题对应的 DSN 到这里即可。\n",
            "placeholder": "Topic 示例: tmq+ws://root:taosdata@localhost:6041/topic"
          }
        },
        "groups": [{
          "name": "订阅设置",
          "display_order": 2,
          "short_description": "TDengine TMQ 订阅设置。",
          "description": "TDengine TMQ 订阅设置。",
          "collapsible": false,
          "connection_option": false,
          "params": [{
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
            "value": "earliest",
            "edit_disabled": true,
          }, {
            "name": "group.id",
            "display": "订阅组 ID",
            "hint": {
              "type": "str"
            },
            "short_description": "订阅组 ID 是用于标识一个订阅组的字符串，最大长度为 192。同一个订阅组内的订阅者共享消费进度。不指定情况下将使用随机生成的 group ID。",
            "description": "订阅组 ID 是用于标识一个订阅组的字符串，最大长度为 192。同一个订阅组内的订阅者共享消费进度。不指定情况下将使用随机生成的 group ID。\n",
            "edit_disabled": true,
          }, {
            "name": "client.id",
            "display": "客户端 ID",
            "hint": {
              "type": "str"
            },
            "short_description": "客户端 ID 是一个用于标识客户端的字符串，最大长度为 192。",
            "description": "客户端 ID 是一个用于标识客户端的字符串，最大长度为 192。\n",
            "required": true,
            "edit_disabled": true,
          }, {
            "name": "timeout",
            "display": "超时",
            "hint": {
              "type": "timeout",
              "choices": [
                {
                  "value": "m",
                  "label": "分钟"
                },
                {
                  "value": "s",
                  "label": "秒"
                },
                {
                  "value": "ms",
                  "label": "毫秒"
                },
              ],
              "min": 0,
              "max": 60000
            },
            "short_description": "超时时间范围内没有新增数据，同步任务将自动结束。",
            "description": "超时时间范围内没有新增数据，同步任务将自动结束。\n可配置为：\n- `0`: 表示无超时时间，持续进行订阅。\n- 指定超时时间：`5s`, `1m` 等。\n",
            "placeholder": "输入范围为[0,60000]整数",
            "type_value": "s",
            "pattern": null,
            "patternMsg": "只能输入正整数或者0",
          }, {
            "name": "experimental.snapshot.enable",
            "display": "同步已落盘数据",
            "hint": {
              "type": "bool"
            },
            "short_description": "如启用，可以同步已经落盘到 TSDB 时序数据存储文件中（即不在 WAL 中）的数据。如关闭，则只同步尚未落盘（即保存在 WAL 中）的数据。",
            "description": "如启用，可以同步已经落盘到 TSDB 时序数据存储文件中（即不在 WAL 中）的数据。如关闭，则只同步尚未落盘（即保存在 WAL 中）的数据。\n",
            "value": "true"
          }, {
            "name": "with.meta.drop",
            "display": "同步删表操作",
            "hint": {
              "type": "bool"
            },
            "short_description": "如启用则会同步删表操作到目标数据库。",
            "description": "如启用则会同步删表操作到目标数据库。\n",
            "value": "true"
          }, {
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
        ],
        "advanced": {
          "name": "高级选项",
          "description": "调整与读并发、写并发和错误日志相关的参数。\n",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "compression",
              "display": "压缩",
              "hint": {
                "type": "bool"
              },
              "short_description": "启用 WebSocket 压缩支持，以降低网络带宽占用。",
              "description": "启用 WebSocket 压缩支持，以降低网络带宽占用。\n",
              "value": "false"
            }
          ]
        }
      },
      {
        "id": "taos",
        "type": "uri",
        "name": "TDengine 2.x",
        "license_id": "td2.6",
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
            "placeholder": "6041",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "端口号的范围是 0-65535",
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
                "display": "密码",
                "description": "TDengine 密码，默认为 `taosdata`。",
                "placeholder": "taosdata",
                "value": "taosdata"
              }
            }
          ]
        },
        "groups": [
          {
            "display": "迁移模式",
            "name": "migrate_options",
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
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "元数据轮询间隔，用于同步过程中的元数据变更检测。",
                "description": "元数据轮询间隔，用于同步过程中的元数据变更检测。",
                "placeholder": "输入范围为[0,60000]整数",
                "value": "5",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              }
            ]
          },
          {
            "display": "表",
            "name": "what_to_migrate",
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
            "display": "时间范围",
            "name": "range",
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
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "天"
                    },
                    {
                      "value": "h",
                      "label": "小时"
                    },
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "查询数据的基本单元，长时间范围的查询会以此为依据切割为多次查询。",
                "description": "查询数据的基本单元，长时间范围的查询会以此为依据切割为多次查询。<br>\n支持使用数字加单位缩写，如\"1ms\"表示1毫秒，\"1s\"表示1秒，\"1m\"表示1分钟，\"1h\"表示1小时，\"1d\"表示1天，\"1w\"表示1周。<br>\n单独使用数字则默认认为是秒。<br>",
                "placeholder": "输入范围为[0,60000]整数",
                "value": "1",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              }
            ]
          },
          {
            "display": "实时同步",
            "name": "realtime_settings",
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
                  "type": "duration",
                  "choices": [
                    {
                      "value": "h",
                      "label": "小时"
                    },
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                    {
                      "value": "ms",
                      "label": "毫秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "在实时同步前回溯一段时间内的数据写入目标库。",
                "description": "在实时同步前回溯一段时间内的数据写入目标库。<br>\n支持使用数字加单位缩写，如\"1ms\"表示1毫秒，\"1s\"表示1秒，\"1m\"表示1分钟，\"1h\"表示1小时，\"1d\"表示1天，\"1w\"表示1周。<br>\n单独使用数字则默认认为是秒。<br>",
                "placeholder": "输入范围为[0,60000]整数",
                "value": "0",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              },
              {
                "name": "interval",
                "display": "间隔",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "轮询查询的时间间隔。",
                "description": "轮询查询的时间间隔。<br>\n支持使用数字加单位缩写，如\"1ms\"表示1毫秒，\"1s\"表示1秒，\"1m\"表示1分钟，\"1h\"表示1小时，\"1d\"表示1天，\"1w\"表示1周。<br>\n单独使用数字则默认认为是秒。<br>",
                "placeholder": "输入范围为[0,60000]整数",
                "value": "1",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              },
              {
                "name": "excursion",
                "display": "乱序",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "等待一段时间的乱序数据入库后再进行查询。",
                "description": "等待一段时间的乱序数据入库后再进行查询。<br>\n支持使用数字加单位缩写，如\"1ms\"表示1毫秒，\"1s\"表示1秒，\"1m\"表示1分钟，\"1h\"表示1小时，\"1d\"表示1天，\"1w\"表示1周。<br>\n单独使用数字则默认认为是秒。<br>",
                "placeholder": "输入范围为[0,60000]整数",
                "value": "500",
                "type_value": "ms",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
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
            },
            {
              "name": "compression",
              "display": "压缩",
              "hint": {
                "type": "bool"
              },
              "short_description": "启用 WebSocket 压缩支持，以降低网络带宽占用。",
              "description": "启用 WebSocket 压缩支持，以降低网络带宽占用。\n",
              "value": "false"
            }
          ]
        }
      },
      {
        "id": "pi",
        "type": "uri",
        "name": "PI",
        "license_id": "pi",
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
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "天"
                    },
                    {
                      "value": "h",
                      "label": "小时"
                    },
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "连接丢失或首次启动时自动回填的最长时间：`2d`、`3h`、`4m` 等。",
                "description": "连接丢失或首次启动时自动回填的最长时间：`2d`、`3h`、`4m` 等。",
                "placeholder": "输入范围为[0,600]整数",
                "value": "0",
                "type_value": "m",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              }
            ]
          }
        ],
        "advanced": {
          "name": "高级选项",
          "description": "对数据源性能、日志等其他参数进行调整，可修改以下选项",
          "collapsible": true,
          "connection_option": false,
          "params": [
            {
              "name": "sync_add_element",
              "display": "同步新增的元素",
              "description": "监听配置的模板下新增的元素，无需重启任务，即可自动同步新增元素",
              "hint": {
                "type": "bool",
              },
              "value": "true",
            },
            {
              "name": "sync_update_attribute",
              "display": "同步静态属性的变化",
              "description": "同步所有静态属性（非 PI Point 属性）的变化",
              "hint": {
                "type": "bool",
              },
              "value": "true",
            },
            {
              "name": "sync_delete_element",
              "display": "同步删除元素的操作",
              "description": "监听配置的模板下删除元素的事件，并同步删除 TDengine 对应子表",
              "hint": {
                "type": "bool",
              },
              "value": "true",
            },
            {
              "name": "sync_delete_data",
              "display": "同步删除 PI Point 历史数据",
              "description": "对于某个元素的动态属性，如果在 PI 中某个时间的数据被删除了，TDengine 对应时间对应列的数据会被置空",
              "hint": {
                "type": "bool",
              },
              "value": "true",
            },
            {
              "name": "sync_update_data",
              "display": "同步修改 PI Point 历史数据",
              "description": "对于某个元素的动态属性，如果在 PI 中历史数据被修改了，TDengine 对应时间的数据也会更新",
              "hint": {
                "type": "bool",
              },
              "value": "true",
            },
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
          "name": "数据模型配置",
          "display": "监测点集",
          "description": "使用默认配置，或者下载并修改后上传。配置入库的点位或者元素，入库的数据模型、数据过滤条件和变换规则。",
          "value": "single-column",
          "categories": [
            {
              "category": "single-column",
              "display": "单列模式",
              "short_description": "单列模式基于点位所属 UOM 建立超级表，每一个点位建立一个子表。",
              "target": {
                "name": "single-column",
              },
              "params": [{
                "name": "filter_value",
                "display": "数据集过滤",
                "placeholder": "通配符*匹配0或者多个字符，通配符?精确匹配一个字符",
                "hint": {
                  "type": "compose",
                  "choices": [
                    "point",
                    "element",
                    "template"
                  ]
                },
                "action": "download",
                "action_text": "下载默认配置",
                "description": "可指定过滤条件，下载默认模板<br> - point: 使用点位名称过滤<br> - element: 使用AF element 名称过滤<br> - template: 使用AF template 名称过滤<br> 过滤条件可以使用通配符*匹配0或者多个字符，使用通配符?精确匹配一个字符",
              }, {
                "name": "transform_config_file",
                "display": "点位配置文件",
                "btnText": "上传配置文件",
                "required": true,
                "hint": {
                  "type": "file"
                },
                "description": "上传单列模式点位列表文件，文件格式为 CSV。",
              }]
            },
            {
              "category": "multi-column",
              "display": "多列模式",
              "short_description": "多列模式基于 AF Template 建立超级表，每一个 AF element建立一个子表。",
              "target": {
                "name": "multi-column",
                "selectable": false
              },
              "params": [{
                "name": "filter_value",
                "display": "数据集过滤",
                "placeholder": "通配符*匹配0或者多个字符，通配符?精确匹配一个字符",
                "hint": {
                  "type": "compose",
                  "choices": [
                    "point",
                    "element",
                    "template"
                  ]
                },
                "action": "download",
                "action_text": "下载默认配置",
                "description": "可指定过滤条件，下载默认模板<br> - point: 使用点位名称过滤<br> - element: 使用AF element 名称过滤<br> - template: 使用AF template 名称过滤<br> 过滤条件可以使用通配符*匹配0或者多个字符，使用通配符?精确匹配一个字符",
              }, {
                "name": "transform_config_file",
                "display": "模型配置文件",
                "required": true,
                "btnText": "上传配置文件",
                "hint": {
                  "type": "file"
                },
                "description": "上传单列模式点位列表文件，文件格式为 CSV。",
              }]
            }
          ]
        }
      },
      {
        "id": "pibackfill",
        "type": "uri",
        "name": "PI Backfill",
        "license_id": "pi",
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
            "params": [{
              "name": "BackfillStartTime",
              "display": "Backfill 开始时间",
              "hint": {
                "type": "time",
              },
              "required": true,
              "short_description": "从该时间开始导入历史数据。",
              "description": "从该时间开始导入历史数据。\n",
              "placeholder": "YYYY-MM-DD HH:mm:ss"
            }, {
              "name": "BackfillEndTime",
              "display": "Backfill 结束时间",
              "hint": {
                "type": "time"
              },
              "required": true,
              "short_description": "导入历史数据以该时间结束，不能大于当前时间。",
              "description": "导入历史数据以该时间结束，不能大于当前时间。\n",
              "placeholder": "YYYY-MM-DD HH:mm:ss"
            }]
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
          "name": "数据模型配置",
          "display": "监测点集",
          "description": "使用默认配置，或者下载并修改后上传。配置入库的点位或者元素，入库的数据模型、数据过滤条件和变换规则。",
          "value": "single-column",
          "categories": [
            {
              "category": "single-column",
              "display": "单列模式",
              "short_description": "单列模式基于点位所属 UOM 建立超级表，每一个点位建立一个子表。",
              "target": {
                "name": "single-column",
              },
              "params": [{
                "name": "filter_value",
                "display": "数据集过滤",
                "placeholder": "通配符*匹配0或者多个字符，通配符?精确匹配一个字符",
                "hint": {
                  "type": "compose",
                  "choices": [
                    "point",
                    "element",
                    "template"
                  ]
                },
                "action": "download",
                "action_text": "下载默认配置",
                "description": "可指定过滤条件，下载默认模板<br> - point: 使用点位名称过滤<br> - element: 使用AF element 名称过滤<br> - template: 使用AF template 名称过滤<br> 过滤条件可以使用通配符*匹配0或者多个字符，使用通配符?精确匹配一个字符",
              }, {
                "name": "transform_config_file",
                "display": "点位配置文件",
                "btnText": "上传配置文件",
                "required": true,
                "hint": {
                  "type": "file"
                },
                "description": "上传单列模式点位列表文件，文件格式为 CSV。",
              }]
            },
            {
              "category": "multi-column",
              "display": "多列模式",
              "short_description": "多列模式基于 AF Template 建立超级表，每一个 AF element建立一个子表。",
              "target": {
                "name": "multi-column",
                "selectable": false
              },
              "params": [{
                "name": "filter_value",
                "display": "数据集过滤",
                "placeholder": "通配符*匹配0或者多个字符，通配符?精确匹配一个字符",
                "hint": {
                  "type": "compose",
                  "choices": [
                    "point",
                    "element",
                    "template"
                  ]
                },
                "action": "download",
                "action_text": "下载默认配置",
                "description": "可指定过滤条件，下载默认模板<br> - point: 使用点位名称过滤<br> - element: 使用AF element 名称过滤<br> - template: 使用AF template 名称过滤<br> 过滤条件可以使用通配符*匹配0或者多个字符，使用通配符?精确匹配一个字符",
              }, {
                "name": "transform_config_file",
                "display": "模型配置文件",
                "required": true,
                "btnText": "上传配置文件",
                "hint": {
                  "type": "file"
                },
                "description": "上传多列模式模型配置文件，文件格式为 CSV。",
              }]
            }
          ]
        }
      },
      {
        "id": "opcua",
        "type": "uri",
        "name": "OPC-UA",
        "license_id": "opc_ua",
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
                  "required": true,
                  "display": "认证证书文件",
                  "hint": {
                    "type": "file"
                  }
                },
                {
                  "name": "auth_private_key",
                  "required": true,
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
              },
              {
                "name": "update_mode",
                "display": "点位更新模式",
                "hint": {
                  "type": "str",
                  "choices": [
                    "none",
                    "append",
                    "update"
                  ]
                },
                "short_description": "点位更新模式，在使用“选择数据点位”时，可以开启动态点位更新。none：不开启动态点位更新；append：开启动态点位更新，但只追加；update：开启动态点位更新，追加或删除。",
                "description": "点位更新模式，在使用“选择数据点位”时，可以开启动态点位更新。none：不开启动态点位更新；append：开启动态点位更新，但只追加；update：开启动态点位更新，追加或删除。\n",
                "value": "none"
              },
              {
                "name": "update_interval",
                "display": "点位更新间隔",
                "hint": {
                  "type": "integer",
                  "min": 60,
                  "max": 2147483647
                },
                "short_description": "动态点位更新间隔，在“点位更新模式”为 append 和 update 时生效，以秒为单位。",
                "description": "动态点位更新间隔，在“点位更新模式”为 append 和 update 时生效，以秒为单位。\n",
                "value": "600"
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
              "description": "OPC 数据写入使用 csv 文件定义每一个数据点位到 TDengine 数据子表的映射规则：\n\n(1) point_id：必填，数据点位在 OPC UA 服务器上的 id；\n\n(2) stable：必填，数据点位对应的 TDengine 超级表；\n\n(3) tbname：必填，数据点位对应的 TDengine 子表；\n\n(4) enable：可选，默认值 '1'，指定是否采集该点位数据。0-不采集并且删除对应子表，1-采集点位数据，没有子表时创建子表；\n\n(5) value_col：可选，默认值 'val'。数据点位采集值在 TDengine 中对应的列名；\n\n(6) value_transform：可选，数据点位采集值在 taosX 中执行的变换函数，目前仅支持数值计算表达式，详见 transform 文档的 expr 表达式说明；\n\n(7) type：可选，默认值取源数据类型。数据点位采集值的数据类型，可用于替换超级表名称中的占位符 {type}；\n\n(8) quality_col：可选，数据点位采集值质量在 TDengine 中对应的列名；\n\n(9) ts_col/received_ts_col：必填，TDengine 时间戳主键定义：只存在 ts_col 时使用原始时间戳作为主键，只存在 received_ts_col 时使用采集时间戳作为主键，两列都存在时，居前的时间戳列作为主键；\n\n(10) ts_transform：可选，原始时间戳变换函数，参考 transform 数值计算表达式 expr 的说明；\n\n(11) received_ts_transform：可选，采集数据时间戳变换函数，参考 transform 数值计算表达式 expr 的说明；\n\n(12) tag::VARCHAR(200)::name：可选/可配置多个tag列；数据点位在 TDengine 中对应的 Tag 列；其中 tag 为保留关键字，表示该列为一个 tag 列；VARCHAR(200) 表示该 tag 的类型，也可以是其它合法的类型；name 是该 tag 的列名。\n\n更多填写规则请参考<a target=\"_blank\" href=\"/docs/advanced/data-in/opcua\">企业版文档</a>。\n",
              "target": {
                "name": "csv_config_file",
                "description": "上传 CSV 配置文件，定义数据点位到 TDengine 数据子表的映射规则。\n",
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
                  "name": "super_table_expression",
                  "display": "超级表名称",
                  "hint": {
                    "type": "str"
                  },
                  "description": "支持 <super table prefix>_{type} 格式，{type} 表示点位的数据类型。\n",
                  "required": true,
                  "value": "opc_{type}"
                },
                {
                  "name": "child_table_expression",
                  "display": "表名称",
                  "hint": {
                    "type": "str"
                  },
                  "description": "支持 <child table prefix>_{ns}_{id} 格式，{ns} 表示点位的namespace，{id} 为点位的 id。\n",
                  "required": true,
                  "value": "t_{ns}_{id}"
                },
                {
                  "name": "table_primary_key",
                  "display": "主键列",
                  "hint": {
                    "type": "str",
                    "choices": [
                      "original_ts",
                      "received_ts"
                    ]
                  },
                  "description": "目标数据表主键将使用选择的值作为时间戳主键列，original_ts 表示使用数据点位上报 OPC 服务时间，received_ts 表示 taosX 任务接收数据的时间。\n",
                  "required": false,
                  "value": "original_ts"
                },
                {
                  "name": "table_primary_key_alias",
                  "display": "主键别名",
                  "hint": {
                    "type": "str"
                  },
                  "description": "在目标数据表中的主键列名称。\n",
                  "required": false,
                  "value": "ts"
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
        "license_id": "opc_da",
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
              },
              {
                "name": "update_mode",
                "display": "点位更新模式",
                "hint": {
                  "type": "str",
                  "choices": [
                    "none",
                    "append",
                    "update"
                  ]
                },
                "short_description": "点位更新模式，在使用“选择数据点位”时，可以开启动态点位更新。none：不开启动态点位更新；append：开启动态点位更新，但只追加；update：开启动态点位更新，追加或删除。",
                "description": "点位更新模式，在使用“选择数据点位”时，可以开启动态点位更新。none：不开启动态点位更新；append：开启动态点位更新，但只追加；update：开启动态点位更新，追加或删除。\n",
                "value": "none"
              },
              {
                "name": "update_interval",
                "display": "点位更新间隔",
                "hint": {
                  "type": "integer",
                  "min": 60,
                  "max": 2147483647
                },
                "short_description": "动态点位更新间隔，在“点位更新模式”为 append 和 update 时生效，以秒为单位。",
                "description": "动态点位更新间隔，在“点位更新模式”为 append 和 update 时生效，以秒为单位。\n",
                "value": "600"
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
              "description": "OPC 数据写入使用 csv 文件定义每一个数据点位到 TDengine 数据子表的映射规则：\n\n(1) tag_name：必填，数据点位在 OPC DA 服务器上的 id；\n\n(2) stable：必填，数据点位对应的 TDengine 超级表；\n\n(3) tbname：必填，数据点位对应的 TDengine 子表；\n\n(4) enable：可选，默认值 '1'，指定是否采集该点位数据。0-不采集并且删除对应子表，1-采集点位数据，没有子表时创建子表；\n\n(5) value_col：可选，默认值 'val'。数据点位采集值在 TDengine 中对应的列名；\n\n(6) value_transform：可选，数据点位采集值在 taosX 中执行的变换函数，目前仅支持数值计算表达式，详见 transform 文档的 expr 表达式说明；\n\n(7) type：可选，默认值取源数据类型。数据点位采集值的数据类型，可用于替换超级表名称中的占位符 {type}；\n\n(8) quality_col：可选，数据点位采集值质量在 TDengine 中对应的列名；\n\n(9) ts_col/received_ts_col：必填，TDengine 时间戳主键定义：只存在 ts_col 时使用原始时间戳作为主键，只存在 received_ts_col 时使用采集时间戳作为主键，两列都存在时，居前的时间戳列作为主键；\n\n(10) ts_transform：可选，原始时间戳变换函数，参考 transform 数值计算表达式 expr 的说明；\n\n(11) received_ts_transform：可选，采集数据时间戳变换函数，参考 transform 数值计算表达式 expr 的说明；\n\n(12) tag::VARCHAR(200)::name：可选/可配置多个tag列；数据点位在 TDengine 中对应的 Tag 列；其中 tag 为保留关键字，表示该列为一个 tag 列；VARCHAR(200) 表示该 tag 的类型，也可以是其它合法的类型；name 是该 tag 的列名。\n\n更多填写规则请参考<a target=\"_blank\" href=\"/docs/advanced/data-in/opcda/\">企业版文档</a>。  \n",
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
                  "name": "super_table_expression",
                  "display": "超级表名称",
                  "hint": {
                    "type": "str"
                  },
                  "description": "支持 <super table prefix>_{type} 格式，{type} 表示点位的数据类型。\n",
                  "required": true,
                  "value": "opc_{type}"
                },
                {
                  "name": "child_table_expression",
                  "display": "表名称",
                  "hint": {
                    "type": "str"
                  },
                  "description": "支持 <child table prefix>_{tag_name} 格式，{tag_name} 表示点位名称。\n",
                  "required": true,
                  "value": "t_{tag_name}"
                },
                {
                  "name": "table_primary_key",
                  "display": "主键列",
                  "hint": {
                    "type": "str",
                    "choices": [
                      "original_ts",
                      "received_ts"
                    ]
                  },
                  "description": "目标数据表主键将使用选择的值作为时间戳主键列，original_ts 表示使用数据点位上报 OPC 服务时间，received_ts 表示 taosX 任务接收数据的时间。\n",
                  "required": false,
                  "value": "original_ts"
                },
                {
                  "name": "table_primary_key_alias",
                  "display": "主键别名",
                  "hint": {
                    "type": "str"
                  },
                  "description": "在目标数据表中的主键列名称。\n",
                  "required": false,
                  "value": "ts"
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
        "license_id": "influxdb",
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
            "placeholder": "8086",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "端口号的范围是 0-65535",
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
        "license_id": "opentsdb",
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
            "placeholder": "4242",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "端口号的范围是 0-65535",
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
        "license_id": "mqtt",
        "description": "MQTT 表示 Message Queuing Telemetry Transport （消息队列遥测传输）。它是一种轻量级的消息协议，易于实现和使用。它非常适合连接资源有限的设备，例如电池供电的设备或带宽较低的设备。MQTT也是实时控制系统等延迟重要的应用程序的不错选择。\n\nMQTT 通过使用发布/订阅模型来工作。这意味着设备可以将消息发布到主题，其他设备可以订阅这些主题以接收消息。这使得轻松将设备解耦，并根据需要扩展应用程序。\n\nMQTT 是物联网应用程序的流行选择。它得到了广泛的设备和平台支持，并提供许多开源和商业实现。\n\ntaosX 可以通过连接器插件从 MQTT 代理订阅数据。请查看每个部分的帮助消息以了解详细信息。\n",
        "options": {
          "host": {
            "required": true,
            "display": "MQTT 地址",
            "description": "MQTT 服务器地址。如: “127.0.0.1”\n如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。\n",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "MQTT 端口",
            "description": "MQTT 服务器端口",
            "placeholder": "1883",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "端口号的范围是 0-65535",
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
                "display": "客户端 ID",
                "hint": {
                  "type": "str"
                },
                "required": true,
                "short_description": "MQTT Broker 客户端 ID。",
                "description": "MQTT Broker 客户端 ID。",
                "placeholder": "示例：client_id"
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
                "placeholder": "topic1::0,topic2::1",
                "edit_disabled": true,
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
        "license_id": "kafka",
        "description": "Apache Kafka 是一个用于流处理、实时数据管道和大规模数据集成的开源分布式流系统。\nTDengine 可以高效地从 Kafka 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据流入库。\n",
        "options": {
          "params": [
            {
              "host": {
                "name": "host",
                "required": true,
                "display": "bootstrap-server",
                "description": "Kafka Server 地址。\n<br/>如果配置多个，所有 Kafka Server 必须属于同一个集群。\n<br/>如果使用了 Agent ，该地址必须能够从 Agent 访问。如果没有使用 Agent, 该地址必须能够从 TDengine 系统所在服务器访问。\n",
                "placeholder": "127.0.0.1"
              },
              "port": {
                "name": "port",
                "required": true,
                "display": "服务端口",
                "description": "Kafka 的端口",
                "placeholder": "9092",
                "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
                "patternMsg": "端口号的范围是 0-65535",
              },
            }
          ]
        },
        "groups": [
          {
            "name": "SASL 认证机制",
            "display_order": 1,
            "short_description": "用来认证服务器与客户端的一种认证机制。",
            "description": "用来认证服务器与客户端的一种认证机制。",
            "collapsible": true,
            "connection_option": true,
            "collapsed": false,
            "params": [
              {
                "name": "sasl_mechanism",
                "display": "认证机制",
                "hint": {
                  "type": "str",
                  "choices": [
                    "PLAIN",
                    "SCRAM-SHA-256",
                    "GSSAPI"
                  ]
                },
                "short_description": "SASL 的认证机制",
                "description": "SASL 的认证机制",
                "required": true,
                "value": "PLAIN"
              },
              {
                "name": "sasl_username",
                "display": "用户名",
                "hint": {
                  "type": "str"
                },
                "short_description": "用于 SASL 认证机制的用户名",
                "description": "用于 SASL 认证机制的用户名",
                "required": true
              },
              {
                "name": "sasl_password",
                "display": "密码",
                "hint": {
                  "type": "password"
                },
                "short_description": "用于 SASL 认证机制的密码",
                "description": "用于 SASL 认证机制的密码",
                "required": true
              },
              {
                "name": "sasl_kerberos_service_name",
                "display": "Kerberos 服务名",
                "description": " 用于 GSSAPI 认证机制的 Kerberos 服务名",
                "placeholder": "示例：kafka",
                "required": true,
                "hint": {
                  "type": "str"
                }
              },
              {
                "name": "sasl_kerberos_principal",
                "display": "Kerberos 主体",
                "description": " 用于 GSSAPI 认证机制的 Kerberos 主体",
                "placeholder": "示例：kafkaclient",
                "required": true,
                "hint": {
                  "type": "str"
                }
              },
              {
                "name": "sasl_kerberos_kinit_cmd",
                "display": "Kerberos 初始化命令",
                "description": "用于 GSSAPI 认证机制的 Kerberos 初始化命令",
                "placeholder": "示例：kinit -R -t '%{sasl.kerberos.keytab}' -k %{sasl.kerberos.principal}",
                "required": false,
                "hint": {
                  "type": "str"
                }
              },
              {
                "name": "sasl_kerberos_keytab",
                "display": "Kerberos 密钥表",
                "description": "用于 GSSAPI 认证机制的 Kerberos 密钥表",
                "required": true,
                "hint": {
                  "type": "file"
                }
              }
            ]
          },
          {
            "name": "SSL 证书",
            "display_order": 2,
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
                "short_description": "CA 证书文件(PEM格式), 用于验证 broker 的密钥。",
                "description": "CA 证书文件(PEM格式), 用于验证 broker 的密钥。",
                "required": true
              },
              {
                "name": "ca_password",
                "display": "CA 密码",
                "hint": {
                  "type": "password"
                },
                "short_description": "CA 私钥密码",
                "description": "CA 私钥密码",
                "required": true
              },
              {
                "name": "cert",
                "display": "客户端证书",
                "hint": {
                  "type": "file"
                },
                "short_description": "用于身份验证的客户端公钥文件(PEM格式)。",
                "description": "用于身份验证的客户端公钥文件(PEM格式)。",
                "required": true
              },
              {
                "name": "cert_key",
                "display": "客户端私钥",
                "hint": {
                  "type": "file"
                },
                "short_description": "用于身份验证的客户端私钥文件(PEM格式)。",
                "description": "用于身份验证的客户端私钥文件(PEM格式)。",
                "required": true
              }
            ]
          },
          {
            "name": "采集配置",
            "display_order": 3,
            "short_description": "数据采集相关配置项。",
            "description": "数据采集相关配置项。",
            "collapsible": false,
            "connection_option": true,
            "params": [
              {
                "name": "timeout",
                "display": "超时时间",
                "hint": {
                  "type": "timeout",
                  "choices": [
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                    {
                      "value": "ms",
                      "label": "毫秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "指定 Kafka Source 的超时时间，当从 Kafka 消费不到任何数据，超过 timeout 后，数据采集任务会退出。 默认值是 0 ms。 当 timeout 设置为 0 时，Kafka Source 会一直等待，直到有数据可用，或者发生错误。",
                "description": "指定 Kafka Source 的超时时间，当从 Kafka 消费不到任何数据，超过 timeout 后，数据采集任务会退出。 默认值是 0 ms。 当 timeout 设置为 `0` 时，Kafka Source 会一直等待，直到有数据可用，或者发生错误。\n",
                "required": false,
                "placeholder": "输入范围为[0,60000]整数",
                "type_value": "ms",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
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
                "placeholder": "tp1,tp2",
                "edit_disabled": true,
              },
              {
                "name": "client_id",
                "display": "客户端 ID",
                "hint": {
                  "type": "str"
                },
                "required": true,
                "short_description": "Kafka Broker 客户端 ID。",
                "description": "Kafka Broker 客户端 ID。",
                "placeholder": "示例：client_id"
              },
              {
                "name": "group",
                "display": "消费者组 ID",
                "hint": {
                  "type": "str"
                },
                "required": true,
                "short_description": "Kafka 消费者组 ID。",
                "description": "Kafka 消费者组 ID。",
                "placeholder": "示例：group_id"
              },
              {
                "name": "client_id",
                "display": "Client ID",
                "hint": {
                  "type": "str"
                },
                "required": true,
                "short_description": "Kafka Broker 客户端 ID。",
                "description": "Kafka Broker 客户端 ID。",
                "placeholder": "client_id"
              },
              {
                "name": "group",
                "display": "消费者组 ID",
                "hint": {
                  "type": "str"
                },
                "required": true,
                "short_description": "Kafka 消费者组 ID。",
                "description": "Kafka 消费者组 ID。",
                "placeholder": "group_id"
              },
              {
                "name": "fallback_offset",
                "display": "Offset",
                "hint": {
                  "type": "str",
                  "choices": [
                    "Earliest",
                    "Latest"
                  ]
                },
                "short_description": "Fallback Offset 参数可以指定以下值：",
                "description": "Fallback Offset 参数可以指定以下值：\n* `Earliest`：用于请求最早的 offset. \n* `Latest`：用于请求最晚的 offset. \n* 默认值为Earliest。",
                "required": false,
                "placeholder": "Earliest",
                "value": "Earliest"
              },
              // {
              //   "name": "fetch_max_wait_time",
              //   "display": "等待超时时间",
              //   "hint": {
              //     "type": "integer",
              //     "min": 0,
              //     "max": 300
              //   },
              //   "short_description": "超时时间范围内没有新增数据，Kafka 任务将自动结束。。",
              //   "description": "超时时间范围内没有新增数据，Kafka 任务将自动结束。\n\n默认为 `0`: 表示无超时时间，持续进行订阅，单位为 s。\n",
              //   "required": false,
              //   "placeholder": "",
              //   "value": 0,
              // }
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
        "license_id": "csv",
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
        "license_id": "avevahistorian",
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
            "placeholder": "1433",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "端口号的范围是 0-65535",
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
                  "type": "duration",
                  "choices": [
                    {
                      "value": "y",
                      "label": "年"
                    },
                    {
                      "value": "mo",
                      "label": "月"
                    },
                    {
                      "value": "d",
                      "label": "天"
                    },
                    {
                      "value": "w",
                      "label": "周"
                    },
                    {
                      "value": "h",
                      "label": "小时"
                    },
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                    {
                      "value": "ms",
                      "label": "毫秒"
                    },
                    {
                      "value": "u",
                      "label": "微秒"
                    },
                    {
                      "value": "ns",
                      "label": "纳秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "历史数据迁移时，每次查询的时间窗口。",
                "description": "历史数据迁移时，每次查询的时间窗口。\n",
                "required": false,
                "placeholder": "输入范围为[0,60000]整数",
                "value": "1",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              },
              {
                "name": "retrieveInterval",
                "display": "实时同步的时间间隔",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "天"
                    },
                    {
                      "value": "h",
                      "label": "小时"
                    },
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                    {
                      "value": "ms",
                      "label": "毫秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "实时数据同步时，每次查询的时间间隔。",
                "description": "实时数据同步时，每次查询的时间间隔。\n",
                "required": false,
                "placeholder": "输入范围为[0,60000]整数",
                "value": "10",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              },
              {
                "name": "tolerance",
                "display": "乱序时间上限",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "天"
                    },
                    {
                      "value": "h",
                      "label": "小时"
                    },
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                    {
                      "value": "ms",
                      "label": "毫秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "容忍乱序数据延迟到达的时间上限。",
                "description": "容忍乱序数据延迟到达的时间上限。\n",
                "required": false,
                "placeholder": "输入范围为[0,60000]整数",
                "value": "0",
                "type_value": "ms",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
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
          "display": "Payload 转换",
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
        "license_id": "mysql",
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
            "placeholder": "3306",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "端口号的范围是 0-65535",
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
                "name": "ssl_mode",
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
                "name": "subtable_fields",
                "display": "子表字段",
                "hint": {
                  "type": "str"
                },
                "short_description": "用于拆分子表的字段。",
                "description": "用于拆分子表的字段。",
                "required": false,
                "placeholder": "select distinct col_name1,col_name2 from table",
              },
              {
                "name": "sql",
                "display": "SQL 模板",
                "hint": {
                  "type": "str"
                },
                "short_description": "用于查询的 SQL 语句，SQL 语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现。",
                "description": "用于查询的 SQL 语句，SQL 语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现（至少一个闭区间）。\nSQL使用不同的占位符表示不同的时间格式要求，具体有以下占位符格式：\n1. `${start}`、`${end}`：表示 RFC3339 格式时间戳，如：2024-03-14T08:00:00+0800\n2. `${start_no_tz}`、`${end_no_tz}`：表示不带时区的 RFC3339 字符串：2024-03-14T08:00:00\n3. `${start_date}`、`${end_date}`：表示仅日期，如：2024-03-14\n\n如果使用子表字段，需要在语句中拼接字段占位符 \`and ${col_name1} and ${col_name2}\`，请注意，字段占位符大小写敏感，需要与数据库中字段保持一致。如果要按指定字段排序（建议按时间正序），需要在语句中拼接 \`ORDER BY time。\`\n\n示例：\`SELECT * FROM table WHERE time >= ${start} AND time < ${end} and ${col_name1} and ${col_name2} ORDER BY time\`",
                "required": true,
                "placeholder": "完整示例请在描述中查看",
                "grid_two": true,
              },
              {
                "name": "start",
                "display": "起始时间",
                "hint": {
                  "type": "time"
                },
                "short_description": "迁移数据的起始时间。",
                "description": "迁移数据的起始时间。\n",
                "required": true,
                "placeholder": "如：2023-01-01 00:00:00"
              },
              {
                "name": "end",
                "display": "结束时间",
                "hint": {
                  "type": "time"
                },
                "short_description": "迁移数据的结束时间，可留空。如果设置，则迁移任务执行到结束时间后，任务完成自动停止；如果留空，则持续同步实时数据，任务不会自动停止。",
                "description": "迁移数据的结束时间，可留空。如果设置，则迁移任务执行到结束时间后，任务完成自动停止；如果留空，则持续同步实时数据，任务不会自动停止。\n",
                "required": false,
                "placeholder": "如：2024-01-01 00:00:00"
              },
              {
                "name": "interval",
                "display": "查询间隔",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "天"
                    },
                    {
                      "value": "h",
                      "label": "小时"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "分段查询数据的时间间隔，默认1天。为了避免查询数据量过大，一次数据同步子任务会使用查询间隔分时间段查询数据。",
                "description": "分段查询数据的时间间隔，默认1天。为了避免查询数据量过大，一次数据同步子任务会使用查询间隔分时间段查询数据。\n",
                "required": false,
                "placeholder": "输入范围为[0,600]整数",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              },
              {
                "name": "delay",
                "display": "延迟时长",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "实时同步数据场景中，为了避免延迟写入的数据丢失，每次同步任务会读取延迟时长之前的数据。",
                "description": "实时同步数据场景中，为了避免延迟写入的数据丢失，每次同步任务会读取延迟时长之前的数据。\n",
                "required": false,
                "placeholder": "输入范围为[0,60000]整数",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
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
        "license_id": "postgres",
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
            "placeholder": "5432",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "端口号的范围是 0-65535",
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
                "name": "application_name",
                "display": "应用名称",
                "hint": {
                  "type": "str"
                },
                "short_description": "设置应用程序名称，用于标识连接的应用程序。",
                "description": "设置应用程序名称，用于标识连接的应用程序。",
                "placeholder": "示例: TDengine"
              },
              {
                "name": "ssl_mode",
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
                "name": "subtable_fields",
                "display": "子表字段",
                "hint": {
                  "type": "str"
                },
                "short_description": "用于拆分子表的字段。",
                "description": "用于拆分子表的字段。",
                "required": false,
                "placeholder": "select distinct col_name1,col_name2 from table",
              },
              {
                "name": "sql",
                "display": "SQL 模板",
                "hint": {
                  "type": "str"
                },
                "short_description": "用于查询的 SQL 语句，SQL 语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现。",
                "description": "用于查询的 SQL 语句，SQL 语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现（至少一个闭区间）。\nSQL使用不同的占位符表示不同的时间格式要求，具体有以下占位符格式：\n1. `${start}`、`${end}`：表示 RFC3339 格式时间戳，如：2024-03-14T08:00:00+0800\n2. `${start_no_tz}`、`${end_no_tz}`：表示不带时区的 RFC3339 字符串：2024-03-14T08:00:00\n3. `${start_date}`、`${end_date}`：表示仅日期，如：2024-03-14\n\n如果使用子表字段，需要在语句中拼接字段占位符 \`and ${col_name1} and ${col_name2}\`，请注意，字段占位符大小写敏感，需要与数据库中字段保持一致。如果要按指定字段排序（建议按时间正序），需要在语句中拼接 \`ORDER BY time\`。\n\n示例：\`SELECT * FROM table WHERE time >= ${start} AND time < ${end} and ${col_name1} and ${col_name2} ORDER BY time\`",
                "required": true,
                "placeholder": "完整示例请在描述中查看",
                "grid_two": true,
              },
              {
                "name": "start",
                "display": "起始时间",
                "hint": {
                  "type": "time"
                },
                "short_description": "迁移数据的起始时间。",
                "description": "迁移数据的起始时间。\n",
                "required": true,
                "placeholder": "如：2023-01-01 00:00:00"
              },
              {
                "name": "end",
                "display": "结束时间",
                "hint": {
                  "type": "time"
                },
                "short_description": "迁移数据的结束时间，可留空。如果设置，则迁移任务执行到结束时间后，任务完成自动停止；如果留空，则持续同步实时数据，任务不会自动停止。",
                "description": "迁移数据的结束时间，可留空。如果设置，则迁移任务执行到结束时间后，任务完成自动停止；如果留空，则持续同步实时数据，任务不会自动停止。\n",
                "required": false,
                "placeholder": "如：2024-01-01 00:00:00"
              },
              {
                "name": "interval",
                "display": "查询间隔",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "天"
                    },
                    {
                      "value": "h",
                      "label": "小时"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "分段查询数据的时间间隔，默认1天。为了避免查询数据量过大，一次数据同步子任务会使用查询间隔分时间段查询数据。",
                "description": "分段查询数据的时间间隔，默认1天。为了避免查询数据量过大，一次数据同步子任务会使用查询间隔分时间段查询数据。\n",
                "required": false,
                "placeholder": "输入范围为[0,600]整数",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              },
              {
                "name": "delay",
                "display": "延迟时长",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "实时同步数据场景中，为了避免延迟写入的数据丢失，每次同步任务会读取延迟时长之前的数据。",
                "description": "实时同步数据场景中，为了避免延迟写入的数据丢失，每次同步任务会读取延迟时长之前的数据。\n",
                "required": false,
                "placeholder": "输入范围为[0,60000]整数",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
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
        "id": "oracle",
        "type": "uri",
        "name": "Oracle",
        "license_id": "oracle",
        "description": "Oracle 数据库系统是世界上流行的关系数据库管理系统，系统可移植性好、使用方便、功能强，适用于各类大、中、小微机环境。它是一种高效率的、可靠性好的、适应高吞吐量的数据库方案。\nTDengine 可以高效地从 Oracle 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n",
        "options": {
          "host": {
            "required": true,
            "display": "服务地址",
            "description": "Oracle 的服务器地址",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "服务端口",
            "description": "Oracle 的端口",
            "placeholder": "1521",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "端口号的范围是 0-65535",
          },
          "subject": {
            "required": true,
            "display": "数据库",
            "description": "Oracle 数据库名称",
            "placeholder": "示例: db1"
          }
        },
        "authentication": {
          "display": "认证",
          "description": "使用用户名和密码访问 Oracle 数据库",
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
            "name": "SQL 查询",
            "display_order": 2,
            "short_description": "数据采集相关配置项。",
            "description": "数据采集相关配置项。",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "subtable_fields",
                "display": "子表字段",
                "hint": {
                  "type": "str"
                },
                "short_description": "用于拆分子表的字段。",
                "description": "用于拆分子表的字段。",
                "required": false,
                "placeholder": "select distinct col_name1,col_name2 from table",
              },
              {
                "name": "sql",
                "display": "SQL 模板",
                "hint": {
                  "type": "str"
                },
                "short_description": "用于查询的 SQL 语句，SQL 语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现。",
                "description": "用于查询的 SQL 语句，SQL 语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现（至少一个闭区间）。\nSQL使用不同的占位符表示不同的时间格式要求，具体有以下占位符格式：\n1. `${start}`、`${end}`：表示 RFC3339 格式时间戳，如：2024-03-14T08:00:00+0800\n2. `${start_no_tz}`、`${end_no_tz}`：表示不带时区的 RFC3339 字符串：2024-03-14T08:00:00\n3. `${start_date}`、`${end_date}`：表示仅日期，但 Oracle 中没有纯日期类型，所以它会带零时零分零秒，如：2024-03-14 00:00:00，所以使用 date <= `${end_date}` 时需要注意，它不能包含 2024-03-14 当天数据。\n\n如果使用子表字段，需要在语句中拼接字段占位符 \`and ${col_name1} and ${col_name2}\`，请注意，字段占位符大小写敏感，需要与数据库中字段保持一致。如果要按指定字段排序（建议按时间正序），需要在语句中拼接 \`ORDER BY time\`。\n\n示例：\`SELECT * FROM table WHERE time >= ${start} AND time < ${end} and ${col_name1} and ${col_name2} ORDER BY time\`",
                "required": true,
                "placeholder": "完整示例请在描述中查看",
                "grid_two": true,
              },
              {
                "name": "start",
                "display": "起始时间",
                "hint": {
                  "type": "time"
                },
                "short_description": "迁移数据的起始时间。",
                "description": "迁移数据的起始时间。\n",
                "required": true,
                "placeholder": "如：2023-01-01 00:00:00"
              },
              {
                "name": "end",
                "display": "结束时间",
                "hint": {
                  "type": "time"
                },
                "short_description": "迁移数据的结束时间，可留空。如果设置，则迁移任务执行到结束时间后，任务完成自动停止；如果留空，则持续同步实时数据，任务不会自动停止。",
                "description": "迁移数据的结束时间，可留空。如果设置，则迁移任务执行到结束时间后，任务完成自动停止；如果留空，则持续同步实时数据，任务不会自动停止。\n",
                "required": false,
                "placeholder": "如：2024-01-01 00:00:00"
              },
              {
                "name": "interval",
                "display": "查询间隔",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "天"
                    },
                    {
                      "value": "h",
                      "label": "小时"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "分段查询数据的时间间隔，默认1天。为了避免查询数据量过大，一次数据同步子任务会使用查询间隔分时间段查询数据。",
                "description": "分段查询数据的时间间隔，默认1天。为了避免查询数据量过大，一次数据同步子任务会使用查询间隔分时间段查询数据。\n",
                "required": false,
                "placeholder": "输入范围为[0,600]整数",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              },
              {
                "name": "delay",
                "display": "延迟时长",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "实时同步数据场景中，为了避免延迟写入的数据丢失，每次同步任务会读取延迟时长之前的数据。",
                "description": "实时同步数据场景中，为了避免延迟写入的数据丢失，每次同步任务会读取延迟时长之前的数据。\n",
                "required": false,
                "placeholder": "输入范围为[0,60000]整数",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
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
        "id": "mssql",
        "type": "uri",
        "name": "Microsoft SQL Server",
        "license_id": "mssql",
        "description": "Microsoft SQL Server 是一种关系型数据库管理系统，由 Microsoft 公司开发，具有使用方便可伸缩性好与相关软件集成程度高等优点。\n\nTDengine 可以高效地从 Microsoft SQL Server 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n",
        "options": {
          "host": {
            "required": true,
            "display": "服务地址",
            "description": "SQL Server 的服务器地址",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "服务端口",
            "description": "SQL Server 的端口",
            "placeholder": "1433",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "端口号的范围是 0-65535",
          },
          "subject": {
            "required": true,
            "display": "数据库",
            "description": "SQL Server 数据库名称",
            "placeholder": "示例: db1"
          }
        },
        "authentication": {
          "display": "认证",
          "description": "使用用户名和密码访问 SQL Server 数据库",
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
                "name": "instance_name",
                "display": "实例名称",
                "hint": {
                  "type": "str"
                },
                "short_description": "SQL Server 实例名称",
                "description": "SQL Server 实例名称",
                "placeholder": "示例: MSSQLSERVER"
              },
              {
                "name": "application_name",
                "display": "应用名称",
                "hint": {
                  "type": "str"
                },
                "short_description": "设置应用程序名称，用于标识连接的应用程序。",
                "description": "设置应用程序名称，用于标识连接的应用程序。",
                "placeholder": "示例: TDengine"
              },
              {
                "name": "encryption",
                "display": "加密",
                "hint": {
                  "type": "str",
                  "choices": [
                    "Off",
                    "On",
                    "NotSupported",
                    "Required"
                  ]
                },
                "short_description": "设置是否使用加密连接。",
                "description": "设置是否使用加密连接。",
                "placeholder": "请选择加密方式",
                "value": "Off"
              },
              {
                "name": "trust_cert",
                "display": "信任证书",
                "hint": {
                  "type": "bool"
                },
                "short_description": "设置是否信任服务器证书。",
                "description": "设置是否信任服务器证书。",
                "placeholder": "请选择是否信任证书",
                "value": "true"
              },
              {
                "name": "trust_cert_ca",
                "display": "信任证书 CA",
                "hint": {
                  "type": "file"
                },
                "short_description": "设置是否信任服务器证书 CA。",
                "description": "设置是否信任服务器证书 CA。",
                "placeholder": "如果信任请上传证书 CA"
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
                "name": "subtable_fields",
                "display": "子表字段",
                "hint": {
                  "type": "str"
                },
                "short_description": "用于拆分子表的字段。",
                "description": "用于拆分子表的字段。",
                "required": false,
                "placeholder": "select distinct col_name1,col_name2 from table",
              },
              {
                "name": "sql",
                "display": "SQL 模板",
                "hint": {
                  "type": "str"
                },
                "short_description": "用于查询的 SQL 语句，SQL 语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现。",
                "description": "用于查询的 SQL 语句，SQL 语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现（至少一个闭区间）。\nSQL使用不同的占位符表示不同的时间格式要求，具体有以下占位符格式：\n1. `${start}`、`${end}`：表示 RFC3339 格式时间戳，如：2024-03-14T08:00:00+0800\n2. `${start_no_tz}`、`${end_no_tz}`：表示不带时区的 RFC3339 字符串：2024-03-14T08:00:00\n3. `${start_date}`、`${end_date}`：表示仅日期，如：2024-03-14\n\n如果使用子表字段，需要在语句中拼接字段占位符 \`and ${col_name1} and ${col_name2}\`，请注意，字段占位符大小写敏感，需要与数据库中字段保持一致。如果要按指定字段排序（建议按时间正序），需要在语句中拼接 \`ORDER BY time\`。\n\n示例：\`SELECT * FROM table WHERE time >= ${start} AND time < ${end} and ${col_name1} and ${col_name2} ORDER BY time\`",
                "required": true,
                "placeholder": "完整示例请在描述中查看",
                "grid_two": true,
              },
              {
                "name": "start",
                "display": "起始时间",
                "hint": {
                  "type": "time"
                },
                "short_description": "迁移数据的起始时间。",
                "description": "迁移数据的起始时间。\n",
                "required": true,
                "placeholder": "如：2023-01-01 00:00:00"
              },
              {
                "name": "end",
                "display": "结束时间",
                "hint": {
                  "type": "time"
                },
                "short_description": "迁移数据的结束时间，可留空。如果设置，则迁移任务执行到结束时间后，任务完成自动停止；如果留空，则持续同步实时数据，任务不会自动停止。",
                "description": "迁移数据的结束时间，可留空。如果设置，则迁移任务执行到结束时间后，任务完成自动停止；如果留空，则持续同步实时数据，任务不会自动停止。\n",
                "required": false,
                "placeholder": "如：2024-01-01 00:00:00"
              },
              {
                "name": "interval",
                "display": "查询间隔",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "天"
                    },
                    {
                      "value": "h",
                      "label": "小时"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "分段查询数据的时间间隔，默认1天。为了避免查询数据量过大，一次数据同步子任务会使用查询间隔分时间段查询数据。",
                "description": "分段查询数据的时间间隔，默认1天。为了避免查询数据量过大，一次数据同步子任务会使用查询间隔分时间段查询数据。\n",
                "required": false,
                "placeholder": "输入范围为[0,600]整数",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              },
              {
                "name": "delay",
                "display": "延迟时长",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "实时同步数据场景中，为了避免延迟写入的数据丢失，每次同步任务会读取延迟时长之前的数据。",
                "description": "实时同步数据场景中，为了避免延迟写入的数据丢失，每次同步任务会读取延迟时长之前的数据。\n",
                "required": false,
                "placeholder": "输入范围为[0,60000]整数",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
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
        "id": "mongodb",
        "type": "uri",
        "name": "MongoDB",
        "license_id": "mongodb",
        "description": "MongoDB 是一个介于关系型数据库与非关系型数据库之间的产品，被广泛应用于内容管理系统、移动应用与物联网等众多领域。\n\nTDengine 可以高效地从 MongoDB 读取数据并将其写入 TDengine，以实现历史数据迁移或实时数据同步。\n",
        "options": {
          "host": {
            "required": true,
            "display": "服务地址",
            "description": "MongoDB 的服务器地址",
            "placeholder": "127.0.0.1"
          },
          "port": {
            "required": true,
            "display": "服务端口",
            "description": "MongoDB 的端口",
            "placeholder": "27017",
            "pattern": "^(?:0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$",
            "patternMsg": "端口号的范围是 0-65535",
          },
          // "load_balanced": {
          //   "required": false,
          //   "display": "是否负载均衡",
          //   "description": "是否通过负载均衡进行连接。\n- *true*:host 地址被当作负载均衡地址 \n- *false*:host 地址被当作数据库地址\n",
          //   "hint": {
          //     "type": "bool",
          //   }
          // },
          // "direct_connection": {
          //   "required": false,
          //   "display": "是否直连",
          //   "description": "是否直接连接到单个主机或者自动发现集群中所有服务器。\n- *true*:host 直接连接到 host:port \n- *false*:host 发现集群中其他服务器\n",
          //   "hint": {
          //     "type": "bool",
          //   },
          //   "value": "true"
          // },
          // "repl_set_name": {
          //   "required": false,
          //   "display": "副本名称",
          //   "description": "客户端连接到指定名称的集群副本。如果指定了副本名称，则只连接到此副本服务器。",
          //   "placeholder": "",
          // },
          // "local_threshold": {
          //   "required": false,
          //   "display": "超时阈值",
          //   "description": "用于确定与所有服务器中最短往返时间相比，客户端与服务器之间的平均往返时间被允许增加多少。当值为 0 时，表示没有延迟窗口，因此只会连接平均往返时间最低的服务器。默认 15 ms。",
          //   "hint": {
          //     "type": "duration",
          //     "choices": [
          //       {
          //         "value": "m",
          //         "label": "分钟"
          //       },
          //       {
          //         "value": "s",
          //         "label": "秒"
          //       },
          //     ]
          //   },
          //   "placeholder": "15",
          //   "value": "15",
          //   "type_value": "s",
          //   "pattern": null,
          //   "patternMsg": "只能输入正整数或者0",
          // }
        },
        "authentication": {
          "display": "认证",
          "description": "使用用户名和密码访问 MongoDB 数据库",
          "value": "plain",
          "alternatives": [
            {
              "name": "plain",
              "display": "用户名密码访问",
              "params": [
                {
                  "name": "username",
                  "required": true,
                  "display": "用户",
                  "placeholder": "请输入用户名"
                },
                {
                  "name": "password",
                  "required": true,
                  "display": "密码",
                  "placeholder": "请输入密码"
                },
                // {
                //   "name": "mechanism",
                //   "required": false,
                //   "display": "认证机制",
                //   "placeholder": "请选择认证机制",
                //   "short_description": "要使用的身份验证机制，如果没有提供，将与服务器协商一个。\n",
                //   "description": "要使用的身份验证机制，如果没有提供，将与服务器协商一个。\n",
                //   "hint": {
                //     "type": "str",
                //     "choices": [
                //       "MongoDbCr",
                //       "ScramSha1",
                //       "ScramSha256",
                //       "MongoDbX509",
                //       "Gssapi",
                //       "Plain",
                //       "MongoDbAws",
                //       "MongoDbOidc",
                //     ]
                //   },
                // },
                {
                  "name": "source",
                  "required": false,
                  "display": "认证数据库",
                  "placeholder": "认证数据库",
                  "short_description": "MongoDB 中存储用户信息的数据库，默认为 admin。\n",
                  "description": "MongoDB 中存储用户信息的数据库，默认为 admin。\n",
                },
              ],
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
                "name": "app_name",
                "display": "应用名称",
                "hint": {
                  "type": "str",
                },
                "short_description": "用于标识客户端。",
                "description": "用于标识客户端。",
                "placeholder": "示例: TDengine",
              },
            ]
          },
          {
            "name": "SSL 证书",
            "short_description": "使用证书和私钥建立连接以启用 SSL。",
            "description": "使用证书和私钥建立连接以启用 SSL。",
            "collapsible": true,
            "connection_option": true,
            "collapsed": false,
            "params": [
              {
                "name": "ca_file_path",
                "display": "CA 文件",
                "hint": {
                  "type": "file"
                },
                "short_description": "CA 证书文件",
                "description": "CA 证书文件",
                "required": true
              },
              {
                "name": "cert_key_file_path",
                "display": "证书文件",
                "hint": {
                  "type": "file"
                },
                "short_description": ".cert 文件",
                "description": ".cert 文件",
                "required": true
              },
            ]
          },
          {
            "name": "数据查询",
            "display_order": 2,
            "short_description": "数据采集相关配置项。",
            "description": "数据采集相关配置项。",
            "collapsible": false,
            "connection_option": false,
            "params": [
              {
                "name": "database",
                "display": "数据库",
                "hint": {
                  "type": "str"
                },
                "short_description": "源数据库。",
                "description": "MongoDB 中源数据库，可以使用占位符进行动态配置，可用占位符列表：\n<ul><li>${Y} 完整的公历年表示，零填充的 4 位整数</li><li>${y} 公历年除以 100，零填充的 2 位整数</li><li>${M} 整数月份（1 - 12）</li><li>${m} 整数月份（01 - 12）</li><li>${B} 月份英文全拼</li><li>${b} 月份英文的缩写（3 个字母）</li><li>${D} 日期的数字表示（1 - 31）</li><li>${d} 日期的数字表示（01 - 31）</li><li>${J} 一年中的第几天（1 - 366）</li><li>${j} 一年中的第几天（001 - 366）</li><li>${F} 相当于 ${Y}-${m}-${d}</li></ul>\n",
                "required": true,
                "placeholder": "database_${Y}",
              },
              {
                "name": "collection",
                "display": "集合",
                "hint": {
                  "type": "str"
                },
                "short_description": "源集合。",
                "description": "MongoDB 中集合，可以使用占位符进行动态配置，可用占位符列表：\n<ul><li>${Y} 完整的公历年表示，零填充的 4 位整数</li><li>${y} 公历年除以 100，零填充的 2 位整数</li><li>${M} 整数月份（1 - 12）</li><li>${m} 整数月份（01 - 12）</li><li>${B} 月份英文全拼</li><li>${b} 月份英文的缩写（3 个字母）</li><li>${D} 日期的数字表示（1 - 31）</li><li>${d} 日期的数字表示（01 - 31）</li><li>${J} 一年中的第几天（1 - 366）</li><li>${j} 一年中的第几天（001 - 366）</li><li>${F} 相当于 ${Y}-${m}-${d}</li></ul>",
                "required": true,
                "placeholder": "collection_${md}",
              },
              {
                "name": "subtable_fields",
                "display": "子表字段",
                "hint": {
                  "type": "str"
                },
                "short_description": "用于拆分子表的字段。",
                "description": "用于拆分子表的字段。",
                "required": false,
                "placeholder": "col_name1,col_name2,...",
              },
              {
                "name": "sql",
                "display": "查询模板",
                "hint": {
                  "type": "str"
                },
                "short_description": "用于查询数据的查询语句，JSON格式，语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现。",
                "description": "用于查询数据的查询语句，JSON格式，语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现（至少一个闭区间）。\n使用不同的占位符表示不同的时间格式要求，具体有以下占位符格式：\n1. `${start_datetime}`、`${end_datetime}`：对应后端 datetime 类型字段的筛选，如：{\"ddate\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}} 将被转换为 {\"ddate\":{\"$gte\":{\"$date\":\"2024-06-01T00:00:00+00:00\"},\"$lt\":{\"$date\":\"2024-07-01T00:00:00+00:00\"}}}\n2. `${start_timestamp}`、`${end_timestamp}`：对应后端 timestamp 类型字段的筛选，如：{\"ttime\":{\"$gte\":${start_timestamp},\"$lt\":${end_timestamp}}} 将被转换为 {\"ttime\":{\"$gte\":{\"$timestamp\":{\"t\":123,\"i\":456}},\"$lt\":{\"$timestamp\":{\"t\":123,\"i\":456}}}}\n\n如果使用子表字段，需要在语句中拼接字段占位符。\n\n示例：\`{\"ddate\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}},${col_name1},${col_name2}}\`",
                "required": true,
                "placeholder": "{\"ddate\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}},${col_name1},${col_name2}}",
                "grid_two": true,
              },
              {
                "name": "sort",
                "display": "查询排序",
                "hint": {
                  "type": "str"
                },
                "short_description": "执行查询时的排序条件。",
                "description": "执行查询时的排序条件。\n\n1.`{\"createtime\":1}`：MongoDB 查询结果按 `createtime` 正序返回。\n\n2.`{\"createdate\":1, \"createtime\":1}`：MongoDB 查询结果按 `createdate` 正序、`createtime` 正序返回。",
                "required": false,
                "placeholder": "{\"createtime\":1}",
                "validator": "checkJson"
              },
              {
                "name": "start",
                "display": "起始时间",
                "hint": {
                  "type": "time"
                },
                "short_description": "迁移数据的起始时间。",
                "description": "迁移数据的起始时间。\n",
                "required": true,
                "placeholder": "如：2023-01-01 00:00:00"
              },
              {
                "name": "end",
                "display": "结束时间",
                "hint": {
                  "type": "time"
                },
                "short_description": "迁移数据的结束时间，可留空。如果设置，则迁移任务执行到结束时间后，任务完成自动停止；如果留空，则持续同步实时数据，任务不会自动停止。",
                "description": "迁移数据的结束时间，可留空。如果设置，则迁移任务执行到结束时间后，任务完成自动停止；如果留空，则持续同步实时数据，任务不会自动停止。\n",
                "required": false,
                "placeholder": "如：2024-01-01 00:00:00"
              },
              {
                "name": "interval",
                "display": "查询间隔",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "d",
                      "label": "天"
                    },
                    {
                      "value": "h",
                      "label": "小时"
                    },
                  ],
                  "min": 0,
                  "max": 600
                },
                "short_description": "分段查询数据的时间间隔，默认1天。为了避免查询数据量过大，一次数据同步子任务会使用查询间隔分时间段查询数据。",
                "description": "分段查询数据的时间间隔，默认1天。为了避免查询数据量过大，一次数据同步子任务会使用查询间隔分时间段查询数据。\n",
                "required": false,
                "placeholder": "输入范围为[0,600]整数",
                "type_value": "d",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
              },
              {
                "name": "delay",
                "display": "延迟时长",
                "hint": {
                  "type": "duration",
                  "choices": [
                    {
                      "value": "m",
                      "label": "分钟"
                    },
                    {
                      "value": "s",
                      "label": "秒"
                    },
                  ],
                  "min": 0,
                  "max": 60000
                },
                "short_description": "实时同步数据场景中，为了避免延迟写入的数据丢失，每次同步任务会读取延迟时长之前的数据。",
                "description": "实时同步数据场景中，为了避免延迟写入的数据丢失，每次同步任务会读取延迟时长之前的数据。\n",
                "required": false,
                "placeholder": "输入范围为[0,60000]整数",
                "type_value": "s",
                "pattern": null,
                "patternMsg": "只能输入正整数或者0",
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
          "display": "Payload 转换",
          "required": true,
          "description": "Kafka 连接器会上传以下六列到服务端：<br>\n\n- **ts**: 采集时间戳。<br>\n- **topic**: 订阅主题名。<br>\n- **partition**: 当前消息所在的分区 ID。<br>\n- **offset**: 当前消息的偏移量。<br>\n- **key**: 当前消息的 Key。<br>\n- **value**: 当前消息的数据内容。<br>\n\ntaosX 可以使用 JSON 提取器解析数据，并允许用户在数据库中指定数据模型，<br>\n包括，指定表名称和超级表名，设置普通列和标签列等。\n",
          "fields": [
            {
              "name": "value",
              "description": "消息体。",
              "type": "varchar"
            }
          ]
        }
      },
    ]
  }

}
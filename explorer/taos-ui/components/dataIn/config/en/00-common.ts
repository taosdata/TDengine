export default {
  exceptionStrategy: `{
    "label": "Exception handling strategy",
    "field": "write_config",
    "description": "Adjust the configuration parameters for the write strategy. The following options can be modified.",
    "type": "collapse",
    "defaultValue": true,
    "collapsible": "one",
    "children": [
      {
        "label": "Database Connection Error",
        "field": "database_connection_error",
        "description": "Represents the operation when database connection error. options: Archive, Skip, Cache. Default: Cache.",
        "defaultValue": "cache",
        "required": false,
        "type": "select",
        "options": [
          {
            "value": "cache",
            "label": "Cache"
          },
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          }
        ]
      },
      {
        "label": "Database Not Exist",
        "field": "database_not_exist",
        "description": "Represents the operation when database not exists. options: Archive, Skip, Break. Default: Break.",
        "defaultValue": "break",
        "required": false,
        "type": "select",
        "options": [
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          }
        ]
      },
      {
        "label": "Table Not Exist",
        "field": "table_not_exist",
        "description": "Represents the operation when table not exists. options: Archive, Skip, Break, Automatically create table & retry. Default: Automatically create table and retry.",
        "defaultValue": "retry",
        "required": false,
        "type": "select",
        "options": [
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          },
          {
            "value": "retry",
            "label": "Automatically create table and retry"
          }
        ]
      },
      {
        "label": "Primary Timestamp Overflow",
        "field": "primary_timestamp_overflow",
        "description":
          "Represents the operation when a timestamp overflow occurs. options: Archive, Skip, Break. Default: Archive.",
        "defaultValue": "archive",
        "required": false,
        "hint": {
          "type": "select",
          "choices": [
            {
              "value": "archive",
              "label": "Archive"
            },
            {
              "value": "skip",
              "label": "Skip"
            },
            {
              "value": "break",
              "label": "Break"
            }
          ]
        },
        "disabledValues": [],
        "type": "select",
        "options": [
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          }
        ]
      },
      {
        "label": "Primary Timestamp Null",
        "field": "primary_timestamp_null",
        "description":
          "Represents the operation when a timestamp is null. options: Use Current Time, Archive, Skip, Break. Default: Archive.",
        "defaultValue": "archive",
        "required": false,
        "disabledValues": [],
        "type": "select",
        "options": [
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          },
          {
            "value": "use_current_time",
            "label": "Use Current Time"
          }
        ]
      },
      {
        "field": "primary_key_null",
        "label": "Primary Key Null",
        "type": "select",
        "choices": [
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          }
        ],
        "description":
          "Represents the operation when a composite primary key column is null. options: Archive, Skip, Break. Default: Archive.",
        "defaultValue": "archive"
      },
      {
        "label": "Table Name Length Overflow",
        "field": "table_name_length_overflow",
        "description":
          "Represents the operation when a table name length overflows. Currently supports Archive, Skip, Truncate, Truncate and Archive, and Break. Default: Archive.",
        "defaultValue": "archive",
        "required": false,
        "hint": {
          "type": "select",
          "choices": [
            {
              "value": "archive",
              "label": "Archive"
            },
            {
              "value": "skip",
              "label": "Skip"
            },
            {
              "value": "break",
              "label": "Break"
            },
            {
              "value": "truncate",
              "label": "Truncate"
            },
            {
              "value": "truncate_and_archive",
              "label": "Truncate and Archive"
            }
          ]
        },
        "disabledValues": [],
        "type": "select",
        "options": [
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          },
          {
            "value": "truncate",
            "label": "Truncate"
          },
          {
            "value": "truncate_and_archive",
            "label": "Truncate and Archive"
          }
        ]
      },
      {
        "label": "Table Name Contains Illegal Char",
        "field": "table_name_contains_illegal_char",
        "description":
          "Represents the strategy when a table name contains illegal characters (e.g., .). options: Replace with a specified character or string, Skip, Archive, Break. Default: Replace with '_'.",
        "defaultValue": "",
        "required": false,
        "hint": {
          "type": "compose",
          "choices": [
            {
              "value": "archive",
              "label": "Archive"
            },
            {
              "value": "skip",
              "label": "Skip"
            },
            {
              "value": "break",
              "label": "Break"
            },
            {
              "value": "replace_to",
              "label": "Replace Illegal Character with Specified String"
            }
          ]
        },
        "unit_value": "replace_to",
        "disabledValues": ["archive", "skip", "break"],
        "type": "compose",
        "options": [
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          },
          {
            "value": "replace_to",
            "label": "Replace Illegal Character with Specified String"
          }
        ]
      },
      {
        "label": "Variable Not Exist in Table Name Template",
        "field": "variable_not_exist_in_table_name_template",
        "description":
          "Represents the strategy when a variable in the table name template is empty. options: Replace with a specified string, Leave blank, Skip the entire row. Default: Replace with NULL.",
        "defaultValue": "",
        "required": false,
        "unit_value": "replace_to",
        "disabledValues": ["leave_blank", "skip"],
        "type": "compose",
        "options": [
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "leave_blank",
            "label": "Leave Blank"
          },
          {
            "value": "replace_to",
            "label": "Replace Variable with Specified String"
          }
        ]
      },
      {
        "field": "field_name_not_found",
        "label": "field Name Not Found",
        "type": "select",
        "options": [
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          },
          {
            "value": "add_field",
            "label": "Automatically Add Missing field"
          }
        ],
        "description":
          "Represents the action when a field name is not found. options: Use current time, Archive, Skip, Break, Automatically add missing field. Default: Archive.",
        "defaultValue": "add_field"
      },
      {
        "label": "field Name Length Overflow",
        "field": "field_name_length_overflow",
        "description":
          "Represents the action when a field name length overflows. options: Use current time, Archive, Skip, Break, Truncate, Truncate and Archive. Default: Archive.",
        "defaultValue": "archive",
        "required": false,
        "hint": {
          "type": "select",
          "choices": [
            {
              "value": "archive",
              "label": "Archive"
            },
            {
              "value": "skip",
              "label": "Skip"
            },
            {
              "value": "break",
              "label": "Break"
            }
          ]
        },
        "disabledValues": [],
        "type": "select",
        "options": [
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          }
        ]
      },
      {
        "field": "field_length_extend",
        "label": "field Length Extend",
        "type": "switch",
        "defaultValue": true,
        "description":
          "When enabled, VARCHAR/VARBINARY/NCHAR columns are automatically resized to the allowable length for storage. Default: true.",
        "value": true
      },
      {
        "field": "field_length_overflow",
        "label": "field Length Overflow",
        "type": "select",
        "options": [
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          },
          {
            "value": "truncate",
            "label": "Truncate"
          },
          {
            "value": "truncate_and_archive",
            "label": "Truncate and Archive"
          }
        ],
        "description":
          "Represents actions for column length overflow. options: Archive, Skip, Break, Truncate, Truncate and Archive. Default: Archive.",
        "defaultValue": "archive"
      },
      {
        "field": "ingesting_error",
        "label": "Ingesting Error",
        "type": "select",
        "options": [
          {
            "value": "archive",
            "label": "Archive"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Break"
          }
        ],
        "description":
          "Actions for data failure when data cannot be ingested into the database. Currently supports Archive, Skip, and Break. Default: Archive.",
        "defaultValue": "archive"
      },
      {
        "field": "connection_timeout_in_second",
        "label": "Connection Timeout",
        "type": "composeAppend",
        "options": [
          {
            "value": "s",
            "label": "Seconds"
          }
        ],
        "min": 1,
        "max": 600,
        "description": "Target database connection timeout, default is 30 seconds.",
        "required": false,
        "placeholder": "Enter an integer between [1,600]",
        "defaultValue": "30s"
      },
      {
        "field": "cache.keep_days",
        "label": "Cache Keep Days",
        "type": "composeAppend",
        "options": [
          {
            "value": "d",
            "label": "Days"
          }
        ],
        "min": 0,
        "max": 65535,
        "description": "When the above operation is set to Cache, this configures the maximum duration for which cache files are retained. Default is 30 days. Configuring 0 means using the default value.",
        "required": false,
        "placeholder": "Enter an integer between [0,65535]",
        "defaultValue": "30d"
      },
      {
        "field": "cache.max_size",
        "label": "Cache Max Size",
        "type": "composeAppend",
        "options": [
          {
            "value": "MB",
            "label": "MB"
          },
          {
            "value": "GB",
            "label": "GB"
          }
        ],
        "min": 0,
        "max": 65535,
        "description":
          "Maximum size for each cache file. Default is 1GB, max is 65535GB, setting to 0 means using the default value. Default path: $DATA_DIR/tasks/:id/cache",
        "required": false,
        "placeholder": "Enter an integer in the range [0, 65535]",
        "defaultValue": "1GB"
      },
      {
        "field": "cache.rotate_count",
        "label": "Cache File Count",
        "type": "number",
        "min": 0,
        "max": 65535,
        "description": "Number of cache storage files. Default is 100. Setting to 0 means using the default value.",
        "required": false,
        "defaultValue": 100
      },
      {
        "field": "cache.location",
        "label": "Cache Location",
        "type": "input",
        "description": "Indicates the location of the temporary storage file. Default: $DATA_DIR/tasks/:id/cache",
        "value": "cache",
        "placeholder": "$DATA_DIR/tasks/:id/cache"
      },
      {
        "field": "cache.on_fail",
        "label": "Cache On Fail",
        "type": "select",
        "options": [
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Report Break and Stop Task"
          }
        ],
        "description":
          "Defines the handling strategy for temporary storage failure. Options include Discard or Report Break and Stop Task. Default is Discard.",
        "defaultValue": "skip"
      },
      {
        "field": "archive.keep_days",
        "label": "Archive Keep Days",
        "type": "composeAppend",
        "options": [
          {
            "value": "d",
            "label": "Days"
          }
        ],
        "min": 0,
        "max": 65535,
        "description":
          "When the above operation is set to Archive, this configures the maximum retention period for archived files. Default is 30 days. Setting it to 0 means no limit.",
        "required": false,
        "placeholder": "Enter a non-negative integer, 0 means unlimited",
        "defaultValue": "30d"
      },
      {
        "field": "archive.max_size",
        "label": "Archive max file size",
        "type": "composeAppend",
        "options": [
          {
            "value": "MB",
            "label": "MB"
          },
          {
            "value": "GB",
            "label": "GB"
          }
        ],
        "min": 0,
        "max": 65535,
        "description":
          "Archive max file size, default is 1G, max is 65535G, setting to 0 means using the default value. Default path: $DATA_DIR/tasks/:id/archived",
        "required": false,
        "placeholder": "Enter an integer in the range [0, 65535]",
        "defaultValue": "1GB"
      },
      {
        "field": "archive.rotate_count",
        "label": "Archive Rotate Count",
        "type": "number",
        "min": 0,
        "max": 65535,
        "description": "Number of archived files to keep. Default is 100. Setting to 0 means using the default value.",
        "required": false,
        "defaultValue": 100
      },
      {
        "field": "archive.location",
        "label": "Archive Location",
        "type": "input",
        "description": "Specifies the location for archived data files. Default is $DATA_DIR/tasks/:id/archived",
        "value": "archived",
        "placeholder": "$DATA_DIR/tasks/:id/archived"
      },
      {
        "field": "archive.on_fail",
        "label": "Archive On Fail",
        "type": "select",
        "options": [
          {
            "value": "rotate",
            "label": "Delete Old Files"
          },
          {
            "value": "skip",
            "label": "Skip"
          },
          {
            "value": "break",
            "label": "Report Break and Stop Task"
          }
        ],
        "description": "Delete old files, discard, or report break and stop the task.",
        "defaultValue": "rotate"
      }
    ]
  }`
};

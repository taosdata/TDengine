
const zh = {
  name: "写入配置",
  description: "对写入策略配置参数进行调整，可修改以下选项。\n",
  collapsible: true,
  connection_option: false,
  params: [
    {
      name: "primary_timestamp_overflow",
      display: "主键时间戳溢出",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "归档",
          },
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "break",
            label: "报错",
          },
        ],
      },
      description: "表示时间戳溢出时的操作，可选：归档、丢弃、报错。默认：归档。\n",
      value: "archive",
    },
    {
      name: "primary_timestamp_null",
      display: "主键时间戳空",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "归档",
          },
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "break",
            label: "报错",
          },
          {
            value: "use_current_time",
            label: "使用当前时间",
          },
        ],
      },
      description: "表示时间戳为空时的操作，可选：使用当前时间、归档、丢弃、报错。默认：归档。\n",
      value: "archive",
    },
    {
      name: "primary_key_null",
      display: "复合主键空",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "归档",
          },
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "break",
            label: "报错",
          },
        ],
      },
      description: "表示复合主键列为空时的操作，可选：归档、丢弃、报错。默认：归档。\n",
      value: "archive",
    },
    {
      name: "table_name_length_overflow",
      display: "表名长度溢出",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "归档",
          },
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "break",
            label: "报错",
          },
          {
            value: "truncate",
            label: "截断",
          },
          {
            value: "truncate_and_archive",
            label: "截断且归档",
          },
        ],
      },
      description: "表示当表名长度溢出时的操作，当前支持 归档、丢弃、截断、截断及归档、报错。默认：归档。\n",
      value: "archive",
    },
    {
      name: "table_name_contains_illegal_char",
      display: "表名非法字符",
      hint: {
        type: "compose",
        choices: [
          {
            value: "archive",
            label: "归档",
          },
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "break",
            label: "报错",
          },
          {
            value: "replace_to",
            label: "非法字符替换为指定字符串",
          },
        ],
      },
      description: "表示当表名包含非法字符时（如 . ）的处置策略，可选：替换为指定字符或字符串、丢弃、归档、报错。默认：替换为 _。\n",
      value: "",
      type_value: "replace_to",
      disabledValues: ['archive', 'skip', 'break']
    },
    {
      name: "variable_not_exist_in_table_name_template",
      display: "表名模板变量空值",
      hint: {
        type: "compose",
        choices: [
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "leave_blank",
            label: "留空",
          },
          {
            value: "replace_to",
            label: "变量替换为指定字符串",
          },
        ],
      },
      description: "表示当表名模板中变量为空时的处置策略，可选：替换为指定字符串、留空、丢弃整行。 默认：替换为 NULL。\n",
      value: "",
      type_value: "replace_to",
      disabledValues: ['leave_blank', 'skip']
    },
    {
      name: "field_name_not_found",
      display: "列名不存在",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "归档",
          },
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "break",
            label: "报错",
          },
          {
            value: "add_field",
            label: "自动增加缺失列",
          },
        ],
      },
      description: "表示列名不存在的操作，可选：使用当前时间、归档、丢弃、报错、自动增加缺失列。默认：归档。\n\n",
      value: "add_field",
    },
    {
      name: "field_name_length_overflow",
      display: "列名长度溢出",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "归档",
          },
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "break",
            label: "报错",
          },
          {
            value: "truncate",
            label: "截断",
          },
          {
            value: "truncate_and_archive",
            label: "截断且归档",
          },
        ],
      },
      description: "表示列名长度溢出的操作，可选：使用当前时间、归档、丢弃、报错、截断、截断且归档。默认：归档。\n",
      value: "archive",
    },
    {
      name: "field_length_extend",
      display: "列自动扩容",
      hint: {
        type: "bool",
      },
      description: "启用时，VARCHAR/VARBINARY/NCHAR 列自动扩容到可入库的长度。默认为 true 。\n",
      value: "true",
    },
    {
      name: "field_length_overflow",
      display: "列长度溢出",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "归档",
          },
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "break",
            label: "报错",
          },
          {
            value: "truncate",
            label: "截断",
          },
          {
            value: "truncate_and_archive",
            label: "截断且归档",
          },
        ],
      },
      description: "表示列长度溢出的操作，可选：归档、丢弃、报错、截断、截断且归档。默认：归档。\n",
      value: "archive",
    },
    {
      name: "ingesting_error",
      display: "数据异常",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "归档",
          },
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "break",
            label: "报错",
          },
        ],
      },
      description: "因数据本身无法入库导致失败时的数据行为，当前支持 归档 、丢弃、报错 三种。默认：归档。\n",
      value: "archive",
    },
    {
      name: "connection_timeout_in_second",
      display: "连接超时",
      hint: {
        type: "timeout",
        choices: [
          {
            value: "s",
            label: "秒",
          },
        ],
        min: 1,
        max: 600,
      },
      description: "目标数据库连接超时，默认为 30s。\n",
      required: false,
      placeholder: "输入范围为[1,600]整数",
      value: '30',
      type_value: "s",
    },
    {
      name: "cache.max_size",
      display: "临时存储可用空间",
      hint: {
        type: "timeout",
        choices: [
          {
            value: "GB",
            label: "GB",
          },
        ],
        min: 0,
        max: 65535,
      },
      description: "启用时，需配置允许占用的磁盘空间，最小为 1G，最大为 65535 G，配置为 0 表示无限制。默认无限制。默认路径是 ： $DATA_DIR/tasks/:id/cache\n",
      required: false,
      placeholder: "输入范围为[1,65535]整数",
      value: '0',
      type_value: "GB",
    },
    {
      name: "cache.location",
      display: "临时存储文件位置",
      hint: {
        type: "str",
      },
      description: "表示临时存储文件位置，默认 $DATA_DIR/tasks/:id/cache \n",
      value: "cache",
    },
    {
      name: "cache.on_fail",
      display: "临时存储失败处理策略",
      hint: {
        type: "select",
        choices: [
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "break",
            label: "报错并停止任务",
          },
        ],
      },
      description: "表示临时存储失败处理策略的操作，可选有丢弃、报错并停止任务，默认：丢弃。\n",
      value: "skip",
    },
    {
      name: "archive.keep_days",
      display: "归档数据保留天数",
      hint: {
        type: "timeout",
        choices: [
          {
            value: "d",
            label: "天",
          },
        ],
        min: 0,
        max: 65535,
      },
      description: "配置以上操作配置为 归档 时，归档文件的最大保留时长。默认 30 天。配置为 0 表示无限制。\n",
      required: false,
      placeholder: "输入非负整数，0 表示无限制",
      value: '30',
      type_value: "d",
    },
    {
      name: "archive.max_size",
      display: "归档数据可用空间",
      hint: {
        type: "timeout",
        choices: [
          {
            value: "GB",
            label: "GB",
          },
        ],
        min: 0,
        max: 65535,
      },
      description: "归档文件的最大可用磁盘空间，最小为 1G，最大为 65535G，配置为 0 表示无限制。默认无限制。默认路径：$DATA_DIR/tasks/:id/archived\n",
      required: false,
      placeholder: "输入范围为[1,65535]整数",
      value: '0',
      type_value: "GB",
    },
    {
      name: "archive.location",
      display: "归档数据文件位置",
      hint: {
        type: "str",
      },
      description: "表示归档数据文件位置，默认：$DATA_DIR/tasks/:id/archived\n",
      value: "archived",
    },
    {
      name: "archive.on_fail",
      display: "归档数据失败处理策略",
      hint: {
        type: "select",
        choices: [
          {
            value: "rotate",
            label: "删除旧文件",
          },
          {
            value: "skip",
            label: "丢弃",
          },
          {
            value: "break",
            label: "报错并停止任务",
          },
        ],
      },
      description: "删除旧文件、报错或丢弃。\n",
      value: "rotate",
    },
  ],
}

const en = {
  name: "Write Configuration",
  description: "Adjust the configuration parameters for the write strategy. The following options can be modified.\n",
  collapsible: true,
  connection_option: false,
  params: [
    {
      name: "primary_timestamp_overflow",
      display: "Primary Timestamp Overflow",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "Archive",
          },
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "break",
            label: "Break",
          },
        ],
      },
      description: "Represents the operation when a timestamp overflow occurs. Options: Archive, Skip, Break. Default: Archive.\n",
      value: "archive",
    },
    {
      name: "primary_timestamp_null",
      display: "Primary Timestamp Null",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "Archive",
          },
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "break",
            label: "Break",
          },
          {
            value: "use_current_time",
            label: "Use Current Time",
          },
        ],
      },
      description: "Represents the operation when a timestamp is null. Options: Use Current Time, Archive, Skip, Break. Default: Archive.\n",
      value: "archive",
    },
    {
      name: "primary_key_null",
      display: "Primary Key Null",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "Archive",
          },
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "break",
            label: "Break",
          },
        ],
      },
      description: "Represents the operation when a composite primary key column is null. Options: Archive, Skip, Break. Default: Archive.\n",
      value: "archive",
    },
    {
      name: "table_name_length_overflow",
      display: "Table Name Length Overflow",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "Archive",
          },
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "break",
            label: "Break",
          },
          {
            value: "truncate",
            label: "Truncate",
          },
          {
            value: "truncate_and_archive",
            label: "Truncate and Archive",
          },
        ],
      },
      description: "Represents the operation when a table name length overflows. Currently supports Archive, Skip, Truncate, Truncate and Archive, and Break. Default: Archive.\n",
      value: "archive",
    },
    {
      name: "table_name_contains_illegal_char",
      display: "Table Name Contains Illegal Char",
      hint: {
        type: "compose",
        choices: [
          {
            value: "archive",
            label: "Archive",
          },
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "break",
            label: "Break",
          },
          {
            value: "replace_to",
            label: "Replace Illegal Character with Specified String",
          },
        ],
      },
      description: "Represents the strategy when a table name contains illegal characters (e.g., .). Options: Replace with a specified character or string, Skip, Archive, Break. Default: Replace with '_'.\n",
      value: "",
      type_value: "replace_to",
      disabledValues: ['archive', 'skip', 'break']
    },
    {
      name: "variable_not_exist_in_table_name_template",
      display: "Variable Not Exist in Table Name Template",
      hint: {
        type: "compose",
        choices: [
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "leave_blank",
            label: "Leave Blank",
          },
          {
            value: "replace_to",
            label: "Replace Variable with Specified String",
          },
        ],
      },
      description: "Represents the strategy when a variable in the table name template is empty. Options: Replace with a specified string, Leave blank, Skip the entire row. Default: Replace with NULL.\n",
      value: "",
      type_value: "replace_to",
      disabledValues: ['leave_blank', 'skip']
    },
    {
      name: "field_name_not_found",
      display: "Field Name Not Found",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "Archive",
          },
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "break",
            label: "Break",
          },
          {
            value: "add_field",
            label: "Automatically Add Missing Field",
          },
        ],
      },
      description: "Represents the action when a field name is not found. Options: Use current time, Archive, Skip, Break, Automatically add missing field. Default: Archive.",
      value: "add_field",
    },
    {
      name: "field_name_length_overflow",
      display: "Field Name Length Overflow",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "Archive",
          },
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "break",
            label: "Break",
          },
          {
            value: "truncate",
            label: "Truncate",
          },
          {
            value: "truncate_and_archive",
            label: "Truncate and Archive",
          },
        ],
      },
      description: "Represents the action when a field name length overflows. Options: Use current time, Archive, Skip, Break, Truncate, Truncate and Archive. Default: Archive.",
      value: "archive",
    },    
    {
      name: "field_length_extend",
      display: "Field Length Extend",
      hint: {
        type: "bool",
      },
      description: "When enabled, VARCHAR/VARBINARY/NCHAR columns are automatically resized to the allowable length for storage. Default: true.",
      value: "true",
    },
    {
      name: "field_length_overflow",
      display: "Field Length Overflow",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "Archive",
          },
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "break",
            label: "Break",
          },
          {
            value: "truncate",
            label: "Truncate",
          },
          {
            value: "truncate_and_archive",
            label: "Truncate and Archive",
          },
        ],
      },
      description: "Represents actions for column length overflow. Options: Archive, Skip, Break, Truncate, Truncate and Archive. Default: Archive.",
      value: "archive",
    },
    {
      name: "ingesting_error",
      display: "Ingesting Error",
      hint: {
        type: "select",
        choices: [
          {
            value: "archive",
            label: "Archive",
          },
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "break",
            label: "Break",
          },
        ],
      },
      description: "Actions for data failure when data cannot be ingested into the database. Currently supports Archive, Skip, and Break. Default: Archive.",
      value: "archive",
    },
    {
      name: "connection_timeout_in_second",
      display: "Connection Timeout",
      hint: {
        type: "timeout",
        choices: [
          {
            value: "s",
            label: "Seconds",
          },
        ],
        min: 1,
        max: 600,
      },
      description: "Target database connection timeout, default is 30 seconds.",
      required: false,
      placeholder: "Enter an integer between [1,600]",
      value: '30',
      type_value: "s",
    },    
    {
      name: "cache.max_size",
      display: "Cache Max Size ",
      hint: {
        type: "timeout",
        choices: [
          {
            value: "GB",
            label: "GB"
          }
        ],
        min: 0,
        max: 65535
      },
      description: "When enabled, configure the allowable disk space to be used. The minimum is 1GB, the maximum is 65535GB, and a value of 0 means no limit. Default is unlimited. Default path: $DATA_DIR/tasks/:id/cache\n",
      required: false,
      placeholder: "Enter an integer in the range [1, 65535]",
      value: '0',
      type_value: "GB"
    },
    {
      name: "cache.location",
      display: "Cache Location",
      hint: {
        type: "str"
      },
      description: "Indicates the location of the temporary storage file. Default: $DATA_DIR/tasks/:id/cache\n",
      value: "cache"
    },
    {
      name: "cache.on_fail",
      display: "Cache On Fail",
      hint: {
        type: "select",
        choices: [
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "break",
            label: "Report Break and Stop Task",
          },
        ],
      },
      description: "Defines the handling strategy for temporary storage failure. Options include Discard or Report Break and Stop Task. Default is Discard.\n",
      value: "skip",
    },
    {
      name: "archive.keep_days",
      display: "Archive Keep Days",
      hint: {
        type: "timeout",
        choices: [
          {
            value: "d",
            label: "Days",
          },
        ],
        min: 0,
        max: 65535,
      },
      description: "When the above operation is set to Archive, this configures the maximum retention period for archived files. Default is 30 days. Setting it to 0 means no limit.\n",
      required: false,
      placeholder: "Enter a non-negative integer, 0 means unlimited",
      value: '30',
      type_value: "d",
    },
    {
      name: "archive.max_size",
      display: "Archive Max Size",
      hint: {
        type: "timeout",
        choices: [
          {
            value: "GB",
            label: "GB",
          },
        ],
        min: 0,
        max: 65535,
      },
      description: "Maximum available disk space for archived files. Minimum is 1GB, maximum is 65535GB. Setting it to 0 means no limit. Default is unlimited. Default path: $DATA_DIR/tasks/:id/archived\n",
      required: false,
      placeholder: "Enter an integer in the range [1, 65535]",
      value: '0',
      type_value: "GB",
    },
    {
      name: "archive.location",
      display: "Archive Location",
      hint: {
        type: "str",
      },
      description: "Specifies the location for archived data files. Default is $DATA_DIR/tasks/:id/archived\n",
      value: "archived",
    },
    {
      name: "archive.on_fail",
      display: "Archive On Fail",
      hint: {
        type: "select",
        choices: [
          {
            value: "rotate",
            label: "Delete Old Files",
          },
          {
            value: "skip",
            label: "Skip",
          },
          {
            value: "break",
            label: "Report Break and Stop Task",
          },
        ],
      },
      description: "Delete old files, discard, or report break and stop the task.\n",
      value: "rotate",
    },
  ],
}

const config = {
  zh,
  en
};
export default config
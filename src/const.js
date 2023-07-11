import mitt from "mitt";
export const BaseRoute = [
  "/instances",
  "/billing",
  "/alert",
  "/activity",
  "/profile",
  "/support",
  "/instanceStatus",
  "/user",
  "/landing",
  "/calculator"
];
// 不需要切换集群的路由
export const NoInstanceSelectRoute = ["/billing", "/alert", "/activity", "/profile", "/support", "/user"];

export const AllClusterStatus = [
  "Ready", //就绪
  "Starting", //启动中
  "Running", //运行中
  "Failed", //失败
  "Error", //错误
  "Expanding", //扩容中
  "Shrinking", //缩容中
  "Upgrading", //升级中
  "Suspending", //暂停中
  "Suspended", //已中止
  "Stopping", //停止中
  "Deleted", //已删除
  "Warning", //异常
];
export const InitClusterStatus = ["Ready", "Starting"];
export const NoOperateStatus = [
  "Starting",
  "Failed", //失败
  "Error", //错误
  "Expanding", //扩容中
  "Shrinking", //缩容中
  "Upgrading", //升级中
  "Suspending", //暂停中
  "Stopping", //停止中
  "Deleted", //已删除
  "Warning", //异常
];
export const NeedRefreshStatus = [
  "Suspending",
  "Starting",
  "Ready",
  "Stopping",
  "Expanding", //扩容中
  "Shrinking", //缩容中
  "Upgrading", //升级中
];
export const InactiveStatus = ["Ready", "Suspending", "Suspended", "Error", "Deleted", "Failed", "Warning", "Stopping"];
export const PermissionMap = {
  1: 1,
  2: 0,
};
export const RedirectKey = "TDengine-Redirect";
export const ReLoginCode = ["502", "401", "432"]; //重新登录的状态码
export const SuccessCode = ["200", "302"]; //请求成功的状态码
export const ServerLevel = {
  FREE: 0,
  STANDARD: 1,
  ENTERPRISE: 2,
  0: "FREE",
  1: "STANDARD",
  2: "ENTERPRISE",
};
export const OFFSETUTCTIME = new Date().getTimezoneOffset() * 60 * 1000;

export const VariableTableColumnType = ["BINARY", "NCHAR", "VARCHAR"];
export const HIDEDB = ["information_schema", "performance_schema"];
export const DBFILED = {
  buffer: { type: "number", alter: false, defaultValue: undefined },
  cachemodel: { type: "string", alter: true, defaultValue: "none" },
  cachesize: { type: "number", alter: true, defaultValue: undefined },
  comp: { type: "number", alter: false, defaultValue: undefined },
  duration: { type: "number", alter: false, defaultValue: "50d" },
  wal_fsync_period: { type: "number", alter: true, defaultValue: undefined },
  maxrows: { type: "number", alter: false, defaultValue: undefined },
  minrows: { type: "number", alter: false, defaultValue: undefined },
  keep: { type: "number", alter: true, defaultValue: "3650d" },
  pages: { type: "number", alter: false, defaultValue: undefined},
  pagesize: { type: "number", alter: false, defaultValue: undefined },
  replica: { type: "number", alter: false, defaultValue: undefined },
  retentions: { type: "number", alter: false, defaultValue: undefined },
  strict: { type: "string", alter: false, defaultValue: undefined },
  wal_level: { type: "number", alter: true, defaultValue: undefined },
  vgroups: { type: "number", alter: false, defaultValue: undefined },
  single_stable: { type: "number", alter: false, defaultValue: undefined },
  wal_retention_period: { type: "number", alter: false, defaultValue: 0 }, //
  wal_retention_size: { type: "number", alter: false, defaultValue: undefined },
  wal_roll_period: { type: "number", alter: false, defaultValue: undefined },
  wal_segment_size: { type: "number", alter: false, defaultValue: undefined },
  precision: { type: "string", alter: false, defaultValue: "ms" },
  stt_trigger: { type: 'number', alter: false, defaultValue: undefined },
  tsdb_pagesize: { type: 'number', alter: false, defaultValue: undefined },
  table_prefix: { type: 'number', alter: false, defaultValue: undefined },
  table_suffix: { type: 'number', alter: false, defaultValue: undefined},  
};
export const TDengineSqlKeywrods = [
  "ABORT",
  "ABS",
  "ACCOUNT",
  "ACCOUNTS",
  "ACOS",
  "ADD",
  "AFTER",
  "ALL",
  "ALTER",
  "AND",
  "APERCENTILE",
  "AS",
  "ASC",
  "ASIN",
  "ATAN",
  "ATTACH",
  "AVG",
  "BEFORE",
  "BEGIN",
  "BETWEEN",
  "BIGINT",
  "BINARY",
  "BITAND",
  "BITNOT",
  "BITOR",
  "BLOCKS",
  "BOOL",
  "BOTTOM",
  "BY",
  "CACHE",
  "CACHELAST",
  "CASCADE",
  "CAST",
  "CEIL",
  "CHANGE",
  "CHAR_LENGTH",
  "CLIENT_VERSION",
  "CLUSTER",
  "COLON",
  "COLUMN",
  "COMMA",
  "COMP",
  "COMPACT",
  "CONCAT",
  "CONCAT_WS",
  "CONFLICT",
  "CONNECTION",
  "CONNECTIONS",
  "CONNS",
  "COPY",
  "COS",
  "COUNT",
  "CREATE",
  "CSUM",
  "CTIME",
  "CURRENT_USER",
  "DATABASE",
  "DATABASES",
  "DAYS",
  "DBS",
  "DEFERRED",
  "DELETE",
  "DELIMITERS",
  "DERIVATIVE",
  "DESC",
  "DESCRIBE",
  "DETACH",
  "DIFF",
  "DISTINCT",
  "DIVIDE",
  "DNODE",
  "DNODES",
  "DOT",
  "DOUBLE",
  "DROP",
  "ELAPSED",
  "END",
  "EQ",
  "EXISTS",
  "EXPLAIN",
  "FAIL",
  "FILE",
  "FILL",
  "FIRST",
  "FLOAT",
  "FLOOR",
  "FOR",
  "FROM",
  "FSYNC",
  "GE",
  "GLOB",
  "GRANTS",
  "GROUP",
  "GT",
  "HAVING",
  "HISTOGRAM",
  "HYPERLOGLOG",
  "ID",
  "IF",
  "IGNORE",
  "IMMEDIA",
  "IMPORT",
  "IN",
  "INITIAL",
  "INSERT",
  "INSTEAD",
  "INT",
  "INTEGER",
  "INTERP",
  "INTERVA",
  "INTO",
  "IRATE",
  "IS",
  "ISNULL",
  "JOIN",
  "KEEP",
  "KEY",
  "KILL",
  "LAST",
  "LAST_ROW",
  "LE",
  "LEASTSQUARES",
  "LENGTH",
  "LIKE",
  "LIMIT",
  "LINEAR",
  "LOCAL",
  "LOG",
  "LOWER",
  "LP",
  "LSHIFT",
  "LT",
  "LTRIM",
  "MATCH",
  "MAVG",
  "MAX",
  "MAXROWS",
  "MIN",
  "MINROWS",
  "MINUS",
  "MNODES",
  "MODE",
  "MODIFY",
  "MODULES",
  "NE",
  "NONE",
  "NOT",
  "NOTNULL",
  "NOW",
  "NULL",
  "OF",
  "OFFSET",
  "OR",
  "ORDER",
  "PARTITION",
  "PASS",
  "PERCENTILE",
  "PLUS",
  "POW",
  "PPS",
  "PRECISION",
  "PREV",
  "PRIVILEGE",
  "QTIME",
  "QUERIE",
  "QUERY",
  "QUORUM",
  "RAISE",
  "REM",
  "REPLACE",
  "REPLICA",
  "RESET",
  "RESTRIC",
  "ROUND",
  "ROW",
  "RP",
  "RSHIFT",
  "RTLIM",
  "SAMPLE",
  "SCORES",
  "SELECT",
  "SEMI",
  "SERVER_STATUS",
  "SERVER_VERSION",
  "SESSION",
  "SET",
  "SHOW",
  "SIN",
  "SLASH",
  "SLIDING",
  "SLIMIT",
  "SMALLIN",
  "SOFFSET",
  "SPREAD",
  "SQRT",
  "STAR",
  "STATE",
  "STATECOUNT",
  "STATEDURATION",
  "STATEMEN",
  "STATE_WI",
  "STDDEV",
  "STORAGE",
  "STREAM",
  "STREAMS",
  "STRING",
  "STable",
  "STableS",
  "SUBSTR",
  "SUM",
  "SYNCDB",
  "TABLE",
  "TABLES",
  "TAG",
  "TAGS",
  "TAIL",
  "TAN",
  "TBNAME",
  "TIMEDIFF",
  "TIMES",
  "TIMESTAMP",
  "TIMETRUNCATE",
  "TIMEZONE",
  "TINYINT",
  "TODAY",
  "TOP",
  "TOPIC",
  "TOPICS",
  "TO_ISO0861",
  "TO_JSON",
  "TO_UNIXTIMESTAMP",
  "TRIGGER",
  "TSERIES",
  "TWA",
  "UMINUS",
  "UNION",
  "UNIQUE",
  "UNSIGNED",
  "UPDATE",
  "UPLUS",
  "UPPER",
  "USE",
  "USER",
  "USERS",
  "USING",
  "VALUES",
  "VARIABLE",
  "VARIABLES",
  "VGROUPS",
  "VIEW",
  "VNODES",
  "WAL",
  "WHERE",
  "_C0",
  "_QDURATION",
  "_QSTART",
  "_QSTOP",
  "_WDURATION",
  "_WSTART",
  "_WSTOP",
];
export const TokenExpire = 1 / 24; //day
export const AppIDKey = "AppID";
export const TokenKey = "TDengine-Token";
export const BusinessEmail = "business@tdengine.com";

export const StreamDocsUrl = navigator.language.includes('en') ? "https://docs.tdengine.com/develop/stream/" : 'https://docs.taosdata.com/develop/stream/';
export const DocsUrl = "https://docs.tdengine.com"

export const TdengineVersion = "3.0.3.2";
export const SubscriptionDocsUrl = navigator.language.includes('en') ? "https://docs.tdengine.com/taos-sql/tmq/#create-a-topic" : 'https://docs.taosdata.com/taos-sql/tmq/#create-a-topic';
export const TDengineTimeUnit = [
  {
    label: "nanosecond",
    value: "b",
  },
  {
    label: "microsecond",
    value: "u",
  },
  {
    label: "millsecond",
    value: "a",
  },
  {
    label: "second",
    value: "s",
  },
  {
    label: "minute",
    value: "m",
  },
  {
    label: "hour",
    value: "h",
  },
  {
    label: "day",
    value: "d",
  },
  {
    label: "week",
    value: "w",
  },
  {
    label: "month",
    value: "n",
  },
  {
    label: "year",
    value: "y",
  },
];
export const RequestCommonConfig = {
  // timeout: 20000,
  withCredentials: false,
};

export const SlowSqlTime = "200ms";

export const ReplicationTaskStatus = ["created", "failed", "cancelled", "deleted", "completed", "interrupted", "stopped", "running"];
export const ReplicationTaskCanStopStatus = ["interrupted", "running", "created"];

export const ReplicationTaskCanStartStatus = ["stopped"];

export const $bus = mitt();

export const CustomShellContent = ["Welcome to TDengine "];

export const IntegerType=['int','int unsinged','bigint','bigint unsinged','float','double','smallint',
'smallint unsigned','tinyint','tinyint unsinged']
export const StringType=['varchar','nchar','binary']
// 数学函数
export const NumbericFn = [
  {
    label: "ABS",
    supportDatatype:IntegerType
  },
  {
    label: "ACOS",
    supportDatatype:IntegerType
  },
  {
    label: "ASIN",
    supportDatatype:IntegerType
  },
  {
    label: "ATAN",
    supportDatatype:IntegerType
  },
  {
    label: "CEIL",
    supportDatatype:IntegerType
  },
  {
    label: "COS",
    supportDatatype:IntegerType
  },
  {
    label: "FLOOR",
    supportDatatype:IntegerType
  },
  {
    label: "LOG",
    supportDatatype:IntegerType,
    filters: [
      {
        type: "select",
        label: "Log Filed",
        options() {
          return this.fieldList.filter(item => item.field != this.field);
        },
        placeholder: "Select Filed",
        field: "logFiled",
        defaultValue: "",
      },
    ],
  },
  {
    label: "POW",
    supportDatatype:IntegerType,
    filters: [
      {
        type: "select",
        label: "Pow Filed",
        options() {
          return this.fieldList.filter(item => item.field != this.field);
        },
        placeholder: "Select Filed",
        field: "powFiled",
        defaultValue: "",
      },
    ],
  },
  {
    label: "ROUND",
    supportDatatype:IntegerType
  },
  {
    label: "SIN",
    supportDatatype:IntegerType
  },
  {
    label: "SQRT",
    supportDatatype:IntegerType
  },
  {
    label: "TAN",
    supportDatatype:IntegerType
  },
];
// 字符串函数
export const StringFn = [
  {
    label: "CHAR_LENGTH",
    supportDatatype:StringType
  },
  {
    label: "CONCAT",
    supportDatatype:StringType,
    filters: [
      {
        type: "select",
        label: "Concat Fields",
        multiple: true,
        options() {
          return this.fieldList.filter(item => item.field != this.field).map(item => ({ label: item.field, value: item.field }));
        },
        placeholder: "Select Fields",
        field: "concatFields",
        defaultValue: [],
      },
    ],
  },
  {
    label: "CONCAT_WS",
    supportDatatype:StringType,
    filters: [
      {
        type: "input",
        label: "Separator Fields",
        placeholder: "Separator Fields",
        field: "separatorFields",
        defaultValue: "",
      },
      {
        type: "select",
        label: "Concat Fields",
        multiple: true,
        options() {
          return this.fieldList.filter(item => item.field != this.field).map(item => ({ label: item.field, value: item.field }));
        },
        placeholder: "Select Fields",
        field: "concatFields",
        defaultValue: [],
      },
    ],
  },
  {
    label: "LENGTH",
    supportDatatype:StringType
  },
  {
    label: "LOWER",
    supportDatatype:StringType
  },
  {
    label: "LTRIM",
    supportDatatype:StringType
  },
  {
    label: "RTRIM",
    supportDatatype:StringType
  },
  {
    label: "SUBSTR",
    supportDatatype:StringType,
    filters: [
      {
        type: "number",
        label: "Position Fields",
        placeholder: "Position Fields",
        field: "posFields",
        defaultValue: "",
      },
      {
        type: "number",
        label: "length",
        placeholder: "length",
        field: "len",
        defaultValue: "",
      },
    ],
  },
  {
    label: "UPPER",
    supportDatatype:StringType
  },
];

// 转换函数
export const CoversionFn = [
  {
    label: "CAST",
    supportDatatype:StringType.concat(IntegerType)
  },
  {
    label: "TO_ISO0861",
    supportDatatype:['int','timestamp']
  },
  {
    label: "TO_JSON",
    supportDatatype:['json']
  },
  {
    label: "TO_UNIXTIMESTAMP",
    supportDatatype:['varchar','nchar']
  }
];
// 时间和日期函数
export const DatetimeFN = [
  {
    label: "NOW",
    supportDatatype:['timestamp']
  },
  {
    label: "TIMEDIFF",
    supportDatatype:['timestamp'],
    filters: [
      {
        type: 'string',
        label: 'TimeUnit Fields',
        placeholder: 'TimeUnit Fields',
        field: 'timeunitFields',
        options: [
          {
            label: "nanosecond",
            value: "1b",
          },
          {
            label: "microsecond",
            value: "1u",
          },
          {
            label: "millsecond",
            value: "1a",
          },
          {
            label: "second",
            value: "1s",
          },
          {
            label: "minute",
            value: "1m",
          },
          {
            label: "hour",
            value: "1h",
          },
          {
            label: "day",
            value: "1d",
          },
          {
            label: "week",
            value: "1w",
          },
        ],
      }
    ]
  },
  {
    label: "TIMETRUNCATE",
    supportDatatype:['timestamp'],
    filters: [
      {
        type: 'string',
        label: 'TimeUnit Fields',
        placeholder: 'TimeUnit Fields',
        field: 'timeunitFields',
        options: [
          {
            label: "nanosecond",
            value: "1b",
          },
          {
            label: "microsecond",
            value: "1u",
          },
          {
            label: "millsecond",
            value: "1a",
          },
          {
            label: "second",
            value: "1s",
          },
          {
            label: "minute",
            value: "1m",
          },
          {
            label: "hour",
            value: "1h",
          },
          {
            label: "day",
            value: "1d",
          },
          {
            label: "week",
            value: "1w",
          },
        ],
      },
      {
        type: 'number',
        label: 'ignore_timezone',
        placeholder: 'ignore_timezone',
        defaultValue: 1,
        field: 'ignore_timezone'
      }
    ]
  },
  {
    label: "TIMEZONE",
    supportDatatype:['timestamp']
  },
  {
    label: "TODAY",
    supportDatatype:['timestamp']
  }
];
// 聚合函数
export const AggregationFn = [
  {
    label: "APERCENTILE",
    supportTopic:false,
    supportDatatype:IntegerType,
    filters: [
      {
        type: "number",
        label: "p",
        placeholder: "p",
        min: 0,
        max: 100,
        field: "p",
        defaultValue: 0,
      },
      {
        type: "select",
        label: "algo_type",
        options: [
          {
            label: "default",
            value: "default",
          },
          {
            label: "t-digest",
            value: "t-digest",
          },
        ],
        placeholder: "algo_type",
        field: "algo_type",
        defaultValue: "default",
      },
    ],
  },
  {
    label: "AVG",
    supportTopic:false,
    supportDatatype:IntegerType,
  },
  {
    label: "COUNT",
    supportTopic:false,
    supportDatatype:['all'],
  },
  {
    label: "ELAPSED",
    supportTopic:false,
    supportStream: false,
    supportDatatype:['timestamp'],
    include: ["TIMESTAMP"],
    filters: [
      {
        type: "select",
        label: "TimeUnit Fields",
        options: [
          {
            label: "nanosecond",
            value: "1b",
          },
          {
            label: "microsecond",
            value: "1u",
          },
          {
            label: "millsecond",
            value: "1a",
          },
          {
            label: "second",
            value: "1s",
          },
          {
            label: "minute",
            value: "1m",
          },
          {
            label: "hour",
            value: "1h",
          },
          {
            label: "day",
            value: "1d",
          },
          {
            label: "week",
            value: "1w",
          },
        ],
        placeholder: "TimeUnit Fields",
        field: "timeunitFields",
        defaultValue: "",
      },
    ],
  },
  {
    label: "LEASTSQUARES",
    supportStream: false,
    supportTopic:false,
    supportDatatype:IntegerType,
    filters: [
      {
        type: "number",
        label: "Start Fields",
        placeholder: "Start Fields",
        min: 0,
        field: "startFields",
        defaultValue: 0,
      },
      {
        type: "number",
        label: "Step Fields",
        placeholder: "Step Fields",
        min: 0,
        field: "stepFields",
        defaultValue: 0,
      },
    ],
  },
  // {
  //   label: "MODE",
  //   supportTopic:false,
  //   supportStream: false,
  //   supportDatatype:['all']
  // },
  {
    label: "SPREAD",
    supportTopic:false,
    supportDatatype:['int','timestamp']
  },
  {
    label: "STDDEV",
    supportTopic:false,
    supportDatatype:IntegerType
  },
  {
    label: "SUM",
    supportTopic:false,
    supportDatatype:IntegerType
  },
  {
    label: "HYPERLOGLOG",
    supportTopic:false,
    supportDatatype:['all']
  },
  // {
  //   label: "HIPERLOGLOG",
  //   supportTopic:false,
  //   supportDatatype:['all']
  // },
  {
    label: "HISTOGRAM",
    supportTopic:false,
    supportStream: false,
    supportDatatype:IntegerType,
    filters: [
      {
        type: "select",
        label: "BinType Fields",
        options: [
          {
            label: "user_input",
            value: "user_input",
          },
          {
            label: "linear_bin",
            value: "linear_bin",
          },
          {
            label: "log_bin",
            value: "log_bin",
          },
        ],
        placeholder: "BinType Fields",
        field: "bintypeFields",
        defaultValue: "",
      },
      {
        type: "input",
        label: "Description Fields",
        placeholder: "Description Fields",
        field: "descriptionFields",
        defaultValue: "",
      },
    ],
  },
  {
    label: "PERCENTILE",
    supportTopic:false,
    supportStream: false,
    supportDatatype:IntegerType,
    filters: [
      {
        type: "number",
        label: "p",
        placeholder: "p",
        min: 0,
        max: 100,
        field: "p",
        defaultValue: 0,
      },
    ],
  },
];
// 选择函数
export const SelectorFn = [
  {
    label: 'BOTTOM',
    supportStream: false,
    supportTopic:false,
    supportDatatype:IntegerType,
    filters: [
      {
        type: "number",
        label: "k",
        placeholder: "k",
        min: 1,
        max: 100,
        field: "k",
        defaultValue: 1,
      }
    ]
  },
  {
    label: 'FIRST',
    supportTopic:false,
    supportDatatype:['all']

  },
  {
    label: 'INTERP',
    supportTopic:false,
    supportStream: false,
    supportDatatype:IntegerType
  },
  {
    label: "LAST",
    supportTopic:false,
    supportDatatype:['all']
  },
  {
    label: "LAST_ROW",
    supportTopic:false,
    supportDatatype:['all']
  },
  {
    label: "MAX",
    supportTopic:false,
    supportDatatype:IntegerType
  },
  {
    label: "MIN",
    supportTopic:false,
    supportDatatype:IntegerType
  },
  {
    label: "MODE",
    supportStream: false,
    supportTopic:false,
    supportDatatype:['all']
  },
  {
    label: "SAMPLE",
    supportTopic:false,
    supportStream: false,
    supportDatatype:['all'],
    filters: [
      {
        type: "number",
        label: "k",
        placeholder: "k",
        min: 1,
        max: 100,
        field: "k",
        defaultValue: 1,
      }
    ]
  },
  {
    label: "TAIL",
    supportStream: false,
    supportTopic:false,
    supportDatatype:IntegerType.concat(StringType),
    filters: [
      {
        type: "number",
        label: "k",
        placeholder: "k",
        min: 1,
        max: 100,
        field: "k",
        defaultValue: 1,
      },
      {
        type: "number",
        label: "offset_rows",
        placeholder: "offset_rows",
        min: 0,
        max: 100,
        field: "offset_rows",
        defaultValue: 0,
      }
    ]
  },
  {
    label: "TOP",
    supportStream: false,
    supportTopic:false,
    supportDatatype:IntegerType,
    filters: [
      {
        type: "number",
        label: "k",
        placeholder: "k",
        min: 1,
        max: 100,
        field: "k",
        defaultValue: 1,
      }
    ]
  },
  {
    label: "UNIQUE",
    supportTopic:false,
    supportStream: false,
    supportDatatype:IntegerType.concat(StringType),
  }









];
// 时序数据特有函数
export const SeriesSpecificFn = [
  {
    label: "CSUM",
    supportStream: false,
    supportDatatype:IntegerType
  },
  {
    label: "DERIVATIVE",
    supportStream: false,
    supportDatatype:IntegerType,
    filters: [
      {
        type: "number",
        label: "Interval Fields",
        placeholder: "Interval Fields",
        min: 1,
        field: "intervalFields",
        defaultValue: 1,
      },
      {
        type: "select",
        label: "Ignore Negative Fields",
        placeholder: "Ignore Negative Fields",
        field: "ignorenegative",
        defaultValue: 1,
        options: [
          {
            label: 'normal',////////需要指定名称？？？
            value: 0
          },
          {
            label: 'negative',
            value: 1
          }
        ]
      }
    ]
  },
  {
    label: "DIFF",
    supportStream: false,
    supportDatatype:IntegerType,
    filters: [
      {
        type: "select",
        label: "Negative Fields",
        placeholder: "Negative Fields",
        field: "negativeFields",
        defaultValue: 0,
        options: [
          {
            label: 'normal',////////需要指定名称？？？
            value: 0
          },
          {
            label: 'negative',
            value: 1
          }
        ]
      }
    ]
  },
  {
    label: "IRATE",
    supportStream: false,
    supportDatatype:IntegerType
  },
  {
    label: "MAVG",
    supportStream: false,
    supportDatatype:IntegerType,
    filters: [
      {
        type: "number",
        label: "k",
        placeholder: "k",
        min: 1,
        max: 1000,
        field: "k",
        defaultValue: 1,
      }
    ]
  },
  {
    label: "STATECOUNT",
    supportStream: false,
    supportDatatype:IntegerType,
    filters: [
      {
        type: "number",
        label: "Value Fields",
        placeholder: "Value Fields",
        field: "valueFields",
        defaultValue: 1,
      },
      {
        type: 'select',
        label: 'Operation Fields',
        placeholder: 'Operation Fields',
        field: 'operationFields',
        options: [
          {
            label: 'LT',
            value: 'LT'
          },
          {
            label: 'GT',
            value: 'GT'
          },
          {
            label: 'LE',
            value: "LE"
          },
          {
            label: 'GE',
            value: 'GE'
          },
          {
            label: 'NE',
            value: 'NE'
          },
          {
            label: 'EQ',
            value: 'EQ'
          }
        ]
      },

    ]
  },
  {
    label: "STATEDURATION",
    supportStream: false,
    supportDatatype:IntegerType,
    filters: [
      {
        type: "number",
        label: "Value Fields",
        placeholder: "Value Fields",
        field: "valueFields",
        defaultValue: 1,
      },
      {
        type: 'select',
        label: 'Operation Fields',
        placeholder: 'oper',
        field: 'oper',
        options: [
          {
            label: 'LT',
            value: "'LT'"
          },
          {
            label: 'GT',
            value: "'GT'"
          },
          {
            label: 'LE',
            value: "'LE'"
          },
          {
            label: 'GE',
            value: "'GE'"
          },
          {
            label: 'NE',
            value: "'NE'"
          },
          {
            label: 'EQ',
            value: "'EQ'"
          }
        ]
      },
      {
        type: "select",
        label: "unit",
        options: [
          {
            label: "nanosecond",
            value: "1b",
          },
          {
            label: "microsecond",
            value: "1u",
          },
          {
            label: "millsecond",
            value: "1a",
          },
          {
            label: "second",
            value: "1s",
          },
          {
            label: "minute",
            value: "1m",
          },
          {
            label: "hour",
            value: "1h",
          },
          {
            label: "day",
            value: "1d",
          },
          {
            label: "week",
            value: "1w",
          },
        ],
        placeholder: "unit",
        field: "unit",
        defaultValue: "",
      }
    ]
  },
  {
    label: "TWA",
    supportStream: false,
    supportDatatype:IntegerType
  }
];
// 系统信息函数
export const SystemFn = [
  {
    label: "DATABASE",
    supportDatatype:['system']
  },
  {
    label: "CLIENT_VERSION",
    supportDatatype:['system']
  },
  {
    label: "SERVER_VERSION",
    supportDatatype:['system']
  },
  {
    label: "SERVER_STATUS",
    supportDatatype:['system']
  }
];


export const TDengineStringType = ["VARCHAR", "BINARY", "NCHAR"];
export const TDengineNumberType = [
  "INT",
  "INT UNSIGNED",
  "BIGINT",
  "BIGINT UNSIGNED",
  "FLOAT",
  "DOUBLE",
  "SMALLINT",
  "SMALLINT UNSIGNED",
  "TINYINT",
  "TINYINT UNSIGNED",
];
// 时间戳可使用的运算符
export const CompareOperator = [">", "<", ">=", "<=", "!=", "="];
export const BooleanOperator = ["="];
export const JsonOperator = ["CONTAINS", "IS NULL", "IS NOT NULL"];
export const GeneralOperator = [
  {
    label: "IN",
    exclude: ["JSON"],
  },
  {
    label: "BETWEEN",
  },
  {
    label: "NOT IN",
  },
  {
    label: "NOT BETWEEN",
  },
  {
    label: "IS NULL",
  },
  {
    label: "IS NOT NULL",
  },
];
export const RegularOperator = ["MATCH", "NMATCH", "LIKE", "NOT LIKE"] 


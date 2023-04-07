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
export const ReLoginCode = ["502", "401", "432", "512"]; //重新登录的状态码
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
  // buffer: { type: "number", alter: false, defaultValue: 32 },
  cachemodel: { type: "string", alter: true, defaultValue: "none" },
  // cachesize: { type: "number", alter: true, defaultValue: 1 },
  // comp: { type: "number", alter: false, defaultValue: 2 },
  duration: { type: "number", alter: false, defaultValue: "50d" },
  // wal_fsync_period: { type: "number", alter: true, defaultValue: 3000 },
  // maxrows: { type: "number", alter: false, defaultValue: 4096 },
  // minrows: { type: "number", alter: false, defaultValue: 100 },
  keep: { type: "number", alter: true, defaultValue: 3650 },
  // pages: { type: "number", alter: false, defaultValue: 256 },
  // pagesize: { type: "number", alter: false, defaultValue: 4 },
  // replica: { type: "number", alter: false, defaultValue: 1 },
  // retentions: { type: "number", alter: false, defaultValue: "" },
  // strict: { type: "string", alter: false, defaultValue: "off" },
  // wal_level: { type: "number", alter: true, defaultValue: 1 },
  // vgroups: { type: "number", alter: false, defaultValue: 1 },
  // single_stable: { type: "number", alter: false, defaultValue: 0 },
  wal_retention_period: { type: "number", alter: false, defaultValue: 0 }, //
  // wal_retention_size: { type: "number", alter: false, defaultValue: 0 },
  // wal_roll_period: { type: "number", alter: false, defaultValue: 0 },
  // wal_segment_size: { type: "number", alter: false, defaultValue: 0 },
  precision: { type: "string", alter: false, defaultValue: "ms" },
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

export const StreamDocsUrl =navigator.language.includes('en')? "https://docs.tdengine.com/develop/stream/":'https://docs.taosdata.com/develop/stream/';
export const DocsUrl = "https://docs.tdengine.com"

export const TdengineVersion = "3.0.3.2";
export const SubscriptionDocsUrl =navigator.language.includes('en')? "https://docs.tdengine.com/taos-sql/tmq/#create-a-topic":'https://docs.taosdata.com/taos-sql/tmq/#create-a-topic';
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

// 数学函数
export const NumbericFn = [
  {
    label: "ABS",
  },
  {
    label: "ACOS",
  },
  {
    label: "ASIN",
  },
  {
    label: "ATAN",
  },
  {
    label: "CEIL",
  },
  {
    label: "COS",
  },
  {
    label: "FLOOR",
  },
  {
    label: "LOG",
    filters: [
      {
        type: "select",
        label: "Log Filed",
        options() {
          return this.fieldList.filter(item => item.filed != this.field);
        },
        placeholder: "Select Filed",
        field: "logFiled",
        defaultValue: "",
      },
    ],
  },
  {
    label: "POW",
    filters: [
      {
        type: "select",
        label: "Pow Filed",
        options() {
          return this.fieldList.filter(item => item.filed != this.field);
        },
        placeholder: "Select Filed",
        field: "powFiled",
        defaultValue: "",
      },
    ],
  },
  {
    label: "ROUND",
  },
  {
    label: "SIN",
  },
  {
    label: "SQRT",
  },
  {
    label: "TAN",
  },
];
// 字符串函数
export const StringFn = [
  {
    label: "CHAR_LENGTH",
  },
  {
    label: "CONCAT",
    filters: [
      {
        type: "select",
        label: "Concat Fileds",
        multiple: true,
        options() {
          return this.fieldList.filter(item => item.filed != this.field).map(item => ({ label: item.filed, value: item.filed }));
        },
        placeholder: "Select Fileds",
        field: "concatFields",
        defaultValue: [],
      },
    ],
  },
  {
    label: "CONCAT_WS",
    filters: [
      {
        type: "input",
        label: "Separator",
        placeholder: "Separator_expr",
        field: "separator",
        defaultValue: "",
      },
      {
        type: "select",
        label: "Concat Fileds",
        multiple: true,
        options() {
          return this.fieldList.filter(item => item.filed != this.field).map(item => ({ label: item.filed, value: item.filed }));
        },
        placeholder: "Select Fileds",
        field: "concatFields",
        defaultValue: [],
      },
    ],
  },
  {
    label: "LENGTH",
  },
  {
    label: "LOWER",
  },
  {
    label: "LTRIM",
  },
  {
    label: "RTRIM",
  },
  {
    label: "SUBSTR",
    filters: [
      {
        type: "number",
        label: "position",
        placeholder: "Separator_expr",
        field: "pos",
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
  },
];

// 转换函数
export const CoversionFn = [
  // "CAST",
  "TO_ISO0861",
  "TO_JSON",
  "TO_UNIXTIMESTAMP",
];
// 时间和日期函数
export const DatetimeFN = ["NOW", "TIMEDIFF", "TIMETRUNCATE", "TIMEZONE", "TODAY"];
// 聚合函数
export const AggregationFn = [
  {
    label: "APERCENTILE",
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
  },
  {
    label: "COUNT",
  },
  {
    label: "ELAPSED",
    include: ["TIMESTAMP"],
    filters: [
      {
        type: "select",
        label: "time_unit",
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
        placeholder: "time_unit",
        field: "time_unit",
        defaultValue: "",
      },
    ],
  },
  {
    label: "LEASTSQUARES",
    filters: [
      {
        type: "number",
        label: "start_val",
        placeholder: "start_val",
        min: 0,
        field: "start_val",
        defaultValue: 0,
      },
      {
        type: "number",
        label: "step_val",
        placeholder: "step_val",
        min: 0,
        field: "step_val",
        defaultValue: 0,
      },
    ],
  },
  {
    label: "MODE",
  },
  {
    label: "SPREAD",
  },
  {
    label: "STDDEV",
  },
  {
    label: "SUM",
  },
  {
    label: "HYPERLOGLOG",
  },
  {
    label: "HIPERLOGLOG",
  },
  {
    label: "HISTOGRAM",
    filters: [
      {
        type: "select",
        label: "bin_type",
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
        placeholder: "bin_type",
        field: "bin_type",
        defaultValue: "",
      },
      {
        type: "input",
        label: "bin_description",
        placeholder: "bin_description",
        field: "bin_description",
        defaultValue: "",
      },
    ],
  },
  {
    label: "PERCENTILE",
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
  // "APERCENTILE",
  "BOTTOM",
  "FIRST",
  "INTERP",
  "LAST",
  "LAST_ROW",
  "MAX",
  "MIN",
  "PERCENTILE",
  // "TAIL",
  "TOP",
  "UNIQUE",
];
// 时序数据特有函数
export const SeriesSpecificFn = ["CSUM", "DERIVATIVE", "DIFF", "IRATE", "MAVG", "SAMPLE", "STATECOUNT", "STATEDURATION", "STATEMENT"];
// 系统信息函数
export const SystemFn = ["DATABASE", "CLIENT_VERSION", "SERVER_VERSION", "SERVER_STATUS", "CURRENT_USER", "USER"];


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
export const CompareOperator = [">", "<", ">=", "<=", "!=", "=="];
export const BooleanOperator = ["=="];
export const JsonOperator = ["Contains"];
export const GeneralOperator = [
  {
    label: "IN",
    exclude: ["JSON"],
  },
  {
    label: "BETWEEN",
  },
  {
    label: "LIKE",
  },
  {
    label: "NOT LIKE",
  },
  {
    label: "NOT IN",
  },
  {
    label: "NOT BETWEEN",
  },
  {
    label: "ISNULL",
  },
];
// 聚合函数

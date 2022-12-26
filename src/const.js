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
  "/calculator",
  "/createFirstInstance",
];
// 不需要切换集群的路由
export const NoInstanceSelectRoute = ["/billing", "/alert", "/activity", "/profile", "/support", "/user"];
export const NoInstanceAccessRoute = ["/createFirstInstance"];
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

export const StreamDocsUrl = "https://docs.tdengine.com/cloud/stream/";

export const SubscriptionDocsUrl = " https://docs.tdengine.com/cloud/tmq/";

export const RequestCommonConfig = {
  timeout: 20000,
  withCredentials: false,
};

export const SlowSqlTime = "200ms";

export const ReplicationTaskStatus = ["created", "failed", "cancelled", "deleted", "completed", "interrupted", "stopped", "running"];
export const ReplicationTaskCanStopStatus = ["interrupted", "running", "created"];

export const ReplicationTaskCanStartStatus = ["stopped"];

export const $bus = mitt();

export const CustomShellContent = ["Welcome to TDengine Cloud"];


%{
package sqlparser

import (
    "bytes"
    "sqlparser/tool"
    "strings"
)

func setParseTree(yylex interface{}, stmt Statement) {
  yylex.(*Scanner).ParseTree = stmt
}

func tokenToInt32(tok Token) int32 {
  v, err := tool.ConvertBytesToInt32(tok.Bytes)
  if err != nil {
    return -1
  }
  return v
}

func makeDropTableEntryText(ifExists bool, name string) string {
  if ifExists {
    return "if exists " + name
  }
  return name
}

func tableNameFromFullName(full string) *TableName {
  parts := strings.SplitN(full, ".", 2)
  if len(parts) == 2 {
    return &TableName{Qualifier: NewTableIdent(parts[0]), Name: NewTableIdent(parts[1])}
  }
  return &TableName{Name: NewTableIdent(full)}
}

func grantPrivNameFromArg(priv int32) string {
  names := map[int32]string{
    PRIV_CM_ALL: "all",
    PRIV_CM_ALTER: "alter",
    PRIV_CM_DROP: "drop",
    PRIV_CM_SHOW: "show",
    PRIV_CM_SHOW_CREATE: "show create",
    PRIV_CM_START: "start",
    PRIV_CM_STOP: "stop",
    PRIV_CM_KILL: "kill",
    PRIV_CM_RECALC: "recalculate",
    PRIV_CM_SUBSCRIBE: "subscribe",

    PRIV_DB_CREATE: "create database",
    PRIV_DB_DROP_OWNED: "drop owned database",
    PRIV_DB_USE: "use database",
    PRIV_DB_FLUSH: "flush database",
    PRIV_DB_COMPACT: "compact database",
    PRIV_DB_TRIM: "trim database",
    PRIV_DB_ROLLUP: "rollup database",
    PRIV_DB_SCAN: "scan database",
    PRIV_DB_SSMIGRATE: "ssmigrate database",

    PRIV_SHOW_VNODES: "show vnodes",
    PRIV_SHOW_VGROUPS: "show vgroups",
    PRIV_SHOW_COMPACTS: "show compacts",
    PRIV_SHOW_RETENTIONS: "show retentions",
    PRIV_SHOW_SCANS: "show scans",
    PRIV_SHOW_SSMIGRATES: "show ssmigrates",

    PRIV_TBL_CREATE: "create table",
    PRIV_TBL_SELECT: "select table",
    PRIV_TBL_INSERT: "insert table",
    PRIV_TBL_UPDATE: "update table",
    PRIV_TBL_DELETE: "delete table",

    PRIV_FUNC_CREATE: "create function",
    PRIV_FUNC_DROP: "drop function",
    PRIV_FUNC_SHOW: "show functions",
    PRIV_IDX_CREATE: "create index",
    PRIV_IDX_DROP: "drop index",
    PRIV_IDX_SHOW: "show indexes",
    PRIV_VIEW_CREATE: "create view",
    PRIV_VIEW_SELECT: "select view",
    PRIV_RSMA_CREATE: "create rsma",
    PRIV_TSMA_CREATE: "create tsma",
    PRIV_MOUNT_CREATE: "create mount",
    PRIV_MOUNT_DROP: "drop mount",
    PRIV_MOUNT_SHOW: "show mounts",
    PRIV_PASS_ALTER: "alter pass",
    PRIV_PASS_ALTER_SELF: "alter self pass",

    PRIV_ROLE_CREATE: "create role",
    PRIV_ROLE_DROP: "drop role",
    PRIV_ROLE_SHOW: "show roles",

    PRIV_USER_CREATE: "create user",
    PRIV_USER_DROP: "drop user",
    PRIV_USER_SET_SECURITY: "set user security info",
    PRIV_USER_SET_AUDIT: "set user audit info",
    PRIV_USER_SET_BASIC: "set user basic info",
    PRIV_USER_UNLOCK: "unlock user",
    PRIV_USER_LOCK: "lock user",
    PRIV_USER_SHOW: "show users",

    PRIV_AUDIT_DB_CREATE: "create audit database",
    PRIV_AUDIT_DB_DROP: "drop audit database",
    PRIV_AUDIT_DB_ALTER: "alter audit database",
    PRIV_AUDIT_DB_USE: "use audit database",

    PRIV_TOKEN_CREATE: "create token",
    PRIV_TOKEN_DROP: "drop token",
    PRIV_TOKEN_ALTER: "alter token",
    PRIV_TOKEN_SHOW: "show tokens",

    PRIV_KEY_UPDATE: "update key",
    PRIV_TOTP_CREATE: "create totp",
    PRIV_TOTP_DROP: "drop totp",
    PRIV_TOTP_UPDATE: "update totp",

    PRIV_GRANT_PRIVILEGE: "grant privilege",
    PRIV_REVOKE_PRIVILEGE: "revoke privilege",
    PRIV_SHOW_PRIVILEGES: "show privileges",

    PRIV_NODE_CREATE: "create node",
    PRIV_NODE_DROP: "drop node",
    PRIV_NODES_SHOW: "show nodes",

    PRIV_VAR_SECURITY_ALTER: "alter security variable",
    PRIV_VAR_AUDIT_ALTER: "alter audit variable",
    PRIV_VAR_SYSTEM_ALTER: "alter system variable",
    PRIV_VAR_DEBUG_ALTER: "alter debug variable",
    PRIV_VAR_SECURITY_SHOW: "show security variables",
    PRIV_VAR_AUDIT_SHOW: "show audit variables",
    PRIV_VAR_SYSTEM_SHOW: "show system variables",
    PRIV_VAR_DEBUG_SHOW: "show debug variables",

    PRIV_TOPIC_CREATE: "create topic",
    PRIV_CONSUMER_SHOW: "show consumers",
    PRIV_SUBSCRIPTION_SHOW: "show subscriptions",
    PRIV_STREAM_CREATE: "create stream",

    PRIV_TRANS_SHOW: "show trans",
    PRIV_TRANS_KILL: "kill trans",
    PRIV_CONNECTION_SHOW: "show connections",
    PRIV_CONNECTION_KILL: "kill connection",
    PRIV_QUERY_SHOW: "show queries",
    PRIV_QUERY_KILL: "kill query",

    PRIV_INFO_SCHEMA_USE: "use information_schema",
    PRIV_PERF_SCHEMA_USE: "use performance_schema",
    PRIV_INFO_SCHEMA_READ_LIMIT: "read information_schema limit",
    PRIV_INFO_SCHEMA_READ_SEC: "read information_schema security",
    PRIV_INFO_SCHEMA_READ_AUDIT: "read information_schema audit",
    PRIV_INFO_SCHEMA_READ_BASIC: "read information_schema basic",
    PRIV_PERF_SCHEMA_READ_LIMIT: "read performance_schema limit",
    PRIV_PERF_SCHEMA_READ_BASIC: "read performance_schema basic",

    PRIV_GRANTS_SHOW: "show grants",
    PRIV_CLUSTER_SHOW: "show cluster",
    PRIV_APPS_SHOW: "show apps",
  }
  if v, ok := names[priv]; ok {
    return v
  }
  return "read"
}

func grantPrivArgFromName(name string) int32 {
  args := map[string]int32{
    "all": PRIV_CM_ALL,
    "alter": PRIV_CM_ALTER,
    "drop": PRIV_CM_DROP,
    "show": PRIV_CM_SHOW,
    "show create": PRIV_CM_SHOW_CREATE,
    "start": PRIV_CM_START,
    "stop": PRIV_CM_STOP,
    "kill": PRIV_CM_KILL,
    "recalculate": PRIV_CM_RECALC,
    "subscribe": PRIV_CM_SUBSCRIBE,

    "create database": PRIV_DB_CREATE,
    "drop owned database": PRIV_DB_DROP_OWNED,
    "use database": PRIV_DB_USE,
    "flush database": PRIV_DB_FLUSH,
    "compact database": PRIV_DB_COMPACT,
    "trim database": PRIV_DB_TRIM,
    "rollup database": PRIV_DB_ROLLUP,
    "scan database": PRIV_DB_SCAN,
    "ssmigrate database": PRIV_DB_SSMIGRATE,
    "use": PRIV_DB_USE,
    "flush": PRIV_DB_FLUSH,
    "compact": PRIV_DB_COMPACT,
    "trim": PRIV_DB_TRIM,
    "rollup": PRIV_DB_ROLLUP,
    "scan": PRIV_DB_SCAN,
    "ssmigrate": PRIV_DB_SSMIGRATE,
    "drop owned": PRIV_DB_DROP_OWNED,

    "show vnodes": PRIV_SHOW_VNODES,
    "show vgroups": PRIV_SHOW_VGROUPS,
    "show compacts": PRIV_SHOW_COMPACTS,
    "show retentions": PRIV_SHOW_RETENTIONS,
    "show scans": PRIV_SHOW_SCANS,
    "show ssmigrates": PRIV_SHOW_SSMIGRATES,

    "create table": PRIV_TBL_CREATE,
    "select table": PRIV_TBL_SELECT,
    "insert table": PRIV_TBL_INSERT,
    "update table": PRIV_TBL_UPDATE,
    "delete table": PRIV_TBL_DELETE,

    "create function": PRIV_FUNC_CREATE,
    "drop function": PRIV_FUNC_DROP,
    "show functions": PRIV_FUNC_SHOW,
    "create index": PRIV_IDX_CREATE,
    "drop index": PRIV_IDX_DROP,
    "show indexes": PRIV_IDX_SHOW,
    "create view": PRIV_VIEW_CREATE,
    "select view": PRIV_VIEW_SELECT,
    "create rsma": PRIV_RSMA_CREATE,
    "create tsma": PRIV_TSMA_CREATE,
    "create mount": PRIV_MOUNT_CREATE,
    "drop mount": PRIV_MOUNT_DROP,
    "show mounts": PRIV_MOUNT_SHOW,
    "alter pass": PRIV_PASS_ALTER,
    "alter self pass": PRIV_PASS_ALTER_SELF,

    "create role": PRIV_ROLE_CREATE,
    "drop role": PRIV_ROLE_DROP,
    "show roles": PRIV_ROLE_SHOW,

    "create user": PRIV_USER_CREATE,
    "drop user": PRIV_USER_DROP,
    "set user security info": PRIV_USER_SET_SECURITY,
    "set user audit info": PRIV_USER_SET_AUDIT,
    "set user basic info": PRIV_USER_SET_BASIC,
    "unlock user": PRIV_USER_UNLOCK,
    "lock user": PRIV_USER_LOCK,
    "show users": PRIV_USER_SHOW,

    "create audit database": PRIV_AUDIT_DB_CREATE,
    "drop audit database": PRIV_AUDIT_DB_DROP,
    "alter audit database": PRIV_AUDIT_DB_ALTER,
    "use audit database": PRIV_AUDIT_DB_USE,

    "create token": PRIV_TOKEN_CREATE,
    "drop token": PRIV_TOKEN_DROP,
    "alter token": PRIV_TOKEN_ALTER,
    "show tokens": PRIV_TOKEN_SHOW,

    "update key": PRIV_KEY_UPDATE,
    "create totp": PRIV_TOTP_CREATE,
    "drop totp": PRIV_TOTP_DROP,
    "update totp": PRIV_TOTP_UPDATE,

    "grant privilege": PRIV_GRANT_PRIVILEGE,
    "revoke privilege": PRIV_REVOKE_PRIVILEGE,
    "show privileges": PRIV_SHOW_PRIVILEGES,

    "create node": PRIV_NODE_CREATE,
    "drop node": PRIV_NODE_DROP,
    "show nodes": PRIV_NODES_SHOW,

    "alter security variable": PRIV_VAR_SECURITY_ALTER,
    "alter audit variable": PRIV_VAR_AUDIT_ALTER,
    "alter system variable": PRIV_VAR_SYSTEM_ALTER,
    "alter debug variable": PRIV_VAR_DEBUG_ALTER,
    "show security variables": PRIV_VAR_SECURITY_SHOW,
    "show audit variables": PRIV_VAR_AUDIT_SHOW,
    "show system variables": PRIV_VAR_SYSTEM_SHOW,
    "show debug variables": PRIV_VAR_DEBUG_SHOW,

    "create topic": PRIV_TOPIC_CREATE,
    "show consumers": PRIV_CONSUMER_SHOW,
    "show subscriptions": PRIV_SUBSCRIPTION_SHOW,
    "create stream": PRIV_STREAM_CREATE,

    "show trans": PRIV_TRANS_SHOW,
    "show transactions": PRIV_TRANS_SHOW,
    "kill trans": PRIV_TRANS_KILL,
    "kill transaction": PRIV_TRANS_KILL,
    "show connections": PRIV_CONNECTION_SHOW,
    "kill connection": PRIV_CONNECTION_KILL,
    "show queries": PRIV_QUERY_SHOW,
    "kill query": PRIV_QUERY_KILL,

    "use information_schema": PRIV_INFO_SCHEMA_USE,
    "use performance_schema": PRIV_PERF_SCHEMA_USE,
    "read information_schema limit": PRIV_INFO_SCHEMA_READ_LIMIT,
    "read information_schema security": PRIV_INFO_SCHEMA_READ_SEC,
    "read information_schema audit": PRIV_INFO_SCHEMA_READ_AUDIT,
    "read information_schema basic": PRIV_INFO_SCHEMA_READ_BASIC,
    "read performance_schema limit": PRIV_PERF_SCHEMA_READ_LIMIT,
    "read performance_schema basic": PRIV_PERF_SCHEMA_READ_BASIC,
    "show grants": PRIV_GRANTS_SHOW,
    "show cluster": PRIV_CLUSTER_SHOW,
    "show apps": PRIV_APPS_SHOW,

    "read": PRIV_TYPE_UNKNOWN,
    "write": PRIV_TYPE_UNKNOWN,
  }
  if v, ok := args[name]; ok {
    return v
  }
  return PRIV_TYPE_UNKNOWN
}

func applyGrantLevel(level string, stmt *GrantStmt) {
  if stmt == nil {
    return
  }
  lvl := strings.TrimSpace(level)
  if lvl == "" {
    return
  }

  kind := ""
  target := lvl
  if i := strings.IndexByte(lvl, ' '); i > 0 {
    head := lvl[:i]
    switch head {
    case "database", "table", "view", "index", "topic", "stream", "rsma", "tsma":
      kind = head
      target = strings.TrimSpace(lvl[i+1:])
    }
  }

  switch kind {
  case "database":
    stmt.Privileges.ObjType = PRIV_OBJ_DB
    stmt.ObjName = target
    return
  case "table", "view", "index", "topic", "stream", "rsma", "tsma":
    stmt.Privileges.ObjType = PRIV_OBJ_TBL
  }

  if target == "*" {
    stmt.Privileges.ObjType = PRIV_OBJ_TBL
    stmt.ObjName = "*"
    return
  }

  if strings.Contains(target, ".") {
    parts := strings.SplitN(target, ".", 2)
    stmt.Privileges.ObjType = PRIV_OBJ_TBL
    stmt.ObjName = parts[0]
    stmt.TabName = parts[1]
    return
  }

  if stmt.Privileges.ObjType == PRIV_OBJ_TBL {
    stmt.ObjName = target
    return
  }

  stmt.Privileges.ObjType = PRIV_OBJ_DB
  stmt.ObjName = target
}

func exprToSQL(expr Expr) string {
  if expr == nil {
    return ""
  }
  tb := &TrackedBuffer{Buffer: &bytes.Buffer{}}
  tb.Myprintf("%v", expr)
  return tb.String()
}

func exprListToSQL(args []Expr) string {
  out := make([]string, 0, len(args))
  for _, a := range args {
    out = append(out, exprToSQL(a))
  }
  return strings.Join(out, ", ")
}

func buildNamedFuncSQL(name string, args []Expr) string {
  return name + "(" + exprListToSQL(args) + ")"
}

func bytes2DToStrings(in [][]byte) []string {
  if len(in) == 0 {
    return nil
  }
  out := make([]string, 0, len(in))
  for _, b := range in {
    out = append(out, string(b))
  }
  return out
}

func dataTypeFromTypeName(lexer yyLexer, name string) DataType {
  n := strings.ToLower(strings.TrimSpace(name))

  switch n {
  case "bool":
    return CreateDataType(TSDB_DATA_TYPE_BOOL)
  case "tinyint":
    return CreateDataType(TSDB_DATA_TYPE_TINYINT)
  case "smallint":
    return CreateDataType(TSDB_DATA_TYPE_SMALLINT)
  case "int", "integer":
    return CreateDataType(TSDB_DATA_TYPE_INT)
  case "bigint":
    return CreateDataType(TSDB_DATA_TYPE_BIGINT)
  case "float":
    return CreateDataType(TSDB_DATA_TYPE_FLOAT)
  case "double":
    return CreateDataType(TSDB_DATA_TYPE_DOUBLE)
  case "timestamp":
    return CreateDataType(TSDB_DATA_TYPE_TIMESTAMP)
  case "tinyint unsigned":
    return CreateDataType(TSDB_DATA_TYPE_UTINYINT)
  case "smallint unsigned":
    return CreateDataType(TSDB_DATA_TYPE_USMALLINT)
  case "int unsigned":
    return CreateDataType(TSDB_DATA_TYPE_UINT)
  case "bigint unsigned":
    return CreateDataType(TSDB_DATA_TYPE_UBIGINT)
  case "json":
    return CreateDataType(TSDB_DATA_TYPE_JSON)
  case "mediumblob":
    return CreateDataType(TSDB_DATA_TYPE_MEDIUMBLOB)
  case "blob":
    return CreateDataType(TSDB_DATA_TYPE_BLOB)
  }

  parseBracketArg := func(prefix string) []byte {
    if !strings.HasPrefix(n, prefix+"(") || !strings.HasSuffix(n, ")") {
      return nil
    }
    body := strings.TrimSpace(n[len(prefix)+1 : len(n)-1])
    return []byte(body)
  }

  if v := parseBracketArg("binary"); v != nil {
    return CreateVarLenDataType(lexer, TSDB_DATA_TYPE_BINARY, v)
  }
  if v := parseBracketArg("nchar"); v != nil {
    return CreateVarLenDataType(lexer, TSDB_DATA_TYPE_NCHAR, v)
  }
  if v := parseBracketArg("varchar"); v != nil {
    return CreateVarLenDataType(lexer, TSDB_DATA_TYPE_VARCHAR, v)
  }
  if v := parseBracketArg("varbinary"); v != nil {
    return CreateVarLenDataType(lexer, TSDB_DATA_TYPE_VARBINARY, v)
  }
  if v := parseBracketArg("geometry"); v != nil {
    return CreateVarLenDataType(lexer, TSDB_DATA_TYPE_GEOMETRY, v)
  }
  if v := parseBracketArg("decimal"); v != nil {
    parts := strings.SplitN(string(v), ",", 2)
    p := []byte(strings.TrimSpace(parts[0]))
    var s []byte
    if len(parts) == 2 {
      sv := strings.TrimSpace(parts[1])
      if sv != "" {
        s = []byte(sv)
      }
    }
    return CreateDecimalDataType(lexer, TSDB_DATA_TYPE_DECIMAL, p, s)
  }

  if n == "binary" {
    return CreateVarLenDataType(lexer, TSDB_DATA_TYPE_BINARY, nil)
  }
  if n == "nchar" {
    return CreateVarLenDataType(lexer, TSDB_DATA_TYPE_NCHAR, nil)
  }
  if n == "varchar" {
    return CreateVarLenDataType(lexer, TSDB_DATA_TYPE_VARCHAR, nil)
  }
  if n == "varbinary" {
    return CreateVarLenDataType(lexer, TSDB_DATA_TYPE_VARBINARY, nil)
  }
  if n == "decimal" {
    return CreateDecimalDataType(lexer, TSDB_DATA_TYPE_DECIMAL, []byte("10"), nil)
  }

  return CreateDataType(TSDB_DATA_TYPE_INT)
}

%}

%union {
    token Token
    empty struct{}
    str   string
    strs  []string
    bytes []byte
    bytes2D [][]byte
    tokens []Token
    ipRangeList []*IpRange
    dateTimeRangeList []*DateTimeRange
    bool  bool
    i32 int32
    stmt  Statement
    userOptions *UserOptions

    createUserStmt *CreateUserStmt
    alterUserStmt *AlterUserStmt
    dropUserStmt *DropUserStmt
    
    tokenOptions *TokenOptions
    createTokenStmt *CreateTokenStmt
    alterTokenStmt *AlterTokenStmt
    dropTokenStmt *DropTokenStmt
    
    createRoleStmt *CreateRoleStmt
    dropRoleStmt *DropRoleStmt
    alterRoleStmt *AlterRoleStmt
    grantRoleStmt *GrantRoleStmt
    revokeRoleStmt *RevokeRoleStmt
    grantStmt *GrantStmt

    createEncryptKeyStmt *CreateEncryptKeyStmt
    alterEncryptKeyStmt *AlterEncryptKeyStmt
    createAlgrStmt *CreateAlgrStmt
    dropAlgrStmt *DropAlgrStmt

    createAnodeStmt *CreateAnodeStmt
    updateAnodeStmt *UpdateAnodeStmt
    dropAnodeStmt *DropAnodeStmt

    createDnodeStmt *CreateDnodeStmt
    dropDnodeStmt *DropDnodeStmt
    alterDnodeStmt *AlterDnodeStmt
    restoreDnodeStmt *RestoreDnodeStmt
    alterDnodesReloadStmt *AlterDnodesReloadStmt
    alterClusterStmt *AlterClusterStmt
    alterLocalStmt *AlterLocalStmt

    createComponentNodeStmt *CreateComponentNodeStmt
    dropComponentNodeStmt *DropComponentNodeStmt
    restoreComponentNodeStmt *RestoreComponentNodeStmt
    createBnodeStmt *CreateBnodeStmt
    dropBnodeStmt *DropBnodeStmt
    BnodeOptions *BnodeOptions

    createDatabaseStmt *CreateDatabaseStmt
    dropDatabaseStmt *DropDatabaseStmt
    useDatabaseStmt *UseDatabaseStmt
    flushDatabaseStmt *FlushDatabaseStmt
    ssMigrateDatabaseStmt *SsMigrateDatabaseStmt
    trimDatabaseStmt *TrimDatabaseStmt
    trimDatabaseWalStmt *TrimDatabaseWalStmt
    createMountStmt *CreateMountStmt
    dropMountStmt *DropMountStmt
    killStmt *KillStmt
    showStmt *ShowStmt
    balanceVgroupStmt *BalanceVgroupStmt
    balanceVgroupLeaderStmt *BalanceVgroupLeaderStmt
    assignLeaderStmt *AssignLeaderStmt
    alterVgroupKeepStmt *AlterVgroupKeepStmt
    mergeVgroupStmt *MergeVgroupStmt
    splitVgroupStmt *SplitVgroupStmt
    redistributeVgroupStmt *RedistributeVgroupStmt
    showTableScope ShowTableScope
    dbOptionKV DatabaseOptionKV
    alterDBOptions *AlterDatabaseOptions
    DatabaseOptions *DatabaseOptions
    tableOptions *TableOptions
    dropViewStmt *DropViewStmt
    streamStmt *StreamStmt
    recalcRange StreamRecalculateRange
    deleteStmt *DeleteStmt
    xnodeStmt *XnodeStmt
    scanStmt *ScanStmt
    compactStmt *CompactStmt
    tableNameNode *TableName
    columnDef *ColumnDef
    columnDefs []*ColumnDef
    columnOption *ColumnOption
    dataType DataType

    // SELECT/query related types
    selectStmt *SelectStmt
    expr Expr
    exprList []Expr
    tableExpr TableExpr
    joinExpr *JoinTableExpr
    columnExpr ColumnExpr
    columnList []ColumnExpr
    orderByExpr OrderByExpr
    orderByList []OrderByExpr
    windowExpr WindowExpr
    countWindowArgs CountWindowArgs
    stateWindowOpt StateWindowOpt
    fillExpr *FillExpr
    limitExpr *LimitExpr
    literal Literal
    groupByExpr *GroupByExpr
    whenThenList []WhenThenExpr
    hintOption *HintOption
    explainOptions ExplainOptions
}


%token LEX_ERROR

// parser tokens
%token <token> ID STRINGVALUE INTEGRALVALUE FLOATVALUE HEXNUMVALUE JSON_EXTRACT_OP NK_VARIABLE
%token <token> NK_INTEGER NK_FLOAT NK_STRING NK_BOOL NK_ID NK_HEX NK_BIN
%token <token> NK_LP NK_RP NK_DOT NK_COMMA NK_COLON NK_SEMI
%token <token> NK_BITNOT
%token <token> NK_ALIAS NK_HINT NK_QUESTION

// key words
%token <token> ACCOUNT
%token <token> ACCOUNTS
%token <token> ADD
%token <token> AGGREGATE
%token <token> ALTER
%token <token> ANALYZE
%token <token> ANODE
%token <token> ANODES
%token <token> ANOMALY_WINDOW
%token <token> APPS
%token <token> AS
%token <token> ASC
%token <token> AUDIT
%token <token> BALANCE
%token <token> BATCH_SCAN
%token <token> BASIC
%token <token> BIGINT
%token <token> BINARY
%token <token> BNODE
%token <token> BNODES
%token <token> BOOL
%token <token> BOTH
%token <token> BUFFER
%token <token> BUFSIZE
%token <token> BY
%token <token> CACHE
%token <token> CACHEMODEL
%token <token> CACHESIZE
%token <token> CALC_NOTIFY_ONLY
%token <token> CASE
%token <token> CAST
%token <token> CHILD
%token <token> CLIENT_VERSION
%token <token> CLUSTER
%token <token> COALESCE
%token <token> COLUMN
%token <token> COMMENT
%token <token> COMP
%token <token> COMPACT
%token <token> COMPACTS
%token <token> COMPACT_INTERVAL
%token <token> COMPACT_TIME_OFFSET
%token <token> COMPACT_TIME_RANGE
%token <token> CONNECTION
%token <token> CONNECTIONS
%token <token> CONNS
%token <token> CONSUMER
%token <token> CONSUMERS
%token <token> COUNT
%token <token> COUNT_WINDOW
%token <token> CREATE
%token <token> CREATEDB
%token <token> CURRENT_USER
%token <token> DATABASE
%token <token> DATABASES
%token <token> DBS
%token <token> DECIMAL
%token <token> DEFAULT
%token <token> DELETE
%token <token> DEBUG
%token <token> DELETE_MARK
%token <token> DELETE_OUTPUT_TABLE
%token <token> DELETE_RECALC
%token <token> DESC
%token <token> DESCRIBE
%token <token> DISTINCT
%token <token> DISTRIBUTED
%token <token> DNODE
%token <token> DNODES
%token <token> DOUBLE
%token <token> DROP
%token <token> DURATION
%token <token> ELSE
%token <token> ENABLE
%token <token> ENCRYPTIONS
%token <token> ENCRYPT_ALGORITHM
%token <token> ENCRYPT_KEY
%token <token> END
%token <token> EXISTS
%token <token> EXPIRED_TIME
%token <token> EXPLAIN
%token <token> EVENT_TYPE
%token <token> EVENT_WINDOW
%token <token> EVERY
%token <token> FILE
%token <token> FILL
%token <token> FILL_HISTORY
%token <token> FILL_HISTORY_FIRST
%token <token> FIRST
%token <token> FLOAT
%token <token> FLUSH
%token <token> FROM
%token <token> FOR
%token <token> FORCE
%token <token> FORCE_OUTPUT
%token <token> FUNCTION
%token <token> FUNCTIONS
%token <token> GEOMETRY
%token <token> GRANT
%token <token> GRANTS
%token <token> INFO
%token <token> INFORMATION_SCHEMA
%token <token> LOGS
%token <token> MACHINES
%token <token> GROUP
%token <token> HASH_JOIN
%token <token> HAVING
%token <token> HOST
%token <token> IF
%token <token> IFNULL
%token <token> IGNORE
%token <token> IGNORE_DISORDER
%token <token> IGNORE_NODATA_TRIGGER
%token <token> IMPORT
%token <token> INDEX
%token <token> INDEXES
%token <token> INSERT
%token <token> INSTANCES
%token <token> INT
%token <token> INTEGER
%token <token> INTERVAL
%token <token> INTO
%token <token> ISNOTNULL
%token <token> ISNULL
%token <token> JSON
%token <token> KEEP
%token <token> KEY
%token <token> KILL
%token <token> LANGUAGE
%token <token> LAST
%token <token> LAST_ROW
%token <token> LEADER
%token <token> LEADING
%token <token> LICENCES
%token <token> LIMIT
%token <token> LINEAR
%token <token> LOCAL
%token <token> LOCK
%token <token> LOW_LATENCY_CALC
%token <token> MASK
%token <token> MAXROWS
%token <token> MAX_DELAY
%token <token> BWLIMIT
%token <token> MERGE
%token <token> META
%token <token> ONLY
%token <token> MINROWS
%token <token> MNODE
%token <token> MNODES
%token <token> MODIFY
%token <token> MODULES
%token <token> MOUNT
%token <token> MOUNTS
%token <token> NORMAL
%token <token> NCHAR
%token <token> NEXT
%token <token> NEAR
%token <token> NODE
%token <token> NODES
%token <token> NONE
%token <token> NOW
%token <token> NOTIFY_OPTIONS
%token <token> NO_BATCH_SCAN
%token <token> NULL
%token <token> NULL_F
%token <token> NULLIF
%token <token> NULLS
%token <token> NVL
%token <token> NVL2
%token <token> OFFSET
%token <token> ORDER
%token <token> OUTPUTTYPE
%token <token> OUTPUT_SUBTABLE
%token <token> OWNED
%token <token> PAGES
%token <token> PAGESIZE
%token <token> PARA_TABLES_SORT
%token <token> PERFORMANCE_SCHEMA
%token <token> PARTITION
%token <token> PARTITION_FIRST
%token <token> PASS
%token <token> PORT
%token <token> POSITION
%token <token> PPS
%token <token> PRIMARY
%token <token> PRE_FILTER
%token <token> COMPOSITE
%token <token> PRECISION
%token <token> PREV
%token <token> PRIVILEGE
%token <token> PRIVILEGES
%token <token> QNODE
%token <token> QNODES
%token <token> QTIME
%token <token> QUERIES
%token <token> QUERY
%token <token> PI
%token <token> RAND
%token <token> RANGE
%token <token> RATIO
%token <token> PERIOD
%token <token> READ
%token <token> RECURSIVE
%token <token> REDISTRIBUTE
%token <token> RENAME
%token <token> RELOAD
%token <token> RECALCULATE
%token <token> REPLACE
%token <token> REPLICAS
%token <token> REPLICA
%token <token> RESET
%token <token> RESTORE
%token <token> RETENTION
%token <token> RETENTIONS
%token <token> REVOKE
%token <token> ROLE
%token <token> ROLES
%token <token> ROLLUP
%token <token> RSMA
%token <token> RSMAS
%token <token> SCHEMALESS
%token <token> SCORES
%token <token> SECURITY
%token <token> SELECT
%token <token> SERVER_STATUS
%token <token> SERVER_VERSION
%token <token> SESSION
%token <token> SELF
%token <token> SET
%token <token> SHOW
%token <token> SINGLE_STABLE
%token <token> SKIP_TSMA
%token <token> SLIDING
%token <token> SLIMIT
%token <token> SMA
%token <token> SMALLDATA_TS_SORT
%token <token> SMALLINT
%token <token> SNODE
%token <token> SNODES
%token <token> SORT_FOR_GROUP
%token <token> SOFFSET
%token <token> SPLIT
%token <token> STABLE
%token <token> STABLES
%token <token> START
%token <token> STATE
%token <token> STATE_WINDOW
%token <token> STOP
%token <token> STORAGE
%token <token> STREAM
%token <token> STREAMS
%token <token> STREAM_OPTIONS
%token <token> STRICT
%token <token> STT_TRIGGER
%token <token> SUBSCRIBE
%token <token> SUBSCRIPTIONS
%token <token> SUBSTR
%token <token> SUBSTRING
%token <token> SYSINFO
%token <token> SYSTEM
%token <token> TABLE
%token <token> TABLES
%token <token> TABLE_PREFIX
%token <token> TABLE_SUFFIX
%token <token> TAG
%token <token> TAGS
%token <token> TBNAME
%token <token> THEN
%token <token> TIMESTAMP
%token <token> TIMEZONE
%token <token> TINYINT
%token <token> TO
%token <token> TODAY
%token <token> TOKEN
%token <token> TOPIC
%token <token> TOPICS
%token <token> TOTP
%token <token> TOTPSEED
%token <token> TOTP_SECRET
%token <token> TRAILING
%token <token> TRANSACTION
%token <token> TRANSACTIONS
%token <token> TRANS
%token <token> TRIGGER
%token <token> TRIM
%token <token> TROWS
%token <token> TSDB_PAGESIZE
%token <token> TSERIES
%token <token> TSMA
%token <token> TSMAS
%token <token> TTL
%token <token> UNLIMITED
%token <token> UNLOCK
%token <token> UNSAFE
%token <token> UNSIGNED
%token <token> UNTREATED
%token <token> UPDATE
%token <token> USE
%token <token> USER
%token <token> USERS
%token <token> USING
%token <token> VALUE
%token <token> VALUE_F
%token <token> VALUES
%token <token> VARIABLE
%token <token> VARCHAR
%token <token> VARIABLES
%token <token> VERBOSE
%token <token> VGROUP
%token <token> VGROUPS
%token <token> VIEW
%token <token> VIEWS
%token <token> VIRTUAL
%token <token> VNODE
%token <token> VNODES
%token <token> VTABLE
%token <token> WAL
%token <token> WAL_FSYNC_PERIOD
%token <token> WAL_LEVEL
%token <token> WAL_RETENTION_PERIOD
%token <token> WAL_RETENTION_SIZE
%token <token> WAL_ROLL_PERIOD
%token <token> WAL_SEGMENT_SIZE
%token <token> WATERMARK
%token <token> WHEN
%token <token> WHERE
%token <token> WINDOW_OPEN
%token <token> WINDOW_CLOSE
%token <token> WIN_OPTIMIZE_BATCH
%token <token> WIN_OPTIMIZE_SINGLE
%token <token> WITH
%token <token> WRITE
%token <token> ROWTS
%token <token> IROWTS
%token <token> IROWTS_ORIGIN
%token <token> ISFILLED
%token <token> QDURATION
%token <token> QEND
%token <token> QSTART
%token <token> QTAGS
%token <token> WDURATION
%token <token> WEND
%token <token> WSTART
%token <token> FLOW
%token <token> FHIGH
%token <token> FROWTS
%token <token> TPREV_TS
%token <token> TCURRENT_TS
%token <token> TNEXT_TS
%token <token> TWSTART
%token <token> TWEND
%token <token> TWDURATION
%token <token> TWROWNUM
%token <token> TPREV_LOCALTIME
%token <token> TNEXT_LOCALTIME
%token <token> TLOCALTIME
%token <token> TGRPID
%token <token> ALIVE
%token <token> VARBINARY
%token <token> SS_CHUNKPAGES
%token <token> SS_KEEPLOCAL
%token <token> SS_COMPACT
%token <token> SSMIGRATE
%token <token> SSMIGRATES
%token <token> KEEP_TIME_OFFSET
%token <token> ARBGROUPS
%token <token> IS_IMPORT
%token <token> DISK_INFO
%token <token> AUTO
%token <token> MEDIUMBLOB
%token <token> BLOB
%token <token> COLS
%token <token> NOTIFY
%token <token> NOTIFY_HISTORY
%token <token> ASSIGN
%token <token> TRUE_FOR
%token <token> VTABLES
%token <token> META_ONLY
%token <token> IMPROWTS
%token <token> IMPMARK
%token <token> SCAN
%token <token> SCANS
%token <token> SVR_KEY
%token <token> DB_KEY
%token <token> ANOMALYMARK
%token <token> CHANGEPASS
%token <token> SESSION_PER_USER
%token <token> CONNECT_TIME
%token <token> CONNECT_IDLE_TIME
%token <token> CALL_PER_SESSION
%token <token> FAILED_LOGIN_ATTEMPTS
%token <token> PASSWORD_LIFE_TIME
%token <token> PASSWORD_REUSE_TIME
%token <token> PASSWORD_REUSE_MAX
%token <token> PASSWORD_LOCK_TIME
%token <token> PASSWORD_GRACE_TIME
%token <token> INACTIVE_ACCOUNT_TIME
%token <token> ALLOW_TOKEN_NUM
%token <token> NOT_ALLOW_HOST
%token <token> ALLOW_DATETIME
%token <token> NOT_ALLOW_DATETIME
%token <token> ENCRYPT_ALGORITHMS
%token <token> ENCRYPT_STATUS
%token <token> ALGR_NAME
%token <token> ALGR_TYPE
%token <token> ENCRYPT_ALGR
%token <token> OSSL_ALGR_NAME
%token <token> PROVIDER
%token <token> EXTRA_INFO
%token <token> TOKENS
%token <token> IS_AUDIT
%token <token> VNODE_PER_CALL
%token <token> XNODE
%token <token> XNODES
%token <token> DRAIN
%token <token> REBALANCE

%token <token> NK_IPTOKEN
%token <token> COLON
%token <token> COMMA

%left <token> OR
%left <token> AND
%left <token> UNION ALL MINUS EXCEPT INTERSECT
%left <token> NK_BITAND NK_BITOR NK_LSHIFT NK_RSHIFT NK_PH
%left <token> NK_LT NK_GT NK_LE NK_GE NK_EQ NK_NE LIKE MATCH NMATCH REGEXP CONTAINS BETWEEN IS IN
%left <token> NK_PLUS NK_MINUS
%left <token> NK_STAR NK_SLASH NK_REM
%left <token> NK_CONCAT

%right <token> NOT
%left <token> NK_ARROW
%right <token> INNER LEFT RIGHT FULL OUTER SEMI ANTI ASOF WINDOW JOIN ON WINDOW_OFFSET JLIMIT
%right NK_RP

%type <token> option_value
%type <token> user_enabled
%type <userOptions> user_option
%type <ipRangeList> ip_range_list
%type <dateTimeRangeList> datetime_range_list
%type <userOptions> create_user_option
%type <userOptions> create_user_options
%type <userOptions> create_user_options_opt
%type <userOptions> alter_user_option
%type <userOptions> alter_user_options

%type <tokenOptions> token_option
%type <tokenOptions> token_options
%type <tokenOptions> token_options_opt

%type <strs> privileges priv_type_list
%type <str> priv_type priv_type_tbl_dml
%type <i32> event_types event_type_list notify_on_opt notify_options_opt notify_options_list notify_option

%type <token> dnode_endpoint
%type <bool> force_opt
%type <bool> unsafe_opt

%type <BnodeOptions> bnode_options

%type <tokens> dnode_list
%type <token> on_vgroup_id
%type <stmt> multi_create_clause
%type <stmt> create_subtable_clause
%type <columnDef> column_stream_def
%type <columnDef> column_def tag_def
%type <columnDefs> column_def_list tag_def_list column_stream_def_list
%type <columnDefs> tags_def tags_def_opt
%type <columnOption> column_options
%type <columnOption> stream_col_options
%type <DatabaseOptions> db_options
%type <alterDBOptions> alter_db_options
%type <dbOptionKV> alter_db_option
%type <tableOptions> table_options
%type <bytes2D> duration_list rollup_func_list integer_list variable_list
%type <bytes2D> signed_duration_list
%type <token> rollup_func_name
%type <bool> not_exists_opt exists_opt force_opt
%type <bool> ignore_opt
%type <bytes2D> retention_list
%type <bytes> retention
%type <str> role_name user_name
%type <token> general_name
%type <str> stream_name
%type <strs> stream_name_list
%type <strs> func_list tsma_func_list
%type <str> xnode_endpoint
%type <str> xnode_resource_type xnode_task_source xnode_task_sink
%type <str> mount_name full_table_name full_view_name full_stream_name full_rsma_name full_tsma_name full_index_name
%type <str> rsma_name tsma_name index_name
%type <str> notification_opt trigger_col_name stream_partition_item stream_tags_def trigger_option trigger_table_opt trigger_type stream_trigger stream_outtable_opt index_options sma_stream_opt
%type <str> priv_level priv_level_opt
%type <str> column_ref column_name_triplet
%type <str> multi_drop_clause drop_table_clause alter_table_clause
%type <str> start_opt end_opt
%type <showTableScope> table_kind_db_name_cond_opt
%type <str> db_kind_opt
%type <str> like_pattern_opt db_name_cond_opt from_db_opt table_name_cond time_point
%type <strs> tag_list_opt
%type <recalcRange> recalculate_range
%type <strs> rsma_func_list
%type <bool> meta_only
%type <str> with_task_options_opt xnode_task_options xnode_task_opt_v xnode_task_from_opt xnode_task_to_opt
%type <strs> alter_table_options column_tag_value_list tags_literal_list
%type <str> alter_table_option column_tag_value tags_literal
%type <str> specific_column_ref col_name col_name_with_mask
%type <strs> specific_column_ref_list column_ref_list col_name_list specific_cols_opt specific_cols_with_mask_opt col_name_ex_list column_name_opt column_name_unit
%type <strs> notify_url_list stream_partition_by_opt stream_partition_list stream_tags_def_list stream_tags_def_opt trigger_option_list trigger_options_opt
%type <str> tag_item
%type <str> table_kind
%type <bool> with_opt
%type <i32> speed_opt
%type <i32> bufsize_opt
%type <bool> or_replace_opt
%type <bool> agg_func_opt

// SELECT statement types
%type <selectStmt> query_expression query_simple query_specification union_query_expression
%type <selectStmt> query_simple_or_subquery query_or_subquery subquery
%type <selectStmt> as_subquery_opt
%type <expr> expression common_expression expr_or_subquery boolean_value_expression boolean_primary
%type <expr> predicate literal column_reference function_expression case_when_expression
%type <expr> if_expression pseudo_column literal_func rand_func in_predicate_value star_func_para cols_func_expression
%type <expr> signed_literal
%type <whenThenList> when_then_list
%type <whenThenList> when_then_expr
%type <expr> case_when_else_opt
%type <exprList> expression_list literal_list select_list select_item star_func_para_list other_para_list cols_func_para_list cols_func_expression_list
%type <columnExpr> column_name table_name db_name column_alias table_alias cols_func substr_func noarg_func
%type <columnList> column_name_list
%type <tableExpr> table_reference table_primary table_reference_list joined_table parenthesized_joined_table from_clause_opt
%type <joinExpr> inner_joined outer_joined semi_joined anti_joined asof_joined win_joined
%type <orderByList> order_by_clause_opt sort_specification_list
%type <orderByExpr> sort_specification
%type <windowExpr> twindow_clause_opt
%type <windowExpr> interval_opt sliding_expr
%type <countWindowArgs> count_window_args
%type <stateWindowOpt> state_window_opt
%type <groupByExpr> group_by_clause_opt group_by_list
%type <expr> having_clause_opt where_clause_opt partition_by_clause_opt search_condition join_on_clause with_clause_opt
%type <expr> output_subtable_opt
%type <exprList> search_condition_list
%type <exprList> partition_list
%type <expr> partition_item
%type <expr> range_opt
%type <expr> window_offset_clause window_offset_literal
%type <fillExpr> fill_opt interp_fill_opt fill_mode fill_position_mode fill_position_mode_extension interp_fill_mode fill_value
%type <limitExpr> limit_clause_opt slimit_clause_opt jlimit_clause_opt
%type <literal> duration_literal interval_sliding_duration_literal sliding_opt offset_opt extend_literal zeroth_literal signed_integer
%type <token> signed signed_float
%type <token> language_opt
%type <literal> true_for_opt every_opt
%type <bool> set_quantifier_opt tag_mode_opt ordering_specification_opt null_ordering_opt
%type <expr> join_on_clause_opt
%type <token> compare_op in_op
%type <token> unsigned_integer
%type <token> signed_variable
%type <columnExpr> alias_opt function_name star_func trim_specification_type type_name type_name_default_len
%type <columnExpr> sma_func_name
%type <str> func
%type <hintOption> hint_list
%type <str> view_name topic_name with_meta cgroup_name
%type <bool> analyze_opt
%type <explainOptions> explain_options

%type <stmt> cmd insert_query
%start any_command

%%

any_command:
  cmd
  {
    setParseTree(yylex, $1)
  }
  | cmd NK_SEMI
  {
    setParseTree(yylex, $1)
  }

not_exists_opt:
/*empty*/
{
  $$ = false
}
| IF NOT EXISTS
{
  $$ = true
}

exists_opt:
/*empty*/
{
  $$ = false
}
| IF EXISTS
{
  $$ = true
}

or_replace_opt:
/*empty*/
{
  $$ = false
}
| OR REPLACE
{
  $$ = true
}

agg_func_opt:
/*empty*/
{
  $$ = false
}
| AGGREGATE
{
  $$ = true
}

bufsize_opt:
/*empty*/
{
  $$ = 0
}
| BUFSIZE NK_INTEGER
{
  $$ = tokenToInt32($2)
}

language_opt:
/*empty*/
{
  $$ = Token{}
}
| LANGUAGE NK_STRING
{
  $$ = $2
}

role_name:
NK_ID
{
  $$ = string($1.Bytes)
}

user_name:
NK_ID
{
  $$ = string($1.Bytes)
}

general_name:
  NK_ID
  {
    $$ = $1
  }

mount_name:
  NK_ID
  {
    $$ = string($1.Bytes)
  }

db_kind_opt:
/* empty */
{
  $$ = ""
}
| USER
{
  $$ = "user"
}
| SYSTEM
{
  $$ = "system"
}

table_kind_db_name_cond_opt:
/* empty */
{
  $$ = ShowTableScope{}
}
| table_kind
{
  $$ = ShowTableScope{TableKind: $1}
}
| db_name NK_DOT
{
  $$ = ShowTableScope{DBName: $1}
}
| table_kind db_name NK_DOT
{
  $$ = ShowTableScope{TableKind: $1, DBName: $2}
}

table_kind:
NORMAL
{
  $$ = "normal"
}
| CHILD
{
  $$ = "child"
}
| VIRTUAL
{
  $$ = "virtual"
}

db_name_cond_opt:
/* empty */
{
  $$ = ""
}
| db_name NK_DOT
{
  $$ = $1
}

like_pattern_opt:
/* empty */
{
  $$ = ""
}
| LIKE NK_STRING
{
  $$ = string($2.Bytes)
}

from_db_opt:
/* empty */
{
  $$ = ""
}
| FROM db_name
{
  $$ = $2
}

table_name_cond:
table_name
{
  $$ = $1
}

tag_list_opt:
  /* empty */
  {
    $$ = nil
  }
  | tag_item
  {
    $$ = []string{$1}
  }
  | tag_list_opt NK_COMMA tag_item
  {
    $$ = append($1, $3)
  }

tag_item:
  TBNAME
  {
    $$ = "tbname"
  }
  | QTAGS
  {
    $$ = "qtags"
  }
  | column_name
  {
    $$ = $1
  }
  | column_name column_alias
  {
    $$ = $1 + " " + $2
  }
  | column_name AS column_alias
  {
    $$ = $1 + " as " + $3
  }

specific_cols_opt:
/* empty */
{
  $$ = nil
}
| NK_LP col_name_list NK_RP
{
  $$ = $2
}

specific_cols_with_mask_opt:
/* empty */
{
  $$ = nil
}
| NK_LP col_name_ex_list NK_RP
{
  $$ = $2
}

col_name:
  column_name
  {
    $$ = $1
  }
  | TBNAME
  {
    $$ = "tbname"
  }

col_name_with_mask:
  MASK NK_LP column_name NK_RP
  {
    $$ = "mask(" + $3 + ")"
  }

col_name_ex_list:
  col_name
  {
    $$ = []string{$1}
  }
  | col_name_with_mask
  {
    $$ = []string{$1}
  }
  | col_name_ex_list NK_COMMA col_name
  {
    $$ = append($1, $3)
  }
  | col_name_ex_list NK_COMMA col_name_with_mask
  {
    $$ = append($1, $3)
  }

col_name_list:
  col_name
  {
    $$ = []string{$1}
  }
  | col_name_list NK_COMMA col_name
  {
    $$ = append($1, $3)
  }

specific_column_ref:
  column_name FROM column_ref
  {
    $$ = $1 + " from " + $3
  }

specific_column_ref_list:
  specific_column_ref
  {
    $$ = []string{$1}
  }
  | specific_column_ref_list NK_COMMA specific_column_ref
  {
    $$ = append($1, $3)
  }

column_ref_list:
column_ref
{
  $$ = []string{$1}
}
| column_ref_list NK_COMMA column_ref
{
  $$ = append($1, $3)
}

tags_literal:
  NK_STRING
  {
    $$ = "'" + strings.ReplaceAll(string($1.Bytes), "'", "''") + "'"
  }
  | NK_STRING NK_PLUS duration_literal
  {
    $$ = "'" + strings.ReplaceAll(string($1.Bytes), "'", "''") + "'" + "+" + string($3.Val.Bytes)
  }
  | NK_STRING NK_MINUS duration_literal
  {
    $$ = "'" + strings.ReplaceAll(string($1.Bytes), "'", "''") + "'" + "-" + string($3.Val.Bytes)
  }
  | NK_INTEGER
  {
    $$ = string($1.Bytes)
  }
  | NK_INTEGER NK_PLUS duration_literal
  {
    $$ = string($1.Bytes) + "+" + string($3.Val.Bytes)
  }
  | NK_INTEGER NK_MINUS duration_literal
  {
    $$ = string($1.Bytes) + "-" + string($3.Val.Bytes)
  }
  | NK_PLUS NK_INTEGER
  {
    $$ = "+" + string($2.Bytes)
  }
  | NK_PLUS NK_INTEGER NK_PLUS duration_literal
  {
    $$ = "+" + string($2.Bytes) + "+" + string($4.Val.Bytes)
  }
  | NK_PLUS NK_INTEGER NK_MINUS duration_literal
  {
    $$ = "+" + string($2.Bytes) + "-" + string($4.Val.Bytes)
  }
  | NK_MINUS NK_INTEGER
  {
    $$ = "-" + string($2.Bytes)
  }
  | NK_MINUS NK_INTEGER NK_PLUS duration_literal
  {
    $$ = "-" + string($2.Bytes) + "+" + string($4.Val.Bytes)
  }
  | NK_MINUS NK_INTEGER NK_MINUS duration_literal
  {
    $$ = "-" + string($2.Bytes) + "-" + string($4.Val.Bytes)
  }
  | NK_FLOAT
  {
    $$ = string($1.Bytes)
  }
  | NK_BIN
  {
    $$ = string($1.Bytes)
  }
  | NK_BIN NK_PLUS duration_literal
  {
    $$ = string($1.Bytes) + "+" + string($3.Val.Bytes)
  }
  | NK_BIN NK_MINUS duration_literal
  {
    $$ = string($1.Bytes) + "-" + string($3.Val.Bytes)
  }
  | NK_HEX
  {
    $$ = string($1.Bytes)
  }
  | NK_HEX NK_PLUS duration_literal
  {
    $$ = string($1.Bytes) + "+" + string($3.Val.Bytes)
  }
  | NK_HEX NK_MINUS duration_literal
  {
    $$ = string($1.Bytes) + "-" + string($3.Val.Bytes)
  }
  | NK_PLUS NK_BIN
  {
    $$ = "+" + string($2.Bytes)
  }
  | NK_PLUS NK_BIN NK_PLUS duration_literal
  {
    $$ = "+" + string($2.Bytes) + "+" + string($4.Val.Bytes)
  }
  | NK_PLUS NK_BIN NK_MINUS duration_literal
  {
    $$ = "+" + string($2.Bytes) + "-" + string($4.Val.Bytes)
  }
  | NK_MINUS NK_BIN
  {
    $$ = "-" + string($2.Bytes)
  }
  | NK_MINUS NK_BIN NK_PLUS duration_literal
  {
    $$ = "-" + string($2.Bytes) + "+" + string($4.Val.Bytes)
  }
  | NK_MINUS NK_BIN NK_MINUS duration_literal
  {
    $$ = "-" + string($2.Bytes) + "-" + string($4.Val.Bytes)
  }
  | NK_PLUS NK_HEX
  {
    $$ = "+" + string($2.Bytes)
  }
  | NK_PLUS NK_HEX NK_PLUS duration_literal
  {
    $$ = "+" + string($2.Bytes) + "+" + string($4.Val.Bytes)
  }
  | NK_PLUS NK_HEX NK_MINUS duration_literal
  {
    $$ = "+" + string($2.Bytes) + "-" + string($4.Val.Bytes)
  }
  | NK_MINUS NK_HEX
  {
    $$ = "-" + string($2.Bytes)
  }
  | NK_MINUS NK_HEX NK_PLUS duration_literal
  {
    $$ = "-" + string($2.Bytes) + "+" + string($4.Val.Bytes)
  }
  | NK_MINUS NK_HEX NK_MINUS duration_literal
  {
    $$ = "-" + string($2.Bytes) + "-" + string($4.Val.Bytes)
  }
  | NK_PLUS NK_FLOAT
  {
    $$ = "+" + string($2.Bytes)
  }
  | NK_MINUS NK_FLOAT
  {
    $$ = "-" + string($2.Bytes)
  }
  | NK_BOOL
  {
    $$ = string($1.Bytes)
  }
  | NULL
  {
    $$ = "null"
  }
  | literal_func
  {
    $$ = SQLNodeToString($1)
  }
  | literal_func NK_PLUS duration_literal
  {
    $$ = SQLNodeToString($1) + "+" + string($3.Val.Bytes)
  }
  | literal_func NK_MINUS duration_literal
  {
    $$ = SQLNodeToString($1) + "-" + string($3.Val.Bytes)
  }

tags_literal_list:
  tags_literal
  {
    $$ = []string{$1}
  }
  | tags_literal_list NK_COMMA tags_literal
  {
    $$ = append($1, $3)
  }

full_table_name:
table_name
{
  $$ = $1
}
| db_name NK_DOT table_name
{
  $$ = $1 + "." + $3
}

xnode_resource_type:
NK_ID
{
  $$ = string($1.Bytes)
}

xnode_task_opt_v:
  NK_STRING
  {
    $$ = string($1.Bytes)
  }
  | NK_INTEGER
  {
    $$ = string($1.Bytes)
  }

xnode_task_options:
  NK_ID xnode_task_opt_v
  {
    $$ = string($1.Bytes) + " " + $2
  }
  | NK_ID NK_EQ xnode_task_opt_v
  {
    $$ = string($1.Bytes) + " = " + $3
  }
  | xnode_task_options NK_ID xnode_task_opt_v
  {
    $$ = $1 + " " + string($2.Bytes) + " " + $3
  }
  | xnode_task_options NK_ID NK_EQ xnode_task_opt_v
  {
    $$ = $1 + " " + string($2.Bytes) + " = " + $4
  }
  | xnode_task_options NK_COMMA NK_ID NK_EQ xnode_task_opt_v
  {
    $$ = $1 + ", " + string($3.Bytes) + " = " + $5
  }
  | xnode_task_options AND NK_ID NK_EQ xnode_task_opt_v
  {
    $$ = $1 + " and " + string($3.Bytes) + " = " + $5
  }
  | xnode_task_options NK_COMMA NK_ID
  {
    $$ = $1 + ", " + string($3.Bytes)
  }
  | xnode_task_options AND NK_ID
  {
    $$ = $1 + " and " + string($3.Bytes)
  }
  | xnode_task_options TRIGGER NK_STRING
  {
    $$ = $1 + " trigger " + string($3.Bytes)
  }
  | xnode_task_options TRIGGER NK_EQ NK_STRING
  {
    $$ = $1 + " trigger = " + string($4.Bytes)
  }
  | xnode_task_options NK_COMMA TRIGGER NK_EQ NK_STRING
  {
    $$ = $1 + ", trigger = " + string($5.Bytes)
  }
  | xnode_task_options AND TRIGGER NK_EQ NK_STRING
  {
    $$ = $1 + " and trigger = " + string($5.Bytes)
  }

with_task_options_opt:
/* empty */
  {
    $$ = ""
  }
  | WITH xnode_task_options
  {
    $$ = $2
  }

xnode_task_source:
  NK_STRING
  {
    $$ = string($1.Bytes)
  }
  | DATABASE db_name
  {
    $$ = $2
  }
  | TOPIC topic_name
  {
    $$ = $2
  }

xnode_task_sink:
  NK_STRING
  {
    $$ = string($1.Bytes)
  }
  | DATABASE db_name
  {
    $$ = $2
  }

xnode_task_from_opt:
/* empty */
  {
    $$ = ""
  }
  | FROM xnode_task_source
  {
    $$ = $2
  }

xnode_task_to_opt:
/* empty */
  {
    $$ = ""
  }
  | TO xnode_task_sink
  {
    $$ = $2
  }

full_view_name:
  view_name
  {
    $$ = $1
  }
  | db_name NK_DOT view_name
  {
    $$ = $1 + "." + $3
  }

full_stream_name:
  stream_name
  {
    $$ = $1
  }
  | db_name NK_DOT stream_name
  {
    $$ = $1 + "." + $3
  }

full_rsma_name:
  rsma_name
  {
    $$ = $1
  }
  | db_name NK_DOT rsma_name
  {
    $$ = $1 + "." + $3
  }

full_tsma_name:
  tsma_name
  {
    $$ = $1
  }
  | db_name NK_DOT tsma_name
  {
    $$ = $1 + "." + $3
  }

full_index_name:
  index_name
  {
    $$ = $1
  }
  | db_name NK_DOT index_name
  {
    $$ = $1 + "." + $3
  }

/************************************************ command entry *************************************************/
cmd:
  CREATE USER not_exists_opt user_name PASS NK_STRING create_user_options_opt
    {
      opts := MergeUserOptions(yylex, $7, nil)
      opts.setUserOptionsPassword(yylex, $6)
      $$ = NewCreateUserStmt(yylex, $4, opts, $3)
    }
  | ALTER USER user_name alter_user_options
    {
      $$ = NewAlterUserStmt(yylex, $3, $4)
    }
  | DROP USER exists_opt user_name
    {
      $$ = NewDropUserStmt(yylex, $4, $3)
    }
  | CREATE TOKEN not_exists_opt NK_ID FROM USER user_name token_options_opt
    {
      $$ = NewCreateTokenStmt(yylex, $4, $7, $8, $3)
    }
  | ALTER TOKEN NK_ID token_options
    {
      $$ = NewAlterTokenStmt(yylex, $3, $4)
    }
  | DROP TOKEN exists_opt NK_ID
    {
      $$ = NewDropTokenStmt(yylex, $4, $3)
    }
  | CREATE ROLE not_exists_opt role_name
    {
      $$ = NewCreateRoleStmt(yylex, $3, $4)
    }
  | DROP ROLE exists_opt role_name
    {
      $$ = NewDropRoleStmt(yylex, $3, $4)
    }
  | LOCK ROLE role_name
    {
      $$ = NewAlterRoleStmt(yylex, $3, TSDB_ALTER_ROLE_LOCK, Token{Type: 1})
    }
  | UNLOCK ROLE role_name
    {
      $$ = NewAlterRoleStmt(yylex, $3, TSDB_ALTER_ROLE_LOCK, Token{Type: 0})
    }
  | GRANT ROLE role_name TO role_name
    {
      $$ = NewGrantRoleStmt(yylex, $3, $5, TSDB_ALTER_ROLE_ROLE)
    }
  | REVOKE ROLE role_name FROM role_name
    {
      $$ = NewRevokeRoleStmt(yylex, $3, $5, TSDB_ALTER_ROLE_ROLE)
    }
  | GRANT privileges priv_level_opt with_clause_opt TO user_name
    {
      name := strings.Join($2, ", ")
      privArg := int32(PRIV_TYPE_UNKNOWN)
      if len($2) > 0 {
        privArg = grantPrivArgFromName($2[0])
      }
      stmt := &GrantStmt{
        OptrType:      0,
        Principal:     $6,
        PrivilegeName: name,
        Privileges: PrivSetArgs{
          PrivArgs: privArg,
        },
        Cond: $4,
      }
      applyGrantLevel($3, stmt)
      $$ = stmt
    }
  | REVOKE privileges priv_level_opt with_clause_opt FROM user_name
    {
      name := strings.Join($2, ", ")
      privArg := int32(PRIV_TYPE_UNKNOWN)
      if len($2) > 0 {
        privArg = grantPrivArgFromName($2[0])
      }
      stmt := &GrantStmt{
        OptrType:      1,
        Principal:     $6,
        PrivilegeName: name,
        Privileges: PrivSetArgs{
          PrivArgs: privArg,
        },
        Cond: $4,
      }
      applyGrantLevel($3, stmt)
      $$ = stmt
    }
  | CREATE ENCRYPT_KEY NK_STRING
    {
      $$ = NewCreateEncryptKeyStmt(yylex, $3)
    }
  | ALTER SYSTEM SET SVR_KEY NK_STRING
    {
      $$ = NewAlterEncryptKeyStmt(yylex, 0, $5)
    }
  | ALTER SYSTEM SET DB_KEY NK_STRING
    {
      $$ = NewAlterEncryptKeyStmt(yylex, 1, $5)
    }
  | CREATE ENCRYPT_ALGR NK_STRING ALGR_NAME NK_STRING DESC NK_STRING ALGR_TYPE NK_STRING OSSL_ALGR_NAME NK_STRING
    {
      $$ = NewCreateAlgrStmt(yylex, $3, $5, $7, $9, $11)
    }
  | DROP ENCRYPT_ALGR NK_STRING
    {
      $$ = NewDropAlgrStmt(yylex, $3)
    }
  | CREATE ANODE NK_STRING
    {
      $$ = NewCreateAnodeStmt(yylex, $3)
    }
  | UPDATE ANODE NK_INTEGER
    {
      $$ = NewUpdateAnodeStmt(yylex, &$3)
    }
  | UPDATE ALL ANODES
    {
      $$ = NewUpdateAnodeStmt(yylex, nil)
    }
  | DROP ANODE NK_INTEGER
    {
      $$ = NewDropAnodeStmt(yylex, &$3, false)
    }
  | CREATE DNODE dnode_endpoint
    {
      $$ = NewCreateDnodeStmt(yylex, $3, nil)
    }
  | CREATE DNODE dnode_endpoint PORT NK_INTEGER
    {
      $$ = NewCreateDnodeStmt(yylex, $3, &$5)
    }
  | DROP DNODE NK_INTEGER force_opt
    {
      $$ = NewDropDnodeStmt(yylex, $3, $4, false)
    }
  | DROP DNODE NK_INTEGER unsafe_opt
    {
      $$ = NewDropDnodeStmt(yylex, $3, false, $4)
    }
  | DROP DNODE dnode_endpoint force_opt
    {
      $$ = NewDropDnodeStmt(yylex, $3, $4, false)
    }
  | DROP DNODE dnode_endpoint unsafe_opt
    {
      $$ = NewDropDnodeStmt(yylex, $3, false, $4)
    }
  | ALTER DNODE NK_INTEGER NK_STRING
    {
      $$ = NewAlterDnodeStmt(yylex, &$3, $4, nil)
    }
  | ALTER DNODE NK_INTEGER NK_STRING NK_STRING
    {
      $$ = NewAlterDnodeStmt(yylex, &$3, $4, &$5)
    }
  | ALTER ALL DNODES NK_STRING
    {
      $$ = NewAlterDnodeStmt(yylex, nil, $3, nil)
    }
  | ALTER ALL DNODES NK_STRING NK_STRING
    {
      $$ = NewAlterDnodeStmt(yylex, nil, $3, &$4)
    }
  | RESTORE DNODE NK_INTEGER
    {
      $$ = NewRestoreDnodeStmt(yylex, $3)
    }
  | ALTER DNODES RELOAD general_name
    {
      $$ = NewAlterDnodesReloadStmt(yylex, $4)
    }
  | ALTER CLUSTER NK_STRING
    {
      $$ = NewAlterClusterStmt(yylex, $3, nil)
    }
  | ALTER CLUSTER NK_STRING NK_STRING
    {
      $$ = NewAlterClusterStmt(yylex, $3, &$4)
    }
  | ALTER LOCAL NK_STRING
    {
      $$ = NewAlterLocalStmt(yylex, $3, nil)
    }
  | ALTER LOCAL NK_STRING NK_STRING
    {
      $$ = NewAlterLocalStmt(yylex, $3, &$4)
    }
  | CREATE QNODE ON DNODE NK_INTEGER
    {
      $$ = NewCreateComponentNodeStmt(yylex, QUERY_NODE_CREATE_QNODE_STMT, $5)
    }
  | DROP QNODE ON DNODE NK_INTEGER
    {
      $$ = NewDropComponentNodeStmt(yylex, QUERY_NODE_DROP_QNODE_STMT, $5)
    }
  | RESTORE QNODE ON DNODE NK_INTEGER
    {
      $$ = NewRestoreComponentNodeStmt(yylex, QUERY_NODE_RESTORE_QNODE_STMT, $5)
    }
  | CREATE SNODE ON DNODE NK_INTEGER
    {
      $$ = NewCreateComponentNodeStmt(yylex, QUERY_NODE_CREATE_SNODE_STMT, $5)
    }
  | DROP SNODE ON DNODE NK_INTEGER
    {
      $$ = NewDropComponentNodeStmt(yylex, QUERY_NODE_DROP_SNODE_STMT, $5)
    }
  | CREATE BNODE ON DNODE NK_INTEGER bnode_options
    {
      $$ = NewCreateBnodeStmt(yylex, $5, $6)
    }
  | DROP BNODE ON DNODE NK_INTEGER
    {
      $$ = NewDropBnodeStmt(yylex, $5)
    }
  | CREATE MNODE ON DNODE NK_INTEGER
    {
      $$ = NewCreateComponentNodeStmt(yylex, QUERY_NODE_CREATE_MNODE_STMT, $5)
    }
  | DROP MNODE ON DNODE NK_INTEGER
    {
      $$ = NewDropComponentNodeStmt(yylex, QUERY_NODE_DROP_MNODE_STMT, $5)
    }
  | RESTORE MNODE ON DNODE NK_INTEGER
    {
      $$ = NewRestoreComponentNodeStmt(yylex, QUERY_NODE_RESTORE_MNODE_STMT, $5)
    }
  | RESTORE VNODE ON DNODE NK_INTEGER
    {
      $$ = NewRestoreComponentNodeStmt(yylex, QUERY_NODE_RESTORE_VNODE_STMT, $5)
    }
  | CREATE DATABASE not_exists_opt db_name db_options
    {
      $$ = NewCreateDatabaseStmt(yylex, $3, Token{Bytes: []byte($4)}, $5)
    }
  | DROP DATABASE exists_opt db_name force_opt
    {
      $$ = NewDropDatabaseStmt(yylex, $3, Token{Bytes: []byte($4)}, $5)
    }
  | USE db_name
    {
      $$ = NewUseDatabaseStmt(yylex, Token{Bytes: []byte($2)})
    }
  | FLUSH DATABASE db_name
    {
      $$ = NewFlushDatabaseStmt(yylex, Token{Bytes: []byte($3)})
    }
  | SSMIGRATE DATABASE db_name
    {
      $$ = NewSsMigrateDatabaseStmt(yylex, Token{Bytes: []byte($3)})
    }
  | TRIM DATABASE db_name speed_opt
    {
      $$ = NewTrimDatabaseStmt(yylex, Token{Bytes: []byte($3)}, $4)
    }
  | TRIM DATABASE db_name WAL
    {
      $$ = NewTrimDatabaseWalStmt(yylex, Token{Bytes: []byte($3)})
    }
  | ALTER DATABASE db_name alter_db_options
    {
      $$ = NewAlterDatabaseStmt($3, ApplyAlterDatabaseOptions(yylex, $4))
    }
  | ROLLUP DATABASE db_name start_opt end_opt
    {
      $$ = NewRollupStmt("database", $3, $4, $5)
    }
  | ROLLUP db_name_cond_opt VGROUPS IN NK_LP integer_list NK_RP start_opt end_opt
    {
      $$ = NewRollupStmt("vgroups", $2, $8, $9)
    }
  | CREATE MOUNT not_exists_opt mount_name ON DNODE NK_INTEGER FROM NK_STRING
    {
      $$ = NewCreateMountStmt(yylex, Token{Bytes: []byte($4)}, $7, $9, $3)
    }
  | DROP MOUNT exists_opt mount_name
    {
      $$ = NewDropMountStmt(Token{Bytes: []byte($4)}, $3)
    }
  | KILL CONNECTION NK_INTEGER
    {
      $$ = NewKillStmt("connection", $3)
    }
  | KILL TRANSACTION NK_INTEGER
    {
      $$ = NewKillStmt("transaction", $3)
    }
  | KILL COMPACT NK_INTEGER
    {
      $$ = NewKillStmt("compact", $3)
    }
  | KILL RETENTION NK_INTEGER
    {
      $$ = NewKillStmt("retention", $3)
    }
  | KILL SCAN NK_INTEGER
    {
      $$ = NewKillStmt("scan", $3)
    }
  | KILL SSMIGRATE NK_INTEGER
    {
      $$ = NewKillStmt("ssmigrate", $3)
    }
  | KILL QUERY NK_STRING
    {
      $$ = NewKillStmt("query", $3)
    }
  | SHOW XNODES
  {
    $$ = NewShowStmt("xnodes")
  }
  | SHOW DNODES
  {
    $$ = NewShowStmt("dnodes")
  }
  | SHOW USERS
  {
    $$ = NewShowStmt("users")
  }
  | SHOW USERS FULL
  {
    $$ = NewShowStmt("users_full")
  }
  | SHOW USER PRIVILEGES
  {
    $$ = NewShowStmt("user_privileges")
  }
  | SHOW ROLES
  {
    $$ = NewShowStmt("roles")
  }
  | SHOW ROLE PRIVILEGES
  {
    $$ = NewShowStmt("role_privileges")
  }
  | SHOW ROLE COLUMN PRIVILEGES
  {
    $$ = NewShowStmt("role_column_privileges")
  }
  | SHOW APPS
  {
    $$ = NewShowStmt("apps")
  }
  | SHOW CONNECTIONS
  {
    $$ = NewShowStmt("connections")
  }
  | SHOW LICENCES
  {
    $$ = NewShowStmt("licences")
  }
  | SHOW GRANTS
  {
    $$ = NewShowStmt("grants")
  }
  | SHOW GRANTS FULL
  {
    $$ = NewShowStmt("grants_full")
  }
  | SHOW GRANTS LOGS
  {
    $$ = NewShowStmt("grants_logs")
  }
  | SHOW ENCRYPTIONS
  {
    $$ = NewShowStmt("encryptions")
  }
  | SHOW ENCRYPT_ALGORITHMS
  {
    $$ = NewShowStmt("encrypt_algorithms")
  }
  | SHOW ENCRYPT_STATUS
  {
    $$ = NewShowStmt("encrypt_status")
  }
  | SHOW ACCOUNTS
  {
    $$ = NewShowStmt("accounts")
  }
  | SHOW QUERIES
  {
    $$ = NewShowStmt("queries")
  }
  | SHOW SCORES
  {
    $$ = NewShowStmt("scores")
  }
  | SHOW TOPICS
  {
    $$ = NewShowStmt("topics")
  }
  | SHOW CONSUMERS
  {
    $$ = NewShowStmt("consumers")
  }
  | SHOW SUBSCRIPTIONS
  {
    $$ = NewShowStmt("subscriptions")
  }
  | SHOW TOKENS
  {
    $$ = NewShowStmt("tokens")
  }
  | SHOW SNODES
  {
    $$ = NewShowStmt("snodes")
  }
  | SHOW ANODES
  {
    $$ = NewShowStmt("anodes")
  }
  | SHOW ANODES FULL
  {
    $$ = NewShowStmt("anodes_full")
  }
  | SHOW ARBGROUPS
  {
    $$ = NewShowStmt("arbgroups")
  }
  | SHOW BNODES
  {
    $$ = NewShowStmt("bnodes")
  }
  | SHOW CLUSTER
  {
    $$ = NewShowStmt("cluster")
  }
  | SHOW CLUSTER MACHINES
  {
    $$ = NewShowStmt("cluster_machines")
  }
  | SHOW COMPACTS
  {
    $$ = NewShowStmt("compacts")
  }
  | SHOW FUNCTIONS
  {
    $$ = NewShowStmt("functions")
  }
  | SHOW MNODES
  {
    $$ = NewShowStmt("mnodes")
  }
  | SHOW MOUNTS
  {
    $$ = NewShowStmt("mounts")
  }
  | SHOW QNODES
  {
    $$ = NewShowStmt("qnodes")
  }
  | SHOW SCANS
  {
    $$ = NewShowStmt("scans")
  }
  | SHOW SSMIGRATES
  {
    $$ = NewShowStmt("ssmigrates")
  }
  | SHOW TRANSACTIONS
  {
    $$ = NewShowStmt("transactions")
  }
  | SHOW VNODES
  {
    $$ = NewShowStmt("vnodes")
  }
  | SHOW VNODES ON DNODE NK_INTEGER
  {
    $$ = NewShowStmtWithID("vnodes", tokenToInt32($5))
  }
  | SHOW TRANSACTION NK_INTEGER
  {
    $$ = NewShowStmtWithID("transaction", tokenToInt32($3))
  }
  | SHOW SCAN NK_INTEGER
  {
    $$ = NewShowStmtWithID("scan", tokenToInt32($3))
  }
  | SHOW COMPACT NK_INTEGER
  {
    $$ = NewShowStmtWithID("compact", tokenToInt32($3))
  }
  | SHOW RETENTION NK_INTEGER
  {
    $$ = NewShowStmtWithID("retention", tokenToInt32($3))
  }
  | SHOW CLUSTER ALIVE
  {
    $$ = NewShowStmt("cluster_alive")
  }
  | SHOW db_kind_opt DATABASES
  {
    $$ = NewShowDatabasesStmt($2)
  }
  | SHOW table_kind_db_name_cond_opt TABLES like_pattern_opt
  {
    $$ = NewShowStmtWithTableScope("tables", $2, $4)
  }
  | SHOW table_kind_db_name_cond_opt STABLES like_pattern_opt
  {
    $$ = NewShowStmtWithTableScope("stables", $2, $4)
  }
  | SHOW table_kind_db_name_cond_opt VTABLES like_pattern_opt
  {
    $$ = NewShowStmtWithTableScope("vtables", $2, $4)
  }
  | SHOW db_name_cond_opt STREAMS
  {
    $$ = NewShowStmtWithDB("streams", $2)
  }
  | SHOW db_name_cond_opt VGROUPS
  {
    $$ = NewShowStmtWithDB("vgroups", $2)
  }
  | SHOW VARIABLES like_pattern_opt
  {
    $$ = NewShowStmtWithPattern("variables", $3)
  }
  | SHOW LOCAL VARIABLES like_pattern_opt
  {
    $$ = NewShowStmtWithPattern("local_variables", $4)
  }
  | SHOW CLUSTER VARIABLES like_pattern_opt
  {
    $$ = NewShowStmtWithPattern("variables", $4)
  }
  | SHOW INSTANCES like_pattern_opt
  {
    $$ = NewShowStmtWithPattern("instances", $3)
  }
  | SHOW db_name_cond_opt ALIVE
  {
    $$ = NewShowStmtWithDB("alive", $2)
  }
  | SHOW db_name_cond_opt VIEWS like_pattern_opt
  {
    $$ = NewShowStmtWithDBPattern("views", $2, $4)
  }
  | SHOW db_name_cond_opt DISK_INFO
  {
    $$ = NewShowStmtWithDB("disk_info", $2)
  }
  | SHOW db_name_cond_opt RSMAS
  {
    $$ = NewShowStmtWithDB("rsmas", $2)
  }
  | SHOW db_name_cond_opt RETENTIONS
  {
    $$ = NewShowStmtWithDB("retentions", $2)
  }
  | SHOW db_name_cond_opt TSMAS
  {
    $$ = NewShowStmtWithDB("tsmas", $2)
  }
  | SHOW DNODE NK_INTEGER VARIABLES like_pattern_opt
  {
    $$ = NewShowStmtWithIDPattern("dnode_variables", tokenToInt32($3), $5)
  }
  | SHOW INDEXES FROM table_name_cond from_db_opt
  {
    $$ = NewShowStmtWithTableDB("indexes", $4, $5)
  }
  | SHOW INDEXES FROM db_name NK_DOT table_name
  {
    $$ = NewShowStmtWithTableDB("indexes", $6, $4)
  }
  | SHOW TAGS FROM table_name_cond from_db_opt
  {
    $$ = NewShowStmtWithTableDB("tags", $4, $5)
  }
  | SHOW TAGS FROM db_name NK_DOT table_name
  {
    $$ = NewShowStmtWithTableDB("tags", $6, $4)
  }
  | SHOW TABLE TAGS tag_list_opt FROM table_name_cond from_db_opt
  {
    $$ = NewShowStmtWithTableDBTags("table_tags", $6, $7, $4)
  }
  | SHOW TABLE TAGS tag_list_opt FROM db_name NK_DOT table_name
  {
    $$ = NewShowStmtWithTableDBTags("table_tags", $8, $6, $4)
  }
  | SHOW TABLE DISTRIBUTED full_table_name
  {
    $$ = NewShowStmtWithObject("table_distributed", $4)
  }
  | SHOW CREATE DATABASE db_name
  {
    $$ = NewShowStmtWithObject("show_create_database", $4)
  }
  | SHOW CREATE TABLE full_table_name
  {
    $$ = NewShowStmtWithObject("show_create_table", $4)
  }
  | SHOW CREATE VTABLE full_table_name
  {
    $$ = NewShowStmtWithObject("show_create_vtable", $4)
  }
  | SHOW CREATE STABLE full_table_name
  {
    $$ = NewShowStmtWithObject("show_create_stable", $4)
  }
  | SHOW CREATE VIEW full_table_name
  {
    $$ = NewShowStmtWithObject("show_create_view", $4)
  }
  | SHOW CREATE RSMA full_table_name
  {
    $$ = NewShowStmtWithObject("show_create_rsma", $4)
  }
  | SHOW XNODE xnode_resource_type
  {
    $$ = NewShowStmtWithObject("xnode", $3)
  }

  | BALANCE VGROUP
    {
      $$ = NewBalanceVgroupStmt()
    }
  | BALANCE VGROUP LEADER DATABASE db_name
    {
      $$ = NewBalanceVgroupLeaderByDBStmt($5)
    }
  | BALANCE VGROUP LEADER on_vgroup_id
    {
      $$ = NewBalanceVgroupLeaderByOptionalID($4)
    }
  | ASSIGN LEADER FORCE
    {
      $$ = NewAssignLeaderStmt()
    }
  | ALTER VGROUP NK_INTEGER SET KEEP NK_INTEGER
    {
      $$ = NewAlterVgroupKeepStmt($3, $6)
    }
  | MERGE VGROUP NK_INTEGER NK_INTEGER
    {
      $$ = NewMergeVgroupStmt($3, $4)
    }
  | SPLIT VGROUP NK_INTEGER force_opt
    {
      $$ = NewSplitVgroupStmt($3, $4)
    }
  | REDISTRIBUTE VGROUP NK_INTEGER dnode_list
    {
      $$ = NewRedistributeVgroupStmt($3, $4)
    }
  | CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options
    {
      $$ = &CreateTableStmt{
        TableName:    tableNameFromFullName($4),
        IgnoreExists: $3,
        Columns:      $6,
        Tags:         $8,
        Options:      $9,
      }
    }
  | CREATE TABLE not_exists_opt USING full_table_name NK_LP tag_list_opt NK_RP FILE NK_STRING
    {
      $$ = NewCreateSubTableFromFileStmt($3, $5, $7, string($10.Bytes))
    }
  | CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options
    {
      $$ = &CreateTableStmt{
        TableName:    tableNameFromFullName($4),
        IgnoreExists: $3,
        Columns:      $6,
        Tags:         $8,
        Options:      $9,
        IsStable:     true,
      }
    }
  | CREATE VTABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP
    {
      $$ = &CreateTableStmt{
        TableName:    tableNameFromFullName($4),
        IgnoreExists: $3,
        Columns:      $6,
        Tags:         nil,
        IsVTable:     true,
      }
    }
  | CREATE VTABLE not_exists_opt full_table_name USING full_table_name specific_cols_opt TAGS NK_LP tags_literal_list NK_RP
    {
      $$ = NewCreateVSubTableStmt($3, $4, $6, $7, nil, $10)
    }
  | CREATE VTABLE not_exists_opt full_table_name NK_LP specific_column_ref_list NK_RP USING full_table_name specific_cols_opt TAGS NK_LP tags_literal_list NK_RP
    {
      $$ = NewCreateVSubTableStmt($3, $4, $9, $10, $6, $13)
    }
  | CREATE VTABLE not_exists_opt full_table_name NK_LP column_ref_list NK_RP USING full_table_name specific_cols_opt TAGS NK_LP tags_literal_list NK_RP
    {
      $$ = NewCreateVSubTableStmt($3, $4, $9, $10, $6, $13)
    }
  | CREATE TABLE multi_create_clause
    {
      $$ = $3
    }
  | DROP TABLE with_opt multi_drop_clause
    {
      $$ = NewDropTableStmt("table", $3, $4)
    }
  | DROP STABLE with_opt exists_opt full_table_name
    {
      $$ = NewDropTableStmt("stable", $3, makeDropTableEntryText($4, $5))
    }
  | DROP VTABLE with_opt exists_opt full_table_name
    {
      $$ = NewDropTableStmt("vtable", $3, makeDropTableEntryText($4, $5))
    }
  | ALTER TABLE alter_table_clause
    {
      $$ = NewAlterTableStmt("table", $3)
    }
  | ALTER STABLE alter_table_clause
    {
      $$ = NewAlterTableStmt("stable", $3)
    }
  | ALTER VTABLE alter_table_clause
    {
      $$ = NewAlterTableStmt("vtable", $3)
    }
  | query_or_subquery
      {
      $$ = $1
    }
  | EXPLAIN analyze_opt explain_options query_or_subquery
    {
      $$ = &ExplainStmt{Analyze: $2, Options: $3, Target: $4}
    }
  | EXPLAIN analyze_opt explain_options insert_query
    {
      $$ = &ExplainStmt{Analyze: $2, Options: $3, Target: $4}
    }
  | CREATE or_replace_opt VIEW full_view_name AS query_or_subquery
    {
      $$ = &CreateViewStmt{Replace: $2, Name: $4, Query: $6}
    }
  | DROP VIEW exists_opt full_view_name
    {
      $$ = NewDropViewStmt($4, $3)
    }
  | CREATE or_replace_opt agg_func_opt FUNCTION not_exists_opt function_name AS NK_STRING OUTPUTTYPE type_name bufsize_opt language_opt
    {
      $$ = NewCreateFunctionStmt($2, $3, $5, $6, $8, $10, $11, $12)
    }
  | DROP FUNCTION exists_opt function_name
    {
      $$ = NewDropFunctionStmt($4, $3)
    }
  | CREATE RSMA not_exists_opt rsma_name ON full_table_name rsma_func_list INTERVAL NK_LP signed_duration_list NK_RP
    {
      $$ = NewCreateRSMAStmt($3, $4, $6, $7, bytes2DToStrings($10))
    }
  | CREATE TSMA not_exists_opt tsma_name ON full_table_name tsma_func_list INTERVAL NK_LP duration_literal NK_RP
    {
      $$ = NewCreateTSMAStmt($3, false, $4, $6, $7, string($10.Val.Bytes))
    }
  | CREATE RECURSIVE TSMA not_exists_opt tsma_name ON full_table_name INTERVAL NK_LP duration_literal NK_RP
    {
      $$ = NewCreateTSMAStmt($4, true, $5, $7, nil, string($10.Val.Bytes))
    }
  | CREATE SMA INDEX not_exists_opt col_name ON full_table_name index_options
    {
      $$ = NewCreateSMAIndexStmt($4, $5, $7, $8)
    }
  | CREATE INDEX not_exists_opt col_name ON full_table_name NK_LP col_name_list NK_RP
    {
      $$ = NewCreateIndexStmt($3, $4, $6, $8)
    }
  | DROP RSMA exists_opt full_rsma_name
    {
      $$ = NewDropNamedStmt("rsma", $4, $3)
    }
  | ALTER RSMA exists_opt full_rsma_name rsma_func_list
    {
      $$ = NewAlterNamedStmt("rsma", $4, $3, $5)
    }
  | DROP TSMA exists_opt full_tsma_name
    {
      $$ = NewDropNamedStmt("tsma", $4, $3)
    }
  | DROP INDEX exists_opt full_index_name
    {
      $$ = NewDropNamedStmt("index", $4, $3)
    }
  | DROP STREAM exists_opt stream_name_list
    {
      $$ = NewStreamStmt("drop", $4, $3, false)
    }
  | CREATE STREAM not_exists_opt full_stream_name stream_trigger stream_outtable_opt as_subquery_opt
    {
      $$ = &StreamStmt{
        Action:    "create",
        Names:     []string{$4},
        NotExists: $3,
        Trigger:   $5,
        OutTable:  $6,
        Query:     $7,
      }
    }
  | STOP STREAM exists_opt full_stream_name
    {
      $$ = NewStreamStmt("stop", []string{$4}, $3, false)
    }
  | START STREAM exists_opt ignore_opt full_stream_name
    {
      $$ = NewStreamStmt("start", []string{$5}, $3, $4)
    }
  | RECALCULATE STREAM full_stream_name recalculate_range
    {
      $$ = NewRecalculateStreamStmt($3, $4)
    }
  | DELETE FROM full_table_name where_clause_opt
    {
      $$ = NewDeleteStmt($3, $4)
    }
  | CREATE XNODE NK_STRING
    {
      $$ = NewXnodeStmt("create", "", string($3.Bytes), -1, false, "", "")
    }
  | CREATE XNODE NK_STRING USER user_name PASS NK_STRING
    {
      $$ = NewXnodeStmt("create_with_auth", "", string($3.Bytes), -1, false, $5, string($7.Bytes))
    }
  | CREATE XNODE xnode_resource_type NK_STRING with_task_options_opt
    {
      stmt := NewXnodeStmt("create_typed", $3, string($4.Bytes), -1, false, "", "")
      stmt.TaskOptions = $5
      $$ = stmt
    }
  | CREATE XNODE xnode_resource_type NK_STRING FROM xnode_task_source TO xnode_task_sink with_task_options_opt
    {
      stmt := NewXnodeStmt("create_typed_flow", $3, string($4.Bytes), -1, false, "", "")
      stmt.TaskFrom = $6
      stmt.TaskTo = $8
      stmt.TaskOptions = $9
      $$ = stmt
    }
  | CREATE XNODE xnode_resource_type ON NK_INTEGER with_task_options_opt
    {
      stmt := NewXnodeStmt("create_typed_on", $3, "", tokenToInt32($5), false, "", "")
      stmt.TaskOptions = $6
      $$ = stmt
    }
  | DROP XNODE xnode_endpoint force_opt
    {
      $$ = NewXnodeStmt("drop", "", $3, -1, $4, "", "")
    }
  | DROP XNODE FORCE xnode_endpoint
    {
      $$ = NewXnodeStmt("drop", "", $4, -1, true, "", "")
    }
  | DROP XNODE xnode_resource_type NK_STRING
    {
      $$ = NewXnodeStmt("drop_typed", $3, string($4.Bytes), -1, false, "", "")
    }
  | DROP XNODE xnode_resource_type NK_INTEGER
    {
      $$ = NewXnodeStmt("drop_typed", $3, "", tokenToInt32($4), false, "", "")
    }
  | DRAIN XNODE NK_INTEGER
    {
      $$ = NewXnodeStmt("drain", "", "", tokenToInt32($3), false, "", "")
    }
  | START XNODE xnode_resource_type NK_INTEGER
    {
      $$ = NewXnodeStmt("start", $3, "", tokenToInt32($4), false, "", "")
    }
  | START XNODE xnode_resource_type NK_STRING
    {
      $$ = NewXnodeStmt("start", $3, string($4.Bytes), -1, false, "", "")
    }
  | STOP XNODE xnode_resource_type NK_INTEGER
    {
      $$ = NewXnodeStmt("stop", $3, "", tokenToInt32($4), false, "", "")
    }
  | STOP XNODE xnode_resource_type NK_STRING
    {
      $$ = NewXnodeStmt("stop", $3, string($4.Bytes), -1, false, "", "")
    }
  | REBALANCE XNODE xnode_resource_type NK_INTEGER with_task_options_opt
    {
      stmt := NewXnodeStmt("rebalance", $3, "", tokenToInt32($4), false, "", "")
      stmt.TaskOptions = $5
      $$ = stmt
    }
  | REBALANCE XNODE xnode_resource_type where_clause_opt
    {
      $$ = NewXnodeStmt("rebalance_where", $3, "", -1, false, "", "")
    }
  | ALTER XNODE xnode_resource_type NK_INTEGER xnode_task_from_opt xnode_task_to_opt with_task_options_opt
    {
      stmt := NewXnodeStmt("alter", $3, "", tokenToInt32($4), false, "", "")
      stmt.TaskFrom = $5
      stmt.TaskTo = $6
      stmt.TaskOptions = $7
      $$ = stmt
    }
  | ALTER XNODE xnode_resource_type NK_STRING xnode_task_from_opt xnode_task_to_opt with_task_options_opt
    {
      stmt := NewXnodeStmt("alter", $3, string($4.Bytes), -1, false, "", "")
      stmt.TaskFrom = $5
      stmt.TaskTo = $6
      stmt.TaskOptions = $7
      $$ = stmt
    }
  | SCAN DATABASE db_name start_opt end_opt
    {
      $$ = NewScanStmt("database", $3, $4, $5)
    }
  | SCAN db_name_cond_opt VGROUPS IN NK_LP integer_list NK_RP start_opt end_opt
    {
      $$ = NewScanStmt("vgroups", $2, $8, $9)
    }
  | COMPACT DATABASE db_name start_opt end_opt meta_only force_opt
    {
      $$ = NewCompactStmt("database", $3, $4, $5, $6, $7)
    }
  | COMPACT db_name_cond_opt VGROUPS IN NK_LP integer_list NK_RP start_opt end_opt meta_only force_opt
    {
      $$ = NewCompactStmt("vgroups", $2, $8, $9, $10, $11)
    }
  | CREATE TOPIC not_exists_opt topic_name AS query_or_subquery
    {
      $$ = &TopicStmt{Reload: false, NotExists: $3, Name: $4, Query: $6}
    }
  | CREATE TOPIC not_exists_opt topic_name with_meta DATABASE db_name
    {
      $$ = &TopicStmt{Reload: false, NotExists: $3, Name: $4, MetaMode: $5, Database: $7}
    }
  | CREATE TOPIC not_exists_opt topic_name with_meta STABLE full_table_name where_clause_opt
    {
      $$ = &TopicStmt{Reload: false, NotExists: $3, Name: $4, MetaMode: $5, Stable: $7, Where: $8}
    }
  | RELOAD TOPIC exists_opt topic_name AS query_or_subquery
    {
      $$ = &TopicStmt{Reload: true, IfExists: $3, Name: $4, Query: $6}
    }
  | DROP TOPIC exists_opt force_opt topic_name
    {
      $$ = &TopicStmt{Drop: true, ExistsOpt: $3, Force: $4, Name: $5}
    }
  | DROP CONSUMER GROUP exists_opt force_opt cgroup_name ON topic_name
    {
      $$ = &TopicStmt{DropGroup: true, ExistsOpt: $4, Force: $5, GroupName: $6, OnTopic: $8}
    }
  | DESC full_table_name
      {
      $$ = &DescribeStmt{Table: $2}
    }
  | DESCRIBE full_table_name
      {
      $$ = &DescribeStmt{Table: $2}
    }
  | RESET QUERY CACHE
      {
      $$ = &ResetQueryCacheStmt{}
    }
  | insert_query
      {
      $$ = $1
    }

option_value:
  DEFAULT { $$ = $1}
  | UNLIMITED { $$ = $1}
  | NK_INTEGER { $$ = $1}


user_enabled:
  ACCOUNT LOCK
  {
    $$ = Token{Bytes: []byte{'0'}}
  }
  | ACCOUNT UNLOCK
  {
    $$ = Token{Bytes: []byte{'1'}}
  }
  | ENABLE NK_INTEGER
  {
    $$ = $2
  }

user_option:
  TOTPSEED NULL
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setUserOptionsTotpseed(yylex, Token{})
    $$ = opts
  }
  | TOTPSEED NK_STRING
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setUserOptionsTotpseed(yylex, $2)
    $$ = opts
  }
  | user_enabled
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setEnable(yylex, $1)
    $$ = opts
  }
  | SYSINFO NK_INTEGER
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setSysinfo(yylex, $2)
    $$ = opts
  }
  | IS_IMPORT NK_INTEGER
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setIsImport(yylex, $2)
    $$ = opts
  }
  | CREATEDB NK_INTEGER
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setCreatedb(yylex, $2)
    $$ = opts
  }
  | CHANGEPASS NK_INTEGER
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setChangepass(yylex, $2)
    $$ = opts
  }
  | SESSION_PER_USER option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setSessionPerUser(yylex, $2)
    $$ = opts
  }
  | CONNECT_TIME option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setConnectTime(yylex, $2)
    $$ = opts
  }
  | CONNECT_IDLE_TIME option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setConnectIdleTime(yylex, $2)
    $$ = opts
  }
  | CALL_PER_SESSION option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setCallPerSession(yylex, $2)
    $$ = opts
  }
  | VNODE_PER_CALL option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setVnodePerCall(yylex, $2)
    $$ = opts
  }
  | FAILED_LOGIN_ATTEMPTS option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setFailedLoginAttempts(yylex, $2)
    $$ = opts
  }
  | PASSWORD_LIFE_TIME option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setPasswordLifeTime(yylex, $2)
    $$ = opts
  }
  | PASSWORD_REUSE_TIME option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setPasswordReuseTime(yylex, $2)
    $$ = opts
  }
  | PASSWORD_REUSE_MAX option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setPasswordReuseMax(yylex, $2)
    $$ = opts
  }
  | PASSWORD_LOCK_TIME option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setPasswordLockTime(yylex, $2)
    $$ = opts
  }
  | PASSWORD_GRACE_TIME option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setPasswordGraceTime(yylex, $2)
    $$ = opts
  }
  | INACTIVE_ACCOUNT_TIME option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setInactiveAccountTime(yylex, $2)
    $$ = opts
  }
  | ALLOW_TOKEN_NUM option_value
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setAllowTokenNum(yylex, $2)
    $$ = opts
  }

ip_range_list:
  NK_STRING
  {
    $$ = AppendIpRange(yylex,nil,$1)
  }
  | ip_range_list ',' NK_STRING
  {
    $$ = AppendIpRange(yylex,$1,$3)
  }

datetime_range_list:
  NK_STRING
  {
    $$ = AppendDateTimeRange(yylex,nil,$1)
  }
  | datetime_range_list ',' NK_STRING
  {
    $$ = AppendDateTimeRange(yylex,$1,$3)
  }

create_user_option:
  HOST ip_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setHostList(yylex, $2)
    $$ = opts
  }
  | NOT_ALLOW_HOST ip_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setNotAllowHostList(yylex, $2)
    $$ = opts
  }
  | ALLOW_DATETIME datetime_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setAllowDateTimeList(yylex, $2)
    $$ = opts
  }
  | NOT_ALLOW_DATETIME datetime_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setNotAllowDateTimeList(yylex, $2)
    $$ = opts
  }

create_user_options:
  user_option
  {
    $$ = $1
  }
  | create_user_option
  {
    $$ = $1
  }
  | create_user_options user_option
  {
    $$ = MergeUserOptions(yylex, $1,$2)
  }
  | create_user_options create_user_option
  {
    $$ = MergeUserOptions(yylex, $1,$2)
  }

create_user_options_opt:
  /* empty */
  {
    $$ = nil
  }
  | create_user_options
  {
    $$ = $1
  }

alter_user_option:
  PASS NK_STRING
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setUserOptionsPassword(yylex, $2)
    $$ = opts
  }
  | ADD HOST ip_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setHostList(yylex, $3)
    $$ = opts
  }
  | ADD NOT_ALLOW_HOST ip_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setNotAllowHostList(yylex, $3)
    $$ = opts
  }
  | DROP HOST ip_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setDropHostList(yylex, $3)
    $$ = opts
  }
  | DROP NOT_ALLOW_HOST ip_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setDropNotAllowHostList(yylex, $3)
    $$ = opts
  }
  | ADD ALLOW_DATETIME datetime_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setAllowDateTimeList(yylex, $3)
    $$ = opts
  }
  | ADD NOT_ALLOW_DATETIME datetime_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setNotAllowDateTimeList(yylex, $3)
    $$ = opts
  }
  | DROP ALLOW_DATETIME datetime_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setDropAllowDateTimeList(yylex, $3)
    $$ = opts
  }
  | DROP NOT_ALLOW_DATETIME datetime_range_list
  {
    opts := MergeUserOptions(yylex, nil,nil)
    opts.setDropNotAllowDateTimeList(yylex, $3)
    $$ = opts
  }

alter_user_options:
  user_option
  {
    $$ = $1
  }
  | alter_user_option
  {
    $$ = $1
  }
  | alter_user_options user_option
  {
    $$ = MergeUserOptions(yylex, $1,$2)
  }
  | alter_user_options alter_user_option
  {
    $$ = MergeUserOptions(yylex, $1,$2)
  }

/************************************************ create/alter/drop token **********************************************/
token_option:
  PROVIDER NK_STRING
  {
    opts := MergeTokenOptions(yylex, nil, nil)
    opts.SetProvider(yylex, $2)
    $$ = opts
  }
  | ENABLE NK_INTEGER
  {
    opts := MergeTokenOptions(yylex, nil, nil)
    opts.SetEnable(yylex, $2)
    $$ = opts
  }
  | TTL NK_INTEGER
  {
    opts := MergeTokenOptions(yylex, nil, nil)
    opts.SetTTL(yylex, $2)
    $$ = opts
  }
  | EXTRA_INFO NK_STRING
  {
    opts := MergeTokenOptions(yylex, nil, nil)
    opts.SetExtraInfo(yylex, $2)
    $$ = opts
  }

token_options:
  token_option
  {
    $$ = $1
  }
  | token_options token_option
  {
    $$ = MergeTokenOptions(yylex, $1, $2)
  }

token_options_opt:
  /* empty */
  {
    $$ = nil
  }
  | token_options
  {
    $$ = $1
  }

/************************************************ create/drop role **********************************************/
with_clause_opt:
  /* empty */
  {
    $$ = nil
  }
  | WITH search_condition
  {
    $$ = $2
  }

privileges:
  priv_type_list
  {
    $$ = $1
  }

priv_type_list:
  priv_type
  {
    $$ = []string{$1}
  }
  | priv_type_list NK_COMMA priv_type
  {
    $$ = append($1, $3)
  }

priv_type:
  ALL
  {
    $$ = grantPrivNameFromArg(PRIV_CM_ALL)
  }
  | ALL PRIVILEGES
  {
    $$ = grantPrivNameFromArg(PRIV_CM_ALL)
  }
  | ALTER
  {
    $$ = grantPrivNameFromArg(PRIV_CM_ALTER)
  }
  | DROP
  {
    $$ = grantPrivNameFromArg(PRIV_CM_DROP)
  }
  | SHOW
  {
    $$ = grantPrivNameFromArg(PRIV_CM_SHOW)
  }
  | SHOW CREATE
  {
    $$ = grantPrivNameFromArg(PRIV_CM_SHOW_CREATE)
  }
  | START
  {
    $$ = grantPrivNameFromArg(PRIV_CM_START)
  }
  | STOP
  {
    $$ = grantPrivNameFromArg(PRIV_CM_STOP)
  }
  | KILL
  {
    $$ = grantPrivNameFromArg(PRIV_CM_KILL)
  }
  | RECALCULATE
  {
    $$ = grantPrivNameFromArg(PRIV_CM_RECALC)
  }
  | SUBSCRIBE
  {
    $$ = grantPrivNameFromArg(PRIV_CM_SUBSCRIBE)
  }
  | READ
  {
    $$ = "read"
  }
  | WRITE
  {
    $$ = "write"
  }
  | CREATE DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_DB_CREATE)
  }
  | DROP OWNED DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_DB_DROP_OWNED)
  }
  | USE DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_DB_USE)
  }
  | FLUSH DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_DB_FLUSH)
  }
  | COMPACT DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_DB_COMPACT)
  }
  | TRIM DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_DB_TRIM)
  }
  | ROLLUP DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_DB_ROLLUP)
  }
  | SCAN DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_DB_SCAN)
  }
  | SSMIGRATE DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_DB_SSMIGRATE)
  }
  | USE
  {
    $$ = grantPrivNameFromArg(PRIV_DB_USE)
  }
  | FLUSH
  {
    $$ = grantPrivNameFromArg(PRIV_DB_FLUSH)
  }
  | COMPACT
  {
    $$ = grantPrivNameFromArg(PRIV_DB_COMPACT)
  }
  | TRIM
  {
    $$ = grantPrivNameFromArg(PRIV_DB_TRIM)
  }
  | ROLLUP
  {
    $$ = grantPrivNameFromArg(PRIV_DB_ROLLUP)
  }
  | SCAN
  {
    $$ = grantPrivNameFromArg(PRIV_DB_SCAN)
  }
  | SSMIGRATE
  {
    $$ = grantPrivNameFromArg(PRIV_DB_SSMIGRATE)
  }
  | DROP OWNED
  {
    $$ = grantPrivNameFromArg(PRIV_DB_DROP_OWNED)
  }
  | SHOW VNODES
  {
    $$ = grantPrivNameFromArg(PRIV_SHOW_VNODES)
  }
  | SHOW VGROUPS
  {
    $$ = grantPrivNameFromArg(PRIV_SHOW_VGROUPS)
  }
  | SHOW COMPACTS
  {
    $$ = grantPrivNameFromArg(PRIV_SHOW_COMPACTS)
  }
  | SHOW RETENTIONS
  {
    $$ = grantPrivNameFromArg(PRIV_SHOW_RETENTIONS)
  }
  | SHOW SCANS
  {
    $$ = grantPrivNameFromArg(PRIV_SHOW_SCANS)
  }
  | SHOW SSMIGRATES
  {
    $$ = grantPrivNameFromArg(PRIV_SHOW_SSMIGRATES)
  }
  | CREATE TABLE
  {
    $$ = grantPrivNameFromArg(PRIV_TBL_CREATE)
  }
  | priv_type_tbl_dml
  {
    $$ = $1
  }
  | CREATE FUNCTION
  {
    $$ = grantPrivNameFromArg(PRIV_FUNC_CREATE)
  }
  | DROP FUNCTION
  {
    $$ = grantPrivNameFromArg(PRIV_FUNC_DROP)
  }
  | SHOW FUNCTIONS
  {
    $$ = grantPrivNameFromArg(PRIV_FUNC_SHOW)
  }
  | CREATE INDEX
  {
    $$ = grantPrivNameFromArg(PRIV_IDX_CREATE)
  }
  | DROP INDEX
  {
    $$ = grantPrivNameFromArg(PRIV_IDX_DROP)
  }
  | SHOW INDEXES
  {
    $$ = grantPrivNameFromArg(PRIV_IDX_SHOW)
  }
  | CREATE VIEW
  {
    $$ = grantPrivNameFromArg(PRIV_VIEW_CREATE)
  }
  | SELECT VIEW
  {
    $$ = grantPrivNameFromArg(PRIV_VIEW_SELECT)
  }
  | CREATE RSMA
  {
    $$ = grantPrivNameFromArg(PRIV_RSMA_CREATE)
  }
  | CREATE TSMA
  {
    $$ = grantPrivNameFromArg(PRIV_TSMA_CREATE)
  }
  | CREATE MOUNT
  {
    $$ = grantPrivNameFromArg(PRIV_MOUNT_CREATE)
  }
  | DROP MOUNT
  {
    $$ = grantPrivNameFromArg(PRIV_MOUNT_DROP)
  }
  | SHOW MOUNTS
  {
    $$ = grantPrivNameFromArg(PRIV_MOUNT_SHOW)
  }
  | ALTER PASS
  {
    $$ = grantPrivNameFromArg(PRIV_PASS_ALTER)
  }
  | ALTER SELF PASS
  {
    $$ = grantPrivNameFromArg(PRIV_PASS_ALTER_SELF)
  }
  | CREATE ROLE
  {
    $$ = grantPrivNameFromArg(PRIV_ROLE_CREATE)
  }
  | DROP ROLE
  {
    $$ = grantPrivNameFromArg(PRIV_ROLE_DROP)
  }
  | SHOW ROLES
  {
    $$ = grantPrivNameFromArg(PRIV_ROLE_SHOW)
  }
  | CREATE USER
  {
    $$ = grantPrivNameFromArg(PRIV_USER_CREATE)
  }
  | DROP USER
  {
    $$ = grantPrivNameFromArg(PRIV_USER_DROP)
  }
  | SET USER SECURITY INFO
  {
    $$ = grantPrivNameFromArg(PRIV_USER_SET_SECURITY)
  }
  | SET USER AUDIT INFO
  {
    $$ = grantPrivNameFromArg(PRIV_USER_SET_AUDIT)
  }
  | SET USER BASIC INFO
  {
    $$ = grantPrivNameFromArg(PRIV_USER_SET_BASIC)
  }
  | UNLOCK USER
  {
    $$ = grantPrivNameFromArg(PRIV_USER_UNLOCK)
  }
  | LOCK USER
  {
    $$ = grantPrivNameFromArg(PRIV_USER_LOCK)
  }
  | SHOW USERS
  {
    $$ = grantPrivNameFromArg(PRIV_USER_SHOW)
  }
  | CREATE AUDIT DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_AUDIT_DB_CREATE)
  }
  | DROP AUDIT DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_AUDIT_DB_DROP)
  }
  | ALTER AUDIT DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_AUDIT_DB_ALTER)
  }
  | USE AUDIT DATABASE
  {
    $$ = grantPrivNameFromArg(PRIV_AUDIT_DB_USE)
  }
  | CREATE TOKEN
  {
    $$ = grantPrivNameFromArg(PRIV_TOKEN_CREATE)
  }
  | DROP TOKEN
  {
    $$ = grantPrivNameFromArg(PRIV_TOKEN_DROP)
  }
  | ALTER TOKEN
  {
    $$ = grantPrivNameFromArg(PRIV_TOKEN_ALTER)
  }
  | SHOW TOKENS
  {
    $$ = grantPrivNameFromArg(PRIV_TOKEN_SHOW)
  }
  | UPDATE KEY
  {
    $$ = grantPrivNameFromArg(PRIV_KEY_UPDATE)
  }
  | CREATE TOTP
  {
    $$ = grantPrivNameFromArg(PRIV_TOTP_CREATE)
  }
  | DROP TOTP
  {
    $$ = grantPrivNameFromArg(PRIV_TOTP_DROP)
  }
  | UPDATE TOTP
  {
    $$ = grantPrivNameFromArg(PRIV_TOTP_UPDATE)
  }
  | GRANT PRIVILEGE
  {
    $$ = grantPrivNameFromArg(PRIV_GRANT_PRIVILEGE)
  }
  | REVOKE PRIVILEGE
  {
    $$ = grantPrivNameFromArg(PRIV_REVOKE_PRIVILEGE)
  }
  | SHOW PRIVILEGES
  {
    $$ = grantPrivNameFromArg(PRIV_SHOW_PRIVILEGES)
  }
  | CREATE NODE
  {
    $$ = grantPrivNameFromArg(PRIV_NODE_CREATE)
  }
  | DROP NODE
  {
    $$ = grantPrivNameFromArg(PRIV_NODE_DROP)
  }
  | SHOW NODES
  {
    $$ = grantPrivNameFromArg(PRIV_NODES_SHOW)
  }
  | ALTER SECURITY VARIABLE
  {
    $$ = grantPrivNameFromArg(PRIV_VAR_SECURITY_ALTER)
  }
  | ALTER AUDIT VARIABLE
  {
    $$ = grantPrivNameFromArg(PRIV_VAR_AUDIT_ALTER)
  }
  | ALTER SYSTEM VARIABLE
  {
    $$ = grantPrivNameFromArg(PRIV_VAR_SYSTEM_ALTER)
  }
  | ALTER DEBUG VARIABLE
  {
    $$ = grantPrivNameFromArg(PRIV_VAR_DEBUG_ALTER)
  }
  | SHOW SECURITY VARIABLES
  {
    $$ = grantPrivNameFromArg(PRIV_VAR_SECURITY_SHOW)
  }
  | SHOW AUDIT VARIABLES
  {
    $$ = grantPrivNameFromArg(PRIV_VAR_AUDIT_SHOW)
  }
  | SHOW SYSTEM VARIABLES
  {
    $$ = grantPrivNameFromArg(PRIV_VAR_SYSTEM_SHOW)
  }
  | SHOW DEBUG VARIABLES
  {
    $$ = grantPrivNameFromArg(PRIV_VAR_DEBUG_SHOW)
  }
  | CREATE TOPIC
  {
    $$ = grantPrivNameFromArg(PRIV_TOPIC_CREATE)
  }
  | SHOW CONSUMERS
  {
    $$ = grantPrivNameFromArg(PRIV_CONSUMER_SHOW)
  }
  | SHOW SUBSCRIPTIONS
  {
    $$ = grantPrivNameFromArg(PRIV_SUBSCRIPTION_SHOW)
  }
  | CREATE STREAM
  {
    $$ = grantPrivNameFromArg(PRIV_STREAM_CREATE)
  }
  | SHOW TRANS
  {
    $$ = grantPrivNameFromArg(PRIV_TRANS_SHOW)
  }
  | KILL TRANS
  {
    $$ = grantPrivNameFromArg(PRIV_TRANS_KILL)
  }
  | SHOW CONNECTIONS
  {
    $$ = grantPrivNameFromArg(PRIV_CONNECTION_SHOW)
  }
  | KILL CONNECTION
  {
    $$ = grantPrivNameFromArg(PRIV_CONNECTION_KILL)
  }
  | SHOW QUERIES
  {
    $$ = grantPrivNameFromArg(PRIV_QUERY_SHOW)
  }
  | KILL QUERY
  {
    $$ = grantPrivNameFromArg(PRIV_QUERY_KILL)
  }
  | USE INFORMATION_SCHEMA
  {
    $$ = grantPrivNameFromArg(PRIV_INFO_SCHEMA_USE)
  }
  | USE PERFORMANCE_SCHEMA
  {
    $$ = grantPrivNameFromArg(PRIV_PERF_SCHEMA_USE)
  }
  | READ INFORMATION_SCHEMA LIMIT
  {
    $$ = grantPrivNameFromArg(PRIV_INFO_SCHEMA_READ_LIMIT)
  }
  | READ INFORMATION_SCHEMA SECURITY
  {
    $$ = grantPrivNameFromArg(PRIV_INFO_SCHEMA_READ_SEC)
  }
  | READ INFORMATION_SCHEMA AUDIT
  {
    $$ = grantPrivNameFromArg(PRIV_INFO_SCHEMA_READ_AUDIT)
  }
  | READ INFORMATION_SCHEMA BASIC
  {
    $$ = grantPrivNameFromArg(PRIV_INFO_SCHEMA_READ_BASIC)
  }
  | READ PERFORMANCE_SCHEMA LIMIT
  {
    $$ = grantPrivNameFromArg(PRIV_PERF_SCHEMA_READ_LIMIT)
  }
  | READ PERFORMANCE_SCHEMA BASIC
  {
    $$ = grantPrivNameFromArg(PRIV_PERF_SCHEMA_READ_BASIC)
  }
  | SHOW GRANTS
  {
    $$ = grantPrivNameFromArg(PRIV_GRANTS_SHOW)
  }
  | SHOW CLUSTER
  {
    $$ = grantPrivNameFromArg(PRIV_CLUSTER_SHOW)
  }
  | SHOW APPS
  {
    $$ = grantPrivNameFromArg(PRIV_APPS_SHOW)
  }

priv_type_tbl_dml:
  SELECT TABLE
  {
    $$ = "select table"
  }
  | SELECT specific_cols_with_mask_opt
  {
    $$ = "select"
  }
  | INSERT TABLE
  {
    $$ = "insert table"
  }
  | INSERT specific_cols_opt
  {
    $$ = "insert"
  }
  | UPDATE TABLE
  {
    $$ = "update table"
  }
  | DELETE TABLE
  {
    $$ = "delete table"
  }
  | DELETE
  {
    $$ = "delete"
  }

priv_level_opt:
  /* empty */
  {
    $$ = ""
  }
  | ON priv_level
  {
    $$ = $2
  }
  | ON DATABASE priv_level
  {
    $$ = "database " + $3
  }
  | ON TABLE priv_level
  {
    $$ = "table " + $3
  }
  | ON VIEW priv_level
  {
    $$ = "view " + $3
  }
  | ON INDEX priv_level
  {
    $$ = "index " + $3
  }
  | ON TOPIC priv_level
  {
    $$ = "topic " + $3
  }
  | ON STREAM priv_level
  {
    $$ = "stream " + $3
  }
  | ON RSMA priv_level
  {
    $$ = "rsma " + $3
  }
  | ON TSMA priv_level
  {
    $$ = "tsma " + $3
  }

priv_level:
  NK_STAR
  {
    $$ = "*"
  }
  | NK_STAR NK_DOT NK_STAR
  {
    $$ = "*.*"
  }
  | db_name NK_DOT NK_STAR
  {
    $$ = $1 + ".*"
  }
  | db_name NK_DOT table_name
  {
    $$ = $1 + "." + $3
  }
  | db_name
  {
    $$ = $1
  }

/************************************************ create/drop role **********************************************/

/************************************************ dnode endpoint *********************************************/
dnode_endpoint:
  NK_STRING { $$ = $1 }
  | NK_ID { $$ = $1 }
  | NK_IPTOKEN { $$ = $1 }

/************************************************ force and unsafe options *********************************************/
force_opt:
  /* empty */
  {
    $$ = false
  }
  | FORCE
  {
    $$ = true
  }

unsafe_opt:
  UNSAFE { $$ = true }

/************************************************ bnode options ***************************************************/
bnode_options:
  /* empty */
  {
    $$ = CreateDefaultBnodeOptions()
  }
  | bnode_options NK_ID NK_STRING
  {
    $$ = SetBnodeOption($1, tool.BytesToString($2.Bytes), tool.BytesToString($3.Bytes))
  }

/************************************************ database options ********************************************/
db_options:
  /*empty*/
  {
    $$ = CreateDefaultDatabaseOptions()
  }
  | db_options BUFFER NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_BUFFER, $3.Bytes)
  }
  | db_options CACHEMODEL NK_STRING
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_CACHEMODEL, $3.Bytes)
  }
  | db_options CACHESIZE NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_CACHESIZE, $3.Bytes)
  }
  | db_options COMP NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_COMP, $3.Bytes)
  }
  | db_options DURATION NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_DAYS, $3.Bytes)
  }
  | db_options DURATION NK_VARIABLE
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_DAYS, $3.Bytes)
  }
  | db_options MAXROWS NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_MAXROWS, $3.Bytes)
  }
  | db_options MINROWS NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_MINROWS, $3.Bytes)
  }
  | db_options KEEP integer_list
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_KEEP, $3)
  }
  | db_options KEEP variable_list
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_KEEP, $3)
  }
  | db_options PAGES NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_PAGES, $3.Bytes)
  }
  | db_options PAGESIZE NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_PAGESIZE, $3.Bytes)
  }
  | db_options TSDB_PAGESIZE NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_TSDB_PAGESIZE, $3.Bytes)
  }
  | db_options PRECISION NK_STRING
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_PRECISION, $3.Bytes)
  }
  | db_options REPLICAS NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_REPLICA, $3.Bytes)
  }
  | db_options REPLICA NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_REPLICA, $3.Bytes)
  }
  | db_options VGROUPS NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_VGROUPS, $3.Bytes)
  }
  | db_options SINGLE_STABLE NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_SINGLE_STABLE, $3.Bytes)
  }
  | db_options SCHEMALESS NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_SCHEMALESS, $3.Bytes)
  }
  | db_options WAL_LEVEL NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_WAL, $3.Bytes)
  }
  | db_options WAL_FSYNC_PERIOD NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_FSYNC, $3.Bytes)
  }
  | db_options WAL_RETENTION_PERIOD NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_WAL_RETENTION_PERIOD, $3.Bytes)
  }
  | db_options WAL_RETENTION_PERIOD NK_MINUS NK_INTEGER
  {
    tok := append([]byte("-"), $4.Bytes...)
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_WAL_RETENTION_PERIOD, tok)
  }
  | db_options WAL_RETENTION_SIZE NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_WAL_RETENTION_SIZE, $3.Bytes)
  }
  | db_options WAL_RETENTION_SIZE NK_MINUS NK_INTEGER
  {
    tok := append([]byte("-"), $4.Bytes...)
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_WAL_RETENTION_SIZE, tok)
  }
  | db_options WAL_ROLL_PERIOD NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_WAL_ROLL_PERIOD, $3.Bytes)
  }
  | db_options WAL_SEGMENT_SIZE NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_WAL_SEGMENT_SIZE, $3.Bytes)
  }
  | db_options STT_TRIGGER NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_STT_TRIGGER, $3.Bytes)
  }
  | db_options TABLE_PREFIX signed
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_TABLE_PREFIX, $3.Bytes)
  }
  | db_options TABLE_SUFFIX signed
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_TABLE_SUFFIX, $3.Bytes)
  }
  | db_options SS_COMPACT NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_SS_COMPACT, $3.Bytes)
  }
  | db_options SS_CHUNKPAGES NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_SS_CHUNKPAGES, $3.Bytes)
  }
  | db_options SS_KEEPLOCAL NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_SS_KEEPLOCAL, $3.Bytes)
  }
  | db_options SS_KEEPLOCAL NK_VARIABLE
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_SS_KEEPLOCAL, $3.Bytes)
  }
  | db_options KEEP_TIME_OFFSET NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_KEEP_TIME_OFFSET, $3.Bytes)
  }
  | db_options KEEP_TIME_OFFSET NK_VARIABLE
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_KEEP_TIME_OFFSET, $3.Bytes)
  }
  | db_options DNODES NK_STRING
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_DNODES, $3.Bytes)
  }
  | db_options COMPACT_INTERVAL NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_COMPACT_INTERVAL, $3.Bytes)
  }
  | db_options COMPACT_INTERVAL NK_VARIABLE
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_COMPACT_INTERVAL, $3.Bytes)
  }
  | db_options COMPACT_TIME_RANGE signed_duration_list
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_COMPACT_TIME_RANGE, $3)
  }
  | db_options COMPACT_TIME_OFFSET NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_COMPACT_TIME_OFFSET, $3.Bytes)
  }
  | db_options COMPACT_TIME_OFFSET NK_VARIABLE
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_COMPACT_TIME_OFFSET, $3.Bytes)
  }
  | db_options IS_AUDIT NK_INTEGER
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_IS_AUDIT, $3.Bytes)
  }
  | db_options ENCRYPT_ALGORITHM NK_STRING
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_ENCRYPT_ALGORITHM, $3.Bytes)
  }
  | db_options RETENTIONS retention_list
  {
    $$ = SetDatabaseOption(yylex, $1, DB_OPTION_RETENTIONS, $3)
  }

/************************************************ retention ****************************************************/
retention:
  NK_VARIABLE NK_COLON NK_VARIABLE
  {
    result := append($1.Bytes, []byte(":")...)
    $$ = append(result, $3.Bytes...)
  }
  | MINUS NK_COLON NK_VARIABLE
  {
    $$ = append([]byte("-:"), $3.Bytes...)
  }

retention_list:
  retention
  {
    $$ = [][]byte{$1}
  }
  | retention_list NK_COMMA retention
  {
    $$ = append($1, $3)
  }

speed_opt:
  /*empty*/
  {
    $$ = 0
  }
  | BWLIMIT NK_INTEGER
  {
    $$ = tokenToInt32($2)
  }

dnode_list:
  DNODE NK_INTEGER
  {
    $$ = []Token{$2}
  }
  | dnode_list DNODE NK_INTEGER
  {
    $$ = append($1, $3)
  }

on_vgroup_id:
  /* empty */
  {
    $$ = Token{}
  }
  | ON NK_INTEGER
  {
    $$ = $2
  }


tags_def_opt:
  /* empty */
  {
    $$ = nil
  }
  | tags_def
  {
    $$ = $1
  }

tags_def:
  TAGS NK_LP tag_def_list NK_RP
  {
    $$ = $3
  }

multi_create_clause:
  create_subtable_clause
  {
    $$ = $1
  }
  | multi_create_clause create_subtable_clause
  {
    entry, ok := $2.(*MultiCreateTableStmt)
    if !ok || len(entry.Entries) == 0 {
      $$ = $1
    } else {
      $$ = AppendMultiCreateTableStmt($1, entry.Entries[0].Target, entry.Entries[0].Using, entry.Entries[0].NotExists, entry.Entries[0].SpecificCols, entry.Entries[0].TagValues, entry.Entries[0].Options)
    }
  }

create_subtable_clause:
  not_exists_opt full_table_name USING full_table_name specific_cols_opt TAGS NK_LP tags_literal_list NK_RP table_options
  {
    $$ = NewMultiCreateTableStmt($2, $4, $1, $5, $8, $10)
  }

with_opt:
/* empty */
  {
    $$ = false
  }
  | WITH
  {
    $$ = true
  }

table_options:
  /* empty */
  {
    $$ = &TableOptions{}
  }
  | table_options COMMENT NK_STRING
  {
    $$ = SetTableOption(yylex, $1, TABLE_OPTION_COMMENT, $3.Bytes)
  }
  | table_options MAX_DELAY duration_list
  {
    $$ = SetTableOption(yylex, $1, TABLE_OPTION_MAXDELAY, $3)
  }
  | table_options WATERMARK duration_list
  {
    $$ = SetTableOption(yylex, $1, TABLE_OPTION_WATERMARK, $3)
  }
  | table_options ROLLUP NK_LP rollup_func_list NK_RP
  {
    $$ = SetTableOption(yylex, $1, TABLE_OPTION_ROLLUP, $4)
  }
  | table_options TTL NK_INTEGER
  {
    $$ = SetTableOption(yylex, $1, TABLE_OPTION_TTL, $3.Bytes)
  }
  | table_options SMA NK_LP col_name_list NK_RP
  {
    $$ = SetTableOption(yylex, $1, TABLE_OPTION_SMA, $4)
  }
  | table_options DELETE_MARK duration_list
  {
    $$ = SetTableOption(yylex, $1, TABLE_OPTION_DELETE_MARK, $3)
  }
  | table_options KEEP NK_INTEGER
  {
    $$ = SetTableOption(yylex, $1, TABLE_OPTION_KEEP, $3.Bytes)
  }
  | table_options KEEP NK_VARIABLE
  {
    $$ = SetTableOption(yylex, $1, TABLE_OPTION_KEEP, $3.Bytes)
  }
  | table_options VIRTUAL NK_INTEGER
  {
    $$ = SetTableOption(yylex, $1, TABLE_OPTION_VIRTUAL, $3.Bytes)
  }

duration_list:
  NK_VARIABLE
  {
    $$ = [][]byte{$1.Bytes}
  }
  | duration_list NK_COMMA NK_VARIABLE
  {
    $$ = append($1, $3.Bytes)
  }

rollup_func_name:
  function_name
  {
    $$ = Token{Bytes: []byte($1)}
  }
  | FIRST
  {
    $$ = $1
  }
  | LAST
  {
    $$ = $1
  }

rollup_func_list:
  rollup_func_name
  {
    $$ = [][]byte{$1.Bytes}
  }
  | rollup_func_list NK_COMMA rollup_func_name
  {
    $$ = append($1, $3.Bytes)
  }

multi_drop_clause:
  drop_table_clause
  {
    $$ = $1
  }
  | multi_drop_clause NK_COMMA drop_table_clause
  {
    $$ = $1 + "," + $3
  }

drop_table_clause:
  exists_opt full_table_name
  {
    $$ = makeDropTableEntryText($1, $2)
  }

alter_table_clause:
  full_table_name alter_table_options
  {
    $$ = $1 + " " + strings.Join($2, " ")
  }
  | full_table_name ADD COLUMN column_name type_name column_options
  {
    $$ = $1 + " add column " + $4 + " " + $5 + formatColumnOptionsForAlter($6)
  }
  | full_table_name DROP COLUMN column_name
  {
    $$ = $1 + " drop column " + $4
  }
  | full_table_name MODIFY COLUMN column_name type_name
  {
    $$ = $1 + " modify column " + $4 + " " + $5
  }
  | full_table_name MODIFY COLUMN column_name column_options
  {
    $$ = $1 + " modify column " + $4 + formatColumnOptionsForAlter($5)
  }
  | full_table_name RENAME COLUMN column_name column_name
  {
    $$ = $1 + " rename column " + $4 + " " + $5
  }
  | full_table_name ADD TAG column_name type_name
  {
    $$ = $1 + " add tag " + $4 + " " + $5
  }
  | full_table_name DROP TAG column_name
  {
    $$ = $1 + " drop tag " + $4
  }
  | full_table_name MODIFY TAG column_name type_name
  {
    $$ = $1 + " modify tag " + $4 + " " + $5
  }
  | full_table_name RENAME TAG column_name column_name
  {
    $$ = $1 + " rename tag " + $4 + " " + $5
  }
  | full_table_name ALTER COLUMN column_name SET column_ref
  {
    $$ = $1 + " alter column " + $4 + " set " + $6
  }
  | full_table_name ALTER COLUMN column_name SET NULL
  {
    $$ = $1 + " alter column " + $4 + " set null"
  }
  | full_table_name SET TAG column_tag_value_list
  {
    $$ = $1 + " set tag " + strings.Join($4, ", ")
  }

alter_table_options:
  alter_table_option
  {
    $$ = []string{$1}
  }
  | alter_table_options alter_table_option
  {
    $$ = append($1, $2)
  }

alter_table_option:
  COMMENT NK_STRING
  {
    $$ = "comment '" + string($2.Bytes) + "'"
  }
  | TTL NK_INTEGER
  {
    $$ = "ttl " + string($2.Bytes)
  }
  | KEEP NK_INTEGER
  {
    $$ = "keep " + string($2.Bytes)
  }
  | KEEP NK_VARIABLE
  {
    $$ = "keep " + string($2.Bytes)
  }

column_tag_value:
  column_name NK_EQ tags_literal
  {
    $$ = $1 + "=" + $3
  }

column_tag_value_list:
  column_tag_value
  {
    $$ = []string{$1}
  }
  | column_tag_value_list NK_COMMA column_tag_value
  {
    $$ = append($1, $3)
  }

column_options:
  /* empty */
  {
    $$ = nil
  }
  | column_options PRIMARY KEY
  {
    $$ = SetColumnOptionsPK($1)
  }
  | column_options COMPOSITE KEY
  {
    $$ = SetColumnOptionsPK($1)
  }
  | column_options NK_ID NK_STRING
  {
    $$ = SetColumnOptions(yylex, $1, $2.Bytes, $3.Bytes)
  }
  | column_options FROM column_ref
  {
    $$ = SetColumnReference($1, $3)
  }

column_ref:
  column_name_triplet
  {
    $$ = $1
  }

column_name_triplet:
  NK_ID
  {
    $$ = string($1.Bytes)
  }
  | column_name_triplet NK_DOT NK_ID
  {
    $$ = $1 + "." + string($3.Bytes)
  }

alter_db_options:
  alter_db_option
  {
    $$ = NewAlterDatabaseOptions($1)
  }
  | alter_db_options alter_db_option
  {
    $$ = AddAlterDatabaseOption($1, $2)
  }

alter_db_option:
  BUFFER NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_BUFFER, Value: $2.Bytes}
  }
  | CACHEMODEL NK_STRING
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_CACHEMODEL, Value: $2.Bytes}
  }
  | CACHESIZE NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_CACHESIZE, Value: $2.Bytes}
  }
  | WAL_FSYNC_PERIOD NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_FSYNC, Value: $2.Bytes}
  }
  | KEEP integer_list
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_KEEP, Value: $2}
  }
  | KEEP variable_list
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_KEEP, Value: $2}
  }
  | PAGES NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_PAGES, Value: $2.Bytes}
  }
  | REPLICA NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_REPLICA, Value: $2.Bytes}
  }
  | REPLICAS NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_REPLICA, Value: $2.Bytes}
  }
  | WAL_LEVEL NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_WAL, Value: $2.Bytes}
  }
  | STT_TRIGGER NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_STT_TRIGGER, Value: $2.Bytes}
  }
  | MINROWS NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_MINROWS, Value: $2.Bytes}
  }
  | WAL_RETENTION_PERIOD NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_WAL_RETENTION_PERIOD, Value: $2.Bytes}
  }
  | WAL_RETENTION_PERIOD NK_MINUS NK_INTEGER
  {
    tok := append([]byte("-"), $3.Bytes...)
    $$ = DatabaseOptionKV{Type: DB_OPTION_WAL_RETENTION_PERIOD, Value: tok}
  }
  | WAL_RETENTION_SIZE NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_WAL_RETENTION_SIZE, Value: $2.Bytes}
  }
  | WAL_RETENTION_SIZE NK_MINUS NK_INTEGER
  {
    tok := append([]byte("-"), $3.Bytes...)
    $$ = DatabaseOptionKV{Type: DB_OPTION_WAL_RETENTION_SIZE, Value: tok}
  }
  | SS_KEEPLOCAL NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_SS_KEEPLOCAL, Value: $2.Bytes}
  }
  | SS_KEEPLOCAL NK_VARIABLE
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_SS_KEEPLOCAL, Value: $2.Bytes}
  }
  | SS_COMPACT NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_SS_COMPACT, Value: $2.Bytes}
  }
  | KEEP_TIME_OFFSET NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_KEEP_TIME_OFFSET, Value: $2.Bytes}
  }
  | KEEP_TIME_OFFSET NK_VARIABLE
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_KEEP_TIME_OFFSET, Value: $2.Bytes}
  }
  | ENCRYPT_ALGORITHM NK_STRING
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_ENCRYPT_ALGORITHM, Value: $2.Bytes}
  }
  | COMPACT_INTERVAL NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_COMPACT_INTERVAL, Value: $2.Bytes}
  }
  | COMPACT_INTERVAL NK_VARIABLE
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_COMPACT_INTERVAL, Value: $2.Bytes}
  }
  | COMPACT_TIME_RANGE signed_duration_list
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_COMPACT_TIME_RANGE, Value: $2}
  }
  | COMPACT_TIME_OFFSET NK_INTEGER
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_COMPACT_TIME_OFFSET, Value: $2.Bytes}
  }
  | COMPACT_TIME_OFFSET NK_VARIABLE
  {
    $$ = DatabaseOptionKV{Type: DB_OPTION_COMPACT_TIME_OFFSET, Value: $2.Bytes}
  }

column_def_list:
  column_def
  {
    $$ = []*ColumnDef{$1}
  }
  | column_def_list NK_COMMA column_def
  {
    $$ = append($1, $3)
  }

column_def:
  column_name type_name column_options
  {
    $$ = &ColumnDef{
      ColName:  $1,
      DataType: dataTypeFromTypeName(yylex, $2),
      Options:  $3,
      SMA:      false,
    }
  }

tag_def_list:
  tag_def
  {
    $$ = []*ColumnDef{$1}
  }
  | tag_def_list NK_COMMA tag_def
  {
    $$ = append($1, $3)
  }

tag_def:
  column_name type_name column_options
  {
    $$ = &ColumnDef{
      ColName:  $1,
      DataType: dataTypeFromTypeName(yylex, $2),
      Options:  $3,
      SMA:      false,
    }
  }

column_name_opt:
  /* empty */
  {
    $$ = nil
  }
  | column_name_unit
  {
    $$ = $1
  }

column_name_unit:
  NK_LP column_stream_def_list NK_RP
  {
    names := make([]string, 0, len($2))
    for _, c := range $2 {
      names = append(names, c.ColName)
    }
    $$ = names
  }

column_stream_def_list:
  column_stream_def
  {
    $$ = []*ColumnDef{$1}
  }
  | column_stream_def_list NK_COMMA column_stream_def
  {
    $$ = append($1, $3)
  }

column_stream_def:
  column_name stream_col_options
  {
    $$ = &ColumnDef{
      ColName:  $1,
      DataType: CreateDataType(TSDB_DATA_TYPE_NULL),
      Options:  $2,
      SMA:      false,
    }
  }

stream_col_options:
  /* empty */
  {
    $$ = &ColumnOption{}
  }
  | stream_col_options PRIMARY KEY
  {
    if $1 == nil {
      $1 = &ColumnOption{}
    }
    $1.PrimaryKey = true
    $$ = $1
  }
  | stream_col_options COMPOSITE KEY
  {
    if $1 == nil {
      $1 = &ColumnOption{}
    }
    $1.PrimaryKey = true
    $$ = $1
  }

/************************************************ SELECT statement *************************************************/

stream_outtable_opt:
  /* empty */
  {
    $$ = ""
  }
  | INTO full_table_name output_subtable_opt column_name_opt stream_tags_def_opt
  {
    out := "into " + $2
    if $3 != nil {
      out += " output_subtable(" + exprToSQL($3) + ")"
    }
    if len($4) > 0 {
      out += " (" + strings.Join($4, ", ") + ")"
    }
    if len($5) > 0 {
      out += " tags(" + strings.Join($5, ", ") + ")"
    }
    $$ = out
  }

stream_trigger:
  trigger_type trigger_table_opt stream_partition_by_opt trigger_options_opt notification_opt
  {
    parts := []string{$1}
    if $2 != "" {
      parts = append(parts, $2)
    }
    if len($3) > 0 {
      parts = append(parts, "partition by "+strings.Join($3, ", "))
    }
    if len($4) > 0 {
      parts = append(parts, "stream_options("+strings.Join($4, " | ")+")")
    }
    if $5 != "" {
      parts = append(parts, $5)
    }
    $$ = strings.Join(parts, " ")
  }

trigger_type:
  SESSION NK_LP column_reference NK_COMMA interval_sliding_duration_literal NK_RP
  {
    $$ = "session(" + exprToSQL($3) + ", " + string($5.Val.Bytes) + ")"
  }
  | STATE_WINDOW NK_LP expr_or_subquery state_window_opt NK_RP true_for_opt
  {
    $$ = "state_window(" + exprToSQL($3) + ")"
  }
  | interval_opt SLIDING NK_LP sliding_expr NK_RP
  {
    $$ = "interval_sliding"
  }
  | EVENT_WINDOW NK_LP START WITH search_condition END WITH search_condition NK_RP true_for_opt
  {
    $$ = "event_window"
  }
  | COUNT_WINDOW NK_LP count_window_args NK_RP
  {
    $$ = "count_window"
  }
  | PERIOD NK_LP interval_sliding_duration_literal offset_opt NK_RP
  {
    $$ = "period(" + string($3.Val.Bytes) + ")"
  }
  | EVENT_WINDOW NK_LP START WITH NK_LP search_condition_list NK_RP END WITH search_condition NK_RP true_for_opt
  {
    $$ = "event_window"
  }
  | EVENT_WINDOW NK_LP START WITH NK_LP search_condition_list NK_RP NK_RP true_for_opt
  {
    $$ = "event_window"
  }

trigger_table_opt:
  /* empty */
  {
    $$ = ""
  }
  | FROM full_table_name
  {
    $$ = "from " + $2
  }

stream_partition_by_opt:
  /* empty */
  {
    $$ = nil
  }
  | PARTITION BY stream_partition_list
  {
    $$ = $3
  }

stream_partition_list:
  stream_partition_item
  {
    $$ = []string{$1}
  }
  | stream_partition_list NK_COMMA stream_partition_item
  {
    $$ = append($1, $3)
  }

stream_partition_item:
  expr_or_subquery
  {
    $$ = exprToSQL($1)
  }
  | expr_or_subquery column_alias
  {
    $$ = exprToSQL($1) + " " + $2
  }

trigger_options_opt:
  /* empty */
  {
    $$ = nil
  }
  | STREAM_OPTIONS NK_LP trigger_option_list NK_RP
  {
    $$ = $3
  }

trigger_option_list:
  trigger_option
  {
    $$ = []string{$1}
  }
  | trigger_option_list NK_BITOR trigger_option
  {
    $$ = append($1, $3)
  }

trigger_option:
  CALC_NOTIFY_ONLY
  {
    $$ = "calc_notify_only"
  }
  | DELETE_RECALC
  {
    $$ = "delete_recalc"
  }
  | DELETE_OUTPUT_TABLE
  {
    $$ = "delete_output_table"
  }
  | EXPIRED_TIME NK_LP duration_literal NK_RP
  {
    $$ = "expired_time(" + string($3.Val.Bytes) + ")"
  }
  | FILL_HISTORY NK_LP time_point NK_RP
  {
    $$ = "fill_history(" + $3 + ")"
  }
  | FILL_HISTORY_FIRST NK_LP time_point NK_RP
  {
    $$ = "fill_history_first(" + $3 + ")"
  }
  | FILL_HISTORY
  {
    $$ = "fill_history"
  }
  | FILL_HISTORY_FIRST
  {
    $$ = "fill_history_first"
  }
  | FORCE_OUTPUT
  {
    $$ = "force_output"
  }
  | IGNORE_DISORDER
  {
    $$ = "ignore_disorder"
  }
  | LOW_LATENCY_CALC
  {
    $$ = "low_latency_calc"
  }
  | MAX_DELAY NK_LP duration_literal NK_RP
  {
    $$ = "max_delay(" + string($3.Val.Bytes) + ")"
  }
  | PRE_FILTER NK_LP search_condition NK_RP
  {
    $$ = "pre_filter(" + exprToSQL($3) + ")"
  }
  | WATERMARK NK_LP duration_literal NK_RP
  {
    $$ = "watermark(" + string($3.Val.Bytes) + ")"
  }
  | EVENT_TYPE NK_LP event_type_list NK_RP
  {
    $$ = "event_type"
  }
  | IGNORE_NODATA_TRIGGER
  {
    $$ = "ignore_nodata_trigger"
  }

notification_opt:
  /* empty */
  {
    $$ = ""
  }
  | NOTIFY NK_LP notify_url_list NK_RP notify_on_opt where_clause_opt notify_options_opt
  {
    out := "notify(" + strings.Join($3, ", ") + ")"
    if $6 != nil {
      out += " where " + exprToSQL($6)
    }
    $$ = out
  }

notify_url_list:
  NK_STRING
  {
    $$ = []string{string($1.Bytes)}
  }
  | notify_url_list NK_COMMA NK_STRING
  {
    $$ = append($1, string($3.Bytes))
  }

notify_on_opt:
  /* empty */
  {
    $$ = 0
  }
  | ON NK_LP event_type_list NK_RP
  {
    $$ = $3
  }

notify_options_opt:
  /* empty */
  {
    $$ = 0
  }
  | NOTIFY_OPTIONS NK_LP notify_options_list NK_RP
  {
    $$ = $3
  }

notify_options_list:
  notify_option
  {
    $$ = $1
  }
  | notify_options_list NK_BITOR notify_option
  {
    $$ = $1 | $3
  }

notify_option:
  NOTIFY_HISTORY
  {
    $$ = 1
  }

event_type_list:
  event_types
  {
    $$ = $1
  }
  | event_type_list NK_BITOR event_types
  {
    $$ = $1 | $3
  }

event_types:
  WINDOW_OPEN
  {
    $$ = 1
  }
  | WINDOW_CLOSE
  {
    $$ = 2
  }

output_subtable_opt:
  /* empty */
  {
    $$ = nil
  }
  | OUTPUT_SUBTABLE NK_LP expr_or_subquery NK_RP
  {
    $$ = $3
  }

stream_tags_def_opt:
  /* empty */
  {
    $$ = nil
  }
  | TAGS NK_LP stream_tags_def_list NK_RP
  {
    $$ = $3
  }

stream_tags_def_list:
  stream_tags_def
  {
    $$ = []string{$1}
  }
  | stream_tags_def_list NK_COMMA stream_tags_def
  {
    $$ = append($1, $3)
  }

stream_tags_def:
  column_name type_name AS expression
  {
    $$ = $1 + " " + $2 + " as " + exprToSQL($4)
  }

trigger_col_name:
  column_name
  {
    $$ = $1
  }
  | TBNAME
  {
    $$ = "tbname"
  }

index_options:
  FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt sma_stream_opt
  {
    out := "function(" + strings.Join($3, ", ") + ") interval(" + string($7.Val.Bytes) + ")"
    if len($9.Val.Bytes) > 0 {
      out += " sliding(" + string($9.Val.Bytes) + ")"
    }
    if $10 != "" {
      out += " " + $10
    }
    $$ = out
  }
  | FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt sma_stream_opt
  {
    out := "function(" + strings.Join($3, ", ") + ") interval(" + string($7.Val.Bytes) + ", " + string($9.Val.Bytes) + ")"
    if len($11.Val.Bytes) > 0 {
      out += " sliding(" + string($11.Val.Bytes) + ")"
    }
    if $12 != "" {
      out += " " + $12
    }
    $$ = out
  }

sma_stream_opt:
  /* empty */
  {
    $$ = ""
  }

start_opt:
/* empty */
  {
    $$ = ""
  }
  | START WITH NK_INTEGER
  {
    $$ = string($3.Bytes)
  }
  | START WITH NK_STRING
  {
    $$ = string($3.Bytes)
  }
  | START WITH TIMESTAMP NK_STRING
  {
    $$ = "timestamp:" + string($4.Bytes)
  }

end_opt:
/* empty */
  {
    $$ = ""
  }
  | END WITH NK_INTEGER
  {
    $$ = string($3.Bytes)
  }
  | END WITH NK_STRING
  {
    $$ = string($3.Bytes)
  }
  | END WITH TIMESTAMP NK_STRING
  {
    $$ = "timestamp:" + string($4.Bytes)
  }

integer_list:
  NK_INTEGER
  {
    $$ = [][]byte{$1.Bytes}
  }
  | integer_list NK_COMMA NK_INTEGER
  {
    $$ = append($1, $3.Bytes)
  }

variable_list:
  NK_VARIABLE
  {
    $$ = [][]byte{$1.Bytes}
  }
  | variable_list NK_COMMA NK_VARIABLE
  {
    $$ = append($1, $3.Bytes)
  }

signed_variable:
  NK_VARIABLE
  {
    $$ = $1
  }
  | NK_PLUS NK_VARIABLE
  {
    $$ = $2
  }
  | NK_MINUS NK_VARIABLE
  {
    tok := Token{Type: $2.Type, Bytes: append([]byte("-"), $2.Bytes...)}
    $$ = tok
  }

signed_duration_list:
  signed_variable
  {
    $$ = [][]byte{$1.Bytes}
  }
  | signed_integer
  {
    $$ = [][]byte{$1.Val.Bytes}
  }
  | signed_duration_list NK_COMMA signed_integer
  {
    $$ = append($1, $3.Val.Bytes)
  }
  | signed_duration_list NK_COMMA signed_variable
  {
    $$ = append($1, $3.Bytes)
  }

meta_only:
/* empty */
  {
    $$ = false
  }
  | META_ONLY
  {
    $$ = true
  }

xnode_endpoint:
  NK_STRING
  {
    $$ = string($1.Bytes)
  }
  | NK_INTEGER
  {
    $$ = string($1.Bytes)
  }

topic_name:
  table_name
  {
    $$ = $1
  }

cgroup_name:
  table_name
  {
    $$ = $1
  }

stream_name:
  table_name
  {
    $$ = $1
  }

stream_name_list:
  full_stream_name
  {
    $$ = []string{$1}
  }
  | stream_name_list NK_COMMA full_stream_name
  {
    $$ = append($1, $3)
  }

recalculate_range:
  FROM time_point
  {
    $$ = StreamRecalculateRange{From: $2}
  }
  | FROM time_point TO time_point
  {
    $$ = StreamRecalculateRange{From: $2, To: $4}
  }

time_point:
  NK_INTEGER
  {
    $$ = string($1.Bytes)
  }
  | NK_STRING
  {
    $$ = string($1.Bytes)
  }

ignore_opt:
/* empty */
  {
    $$ = false
  }
  | IGNORE UNTREATED
  {
    $$ = true
  }

with_meta:
  AS
  {
    $$ = "as"
  }
  | WITH META AS
  {
    $$ = "with_meta_as"
  }
  | ONLY META AS
  {
    $$ = "only_meta_as"
  }

rsma_func_list:
  /* empty */
  {
    $$ = nil
  }
  | FUNCTION NK_LP NK_RP
  {
    $$ = nil
  }
  | FUNCTION NK_LP func_list NK_RP
  {
    $$ = $3
  }

tsma_func_list:
  FUNCTION NK_LP func_list NK_RP
  {
    $$ = $3
  }

func_list:
  func
  {
    $$ = []string{$1}
  }
  | func_list NK_COMMA func
  {
    $$ = append($1, $3)
  }

func:
  sma_func_name NK_LP expression_list NK_RP
  {
    $$ = buildNamedFuncSQL($1, $3)
  }

sma_func_name:
  function_name
  {
    $$ = $1
  }
  | COUNT
  {
    $$ = "count"
  }
  | FIRST
  {
    $$ = "first"
  }
  | LAST
  {
    $$ = "last"
  }
  | LAST_ROW
  {
    $$ = "last_row"
  }

view_name:
  table_name
  {
    $$ = $1
  }

rsma_name:
  table_name
  {
    $$ = $1
  }

tsma_name:
  table_name
  {
    $$ = $1
  }

index_name:
  table_name
  {
    $$ = $1
  }

analyze_opt:
  /* empty */
  {
    $$ = false
  }
  | ANALYZE
  {
    $$ = true
  }

explain_options:
  /* empty */
  {
    $$ = ExplainOptions{}
  }
  | explain_options VERBOSE NK_BOOL
  {
    opt := $1
    opt.VerboseSet = true
    opt.Verbose = string($3.Bytes) == "true" || string($3.Bytes) == "1"
    $$ = opt
  }
  | explain_options RATIO NK_FLOAT
  {
    opt := $1
    opt.RatioSet = true
    opt.Ratio = Literal{Val: $3, Type: LiteralFloat}
    $$ = opt
  }

insert_query:
  INSERT INTO full_table_name NK_LP column_name_list NK_RP query_or_subquery
  {
    $$ = &InsertQueryStmt{Table: $3, Columns: $5, Query: $7}
  }
  | INSERT INTO full_table_name query_or_subquery
  {
    $$ = &InsertQueryStmt{Table: $3, Query: $4}
  }

query_expression:
  query_simple order_by_clause_opt slimit_clause_opt limit_clause_opt
  {
    $$ = NewSelectStmtWithClauses(yylex, $1, $2, $3, $4)
  }

query_simple:
  query_specification
  {
    $$ = $1
  }
  | union_query_expression
  {
    $$ = $1
  }

union_query_expression:
  query_simple_or_subquery UNION ALL query_simple_or_subquery
  {
    $$ = NewUnionStmt(yylex, $1, $4, true)
  }
  | query_simple_or_subquery UNION query_simple_or_subquery
  {
    $$ = NewUnionStmt(yylex, $1, $3, false)
  }

query_simple_or_subquery:
  query_simple
  {
    $$ = $1
  }
  | subquery
  {
    $$ = $1
  }

query_or_subquery:
  query_expression
  {
    $$ = $1
  }
  | subquery
  {
    $$ = $1
  }

as_subquery_opt:
  /* empty */
  {
    $$ = nil
  }
  | AS query_or_subquery
  {
    $$ = $2
  }

/************************************************ query_specification *************************************************/
query_specification:
  SELECT hint_list set_quantifier_opt tag_mode_opt select_list from_clause_opt where_clause_opt partition_by_clause_opt range_opt every_opt interp_fill_opt twindow_clause_opt group_by_clause_opt having_clause_opt
  {
    $$ = NewSelectStmt(yylex, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
  }

hint_list:
  /* empty */
  {
    $$ = nil
  }
  | NK_HINT
  {
    $$ = NewHintOptionFromHintToken($1)
  }

set_quantifier_opt:
  /* empty */
  {
    $$ = false
  }
  | DISTINCT
  {
    $$ = true
  }
  | ALL
  {
    $$ = false
  }

select_list:
  select_item
  {
    $$ = $1
  }
  | select_list NK_COMMA select_item
  {
    $$ = append($1, $3...)
  }

select_item:
  NK_STAR
  {
    $$ = []Expr{&StarExpr{}}
  }
  | common_expression
  {
    $$ = []Expr{$1}
  }
  | common_expression column_alias
  {
    $$ = []Expr{&AliasedExpr{Expr: $1, Alias: $2}}
  }
  | common_expression AS column_alias
  {
    $$ = []Expr{&AliasedExpr{Expr: $1, Alias: $3}}
  }
  | table_name NK_DOT NK_STAR
  {
    $$ = []Expr{&StarExpr{TableName: $1}}
  }

/************************************************ from_clause *********************************************************/
from_clause_opt:
  /* empty */
  {
    $$ = nil
  }
  | FROM table_reference_list
  {
    $$ = $2
  }

table_reference_list:
  table_reference
  {
    $$ = $1
  }
  | table_reference_list NK_COMMA table_reference
  {
    $$ = NewJoinTableExpr(yylex, $1, $3, JoinTypeInner, nil)
  }

table_reference:
  table_primary
  {
    $$ = $1
  }
  | joined_table
  {
    $$ = $1
  }

table_primary:
  table_name alias_opt
  {
    $$ = NewTableNameExpr(yylex, "", $1, $2)
  }
  | db_name NK_DOT table_name alias_opt
  {
    $$ = NewTableNameExpr(yylex, $1, $3, $4)
  }
  | subquery alias_opt
  {
    $$ = NewSubqueryTableExpr(yylex, $1, $2)
  }
  | NK_PH TBNAME alias_opt
  {
    $$ = NewTableNameExpr(yylex, "", "_ph_tbname", $3)
  }
  | NK_PH TROWS alias_opt
  {
    $$ = NewTableNameExpr(yylex, "", "_ph_trows", $3)
  }
  | parenthesized_joined_table
  {
    $$ = $1
  }

alias_opt:
  /* empty */
  {
    $$ = ""
  }
  | table_alias
  {
    $$ = $1
  }
  | AS table_alias
  {
    $$ = $2
  }

/************************************************ joined_table ********************************************************/
joined_table:
  inner_joined
  {
    $$ = $1
  }
  | outer_joined
  {
    $$ = $1
  }
  | semi_joined
  {
    $$ = $1
  }
  | anti_joined
  {
    $$ = $1
  }
  | asof_joined
  {
    $$ = $1
  }
  | win_joined
  {
    $$ = $1
  }

inner_joined:
  table_reference JOIN table_reference join_on_clause_opt
  {
    $$ = NewJoinTableExpr(yylex, $1, $3, JoinTypeInner, $4)
  }
  | table_reference INNER JOIN table_reference join_on_clause_opt
  {
    $$ = NewJoinTableExpr(yylex, $1, $4, JoinTypeInner, $5)
  }

outer_joined:
  table_reference LEFT JOIN table_reference join_on_clause
  {
    $$ = NewJoinTableExpr(yylex, $1, $4, JoinTypeLeft, $5)
  }
  | table_reference RIGHT JOIN table_reference join_on_clause
  {
    $$ = NewJoinTableExpr(yylex, $1, $4, JoinTypeRight, $5)
  }
  | table_reference FULL JOIN table_reference join_on_clause
  {
    $$ = NewJoinTableExpr(yylex, $1, $4, JoinTypeFull, $5)
  }
  | table_reference LEFT OUTER JOIN table_reference join_on_clause
  {
    $$ = NewJoinTableExpr(yylex, $1, $5, JoinTypeLeft, $6)
  }
  | table_reference RIGHT OUTER JOIN table_reference join_on_clause
  {
    $$ = NewJoinTableExpr(yylex, $1, $5, JoinTypeRight, $6)
  }
  | table_reference FULL OUTER JOIN table_reference join_on_clause
  {
    $$ = NewJoinTableExpr(yylex, $1, $5, JoinTypeFull, $6)
  }

semi_joined:
  table_reference LEFT SEMI JOIN table_reference join_on_clause
  {
    $$ = NewJoinTableExpr(yylex, $1, $5, JoinTypeLeftSemi, $6)
  }
  | table_reference RIGHT SEMI JOIN table_reference join_on_clause
  {
    $$ = NewJoinTableExpr(yylex, $1, $5, JoinTypeRightSemi, $6)
  }

anti_joined:
  table_reference LEFT ANTI JOIN table_reference join_on_clause
  {
    $$ = NewJoinTableExpr(yylex, $1, $5, JoinTypeLeftAnti, $6)
  }
  | table_reference RIGHT ANTI JOIN table_reference join_on_clause
  {
    $$ = NewJoinTableExpr(yylex, $1, $5, JoinTypeRightAnti, $6)
  }

asof_joined:
  table_reference LEFT ASOF JOIN table_reference join_on_clause_opt jlimit_clause_opt
  {
    $$ = SetJoinWindowOffsetAndLimit(NewJoinTableExpr(yylex, $1, $5, JoinTypeLeftAsof, $6), nil, $7)
  }
  | table_reference RIGHT ASOF JOIN table_reference join_on_clause_opt jlimit_clause_opt
  {
    $$ = SetJoinWindowOffsetAndLimit(NewJoinTableExpr(yylex, $1, $5, JoinTypeRightAsof, $6), nil, $7)
  }

win_joined:
  table_reference LEFT WINDOW JOIN table_reference join_on_clause_opt window_offset_clause jlimit_clause_opt
  {
    $$ = SetJoinWindowOffsetAndLimit(NewJoinTableExpr(yylex, $1, $5, JoinTypeLeftWindow, $6), $7, $8)
  }
  | table_reference RIGHT WINDOW JOIN table_reference join_on_clause_opt window_offset_clause jlimit_clause_opt
  {
    $$ = SetJoinWindowOffsetAndLimit(NewJoinTableExpr(yylex, $1, $5, JoinTypeRightWindow, $6), $7, $8)
  }

join_on_clause_opt:
  /* empty */
  %prec ON
  {
    $$ = nil
  }
  | join_on_clause
  {
    $$ = $1
  }

join_on_clause:
  ON search_condition
  {
    $$ = $2
  }

window_offset_clause:
  WINDOW_OFFSET NK_LP window_offset_literal NK_COMMA window_offset_literal NK_RP
  {
    $$ = NewWindowOffsetExpr($3, $5)
  }

window_offset_literal:
  NK_VARIABLE
  {
    $$ = NewLiteralExpr(yylex, $1, LiteralDuration)
  }
  | NK_MINUS NK_VARIABLE
  {
    tok := Token{Bytes: append([]byte("-"), $2.Bytes...)}
    $$ = NewLiteralExpr(yylex, tok, LiteralDuration)
  }

unsigned_integer:
  NK_INTEGER
  {
    $$ = $1
  }
  | NK_QUESTION
  {
    tok := $1
    tok.Bytes = []byte("?")
    $$ = tok
  }

jlimit_clause_opt:
  /* empty */
  %prec JLIMIT
  {
    $$ = nil
  }
  | JLIMIT unsigned_integer
  {
    $$ = &LimitExpr{Limit: $2}
  }

/************************************************ where_clause ********************************************************/
where_clause_opt:
  /* empty */
  {
    $$ = nil
  }
  | WHERE search_condition
  {
    $$ = $2
  }

/************************************************ group_by_clause *****************************************************/
group_by_clause_opt:
  /* empty */
  {
    $$ = nil
  }
  | GROUP BY group_by_list
  {
    $$ = $3
  }

group_by_list:
  expr_or_subquery
  {
    $$ = &GroupByExpr{Exprs: []Expr{$1}}
  }
  | group_by_list NK_COMMA expr_or_subquery
  {
    $$ = &GroupByExpr{Exprs: append($1.Exprs, $3)}
  }

/************************************************ having_clause *******************************************************/
having_clause_opt:
  /* empty */
  {
    $$ = nil
  }
  | HAVING search_condition
  {
    $$ = $2
  }

range_opt:
  /* empty */
  {
    $$ = nil
  }
  | RANGE NK_LP expr_or_subquery NK_COMMA expr_or_subquery NK_COMMA expr_or_subquery NK_RP
  {
    $$ = &RawExpr{Kind: "range_3", Args: []Expr{$3, $5, $7}}
  }
  | RANGE NK_LP expr_or_subquery NK_COMMA expr_or_subquery NK_RP
  {
    $$ = &RawExpr{Kind: "range_2", Args: []Expr{$3, $5}}
  }
  | RANGE NK_LP expr_or_subquery NK_RP
  {
    $$ = &RawExpr{Kind: "range_1", Args: []Expr{$3}}
  }

every_opt:
  /* empty */
  {
    $$ = Literal{}
  }
  | EVERY NK_LP duration_literal NK_RP
  {
    $$ = $3
  }

/************************************************ order_by_clause *****************************************************/
order_by_clause_opt:
  /* empty */
  {
    $$ = nil
  }
  | ORDER BY sort_specification_list
  {
    $$ = $3
  }

sort_specification_list:
  sort_specification
  {
    $$ = []OrderByExpr{$1}
  }
  | sort_specification_list NK_COMMA sort_specification
  {
    $$ = append($1, $3)
  }

sort_specification:
  expr_or_subquery ordering_specification_opt null_ordering_opt
  {
    $$ = OrderByExpr{Expr: $1, Asc: $2, NullsFirst: $3}
  }

ordering_specification_opt:
  /* empty */
  {
    $$ = true
  }
  | ASC
  {
    $$ = true
  }
  | DESC
  {
    $$ = false
  }

null_ordering_opt:
  /* empty */
  {
    $$ = false
  }
  | NULLS FIRST
  {
    $$ = true
  }
  | NULLS LAST
  {
    $$ = false
  }

/************************************************ limit_clause ********************************************************/
limit_clause_opt:
  /* empty */
  {
    $$ = nil
  }
  | LIMIT unsigned_integer
  {
    $$ = &LimitExpr{Limit: $2}
  }
  | LIMIT unsigned_integer OFFSET unsigned_integer
  {
    $$ = &LimitExpr{Limit: $2, Offset: $4}
  }
  | LIMIT unsigned_integer NK_COMMA unsigned_integer
  {
    $$ = &LimitExpr{Offset: $2, Limit: $4}
  }

slimit_clause_opt:
  /* empty */
  {
    $$ = nil
  }
  | SLIMIT unsigned_integer
  {
    $$ = &LimitExpr{SLimit: $2}
  }
  | SLIMIT unsigned_integer SOFFSET unsigned_integer
  {
    $$ = &LimitExpr{SLimit: $2, SOffset: $4}
  }
  | SLIMIT unsigned_integer NK_COMMA unsigned_integer
  {
    $$ = &LimitExpr{SOffset: $2, SLimit: $4}
  }

/************************************************ subquery ************************************************************/
subquery:
  NK_LP query_expression NK_RP
  {
    $$ = $2
  }
  | NK_LP subquery NK_RP
  {
    $$ = $2
  }

/************************************************ common_expression ***************************************************/
common_expression:
  expr_or_subquery
  {
    $$ = $1
  }
  | boolean_value_expression
  {
    $$ = $1
  }

expr_or_subquery:
  expression
  {
    $$ = $1
  }
  | subquery %prec NK_RP
  {
    $$ = $1
  }

/************************************************ expression **********************************************************/
expression:
  literal
  {
    $$ = $1
  }
  | pseudo_column
  {
    $$ = $1
  }
  | column_reference
  {
    $$ = $1
  }
  | function_expression
  {
    $$ = $1
  }
  | if_expression
  {
    $$ = $1
  }
  | case_when_expression
  {
    $$ = $1
  }
  | NK_LP expression NK_RP
  {
    $$ = $2
  }
  | NK_PLUS expr_or_subquery
  {
    $$ = NewUnaryExpr(yylex, OP_TYPE_UPLUS, $2)
  }
  | NK_MINUS expr_or_subquery
  {
    $$ = NewUnaryExpr(yylex, OP_TYPE_MINUS, $2)
  }
  | expr_or_subquery NK_PLUS expr_or_subquery
  {
    $$ = NewBinaryExpr(yylex, $1, OP_TYPE_ADD, $3)
  }
  | expr_or_subquery NK_MINUS expr_or_subquery
  {
    $$ = NewBinaryExpr(yylex, $1, OP_TYPE_SUB, $3)
  }
  | expr_or_subquery NK_STAR expr_or_subquery
  {
    $$ = NewBinaryExpr(yylex, $1, OP_TYPE_MULTI, $3)
  }
  | expr_or_subquery NK_SLASH expr_or_subquery
  {
    $$ = NewBinaryExpr(yylex, $1, OP_TYPE_DIV, $3)
  }
  | expr_or_subquery NK_REM expr_or_subquery
  {
    $$ = NewBinaryExpr(yylex, $1, OP_TYPE_REM, $3)
  }
  | expr_or_subquery NK_BITAND expr_or_subquery
  {
    $$ = NewBinaryExpr(yylex, $1, OP_TYPE_BIT_AND, $3)
  }
  | expr_or_subquery NK_BITOR expr_or_subquery
  {
    $$ = NewBinaryExpr(yylex, $1, OP_TYPE_BIT_OR, $3)
  }
  | column_reference NK_ARROW NK_STRING
  {
    $$ = NewJsonExpr(yylex, $1, $3)
  }

expression_list:
  expr_or_subquery
  {
    $$ = []Expr{$1}
  }
  | expression_list NK_COMMA expr_or_subquery
  {
    $$ = append($1, $3)
  }

/************************************************ column_reference ****************************************************/
column_reference:
  column_name
  {
    $$ = NewColNameExpr(yylex, "", $1)
  }
  | NK_ALIAS
  {
    $$ = NewColNameExpr(yylex, "", string($1.Bytes))
  }
  | table_name NK_DOT column_name
  {
    $$ = NewColNameExpr(yylex, $1, $3)
  }
  | table_name NK_DOT NK_ALIAS
  {
    $$ = NewColNameExpr(yylex, $1, string($3.Bytes))
  }

column_name:
  NK_ID
  {
    $$ = string($1.Bytes)
  }

table_name:
  NK_ID
  {
    $$ = string($1.Bytes)
  }

db_name:
  NK_ID
  {
    $$ = string($1.Bytes)
  }

column_alias:
  NK_ID
  {
    $$ = string($1.Bytes)
  }
  | NK_ALIAS
  {
    $$ = string($1.Bytes)
  }

table_alias:
  NK_ID
  {
    $$ = string($1.Bytes)
  }

column_name_list:
  trigger_col_name
  {
    $$ = []string{$1}
  }
  | column_name_list NK_COMMA trigger_col_name
  {
    $$ = append($1, $3)
  }

/************************************************ pseudo_column *******************************************************/
pseudo_column:
  ROWTS
  {
    $$ = NewPseudoColumnExpr(yylex, "_rowts")
  }
  | TBNAME
  {
    $$ = NewPseudoColumnExpr(yylex, "tbname")
  }
  | QSTART
  {
    $$ = NewPseudoColumnExpr(yylex, "_qstart")
  }
  | QEND
  {
    $$ = NewPseudoColumnExpr(yylex, "_qend")
  }
  | QDURATION
  {
    $$ = NewPseudoColumnExpr(yylex, "_qduration")
  }
  | WSTART
  {
    $$ = NewPseudoColumnExpr(yylex, "_wstart")
  }
  | WEND
  {
    $$ = NewPseudoColumnExpr(yylex, "_wend")
  }
  | WDURATION
  {
    $$ = NewPseudoColumnExpr(yylex, "_wduration")
  }
  | IROWTS
  {
    $$ = NewPseudoColumnExpr(yylex, "_irowts")
  }
  | ISFILLED
  {
    $$ = NewPseudoColumnExpr(yylex, "_isfilled")
  }
  | QTAGS
  {
    $$ = NewPseudoColumnExpr(yylex, "_tags")
  }
  | FLOW
  {
    $$ = NewPseudoColumnExpr(yylex, "_flow")
  }
  | FHIGH
  {
    $$ = NewPseudoColumnExpr(yylex, "_fhigh")
  }
  | FROWTS
  {
    $$ = NewPseudoColumnExpr(yylex, "_frowts")
  }
  | IROWTS_ORIGIN
  {
    $$ = NewPseudoColumnExpr(yylex, "_irowts_origin")
  }
  | TPREV_TS
  {
    $$ = NewPseudoColumnExpr(yylex, "_tprev_ts")
  }
  | TCURRENT_TS
  {
    $$ = NewPseudoColumnExpr(yylex, "_tcurrent_ts")
  }
  | TNEXT_TS
  {
    $$ = NewPseudoColumnExpr(yylex, "_tnext_ts")
  }
  | TWSTART
  {
    $$ = NewPseudoColumnExpr(yylex, "_twstart")
  }
  | TWEND
  {
    $$ = NewPseudoColumnExpr(yylex, "_twend")
  }
  | TWDURATION
  {
    $$ = NewPseudoColumnExpr(yylex, "_twduration")
  }
  | TWROWNUM
  {
    $$ = NewPseudoColumnExpr(yylex, "_twrownum")
  }
  | TPREV_LOCALTIME
  {
    $$ = NewPseudoColumnExpr(yylex, "_tprev_localtime")
  }
  | TNEXT_LOCALTIME
  {
    $$ = NewPseudoColumnExpr(yylex, "_tnext_localtime")
  }
  | TLOCALTIME
  {
    $$ = NewPseudoColumnExpr(yylex, "_tlocaltime")
  }
  | TGRPID
  {
    $$ = NewPseudoColumnExpr(yylex, "_tgrpid")
  }
  | NK_PH NK_INTEGER
  {
    $$ = NewPseudoColumnExpr(yylex, "_ph_int")
  }
  | NK_PH TBNAME
  {
    $$ = NewPseudoColumnExpr(yylex, "_ph_tbname")
  }
  | IMPROWTS
  {
    $$ = NewPseudoColumnExpr(yylex, "_improws")
  }
  | IMPMARK
  {
    $$ = NewPseudoColumnExpr(yylex, "_impmark")
  }
  | ANOMALYMARK
  {
    $$ = NewPseudoColumnExpr(yylex, "_anomalymark")
  }
  | table_name NK_DOT TBNAME
  {
    $$ = NewPseudoColumnExpr(yylex, $1+".tbname")
  }

/************************************************ function_expression *************************************************/
function_expression:
  function_name NK_LP expression_list NK_RP
  {
    $$ = NewFuncExpr(yylex, $1, $3)
  }
  | star_func NK_LP star_func_para_list NK_RP
  {
    $$ = NewFuncExpr(yylex, $1, $3)
  }
  | cols_func NK_LP cols_func_para_list NK_RP
  {
    $$ = NewFuncExpr(yylex, $1, $3)
  }
  | CAST NK_LP common_expression AS type_name NK_RP
  {
    $$ = NewCastExpr(yylex, $3, $5)
  }
  | CAST NK_LP common_expression AS type_name_default_len NK_RP
  {
    $$ = NewCastExpr(yylex, $3, $5)
  }
  | TRIM NK_LP expr_or_subquery NK_RP
  {
    $$ = NewTrimExpr(yylex, $3, "")
  }
  | TRIM NK_LP trim_specification_type FROM expr_or_subquery NK_RP
  {
    $$ = NewTrimExpr(yylex, $5, $3)
  }
  | TRIM NK_LP expr_or_subquery FROM expr_or_subquery NK_RP
  {
    $$ = NewTrimExprWithPattern(yylex, $3, $5, "")
  }
  | TRIM NK_LP trim_specification_type expr_or_subquery FROM expr_or_subquery NK_RP
  {
    $$ = NewTrimExprWithPattern(yylex, $4, $6, $3)
  }
  | POSITION NK_LP expr_or_subquery IN expr_or_subquery NK_RP
  {
    $$ = NewPositionExpr(yylex, $3, $5)
  }
  | substr_func NK_LP expression_list NK_RP
  {
    $$ = NewFuncExpr(yylex, $1, $3)
  }
  | substr_func NK_LP expr_or_subquery FROM expr_or_subquery NK_RP
  {
    $$ = NewFuncExpr(yylex, $1, []Expr{$3, $5})
  }
  | substr_func NK_LP expr_or_subquery FROM expr_or_subquery FOR expr_or_subquery NK_RP
  {
    $$ = NewFuncExpr(yylex, $1, []Expr{$3, $5, $7})
  }
  | REPLACE NK_LP expression_list NK_RP
  {
    $$ = NewFuncExpr(yylex, "replace", $3)
  }
  | literal_func
  {
    $$ = $1
  }
  | rand_func
  {
    $$ = $1
  }

literal_func:
  noarg_func NK_LP NK_RP
  {
    $$ = NewFuncExpr(yylex, $1, nil)
  }
  | NOW
  {
    $$ = NewFuncExpr(yylex, "now", nil)
  }
  | TODAY
  {
    $$ = NewFuncExpr(yylex, "today", nil)
  }

rand_func:
  RAND NK_LP NK_RP
  {
    $$ = NewFuncExpr(yylex, "rand", nil)
  }
  | RAND NK_LP expression_list NK_RP
  {
    $$ = NewFuncExpr(yylex, "rand", $3)
  }

substr_func:
  SUBSTR
  {
    $$ = "substr"
  }
  | SUBSTRING
  {
    $$ = "substring"
  }

noarg_func:
  NOW
  {
    $$ = "now"
  }
  | TODAY
  {
    $$ = "today"
  }
  | TIMEZONE
  {
    $$ = "timezone"
  }
  | DATABASE
  {
    $$ = "database"
  }
  | CLIENT_VERSION
  {
    $$ = "client_version"
  }
  | SERVER_VERSION
  {
    $$ = "server_version"
  }
  | SERVER_STATUS
  {
    $$ = "server_status"
  }
  | CURRENT_USER
  {
    $$ = "current_user"
  }
  | USER
  {
    $$ = "user"
  }
  | PI
  {
    $$ = "pi"
  }

function_name:
  NK_ID
  {
    $$ = string($1.Bytes)
  }

star_func:
  COUNT
  {
    $$ = "count"
  }
  | FIRST
  {
    $$ = "first"
  }
  | LAST
  {
    $$ = "last"
  }
  | LAST_ROW
  {
    $$ = "last_row"
  }

cols_func:
  COLS
  {
    $$ = "cols"
  }

cols_func_para_list:
  function_expression NK_COMMA cols_func_expression_list
  {
    $$ = append([]Expr{$1}, $3...)
  }

cols_func_expression:
  expr_or_subquery
  {
    $$ = $1
  }
  | NK_STAR
  {
    $$ = &StarExpr{}
  }
  | expr_or_subquery column_alias
  {
    $$ = &AliasedExpr{Expr: $1, Alias: $2}
  }
  | expr_or_subquery AS column_alias
  {
    $$ = &AliasedExpr{Expr: $1, Alias: $3}
  }

cols_func_expression_list:
  cols_func_expression
  {
    $$ = []Expr{$1}
  }
  | cols_func_expression_list NK_COMMA cols_func_expression
  {
    $$ = append($1, $3)
  }

star_func_para_list:
  NK_STAR
  {
    $$ = []Expr{&StarExpr{}}
  }
  | other_para_list
  {
    $$ = $1
  }

other_para_list:
  star_func_para
  {
    $$ = []Expr{$1}
  }
  | other_para_list NK_COMMA star_func_para
  {
    $$ = append($1, $3)
  }

star_func_para:
  expr_or_subquery
  {
    $$ = $1
  }
  | table_name NK_DOT NK_STAR
  {
    $$ = &StarExpr{TableName: $1}
  }

trim_specification_type:
  BOTH
  {
    $$ = "both"
  }
  | LEADING
  {
    $$ = "leading"
  }
  | TRAILING
  {
    $$ = "trailing"
  }

type_name:
  BIGINT
  {
    $$ = "bigint"
  }
  | BOOL
  {
    $$ = "bool"
  }
  | DOUBLE
  {
    $$ = "double"
  }
  | FLOAT
  {
    $$ = "float"
  }
  | INT
  {
    $$ = "int"
  }
  | INTEGER
  {
    $$ = "integer"
  }
  | SMALLINT
  {
    $$ = "smallint"
  }
  | TIMESTAMP
  {
    $$ = "timestamp"
  }
  | TINYINT
  {
    $$ = "tinyint"
  }
  | BIGINT UNSIGNED
  {
    $$ = "bigint unsigned"
  }
  | INT UNSIGNED
  {
    $$ = "int unsigned"
  }
  | SMALLINT UNSIGNED
  {
    $$ = "smallint unsigned"
  }
  | TINYINT UNSIGNED
  {
    $$ = "tinyint unsigned"
  }
  | BINARY NK_LP NK_INTEGER NK_RP
  {
    $$ = "binary(" + string($3.Bytes) + ")"
  }
  | NCHAR NK_LP NK_INTEGER NK_RP
  {
    $$ = "nchar(" + string($3.Bytes) + ")"
  }
  | VARCHAR NK_LP NK_INTEGER NK_RP
  {
    $$ = "varchar(" + string($3.Bytes) + ")"
  }
  | VARBINARY NK_LP NK_INTEGER NK_RP
  {
    $$ = "varbinary(" + string($3.Bytes) + ")"
  }
  | DECIMAL NK_LP NK_INTEGER NK_RP
  {
    $$ = "decimal(" + string($3.Bytes) + ")"
  }
  | DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP
  {
    $$ = "decimal(" + string($3.Bytes) + ", " + string($5.Bytes) + ")"
  }
  | GEOMETRY NK_LP NK_INTEGER NK_RP
  {
    $$ = "geometry(" + string($3.Bytes) + ")"
  }
  | JSON
  {
    $$ = "json"
  }
  | BLOB
  {
    $$ = "blob"
  }
  | MEDIUMBLOB
  {
    $$ = "mediumblob"
  }

type_name_default_len:
  BINARY
  {
    $$ = "binary"
  }
  | NCHAR
  {
    $$ = "nchar"
  }
  | VARCHAR
  {
    $$ = "varchar"
  }
  | VARBINARY
  {
    $$ = "varbinary"
  }

/************************************************ if_expression *******************************************************/
if_expression:
  IF NK_LP common_expression NK_COMMA common_expression NK_COMMA common_expression NK_RP
  {
    $$ = NewIfExpr(yylex, $3, $5, $7)
  }
  | IFNULL NK_LP common_expression NK_COMMA common_expression NK_RP
  {
    $$ = NewIfNullExpr(yylex, $3, $5)
  }
  | NVL NK_LP common_expression NK_COMMA common_expression NK_RP
  {
    $$ = NewIfNullExpr(yylex, $3, $5)
  }
  | NVL2 NK_LP common_expression NK_COMMA common_expression NK_COMMA common_expression NK_RP
  {
    $$ = NewFuncExpr(yylex, "nvl2", []Expr{$3, $5, $7})
  }
  | NULLIF NK_LP common_expression NK_COMMA common_expression NK_RP
  {
    $$ = NewNullIfExpr(yylex, $3, $5)
  }
  | COALESCE NK_LP expression_list NK_RP
  {
    $$ = NewCoalesceExpr(yylex, $3)
  }

/************************************************ case_when_expression ************************************************/
case_when_expression:
  CASE when_then_list case_when_else_opt END
  {
    $$ = NewCaseWhenExpr(yylex, nil, $2, $3)
  }
  | CASE common_expression when_then_list case_when_else_opt END
  {
    $$ = NewCaseWhenExpr(yylex, $2, $3, $4)
  }

when_then_list:
  when_then_expr
  {
    $$ = $1
  }
  | when_then_list when_then_expr
  {
    $$ = append($1, $2...)
  }

when_then_expr:
  WHEN common_expression THEN common_expression
  {
    $$ = []WhenThenExpr{{When: $2, Then: $4}}
  }

case_when_else_opt:
  /* empty */
  {
    $$ = nil
  }
  | ELSE common_expression
  {
    $$ = $2
  }

/************************************************ literal *************************************************************/
literal:
  NK_INTEGER
  {
    $$ = NewLiteralExpr(yylex, $1, LiteralInt)
  }
  | NK_FLOAT
  {
    $$ = NewLiteralExpr(yylex, $1, LiteralFloat)
  }
  | NK_STRING
  {
    $$ = NewLiteralExpr(yylex, $1, LiteralString)
  }
  | NK_BOOL
  {
    $$ = NewLiteralExpr(yylex, $1, LiteralBool)
  }
  | TIMESTAMP NK_STRING
  {
    tok := Token{Bytes: []byte("timestamp '" + string($2.Bytes) + "'")}
    $$ = NewLiteralExpr(yylex, tok, LiteralDuration)
  }
  | duration_literal
  {
    $$ = $1
  }
  | NULL
  {
    $$ = NewLiteralExpr(yylex, Token{Bytes: []byte("null")}, LiteralNull)
  }
  | NK_QUESTION
  {
    tok := $1
    tok.Bytes = []byte("?")
    $$ = NewLiteralExpr(yylex, tok, LiteralNull)
  }

/************************************************ boolean_value_expression ********************************************/
boolean_value_expression:
  boolean_primary
  {
    $$ = $1
  }
  | NOT boolean_primary
  {
    $$ = NewUnaryExpr(yylex, LOGIC_COND_TYPE_NOT, $2)
  }
  | boolean_value_expression OR boolean_value_expression
  {
    $$ = NewBinaryExpr(yylex, $1, LOGIC_COND_TYPE_OR, $3)
  }
  | boolean_value_expression AND boolean_value_expression
  {
    $$ = NewBinaryExpr(yylex, $1, LOGIC_COND_TYPE_AND, $3)
  }

boolean_primary:
  predicate
  {
    $$ = $1
  }
  | NK_LP boolean_value_expression NK_RP
  {
    $$ = $2
  }

/************************************************ predicate ***********************************************************/
predicate:
  expr_or_subquery compare_op expr_or_subquery
  {
    $$ = NewComparisonExpr(yylex, $1, $2, $3)
  }
  | expr_or_subquery BETWEEN expr_or_subquery AND expr_or_subquery
  {
    $$ = NewBetweenExpr(yylex, $1, $3, $5, false)
  }
  | expr_or_subquery NOT BETWEEN expr_or_subquery AND expr_or_subquery
  {
    $$ = NewBetweenExpr(yylex, $1, $4, $6, true)
  }
  | expr_or_subquery IS NULL
  {
    $$ = NewIsNullExpr(yylex, $1, false)
  }
  | expr_or_subquery IS NOT NULL
  {
    $$ = NewIsNullExpr(yylex, $1, true)
  }
  | ISNULL NK_LP expr_or_subquery NK_RP
  {
    $$ = NewIsNullExpr(yylex, $3, false)
  }
  | ISNOTNULL NK_LP expr_or_subquery NK_RP
  {
    $$ = NewIsNullExpr(yylex, $3, true)
  }
  | expr_or_subquery in_op in_predicate_value
  {
    $$ = NewInPredicateExpr(yylex, $1, $2, $3)
  }

compare_op:
  NK_LT
  {
    $$ = OP_TYPE_LOWER_THAN
  }
  | NK_GT
  {
    $$ = OP_TYPE_GREATER_THAN
  }
  | NK_LE
  {
    $$ = OP_TYPE_LOWER_EQUAL
  }
  | NK_GE
  {
    $$ = OP_TYPE_GREATER_EQUAL
  }
  | NK_NE
  {
    $$ = OP_TYPE_NOT_EQUAL
  }
  | NK_EQ
  {
    $$ = OP_TYPE_EQUAL
  }
  | LIKE
  {
    $$ = OP_TYPE_LIKE
  }
  | NOT LIKE
  {
    $$ = OP_TYPE_NOT_LIKE
  }
  | MATCH
  {
    $$ = OP_TYPE_MATCH
  }
  | NMATCH
  {
    $$ = OP_TYPE_NMATCH
  }
  | REGEXP
  {
    $$ = OP_TYPE_MATCH
  }
  | NOT REGEXP
  {
    $$ = OP_TYPE_NMATCH
  }
  | CONTAINS
  {
    $$ = OP_TYPE_JSON_CONTAINS
  }

in_op:
  IN
  {
    $$ = OP_TYPE_IN
  }
  | NOT IN
  {
    $$ = OP_TYPE_NOT_IN
  }

in_predicate_value:
  NK_LP literal_list NK_RP
  {
    $$ = &RawExpr{Kind: "in_list", Args: $2}
  }

literal_list:
  signed_literal
  {
    $$ = []Expr{$1}
  }
  | literal_list NK_COMMA signed_literal
  {
    $$ = append($1, $3)
  }

signed_literal:
  signed
  {
    lit := NewLiteralExpr(yylex, $1, LiteralFloat)
    $$ = &lit
  }
  | NK_STRING
  {
    lit := NewLiteralExpr(yylex, $1, LiteralString)
    $$ = &lit
  }
  | NK_BOOL
  {
    lit := NewLiteralExpr(yylex, $1, LiteralBool)
    $$ = &lit
  }
  | TIMESTAMP NK_STRING
  {
    tok := Token{Bytes: []byte("timestamp '" + string($2.Bytes) + "'")}
    lit := NewLiteralExpr(yylex, tok, LiteralDuration)
    $$ = &lit
  }
  | duration_literal
  {
    $$ = &$1
  }
  | NULL
  {
    lit := NewLiteralExpr(yylex, Token{Bytes: []byte("null")}, LiteralNull)
    $$ = &lit
  }
  | literal_func
  {
    $$ = $1
  }
  | NK_QUESTION
  {
    lit := NewLiteralExpr(yylex, Token{Bytes: []byte("?")}, LiteralNull)
    $$ = &lit
  }

/************************************************ search_condition ****************************************************/
search_condition:
  common_expression
  {
    $$ = $1
  }

search_condition_list:
  search_condition NK_COMMA search_condition
  {
    $$ = []Expr{$1, $3}
  }
  | search_condition_list NK_COMMA search_condition
  {
    $$ = append($1, $3)
  }

/************************************************ partition_by_clause *************************************************/
partition_by_clause_opt:
  /* empty */
  {
    $$ = nil
  }
  | PARTITION BY partition_list
  {
    $$ = NewPartitionByExpr(yylex, $3)
  }

partition_list:
  partition_item
  {
    $$ = []Expr{$1}
  }
  | partition_list NK_COMMA partition_item
  {
    $$ = append($1, $3)
  }

partition_item:
  expr_or_subquery
  {
    $$ = $1
  }
  | expr_or_subquery column_alias
  {
    $$ = &AliasedExpr{Expr: $1, Alias: $2}
  }
  | expr_or_subquery AS column_alias
  {
    $$ = &AliasedExpr{Expr: $1, Alias: $3}
  }

/************************************************ twindow_clause ******************************************************/
twindow_clause_opt:
  /* empty */
  {
    $$ = WindowExpr{}
  }
  | INTERVAL NK_LP interval_sliding_duration_literal NK_RP sliding_opt fill_opt
  {
    $$ = WindowExpr{Interval: $3, Sliding: $5, Fill: $6}
  }
  | INTERVAL NK_LP interval_sliding_duration_literal NK_COMMA interval_sliding_duration_literal NK_RP sliding_opt fill_opt
  {
    $$ = WindowExpr{Interval: $3, Offset: $5, Sliding: $7, Fill: $8}
  }
  | INTERVAL NK_LP interval_sliding_duration_literal NK_COMMA AUTO NK_RP sliding_opt fill_opt
  {
    $$ = NewIntervalAutoWindowExpr(yylex, $3, $5, $7, $8)
  }
  | SESSION NK_LP column_reference NK_COMMA interval_sliding_duration_literal NK_RP
  {
    $$ = WindowExpr{Session: $3, SessionGap: $5}
  }
  | STATE_WINDOW NK_LP expr_or_subquery state_window_opt NK_RP true_for_opt
  {
    $$ = WindowExpr{StateWindow: $3, StateWindowOpt: $4, TrueFor: $6}
  }
  | EVENT_WINDOW START WITH search_condition END WITH search_condition true_for_opt
  {
    $$ = WindowExpr{EventWindowStart: $4, EventWindowEnd: $7, TrueFor: $8}
  }
  | COUNT_WINDOW NK_LP count_window_args NK_RP
  {
    $$ = WindowExpr{CountWindow: $3.Count, CountWindowSlide: $3.Slide, CountWindowCols: $3.Cols}
  }
  | ANOMALY_WINDOW NK_LP expr_or_subquery NK_RP
  {
    $$ = WindowExpr{AnomalyWindow: $3}
  }
  | ANOMALY_WINDOW NK_LP expr_or_subquery NK_COMMA NK_STRING NK_RP
  {
    $$ = WindowExpr{AnomalyWindow: $3, AnomalyTag: $5}
  }

interval_opt:
  /* empty */
  {
    $$ = WindowExpr{}
  }
  | INTERVAL NK_LP interval_sliding_duration_literal NK_RP
  {
    $$ = WindowExpr{Interval: $3}
  }
  | INTERVAL NK_LP interval_sliding_duration_literal NK_COMMA interval_sliding_duration_literal NK_RP
  {
    $$ = WindowExpr{Interval: $3, Offset: $5}
  }

sliding_expr:
  interval_sliding_duration_literal
  {
    $$ = WindowExpr{Sliding: $1}
  }
  | interval_sliding_duration_literal NK_COMMA interval_sliding_duration_literal
  {
    $$ = WindowExpr{Sliding: $1, Offset: $3}
  }

count_window_args:
  NK_INTEGER
  {
    $$ = CountWindowArgs{Count: $1}
  }
  | NK_INTEGER NK_COMMA NK_INTEGER
  {
    $$ = CountWindowArgs{Count: $1, Slide: $3}
  }
  | NK_INTEGER NK_COMMA column_name_list
  {
    $$ = CountWindowArgs{Count: $1, Cols: $3}
  }
  | NK_INTEGER NK_COMMA NK_INTEGER NK_COMMA column_name_list
  {
    $$ = CountWindowArgs{Count: $1, Slide: $3, Cols: $5}
  }

interval_sliding_duration_literal:
  NK_VARIABLE
  {
    $$ = Literal{Val: $1, Type: LiteralDuration}
  }
  | NK_STRING
  {
    $$ = Literal{Val: $1, Type: LiteralDuration}
  }
  | NK_INTEGER
  {
    $$ = Literal{Val: $1, Type: LiteralDuration}
  }

duration_literal:
  NK_VARIABLE
  {
    $$ = Literal{Val: $1, Type: LiteralDuration}
  }

sliding_opt:
  /* empty */
  {
    $$ = Literal{}
  }
  | SLIDING NK_LP interval_sliding_duration_literal NK_RP
  {
    $$ = $3
  }

offset_opt:
  /* empty */
  {
    $$ = Literal{}
  }
  | NK_COMMA interval_sliding_duration_literal
  {
    $$ = $2
  }

extend_literal:
  NK_INTEGER
  {
    $$ = Literal{Val: $1, Type: LiteralInt}
  }

signed_integer:
  NK_INTEGER
  {
    $$ = Literal{Val: $1, Type: LiteralInt}
  }
  | NK_PLUS NK_INTEGER
  {
    $$ = Literal{Val: $2, Type: LiteralInt}
  }
  | NK_MINUS NK_INTEGER
  {
    tok := Token{Type: $2.Type, Bytes: append([]byte("-"), $2.Bytes...)}
    $$ = Literal{Val: tok, Type: LiteralInt}
  }

signed_float:
  NK_FLOAT
  {
    $$ = $1
  }
  | NK_PLUS NK_FLOAT
  {
    $$ = $2
  }
  | NK_MINUS NK_FLOAT
  {
    tok := Token{Type: $2.Type, Bytes: append([]byte("-"), $2.Bytes...)}
    $$ = tok
  }

signed:
  signed_integer
  {
    $$ = $1.Val
  }
  | signed_float
  {
    $$ = $1
  }

zeroth_literal:
  signed_integer
  {
    $$ = $1
  }
  | NK_STRING
  {
    $$ = Literal{Val: $1, Type: LiteralString}
  }
  | NK_BOOL
  {
    $$ = Literal{Val: $1, Type: LiteralBool}
  }

state_window_opt:
  /* empty */
  {
    $$ = StateWindowOpt{}
  }
  | NK_COMMA extend_literal
  {
    $$ = StateWindowOpt{HasExtend: true, Extend: $2}
  }
  | NK_COMMA extend_literal NK_COMMA zeroth_literal
  {
    $$ = StateWindowOpt{HasExtend: true, Extend: $2, HasZeroth: true, Zeroth: $4}
  }

true_for_opt:
  /* empty */
  {
    $$ = Literal{}
  }
  | TRUE_FOR NK_LP interval_sliding_duration_literal NK_RP
  {
    $$ = $3
  }

/************************************************ fill_clause *********************************************************/
fill_opt:
  /* empty */
  {
    $$ = nil
  }
  | fill_value
  {
    $$ = $1
  }
  | FILL NK_LP fill_mode NK_RP
  {
    $$ = &FillExpr{Mode: $3}
  }

interp_fill_opt:
  /* empty */
  {
    $$ = nil
  }
  | fill_value
  {
    $$ = $1
  }
  | FILL NK_LP fill_position_mode_extension NK_COMMA expression_list NK_RP
  {
    $$ = &FillExpr{Mode: $3, Values: $5}
  }
  | FILL NK_LP interp_fill_mode NK_RP
  {
    $$ = &FillExpr{Mode: $3}
  }

fill_value:
  FILL NK_LP VALUE NK_COMMA expression_list NK_RP
  {
    $$ = &FillExpr{Mode: FILL_MODE_VALUE, Values: $5}
  }
  | FILL NK_LP VALUE_F NK_COMMA expression_list NK_RP
  {
    $$ = &FillExpr{Mode: FILL_MODE_VALUE_F, Values: $5}
  }

fill_mode:
  NONE
  {
    $$ = FILL_MODE_NONE
  }
  | NULL
  {
    $$ = FILL_MODE_NULL
  }
  | NULL_F
  {
    $$ = FILL_MODE_NULL_F
  }
  | LINEAR
  {
    $$ = FILL_MODE_LINEAR
  }
  | fill_position_mode
  {
    $$ = $1
  }

fill_position_mode:
  PREV
  {
    $$ = FILL_MODE_PREV
  }
  | NEXT
  {
    $$ = FILL_MODE_NEXT
  }

fill_position_mode_extension:
  fill_position_mode
  {
    $$ = $1
  }
  | NEAR
  {
    $$ = FILL_MODE_NEAR
  }

interp_fill_mode:
  fill_mode
  {
    $$ = $1
  }
  | NEAR
  {
    $$ = FILL_MODE_NEAR
  }

/************************************************ tag_mode ************************************************************/
tag_mode_opt:
  /* empty */
  {
    $$ = false
  }
  | TAGS
  {
    $$ = true
  }

/************************************************ parenthesized_joined_table ******************************************/
parenthesized_joined_table:
  NK_LP joined_table NK_RP
  {
    $$ = $2
  }
  | NK_LP parenthesized_joined_table NK_RP
  {
    $$ = $2
  }

// todo too large
/************************************************ create drop update xnode ***************************************/

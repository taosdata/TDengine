//! TDengine error codes.
//! THIS IS AUTO GENERATED FROM TDENGINE <taoserror.h>, MAKE SURE YOU KNOW WHAT YOU ARE CHANING.

use std::fmt;

use num_enum::{FromPrimitive, IntoPrimitive};

/// TDengine error code.
#[derive(Debug, Clone, Copy, Eq, PartialEq, FromPrimitive, IntoPrimitive)]
#[repr(i32)]
#[derive(serde::Deserialize)]
pub enum TaosCode {
    /// Success, 0
    Success = 0x0000,
    /// RPC_ACTION_IN_PROGRESS: Action in progress
    RpcActionInProgress = 0x0001,
    /// RPC_AUTH_REQUIRED: Authentication required
    RpcAuthRequired = 0x0002,
    /// RPC_AUTH_FAILURE: Authentication failure
    RpcAuthFailure = 0x0003,
    /// RPC_REDIRECT: Redirect
    RpcRedirect = 0x0004,
    /// RPC_NOT_READY: System not ready
    RpcNotReady = 0x0005,
    /// RPC_ALREADY_PROCESSED: Message already processed
    RpcAlreadyProcessed = 0x0006,
    /// RPC_LAST_SESSION_NOT_FINISHED: Last session not finished
    RpcLastSessionNotFinished = 0x0007,
    /// RPC_MISMATCHED_LINK_ID: Mismatched meter id
    RpcMismatchedLinkId = 0x0008,
    /// RPC_TOO_SLOW: Processing of request timed out
    RpcTooSlow = 0x0009,
    /// RPC_MAX_SESSIONS: Number of sessions reached limit
    RpcMaxSessions = 0x000A,
    /// RPC_NETWORK_UNAVAIL: Unable to establish connection
    RpcNetworkUnavail = 0x000B,
    /// RPC_APP_ERROR: Unexpected generic error in RPC
    RpcAppError = 0x000C,
    /// RPC_UNEXPECTED_RESPONSE: Unexpected response
    RpcUnexpectedResponse = 0x000D,
    /// RPC_INVALID_VALUE: Invalid value
    RpcInvalidValue = 0x000E,
    /// RPC_INVALID_TRAN_ID: Invalid transaction id
    RpcInvalidTranId = 0x000F,
    /// RPC_INVALID_SESSION_ID: Invalid session id
    RpcInvalidSessionId = 0x0010,
    /// RPC_INVALID_MSG_TYPE: Invalid message type
    RpcInvalidMsgType = 0x0011,
    /// RPC_INVALID_RESPONSE_TYPE: Invalid response type
    RpcInvalidResponseType = 0x0012,
    /// RPC_INVALID_TIME_STAMP: Client and server's time is not synchronized
    RpcInvalidTimeStamp = 0x0013,
    /// APP_NOT_READY: Database not ready
    AppNotReady = 0x0014,
    /// RPC_FQDN_ERROR: Unable to resolve FQDN
    RpcFqdnError = 0x0015,
    /// RPC_INVALID_VERSION: Invalid app version
    RpcInvalidVersion = 0x0016,
    /// COM_OPS_NOT_SUPPORT: Operation not supported
    ComOpsNotSupport = 0x0100,
    /// COM_MEMORY_CORRUPTED: Memory corrupted
    ComMemoryCorrupted = 0x0101,
    /// COM_OUT_OF_MEMORY: Out of memory
    ComOutOfMemory = 0x0102,
    /// COM_INVALID_CFG_MSG: Invalid config message
    ComInvalidCfgMsg = 0x0103,
    /// COM_FILE_CORRUPTED: Data file corrupted
    ComFileCorrupted = 0x0104,
    /// REF_NO_MEMORY: Ref out of memory
    RefNoMemory = 0x0105,
    /// REF_FULL: too many Ref Objs
    RefFull = 0x0106,
    /// REF_ID_REMOVED: Ref ID is removed
    RefIdRemoved = 0x0107,
    /// REF_INVALID_ID: Invalid Ref ID
    RefInvalidId = 0x0108,
    /// REF_ALREADY_EXIST: Ref is already there
    RefAlreadyExist = 0x0109,
    /// REF_NOT_EXIST: Ref is not there
    RefNotExist = 0x010A,
    /// TSC_INVALID_SQL: Invalid SQL statement
    TscInvalidSql = 0x0200,
    /// TSC_INVALID_QHANDLE: Invalid qhandle
    TscInvalidQhandle = 0x0201,
    /// TSC_INVALID_TIME_STAMP: Invalid combination of client/service time
    TscInvalidTimeStamp = 0x0202,
    /// TSC_INVALID_VALUE: Invalid value in client
    TscInvalidValue = 0x0203,
    /// TSC_INVALID_VERSION: Invalid client version
    TscInvalidVersion = 0x0204,
    /// TSC_INVALID_IE: Invalid client ie
    TscInvalidIe = 0x0205,
    /// TSC_INVALID_FQDN: Invalid host name
    TscInvalidFqdn = 0x0206,
    /// TSC_INVALID_USER_LENGTH: Invalid user name
    TscInvalidUserLength = 0x0207,
    /// TSC_INVALID_PASS_LENGTH: Invalid password
    TscInvalidPassLength = 0x0208,
    /// TSC_INVALID_DB_LENGTH: Database name too long
    TscInvalidDbLength = 0x0209,
    /// TSC_INVALID_TABLE_ID_LENGTH: Table name too long
    TscInvalidTableIdLength = 0x020A,
    /// TSC_INVALID_CONNECTION: Invalid connection
    TscInvalidConnection = 0x020B,
    /// TSC_OUT_OF_MEMORY: System out of memory
    TscOutOfMemory = 0x020C,
    /// TSC_NO_DISKSPACE: System out of disk space
    TscNoDiskspace = 0x020D,
    /// TSC_QUERY_CACHE_ERASED: Query cache erased
    TscQueryCacheErased = 0x020E,
    /// TSC_QUERY_CANCELLED: Query terminated
    TscQueryCancelled = 0x020F,
    /// TSC_SORTED_RES_TOO_MANY: Result set too large to be sorted
    TscSortedResTooMany = 0x0210,
    /// TSC_APP_ERROR: Application error
    TscAppError = 0x0211,
    /// TSC_ACTION_IN_PROGRESS: Action in progress
    TscActionInProgress = 0x0212,
    /// TSC_DISCONNECTED: Disconnected from service
    TscDisconnected = 0x0213,
    /// TSC_NO_WRITE_AUTH: No write permission
    TscNoWriteAuth = 0x0214,
    /// TSC_CONN_KILLED: Connection killed
    TscConnKilled = 0x0215,
    /// TSC_SQL_SYNTAX_ERROR: Syntax error in SQL
    TscSqlSyntaxError = 0x0216,
    /// TSC_DB_NOT_SELECTED: Database not specified or available
    TscDbNotSelected = 0x0217,
    /// TSC_INVALID_TABLE_NAME: Table does not exist
    TscInvalidTableName = 0x0218,
    /// TSC_EXCEED_SQL_LIMIT: SQL statement too long check maxSQLLength config
    TscExceedSqlLimit = 0x0219,
    /// MND_MSG_NOT_PROCESSED: Message not processed
    MndMsgNotProcessed = 0x0300,
    /// MND_ACTION_IN_PROGRESS: Message is progressing
    MndActionInProgress = 0x0301,
    /// MND_ACTION_NEED_REPROCESSED: Messag need to be reprocessed
    MndActionNeedReprocessed = 0x0302,
    /// MND_NO_RIGHTS: Insufficient privilege for operation
    MndNoRights = 0x0303,
    /// MND_APP_ERROR: Unexpected generic error in mnode
    MndAppError = 0x0304,
    /// MND_INVALID_CONNECTION: Invalid message connection
    MndInvalidConnection = 0x0305,
    /// MND_INVALID_MSG_VERSION: Incompatible protocol version
    MndInvalidMsgVersion = 0x0306,
    /// MND_INVALID_MSG_LEN: Invalid message length
    MndInvalidMsgLen = 0x0307,
    /// MND_INVALID_MSG_TYPE: Invalid message type
    MndInvalidMsgType = 0x0308,
    /// MND_TOO_MANY_SHELL_CONNS: Too many connections
    MndTooManyShellConns = 0x0309,
    /// MND_OUT_OF_MEMORY: Out of memory in mnode
    MndOutOfMemory = 0x030A,
    /// MND_INVALID_SHOWOBJ: Data expired
    MndInvalidShowobj = 0x030B,
    /// MND_INVALID_QUERY_ID: Invalid query id
    MndInvalidQueryId = 0x030C,
    /// MND_INVALID_STREAM_ID: Invalid stream id
    MndInvalidStreamId = 0x030D,
    /// MND_INVALID_CONN_ID: Invalid connection id
    MndInvalidConnId = 0x030E,
    /// MND_MNODE_IS_RUNNING: mnode is alreay running
    MndMnodeIsRunning = 0x0310,
    /// MND_FAILED_TO_CONFIG_SYNC: failed to config sync
    MndFailedToConfigSync = 0x0311,
    /// MND_FAILED_TO_START_SYNC: failed to start sync
    MndFailedToStartSync = 0x0312,
    /// MND_FAILED_TO_CREATE_DIR: failed to create mnode dir
    MndFailedToCreateDir = 0x0313,
    /// MND_FAILED_TO_INIT_STEP: failed to init components
    MndFailedToInitStep = 0x0314,
    /// MND_SDB_OBJ_ALREADY_THERE: Object already there
    MndSdbObjAlreadyThere = 0x0320,
    /// MND_SDB_ERROR: Unexpected generic error in sdb
    MndSdbError = 0x0321,
    /// MND_SDB_INVALID_TABLE_TYPE: Invalid table type
    MndSdbInvalidTableType = 0x0322,
    /// MND_SDB_OBJ_NOT_THERE: Object not there
    MndSdbObjNotThere = 0x0323,
    /// MND_SDB_INVAID_META_ROW: Invalid meta row
    MndSdbInvaidMetaRow = 0x0324,
    /// MND_SDB_INVAID_KEY_TYPE: Invalid key type
    MndSdbInvaidKeyType = 0x0325,
    /// MND_DNODE_ALREADY_EXIST: DNode already exists
    MndDnodeAlreadyExist = 0x0330,
    /// MND_DNODE_NOT_EXIST: DNode does not exist
    MndDnodeNotExist = 0x0331,
    /// MND_VGROUP_NOT_EXIST: VGroup does not exist
    MndVgroupNotExist = 0x0332,
    /// MND_NO_REMOVE_MASTER: Master DNode cannot be removed
    MndNoRemoveMaster = 0x0333,
    /// MND_NO_ENOUGH_DNODES: Out of DNodes
    MndNoEnoughDnodes = 0x0334,
    /// MND_CLUSTER_CFG_INCONSISTENT: Cluster cfg inconsistent
    MndClusterCfgInconsistent = 0x0335,
    /// MND_INVALID_DNODE_CFG_OPTION: Invalid dnode cfg option
    MndInvalidDnodeCfgOption = 0x0336,
    /// MND_BALANCE_ENABLED: Balance already enabled
    MndBalanceEnabled = 0x0337,
    /// MND_VGROUP_NOT_IN_DNODE: Vgroup not in dnode
    MndVgroupNotInDnode = 0x0338,
    /// MND_VGROUP_ALREADY_IN_DNODE: Vgroup already in dnode
    MndVgroupAlreadyInDnode = 0x0339,
    /// MND_DNODE_NOT_FREE: Dnode not avaliable
    MndDnodeNotFree = 0x033A,
    /// MND_INVALID_CLUSTER_ID: Cluster id not match
    MndInvalidClusterId = 0x033B,
    /// MND_NOT_READY: Cluster not ready
    MndNotReady = 0x033C,
    /// MND_DNODE_ID_NOT_CONFIGURED: Dnode Id not configured
    MndDnodeIdNotConfigured = 0x033D,
    /// MND_DNODE_EP_NOT_CONFIGURED: Dnode Ep not configured
    MndDnodeEpNotConfigured = 0x033E,
    /// MND_ACCT_ALREADY_EXIST: Account already exists
    MndAcctAlreadyExist = 0x0340,
    /// MND_INVALID_ACCT: Invalid account
    MndInvalidAcct = 0x0341,
    /// MND_INVALID_ACCT_OPTION: Invalid account options
    MndInvalidAcctOption = 0x0342,
    /// MND_ACCT_EXPIRED: Account authorization has expired
    MndAcctExpired = 0x0343,
    /// MND_USER_ALREADY_EXIST: User already exists
    MndUserAlreadyExist = 0x0350,
    /// MND_INVALID_USER: Invalid user
    MndInvalidUser = 0x0351,
    /// MND_INVALID_USER_FORMAT: Invalid user format
    MndInvalidUserFormat = 0x0352,
    /// MND_INVALID_PASS_FORMAT: Invalid password format
    MndInvalidPassFormat = 0x0353,
    /// MND_NO_USER_FROM_CONN: Can not get user from conn
    MndNoUserFromConn = 0x0354,
    /// MND_TOO_MANY_USERS: Too many users
    MndTooManyUsers = 0x0355,
    /// MND_TABLE_ALREADY_EXIST: Table already exists
    MndTableAlreadyExist = 0x0360,
    /// MND_INVALID_TABLE_ID: Table name too long
    MndInvalidTableId = 0x0361,
    /// MND_INVALID_TABLE_NAME: Table does not exist
    MndInvalidTableName = 0x0362,
    /// MND_INVALID_TABLE_TYPE: Invalid table type in tsdb
    MndInvalidTableType = 0x0363,
    /// MND_TOO_MANY_TAGS: Too many tags
    MndTooManyTags = 0x0364,
    /// MND_TOO_MANY_COLUMNS: Too many columns
    MndTooManyColumns = 0x0365,
    /// MND_TOO_MANY_TIMESERIES: Too many time series
    MndTooManyTimeseries = 0x0366,
    /// MND_NOT_SUPER_TABLE: Not super table
    MndNotSuperTable = 0x0367,
    /// MND_COL_NAME_TOO_LONG: Tag name too long
    MndColNameTooLong = 0x0368,
    /// MND_TAG_ALREAY_EXIST: Tag already exists
    MndTagAlreayExist = 0x0369,
    /// MND_TAG_NOT_EXIST: Tag does not exist
    MndTagNotExist = 0x036A,
    /// MND_FIELD_ALREAY_EXIST: Field already exists
    MndFieldAlreayExist = 0x036B,
    /// MND_FIELD_NOT_EXIST: Field does not exist
    MndFieldNotExist = 0x036C,
    /// MND_INVALID_STABLE_NAME: Super table does not exist
    MndInvalidStableName = 0x036D,
    /// MND_INVALID_CREATE_TABLE_MSG: Invalid create table message
    MndInvalidCreateTableMsg = 0x036E,
    /// MND_DB_NOT_SELECTED: Database not specified or available
    MndDbNotSelected = 0x0380,
    /// MND_DB_ALREADY_EXIST: Database already exists
    MndDbAlreadyExist = 0x0381,
    /// MND_INVALID_DB_OPTION: Invalid database options
    MndInvalidDbOption = 0x0382,
    /// MND_INVALID_DB: Invalid database name
    MndInvalidDb = 0x0383,
    /// MND_MONITOR_DB_FORBIDDEN: Cannot delete monitor database
    MndMonitorDbForbidden = 0x0384,
    /// MND_TOO_MANY_DATABASES: Too many databases for account
    MndTooManyDatabases = 0x0385,
    /// MND_DB_IN_DROPPING: Database not available
    MndDbInDropping = 0x0386,
    /// MND_VGROUP_NOT_READY: Database unsynced
    MndVgroupNotReady = 0x0387,
    /// MND_INVALID_DB_OPTION_DAYS: Invalid database option: days out of range
    MndInvalidDbOptionDays = 0x0390,
    /// MND_INVALID_DB_OPTION_KEEP: Invalid database option: keep >= keep1 >= keep0 >= days
    MndInvalidDbOptionKeep = 0x0391,
    /// MND_INVALID_TOPIC: Invalid topic nam
    MndInvalidTopic = 0x0392,
    /// MND_INVALID_TOPIC_OPTION: Invalid topic optio
    MndInvalidTopicOption = 0x0393,
    /// MND_INVALID_TOPIC_PARTITONS: Invalid topic partitons num, valid range: [1, 1000
    MndInvalidTopicPartitons = 0x0394,
    /// MND_TOPIC_ALREADY_EXIST: Topic already exist
    MndTopicAlreadyExist = 0x0395,
    /// DND_MSG_NOT_PROCESSED: Message not processed
    DndMsgNotProcessed = 0x0400,
    /// DND_OUT_OF_MEMORY: Dnode out of memory
    DndOutOfMemory = 0x0401,
    /// DND_NO_WRITE_ACCESS: No permission for disk files in dnode
    DndNoWriteAccess = 0x0402,
    /// DND_INVALID_MSG_LEN: Invalid message length
    DndInvalidMsgLen = 0x0403,
    /// DND_ACTION_IN_PROGRESS: Action in progress
    DndActionInProgress = 0x0404,
    /// DND_TOO_MANY_VNODES: Too many vnode directories
    DndTooManyVnodes = 0x0405,
    /// VND_ACTION_IN_PROGRESS: Action in progress
    VndActionInProgress = 0x0500,
    /// VND_MSG_NOT_PROCESSED: Message not processed
    VndMsgNotProcessed = 0x0501,
    /// VND_ACTION_NEED_REPROCESSED: Action need to be reprocessed
    VndActionNeedReprocessed = 0x0502,
    /// VND_INVALID_VGROUP_ID: Invalid Vgroup ID
    VndInvalidVgroupId = 0x0503,
    /// VND_INIT_FAILED: Vnode initialization failed
    VndInitFailed = 0x0504,
    /// VND_NO_DISKSPACE: System out of disk space
    VndNoDiskspace = 0x0505,
    /// VND_NO_DISK_PERMISSIONS: No write permission for disk files
    VndNoDiskPermissions = 0x0506,
    /// VND_NO_SUCH_FILE_OR_DIR: Missing data file
    VndNoSuchFileOrDir = 0x0507,
    /// VND_OUT_OF_MEMORY: Out of memory
    VndOutOfMemory = 0x0508,
    /// VND_APP_ERROR: Unexpected generic error in vnode
    VndAppError = 0x0509,
    /// VND_INVALID_VRESION_FILE: Invalid version file
    VndInvalidVresionFile = 0x050A,
    /// VND_IS_FULL: Database memory is full for commit failed
    VndIsFull = 0x050B,
    /// VND_IS_FLOWCTRL: Database memory is full for waiting commit
    VndIsFlowctrl = 0x050C,
    /// VND_IS_DROPPING: Database is dropping
    VndIsDropping = 0x050D,
    /// VND_IS_BALANCING: Database is balancing
    VndIsBalancing = 0x050E,
    /// VND_NOT_SYNCED: Database suspended
    VndNotSynced = 0x0511,
    /// VND_NO_WRITE_AUTH: Database write operation denied
    VndNoWriteAuth = 0x0512,
    /// VND_IS_SYNCING: Database is syncing
    VndIsSyncing = 0x0513,
    /// TDB_INVALID_TABLE_ID: Invalid table ID
    TdbInvalidTableId = 0x0600,
    /// TDB_INVALID_TABLE_TYPE: Invalid table type
    TdbInvalidTableType = 0x0601,
    /// TDB_IVD_TB_SCHEMA_VERSION: Invalid table schema version
    TdbIvdTbSchemaVersion = 0x0602,
    /// TDB_TABLE_ALREADY_EXIST: Table already exists
    TdbTableAlreadyExist = 0x0603,
    /// TDB_INVALID_CONFIG: Invalid configuration
    TdbInvalidConfig = 0x0604,
    /// TDB_INIT_FAILED: Tsdb init failed
    TdbInitFailed = 0x0605,
    /// TDB_NO_DISKSPACE: No diskspace for tsdb
    TdbNoDiskspace = 0x0606,
    /// TDB_NO_DISK_PERMISSIONS: No permission for disk files
    TdbNoDiskPermissions = 0x0607,
    /// TDB_FILE_CORRUPTED: Data file(s) corrupted
    TdbFileCorrupted = 0x0608,
    /// TDB_OUT_OF_MEMORY: Out of memory
    TdbOutOfMemory = 0x0609,
    /// TDB_TAG_VER_OUT_OF_DATE: Tag too old
    TdbTagVerOutOfDate = 0x060A,
    /// TDB_TIMESTAMP_OUT_OF_RANGE: Timestamp data out of range
    TdbTimestampOutOfRange = 0x060B,
    /// TDB_SUBMIT_MSG_MSSED_UP: Submit message is messed up
    TdbSubmitMsgMssedUp = 0x060C,
    /// TDB_INVALID_ACTION: Invalid operation
    TdbInvalidAction = 0x060D,
    /// TDB_INVALID_CREATE_TB_MSG: Invalid creation of table
    TdbInvalidCreateTbMsg = 0x060E,
    /// TDB_NO_TABLE_DATA_IN_MEM: No table data in memory skiplist
    TdbNoTableDataInMem = 0x060F,
    /// TDB_FILE_ALREADY_EXISTS: File already exists
    TdbFileAlreadyExists = 0x0610,
    /// TDB_TABLE_RECONFIGURE: Need to reconfigure table
    TdbTableReconfigure = 0x0611,
    /// TDB_IVD_CREATE_TABLE_INFO: Invalid information to create table
    TdbIvdCreateTableInfo = 0x0612,
    /// TDB_NO_AVAIL_DISK: No available disk
    TdbNoAvailDisk = 0x0613,
    /// TDB_MESSED_MSG: TSDB messed message
    TdbMessedMsg = 0x0614,
    /// TDB_IVLD_TAG_VAL: TSDB invalid tag value
    TdbIvldTagVal = 0x0615,
    /// QRY_INVALID_QHANDLE: Invalid handle
    QryInvalidQhandle = 0x0700,
    /// QRY_INVALID_MSG: Invalid message
    QryInvalidMsg = 0x0701,
    /// QRY_NO_DISKSPACE: No diskspace for query
    QryNoDiskspace = 0x0702,
    /// QRY_OUT_OF_MEMORY: System out of memory
    QryOutOfMemory = 0x0703,
    /// QRY_APP_ERROR: Unexpected generic error in query
    QryAppError = 0x0704,
    /// QRY_DUP_JOIN_KEY: Duplicated join key
    QryDupJoinKey = 0x0705,
    /// QRY_EXCEED_TAGS_LIMIT: Tag conditon too many
    QryExceedTagsLimit = 0x0706,
    /// QRY_NOT_READY: Query not ready
    QryNotReady = 0x0707,
    /// QRY_HAS_RSP: Query should response
    QryHasRsp = 0x0708,
    /// QRY_IN_EXEC: Multiple retrieval of this query
    QryInExec = 0x0709,
    /// QRY_TOO_MANY_TIMEWINDOW: Too many time window in query
    QryTooManyTimewindow = 0x070A,
    /// QRY_NOT_ENOUGH_BUFFER: Query buffer limit has reached
    QryNotEnoughBuffer = 0x070B,
    /// QRY_INCONSISTAN: File inconsistency in replica
    QryInconsistan = 0x070C,
    /// GRANT_EXPIRED: License expired
    GrantExpired = 0x0800,
    /// GRANT_DNODE_LIMITED: DNode creation limited by licence
    GrantDnodeLimited = 0x0801,
    /// GRANT_ACCT_LIMITED: Account creation limited by license
    GrantAcctLimited = 0x0802,
    /// GRANT_TIMESERIES_LIMITED: Table creation limited by license
    GrantTimeseriesLimited = 0x0803,
    /// GRANT_DB_LIMITED: DB creation limited by license
    GrantDbLimited = 0x0804,
    /// GRANT_USER_LIMITED: User creation limited by license
    GrantUserLimited = 0x0805,
    /// GRANT_CONN_LIMITED: Conn creation limited by license
    GrantConnLimited = 0x0806,
    /// GRANT_STREAM_LIMITED: Stream creation limited by license
    GrantStreamLimited = 0x0807,
    /// GRANT_SPEED_LIMITED: Write speed limited by license
    GrantSpeedLimited = 0x0808,
    /// GRANT_STORAGE_LIMITED: Storage capacity limited by license
    GrantStorageLimited = 0x0809,
    /// GRANT_QUERYTIME_LIMITED: Query time limited by license
    GrantQuerytimeLimited = 0x080A,
    /// GRANT_CPU_LIMITED: CPU cores limited by license
    GrantCpuLimited = 0x080B,
    /// SYN_INVALID_CONFIG: Invalid Sync Configuration
    SynInvalidConfig = 0x0900,
    /// SYN_NOT_ENABLED: Sync module not enabled
    SynNotEnabled = 0x0901,
    /// SYN_INVALID_VERSION: Invalid Sync version
    SynInvalidVersion = 0x0902,
    /// SYN_CONFIRM_EXPIRED: Sync confirm expired
    SynConfirmExpired = 0x0903,
    /// SYN_TOO_MANY_FWDINFO: Too many sync fwd infos
    SynTooManyFwdinfo = 0x0904,
    /// SYN_MISMATCHED_PROTOCOL: Mismatched protocol
    SynMismatchedProtocol = 0x0905,
    /// SYN_MISMATCHED_CLUSTERID: Mismatched clusterId
    SynMismatchedClusterid = 0x0906,
    /// SYN_MISMATCHED_SIGNATURE: Mismatched signature
    SynMismatchedSignature = 0x0907,
    /// SYN_INVALID_CHECKSUM: Invalid msg checksum
    SynInvalidChecksum = 0x0908,
    /// SYN_INVALID_MSGLEN: Invalid msg length
    SynInvalidMsglen = 0x0909,
    /// SYN_INVALID_MSGTYPE: Invalid msg type
    SynInvalidMsgtype = 0x090A,
    /// WAL_APP_ERROR: Unexpected generic error in wal
    WalAppError = 0x1000,
    /// WAL_FILE_CORRUPTED: WAL file is corrupted
    WalFileCorrupted = 0x1001,
    /// WAL_SIZE_LIMIT: WAL size exceeds limit
    WalSizeLimit = 0x1002,
    /// HTTP_SERVER_OFFLINE: http server is not onlin
    HttpServerOffline = 0x1100,
    /// HTTP_UNSUPPORT_URL: url is not support
    HttpUnsupportUrl = 0x1101,
    /// HTTP_INVALID_URL: nvalid url format
    HttpInvalidUrl = 0x1102,
    /// HTTP_NO_ENOUGH_MEMORY: no enough memory
    HttpNoEnoughMemory = 0x1103,
    /// HTTP_REQUSET_TOO_BIG: request size is too big
    HttpRequsetTooBig = 0x1104,
    /// HTTP_NO_AUTH_INFO: no auth info input
    HttpNoAuthInfo = 0x1105,
    /// HTTP_NO_MSG_INPUT: request is empty
    HttpNoMsgInput = 0x1106,
    /// HTTP_NO_SQL_INPUT: no sql input
    HttpNoSqlInput = 0x1107,
    /// HTTP_NO_EXEC_USEDB: no need to execute use db cmd
    HttpNoExecUsedb = 0x1108,
    /// HTTP_SESSION_FULL: session list was full
    HttpSessionFull = 0x1109,
    /// HTTP_GEN_TAOSD_TOKEN_ERR: generate taosd token error
    HttpGenTaosdTokenErr = 0x110A,
    /// HTTP_INVALID_MULTI_REQUEST: size of multi request is 0
    HttpInvalidMultiRequest = 0x110B,
    /// HTTP_CREATE_GZIP_FAILED: failed to create gzip
    HttpCreateGzipFailed = 0x110C,
    /// HTTP_FINISH_GZIP_FAILED: failed to finish gzip
    HttpFinishGzipFailed = 0x110D,
    /// HTTP_LOGIN_FAILED: failed to login
    HttpLoginFailed = 0x110E,
    /// HTTP_INVALID_VERSION: invalid http version
    HttpInvalidVersion = 0x1120,
    /// HTTP_INVALID_CONTENT_LENGTH: invalid content length
    HttpInvalidContentLength = 0x1121,
    /// HTTP_INVALID_AUTH_TYPE: invalid type of Authorization
    HttpInvalidAuthType = 0x1122,
    /// HTTP_INVALID_AUTH_FORMAT: invalid format of Authorization
    HttpInvalidAuthFormat = 0x1123,
    /// HTTP_INVALID_BASIC_AUTH: invalid basic Authorization
    HttpInvalidBasicAuth = 0x1124,
    /// HTTP_INVALID_TAOSD_AUTH: invalid taosd Authorization
    HttpInvalidTaosdAuth = 0x1125,
    /// HTTP_PARSE_METHOD_FAILED: failed to parse method
    HttpParseMethodFailed = 0x1126,
    /// HTTP_PARSE_TARGET_FAILED: failed to parse target
    HttpParseTargetFailed = 0x1127,
    /// HTTP_PARSE_VERSION_FAILED: failed to parse http version
    HttpParseVersionFailed = 0x1128,
    /// HTTP_PARSE_SP_FAILED: failed to parse sp
    HttpParseSpFailed = 0x1129,
    /// HTTP_PARSE_STATUS_FAILED: failed to parse status
    HttpParseStatusFailed = 0x112A,
    /// HTTP_PARSE_PHRASE_FAILED: failed to parse phrase
    HttpParsePhraseFailed = 0x112B,
    /// HTTP_PARSE_CRLF_FAILED: failed to parse crlf
    HttpParseCrlfFailed = 0x112C,
    /// HTTP_PARSE_HEADER_FAILED: failed to parse header
    HttpParseHeaderFailed = 0x112D,
    /// HTTP_PARSE_HEADER_KEY_FAILED: failed to parse header key
    HttpParseHeaderKeyFailed = 0x112E,
    /// HTTP_PARSE_HEADER_VAL_FAILED: failed to parse header val
    HttpParseHeaderValFailed = 0x112F,
    /// HTTP_PARSE_CHUNK_SIZE_FAILED: failed to parse chunk size
    HttpParseChunkSizeFailed = 0x1130,
    /// HTTP_PARSE_CHUNK_FAILED: failed to parse chunk
    HttpParseChunkFailed = 0x1131,
    /// HTTP_PARSE_END_FAILED: failed to parse end section
    HttpParseEndFailed = 0x1132,
    /// HTTP_PARSE_INVALID_STATE: invalid parse state
    HttpParseInvalidState = 0x1134,
    /// HTTP_PARSE_ERROR_STATE: failed to parse error section
    HttpParseErrorState = 0x1135,
    /// HTTP_GC_QUERY_NULL: query size is 0
    HttpGcQueryNull = 0x1150,
    /// HTTP_GC_QUERY_SIZE: query size can not more than 100
    HttpGcQuerySize = 0x1151,
    /// HTTP_GC_REQ_PARSE_ERROR: parse grafana json error
    HttpGcReqParseError = 0x1152,
    /// HTTP_TG_DB_NOT_INPUT: database name can not be null
    HttpTgDbNotInput = 0x1160,
    /// HTTP_TG_DB_TOO_LONG: database name too long
    HttpTgDbTooLong = 0x1161,
    /// HTTP_TG_INVALID_JSON: invalid telegraf json fromat
    HttpTgInvalidJson = 0x1162,
    /// HTTP_TG_METRICS_NULL: metrics size is 0
    HttpTgMetricsNull = 0x1163,
    /// HTTP_TG_METRICS_SIZE: metrics size can not more than 1K
    HttpTgMetricsSize = 0x1164,
    /// HTTP_TG_METRIC_NULL: metric name not find
    HttpTgMetricNull = 0x1165,
    /// HTTP_TG_METRIC_TYPE: metric name type should be string
    HttpTgMetricType = 0x1166,
    /// HTTP_TG_METRIC_NAME_NULL: metric name length is 0
    HttpTgMetricNameNull = 0x1167,
    /// HTTP_TG_METRIC_NAME_LONG: metric name length too long
    HttpTgMetricNameLong = 0x1168,
    /// HTTP_TG_TIMESTAMP_NULL: timestamp not find
    HttpTgTimestampNull = 0x1169,
    /// HTTP_TG_TIMESTAMP_TYPE: timestamp type should be integer
    HttpTgTimestampType = 0x116A,
    /// HTTP_TG_TIMESTAMP_VAL_NULL: timestamp value smaller than 0
    HttpTgTimestampValNull = 0x116B,
    /// HTTP_TG_TAGS_NULL: tags not find
    HttpTgTagsNull = 0x116C,
    /// HTTP_TG_TAGS_SIZE_0: tags size is 0
    HttpTgTagsSize0 = 0x116D,
    /// HTTP_TG_TAGS_SIZE_LONG: tags size too long
    HttpTgTagsSizeLong = 0x116E,
    /// HTTP_TG_TAG_NULL: tag is null
    HttpTgTagNull = 0x116F,
    /// HTTP_TG_TAG_NAME_NULL: tag name is null
    HttpTgTagNameNull = 0x1170,
    /// HTTP_TG_TAG_NAME_SIZE: tag name length too long
    HttpTgTagNameSize = 0x1171,
    /// HTTP_TG_TAG_VALUE_TYPE: tag value type should be number or string
    HttpTgTagValueType = 0x1172,
    /// HTTP_TG_TAG_VALUE_NULL: tag value is null
    HttpTgTagValueNull = 0x1173,
    /// HTTP_TG_TABLE_NULL: table is null
    HttpTgTableNull = 0x1174,
    /// HTTP_TG_TABLE_SIZE: table name length too long
    HttpTgTableSize = 0x1175,
    /// HTTP_TG_FIELDS_NULL: fields not find
    HttpTgFieldsNull = 0x1176,
    /// HTTP_TG_FIELDS_SIZE_0: fields size is 0
    HttpTgFieldsSize0 = 0x1177,
    /// HTTP_TG_FIELDS_SIZE_LONG: fields size too long
    HttpTgFieldsSizeLong = 0x1178,
    /// HTTP_TG_FIELD_NULL: field is null
    HttpTgFieldNull = 0x1179,
    /// HTTP_TG_FIELD_NAME_NULL: field name is null
    HttpTgFieldNameNull = 0x117A,
    /// HTTP_TG_FIELD_NAME_SIZE: field name length too long
    HttpTgFieldNameSize = 0x117B,
    /// HTTP_TG_FIELD_VALUE_TYPE: field value type should be number or string
    HttpTgFieldValueType = 0x117C,
    /// HTTP_TG_FIELD_VALUE_NULL: field value is null
    HttpTgFieldValueNull = 0x117D,
    /// HTTP_TG_HOST_NOT_STRING: host type should be string
    HttpTgHostNotString = 0x117E,
    /// HTTP_TG_STABLE_NOT_EXIST: stable not exist
    HttpTgStableNotExist = 0x117F,
    /// HTTP_OP_DB_NOT_INPUT: database name can not be null
    HttpOpDbNotInput = 0x1190,
    /// HTTP_OP_DB_TOO_LONG: database name too long
    HttpOpDbTooLong = 0x1191,
    /// HTTP_OP_INVALID_JSON: invalid opentsdb json fromat
    HttpOpInvalidJson = 0x1192,
    /// HTTP_OP_METRICS_NULL: metrics size is 0
    HttpOpMetricsNull = 0x1193,
    /// HTTP_OP_METRICS_SIZE: metrics size can not more than 10K
    HttpOpMetricsSize = 0x1194,
    /// HTTP_OP_METRIC_NULL: metric name not find
    HttpOpMetricNull = 0x1195,
    /// HTTP_OP_METRIC_TYPE: metric name type should be string
    HttpOpMetricType = 0x1196,
    /// HTTP_OP_METRIC_NAME_NULL: metric name length is 0
    HttpOpMetricNameNull = 0x1197,
    /// HTTP_OP_METRIC_NAME_LONG: metric name length can not more than 22
    HttpOpMetricNameLong = 0x1198,
    /// HTTP_OP_TIMESTAMP_NULL: timestamp not find
    HttpOpTimestampNull = 0x1199,
    /// HTTP_OP_TIMESTAMP_TYPE: timestamp type should be integer
    HttpOpTimestampType = 0x119A,
    /// HTTP_OP_TIMESTAMP_VAL_NULL: timestamp value smaller than 0
    HttpOpTimestampValNull = 0x119B,
    /// HTTP_OP_TAGS_NULL: tags not find
    HttpOpTagsNull = 0x119C,
    /// HTTP_OP_TAGS_SIZE_0: tags size is 0
    HttpOpTagsSize0 = 0x119D,
    /// HTTP_OP_TAGS_SIZE_LONG: tags size too long
    HttpOpTagsSizeLong = 0x119E,
    /// HTTP_OP_TAG_NULL: tag is null
    HttpOpTagNull = 0x119F,
    /// HTTP_OP_TAG_NAME_NULL: tag name is null
    HttpOpTagNameNull = 0x11A0,
    /// HTTP_OP_TAG_NAME_SIZE: tag name length too long
    HttpOpTagNameSize = 0x11A1,
    /// HTTP_OP_TAG_VALUE_TYPE: tag value type should be boolean number or string
    HttpOpTagValueType = 0x11A2,
    /// HTTP_OP_TAG_VALUE_NULL: tag value is null
    HttpOpTagValueNull = 0x11A3,
    /// HTTP_OP_TAG_VALUE_TOO_LONG: tag value can not more than 64
    HttpOpTagValueTooLong = 0x11A4,
    /// HTTP_OP_VALUE_NULL: value not find
    HttpOpValueNull = 0x11A5,
    /// HTTP_OP_VALUE_TYPE: value type should be boolean number or string
    HttpOpValueType = 0x11A6,
    /// ODBC_OOM: out of memory
    OdbcOom = 0x2100,
    /// ODBC_CONV_CHAR_NOT_NUM: convertion not a valid literal input
    OdbcConvCharNotNum = 0x2101,
    /// ODBC_CONV_UNDEF: convertion undefined
    OdbcConvUndef = 0x2102,
    /// ODBC_CONV_TRUNC_FRAC: convertion fractional truncated
    OdbcConvTruncFrac = 0x2103,
    /// ODBC_CONV_TRUNC: convertion truncated
    OdbcConvTrunc = 0x2104,
    /// ODBC_CONV_NOT_SUPPORT: convertion not supported
    OdbcConvNotSupport = 0x2105,
    /// ODBC_CONV_OOR: convertion numeric value out of range
    OdbcConvOor = 0x2106,
    /// ODBC_OUT_OF_RANGE: out of range
    OdbcOutOfRange = 0x2107,
    /// ODBC_NOT_SUPPORT: not supported yet
    OdbcNotSupport = 0x2108,
    /// ODBC_INVALID_HANDLE: invalid handle
    OdbcInvalidHandle = 0x2109,
    /// ODBC_NO_RESULT: no result set
    OdbcNoResult = 0x210a,
    /// ODBC_NO_FIELDS: no fields returned
    OdbcNoFields = 0x210b,
    /// ODBC_INVALID_CURSOR: invalid cursor
    OdbcInvalidCursor = 0x210c,
    /// ODBC_STATEMENT_NOT_READY: statement not ready
    OdbcStatementNotReady = 0x210d,
    /// ODBC_CONNECTION_BUSY: connection still busy
    OdbcConnectionBusy = 0x210e,
    /// ODBC_BAD_CONNSTR: bad connection string
    OdbcBadConnstr = 0x210f,
    /// ODBC_BAD_ARG: bad argument
    OdbcBadArg = 0x2110,
    /// ODBC_CONV_NOT_VALID_TS: not a valid timestamp
    OdbcConvNotValidTs = 0x2111,
    /// ODBC_CONV_SRC_TOO_LARGE: src too large
    OdbcConvSrcTooLarge = 0x2112,
    /// ODBC_CONV_SRC_BAD_SEQ: src bad sequence
    OdbcConvSrcBadSeq = 0x2113,
    /// ODBC_CONV_SRC_INCOMPLETE: src incomplete
    OdbcConvSrcIncomplete = 0x2114,
    /// ODBC_CONV_SRC_GENERAL: src general
    OdbcConvSrcGeneral = 0x2115,
    /// FS_OUT_OF_MEMORY: tfs out of memory
    FsOutOfMemory = 0x2200,
    /// FS_INVLD_CFG: tfs invalid mount config
    FsInvldCfg = 0x2201,
    /// FS_TOO_MANY_MOUNT: tfs too many mount
    FsTooManyMount = 0x2202,
    /// FS_DUP_PRIMARY: tfs duplicate primary mount
    FsDupPrimary = 0x2203,
    /// FS_NO_PRIMARY_DISK: tfs no primary mount
    FsNoPrimaryDisk = 0x2204,
    /// FS_NO_MOUNT_AT_TIER: tfs no mount at tier
    FsNoMountAtTier = 0x2205,
    /// FS_FILE_ALREADY_EXISTS: tfs file already exists
    FsFileAlreadyExists = 0x2206,
    /// FS_INVLD_LEVEL: tfs invalid level
    FsInvldLevel = 0x2207,
    /// FS_NO_VALID_DISK: tfs no valid disk
    FsNoValidDisk = 0x2208,

    #[num_enum(default)]
    Unknown = 0xffff,
}

use TaosCode::*;

impl fmt::Display for TaosCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as i32)
    }
}

impl TaosCode {
    pub fn success(&self) -> bool {
        matches!(self, Success)
    }
    /// RPC_ACTION_IN_PROGRESS: Action in progress
    pub fn rpc_action_in_progress(&self) -> bool {
        matches!(self, RpcActionInProgress)
    }
    /// RPC_AUTH_REQUIRED: Authentication required
    pub fn rpc_auth_required(&self) -> bool {
        matches!(self, RpcAuthRequired)
    }
    /// RPC_AUTH_FAILURE: Authentication failure
    pub fn rpc_auth_failure(&self) -> bool {
        matches!(self, RpcAuthFailure)
    }
    /// RPC_REDIRECT: Redirect
    pub fn rpc_redirect(&self) -> bool {
        matches!(self, RpcRedirect)
    }
    /// RPC_NOT_READY: System not ready
    pub fn rpc_not_ready(&self) -> bool {
        matches!(self, RpcNotReady)
    }
    /// RPC_ALREADY_PROCESSED: Message already processed
    pub fn rpc_already_processed(&self) -> bool {
        matches!(self, RpcAlreadyProcessed)
    }
    /// RPC_LAST_SESSION_NOT_FINISHED: Last session not finished
    pub fn rpc_last_session_not_finished(&self) -> bool {
        matches!(self, RpcLastSessionNotFinished)
    }
    /// RPC_MISMATCHED_LINK_ID: Mismatched meter id
    pub fn rpc_mismatched_link_id(&self) -> bool {
        matches!(self, RpcMismatchedLinkId)
    }
    /// RPC_TOO_SLOW: Processing of request timed out
    pub fn rpc_too_slow(&self) -> bool {
        matches!(self, RpcTooSlow)
    }
    /// RPC_MAX_SESSIONS: Number of sessions reached limit
    pub fn rpc_max_sessions(&self) -> bool {
        matches!(self, RpcMaxSessions)
    }
    /// RPC_NETWORK_UNAVAIL: Unable to establish connection
    pub fn rpc_network_unavail(&self) -> bool {
        matches!(self, RpcNetworkUnavail)
    }
    /// RPC_APP_ERROR: Unexpected generic error in RPC
    pub fn rpc_app_error(&self) -> bool {
        matches!(self, RpcAppError)
    }
    /// RPC_UNEXPECTED_RESPONSE: Unexpected response
    pub fn rpc_unexpected_response(&self) -> bool {
        matches!(self, RpcUnexpectedResponse)
    }
    /// RPC_INVALID_VALUE: Invalid value
    pub fn rpc_invalid_value(&self) -> bool {
        matches!(self, RpcInvalidValue)
    }
    /// RPC_INVALID_TRAN_ID: Invalid transaction id
    pub fn rpc_invalid_tran_id(&self) -> bool {
        matches!(self, RpcInvalidTranId)
    }
    /// RPC_INVALID_SESSION_ID: Invalid session id
    pub fn rpc_invalid_session_id(&self) -> bool {
        matches!(self, RpcInvalidSessionId)
    }
    /// RPC_INVALID_MSG_TYPE: Invalid message type
    pub fn rpc_invalid_msg_type(&self) -> bool {
        matches!(self, RpcInvalidMsgType)
    }
    /// RPC_INVALID_RESPONSE_TYPE: Invalid response type
    pub fn rpc_invalid_response_type(&self) -> bool {
        matches!(self, RpcInvalidResponseType)
    }
    /// RPC_INVALID_TIME_STAMP: Client and server's time is not synchronized
    pub fn rpc_invalid_time_stamp(&self) -> bool {
        matches!(self, RpcInvalidTimeStamp)
    }
    /// APP_NOT_READY: Database not ready
    pub fn app_not_ready(&self) -> bool {
        matches!(self, AppNotReady)
    }
    /// RPC_FQDN_ERROR: Unable to resolve FQDN
    pub fn rpc_fqdn_error(&self) -> bool {
        matches!(self, RpcFqdnError)
    }
    /// RPC_INVALID_VERSION: Invalid app version
    pub fn rpc_invalid_version(&self) -> bool {
        matches!(self, RpcInvalidVersion)
    }
    /// COM_OPS_NOT_SUPPORT: Operation not supported
    pub fn com_ops_not_support(&self) -> bool {
        matches!(self, ComOpsNotSupport)
    }
    /// COM_MEMORY_CORRUPTED: Memory corrupted
    pub fn com_memory_corrupted(&self) -> bool {
        matches!(self, ComMemoryCorrupted)
    }
    /// COM_OUT_OF_MEMORY: Out of memory
    pub fn com_out_of_memory(&self) -> bool {
        matches!(self, ComOutOfMemory)
    }
    /// COM_INVALID_CFG_MSG: Invalid config message
    pub fn com_invalid_cfg_msg(&self) -> bool {
        matches!(self, ComInvalidCfgMsg)
    }
    /// COM_FILE_CORRUPTED: Data file corrupted
    pub fn com_file_corrupted(&self) -> bool {
        matches!(self, ComFileCorrupted)
    }
    /// REF_NO_MEMORY: Ref out of memory
    pub fn ref_no_memory(&self) -> bool {
        matches!(self, RefNoMemory)
    }
    /// REF_FULL: too many Ref Objs
    pub fn ref_full(&self) -> bool {
        matches!(self, RefFull)
    }
    /// REF_ID_REMOVED: Ref ID is removed
    pub fn ref_id_removed(&self) -> bool {
        matches!(self, RefIdRemoved)
    }
    /// REF_INVALID_ID: Invalid Ref ID
    pub fn ref_invalid_id(&self) -> bool {
        matches!(self, RefInvalidId)
    }
    /// REF_ALREADY_EXIST: Ref is already there
    pub fn ref_already_exist(&self) -> bool {
        matches!(self, RefAlreadyExist)
    }
    /// REF_NOT_EXIST: Ref is not there
    pub fn ref_not_exist(&self) -> bool {
        matches!(self, RefNotExist)
    }
    /// TSC_INVALID_SQL: Invalid SQL statement
    pub fn tsc_invalid_sql(&self) -> bool {
        matches!(self, TscInvalidSql)
    }
    /// TSC_INVALID_QHANDLE: Invalid qhandle
    pub fn tsc_invalid_qhandle(&self) -> bool {
        matches!(self, TscInvalidQhandle)
    }
    /// TSC_INVALID_TIME_STAMP: Invalid combination of client/service time
    pub fn tsc_invalid_time_stamp(&self) -> bool {
        matches!(self, TscInvalidTimeStamp)
    }
    /// TSC_INVALID_VALUE: Invalid value in client
    pub fn tsc_invalid_value(&self) -> bool {
        matches!(self, TscInvalidValue)
    }
    /// TSC_INVALID_VERSION: Invalid client version
    pub fn tsc_invalid_version(&self) -> bool {
        matches!(self, TscInvalidVersion)
    }
    /// TSC_INVALID_IE: Invalid client ie
    pub fn tsc_invalid_ie(&self) -> bool {
        matches!(self, TscInvalidIe)
    }
    /// TSC_INVALID_FQDN: Invalid host name
    pub fn tsc_invalid_fqdn(&self) -> bool {
        matches!(self, TscInvalidFqdn)
    }
    /// TSC_INVALID_USER_LENGTH: Invalid user name
    pub fn tsc_invalid_user_length(&self) -> bool {
        matches!(self, TscInvalidUserLength)
    }
    /// TSC_INVALID_PASS_LENGTH: Invalid password
    pub fn tsc_invalid_pass_length(&self) -> bool {
        matches!(self, TscInvalidPassLength)
    }
    /// TSC_INVALID_DB_LENGTH: Database name too long
    pub fn tsc_invalid_db_length(&self) -> bool {
        matches!(self, TscInvalidDbLength)
    }
    /// TSC_INVALID_TABLE_ID_LENGTH: Table name too long
    pub fn tsc_invalid_table_id_length(&self) -> bool {
        matches!(self, TscInvalidTableIdLength)
    }
    /// TSC_INVALID_CONNECTION: Invalid connection
    pub fn tsc_invalid_connection(&self) -> bool {
        matches!(self, TscInvalidConnection)
    }
    /// TSC_OUT_OF_MEMORY: System out of memory
    pub fn tsc_out_of_memory(&self) -> bool {
        matches!(self, TscOutOfMemory)
    }
    /// TSC_NO_DISKSPACE: System out of disk space
    pub fn tsc_no_diskspace(&self) -> bool {
        matches!(self, TscNoDiskspace)
    }
    /// TSC_QUERY_CACHE_ERASED: Query cache erased
    pub fn tsc_query_cache_erased(&self) -> bool {
        matches!(self, TscQueryCacheErased)
    }
    /// TSC_QUERY_CANCELLED: Query terminated
    pub fn tsc_query_cancelled(&self) -> bool {
        matches!(self, TscQueryCancelled)
    }
    /// TSC_SORTED_RES_TOO_MANY: Result set too large to be sorted
    pub fn tsc_sorted_res_too_many(&self) -> bool {
        matches!(self, TscSortedResTooMany)
    }
    /// TSC_APP_ERROR: Application error
    pub fn tsc_app_error(&self) -> bool {
        matches!(self, TscAppError)
    }
    /// TSC_ACTION_IN_PROGRESS: Action in progress
    pub fn tsc_action_in_progress(&self) -> bool {
        matches!(self, TscActionInProgress)
    }
    /// TSC_DISCONNECTED: Disconnected from service
    pub fn tsc_disconnected(&self) -> bool {
        matches!(self, TscDisconnected)
    }
    /// TSC_NO_WRITE_AUTH: No write permission
    pub fn tsc_no_write_auth(&self) -> bool {
        matches!(self, TscNoWriteAuth)
    }
    /// TSC_CONN_KILLED: Connection killed
    pub fn tsc_conn_killed(&self) -> bool {
        matches!(self, TscConnKilled)
    }
    /// TSC_SQL_SYNTAX_ERROR: Syntax error in SQL
    pub fn tsc_sql_syntax_error(&self) -> bool {
        matches!(self, TscSqlSyntaxError)
    }
    /// TSC_DB_NOT_SELECTED: Database not specified or available
    pub fn tsc_db_not_selected(&self) -> bool {
        matches!(self, TscDbNotSelected)
    }
    /// TSC_INVALID_TABLE_NAME: Table does not exist
    pub fn tsc_invalid_table_name(&self) -> bool {
        matches!(self, TscInvalidTableName)
    }
    /// TSC_EXCEED_SQL_LIMIT: SQL statement too long check maxSQLLength config
    pub fn tsc_exceed_sql_limit(&self) -> bool {
        matches!(self, TscExceedSqlLimit)
    }
    /// MND_MSG_NOT_PROCESSED: Message not processed
    pub fn mnd_msg_not_processed(&self) -> bool {
        matches!(self, MndMsgNotProcessed)
    }
    /// MND_ACTION_IN_PROGRESS: Message is progressing
    pub fn mnd_action_in_progress(&self) -> bool {
        matches!(self, MndActionInProgress)
    }
    /// MND_ACTION_NEED_REPROCESSED: Messag need to be reprocessed
    pub fn mnd_action_need_reprocessed(&self) -> bool {
        matches!(self, MndActionNeedReprocessed)
    }
    /// MND_NO_RIGHTS: Insufficient privilege for operation
    pub fn mnd_no_rights(&self) -> bool {
        matches!(self, MndNoRights)
    }
    /// MND_APP_ERROR: Unexpected generic error in mnode
    pub fn mnd_app_error(&self) -> bool {
        matches!(self, MndAppError)
    }
    /// MND_INVALID_CONNECTION: Invalid message connection
    pub fn mnd_invalid_connection(&self) -> bool {
        matches!(self, MndInvalidConnection)
    }
    /// MND_INVALID_MSG_VERSION: Incompatible protocol version
    pub fn mnd_invalid_msg_version(&self) -> bool {
        matches!(self, MndInvalidMsgVersion)
    }
    /// MND_INVALID_MSG_LEN: Invalid message length
    pub fn mnd_invalid_msg_len(&self) -> bool {
        matches!(self, MndInvalidMsgLen)
    }
    /// MND_INVALID_MSG_TYPE: Invalid message type
    pub fn mnd_invalid_msg_type(&self) -> bool {
        matches!(self, MndInvalidMsgType)
    }
    /// MND_TOO_MANY_SHELL_CONNS: Too many connections
    pub fn mnd_too_many_shell_conns(&self) -> bool {
        matches!(self, MndTooManyShellConns)
    }
    /// MND_OUT_OF_MEMORY: Out of memory in mnode
    pub fn mnd_out_of_memory(&self) -> bool {
        matches!(self, MndOutOfMemory)
    }
    /// MND_INVALID_SHOWOBJ: Data expired
    pub fn mnd_invalid_showobj(&self) -> bool {
        matches!(self, MndInvalidShowobj)
    }
    /// MND_INVALID_QUERY_ID: Invalid query id
    pub fn mnd_invalid_query_id(&self) -> bool {
        matches!(self, MndInvalidQueryId)
    }
    /// MND_INVALID_STREAM_ID: Invalid stream id
    pub fn mnd_invalid_stream_id(&self) -> bool {
        matches!(self, MndInvalidStreamId)
    }
    /// MND_INVALID_CONN_ID: Invalid connection id
    pub fn mnd_invalid_conn_id(&self) -> bool {
        matches!(self, MndInvalidConnId)
    }
    /// MND_MNODE_IS_RUNNING: mnode is alreay running
    pub fn mnd_mnode_is_running(&self) -> bool {
        matches!(self, MndMnodeIsRunning)
    }
    /// MND_FAILED_TO_CONFIG_SYNC: failed to config sync
    pub fn mnd_failed_to_config_sync(&self) -> bool {
        matches!(self, MndFailedToConfigSync)
    }
    /// MND_FAILED_TO_START_SYNC: failed to start sync
    pub fn mnd_failed_to_start_sync(&self) -> bool {
        matches!(self, MndFailedToStartSync)
    }
    /// MND_FAILED_TO_CREATE_DIR: failed to create mnode dir
    pub fn mnd_failed_to_create_dir(&self) -> bool {
        matches!(self, MndFailedToCreateDir)
    }
    /// MND_FAILED_TO_INIT_STEP: failed to init components
    pub fn mnd_failed_to_init_step(&self) -> bool {
        matches!(self, MndFailedToInitStep)
    }
    /// MND_SDB_OBJ_ALREADY_THERE: Object already there
    pub fn mnd_sdb_obj_already_there(&self) -> bool {
        matches!(self, MndSdbObjAlreadyThere)
    }
    /// MND_SDB_ERROR: Unexpected generic error in sdb
    pub fn mnd_sdb_error(&self) -> bool {
        matches!(self, MndSdbError)
    }
    /// MND_SDB_INVALID_TABLE_TYPE: Invalid table type
    pub fn mnd_sdb_invalid_table_type(&self) -> bool {
        matches!(self, MndSdbInvalidTableType)
    }
    /// MND_SDB_OBJ_NOT_THERE: Object not there
    pub fn mnd_sdb_obj_not_there(&self) -> bool {
        matches!(self, MndSdbObjNotThere)
    }
    /// MND_SDB_INVAID_META_ROW: Invalid meta row
    pub fn mnd_sdb_invaid_meta_row(&self) -> bool {
        matches!(self, MndSdbInvaidMetaRow)
    }
    /// MND_SDB_INVAID_KEY_TYPE: Invalid key type
    pub fn mnd_sdb_invaid_key_type(&self) -> bool {
        matches!(self, MndSdbInvaidKeyType)
    }
    /// MND_DNODE_ALREADY_EXIST: DNode already exists
    pub fn mnd_dnode_already_exist(&self) -> bool {
        matches!(self, MndDnodeAlreadyExist)
    }
    /// MND_DNODE_NOT_EXIST: DNode does not exist
    pub fn mnd_dnode_not_exist(&self) -> bool {
        matches!(self, MndDnodeNotExist)
    }
    /// MND_VGROUP_NOT_EXIST: VGroup does not exist
    pub fn mnd_vgroup_not_exist(&self) -> bool {
        matches!(self, MndVgroupNotExist)
    }
    /// MND_NO_REMOVE_MASTER: Master DNode cannot be removed
    pub fn mnd_no_remove_master(&self) -> bool {
        matches!(self, MndNoRemoveMaster)
    }
    /// MND_NO_ENOUGH_DNODES: Out of DNodes
    pub fn mnd_no_enough_dnodes(&self) -> bool {
        matches!(self, MndNoEnoughDnodes)
    }
    /// MND_CLUSTER_CFG_INCONSISTENT: Cluster cfg inconsistent
    pub fn mnd_cluster_cfg_inconsistent(&self) -> bool {
        matches!(self, MndClusterCfgInconsistent)
    }
    /// MND_INVALID_DNODE_CFG_OPTION: Invalid dnode cfg option
    pub fn mnd_invalid_dnode_cfg_option(&self) -> bool {
        matches!(self, MndInvalidDnodeCfgOption)
    }
    /// MND_BALANCE_ENABLED: Balance already enabled
    pub fn mnd_balance_enabled(&self) -> bool {
        matches!(self, MndBalanceEnabled)
    }
    /// MND_VGROUP_NOT_IN_DNODE: Vgroup not in dnode
    pub fn mnd_vgroup_not_in_dnode(&self) -> bool {
        matches!(self, MndVgroupNotInDnode)
    }
    /// MND_VGROUP_ALREADY_IN_DNODE: Vgroup already in dnode
    pub fn mnd_vgroup_already_in_dnode(&self) -> bool {
        matches!(self, MndVgroupAlreadyInDnode)
    }
    /// MND_DNODE_NOT_FREE: Dnode not avaliable
    pub fn mnd_dnode_not_free(&self) -> bool {
        matches!(self, MndDnodeNotFree)
    }
    /// MND_INVALID_CLUSTER_ID: Cluster id not match
    pub fn mnd_invalid_cluster_id(&self) -> bool {
        matches!(self, MndInvalidClusterId)
    }
    /// MND_NOT_READY: Cluster not ready
    pub fn mnd_not_ready(&self) -> bool {
        matches!(self, MndNotReady)
    }
    /// MND_DNODE_ID_NOT_CONFIGURED: Dnode Id not configured
    pub fn mnd_dnode_id_not_configured(&self) -> bool {
        matches!(self, MndDnodeIdNotConfigured)
    }
    /// MND_DNODE_EP_NOT_CONFIGURED: Dnode Ep not configured
    pub fn mnd_dnode_ep_not_configured(&self) -> bool {
        matches!(self, MndDnodeEpNotConfigured)
    }
    /// MND_ACCT_ALREADY_EXIST: Account already exists
    pub fn mnd_acct_already_exist(&self) -> bool {
        matches!(self, MndAcctAlreadyExist)
    }
    /// MND_INVALID_ACCT: Invalid account
    pub fn mnd_invalid_acct(&self) -> bool {
        matches!(self, MndInvalidAcct)
    }
    /// MND_INVALID_ACCT_OPTION: Invalid account options
    pub fn mnd_invalid_acct_option(&self) -> bool {
        matches!(self, MndInvalidAcctOption)
    }
    /// MND_ACCT_EXPIRED: Account authorization has expired
    pub fn mnd_acct_expired(&self) -> bool {
        matches!(self, MndAcctExpired)
    }
    /// MND_USER_ALREADY_EXIST: User already exists
    pub fn mnd_user_already_exist(&self) -> bool {
        matches!(self, MndUserAlreadyExist)
    }
    /// MND_INVALID_USER: Invalid user
    pub fn mnd_invalid_user(&self) -> bool {
        matches!(self, MndInvalidUser)
    }
    /// MND_INVALID_USER_FORMAT: Invalid user format
    pub fn mnd_invalid_user_format(&self) -> bool {
        matches!(self, MndInvalidUserFormat)
    }
    /// MND_INVALID_PASS_FORMAT: Invalid password format
    pub fn mnd_invalid_pass_format(&self) -> bool {
        matches!(self, MndInvalidPassFormat)
    }
    /// MND_NO_USER_FROM_CONN: Can not get user from conn
    pub fn mnd_no_user_from_conn(&self) -> bool {
        matches!(self, MndNoUserFromConn)
    }
    /// MND_TOO_MANY_USERS: Too many users
    pub fn mnd_too_many_users(&self) -> bool {
        matches!(self, MndTooManyUsers)
    }
    /// MND_TABLE_ALREADY_EXIST: Table already exists
    pub fn mnd_table_already_exist(&self) -> bool {
        matches!(self, MndTableAlreadyExist)
    }
    /// MND_INVALID_TABLE_ID: Table name too long
    pub fn mnd_invalid_table_id(&self) -> bool {
        matches!(self, MndInvalidTableId)
    }
    /// MND_INVALID_TABLE_NAME: Table does not exist
    pub fn mnd_invalid_table_name(&self) -> bool {
        matches!(self, MndInvalidTableName)
    }
    /// MND_INVALID_TABLE_TYPE: Invalid table type in tsdb
    pub fn mnd_invalid_table_type(&self) -> bool {
        matches!(self, MndInvalidTableType)
    }
    /// MND_TOO_MANY_TAGS: Too many tags
    pub fn mnd_too_many_tags(&self) -> bool {
        matches!(self, MndTooManyTags)
    }
    /// MND_TOO_MANY_COLUMNS: Too many columns
    pub fn mnd_too_many_columns(&self) -> bool {
        matches!(self, MndTooManyColumns)
    }
    /// MND_TOO_MANY_TIMESERIES: Too many time series
    pub fn mnd_too_many_timeseries(&self) -> bool {
        matches!(self, MndTooManyTimeseries)
    }
    /// MND_NOT_SUPER_TABLE: Not super table
    pub fn mnd_not_super_table(&self) -> bool {
        matches!(self, MndNotSuperTable)
    }
    /// MND_COL_NAME_TOO_LONG: Tag name too long
    pub fn mnd_col_name_too_long(&self) -> bool {
        matches!(self, MndColNameTooLong)
    }
    /// MND_TAG_ALREAY_EXIST: Tag already exists
    pub fn mnd_tag_alreay_exist(&self) -> bool {
        matches!(self, MndTagAlreayExist)
    }
    /// MND_TAG_NOT_EXIST: Tag does not exist
    pub fn mnd_tag_not_exist(&self) -> bool {
        matches!(self, MndTagNotExist)
    }
    /// MND_FIELD_ALREAY_EXIST: Field already exists
    pub fn mnd_field_alreay_exist(&self) -> bool {
        matches!(self, MndFieldAlreayExist)
    }
    /// MND_FIELD_NOT_EXIST: Field does not exist
    pub fn mnd_field_not_exist(&self) -> bool {
        matches!(self, MndFieldNotExist)
    }
    /// MND_INVALID_STABLE_NAME: Super table does not exist
    pub fn mnd_invalid_stable_name(&self) -> bool {
        matches!(self, MndInvalidStableName)
    }
    /// MND_INVALID_CREATE_TABLE_MSG: Invalid create table message
    pub fn mnd_invalid_create_table_msg(&self) -> bool {
        matches!(self, MndInvalidCreateTableMsg)
    }
    /// MND_DB_NOT_SELECTED: Database not specified or available
    pub fn mnd_db_not_selected(&self) -> bool {
        matches!(self, MndDbNotSelected)
    }
    /// MND_DB_ALREADY_EXIST: Database already exists
    pub fn mnd_db_already_exist(&self) -> bool {
        matches!(self, MndDbAlreadyExist)
    }
    /// MND_INVALID_DB_OPTION: Invalid database options
    pub fn mnd_invalid_db_option(&self) -> bool {
        matches!(self, MndInvalidDbOption)
    }
    /// MND_INVALID_DB: Invalid database name
    pub fn mnd_invalid_db(&self) -> bool {
        matches!(self, MndInvalidDb)
    }
    /// MND_MONITOR_DB_FORBIDDEN: Cannot delete monitor database
    pub fn mnd_monitor_db_forbidden(&self) -> bool {
        matches!(self, MndMonitorDbForbidden)
    }
    /// MND_TOO_MANY_DATABASES: Too many databases for account
    pub fn mnd_too_many_databases(&self) -> bool {
        matches!(self, MndTooManyDatabases)
    }
    /// MND_DB_IN_DROPPING: Database not available
    pub fn mnd_db_in_dropping(&self) -> bool {
        matches!(self, MndDbInDropping)
    }
    /// MND_VGROUP_NOT_READY: Database unsynced
    pub fn mnd_vgroup_not_ready(&self) -> bool {
        matches!(self, MndVgroupNotReady)
    }
    /// MND_INVALID_DB_OPTION_DAYS: Invalid database option: days out of range
    pub fn mnd_invalid_db_option_days(&self) -> bool {
        matches!(self, MndInvalidDbOptionDays)
    }
    /// MND_INVALID_DB_OPTION_KEEP: Invalid database option: keep >= keep1 >= keep0 >= days
    pub fn mnd_invalid_db_option_keep(&self) -> bool {
        matches!(self, MndInvalidDbOptionKeep)
    }
    /// MND_INVALID_TOPIC: Invalid topic nam
    pub fn mnd_invalid_topic(&self) -> bool {
        matches!(self, MndInvalidTopic)
    }
    /// MND_INVALID_TOPIC_OPTION: Invalid topic optio
    pub fn mnd_invalid_topic_option(&self) -> bool {
        matches!(self, MndInvalidTopicOption)
    }
    /// MND_INVALID_TOPIC_PARTITONS: Invalid topic partitons num, valid range: [1, 1000
    pub fn mnd_invalid_topic_partitons(&self) -> bool {
        matches!(self, MndInvalidTopicPartitons)
    }
    /// MND_TOPIC_ALREADY_EXIST: Topic already exist
    pub fn mnd_topic_already_exist(&self) -> bool {
        matches!(self, MndTopicAlreadyExist)
    }
    /// DND_MSG_NOT_PROCESSED: Message not processed
    pub fn dnd_msg_not_processed(&self) -> bool {
        matches!(self, DndMsgNotProcessed)
    }
    /// DND_OUT_OF_MEMORY: Dnode out of memory
    pub fn dnd_out_of_memory(&self) -> bool {
        matches!(self, DndOutOfMemory)
    }
    /// DND_NO_WRITE_ACCESS: No permission for disk files in dnode
    pub fn dnd_no_write_access(&self) -> bool {
        matches!(self, DndNoWriteAccess)
    }
    /// DND_INVALID_MSG_LEN: Invalid message length
    pub fn dnd_invalid_msg_len(&self) -> bool {
        matches!(self, DndInvalidMsgLen)
    }
    /// DND_ACTION_IN_PROGRESS: Action in progress
    pub fn dnd_action_in_progress(&self) -> bool {
        matches!(self, DndActionInProgress)
    }
    /// DND_TOO_MANY_VNODES: Too many vnode directories
    pub fn dnd_too_many_vnodes(&self) -> bool {
        matches!(self, DndTooManyVnodes)
    }
    /// VND_ACTION_IN_PROGRESS: Action in progress
    pub fn vnd_action_in_progress(&self) -> bool {
        matches!(self, VndActionInProgress)
    }
    /// VND_MSG_NOT_PROCESSED: Message not processed
    pub fn vnd_msg_not_processed(&self) -> bool {
        matches!(self, VndMsgNotProcessed)
    }
    /// VND_ACTION_NEED_REPROCESSED: Action need to be reprocessed
    pub fn vnd_action_need_reprocessed(&self) -> bool {
        matches!(self, VndActionNeedReprocessed)
    }
    /// VND_INVALID_VGROUP_ID: Invalid Vgroup ID
    pub fn vnd_invalid_vgroup_id(&self) -> bool {
        matches!(self, VndInvalidVgroupId)
    }
    /// VND_INIT_FAILED: Vnode initialization failed
    pub fn vnd_init_failed(&self) -> bool {
        matches!(self, VndInitFailed)
    }
    /// VND_NO_DISKSPACE: System out of disk space
    pub fn vnd_no_diskspace(&self) -> bool {
        matches!(self, VndNoDiskspace)
    }
    /// VND_NO_DISK_PERMISSIONS: No write permission for disk files
    pub fn vnd_no_disk_permissions(&self) -> bool {
        matches!(self, VndNoDiskPermissions)
    }
    /// VND_NO_SUCH_FILE_OR_DIR: Missing data file
    pub fn vnd_no_such_file_or_dir(&self) -> bool {
        matches!(self, VndNoSuchFileOrDir)
    }
    /// VND_OUT_OF_MEMORY: Out of memory
    pub fn vnd_out_of_memory(&self) -> bool {
        matches!(self, VndOutOfMemory)
    }
    /// VND_APP_ERROR: Unexpected generic error in vnode
    pub fn vnd_app_error(&self) -> bool {
        matches!(self, VndAppError)
    }
    /// VND_INVALID_VRESION_FILE: Invalid version file
    pub fn vnd_invalid_vresion_file(&self) -> bool {
        matches!(self, VndInvalidVresionFile)
    }
    /// VND_IS_FULL: Database memory is full for commit failed
    pub fn vnd_is_full(&self) -> bool {
        matches!(self, VndIsFull)
    }
    /// VND_IS_FLOWCTRL: Database memory is full for waiting commit
    pub fn vnd_is_flowctrl(&self) -> bool {
        matches!(self, VndIsFlowctrl)
    }
    /// VND_IS_DROPPING: Database is dropping
    pub fn vnd_is_dropping(&self) -> bool {
        matches!(self, VndIsDropping)
    }
    /// VND_IS_BALANCING: Database is balancing
    pub fn vnd_is_balancing(&self) -> bool {
        matches!(self, VndIsBalancing)
    }
    /// VND_NOT_SYNCED: Database suspended
    pub fn vnd_not_synced(&self) -> bool {
        matches!(self, VndNotSynced)
    }
    /// VND_NO_WRITE_AUTH: Database write operation denied
    pub fn vnd_no_write_auth(&self) -> bool {
        matches!(self, VndNoWriteAuth)
    }
    /// VND_IS_SYNCING: Database is syncing
    pub fn vnd_is_syncing(&self) -> bool {
        matches!(self, VndIsSyncing)
    }
    /// TDB_INVALID_TABLE_ID: Invalid table ID
    pub fn tdb_invalid_table_id(&self) -> bool {
        matches!(self, TdbInvalidTableId)
    }
    /// TDB_INVALID_TABLE_TYPE: Invalid table type
    pub fn tdb_invalid_table_type(&self) -> bool {
        matches!(self, TdbInvalidTableType)
    }
    /// TDB_IVD_TB_SCHEMA_VERSION: Invalid table schema version
    pub fn tdb_ivd_tb_schema_version(&self) -> bool {
        matches!(self, TdbIvdTbSchemaVersion)
    }
    /// TDB_TABLE_ALREADY_EXIST: Table already exists
    pub fn tdb_table_already_exist(&self) -> bool {
        matches!(self, TdbTableAlreadyExist)
    }
    /// TDB_INVALID_CONFIG: Invalid configuration
    pub fn tdb_invalid_config(&self) -> bool {
        matches!(self, TdbInvalidConfig)
    }
    /// TDB_INIT_FAILED: Tsdb init failed
    pub fn tdb_init_failed(&self) -> bool {
        matches!(self, TdbInitFailed)
    }
    /// TDB_NO_DISKSPACE: No diskspace for tsdb
    pub fn tdb_no_diskspace(&self) -> bool {
        matches!(self, TdbNoDiskspace)
    }
    /// TDB_NO_DISK_PERMISSIONS: No permission for disk files
    pub fn tdb_no_disk_permissions(&self) -> bool {
        matches!(self, TdbNoDiskPermissions)
    }
    /// TDB_FILE_CORRUPTED: Data file(s) corrupted
    pub fn tdb_file_corrupted(&self) -> bool {
        matches!(self, TdbFileCorrupted)
    }
    /// TDB_OUT_OF_MEMORY: Out of memory
    pub fn tdb_out_of_memory(&self) -> bool {
        matches!(self, TdbOutOfMemory)
    }
    /// TDB_TAG_VER_OUT_OF_DATE: Tag too old
    pub fn tdb_tag_ver_out_of_date(&self) -> bool {
        matches!(self, TdbTagVerOutOfDate)
    }
    /// TDB_TIMESTAMP_OUT_OF_RANGE: Timestamp data out of range
    pub fn tdb_timestamp_out_of_range(&self) -> bool {
        matches!(self, TdbTimestampOutOfRange)
    }
    /// TDB_SUBMIT_MSG_MSSED_UP: Submit message is messed up
    pub fn tdb_submit_msg_mssed_up(&self) -> bool {
        matches!(self, TdbSubmitMsgMssedUp)
    }
    /// TDB_INVALID_ACTION: Invalid operation
    pub fn tdb_invalid_action(&self) -> bool {
        matches!(self, TdbInvalidAction)
    }
    /// TDB_INVALID_CREATE_TB_MSG: Invalid creation of table
    pub fn tdb_invalid_create_tb_msg(&self) -> bool {
        matches!(self, TdbInvalidCreateTbMsg)
    }
    /// TDB_NO_TABLE_DATA_IN_MEM: No table data in memory skiplist
    pub fn tdb_no_table_data_in_mem(&self) -> bool {
        matches!(self, TdbNoTableDataInMem)
    }
    /// TDB_FILE_ALREADY_EXISTS: File already exists
    pub fn tdb_file_already_exists(&self) -> bool {
        matches!(self, TdbFileAlreadyExists)
    }
    /// TDB_TABLE_RECONFIGURE: Need to reconfigure table
    pub fn tdb_table_reconfigure(&self) -> bool {
        matches!(self, TdbTableReconfigure)
    }
    /// TDB_IVD_CREATE_TABLE_INFO: Invalid information to create table
    pub fn tdb_ivd_create_table_info(&self) -> bool {
        matches!(self, TdbIvdCreateTableInfo)
    }
    /// TDB_NO_AVAIL_DISK: No available disk
    pub fn tdb_no_avail_disk(&self) -> bool {
        matches!(self, TdbNoAvailDisk)
    }
    /// TDB_MESSED_MSG: TSDB messed message
    pub fn tdb_messed_msg(&self) -> bool {
        matches!(self, TdbMessedMsg)
    }
    /// TDB_IVLD_TAG_VAL: TSDB invalid tag value
    pub fn tdb_ivld_tag_val(&self) -> bool {
        matches!(self, TdbIvldTagVal)
    }
    /// QRY_INVALID_QHANDLE: Invalid handle
    pub fn qry_invalid_qhandle(&self) -> bool {
        matches!(self, QryInvalidQhandle)
    }
    /// QRY_INVALID_MSG: Invalid message
    pub fn qry_invalid_msg(&self) -> bool {
        matches!(self, QryInvalidMsg)
    }
    /// QRY_NO_DISKSPACE: No diskspace for query
    pub fn qry_no_diskspace(&self) -> bool {
        matches!(self, QryNoDiskspace)
    }
    /// QRY_OUT_OF_MEMORY: System out of memory
    pub fn qry_out_of_memory(&self) -> bool {
        matches!(self, QryOutOfMemory)
    }
    /// QRY_APP_ERROR: Unexpected generic error in query
    pub fn qry_app_error(&self) -> bool {
        matches!(self, QryAppError)
    }
    /// QRY_DUP_JOIN_KEY: Duplicated join key
    pub fn qry_dup_join_key(&self) -> bool {
        matches!(self, QryDupJoinKey)
    }
    /// QRY_EXCEED_TAGS_LIMIT: Tag conditon too many
    pub fn qry_exceed_tags_limit(&self) -> bool {
        matches!(self, QryExceedTagsLimit)
    }
    /// QRY_NOT_READY: Query not ready
    pub fn qry_not_ready(&self) -> bool {
        matches!(self, QryNotReady)
    }
    /// QRY_HAS_RSP: Query should response
    pub fn qry_has_rsp(&self) -> bool {
        matches!(self, QryHasRsp)
    }
    /// QRY_IN_EXEC: Multiple retrieval of this query
    pub fn qry_in_exec(&self) -> bool {
        matches!(self, QryInExec)
    }
    /// QRY_TOO_MANY_TIMEWINDOW: Too many time window in query
    pub fn qry_too_many_timewindow(&self) -> bool {
        matches!(self, QryTooManyTimewindow)
    }
    /// QRY_NOT_ENOUGH_BUFFER: Query buffer limit has reached
    pub fn qry_not_enough_buffer(&self) -> bool {
        matches!(self, QryNotEnoughBuffer)
    }
    /// QRY_INCONSISTAN: File inconsistency in replica
    pub fn qry_inconsistan(&self) -> bool {
        matches!(self, QryInconsistan)
    }
    /// GRANT_EXPIRED: License expired
    pub fn grant_expired(&self) -> bool {
        matches!(self, GrantExpired)
    }
    /// GRANT_DNODE_LIMITED: DNode creation limited by licence
    pub fn grant_dnode_limited(&self) -> bool {
        matches!(self, GrantDnodeLimited)
    }
    /// GRANT_ACCT_LIMITED: Account creation limited by license
    pub fn grant_acct_limited(&self) -> bool {
        matches!(self, GrantAcctLimited)
    }
    /// GRANT_TIMESERIES_LIMITED: Table creation limited by license
    pub fn grant_timeseries_limited(&self) -> bool {
        matches!(self, GrantTimeseriesLimited)
    }
    /// GRANT_DB_LIMITED: DB creation limited by license
    pub fn grant_db_limited(&self) -> bool {
        matches!(self, GrantDbLimited)
    }
    /// GRANT_USER_LIMITED: User creation limited by license
    pub fn grant_user_limited(&self) -> bool {
        matches!(self, GrantUserLimited)
    }
    /// GRANT_CONN_LIMITED: Conn creation limited by license
    pub fn grant_conn_limited(&self) -> bool {
        matches!(self, GrantConnLimited)
    }
    /// GRANT_STREAM_LIMITED: Stream creation limited by license
    pub fn grant_stream_limited(&self) -> bool {
        matches!(self, GrantStreamLimited)
    }
    /// GRANT_SPEED_LIMITED: Write speed limited by license
    pub fn grant_speed_limited(&self) -> bool {
        matches!(self, GrantSpeedLimited)
    }
    /// GRANT_STORAGE_LIMITED: Storage capacity limited by license
    pub fn grant_storage_limited(&self) -> bool {
        matches!(self, GrantStorageLimited)
    }
    /// GRANT_QUERYTIME_LIMITED: Query time limited by license
    pub fn grant_querytime_limited(&self) -> bool {
        matches!(self, GrantQuerytimeLimited)
    }
    /// GRANT_CPU_LIMITED: CPU cores limited by license
    pub fn grant_cpu_limited(&self) -> bool {
        matches!(self, GrantCpuLimited)
    }
    /// SYN_INVALID_CONFIG: Invalid Sync Configuration
    pub fn syn_invalid_config(&self) -> bool {
        matches!(self, SynInvalidConfig)
    }
    /// SYN_NOT_ENABLED: Sync module not enabled
    pub fn syn_not_enabled(&self) -> bool {
        matches!(self, SynNotEnabled)
    }
    /// SYN_INVALID_VERSION: Invalid Sync version
    pub fn syn_invalid_version(&self) -> bool {
        matches!(self, SynInvalidVersion)
    }
    /// SYN_CONFIRM_EXPIRED: Sync confirm expired
    pub fn syn_confirm_expired(&self) -> bool {
        matches!(self, SynConfirmExpired)
    }
    /// SYN_TOO_MANY_FWDINFO: Too many sync fwd infos
    pub fn syn_too_many_fwdinfo(&self) -> bool {
        matches!(self, SynTooManyFwdinfo)
    }
    /// SYN_MISMATCHED_PROTOCOL: Mismatched protocol
    pub fn syn_mismatched_protocol(&self) -> bool {
        matches!(self, SynMismatchedProtocol)
    }
    /// SYN_MISMATCHED_CLUSTERID: Mismatched clusterId
    pub fn syn_mismatched_clusterid(&self) -> bool {
        matches!(self, SynMismatchedClusterid)
    }
    /// SYN_MISMATCHED_SIGNATURE: Mismatched signature
    pub fn syn_mismatched_signature(&self) -> bool {
        matches!(self, SynMismatchedSignature)
    }
    /// SYN_INVALID_CHECKSUM: Invalid msg checksum
    pub fn syn_invalid_checksum(&self) -> bool {
        matches!(self, SynInvalidChecksum)
    }
    /// SYN_INVALID_MSGLEN: Invalid msg length
    pub fn syn_invalid_msglen(&self) -> bool {
        matches!(self, SynInvalidMsglen)
    }
    /// SYN_INVALID_MSGTYPE: Invalid msg type
    pub fn syn_invalid_msgtype(&self) -> bool {
        matches!(self, SynInvalidMsgtype)
    }
    /// WAL_APP_ERROR: Unexpected generic error in wal
    pub fn wal_app_error(&self) -> bool {
        matches!(self, WalAppError)
    }
    /// WAL_FILE_CORRUPTED: WAL file is corrupted
    pub fn wal_file_corrupted(&self) -> bool {
        matches!(self, WalFileCorrupted)
    }
    /// WAL_SIZE_LIMIT: WAL size exceeds limit
    pub fn wal_size_limit(&self) -> bool {
        matches!(self, WalSizeLimit)
    }
    /// HTTP_SERVER_OFFLINE: http server is not onlin
    pub fn http_server_offline(&self) -> bool {
        matches!(self, HttpServerOffline)
    }
    /// HTTP_UNSUPPORT_URL: url is not support
    pub fn http_unsupport_url(&self) -> bool {
        matches!(self, HttpUnsupportUrl)
    }
    /// HTTP_INVALID_URL: nvalid url format
    pub fn http_invalid_url(&self) -> bool {
        matches!(self, HttpInvalidUrl)
    }
    /// HTTP_NO_ENOUGH_MEMORY: no enough memory
    pub fn http_no_enough_memory(&self) -> bool {
        matches!(self, HttpNoEnoughMemory)
    }
    /// HTTP_REQUSET_TOO_BIG: request size is too big
    pub fn http_requset_too_big(&self) -> bool {
        matches!(self, HttpRequsetTooBig)
    }
    /// HTTP_NO_AUTH_INFO: no auth info input
    pub fn http_no_auth_info(&self) -> bool {
        matches!(self, HttpNoAuthInfo)
    }
    /// HTTP_NO_MSG_INPUT: request is empty
    pub fn http_no_msg_input(&self) -> bool {
        matches!(self, HttpNoMsgInput)
    }
    /// HTTP_NO_SQL_INPUT: no sql input
    pub fn http_no_sql_input(&self) -> bool {
        matches!(self, HttpNoSqlInput)
    }
    /// HTTP_NO_EXEC_USEDB: no need to execute use db cmd
    pub fn http_no_exec_usedb(&self) -> bool {
        matches!(self, HttpNoExecUsedb)
    }
    /// HTTP_SESSION_FULL: session list was full
    pub fn http_session_full(&self) -> bool {
        matches!(self, HttpSessionFull)
    }
    /// HTTP_GEN_TAOSD_TOKEN_ERR: generate taosd token error
    pub fn http_gen_taosd_token_err(&self) -> bool {
        matches!(self, HttpGenTaosdTokenErr)
    }
    /// HTTP_INVALID_MULTI_REQUEST: size of multi request is 0
    pub fn http_invalid_multi_request(&self) -> bool {
        matches!(self, HttpInvalidMultiRequest)
    }
    /// HTTP_CREATE_GZIP_FAILED: failed to create gzip
    pub fn http_create_gzip_failed(&self) -> bool {
        matches!(self, HttpCreateGzipFailed)
    }
    /// HTTP_FINISH_GZIP_FAILED: failed to finish gzip
    pub fn http_finish_gzip_failed(&self) -> bool {
        matches!(self, HttpFinishGzipFailed)
    }
    /// HTTP_LOGIN_FAILED: failed to login
    pub fn http_login_failed(&self) -> bool {
        matches!(self, HttpLoginFailed)
    }
    /// HTTP_INVALID_VERSION: invalid http version
    pub fn http_invalid_version(&self) -> bool {
        matches!(self, HttpInvalidVersion)
    }
    /// HTTP_INVALID_CONTENT_LENGTH: invalid content length
    pub fn http_invalid_content_length(&self) -> bool {
        matches!(self, HttpInvalidContentLength)
    }
    /// HTTP_INVALID_AUTH_TYPE: invalid type of Authorization
    pub fn http_invalid_auth_type(&self) -> bool {
        matches!(self, HttpInvalidAuthType)
    }
    /// HTTP_INVALID_AUTH_FORMAT: invalid format of Authorization
    pub fn http_invalid_auth_format(&self) -> bool {
        matches!(self, HttpInvalidAuthFormat)
    }
    /// HTTP_INVALID_BASIC_AUTH: invalid basic Authorization
    pub fn http_invalid_basic_auth(&self) -> bool {
        matches!(self, HttpInvalidBasicAuth)
    }
    /// HTTP_INVALID_TAOSD_AUTH: invalid taosd Authorization
    pub fn http_invalid_taosd_auth(&self) -> bool {
        matches!(self, HttpInvalidTaosdAuth)
    }
    /// HTTP_PARSE_METHOD_FAILED: failed to parse method
    pub fn http_parse_method_failed(&self) -> bool {
        matches!(self, HttpParseMethodFailed)
    }
    /// HTTP_PARSE_TARGET_FAILED: failed to parse target
    pub fn http_parse_target_failed(&self) -> bool {
        matches!(self, HttpParseTargetFailed)
    }
    /// HTTP_PARSE_VERSION_FAILED: failed to parse http version
    pub fn http_parse_version_failed(&self) -> bool {
        matches!(self, HttpParseVersionFailed)
    }
    /// HTTP_PARSE_SP_FAILED: failed to parse sp
    pub fn http_parse_sp_failed(&self) -> bool {
        matches!(self, HttpParseSpFailed)
    }
    /// HTTP_PARSE_STATUS_FAILED: failed to parse status
    pub fn http_parse_status_failed(&self) -> bool {
        matches!(self, HttpParseStatusFailed)
    }
    /// HTTP_PARSE_PHRASE_FAILED: failed to parse phrase
    pub fn http_parse_phrase_failed(&self) -> bool {
        matches!(self, HttpParsePhraseFailed)
    }
    /// HTTP_PARSE_CRLF_FAILED: failed to parse crlf
    pub fn http_parse_crlf_failed(&self) -> bool {
        matches!(self, HttpParseCrlfFailed)
    }
    /// HTTP_PARSE_HEADER_FAILED: failed to parse header
    pub fn http_parse_header_failed(&self) -> bool {
        matches!(self, HttpParseHeaderFailed)
    }
    /// HTTP_PARSE_HEADER_KEY_FAILED: failed to parse header key
    pub fn http_parse_header_key_failed(&self) -> bool {
        matches!(self, HttpParseHeaderKeyFailed)
    }
    /// HTTP_PARSE_HEADER_VAL_FAILED: failed to parse header val
    pub fn http_parse_header_val_failed(&self) -> bool {
        matches!(self, HttpParseHeaderValFailed)
    }
    /// HTTP_PARSE_CHUNK_SIZE_FAILED: failed to parse chunk size
    pub fn http_parse_chunk_size_failed(&self) -> bool {
        matches!(self, HttpParseChunkSizeFailed)
    }
    /// HTTP_PARSE_CHUNK_FAILED: failed to parse chunk
    pub fn http_parse_chunk_failed(&self) -> bool {
        matches!(self, HttpParseChunkFailed)
    }
    /// HTTP_PARSE_END_FAILED: failed to parse end section
    pub fn http_parse_end_failed(&self) -> bool {
        matches!(self, HttpParseEndFailed)
    }
    /// HTTP_PARSE_INVALID_STATE: invalid parse state
    pub fn http_parse_invalid_state(&self) -> bool {
        matches!(self, HttpParseInvalidState)
    }
    /// HTTP_PARSE_ERROR_STATE: failed to parse error section
    pub fn http_parse_error_state(&self) -> bool {
        matches!(self, HttpParseErrorState)
    }
    /// HTTP_GC_QUERY_NULL: query size is 0
    pub fn http_gc_query_null(&self) -> bool {
        matches!(self, HttpGcQueryNull)
    }
    /// HTTP_GC_QUERY_SIZE: query size can not more than 100
    pub fn http_gc_query_size(&self) -> bool {
        matches!(self, HttpGcQuerySize)
    }
    /// HTTP_GC_REQ_PARSE_ERROR: parse grafana json error
    pub fn http_gc_req_parse_error(&self) -> bool {
        matches!(self, HttpGcReqParseError)
    }
    /// HTTP_TG_DB_NOT_INPUT: database name can not be null
    pub fn http_tg_db_not_input(&self) -> bool {
        matches!(self, HttpTgDbNotInput)
    }
    /// HTTP_TG_DB_TOO_LONG: database name too long
    pub fn http_tg_db_too_long(&self) -> bool {
        matches!(self, HttpTgDbTooLong)
    }
    /// HTTP_TG_INVALID_JSON: invalid telegraf json fromat
    pub fn http_tg_invalid_json(&self) -> bool {
        matches!(self, HttpTgInvalidJson)
    }
    /// HTTP_TG_METRICS_NULL: metrics size is 0
    pub fn http_tg_metrics_null(&self) -> bool {
        matches!(self, HttpTgMetricsNull)
    }
    /// HTTP_TG_METRICS_SIZE: metrics size can not more than 1K
    pub fn http_tg_metrics_size(&self) -> bool {
        matches!(self, HttpTgMetricsSize)
    }
    /// HTTP_TG_METRIC_NULL: metric name not find
    pub fn http_tg_metric_null(&self) -> bool {
        matches!(self, HttpTgMetricNull)
    }
    /// HTTP_TG_METRIC_TYPE: metric name type should be string
    pub fn http_tg_metric_type(&self) -> bool {
        matches!(self, HttpTgMetricType)
    }
    /// HTTP_TG_METRIC_NAME_NULL: metric name length is 0
    pub fn http_tg_metric_name_null(&self) -> bool {
        matches!(self, HttpTgMetricNameNull)
    }
    /// HTTP_TG_METRIC_NAME_LONG: metric name length too long
    pub fn http_tg_metric_name_long(&self) -> bool {
        matches!(self, HttpTgMetricNameLong)
    }
    /// HTTP_TG_TIMESTAMP_NULL: timestamp not find
    pub fn http_tg_timestamp_null(&self) -> bool {
        matches!(self, HttpTgTimestampNull)
    }
    /// HTTP_TG_TIMESTAMP_TYPE: timestamp type should be integer
    pub fn http_tg_timestamp_type(&self) -> bool {
        matches!(self, HttpTgTimestampType)
    }
    /// HTTP_TG_TIMESTAMP_VAL_NULL: timestamp value smaller than 0
    pub fn http_tg_timestamp_val_null(&self) -> bool {
        matches!(self, HttpTgTimestampValNull)
    }
    /// HTTP_TG_TAGS_NULL: tags not find
    pub fn http_tg_tags_null(&self) -> bool {
        matches!(self, HttpTgTagsNull)
    }
    /// HTTP_TG_TAGS_SIZE_0: tags size is 0
    pub fn http_tg_tags_size_0(&self) -> bool {
        matches!(self, HttpTgTagsSize0)
    }
    /// HTTP_TG_TAGS_SIZE_LONG: tags size too long
    pub fn http_tg_tags_size_long(&self) -> bool {
        matches!(self, HttpTgTagsSizeLong)
    }
    /// HTTP_TG_TAG_NULL: tag is null
    pub fn http_tg_tag_null(&self) -> bool {
        matches!(self, HttpTgTagNull)
    }
    /// HTTP_TG_TAG_NAME_NULL: tag name is null
    pub fn http_tg_tag_name_null(&self) -> bool {
        matches!(self, HttpTgTagNameNull)
    }
    /// HTTP_TG_TAG_NAME_SIZE: tag name length too long
    pub fn http_tg_tag_name_size(&self) -> bool {
        matches!(self, HttpTgTagNameSize)
    }
    /// HTTP_TG_TAG_VALUE_TYPE: tag value type should be number or string
    pub fn http_tg_tag_value_type(&self) -> bool {
        matches!(self, HttpTgTagValueType)
    }
    /// HTTP_TG_TAG_VALUE_NULL: tag value is null
    pub fn http_tg_tag_value_null(&self) -> bool {
        matches!(self, HttpTgTagValueNull)
    }
    /// HTTP_TG_TABLE_NULL: table is null
    pub fn http_tg_table_null(&self) -> bool {
        matches!(self, HttpTgTableNull)
    }
    /// HTTP_TG_TABLE_SIZE: table name length too long
    pub fn http_tg_table_size(&self) -> bool {
        matches!(self, HttpTgTableSize)
    }
    /// HTTP_TG_FIELDS_NULL: fields not find
    pub fn http_tg_fields_null(&self) -> bool {
        matches!(self, HttpTgFieldsNull)
    }
    /// HTTP_TG_FIELDS_SIZE_0: fields size is 0
    pub fn http_tg_fields_size_0(&self) -> bool {
        matches!(self, HttpTgFieldsSize0)
    }
    /// HTTP_TG_FIELDS_SIZE_LONG: fields size too long
    pub fn http_tg_fields_size_long(&self) -> bool {
        matches!(self, HttpTgFieldsSizeLong)
    }
    /// HTTP_TG_FIELD_NULL: field is null
    pub fn http_tg_field_null(&self) -> bool {
        matches!(self, HttpTgFieldNull)
    }
    /// HTTP_TG_FIELD_NAME_NULL: field name is null
    pub fn http_tg_field_name_null(&self) -> bool {
        matches!(self, HttpTgFieldNameNull)
    }
    /// HTTP_TG_FIELD_NAME_SIZE: field name length too long
    pub fn http_tg_field_name_size(&self) -> bool {
        matches!(self, HttpTgFieldNameSize)
    }
    /// HTTP_TG_FIELD_VALUE_TYPE: field value type should be number or string
    pub fn http_tg_field_value_type(&self) -> bool {
        matches!(self, HttpTgFieldValueType)
    }
    /// HTTP_TG_FIELD_VALUE_NULL: field value is null
    pub fn http_tg_field_value_null(&self) -> bool {
        matches!(self, HttpTgFieldValueNull)
    }
    /// HTTP_TG_HOST_NOT_STRING: host type should be string
    pub fn http_tg_host_not_string(&self) -> bool {
        matches!(self, HttpTgHostNotString)
    }
    /// HTTP_TG_STABLE_NOT_EXIST: stable not exist
    pub fn http_tg_stable_not_exist(&self) -> bool {
        matches!(self, HttpTgStableNotExist)
    }
    /// HTTP_OP_DB_NOT_INPUT: database name can not be null
    pub fn http_op_db_not_input(&self) -> bool {
        matches!(self, HttpOpDbNotInput)
    }
    /// HTTP_OP_DB_TOO_LONG: database name too long
    pub fn http_op_db_too_long(&self) -> bool {
        matches!(self, HttpOpDbTooLong)
    }
    /// HTTP_OP_INVALID_JSON: invalid opentsdb json fromat
    pub fn http_op_invalid_json(&self) -> bool {
        matches!(self, HttpOpInvalidJson)
    }
    /// HTTP_OP_METRICS_NULL: metrics size is 0
    pub fn http_op_metrics_null(&self) -> bool {
        matches!(self, HttpOpMetricsNull)
    }
    /// HTTP_OP_METRICS_SIZE: metrics size can not more than 10K
    pub fn http_op_metrics_size(&self) -> bool {
        matches!(self, HttpOpMetricsSize)
    }
    /// HTTP_OP_METRIC_NULL: metric name not find
    pub fn http_op_metric_null(&self) -> bool {
        matches!(self, HttpOpMetricNull)
    }
    /// HTTP_OP_METRIC_TYPE: metric name type should be string
    pub fn http_op_metric_type(&self) -> bool {
        matches!(self, HttpOpMetricType)
    }
    /// HTTP_OP_METRIC_NAME_NULL: metric name length is 0
    pub fn http_op_metric_name_null(&self) -> bool {
        matches!(self, HttpOpMetricNameNull)
    }
    /// HTTP_OP_METRIC_NAME_LONG: metric name length can not more than 22
    pub fn http_op_metric_name_long(&self) -> bool {
        matches!(self, HttpOpMetricNameLong)
    }
    /// HTTP_OP_TIMESTAMP_NULL: timestamp not find
    pub fn http_op_timestamp_null(&self) -> bool {
        matches!(self, HttpOpTimestampNull)
    }
    /// HTTP_OP_TIMESTAMP_TYPE: timestamp type should be integer
    pub fn http_op_timestamp_type(&self) -> bool {
        matches!(self, HttpOpTimestampType)
    }
    /// HTTP_OP_TIMESTAMP_VAL_NULL: timestamp value smaller than 0
    pub fn http_op_timestamp_val_null(&self) -> bool {
        matches!(self, HttpOpTimestampValNull)
    }
    /// HTTP_OP_TAGS_NULL: tags not find
    pub fn http_op_tags_null(&self) -> bool {
        matches!(self, HttpOpTagsNull)
    }
    /// HTTP_OP_TAGS_SIZE_0: tags size is 0
    pub fn http_op_tags_size_0(&self) -> bool {
        matches!(self, HttpOpTagsSize0)
    }
    /// HTTP_OP_TAGS_SIZE_LONG: tags size too long
    pub fn http_op_tags_size_long(&self) -> bool {
        matches!(self, HttpOpTagsSizeLong)
    }
    /// HTTP_OP_TAG_NULL: tag is null
    pub fn http_op_tag_null(&self) -> bool {
        matches!(self, HttpOpTagNull)
    }
    /// HTTP_OP_TAG_NAME_NULL: tag name is null
    pub fn http_op_tag_name_null(&self) -> bool {
        matches!(self, HttpOpTagNameNull)
    }
    /// HTTP_OP_TAG_NAME_SIZE: tag name length too long
    pub fn http_op_tag_name_size(&self) -> bool {
        matches!(self, HttpOpTagNameSize)
    }
    /// HTTP_OP_TAG_VALUE_TYPE: tag value type should be boolean number or string
    pub fn http_op_tag_value_type(&self) -> bool {
        matches!(self, HttpOpTagValueType)
    }
    /// HTTP_OP_TAG_VALUE_NULL: tag value is null
    pub fn http_op_tag_value_null(&self) -> bool {
        matches!(self, HttpOpTagValueNull)
    }
    /// HTTP_OP_TAG_VALUE_TOO_LONG: tag value can not more than 64
    pub fn http_op_tag_value_too_long(&self) -> bool {
        matches!(self, HttpOpTagValueTooLong)
    }
    /// HTTP_OP_VALUE_NULL: value not find
    pub fn http_op_value_null(&self) -> bool {
        matches!(self, HttpOpValueNull)
    }
    /// HTTP_OP_VALUE_TYPE: value type should be boolean number or string
    pub fn http_op_value_type(&self) -> bool {
        matches!(self, HttpOpValueType)
    }
    /// ODBC_OOM: out of memory
    pub fn odbc_oom(&self) -> bool {
        matches!(self, OdbcOom)
    }
    /// ODBC_CONV_CHAR_NOT_NUM: convertion not a valid literal input
    pub fn odbc_conv_char_not_num(&self) -> bool {
        matches!(self, OdbcConvCharNotNum)
    }
    /// ODBC_CONV_UNDEF: convertion undefined
    pub fn odbc_conv_undef(&self) -> bool {
        matches!(self, OdbcConvUndef)
    }
    /// ODBC_CONV_TRUNC_FRAC: convertion fractional truncated
    pub fn odbc_conv_trunc_frac(&self) -> bool {
        matches!(self, OdbcConvTruncFrac)
    }
    /// ODBC_CONV_TRUNC: convertion truncated
    pub fn odbc_conv_trunc(&self) -> bool {
        matches!(self, OdbcConvTrunc)
    }
    /// ODBC_CONV_NOT_SUPPORT: convertion not supported
    pub fn odbc_conv_not_support(&self) -> bool {
        matches!(self, OdbcConvNotSupport)
    }
    /// ODBC_CONV_OOR: convertion numeric value out of range
    pub fn odbc_conv_oor(&self) -> bool {
        matches!(self, OdbcConvOor)
    }
    /// ODBC_OUT_OF_RANGE: out of range
    pub fn odbc_out_of_range(&self) -> bool {
        matches!(self, OdbcOutOfRange)
    }
    /// ODBC_NOT_SUPPORT: not supported yet
    pub fn odbc_not_support(&self) -> bool {
        matches!(self, OdbcNotSupport)
    }
    /// ODBC_INVALID_HANDLE: invalid handle
    pub fn odbc_invalid_handle(&self) -> bool {
        matches!(self, OdbcInvalidHandle)
    }
    /// ODBC_NO_RESULT: no result set
    pub fn odbc_no_result(&self) -> bool {
        matches!(self, OdbcNoResult)
    }
    /// ODBC_NO_FIELDS: no fields returned
    pub fn odbc_no_fields(&self) -> bool {
        matches!(self, OdbcNoFields)
    }
    /// ODBC_INVALID_CURSOR: invalid cursor
    pub fn odbc_invalid_cursor(&self) -> bool {
        matches!(self, OdbcInvalidCursor)
    }
    /// ODBC_STATEMENT_NOT_READY: statement not ready
    pub fn odbc_statement_not_ready(&self) -> bool {
        matches!(self, OdbcStatementNotReady)
    }
    /// ODBC_CONNECTION_BUSY: connection still busy
    pub fn odbc_connection_busy(&self) -> bool {
        matches!(self, OdbcConnectionBusy)
    }
    /// ODBC_BAD_CONNSTR: bad connection string
    pub fn odbc_bad_connstr(&self) -> bool {
        matches!(self, OdbcBadConnstr)
    }
    /// ODBC_BAD_ARG: bad argument
    pub fn odbc_bad_arg(&self) -> bool {
        matches!(self, OdbcBadArg)
    }
    /// ODBC_CONV_NOT_VALID_TS: not a valid timestamp
    pub fn odbc_conv_not_valid_ts(&self) -> bool {
        matches!(self, OdbcConvNotValidTs)
    }
    /// ODBC_CONV_SRC_TOO_LARGE: src too large
    pub fn odbc_conv_src_too_large(&self) -> bool {
        matches!(self, OdbcConvSrcTooLarge)
    }
    /// ODBC_CONV_SRC_BAD_SEQ: src bad sequence
    pub fn odbc_conv_src_bad_seq(&self) -> bool {
        matches!(self, OdbcConvSrcBadSeq)
    }
    /// ODBC_CONV_SRC_INCOMPLETE: src incomplete
    pub fn odbc_conv_src_incomplete(&self) -> bool {
        matches!(self, OdbcConvSrcIncomplete)
    }
    /// ODBC_CONV_SRC_GENERAL: src general
    pub fn odbc_conv_src_general(&self) -> bool {
        matches!(self, OdbcConvSrcGeneral)
    }
    /// FS_OUT_OF_MEMORY: tfs out of memory
    pub fn fs_out_of_memory(&self) -> bool {
        matches!(self, FsOutOfMemory)
    }
    /// FS_INVLD_CFG: tfs invalid mount config
    pub fn fs_invld_cfg(&self) -> bool {
        matches!(self, FsInvldCfg)
    }
    /// FS_TOO_MANY_MOUNT: tfs too many mount
    pub fn fs_too_many_mount(&self) -> bool {
        matches!(self, FsTooManyMount)
    }
    /// FS_DUP_PRIMARY: tfs duplicate primary mount
    pub fn fs_dup_primary(&self) -> bool {
        matches!(self, FsDupPrimary)
    }
    /// FS_NO_PRIMARY_DISK: tfs no primary mount
    pub fn fs_no_primary_disk(&self) -> bool {
        matches!(self, FsNoPrimaryDisk)
    }
    /// FS_NO_MOUNT_AT_TIER: tfs no mount at tier
    pub fn fs_no_mount_at_tier(&self) -> bool {
        matches!(self, FsNoMountAtTier)
    }
    /// FS_FILE_ALREADY_EXISTS: tfs file already exists
    pub fn fs_file_already_exists(&self) -> bool {
        matches!(self, FsFileAlreadyExists)
    }
    /// FS_INVLD_LEVEL: tfs invalid level
    pub fn fs_invld_level(&self) -> bool {
        matches!(self, FsInvldLevel)
    }
    /// FS_NO_VALID_DISK: tfs no valid disk
    pub fn fs_no_valid_disk(&self) -> bool {
        matches!(self, FsNoValidDisk)
    }
}

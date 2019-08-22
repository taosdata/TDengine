/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_TAOSMSG_H
#define TDENGINE_TAOSMSG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tsdb.h"

#define TSDB_CODE_SUCCESS                    0
#define TSDB_CODE_ACTION_IN_PROGRESS         1

#define TSDB_CODE_LAST_SESSION_NOT_FINISHED  5
#define TSDB_CODE_INVALID_SESSION_ID         6
#define TSDB_CODE_INVALID_TRAN_ID            7
#define TSDB_CODE_INVALID_MSG_TYPE           8
#define TSDB_CODE_ALREADY_PROCESSED          9
#define TSDB_CODE_AUTH_FAILURE               10
#define TSDB_CODE_WRONG_MSG_SIZE             11
#define TSDB_CODE_UNEXPECTED_RESPONSE        12
#define TSDB_CODE_INVALID_RESPONSE_TYPE      13
#define TSDB_CODE_NO_RESOURCE                14
#define TSDB_CODE_INVALID_TIME_STAMP         15
#define TSDB_CODE_MISMATCHED_METER_ID        16
#define TSDB_CODE_ACTION_TRANS_NOT_FINISHED  17
#define TSDB_CODE_ACTION_NOT_ONLINE          18
#define TSDB_CODE_ACTION_SEND_FAILD          19
#define TSDB_CODE_NOT_ACTIVE_SESSION         20
#define TSDB_CODE_INSERT_FAILED              21
#define TSDB_CODE_APP_ERROR                  22
#define TSDB_CODE_INVALID_IE                 23
#define TSDB_CODE_INVALID_VALUE              24
#define TSDB_CODE_REDIRECT                   25
#define TSDB_CODE_ALREADY_THERE              26
#define TSDB_CODE_INVALID_METER_ID           27
#define TSDB_CODE_INVALID_SQL                28
#define TSDB_CODE_NETWORK_UNAVAIL            29
#define TSDB_CODE_INVALID_MSG_LEN            30
#define TSDB_CODE_INVALID_DB                 31
#define TSDB_CODE_INVALID_TABLE              32
#define TSDB_CODE_DB_ALREADY_EXIST           33
#define TSDB_CODE_TABLE_ALREADY_EXIST        34
#define TSDB_CODE_INVALID_USER               35
#define TSDB_CODE_INVALID_ACCT               36
#define TSDB_CODE_INVALID_PASS               37
#define TSDB_CODE_DB_NOT_SELECTED            38
#define TSDB_CODE_MEMORY_CORRUPTED           39
#define TSDB_CODE_USER_ALREADY_EXIST         40
#define TSDB_CODE_NO_RIGHTS                  41
#define TSDB_CODE_DISCONNECTED               42
#define TSDB_CODE_NO_MASTER                  43
#define TSDB_CODE_NOT_CONFIGURED             44
#define TSDB_CODE_INVALID_OPTION             45
#define TSDB_CODE_NODE_OFFLINE               46
#define TSDB_CODE_SYNC_REQUIRED              47
#define TSDB_CODE_NO_ENOUGH_PNODES           48
#define TSDB_CODE_UNSYNCED                   49
#define TSDB_CODE_TOO_SLOW                   50
#define TSDB_CODE_OTHERS                     51
#define TSDB_CODE_NO_REMOVE_MASTER           52
#define TSDB_CODE_WRONG_SCHEMA               53
#define TSDB_CODE_NO_RESULT                  54
#define TSDB_CODE_TOO_MANY_USERS             55
#define TSDB_CODE_TOO_MANY_DATABSES          56
#define TSDB_CODE_TOO_MANY_TABLES            57
#define TSDB_CODE_TOO_MANY_DNODES            58
#define TSDB_CODE_TOO_MANY_ACCTS             59
#define TSDB_CODE_ACCT_ALREADY_EXIST         60
#define TSDB_CODE_DNODE_ALREADY_EXIST        61
#define TSDB_CODE_SDB_ERROR                  62
#define TSDB_CODE_METRICMETA_EXPIRED         63    // local cached metric-meta expired causes error in metric query
#define TSDB_CODE_NOT_READY                  64    // peer is not ready to process data
#define TSDB_CODE_MAX_SESSIONS               65    // too many sessions
#define TSDB_CODE_MAX_CONNECTIONS            66    // too many connections
#define TSDB_CODE_SESSION_ALREADY_EXIST      67
#define TSDB_CODE_NO_QSUMMARY                68
#define TSDB_CODE_SERV_OUT_OF_MEMORY         69
#define TSDB_CODE_INVALID_QHANDLE            70
#define TSDB_CODE_RELATED_TABLES_EXIST       71
#define TSDB_CODE_MONITOR_DB_FORBEIDDEN      72
#define TSDB_CODE_VG_COMMITLOG_INIT_FAILED   73
#define TSDB_CODE_VG_INIT_FAILED             74
#define TSDB_CODE_DATA_ALREADY_IMPORTED      75
#define TSDB_CODE_OPS_NOT_SUPPORT            76
#define TSDB_CODE_INVALID_QUERY_ID           77
#define TSDB_CODE_INVALID_STREAM_ID          78
#define TSDB_CODE_INVALID_CONNECTION         79
#define TSDB_CODE_ACTION_NOT_BALANCED        80
#define TSDB_CODE_CLI_OUT_OF_MEMORY          81
#define TSDB_CODE_DATA_OVERFLOW              82
#define TSDB_CODE_QUERY_CANCELLED            83
#define TSDB_CODE_GRANT_TIMESERIES_LIMITED   84
#define TSDB_CODE_GRANT_EXPIRED              85
#define TSDB_CODE_CLI_NO_DISKSPACE           86
#define TSDB_CODE_FILE_CORRUPTED             87
#define TSDB_CODE_INVALID_CLIENT_VERSION     88
#define TSDB_CODE_INVALID_ACCT_PARAMETER     89
#define TSDB_CODE_NOT_ENOUGH_TIME_SERIES     90
#define TSDB_CODE_NO_WRITE_ACCESS            91
#define TSDB_CODE_NO_READ_ACCESS             92
#define TSDB_CODE_GRANT_DB_LIMITED           93
#define TSDB_CODE_GRANT_USER_LIMITED         94
#define TSDB_CODE_GRANT_CONN_LIMITED         95
#define TSDB_CODE_GRANT_STREAM_LIMITED       96
#define TSDB_CODE_GRANT_SPEED_LIMITED        97
#define TSDB_CODE_GRANT_STORAGE_LIMITED      98
#define TSDB_CODE_GRANT_QUERYTIME_LIMITED    99
#define TSDB_CODE_GRANT_ACCT_LIMITED         100
#define TSDB_CODE_GRANT_DNODE_LIMITED        101
#define TSDB_CODE_GRANT_CPU_LIMITED          102
#define TSDB_CODE_SESSION_NOT_READY          103      // table NOT in ready state
#define TSDB_CODE_BATCH_SIZE_TOO_BIG         104
#define TSDB_CODE_TIMESTAMP_OUT_OF_RANGE     105
#define TSDB_CODE_INVALID_QUERY_MSG          106      // failed to validate the sql expression msg by vnode
#define TSDB_CODE_CACHE_BLOCK_TS_DISORDERED  107      // time stamp in cache block is disordered
#define TSDB_CODE_FILE_BLOCK_TS_DISORDERED   108      // time stamp in file block is disordered
#define TSDB_CODE_INVALID_COMMIT_LOG         109      // invalid commit log may be caused by insufficient sotrage

// message type
#define TSDB_MSG_TYPE_REG              1
#define TSDB_MSG_TYPE_REG_RSP          2
#define TSDB_MSG_TYPE_SUBMIT           3
#define TSDB_MSG_TYPE_SUBMIT_RSP       4
#define TSDB_MSG_TYPE_NWCHANGE         5
#define TSDB_MSG_TYPE_NWCHANGE_RSP     6
#define TSDB_MSG_TYPE_DELIVER          7
#define TSDB_MSG_TYPE_DELIVER_RSP      8

#define TSDB_MSG_TYPE_CREATE           9
#define TSDB_MSG_TYPE_CREATE_RSP       10
#define TSDB_MSG_TYPE_REMOVE           11
#define TSDB_MSG_TYPE_REMOVE_RSP       12
#define TSDB_MSG_TYPE_VPEERS           13
#define TSDB_MSG_TYPE_VPEERS_RSP       14
#define TSDB_MSG_TYPE_FREE_VNODE       15
#define TSDB_MSG_TYPE_FREE_VNODE_RSP   16
#define TSDB_MSG_TYPE_VPEER_CFG        17
#define TSDB_MSG_TYPE_VPEER_CFG_RSP    18
#define TSDB_MSG_TYPE_METER_CFG        19
#define TSDB_MSG_TYPE_METER_CFG_RSP    20

#define TSDB_MSG_TYPE_VPEER_FWD        21
#define TSDB_MSG_TYPE_VPEER_FWD_RSP    22
#define TSDB_MSG_TYPE_SYNC             23
#define TSDB_MSG_TYPE_SYNC_RSP         24

#define TSDB_MSG_TYPE_INSERT           25
#define TSDB_MSG_TYPE_INSERT_RSP       26
#define TSDB_MSG_TYPE_QUERY            27
#define TSDB_MSG_TYPE_QUERY_RSP        28
#define TSDB_MSG_TYPE_RETRIEVE         29
#define TSDB_MSG_TYPE_RETRIEVE_RSP     30

#define TSDB_MSG_TYPE_CONNECT          31
#define TSDB_MSG_TYPE_CONNECT_RSP      32
#define TSDB_MSG_TYPE_CREATE_ACCT      33
#define TSDB_MSG_TYPE_CREATE_ACCT_RSP  34
#define TSDB_MSG_TYPE_CREATE_USER      35
#define TSDB_MSG_TYPE_CREATE_USER_RSP  36
#define TSDB_MSG_TYPE_DROP_ACCT        37
#define TSDB_MSG_TYPE_DROP_ACCT_RSP    38
#define TSDB_MSG_TYPE_DROP_USER        39
#define TSDB_MSG_TYPE_DROP_USER_RSP    40
#define TSDB_MSG_TYPE_ALTER_USER       41
#define TSDB_MSG_TYPE_ALTER_USER_RSP   42
#define TSDB_MSG_TYPE_CREATE_MNODE     43
#define TSDB_MSG_TYPE_CREATE_MNODE_RSP 44
#define TSDB_MSG_TYPE_DROP_MNODE       45
#define TSDB_MSG_TYPE_DROP_MNODE_RSP   46
#define TSDB_MSG_TYPE_CREATE_PNODE     47
#define TSDB_MSG_TYPE_CREATE_PNODE_RSP 48
#define TSDB_MSG_TYPE_DROP_PNODE       49
#define TSDB_MSG_TYPE_DROP_PNODE_RSP   50
#define TSDB_MSG_TYPE_CREATE_DB        51
#define TSDB_MSG_TYPE_CREATE_DB_RSP    52
#define TSDB_MSG_TYPE_DROP_DB          53
#define TSDB_MSG_TYPE_DROP_DB_RSP      54
#define TSDB_MSG_TYPE_USE_DB           55
#define TSDB_MSG_TYPE_USE_DB_RSP       56
#define TSDB_MSG_TYPE_CREATE_TABLE     57
#define TSDB_MSG_TYPE_CREATE_TABLE_RSP 58
#define TSDB_MSG_TYPE_DROP_TABLE       59
#define TSDB_MSG_TYPE_DROP_TABLE_RSP   60
#define TSDB_MSG_TYPE_METERINFO        61
#define TSDB_MSG_TYPE_METERINFO_RSP    62
#define TSDB_MSG_TYPE_METRIC_META      63
#define TSDB_MSG_TYPE_METRIC_META_RSP  64
#define TSDB_MSG_TYPE_SHOW             65
#define TSDB_MSG_TYPE_SHOW_RSP         66

#define TSDB_MSG_TYPE_FORWARD          67
#define TSDB_MSG_TYPE_FORWARD_RSP      68

#define TSDB_MSG_TYPE_CFG_PNODE        69
#define TSDB_MSG_TYPE_CFG_PNODE_RSP    70
#define TSDB_MSG_TYPE_CFG_MNODE        71
#define TSDB_MSG_TYPE_CFG_MNODE_RSP    72

#define TSDB_MSG_TYPE_KILL_QUERY       73
#define TSDB_MSG_TYPE_KILL_QUERY_RSP   74
#define TSDB_MSG_TYPE_KILL_STREAM      75
#define TSDB_MSG_TYPE_KILL_STREAM_RSP  76
#define TSDB_MSG_TYPE_KILL_CONNECTION  77
#define TSDB_MSG_TYPE_KILL_CONNECTION_RSP 78

#define TSDB_MSG_TYPE_ALTER_STREAM     79
#define TSDB_MSG_TYPE_ALTER_STREAM_RSP 80
#define TSDB_MSG_TYPE_ALTER_TABLE      81
#define TSDB_MSG_TYPE_ALTER_TABLE_RSP  82
#define TSDB_MSG_TYPE_ALTER_DB         83
#define TSDB_MSG_TYPE_ALTER_DB_RSP     84

#define TSDB_MSG_TYPE_HEARTBEAT        91
#define TSDB_MSG_TYPE_HEARTBEAT_RSP    92
#define TSDB_MSG_TYPE_STATUS           93
#define TSDB_MSG_TYPE_STATUS_RSP       94
#define TSDB_MSG_TYPE_GRANT            95
#define TSDB_MSG_TYPE_GRANT_RSP        96

#define TSDB_MSG_TYPE_ALTER_ACCT       97
#define TSDB_MSG_TYPE_ALTER_ACCT_RSP   98
#define TSDB_MSG_TYPE_MAX              99

// IE type
#define TSDB_IE_TYPE_SEC               1
#define TSDB_IE_TYPE_META              2
#define TSDB_IE_TYPE_MGMT_IP           3
#define TSDB_IE_TYPE_DNODE_CFG         4
#define TSDB_IE_TYPE_NEW_VERSION       5
#define TSDB_IE_TYPE_DNODE_EXT         6
#define TSDB_IE_TYPE_DNODE_STATE       7

// mgmt table
enum _mgmt_table {
  TSDB_MGMT_TABLE_USER,
  TSDB_MGMT_TABLE_DB,
  TSDB_MGMT_TABLE_TABLE,
  TSDB_MGMT_TABLE_PNODE,
  TSDB_MGMT_TABLE_VGROUP,
  TSDB_MGMT_TABLE_METRIC,
  TSDB_MGMT_TABLE_QUERIES,
  TSDB_MGMT_TABLE_STREAMS,
  TSDB_MGMT_TABLE_CONNS,
  TSDB_MGMT_TABLE_MAX,
};

#define TSDB_ALTER_TABLE_ADD_TAG_COLUMN     1
#define TSDB_ALTER_TABLE_DROP_TAG_COLUMN    2
#define TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN  3
#define TSDB_ALTER_TABLE_UPDATE_TAG_VAL     4

#define TSDB_ALTER_TABLE_ADD_COLUMN         5
#define TSDB_ALTER_TABLE_DROP_COLUMN        6

#define TSDB_INTERPO_NONE              0
#define TSDB_INTERPO_NULL              1
#define TSDB_INTERPO_SET_VALUE         2
#define TSDB_INTERPO_LINEAR            3
#define TSDB_INTERPO_PREV              4

#define TSDB_ALTER_USER_PASSWD         0x1
#define TSDB_ALTER_USER_PRIVILEGES     0x2

#define TSDB_KILL_MSG_LEN              30

#define TSDB_METER_METRIC              0  // metric
#define TSDB_METER_MTABLE              1  // table created from metric
#define TSDB_METER_OTABLE              2  // ordinary table
#define TSDB_METER_STABLE              3  // table created from stream computing
#define TSDB_MAX_METER_TYPES           4

#define TSDB_VN_READ_ACCCESS           ((char)0x1)
#define TSDB_VN_WRITE_ACCCESS          ((char)0x2)
#define TSDB_VN_ALL_ACCCESS            (TSDB_VN_READ_ACCCESS|TSDB_VN_WRITE_ACCCESS)


extern char *taosMsg[];
extern char *tsError[];

#pragma pack(1)

typedef struct {
  char     numOfIps;
  uint32_t ip[];
} SIpList;

typedef struct {
  char     numOfIps;
  uint32_t ip[TSDB_MAX_MGMT_IPS];
} SMgmtIpList;

typedef struct {
  char     version : 4;
  char     comp : 4;
  char     tcp : 2;
  char     spi : 3;
  char     encrypt : 3;
  uint16_t tranId;
  uint32_t uid;  // for unique ID inside a client
  uint32_t sourceId;

  // internal part
  uint32_t destId;
  char     meterId[TSDB_UNI_LEN];
  short    port;  // for UDP only
  char     empty[1];
  char     msgType;
  int32_t  msgLen;
  uint8_t  content[0];
} STaosHeader;

typedef struct {
  uint32_t timeStamp;
  uint8_t  auth[TSDB_AUTH_LEN];
} STaosDigest;

typedef struct {
  unsigned char code;
  char          more[];
} STaosRsp, SMsgReply;

typedef struct {
  uint32_t customerId;
  uint32_t osId;
  uint32_t appId;
  char     hwId[TSDB_UNI_LEN];
  char     hwVersion[TSDB_VERSION_LEN];
  char     osVersion[TSDB_VERSION_LEN];
  char     appVersion[TSDB_VERSION_LEN];
  char     sdkVersion[TSDB_VERSION_LEN];
  char     name[TSDB_UNI_LEN];
  char     street[TSDB_STREET_LEN];
  char     city[TSDB_CITY_LEN];
  char     state[TSDB_STATE_LEN];
  char     country[TSDB_COUNTRY_LEN];
  uint32_t longitude;
  uint32_t latitude;
} SRegMsg;

typedef struct {
  short numOfRows;
  char  payLoad[];
} SSubmitMsg;

typedef struct {
  int32_t  sid;
  int32_t  sversion;
  uint64_t uid;
  short    numOfRows;
  char     payLoad[];
} SShellSubmitBlock;

typedef struct {
  short   import;
  short   vnode;
  int32_t numOfSid; /* total number of sid */
  char    blks[];   /* numOfSid blocks, each blocks for one meter */
} SShellSubmitMsg;

typedef struct SSchema {
  char  type;
  char  name[TSDB_COL_NAME_LEN];
  short colId;
  short bytes;
} SSchema;

typedef struct SMColumn {
  char  type;
  short colId;
  short bytes;
} SMColumn;

typedef struct {
  short    vnode;
  int32_t  sid;
  uint64_t uid;
  char     spi;
  char     encrypt;
  char     meterId[TSDB_METER_ID_LEN];
  char     secret[TSDB_KEY_LEN];
  char     cipheringKey[TSDB_KEY_LEN];
  uint64_t timeStamp;
  uint64_t lastCreate;
  short    numOfColumns;
  short    sqlLen;  // SQL string is after schema
  char     reserved[16];
  int32_t  sversion;
  SMColumn schema[];
} SCreateMsg;

typedef struct {
  char  db[TSDB_DB_NAME_LEN];
  short ignoreNotExists;
} SDropDbMsg, SUseDbMsg;

typedef struct { char user[TSDB_USER_LEN]; } SDropUserMsg, SDropAcctMsg;

typedef struct { char db[TSDB_DB_NAME_LEN]; } SShowTableMsg;

typedef struct {
  char meterId[TSDB_METER_ID_LEN];
  char igExists;

  short numOfTags;

  short numOfColumns;
  short sqlLen;  // the length of SQL, it starts after schema , sql is a
  // null-terminated string
  char reserved[16];

  SSchema schema[];
} SCreateTableMsg;

typedef struct {
  char meterId[TSDB_METER_ID_LEN];
  char igNotExists;
} SDropTableMsg;

typedef struct {
  char    meterId[TSDB_METER_ID_LEN];
  short   type; /* operation type   */
  char    tagVal[TSDB_MAX_BYTES_PER_ROW];
  short   numOfCols; /* number of schema */
  SSchema schema[];
} SAlterTableMsg;

typedef struct { char db[TSDB_METER_ID_LEN]; } SConnectMsg;

typedef struct {
  int32_t maxUsers;
  int32_t maxDbs;
  int32_t maxTimeSeries;
  int32_t maxConnections;
  int32_t maxStreams;
  int32_t maxPointsPerSecond;
  int64_t maxStorage;    // In unit of GB
  int64_t maxQueryTime;  // In unit of hour
  int64_t maxInbound;
  int64_t maxOutbound;
  char    accessState;  // Configured only by command
} SAcctCfg;

typedef struct {
  char     user[TSDB_USER_LEN];
  char     pass[TSDB_KEY_LEN];
  SAcctCfg cfg;
} SCreateAcctMsg, SAlterAcctMsg;

typedef struct {
  char user[TSDB_USER_LEN];
  char pass[TSDB_KEY_LEN];
  char privilege;
  char flag;
} SCreateUserMsg, SAlterUserMsg;

typedef struct { char db[TSDB_METER_ID_LEN]; } SMgmtHead;

typedef struct {
  char acctId[TSDB_ACCT_LEN];
  char version[TSDB_VERSION_LEN];
  char writeAuth;
  char superAuth;
} SConnectRsp;

typedef struct {
  short    vnode;
  int32_t  sid;
  uint64_t uid;
  char     meterId[TSDB_METER_ID_LEN];
} SRemoveMeterMsg;

typedef struct { short vnode; } SFreeVnodeMsg;

typedef struct SColIndex {
  int16_t colId;
  int16_t colIdx;
  bool    isTag;
} SColIndex;

typedef struct SColIndexEx {
  int16_t colId;
  /*
   * colIdx is the index of column in latest schema of table
   * it is available in the client side. Also used to determine
   * whether current meter schema is up-to-date.
   *
   * colIdxInBuf is used to denote the index of column in pQuery->colList,
   * this value is invalid in client side, as well as in cache block of vnode
   * either.
   */
  int16_t colIdx;
  int16_t colIdxInBuf;
  bool    isTag;
} SColIndexEx;

/* sql function msg, to describe the message to vnode about sql function
 * operations in select clause */
typedef struct SSqlFuncExprMsg {
  int16_t functionId;
  int16_t numOfParams;

  SColIndexEx colInfo;
  struct ArgElem {
    int16_t argType;
    int16_t argBytes;
    union {
      double  d;
      int64_t i64;
      char *  pz;
    } argValue;
  } arg[3];
} SSqlFuncExprMsg;

typedef struct SSqlBinaryExprInfo {
  struct tSQLBinaryExpr *pBinExpr;    /*  for binary expression */
  int32_t                numOfCols;   /*  binary expression involves the readed number of columns*/
  SColIndexEx *          pReqColumns; /*  source column list */
} SSqlBinaryExprInfo;

typedef struct SSqlFunctionExpr {
  SSqlFuncExprMsg    pBase;
  SSqlBinaryExprInfo pBinExprInfo;
  int16_t            resBytes;
  int16_t            resType;
} SSqlFunctionExpr;

typedef struct SColumnFilterMsg {
  /* for client side struct, we only need the column id, type, bytes are not
   * necessary
   * But for data in vnode side, we need all the following information.
   * */
  int16_t colId;
  int16_t type;
  int16_t bytes;

  int16_t filterOn; /* denote if the filter is active       */
  int16_t lowerRelOptr;
  int16_t upperRelOptr;
  int16_t filterOnBinary; /* denote if current column is binary   */

  /* double/int64_t/float/int share the this memory */
  union {
    struct {
      int64_t lowerBndi;
      int64_t upperBndi;
    };
    struct {
      double lowerBndd;
      double upperBndd;
    };
    struct {
      int64_t pz;
      int64_t len;
    };
  };
} SColumnFilterMsg;

/*
 * enable vnode to understand how to group several tables with different tag;
 */
typedef struct SMeterSidExtInfo {
  // union    {int32_t sid; void* pObj;};
  int32_t sid;
  void *  pObj;
  char    tags[];
} SMeterSidExtInfo;

/*
 * the outputCols is equalled to or larger than numOfCols
 * e.g., select min(colName), max(colName), avg(colName) from meter_name
 * the outputCols will be 3 while the numOfCols is 1.
 */
typedef struct {
  int16_t  vnode;
  int32_t  numOfSids;
  uint64_t pSidExtInfo;  // meter id & tag info ptr, in windows pointer may
  // occupy only 4bytes

  uint64_t uid;
  char     meterId[TSDB_METER_ID_LEN];
  TSKEY    skey;
  TSKEY    ekey;
  int32_t  num;

  int16_t order;
  int16_t orderColId;

  int16_t numOfCols;         // the number of columns will be load from vnode
  char    intervalTimeUnit;  // time interval type, for revisement of interval(1d)

  int64_t  nAggTimeInterval;  // time interval for aggregation, in million second
  uint64_t pTagSchema;        // tag schema, used to parse tag information in pSidExtInfo

  int16_t numOfTagsCols;  // required number of tags
  int16_t tagLength;      // tag length in current query

  int16_t  numOfGroupbyCols;  // num of group by columns
  int16_t  orderByIdx;
  int16_t  orderType;  // used in group by xx order by xxx
  uint64_t groupbyTagIds;

  int64_t limit;
  int64_t offset;

  int16_t metricQuery;      // denote another query process
  int16_t numOfOutputCols;  // final output columns numbers

  int16_t  interpoType;  // interpolate type
  uint64_t defaultVal;   // default value array list

  int32_t colNameLen;
  int64_t colNameList;

  int64_t          pSqlFuncExprs;
  SColumnFilterMsg colList[];
} SQueryMeterMsg;

typedef struct {
  char     code;
  uint64_t qhandle;
} SQueryMeterRsp;

typedef struct {
  TSKEY   skey;
  TSKEY   ekey;
  int32_t num;
  short   order;
  short   numOfCols;
  short   colList[];
} SQueryMsg;

typedef struct {
  uint64_t qhandle;
  char     free;
} SRetrieveMeterMsg;

typedef struct {
  int32_t numOfRows;
  int16_t precision;
  int64_t offset;  // updated offset value for multi-vnode projection query
  int64_t useconds;
  char    data[];
} SRetrieveMeterRsp;

typedef struct {
  uint32_t vnode;
  uint32_t vgId;
  uint8_t  status;
  uint8_t  dropStatus;
  uint8_t  accessState;
  int64_t  totalStorage;
  int64_t  compStorage;
  int64_t  pointsWritten;
  char     reserved[16];
} SVnodeLoad;

typedef struct {
  uint32_t vnode;
  char     accessState;
} SVnodeAccess;

// NOTE: sizeof(SVnodeCfg) < TSDB_FILE_HEADER_LEN/4
typedef struct {
  char     acct[TSDB_USER_LEN];
  char     db[TSDB_DB_NAME_LEN];
  uint32_t vgId;
  int32_t  maxSessions;
  int32_t  cacheBlockSize;
  union {
    int32_t totalBlocks;
    float   fraction;
  } cacheNumOfBlocks;
  int32_t daysPerFile;

  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t daysToKeep;

  int32_t commitTime;
  int32_t rowsInFileBlock;
  int16_t blocksPerMeter;
  char    compression;
  char    commitLog;
  char    replications;

  char repStrategy;
  char loadLatest;  // load into mem or not
  char precision;   // time resoluation

  char reserved[16];
} SVnodeCfg, SCreateDbMsg, SDbCfg, SAlterDbMsg;

// IMPORTANT: sizeof(SVnodeStatisticInfo) should not exceed
// TSDB_FILE_HEADER_LEN/4 - TSDB_FILE_HEADER_VERSION_SIZE
typedef struct {
  int64_t pointsWritten;  // In unit of points
  int64_t totalStorage;   // In unit of bytes
  int64_t compStorage;    // In unit of bytes
  int64_t queryTime;      // In unit of second ??
  char    reserved[64];
} SVnodeStatisticInfo;

typedef struct {
  uint32_t   version;
  uint32_t   publicIp;
  uint32_t   lastReboot;  // time stamp for last reboot
  uint16_t   numOfCores;
  uint8_t    alternativeRole;
  uint8_t    reserve;
  float      memoryAvailable;  // MB
  float      diskAvailable;    // GB
  uint32_t   openVnodes;
  char       reserved[16];
  SVnodeLoad load[];
} SStatusMsg;

typedef struct {
  uint32_t moduleStatus;
  uint32_t createdTime;
  uint32_t numOfVnodes;
  uint32_t reserved;
} SDnodeState;

// internal message
typedef struct {
  uint32_t destId;
  char     meterId[TSDB_UNI_LEN];
  char     empty[3];
  char     msgType;
  int32_t  msgLen;
  uint8_t  content[0];
} SIntMsg;

typedef struct {
  char spi;
  char encrypt;
  char secret[TSDB_KEY_LEN];  // key is changed if updated
  char cipheringKey[TSDB_KEY_LEN];
} SSecIe;

typedef struct {
  uint32_t ip;
  uint32_t vnode;
} SVPeerDesc;

typedef struct {
  int32_t    vnode;
  SVnodeCfg  cfg;
  SVPeerDesc vpeerDesc[];
} SVPeersMsg;

typedef struct {
  char  meterId[TSDB_METER_ID_LEN];
  short createFlag;
  char  tags[];
} SMeterInfoMsg;

typedef struct {
  char meterId[TSDB_METER_ID_LEN];

  int16_t  numOfGroupbyCols;  // num of group by columns
  int16_t  orderIndex;
  int16_t  orderType;  // used in group by xx order by xxx
  uint64_t groupbyTagIds;

  int16_t tagCols[TSDB_MAX_TAGS + 1];  // required tag columns, plus one is for table name
  int16_t numOfTags;                   // required number of tags

  int64_t limit;
  int64_t offset;

  int32_t condLength;
  int16_t type;  // denotes if it has the meter id pools
  char    tags[];
} SMetricMetaMsg;

typedef struct {
  SVPeerDesc vpeerDesc[TSDB_VNODES_SUPPORT];
  int16_t    index;  // used locally
  int32_t    numOfSids;
  int32_t    pSidExtInfoList[];  // offset value of SMeterSidExtInfo
} SVnodeSidList;

typedef struct {
  int32_t  numOfMeters;
  int32_t  numOfVnodes;
  uint16_t tagLen; /* tag value length */
  int32_t  list[]; /* offset of SVnodeSidList, compared to the SMetricMeta struct */
} SMetricMeta;

typedef struct SMeterMeta {
  int16_t numOfTags;
  int16_t precision;
  int16_t numOfColumns;

  int32_t    sversion;
  uint64_t   pSchema;
  SVPeerDesc vpeerDesc[TSDB_VNODES_SUPPORT];

  int32_t sid;
  int32_t vgid;

  uint64_t uid;
  int16_t  meterType;
  int16_t  index;    // used locally
  int32_t  rowSize;  // used locally, calculated in client
  uint64_t tags;
} SMeterMeta;

typedef struct {
  char name[TSDB_METER_ID_LEN];
  char data[TSDB_MAX_TAGS_LEN];
} STagData;

/*
 * sql: show tables like '%a_%'
 * payload is the query condition, e.g., '%a_%'
 * payloadLen is the length of payload
 */
typedef struct {
  char     type;
  uint16_t payloadLen;
  char     payload[];
} SShowMsg;

typedef struct { char ip[20]; } SCreateMnodeMsg, SDropMnodeMsg, SCreateDnodeMsg, SDropDnodeMsg;

typedef struct {
  uint64_t   qhandle;
  SMeterMeta meterMeta;
} SShowRspMsg;

typedef struct {
  int32_t vnode;
  int32_t sid;
} SMeterCfgMsg;

typedef struct { int32_t vnode; } SVpeerCfgMsg;

typedef struct {
  char ip[20];
  char config[60];
} SCfgMsg;

typedef struct {
  uint32_t queryId;
  uint32_t streamId;
  char     killConnection;
  SIpList  ipList;
} SHeartBeatRsp;

typedef struct {
  char     sql[TSDB_SHOW_SQL_LEN];
  uint32_t queryId;
  int64_t  useconds;
  int64_t  stime;
} SQDesc;

typedef struct {
  char     sql[TSDB_SHOW_SQL_LEN];
  uint32_t streamId;
  int64_t  num;  // number of computing/cycles
  int64_t  useconds;
  int64_t  ctime;
  int64_t  stime;
  int64_t  slidingTime;
  int64_t  interval;
} SSDesc;

typedef struct {
  int32_t numOfQueries;
  SQDesc  qdesc[];
} SQList;

typedef struct {
  int32_t numOfStreams;
  SSDesc  sdesc[];
} SSList;

typedef struct {
  uint64_t handle;
  char     queryId[TSDB_KILL_MSG_LEN];
} SKillQuery, SKillStream, SKillConnection;

typedef struct {
  short    vnode;
  int32_t  sid;
  uint64_t uid;
  uint64_t stime;  // stream starting time
  char     status;
} SAlterStreamMsg;

#pragma pack()

#ifdef __cplusplus
}
#endif

#endif

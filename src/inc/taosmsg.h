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

#include <stdint.h>
#include <stdbool.h>

#include "taosdef.h"
#include "taoserror.h"
#include "taosdef.h"

// message type
#define TSDB_MSG_TYPE_REG                           1
#define TSDB_MSG_TYPE_REG_RSP                       2
#define TSDB_MSG_TYPE_DNODE_SUBMIT                  3
#define TSDB_MSG_TYPE_DNODE_SUBMIT_RSP              4
#define TSDB_MSG_TYPE_DNODE_QUERY                   5
#define TSDB_MSG_TYPE_DNODE_QUERY_RSP               6
#define TSDB_MSG_TYPE_DNODE_RETRIEVE                7
#define TSDB_MSG_TYPE_DNODE_RETRIEVE_RSP            8
#define TSDB_MSG_TYPE_DNODE_CREATE_CHILD_TABLE      9
#define TSDB_MSG_TYPE_DNODE_CREATE_CHILD_TABLE_RSP  10
#define TSDB_MSG_TYPE_DNODE_CREATE_NORMAL_TABLE     11
#define TSDB_MSG_TYPE_DNODE_CREATE_NORMAL_TABLE_RSP 12
#define TSDB_MSG_TYPE_DNODE_CREATE_STREAM_TABLE     13
#define TSDB_MSG_TYPE_DNODE_CREATE_STREAM_TABLE_RSP 14
#define TSDB_MSG_TYPE_DNODE_CREATE_SUPER_TABLE      15
#define TSDB_MSG_TYPE_DNODE_CREATE_SUPER_TABLE_RSP  16
#define TSDB_MSG_TYPE_DNODE_REMOVE_CHILD_TABLE      17
#define TSDB_MSG_TYPE_DNODE_REMOVE_CHILD_TABLE_RSP  18
#define TSDB_MSG_TYPE_DNODE_REMOVE_NORMAL_TABLE     19
#define TSDB_MSG_TYPE_DNODE_REMOVE_NORMAL_TABLE_RSP 20
#define TSDB_MSG_TYPE_DNODE_REMOVE_STREAM_TABLE     21
#define TSDB_MSG_TYPE_DNODE_REMOVE_STREAM_TABLE_RSP 22
#define TSDB_MSG_TYPE_DNODE_REMOVE_SUPER_TABLE      23
#define TSDB_MSG_TYPE_DNODE_REMOVE_SUPER_TABLE_RSP  24
#define TSDB_MSG_TYPE_DNODE_ALTER_CHILD_TABLE       25
#define TSDB_MSG_TYPE_DNODE_ALTER_CHILD_TABLE_RSP   26
#define TSDB_MSG_TYPE_DNODE_ALTER_NORMAL_TABLE      27
#define TSDB_MSG_TYPE_DNODE_ALTER_NORMAL_TABLE_RSP  28
#define TSDB_MSG_TYPE_DNODE_ALTER_STREAM_TABLE      29
#define TSDB_MSG_TYPE_DNODE_ALTER_STREAM_TABLE_RSP  30
#define TSDB_MSG_TYPE_DNODE_ALTER_SUPER_TABLE       31
#define TSDB_MSG_TYPE_DNODE_ALTER_SUPER_TABLE_RSP   32
#define TSDB_MSG_TYPE_DNODE_VPEERS                  33
#define TSDB_MSG_TYPE_DNODE_VPEERS_RSP              34
#define TSDB_MSG_TYPE_DNODE_FREE_VNODE              35
#define TSDB_MSG_TYPE_DNODE_FREE_VNODE_RSP          36
#define TSDB_MSG_TYPE_DNODE_CFG                     37
#define TSDB_MSG_TYPE_DNODE_CFG_RSP                 38
#define TSDB_MSG_TYPE_DNODE_ALTER_STREAM            39
#define TSDB_MSG_TYPE_DNODE_ALTER_STREAM_RSP        40

#define TSDB_MSG_TYPE_SDB_SYNC             41
#define TSDB_MSG_TYPE_SDB_SYNC_RSP         42
#define TSDB_MSG_TYPE_SDB_FORWARD          43
#define TSDB_MSG_TYPE_SDB_FORWARD_RSP      44
#define TSDB_MSG_TYPE_CONNECT              51
#define TSDB_MSG_TYPE_CONNECT_RSP          52
#define TSDB_MSG_TYPE_CREATE_ACCT          53
#define TSDB_MSG_TYPE_CREATE_ACCT_RSP      54
#define TSDB_MSG_TYPE_ALTER_ACCT           55
#define TSDB_MSG_TYPE_ALTER_ACCT_RSP       56
#define TSDB_MSG_TYPE_DROP_ACCT            57
#define TSDB_MSG_TYPE_DROP_ACCT_RSP        58
#define TSDB_MSG_TYPE_CREATE_USER          59
#define TSDB_MSG_TYPE_CREATE_USER_RSP      60
#define TSDB_MSG_TYPE_ALTER_USER           61
#define TSDB_MSG_TYPE_ALTER_USER_RSP       62
#define TSDB_MSG_TYPE_DROP_USER            63
#define TSDB_MSG_TYPE_DROP_USER_RSP        64
#define TSDB_MSG_TYPE_CREATE_MNODE         65
#define TSDB_MSG_TYPE_CREATE_MNODE_RSP     66
#define TSDB_MSG_TYPE_DROP_MNODE           67
#define TSDB_MSG_TYPE_DROP_MNODE_RSP       68
#define TSDB_MSG_TYPE_CREATE_DNODE         69
#define TSDB_MSG_TYPE_CREATE_DNODE_RSP     70
#define TSDB_MSG_TYPE_DROP_DNODE           71
#define TSDB_MSG_TYPE_DROP_DNODE_RSP       72
#define TSDB_MSG_TYPE_ALTER_DNODE          73
#define TSDB_MSG_TYPE_ALTER_DNODE_RSP      74
#define TSDB_MSG_TYPE_CREATE_DB            75
#define TSDB_MSG_TYPE_CREATE_DB_RSP        76
#define TSDB_MSG_TYPE_DROP_DB              77
#define TSDB_MSG_TYPE_DROP_DB_RSP          78
#define TSDB_MSG_TYPE_USE_DB               79
#define TSDB_MSG_TYPE_USE_DB_RSP           80
#define TSDB_MSG_TYPE_ALTER_DB             81
#define TSDB_MSG_TYPE_ALTER_DB_RSP         82
#define TSDB_MSG_TYPE_CREATE_TABLE         83
#define TSDB_MSG_TYPE_CREATE_TABLE_RSP     84
#define TSDB_MSG_TYPE_DROP_TABLE           85
#define TSDB_MSG_TYPE_DROP_TABLE_RSP       86
#define TSDB_MSG_TYPE_ALTER_TABLE          87
#define TSDB_MSG_TYPE_ALTER_TABLE_RSP      88
#define TSDB_MSG_TYPE_VNODE_CFG            89
#define TSDB_MSG_TYPE_VNODE_CFG_RSP        90
#define TSDB_MSG_TYPE_TABLE_CFG            91
#define TSDB_MSG_TYPE_TABLE_CFG_RSP        92
#define TSDB_MSG_TYPE_TABLE_META           93
#define TSDB_MSG_TYPE_TABLE_META_RSP       94
#define TSDB_MSG_TYPE_STABLE_META          95
#define TSDB_MSG_TYPE_STABLE_META_RSP      96
#define TSDB_MSG_TYPE_MULTI_TABLE_META     97
#define TSDB_MSG_TYPE_MULTI_TABLE_META_RSP 98
#define TSDB_MSG_TYPE_ALTER_STREAM         99
#define TSDB_MSG_TYPE_ALTER_STREAM_RSP     100
#define TSDB_MSG_TYPE_SHOW                 101
#define TSDB_MSG_TYPE_SHOW_RSP             102
#define TSDB_MSG_TYPE_CFG_MNODE            103
#define TSDB_MSG_TYPE_CFG_MNODE_RSP        104
#define TSDB_MSG_TYPE_KILL_QUERY           105
#define TSDB_MSG_TYPE_KILL_QUERY_RSP       106
#define TSDB_MSG_TYPE_KILL_STREAM          107
#define TSDB_MSG_TYPE_KILL_STREAM_RSP      108
#define TSDB_MSG_TYPE_KILL_CONNECTION      109
#define TSDB_MSG_TYPE_KILL_CONNECTION_RSP  110
#define TSDB_MSG_TYPE_HEARTBEAT            111
#define TSDB_MSG_TYPE_HEARTBEAT_RSP        112
#define TSDB_MSG_TYPE_STATUS               113
#define TSDB_MSG_TYPE_STATUS_RSP           114
#define TSDB_MSG_TYPE_GRANT                115
#define TSDB_MSG_TYPE_GRANT_RSP            116
#define TSDB_MSG_TYPE_MAX                  117

// IE type
#define TSDB_IE_TYPE_SEC               1
#define TSDB_IE_TYPE_META              2
#define TSDB_IE_TYPE_MGMT_IP           3
#define TSDB_IE_TYPE_DNODE_CFG         4
#define TSDB_IE_TYPE_NEW_VERSION       5
#define TSDB_IE_TYPE_DNODE_EXT         6
#define TSDB_IE_TYPE_DNODE_STATE       7

enum _mgmt_table {
  TSDB_MGMT_TABLE_ACCT,
  TSDB_MGMT_TABLE_USER,
  TSDB_MGMT_TABLE_DB,
  TSDB_MGMT_TABLE_TABLE,
  TSDB_MGMT_TABLE_DNODE,
  TSDB_MGMT_TABLE_MNODE,
  TSDB_MGMT_TABLE_VGROUP,
  TSDB_MGMT_TABLE_METRIC,
  TSDB_MGMT_TABLE_MODULE,
  TSDB_MGMT_TABLE_QUERIES,
  TSDB_MGMT_TABLE_STREAMS,
  TSDB_MGMT_TABLE_CONFIGS,
  TSDB_MGMT_TABLE_CONNS,
  TSDB_MGMT_TABLE_SCORES,
  TSDB_MGMT_TABLE_GRANTS,
  TSDB_MGMT_TABLE_VNODES,
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

typedef enum {
  TSDB_TABLE_TYPE_SUPER_TABLE        = 0,  // super table
  TSDB_TABLE_TYPE_CHILD_TABLE        = 1,  // table created from super table
  TSDB_TABLE_TYPE_NORMAL_TABLE       = 2,  // ordinary table
  TSDB_TABLE_TYPE_STREAM_TABLE       = 3,  // table created from stream computing
  TSDB_TABLE_TYPE_MAX                = 4
} ETableType;


#define TSDB_VN_READ_ACCCESS  ((char)0x1)
#define TSDB_VN_WRITE_ACCCESS ((char)0x2)
#define TSDB_VN_ALL_ACCCESS (TSDB_VN_READ_ACCCESS | TSDB_VN_WRITE_ACCCESS)

#define TSDB_COL_NORMAL                0x0U
#define TSDB_COL_TAG                   0x1U
#define TSDB_COL_JOIN                  0x2U

extern char *taosMsg[];

#pragma pack(push, 1)

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
  uint32_t destIp;
  char     meterId[TSDB_UNI_LEN];
  uint16_t port;  // for UDP only
  char     empty[1];
  uint8_t  msgType;
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
  int32_t  vnode;
  int32_t  sid;
  int32_t  sversion;
  uint64_t uid;
  short    numOfRows;
  char     payLoad[];
} SShellSubmitBlock;

typedef struct {
  int8_t  import;
  int8_t  reserved[3];
  int32_t numOfSid; /* total number of sid */
  char    blks[];   /* numOfSid blocks, each blocks for one meter */
} SShellSubmitMsg;

typedef struct {
  int32_t index; // index of failed block in submit blocks
  int32_t vnode; // vnode index of failed block
  int32_t sid;   // table index of failed block
  int32_t code;  // errorcode while write data to vnode, such as not created, dropped, no space, invalid table
} SShellSubmitRspBlock;

typedef struct {
  int32_t code;         // 0-success, > 0 error code
  int32_t numOfRows;    // number of records the client is trying to write
  int32_t affectedRows; // number of records actually written
  int32_t failedRows;   // number of failed records (exclude duplicate records)
  int32_t numOfFailedBlocks;
  SShellSubmitRspBlock *failedBlocks;
} SShellSubmitRspMsg;

typedef struct SSchema {
  uint8_t  type;
  char  name[TSDB_COL_NAME_LEN];
  short colId;
  short bytes;
} SSchema;

typedef struct SMColumn {
  int8_t  type;
  int16_t colId;
  int16_t bytes;
} SMColumn;

typedef struct {
  int32_t size;
  int8_t* data;
} SVariableMsg;

typedef struct {
  short    vnode;
  int32_t  sid;
  uint64_t uid;
  char     spi;
  char     encrypt;
  char     meterId[TSDB_TABLE_ID_LEN];
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
  int32_t  vnode;
  int32_t  sid;
  uint64_t uid;
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  char     superTableId[TSDB_TABLE_ID_LEN + 1];
  uint64_t createdTime;
  int32_t  sversion;
  int16_t  numOfColumns;
  int16_t  numOfTags;
  int32_t  tagDataLen;
  int8_t   data[];
} SCreateChildTableMsg;

typedef struct {
  int32_t  vnode;
  int32_t  sid;
  uint64_t uid;
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  uint64_t createdTime;
  int32_t  sversion;
  int16_t  numOfColumns;
  int8_t   data[];
} SCreateNormalTableMsg;

typedef struct {
  int32_t  vnode;
  int32_t  sid;
  uint64_t uid;
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  uint64_t createdTime;
  int32_t  sversion;
  int16_t  numOfColumns;
  int32_t  sqlLen;
  int8_t   data[];
} SCreateStreamTableMsg;


typedef struct {
  char  db[TSDB_TABLE_ID_LEN];
  uint8_t ignoreNotExists;
} SDropDbMsg, SUseDbMsg;

typedef struct {
  char user[TSDB_USER_LEN];
} SDropUserMsg, SDropAcctMsg;

typedef struct {
  char db[TSDB_DB_NAME_LEN];
} SShowTableMsg;

typedef struct {
  char meterId[TSDB_TABLE_ID_LEN];
  char igExists;

  short numOfTags;

  short numOfColumns;
  short sqlLen;  // the length of SQL, it starts after schema , sql is a
  // null-terminated string
  char reserved[16];

  SSchema schema[];
} SCreateTableMsg;

typedef struct {
  char meterId[TSDB_TABLE_ID_LEN];
  char igNotExists;
} SDropTableMsg;

typedef struct {
  char    meterId[TSDB_TABLE_ID_LEN];
  short   type; /* operation type   */
  char    tagVal[TSDB_MAX_BYTES_PER_ROW];
  short   numOfCols; /* number of schema */
  SSchema schema[];
} SAlterTableMsg;

typedef struct {
  char clientVersion[TSDB_VERSION_LEN];
  char db[TSDB_TABLE_ID_LEN];
} SConnectMsg;

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

typedef struct {
  char db[TSDB_TABLE_ID_LEN];
} SMgmtHead;

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
  char     meterId[TSDB_TABLE_ID_LEN];
} SRemoveMeterMsg;

typedef struct {
  short vnode;
} SFreeVnodeMsg;

typedef struct SColIndexEx {
  int16_t colId;
  /*
   * colIdx is the index of column in latest schema of table
   * it is available in the client side. Also used to determine
   * whether current meter schema is up-to-date.
   *
   * colIdxInBuf is used to denote the index of column in pQuery->colList,
   * this value is invalid in client side, as well as in cache block of vnode either.
   */
  int16_t  colIdx;
  int16_t  colIdxInBuf;
  uint16_t flag;         // denote if it is a tag or not
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
  int16_t            interResBytes;
} SSqlFunctionExpr;

typedef struct SColumnFilterInfo {
  int16_t lowerRelOptr;
  int16_t upperRelOptr;
  int16_t filterOnBinary; /* denote if current column is binary   */

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
} SColumnFilterInfo;

/*
 * for client side struct, we only need the column id, type, bytes are not necessary
 * But for data in vnode side, we need all the following information.
 */
typedef struct SColumnInfo {
  int16_t            colId;
  int16_t            type;
  int16_t            bytes;
  int16_t            numOfFilters;
  SColumnFilterInfo *filters;
} SColumnInfo;

/*
 * enable vnode to understand how to group several tables with different tag;
 */
typedef struct SMeterSidExtInfo {
  int32_t sid;
  int64_t uid;
  TSKEY   key;   // key for subscription
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

  uint64_t uid;
  TSKEY    skey;
  TSKEY    ekey;

  int16_t order;
  int16_t orderColId;

  int16_t numOfCols;         // the number of columns will be load from vnode
  char    intervalTimeUnit;  // time interval type, for revisement of interval(1d)

  int64_t nAggTimeInterval;  // time interval for aggregation, in million second
  int64_t slidingTime;       // value for sliding window
  
  // tag schema, used to parse tag information in pSidExtInfo
  uint64_t pTagSchema;

  int16_t numOfTagsCols;  // required number of tags
  int16_t tagLength;      // tag length in current query

  int16_t  numOfGroupCols;  // num of group by columns
  int16_t  orderByIdx;
  int16_t  orderType;  // used in group by xx order by xxx
  uint64_t groupbyTagIds;

  int64_t limit;
  int64_t offset;

  int16_t queryType;        // denote another query process
  int16_t numOfOutputCols;  // final output columns numbers

  int16_t  interpoType;  // interpolate type
  uint64_t defaultVal;   // default value array list

  int32_t colNameLen;
  int64_t colNameList;

  int64_t pSqlFuncExprs;

  int32_t     tsOffset;       // offset value in current msg body, NOTE: ts list is compressed
  int32_t     tsLen;          // total length of ts comp block
  int32_t     tsNumOfBlocks;  // ts comp block numbers
  int32_t     tsOrder;        // ts comp block order
  SColumnInfo colList[];
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
  uint16_t free;
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
  uint8_t  syncStatus;
  uint8_t  reserved[15];
} SVnodeLoad;

typedef struct {
  uint32_t vnode;
  char     accessState;
} SVnodeAccess;

// NOTE: sizeof(SVnodeCfg) < TSDB_FILE_HEADER_LEN/4
typedef struct {
  char     acct[TSDB_USER_LEN];
  /*
   * the message is too large, so it may will overwrite the cfg information in meterobj.v*
   * recover to origin codes
   */
  //char     db[TSDB_TABLE_ID_LEN+2]; // 8bytes align
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
  uint8_t precision;   // time resolution

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
  uint16_t   numOfTotalVnodes;  // from config file
  uint16_t   unused;
  float      diskAvailable;  // GB
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
  uint32_t destIp;
  char     meterId[TSDB_UNI_LEN];
  char     empty[3];
  uint8_t  msgType;
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
  int32_t dnode;  //the ID of dnode
  int32_t vnode;  //the index of vnode
} SVPeerDesc;

typedef struct {
  int32_t numOfVPeers;
  SVPeerDesc vpeerDesc[];
} SVpeerDescArray;

typedef struct {
  int32_t    vnode;
  SVnodeCfg  cfg;
  SVPeerDesc vpeerDesc[];
} SVPeersMsg;

typedef struct {
  char  meterId[TSDB_TABLE_ID_LEN];
  short createFlag;
  char  tags[];
} SMeterInfoMsg;

typedef struct {
  int32_t numOfMeters;
  char    meterId[];
} SMultiMeterInfoMsg;

typedef struct {
  int16_t elemLen;

  char    meterId[TSDB_TABLE_ID_LEN];
  int16_t orderIndex;
  int16_t orderType;  // used in group by xx order by xxx

  int16_t rel;  // denotes the relation between condition and table list

  int32_t tableCond;  // offset value of table name condition
  int32_t tableCondLen;

  int32_t cond;  // offset of column query condition
  int32_t condLen;

  int16_t tagCols[TSDB_MAX_TAGS + 1];  // required tag columns, plus one is for table name
  int16_t numOfTags;                   // required number of tags

  int16_t numOfGroupCols;  // num of group by columns
  int32_t groupbyTagColumnList;
} SMetricMetaElemMsg;

typedef struct {
  int32_t numOfMeters;
  int32_t join;
  int32_t joinCondLen;  // for join condition
  int32_t metaElem[TSDB_MAX_JOIN_TABLE_NUM];
} SSuperTableMetaMsg;

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
  uint8_t numOfTags : 6;
  uint8_t precision : 2;
  uint8_t tableType : 4;
  uint8_t index : 4;  // used locally

  int16_t numOfColumns;

  int16_t rowSize;  // used locally, calculated in client
  int16_t sversion;

  SVPeerDesc vpeerDesc[TSDB_VNODES_SUPPORT];

  int32_t  sid;
  int32_t  vgid;
  uint64_t uid;
} SMeterMeta;

typedef struct SMultiMeterMeta {
  char       meterId[TSDB_TABLE_ID_LEN];  // note: This field must be at the front
  SMeterMeta meta;
} SMultiMeterMeta;

typedef struct {
  char name[TSDB_TABLE_ID_LEN];
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

typedef struct {
  char ip[20];
} SCreateMnodeMsg, SDropMnodeMsg, SCreateDnodeMsg, SDropDnodeMsg;

typedef struct {
  uint64_t   qhandle;
  SMeterMeta meterMeta;
} SShowRspMsg;

typedef struct {
  int32_t vnode;
  int32_t sid;
} SMeterCfgMsg;

typedef struct {
  int32_t vnode;
} SVpeerCfgMsg;

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

#pragma pack(pop)

#ifdef __cplusplus
}
#endif

#endif

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

#include <stdbool.h>
#include <stdint.h>

#include "taosdef.h"
#include "taoserror.h"
#include "trpc.h"

// message type
#define TSDB_MSG_TYPE_REG               1
#define TSDB_MSG_TYPE_REG_RSP           2
#define TSDB_MSG_TYPE_SUBMIT            3
#define TSDB_MSG_TYPE_SUBMIT_RSP        4
#define TSDB_MSG_TYPE_QUERY             5
#define TSDB_MSG_TYPE_QUERY_RSP         6
#define TSDB_MSG_TYPE_RETRIEVE          7
#define TSDB_MSG_TYPE_RETRIEVE_RSP      8

// message from mnode to dnode
#define TSDB_MSG_TYPE_MD_CREATE_TABLE     9
#define TSDB_MSG_TYPE_MD_CREATE_TABLE_RSP 10
#define TSDB_MSG_TYPE_MD_DROP_TABLE       11
#define TSDB_MSG_TYPE_MD_DROP_TABLE_RSP   12
#define TSDB_MSG_TYPE_MD_ALTER_TABLE      13
#define TSDB_MSG_TYPE_MD_ALTER_TABLE_RSP  14
#define TSDB_MSG_TYPE_MD_CREATE_VNODE     15
#define TSDB_MSG_TYPE_MD_CREATE_VNODE_RSP 16
#define TSDB_MSG_TYPE_MD_DROP_VNODE       17
#define TSDB_MSG_TYPE_MD_DROP_VNODE_RSP   18
#define TSDB_MSG_TYPE_MD_DROP_STABLE      19
#define TSDB_MSG_TYPE_MD_DROP_STABLE_RSP  20
#define TSDB_MSG_TYPE_MD_ALTER_STREAM     21
#define TSDB_MSG_TYPE_MD_ALTER_STREAM_RSP 22
#define TSDB_MSG_TYPE_MD_CONFIG_DNODE     23
#define TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP 24

// message from client to mnode
#define TSDB_MSG_TYPE_CM_CONNECT          31
#define TSDB_MSG_TYPE_CM_CONNECT_RSP      32
#define TSDB_MSG_TYPE_CM_CREATE_ACCT      33
#define TSDB_MSG_TYPE_CM_CREATE_ACCT_RSP  34
#define TSDB_MSG_TYPE_CM_ALTER_ACCT       35
#define TSDB_MSG_TYPE_CM_ALTER_ACCT_RSP   36
#define TSDB_MSG_TYPE_CM_DROP_ACCT        37
#define TSDB_MSG_TYPE_CM_DROP_ACCT_RSP    38
#define TSDB_MSG_TYPE_CM_CREATE_USER      39
#define TSDB_MSG_TYPE_CM_CREATE_USER_RSP  40
#define TSDB_MSG_TYPE_CM_ALTER_USER       41
#define TSDB_MSG_TYPE_CM_ALTER_USER_RSP   42
#define TSDB_MSG_TYPE_CM_DROP_USER        43
#define TSDB_MSG_TYPE_CM_DROP_USER_RSP    44
#define TSDB_MSG_TYPE_CM_CREATE_DNODE     45
#define TSDB_MSG_TYPE_CM_CREATE_DNODE_RSP 46
#define TSDB_MSG_TYPE_CM_DROP_DNODE       47
#define TSDB_MSG_TYPE_CM_DROP_DNODE_RSP   48
#define TSDB_MSG_TYPE_CM_CONFIG_DNODE     TSDB_MSG_TYPE_MD_CONFIG_DNODE
#define TSDB_MSG_TYPE_CM_CONFIG_DNODE_RSP TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP
#define TSDB_MSG_TYPE_CM_CREATE_DB        49
#define TSDB_MSG_TYPE_CM_CREATE_DB_RSP    50
#define TSDB_MSG_TYPE_CM_DROP_DB          51
#define TSDB_MSG_TYPE_CM_DROP_DB_RSP      52
#define TSDB_MSG_TYPE_CM_USE_DB           53
#define TSDB_MSG_TYPE_CM_USE_DB_RSP       54
#define TSDB_MSG_TYPE_CM_ALTER_DB         55
#define TSDB_MSG_TYPE_CM_ALTER_DB_RSP     56
#define TSDB_MSG_TYPE_CM_CREATE_TABLE     57
#define TSDB_MSG_TYPE_CM_CREATE_TABLE_RSP 58
#define TSDB_MSG_TYPE_CM_DROP_TABLE       59
#define TSDB_MSG_TYPE_CM_DROP_TABLE_RSP   60
#define TSDB_MSG_TYPE_CM_ALTER_TABLE      61
#define TSDB_MSG_TYPE_CM_ALTER_TABLE_RSP  62
#define TSDB_MSG_TYPE_CM_TABLE_META       63
#define TSDB_MSG_TYPE_CM_TABLE_META_RSP   64
#define TSDB_MSG_TYPE_CM_STABLE_VGROUP    65
#define TSDB_MSG_TYPE_CM_STABLE_VGROUP_RSP 66
#define TSDB_MSG_TYPE_CM_TABLES_META      67
#define TSDB_MSG_TYPE_CM_TABLES_META_RSP  68
#define TSDB_MSG_TYPE_CM_ALTER_STREAM     69
#define TSDB_MSG_TYPE_CM_ALTER_STREAM_RSP 70
#define TSDB_MSG_TYPE_CM_SHOW             71
#define TSDB_MSG_TYPE_CM_SHOW_RSP         72
#define TSDB_MSG_TYPE_CM_KILL_QUERY       73
#define TSDB_MSG_TYPE_CM_KILL_QUERY_RSP   74
#define TSDB_MSG_TYPE_CM_KILL_STREAM      75
#define TSDB_MSG_TYPE_CM_KILL_STREAM_RSP 76
#define TSDB_MSG_TYPE_CM_KILL_CONN 77
#define TSDB_MSG_TYPE_CM_KILL_CONN_RSP 78
#define TSDB_MSG_TYPE_CM_HEARTBEAT 79
#define TSDB_MSG_TYPE_CM_HEARTBEAT_RSP 80

// message from dnode to mnode
#define TSDB_MSG_TYPE_DM_CONFIG_TABLE 91
#define TSDB_MSG_TYPE_DM_CONFIG_TABLE_RSP 92
#define TSDB_MSG_TYPE_DM_CONFIG_VNODE 93
#define TSDB_MSG_TYPE_DM_CONFIG_VNODE_RSP 94
#define TSDB_MSG_TYPE_DM_STATUS 95
#define TSDB_MSG_TYPE_DM_STATUS_RSP 96
#define TSDB_MSG_TYPE_DM_GRANT 97
#define TSDB_MSG_TYPE_DM_GRANT_RSP 98

#define TSDB_MSG_TYPE_SDB_SYNC 101
#define TSDB_MSG_TYPE_SDB_SYNC_RSP 102
#define TSDB_MSG_TYPE_SDB_FORWARD 103
#define TSDB_MSG_TYPE_SDB_FORWARD_RSP 104

#define TSDB_MSG_TYPE_MAX 105

// IE type
#define TSDB_IE_TYPE_SEC 1
#define TSDB_IE_TYPE_META 2
#define TSDB_IE_TYPE_MGMT_IP 3
#define TSDB_IE_TYPE_DNODE_CFG 4
#define TSDB_IE_TYPE_NEW_VERSION 5
#define TSDB_IE_TYPE_DNODE_EXT 6
#define TSDB_IE_TYPE_DNODE_STATE 7

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

#define TSDB_ALTER_TABLE_ADD_TAG_COLUMN 1
#define TSDB_ALTER_TABLE_DROP_TAG_COLUMN 2
#define TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN 3
#define TSDB_ALTER_TABLE_UPDATE_TAG_VAL 4

#define TSDB_ALTER_TABLE_ADD_COLUMN 5
#define TSDB_ALTER_TABLE_DROP_COLUMN 6

#define TSDB_INTERPO_NONE 0
#define TSDB_INTERPO_NULL 1
#define TSDB_INTERPO_SET_VALUE 2
#define TSDB_INTERPO_LINEAR 3
#define TSDB_INTERPO_PREV 4

#define TSDB_ALTER_USER_PASSWD 0x1
#define TSDB_ALTER_USER_PRIVILEGES 0x2

#define TSDB_KILL_MSG_LEN 30

#define TSDB_VN_READ_ACCCESS ((char)0x1)
#define TSDB_VN_WRITE_ACCCESS ((char)0x2)
#define TSDB_VN_ALL_ACCCESS (TSDB_VN_READ_ACCCESS | TSDB_VN_WRITE_ACCCESS)

#define TSDB_COL_NORMAL 0x0u
#define TSDB_COL_TAG 0x1u
#define TSDB_COL_JOIN 0x2u

extern char *taosMsg[];

#pragma pack(push, 1)

typedef struct {
  uint32_t ip;
  uint16_t port;
} SIpAddr;

typedef struct {
  int32_t numOfVnodes;
} SMsgDesc;

typedef struct SMsgHead {
  int32_t contLen;
  int32_t vgId;
} SMsgHead;

// Submit message for one table
typedef struct SSubmitBlk {
  int64_t uid;        // table unique id
  int32_t tid;        // table id
  int32_t padding;    // TODO just for padding here
  int32_t sversion;   // data schema version
  int32_t len;        // data part length, not including the SSubmitBlk head
  int16_t numOfRows;  // total number of rows in current submit block
  char    data[];
} SSubmitBlk;

// Submit message for this TSDB
typedef struct SSubmitMsg {
  SMsgHead   header;
  int32_t    length;
  int32_t    compressed : 2;
  int32_t    numOfBlocks : 30;
  SSubmitBlk blocks[];
} SSubmitMsg;

typedef struct {
  int32_t index;  // index of failed block in submit blocks
  int32_t vnode;  // vnode index of failed block
  int32_t sid;    // table index of failed block
  int32_t code;   // errorcode while write data to vnode, such as not created, dropped, no space, invalid table
} SShellSubmitRspBlock;

typedef struct {
  int32_t              code;          // 0-success, > 0 error code
  int32_t              numOfRows;     // number of records the client is trying to write
  int32_t              affectedRows;  // number of records actually written
  int32_t              failedRows;    // number of failed records (exclude duplicate records)
  int32_t              numOfFailedBlocks;
  SShellSubmitRspBlock failedBlocks[];
} SShellSubmitRspMsg;

typedef struct SSchema {
  uint8_t type;
  char    name[TSDB_COL_NAME_LEN];
  int16_t colId;
  int16_t bytes;
} SSchema;

typedef struct {
  int32_t  contLen;
  int32_t  vgId;
  int8_t   tableType;
  int16_t  numOfColumns;
  int16_t  numOfTags;
  int32_t  sid;
  int32_t  sversion;
  int32_t  tagDataLen;
  int32_t  sqlDataLen;
  uint64_t uid;
  uint64_t superTableUid;
  uint64_t createdTime;
  char     tableId[TSDB_TABLE_ID_LEN];
  char     superTableId[TSDB_TABLE_ID_LEN];
  char     data[];
} SMDCreateTableMsg;

typedef struct {
  char    tableId[TSDB_TABLE_ID_LEN];
  char    db[TSDB_DB_NAME_LEN];
  int8_t  igExists;
  int16_t numOfTags;
  int16_t numOfColumns;
  int16_t sqlLen;  // the length of SQL, it starts after schema , sql is a null-terminated string
  int32_t contLen;
  int8_t  reserved[16];
  char    schema[];
} SCMCreateTableMsg;

typedef struct {
  char   tableId[TSDB_TABLE_ID_LEN];
  int8_t igNotExists;
} SCMDropTableMsg;

typedef struct {
  char    tableId[TSDB_TABLE_ID_LEN];
  char    db[TSDB_DB_NAME_LEN];
  int16_t type; /* operation type   */
  char    tagVal[TSDB_MAX_BYTES_PER_ROW];
  int8_t  numOfCols; /* number of schema */
  SSchema schema[];
} SCMAlterTableMsg;

typedef struct {
  char clientVersion[TSDB_VERSION_LEN];
  char msgVersion[TSDB_VERSION_LEN];
  char db[TSDB_TABLE_ID_LEN + 1];
} SCMConnectMsg;

typedef struct {
  char      acctId[TSDB_ACCT_LEN + 1];
  char      serverVersion[TSDB_VERSION_LEN];
  int8_t    writeAuth;
  int8_t    superAuth;
  SRpcIpSet ipList;
} SCMConnectRsp;

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
  int8_t  accessState;  // Configured only by command
} SAcctCfg;

typedef struct {
  char     user[TSDB_USER_LEN + 1];
  char     pass[TSDB_KEY_LEN + 1];
  SAcctCfg cfg;
} SCMCreateAcctMsg, SCMAlterAcctMsg;

typedef struct {
  char user[TSDB_USER_LEN + 1];
} SCMDropUserMsg, SCMDropAcctMsg;

typedef struct {
  char   user[TSDB_USER_LEN + 1];
  char   pass[TSDB_KEY_LEN + 1];
  int8_t privilege;
  int8_t flag;
} SCMCreateUserMsg, SCMAlterUserMsg;

typedef struct {
  char db[TSDB_TABLE_ID_LEN + 1];
} SMgmtHead;

typedef struct {
  int32_t  contLen;
  int32_t  vgId;
  int32_t  sid;
  uint64_t uid;
  char     tableId[TSDB_TABLE_ID_LEN + 1];
} SMDDropTableMsg;

typedef struct {
  int32_t contLen;
  int32_t vgId;
  int64_t uid;
  char    tableId[TSDB_TABLE_ID_LEN + 1];
} SMDDropSTableMsg;

typedef struct {
  int32_t vgId;
} SMDDropVnodeMsg;

typedef struct SColIndex {
  int16_t colId;
  /*
   * colIdx is the index of column in latest schema of table
   * it is available in the client side. Also used to determine
   * whether current table schema is up-to-date.
   *
   * colIdxInBuf is used to denote the index of column in pQuery->colList,
   * this value is invalid in client side, as well as in cache block of vnode either.
   */
  int16_t  colIndex;
  uint16_t flag;  // denote if it is a tag or not
  char     name[TSDB_COL_NAME_LEN];
} SColIndex;

/* sql function msg, to describe the message to vnode about sql function
 * operations in select clause */
typedef struct SSqlFuncMsg {
  int16_t functionId;
  int16_t numOfParams;

  SColIndex colInfo;
  struct ArgElem {
    int16_t argType;
    int16_t argBytes;
    union {
      double  d;
      int64_t i64;
      char *  pz;
    } argValue;
  } arg[3];
} SSqlFuncMsg;

typedef struct SExprInfo {
  struct tExprNode *pBinExpr;    /*  for binary expression */
  int32_t           numOfCols;   /*  binary expression involves the readed number of columns*/
  SColIndex *     pReqColumns;   /*  source column list */
} SExprInfo;

typedef struct SArithExprInfo {
  SSqlFuncMsg pBase;
  SExprInfo   binExprInfo;
  int16_t     bytes;
  int16_t     type;
  int16_t     interResBytes;
} SArithExprInfo;

typedef struct SColumnFilterInfo {
  int16_t lowerRelOptr;
  int16_t upperRelOptr;
  int16_t filterstr;   // denote if current column is char(binary/nchar)

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

typedef struct STableIdInfo {
  int32_t sid;
  int64_t uid;
  TSKEY   key;  // last accessed ts, for subscription
} STableIdInfo;

typedef struct STimeWindow {
  TSKEY skey;
  TSKEY ekey;
} STimeWindow;

/*
 * the outputCols is equalled to or larger than numOfCols
 * e.g., select min(colName), max(colName), avg(colName) from table
 * the outputCols will be 3 while the numOfCols is 1.
 */
typedef struct {
  SMsgHead    head;
  STimeWindow window;
  int32_t     numOfTables;
  int16_t     order;
  int16_t     orderColId;
  int16_t     numOfCols;        // the number of columns will be load from vnode
  int64_t     intervalTime;     // time interval for aggregation, in million second
  int64_t     intervalOffset;   // start offset for interval query
  int64_t     slidingTime;      // value for sliding window
  char        slidingTimeUnit;  // time interval type, for revisement of interval(1d)
  uint16_t    tagCondLen;       // tag length in current query
  int16_t     numOfGroupCols;   // num of group by columns
  int16_t     orderByIdx;
  int16_t     orderType;        // used in group by xx order by xxx
  int64_t     limit;
  int64_t     offset;
  uint16_t    queryType;        // denote another query process
  int16_t     numOfOutput;  // final output columns numbers
  int16_t     interpoType;      // interpolate type
  uint64_t    defaultVal;       // default value array list

  int32_t     colNameLen;
  int64_t     colNameList;
  int32_t     tsOffset;       // offset value in current msg body, NOTE: ts list is compressed
  int32_t     tsLen;          // total length of ts comp block
  int32_t     tsNumOfBlocks;  // ts comp block numbers
  int32_t     tsOrder;        // ts comp block order
  SColumnInfo colList[];
} SQueryTableMsg;

typedef struct {
  int32_t  code;
  uint64_t qhandle;
} SQueryTableRsp;

typedef struct {
  SMsgHead header;
  uint64_t qhandle;
  uint16_t free;
} SRetrieveTableMsg;

typedef struct SRetrieveTableRsp {
  int32_t numOfRows;
  int8_t  completed;  // all results are returned to client
  int16_t precision;
  int64_t offset;  // updated offset value for multi-vnode projection query
  int64_t useconds;
  char    data[];
} SRetrieveTableRsp;

typedef struct {
  int32_t vgId;
  int64_t totalStorage;
  int64_t compStorage;
  int64_t pointsWritten;
  uint8_t status;
  uint8_t role;
  uint8_t replica;
  uint8_t reserved[5];
} SVnodeLoad;

typedef struct {
  char     acct[TSDB_USER_LEN + 1];
  char     db[TSDB_DB_NAME_LEN + 1];
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
  int16_t blocksPerTable;
  int8_t  compression;
  int8_t  commitLog;
  int8_t  replications;
  int8_t  repStrategy;
  int8_t  loadLatest;  // load into mem or not
  uint8_t precision;   // time resolution
  int8_t  reserved[16];
} SDbCfg, SCMCreateDbMsg, SCMAlterDbMsg;

typedef struct {
  char    db[TSDB_TABLE_ID_LEN + 1];
  uint8_t ignoreNotExists;
} SCMDropDbMsg, SCMUseDbMsg;

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
  int32_t  vgId;
  int8_t   accessState;
} SDMVgroupAccess;

typedef struct {
  int32_t  dnodeId;
  uint32_t moduleStatus;
  uint32_t numOfVnodes;
} SDMDnodeCfg;

typedef struct {
  int32_t   nodeId;
  uint32_t  nodeIp;
  uint16_t  nodePort;
  uint16_t  syncPort;
  char      nodeName[TSDB_NODE_NAME_LEN + 1];
} SDMMnodeInfo;

typedef struct {
  int8_t       inUse;
  int8_t       nodeNum;
  SDMMnodeInfo nodeInfos[TSDB_MAX_MPEERS];
} SDMMnodeInfos;

typedef struct {
  uint32_t   version;
  int32_t    dnodeId;
  char       dnodeName[TSDB_NODE_NAME_LEN + 1];
  uint32_t   privateIp;
  uint32_t   publicIp;
  uint32_t   moduleStatus;
  uint32_t   lastReboot;        // time stamp for last reboot
  uint16_t   numOfTotalVnodes;  // from config file
  uint16_t   openVnodes;
  uint16_t   numOfCores;
  float      diskAvailable;  // GB
  uint8_t    alternativeRole;
  uint8_t    reserve[15];
  SVnodeLoad load[];
} SDMStatusMsg;

typedef struct {
  SDMMnodeInfos    mnodes;
  SDMDnodeCfg      dnodeCfg;
  SDMVgroupAccess  vgAccess[];
} SDMStatusRsp;

typedef struct {
  uint32_t vgId;
  int32_t  maxTables;
  int64_t  maxCacheSize;
  int32_t  minRowsPerFileBlock;
  int32_t  maxRowsPerFileBlock;
  int32_t  daysPerFile;
  int32_t  daysToKeep;
  int32_t  daysToKeep1;
  int32_t  daysToKeep2;
  int32_t  commitTime;
  uint8_t  precision;  // time resolution
  int8_t   compression;
  int8_t   wals;
  int8_t   commitLog;
  int8_t   replications;
  int8_t   quorum;
  uint32_t arbitratorIp;
  int8_t   reserved[16];
} SMDVnodeCfg;

typedef struct {
  int32_t  nodeId;
  uint32_t nodeIp;
  char     nodeName[TSDB_NODE_NAME_LEN + 1];
} SMDVnodeDesc;

typedef struct {
  SMDVnodeCfg  cfg;
  SMDVnodeDesc nodes[TSDB_MAX_MPEERS];
} SMDCreateVnodeMsg;

typedef struct {
  char    tableId[TSDB_TABLE_ID_LEN + 1];
  int16_t createFlag;
  char    tags[];
} SCMTableInfoMsg;

typedef struct {
  int32_t numOfTables;
  char    tableIds[];
} SCMMultiTableInfoMsg;

typedef struct SCMSTableVgroupMsg {
  char tableId[TSDB_TABLE_ID_LEN];
} SCMSTableVgroupMsg;

typedef struct {
  int32_t   vgId;
  int8_t    numOfIps;
  SIpAddr   ipAddr[TSDB_REPLICA_MAX_NUM];
} SCMVgroupInfo;

typedef struct {
  int32_t  numOfVgroups;
  SCMVgroupInfo vgroups[];
} SCMSTableVgroupRspMsg;

typedef struct {
  int16_t elemLen;

  char    tableId[TSDB_TABLE_ID_LEN + 1];
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
} SSuperTableMetaElemMsg;

typedef struct {
  int32_t numOfTables;
  int32_t join;
  int32_t joinCondLen;  // for join condition
  int32_t metaElem[TSDB_MAX_JOIN_TABLE_NUM];
} SSuperTableMetaMsg;

typedef struct {
  int32_t  nodeId;
  uint32_t nodeIp;
  uint16_t nodePort;
} SVnodeDesc;

typedef struct {
  SVnodeDesc vpeerDesc[TSDB_REPLICA_MAX_NUM];
  int16_t    index;  // used locally
  int32_t    vgId;
  int32_t    numOfSids;
  int32_t    pSidExtInfoList[];  // offset value of STableIdInfo
} SVnodeSidList;

typedef struct {
  int32_t  numOfTables;
  int32_t  numOfVnodes;
  uint16_t tagLen; /* tag value length */
  int32_t  list[]; /* offset of SVnodeSidList, compared to the SSuperTableMeta struct */
} SSuperTableMeta;

typedef struct STableMetaMsg {
  int32_t       contLen;
  char          tableId[TSDB_TABLE_ID_LEN];   // table id
  char          stableId[TSDB_TABLE_ID_LEN];  // stable name if it is created according to super table
  uint8_t       numOfTags;
  uint8_t       precision;
  uint8_t       tableType;
  int16_t       numOfColumns;
  int16_t       sversion;
  int32_t       sid;
  uint64_t      uid;
  SCMVgroupInfo vgroup;
  SSchema       schema[];
} STableMetaMsg;

typedef struct SMultiTableMeta {
  int32_t       numOfTables;
  int32_t       contLen;
  STableMetaMsg metas[];
} SMultiTableMeta;

typedef struct {
  char name[TSDB_TABLE_ID_LEN + 1];
  char data[TSDB_MAX_TAGS_LEN];
} STagData;

/*
 * sql: show tables like '%a_%'
 * payload is the query condition, e.g., '%a_%'
 * payloadLen is the length of payload
 */
typedef struct {
  int8_t   type;
  char     db[TSDB_DB_NAME_LEN + 1];
  uint16_t payloadLen;
  char     payload[];
} SCMShowMsg;

typedef struct SCMShowRsp {
  uint64_t      qhandle;
  STableMetaMsg tableMeta;
} SCMShowRsp;

typedef struct {
  char ip[32];
} SCMCreateDnodeMsg, SCMDropDnodeMsg;

typedef struct {
  uint32_t dnode;
  int32_t  vnode;
  int32_t  sid;
} SDMConfigTableMsg;

typedef struct {
  uint32_t dnodeId;
  int32_t  vgId;
} SDMConfigVnodeMsg;

typedef struct {
  char ip[32];
  char config[64];
} SMDCfgDnodeMsg, SCMCfgDnodeMsg;

typedef struct {
  char     sql[TSDB_SHOW_SQL_LEN + 1];
  uint32_t queryId;
  int64_t  useconds;
  int64_t  stime;
} SQueryDesc;

typedef struct {
  char     sql[TSDB_SHOW_SQL_LEN + 1];
  uint32_t streamId;
  int64_t  num;  // number of computing/cycles
  int64_t  useconds;
  int64_t  ctime;
  int64_t  stime;
  int64_t  slidingTime;
  int64_t  interval;
} SStreamDesc;

typedef struct {
  int32_t    numOfQueries;
  SQueryDesc *qdesc;
} SQqueryList;

typedef struct {
  int32_t     numOfStreams;
  SStreamDesc *sdesc;
} SStreamList;

typedef struct {
  SQqueryList qlist;
  SStreamList slist;
} SCMHeartBeatMsg;

typedef struct {
  uint32_t  queryId;
  uint32_t  streamId;
  int8_t    killConnection;
  SRpcIpSet ipList;
} SCMHeartBeatRsp;

typedef struct {
  char queryId[TSDB_KILL_MSG_LEN + 1];
} SCMKillQueryMsg, SCMKillStreamMsg, SCMKillConnMsg;

typedef struct {
  int32_t  vnode;
  int32_t  sid;
  uint64_t uid;
  uint64_t stime;  // stream starting time
  int32_t  status;
  char     tableId[TSDB_TABLE_ID_LEN + 1];
} SMDAlterStreamMsg;

#pragma pack(pop)

#ifdef __cplusplus
}
#endif

#endif

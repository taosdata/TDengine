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

#ifdef TAOS_MESSAGE_C
#define TAOS_DEFINE_MESSAGE_TYPE( name, msg ) msg, msg "-rsp",
char *taosMsg[] = {
  "null",
#else
#define TAOS_DEFINE_MESSAGE_TYPE( name, msg ) name, name##_RSP,
enum {
  TSDB_MESSAGE_NULL = 0,
#endif

TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_REG, "registration" )             	// 1
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_SUBMIT, "submit" )	                // 3
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_QUERY, "query" )	                  // 5
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_RETRIEVE, "retrieve" )	            // 7

// message from mnode to dnode
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_CREATE_TABLE, "create-table" )	  // 9
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_DROP_TABLE, "drop-table" )	      // 11
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_ALTER_TABLE, "alter-table" )	    // 13
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_CREATE_VNODE, "create-vnode" )	  // 15
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_DROP_VNODE, "drop-vnode" )	      // 17
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_DROP_STABLE, "drop-stable" )	    // 19
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_ALTER_STREAM, "alter-stream" )	  // 21
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_CONFIG_DNODE, "config-dnode" )	  // 23

// message from client to mnode
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CONNECT, "connect" )	            // 31
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CREATE_ACCT, "create-acct" )	    // 33
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_ALTER_ACCT, "alter-acct" )	      // 35
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_DROP_ACCT, "drop-acct" )	        // 37
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CREATE_USER, "create-user" )	    // 39
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_ALTER_USER, "alter-user" )	      // 41
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_DROP_USER, "drop-user" )         // 43
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CREATE_DNODE, "create-dnode" )	  // 45
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_DROP_DNODE, "drop-dnode" )     	// 47
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CREATE_DB, "create-db" )	        // 49
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_DROP_DB, "drop-db" )	            // 51
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_USE_DB, "use-db" )	              // 53
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_ALTER_DB, "alter-db" )	          // 55
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CREATE_TABLE, "create-table" )	  // 57
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_DROP_TABLE, "drop-table" )	      // 59
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_ALTER_TABLE, "alter-table" )	    // 61
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_TABLE_META, "table-meta" )	      // 63
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_STABLE_VGROUP, "stable-vgroup" )	// 65
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_TABLES_META, "tables-meta" )	    // 67
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_ALTER_STREAM, "alter-stream" )	  // 69
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_SHOW, "show" )	                  // 71
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_KILL_QUERY, "kill-query" )	      // 73
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_KILL_STREAM, "kill-stream" )	    // 75
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_KILL_CONN, "kill-conn" )	        // 77
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_HEARTBEAT, "heartbeat" )	        // 79

// message from dnode to mnode
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DM_CONFIG_TABLE, "config-table" )	  // 91
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DM_CONFIG_VNODE, "config-vnode" )	  // 93
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DM_STATUS, "status" )	              // 95
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DM_GRANT, "grant" )	                // 97

TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_SDB_SYNC, "sdb-sync" )	            // 101
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_SDB_FORWARD, "sdb-forward" )	      // 103

#ifndef TAOS_MESSAGE_C
  TSDB_MSG_TYPE_MAX  // 105
#endif

};

#define TSDB_MSG_TYPE_CM_CONFIG_DNODE     TSDB_MSG_TYPE_MD_CONFIG_DNODE
#define TSDB_MSG_TYPE_CM_CONFIG_DNODE_RSP TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP

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
  char     fqdn[TSDB_FQDN_LEN];
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
  char    name[TSDB_COL_NAME_LEN + 1];
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
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  char     superTableId[TSDB_TABLE_ID_LEN + 1];
  char     data[];
} SMDCreateTableMsg;

typedef struct {
  char    tableId[TSDB_TABLE_ID_LEN + 1];
  char    db[TSDB_DB_NAME_LEN + 1];
  int8_t  igExists;
  int8_t  getMeta;
  int16_t numOfTags;
  int16_t numOfColumns;
  int16_t sqlLen;  // the length of SQL, it starts after schema , sql is a null-terminated string
  int32_t contLen;
  int8_t  reserved[16];
  char    schema[];
} SCMCreateTableMsg;

typedef struct {
  char   tableId[TSDB_TABLE_ID_LEN + 1];
  int8_t igNotExists;
} SCMDropTableMsg;

typedef struct {
  char    tableId[TSDB_TABLE_ID_LEN + 1];
  char    db[TSDB_DB_NAME_LEN + 1];
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
  int32_t vgId;
  int64_t uid;
  char    tableId[TSDB_TABLE_ID_LEN + 1];
} SMDDropSTableMsg;

typedef struct {
  int32_t vgId;
} SMDDropVnodeMsg;

typedef struct SColIndex {
  int16_t  colId;      // column id
  int16_t  colIndex;   // column index in colList if it is a normal column or index in tagColList if a tag
  uint16_t flag;       // denote if it is a tag or a normal column
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
  SSqlFuncMsg base;
  struct tExprNode* pExpr;
  int16_t     bytes;
  int16_t     type;
  int16_t     interResBytes;
} SExprInfo;

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
  int64_t uid;
  int32_t tid;
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
  int16_t     tagNameRelType;   // relation of tag criteria and tbname criteria
  int16_t     interpoType;      // interpolate type
  uint64_t    defaultVal;       // default value array list
  int32_t     tsOffset;       // offset value in current msg body, NOTE: ts list is compressed
  int32_t     tsLen;          // total length of ts comp block
  int32_t     tsNumOfBlocks;  // ts comp block numbers
  int32_t     tsOrder;        // ts comp block order
  int32_t     numOfTags;      // number of tags columns involved
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
  int32_t cfgVersion;
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
  int32_t  maxSessions;
  int32_t  cacheBlockSize; //MB
  int32_t  totalBlocks;
  int32_t  daysPerFile;
  int32_t  daysToKeep1;
  int32_t  daysToKeep2;
  int32_t  daysToKeep;
  int32_t  commitTime;
  int32_t  minRowsPerFileBlock;
  int32_t  maxRowsPerFileBlock;
  int8_t   compression;
  int8_t   commitLog;
  int8_t   replications;
  uint8_t  precision;   // time resolution
  int8_t   ignoreExist;
} SCMCreateDbMsg, SCMAlterDbMsg;

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
  char      nodeEp[TSDB_FQDN_LEN];
} SDMMnodeInfo;

typedef struct {
  int8_t       inUse;
  int8_t       nodeNum;
  SDMMnodeInfo nodeInfos[TSDB_MAX_REPLICA];
} SDMMnodeInfos;

typedef struct {
  uint32_t   version;
  int32_t    dnodeId;
  char       dnodeEp[TSDB_FQDN_LEN];
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
  int32_t  cfgVersion;
  int32_t  cacheBlockSize;
  int32_t  totalBlocks;
  int32_t  maxTables;
  int32_t  daysPerFile;
  int32_t  daysToKeep;
  int32_t  daysToKeep1;
  int32_t  daysToKeep2;
  int32_t  minRowsPerFileBlock;
  int32_t  maxRowsPerFileBlock;
  int32_t  commitTime;
  int8_t   precision;
  int8_t   compression;
  int8_t   commitLog;
  int8_t   replications;
  int8_t   wals;
  int8_t   quorum;
  int8_t   reserved[16];
} SMDVnodeCfg;

typedef struct {
  int32_t  nodeId;
  char     nodeEp[TSDB_FQDN_LEN];
} SMDVnodeDesc;

typedef struct {
  SMDVnodeCfg  cfg;
  SMDVnodeDesc nodes[TSDB_MAX_REPLICA];
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
  int32_t numOfTables;
} SCMSTableVgroupMsg, SCMSTableVgroupRspMsg;

typedef struct {
  int32_t   vgId;
  int8_t    numOfIps;
  SIpAddr   ipAddr[TSDB_MAX_REPLICA_NUM];
} SCMVgroupInfo;

typedef struct {
  int32_t numOfVgroups;
  SCMVgroupInfo vgroups[];
} SVgroupsInfo;

//typedef struct {
//  int32_t numOfTables;
//  int32_t join;
//  int32_t joinCondLen;  // for join condition
//  int32_t metaElem[TSDB_MAX_JOIN_TABLE_NUM];
//} SSuperTableMetaMsg;

typedef struct STableMetaMsg {
  int32_t       contLen;
  char          tableId[TSDB_TABLE_ID_LEN + 1];   // table id
  char          stableId[TSDB_TABLE_ID_LEN + 1];  // stable name if it is created according to super table
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
  char     ep[TSDB_FQDN_LEN];  // end point, hostname:port
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
  char ep[TSDB_FQDN_LEN];  // end point, hostname:port
  char config[64];
} SMDCfgDnodeMsg, SCMCfgDnodeMsg;

typedef struct {
  char     sql[TSDB_SHOW_SQL_LEN];
  uint32_t queryId;
  int64_t  useconds;
  int64_t  stime;
} SQueryDesc;

typedef struct {
  char     sql[TSDB_SHOW_SQL_LEN];
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
} SQqueryList;

typedef struct {
  int32_t     numOfStreams;
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

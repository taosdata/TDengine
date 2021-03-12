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
#include "tdataformat.h"

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

// message from client to dnode
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_SUBMIT, "submit" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_QUERY, "query" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_FETCH, "fetch" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_UPDATE_TAG_VAL, "update-tag-val" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY1, "dummy1" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY2, "dummy2" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY3, "dummy3" )

// message from mnode to dnode
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_CREATE_TABLE, "create-table" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_DROP_TABLE, "drop-table" )	
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_ALTER_TABLE, "alter-table" )	
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_CREATE_VNODE, "create-vnode" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_DROP_VNODE, "drop-vnode" )	
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_DROP_STABLE, "drop-stable" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_ALTER_STREAM, "alter-stream" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_CONFIG_DNODE, "config-dnode" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_ALTER_VNODE, "alter-vnode" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_MD_CREATE_MNODE, "create-mnode" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY6, "dummy6" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY7, "dummy7" )

// message from client to mnode
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CONNECT, "connect" )	 
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CREATE_ACCT, "create-acct" )	
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_ALTER_ACCT, "alter-acct" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_DROP_ACCT, "drop-acct" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CREATE_USER, "create-user" )	
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_ALTER_USER, "alter-user" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_DROP_USER, "drop-user" ) 
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CREATE_DNODE, "create-dnode" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_DROP_DNODE, "drop-dnode" )   
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CREATE_DB, "create-db" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_DROP_DB, "drop-db" )	  
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_USE_DB, "use-db" )	 
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_ALTER_DB, "alter-db" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CREATE_TABLE, "create-table" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_DROP_TABLE, "drop-table" )	
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_ALTER_TABLE, "alter-table" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_TABLE_META, "table-meta" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_STABLE_VGROUP, "stable-vgroup" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_TABLES_META, "tables-meta" )	  
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_ALTER_STREAM, "alter-stream" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_SHOW, "show" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_RETRIEVE, "retrieve" )     
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_KILL_QUERY, "kill-query" )	
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_KILL_STREAM, "kill-stream" )	
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_KILL_CONN, "kill-conn" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CONFIG_DNODE, "cm-config-dnode" ) 
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_HEARTBEAT, "heartbeat" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY8, "dummy8" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY9, "dummy9" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY10, "dummy10" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY11, "dummy11" )

// message from dnode to mnode
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DM_CONFIG_TABLE, "config-table" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DM_CONFIG_VNODE, "config-vnode" )	
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DM_STATUS, "status" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DM_GRANT, "grant" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DM_AUTH, "auth" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY12, "dummy12" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY13, "dummy13" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_DUMMY14, "dummy14" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_NETWORK_TEST, "nettest" )

// message for topic
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_CREATE_TP, "create-tp" )
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_DROP_TP, "drop-tp" )	  
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_USE_TP, "use-tp" )	 
TAOS_DEFINE_MESSAGE_TYPE( TSDB_MSG_TYPE_CM_ALTER_TP, "alter-tp" )

#ifndef TAOS_MESSAGE_C
  TSDB_MSG_TYPE_MAX  // 105
#endif

};

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
  TSDB_MGMT_TABLE_VARIABLES,
  TSDB_MGMT_TABLE_CONNS,
  TSDB_MGMT_TABLE_SCORES,
  TSDB_MGMT_TABLE_GRANTS,
  TSDB_MGMT_TABLE_VNODES,
  TSDB_MGMT_TABLE_STREAMTABLES,
  TSDB_MGMT_TABLE_CLUSTER,
  TSDB_MGMT_TABLE_TP,
  TSDB_MGMT_TABLE_MAX,
};

#define TSDB_ALTER_TABLE_ADD_TAG_COLUMN    1
#define TSDB_ALTER_TABLE_DROP_TAG_COLUMN   2
#define TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN 3
#define TSDB_ALTER_TABLE_UPDATE_TAG_VAL    4

#define TSDB_ALTER_TABLE_ADD_COLUMN        5
#define TSDB_ALTER_TABLE_DROP_COLUMN       6
#define TSDB_ALTER_TABLE_CHANGE_COLUMN     7

#define TSDB_FILL_NONE             0
#define TSDB_FILL_NULL             1
#define TSDB_FILL_SET_VALUE        2
#define TSDB_FILL_LINEAR           3
#define TSDB_FILL_PREV             4
#define TSDB_FILL_NEXT             5

#define TSDB_ALTER_USER_PASSWD     0x1
#define TSDB_ALTER_USER_PRIVILEGES 0x2

#define TSDB_KILL_MSG_LEN          30

#define TSDB_VN_READ_ACCCESS       ((char)0x1)
#define TSDB_VN_WRITE_ACCCESS      ((char)0x2)
#define TSDB_VN_ALL_ACCCESS (TSDB_VN_READ_ACCCESS | TSDB_VN_WRITE_ACCCESS)

#define TSDB_COL_NORMAL             0x0u    // the normal column of the table
#define TSDB_COL_TAG                0x1u    // the tag column type
#define TSDB_COL_UDC                0x2u    // the user specified normal string column, it is a dummy column
#define TSDB_COL_NULL               0x4u    // the column filter NULL or not

#define TSDB_COL_IS_TAG(f)          (((f&(~(TSDB_COL_NULL)))&TSDB_COL_TAG) != 0)
#define TSDB_COL_IS_NORMAL_COL(f)   ((f&(~(TSDB_COL_NULL))) == TSDB_COL_NORMAL)
#define TSDB_COL_IS_UD_COL(f)       ((f&(~(TSDB_COL_NULL))) == TSDB_COL_UDC)
#define TSDB_COL_REQ_NULL(f)        (((f)&TSDB_COL_NULL) != 0)


extern char *taosMsg[];

#pragma pack(push, 1)

// null-terminated string instead of char array to avoid too many memory consumption in case of more than 1M tableMeta
typedef struct {
  char     fqdn[TSDB_FQDN_LEN];
  uint16_t port;
} SEpAddrMsg;

typedef struct {
  char*    fqdn;
  uint16_t port;
} SEpAddr1;

typedef struct {
  int32_t numOfVnodes;
} SMsgDesc;

typedef struct SMsgHead {
  int32_t contLen;
  int32_t vgId;
} SMsgHead;

// Submit message for one table
typedef struct SSubmitBlk {
  uint64_t uid;        // table unique id
  int32_t  tid;        // table id
  int32_t  padding;    // TODO just for padding here
  int32_t  sversion;   // data schema version
  int32_t  dataLen;    // data part length, not including the SSubmitBlk head
  int32_t  schemaLen;  // schema length, if length is 0, no schema exists
  int16_t  numOfRows;  // total number of rows in current submit block
  char     data[];
} SSubmitBlk;

// Submit message for this TSDB
typedef struct SSubmitMsg {
  SMsgHead   header;
  int32_t    length;
  int32_t    numOfBlocks;
  char       blocks[];
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
  int32_t  tid;
  int32_t  sversion;
  int32_t  tversion;
  int32_t  tagDataLen;
  int32_t  sqlDataLen;
  uint64_t uid;
  uint64_t superTableUid;
  uint64_t createdTime;
  char     tableFname[TSDB_TABLE_FNAME_LEN];
  char     stableFname[TSDB_TABLE_FNAME_LEN];
  char     data[];
} SMDCreateTableMsg;

typedef struct {
  int32_t len;  // one create table message
  char    tableName[TSDB_TABLE_FNAME_LEN];
  int8_t  igExists;
  int8_t  getMeta;
  int16_t numOfTags;
  int16_t numOfColumns;
  int16_t sqlLen;  // the length of SQL, it starts after schema , sql is a null-terminated string
  int8_t  reserved[16];
  char    schema[];
} SCreateTableMsg;

typedef struct {
  int32_t numOfTables;
  int32_t contLen;
} SCMCreateTableMsg;

typedef struct {
  char   name[TSDB_TABLE_FNAME_LEN];
  int8_t igNotExists;
} SCMDropTableMsg;

typedef struct {
  char    tableFname[TSDB_TABLE_FNAME_LEN];
  char    db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  int16_t type; /* operation type   */
  int16_t numOfCols; /* number of schema */
  int32_t tagValLen;
  SSchema schema[];
  // tagVal is padded after schema
  // char    tagVal[];
} SAlterTableMsg;

typedef struct {
  SMsgHead  head;
  int64_t   uid;
  int32_t   tid;
  int16_t   tversion;
  int16_t   colId;
  int8_t    type;
  int16_t   bytes;
  int32_t   tagValLen;
  int16_t   numOfTags;
  int32_t   schemaLen;
  char      data[];
} SUpdateTableTagValMsg;

typedef struct {
  char    clientVersion[TSDB_VERSION_LEN];
  char    msgVersion[TSDB_VERSION_LEN];
  char    db[TSDB_TABLE_FNAME_LEN];
  char    appName[TSDB_APPNAME_LEN];
  int32_t pid;
} SConnectMsg;

typedef struct {
  char      acctId[TSDB_ACCT_ID_LEN];
  char      serverVersion[TSDB_VERSION_LEN];
  char      clusterId[TSDB_CLUSTER_ID_LEN];
  int8_t    writeAuth;
  int8_t    superAuth;
  int8_t    reserved1;
  int8_t    reserved2;
  int32_t   connId;
  SRpcEpSet epSet;
} SConnectRsp;

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
  char     user[TSDB_USER_LEN];
  char     pass[TSDB_KEY_LEN];
  SAcctCfg cfg;
} SCreateAcctMsg, SAlterAcctMsg;

typedef struct {
  char user[TSDB_USER_LEN];
} SDropUserMsg, SDropAcctMsg;

typedef struct {
  char   user[TSDB_USER_LEN];
  char   pass[TSDB_KEY_LEN];
  int8_t privilege;
  int8_t flag;
} SCreateUserMsg, SAlterUserMsg;

typedef struct {
  int32_t  contLen;
  int32_t  vgId;
  int32_t  tid;
  uint64_t uid;
  char     tableFname[TSDB_TABLE_FNAME_LEN];
} SMDDropTableMsg;

typedef struct {
  int32_t  contLen;
  int32_t  vgId;
  uint64_t uid;
  char    tableFname[TSDB_TABLE_FNAME_LEN];
} SDropSTableMsg;

typedef struct {
  int32_t vgId;
} SDropVnodeMsg;

typedef struct SColIndex {
  int16_t  colId;      // column id
  int16_t  colIndex;   // column index in colList if it is a normal column or index in tagColList if a tag
  uint16_t flag;       // denote if it is a tag or a normal column
  char     name[TSDB_COL_NAME_LEN];  // TODO remove it
} SColIndex;

/* sql function msg, to describe the message to vnode about sql function
 * operations in select clause */
typedef struct SSqlFuncMsg {
  int16_t functionId;
  int16_t numOfParams;

  int16_t resColId;      // result column id, id of the current output column
  int16_t colType;
  int16_t colBytes;

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
  int32_t     interBytes;
  int64_t     uid;
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
  union{
    int64_t placeholder;
    SColumnFilterInfo *filters;
  };
} SColumnInfo;

typedef struct STableIdInfo {
  uint64_t uid;
  int32_t  tid;
  TSKEY    key;  // last accessed ts, for subscription
} STableIdInfo;

typedef struct STimeWindow {
  TSKEY skey;
  TSKEY ekey;
} STimeWindow;

typedef struct {
  SMsgHead    head;
  char        version[TSDB_VERSION_LEN];

  STimeWindow window;
  int32_t     numOfTables;
  int16_t     order;
  int16_t     orderColId;
  int16_t     numOfCols;        // the number of columns will be load from vnode
  SInterval   interval;
  uint16_t    tagCondLen;       // tag length in current query
  uint32_t    tbnameCondLen;    // table name filter condition string length
  int16_t     numOfGroupCols;   // num of group by columns
  int16_t     orderByIdx;
  int16_t     orderType;        // used in group by xx order by xxx
  int64_t     vgroupLimit;       // limit the number of rows for each table, used in order by + limit in stable projection query.
  int16_t     prjOrder;         // global order in super table projection query.
  int64_t     limit;
  int64_t     offset;
  uint32_t    queryType;        // denote another query process
  int16_t     numOfOutput;      // final output columns numbers
  int16_t     tagNameRelType;   // relation of tag criteria and tbname criteria
  int16_t     fillType;         // interpolate type
  uint64_t    fillVal;          // default value array list
  int32_t     secondStageOutput;
  int32_t     tsOffset;         // offset value in current msg body, NOTE: ts list is compressed
  int32_t     tsLen;            // total length of ts comp block
  int32_t     tsNumOfBlocks;    // ts comp block numbers
  int32_t     tsOrder;          // ts comp block order
  int32_t     numOfTags;        // number of tags columns involved
  int32_t     sqlstrLen;        // sql query string
  int32_t     prevResultLen;    // previous result length
  SColumnInfo colList[];
} SQueryTableMsg;

typedef struct {
  int32_t  code;
  uint64_t qhandle; // query handle
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
  int64_t offset;     // updated offset value for multi-vnode projection query
  int64_t useconds;
  char    data[];
} SRetrieveTableRsp;

typedef struct {
  int32_t  vgId;
  int32_t  dbCfgVersion;
  int64_t  totalStorage;
  int64_t  compStorage;
  int64_t  pointsWritten;
  uint64_t vnodeVersion;
  int32_t  vgCfgVersion;
  uint8_t  status;
  uint8_t  role;
  uint8_t  replica;
  uint8_t  reserved;
} SVnodeLoad;

typedef struct {
  char     db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  int32_t  cacheBlockSize; //MB
  int32_t  totalBlocks;
  int32_t  maxTables;
  int32_t  daysPerFile;
  int32_t  daysToKeep;
  int32_t  daysToKeep1;
  int32_t  daysToKeep2;
  int32_t  minRowsPerFileBlock;
  int32_t  maxRowsPerFileBlock;
  int32_t  commitTime;
  int32_t  fsyncPeriod;
  uint8_t  precision;   // time resolution
  int8_t   compression;
  int8_t   walLevel;
  int8_t   replications;
  int8_t   quorum;
  int8_t   ignoreExist;
  int8_t   update;
  int8_t   cacheLastRow;
  int8_t   dbType;
  int16_t  partitions;
  int8_t   reserve[5];
} SCreateDbMsg, SAlterDbMsg;

typedef struct {
  char    db[TSDB_TABLE_FNAME_LEN];
  uint8_t ignoreNotExists;
} SDropDbMsg, SUseDbMsg;

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
} SVgroupAccess;

typedef struct {
  int32_t  dnodeId;
  uint32_t moduleStatus;
  uint32_t numOfVnodes;
  char     clusterId[TSDB_CLUSTER_ID_LEN];
  char     reserved[16];
} SDnodeCfg;

typedef struct {
  int32_t  dnodeId;
  uint16_t dnodePort;
  char     dnodeFqdn[TSDB_FQDN_LEN];
} SDnodeEp;

typedef struct {
  int32_t  dnodeNum;
  SDnodeEp dnodeEps[];
} SDnodeEps;

typedef struct {
  int32_t mnodeId;
  char    mnodeEp[TSDB_EP_LEN];
} SMInfo;

typedef struct {
  int8_t inUse;
  int8_t mnodeNum;
  SMInfo mnodeInfos[TSDB_MAX_REPLICA];
} SMInfos;

typedef struct {
  int32_t  numOfMnodes;               // tsNumOfMnodes
  int32_t  mnodeEqualVnodeNum;        // tsMnodeEqualVnodeNum
  int32_t  offlineThreshold;          // tsOfflineThreshold
  int32_t  statusInterval;            // tsStatusInterval
  int32_t  maxtablesPerVnode;
  int32_t  maxVgroupsPerDb;
  char     arbitrator[TSDB_EP_LEN];   // tsArbitrator
  char     timezone[64];              // tsTimezone
  int64_t  checkTime;                 // 1970-01-01 00:00:00.000
  char     locale[TSDB_LOCALE_LEN];   // tsLocale
  char     charset[TSDB_LOCALE_LEN];  // tsCharset
  int8_t   enableBalance;             // tsEnableBalance
  int8_t   flowCtrl;
  int8_t   slaveQuery;
  int8_t   adjustMaster;
  int8_t   reserved[4];
} SClusterCfg;

typedef struct {
  uint32_t    version;
  int32_t     dnodeId;
  char        dnodeEp[TSDB_EP_LEN];
  uint32_t    moduleStatus;
  uint32_t    lastReboot;        // time stamp for last reboot
  uint16_t    reserve1;          // from config file
  uint16_t    openVnodes;
  uint16_t    numOfCores;
  float       diskAvailable;  // GB
  char        clusterId[TSDB_CLUSTER_ID_LEN];
  uint8_t     alternativeRole;
  uint8_t     reserve2[15];
  SClusterCfg clusterCfg;
  SVnodeLoad  load[];
} SStatusMsg;

typedef struct {
  SMInfos       mnodes;
  SDnodeCfg     dnodeCfg;
  SVgroupAccess vgAccess[];
} SStatusRsp;

typedef struct {
  uint32_t vgId;
  int32_t  dbCfgVersion;
  int32_t  maxTables;
  int32_t  cacheBlockSize;
  int32_t  totalBlocks;
  int32_t  daysPerFile;
  int32_t  daysToKeep;
  int32_t  daysToKeep1;
  int32_t  daysToKeep2;
  int32_t  minRowsPerFileBlock;
  int32_t  maxRowsPerFileBlock;
  int32_t  commitTime;
  int32_t  fsyncPeriod;
  int8_t   precision;
  int8_t   compression;
  int8_t   walLevel;
  int8_t   vgReplica;
  int8_t   wals;
  int8_t   quorum;
  int8_t   update;
  int8_t   cacheLastRow;
  int32_t  vgCfgVersion;
  int8_t   dbReplica;
  int8_t   dbType;
  int8_t   reserved[8];
} SVnodeCfg;

typedef struct {
  int32_t  nodeId;
  char     nodeEp[TSDB_EP_LEN];
} SVnodeDesc;

typedef struct {
  char       db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  SVnodeCfg  cfg;
  SVnodeDesc nodes[TSDB_MAX_REPLICA];
} SCreateVnodeMsg, SAlterVnodeMsg;

typedef struct {
  char    tableFname[TSDB_TABLE_FNAME_LEN];
  int16_t createFlag;
  char    tags[];
} STableInfoMsg;

typedef struct {
  int32_t numOfTables;
  char    tableIds[];
} SMultiTableInfoMsg;

typedef struct SSTableVgroupMsg {
  int32_t numOfTables;
} SSTableVgroupMsg, SSTableVgroupRspMsg;

typedef struct {
  int32_t       vgId;
  int8_t        numOfEps;
  SEpAddr1      epAddr[TSDB_MAX_REPLICA];
} SVgroupInfo;

typedef struct {
  int32_t    vgId;
  int8_t     numOfEps;
  SEpAddrMsg epAddr[TSDB_MAX_REPLICA];
} SVgroupMsg;

typedef struct {
  int32_t numOfVgroups;
  SVgroupInfo vgroups[];
} SVgroupsInfo;

typedef struct {
  int32_t numOfVgroups;
  SVgroupMsg vgroups[];
} SVgroupsMsg;

typedef struct STableMetaMsg {
  int32_t       contLen;
  char          tableFname[TSDB_TABLE_FNAME_LEN];   // table id
  uint8_t       numOfTags;
  uint8_t       precision;
  uint8_t       tableType;
  int16_t       numOfColumns;
  int16_t       sversion;
  int16_t       tversion;
  int32_t       tid;
  uint64_t      uid;
  SVgroupMsg    vgroup;

  char          sTableName[TSDB_TABLE_FNAME_LEN];
  uint64_t      suid;
  SSchema       schema[];
} STableMetaMsg;

typedef struct SMultiTableMeta {
  int32_t       numOfTables;
  int32_t       contLen;
  char          metas[];
} SMultiTableMeta;

typedef struct {
  int32_t dataLen;
  char    name[TSDB_TABLE_FNAME_LEN];
  char   *data;
} STagData;

/*
 * sql: show tables like '%a_%'
 * payload is the query condition, e.g., '%a_%'
 * payloadLen is the length of payload
 */
typedef struct {
  int8_t   type;
  char     db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  uint16_t payloadLen;
  char     payload[];
} SShowMsg;

typedef struct SShowRsp {
  uint64_t      qhandle;
  STableMetaMsg tableMeta;
} SShowRsp;

typedef struct {
  char ep[TSDB_EP_LEN];  // end point, hostname:port
} SCreateDnodeMsg, SDropDnodeMsg;

typedef struct {
  int32_t dnodeId;
  char    dnodeEp[TSDB_EP_LEN];  // end point, hostname:port
  SMInfos mnodes;
} SCreateMnodeMsg;

typedef struct {
  int32_t dnodeId;
  int32_t vgId;
  int32_t tid;
} SConfigTableMsg;

typedef struct {
  uint32_t dnodeId;
  int32_t  vgId;
} SConfigVnodeMsg;

typedef struct {
  char ep[TSDB_EP_LEN];  // end point, hostname:port
  char config[64];
} SCfgDnodeMsg;

typedef struct {
  char     sql[TSDB_SHOW_SQL_LEN];
  uint32_t queryId;
  int64_t  useconds;
  int64_t  stime;
  uint64_t qHandle;
} SQueryDesc;

typedef struct {
  char     sql[TSDB_SHOW_SQL_LEN];
  char     dstTable[TSDB_TABLE_NAME_LEN];
  uint32_t streamId;
  int64_t  num;  // number of computing/cycles
  int64_t  useconds;
  int64_t  ctime;
  int64_t  stime;
  int64_t  slidingTime;
  int64_t  interval;
} SStreamDesc;

typedef struct {
  char     clientVer[TSDB_VERSION_LEN];
  uint32_t connId;
  int32_t  pid;
  int32_t  numOfQueries;
  int32_t  numOfStreams;
  char     appName[TSDB_APPNAME_LEN];
  char     pData[];
} SHeartBeatMsg;

typedef struct {
  uint32_t  queryId;
  uint32_t  streamId;
  uint32_t  totalDnodes;
  uint32_t  onlineDnodes;
  uint32_t  connId;
  int8_t    killConnection;
  SRpcEpSet epSet;
} SHeartBeatRsp;

typedef struct {
  char queryId[TSDB_KILL_MSG_LEN + 1];
} SKillQueryMsg, SKillStreamMsg, SKillConnMsg;

typedef struct {
  int32_t  vnode;
  int32_t  sid;
  uint64_t uid;
  uint64_t stime;  // stream starting time
  int32_t  status;
  char     tableFname[TSDB_TABLE_FNAME_LEN];
} SAlterStreamMsg;

typedef struct {
  char user[TSDB_USER_LEN];
  char spi;
  char encrypt;
  char secret[TSDB_KEY_LEN];
  char ckey[TSDB_KEY_LEN];
} SAuthMsg, SAuthRsp;

typedef struct {
  int8_t  finished;
  int8_t  reserved1[7];
  char    name[TSDB_STEP_NAME_LEN];
  char    desc[TSDB_STEP_DESC_LEN];
  char    reserved2[64];
} SStartupStep;

#pragma pack(pop)

#ifdef __cplusplus
}
#endif

#endif

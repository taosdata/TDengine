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

#ifndef _TD_MNODE_DEF_H_
#define _TD_MNODE_DEF_H_

#include "taosmsg.h"
#include "tlog.h"
#include "trpc.h"
#include "ttimer.h"
#include "cJSON.h"
#include "mnode.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int32_t mDebugFlag;

// mnode log function
#define mFatal(...) { if (mDebugFlag & DEBUG_FATAL) { taosPrintLog("MND FATAL ", 255, __VA_ARGS__); }}
#define mError(...) { if (mDebugFlag & DEBUG_ERROR) { taosPrintLog("MND ERROR ", 255, __VA_ARGS__); }}
#define mWarn(...)  { if (mDebugFlag & DEBUG_WARN)  { taosPrintLog("MND WARN ", 255, __VA_ARGS__); }}
#define mInfo(...)  { if (mDebugFlag & DEBUG_INFO)  { taosPrintLog("MND ", 255, __VA_ARGS__); }}
#define mDebug(...) { if (mDebugFlag & DEBUG_DEBUG) { taosPrintLog("MND ", mDebugFlag, __VA_ARGS__); }}
#define mTrace(...) { if (mDebugFlag & DEBUG_TRACE) { taosPrintLog("MND ", mDebugFlag, __VA_ARGS__); }}

// #define mLError(...) { monSaveLog(2, __VA_ARGS__); mError(__VA_ARGS__) }
// #define mLWarn(...)  { monSaveLog(1, __VA_ARGS__); mWarn(__VA_ARGS__)  }
// #define mLInfo(...)  { monSaveLog(0, __VA_ARGS__); mInfo(__VA_ARGS__) }

#define mLError(...) {mError(__VA_ARGS__) }
#define mLWarn(...)  {mWarn(__VA_ARGS__)  }
#define mLInfo(...)  {mInfo(__VA_ARGS__) }

typedef struct SClusterObj SClusterObj; 
typedef struct SDnodeObj SDnodeObj;
typedef struct SMnodeObj SMnodeObj;
typedef struct SAcctObj SAcctObj;
typedef struct SUserObj SUserObj;
typedef struct SDbObj SDbObj;
typedef struct SVgObj SVgObj;
typedef struct SSTableObj SSTableObj;
typedef struct SFuncObj SFuncObj; 
typedef struct SOperObj SOperObj;
typedef struct SMnMsg SMnMsg;

typedef enum {
  MN_SDB_START = 0,
  MN_SDB_CLUSTER = 1,
  MN_SDB_DNODE = 2,
  MN_SDB_MNODE = 3,
  MN_SDB_ACCT = 4,
  MN_SDB_AUTH = 5,
  MN_SDB_USER = 6,
  MN_SDB_DB = 7,
  MN_SDB_VGROUP = 8,
  MN_SDB_STABLE = 9,
  MN_SDB_FUNC = 10,
  MN_SDB_OPER = 11,
  MN_SDB_MAX = 12
} EMnSdb;

typedef enum { MN_OP_START = 0, MN_OP_INSERT = 1, MN_OP_UPDATE = 2, MN_OP_DELETE = 3, MN_OP_MAX = 4 } EMnOp;

typedef enum { MN_KEY_START = 0, MN_KEY_BINARY = 1, MN_KEY_INT32 = 2, MN_KEY_INT64 = 3, MN_KEY_MAX } EMnKey;

typedef enum {
  MN_AUTH_ACCT_START = 0,
  MN_AUTH_ACCT_USER,
  MN_AUTH_ACCT_DNODE,
  MN_AUTH_ACCT_MNODE,
  MN_AUTH_ACCT_DB,
  MN_AUTH_ACCT_TABLE,
  MN_AUTH_ACCT_MAX
} EMnAuthAcct;

typedef enum {
  MN_AUTH_OP_START = 0,
  MN_AUTH_OP_CREATE_USER,
  MN_AUTH_OP_ALTER_USER,
  MN_AUTH_OP_DROP_USER,
  MN_AUTH_MAX
} EMnAuthOp;

typedef enum { MN_STATUS_UNINIT = 0, MN_STATUS_INIT = 1, MN_STATUS_READY = 2, MN_STATUS_CLOSING = 3 } EMnStatus;

typedef enum { MN_SDB_STAT_AVAIL = 0, MN_SDB_STAT_DROPPED = 1 } EMnSdbStat;

typedef struct {
  int8_t type;
  int8_t status;
} SdbHead;
typedef struct SClusterObj {
  SdbHead head;
  int64_t id;
  char    uid[TSDB_CLUSTER_ID_LEN];
  int64_t createdTime;
  int64_t updateTime;
} SClusterObj;

typedef struct SDnodeObj {
  SdbHead  head;
  int32_t  id;
  int32_t  vnodes;
  int64_t  createdTime;
  int64_t  updateTime;
  int64_t  lastAccess;
  int64_t  lastReboot;  // time stamp for last reboot
  char     fqdn[TSDB_FQDN_LEN];
  char     ep[TSDB_EP_LEN];
  uint16_t port;
  int16_t  numOfCores;       // from dnode status msg
  int8_t   alternativeRole;  // from dnode status msg, 0-any, 1-mgmt, 2-dnode
  int8_t   status;           // set in balance function
  int8_t   offlineReason;
} SDnodeObj;

typedef struct SMnodeObj {
  SdbHead    head;
  int32_t    id;
  int8_t     status;
  int8_t     role;
  int32_t    roleTerm;
  int64_t    roleTime;
  int64_t    createdTime;
  int64_t    updateTime;
  SDnodeObj *pDnode;
} SMnodeObj;

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
  int64_t totalStorage;  // Total storage wrtten from this account
  int64_t compStorage;   // Compressed storage on disk
  int64_t queryTime;
  int64_t totalPoints;
  int64_t inblound;
  int64_t outbound;
  int64_t sKey;
  int32_t numOfUsers;
  int32_t numOfDbs;
  int32_t numOfTimeSeries;
  int32_t numOfPointsPerSecond;
  int32_t numOfConns;
  int32_t numOfQueries;
  int32_t numOfStreams;
  int8_t  accessState;   // Checked by mgmt heartbeat message
} SAcctInfo;

typedef struct SAcctObj {
  SdbHead   head;
  char      acct[TSDB_USER_LEN];
  int64_t   createdTime;
  int64_t   updateTime;
  int32_t   acctId;
  int8_t    status;
  SAcctCfg  cfg;
  SAcctInfo info;
} SAcctObj;

typedef struct SUserObj {
  SdbHead   head;
  char      user[TSDB_USER_LEN];
  char      pass[TSDB_KEY_LEN];
  char      acct[TSDB_USER_LEN];
  int64_t   createdTime;
  int64_t   updateTime;
  int8_t    rootAuth;
  SAcctObj *pAcct;
} SUserObj;

typedef struct {
  int32_t cacheBlockSize;
  int32_t totalBlocks;
  int32_t maxTables;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t minRowsPerFileBlock;
  int32_t maxRowsPerFileBlock;
  int32_t commitTime;
  int32_t fsyncPeriod;
  int8_t  precision;
  int8_t  compression;
  int8_t  walLevel;
  int8_t  replications;
  int8_t  quorum;
  int8_t  update;
  int8_t  cacheLastRow;
  int8_t  dbType;
  int16_t partitions;
} SDbCfg;

typedef struct SDbObj {
  SdbHead   head;
  char      name[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  char      acct[TSDB_USER_LEN];
  int64_t   createdTime;
  int64_t   updateTime;
  SDbCfg    cfg;
  int8_t    status;
  int32_t   numOfVgroups;
  int32_t   numOfTables;
  int32_t   numOfSuperTables;
  int32_t   vgListSize;
  int32_t   vgListIndex;
  SVgObj  **vgList;
  SAcctObj *pAcct;
} SDbObj;

typedef struct {
  int32_t    dnodeId;
  int8_t     role;
  SDnodeObj *pDnode;
} SVnodeGid;

typedef struct SVgObj {
  uint32_t  vgId;
  int32_t   numOfVnodes;
  int64_t   createdTime;
  int64_t   updateTime;
  int32_t   lbDnodeId;
  int32_t   lbTime;
  char      dbName[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  int8_t    inUse;
  int8_t    accessState;
  int8_t    status;
  SVnodeGid vnodeGid[TSDB_MAX_REPLICA];
  int32_t   vgCfgVersion;
  int8_t    compact;
  int32_t   numOfTables;
  int64_t   totalStorage;
  int64_t   compStorage;
  int64_t   pointsWritten;
  SDbObj   *pDb;
} SVgObj;

typedef struct SSTableObj {
  SdbHead    head;
  char       tableId[TSDB_TABLE_NAME_LEN];
  uint64_t   uid;
  int64_t    createdTime;
  int64_t    updateTime;
  int32_t    numOfColumns;  // used by normal table
  int32_t    numOfTags;
  SSchema *  schema;
} SSTableObj;

typedef struct SFuncObj {
  SdbHead head;
  char    name[TSDB_FUNC_NAME_LEN];
  char    path[128];
  int32_t contLen;
  char    cont[TSDB_FUNC_CODE_LEN];
  int32_t funcType;
  int32_t bufSize;
  int64_t createdTime;
  uint8_t resType;
  int16_t resBytes;
  int64_t sig;
  int16_t type;
} SFuncObj;

typedef struct {
  int8_t   type;
  int8_t   maxReplica;
  int16_t  numOfColumns;
  int32_t  index;
  int32_t  rowSize;
  int32_t  numOfRows;
  int32_t  numOfReads;
  uint16_t payloadLen;
  void    *pIter;
  void    *pVgIter;
  void   **ppShow;
  char     db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  int16_t  offset[TSDB_MAX_COLUMNS];
  int32_t  bytes[TSDB_MAX_COLUMNS];
  char     payload[];
} SShowObj;

typedef struct {
  int32_t len;
  void   *rsp;
} SMnRsp;

typedef struct SMnMsg {
  void (*fp)(SMnMsg *pMsg, int32_t code);
  SUserObj *pUser;
  int16_t   received;
  int16_t   successed;
  int16_t   expected;
  int16_t   retry;
  int32_t   code;
  int64_t   createdTime;
  SMnRsp    rpcRsp;
  SRpcMsg   rpcMsg;
  char      pCont[];
} SMnReq;

#ifdef __cplusplus
}
#endif

#endif /*_TD_MNODE_DEF_H_*/

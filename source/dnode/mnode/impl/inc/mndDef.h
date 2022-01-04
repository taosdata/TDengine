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

#ifndef _TD_MND_DEF_H_
#define _TD_MND_DEF_H_

#include "os.h"

#include "cJSON.h"
#include "sync.h"
#include "tmsg.h"
#include "thash.h"
#include "tlog.h"
#include "trpc.h"
#include "ttimer.h"

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

typedef enum {
  MND_AUTH_ACCT_START = 0,
  MND_AUTH_ACCT_USER,
  MND_AUTH_ACCT_DNODE,
  MND_AUTH_ACCT_MNODE,
  MND_AUTH_ACCT_DB,
  MND_AUTH_ACCT_TABLE,
  MND_AUTH_ACCT_MAX
} EAuthAcct;

typedef enum {
  MND_AUTH_OP_START = 0,
  MND_AUTH_OP_CREATE_USER,
  MND_AUTH_OP_ALTER_USER,
  MND_AUTH_OP_DROP_USER,
  MND_AUTH_MAX
} EAuthOp;

typedef enum {
  TRN_STAGE_PREPARE = 0,
  TRN_STAGE_REDO_LOG = 1,
  TRN_STAGE_REDO_ACTION = 2,
  TRN_STAGE_UNDO_LOG = 3,
  TRN_STAGE_UNDO_ACTION = 4,
  TRN_STAGE_COMMIT_LOG = 5,
  TRN_STAGE_COMMIT = 6,
  TRN_STAGE_ROLLBACK = 7,
  TRN_STAGE_FINISHED = 8
} ETrnStage;

typedef enum { TRN_POLICY_ROLLBACK = 0, TRN_POLICY_RETRY = 1 } ETrnPolicy;

typedef enum {
  DND_STATUS_OFFLINE = 0,
  DND_STATUS_READY = 1,
  DND_STATUS_CREATING = 2,
  DND_STATUS_DROPPING = 3
} EDndStatus;

typedef enum {
  DND_REASON_ONLINE = 0,
  DND_REASON_STATUS_MSG_TIMEOUT,
  DND_REASON_STATUS_NOT_RECEIVED,
  DND_REASON_VERSION_NOT_MATCH,
  DND_REASON_DNODE_ID_NOT_MATCH,
  DND_REASON_CLUSTER_ID_NOT_MATCH,
  DND_REASON_STATUS_INTERVAL_NOT_MATCH,
  DND_REASON_TIME_ZONE_NOT_MATCH,
  DND_REASON_LOCALE_NOT_MATCH,
  DND_REASON_CHARSET_NOT_MATCH,
  DND_REASON_OTHERS
} EDndReason;

typedef struct {
  int32_t    id;
  ETrnStage  stage;
  ETrnPolicy policy;
  int32_t    code;
  int32_t    failedTimes;
  void      *rpcHandle;
  void      *rpcAHandle;
  SArray    *redoLogs;
  SArray    *undoLogs;
  SArray    *commitLogs;
  SArray    *redoActions;
  SArray    *undoActions;
} STrans;

typedef struct {
  int64_t id;
  char    name[TSDB_CLUSTER_ID_LEN];
  int64_t createdTime;
  int64_t updateTime;
} SClusterObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  int64_t    rebootTime;
  int64_t    lastAccessTime;
  int32_t    accessTimes;
  int16_t    numOfMnodes;
  int16_t    numOfVnodes;
  int16_t    numOfQnodes;
  int16_t    numOfSupportMnodes;
  int16_t    numOfSupportVnodes;
  int16_t    numOfSupportQnodes;
  int16_t    numOfCores;
  EDndStatus status;
  EDndReason offlineReason;
  uint16_t   port;
  char       fqdn[TSDB_FQDN_LEN];
  char       ep[TSDB_EP_LEN];
} SDnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  ESyncState role;
  int32_t    roleTerm;
  int64_t    roleTime;
  SDnodeObj *pDnode;
} SMnodeObj;

typedef struct {
  int32_t maxUsers;
  int32_t maxDbs;
  int32_t maxTimeSeries;
  int32_t maxStreams;
  int64_t maxStorage;   // In unit of GB
  int32_t accessState;  // Configured only by command
} SAcctCfg;

typedef struct {
  int32_t numOfUsers;
  int32_t numOfDbs;
  int32_t numOfTimeSeries;
  int32_t numOfStreams;
  int64_t totalStorage;  // Total storage wrtten from this account
  int64_t compStorage;   // Compressed storage on disk
} SAcctInfo;

typedef struct {
  char      acct[TSDB_USER_LEN];
  int64_t   createdTime;
  int64_t   updateTime;
  int32_t   acctId;
  int32_t   status;
  SAcctCfg  cfg;
  SAcctInfo info;
} SAcctObj;

typedef struct {
  char      user[TSDB_USER_LEN];
  char      pass[TSDB_PASSWORD_LEN];
  char      acct[TSDB_USER_LEN];
  int64_t   createdTime;
  int64_t   updateTime;
  int8_t    superUser;
  int32_t   acctId;
  SHashObj *prohibitDbHash;
} SUserObj;

typedef struct {
  int32_t numOfVgroups;
  int32_t cacheBlockSize;
  int32_t totalBlocks;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t minRows;
  int32_t maxRows;
  int32_t commitTime;
  int32_t fsyncPeriod;
  int8_t  walLevel;
  int8_t  precision;
  int8_t  compression;
  int8_t  replications;
  int8_t  quorum;
  int8_t  update;
  int8_t  cacheLastRow;
} SDbCfg;

typedef struct {
  char    name[TSDB_DB_FNAME_LEN];
  char    acct[TSDB_USER_LEN];
  int64_t createdTime;
  int64_t updateTime;
  int64_t uid;
  int32_t cfgVersion;
  int32_t vgVersion;
  int8_t  hashMethod;  // default is 1
  SDbCfg  cfg;
} SDbObj;

typedef struct {
  int32_t    dnodeId;
  ESyncState role;
} SVnodeGid;

typedef struct {
  int32_t   vgId;
  int64_t   createdTime;
  int64_t   updateTime;
  int32_t   version;
  uint32_t  hashBegin;
  uint32_t  hashEnd;
  char      dbName[TSDB_DB_FNAME_LEN];
  int64_t   dbUid;
  int32_t   numOfTables;
  int32_t   numOfTimeSeries;
  int64_t   totalStorage;
  int64_t   compStorage;
  int64_t   pointsWritten;
  int8_t    compact;
  int8_t    replica;
  SVnodeGid vnodeGid[TSDB_MAX_REPLICA];
} SVgObj;

typedef struct {
  char     name[TSDB_TABLE_FNAME_LEN];
  char     db[TSDB_DB_FNAME_LEN];
  int64_t  createdTime;
  int64_t  updateTime;
  uint64_t uid;
  uint64_t dbUid;
  int32_t  version;
  int32_t  numOfColumns;
  int32_t  numOfTags;
  SRWLatch lock;
  SSchema *pSchema;
} SStbObj;

typedef struct {
  char    name[TSDB_FUNC_NAME_LEN];
  int64_t createdTime;
  int8_t  funcType;
  int8_t  scriptType;
  int8_t  align;
  int8_t  outputType;
  int32_t outputLen;
  int32_t bufSize;
  int64_t sigature;
  int32_t commentSize;
  int32_t codeSize;
  char   *pComment;
  char   *pCode;
  char    pData[];
} SFuncObj;

typedef struct {
  int32_t id;
  int8_t  type;
  int8_t  replica;
  int16_t numOfColumns;
  int32_t rowSize;
  int32_t numOfRows;
  int32_t numOfReads;
  int32_t payloadLen;
  void   *pIter;
  SMnode *pMnode;
  char    db[TSDB_DB_FNAME_LEN];
  int16_t offset[TSDB_MAX_COLUMNS];
  int32_t bytes[TSDB_MAX_COLUMNS];
  char    payload[];
} SShowObj;

typedef struct {
  char name[TSDB_TOPIC_FNAME_LEN];
  char db[TSDB_DB_FNAME_LEN];
  int64_t createTime;
  int64_t updateTime;
  uint64_t uid;
  uint64_t dbUid;
  int32_t version;
  SRWLatch lock;
  int32_t  execLen;
  void*    executor;
  int32_t  sqlLen;
  char*    sql;
} STopicObj;

typedef struct SMnodeMsg {
  char    user[TSDB_USER_LEN];
  char    db[TSDB_DB_FNAME_LEN];
  int32_t acctId;
  SMnode *pMnode;
  int64_t createdTime;
  SRpcMsg rpcMsg;
  int32_t contLen;
  void   *pCont;
} SMnodeMsg;

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_DEF_H_*/

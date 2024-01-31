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

#ifndef _TD_MND_INT_H_
#define _TD_MND_INT_H_

#include "mndDef.h"

#include "sdb.h"
#include "sync.h"
#include "tcache.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tgrant.h"
#include "tqueue.h"
#include "ttime.h"
#include "version.h"
#include "wal.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define mFatal(...) { if (mDebugFlag & DEBUG_FATAL) { taosPrintLog("MND FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }}
#define mError(...) { if (mDebugFlag & DEBUG_ERROR) { taosPrintLog("MND ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }}
#define mWarn(...)  { if (mDebugFlag & DEBUG_WARN)  { taosPrintLog("MND WARN ",  DEBUG_WARN,  255,        __VA_ARGS__); }}
#define mInfo(...)  { if (mDebugFlag & DEBUG_INFO)  { taosPrintLog("MND ",       DEBUG_INFO,  255,        __VA_ARGS__); }}
#define mDebug(...) { if (mDebugFlag & DEBUG_DEBUG) { taosPrintLog("MND ",       DEBUG_DEBUG, mDebugFlag, __VA_ARGS__); }}
#define mTrace(...) { if (mDebugFlag & DEBUG_TRACE) { taosPrintLog("MND ",       DEBUG_TRACE, mDebugFlag, __VA_ARGS__); }}

#define mGFatal(param, ...) { if (mDebugFlag & DEBUG_FATAL){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); mFatal(param ", gtid:%s", __VA_ARGS__, buf);}} 
#define mGError(param, ...) { if (mDebugFlag & DEBUG_ERROR){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); mError(param ", gtid:%s", __VA_ARGS__, buf);}}
#define mGWarn(param, ...)  { if (mDebugFlag & DEBUG_WARN){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); mWarn (param ", gtid:%s", __VA_ARGS__, buf);}}
#define mGInfo(param, ...)  { if (mDebugFlag & DEBUG_INFO){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); mInfo (param ", gtid:%s", __VA_ARGS__, buf);}}
#define mGDebug(param, ...) { if (mDebugFlag & DEBUG_DEBUG){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); mDebug(param ", gtid:%s", __VA_ARGS__, buf);}}
#define mGTrace(param, ...) { if (mDebugFlag & DEBUG_TRACE){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); mTrace(param ", gtid:%s", __VA_ARGS__, buf);}}
// clang-format on

#define SYSTABLE_SCH_TABLE_NAME_LEN ((TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_DB_NAME_LEN    ((TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_COL_NAME_LEN   ((TSDB_COL_NAME_LEN - 1) + VARSTR_HEADER_SIZE)

typedef int32_t (*MndMsgFp)(SRpcMsg *pMsg);
typedef int32_t (*MndInitFp)(SMnode *pMnode);
typedef void (*MndCleanupFp)(SMnode *pMnode);
typedef int32_t (*ShowRetrieveFp)(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
typedef void (*ShowFreeIterFp)(SMnode *pMnode, void *pIter);
typedef struct SQueueWorker SQHandle;

typedef struct {
  const char  *name;
  MndInitFp    initFp;
  MndCleanupFp cleanupFp;
} SMnodeStep;

typedef struct {
  int64_t        showId;
  ShowRetrieveFp retrieveFps[TSDB_MGMT_TABLE_MAX];
  ShowFreeIterFp freeIterFps[TSDB_MGMT_TABLE_MAX];
  SCacheObj     *cache;
} SShowMgmt;

typedef struct {
  SCacheObj *connCache;
  SCacheObj *appCache;
} SProfileMgmt;

typedef struct {
  TdThreadMutex lock;
  char          email[TSDB_FQDN_LEN];
} STelemMgmt;

typedef struct {
  tsem_t        syncSem;
  int64_t       sync;
  int32_t       errCode;
  int32_t       transId;
  int32_t       transSec;
  int64_t       transSeq;
  TdThreadMutex lock;
  int8_t        selfIndex;
  int8_t        numOfTotalReplicas;
  int8_t        numOfReplicas;
  SReplica      replicas[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  ESyncRole     nodeRoles[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  SyncIndex     lastIndex;
} SSyncMgmt;

typedef struct {
  int64_t expireTimeMS;
  int64_t timeseriesAllowed;
} SGrantInfo;

typedef struct SMnode {
  int32_t        selfDnodeId;
  int64_t        clusterId;
  TdThread       thread;
  TdThreadRwlock lock;
  int32_t        rpcRef;
  int32_t        syncRef;
  bool           stopped;
  bool           restored;
  bool           deploy;
  char          *path;
  int64_t        checkTime;
  SyncIndex      applied;
  SSdb          *pSdb;
  SArray        *pSteps;
  SQHandle      *pQuery;
  SHashObj      *infosMeta;
  SHashObj      *perfsMeta;
  SWal          *pWal;
  SShowMgmt      showMgmt;
  SProfileMgmt   profileMgmt;
  STelemMgmt     telemMgmt;
  SSyncMgmt      syncMgmt;
  SGrantInfo     grant;
  MndMsgFp       msgFp[TDMT_MAX];
  SMsgCb         msgCb;
  int64_t        ipWhiteVer;
} SMnode;

void    mndSetMsgHandle(SMnode *pMnode, tmsg_t msgType, MndMsgFp fp);
int64_t mndGenerateUid(const char *name, int32_t len);

void mndSetRestored(SMnode *pMnode, bool restored);
bool mndGetRestored(SMnode *pMnode);
void mndSetStop(SMnode *pMnode);
bool mndGetStop(SMnode *pMnode);

SArray *mndGetAllDnodeFqdns(SMnode *pMnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_INT_H_*/

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
#include "tcache.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tqueue.h"
#include "ttime.h"
#include "version.h"
#include "wal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define mFatal(...) { if (mDebugFlag & DEBUG_FATAL) { taosPrintLog("MND FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}
#define mError(...) { if (mDebugFlag & DEBUG_ERROR) { taosPrintLog("MND ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}
#define mWarn(...)  { if (mDebugFlag & DEBUG_WARN)  { taosPrintLog("MND WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}
#define mInfo(...)  { if (mDebugFlag & DEBUG_INFO)  { taosPrintLog("MND ", DEBUG_INFO, 255, __VA_ARGS__); }}
#define mDebug(...) { if (mDebugFlag & DEBUG_DEBUG) { taosPrintLog("MND ", DEBUG_DEBUG, mDebugFlag, __VA_ARGS__); }}
#define mTrace(...) { if (mDebugFlag & DEBUG_TRACE) { taosPrintLog("MND ", DEBUG_TRACE, mDebugFlag, __VA_ARGS__); }}

typedef int32_t (*MndMsgFp)(SNodeMsg *pMsg);
typedef int32_t (*MndInitFp)(SMnode *pMnode);
typedef void (*MndCleanupFp)(SMnode *pMnode);
typedef int32_t (*ShowMetaFp)(SNodeMsg *pMsg, SShowObj *pShow, STableMetaRsp *pMeta);
typedef int32_t (*ShowRetrieveFp)(SNodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
typedef void (*ShowFreeIterFp)(SMnode *pMnode, void *pIter);

typedef struct SMnodeLoad {
  int64_t numOfDnode;
  int64_t numOfMnode;
  int64_t numOfVgroup;
  int64_t numOfDatabase;
  int64_t numOfSuperTable;
  int64_t numOfChildTable;
  int64_t numOfNormalTable;
  int64_t numOfColumn;
  int64_t totalPoints;
  int64_t totalStorage;
  int64_t compStorage;
} SMnodeLoad;

typedef struct {
  const char  *name;
  MndInitFp    initFp;
  MndCleanupFp cleanupFp;
} SMnodeStep;

typedef struct {
  int64_t        showId;
  ShowMetaFp     metaFps[TSDB_MGMT_TABLE_MAX];
  ShowRetrieveFp retrieveFps[TSDB_MGMT_TABLE_MAX];
  ShowFreeIterFp freeIterFps[TSDB_MGMT_TABLE_MAX];
  SCacheObj     *cache;
} SShowMgmt;

typedef struct {
  int32_t    connId;
  SCacheObj *cache;
} SProfileMgmt;

typedef struct {
  bool     enable;
  SRWLatch lock;
  char     email[TSDB_FQDN_LEN];
} STelemMgmt;

typedef struct {
  int32_t    errCode;
  sem_t      syncSem;
  SWal      *pWal;
  SSyncNode *pSyncNode;
  ESyncState state;
} SSyncMgmt;

typedef struct {
  int64_t expireTimeMS;
  int64_t timeseriesAllowed;
} SGrantInfo;

typedef struct SMnode {
  int32_t           dnodeId;
  int64_t           clusterId;
  int8_t            replica;
  int8_t            selfIndex;
  SReplica          replicas[TSDB_MAX_REPLICA];
  tmr_h             timer;
  tmr_h             transTimer;
  tmr_h             mqTimer;
  tmr_h             telemTimer;
  char             *path;
  int64_t           checkTime;
  SSdb             *pSdb;
  SMgmtWrapper     *pWrapper;
  SArray           *pSteps;
  SShowMgmt         showMgmt;
  SProfileMgmt      profileMgmt;
  STelemMgmt        telemMgmt;
  SSyncMgmt         syncMgmt;
  SHashObj         *infosMeta;
  SGrantInfo        grant;
  MndMsgFp          msgFp[TDMT_MAX];
  SMsgCb            msgCb;
} SMnode;

void    mndSetMsgHandle(SMnode *pMnode, tmsg_t msgType, MndMsgFp fp);
int64_t mndGenerateUid(char *name, int32_t len);
void    mndGetLoad(SMnode *pMnode, SMnodeLoad *pLoad);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_INT_H_*/

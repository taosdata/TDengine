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

#ifndef _TD_DND_VNODES_INT_H_
#define _TD_DND_VNODES_INT_H_

#include "dnd.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int32_t openVnodes;
  int32_t totalVnodes;
  int32_t masterNum;
  int64_t numOfSelectReqs;
  int64_t numOfInsertReqs;
  int64_t numOfInsertSuccessReqs;
  int64_t numOfBatchInsertReqs;
  int64_t numOfBatchInsertSuccessReqs;
} SVnodesStat;

typedef struct SVnodesMgmt {
  SHashObj     *hash;
  SRWLatch      latch;
  SVnodesStat   state;
  STfs         *pTfs;
  SQWorkerPool  queryPool;
  SFWorkerPool  fetchPool;
  SWWorkerPool  syncPool;
  SWWorkerPool  writePool;
  const char   *path;
  SMnode       *pMnode;
  SDnode       *pDnode;
  SMgmtWrapper *pWrapper;
  SDnodeWorker  mgmtWorker;
} SVnodesMgmt;

typedef struct {
  int32_t  vgId;
  int32_t  vgVersion;
  int8_t   dropped;
  uint64_t dbUid;
  char     db[TSDB_DB_FNAME_LEN];
  char     path[PATH_MAX + 20];
} SWrapperCfg;

typedef struct {
  int32_t     vgId;
  int32_t     refCount;
  int32_t     vgVersion;
  int8_t      dropped;
  int8_t      accessState;
  uint64_t    dbUid;
  char       *db;
  char       *path;
  SVnode     *pImpl;
  STaosQueue *pWriteQ;
  STaosQueue *pSyncQ;
  STaosQueue *pApplyQ;
  STaosQueue *pQueryQ;
  STaosQueue *pFetchQ;
} SVnodeObj;

typedef struct {
  int32_t      vnodeNum;
  int32_t      opened;
  int32_t      failed;
  int32_t      threadIndex;
  pthread_t    thread;
  SVnodesMgmt *pMgmt;
  SWrapperCfg *pCfgs;
} SVnodeThread;

// interface
void    vmGetMgmtFp(SMgmtWrapper *pWrapper);
void    vmGetVnodeLoads(SMgmtWrapper *pWrapper, SArray *pLoads);
int32_t vmGetTfsMonitorInfo(SMgmtWrapper *pWrapper, SMonDiskInfo *pInfo);
void    vmGetVnodeReqs(SMgmtWrapper *pWrapper, SMonDnodeInfo *pInfo);

// vmInt.h
SVnodeObj *vmAcquireVnode(SVnodesMgmt *pMgmt, int32_t vgId);
void       vmReleaseVnode(SVnodesMgmt *pMgmt, SVnodeObj *pVnode);
int32_t    vmOpenVnode(SVnodesMgmt *pMgmt, SWrapperCfg *pCfg, SVnode *pImpl);
void       vmCloseVnode(SVnodesMgmt *pMgmt, SVnodeObj *pVnode);

int32_t vmProcessMgmtMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_VNODES_INT_H_*/
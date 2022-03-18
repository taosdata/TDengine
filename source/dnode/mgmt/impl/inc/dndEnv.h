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

#ifndef _TD_DND_ENV_H_
#define _TD_DND_ENV_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "dndInt.h"

typedef struct {
  EWorkerType type;
  const char *name;
  int32_t     minNum;
  int32_t     maxNum;
  void       *queueFp;
  SDnode     *pDnode;
  STaosQueue *queue;
  union {
    SQWorkerPool pool;
    SWWorkerPool mpool;
  };
} SDnodeWorker;

typedef struct {
  char *dnode;
  char *mnode;
  char *snode;
  char *bnode;
  char *vnodes;
} SDnodeDir;

typedef struct {
  int32_t      dnodeId;
  int32_t      dropped;
  int64_t      clusterId;
  int64_t      dver;
  int64_t      rebootTime;
  int64_t      updateTime;
  int8_t       statusSent;
  SEpSet       mnodeEpSet;
  char        *file;
  SHashObj    *dnodeHash;
  SArray      *pDnodeEps;
  pthread_t   *threadId;
  SRWLatch     latch;
  SDnodeWorker mgmtWorker;
  SDnodeWorker statusWorker;
} SDnodeMgmt;

typedef struct {
  int32_t      refCount;
  int8_t       deployed;
  int8_t       dropped;
  SMnode      *pMnode;
  SRWLatch     latch;
  SDnodeWorker readWorker;
  SDnodeWorker writeWorker;
  SDnodeWorker syncWorker;
  int8_t       replica;
  int8_t       selfIndex;
  SReplica     replicas[TSDB_MAX_REPLICA];
} SMnodeMgmt;

typedef struct {
  int32_t      refCount;
  int8_t       deployed;
  int8_t       dropped;
  SQnode      *pQnode;
  SRWLatch     latch;
  SDnodeWorker queryWorker;
  SDnodeWorker fetchWorker;
} SQnodeMgmt;

typedef struct {
  int32_t      refCount;
  int8_t       deployed;
  int8_t       dropped;
  int8_t       uniqueWorkerInUse;
  SSnode      *pSnode;
  SRWLatch     latch;
  SArray      *uniqueWorkers;  // SArray<SDnodeWorker*>
  SDnodeWorker sharedWorker;
} SSnodeMgmt;

typedef struct {
  int32_t      refCount;
  int8_t       deployed;
  int8_t       dropped;
  SBnode      *pBnode;
  SRWLatch     latch;
  SDnodeWorker writeWorker;
} SBnodeMgmt;

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

typedef struct {
  SVnodesStat  stat;
  SHashObj    *hash;
  SRWLatch     latch;
  SQWorkerPool queryPool;
  SFWorkerPool fetchPool;
  SWWorkerPool syncPool;
  SWWorkerPool writePool;
} SVnodesMgmt;

typedef struct {
  void    *serverRpc;
  void    *clientRpc;
  DndMsgFp msgFp[TDMT_MAX];
} STransMgmt;

typedef struct SDnode {
  EStat        stat;
  SDnodeObjCfg cfg;
  SDnodeDir    dir;
  TdFilePtr    pLockFile;
  SDnodeMgmt   dmgmt;
  SMnodeMgmt   mmgmt;
  SQnodeMgmt   qmgmt;
  SSnodeMgmt   smgmt;
  SBnodeMgmt   bmgmt;
  SVnodesMgmt  vmgmt;
  STransMgmt   tmgmt;
  STfs        *pTfs;
  SStartupReq  startup;
} SDnode;

int32_t dndGetMonitorDiskInfo(SDnode *pDnode, SMonDiskInfo *pInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_ENV_H_*/

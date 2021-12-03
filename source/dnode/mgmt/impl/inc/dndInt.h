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

#ifndef _TD_DND_INT_H_
#define _TD_DND_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "cJSON.h"
#include "os.h"
#include "taosmsg.h"
#include "thash.h"
#include "tlockfree.h"
#include "tlog.h"
#include "tqueue.h"
#include "trpc.h"
#include "tthread.h"
#include "ttime.h"
#include "tworker.h"

#include "dnode.h"
#include "mnode.h"
#include "vnode.h"

extern int32_t dDebugFlag;

#define dFatal(...) { if (dDebugFlag & DEBUG_FATAL) { taosPrintLog("DND FATAL ", 255, __VA_ARGS__); }}
#define dError(...) { if (dDebugFlag & DEBUG_ERROR) { taosPrintLog("DND ERROR ", 255, __VA_ARGS__); }}
#define dWarn(...)  { if (dDebugFlag & DEBUG_WARN)  { taosPrintLog("DND WARN ", 255, __VA_ARGS__); }}
#define dInfo(...)  { if (dDebugFlag & DEBUG_INFO)  { taosPrintLog("DND ", 255, __VA_ARGS__); }}
#define dDebug(...) { if (dDebugFlag & DEBUG_DEBUG) { taosPrintLog("DND ", dDebugFlag, __VA_ARGS__); }}
#define dTrace(...) { if (dDebugFlag & DEBUG_TRACE) { taosPrintLog("DND ", dDebugFlag, __VA_ARGS__); }}

typedef enum { DND_STAT_INIT, DND_STAT_RUNNING, DND_STAT_STOPPED } EStat;
typedef void (*DndMsgFp)(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEps);

typedef struct {
  char *dnode;
  char *mnode;
  char *vnodes;
} SDnodeDir;

typedef struct {
  int32_t    dnodeId;
  int32_t    dropped;
  int32_t    clusterId;
  uint32_t   rebootTime;
  SEpSet     mnodeEpSet;
  char      *file;
  SHashObj  *dnodeHash;
  SDnodeEps *dnodeEps;
  pthread_t *threadId;
  SRWLatch   latch;
} SDnodeMgmt;

typedef struct {
  int32_t     refCount;
  int8_t      deployed;
  int8_t      dropped;
  int8_t      replica;
  int8_t      selfIndex;
  SReplica    replicas[TSDB_MAX_REPLICA];
  char       *file;
  SMnode     *pMnode;
  SRWLatch    latch;
  taos_queue  pReadQ;
  taos_queue  pWriteQ;
  taos_queue  pApplyQ;
  taos_queue  pSyncQ;
  taos_queue  pMgmtQ;
  SWorkerPool mgmtPool;
  SWorkerPool readPool;
  SWorkerPool writePool;
  SWorkerPool syncPool;
} SMnodeMgmt;

typedef struct {
  SHashObj    *hash;
  int32_t      openVnodes;
  int32_t      totalVnodes;
  SRWLatch     latch;
  taos_queue   pMgmtQ;
  SWorkerPool  mgmtPool;
  SWorkerPool  queryPool;
  SWorkerPool  fetchPool;
  SMWorkerPool syncPool;
  SMWorkerPool writePool;
} SVnodesMgmt;

typedef struct {
  void    *serverRpc;
  void    *clientRpc;
  DndMsgFp msgFp[TSDB_MSG_TYPE_MAX];
} STransMgmt;

typedef struct SDnode {
  EStat       stat;
  SDnodeOpt   opt;
  SDnodeDir   dir;
  SDnodeMgmt  dmgmt;
  SMnodeMgmt  mmgmt;
  SVnodesMgmt vmgmt;
  STransMgmt  tmgmt;
  SStartupMsg startup;
} SDnode;

EStat dndGetStat(SDnode *pDnode);
void  dndSetStat(SDnode *pDnode, EStat stat);
char *dndStatStr(EStat stat);

void dndReportStartup(SDnode *pDnode, char *pName, char *pDesc);
void dndGetStartup(SDnode *pDnode, SStartupMsg *pStartup);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_INT_H_*/
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

#include "os.h"

#include "cJSON.h"
#include "monitor.h"
#include "tcache.h"
#include "tcrc32c.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "thash.h"
#include "tlockfree.h"
#include "tlog.h"
#include "tmsg.h"
#include "tprocess.h"
#include "tqueue.h"
#include "trpc.h"
#include "tthread.h"
#include "ttime.h"
#include "tworker.h"

#include "dnode.h"

#include "bnode.h"
#include "mnode.h"
#include "qnode.h"
#include "snode.h"
#include "tfs.h"
#include "vnode.h"

#define dFatal(...) { if (dDebugFlag & DEBUG_FATAL) { taosPrintLog("DND FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}
#define dError(...) { if (dDebugFlag & DEBUG_ERROR) { taosPrintLog("DND ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}
#define dWarn(...)  { if (dDebugFlag & DEBUG_WARN)  { taosPrintLog("DND WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}
#define dInfo(...)  { if (dDebugFlag & DEBUG_INFO)  { taosPrintLog("DND ", DEBUG_INFO, 255, __VA_ARGS__); }}
#define dDebug(...) { if (dDebugFlag & DEBUG_DEBUG) { taosPrintLog("DND ", DEBUG_DEBUG, dDebugFlag, __VA_ARGS__); }}
#define dTrace(...) { if (dDebugFlag & DEBUG_TRACE) { taosPrintLog("DND ", DEBUG_TRACE, dDebugFlag, __VA_ARGS__); }}

typedef enum { MNODE, NODE_MAX, VNODES, QNODE, SNODE, BNODE } ENodeType;
typedef enum { PROC_SINGLE, PROC_CHILD, PROC_PARENT } EProcType;
typedef enum { DND_STAT_INIT, DND_STAT_RUNNING, DND_STAT_STOPPED } EDndStatus;
typedef enum { DND_WORKER_SINGLE, DND_WORKER_MULTI } EWorkerType;
typedef enum { DND_ENV_INIT, DND_ENV_READY, DND_ENV_CLEANUP } EEnvStat;

typedef struct SMgmtFp      SMgmtFp;
typedef struct SMgmtWrapper SMgmtWrapper;

typedef void (*DndMsgFp)(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEps);
typedef int32_t (*MndMsgFp)(SDnode *pDnode, SMndMsg *pMsg);
typedef SMgmtWrapper *(*MgmtOpenFp)(SDnode *pDnode, const char *path);
typedef void (*MgmtCloseFp)(SDnode *pDnode, SMgmtWrapper *pMgmt);
typedef bool (*MgmtRequiredFp)(SMgmtWrapper *pMgmt);
typedef SArray *(*MgmtMsgFp)(SMgmtWrapper *pNode, SNodeMsg *pMsg);

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
  int32_t      dnodeId;
  int32_t      dropped;
  int64_t      clusterId;
  int64_t      dver;
  int64_t      rebootTime;
  int64_t      updateTime;
  int8_t       statusSent;
  SEpSet       mnodeEpSet;
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

  //
  MndMsgFp  msgFp[TDMT_MAX];
  SProcObj *pProcess;
  bool      singleProc;
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
  SSnode      *pSnode;
  SRWLatch     latch;
  SDnodeWorker writeWorker;
} SSnodeMgmt;

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
  int32_t      refCount;
  int8_t       deployed;
  int8_t       dropped;
  SBnode      *pBnode;
  SRWLatch     latch;
  SDnodeWorker writeWorker;
} SBnodeMgmt;

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

typedef struct SMgmtFp {
  MgmtOpenFp     openFp;
  MgmtCloseFp    closeFp;
  MgmtRequiredFp requiredFp;
  MgmtMsgFp      msgFp;
} SMgmtFp;

typedef struct SMgmtWrapper {
  const char *name;
  char       *path;
  bool        required;
  EProcType   procType;
  SProcObj   *pProc;
  void       *pMgmt;
  SMgmtFp     fp;
  SDnode     *pDnode;
} SMgmtWrapper;

typedef struct SDnode {
  EDndStatus   status;
  EDndEvent    event;
  EProcType    procType;
  SDndCfg      cfg;
  SStartupReq  startup;
  TdFilePtr    pLockFile;
  SDnodeMgmt   dmgmt;
  STransMgmt   tmgmt;
  STfs        *pTfs;
  SMgmtFp      fps[NODE_MAX];
  SMgmtWrapper mgmts[NODE_MAX];
  char        *path;
} SDnode;

EDndStatus  dndGetStatus(SDnode *pDnode);
void        dndSetStatus(SDnode *pDnode, EDndStatus stat);
const char *dndStatStr(EDndStatus stat);
void        dndReportStartup(SDnode *pDnode, char *pName, char *pDesc);
void        dndGetStartup(SDnode *pDnode, SStartupReq *pStartup);
TdFilePtr   dndCheckRunning(char *dataDir);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_INT_H_*/
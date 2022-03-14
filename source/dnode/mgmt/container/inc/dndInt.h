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

typedef enum { DNODE, MNODE, NODE_MAX, VNODES, QNODE, SNODE, BNODE } ENodeType;
typedef enum { PROC_SINGLE, PROC_CHILD, PROC_PARENT } EProcType;
typedef enum { DND_STAT_INIT, DND_STAT_RUNNING, DND_STAT_STOPPED } EDndStatus;
typedef enum { DND_WORKER_SINGLE, DND_WORKER_MULTI } EWorkerType;
typedef enum { DND_ENV_INIT, DND_ENV_READY, DND_ENV_CLEANUP } EEnvStat;

typedef struct SDnodeMgmt SDnodeMgmt;

typedef struct SMgmtFp      SMgmtFp;
typedef struct SMgmtWrapper SMgmtWrapper;
typedef struct SMsgHandle   SMsgHandle;

typedef void (*RpcMsgFp)(SDnode *pDnode, SMgmtWrapper *pWrapper, SRpcMsg *pMsg, SEpSet *pEps);
typedef void (*NodeMsgFp)(SDnode *pDnode, SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
typedef int32_t (*OpenNodeFp)(SMgmtWrapper *pWrapper);
typedef void (*CloseNodeFp)(SMgmtWrapper *pWrapper);
typedef bool (*RequireNodeFp)(SMgmtWrapper *pWrapper);
typedef SMsgHandle (*GetMsgHandleFp)(SMgmtWrapper *pWrapper, int32_t msgIndex);

typedef struct SMsgHandle {
  RpcMsgFp      rpcMsgFp;
  NodeMsgFp     nodeMsgFp;
  SMgmtWrapper *pWrapper;
} SMsgHandle;

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
  SMsgHandle  msgHandles[TDMT_MAX];
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

  //
  SMsgHandle msgHandles[TDMT_MAX];
  SProcObj  *pProcess;
  bool       singleProc;
} SQnodeMgmt;

typedef struct {
  int32_t      refCount;
  int8_t       deployed;
  int8_t       dropped;
  SSnode      *pSnode;
  SRWLatch     latch;
  SDnodeWorker writeWorker;

    //
  SMsgHandle msgHandles[TDMT_MAX];
  SProcObj  *pProcess;
  bool       singleProc;
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

    //
  SMsgHandle msgHandles[TDMT_MAX];
  SProcObj  *pProcess;
  bool       singleProc;
} SBnodeMgmt;

typedef struct {
  SVnodesStat  stat;
  SHashObj    *hash;
  SRWLatch     latch;
  SQWorkerPool queryPool;
  SFWorkerPool fetchPool;
  SWWorkerPool syncPool;
  SWWorkerPool writePool;

    //
  SMsgHandle msgHandles[TDMT_MAX];
  SProcObj  *pProcess;
  bool       singleProc;
} SVnodesMgmt;

typedef struct {
  void          *serverRpc;
  void          *clientRpc;
  SMsgHandle msgHandles[TDMT_MAX];
} STransMgmt;

typedef struct SMgmtFp {
  OpenNodeFp     openFp;
  CloseNodeFp    closeFp;
  RequireNodeFp  requiredFp;
  GetMsgHandleFp getMsgHandleFp;
} SMgmtFp;

typedef struct SMgmtWrapper {
  const char *name;
  char       *path;
  bool        required;
  EProcType   procType;
  SProcObj   *pProc;
  void       *pMgmt;
  SDnode     *pDnode;
  SMgmtFp     fp;
} SMgmtWrapper;

typedef struct SDnode {
  EDndStatus   status;
  EDndEvent    event;
  EProcType    procType;
  SDndCfg      cfg;
  SStartupReq  startup;
  TdFilePtr    pLockFile;
  STransMgmt   tmgmt;
  STfs        *pTfs;
  SMgmtFp      fps[NODE_MAX];
  SMgmtWrapper wrappers[NODE_MAX];
} SDnode;

EDndStatus  dndGetStatus(SDnode *pDnode);
void        dndSetStatus(SDnode *pDnode, EDndStatus stat);
const char *dndStatStr(EDndStatus stat);
void        dndReportStartup(SDnode *pDnode, char *pName, char *pDesc);
void        dndGetStartup(SDnode *pDnode, SStartupReq *pStartup);
TdFilePtr   dndCheckRunning(char *dataDir);

SMgmtWrapper *dndGetWrapper(SDnode *pDnode, ENodeType nodeType) ;

void dndProcessRpcMsg(SDnode *pDnode, SMgmtWrapper *pWrapper, SRpcMsg *pMsg, SEpSet *pEpSet);

SMgmtFp dmGetMgmtFp();

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_INT_H_*/
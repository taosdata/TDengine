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
#include "monitor.h"

#ifdef __cplusplus
extern "C" {
#endif

#define dFatal(...) { if (dDebugFlag & DEBUG_FATAL) { taosPrintLog("DND FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}
#define dError(...) { if (dDebugFlag & DEBUG_ERROR) { taosPrintLog("DND ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}
#define dWarn(...)  { if (dDebugFlag & DEBUG_WARN)  { taosPrintLog("DND WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}
#define dInfo(...)  { if (dDebugFlag & DEBUG_INFO)  { taosPrintLog("DND ", DEBUG_INFO, 255, __VA_ARGS__); }}
#define dDebug(...) { if (dDebugFlag & DEBUG_DEBUG) { taosPrintLog("DND ", DEBUG_DEBUG, dDebugFlag, __VA_ARGS__); }}
#define dTrace(...) { if (dDebugFlag & DEBUG_TRACE) { taosPrintLog("DND ", DEBUG_TRACE, dDebugFlag, __VA_ARGS__); }}

typedef enum { DNODE, VNODES, QNODE, SNODE, MNODE, BNODE, NODE_MAX } ENodeType;
typedef enum { PROC_SINGLE, PROC_CHILD, PROC_PARENT } EProcType;
typedef enum { DND_STAT_INIT, DND_STAT_RUNNING, DND_STAT_STOPPED } EDndStatus;
typedef enum { DND_WORKER_SINGLE, DND_WORKER_MULTI } EWorkerType;
typedef enum { DND_ENV_INIT, DND_ENV_READY, DND_ENV_CLEANUP } EEnvStat;

typedef struct SMgmtFp      SMgmtFp;
typedef struct SMgmtWrapper SMgmtWrapper;
typedef struct SMsgHandle   SMsgHandle;
typedef struct SDnodeMgmt   SDnodeMgmt;
typedef struct SVnodesMgmt  SVnodesMgmt;
typedef struct SMnodeMgmt   SMnodeMgmt;
typedef struct SQnodeMgmt   SQnodeMgmt;
typedef struct SSnodeMgmt   SSnodeMgmt;
typedef struct SBnodeMgmt   SBnodeMgmt;

typedef int32_t (*NodeMsgFp)(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
typedef int32_t (*OpenNodeFp)(SMgmtWrapper *pWrapper);
typedef void (*CloseNodeFp)(SMgmtWrapper *pWrapper);
typedef bool (*RequireNodeFp)(SMgmtWrapper *pWrapper);

typedef struct {
  EWorkerType type;
  const char *name;
  int32_t     minNum;
  int32_t     maxNum;
  void       *queueFp;
  void       *param;
  STaosQueue *queue;
  union {
    SQWorkerPool pool;
    SWWorkerPool mpool;
  };
} SDnodeWorker;

typedef struct SMsgHandle {
  NodeMsgFp     msgFp;
  SMgmtWrapper *pWrapper;
} SMsgHandle;

typedef struct SMgmtFp {
  OpenNodeFp    openFp;
  CloseNodeFp   closeFp;
  RequireNodeFp requiredFp;
} SMgmtFp;

typedef struct SMgmtWrapper {
  const char *name;
  char       *path;
  bool        required;
  EProcType   procType;
  SProcObj   *pProc;
  void       *pMgmt;
  SDnode     *pDnode;
  NodeMsgFp   msgFps[TDMT_MAX];
  SMgmtFp     fp;
} SMgmtWrapper;

typedef struct {
  void      *serverRpc;
  void      *clientRpc;
  SMsgHandle msgHandles[TDMT_MAX];
} STransMgmt;

typedef struct SDnode {
  int64_t      rebootTime;
  EDndStatus   status;
  EDndEvent    event;
  EProcType    procType;
  SDndCfg      cfg;
  SStartupReq  startup;
  TdFilePtr    pLockFile;
  STransMgmt   trans;
  SMgmtWrapper wrappers[NODE_MAX];
} SDnode;

// dndInt.h
int32_t       dndInit();
void          dndCleanup();
EDndStatus    dndGetStatus(SDnode *pDnode);
void          dndSetStatus(SDnode *pDnode, EDndStatus stat);
const char   *dndStatStr(EDndStatus stat);
void          dndReportStartup(SDnode *pDnode, char *pName, char *pDesc);
void          dndGetStartup(SDnode *pDnode, SStartupReq *pStartup);
TdFilePtr     dndCheckRunning(char *dataDir);
SMgmtWrapper *dndGetWrapper(SDnode *pDnode, ENodeType nodeType);
void          dndSetMsgHandle(SMgmtWrapper *pWrapper, int32_t msgType, NodeMsgFp nodeMsgFp);
void          dndProcessStartupReq(SDnode *pDnode, SRpcMsg *pMsg);

// dndMonitor.h
void dndSendMonitorReport(SDnode *pDnode);

// dndNode.h
SDnode *dndCreate(SDndCfg *pCfg);
void    dndClose(SDnode *pDnode);
int32_t dndRun(SDnode *pDnode);
void    dndeHandleEvent(SDnode *pDnode, EDndEvent event);
void    dndProcessRpcMsg(SMgmtWrapper *pWrapper, SRpcMsg *pMsg, SEpSet *pEpSet);
void    dndSendRsp(SMgmtWrapper *pWrapper, SRpcMsg *pRsp);

// dndTransport.h
int32_t dndSendReqToMnode(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t dndSendReqToDnode(SDnode *pDnode, SEpSet *pEpSet, SRpcMsg *pRpcMsg);

// dndWorker.h
int32_t dndInitWorker(void *param, SDnodeWorker *pWorker, EWorkerType type, const char *name, int32_t minNum,
                      int32_t maxNum, void *queueFp);
void    dndCleanupWorker(SDnodeWorker *pWorker);
int32_t dndWriteMsgToWorker(SDnodeWorker *pWorker, void *pCont, int32_t contLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_INT_H_*/
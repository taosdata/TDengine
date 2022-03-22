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

#ifndef _TD_DND_H_
#define _TD_DND_H_

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
#include "tfs.h"
#include "wal.h"

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
typedef enum { DND_STAT_INIT, DND_STAT_RUNNING, DND_STAT_STOPPED } EDndStatus;
typedef enum { DND_ENV_INIT, DND_ENV_READY, DND_ENV_CLEANUP } EEnvStatus;
typedef enum { DND_WORKER_SINGLE, DND_WORKER_MULTI } EWorkerType;
typedef enum { PROC_SINGLE, PROC_CHILD, PROC_PARENT } EProcType;

typedef struct SMgmtFp      SMgmtFp;
typedef struct SMgmtWrapper SMgmtWrapper;
typedef struct SMsgHandle   SMsgHandle;
typedef struct SDnodeMgmt   SDnodeMgmt;
typedef struct SVnodesMgmt  SVnodesMgmt;
typedef struct SMnodeMgmt   SMnodeMgmt;
typedef struct SQnodeMgmt   SQnodeMgmt;
typedef struct SSnodeMgmt   SSnodeMgmt;
typedef struct SBnodeMgmt   SBnodeMgmt;

typedef int32_t (*NodeMsgFp)(void *pMgmt, SNodeMsg *pMsg);
typedef int32_t (*OpenNodeFp)(SMgmtWrapper *pWrapper);
typedef void (*CloseNodeFp)(SMgmtWrapper *pWrapper);
typedef int32_t (*StartNodeFp)(SMgmtWrapper *pWrapper);
typedef int32_t (*CreateNodeFp)(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
typedef int32_t (*DropNodeFp)(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
typedef int32_t (*RequireNodeFp)(SMgmtWrapper *pWrapper, bool *required);

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
  int32_t       vgId;
  NodeMsgFp     vgIdMsgFp;
  SMgmtWrapper *pVgIdWrapper;  // Handle the case where the same message type is distributed to qnode or vnode
  NodeMsgFp     msgFp;
  SMgmtWrapper *pWrapper;
} SMsgHandle;

typedef struct SMgmtFp {
  OpenNodeFp    openFp;
  CloseNodeFp   closeFp;
  StartNodeFp   startFp;
  CreateNodeFp  createMsgFp;
  DropNodeFp    dropMsgFp;
  RequireNodeFp requiredFp;
} SMgmtFp;

typedef struct SMgmtWrapper {
  const char *name;
  char       *path;
  int32_t     refCount;
  SRWLatch    latch;
  bool        deployed;
  bool        required;
  EProcType   procType;
  SProcObj   *pProc;
  void       *pMgmt;
  SDnode     *pDnode;
  NodeMsgFp   msgFps[TDMT_MAX];
  int32_t     msgVgIds[TDMT_MAX];  // Handle the case where the same message type is distributed to qnode or vnode
  SMgmtFp     fp;
} SMgmtWrapper;

typedef struct {
  void      *serverRpc;
  void      *clientRpc;
  SMsgHandle msgHandles[TDMT_MAX];
} STransMgmt;

typedef struct SDnode {
  int64_t      clusterId;
  int32_t      dnodeId;
  int32_t      numOfSupportVnodes;
  int64_t      rebootTime;
  char        *localEp;
  char        *localFqdn;
  char        *firstEp;
  char        *secondEp;
  char        *dataDir;
  SDiskCfg    *pDisks;
  int32_t      numOfDisks;
  uint16_t     serverPort;
  bool         dropped;
  EDndStatus   status;
  EDndEvent    event;
  EProcType    procType;
  SStartupReq  startup;
  TdFilePtr    pLockFile;
  STransMgmt   trans;
  SMgmtWrapper wrappers[NODE_MAX];
} SDnode;

EDndStatus    dndGetStatus(SDnode *pDnode);
void          dndSetStatus(SDnode *pDnode, EDndStatus stat);
SMgmtWrapper *dndAcquireWrapper(SDnode *pDnode, ENodeType nodeType);
void          dndSetMsgHandle(SMgmtWrapper *pWrapper, int32_t msgType, NodeMsgFp nodeMsgFp, int32_t vgId);
void          dndReportStartup(SDnode *pDnode, char *pName, char *pDesc);
void          dndSendMonitorReport(SDnode *pDnode);

int32_t dndSendReqToMnode(SMgmtWrapper *pWrapper, SRpcMsg *pMsg);
int32_t dndSendReqToDnode(SMgmtWrapper *pWrapper, SEpSet *pEpSet, SRpcMsg *pMsg);
void    dndSendRsp(SMgmtWrapper *pWrapper, SRpcMsg *pRsp);

int32_t dndInitWorker(void *param, SDnodeWorker *pWorker, EWorkerType type, const char *name, int32_t minNum,
                      int32_t maxNum, void *queueFp);
void    dndCleanupWorker(SDnodeWorker *pWorker);
int32_t dndWriteMsgToWorker(SDnodeWorker *pWorker, void *pMsg);

int32_t dndProcessNodeMsg(SDnode *pDnode, SNodeMsg *pMsg);

int32_t dndReadFile(SMgmtWrapper *pWrapper, bool *pDeployed);
int32_t dndWriteFile(SMgmtWrapper *pWrapper, bool deployed);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_H_*/
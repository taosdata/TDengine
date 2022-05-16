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

#ifndef _TD_DND_IMP_H_
#define _TD_DND_IMP_H_

// tobe deleted
#include "uv.h"

#include "dmInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMgmtWrapper SMgmtWrapper;

#define SINGLE_PROC             0
#define CHILD_PROC              1
#define PARENT_PROC             2
#define TEST_PROC               3
#define OnlyInSingleProc(ptype) (ptype == SINGLE_PROC)
#define OnlyInChildProc(ptype)  (ptype == CHILD_PROC)
#define OnlyInParentProc(ptype) (ptype == PARENT_PROC)
#define OnlyInTestProc(ptype)   (ptype == TEST_PROC)
#define InChildProc(ptype)      (ptype & CHILD_PROC)
#define InParentProc(ptype)     (ptype & PARENT_PROC)

typedef struct {
  int32_t       head;
  int32_t       tail;
  int32_t       total;
  int32_t       avail;
  int32_t       items;
  char          name[8];
  TdThreadMutex mutex;
  tsem_t        sem;
  char          pBuffer[];
} SProcQueue;

typedef struct {
  SMgmtWrapper *wrapper;
  const char   *name;
  SHashObj     *hash;
  SProcQueue   *pqueue;
  SProcQueue   *cqueue;
  TdThread      pthread;
  TdThread      cthread;
  SShm          shm;
  int32_t       pid;
  EDndProcType  ptype;
  bool          stop;
} SProc;

typedef struct SMgmtWrapper {
  SDnode      *pDnode;
  SMgmtFunc    func;
  void        *pMgmt;
  const char  *name;
  char        *path;
  int32_t      refCount;
  SRWLatch     latch;
  EDndNodeType ntype;
  bool         deployed;
  bool         required;
  SProc        proc;
  NodeMsgFp    msgFps[TDMT_MAX];
} SMgmtWrapper;

typedef struct {
  EDndNodeType defaultNtype;
  bool         needCheckVgId;
} SMsgHandle;

typedef struct {
  void      *serverRpc;
  void      *clientRpc;
  SMsgHandle msgHandles[TDMT_MAX];
} SDnodeTrans;

typedef struct {
  char name[TSDB_STEP_NAME_LEN];
  char desc[TSDB_STEP_DESC_LEN];
} SStartupInfo;

typedef struct SUdfdData {
  bool         startCalled;
  bool         needCleanUp;
  uv_loop_t    loop;
  uv_thread_t  thread;
  uv_barrier_t barrier;
  uv_process_t process;
  int          spawnErr;
  uv_pipe_t    ctrlPipe;
  uv_async_t   stopAsync;
  int32_t      stopCalled;
  int32_t      dnodeId;
} SUdfdData;

typedef struct SDnode {
  EDndProcType  ptype;
  EDndNodeType  rtype;
  EDndEvent     event;
  EDndRunStatus status;
  SStartupInfo  startup;
  SDnodeTrans   trans;
  SUdfdData     udfdData;
  TdThreadMutex mutex;
  TdFilePtr     lockfile;
  SDnodeData    data;
  SMgmtWrapper  wrappers[NODE_END];
} SDnode;

// dmExec.c
int32_t dmOpenNode(SMgmtWrapper *pWrapper);
void    dmCloseNode(SMgmtWrapper *pWrapper);

// dmObj.c
SMgmtWrapper *dmAcquireWrapper(SDnode *pDnode, EDndNodeType nType);
int32_t       dmMarkWrapper(SMgmtWrapper *pWrapper);
void          dmReleaseWrapper(SMgmtWrapper *pWrapper);
SMgmtInputOpt dmBuildMgmtInputOpt(SMgmtWrapper *pWrapper);

void dmSetStatus(SDnode *pDnode, EDndRunStatus stype);
void dmSetEvent(SDnode *pDnode, EDndEvent event);
void dmReportStartup(SDnode *pDnode, const char *pName, const char *pDesc);
void dmReportStartupByWrapper(SMgmtWrapper *pWrapper, const char *pName, const char *pDesc);
void dmProcessServerStartupStatus(SDnode *pDnode, SRpcMsg *pMsg);
void dmProcessNetTestReq(SDnode *pDnode, SRpcMsg *pMsg);

// dmProc.c
int32_t dmInitProc(struct SMgmtWrapper *pWrapper);
void    dmCleanupProc(struct SMgmtWrapper *pWrapper);
int32_t dmRunProc(SProc *proc);
void    dmStopProc(SProc *proc);
int64_t dmRemoveProcRpcHandle(SProc *proc, void *handle);
void    dmCloseProcRpcHandles(SProc *proc);
int32_t dmPutToProcCQueue(SProc *proc, const void *pHead, int16_t headLen, const void *pBody, int32_t bodyLen,
                          void *handle, int64_t handleRef, EProcFuncType ftype);
void    dmPutToProcPQueue(SProc *proc, const void *pHead, int16_t headLen, const void *pBody, int32_t bodyLen,
                          EProcFuncType ftype);

// dmTransport.c
int32_t dmInitServer(SDnode *pDnode);
void    dmCleanupServer(SDnode *pDnode);
int32_t dmInitClient(SDnode *pDnode);
void    dmCleanupClient(SDnode *pDnode);
SMsgCb  dmGetMsgcb(SMgmtWrapper *pWrapper);
int32_t dmInitMsgHandle(SDnode *pDnode);
int32_t dmProcessNodeMsg(SMgmtWrapper *pWrapper, SRpcMsg *pMsg);

// mgmt nodes
SMgmtFunc dmGetMgmtFunc();
SMgmtFunc bmGetMgmtFunc();
SMgmtFunc qmGetMgmtFunc();
SMgmtFunc smGetMgmtFunc();
SMgmtFunc vmGetMgmtFunc();
SMgmtFunc mmGetMgmtFunc();

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_IMP_H_*/
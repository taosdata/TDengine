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

#include "dmUtil.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMgmtWrapper {
  SDnode      *pDnode;
  SMgmtFunc    func;
  void        *pMgmt;
  const char  *name;
  char        *path;
  int32_t      refCount;
  SRWLatch     latch;
  EDndNodeType nodeType;
  bool         deployed;
  bool         required;
  EDndProcType procType;
  int32_t      procId;
  SProcObj    *procObj;
  SShm         procShm;
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
  EDndNodeType  ntype;
  EDndEvent     event;
  EDndRunStatus status;
  SStartupInfo  startup;
  SDnodeTrans   trans;
  SUdfdData     udfdData;
  TdThreadMutex mutex;
  SRWLatch      latch;
  SEpSet        mnodeEps;
  TdFilePtr     lockfile;
  SMgmtInputOpt input;
  SMgmtWrapper  wrappers[NODE_END];
} SDnode;

// dmExec.c
int32_t dmOpenNode(SMgmtWrapper *pWrapper);
void    dmCloseNode(SMgmtWrapper *pWrapper);

// dmObj.c
SMgmtWrapper *dmAcquireWrapper(SDnode *pDnode, EDndNodeType nType);
int32_t       dmMarkWrapper(SMgmtWrapper *pWrapper);
void          dmReleaseWrapper(SMgmtWrapper *pWrapper);

void dmSetStatus(SDnode *pDnode, EDndRunStatus stype);
void dmSetEvent(SDnode *pDnode, EDndEvent event);
void dmReportStartup(SDnode *pDnode, const char *pName, const char *pDesc);
void dmReportStartupByWrapper(SMgmtWrapper *pWrapper, const char *pName, const char *pDesc);

void    dmProcessServerStatusReq(SDnode *pDnode, SRpcMsg *pMsg);
void    dmProcessNetTestReq(SDnode *pDnode, SRpcMsg *pMsg);
int32_t dmProcessCreateNodeReq(SDnode *pDnode, EDndNodeType ntype, SNodeMsg *pMsg);
int32_t dmProcessDropNodeReq(SDnode *pDnode, EDndNodeType ntype, SNodeMsg *pMsg);

// dmTransport.c
int32_t  dmInitServer(SDnode *pDnode);
void     dmCleanupServer(SDnode *pDnode);
int32_t  dmInitClient(SDnode *pDnode);
void     dmCleanupClient(SDnode *pDnode);
SProcCfg dmGenProcCfg(SMgmtWrapper *pWrapper);
SMsgCb   dmGetMsgcb(SMgmtWrapper *pWrapper);
int32_t  dmInitMsgHandle(SDnode *pDnode);
void     dmSendRecv(SDnode *pDnode, SEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp);
void     dmSendToMnodeRecv(SDnode *pDnode, SRpcMsg *pReq, SRpcMsg *pRsp);

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
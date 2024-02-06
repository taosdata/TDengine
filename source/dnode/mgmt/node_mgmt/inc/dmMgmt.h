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

#ifndef _TD_DND_MGMT_H_
#define _TD_DND_MGMT_H_

// tobe deleted
#include "uv.h"

#include "dmInt.h"
#include "tfs.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMgmtWrapper {
  SMgmtFunc      func;
  struct SDnode *pDnode;
  void          *pMgmt;
  const char    *name;
  char          *path;
  int32_t        refCount;
  TdThreadRwlock lock;
  EDndNodeType   ntype;
  bool           deployed;
  bool           required;
  NodeMsgFp      msgFps[TDMT_MAX];
} SMgmtWrapper;

typedef struct {
  EDndNodeType defaultNtype;
  bool         needCheckVgId;
} SDnodeHandle;

typedef struct {
  void        *serverRpc;
  void        *clientRpc;
  void        *statusRpc;
  void        *syncRpc;
  SDnodeHandle msgHandles[TDMT_MAX];
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
  int8_t        once;
  bool          stop;
  EDndRunStatus status;
  SStartupInfo  startup;
  SDnodeData    data;
  SUdfdData     udfdData;
  TdThreadMutex mutex;
  TdFilePtr     lockfile;
  STfs         *pTfs;
  SMgmtWrapper  wrappers[NODE_END];
  SDnodeTrans   trans;
} SDnode;

// dmEnv.c
SDnode *dmInstance();
void    dmReportStartup(const char *pName, const char *pDesc);
int64_t dmGetClusterId();

// dmMgmt.c
int32_t       dmInitDnode(SDnode *pDnode);
void          dmCleanupDnode(SDnode *pDnode);
SMgmtWrapper *dmAcquireWrapper(SDnode *pDnode, EDndNodeType nType);
int32_t       dmMarkWrapper(SMgmtWrapper *pWrapper);
void          dmReleaseWrapper(SMgmtWrapper *pWrapper);
int32_t       dmInitVars(SDnode *pDnode);
void          dmClearVars(SDnode *pDnode);
int32_t       dmInitModule(SDnode *pDnode);
SMgmtInputOpt dmBuildMgmtInputOpt(SMgmtWrapper *pWrapper);
void          dmSetStatus(SDnode *pDnode, EDndRunStatus stype);
void          dmProcessServerStartupStatus(SDnode *pDnode, SRpcMsg *pMsg);
void          dmProcessNetTestReq(SDnode *pDnode, SRpcMsg *pMsg);

// dmNodes.c
int32_t dmOpenNode(SMgmtWrapper *pWrapper);
int32_t dmStartNode(SMgmtWrapper *pWrapper);
void    dmStopNode(SMgmtWrapper *pWrapper);
void    dmCloseNode(SMgmtWrapper *pWrapper);
int32_t dmRunDnode(SDnode *pDnode);

// dmTransport.c
int32_t dmInitServer(SDnode *pDnode);
void    dmCleanupServer(SDnode *pDnode);
int32_t dmInitClient(SDnode *pDnode);
int32_t dmInitStatusClient(SDnode *pDnode);
int32_t dmInitSyncClient(SDnode *pDnode);
void    dmCleanupClient(SDnode *pDnode);
void    dmCleanupStatusClient(SDnode *pDnode);
void    dmCleanupSyncClient(SDnode *pDnode);
SMsgCb  dmGetMsgcb(SDnode *pDnode);
int32_t dmInitMsgHandle(SDnode *pDnode);
int32_t dmProcessNodeMsg(SMgmtWrapper *pWrapper, SRpcMsg *pMsg);

// dmMonitor.c
void dmSendMonitorReport();
void dmSendAuditRecords();
void dmSendMonitorReportBasic();
void dmGetVnodeLoads(SMonVloadInfo *pInfo);
void dmGetVnodeLoadsLite(SMonVloadInfo *pInfo);
void dmGetMnodeLoads(SMonMloadInfo *pInfo);
void dmGetQnodeLoads(SQnodeLoad *pInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MGMT_H_*/

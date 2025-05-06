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

#ifndef _TD_MND_STREAM_H_
#define _TD_MND_STREAM_H_
#include "stream.h"
#include "mndInt.h"
#include "mndTrans.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  STM_ERR_TASK_NOT_EXISTS = 1,
} EStmErrType;

typedef enum {
  MND_STM_PHASE_WATCH = 0,
  MND_STM_PHASE_NORMAL,
  MND_STM_PHASE_CLEANUP,
  MND_STM_PHASE_DEPLOYALL
} EMndStmPhase;

#define MND_STREAM_RUNNER_DEPLOY_NUM 3
#define MND_STREAM_REPORT_PERIOD  (STREAM_HB_INTERVAL_MS * STREAM_MAX_GROUP_NUM)
#define MND_STREAM_WATCH_DURATION (MND_STREAM_REPORT_PERIOD * 3)
#define MND_STREAM_HEALTH_CHECK_PERIOD_SEC (MND_STREAM_WATCH_DURATION / 1000)

#define STREAM_RUNNER_MAX_DEPLOYS 
#define STREAM_RUNNER_MAX_REPLICA 

#define STREAM_ACT_DEPLOY (1 << 0)
#define STREAM_ACT_UNDEPLOY (1 << 1)
#define STREAM_ACT_START (1 << 2)
#define STREAM_ACT_ERR_HANDLE (1 << 3)

#define STREAM_FLAG_TRIGGER_READER (1 << 0)

#define STREAM_IS_TRIGGER_READER(_flags) ((_flags) & STREAM_FLAG_TRIGGER_READER)

#define MND_STREAM_RESERVE_SIZE      64
#define MND_STREAM_VER_NUMBER        6
#define MND_STREAM_TRIGGER_NAME_SIZE 20
#define MND_STREAM_DEFAULT_NUM       100
#define MND_STREAM_DEFAULT_TASK_NUM  200

#define MND_SET_RUNNER_TASKIDX(_level, _idx) (((_level) << 16) & (_idx))

#define MND_GET_RUNNER_SUBPLANID(_id) ((_id) &0xFFFFFFFF)

#define MND_STREAM_CREATE_NAME       "stream-create"
#define MND_STREAM_PAUSE_NAME        "stream-pause"
#define MND_STREAM_RESUME_NAME       "stream-resume"
#define MND_STREAM_DROP_NAME         "stream-drop"
#define MND_STREAM_STOP_NAME         "stream-stop"

#define GOT_SNODE(_snodeId) ((_snodeId) != INT32_MIN)

typedef struct SVgroupChangeInfo {
  SHashObj *pDBMap;
  SArray   *pUpdateNodeList;  // SArray<SNodeUpdateInfo>
} SVgroupChangeInfo;

typedef struct SStmQNode {
  int64_t              streamId;
  char*                streamName;
  int32_t              action;
  void*                next;
} SStmQNode;

typedef struct SStmActionQ {
  bool          stopQueue;
  SStmQNode* head;
  SStmQNode* tail;
  uint64_t      qRemainNum;
} SStmActionQ;

typedef struct SStmTaskId {
  int64_t taskId;
  int32_t nodeId;
  int32_t taskIdx;
} SStmTaskId;

typedef struct SStmTaskStatus {
  SStmTaskId    id;
  int64_t       flags;
  EStreamStatus status;
  int64_t       lastUpTs;
} SStmTaskStatus;

typedef struct SStmTaskSrcAddr {
  int64_t taskId;
  int32_t vgId;
  int32_t groupId;
  SEpSet  epset;
} SStmTaskSrcAddr;

typedef struct SStmStatus {
  int64_t           lastActTs;
  int32_t           readerNum[2];      // trigger reader num & calc reader num
  SArray*           readerList;        // SArray<SStmTaskStatus>
  SStmTaskStatus*   triggerTask;
  int32_t           runnerDeploys;
  int32_t           runnerReplica;
  SArray*           runnerTopIdx;      // top runner task index in runnerList, num is runnerDeploys
  SArray*           runnerList;        // SArray<SStmTaskStatus>
} SStmStatus;

typedef struct SStmTaskStatusExt{
  int64_t         streamId;
  SStmTaskStatus* status;
} SStmTaskStatusExt;

typedef struct SStmSnodeTasksStatus {
  int32_t  runnerThreadNum; // runner thread num in snode
  SRWLatch lock;
  SArray*  triggerList;     // SArray<SStmTaskStatusExt>
  SArray*  runnerList;      // SArray<SStmTaskStatusExt>
} SStmSnodeTasksStatus;

typedef struct SStmVgroupTasksStatus {
  SRWLatch lock;
  int32_t  leaderDnodeId;
  int64_t  streamVer;
  SArray*  taskList;       // SArray<SStmTaskStatusExt>
} SStmVgroupTasksStatus;

typedef struct SStmTaskDeployExt {
  bool           deployed;
  SStmTaskDeploy deploy;
} SStmTaskDeployExt;

typedef struct SStmVgroupTasksDeploy {
  SRWLatch lock;
  int64_t  streamVer;
  int32_t  deployed;
  SArray*  taskList;       // SArray<SStmTaskDeployExt>
} SStmVgroupTasksDeploy;

typedef struct SStmSnodeTasksDeploy {
  SRWLatch lock;
  int32_t  triggerDeployed;
  int32_t  runnerDeployed;
  SArray*  triggerList;  // SArray<SStmTaskDeployExt>
  SArray*  runnerList;   // SArray<SStmTaskDeployExt>
} SStmSnodeTasksDeploy;

typedef struct SStmStreamUndeploy{
  SArray*     taskList;      // SArray<SStreamTask*>
  int8_t      doCheckpoint;
  int8_t      doCleanup;
} SStmStreamUndeploy;

typedef struct SStmAction {
  int32_t            actions;
  SStmStreamDeploy   deploy;
  SStmStreamUndeploy undeploy;
} SStmAction;

typedef struct SStmGrpCtx {
  SMnode*           pMnode;
  int64_t           currTs; 
  SStreamHbMsg*     pReq;
  SMStreamHbRspMsg* pRsp;

  int32_t           tidx;

  // status update
  int32_t          taskNum;

  SHashObj*        deployStm;
  SHashObj*        actionStm;
} SStmGrpCtx;

typedef struct SStmThreadCtx {
  SStmGrpCtx       grpCtx[STREAM_MAX_GROUP_NUM];
  SHashObj*        deployStm[STREAM_MAX_GROUP_NUM];    // streamId => SStmStreamDeploy
  SHashObj*        actionStm[STREAM_MAX_GROUP_NUM];    // streamId => SStmAction
} SStmThreadCtx;

typedef struct SStmRuntime {
  bool             initialized;
  bool             isLeader;
  SRWLatch         runtimeLock;
  int32_t          activeStreamNum;
  int64_t          startTs;
  EMndStmPhase     phase;

  SRWLatch         actionQLock;
  SStmActionQ*     actionQ;
  
  int32_t           threadNum;
  SStmThreadCtx*    tCtx;

  int64_t          lastTaskId;

  // ST
  SHashObj*        streamMap;  // streamId => SStmStatus
  SHashObj*        taskMap;    // streamId + taskId => SStmTaskStatus*
  SHashObj*        vgroupMap;  // vgId => SStmVgroupTasksStatus (only reader tasks)
  SHashObj*        snodeMap;   // snodeId => SStmSnodeTasksStatus (only trigger and runner tasks)
  SHashObj*        dnodeMap;   // dnodeId => lastUpTs

  // TD
  int32_t          toDeployVgTaskNum;
  SHashObj*        toDeployVgMap;      // vgId => SStmVgroupTasksDeploy (only reader tasks)
  int32_t          toDeploySnodeTaskNum;
  SHashObj*        toDeploySnodeMap;   // snodeId => SStmSnodeTasksDeploy (only trigger and runner tasks)

  // UP
  int32_t          toUpdateRunnerNum;
  SHashObj*        toUpdateRunnerMap;   // streamId + subplanId => SStmTaskSrcAddr (only scan's target runner tasks)
} SStmRuntime;

extern SStmRuntime         mStreamMgmt;

int32_t mndInitStream(SMnode *pMnode);
void    mndCleanupStream(SMnode *pMnode);
int32_t mndAcquireStream(SMnode *pMnode, char *streamName, SStreamObj **pStream);
void    mndReleaseStream(SMnode *pMnode, SStreamObj *pStream);
int32_t mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);

int32_t  mstGetStreamsNumInDb(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams);
int32_t  setTransAction(STrans *pTrans, void *pCont, int32_t contLen, int32_t msgType, const SEpSet *pEpset,
                        int32_t retryCode, int32_t acceptCode);
int32_t  doCreateTrans(SMnode *pMnode, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *name,
                       const char *pMsg, STrans **pTrans1);
int32_t  mndPersistTransLog(SStreamObj *pStream, STrans *pTrans, int32_t status);
SSdbRaw *mndStreamActionEncode(SStreamObj *pStream);

int32_t mndGetStreamObj(SMnode *pMnode, int64_t streamId, SStreamObj **pStream);
int32_t mndCheckForSnode(SMnode *pMnode, SDbObj *pSrcDb);

int32_t mndProcessStreamHb(SRpcMsg *pReq);
int32_t mndStreamSetDropAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t mndStreamSetDropActionFromList(SMnode *pMnode, STrans *pTrans, SArray *pList);
int32_t mndStreamSetStopStreamTasksActions(SMnode* pMnode, STrans *pTrans, uint64_t dbUid);

int32_t msmInitRuntimeInfo(SMnode *pMnode);
int32_t mndStreamTransAppend(SStreamObj *pStream, STrans *pTrans, int32_t status);
int32_t mndStreamCreateTrans(SMnode *pMnode, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *name, STrans **ppTrans);
int32_t setStreamAttrInResBlock(SStreamObj *pStream, SSDataBlock *pBlock, int32_t numOfRows);
int32_t mstCheckSnodeExists(SMnode *pMnode);
void msmClearStreamToDeployMaps(SStreamHbMsg* pHb);
void msmCleanStreamGrpCtx(SStreamHbMsg* pHb);
int32_t msmHandleStreamHbMsg(SMnode* pMnode, int64_t currTs, SStreamHbMsg* pHb, SMStreamHbRspMsg* pRsp);
int32_t msmHandleGrantExpired(SMnode *pMnode);
bool mndStreamActionDequeue(SStmActionQ* pQueue, SStmQNode **param);
void msmHandleBecomeLeader(SMnode *pMnode);
void msmHandleBecomeNotLeader(SMnode *pMnode);
int32_t msmUndeployStream(SMnode* pMnode, int64_t streamId, char* streamName);
int32_t mstIsStreamDropped(SMnode *pMnode, int64_t streamId, bool* dropped);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_STREAM_H_*/

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

#include "mndInt.h"
#include "mndTrans.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  STM_ERR_TASK_NOT_EXISTS,
} EStmStatusErrType;

#define STREAM_ACT_DEPLOY (1 << 0)
#define STREAM_ACT_UNDEPLOY (1 << 1)
#define STREAM_ACT_START (1 << 2)
#define STREAM_ACT_ERR_HANDLE (1 << 3)

#define MND_STREAM_RESERVE_SIZE      64
#define MND_STREAM_VER_NUMBER        6
#define MND_STREAM_TRIGGER_NAME_SIZE 20
#define MND_STREAM_DEFAULT_NUM       100
#define MND_STREAM_DEFAULT_TASK_NUM  200

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
  struct SStmtQNode*   next;
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
  int16_t taskIdx;
} SStmTaskId;

typedef struct SStmTaskStatus {
  SStmTaskId    id;
  EStreamStatus status;
  int64_t       lastUpTs;
} SStmTaskStatus;

typedef struct SStmReadersStatus {
  SArray* vgList;   // SArray<SStmTaskStatus>, all readers from triger and calc list
} SStmReadersStatus;

typedef struct SStmStatus {
  int64_t           lastActTs;
  SArray*           readerList;        // SArray<SStmTaskStatus>
  SStmTaskStatus    triggerTask;
  SArray*           runnerList;        // SArray<SStmTaskStatus>
} SStmStatus;

typedef struct SStmSnodeTasksStatus {
  SRWLatch lock;
  SArray*  triggerList;  // SArray<SStmTaskStatus*>
  SArray*  runnerList;   // SArray<SStmTaskStatus*>
} SStmSnodeTasksStatus;

typedef struct SStmVgroupTasksStatus {
  SRWLatch lock;
  int64_t  streamVer;
  SArray*  taskList;       // SArray<SStmTaskStatus*>
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
  SArray*  triggerList;  // SArray<SStmTaskDeploy>
  SArray*  runnerList;   // SArray<SStmTaskDeploy>
} SStmSnodeTasksDeploy;

typedef struct SStmThreadCtx {
  SStmActionQ*     actionQ;
  SHashObj*        deployStm[STREAM_MAX_GROUP_NUM];    // streamId => SStmStreamDeploy
  SHashObj*        actionStm[STREAM_MAX_GROUP_NUM];    // streamId => actions
} SStmThreadCtx;

typedef struct SStmRuntime {
  bool             initialized;
  bool             isLeader;
  int32_t          activeStreamNum;
  
  int32_t           threadNum;
  SStmThreadCtx*    tCtx;

  int64_t          lastTaskId;
  int16_t          runnerMulti;
  
  SHashObj*        streamMap;  // streamId => SStreamStatus
  SHashObj*        taskMap;    // streamId + taskId => SStmTaskStatus*
  SHashObj*        vgroupMap;  // vgId => SStmVgroupTasksStatus (only reader tasks)
  SHashObj*        snodeMap;   // snodeId => SStmSnodeTasksStatus (only trigger and runner tasks)
  SHashObj*        dnodeMap;   // dnodeId => lastUpTs

  int32_t          toDeployVgTaskNum;
  SHashObj*        toDeployVgMap;      // vgId => SStmVgroupTasksDeploy (only reader tasks)
  int32_t          toDeploySnodeTaskNum;
  SHashObj*        toDeploySnodeMap;   // snodeId => SStmSnodeTasksDeploy (only trigger and runner tasks)
} SStmRuntime;

extern SStmRuntime         mStreamMgmt;

int32_t mndInitStream(SMnode *pMnode);
void    mndCleanupStream(SMnode *pMnode);
int32_t mndAcquireStream(SMnode *pMnode, char *streamName, SStreamObj **pStream);
void    mndReleaseStream(SMnode *pMnode, SStreamObj *pStream);
int32_t mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);

int32_t  mndGetNumOfStreams(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams);
int32_t  mndGetNumOfStreamTasks(const SStreamObj *pStream);
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
int32_t mndStreamSetCheckpointAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask, int64_t checkpointId,
                                     int8_t mndTrigger);
int32_t mndStreamSetStopStreamTasksActions(SMnode* pMnode, STrans *pTrans, uint64_t dbUid);

int32_t mndInitExecInfo();
void    removeStreamTasksInBuf(SStreamObj *pStream, SStreamExecInfo *pExecNode);

int32_t setStreamAttrInResBlock(SStreamObj *pStream, SSDataBlock *pBlock, int32_t numOfRows);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_STREAM_H_*/

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
  STREAM_ACTION_DEPLOY,
  STREAM_ACTION_UNDEPLOY,
} MND_STREAM_ACTION;

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

typedef struct SStreamTransInfo {
  int64_t     startTime;
  int64_t     streamId;
  const char *name;
  int32_t     transId;
} SStreamTransInfo;

typedef struct SVgroupChangeInfo {
  SHashObj *pDBMap;
  SArray   *pUpdateNodeList;  // SArray<SNodeUpdateInfo>
} SVgroupChangeInfo;

typedef struct SStreamQNode {
  int64_t              streamId;
  char*                streamName;
  MND_STREAM_ACTION    action;
  struct SStmtQNode*   next;
} SStreamQNode;

typedef struct SStreamActionQ {
  bool          stopQueue;
  SStreamQNode* head;
  SStreamQNode* tail;
  uint64_t      qRemainNum;
} SStreamActionQ;

typedef struct SStreamTaskId {
  int64_t taskId;
  int32_t nodeId;
  int16_t taskIdx;
} SStreamTaskId;

typedef struct SStreamTaskState {
  SStreamTaskId id;
  int64_t       lastUpTs;
} SStreamTaskState;

typedef struct SStreamReaderTasksState {
  SArray* vgList;   // SArray<SStreamTaskState>, triger and calc list
} SStreamReaderTasksState;

typedef struct SStreamTasksInfo {
  SArray* readerTaskList;        // SArray<SStreamReaderTasksState>
  SArray* triggerTaskList;       // SArray<SStreamTaskState>
  SArray* runnerTaskList;        // SArray<SStreamTaskState>
} SStreamTasksInfo;

typedef struct SStreamSnodeTasksStatus {
  SRWLatch lock;
  SArray*  triggerTaskList;  // SArray<SStreamTaskState*>
  SArray*  runnerTaskList;   // SArray<SStreamTaskState*>
} SStreamSnodeTasksStatus;

typedef struct SStreamVgReaderTasksStatus {
  SRWLatch lock;
  int64_t  streamVer;
  SArray*  taskList;       // SArray<SStreamTaskState*>
} SStreamVgReaderTasksStatus;

typedef struct SStreamDeployTaskExtInfo {
  bool                  deployed;
  SStreamDeployTaskInfo deploy;
} SStreamDeployTaskExtInfo;

typedef struct SStreamVgReaderTasksDeploy {
  SRWLatch lock;
  int64_t  streamVer;
  int32_t  deployed;
  SArray*  taskList;       // SArray<SStreamDeployTaskExtInfo>
} SStreamVgReaderTasksDeploy;

typedef struct SStreamSnodeTasksDeploy {
  SRWLatch lock;
  int32_t  triggerDeployed;
  int32_t  runnerDeployed;
  SArray*  triggerTaskList;  // SArray<SStreamDeployTaskInfo>
  SArray*  runnerTaskList;   // SArray<SStreamDeployTaskInfo>
} SStreamSnodeTasksDeploy;

typedef struct SStreamThreadGrp {
  SStreamActionQ*  actionQ;
  SHashObj*        deployStm[STREAM_MAX_GROUP_NUM];  // streadId => SStreamTasksDeploy
} SStreamThreadGrp;

typedef struct SStreamRuntime {
  int32_t          activeStreamNum;
  
  int32_t           threadNum;
  SStreamThreadGrp* threadGrp;

  int64_t          lastTaskId;
  SHashObj*        streamMap;  // streamId => SStreamTasksInfo
  SHashObj*        taskMap;    // streamId + taskId => SStreamTaskState*
  SHashObj*        vgroupMap;  // vgId => SStreamVgReaderTasksStatus (only reader tasks)
  SHashObj*        snodeMap;   // snodeId => SStreamSnodeTasks (only trigger and runner tasks)
  SHashObj*        dnodeMap;   // dnodeId => lastUpTs

  int32_t          toDeployVgTaskNum;
  SHashObj*        toDeployVgMap;      // vgId => SStreamVgReaderTasksDeploy (only reader tasks)
  int32_t          toDeploySnodeTaskNum;
  SHashObj*        toDeploySnodeMap;   // snodeId => SStreamSnodeTasksDeploy (only trigger and runner tasks)
  
  int32_t          role;
  bool             switchFromFollower;
  bool             initTaskList;
  TdThreadMutex    lock;
} SStreamRuntime;

extern SStreamRuntime         mStreamMgmt;
typedef struct SStreamTaskIter SStreamTaskIter;

typedef struct SNodeEntry {
  int32_t nodeId;
  bool    stageUpdated;  // the stage has been updated due to the leader/follower change or node reboot.
  SEpSet  epset;         // compare the epset to identify the vgroup tranferring between different dnodes.
  int64_t hbTimestamp;   // second
  int32_t lastHbMsgId;   // latest hb msgId
  int64_t lastHbMsgTs;
} SNodeEntry;

typedef struct {
  SMsgHead head;
} SMStreamReqCheckpointRsp, SMStreamUpdateChkptRsp, SMStreamReqConsensChkptRsp;

typedef struct STaskChkptInfo {
  int32_t nodeId;
  int32_t taskId;
  int64_t streamId;
  int64_t checkpointId;
  int64_t version;
  int64_t ts;
  int32_t transId;
  int8_t  dropHTask;
} STaskChkptInfo;

int32_t mndInitStream(SMnode *pMnode);
void    mndCleanupStream(SMnode *pMnode);
int32_t mndAcquireStream(SMnode *pMnode, char *streamName, SStreamObj **pStream);
void    mndReleaseStream(SMnode *pMnode, SStreamObj *pStream);
int32_t mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);

int32_t  mndGetNumOfStreams(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams);
int32_t  mndGetNumOfStreamTasks(const SStreamObj *pStream);
int32_t  mndTakeVgroupSnapshot(SMnode *pMnode, bool *allReady, SArray **pList, SHashObj* pTermMap);
void     mndDestroyVgroupChangeInfo(SVgroupChangeInfo *pInfo);
int32_t  setTransAction(STrans *pTrans, void *pCont, int32_t contLen, int32_t msgType, const SEpSet *pEpset,
                        int32_t retryCode, int32_t acceptCode);
int32_t  doCreateTrans(SMnode *pMnode, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *name,
                       const char *pMsg, STrans **pTrans1);
int32_t  mndPersistTransLog(SStreamObj *pStream, STrans *pTrans, int32_t status);
SSdbRaw *mndStreamActionEncode(SStreamObj *pStream);

int32_t mndGetStreamObj(SMnode *pMnode, int64_t streamId, SStreamObj **pStream);
int32_t mndCheckForSnode(SMnode *pMnode, SDbObj *pSrcDb);

int32_t extractNodeEpset(SMnode *pMnode, SEpSet *pEpSet, bool *hasEpset, int32_t taskId, int32_t nodeId);
int32_t mndProcessStreamHb(SRpcMsg *pReq);
int32_t extractStreamNodeList(SMnode *pMnode);
int32_t mndStreamSetDropAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t mndStreamSetDropActionFromList(SMnode *pMnode, STrans *pTrans, SArray *pList);
int32_t mndStreamSetCheckpointAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask, int64_t checkpointId,
                                     int8_t mndTrigger);
int32_t mndStreamSetStopStreamTasksActions(SMnode* pMnode, STrans *pTrans, uint64_t dbUid);

void    removeTasksInBuf(SArray *pTaskIds, SStreamExecInfo *pExecInfo);
int32_t mndFindChangedNodeInfo(SMnode *pMnode, const SArray *pPrevNodeList, const SArray *pNodeList,
                               SVgroupChangeInfo *pInfo);

int32_t createStreamTaskIter(SStreamObj *pStream, SStreamTaskIter **pIter);
void    destroyStreamTaskIter(SStreamTaskIter *pIter);
bool    streamTaskIterNextTask(SStreamTaskIter *pIter);
int32_t streamTaskIterGetCurrent(SStreamTaskIter *pIter, SStreamTask **pTask);

int32_t mndInitExecInfo();
void    mndInitStreamExecInfo(SMnode *pMnode, SStreamExecInfo *pExecInfo);
void    mndStreamResetInitTaskListLoadFlag();
void    mndUpdateStreamExecInfoRole(SMnode *pMnode, int32_t role);
int32_t removeExpiredNodeEntryAndTaskInBuf(SArray *pNodeSnapshot);
void    removeStreamTasksInBuf(SStreamObj *pStream, SStreamExecInfo *pExecNode);

int32_t mndClearConsensusCheckpointId(SHashObj *pHash, int64_t streamId);
int32_t mndClearChkptReportInfo(SHashObj *pHash, int64_t streamId);

int32_t setStreamAttrInResBlock(SStreamObj *pStream, SSDataBlock *pBlock, int32_t numOfRows);
int32_t setTaskAttrInResBlock(SStreamObj *pStream, SStreamTask *pTask, SSDataBlock *pBlock, int32_t nRows, int32_t p);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_STREAM_H_*/

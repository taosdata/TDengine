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

#define MND_STREAM_RESERVE_SIZE 64
#define MND_STREAM_VER_NUMBER   5

#define MND_STREAM_CREATE_NAME       "stream-create"
#define MND_STREAM_CHECKPOINT_NAME   "stream-checkpoint"
#define MND_STREAM_PAUSE_NAME        "stream-pause"
#define MND_STREAM_RESUME_NAME       "stream-resume"
#define MND_STREAM_DROP_NAME         "stream-drop"
#define MND_STREAM_TASK_RESET_NAME   "stream-task-reset"
#define MND_STREAM_TASK_UPDATE_NAME  "stream-task-update"
#define MND_STREAM_CHKPT_UPDATE_NAME "stream-chkpt-update"

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

typedef struct SStreamTransMgmt {
  SHashObj *pDBTrans;
} SStreamTransMgmt;

typedef struct SStreamExecInfo {
  bool             initTaskList;
  SArray          *pNodeList;
  int64_t          ts;  // snapshot ts
  SStreamTransMgmt transMgmt;
  SHashObj        *pTaskMap;
  SArray          *pTaskList;
  TdThreadMutex    lock;
  SHashObj        *pTransferStateStreams;
  SHashObj        *pChkptStreams;
} SStreamExecInfo;

extern SStreamExecInfo         execInfo;
typedef struct SStreamTaskIter SStreamTaskIter;

typedef struct SNodeEntry {
  int32_t nodeId;
  bool    stageUpdated;  // the stage has been updated due to the leader/follower change or node reboot.
  SEpSet  epset;         // compare the epset to identify the vgroup tranferring between different dnodes.
  int64_t hbTimestamp;   // second
} SNodeEntry;

typedef struct SOrphanTask {
  int64_t streamId;
  int32_t taskId;
  int32_t nodeId;
} SOrphanTask;

typedef struct {
  SMsgHead head;
} SMStreamHbRspMsg, SMStreamReqCheckpointRsp, SMStreamUpdateChkptRsp;

typedef struct STaskChkptInfo {
  int32_t nodeId;
  int32_t taskId;
  int64_t streamId;
  int64_t checkpointId;
  int64_t version;
  int64_t ts;
  int32_t transId;
  int8_t  dropHTask;
}STaskChkptInfo;

int32_t     mndInitStream(SMnode *pMnode);
void        mndCleanupStream(SMnode *pMnode);
SStreamObj *mndAcquireStream(SMnode *pMnode, char *streamName);
void        mndReleaseStream(SMnode *pMnode, SStreamObj *pStream);
int32_t     mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t     mndPersistStream(STrans *pTrans, SStreamObj *pStream);
int32_t     mndStreamRegisterTrans(STrans *pTrans, const char *pTransName, int64_t streamId);
int32_t     mndStreamClearFinishedTrans(SMnode *pMnode, int32_t *pNumOfActiveChkpt);
bool        mndStreamTransConflictCheck(SMnode *pMnode, int64_t streamId, const char *pTransName, bool lock);
int32_t     mndStreamGetRelTrans(SMnode *pMnode, int64_t streamId);

int32_t  mndGetNumOfStreams(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams);
int32_t  mndGetNumOfStreamTasks(const SStreamObj *pStream);
SArray  *mndTakeVgroupSnapshot(SMnode *pMnode, bool *allReady);
void     mndKillTransImpl(SMnode *pMnode, int32_t transId, const char *pDbName);
int32_t  setTransAction(STrans *pTrans, void *pCont, int32_t contLen, int32_t msgType, const SEpSet *pEpset,
                        int32_t retryCode, int32_t acceptCode);
STrans  *doCreateTrans(SMnode *pMnode, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *name, const char *pMsg);
int32_t  mndPersistTransLog(SStreamObj *pStream, STrans *pTrans, int32_t status);
SSdbRaw *mndStreamActionEncode(SStreamObj *pStream);
void     killAllCheckpointTrans(SMnode *pMnode, SVgroupChangeInfo *pChangeInfo);
int32_t  mndStreamSetUpdateEpsetAction(SMnode *pMnode, SStreamObj *pStream, SVgroupChangeInfo *pInfo, STrans *pTrans);

SStreamObj *mndGetStreamObj(SMnode *pMnode, int64_t streamId);
int32_t     extractNodeEpset(SMnode *pMnode, SEpSet *pEpSet, bool *hasEpset, int32_t taskId, int32_t nodeId);
int32_t     mndProcessStreamHb(SRpcMsg *pReq);
void        saveTaskAndNodeInfoIntoBuf(SStreamObj *pStream, SStreamExecInfo *pExecNode);
int32_t     extractStreamNodeList(SMnode *pMnode);
int32_t     mndStreamSetResumeAction(STrans *pTrans, SMnode *pMnode, SStreamObj *pStream, int8_t igUntreated);
int32_t     mndStreamSetPauseAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t     mndStreamSetDropAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t     mndStreamSetDropActionFromList(SMnode *pMnode, STrans *pTrans, SArray *pList);
int32_t     mndStreamSetResetTaskAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t     mndCreateStreamResetStatusTrans(SMnode *pMnode, SStreamObj *pStream);
int32_t     mndStreamSetUpdateChkptAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t     mndCreateStreamChkptInfoUpdateTrans(SMnode *pMnode, SStreamObj *pStream, SArray *pChkptInfoList);
int32_t     mndScanCheckpointReportInfo(SRpcMsg *pReq);
void        removeTasksInBuf(SArray *pTaskIds, SStreamExecInfo *pExecInfo);

SStreamTaskIter *createStreamTaskIter(SStreamObj *pStream);
void             destroyStreamTaskIter(SStreamTaskIter *pIter);
bool             streamTaskIterNextTask(SStreamTaskIter *pIter);
SStreamTask     *streamTaskIterGetCurrent(SStreamTaskIter *pIter);
void             mndInitExecInfo();
void             mndInitStreamExecInfo(SMnode *pMnode, SStreamExecInfo *pExecInfo);
int32_t          removeExpiredNodeEntryAndTaskInBuf(SArray *pNodeSnapshot);
void             removeStreamTasksInBuf(SStreamObj *pStream, SStreamExecInfo *pExecNode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_STREAM_H_*/

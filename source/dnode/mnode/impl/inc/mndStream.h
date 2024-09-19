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
#define MND_STREAM_CHKPT_CONSEN_NAME "stream-chkpt-consen"

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

typedef struct SStreamTaskResetMsg {
  int64_t streamId;
  int32_t transId;
} SStreamTaskResetMsg;

typedef struct SChkptReportInfo {
  SArray* pTaskList;
  int64_t reportChkpt;
  int64_t streamId;
} SChkptReportInfo;

typedef struct SStreamExecInfo {
  int32_t          role;
  bool             switchFromFollower;
  bool             initTaskList;
  SArray          *pNodeList;
  int64_t          ts;  // snapshot ts
  SStreamTransMgmt transMgmt;
  SHashObj        *pTaskMap;
  SArray          *pTaskList;
  TdThreadMutex    lock;
  SHashObj        *pTransferStateStreams;
  SHashObj        *pChkptStreams;  // use to update the checkpoint info, if all tasks send the checkpoint-report msgs
  SHashObj        *pStreamConsensus;
  SArray          *pKilledChkptTrans;  // SArray<SStreamTaskResetMsg>
} SStreamExecInfo;

extern SStreamExecInfo         execInfo;
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
}STaskChkptInfo;

int32_t mndInitStream(SMnode *pMnode);
void    mndCleanupStream(SMnode *pMnode);
int32_t mndAcquireStream(SMnode *pMnode, char *streamName, SStreamObj **pStream);
void    mndReleaseStream(SMnode *pMnode, SStreamObj *pStream);
int32_t mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t mndPersistStream(STrans *pTrans, SStreamObj *pStream);
int32_t mndStreamRegisterTrans(STrans *pTrans, const char *pTransName, int64_t streamId);
int32_t mndStreamClearFinishedTrans(SMnode *pMnode, int32_t *pNumOfActiveChkpt);
int32_t mndStreamTransConflictCheck(SMnode *pMnode, int64_t streamId, const char *pTransName, bool lock);
int32_t mndStreamGetRelTrans(SMnode *pMnode, int64_t streamId);

int32_t  mndGetNumOfStreams(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams);
int32_t  mndGetNumOfStreamTasks(const SStreamObj *pStream);
int32_t  mndTakeVgroupSnapshot(SMnode *pMnode, bool *allReady, SArray** pList);
void     mndKillTransImpl(SMnode *pMnode, int32_t transId, const char *pDbName);
int32_t  setTransAction(STrans *pTrans, void *pCont, int32_t contLen, int32_t msgType, const SEpSet *pEpset,
                        int32_t retryCode, int32_t acceptCode);
int32_t  doCreateTrans(SMnode *pMnode, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *name,
                       const char *pMsg, STrans **pTrans1);
int32_t  mndPersistTransLog(SStreamObj *pStream, STrans *pTrans, int32_t status);
SSdbRaw *mndStreamActionEncode(SStreamObj *pStream);
void     killAllCheckpointTrans(SMnode *pMnode, SVgroupChangeInfo *pChangeInfo);
int32_t  mndStreamSetUpdateEpsetAction(SMnode *pMnode, SStreamObj *pStream, SVgroupChangeInfo *pInfo, STrans *pTrans);

int32_t mndGetStreamObj(SMnode *pMnode, int64_t streamId, SStreamObj** pStream);
int32_t extractNodeEpset(SMnode *pMnode, SEpSet *pEpSet, bool *hasEpset, int32_t taskId, int32_t nodeId);
int32_t mndProcessStreamHb(SRpcMsg *pReq);
void    saveTaskAndNodeInfoIntoBuf(SStreamObj *pStream, SStreamExecInfo *pExecNode);
int32_t extractStreamNodeList(SMnode *pMnode);
int32_t mndStreamSetResumeAction(STrans *pTrans, SMnode *pMnode, SStreamObj *pStream, int8_t igUntreated);
int32_t mndStreamSetPauseAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t mndStreamSetDropAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t mndStreamSetDropActionFromList(SMnode *pMnode, STrans *pTrans, SArray *pList);
int32_t mndStreamSetResetTaskAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t mndCreateStreamResetStatusTrans(SMnode *pMnode, SStreamObj *pStream);
int32_t mndStreamSetUpdateChkptAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t mndCreateStreamChkptInfoUpdateTrans(SMnode *pMnode, SStreamObj *pStream, SArray *pChkptInfoList);
int32_t mndScanCheckpointReportInfo(SRpcMsg *pReq);
int32_t mndCreateSetConsensusChkptIdTrans(SMnode *pMnode, SStreamObj *pStream, int32_t taskId, int64_t checkpointId,
                                          int64_t ts);
void    removeTasksInBuf(SArray *pTaskIds, SStreamExecInfo *pExecInfo);

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

int32_t mndGetConsensusInfo(SHashObj *pHash, int64_t streamId, int32_t numOfTasks, SCheckpointConsensusInfo **pInfo);
void    mndAddConsensusTasks(SCheckpointConsensusInfo *pInfo, const SRestoreCheckpointInfo *pRestoreInfo);
void    mndClearConsensusRspEntry(SCheckpointConsensusInfo *pInfo);
int64_t mndClearConsensusCheckpointId(SHashObj* pHash, int64_t streamId);
int64_t mndClearChkptReportInfo(SHashObj* pHash, int64_t streamId);
int32_t mndResetChkptReportInfo(SHashObj* pHash, int64_t streamId);

int32_t setStreamAttrInResBlock(SStreamObj *pStream, SSDataBlock *pBlock, int32_t numOfRows);
int32_t setTaskAttrInResBlock(SStreamObj *pStream, SStreamTask *pTask, SSDataBlock *pBlock, int32_t numOfRows);

int32_t mndProcessResetStatusReq(SRpcMsg *pReq);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_STREAM_H_*/

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

#include "mndDb.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "taoserror.h"
#include "tmisce.h"

static int32_t doSetPauseAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask) {
  SVPauseStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVPauseStreamTaskReq));
  if (pReq == NULL) {
    mError("failed to malloc in pause stream, size:%" PRIzu ", code:%s", sizeof(SVPauseStreamTaskReq),
           tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    // terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    terrno = code;
    taosMemoryFree(pReq);
    return code;
  }

  char buf[256] = {0};
  code = epsetToStr(&epset, buf, tListLen(buf));
  if (code != 0) {  // print error and continue
    mError("failed to convert epset to str, code:%s", tstrerror(code));
  }

  mDebug("pause stream task in node:%d, epset:%s", pTask->info.nodeId, buf);
  code = setTransAction(pTrans, pReq, sizeof(SVPauseStreamTaskReq), TDMT_STREAM_TASK_PAUSE, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != 0) {
    taosMemoryFree(pReq);
    return code;
  }
  return 0;
}

static int32_t doSetDropAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask) {
  SVDropStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {  // no valid epset, return directly without redoAction
    return code;
  }

  // The epset of nodeId of this task may have been expired now, let's use the newest epset from mnode.
  code = setTransAction(pTrans, pReq, sizeof(SVDropStreamTaskReq), TDMT_STREAM_TASK_DROP, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != 0) {
    taosMemoryFree(pReq);
    return code;
  }

  return 0;
}

static int32_t doSetResumeAction(STrans *pTrans, SMnode *pMnode, SStreamTask *pTask, int8_t igUntreated) {
  terrno = 0;

  SVResumeStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVResumeStreamTaskReq));
  if (pReq == NULL) {
    mError("failed to malloc in resume stream, size:%" PRIzu ", code:%s", sizeof(SVResumeStreamTaskReq),
           tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;
  pReq->igUntreated = igUntreated;

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || (!hasEpset)) {
    taosMemoryFree(pReq);
    return code;
  }

  code = setTransAction(pTrans, pReq, sizeof(SVResumeStreamTaskReq), TDMT_STREAM_TASK_RESUME, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != 0) {
    taosMemoryFree(pReq);
    return code;
  }

  mDebug("set the resume action for trans:%d", pTrans->id);
  return code;
}

static int32_t doSetDropActionFromId(SMnode *pMnode, STrans *pTrans, SOrphanTask* pTask) {
  SVDropStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    // terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->nodeId);
  pReq->taskId = pTask->taskId;
  pReq->streamId = pTask->streamId;

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->taskId, pTask->nodeId);
  if (code != TSDB_CODE_SUCCESS || (!hasEpset)) {  // no valid epset, return directly without redoAction
    taosMemoryFree(pReq);
    return code;
  }

  // The epset of nodeId of this task may have been expired now, let's use the newest epset from mnode.
  code = setTransAction(pTrans, pReq, sizeof(SVDropStreamTaskReq), TDMT_STREAM_TASK_DROP, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != 0) {
    taosMemoryFree(pReq);
    return code;
  }

  return 0;
}

static void initNodeUpdateMsg(SStreamTaskNodeUpdateMsg *pMsg, const SVgroupChangeInfo *pInfo, SStreamTaskId *pId,
                              int32_t transId) {
  int32_t code = 0;

  pMsg->streamId = pId->streamId;
  pMsg->taskId = pId->taskId;
  pMsg->transId = transId;
  pMsg->pNodeList = taosArrayInit(taosArrayGetSize(pInfo->pUpdateNodeList), sizeof(SNodeUpdateInfo));
  if (pMsg->pNodeList == NULL) {
    mError("failed to prepare node list, code:%s", tstrerror(terrno));
    code = terrno;
  }

  if (code == 0) {
    void *p = taosArrayAddAll(pMsg->pNodeList, pInfo->pUpdateNodeList);
    if (p == NULL) {
      mError("failed to add update node list into nodeList");
    }
  }
}

static int32_t doBuildStreamTaskUpdateMsg(void **pBuf, int32_t *pLen, SVgroupChangeInfo *pInfo, int32_t nodeId,
                                          SStreamTaskId *pId, int32_t transId) {
  SStreamTaskNodeUpdateMsg req = {0};
  initNodeUpdateMsg(&req, pInfo, pId, transId);

  int32_t code = 0;
  int32_t blen;

  tEncodeSize(tEncodeStreamTaskUpdateMsg, &req, blen, code);
  if (code < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosArrayDestroy(req.pNodeList);
    return terrno;
  }

  int32_t tlen = sizeof(SMsgHead) + blen;

  void *buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    taosArrayDestroy(req.pNodeList);
    return terrno;
  }

  void    *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  code = tEncodeStreamTaskUpdateMsg(&encoder, &req);
  if (code == -1) {
    tEncoderClear(&encoder);
    taosMemoryFree(buf);
    taosArrayDestroy(req.pNodeList);
    return code;
  }

  SMsgHead *pMsgHead = (SMsgHead *)buf;
  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(nodeId);

  tEncoderClear(&encoder);

  *pBuf = buf;
  *pLen = tlen;

  taosArrayDestroy(req.pNodeList);
  return TSDB_CODE_SUCCESS;
}

static int32_t doSetUpdateTaskAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask, SVgroupChangeInfo *pInfo) {
  void   *pBuf = NULL;
  int32_t len = 0;
  SEpSet  epset = {0};
  bool    hasEpset = false;

  bool    unusedRet = streamTaskUpdateEpsetInfo(pTask, pInfo->pUpdateNodeList);
  int32_t code = doBuildStreamTaskUpdateMsg(&pBuf, &len, pInfo, pTask->info.nodeId, &pTask->id, pTrans->id);
  if (code) {
    mError("failed to build stream task epset update msg, code:%s", tstrerror(code));
    return code;
  }

  code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    mError("failed to extract epset during create update epset, code:%s", tstrerror(code));
    return code;
  }

  code = setTransAction(pTrans, pBuf, len, TDMT_VND_STREAM_TASK_UPDATE, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed to create update task epset trans, code:%s", tstrerror(code));
    taosMemoryFree(pBuf);
  }

  return code;
}

static int32_t doSetUpdateChkptAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask) {
  SVUpdateCheckpointInfoReq *pReq = taosMemoryCalloc(1, sizeof(SVUpdateCheckpointInfoReq));
  if (pReq == NULL) {
    mError("failed to malloc in reset stream, size:%" PRIzu ", code:%s", sizeof(SVUpdateCheckpointInfoReq),
           tstrerror(terrno));
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;

  SChkptReportInfo *pStreamItem = (SChkptReportInfo*)taosHashGet(execInfo.pChkptStreams, &pTask->id.streamId, sizeof(pTask->id.streamId));
  if (pStreamItem == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t size = taosArrayGetSize(pStreamItem->pTaskList);
  for(int32_t i = 0; i < size; ++i) {
    STaskChkptInfo* pInfo = taosArrayGet(pStreamItem->pTaskList, i);
    if (pInfo == NULL) {
      continue;
    }

    if (pInfo->taskId == pTask->id.taskId) {
      pReq->checkpointId = pInfo->checkpointId;
      pReq->checkpointVer = pInfo->version;
      pReq->checkpointTs = pInfo->ts;
      pReq->dropRelHTask = pInfo->dropHTask;
      pReq->transId = pInfo->transId;
      pReq->hStreamId = pTask->hTaskInfo.id.streamId;
      pReq->hTaskId = pTask->hTaskInfo.id.taskId;
    }
  }

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    taosMemoryFree(pReq);
    return code;
  }

  code = setTransAction(pTrans, pReq, sizeof(SVUpdateCheckpointInfoReq), TDMT_STREAM_TASK_UPDATE_CHKPT, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pReq);
  }

  return code;
}

static int32_t doSetResetAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask) {
  SVResetStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVResetStreamTaskReq));
  if (pReq == NULL) {
    mError("failed to malloc in reset stream, size:%" PRIzu ", code:%s", sizeof(SVResetStreamTaskReq),
           tstrerror(terrno));
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    taosMemoryFree(pReq);
    return code;
  }

  code = setTransAction(pTrans, pReq, sizeof(SVResetStreamTaskReq), TDMT_VND_STREAM_TASK_RESET, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pReq);
  }

  return code;
}

static int32_t mndBuildStreamCheckpointSourceReq(void **pBuf, int32_t *pLen, int32_t nodeId, int64_t checkpointId,
                                                 int64_t streamId, int32_t taskId, int32_t transId, int8_t mndTrigger) {
  SStreamCheckpointSourceReq req = {0};
  req.checkpointId = checkpointId;
  req.nodeId = nodeId;
  req.expireTime = -1;
  req.streamId = streamId;  // pTask->id.streamId;
  req.taskId = taskId;      // pTask->id.taskId;
  req.transId = transId;
  req.mndTrigger = mndTrigger;

  int32_t code;
  int32_t blen;

  tEncodeSize(tEncodeStreamCheckpointSourceReq, &req, blen, code);
  if (code < 0) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  int32_t tlen = sizeof(SMsgHead) + blen;

  void *buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    return terrno;
  }

  void    *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  int32_t pos = tEncodeStreamCheckpointSourceReq(&encoder, &req);
  if (pos == -1) {
    tEncoderClear(&encoder);
    return TSDB_CODE_INVALID_MSG;
  }

  SMsgHead *pMsgHead = (SMsgHead *)buf;
  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(nodeId);

  tEncoderClear(&encoder);

  *pBuf = buf;
  *pLen = tlen;

  return 0;
}
int32_t mndStreamSetPauseAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = NULL;

  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
    }

    code = doSetPauseAction(pMnode, pTrans, pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
    }

    if (atomic_load_8(&pTask->status.taskStatus) != TASK_STATUS__PAUSE) {
      atomic_store_8(&pTask->status.statusBackup, pTask->status.taskStatus);
      atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__PAUSE);
    }
  }

  destroyStreamTaskIter(pIter);
  return code;
}

int32_t mndStreamSetDropAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = NULL;

  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while(streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
    }

    code = doSetDropAction(pMnode, pTrans, pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
    }
  }
  destroyStreamTaskIter(pIter);
  return 0;
}

int32_t mndStreamSetResumeAction(STrans *pTrans, SMnode *pMnode, SStreamObj *pStream, int8_t igUntreated) {
  SStreamTaskIter *pIter = NULL;
  int32_t          code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code || pTask == NULL) {
      destroyStreamTaskIter(pIter);
      return code;
    }

    code = doSetResumeAction(pTrans, pMnode, pTask, igUntreated);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
    }

    if (atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__PAUSE) {
      atomic_store_8(&pTask->status.taskStatus, pTask->status.statusBackup);
    }
  }
  destroyStreamTaskIter(pIter);
  return 0;
}

// build trans to update the epset
int32_t mndStreamSetUpdateEpsetAction(SMnode *pMnode, SStreamObj *pStream, SVgroupChangeInfo *pInfo, STrans *pTrans) {
  mDebug("stream:0x%" PRIx64 " set tasks epset update action", pStream->uid);
  SStreamTaskIter *pIter = NULL;

  taosWLockLatch(&pStream->lock);
  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    taosWUnLockLatch(&pStream->lock);
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }

    code = doSetUpdateTaskAction(pMnode, pTrans, pTask, pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }
  }

  destroyStreamTaskIter(pIter);
  taosWUnLockLatch(&pStream->lock);
  return 0;
}

int32_t mndStreamSetUpdateChkptAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = NULL;

  taosWLockLatch(&pStream->lock);
  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    taosWUnLockLatch(&pStream->lock);
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }

    code = doSetUpdateChkptAction(pMnode, pTrans, pTask);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }
  }

  destroyStreamTaskIter(pIter);
  taosWUnLockLatch(&pStream->lock);
  return code;
}

int32_t mndStreamSetDropActionFromList(SMnode *pMnode, STrans *pTrans, SArray* pList) {
  for(int32_t i = 0; i < taosArrayGetSize(pList); ++i) {
    SOrphanTask* pTask = taosArrayGet(pList, i);
    if (pTask == NULL) {
      return terrno;
    }

    int32_t code = doSetDropActionFromId(pMnode, pTrans, pTask);
    if (code != 0) {
      return code;
    } else {
      mDebug("add drop task:0x%x action to drop orphan task", pTask->taskId);
    }
  }
  return 0;
}

int32_t mndStreamSetResetTaskAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = NULL;

  taosWLockLatch(&pStream->lock);
  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    taosWUnLockLatch(&pStream->lock);
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }

    code = doSetResetAction(pMnode, pTrans, pTask);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }
  }

  destroyStreamTaskIter(pIter);
  taosWUnLockLatch(&pStream->lock);
  return 0;
}

int32_t mndStreamSetChkptIdAction(SMnode *pMnode, STrans *pTrans, SStreamTask* pTask, int64_t checkpointId, int64_t ts) {
  SRestoreCheckpointInfo req = {
      .taskId = pTask->id.taskId,
      .streamId = pTask->id.streamId,
      .checkpointId = checkpointId,
      .startTs = ts,
      .nodeId = pTask->info.nodeId,
      .transId = pTrans->id,
  };

  int32_t code = 0;
  int32_t blen;
  tEncodeSize(tEncodeRestoreCheckpointInfo, &req, blen, code);
  if (code < 0) {
    return terrno;
  }

  int32_t tlen = sizeof(SMsgHead) + blen;

  void *pBuf = taosMemoryMalloc(tlen);
  if (pBuf == NULL) {
    return terrno;
  }

  void    *abuf = POINTER_SHIFT(pBuf, sizeof(SMsgHead));
  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  code = tEncodeRestoreCheckpointInfo(&encoder, &req);
  tEncoderClear(&encoder);
  if (code == -1) {
    taosMemoryFree(pBuf);
    return code;
  }

  SMsgHead *pMsgHead = (SMsgHead *)pBuf;
  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(pTask->info.nodeId);

  SEpSet  epset = {0};
  bool    hasEpset = false;
  code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    taosMemoryFree(pBuf);
    return code;
  }

  code = setTransAction(pTrans, pBuf, tlen, TDMT_STREAM_CONSEN_CHKPT, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pBuf);
  }

  return code;
}


int32_t mndStreamSetCheckpointAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask, int64_t checkpointId,
                                     int8_t mndTrigger) {
  void   *buf;
  int32_t tlen;
  int32_t code = 0;
  SEpSet  epset = {0};
  bool    hasEpset = false;

  if ((code = mndBuildStreamCheckpointSourceReq(&buf, &tlen, pTask->info.nodeId, checkpointId, pTask->id.streamId,
                                                pTask->id.taskId, pTrans->id, mndTrigger)) < 0) {
    taosMemoryFree(buf);
    return code;
  }

  code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    taosMemoryFree(buf);
    return code;
  }

  code = setTransAction(pTrans, buf, tlen, TDMT_VND_STREAM_CHECK_POINT_SOURCE, &epset, TSDB_CODE_SYN_PROPOSE_NOT_READY,
                        TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != 0) {
    taosMemoryFree(buf);
  }

  return code;
}

int32_t mndStreamSetRestartAction(SMnode* pMnode, STrans *pTrans, SStreamObj* pStream) {
  return 0;
}


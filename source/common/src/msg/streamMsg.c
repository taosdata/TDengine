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

#include "streamMsg.h"
#include "tmsg.h"
#include "os.h"
#include "tcommon.h"

typedef struct STaskId {
  int64_t streamId;
  int64_t taskId;
} STaskId;

typedef enum EWindowType {
  WINDOW_TYPE_INTERVAL = 1,
  WINDOW_TYPE_SESSION,
  WINDOW_TYPE_STATE,
  WINDOW_TYPE_EVENT,
  WINDOW_TYPE_COUNT,
  WINDOW_TYPE_ANOMALY,
  WINDOW_TYPE_EXTERNAL,
  WINDOW_TYPE_PERIOD
} EWindowType;

typedef struct STaskCkptInfo {
  int64_t latestId;          // saved checkpoint id
  int64_t latestVer;         // saved checkpoint ver
  int64_t latestTime;        // latest checkpoint time
  int64_t latestSize;        // latest checkpoint size
  int8_t  remoteBackup;      // latest checkpoint backup done
  int64_t activeId;          // current active checkpoint id
  int32_t activeTransId;     // checkpoint trans id
  int8_t  failed;            // denote if the checkpoint is failed or not
  int8_t  consensusChkptId;  // required the consensus-checkpointId
  int64_t consensusTs;       //
} STaskCkptInfo;

typedef struct STaskStatusEntry {
  STaskId       id;
  int32_t       status;
  int32_t       statusLastDuration;  // to record the last duration of current status
  int64_t       stage;
  int32_t       nodeId;
  SVersionRange verRange;      // start/end version in WAL, only valid for source task
  int64_t       processedVer;  // only valid for source task
  double        inputQUsed;    // in MiB
  double        inputRate;
  double        procsThroughput;   // duration between one element put into input queue and being processed.
  double        procsTotal;        // duration between one element put into input queue and being processed.
  double        outputThroughput;  // the size of dispatched result blocks in bytes
  double        outputTotal;       // the size of dispatched result blocks in bytes
  double        sinkQuota;         // existed quota size for sink task
  double        sinkDataSize;      // sink to dst data size
  int64_t       startTime;
  int64_t       startCheckpointId;
  int64_t       startCheckpointVer;
  int64_t       hTaskId;
  STaskCkptInfo checkpointInfo;
  STaskNotifyEventStat notifyEventStat;
} STaskStatusEntry;

int32_t tEncodeStreamEpInfo(SEncoder* pEncoder, const SStreamUpstreamEpInfo* pInfo) {
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pInfo->taskId));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pInfo->nodeId));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pInfo->childId));
  TAOS_CHECK_RETURN(tEncodeSEpSet(pEncoder, &pInfo->epSet));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pInfo->stage));
  return 0;
}

int32_t tDecodeStreamEpInfo(SDecoder* pDecoder, SStreamUpstreamEpInfo* pInfo) {
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pInfo->taskId));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pInfo->nodeId));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pInfo->childId));
  TAOS_CHECK_RETURN(tDecodeSEpSet(pDecoder, &pInfo->epSet));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pInfo->stage));
  return 0;
}

int32_t tEncodeStreamTaskUpdateMsg(SEncoder* pEncoder, const SStreamTaskNodeUpdateMsg* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->streamId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->taskId));

  int32_t size = taosArrayGetSize(pMsg->pNodeList);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, size));

  for (int32_t i = 0; i < size; ++i) {
    SNodeUpdateInfo* pInfo = taosArrayGet(pMsg->pNodeList, i);
    if (pInfo == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pInfo->nodeId));
    TAOS_CHECK_EXIT(tEncodeSEpSet(pEncoder, &pInfo->prevEp));
    TAOS_CHECK_EXIT(tEncodeSEpSet(pEncoder, &pInfo->newEp));
  }

  // todo this new attribute will be result in being incompatible with previous version
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->transId));

  int32_t numOfTasks = taosArrayGetSize(pMsg->pTaskList);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, numOfTasks));

  for (int32_t i = 0; i < numOfTasks; ++i) {
    int32_t* pId = taosArrayGet(pMsg->pTaskList, i);
    if (pId == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, *(int32_t*)pId));
  }

  tEndEncode(pEncoder);
_exit:
  if (code) {
    return code;
  } else {
    return pEncoder->pos;
  }
}

int32_t tDecodeStreamTaskUpdateMsg(SDecoder* pDecoder, SStreamTaskNodeUpdateMsg* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->taskId));

  int32_t size = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));

  pMsg->pNodeList = taosArrayInit(size, sizeof(SNodeUpdateInfo));
  TSDB_CHECK_NULL(pMsg->pNodeList, code, lino, _exit, terrno);

  for (int32_t i = 0; i < size; ++i) {
    SNodeUpdateInfo info = {0};
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &info.nodeId));
    TAOS_CHECK_EXIT(tDecodeSEpSet(pDecoder, &info.prevEp));
    TAOS_CHECK_EXIT(tDecodeSEpSet(pDecoder, &info.newEp));

    if (taosArrayPush(pMsg->pNodeList, &info) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->transId));

  // number of tasks
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
  pMsg->pTaskList = taosArrayInit(size, sizeof(int32_t));
  if (pMsg->pTaskList == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < size; ++i) {
    int32_t id = 0;
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &id));
    if (taosArrayPush(pMsg->pTaskList, &id) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  tEndDecode(pDecoder);
_exit:
  return code;
}

void tDestroyNodeUpdateMsg(SStreamTaskNodeUpdateMsg* pMsg) {
  taosArrayDestroy(pMsg->pNodeList);
  taosArrayDestroy(pMsg->pTaskList);
  pMsg->pNodeList = NULL;
  pMsg->pTaskList = NULL;
}

int32_t tEncodeStreamTaskCheckReq(SEncoder* pEncoder, const SStreamTaskCheckReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->reqId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->upstreamNodeId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->upstreamTaskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->downstreamNodeId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->downstreamTaskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->childId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->stage));
  tEndEncode(pEncoder);

_exit:
  if (code) {
    return code;
  } else {
    return pEncoder->pos;
  }
}

int32_t tDecodeStreamTaskCheckReq(SDecoder* pDecoder, SStreamTaskCheckReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->reqId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->upstreamNodeId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->upstreamTaskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->downstreamNodeId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->downstreamTaskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->childId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->stage));
  tEndDecode(pDecoder);

_exit:
  return code;
}

int32_t tEncodeStreamTaskCheckRsp(SEncoder* pEncoder, const SStreamTaskCheckRsp* pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRsp->reqId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRsp->streamId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->upstreamNodeId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->upstreamTaskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->downstreamNodeId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->downstreamTaskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->childId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRsp->oldStage));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pRsp->status));
  tEndEncode(pEncoder);

_exit:
  if (code) {
    return code;
  } else {
    return pEncoder->pos;
  }
}

int32_t tDecodeStreamTaskCheckRsp(SDecoder* pDecoder, SStreamTaskCheckRsp* pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pRsp->reqId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pRsp->streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->upstreamNodeId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->upstreamTaskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->downstreamNodeId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->downstreamTaskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->childId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pRsp->oldStage));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pRsp->status));
  tEndDecode(pDecoder);

_exit:
  return code;
}

int32_t tEncodeStreamDispatchReq(SEncoder* pEncoder, const SStreamDispatchReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->stage));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->msgId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->srcVgId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->type));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->type));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->upstreamTaskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->upstreamChildId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->upstreamNodeId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->upstreamRelTaskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->blockNum));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->totalLen));

  if (taosArrayGetSize(pReq->data) != pReq->blockNum || taosArrayGetSize(pReq->dataLen) != pReq->blockNum) {
    uError("invalid dispatch req msg");
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

  for (int32_t i = 0; i < pReq->blockNum; i++) {
    int32_t* pLen = taosArrayGet(pReq->dataLen, i);
    void*    data = taosArrayGetP(pReq->data, i);
    if (data == NULL || pLen == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, *pLen));
    TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, data, *pLen));
  }
  tEndEncode(pEncoder);
_exit:
  if (code) {
    return code;
  } else {
    return pEncoder->pos;
  }
}

int32_t tDecodeStreamDispatchReq(SDecoder* pDecoder, SStreamDispatchReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->stage));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->msgId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->srcVgId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->type));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->type));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->upstreamTaskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->upstreamChildId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->upstreamNodeId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->upstreamRelTaskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->blockNum));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->totalLen));

  if ((pReq->data = taosArrayInit(pReq->blockNum, sizeof(void*))) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  if ((pReq->dataLen = taosArrayInit(pReq->blockNum, sizeof(int32_t))) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < pReq->blockNum; i++) {
    int32_t  len1;
    uint64_t len2;
    void*    data;
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &len1));
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, &data, &len2));

    if (len1 != len2) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }

    if (taosArrayPush(pReq->dataLen, &len1) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    if (taosArrayPush(pReq->data, &data) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  tEndDecode(pDecoder);
_exit:
  return code;
}

void tCleanupStreamDispatchReq(SStreamDispatchReq* pReq) {
  taosArrayDestroyP(pReq->data, NULL);
  taosArrayDestroy(pReq->dataLen);
}

int32_t tEncodeStreamRetrieveReq(SEncoder* pEncoder, const SStreamRetrieveReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->reqId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->dstNodeId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->dstTaskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->srcNodeId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->srcTaskId));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, (const uint8_t*)pReq->pRetrieve, pReq->retrieveLen));
  tEndEncode(pEncoder);

_exit:
  if (code) {
    return code;
  } else {
    return pEncoder->pos;
  }
}

int32_t tDecodeStreamRetrieveReq(SDecoder* pDecoder, SStreamRetrieveReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->reqId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->dstNodeId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->dstTaskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->srcNodeId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->srcTaskId));
  uint64_t len = 0;
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->pRetrieve, &len));
  pReq->retrieveLen = (int32_t)len;
  tEndDecode(pDecoder);

_exit:
  return code;
}

void tCleanupStreamRetrieveReq(SStreamRetrieveReq* pReq) { taosMemoryFree(pReq->pRetrieve); }

int32_t tEncodeStreamTask(SEncoder* pEncoder, const SStreamTask* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->type));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->streamId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->taskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->nodeId));
  // SKIP SESSIONID
  TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pTask->taskIdx));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->status));

_exit:

  return code;
}


int32_t tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, (int32_t*)&pTask->type));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->taskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->nodeId));
  // SKIP SESSIONID
  TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pTask->taskIdx));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, (int32_t*)&pTask->status));

_exit:

  return code;
}


int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->streamGId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->snodeId));
  int32_t vgLeaderNum = taosArrayGetSize(pReq->pVgLeaders);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, vgLeaderNum));
  for (int32_t i = 0; i < vgLeaderNum; ++i) {
    int32_t* vgId = taosArrayGet(pReq->pVgLeaders, i);
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, *vgId));
  }
  int32_t statusNum = taosArrayGetSize(pReq->pStreamStatus);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, statusNum));
  for (int32_t i = 0; i < statusNum; ++i) {
    SStmTaskStatusMsg* pStatus = taosArrayGet(pReq->pStreamStatus, i);
    TAOS_CHECK_EXIT(tEncodeStreamTask(pEncoder, (SStreamTask*)pStatus));
  }
  tEndEncode(pEncoder);

_exit:
  if (code) {
    return code;
  } else {
    return pEncoder->pos;
  }
}

int32_t tDecodeStreamHbMsg(SDecoder* pDecoder, SStreamHbMsg* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->streamGId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->snodeId));

  int32_t vgLearderNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &vgLearderNum));
  if (vgLearderNum > 0) {
    pReq->pVgLeaders = taosArrayInit(vgLearderNum, sizeof(int32_t));
    if (NULL == pReq->pVgLeaders) {
      code = terrno;
      goto _exit;
    }
  }
  for (int32_t i = 0; i < vgLearderNum; ++i) {
    int32_t vgId = 0;
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &vgId));
    if (NULL == taosArrayPush(pReq->pVgLeaders, &vgId)) {
      code = terrno;
      goto _exit;
    }
  }

  int32_t statusNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &statusNum));
  if (statusNum > 0) {
    pReq->pStreamStatus = taosArrayInit_s(sizeof(SStmTaskStatusMsg), statusNum);
    if (NULL == pReq->pStreamStatus) {
      code = terrno;
      goto _exit;
    }
  }
  for (int32_t i = 0; i < statusNum; ++i) {
    SStmTaskStatusMsg* pTask = taosArrayGet(pReq->pStreamStatus, i);
    if (NULL == pTask) {
      code = terrno;
      goto _exit;
    }
    TAOS_CHECK_EXIT(tDecodeStreamTask(pDecoder, (SStreamTask*)pTask));
  }
  
  tEndDecode(pDecoder);

_exit:
  return code;
}

void tCleanupStreamHbMsg(SStreamHbMsg* pMsg) {
  if (pMsg == NULL) {
    return;
  }

  //STREAMTODO
}

int32_t tEncodeSStreamReaderDeployFromTrigger(SEncoder* pEncoder, const SStreamReaderDeployFromTrigger* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->triggerTblName, pMsg->triggerTblName == NULL ? 0 : (int32_t)strlen(pMsg->triggerTblName) + 1));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->triggerTblUid));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->triggerTblType));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->deleteReCalc));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->deleteOutTbl));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->partitionCols, pMsg->partitionCols == NULL ? 0 : (int32_t)strlen(pMsg->partitionCols) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->triggerCols, pMsg->triggerCols == NULL ? 0 : (int32_t)strlen(pMsg->triggerCols) + 1));
  //TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->triggerPrevFilter, pMsg->triggerPrevFilter == NULL ? 0 : (int32_t)strlen(pMsg->triggerPrevFilter) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->triggerScanPlan, pMsg->triggerScanPlan == NULL ? 0 : (int32_t)strlen(pMsg->triggerScanPlan) + 1));

_exit:

  return code;
}

int32_t tEncodeSStreamReaderDeployFromCalc(SEncoder* pEncoder, const SStreamReaderDeployFromCalc* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->calcScanPlan, pMsg->calcScanPlan == NULL ? 0 : (int32_t)strlen(pMsg->calcScanPlan) + 1));

_exit:

  return code;
}


int32_t tEncodeSStreamReaderDeployMsg(SEncoder* pEncoder, const SStreamReaderDeployMsg* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->triggerReader));
  if (pMsg->triggerReader) {
    TAOS_CHECK_EXIT(tEncodeSStreamReaderDeployFromTrigger(pEncoder, &pMsg->msg.trigger));
  } else {
    TAOS_CHECK_EXIT(tEncodeSStreamReaderDeployFromCalc(pEncoder, &pMsg->msg.calc));
  }
  
_exit:

  return code;
}

int32_t tEncodeSStreamTaskAddr(SEncoder* pEncoder, const SStreamTaskAddr* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->taskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->nodeId));
  TAOS_CHECK_EXIT(tEncodeSEpSet(pEncoder, &pMsg->epset));

_exit:

  return code;
}

int32_t tEncodeSStreamRunnerTarget(SEncoder* pEncoder, const SStreamRunnerTarget* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeSStreamTaskAddr(pEncoder, &pMsg->addr));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->execReplica));

_exit:

  return code;
}


int32_t tEncodeSStreamTriggerDeployMsg(SEncoder* pEncoder, const SStreamTriggerDeployMsg* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->triggerType));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->igDisorder));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->fillHistory));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->fillHistoryFirst));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->lowLatencyCalc));

  int32_t addrSize = (int32_t)taosArrayGetSize(pMsg->pNotifyAddrUrls);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, addrSize));
  for (int32_t i = 0; i < addrSize; ++i) {
    const char *url = taosArrayGetP(pMsg->pNotifyAddrUrls, i);
    TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, url, NULL == url ? 0 : (int32_t)strlen(url) + 1));
  }
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->notifyEventTypes));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->notifyErrorHandle));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->notifyHistory));

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->maxDelay));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->fillHistoryStartTime));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->watermark));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->expiredTime));

  switch (pMsg->triggerType) {
    case WINDOW_TYPE_SESSION: {
      // session trigger
      TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pMsg->trigger.session.slotId));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.session.sessionVal));
      break;
    }
    case WINDOW_TYPE_STATE: {
      // state trigger
      TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pMsg->trigger.stateWin.slotId));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.stateWin.trueForDuration));
      break;
    }
    case WINDOW_TYPE_INTERVAL: {
      // slide trigger
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.sliding.interval));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.sliding.sliding));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.sliding.offset));
      break;
    }
    case WINDOW_TYPE_EVENT: {
      // event trigger
      int32_t eventWindowStartCondLen = pMsg->trigger.event.startCond == NULL ? 0 : (int32_t)strlen((char*)pMsg->trigger.event.startCond) + 1;
      int32_t eventWindowEndCondLen = pMsg->trigger.event.endCond == NULL ? 0 : (int32_t)strlen((char*)pMsg->trigger.event.endCond) + 1;

      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->trigger.event.startCond, eventWindowStartCondLen));
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->trigger.event.endCond, eventWindowEndCondLen));

      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.event.trueForDuration));
      break;
    }
    case WINDOW_TYPE_COUNT: {
      // count trigger
      int32_t countWindowCondColsLen = pMsg->trigger.count.condCols == NULL ? 0 : (int32_t)strlen((char*)pMsg->trigger.count.condCols) + 1;
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->trigger.count.condCols, countWindowCondColsLen));

      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.count.countVal));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.count.sliding));
      break;
    }
    case WINDOW_TYPE_PERIOD: {
      // period trigger
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.period.period));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.period.offset));
      break;
    }
    default:
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      break;
  }

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->eventTypes));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->placeHolderBitmap));

  int32_t readerNum = taosArrayGetSize(pMsg->readerList);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, readerNum));
  for (int32_t i = 0; i < readerNum; ++i) {
    SStreamTaskAddr* pAddr = (SStreamTaskAddr*)taosArrayGet(pMsg->readerList, i);
    TAOS_CHECK_EXIT(tEncodeSStreamTaskAddr(pEncoder, pAddr));
  }

  int32_t runnerNum = taosArrayGetSize(pMsg->runnerList);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, runnerNum));
  for (int32_t i = 0; i < runnerNum; ++i) {
    SStreamRunnerTarget* pTarget = (SStreamRunnerTarget*)taosArrayGet(pMsg->runnerList, i);
    TAOS_CHECK_EXIT(tEncodeSStreamRunnerTarget(pEncoder, pTarget));
  }

  //STREAMTODO VTABLE

_exit:

  return code;
}


int32_t tSerializeSFieldWithOptions(SEncoder* pEncoder, const SFieldWithOptions *pField) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pField->name));
  TAOS_CHECK_EXIT(tEncodeU8(pEncoder, pField->type));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pField->flags));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pField->bytes));
  TAOS_CHECK_EXIT(tEncodeU32(pEncoder, pField->compress));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pField->typeMod));

_exit:

  return code;
}


int32_t tEncodeSStreamRunnerDeployMsg(SEncoder* pEncoder, const SStreamRunnerDeployMsg* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->execReplica));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->pPlan, NULL == pMsg->pPlan ? 0 : (int32_t)strlen(pMsg->pPlan) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->outDBFName, NULL == pMsg->outDBFName ? 0 : (int32_t)strlen(pMsg->outDBFName) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->outTblName, NULL == pMsg->outTblName ? 0 : (int32_t)strlen(pMsg->outTblName) + 1));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->outTblType));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->calcNotifyOnly));

  int32_t addrSize = (int32_t)taosArrayGetSize(pMsg->pNotifyAddrUrls);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, addrSize));
  for (int32_t i = 0; i < addrSize; ++i) {
    const char *url = taosArrayGetP(pMsg->pNotifyAddrUrls, i);
    TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, url, NULL == url ? 0 : (int32_t)strlen(url) + 1));
  }
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->notifyErrorHandle));

  int32_t outColNum = (int32_t)taosArrayGetSize(pMsg->outCols);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, outColNum));
  for (int32_t i = 0; i < outColNum; ++i) {
    SFieldWithOptions *pCol = taosArrayGet(pMsg->outCols, i);
    TAOS_CHECK_EXIT(tSerializeSFieldWithOptions(pEncoder, pCol));
  }

  int32_t outTagNum = (int32_t)taosArrayGetSize(pMsg->outTags);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, outTagNum));
  for (int32_t i = 0; i < outTagNum; ++i) {
    SFieldWithOptions *pTag = taosArrayGet(pMsg->outTags, i);
    TAOS_CHECK_EXIT(tSerializeSFieldWithOptions(pEncoder, pTag));
  }

  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pMsg->outStbUid));

  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->subTblNameExpr, NULL == pMsg->subTblNameExpr ? 0 : (int32_t)strlen(pMsg->subTblNameExpr) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->tagValueExpr, NULL == pMsg->tagValueExpr ? 0 : (int32_t)strlen(pMsg->tagValueExpr) + 1));

  int32_t forceOutColsSize = (int32_t)taosArrayGetSize(pMsg->forceOutCols);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, forceOutColsSize));
  for (int32_t i = 0; i < forceOutColsSize; ++i) {
    SStreamOutCol *pCoutCol = (SStreamOutCol*)taosArrayGet(pMsg->forceOutCols, i);
    int32_t        exprLen = pCoutCol->expr == NULL ? 0 : (int32_t)strlen((char*)pCoutCol->expr) + 1;

    TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pCoutCol->expr, exprLen));
    TAOS_CHECK_EXIT(tEncodeU8(pEncoder, pCoutCol->type.type));
    TAOS_CHECK_EXIT(tEncodeU8(pEncoder, pCoutCol->type.precision));
    TAOS_CHECK_EXIT(tEncodeU8(pEncoder, pCoutCol->type.scale));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pCoutCol->type.bytes));
  }

_exit:

  return code;
}


int32_t tEncodeSStmTaskDeploy(SEncoder* pEncoder, const SStmTaskDeploy* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeStreamTask(pEncoder, (SStreamTask*)&pTask->task));
  switch (pTask->task.type) {
    case STREAM_READER_TASK:
      TAOS_CHECK_EXIT(tEncodeSStreamReaderDeployMsg(pEncoder, &pTask->msg.reader));
      break;
    case STREAM_TRIGGER_TASK:
      TAOS_CHECK_EXIT(tEncodeSStreamTriggerDeployMsg(pEncoder, &pTask->msg.trigger));
      break;
    case STREAM_RUNNER_TASK:
      TAOS_CHECK_EXIT(tEncodeSStreamRunnerDeployMsg(pEncoder, &pTask->msg.runner));
      break;
    default:
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      break;
  }
  
_exit:

  return code;
}


int32_t tEncodeSStmStreamDeploy(SEncoder* pEncoder, const SStmStreamDeploy* pStream) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pStream->streamId));

  int32_t readerNum = taosArrayGetSize(pStream->readerTasks);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, readerNum));
  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskDeploy** ppDeploy = taosArrayGet(pStream->readerTasks, i);
    TAOS_CHECK_EXIT(tEncodeSStmTaskDeploy(pEncoder, *ppDeploy));
  }

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pStream->triggerTask ? 1 : 0));
  if (pStream->triggerTask) {
    TAOS_CHECK_EXIT(tEncodeSStmTaskDeploy(pEncoder, pStream->triggerTask));
  }
  
  int32_t runnerNum = taosArrayGetSize(pStream->runnerTasks);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, runnerNum));
  for (int32_t i = 0; i < runnerNum; ++i) {
    SStmTaskDeploy** ppDeploy = taosArrayGet(pStream->runnerTasks, i);
    TAOS_CHECK_EXIT(tEncodeSStmTaskDeploy(pEncoder, *ppDeploy));
  }

_exit:

  return code;
}

int32_t tEncodeSStreamStartTaskMsg(SEncoder* pEncoder, const SStreamStartTaskMsg* pStart) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pStart->header.msgType));

_exit:

  return code;
}

int32_t tEncodeSStreamTaskStart(SEncoder* pEncoder, const SStreamTaskStart* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeStreamTask(pEncoder, (SStreamTask*)&pTask->task));
  TAOS_CHECK_EXIT(tEncodeSStreamStartTaskMsg(pEncoder, (SStreamStartTaskMsg*)&pTask->startMsg));

_exit:

  return code;
}

int32_t tEncodeSStreamUndeployTaskMsg(SEncoder* pEncoder, const SStreamUndeployTaskMsg* pUndeploy) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pUndeploy->header.msgType));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pUndeploy->doCheckpoint));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pUndeploy->doCleanup));

_exit:

  return code;
}

int32_t tEncodeSStreamTaskUndeploy(SEncoder* pEncoder, const SStreamTaskUndeploy* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeStreamTask(pEncoder, (SStreamTask*)&pTask->task));
  TAOS_CHECK_EXIT(tEncodeSStreamUndeployTaskMsg(pEncoder, (SStreamUndeployTaskMsg*)&pTask->undeployMsg));

_exit:

  return code;
}


int32_t tEncodeStreamHbRsp(SEncoder* pEncoder, const SMStreamHbRspMsg* pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->streamGId));
  int32_t deployNum = taosArrayGetSize(pRsp->deploy.streamList);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, deployNum));
  for (int32_t i = 0; i < deployNum; ++i) {
    SStmStreamDeploy* pStream = (SStmStreamDeploy*)taosArrayGet(pRsp->deploy.streamList, i);
    TAOS_CHECK_EXIT(tEncodeSStmStreamDeploy(pEncoder, pStream));
  }

  int32_t startNum = taosArrayGetSize(pRsp->start.taskList);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, startNum));
  for (int32_t i = 0; i < startNum; ++i) {
    SStreamTaskStart* pTask = (SStreamTaskStart*)taosArrayGet(pRsp->start.taskList, i);
    TAOS_CHECK_EXIT(tEncodeSStreamTaskStart(pEncoder, pTask));
  }

  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pRsp->undeploy.undeployAll));
  if (!pRsp->undeploy.undeployAll) {
    int32_t undeployNum = taosArrayGetSize(pRsp->undeploy.taskList);
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, undeployNum));
    for (int32_t i = 0; i < undeployNum; ++i) {
      SStreamTaskUndeploy* pTask = (SStreamTaskUndeploy*)taosArrayGet(pRsp->undeploy.taskList, i);
      TAOS_CHECK_EXIT(tEncodeSStreamTaskUndeploy(pEncoder, pTask));
    }
  }
  
_exit:

  tEndEncode(pEncoder);

  return code;
}

int32_t tDecodeSStreamReaderDeployFromTrigger(SDecoder* pDecoder, SStreamReaderDeployFromTrigger* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->triggerTblName, NULL));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->triggerTblUid));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->triggerTblType));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->deleteReCalc));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->deleteOutTbl));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->partitionCols, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->triggerCols, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->triggerScanPlan, NULL));

_exit:

  return code;
}


int32_t tDecodeSStreamReaderDeployFromCalc(SDecoder* pDecoder, SStreamReaderDeployFromCalc* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->calcScanPlan, NULL));

_exit:

  return code;
}


int32_t tDecodeSStreamReaderDeployMsg(SDecoder* pDecoder, SStreamReaderDeployMsg* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->triggerReader));
  if (pMsg->triggerReader) {
    TAOS_CHECK_EXIT(tDecodeSStreamReaderDeployFromTrigger(pDecoder, &pMsg->msg.trigger));
  } else {
    TAOS_CHECK_EXIT(tDecodeSStreamReaderDeployFromCalc(pDecoder, &pMsg->msg.calc));
  }
  
_exit:

  return code;
}


int32_t tDecodeSStreamTaskAddr(SDecoder* pDecoder, SStreamTaskAddr* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->taskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->nodeId));
  TAOS_CHECK_EXIT(tDecodeSEpSet(pDecoder, &pMsg->epset));

_exit:

  return code;
}


int32_t tDecodeSStreamRunnerTarget(SDecoder* pDecoder, SStreamRunnerTarget* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeSStreamTaskAddr(pDecoder, &pMsg->addr));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->execReplica));

_exit:

  return code;
}


int32_t tDecodeSStreamTriggerDeployMsg(SDecoder* pDecoder, SStreamTriggerDeployMsg* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->triggerType));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->igDisorder));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->fillHistory));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->fillHistoryFirst));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->lowLatencyCalc));

  int32_t addrSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &addrSize));
  if (addrSize > 0) {
    pMsg->pNotifyAddrUrls = taosArrayInit_s(POINTER_BYTES, addrSize);
    TSDB_CHECK_NULL(pMsg->pNotifyAddrUrls, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < addrSize; ++i) {
    const char **url = taosArrayGet(pMsg->pNotifyAddrUrls, i);
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)url, NULL));
  }
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->notifyEventTypes));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->notifyErrorHandle));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->notifyHistory));

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->maxDelay));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->fillHistoryStartTime));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->watermark));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->expiredTime));

  switch (pMsg->triggerType) {
    case WINDOW_TYPE_SESSION:
      // session trigger
      TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pMsg->trigger.session.slotId));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.session.sessionVal));
      break;
    case WINDOW_TYPE_STATE:
      // state trigger
      TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pMsg->trigger.stateWin.slotId));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.stateWin.trueForDuration));
      break;
    
    case WINDOW_TYPE_INTERVAL:
      // slide trigger
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.sliding.interval));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.sliding.sliding));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.sliding.offset));
      break;
    
    case WINDOW_TYPE_EVENT:
      // event trigger
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->trigger.event.startCond, NULL));
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->trigger.event.endCond, NULL));
      
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.event.trueForDuration));
      break;
    
    case WINDOW_TYPE_COUNT:
      // count trigger
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->trigger.count.condCols, NULL));
      
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.count.countVal));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.count.sliding));
      break;
    
    case WINDOW_TYPE_PERIOD:
      // period trigger
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.period.period));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.period.offset));
      break;
    default:
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      break;
  }

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->eventTypes));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->placeHolderBitmap));

  int32_t readerNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &readerNum));
  if (readerNum > 0) {
    pMsg->readerList = taosArrayInit_s(sizeof(SStreamTaskAddr), readerNum);
    TSDB_CHECK_NULL(pMsg->readerList, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < readerNum; ++i) {
    SStreamTaskAddr* pAddr = (SStreamTaskAddr*)taosArrayGet(pMsg->readerList, i);
    TAOS_CHECK_EXIT(tDecodeSStreamTaskAddr(pDecoder, pAddr));
  }

  int32_t runnerNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &runnerNum));
  if (runnerNum > 0) {
    pMsg->runnerList = taosArrayInit_s(sizeof(SStreamRunnerTarget), runnerNum);
    TSDB_CHECK_NULL(pMsg->runnerList, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < runnerNum; ++i) {
    SStreamRunnerTarget* pTarget = (SStreamRunnerTarget*)taosArrayGet(pMsg->runnerList, i);
    TAOS_CHECK_EXIT(tDecodeSStreamRunnerTarget(pDecoder, pTarget));
  }

  //STREAMTODO VTABLE

_exit:

  return code;
}



int32_t tDeserializeSFieldWithOptions(SDecoder *pDecoder, SFieldWithOptions *pField) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pField->name));
  TAOS_CHECK_EXIT(tDecodeU8(pDecoder, &pField->type));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pField->flags));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pField->bytes));
  TAOS_CHECK_EXIT(tDecodeU32(pDecoder, &pField->compress));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pField->typeMod));

_exit:

  return code;
}



int32_t tDecodeSStreamRunnerDeployMsg(SDecoder* pDecoder, SStreamRunnerDeployMsg* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->execReplica));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->pPlan, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->outDBFName, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->outTblName, NULL));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->outTblType));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->calcNotifyOnly));

  int32_t addrSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &addrSize));
  if (addrSize > 0) {
    pMsg->pNotifyAddrUrls = taosArrayInit_s(POINTER_BYTES, addrSize);
    TSDB_CHECK_NULL(pMsg->pNotifyAddrUrls, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < addrSize; ++i) {
    const char **url = taosArrayGet(pMsg->pNotifyAddrUrls, i);
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)url, NULL));
  }
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->notifyErrorHandle));

  int32_t outColNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &outColNum));
  if (outColNum > 0) {
    pMsg->outCols = taosArrayInit_s(sizeof(SFieldWithOptions), outColNum);
    TSDB_CHECK_NULL(pMsg->outCols, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < outColNum; ++i) {
    SFieldWithOptions *pCol = taosArrayGet(pMsg->outCols, i);
    TAOS_CHECK_EXIT(tDeserializeSFieldWithOptions(pDecoder, pCol));
  }

  int32_t outTagNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &outTagNum));
  if (outTagNum > 0) {
    pMsg->outTags = taosArrayInit_s(sizeof(SFieldWithOptions), outTagNum);
    TSDB_CHECK_NULL(pMsg->outTags, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < outTagNum; ++i) {
    SFieldWithOptions *pTag = taosArrayGet(pMsg->outTags, i);
    TAOS_CHECK_EXIT(tDeserializeSFieldWithOptions(pDecoder, pTag));
  }

  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pMsg->outStbUid));

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->subTblNameExpr, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->tagValueExpr, NULL));

  int32_t forceOutColsSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &forceOutColsSize));
  if (forceOutColsSize > 0) {
    pMsg->forceOutCols = taosArrayInit_s(sizeof(SStreamOutCol), forceOutColsSize);
    TSDB_CHECK_NULL(pMsg->forceOutCols, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < forceOutColsSize; ++i) {
    SStreamOutCol *pCoutCol = (SStreamOutCol*)taosArrayGet(pMsg->forceOutCols, i);

    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pCoutCol->expr, NULL));
    TAOS_CHECK_EXIT(tDecodeU8(pDecoder, &pCoutCol->type.type));
    TAOS_CHECK_EXIT(tDecodeU8(pDecoder, &pCoutCol->type.precision));
    TAOS_CHECK_EXIT(tDecodeU8(pDecoder, &pCoutCol->type.scale));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pCoutCol->type.bytes));
  }

_exit:

  return code;
}

int32_t tDecodeSStmTaskDeploy(SDecoder* pDecoder, SStmTaskDeploy* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeStreamTask(pDecoder, (SStreamTask*)&pTask->task));
  switch (pTask->task.type) {
    case STREAM_READER_TASK:
      TAOS_CHECK_EXIT(tDecodeSStreamReaderDeployMsg(pDecoder, &pTask->msg.reader));
      break;
    case STREAM_TRIGGER_TASK:
      TAOS_CHECK_EXIT(tDecodeSStreamTriggerDeployMsg(pDecoder, &pTask->msg.trigger));
      break;
    case STREAM_RUNNER_TASK:
      TAOS_CHECK_EXIT(tDecodeSStreamRunnerDeployMsg(pDecoder, &pTask->msg.runner));
      break;
    default:
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      break;
  }
  
_exit:

  return code;
}


int32_t tDecodeSStmStreamDeploy(SDecoder* pDecoder, SStmStreamDeploy* pStream) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pStream->streamId));

  int32_t readerNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &readerNum));
  if (readerNum > 0) {
    pStream->readerTasks = taosArrayInit_s(sizeof(SStmTaskDeploy), readerNum);
    TSDB_CHECK_NULL(pStream->readerTasks, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskDeploy* pTask = taosArrayGet(pStream->readerTasks, i);
    TAOS_CHECK_EXIT(tDecodeSStmTaskDeploy(pDecoder, pTask));
  }

  int32_t triggerTask = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &triggerTask));
  if (triggerTask) {
    pStream->triggerTask = taosMemoryCalloc(1, sizeof(SStmTaskDeploy));
    TSDB_CHECK_NULL(pStream->triggerTask, code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(tDecodeSStmTaskDeploy(pDecoder, pStream->triggerTask));
  }
  
  int32_t runnerNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &runnerNum));
  if (runnerNum > 0) {
    pStream->runnerTasks = taosArrayInit_s(sizeof(SStmTaskDeploy), runnerNum);
    TSDB_CHECK_NULL(pStream->runnerTasks, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < runnerNum; ++i) {
    SStmTaskDeploy* pTask = taosArrayGet(pStream->runnerTasks, i);
    TAOS_CHECK_EXIT(tDecodeSStmTaskDeploy(pDecoder, pTask));
  }

_exit:

  return code;
}


int32_t tDecodeSStreamStartTaskMsg(SDecoder* pDecoder, SStreamStartTaskMsg* pStart) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pStart->header.msgType));

_exit:

  return code;
}


int32_t tDecodeSStreamTaskStart(SDecoder* pDecoder, SStreamTaskStart* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeStreamTask(pDecoder, (SStreamTask*)&pTask->task));
  TAOS_CHECK_EXIT(tDecodeSStreamStartTaskMsg(pDecoder, (SStreamStartTaskMsg*)&pTask->startMsg));

_exit:

  return code;
}


int32_t tDecodeSStreamUndeployTaskMsg(SDecoder* pDecoder, SStreamUndeployTaskMsg* pUndeploy) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pUndeploy->header.msgType));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pUndeploy->doCheckpoint));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pUndeploy->doCleanup));

_exit:

  return code;
}


int32_t tDecodeSStreamTaskUndeploy(SDecoder* pDecoder, SStreamTaskUndeploy* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeStreamTask(pDecoder, (SStreamTask*)&pTask->task));
  TAOS_CHECK_EXIT(tDecodeSStreamUndeployTaskMsg(pDecoder, (SStreamUndeployTaskMsg*)&pTask->undeployMsg));

_exit:

  return code;
}



int32_t tDecodeStreamHbRsp(SDecoder* pDecoder, SMStreamHbRspMsg* pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->streamGId));
  int32_t deployNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &deployNum));
  if (deployNum > 0) {
    pRsp->deploy.streamList = taosArrayInit_s(sizeof(SStmStreamDeploy), deployNum);
    TSDB_CHECK_NULL(pRsp->deploy.streamList, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < deployNum; ++i) {
    SStmStreamDeploy* pStream = taosArrayGet(pRsp->deploy.streamList, i);
    TAOS_CHECK_EXIT(tDecodeSStmStreamDeploy(pDecoder, pStream));
  }

  int32_t startNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &startNum));
  if (startNum > 0) {
    pRsp->start.taskList = taosArrayInit_s(sizeof(SStreamTaskStart), startNum);
    TSDB_CHECK_NULL(pRsp->start.taskList, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < startNum; ++i) {
    SStreamTaskStart* pTask = (SStreamTaskStart*)taosArrayGet(pRsp->start.taskList, i);
    TAOS_CHECK_EXIT(tDecodeSStreamTaskStart(pDecoder, pTask));
  }

  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pRsp->undeploy.undeployAll));
  if (!pRsp->undeploy.undeployAll) {
    int32_t undeployNum = 0;
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &undeployNum));
    if (undeployNum > 0) {
      pRsp->undeploy.taskList = taosArrayInit_s(sizeof(SStreamTaskUndeploy), undeployNum);
      TSDB_CHECK_NULL(pRsp->undeploy.taskList, code, lino, _exit, terrno);
    }
    for (int32_t i = 0; i < undeployNum; ++i) {
      SStreamTaskUndeploy* pTask = (SStreamTaskUndeploy*)taosArrayGet(pRsp->undeploy.taskList, i);
      TAOS_CHECK_EXIT(tDecodeSStreamTaskUndeploy(pDecoder, pTask));
    }
  }  

  tEndDecode(pDecoder);

_exit:
  return code;
}

int32_t tEncodeStreamTaskRunReq (SEncoder* pEncoder, const SStreamTaskRunReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->reqType));
  tEndEncode(pEncoder);

_exit:
  return code;
}

int32_t tDecodeStreamTaskRunReq(SDecoder* pDecoder, SStreamTaskRunReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->reqType));
  tEndDecode(pDecoder);

_exit:
  return code;
}

int32_t tEncodeStreamTaskStopReq(SEncoder* pEncoder, const SStreamTaskStopReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  tEndEncode(pEncoder);

_exit:
  return code;
}

int32_t tDecodeStreamTaskStopReq(SDecoder* pDecoder, SStreamTaskStopReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->streamId));
  tEndDecode(pDecoder);

_exit:
  return code;

}


int32_t tSerializeSCMCreateStreamReqImpl(SEncoder* pEncoder, const SCMCreateStreamReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  // name part
  int32_t sqlLen = pReq->sql == NULL ? 0 : (int32_t)strlen(pReq->sql) + 1;
  int32_t nameLen = pReq->name == NULL ? 0 : (int32_t)strlen(pReq->name) + 1;
  int32_t outDbLen = pReq->outDB == NULL ? 0 : (int32_t)strlen(pReq->outDB) + 1;
  int32_t streamDBLen = pReq->streamDB == NULL ? 0 : (int32_t)strlen(pReq->streamDB) + 1;
  int32_t triggerDBLen = pReq->triggerDB == NULL ? 0 : (int32_t)strlen(pReq->triggerDB) + 1;
  int32_t triggerTblNameLen = pReq->triggerTblName == NULL ? 0 : (int32_t)strlen(pReq->triggerTblName) + 1;
  int32_t outTblNameLen = pReq->outTblName == NULL ? 0 : (int32_t)strlen(pReq->outTblName) + 1;

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));

  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->name, nameLen));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->sql, sqlLen));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->outDB, outDbLen));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->streamDB, streamDBLen));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->triggerDB, triggerDBLen));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->triggerTblName, triggerTblNameLen));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->outTblName, outTblNameLen));

  int32_t calcDbSize = (int32_t)taosArrayGetSize(pReq->calcDB);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, calcDbSize));
  for (int32_t i = 0; i < calcDbSize; ++i) {
    const char *dbName = taosArrayGetP(pReq->calcDB, i);
    TAOS_CHECK_EXIT((tEncodeCStr(pEncoder, dbName)));
  }

  // trigger control part
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->igExists));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->triggerType));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->igDisorder));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->deleteReCalc));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->deleteOutTbl));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->fillHistory));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->fillHistoryFirst));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->calcNotifyOnly));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->lowLatencyCalc));

  // notify part
  int32_t addrSize = (int32_t)taosArrayGetSize(pReq->pNotifyAddrUrls);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, addrSize));
  for (int32_t i = 0; i < addrSize; ++i) {
    const char *url = taosArrayGetP(pReq->pNotifyAddrUrls, i);
    TAOS_CHECK_EXIT((tEncodeCStr(pEncoder, url)));
  }
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->notifyEventTypes));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->notifyErrorHandle));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->notifyHistory));

  // out table part

  // trigger cols and partition cols
  int32_t triggerColsLen = pReq->triggerCols == NULL ? 0 : (int32_t)strlen((char*)pReq->triggerCols) + 1;
  int32_t partitionColsLen = pReq->partitionCols == NULL ? 0 : (int32_t)strlen((char*)pReq->partitionCols) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->triggerCols, triggerColsLen));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->partitionCols, partitionColsLen));

  // out col
  int32_t outColSize = (int32_t )taosArrayGetSize(pReq->outCols);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, outColSize));
  for (int32_t i = 0; i < outColSize; ++i) {
    SFieldWithOptions *pField = taosArrayGet(pReq->outCols, i);
    TAOS_CHECK_EXIT(tSerializeSFieldWithOptions(pEncoder, pField));
  }

  // out tag
  int32_t outTagSize = (int32_t )taosArrayGetSize(pReq->outTags);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, outTagSize));
  for (int32_t i = 0; i < outTagSize; ++i) {
    SField *pField = taosArrayGet(pReq->outTags, i);
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pField->type));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pField->flags));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pField->bytes));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pField->name));
  }

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->maxDelay));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->fillHistoryStartTime));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->watermark));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->expiredTime));

  switch (pReq->triggerType) {
    case WINDOW_TYPE_SESSION: {
      // session trigger
      TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pReq->trigger.session.slotId));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.session.sessionVal));
      break;
    }
    case WINDOW_TYPE_STATE: {
      // state trigger
      TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pReq->trigger.stateWin.slotId));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.stateWin.trueForDuration));
      break;
    }
    case WINDOW_TYPE_INTERVAL: {
      // slide trigger
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->trigger.sliding.intervalUnit));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->trigger.sliding.slidingUnit));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->trigger.sliding.offsetUnit));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->trigger.sliding.soffsetUnit));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->trigger.sliding.precision));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.sliding.interval));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.sliding.offset));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.sliding.sliding));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.sliding.soffset));
      break;
    }
    case WINDOW_TYPE_EVENT: {
      // event trigger
      int32_t eventWindowStartCondLen = pReq->trigger.event.startCond == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.event.startCond) + 1;
      int32_t eventWindowEndCondLen = pReq->trigger.event.endCond == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.event.endCond) + 1;

      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->trigger.event.startCond, eventWindowStartCondLen));
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->trigger.event.endCond, eventWindowEndCondLen));

      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.event.trueForDuration));
      break;
    }
    case WINDOW_TYPE_COUNT: {
      // count trigger
      int32_t countWindowCondColsLen = pReq->trigger.count.condCols == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.count.condCols) + 1;
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->trigger.count.condCols, countWindowCondColsLen));

      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.count.countVal));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.count.sliding));
      break;
    }
    case WINDOW_TYPE_PERIOD: {
      // period trigger
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.period.period));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.period.offset));
      break;
    }
  }

  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->triggerTblType));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pReq->triggerTblUid));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->outTblType));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->outStbExists));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pReq->outStbUid));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->eventTypes));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->flags));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->tsmaId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->placeHolderBitmap));

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->triggerTblVgId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->outTblVgId));

  int32_t triggerScanPlanLen = pReq->triggerScanPlan == NULL ? 0 : (int32_t)strlen((char*)pReq->triggerScanPlan) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->triggerScanPlan, triggerScanPlanLen));

  int32_t calcScanPlanListSize = (int32_t)taosArrayGetSize(pReq->calcScanPlanList);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, calcScanPlanListSize));
  for (int32_t i = 0; i < calcScanPlanListSize; ++i) {
    SStreamCalcScan* pCalcScanPlan = (SStreamCalcScan*)taosArrayGet(pReq->calcScanPlanList, i);
    int32_t          vgListSize = (int32_t)taosArrayGetSize(pCalcScanPlan->vgList);
    int32_t          scanPlanLen = pCalcScanPlan->scanPlan == NULL ? 0 : (int32_t)strlen((char*)pCalcScanPlan->scanPlan) + 1;
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, vgListSize));
    for (int32_t j = 0; j < vgListSize; ++j) {
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, *(int32_t*)taosArrayGet(pCalcScanPlan->vgList, j)));
    }
    TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pCalcScanPlan->scanPlan, scanPlanLen));
  }

  int32_t vgNum = (int32_t)taosArrayGetSize(pReq->pVSubTables);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, vgNum));
  for (int32_t i = 0; i < vgNum; ++i) {
    TAOS_CHECK_EXIT(tSerializeSVSubTablesRspImpl(pEncoder, (SVSubTablesRsp*)taosArrayGet(pReq->pVSubTables, i)));
  }

  int32_t calcPlanLen = pReq->calcPlan == NULL ? 0 : (int32_t)strlen((char*)pReq->calcPlan) + 1;
  int32_t subTblNameExprLen = pReq->subTblNameExpr == NULL ? 0 : (int32_t)strlen((char*)pReq->subTblNameExpr) + 1;
  int32_t tagValueExprLen = pReq->tagValueExpr == NULL ? 0 : (int32_t)strlen((char*)pReq->tagValueExpr) + 1;

  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->calcPlan, calcPlanLen));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->subTblNameExpr, subTblNameExprLen));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->tagValueExpr, tagValueExprLen));

  int32_t forceOutColsSize = (int32_t)taosArrayGetSize(pReq->forceOutCols);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, forceOutColsSize));
  for (int32_t i = 0; i < forceOutColsSize; ++i) {
    SStreamOutCol *pCoutCol = (SStreamOutCol*)taosArrayGet(pReq->forceOutCols, i);
    int32_t        exprLen = pCoutCol->expr == NULL ? 0 : (int32_t)strlen((char*)pCoutCol->expr) + 1;

    TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pCoutCol->expr, exprLen));
    TAOS_CHECK_EXIT(tEncodeU8(pEncoder, pCoutCol->type.type));
    TAOS_CHECK_EXIT(tEncodeU8(pEncoder, pCoutCol->type.precision));
    TAOS_CHECK_EXIT(tEncodeU8(pEncoder, pCoutCol->type.scale));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pCoutCol->type.bytes));
  }

_exit:

  if (code) {
    return code;
  }
  
  return 0;
}

int32_t tSerializeSCMCreateStreamReq(void *buf, int32_t bufLen, const SCMCreateStreamReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tSerializeSCMCreateStreamReqImpl(&encoder, pReq));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    int32_t tlen = encoder.pos;
    tEncoderClear(&encoder);
    return tlen;
  }
  return 0;
}


int32_t tDeserializeSCMCreateStreamReqImpl(SDecoder *pDecoder, SCMCreateStreamReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->streamId));

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->name, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->sql, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->outDB, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->streamDB, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->triggerDB, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->triggerTblName, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->outTblName, NULL));

  int32_t calcDbSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &calcDbSize));
  pReq->calcDB = taosArrayInit(calcDbSize, POINTER_BYTES);
  if (pReq->calcDB == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < calcDbSize; ++i) {
    char *calcDb = NULL;
    TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &calcDb));
    calcDb = taosStrndup(calcDb, TSDB_DB_FNAME_LEN);
    if (calcDb == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    if (taosArrayPush(pReq->calcDB, &calcDb) == NULL) {
      taosMemoryFree(calcDb);
      TAOS_CHECK_EXIT(terrno);
    }
  }

  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->igExists));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->triggerType));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->igDisorder));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->deleteReCalc));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->deleteOutTbl));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->fillHistory));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->fillHistoryFirst));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->calcNotifyOnly));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->lowLatencyCalc));

  int32_t addrSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &addrSize));
  pReq->pNotifyAddrUrls = taosArrayInit(addrSize, POINTER_BYTES);
  if (pReq->pNotifyAddrUrls == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < addrSize; ++i) {
    char *url = NULL;
    TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &url));
    url = taosStrndup(url, TSDB_STREAM_NOTIFY_URL_LEN);
    if (url == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    if (taosArrayPush(pReq->pNotifyAddrUrls, &url) == NULL) {
      taosMemoryFree(url);
      TAOS_CHECK_EXIT(terrno);
    }
  }
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->notifyEventTypes));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->notifyErrorHandle));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->notifyHistory));

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->triggerCols, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->partitionCols, NULL));

  int32_t outColSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &outColSize));
  if (outColSize > 0) {
    pReq->outCols = taosArrayInit_s(sizeof(SFieldWithOptions), outColSize);
    if (pReq->outCols == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = 0; i < outColSize; ++i) {
      SFieldWithOptions* pField = taosArrayGet(pReq->outCols, i);
      TAOS_CHECK_EXIT(tDeserializeSFieldWithOptions(pDecoder, pField));
    }
  }

  int32_t outTagSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &outTagSize));
  if (outTagSize > 0) {
    pReq->outTags = taosArrayInit(outTagSize, sizeof(SField));
    if (pReq->outTags == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = 0; i < outTagSize; ++i) {
      SField field = {0};
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &field.type));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &field.flags));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &field.bytes));
      TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, field.name));
      if (taosArrayPush(pReq->outTags, &field) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->maxDelay));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->fillHistoryStartTime));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->watermark));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->expiredTime));

  switch (pReq->triggerType) {
    case WINDOW_TYPE_SESSION: {
      // session trigger
      TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pReq->trigger.session.slotId));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.session.sessionVal));
      break;
    }
      case WINDOW_TYPE_STATE: {
        // state trigger
        TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pReq->trigger.stateWin.slotId));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.stateWin.trueForDuration));
        break;
      }
      case WINDOW_TYPE_INTERVAL: {
        // slide trigger
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->trigger.sliding.intervalUnit));
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->trigger.sliding.slidingUnit));
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->trigger.sliding.offsetUnit));
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->trigger.sliding.soffsetUnit));
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->trigger.sliding.precision));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.sliding.interval));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.sliding.offset));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.sliding.sliding));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.sliding.soffset));
        break;
      }
      case WINDOW_TYPE_EVENT: {
        // event trigger
        int32_t eventWindowStartCondLen = pReq->trigger.event.startCond == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.event.startCond);
        int32_t eventWindowEndCondLen = pReq->trigger.event.endCond == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.event.endCond);

        TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->trigger.event.startCond, NULL));
        TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->trigger.event.endCond, NULL));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.event.trueForDuration));
        break;
      }
      case WINDOW_TYPE_COUNT: {
        int32_t countWindowCondColsLen = pReq->trigger.count.condCols == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.count.condCols);
        TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->trigger.count.condCols, NULL));

        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.count.countVal));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.count.sliding));
        break;
      }
      case WINDOW_TYPE_PERIOD: {
        // period trigger
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.period.period));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.period.offset));
        break;
      }
      default:
        TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->triggerTblType));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pReq->triggerTblUid));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->outTblType));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->outStbExists));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pReq->outStbUid));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->eventTypes));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->flags));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->tsmaId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->placeHolderBitmap));

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->triggerTblVgId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->outTblVgId));

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->triggerScanPlan, NULL));

  int32_t calcScanPlanListSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &calcScanPlanListSize));
  if (calcScanPlanListSize > 0) {
    pReq->calcScanPlanList = taosArrayInit(calcScanPlanListSize, sizeof(SStreamCalcScan));
    if (pReq->calcScanPlanList == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < calcScanPlanListSize; ++i) {
      SStreamCalcScan calcScan = {0};
      int32_t         vgListSize = 0;
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &vgListSize));
      if (vgListSize > 0) {
        calcScan.vgList = taosArrayInit(vgListSize, sizeof(int32_t));
        if (calcScan.vgList == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
        for (int32_t j = 0; j < vgListSize; ++j) {
          int32_t vgId = 0;
          TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &vgId));
          if (taosArrayPush(calcScan.vgList, &vgId) == NULL) {
            TAOS_CHECK_EXIT(terrno);
          }
        }
        TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&calcScan.scanPlan, NULL));
      }
      taosArrayPush(pReq->calcScanPlanList, &calcScan);
    }
  }

  int32_t vgNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &vgNum));
  if (vgNum > 0) {
    pReq->pVSubTables = taosArrayInit(vgNum, sizeof(SVSubTablesRsp));
    if (pReq->pVSubTables == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    SVSubTablesRsp vgTables = {0};
    for (int32_t i = 0; i < vgNum; ++i) {
      vgTables.pTables = NULL;
      TAOS_CHECK_EXIT(tDeserializeSVSubTablesRspImpl(pDecoder, &vgTables));
      if (taosArrayPush(pReq->pVSubTables, &vgTables) == NULL) {
        tDestroySVSubTablesRsp(&vgTables);
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->calcPlan, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->subTblNameExpr, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->tagValueExpr, NULL));

  int32_t forceOutColsSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &forceOutColsSize));
  if (forceOutColsSize > 0) {
    pReq->forceOutCols = taosArrayInit(forceOutColsSize, sizeof(SStreamOutCol));
    if (pReq->forceOutCols == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < forceOutColsSize; ++i) {
      SStreamOutCol outCol = {0};
      int64_t       exprLen = 0;
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&outCol.expr, &exprLen));
      TAOS_CHECK_EXIT(tDecodeU8(pDecoder, &outCol.type.type));
      TAOS_CHECK_EXIT(tDecodeU8(pDecoder, &outCol.type.precision));
      TAOS_CHECK_EXIT(tDecodeU8(pDecoder, &outCol.type.scale));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &outCol.type.bytes));
      if (taosArrayPush(pReq->forceOutCols, &outCol) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

_exit:

  return code;
}


int32_t tDeserializeSCMCreateStreamReq(void *buf, int32_t bufLen, SCMCreateStreamReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  
  TAOS_CHECK_EXIT(tDeserializeSCMCreateStreamReqImpl(&decoder, pReq));

  tEndDecode(&decoder);

_exit:

  tDecoderClear(&decoder);
  return code;
}


int32_t tSerializeSMDropStreamReq(void *buf, int32_t bufLen, const SMDropStreamReq *pReq) {
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igNotExists));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropStreamReq(void *buf, int32_t bufLen, SMDropStreamReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeMDropStreamReq(SMDropStreamReq *pReq) { FREESQL(); }

void tFreeSCMCreateStreamReq(SCMCreateStreamReq *pReq) {
  if (NULL == pReq) {
    return;
  }
}

static int32_t tEncodeStreamProgressReq(SEncoder *pEncoder, const SStreamProgressReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->vgId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->fetchIdx));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->subFetchIdx));

_exit:
  return code;
}

int32_t tSerializeStreamProgressReq(void *buf, int32_t bufLen, const SStreamProgressReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeStreamProgressReq(&encoder, pReq));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

static int32_t tDecodeStreamProgressReq(SDecoder *pDecoder, SStreamProgressReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->fetchIdx));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->subFetchIdx));

_exit:
  return code;
}

int32_t tDeserializeStreamProgressReq(void *buf, int32_t bufLen, SStreamProgressReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, (char *)buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeStreamProgressReq(&decoder, pReq));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

static int32_t tEncodeStreamProgressRsp(SEncoder *pEncoder, const SStreamProgressRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRsp->streamId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->vgId));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pRsp->fillHisFinished));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRsp->progressDelay));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->fetchIdx));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->subFetchIdx));

_exit:
  return code;
}

int32_t tSerializeStreamProgressRsp(void *buf, int32_t bufLen, const SStreamProgressRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeStreamProgressRsp(&encoder, pRsp));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

static int32_t tDecodeStreamProgressRsp(SDecoder *pDecoder, SStreamProgressRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pRsp->streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->vgId));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, (int8_t *)&pRsp->fillHisFinished));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pRsp->progressDelay));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->fetchIdx));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->subFetchIdx));

_exit:
  return code;
}

int32_t tDeserializeSStreamProgressRsp(void *buf, int32_t bufLen, SStreamProgressRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeStreamProgressRsp(&decoder, pRsp));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSTriggerPullRequest(void* buf, int32_t bufLen, const SSTriggerPullRequest* pReq) {
  SEncoder encoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  tlen = 0;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->type));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->readerTaskId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->sessionId));

  switch (pReq->type) {
    case STRIGGER_PULL_LAST_TS: {
      break;
    }
    case STRIGGER_PULL_FIRST_TS: {
      SSTriggerFirstTsRequest* pRequest = (SSTriggerFirstTsRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->startTime));
      break;
    }
    case STRIGGER_PULL_TSDB_META: {
      SSTriggerTsdbMetaRequest* pRequest = (SSTriggerTsdbMetaRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->startTime));
      break;
    }
    case STRIGGER_PULL_TSDB_META_NEXT: {
      break;
    }
    case STRIGGER_PULL_TSDB_TS_DATA: {
      SSTriggerTsdbTsDataRequest* pRequest = (SSTriggerTsdbTsDataRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->uid));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->skey));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ekey));
      break;
    }
    case STRIGGER_PULL_TSDB_TRIGGER_DATA: {
      SSTriggerTsdbTriggerDataRequest* pRequest = (SSTriggerTsdbTriggerDataRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->startTime));
      break;
    }
    case STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT: {
      break;
    }
    case STRIGGER_PULL_TSDB_CALC_DATA: {
      SSTriggerTsdbCalcDataRequest* pRequest = (SSTriggerTsdbCalcDataRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->gid));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->skey));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ekey));
      break;
    }
    case STRIGGER_PULL_TSDB_CALC_DATA_NEXT: {
      break;
    }
    case STRIGGER_PULL_WAL_META: {
      SSTriggerWalMetaRequest* pRequest = (SSTriggerWalMetaRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->lastVer));
      break;
    }
    case STRIGGER_PULL_WAL_TS_DATA: {
      SSTriggerWalTsDataRequest* pRequest = (SSTriggerWalTsDataRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
      break;
    }
    case STRIGGER_PULL_WAL_TRIGGER_DATA: {
      SSTriggerWalTriggerDataRequest* pRequest = (SSTriggerWalTriggerDataRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
      break;
    }
    case STRIGGER_PULL_WAL_CALC_DATA: {
      SSTriggerWalCalcDataRequest* pRequest = (SSTriggerWalCalcDataRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->skey));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ekey));
      break;
    }
    default: {
      uError("unknown pull type %d", pReq->type);
      code = TSDB_CODE_INVALID_PARA;
      break;
    }
  }

  tEndEncode(&encoder);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDserializeSTriggerPullRequest(void* buf, int32_t bufLen, SSTriggerPullRequestUnion* pReq) {
  SDecoder decoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t type = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &type));
  SSTriggerPullRequest* pBase = &(pReq->base);
  pBase->type = type;
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pBase->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pBase->readerTaskId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pBase->sessionId));

  switch (type) {
    case STRIGGER_PULL_LAST_TS: {
      break;
    }
    case STRIGGER_PULL_FIRST_TS: {
      SSTriggerFirstTsRequest* pRequest = &(pReq->firstTsReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->startTime));
      break;
    }
    case STRIGGER_PULL_TSDB_META: {
      SSTriggerTsdbMetaRequest* pRequest = &(pReq->tsdbMetaReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->startTime));
      break;
    }
    case STRIGGER_PULL_TSDB_META_NEXT: {
      break;
    }
    case STRIGGER_PULL_TSDB_TS_DATA: {
      SSTriggerTsdbTsDataRequest* pRequest = &(pReq->tsdbTsDataReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->uid));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->skey));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ekey));
      break;
    }
    case STRIGGER_PULL_TSDB_TRIGGER_DATA: {
      SSTriggerTsdbTriggerDataRequest* pRequest = &(pReq->tsdbTriggerDataReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->startTime));
      break;
    }
    case STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT: {
      break;
    }
    case STRIGGER_PULL_TSDB_CALC_DATA: {
      SSTriggerTsdbCalcDataRequest* pRequest = &(pReq->tsdbCalcDataReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->gid));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->skey));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ekey));
      break;
    }
    case STRIGGER_PULL_TSDB_CALC_DATA_NEXT: {
      break;
    }
    case STRIGGER_PULL_WAL_META: {
      SSTriggerWalMetaRequest* pRequest = &(pReq->walMetaReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->lastVer));
      break;
    }
    case STRIGGER_PULL_WAL_TS_DATA: {
      SSTriggerWalTsDataRequest* pRequest = &(pReq->walTsDataReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));
      break;
    }
    case STRIGGER_PULL_WAL_TRIGGER_DATA: {
      SSTriggerWalTriggerDataRequest* pRequest = &(pReq->walTriggerDataReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));
      break;
    }
    case STRIGGER_PULL_WAL_CALC_DATA: {
      SSTriggerWalCalcDataRequest* pRequest = &(pReq->walCalcDataReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->skey));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ekey));
      break;
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSTriggerCalcRequest(void* buf, int32_t bufLen, const SSTriggerCalcRequest* pReq) {
  SEncoder encoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  tlen = 0;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->runnerTaskId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->sessionId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->gid));

  int32_t size = taosArrayGetSize(pReq->params);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, size));
  for (int32_t i = 0; i < size; ++i) {
    SSTriggerCalcParam* param = taosArrayGet(pReq->params, i);
    if (param == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, param->currentTs));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, param->wstart));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, param->wend));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, param->wduration));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, param->wrownum));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, param->triggerTime));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, param->notifyType));
    uint32_t len = (param->extraNotifyContent != NULL) ? strlen(param->extraNotifyContent) : 0;
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (uint8_t*)param->extraNotifyContent, len));
  }

  size = taosArrayGetSize(pReq->groupColVals);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, size));
  for (int32_t i = 0; i < size; ++i) {
    SValue* pValue = taosArrayGet(pReq->groupColVals, i);
    if (pValue == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pValue->type));
    if (IS_VAR_DATA_TYPE(pValue->type)) {
      TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pValue->pData, pValue->nData));
    } else {
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pValue->val));
    }
  }

  tEndEncode(&encoder);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

static void tDestroySSTriggerCalcParam(void* ptr) {
  SSTriggerCalcParam* pParam = ptr;
  if (pParam && pParam->extraNotifyContent != NULL) {
    taosMemoryFreeClear(pParam->extraNotifyContent);
  }
}

static void tDestroySValue(void* ptr) {
  SValue* pValue = ptr;
  if (ptr && IS_VAR_DATA_TYPE(pValue->type)) {
    taosMemoryFreeClear(pValue->pData);
    pValue->nData = 0;
  }
}

int32_t tDeserializeSTriggerCalcRequest(void* buf, int32_t bufLen, SSTriggerCalcRequest* pReq) {
  SDecoder decoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->runnerTaskId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->sessionId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->gid));

  int32_t size = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &size));
  taosArrayClearP(pReq->params, tDestroySSTriggerCalcParam);
  if (size > 0) {
    if (pReq->params == NULL) {
      pReq->params = taosArrayInit(size, sizeof(SSTriggerCalcParam));
      if (pReq->params == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    } else {
      TAOS_CHECK_EXIT(taosArrayEnsureCap(pReq->params, size));
    }
    TARRAY_SIZE(pReq->params) = size;
  }
  for (int32_t i = 0; i < size; ++i) {
    SSTriggerCalcParam* param = taosArrayGet(pReq->params, i);
    if (param == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &param->currentTs));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &param->wstart));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &param->wend));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &param->wduration));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &param->wrownum));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &param->triggerTime));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &param->notifyType));
    uint64_t len = 0;
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void**)&param->extraNotifyContent, &len));
  }

  size = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &size));
  taosArrayClearP(pReq->groupColVals, tDestroySValue);
  if (size > 0) {
    if (pReq->groupColVals == NULL) {
      pReq->groupColVals = taosArrayInit(size, sizeof(SValue));
      if (pReq->groupColVals == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    } else {
      TAOS_CHECK_EXIT(taosArrayEnsureCap(pReq->groupColVals, size));
    }
    TARRAY_SIZE(pReq->groupColVals) = size;
  }
  for (int32_t i = 0; i < size; ++i) {
    SValue* pValue = taosArrayGet(pReq->groupColVals, i);
    if (pValue == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pValue->type));
    if (IS_VAR_DATA_TYPE(pValue->type)) {
      uint64_t len = 0;
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void**)&pValue->pData, &len));
      pValue->nData = len;
    } else {
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pValue->val));
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tDestroySTriggerCalcRequest(SSTriggerCalcRequest* pReq) {
  if (pReq != NULL) {
    taosArrayClearP(pReq->params, tDestroySSTriggerCalcParam);
    taosArrayClearP(pReq->groupColVals, tDestroySValue);
    taosMemoryFreeClear(pReq);
  }
}

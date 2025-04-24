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

int32_t tEncodeStreamCheckpointSourceReq(SEncoder* pEncoder, const SStreamCheckpointSourceReq* pReq) {
  TAOS_CHECK_RETURN(tStartEncode(pEncoder));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->checkpointId));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pReq->taskId));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pReq->nodeId));
  TAOS_CHECK_RETURN(tEncodeSEpSet(pEncoder, &pReq->mgmtEps));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pReq->mnodeId));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->expireTime));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pReq->transId));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pReq->mndTrigger));
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamCheckpointSourceReq(SDecoder* pDecoder, SStreamCheckpointSourceReq* pReq) {
  TAOS_CHECK_RETURN(tStartDecode(pDecoder));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pReq->streamId));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pReq->checkpointId));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pReq->taskId));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pReq->nodeId));
  TAOS_CHECK_RETURN(tDecodeSEpSet(pDecoder, &pReq->mgmtEps));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pReq->mnodeId));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pReq->expireTime));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pReq->transId));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pReq->mndTrigger));
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamCheckpointSourceRsp(SEncoder* pEncoder, const SStreamCheckpointSourceRsp* pRsp) {
  TAOS_CHECK_RETURN(tStartEncode(pEncoder));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pRsp->streamId));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pRsp->checkpointId));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->taskId));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->nodeId));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pRsp->expireTime));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pRsp->success));
  tEndEncode(pEncoder);
  return pEncoder->pos;
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

int32_t tEncodeStreamCheckpointReadyMsg(SEncoder* pEncoder, const SStreamCheckpointReadyMsg* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->checkpointId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->downstreamTaskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->downstreamNodeId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->upstreamTaskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->upstreamNodeId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->childId));
  tEndEncode(pEncoder);

_exit:
  if (code) {
    return code;
  } else {
    return pEncoder->pos;
  }
}

int32_t tDecodeStreamCheckpointReadyMsg(SDecoder* pDecoder, SStreamCheckpointReadyMsg* pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pRsp->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pRsp->checkpointId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->downstreamTaskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->downstreamNodeId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->upstreamTaskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->upstreamNodeId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->childId));
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

int32_t tEncodeStreamTaskCheckpointReq(SEncoder* pEncoder, const SStreamTaskCheckpointReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->nodeId));
  tEndEncode(pEncoder);

_exit:
  return code;
}

int32_t tDecodeStreamTaskCheckpointReq(SDecoder* pDecoder, SStreamTaskCheckpointReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->nodeId));
  tEndDecode(pDecoder);

_exit:
  return code;
}

int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->dnodeId));
  //STREAMTODO
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
  //STREAMTODO
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

int32_t tEncodeStreamHbRsp(SEncoder* pEncoder, const SMStreamHbRspMsg* pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->streamGId));
  //STREAMTODO
  tEndEncode(pEncoder);

_exit:
  return code;
}

int32_t tDecodeStreamHbRsp(SDecoder* pDecoder, SMStreamHbRspMsg* pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->streamGId));
  //STREAMTODO
  tEndDecode(pDecoder);

_exit:
  return code;
}

int32_t tEncodeRetrieveChkptTriggerReq(SEncoder* pEncoder, const SRetrieveChkptTriggerReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->checkpointId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->upstreamNodeId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->upstreamTaskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->downstreamNodeId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->downstreamTaskId));
  tEndEncode(pEncoder);

_exit:
  return code;
}

int32_t tDecodeRetrieveChkptTriggerReq(SDecoder* pDecoder, SRetrieveChkptTriggerReq* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->checkpointId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->upstreamNodeId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->upstreamTaskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->downstreamNodeId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->downstreamTaskId));
  tEndDecode(pDecoder);

_exit:
  return code;
}

int32_t tEncodeCheckpointTriggerRsp(SEncoder* pEncoder, const SCheckpointTriggerRsp* pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRsp->streamId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRsp->checkpointId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->upstreamTaskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->taskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->transId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->rspCode));
  tEndEncode(pEncoder);

_exit:
  return code;
}

int32_t tDecodeCheckpointTriggerRsp(SDecoder* pDecoder, SCheckpointTriggerRsp* pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pRsp->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pRsp->checkpointId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->upstreamTaskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->taskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->transId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->rspCode));
  tEndDecode(pDecoder);

_exit:
  return code;
}

int32_t tEncodeStreamTaskChkptReport(SEncoder* pEncoder, const SCheckpointReport* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->nodeId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->checkpointId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->checkpointVer));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->checkpointTs));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->transId));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->dropHTask));
  tEndEncode(pEncoder);

_exit:
  return code;
}

int32_t tDecodeStreamTaskChkptReport(SDecoder* pDecoder, SCheckpointReport* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->nodeId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->checkpointId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->checkpointVer));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->checkpointTs));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->transId));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->dropHTask));
  tEndDecode(pDecoder);

_exit:
  return code;
}

int32_t tEncodeRestoreCheckpointInfo(SEncoder* pEncoder, const SRestoreCheckpointInfo* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->startTs));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->checkpointId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->transId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->nodeId));
  tEndEncode(pEncoder);

_exit:
  return code;
}

// todo: serialized term attributes.
int32_t tDecodeRestoreCheckpointInfo(SDecoder* pDecoder, SRestoreCheckpointInfo* pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->startTs));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->checkpointId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->transId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->nodeId));
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

int32_t tSerializeSCMCreateStreamReq(void *buf, int32_t bufLen, const SCMCreateStreamReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t sqlLen = pReq->sql == NULL ? 0 : (int32_t)strlen(pReq->sql);
  int32_t nameLen = pReq->name == NULL ? 0 : (int32_t)strlen(pReq->name);
  int32_t outDbLen = pReq->outDB == NULL ? 0 : (int32_t)strlen(pReq->outDB);
  int32_t streamDBLen = pReq->streamDB == NULL ? 0 : (int32_t)strlen(pReq->streamDB);
  int32_t triggerDBLen = pReq->triggerDB == NULL ? 0 : (int32_t)strlen(pReq->triggerDB);
  int32_t triggerTblNameLen = pReq->triggerTblName == NULL ? 0 : (int32_t)strlen(pReq->triggerTblName);
  int32_t outTblNameLen = pReq->outTblName == NULL ? 0 : (int32_t)strlen(pReq->outTblName);

  // name part
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->name, nameLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->sql, sqlLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->outDB, outDbLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->streamDB, streamDBLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->triggerDB, triggerDBLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->triggerTblName, triggerTblNameLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->outTblName, outTblNameLen));

  // trigger control part
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igExists));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->triggerType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igDisorder));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->deleteReCalc));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->deleteOutTbl));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->fillHistory));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->fillHistoryFirst));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->calcNotifyOnly));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->lowLatencyCalc));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->forceOutput));

  // notify part
  int32_t addrSize = (int32_t)taosArrayGetSize(pReq->pNotifyAddrUrls);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, addrSize));
  for (int32_t i = 0; i < addrSize; ++i) {
    const char *url = taosArrayGetP(pReq->pNotifyAddrUrls, i);
    TAOS_CHECK_EXIT((tEncodeCStr(&encoder, url)));
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->notifyEventTypes));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->notifyErrorHandle));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->notifyHistory));

  // out table part
  // out col
  int32_t outColSize = (int32_t )taosArrayGetSize(pReq->outCols);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, outColSize));
  for (int32_t i = 0; i < outColSize; ++i) {
    SFieldWithOptions *pField = taosArrayGet(pReq->outCols, i);
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->type));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->flags));
    int32_t bytes = pField->bytes;
    if (IS_DECIMAL_TYPE(pField->type)) {
      uint8_t prec = 0, scale = 0;
      extractTypeFromTypeMod(pField->type, pField->typeMod, &prec, &scale, NULL);
      fillBytesForDecimalType(&bytes, pField->type, prec, scale);
    }
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, bytes));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pField->name));
  }

  // out tag
  int32_t outTagSize = (int32_t )taosArrayGetSize(pReq->outTags);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, outTagSize));
  for (int32_t i = 0; i < outTagSize; ++i) {
    SField *pField = taosArrayGet(pReq->outTags, i);
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->type));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->flags));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pField->bytes));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pField->name));
  }

  // partition col part
  int32_t parColSize = (int32_t)taosArrayGetSize(pReq->partitionCols);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, parColSize));
  for (int32_t i = 0; i < parColSize; ++i) {
    SField *pField = taosArrayGet(pReq->partitionCols, i);
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->type));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->flags));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pField->bytes));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pField->name));
  }

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->maxDelay));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->fillHistoryStartTime));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->watermark));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->expiredTime));

  // session trigger
  TAOS_CHECK_EXIT(tEncodeI16(&encoder, pReq->trigger.session.slotId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->trigger.session.sessionVal));

  // state trigger
  TAOS_CHECK_EXIT(tEncodeI16(&encoder, pReq->trigger.stateWin.slotId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->trigger.stateWin.trueForDuration));

  // slide trigger
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->trigger.sliding.interval));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->trigger.sliding.sliding));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->trigger.sliding.offset));

  // event trigger
  int32_t eventWindowStartCondLen = pReq->trigger.event.startCond == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.event.startCond);
  int32_t eventWindowEndCondLen = pReq->trigger.event.endCond == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.event.endCond);

  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->trigger.event.startCond, eventWindowStartCondLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->trigger.event.endCond, eventWindowEndCondLen));

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->trigger.event.trueForDuration));

  // count trigger
  int32_t countWindowCondColsLen = pReq->trigger.count.condCols == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.count.condCols);
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->trigger.count.condCols, countWindowCondColsLen));

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->trigger.count.countVal));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->trigger.count.sliding));

  // period trigger
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->trigger.period.period));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->trigger.period.offset));

  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->triggerTblType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->outTblType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->outStbExists));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->outStbUid));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->eventTypes));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->triggerTblVgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->outTblVgId));

  int32_t triggerPrevFilterLen = pReq->triggerPrevFilter == NULL ? 0 : (int32_t)strlen((char*)pReq->triggerPrevFilter);
  int32_t triggerWalScanPlanLen = pReq->triggerWalScanPlan == NULL ? 0 : (int32_t)strlen((char*)pReq->triggerWalScanPlan);
  int32_t triggerTsdbScanPlanLen = pReq->triggerTsdbScanPlan == NULL ? 0 : (int32_t)strlen((char*)pReq->triggerTsdbScanPlan);

  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->triggerPrevFilter, triggerPrevFilterLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->triggerWalScanPlan, triggerWalScanPlanLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->triggerTsdbScanPlan, triggerTsdbScanPlanLen));

  int32_t calcScanPlanListSize = (int32_t)taosArrayGetSize(pReq->calcScanPlanList);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, calcScanPlanListSize));
  for (int32_t i = 0; i < calcScanPlanListSize; ++i) {
    SStreamCalcScan* pCalcScanPlan = (SStreamCalcScan*)taosArrayGet(pReq->calcScanPlanList, i);
    int32_t          vgListSize = (int32_t)taosArrayGetSize(pCalcScanPlan->vgList);
    int32_t          scanPlanLen = pCalcScanPlan->scanPlan == NULL ? 0 : (int32_t)strlen((char*)pCalcScanPlan->scanPlan);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, vgListSize));
    for (int32_t j = 0; j < vgListSize; ++j) {
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, *(int32_t*)taosArrayGet(pCalcScanPlan->vgList, j)));
    }
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pCalcScanPlan->scanPlan, scanPlanLen));
  }

  int32_t vgNum = (int32_t)taosArrayGetSize(pReq->pVSubTables);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, vgNum));
  for (int32_t i = 0; i < vgNum; ++i) {
    TAOS_CHECK_EXIT(tSerializeSVSubTablesRspImpl(&encoder, (SVSubTablesRsp*)taosArrayGet(pReq->pVSubTables, i)));
  }

  int32_t calcPlanLen = pReq->calcPlan == NULL ? 0 : (int32_t)strlen((char*)pReq->calcPlan);
  int32_t subTblNameExprLen = pReq->subTblNameExpr == NULL ? 0 : (int32_t)strlen((char*)pReq->subTblNameExpr);
  int32_t tagValueExprLen = pReq->tagValueExpr == NULL ? 0 : (int32_t)strlen((char*)pReq->tagValueExpr);

  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->calcPlan, calcPlanLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->subTblNameExpr, subTblNameExprLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->tagValueExpr, tagValueExprLen));

  int32_t forceOutColsSize = (int32_t)taosArrayGetSize(pReq->forceOutCols);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, forceOutColsSize));
  for (int32_t i = 0; i < forceOutColsSize; ++i) {
    SStreamOutCol *pCoutCol = (SStreamOutCol*)taosArrayGet(pReq->forceOutCols, i);
    int32_t        exprLen = pCoutCol->expr == NULL ? 0 : (int32_t)strlen((char*)pCoutCol->expr);

    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pCoutCol->expr, exprLen));
    TAOS_CHECK_EXIT(tEncodeU8(&encoder, pCoutCol->type.type));
    TAOS_CHECK_EXIT(tEncodeU8(&encoder, pCoutCol->type.precision));
    TAOS_CHECK_EXIT(tEncodeU8(&encoder, pCoutCol->type.scale));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pCoutCol->type.bytes));
  }

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

int32_t tDeserializeSCMCreateStreamReq(void *buf, int32_t bufLen, SCMCreateStreamReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->name, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->sql, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->outDB, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->streamDB, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->triggerDB, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->triggerTblName, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->outTblName, NULL));

  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igExists));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->triggerType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igDisorder));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->deleteReCalc));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->deleteOutTbl));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->fillHistory));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->fillHistoryFirst));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->calcNotifyOnly));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->lowLatencyCalc));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->forceOutput));

  int32_t addrSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &addrSize));
  pReq->pNotifyAddrUrls = taosArrayInit(addrSize, POINTER_BYTES);
  if (pReq->pNotifyAddrUrls == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < addrSize; ++i) {
    char *url = NULL;
    TAOS_CHECK_EXIT(tDecodeCStr(&decoder, &url));
    url = taosStrndup(url, TSDB_STREAM_NOTIFY_URL_LEN);
    if (url == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    if (taosArrayPush(pReq->pNotifyAddrUrls, &url) == NULL) {
      taosMemoryFree(url);
      TAOS_CHECK_EXIT(terrno);
    }
  }
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->notifyEventTypes));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->notifyErrorHandle));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->notifyHistory));

  int32_t outColSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &outColSize));
  if (outColSize > 0) {
    pReq->outCols = taosArrayInit(outColSize, sizeof(SField));
    if (pReq->outCols == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = 0; i < outColSize; ++i) {
      SField field = {0};
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.type));
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.flags));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &field.bytes));
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, field.name));
      if (taosArrayPush(pReq->outCols, &field) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  int32_t outTagSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &outTagSize));
  if (outTagSize > 0) {
    pReq->outTags = taosArrayInit(outTagSize, sizeof(SField));
    if (pReq->outTags == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = 0; i < outTagSize; ++i) {
      SField field = {0};
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.type));
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.flags));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &field.bytes));
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, field.name));
      if (taosArrayPush(pReq->outTags, &field) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  int32_t parColSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &parColSize));
  if (parColSize > 0) {
    pReq->partitionCols = taosArrayInit(parColSize, sizeof(SField));
    if (pReq->partitionCols == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = 0; i < parColSize; ++i) {
      SField field = {0};
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.type));
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.flags));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &field.bytes));
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, field.name));
      if (taosArrayPush(pReq->partitionCols, &field) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->maxDelay));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->fillHistoryStartTime));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->watermark));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->expiredTime));

  // session trigger
  TAOS_CHECK_EXIT(tDecodeI16(&decoder, &pReq->trigger.session.slotId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->trigger.session.sessionVal));

  // state trigger
  TAOS_CHECK_EXIT(tDecodeI16(&decoder, &pReq->trigger.stateWin.slotId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->trigger.stateWin.trueForDuration));

  // slide trigger
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->trigger.sliding.interval));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->trigger.sliding.sliding));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->trigger.sliding.offset));


  // event trigger
  int32_t eventWindowStartCondLen = pReq->trigger.event.startCond == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.event.startCond);
  int32_t eventWindowEndCondLen = pReq->trigger.event.endCond == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.event.endCond);

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->trigger.event.startCond, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->trigger.event.endCond, NULL));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->trigger.event.trueForDuration));

  // count trigger
  int32_t countWindowCondColsLen = pReq->trigger.count.condCols == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.count.condCols);
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->trigger.count.condCols, NULL));

  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->trigger.count.countVal));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->trigger.count.sliding));

  // period trigger
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->trigger.period.period));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->trigger.period.offset));

  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->triggerTblType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->outTblType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->outStbExists));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->outStbUid));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->eventTypes));

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->triggerTblVgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->outTblVgId));

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->triggerPrevFilter, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->triggerWalScanPlan, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->triggerTsdbScanPlan, NULL));

  int32_t calcScanPlanListSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &calcScanPlanListSize));
  if (calcScanPlanListSize > 0) {
    pReq->calcScanPlanList = taosArrayInit(calcScanPlanListSize, sizeof(SStreamCalcScan));
    if (pReq->partitionCols == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < calcScanPlanListSize; ++i) {
      SStreamCalcScan calcScan = {0};
      int32_t         vgListSize = 0;
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &vgListSize));
      if (vgListSize > 0) {
        calcScan.vgList = taosArrayInit(vgListSize, sizeof(int32_t));
        if (calcScan.vgList == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
        for (int32_t j = 0; j < vgListSize; ++j) {
          int32_t vgId = 0;
          TAOS_CHECK_EXIT(tDecodeI32(&decoder, &vgId));
          if (taosArrayPush(calcScan.vgList, &vgId) == NULL) {
            TAOS_CHECK_EXIT(terrno);
          }
        }
        TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&calcScan.scanPlan, NULL));
      }
      taosArrayPush(pReq->calcScanPlanList, &calcScan);
    }
  }

  int32_t vgNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &vgNum));
  if (vgNum > 0) {
    pReq->pVSubTables = taosArrayInit(vgNum, sizeof(SVSubTablesRsp));
    if (pReq->pVSubTables == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    SVSubTablesRsp vgTables = {0};
    for (int32_t i = 0; i < vgNum; ++i) {
      vgTables.pTables = NULL;
      TAOS_CHECK_EXIT(tDeserializeSVSubTablesRspImpl(&decoder, &vgTables));
      if (taosArrayPush(pReq->pVSubTables, &vgTables) == NULL) {
        tDestroySVSubTablesRsp(&vgTables);
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->calcPlan, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->subTblNameExpr, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&pReq->tagValueExpr, NULL));

  int32_t forceOutColsSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &forceOutColsSize));
  if (forceOutColsSize > 0) {
    pReq->forceOutCols = taosArrayInit(forceOutColsSize, sizeof(SStreamOutCol));
    if (pReq->forceOutCols == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < forceOutColsSize; ++i) {
      SStreamOutCol outCol = {0};
      int32_t       exprLen = 0;
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc32(&decoder, (void**)&outCol.expr, &exprLen));
      TAOS_CHECK_EXIT(tDecodeU8(&decoder, &outCol.type.type));
      TAOS_CHECK_EXIT(tDecodeU8(&decoder, &outCol.type.precision));
      TAOS_CHECK_EXIT(tDecodeU8(&decoder, &outCol.type.scale));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &outCol.type.bytes));
      if (taosArrayPush(pReq->forceOutCols, &outCol) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

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
//  taosMemoryFreeClear(pReq->sql);
//  taosMemoryFreeClear(pReq->ast);
//  taosArrayDestroy(pReq->pTags);
//  taosArrayDestroy(pReq->fillNullCols);
//  taosArrayDestroy(pReq->pVgroupVerList);
//  taosArrayDestroy(pReq->pCols);
//  taosArrayDestroyP(pReq->pNotifyAddrUrls, NULL);
//  taosArrayDestroyEx(pReq->pVSubTables, tDestroySVSubTablesRsp);
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



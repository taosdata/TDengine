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
#include "os.h"
#include "tstream.h"
#include "streamInt.h"

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
  if (pMsg->pNodeList == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
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

  tEndDecode(pDecoder);
_exit:
  return code;
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
    stError("invalid dispatch req msg");
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
  taosArrayDestroyP(pReq->data, taosMemoryFree);
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
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->vgId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->numOfTasks));

  for (int32_t i = 0; i < pReq->numOfTasks; ++i) {
    STaskStatusEntry* ps = taosArrayGet(pReq->pTaskStatus, i);
    if (ps == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->id.streamId));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, ps->id.taskId));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, ps->status));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->stage));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, ps->nodeId));
    TAOS_CHECK_EXIT(tEncodeDouble(pEncoder, ps->inputQUsed));
    TAOS_CHECK_EXIT(tEncodeDouble(pEncoder, ps->inputRate));
    TAOS_CHECK_EXIT(tEncodeDouble(pEncoder, ps->procsTotal));
    TAOS_CHECK_EXIT(tEncodeDouble(pEncoder, ps->procsThroughput));
    TAOS_CHECK_EXIT(tEncodeDouble(pEncoder, ps->outputTotal));
    TAOS_CHECK_EXIT(tEncodeDouble(pEncoder, ps->outputThroughput));
    TAOS_CHECK_EXIT(tEncodeDouble(pEncoder, ps->sinkQuota));
    TAOS_CHECK_EXIT(tEncodeDouble(pEncoder, ps->sinkDataSize));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->processedVer));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->verRange.minVer));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->verRange.maxVer));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->checkpointInfo.activeId));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, ps->checkpointInfo.failed));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, ps->checkpointInfo.activeTransId));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->checkpointInfo.latestId));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->checkpointInfo.latestVer));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->checkpointInfo.latestTime));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->checkpointInfo.latestSize));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, ps->checkpointInfo.remoteBackup));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, ps->checkpointInfo.consensusChkptId));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->checkpointInfo.consensusTs));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->startTime));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->startCheckpointId));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->startCheckpointVer));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, ps->hTaskId));
  }

  int32_t numOfVgs = taosArrayGetSize(pReq->pUpdateNodes);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, numOfVgs));

  for (int j = 0; j < numOfVgs; ++j) {
    int32_t* pVgId = taosArrayGet(pReq->pUpdateNodes, j);
    if (pVgId == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, *pVgId));
  }

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->msgId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->ts));
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
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->numOfTasks));

  if ((pReq->pTaskStatus = taosArrayInit(pReq->numOfTasks, sizeof(STaskStatusEntry))) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < pReq->numOfTasks; ++i) {
    int32_t          taskId = 0;
    STaskStatusEntry entry = {0};

    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.id.streamId));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &taskId));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &entry.status));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.stage));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &entry.nodeId));
    TAOS_CHECK_EXIT(tDecodeDouble(pDecoder, &entry.inputQUsed));
    TAOS_CHECK_EXIT(tDecodeDouble(pDecoder, &entry.inputRate));
    TAOS_CHECK_EXIT(tDecodeDouble(pDecoder, &entry.procsTotal));
    TAOS_CHECK_EXIT(tDecodeDouble(pDecoder, &entry.procsThroughput));
    TAOS_CHECK_EXIT(tDecodeDouble(pDecoder, &entry.outputTotal));
    TAOS_CHECK_EXIT(tDecodeDouble(pDecoder, &entry.outputThroughput));
    TAOS_CHECK_EXIT(tDecodeDouble(pDecoder, &entry.sinkQuota));
    TAOS_CHECK_EXIT(tDecodeDouble(pDecoder, &entry.sinkDataSize));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.processedVer));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.verRange.minVer));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.verRange.maxVer));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.checkpointInfo.activeId));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &entry.checkpointInfo.failed));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &entry.checkpointInfo.activeTransId));

    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.checkpointInfo.latestId));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.checkpointInfo.latestVer));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.checkpointInfo.latestTime));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.checkpointInfo.latestSize));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &entry.checkpointInfo.remoteBackup));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &entry.checkpointInfo.consensusChkptId));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.checkpointInfo.consensusTs));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.startTime));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.startCheckpointId));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.startCheckpointVer));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.hTaskId));

    entry.id.taskId = taskId;
    if (taosArrayPush(pReq->pTaskStatus, &entry) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  int32_t numOfVgs = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &numOfVgs));

  if ((pReq->pUpdateNodes = taosArrayInit(numOfVgs, sizeof(int32_t))) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int j = 0; j < numOfVgs; ++j) {
    int32_t vgId = 0;
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &vgId));
    if (taosArrayPush(pReq->pUpdateNodes, &vgId) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->msgId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->ts));
  tEndDecode(pDecoder);

_exit:
  return code;
}

void tCleanupStreamHbMsg(SStreamHbMsg* pMsg) {
  if (pMsg == NULL) {
    return;
  }

  if (pMsg->pUpdateNodes != NULL) {
    taosArrayDestroy(pMsg->pUpdateNodes);
    pMsg->pUpdateNodes = NULL;
  }

  if (pMsg->pTaskStatus != NULL) {
    taosArrayDestroy(pMsg->pTaskStatus);
    pMsg->pTaskStatus = NULL;
  }

  pMsg->msgId = -1;
  pMsg->vgId = -1;
  pMsg->numOfTasks = -1;
}

int32_t tEncodeStreamTask(SEncoder* pEncoder, const SStreamTask* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->ver));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->id.streamId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->id.taskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->info.trigger));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->info.taskLevel));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->outputInfo.type));
  TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pTask->msgInfo.msgType));

  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->status.taskStatus));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->status.schedStatus));

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->info.selfChildId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->info.nodeId));
  TAOS_CHECK_EXIT(tEncodeSEpSet(pEncoder, &pTask->info.epSet));
  TAOS_CHECK_EXIT(tEncodeSEpSet(pEncoder, &pTask->info.mnodeEpset));

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->chkInfo.checkpointId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->chkInfo.checkpointVer));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->info.fillHistory));

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->hTaskInfo.id.streamId));
  int32_t taskId = pTask->hTaskInfo.id.taskId;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, taskId));

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->streamTaskId.streamId));
  taskId = pTask->streamTaskId.taskId;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, taskId));

  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pTask->dataRange.range.minVer));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pTask->dataRange.range.maxVer));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->dataRange.window.skey));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->dataRange.window.ekey));

  int32_t epSz = taosArrayGetSize(pTask->upstreamInfo.pList);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, epSz));
  for (int32_t i = 0; i < epSz; i++) {
    SStreamUpstreamEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    TAOS_CHECK_EXIT(tEncodeStreamEpInfo(pEncoder, pInfo));
  }

  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTask->exec.qmsg));
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->outputInfo.tbSink.stbUid));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTask->outputInfo.tbSink.stbFullName));
    TAOS_CHECK_EXIT(tEncodeSSchemaWrapper(pEncoder, pTask->outputInfo.tbSink.pSchemaWrapper));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SMA) {
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->outputInfo.smaSink.smaId));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FETCH) {
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->outputInfo.fetchSink.reserved));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->outputInfo.fixedDispatcher.taskId));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->outputInfo.fixedDispatcher.nodeId));
    TAOS_CHECK_EXIT(tEncodeSEpSet(pEncoder, &pTask->outputInfo.fixedDispatcher.epSet));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    TAOS_CHECK_EXIT(tSerializeSUseDbRspImp(pEncoder, &pTask->outputInfo.shuffleDispatcher.dbInfo));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTask->outputInfo.shuffleDispatcher.stbFullName));
  }
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->info.delaySchedParam));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->subtableWithoutMd5));
  TAOS_CHECK_EXIT(tEncodeCStrWithLen(pEncoder, pTask->reserve, sizeof(pTask->reserve) - 1));

  tEndEncode(pEncoder);
_exit:
  return code;
}

int32_t tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask) {
  int32_t taskId = 0;
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->ver));
  if (pTask->ver <= SSTREAM_TASK_INCOMPATIBLE_VER || pTask->ver > SSTREAM_TASK_VER) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->id.streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->id.taskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->info.trigger));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->info.taskLevel));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->outputInfo.type));
  TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pTask->msgInfo.msgType));

  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->status.taskStatus));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->status.schedStatus));

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->info.selfChildId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->info.nodeId));
  TAOS_CHECK_EXIT(tDecodeSEpSet(pDecoder, &pTask->info.epSet));
  TAOS_CHECK_EXIT(tDecodeSEpSet(pDecoder, &pTask->info.mnodeEpset));

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->chkInfo.checkpointId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->chkInfo.checkpointVer));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->info.fillHistory));

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->hTaskInfo.id.streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &taskId));
  pTask->hTaskInfo.id.taskId = taskId;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->streamTaskId.streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &taskId));
  pTask->streamTaskId.taskId = taskId;

  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, (uint64_t*)&pTask->dataRange.range.minVer));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, (uint64_t*)&pTask->dataRange.range.maxVer));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->dataRange.window.skey));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->dataRange.window.ekey));

  int32_t epSz = -1;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &epSz) < 0);

  if ((pTask->upstreamInfo.pList = taosArrayInit(epSz, POINTER_BYTES)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < epSz; i++) {
    SStreamUpstreamEpInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamUpstreamEpInfo));
    if (pInfo == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    if ((code = tDecodeStreamEpInfo(pDecoder, pInfo)) < 0) {
      taosMemoryFreeClear(pInfo);
      goto _exit;
    }
    if (taosArrayPush(pTask->upstreamInfo.pList, &pInfo) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &pTask->exec.qmsg));
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->outputInfo.tbSink.stbUid));
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTask->outputInfo.tbSink.stbFullName));
    pTask->outputInfo.tbSink.pSchemaWrapper = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
    if (pTask->outputInfo.tbSink.pSchemaWrapper == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeSSchemaWrapper(pDecoder, pTask->outputInfo.tbSink.pSchemaWrapper));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SMA) {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->outputInfo.smaSink.smaId));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FETCH) {
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->outputInfo.fetchSink.reserved));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->outputInfo.fixedDispatcher.taskId));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->outputInfo.fixedDispatcher.nodeId));
    TAOS_CHECK_EXIT(tDecodeSEpSet(pDecoder, &pTask->outputInfo.fixedDispatcher.epSet));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    TAOS_CHECK_EXIT(tDeserializeSUseDbRspImp(pDecoder, &pTask->outputInfo.shuffleDispatcher.dbInfo));
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTask->outputInfo.shuffleDispatcher.stbFullName));
  }
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->info.delaySchedParam));
  if (pTask->ver >= SSTREAM_TASK_SUBTABLE_CHANGED_VER) {
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->subtableWithoutMd5));
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTask->reserve));

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
  if (code) {
    return code;
  } else {
    return pEncoder->pos;
  }
}

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

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
#include "taos.h"
#include "tarray.h"
#include "tdatablock.h"
#include "thash.h"
#include "tlist.h"
#include "tmsg.h"
#include "os.h"
#include "tcommon.h"
#include "tsimplehash.h"

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


int32_t tEncodeSStreamMgmtReq(SEncoder* pEncoder, const SStreamMgmtReq* pReq) {
  int32_t code = 0;
  int32_t lino = 0;
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->reqId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->type));
  switch (pReq->type) {
    case STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER: {
      if (pReq->cont.pReqs) {
        int32_t num = taosArrayGetSize(pReq->cont.pReqs);
        TAOS_CHECK_EXIT(tEncodeI32(pEncoder, num));
        for (int32_t i = 0; i < num; ++i) {
          SStreamDbTableName* pName = taosArrayGet(pReq->cont.pReqs, i);
          TAOS_CHECK_EXIT(tEncodeCStrWithLen(pEncoder, pName->dbFName, strlen(pName->dbFName) + 1));
          TAOS_CHECK_EXIT(tEncodeCStrWithLen(pEncoder, pName->tbName, strlen(pName->tbName) + 1));
        }
      } else {
        TAOS_CHECK_EXIT(tEncodeI32(pEncoder, 0));
      }
      break;
    }
    case STREAM_MGMT_REQ_RUNNER_ORIGTBL_READER: {
      if (pReq->cont.pReqs) {
        int32_t num = taosArrayGetSize(pReq->cont.pReqs);
        TAOS_CHECK_EXIT(tEncodeI32(pEncoder, num));
        for (int32_t i = 0; i < num; ++i) {
          SStreamOReaderDeployReq* pDeploy = taosArrayGet(pReq->cont.pReqs, i);
          int32_t vgIdNum = taosArrayGetSize(pDeploy->vgIds);
          TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pDeploy->execId));
          TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pDeploy->uid));
          TAOS_CHECK_EXIT(tEncodeI32(pEncoder, vgIdNum));
          for (int32_t n = 0; n < vgIdNum; ++n) {
            TAOS_CHECK_EXIT(tEncodeI32(pEncoder, *(int32_t*)taosArrayGet(pDeploy->vgIds, n)));
          }
        }
      } else {
        TAOS_CHECK_EXIT(tEncodeI32(pEncoder, 0));
      }
      break;
    }
    default:
      code = TSDB_CODE_STREAM_INVALID_TASK_TYPE;
      break;
  }

_exit:

  return code;
}

void tFreeRunnerOReaderDeployReq(void* param) {
  SStreamOReaderDeployReq* pReq = (SStreamOReaderDeployReq*)param;
  if (pReq) {
    taosArrayDestroy(pReq->vgIds);
  }
}

void tFreeSStreamMgmtReq(SStreamMgmtReq* pReq) {
  if (NULL == pReq) {
    return;
  }

  switch (pReq->type) {
    case STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER:
      taosArrayDestroy(pReq->cont.pReqs);
      break;
    case STREAM_MGMT_REQ_RUNNER_ORIGTBL_READER:
      taosArrayDestroyEx(pReq->cont.pReqs, tFreeRunnerOReaderDeployReq);
      break;
    default:
      break;
  }
}


int32_t tCloneSStreamMgmtReq(SStreamMgmtReq* pSrc, SStreamMgmtReq** ppDst) {
  *ppDst = NULL;
  
  if (NULL == pSrc) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0, lino = 0;
  *ppDst = taosMemoryCalloc(1, sizeof(SStreamMgmtReq));
  TSDB_CHECK_NULL(*ppDst, code, lino, _exit, terrno);

  memcpy(*ppDst, pSrc, sizeof(*pSrc));
  if (pSrc->cont.pReqs) {
    switch (pSrc->type) {
      case STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER:
        (*ppDst)->cont.pReqs = taosArrayDup(pSrc->cont.pReqs, NULL);
        TSDB_CHECK_NULL((*ppDst)->cont.pReqs, code, lino, _exit, terrno);
        break;
      case STREAM_MGMT_REQ_RUNNER_ORIGTBL_READER: {
        int32_t reqNum = taosArrayGetSize(pSrc->cont.pReqs);
        (*ppDst)->cont.pReqs = taosArrayInit_s(sizeof(SStreamOReaderDeployReq), reqNum);
        TSDB_CHECK_NULL((*ppDst)->cont.pReqs, code, lino, _exit, terrno);
        for (int32_t i = 0; i < reqNum; ++i) {
          SStreamOReaderDeployReq* pNew = taosArrayGet((*ppDst)->cont.pReqs, i);
          SStreamOReaderDeployReq* pReq = taosArrayGet(pSrc->cont.pReqs, i);
          pNew->vgIds = taosArrayDup(pReq->vgIds, NULL);
          TSDB_CHECK_NULL(pNew->vgIds, code, lino, _exit, terrno);
          pNew->execId = pReq->execId;
          pNew->uid = pReq->uid;
        }
        break;
      }  
      default:
        break;
    }
  }
  
_exit:

  if (code) {
    tFreeSStreamMgmtReq(*ppDst);
    taosMemoryFreeClear(*ppDst);
    uError("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}


int32_t tDecodeSStreamMgmtReq(SDecoder* pDecoder, SStreamMgmtReq* pReq) {
  int32_t code = 0;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->reqId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, (int32_t*)&pReq->type));
  switch (pReq->type) {
    case STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER: {
      int32_t num = 0;
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &num));
      if (num > 0) {
        pReq->cont.pReqs = taosArrayInit(num, sizeof(SStreamDbTableName));
        TSDB_CHECK_NULL(pReq->cont.pReqs, code, lino, _exit, terrno);
        for (int32_t i = 0; i < num; ++i) {
          SStreamDbTableName* p = taosArrayReserve(pReq->cont.pReqs, 1);
          TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, p->dbFName));
          TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, p->tbName));
        }
      }
      break;
    }
    case STREAM_MGMT_REQ_RUNNER_ORIGTBL_READER: {
      int32_t num = 0, vgIdNum = 0;
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &num));
      if (num > 0) {
        pReq->cont.pReqs = taosArrayInit_s(sizeof(SStreamOReaderDeployReq), num);
        TSDB_CHECK_NULL(pReq->cont.pReqs, code, lino, _exit, terrno);
        for (int32_t i = 0; i < num; ++i) {
          SStreamOReaderDeployReq* p = taosArrayGet(pReq->cont.pReqs, i);
          TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &p->execId));
          TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &p->uid));
          TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &vgIdNum));
          if (vgIdNum > 0) {
            p->vgIds = taosArrayInit_s(sizeof(int32_t), vgIdNum);
            TSDB_CHECK_NULL(p->vgIds, code, lino, _exit, terrno);
          }
          for (int32_t n = 0; n < vgIdNum; ++n) {
            int32_t* vgId = taosArrayGet(p->vgIds, n);
            TAOS_CHECK_EXIT(tDecodeI32(pDecoder, vgId));
          }
        }
      }
      break;
    }
    default:
      code = TSDB_CODE_STREAM_INVALID_TASK_TYPE;
      break;
  }

_exit:

  return code;  
}

int32_t tEncodeStreamTask(SEncoder* pEncoder, const SStreamTask* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->type));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->streamId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->taskId));

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->flags));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->seriousId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->deployId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->nodeId));
  // SKIP SESSIONID
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->taskIdx));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->status));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->detailStatus));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->errorCode));
  if (pTask->pMgmtReq) {
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, 1));
    TAOS_CHECK_EXIT(tEncodeSStreamMgmtReq(pEncoder, pTask->pMgmtReq));
  } else {
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, 0));
  }

_exit:

  return code;
}


int32_t tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, (int32_t*)&pTask->type));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->taskId));
  
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->flags));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->seriousId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->deployId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->nodeId));
  // SKIP SESSIONID
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->taskIdx));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, (int32_t*)&pTask->status));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->detailStatus));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->errorCode));
  int32_t req = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &req));
  if (req) {
    pTask->pMgmtReq = taosMemoryCalloc(1, sizeof(SStreamMgmtReq));
    TSDB_CHECK_NULL(pTask->pMgmtReq, code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(tDecodeSStreamMgmtReq(pDecoder, pTask->pMgmtReq));
  }

_exit:

  return code;
}

int32_t tEncodeSSTriggerRecalcProgress(SEncoder* pEncoder, const SSTriggerRecalcProgress* pProgress) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pProgress->recalcId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pProgress->progress));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pProgress->start));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pProgress->end));

_exit:

  return code;
}

int32_t tDecodeSSTriggerRecalcProgress(SDecoder* pDecoder, SSTriggerRecalcProgress* pProgress) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pProgress->recalcId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pProgress->progress));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pProgress->start));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pProgress->end));

_exit:

  return code;
}


int32_t tEncodeSSTriggerRuntimeStatus(SEncoder* pEncoder, const SSTriggerRuntimeStatus* pStatus) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pStatus->autoRecalcNum));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pStatus->realtimeSessionNum));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pStatus->historySessionNum));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pStatus->recalcSessionNum));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pStatus->histroyProgress));

  int32_t recalcNum = (int32_t)taosArrayGetSize(pStatus->userRecalcs);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, recalcNum));
  for (int32_t i = 0; i < recalcNum; ++i) {
    SSTriggerRecalcProgress* pProgress = taosArrayGet(pStatus->userRecalcs, i);
    TAOS_CHECK_EXIT(tEncodeSSTriggerRecalcProgress(pEncoder, pProgress));
  }

_exit:

  return code;
}

int32_t tDecodeSSTriggerRuntimeStatus(SDecoder* pDecoder, SSTriggerRuntimeStatus* pStatus) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pStatus->autoRecalcNum));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pStatus->realtimeSessionNum));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pStatus->historySessionNum));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pStatus->realtimeSessionNum));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pStatus->histroyProgress));

  int32_t recalcNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &recalcNum));
  if (recalcNum > 0) {
    pStatus->userRecalcs = taosArrayInit_s(sizeof(SSTriggerRecalcProgress), recalcNum);
    if (NULL == pStatus->userRecalcs) {
      code = terrno;
      goto _exit;
    }
  }

  for (int32_t i = 0; i < recalcNum; ++i) {
    SSTriggerRecalcProgress* pProgress = taosArrayGet(pStatus->userRecalcs, i);
    TAOS_CHECK_EXIT(tDecodeSSTriggerRecalcProgress(pDecoder, pProgress));
  }

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
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->runnerThreadNum));

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

  int32_t reqNum = taosArrayGetSize(pReq->pStreamReq);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, reqNum));
  for (int32_t i = 0; i < reqNum; ++i) {
    int32_t* idx = taosArrayGet(pReq->pStreamReq, i);
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, *idx));
  }

  int32_t triggerNum = taosArrayGetSize(pReq->pTriggerStatus);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, triggerNum));
  for (int32_t i = 0; i < triggerNum; ++i) {
    SSTriggerRuntimeStatus* pTrigger = taosArrayGet(pReq->pTriggerStatus, i);
    TAOS_CHECK_EXIT(tEncodeSSTriggerRuntimeStatus(pEncoder, pTrigger));
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
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->runnerThreadNum));

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


  int32_t reqNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &reqNum));
  if (reqNum > 0) {
    pReq->pStreamReq = taosArrayInit_s(sizeof(int32_t), reqNum);
    if (NULL == pReq->pStreamReq) {
      code = terrno;
      goto _exit;
    }
  }
  for (int32_t i = 0; i < reqNum; ++i) {
    int32_t* pIdx = taosArrayGet(pReq->pStreamReq, i);
    if (NULL == pIdx) {
      code = terrno;
      goto _exit;
    }
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, pIdx));
  }


  int32_t triggerNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &triggerNum));
  if (triggerNum > 0) {
    pReq->pTriggerStatus = taosArrayInit_s(sizeof(SSTriggerRuntimeStatus), triggerNum);
    if (NULL == pReq->pTriggerStatus) {
      code = terrno;
      goto _exit;
    }
  }
  for (int32_t i = 0; i < triggerNum; ++i) {
    SSTriggerRuntimeStatus* pStatus = taosArrayGet(pReq->pTriggerStatus, i);
    if (NULL == pStatus) {
      code = terrno;
      goto _exit;
    }
    TAOS_CHECK_EXIT(tDecodeSSTriggerRuntimeStatus(pDecoder, pStatus));
  }

  
  tEndDecode(pDecoder);

_exit:
  return code;
}

void tFreeSSTriggerRuntimeStatus(void* param) {
  SSTriggerRuntimeStatus* pStatus = (SSTriggerRuntimeStatus*)param;
  if (NULL == pStatus) {
    return;
  }
  taosArrayDestroy(pStatus->userRecalcs);
}

void tCleanupStreamHbMsg(SStreamHbMsg* pMsg, bool deepClean) {
  if (pMsg == NULL) {
    return;
  }

  taosArrayDestroy(pMsg->pVgLeaders);
  if (deepClean) {
    int32_t reqNum = taosArrayGetSize(pMsg->pStreamReq);
    for (int32_t i = 0; i < reqNum; ++i) {
      int32_t* idx = taosArrayGet(pMsg->pStreamReq, i);
      SStmTaskStatusMsg* pTask = taosArrayGet(pMsg->pStreamStatus, *idx);
      if (NULL == pTask) {
        continue;
      }

      tFreeSStreamMgmtReq(pTask->pMgmtReq);
      taosMemoryFree(pTask->pMgmtReq);
    }
  }
  taosArrayDestroy(pMsg->pStreamReq);
  taosArrayDestroy(pMsg->pStreamStatus);
  taosArrayDestroyEx(pMsg->pTriggerStatus, tFreeSSTriggerRuntimeStatus);
}

int32_t tEncodeSStreamReaderDeployFromTrigger(SEncoder* pEncoder, const SStreamReaderDeployFromTrigger* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->triggerTblName, pMsg->triggerTblName == NULL ? 0 : (int32_t)strlen(pMsg->triggerTblName) + 1));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->triggerTblUid));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->triggerTblSuid));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->triggerTblType));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->deleteReCalc));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->deleteOutTbl));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->partitionCols, pMsg->partitionCols == NULL ? 0 : (int32_t)strlen(pMsg->partitionCols) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->triggerCols, pMsg->triggerCols == NULL ? 0 : (int32_t)strlen(pMsg->triggerCols) + 1));
  //TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->triggerPrevFilter, pMsg->triggerPrevFilter == NULL ? 0 : (int32_t)strlen(pMsg->triggerPrevFilter) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->triggerScanPlan, pMsg->triggerScanPlan == NULL ? 0 : (int32_t)strlen(pMsg->triggerScanPlan) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->calcCacheScanPlan, pMsg->calcCacheScanPlan == NULL ? 0 : (int32_t)strlen(pMsg->calcCacheScanPlan) + 1));

_exit:

  return code;
}

int32_t tEncodeSStreamReaderDeployFromCalc(SEncoder* pEncoder, const SStreamReaderDeployFromCalc* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->execReplica));
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
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->igNoDataTrigger));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->isTriggerTblVirt));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->triggerHasPF));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->isTriggerTblStb));
  int32_t partitionColsLen = pMsg->partitionCols == NULL ? 0 : (int32_t)strlen((char*)pMsg->partitionCols) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->partitionCols, partitionColsLen));

  int32_t addrSize = (int32_t)taosArrayGetSize(pMsg->pNotifyAddrUrls);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, addrSize));
  for (int32_t i = 0; i < addrSize; ++i) {
    const char *url = taosArrayGetP(pMsg->pNotifyAddrUrls, i);
    TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, url, NULL == url ? 0 : (int32_t)strlen(url) + 1));
  }
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->notifyEventTypes));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->addOptions));
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
      TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pMsg->trigger.stateWin.extend));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.stateWin.trueForDuration));
      int32_t stateWindowZerothLen = 
          pMsg->trigger.stateWin.zeroth == NULL ? 0 : (int32_t)strlen((char*)pMsg->trigger.stateWin.zeroth) + 1;
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->trigger.stateWin.zeroth, stateWindowZerothLen));
      int32_t stateWindowExprLen =
          pMsg->trigger.stateWin.expr == NULL ? 0 : (int32_t)strlen((char*)pMsg->trigger.stateWin.expr) + 1;
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->trigger.stateWin.expr, stateWindowExprLen));
      break;
    }
    case WINDOW_TYPE_INTERVAL: {
      // slide trigger
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->trigger.sliding.intervalUnit));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->trigger.sliding.slidingUnit));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->trigger.sliding.offsetUnit));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->trigger.sliding.soffsetUnit));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->trigger.sliding.precision));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.sliding.interval));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.sliding.offset));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.sliding.sliding));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.sliding.soffset));
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
  TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pMsg->calcTsSlotId));
  TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pMsg->triTsSlotId));
  int32_t triggerPrevFilterLen = (pMsg->triggerPrevFilter == NULL) ? 0 : ((int32_t)strlen(pMsg->triggerPrevFilter) + 1);
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->triggerPrevFilter, triggerPrevFilterLen));
  int32_t triggerScanPlanLen = (pMsg->triggerScanPlan == NULL) ? 0 : ((int32_t)strlen(pMsg->triggerScanPlan) + 1);
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->triggerScanPlan, triggerScanPlanLen));
  int32_t calcCacheScanPlanLen = (pMsg->calcCacheScanPlan == NULL) ? 0 : ((int32_t)strlen(pMsg->calcCacheScanPlan) + 1);
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->calcCacheScanPlan, calcCacheScanPlanLen));

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

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->leaderSnodeId));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->streamName, (int32_t)strlen(pMsg->streamName) + 1));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->precision));

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
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->streamName, (int32_t)strlen(pMsg->streamName) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->pPlan, NULL == pMsg->pPlan ? 0 : (int32_t)strlen(pMsg->pPlan) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->outDBFName, NULL == pMsg->outDBFName ? 0 : (int32_t)strlen(pMsg->outDBFName) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->outTblName, NULL == pMsg->outTblName ? 0 : (int32_t)strlen(pMsg->outTblName) + 1));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->outTblType));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->calcNotifyOnly));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->topPlan));

  int32_t addrSize = (int32_t)taosArrayGetSize(pMsg->pNotifyAddrUrls);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, addrSize));
  for (int32_t i = 0; i < addrSize; ++i) {
    const char *url = taosArrayGetP(pMsg->pNotifyAddrUrls, i);
    TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, url, NULL == url ? 0 : (int32_t)strlen(url) + 1));
  }
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->addOptions));

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
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->outStbSversion));

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

  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->lowLatencyCalc));

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
    SStmTaskDeploy* pDeploy = taosArrayGet(pStream->readerTasks, i);
    TAOS_CHECK_EXIT(tEncodeSStmTaskDeploy(pEncoder, pDeploy));
  }

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pStream->triggerTask ? 1 : 0));
  if (pStream->triggerTask) {
    TAOS_CHECK_EXIT(tEncodeSStmTaskDeploy(pEncoder, pStream->triggerTask));
  }
  
  int32_t runnerNum = taosArrayGetSize(pStream->runnerTasks);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, runnerNum));
  for (int32_t i = 0; i < runnerNum; ++i) {
    SStmTaskDeploy* pDeploy = taosArrayGet(pStream->runnerTasks, i);
    TAOS_CHECK_EXIT(tEncodeSStmTaskDeploy(pEncoder, pDeploy));
  }

_exit:

  return code;
}

int32_t tEncodeSStreamMsg(SEncoder* pEncoder, const SStreamMsg* pMsg) {
  int32_t code = 0;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->msgType));

_exit:
  return code;
}

int32_t tDecodeSStreamMsg(SDecoder* pDecoder, SStreamMsg* pMsg) {
  int32_t code = 0;
  int32_t lino;

  int32_t type = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &type));
  pMsg->msgType = type;

_exit:
  return code;
}

int32_t tEncodeSStreamStartTaskMsg(SEncoder* pEncoder, const SStreamStartTaskMsg* pStart) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeSStreamMsg(pEncoder, &pStart->header));

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

  TAOS_CHECK_EXIT(tEncodeSStreamMsg(pEncoder, &pUndeploy->header));
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


int32_t tEncodeSStreamRecalcReq(SEncoder* pEncoder, const SStreamRecalcReq* recalc) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, recalc->recalcId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, recalc->start));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, recalc->end));

_exit:

  return code;
}

int32_t tEncodeSStreamMgmtRspCont(SEncoder* pEncoder, SStreamMsgType msgType, const SStreamMgmtRspCont* pRsp) {
  int32_t code = 0;
  int32_t lino;

  switch (msgType) {
    case STREAM_MSG_ORIGTBL_READER_INFO: {
      int32_t vgNum = taosArrayGetSize(pRsp->vgIds);
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, vgNum));

      for (int32_t i = 0; i < vgNum; ++i) {
        int32_t* vgId = taosArrayGet(pRsp->vgIds, i);
        TAOS_CHECK_EXIT(tEncodeI32(pEncoder, *vgId));
      }

      int32_t readerNum = taosArrayGetSize(pRsp->readerList);
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, readerNum));
      
      for (int32_t i = 0; i < readerNum; ++i) {
        SStreamTaskAddr* addr = taosArrayGet(pRsp->readerList, i);
        TAOS_CHECK_EXIT(tEncodeSStreamTaskAddr(pEncoder, addr));
      }
      break;
    }
    case STREAM_MSG_UPDATE_RUNNER: {
      int32_t runnerNum = taosArrayGetSize(pRsp->runnerList);
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, runnerNum));
      
      for (int32_t i = 0; i < runnerNum; ++i) {
        SStreamRunnerTarget* target = taosArrayGet(pRsp->runnerList, i);
        TAOS_CHECK_EXIT(tEncodeSStreamRunnerTarget(pEncoder, target));
      }
      break;
    }
    case STREAM_MSG_USER_RECALC: {
      int32_t recalcNum = taosArrayGetSize(pRsp->recalcList);
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, recalcNum));
      
      for (int32_t i = 0; i < recalcNum; ++i) {
        SStreamRecalcReq* recalc = taosArrayGet(pRsp->recalcList, i);
        TAOS_CHECK_EXIT(tEncodeSStreamRecalcReq(pEncoder, recalc));
      }
      break;
    }
    case STREAM_MSG_RUNNER_ORIGTBL_READER: {
      int32_t rspNum = taosArrayGetSize(pRsp->execRspList);
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, rspNum));
      
      for (int32_t i = 0; i < rspNum; ++i) {
        SStreamOReaderDeployRsp* pDeployRsp = taosArrayGet(pRsp->execRspList, i);
        TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pDeployRsp->execId));
        int32_t vgNum = taosArrayGetSize(pDeployRsp->vgList);
        TAOS_CHECK_EXIT(tEncodeI32(pEncoder, vgNum));
        for (int32_t n = 0; n < vgNum; ++n) {
          TAOS_CHECK_EXIT(tEncodeSStreamTaskAddr(pEncoder, taosArrayGet(pDeployRsp->vgList, n)));
        }
      }
      break;
    }
    default:
      break;
  }

_exit:

  return code;
}

int32_t tEncodeSStreamMgmtRsp(SEncoder* pEncoder, const SStreamMgmtRsp* pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeSStreamMsg(pEncoder, &pRsp->header));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRsp->reqId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->code));
  TAOS_CHECK_EXIT(tEncodeStreamTask(pEncoder, &pRsp->task));
  TAOS_CHECK_EXIT(tEncodeSStreamMgmtRspCont(pEncoder, pRsp->header.msgType, (SStreamMgmtRspCont*)&pRsp->cont));

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

  int32_t rspNum = taosArrayGetSize(pRsp->rsps.rspList);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, rspNum));
  for (int32_t i = 0; i < rspNum; ++i) {
    SStreamMgmtRsp* pMgmtRsp = (SStreamMgmtRsp*)taosArrayGet(pRsp->rsps.rspList, i);
    TAOS_CHECK_EXIT(tEncodeSStreamMgmtRsp(pEncoder, pMgmtRsp));
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
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->triggerTblSuid));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->triggerTblType));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->deleteReCalc));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->deleteOutTbl));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->partitionCols, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->triggerCols, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->triggerScanPlan, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->calcCacheScanPlan, NULL));

_exit:

  return code;
}


int32_t tDecodeSStreamReaderDeployFromCalc(SDecoder* pDecoder, SStreamReaderDeployFromCalc* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->execReplica));
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
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->igNoDataTrigger));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->isTriggerTblVirt));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->triggerHasPF));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->isTriggerTblStb));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->partitionCols, NULL));

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
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->addOptions));
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
      TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pMsg->trigger.stateWin.extend));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.stateWin.trueForDuration));
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->trigger.stateWin.zeroth, NULL));
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->trigger.stateWin.expr, NULL));
      break;
    
    case WINDOW_TYPE_INTERVAL:
      // slide trigger
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->trigger.sliding.intervalUnit));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->trigger.sliding.slidingUnit));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->trigger.sliding.offsetUnit));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->trigger.sliding.soffsetUnit));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->trigger.sliding.precision));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.sliding.interval));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.sliding.offset));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.sliding.sliding));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.sliding.soffset));
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
  TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pMsg->calcTsSlotId));
  TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pMsg->triTsSlotId));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->triggerPrevFilter, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->triggerScanPlan, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->calcCacheScanPlan, NULL));

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

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->leaderSnodeId));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->streamName, NULL));
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->precision));
  }

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

void destroySStreamOutCols(void* p){
  if (p == NULL) return;
  SStreamOutCol* col = (SStreamOutCol*)p;
  taosMemoryFreeClear(col->expr);
}

int32_t tDecodeSStreamRunnerDeployMsg(SDecoder* pDecoder, SStreamRunnerDeployMsg* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->execReplica));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->streamName, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->pPlan, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->outDBFName, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->outTblName, NULL));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->outTblType));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->calcNotifyOnly));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->topPlan));

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
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->addOptions));

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
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->outStbSversion));

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

  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->lowLatencyCalc));
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

  TAOS_CHECK_EXIT(tDecodeSStreamMsg(pDecoder, &pStart->header));

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

  TAOS_CHECK_EXIT(tDecodeSStreamMsg(pDecoder, &pUndeploy->header));
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

int32_t tDecodeSStreamRecalcReq(SDecoder* pDecoder, SStreamRecalcReq* recalc) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &recalc->recalcId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &recalc->start));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &recalc->end));

_exit:

  return code;
}

int32_t tDecodeSStreamMgmtRspCont(SDecoder* pDecoder, SStreamMsgType msgType, SStreamMgmtRspCont* pCont) {
  int32_t code = 0;
  int32_t lino;

  switch (msgType) {
    case STREAM_MSG_ORIGTBL_READER_INFO: {
      int32_t vgNum = 0;
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &vgNum));  
      if (vgNum > 0) {
        pCont->vgIds = taosArrayInit_s(sizeof(int32_t), vgNum);
        TSDB_CHECK_NULL(pCont->vgIds, code, lino, _exit, terrno);
      }
      for (int32_t i = 0; i < vgNum; ++i) {
        int32_t *vgId = taosArrayGet(pCont->vgIds, i);
        TAOS_CHECK_EXIT(tDecodeI32(pDecoder, vgId));  
      }

      int32_t readerNum = 0;
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &readerNum));  
      if (readerNum > 0) {
        pCont->readerList = taosArrayInit_s(sizeof(SStreamTaskAddr), readerNum);
        TSDB_CHECK_NULL(pCont->readerList, code, lino, _exit, terrno);
      }
      for (int32_t i = 0; i < readerNum; ++i) {
        SStreamTaskAddr *addr = taosArrayGet(pCont->readerList, i);
        TAOS_CHECK_EXIT(tDecodeSStreamTaskAddr(pDecoder, addr));  
      }
      break;
    }
    case STREAM_MSG_UPDATE_RUNNER: {
      int32_t runnerNum = 0;
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &runnerNum));  
      if (runnerNum > 0) {
        pCont->runnerList = taosArrayInit_s(sizeof(SStreamRunnerTarget), runnerNum);
        TSDB_CHECK_NULL(pCont->runnerList, code, lino, _exit, terrno);
      }
      for (int32_t i = 0; i < runnerNum; ++i) {
        SStreamRunnerTarget *target = taosArrayGet(pCont->runnerList, i);
        TAOS_CHECK_EXIT(tDecodeSStreamRunnerTarget(pDecoder, target));  
      }
      break;
    }
    case STREAM_MSG_USER_RECALC: {
      int32_t recalcNum = 0;
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &recalcNum));  
      if (recalcNum > 0) {
        pCont->recalcList = taosArrayInit_s(sizeof(SStreamRecalcReq), recalcNum);
        TSDB_CHECK_NULL(pCont->recalcList, code, lino, _exit, terrno);
      }
      for (int32_t i = 0; i < recalcNum; ++i) {
        SStreamRecalcReq *recalc = taosArrayGet(pCont->recalcList, i);
        TAOS_CHECK_EXIT(tDecodeSStreamRecalcReq(pDecoder, recalc));  
      }
      break;
    }
    case STREAM_MSG_RUNNER_ORIGTBL_READER: {
      int32_t rspNum = 0, vgNum = 0;
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &rspNum));  
      if (rspNum > 0) {
        pCont->execRspList = taosArrayInit_s(sizeof(SStreamOReaderDeployRsp), rspNum);
        TSDB_CHECK_NULL(pCont->execRspList, code, lino, _exit, terrno);
      }
      for (int32_t i = 0; i < rspNum; ++i) {
        SStreamOReaderDeployRsp *pDeployRsp = taosArrayGet(pCont->execRspList, i);
        TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pDeployRsp->execId));  
        TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &vgNum));
        if (vgNum > 0) {
          pDeployRsp->vgList = taosArrayInit_s(sizeof(SStreamTaskAddr), vgNum);
          TSDB_CHECK_NULL(pDeployRsp->vgList, code, lino, _exit, terrno);
        }
        for (int32_t n = 0; n < vgNum; ++n) {
          SStreamTaskAddr* pAddr = taosArrayGet(pDeployRsp->vgList, n);
          TAOS_CHECK_EXIT(tDecodeSStreamTaskAddr(pDecoder, pAddr));  
        }
      }
      break;
    }
    default:
      break;
  }

_exit:

  return code;
}


int32_t tDecodeSStreamMgmtRsp(SDecoder* pDecoder, SStreamMgmtRsp* pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeSStreamMsg(pDecoder, &pRsp->header));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pRsp->reqId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->code));
  TAOS_CHECK_EXIT(tDecodeStreamTask(pDecoder, &pRsp->task));
  TAOS_CHECK_EXIT(tDecodeSStreamMgmtRspCont(pDecoder, pRsp->header.msgType, &pRsp->cont));

_exit:

  return code;
}

void tFreeSStreamOReaderDeployRsp(void* param) {
  if (NULL == param) {
    return;
  }

  SStreamOReaderDeployRsp* pRsp = (SStreamOReaderDeployRsp*)param;
  taosArrayDestroy(pRsp->vgList);
}

void tFreeSStreamMgmtRsp(void* param) {
  if (NULL == param) {
    return;
  }
  
  SStreamMgmtRsp* pRsp = (SStreamMgmtRsp*)param;

  taosArrayDestroy(pRsp->cont.vgIds);
  taosArrayDestroy(pRsp->cont.readerList);
  taosArrayDestroy(pRsp->cont.runnerList);
  taosArrayDestroy(pRsp->cont.recalcList);
  taosArrayDestroyEx(pRsp->cont.execRspList, tFreeSStreamOReaderDeployRsp);
}

void tFreeSStreamReaderDeployMsg(SStreamReaderDeployMsg* pReader) {
  if (NULL == pReader) {
    return;
  }
  
  if (pReader->triggerReader) {
    SStreamReaderDeployFromTrigger* pMsg = (SStreamReaderDeployFromTrigger*)&pReader->msg.trigger;
    taosMemoryFree(pMsg->triggerTblName);
    taosMemoryFree(pMsg->partitionCols);
    taosMemoryFree(pMsg->triggerCols);
    taosMemoryFree(pMsg->triggerScanPlan);
    taosMemoryFree(pMsg->calcCacheScanPlan);
  } else {
    SStreamReaderDeployFromCalc* pMsg = (SStreamReaderDeployFromCalc*)&pReader->msg.calc;
    taosMemoryFree(pMsg->calcScanPlan);
  }
}

void tFreeStreamNotifyUrl(void* param) {
  if (NULL == param) {
    return;
  }

  taosMemoryFree(*(void**)param);
}

void tFreeSStreamTriggerDeployMsg(SStreamTriggerDeployMsg* pTrigger) {
  if (NULL == pTrigger) {
    return;
  }
  
  taosArrayDestroyEx(pTrigger->pNotifyAddrUrls, tFreeStreamNotifyUrl);
  switch (pTrigger->triggerType) {
    case WINDOW_TYPE_STATE:
      taosMemoryFree(pTrigger->trigger.stateWin.zeroth);
      taosMemoryFree(pTrigger->trigger.stateWin.expr);
      break;
    case WINDOW_TYPE_EVENT:
      taosMemoryFree(pTrigger->trigger.event.startCond);
      taosMemoryFree(pTrigger->trigger.event.endCond);
      break;
    case WINDOW_TYPE_COUNT:
      taosMemoryFree(pTrigger->trigger.count.condCols);  
      break;
    default:
      break;
  }

  taosMemoryFree(pTrigger->partitionCols);
  taosMemoryFree(pTrigger->triggerPrevFilter);
  taosMemoryFree(pTrigger->triggerScanPlan);
  taosMemoryFree(pTrigger->calcCacheScanPlan);

  taosArrayDestroy(pTrigger->readerList);
  taosArrayDestroy(pTrigger->runnerList);
  taosMemoryFree(pTrigger->streamName);
}

void tFreeSStreamOutCol(void* param) {
  if (NULL == param) {
    return;
  }

  SStreamOutCol* pOut = (SStreamOutCol*)param;
  taosMemoryFree(pOut->expr);
}

void tFreeSStreamRunnerDeployMsg(SStreamRunnerDeployMsg* pRunner) {
  if (NULL == pRunner) {
    return;
  }

  taosMemoryFree(pRunner->streamName);
  taosMemoryFree(pRunner->pPlan);
  taosMemoryFree(pRunner->outDBFName);
  taosMemoryFree(pRunner->outTblName);

  taosArrayDestroyEx(pRunner->pNotifyAddrUrls, tFreeStreamNotifyUrl);
  taosArrayDestroy(pRunner->outCols);
  taosArrayDestroy(pRunner->outTags);

  taosMemoryFree(pRunner->subTblNameExpr);
  taosMemoryFree(pRunner->tagValueExpr);
  taosArrayDestroyEx(pRunner->forceOutCols, tFreeSStreamOutCol);
}

void tFreeSStmTaskDeploy(void* param) {
  if (NULL == param) {
    return;
  }

  SStmTaskDeploy* pTask = (SStmTaskDeploy*)param;
  switch (pTask->task.type)  {
    case STREAM_READER_TASK:
      tFreeSStreamReaderDeployMsg(&pTask->msg.reader);
      break;
    case STREAM_TRIGGER_TASK:
      tFreeSStreamTriggerDeployMsg(&pTask->msg.trigger);
      break;
    case STREAM_RUNNER_TASK:
      tFreeSStreamRunnerDeployMsg(&pTask->msg.runner);
      break;
    default:
      break;
  }
}

void tFreeSStmStreamDeploy(void* param) {
  if (NULL == param) {
    return;
  }
  
  SStmStreamDeploy* pDeploy = (SStmStreamDeploy*)param;
  taosArrayDestroy(pDeploy->readerTasks);
  if (pDeploy->triggerTask) {
    taosArrayDestroy(pDeploy->triggerTask->msg.trigger.readerList);
    taosArrayDestroy(pDeploy->triggerTask->msg.trigger.runnerList);
    taosMemoryFree(pDeploy->triggerTask);
  }

  int32_t runnerNum = taosArrayGetSize(pDeploy->runnerTasks);
  for (int32_t i = 0; i < runnerNum; ++i) {
    SStmTaskDeploy* pRunner = taosArrayGet(pDeploy->runnerTasks, i);
    taosMemoryFree(pRunner->msg.runner.pPlan);
  }
  taosArrayDestroy(pDeploy->runnerTasks);
}

void tDeepFreeSStmStreamDeploy(void* param) {
  if (NULL == param) {
    return;
  }
  
  SStmStreamDeploy* pDeploy = (SStmStreamDeploy*)param;
  taosArrayDestroyEx(pDeploy->readerTasks, tFreeSStmTaskDeploy);
  tFreeSStmTaskDeploy(pDeploy->triggerTask);
  taosMemoryFree(pDeploy->triggerTask);
  taosArrayDestroyEx(pDeploy->runnerTasks, tFreeSStmTaskDeploy);
}


void tFreeSMStreamHbRspMsg(SMStreamHbRspMsg* pRsp) {
  if (NULL == pRsp) {
    return;
  }
  taosArrayDestroyEx(pRsp->deploy.streamList, tFreeSStmStreamDeploy);
  taosArrayDestroy(pRsp->start.taskList);
  taosArrayDestroy(pRsp->undeploy.taskList);
  taosArrayDestroyEx(pRsp->rsps.rspList, tFreeSStreamMgmtRsp);
}

void tDeepFreeSMStreamHbRspMsg(SMStreamHbRspMsg* pRsp) {
  if (NULL == pRsp) {
    return;
  }
  taosArrayDestroyEx(pRsp->deploy.streamList, tDeepFreeSStmStreamDeploy);
  taosArrayDestroy(pRsp->start.taskList);
  taosArrayDestroy(pRsp->undeploy.taskList);
  taosArrayDestroyEx(pRsp->rsps.rspList, tFreeSStreamMgmtRsp);
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

  int32_t rspNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &rspNum));
  if (rspNum > 0) {
    pRsp->rsps.rspList = taosArrayInit_s(sizeof(SStreamMgmtRsp), rspNum);
    TSDB_CHECK_NULL(pRsp->rsps.rspList, code, lino, _exit, terrno);
    for (int32_t i = 0; i < rspNum; ++i) {
      SStreamMgmtRsp* pMgmtRsp = (SStreamMgmtRsp*)taosArrayGet(pRsp->rsps.rspList, i);
      TAOS_CHECK_EXIT(tDecodeSStreamMgmtRsp(pDecoder, pMgmtRsp));
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
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->igNoDataTrigger));

  // notify part
  int32_t addrSize = (int32_t)taosArrayGetSize(pReq->pNotifyAddrUrls);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, addrSize));
  for (int32_t i = 0; i < addrSize; ++i) {
    const char *url = taosArrayGetP(pReq->pNotifyAddrUrls, i);
    TAOS_CHECK_EXIT((tEncodeCStr(pEncoder, url)));
  }
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->notifyEventTypes));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->addOptions));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->notifyHistory));

  // out table part

  // trigger cols and partition cols
  int32_t filterColsLen = pReq->triggerFilterCols == NULL ? 0 : (int32_t)strlen((char*)pReq->triggerFilterCols) + 1;
  int32_t triggerColsLen = pReq->triggerCols == NULL ? 0 : (int32_t)strlen((char*)pReq->triggerCols) + 1;
  int32_t partitionColsLen = pReq->partitionCols == NULL ? 0 : (int32_t)strlen((char*)pReq->partitionCols) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->triggerFilterCols, filterColsLen));
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
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->trigger.period.precision));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->trigger.period.periodUnit));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->trigger.period.offsetUnit));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.period.period));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->trigger.period.offset));
      break;
    }
  }

  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->triggerTblType));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pReq->triggerTblUid));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pReq->triggerTblSuid));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->vtableCalc));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->outTblType));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->outStbExists));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pReq->outStbUid));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->outStbSversion));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->eventTypes));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->flags));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->tsmaId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->placeHolderBitmap));
  TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pReq->calcTsSlotId));
  TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pReq->triTsSlotId));

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->triggerTblVgId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->outTblVgId));

  int32_t triggerScanPlanLen = pReq->triggerScanPlan == NULL ? 0 : (int32_t)strlen((char*)pReq->triggerScanPlan) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->triggerScanPlan, triggerScanPlanLen));

  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->triggerHasPF));
  int32_t triggerFilterLen = pReq->triggerPrevFilter == NULL ? 0 : (int32_t)strlen((char*)pReq->triggerPrevFilter) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->triggerPrevFilter, triggerFilterLen));

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
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pCalcScanPlan->readFromCache));
    TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pCalcScanPlan->scanPlan, scanPlanLen));
  }

  int32_t calcPlanLen = pReq->calcPlan == NULL ? 0 : (int32_t)strlen((char*)pReq->calcPlan) + 1;
  int32_t subTblNameExprLen = pReq->subTblNameExpr == NULL ? 0 : (int32_t)strlen((char*)pReq->subTblNameExpr) + 1;
  int32_t tagValueExprLen = pReq->tagValueExpr == NULL ? 0 : (int32_t)strlen((char*)pReq->tagValueExpr) + 1;

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->numOfCalcSubplan));
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

  switch (pReq->triggerType) {
    case WINDOW_TYPE_STATE: {
      // state trigger
      int32_t stateExprLen = pReq->trigger.stateWin.expr == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.stateWin.expr) + 1;
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->trigger.stateWin.expr, stateExprLen));
      TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pReq->trigger.stateWin.extend));
      int32_t stateWindowZerothLen = pReq->trigger.stateWin.zeroth == NULL ? 0 : (int32_t)strlen((char*)pReq->trigger.stateWin.zeroth) + 1;
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->trigger.stateWin.zeroth, stateWindowZerothLen));
      break;
    }
    case WINDOW_TYPE_INTERVAL: {
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->trigger.sliding.overlap));
      break;
    }
    default: {
      break;
    }
  }

  TAOS_CHECK_EXIT(tEncodeU8(pEncoder, pReq->triggerPrec));

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
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->igNoDataTrigger));

  int32_t addrSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &addrSize));
  if (addrSize > 0) {
    pReq->pNotifyAddrUrls = taosArrayInit(addrSize, POINTER_BYTES);
    if (pReq->pNotifyAddrUrls == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
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
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->addOptions));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->notifyHistory));

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->triggerFilterCols, NULL));
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
    pReq->outTags = taosArrayInit(outTagSize, sizeof(SFieldWithOptions));
    if (pReq->outTags == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = 0; i < outTagSize; ++i) {
      SFieldWithOptions field = {0};
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
        pReq->trigger.stateWin.extend = 0;
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
        TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->trigger.event.startCond, NULL));
        TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->trigger.event.endCond, NULL));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.event.trueForDuration));
        break;
      }
      case WINDOW_TYPE_COUNT: {
        TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->trigger.count.condCols, NULL));

        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.count.countVal));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.count.sliding));
        break;
      }
      case WINDOW_TYPE_PERIOD: {
        // period trigger
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->trigger.period.precision));
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->trigger.period.periodUnit));
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->trigger.period.offsetUnit));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.period.period));
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->trigger.period.offset));
        break;
      }
      default:
        TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->triggerTblType));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pReq->triggerTblUid));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pReq->triggerTblSuid));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->vtableCalc));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->outTblType));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->outStbExists));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pReq->outStbUid));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->outStbSversion));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->eventTypes));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->flags));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->tsmaId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->placeHolderBitmap));
  TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pReq->calcTsSlotId));
  TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pReq->triTsSlotId));

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->triggerTblVgId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->outTblVgId));

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->triggerScanPlan, NULL));

  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->triggerHasPF));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->triggerPrevFilter, NULL));

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
      }
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &calcScan.readFromCache));
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&calcScan.scanPlan, NULL));
      if (taosArrayPush(pReq->calcScanPlanList, &calcScan) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->numOfCalcSubplan));
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

  switch (pReq->triggerType) {
    case WINDOW_TYPE_STATE: {
      // state trigger
      if (!tDecodeIsEnd(pDecoder)) {
        TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->trigger.stateWin.expr, NULL));
      }
      if (!tDecodeIsEnd(pDecoder)) {
        TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pReq->trigger.stateWin.extend));
      }
      if (!tDecodeIsEnd(pDecoder)) {
        TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->trigger.stateWin.zeroth, NULL));
      }
      break;
    }
    case WINDOW_TYPE_INTERVAL: {
      if (!tDecodeIsEnd(pDecoder)) {
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->trigger.sliding.overlap));
      }
      break;
    }
    default:
      break;
  }

  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_EXIT(tDecodeU8(pDecoder, &pReq->triggerPrec));
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

  int32_t nameLen = pReq->name == NULL ? 0 : (int32_t)strlen(pReq->name) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->name, nameLen));
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
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void**)&pReq->name, NULL));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeMDropStreamReq(SMDropStreamReq *pReq) {
  taosMemoryFreeClear(pReq->name);
}

static FORCE_INLINE void tFreeStreamCalcScan(void* pScan) {
  if (pScan == NULL) {
    return;
  }
  SStreamCalcScan *pCalcScan = (SStreamCalcScan *)pScan;
  taosArrayDestroy(pCalcScan->vgList);
  taosMemoryFreeClear(pCalcScan->scanPlan);
}

void tFreeStreamOutCol(void* pCol) {
  if (pCol == NULL) {
    return;
  }
  SStreamOutCol *pOutCol = (SStreamOutCol *)pCol;
  taosMemoryFreeClear(pOutCol->expr);
}



void tFreeSCMCreateStreamReq(SCMCreateStreamReq *pReq) {
  if (NULL == pReq) {
    return;
  }
  taosMemoryFreeClear(pReq->name);
  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->streamDB);
  taosMemoryFreeClear(pReq->triggerDB);
  taosMemoryFreeClear(pReq->outDB);
  taosMemoryFreeClear(pReq->triggerTblName);
  taosMemoryFreeClear(pReq->outTblName);

  taosArrayDestroyP(pReq->calcDB, NULL);
  pReq->calcDB = NULL;
  taosArrayDestroyP(pReq->pNotifyAddrUrls, NULL);
  pReq->pNotifyAddrUrls = NULL;

  taosMemoryFreeClear(pReq->triggerFilterCols);
  taosMemoryFreeClear(pReq->triggerCols);
  taosMemoryFreeClear(pReq->partitionCols);

  taosArrayDestroy(pReq->outTags);
  pReq->outTags = NULL;
  taosArrayDestroy(pReq->outCols);
  pReq->outCols = NULL;

  switch (pReq->triggerType) {
    case WINDOW_TYPE_STATE:
      taosMemoryFreeClear(pReq->trigger.stateWin.zeroth);
      taosMemoryFreeClear(pReq->trigger.stateWin.expr);
      break;
    case WINDOW_TYPE_EVENT:
      taosMemoryFreeClear(pReq->trigger.event.startCond);
      taosMemoryFreeClear(pReq->trigger.event.endCond);
      break;
    default:
      break;
  }

  taosMemoryFreeClear(pReq->triggerScanPlan);
  taosArrayDestroyEx(pReq->calcScanPlanList, tFreeStreamCalcScan);
  pReq->calcScanPlanList = NULL;
  taosMemoryFreeClear(pReq->triggerPrevFilter);

  taosMemoryFreeClear(pReq->calcPlan);
  taosMemoryFreeClear(pReq->subTblNameExpr);
  taosMemoryFreeClear(pReq->tagValueExpr);
  taosArrayDestroyEx(pReq->forceOutCols, tFreeStreamOutCol);
  pReq->forceOutCols = NULL;
}

int32_t tCloneStreamCreateDeployPointers(SCMCreateStreamReq *pSrc, SCMCreateStreamReq** ppDst) {
  int32_t code = 0, lino = 0;
  if (NULL == pSrc) {
    return code;
  } 

  void* p = NULL;
  int32_t num = 0;
  *ppDst = taosMemoryCalloc(1, sizeof(SCMCreateStreamReq));
  TSDB_CHECK_NULL(*ppDst, code, lino, _exit, terrno);

  SCMCreateStreamReq* pDst = *ppDst;

  if (pSrc->outDB) {
    pDst->outDB = COPY_STR(pSrc->outDB);
    TSDB_CHECK_NULL(pDst->outDB, code, lino, _exit, terrno);
  }
  
  if (pSrc->triggerTblName) {
    pDst->triggerTblName = COPY_STR(pSrc->triggerTblName);
    TSDB_CHECK_NULL(pDst->triggerTblName, code, lino, _exit, terrno);
  }
  
  if (pSrc->outTblName) {
    pDst->outTblName = COPY_STR(pSrc->outTblName);
    TSDB_CHECK_NULL(pDst->outTblName, code, lino, _exit, terrno);
  }
  
  if (pSrc->pNotifyAddrUrls) {
    num = taosArrayGetSize(pSrc->pNotifyAddrUrls);
    if (num > 0) {
      pDst->pNotifyAddrUrls = taosArrayInit(num, POINTER_BYTES);
      TSDB_CHECK_NULL(pDst->pNotifyAddrUrls, code, lino, _exit, terrno);
    }
    for (int32_t i = 0; i < num; ++i) {
      p = taosStrdup(taosArrayGetP(pSrc->pNotifyAddrUrls, i));
      TSDB_CHECK_NULL(p, code, lino, _exit, terrno);
      TSDB_CHECK_NULL(taosArrayPush(pDst->pNotifyAddrUrls, &p), code, lino, _exit, terrno);
    }
  }
  
  if (pSrc->triggerFilterCols) {
    pDst->triggerFilterCols = COPY_STR(pSrc->triggerFilterCols);
    TSDB_CHECK_NULL(pDst->triggerFilterCols, code, lino, _exit, terrno);
  }
  
  if (pSrc->triggerCols) {
    pDst->triggerCols = COPY_STR(pSrc->triggerCols);
    TSDB_CHECK_NULL(pDst->triggerCols, code, lino, _exit, terrno);
  }
  
  if (pSrc->partitionCols) {
    pDst->partitionCols = COPY_STR(pSrc->partitionCols);
    TSDB_CHECK_NULL(pDst->partitionCols, code, lino, _exit, terrno);
  }
  
  if (pSrc->outCols) {
    pDst->outCols = taosArrayDup(pSrc->outCols, NULL);
    TSDB_CHECK_NULL(pDst->outCols, code, lino, _exit, terrno);
  }
  
  if (pSrc->outTags) {
    pDst->outTags = taosArrayDup(pSrc->outTags, NULL);
    TSDB_CHECK_NULL(pDst->outTags, code, lino, _exit, terrno);
  }

  pDst->triggerType = pSrc->triggerType;
  
  switch (pSrc->triggerType) {
    case WINDOW_TYPE_STATE:
      pDst->trigger.stateWin.slotId = pSrc->trigger.stateWin.slotId;
      pDst->trigger.stateWin.extend = pSrc->trigger.stateWin.extend;
      pDst->trigger.stateWin.trueForDuration = pSrc->trigger.stateWin.trueForDuration;
      if (pSrc->trigger.stateWin.zeroth) {
        pDst->trigger.stateWin.zeroth = COPY_STR(pSrc->trigger.stateWin.zeroth);
        TSDB_CHECK_NULL(pDst->trigger.stateWin.zeroth, code, lino, _exit, terrno);
      }
      if (pSrc->trigger.stateWin.expr) {
        pDst->trigger.stateWin.expr = COPY_STR(pSrc->trigger.stateWin.expr);
        TSDB_CHECK_NULL(pDst->trigger.stateWin.expr, code, lino, _exit, terrno);
      }
      break;
    case WINDOW_TYPE_EVENT:
      if (pSrc->trigger.event.startCond) {
        pDst->trigger.event.startCond = COPY_STR(pSrc->trigger.event.startCond);
        TSDB_CHECK_NULL(pDst->trigger.event.startCond, code, lino, _exit, terrno);
      }
      
      if (pSrc->trigger.event.endCond) {
        pDst->trigger.event.endCond = COPY_STR(pSrc->trigger.event.endCond);
        TSDB_CHECK_NULL(pDst->trigger.event.endCond, code, lino, _exit, terrno);
      }
      pDst->trigger.event.trueForDuration = pSrc->trigger.event.trueForDuration;
      break;
    default:
      pDst->trigger = pSrc->trigger;
      break;
  }


  if (pSrc->triggerScanPlan) {
    pDst->triggerScanPlan = COPY_STR(pSrc->triggerScanPlan);
    TSDB_CHECK_NULL(pDst->triggerScanPlan, code, lino, _exit, terrno);
  }
  
  if (pSrc->calcScanPlanList) {
    num = taosArrayGetSize(pSrc->calcScanPlanList);
    if (num > 0) {
      pDst->calcScanPlanList = taosArrayInit(num, sizeof(SStreamCalcScan));
      TSDB_CHECK_NULL(pDst->calcScanPlanList, code, lino, _exit, terrno);
    }
    for (int32_t i = 0; i < num; ++i) {
      SStreamCalcScan* sscan = taosArrayGet(pSrc->calcScanPlanList, i);
      SStreamCalcScan  dscan = {.readFromCache = sscan->readFromCache};

      dscan.vgList = taosArrayDup(sscan->vgList, NULL);
      TSDB_CHECK_NULL(dscan.vgList, code, lino, _exit, terrno);

      dscan.scanPlan = COPY_STR(sscan->scanPlan);
      TSDB_CHECK_NULL(dscan.scanPlan, code, lino, _exit, terrno);
      
      TSDB_CHECK_NULL(taosArrayPush(pDst->calcScanPlanList, &dscan), code, lino, _exit, terrno);
    }
  }
  
  if (pSrc->triggerPrevFilter) {
    pDst->triggerPrevFilter = COPY_STR(pSrc->triggerPrevFilter);
    TSDB_CHECK_NULL(pDst->triggerPrevFilter, code, lino, _exit, terrno);
  }
  
  if (pSrc->calcPlan) {
    pDst->calcPlan = COPY_STR(pSrc->calcPlan);
    TSDB_CHECK_NULL(pDst->calcPlan, code, lino, _exit, terrno);
  }
  
  if (pSrc->subTblNameExpr) {
    pDst->subTblNameExpr = COPY_STR(pSrc->subTblNameExpr);
    TSDB_CHECK_NULL(pDst->subTblNameExpr, code, lino, _exit, terrno);
  }
  
  if (pSrc->tagValueExpr) {
    pDst->tagValueExpr = COPY_STR(pSrc->tagValueExpr);
    TSDB_CHECK_NULL(pDst->tagValueExpr, code, lino, _exit, terrno);
  }
  
  if (pSrc->forceOutCols) {
    num = taosArrayGetSize(pSrc->forceOutCols);
    if (num > 0) {
      pDst->forceOutCols = taosArrayInit(num, sizeof(SStreamOutCol));
      TSDB_CHECK_NULL(pDst->forceOutCols, code, lino, _exit, terrno);
    }
    for (int32_t i = 0; i < num; ++i) {
      SStreamOutCol* scol = taosArrayGet(pSrc->forceOutCols, i);
      SStreamOutCol  dcol = {.type = scol->type};

      dcol.expr = COPY_STR(scol->expr);
      TSDB_CHECK_NULL(dcol.expr, code, lino, _exit, terrno);
      
      TSDB_CHECK_NULL(taosArrayPush(pDst->forceOutCols, &dcol), code, lino, _exit, terrno);
    }
  }

  pDst->triggerTblUid = pSrc->triggerTblUid;
  pDst->triggerTblType = pSrc->triggerTblType;
  pDst->triggerPrec = pSrc->triggerPrec;
  pDst->deleteReCalc = pSrc->deleteReCalc;
  pDst->deleteOutTbl = pSrc->deleteOutTbl;
  
_exit:

  if (code) {
    tFreeSCMCreateStreamReq(pDst);
    uError("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


int32_t tSerializeSMPauseStreamReq(void *buf, int32_t bufLen, const SMPauseStreamReq *pReq) {
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t nameLen = pReq->name == NULL ? 0 : (int32_t)strlen(pReq->name) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->name, nameLen));
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

int32_t tDeserializeSMPauseStreamReq(void *buf, int32_t bufLen, SMPauseStreamReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void**)&pReq->name, NULL));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeMPauseStreamReq(SMPauseStreamReq *pReq) {
  taosMemoryFreeClear(pReq->name);
}

int32_t tSerializeSMResumeStreamReq(void *buf, int32_t bufLen, const SMResumeStreamReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  int32_t nameLen = pReq->name == NULL ? 0 : (int32_t)strlen(pReq->name) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->name, nameLen));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igNotExists));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igUntreated));
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

int32_t tDeserializeSMResumeStreamReq(void *buf, int32_t bufLen, SMResumeStreamReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void**)&pReq->name, NULL));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igUntreated));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeMResumeStreamReq(SMResumeStreamReq *pReq) {
  taosMemoryFreeClear(pReq->name);
}

int32_t tSerializeSMRecalcStreamReq(void *buf, int32_t bufLen, const SMRecalcStreamReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  int32_t nameLen = pReq->name == NULL ? 0 : (int32_t)strlen(pReq->name) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->name, nameLen));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->calcAll));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->timeRange.skey));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->timeRange.ekey));
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

int32_t tDeserializeSMRecalcStreamReq(void *buf, int32_t bufLen, SMRecalcStreamReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void**)&pReq->name, NULL));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->calcAll));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->timeRange.skey));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->timeRange.ekey));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeMRecalcStreamReq(SMRecalcStreamReq *pReq) {
  taosMemoryFreeClear(pReq->name);
}

static int32_t tEncodeStreamProgressReq(SEncoder *pEncoder, const SStreamProgressReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->fetchIdx));

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
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->fetchIdx));

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
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pRsp->fillHisFinished));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRsp->progressDelay));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->fetchIdx));

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
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, (int8_t *)&pRsp->fillHisFinished));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pRsp->progressDelay));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->fetchIdx));

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

int32_t tSerializeSTriggerOrigTableInfoRsp(void* buf, int32_t bufLen, const SSTriggerOrigTableInfoRsp* pRsp){
  SEncoder encoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  tlen = 0;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t size = taosArrayGetSize(pRsp->cols);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, size));
  for (int32_t i = 0; i < size; ++i) {
    OTableInfoRsp* oInfo = taosArrayGet(pRsp->cols, i);
    if (oInfo == NULL) {
      uError("col id is NULL at index %d", i);
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, oInfo->suid));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, oInfo->uid));
    TAOS_CHECK_EXIT(tEncodeI16(&encoder, oInfo->cid));
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

int32_t tDserializeSTriggerOrigTableInfoRsp(void* buf, int32_t bufLen, SSTriggerOrigTableInfoRsp* pRsp){
  SDecoder decoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t size = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &size));
  pRsp->cols = taosArrayInit(size, sizeof(OTableInfoRsp));
  if (pRsp->cols == NULL) {
    code = terrno;
    uError("failed to allocate memory for cids, size: %d, errno: %d", size, code);
    goto _exit;
  }
  for (int32_t i = 0; i < size; ++i) {
    OTableInfoRsp* oInfo = taosArrayReserve(pRsp->cols, 1);
    if (oInfo == NULL) {
      code = terrno;
      uError("failed to reserve memory for OTableInfo, size: %d, errno: %d", size, code);
      goto _exit;
    }
    TAOS_CHECK_RETURN(tDecodeI64(&decoder, &oInfo->suid));
    TAOS_CHECK_RETURN(tDecodeI64(&decoder, &oInfo->uid));
    TAOS_CHECK_RETURN(tDecodeI16(&decoder, &oInfo->cid));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void    tDestroySTriggerOrigTableInfoRsp(SSTriggerOrigTableInfoRsp* pRsp){
  taosArrayDestroy(pRsp->cols);
}

void tDestroySTriggerPullRequest(SSTriggerPullRequestUnion* pReq) {
  if (pReq == NULL) return;
  if (pReq->base.type == STRIGGER_PULL_WAL_DATA_NEW || pReq->base.type == STRIGGER_PULL_WAL_CALC_DATA_NEW) {
    SSTriggerWalDataNewRequest* pRequest = (SSTriggerWalDataNewRequest*)pReq;
    taosArrayDestroy(pRequest->versions);
    tSimpleHashCleanup(pRequest->ranges);
  } else if (pReq->base.type == STRIGGER_PULL_TSDB_DATA) {
    SSTriggerTsdbDataRequest* pRequest = (SSTriggerTsdbDataRequest*)pReq;
    if (pRequest->cids != NULL) {
      taosArrayDestroy(pRequest->cids);
      pRequest->cids = NULL;
    }
  } else if (pReq->base.type == STRIGGER_PULL_VTABLE_INFO) {
    SSTriggerVirTableInfoRequest* pRequest = (SSTriggerVirTableInfoRequest*)pReq;
    if (pRequest->cids != NULL) {
      taosArrayDestroy(pRequest->cids);
      pRequest->cids = NULL;
    }
  } else if (pReq->base.type == STRIGGER_PULL_VTABLE_PSEUDO_COL) {
    SSTriggerVirTablePseudoColRequest *pRequest = (SSTriggerVirTablePseudoColRequest*)pReq;
    if (pRequest->cids != NULL) {
      taosArrayDestroy(pRequest->cids);
      pRequest->cids = NULL;
    }
  } else if (pReq->base.type == STRIGGER_PULL_OTABLE_INFO) {
    SSTriggerOrigTableInfoRequest* pRequest = (SSTriggerOrigTableInfoRequest*)pReq;
    if (pRequest->cols != NULL) {
      taosArrayDestroy(pRequest->cols);
      pRequest->cols = NULL;
    }
  } else if (pReq->base.type == STRIGGER_PULL_SET_TABLE) {
    SSTriggerSetTableRequest* pRequest = (SSTriggerSetTableRequest*)pReq;
    tSimpleHashCleanup(pRequest->uidInfoTrigger);
    tSimpleHashCleanup(pRequest->uidInfoCalc);
  }
}

int32_t encodeColsArray(SEncoder* encoder, SArray* cids) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t size = taosArrayGetSize(cids);
  TAOS_CHECK_EXIT(tEncodeI32(encoder, size));
  for (int32_t i = 0; i < size; ++i) {
    col_id_t* pColId = taosArrayGet(cids, i);
    if (pColId == NULL) {
      uError("col id is NULL at index %d", i);
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    TAOS_CHECK_EXIT(tEncodeI16(encoder, *pColId));
  }
  _exit:

  return code;
}

int32_t decodeColsArray(SDecoder* decoder, SArray** cids) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t size = 0;

  TAOS_CHECK_EXIT(tDecodeI32(decoder, &size));
  if (size > 0){
    *cids = taosArrayInit(size, sizeof(col_id_t));
    if (*cids == NULL) {
      code = terrno;
      uError("failed to allocate memory for cids, size: %d, errno: %d", size, code);
      goto _exit;
    }
  
    for (int32_t i = 0; i < size; ++i) {
      col_id_t* pColId = taosArrayReserve(*cids, 1);
      if (pColId == NULL) {
        code = terrno;
        uError("failed to reserve memory for col id at index %d, errno: %d", i, code);
        goto _exit;
      }
      TAOS_CHECK_RETURN(tDecodeI16(decoder, pColId));
    }  
  }
  
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(*cids);
    *cids = NULL;
  }
  return code;
}

static int32_t encodeSetTableMapInfo(SEncoder* encoder, SSHashObj* pInfo) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t size = tSimpleHashGetSize(pInfo);
  TAOS_CHECK_EXIT(tEncodeI32(encoder, size));
  int32_t iter = 0;
  void*   px = tSimpleHashIterate(pInfo, NULL, &iter);
  while (px != NULL) {
    int64_t* uid = tSimpleHashGetKey(px, NULL);
    TAOS_CHECK_EXIT(tEncodeI64(encoder, *uid));
    TAOS_CHECK_EXIT(tEncodeI64(encoder, *(uid + 1)));
    SSHashObj* info = *(SSHashObj**)px;
    int32_t len = tSimpleHashGetSize(info);
    TAOS_CHECK_EXIT(tEncodeI32(encoder, len));
    int32_t iter1 = 0;
    void*   px1 = tSimpleHashIterate(info, NULL, &iter1);
    while (px1 != NULL) {
      int16_t* slot = tSimpleHashGetKey(px1, NULL);
      int16_t* cid = (int16_t*)px1;
      TAOS_CHECK_EXIT(tEncodeI16(encoder, *slot));
      TAOS_CHECK_EXIT(tEncodeI16(encoder, *cid));

      px1 = tSimpleHashIterate(info, px1, &iter1);
    }

    px = tSimpleHashIterate(pInfo, px, &iter);
  }
  
_exit:
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
    case STRIGGER_PULL_SET_TABLE: {
      SSTriggerSetTableRequest* pRequest = (SSTriggerSetTableRequest*)pReq;
      TAOS_CHECK_EXIT(encodeSetTableMapInfo(&encoder, pRequest->uidInfoTrigger));
      TAOS_CHECK_EXIT(encodeSetTableMapInfo(&encoder, pRequest->uidInfoCalc));
      break;
    }
    case STRIGGER_PULL_LAST_TS: {
      break;
    }
    case STRIGGER_PULL_FIRST_TS: {
      SSTriggerFirstTsRequest* pRequest = (SSTriggerFirstTsRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->gid));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->startTime));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
      break;
    }
    case STRIGGER_PULL_TSDB_META: {
      SSTriggerTsdbMetaRequest* pRequest = (SSTriggerTsdbMetaRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->startTime));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->endTime));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->gid));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRequest->order));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
      break;
    }
    case STRIGGER_PULL_TSDB_META_NEXT: {
      break;
    }
    case STRIGGER_PULL_TSDB_TS_DATA: {
      SSTriggerTsdbTsDataRequest* pRequest = (SSTriggerTsdbTsDataRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->suid));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->uid));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->skey));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ekey));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
      break;
    }
    case STRIGGER_PULL_TSDB_TRIGGER_DATA: {
      SSTriggerTsdbTriggerDataRequest* pRequest = (SSTriggerTsdbTriggerDataRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->startTime));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->gid));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRequest->order));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
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
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
      break;
    }
    case STRIGGER_PULL_TSDB_CALC_DATA_NEXT: {
      break;
    }
    case STRIGGER_PULL_TSDB_DATA: {
      SSTriggerTsdbDataRequest* pRequest = (SSTriggerTsdbDataRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->suid));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->uid));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->skey));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ekey));
      TAOS_CHECK_EXIT(encodeColsArray(&encoder, pRequest->cids));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRequest->order));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
      break;
    }
    case STRIGGER_PULL_TSDB_DATA_NEXT: {
      break;
    }
    case STRIGGER_PULL_WAL_META_NEW: {
      SSTriggerWalMetaNewRequest* pRequest = (SSTriggerWalMetaNewRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->lastVer));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ctime));
      break;
    }
    case STRIGGER_PULL_WAL_DATA_NEW:
    case STRIGGER_PULL_WAL_CALC_DATA_NEW: {
      SSTriggerWalDataNewRequest* pRequest = (SSTriggerWalDataNewRequest*)pReq;
      int32_t                     nVersion = taosArrayGetSize(pRequest->versions);
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, nVersion));
      for (int32_t i = 0; i < nVersion; i++) {
        int64_t ver = *(int64_t*)TARRAY_GET_ELEM(pRequest->versions, i);
        TAOS_CHECK_EXIT(tEncodeI64(&encoder, ver));
      }
      int32_t nRanges = tSimpleHashGetSize(pRequest->ranges);
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, nRanges));
      int32_t iter = 0;
      void*   px = tSimpleHashIterate(pRequest->ranges, NULL, &iter);
      while (px != NULL) {
        uint64_t* gid = tSimpleHashGetKey(px, NULL);
        TAOS_CHECK_EXIT(tEncodeU64(&encoder, *gid));
        int64_t* key = (int64_t*)px;
        TAOS_CHECK_EXIT(tEncodeI64(&encoder, key[0]));
        TAOS_CHECK_EXIT(tEncodeI64(&encoder, key[1]));

        px = tSimpleHashIterate(pRequest->ranges, px, &iter);
      }
      break;
    }
    case STRIGGER_PULL_WAL_META_DATA_NEW: {
      SSTriggerWalMetaDataNewRequest* pRequest = (SSTriggerWalMetaDataNewRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->lastVer));
      break;
    }
    case STRIGGER_PULL_GROUP_COL_VALUE: {
      SSTriggerGroupColValueRequest* pRequest = (SSTriggerGroupColValueRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->gid));
      break;
    }
    case STRIGGER_PULL_VTABLE_INFO: {
      SSTriggerVirTableInfoRequest* pRequest = (SSTriggerVirTableInfoRequest*)pReq;
      TAOS_CHECK_EXIT(encodeColsArray(&encoder, pRequest->cids));
      break;
    }
    case STRIGGER_PULL_VTABLE_PSEUDO_COL: {
      SSTriggerVirTablePseudoColRequest* pRequest = (SSTriggerVirTablePseudoColRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->uid));
      TAOS_CHECK_EXIT(encodeColsArray(&encoder, pRequest->cids));
      break;
    }
    case STRIGGER_PULL_OTABLE_INFO: {
      SSTriggerOrigTableInfoRequest* pRequest = (SSTriggerOrigTableInfoRequest*)pReq;
      int32_t size = taosArrayGetSize(pRequest->cols);
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, size));
      for (int32_t i = 0; i < size; ++i) {
        OTableInfo* oInfo = taosArrayGet(pRequest->cols, i);
        if (oInfo == NULL) {
          uError("col id is NULL at index %d", i);
          code = TSDB_CODE_INVALID_PARA;
          goto _exit;
        }
        TAOS_CHECK_EXIT(tEncodeCStr(&encoder, oInfo->refTableName));
        TAOS_CHECK_EXIT(tEncodeCStr(&encoder, oInfo->refColName));
      }
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

static void destroyHash(void* data){
  if (data){
    SSHashObj* tmp = *(SSHashObj**)data;
    tSimpleHashCleanup(tmp);
  }
}

static int32_t decodeSetTableMapInfo(SDecoder* decoder, SSHashObj** ppInfo) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t size = 0;
  TAOS_CHECK_EXIT(tDecodeI32(decoder, &size));
  *ppInfo = tSimpleHashInit(size, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (*ppInfo == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  tSimpleHashSetFreeFp(*ppInfo, destroyHash);
  
  for (int32_t i = 0; i < size; ++i) {
    int64_t id[2] = {0};
    TAOS_CHECK_EXIT(tDecodeI64(decoder, id));
    TAOS_CHECK_EXIT(tDecodeI64(decoder, id+1));
    int32_t len = 0;
    TAOS_CHECK_EXIT(tDecodeI32(decoder, &len));
    SSHashObj* tmp = tSimpleHashInit(len, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
    if (tmp == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tSimpleHashPut(*ppInfo, id, sizeof(id), &tmp, POINTER_BYTES));

    for (int32_t j = 0; j < len; ++j) {
      int16_t slotId = 0;
      int16_t cid = 0;
      TAOS_CHECK_EXIT(tDecodeI16(decoder, &slotId));
      TAOS_CHECK_EXIT(tDecodeI16(decoder, &cid));
      TAOS_CHECK_EXIT(tSimpleHashPut(tmp, &slotId, sizeof(slotId), &cid, sizeof(cid)));
    }
  }
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    tSimpleHashCleanup(*ppInfo);
    *ppInfo = NULL;
  }
  return code;
}

int32_t tDeserializeSTriggerPullRequest(void* buf, int32_t bufLen, SSTriggerPullRequestUnion* pReq) {
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
    case STRIGGER_PULL_SET_TABLE: {
      SSTriggerSetTableRequest* pRequest = &(pReq->setTableReq);
      TAOS_CHECK_EXIT(decodeSetTableMapInfo(&decoder, &pRequest->uidInfoTrigger));
      TAOS_CHECK_EXIT(decodeSetTableMapInfo(&decoder, &pRequest->uidInfoCalc));
      break;
    }
    case STRIGGER_PULL_LAST_TS: {
      break;
    }
    case STRIGGER_PULL_FIRST_TS: {
      SSTriggerFirstTsRequest* pRequest = &(pReq->firstTsReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->gid));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->startTime));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));
      break;
    }
    case STRIGGER_PULL_TSDB_META: {
      SSTriggerTsdbMetaRequest* pRequest = &(pReq->tsdbMetaReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->startTime));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->endTime));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->gid));
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pRequest->order));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));
      break;
    }
    case STRIGGER_PULL_TSDB_META_NEXT: {
      break;
    }
    case STRIGGER_PULL_TSDB_TS_DATA: {
      SSTriggerTsdbTsDataRequest* pRequest = &(pReq->tsdbTsDataReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->suid));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->uid));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->skey));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ekey));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));
      break;
    }
    case STRIGGER_PULL_TSDB_TRIGGER_DATA: {
      SSTriggerTsdbTriggerDataRequest* pRequest = &(pReq->tsdbTriggerDataReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->startTime));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->gid));
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pRequest->order));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));
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
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));
      break;
    }
    case STRIGGER_PULL_TSDB_CALC_DATA_NEXT: {
      break;
    }
    case STRIGGER_PULL_TSDB_DATA: {
      SSTriggerTsdbDataRequest* pRequest = &(pReq->tsdbDataReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->suid));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->uid));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->skey));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ekey));
      TAOS_CHECK_EXIT(decodeColsArray(&decoder, &pRequest->cids));
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pRequest->order));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));
      break;
    }
    case STRIGGER_PULL_TSDB_DATA_NEXT: {
      break;
    }
    case STRIGGER_PULL_WAL_META_NEW: {
      SSTriggerWalMetaNewRequest* pRequest = &(pReq->walMetaNewReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->lastVer));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ctime));
      break;
    }
    case STRIGGER_PULL_WAL_DATA_NEW:
    case STRIGGER_PULL_WAL_CALC_DATA_NEW: {
      SSTriggerWalDataNewRequest* pRequest = &(pReq->walDataNewReq);
      int32_t                     nVersion = 0;
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nVersion));
      pRequest->versions = taosArrayInit_s(sizeof(int64_t), nVersion);
      for (int32_t i = 0; i < nVersion; i++) {
        int64_t* pVer = TARRAY_GET_ELEM(pRequest->versions, i);
        TAOS_CHECK_EXIT(tDecodeI64(&decoder, pVer));
      }
      int32_t nRanges = 0;
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nRanges));
      pRequest->ranges = tSimpleHashInit(nRanges, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
      if (pRequest->ranges == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      for (int32_t i = 0; i < nRanges; i++) {
        uint64_t gid = 0;
        int64_t pRange[2] = {0};
        TAOS_CHECK_EXIT(tDecodeU64(&decoder, &gid));
        TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRange[0]));
        TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRange[1]));
        TAOS_CHECK_EXIT(tSimpleHashPut(pRequest->ranges, &gid, sizeof(gid), pRange, sizeof(pRange)));
      }
      break;
    }
    case STRIGGER_PULL_WAL_META_DATA_NEW: {
      SSTriggerWalMetaDataNewRequest* pRequest = &(pReq->walMetaDataNewReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->lastVer));
      break;
    }
    case STRIGGER_PULL_GROUP_COL_VALUE: {
      SSTriggerGroupColValueRequest* pRequest = &(pReq->groupColValueReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->gid));
      break;
    }
    case STRIGGER_PULL_VTABLE_INFO: {
      SSTriggerVirTableInfoRequest* pRequest = &(pReq->virTableInfoReq);
      TAOS_CHECK_EXIT(decodeColsArray(&decoder, &pRequest->cids));
      break;
    }
    case STRIGGER_PULL_VTABLE_PSEUDO_COL: {
      SSTriggerVirTablePseudoColRequest* pRequest = &(pReq->virTablePseudoColReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->uid));
      TAOS_CHECK_EXIT(decodeColsArray(&decoder, &pRequest->cids));
      break;
    }
    case STRIGGER_PULL_OTABLE_INFO: {
      SSTriggerOrigTableInfoRequest* pRequest = &(pReq->origTableInfoReq);
      int32_t size = 0;
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &size));
      pRequest->cols = taosArrayInit(size, sizeof(OTableInfo));
      if (pRequest->cols == NULL) {
        code = terrno;
        uError("failed to allocate memory for cids, size: %d, errno: %d", size, code);
        goto _exit;
      }
      for (int32_t i = 0; i < size; ++i) {
        OTableInfo* oInfo = taosArrayReserve(pRequest->cols, 1);
        if (oInfo == NULL) {
          code = terrno;
          uError("failed to reserve memory for OTableInfo, size: %d, errno: %d", size, code);
          goto _exit;
        }
        TAOS_CHECK_RETURN(tDecodeCStrTo(&decoder, oInfo->refTableName));
        TAOS_CHECK_RETURN(tDecodeCStrTo(&decoder, oInfo->refColName));
      }
      break;
    }
    default: {
      uError("unknown pull type %d", type);
      code = TSDB_CODE_INVALID_PARA;
      break;
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

static int32_t tSerializeSTriggerCalcParam(SEncoder* pEncoder, SArray* pParams, bool ignoreNotificationInfo, bool full) {
  int32_t size = full ? taosArrayGetSize(pParams) : 0;
  int32_t code = 0;
  int32_t lino = 0;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, size));
  for (int32_t i = 0; i < size; ++i) {
    SSTriggerCalcParam* param = taosArrayGet(pParams, i);
    if (param == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    int64_t plainFieldSize = offsetof(SSTriggerCalcParam, notifyType);
    if (pEncoder->data) {
      TAOS_MEMCPY(pEncoder->data + pEncoder->pos, param, plainFieldSize);
    }
    pEncoder->pos += plainFieldSize;

    if (!ignoreNotificationInfo) {
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, param->notifyType));
      uint64_t len = (param->extraNotifyContent != NULL) ? strlen(param->extraNotifyContent) + 1 : 0;
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, (uint8_t*)param->extraNotifyContent, len));
    }
  }
_exit:
  return code;
}

void tDestroySSTriggerCalcParam(void* ptr) {
  SSTriggerCalcParam* pParam = ptr;
  if (pParam && pParam->extraNotifyContent != NULL) {
    taosMemoryFreeClear(pParam->extraNotifyContent);
  }
  if (pParam && pParam->resultNotifyContent != NULL) {
    taosMemoryFreeClear(pParam->resultNotifyContent);
  }
}

void tDestroySStreamGroupValue(void* ptr) {
  SStreamGroupValue* pValue = ptr;
  if ((pValue != NULL) && (IS_VAR_DATA_TYPE(pValue->data.type) || pValue->data.type == TSDB_DATA_TYPE_DECIMAL)) {
    taosMemoryFreeClear(pValue->data.pData);
    pValue->data.nData = 0;
  }
}

static int32_t tDeserializeSTriggerCalcParam(SDecoder* pDecoder, SArray**ppParams, bool ignoreNotificationInfo) {
  int32_t size = 0, code = 0, lino = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
  *ppParams = taosArrayInit(size, sizeof(SSTriggerCalcParam));
  if (*ppParams == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < size; ++i) {
    SSTriggerCalcParam* param = taosArrayReserve(*ppParams, 1);
    if (param == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    int64_t plainFieldSize = offsetof(SSTriggerCalcParam, notifyType);
    TAOS_MEMCPY(param, pDecoder->data + pDecoder->pos, plainFieldSize);
    pDecoder->pos += plainFieldSize;

    if (!ignoreNotificationInfo) {
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &param->notifyType));
      uint64_t len = 0;
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&param->extraNotifyContent, &len));
    }
  }

_exit:
  return code;
}

static int32_t tSerializeStriggerGroupColVals(SEncoder* pEncoder, SArray* pGroupColVals, int32_t vgId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  int32_t size = taosArrayGetSize(pGroupColVals);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, size));
  for (int32_t i = 0; i < size; ++i) {
    SStreamGroupValue* pValue = taosArrayGet(pGroupColVals, i);
    if (pValue == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tEncodeBool(pEncoder, pValue->isNull));
    if (pValue->isNull) {
      continue;
    }
    TAOS_CHECK_EXIT(tEncodeBool(pEncoder, pValue->isTbname));
    if (pValue->isTbname) {
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pValue->uid));
      if (vgId != -1) { pValue->vgId = vgId; }
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pValue->vgId));
    }
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pValue->data.type));
    if (IS_VAR_DATA_TYPE(pValue->data.type)) {
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pValue->data.pData, pValue->data.nData));
    } else {
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pValue->data.val));
    }
  }

_exit:
  return code;
}

static int32_t tDeserializeStriggerGroupColVals(SDecoder* pDecoder, SArray** ppGroupColVals) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t size = 0;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
  taosArrayClearEx(*ppGroupColVals, tDestroySStreamGroupValue);
  if (size > 0) {
    if (*ppGroupColVals == NULL) {
      *ppGroupColVals = taosArrayInit(size, sizeof(SStreamGroupValue));
      if (*ppGroupColVals == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    } else {
      TAOS_CHECK_EXIT(taosArrayEnsureCap(*ppGroupColVals, size));
    }
  }
  for (int32_t i = 0; i < size; ++i) {
    SStreamGroupValue* pValue = taosArrayReserve(*ppGroupColVals, 1);
    if (pValue == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeBool(pDecoder, &pValue->isNull));
    if (pValue->isNull) {
      continue;
    }
    TAOS_CHECK_EXIT(tDecodeBool(pDecoder, &pValue->isTbname));
    if (pValue->isTbname) {
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pValue->uid));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pValue->vgId));
    }
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pValue->data.type));
    if (IS_VAR_DATA_TYPE(pValue->data.type)) {
      uint64_t len = 0;
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pValue->data.pData, &len));
      pValue->data.nData = len;
    } else {
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pValue->data.val));
    }
  }
_exit:
  return code;
}

int32_t tSerializeSStreamGroupInfo(void* buf, int32_t bufLen, const SStreamGroupInfo* gInfo, int32_t vgId) {
  SEncoder encoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  tlen = 0;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tSerializeStriggerGroupColVals(&encoder, gInfo->gInfo, vgId));

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

int32_t tDeserializeSStreamGroupInfo(void* buf, int32_t bufLen, SStreamGroupInfo* gInfo) {
  SDecoder decoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  size = 0;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDeserializeStriggerGroupColVals(&decoder, &gInfo->gInfo));

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
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->triggerType));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->gid));

  TAOS_CHECK_EXIT(tSerializeSTriggerCalcParam(&encoder, pReq->params, false, true));
  TAOS_CHECK_EXIT(tSerializeStriggerGroupColVals(&encoder, pReq->groupColVals, -1));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->createTable));
  TAOS_CHECK_EXIT(tEncodeBool(&encoder, pReq->isWindowTrigger));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->precision));

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

int32_t tDeserializeSTriggerCalcRequest(void* buf, int32_t bufLen, SSTriggerCalcRequest* pReq) {
  SDecoder decoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->runnerTaskId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->sessionId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->triggerType));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->gid));

  TAOS_CHECK_EXIT(tDeserializeSTriggerCalcParam(&decoder, &pReq->params, false));
  TAOS_CHECK_EXIT(tDeserializeStriggerGroupColVals(&decoder, &pReq->groupColVals));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->createTable));
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeBool(&decoder, &pReq->isWindowTrigger));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->precision));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tDestroySTriggerCalcRequest(SSTriggerCalcRequest* pReq) {
  if (pReq != NULL) {
    if (pReq->params != NULL) {
      taosArrayDestroyEx(pReq->params, tDestroySSTriggerCalcParam);
      pReq->params = NULL;
    }
    if (pReq->groupColVals != NULL) {
      taosArrayDestroyEx(pReq->groupColVals, tDestroySStreamGroupValue);
      pReq->groupColVals = NULL;
    }
    blockDataDestroy(pReq->pOutBlock);
  }
}

int32_t tSerializeSTriggerDropTableRequest(void* buf, int32_t bufLen, const SSTriggerDropRequest* pReq) {
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

  TAOS_CHECK_EXIT(tSerializeStriggerGroupColVals(&encoder, pReq->groupColVals, -1));

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

int32_t tDeserializeSTriggerDropTableRequest(void* buf, int32_t bufLen, SSTriggerDropRequest* pReq) {
  SDecoder decoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->runnerTaskId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->sessionId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->gid));

  TAOS_CHECK_EXIT(tDeserializeStriggerGroupColVals(&decoder, &pReq->groupColVals));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tDestroySSTriggerDropRequest(SSTriggerDropRequest* pReq) {
  if (pReq != NULL) {
    if (pReq->groupColVals != NULL) {
      taosArrayDestroyEx(pReq->groupColVals, tDestroySStreamGroupValue);
      pReq->groupColVals = NULL;
    }
  }
}

int32_t tSerializeSTriggerCtrlRequest(void* buf, int32_t bufLen, const SSTriggerCtrlRequest* pReq) {
  SEncoder encoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  tlen = 0;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->type));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->streamId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->sessionId));

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

int32_t tDeserializeSTriggerCtrlRequest(void* buf, int32_t bufLen, SSTriggerCtrlRequest* pReq) {
  SDecoder decoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t type = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &type));
  pReq->type = type;
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->sessionId));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeStRtFuncInfo(SEncoder* pEncoder, const SStreamRuntimeFuncInfo* pInfo, bool full) {
  int32_t code = 0, lino = 0;
  TAOS_CHECK_EXIT(tSerializeSTriggerCalcParam(pEncoder, pInfo->pStreamPesudoFuncVals, true, full));
  TAOS_CHECK_EXIT(tSerializeStriggerGroupColVals(pEncoder, pInfo->pStreamPartColVals, -1));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pInfo->curWindow.skey));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pInfo->curWindow.ekey));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pInfo->groupId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pInfo->curIdx));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pInfo->sessionId));
  TAOS_CHECK_EXIT(tEncodeBool(pEncoder, pInfo->withExternalWindow));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pInfo->triggerType));
  TAOS_CHECK_EXIT(tEncodeBool(pEncoder, pInfo->isWindowTrigger));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pInfo->precision));
_exit:
  return code;
}

int32_t tDeserializeStRtFuncInfo(SDecoder* pDecoder, SStreamRuntimeFuncInfo* pInfo) {
  int32_t code = 0, lino = 0;
  int32_t size = 0;
  TAOS_CHECK_EXIT(tDeserializeSTriggerCalcParam(pDecoder, &pInfo->pStreamPesudoFuncVals, true));
  TAOS_CHECK_EXIT(tDeserializeStriggerGroupColVals(pDecoder, &pInfo->pStreamPartColVals));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pInfo->curWindow.skey));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pInfo->curWindow.ekey));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pInfo->groupId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pInfo->curIdx));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pInfo->sessionId));
  TAOS_CHECK_EXIT(tDecodeBool(pDecoder, &pInfo->withExternalWindow));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pInfo->triggerType));
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_EXIT(tDecodeBool(pDecoder, &pInfo->isWindowTrigger));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pInfo->precision));
  }
_exit:
  return code;
}

void tDestroyStRtFuncInfo(SStreamRuntimeFuncInfo* pInfo){
  if (pInfo == NULL) return;
  if (pInfo->pStreamPesudoFuncVals != NULL) {
    taosArrayDestroyEx(pInfo->pStreamPesudoFuncVals, tDestroySSTriggerCalcParam);
    pInfo->pStreamPesudoFuncVals = NULL;
  }
  if (pInfo->pStreamPartColVals != NULL) {
    taosArrayDestroyEx(pInfo->pStreamPartColVals, tDestroySStreamGroupValue);
    pInfo->pStreamPartColVals = NULL;
  }
}

int32_t tSerializeSStreamMsgVTableInfo(void* buf, int32_t bufLen, const SStreamMsgVTableInfo* pRsp){
  SEncoder encoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  tlen = 0;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t size = taosArrayGetSize(pRsp->infos);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, size));
  for (int32_t i = 0; i < size; ++i) {
    VTableInfo* info = taosArrayGet(pRsp->infos, i);
    if (info == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, info->gId));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, info->uid));
    TAOS_CHECK_EXIT(tEncodeSColRefWrapper(&encoder, &info->cols));
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

int32_t tDeserializeSStreamMsgVTableInfo(void* buf, int32_t bufLen, SStreamMsgVTableInfo *vTableInfo){
  SDecoder decoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  size = 0;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &size));
  vTableInfo->infos = taosArrayInit(size, sizeof(VTableInfo));
  if (vTableInfo->infos == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < size; ++i) {
    VTableInfo* info = taosArrayReserve(vTableInfo->infos, 1);
    if (info == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &info->gId));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &info->uid));
    TAOS_CHECK_EXIT(tDecodeSColRefWrapperEx(&decoder, &info->cols, false));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}


void tDestroyVTableInfo(void *ptr) {
  if (NULL == ptr) {
    return;
  }
  VTableInfo* pTable = (VTableInfo*)ptr;
  taosMemoryFree(pTable->cols.pColRef);
}

void tDestroySStreamMsgVTableInfo(SStreamMsgVTableInfo *ptr) {
  if (ptr == NULL) return;
  taosArrayDestroyEx(ptr->infos, tDestroyVTableInfo);
  ptr->infos = NULL;
}

int32_t tSerializeSStreamTsResponse(void* buf, int32_t bufLen, const SStreamTsResponse* pRsp) {
  SEncoder encoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  tlen = 0;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->ver));
  int32_t size = taosArrayGetSize(pRsp->tsInfo);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, size));
  for (int32_t i = 0; i < size; ++i) {
    STsInfo* tsInfo = taosArrayGet(pRsp->tsInfo, i);
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, tsInfo->gId));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, tsInfo->ts));
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

int32_t tDeserializeSStreamTsResponse(void* buf, int32_t bufLen, void *pBlock) {
  SDecoder decoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  SSDataBlock *pResBlock = pBlock;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI64(&decoder, (int64_t*)&pResBlock->info.id.groupId));
  int32_t numOfCols = 2;
  if (pResBlock->pDataBlock == NULL) {
    pResBlock->pDataBlock = taosArrayInit_s(sizeof(SColumnInfoData), numOfCols);
    if (pResBlock->pDataBlock == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i< numOfCols; ++i) {
      SColumnInfoData *pColInfoData = taosArrayGet(pResBlock->pDataBlock, i);
      if (pColInfoData == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      pColInfoData->info.type = TSDB_DATA_TYPE_BIGINT;
      pColInfoData->info.bytes = sizeof(int64_t);
    }
  }
  int32_t numOfRows = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numOfRows));
  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pResBlock, numOfRows));
  for (int32_t i = 0; i < numOfRows; ++i) {
    for (int32_t j = 0; j < numOfCols; ++j) {
      SColumnInfoData *pColInfoData = taosArrayGet(pResBlock->pDataBlock, j);
      if (pColInfoData == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      int64_t value = 0;
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &value));
      colDataSetInt64(pColInfoData, i, &value);
    }
  }

  pResBlock->info.dataLoad = 1;
  pResBlock->info.rows = numOfRows;

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

static int32_t encodeData(SEncoder* encoder, void* pBlock, SSHashObj* indexHash) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t len = 0;
  if (encoder->data == NULL){
    len = blockGetEncodeSize(pBlock);
  } else {
    len = blockEncode(pBlock, (char*)(encoder->data + encoder->pos), encoder->size - encoder->pos, blockDataGetNumOfCols(pBlock));
    if (len < 0) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  encoder->pos += len;

  if (indexHash == NULL) {
    goto _exit;
  } 
  
  uint32_t pos = encoder->pos;
  encoder->pos += sizeof(uint32_t); // reserve space for tables
  int32_t tables = 0;
  
  void*   pe = NULL;
  int32_t iter = 0;
  while ((pe = tSimpleHashIterate(indexHash, pe, &iter)) != NULL) {
    SStreamWalDataSlice* pInfo = (SStreamWalDataSlice*)pe;
    if (pInfo->gId == -1){
      continue;
    }
    int64_t uid = *(int64_t*)(tSimpleHashGetKey(pe, NULL));
    TAOS_CHECK_EXIT(tEncodeI64(encoder, uid));
    TAOS_CHECK_EXIT(tEncodeU64(encoder, pInfo->gId));
    TAOS_CHECK_EXIT(tEncodeI32(encoder, pInfo->startRowIdx));
    TAOS_CHECK_EXIT(tEncodeI32(encoder, pInfo->numRows));
    tables++;
  }
  uint32_t tmpPos = encoder->pos;
  encoder->pos = pos;
  TAOS_CHECK_EXIT(tEncodeI32(encoder, tables));
  encoder->pos = tmpPos;
_exit:
  return code;
}
 
int32_t tSerializeSStreamWalDataResponse(void* buf, int32_t bufLen, SSTriggerWalNewRsp* rsp) {
  SEncoder encoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  tlen = 0;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  if (rsp->dataBlock != NULL && ((SSDataBlock*)rsp->dataBlock)->info.rows > 0) {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 1)); // has real data
    TAOS_CHECK_EXIT(encodeData(&encoder, rsp->dataBlock, rsp->indexHash));
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 0));  // no real data
  }

  if (rsp->metaBlock != NULL && ((SSDataBlock*)rsp->metaBlock)->info.rows > 0) {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 1)); // has metada
    TAOS_CHECK_EXIT(encodeData(&encoder, rsp->metaBlock, NULL));
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 0));  // no meta data
  }

  if (rsp->deleteBlock != NULL && ((SSDataBlock*)rsp->deleteBlock)->info.rows > 0) {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 1)); // has deletedata
    TAOS_CHECK_EXIT(encodeData(&encoder, rsp->deleteBlock, NULL));
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 0));  // no delete data
  }

  if (rsp->dropBlock != NULL && ((SSDataBlock*)rsp->dropBlock)->info.rows > 0) {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 1)); // has drop table data
    TAOS_CHECK_EXIT(encodeData(&encoder, rsp->dropBlock, NULL));
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 0));  // no drop table data
  }

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, rsp->ver));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, rsp->verTime));
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

int32_t tDeserializeSStreamWalDataResponse(void* buf, int32_t bufLen, SSTriggerWalNewRsp* pRsp, SArray* pSlices){
  SDecoder     decoder = {0};
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = NULL;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  // decode data block
  int8_t hasData = false;
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &hasData));
  pBlock = pRsp->dataBlock;
  if (hasData) {
    TAOS_CHECK_EXIT(pBlock != NULL ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA);
    const char* pEndPos = NULL;
    TAOS_CHECK_EXIT(blockDecode(pBlock, (char*)decoder.data + decoder.pos, &pEndPos));
    decoder.pos = (uint8_t*)pEndPos - decoder.data;

    int32_t nSlices = 0;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nSlices));
    TAOS_CHECK_EXIT(taosArrayEnsureCap(pSlices, nSlices));
    taosArrayClear(pSlices);
    int64_t  uid = 0;
    uint64_t gid = 0;
    int32_t  startIdx = 0;
    int32_t  numRows = 0;
    for (int32_t i = 0; i < nSlices; i++) {
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &uid));
      TAOS_CHECK_EXIT(tDecodeU64(&decoder, &gid));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &startIdx));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numRows));
      int32_t endIdx = startIdx + numRows;
      int64_t value[3] = {gid, uid, (int64_t)startIdx << 32 | endIdx};
      void*   px = taosArrayPush(pSlices, value);
      if (px == NULL) {
        code = terrno;
        goto _exit;
      }
    }
  } else if (pBlock != NULL) {
    blockDataEmpty(pBlock);
    taosArrayClear(pSlices);
  }

  int8_t hasMeta = false;
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &hasMeta));
  pBlock = pRsp->metaBlock;
  if (hasMeta) {
    TAOS_CHECK_EXIT(pBlock != NULL ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA);
    const char* pEndPos = NULL;
    TAOS_CHECK_EXIT(blockDecode(pBlock, (char*)decoder.data + decoder.pos, &pEndPos));
    decoder.pos = (uint8_t*)pEndPos - decoder.data;
  } else if (pBlock != NULL) {
    blockDataEmpty(pBlock);
  }

  int8_t hasDel = false;
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &hasDel));
  pBlock = pRsp->deleteBlock;
  if (hasDel) {
    TAOS_CHECK_EXIT(pBlock != NULL ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA);
    const char* pEndPos = NULL;
    TAOS_CHECK_EXIT(blockDecode(pBlock, (char*)decoder.data + decoder.pos, &pEndPos));
    decoder.pos = (uint8_t*)pEndPos - decoder.data;
  } else if (pBlock != NULL) {
    blockDataEmpty(pBlock);
  }

  int8_t hasDrop = false;
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &hasDrop));
  pBlock = pRsp->dropBlock;
  if (hasDrop) {
    TAOS_CHECK_EXIT(pBlock != NULL ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA);
    const char* pEndPos = NULL;
    TAOS_CHECK_EXIT(blockDecode(pBlock, (char*)decoder.data + decoder.pos, &pEndPos));
    decoder.pos = (uint8_t*)pEndPos - decoder.data;
  } else if (pBlock != NULL) {
    blockDataEmpty(pBlock);
  }
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->ver));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->verTime));

  tEndDecode(&decoder);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  tDecoderClear(&decoder);
  return code;
}

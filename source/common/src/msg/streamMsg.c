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
#include <limits.h>
#include "os.h"
#include "taos.h"
#include "tarray.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "thash.h"
#include "tlist.h"
#include "tmsg.h"
#include "tsimplehash.h"

void tCleanupSStreamExtTriggerSpec(SStreamExtTriggerSpec* pSpec) {
  if (pSpec == NULL) return;

  taosArrayDestroy(pSpec->triggerColumns);
  taosMemoryFree(pSpec->pColMappings);
  taosArrayDestroy(pSpec->calcColumns);
  taosMemoryFree(pSpec->pCalcMappings);
  taosMemoryFree(pSpec->prefilter);
  taosMemoryFree(pSpec->triggerPrefilter);
  taosArrayDestroy(pSpec->partitionTagCols);
  taosArrayDestroyP(pSpec->partitionTagExprs, NULL);
  pSpec->triggerColumns = NULL;
  pSpec->pColMappings = NULL;
  pSpec->calcColumns = NULL;
  pSpec->pCalcMappings = NULL;
  pSpec->prefilter = NULL;
  pSpec->triggerPrefilter = NULL;
  pSpec->partitionTagCols = NULL;
  pSpec->partitionTagExprs = NULL;
  pSpec->numColMappings = 0;
  pSpec->numCalcMappings = 0;
}

void tFreeSStreamExtTriggerSpec(SStreamExtTriggerSpec* pSpec) {
  if (pSpec == NULL) return;
  tCleanupSStreamExtTriggerSpec(pSpec);
  taosMemoryFree(pSpec);
}

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
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pStatus->recalcSessionNum));
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

#define STREAM_HB_METRICS_FIXED_PAYLOAD_SIZE \
  (sizeof(uint64_t) * 7 + sizeof(int8_t) * 2 + sizeof(int64_t) + sizeof(int32_t) * 2)
#define STREAM_HB_RECALC_WIRE_SIZE          (sizeof(int64_t) * 3 + sizeof(int32_t) * 2)
#define STREAM_HB_METRICS_ENTRY_HEADER_SIZE (sizeof(int32_t) * 2 + sizeof(int64_t) * 3)

static int32_t tEncodeStreamTaskMetricsPayload(SEncoder* pEncoder, const SStreamTaskMetricsEntry* pEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pEntry->snapshot.applicableMask));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pEntry->snapshot.validMask));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pEntry->snapshot.windowReady));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pEntry->snapshot.physicalInputRows1m));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pEntry->snapshot.logicalInputRows1m));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pEntry->snapshot.deliveredOutputRows1m));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pEntry->snapshot.resultLatencyUs1m));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pEntry->snapshot.resultLatencySamples1m));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pEntry->snapshot.realtimeLagMs));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pEntry->snapshot.historyProgressValid));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pEntry->snapshot.historyProgressPct));

  size_t recalcNum = taosArrayGetSize(pEntry->snapshot.pRecalculates);
  if (recalcNum > INT32_MAX) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_RANGE);
  }
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, (int32_t)recalcNum));
  for (int32_t i = 0; i < (int32_t)recalcNum; ++i) {
    SStreamRecalcSnapshot* pRecalc = taosArrayGet(pEntry->snapshot.pRecalculates, i);
    if (pRecalc == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRecalc->recalcId));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRecalc->start));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRecalc->end));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRecalc->progressPct));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRecalc->status));
  }

_exit:
  return code;
}

static int32_t tGetStreamTaskMetricsPayloadSize(const SStreamTaskMetricsEntry* pEntry, int32_t* pSize) {
  int32_t  code = 0;
  int32_t  lino = 0;
  size_t   recalcNum = taosArrayGetSize(pEntry->snapshot.pRecalculates);
  SEncoder encoder = {0};
  if (recalcNum > (INT32_MAX - STREAM_HB_METRICS_FIXED_PAYLOAD_SIZE) / STREAM_HB_RECALC_WIRE_SIZE) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_RANGE);
  }

  tEncoderInit(&encoder, NULL, 0);
  TAOS_CHECK_EXIT(tEncodeStreamTaskMetricsPayload(&encoder, pEntry));
  if (encoder.pos > INT32_MAX) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_RANGE);
  }
  *pSize = (int32_t)encoder.pos;

_exit:
  tEncoderClear(&encoder);
  return code;
}

static int32_t tEncodeStreamHbObservabilityTail(SEncoder* pEncoder, const SStreamHbMsg* pReq) {
  int32_t code = 0;
  int32_t lino = 0;
  size_t  entryNum = taosArrayGetSize(pReq->pTaskMetrics);
  if (entryNum > INT32_MAX) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_RANGE);
  }

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, (int32_t)entryNum));
  for (int32_t i = 0; i < (int32_t)entryNum; ++i) {
    SStreamTaskMetricsEntry* pEntry = taosArrayGet(pReq->pTaskMetrics, i);
    if (pEntry == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    int32_t entryLength = 0;
    TAOS_CHECK_EXIT(tGetStreamTaskMetricsPayloadSize(pEntry, &entryLength));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pEntry->taskStatusIndex));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pEntry->streamId));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pEntry->taskId));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pEntry->seriousId));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, entryLength));
    TAOS_CHECK_EXIT(tEncodeStreamTaskMetricsPayload(pEncoder, pEntry));
  }

_exit:
  return code;
}

static int32_t tGetStreamHbObservabilityTailSize(const SStreamHbMsg* pReq, int32_t* pSize) {
  int32_t  code = 0;
  int32_t  lino = 0;
  size_t   entryNum = taosArrayGetSize(pReq->pTaskMetrics);
  uint64_t totalSize = sizeof(int32_t);
  SEncoder encoder = {0};
  if (entryNum > INT32_MAX) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_RANGE);
  }

  for (int32_t i = 0; i < (int32_t)entryNum; ++i) {
    SStreamTaskMetricsEntry* pEntry = taosArrayGet(pReq->pTaskMetrics, i);
    if (pEntry == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    int32_t entryLength = 0;
    TAOS_CHECK_EXIT(tGetStreamTaskMetricsPayloadSize(pEntry, &entryLength));
    totalSize += STREAM_HB_METRICS_ENTRY_HEADER_SIZE + (uint32_t)entryLength;
    if (totalSize > INT32_MAX) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_RANGE);
    }
  }

  tEncoderInit(&encoder, NULL, 0);
  TAOS_CHECK_EXIT(tEncodeStreamHbObservabilityTail(&encoder, pReq));
  if (encoder.pos != totalSize || encoder.pos > INT32_MAX) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_RANGE);
  }
  *pSize = (int32_t)encoder.pos;

_exit:
  tEncoderClear(&encoder);
  return code;
}

static int32_t tDecodeStreamTaskMetricsPayload(SDecoder* pDecoder, SStreamTaskMetricsEntry* pEntry) {
  int32_t code = 0;
  int32_t lino = 0;
  int8_t  windowReady = 0;
  int8_t  historyProgressValid = 0;

  if (tDecodeU64(pDecoder, &pEntry->snapshot.applicableMask) != 0 ||
      tDecodeU64(pDecoder, &pEntry->snapshot.validMask) != 0 || tDecodeI8(pDecoder, &windowReady) != 0 ||
      tDecodeU64(pDecoder, &pEntry->snapshot.physicalInputRows1m) != 0 ||
      tDecodeU64(pDecoder, &pEntry->snapshot.logicalInputRows1m) != 0 ||
      tDecodeU64(pDecoder, &pEntry->snapshot.deliveredOutputRows1m) != 0 ||
      tDecodeU64(pDecoder, &pEntry->snapshot.resultLatencyUs1m) != 0 ||
      tDecodeU64(pDecoder, &pEntry->snapshot.resultLatencySamples1m) != 0 ||
      tDecodeI64(pDecoder, &pEntry->snapshot.realtimeLagMs) != 0 || tDecodeI8(pDecoder, &historyProgressValid) != 0 ||
      tDecodeI32(pDecoder, &pEntry->snapshot.historyProgressPct) != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
  pEntry->snapshot.windowReady = windowReady;
  pEntry->snapshot.historyProgressValid = historyProgressValid;

  int32_t recalcNum = 0;
  if (tDecodeI32(pDecoder, &recalcNum) != 0 || recalcNum < 0 ||
      (uint32_t)recalcNum > TD_CODER_REMAIN_CAPACITY(pDecoder) / STREAM_HB_RECALC_WIRE_SIZE) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
  if (recalcNum > 0) {
    pEntry->snapshot.pRecalculates = taosArrayInit(recalcNum, sizeof(SStreamRecalcSnapshot));
    if (pEntry->snapshot.pRecalculates == NULL) {
      code = terrno;
      goto _exit;
    }
  }

  for (int32_t i = 0; i < recalcNum; ++i) {
    SStreamRecalcSnapshot recalc = {0};
    int32_t               status = 0;
    if (tDecodeI64(pDecoder, &recalc.recalcId) != 0 || tDecodeI64(pDecoder, &recalc.start) != 0 ||
        tDecodeI64(pDecoder, &recalc.end) != 0 || tDecodeI32(pDecoder, &recalc.progressPct) != 0 ||
        tDecodeI32(pDecoder, &status) != 0) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    recalc.status = status;
    if (taosArrayPush(pEntry->snapshot.pRecalculates, &recalc) == NULL) {
      code = terrno;
      goto _exit;
    }
  }

_exit:
  return code;
}

static void tFreeSStreamTaskMetricsEntry(void* param) {
  SStreamTaskMetricsEntry* pEntry = param;
  if (pEntry == NULL) {
    return;
  }
  taosArrayDestroy(pEntry->snapshot.pRecalculates);
  pEntry->snapshot.pRecalculates = NULL;
}

static int32_t tDecodeStreamHbObservabilityTail(SDecoder* pDecoder, SStreamHbMsg* pReq) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t entryNum = 0;
  if (tDecodeI32(pDecoder, &entryNum) != 0 || entryNum < 0 ||
      (uint32_t)entryNum > TD_CODER_REMAIN_CAPACITY(pDecoder) / STREAM_HB_METRICS_ENTRY_HEADER_SIZE) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

  pReq->pTaskMetrics = taosArrayInit(entryNum, sizeof(SStreamTaskMetricsEntry));
  if (pReq->pTaskMetrics == NULL) {
    code = terrno;
    goto _exit;
  }

  for (int32_t i = 0; i < entryNum; ++i) {
    SStreamTaskMetricsEntry entry = {0};
    int32_t                 entryLength = 0;
    if (tDecodeI32(pDecoder, &entry.taskStatusIndex) != 0 || tDecodeI64(pDecoder, &entry.streamId) != 0 ||
        tDecodeI64(pDecoder, &entry.taskId) != 0 || tDecodeI64(pDecoder, &entry.seriousId) != 0 ||
        tDecodeI32(pDecoder, &entryLength) != 0 || entryLength < 0 ||
        (uint32_t)entryLength > TD_CODER_REMAIN_CAPACITY(pDecoder)) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }

    uint32_t entryEnd = pDecoder->pos + (uint32_t)entryLength;
    uint32_t tailEnd = pDecoder->size;
    pDecoder->size = entryEnd;
    entry.decodeCode = tDecodeStreamTaskMetricsPayload(pDecoder, &entry);
    pDecoder->size = tailEnd;
    pDecoder->pos = entryEnd;
    if (entry.decodeCode != TSDB_CODE_SUCCESS) {
      tFreeSStreamTaskMetricsEntry(&entry);
      memset(&entry.snapshot, 0, sizeof(entry.snapshot));
      if (entry.decodeCode != TSDB_CODE_INVALID_MSG) {
        code = entry.decodeCode;
        goto _exit;
      }
    }

    if (taosArrayPush(pReq->pTaskMetrics, &entry) == NULL) {
      tFreeSStreamTaskMetricsEntry(&entry);
      code = terrno;
      goto _exit;
    }
    entry.snapshot.pRecalculates = NULL;
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

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, statusNum));
  for (int32_t i = 0; i < statusNum; ++i) {
    SStmTaskStatusMsg* pStatus = taosArrayGet(pReq->pStreamStatus, i);
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pStatus->extraErrMsg));
  }

  if (pReq->observabilityVersion != 0) {
    if (pReq->observabilityVersion != STREAM_HB_OBSERVABILITY_VERSION_V1) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    int32_t tailLength = 0;
    TAOS_CHECK_EXIT(tGetStreamHbObservabilityTailSize(pReq, &tailLength));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->observabilityVersion));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, tailLength));
    TAOS_CHECK_EXIT(tEncodeStreamHbObservabilityTail(pEncoder, pReq));
  }

  if (pEncoder->pos > INT32_MAX) {
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _exit;
  }
  tEndEncode(pEncoder);
  if (pEncoder->pos > INT32_MAX) {
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _exit;
  }

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

  pReq->observabilityVersion = 0;
  pReq->pTaskMetrics = NULL;

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

  if (!tDecodeIsEnd(pDecoder)) {
    int32_t errMsgNum = 0;
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &errMsgNum));
    if (errMsgNum < 0 || errMsgNum > statusNum) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }

    for (int32_t i = 0; i < errMsgNum; ++i) {
      SStmTaskStatusMsg* pTask = taosArrayGet(pReq->pStreamStatus, i);
      if (NULL == pTask) {
        code = terrno;
        goto _exit;
      }
      TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &pTask->extraErrMsg));
    }
  }

  if (!tDecodeIsEnd(pDecoder)) {
    int32_t tailLength = 0;
    if (tDecodeI32(pDecoder, &pReq->observabilityVersion) != 0 || tDecodeI32(pDecoder, &tailLength) != 0 ||
        tailLength < 0 || (uint32_t)tailLength > TD_CODER_REMAIN_CAPACITY(pDecoder)) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }

    uint32_t tailEnd = pDecoder->pos + (uint32_t)tailLength;
    if (pReq->observabilityVersion == STREAM_HB_OBSERVABILITY_VERSION_V1) {
      uint32_t messageEnd = pDecoder->size;
      pDecoder->size = tailEnd;
      code = tDecodeStreamHbObservabilityTail(pDecoder, pReq);
      pDecoder->size = messageEnd;
      if (code != TSDB_CODE_SUCCESS) {
        goto _exit;
      }
    }
    pDecoder->pos = tailEnd;
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
  int32_t statusNum = taosArrayGetSize(pMsg->pStreamStatus);
  for (int32_t i = 0; i < statusNum; ++i) {
    SStmTaskStatusMsg* pTask = taosArrayGet(pMsg->pStreamStatus, i);
    if (pTask != NULL) {
      taosMemoryFreeClear(pTask->extraErrMsg);
    }
  }
  taosArrayDestroy(pMsg->pStreamReq);
  taosArrayDestroy(pMsg->pStreamStatus);
  taosArrayDestroyEx(pMsg->pTriggerStatus, tFreeSSTriggerRuntimeStatus);
  taosArrayDestroyEx(pMsg->pTaskMetrics, tFreeSStreamTaskMetricsEntry);
  pMsg->pVgLeaders = NULL;
  pMsg->pStreamReq = NULL;
  pMsg->pStreamStatus = NULL;
  pMsg->pTriggerStatus = NULL;
  pMsg->pTaskMetrics = NULL;
}

int32_t tEncodeSStreamReaderDeployFromTrigger(SEncoder* pEncoder, const SStreamReaderDeployFromTrigger* pMsg) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, (const uint8_t*)pMsg->triggerTblName, pMsg->triggerTblName == NULL ? 0 : (int32_t)strlen(pMsg->triggerTblName) + 1));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->triggerTblUid));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->triggerTblSuid));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->triggerTblType));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->isTriggerTblVirt));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->deleteReCalc));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->deleteOutTbl));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->partitionCols, pMsg->partitionCols == NULL ? 0 : (int32_t)strlen(pMsg->partitionCols) + 1));
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->rollupTagCols,
                                pMsg->rollupTagCols == NULL ? 0 : (int32_t)strlen(pMsg->rollupTagCols) + 1));
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
  /* Per-scan ext table identity (federated multi-source calc). */
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pMsg->extTable));
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pMsg->tsColumn));

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

  /* Encode pExtSpec for federated (ext-source) trigger readers.
   * A hasExtSpec flag (int8) distinguishes NULL from non-NULL.
   * When present, all SStreamExtTriggerSpec fields are written in order,
   * including encryptedPassword bytes (mnode fills these from sdb in P1 B2). */
  int8_t hasExtSpec = (pMsg->pExtSpec != NULL) ? 1 : 0;
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, hasExtSpec));
  if (hasExtSpec) {
    const SStreamExtTriggerSpec *pSpec = pMsg->pExtSpec;
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pSpec->sourceName));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pSpec->sourceType));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pSpec->extDb));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pSpec->extSchema));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pSpec->extTable));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pSpec->tsColumn));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pSpec->host));
    TAOS_CHECK_EXIT(tEncodeU16(pEncoder, pSpec->port));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pSpec->user));
    TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pSpec->encryptedPassword, sizeof(pSpec->encryptedPassword)));
    TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pSpec->connCfgVersion));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pSpec->options));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pSpec->partitionByTag));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pSpec->partitionByTbname));
    /* Encode prefilter (calc reader WHERE) and triggerPrefilter (trigger reader
     * PRE_FILTER) as nullable C strings: NULL encodes as an empty entry and the
     * length is carried by the string itself. */
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pSpec->prefilter));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pSpec->triggerPrefilter));
    /* partitionTagCols: one entry per PARTITION BY list item, in order. A
     * bare tag stores its name, a complete expression stores an empty name,
     * and a bare tbname stores the dedicated sentinel. The parallel
     * partitionTagExprs array carries every complete expression AST. */
    int32_t numPartTags =
        (pSpec->partitionTagCols != NULL) ? (int32_t)taosArrayGetSize(pSpec->partitionTagCols) : 0;
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, numPartTags));
    for (int32_t j = 0; j < numPartTags; ++j) {
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, (char*)taosArrayGet(pSpec->partitionTagCols, j)));
    }
    /* partitionTagExprs: parallel to partitionTagCols, same length whenever
     * partitionTagCols is non-empty (buildExtSpecs always allocates/frees
     * them together -- see SStreamExtTriggerSpec.partitionTagExprs in
     * streamMsg.h). SArray<char*>, same encode shape as pNotifyAddrUrls. */
    int32_t numPartExprs =
        (pSpec->partitionTagExprs != NULL) ? (int32_t)taosArrayGetSize(pSpec->partitionTagExprs) : 0;
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, numPartExprs));
    for (int32_t j = 0; j < numPartExprs; ++j) {
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, (char*)taosArrayGetP(pSpec->partitionTagExprs, j)));
    }
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
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->enableMultiGroupCalc));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->isTriggerTblVirt));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->triggerHasPF));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->isTriggerTblStb));
  int32_t partitionColsLen = pMsg->partitionCols == NULL ? 0 : (int32_t)strlen((char*)pMsg->partitionCols) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->partitionCols, partitionColsLen));
  int32_t len = pMsg->rollupTagCols == NULL ? 0 : (int32_t)strlen((char*)pMsg->rollupTagCols) + 1;
  TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMsg->rollupTagCols, len));

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
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->idleTimeoutMs));

  switch (pMsg->triggerType) {
    case WINDOW_TYPE_SESSION: {
      // session trigger
      TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pMsg->trigger.session.slotId));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.session.sessionVal));
      break;
    }
    case WINDOW_TYPE_STATE: {
      /*
       * state trigger – always v2 format:
       * I16(STATE_WIN_SLOT_SENTINEL_V2) + I32(slotNum)
       *   + N*I16(slotId) + ...
       */
      int32_t slotNum = pMsg->trigger.stateWin.pSlotIds == NULL ?
        0 : taosArrayGetSize(pMsg->trigger.stateWin.pSlotIds);
      TAOS_CHECK_EXIT(
        tEncodeI16(pEncoder, STATE_WIN_SLOT_SENTINEL_V2));
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, slotNum));
      for (int32_t i = 0; i < slotNum; ++i) {
        TAOS_CHECK_EXIT(tEncodeI16(
          pEncoder,
          *(int16_t*)taosArrayGet(
            pMsg->trigger.stateWin.pSlotIds, i)));
      }
      TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pMsg->trigger.stateWin.extend));
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->trigger.stateWin.trueForType));
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->trigger.stateWin.trueForCount));
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
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->trigger.event.trueForType));
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->trigger.event.trueForCount));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.event.trueForDuration));
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->trigger.event.startTrueForType));
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->trigger.event.startTrueForCount));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.event.startTrueForDuration));
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->trigger.event.endTrueForType));
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMsg->trigger.event.endTrueForCount));
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMsg->trigger.event.endTrueForDuration));
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
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->trigger.period.periodUnit));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->trigger.period.offsetUnit));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->trigger.period.precision));
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
  TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pMsg->calcPkSlotId));
  TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pMsg->triPkSlotId));
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
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pMsg->nodelayCreateSubtable));

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

  // colCids and tagCids - always encode size (0 if NULL) for compatibility
  int32_t colCidsSize = (int32_t)taosArrayGetSize(pMsg->colCids);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, colCidsSize));
  if (colCidsSize > 0) {
    for (int32_t i = 0; i < colCidsSize; ++i) {
      int16_t* pCid = (int16_t*)taosArrayGet(pMsg->colCids, i);
      TAOS_CHECK_EXIT(tEncodeI16(pEncoder, *pCid));
    }
  }

  int32_t tagCidsSize = (int32_t)taosArrayGetSize(pMsg->tagCids);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, tagCidsSize));
  if (tagCidsSize > 0) {
    for (int32_t i = 0; i < tagCidsSize; ++i) {
      int16_t* pCid = (int16_t*)taosArrayGet(pMsg->tagCids, i);
      TAOS_CHECK_EXIT(tEncodeI16(pEncoder, *pCid));
    }
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

int32_t tStartEncodeStreamTailFrame(SEncoder* pEncoder, uint32_t magic, uint16_t version, uint16_t flags) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(tEncodeU32(pEncoder, magic));
  TAOS_CHECK_EXIT(tEncodeU16(pEncoder, version));
  TAOS_CHECK_EXIT(tEncodeU16(pEncoder, flags));
  TAOS_CHECK_EXIT(tStartEncode(pEncoder));

_exit:
  return code;
}

void tEndEncodeStreamTailFrame(SEncoder* pEncoder) { tEndEncode(pEncoder); }

int32_t tDecodeNextStreamTailFrame(SDecoder* pParent, SStreamTailFrameDecoder* pFrame) {
  if (pParent == NULL || pFrame == NULL ||
      TD_CODER_REMAIN_CAPACITY(pParent) < sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2) {
    return TSDB_CODE_INVALID_MSG;
  }

  memset(pFrame, 0, sizeof(*pFrame));
  int32_t code = tDecodeU32(pParent, &pFrame->magic);
  if (code == TSDB_CODE_SUCCESS) code = tDecodeU16(pParent, &pFrame->version);
  if (code == TSDB_CODE_SUCCESS) code = tDecodeU16(pParent, &pFrame->flags);
  if (code == TSDB_CODE_SUCCESS) code = tDecodeU32(pParent, &pFrame->payloadLength);
  if (code != TSDB_CODE_SUCCESS || pFrame->payloadLength > INT32_MAX ||
      pFrame->payloadLength > TD_CODER_REMAIN_CAPACITY(pParent)) {
    return code == TSDB_CODE_SUCCESS ? TSDB_CODE_INVALID_MSG : code;
  }

  tDecoderInit(&pFrame->payloadDecoder, TD_CODER_CURRENT(pParent), pFrame->payloadLength);
  pParent->pos += pFrame->payloadLength;
  return TSDB_CODE_SUCCESS;
}

int32_t tFinishDecodeStreamTailFrame(SStreamTailFrameDecoder* pFrame, bool requirePayloadEnd) {
  if (pFrame == NULL) return TSDB_CODE_INVALID_PARA;
  const int32_t code =
      requirePayloadEnd && !tDecodeIsEnd(&pFrame->payloadDecoder) ? TSDB_CODE_INVALID_MSG : TSDB_CODE_SUCCESS;
  tDecoderClear(&pFrame->payloadDecoder);
  return code;
}

static int32_t tEncodeStreamHbWindowPlanFrame(SEncoder* pEncoder, const SMStreamHbRspMsg* pRsp) {
  int32_t       nestedNum = 0;
  const int32_t deployNum = taosArrayGetSize(pRsp->deploy.streamList);
  for (int32_t i = 0; i < deployNum; ++i) {
    const SStmStreamDeploy* pStream = taosArrayGet(pRsp->deploy.streamList, i);
    if (pStream->triggerTask == NULL) continue;
    const SStreamTriggerDeployMsg* pTrigger = &pStream->triggerTask->msg.trigger;
    const bool                     nested = BIT_FLAG_TEST_MASK(pTrigger->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN);
    if (nested != (pTrigger->pWindowPlan != NULL)) return TSDB_CODE_INVALID_PARA;
    if (nested) ++nestedNum;
  }
  if (nestedNum == 0) return TSDB_CODE_SUCCESS;

  int32_t code =
      tStartEncodeStreamTailFrame(pEncoder, STREAM_WINDOW_PLAN_FRAME_MAGIC, STREAM_WINDOW_PLAN_FRAME_VERSION, 0);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = tEncodeU32(pEncoder, (uint32_t)nestedNum);
  for (int32_t i = 0; code == TSDB_CODE_SUCCESS && i < deployNum; ++i) {
    const SStmStreamDeploy* pStream = taosArrayGet(pRsp->deploy.streamList, i);
    if (pStream->triggerTask == NULL) continue;
    const SStreamTriggerDeployMsg* pTrigger = &pStream->triggerTask->msg.trigger;
    if (!BIT_FLAG_TEST_MASK(pTrigger->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN)) continue;
    code = tEncodeI64(pEncoder, pStream->streamId);
    if (code == TSDB_CODE_SUCCESS) code = tEncodeI64(pEncoder, pStream->triggerTask->task.taskId);
    if (code == TSDB_CODE_SUCCESS) code = tEncodeStreamWindowPlan(pEncoder, pTrigger->pWindowPlan);
  }
  tEndEncodeStreamTailFrame(pEncoder);
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
  TAOS_CHECK_EXIT(tEncodeStreamHbWindowPlanFrame(pEncoder, pRsp));

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
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->isTriggerTblVirt));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->deleteReCalc));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->deleteOutTbl));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->partitionCols, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->rollupTagCols, NULL));
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
  /* Per-scan ext table identity (federated multi-source calc). */
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pMsg->extTable));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pMsg->tsColumn));

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

  /* Decode pExtSpec for federated (ext-source) trigger readers.
   * Mirrors tEncodeSStreamReaderDeployMsg encoding above. */
  int8_t hasExtSpec = 0;
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &hasExtSpec));
  if (hasExtSpec) {
    SStreamExtTriggerSpec *pSpec = (SStreamExtTriggerSpec *)taosMemoryCalloc(1, sizeof(SStreamExtTriggerSpec));
    if (pSpec == NULL) {
      code = terrno;
      lino = __LINE__;
      goto _exit;
    }
    /* Assign early so tFreeSStreamReaderDeployMsg can free pSpec on error. */
    pMsg->pExtSpec = pSpec;
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pSpec->sourceName));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pSpec->sourceType));
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pSpec->extDb));
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pSpec->extSchema));
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pSpec->extTable));
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pSpec->tsColumn));
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pSpec->host));
    TAOS_CHECK_EXIT(tDecodeU16(pDecoder, &pSpec->port));
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pSpec->user));
    {
      uint64_t binaryLen = 0;
      void *pBuf = NULL;
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, &pBuf, &binaryLen));
      if (binaryLen > sizeof(pSpec->encryptedPassword)) {
        taosMemoryFree(pBuf);
        code = TSDB_CODE_INVALID_MSG;
        lino = __LINE__;
        goto _exit;
      }
      if (binaryLen > 0 && pBuf != NULL) {
        memcpy(pSpec->encryptedPassword, pBuf, binaryLen);
      }
      taosMemoryFree(pBuf);
    }
    TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pSpec->connCfgVersion));
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pSpec->options));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pSpec->partitionByTag));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pSpec->partitionByTbname));
    /* Decode prefilter / triggerPrefilter as nullable, null-terminated C strings
     * (mirror of the tEncodeCStr calls above; empty entry decodes back to NULL). */
    TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &pSpec->prefilter));
    TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &pSpec->triggerPrefilter));
    /* partitionTagCols (mirror of encode above). */
    int32_t numPartTags = 0;
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &numPartTags));
    if (numPartTags > 0) {
      pSpec->partitionTagCols = taosArrayInit(numPartTags, TSDB_COL_NAME_LEN);
      if (pSpec->partitionTagCols == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      for (int32_t j = 0; j < numPartTags; ++j) {
        char colName[TSDB_COL_NAME_LEN] = {0};
        TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, colName));
        if (taosArrayPush(pSpec->partitionTagCols, colName) == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
      }
    }
    /* partitionTagExprs (mirror of encode above; same shape as pNotifyAddrUrls). */
    int32_t numPartExprs = 0;
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &numPartExprs));
    if (numPartExprs > 0) {
      pSpec->partitionTagExprs = taosArrayInit_s(POINTER_BYTES, numPartExprs);
      if (pSpec->partitionTagExprs == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      for (int32_t j = 0; j < numPartExprs; ++j) {
        char **pExpr = taosArrayGet(pSpec->partitionTagExprs, j);
        TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, pExpr));
      }
    }
  } else {
    pMsg->pExtSpec = NULL;
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
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->enableMultiGroupCalc));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->isTriggerTblVirt));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->triggerHasPF));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->isTriggerTblStb));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->partitionCols, NULL));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->rollupTagCols, NULL));

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
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->idleTimeoutMs));

  switch (pMsg->triggerType) {
    case WINDOW_TYPE_SESSION:
      // session trigger
      TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pMsg->trigger.session.slotId));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.session.sessionVal));
      break;
    case WINDOW_TYPE_STATE: {
      /*
        state trigger
        v1 format: single slotId as int16 (may be -1 for expression key)
        v2 format: first int16 is STATE_WIN_SLOT_SENTINEL_V2 (-2), then slotIds array
        decoder is compatible with v1/v2 (to support reading legacy or old-end data).
      */
      int16_t firstI16 = 0;
      TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &firstI16));
      if (firstI16 == STATE_WIN_SLOT_SENTINEL_V2) {
        // v2 format: sentinel + I32(slotNum) + N*I16(slotId)
        int32_t slotNum = 0;
        TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &slotNum));
        if (slotNum > 0) {
          pMsg->trigger.stateWin.pSlotIds = taosArrayInit(slotNum, sizeof(int16_t));
          TSDB_CHECK_NULL(pMsg->trigger.stateWin.pSlotIds, code, lino, _exit, terrno);
        }
        for (int32_t i = 0; i < slotNum; ++i) {
          int16_t slotId = -1;
          TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &slotId));
          TSDB_CHECK_NULL(taosArrayPush(pMsg->trigger.stateWin.pSlotIds, &slotId), code, lino, _exit, terrno);
        }
      } else {
        // v1 format: firstI16 is the single slotId (>= 0 for column, -1 for expr)
        pMsg->trigger.stateWin.pSlotIds = taosArrayInit(1, sizeof(int16_t));
        TSDB_CHECK_NULL(pMsg->trigger.stateWin.pSlotIds, code, lino, _exit, terrno);
        TSDB_CHECK_NULL(taosArrayPush(pMsg->trigger.stateWin.pSlotIds, &firstI16), code, lino, _exit, terrno);
      }
      TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pMsg->trigger.stateWin.extend));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->trigger.stateWin.trueForType));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->trigger.stateWin.trueForCount));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.stateWin.trueForDuration));
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->trigger.stateWin.zeroth, NULL));
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->trigger.stateWin.expr, NULL));
      break;
    }
    
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
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->trigger.event.trueForType));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->trigger.event.trueForCount));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.event.trueForDuration));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->trigger.event.startTrueForType));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->trigger.event.startTrueForCount));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.event.startTrueForDuration));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->trigger.event.endTrueForType));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pMsg->trigger.event.endTrueForCount));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.event.endTrueForDuration));
      break;
    
    case WINDOW_TYPE_COUNT:
      // count trigger
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pMsg->trigger.count.condCols, NULL));
      
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.count.countVal));
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pMsg->trigger.count.sliding));
      break;
    
    case WINDOW_TYPE_PERIOD:
      // period trigger
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, (int8_t*)&pMsg->trigger.period.periodUnit));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, (int8_t*)&pMsg->trigger.period.offsetUnit));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->trigger.period.precision));
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
  TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pMsg->calcPkSlotId));
  TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pMsg->triPkSlotId));
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
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pMsg->nodelayCreateSubtable));
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

  // colCids and tagCids - always decode size, create array only if size > 0
  // For backward compatibility, check if there's more data before decoding
  if (!tDecodeIsEnd(pDecoder)) {
    int32_t colCidsSize = 0;
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &colCidsSize));
    if (colCidsSize > 0 && colCidsSize <= TSDB_MAX_COLUMNS) {  // Sanity check
      pMsg->colCids = taosArrayInit(colCidsSize, sizeof(int16_t));
      TSDB_CHECK_NULL(pMsg->colCids, code, lino, _exit, terrno);
      for (int32_t i = 0; i < colCidsSize; ++i) {
        int16_t cid = 0;
        TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &cid));
        if (taosArrayPush(pMsg->colCids, &cid) == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
      }
    }
  }
  // Try to decode tagCids if there's more data
  if (!tDecodeIsEnd(pDecoder)) {
    int32_t tagCidsSize = 0;
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &tagCidsSize));
    if (tagCidsSize > 0 && tagCidsSize <= TSDB_MAX_TAGS) {  // Sanity check
      pMsg->tagCids = taosArrayInit(tagCidsSize, sizeof(int16_t));
      TSDB_CHECK_NULL(pMsg->tagCids, code, lino, _exit, terrno);
      for (int32_t i = 0; i < tagCidsSize; ++i) {
        int16_t cid = 0;
        TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &cid));
        if (taosArrayPush(pMsg->tagCids, &cid) == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
      }
    }
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
    taosMemoryFree(pMsg->rollupTagCols);
    taosMemoryFree(pMsg->triggerCols);
    taosMemoryFree(pMsg->triggerScanPlan);
    taosMemoryFree(pMsg->calcCacheScanPlan);
  } else {
    SStreamReaderDeployFromCalc* pMsg = (SStreamReaderDeployFromCalc*)&pReader->msg.calc;
    taosMemoryFree(pMsg->calcScanPlan);
  }

  /* Free ext spec allocated by tDecodeSStreamReaderDeployMsg for federated readers. */
  if (pReader->pExtSpec != NULL) {
    tFreeSStreamExtTriggerSpec(pReader->pExtSpec);
    pReader->pExtSpec = NULL;
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
      taosArrayDestroy(pTrigger->trigger.stateWin.pSlotIds);
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
  taosMemoryFree(pTrigger->rollupTagCols);
  taosMemoryFree(pTrigger->triggerPrevFilter);
  taosMemoryFree(pTrigger->triggerScanPlan);
  taosMemoryFree(pTrigger->calcCacheScanPlan);

  taosArrayDestroy(pTrigger->readerList);
  taosArrayDestroy(pTrigger->runnerList);
  taosMemoryFree(pTrigger->streamName);
  tDestroyStreamWindowPlan(&pTrigger->pWindowPlan);
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
  int32_t readerNum = taosArrayGetSize(pDeploy->readerTasks);
  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskDeploy* pReader = taosArrayGet(pDeploy->readerTasks, i);
    if (!pReader->msg.reader.triggerReader && pReader->msg.reader.msg.calc.freeScanPlan) {
      taosMemoryFreeClear(pReader->msg.reader.msg.calc.calcScanPlan);
    }
  }
  taosArrayDestroy(pDeploy->readerTasks);

  if (pDeploy->triggerTask) {
    taosArrayDestroy(pDeploy->triggerTask->msg.trigger.readerList);
    taosArrayDestroy(pDeploy->triggerTask->msg.trigger.runnerList);
    tDestroyStreamWindowPlan(&pDeploy->triggerTask->msg.trigger.pWindowPlan);
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

typedef struct {
  int64_t            streamId;
  int64_t            triggerTaskId;
  SStreamWindowPlan* pWindowPlan;
} SStreamHbWindowPlanEntry;

static void tFreeStreamHbWindowPlanEntry(void* pParam) {
  SStreamHbWindowPlanEntry* pEntry = pParam;
  tDestroyStreamWindowPlan(&pEntry->pWindowPlan);
}

static int32_t tCountStreamHbTriggerDeploys(const SMStreamHbRspMsg* pRsp) {
  int32_t       count = 0;
  const int32_t deployNum = taosArrayGetSize(pRsp->deploy.streamList);
  for (int32_t i = 0; i < deployNum; ++i) {
    const SStmStreamDeploy* pStream = taosArrayGet(pRsp->deploy.streamList, i);
    if (pStream->triggerTask != NULL) ++count;
  }
  return count;
}

static SStreamTriggerDeployMsg* tFindStreamHbTriggerDeploy(SMStreamHbRspMsg* pRsp, int64_t streamId,
                                                           int64_t triggerTaskId) {
  const int32_t deployNum = taosArrayGetSize(pRsp->deploy.streamList);
  for (int32_t i = 0; i < deployNum; ++i) {
    SStmStreamDeploy* pStream = taosArrayGet(pRsp->deploy.streamList, i);
    if (pStream->streamId == streamId && pStream->triggerTask != NULL &&
        pStream->triggerTask->task.taskId == triggerTaskId) {
      return &pStream->triggerTask->msg.trigger;
    }
  }
  return NULL;
}

static int32_t tDecodeStreamHbWindowPlanFrame(SStreamTailFrameDecoder* pFrame, SMStreamHbRspMsg* pRsp,
                                              SArray** ppEntries) {
  uint32_t      entryNum = 0;
  int32_t       code = tDecodeU32(&pFrame->payloadDecoder, &entryNum);
  const int32_t triggerNum = tCountStreamHbTriggerDeploys(pRsp);
  if (code != TSDB_CODE_SUCCESS || entryNum > (uint32_t)triggerNum) {
    return code == TSDB_CODE_SUCCESS ? TSDB_CODE_INVALID_MSG : code;
  }

  SArray* pEntries = taosArrayInit(entryNum, sizeof(SStreamHbWindowPlanEntry));
  if (pEntries == NULL) return terrno;
  for (uint32_t i = 0; i < entryNum; ++i) {
    SStreamHbWindowPlanEntry entry = {0};
    code = tDecodeI64(&pFrame->payloadDecoder, &entry.streamId);
    if (code == TSDB_CODE_SUCCESS) code = tDecodeI64(&pFrame->payloadDecoder, &entry.triggerTaskId);
    for (int32_t j = 0; code == TSDB_CODE_SUCCESS && j < taosArrayGetSize(pEntries); ++j) {
      const SStreamHbWindowPlanEntry* pPrior = taosArrayGet(pEntries, j);
      if (pPrior->streamId == entry.streamId && pPrior->triggerTaskId == entry.triggerTaskId) {
        code = TSDB_CODE_INVALID_MSG;
      }
    }
    SStreamTriggerDeployMsg* pTarget =
        code == TSDB_CODE_SUCCESS ? tFindStreamHbTriggerDeploy(pRsp, entry.streamId, entry.triggerTaskId) : NULL;
    if (code == TSDB_CODE_SUCCESS &&
        (pTarget == NULL || !BIT_FLAG_TEST_MASK(pTarget->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN))) {
      code = TSDB_CODE_INVALID_MSG;
    }
    if (code == TSDB_CODE_SUCCESS) code = tDecodeStreamWindowPlan(&pFrame->payloadDecoder, &entry.pWindowPlan);
    if (code == TSDB_CODE_SUCCESS && taosArrayPush(pEntries, &entry) == NULL) code = terrno;
    if (code != TSDB_CODE_SUCCESS) {
      tDestroyStreamWindowPlan(&entry.pWindowPlan);
      taosArrayDestroyEx(pEntries, tFreeStreamHbWindowPlanEntry);
      return code;
    }
  }
  *ppEntries = pEntries;
  return TSDB_CODE_SUCCESS;
}

static int32_t tBindStreamHbWindowPlans(SMStreamHbRspMsg* pRsp, SArray* pEntries) {
  const int32_t deployNum = taosArrayGetSize(pRsp->deploy.streamList);
  for (int32_t i = 0; i < deployNum; ++i) {
    SStmStreamDeploy* pStream = taosArrayGet(pRsp->deploy.streamList, i);
    if (pStream->triggerTask == NULL) continue;
    SStreamTriggerDeployMsg* pTrigger = &pStream->triggerTask->msg.trigger;
    const bool               required = BIT_FLAG_TEST_MASK(pTrigger->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN);
    if (required) {
      for (int32_t j = 0; j < i; ++j) {
        const SStmStreamDeploy* pPrior = taosArrayGet(pRsp->deploy.streamList, j);
        if (pPrior->triggerTask != NULL && pPrior->streamId == pStream->streamId &&
            pPrior->triggerTask->task.taskId == pStream->triggerTask->task.taskId &&
            BIT_FLAG_TEST_MASK(pPrior->triggerTask->msg.trigger.addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN)) {
          return TSDB_CODE_INVALID_MSG;
        }
      }
    }
    int32_t                  match = -1;
    for (int32_t j = 0; j < taosArrayGetSize(pEntries); ++j) {
      const SStreamHbWindowPlanEntry* pEntry = taosArrayGet(pEntries, j);
      if (pEntry->streamId == pStream->streamId && pEntry->triggerTaskId == pStream->triggerTask->task.taskId) {
        match = j;
        break;
      }
    }
    if (required != (match >= 0)) return TSDB_CODE_INVALID_MSG;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pEntries); ++i) {
    SStreamHbWindowPlanEntry* pEntry = taosArrayGet(pEntries, i);
    SStreamTriggerDeployMsg*  pTarget = tFindStreamHbTriggerDeploy(pRsp, pEntry->streamId, pEntry->triggerTaskId);
    TSWAP(pTarget->pWindowPlan, pEntry->pWindowPlan);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tDecodeStreamHbRsp(SDecoder* pDecoder, SMStreamHbRspMsg* pRsp) {
  int32_t code = 0;
  int32_t lino;
  bool    windowPlanFrameSeen = false;
  SArray* pWindowPlanEntries = NULL;

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

  while (!tDecodeIsEnd(pDecoder)) {
    SStreamTailFrameDecoder frame = {0};
    TAOS_CHECK_EXIT(tDecodeNextStreamTailFrame(pDecoder, &frame));
    if (frame.magic == STREAM_WINDOW_PLAN_FRAME_MAGIC) {
      if (windowPlanFrameSeen || frame.version != STREAM_WINDOW_PLAN_FRAME_VERSION || frame.flags != 0) {
        tFinishDecodeStreamTailFrame(&frame, false);
        TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
      }
      windowPlanFrameSeen = true;
      code = tDecodeStreamHbWindowPlanFrame(&frame, pRsp, &pWindowPlanEntries);
      if (code == TSDB_CODE_SUCCESS)
        code = tFinishDecodeStreamTailFrame(&frame, true);
      else
        tFinishDecodeStreamTailFrame(&frame, false);
      TAOS_CHECK_EXIT(code);
    } else {
      TAOS_CHECK_EXIT(tFinishDecodeStreamTailFrame(&frame, false));
    }
  }
  TAOS_CHECK_EXIT(tBindStreamHbWindowPlans(pRsp, pWindowPlanEntries));

  tEndDecode(pDecoder);

_exit:
  taosArrayDestroyEx(pWindowPlanEntries, tFreeStreamHbWindowPlanEntry);
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
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  char*   json = NULL;
  int32_t jsonLen = 0;
  TAOS_CHECK_EXIT(scmCreateStreamReqToJson(pReq, false, &json, &jsonLen));
  TAOS_CHECK_EXIT(tEncodeCStrWithLen(pEncoder, json, jsonLen));

_exit:
  taosMemoryFreeClear(json);
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

// Old version deserialization for backward compatibility,
// especially for stream version number 7
int32_t tDeserializeSCMCreateStreamReqImplOld(SDecoder *pDecoder, SCMCreateStreamReq *pReq, int32_t leftBytes) {
  int32_t code = 0;
  int32_t lino;
  pReq->calcPkSlotId = -1;
  pReq->triPkSlotId = -1;

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
        int16_t slotId = -1;
        TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &slotId));
        pReq->trigger.stateWin.pSlotIds = taosArrayInit(1, sizeof(int16_t));
        TSDB_CHECK_NULL(pReq->trigger.stateWin.pSlotIds, code, lino, _exit, terrno);
        TSDB_CHECK_NULL(taosArrayPush(pReq->trigger.stateWin.pSlotIds, &slotId), code, lino, _exit, terrno);
        pReq->trigger.stateWin.extend = 0;
        pReq->trigger.stateWin.trueForType = 0;
        pReq->trigger.stateWin.trueForCount = 0;
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
        pReq->trigger.event.trueForType = 0;
        pReq->trigger.event.trueForCount = 0;
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

  // LeftBytes is the size of all fields at the tail of SStreamObj.
  // If there are more data in the buffer, then it means
  // the new fields are added in SStreamObj, need to decode them.
  if (pDecoder->size - pDecoder->pos > leftBytes) {
    switch (pReq->triggerType) {
      case WINDOW_TYPE_STATE: {
        // state trigger
        if (!tDecodeIsEnd(pDecoder)) {
          TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, (void**)&pReq->trigger.stateWin.expr, NULL));
        }
        if (!tDecodeIsEnd(pDecoder)) {
          TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pReq->trigger.stateWin.extend));
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
  }

  if (pDecoder->size - pDecoder->pos > leftBytes) {
    if (!tDecodeIsEnd(pDecoder)) {
      TAOS_CHECK_EXIT(tDecodeU8(pDecoder, &pReq->triggerPrec));
    }
  }

_exit:

  return code;
}

// New deserialization using JSON
// start from taosd ver-3.3.8.6, stream version number 8
int32_t tDeserializeSCMCreateStreamReqImpl(SDecoder *pDecoder, SCMCreateStreamReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  char* json = NULL;
  SJson* pJson = NULL;
  TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &json));
  pJson = tjsonParse(json);
  if (pJson == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INVALID_JSON);
  }
  TAOS_CHECK_EXIT(jsonToSCMCreateStreamReq(pJson, pReq));

_exit:
  taosMemoryFreeClear(json);
  if (NULL != pJson) {
    tjsonDelete(pJson);
  }

  return code;
}


int32_t tDeserializeSCMCreateStreamReq(void *buf, int32_t bufLen, SCMCreateStreamReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  
  code = tDeserializeSCMCreateStreamReqImpl(&decoder, pReq);
  if (TSDB_CODE_MND_STREAM_INVALID_JSON == code) {
    uError("invalid json for stream create request, try old deserialization");
    // try old deserialization for backward compatibility
    tDecoderClear(&decoder);
    tDecoderInit(&decoder, buf, bufLen);
    TAOS_CHECK_EXIT(tStartDecode(&decoder));
    TAOS_CHECK_EXIT(tDeserializeSCMCreateStreamReqImplOld(&decoder, pReq, 0));
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

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->count));
  for (int32_t i = 0; i < pReq->count; i++) {
    int32_t nameLen = pReq->name[i] == NULL ? 0 : (int32_t)strlen(pReq->name[i]) + 1;
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->name[i], nameLen));
  }
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
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->count));
  if (pReq->count > 0) {
    pReq->name = taosMemoryCalloc(pReq->count, sizeof(char*));
    if (pReq->name == NULL) {
      code = terrno;
      goto _exit;
    }
    for (int32_t i = 0; i < pReq->count; i++) {
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void**)&pReq->name[i], NULL));
    }
  }
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeMDropStreamReq(SMDropStreamReq *pReq) {
  if (NULL == pReq) {
    return;
  }
  if (pReq->name) {
    for (int32_t i = 0; i < pReq->count; i++) {
      taosMemoryFreeClear(pReq->name[i]);
    }
    taosMemoryFreeClear(pReq->name);
  }
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
  taosMemoryFreeClear(pReq->rollupTagCols);

  taosArrayDestroy(pReq->outTags);
  pReq->outTags = NULL;
  taosArrayDestroy(pReq->outCols);
  pReq->outCols = NULL;

  switch (pReq->triggerType) {
    case WINDOW_TYPE_STATE:
      taosArrayDestroy(pReq->trigger.stateWin.pSlotIds);
      pReq->trigger.stateWin.pSlotIds = NULL;
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
  taosArrayDestroy(pReq->colCids);
  pReq->colCids = NULL;
  taosArrayDestroy(pReq->tagCids);
  pReq->tagCids = NULL;

  // Federated query: free extSpecs SArray (each spec owns triggerColumns +
  // prefilter heap). Pt A6 / P1 B6 will also free those inner allocations
  // when wire format is wired up; for now extSpecs entries hold only static
  // fixed-size fields so a simple element-free suffices.
  if (pReq->extSpecs != NULL) {
    int32_t n = (int32_t)taosArrayGetSize(pReq->extSpecs);
    for (int32_t i = 0; i < n; ++i) {
      SStreamExtTriggerSpec* pSpec = *(SStreamExtTriggerSpec**)taosArrayGet(pReq->extSpecs, i);
      if (pSpec == NULL) continue;
      tFreeSStreamExtTriggerSpec(pSpec);
    }
    taosArrayDestroy(pReq->extSpecs);
    pReq->extSpecs = NULL;
    pReq->numOfExtSpecs = 0;
  }
  tDestroyStreamWindowPlan(&pReq->pWindowPlan);
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

  if (pSrc->pWindowPlan != NULL) {
    TAOS_CHECK_EXIT(tCloneStreamWindowPlan(pSrc->pWindowPlan, &pDst->pWindowPlan));
  }

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

  if (pSrc->rollupTagCols) {
    pDst->rollupTagCols = COPY_STR(pSrc->rollupTagCols);
    TSDB_CHECK_NULL(pDst->rollupTagCols, code, lino, _exit, terrno);
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
      if (pSrc->trigger.stateWin.pSlotIds) {
        pDst->trigger.stateWin.pSlotIds = taosArrayDup(pSrc->trigger.stateWin.pSlotIds, NULL);
        TSDB_CHECK_NULL(pDst->trigger.stateWin.pSlotIds, code, lino, _exit, terrno);
      }
      pDst->trigger.stateWin.extend = pSrc->trigger.stateWin.extend;
      pDst->trigger.stateWin.trueForType = pSrc->trigger.stateWin.trueForType;
      pDst->trigger.stateWin.trueForCount = pSrc->trigger.stateWin.trueForCount;
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
      pDst->trigger.event.trueForType = pSrc->trigger.event.trueForType;
      pDst->trigger.event.trueForCount = pSrc->trigger.event.trueForCount;
      pDst->trigger.event.trueForDuration = pSrc->trigger.event.trueForDuration;
      pDst->trigger.event.startTrueForType = pSrc->trigger.event.startTrueForType;
      pDst->trigger.event.startTrueForCount = pSrc->trigger.event.startTrueForCount;
      pDst->trigger.event.startTrueForDuration = pSrc->trigger.event.startTrueForDuration;
      pDst->trigger.event.endTrueForType = pSrc->trigger.event.endTrueForType;
      pDst->trigger.event.endTrueForCount = pSrc->trigger.event.endTrueForCount;
      pDst->trigger.event.endTrueForDuration = pSrc->trigger.event.endTrueForDuration;
      break;
    case WINDOW_TYPE_COUNT:
      pDst->trigger.count.countVal = pSrc->trigger.count.countVal;
      pDst->trigger.count.sliding = pSrc->trigger.count.sliding;
      if (pSrc->trigger.count.condCols) {
        pDst->trigger.count.condCols = COPY_STR(pSrc->trigger.count.condCols);
        TSDB_CHECK_NULL(pDst->trigger.count.condCols, code, lino, _exit, terrno);
      }
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

      /* Per-scan ext source identity (fixed-size arrays; federated multi-source calc). */
      tstrncpy(dscan.sourceName, sscan->sourceName, sizeof(dscan.sourceName));
      tstrncpy(dscan.extTable, sscan->extTable, sizeof(dscan.extTable));
      tstrncpy(dscan.tsColumn, sscan->tsColumn, sizeof(dscan.tsColumn));
      
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

  if (pSrc->colCids) {
    pDst->colCids = taosArrayDup(pSrc->colCids, NULL);
    TSDB_CHECK_NULL(pDst->colCids, code, lino, _exit, terrno);
  }

  if (pSrc->tagCids) {
    pDst->tagCids = taosArrayDup(pSrc->tagCids, NULL);
    TSDB_CHECK_NULL(pDst->tagCids, code, lino, _exit, terrno);
  }

  pDst->triggerTblUid = pSrc->triggerTblUid;
  pDst->triggerTblSuid = pSrc->triggerTblSuid;
  pDst->triggerTblType = pSrc->triggerTblType;
  pDst->triggerPrec = pSrc->triggerPrec;
  pDst->deleteReCalc = pSrc->deleteReCalc;
  pDst->deleteOutTbl = pSrc->deleteOutTbl;
  pDst->flags = pSrc->flags;
  
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
    if (pRequest->uids != NULL) {
      taosArrayDestroy(pRequest->uids);
      pRequest->uids = NULL;
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
  } else if (pReq->base.type == STRIGGER_PULL_LAST_TS_EXT ||
             pReq->base.type == STRIGGER_PULL_META_EXT ||
             pReq->base.type == STRIGGER_PULL_DATA_EXT ||
             pReq->base.type == STRIGGER_PULL_META_DATA_EXT ||
             pReq->base.type == STRIGGER_PULL_CALC_DATA_EXT ||
             pReq->base.type == STRIGGER_PULL_GROUP_COL_VALUE_EXT) {
    SSTriggerExtPullReq* pExtReq = (SSTriggerExtPullReq*)pReq;
    tSimpleHashCleanup(pExtReq->pUidMaxTs);
    pExtReq->pUidMaxTs = NULL;
    tSimpleHashCleanup(pExtReq->pUidWindow);
    pExtReq->pUidWindow = NULL;
  }
  pReq->base.progressStepId = 0;
  pReq->base.progressRequestToken = 0;
}

int32_t encodePlainArray(SEncoder *encoder, SArray *pArr) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  nEle = taosArrayGetSize(pArr);
  uint8_t* buf = (nEle > 0) ? TARRAY_DATA(pArr) : NULL;
  int32_t  len = (nEle > 0) ? (nEle * pArr->elemSize) : 0;
  TAOS_CHECK_EXIT(tEncodeBinary(encoder, buf, len));

_exit:
  return code;
}

int32_t decodePlainArray(SDecoder* decoder, SArray** ppArr, uint32_t elemSize) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  void*    buf = NULL;
  uint64_t len = 0;
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(decoder, &buf, &len));

  if (len > 0) {
    *ppArr = taosArrayInit(0, elemSize);
    TSDB_CHECK_NULL(*ppArr, code, lino, _exit, terrno);
    TSWAP((*ppArr)->pData, buf);
    (*ppArr)->size = (*ppArr)->capacity = len / elemSize;
  }

_exit:
  if (buf != NULL) {
    taosMemoryFree(buf);
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
      TAOS_CHECK_EXIT(encodePlainArray(&encoder, pRequest->cids));
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
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->endVer));
      break;
    }
    case STRIGGER_PULL_GROUP_COL_VALUE: {
      SSTriggerGroupColValueRequest* pRequest = (SSTriggerGroupColValueRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->gid));
      break;
    }
    case STRIGGER_PULL_VTABLE_INFO: {
      SSTriggerVirTableInfoRequest* pRequest = (SSTriggerVirTableInfoRequest*)pReq;
      TAOS_CHECK_EXIT(encodePlainArray(&encoder, pRequest->cids));
      TAOS_CHECK_EXIT(encodePlainArray(&encoder, pRequest->uids));
      TAOS_CHECK_EXIT(tEncodeBool(&encoder, pRequest->fetchAllTable));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
      break;
    }
    case STRIGGER_PULL_VTABLE_PSEUDO_COL: {
      SSTriggerVirTablePseudoColRequest* pRequest = (SSTriggerVirTablePseudoColRequest*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->uid));
      TAOS_CHECK_EXIT(encodePlainArray(&encoder, pRequest->cids));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
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
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRequest->ver));
      break;
    }
    case STRIGGER_PULL_LAST_TS_EXT:
    case STRIGGER_PULL_META_EXT:
    case STRIGGER_PULL_DATA_EXT:
    case STRIGGER_PULL_META_DATA_EXT:
    case STRIGGER_PULL_CALC_DATA_EXT:
    case STRIGGER_PULL_GROUP_COL_VALUE_EXT: {
      SSTriggerExtPullReq* pExtReq = (SSTriggerExtPullReq*)pReq;
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, pExtReq->gid));
      /* encode pUidMaxTs: count + (uid, maxTs) pairs */
      int32_t nMaxTs = (pExtReq->pUidMaxTs != NULL) ? tSimpleHashGetSize(pExtReq->pUidMaxTs) : 0;
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, nMaxTs));
      if (nMaxTs > 0) {
        int32_t iter = 0;
        void*   px = tSimpleHashIterate(pExtReq->pUidMaxTs, NULL, &iter);
        while (px != NULL) {
          int64_t* uid   = tSimpleHashGetKey(px, NULL);
          int64_t  maxTs = *(int64_t*)px;
          TAOS_CHECK_EXIT(tEncodeI64(&encoder, *uid));
          TAOS_CHECK_EXIT(tEncodeI64(&encoder, maxTs));
          px = tSimpleHashIterate(pExtReq->pUidMaxTs, px, &iter);
        }
      }
      /* encode pUidWindow: count + (uid, skey, ekey) triples */
      int32_t nWin = (pExtReq->pUidWindow != NULL) ? tSimpleHashGetSize(pExtReq->pUidWindow) : 0;
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, nWin));
      if (nWin > 0) {
        int32_t iter = 0;
        void*   px = tSimpleHashIterate(pExtReq->pUidWindow, NULL, &iter);
        while (px != NULL) {
          int64_t*       uid = tSimpleHashGetKey(px, NULL);
          SExtUidWindow* win = (SExtUidWindow*)px;
          TAOS_CHECK_EXIT(tEncodeI64(&encoder, *uid));
          TAOS_CHECK_EXIT(tEncodeI64(&encoder, win->skey));
          TAOS_CHECK_EXIT(tEncodeI64(&encoder, win->ekey));
          px = tSimpleHashIterate(pExtReq->pUidWindow, px, &iter);
        }
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
  SSTriggerPullRequest* pBase = &(pReq->base);
  pBase->progressStepId = 0;
  pBase->progressRequestToken = 0;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t type = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &type));
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
      TAOS_CHECK_EXIT(decodePlainArray(&decoder, &pRequest->cids, sizeof(col_id_t)));
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
      pRequest->endVer = 0;
      if (!tDecodeIsEnd(&decoder)) {
        TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->endVer));
      }
      break;
    }
    case STRIGGER_PULL_GROUP_COL_VALUE: {
      SSTriggerGroupColValueRequest* pRequest = &(pReq->groupColValueReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->gid));
      break;
    }
    case STRIGGER_PULL_VTABLE_INFO: {
      SSTriggerVirTableInfoRequest* pRequest = &(pReq->virTableInfoReq);
      TAOS_CHECK_EXIT(decodePlainArray(&decoder, &pRequest->cids, sizeof(col_id_t)));
      TAOS_CHECK_EXIT(decodePlainArray(&decoder, &pRequest->uids, sizeof(int64_t)));
      TAOS_CHECK_EXIT(tDecodeBool(&decoder, &pRequest->fetchAllTable));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));
      break;
    }
    case STRIGGER_PULL_VTABLE_PSEUDO_COL: {
      SSTriggerVirTablePseudoColRequest* pRequest = &(pReq->virTablePseudoColReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->uid));
      TAOS_CHECK_EXIT(decodePlainArray(&decoder, &pRequest->cids, sizeof(col_id_t)));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));
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
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRequest->ver));

      break;
    }
    case STRIGGER_PULL_LAST_TS_EXT:
    case STRIGGER_PULL_META_EXT:
    case STRIGGER_PULL_DATA_EXT:
    case STRIGGER_PULL_META_DATA_EXT:
    case STRIGGER_PULL_CALC_DATA_EXT:
    case STRIGGER_PULL_GROUP_COL_VALUE_EXT: {
      SSTriggerExtPullReq* pExtReq = &(pReq->extPullReq);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pExtReq->gid));
      /* decode pUidMaxTs */
      int32_t nMaxTs = 0;
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nMaxTs));
      if (nMaxTs > 0) {
        pExtReq->pUidMaxTs = tSimpleHashInit(nMaxTs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
        if (pExtReq->pUidMaxTs == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
        for (int32_t i = 0; i < nMaxTs; i++) {
          int64_t uid = 0, maxTs = 0;
          TAOS_CHECK_EXIT(tDecodeI64(&decoder, &uid));
          TAOS_CHECK_EXIT(tDecodeI64(&decoder, &maxTs));
          TAOS_CHECK_EXIT(tSimpleHashPut(pExtReq->pUidMaxTs, &uid, sizeof(uid), &maxTs, sizeof(maxTs)));
        }
      }
      /* decode pUidWindow */
      int32_t nWin = 0;
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nWin));
      if (nWin > 0) {
        pExtReq->pUidWindow = tSimpleHashInit(nWin, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
        if (pExtReq->pUidWindow == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
        for (int32_t i = 0; i < nWin; i++) {
          int64_t uid = 0;
          SExtUidWindow win = {0};
          TAOS_CHECK_EXIT(tDecodeI64(&decoder, &uid));
          TAOS_CHECK_EXIT(tDecodeI64(&decoder, &win.skey));
          TAOS_CHECK_EXIT(tDecodeI64(&decoder, &win.ekey));
          TAOS_CHECK_EXIT(tSimpleHashPut(pExtReq->pUidWindow, &uid, sizeof(uid), &win, sizeof(win)));
        }
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
  /* On a partial-decode failure of an EXT pull, free the uid hashes that were
   * already allocated — callers leave reqDecoded=false and do not destroy pReq
   * in that case, so they would otherwise leak.  tSimpleHashCleanup + NULL is
   * idempotent, so this is safe even if a caller also destroys pReq. */
  if (code != TSDB_CODE_SUCCESS &&
      (type == STRIGGER_PULL_LAST_TS_EXT || type == STRIGGER_PULL_META_EXT ||
       type == STRIGGER_PULL_DATA_EXT || type == STRIGGER_PULL_META_DATA_EXT ||
       type == STRIGGER_PULL_CALC_DATA_EXT || type == STRIGGER_PULL_GROUP_COL_VALUE_EXT)) {
    SSTriggerExtPullReq* pExtReq = &(pReq->extPullReq);
    tSimpleHashCleanup(pExtReq->pUidMaxTs);
    pExtReq->pUidMaxTs = NULL;
    tSimpleHashCleanup(pExtReq->pUidWindow);
    pExtReq->pUidWindow = NULL;
  }
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
  if (pParam && pParam->pExternalWindowData != NULL) {
    taosArrayDestroyEx(pParam->pExternalWindowData, tDestroySStreamGroupValue);
    pParam->pExternalWindowData = NULL;
  }
}

void tDestroySSTriggerGroupCalcInfo(void* ptr) {
  SSTriggerGroupCalcInfo* pCalcInfo = ptr;
  if (pCalcInfo && pCalcInfo->pParams != NULL) {
    taosArrayDestroyEx(pCalcInfo->pParams, tDestroySSTriggerCalcParam);
    pCalcInfo->pParams = NULL;
  }
  if (pCalcInfo && pCalcInfo->pGroupColVals != NULL) {
    taosArrayDestroyEx(pCalcInfo->pGroupColVals, tDestroySStreamGroupValue);
    pCalcInfo->pGroupColVals = NULL;
  }
}

void tDestroySSTriggerGroupReadInfo(void* ptr) {
  SSTriggerGroupReadInfo* pReadInfo = ptr;
  if (pReadInfo && pReadInfo->pTables) {
    taosArrayDestroy(pReadInfo->pTables);
    pReadInfo->pTables = NULL;
  }
}

void tDestroySSTriggerGroupReadInfoArray(void* ptr) {
  if (ptr != NULL && *(SArray**)ptr != NULL) {
    SArray* pArray = *(SArray**)ptr;
    taosArrayDestroyEx(pArray, tDestroySSTriggerGroupReadInfo);
    *(SArray**)ptr = NULL;
  }
}

void tDestroySStreamGroupValue(void* ptr) {
  SStreamGroupValue* pValue = ptr;
  if ((pValue != NULL) && (IS_VAR_DATA_TYPE(pValue->data.type) || pValue->data.type == TSDB_DATA_TYPE_DECIMAL)) {
    taosMemoryFreeClear(pValue->data.pData);
    pValue->data.nData = 0;
  }
}

int32_t tGetStreamRollupGroupLeaf(const SStreamGroupValue* pValue, const char** ppLeaf, int32_t* pLeafLen) {
  if (pValue == NULL || ppLeaf == NULL || pLeafLen == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppLeaf = NULL;
  *pLeafLen = 0;
  if (pValue->isNull) {
    return TSDB_CODE_SUCCESS;
  }
  if (pValue->data.pData == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pValue->data.type == TSDB_DATA_TYPE_NCHAR) {
    if (pValue->data.nData % TSDB_NCHAR_SIZE != 0) {
      return TSDB_CODE_INVALID_MSG;
    }

    int32_t       start = 0;
    int32_t       numOfChars = pValue->data.nData / TSDB_NCHAR_SIZE;
    const TdUcs4* pUcs4 = (const TdUcs4*)pValue->data.pData;
    for (int32_t i = 0; i < numOfChars; ++i) {
      if (pUcs4[i] == '.') {
        start = i + 1;
      }
    }

    *ppLeaf = (const char*)pValue->data.pData + start * TSDB_NCHAR_SIZE;
    *pLeafLen = pValue->data.nData - start * TSDB_NCHAR_SIZE;
    return TSDB_CODE_SUCCESS;
  }

  if (pValue->data.type == TSDB_DATA_TYPE_VARCHAR || pValue->data.type == TSDB_DATA_TYPE_BINARY) {
    int32_t start = 0;
    for (int32_t i = 0; i < pValue->data.nData; ++i) {
      if (((const char*)pValue->data.pData)[i] == '.') {
        start = i + 1;
      }
    }

    *ppLeaf = (const char*)pValue->data.pData + start;
    *pLeafLen = pValue->data.nData - start;
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_INVALID_MSG;
}

static int32_t tDeserializeSTriggerCalcParam(SDecoder* pDecoder, SArray**ppParams, bool ignoreNotificationInfo) {
  int32_t size = 0, code = 0, lino = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
  if (size <= 0) {
    return code;
  }
  
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
    if (IS_VAR_DATA_TYPE(pValue->data.type) || pValue->data.type == TSDB_DATA_TYPE_DECIMAL) {
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
    if (IS_VAR_DATA_TYPE(pValue->data.type) || pValue->data.type == TSDB_DATA_TYPE_DECIMAL) {
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

static int32_t tSerializeSSTriggerGroupCalcInfo(SEncoder* pEncoder, SSTriggerGroupCalcInfo* pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(tSerializeSTriggerCalcParam(pEncoder, pInfo->pParams, false, true));
  TAOS_CHECK_EXIT(tSerializeStriggerGroupColVals(pEncoder, pInfo->pGroupColVals, -1));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pInfo->createTable));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pInfo->rollupTbCount));

_exit:
  return code;
}

static int32_t tSerializeSSTriggerGroupReadInfo(SEncoder* pEncoder, SSTriggerGroupReadInfo* pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pInfo->gid));
  int64_t plainFieldSize = offsetof(SSTriggerCalcParam, notifyType);
  if (pEncoder->data) {
    TAOS_MEMCPY(pEncoder->data + pEncoder->pos, &pInfo->firstParam, plainFieldSize);
  }
  pEncoder->pos += plainFieldSize;
  if (pEncoder->data) {
    TAOS_MEMCPY(pEncoder->data + pEncoder->pos, &pInfo->lastParam, plainFieldSize);
  }
  pEncoder->pos += plainFieldSize;

  int32_t nTables = taosArrayGetSize(pInfo->pTables);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, nTables));
  if (pEncoder->data && nTables > 0) {
    TAOS_MEMCPY(pEncoder->data + pEncoder->pos, pInfo->pTables->pData, nTables * sizeof(int64_t));
  }
  pEncoder->pos += nTables * sizeof(int64_t);

_exit:
  return code;
}

static bool tIsAncestorWindowType(int8_t type) {
  return type == WINDOW_TYPE_INTERVAL || type == WINDOW_TYPE_SESSION || type == WINDOW_TYPE_STATE ||
         type == WINDOW_TYPE_EVENT || type == WINDOW_TYPE_COUNT;
}

static void tDestroyWindowLineage(SWindowLineage* pLineage) {
  if (pLineage == NULL) return;
  taosArrayDestroy(pLineage->pScopes);
  pLineage->pScopes = NULL;
}

static void tDestroyAncestorParamContext(void* pValue) {
  SStreamAncestorParamContext* pContext = pValue;
  if (pContext == NULL) return;
  tDestroyWindowLineage(&pContext->leafIdentity.lineage);
  taosArrayDestroy(pContext->pSnapshots);
  pContext->pSnapshots = NULL;
}

static void tDestroyReadScopeBinding(void* pValue) {
  SStreamReadScopeBinding* pBinding = pValue;
  if (pBinding == NULL) return;
  tDestroyWindowLineage(&pBinding->scope.lineage);
}

void tDestroyStreamAncestorContext(SStreamAncestorContext** ppContext) {
  if (ppContext == NULL || *ppContext == NULL) return;
  SStreamAncestorContext* pContext = *ppContext;
  taosArrayDestroyEx(pContext->pParamContexts, tDestroyAncestorParamContext);
  taosArrayDestroyEx(pContext->pReadScopeBindings, tDestroyReadScopeBinding);
  taosMemoryFree(pContext);
  *ppContext = NULL;
}

void tDestroyStreamContextPolicy(SStreamContextPolicy** ppPolicy) {
  if (ppPolicy == NULL || *ppPolicy == NULL) return;
  SStreamContextPolicy* pPolicy = *ppPolicy;
  taosArrayDestroy(pPolicy->pEntries);
  taosMemoryFree(pPolicy);
  *ppPolicy = NULL;
}

static int32_t tCompareStreamContextPolicyKey(const SStreamContextPolicyEntry* pLeft,
                                              const SStreamContextPolicyEntry* pRight) {
  if (pLeft->gid < pRight->gid) return -1;
  if (pLeft->gid > pRight->gid) return 1;
  if (pLeft->paramIndex < pRight->paramIndex) return -1;
  if (pLeft->paramIndex > pRight->paramIndex) return 1;
  return 0;
}

static int32_t tValidateStreamContextPolicy(const SStreamContextPolicy* pPolicy) {
  if (pPolicy == NULL) return TSDB_CODE_INVALID_PARA;
  if (pPolicy->pEntries != NULL && pPolicy->pEntries->elemSize != sizeof(SStreamContextPolicyEntry)) {
    return TSDB_CODE_INVALID_PARA;
  }

  const int32_t count = taosArrayGetSize(pPolicy->pEntries);
  if (count > 1024 * 1024) return TSDB_CODE_INVALID_PARA;
  const SStreamContextPolicyEntry* pPrior = NULL;
  for (int32_t i = 0; i < count; ++i) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pPolicy->pEntries, i);
    if (pEntry == NULL || pEntry->paramIndex < 0 ||
        (pEntry->contextPolicy != STREAM_CONTEXT_POLICY_NONE &&
         pEntry->contextPolicy != STREAM_CONTEXT_POLICY_ANCESTOR) ||
        (pPrior != NULL && tCompareStreamContextPolicyKey(pPrior, pEntry) >= 0)) {
      return TSDB_CODE_INVALID_PARA;
    }
    pPrior = pEntry;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tEncodeStreamContextPolicy(SEncoder* pEncoder, const SStreamContextPolicy* pPolicy) {
  if (pEncoder == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t code = tValidateStreamContextPolicy(pPolicy);
  if (code != TSDB_CODE_SUCCESS) return code;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, STREAM_CONTEXT_POLICY_VERSION));
  const int32_t count = taosArrayGetSize(pPolicy->pEntries);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, count));
  for (int32_t i = 0; i < count; ++i) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pPolicy->pEntries, i);
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pEntry->gid));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pEntry->paramIndex));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pEntry->contextPolicy));
  }

_exit:
  return code;
}

int32_t tDecodeStreamContextPolicy(SDecoder* pDecoder, SStreamContextPolicy** ppPolicy) {
  if (ppPolicy == NULL) return TSDB_CODE_INVALID_PARA;
  *ppPolicy = NULL;
  if (pDecoder == NULL) return TSDB_CODE_INVALID_PARA;

  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  int32_t               version = 0;
  int32_t               count = 0;
  SStreamContextPolicy* pPolicy = NULL;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &version));
  QUERY_CHECK_CONDITION(version == STREAM_CONTEXT_POLICY_VERSION, code, lino, _exit, TSDB_CODE_INVALID_MSG);
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &count));
  QUERY_CHECK_CONDITION(count >= 0 && count <= 1024 * 1024, code, lino, _exit, TSDB_CODE_INVALID_MSG);
  QUERY_CHECK_CONDITION(
      (uint32_t)count <= TD_CODER_REMAIN_CAPACITY(pDecoder) / (sizeof(int64_t) + sizeof(int32_t) + sizeof(int8_t)),
      code, lino, _exit, TSDB_CODE_INVALID_MSG);

  pPolicy = taosMemoryCalloc(1, sizeof(*pPolicy));
  QUERY_CHECK_NULL(pPolicy, code, lino, _exit, terrno);
  pPolicy->pEntries = taosArrayInit(count == 0 ? 1 : count, sizeof(SStreamContextPolicyEntry));
  QUERY_CHECK_NULL(pPolicy->pEntries, code, lino, _exit, terrno);
  for (int32_t i = 0; i < count; ++i) {
    SStreamContextPolicyEntry entry = {0};
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &entry.gid));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &entry.paramIndex));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &entry.contextPolicy));
    QUERY_CHECK_NULL(taosArrayPush(pPolicy->pEntries, &entry), code, lino, _exit, terrno);
  }
  TAOS_CHECK_EXIT(tValidateStreamContextPolicy(pPolicy));
  *ppPolicy = pPolicy;
  pPolicy = NULL;

_exit:
  tDestroyStreamContextPolicy(&pPolicy);
  return code;
}

int32_t tCloneStreamContextPolicy(const SStreamContextPolicy* pSrc, SStreamContextPolicy** ppDst) {
  if (ppDst == NULL) return TSDB_CODE_INVALID_PARA;
  *ppDst = NULL;
  int32_t code = tValidateStreamContextPolicy(pSrc);
  if (code != TSDB_CODE_SUCCESS) return code;

  SStreamContextPolicy* pDst = taosMemoryCalloc(1, sizeof(*pDst));
  if (pDst == NULL) return terrno;
  pDst->pEntries = taosArrayDup(pSrc->pEntries, NULL);
  if (pDst->pEntries == NULL) {
    code = terrno;
    tDestroyStreamContextPolicy(&pDst);
    return code;
  }
  *ppDst = pDst;
  return TSDB_CODE_SUCCESS;
}

static int32_t tValidateWindowLineage(const SWindowLineage* pLineage, bool requireAncestor) {
  if (pLineage == NULL) return TSDB_CODE_INVALID_PARA;
  if (pLineage->pScopes != NULL && pLineage->pScopes->elemSize != sizeof(SScopeInstanceId)) {
    return TSDB_CODE_INVALID_PARA;
  }
  const int32_t count = taosArrayGetSize(pLineage->pScopes);
  if ((requireAncestor && count == 0) || count >= STREAM_WINDOW_MAX_LAYERS) return TSDB_CODE_INVALID_PARA;
  for (int32_t i = 0; i < count; ++i) {
    const SScopeInstanceId* pScope = taosArrayGet(pLineage->pScopes, i);
    if (pScope == NULL || pScope->layerIndex != i || !tIsAncestorWindowType(pScope->triggerType)) {
      return TSDB_CODE_INVALID_PARA;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool tWindowLineageEqual(const SWindowLineage* pLeft, const SWindowLineage* pRight) {
  const int32_t leftCount = taosArrayGetSize(pLeft == NULL ? NULL : pLeft->pScopes);
  const int32_t rightCount = taosArrayGetSize(pRight == NULL ? NULL : pRight->pScopes);
  if (leftCount != rightCount) return false;
  for (int32_t i = 0; i < leftCount; ++i) {
    const SScopeInstanceId* pLeftScope = taosArrayGet(pLeft->pScopes, i);
    const SScopeInstanceId* pRightScope = taosArrayGet(pRight->pScopes, i);
    if (pLeftScope->layerIndex != pRightScope->layerIndex || pLeftScope->triggerType != pRightScope->triggerType ||
        pLeftScope->openingTs != pRightScope->openingTs ||
        pLeftScope->nativeDiscriminator != pRightScope->nativeDiscriminator) {
      return false;
    }
  }
  return true;
}

typedef struct {
  int64_t gid;
  int32_t paramIndex;
  int32_t reserved;
} SStreamContextParamKey;

typedef struct {
  int32_t layerIndex;
  int8_t  triggerType;
  int8_t  reserved[3];
  TSKEY   openingTs;
  int64_t nativeDiscriminator;
} SStreamContextLineageScopeKey;

typedef struct {
  int64_t                       gid;
  int32_t                       scopeCount;
  int32_t                       reserved;
  SStreamContextLineageScopeKey scopes[STREAM_WINDOW_MAX_LAYERS - 1];
} SStreamContextLineageKey;

typedef struct {
  int32_t vgId;
  int32_t readInfoIndex;
} SStreamContextReadKey;

typedef struct {
  SSHashObj* pParams;
  SSHashObj* pLineages;
  SSHashObj* pReadBindings;
} SStreamAncestorContextIndex;

static SStreamContextParamKey tMakeStreamContextParamKey(int64_t gid, int32_t paramIndex) {
  SStreamContextParamKey key = {0};
  key.gid = gid;
  key.paramIndex = paramIndex;
  return key;
}

static SStreamContextReadKey tMakeStreamContextReadKey(int32_t vgId, int32_t readInfoIndex) {
  SStreamContextReadKey key = {0};
  key.vgId = vgId;
  key.readInfoIndex = readInfoIndex;
  return key;
}

static int32_t tMakeStreamContextLineageKey(int64_t gid, const SWindowLineage* pLineage,
                                            SStreamContextLineageKey* pKey) {
  if (pKey == NULL) return TSDB_CODE_INVALID_PARA;
  memset(pKey, 0, sizeof(*pKey));
  pKey->gid = gid;
  pKey->scopeCount = taosArrayGetSize(pLineage == NULL ? NULL : pLineage->pScopes);
  if (pKey->scopeCount <= 0 || pKey->scopeCount >= STREAM_WINDOW_MAX_LAYERS) {
    return TSDB_CODE_INVALID_PARA;
  }
  for (int32_t i = 0; i < pKey->scopeCount; ++i) {
    const SScopeInstanceId* pScope = taosArrayGet(pLineage->pScopes, i);
    if (pScope == NULL) return TSDB_CODE_INVALID_PARA;
    pKey->scopes[i].layerIndex = pScope->layerIndex;
    pKey->scopes[i].triggerType = pScope->triggerType;
    pKey->scopes[i].openingTs = pScope->openingTs;
    pKey->scopes[i].nativeDiscriminator = pScope->nativeDiscriminator;
  }
  return TSDB_CODE_SUCCESS;
}

static void tCleanupStreamAncestorContextIndex(SStreamAncestorContextIndex* pIndex) {
  if (pIndex == NULL) return;
  tSimpleHashCleanup(pIndex->pParams);
  tSimpleHashCleanup(pIndex->pLineages);
  tSimpleHashCleanup(pIndex->pReadBindings);
  memset(pIndex, 0, sizeof(*pIndex));
}

static int32_t tInitStreamAncestorContextIndex(int32_t paramCount, int32_t bindingCount,
                                               SStreamAncestorContextIndex* pIndex) {
  if (pIndex == NULL) return TSDB_CODE_INVALID_PARA;
  memset(pIndex, 0, sizeof(*pIndex));
  pIndex->pParams = tSimpleHashInit(paramCount, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  pIndex->pLineages = tSimpleHashInit(paramCount, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  pIndex->pReadBindings = tSimpleHashInit(bindingCount, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (pIndex->pParams == NULL || pIndex->pLineages == NULL || pIndex->pReadBindings == NULL) {
    int32_t code = terrno;
    tCleanupStreamAncestorContextIndex(pIndex);
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

typedef enum {
  STREAM_ANCESTOR_SNAPSHOT_SLIDING = 0,
  STREAM_ANCESTOR_SNAPSHOT_WINDOW = 1,
} EStreamAncestorSnapshotRepresentation;

static int32_t tClassifyAncestorSnapshot(const SWindowAncestorSnapshot*         pSnapshot,
                                         EStreamAncestorSnapshotRepresentation* pRepresentation) {
  if (pSnapshot == NULL || pRepresentation == NULL) return TSDB_CODE_INVALID_PARA;
  const int64_t slidingMask = PLACE_HOLDER_PREV_TS | PLACE_HOLDER_CURRENT_TS | PLACE_HOLDER_NEXT_TS;
  const int64_t windowMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND | PLACE_HOLDER_WDURATION | PLACE_HOLDER_WROWNUM;
  const int64_t allowedMask = slidingMask | windowMask;
  if ((pSnapshot->placeholderMask & ~allowedMask) != 0) return TSDB_CODE_INVALID_PARA;

  const bool hasSliding = (pSnapshot->placeholderMask & slidingMask) != 0;
  const bool hasWindow = (pSnapshot->placeholderMask & windowMask) != 0;
  if (hasSliding && (hasWindow || pSnapshot->triggerType != WINDOW_TYPE_INTERVAL)) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pRepresentation = hasSliding ? STREAM_ANCESTOR_SNAPSHOT_SLIDING : STREAM_ANCESTOR_SNAPSHOT_WINDOW;
  return TSDB_CODE_SUCCESS;
}

static int32_t tValidateAncestorParamContext(const SStreamAncestorParamContext* pParam) {
  if (pParam == NULL || pParam->paramIndex < 0 || !tIsAncestorWindowType(pParam->leafIdentity.triggerType)) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = tValidateWindowLineage(&pParam->leafIdentity.lineage, true);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (pParam->pSnapshots == NULL || pParam->pSnapshots->elemSize != sizeof(SWindowAncestorSnapshot)) {
    return TSDB_CODE_INVALID_PARA;
  }
  const int32_t lineageCount = taosArrayGetSize(pParam->leafIdentity.lineage.pScopes);
  if (taosArrayGetSize(pParam->pSnapshots) != lineageCount) return TSDB_CODE_INVALID_PARA;
  for (int32_t i = 0; i < lineageCount; ++i) {
    const SScopeInstanceId*        pScope = taosArrayGet(pParam->leafIdentity.lineage.pScopes, i);
    const SWindowAncestorSnapshot* pSnapshot = taosArrayGet(pParam->pSnapshots, i);
    if (pSnapshot == NULL || pSnapshot->layerIndex != i || pSnapshot->layerIndex != pScope->layerIndex ||
        pSnapshot->triggerType != pScope->triggerType) {
      return TSDB_CODE_INVALID_PARA;
    }
    EStreamAncestorSnapshotRepresentation representation = STREAM_ANCESTOR_SNAPSHOT_WINDOW;
    if (tClassifyAncestorSnapshot(pSnapshot, &representation) != TSDB_CODE_SUCCESS) return TSDB_CODE_INVALID_PARA;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tBuildStreamAncestorContextIndex(const SStreamAncestorContext* pContext,
                                                SStreamAncestorContextIndex*  pIndex) {
  if (pContext == NULL || pContext->pParamContexts == NULL ||
      pContext->pParamContexts->elemSize != sizeof(SStreamAncestorParamContext) ||
      taosArrayGetSize(pContext->pParamContexts) <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pContext->pReadScopeBindings != NULL &&
      pContext->pReadScopeBindings->elemSize != sizeof(SStreamReadScopeBinding)) {
    return TSDB_CODE_INVALID_PARA;
  }

  const int32_t paramCount = taosArrayGetSize(pContext->pParamContexts);
  const int32_t bindingCount = taosArrayGetSize(pContext->pReadScopeBindings);
  int32_t       code = tInitStreamAncestorContextIndex(paramCount, bindingCount, pIndex);
  if (code != TSDB_CODE_SUCCESS) return code;

  for (int32_t i = 0; i < paramCount; ++i) {
    const SStreamAncestorParamContext* pParam = taosArrayGet(pContext->pParamContexts, i);
    code = tValidateAncestorParamContext(pParam);
    if (code != TSDB_CODE_SUCCESS) goto _exit;

    const SStreamContextParamKey paramKey = tMakeStreamContextParamKey(pParam->leafIdentity.gid, pParam->paramIndex);
    if (tSimpleHashGet(pIndex->pParams, &paramKey, sizeof(paramKey)) != NULL) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    TAOS_CHECK_GOTO(tSimpleHashPut(pIndex->pParams, &paramKey, sizeof(paramKey), &i, sizeof(i)), NULL, _exit);

    SStreamContextLineageKey lineageKey = {0};
    TAOS_CHECK_GOTO(tMakeStreamContextLineageKey(pParam->leafIdentity.gid, &pParam->leafIdentity.lineage, &lineageKey),
                    NULL, _exit);
    int32_t* pLineageCount = tSimpleHashGet(pIndex->pLineages, &lineageKey, sizeof(lineageKey));
    if (pLineageCount == NULL) {
      const int32_t one = 1;
      TAOS_CHECK_GOTO(tSimpleHashPut(pIndex->pLineages, &lineageKey, sizeof(lineageKey), &one, sizeof(one)), NULL,
                      _exit);
    } else {
      ++*pLineageCount;
    }
  }

  for (int32_t i = 0; i < bindingCount; ++i) {
    const SStreamReadScopeBinding* pBinding = taosArrayGet(pContext->pReadScopeBindings, i);
    if (pBinding == NULL || pBinding->readInfoIndex < 0 ||
        tValidateWindowLineage(&pBinding->scope.lineage, true) != TSDB_CODE_SUCCESS) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }

    const SStreamContextReadKey readKey = tMakeStreamContextReadKey(pBinding->vgId, pBinding->readInfoIndex);
    if (tSimpleHashGet(pIndex->pReadBindings, &readKey, sizeof(readKey)) != NULL) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    TAOS_CHECK_GOTO(tSimpleHashPut(pIndex->pReadBindings, &readKey, sizeof(readKey), &i, sizeof(i)), NULL, _exit);

    SStreamContextLineageKey lineageKey = {0};
    TAOS_CHECK_GOTO(tMakeStreamContextLineageKey(pBinding->scope.gid, &pBinding->scope.lineage, &lineageKey), NULL,
                    _exit);
    if (tSimpleHashGet(pIndex->pLineages, &lineageKey, sizeof(lineageKey)) == NULL) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
  }

  return TSDB_CODE_SUCCESS;

_exit:
  tCleanupStreamAncestorContextIndex(pIndex);
  return code;
}

static int32_t tValidateStreamAncestorContext(const SStreamAncestorContext* pContext) {
  SStreamAncestorContextIndex index = {0};
  int32_t                     code = tBuildStreamAncestorContextIndex(pContext, &index);
  tCleanupStreamAncestorContextIndex(&index);
  return code;
}

static const SStreamContextPolicyEntry* tFindStreamContextPolicyEntry(const SStreamContextPolicy* pPolicy, int64_t gid,
                                                                      int32_t paramIndex) {
  int32_t low = 0;
  int32_t high = taosArrayGetSize(pPolicy == NULL ? NULL : pPolicy->pEntries) - 1;
  while (low <= high) {
    const int32_t                    mid = low + (high - low) / 2;
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pPolicy->pEntries, mid);
    if (pEntry == NULL) return NULL;
    if (pEntry->gid == gid && pEntry->paramIndex == paramIndex) return pEntry;
    if (pEntry->gid < gid || (pEntry->gid == gid && pEntry->paramIndex < paramIndex)) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return NULL;
}

int32_t tAdmitStreamContext(const SStreamContextPolicy* pPolicy, const SStreamAncestorContext* pContext,
                            bool requiresContextPolicy) {
  if (!requiresContextPolicy) {
    return pPolicy == NULL && pContext == NULL ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;
  }
  int32_t code = tValidateStreamContextPolicy(pPolicy);
  if (code != TSDB_CODE_SUCCESS) return code;

  int32_t ancestorCount = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pPolicy->pEntries); ++i) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pPolicy->pEntries, i);
    if (pEntry->contextPolicy == STREAM_CONTEXT_POLICY_ANCESTOR) ++ancestorCount;
  }

  if (ancestorCount == 0) return pContext == NULL ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;

  SStreamAncestorContextIndex index = {0};
  code = tBuildStreamAncestorContextIndex(pContext, &index);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (taosArrayGetSize(pContext->pParamContexts) != ancestorCount) {
    code = TSDB_CODE_INVALID_PARA;
    goto _exit;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pPolicy->pEntries); ++i) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pPolicy->pEntries, i);
    if (pEntry->contextPolicy != STREAM_CONTEXT_POLICY_ANCESTOR) continue;
    const SStreamContextParamKey paramKey = tMakeStreamContextParamKey(pEntry->gid, pEntry->paramIndex);
    if (tSimpleHashGet(index.pParams, &paramKey, sizeof(paramKey)) == NULL) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
  }
  for (int32_t i = 0; i < taosArrayGetSize(pContext->pParamContexts); ++i) {
    const SStreamAncestorParamContext* pParam = taosArrayGet(pContext->pParamContexts, i);
    const SStreamContextPolicyEntry*   pEntry =
        tFindStreamContextPolicyEntry(pPolicy, pParam->leafIdentity.gid, pParam->paramIndex);
    if (pEntry == NULL || pEntry->contextPolicy != STREAM_CONTEXT_POLICY_ANCESTOR) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
  }

_exit:
  tCleanupStreamAncestorContextIndex(&index);
  return code;
}

static int32_t tEncodeWindowLineage(SEncoder* pEncoder, const SWindowLineage* pLineage) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  const int32_t count = taosArrayGetSize(pLineage->pScopes);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, count));
  for (int32_t i = 0; i < count; ++i) {
    const SScopeInstanceId* pScope = taosArrayGet(pLineage->pScopes, i);
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pScope->layerIndex));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pScope->triggerType));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pScope->openingTs));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pScope->nativeDiscriminator));
  }
_exit:
  return code;
}

static int32_t tEncodeAncestorSnapshot(SEncoder* pEncoder, const SWindowAncestorSnapshot* pSnapshot) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  EStreamAncestorSnapshotRepresentation representation = STREAM_ANCESTOR_SNAPSHOT_WINDOW;
  TAOS_CHECK_EXIT(tClassifyAncestorSnapshot(pSnapshot, &representation));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pSnapshot->layerIndex));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pSnapshot->triggerType));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pSnapshot->placeholderMask));
  if (representation == STREAM_ANCESTOR_SNAPSHOT_SLIDING) {
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pSnapshot->values.sliding.prevTs));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pSnapshot->values.sliding.currentTs));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pSnapshot->values.sliding.nextTs));
  } else {
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pSnapshot->values.window.start));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pSnapshot->values.window.end));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pSnapshot->values.window.duration));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pSnapshot->values.window.rownum));
  }
_exit:
  return code;
}

int32_t tEncodeStreamAncestorContext(SEncoder* pEncoder, const SStreamAncestorContext* pContext) {
  if (pEncoder == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t code = tValidateStreamAncestorContext(pContext);
  int32_t lino = 0;
  if (code != TSDB_CODE_SUCCESS) return code;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, STREAM_ANCESTOR_CONTEXT_VERSION));
  const int32_t paramCount = taosArrayGetSize(pContext->pParamContexts);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, paramCount));
  for (int32_t i = 0; i < paramCount; ++i) {
    const SStreamAncestorParamContext* pParam = taosArrayGet(pContext->pParamContexts, i);
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pParam->paramIndex));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pParam->leafIdentity.gid));
    TAOS_CHECK_EXIT(tEncodeWindowLineage(pEncoder, &pParam->leafIdentity.lineage));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pParam->leafIdentity.triggerType));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pParam->leafIdentity.openingTs));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pParam->leafIdentity.nativeDiscriminator));
    const int32_t snapshotCount = taosArrayGetSize(pParam->pSnapshots);
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, snapshotCount));
    for (int32_t j = 0; j < snapshotCount; ++j) {
      TAOS_CHECK_EXIT(tEncodeAncestorSnapshot(pEncoder, taosArrayGet(pParam->pSnapshots, j)));
    }
  }
  const int32_t bindingCount = taosArrayGetSize(pContext->pReadScopeBindings);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, bindingCount));
  for (int32_t i = 0; i < bindingCount; ++i) {
    const SStreamReadScopeBinding* pBinding = taosArrayGet(pContext->pReadScopeBindings, i);
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pBinding->vgId));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pBinding->readInfoIndex));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pBinding->scope.gid));
    TAOS_CHECK_EXIT(tEncodeWindowLineage(pEncoder, &pBinding->scope.lineage));
  }
_exit:
  return code;
}

static int32_t tDecodeAncestorCount(SDecoder* pDecoder, uint32_t minimumBytes, int32_t* pCount) {
  int32_t code = tDecodeI32(pDecoder, pCount);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (*pCount < 0 || (minimumBytes > 0 && (uint32_t)*pCount > TD_CODER_REMAIN_CAPACITY(pDecoder) / minimumBytes)) {
    return TSDB_CODE_INVALID_MSG;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tDecodeWindowLineage(SDecoder* pDecoder, SWindowLineage* pLineage) {
  int32_t count = 0;
  int32_t code = tDecodeAncestorCount(pDecoder, sizeof(int32_t) + sizeof(int8_t) + sizeof(int64_t) * 2, &count);
  int32_t lino = 0;
  if (code != TSDB_CODE_SUCCESS || count >= STREAM_WINDOW_MAX_LAYERS) {
    return code == TSDB_CODE_SUCCESS ? TSDB_CODE_INVALID_MSG : code;
  }
  if (count > 0) {
    pLineage->pScopes = taosArrayInit(count, sizeof(SScopeInstanceId));
    QUERY_CHECK_NULL(pLineage->pScopes, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < count; ++i) {
    SScopeInstanceId scope = {0};
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &scope.layerIndex));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &scope.triggerType));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &scope.openingTs));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &scope.nativeDiscriminator));
    QUERY_CHECK_NULL(taosArrayPush(pLineage->pScopes, &scope), code, lino, _exit, terrno);
  }
_exit:
  return code;
}

static int32_t tDecodeAncestorSnapshot(SDecoder* pDecoder, SWindowAncestorSnapshot* pSnapshot) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pSnapshot->layerIndex));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pSnapshot->triggerType));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pSnapshot->placeholderMask));
  EStreamAncestorSnapshotRepresentation representation = STREAM_ANCESTOR_SNAPSHOT_WINDOW;
  TAOS_CHECK_EXIT(tClassifyAncestorSnapshot(pSnapshot, &representation));
  if (representation == STREAM_ANCESTOR_SNAPSHOT_SLIDING) {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pSnapshot->values.sliding.prevTs));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pSnapshot->values.sliding.currentTs));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pSnapshot->values.sliding.nextTs));
  } else {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pSnapshot->values.window.start));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pSnapshot->values.window.end));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pSnapshot->values.window.duration));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pSnapshot->values.window.rownum));
  }
_exit:
  return code;
}

int32_t tDecodeStreamAncestorContext(SDecoder* pDecoder, SStreamAncestorContext** ppContext) {
  if (ppContext == NULL) return TSDB_CODE_INVALID_PARA;
  *ppContext = NULL;
  if (pDecoder == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t version = 0;
  int32_t code = tDecodeI32(pDecoder, &version);
  int32_t lino = 0;
  if (code != TSDB_CODE_SUCCESS || version != STREAM_ANCESTOR_CONTEXT_VERSION) {
    return code == TSDB_CODE_SUCCESS ? TSDB_CODE_INVALID_MSG : code;
  }
  SStreamAncestorContext* pContext = taosMemoryCalloc(1, sizeof(*pContext));
  QUERY_CHECK_NULL(pContext, code, lino, _exit, terrno);

  int32_t paramCount = 0;
  TAOS_CHECK_EXIT(tDecodeAncestorCount(pDecoder, sizeof(int32_t) + sizeof(int64_t), &paramCount));
  QUERY_CHECK_CONDITION(paramCount > 0, code, lino, _exit, TSDB_CODE_INVALID_MSG);
  pContext->pParamContexts = taosArrayInit(paramCount, sizeof(SStreamAncestorParamContext));
  QUERY_CHECK_NULL(pContext->pParamContexts, code, lino, _exit, terrno);
  for (int32_t i = 0; i < paramCount; ++i) {
    SStreamAncestorParamContext param = {0};
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &param.paramIndex));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &param.leafIdentity.gid));
    code = tDecodeWindowLineage(pDecoder, &param.leafIdentity.lineage);
    if (code != TSDB_CODE_SUCCESS) {
      tDestroyAncestorParamContext(&param);
      goto _exit;
    }
    code = tDecodeI8(pDecoder, &param.leafIdentity.triggerType);
    if (code == TSDB_CODE_SUCCESS) code = tDecodeI64(pDecoder, &param.leafIdentity.openingTs);
    if (code == TSDB_CODE_SUCCESS) code = tDecodeI64(pDecoder, &param.leafIdentity.nativeDiscriminator);
    int32_t snapshotCount = 0;
    if (code == TSDB_CODE_SUCCESS)
      code = tDecodeAncestorCount(pDecoder, sizeof(int32_t) + sizeof(int8_t), &snapshotCount);
    if (code == TSDB_CODE_SUCCESS && snapshotCount > 0) {
      param.pSnapshots = taosArrayInit(snapshotCount, sizeof(SWindowAncestorSnapshot));
      if (param.pSnapshots == NULL) code = terrno;
    }
    for (int32_t j = 0; code == TSDB_CODE_SUCCESS && j < snapshotCount; ++j) {
      SWindowAncestorSnapshot snapshot = {0};
      code = tDecodeAncestorSnapshot(pDecoder, &snapshot);
      if (code == TSDB_CODE_SUCCESS && taosArrayPush(param.pSnapshots, &snapshot) == NULL) code = terrno;
    }
    if (code != TSDB_CODE_SUCCESS || taosArrayPush(pContext->pParamContexts, &param) == NULL) {
      if (code == TSDB_CODE_SUCCESS) code = terrno;
      tDestroyAncestorParamContext(&param);
      goto _exit;
    }
  }

  int32_t bindingCount = 0;
  TAOS_CHECK_EXIT(tDecodeAncestorCount(pDecoder, sizeof(int32_t) * 2 + sizeof(int64_t), &bindingCount));
  if (bindingCount > 0) {
    pContext->pReadScopeBindings = taosArrayInit(bindingCount, sizeof(SStreamReadScopeBinding));
    QUERY_CHECK_NULL(pContext->pReadScopeBindings, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < bindingCount; ++i) {
    SStreamReadScopeBinding binding = {0};
    code = tDecodeI32(pDecoder, &binding.vgId);
    if (code == TSDB_CODE_SUCCESS) code = tDecodeI32(pDecoder, &binding.readInfoIndex);
    if (code == TSDB_CODE_SUCCESS) code = tDecodeI64(pDecoder, &binding.scope.gid);
    if (code == TSDB_CODE_SUCCESS) code = tDecodeWindowLineage(pDecoder, &binding.scope.lineage);
    if (code != TSDB_CODE_SUCCESS || taosArrayPush(pContext->pReadScopeBindings, &binding) == NULL) {
      if (code == TSDB_CODE_SUCCESS) code = terrno;
      tDestroyReadScopeBinding(&binding);
      goto _exit;
    }
  }
  TAOS_CHECK_EXIT(tValidateStreamAncestorContext(pContext));
  *ppContext = pContext;
  pContext = NULL;

_exit:
  tDestroyStreamAncestorContext(&pContext);
  return code;
}

int32_t tCloneStreamAncestorContext(const SStreamAncestorContext* pSrc, SStreamAncestorContext** ppDst) {
  if (ppDst == NULL) return TSDB_CODE_INVALID_PARA;
  *ppDst = NULL;
  int32_t code = tValidateStreamAncestorContext(pSrc);
  if (code != TSDB_CODE_SUCCESS) return code;
  SEncoder sizer = {0};
  tEncoderInit(&sizer, NULL, 0);
  code = tEncodeStreamAncestorContext(&sizer, pSrc);
  const int32_t size = sizer.pos;
  tEncoderClear(&sizer);
  if (code != TSDB_CODE_SUCCESS) return code;
  void* pBuffer = taosMemoryMalloc(size);
  if (pBuffer == NULL) return terrno;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, pBuffer, size);
  code = tEncodeStreamAncestorContext(&encoder, pSrc);
  tEncoderClear(&encoder);
  if (code == TSDB_CODE_SUCCESS) {
    SDecoder decoder = {0};
    tDecoderInit(&decoder, pBuffer, size);
    code = tDecodeStreamAncestorContext(&decoder, ppDst);
    if (code == TSDB_CODE_SUCCESS && !tDecodeIsEnd(&decoder)) code = TSDB_CODE_INVALID_MSG;
    tDecoderClear(&decoder);
  }
  if (code != TSDB_CODE_SUCCESS) tDestroyStreamAncestorContext(ppDst);
  taosMemoryFree(pBuffer);
  return code;
}

int32_t tProjectStreamAncestorContext(const SStreamAncestorContext* pSrc, int64_t gid, int32_t srcParamIndex,
                                      int32_t dstParamIndex, SStreamAncestorContext** ppDst) {
  if (ppDst == NULL) return TSDB_CODE_INVALID_PARA;
  *ppDst = NULL;
  if (dstParamIndex < 0) return TSDB_CODE_INVALID_PARA;
  int32_t code = tValidateStreamAncestorContext(pSrc);
  if (code != TSDB_CODE_SUCCESS) return code;
  const SStreamAncestorParamContext* pMatch = NULL;
  const int32_t                      paramCount = taosArrayGetSize(pSrc->pParamContexts);
  for (int32_t i = 0; i < paramCount; ++i) {
    const SStreamAncestorParamContext* pParam = taosArrayGet(pSrc->pParamContexts, i);
    if (pParam->leafIdentity.gid == gid && pParam->paramIndex == srcParamIndex) {
      if (pMatch != NULL) return TSDB_CODE_INVALID_PARA;
      pMatch = pParam;
    }
  }
  if (pMatch == NULL) return TSDB_CODE_INVALID_PARA;

  SStreamAncestorContext selected = {0};
  selected.pParamContexts = taosArrayInit(1, sizeof(SStreamAncestorParamContext));
  if (selected.pParamContexts == NULL) return terrno;
  if (taosArrayPush(selected.pParamContexts, pMatch) == NULL) {
    taosArrayDestroy(selected.pParamContexts);
    return terrno;
  }
  const int32_t bindingCount = taosArrayGetSize(pSrc->pReadScopeBindings);
  for (int32_t i = 0; i < bindingCount; ++i) {
    const SStreamReadScopeBinding* pBinding = taosArrayGet(pSrc->pReadScopeBindings, i);
    if (pBinding->scope.gid == gid && tWindowLineageEqual(&pBinding->scope.lineage, &pMatch->leafIdentity.lineage)) {
      if (selected.pReadScopeBindings == NULL) {
        selected.pReadScopeBindings = taosArrayInit(1, sizeof(SStreamReadScopeBinding));
        if (selected.pReadScopeBindings == NULL) {
          taosArrayDestroy(selected.pParamContexts);
          return terrno;
        }
      }
      if (taosArrayPush(selected.pReadScopeBindings, pBinding) == NULL) {
        taosArrayDestroy(selected.pParamContexts);
        taosArrayDestroy(selected.pReadScopeBindings);
        return terrno;
      }
    }
  }
  code = tCloneStreamAncestorContext(&selected, ppDst);
  taosArrayDestroy(selected.pParamContexts);
  taosArrayDestroy(selected.pReadScopeBindings);
  if (code == TSDB_CODE_SUCCESS) {
    SStreamAncestorParamContext* pProjected = taosArrayGet((*ppDst)->pParamContexts, 0);
    pProjected->paramIndex = dstParamIndex;
  }
  return code;
}

static const SStreamAncestorParamContext* tFindAncestorParamContext(const SStreamAncestorContext* pContext, int64_t gid,
                                                                    int32_t paramIndex) {
  for (int32_t i = 0; i < taosArrayGetSize(pContext == NULL ? NULL : pContext->pParamContexts); ++i) {
    const SStreamAncestorParamContext* pParam = taosArrayGet(pContext->pParamContexts, i);
    if (pParam->leafIdentity.gid == gid && pParam->paramIndex == paramIndex) return pParam;
  }
  return NULL;
}

static const SStreamReadScopeBinding* tFindFetchReadBinding(const SStreamRuntimeFuncInfo* pInfo,
                                                            int32_t                       readInfoIndex) {
  for (int32_t i = 0;
       i < taosArrayGetSize(pInfo->pAncestorContext == NULL ? NULL : pInfo->pAncestorContext->pReadScopeBindings);
       ++i) {
    const SStreamReadScopeBinding* pBinding = taosArrayGet(pInfo->pAncestorContext->pReadScopeBindings, i);
    if (pBinding->vgId == pInfo->curNodeId && pBinding->readInfoIndex == readInfoIndex) return pBinding;
  }
  return NULL;
}

static bool tReadBindingMatchesParam(const SStreamReadScopeBinding*     pBinding,
                                     const SStreamAncestorParamContext* pParam) {
  return pBinding != NULL && pParam != NULL && pBinding->scope.gid == pParam->leafIdentity.gid &&
         tWindowLineageEqual(&pBinding->scope.lineage, &pParam->leafIdentity.lineage);
}

static bool tBindingSelectsAncestorParam(const SStreamRuntimeFuncInfo* pInfo, const SStreamContextPolicyEntry* pEntry) {
  const SStreamAncestorParamContext* pParam =
      tFindAncestorParamContext(pInfo->pAncestorContext, pEntry->gid, pEntry->paramIndex);
  if (pParam == NULL) return false;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->curGrpRead); ++i) {
    const SSTriggerGroupReadInfo*  pRead = taosArrayGet(pInfo->curGrpRead, i);
    const SStreamReadScopeBinding* pBinding = tFindFetchReadBinding(pInfo, i);
    if (pRead == NULL || pBinding == NULL || pBinding->scope.gid != pRead->gid ||
        !tReadBindingMatchesParam(pBinding, pParam)) {
      continue;
    }

    for (int32_t j = 0; j < taosArrayGetSize(pInfo->pContextPolicy->pEntries); ++j) {
      const SStreamContextPolicyEntry* pCandidate = taosArrayGet(pInfo->pContextPolicy->pEntries, j);
      if (pCandidate->contextPolicy != STREAM_CONTEXT_POLICY_ANCESTOR) continue;
      const SStreamAncestorParamContext* pCandidateParam =
          tFindAncestorParamContext(pInfo->pAncestorContext, pCandidate->gid, pCandidate->paramIndex);
      if (tReadBindingMatchesParam(pBinding, pCandidateParam)) return pCandidate == pEntry;
    }
  }
  return false;
}

static int32_t tValidateFetchBindingDependencies(const SStreamRuntimeFuncInfo* pInfo) {
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->curGrpRead); ++i) {
    const SSTriggerGroupReadInfo*  pRead = taosArrayGet(pInfo->curGrpRead, i);
    const SStreamReadScopeBinding* pBinding = tFindFetchReadBinding(pInfo, i);
    if (pRead == NULL || pBinding == NULL || pBinding->scope.gid != pRead->gid) {
      return TSDB_CODE_INVALID_PARA;
    }
    bool hasDependency = false;
    for (int32_t j = 0; j < taosArrayGetSize(pInfo->pContextPolicy->pEntries); ++j) {
      const SStreamContextPolicyEntry* pEntry = taosArrayGet(pInfo->pContextPolicy->pEntries, j);
      if (pEntry->contextPolicy != STREAM_CONTEXT_POLICY_ANCESTOR) continue;
      const SStreamAncestorParamContext* pParam =
          tFindAncestorParamContext(pInfo->pAncestorContext, pEntry->gid, pEntry->paramIndex);
      if (tReadBindingMatchesParam(pBinding, pParam)) {
        hasDependency = true;
        break;
      }
    }
    if (!hasDependency) return TSDB_CODE_INVALID_PARA;
  }
  return TSDB_CODE_SUCCESS;
}

static bool tGroupReadHasAncestorDependency(const SStreamAncestorContext* pContext, int64_t gid) {
  for (int32_t i = 0; i < taosArrayGetSize(pContext == NULL ? NULL : pContext->pParamContexts); ++i) {
    const SStreamAncestorParamContext* pParam = taosArrayGet(pContext->pParamContexts, i);
    if (pParam->leafIdentity.gid == gid) return true;
  }
  return false;
}

static int32_t tCountGroupReadBindings(const SStreamAncestorContext* pContext, int32_t vgId, int32_t readInfoIndex,
                                       const SStreamReadScopeBinding** ppMatch) {
  int32_t count = 0;
  *ppMatch = NULL;
  for (int32_t i = 0; i < taosArrayGetSize(pContext == NULL ? NULL : pContext->pReadScopeBindings); ++i) {
    const SStreamReadScopeBinding* pBinding = taosArrayGet(pContext->pReadScopeBindings, i);
    if (pBinding->vgId == vgId && pBinding->readInfoIndex == readInfoIndex) {
      *ppMatch = pBinding;
      ++count;
    }
  }
  return count;
}

static int32_t tValidateGroupReadBindings(SSHashObj* pGroupReadInfos, const SStreamAncestorContext* pContext) {
  const int32_t bindingCount = taosArrayGetSize(pContext == NULL ? NULL : pContext->pReadScopeBindings);
  if (pGroupReadInfos == NULL) return bindingCount == 0 ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;

  int32_t expectedCount = 0;
  int32_t iter = 0;
  void*   px = tSimpleHashIterate(pGroupReadInfos, NULL, &iter);
  while (px != NULL) {
    const int32_t vgId = *(int32_t*)tSimpleHashGetKey(px, NULL);
    const SArray* pInfos = *(SArray**)px;
    for (int32_t i = 0; i < taosArrayGetSize(pInfos); ++i) {
      const SSTriggerGroupReadInfo* pRead = taosArrayGet(pInfos, i);
      if (pRead == NULL) return TSDB_CODE_INVALID_PARA;
      if (!tGroupReadHasAncestorDependency(pContext, pRead->gid)) continue;

      ++expectedCount;
      const SStreamReadScopeBinding* pBinding = NULL;
      if (tCountGroupReadBindings(pContext, vgId, i, &pBinding) != 1 || pBinding->scope.gid != pRead->gid) {
        return TSDB_CODE_INVALID_PARA;
      }
    }
    px = tSimpleHashIterate(pGroupReadInfos, px, &iter);
  }
  return expectedCount == bindingCount ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;
}

static bool tFetchPolicyEntrySelected(const SStreamRuntimeFuncInfo* pInfo, const SStreamContextPolicyEntry* pEntry,
                                      bool needStreamRtInfo, bool bindingOnly) {
  if (bindingOnly) {
    if (pEntry->contextPolicy != STREAM_CONTEXT_POLICY_ANCESTOR) return false;
    return tBindingSelectsAncestorParam(pInfo, pEntry);
  }
  if (pInfo->isMultiGroupCalc) {
    if (needStreamRtInfo) return true;
    return pEntry->gid == pInfo->groupId && pEntry->paramIndex == pInfo->curIdx;
  }
  if (pEntry->gid != pInfo->groupId) return false;
  return needStreamRtInfo || pEntry->paramIndex == pInfo->curIdx;
}

static int32_t tProjectFetchAncestorContext(const SStreamRuntimeFuncInfo* pInfo,
                                            const SStreamContextPolicy* pProjectedPolicy, bool filteredBindings,
                                            SStreamAncestorContext** ppContext) {
  *ppContext = NULL;
  int32_t ancestorCount = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pProjectedPolicy->pEntries); ++i) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pProjectedPolicy->pEntries, i);
    if (pEntry->contextPolicy == STREAM_CONTEXT_POLICY_ANCESTOR) ++ancestorCount;
  }
  if (ancestorCount == 0) return TSDB_CODE_SUCCESS;

  int32_t                code = TSDB_CODE_SUCCESS;
  SStreamAncestorContext selected = {0};
  selected.pParamContexts = taosArrayInit(ancestorCount, sizeof(SStreamAncestorParamContext));
  if (selected.pParamContexts == NULL) return terrno;
  for (int32_t i = 0; i < taosArrayGetSize(pProjectedPolicy->pEntries); ++i) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pProjectedPolicy->pEntries, i);
    if (pEntry->contextPolicy != STREAM_CONTEXT_POLICY_ANCESTOR) continue;
    const SStreamAncestorParamContext* pParam =
        tFindAncestorParamContext(pInfo->pAncestorContext, pEntry->gid, pEntry->paramIndex);
    if (pParam == NULL || taosArrayPush(selected.pParamContexts, pParam) == NULL) {
      code = pParam == NULL ? TSDB_CODE_INVALID_PARA : terrno;
      goto _exit;
    }
  }

  if (pInfo->isMultiGroupCalc && filteredBindings) {
    selected.pReadScopeBindings = taosArrayInit(taosArrayGetSize(pInfo->curGrpRead), sizeof(SStreamReadScopeBinding));
    if (selected.pReadScopeBindings == NULL) {
      code = terrno;
      goto _exit;
    }
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->curGrpRead); ++i) {
      const SSTriggerGroupReadInfo*  pRead = taosArrayGet(pInfo->curGrpRead, i);
      const SStreamReadScopeBinding* pMatch = tFindFetchReadBinding(pInfo, i);
      if (pRead == NULL || pMatch == NULL || pMatch->scope.gid != pRead->gid) {
        code = TSDB_CODE_INVALID_PARA;
        goto _exit;
      }
      bool matchesProjectedParam = false;
      for (int32_t j = 0; j < taosArrayGetSize(selected.pParamContexts); ++j) {
        if (tReadBindingMatchesParam(pMatch, taosArrayGet(selected.pParamContexts, j))) {
          matchesProjectedParam = true;
          break;
        }
      }
      if (!matchesProjectedParam) {
        code = TSDB_CODE_INVALID_PARA;
        goto _exit;
      }
      SStreamReadScopeBinding projected = *pMatch;
      projected.readInfoIndex = i;
      if (taosArrayPush(selected.pReadScopeBindings, &projected) == NULL) {
        code = terrno;
        goto _exit;
      }
    }
  } else if (pInfo->isMultiGroupCalc && taosArrayGetSize(pInfo->pAncestorContext->pReadScopeBindings) > 0) {
    selected.pReadScopeBindings =
        taosArrayInit(taosArrayGetSize(pInfo->pAncestorContext->pReadScopeBindings), sizeof(SStreamReadScopeBinding));
    if (selected.pReadScopeBindings == NULL) {
      code = terrno;
      goto _exit;
    }
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->pAncestorContext->pReadScopeBindings); ++i) {
      const SStreamReadScopeBinding* pBinding = taosArrayGet(pInfo->pAncestorContext->pReadScopeBindings, i);
      bool                           matchesProjectedParam = false;
      for (int32_t j = 0; j < taosArrayGetSize(selected.pParamContexts); ++j) {
        if (tReadBindingMatchesParam(pBinding, taosArrayGet(selected.pParamContexts, j))) {
          matchesProjectedParam = true;
          break;
        }
      }
      if (matchesProjectedParam && taosArrayPush(selected.pReadScopeBindings, pBinding) == NULL) {
        code = terrno;
        goto _exit;
      }
    }
  }

  code = tCloneStreamAncestorContext(&selected, ppContext);

_exit:
  taosArrayDestroy(selected.pParamContexts);
  taosArrayDestroy(selected.pReadScopeBindings);
  return code;
}

int32_t tProjectStreamCalcContextForFetch(const SStreamRuntimeFuncInfo* pInfo, bool needStreamRtInfo,
                                          bool effectiveNeedStreamGrpInfo, SStreamContextPolicy** ppPolicy,
                                          SStreamAncestorContext** ppContext) {
  if (ppPolicy == NULL || ppContext == NULL) return TSDB_CODE_INVALID_PARA;
  *ppPolicy = NULL;
  *ppContext = NULL;
  if (pInfo == NULL) return TSDB_CODE_SUCCESS;
  const bool requiresContextPolicy = BIT_FLAG_TEST_MASK(pInfo->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN);
  int32_t    code = tAdmitStreamContext(pInfo->pContextPolicy, pInfo->pAncestorContext, requiresContextPolicy);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (!requiresContextPolicy) return TSDB_CODE_SUCCESS;

  if (!pInfo->isMultiGroupCalc) {
    const int32_t paramCount = taosArrayGetSize(pInfo->pStreamPesudoFuncVals);
    if (taosArrayGetSize(pInfo->pContextPolicy->pEntries) != paramCount) return TSDB_CODE_INVALID_PARA;
    for (int32_t i = 0; i < paramCount; ++i) {
      if (tFindStreamContextPolicyEntry(pInfo->pContextPolicy, pInfo->groupId, i) == NULL) {
        return TSDB_CODE_INVALID_PARA;
      }
    }
  } else if (pInfo->pGroupCalcInfos != NULL) {
    int32_t                 expectedCount = 0;
    int32_t                 iter = 0;
    SSTriggerGroupCalcInfo* pCalcInfo = tSimpleHashIterate(pInfo->pGroupCalcInfos, NULL, &iter);
    while (pCalcInfo != NULL) {
      int64_t* pGid = tSimpleHashGetKey(pCalcInfo, NULL);
      for (int32_t i = 0; i < taosArrayGetSize(pCalcInfo->pParams); ++i) {
        if (tFindStreamContextPolicyEntry(pInfo->pContextPolicy, *pGid, i) == NULL) {
          return TSDB_CODE_INVALID_PARA;
        }
        ++expectedCount;
      }
      pCalcInfo = tSimpleHashIterate(pInfo->pGroupCalcInfos, pCalcInfo, &iter);
    }
    if (taosArrayGetSize(pInfo->pContextPolicy->pEntries) != expectedCount) return TSDB_CODE_INVALID_PARA;
    if (needStreamRtInfo && !effectiveNeedStreamGrpInfo) {
      code = tValidateGroupReadBindings(pInfo->pGroupReadInfos, pInfo->pAncestorContext);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
  }

  bool bindingOnly = false;
  if (pInfo->isMultiGroupCalc && needStreamRtInfo && effectiveNeedStreamGrpInfo) {
    const SSTriggerGroupReadInfo* pFirst = taosArrayGet(pInfo->curGrpRead, 0);
    bindingOnly = pFirst != NULL && taosArrayGetSize(pFirst->pTables) > 0;
  }
  if (bindingOnly) {
    code = tValidateFetchBindingDependencies(pInfo);
    if (code != TSDB_CODE_SUCCESS) return code;
  }

  SStreamContextPolicy* pProjectedPolicy = taosMemoryCalloc(1, sizeof(*pProjectedPolicy));
  if (pProjectedPolicy == NULL) return terrno;
  pProjectedPolicy->pEntries =
      taosArrayInit(taosArrayGetSize(pInfo->pContextPolicy->pEntries) + 1, sizeof(SStreamContextPolicyEntry));
  if (pProjectedPolicy->pEntries == NULL) {
    code = terrno;
    goto _exit;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pContextPolicy->pEntries); ++i) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pInfo->pContextPolicy->pEntries, i);
    if (tFetchPolicyEntrySelected(pInfo, pEntry, needStreamRtInfo, bindingOnly) &&
        taosArrayPush(pProjectedPolicy->pEntries, pEntry) == NULL) {
      code = terrno;
      goto _exit;
    }
  }
  TAOS_CHECK_GOTO(tValidateStreamContextPolicy(pProjectedPolicy), NULL, _exit);
  TAOS_CHECK_GOTO(tProjectFetchAncestorContext(pInfo, pProjectedPolicy,
                                               pInfo->isMultiGroupCalc && effectiveNeedStreamGrpInfo, ppContext),
                  NULL, _exit);
  if (pInfo->isMultiGroupCalc && !needStreamRtInfo) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pProjectedPolicy->pEntries, 0);
    if (taosArrayGetSize(pProjectedPolicy->pEntries) != 1 || pEntry == NULL || pEntry->gid != pInfo->groupId ||
        pEntry->paramIndex != pInfo->curIdx) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    const int32_t bindingCount = taosArrayGetSize(*ppContext == NULL ? NULL : (*ppContext)->pReadScopeBindings);
    if (pEntry->contextPolicy == STREAM_CONTEXT_POLICY_ANCESTOR && bindingCount != 1) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
  }
  TAOS_CHECK_GOTO(tAdmitStreamContext(pProjectedPolicy, *ppContext, true), NULL, _exit);
  *ppPolicy = pProjectedPolicy;
  pProjectedPolicy = NULL;

_exit:
  if (code != TSDB_CODE_SUCCESS) tDestroyStreamAncestorContext(ppContext);
  tDestroyStreamContextPolicy(&pProjectedPolicy);
  return code;
}

static int32_t tGetContextPolicyCalcParam(const SSTriggerCalcRequest* pReq, int64_t gid, int32_t paramIndex,
                                          const SSTriggerCalcParam** ppParam) {
  *ppParam = NULL;
  if (paramIndex < 0) return TSDB_CODE_INVALID_PARA;
  SArray* pParams = NULL;
  if (!pReq->isMultiGroupCalc) {
    if (pReq->gid != gid) return TSDB_CODE_INVALID_PARA;
    pParams = pReq->params;
  } else {
    SSTriggerGroupCalcInfo* pInfo = tSimpleHashGet(pReq->pGroupCalcInfos, &gid, sizeof(gid));
    if (pInfo == NULL) return TSDB_CODE_INVALID_PARA;
    pParams = pInfo->pParams;
  }
  if (paramIndex >= taosArrayGetSize(pParams)) return TSDB_CODE_INVALID_PARA;
  *ppParam = taosArrayGet(pParams, paramIndex);
  return *ppParam == NULL ? TSDB_CODE_INVALID_PARA : TSDB_CODE_SUCCESS;
}

static int32_t tValidateContextPolicyParamArray(const SStreamContextPolicy* pPolicy, int64_t gid,
                                                const SArray* pParams) {
  for (int32_t i = 0; i < taosArrayGetSize(pParams); ++i) {
    if (tFindStreamContextPolicyEntry(pPolicy, gid, i) == NULL) return TSDB_CODE_INVALID_PARA;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tValidateSTriggerCalcRequestAncestorContext(const SSTriggerCalcRequest* pReq, bool nested) {
  if (pReq == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t code = tAdmitStreamContext(pReq->pContextPolicy, pReq->pAncestorContext, nested);
  if (code != TSDB_CODE_SUCCESS || !nested) return code;

  for (int32_t i = 0; i < taosArrayGetSize(pReq->pContextPolicy->pEntries); ++i) {
    const SStreamContextPolicyEntry* pEntry = taosArrayGet(pReq->pContextPolicy->pEntries, i);
    const SSTriggerCalcParam*        pParam = NULL;
    if (tGetContextPolicyCalcParam(pReq, pEntry->gid, pEntry->paramIndex, &pParam) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  if (!pReq->isMultiGroupCalc) {
    if (pReq->pAncestorContext != NULL && taosArrayGetSize(pReq->pAncestorContext->pReadScopeBindings) != 0) {
      return TSDB_CODE_INVALID_PARA;
    }
    code = tValidateContextPolicyParamArray(pReq->pContextPolicy, pReq->gid, pReq->params);
    if (code != TSDB_CODE_SUCCESS) return code;
  } else {
    int32_t                 iter = 0;
    SSTriggerGroupCalcInfo* pInfo = tSimpleHashIterate(pReq->pGroupCalcInfos, NULL, &iter);
    while (pInfo != NULL) {
      int64_t* pGid = tSimpleHashGetKey(pInfo, NULL);
      code = tValidateContextPolicyParamArray(pReq->pContextPolicy, *pGid, pInfo->pParams);
      if (code != TSDB_CODE_SUCCESS) return code;
      pInfo = tSimpleHashIterate(pReq->pGroupCalcInfos, pInfo, &iter);
    }
    code = tValidateGroupReadBindings(pReq->pGroupReadInfos, pReq->pAncestorContext);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  return TSDB_CODE_SUCCESS;
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
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->isMultiGroupCalc));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->stbPartByTbname));

  if (!pReq->isMultiGroupCalc) {
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->gid));
    TAOS_CHECK_EXIT(tSerializeSTriggerCalcParam(&encoder, pReq->params, false, true));
    TAOS_CHECK_EXIT(tSerializeStriggerGroupColVals(&encoder, pReq->groupColVals, -1));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->createTable));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->rollupTbCount));
  } else {
    int32_t nGroups = tSimpleHashGetSize(pReq->pGroupCalcInfos);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, nGroups));
    int32_t                 iter1 = 0;
    SSTriggerGroupCalcInfo* pCalcInfo = tSimpleHashIterate(pReq->pGroupCalcInfos, NULL, &iter1);
    while (pCalcInfo != NULL) {
      int64_t* gid = tSimpleHashGetKey(pCalcInfo, NULL);
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, *gid));
      TAOS_CHECK_EXIT(tSerializeSSTriggerGroupCalcInfo(&encoder, pCalcInfo));
      pCalcInfo = tSimpleHashIterate(pReq->pGroupCalcInfos, pCalcInfo, &iter1);
    }

    int32_t nVnodes = tSimpleHashGetSize(pReq->pGroupReadInfos);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, nVnodes));
    int32_t iter2 = 0;
    void*   px = tSimpleHashIterate(pReq->pGroupReadInfos, NULL, &iter2);
    while (px != NULL) {
      int32_t* vgId = tSimpleHashGetKey(px, NULL);
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, *vgId));
      SArray* pInfos = *(SArray**)px;
      int32_t nGroups = taosArrayGetSize(pInfos);
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, nGroups));
      for (int32_t i = 0; i < nGroups; ++i) {
        SSTriggerGroupReadInfo* pReadInfo = TARRAY_GET_ELEM(pInfos, i);
        TAOS_CHECK_EXIT(tSerializeSSTriggerGroupReadInfo(&encoder, pReadInfo));
      }
      px = tSimpleHashIterate(pReq->pGroupReadInfos, px, &iter2);
    }
  }

  TAOS_CHECK_EXIT(tEncodeBool(&encoder, pReq->isWindowTrigger));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->precision));

  if (pReq->pContextPolicy != NULL || pReq->pAncestorContext != NULL) {
    TAOS_CHECK_EXIT(tValidateSTriggerCalcRequestAncestorContext(pReq, true));
    TAOS_CHECK_EXIT(tStartEncodeStreamTailFrame(&encoder, STREAM_CONTEXT_POLICY_FRAME_MAGIC,
                                                STREAM_CONTEXT_POLICY_FRAME_VERSION, 0));
    TAOS_CHECK_EXIT(tEncodeStreamContextPolicy(&encoder, pReq->pContextPolicy));
    tEndEncodeStreamTailFrame(&encoder);
  }
  if (pReq->pAncestorContext != NULL) {
    TAOS_CHECK_EXIT(
        tStartEncodeStreamTailFrame(&encoder, STREAM_ANCESTOR_FRAME_MAGIC, STREAM_ANCESTOR_FRAME_VERSION, 0));
    TAOS_CHECK_EXIT(tEncodeStreamAncestorContext(&encoder, pReq->pAncestorContext));
    tEndEncodeStreamTailFrame(&encoder);
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

static int32_t tDeserializeSSTriggerGroupCalcInfo(SDecoder* pDecoder, SSTriggerGroupCalcInfo* pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(tDeserializeSTriggerCalcParam(pDecoder, &pInfo->pParams, false));
  TAOS_CHECK_EXIT(tDeserializeStriggerGroupColVals(pDecoder, &pInfo->pGroupColVals));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pInfo->createTable));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pInfo->rollupTbCount));

_exit:
  return code;
}

static int32_t tDeserializeSSTriggerGroupReadInfo(SDecoder* pDecoder, SSTriggerGroupReadInfo* pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pInfo->gid));
  int64_t plainFieldSize = offsetof(SSTriggerCalcParam, notifyType);
  TAOS_MEMCPY(&pInfo->firstParam, pDecoder->data + pDecoder->pos, plainFieldSize);
  pDecoder->pos += plainFieldSize;
  TAOS_MEMCPY(&pInfo->lastParam, pDecoder->data + pDecoder->pos, plainFieldSize);
  pDecoder->pos += plainFieldSize;

  int32_t nTables = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &nTables));
  if (nTables > 0) {
    pInfo->pTables = taosArrayInit_s(sizeof(int64_t), nTables);
    QUERY_CHECK_NULL(pInfo->pTables, code, lino, _exit, terrno);
    TAOS_MEMCPY(pInfo->pTables->pData, pDecoder->data + pDecoder->pos, nTables * sizeof(int64_t));
  }
  pDecoder->pos += nTables * sizeof(int64_t);

_exit:
  return code;
}

int32_t tDeserializeSTriggerCalcRequest(void* buf, int32_t bufLen, SSTriggerCalcRequest* pReq) {
  SDecoder                decoder = {0};
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SStreamContextPolicy*   pContextPolicy = NULL;
  SStreamAncestorContext* pAncestorContext = NULL;

  tDecoderInit(&decoder, buf, bufLen);
  pReq->progressStepId = 0;
  pReq->progressRequestToken = 0;
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->streamId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->runnerTaskId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->sessionId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->triggerType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->isMultiGroupCalc));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->stbPartByTbname));

  if (!pReq->isMultiGroupCalc) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->gid));
    TAOS_CHECK_EXIT(tDeserializeSTriggerCalcParam(&decoder, &pReq->params, false));
    TAOS_CHECK_EXIT(tDeserializeStriggerGroupColVals(&decoder, &pReq->groupColVals));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->createTable));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->rollupTbCount));
  } else {
    pReq->pGroupCalcInfos = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    QUERY_CHECK_NULL(pReq->pGroupCalcInfos, code, lino, _exit, terrno);
    tSimpleHashSetFreeFp(pReq->pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
    int32_t nGroups = 0;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nGroups));
    for (int32_t i = 0; i < nGroups; i++) {
      int64_t gid = 0;
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &gid));
      SSTriggerGroupCalcInfo info = {0};
      TAOS_CHECK_EXIT(tSimpleHashPut(pReq->pGroupCalcInfos, &gid, sizeof(int64_t), &info, sizeof(info)));
      SSTriggerGroupCalcInfo* pCalcInfo = tSimpleHashGet(pReq->pGroupCalcInfos, &gid, sizeof(int64_t));
      QUERY_CHECK_NULL(pCalcInfo, code, lino, _exit, TSDB_CODE_INTERNAL_ERROR);
      TAOS_CHECK_EXIT(tDeserializeSSTriggerGroupCalcInfo(&decoder, pCalcInfo));
    }

    int32_t nVnodes = 0;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nVnodes));
    pReq->pGroupReadInfos = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    QUERY_CHECK_NULL(pReq->pGroupReadInfos, code, lino, _exit, terrno);
    tSimpleHashSetFreeFp(pReq->pGroupReadInfos, tDestroySSTriggerGroupReadInfoArray);
    for (int32_t i = 0; i < nVnodes; i++) {
      int32_t vgId = 0;
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &vgId));
      int32_t nGroups = 0;
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nGroups));
      SArray* pInfos = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), nGroups);
      QUERY_CHECK_NULL(pInfos, code, lino, _exit, terrno);
      code = tSimpleHashPut(pReq->pGroupReadInfos, &vgId, sizeof(int32_t), &pInfos, POINTER_BYTES);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pInfos);
        TAOS_CHECK_EXIT(code);
      }
      for (int32_t j = 0; j < nGroups; ++j) {
        SSTriggerGroupReadInfo* pReadInfo = TARRAY_GET_ELEM(pInfos, j);
        TAOS_CHECK_EXIT(tDeserializeSSTriggerGroupReadInfo(&decoder, pReadInfo));
      }
    }
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeBool(&decoder, &pReq->isWindowTrigger));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->precision));
  }

  bool policySeen = false;
  bool ancestorSeen = false;
  while (!tDecodeIsEnd(&decoder)) {
    SStreamTailFrameDecoder frame = {0};
    TAOS_CHECK_EXIT(tDecodeNextStreamTailFrame(&decoder, &frame));
    if (frame.magic == STREAM_CONTEXT_POLICY_FRAME_MAGIC) {
      if (policySeen || ancestorSeen || frame.version != STREAM_CONTEXT_POLICY_FRAME_VERSION || frame.flags != 0) {
        code = TSDB_CODE_INVALID_MSG;
        tFinishDecodeStreamTailFrame(&frame, false);
        goto _exit;
      }
      policySeen = true;
      code = tDecodeStreamContextPolicy(&frame.payloadDecoder, &pContextPolicy);
      if (code == TSDB_CODE_SUCCESS)
        code = tFinishDecodeStreamTailFrame(&frame, true);
      else
        tFinishDecodeStreamTailFrame(&frame, false);
      TAOS_CHECK_EXIT(code);
      continue;
    }
    if (frame.magic != STREAM_ANCESTOR_FRAME_MAGIC) {
      TAOS_CHECK_EXIT(tFinishDecodeStreamTailFrame(&frame, false));
      continue;
    }
    if (ancestorSeen || frame.version != STREAM_ANCESTOR_FRAME_VERSION || frame.flags != 0) {
      code = TSDB_CODE_INVALID_MSG;
      tFinishDecodeStreamTailFrame(&frame, false);
      goto _exit;
    }
    ancestorSeen = true;
    code = tDecodeStreamAncestorContext(&frame.payloadDecoder, &pAncestorContext);
    if (code == TSDB_CODE_SUCCESS)
      code = tFinishDecodeStreamTailFrame(&frame, true);
    else
      tFinishDecodeStreamTailFrame(&frame, false);
    TAOS_CHECK_EXIT(code);
  }
  pReq->pContextPolicy = pContextPolicy;
  pContextPolicy = NULL;
  pReq->pAncestorContext = pAncestorContext;
  pAncestorContext = NULL;

  tEndDecode(&decoder);

_exit:
  tDestroyStreamContextPolicy(&pContextPolicy);
  tDestroyStreamAncestorContext(&pAncestorContext);
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
    if (pReq->pGroupCalcInfos != NULL) {
      tSimpleHashCleanup(pReq->pGroupCalcInfos);
      pReq->pGroupCalcInfos = NULL;
    }
    if (pReq->pGroupReadInfos != NULL) {
      tSimpleHashCleanup(pReq->pGroupReadInfos);
      pReq->pGroupReadInfos = NULL;
    }
    tDestroyStreamContextPolicy(&pReq->pContextPolicy);
    tDestroyStreamAncestorContext(&pReq->pAncestorContext);
    blockDataDestroy(pReq->pOutBlock);
    pReq->pOutBlock = NULL;
    pReq->progressStepId = 0;
    pReq->progressRequestToken = 0;
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

int32_t tSerializeStRtFuncInfo(SEncoder* pEncoder, const SStreamRuntimeFuncInfo* pInfo, bool needStreamRtInfo, bool needStreamGrpInfo) {
  int32_t code = 0, lino = 0;
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pInfo->isMultiGroupCalc));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pInfo->stbPartByTbname));
  TAOS_CHECK_EXIT(tEncodeBool(pEncoder, needStreamRtInfo));
  TAOS_CHECK_EXIT(tEncodeBool(pEncoder, needStreamGrpInfo));
  if (pInfo->isMultiGroupCalc) {
    if (needStreamRtInfo) {
      if (needStreamGrpInfo) {
        int32_t nGroups = taosArrayGetSize(pInfo->curGrpRead);
        TAOS_CHECK_EXIT(tEncodeI32(pEncoder, nGroups));
        int8_t withGrpCalcInfo = 1;
        for (int32_t i = 0; i < nGroups; ++i) {
          SSTriggerGroupReadInfo* pReadInfo = TARRAY_GET_ELEM(pInfo->curGrpRead, i);
          TAOS_CHECK_EXIT(tSerializeSSTriggerGroupReadInfo(pEncoder, pReadInfo));
          if (0 == i) {
            withGrpCalcInfo = (taosArrayGetSize(pReadInfo->pTables) <= 0);
          }
        }
        
        TAOS_CHECK_EXIT(tEncodeI8(pEncoder, withGrpCalcInfo));
        if (withGrpCalcInfo) {
          int32_t nGroups = tSimpleHashGetSize(pInfo->pGroupCalcInfos);
          TAOS_CHECK_EXIT(tEncodeI32(pEncoder, nGroups));
          int32_t                 iter1 = 0;
          SSTriggerGroupCalcInfo* pCalcInfo = tSimpleHashIterate(pInfo->pGroupCalcInfos, NULL, &iter1);
          while (pCalcInfo != NULL) {
            int64_t* gid = tSimpleHashGetKey(pCalcInfo, NULL);
            TAOS_CHECK_EXIT(tEncodeI64(pEncoder, *gid));
            TAOS_CHECK_EXIT(tSerializeSSTriggerGroupCalcInfo(pEncoder, pCalcInfo));
            pCalcInfo = tSimpleHashIterate(pInfo->pGroupCalcInfos, pCalcInfo, &iter1);
          }
        }
      } else {
        int32_t nGroups = tSimpleHashGetSize(pInfo->pGroupCalcInfos);
        TAOS_CHECK_EXIT(tEncodeI32(pEncoder, nGroups));
        int32_t                 iter1 = 0;
        SSTriggerGroupCalcInfo* pCalcInfo = tSimpleHashIterate(pInfo->pGroupCalcInfos, NULL, &iter1);
        while (pCalcInfo != NULL) {
          int64_t* gid = tSimpleHashGetKey(pCalcInfo, NULL);
          TAOS_CHECK_EXIT(tEncodeI64(pEncoder, *gid));
          TAOS_CHECK_EXIT(tSerializeSSTriggerGroupCalcInfo(pEncoder, pCalcInfo));
          pCalcInfo = tSimpleHashIterate(pInfo->pGroupCalcInfos, pCalcInfo, &iter1);
        }

        int32_t nVnodes = tSimpleHashGetSize(pInfo->pGroupReadInfos);
        TAOS_CHECK_EXIT(tEncodeI32(pEncoder, nVnodes));
        int32_t iter2 = 0;
        void*   px = tSimpleHashIterate(pInfo->pGroupReadInfos, NULL, &iter2);
        while (px != NULL) {
          int32_t* vgId = tSimpleHashGetKey(px, NULL);
          TAOS_CHECK_EXIT(tEncodeI32(pEncoder, *vgId));
          SArray* pInfos = *(SArray**)px;
          int32_t nGroups = taosArrayGetSize(pInfos);
          TAOS_CHECK_EXIT(tEncodeI32(pEncoder, nGroups));
          for (int32_t i = 0; i < nGroups; ++i) {
            SSTriggerGroupReadInfo* pReadInfo = TARRAY_GET_ELEM(pInfos, i);
            TAOS_CHECK_EXIT(tSerializeSSTriggerGroupReadInfo(pEncoder, pReadInfo));
          }
          px = tSimpleHashIterate(pInfo->pGroupReadInfos, px, &iter2);
        }
      }
    }
  } else {
    TAOS_CHECK_EXIT(tSerializeSTriggerCalcParam(pEncoder, pInfo->pStreamPesudoFuncVals, true, needStreamRtInfo));
    TAOS_CHECK_EXIT(tSerializeStriggerGroupColVals(pEncoder, pInfo->pStreamPartColVals, -1));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pInfo->groupId));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pInfo->rollupTbCount));
  }

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pInfo->curWindow.skey));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pInfo->curWindow.ekey));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pInfo->curIdx));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pInfo->sessionId));
  TAOS_CHECK_EXIT(tEncodeBool(pEncoder, pInfo->withExternalWindow));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pInfo->triggerType));
  TAOS_CHECK_EXIT(tEncodeBool(pEncoder, pInfo->isWindowTrigger));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pInfo->precision));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pInfo->streamGen));
_exit:
  return code;
}

int32_t tDeserializeStRtFuncInfo(SDecoder* pDecoder, SStreamRuntimeFuncInfo* pInfo) {
  int32_t code = 0, lino = 0;
  int32_t size = 0;
  bool needStreamRtInfo = false;
  bool needStreamGrpInfo = false;
  
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pInfo->isMultiGroupCalc));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pInfo->stbPartByTbname));
  TAOS_CHECK_EXIT(tDecodeBool(pDecoder, &needStreamRtInfo));
  TAOS_CHECK_EXIT(tDecodeBool(pDecoder, &needStreamGrpInfo));
  
  if (pInfo->isMultiGroupCalc) {
    if (needStreamRtInfo) {
      if (needStreamGrpInfo) {
        int32_t nGroups = 0;
        TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &nGroups));
        if (nGroups > 0) {
          pInfo->curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), nGroups);
          QUERY_CHECK_NULL(pInfo->curGrpRead, code, lino, _exit, terrno);
        }
        for (int32_t j = 0; j < nGroups; ++j) {
          SSTriggerGroupReadInfo* pReadInfo = TARRAY_GET_ELEM(pInfo->curGrpRead, j);
          TAOS_CHECK_EXIT(tDeserializeSSTriggerGroupReadInfo(pDecoder, pReadInfo));
        }
        int8_t withGrpCalcInfo = 0;
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &withGrpCalcInfo));
        if (withGrpCalcInfo) {
          int32_t nGroups = 0;
          TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &nGroups));
          pInfo->pGroupCalcInfos = tSimpleHashInit(nGroups, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
          QUERY_CHECK_NULL(pInfo->pGroupCalcInfos, code, lino, _exit, terrno);
          tSimpleHashSetFreeFp(pInfo->pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);

          for (int32_t i = 0; i < nGroups; i++) {
            int64_t gid = 0;
            TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &gid));
            SSTriggerGroupCalcInfo info = {0};
            TAOS_CHECK_EXIT(tSimpleHashPut(pInfo->pGroupCalcInfos, &gid, sizeof(int64_t), &info, sizeof(info)));
            SSTriggerGroupCalcInfo* pCalcInfo = tSimpleHashGet(pInfo->pGroupCalcInfos, &gid, sizeof(int64_t));
            QUERY_CHECK_NULL(pCalcInfo, code, lino, _exit, TSDB_CODE_INTERNAL_ERROR);
            TAOS_CHECK_EXIT(tDeserializeSSTriggerGroupCalcInfo(pDecoder, pCalcInfo));
          }
        }
      } else {
        int32_t nGroups = 0;
        TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &nGroups));
        pInfo->pGroupCalcInfos = tSimpleHashInit(nGroups, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
        QUERY_CHECK_NULL(pInfo->pGroupCalcInfos, code, lino, _exit, terrno);
        tSimpleHashSetFreeFp(pInfo->pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);

        for (int32_t i = 0; i < nGroups; i++) {
          int64_t gid = 0;
          TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &gid));
          SSTriggerGroupCalcInfo info = {0};
          TAOS_CHECK_EXIT(tSimpleHashPut(pInfo->pGroupCalcInfos, &gid, sizeof(int64_t), &info, sizeof(info)));
          SSTriggerGroupCalcInfo* pCalcInfo = tSimpleHashGet(pInfo->pGroupCalcInfos, &gid, sizeof(int64_t));
          QUERY_CHECK_NULL(pCalcInfo, code, lino, _exit, TSDB_CODE_INTERNAL_ERROR);
          TAOS_CHECK_EXIT(tDeserializeSSTriggerGroupCalcInfo(pDecoder, pCalcInfo));
        }

        int32_t nVnodes = 0;
        TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &nVnodes));
        pInfo->pGroupReadInfos = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
        QUERY_CHECK_NULL(pInfo->pGroupReadInfos, code, lino, _exit, terrno);
        tSimpleHashSetFreeFp(pInfo->pGroupReadInfos, tDestroySSTriggerGroupReadInfoArray);
        for (int32_t i = 0; i < nVnodes; i++) {
          int32_t vgId = 0;
          TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &vgId));
          int32_t nGroups = 0;
          TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &nGroups));
          SArray* pInfos = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), nGroups);
          QUERY_CHECK_NULL(pInfos, code, lino, _exit, terrno);
          code = tSimpleHashPut(pInfo->pGroupReadInfos, &vgId, sizeof(int32_t), &pInfos, POINTER_BYTES);
          if (code != TSDB_CODE_SUCCESS) {
            taosArrayDestroy(pInfos);
            TAOS_CHECK_EXIT(code);
          }
          for (int32_t j = 0; j < nGroups; ++j) {
            SSTriggerGroupReadInfo* pReadInfo = TARRAY_GET_ELEM(pInfos, j);
            TAOS_CHECK_EXIT(tDeserializeSSTriggerGroupReadInfo(pDecoder, pReadInfo));
          }
        }
      }
    }
  } else {
    TAOS_CHECK_EXIT(tDeserializeSTriggerCalcParam(pDecoder, &pInfo->pStreamPesudoFuncVals, true));
    TAOS_CHECK_EXIT(tDeserializeStriggerGroupColVals(pDecoder, &pInfo->pStreamPartColVals));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pInfo->groupId));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pInfo->rollupTbCount));
  }

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pInfo->curWindow.skey));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pInfo->curWindow.ekey));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pInfo->curIdx));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pInfo->sessionId));
  TAOS_CHECK_EXIT(tDecodeBool(pDecoder, &pInfo->withExternalWindow));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pInfo->triggerType));
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_EXIT(tDecodeBool(pDecoder, &pInfo->isWindowTrigger));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pInfo->precision));
  }
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pInfo->streamGen));
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
  if (pInfo->pGroupCalcInfos != NULL) {
    tSimpleHashCleanup(pInfo->pGroupCalcInfos);
    pInfo->pGroupCalcInfos = NULL;
  }
  if (pInfo->pGroupReadInfos != NULL) {
    tSimpleHashCleanup(pInfo->pGroupReadInfos);
    pInfo->pGroupReadInfos = NULL;
  }
  tDestroyStreamContextPolicy(&pInfo->pContextPolicy);
  tDestroyStreamAncestorContext(&pInfo->pAncestorContext);
  if (pInfo->outNormalTable != NULL) {
    taosMemoryFreeClear(pInfo->outNormalTable);
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
    TAOS_CHECK_EXIT(tDecodeSColRefWrapperEx(&decoder, &info->cols));
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
    int64_t uid = pInfo->uid;
    if (uid == 0) {
      uid = *(int64_t*)(tSimpleHashGetKey(pe, NULL));
    }
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
 
static int32_t encodeBlock(SEncoder* encoder, void* block, SSHashObj* indexHash) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  if (block != NULL && ((SSDataBlock*)block)->info.rows > 0) {
    TAOS_CHECK_EXIT(tEncodeI8(encoder, 1));
    TAOS_CHECK_EXIT(encodeData(encoder, block, indexHash));
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(encoder, 0));
  }

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

  TAOS_CHECK_EXIT(encodeBlock(&encoder, rsp->dataBlock, rsp->indexHash));
  TAOS_CHECK_EXIT(encodeBlock(&encoder, rsp->metaBlock, NULL));
  TAOS_CHECK_EXIT(encodeBlock(&encoder, rsp->deleteBlock, NULL));
  TAOS_CHECK_EXIT(encodeBlock(&encoder, rsp->tableBlock, NULL));

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

static int32_t decodeBlock(SDecoder* decoder, void* pBlock) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  
  int8_t hasData = false;
  TAOS_CHECK_EXIT(tDecodeI8(decoder, &hasData));
  if (hasData) {
    TAOS_CHECK_EXIT(pBlock != NULL ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA);
    const char* pEndPos = NULL;
    TAOS_CHECK_EXIT(blockDecode(pBlock, (char*)decoder->data + decoder->pos, &pEndPos));
    decoder->pos = (uint8_t*)pEndPos - decoder->data;
  } else if (pBlock != NULL) {
    blockDataEmpty(pBlock);
  }

_exit:
  return code;
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

  TAOS_CHECK_EXIT(decodeBlock(&decoder, pRsp->metaBlock));
  TAOS_CHECK_EXIT(decodeBlock(&decoder, pRsp->deleteBlock));
  TAOS_CHECK_EXIT(decodeBlock(&decoder, pRsp->tableBlock));
  
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->ver));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->verTime));

  TAOS_CHECK_EXIT(tDecodeIsEnd(&decoder) ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_MSG);
  tEndDecode(&decoder);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  tDecoderClear(&decoder);
  return code;
}

// ========== SVTableRefResolve serde ==========

int32_t tSerializeSVTableRefResolveReq(void *buf, int32_t bufLen, const SVTableRefResolveReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino = 0;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->ver));

  // Table-grouped format: each group = (dbName, tableName, cols[])
  int32_t nGroups = (pReq->groups != NULL) ? taosArrayGetSize(pReq->groups) : 0;
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, nGroups));
  for (int32_t i = 0; i < nGroups; ++i) {
    SVTableRefResolveGroupItem *g = taosArrayGet(pReq->groups, i);
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, g->dbName));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, g->tableName));
    int32_t nCols = (g->cols != NULL) ? taosArrayGetSize(g->cols) : 0;
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, nCols));
    for (int32_t j = 0; j < nCols; ++j) {
      SVTableRefResolveColSpec *c = taosArrayGet(g->cols, j);
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, c->colName));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, c->kind));
    }
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return -1;
  } else {
    int32_t tlen = encoder.pos;
    tEncoderClear(&encoder);
    return tlen;
  }
}

int32_t tDeserializeSVTableRefResolveReq(void *buf, int32_t bufLen, SVTableRefResolveReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino = 0;
  SArray  *pendingCols = NULL;  // track cols not yet pushed to groups

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->ver));

  // Table-grouped format
  int32_t nGroups = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nGroups));
  if (nGroups > 0) {
    pReq->groups = taosArrayInit(nGroups, sizeof(SVTableRefResolveGroupItem));
    if (pReq->groups == NULL) {
      code = terrno;
      goto _exit;
    }
  }

  for (int32_t i = 0; i < nGroups; ++i) {
    SVTableRefResolveGroupItem g = {0};
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, g.dbName));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, g.tableName));
    int32_t nCols = 0;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nCols));
    if (nCols > 0) {
      g.cols = taosArrayInit(nCols, sizeof(SVTableRefResolveColSpec));
      if (g.cols == NULL) {
        code = terrno;
        goto _exit;
      }
    }
    pendingCols = g.cols;  // track in case we fail before push
    for (int32_t j = 0; j < nCols; ++j) {
      SVTableRefResolveColSpec c = {0};
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, c.colName));
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &c.kind));
      if (taosArrayPush(g.cols, &c) == NULL) {
        code = terrno;
        goto _exit;
      }
    }
    if (taosArrayPush(pReq->groups, &g) == NULL) {
      if (g.cols != NULL) taosArrayDestroy(g.cols);
      pendingCols = NULL;
      code = terrno;
      goto _exit;
    }
    pendingCols = NULL;  // ownership transferred to groups
  }

  tEndDecode(&decoder);

_exit:
  if (code) {
    // Free cols array that was allocated but never pushed to groups.
    taosArrayDestroy(pendingCols);
    if (pReq->groups != NULL) {
      for (int32_t i = 0; i < taosArrayGetSize(pReq->groups); ++i) {
        SVTableRefResolveGroupItem *g = taosArrayGet(pReq->groups, i);
        taosArrayDestroy(g->cols);
      }
      taosArrayDestroy(pReq->groups);
      pReq->groups = NULL;
    }
    tDecoderClear(&decoder);
    return -1;
  }
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSVTableRefResolveReq(SVTableRefResolveReq *pReq) {
  if (pReq == NULL) return;
  if (pReq->groups != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pReq->groups); ++i) {
      SVTableRefResolveGroupItem *g = taosArrayGet(pReq->groups, i);
      taosArrayDestroy(g->cols);
    }
    taosArrayDestroy(pReq->groups);
    pReq->groups = NULL;
  }
}

int32_t tSerializeSVTableRefResolveRsp(void *buf, int32_t bufLen, const SVTableRefResolveRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino = 0;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t n = (pRsp->items != NULL) ? taosArrayGetSize(pRsp->items) : 0;
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, n));

  for (int32_t i = 0; i < n; ++i) {
    SVTableRefResolveRspItem *p = taosArrayGet(pRsp->items, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, p->code));
    TAOS_CHECK_EXIT(tEncodeI8 (&encoder, p->terminated));
    TAOS_CHECK_EXIT(tEncodeI8 (&encoder, p->nextRef.kind));
    TAOS_CHECK_EXIT(tEncodeBool(&encoder, p->nextRef.hasRef));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, p->nextRef.refDbName));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, p->nextRef.refTableName));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, p->nextRef.refColName));
    TAOS_CHECK_EXIT(tEncodeI8 (&encoder, p->tagType));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, p->tagLen));

    // Only write tagData when terminated=true AND kind=TAG
    if (p->terminated && p->nextRef.kind == STREAM_VREF_KIND_TAG && p->tagLen > 0) {
      TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t*)p->tagData, p->tagLen));
    }
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return -1;
  } else {
    int32_t tlen = encoder.pos;
    tEncoderClear(&encoder);
    return tlen;
  }
}

int32_t tDeserializeSVTableRefResolveRsp(void *buf, int32_t bufLen, SVTableRefResolveRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino = 0;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t n = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &n));
  if (n > 0) {
    pRsp->items = taosArrayInit(n, sizeof(SVTableRefResolveRspItem));
    if (pRsp->items == NULL) {
      code = terrno;
      goto _exit;
    }
  }

  for (int32_t i = 0; i < n; ++i) {
    SVTableRefResolveRspItem item = {0};
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &item.code));
    TAOS_CHECK_EXIT(tDecodeI8 (&decoder, (int8_t*)&item.terminated));
    TAOS_CHECK_EXIT(tDecodeI8 (&decoder, &item.nextRef.kind));
    TAOS_CHECK_EXIT(tDecodeBool(&decoder, &item.nextRef.hasRef));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, item.nextRef.refDbName));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, item.nextRef.refTableName));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, item.nextRef.refColName));
    TAOS_CHECK_EXIT(tDecodeI8 (&decoder, &item.tagType));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &item.tagLen));

    // Only read tagData when terminated=true AND kind=TAG AND tagLen>0
    if (item.terminated && item.nextRef.kind == STREAM_VREF_KIND_TAG && item.tagLen > 0) {
      item.tagData = taosMemoryMalloc(item.tagLen);
      if (item.tagData == NULL) {
        code = terrno;
        goto _exit;
      }
      // Free the just-allocated tagData on decode failure: the stack-local
      // `item` is not in pRsp->items yet, so the _exit cleanup loop would
      // otherwise miss it and leak this buffer.
      code = tDecodeBinaryTo(&decoder, (uint8_t*)item.tagData, item.tagLen);
      if (code) {
        taosMemoryFreeClear(item.tagData);
        lino = __LINE__;
        goto _exit;
      }
    } else {
      item.tagData = NULL;
    }

    if (taosArrayPush(pRsp->items, &item) == NULL) {
      if (item.tagData != NULL) {
        taosMemoryFree(item.tagData);
      }
      code = terrno;
      goto _exit;
    }
  }

  tEndDecode(&decoder);

_exit:
  if (code) {
    if (pRsp->items != NULL) {
      for (int32_t i = 0; i < taosArrayGetSize(pRsp->items); ++i) {
        SVTableRefResolveRspItem *p = taosArrayGet(pRsp->items, i);
        taosMemoryFreeClear(p->tagData);
      }
      taosArrayDestroy(pRsp->items);
      pRsp->items = NULL;
    }
    tDecoderClear(&decoder);
    return -1;
  }
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSVTableRefResolveRsp(SVTableRefResolveRsp *pRsp) {
  if (pRsp == NULL) return;
  if (pRsp->items != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pRsp->items); ++i) {
      SVTableRefResolveRspItem *p = taosArrayGet(pRsp->items, i);
      taosMemoryFreeClear(p->tagData);
    }
    taosArrayDestroy(pRsp->items);
    pRsp->items = NULL;
  }
}

int32_t tSerializeGetStreamCreateSqlReq(void* buf, int32_t bufLen, const SGetStreamCreateSqlReq* pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  int32_t code = 0, lino = 0;
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  tEndEncode(&encoder);
_exit:
  if (code) uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  int32_t len = encoder.pos;
  tEncoderClear(&encoder);
  return (code != 0) ? code : len;
}

int32_t tDeserializeGetStreamCreateSqlReq(void* buf, int32_t bufLen, SGetStreamCreateSqlReq* pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  int32_t code = 0, lino = 0;
  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  tEndDecode(&decoder);
_exit:
  if (code) uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeGetStreamCreateSqlRsp(void* buf, int32_t bufLen, const SGetStreamCreateSqlRsp* pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  int32_t code = 0, lino = 0;
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->sql ? pRsp->sql : ""));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->triggerDB ? pRsp->triggerDB : ""));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->triggerTblName ? pRsp->triggerTblName : ""));
  tEndEncode(&encoder);
_exit:
  if (code) uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  int32_t len = encoder.pos;
  tEncoderClear(&encoder);
  return (code != 0) ? code : len;
}

int32_t tDeserializeGetStreamCreateSqlRsp(void* buf, int32_t bufLen, SGetStreamCreateSqlRsp* pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  int32_t code = 0, lino = 0;
  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pRsp->sql));
  if (decoder.pos < decoder.size) {
    TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pRsp->triggerDB));
  }
  if (decoder.pos < decoder.size) {
    TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pRsp->triggerTblName));
  }
  tEndDecode(&decoder);
_exit:
  if (code) uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  tDecoderClear(&decoder);
  return code;
}

void tFreeGetStreamCreateSqlRsp(SGetStreamCreateSqlRsp* pRsp) {
  if (pRsp) {
    taosMemoryFreeClear(pRsp->sql);
    taosMemoryFreeClear(pRsp->triggerDB);
    taosMemoryFreeClear(pRsp->triggerTblName);
  }
}

/* ---------------------------------------------------------------------------
 * SSTriggerExtPullRsp serialize / deserialize / destroy
 *
 * Wire format:
 *   I32  pullType
 *   I32  code
 *   I8   hasLastTsArr  [if 1: I32 count + (I64 uid, I64 gid, I64 ts) * count]
 *   I8   hasMetaBlock  [if 1: blockEncode(pMetaBlock)]
 *   I8   hasDataBlock  [if 1: blockEncode(pDataBlock) +
 *                              I32 nIndex + (I64 uid, I32 startRow, I32 rowCount) * nIndex]
 * --------------------------------------------------------------------------- */

int32_t tSerializeSSTriggerExtPullRsp(void* buf, int32_t bufLen, const SSTriggerExtPullRsp* pRsp) {
  SEncoder encoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  tlen = 0;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, (int32_t)pRsp->pullType));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->code));

  /* pLastTsArr */
  int32_t nLastTs = (pRsp->pLastTsArr != NULL) ? (int32_t)taosArrayGetSize(pRsp->pLastTsArr) : 0;
  if (nLastTs > 0) {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 1));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, nLastTs));
    for (int32_t i = 0; i < nLastTs; i++) {
      SExtLastTsInfo* p = taosArrayGet(pRsp->pLastTsArr, i);
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, p->uid));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, p->gid));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, p->ts));
    }
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 0));
  }

  /* pMetaBlock */
  TAOS_CHECK_EXIT(encodeBlock(&encoder, pRsp->pMetaBlock, NULL));

  /* pDataBlock + pIndexHash */
  if (pRsp->pDataBlock != NULL && pRsp->pDataBlock->info.rows > 0) {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 1));
    /* encode data block */
    int32_t len = 0;
    if (encoder.data == NULL) {
      len = blockGetEncodeSize(pRsp->pDataBlock);
    } else {
      len = blockEncode(pRsp->pDataBlock, (char*)(encoder.data + encoder.pos),
                        encoder.size - encoder.pos,
                        (int32_t)blockDataGetNumOfCols(pRsp->pDataBlock));
      if (len < 0) TAOS_CHECK_EXIT(terrno);
    }
    encoder.pos += len;
    /* encode pIndexHash */
    int32_t nIdx = (pRsp->pIndexHash != NULL) ? tSimpleHashGetSize(pRsp->pIndexHash) : 0;
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, nIdx));
    if (nIdx > 0) {
      int32_t iter = 0;
      void*   px = tSimpleHashIterate(pRsp->pIndexHash, NULL, &iter);
      while (px != NULL) {
        int64_t*       uid = tSimpleHashGetKey(px, NULL);
        SExtIndexEntry* e  = (SExtIndexEntry*)px;
        TAOS_CHECK_EXIT(tEncodeI64(&encoder, *uid));
        TAOS_CHECK_EXIT(tEncodeI32(&encoder, e->startRow));
        TAOS_CHECK_EXIT(tEncodeI32(&encoder, e->rowCount));
        px = tSimpleHashIterate(pRsp->pIndexHash, px, &iter);
      }
    }
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, 0));
  }

  /* pGroupColVals */
  TAOS_CHECK_EXIT(tSerializeStriggerGroupColVals(&encoder, pRsp->pGroupColVals, -1));

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

int32_t tDeserializeSSTriggerExtPullRsp(void* buf, int32_t bufLen, SSTriggerExtPullRsp* pRsp) {
  SDecoder decoder = {0};
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t pullType = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pullType));
  pRsp->pullType = (ESTriggerPullType)pullType;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->code));

  /* pLastTsArr */
  int8_t hasLastTs = 0;
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &hasLastTs));
  if (hasLastTs) {
    int32_t nLastTs = 0;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nLastTs));
    pRsp->pLastTsArr = taosArrayInit(nLastTs, sizeof(SExtLastTsInfo));
    if (pRsp->pLastTsArr == NULL) TAOS_CHECK_EXIT(terrno);
    for (int32_t i = 0; i < nLastTs; i++) {
      SExtLastTsInfo info = {0};
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &info.uid));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &info.gid));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &info.ts));
      if (taosArrayPush(pRsp->pLastTsArr, &info) == NULL) TAOS_CHECK_EXIT(terrno);
    }
  }

  /* pMetaBlock */
  int8_t hasMetaBlock = 0;
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &hasMetaBlock));
  if (hasMetaBlock) {
    /* Allocate a bare block (pDataBlock=NULL) so blockDecode's taosArrayInit_s branch fires.
     * createDataBlock pre-initialises pDataBlock to an empty array (size=0), which defeats
     * the NULL check in blockDecodeImpl and causes taosArrayGet to fail on index 0. */
    pRsp->pMetaBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
    if (pRsp->pMetaBlock == NULL) TAOS_CHECK_EXIT(terrno);
    const char* pEnd = NULL;
    TAOS_CHECK_EXIT(blockDecode(pRsp->pMetaBlock, (char*)decoder.data + decoder.pos, &pEnd));
    decoder.pos = (uint8_t*)pEnd - decoder.data;
  }

  /* pDataBlock + pIndexHash */
  int8_t hasDataBlock = 0;
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &hasDataBlock));
  if (hasDataBlock) {
    /* Same reasoning as pMetaBlock: use a bare calloc instead of createDataBlock. */
    pRsp->pDataBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
    if (pRsp->pDataBlock == NULL) TAOS_CHECK_EXIT(terrno);
    const char* pEnd = NULL;
    TAOS_CHECK_EXIT(blockDecode(pRsp->pDataBlock, (char*)decoder.data + decoder.pos, &pEnd));
    decoder.pos = (uint8_t*)pEnd - decoder.data;

    int32_t nIdx = 0;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nIdx));
    if (nIdx > 0) {
      pRsp->pIndexHash = tSimpleHashInit(nIdx, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
      if (pRsp->pIndexHash == NULL) TAOS_CHECK_EXIT(terrno);
      for (int32_t i = 0; i < nIdx; i++) {
        int64_t uid = 0;
        SExtIndexEntry e = {0};
        TAOS_CHECK_EXIT(tDecodeI64(&decoder, &uid));
        TAOS_CHECK_EXIT(tDecodeI32(&decoder, &e.startRow));
        TAOS_CHECK_EXIT(tDecodeI32(&decoder, &e.rowCount));
        TAOS_CHECK_EXIT(tSimpleHashPut(pRsp->pIndexHash, &uid, sizeof(uid), &e, sizeof(e)));
      }
    }
  }

  /* pGroupColVals */
  TAOS_CHECK_EXIT(tDeserializeStriggerGroupColVals(&decoder, &pRsp->pGroupColVals));

  tEndDecode(&decoder);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    tDestroySSTriggerExtPullRsp(pRsp);
  }
  tDecoderClear(&decoder);
  return code;
}

void tDestroySSTriggerExtPullRsp(SSTriggerExtPullRsp* pRsp) {
  if (pRsp == NULL) return;
  taosArrayDestroy(pRsp->pLastTsArr);
  pRsp->pLastTsArr = NULL;
  blockDataDestroy(pRsp->pMetaBlock);
  pRsp->pMetaBlock = NULL;
  blockDataDestroy(pRsp->pDataBlock);
  pRsp->pDataBlock = NULL;
  tSimpleHashCleanup(pRsp->pIndexHash);
  pRsp->pIndexHash = NULL;
  taosArrayDestroyEx(pRsp->pGroupColVals, tDestroySStreamGroupValue);
  pRsp->pGroupColVals = NULL;
}

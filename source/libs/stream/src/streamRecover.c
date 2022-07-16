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

#include "streamInc.h"

int32_t tEncodeStreamTaskRecoverReq(SEncoder* pEncoder, const SStreamTaskRecoverReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->sourceTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->sourceVg) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTaskRecoverReq(SDecoder* pDecoder, SStreamTaskRecoverReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->sourceTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->sourceVg) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamTaskRecoverRsp(SEncoder* pEncoder, const SStreamTaskRecoverRsp* pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->taskId) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->inputStatus) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTaskRecoverRsp(SDecoder* pDecoder, SStreamTaskRecoverRsp* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI8(pDecoder, &pReq->inputStatus) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeSMStreamTaskRecoverReq(SEncoder* pEncoder, const SMStreamTaskRecoverReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSMStreamTaskRecoverReq(SDecoder* pDecoder, SMStreamTaskRecoverReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeSMStreamTaskRecoverRsp(SEncoder* pEncoder, const SMStreamTaskRecoverRsp* pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->taskId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSMStreamTaskRecoverRsp(SDecoder* pDecoder, SMStreamTaskRecoverRsp* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t streamProcessFailRecoverReq(SStreamTask* pTask, SMStreamTaskRecoverReq* pReq, SRpcMsg* pRsp) {
  if (pTask->taskStatus != TASK_STATUS__FAIL) {
    return 0;
  }

  if (pTask->isStreamDistributed) {
    if (pTask->isDataScan) {
      pTask->taskStatus = TASK_STATUS__PREPARE_RECOVER;
    } else if (pTask->execType != TASK_EXEC__NONE) {
      pTask->taskStatus = TASK_STATUS__PREPARE_RECOVER;
      bool    hasCheckpoint = false;
      int32_t childSz = taosArrayGetSize(pTask->childEpInfo);
      for (int32_t i = 0; i < childSz; i++) {
        SStreamChildEpInfo* pEpInfo = taosArrayGetP(pTask->childEpInfo, i);
        if (pEpInfo->checkpointVer == -1) {
          hasCheckpoint = true;
          break;
        }
      }
      if (hasCheckpoint) {
        // load from checkpoint
      } else {
        // recover child
      }
    }
  } else {
    if (pTask->isDataScan) {
      if (pTask->checkpointVer != -1) {
        // load from checkpoint
      } else {
        // reset stream query task info
        // TODO get snapshot ver
        pTask->recoverSnapVer = -1;
        qStreamPrepareRecover(pTask->exec.executor, pTask->startVer, pTask->recoverSnapVer);
        pTask->taskStatus = TASK_STATUS__RECOVERING;
      }
    }
  }

  if (pTask->taskStatus == TASK_STATUS__RECOVERING) {
    if (streamPipelineExec(pTask, 10) < 0) {
      // set fail
      return -1;
    }
  }

  return 0;
}

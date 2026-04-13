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

#include "clientTmq.h"
#include "taos.h"
#include "tmsg.h"

// ============================================================
// tmq_committed callback
// ============================================================
static int32_t tmCommittedCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (param == NULL) {
    return code;
  }
  SMqCommittedParam* pParam = param;

  if (code != 0) {
    goto end;
  }
  if (pMsg) {
    SDecoder decoder = {0};
    tDecoderInit(&decoder, (uint8_t*)pMsg->pData, pMsg->len);
    int32_t err = tDecodeMqVgOffset(&decoder, &pParam->vgOffset);
    if (err < 0) {
      tOffsetDestroy(&pParam->vgOffset.offset);
      code = err;
      goto end;
    }
    tDecoderClear(&decoder);
  }

  end:
  if (pMsg) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
  }
  pParam->code = code;
  if (tsem2_post(&pParam->sem) != 0){
    tqErrorC("failed to post semaphore in tmCommittedCb");
  }
  return code;
}

// ============================================================
// get committed from server
// ============================================================
int32_t getCommittedFromServer(tmq_t* tmq, char* tname, int32_t vgId, SEpSet* epSet, int64_t* committed) {
  if (tmq == NULL || tname == NULL || epSet == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t     code = 0;
  SMqVgOffset pOffset = {0};

  pOffset.consumerId = tmq->consumerId;
  (void)snprintf(pOffset.offset.subKey, TSDB_SUBSCRIBE_KEY_LEN, "%s%s%s", tmq->groupId, TMQ_SEPARATOR, tname);

  int32_t len = 0;
  tEncodeSize(tEncodeMqVgOffset, &pOffset, len, code);
  if (code < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  void* buf = taosMemoryCalloc(1, sizeof(SMsgHead) + len);
  if (buf == NULL) {
    return terrno;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);
  code = tEncodeMqVgOffset(&encoder, &pOffset);
  if (code < 0) {
    taosMemoryFree(buf);
    tEncoderClear(&encoder);
    return code;
  }
  tEncoderClear(&encoder);

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(buf);
    return terrno;
  }

  SMqCommittedParam* pParam = taosMemoryMalloc(sizeof(SMqCommittedParam));
  if (pParam == NULL) {
    taosMemoryFree(buf);
    taosMemoryFree(sendInfo);
    return terrno;
  }
  if (tsem2_init(&pParam->sem, 0, 0) != 0) {
    taosMemoryFree(buf);
    taosMemoryFree(sendInfo);
    taosMemoryFree(pParam);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = buf, .len = sizeof(SMsgHead) + len, .handle = NULL};
  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmCommittedCb;
  sendInfo->msgType = TDMT_VND_TMQ_VG_COMMITTEDINFO;

  code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, epSet, NULL, sendInfo);
  if (code != 0) {
    if(tsem2_destroy(&pParam->sem) != 0) {
      tqErrorC("failed to destroy semaphore in get committed from server1");
    }
    taosMemoryFree(pParam);
    return code;
  }

  if (tsem2_wait(&pParam->sem) != 0){
    tqErrorC("failed to wait semaphore in get committed from server");
  }
  code = pParam->code;
  if (code == TSDB_CODE_SUCCESS) {
    if (pParam->vgOffset.offset.val.type == TMQ_OFFSET__LOG) {
      *committed = pParam->vgOffset.offset.val.version;
    } else {
      tOffsetDestroy(&pParam->vgOffset.offset);
      code = TSDB_CODE_TMQ_SNAPSHOT_ERROR;
    }
  }
  if(tsem2_destroy(&pParam->sem) != 0) {
    tqErrorC("failed to destroy semaphore in get committed from server2");
  }
  taosMemoryFree(pParam);

  return code;
}

// ============================================================
// tmq_position
// ============================================================
int64_t tmq_position(tmq_t* tmq, const char* pTopicName, int32_t vgId) {
  if (tmq == NULL || pTopicName == NULL) {
    tqErrorC("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  char tname[TSDB_TOPIC_FNAME_LEN] = {0};
  buildTopicFullName(tmq, pTopicName, tname);

  tmqWlock(tmq);

  SMqClientVg* pVg = NULL;
  int32_t      code = getClientVg(tmq, tname, vgId, &pVg);
  if (code != 0) {
    tmqWUnlock(tmq);
    return code;
  }

  SVgOffsetInfo* pOffsetInfo = &pVg->offsetInfo;
  int32_t        type = pOffsetInfo->endOffset.type;
  if (isInSnapshotMode(type, tmq->useSnapshot)) {
    tqErrorC("consumer:0x%" PRIx64 " offset type:%d not wal version, position error", tmq->consumerId, type);
    tmqWUnlock(tmq);
    return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
  }

  code = checkWalRange(pOffsetInfo, -1);
  if (code != 0) {
    tmqWUnlock(tmq);
    return code;
  }
  SEpSet  epSet = pVg->epSet;
  int64_t begin = pVg->offsetInfo.walVerBegin;
  int64_t end = pVg->offsetInfo.walVerEnd;
  tmqWUnlock(tmq);

  int64_t position = 0;
  if (type == TMQ_OFFSET__LOG) {
    position = pOffsetInfo->endOffset.version;
  } else if (type == TMQ_OFFSET__RESET_EARLIEST || type == TMQ_OFFSET__RESET_LATEST) {
    code = getCommittedFromServer(tmq, tname, vgId, &epSet, &position);
    if (code == TSDB_CODE_TMQ_NO_COMMITTED) {
      if (type == TMQ_OFFSET__RESET_EARLIEST) {
        position = begin;
      } else if (type == TMQ_OFFSET__RESET_LATEST) {
        position = end;
      } else {
        tqErrorC("consumer:0x%" PRIx64 " invalid offset type:%d", tmq->consumerId, type);
        return TSDB_CODE_INTERNAL_ERROR;
      }
    } else if(code != 0) {
      tqErrorC("consumer:0x%" PRIx64 " getCommittedFromServer error,%d", tmq->consumerId, code);
      return code;
    }
  } else {
    tqErrorC("consumer:0x%" PRIx64 " offset type:%d can not be reach here", tmq->consumerId, type);
    return TSDB_CODE_INTERNAL_ERROR;
  }

  tqDebugC("consumer:0x%" PRIx64 " tmq_position vgId:%d position:%" PRId64, tmq->consumerId, vgId, position);
  return position;
}

// ============================================================
// tmq_committed
// ============================================================
int64_t tmq_committed(tmq_t* tmq, const char* pTopicName, int32_t vgId) {
  if (tmq == NULL || pTopicName == NULL) {
    tqErrorC("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  char tname[TSDB_TOPIC_FNAME_LEN] = {0};
  buildTopicFullName(tmq, pTopicName, tname);

  tmqWlock(tmq);

  SMqClientVg* pVg = NULL;
  int32_t      code = getClientVg(tmq, tname, vgId, &pVg);
  if (code != 0) {
    tmqWUnlock(tmq);
    return code;
  }

  SVgOffsetInfo* pOffsetInfo = &pVg->offsetInfo;
  if (isInSnapshotMode(pOffsetInfo->endOffset.type, tmq->useSnapshot)) {
    tqErrorC("consumer:0x%" PRIx64 " offset type:%d not wal version, committed error", tmq->consumerId,
             pOffsetInfo->endOffset.type);
    tmqWUnlock(tmq);
    return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
  }

  if (isInSnapshotMode(pOffsetInfo->committedOffset.type, tmq->useSnapshot)) {
    tqErrorC("consumer:0x%" PRIx64 " offset type:%d not wal version, committed error", tmq->consumerId,
             pOffsetInfo->committedOffset.type);
    tmqWUnlock(tmq);
    return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
  }

  int64_t committed = 0;
  if (pOffsetInfo->committedOffset.type == TMQ_OFFSET__LOG) {
    committed = pOffsetInfo->committedOffset.version;
    tmqWUnlock(tmq);
    goto end;
  }
  SEpSet epSet = pVg->epSet;
  tmqWUnlock(tmq);

  code = getCommittedFromServer(tmq, tname, vgId, &epSet, &committed);
  if (code != 0) {
    tqErrorC("consumer:0x%" PRIx64 " getCommittedFromServer error,%d", tmq->consumerId, code);
    return code;
  }

  end:
  tqDebugC("consumer:0x%" PRIx64 " tmq_committed vgId:%d committed:%" PRId64, tmq->consumerId, vgId, committed);
  return committed;
}

// ============================================================
// helpers for assignment
// ============================================================
void destroyCommonInfo(SMqVgCommon* pCommon) {
  if (pCommon == NULL) {
    return;
  }
  taosArrayDestroy(pCommon->pList);
  pCommon->pList = NULL;
  if(tsem2_destroy(&pCommon->rsp) != 0) {
    tqErrorC("failed to destroy semaphore for topic:%s", pCommon->pTopicName);
  }
  taosMemoryFreeClear(pCommon->pTopicName);
  (void)taosThreadMutexDestroy(&pCommon->mutex);
  taosMemoryFree(pCommon);
}

// ============================================================
// get wal info callback (for assignment)
// ============================================================
static int32_t tmqGetWalInfoCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (param == NULL || pMsg == NULL) {
    return code;
  }
  SMqVgWalInfoParam* pParam = param;
  SMqVgCommon*       pCommon = pParam->pCommon;

  if (code != TSDB_CODE_SUCCESS) {
    tqErrorC("consumer:0x%" PRIx64 " failed to get the wal info from vgId:%d for topic:%s", pCommon->consumerId,
             pParam->vgId, pCommon->pTopicName);

  } else {
    SMqDataRsp rsp = {0};
    SDecoder   decoder = {0};
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    code = tDecodeMqDataRsp(&decoder, &rsp);
    tDecoderClear(&decoder);
    if (code != 0) {
      goto END;
    }

    SMqRspHead*          pHead = pMsg->pData;
    tmq_topic_assignment assignment = {.begin = pHead->walsver,
        .end = pHead->walever + 1,
        .currentOffset = rsp.rspOffset.version,
        .vgId = pParam->vgId};

    (void)taosThreadMutexLock(&pCommon->mutex);
    if (taosArrayPush(pCommon->pList, &assignment) == NULL) {
      tqErrorC("consumer:0x%" PRIx64 " failed to push the wal info from vgId:%d for topic:%s", pCommon->consumerId,
               pParam->vgId, pCommon->pTopicName);
      code = TSDB_CODE_TSC_INTERNAL_ERROR;
    }
    (void)taosThreadMutexUnlock(&pCommon->mutex);
  }

  END:
  pCommon->code = code;
  int32_t total = atomic_add_fetch_32(&pCommon->numOfRsp, 1);
  if (total == pParam->totalReq) {
    if (tsem2_post(&pCommon->rsp) != 0) {
      tqErrorC("failed to post semaphore in get wal cb");
    }
  }

  if (pMsg) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
  }

  return code;
}

// ============================================================
// tmq_get_topic_assignment
// ============================================================
static int32_t fetchRemoteAssignments(tmq_t* tmq, SMqClientTopic* pTopic, int32_t numOfVgs,
                                      tmq_topic_assignment** assignment, int32_t* numOfAssignment,
                                      SMqVgCommon** ppCommon) {
  int32_t code = 0;
  SMqVgCommon* pCommon = taosMemoryCalloc(1, sizeof(SMqVgCommon));
  if (pCommon == NULL) {
    return terrno;
  }
  *ppCommon = pCommon;

  pCommon->pList = taosArrayInit(4, sizeof(tmq_topic_assignment));
  if (pCommon->pList == NULL) {
    return terrno;
  }

  code = tsem2_init(&pCommon->rsp, 0, 0);
  if (code != 0) {
    return code;
  }
  (void)taosThreadMutexInit(&pCommon->mutex, 0);
  pCommon->pTopicName = taosStrdup(pTopic->topicName);
  if (pCommon->pTopicName == NULL) {
    return terrno;
  }
  pCommon->consumerId = tmq->consumerId;

  for (int32_t i = 0; i < numOfVgs; ++i) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);
    if (pClientVg == NULL) {
      continue;
    }
    SMqVgWalInfoParam* pParam = taosMemoryMalloc(sizeof(SMqVgWalInfoParam));
    if (pParam == NULL) {
      return terrno;
    }

    pParam->epoch = atomic_load_32(&tmq->epoch);
    pParam->vgId = pClientVg->vgId;
    pParam->totalReq = numOfVgs;
    pParam->pCommon = pCommon;

    SMqPollReq req = {0};
    tmqBuildConsumeReqImpl(&req, tmq, pTopic, pClientVg);
    req.reqOffset = pClientVg->offsetInfo.beginOffset;

    int32_t msgSize = tSerializeSMqPollReq(NULL, 0, &req);
    if (msgSize < 0) {
      taosMemoryFree(pParam);
      return msgSize;
    }

    char* msg = taosMemoryCalloc(1, msgSize);
    if (NULL == msg) {
      taosMemoryFree(pParam);
      return terrno;
    }

    msgSize = tSerializeSMqPollReq(msg, msgSize, &req);
    if (msgSize < 0) {
      taosMemoryFree(msg);
      taosMemoryFree(pParam);
      return msgSize;
    }

    SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
    if (sendInfo == NULL) {
      taosMemoryFree(pParam);
      taosMemoryFree(msg);
      return terrno;
    }

    sendInfo->msgInfo = (SDataBuf){.pData = msg, .len = msgSize, .handle = NULL};
    sendInfo->requestId = req.reqId;
    sendInfo->requestObjRefId = 0;
    sendInfo->param = pParam;
    sendInfo->paramFreeFp = taosAutoMemoryFree;
    sendInfo->fp = tmqGetWalInfoCb;
    sendInfo->msgType = TDMT_VND_TMQ_VG_WALINFO;

    char offsetFormatBuf[TSDB_OFFSET_LEN] = {0};
    tFormatOffset(offsetFormatBuf, tListLen(offsetFormatBuf), &pClientVg->offsetInfo.beginOffset);

    tqDebugC("consumer:0x%" PRIx64 " %s retrieve wal info vgId:%d, epoch %d, req:%s, QID:0x%" PRIx64, tmq->consumerId,
            pTopic->topicName, pClientVg->vgId, tmq->epoch, offsetFormatBuf, req.reqId);
    code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pClientVg->epSet, NULL, sendInfo);
    if (code != 0) {
      return code;
    }
  }

  if (tsem2_wait(&pCommon->rsp) != 0){
    tqErrorC("consumer:0x%" PRIx64 " failed to wait sem in get assignment", tmq->consumerId);
  }
  code = pCommon->code;
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t num = taosArrayGetSize(pCommon->pList);
  for (int32_t i = 0; i < num; ++i) {
    (*assignment)[i] = *(tmq_topic_assignment*)taosArrayGet(pCommon->pList, i);
  }
  *numOfAssignment = num;

  for (int32_t j = 0; j < (*numOfAssignment); ++j) {
    tmq_topic_assignment* p = &(*assignment)[j];

    for (int32_t i = 0; i < taosArrayGetSize(pTopic->vgs); ++i) {
      SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);
      if (pClientVg == NULL) {
        continue;
      }
      if (pClientVg->vgId != p->vgId) {
        continue;
      }

      SVgOffsetInfo* pOffsetInfo = &pClientVg->offsetInfo;
      tqDebugC("consumer:0x%" PRIx64 " %s vgId:%d offset is update to:%" PRId64, tmq->consumerId, pTopic->topicName,
              p->vgId, p->currentOffset);

      pOffsetInfo->walVerBegin = p->begin;
      pOffsetInfo->walVerEnd = p->end;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tmq_get_topic_assignment(tmq_t* tmq, const char* pTopicName, tmq_topic_assignment** assignment,
                                 int32_t* numOfAssignment) {
  if (tmq == NULL || pTopicName == NULL || assignment == NULL || numOfAssignment == NULL) {
    tqErrorC("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }
  *numOfAssignment = 0;
  *assignment = NULL;
  SMqVgCommon* pCommon = NULL;

  char tname[TSDB_TOPIC_FNAME_LEN] = {0};
  buildTopicFullName(tmq, pTopicName, tname);

  tmqWlock(tmq);

  SMqClientTopic* pTopic = NULL;
  int32_t         code = getTopicByName(tmq, tname, &pTopic);
  if (code != 0) {
    tqErrorC("consumer:0x%" PRIx64 " invalid topic name:%s", tmq->consumerId, pTopicName);
    goto end;
  }

  // in case of snapshot is opened, no valid offset will return
  *numOfAssignment = taosArrayGetSize(pTopic->vgs);
  for (int32_t j = 0; j < (*numOfAssignment); ++j) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, j);
    if (pClientVg == NULL) {
      continue;
    }
    int32_t type = pClientVg->offsetInfo.beginOffset.type;
    if (isInSnapshotMode(type, tmq->useSnapshot)) {
      tqErrorC("consumer:0x%" PRIx64 " offset type:%d not wal version, assignment not allowed", tmq->consumerId, type);
      code = TSDB_CODE_TMQ_SNAPSHOT_ERROR;
      goto end;
    }
  }

  *assignment = taosMemoryCalloc(*numOfAssignment, sizeof(tmq_topic_assignment));
  if (*assignment == NULL) {
    tqErrorC("consumer:0x%" PRIx64 " failed to malloc buffer, size:%" PRIzu, tmq->consumerId,
             (*numOfAssignment) * sizeof(tmq_topic_assignment));
    code = terrno;
    goto end;
  }

  bool needFetch = false;

  for (int32_t j = 0; j < (*numOfAssignment); ++j) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, j);
    if (pClientVg == NULL) {
      continue;
    }
    if (pClientVg->offsetInfo.beginOffset.type != TMQ_OFFSET__LOG) {
      needFetch = true;
      break;
    }

    tmq_topic_assignment* pAssignment = &(*assignment)[j];
    pAssignment->currentOffset = pClientVg->offsetInfo.beginOffset.version;
    pAssignment->begin = pClientVg->offsetInfo.walVerBegin;
    pAssignment->end = pClientVg->offsetInfo.walVerEnd;
    pAssignment->vgId = pClientVg->vgId;
    tqDebugC("consumer:0x%" PRIx64 " get assignment from local:%d->%" PRId64, tmq->consumerId, pAssignment->vgId,
            pAssignment->currentOffset);
  }

  if (needFetch) {
    code = fetchRemoteAssignments(tmq, pTopic, *numOfAssignment, assignment, numOfAssignment, &pCommon);
    if (code != 0) {
      goto end;
    }
  }

  end:
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(*assignment);
    *assignment = NULL;
    *numOfAssignment = 0;
    tqDebugC("consumer:0x%" PRIx64 " get assignment for topic:%s failed, code:%s", tmq->consumerId, pTopicName, tstrerror(code));
  } else {
    tqDebugC("consumer:0x%" PRIx64 " get assignment for topic:%s, numOfAssignment:%d", tmq->consumerId, pTopicName, *numOfAssignment);
    for (int32_t i = 0; i < *numOfAssignment; ++i) {
      tmq_topic_assignment* p = &(*assignment)[i];
      tqTraceC("consumer:0x%" PRIx64 " assignment[%d] vgId:%d offset:%" PRId64 " begin:%" PRId64 " end:%" PRId64,
               tmq->consumerId, i, p->vgId, p->currentOffset, p->begin, p->end);
    }
  }
  destroyCommonInfo(pCommon);
  tmqWUnlock(tmq);
  return code;
}

void tmq_free_assignment(tmq_topic_assignment* pAssignment) {
  if (pAssignment == NULL) {
    return;
  }

  taosMemoryFree(pAssignment);
}

// ============================================================
// tmq_offset_seek
// ============================================================
int32_t tmq_offset_seek(tmq_t* tmq, const char* pTopicName, int32_t vgId, int64_t offset) {
  if (tmq == NULL || pTopicName == NULL) {
    tqErrorC("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  char tname[TSDB_TOPIC_FNAME_LEN] = {0};
  buildTopicFullName(tmq, pTopicName, tname);

  tmqWlock(tmq);

  SMqClientVg* pVg = NULL;
  int32_t      code = getClientVg(tmq, tname, vgId, &pVg);
  if (code != 0) {
    tmqWUnlock(tmq);
    return code;
  }

  SVgOffsetInfo* pOffsetInfo = &pVg->offsetInfo;

  int32_t type = pOffsetInfo->endOffset.type;
  if (isInSnapshotMode(type, tmq->useSnapshot)) {
    tqErrorC("consumer:0x%" PRIx64 " offset type:%d not wal version, seek not allowed", tmq->consumerId, type);
    tmqWUnlock(tmq);
    return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
  }

  code = checkWalRange(pOffsetInfo, offset);
  if (code != 0) {
    tmqWUnlock(tmq);
    return code;
  }

  tqDebugC("consumer:0x%" PRIx64 " seek to %" PRId64 " on vgId:%d", tmq->consumerId, offset, vgId);
  // update the offset, and then commit to vnode
  pOffsetInfo->endOffset.type = TMQ_OFFSET__LOG;
  pOffsetInfo->endOffset.version = offset;
  pOffsetInfo->beginOffset = pOffsetInfo->endOffset;
  pVg->seekUpdated = true;
  tmqWUnlock(tmq);

  return code;
}

// ============================================================
// tmqGetNextResInfo (get next result block)
// ============================================================
int32_t tmqGetNextResInfo(TAOS_RES* res, bool convertUcs4, SReqResultInfo** pResInfo) {
  if (res == NULL || pResInfo == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SMqRspObj*  pRspObj = (SMqRspObj*)res;
  SMqDataRsp* data = &pRspObj->dataRsp;

  pRspObj->resIter++;
  if (pRspObj->resIter < data->blockNum) {
    doFreeReqResultInfo(&pRspObj->resInfo);
    SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(data->blockSchema, pRspObj->resIter);
    if (pSW) {
      TAOS_CHECK_RETURN(setResSchemaInfo(&pRspObj->resInfo, pSW->pSchema, pSW->nCols, NULL, false));
    }
    void*   pRetrieve = taosArrayGetP(data->blockData, pRspObj->resIter);
    void*   rawData = NULL;
    int64_t rows = 0;
    int32_t precision = 0;

    // inline helper to get raw data rows and precision
    if (pRetrieve != NULL) {
      if (*(int64_t*)pRetrieve == 0) {
        rawData = ((SRetrieveTableRsp*)pRetrieve)->data;
        rows = htobe64(((SRetrieveTableRsp*)pRetrieve)->numOfRows);
        precision = ((SRetrieveTableRsp*)pRetrieve)->precision;
      } else if (*(int64_t*)pRetrieve == 1) {
        rawData = ((SRetrieveTableRspForTmq*)pRetrieve)->data;
        rows = htobe64(((SRetrieveTableRspForTmq*)pRetrieve)->numOfRows);
        precision = ((SRetrieveTableRspForTmq*)pRetrieve)->precision;
      }
    }

    pRspObj->resInfo.pData = rawData;
    pRspObj->resInfo.numOfRows = rows;
    pRspObj->resInfo.current = 0;
    pRspObj->resInfo.precision = precision;

    pRspObj->resInfo.totalRows += pRspObj->resInfo.numOfRows;
    int32_t code = setResultDataPtr(&pRspObj->resInfo, convertUcs4, false);
    if (code != 0) {
      return code;
    }
    *pResInfo = &pRspObj->resInfo;
    return code;
  }

  return TSDB_CODE_TSC_INTERNAL_ERROR;
}

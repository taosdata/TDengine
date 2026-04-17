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
// commit done / count down
// ============================================================
int32_t tmqCommitDone(SMqCommitCbParamSet* pParamSet) {
  if (pParamSet == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int64_t refId = pParamSet->refId;
  int32_t code = 0;
  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    code = TSDB_CODE_TMQ_CONSUMER_CLOSED;
  }

  // if no more waiting rsp
  if (pParamSet->callbackFn != NULL) {
    pParamSet->callbackFn(tmq, pParamSet->code, pParamSet->userParam);
  }

  taosMemoryFree(pParamSet);
  if (tmq != NULL) {
    code = taosReleaseRef(tmqMgmt.rsetId, refId);
  }

  return code;
}

int32_t commitRspCountDown(SMqCommitCbParamSet* pParamSet, int64_t consumerId, const char* pTopic, int32_t vgId) {
  if (pParamSet == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t waitingRspNum = atomic_sub_fetch_32(&pParamSet->waitingRspNum, 1);
  if (waitingRspNum == 0) {
    tqDebugC("consumer:0x%" PRIx64 " topic:%s vgId:%d all commit-rsp received, commit completed", consumerId, pTopic,
             vgId);
    return tmqCommitDone(pParamSet);
  } else {
    tqDebugC("consumer:0x%" PRIx64 " topic:%s vgId:%d commit-rsp received, remain:%d", consumerId, pTopic, vgId,
             waitingRspNum);
  }
  return 0;
}

// ============================================================
// commit callback
// ============================================================
static int32_t tmqCommitCb(void* param, SDataBuf* pBuf, int32_t code) {
  if (pBuf){
    taosMemoryFreeClear(pBuf->pData);
    taosMemoryFreeClear(pBuf->pEpSet);
  }
  if(param == NULL){
    return TSDB_CODE_INVALID_PARA;
  }
  SMqCommitCbParam*    pParam = (SMqCommitCbParam*)param;
  SMqCommitCbParamSet* pParamSet = (SMqCommitCbParamSet*)pParam->params;

  return commitRspCountDown(pParamSet, pParam->consumerId, pParam->topicName, pParam->vgId);
}

// ============================================================
// send commit msg
// ============================================================
int32_t doSendCommitMsg(tmq_t* tmq, int32_t vgId, SEpSet* epSet, STqOffsetVal* offset, const char* pTopicName,
                        SMqCommitCbParamSet* pParamSet) {
  if (tmq == NULL || epSet == NULL || offset == NULL || pTopicName == NULL || pParamSet == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SMqVgOffset pOffset = {0};

  pOffset.consumerId = tmq->consumerId;
  pOffset.offset.val = *offset;
  (void)snprintf(pOffset.offset.subKey, TSDB_SUBSCRIBE_KEY_LEN, "%s%s%s", tmq->groupId, TMQ_SEPARATOR, pTopicName);
  int32_t len = 0;
  int32_t code = 0;
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
  if (tEncodeMqVgOffset(&encoder, &pOffset) < 0) {
    tEncoderClear(&encoder);
    taosMemoryFree(buf);
    return TSDB_CODE_INVALID_PARA;
  }
  tEncoderClear(&encoder);

  // build param
  SMqCommitCbParam* pParam = taosMemoryCalloc(1, sizeof(SMqCommitCbParam));
  if (pParam == NULL) {
    taosMemoryFree(buf);
    return terrno;
  }

  pParam->params = pParamSet;
  pParam->vgId = vgId;
  pParam->consumerId = tmq->consumerId;

  tstrncpy(pParam->topicName, pTopicName, tListLen(pParam->topicName));

  // build send info
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pMsgSendInfo == NULL) {
    taosMemoryFree(buf);
    taosMemoryFree(pParam);
    return terrno;
  }

  pMsgSendInfo->msgInfo = (SDataBuf){.pData = buf, .len = sizeof(SMsgHead) + len, .handle = NULL};

  pMsgSendInfo->requestId = generateRequestId();
  pMsgSendInfo->requestObjRefId = 0;
  pMsgSendInfo->param = pParam;
  pMsgSendInfo->paramFreeFp = taosAutoMemoryFree;
  pMsgSendInfo->fp = tmqCommitCb;
  pMsgSendInfo->msgType = TDMT_VND_TMQ_COMMIT_OFFSET;

  // int64_t transporterId = 0;
  (void)atomic_add_fetch_32(&pParamSet->waitingRspNum, 1);
  code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, epSet, NULL, pMsgSendInfo);
  if (code != 0) {
    (void)atomic_sub_fetch_32(&pParamSet->waitingRspNum, 1);
  }
  return code;
}

// ============================================================
// prepare commit param set
// ============================================================
int32_t prepareCommitCbParamSet(tmq_t* tmq, tmq_commit_cb* pCommitFp, void* userParam, int32_t rspNum,
                                SMqCommitCbParamSet** ppParamSet) {
  if (tmq == NULL || ppParamSet == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SMqCommitCbParamSet* pParamSet = taosMemoryCalloc(1, sizeof(SMqCommitCbParamSet));
  if (pParamSet == NULL) {
    return terrno;
  }

  pParamSet->refId = tmq->refId;
  pParamSet->epoch = atomic_load_32(&tmq->epoch);
  pParamSet->callbackFn = pCommitFp;
  pParamSet->userParam = userParam;
  pParamSet->waitingRspNum = rspNum;
  *ppParamSet = pParamSet;
  return 0;
}

// ============================================================
// wal marker
// ============================================================
static int32_t sendWalMarkMsgToMnodeCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (pMsg) {
    taosMemoryFreeClear(pMsg->pEpSet);
    taosMemoryFreeClear(pMsg->pData);
  }
  tqDebugC("sendWalMarkMsgToMnodeCb code:%d", code);
  return 0;
}

void asyncSendWalMarkMsgToMnode(tmq_t* tmq, int32_t vgId, int64_t keepVersion) {
  if (tmq == NULL) return ;
  void*           buf = NULL;
  SMsgSendInfo*   sendInfo = NULL;
  SMndSetVgroupKeepVersionReq req = {0};

  tqDebugC("consumer:0x%" PRIx64 " send vgId:%d keepVersion:%"PRId64, tmq->consumerId, vgId, keepVersion);
  req.vgId = vgId;
  req.keepVersion = keepVersion;

  int32_t tlen = tSerializeSMndSetVgroupKeepVersionReq(NULL, 0, &req);
  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    return;
  }
  tlen = tSerializeSMndSetVgroupKeepVersionReq(buf, tlen, &req);

  sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(buf);
    return;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = buf, .len = tlen, .handle = NULL};
  sendInfo->requestId = generateRequestId();
  sendInfo->fp = sendWalMarkMsgToMnodeCb;
  sendInfo->msgType = TDMT_MND_SET_VGROUP_KEEP_VERSION;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int32_t code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, NULL, sendInfo);
  if (code != 0) {
    tqErrorC("consumer:0x%" PRIx64 " send wal mark msg to mnode failed, code:%s", tmq->consumerId,
             tstrerror(terrno));
  }
}

// ============================================================
// inner commit
// ============================================================
int32_t innerCommit(tmq_t* tmq, char* pTopicName, STqOffsetVal* offsetVal, SMqClientVg* pVg, SMqCommitCbParamSet* pParamSet){
  if (tmq == NULL || pTopicName == NULL || offsetVal == NULL || pVg == NULL || pParamSet == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;
  if (offsetVal->type <= 0) {
    code = TSDB_CODE_TMQ_INVALID_MSG;
    return code;
  }
  if (tOffsetEqual(offsetVal, &pVg->offsetInfo.committedOffset)) {
    code = TSDB_CODE_TMQ_SAME_COMMITTED_VALUE;
    return code;
  }
  char offsetBuf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(offsetBuf, tListLen(offsetBuf), offsetVal);

  char commitBuf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(commitBuf, tListLen(commitBuf), &pVg->offsetInfo.committedOffset);

  code = doSendCommitMsg(tmq, pVg->vgId, &pVg->epSet, offsetVal, pTopicName, pParamSet);
  if (code != TSDB_CODE_SUCCESS) {
    tqErrorC("consumer:0x%" PRIx64 " topic:%s on vgId:%d end commit msg failed, send offset:%s committed:%s, code:%s",
             tmq->consumerId, pTopicName, pVg->vgId, offsetBuf, commitBuf, tstrerror(terrno));
    return code;
  }

  if (tmq->enableWalMarker && offsetVal->type == TMQ_OFFSET__LOG) {
    asyncSendWalMarkMsgToMnode(tmq, pVg->vgId, offsetVal->version);
  }
  tqDebugC("consumer:0x%" PRIx64 " topic:%s on vgId:%d send commit msg success, send offset:%s committed:%s",
           tmq->consumerId, pTopicName, pVg->vgId, offsetBuf, commitBuf);
  tOffsetCopy(&pVg->offsetInfo.committedOffset, offsetVal);
  return code;
}

// ============================================================
// async commit offset (single)
// ============================================================
int32_t asyncCommitOffset(tmq_t* tmq, char* pTopicName, int32_t vgId, STqOffsetVal* offsetVal,
                          tmq_commit_cb* pCommitFp, void* userParam) {
  if (tmq == NULL || pTopicName == NULL || offsetVal == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  tqDebugC("consumer:0x%" PRIx64 " do manual commit offset for %s, vgId:%d", tmq->consumerId, pTopicName, vgId);
  SMqCommitCbParamSet* pParamSet = NULL;
  int32_t code = prepareCommitCbParamSet(tmq, pCommitFp, userParam, 0, &pParamSet);
  if (code != 0){
    return code;
  }

  tmqRlock(tmq);
  SMqClientVg* pVg = NULL;
  code = getClientVg(tmq, pTopicName, vgId, &pVg);
  if (code == 0) {
    code = innerCommit(tmq, pTopicName, offsetVal, pVg, pParamSet);
  }
  tmqRUnlock(tmq);

  if (code != 0){
    taosMemoryFree(pParamSet);
  }
  return code;
}

// ============================================================
// async commit from result
// ============================================================
void asyncCommitFromResult(tmq_t* tmq, const TAOS_RES* pRes, tmq_commit_cb* pCommitFp, void* userParam) {
  char*        pTopicName = NULL;
  int32_t      vgId = 0;
  STqOffsetVal offsetVal = {0};
  int32_t      code = 0;

  if (pRes == NULL || tmq == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  if (TD_RES_TMQ(pRes) || TD_RES_TMQ_RAW(pRes) || TD_RES_TMQ_META(pRes) ||
      TD_RES_TMQ_METADATA(pRes) || TD_RES_TMQ_BATCH_META(pRes)) {
    SMqRspObj* pRspObj = (SMqRspObj*)pRes;
    pTopicName = pRspObj->topic;
    vgId = pRspObj->vgId;
    offsetVal = pRspObj->rspOffset;
  } else {
    code = TSDB_CODE_TMQ_INVALID_MSG;
    goto end;
  }

  code = asyncCommitOffset(tmq, pTopicName, vgId, &offsetVal, pCommitFp, userParam);

  end:
  if (code != TSDB_CODE_SUCCESS && pCommitFp != NULL) {
    if (code == TSDB_CODE_TMQ_SAME_COMMITTED_VALUE) code = TSDB_CODE_SUCCESS;
    pCommitFp(tmq, code, userParam);
  }
}

// ============================================================
// inner commit all
// ============================================================
static int32_t innerCommitAll(tmq_t* tmq, SMqCommitCbParamSet* pParamSet){
  if (tmq == NULL || pParamSet == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;
  tmqRlock(tmq);
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  tqDebugC("consumer:0x%" PRIx64 " start to commit offset for %d topics", tmq->consumerId, numOfTopics);

  for (int32_t i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (pTopic == NULL) {
      code = TSDB_CODE_TMQ_INVALID_TOPIC;
      goto END;
    }
    int32_t numOfVgroups = taosArrayGetSize(pTopic->vgs);
    tqDebugC("consumer:0x%" PRIx64 " commit offset for topics:%s, numOfVgs:%d", tmq->consumerId, pTopic->topicName, numOfVgroups);
    for (int32_t j = 0; j < numOfVgroups; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (pVg == NULL) {
        code = terrno;
        goto END;
      }

      code = innerCommit(tmq, pTopic->topicName, &pVg->offsetInfo.endOffset, pVg, pParamSet);
      if (code != 0 && code != TSDB_CODE_TMQ_SAME_COMMITTED_VALUE){
        tqErrorC("consumer:0x%" PRIx64 " topic:%s vgId:%d, no commit, code:%s, current offset version:%" PRId64 ", ordinal:%d/%d",
                 tmq->consumerId, pTopic->topicName, pVg->vgId, tstrerror(code), pVg->offsetInfo.endOffset.version, j + 1, numOfVgroups);
      }
    }
  }
  tqDebugC("consumer:0x%" PRIx64 " total commit:%d for %d topics", tmq->consumerId, pParamSet->waitingRspNum - DEFAULT_COMMIT_CNT,
           numOfTopics);
  END:
  tmqRUnlock(tmq);
  return code;
}

// ============================================================
// async commit all offsets
// ============================================================
void asyncCommitAllOffsets(tmq_t* tmq, tmq_commit_cb* pCommitFp, void* userParam) {
  if (tmq == NULL) {
    return;
  }
  int32_t code = 0;
  SMqCommitCbParamSet* pParamSet = NULL;
  // init waitingRspNum as DEFAULT_COMMIT_CNT to prevent concurrency issue
  code = prepareCommitCbParamSet(tmq, pCommitFp, userParam, DEFAULT_COMMIT_CNT, &pParamSet);
  if (code != 0) {
    tqErrorC("consumer:0x%" PRIx64 " prepareCommitCbParamSet failed, code:%s", tmq->consumerId, tstrerror(code));
    if (pCommitFp != NULL) {
      pCommitFp(tmq, code, userParam);
    }
    return;
  }
  code = innerCommitAll(tmq, pParamSet);
  if (code != 0 && code != TSDB_CODE_TMQ_SAME_COMMITTED_VALUE){
    tqErrorC("consumer:0x%" PRIx64 " innerCommitAll failed, code:%s", tmq->consumerId, tstrerror(code));
  }

  code = commitRspCountDown(pParamSet, tmq->consumerId, "init", -1);
  if (code != 0) {
    tqErrorC("consumer:0x%" PRIx64 " commit rsp count down failed, code:%s", tmq->consumerId, tstrerror(code));
  }
  return;
}

// ============================================================
// sync commit helpers
// ============================================================
void commitCallBackFn(tmq_t* tmq, int32_t code, void* param) {
  if (param == NULL) {
    tqErrorC("invalid param in commit cb");
    return;
  }
  SSyncCommitInfo* pInfo = (SSyncCommitInfo*)param;
  pInfo->code = code;
  if (tsem2_post(&pInfo->sem) != 0){
    tqErrorC("failed to post rsp sem in commit cb");
  }
}

// ============================================================
// public commit APIs
// ============================================================
void tmq_commit_async(tmq_t* tmq, const TAOS_RES* pRes, tmq_commit_cb* cb, void* param) {
  if (tmq == NULL) {
    tqErrorC("invalid tmq handle, null");
    if (cb != NULL) {
      cb(tmq, TSDB_CODE_INVALID_PARA, param);
    }
    return;
  }
  if (pRes == NULL) {  // here needs to commit all offsets.
    asyncCommitAllOffsets(tmq, cb, param);
  } else {  // only commit one offset
    asyncCommitFromResult(tmq, pRes, cb, param);
  }
}

int32_t tmq_commit_sync(tmq_t* tmq, const TAOS_RES* pRes) {
  if (tmq == NULL) {
    tqErrorC("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;

  SSyncCommitInfo* pInfo = taosMemoryMalloc(sizeof(SSyncCommitInfo));
  if (pInfo == NULL) {
    tqErrorC("failed to allocate memory for sync commit");
    return terrno;
  }

  code = tsem2_init(&pInfo->sem, 0, 0);
  if (code != 0) {
    tqErrorC("failed to init sem for sync commit");
    taosMemoryFree(pInfo);
    return code;
  }
  pInfo->code = 0;

  if (pRes == NULL) {
    asyncCommitAllOffsets(tmq, commitCallBackFn, pInfo);
  } else {
    asyncCommitFromResult(tmq, pRes, commitCallBackFn, pInfo);
  }

  if (tsem2_wait(&pInfo->sem) != 0){
    tqErrorC("failed to wait sem for sync commit");
  }
  code = pInfo->code;

  if(tsem2_destroy(&pInfo->sem) != 0) {
    tqErrorC("failed to destroy sem for sync commit");
  }
  taosMemoryFree(pInfo);

  tqDebugC("consumer:0x%" PRIx64 " sync res commit done, code:%s", tmq->consumerId, tstrerror(code));
  return code;
}

// wal range will be ok after calling tmq_get_topic_assignment or poll interface
int32_t checkWalRange(SVgOffsetInfo* offset, int64_t value) {
  if (offset == NULL) {
    tqErrorC("invalid offset, null");
    return TSDB_CODE_INVALID_PARA;
  }
  if (offset->walVerBegin == -1 || offset->walVerEnd == -1) {
    tqErrorC("Assignment or poll interface need to be called first");
    return TSDB_CODE_TMQ_NEED_INITIALIZED;
  }

  if (value != -1 && (value < offset->walVerBegin || value > offset->walVerEnd)) {
    tqErrorC("invalid seek params, offset:%" PRId64 ", valid range:[%" PRId64 ", %" PRId64 "]", value,
             offset->walVerBegin, offset->walVerEnd);
    return TSDB_CODE_TMQ_VERSION_OUT_OF_RANGE;
  }

  return 0;
}

bool isInSnapshotMode(int8_t type, bool useSnapshot) {
  if ((type < TMQ_OFFSET__LOG && useSnapshot) || type > TMQ_OFFSET__LOG) {
    return true;
  }
  return false;
}

int32_t tmq_commit_offset_sync(tmq_t* tmq, const char* pTopicName, int32_t vgId, int64_t offset) {
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
  code = checkWalRange(pOffsetInfo, offset);
  if (code != 0) {
    tmqWUnlock(tmq);
    return code;
  }
  tmqWUnlock(tmq);

  STqOffsetVal offsetVal = {.type = TMQ_OFFSET__LOG, .version = offset};

  SSyncCommitInfo* pInfo = taosMemoryMalloc(sizeof(SSyncCommitInfo));
  if (pInfo == NULL) {
    tqErrorC("consumer:0x%" PRIx64 " failed to prepare seek operation", tmq->consumerId);
    return terrno;
  }

  code = tsem2_init(&pInfo->sem, 0, 0);
  if (code != 0) {
    taosMemoryFree(pInfo);
    return code;
  }
  pInfo->code = 0;

  code = asyncCommitOffset(tmq, tname, vgId, &offsetVal, commitCallBackFn, pInfo);
  if (code == 0) {
    if (tsem2_wait(&pInfo->sem) != 0){
      tqErrorC("failed to wait sem for sync commit offset");
    }
    code = pInfo->code;
  }

  if (code == TSDB_CODE_TMQ_SAME_COMMITTED_VALUE) code = TSDB_CODE_SUCCESS;
  if(tsem2_destroy(&pInfo->sem) != 0) {
    tqErrorC("failed to destroy sem for sync commit offset");
  }
  taosMemoryFree(pInfo);

  tqDebugC("consumer:0x%" PRIx64 " sync send commit to vgId:%d, offset:%" PRId64 " code:%s", tmq->consumerId, vgId,
          offset, tstrerror(code));

  return code;
}

void tmq_commit_offset_async(tmq_t* tmq, const char* pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb* cb,
                             void* param) {
  int32_t code = 0;
  if (tmq == NULL || pTopicName == NULL) {
    tqErrorC("invalid tmq handle, null");
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  char tname[TSDB_TOPIC_FNAME_LEN] = {0};
  buildTopicFullName(tmq, pTopicName, tname);

  tmqWlock(tmq);
  SMqClientVg* pVg = NULL;
  code = getClientVg(tmq, tname, vgId, &pVg);
  if (code != 0) {
    tmqWUnlock(tmq);
    goto end;
  }

  SVgOffsetInfo* pOffsetInfo = &pVg->offsetInfo;
  code = checkWalRange(pOffsetInfo, offset);
  if (code != 0) {
    tmqWUnlock(tmq);
    goto end;
  }
  tmqWUnlock(tmq);

  STqOffsetVal offsetVal = {.type = TMQ_OFFSET__LOG, .version = offset};

  code = asyncCommitOffset(tmq, tname, vgId, &offsetVal, cb, param);

  tqDebugC("consumer:0x%" PRIx64 " async send commit to vgId:%d, offset:%" PRId64 " code:%s", tmq->consumerId, vgId,
          offset, tstrerror(code));

  end:
  if (code != 0 && cb != NULL) {
    if (code == TSDB_CODE_TMQ_SAME_COMMITTED_VALUE) code = TSDB_CODE_SUCCESS;
    cb(tmq, code, param);
  }
}

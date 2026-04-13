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
#include "cJSON.h"
#include "parser.h"
#include "taos.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tmsg.h"

// ============================================================
// global variables
// ============================================================
TdThreadOnce   tmqInit = PTHREAD_ONCE_INIT;  // initialize only once
volatile int32_t tmqInitRes = 0;               // initialize rsp code
SMqMgmt        tmqMgmt = {0};

int32_t getTopicByName(tmq_t* tmq, const char* pTopicName, SMqClientTopic** topic) {
  if (tmq == NULL || pTopicName == NULL || topic == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  for (int32_t i = 0; i < numOfTopics; ++i) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (pTopic == NULL || strcmp(pTopic->topicName, pTopicName) != 0) {
      continue;
    }
    *topic = pTopic;
    return 0;
  }

  tqErrorC("consumer:0x%" PRIx64 ", total:%d, failed to find topic:%s", tmq->consumerId, numOfTopics, pTopicName);
  return TSDB_CODE_TMQ_INVALID_TOPIC;
}

int32_t getClientVg(tmq_t* tmq, char* pTopicName, int32_t vgId, SMqClientVg** pVg) {
  if (tmq == NULL || pTopicName == NULL || pVg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SMqClientTopic* pTopic = NULL;
  int32_t         code = getTopicByName(tmq, pTopicName, &pTopic);
  if (code != 0) {
    tqErrorC("consumer:0x%" PRIx64 " invalid topic name:%s", tmq->consumerId, pTopicName);
    return code;
  }

  int32_t numOfVgs = taosArrayGetSize(pTopic->vgs);
  for (int32_t i = 0; i < numOfVgs; ++i) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);
    if (pClientVg && pClientVg->vgId == vgId) {
      *pVg = pClientVg;
      break;
    }
  }

  return *pVg == NULL ? TSDB_CODE_TMQ_INVALID_VGID : TSDB_CODE_SUCCESS;
}

static void generateTimedTask(int64_t refId, int32_t type) {
  tmq_t*  tmq = NULL;
  int8_t* pTaskType = NULL;
  int32_t code = 0;

  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) return;

  code = taosAllocateQitem(sizeof(int8_t), DEF_QITEM, 0, (void**)&pTaskType);
  if (code == TSDB_CODE_SUCCESS) {
    *pTaskType = type;
    if (taosWriteQitem(tmq->delayedTask, pTaskType) == 0) {
      if (tsem2_post(&tmq->rspSem) != 0){
        tqErrorC("consumer:0x%" PRIx64 " failed to post sem, type:%d", tmq->consumerId, type);
      }
    }else{
      taosFreeQitem(pTaskType);
    }
  }

  code = taosReleaseRef(tmqMgmt.rsetId, refId);
  if (code != 0){
    tqErrorC("failed to release ref:%"PRId64 ", type:%d, code:%d", refId, type, code);
  }
}

void tmqAssignAskEpTask(void* param, void* tmrId) {
  int64_t refId = (int64_t)param;
  generateTimedTask(refId, TMQ_DELAYED_TASK__ASK_EP);
}

void tmqReplayTask(void* param, void* tmrId) {
  int64_t refId = (int64_t)param;
  tmq_t*  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) return;

  if (tsem2_post(&tmq->rspSem) != 0){
    tqErrorC("consumer:0x%" PRIx64 " failed to post sem, replay", tmq->consumerId);
  }
  int32_t code = taosReleaseRef(tmqMgmt.rsetId, refId);
  if (code != 0){
    tqErrorC("failed to release ref:%"PRId64 ", code:%d", refId, code);
  }
}

void tmqAssignDelayedCommitTask(void* param, void* tmrId) {
  int64_t refId = (int64_t)param;
  generateTimedTask(refId, TMQ_DELAYED_TASK__COMMIT);
}

int32_t tmqHbCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (pMsg == NULL || param == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int64_t refId = (int64_t)param;
  tmq_t*  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);

  if (tmq == NULL) {
    goto END;
  }

  atomic_store_32(&tmq->tokenCode, code);
  if (code != 0){
    goto END;
  }

  SMqHbRsp rsp = {0};
  code = tDeserializeSMqHbRsp(pMsg->pData, pMsg->len, &rsp);
  if (code != 0) {
    goto END;
  }

  tmqWlock(tmq);
  for (int32_t i = 0; i < taosArrayGetSize(rsp.topicPrivileges); i++) {
    STopicPrivilege* privilege = taosArrayGet(rsp.topicPrivileges, i);
    if (privilege == NULL) {
      continue;
    }
    int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
    for (int32_t j = 0; j < topicNumCur; j++) {
      SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, j);
      if (pTopicCur && strcmp(pTopicCur->topicName, privilege->topic) == 0 && pTopicCur->noPrivilege != privilege->noPrivilege) {
        tqInfoC("consumer:0x%" PRIx64 ", update privilege:%s, topic:%s", tmq->consumerId, privilege->noPrivilege ? "false" : "true", privilege->topic);
        pTopicCur->noPrivilege = privilege->noPrivilege;
      }
    }
  }
  tmqWUnlock(tmq);

  tqClientDebugFlag = rsp.debugFlag;

  tDestroySMqHbRsp(&rsp);

END:
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);
  if (tmq != NULL) {
    int32_t ret = taosReleaseRef(tmqMgmt.rsetId, refId);
    if (ret != 0){
      tqErrorC("failed to release ref:%"PRId64 ", code:%d", refId, ret);
    }
  }
  if (code != 0){
    tqErrorC("failed to process heartbeat, refId:%"PRId64 ", code:%d", refId, code);
  }
  return code;
}

static void buildVgHeartbeatData(tmq_t* tmq, SMqHbReq* req) {
  if (tmq == NULL || req == NULL) return;
  tmqRlock(tmq);
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (pTopic == NULL) {
      continue;
    }
    int32_t          numOfVgroups = taosArrayGetSize(pTopic->vgs);
    TopicOffsetRows* data = taosArrayReserve(req->topics, 1);
    if (data == NULL) {
      continue;
    }
    tstrncpy(data->topicName, pTopic->topicName, TSDB_TOPIC_FNAME_LEN);
    data->offsetRows = taosArrayInit(numOfVgroups, sizeof(OffsetRows));
    if (data->offsetRows == NULL) {
      continue;
    }
    for (int j = 0; j < numOfVgroups; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (pVg == NULL) {
        continue;
      }
      OffsetRows* offRows = taosArrayReserve(data->offsetRows, 1);
      if (offRows == NULL) {
        continue;
      }
      offRows->vgId = pVg->vgId;
      offRows->rows = pVg->numOfRows;
      offRows->offset = pVg->offsetInfo.endOffset;
      offRows->ever = pVg->offsetInfo.walVerEnd == -1 ? 0 : pVg->offsetInfo.walVerEnd;
      char buf[TSDB_OFFSET_LEN] = {0};
      tFormatOffset(buf, TSDB_OFFSET_LEN, &offRows->offset);
      tqDebugC("consumer:0x%" PRIx64 ",report offset, group:%s vgId:%d, offset:%s/%" PRId64 ", rows:%" PRId64,
               tmq->consumerId, tmq->groupId, offRows->vgId, buf, offRows->ever, offRows->rows);
    }
  }
  tmqRUnlock(tmq);
}

void tmqSendHbReq(void* param, void* tmrId) {
  int64_t refId = (int64_t)param;

  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    return;
  }

  SMqHbReq req = {0};
  req.consumerId = tmq->consumerId;
  req.epoch = atomic_load_32(&tmq->epoch);
  req.pollFlag = atomic_load_8(&tmq->pollFlag);
  tqDebugC("consumer:0x%" PRIx64 " send heartbeat, pollFlag:%d", tmq->consumerId, req.pollFlag);
  req.topics = taosArrayInit(taosArrayGetSize(tmq->clientTopics), sizeof(TopicOffsetRows));
  if (req.topics == NULL) {
    goto END;
  }
  buildVgHeartbeatData(tmq, &req);

  int32_t tlen = tSerializeSMqHbReq(NULL, 0, &req);
  if (tlen < 0) {
    tqErrorC("tSerializeSMqHbReq failed, size:%d", tlen);
    goto END;
  }

  void* pReq = taosMemoryCalloc(1, tlen);
  if (pReq == NULL) {
    tqErrorC("failed to malloc MqHbReq msg, code:%d", terrno);
    goto END;
  }

  if (tSerializeSMqHbReq(pReq, tlen, &req) < 0) {
    tqErrorC("tSerializeSMqHbReq %d failed", tlen);
    taosMemoryFree(pReq);
    goto END;
  }

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(pReq);
    goto END;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = pReq, .len = tlen, .handle = NULL};

  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = (void*)refId;
  sendInfo->fp = tmqHbCb;
  sendInfo->msgType = TDMT_MND_TMQ_HB;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int32_t code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, NULL, sendInfo);
  if (code != 0) {
    tqErrorC("tmqSendHbReq asyncSendMsgToServer failed");
  }
  (void)atomic_val_compare_exchange_8(&tmq->pollFlag, 1, 0);

  END:
  tDestroySMqHbReq(&req);
  if (tmrId != NULL) {
    bool ret = taosTmrReset(tmqSendHbReq, tmq->heartBeatIntervalMs, param, tmqMgmt.timer, &tmq->hbLiveTimer);
    tqDebugC("consumer:0x%" PRIx64 " reset timer for tmq heartbeat ret:%d, interval:%d, pollFlag:%d", tmq->consumerId, ret, tmq->heartBeatIntervalMs, tmq->pollFlag);
  }
  int32_t ret = taosReleaseRef(tmqMgmt.rsetId, refId);
  if (ret != 0){
    tqErrorC("failed to release ref:%"PRId64 ", code:%d", refId, ret);
  }
}

static void defaultCommitCbFn(tmq_t* pTmq, int32_t code, void* param) {
  if (code != 0 && pTmq != NULL) {
    tqErrorC("consumer:0x%" PRIx64 ", failed to commit offset, code:%s", pTmq->consumerId, tstrerror(code));
  }
}

void tmqFreeRspWrapper(SMqRspWrapper* rspWrapper) {
  if (rspWrapper == NULL) {
    return;
  }
  if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__EP_RSP) {
    tDeleteSMqAskEpRsp(&rspWrapper->epRsp);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_RSP) {
    DELETE_POLL_RSP(tDeleteMqDataRsp, &pRsp->dataRsp)
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP){
    DELETE_POLL_RSP(tDeleteSTaosxRsp, &pRsp->dataRsp)
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP) {
    DELETE_POLL_RSP(tDeleteMqMetaRsp,&pRsp->metaRsp)
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_BATCH_META_RSP) {
    DELETE_POLL_RSP(tDeleteMqBatchMetaRsp,&pRsp->batchMetaRsp)
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_RAW_DATA_RSP) {
    DELETE_POLL_RSP(tDeleteMqRawDataRsp, &pRsp->dataRsp)
  }
}

void freeClientVg(void* param) {
  if (param == NULL) {
    return;
  }
  SMqClientVg* pVg = param;
  tqTraceC("freeClientVg vgId:%d", pVg->vgId);
  tOffsetDestroy(&pVg->offsetInfo.endOffset);
  tOffsetDestroy(&pVg->offsetInfo.beginOffset);
  tOffsetDestroy(&pVg->offsetInfo.committedOffset);
}
void freeClientTopic(void* param) {
  if (param == NULL) {
    return;
  }
  SMqClientTopic* pTopic = param;
  tqTraceC("freeClientTopic topic:%s, vgs:%d", pTopic->topicName, (int)taosArrayGetSize(pTopic->vgs));
  taosArrayDestroyEx(pTopic->vgs, freeClientVg);
}

static void initClientTopicFromRsp(SMqClientTopic* pTopic, SMqSubTopicEp* pTopicEp, SHashObj* pVgOffsetHashMap,
                                   tmq_t* tmq) {
  if (pTopic == NULL || pTopicEp == NULL || pVgOffsetHashMap == NULL || tmq == NULL) {
    return;
  }

  char    vgKey[TSDB_TOPIC_FNAME_LEN + 22] = {0};
  int32_t vgNumGet = taosArrayGetSize(pTopicEp->vgs);

  tstrncpy(pTopic->topicName, pTopicEp->topic, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pTopic->db, pTopicEp->db, TSDB_DB_FNAME_LEN);

  tqInfoC("consumer:0x%" PRIx64 ", update topic:%s, new numOfVgs:%d", tmq->consumerId, pTopic->topicName, vgNumGet);
  pTopic->vgs = taosArrayInit(vgNumGet, sizeof(SMqClientVg));
  if (pTopic->vgs == NULL) {
    tqErrorC("consumer:0x%" PRIx64 ", failed to init vgs for topic:%s", tmq->consumerId, pTopic->topicName);
    return;
  }
  for (int32_t j = 0; j < vgNumGet; j++) {
    SMqSubVgEp* pVgEp = taosArrayGet(pTopicEp->vgs, j);
    if (pVgEp == NULL) {
      continue;
    }
    (void)snprintf(vgKey, sizeof(vgKey), "%s:%d", pTopic->topicName, pVgEp->vgId);
    SVgroupSaveInfo* pInfo = taosHashGet(pVgOffsetHashMap, vgKey, strlen(vgKey));

    STqOffsetVal offsetNew = {0};
    offsetNew.type = tmq->resetOffsetCfg;

    tqInfoC("consumer:0x%" PRIx64 ", update topic:%s, new numOfVgs:%d, num:%d, port:%d", tmq->consumerId,
            pTopic->topicName, vgNumGet, pVgEp->epSet.numOfEps, pVgEp->epSet.eps[pVgEp->epSet.inUse].port);

    SMqClientVg clientVg = {
        .pollCnt = 0,
        .vgId = pVgEp->vgId,
        .epSet = pVgEp->epSet,
        .vgStatus = pInfo ? pInfo->vgStatus : TMQ_VG_STATUS__IDLE,
        .vgSkipCnt = 0,
        .emptyBlockReceiveTs = 0,
        .blockReceiveTs = 0,
        .blockSleepForReplay = 0,
        .numOfRows = pInfo ? pInfo->numOfRows : 0,
    };

    clientVg.offsetInfo.walVerBegin = -1;
    clientVg.offsetInfo.walVerEnd = -1;
    clientVg.seekUpdated = false;
    if (pInfo) {
      tOffsetCopy(&clientVg.offsetInfo.endOffset, &pInfo->currentOffset);
      tOffsetCopy(&clientVg.offsetInfo.committedOffset, &pInfo->commitOffset);
      tOffsetCopy(&clientVg.offsetInfo.beginOffset, &pInfo->seekOffset);
      clientVg.offsetInfo.walVerBegin = pInfo->walVerBegin;
      clientVg.offsetInfo.walVerEnd = pInfo->walVerEnd;
    } else {
      clientVg.offsetInfo.endOffset = offsetNew;
      clientVg.offsetInfo.committedOffset = offsetNew;
      clientVg.offsetInfo.beginOffset = offsetNew;
    }
    if (taosArrayPush(pTopic->vgs, &clientVg) == NULL) {
      tqErrorC("consumer:0x%" PRIx64 ", failed to push vg:%d into topic:%s", tmq->consumerId, pVgEp->vgId,
               pTopic->topicName);
      freeClientVg(&clientVg);
    }
  }
}

static void buildNewTopicList(tmq_t* tmq, SArray* newTopics, const SMqAskEpRsp* pRsp){
  if (tmq == NULL || newTopics == NULL || pRsp == NULL) {
    return;
  }
  SHashObj* pVgOffsetHashMap = taosHashInit(64, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pVgOffsetHashMap == NULL) {
    tqErrorC("consumer:0x%" PRIx64 " taos hash init null, code:%d", tmq->consumerId, terrno);
    return;
  }

  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  for (int32_t i = 0; i < topicNumCur; i++) {
    // find old topic
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if (pTopicCur && pTopicCur->vgs) {
      int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
      tqInfoC("consumer:0x%" PRIx64 ", current vg num:%d", tmq->consumerId, vgNumCur);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        if (pVgCur == NULL) {
          continue;
        }
        char vgKey[TSDB_TOPIC_FNAME_LEN + 22] = {0};
        (void)snprintf(vgKey, sizeof(vgKey), "%s:%d", pTopicCur->topicName, pVgCur->vgId);

        char buf[TSDB_OFFSET_LEN] = {0};
        tFormatOffset(buf, TSDB_OFFSET_LEN, &pVgCur->offsetInfo.endOffset);
        tqInfoC("consumer:0x%" PRIx64 ", vgId:%d vgKey:%s, offset:%s", tmq->consumerId, pVgCur->vgId, vgKey, buf);

        SVgroupSaveInfo info = {.currentOffset = pVgCur->offsetInfo.endOffset,
            .seekOffset = pVgCur->offsetInfo.beginOffset,
            .commitOffset = pVgCur->offsetInfo.committedOffset,
            .numOfRows = pVgCur->numOfRows,
            .vgStatus = pVgCur->vgStatus,
            .walVerBegin = pVgCur->offsetInfo.walVerBegin,
            .walVerEnd = pVgCur->offsetInfo.walVerEnd
        };
        if (taosHashPut(pVgOffsetHashMap, vgKey, strlen(vgKey), &info, sizeof(SVgroupSaveInfo)) != 0) {
          tqErrorC("consumer:0x%" PRIx64 ", failed to put vg:%d into hashmap", tmq->consumerId, pVgCur->vgId);
        }
      }
    }
  }

  for (int32_t i = 0; i < taosArrayGetSize(pRsp->topics); i++) {
    SMqClientTopic topic = {0};
    SMqSubTopicEp* pTopicEp = taosArrayGet(pRsp->topics, i);
    if (pTopicEp == NULL) {
      continue;
    }
    initClientTopicFromRsp(&topic, pTopicEp, pVgOffsetHashMap, tmq);
    if (taosArrayPush(newTopics, &topic) == NULL) {
      tqErrorC("consumer:0x%" PRIx64 ", failed to push topic:%s into new topics", tmq->consumerId, topic.topicName);
      freeClientTopic(&topic);
    }
  }

  taosHashCleanup(pVgOffsetHashMap);
}

static void doUpdateLocalEp(tmq_t* tmq, int32_t epoch, const SMqAskEpRsp* pRsp) {
  if (tmq == NULL || pRsp == NULL) {
    return;
  }
  int32_t topicNumGet = taosArrayGetSize(pRsp->topics);
  // vnode transform (epoch == tmq->epoch && topicNumGet != 0)
  // ask ep rsp (epoch == tmq->epoch && topicNumGet == 0)
  if (epoch < atomic_load_32(&tmq->epoch) || (epoch == atomic_load_32(&tmq->epoch) && topicNumGet == 0)) {
    tqDebugC("consumer:0x%" PRIx64 " no update ep epoch from %d to epoch %d, incoming topics:%d", tmq->consumerId,
             tmq->epoch, epoch, topicNumGet);
    return;
  }

  SArray* newTopics = taosArrayInit(topicNumGet, sizeof(SMqClientTopic));
  if (newTopics == NULL) {
    tqErrorC("consumer:0x%" PRIx64 " taos array init null, code:%d", tmq->consumerId, terrno);
    return;
  }
  tqInfoC("consumer:0x%" PRIx64 " update ep epoch from %d to epoch %d, incoming topics:%d, existed topics:%d",
          tmq->consumerId, tmq->epoch, epoch, topicNumGet, (int)taosArrayGetSize(tmq->clientTopics));

  tmqWlock(tmq);
  if (topicNumGet > 0){
    buildNewTopicList(tmq, newTopics, pRsp);
  }
  // destroy current buffered existed topics info
  if (tmq->clientTopics) {
    taosArrayDestroyEx(tmq->clientTopics, freeClientTopic);
  }
  tmq->clientTopics = newTopics;
  tmqWUnlock(tmq);

  atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__READY);
  atomic_store_32(&tmq->epoch, epoch);

  tqInfoC("consumer:0x%" PRIx64 " update topic info completed", tmq->consumerId);
}

static int32_t askEpCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqAskEpRsp rsp = {0};

  SMqAskEpCbParam* pParam = (SMqAskEpCbParam*)param;
  if (pParam == NULL) {
    goto _ERR;
  }

  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, pParam->refId);
  if (tmq == NULL) {
    code = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    goto _ERR;
  }

  if (code != TSDB_CODE_SUCCESS) {
    tqErrorC("consumer:0x%" PRIx64 ", get topic endpoint error, code:%s", tmq->consumerId, tstrerror(code));
    if (code == TSDB_CODE_MND_CONSUMER_NOT_EXIST){
      atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__CLOSED);
    }
    goto END;
  }

  if (pMsg == NULL) {
    goto END;
  }
  SMqRspHead* head = pMsg->pData;
  int32_t     epoch = atomic_load_32(&tmq->epoch);
  tqDebugC("consumer:0x%" PRIx64 ", recv ep, msg epoch %d, current epoch %d", tmq->consumerId, head->epoch, epoch);

  if (tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &rsp) == NULL) {
    code = TSDB_CODE_TMQ_INVALID_MSG;
    tqErrorC("consumer:0x%" PRIx64 ", decode ep rsp failed", tmq->consumerId);
    goto END;
  }

  if (rsp.code != TSDB_CODE_SUCCESS) {
    code = rsp.code;
    goto END;
  }

  if (pParam->sync) {
    doUpdateLocalEp(tmq, head->epoch, &rsp);
  } else {
    SMqRspWrapper* pWrapper = NULL;
    code = taosAllocateQitem(sizeof(SMqRspWrapper), DEF_QITEM, 0, (void**)&pWrapper);
    if (code) {
      goto END;
    }

    pWrapper->tmqRspType = TMQ_MSG_TYPE__EP_RSP;
    pWrapper->epoch = head->epoch;
    TSWAP(pWrapper->epRsp, rsp);
    code = taosWriteQitem(tmq->mqueue, pWrapper);
    if (code != 0) {
      tmqFreeRspWrapper((SMqRspWrapper*)pWrapper);
      taosFreeQitem(pWrapper);
      tqErrorC("consumer:0x%" PRIx64 " put ep res into mqueue failed, code:%d", tmq->consumerId, code);
    }
  }

END:
  tDeleteSMqAskEpRsp(&rsp);
  int32_t ret = taosReleaseRef(tmqMgmt.rsetId, pParam->refId);
  if (ret != 0){
    tqErrorC("failed to release ref:%"PRId64 ", code:%d", pParam->refId, ret);
  }

_ERR:
  if (pParam && pParam->sync) {
    SAskEpInfo* pInfo = pParam->pParam;
    if (pInfo) {
      pInfo->code = code;
      if (tsem2_post(&pInfo->sem) != 0){
        tqErrorC("failed to post rsp sem askep cb");
      }
    }
  }

  if (pMsg) {
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pMsg->pData);
  }

  return code;
}

int32_t askEp(tmq_t* pTmq, void* param, bool sync, bool updateEpSet) {
  if (pTmq == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;
  int32_t lino = 0;
  SMqAskEpReq req = {0};
  req.consumerId = pTmq->consumerId;
  req.epoch = updateEpSet ? -1 : atomic_load_32(&pTmq->epoch);
  tstrncpy(req.cgroup, pTmq->groupId, TSDB_CGROUP_LEN);
  SMqAskEpCbParam* pParam = NULL;
  void*            pReq = NULL;

  int32_t tlen = tSerializeSMqAskEpReq(NULL, 0, &req);
  TSDB_CHECK_CONDITION(tlen >= 0, code, lino, END, TSDB_CODE_INVALID_PARA);
  pReq = taosMemoryCalloc(1, tlen);
  TSDB_CHECK_NULL(pReq, code, lino, END, terrno);

  code = tSerializeSMqAskEpReq(pReq, tlen, &req);
  TSDB_CHECK_CONDITION(code >= 0, code, lino, END, TSDB_CODE_INVALID_PARA);

  pParam = taosMemoryCalloc(1, sizeof(SMqAskEpCbParam));
  TSDB_CHECK_NULL(pParam, code, lino, END, terrno);

  pParam->refId = pTmq->refId;
  pParam->sync = sync;
  pParam->pParam = param;

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  TSDB_CHECK_NULL(sendInfo, code, lino, END, terrno);

  sendInfo->msgInfo = (SDataBuf){.pData = pReq, .len = tlen, .handle = NULL};
  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->paramFreeFp = taosAutoMemoryFree;
  sendInfo->fp = askEpCb;
  sendInfo->msgType = TDMT_MND_TMQ_ASK_EP;

  pReq = NULL;
  pParam = NULL;

  SEpSet epSet = getEpSet_s(&pTmq->pTscObj->pAppInfo->mgmtEp);
  tqDebugC("consumer:0x%" PRIx64 " ask ep from mnode, QID:0x%" PRIx64, pTmq->consumerId, sendInfo->requestId);
  code = asyncSendMsgToServer(pTmq->pTscObj->pAppInfo->pTransporter, &epSet, NULL, sendInfo);

END:
  if (code != 0) {
    tqErrorC("%s failed at %d, msg:%s", __func__, lino, tstrerror(code));
  }
  taosMemoryFree(pReq);
  taosMemoryFree(pParam);
  return code;
}

static int32_t tmqHandleAllDelayedTask(tmq_t* pTmq) {
  tqDebugC("consumer:0x%" PRIx64 " handle delayed %d tasks before poll data", pTmq->consumerId, taosQueueItemSize(pTmq->delayedTask));
  while (1) {
    int8_t* pTaskType = NULL;
    taosReadQitem(pTmq->delayedTask, (void**)&pTaskType);
    if (pTaskType == NULL) {break;}
    if (*pTaskType == TMQ_DELAYED_TASK__ASK_EP) {
      tqDebugC("consumer:0x%" PRIx64 " retrieve ask ep timer", pTmq->consumerId);
      int32_t code = askEp(pTmq, NULL, false, false);
      if (code != 0) {
        tqErrorC("consumer:0x%" PRIx64 " failed to ask ep, code:%s", pTmq->consumerId, tstrerror(code));
      }
      tqDebugC("consumer:0x%" PRIx64 " retrieve ep from mnode in 1s", pTmq->consumerId);
      bool ret = taosTmrReset(tmqAssignAskEpTask, DEFAULT_ASKEP_INTERVAL, (void*)(pTmq->refId), tmqMgmt.timer,
                              &pTmq->epTimer);
      tqDebugC("reset timer for tmq ask ep:%d", ret);
    } else if (*pTaskType == TMQ_DELAYED_TASK__COMMIT) {
      tmq_commit_cb* pCallbackFn = (pTmq->commitCb != NULL) ? pTmq->commitCb : defaultCommitCbFn;
      asyncCommitAllOffsets(pTmq, pCallbackFn, pTmq->commitCbUserParam);
      tqDebugC("consumer:0x%" PRIx64 " next commit to vnode(s) in %.2fs", pTmq->consumerId,
               pTmq->autoCommitInterval / 1000.0);
      bool ret = taosTmrReset(tmqAssignDelayedCommitTask, pTmq->autoCommitInterval, (void*)(pTmq->refId), tmqMgmt.timer,
                              &pTmq->commitTimer);
      tqDebugC("reset timer for commit:%d", ret);
    } else {
      tqErrorC("consumer:0x%" PRIx64 " invalid task type:%d", pTmq->consumerId, *pTaskType);
    }

    taosFreeQitem(pTaskType);
  }

  return 0;
}

void tmqClearUnhandleMsg(tmq_t* tmq) {
  if (tmq == NULL) return;
  while (1) {
    SMqRspWrapper* rspWrapper = NULL;
    taosReadQitem(tmq->mqueue, (void**)&rspWrapper);
    if (rspWrapper == NULL) break;
    tmqFreeRspWrapper(rspWrapper);
    taosFreeQitem(rspWrapper);
  }
}

int32_t tmqSubscribeCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (pMsg) {
    taosMemoryFreeClear(pMsg->pEpSet);
    taosMemoryFreeClear(pMsg->pData);
  }

  if (param == NULL) {
    return code;
  }

  SMqSubscribeCbParam* pParam = (SMqSubscribeCbParam*)param;
  pParam->rspErr = code;

  if (tsem2_post(&pParam->rspSem) != 0){
    tqErrorC("failed to post sem, subscribe cb");
  }
  return 0;
}

int32_t tmq_subscription(tmq_t* tmq, tmq_list_t** topics) {
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;
  if (*topics == NULL) {
    *topics = tmq_list_new();
    if (*topics == NULL) {
      return terrno;
    }
  }
  tmqRlock(tmq);
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* topic = taosArrayGet(tmq->clientTopics, i);
    if (topic == NULL) {
      tqErrorC("topic is null");
      continue;
    }
    char* tmp = strchr(topic->topicName, '.');
    if (tmp == NULL) {
      tqErrorC("topic name is invalid:%s", topic->topicName);
      continue;
    }
    if (tmq_list_append(*topics, tmp + 1) != 0) {
      tqErrorC("failed to append topic:%s", tmp + 1);
      continue;
    }
  }
  tmqRUnlock(tmq);
  return 0;
}

void tmqFreeImpl(void* handle) {
  if (handle == NULL) return;
  tmq_t*  tmq = (tmq_t*)handle;
  int64_t id = tmq->consumerId;

  if (tmq->mqueue) {
    tmqClearUnhandleMsg(tmq);
    taosCloseQueue(tmq->mqueue);
  }

  if (tmq->delayedTask) {
    taosCloseQueue(tmq->delayedTask);
  }

  if(tsem2_destroy(&tmq->rspSem) != 0) {
    tqErrorC("failed to destroy sem in free tmq");
  }

  taosArrayDestroyEx(tmq->clientTopics, freeClientTopic);
  taos_close_internal(tmq->pTscObj);

  if (tmq->commitTimer) {
    if (!taosTmrStopA(&tmq->commitTimer)) {
      tqErrorC("failed to stop commit timer");
    }
  }
  if (tmq->epTimer) {
    if (!taosTmrStopA(&tmq->epTimer)) {
      tqErrorC("failed to stop ep timer");
    }
  }
  if (tmq->hbLiveTimer) {
    if (!taosTmrStopA(&tmq->hbLiveTimer)) {
      tqErrorC("failed to stop hb timer");
    }
  }
  taosMemoryFree(tmq);

  tqInfoC("consumer:0x%" PRIx64 " closed", id);
}

void tmqMgmtInit(void) {
  tmqInitRes = 0;

  if (taosThreadMutexInit(&tmqMgmt.lock, NULL) != 0){
    goto END;
  }

  tmqMgmt.timer = taosTmrInit(1000, 100, 360000, "TMQ");

  if (tmqMgmt.timer == NULL) {
    goto END;
  }

  tmqMgmt.rsetId = taosOpenRef(10000, tmqFreeImpl);
  if (tmqMgmt.rsetId < 0) {
    goto END;
  }

  return;
END:
  tmqInitRes = terrno;
}

void tmqMgmtClose(void) {
  if (tmqMgmt.timer) {
    taosTmrCleanUp(tmqMgmt.timer);
    tmqMgmt.timer = NULL;
  }

  if (tmqMgmt.rsetId > 0) {
    (void) taosThreadMutexLock(&tmqMgmt.lock);
    tmq_t *tmq = taosIterateRef(tmqMgmt.rsetId, 0);
    int64_t  refId = 0;

    while (tmq) {
      refId = tmq->refId;
      if (refId == 0) {
        break;
      }
      atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__CLOSED);
      tmq = taosIterateRef(tmqMgmt.rsetId, refId);
    }
    taosCloseRef(tmqMgmt.rsetId);
    tmqMgmt.rsetId = -1;
    (void)taosThreadMutexUnlock(&tmqMgmt.lock);
  }
  (void)taosThreadMutexUnlock(&tmqMgmt.lock);
}

tmq_t* tmq_consumer_new(tmq_conf_t* conf, char* errstr, int32_t errstrLen) {
  int32_t code = 0;

  if (conf == NULL) {
    SET_ERROR_MSG_TMQ("configure is null")
    return NULL;
  }
  code = taosThreadOnce(&tmqInit, tmqMgmtInit);
  if (code != 0) {
    tqErrorC("failed to tmqInit, code:%s", tstrerror(code));
    SET_ERROR_MSG_TMQ("tmq init error")
    return NULL;
  }
  if (tmqInitRes != 0) {
    SET_ERROR_MSG_TMQ("tmqInitRes init error")
    return NULL;
  }

  tmq_t* pTmq = taosMemoryCalloc(1, sizeof(tmq_t));
  if (pTmq == NULL) {
    tqErrorC("failed to create consumer, code:%s", terrstr());
    SET_ERROR_MSG_TMQ("malloc tmq failed")
    return NULL;
  }

  const char* user = conf->user == NULL ? TSDB_DEFAULT_USER : conf->user;
  const char* pass = conf->pass == NULL ? TSDB_DEFAULT_PASS : conf->pass;

  pTmq->clientTopics = taosArrayInit(0, sizeof(SMqClientTopic));
  if (pTmq->clientTopics == NULL) {
    tqErrorC("failed to init topic array, since:%s", terrstr());
    SET_ERROR_MSG_TMQ("malloc client topics failed")
    goto _failed;
  }
  code = taosOpenQueue(&pTmq->mqueue);
  if (code) {
    tqErrorC("open mqueue failed since %s", tstrerror(code));
    SET_ERROR_MSG_TMQ("open mqueue failed")
    goto _failed;
  }

  code = taosOpenQueue(&pTmq->delayedTask);
  if (code) {
    tqErrorC("open delayed task queue failed since %s", tstrerror(code));
    SET_ERROR_MSG_TMQ("open delayed task queue failed")
    goto _failed;
  }

  if (conf->groupId[0] == 0) {
    SET_ERROR_MSG_TMQ("group is empty")
    goto _failed;
  }

  // init status
  pTmq->status = TMQ_CONSUMER_STATUS__INIT;
  pTmq->pollCnt = 0;
  pTmq->epoch = 0;
  pTmq->pollFlag = 0;

  // set conf
  tstrncpy(pTmq->clientId, conf->clientId, TSDB_CLIENT_ID_LEN);
  tstrncpy(pTmq->groupId, conf->groupId, TSDB_CGROUP_LEN);
  pTmq->withTbName = conf->withTbName;
  pTmq->useSnapshot = conf->snapEnable;
  pTmq->enableWalMarker = conf->enableWalMarker;
  pTmq->autoCommit = conf->autoCommit;
  pTmq->autoCommitInterval = conf->autoCommitInterval;
  pTmq->sessionTimeoutMs = conf->sessionTimeoutMs;
  pTmq->heartBeatIntervalMs = conf->heartBeatIntervalMs;
  pTmq->maxPollIntervalMs = conf->maxPollIntervalMs;
  pTmq->commitCb = conf->commitCb;
  pTmq->commitCbUserParam = conf->commitCbUserParam;
  pTmq->resetOffsetCfg = conf->resetOffset;
  pTmq->replayEnable = conf->replayEnable;
  pTmq->sourceExcluded = conf->sourceExcluded;
  pTmq->rawData = conf->rawData;
  pTmq->maxPollWaitTime = conf->maxPollWaitTime;
  pTmq->minPollRows = conf->minPollRows;
  pTmq->enableBatchMeta = conf->enableBatchMeta;
  tstrncpy(pTmq->user, user, TSDB_USER_LEN);
  if (taosGetFqdn(pTmq->fqdn) != 0) {
    tstrncpy(pTmq->fqdn, "localhost", TSDB_FQDN_LEN);
  }
  if (conf->replayEnable) {
    pTmq->autoCommit = false;
  }
  taosInitRWLatch(&pTmq->lock);

  // assign consumerId
  pTmq->consumerId = tGenIdPI64();

  // init semaphore
  if (tsem2_init(&pTmq->rspSem, 0, 0) != 0) {
    tqErrorC("consumer:0x %" PRIx64 " init semaphore failed since %s, consumer group %s", pTmq->consumerId, terrstr(), pTmq->groupId);
    SET_ERROR_MSG_TMQ("init t_sem failed")
    goto _failed;
  }

  if (conf->token != NULL) {
    code = taos_connect_by_auth(conf->ip, NULL, conf->token, NULL, NULL, conf->port, CONN_TYPE__TMQ, &pTmq->pTscObj);
    if (code) {
      tqErrorC("consumer:0x%" PRIx64 " connect by token failed since %s, groupId:%s", pTmq->consumerId, terrstr(), pTmq->groupId);
      SET_ERROR_MSG_TMQ(terrstr())
      goto _failed;
    }
  } else {
    // init connection
    code = taos_connect_internal(conf->ip, user, pass, NULL, NULL, conf->port, CONN_TYPE__TMQ, &pTmq->pTscObj);
    if (code) {
      tqErrorC("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, terrstr(), pTmq->groupId);
      SET_ERROR_MSG_TMQ(terrstr())
      goto _failed;
    }
  }
  
  pTmq->refId = taosAddRef(tmqMgmt.rsetId, pTmq);
  if (pTmq->refId < 0) {
    SET_ERROR_MSG_TMQ("add tscObj ref failed")
    goto _failed;
  }

  pTmq->hbLiveTimer = taosTmrStart(tmqSendHbReq, pTmq->heartBeatIntervalMs, (void*)pTmq->refId, tmqMgmt.timer);
  if (pTmq->hbLiveTimer == NULL) {
    SET_ERROR_MSG_TMQ("start heartbeat timer failed")
    goto _failed;
  }
  char         buf[TSDB_OFFSET_LEN] = {0};
  STqOffsetVal offset = {.type = pTmq->resetOffsetCfg};
  tFormatOffset(buf, tListLen(buf), &offset);
  tqInfoC("consumer:0x%" PRIx64 " is setup, refId:%" PRId64
              ", groupId:%s, snapshot:%d, autoCommit:%d, commitInterval:%dms, offset:%s, maxPollIntervalMs:%dms, sessionTimeoutMs:%dms",
          pTmq->consumerId, pTmq->refId, pTmq->groupId, pTmq->useSnapshot, pTmq->autoCommit, pTmq->autoCommitInterval,
          buf, pTmq->maxPollIntervalMs, pTmq->sessionTimeoutMs);

  return pTmq;

_failed:
  tmqFreeImpl(pTmq);
  return NULL;
}

int32_t syncAskEp(tmq_t* pTmq) {
  if (pTmq == NULL) return TSDB_CODE_INVALID_PARA;
  SAskEpInfo* pInfo = taosMemoryMalloc(sizeof(SAskEpInfo));
  if (pInfo == NULL) return terrno;
  if (tsem2_init(&pInfo->sem, 0, 0) != 0) {
    taosMemoryFree(pInfo);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  int32_t code = askEp(pTmq, pInfo, true, false);
  if (code == 0) {
    if (tsem2_wait(&pInfo->sem) != 0){
      tqErrorC("consumer:0x%" PRIx64 ", failed to wait for sem", pTmq->consumerId);
    }
    code = pInfo->code;
  }

  if(tsem2_destroy(&pInfo->sem) != 0) {
    tqErrorC("failed to destroy sem sync ask ep");
  }
  taosMemoryFree(pInfo);
  return code;
}

int32_t tmq_subscribe(tmq_t* tmq, const tmq_list_t* topic_list) {
  if (tmq == NULL || topic_list == NULL) return TSDB_CODE_INVALID_PARA;
  const SArray*   container = &topic_list->container;
  int32_t         sz = taosArrayGetSize(container);
  void*           buf = NULL;
  SMsgSendInfo*   sendInfo = NULL;
  SCMSubscribeReq req = {0};
  int32_t         code = 0;

  tqInfoC("consumer:0x%" PRIx64 " cgroup:%s, subscribe %d topics", tmq->consumerId, tmq->groupId, sz);

  req.consumerId = tmq->consumerId;
  tstrncpy(req.clientId, tmq->clientId, TSDB_CLIENT_ID_LEN);
  tstrncpy(req.cgroup, tmq->groupId, TSDB_CGROUP_LEN);
  tstrncpy(req.user, tmq->user, TSDB_USER_LEN);
  tstrncpy(req.fqdn, tmq->fqdn, TSDB_FQDN_LEN);

  req.topicNames = taosArrayInit(sz, sizeof(void*));
  if (req.topicNames == NULL) {
    code = terrno;
    goto END;
  }

  req.withTbName = tmq->withTbName;
  req.autoCommit = tmq->autoCommit;
  req.autoCommitInterval = tmq->autoCommitInterval;
  req.sessionTimeoutMs = tmq->sessionTimeoutMs;
  req.maxPollIntervalMs = tmq->maxPollIntervalMs;
  req.resetOffsetCfg = tmq->resetOffsetCfg;
  req.enableReplay = tmq->replayEnable;
  req.enableBatchMeta = tmq->enableBatchMeta;

  for (int32_t i = 0; i < sz; i++) {
    char* topic = taosArrayGetP(container, i);
    if (topic == NULL) {
      code = terrno;
      goto END;
    }
    SName name = {0};
    code = tNameSetDbName(&name, tmq->pTscObj->acctId, topic, strlen(topic));
    if (code) {
      tqErrorC("consumer:0x%" PRIx64 " cgroup:%s, failed to set topic name, code:%d", tmq->consumerId, tmq->groupId,
               code);
      goto END;
    }
    char* topicFName = taosMemoryCalloc(1, TSDB_TOPIC_FNAME_LEN);
    if (topicFName == NULL) {
      code = terrno;
      goto END;
    }

    code = tNameExtractFullName(&name, topicFName);
    if (code) {
      tqErrorC("consumer:0x%" PRIx64 " cgroup:%s, failed to extract topic name, code:%d", tmq->consumerId, tmq->groupId,
               code);
      taosMemoryFree(topicFName);
      goto END;
    }

    if (taosArrayPush(req.topicNames, &topicFName) == NULL) {
      code = terrno;
      taosMemoryFree(topicFName);
      goto END;
    }
    tqInfoC("consumer:0x%" PRIx64 " subscribe topic:%s", tmq->consumerId, topicFName);
  }

  int32_t tlen = tSerializeSCMSubscribeReq(NULL, &req);
  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    code = terrno;
    goto END;
  }

  void* abuf = buf;
  tlen = tSerializeSCMSubscribeReq(&abuf, &req);

  sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    code = terrno;
    taosMemoryFree(buf);
    goto END;
  }

  SMqSubscribeCbParam param = {.rspErr = 0};
  if (tsem2_init(&param.rspSem, 0, 0) != 0) {
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    taosMemoryFree(buf);
    taosMemoryFree(sendInfo);
    goto END;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = buf, .len = tlen, .handle = NULL};
  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = &param;
  sendInfo->fp = tmqSubscribeCb;
  sendInfo->msgType = TDMT_MND_TMQ_SUBSCRIBE;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, NULL, sendInfo);
  if (code != 0) {
    goto END;
  }

  if (tsem2_wait(&param.rspSem) != 0){
    tqErrorC("consumer:0x%" PRIx64 ", failed to wait semaphore in subscribe", tmq->consumerId);
  }
  if(tsem2_destroy(&param.rspSem) != 0) {
    tqErrorC("consumer:0x%" PRIx64 ", failed to destroy semaphore in subscribe", tmq->consumerId);
  }

  if (param.rspErr != 0) {
    code = param.rspErr;
    goto END;
  }

  int32_t retryCnt = 0;
  while ((code = syncAskEp(tmq)) != 0) {
    if (retryCnt++ > SUBSCRIBE_RETRY_MAX_COUNT || code == TSDB_CODE_MND_CONSUMER_NOT_EXIST) {
      tqErrorC("consumer:0x%" PRIx64 ", mnd not ready for subscribe, retry more than 2 minutes or code:%s",
               tmq->consumerId, tstrerror(code));
      if (code == TSDB_CODE_MND_CONSUMER_NOT_EXIST) {
        code = 0;
      }
      goto END;
    }

    tqInfoC("consumer:0x%" PRIx64 ", mnd not ready for subscribe, retry:%d in 500ms", tmq->consumerId, retryCnt);
    taosMsleep(SUBSCRIBE_RETRY_INTERVAL);
  }

  if (tmq->epTimer == NULL){
    tmq->epTimer = taosTmrStart(tmqAssignAskEpTask, DEFAULT_ASKEP_INTERVAL, (void*)(tmq->refId), tmqMgmt.timer);
    if (tmq->epTimer == NULL) {
      code = TSDB_CODE_TSC_INTERNAL_ERROR;
      goto END;
    }
  }
  if (tmq->autoCommit && tmq->commitTimer == NULL){
    tmq->commitTimer = taosTmrStart(tmqAssignDelayedCommitTask, tmq->autoCommitInterval, (void*)(tmq->refId), tmqMgmt.timer);
    if (tmq->commitTimer == NULL) {
      code = TSDB_CODE_TSC_INTERNAL_ERROR;
      goto END;
    }
  }

  END:
  taosArrayDestroyP(req.topicNames, NULL);
  return code;
}

void getVgInfo(tmq_t* tmq, char* topicName, int32_t vgId, SMqClientVg** pVg) {
  if (tmq == NULL || topicName == NULL || pVg == NULL) {
    return;
  }
  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  for (int i = 0; i < topicNumCur; i++) {
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if (pTopicCur && strcmp(pTopicCur->topicName, topicName) == 0) {
      int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        if (pVgCur && pVgCur->vgId == vgId) {
          *pVg = pVgCur;
          tqTraceC("consumer:0x%" PRIx64 " getVgInfo found topic:%s vgId:%d", tmq->consumerId, topicName, vgId);
          return;
        }
      }
    }
  }
  tqTraceC("consumer:0x%" PRIx64 " getVgInfo not found topic:%s vgId:%d, total topics:%d", tmq->consumerId, topicName, vgId, topicNumCur);
}

SMqClientTopic* getTopicInfo(tmq_t* tmq, char* topicName) {
  if (tmq == NULL || topicName == NULL) {
    return NULL;
  }
  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  for (int i = 0; i < topicNumCur; i++) {
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if (strcmp(pTopicCur->topicName, topicName) == 0) {
      tqTraceC("consumer:0x%" PRIx64 " getTopicInfo found topic:%s", tmq->consumerId, topicName);
      return pTopicCur;
    }
  }
  tqTraceC("consumer:0x%" PRIx64 " getTopicInfo not found topic:%s, total:%d", tmq->consumerId, topicName, topicNumCur);
  return NULL;
}

int32_t tmqPollCb(void* param, SDataBuf* pMsg, int32_t code) {
  tmq_t*             tmq = NULL;
  SMqRspWrapper*     pRspWrapper = NULL;
  int8_t             rspType = 0;
  int32_t            vgId = 0;
  uint64_t           requestId = 0;
  SMqPollCbParam*    pParam = (SMqPollCbParam*)param;
  if (pMsg == NULL) {
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  if (pParam == NULL) {
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    goto EXIT;
  }
  int64_t refId = pParam->refId;
  vgId = pParam->vgId;
  requestId = pParam->requestId;
  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    code = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    goto EXIT;
  }

  int32_t ret = taosAllocateQitem(sizeof(SMqRspWrapper), DEF_QITEM, 0, (void**)&pRspWrapper);
  if (ret) {
    code = ret;
    tqWarnC("consumer:0x%" PRIx64 " msg discard from vgId:%d, since out of memory", tmq->consumerId, vgId);
    goto END;
  }

  if (code != 0) {
    goto END;
  }

  if (pMsg->pData == NULL) {
    tqErrorC("consumer:0x%" PRIx64 " msg discard from vgId:%d, since msg is NULL", tmq->consumerId, vgId);
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    goto END;
  }

  int32_t msgEpoch = ((SMqRspHead*)pMsg->pData)->epoch;
  int32_t clientEpoch = atomic_load_32(&tmq->epoch);

  if (msgEpoch != clientEpoch) {
    tqWarnC("consumer:0x%" PRIx64" msg discard from vgId:%d since epoch not equal, rsp epoch %d, current epoch %d, reqId:0x%" PRIx64,
             tmq->consumerId, vgId, msgEpoch, clientEpoch, requestId);
    code = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
    goto END;
  }
  rspType = ((SMqRspHead*)pMsg->pData)->mqMsgType;
  tqDebugC("consumer:0x%" PRIx64 " recv poll rsp, vgId:%d, type %d(%s), QID:0x%" PRIx64, tmq->consumerId, vgId, rspType, tmqMsgTypeStr[rspType], requestId);

  pRspWrapper->tmqRspType = rspType;
  pRspWrapper->pollRsp.reqId = requestId;
  pRspWrapper->pollRsp.pEpset = pMsg->pEpSet;
  pRspWrapper->pollRsp.data = pMsg->pData;
  pRspWrapper->pollRsp.len = pMsg->len;
  pMsg->pData = NULL;
  pMsg->pEpSet = NULL;

  END:
  if (pRspWrapper) {
    pRspWrapper->code = code;
    pRspWrapper->pollRsp.vgId = vgId;
    tstrncpy(pRspWrapper->pollRsp.topicName, pParam->topicName, TSDB_TOPIC_FNAME_LEN);
    code = taosWriteQitem(tmq->mqueue, pRspWrapper);
    if (code != 0) {
      tmqFreeRspWrapper(pRspWrapper);
      taosFreeQitem(pRspWrapper);
      tqErrorC("consumer:0x%" PRIx64 " put poll res into mqueue failed, code:%d", tmq->consumerId, code);
    } else {
      tqDebugC("consumer:0x%" PRIx64 " put poll res into mqueue, type:%d(%s), vgId:%d, total in queue:%d, QID:0x%" PRIx64,
               tmq ? tmq->consumerId : 0, rspType, tmqMsgTypeStr[rspType], vgId, taosQueueItemSize(tmq->mqueue), requestId);
    }
  }

  if (tsem2_post(&tmq->rspSem) != 0){
    tqErrorC("failed to post rsp sem, consumer:0x%" PRIx64, tmq->consumerId);
  }
  ret = taosReleaseRef(tmqMgmt.rsetId, refId);
  if (ret != 0){
    tqErrorC("failed to release ref:%"PRId64 ", code:%d", refId, ret);
  }

  EXIT:
  taosMemoryFreeClear(pMsg->pData);
  taosMemoryFreeClear(pMsg->pEpSet);
  return code;
}

void tmqBuildConsumeReqImpl(SMqPollReq* pReq, tmq_t* tmq, SMqClientTopic* pTopic, SMqClientVg* pVg) {
  if (pReq == NULL || tmq == NULL || pTopic == NULL || pVg == NULL) {
    return;
  }
  (void)snprintf(pReq->subKey, TSDB_SUBSCRIBE_KEY_LEN, "%s%s%s", tmq->groupId, TMQ_SEPARATOR, pTopic->topicName);
  pReq->withTbName = tmq->withTbName;
  pReq->consumerId = tmq->consumerId;
  pReq->timeout = tmq->maxPollWaitTime;
  pReq->minPollRows = tmq->minPollRows;
  pReq->epoch = atomic_load_32(&tmq->epoch);
  pReq->reqOffset = pVg->offsetInfo.endOffset;
  pReq->head.vgId = pVg->vgId;
  pReq->useSnapshot = tmq->useSnapshot;
  pReq->reqId = generateRequestId();
  pReq->enableReplay = tmq->replayEnable;
  pReq->sourceExcluded = tmq->sourceExcluded;
  pReq->rawData = tmq->rawData;
  pReq->enableBatchMeta = tmq->enableBatchMeta;
}

void changeByteEndian(char* pData) {
  if (pData == NULL) {
    return;
  }
  char* p = pData;

  // | version | total length | total rows | total columns | flag seg| block group id | column schema | each column
  // length | version:
  int32_t blockVersion = *(int32_t*)p;
  if (blockVersion != BLOCK_VERSION_1) {
    tqErrorC("invalid block version:%d", blockVersion);
    return;
  }
  *(int32_t*)p = BLOCK_VERSION_2;

  p += sizeof(int32_t);
  p += sizeof(int32_t);
  p += sizeof(int32_t);
  int32_t cols = *(int32_t*)p;
  p += sizeof(int32_t);
  p += sizeof(int32_t);
  p += sizeof(uint64_t);
  // check fields
  p += cols * (sizeof(int8_t) + sizeof(int32_t));

  int32_t* colLength = (int32_t*)p;
  for (int32_t i = 0; i < cols; ++i) {
    colLength[i] = htonl(colLength[i]);
  }
}

static void tmqGetRawDataRowsPrecisionFromRes(void* pRetrieve, void** rawData, int64_t* rows, int32_t* precision) {
  if (pRetrieve == NULL || rawData == NULL || rows == NULL) {
    return;
  }
  if (*(int64_t*)pRetrieve == 0) {
    *rawData = ((SRetrieveTableRsp*)pRetrieve)->data;
    *rows = htobe64(((SRetrieveTableRsp*)pRetrieve)->numOfRows);
    if (precision != NULL) {
      *precision = ((SRetrieveTableRsp*)pRetrieve)->precision;
    }
  } else if (*(int64_t*)pRetrieve == 1) {
    *rawData = ((SRetrieveTableRspForTmq*)pRetrieve)->data;
    *rows = htobe64(((SRetrieveTableRspForTmq*)pRetrieve)->numOfRows);
    if (precision != NULL) {
      *precision = ((SRetrieveTableRspForTmq*)pRetrieve)->precision;
    }
  }
}

static void tmqBuildRspFromWrapperInner(SMqPollRspWrapper* pWrapper, SMqClientVg* pVg, int64_t* numOfRows,
                                        SMqRspObj* pRspObj) {
  if (pWrapper == NULL || pVg == NULL || numOfRows == NULL || pRspObj == NULL) {
    return;
  }
  pRspObj->resIter = -1;
  pRspObj->resInfo.totalRows = 0;
  pRspObj->resInfo.precision = TSDB_TIME_PRECISION_MILLI;

  SMqDataRsp* pDataRsp = &pRspObj->dataRsp;
  // extract the rows in this data packet
  for (int32_t i = 0; i < pDataRsp->blockNum; ++i) {
    void*   pRetrieve = taosArrayGetP(pDataRsp->blockData, i);
    void*   rawData = NULL;
    int64_t rows = 0;
    // deal with compatibility
    tmqGetRawDataRowsPrecisionFromRes(pRetrieve, &rawData, &rows, NULL);

    pVg->numOfRows += rows;
    (*numOfRows) += rows;
    changeByteEndian(rawData);
  }
}

static int32_t doTmqPollImpl(tmq_t* pTmq, SMqClientTopic* pTopic, SMqClientVg* pVg) {
  SMqPollReq      req = {0};
  char*           msg = NULL;
  SMqPollCbParam* pParam = NULL;
  SMsgSendInfo*   sendInfo = NULL;
  int             code = 0;
  int             lino = 0;
  tmqBuildConsumeReqImpl(&req, pTmq, pTopic, pVg);

  int32_t msgSize = tSerializeSMqPollReq(NULL, 0, &req);
  TSDB_CHECK_CONDITION(msgSize >= 0, code, lino, END, TSDB_CODE_INVALID_MSG);

  msg = taosMemoryCalloc(1, msgSize);
  TSDB_CHECK_NULL(msg, code, lino, END, terrno);

  TSDB_CHECK_CONDITION(tSerializeSMqPollReq(msg, msgSize, &req) >= 0, code, lino, END, TSDB_CODE_INVALID_MSG);

  pParam = taosMemoryMalloc(sizeof(SMqPollCbParam));
  TSDB_CHECK_NULL(pParam, code, lino, END, terrno);

  pParam->refId = pTmq->refId;
  tstrncpy(pParam->topicName, pTopic->topicName, TSDB_TOPIC_FNAME_LEN);
  pParam->vgId = pVg->vgId;
  pParam->requestId = req.reqId;

  sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  TSDB_CHECK_NULL(sendInfo, code, lino, END, terrno);

  sendInfo->msgInfo = (SDataBuf){.pData = msg, .len = msgSize, .handle = NULL};
  sendInfo->requestId = req.reqId;
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->paramFreeFp = taosAutoMemoryFree;
  sendInfo->fp = tmqPollCb;
  sendInfo->msgType = TDMT_VND_TMQ_CONSUME;

  msg = NULL;
  pParam = NULL;

  char offsetFormatBuf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(offsetFormatBuf, tListLen(offsetFormatBuf), &pVg->offsetInfo.endOffset);
  code = asyncSendMsgToServer(pTmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, NULL, sendInfo);
  tqDebugC("consumer:0x%" PRIx64 " send poll to %s vgId:%d, code:%d, epoch %d, req:%s, QID:0x%" PRIx64, pTmq->consumerId,
           pTopic->topicName, pVg->vgId, code, pTmq->epoch, offsetFormatBuf, req.reqId);
  TSDB_CHECK_CODE(code, lino, END);

  pVg->pollCnt++;
  pVg->seekUpdated = false;  // reset this flag.
  pTmq->pollCnt++;

END:
  if (code != 0){
    tqErrorC("%s failed at %d msg:%s", __func__, lino, tstrerror(code));
  }
  taosMemoryFreeClear(pParam);
  taosMemoryFreeClear(msg);
  return code;
}

static int32_t tmqPollImpl(tmq_t* tmq) {
  if (tmq == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  tmqWlock(tmq);

  int32_t code = atomic_load_32(&tmq->tokenCode);
  if (code == TSDB_CODE_MND_TOKEN_NOT_EXIST || code == TSDB_CODE_MND_TOKEN_DISABLED || code == TSDB_CODE_MND_TOKEN_EXPIRED){
    goto end;
  } else {
    code = 0;
  }

  if (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__CLOSED){
    code = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
    goto end;
  }

  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  tqDebugC("consumer:0x%" PRIx64 " start to poll data, numOfTopics:%d", tmq->consumerId, numOfTopics);

  for (int i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (pTopic == NULL) {
      continue;
    }
    int32_t numOfVg = taosArrayGetSize(pTopic->vgs);
    if (pTopic->noPrivilege) {
      tqDebugC("consumer:0x%" PRIx64 " has no privilegr for topic:%s", tmq->consumerId, pTopic->topicName);
      continue;
    }
    for (int j = 0; j < numOfVg; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (pVg == NULL) {
        continue;
      }

      int64_t elapsed = taosGetTimestampMs() - pVg->emptyBlockReceiveTs;
      if (elapsed < EMPTY_BLOCK_POLL_IDLE_DURATION && elapsed >= 0) {  // less than EMPTY_BLOCK_POLL_IDLE_DURATION
        tqDebugC("consumer:0x%" PRIx64 " epoch %d, vgId:%d idle for %" PRId64 "ms before start next poll",
                 tmq->consumerId, tmq->epoch, pVg->vgId, elapsed);
        continue;
      }

      elapsed = taosGetTimestampMs() - pVg->blockReceiveTs;
      if (tmq->replayEnable && elapsed < pVg->blockSleepForReplay && elapsed >= 0) {
        tqDebugC("consumer:0x%" PRIx64 " epoch %d, vgId:%d idle for %" PRId64 "ms before start next poll when replay",
                 tmq->consumerId, tmq->epoch, pVg->vgId, pVg->blockSleepForReplay);
        continue;
      }

      int32_t vgStatus = atomic_val_compare_exchange_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE, TMQ_VG_STATUS__WAIT);
      if (vgStatus == TMQ_VG_STATUS__WAIT) {
        int32_t vgSkipCnt = atomic_add_fetch_32(&pVg->vgSkipCnt, 1);
        if (vgSkipCnt % 10000 == 0) {
          tqInfoC("consumer:0x%" PRIx64 " epoch %d, vgId:%d has skipped poll %d times in a row", tmq->consumerId,
                  tmq->epoch, pVg->vgId, vgSkipCnt);
        }
        tqDebugC("consumer:0x%" PRIx64 " epoch %d wait poll-rsp, skip vgId:%d skip cnt %d", tmq->consumerId, tmq->epoch,
                 pVg->vgId, vgSkipCnt);
        continue;
      }

      atomic_store_32(&pVg->vgSkipCnt, 0);
      code = doTmqPollImpl(tmq, pTopic, pVg);
      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
    }
  }

  end:
  tmqWUnlock(tmq);
  tqDebugC("consumer:0x%" PRIx64 " end to poll data, code:%d", tmq->consumerId, code);
  return code;
}

static void updateVgInfo(SMqClientVg* pVg, STqOffsetVal* reqOffset, STqOffsetVal* rspOffset, int64_t sver, int64_t ever,
                         int64_t consumerId, bool hasData) {
  if (pVg == NULL || reqOffset == NULL || rspOffset == NULL) {
    return;
  }
  if (!pVg->seekUpdated) {
    tqDebugC("consumer:0x%" PRIx64 " local offset is update, since seekupdate not set", consumerId);
    if (hasData) {
      tOffsetCopy(&pVg->offsetInfo.beginOffset, reqOffset);
    }
    tOffsetCopy(&pVg->offsetInfo.endOffset, rspOffset);
  } else {
    tqDebugC("consumer:0x%" PRIx64 " local offset is NOT update, since seekupdate is set", consumerId);
  }

  // update the status
  atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);

  // update the valid wal version range
  pVg->offsetInfo.walVerBegin = sver;
  pVg->offsetInfo.walVerEnd = ever + 1;
}

static SMqRspObj* buildRsp(SMqPollRspWrapper* pollRspWrapper){
  typedef union {
    SMqDataRsp      dataRsp;
    SMqMetaRsp      metaRsp;
    SMqBatchMetaRsp batchMetaRsp;
  } MEMSIZE;

  SMqRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqRspObj));
  if (pRspObj == NULL) {
    tqErrorC("buildRsp:failed to allocate memory");
    return NULL;
  }
  (void)memcpy(&pRspObj->dataRsp, &pollRspWrapper->dataRsp, sizeof(MEMSIZE));
  tstrncpy(pRspObj->topic, pollRspWrapper->topicName, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pRspObj->db, pollRspWrapper->topicHandle->db, TSDB_DB_FNAME_LEN);
  pRspObj->vgId = pollRspWrapper->vgId;
  (void)memset(&pollRspWrapper->dataRsp, 0, sizeof(MEMSIZE));
  return pRspObj;
}

static int32_t processMqRspError(tmq_t* tmq, SMqRspWrapper* pRspWrapper){
  int32_t code = pRspWrapper->code;
  SMqPollRspWrapper* pollRspWrapper = &pRspWrapper->pollRsp;

  tqErrorC("consumer:0x%" PRIx64 " msg from vgId:%d discarded, since %s", tmq->consumerId, pollRspWrapper->vgId,
    tstrerror(pRspWrapper->code));
  if (pRspWrapper->code == TSDB_CODE_VND_INVALID_VGROUP_ID ||   // for vnode transform
      pRspWrapper->code == TSDB_CODE_SYN_NOT_LEADER) {          // for vnode split
    int32_t ret = askEp(tmq, NULL, false, true);
    if (ret != 0) {
      tqErrorC("consumer:0x%" PRIx64 " failed to ask ep when vnode transform, ret:%s", tmq->consumerId, tstrerror(ret));
    }
  } else if (pRspWrapper->code == TSDB_CODE_TMQ_CONSUMER_MISMATCH) {
    code = syncAskEp(tmq);
    if (code != 0) {
      tqWarnC("consumer:0x%" PRIx64 " failed to ask ep when consumer mismatch, code:%s", tmq->consumerId, tstrerror(code));
    }
  } else if (pRspWrapper->code == TSDB_CODE_TMQ_NO_TABLE_QUALIFIED){
    code = 0;
  }
  
  tmqWlock(tmq);
  SMqClientVg* pVg = NULL;
  getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId, &pVg);
  if (pVg) {
    pVg->emptyBlockReceiveTs = taosGetTimestampMs();
    atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
  }
  tmqWUnlock(tmq);

  return code;
}

static int32_t processWrapperData(SMqRspWrapper* pRspWrapper){
  int32_t code = 0;
  if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_RSP) {
    PROCESS_POLL_RSP(tDecodeMqDataRsp, &pRspWrapper->pollRsp.dataRsp)
    pRspWrapper->pollRsp.dataRsp.data = pRspWrapper->pollRsp.data;
    pRspWrapper->pollRsp.data = NULL;
  } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP) {
    PROCESS_POLL_RSP(tDecodeMqMetaRsp, &pRspWrapper->pollRsp.metaRsp)
  } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
    PROCESS_POLL_RSP(tDecodeSTaosxRsp, &pRspWrapper->pollRsp.dataRsp)
    pRspWrapper->pollRsp.dataRsp.data = pRspWrapper->pollRsp.data;
    pRspWrapper->pollRsp.data = NULL;
  } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_BATCH_META_RSP) {
    PROCESS_POLL_RSP(tSemiDecodeMqBatchMetaRsp, &pRspWrapper->pollRsp.batchMetaRsp)
  } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_RAW_DATA_RSP) {
    PROCESS_POLL_RSP(tDecodeMqRawDataRsp, &pRspWrapper->pollRsp.dataRsp)
    pRspWrapper->pollRsp.dataRsp.len = pRspWrapper->pollRsp.len - sizeof(SMqRspHead);
    pRspWrapper->pollRsp.dataRsp.rawData = POINTER_SHIFT(pRspWrapper->pollRsp.data, sizeof(SMqRspHead));
    pRspWrapper->pollRsp.data = NULL;
  } else {
    tqErrorC("invalid rsp msg received, type:%d ignored", pRspWrapper->tmqRspType);
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    goto END;
  }
  END:
  return code;
}

static int32_t processMqRsp(tmq_t* tmq, SMqRspWrapper* pRspWrapper, SMqRspObj** pRspObj){
  int32_t    code = 0;

  if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__EP_RSP) {
    tqDebugC("consumer:0x%" PRIx64 " ep msg received", tmq->consumerId);
    SMqAskEpRsp*        rspMsg = &pRspWrapper->epRsp;
    doUpdateLocalEp(tmq, pRspWrapper->epoch, rspMsg);
    return code;
  }

  code = processWrapperData(pRspWrapper);
  if (code != 0) {
    return code;
  }
  SMqPollRspWrapper* pollRspWrapper = &pRspWrapper->pollRsp;
  tmqWlock(tmq);
  SMqClientVg* pVg = NULL;
  getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId, &pVg);
  if(pVg == NULL) {
    tqErrorC("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
             pollRspWrapper->topicName, pollRspWrapper->vgId);
    code = TSDB_CODE_TMQ_INVALID_VGID;
    goto END;
  }
  pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
  if (pollRspWrapper->pEpset != NULL) {
    pVg->epSet = *pollRspWrapper->pEpset;
  }

  if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_RSP ||
      pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP ||
      pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_RAW_DATA_RSP) {
    updateVgInfo(pVg, &pollRspWrapper->dataRsp.reqOffset, &pollRspWrapper->dataRsp.rspOffset, pollRspWrapper->head.walsver, pollRspWrapper->head.walever,
                 tmq->consumerId, pollRspWrapper->dataRsp.blockNum != 0);
    
    if (pollRspWrapper->dataRsp.timeout) {
      tqInfoC("consumer:0x%" PRIx64 " poll data timeout, vgId:%d", tmq->consumerId, pVg->vgId);
      code = TSDB_CODE_TMQ_FETCH_TIMEOUT;
      goto END;
    }
    char buf[TSDB_OFFSET_LEN] = {0};
    tFormatOffset(buf, TSDB_OFFSET_LEN, &pollRspWrapper->rspOffset);
    if (pollRspWrapper->dataRsp.blockNum == 0) {
      pVg->emptyBlockReceiveTs = taosGetTimestampMs();
      tqDebugC("consumer:0x%" PRIx64 " empty block received, vgId:%d, offset:%s, vg total:%" PRId64
                   ", total:%" PRId64 ", QID:0x%" PRIx64,
               tmq->consumerId, pVg->vgId, buf, pVg->numOfRows, tmq->totalRows, pollRspWrapper->reqId);
    } else {
      *pRspObj = buildRsp(pollRspWrapper);
      if (*pRspObj == NULL) {
        tqErrorC("consumer:0x%" PRIx64 " failed to allocate memory for meta rsp", tmq->consumerId);
        code = terrno;
        goto END;
      }
      (*pRspObj)->resType = pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_RAW_DATA_RSP ? RES_TYPE__TMQ_RAWDATA :
                         (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_RSP ? RES_TYPE__TMQ : RES_TYPE__TMQ_METADATA);
      int64_t numOfRows = 0;
      if (pRspWrapper->tmqRspType != TMQ_MSG_TYPE__POLL_RAW_DATA_RSP){
        tmqBuildRspFromWrapperInner(pollRspWrapper, pVg, &numOfRows, *pRspObj);
        tmq->totalRows += numOfRows;
      }
      if (tmq->replayEnable && pRspWrapper->tmqRspType != TMQ_MSG_TYPE__POLL_RAW_DATA_RSP) {
        pVg->blockReceiveTs = taosGetTimestampMs();
        pVg->blockSleepForReplay = (*pRspObj)->dataRsp.sleepTime;
        if (pVg->blockSleepForReplay > 0) {
          if (taosTmrStart(tmqReplayTask, pVg->blockSleepForReplay, (void*)(tmq->refId), tmqMgmt.timer) == NULL) {
            tqErrorC("consumer:0x%" PRIx64 " failed to start replay timer, vgId:%d, sleep:%" PRId64,
                     tmq->consumerId, pVg->vgId, pVg->blockSleepForReplay);
          }
        }
      }
      pVg->emptyBlockReceiveTs = 0;
      tqDebugC("consumer:0x%" PRIx64 " process poll rsp, vgId:%d, offset:%s, blocks:%d, rows:%" PRId64
                   ", vg total:%" PRId64 ", total:%" PRId64 ", QID:0x%" PRIx64,
               tmq->consumerId, pVg->vgId, buf, (*pRspObj)->dataRsp.blockNum, numOfRows, pVg->numOfRows, tmq->totalRows,
               pollRspWrapper->reqId);
    }
  } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP || pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_BATCH_META_RSP) {
    updateVgInfo(pVg, &pollRspWrapper->rspOffset, &pollRspWrapper->rspOffset,
                 pollRspWrapper->head.walsver, pollRspWrapper->head.walever, tmq->consumerId, true);

    *pRspObj = buildRsp(pollRspWrapper);
    if (*pRspObj == NULL) {
      tqErrorC("consumer:0x%" PRIx64 " failed to allocate memory for meta rsp", tmq->consumerId);
      code = terrno;
      goto END;
    }
    (*pRspObj)->resType = pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP ? RES_TYPE__TMQ_META : RES_TYPE__TMQ_BATCH_META;
  }

END:
  tmqWUnlock(tmq);
  return code;
}

static int32_t tmqHandleAllRsp(tmq_t* tmq, SMqRspObj** rspObj) {
  tqDebugC("consumer:0x%" PRIx64 " start to handle the rsp, total:%d", tmq->consumerId, taosQueueItemSize(tmq->mqueue));

  int32_t code = 0;
  while (1) {
    SMqRspWrapper* pRspWrapper = NULL;
    taosReadQitem(tmq->mqueue, (void**)&pRspWrapper);
    if (pRspWrapper == NULL) {break;}

    tqDebugC("consumer:0x%" PRIx64 " handle rsp, type:%s", tmq->consumerId, tmqMsgTypeStr[pRspWrapper->tmqRspType]);
    if (pRspWrapper->code != 0) {
      code = processMqRspError(tmq, pRspWrapper);
    }else{
      code = processMqRsp(tmq, pRspWrapper, rspObj);
    }

    tmqFreeRspWrapper(pRspWrapper);
    taosFreeQitem(pRspWrapper);
    if(*rspObj != NULL || code != 0){
      break;
    }
  }

END:
  return code;
}

static void printResult(TAOS_RES* res) {
  typedef union {
    SMqDataRsp      dataRsp;
    SMqMetaRsp      metaRsp;
    SMqBatchMetaRsp batchMetaRsp;
  } MEMSIZE;
  SMqRspObj* msg = (SMqRspObj*)res;

  SMqRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqRspObj));
  if (pRspObj == NULL) {
    tqErrorC("%s:failed to allocate memory", __func__);
    return;
  }
  (void)memcpy(&pRspObj->dataRsp, &msg->dataRsp, sizeof(MEMSIZE));
  tstrncpy(pRspObj->topic, msg->topic, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pRspObj->db, msg->db, TSDB_DB_FNAME_LEN);
  pRspObj->vgId = msg->vgId;

  const char* topicName = tmq_get_topic_name(pRspObj);
  const char* dbName = tmq_get_db_name(pRspObj);
  int32_t     vgroupId = tmq_get_vgroup_id(pRspObj);
  char        buf[1024] = {0};

  while (1) {
    TAOS_ROW row = taos_fetch_row(pRspObj);
    if (row == NULL) break;

    TAOS_FIELD* fields = taos_fetch_fields(pRspObj);
    int32_t     numOfFields = taos_field_count(pRspObj);
    int32_t precision = taos_result_precision(pRspObj);
    taos_print_row_with_size(buf, sizeof(buf), row, fields, numOfFields);
    tqTraceC("topic: %s, db: %s, vgId: %d, precision: %d, row content: %s", topicName, dbName, vgroupId, precision, buf);
  }
  taosMemoryFree(pRspObj);
}

static int64_t getElapsedTime(int64_t startTime) {
  int64_t currentTime = taosGetTimestampMs();
  int64_t elapsedTime = currentTime - startTime;
  return elapsedTime;
}

TAOS_RES* tmq_consumer_poll(tmq_t* tmq, int64_t timeout) {
  int32_t lino = 0;
  int32_t code = 0;
  terrno = 0;
  TSDB_CHECK_NULL(tmq, code, lino, END, TSDB_CODE_INVALID_PARA);

  int64_t startTime = taosGetTimestampMs();

  tqDebugC("%s consumer:0x%" PRIx64 " start to poll at %" PRId64 ", timeout:%" PRId64, __func__, tmq->consumerId, startTime, timeout);
  TSDB_CHECK_CONDITION(atomic_load_8(&tmq->status) != TMQ_CONSUMER_STATUS__INIT, code, lino, END, TSDB_CODE_TMQ_INVALID_STATUS);

  (void)atomic_val_compare_exchange_8(&tmq->pollFlag, 0, 1);

  while (1) {
    code = tmqHandleAllDelayedTask(tmq);
    TSDB_CHECK_CODE(code, lino, END);

    SMqRspObj*   rspObj = NULL;
    code = tmqHandleAllRsp(tmq, &rspObj);
    if (rspObj) {
      tqDebugC("%s consumer:0x%" PRIx64 " end to poll, return rsp:%p", __func__, tmq->consumerId, rspObj);
      if ((rspObj->resType == RES_TYPE__TMQ || rspObj->resType == RES_TYPE__TMQ_METADATA) && (tqClientDebugFlag & DEBUG_TRACE)) {
        printResult((TAOS_RES*)rspObj);
      }
      return (TAOS_RES*)rspObj;
    }
    TSDB_CHECK_CODE(code, lino, END);

    code = tmqPollImpl(tmq);
    TSDB_CHECK_CODE(code, lino, END);

    if (timeout >= 0) {
      int64_t elapsedTime = getElapsedTime(startTime);
      TSDB_CHECK_CONDITION(elapsedTime < timeout && elapsedTime >= 0, code, lino, END, 0);
      int64_t remaining = timeout - elapsedTime;  
      int64_t waitMs = (remaining < EMPTY_BLOCK_POLL_IDLE_DURATION) ? remaining : EMPTY_BLOCK_POLL_IDLE_DURATION;  
      (void)tsem2_timewait(&tmq->rspSem, waitMs);  
    } else {
      (void)tsem2_timewait(&tmq->rspSem, 1000);
    }
  }

END:
  if (code != 0) {
    terrno = code;
    tqErrorC("%s consumer:0x%" PRIx64 " poll error at line:%d, msg:%s", __func__, tmq != NULL ? tmq->consumerId : 0, lino, tstrerror(code));
  } else {
    tqDebugC("%s consumer:0x%" PRIx64 " poll end with timeout, msg:%s", __func__, tmq != NULL ? tmq->consumerId : 0, tstrerror(terrno));
  }
  return NULL;
}

static void displayConsumeStatistics(tmq_t* pTmq) {
  if (pTmq == NULL) return;
  tmqRlock(pTmq);
  int32_t numOfTopics = taosArrayGetSize(pTmq->clientTopics);
  tqInfoC("consumer:0x%" PRIx64 " closing poll:%" PRId64 " rows:%" PRId64 " topics:%d, final epoch:%d",
          pTmq->consumerId, pTmq->pollCnt, pTmq->totalRows, numOfTopics, pTmq->epoch);

  tqInfoC("consumer:0x%" PRIx64 " rows dist begin: ", pTmq->consumerId);
  for (int32_t i = 0; i < numOfTopics; ++i) {
    SMqClientTopic* pTopics = taosArrayGet(pTmq->clientTopics, i);
    if (pTopics == NULL) continue;
    tqInfoC("consumer:0x%" PRIx64 " topic:%d", pTmq->consumerId, i);
    int32_t numOfVgs = taosArrayGetSize(pTopics->vgs);
    for (int32_t j = 0; j < numOfVgs; ++j) {
      SMqClientVg* pVg = taosArrayGet(pTopics->vgs, j);
      if (pVg == NULL) continue;
      tqInfoC("topic:%s, %d. vgId:%d rows:%" PRId64, pTopics->topicName, j, pVg->vgId, pVg->numOfRows);
    }
  }
  tmqRUnlock(pTmq);
  tqInfoC("consumer:0x%" PRIx64 " rows dist end", pTmq->consumerId);
}

int32_t tmq_unsubscribe(tmq_t* tmq) {
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t code = 0;
  int8_t status = atomic_load_8(&tmq->status);
  tqInfoC("consumer:0x%" PRIx64 " start to unsubscribe consumer, status:%d", tmq->consumerId, status);

  displayConsumeStatistics(tmq);
  if (status != TMQ_CONSUMER_STATUS__READY) {
    tqInfoC("consumer:0x%" PRIx64 " status:%d, not in ready state, no need unsubscribe", tmq->consumerId, status);
    goto END;
  }
  if (tmq->autoCommit) {
    code = tmq_commit_sync(tmq, NULL);
    if (code != 0) {
      goto END;
    }
  }
  tmqSendHbReq((void*)(tmq->refId), NULL);

  tmq_list_t* lst = tmq_list_new();
  if (lst == NULL) {
    code = terrno;
    goto END;
  }
  code = tmq_subscribe(tmq, lst);
  tmq_list_destroy(lst);
  tmqClearUnhandleMsg(tmq);
  atomic_store_32(&tmq->epoch, 0);
  if(code != 0){
    goto END;
  }

  END:
  return code;
}

int32_t tmq_consumer_close(tmq_t* tmq) {
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t code = 0;
  (void) taosThreadMutexLock(&tmqMgmt.lock);
  if (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__CLOSED){
    goto end;
  }
  tqInfoC("consumer:0x%" PRIx64 " start to close consumer, status:%d", tmq->consumerId, tmq->status);
  code = tmq_unsubscribe(tmq);
  if (code == 0) {
    atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__CLOSED);
    code = taosRemoveRef(tmqMgmt.rsetId, tmq->refId);
    if (code != 0){
      tqErrorC("tmq close failed to remove ref:%" PRId64 ", code:%d", tmq->refId, code);
    }
  }

end:
  (void)taosThreadMutexUnlock(&tmqMgmt.lock);
  return code;
}

const char* tmq_err2str(int32_t err) {
  if (err == 0) {
    return "success";
  } else if (err == -1) {
    return "fail";
  } else {
    if (*(taosGetErrMsg()) == 0) {
      return tstrerror(err);
    } else {
      (void)snprintf(taosGetErrMsgReturn(), ERR_MSG_LEN, "%s,detail:%s", tstrerror(err), taosGetErrMsg());
      return (const char*)taosGetErrMsgReturn();
    }
  }
}

tmq_res_t tmq_get_res_type(TAOS_RES* res) {
  if (res == NULL) {
    return TMQ_RES_INVALID;
  }
  if (TD_RES_TMQ(res)) {
    return TMQ_RES_DATA;
  } else if (TD_RES_TMQ_META(res)) {
    return TMQ_RES_TABLE_META;
  } else if (TD_RES_TMQ_METADATA(res)) {
    return TMQ_RES_METADATA;
  } else if (TD_RES_TMQ_BATCH_META(res)) {
    return TMQ_RES_TABLE_META;
  } else if (TD_RES_TMQ_RAW(res)) {
    return TMQ_RES_RAWDATA;
  } else {
    return TMQ_RES_INVALID;
  }
}

const char* tmq_get_topic_name(TAOS_RES* res) {
  if (res == NULL) {
    return NULL;
  }
  if (TD_RES_TMQ_RAW(res) || TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    char* tmp = strchr(((SMqRspObj*)res)->topic, '.');
    if (tmp == NULL) {
      return NULL;
    }
    return tmp + 1;
  } else {
    return NULL;
  }
}

const char* tmq_get_db_name(TAOS_RES* res) {
  if (res == NULL) {
    return NULL;
  }

  if (TD_RES_TMQ_RAW(res) || TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_BATCH_META(res) || TD_RES_TMQ_META(res)) {
    char* tmp = strchr(((SMqRspObj*)res)->db, '.');
    if (tmp == NULL) {
      return NULL;
    }
    return tmp + 1;
  } else {
    return NULL;
  }
}

int32_t tmq_get_vgroup_id(TAOS_RES* res) {
  if (res == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (TD_RES_TMQ_RAW(res) || TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_BATCH_META(res) || TD_RES_TMQ_META(res)) {
    return ((SMqRspObj*)res)->vgId;
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

int64_t tmq_get_vgroup_offset(TAOS_RES* res) {
  if (res == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (TD_RES_TMQ_RAW(res) || TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    STqOffsetVal*     pOffset = &pRspObj->dataRsp.reqOffset;
    if (pOffset->type == TMQ_OFFSET__LOG) {
      return pOffset->version;
    } else {
      tqErrorC("invalid offset type:%d", pOffset->type);
    }
  } else if (TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    if (pRspObj->rspOffset.type == TMQ_OFFSET__LOG) {
      return pRspObj->rspOffset.version;
    }
  } else {
    tqErrorC("invalid tmq type:%d", *(int8_t*)res);
  }

  // data from tsdb, no valid offset info
  return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
}

const char* tmq_get_table_name(TAOS_RES* res) {
  if (res == NULL) {
    return NULL;
  }
  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res) ) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    SMqDataRsp* data = &pRspObj->dataRsp;
    if (!data->withTbName || data->blockTbName == NULL || pRspObj->resIter < 0 ||
        pRspObj->resIter >= data->blockNum) {
      return NULL;
    }
    return (const char*)taosArrayGetP(data->blockTbName, pRspObj->resIter);
  }
  return NULL;
}

TAOS* tmq_get_connect(tmq_t* tmq) {
  if (tmq && tmq->pTscObj) {
    return (TAOS*)(&(tmq->pTscObj->id));
  }
  return NULL;
}


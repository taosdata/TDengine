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
#define _DEFAULT_SOURCE
#include "mndDef.h"
#include "mndConsumer.h"
#include "taoserror.h"

static void *freeStreamTasks(SArray *pTaskLevel);

int32_t tEncodeSStreamObj(SEncoder *pEncoder, const SStreamObj *pObj) {
  TAOS_CHECK_RETURN(tStartEncode(pEncoder));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pObj->name));

  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->createTime));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->updateTime));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pObj->version));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pObj->totalLevel));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->smaId));

  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->uid));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pObj->status));

  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pObj->conf.igExpired));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pObj->conf.trigger));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pObj->conf.fillHistory));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->conf.triggerParam));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->conf.watermark));

  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->sourceDbUid));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->targetDbUid));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pObj->sourceDb));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pObj->targetDb));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pObj->targetSTbName));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->targetStbUid));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pObj->fixedSinkVgId));

  if (pObj->sql != NULL) {
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pObj->sql));
  } else {
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, ""));
  }

  if (pObj->ast != NULL) {
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pObj->ast));
  } else {
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, ""));
  }

  if (pObj->physicalPlan != NULL) {
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pObj->physicalPlan));
  } else {
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, ""));
  }

  int32_t sz = taosArrayGetSize(pObj->tasks);
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, sz));
  for (int32_t i = 0; i < sz; i++) {
    SArray *pArray = taosArrayGetP(pObj->tasks, i);
    int32_t innerSz = taosArrayGetSize(pArray);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, innerSz));
    for (int32_t j = 0; j < innerSz; j++) {
      SStreamTask *pTask = taosArrayGetP(pArray, j);
      if (pTask->ver < SSTREAM_TASK_SUBTABLE_CHANGED_VER){
        pTask->ver = SSTREAM_TASK_VER;
      }
      TAOS_CHECK_RETURN(tEncodeStreamTask(pEncoder, pTask));
    }
  }

  TAOS_CHECK_RETURN(tEncodeSSchemaWrapper(pEncoder, &pObj->outputSchema));

  // 3.0.20 ver =2
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->checkpointFreq));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pObj->igCheckUpdate));

  // 3.0.50 ver = 3
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->checkpointId));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pObj->subTableWithoutMd5));

  TAOS_CHECK_RETURN(tEncodeCStrWithLen(pEncoder, pObj->reserve, sizeof(pObj->reserve) - 1));

  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSStreamObj(SDecoder *pDecoder, SStreamObj *pObj, int32_t sver) {
  int32_t code = 0;
  TAOS_CHECK_RETURN(tStartDecode(pDecoder));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pObj->name));

  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->createTime));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->updateTime));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pObj->version));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pObj->totalLevel));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->smaId));

  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->uid));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pObj->status));

  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pObj->conf.igExpired));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pObj->conf.trigger));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pObj->conf.fillHistory));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->conf.triggerParam));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->conf.watermark));

  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->sourceDbUid));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->targetDbUid));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pObj->sourceDb));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pObj->targetDb));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pObj->targetSTbName));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->targetStbUid));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pObj->fixedSinkVgId));

  TAOS_CHECK_RETURN(tDecodeCStrAlloc(pDecoder, &pObj->sql));
  TAOS_CHECK_RETURN(tDecodeCStrAlloc(pDecoder, &pObj->ast));
  TAOS_CHECK_RETURN(tDecodeCStrAlloc(pDecoder, &pObj->physicalPlan));

  if (pObj->tasks != NULL) {
    pObj->tasks = freeStreamTasks(pObj->tasks);
  }

  int32_t sz;
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &sz));

  if (sz != 0) {
    pObj->tasks = taosArrayInit(sz, sizeof(void *));
    if (pObj->tasks == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TAOS_RETURN(code);
    }

    for (int32_t i = 0; i < sz; i++) {
      int32_t innerSz;
      TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &innerSz));
      SArray *pArray = taosArrayInit(innerSz, sizeof(void *));
      if (pArray != NULL) {
        for (int32_t j = 0; j < innerSz; j++) {
          SStreamTask *pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
          if (pTask == NULL) {
            taosArrayDestroy(pArray);
            code = TSDB_CODE_OUT_OF_MEMORY;
            TAOS_RETURN(code);
          }
          if ((code = tDecodeStreamTask(pDecoder, pTask)) < 0) {
            taosMemoryFree(pTask);
            taosArrayDestroy(pArray);
            TAOS_RETURN(code);
          }
          if (taosArrayPush(pArray, &pTask) == NULL) {
            taosMemoryFree(pTask);
            taosArrayDestroy(pArray);
            code = TSDB_CODE_OUT_OF_MEMORY;
            TAOS_RETURN(code);
          }
        }
      }
      if (taosArrayPush(pObj->tasks, &pArray) == NULL) {
        taosArrayDestroy(pArray);
        code = TSDB_CODE_OUT_OF_MEMORY;
        TAOS_RETURN(code);
      }
    }
  }

  TAOS_CHECK_RETURN(tDecodeSSchemaWrapper(pDecoder, &pObj->outputSchema));

  // 3.0.20
  if (sver >= 2) {
    TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->checkpointFreq));
    if (!tDecodeIsEnd(pDecoder)) {
      TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pObj->igCheckUpdate));
    }
  }
  if (sver >= 3) {
    TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->checkpointId));
  }

  if (sver >= 5) {
    TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pObj->subTableWithoutMd5));
  }
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pObj->reserve));

  tEndDecode(pDecoder);
  TAOS_RETURN(code);
}

void *freeStreamTasks(SArray *pTaskLevel) {
  int32_t numOfLevel = taosArrayGetSize(pTaskLevel);

  for (int32_t i = 0; i < numOfLevel; i++) {
    SArray *pLevel = taosArrayGetP(pTaskLevel, i);
    int32_t taskSz = taosArrayGetSize(pLevel);
    for (int32_t j = 0; j < taskSz; j++) {
      SStreamTask *pTask = taosArrayGetP(pLevel, j);
      tFreeStreamTask(pTask);
    }

    taosArrayDestroy(pLevel);
  }

  taosArrayDestroy(pTaskLevel);

  return NULL;
}

void tFreeStreamObj(SStreamObj *pStream) {
  taosMemoryFree(pStream->sql);
  taosMemoryFree(pStream->ast);
  taosMemoryFree(pStream->physicalPlan);

  if (pStream->outputSchema.nCols || pStream->outputSchema.pSchema) {
    taosMemoryFree(pStream->outputSchema.pSchema);
  }

  pStream->tasks = freeStreamTasks(pStream->tasks);
  pStream->pHTasksList = freeStreamTasks(pStream->pHTasksList);

  // tagSchema.pSchema
  if (pStream->tagSchema.nCols > 0) {
    taosMemoryFree(pStream->tagSchema.pSchema);
  }
}

SMqVgEp *tCloneSMqVgEp(const SMqVgEp *pVgEp) {
  SMqVgEp *pVgEpNew = taosMemoryMalloc(sizeof(SMqVgEp));
  if (pVgEpNew == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  pVgEpNew->vgId = pVgEp->vgId;
  //  pVgEpNew->qmsg = taosStrdup(pVgEp->qmsg);
  pVgEpNew->epSet = pVgEp->epSet;
  return pVgEpNew;
}

void tDeleteSMqVgEp(SMqVgEp *pVgEp) {
  if (pVgEp) {
    //    taosMemoryFreeClear(pVgEp->qmsg);
    taosMemoryFree(pVgEp);
  }
}

int32_t tEncodeSMqVgEp(void **buf, const SMqVgEp *pVgEp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pVgEp->vgId);
  //  tlen += taosEncodeString(buf, pVgEp->qmsg);
  tlen += taosEncodeSEpSet(buf, &pVgEp->epSet);
  return tlen;
}

void *tDecodeSMqVgEp(const void *buf, SMqVgEp *pVgEp, int8_t sver) {
  buf = taosDecodeFixedI32(buf, &pVgEp->vgId);
  if (sver == 1) {
    uint64_t size = 0;
    buf = taosDecodeVariantU64(buf, &size);
    buf = POINTER_SHIFT(buf, size);
  }
  buf = taosDecodeSEpSet(buf, &pVgEp->epSet);
  return (void *)buf;
}

static void *topicNameDup(void *p) { return taosStrdup((char *)p); }

int32_t tNewSMqConsumerObj(int64_t consumerId, char *cgroup, int8_t updateType,
                                   char *topic, SCMSubscribeReq *subscribe, SMqConsumerObj** ppConsumer) {
  int32_t code = 0;
  SMqConsumerObj *pConsumer = taosMemoryCalloc(1, sizeof(SMqConsumerObj));
  if (pConsumer == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }

  pConsumer->consumerId = consumerId;
  (void)memcpy(pConsumer->cgroup, cgroup, TSDB_CGROUP_LEN);

  pConsumer->epoch = 0;
  pConsumer->status = MQ_CONSUMER_STATUS_REBALANCE;
  pConsumer->hbStatus = 0;
  pConsumer->pollStatus = 0;

  taosInitRWLatch(&pConsumer->lock);
  pConsumer->createTime = taosGetTimestampMs();
  pConsumer->updateType = updateType;

  if (updateType == CONSUMER_ADD_REB){
    pConsumer->rebNewTopics = taosArrayInit(0, sizeof(void *));
    if(pConsumer->rebNewTopics == NULL){
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }

    char* topicTmp = taosStrdup(topic);
    if (taosArrayPush(pConsumer->rebNewTopics, &topicTmp) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }
  }else if (updateType == CONSUMER_REMOVE_REB) {
    pConsumer->rebRemovedTopics = taosArrayInit(0, sizeof(void *));
    if(pConsumer->rebRemovedTopics == NULL){
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }
    char* topicTmp = taosStrdup(topic);
    if (taosArrayPush(pConsumer->rebRemovedTopics, &topicTmp) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }
  }else if (updateType == CONSUMER_INSERT_SUB){
    tstrncpy(pConsumer->clientId, subscribe->clientId, tListLen(pConsumer->clientId));
    pConsumer->withTbName = subscribe->withTbName;
    pConsumer->autoCommit = subscribe->autoCommit;
    pConsumer->autoCommitInterval = subscribe->autoCommitInterval;
    pConsumer->resetOffsetCfg = subscribe->resetOffsetCfg;
    pConsumer->maxPollIntervalMs = subscribe->maxPollIntervalMs;
    pConsumer->sessionTimeoutMs = subscribe->sessionTimeoutMs;
    tstrncpy(pConsumer->user, subscribe->user, TSDB_USER_LEN);
    tstrncpy(pConsumer->fqdn, subscribe->fqdn, TSDB_FQDN_LEN);

    pConsumer->rebNewTopics = taosArrayDup(subscribe->topicNames, topicNameDup);
    if (pConsumer->rebNewTopics == NULL){
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }
    pConsumer->assignedTopics = subscribe->topicNames;
    subscribe->topicNames = NULL;
  }else if (updateType == CONSUMER_UPDATE_SUB){
    pConsumer->assignedTopics = subscribe->topicNames;
    subscribe->topicNames = NULL;
  }

  *ppConsumer = pConsumer;
  return 0;

END:
  tDeleteSMqConsumerObj(pConsumer);
  return code;
}

void tClearSMqConsumerObj(SMqConsumerObj *pConsumer) {
  if (pConsumer == NULL) return;
  taosArrayDestroyP(pConsumer->currentTopics, NULL);
  taosArrayDestroyP(pConsumer->rebNewTopics, NULL);
  taosArrayDestroyP(pConsumer->rebRemovedTopics, NULL);
  taosArrayDestroyP(pConsumer->assignedTopics, NULL);
}

void tDeleteSMqConsumerObj(SMqConsumerObj *pConsumer) {
  tClearSMqConsumerObj(pConsumer);
  taosMemoryFree(pConsumer);
}

int32_t tEncodeSMqConsumerObj(void **buf, const SMqConsumerObj *pConsumer) {
  int32_t tlen = 0;
  int32_t sz;
  tlen += taosEncodeFixedI64(buf, pConsumer->consumerId);
  tlen += taosEncodeString(buf, pConsumer->clientId);
  tlen += taosEncodeString(buf, pConsumer->cgroup);
  tlen += taosEncodeFixedI8(buf, pConsumer->updateType);
  tlen += taosEncodeFixedI32(buf, pConsumer->epoch);
  tlen += taosEncodeFixedI32(buf, pConsumer->status);

  tlen += taosEncodeFixedI32(buf, pConsumer->pid);
  tlen += taosEncodeSEpSet(buf, &pConsumer->ep);
  tlen += taosEncodeFixedI64(buf, pConsumer->createTime);
  tlen += taosEncodeFixedI64(buf, pConsumer->subscribeTime);
  tlen += taosEncodeFixedI64(buf, pConsumer->rebalanceTime);

  // current topics
  if (pConsumer->currentTopics) {
    sz = taosArrayGetSize(pConsumer->currentTopics);
    tlen += taosEncodeFixedI32(buf, sz);
    for (int32_t i = 0; i < sz; i++) {
      char *topic = taosArrayGetP(pConsumer->currentTopics, i);
      tlen += taosEncodeString(buf, topic);
    }
  } else {
    tlen += taosEncodeFixedI32(buf, 0);
  }

  // reb new topics
  if (pConsumer->rebNewTopics) {
    sz = taosArrayGetSize(pConsumer->rebNewTopics);
    tlen += taosEncodeFixedI32(buf, sz);
    for (int32_t i = 0; i < sz; i++) {
      char *topic = taosArrayGetP(pConsumer->rebNewTopics, i);
      tlen += taosEncodeString(buf, topic);
    }
  } else {
    tlen += taosEncodeFixedI32(buf, 0);
  }

  // reb removed topics
  if (pConsumer->rebRemovedTopics) {
    sz = taosArrayGetSize(pConsumer->rebRemovedTopics);
    tlen += taosEncodeFixedI32(buf, sz);
    for (int32_t i = 0; i < sz; i++) {
      char *topic = taosArrayGetP(pConsumer->rebRemovedTopics, i);
      tlen += taosEncodeString(buf, topic);
    }
  } else {
    tlen += taosEncodeFixedI32(buf, 0);
  }

  // lost topics
  if (pConsumer->assignedTopics) {
    sz = taosArrayGetSize(pConsumer->assignedTopics);
    tlen += taosEncodeFixedI32(buf, sz);
    for (int32_t i = 0; i < sz; i++) {
      char *topic = taosArrayGetP(pConsumer->assignedTopics, i);
      tlen += taosEncodeString(buf, topic);
    }
  } else {
    tlen += taosEncodeFixedI32(buf, 0);
  }

  tlen += taosEncodeFixedI8(buf, pConsumer->withTbName);
  tlen += taosEncodeFixedI8(buf, pConsumer->autoCommit);
  tlen += taosEncodeFixedI32(buf, pConsumer->autoCommitInterval);
  tlen += taosEncodeFixedI32(buf, pConsumer->resetOffsetCfg);
  tlen += taosEncodeFixedI32(buf, pConsumer->maxPollIntervalMs);
  tlen += taosEncodeFixedI32(buf, pConsumer->sessionTimeoutMs);
  tlen += taosEncodeString(buf, pConsumer->user);
  tlen += taosEncodeString(buf, pConsumer->fqdn);
  return tlen;
}

void *tDecodeSMqConsumerObj(const void *buf, SMqConsumerObj *pConsumer, int8_t sver) {
  int32_t sz;
  buf = taosDecodeFixedI64(buf, &pConsumer->consumerId);
  buf = taosDecodeStringTo(buf, pConsumer->clientId);
  buf = taosDecodeStringTo(buf, pConsumer->cgroup);
  buf = taosDecodeFixedI8(buf, &pConsumer->updateType);
  buf = taosDecodeFixedI32(buf, &pConsumer->epoch);
  buf = taosDecodeFixedI32(buf, &pConsumer->status);

  buf = taosDecodeFixedI32(buf, &pConsumer->pid);
  buf = taosDecodeSEpSet(buf, &pConsumer->ep);
  buf = taosDecodeFixedI64(buf, &pConsumer->createTime);
  buf = taosDecodeFixedI64(buf, &pConsumer->subscribeTime);
  buf = taosDecodeFixedI64(buf, &pConsumer->rebalanceTime);

  // current topics
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->currentTopics = taosArrayInit(sz, sizeof(void *));
  if (pConsumer->currentTopics == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = taosDecodeString(buf, &topic);
    if (taosArrayPush(pConsumer->currentTopics, &topic) == NULL) {
      return NULL;
    }
  }

  // reb new topics
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->rebNewTopics = taosArrayInit(sz, sizeof(void *));
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = taosDecodeString(buf, &topic);
    if (taosArrayPush(pConsumer->rebNewTopics, &topic) == NULL) {
      return NULL;
    }
  }

  // reb removed topics
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->rebRemovedTopics = taosArrayInit(sz, sizeof(void *));
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = taosDecodeString(buf, &topic);
    if (taosArrayPush(pConsumer->rebRemovedTopics, &topic) == NULL) {
      return NULL;
    }
  }

  // reb removed topics
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->assignedTopics = taosArrayInit(sz, sizeof(void *));
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = taosDecodeString(buf, &topic);
    if (taosArrayPush(pConsumer->assignedTopics, &topic) == NULL) {
      return NULL;
    }
  }

  if (sver > 1) {
    buf = taosDecodeFixedI8(buf, &pConsumer->withTbName);
    buf = taosDecodeFixedI8(buf, &pConsumer->autoCommit);
    buf = taosDecodeFixedI32(buf, &pConsumer->autoCommitInterval);
    buf = taosDecodeFixedI32(buf, &pConsumer->resetOffsetCfg);
  }
  if (sver > 2){
    buf = taosDecodeFixedI32(buf, &pConsumer->maxPollIntervalMs);
    buf = taosDecodeFixedI32(buf, &pConsumer->sessionTimeoutMs);
    buf = taosDecodeStringTo(buf, pConsumer->user);
    buf = taosDecodeStringTo(buf, pConsumer->fqdn);
  } else{
    pConsumer->maxPollIntervalMs = DEFAULT_MAX_POLL_INTERVAL;
    pConsumer->sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT;
  }

  return (void *)buf;
}

int32_t tEncodeOffRows(void **buf, SArray *offsetRows){
  int32_t tlen = 0;
  int32_t szVgs = taosArrayGetSize(offsetRows);
  tlen += taosEncodeFixedI32(buf, szVgs);
  for (int32_t j = 0; j < szVgs; ++j) {
    OffsetRows *offRows = taosArrayGet(offsetRows, j);
    tlen += taosEncodeFixedI32(buf, offRows->vgId);
    tlen += taosEncodeFixedI64(buf, offRows->rows);
    tlen += taosEncodeFixedI8(buf, offRows->offset.type);
    if (offRows->offset.type == TMQ_OFFSET__SNAPSHOT_DATA || offRows->offset.type == TMQ_OFFSET__SNAPSHOT_META) {
      tlen += taosEncodeFixedI64(buf, offRows->offset.uid);
      tlen += taosEncodeFixedI64(buf, offRows->offset.ts);
    } else if (offRows->offset.type == TMQ_OFFSET__LOG) {
      tlen += taosEncodeFixedI64(buf, offRows->offset.version);
    } else {
      // do nothing
    }
    tlen += taosEncodeFixedI64(buf, offRows->ever);
  }

  return tlen;
}

int32_t tEncodeSMqConsumerEp(void **buf, const SMqConsumerEp *pConsumerEp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pConsumerEp->consumerId);
  tlen += taosEncodeArray(buf, pConsumerEp->vgs, (FEncode)tEncodeSMqVgEp);


  return tlen + tEncodeOffRows(buf, pConsumerEp->offsetRows);
}

void *tDecodeOffRows(const void *buf, SArray **offsetRows, int8_t sver){
  int32_t szVgs = 0;
  buf = taosDecodeFixedI32(buf, &szVgs);
  if (szVgs > 0) {
    *offsetRows = taosArrayInit(szVgs, sizeof(OffsetRows));
    if (NULL == *offsetRows) return NULL;
    for (int32_t j = 0; j < szVgs; ++j) {
      OffsetRows *offRows = taosArrayReserve(*offsetRows, 1);
      buf = taosDecodeFixedI32(buf, &offRows->vgId);
      buf = taosDecodeFixedI64(buf, &offRows->rows);
      buf = taosDecodeFixedI8(buf, &offRows->offset.type);
      if (offRows->offset.type == TMQ_OFFSET__SNAPSHOT_DATA || offRows->offset.type == TMQ_OFFSET__SNAPSHOT_META) {
        buf = taosDecodeFixedI64(buf, &offRows->offset.uid);
        buf = taosDecodeFixedI64(buf, &offRows->offset.ts);
      } else if (offRows->offset.type == TMQ_OFFSET__LOG) {
        buf = taosDecodeFixedI64(buf, &offRows->offset.version);
      } else {
        // do nothing
      }
      if(sver > 2){
        buf = taosDecodeFixedI64(buf, &offRows->ever);
      }
    }
  }
  return (void *)buf;
}

void *tDecodeSMqConsumerEp(const void *buf, SMqConsumerEp *pConsumerEp, int8_t sver) {
  buf = taosDecodeFixedI64(buf, &pConsumerEp->consumerId);
  buf = taosDecodeArray(buf, &pConsumerEp->vgs, (FDecode)tDecodeSMqVgEp, sizeof(SMqVgEp), sver);
  if (sver > 1) {
    buf = tDecodeOffRows(buf, &pConsumerEp->offsetRows, sver);
  }

  return (void *)buf;
}

int32_t tNewSubscribeObj(const char *key, SMqSubscribeObj **ppSub) {
  int32_t code = 0;
  SMqSubscribeObj *pSubObj = taosMemoryCalloc(1, sizeof(SMqSubscribeObj));
  MND_TMQ_NULL_CHECK(pSubObj);

  (void)memcpy(pSubObj->key, key, TSDB_SUBSCRIBE_KEY_LEN);
  taosInitRWLatch(&pSubObj->lock);
  pSubObj->vgNum = 0;
  pSubObj->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  MND_TMQ_NULL_CHECK(pSubObj->consumerHash);
  pSubObj->unassignedVgs = taosArrayInit(0, POINTER_BYTES);
  MND_TMQ_NULL_CHECK(pSubObj->unassignedVgs);
  if (ppSub){
    *ppSub = pSubObj;
  }
  return code;

END:
  taosMemoryFree(pSubObj);
  return code;
}

int32_t tCloneSubscribeObj(const SMqSubscribeObj *pSub, SMqSubscribeObj **ppSub) {
  int32_t code = 0;
  SMqSubscribeObj *pSubNew = taosMemoryMalloc(sizeof(SMqSubscribeObj));
  if (pSubNew == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }
  (void)memcpy(pSubNew->key, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);
  taosInitRWLatch(&pSubNew->lock);

  pSubNew->dbUid = pSub->dbUid;
  pSubNew->stbUid = pSub->stbUid;
  pSubNew->subType = pSub->subType;
  pSubNew->withMeta = pSub->withMeta;

  pSubNew->vgNum = pSub->vgNum;
  pSubNew->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);

  void          *pIter = NULL;
  SMqConsumerEp *pConsumerEp = NULL;
  while (1) {
    pIter = taosHashIterate(pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    pConsumerEp = (SMqConsumerEp *)pIter;
    SMqConsumerEp newEp = {
        .consumerId = pConsumerEp->consumerId,
        .vgs = taosArrayDup(pConsumerEp->vgs, (__array_item_dup_fn_t)tCloneSMqVgEp),
    };
    if ((code = taosHashPut(pSubNew->consumerHash, &newEp.consumerId, sizeof(int64_t), &newEp,
                            sizeof(SMqConsumerEp))) != 0)
      goto END;
  }
  pSubNew->unassignedVgs = taosArrayDup(pSub->unassignedVgs, (__array_item_dup_fn_t)tCloneSMqVgEp);
  pSubNew->offsetRows = taosArrayDup(pSub->offsetRows, NULL);
  (void)memcpy(pSubNew->dbName, pSub->dbName, TSDB_DB_FNAME_LEN);
  pSubNew->qmsg = taosStrdup(pSub->qmsg);
  if (ppSub) {
    *ppSub = pSubNew;
  }
END:
  return code;
}

void tDeleteSubscribeObj(SMqSubscribeObj *pSub) {
  if (pSub == NULL) return;
  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    taosArrayDestroyP(pConsumerEp->vgs, (FDelete)tDeleteSMqVgEp);
    taosArrayDestroy(pConsumerEp->offsetRows);
  }
  taosHashCleanup(pSub->consumerHash);
  taosArrayDestroyP(pSub->unassignedVgs, (FDelete)tDeleteSMqVgEp);
  taosMemoryFreeClear(pSub->qmsg);
  taosArrayDestroy(pSub->offsetRows);
}

int32_t tEncodeSubscribeObj(void **buf, const SMqSubscribeObj *pSub) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pSub->key);
  tlen += taosEncodeFixedI64(buf, pSub->dbUid);
  tlen += taosEncodeFixedI32(buf, pSub->vgNum);
  tlen += taosEncodeFixedI8(buf, pSub->subType);
  tlen += taosEncodeFixedI8(buf, pSub->withMeta);
  tlen += taosEncodeFixedI64(buf, pSub->stbUid);

  void   *pIter = NULL;
  int32_t sz = taosHashGetSize(pSub->consumerHash);
  tlen += taosEncodeFixedI32(buf, sz);

  int32_t cnt = 0;
  while (1) {
    pIter = taosHashIterate(pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    tlen += tEncodeSMqConsumerEp(buf, pConsumerEp);
    cnt++;
  }
  if (cnt != sz) return -1;
  tlen += taosEncodeArray(buf, pSub->unassignedVgs, (FEncode)tEncodeSMqVgEp);
  tlen += taosEncodeString(buf, pSub->dbName);

  tlen += tEncodeOffRows(buf, pSub->offsetRows);
  tlen += taosEncodeString(buf, pSub->qmsg);
  return tlen;
}

void *tDecodeSubscribeObj(const void *buf, SMqSubscribeObj *pSub, int8_t sver) {
  //
  buf = taosDecodeStringTo(buf, pSub->key);
  buf = taosDecodeFixedI64(buf, &pSub->dbUid);
  buf = taosDecodeFixedI32(buf, &pSub->vgNum);
  buf = taosDecodeFixedI8(buf, &pSub->subType);
  buf = taosDecodeFixedI8(buf, &pSub->withMeta);
  buf = taosDecodeFixedI64(buf, &pSub->stbUid);

  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);

  pSub->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp consumerEp = {0};
    buf = tDecodeSMqConsumerEp(buf, &consumerEp, sver);
    if (taosHashPut(pSub->consumerHash, &consumerEp.consumerId, sizeof(int64_t), &consumerEp, sizeof(SMqConsumerEp)) !=
        0)
      return NULL;
  }

  buf = taosDecodeArray(buf, &pSub->unassignedVgs, (FDecode)tDecodeSMqVgEp, sizeof(SMqVgEp), sver);
  buf = taosDecodeStringTo(buf, pSub->dbName);

  if (sver > 1) {
    buf = tDecodeOffRows(buf, &pSub->offsetRows, sver);
    buf = taosDecodeString(buf, &pSub->qmsg);
  } else {
    pSub->qmsg = taosStrdup("");
  }
  return (void *)buf;
}

// SMqSubActionLogEntry *tCloneSMqSubActionLogEntry(SMqSubActionLogEntry *pEntry) {
//   SMqSubActionLogEntry *pEntryNew = taosMemoryMalloc(sizeof(SMqSubActionLogEntry));
//   if (pEntryNew == NULL) return NULL;
//   pEntryNew->epoch = pEntry->epoch;
//   pEntryNew->consumers = taosArrayDup(pEntry->consumers, (__array_item_dup_fn_t)tCloneSMqConsumerEp);
//   return pEntryNew;
// }
//
// void tDeleteSMqSubActionLogEntry(SMqSubActionLogEntry *pEntry) {
//   taosArrayDestroyEx(pEntry->consumers, (FDelete)tDeleteSMqConsumerEp);
// }

// int32_t tEncodeSMqSubActionLogEntry(void **buf, const SMqSubActionLogEntry *pEntry) {
//   int32_t tlen = 0;
//   tlen += taosEncodeFixedI32(buf, pEntry->epoch);
//   tlen += taosEncodeArray(buf, pEntry->consumers, (FEncode)tEncodeSMqSubActionLogEntry);
//   return tlen;
// }
//
// void *tDecodeSMqSubActionLogEntry(const void *buf, SMqSubActionLogEntry *pEntry) {
//   buf = taosDecodeFixedI32(buf, &pEntry->epoch);
//   buf = taosDecodeArray(buf, &pEntry->consumers, (FDecode)tDecodeSMqSubActionLogEntry, sizeof(SMqSubActionLogEntry));
//   return (void *)buf;
// }

// SMqSubActionLogObj *tCloneSMqSubActionLogObj(SMqSubActionLogObj *pLog) {
//   SMqSubActionLogObj *pLogNew = taosMemoryMalloc(sizeof(SMqSubActionLogObj));
//   if (pLogNew == NULL) return pLogNew;
//   memcpy(pLogNew->key, pLog->key, TSDB_SUBSCRIBE_KEY_LEN);
//   pLogNew->logs = taosArrayDup(pLog->logs, (__array_item_dup_fn_t)tCloneSMqConsumerEp);
//   return pLogNew;
// }
//
// void tDeleteSMqSubActionLogObj(SMqSubActionLogObj *pLog) {
//   taosArrayDestroyEx(pLog->logs, (FDelete)tDeleteSMqConsumerEp);
// }

// int32_t tEncodeSMqSubActionLogObj(void **buf, const SMqSubActionLogObj *pLog) {
//   int32_t tlen = 0;
//   tlen += taosEncodeString(buf, pLog->key);
//   tlen += taosEncodeArray(buf, pLog->logs, (FEncode)tEncodeSMqSubActionLogEntry);
//   return tlen;
// }
//
// void *tDecodeSMqSubActionLogObj(const void *buf, SMqSubActionLogObj *pLog) {
//   buf = taosDecodeStringTo(buf, pLog->key);
//   buf = taosDecodeArray(buf, &pLog->logs, (FDecode)tDecodeSMqSubActionLogEntry, sizeof(SMqSubActionLogEntry));
//   return (void *)buf;
// }
//
// int32_t tEncodeSMqOffsetObj(void **buf, const SMqOffsetObj *pOffset) {
//   int32_t tlen = 0;
//   tlen += taosEncodeString(buf, pOffset->key);
//   tlen += taosEncodeFixedI64(buf, pOffset->offset);
//   return tlen;
// }
//
// void *tDecodeSMqOffsetObj(void *buf, SMqOffsetObj *pOffset) {
//   buf = taosDecodeStringTo(buf, pOffset->key);
//   buf = taosDecodeFixedI64(buf, &pOffset->offset);
//   return buf;
// }

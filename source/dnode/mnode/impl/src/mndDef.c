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

int32_t tEncodeSStreamObj(SEncoder *pEncoder, const SStreamObj *pObj) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->name) < 0) return -1;

  if (tEncodeI64(pEncoder, pObj->createTime) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->updateTime) < 0) return -1;
  if (tEncodeI32(pEncoder, pObj->version) < 0) return -1;
  if (tEncodeI32(pEncoder, pObj->totalLevel) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->smaId) < 0) return -1;

  if (tEncodeI64(pEncoder, pObj->uid) < 0) return -1;
  if (tEncodeI8(pEncoder, pObj->status) < 0) return -1;

  if (tEncodeI8(pEncoder, pObj->igExpired) < 0) return -1;
  if (tEncodeI8(pEncoder, pObj->trigger) < 0) return -1;
  if (tEncodeI8(pEncoder, pObj->fillHistory) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->triggerParam) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->watermark) < 0) return -1;

  if (tEncodeI64(pEncoder, pObj->sourceDbUid) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->targetDbUid) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->sourceDb) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->targetDb) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->targetSTbName) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->targetStbUid) < 0) return -1;
  if (tEncodeI32(pEncoder, pObj->fixedSinkVgId) < 0) return -1;

  if (pObj->sql != NULL) {
    if (tEncodeCStr(pEncoder, pObj->sql) < 0) return -1;
  } else {
    if (tEncodeCStr(pEncoder, "") < 0) return -1;
  }

  if (pObj->ast != NULL) {
    if (tEncodeCStr(pEncoder, pObj->ast) < 0) return -1;
  } else {
    if (tEncodeCStr(pEncoder, "") < 0) return -1;
  }

  if (pObj->physicalPlan != NULL) {
    if (tEncodeCStr(pEncoder, pObj->physicalPlan) < 0) return -1;
  } else {
    if (tEncodeCStr(pEncoder, "") < 0) return -1;
  }

  int32_t sz = taosArrayGetSize(pObj->tasks);
  if (tEncodeI32(pEncoder, sz) < 0) return -1;
  for (int32_t i = 0; i < sz; i++) {
    SArray *pArray = taosArrayGetP(pObj->tasks, i);
    int32_t innerSz = taosArrayGetSize(pArray);
    if (tEncodeI32(pEncoder, innerSz) < 0) return -1;
    for (int32_t j = 0; j < innerSz; j++) {
      SStreamTask *pTask = taosArrayGetP(pArray, j);
      if (tEncodeSStreamTask(pEncoder, pTask) < 0) return -1;
    }
  }

  if (tEncodeSSchemaWrapper(pEncoder, &pObj->outputSchema) < 0) return -1;

  // 3.0.20
  if (tEncodeI64(pEncoder, pObj->checkpointFreq) < 0) return -1;
  if (tEncodeI8(pEncoder, pObj->igCheckUpdate) < 0) return -1;

  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSStreamObj(SDecoder *pDecoder, SStreamObj *pObj, int32_t sver) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pObj->name) < 0) return -1;

  if (tDecodeI64(pDecoder, &pObj->createTime) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->updateTime) < 0) return -1;
  if (tDecodeI32(pDecoder, &pObj->version) < 0) return -1;
  if (tDecodeI32(pDecoder, &pObj->totalLevel) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->smaId) < 0) return -1;

  if (tDecodeI64(pDecoder, &pObj->uid) < 0) return -1;
  if (tDecodeI8(pDecoder, &pObj->status) < 0) return -1;

  if (tDecodeI8(pDecoder, &pObj->igExpired) < 0) return -1;
  if (tDecodeI8(pDecoder, &pObj->trigger) < 0) return -1;
  if (tDecodeI8(pDecoder, &pObj->fillHistory) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->triggerParam) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->watermark) < 0) return -1;

  if (tDecodeI64(pDecoder, &pObj->sourceDbUid) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->targetDbUid) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pObj->sourceDb) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pObj->targetDb) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pObj->targetSTbName) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->targetStbUid) < 0) return -1;
  if (tDecodeI32(pDecoder, &pObj->fixedSinkVgId) < 0) return -1;

  if (tDecodeCStrAlloc(pDecoder, &pObj->sql) < 0) return -1;
  if (tDecodeCStrAlloc(pDecoder, &pObj->ast) < 0) return -1;
  if (tDecodeCStrAlloc(pDecoder, &pObj->physicalPlan) < 0) return -1;

  pObj->tasks = NULL;
  int32_t sz;
  if (tDecodeI32(pDecoder, &sz) < 0) return -1;
  if (sz != 0) {
    pObj->tasks = taosArrayInit(sz, sizeof(void *));
    for (int32_t i = 0; i < sz; i++) {
      int32_t innerSz;
      if (tDecodeI32(pDecoder, &innerSz) < 0) return -1;
      SArray *pArray = taosArrayInit(innerSz, sizeof(void *));
      for (int32_t j = 0; j < innerSz; j++) {
        SStreamTask *pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
        if (pTask == NULL) {
          taosArrayDestroy(pArray);
          return -1;
        }
        if (tDecodeSStreamTask(pDecoder, pTask) < 0) {
          taosMemoryFree(pTask);
          taosArrayDestroy(pArray);
          return -1;
        }
        taosArrayPush(pArray, &pTask);
      }
      taosArrayPush(pObj->tasks, &pArray);
    }
  }

  if (tDecodeSSchemaWrapper(pDecoder, &pObj->outputSchema) < 0) return -1;

  // 3.0.20
  if (sver >= 2) {
    if (tDecodeI64(pDecoder, &pObj->checkpointFreq) < 0) return -1;
    if (tDecodeI8(pDecoder, &pObj->igCheckUpdate) < 0) return -1;
  }
  tEndDecode(pDecoder);
  return 0;
}

void tFreeStreamObj(SStreamObj *pStream) {
  taosMemoryFree(pStream->sql);
  taosMemoryFree(pStream->ast);
  taosMemoryFree(pStream->physicalPlan);
  if (pStream->outputSchema.nCols) taosMemoryFree(pStream->outputSchema.pSchema);

  int32_t sz = taosArrayGetSize(pStream->tasks);
  for (int32_t i = 0; i < sz; i++) {
    SArray *pLevel = taosArrayGetP(pStream->tasks, i);
    int32_t taskSz = taosArrayGetSize(pLevel);
    for (int32_t j = 0; j < taskSz; j++) {
      SStreamTask *pTask = taosArrayGetP(pLevel, j);
      tFreeSStreamTask(pTask);
    }
    taosArrayDestroy(pLevel);
  }
  taosArrayDestroy(pStream->tasks);
}

SMqVgEp *tCloneSMqVgEp(const SMqVgEp *pVgEp) {
  SMqVgEp *pVgEpNew = taosMemoryMalloc(sizeof(SMqVgEp));
  if (pVgEpNew == NULL) return NULL;
  pVgEpNew->vgId = pVgEp->vgId;
  pVgEpNew->qmsg = strdup(pVgEp->qmsg);
  pVgEpNew->epSet = pVgEp->epSet;
  return pVgEpNew;
}

void tDeleteSMqVgEp(SMqVgEp *pVgEp) {
  if (pVgEp) {
    taosMemoryFreeClear(pVgEp->qmsg);
    taosMemoryFree(pVgEp);
  }
}

int32_t tEncodeSMqVgEp(void **buf, const SMqVgEp *pVgEp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pVgEp->vgId);
  tlen += taosEncodeString(buf, pVgEp->qmsg);
  tlen += taosEncodeSEpSet(buf, &pVgEp->epSet);
  return tlen;
}

void *tDecodeSMqVgEp(const void *buf, SMqVgEp *pVgEp) {
  buf = taosDecodeFixedI32(buf, &pVgEp->vgId);
  buf = taosDecodeString(buf, &pVgEp->qmsg);
  buf = taosDecodeSEpSet(buf, &pVgEp->epSet);
  return (void *)buf;
}

SMqConsumerObj *tNewSMqConsumerObj(int64_t consumerId, char cgroup[TSDB_CGROUP_LEN]) {
  SMqConsumerObj *pConsumer = taosMemoryCalloc(1, sizeof(SMqConsumerObj));
  if (pConsumer == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pConsumer->consumerId = consumerId;
  memcpy(pConsumer->cgroup, cgroup, TSDB_CGROUP_LEN);

  pConsumer->epoch = 0;
  pConsumer->status = MQ_CONSUMER_STATUS__MODIFY;
  pConsumer->hbStatus = 0;

  taosInitRWLatch(&pConsumer->lock);

  pConsumer->currentTopics = taosArrayInit(0, sizeof(void *));
  pConsumer->rebNewTopics = taosArrayInit(0, sizeof(void *));
  pConsumer->rebRemovedTopics = taosArrayInit(0, sizeof(void *));
  pConsumer->assignedTopics = taosArrayInit(0, sizeof(void *));

  if (pConsumer->currentTopics == NULL || pConsumer->rebNewTopics == NULL || pConsumer->rebRemovedTopics == NULL ||
      pConsumer->assignedTopics == NULL) {
    taosArrayDestroy(pConsumer->currentTopics);
    taosArrayDestroy(pConsumer->rebNewTopics);
    taosArrayDestroy(pConsumer->rebRemovedTopics);
    taosArrayDestroy(pConsumer->assignedTopics);
    taosMemoryFree(pConsumer);
    return NULL;
  }

  pConsumer->upTime = taosGetTimestampMs();

  return pConsumer;
}

void tDeleteSMqConsumerObj(SMqConsumerObj *pConsumer) {
  taosArrayDestroyP(pConsumer->currentTopics, (FDelete)taosMemoryFree);
  taosArrayDestroyP(pConsumer->rebNewTopics, (FDelete)taosMemoryFree);
  taosArrayDestroyP(pConsumer->rebRemovedTopics, (FDelete)taosMemoryFree);
  taosArrayDestroyP(pConsumer->assignedTopics, (FDelete)taosMemoryFree);
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
  tlen += taosEncodeFixedI64(buf, pConsumer->upTime);
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

  return tlen;
}

void *tDecodeSMqConsumerObj(const void *buf, SMqConsumerObj *pConsumer) {
  int32_t sz;
  buf = taosDecodeFixedI64(buf, &pConsumer->consumerId);
  buf = taosDecodeStringTo(buf, pConsumer->clientId);
  buf = taosDecodeStringTo(buf, pConsumer->cgroup);
  buf = taosDecodeFixedI8(buf, &pConsumer->updateType);
  buf = taosDecodeFixedI32(buf, &pConsumer->epoch);
  buf = taosDecodeFixedI32(buf, &pConsumer->status);

  buf = taosDecodeFixedI32(buf, &pConsumer->pid);
  buf = taosDecodeSEpSet(buf, &pConsumer->ep);
  buf = taosDecodeFixedI64(buf, &pConsumer->upTime);
  buf = taosDecodeFixedI64(buf, &pConsumer->subscribeTime);
  buf = taosDecodeFixedI64(buf, &pConsumer->rebalanceTime);

  // current topics
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->currentTopics = taosArrayInit(sz, sizeof(void *));
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = taosDecodeString(buf, &topic);
    taosArrayPush(pConsumer->currentTopics, &topic);
  }

  // reb new topics
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->rebNewTopics = taosArrayInit(sz, sizeof(void *));
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = taosDecodeString(buf, &topic);
    taosArrayPush(pConsumer->rebNewTopics, &topic);
  }

  // reb removed topics
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->rebRemovedTopics = taosArrayInit(sz, sizeof(void *));
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = taosDecodeString(buf, &topic);
    taosArrayPush(pConsumer->rebRemovedTopics, &topic);
  }

  // reb removed topics
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->assignedTopics = taosArrayInit(sz, sizeof(void *));
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = taosDecodeString(buf, &topic);
    taosArrayPush(pConsumer->assignedTopics, &topic);
  }

  return (void *)buf;
}

SMqConsumerEp *tCloneSMqConsumerEp(const SMqConsumerEp *pConsumerEpOld) {
  SMqConsumerEp *pConsumerEpNew = taosMemoryMalloc(sizeof(SMqConsumerEp));
  if (pConsumerEpNew == NULL) return NULL;
  pConsumerEpNew->consumerId = pConsumerEpOld->consumerId;
  pConsumerEpNew->vgs = taosArrayDup(pConsumerEpOld->vgs, (__array_item_dup_fn_t)tCloneSMqVgEp);
  return pConsumerEpNew;
}

void tDeleteSMqConsumerEp(void *data) {
  SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)data;
  taosArrayDestroyP(pConsumerEp->vgs, (FDelete)tDeleteSMqVgEp);
}

int32_t tEncodeSMqConsumerEp(void **buf, const SMqConsumerEp *pConsumerEp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pConsumerEp->consumerId);
  tlen += taosEncodeArray(buf, pConsumerEp->vgs, (FEncode)tEncodeSMqVgEp);
#if 0
  int32_t sz = taosArrayGetSize(pConsumerEp->vgs);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, i);
    tlen += tEncodeSMqVgEp(buf, pVgEp);
  }
#endif
  return tlen;
}

void *tDecodeSMqConsumerEp(const void *buf, SMqConsumerEp *pConsumerEp) {
  buf = taosDecodeFixedI64(buf, &pConsumerEp->consumerId);
  buf = taosDecodeArray(buf, &pConsumerEp->vgs, (FDecode)tDecodeSMqVgEp, sizeof(SMqVgEp));
#if 0
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumerEp->vgs = taosArrayInit(sz, sizeof(void *));
  for (int32_t i = 0; i < sz; i++) {
    SMqVgEp *pVgEp = taosMemoryMalloc(sizeof(SMqVgEp));
    buf = tDecodeSMqVgEp(buf, pVgEp);
    taosArrayPush(pConsumerEp->vgs, &pVgEp);
  }
#endif

  return (void *)buf;
}

SMqSubscribeObj *tNewSubscribeObj(const char key[TSDB_SUBSCRIBE_KEY_LEN]) {
  SMqSubscribeObj *pSubNew = taosMemoryCalloc(1, sizeof(SMqSubscribeObj));
  if (pSubNew == NULL) return NULL;
  memcpy(pSubNew->key, key, TSDB_SUBSCRIBE_KEY_LEN);
  taosInitRWLatch(&pSubNew->lock);
  pSubNew->vgNum = 0;
  pSubNew->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  // TODO set hash free fp
  /*taosHashSetFreeFp(pSubNew->consumerHash, tDeleteSMqConsumerEp);*/

  pSubNew->unassignedVgs = taosArrayInit(0, sizeof(void *));

  return pSubNew;
}

SMqSubscribeObj *tCloneSubscribeObj(const SMqSubscribeObj *pSub) {
  SMqSubscribeObj *pSubNew = taosMemoryMalloc(sizeof(SMqSubscribeObj));
  if (pSubNew == NULL) return NULL;
  memcpy(pSubNew->key, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);
  taosInitRWLatch(&pSubNew->lock);

  pSubNew->dbUid = pSub->dbUid;
  pSubNew->stbUid = pSub->stbUid;
  pSubNew->subType = pSub->subType;
  pSubNew->withMeta = pSub->withMeta;

  pSubNew->vgNum = pSub->vgNum;
  pSubNew->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  // TODO set hash free fp
  /*taosHashSetFreeFp(pSubNew->consumerHash, tDeleteSMqConsumerEp);*/
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
    taosHashPut(pSubNew->consumerHash, &newEp.consumerId, sizeof(int64_t), &newEp, sizeof(SMqConsumerEp));
  }
  pSubNew->unassignedVgs = taosArrayDup(pSub->unassignedVgs, (__array_item_dup_fn_t)tCloneSMqVgEp);
  memcpy(pSubNew->dbName, pSub->dbName, TSDB_DB_FNAME_LEN);
  return pSubNew;
}

void tDeleteSubscribeObj(SMqSubscribeObj *pSub) {
  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    taosArrayDestroyP(pConsumerEp->vgs, (FDelete)tDeleteSMqVgEp);
  }
  taosHashCleanup(pSub->consumerHash);
  taosArrayDestroyP(pSub->unassignedVgs, (FDelete)tDeleteSMqVgEp);
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
  return tlen;
}

void *tDecodeSubscribeObj(const void *buf, SMqSubscribeObj *pSub) {
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
    buf = tDecodeSMqConsumerEp(buf, &consumerEp);
    taosHashPut(pSub->consumerHash, &consumerEp.consumerId, sizeof(int64_t), &consumerEp, sizeof(SMqConsumerEp));
  }

  buf = taosDecodeArray(buf, &pSub->unassignedVgs, (FDecode)tDecodeSMqVgEp, sizeof(SMqVgEp));
  buf = taosDecodeStringTo(buf, pSub->dbName);
  return (void *)buf;
}

SMqSubActionLogEntry *tCloneSMqSubActionLogEntry(SMqSubActionLogEntry *pEntry) {
  SMqSubActionLogEntry *pEntryNew = taosMemoryMalloc(sizeof(SMqSubActionLogEntry));
  if (pEntryNew == NULL) return NULL;
  pEntryNew->epoch = pEntry->epoch;
  pEntryNew->consumers = taosArrayDup(pEntry->consumers, (__array_item_dup_fn_t)tCloneSMqConsumerEp);
  return pEntryNew;
}

void tDeleteSMqSubActionLogEntry(SMqSubActionLogEntry *pEntry) {
  taosArrayDestroyEx(pEntry->consumers, (FDelete)tDeleteSMqConsumerEp);
}

int32_t tEncodeSMqSubActionLogEntry(void **buf, const SMqSubActionLogEntry *pEntry) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pEntry->epoch);
  tlen += taosEncodeArray(buf, pEntry->consumers, (FEncode)tEncodeSMqSubActionLogEntry);
  return tlen;
}

void *tDecodeSMqSubActionLogEntry(const void *buf, SMqSubActionLogEntry *pEntry) {
  buf = taosDecodeFixedI32(buf, &pEntry->epoch);
  buf = taosDecodeArray(buf, &pEntry->consumers, (FDecode)tDecodeSMqSubActionLogEntry, sizeof(SMqSubActionLogEntry));
  return (void *)buf;
}

SMqSubActionLogObj *tCloneSMqSubActionLogObj(SMqSubActionLogObj *pLog) {
  SMqSubActionLogObj *pLogNew = taosMemoryMalloc(sizeof(SMqSubActionLogObj));
  if (pLogNew == NULL) return pLogNew;
  memcpy(pLogNew->key, pLog->key, TSDB_SUBSCRIBE_KEY_LEN);
  pLogNew->logs = taosArrayDup(pLog->logs, (__array_item_dup_fn_t)tCloneSMqConsumerEp);
  return pLogNew;
}

void tDeleteSMqSubActionLogObj(SMqSubActionLogObj *pLog) {
  taosArrayDestroyEx(pLog->logs, (FDelete)tDeleteSMqConsumerEp);
}

int32_t tEncodeSMqSubActionLogObj(void **buf, const SMqSubActionLogObj *pLog) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pLog->key);
  tlen += taosEncodeArray(buf, pLog->logs, (FEncode)tEncodeSMqSubActionLogEntry);
  return tlen;
}

void *tDecodeSMqSubActionLogObj(const void *buf, SMqSubActionLogObj *pLog) {
  buf = taosDecodeStringTo(buf, pLog->key);
  buf = taosDecodeArray(buf, &pLog->logs, (FDecode)tDecodeSMqSubActionLogEntry, sizeof(SMqSubActionLogEntry));
  return (void *)buf;
}

int32_t tEncodeSMqOffsetObj(void **buf, const SMqOffsetObj *pOffset) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pOffset->key);
  tlen += taosEncodeFixedI64(buf, pOffset->offset);
  return tlen;
}

void *tDecodeSMqOffsetObj(void *buf, SMqOffsetObj *pOffset) {
  buf = taosDecodeStringTo(buf, pOffset->key);
  buf = taosDecodeFixedI64(buf, &pOffset->offset);
  return buf;
}

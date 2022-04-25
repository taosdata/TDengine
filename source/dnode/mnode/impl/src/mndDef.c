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

#include "mndDef.h"
#include "mndConsumer.h"

SMqConsumerObj *tNewSMqConsumerObj(int64_t consumerId, char cgroup[TSDB_CGROUP_LEN]) {
  SMqConsumerObj *pConsumer = taosMemoryCalloc(1, sizeof(SMqConsumerObj));
  if (pConsumer == NULL) {
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

  if (pConsumer->currentTopics == NULL || pConsumer->rebNewTopics == NULL || pConsumer->rebRemovedTopics == NULL) {
    taosArrayDestroy(pConsumer->currentTopics);
    taosArrayDestroy(pConsumer->rebNewTopics);
    taosArrayDestroy(pConsumer->rebRemovedTopics);
    taosMemoryFree(pConsumer);
    return NULL;
  }

  return pConsumer;
}

void tDeleteSMqConsumerObj(SMqConsumerObj *pConsumer) {
  if (pConsumer->currentTopics) {
    taosArrayDestroyP(pConsumer->currentTopics, (FDelete)taosMemoryFree);
  }
  if (pConsumer->rebNewTopics) {
    taosArrayDestroyP(pConsumer->rebNewTopics, (FDelete)taosMemoryFree);
  }
  if (pConsumer->rebRemovedTopics) {
    taosArrayDestroyP(pConsumer->rebRemovedTopics, (FDelete)taosMemoryFree);
  }
}

int32_t tEncodeSMqConsumerObj(void **buf, const SMqConsumerObj *pConsumer) {
  int32_t tlen = 0;
  int32_t sz;
  tlen += taosEncodeFixedI64(buf, pConsumer->consumerId);
  tlen += taosEncodeString(buf, pConsumer->cgroup);
  tlen += taosEncodeFixedI8(buf, pConsumer->updateType);
  tlen += taosEncodeFixedI32(buf, pConsumer->epoch);
  tlen += taosEncodeFixedI32(buf, pConsumer->status);

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

  return tlen;
}

void *tDecodeSMqConsumerObj(const void *buf, SMqConsumerObj *pConsumer) {
  int32_t sz;
  buf = taosDecodeFixedI64(buf, &pConsumer->consumerId);
  buf = taosDecodeStringTo(buf, pConsumer->cgroup);
  buf = taosDecodeFixedI8(buf, &pConsumer->updateType);
  buf = taosDecodeFixedI32(buf, &pConsumer->epoch);
  buf = taosDecodeFixedI32(buf, &pConsumer->status);

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

  return (void *)buf;
}

SMqVgEp *tCloneSMqVgEp(const SMqVgEp *pVgEp) {
  SMqVgEp *pVgEpNew = taosMemoryMalloc(sizeof(SMqVgEp));
  if (pVgEpNew == NULL) return NULL;
  pVgEpNew->vgId = pVgEp->vgId;
  pVgEpNew->qmsg = strdup(pVgEp->qmsg);
  pVgEpNew->epSet = pVgEp->epSet;
  return pVgEpNew;
}

void tDeleteSMqVgEp(SMqVgEp *pVgEp) { taosMemoryFree(pVgEp->qmsg); }

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

SMqConsumerEpInSub *tCloneSMqConsumerEpInSub(const SMqConsumerEpInSub *pEpInSub) {
  SMqConsumerEpInSub *pEpInSubNew = taosMemoryMalloc(sizeof(SMqConsumerEpInSub));
  if (pEpInSubNew == NULL) return NULL;
  pEpInSubNew->consumerId = pEpInSub->consumerId;
  pEpInSubNew->vgs = taosArrayDeepCopy(pEpInSub->vgs, (FCopy)tCloneSMqVgEp);
  return pEpInSubNew;
}

void tDeleteSMqConsumerEpInSub(SMqConsumerEpInSub *pEpInSub) {
  taosArrayDestroyEx(pEpInSub->vgs, (FDelete)tDeleteSMqVgEp);
}

int32_t tEncodeSMqConsumerEpInSub(void **buf, const SMqConsumerEpInSub *pEpInSub) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pEpInSub->consumerId);
  int32_t sz = taosArrayGetSize(pEpInSub->vgs);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqVgEp *pVgEp = taosArrayGetP(pEpInSub->vgs, i);
    tlen += tEncodeSMqVgEp(buf, pVgEp);
  }
  /*tlen += taosEncodeArray(buf, pEpInSub->vgs, (FEncode)tEncodeSMqVgEp);*/
  return tlen;
}

void *tDecodeSMqConsumerEpInSub(const void *buf, SMqConsumerEpInSub *pEpInSub) {
  buf = taosDecodeFixedI64(buf, &pEpInSub->consumerId);
  /*buf = taosDecodeArray(buf, &pEpInSub->vgs, (FDecode)tDecodeSMqVgEp, sizeof(SMqSubVgEp));*/
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pEpInSub->vgs = taosArrayInit(sz, sizeof(void *));
  for (int32_t i = 0; i < sz; i++) {
    SMqVgEp *pVgEp = taosMemoryMalloc(sizeof(SMqVgEp));
    buf = tDecodeSMqVgEp(buf, pVgEp);
    taosArrayPush(pEpInSub->vgs, &pVgEp);
  }

  return (void *)buf;
}

SMqSubscribeObj *tNewSubscribeObj(const char key[TSDB_SUBSCRIBE_KEY_LEN]) {
  SMqSubscribeObj *pSubNew = taosMemoryCalloc(1, sizeof(SMqSubscribeObj));
  if (pSubNew == NULL) return NULL;
  memcpy(pSubNew->key, key, TSDB_SUBSCRIBE_KEY_LEN);
  taosInitRWLatch(&pSubNew->lock);
  pSubNew->vgNum = -1;
  pSubNew->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  // TODO set free fp
  SMqConsumerEpInSub epInSub = {
      .consumerId = -1,
      .vgs = taosArrayInit(0, sizeof(void *)),
  };
  int64_t unexistKey = -1;
  taosHashPut(pSubNew->consumerHash, &unexistKey, sizeof(int64_t), &epInSub, sizeof(SMqConsumerEpInSub));
  return pSubNew;
}

SMqSubscribeObj *tCloneSubscribeObj(const SMqSubscribeObj *pSub) {
  SMqSubscribeObj *pSubNew = taosMemoryMalloc(sizeof(SMqSubscribeObj));
  if (pSubNew == NULL) return NULL;
  memcpy(pSubNew->key, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);
  taosInitRWLatch(&pSubNew->lock);

  pSubNew->subType = pSub->subType;
  pSubNew->withTbName = pSub->withTbName;
  pSubNew->withSchema = pSub->withSchema;
  pSubNew->withTag = pSub->withTag;

  pSubNew->vgNum = pSub->vgNum;
  pSubNew->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  /*taosHashSetFreeFp(pSubNew->consumerHash, taosArrayDestroy);*/
  void               *pIter = NULL;
  SMqConsumerEpInSub *pEpInSub = NULL;
  while (1) {
    pIter = taosHashIterate(pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    pEpInSub = (SMqConsumerEpInSub *)pIter;
    SMqConsumerEpInSub newEp = {
        .consumerId = pEpInSub->consumerId,
        .vgs = taosArrayDeepCopy(pEpInSub->vgs, (FCopy)tCloneSMqVgEp),
    };
    taosHashPut(pSubNew->consumerHash, &newEp.consumerId, sizeof(int64_t), &newEp, sizeof(SMqConsumerEpInSub));
  }
  return pSubNew;
}

void tDeleteSubscribeObj(SMqSubscribeObj *pSub) {
  /*taosArrayDestroyEx(pSub->consumerEps, (FDelete)tDeleteSMqConsumerEpInSub);*/
  taosHashCleanup(pSub->consumerHash);
}

int32_t tEncodeSubscribeObj(void **buf, const SMqSubscribeObj *pSub) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pSub->key);
  tlen += taosEncodeFixedI32(buf, pSub->vgNum);
  tlen += taosEncodeFixedI8(buf, pSub->subType);
  tlen += taosEncodeFixedI8(buf, pSub->withTbName);
  tlen += taosEncodeFixedI8(buf, pSub->withSchema);
  tlen += taosEncodeFixedI8(buf, pSub->withTag);

  void   *pIter = NULL;
  int32_t sz = taosHashGetSize(pSub->consumerHash);
  tlen += taosEncodeFixedI32(buf, sz);

  int32_t cnt = 0;
  while (1) {
    pIter = taosHashIterate(pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEpInSub *pEpInSub = (SMqConsumerEpInSub *)pIter;
    tlen += tEncodeSMqConsumerEpInSub(buf, pEpInSub);
    cnt++;
  }
  ASSERT(cnt == sz);
  /*tlen += taosEncodeArray(buf, pSub->consumerEps, (FEncode)tEncodeSMqConsumerEpInSub);*/
  return tlen;
}

void *tDecodeSubscribeObj(const void *buf, SMqSubscribeObj *pSub) {
  //
  buf = taosDecodeStringTo(buf, pSub->key);
  buf = taosDecodeFixedI32(buf, &pSub->vgNum);
  buf = taosDecodeFixedI8(buf, &pSub->subType);
  buf = taosDecodeFixedI8(buf, &pSub->withTbName);
  buf = taosDecodeFixedI8(buf, &pSub->withSchema);
  buf = taosDecodeFixedI8(buf, &pSub->withTag);

  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);

  pSub->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  for (int32_t i = 0; i < sz; i++) {
    /*SMqConsumerEpInSub* pEpInSub = taosMemoryMalloc(sizeof(SMqConsumerEpInSub));*/
    SMqConsumerEpInSub epInSub = {0};
    buf = tDecodeSMqConsumerEpInSub(buf, &epInSub);
    taosHashPut(pSub->consumerHash, &epInSub.consumerId, sizeof(int64_t), &epInSub, sizeof(SMqConsumerEpInSub));
  }

  /*buf = taosDecodeArray(buf, &pSub->consumerEps, (FDecode)tDecodeSMqConsumerEpInSub, sizeof(SMqConsumerEpInSub));*/
  return (void *)buf;
}

SMqSubActionLogEntry *tCloneSMqSubActionLogEntry(SMqSubActionLogEntry *pEntry) {
  SMqSubActionLogEntry *pEntryNew = taosMemoryMalloc(sizeof(SMqSubActionLogEntry));
  if (pEntryNew == NULL) return NULL;
  pEntryNew->epoch = pEntry->epoch;
  pEntryNew->consumers = taosArrayDeepCopy(pEntry->consumers, (FCopy)tCloneSMqConsumerEpInSub);
  return pEntryNew;
}

void tDeleteSMqSubActionLogEntry(SMqSubActionLogEntry *pEntry) {
  taosArrayDestroyEx(pEntry->consumers, (FDelete)tDeleteSMqConsumerEpInSub);
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
  pLogNew->logs = taosArrayDeepCopy(pLog->logs, (FCopy)tCloneSMqConsumerEpInSub);
  return pLogNew;
}

void tDeleteSMqSubActionLogObj(SMqSubActionLogObj *pLog) {
  taosArrayDestroyEx(pLog->logs, (FDelete)tDeleteSMqConsumerEpInSub);
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

int32_t tEncodeSStreamObj(SCoder *pEncoder, const SStreamObj *pObj) {
  int32_t sz = 0;
  /*int32_t outputNameSz = 0;*/
  if (tEncodeCStr(pEncoder, pObj->name) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->db) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->createTime) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->updateTime) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->uid) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->dbUid) < 0) return -1;
  if (tEncodeI32(pEncoder, pObj->version) < 0) return -1;
  if (tEncodeI8(pEncoder, pObj->status) < 0) return -1;
  if (tEncodeI8(pEncoder, pObj->createdBy) < 0) return -1;
  if (tEncodeI32(pEncoder, pObj->fixedSinkVgId) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->smaId) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->sql) < 0) return -1;
  /*if (tEncodeCStr(pEncoder, pObj->logicalPlan) < 0) return -1;*/
  if (tEncodeCStr(pEncoder, pObj->physicalPlan) < 0) return -1;
  // TODO encode tasks
  if (pObj->tasks) {
    sz = taosArrayGetSize(pObj->tasks);
  }
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

#if 0
  if (pObj->ColAlias != NULL) {
    outputNameSz = taosArrayGetSize(pObj->ColAlias);
  }
  if (tEncodeI32(pEncoder, outputNameSz) < 0) return -1;
  for (int32_t i = 0; i < outputNameSz; i++) {
    char *name = taosArrayGetP(pObj->ColAlias, i);
    if (tEncodeCStr(pEncoder, name) < 0) return -1;
  }
#endif
  return pEncoder->pos;
}

int32_t tDecodeSStreamObj(SCoder *pDecoder, SStreamObj *pObj) {
  if (tDecodeCStrTo(pDecoder, pObj->name) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pObj->db) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->createTime) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->updateTime) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->uid) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->dbUid) < 0) return -1;
  if (tDecodeI32(pDecoder, &pObj->version) < 0) return -1;
  if (tDecodeI8(pDecoder, &pObj->status) < 0) return -1;
  if (tDecodeI8(pDecoder, &pObj->createdBy) < 0) return -1;
  if (tDecodeI32(pDecoder, &pObj->fixedSinkVgId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->smaId) < 0) return -1;
  if (tDecodeCStrAlloc(pDecoder, &pObj->sql) < 0) return -1;
  /*if (tDecodeCStrAlloc(pDecoder, &pObj->logicalPlan) < 0) return -1;*/
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
        if (pTask == NULL) return -1;
        if (tDecodeSStreamTask(pDecoder, pTask) < 0) return -1;
        taosArrayPush(pArray, &pTask);
      }
      taosArrayPush(pObj->tasks, &pArray);
    }
  }

  if (tDecodeSSchemaWrapper(pDecoder, &pObj->outputSchema) < 0) return -1;
#if 0
  int32_t outputNameSz;
  if (tDecodeI32(pDecoder, &outputNameSz) < 0) return -1;
  if (outputNameSz != 0) {
    pObj->ColAlias = taosArrayInit(outputNameSz, sizeof(void *));
    if (pObj->ColAlias == NULL) {
      return -1;
    }
  }
  for (int32_t i = 0; i < outputNameSz; i++) {
    char *name;
    if (tDecodeCStrAlloc(pDecoder, &name) < 0) return -1;
    taosArrayPush(pObj->ColAlias, &name);
  }
#endif
  return 0;
}

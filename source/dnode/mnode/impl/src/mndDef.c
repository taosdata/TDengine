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
#include "mndConsumer.h"
#include "mndDef.h"
#include "mndStream.h"
#include "taoserror.h"
#include "tunit.h"

int32_t tEncodeSStreamObj(SEncoder *pEncoder, const SStreamObj *pObj) {
  TAOS_CHECK_RETURN(tStartEncode(pEncoder));

  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pObj->name));
  TAOS_CHECK_RETURN(tSerializeSCMCreateStreamReqImpl(pEncoder, pObj->pCreate));

  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pObj->mainSnodeId));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pObj->userStopped));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->createTime));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->updateTime));

  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSStreamObj(SDecoder *pDecoder, SStreamObj *pObj, int32_t sver) {
  int32_t code = 0;
  int32_t lino = 0;
  TAOS_CHECK_RETURN(tStartDecode(pDecoder));

  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pObj->name));
  pObj->pCreate = taosMemoryCalloc(1, sizeof(*pObj->pCreate));
  if (NULL == pObj) {
    TAOS_CHECK_EXIT(terrno);
  }
  
  if (MND_STREAM_VER_NUMBER == sver) {
    TAOS_CHECK_RETURN(tDeserializeSCMCreateStreamReqImpl(pDecoder, pObj->pCreate));
  } else {
    TAOS_CHECK_RETURN(
      tDeserializeSCMCreateStreamReqImplOld(pDecoder, pObj->pCreate, 21));
  }

  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pObj->mainSnodeId));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pObj->userStopped));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->createTime));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->updateTime));

_exit:

  tEndDecode(pDecoder);
  tDecoderClear(pDecoder);  
  
  TAOS_RETURN(code);
}

void tFreeStreamObj(SStreamObj *pStream) {
  tFreeSCMCreateStreamReq(pStream->pCreate);
  taosMemoryFreeClear(pStream->pCreate);
}

void freeSMqConsumerEp(void* data) {
  if (data == NULL) return;
  SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)data;
  taosArrayDestroy(pConsumerEp->vgs);
  pConsumerEp->vgs = NULL;
  taosArrayDestroy(pConsumerEp->offsetRows);
  pConsumerEp->offsetRows = NULL;
}

int32_t tEncodeSMqVgEp(void **buf, const SMqVgEp *pVgEp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pVgEp->vgId);
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

int32_t tNewSMqConsumerObj(int64_t consumerId, char *cgroup, int8_t updateType, char *topic, SCMSubscribeReq *subscribe,
                           SMqConsumerObj **ppConsumer) {
  int32_t         code = 0;
  SMqConsumerObj *pConsumer = taosMemoryCalloc(1, sizeof(SMqConsumerObj));
  if (pConsumer == NULL) {
    code = terrno;
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

  if (updateType == CONSUMER_ADD_REB) {
    pConsumer->rebNewTopics = taosArrayInit(0, sizeof(void *));
    if (pConsumer->rebNewTopics == NULL) {
      code = terrno;
      goto END;
    }

    char *topicTmp = taosStrdup(topic);
    if (taosArrayPush(pConsumer->rebNewTopics, &topicTmp) == NULL) {
      code = terrno;
      goto END;
    }
  } else if (updateType == CONSUMER_REMOVE_REB) {
    pConsumer->rebRemovedTopics = taosArrayInit(0, sizeof(void *));
    if (pConsumer->rebRemovedTopics == NULL) {
      code = terrno;
      goto END;
    }
    char *topicTmp = taosStrdup(topic);
    if (taosArrayPush(pConsumer->rebRemovedTopics, &topicTmp) == NULL) {
      code = terrno;
      goto END;
    }
  } else if (updateType == CONSUMER_INSERT_SUB) {
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
    if (pConsumer->rebNewTopics == NULL) {
      code = terrno;
      goto END;
    }
    pConsumer->assignedTopics = subscribe->topicNames;
    subscribe->topicNames = NULL;
  } else if (updateType == CONSUMER_UPDATE_SUB) {
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
  if (sver > 2) {
    buf = taosDecodeFixedI32(buf, &pConsumer->maxPollIntervalMs);
    buf = taosDecodeFixedI32(buf, &pConsumer->sessionTimeoutMs);
    buf = taosDecodeStringTo(buf, pConsumer->user);
    buf = taosDecodeStringTo(buf, pConsumer->fqdn);
  } else {
    pConsumer->maxPollIntervalMs = DEFAULT_MAX_POLL_INTERVAL;
    pConsumer->sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT;
  }

  return (void *)buf;
}

int32_t tEncodeOffRows(void **buf, SArray *offsetRows) {
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
  int32_t sz = taosArrayGetSize(pConsumerEp->vgs);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    void* data = taosArrayGet(pConsumerEp->vgs, i);
    tlen += tEncodeSMqVgEp(buf, data);
  }

  return tlen + tEncodeOffRows(buf, pConsumerEp->offsetRows);
}

void *tDecodeOffRows(const void *buf, SArray **offsetRows, int8_t sver) {
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
      if (sver > 2) {
        buf = taosDecodeFixedI64(buf, &offRows->ever);
      }
    }
  }
  return (void *)buf;
}

void *tDecodeSMqConsumerEp(const void *buf, SMqConsumerEp *pConsumerEp, int8_t sver) {
  buf = taosDecodeFixedI64(buf, &pConsumerEp->consumerId);
  int32_t sz = 0;
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumerEp->vgs = taosArrayInit(sz, sizeof(SMqVgEp));
  if (pConsumerEp->vgs == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqVgEp* vgEp = taosArrayReserve(pConsumerEp->vgs, 1);
    if (vgEp != NULL)
      buf = tDecodeSMqVgEp(buf, vgEp, sver);
  }
  if (sver > 1) {
    buf = tDecodeOffRows(buf, &pConsumerEp->offsetRows, sver);
  }

  return (void *)buf;
}

int32_t tNewSubscribeObj(const char *key, SMqSubscribeObj **ppSub) {
  int32_t          code = 0;
  SMqSubscribeObj *pSubObj = taosMemoryCalloc(1, sizeof(SMqSubscribeObj));
  MND_TMQ_NULL_CHECK(pSubObj);
  *ppSub = pSubObj;
  
  (void)memcpy(pSubObj->key, key, TSDB_SUBSCRIBE_KEY_LEN);
  taosInitRWLatch(&pSubObj->lock);
  pSubObj->vgNum = 0;
  pSubObj->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  MND_TMQ_NULL_CHECK(pSubObj->consumerHash);
  taosHashSetFreeFp(pSubObj->consumerHash, freeSMqConsumerEp);

  pSubObj->unassignedVgs = taosArrayInit(0, sizeof(SMqVgEp));
  MND_TMQ_NULL_CHECK(pSubObj->unassignedVgs);

END:
  return code;
}

int32_t tCloneSubscribeObj(const SMqSubscribeObj *pSub, SMqSubscribeObj **ppSub) {
  int32_t          code = 0;
  SMqSubscribeObj *pSubNew = taosMemoryMalloc(sizeof(SMqSubscribeObj));
  if (pSubNew == NULL) {
    code = terrno;
    goto END;
  }
  *ppSub = pSubNew;

  (void)memcpy(pSubNew->key, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);
  taosInitRWLatch(&pSubNew->lock);

  pSubNew->dbUid = pSub->dbUid;
  pSubNew->stbUid = pSub->stbUid;
  pSubNew->subType = pSub->subType;
  pSubNew->withMeta = pSub->withMeta;

  pSubNew->vgNum = pSub->vgNum;
  pSubNew->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (pSubNew->consumerHash == NULL) {
    code = terrno;
    goto END;
  }
  taosHashSetFreeFp(pSubNew->consumerHash, freeSMqConsumerEp);

  void          *pIter = NULL;
  SMqConsumerEp *pConsumerEp = NULL;
  while (1) {
    pIter = taosHashIterate(pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    pConsumerEp = (SMqConsumerEp *)pIter;
    SMqConsumerEp newEp = {
        .consumerId = pConsumerEp->consumerId,
        .vgs = taosArrayDup(pConsumerEp->vgs, NULL),
    };
    if ((code = taosHashPut(pSubNew->consumerHash, &newEp.consumerId, sizeof(int64_t), &newEp,
                            sizeof(SMqConsumerEp))) != 0)
      goto END;
  }
  pSubNew->unassignedVgs = taosArrayDup(pSub->unassignedVgs, NULL);
  if (pSubNew->unassignedVgs == NULL) {
    code = terrno;
    goto END;
  }
  pSubNew->offsetRows = taosArrayDup(pSub->offsetRows, NULL);
  if (pSub->offsetRows != NULL && pSubNew->offsetRows == NULL) {
    code = terrno;
    goto END;
  }
  (void)memcpy(pSubNew->dbName, pSub->dbName, TSDB_DB_FNAME_LEN);
  pSubNew->qmsg = taosStrdup(pSub->qmsg);
  if (pSubNew->qmsg == NULL) {
    code = terrno;
    goto END;
  }

END:
  return code;
}

void tDeleteSubscribeObj(SMqSubscribeObj *pSub) {
  if (pSub == NULL) return;
  taosHashCleanup(pSub->consumerHash);
  pSub->consumerHash = NULL;
  taosArrayDestroy(pSub->unassignedVgs);
  pSub->unassignedVgs = NULL;
  taosMemoryFreeClear(pSub->qmsg);
  taosArrayDestroy(pSub->offsetRows);
  pSub->offsetRows = NULL;
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
  int32_t len = taosArrayGetSize(pSub->unassignedVgs);
  tlen += taosEncodeFixedI32(buf, len);
  for (int32_t i = 0; i < len; i++) {
    void* data = taosArrayGet(pSub->unassignedVgs, i);
    tlen += tEncodeSMqVgEp(buf, data);
  }
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
  if (pSub->consumerHash == NULL) {
    return NULL;
  }
  taosHashSetFreeFp(pSub->consumerHash, freeSMqConsumerEp);

  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp consumerEp = {0};
    buf = tDecodeSMqConsumerEp(buf, &consumerEp, sver);
    if (taosHashPut(pSub->consumerHash, &consumerEp.consumerId, sizeof(int64_t), &consumerEp, sizeof(SMqConsumerEp)) !=
        0)
      return NULL;
  }

  int32_t len = 0;
  buf = taosDecodeFixedI32(buf, &len);
  pSub->unassignedVgs = taosArrayInit(len, sizeof(SMqVgEp));
  if (pSub->unassignedVgs == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < len; i++) {
    SMqVgEp* vgEp = taosArrayReserve(pSub->unassignedVgs, 1);
    if (vgEp != NULL)
      buf = tDecodeSMqVgEp(buf, vgEp, sver);
  }

  buf = taosDecodeStringTo(buf, pSub->dbName);

  if (sver > 1) {
    buf = tDecodeOffRows(buf, &pSub->offsetRows, sver);
    buf = taosDecodeString(buf, &pSub->qmsg);
  } else {
    pSub->qmsg = taosStrdup("");
  }
  return (void *)buf;
}

int32_t mndInitConfigObj(SConfigItem *pItem, SConfigObj *pObj) {
  tstrncpy(pObj->name, pItem->name, CFG_NAME_MAX_LEN);
  pObj->dtype = pItem->dtype;
  switch (pItem->dtype) {
    case CFG_DTYPE_NONE:
      break;
    case CFG_DTYPE_BOOL:
      pObj->bval = pItem->bval;
      break;
    case CFG_DTYPE_INT32:
      pObj->i32 = pItem->i32;
      break;
    case CFG_DTYPE_INT64:
      pObj->i64 = pItem->i64;
      break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE:
      pObj->fval = pItem->fval;
      break;
    case CFG_DTYPE_STRING:
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_TIMEZONE:
      pObj->str = taosStrdup(pItem->str);
      if (pObj->str == NULL) {
        taosMemoryFree(pObj);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      break;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t mndUpdateObj(SConfigObj *pObjNew, const char *name, char *value) {
  int32_t code = 0;
  switch (pObjNew->dtype) {
    case CFG_DTYPE_BOOL: {
      bool tmp = false;
      if (strcasecmp(value, "true") == 0) {
        tmp = true;
      }
      if (taosStr2Int32(value, NULL, 10) > 0) {
        tmp = true;
      }
      pObjNew->bval = tmp;
      break;
    }
    case CFG_DTYPE_INT32: {
      int32_t ival;
      TAOS_CHECK_RETURN(taosStrHumanToInt32(value, &ival));
      pObjNew->i32 = ival;
      break;
    }
    case CFG_DTYPE_INT64: {
      int64_t ival;
      TAOS_CHECK_RETURN(taosStrHumanToInt64(value, &ival));
      pObjNew->i64 = ival;
      break;
    }
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE: {
      float dval = 0;
      TAOS_CHECK_RETURN(parseCfgReal(value, &dval));
      pObjNew->fval = dval;
      break;
    }
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_TIMEZONE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_STRING: {
      pObjNew->str = taosStrdup(value);
      if (pObjNew->str == NULL) {
        code = terrno;
        return code;
      }
      break;
    }
    case CFG_DTYPE_NONE:
      break;
    default:
      code = TSDB_CODE_INVALID_CFG;
      break;
  }
  return code;
}

SConfigObj mndInitConfigVersion() {
  SConfigObj obj;
  memset(&obj, 0, sizeof(SConfigObj));

  tstrncpy(obj.name, "tsmmConfigVersion", CFG_NAME_MAX_LEN);
  obj.dtype = CFG_DTYPE_INT32;
  obj.i32 = 0;
  return obj;
}

int32_t tEncodeSConfigObj(SEncoder *pEncoder, const SConfigObj *pObj) {
  TAOS_CHECK_RETURN(tStartEncode(pEncoder));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pObj->name));

  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pObj->dtype));
  switch (pObj->dtype) {
    case CFG_DTYPE_BOOL:
      TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pObj->bval));
      break;
    case CFG_DTYPE_INT32:
      TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pObj->i32));
      break;
    case CFG_DTYPE_INT64:
      TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->i64));
      break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE:
      TAOS_CHECK_RETURN(tEncodeFloat(pEncoder, pObj->fval));
      break;
    case CFG_DTYPE_STRING:
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_TIMEZONE:
      if (pObj->str != NULL) {
        TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pObj->str));
      } else {
        TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, ""));
      }
      break;
    default:
      break;
  }
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSConfigObj(SDecoder *pDecoder, SConfigObj *pObj) {
  TAOS_CHECK_RETURN(tStartDecode(pDecoder));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pObj->name));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, (int32_t *)&pObj->dtype));
  switch (pObj->dtype) {
    case CFG_DTYPE_NONE:
      break;
    case CFG_DTYPE_BOOL:
      TAOS_CHECK_RETURN(tDecodeBool(pDecoder, &pObj->bval));
      break;
    case CFG_DTYPE_INT32:
      TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pObj->i32));
      break;
    case CFG_DTYPE_INT64:
      TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->i64));
      break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE:
      TAOS_CHECK_RETURN(tDecodeFloat(pDecoder, &pObj->fval));
      break;
    case CFG_DTYPE_STRING:
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_TIMEZONE:
      TAOS_CHECK_RETURN(tDecodeCStrAlloc(pDecoder, &pObj->str));
      break;
  }
  tEndDecode(pDecoder);
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

void tFreeSConfigObj(SConfigObj *obj) {
  if (obj == NULL) {
    return;
  }
  if (obj->dtype == CFG_DTYPE_STRING || obj->dtype == CFG_DTYPE_DIR || obj->dtype == CFG_DTYPE_LOCALE ||
      obj->dtype == CFG_DTYPE_CHARSET || obj->dtype == CFG_DTYPE_TIMEZONE) {
    taosMemoryFree(obj->str);
  }
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

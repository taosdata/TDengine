/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pObj->createUser));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->ownerId));

  // flags (appended after ownerId; absent in sver < 9  -  guarded by tDecodeIsEnd).
  // extSpecs has no separate encoding here: it lives solely in pObj->pCreate
  // and is carried by the tSerializeSCMCreateStreamReqImpl call above (JSON).
  TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pObj->flags));

  TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pObj->recalcRevision));
  size_t recalcNum = taosArrayGetSize(pObj->pIncompleteRecalcs);
  if (recalcNum > INT32_MAX) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, (int32_t)recalcNum));
  for (int32_t i = 0; i < (int32_t)recalcNum; ++i) {
    const SStreamRecalcPersistReq *pReq = taosArrayGet(pObj->pIncompleteRecalcs, i);
    if (pReq == NULL || pReq->recalcId == 0 || pReq->end <= pReq->start || pReq->requestTimeMs <= 0) {
      return TSDB_CODE_INVALID_MSG;
    }
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->recalcId));
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->start));
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->end));
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->requestTimeMs));
  }
  // batch-meta-txn (stream phase 2): appended after the recalc block. Persisting these is
  // required for correctness across a leader transfer - see DS 5.2. Streams
  // written before this change decode as txnId=0/txnStatus=NORMAL via
  // tDecodeIsEnd, i.e. not owned by any txn.
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pObj->txnId));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pObj->txnStatus));

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
  
  /* sver >= 8 uses the current stream req impl; sver <= 7 uses the old impl. */
  if (sver >= MND_STREAM_VER_NUMBER) {
    TAOS_CHECK_RETURN(tDeserializeSCMCreateStreamReqImpl(pDecoder, pObj->pCreate));
  } else {
    TAOS_CHECK_RETURN(
      tDeserializeSCMCreateStreamReqImplOld(pDecoder, pObj->pCreate, 21));
  }

  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pObj->mainSnodeId));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pObj->userStopped));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->createTime));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->updateTime));
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pObj->createUser));
    TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->ownerId));
  }

  // flags (sver < 9: leave flags=0). extSpecs has no separate decoding here:
  // it is restored solely from pObj->pCreate (JSON, decoded above).
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pObj->flags));
  }

  pObj->recalcRevision = 0;
  pObj->pIncompleteRecalcs = NULL;
  if (!tDecodeIsEnd(pDecoder)) {
    int32_t recalcNum = 0;
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pObj->recalcRevision));
    TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &recalcNum));
    if (recalcNum < 0 || (uint32_t)recalcNum > TD_CODER_REMAIN_CAPACITY(pDecoder) / (4 * sizeof(int64_t))) {
      TAOS_CHECK_RETURN(TSDB_CODE_INVALID_MSG);
    }
    if (recalcNum > 0) {
      pObj->pIncompleteRecalcs = taosArrayInit(recalcNum, sizeof(SStreamRecalcPersistReq));
      TSDB_CHECK_NULL(pObj->pIncompleteRecalcs, code, lino, _exit, terrno);
    }
    for (int32_t i = 0; i < recalcNum; ++i) {
      SStreamRecalcPersistReq req = {0};
      TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &req.recalcId));
      TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &req.start));
      TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &req.end));
      TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &req.requestTimeMs));
      if (req.recalcId == 0 || req.end <= req.start || req.requestTimeMs <= 0 ||
          taosArrayPush(pObj->pIncompleteRecalcs, &req) == NULL) {
        TAOS_CHECK_RETURN(terrno == TSDB_CODE_SUCCESS ? TSDB_CODE_INVALID_MSG : terrno);
      }
    }
  }

  // batch-meta-txn (stream phase 2): txn markers are appended after the recalc
  // block, so they get their own end-guard. They must NOT sit inside the recalc
  // guard above: a stream persisted before this change still has the recalc
  // fields (recalcRevision + recalcNum) to decode, so tDecodeIsEnd is false when
  // that block is entered, and once the recalc fields are consumed the decoder
  // is at the buffer end - decoding txnId from there returns OUT_OF_RANGE and
  // every pre-upgrade stream fails to load. A separate guard decodes them only
  // when the writer actually appended them.
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pObj->txnId));
    TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pObj->txnStatus));
  }

_exit:

  tEndDecode(pDecoder);
  tDecoderClear(pDecoder);  
  
  TAOS_RETURN(code);
}

void tFreeStreamObj(SStreamObj *pStream) {
  taosArrayDestroy(pStream->pIncompleteRecalcs);
  pStream->pIncompleteRecalcs = NULL;
  // tFreeSCMCreateStreamReq frees pCreate->extSpecs (each spec's
  // triggerColumns/pColMappings/calcColumns/pCalcMappings/prefilter/
  // triggerPrefilter/partitionTagCols included) since that is now the only
  // place extSpecs is held.
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

static void freeVgDnodeId(void* data) {
  if (data == NULL) {
    return;
  }
  SArray **ppDnodeIds = (SArray **)data;
  if (ppDnodeIds == NULL || *ppDnodeIds == NULL) {
    return;
  }
  taosArrayDestroy(*ppDnodeIds);
}

// ---------------------------------------------------------------------------
// Bounded decode helpers for the legacy TMQ sdb row decoders.
//
// The mnode TMQ tables (SDB_CONSUMER / SDB_SUBSCRIBE / SDB_TOPIC) are rebuilt
// from SSdbRaw payload bytes by legacy `void *` walkers that used to receive NO
// buffer length and performed NO destination-capacity / no remaining-length
// checks. A crafted payload (an injected raft log entry, a pushed snapshot, or a
// corrupted sdb.data that is re-decoded on every restart) could therefore read /
// write past the buffer and overflow the fixed-size struct members on the mnode
// heap (CWE-122 heap overflow / CWE-125 heap over-read). See security finding
// "item 240".
//
// These helpers thread an explicit end-of-buffer pointer (bufEnd) and reject any
// read that would run past it or overflow a fixed-size destination. They return
// NULL on violation so the caller can abort the decode and leave the heap intact.
// ---------------------------------------------------------------------------
#define TMQ_DECODE_MAX_COUNT (1024 * 1024)  // generous cap to avoid insane allocations

#define MQ_NEED(buf, bufEnd, n) ((const uint8_t *)(buf) + (n) <= (const uint8_t *)(bufEnd))

// Bounded unsigned varint decode (mirrors taosDecodeVariantU64) that never reads past bufEnd.
static const void *tDecodeVarintUSafe(const void *buf, const void *bufEnd, uint64_t *pVal) {
  *pVal = 0;
  for (int32_t i = 0; i < 10; i++) {
    if ((const uint8_t *)buf + i >= (const uint8_t *)bufEnd) return NULL;
    uint8_t b = ((const uint8_t *)buf)[i];
    if (b < 0x80) {
      *pVal |= ((uint64_t)b) << (7 * i);
      return (const uint8_t *)buf + i + 1;
    }
    *pVal |= ((uint64_t)(b & 0x7F)) << (7 * i);
  }
  return NULL;
}

// Bounded string-into-fixed-buffer decode (mirrors taosDecodeStringTo).
static const void *tDecodeStrToSafe(const void *buf, const void *bufEnd, char *dest, int32_t destCap) {
  uint64_t size = 0;
  buf = tDecodeVarintUSafe(buf, bufEnd, &size);
  if (buf == NULL) return NULL;
  if (size > (uint64_t)((const uint8_t *)bufEnd - (const uint8_t *)buf)) return NULL;  // source over-read
  if (size >= (uint64_t)destCap) return NULL;                                          // dest overflow (keep '\0')
  if (size > 0) memcpy(dest, buf, (size_t)size);
  dest[size] = '\0';
  return (const uint8_t *)buf + size;
}

// Bounded heap string decode (mirrors taosDecodeString).
static const void *tDecodeStrSafe(const void *buf, const void *bufEnd, char **out) {
  uint64_t size = 0;
  buf = tDecodeVarintUSafe(buf, bufEnd, &size);
  if (buf == NULL) return NULL;
  if (size > (uint64_t)((const uint8_t *)bufEnd - (const uint8_t *)buf)) return NULL;
  if (size > TMQ_DECODE_MAX_COUNT) return NULL;
  char *v = (char *)taosMemoryMalloc((size_t)size + 1);
  if (v == NULL) return NULL;
  if (size > 0) memcpy(v, buf, (size_t)size);
  v[size] = '\0';
  *out = v;
  return (const uint8_t *)buf + size;
}

// Bounded fixed-width readers (mirror taosDecodeFixed*) with a buffer-end guard.
static const void *tDecodeI8Safe(const void *buf, const void *bufEnd, int8_t *v) {
  if (!MQ_NEED(buf, bufEnd, 1)) return NULL;
  return taosDecodeFixedI8(buf, v);
}
static const void *tDecodeI16Safe(const void *buf, const void *bufEnd, int16_t *v) {
  if (!MQ_NEED(buf, bufEnd, 2)) return NULL;
  return taosDecodeFixedI16(buf, v);
}
static const void *tDecodeI32Safe(const void *buf, const void *bufEnd, int32_t *v) {
  if (!MQ_NEED(buf, bufEnd, 4)) return NULL;
  return taosDecodeFixedI32(buf, v);
}
static const void *tDecodeI64Safe(const void *buf, const void *bufEnd, int64_t *v) {
  if (!MQ_NEED(buf, bufEnd, 8)) return NULL;
  return taosDecodeFixedI64(buf, v);
}
static const void *tDecodeU16Safe(const void *buf, const void *bufEnd, uint16_t *v) {
  if (!MQ_NEED(buf, bufEnd, 2)) return NULL;
  return taosDecodeFixedU16(buf, v);
}

// Bounded SEpSet decode (mirrors taosDecodeSEpSet) restricting each fqdn to its buffer.
static const void *tDecodeSEpSetSafe(const void *buf, const void *bufEnd, SEpSet *pEp) {
  if (!MQ_NEED(buf, bufEnd, sizeof(pEp->inUse) + sizeof(pEp->numOfEps))) return NULL;
  buf = taosDecodeFixedI8(buf, &pEp->inUse);
  buf = taosDecodeFixedI8(buf, &pEp->numOfEps);
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    buf = tDecodeU16Safe(buf, bufEnd, &pEp->eps[i].port);
    if (buf == NULL) return NULL;
    buf = tDecodeStrToSafe(buf, bufEnd, pEp->eps[i].fqdn, (int32_t)sizeof(pEp->eps[i].fqdn));
    if (buf == NULL) return NULL;
  }
  return buf;
}

static void *tDecodeSMqVgEp(const void *buf, const void *bufEnd, int32_t *vgId, int8_t sver) {
  buf = tDecodeI32Safe(buf, bufEnd, vgId);
  if (buf == NULL) return NULL;
  if (sver == 1) {
    uint64_t size = 0;
    buf = tDecodeVarintUSafe(buf, bufEnd, &size);
    if (buf == NULL) return NULL;
    if (size > (uint64_t)((const uint8_t *)bufEnd - (const uint8_t *)buf)) return NULL;  // bounded skip
    buf = (const uint8_t *)buf + size;
  }
  if (sver <= 3) {
    SEpSet epSet = {0};
    buf = tDecodeSEpSetSafe(buf, bufEnd, &epSet);
    if (buf == NULL) return NULL;
  }
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

void *tDecodeSMqConsumerObj(const void *buf, const void *bufEnd, SMqConsumerObj *pConsumer, int8_t sver) {
  int32_t sz;
  buf = tDecodeI64Safe(buf, bufEnd, &pConsumer->consumerId);
  if (buf == NULL) goto END;
  buf = tDecodeStrToSafe(buf, bufEnd, pConsumer->clientId, (int32_t)sizeof(pConsumer->clientId));
  if (buf == NULL) goto END;
  buf = tDecodeStrToSafe(buf, bufEnd, pConsumer->cgroup, (int32_t)sizeof(pConsumer->cgroup));
  if (buf == NULL) goto END;
  buf = tDecodeI8Safe(buf, bufEnd, &pConsumer->updateType);
  if (buf == NULL) goto END;
  buf = tDecodeI32Safe(buf, bufEnd, &pConsumer->epoch);
  if (buf == NULL) goto END;
  buf = tDecodeI32Safe(buf, bufEnd, &pConsumer->status);
  if (buf == NULL) goto END;

  buf = tDecodeI32Safe(buf, bufEnd, &pConsumer->pid);
  if (buf == NULL) goto END;
  if (sver <= 3) {
    SEpSet ep = {0};
    buf = tDecodeSEpSetSafe(buf, bufEnd, &ep);
    if (buf == NULL) goto END;
  }
  buf = tDecodeI64Safe(buf, bufEnd, &pConsumer->createTime);
  if (buf == NULL) goto END;
  buf = tDecodeI64Safe(buf, bufEnd, &pConsumer->subscribeTime);
  if (buf == NULL) goto END;
  buf = tDecodeI64Safe(buf, bufEnd, &pConsumer->rebalanceTime);
  if (buf == NULL) goto END;

  // current topics
  buf = tDecodeI32Safe(buf, bufEnd, &sz);
  if (buf == NULL) goto END;
  if (sz < 0 || sz > TMQ_DECODE_MAX_COUNT) goto END;
  pConsumer->currentTopics = taosArrayInit(sz, sizeof(void *));
  if (pConsumer->currentTopics == NULL) {
    goto END;
  }
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = tDecodeStrSafe(buf, bufEnd, &topic);
    if (buf == NULL) goto END;
    if (taosArrayPush(pConsumer->currentTopics, &topic) == NULL) {
      goto END;
    }
  }

  // reb new topics
  buf = tDecodeI32Safe(buf, bufEnd, &sz);
  if (buf == NULL) goto END;
  if (sz < 0 || sz > TMQ_DECODE_MAX_COUNT) goto END;
  pConsumer->rebNewTopics = taosArrayInit(sz, sizeof(void *));
  if (pConsumer->rebNewTopics == NULL) goto END;
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = tDecodeStrSafe(buf, bufEnd, &topic);
    if (buf == NULL) goto END;
    if (taosArrayPush(pConsumer->rebNewTopics, &topic) == NULL) {
      goto END;
    }
  }

  // reb removed topics
  buf = tDecodeI32Safe(buf, bufEnd, &sz);
  if (buf == NULL) goto END;
  if (sz < 0 || sz > TMQ_DECODE_MAX_COUNT) goto END;
  pConsumer->rebRemovedTopics = taosArrayInit(sz, sizeof(void *));
  if (pConsumer->rebRemovedTopics == NULL) goto END;
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = tDecodeStrSafe(buf, bufEnd, &topic);
    if (buf == NULL) goto END;
    if (taosArrayPush(pConsumer->rebRemovedTopics, &topic) == NULL) {
      goto END;
    }
  }

  // assigned topics
  buf = tDecodeI32Safe(buf, bufEnd, &sz);
  if (buf == NULL) goto END;
  if (sz < 0 || sz > TMQ_DECODE_MAX_COUNT) goto END;
  pConsumer->assignedTopics = taosArrayInit(sz, sizeof(void *));
  if (pConsumer->assignedTopics == NULL) goto END;
  for (int32_t i = 0; i < sz; i++) {
    char *topic;
    buf = tDecodeStrSafe(buf, bufEnd, &topic);
    if (buf == NULL) goto END;
    if (taosArrayPush(pConsumer->assignedTopics, &topic) == NULL) {
      goto END;
    }
  }

  if (sver > 1) {
    buf = tDecodeI8Safe(buf, bufEnd, &pConsumer->withTbName);
    if (buf == NULL) goto END;
    buf = tDecodeI8Safe(buf, bufEnd, &pConsumer->autoCommit);
    if (buf == NULL) goto END;
    buf = tDecodeI32Safe(buf, bufEnd, &pConsumer->autoCommitInterval);
    if (buf == NULL) goto END;
    buf = tDecodeI32Safe(buf, bufEnd, &pConsumer->resetOffsetCfg);
    if (buf == NULL) goto END;
  }
  if (sver > 2) {
    buf = tDecodeI32Safe(buf, bufEnd, &pConsumer->maxPollIntervalMs);
    if (buf == NULL) goto END;
    buf = tDecodeI32Safe(buf, bufEnd, &pConsumer->sessionTimeoutMs);
    if (buf == NULL) goto END;
    buf = tDecodeStrToSafe(buf, bufEnd, pConsumer->user, (int32_t)sizeof(pConsumer->user));
    if (buf == NULL) goto END;
    buf = tDecodeStrToSafe(buf, bufEnd, pConsumer->fqdn, (int32_t)sizeof(pConsumer->fqdn));
    if (buf == NULL) goto END;
  } else {
    pConsumer->maxPollIntervalMs = DEFAULT_MAX_POLL_INTERVAL;
    pConsumer->sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT;
  }

  return (void *)buf;

END:
  // On any early decode failure pConsumer may be only partially built (some of the
  // currentTopics/rebNewTopics/rebRemovedTopics/assignedTopics arrays and their heap-allocated
  // topic strings may already be allocated). pConsumer itself is still owned by the caller's
  // pRow, so clear its contents here but do NOT taosMemoryFree it.
  tClearSMqConsumerObj(pConsumer);
  return NULL;
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
    int32_t* vgId = (int32_t*)taosArrayGet(pConsumerEp->vgs, i);
    if (vgId == NULL) continue;
    tlen += taosEncodeFixedI32(buf, *vgId);
  }

  return tlen + tEncodeOffRows(buf, pConsumerEp->offsetRows);
}

void *tDecodeOffRows(const void *buf, const void *bufEnd, SArray **offsetRows, int8_t sver) {
  int32_t szVgs = 0;
  buf = tDecodeI32Safe(buf, bufEnd, &szVgs);
  if (buf == NULL) return NULL;
  if (szVgs < 0 || szVgs > TMQ_DECODE_MAX_COUNT) return NULL;
  if (szVgs > 0) {
    *offsetRows = taosArrayInit(szVgs, sizeof(OffsetRows));
    if (NULL == *offsetRows) return NULL;
    for (int32_t j = 0; j < szVgs; ++j) {
      OffsetRows *offRows = taosArrayReserve(*offsetRows, 1);
      if (offRows == NULL) return NULL;
      buf = tDecodeI32Safe(buf, bufEnd, &offRows->vgId);
      if (buf == NULL) return NULL;
      buf = tDecodeI64Safe(buf, bufEnd, &offRows->rows);
      if (buf == NULL) return NULL;
      buf = tDecodeI8Safe(buf, bufEnd, &offRows->offset.type);
      if (buf == NULL) return NULL;
      if (offRows->offset.type == TMQ_OFFSET__SNAPSHOT_DATA || offRows->offset.type == TMQ_OFFSET__SNAPSHOT_META) {
        buf = tDecodeI64Safe(buf, bufEnd, &offRows->offset.uid);
        if (buf == NULL) return NULL;
        buf = tDecodeI64Safe(buf, bufEnd, &offRows->offset.ts);
        if (buf == NULL) return NULL;
      } else if (offRows->offset.type == TMQ_OFFSET__LOG) {
        buf = tDecodeI64Safe(buf, bufEnd, &offRows->offset.version);
        if (buf == NULL) return NULL;
      } else {
        // do nothing
      }
      if (sver > 2) {
        buf = tDecodeI64Safe(buf, bufEnd, &offRows->ever);
        if (buf == NULL) return NULL;
      }
    }
  }
  return (void *)buf;
}

void *tDecodeSMqConsumerEp(const void *buf, const void *bufEnd, SMqConsumerEp *pConsumerEp, int8_t sver) {
  buf = tDecodeI64Safe(buf, bufEnd, &pConsumerEp->consumerId);
  if (buf == NULL) return NULL;
  int32_t sz = 0;
  buf = tDecodeI32Safe(buf, bufEnd, &sz);
  if (buf == NULL) return NULL;
  if (sz < 0 || sz > TMQ_DECODE_MAX_COUNT) return NULL;
  pConsumerEp->vgs = taosArrayInit(sz, sizeof(int32_t));
  if (pConsumerEp->vgs == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    int32_t* vgId = taosArrayReserve(pConsumerEp->vgs, 1);
    if (vgId == NULL) {
      return NULL;
    }
    if (sver <= 3) {
      buf = tDecodeSMqVgEp(buf, bufEnd, vgId, sver);
    } else {
      buf = tDecodeI32Safe(buf, bufEnd, vgId);
    }
    if (buf == NULL) return NULL;
  }
  if (sver > 1) {
    buf = tDecodeOffRows(buf, bufEnd, &pConsumerEp->offsetRows, sver);
    if (buf == NULL) return NULL;
  }

  return (void *)buf;
}

int32_t tNewSubscribeObj(const char *key, SMqSubscribeObj **ppSub) {
  int32_t          code = 0;
  int32_t          lino = 0;
  PRINT_LOG_START
  SMqSubscribeObj *pSubObj = taosMemoryCalloc(1, sizeof(SMqSubscribeObj));
  MND_TMQ_NULL_CHECK(pSubObj);
  
  (void)memcpy(pSubObj->key, key, TSDB_SUBSCRIBE_KEY_LEN);
  taosInitRWLatch(&pSubObj->lock);
  pSubObj->vgNum = 0;
  pSubObj->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  MND_TMQ_NULL_CHECK(pSubObj->consumerHash);
  taosHashSetFreeFp(pSubObj->consumerHash, freeSMqConsumerEp);

  pSubObj->unassignedVgs = taosArrayInit(0, sizeof(int32_t));
  MND_TMQ_NULL_CHECK(pSubObj->unassignedVgs);
  pSubObj->vgDnodeId = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  MND_TMQ_NULL_CHECK(pSubObj->vgDnodeId);
  taosHashSetFreeFp(pSubObj->vgDnodeId, freeVgDnodeId);
  *ppSub = pSubObj;
  pSubObj = NULL;

END:
  tDeleteSubscribeObj(pSubObj);
  PRINT_LOG_END
  return code;
}

int32_t tCloneSubscribeObj(SMqSubscribeObj *pSub, SMqSubscribeObj **ppSub) {
  int32_t          code = 0;
  int32_t          lino = 0;
  PRINT_LOG_START
  taosRLockLatch(&pSub->lock);

  SMqSubscribeObj *pSubNew = taosMemoryCalloc(1, sizeof(SMqSubscribeObj));
  MND_TMQ_NULL_CHECK(pSubNew);
  *ppSub = pSubNew;

  (void)memcpy(pSubNew->key, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);
  taosInitRWLatch(&pSubNew->lock);

  pSubNew->dbUid = pSub->dbUid;
  pSubNew->stbUid = pSub->stbUid;
  pSubNew->subType = pSub->subType;
  pSubNew->withMeta = pSub->withMeta;

  pSubNew->vgNum = pSub->vgNum;
  pSubNew->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  MND_TMQ_NULL_CHECK(pSubNew->consumerHash);
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
    MND_TMQ_RETURN_CHECK(taosHashPut(pSubNew->consumerHash, &newEp.consumerId, sizeof(int64_t), &newEp, sizeof(SMqConsumerEp)));
  }
  pSubNew->unassignedVgs = taosArrayDup(pSub->unassignedVgs, NULL);
  MND_TMQ_NULL_CHECK(pSubNew->unassignedVgs);
  pSubNew->vgDnodeId = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  MND_TMQ_NULL_CHECK(pSubNew->vgDnodeId);
  taosHashSetFreeFp(pSubNew->vgDnodeId, freeVgDnodeId);
  void *pVgIter = NULL;
  while (1) {
    pVgIter = taosHashIterate(pSub->vgDnodeId, pVgIter);
    if (pVgIter == NULL) {
      break;
    }
    int32_t *pVgId = (int32_t *)taosHashGetKey(pVgIter, NULL);
    SArray **ppDnodeIds = (SArray **)pVgIter;
    MND_TMQ_NULL_CHECK(pVgId);
    MND_TMQ_NULL_CHECK(ppDnodeIds);
    SArray *pDnodeIdsNew = taosArrayDup(*ppDnodeIds, NULL);
    MND_TMQ_NULL_CHECK(pDnodeIdsNew);
    if (taosHashPut(pSubNew->vgDnodeId, pVgId, sizeof(int32_t), &pDnodeIdsNew, sizeof(SArray*)) != 0) {
      taosArrayDestroy(pDnodeIdsNew);
      code = terrno;
      lino = __LINE__;
      goto END;
    }
  }
  
  pSubNew->offsetRows = taosArrayDup(pSub->offsetRows, NULL);
  MND_TMQ_CONDITION_CHECK(pSub->offsetRows == NULL || pSubNew->offsetRows != NULL, terrno);
  (void)memcpy(pSubNew->dbName, pSub->dbName, TSDB_DB_FNAME_LEN);

END:
  taosRUnLockLatch(&pSub->lock);
  return code;
}

void tDeleteSubscribeObj(SMqSubscribeObj *pSub) {
  if (pSub == NULL) return;
  taosHashCleanup(pSub->consumerHash);
  pSub->consumerHash = NULL;
  taosArrayDestroy(pSub->unassignedVgs);
  pSub->unassignedVgs = NULL;
  taosArrayDestroy(pSub->offsetRows);
  pSub->offsetRows = NULL;
  taosHashCleanup(pSub->vgDnodeId);
  pSub->vgDnodeId = NULL;
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
    int32_t* vgId = (int32_t*)taosArrayGet(pSub->unassignedVgs, i);
    if (vgId == NULL) continue;
    tlen += taosEncodeFixedI32(buf, *vgId);
  }
  tlen += taosEncodeString(buf, pSub->dbName);
  int32_t vgDnodeNum = taosHashGetSize(pSub->vgDnodeId);
  tlen += taosEncodeFixedI32(buf, vgDnodeNum);
  pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pSub->vgDnodeId, pIter);
    if (pIter == NULL) {
      break;
    }
    int32_t *pVgId = (int32_t *)taosHashGetKey(pIter, NULL);
    SArray **ppDnodeIds = (SArray **)pIter;
    if (pVgId == NULL || ppDnodeIds == NULL || *ppDnodeIds == NULL) {
      return -1;
    }
    int32_t dnodeNum = taosArrayGetSize(*ppDnodeIds);
    tlen += taosEncodeFixedI32(buf, *pVgId);
    tlen += taosEncodeFixedI32(buf, dnodeNum);
    for (int32_t i = 0; i < dnodeNum; ++i) {
      int32_t *pDnodeId = (int32_t *)taosArrayGet(*ppDnodeIds, i);
      if (pDnodeId == NULL) {
        return -1;
      }
      tlen += taosEncodeFixedI32(buf, *pDnodeId);
    }
  }

  tlen += tEncodeOffRows(buf, pSub->offsetRows);
  return tlen;
}

void *tDecodeSubscribeObj(const void *buf, const void *bufEnd, SMqSubscribeObj *pSub, int8_t sver) {
  //
  buf = tDecodeStrToSafe(buf, bufEnd, pSub->key, (int32_t)sizeof(pSub->key));
  if (buf == NULL) goto END;
  buf = tDecodeI64Safe(buf, bufEnd, &pSub->dbUid);
  if (buf == NULL) goto END;
  buf = tDecodeI32Safe(buf, bufEnd, &pSub->vgNum);
  if (buf == NULL) goto END;
  buf = tDecodeI8Safe(buf, bufEnd, &pSub->subType);
  if (buf == NULL) goto END;
  buf = tDecodeI8Safe(buf, bufEnd, &pSub->withMeta);
  if (buf == NULL) goto END;
  buf = tDecodeI64Safe(buf, bufEnd, &pSub->stbUid);
  if (buf == NULL) goto END;

  int32_t sz;
  buf = tDecodeI32Safe(buf, bufEnd, &sz);
  if (buf == NULL) goto END;
  if (sz < 0 || sz > TMQ_DECODE_MAX_COUNT) goto END;

  pSub->consumerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (pSub->consumerHash == NULL) {
    goto END;
  }
  taosHashSetFreeFp(pSub->consumerHash, freeSMqConsumerEp);

  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp consumerEp = {0};
    buf = tDecodeSMqConsumerEp(buf, bufEnd, &consumerEp, sver);
    if (buf == NULL) {
      freeSMqConsumerEp(&consumerEp);
      goto END;
    }
    if (taosHashPut(pSub->consumerHash, &consumerEp.consumerId, sizeof(int64_t), &consumerEp, sizeof(SMqConsumerEp)) !=
        0) {
      mError("failed to store subscribe consumer, consumerId:%" PRId64, consumerEp.consumerId);
      freeSMqConsumerEp(&consumerEp);
      goto END;
    }
  }

  int32_t len = 0;
  buf = tDecodeI32Safe(buf, bufEnd, &len);
  if (buf == NULL) goto END;
  if (len < 0 || len > TMQ_DECODE_MAX_COUNT) goto END;
  pSub->unassignedVgs = taosArrayInit(len, sizeof(int32_t));
  if (pSub->unassignedVgs == NULL) {
    goto END;
  }
  for (int32_t i = 0; i < len; i++) {
    int32_t* vgId = taosArrayReserve(pSub->unassignedVgs, 1);
    if (vgId == NULL) {
      goto END;
    }
    if (sver <= 3) {
      buf = tDecodeSMqVgEp(buf, bufEnd, vgId, sver);
    } else {
      buf = tDecodeI32Safe(buf, bufEnd, vgId);
    }
    if (buf == NULL) goto END;
  }

  buf = tDecodeStrToSafe(buf, bufEnd, pSub->dbName, (int32_t)sizeof(pSub->dbName));
  if (buf == NULL) goto END;

  pSub->vgDnodeId = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pSub->vgDnodeId == NULL) {
    goto END;
  }
  taosHashSetFreeFp(pSub->vgDnodeId, freeVgDnodeId);
  if (sver > 4) {
    int32_t vgDnodeNum = 0;
    buf = tDecodeI32Safe(buf, bufEnd, &vgDnodeNum);
    if (buf == NULL) goto END;
    if (vgDnodeNum < 0 || vgDnodeNum > TMQ_DECODE_MAX_COUNT) goto END;
    for (int32_t i = 0; i < vgDnodeNum; ++i) {
      int32_t vgId = 0;
      int32_t dnodeNum = 0;
      SArray *pDnodeIds = NULL;
      buf = tDecodeI32Safe(buf, bufEnd, &vgId);
      if (buf == NULL) goto END;
      buf = tDecodeI32Safe(buf, bufEnd, &dnodeNum);
      if (buf == NULL) goto END;
      if (dnodeNum < 0 || dnodeNum > TMQ_DECODE_MAX_COUNT) goto END;
      pDnodeIds = taosArrayInit(dnodeNum, sizeof(int32_t));
      if (pDnodeIds == NULL) {
        mError("failed to decode subscribe vg dnode ids, vgId:%d dnodeNum:%d", vgId, dnodeNum);
        goto END;
      }
      for (int32_t j = 0; j < dnodeNum; ++j) {
        int32_t *pDnodeId = taosArrayReserve(pDnodeIds, 1);
        if (pDnodeId == NULL) {
          mError("failed to decode subscribe dnode id, vgId:%d dnodeIndex:%d", vgId, j);
          taosArrayDestroy(pDnodeIds);
          goto END;
        }
        buf = tDecodeI32Safe(buf, bufEnd, pDnodeId);
        if (buf == NULL) {
          taosArrayDestroy(pDnodeIds);
          goto END;
        }
      }
      if (taosHashPut(pSub->vgDnodeId, &vgId, sizeof(int32_t), &pDnodeIds, sizeof(SArray*)) != 0) {
        mError("failed to store subscribe vg dnode ids, vgId:%d dnodeNum:%d", vgId, dnodeNum);
        taosArrayDestroy(pDnodeIds);
        goto END;
      }
    }
  }

  if (sver > 1) {
    buf = tDecodeOffRows(buf, bufEnd, &pSub->offsetRows, sver);
    if (buf == NULL) goto END;
  }
  return (void *)buf;

END:
  tDeleteSubscribeObj(pSub);
  return NULL;
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
  // pObj->name is the fixed CFG_NAME_MAX_LEN array of the sdb row, not a caller-owned buffer that
  // is guaranteed to hold the whole message: bind this decode to the destination so a raw that
  // carries a longer name is rejected instead of overflowing into pObj->dtype / the value union
  // and the heap past the row (tDecodeCStrTo only checks the length against the source).
  TAOS_CHECK_RETURN(tDecodeCStrToFixed(pDecoder, pObj->name, CFG_NAME_MAX_LEN));

  int32_t dtype = 0;
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &dtype));
  // No producer emits a dtype outside ECfgDataType, and neither the value switch below nor the
  // consumers of SConfigObj (mndCfgActionUpdate, initConfigArrayFromSdb) have a default branch,
  // so an out-of-range value would be stored as-is by a crafted raw. Reject it here.
  if (dtype < CFG_DTYPE_NONE || dtype > CFG_DTYPE_TIMEZONE) {
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }
  pObj->dtype = (ECfgDataType)dtype;

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

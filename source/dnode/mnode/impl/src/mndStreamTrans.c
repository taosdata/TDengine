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

#include "mndStream.h"
#include "mndTrans.h"

#define MAX_CHKPT_EXEC_ELAPSED (600*1000*3)  // 600s

typedef struct SKeyInfo {
  void   *pKey;
  int32_t keyLen;
} SKeyInfo;

static bool identicalName(const char *pDb, const char *pParam, int32_t len) {
  return (strlen(pDb) == len) && (strncmp(pDb, pParam, len) == 0);
}

int32_t mndStreamCreateTrans(SMnode *pMnode, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *name, STrans **ppTrans) {
  int64_t streamId = pStream->pCreate->streamId;
  int32_t code = 0;

  STrans *p = mndTransCreate(pMnode, TRN_POLICY_RETRY, conflict, pReq, name);
  if (p == NULL) {
    mstsError("failed to build trans:%s, reason: %s", name, tstrerror(terrno));
    return terrno;
  }

  mstsInfo("start to build trans %s, transId:%d", name, p->id);
  p->ableToBeKilled = true;

  mndTransSetDbName(p, pStream->pCreate->streamDB, pStream->name);
  if ((code = mndTransCheckConflict(pMnode, p)) != 0) {
    mstsError("failed to build trans:%s for stream, code:%s", name, tstrerror(terrno));
    mndTransDrop(p);
    return code;
  }

  *ppTrans = p;
  return code;
}

static SSdbRaw *mndStreamActionEncodeImmutable(const SStreamObj *pStream) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *buf = NULL;
  int64_t streamId = pStream->pCreate->streamId;

  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);
  if ((code = tEncodeSStreamObj(&encoder, pStream)) < 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code, lino, _over);
  }

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  int32_t  size = sizeof(int32_t) + tlen + MND_STREAM_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_STREAM, MND_STREAM_VER_NUMBER, size);
  TSDB_CHECK_NULL(pRaw, code, lino, _over, terrno);

  buf = taosMemoryMalloc(tlen);
  TSDB_CHECK_NULL(buf, code, lino, _over, terrno);

  tEncoderInit(&encoder, buf, tlen);
  if ((code = tEncodeSStreamObj(&encoder, pStream)) < 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code, lino, _over);
  }

  tEncoderClear(&encoder);

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, _over);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, _over);
  SDB_SET_INT8(pRaw, dataPos, MND_STREAM_RAW_UPDATE_FULL, _over);
  SDB_SET_DATALEN(pRaw, dataPos, _over);

_over:

  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    mstsError("failed to encode stream %s to raw:%p at line:%d since %s", pStream->pCreate->name, pRaw, lino, tstrerror(code));
    sdbFreeRaw(pRaw);
    terrno = code;
    return NULL;
  }

  mstsTrace("stream %s encoded to raw:%p", pStream->pCreate->name, pRaw);
         
  return pRaw;
}

static int32_t mndStreamValidateRecalcPatchPayloadSize(const SStreamObj *pStream, size_t requestNum) {
  if (requestNum > INT32_MAX) return TSDB_CODE_OUT_OF_RANGE;

  int32_t  code = TSDB_CODE_SUCCESS;
  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);
  if ((code = tStartEncode(&encoder)) != TSDB_CODE_SUCCESS ||
      (code = tEncodeCStr(&encoder, pStream->name)) != TSDB_CODE_SUCCESS ||
      (code = tEncodeU64(&encoder, 0)) != TSDB_CODE_SUCCESS ||
      (code = tEncodeI32(&encoder, (int32_t)requestNum)) != TSDB_CODE_SUCCESS) {
    tEncoderClear(&encoder);
    return code;
  }

  const uint64_t fixedLength = encoder.pos;
  const uint64_t maxPayloadLength = INT32_MAX - sizeof(int32_t) - MND_STREAM_RESERVE_SIZE;
  tEncoderClear(&encoder);
  if (fixedLength > maxPayloadLength || requestNum > (maxPayloadLength - fixedLength) / (4 * sizeof(int64_t))) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t mndStreamEncodeRecalcPatchPayload(SEncoder *pEncoder, const SStreamObj *pStream, uint64_t revision,
                                                 const SArray *pRequests) {
  size_t requestNum = taosArrayGetSize(pRequests);
  TAOS_CHECK_RETURN(mndStreamValidateRecalcPatchPayloadSize(pStream, requestNum));

  TAOS_CHECK_RETURN(tStartEncode(pEncoder));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pStream->name));
  TAOS_CHECK_RETURN(tEncodeU64(pEncoder, revision));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, (int32_t)requestNum));
  for (int32_t i = 0; i < (int32_t)requestNum; ++i) {
    const SStreamRecalcPersistReq *pReq = taosArrayGet(pRequests, i);
    if (pReq == NULL || pReq->recalcId == 0 || pReq->end <= pReq->start || pReq->requestTimeMs <= 0) {
      return TSDB_CODE_INVALID_MSG;
    }
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->recalcId));
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->start));
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->end));
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->requestTimeMs));
  }
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

static SSdbRaw *mndStreamActionEncodeRecalcPatch(const SStreamObj *pStream, uint64_t revision,
                                                 const SArray *pRequests) {
  int32_t  code = 0;
  int32_t  lino = 0;
  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int64_t  streamId = pStream->pCreate->streamId;

  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);
  code = mndStreamEncodeRecalcPatchPayload(&encoder, pStream, revision, pRequests);
  if (code < 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code, lino, _over);
  }

  uint32_t payloadLength = encoder.pos;
  tEncoderClear(&encoder);
  if (payloadLength > (uint32_t)(INT32_MAX - sizeof(int32_t) - MND_STREAM_RESERVE_SIZE)) {
    code = TSDB_CODE_OUT_OF_RANGE;
    TSDB_CHECK_CODE(code, lino, _over);
  }
  int32_t tlen = (int32_t)payloadLength;

  int32_t size = sizeof(int32_t) + tlen + MND_STREAM_RESERVE_SIZE;
  pRaw = sdbAllocRaw(SDB_STREAM, MND_STREAM_VER_NUMBER, size);
  TSDB_CHECK_NULL(pRaw, code, lino, _over, terrno);

  buf = taosMemoryMalloc(tlen);
  TSDB_CHECK_NULL(buf, code, lino, _over, terrno);

  tEncoderInit(&encoder, buf, tlen);
  code = mndStreamEncodeRecalcPatchPayload(&encoder, pStream, revision, pRequests);
  if (code < 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code, lino, _over);
  }
  tEncoderClear(&encoder);

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, _over);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, _over);
  SDB_SET_INT8(pRaw, dataPos, MND_STREAM_RAW_UPDATE_RECALC_PATCH, _over);
  SDB_SET_DATALEN(pRaw, dataPos, _over);

_over:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    mstsError("failed to encode stream %s recalculation patch at line:%d since %s", pStream->name, lino,
              tstrerror(code));
    sdbFreeRaw(pRaw);
    terrno = code;
    return NULL;
  }
  return pRaw;
}

SSdbRaw *mndStreamActionEncode(SStreamObj *pStream) {
  taosRLockLatch(&pStream->lock);
  SSdbRaw *pRaw = mndStreamActionEncodeImmutable(pStream);
  taosRUnLockLatch(&pStream->lock);
  return pRaw;
}

static int32_t mndStreamTransAppendRaw(int64_t streamId, STrans *pTrans, SSdbRaw *pCommitRaw, int32_t status) {
  if (pCommitRaw == NULL) {
    mstsError("failed to encode stream since %s", terrstr());
    return terrno;
  }

  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mstsError("stream trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    sdbFreeRaw(pCommitRaw);
    return terrno;
  }

  if (sdbSetRawStatus(pCommitRaw, status) != 0) {
    mstsError("stream trans:%d failed to set raw status:%d since %s", pTrans->id, status, terrstr());
    return terrno;
  }

  return 0;
}

int32_t mndStreamTransAppend(SStreamObj *pStream, STrans *pTrans, int32_t status) {
  taosRLockLatch(&pStream->lock);
  int64_t  streamId = pStream->pCreate->streamId;
  SSdbRaw *pRaw = mndStreamActionEncodeImmutable(pStream);
  taosRUnLockLatch(&pStream->lock);
  return mndStreamTransAppendRaw(streamId, pTrans, pRaw, status);
}

int32_t mndStreamTransAppendLifecycleUpdate(SStreamObj *pStream, int8_t userStopped, int64_t updateTime,
                                            STrans *pTrans) {
  taosRLockLatch(&pStream->lock);
  SStreamObj updated = *pStream;
  updated.userStopped = userStopped;
  updated.updateTime = updateTime;
  SSdbRaw *pRaw = mndStreamActionEncodeImmutable(&updated);
  taosRUnLockLatch(&pStream->lock);
  return mndStreamTransAppendRaw(pStream->pCreate->streamId, pTrans, pRaw, SDB_STATUS_READY);
}

int32_t mndStreamTransAppendRecalcUpdate(SStreamObj *pStream, uint64_t revision, SArray *pRequests, STrans *pTrans,
                                         int32_t status) {
  taosRLockLatch(&pStream->lock);
  int64_t  streamId = pStream->pCreate->streamId;
  SSdbRaw *pRaw = mndStreamActionEncodeRecalcPatch(pStream, revision, pRequests);
  taosRUnLockLatch(&pStream->lock);
  return mndStreamTransAppendRaw(streamId, pTrans, pRaw, status);
}

int32_t setTransAction(STrans *pTrans, void *pCont, int32_t contLen, int32_t msgType, const SEpSet *pEpset,
                       int32_t retryCode, int32_t acceptCode) {
  STransAction action = {.epSet = *pEpset,
                         .contLen = contLen,
                         .pCont = pCont,
                         .msgType = msgType,
                         .retryCode = retryCode,
                         .acceptableCode = acceptCode};
  return mndTransAppendRedoAction(pTrans, &action);
}

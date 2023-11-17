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
#include "mndCompact.h"
#include "mndTrans.h"

#define MND_COMPACT_VER_NUMBER 1

int32_t mndInitCompact(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_COMPACT,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndCompactActionEncode,
      .decodeFp = (SdbDecodeFp)mndCompactActionDecode,
      .insertFp = (SdbInsertFp)mndCompactActionInsert,
      .updateFp = (SdbUpdateFp)mndCompactActionUpdate,
      .deleteFp = (SdbDeleteFp)mndCompactActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupCompact(SMnode *pMnode) {
  mDebug("mnd view cleanup");
}

int32_t mndProcessCreateCompactReq(SRpcMsg *pReq) {
  return 0;
}

void tFreeCompactObj(SCompactObj *pCompact) {
  //int32_t size = taosArrayGetSize(pCompact->compactDetail);

  //for (int32_t i = 0; i < size; ++i) {
  //  SCompactDetailObj *detail = taosArrayGet(pCompact->compactDetail, i);
  //  taosMemoryFree(detail);
  //}

  taosArrayDestroy(pCompact->compactDetail);
}

int32_t tSerializeSCompactObj(void *buf, int32_t bufLen, const SCompactObj *pObj) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI64(&encoder, pObj->compactId) < 0) return -1;
  if (tEncodeCStr(&encoder, pObj->dbname) < 0) return -1;
  if (tEncodeI64(&encoder, pObj->startTime) < 0) return -1;

  int32_t size = taosArrayGetSize(pObj->compactDetail);
  if (tEncodeI32(&encoder, size) < 0) return -1;

  for (int32_t i = 0; i < size; ++i) {
    SCompactDetailObj *detail = taosArrayGet(pObj->compactDetail, i);
    if (tEncodeI32(&encoder, detail->vgId) < 0) return -1;
    if (tEncodeI32(&encoder, detail->dnodeId) < 0) return -1;
    if (tEncodeI32(&encoder, detail->numberFileset) < 0) return -1;
    if (tEncodeI32(&encoder, detail->finished) < 0) return -1;
    if (tEncodeI64(&encoder, detail->startTime) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCompactObj(void *buf, int32_t bufLen, SCompactObj *pObj) {
  int8_t ex = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pObj->compactId) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pObj->dbname) < 0) return -1;
  if (tDecodeI64(&decoder, &pObj->startTime) < 0) return -1;
  
  int32_t size = 0;
  if (tDecodeI32(&decoder, &size) < 0) return -1;
  pObj->compactDetail = taosArrayInit(size, sizeof(SCompactObj));

  for (int32_t i = 0; i < size; ++i){
    SCompactDetailObj detail = {0};
    if (tDecodeI32(&decoder, &detail.vgId) < 0) return -1;
    if (tDecodeI32(&decoder, &detail.dnodeId) < 0) return -1;
    if (tDecodeI32(&decoder, &detail.numberFileset) < 0) return -1;
    if (tDecodeI32(&decoder, &detail.finished) < 0) return -1;
    if (tDecodeI64(&decoder, &detail.startTime) < 0) return -1;
    taosArrayPush(pObj->compactDetail, &detail);
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

SSdbRaw *mndCompactActionEncode(SCompactObj *pCompact) {
  terrno = TSDB_CODE_SUCCESS;

  void *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSCompactObj(NULL, 0, pCompact);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }
  
  int32_t  size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_COMPACT, MND_COMPACT_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSCompactObj(buf, tlen, pCompact);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, OVER);
  SDB_SET_DATALEN(pRaw, dataPos, OVER);


OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("compact:%" PRId64 ", failed to encode to raw:%p since %s", pCompact->compactId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("compact:%" PRId64 ", encode to raw:%p, row:%p", pCompact->compactId, pRaw, pCompact);
  return pRaw;
}

SSdbRow *mndCompactActionDecode(SSdbRaw *pRaw) {
  SSdbRow       *pRow = NULL;
  SCompactObj   *pCompact = NULL;
  void          *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_COMPACT_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("view read invalid ver, data ver: %d, curr ver: %d", sver, MND_COMPACT_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SCompactObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pCompact = sdbGetRowObj(pRow);
  if (pCompact == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, OVER);

  if (tDeserializeSCompactObj(buf, tlen, pCompact) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  //taosInitRWLatch(&pView->lock);

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("compact:%" PRId64 ", failed to decode from raw:%p since %s", pCompact->compactId, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("compact:%" PRId64 ", decode from raw:%p, row:%p", pCompact->compactId, pRaw, pCompact);
  return pRow;
}

int32_t mndCompactActionInsert(SSdb *pSdb, SCompactObj *pCompact) {
  mTrace("compact:%" PRId64 ", perform insert action", pCompact->compactId);
  return 0;
}

int32_t mndCompactActionDelete(SSdb *pSdb, SCompactObj *pCompact) {
  mTrace("compact:%" PRId64 ", perform insert action", pCompact->compactId);
  tFreeCompactObj(pCompact);
  return 0;
}

int32_t mndCompactActionUpdate(SSdb *pSdb, SCompactObj *pOldCompact, SCompactObj *pNewCompact) {
  mTrace("compact:%" PRId64 ", perform update action, old row:%p new row:%p", 
          pOldCompact->compactId, pOldCompact, pNewCompact);

  TSWAP(pOldCompact->compactDetail, pNewCompact->compactDetail);

  return 0;
}

int32_t mndAddCompactToTran(SMnode *pMnode, STrans *pTrans, SCompactObj* pCompact, SDbObj *pDb){
  char uuid[40];
  int32_t code = taosGetSystemUUID(uuid, 40);
  if (code != 0) {
    strcpy(uuid, "tdengine3.0");
    mError("failed to get name from system, set to default val %s", uuid);
  }

  pCompact->compactId = mndGenerateUid(uuid, TSDB_CLUSTER_ID_LEN);
  pCompact->compactId = (pCompact->compactId >= 0 ? pCompact->compactId : -pCompact->compactId);

  strcpy(pCompact->dbname, pDb->name);

  pCompact->startTime = taosGetTimestampMs();

  SSdbRaw *pVgRaw = mndCompactActionEncode(pCompact);
  if (pVgRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pVgRaw) != 0) {
    sdbFreeRaw(pVgRaw);
    return -1;
  }
  (void)sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);

  return 0;
}

int32_t mndAddCompactDetailToTran(SMnode *pMnode, STrans *pTrans, SCompactObj* pCompact, SVgObj *pVgroup){
  SCompactDetailObj compactDetail;
  
  compactDetail.vgId = pVgroup->vgId;
  compactDetail.startTime = taosGetTimestampMs();

  taosArrayPush(pCompact->compactDetail, &compactDetail);

  return 0;
}
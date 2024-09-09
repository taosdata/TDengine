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
#include "mndCompactDetail.h"
#include "mndDb.h"
#include "mndShow.h"
#include "mndTrans.h"

#define MND_COMPACT_VER_NUMBER 1

int32_t mndInitCompactDetail(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_COMPACT_DETAIL, mndRetrieveCompactDetail);

  SSdbTable table = {
      .sdbType = SDB_COMPACT_DETAIL,
      .keyType = SDB_KEY_INT64,
      .encodeFp = (SdbEncodeFp)mndCompactDetailActionEncode,
      .decodeFp = (SdbDecodeFp)mndCompactDetailActionDecode,
      .insertFp = (SdbInsertFp)mndCompactDetailActionInsert,
      .updateFp = (SdbUpdateFp)mndCompactDetailActionUpdate,
      .deleteFp = (SdbDeleteFp)mndCompactDetailActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupCompactDetail(SMnode *pMnode) { mDebug("mnd compact detail cleanup"); }

int32_t mndRetrieveCompactDetail(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode            *pMnode = pReq->info.node;
  SSdb              *pSdb = pMnode->pSdb;
  int32_t            numOfRows = 0;
  SCompactDetailObj *pCompactDetail = NULL;
  char              *sep = NULL;
  SDbObj            *pDb = NULL;

  if (strlen(pShow->db) > 0) {
    sep = strchr(pShow->db, '.');
    if (sep &&
        ((0 == strcmp(sep + 1, TSDB_INFORMATION_SCHEMA_DB) || (0 == strcmp(sep + 1, TSDB_PERFORMANCE_SCHEMA_DB))))) {
      sep++;
    } else {
      pDb = mndAcquireDb(pMnode, pShow->db);
      if (pDb == NULL) return terrno;
    }
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_COMPACT_DETAIL, pShow->pIter, (void **)&pCompactDetail);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->compactId, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->vgId, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->dnodeId, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->numberFileset, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->finished, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->startTime, false);

    numOfRows++;
    sdbRelease(pSdb, pCompactDetail);
  }

  pShow->numOfRows += numOfRows;
  mndReleaseDb(pMnode, pDb);
  return numOfRows;
}

void tFreeCompactDetailObj(SCompactDetailObj *pCompact) {}

int32_t tSerializeSCompactDetailObj(void *buf, int32_t bufLen, const SCompactDetailObj *pObj) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->compactDetailId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->compactId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->vgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->numberFileset));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->finished));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->startTime));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->newNumberFileset));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->newFinished));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCompactDetailObj(void *buf, int32_t bufLen, SCompactDetailObj *pObj) {
  int32_t  code = 0;
  int32_t  lino;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->compactDetailId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->compactId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->vgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->numberFileset));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->finished));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->startTime));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->newNumberFileset));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->newFinished));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

SSdbRaw *mndCompactDetailActionEncode(SCompactDetailObj *pCompact) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSCompactDetailObj(NULL, 0, pCompact);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_COMPACT_DETAIL, MND_COMPACT_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSCompactDetailObj(buf, tlen, pCompact);
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
    mError("compact detail:%" PRId32 ", failed to encode to raw:%p since %s", pCompact->compactId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("compact detail:%" PRId32 ", encode to raw:%p, row:%p", pCompact->compactId, pRaw, pCompact);
  return pRaw;
}

SSdbRow *mndCompactDetailActionDecode(SSdbRaw *pRaw) {
  int32_t            code = 0;
  int32_t            lino = 0;
  SSdbRow           *pRow = NULL;
  SCompactDetailObj *pCompact = NULL;
  void              *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_COMPACT_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("compact detail read invalid ver, data ver: %d, curr ver: %d", sver, MND_COMPACT_VER_NUMBER);
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

  if ((terrno = tDeserializeSCompactDetailObj(buf, tlen, pCompact)) < 0) {
    goto OVER;
  }

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("compact detail:%" PRId32 ", failed to decode from raw:%p since %s", pCompact->compactId, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("compact detail:%" PRId32 ", decode from raw:%p, row:%p", pCompact->compactId, pRaw, pCompact);
  return pRow;
}

int32_t mndCompactDetailActionInsert(SSdb *pSdb, SCompactDetailObj *pCompact) {
  mTrace("compact detail:%" PRId32 ", perform insert action", pCompact->compactId);
  return 0;
}

int32_t mndCompactDetailActionDelete(SSdb *pSdb, SCompactDetailObj *pCompact) {
  mTrace("compact detail:%" PRId32 ", perform insert action", pCompact->compactId);
  tFreeCompactDetailObj(pCompact);
  return 0;
}

int32_t mndCompactDetailActionUpdate(SSdb *pSdb, SCompactDetailObj *pOldCompact, SCompactDetailObj *pNewCompact) {
  mTrace("compact detail:%" PRId32 ", perform update action, old row:%p new row:%p", pOldCompact->compactId,
         pOldCompact, pNewCompact);

  pOldCompact->numberFileset = pNewCompact->numberFileset;
  pOldCompact->finished = pNewCompact->finished;

  return 0;
}

int32_t mndAddCompactDetailToTran(SMnode *pMnode, STrans *pTrans, SCompactObj *pCompact, SVgObj *pVgroup,
                                  SVnodeGid *pVgid, int32_t index) {
  int32_t           code = 0;
  SCompactDetailObj compactDetail = {0};
  compactDetail.compactDetailId = index;
  compactDetail.compactId = pCompact->compactId;
  compactDetail.vgId = pVgroup->vgId;
  compactDetail.dnodeId = pVgid->dnodeId;
  compactDetail.startTime = taosGetTimestampMs();
  compactDetail.numberFileset = -1;
  compactDetail.finished = -1;
  compactDetail.newNumberFileset = -1;
  compactDetail.newFinished = -1;

  mInfo("compact:%d, add compact detail to trans, index:%d, vgId:%d, dnodeId:%d", compactDetail.compactId,
        compactDetail.compactDetailId, compactDetail.vgId, compactDetail.dnodeId);

  SSdbRaw *pVgRaw = mndCompactDetailActionEncode(&compactDetail);
  if (pVgRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pVgRaw) != 0) {
    sdbFreeRaw(pVgRaw);
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  (void)sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);

  TAOS_RETURN(code);
}

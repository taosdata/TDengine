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
#include "mndScanDetail.h"
#include "mndDb.h"
#include "mndShow.h"
#include "mndTrans.h"

#define MND_SCAN_VER_NUMBER 1

static int32_t  mndRetrieveScanDetail(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static int32_t  mndScanDetailActionInsert(SSdb *pSdb, SScanDetailObj *pScan);
static int32_t  mndScanDetailActionUpdate(SSdb *pSdb, SScanDetailObj *pOldScan, SScanDetailObj *pNewScan);
static int32_t  mndScanDetailActionDelete(SSdb *pSdb, SScanDetailObj *pScan);

int32_t mndInitScanDetail(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SCAN_DETAIL, mndRetrieveScanDetail);

  SSdbTable table = {
      .sdbType = SDB_SCAN_DETAIL,
      .keyType = SDB_KEY_INT64,
      .encodeFp = (SdbEncodeFp)mndScanDetailActionEncode,
      .decodeFp = (SdbDecodeFp)mndScanDetailActionDecode,
      .insertFp = (SdbInsertFp)mndScanDetailActionInsert,
      .updateFp = (SdbUpdateFp)mndScanDetailActionUpdate,
      .deleteFp = (SdbDeleteFp)mndScanDetailActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupScanDetail(SMnode *pMnode) { mDebug("mnd scan detail cleanup"); }

static int32_t mndRetrieveScanDetail(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode         *pMnode = pReq->info.node;
  SSdb           *pSdb = pMnode->pSdb;
  int32_t         numOfRows = 0;
  SScanDetailObj *pScanDetail = NULL;
  char           *sep = NULL;
  SDbObj         *pDb = NULL;

  mInfo("retrieve scan detail");

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
    pShow->pIter = sdbFetch(pSdb, SDB_SCAN_DETAIL, pShow->pIter, (void **)&pScanDetail);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pScanDetail->scanId, false), pSdb,
                                   pScanDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pScanDetail->vgId, false), pSdb,
                                   pScanDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pScanDetail->dnodeId, false), pSdb,
                                   pScanDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pScanDetail->numberFileset, false),
                                   pSdb, pScanDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pScanDetail->finished, false),
                                   pSdb, pScanDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pScanDetail->startTime, false),
                                   pSdb, pScanDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pScanDetail->progress, false),
                                   pSdb, pScanDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pScanDetail->remainingTime, false),
                                   pSdb, pScanDetail);

    numOfRows++;
    sdbRelease(pSdb, pScanDetail);
  }

  pShow->numOfRows += numOfRows;
  mndReleaseDb(pMnode, pDb);
  return numOfRows;
}

void tFreeScanDetailObj(SScanDetailObj *pScan) {}

static int32_t tSerializeSScanDetailObj(void *buf, int32_t bufLen, const SScanDetailObj *pObj) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->scanDetailId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->scanId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->vgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->numberFileset));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->finished));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->startTime));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->newNumberFileset));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->newFinished));
  // 1. add progress and remaining time
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->progress));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->remainingTime));

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

static int32_t tDeserializeSScanDetailObj(void *buf, int32_t bufLen, SScanDetailObj *pObj) {
  int32_t  code = 0;
  int32_t  lino;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->scanDetailId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->scanId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->vgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->numberFileset));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->finished));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->startTime));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->newNumberFileset));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->newFinished));
  // 1. add progress and remaining time decode
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pObj->progress));
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->remainingTime));
  } else {
    pObj->progress = 0;
    pObj->remainingTime = 0;
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

SSdbRaw *mndScanDetailActionEncode(SScanDetailObj *pScan) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSScanDetailObj(NULL, 0, pScan);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_SCAN_DETAIL, MND_SCAN_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSScanDetailObj(buf, tlen, pScan);
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
    mError("scan detail:%" PRId32 ", failed to encode to raw:%p since %s", pScan->scanId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("scan detail:%" PRId32 ", encode to raw:%p, row:%p", pScan->scanId, pRaw, pScan);
  return pRaw;
}

SSdbRow *mndScanDetailActionDecode(SSdbRaw *pRaw) {
  int32_t         code = 0;
  int32_t         lino = 0;
  SSdbRow        *pRow = NULL;
  SScanDetailObj *pScan = NULL;
  void           *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_SCAN_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("scan detail read invalid ver, data ver: %d, curr ver: %d", sver, MND_SCAN_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SScanObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pScan = sdbGetRowObj(pRow);
  if (pScan == NULL) {
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

  if ((terrno = tDeserializeSScanDetailObj(buf, tlen, pScan)) < 0) {
    goto OVER;
  }

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("scan detail:%" PRId32 ", failed to decode from raw:%p since %s", pScan->scanId, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("scan detail:%" PRId32 ", decode from raw:%p, row:%p", pScan->scanId, pRaw, pScan);
  return pRow;
}

static int32_t mndScanDetailActionInsert(SSdb *pSdb, SScanDetailObj *pScan) {
  mTrace("scan detail:%" PRId32 ", perform insert action", pScan->scanId);
  return 0;
}

static int32_t mndScanDetailActionDelete(SSdb *pSdb, SScanDetailObj *pScan) {
  mTrace("scan detail:%" PRId32 ", perform insert action", pScan->scanId);
  tFreeScanDetailObj(pScan);
  return 0;
}

static int32_t mndScanDetailActionUpdate(SSdb *pSdb, SScanDetailObj *pOldScan, SScanDetailObj *pNewScan) {
  mTrace("scan detail:%" PRId32 ", perform update action, old row:%p new row:%p", pOldScan->scanId, pOldScan, pNewScan);

  pOldScan->numberFileset = pNewScan->numberFileset;
  pOldScan->finished = pNewScan->finished;

  return 0;
}

int32_t mndAddScanDetailToTran(SMnode *pMnode, STrans *pTrans, SScanObj *pScan, SVgObj *pVgroup, SVnodeGid *pVgid,
                               int32_t index) {
  int32_t        code = 0;
  SScanDetailObj scanDetail = {0};
  scanDetail.scanDetailId = index;
  scanDetail.scanId = pScan->scanId;
  scanDetail.vgId = pVgroup->vgId;
  scanDetail.dnodeId = pVgid->dnodeId;
  scanDetail.startTime = taosGetTimestampMs();
  scanDetail.numberFileset = -1;
  scanDetail.finished = -1;
  scanDetail.newNumberFileset = -1;
  scanDetail.newFinished = -1;

  mInfo("scan:%d, add scan detail to trans, index:%d, vgId:%d, dnodeId:%d", scanDetail.scanId, scanDetail.scanDetailId,
        scanDetail.vgId, scanDetail.dnodeId);

  SSdbRaw *pVgRaw = mndScanDetailActionEncode(&scanDetail);
  if (pVgRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pVgRaw) != 0) {
    sdbFreeRaw(pVgRaw);
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);

  TAOS_RETURN(code);
}

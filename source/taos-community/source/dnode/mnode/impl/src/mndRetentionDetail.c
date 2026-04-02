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
#include "mndRetentionDetail.h"
#include "mndCompactDetail.h"
#include "mndDb.h"
#include "mndShow.h"
#include "mndTrans.h"

#define MND_RETENTION_DETAIL_VER_NUMBER 1

static int32_t mndRetrieveRetentionDetail(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveRetentionDetail(SMnode *pMnode, void *pIter);

int32_t mndInitRetentionDetail(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_RETENTION_DETAIL, mndRetrieveRetentionDetail);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_RETENTION_DETAIL, mndCancelRetrieveRetentionDetail);

  SSdbTable table = {
      .sdbType = SDB_RETENTION_DETAIL,
      .keyType = SDB_KEY_INT64,
      .encodeFp = (SdbEncodeFp)mndRetentionDetailActionEncode,
      .decodeFp = (SdbDecodeFp)mndRetentionDetailActionDecode,
      .insertFp = (SdbInsertFp)mndRetentionDetailActionInsert,
      .updateFp = (SdbUpdateFp)mndRetentionDetailActionUpdate,
      .deleteFp = (SdbDeleteFp)mndRetentionDetailActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupRetentionDetail(SMnode *pMnode) { mDebug("mnd retention detail cleanup"); }

int32_t mndRetrieveRetentionDetail(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode              *pMnode = pReq->info.node;
  SSdb                *pSdb = pMnode->pSdb;
  int32_t              numOfRows = 0;
  int32_t              code = 0, lino = 0;
  SRetentionDetailObj *pDetail = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_RETENTION_DETAIL, pShow->pIter, (void **)&pDetail);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo = NULL;
    int32_t          cols = 0;

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pDetail->id, false, pDetail, pShow->pIter, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pDetail->vgId, false, pDetail, pShow->pIter, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pDetail->dnodeId, false, pDetail, pShow->pIter, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pDetail->numberFileset, false, pDetail, pShow->pIter, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pDetail->finished, false, pDetail, pShow->pIter, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pDetail->startTime, false, pDetail, pShow->pIter, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pDetail->progress, false, pDetail, pShow->pIter, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pDetail->remainingTime, false, pDetail, pShow->pIter, _exit);

    numOfRows++;
    sdbRelease(pSdb, pDetail);
  }
_exit:
  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelRetrieveRetentionDetail(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_RETENTION_DETAIL);
}

void tFreeRetentionDetailObj(SRetentionDetailObj *pObj) {}

SSdbRaw *mndRetentionDetailActionEncode(SRetentionDetailObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSCompactDetailObj(NULL, 0, pObj);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_RETENTION_DETAIL, MND_RETENTION_DETAIL_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSCompactDetailObj(buf, tlen, pObj);
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
    mError("retention detail:%" PRId32 ", failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("retention detail:%" PRId32 ", encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

SSdbRow *mndRetentionDetailActionDecode(SSdbRaw *pRaw) {
  int32_t              code = 0;
  int32_t              lino = 0;
  SSdbRow             *pRow = NULL;
  SRetentionDetailObj *pObj = NULL;
  void                *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_RETENTION_DETAIL_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("retention detail read invalid ver, data ver: %d, curr ver: %d", sver, MND_RETENTION_DETAIL_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SRetentionDetailObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) {
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

  if ((terrno = tDeserializeSCompactDetailObj(buf, tlen, pObj)) < 0) {
    goto OVER;
  }

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("retention detail:%" PRId32 ", failed to decode from raw:%p since %s", pObj->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("retention detail:%" PRId32 ", decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

int32_t mndRetentionDetailActionInsert(SSdb *pSdb, SRetentionDetailObj *pObj) {
  mTrace("retention detail:%" PRId32 ", perform insert action", pObj->id);
  return 0;
}

int32_t mndRetentionDetailActionDelete(SSdb *pSdb, SRetentionDetailObj *pObj) {
  mTrace("retention detail:%" PRId32 ", perform insert action", pObj->id);
  tFreeRetentionDetailObj(pObj);
  return 0;
}

int32_t mndRetentionDetailActionUpdate(SSdb *pSdb, SRetentionDetailObj *pOldObj, SRetentionDetailObj *pNewObj) {
  mTrace("retention detail:%" PRId32 ", perform update action, old row:%p new row:%p", pOldObj->id, pOldObj, pNewObj);

  pOldObj->numberFileset = pNewObj->numberFileset;
  pOldObj->finished = pNewObj->finished;

  return 0;
}

int32_t mndAddRetentionDetailToTrans(SMnode *pMnode, STrans *pTrans, SRetentionObj *pObj, SVgObj *pVgroup,
                                     SVnodeGid *pVgid, int32_t index) {
  int32_t             code = 0;
  SRetentionDetailObj detail = {0};
  detail.detailId = index;
  detail.id = pObj->id;
  detail.vgId = pVgroup->vgId;
  detail.dnodeId = pVgid->dnodeId;
  detail.startTime = taosGetTimestampMs();
  detail.numberFileset = -1;
  detail.finished = -1;
  detail.newNumberFileset = -1;
  detail.newFinished = -1;

  mInfo("retention:%d, add retention detail to trans, index:%d, vgId:%d, dnodeId:%d", detail.id, detail.detailId,
        detail.vgId, detail.dnodeId);

  SSdbRaw *pVgRaw = mndRetentionDetailActionEncode(&detail);
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
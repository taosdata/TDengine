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
#include "mndRetentionDetail.h"
#include "mndDb.h"
#include "mndShow.h"
#include "mndTrans.h"

#define MND_RETENTION_DETAIL_VER_NUMBER 1

int32_t mndInitRetentionDetail(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_RETENTION_DETAIL, mndRetrieveRetentionDetail);

  SSdbTable table = {
      .sdbType = SDB_RETENTION_DETAIL,
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

int32_t mndRetrieveRetentionDetail(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode            *pMnode = pReq->info.node;
  SSdb              *pSdb = pMnode->pSdb;
  int32_t            numOfRows = 0;
  SCompactDetailObj *pCompactDetail = NULL;
  char              *sep = NULL;
  SDbObj            *pDb = NULL;

  mInfo("retrieve compact detail");

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
    pShow->pIter = sdbFetch(pSdb, SDB_RETENTION_DETAIL, pShow->pIter, (void **)&pCompactDetail);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->compactId, false),
                                   pSdb, pCompactDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->vgId, false), pSdb,
                                   pCompactDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->dnodeId, false),
                                   pSdb, pCompactDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(
        colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->numberFileset, false), pSdb, pCompactDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->finished, false),
                                   pSdb, pCompactDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->startTime, false),
                                   pSdb, pCompactDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->progress, false),
                                   pSdb, pCompactDetail);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(
        colDataSetVal(pColInfo, numOfRows, (const char *)&pCompactDetail->remainingTime, false), pSdb, pCompactDetail);

    numOfRows++;
    sdbRelease(pSdb, pCompactDetail);
  }

  pShow->numOfRows += numOfRows;
  mndReleaseDb(pMnode, pDb);
  return numOfRows;
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
  code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);

  TAOS_RETURN(code);
}

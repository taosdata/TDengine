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
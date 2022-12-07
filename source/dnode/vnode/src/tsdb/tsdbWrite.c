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

#include "tsdb.h"

/**
 * @brief max key by precision
 *  approximately calculation:
 *  ms: 3600*1000*8765*1000         // 1970 + 1000 years
 *  us: 3600*1000000*8765*1000      // 1970 + 1000 years
 *  ns: 3600*1000000000*8765*292    // 1970 + 292 years
 */
static int64_t tsMaxKeyByPrecision[] = {31556995200000L, 31556995200000000L, 9214646400000000000L};

// static int tsdbScanAndConvertSubmitMsg(STsdb *pTsdb, SSubmitReq *pMsg);

int tsdbInsertData(STsdb *pTsdb, int64_t version, SSubmitReq *pMsg, SSubmitRsp *pRsp) {
  SSubmitMsgIter msgIter = {0};
  SSubmitBlk    *pBlock = NULL;
  int32_t        affectedrows = 0;
  int32_t        numOfRows = 0;

  ASSERT(pTsdb->mem != NULL);

  // scan and convert
  if (tsdbScanAndConvertSubmitMsg(pTsdb, pMsg) < 0) {
    if (terrno != TSDB_CODE_TDB_TABLE_RECONFIGURE) {
      tsdbError("vgId:%d, failed to insert data since %s", TD_VID(pTsdb->pVnode), tstrerror(terrno));
    }
    return -1;
  }

  // loop to insert
  if (tInitSubmitMsgIter(pMsg, &msgIter) < 0) {
    return -1;
  }
  while (true) {
    SSubmitBlkRsp r = {0};
    tGetSubmitMsgNext(&msgIter, &pBlock);
    if (pBlock == NULL) break;
    if ((terrno = tsdbInsertTableData(pTsdb, version, &msgIter, pBlock, &r)) < 0) {
      return -1;
    }

    numOfRows += msgIter.numOfRows;
  }

  if (pRsp != NULL) {
    // pRsp->affectedRows = affectedrows;
    // pRsp->numOfRows = numOfRows;
  }

  return 0;
}

#if 0
static FORCE_INLINE int tsdbCheckRowRange(STsdb *pTsdb, STable *pTable, STSRow *row, TSKEY minKey, TSKEY maxKey,
                                          TSKEY now) {
  TSKEY rowKey = TD_ROW_KEY(row);
  if (rowKey < minKey || rowKey > maxKey) {
    tsdbError("vgId:%d, table %s tid %d uid %" PRIu64 " timestamp is out of range! now %" PRId64 " minKey %" PRId64
              " maxKey %" PRId64 " row key %" PRId64,
              REPO_ID(pTsdb), TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TABLE_UID(pTable), now, minKey, maxKey,
              rowKey);
    terrno = TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE;
    return -1;
  }

  return 0;
}
#endif

static FORCE_INLINE int tsdbCheckRowRange(STsdb *pTsdb, tb_uid_t uid, STSRow *row, TSKEY minKey, TSKEY maxKey,
                                          TSKEY now) {
  TSKEY rowKey = TD_ROW_KEY(row);
  if (rowKey < minKey || rowKey > maxKey) {
    tsdbError("vgId:%d, table uid %" PRIu64 " timestamp is out of range! now %" PRId64 " minKey %" PRId64
              " maxKey %" PRId64 " row key %" PRId64,
              TD_VID(pTsdb->pVnode), uid, now, minKey, maxKey, rowKey);
    terrno = TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE;
    return -1;
  }

  return 0;
}

int tsdbScanAndConvertSubmitMsg(STsdb *pTsdb, SSubmitReq *pMsg) {
  ASSERT(pMsg != NULL);
  // STsdbMeta *    pMeta = pTsdb->tsdbMeta;
  SSubmitMsgIter msgIter = {0};
  SSubmitBlk    *pBlock = NULL;
  SSubmitBlkIter blkIter = {0};
  STSRow        *row = NULL;
  STsdbKeepCfg  *pCfg = &pTsdb->keepCfg;
  TSKEY          now = taosGetTimestamp(pCfg->precision);
  TSKEY          minKey = now - tsTickPerMin[pCfg->precision] * pCfg->keep2;
  TSKEY          maxKey = tsMaxKeyByPrecision[pCfg->precision];

  terrno = TSDB_CODE_SUCCESS;
  // pMsg->length = htonl(pMsg->length);
  // pMsg->numOfBlocks = htonl(pMsg->numOfBlocks);

  if (tInitSubmitMsgIter(pMsg, &msgIter) < 0) return -1;
  while (true) {
    if (tGetSubmitMsgNext(&msgIter, &pBlock) < 0) return -1;
    if (pBlock == NULL) break;

      // pBlock->uid = htobe64(pBlock->uid);
      // pBlock->suid = htobe64(pBlock->suid);
      // pBlock->sversion = htonl(pBlock->sversion);
      // pBlock->dataLen = htonl(pBlock->dataLen);
      // pBlock->schemaLen = htonl(pBlock->schemaLen);
      // pBlock->numOfRows = htonl(pBlock->numOfRows);

#if 0
    if (pBlock->tid <= 0 || pBlock->tid >= pMeta->maxTables) {
      tsdbError("vgId:%d, failed to get table to insert data, uid %" PRIu64 " tid %d", REPO_ID(pTsdb), pBlock->uid,
                pBlock->tid);
      terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
      return -1;
    }

    STable *pTable = pMeta->tables[pBlock->tid];
    if (pTable == NULL || TABLE_UID(pTable) != pBlock->uid) {
      tsdbError("vgId:%d, failed to get table to insert data, uid %" PRIu64 " tid %d", REPO_ID(pTsdb), pBlock->uid,
                pBlock->tid);
      terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
      return -1;
    }

    if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
      tsdbError("vgId:%d, invalid action trying to insert a super table %s", REPO_ID(pTsdb), TABLE_CHAR_NAME(pTable));
      terrno = TSDB_CODE_TDB_INVALID_ACTION;
      return -1;
    }

    // Check schema version and update schema if needed
    if (tsdbCheckTableSchema(pTsdb, pBlock, pTable) < 0) {
      if (terrno == TSDB_CODE_TDB_TABLE_RECONFIGURE) {
        continue;
      } else {
        return -1;
      }
    }
#endif
    tInitSubmitBlkIter(&msgIter, pBlock, &blkIter);
    while ((row = tGetSubmitBlkNext(&blkIter)) != NULL) {
      if (tsdbCheckRowRange(pTsdb, msgIter.uid, row, minKey, maxKey, now) < 0) {
        return -1;
      }
    }
  }

  if (terrno != TSDB_CODE_SUCCESS) return -1;
  return 0;
}
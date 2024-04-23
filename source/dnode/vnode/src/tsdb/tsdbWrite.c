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

#include "cos.h"
#include "tsdb.h"

/**
 * @brief max key by precision
 *  approximately calculation:
 *  ms: 3600*1000*8765*1000         // 1970 + 1000 years
 *  us: 3600*1000000*8765*1000      // 1970 + 1000 years
 *  ns: 3600*1000000000*8765*292    // 1970 + 292 years
 */
int64_t tsMaxKeyByPrecision[] = {31556995200000L, 31556995200000000L, 9214646400000000000L};

// static int tsdbScanAndConvertSubmitMsg(STsdb *pTsdb, SSubmitReq *pMsg);

int tsdbInsertData(STsdb *pTsdb, int64_t version, SSubmitReq2 *pMsg, SSubmitRsp2 *pRsp) {
  int32_t arrSize = 0;
  int32_t affectedrows = 0;
  int32_t numOfRows = 0;

  if (ASSERTS(pTsdb->mem != NULL, "vgId:%d, mem is NULL", TD_VID(pTsdb->pVnode))) {
    return -1;
  }

  arrSize = taosArrayGetSize(pMsg->aSubmitTbData);

  // scan and convert
  if ((terrno = tsdbScanAndConvertSubmitMsg(pTsdb, pMsg)) < 0) {
    if (terrno != TSDB_CODE_TDB_TABLE_RECONFIGURE) {
      tsdbError("vgId:%d, failed to insert data since %s", TD_VID(pTsdb->pVnode), tstrerror(terrno));
    }
    return -1;
  }

  // loop to insert
  for (int32_t i = 0; i < arrSize; ++i) {
    if ((terrno = tsdbInsertTableData(pTsdb, version, taosArrayGet(pMsg->aSubmitTbData, i), &affectedrows)) < 0) {
      return -1;
    }
  }

  if (pRsp != NULL) {
    // pRsp->affectedRows = affectedrows;
    // pRsp->numOfRows = numOfRows;
  }

  return 0;
}

static FORCE_INLINE int tsdbCheckRowRange(STsdb *pTsdb, tb_uid_t uid, TSKEY rowKey, TSKEY minKey, TSKEY maxKey,
                                          TSKEY now) {
  if (rowKey < minKey || rowKey > maxKey) {
    tsdbError("vgId:%d, table uid %" PRIu64 " timestamp is out of range! now %" PRId64 " minKey %" PRId64
              " maxKey %" PRId64 " row key %" PRId64,
              TD_VID(pTsdb->pVnode), uid, now, minKey, maxKey, rowKey);
    return TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE;
  }

  return 0;
}

int tsdbScanAndConvertSubmitMsg(STsdb *pTsdb, SSubmitReq2 *pMsg) {
  int32_t       code = 0;
  STsdbKeepCfg *pCfg = &pTsdb->keepCfg;
  TSKEY         now = taosGetTimestamp(pCfg->precision);
  TSKEY         minKey = now - tsTickPerMin[pCfg->precision] * pCfg->keep2;
  TSKEY         maxKey = tsMaxKeyByPrecision[pCfg->precision];
  int32_t       size = taosArrayGetSize(pMsg->aSubmitTbData);
  /*
  int32_t nlevel = tfsGetLevel(pTsdb->pVnode->pTfs);
  if (nlevel > 1 && tsS3Enabled) {
    if (nlevel == 3) {
      minKey = now - tsTickPerMin[pCfg->precision] * pCfg->keep1;
    } else if (nlevel == 2) {
      minKey = now - tsTickPerMin[pCfg->precision] * pCfg->keep0;
    }
  }
  */
  for (int32_t i = 0; i < size; ++i) {
    SSubmitTbData *pData = TARRAY_GET_ELEM(pMsg->aSubmitTbData, i);
    if (pData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      uint64_t  nColData = TARRAY_SIZE(pData->aCol);
      SColData *aColData = (SColData *)TARRAY_DATA(pData->aCol);
      if (nColData > 0) {
        int32_t nRows = aColData[0].nVal;
        TSKEY  *aKey = (TSKEY *)aColData[0].pData;
        for (int32_t r = 0; r < nRows; ++r) {
          if ((code = tsdbCheckRowRange(pTsdb, pData->uid, aKey[r], minKey, maxKey, now)) < 0) {
            goto _exit;
          }
        }
      }
    } else {
      int32_t nRows = taosArrayGetSize(pData->aRowP);
      for (int32_t r = 0; r < nRows; ++r) {
        SRow *pRow = (SRow *)taosArrayGetP(pData->aRowP, r);
        if ((code = tsdbCheckRowRange(pTsdb, pData->uid, pRow->ts, minKey, maxKey, now)) < 0) {
          goto _exit;
        }
      }
    }
  }

_exit:
  return code;
}

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

#include "decimal.h"
#include "meta.h"
#include "tsdb.h"
#include "tsdbInt.h"
#include "vnd.h"

extern bool    tsdbRowIsDeleted(SCompactor2 *compactor, TSDBROW *row);
extern bool    tsdbRowIsExpired(SCompactor2 *compactor, TSDBROW *row, int64_t keep, int64_t expireTs);
extern int32_t tsdbCompactFSetGetKeep(SCompactor2 *compactor, int64_t suid, int64_t *keep);
extern int32_t tsdbCompactFSetTableDataEnd(SCompactor2 *compactor);
extern int32_t tsdbCompactFSetTableDataBegin(SCompactor2 *compactor, const TABLEID *tbid);
extern bool    tsdbShouldCompact(STFileSet *fset, int32_t vgId, int32_t expLevel, ETsdbOpType type);
extern int32_t tsdbDoCompact(SCompactor2 *compactor);

extern int32_t tdRollupCtxInit(SRollupCtx *pCtx, SRSchema *pRSchema, int8_t precision, const char *dbName);
extern void    tdRollupCtxCleanup(SRollupCtx *pCtx, bool deep);
extern int32_t tdRollupCtxReset(SRollupCtx *pCtx);
extern int32_t tdRollupDoAggregate(SRollupCtx *pCtx);
extern int32_t tdRollupFinalize(SRollupCtx *pCtx);

static bool tsdbRollupCheck(SRSchema *pRSchema, int32_t expLevel, int64_t duration) {
  if (pRSchema->funcIds && (expLevel == 1 || expLevel == 2)) {
    int64_t interval = pRSchema->interval[expLevel - 1];
    if ((interval > 0) && ((duration % interval) == 0)) {  // strict check when execute rollup
      return true;
    }
  }
  return false;
}

static void tsdbRollupSetAggWindow(int64_t interval, int64_t ts, int64_t *itvStart, int64_t *itvEnd) {
  *itvStart = (ts >= 0) ? (ts / interval) * interval : ((ts - interval + 1) / interval) * interval;
  *itvEnd = *itvStart + interval - 1;
}

static bool tdRollupCheckUpdate(int32_t nRows, STsdbRowKey *lastRowKey, STsdbRowKey *curRowKey) {
  return (nRows > 0) && (tRowKeyCompare(&lastRowKey->key, &curRowKey->key) == 0);
}

int32_t tdRollupStashRow(SRollupCtx *pCtx, SBlockData *pBlockData, SRowInfo *row, bool update) {
  int32_t code = 0, lino = 0;

  if (row->row.type != TSDBROW_COL_FMT) {  // only col format is needed currently
    TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);
  }

  if (update) {
    TAOS_CHECK_EXIT(tBlockDataUpdateRow(pBlockData, &row->row, NULL));
  } else {
    TAOS_CHECK_EXIT(tBlockDataAppendRow(pBlockData, &row->row, NULL, row->uid));
  }
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(((STsdb *)pCtx->pTsdb)->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tdRollupFetchData(SRollupCtx *pCtx, SBlockData *pBlockDataFrom) {
  int32_t          code = 0, lino = 0;
  SColumnInfoData *colInfo = NULL;
  SColVal          cv = {0};

  int32_t   iColDataFrom = 0;
  SColData *pColDataFrom = (iColDataFrom < pBlockDataFrom->nColData) ? &pBlockDataFrom->aColData[iColDataFrom] : NULL;

  SSDataBlock *pInputData = pCtx->pInputBlock;
  int32_t      nColDataTo = taosArrayGetSize(pInputData->pDataBlock);

  if ((nColDataTo > 1) && (colInfo = TARRAY_GET_ELEM(pInputData->pDataBlock, 0)) &&
      (colInfo->info.colId == PRIMARYKEY_TIMESTAMP_COL_ID)) {
    for (int32_t r = 0; r < pBlockDataFrom->nRow; r++) {
      colDataSetVal(colInfo, r, (const char *)&pBlockDataFrom->aTSKEY[r], false);
    }
  } else {
    TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
  }
  for (int32_t c = 1; c < nColDataTo; ++c) {
    colInfo = TARRAY_GET_ELEM(pInputData->pDataBlock, c);
    while (pColDataFrom && pColDataFrom->cid < colInfo->info.colId) {
      pColDataFrom = (++iColDataFrom < pBlockDataFrom->nColData) ? &pBlockDataFrom->aColData[iColDataFrom] : NULL;
    }

    if (pColDataFrom && (pColDataFrom->cid == colInfo->info.colId)) {
      for (int32_t r = 0; r < pBlockDataFrom->nRow; r++) {
        TAOS_CHECK_EXIT(tColDataGetValue(pColDataFrom, r, &cv));
        if (COL_VAL_IS_VALUE(&cv)) {
          if (IS_VAR_DATA_TYPE(colInfo->info.type)) {
            STR_WITH_SIZE_TO_VARSTR(pCtx->pBuf, cv.value.pData, cv.value.nData);
            TAOS_CHECK_EXIT(colDataSetVal(colInfo, r, (const char *)pCtx->pBuf, false));
          } else if (colInfo->info.type == TSDB_DATA_TYPE_DECIMAL) {
            TAOS_CHECK_EXIT(colDataSetVal(colInfo, r, (const char *)cv.value.pData, false));
          } else {
            TAOS_CHECK_EXIT(colDataSetVal(colInfo, r, (const char *)&cv.value.val, false));
          }
        } else {
          TAOS_CHECK_EXIT(colDataSetVal(colInfo, r, NULL, true));
        }
      }
    } else {
      colDataSetNItemsNull(colInfo, 0, pBlockDataFrom->nRow);
    }

    pInputData->info.rows = pBlockDataFrom->nRow;  // TODO: += ?

    pColDataFrom = (++iColDataFrom < pBlockDataFrom->nColData) ? &pBlockDataFrom->aColData[iColDataFrom] : NULL;
  }
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(((STsdb *)pCtx->pTsdb)->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tdRollupGenerateAggRow(SRollupCtx *pCtx, SRSchema *pRSchema, SRowInfo *aggRow) {
  int32_t      code = 0, lino = 0;
  STSchema    *pTSchema = pRSchema->tSchema;
  SSDataBlock *pResBlock = pCtx->pResBlock;
  SArray      *colValArray = pCtx->pColValArr;
  int32_t      nCols = taosArrayGetSize(pResBlock->pDataBlock);

  if (pResBlock->info.rows != 1) {
    tsdbError("vgId:%d %s unexpected rows:%" PRId64 " in res block, suid:%" PRId64 ", uid:%" PRId64,
              TD_VID(((STsdb *)pCtx->pTsdb)->pVnode), __func__, pResBlock->info.rows, aggRow->suid, aggRow->uid);
    TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
  } else {
    tsdbTrace("vgId:%d %s res block has 1 row with %d cols, suid:%" PRId64 ", uid:%" PRId64,
              TD_VID(((STsdb *)pCtx->pTsdb)->pVnode), __func__, nCols, aggRow->suid, aggRow->uid);
  }

  if (pTSchema->numOfCols != nCols + 1) {  // skip the primary timestamp column
    TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
  }

  taosArrayClear(pCtx->pColValArr);
  tRowDestroy(aggRow->row.pTSRow);
  aggRow->row.pTSRow = NULL;

  STColumn *pCols = &pTSchema->columns[1];
  SColVal   primaryTS = {.cid = PRIMARYKEY_TIMESTAMP_COL_ID,
                         .flag = CV_FLAG_VALUE,
                         .value = {.type = TSDB_DATA_TYPE_TIMESTAMP, .val = pCtx->winStartTs}};

  if (taosArrayPush(colValArray, &primaryTS) == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  for (int32_t i = 0; i < nCols; ++i) {
    SColumnInfoData *pColData = TARRAY_GET_ELEM(pResBlock->pDataBlock, i);
    if (!pColData) {
      TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
    }
    SColumnInfo *pColInfo = &pColData->info;
    STColumn    *pCol = pCols + i;

    if (pColData->info.colId != pCol->colId) {
      TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
    }

    bool    isNull = colDataIsNull_s(pColData, 0);
    SColVal cv = {0};
    if (isNull) {
      cv = COL_VAL_NULL(pCol->colId, pCol->type);
    } else {
      SValue val = {.type = pCol->type};
      char  *p = colDataGetData(pColData, 0);
      if (IS_VAR_DATA_TYPE(pColData->info.type)) {
        val.pData = varDataVal(p);
        val.nData = varDataLen(p);
      } else {
        if (val.type == pColData->info.type) {
          if (val.type != TSDB_DATA_TYPE_DECIMAL) {
            memcpy(&val.val, p, pColData->info.bytes);
          } else {
            val.pData = p;
            val.nData = pColData->info.bytes;
          }
        } else {
          /**
           *  1. sum/avg would convert to int64_t/uint64_t/double during aggregation
           *  2. below conversion may lead to overflow or loss, the designer'd select the proper data type.
           */
          char tv[DATUM_MAX_SIZE] = {0};
          if (IS_SIGNED_NUMERIC_TYPE(pColInfo->type)) {
            int64_t v = 0;
            GET_TYPED_DATA(v, int64_t, pColInfo->type, p, typeGetTypeModFromColInfo(pColInfo));
            SET_TYPED_DATA(&tv, val.type, v);
          } else if (IS_UNSIGNED_NUMERIC_TYPE(pColInfo->type)) {
            uint64_t v = 0;
            GET_TYPED_DATA(v, uint64_t, pColInfo->type, p, typeGetTypeModFromColInfo(pColInfo));
            SET_TYPED_DATA(&tv, val.type, v);
          } else if (pColInfo->type == TSDB_DATA_TYPE_DOUBLE) {
            double v = 0;
            GET_TYPED_DATA(v, double, pColInfo->type, p, typeGetTypeModFromColInfo(pColInfo));
            SET_TYPED_DATA(&tv, val.type, v);
          } else if (pColInfo->type == TSDB_DATA_TYPE_FLOAT) {
            float v = 0;
            GET_TYPED_DATA(v, float, pColInfo->type, p, typeGetTypeModFromColInfo(pColInfo));
            SET_TYPED_DATA(&tv, val.type, v);
          } else if (pColInfo->type == TSDB_DATA_TYPE_DECIMAL) {
            TEST_decimal64FromDecimal128((const Decimal128 *)p, pColInfo->precision, pColInfo->scale, (Decimal64 *)tv,
                                         pColInfo->precision, pColInfo->scale);
          } else {
            TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);
          }
          valueSetDatum(&val, val.type, tv, tDataTypes[val.type].bytes);
        }
      }
      cv = COL_VAL_VALUE(pCol->colId, val);
    }
    if (taosArrayPush(colValArray, &cv) == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SRowBuildScanInfo info = {0};
  TAOS_CHECK_EXIT(tRowBuild(colValArray, pTSchema, &aggRow->row.pTSRow, &info));

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(((STsdb *)pCtx->pTsdb)->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tdRollupCalcStashRows(SRollupCtx *pCtx, SCompactor2 *compactor, SBlockData *stashBlock,
                                     SRSchema *pRSchema, int64_t *nWrittenAggRows, SRowInfo *aggRow) {
  int32_t code = 0, lino = 0;
  if (stashBlock->nRow > 0) {
    TAOS_CHECK_EXIT(tdRollupFetchData(pCtx, stashBlock));
    TAOS_CHECK_EXIT(tdRollupDoAggregate(pCtx));
    pCtx->winTotalRows += stashBlock->nRow;
    tBlockDataClear(stashBlock);
  }
  if (pCtx->winTotalRows > 0) {
    TAOS_CHECK_EXIT(tdRollupFinalize(pCtx));
    TAOS_CHECK_EXIT(tdRollupGenerateAggRow(pCtx, pRSchema, aggRow));
    TAOS_CHECK_EXIT(tsdbFSetWriteRow(compactor->ctx->writer, aggRow));
    ++(*nWrittenAggRows);
    TAOS_CHECK_EXIT(tdRollupCtxReset(pCtx));
  }
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(compactor->tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

/**
 * Time-Series Data Rollup procedure:
 * - Calculation Timing: 1. When the ts exceeds the current aggregation window; 2. When the input data rows of a
 * sub-table end and there is data in the aggregation window.
 * - Calculation Method: Convertn SBlockData into the data structure required for builtIns operator calculation.
 * - Calculation Result Acquisition/Stash/Writing: Finalize the calculation results. Construct SBlockData specific
 * to stash aggregation results, and when the number of stashed rows accumulates to 4096 or a sub-table ends, write
 * it into the file.
 *
 * Agg func supported currently:
 *  - min, max, sum, avg, first, last
 *
 */
int32_t tsdbCompactFSetRollup(SCompactor2 *compactor) {
  int32_t     code = 0, lino = 0;
  SMetaInfo   info = {0};
  STsdb      *pTsdb = compactor->tsdb;
  SVnode     *pVnode = pTsdb->pVnode;
  SRSchema   *pRSchema = NULL;
  int64_t     nTotalRawRows = 0;
  int64_t     nSkippedRawRows = 0;
  int64_t     nWrittenRawRows = 0;
  int64_t     nWrittenAggRows = 0;
  int64_t     keep = 0;
  int64_t     now = taosGetTimestamp(pTsdb->keepCfg.precision);
  int64_t     expireTs = 0;
  int64_t     winEndTs = 0;  // current interval
  int64_t     interval = 0;
  bool        rollup = false;
  STsdbRowKey lastRowKey = {0};
  int64_t     rowTS = 0;
  int64_t     durationInPrecision = 0;
  SBlockData  stashBlock = {0};  // used to stash merged rows, the source of SRollupCtx.pInputBlock
  SRollupCtx  ctx = {.pTsdb = compactor->tsdb};
  SRowInfo    aggRow = {.row.type = TSDBROW_ROW_FMT};

  if (tsdbRetentionTaskKilled(pTsdb)) {
    tsdbInfo("vgId:%d fid:%d rollup killed during data processing", TD_VID(pVnode), compactor->fset->fid);
    TAOS_CHECK_EXIT(TSDB_CODE_TSC_QUERY_KILLED);
  }

  TAOS_CHECK_EXIT(getDuration(pTsdb->keepCfg.days, TIME_UNIT_MINUTE, &durationInPrecision, pTsdb->keepCfg.precision));
  TAOS_CHECK_EXIT(tBlockDataCreate(&stashBlock));

  for (SRowInfo *row; (row = tsdbIterMergerGetData(compactor->ctx->dataIterMerger)) != NULL;) {
    if (row->row.type != TSDBROW_COL_FMT) {
      TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
    }
    ++nTotalRawRows;
    rowTS = TSDBROW_TS(&row->row);
    if (row->suid != compactor->ctx->tbid->suid) {
      if (rollup) {
        TAOS_CHECK_EXIT(tdRollupCalcStashRows(&ctx, compactor, &stashBlock, pRSchema, &nWrittenAggRows, &aggRow));
      }
      // reset table suid and schema
      code = tsdbCompactFSetGetKeep(compactor, row->suid, &keep);
      expireTs = now - (tsTickPerMin[pTsdb->keepCfg.precision] * keep);
      TAOS_CHECK_EXIT(code);
      tFreeSRSchema(&pRSchema);
      compactor->ctx->tbid->suid = row->suid;
      if (!(pRSchema = metaGetTbTSchemaR(pVnode->pMeta, compactor->ctx->tbid->suid, -1, 1))) {
        TAOS_CHECK_EXIT(terrno);
      }
      rollup = tsdbRollupCheck(pRSchema, compactor->ctx[0].expLevel, durationInPrecision);
      if (rollup) {
        tdRollupCtxCleanup(&ctx, false);
        interval = pRSchema->interval[compactor->ctx[0].expLevel - 1];  // reset interval for new table
        TAOS_CHECK_EXIT(tdRollupCtxInit(&ctx, pRSchema, pTsdb->keepCfg.precision, pVnode->config.dbname));
      }
    } else if (!pRSchema) {
      TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);  // should not happen
    }

    if (row->uid != compactor->ctx->tbid->uid) {
      if (tsdbRetentionTaskKilled(pTsdb)) {
        tsdbInfo("vgId:%d fid:%d rollup killed during data processing", TD_VID(pVnode), compactor->fset->fid);
        TAOS_CHECK_EXIT(TSDB_CODE_TSC_QUERY_KILLED);
      }
      TAOS_CHECK_EXIT(tsdbCompactFSetTableDataEnd(compactor));

      if (metaGetInfo(pVnode->pMeta, row->uid, &info, NULL) != 0) {
        TABLEID tbid = {.suid = row->suid, .uid = row->uid};
        TAOS_CHECK_EXIT(tsdbIterMergerSkipTableData(compactor->ctx->dataIterMerger, &tbid));
        ++nSkippedRawRows;
        continue;
      }

      TAOS_CHECK_EXIT(tsdbCompactFSetTableDataBegin(compactor, (TABLEID *)row));
      if (rollup) {
        TAOS_CHECK_EXIT(tdRollupCalcStashRows(&ctx, compactor, &stashBlock, pRSchema, &nWrittenAggRows, &aggRow));
        tsdbRollupSetAggWindow(interval, rowTS, &ctx.winStartTs, &winEndTs);
        TAOS_CHECK_EXIT(tBlockDataInit(&stashBlock, (TABLEID *)row, pRSchema->tSchema, NULL, 0));
        aggRow.suid = row->suid;
        aggRow.uid = row->uid;
        aggRow.row.version = TSDBROW_VERSION(&row->row);
      }
    }

    if ((compactor->ctx->pDKey == NULL || !tsdbRowIsDeleted(compactor, &row->row)) &&
        !tsdbRowIsExpired(compactor, &row->row, keep, expireTs)) {
      if (rollup) {
        STsdbRowKey curRowKey;
        tsdbRowGetKey((TSDBROW *)&row->row, &curRowKey);
        // curRowKey.key.numOfPKs = 0;  // ignore numOfPKs when do aggregation
        if (rowTS > winEndTs) {
          TAOS_CHECK_EXIT(tdRollupCalcStashRows(&ctx, compactor, &stashBlock, pRSchema, &nWrittenAggRows, &aggRow));
          TAOS_CHECK_EXIT(tdRollupStashRow(&ctx, &stashBlock, row, false));
          tsdbRollupSetAggWindow(interval, rowTS, &ctx.winStartTs, &winEndTs);  // move to next interval
          aggRow.row.version = TSDBROW_VERSION(&row->row);
        } else {
          bool update = tdRollupCheckUpdate(stashBlock.nRow, &lastRowKey, &curRowKey);
          if (update) {
            TAOS_CHECK_EXIT(tdRollupStashRow(&ctx, &stashBlock, row, true));
          } else {
            // when the number of stashed rows accumulates to maxBufRows, do aggregate
            if (stashBlock.nRow >= ctx.maxBufRows) {
              if (tsdbRetentionTaskKilled(pTsdb)) {
                tsdbInfo("vgId:%d fid:%d rollup killed during data processing", TD_VID(pVnode), compactor->fset->fid);
                TAOS_CHECK_EXIT(TSDB_CODE_TSC_QUERY_KILLED);
              }
              TAOS_CHECK_EXIT(tdRollupFetchData(&ctx, &stashBlock));
              TAOS_CHECK_EXIT(tdRollupDoAggregate(&ctx));
              ctx.winTotalRows += stashBlock.nRow;
              tBlockDataClear(&stashBlock);
            }
            TAOS_CHECK_EXIT(tdRollupStashRow(&ctx, &stashBlock, row, false));
          }
        }
        lastRowKey = curRowKey;
      } else {
        TAOS_CHECK_EXIT(tsdbFSetWriteRow(compactor->ctx->writer, row));
        ++nWrittenRawRows;  // not accurate currently since merge may skip some rows
      }
    } else {
      ++nSkippedRawRows;
    }
    TAOS_CHECK_EXIT(tsdbIterMergerNext(compactor->ctx->dataIterMerger));
  }

  if (rollup) {
    TAOS_CHECK_EXIT(tdRollupCalcStashRows(&ctx, compactor, &stashBlock, pRSchema, &nWrittenAggRows, &aggRow));
  }
_exit:
  tFreeSRSchema(&pRSchema);
  tBlockDataDestroy(&stashBlock);
  tdRollupCtxCleanup(&ctx, true);
  if (aggRow.row.pTSRow) {
    tRowDestroy(aggRow.row.pTSRow);
    aggRow.row.pTSRow = NULL;
  }
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d fid:%d rollup %" PRId64 " rows, skip %" PRId64 " rows, write %" PRId64 " raw rows/%" PRId64
             " agg rows",
             TD_VID(pVnode), compactor->fset->fid, nTotalRawRows, nSkippedRawRows, nWrittenRawRows, nWrittenAggRows);
  }
  return code;
}

static int32_t tsdbRollup(SRTNer *rtner, void *arg) {
  int32_t code = 0, lino = 0;
  bool    rollup = false;

  SCompactArg *compactArg = (SCompactArg *)arg;
  STsdb       *tsdb = compactArg->tsdb;
  SCompactor2  compactor = {
       .tsdb = tsdb,
       .szPage = tsdb->pVnode->config.tsdbPageSize,
       .minRow = tsdb->pVnode->config.tsdbCfg.minRows,
       .maxRow = tsdb->pVnode->config.tsdbCfg.maxRows,
       .cmprAlg = tsdb->pVnode->config.tsdbCfg.compression,
       .optrType = compactArg->type,
       .cid = tsdbFSAllocEid(tsdb->pFS),
       .compactVersion = INT64_MAX,
       .fset = rtner->fset,
  };

  // do compact
  if (compactor.fset) {
    compactor.ctx->expLevel = tsdbFidLevel(compactor.fset->fid, &compactor.tsdb->keepCfg, taosGetTimestampSec());
    if (tsdbShouldCompact(compactor.fset, TD_VID(tsdb->pVnode), compactor.ctx->expLevel, compactArg->type)) {
      code = tsdbDoCompact(&compactor);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbDoRollup(SRTNer *rtner) {
  int32_t      code = 0, lino = 0;
  int32_t      vid = TD_VID(rtner->tsdb->pVnode);
  SVnodeCfg   *pCfg = &rtner->tsdb->pVnode->config;
  STFileSet   *fset = rtner->fset;
  SCompactArg *arg = NULL;

  tsdbInfo("vgId:%d, fid:%d, rollup started", vid, fset->fid);

  STimeWindow win = {0};
  tsdbFidKeyRange(fset->fid, rtner->tsdb->keepCfg.days, rtner->tsdb->keepCfg.precision, &win.skey, &win.ekey);

  if (!(arg = taosMemoryCalloc(1, sizeof(*arg)))) {
    TAOS_CHECK_EXIT(terrno);
  }
  arg->tsdb = rtner->tsdb;
  arg->fid = fset->fid;
  arg->type = TSDB_OPTR_ROLLUP;

  TAOS_CHECK_EXIT(tsdbRollup(rtner, arg));
_exit:
  if (code != 0) {
    tsdbError("vgId:%d, fid:%d, failed at line %d rollup %s", vid, fset->fid, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d, fid:%d, rollup finished", vid, fset->fid);
  }
  taosMemFreeClear(arg);
  TAOS_RETURN(code);
}

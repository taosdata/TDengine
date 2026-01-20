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

#include "executor.h"
#include "query.h"
#include "tarray.h"
#include "tdef.h"
#include "tmsg.h"
#include "ttypes.h"

#include "executorInt.h"
#include "tcommon.h"
#include "ttime.h"

#include "executorInt.h"
#include "function.h"
#include "querynodes.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tfill.h"

#define FILL_IS_ASC_FILL(_f) ((_f)->order == TSDB_ORDER_ASC)
#define DO_INTERPOLATION(_v1, _v2, _k1, _k2, _k) \
  ((_v1) + ((_v2) - (_v1)) * (((double)(_k)) - ((double)(_k1))) / (((double)(_k2)) - ((double)(_k1))))

static int32_t doSetVal(SColumnInfoData* pDstColInfoData, int64_t rowIndex,
                        const SGroupKeys* pKey);
static int32_t doSetUserSpecifiedValue(SColumnInfoData* pDst, SVariant* pVar,
                                       int64_t rowIndex);

/**
  @brief Get the specified row's timestamp from the block.
*/
static TSKEY getBlockCurTs(const SSDataBlock* pBlock, const int64_t rowIdx,
                           const int32_t tsSlotId) {
  if (pBlock) {
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, tsSlotId);
    return ((TSKEY*)pTsCol->pData)[rowIdx];
  }
  return -1;
}

/**
  @param fillFromHead Whether current situation is filling from head.
*/
static bool setNotFillColumn(SFillInfo* pFillInfo,
                             SSDataBlock* pFillBlock, int64_t rowIndex,
                             int32_t colIdx, bool fillFromHead) {
  const SFillColInfo* pCol = &pFillInfo->pFillCol[colIdx];
  SColumnInfoData* pDstColInfo = taosArrayGet(pFillBlock->pDataBlock,
                                              GET_DEST_SLOT_ID(pCol));
  if (pCol->fillNull) {
    colDataSetNULL(pDstColInfo, (uint32_t)rowIndex);
    return false;
  }

  const SRowVal* pRowVal = NULL;
  bool           ascNext = false;
  bool           descPrev = false;
  if (pFillInfo->type == TSDB_FILL_NEXT) {
    pRowVal = FILL_IS_ASC_FILL(pFillInfo) ?
                &pFillInfo->next : &pFillInfo->prev;
    if (FILL_IS_ASC_FILL(pFillInfo)) ascNext = true;
  } else {
    pRowVal = FILL_IS_ASC_FILL(pFillInfo) ?
                &pFillInfo->prev : &pFillInfo->next;
    if (!FILL_IS_ASC_FILL(pFillInfo)) descPrev = true;
  }

  const bool* pNullValueFlag = taosArrayGet(pRowVal->pNullValueFlag, colIdx);
  if (*pNullValueFlag && pFillInfo->numOfRows > 0 && (ascNext || descPrev)) {
    return true;
  }

  TSKEY* pColValueTs = taosArrayGet(pRowVal->pValueTs, colIdx);
  const SGroupKeys* pKey = taosArrayGet(pRowVal->pRowVal, colIdx);
  TSKEY targetRowTs = 0;
  if (fillFromHead) {
    /*
      If filling falling-behind rows from head, the target row timestamp has
      already been stored in the `pFillBlock` by the previous fill actions.
    */
    const SFillColInfo* pTsCol = &pFillInfo->pFillCol[pFillInfo->tsSlotId];
    int32_t tsSlotIdInFillBlock = GET_DEST_SLOT_ID(pTsCol);
    targetRowTs = getBlockCurTs(pFillBlock, rowIndex, tsSlotIdInFillBlock);
  } else {
    targetRowTs = pFillInfo->currentKey;
  }
  /*
    Check surroundingTime:
      if the time difference between the fill reference row and target row
      exceeds surroundingTime or the fill reference row is NULL, use fillVal
      instead of using the reference row.
  */
  int64_t timeDiff = llabs(targetRowTs - *pColValueTs);
  if (pFillInfo->surroundingTime > 0 &&
      (timeDiff > pFillInfo->surroundingTime || pKey->isNull)) {
    /*
      Use fillVal to fill when time difference exceeds surroundingTime
      or the fill reference row is NULL.
    */
    SVariant* pVar = &pFillInfo->pFillCol[colIdx].fillVal;
    int32_t code = doSetUserSpecifiedValue(pDstColInfo, pVar, rowIndex);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__,
             tstrerror(code));
      T_LONG_JMP(pFillInfo->pTaskInfo->env, code);
    }
    return false;
  }

  if (!pKey) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    T_LONG_JMP(pFillInfo->pTaskInfo->env, terrno);
  }
  int32_t code = doSetVal(pDstColInfo, rowIndex, pKey);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    T_LONG_JMP(pFillInfo->pTaskInfo->env, code);
  }
  return false;
}

static void setNullCol(SSDataBlock* pFillBlock, SFillInfo* pFillInfo,
                       int64_t rowIdx, int32_t colIdx, bool fillFromHead) {
  const SFillColInfo* pCol = &pFillInfo->pFillCol[colIdx];
  SColumnInfoData* pDstColInfo = taosArrayGet(pFillBlock->pDataBlock,
                                              GET_DEST_SLOT_ID(pCol));
  if (pCol->notFillCol) {
    bool filled = fillIfWindowPseudoColumn(pFillInfo, pFillBlock,
                                           rowIdx, colIdx);
    if (!filled) {
      TAOS_UNUSED(setNotFillColumn(pFillInfo, pFillBlock, rowIdx, colIdx,
                                   fillFromHead));
    }
  } else {
    colDataSetNULL(pDstColInfo, (uint32_t)rowIdx);
  }
}

static int32_t doSetUserSpecifiedValue(SColumnInfoData* pDst, SVariant* pVar,
                                       int64_t rowIndex) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    isNull = (TSDB_DATA_TYPE_NULL == pVar->nType) ? true : false;
  if (pDst->info.type == TSDB_DATA_TYPE_FLOAT) {
    float v = 0;
    GET_TYPED_DATA(v, float, pVar->nType, &pVar->f, typeGetTypeModFromColInfo(&pDst->info));
    code = colDataSetVal(pDst, (uint32_t)rowIndex, (char*)&v, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pDst->info.type == TSDB_DATA_TYPE_DOUBLE) {
    double v = 0;
    GET_TYPED_DATA(v, double, pVar->nType, &pVar->d, typeGetTypeModFromColInfo(&pDst->info));
    code = colDataSetVal(pDst, (uint32_t)rowIndex, (char*)&v, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (IS_SIGNED_NUMERIC_TYPE(pDst->info.type) || pDst->info.type == TSDB_DATA_TYPE_BOOL) {
    int64_t v = 0;
    GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i, typeGetTypeModFromColInfo(&pDst->info));
    code = colDataSetVal(pDst, (uint32_t)rowIndex, (char*)&v, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (IS_UNSIGNED_NUMERIC_TYPE(pDst->info.type)) {
    uint64_t v = 0;
    GET_TYPED_DATA(v, uint64_t, pVar->nType, &pVar->u, typeGetTypeModFromColInfo(&pDst->info));
    code = colDataSetVal(pDst, (uint32_t)rowIndex, (char*)&v, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pDst->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
    int64_t v = 0;
    GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->u, typeGetTypeModFromColInfo(&pDst->info));
    code = colDataSetVal(pDst, (uint32_t)rowIndex, (const char*)&v, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pDst->info.type == TSDB_DATA_TYPE_NCHAR || pDst->info.type == TSDB_DATA_TYPE_VARCHAR ||
             pDst->info.type == TSDB_DATA_TYPE_VARBINARY) {
    code = colDataSetVal(pDst, (uint32_t)rowIndex, pVar->pz, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pDst->info.type == TSDB_DATA_TYPE_DECIMAL64) {
    code = colDataSetVal(pDst, (uint32_t)rowIndex, (char*)&pVar->i, isNull);
  } else if (pDst->info.type == TSDB_DATA_TYPE_DECIMAL) {
    code = colDataSetVal(pDst, (uint32_t)rowIndex, (char*)pVar->pz, isNull);
  } else {  // others data
    colDataSetNULL(pDst, (uint32_t)rowIndex);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// fill windows pseudo column, _wstart, _wend, _wduration and return true, otherwise return false
bool fillIfWindowPseudoColumn(SFillInfo* pFillInfo,
                              const SSDataBlock* pFillBlock,
                              const int64_t rowIndex, const int32_t colIdx) {
  SFillColInfo*    pCol = &pFillInfo->pFillCol[colIdx];
  SColumnInfoData* pDstColInfo = taosArrayGet(pFillBlock->pDataBlock,
                                              GET_DEST_SLOT_ID(pCol));
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (!pCol->notFillCol) {
    return false;
  }
  if (pCol->pExpr->pExpr->nodeType == QUERY_NODE_COLUMN) {
    if (pCol->pExpr->base.numOfParams != 1) {
      return false;
    }
    if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_START) {
      code = colDataSetVal(pDstColInfo, (uint32_t)rowIndex, (const char*)&pFillInfo->currentKey, false);
      QUERY_CHECK_CODE(code, lino, _end);
      return true;
    } else if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_END) {
      // TODO: include endpoint
      const SInterval* pInterval = &pFillInfo->interval;
      int64_t    windowEnd =
          taosTimeAdd(pFillInfo->currentKey, pInterval->interval, pInterval->intervalUnit, pInterval->precision, NULL);
      code = colDataSetVal(pDstColInfo, (uint32_t)rowIndex, (const char*)&windowEnd, false);
      QUERY_CHECK_CODE(code, lino, _end);
      return true;
    } else if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_DURATION) {
      // TODO: include endpoint
      code = colDataSetVal(pDstColInfo, (uint32_t)rowIndex, (const char*)&pFillInfo->interval.interval, false);
      QUERY_CHECK_CODE(code, lino, _end);
      return true;
    } else if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_IS_WINDOW_FILLED) {
      code = colDataSetVal(pDstColInfo, (uint32_t)rowIndex, (const char*)&pFillInfo->isFilled, false);
      QUERY_CHECK_CODE(code, lino, _end);
      return true;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pFillInfo->pTaskInfo->env, code);
  }
  return false;
}

static int32_t doSetVal(SColumnInfoData* pDstCol, int64_t rowIndex,
                        const SGroupKeys* pKey) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pKey->isNull) {
    colDataSetNULL(pDstCol, (uint32_t)rowIndex);
  } else {
    code = colDataSetVal(pDstCol, (uint32_t)rowIndex, pKey->pData, false);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t initBeforeAfterDataBuf(SFillInfo* pFillInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (taosArrayGetSize(pFillInfo->next.pRowVal) > 0) {
    goto _end;
  }

  for (int i = 0; i < pFillInfo->numOfCols; i++) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[i];

    SGroupKeys  key = {0};
    SResSchema* pSchema = &pCol->pExpr->base.resSchema;
    key.pData = taosMemoryMalloc(pSchema->bytes);
    QUERY_CHECK_NULL(key.pData, code, lino, _end, terrno);
    key.isNull = true;
    key.bytes = pSchema->bytes;
    key.type = pSchema->type;
    bool nullValueFlag = false;
    TSKEY initialVal = TSKEY_INITIAL_VAL;

    void* tmp = taosArrayPush(pFillInfo->next.pValueTs, &initialVal);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

    tmp = taosArrayPush(pFillInfo->next.pRowVal, &key);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

    tmp = taosArrayPush(pFillInfo->next.pNullValueFlag, &nullValueFlag);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

    key.pData = taosMemoryMalloc(pSchema->bytes);
    QUERY_CHECK_NULL(key.pData, code, lino, _end, terrno);

    tmp = taosArrayPush(pFillInfo->prev.pValueTs, &initialVal);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

    tmp = taosArrayPush(pFillInfo->prev.pRowVal, &key);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

    tmp = taosArrayPush(pFillInfo->prev.pNullValueFlag, &nullValueFlag);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t taosNumOfRemainRows(SFillInfo* pFillInfo) {
  if (pFillInfo->numOfRows == 0 || (pFillInfo->numOfRows > 0 && pFillInfo->index >= pFillInfo->numOfRows)) {
    return 0;
  }

  return pFillInfo->numOfRows - pFillInfo->index;
}

int32_t taosCreateFillInfo(TSKEY skey, int32_t numOfFillCols,
                           int32_t numOfNotFillCols, int32_t fillNullCols,
                           int32_t capacity, const SInterval* pInterval,
                           int32_t fillType, struct SFillColInfo* pCol,
                           int32_t primaryTsSlotId, int32_t order,
                           const char* id, SExecTaskInfo* pTaskInfo,
                           int64_t surroundingTime, SFillInfo** ppFillInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (fillType == TSDB_FILL_NONE) {
    (*ppFillInfo) = NULL;
    return code;
  }

  SFillInfo* pFillInfo = taosMemoryCalloc(1, sizeof(SFillInfo));
  QUERY_CHECK_NULL(pFillInfo, code, lino, _end, terrno);

  pFillInfo->order = order;
  pFillInfo->srcTsSlotId = primaryTsSlotId;

  for (int32_t i = 0; i < numOfNotFillCols; ++i) {
    SFillColInfo* p = &pCol[i + numOfFillCols];
    int32_t       srcSlotId = GET_DEST_SLOT_ID(p);
    if (srcSlotId == primaryTsSlotId) {
      pFillInfo->tsSlotId = i + numOfFillCols;
      break;
    }
  }

  taosResetFillInfo(pFillInfo, skey);

  pFillInfo->type = fillType;
  pFillInfo->pFillCol = pCol;
  pFillInfo->numOfCols = numOfFillCols + numOfNotFillCols + fillNullCols;
  pFillInfo->alloc = capacity;
  pFillInfo->id = id;
  pFillInfo->interval = *pInterval;
  pFillInfo->surroundingTime = surroundingTime;

  pFillInfo->next.pValueTs = taosArrayInit(pFillInfo->numOfCols,
                                           sizeof(TSKEY));
  QUERY_CHECK_NULL(pFillInfo->next.pValueTs, code, lino, _end, terrno);

  pFillInfo->next.pRowVal = taosArrayInit(pFillInfo->numOfCols,
                                          sizeof(SGroupKeys));
  QUERY_CHECK_NULL(pFillInfo->next.pRowVal, code, lino, _end, terrno);

  pFillInfo->next.pNullValueFlag = taosArrayInit(pFillInfo->numOfCols,
                                                 sizeof(bool));
  QUERY_CHECK_NULL(pFillInfo->next.pNullValueFlag, code, lino, _end, terrno);

  pFillInfo->prev.pValueTs = taosArrayInit(pFillInfo->numOfCols,
                                           sizeof(TSKEY));
  QUERY_CHECK_NULL(pFillInfo->prev.pValueTs, code, lino, _end, terrno);

  pFillInfo->prev.pRowVal = taosArrayInit(pFillInfo->numOfCols,
                                          sizeof(SGroupKeys));
  QUERY_CHECK_NULL(pFillInfo->prev.pRowVal, code, lino, _end, terrno);

  pFillInfo->prev.pNullValueFlag = taosArrayInit(pFillInfo->numOfCols,
                                                 sizeof(bool));
  QUERY_CHECK_NULL(pFillInfo->prev.pNullValueFlag, code, lino, _end, terrno);

  code = initBeforeAfterDataBuf(pFillInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  pFillInfo->pTaskInfo = pTaskInfo;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pFillInfo = taosDestroyFillInfo(pFillInfo);
  }
  (*ppFillInfo) = pFillInfo;
  return code;
}

void taosResetFillInfo(SFillInfo* pFillInfo, TSKEY startTimestamp) {
  pFillInfo->start = startTimestamp;
  pFillInfo->currentKey = startTimestamp;
  pFillInfo->end = startTimestamp;
  pFillInfo->index = -1;
  pFillInfo->numOfRows = 0;
  pFillInfo->numOfCurrent = 0;
  pFillInfo->numOfTotal = 0;
}

void* taosDestroyFillInfo(SFillInfo* pFillInfo) {
  if (pFillInfo == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pFillInfo->prev.pRowVal); ++i) {
    SGroupKeys* pKey = taosArrayGet(pFillInfo->prev.pRowVal, i);
    if (pKey) taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pFillInfo->prev.pNullValueFlag);
  taosArrayDestroy(pFillInfo->prev.pRowVal);
  taosArrayDestroy(pFillInfo->prev.pValueTs);
  for (int32_t i = 0; i < taosArrayGetSize(pFillInfo->next.pRowVal); ++i) {
    SGroupKeys* pKey = taosArrayGet(pFillInfo->next.pRowVal, i);
    if (pKey) taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pFillInfo->next.pNullValueFlag);
  taosArrayDestroy(pFillInfo->next.pRowVal);
  taosArrayDestroy(pFillInfo->next.pValueTs);

  // free pFillCol
  if (pFillInfo->pFillCol) {
    for (int32_t i = 0; i < pFillInfo->numOfCols; i++) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];
      if (!pCol->notFillCol) {
        if (pCol->fillVal.nType == TSDB_DATA_TYPE_VARBINARY || pCol->fillVal.nType == TSDB_DATA_TYPE_VARCHAR ||
            pCol->fillVal.nType == TSDB_DATA_TYPE_NCHAR || pCol->fillVal.nType == TSDB_DATA_TYPE_JSON ||
            pCol->fillVal.nType == TSDB_DATA_TYPE_DECIMAL) {
          if (pCol->fillVal.pz) {
            taosMemoryFree(pCol->fillVal.pz);
            pCol->fillVal.pz = NULL;
          }
        }
      }
    }
  }

  taosMemoryFreeClear(pFillInfo->pTags);
  taosMemoryFreeClear(pFillInfo->pFillCol);
  taosArrayDestroy(pFillInfo->pColFillProgress);
  tdListFreeP(pFillInfo->pFillSavedBlockList, destroyFillBlock);
  taosMemoryFreeClear(pFillInfo);
  return NULL;
}

void taosFillSetStartInfo(SFillInfo* pFillInfo, int32_t numOfRows, TSKEY endKey) {
  if (pFillInfo->type == TSDB_FILL_NONE) {
    return;
  }

  // the endKey is now the aligned time window value. truncate time window isn't correct.
  pFillInfo->end = endKey;
  pFillInfo->index = 0;
  pFillInfo->numOfRows = numOfRows;
}

void taosFillSetInputDataBlock(SFillInfo* pFillInfo, const SSDataBlock* pInput) {
  pFillInfo->pSrcBlock = (SSDataBlock*)pInput;
}

void taosFillUpdateStartTimestampInfo(SFillInfo* pFillInfo, int64_t ts) {
  pFillInfo->start = ts;
  pFillInfo->currentKey = ts;
}

bool taosFillNotStarted(const SFillInfo* pFillInfo) { return pFillInfo->start == pFillInfo->currentKey; }

bool taosFillHasMoreResults(SFillInfo* pFillInfo) {
  int32_t remain = taosNumOfRemainRows(pFillInfo);
  if (remain > 0) {
    return true;
  }

  bool ascFill = FILL_IS_ASC_FILL(pFillInfo);
  if (pFillInfo->numOfTotal > 0 &&
      (((pFillInfo->end > pFillInfo->start) && ascFill) || (pFillInfo->end < pFillInfo->start && !ascFill))) {
    return getNumOfResultsAfterFillGap(pFillInfo, pFillInfo->end, 4096) > 0;
  }

  return false;
}

int64_t getNumOfResultsAfterFillGap(SFillInfo* pFillInfo, TSKEY ekey, int32_t maxNumOfRows) {
  int32_t numOfRows = taosNumOfRemainRows(pFillInfo);

  TSKEY ekey1 = ekey;

  int64_t numOfRes = -1;
  if (numOfRows > 0) {  // still fill gap within current data block, not generating data after the result set.
    SColumnInfoData* pCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, pFillInfo->srcTsSlotId);
    int64_t*         tsList = (int64_t*)pCol->pData;
    TSKEY            lastKey = tsList[pFillInfo->numOfRows - 1];
    numOfRes =
        taosTimeCountIntervalForFill(lastKey, pFillInfo->currentKey, pFillInfo->interval.sliding,
                                     pFillInfo->interval.slidingUnit, pFillInfo->interval.precision, pFillInfo->order);
  } else {  // reach the end of data
    if ((ekey1 < pFillInfo->currentKey && FILL_IS_ASC_FILL(pFillInfo)) ||
        (ekey1 > pFillInfo->currentKey && !FILL_IS_ASC_FILL(pFillInfo))) {
      return 0;
    }

    numOfRes =
        taosTimeCountIntervalForFill(ekey1, pFillInfo->currentKey, pFillInfo->interval.sliding,
                                     pFillInfo->interval.slidingUnit, pFillInfo->interval.precision, pFillInfo->order);
  }

  return (numOfRes > maxNumOfRows) ? maxNumOfRows : numOfRes;
}

void taosGetLinearInterpolationVal(SPoint* point, int32_t outputType, SPoint* point1, SPoint* point2, int32_t inputType,
                                   STypeMod inputTypeMod) {
  double v1 = -1, v2 = -1;
  GET_TYPED_DATA(v1, double, inputType, point1->val, inputTypeMod);
  GET_TYPED_DATA(v2, double, inputType, point2->val, inputTypeMod);

  double r = 0;
  if (!IS_BOOLEAN_TYPE(inputType)) {
    r = DO_INTERPOLATION(v1, v2, point1->key, point2->key, point->key);
  } else {
    r = (v1 < 1 || v2 < 1) ? 0 : 1;
  }
  SET_TYPED_DATA(point->val, outputType, r);
}

int64_t getFillInfoStart(struct SFillInfo* pFillInfo) { return pFillInfo->start; }

SFillColInfo* createFillColInfo(SExprInfo* pExpr, int32_t numOfFillExpr, SExprInfo* pNotFillExpr,
                                int32_t numOfNoFillExpr, SExprInfo* pFillNullExpr, int32_t numOfFillNullExpr,
                                const struct SNodeListNode* pValNode) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SFillColInfo* pFillCol = taosMemoryCalloc(numOfFillExpr + numOfNoFillExpr + numOfFillNullExpr, sizeof(SFillColInfo));
  if (pFillCol == NULL) {
    return NULL;
  }

  size_t len = (pValNode != NULL) ? LIST_LENGTH(pValNode->pNodeList) : 0;
  for (int32_t i = 0; i < numOfFillExpr; ++i) {
    SExprInfo* pExprInfo = &pExpr[i];
    pFillCol[i].pExpr = pExprInfo;
    pFillCol[i].notFillCol = false;

    // todo refactor
    if (len > 0) {
      // if the user specified value is less than the column, alway use the last one as the fill value
      int32_t index = (i >= len) ? (len - 1) : i;

      SValueNode* pv = (SValueNode*)nodesListGetNode(pValNode->pNodeList, index);
      QUERY_CHECK_NULL(pv, code, lino, _end, terrno);
      code = nodesValueNodeToVariant(pv, &pFillCol[i].fillVal);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    if (TSDB_CODE_SUCCESS != code) {
      goto _end;
    }
  }
  pFillCol->numOfFillExpr = numOfFillExpr;

  for (int32_t i = 0; i < numOfNoFillExpr; ++i) {
    SExprInfo* pExprInfo = &pNotFillExpr[i];
    pFillCol[i + numOfFillExpr].pExpr = pExprInfo;
    pFillCol[i + numOfFillExpr].notFillCol = true;
  }

  for (int32_t i = 0; i < numOfFillNullExpr; ++i) {
    SExprInfo* pExprInfo = &pFillNullExpr[i];
    pFillCol[i + numOfFillExpr + numOfNoFillExpr].pExpr = pExprInfo;
    pFillCol[i + numOfFillExpr + numOfNoFillExpr].notFillCol = true;
    pFillCol[i + numOfFillExpr + numOfNoFillExpr].fillNull = true;
  }

  return pFillCol;

_end:
  for (int32_t i = 0; i < numOfFillExpr; ++i) {
    taosVariantDestroy(&pFillCol[i].fillVal);
  }
  taosMemoryFree(pFillCol);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return NULL;
}

static bool fillShouldPause(SFillInfo* pFillInfo, const SSDataBlock* pDstBlock) {
  if (pFillInfo->pSrcBlock && pFillInfo->index >= pFillInfo->pSrcBlock->info.rows) return true;
  if (pDstBlock->info.rows > 0) return true;
  if (pFillInfo->numOfRows == 0) return true;
  if (pFillInfo->order == TSDB_ORDER_ASC && pFillInfo->currentKey > pFillInfo->end) return true;
  if (pFillInfo->order == TSDB_ORDER_DESC && pFillInfo->currentKey < pFillInfo->end) return true;
  if (pDstBlock->info.rows > 0) return true;
  return false;
}

/**
  @brief Copy the current value into the `pRowVal` buffer, reset the null flag and
  record the timestamp for this column value.
  @param pRowVal Pointer to the SRowVal struct to store the extracted data.
  @param colIdx  The index of the column to save.
  @param src     The source data of the column.
  @param isNull  Whether the column value is null.
  @param rowTs   The timestamp of the row being copied.
*/
static void copyCurrentValIntoBuf(const SRowVal* pRowVal, int32_t colIdx,
                                  const char* src, bool isNull, TSKEY rowTs) {
  SGroupKeys* pKey = taosArrayGet(pRowVal->pRowVal, colIdx);
  if (isNull) {
    pKey->isNull = true;
  } else {
    if (IS_VAR_DATA_TYPE(pKey->type)) {
      int32_t bytes = calcStrBytesByType(pKey->type, (char*)src);
      memcpy(pKey->pData, src, bytes);
    } else {
      memcpy(pKey->pData, src, pKey->bytes);
    }
    pKey->isNull = false;
    int64_t* pValueTs = taosArrayGet(pRowVal->pValueTs, colIdx);
    *pValueTs = rowTs;
  }
}

/**
  @brief Copy the current row of data from the source block into a buffer for
  filling operations.
  @param pFillInfo Pointer to the SFillInfo context.
  @param rowIndex  The index of the row in the source block to copy.
  @param pRowVal   Pointer to the SRowVal struct to store the extracted data.
  @param rowTs     The timestamp of the row being copied.
*/
static int32_t copyCurrentRowIntoBuf(const SFillInfo* pFillInfo, int32_t rowIndex,
                                     const SRowVal* pRowVal, TSKEY rowTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t fillType = pFillInfo->type;
  bool    fillNext = fillType == TSDB_FILL_NEXT;
  bool    fillPrev = fillType == TSDB_FILL_PREV;
  bool    ascFill  = FILL_IS_ASC_FILL(pFillInfo);
  bool    ascNext  = ascFill && fillNext;
  bool    descPrev = !ascFill && fillPrev;

  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    int32_t type = pFillInfo->pFillCol[i].pExpr->pExpr->nodeType;
    if (type == QUERY_NODE_COLUMN ||
        type == QUERY_NODE_OPERATOR ||
        type == QUERY_NODE_FUNCTION) {
      int32_t srcSlotId = GET_DEST_SLOT_ID(&pFillInfo->pFillCol[i]);

      if (rowTs != pFillInfo->currentKey) {
        if (!pFillInfo->pFillCol[i].notFillCol) {
          if (!ascNext && !descPrev) continue;
        }
        if (srcSlotId == pFillInfo->srcTsSlotId &&
            pFillInfo->type == TSDB_FILL_LINEAR) {
          continue;
        }
      }

      const SColumnInfoData* pSrcCol = taosArrayGet(
        pFillInfo->pSrcBlock->pDataBlock, srcSlotId);
      QUERY_CHECK_NULL(pSrcCol, code, lino, _end, terrno);
      bool  isNull = colDataIsNull_s(pSrcCol, rowIndex);
      const char* p = colDataGetData(pSrcCol, rowIndex);
      if ((fillNext || fillPrev) && !pFillInfo->pFillCol[i].notFillCol &&
          pRowVal->pNullValueFlag != NULL) {
        bool* pNullValueFlag = taosArrayGet(pRowVal->pNullValueFlag, i);
        *pNullValueFlag = isNull;
        bool ascPrevOrDescNext = (fillPrev && ascFill) ||
                                 (fillNext && !ascFill);
        /*
          For ascPrev and descNext, we only set NULL flag, but do not save NULL
          values into prev/next pRowVal, because the last prev or last next will
          be used for later filling, and should not be overridden by NULL values
        */
        if (isNull && ascPrevOrDescNext) continue;
      }

      copyCurrentValIntoBuf(pRowVal, i, p, isNull, rowTs);
    } else {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t fillTrySaveRow(struct SFillInfo* pFillInfo, const SSDataBlock* pBlock, int32_t rowIdx) {
  if (!pBlock) return TSDB_CODE_SUCCESS;
  bool     ascFill = FILL_IS_ASC_FILL(pFillInfo);
  TSKEY    rowTs = getBlockCurTs(pBlock, rowIdx, pFillInfo->srcTsSlotId);
  int32_t  fillType = pFillInfo->type;
  SRowVal* pFillRow = ascFill ? (fillType == TSDB_FILL_NEXT ? &pFillInfo->next : &pFillInfo->prev)
                              : (fillType == TSDB_FILL_PREV ? &pFillInfo->next : &pFillInfo->prev);
  return copyCurrentRowIntoBuf(pFillInfo, rowIdx, pFillRow, rowTs);
}

static int32_t tIsColFallBehind(struct SFillInfo* pFillInfo, int32_t colIdx) {
  SColumnFillProgress* pColProgress = taosArrayGet(pFillInfo->pColFillProgress, colIdx);
  if (!pColProgress) {
    qError("failed to get col progress for col %d, size: %lu", colIdx, taosArrayGetSize(pFillInfo->pColFillProgress));
    return TSDB_CODE_INTERNAL_ERROR;
  }
  if (pColProgress->pBlockNode) {
    return true;
  }
  return false;
}

static bool tFillTrySaveColProgress(const struct SFillInfo* pFillInfo,
                                    int32_t colIdx, SListNode* pBlockNode,
                                    int64_t rowIdx) {
  bool ascNext = pFillInfo->type == TSDB_FILL_NEXT && pFillInfo->order == TSDB_ORDER_ASC;
  bool descPrev = pFillInfo->type == TSDB_FILL_PREV && pFillInfo->order == TSDB_ORDER_DESC;
  if (ascNext || descPrev) {
    SColumnFillProgress* pProgress = taosArrayGet(pFillInfo->pColFillProgress, colIdx);
    if (!pProgress->pBlockNode) {
      pProgress->pBlockNode = pBlockNode;
      pProgress->rowIdx = rowIdx;
      const SFillBlock*   pFillBlock = (SFillBlock*)pBlockNode->data;
      SBlockFillProgress* pFillProg = taosArrayGet(pFillBlock->pFillProgress, colIdx);
      pFillProg->rowIdx = rowIdx;
    }
    return true;
  }
  return false;
}

static bool doFillOneCol(SFillInfo* pFillInfo, SSDataBlock* pFillBlock,
                         TSKEY ts, int32_t colIdx, int64_t rowIdx,
                         bool outOfBound, bool fillFromHead) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    saveProgress = false;
  SFillColInfo* pCol = &pFillInfo->pFillCol[colIdx];
  SColumnInfoData* pDstCol = taosArrayGet(pFillBlock->pDataBlock,
                                          GET_DEST_SLOT_ID(pCol));
  if (pFillInfo->type == TSDB_FILL_PREV || pFillInfo->type == TSDB_FILL_NEXT) {
    bool filled = fillIfWindowPseudoColumn(pFillInfo, pFillBlock, rowIdx, colIdx);
    if (!filled) {
      saveProgress = setNotFillColumn(pFillInfo, pFillBlock, rowIdx, colIdx,
                                      fillFromHead);
      saveProgress = saveProgress && (
        (pFillInfo->type == TSDB_FILL_PREV &&
          pFillInfo->order == TSDB_ORDER_DESC) ||
        (pFillInfo->type == TSDB_FILL_NEXT &&
          pFillInfo->order == TSDB_ORDER_ASC)
      );
    }
  } else if (pFillInfo->type == TSDB_FILL_LINEAR) {
    // TODO : linear interpolation supports NULL value
    if (outOfBound) {
      setNullCol(pFillBlock, pFillInfo, rowIdx, colIdx, fillFromHead);
    } else {
      if (pCol->notFillCol) {
        bool filled = fillIfWindowPseudoColumn(pFillInfo, pFillBlock, rowIdx, colIdx);
        if (!filled) {
          (void)setNotFillColumn(pFillInfo, pFillBlock, rowIdx, colIdx,
                                 fillFromHead);
        }
      } else {
        const SRowVal*   pRVal = &pFillInfo->prev;
        SGroupKeys*      pKey = taosArrayGet(pRVal->pRowVal, colIdx);
        SColumnInfoData* pSrcCol =
          taosArrayGet(pFillInfo->pSrcBlock->pDataBlock,
                       pFillInfo->srcTsSlotId);
        int16_t type = pDstCol->info.type;
        if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_BOOL ||
            pKey->isNull || colDataIsNull_s(pSrcCol, pFillInfo->index)) {
          colDataSetNULL(pDstCol, (uint32_t)rowIdx);
        } else {
          SGroupKeys* pKey1 = taosArrayGet(pRVal->pRowVal, pFillInfo->tsSlotId);

          int64_t prevTs = *(int64_t*)pKey1->pData;
          char*   data = colDataGetData(pSrcCol, pFillInfo->index);
          SPoint  point1, point2, point;

          point1 = (SPoint){.key = prevTs, .val = pKey->pData};
          point2 = (SPoint){.key = ts, .val = data};

          int64_t out = 0;
          point = (SPoint){.key = pFillInfo->currentKey, .val = &out};
          taosGetLinearInterpolationVal(&point, type, &point1, &point2, type,
                                        typeGetTypeModFromColInfo(&pDstCol->info));

          code = colDataSetVal(pDstCol, (uint32_t)rowIdx, (const char*)&out, false);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  } else if (pFillInfo->type == TSDB_FILL_NULL || pFillInfo->type == TSDB_FILL_NULL_F) {  // fill with NULL
    setNullCol(pFillBlock, pFillInfo, rowIdx, colIdx, fillFromHead);
  } else {  // fill with user specified value for each column
    if (pCol->notFillCol) {
      bool filled = fillIfWindowPseudoColumn(pFillInfo, pFillBlock, rowIdx, colIdx);
      if (!filled) {
        (void)setNotFillColumn(pFillInfo, pFillBlock, rowIdx, colIdx,
                               fillFromHead);
      }
    } else {
      SVariant* pVar = &pFillInfo->pFillCol[colIdx].fillVal;
      code = doSetUserSpecifiedValue(pDstCol, pVar, rowIdx);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pFillInfo->pTaskInfo->env, code);
  }
  return saveProgress;
}

static int32_t tFillFromHeadForCol(struct SFillInfo* pFillInfo, TSKEY ts, int32_t colIdx, bool outOfBound) {
  int32_t code = 0;
  // Check the progress of this col, start fill from the start block
  // Here we will always fill till the last row of last block in list. Cause this is always the first time we meet
  // non-null value after fill till current key, we should update it's progress, set no lag for this col
  SColumnFillProgress* pColProgress = taosArrayGet(pFillInfo->pColFillProgress, colIdx);
  if (!pColProgress) {
    qError("failed to get col progress for col %d, size: %lu", colIdx, taosArrayGetSize(pFillInfo->pColFillProgress));
    return TSDB_CODE_INTERNAL_ERROR;
  }
  SListNode* pListNode = pColProgress->pBlockNode;

  while (pListNode) {
    SFillBlock*                pFillBlock = (SFillBlock*)pListNode->data;
    const SColumnFillProgress* pProgress = taosArrayGet(pFillInfo->pColFillProgress, colIdx);
    for (int32_t rowIdx = pProgress->rowIdx; rowIdx < pFillBlock->pBlock->info.rows; ++rowIdx) {
      TAOS_UNUSED(doFillOneCol(pFillInfo, pFillBlock->pBlock, ts, colIdx,
                               rowIdx, outOfBound, true));
    }
    SBlockFillProgress* pMyBlockProgress = taosArrayGet(pFillBlock->pFillProgress, colIdx);
    pMyBlockProgress->rowIdx = pFillBlock->pBlock->info.rows;
    bool allColFinished = true;
    for (int32_t i = 0; i < taosArrayGetSize(pFillBlock->pFillProgress); ++i) {
      SBlockFillProgress* pBProgress = taosArrayGet(pFillBlock->pFillProgress, i);
      if (pBProgress->rowIdx < pFillBlock->pBlock->info.rows) {
        allColFinished = false;
        break;
      }
    }
    pFillBlock->allColFinished = allColFinished;
    pListNode = TD_DLIST_NODE_NEXT(pListNode);
    // update progress
    pColProgress->pBlockNode = pListNode;
    pColProgress->rowIdx = 0;
  }
  return code;
}

/**
  @brief Fill one not existing row of time `pFillInfo->currentKey`. This
  function is called when we meet a row with larger timestamp than
  `pFillInfo->currentKey` or whole datablock is consumed.
  @param pFillInfo  Fill info.
  @param pBlock     Data block to store filled data.
  @param ts         Timestamp of the row to fill.
  @param outOfBound Whether the datablock is consumed.
*/
static void doFillOneRow(SFillInfo* pFillInfo, SSDataBlock* pFillBlock,
                         int64_t ts, bool outOfBound) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);
  int64_t rowIdx = pFillBlock->info.rows;

  for (int32_t colIdx = 0; code == TSDB_CODE_SUCCESS &&
       colIdx < pFillInfo->numOfCols; ++colIdx) {
    if (outOfBound && tIsColFallBehind(pFillInfo, colIdx)) {
      code = tFillFromHeadForCol(pFillInfo, ts, colIdx, true);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    bool saveProgress = doFillOneCol(pFillInfo, pFillBlock, ts, colIdx, rowIdx,
                                     outOfBound, false);
    if (saveProgress) {
      SListNode* pFillBlockListNode =
        tdListGetTail(pFillInfo->pFillSavedBlockList);
      (void)tFillTrySaveColProgress(pFillInfo, colIdx, pFillBlockListNode,
                                    rowIdx);
    }
  }
  const SInterval* pInterval = &pFillInfo->interval;
  pFillInfo->currentKey = taosTimeAdd(pFillInfo->currentKey,
                                      pInterval->sliding * step,
                                      pInterval->slidingUnit,
                                      pInterval->precision, NULL);
  pFillInfo->numOfCurrent++;
  pFillBlock->info.rows += 1;
  /*
    note: we are filling one not existing row, 
      so the `pFillInfo->index` is not changed.
  */

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pFillInfo->pTaskInfo->env, code);
  }
}

static void tryExtractReadyBlocks(struct SFillInfo* pFillInfo, SSDataBlock* pDstBlock, int32_t capacity) {
  SListNode* pListNode = tdListGetHead(pFillInfo->pFillSavedBlockList);
  bool       allFinished = true;
  bool       noMoreBlocks = pFillInfo->numOfRows == 0;
  if (pListNode) {
    SFillBlock* pFillBlock = (SFillBlock*)pListNode->data;
    if (!noMoreBlocks) {
      if (pFillBlock->pBlock->info.rows < capacity) return;
      for (int32_t colIdx = 0; colIdx < pFillInfo->numOfCols; ++colIdx) {
        SColumnFillProgress* pProg = taosArrayGet(pFillInfo->pColFillProgress, colIdx);
        if (pProg->pBlockNode == pListNode) {
          allFinished = false;
          break;
        }
      }
    }
    if (allFinished || noMoreBlocks) {
      TSWAP(pDstBlock->info.rows, pFillBlock->pBlock->info.rows);
      TSWAP(pDstBlock->pDataBlock, pFillBlock->pBlock->pDataBlock);
      SListNode *tNode = tdListPopNode(pFillInfo->pFillSavedBlockList, pListNode);
      destroyFillBlock(pListNode->data);
      taosMemFreeClear(pListNode);
    }
  }
}

static SSDataBlock* createNewSavedBlock(struct SFillInfo* pFillInfo, SSDataBlock* pDstBlock, int32_t capacity) {
  int32_t      code = 0;
  SSDataBlock* pBlock = NULL;
  code = createOneDataBlock(pDstBlock, false, &pBlock);
  if (code != 0) return NULL;
  code = blockDataEnsureCapacity(pBlock, capacity);
  if (code != 0) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  return pBlock;
}

static int32_t trySaveNewBlock(struct SFillInfo* pFillInfo, SSDataBlock* pDstBlock, int32_t capacity,
                               SFillBlock** ppFillBlock) {
  int32_t      code = 0;
  SSDataBlock* pBlock = createNewSavedBlock(pFillInfo, pDstBlock, capacity);
  if (!pBlock) {
    code = terrno;
    goto _end;
  }
  SArray* pProgress = taosArrayInit(pFillInfo->numOfCols, sizeof(SBlockFillProgress));
  if (!pProgress) {
    code = terrno;
    goto _end;
  }
  SBlockFillProgress prog = {INT32_MAX};
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    if (NULL == taosArrayPush(pProgress, &prog)) {
      code = terrno;
      goto _end;
    }
  }
  *ppFillBlock = tFillSaveBlock(pFillInfo, pBlock, pProgress);
  if (!*ppFillBlock) {
    code = terrno;
    goto _end;
  }
  return 0;
_end:
  if (pBlock) blockDataDestroy(pBlock);
  if (pProgress) taosArrayDestroy(pProgress);
  return code;
}

static int32_t fillInitSavedBlockList(struct SFillInfo* pFillInfo, SSDataBlock* pDstBlock, int32_t capacity) {
  int32_t     code = 0;
  SFillBlock* pFillBlock = NULL;
  pFillInfo->pFillSavedBlockList = tdListNew(sizeof(SFillBlock));
  if (!pFillInfo->pFillSavedBlockList) return terrno;
  code = trySaveNewBlock(pFillInfo, pDstBlock, capacity, &pFillBlock);
  if (code != 0) return code;

  pFillInfo->pColFillProgress = taosArrayInit(pFillInfo->numOfCols, sizeof(SColumnFillProgress));
  if (!pFillInfo->pColFillProgress) {
    return terrno;
  }
  SColumnFillProgress prog = {.pBlockNode = NULL, .rowIdx = 0};
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    if (NULL == taosArrayPush(pFillInfo->pColFillProgress, &prog)) {
      return terrno;
    }
  }
  return code;
}

static void tryResetColNextPrev(struct SFillInfo* pFillInfo, int32_t colIdx) {
  bool    ascFill = FILL_IS_ASC_FILL(pFillInfo);
  int32_t fillType = pFillInfo->type;
  bool    ascNext = ascFill && fillType == TSDB_FILL_NEXT, descPrev = !ascFill && fillType == TSDB_FILL_PREV;
  if ((ascNext || descPrev) && !pFillInfo->pFillCol[colIdx].notFillCol) {
    SRowVal*    pFillRow = ascFill ? (fillType == TSDB_FILL_NEXT ? &pFillInfo->next : &pFillInfo->prev)
                                   : (fillType == TSDB_FILL_NEXT ? &pFillInfo->prev : &pFillInfo->next);
    SGroupKeys* pKey = taosArrayGet(pFillRow->pRowVal, colIdx);
    pKey->isNull = true;
  }
}

int32_t taosFillResultDataBlock(struct SFillInfo* pFillInfo, SSDataBlock* pDstBlock, int32_t capacity,
                                 bool* wantMoreBlock) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SFillBlock* pFillBlock = NULL;
  SListNode*  pFillBlockListNode = NULL;
  pFillInfo->numOfCurrent = 0;

  if (!pFillInfo->pFillSavedBlockList) {
    code = fillInitSavedBlockList(pFillInfo, pDstBlock, capacity);
    if (code != 0) goto _end;
  }
  pFillBlockListNode = tdListGetTail(pFillInfo->pFillSavedBlockList);
  pFillBlock = pFillBlockListNode ? (SFillBlock*)pFillBlockListNode->data : NULL;

  // if all blocks are consumed, we have to fill for not filled cols
  if (pFillInfo->numOfRows == 0) {
    if (!pFillBlock) {
      code = trySaveNewBlock(pFillInfo, pDstBlock, capacity, &pFillBlock);
      if (code != 0) goto _end;
      pFillBlockListNode = tdListGetTail(pFillInfo->pFillSavedBlockList);
    }
    bool allFilled = pFillInfo->order == TSDB_ORDER_ASC ? pFillInfo->currentKey > pFillInfo->end
                                                        : pFillInfo->currentKey < pFillInfo->end;
    while (!allFilled && pFillBlock->pBlock->info.rows < capacity) {
      doFillOneRow(pFillInfo, pFillBlock->pBlock, pFillInfo->start, true);
      allFilled = pFillInfo->order == TSDB_ORDER_ASC ? pFillInfo->currentKey > pFillInfo->end
                                                     : pFillInfo->currentKey < pFillInfo->end;
    }

    for (int32_t colIdx = 0; colIdx < pFillInfo->numOfCols; ++colIdx) {
      if (tIsColFallBehind(pFillInfo, colIdx)) {
        code = tFillFromHeadForCol(pFillInfo, pFillInfo->start, colIdx, true);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

  // check from list head if we have already filled all rows in blocks, if any block is full, send it out
  tryExtractReadyBlocks(pFillInfo, pDstBlock, capacity);
  TSKEY lastSavedTs = -1;
  while (!fillShouldPause(pFillInfo, pDstBlock)) {
    if (!pFillBlock || pFillBlock->pBlock->info.rows >= capacity) {
      code = trySaveNewBlock(pFillInfo, pDstBlock, capacity, &pFillBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      pFillBlockListNode = tdListGetTail(pFillInfo->pFillSavedBlockList);
    }
    TSKEY fillCurTs = pFillInfo->currentKey;
    TSKEY blockCurTs = getBlockCurTs(pFillInfo->pSrcBlock, pFillInfo->index,
                                     pFillInfo->srcTsSlotId);
    if (pFillInfo->pSrcBlock && (lastSavedTs != blockCurTs || blockCurTs == fillCurTs))
      code = fillTrySaveRow(pFillInfo, pFillInfo->pSrcBlock, pFillInfo->index);
    lastSavedTs = blockCurTs;
    QUERY_CHECK_CODE(code, lino, _end);

    if (blockCurTs != fillCurTs || !pFillInfo->pSrcBlock) {
      doFillOneRow(pFillInfo, pFillBlock->pBlock, blockCurTs, false);
    } else {
      for (int32_t colIdx = 0; colIdx < pFillInfo->numOfCols; ++colIdx) {
        const SFillColInfo* pCol = &pFillInfo->pFillCol[colIdx];
        int64_t rowIdx = pFillBlock->pBlock->info.rows;
        int32_t dstSlotId = GET_DEST_SLOT_ID(pCol);
        SColumnInfoData* pDst = taosArrayGet(pFillBlock->pBlock->pDataBlock,
                                             dstSlotId);
        const SColumnInfoData* pSrc = taosArrayGet(
          pFillInfo->pSrcBlock->pDataBlock, dstSlotId);

        const char* src = colDataGetData(pSrc, pFillInfo->index);
        if (!colDataIsNull_s(pSrc, pFillInfo->index)) {
          if (tIsColFallBehind(pFillInfo, colIdx)) {
            code = tFillFromHeadForCol(pFillInfo, blockCurTs, colIdx, false);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          code = colDataSetVal(pDst, (uint32_t)rowIdx, src, false);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          // if col value in block is NULL, skip setting value for this col, save current position, wait till we got
          // non-null data if there is no lag for this col, then we should fill from (pFillBlock, index) when we got
          // non-null value. if this col is already fall behind, do nothing. Cause when we meet non-null value for this
          // col, we will fill till the last row of last block in list.
          bool saved = tFillTrySaveColProgress(pFillInfo, colIdx, pFillBlockListNode, rowIdx);
          if (!saved) {
            TAOS_UNUSED(doFillOneCol(pFillInfo, pFillBlock->pBlock, blockCurTs,
                                     colIdx, rowIdx, false, false));
          }
        }
        tryResetColNextPrev(pFillInfo, colIdx);
      }
      const SInterval* pInterval = &pFillInfo->interval;
      pFillInfo->currentKey =
        taosTimeAdd(pFillInfo->currentKey,
                    pInterval->sliding *
                      GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order),
                    pInterval->slidingUnit, pInterval->precision, NULL);
      pFillBlock->pBlock->info.rows += 1;
      pFillInfo->index += 1;
      pFillInfo->numOfCurrent += 1;
    }
    tryExtractReadyBlocks(pFillInfo, pDstBlock, capacity);
  }

_end:
  pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
  if (!isListEmpty(pFillInfo->pFillSavedBlockList)) {
    if (wantMoreBlock) *wantMoreBlock = true;
  } else {
    if (wantMoreBlock) *wantMoreBlock = false;
  }
  return code;
}

void destroyFillBlock(void* p) {
  SFillBlock* pFillBlock = p;
  taosArrayDestroy(pFillBlock->pFillProgress);
  blockDataDestroy(pFillBlock->pBlock);
}

SFillBlock* tFillSaveBlock(SFillInfo* pFill, SSDataBlock* pBlock, SArray* pProgress) {
  SFillBlock block = {.pBlock = pBlock, .pFillProgress = pProgress, .allColFinished = false};
  SListNode* pNode = tdListAdd(pFill->pFillSavedBlockList, &block);
  if (!pNode) {
    return NULL;
  }
  return (SFillBlock*)pNode->data;
}

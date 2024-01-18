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

#include "executorInt.h"
#include "filter.h"
#include "function.h"
#include "operator.h"
#include "os.h"
#include "querynodes.h"
#include "querytask.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "ttypes.h"
#include "mergejoin.h"


void mJoinTrimKeepOneRow(SSDataBlock* pBlock, int32_t totalRows, const bool* pBoolList) {
  //  int32_t totalRows = pBlock->info.rows;
  int32_t bmLen = BitmapLen(totalRows);
  char*   pBitmap = NULL;
  int32_t maxRows = 0;
  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
    // it is a reserved column for scalar function, and no data in this column yet.
    if (pDst->pData == NULL || (IS_VAR_DATA_TYPE(pDst->info.type) && pDst->varmeta.length == 0)) {
      continue;
    }

    int32_t numOfRows = 0;
    if (IS_VAR_DATA_TYPE(pDst->info.type)) {
      int32_t j = 0;
      pDst->varmeta.length = 0;

      while (j < totalRows) {
        if (pBoolList[j] == 0) {
          j += 1;
          continue;
        }

        if (colDataIsNull_var(pDst, j)) {
          colDataSetNull_var(pDst, numOfRows);
        } else {
          // fix address sanitizer error. p1 may point to memory that will change during realloc of colDataSetVal, first
          // copy it to p2
          char*   p1 = colDataGetVarData(pDst, j);
          int32_t len = 0;
          if (pDst->info.type == TSDB_DATA_TYPE_JSON) {
            len = getJsonValueLen(p1);
          } else {
            len = varDataTLen(p1);
          }
          char* p2 = taosMemoryMalloc(len);
          memcpy(p2, p1, len);
          colDataSetVal(pDst, numOfRows, p2, false);
          taosMemoryFree(p2);
        }
        numOfRows += 1;
        j += 1;
        break;
      }

      if (maxRows < numOfRows) {
        maxRows = numOfRows;
      }
    } else {
      if (pBitmap == NULL) {
        pBitmap = taosMemoryCalloc(1, bmLen);
      }

      memcpy(pBitmap, pDst->nullbitmap, bmLen);
      memset(pDst->nullbitmap, 0, bmLen);

      int32_t j = 0;

      switch (pDst->info.type) {
        case TSDB_DATA_TYPE_BIGINT:
        case TSDB_DATA_TYPE_UBIGINT:
        case TSDB_DATA_TYPE_DOUBLE:
        case TSDB_DATA_TYPE_TIMESTAMP:
          while (j < totalRows) {
            if (pBoolList[j] == 0) {
              j += 1;
              continue;
            }

            if (colDataIsNull_f(pBitmap, j)) {
              colDataSetNull_f(pDst->nullbitmap, numOfRows);
            } else {
              ((int64_t*)pDst->pData)[numOfRows] = ((int64_t*)pDst->pData)[j];
            }
            numOfRows += 1;
            j += 1;
            break;
          }
          break;
        case TSDB_DATA_TYPE_FLOAT:
        case TSDB_DATA_TYPE_INT:
        case TSDB_DATA_TYPE_UINT:
          while (j < totalRows) {
            if (pBoolList[j] == 0) {
              j += 1;
              continue;
            }
            if (colDataIsNull_f(pBitmap, j)) {
              colDataSetNull_f(pDst->nullbitmap, numOfRows);
            } else {
              ((int32_t*)pDst->pData)[numOfRows] = ((int32_t*)pDst->pData)[j];
            }
            numOfRows += 1;
            j += 1;
            break;
          }
          break;
        case TSDB_DATA_TYPE_SMALLINT:
        case TSDB_DATA_TYPE_USMALLINT:
          while (j < totalRows) {
            if (pBoolList[j] == 0) {
              j += 1;
              continue;
            }
            if (colDataIsNull_f(pBitmap, j)) {
              colDataSetNull_f(pDst->nullbitmap, numOfRows);
            } else {
              ((int16_t*)pDst->pData)[numOfRows] = ((int16_t*)pDst->pData)[j];
            }
            numOfRows += 1;
            j += 1;
            break;
          }
          break;
        case TSDB_DATA_TYPE_BOOL:
        case TSDB_DATA_TYPE_TINYINT:
        case TSDB_DATA_TYPE_UTINYINT:
          while (j < totalRows) {
            if (pBoolList[j] == 0) {
              j += 1;
              continue;
            }
            if (colDataIsNull_f(pBitmap, j)) {
              colDataSetNull_f(pDst->nullbitmap, numOfRows);
            } else {
              ((int8_t*)pDst->pData)[numOfRows] = ((int8_t*)pDst->pData)[j];
            }
            numOfRows += 1;
            j += 1;
            break;
          }
          break;
      }
    }

    if (maxRows < numOfRows) {
      maxRows = numOfRows;
    }
  }

  pBlock->info.rows = maxRows;
  if (pBitmap != NULL) {
    taosMemoryFree(pBitmap);
  }
}


int32_t mJoinFilterAndMarkHashRows(SSDataBlock* pBlock, SFilterInfo* pFilterInfo, SMJoinTableCtx* build, int32_t startRowIdx) {
  if (pFilterInfo == NULL || pBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SFilterColumnParam param1 = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
  SColumnInfoData*   p = NULL;

  int32_t code = filterSetDataFromSlotId(pFilterInfo, &param1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  int32_t status = 0;
  code = filterExecute(pFilterInfo, pBlock, &p, NULL, param1.numOfCols, &status);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  if (!build->pHashGrpRows->allRowsMatch && (status == FILTER_RESULT_ALL_QUALIFIED || status == FILTER_RESULT_PARTIAL_QUALIFIED)) {
    if (status == FILTER_RESULT_ALL_QUALIFIED && taosArrayGetSize(build->pHashCurGrp) == pBlock->info.rows) {
      build->pHashGrpRows->allRowsMatch = true;
    } else {
      bool* pRes = (bool*)p->pData;
      for (int32_t i = 0; i < pBlock->info.rows; ++i) {
        if ((status == FILTER_RESULT_PARTIAL_QUALIFIED && false == *(pRes + i)) || MJOIN_ROW_BITMAP_SET(build->pRowBitmap, build->pHashGrpRows->rowBitmapOffset, startRowIdx + i)) {
          continue;
        }

        MJOIN_SET_ROW_BITMAP(build->pRowBitmap, build->pHashGrpRows->rowBitmapOffset, startRowIdx + i);
        build->pHashGrpRows->rowMatchNum++;
      }

      if (build->pHashGrpRows->rowMatchNum == taosArrayGetSize(build->pHashGrpRows->pRows)) {
        build->pHashGrpRows->allRowsMatch = true;
      }
    }
  }

  extractQualifiedTupleByFilterResult(pBlock, p, status);

  code = TSDB_CODE_SUCCESS;

_err:
  colDataDestroy(p);
  taosMemoryFree(p);
  return code;
}

int32_t mJoinFilterAndMarkRows(SSDataBlock* pBlock, SFilterInfo* pFilterInfo, SMJoinTableCtx* build, int32_t startGrpIdx, int32_t startRowIdx) {
  if (pFilterInfo == NULL || pBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SFilterColumnParam param1 = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
  SColumnInfoData*   p = NULL;

  int32_t code = filterSetDataFromSlotId(pFilterInfo, &param1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  int32_t status = 0;
  code = filterExecute(pFilterInfo, pBlock, &p, NULL, param1.numOfCols, &status);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  int32_t rowNum = 0;
  bool* pRes = (bool*)p->pData;  
  int32_t grpNum = taosArrayGetSize(build->eqGrps);
  if (status == FILTER_RESULT_ALL_QUALIFIED || status == FILTER_RESULT_PARTIAL_QUALIFIED) {
    for (int32_t i = startGrpIdx; i < grpNum && rowNum < pBlock->info.rows; startRowIdx = 0, ++i) {
      SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, i);
      if (buildGrp->allRowsMatch) {
        rowNum += buildGrp->endIdx - startRowIdx + 1;
        continue;
      }
      
      if (status == FILTER_RESULT_ALL_QUALIFIED && startRowIdx == buildGrp->beginIdx && ((pBlock->info.rows - rowNum) >= (buildGrp->endIdx - startRowIdx + 1))) {
        buildGrp->allRowsMatch = true;
        rowNum += buildGrp->endIdx - startRowIdx + 1;
        continue;
      }

      for (int32_t m = startRowIdx; m <= buildGrp->endIdx && rowNum < pBlock->info.rows; ++m, ++rowNum) {
        if ((status == FILTER_RESULT_PARTIAL_QUALIFIED && false == *(pRes + rowNum)) || MJOIN_ROW_BITMAP_SET(build->pRowBitmap, buildGrp->rowBitmapOffset, m - buildGrp->beginIdx)) {
          continue;
        }

        MJOIN_SET_ROW_BITMAP(build->pRowBitmap, buildGrp->rowBitmapOffset, m - buildGrp->beginIdx);
        buildGrp->rowMatchNum++;
      }

      if (buildGrp->rowMatchNum == (buildGrp->endIdx - buildGrp->beginIdx + 1)) {
        buildGrp->allRowsMatch = true;
      }
    }
  } 
  
  extractQualifiedTupleByFilterResult(pBlock, p, status);

  code = TSDB_CODE_SUCCESS;

_err:
  colDataDestroy(p);
  taosMemoryFree(p);
  return code;
}

int32_t mJoinFilterAndKeepSingleRow(SSDataBlock* pBlock, SFilterInfo* pFilterInfo) {
  if (pFilterInfo == NULL || pBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SFilterColumnParam param1 = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
  SColumnInfoData*   p = NULL;

  int32_t code = filterSetDataFromSlotId(pFilterInfo, &param1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  int32_t status = 0;
  code = filterExecute(pFilterInfo, pBlock, &p, NULL, param1.numOfCols, &status);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }
  
  if (status == FILTER_RESULT_ALL_QUALIFIED) {
    pBlock->info.rows = 1;
  } else if (status == FILTER_RESULT_NONE_QUALIFIED) {
    pBlock->info.rows = 0;
  } else if (status == FILTER_RESULT_PARTIAL_QUALIFIED) {
    mJoinTrimKeepOneRow(pBlock, pBlock->info.rows, (bool*)p->pData);
  }

  code = TSDB_CODE_SUCCESS;

_err:
  colDataDestroy(p);
  taosMemoryFree(p);
  return code;
}

int32_t mJoinFilterAndNoKeepRows(SSDataBlock* pBlock, SFilterInfo* pFilterInfo) {
  if (pFilterInfo == NULL || pBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SFilterColumnParam param1 = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
  SColumnInfoData*   p = NULL;

  int32_t code = filterSetDataFromSlotId(pFilterInfo, &param1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  int32_t status = 0;
  code = filterExecute(pFilterInfo, pBlock, &p, NULL, param1.numOfCols, &status);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }
  
  if (status == FILTER_RESULT_NONE_QUALIFIED) {
    pBlock->info.rows = 0;
  }

  code = TSDB_CODE_SUCCESS;

_err:
  colDataDestroy(p);
  taosMemoryFree(p);
  return code;
}


int32_t mJoinCopyMergeMidBlk(SMJoinMergeCtx* pCtx, SSDataBlock** ppMid, SSDataBlock** ppFin) {
  SSDataBlock* pLess = NULL;
  SSDataBlock* pMore = NULL;
  if ((*ppMid)->info.rows < (*ppFin)->info.rows) {
    pLess = (*ppMid);
    pMore = (*ppFin);
  } else {
    pLess = (*ppFin);
    pMore = (*ppMid);
  }

  int32_t totalRows = pMore->info.rows + pLess->info.rows;
  if (totalRows <= pMore->info.capacity) {
    MJ_ERR_RET(blockDataMerge(pMore, pLess));
    blockDataCleanup(pLess);
    pCtx->midRemains = false;
  } else {
    int32_t copyRows = pMore->info.capacity - pMore->info.rows;
    MJ_ERR_RET(blockDataMergeNRows(pMore, pLess, pLess->info.rows - copyRows, copyRows));
    blockDataShrinkNRows(pLess, copyRows);
    pCtx->midRemains = true;
  }

  if (pMore != (*ppFin)) {
    TSWAP(*ppMid, *ppFin);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t mJoinHandleMidRemains(SMJoinMergeCtx* pCtx) {
  ASSERT(0 < pCtx->midBlk->info.rows);

  TSWAP(pCtx->midBlk, pCtx->finBlk);

  pCtx->midRemains = false;

  return TSDB_CODE_SUCCESS;
}

int32_t mJoinNonEqGrpCart(SMJoinOperatorInfo* pJoin, SSDataBlock* pRes, bool append, SMJoinGrpRows* pGrp, bool probeGrp) {
  SMJoinTableCtx* probe = probeGrp ? pJoin->probe : pJoin->build;
  SMJoinTableCtx* build = probeGrp ? pJoin->build : pJoin->probe;
  int32_t currRows = append ? pRes->info.rows : 0;
  int32_t firstRows = GRP_REMAIN_ROWS(pGrp);
  
  for (int32_t c = 0; c < probe->finNum; ++c) {
    SMJoinColMap* pFirstCol = probe->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pGrp->blk->pDataBlock, pFirstCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pRes->pDataBlock, pFirstCol->dstSlot);
    colDataAssignNRows(pOutCol, currRows, pInCol, pGrp->readIdx, firstRows);
  }
  
  for (int32_t c = 0; c < build->finNum; ++c) {
    SMJoinColMap* pSecondCol = build->finCols + c;
    SColumnInfoData* pOutCol = taosArrayGet(pRes->pDataBlock, pSecondCol->dstSlot);
    colDataSetNItemsNull(pOutCol, currRows, firstRows);
  }
  
  pRes->info.rows = append ? (pRes->info.rows + firstRows) : firstRows;
  return TSDB_CODE_SUCCESS;
}


int32_t mJoinNonEqCart(SMJoinCommonCtx* pCtx, SMJoinGrpRows* pGrp, bool probeGrp) {
  pCtx->lastEqGrp = false;
  pCtx->lastProbeGrp = probeGrp;

  int32_t rowsLeft = pCtx->finBlk->info.capacity - pCtx->finBlk->info.rows;
  if (rowsLeft <= 0) {
    pCtx->grpRemains = pGrp->readIdx <= pGrp->endIdx;
    return TSDB_CODE_SUCCESS;
  }

  if (GRP_REMAIN_ROWS(pGrp) <= rowsLeft) {
    MJ_ERR_RET(mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, pGrp, probeGrp));
    pGrp->readIdx = pGrp->endIdx + 1;
    pCtx->grpRemains = false;
  } else {
    int32_t endIdx = pGrp->endIdx;
    pGrp->endIdx = pGrp->readIdx + rowsLeft - 1;
    MJ_ERR_RET(mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, pGrp, probeGrp));
    pGrp->readIdx = pGrp->endIdx + 1;
    pGrp->endIdx = endIdx;
    pCtx->grpRemains = true;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mJoinMergeGrpCart(SMJoinOperatorInfo* pJoin, SSDataBlock* pRes, bool append, SMJoinGrpRows* pFirst, SMJoinGrpRows* pSecond) {
  SMJoinTableCtx* probe = pJoin->probe;
  SMJoinTableCtx* build = pJoin->build;
  int32_t currRows = append ? pRes->info.rows : 0;
  int32_t firstRows = GRP_REMAIN_ROWS(pFirst);  
  int32_t secondRows = GRP_REMAIN_ROWS(pSecond);
  ASSERT(secondRows > 0);

  for (int32_t c = 0; c < probe->finNum; ++c) {
    SMJoinColMap* pFirstCol = probe->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pFirst->blk->pDataBlock, pFirstCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pRes->pDataBlock, pFirstCol->dstSlot);
    for (int32_t r = 0; r < firstRows; ++r) {
      if (colDataIsNull_s(pInCol, pFirst->readIdx + r)) {
        colDataSetNItemsNull(pOutCol, currRows + r * secondRows, secondRows);
      } else {
        ASSERT(pRes->info.capacity >= (pRes->info.rows + firstRows * secondRows));
        uint32_t startOffset = (IS_VAR_DATA_TYPE(pOutCol->info.type)) ? pOutCol->varmeta.length : ((currRows + r * secondRows) * pOutCol->info.bytes);
        ASSERT((startOffset + 1 * pOutCol->info.bytes) <= pRes->info.capacity * pOutCol->info.bytes);
        colDataSetNItems(pOutCol, currRows + r * secondRows, colDataGetData(pInCol, pFirst->readIdx + r), secondRows, true);
      }
    }
  }

  for (int32_t c = 0; c < build->finNum; ++c) {
    SMJoinColMap* pSecondCol = build->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pSecond->blk->pDataBlock, pSecondCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pRes->pDataBlock, pSecondCol->dstSlot);
    for (int32_t r = 0; r < firstRows; ++r) {
      colDataAssignNRows(pOutCol, currRows + r * secondRows, pInCol, pSecond->readIdx, secondRows);
    }
  }

  pRes->info.rows = append ? (pRes->info.rows + firstRows * secondRows) : firstRows * secondRows;
  return TSDB_CODE_SUCCESS;
}



bool mJoinHashGrpCart(SSDataBlock* pBlk, SMJoinGrpRows* probeGrp, bool append, SMJoinTableCtx* probe, SMJoinTableCtx* build) {
  int32_t rowsLeft = append ? (pBlk->info.capacity - pBlk->info.rows) : pBlk->info.capacity;
  if (rowsLeft <= 0) {
    return false;
  }
  
  int32_t buildGrpRows = taosArrayGetSize(build->pHashCurGrp);
  int32_t grpRows = buildGrpRows - build->grpRowIdx;
  if (grpRows <= 0 || build->grpRowIdx < 0) {
    build->grpRowIdx = -1;
    return true;
  }
  
  int32_t actRows = TMIN(grpRows, rowsLeft);
  int32_t currRows = append ? pBlk->info.rows : 0;

  for (int32_t c = 0; c < probe->finNum; ++c) {
    SMJoinColMap* pFirstCol = probe->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(probeGrp->blk->pDataBlock, pFirstCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pBlk->pDataBlock, pFirstCol->dstSlot);
    if (colDataIsNull_s(pInCol, probeGrp->readIdx)) {
      colDataSetNItemsNull(pOutCol, currRows, actRows);
    } else {
      colDataSetNItems(pOutCol, currRows, colDataGetData(pInCol, probeGrp->readIdx), actRows, true);
    }
  }

  for (int32_t c = 0; c < build->finNum; ++c) {
    SMJoinColMap* pSecondCol = build->finCols + c;
    SColumnInfoData* pOutCol = taosArrayGet(pBlk->pDataBlock, pSecondCol->dstSlot);
    for (int32_t r = 0; r < actRows; ++r) {
      SMJoinRowPos* pRow = taosArrayGet(build->pHashCurGrp, build->grpRowIdx + r);
      SColumnInfoData* pInCol = taosArrayGet(pRow->pBlk->pDataBlock, pSecondCol->srcSlot);
      colDataAssignNRows(pOutCol, currRows + r, pInCol, pRow->pos, 1);
    }
  }

  pBlk->info.rows += actRows;
  
  if (actRows == grpRows) {
    build->grpRowIdx = -1;
  } else {
    build->grpRowIdx += actRows;
  }
  
  if (actRows == rowsLeft) {
    return false;
  }

  return true;
}

int32_t mJoinAllocGrpRowBitmap(SMJoinTableCtx*        pTb) {
  int32_t grpNum = taosArrayGetSize(pTb->eqGrps);
  for (int32_t i = 0; i < grpNum; ++i) {
    SMJoinGrpRows* pGrp = (SMJoinGrpRows*)taosArrayGet(pTb->eqGrps, i);
    MJ_ERR_RET(mJoinGetRowBitmapOffset(pTb, pGrp->endIdx - pGrp->beginIdx + 1, &pGrp->rowBitmapOffset));
    pGrp->rowMatchNum = 0;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t mJoinProcessEqualGrp(SMJoinMergeCtx* pCtx, int64_t timestamp, bool lastBuildGrp) {
  SMJoinOperatorInfo* pJoin = pCtx->pJoin;

  pCtx->lastEqGrp = true;

  mJoinBuildEqGroups(pJoin->probe, timestamp, NULL, true);
  if (!lastBuildGrp) {
    mJoinRetrieveEqGrpRows(pJoin->pOperator, pJoin->build, timestamp);
  } else {
    pJoin->build->grpIdx = 0;
  }
  
  if (pCtx->hashCan && REACH_HJOIN_THRESHOLD(pJoin->probe, pJoin->build)) {
    if (!lastBuildGrp || !pCtx->hashJoin) {
      if (pJoin->build->rowBitmapSize > 0) {
        MJ_ERR_RET(mJoinCreateFullBuildTbHash(pJoin, pJoin->build));
      } else {
        MJ_ERR_RET(mJoinCreateBuildTbHash(pJoin, pJoin->build));
      }
    }

    if (pJoin->probe->newBlk) {
      MJ_ERR_RET(mJoinSetKeyColsData(pJoin->probe->blk, pJoin->probe));
      pJoin->probe->newBlk = false;
    }
    
    pCtx->hashJoin = true;    

    return (*pCtx->hashCartFp)(pCtx);
  }

  pCtx->hashJoin = false;    

  if (!lastBuildGrp && pJoin->build->rowBitmapSize > 0) {
    mJoinAllocGrpRowBitmap(pJoin->build);
  }
  
  return (*pCtx->mergeCartFp)(pCtx);
}

int32_t mJoinProcessLowerGrp(SMJoinMergeCtx* pCtx, SMJoinTableCtx* pTb, SColumnInfoData* pCol,  int64_t* probeTs, int64_t* buildTs) {
  pCtx->probeNEqGrp.blk = pTb->blk;
  pCtx->probeNEqGrp.beginIdx = pTb->blkRowIdx;
  pCtx->probeNEqGrp.readIdx = pCtx->probeNEqGrp.beginIdx;
  pCtx->probeNEqGrp.endIdx = pCtx->probeNEqGrp.beginIdx;
  
  while (++pTb->blkRowIdx < pTb->blk->info.rows) {
    MJOIN_GET_TB_CUR_TS(pCol, *probeTs, pTb);
    if (PROBE_TS_LOWER(pCtx->ascTs, *probeTs, *buildTs)) {
      pCtx->probeNEqGrp.endIdx = pTb->blkRowIdx;
      continue;
    }
    
    break;
  }

  return mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->probeNEqGrp, true);  
}

int32_t mJoinProcessGreaterGrp(SMJoinMergeCtx* pCtx, SMJoinTableCtx* pTb, SColumnInfoData* pCol,  int64_t* probeTs, int64_t* buildTs) {
  pCtx->buildNEqGrp.blk = pTb->blk;
  pCtx->buildNEqGrp.beginIdx = pTb->blkRowIdx;
  pCtx->buildNEqGrp.readIdx = pCtx->buildNEqGrp.beginIdx;
  pCtx->buildNEqGrp.endIdx = pCtx->buildNEqGrp.beginIdx;
  
  while (++pTb->blkRowIdx < pTb->blk->info.rows) {
    MJOIN_GET_TB_CUR_TS(pCol, *buildTs, pTb);
    if (PROBE_TS_GREATER(pCtx->ascTs, *probeTs, *buildTs)) {
      pCtx->buildNEqGrp.endIdx = pTb->blkRowIdx;
      continue;
    }
    
    break;
  }

  return mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->buildNEqGrp, false);  
}


SOperatorInfo** mJoinBuildDownstreams(SMJoinOperatorInfo* pInfo, SOperatorInfo** pDownstream) {
  SOperatorInfo** p = taosMemoryMalloc(2 * POINTER_BYTES);
  if (p) {
    p[0] = pDownstream[0];
    p[1] = pDownstream[0];
  }

  return p;
}

int32_t mJoinInitDownstreamInfo(SMJoinOperatorInfo* pInfo, SOperatorInfo** pDownstream, int32_t *numOfDownstream, bool *newDownstreams) {
  if (1 == *numOfDownstream) {
    *newDownstreams = true;
    pDownstream = mJoinBuildDownstreams(pInfo, pDownstream);
    if (NULL == pDownstream) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    *numOfDownstream = 2;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitPrimKeyInfo(SMJoinTableCtx* pTable, int32_t slotId) {
  pTable->primCol = taosMemoryMalloc(sizeof(SMJoinColInfo));
  if (NULL == pTable->primCol) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTable->primCol->srcSlot = slotId;

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitColsInfo(int32_t* colNum, int64_t* rowSize, SMJoinColInfo** pCols, SNodeList* pList) {
  *colNum = LIST_LENGTH(pList);
  
  *pCols = taosMemoryMalloc((*colNum) * sizeof(SMJoinColInfo));
  if (NULL == *pCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *rowSize = 0;
  
  int32_t i = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SColumnNode* pColNode = (SColumnNode*)pNode;
    (*pCols)[i].srcSlot = pColNode->slotId;
    (*pCols)[i].vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
    (*pCols)[i].bytes = pColNode->node.resType.bytes;
    *rowSize += pColNode->node.resType.bytes;
    ++i;
  }  

  return TSDB_CODE_SUCCESS;
}


static int32_t mJoinInitKeyColsInfo(SMJoinTableCtx* pTable, SNodeList* pList, bool allocKeyBuf) {
  int64_t rowSize = 0;
  MJ_ERR_RET(mJoinInitColsInfo(&pTable->keyNum, &rowSize, &pTable->keyCols, pList));

  if (pTable->keyNum > 1 || allocKeyBuf) {
    if (rowSize > 1) {
      pTable->keyNullSize = 1;
    } else {
      pTable->keyNullSize = 2;
    }

    pTable->keyBuf = taosMemoryMalloc(TMAX(rowSize, pTable->keyNullSize));
    if (NULL == pTable->keyBuf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}


static int32_t mJoinInitFinColsInfo(SMJoinTableCtx* pTable, SNodeList* pList) {
  pTable->finCols = taosMemoryMalloc(LIST_LENGTH(pList) * sizeof(SMJoinColMap));
  if (NULL == pTable->finCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  int32_t i = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    STargetNode* pTarget = (STargetNode*)pNode;
    SColumnNode* pColumn = (SColumnNode*)pTarget->pExpr;
    if (pColumn->dataBlockId == pTable->blkId) {
      pTable->finCols[i].srcSlot = pColumn->slotId;
      pTable->finCols[i].dstSlot = pTarget->slotId;
      pTable->finCols[i].bytes = pColumn->node.resType.bytes;
      pTable->finCols[i].vardata = IS_VAR_DATA_TYPE(pColumn->node.resType.type);
      ++i;
    }
  }  

  pTable->finNum = i;

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitTableInfo(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode, SOperatorInfo** pDownstream, int32_t idx, SQueryStat* pStat) {
  SMJoinTableCtx* pTable = &pJoin->tbs[idx];
  pTable->downStream = pDownstream[idx];
  pTable->blkId = pDownstream[idx]->resultDataBlockId;
  MJ_ERR_RET(mJoinInitPrimKeyInfo(pTable, (0 == idx) ? pJoinNode->leftPrimSlotId : pJoinNode->rightPrimSlotId));

  MJ_ERR_RET(mJoinInitKeyColsInfo(pTable, (0 == idx) ? pJoinNode->pEqLeft : pJoinNode->pEqRight, JOIN_TYPE_FULL == pJoin->joinType));
  MJ_ERR_RET(mJoinInitFinColsInfo(pTable, pJoinNode->pTargets));

  memcpy(&pTable->inputStat, pStat, sizeof(*pStat));

  pTable->eqGrps = taosArrayInit(8, sizeof(SMJoinGrpRows));
  taosArrayReserve(pTable->eqGrps, 1);
  
  if (E_JOIN_TB_BUILD == pTable->type) {
    pTable->createdBlks = taosArrayInit(8, POINTER_BYTES);
    pTable->pGrpArrays = taosArrayInit(32, POINTER_BYTES);
    pTable->pGrpHash = tSimpleHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    if (NULL == pTable->createdBlks || NULL == pTable->pGrpArrays || NULL == pTable->pGrpHash) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    if (pJoin->pFPreFilter && IS_FULL_OUTER_JOIN(pJoin->joinType, pJoin->subType)) {
      pTable->rowBitmapSize = MJOIN_ROW_BITMAP_SIZE;
      pTable->pRowBitmap = taosMemoryMalloc(pTable->rowBitmapSize);
      if (NULL == pTable->pRowBitmap) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }

    pTable->noKeepEqGrpRows = (JOIN_STYPE_ANTI == pJoin->subType && NULL == pJoin->pFPreFilter);
    pTable->multiEqGrpRows = !((JOIN_STYPE_SEMI == pJoin->subType || JOIN_STYPE_ANTI == pJoin->subType) && NULL == pJoin->pFPreFilter);
    pTable->multiRowsGrp = !((JOIN_STYPE_SEMI == pJoin->subType || JOIN_STYPE_ANTI == pJoin->subType) && NULL == pJoin->pPreFilter);
    if (JOIN_STYPE_ASOF == pJoin->subType) {
      pTable->eqRowLimit = pJoinNode->pJLimit ? ((SLimitNode*)pJoinNode->pJLimit)->limit : 1;
    }
  } else {
    pTable->multiEqGrpRows = true;
  }
  
  return TSDB_CODE_SUCCESS;
}

static void mJoinSetBuildAndProbeTable(SMJoinOperatorInfo* pInfo, SSortMergeJoinPhysiNode* pJoinNode) {
  int32_t buildIdx = 0;
  int32_t probeIdx = 1;

  pInfo->joinType = pJoinNode->joinType;
  pInfo->subType = pJoinNode->subType;
  
  switch (pInfo->joinType) {
    case JOIN_TYPE_INNER:
    case JOIN_TYPE_FULL:
      buildIdx = 1;
      probeIdx = 0;
      break;
    case JOIN_TYPE_LEFT:
      buildIdx = 1;
      probeIdx = 0;
      break;
    case JOIN_TYPE_RIGHT:
      buildIdx = 0;
      probeIdx = 1;
      break;
    default:
      break;
  } 
  
  pInfo->build = &pInfo->tbs[buildIdx];
  pInfo->probe = &pInfo->tbs[probeIdx];
  
  pInfo->build->downStreamIdx = buildIdx;
  pInfo->probe->downStreamIdx = probeIdx;

  pInfo->build->type = E_JOIN_TB_BUILD;
  pInfo->probe->type = E_JOIN_TB_PROBE;
}

static int32_t mJoinInitCtx(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode) {
  if ((JOIN_STYPE_ASOF == pJoin->subType && (ASOF_LOWER_ROW_INCLUDED(pJoinNode->asofOpType) || ASOF_GREATER_ROW_INCLUDED(pJoinNode->asofOpType))) 
       || JOIN_STYPE_WIN == pJoin->subType) {
    return mJoinInitWindowCtx(pJoin, pJoinNode);
  }
  
  return mJoinInitMergeCtx(pJoin, pJoinNode);
}

void mJoinSetDone(SOperatorInfo* pOperator) {
  setOperatorCompleted(pOperator);
  if (pOperator->pDownstreamGetParams) {
    freeOperatorParam(pOperator->pDownstreamGetParams[0], OP_GET_PARAM);
    freeOperatorParam(pOperator->pDownstreamGetParams[1], OP_GET_PARAM);
    pOperator->pDownstreamGetParams[0] = NULL;
    pOperator->pDownstreamGetParams[1] = NULL;
  }
}

bool mJoinRetrieveImpl(SMJoinOperatorInfo* pJoin, int32_t* pIdx, SSDataBlock** ppBlk, SMJoinTableCtx* pTb) {
  if (pTb->dsFetchDone) {
    return (NULL == (*ppBlk) || *pIdx >= (*ppBlk)->info.rows) ? false : true;
  }
  
  if (NULL == (*ppBlk) || *pIdx >= (*ppBlk)->info.rows) {
    (*ppBlk) = getNextBlockFromDownstreamRemain(pJoin->pOperator, pTb->downStreamIdx);
    pTb->dsInitDone = true;

    qDebug("%s merge join %s table got %" PRId64 " rows block", GET_TASKID(pJoin->pOperator->pTaskInfo), MJOIN_TBTYPE(pTb->type), (*ppBlk) ? (*ppBlk)->info.rows : 0);

    *pIdx = 0;
    if (NULL == (*ppBlk)) {
      pTb->dsFetchDone = true;
    } else {
      pTb->newBlk = true;
    }
    
    return ((*ppBlk) == NULL) ? false : true;
  }

  return true;
}

static void mJoinDestroyCreatedBlks(SArray* pCreatedBlks) {
  int32_t blkNum = taosArrayGetSize(pCreatedBlks);
  for (int32_t i = 0; i < blkNum; ++i) {
    blockDataDestroy(*(SSDataBlock**)TARRAY_GET_ELEM(pCreatedBlks, i));
  }
  taosArrayClear(pCreatedBlks);
}

int32_t mJoinGetRowBitmapOffset(SMJoinTableCtx* pTable, int32_t rowNum, int32_t *rowBitmapOffset) {
  int32_t bitmapLen = BitmapLen(rowNum);
  int64_t reqSize = pTable->rowBitmapOffset + bitmapLen;
  if (reqSize > pTable->rowBitmapSize) {
    int64_t newSize = reqSize * 1.1;
    pTable->pRowBitmap = taosMemoryRealloc(pTable->pRowBitmap, newSize);
    if (NULL == pTable->pRowBitmap) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pTable->rowBitmapSize = newSize;
  }

  memset(pTable->pRowBitmap + pTable->rowBitmapOffset, 0xFFFFFFFF, bitmapLen);
  
  *rowBitmapOffset = pTable->rowBitmapOffset;
  pTable->rowBitmapOffset += bitmapLen;

  return TSDB_CODE_SUCCESS;
}

void mJoinResetForBuildTable(SMJoinTableCtx* pTable) {
  pTable->grpTotalRows = 0;
  pTable->grpIdx = 0;
  pTable->eqRowNum = 0;
  mJoinDestroyCreatedBlks(pTable->createdBlks);
  taosArrayClear(pTable->eqGrps);
  if (pTable->rowBitmapSize > 0) {
    pTable->rowBitmapOffset = 1;
    memset(&pTable->nMatchCtx, 0, sizeof(pTable->nMatchCtx));
  }
}

int32_t mJoinBuildEqGroups(SMJoinTableCtx* pTable, int64_t timestamp, bool* wholeBlk, bool restart) {
  SColumnInfoData* pCol = taosArrayGet(pTable->blk->pDataBlock, pTable->primCol->srcSlot);
  SMJoinGrpRows* pGrp = NULL;

  if (*(int64_t*)colDataGetNumData(pCol, pTable->blkRowIdx) != timestamp) {
    return TSDB_CODE_SUCCESS;
  }

  if (restart) {
    mJoinResetForBuildTable(pTable);
  }

  bool keepGrp = true;
  pGrp = taosArrayReserve(pTable->eqGrps, 1);

  pGrp->beginIdx = pTable->blkRowIdx++;
  pGrp->readIdx = pGrp->beginIdx;
  pGrp->endIdx = pGrp->beginIdx;
  pGrp->readMatch = false;
  pGrp->blk = pTable->blk;

  char* pEndVal = colDataGetNumData(pCol, pTable->blk->info.rows - 1);
  if (timestamp == *(int64_t*)pEndVal) {
    if (pTable->multiEqGrpRows) {
      pGrp->endIdx = pTable->blk->info.rows - 1;
    } else {
      pGrp->endIdx = pGrp->beginIdx;
    }
    
    pTable->blkRowIdx = pTable->blk->info.rows;
  } else {
    for (; pTable->blkRowIdx < pTable->blk->info.rows; ++pTable->blkRowIdx) {
      char* pNextVal = colDataGetNumData(pCol, pTable->blkRowIdx);
      if (timestamp == *(int64_t*)pNextVal) {
        pGrp->endIdx++;
        continue;
      }

      if (!pTable->multiEqGrpRows) {
        pGrp->endIdx = pGrp->beginIdx;
      } else if (0 == pTable->eqRowLimit) {
        // DO NOTHING
      } else if (pTable->eqRowLimit == pTable->eqRowNum) {
        keepGrp = false;
      } else {
        int64_t rowNum = TMIN(pGrp->endIdx - pGrp->beginIdx + 1, pTable->eqRowLimit - pTable->eqRowNum);
        pGrp->endIdx = pGrp->beginIdx + rowNum - 1;
        pTable->eqRowNum += rowNum;
      }
      
      goto _return;
    }
  }

  if (wholeBlk && (pTable->multiEqGrpRows || restart)) {
    *wholeBlk = true;
    
    if (pTable->noKeepEqGrpRows || !keepGrp) {
      goto _return;
    }
    
    if (0 == pGrp->beginIdx && pTable->multiEqGrpRows && 0 == pTable->eqRowLimit) {
      pGrp->blk = createOneDataBlock(pTable->blk, true);
      taosArrayPush(pTable->createdBlks, &pGrp->blk);
    } else {
      if (!pTable->multiEqGrpRows) {
        pGrp->endIdx = pGrp->beginIdx;
      }

      int64_t rowNum = 0;
      if (!pTable->multiEqGrpRows) {
        rowNum = 1;
        pGrp->endIdx = pGrp->beginIdx;
      } else if (0 == pTable->eqRowLimit) {
        rowNum = pGrp->endIdx - pGrp->beginIdx + 1;
      } else if (pTable->eqRowLimit == pTable->eqRowNum) {
        keepGrp = false;
      } else {
        rowNum = TMIN(pGrp->endIdx - pGrp->beginIdx + 1, pTable->eqRowLimit - pTable->eqRowNum);
        pGrp->endIdx = pGrp->beginIdx + rowNum - 1;
      }

      if (keepGrp && rowNum > 0) {
        pTable->eqRowNum += rowNum;

        pGrp->blk = blockDataExtractBlock(pTable->blk, pGrp->beginIdx, rowNum);
        pGrp->endIdx -= pGrp->beginIdx;
        pGrp->beginIdx = 0;
        pGrp->readIdx = 0;
        taosArrayPush(pTable->createdBlks, &pGrp->blk);
      }
    }
    
  }

_return:

  if (pTable->noKeepEqGrpRows || !keepGrp || (!pTable->multiEqGrpRows && !restart)) {
    taosArrayPop(pTable->eqGrps);
  } else {
    pTable->grpTotalRows += pGrp->endIdx - pGrp->beginIdx + 1;  
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t mJoinRetrieveEqGrpRows(SOperatorInfo* pOperator, SMJoinTableCtx* pTable, int64_t timestamp) {
  bool wholeBlk = false;
  
  mJoinBuildEqGroups(pTable, timestamp, &wholeBlk, true);
  
  while (wholeBlk) {
    pTable->blk = getNextBlockFromDownstreamRemain(pOperator, pTable->downStreamIdx);
    qDebug("%s merge join %s table got block for same ts, rows:%" PRId64, GET_TASKID(pOperator->pTaskInfo), MJOIN_TBTYPE(pTable->type), pTable->blk ? pTable->blk->info.rows : 0);

    pTable->blkRowIdx = 0;

    if (NULL == pTable->blk) {
      pTable->dsFetchDone = true;
      break;
    }

    wholeBlk = false;
    mJoinBuildEqGroups(pTable, timestamp, &wholeBlk, false);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mJoinSetKeyColsData(SSDataBlock* pBlock, SMJoinTableCtx* pTable) {
  for (int32_t i = 0; i < pTable->keyNum; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pTable->keyCols[i].srcSlot);
    if (pTable->keyCols[i].vardata != IS_VAR_DATA_TYPE(pCol->info.type))  {
      qError("column type mismatch, idx:%d, slotId:%d, type:%d, vardata:%d", i, pTable->keyCols[i].srcSlot, pCol->info.type, pTable->keyCols[i].vardata);
      return TSDB_CODE_INVALID_PARA;
    }
    if (pTable->keyCols[i].bytes != pCol->info.bytes)  {
      qError("column bytes mismatch, idx:%d, slotId:%d, bytes:%d, %d", i, pTable->keyCols[i].srcSlot, pCol->info.bytes, pTable->keyCols[i].bytes);
      return TSDB_CODE_INVALID_PARA;
    }
    pTable->keyCols[i].data = pCol->pData;
    if (pTable->keyCols[i].vardata) {
      pTable->keyCols[i].offset = pCol->varmeta.offset;
    }
    pTable->keyCols[i].colData = pCol;
  }

  return TSDB_CODE_SUCCESS;
}

bool mJoinCopyKeyColsDataToBuf(SMJoinTableCtx* pTable, int32_t rowIdx, size_t *pBufLen) {
  char *pData = NULL;
  size_t bufLen = 0;
  
  if (1 == pTable->keyNum) {
    if (colDataIsNull_s(pTable->keyCols[0].colData, rowIdx)) {
      return true;
    }
    if (pTable->keyCols[0].vardata) {
      pData = pTable->keyCols[0].data + pTable->keyCols[0].offset[rowIdx];
      bufLen = varDataTLen(pData);
    } else {
      pData = pTable->keyCols[0].data + pTable->keyCols[0].bytes * rowIdx;
      bufLen = pTable->keyCols[0].bytes;
    }
    pTable->keyData = pData;
  } else {
    for (int32_t i = 0; i < pTable->keyNum; ++i) {
      if (colDataIsNull_s(pTable->keyCols[i].colData, rowIdx)) {
        return true;
      }
      if (pTable->keyCols[i].vardata) {
        pData = pTable->keyCols[i].data + pTable->keyCols[i].offset[rowIdx];
        memcpy(pTable->keyBuf + bufLen, pData, varDataTLen(pData));
        bufLen += varDataTLen(pData);
      } else {
        pData = pTable->keyCols[i].data + pTable->keyCols[i].bytes * rowIdx;
        memcpy(pTable->keyBuf + bufLen, pData, pTable->keyCols[i].bytes);
        bufLen += pTable->keyCols[i].bytes;
      }
    }
    pTable->keyData = pTable->keyBuf;
  }

  if (pBufLen) {
    *pBufLen = bufLen;
  }

  return false;
}

static int32_t mJoinGetAvailableGrpArray(SMJoinTableCtx* pTable, SArray** ppRes) {
  do {
    if (pTable->grpArrayIdx < taosArrayGetSize(pTable->pGrpArrays)) {
      *ppRes = taosArrayGetP(pTable->pGrpArrays, pTable->grpArrayIdx++);
      taosArrayClear(*ppRes);
      return TSDB_CODE_SUCCESS;
    }

    SArray* pNew = taosArrayInit(4, sizeof(SMJoinRowPos));
    if (NULL == pNew) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosArrayPush(pTable->pGrpArrays, &pNew);
  } while (true);

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinAddRowToHash(SMJoinOperatorInfo* pJoin, size_t keyLen, SSDataBlock* pBlock, int32_t rowIdx) {
  SMJoinTableCtx* pBuild = pJoin->build;
  SMJoinRowPos pos = {pBlock, rowIdx};
  SArray** pGrpRows = tSimpleHashGet(pBuild->pGrpHash, pBuild->keyData, keyLen);
  if (!pGrpRows) {
    SArray* pNewGrp = NULL;
    MJ_ERR_RET(mJoinGetAvailableGrpArray(pBuild, &pNewGrp));

    taosArrayPush(pNewGrp, &pos);
    tSimpleHashPut(pBuild->pGrpHash, pBuild->keyData, keyLen, &pNewGrp, POINTER_BYTES);
  } else if (pBuild->multiRowsGrp) {
    taosArrayPush(*pGrpRows, &pos);
  }

  return TSDB_CODE_SUCCESS;
}


static int32_t mJoinAddRowToFullHash(SMJoinOperatorInfo* pJoin, size_t keyLen, SSDataBlock* pBlock, int32_t rowIdx) {
  SMJoinTableCtx* pBuild = pJoin->build;
  SMJoinRowPos pos = {pBlock, rowIdx};
  SMJoinHashGrpRows* pGrpRows = (SMJoinHashGrpRows*)tSimpleHashGet(pBuild->pGrpHash, pBuild->keyData, keyLen);
  if (!pGrpRows) {
    SMJoinHashGrpRows pNewGrp = {0};
    MJ_ERR_RET(mJoinGetAvailableGrpArray(pBuild, &pNewGrp.pRows));

    taosArrayPush(pNewGrp.pRows, &pos);
    tSimpleHashPut(pBuild->pGrpHash, pBuild->keyData, keyLen, &pNewGrp, sizeof(pNewGrp));
  } else {
    taosArrayPush(pGrpRows->pRows, &pos);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mJoinCreateFullBuildTbHash(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pTable) {
  size_t bufLen = 0;

  tSimpleHashClear(pJoin->build->pGrpHash);
  pJoin->build->grpArrayIdx = 0;

  pJoin->build->grpRowIdx = -1;
  
  int32_t grpNum = taosArrayGetSize(pTable->eqGrps);
  for (int32_t g = 0; g < grpNum; ++g) {
    SMJoinGrpRows* pGrp = taosArrayGet(pTable->eqGrps, g);
    MJ_ERR_RET(mJoinSetKeyColsData(pGrp->blk, pTable));

    int32_t grpRows = GRP_REMAIN_ROWS(pGrp);
    for (int32_t r = 0; r < grpRows; ++r) {
      if (mJoinCopyKeyColsDataToBuf(pTable, pGrp->beginIdx + r, &bufLen)) {
        *(int16_t *)pTable->keyBuf = 0;
        pTable->keyData = pTable->keyBuf;
        bufLen = pTable->keyNullSize;
      }

      MJ_ERR_RET(mJoinAddRowToFullHash(pJoin, bufLen, pGrp->blk, pGrp->beginIdx + r));
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mJoinCreateBuildTbHash(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pTable) {
  size_t bufLen = 0;

  tSimpleHashClear(pJoin->build->pGrpHash);
  pJoin->build->grpArrayIdx = 0;

  pJoin->build->grpRowIdx = -1;
  
  int32_t grpNum = taosArrayGetSize(pTable->eqGrps);
  for (int32_t g = 0; g < grpNum; ++g) {
    SMJoinGrpRows* pGrp = taosArrayGet(pTable->eqGrps, g);
    MJ_ERR_RET(mJoinSetKeyColsData(pGrp->blk, pTable));

    int32_t grpRows = GRP_REMAIN_ROWS(pGrp);
    for (int32_t r = 0; r < grpRows; ++r) {
      if (mJoinCopyKeyColsDataToBuf(pTable, pGrp->beginIdx + r, &bufLen)) {
        continue;
      }

      MJ_ERR_RET(mJoinAddRowToHash(pJoin, bufLen, pGrp->blk, pGrp->beginIdx + r));
    }
  }

  return TSDB_CODE_SUCCESS;
}


void mJoinResetTableCtx(SMJoinTableCtx* pCtx) {
  pCtx->dsInitDone = false;
  pCtx->dsFetchDone = false;

  mJoinDestroyCreatedBlks(pCtx->createdBlks);
  tSimpleHashClear(pCtx->pGrpHash);
}

void mJoinResetMergeCtx(SMJoinMergeCtx* pCtx) {
  pCtx->grpRemains = false;
  pCtx->midRemains = false;
  pCtx->lastEqGrp = false;

  pCtx->lastEqTs = INT64_MIN;
  pCtx->hashJoin = false;
}

void mJoinResetCtx(SMJoinOperatorInfo* pJoin) {
  mJoinResetMergeCtx(&pJoin->ctx.mergeCtx);
}

void mJoinResetOperator(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;

  mJoinResetTableCtx(pJoin->build);
  mJoinResetTableCtx(pJoin->probe);

  mJoinResetCtx(pJoin);

  pOperator->status = OP_OPENED;
}

SSDataBlock* mJoinMainProcess(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    if (NULL == pOperator->pDownstreamGetParams || NULL == pOperator->pDownstreamGetParams[0] || NULL == pOperator->pDownstreamGetParams[1]) {
      qDebug("%s merge join done", GET_TASKID(pOperator->pTaskInfo));
      return NULL;
    } else {
      mJoinResetOperator(pOperator);
      qDebug("%s start new round merge join", GET_TASKID(pOperator->pTaskInfo));
    }
  }

  int64_t st = 0;
  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  SSDataBlock* pBlock = NULL;
  while (true) {
    pBlock = (*pJoin->joinFp)(pOperator);
    if (NULL == pBlock) {
      if (pJoin->errCode) {
        ASSERT(0);
        T_LONG_JMP(pOperator->pTaskInfo->env, pJoin->errCode);
      }
      break;
    }
    if (pJoin->pFinFilter != NULL) {
      doFilter(pBlock, pJoin->pFinFilter, NULL);
    }
    
    if (pBlock->info.rows > 0 || pOperator->status == OP_EXEC_DONE) {
      break;
    }
  }

  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }
  
  return (pBlock && pBlock->info.rows > 0) ? pBlock : NULL;
}

void destroyGrpArray(void* ppArray) {
  SArray* pArray = *(SArray**)ppArray;
  taosArrayDestroy(pArray);
}

void destroyMergeJoinTableCtx(SMJoinTableCtx* pTable) {
  mJoinDestroyCreatedBlks(pTable->createdBlks);
  taosArrayDestroy(pTable->createdBlks);
  tSimpleHashCleanup(pTable->pGrpHash);

  taosMemoryFree(pTable->primCol);
  taosMemoryFree(pTable->finCols);
  taosMemoryFree(pTable->keyCols);
  taosMemoryFree(pTable->keyBuf);
  taosMemoryFree(pTable->pRowBitmap);

  taosArrayDestroy(pTable->eqGrps);
  taosArrayDestroyEx(pTable->pGrpArrays, destroyGrpArray);
}

void destroyMergeJoinOperator(void* param) {
  SMJoinOperatorInfo* pJoin = (SMJoinOperatorInfo*)param;
  pJoin->ctx.mergeCtx.finBlk = blockDataDestroy(pJoin->ctx.mergeCtx.finBlk);
  pJoin->ctx.mergeCtx.midBlk = blockDataDestroy(pJoin->ctx.mergeCtx.midBlk);

  if (pJoin->pFPreFilter != NULL) {
    filterFreeInfo(pJoin->pFPreFilter);
    pJoin->pFPreFilter = NULL;
  }
  if (pJoin->pPreFilter != NULL) {
    filterFreeInfo(pJoin->pPreFilter);
    pJoin->pPreFilter = NULL;
  }
  if (pJoin->pFinFilter != NULL) {
    filterFreeInfo(pJoin->pFinFilter);
    pJoin->pFinFilter = NULL;
  }

  destroyMergeJoinTableCtx(pJoin->probe);
  destroyMergeJoinTableCtx(pJoin->build);

  taosMemoryFreeClear(pJoin);
}

int32_t mJoinHandleConds(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode) {
  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER: {
      SNode* pCond = NULL;
      if (pJoinNode->pFullOnCond != NULL) {
        if (pJoinNode->node.pConditions != NULL) {
          MJ_ERR_RET(mergeJoinConds(&pJoinNode->pFullOnCond, &pJoinNode->node.pConditions));
        }
        pCond = pJoinNode->pFullOnCond;
      } else if (pJoinNode->node.pConditions != NULL) {
        pCond = pJoinNode->node.pConditions;
      }
      
      MJ_ERR_RET(filterInitFromNode(pCond, &pJoin->pFinFilter, 0));
      break;
    }
    case JOIN_TYPE_LEFT:
    case JOIN_TYPE_RIGHT:
    case JOIN_TYPE_FULL:
      if (pJoinNode->pFullOnCond != NULL) {
        MJ_ERR_RET(filterInitFromNode(pJoinNode->pFullOnCond, &pJoin->pFPreFilter, 0));
      }
      if (pJoinNode->pColOnCond != NULL) {
        MJ_ERR_RET(filterInitFromNode(pJoinNode->pColOnCond, &pJoin->pPreFilter, 0));
      }
      if (pJoinNode->node.pConditions != NULL) {
        MJ_ERR_RET(filterInitFromNode(pJoinNode->node.pConditions, &pJoin->pFinFilter, 0));
      }
      break;
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mJoinSetImplFp(SMJoinOperatorInfo* pJoin) {
  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER:
      pJoin->joinFp = mInnerJoinDo;
      break;
    case JOIN_TYPE_LEFT:
    case JOIN_TYPE_RIGHT: {
      switch (pJoin->subType) {
        case JOIN_STYPE_OUTER:          
          pJoin->joinFp = mLeftJoinDo;
          break;
        case JOIN_STYPE_SEMI: 
          pJoin->joinFp = mSemiJoinDo;
          break;
        case JOIN_STYPE_ANTI:
          pJoin->joinFp = mAntiJoinDo;
          break;
        case JOIN_STYPE_WIN:
          //pJoin->joinFp = mWinJoinDo;
          break;
        default:
          break;
      }
      break;
    }      
    case JOIN_TYPE_FULL:
      pJoin->joinFp = mFullJoinDo;
      break;
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createMergeJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SSortMergeJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo) {
  SMJoinOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SMJoinOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  bool newDownstreams = false;
  
  int32_t code = TSDB_CODE_SUCCESS;
  if (pOperator == NULL || pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }

  pInfo->pOperator = pOperator;
  MJ_ERR_JRET(mJoinInitDownstreamInfo(pInfo, pDownstream, &numOfDownstream, &newDownstreams));

  setOperatorInfo(pOperator, "MergeJoinOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  mJoinSetBuildAndProbeTable(pInfo, pJoinNode);

  MJ_ERR_JRET(mJoinHandleConds(pInfo, pJoinNode));

  mJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 0, &pJoinNode->inputStat[0]);
  mJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 1, &pJoinNode->inputStat[1]);

  MJ_ERR_JRET(mJoinInitCtx(pInfo, pJoinNode));
  MJ_ERR_JRET(mJoinSetImplFp(pInfo));

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, mJoinMainProcess, NULL, destroyMergeJoinOperator, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  MJ_ERR_JRET(appendDownstream(pOperator, pDownstream, numOfDownstream));

  if (newDownstreams) {
    taosMemoryFree(pDownstream);
    pOperator->numOfRealDownstream = 1;
  } else {
    pOperator->numOfRealDownstream = 2;
  }
  
  return pOperator;

_return:
  if (pInfo != NULL) {
    destroyMergeJoinOperator(pInfo);
  }
  if (newDownstreams) {
    taosMemoryFree(pDownstream);
  }

  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}


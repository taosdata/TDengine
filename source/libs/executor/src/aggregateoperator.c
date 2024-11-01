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

#include "filter.h"
#include "function.h"
#include "os.h"
#include "querynodes.h"
#include "tfill.h"
#include "tname.h"

#include "executorInt.h"
#include "index.h"
#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "thash.h"
#include "ttypes.h"

typedef struct {
  bool    hasAgg;
  int32_t numOfRows;
  int32_t startOffset;
} SFunctionCtxStatus;

typedef struct SAggOperatorInfo {
  SOptrBasicInfo   binfo;
  SAggSupporter    aggSup;
  STableQueryInfo* current;
  uint64_t         groupId;
  SGroupResInfo    groupResInfo;
  SExprSupp        scalarExprSup;
  bool             groupKeyOptimized;
  bool             hasValidBlock;
  SSDataBlock*     pNewGroupBlock;
  bool             hasCountFunc;
  SOperatorInfo*   pOperator;
} SAggOperatorInfo;

static void destroyAggOperatorInfo(void* param);
static int32_t setExecutionContext(SOperatorInfo* pOperator, int32_t numOfOutput, uint64_t groupId);

static int32_t createDataBlockForEmptyInput(SOperatorInfo* pOperator, SSDataBlock** ppBlock);
static void    destroyDataBlockForEmptyInput(bool blockAllocated, SSDataBlock** ppBlock);

static int32_t doAggregateImpl(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx);
static int32_t getAggregateResultNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
static int32_t doInitAggInfoSup(SAggSupporter* pAggSup, SqlFunctionCtx* pCtx, int32_t numOfOutput, size_t keyBufSize,
                                const char* pKey);

static int32_t addNewResultRowBuf(SResultRow* pWindowRes, SDiskbasedBuf* pResultBuf, uint32_t size);

static int32_t doSetTableGroupOutputBuf(SOperatorInfo* pOperator, int32_t numOfOutput, uint64_t groupId);

static void functionCtxSave(SqlFunctionCtx* pCtx, SFunctionCtxStatus* pStatus);
static void functionCtxRestore(SqlFunctionCtx* pCtx, SFunctionCtxStatus* pStatus);

int32_t createAggregateOperatorInfo(SOperatorInfo* downstream, SAggPhysiNode* pAggNode, SExecTaskInfo* pTaskInfo,
                                    SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t    lino = 0;
  int32_t    code = 0;
  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;
  int32_t    numOfScalarExpr = 0;
  SExprInfo* pScalarExprInfo = NULL;

  SAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SAggOperatorInfo));
  SOperatorInfo*    pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->exprSupp.hasWindowOrGroup = false;

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pAggNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);

  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = createExprInfo(pAggNode->pAggFuncs, pAggNode->pGroupKeys, &pExprInfo, &num);
  TSDB_CHECK_CODE(code, lino, _error);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                               pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  TSDB_CHECK_CODE(code, lino, _error);

  if (pAggNode->pExprs != NULL) {
    code = createExprInfo(pAggNode->pExprs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    TSDB_CHECK_CODE(code, lino, _error);
  }

  code = initExprSupp(&pInfo->scalarExprSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
  TSDB_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)pAggNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  TSDB_CHECK_CODE(code, lino, _error);

  pInfo->binfo.mergeResultBlock = pAggNode->mergeDataBlock;
  pInfo->groupKeyOptimized = pAggNode->groupKeyOptimized;
  pInfo->groupId = UINT64_MAX;
  pInfo->binfo.inputTsOrder = pAggNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pAggNode->node.outputTsOrder;
  pInfo->hasCountFunc = pAggNode->hasCountLikeFunc;
  pInfo->pOperator = pOperator;

  setOperatorInfo(pOperator, "TableAggregate", QUERY_NODE_PHYSICAL_PLAN_HASH_AGG,
                  !pAggNode->node.forceCreateNonBlockingOptr, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, getAggregateResultNext, NULL, destroyAggOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pTableScanInfo = downstream->info;
    pTableScanInfo->base.pdInfo.pExprSup = &pOperator->exprSupp;
    pTableScanInfo->base.pdInfo.pAggSup = &pInfo->aggSup;
  }

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  if (pInfo != NULL) {
    destroyAggOperatorInfo(pInfo);
  }
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

void destroyAggOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }
  SAggOperatorInfo* pInfo = (SAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);

  if (pInfo->pOperator) {
    cleanupResultInfoWithoutHash(pInfo->pOperator->pTaskInfo, &pInfo->pOperator->exprSupp, pInfo->aggSup.pResultBuf,
                                 &pInfo->groupResInfo);
    pInfo->pOperator = NULL;
  }
  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarExprSup);
  cleanupGroupResInfo(&pInfo->groupResInfo);
  taosMemoryFreeClear(param);
}

/**
 * @brief get blocks from downstream and fill results into groupedRes after aggragation
 * @retval false if no more groups
 * @retval true if there could have new groups coming
 * @note if pOperator.blocking is true, scan all blocks from downstream, all groups are handled
 *       if false, fill results of ONE GROUP
 * */
static bool nextGroupedResult(SOperatorInfo* pOperator) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  SExecTaskInfo*    pTaskInfo = pOperator->pTaskInfo;
  SAggOperatorInfo* pAggInfo = pOperator->info;

  if (pOperator->blocking && pAggInfo->hasValidBlock) {
    return false;
  }

  SExprSupp*   pSup = &pOperator->exprSupp;
  int64_t      st = taosGetTimestampUs();
  int32_t      order = pAggInfo->binfo.inputTsOrder;
  SSDataBlock* pBlock = pAggInfo->pNewGroupBlock;

  if (pBlock) {
    pAggInfo->pNewGroupBlock = NULL;
    tSimpleHashClear(pAggInfo->aggSup.pResultRowHashTable);
    code = setExecutionContext(pOperator, pOperator->exprSupp.numOfExprs, pBlock->info.id.groupId);
    QUERY_CHECK_CODE(code, lino, _end);
    code = setInputDataBlock(pSup, pBlock, order, pBlock->info.scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doAggregateImpl(pOperator, pSup->pCtx);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  while (1) {
    bool blockAllocated = false;
    pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      if (!pAggInfo->hasValidBlock) {
        code = createDataBlockForEmptyInput(pOperator, &pBlock);
        QUERY_CHECK_CODE(code, lino, _end);

        if (pBlock == NULL) {
          break;
        }
        blockAllocated = true;
      } else {
        break;
      }
    }
    pAggInfo->hasValidBlock = true;
    pAggInfo->binfo.pRes->info.scanFlag = pBlock->info.scanFlag;

    // there is an scalar expression that needs to be calculated before apply the group aggregation.
    if (pAggInfo->scalarExprSup.pExprInfo != NULL && !blockAllocated) {
      SExprSupp* pSup1 = &pAggInfo->scalarExprSup;
      code = projectApplyFunctions(pSup1->pExprInfo, pBlock, pBlock, pSup1->pCtx, pSup1->numOfExprs, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        destroyDataBlockForEmptyInput(blockAllocated, &pBlock);
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }
    // if non-blocking mode and new group arrived, save the block and break
    if (!pOperator->blocking && pAggInfo->groupId != UINT64_MAX && pBlock->info.id.groupId != pAggInfo->groupId) {
      pAggInfo->pNewGroupBlock = pBlock;
      break;
    }
    // the pDataBlock are always the same one, no need to call this again
    code = setExecutionContext(pOperator, pOperator->exprSupp.numOfExprs, pBlock->info.id.groupId);
    if (code != TSDB_CODE_SUCCESS) {
      destroyDataBlockForEmptyInput(blockAllocated, &pBlock);
      T_LONG_JMP(pTaskInfo->env, code);
    }
    code = setInputDataBlock(pSup, pBlock, order, pBlock->info.scanFlag, true);
    if (code != TSDB_CODE_SUCCESS) {
      destroyDataBlockForEmptyInput(blockAllocated, &pBlock);
      T_LONG_JMP(pTaskInfo->env, code);
    }

    code = doAggregateImpl(pOperator, pSup->pCtx);
    if (code != TSDB_CODE_SUCCESS) {
      destroyDataBlockForEmptyInput(blockAllocated, &pBlock);
      T_LONG_JMP(pTaskInfo->env, code);
    }

    destroyDataBlockForEmptyInput(blockAllocated, &pBlock);
  }

  // the downstream operator may return with error code, so let's check the code before generating results.
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
  }

  code = initGroupedResultInfo(&pAggInfo->groupResInfo, pAggInfo->aggSup.pResultRowHashTable, 0);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return pBlock != NULL;
}

int32_t getAggregateResultNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  SAggOperatorInfo* pAggInfo = pOperator->info;
  SOptrBasicInfo*   pInfo = &pAggInfo->binfo;

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  bool           hasNewGroups = false;
  do {
    hasNewGroups = nextGroupedResult(pOperator);
    code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
    QUERY_CHECK_CODE(code, lino, _end);

    while (1) {
      doBuildResultDatablock(pOperator, pInfo, &pAggInfo->groupResInfo, pAggInfo->aggSup.pResultBuf);
      code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

      if (!hasRemainResults(&pAggInfo->groupResInfo)) {
        if (!hasNewGroups) setOperatorCompleted(pOperator);
        break;
      }

      if (pInfo->pRes->info.rows > 0) {
        break;
      }
    }
  } while (pInfo->pRes->info.rows == 0 && hasNewGroups);

  size_t rows = blockDataGetNumOfRows(pInfo->pRes);
  pOperator->resultInfo.totalRows += rows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  (*ppRes) = (rows == 0) ? NULL : pInfo->pRes;
  return code;
}

static SSDataBlock* getAggregateResult(SOperatorInfo* pOperator) {
  SSDataBlock* pRes = NULL;
  int32_t code = getAggregateResultNext(pOperator, &pRes);
  return pRes;
}

int32_t doAggregateImpl(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t k = 0; k < pOperator->exprSupp.numOfExprs; ++k) {
    if (functionNeedToExecute(&pCtx[k])) {
      // todo add a dummy function to avoid process check
      if (pCtx[k].fpSet.process == NULL) {
        continue;
      }

      if ((&pCtx[k])->input.pData[0] == NULL) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        qError("%s aggregate function error happens, input data is NULL.", GET_TASKID(pOperator->pTaskInfo));
      } else {
        code = pCtx[k].fpSet.process(&pCtx[k]);
      }

      if (code != TSDB_CODE_SUCCESS) {
        if (pCtx[k].fpSet.cleanup != NULL) {
          pCtx[k].fpSet.cleanup(&pCtx[k]);
        }
        qError("%s aggregate function error happens, code: %s", GET_TASKID(pOperator->pTaskInfo), tstrerror(code));
        return code;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t createDataBlockForEmptyInput(SOperatorInfo* pOperator, SSDataBlock** ppBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSDataBlock* pBlock = NULL;
  if (!tsCountAlwaysReturnValue) {
    return TSDB_CODE_SUCCESS;
  }

  SAggOperatorInfo* pAggInfo = pOperator->info;
  if (pAggInfo->groupKeyOptimized) {
    return TSDB_CODE_SUCCESS;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_PARTITION ||
      downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_SORT ||
      (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN &&
       ((STableScanInfo*)downstream->info)->hasGroupByTag == true)) {
    return TSDB_CODE_SUCCESS;
  }

  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;

  if (!pAggInfo->hasCountFunc) {
    return TSDB_CODE_SUCCESS;
  }

  code = createDataBlock(&pBlock);
  if (code) {
    return code;
  }

  pBlock->info.rows = 1;
  pBlock->info.capacity = 0;

  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    SColumnInfoData colInfo = {0};
    colInfo.hasNull = true;
    colInfo.info.type = TSDB_DATA_TYPE_NULL;
    colInfo.info.bytes = 1;

    SExprInfo* pOneExpr = &pOperator->exprSupp.pExprInfo[i];
    for (int32_t j = 0; j < pOneExpr->base.numOfParams; ++j) {
      SFunctParam* pFuncParam = &pOneExpr->base.pParam[j];
      if (pFuncParam->type == FUNC_PARAM_TYPE_COLUMN) {
        int32_t slotId = pFuncParam->pCol->slotId;
        int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
        if (slotId >= numOfCols) {
          code = taosArrayEnsureCap(pBlock->pDataBlock, slotId + 1);
          QUERY_CHECK_CODE(code, lino, _end);

          for (int32_t k = numOfCols; k < slotId + 1; ++k) {
            void* tmp = taosArrayPush(pBlock->pDataBlock, &colInfo);
            QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
          }
        }
      } else if (pFuncParam->type == FUNC_PARAM_TYPE_VALUE) {
        // do nothing
      }
    }
  }

  code = blockDataEnsureCapacity(pBlock, pBlock->info.rows);
  QUERY_CHECK_CODE(code, lino, _end);

  for (int32_t i = 0; i < blockDataGetNumOfCols(pBlock); ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, 0);
  }
  *ppBlock = pBlock;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
  return code;
}

void destroyDataBlockForEmptyInput(bool blockAllocated, SSDataBlock** ppBlock) {
  if (!blockAllocated) {
    return;
  }

  blockDataDestroy(*ppBlock);
  *ppBlock = NULL;
}

int32_t setExecutionContext(SOperatorInfo* pOperator, int32_t numOfOutput, uint64_t groupId) {
  int32_t           code = TSDB_CODE_SUCCESS;
  SAggOperatorInfo* pAggInfo = pOperator->info;
  if (pAggInfo->groupId != UINT64_MAX && pAggInfo->groupId == groupId) {
    return code;
  }

  code = doSetTableGroupOutputBuf(pOperator, numOfOutput, groupId);

  // record the current active group id
  pAggInfo->groupId = groupId;
  return code;
}

int32_t doSetTableGroupOutputBuf(SOperatorInfo* pOperator, int32_t numOfOutput, uint64_t groupId) {
  // for simple group by query without interval, all the tables belong to one group result.
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  SExecTaskInfo*    pTaskInfo = pOperator->pTaskInfo;
  SAggOperatorInfo* pAggInfo = pOperator->info;

  SResultRowInfo* pResultRowInfo = &pAggInfo->binfo.resultRowInfo;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;
  int32_t*        rowEntryInfoOffset = pOperator->exprSupp.rowEntryInfoOffset;

  SResultRow* pResultRow =
      doSetResultOutBufByKey(pAggInfo->aggSup.pResultBuf, pResultRowInfo, (char*)&groupId, sizeof(groupId), true,
                             groupId, pTaskInfo, false, &pAggInfo->aggSup, true);
  if (pResultRow == NULL || pTaskInfo->code != 0) {
    code = pTaskInfo->code;
    lino = __LINE__;
    goto _end;
  }
  /*
   * not assign result buffer yet, add new result buffer
   * all group belong to one result set, and each group result has different group id so set the id to be one
   */
  if (pResultRow->pageId == -1) {
    code = addNewResultRowBuf(pResultRow, pAggInfo->aggSup.pResultBuf, pAggInfo->binfo.pRes->info.rowSize);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  code = setResultRowInitCtx(pResultRow, pCtx, numOfOutput, rowEntryInfoOffset);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// a new buffer page for each table. Needs to opt this design
int32_t addNewResultRowBuf(SResultRow* pWindowRes, SDiskbasedBuf* pResultBuf, uint32_t size) {
  if (pWindowRes->pageId != -1) {
    return 0;
  }

  SFilePage* pData = NULL;

  // in the first scan, new space needed for results
  int32_t pageId = -1;
  SArray* list = getDataBufPagesIdList(pResultBuf);

  if (taosArrayGetSize(list) == 0) {
    pData = getNewBufPage(pResultBuf, &pageId);
    if (pData == NULL) {
      qError("failed to get buffer, code:%s", tstrerror(terrno));
      return terrno;
    }
    pData->num = sizeof(SFilePage);
  } else {
    SPageInfo* pi = getLastPageInfo(list);
    pData = getBufPage(pResultBuf, getPageId(pi));
    if (pData == NULL) {
      qError("failed to get buffer, code:%s", tstrerror(terrno));
      return terrno;
    }

    pageId = getPageId(pi);

    if (pData->num + size > getBufPageSize(pResultBuf)) {
      // release current page first, and prepare the next one
      releaseBufPageInfo(pResultBuf, pi);

      pData = getNewBufPage(pResultBuf, &pageId);
      if (pData == NULL) {
        qError("failed to get buffer, code:%s", tstrerror(terrno));
        return terrno;
      }
      pData->num = sizeof(SFilePage);
    }
  }

  if (pData == NULL) {
    return -1;
  }

  // set the number of rows in current disk page
  if (pWindowRes->pageId == -1) {  // not allocated yet, allocate new buffer
    pWindowRes->pageId = pageId;
    pWindowRes->offset = (int32_t)pData->num;

    pData->num += size;
  }

  return 0;
}

int32_t doInitAggInfoSup(SAggSupporter* pAggSup, SqlFunctionCtx* pCtx, int32_t numOfOutput, size_t keyBufSize,
                         const char* pKey) {
  int32_t code = 0;
  //  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);

  pAggSup->currentPageId = -1;
  pAggSup->resultRowSize = getResultRowSize(pCtx, numOfOutput);
  pAggSup->keyBuf = taosMemoryCalloc(1, keyBufSize + POINTER_BYTES + sizeof(int64_t));
  pAggSup->pResultRowHashTable = tSimpleHashInit(100, taosFastHash);

  if (pAggSup->keyBuf == NULL || pAggSup->pResultRowHashTable == NULL) {
    return terrno;
  }

  uint32_t defaultPgsz = 0;
  uint32_t defaultBufsz = 0;
  code = getBufferPgSize(pAggSup->resultRowSize, &defaultPgsz, &defaultBufsz);
  if (code) {
    qError("failed to get buff page size, rowSize:%d", pAggSup->resultRowSize);
    return code;
  }

  if (!osTempSpaceAvailable()) {
    code = TSDB_CODE_NO_DISKSPACE;
    qError("Init stream agg supporter failed since %s, key:%s, tempDir:%s", tstrerror(code), pKey, tsTempDir);
    return code;
  }

  code = createDiskbasedBuf(&pAggSup->pResultBuf, defaultPgsz, defaultBufsz, pKey, tsTempDir);
  if (code != TSDB_CODE_SUCCESS) {
    qError("Create agg result buf failed since %s, %s", tstrerror(code), pKey);
    return code;
  }

  return code;
}

void cleanupResultInfoInStream(SExecTaskInfo* pTaskInfo, void* pState, SExprSupp* pSup, SGroupResInfo* pGroupResInfo) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;
  int32_t         numOfExprs = pSup->numOfExprs;
  int32_t*        rowEntryOffset = pSup->rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pSup->pCtx;
  int32_t         numOfRows = getNumOfTotalRes(pGroupResInfo);
  bool            needCleanup = false;

  for (int32_t j = 0; j < numOfExprs; ++j) {
    needCleanup |= pCtx[j].needCleanup;
  }
  if (!needCleanup) {
    return;
  }
  
  for (int32_t i = pGroupResInfo->index; i < numOfRows; i += 1) {
    SResultWindowInfo* pWinInfo = taosArrayGet(pGroupResInfo->pRows, i);
    SRowBuffPos*       pPos = pWinInfo->pStatePos;
    SResultRow*        pRow = NULL;

    code = pAPI->stateStore.streamStateGetByPos(pState, pPos, (void**)&pRow);
    if (TSDB_CODE_SUCCESS != code) {
      qError("failed to get state by pos, code:%s, %s", tstrerror(code), GET_TASKID(pTaskInfo));
      continue;
    }

    for (int32_t j = 0; j < numOfExprs; ++j) {
      pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
      if (pCtx[j].fpSet.cleanup) {
        pCtx[j].fpSet.cleanup(&pCtx[j]);
      }
    }
  }
}

void cleanupResultInfoWithoutHash(SExecTaskInfo* pTaskInfo, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                                  SGroupResInfo* pGroupResInfo) {
  int32_t         numOfExprs = pSup->numOfExprs;
  int32_t*        rowEntryOffset = pSup->rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pSup->pCtx;
  int32_t         numOfRows = getNumOfTotalRes(pGroupResInfo);
  bool            needCleanup = false;

  for (int32_t j = 0; j < numOfExprs; ++j) {
    needCleanup |= pCtx[j].needCleanup;
  }
  if (!needCleanup) {
    return;
  }

  for (int32_t i = pGroupResInfo->index; i < numOfRows; i += 1) {
    SResultRow*        pRow = NULL;
    SResKeyPos*        pPos = taosArrayGetP(pGroupResInfo->pRows, i);
    SFilePage*         page = getBufPage(pBuf, pPos->pos.pageId);
    if (page == NULL) {
      qError("failed to get buffer, code:%s, %s", tstrerror(terrno), GET_TASKID(pTaskInfo));
      continue;
    }
    pRow = (SResultRow*)((char*)page + pPos->pos.offset);


    for (int32_t j = 0; j < numOfExprs; ++j) {
      pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
      if (pCtx[j].fpSet.cleanup) {
        pCtx[j].fpSet.cleanup(&pCtx[j]);
      }
    }
    releaseBufPage(pBuf, page);
  }
}

void cleanupResultInfo(SExecTaskInfo* pTaskInfo, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                       SGroupResInfo* pGroupResInfo, SSHashObj* pHashmap) {
  int32_t         numOfExprs = pSup->numOfExprs;
  int32_t*        rowEntryOffset = pSup->rowEntryInfoOffset;
  SqlFunctionCtx* pCtx = pSup->pCtx;
  bool            needCleanup = false;
  for (int32_t j = 0; j < numOfExprs; ++j) {
    needCleanup |= pCtx[j].needCleanup;
  }
  if (!needCleanup) {
    return;
  }

  // begin from last iter
  void*   pData = pGroupResInfo->dataPos;
  int32_t iter = pGroupResInfo->iter;
  while ((pData = tSimpleHashIterate(pHashmap, pData, &iter)) != NULL) {
    SResultRowPosition* pos = pData;

    SFilePage* page = getBufPage(pBuf, pos->pageId);
    if (page == NULL) {
      qError("failed to get buffer, code:%s, %s", tstrerror(terrno), GET_TASKID(pTaskInfo));
      continue;
    }

    SResultRow* pRow = (SResultRow*)((char*)page + pos->offset);

    for (int32_t j = 0; j < numOfExprs; ++j) {
      pCtx[j].resultInfo = getResultEntryInfo(pRow, j, rowEntryOffset);
      if (pCtx[j].fpSet.cleanup) {
        pCtx[j].fpSet.cleanup(&pCtx[j]);
      }
    }

    releaseBufPage(pBuf, page);
  }
}

void cleanupAggSup(SAggSupporter* pAggSup) {
  taosMemoryFreeClear(pAggSup->keyBuf);
  tSimpleHashCleanup(pAggSup->pResultRowHashTable);
  destroyDiskbasedBuf(pAggSup->pResultBuf);
}

int32_t initAggSup(SExprSupp* pSup, SAggSupporter* pAggSup, SExprInfo* pExprInfo, int32_t numOfCols, size_t keyBufSize,
                   const char* pkey, void* pState, SFunctionStateStore* pStore) {
  int32_t code = initExprSupp(pSup, pExprInfo, numOfCols, pStore);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = doInitAggInfoSup(pAggSup, pSup->pCtx, numOfCols, keyBufSize, pkey);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    pSup->pCtx[i].hasWindowOrGroup = pSup->hasWindowOrGroup;
    if (pState) {
      pSup->pCtx[i].saveHandle.pBuf = NULL;
      pSup->pCtx[i].saveHandle.pState = pState;
      pSup->pCtx[i].exprIdx = i;
    } else {
      pSup->pCtx[i].saveHandle.pBuf = pAggSup->pResultBuf;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t applyAggFunctionOnPartialTuples(SExecTaskInfo* taskInfo, SqlFunctionCtx* pCtx, SColumnInfoData* pTimeWindowData,
                                        int32_t offset, int32_t forwardStep, int32_t numOfTotal, int32_t numOfOutput) {
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t k = 0; k < numOfOutput; ++k) {
    // keep it temporarily
    SFunctionCtxStatus status = {0};
    functionCtxSave(&pCtx[k], &status);

    pCtx[k].input.startRowIndex = offset;
    pCtx[k].input.numOfRows = forwardStep;

    // not a whole block involved in query processing, statistics data can not be used
    // NOTE: the original value of isSet have been changed here
    if (pCtx[k].input.colDataSMAIsSet && forwardStep < numOfTotal) {
      pCtx[k].input.colDataSMAIsSet = false;
    }

    if (pCtx[k].isPseudoFunc) {
      SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(&pCtx[k]);

      char* p = GET_ROWCELL_INTERBUF(pEntryInfo);

      SColumnInfoData idata = {0};
      idata.info.type = TSDB_DATA_TYPE_BIGINT;
      idata.info.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
      idata.pData = p;

      SScalarParam out = {.columnData = &idata};
      SScalarParam tw = {.numOfRows = 5, .columnData = pTimeWindowData};
      code = pCtx[k].sfp.process(&tw, 1, &out);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        taskInfo->code = code;
        return code;
      }
      pEntryInfo->numOfRes = 1;
    } else {
      if (functionNeedToExecute(&pCtx[k]) && pCtx[k].fpSet.process != NULL) {
        if ((&pCtx[k])->input.pData[0] == NULL) {
          code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
          qError("%s apply functions error, input data is NULL.", GET_TASKID(taskInfo));
        } else {
          code = pCtx[k].fpSet.process(&pCtx[k]);
        }

        if (code != TSDB_CODE_SUCCESS) {
          if (pCtx[k].fpSet.cleanup != NULL) {
            pCtx[k].fpSet.cleanup(&pCtx[k]);
          }
          qError("%s apply functions error, code: %s", GET_TASKID(taskInfo), tstrerror(code));
          taskInfo->code = code;
          return code;
        }
      }

      // restore it
      functionCtxRestore(&pCtx[k], &status);
    }
  }
  return code;
}

void functionCtxSave(SqlFunctionCtx* pCtx, SFunctionCtxStatus* pStatus) {
  pStatus->hasAgg = pCtx->input.colDataSMAIsSet;
  pStatus->numOfRows = pCtx->input.numOfRows;
  pStatus->startOffset = pCtx->input.startRowIndex;
}

void functionCtxRestore(SqlFunctionCtx* pCtx, SFunctionCtxStatus* pStatus) {
  pCtx->input.colDataSMAIsSet = pStatus->hasAgg;
  pCtx->input.numOfRows = pStatus->numOfRows;
  pCtx->input.startRowIndex = pStatus->startOffset;
}

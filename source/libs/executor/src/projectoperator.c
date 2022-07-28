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

#include "executorimpl.h"
#include "functionMgt.h"

static SSDataBlock* doGenerateSourceData(SOperatorInfo* pOperator);
static SSDataBlock* doProjectOperation(SOperatorInfo* pOperator);
static SSDataBlock* doApplyIndefinitFunction(SOperatorInfo* pOperator);
static SArray*      setRowTsColumnOutputInfo(SqlFunctionCtx* pCtx, int32_t numOfCols);
static void setFunctionResultOutput(SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SAggSupporter* pSup, int32_t stage,
                                    int32_t numOfExprs);

static void destroyProjectOperatorInfo(void* param, int32_t numOfOutput) {
  if (NULL == param) {
    return;
  }

  SProjectOperatorInfo* pInfo = (SProjectOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  cleanupAggSup(&pInfo->aggSup);
  taosArrayDestroy(pInfo->pPseudoColInfo);

  blockDataDestroy(pInfo->pFinalRes);
  taosMemoryFreeClear(param);
}

static void destroyIndefinitOperatorInfo(void* param, int32_t numOfOutput) {
  SIndefOperatorInfo* pInfo = (SIndefOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);

  taosArrayDestroy(pInfo->pPseudoColInfo);
  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);

  taosMemoryFreeClear(param);
}

SOperatorInfo* createProjectOperatorInfo(SOperatorInfo* downstream, SProjectPhysiNode* pProjPhyNode,
                                         SExecTaskInfo* pTaskInfo) {
  SProjectOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SProjectOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pProjPhyNode->pProjections, NULL, &numOfCols);

  SSDataBlock* pResBlock = createResDataBlock(pProjPhyNode->node.pOutputDataBlockDesc);
  initLimitInfo(pProjPhyNode->node.pLimit, pProjPhyNode->node.pSlimit, &pInfo->limitInfo);

  pInfo->binfo.pRes = pResBlock;
  pInfo->pFinalRes  = createOneDataBlock(pResBlock, false);
  pInfo->pFilterNode = pProjPhyNode->node.pConditions;
  pInfo->mergeDataBlocks = pProjPhyNode->mergeDataBlock;

  // todo remove it soon

  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM) {
    pInfo->mergeDataBlocks = false;
  }


  int32_t numOfRows = 4096;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  // Make sure the size of SSDataBlock will never exceed the size of 2MB.
  int32_t TWOMB = 2 * 1024 * 1024;
  if (numOfRows * pResBlock->info.rowSize > TWOMB) {
    numOfRows = TWOMB / pResBlock->info.rowSize;
  }
  initResultSizeInfo(&pOperator->resultInfo, numOfRows);

  initAggInfo(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str);
  initBasicInfo(&pInfo->binfo, pResBlock);
  setFunctionResultOutput(pOperator, &pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, numOfCols);

  pInfo->pPseudoColInfo = setRowTsColumnOutputInfo(pOperator->exprSupp.pCtx, numOfCols);
  pOperator->name         = "ProjectOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_PROJECT;
  pOperator->blocking     = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->pTaskInfo    = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doProjectOperation, NULL, NULL,
                                         destroyProjectOperatorInfo, NULL, NULL, NULL);

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

  _error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

static int32_t discardGroupDataBlock(SSDataBlock* pBlock, SLimitInfo* pLimitInfo) {
  if (pLimitInfo->remainGroupOffset > 0) {
    // it is the first group
    if (pLimitInfo->currentGroupId == 0 || pLimitInfo->currentGroupId == pBlock->info.groupId) {
      pLimitInfo->currentGroupId = pBlock->info.groupId;
      return PROJECT_RETRIEVE_CONTINUE;
    } else if (pLimitInfo->currentGroupId != pBlock->info.groupId) {
      // now it is the data from a new group
      pLimitInfo->remainGroupOffset -= 1;
      pLimitInfo->currentGroupId = pBlock->info.groupId;

      // ignore data block in current group
      if (pLimitInfo->remainGroupOffset > 0) {
        return PROJECT_RETRIEVE_CONTINUE;
      }
    }

    // set current group id of the project operator
    pLimitInfo->currentGroupId = pBlock->info.groupId;
  }

  return PROJECT_RETRIEVE_DONE;
}

static int32_t setInfoForNewGroup(SSDataBlock* pBlock, SLimitInfo* pLimitInfo, SOperatorInfo* pOperator) {
  // remainGroupOffset == 0
  // here check for a new group data, we need to handle the data of the previous group.
  ASSERT(pLimitInfo->remainGroupOffset == 0 || pLimitInfo->remainGroupOffset == -1);

  if (pLimitInfo->currentGroupId != 0 && pLimitInfo->currentGroupId != pBlock->info.groupId) {
    pLimitInfo->numOfOutputGroups += 1;
    if ((pLimitInfo->slimit.limit > 0) && (pLimitInfo->slimit.limit <= pLimitInfo->numOfOutputGroups)) {
      doSetOperatorCompleted(pOperator);
      return PROJECT_RETRIEVE_DONE;
    }

    // reset the value for a new group data
    // existing rows that belongs to previous group.
    pLimitInfo->numOfOutputRows = 0;
    pLimitInfo->remainOffset = pLimitInfo->limit.offset;
  }

  return PROJECT_RETRIEVE_DONE;
}

static int32_t doIngroupLimitOffset(SLimitInfo* pLimitInfo, uint64_t groupId, SSDataBlock* pBlock, SOperatorInfo* pOperator) {
  // set current group id
  pLimitInfo->currentGroupId = groupId;

  if (pLimitInfo->remainOffset >= pBlock->info.rows) {
    pLimitInfo->remainOffset -= pBlock->info.rows;
    blockDataCleanup(pBlock);
    return PROJECT_RETRIEVE_CONTINUE;
  } else if (pLimitInfo->remainOffset < pBlock->info.rows && pLimitInfo->remainOffset > 0) {
    blockDataTrimFirstNRows(pBlock, pLimitInfo->remainOffset);
    pLimitInfo->remainOffset = 0;
  }

  // check for the limitation in each group
  if (pLimitInfo->limit.limit >= 0 &&
      pLimitInfo->numOfOutputRows + pBlock->info.rows >= pLimitInfo->limit.limit) {
    int32_t keepRows = (int32_t)(pLimitInfo->limit.limit - pLimitInfo->numOfOutputRows);
    blockDataKeepFirstNRows(pBlock, keepRows);
    if (pLimitInfo->slimit.limit > 0 && pLimitInfo->slimit.limit <= pLimitInfo->numOfOutputGroups) {
      doSetOperatorCompleted(pOperator);
    }
  }

  pLimitInfo->numOfOutputRows += pBlock->info.rows;
  return PROJECT_RETRIEVE_DONE;
}

void printDataBlock1(SSDataBlock* pBlock, const char* flag) {
  if (!pBlock || pBlock->info.rows == 0) {
    qDebug("===stream===printDataBlock: Block is Null or Empty");
    return;
  }
  char* pBuf = NULL;
  qDebug("%s", dumpBlockData(pBlock, flag, &pBuf));
  taosMemoryFreeClear(pBuf);
}

SSDataBlock* doProjectOperation(SOperatorInfo* pOperator) {
  SProjectOperatorInfo* pProjectInfo = pOperator->info;
  SOptrBasicInfo*       pInfo = &pProjectInfo->binfo;

  SExprSupp*   pSup = &pOperator->exprSupp;
  SSDataBlock* pRes = pInfo->pRes;
  SSDataBlock* pFinalRes = pProjectInfo->pFinalRes;

  blockDataCleanup(pFinalRes);

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    if (pTaskInfo->execModel == OPTR_EXEC_MODEL_QUEUE) {
      pOperator->status = OP_OPENED;
      return NULL;
    }

    return NULL;
  }

  int64_t st = 0;
  int32_t order = 0;
  int32_t scanFlag = 0;

  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  SLimitInfo* pLimitInfo = &pProjectInfo->limitInfo;

  if (downstream == NULL) {
    return doGenerateSourceData(pOperator);
  }

  while (1) {
    while (1) {
      blockDataCleanup(pRes);

      // The downstream exec may change the value of the newgroup, so use a local variable instead.
      SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
      if (pBlock == NULL) {
        doSetOperatorCompleted(pOperator);
        break;
      }

      // for stream interval
      if (pBlock->info.type == STREAM_RETRIEVE) {
        // printDataBlock1(pBlock, "project1");
        return pBlock;
      }

      int32_t status = discardGroupDataBlock(pBlock, pLimitInfo);
      if (status == PROJECT_RETRIEVE_CONTINUE) {
        continue;
      }

      setInfoForNewGroup(pBlock, pLimitInfo, pOperator);
      if (pOperator->status == OP_EXEC_DONE) {
        break;
      }

      // the pDataBlock are always the same one, no need to call this again
      int32_t code = getTableScanInfo(downstream, &order, &scanFlag);
      if (code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, code);
      }

      setInputDataBlock(pOperator, pSup->pCtx, pBlock, order, scanFlag, false);
      blockDataEnsureCapacity(pInfo->pRes, pInfo->pRes->info.rows + pBlock->info.rows);

      code = projectApplyFunctions(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs,
                                   pProjectInfo->pPseudoColInfo);
      if (code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, code);
      }

      status = doIngroupLimitOffset(pLimitInfo, pBlock->info.groupId, pInfo->pRes, pOperator);
      if (status == PROJECT_RETRIEVE_CONTINUE) {
        continue;
      }

      break;
    }

    if (pProjectInfo->mergeDataBlocks) {
      if (pRes->info.rows > 0) {
        pFinalRes->info.groupId = pRes->info.groupId;
        pFinalRes->info.version = pRes->info.version;

        // continue merge data, ignore the group id
        blockDataMerge(pFinalRes, pRes);
        if (pFinalRes->info.rows + pRes->info.rows <= pOperator->resultInfo.threshold) {
          continue;
        }
      }

      // do apply filter
      doFilter(pProjectInfo->pFilterNode, pFinalRes, NULL);

      // when apply the limit/offset for each group, pRes->info.rows may be 0, due to limit constraint.
      if (pFinalRes->info.rows > 0 || (pOperator->status == OP_EXEC_DONE)) {
        break;
      }
    } else {
      // do apply filter
      if (pRes->info.rows > 0) {
        doFilter(pProjectInfo->pFilterNode, pRes, NULL);
        if (pRes->info.rows == 0) {
          continue;
        }
      }

      // no results generated
      break;
    }
  }

  SSDataBlock* p = pProjectInfo->mergeDataBlocks ? pFinalRes : pRes;
  pOperator->resultInfo.totalRows += p->info.rows;

  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }
  
  // printDataBlock1(p, "project");
  return (p->info.rows > 0) ? p : NULL;
}

SOperatorInfo* createIndefinitOutputOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pNode,
                                                 SExecTaskInfo* pTaskInfo) {
  SIndefOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SIndefOperatorInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SExprSupp* pSup = &pOperator->exprSupp;

  SIndefRowsFuncPhysiNode* pPhyNode = (SIndefRowsFuncPhysiNode*)pNode;

  int32_t    numOfExpr = 0;
  SExprInfo* pExprInfo = createExprInfo(pPhyNode->pFuncs, NULL, &numOfExpr);

  if (pPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pSExpr = createExprInfo(pPhyNode->pExprs, NULL, &num);
    int32_t    code = initExprSupp(&pInfo->scalarSup, pSExpr, num);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  SSDataBlock* pResBlock = createResDataBlock(pPhyNode->node.pOutputDataBlockDesc);

  int32_t numOfRows = 4096;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  // Make sure the size of SSDataBlock will never exceed the size of 2MB.
  int32_t TWOMB = 2 * 1024 * 1024;
  if (numOfRows * pResBlock->info.rowSize > TWOMB) {
    numOfRows = TWOMB / pResBlock->info.rowSize;
  }

  initResultSizeInfo(&pOperator->resultInfo, numOfRows);

  initAggInfo(pSup, &pInfo->aggSup, pExprInfo, numOfExpr, keyBufSize, pTaskInfo->id.str);
  initBasicInfo(&pInfo->binfo, pResBlock);

  setFunctionResultOutput(pOperator, &pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, numOfExpr);

  pInfo->binfo.pRes = pResBlock;
  pInfo->pCondition = pPhyNode->node.pConditions;
  pInfo->pPseudoColInfo = setRowTsColumnOutputInfo(pSup->pCtx, numOfExpr);

  pOperator->name = "IndefinitOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doApplyIndefinitFunction, NULL, NULL,
                                         destroyIndefinitOperatorInfo, NULL, NULL, NULL);

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

  _error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

static void doHandleDataBlock(SOperatorInfo* pOperator, SSDataBlock* pBlock, SOperatorInfo* downstream,
                              SExecTaskInfo* pTaskInfo) {
  int32_t order = 0;
  int32_t scanFlag = 0;

  SIndefOperatorInfo* pIndefInfo = pOperator->info;
  SOptrBasicInfo*     pInfo = &pIndefInfo->binfo;
  SExprSupp*          pSup = &pOperator->exprSupp;

  // the pDataBlock are always the same one, no need to call this again
  int32_t code = getTableScanInfo(downstream, &order, &scanFlag);
  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pTaskInfo->env, code);
  }

  // there is an scalar expression that needs to be calculated before apply the group aggregation.
  SExprSupp* pScalarSup = &pIndefInfo->scalarSup;
  if (pScalarSup->pExprInfo != NULL) {
    code = projectApplyFunctions(pScalarSup->pExprInfo, pBlock, pBlock, pScalarSup->pCtx, pScalarSup->numOfExprs,
                                 pIndefInfo->pPseudoColInfo);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, code);
    }
  }

  setInputDataBlock(pOperator, pSup->pCtx, pBlock, order, scanFlag, false);
  blockDataEnsureCapacity(pInfo->pRes, pInfo->pRes->info.rows + pBlock->info.rows);

  code = projectApplyFunctions(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs,
                               pIndefInfo->pPseudoColInfo);
  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pTaskInfo->env, code);
  }
}

SSDataBlock* doApplyIndefinitFunction(SOperatorInfo* pOperator) {
  SIndefOperatorInfo* pIndefInfo = pOperator->info;
  SOptrBasicInfo*     pInfo = &pIndefInfo->binfo;
  SExprSupp*          pSup = &pOperator->exprSupp;

  SSDataBlock* pRes = pInfo->pRes;
  blockDataCleanup(pRes);

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  int64_t st = 0;

  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    // here we need to handle the existsed group results
    if (pIndefInfo->pNextGroupRes != NULL) {  // todo extract method
      for (int32_t k = 0; k < pSup->numOfExprs; ++k) {
        SqlFunctionCtx* pCtx = &pSup->pCtx[k];

        SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
        pResInfo->initialized = false;
        pCtx->pOutput = NULL;
      }

      doHandleDataBlock(pOperator, pIndefInfo->pNextGroupRes, downstream, pTaskInfo);
      pIndefInfo->pNextGroupRes = NULL;
    }

    if (pInfo->pRes->info.rows < pOperator->resultInfo.threshold) {
      while (1) {
        // The downstream exec may change the value of the newgroup, so use a local variable instead.
        SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
        if (pBlock == NULL) {
          doSetOperatorCompleted(pOperator);
          break;
        }

        if (pIndefInfo->groupId == 0 && pBlock->info.groupId != 0) {
          pIndefInfo->groupId = pBlock->info.groupId;  // this is the initial group result
        } else {
          if (pIndefInfo->groupId != pBlock->info.groupId) {  // reset output buffer and computing status
            pIndefInfo->groupId = pBlock->info.groupId;
            pIndefInfo->pNextGroupRes = pBlock;
            break;
          }
        }

        doHandleDataBlock(pOperator, pBlock, downstream, pTaskInfo);
        if (pInfo->pRes->info.rows >= pOperator->resultInfo.threshold) {
          break;
        }
      }
    }

    doFilter(pIndefInfo->pCondition, pInfo->pRes, NULL);
    size_t rows = pInfo->pRes->info.rows;
    if (rows > 0 || pOperator->status == OP_EXEC_DONE) {
      break;
    } else {
      blockDataCleanup(pInfo->pRes);
    }
  }

  size_t rows = pInfo->pRes->info.rows;
  pOperator->resultInfo.totalRows += rows;

  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }

  return (rows > 0) ? pInfo->pRes : NULL;
}

void initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size) {
  for (int32_t j = 0; j < size; ++j) {
    struct SResultRowEntryInfo* pResInfo = GET_RES_INFO(&pCtx[j]);
    if (isRowEntryInitialized(pResInfo) || fmIsPseudoColumnFunc(pCtx[j].functionId) || pCtx[j].functionId == -1 ||
        fmIsScalarFunc(pCtx[j].functionId)) {
      continue;
    }

    pCtx[j].fpSet.init(&pCtx[j], pCtx[j].resultInfo);
  }
}

/*
 * The start of each column SResultRowEntryInfo is denote by RowCellInfoOffset.
 * Note that in case of top/bottom query, the whole multiple rows of result is treated as only one row of results.
 * +------------+-----------------result column 1------------+------------------result column 2-----------+
 * | SResultRow | SResultRowEntryInfo | intermediate buffer1 | SResultRowEntryInfo | intermediate buffer 2|
 * +------------+--------------------------------------------+--------------------------------------------+
 *           offset[0]                                  offset[1]                                   offset[2]
 */
// TODO refactor: some function move away
void setFunctionResultOutput(SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SAggSupporter* pSup, int32_t stage,
                             int32_t numOfExprs) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;
  int32_t*        rowEntryInfoOffset = pOperator->exprSupp.rowEntryInfoOffset;

  SResultRowInfo* pResultRowInfo = &pInfo->resultRowInfo;
  initResultRowInfo(pResultRowInfo);

  int64_t     tid = 0;
  int64_t     groupId = 0;
  SResultRow* pRow = doSetResultOutBufByKey(pSup->pResultBuf, pResultRowInfo, (char*)&tid, sizeof(tid), true, groupId,
                                            pTaskInfo, false, pSup);

  for (int32_t i = 0; i < numOfExprs; ++i) {
    struct SResultRowEntryInfo* pEntry = getResultEntryInfo(pRow, i, rowEntryInfoOffset);
    cleanupResultRowEntry(pEntry);

    pCtx[i].resultInfo = pEntry;
    pCtx[i].scanFlag = stage;
  }

  initCtxOutputBuffer(pCtx, numOfExprs);
}

SArray* setRowTsColumnOutputInfo(SqlFunctionCtx* pCtx, int32_t numOfCols) {
  SArray* pList = taosArrayInit(4, sizeof(int32_t));
  for (int32_t i = 0; i < numOfCols; ++i) {
    if (fmIsPseudoColumnFunc(pCtx[i].functionId)) {
      taosArrayPush(pList, &i);
    }
  }

  return pList;
}

SSDataBlock* doGenerateSourceData(SOperatorInfo* pOperator) {
  SProjectOperatorInfo* pProjectInfo = pOperator->info;

  SExprSupp*   pSup = &pOperator->exprSupp;
  SSDataBlock* pRes = pProjectInfo->binfo.pRes;

  blockDataEnsureCapacity(pRes, pOperator->resultInfo.capacity);
  SExprInfo* pExpr = pSup->pExprInfo;

  int64_t st = taosGetTimestampUs();

  for (int32_t k = 0; k < pSup->numOfExprs; ++k) {
    int32_t outputSlotId = pExpr[k].base.resSchema.slotId;

    ASSERT(pExpr[k].pExpr->nodeType == QUERY_NODE_VALUE);
    SColumnInfoData* pColInfoData = taosArrayGet(pRes->pDataBlock, outputSlotId);

    int32_t type = pExpr[k].base.pParam[0].param.nType;
    if (TSDB_DATA_TYPE_NULL == type) {
      colDataAppendNNULL(pColInfoData, 0, 1);
    } else {
      colDataAppend(pColInfoData, 0, taosVariantGet(&pExpr[k].base.pParam[0].param, type), false);
    }
  }

  pRes->info.rows = 1;
  doFilter(pProjectInfo->pFilterNode, pRes, NULL);

  /*int32_t status = */doIngroupLimitOffset(&pProjectInfo->limitInfo, 0, pRes, pOperator);

  pOperator->resultInfo.totalRows += pRes->info.rows;

  doSetOperatorCompleted(pOperator);
  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }

  return (pRes->info.rows > 0) ? pRes : NULL;
}

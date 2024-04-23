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
#include "functionMgt.h"
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"

typedef struct SProjectOperatorInfo {
  SOptrBasicInfo binfo;
  SAggSupporter  aggSup;
  SArray*        pPseudoColInfo;
  SLimitInfo     limitInfo;
  bool           mergeDataBlocks;
  SSDataBlock*   pFinalRes;
  bool           inputIgnoreGroup;
} SProjectOperatorInfo;

typedef struct SIndefOperatorInfo {
  SOptrBasicInfo binfo;
  SAggSupporter  aggSup;
  SArray*        pPseudoColInfo;
  SExprSupp      scalarSup;
  uint64_t       groupId;
  SSDataBlock*   pNextGroupRes;
} SIndefOperatorInfo;

static int32_t      doGenerateSourceData(SOperatorInfo* pOperator);
static SSDataBlock* doProjectOperation(SOperatorInfo* pOperator);
static SSDataBlock* doApplyIndefinitFunction(SOperatorInfo* pOperator);
static SArray*      setRowTsColumnOutputInfo(SqlFunctionCtx* pCtx, int32_t numOfCols);
static void setFunctionResultOutput(SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SAggSupporter* pSup, int32_t stage,
                                    int32_t numOfExprs);

static void destroyProjectOperatorInfo(void* param) {
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

static void destroyIndefinitOperatorInfo(void* param) {
  SIndefOperatorInfo* pInfo = (SIndefOperatorInfo*)param;
  if (pInfo == NULL) {
    return;
  }

  cleanupBasicInfo(&pInfo->binfo);
  taosArrayDestroy(pInfo->pPseudoColInfo);
  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);

  taosMemoryFreeClear(param);
}

void streamOperatorReleaseState(SOperatorInfo* pOperator) {
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.releaseStreamStateFn) {
    downstream->fpSet.releaseStreamStateFn(downstream);
  }
}

void streamOperatorReloadState(SOperatorInfo* pOperator) {
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
}

SOperatorInfo* createProjectOperatorInfo(SOperatorInfo* downstream, SProjectPhysiNode* pProjPhyNode,
                                         SExecTaskInfo* pTaskInfo) {
  int32_t               code = TSDB_CODE_SUCCESS;
  SProjectOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SProjectOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pProjPhyNode->pProjections, NULL, &numOfCols);

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pProjPhyNode->node.pOutputDataBlockDesc);
  initLimitInfo(pProjPhyNode->node.pLimit, pProjPhyNode->node.pSlimit, &pInfo->limitInfo);

  pInfo->binfo.pRes = pResBlock;
  pInfo->pFinalRes = createOneDataBlock(pResBlock, false);
  pInfo->binfo.inputTsOrder = pProjPhyNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pProjPhyNode->node.outputTsOrder;
  pInfo->inputIgnoreGroup = pProjPhyNode->inputIgnoreGroup;
  
  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM || pTaskInfo->execModel == OPTR_EXEC_MODEL_QUEUE) {
    pInfo->mergeDataBlocks = false;
  } else {
    if (!pProjPhyNode->ignoreGroupId) {
      pInfo->mergeDataBlocks = false;
    } else {
      pInfo->mergeDataBlocks = pProjPhyNode->mergeDataBlock;
    }
  }

  int32_t numOfRows = 4096;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  // Make sure the size of SSDataBlock will never exceed the size of 2MB.
  int32_t TWOMB = 2 * 1024 * 1024;
  if (numOfRows * pResBlock->info.rowSize > TWOMB) {
    numOfRows = TWOMB / pResBlock->info.rowSize;
  }

  initResultSizeInfo(&pOperator->resultInfo, numOfRows);
  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initBasicInfo(&pInfo->binfo, pResBlock);
  setFunctionResultOutput(pOperator, &pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, numOfCols);

  code = filterInitFromNode((SNode*)pProjPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pPseudoColInfo = setRowTsColumnOutputInfo(pOperator->exprSupp.pCtx, numOfCols);

  setOperatorInfo(pOperator, "ProjectOperator", QUERY_NODE_PHYSICAL_PLAN_PROJECT, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doProjectOperation, NULL, destroyProjectOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
   setOperatorStreamStateFn(pOperator, streamOperatorReleaseState, streamOperatorReloadState);

  if (NULL != downstream) {
    code = appendDownstream(pOperator, &downstream, 1);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  return pOperator;

_error:
  destroyProjectOperatorInfo(pInfo);
  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

static int32_t discardGroupDataBlock(SSDataBlock* pBlock, SLimitInfo* pLimitInfo) {
  if (pLimitInfo->remainGroupOffset > 0) {
    // it is the first group
    if (pLimitInfo->currentGroupId == 0 || pLimitInfo->currentGroupId == pBlock->info.id.groupId) {
      pLimitInfo->currentGroupId = pBlock->info.id.groupId;
      return PROJECT_RETRIEVE_CONTINUE;
    } else if (pLimitInfo->currentGroupId != pBlock->info.id.groupId) {
      // now it is the data from a new group
      pLimitInfo->remainGroupOffset -= 1;
      pLimitInfo->currentGroupId = pBlock->info.id.groupId;

      // ignore data block in current group
      if (pLimitInfo->remainGroupOffset > 0) {
        return PROJECT_RETRIEVE_CONTINUE;
      }

      pLimitInfo->currentGroupId = 0;
    }
  }

  return PROJECT_RETRIEVE_DONE;
}

static int32_t setInfoForNewGroup(SSDataBlock* pBlock, SLimitInfo* pLimitInfo, SOperatorInfo* pOperator) {
  // remainGroupOffset == 0
  // here check for a new group data, we need to handle the data of the previous group.
  ASSERT(pLimitInfo->remainGroupOffset == 0 || pLimitInfo->remainGroupOffset == -1);

  bool newGroup = false;
  if (0 == pBlock->info.id.groupId) {
    pLimitInfo->numOfOutputGroups = 1;
  } else if (pLimitInfo->currentGroupId != pBlock->info.id.groupId) {
    pLimitInfo->currentGroupId = pBlock->info.id.groupId;
    pLimitInfo->numOfOutputGroups += 1;
    newGroup = true;
  } else {
    return PROJECT_RETRIEVE_CONTINUE;
  }

  if ((pLimitInfo->slimit.limit >= 0) && (pLimitInfo->slimit.limit < pLimitInfo->numOfOutputGroups)) {
    setOperatorCompleted(pOperator);
    return PROJECT_RETRIEVE_DONE;
  }

  // reset the value for a new group data
  // existing rows that belongs to previous group.
  if (newGroup) {
    resetLimitInfoForNextGroup(pLimitInfo);
  }

  return PROJECT_RETRIEVE_CONTINUE;
}

// todo refactor
static int32_t doIngroupLimitOffset(SLimitInfo* pLimitInfo, uint64_t groupId, SSDataBlock* pBlock,
                                    SOperatorInfo* pOperator) {
  // set current group id
  pLimitInfo->currentGroupId = groupId;
  bool limitReached = applyLimitOffset(pLimitInfo, pBlock, pOperator->pTaskInfo);
  if (pBlock->info.rows == 0) {
    return PROJECT_RETRIEVE_CONTINUE;
  } else {
    if (limitReached && (pLimitInfo->slimit.limit >= 0 && pLimitInfo->slimit.limit <= pLimitInfo->numOfOutputGroups)) {
      setOperatorCompleted(pOperator);
    } else if (limitReached && groupId == 0) {
      setOperatorCompleted(pOperator);
    }
  }

  return PROJECT_RETRIEVE_DONE;
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
    return NULL;
  }

  int64_t st = 0;
  int32_t order = pInfo->inputTsOrder;
  int32_t scanFlag = 0;
  int32_t code = TSDB_CODE_SUCCESS;

  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  SOperatorInfo* downstream = pOperator->numOfDownstream > 0 ? pOperator->pDownstream[0] : NULL;
  SLimitInfo*    pLimitInfo = &pProjectInfo->limitInfo;

  if (downstream == NULL) {
    code = doGenerateSourceData(pOperator);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    return (pRes->info.rows > 0) ? pRes : NULL;
  }

  while (1) {
    while (1) {
      blockDataCleanup(pRes);

      // The downstream exec may change the value of the newgroup, so use a local variable instead.
      SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
      if (pBlock == NULL) {
        qDebug("set op close, exec %d, status %d rows %" PRId64 , pTaskInfo->execModel, pOperator->status, pFinalRes->info.rows);
        setOperatorCompleted(pOperator);
        break;
      }
//      if (pTaskInfo->execModel == OPTR_EXEC_MODEL_QUEUE) {
//        qDebug("set status recv");
//        pOperator->status = OP_EXEC_RECV;
//      }

      // for stream interval
      if (pBlock->info.type == STREAM_RETRIEVE || pBlock->info.type == STREAM_DELETE_RESULT ||
          pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_CREATE_CHILD_TABLE ||
          pBlock->info.type == STREAM_CHECKPOINT) {
        return pBlock;
      }

      if (pProjectInfo->inputIgnoreGroup) {
        pBlock->info.id.groupId = 0;
      }

      int32_t status = discardGroupDataBlock(pBlock, pLimitInfo);
      if (status == PROJECT_RETRIEVE_CONTINUE) {
        continue;
      }

      setInfoForNewGroup(pBlock, pLimitInfo, pOperator);
      if (pOperator->status == OP_EXEC_DONE) {
        break;
      }

      if (pProjectInfo->mergeDataBlocks) {
        pFinalRes->info.scanFlag = scanFlag = pBlock->info.scanFlag;
      } else {
        pRes->info.scanFlag = scanFlag = pBlock->info.scanFlag;
      }

      setInputDataBlock(pSup, pBlock, order, scanFlag, false);
      blockDataEnsureCapacity(pInfo->pRes, pInfo->pRes->info.rows + pBlock->info.rows);

      code = projectApplyFunctions(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs,
                                   pProjectInfo->pPseudoColInfo);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }

      status = doIngroupLimitOffset(pLimitInfo, pBlock->info.id.groupId, pInfo->pRes, pOperator);
      if (status == PROJECT_RETRIEVE_CONTINUE) {
        continue;
      }

      break;
    }

    if (pProjectInfo->mergeDataBlocks) {
      if (pRes->info.rows > 0) {
        pFinalRes->info.id.groupId = 0;  // clear groupId
        pFinalRes->info.version = pRes->info.version;

        // continue merge data, ignore the group id
        blockDataMerge(pFinalRes, pRes);
        if (pFinalRes->info.rows + pRes->info.rows <= pOperator->resultInfo.threshold) {
          continue;
        }
      }

      // do apply filter
      doFilter(pFinalRes, pOperator->exprSupp.pFilterInfo, NULL);

      // when apply the limit/offset for each group, pRes->info.rows may be 0, due to limit constraint.
      if (pFinalRes->info.rows > 0 || (pOperator->status == OP_EXEC_DONE)) {
        qDebug("project return %" PRId64 " rows, status %d", pFinalRes->info.rows, pOperator->status);
        break;
      }
    } else {
      // do apply filter
      if (pRes->info.rows > 0) {
        doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
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
  p->info.dataLoad = 1;

  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }

  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM) {
    printDataBlock(p, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
  }

  return (p->info.rows > 0) ? p : NULL;
}

SOperatorInfo* createIndefinitOutputOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pNode,
                                                 SExecTaskInfo* pTaskInfo) {
  SIndefOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SIndefOperatorInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;

  SExprSupp* pSup = &pOperator->exprSupp;

  SIndefRowsFuncPhysiNode* pPhyNode = (SIndefRowsFuncPhysiNode*)pNode;

  int32_t    numOfExpr = 0;
  SExprInfo* pExprInfo = createExprInfo(pPhyNode->pFuncs, NULL, &numOfExpr);

  if (pPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pSExpr = createExprInfo(pPhyNode->pExprs, NULL, &num);
    int32_t    code = initExprSupp(&pInfo->scalarSup, pSExpr, num, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->node.pOutputDataBlockDesc);

  int32_t numOfRows = 4096;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  // Make sure the size of SSDataBlock will never exceed the size of 2MB.
  int32_t TWOMB = 2 * 1024 * 1024;
  if (numOfRows * pResBlock->info.rowSize > TWOMB) {
    numOfRows = TWOMB / pResBlock->info.rowSize;
  }

  initBasicInfo(&pInfo->binfo, pResBlock);
  initResultSizeInfo(&pOperator->resultInfo, numOfRows);
  blockDataEnsureCapacity(pResBlock, numOfRows);

  int32_t code = initAggSup(pSup, &pInfo->aggSup, pExprInfo, numOfExpr, keyBufSize, pTaskInfo->id.str,
                            pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  setFunctionResultOutput(pOperator, &pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, numOfExpr);
  code = filterInitFromNode((SNode*)pPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->binfo.pRes = pResBlock;
  pInfo->binfo.inputTsOrder = pNode->inputTsOrder;
  pInfo->binfo.outputTsOrder = pNode->outputTsOrder;
  pInfo->pPseudoColInfo = setRowTsColumnOutputInfo(pSup->pCtx, numOfExpr);

  setOperatorInfo(pOperator, "IndefinitOperator", QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doApplyIndefinitFunction, NULL, destroyIndefinitOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyIndefinitOperatorInfo(pInfo);
  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

static void doHandleDataBlock(SOperatorInfo* pOperator, SSDataBlock* pBlock, SOperatorInfo* downstream,
                              SExecTaskInfo* pTaskInfo) {
  SIndefOperatorInfo* pIndefInfo = pOperator->info;
  SOptrBasicInfo*     pInfo = &pIndefInfo->binfo;
  SExprSupp*          pSup = &pOperator->exprSupp;

  int32_t order = pInfo->inputTsOrder;
  int32_t scanFlag = pBlock->info.scanFlag;
  int32_t code = TSDB_CODE_SUCCESS;

  // there is an scalar expression that needs to be calculated before apply the group aggregation.
  SExprSupp* pScalarSup = &pIndefInfo->scalarSup;
  if (pScalarSup->pExprInfo != NULL) {
    code = projectApplyFunctions(pScalarSup->pExprInfo, pBlock, pBlock, pScalarSup->pCtx, pScalarSup->numOfExprs,
                                 pIndefInfo->pPseudoColInfo);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  setInputDataBlock(pSup, pBlock, order, scanFlag, false);
  blockDataEnsureCapacity(pInfo->pRes, pInfo->pRes->info.rows + pBlock->info.rows);

  code = projectApplyFunctions(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs,
                               pIndefInfo->pPseudoColInfo);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
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
        SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
        if (pBlock == NULL) {
          setOperatorCompleted(pOperator);
          break;
        }
        pInfo->pRes->info.scanFlag = pBlock->info.scanFlag;

        if (pIndefInfo->groupId == 0 && pBlock->info.id.groupId != 0) {
          pIndefInfo->groupId = pBlock->info.id.groupId;  // this is the initial group result
        } else {
          if (pIndefInfo->groupId != pBlock->info.id.groupId) {  // reset output buffer and computing status
            pIndefInfo->groupId = pBlock->info.id.groupId;
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

    doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
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
                                            pTaskInfo, false, pSup, true);

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

int32_t doGenerateSourceData(SOperatorInfo* pOperator) {
  SProjectOperatorInfo* pProjectInfo = pOperator->info;

  SExprSupp*   pSup = &pOperator->exprSupp;
  SSDataBlock* pRes = pProjectInfo->binfo.pRes;

  blockDataEnsureCapacity(pRes, pOperator->resultInfo.capacity);
  SExprInfo* pExpr = pSup->pExprInfo;

  int64_t st = taosGetTimestampUs();

  for (int32_t k = 0; k < pSup->numOfExprs; ++k) {
    int32_t outputSlotId = pExpr[k].base.resSchema.slotId;

    if (pExpr[k].pExpr->nodeType == QUERY_NODE_VALUE) {
      SColumnInfoData* pColInfoData = taosArrayGet(pRes->pDataBlock, outputSlotId);

      int32_t type = pExpr[k].base.pParam[0].param.nType;
      if (TSDB_DATA_TYPE_NULL == type) {
        colDataSetNNULL(pColInfoData, 0, 1);
      } else {
        colDataSetVal(pColInfoData, 0, taosVariantGet(&pExpr[k].base.pParam[0].param, type), false);
      }
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_FUNCTION) {
      SqlFunctionCtx* pfCtx = &pSup->pCtx[k];

      // UDF scalar functions will be calculated here, for example, select foo(n) from (select 1 n).
      // UDF aggregate functions will be handled in agg operator.
      if (fmIsScalarFunc(pfCtx->functionId)) {
        SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
        taosArrayPush(pBlockList, &pRes);

        SColumnInfoData* pResColData = taosArrayGet(pRes->pDataBlock, outputSlotId);
        SColumnInfoData  idata = {.info = pResColData->info, .hasNull = true};

        SScalarParam dest = {.columnData = &idata};
        int32_t      code = scalarCalculate((SNode*)pExpr[k].pExpr->_function.pFunctNode, pBlockList, &dest);
        if (code != TSDB_CODE_SUCCESS) {
          taosArrayDestroy(pBlockList);
          return code;
        }

        int32_t startOffset = pRes->info.rows;
        ASSERT(pRes->info.capacity > 0);
        colDataAssign(pResColData, &idata, dest.numOfRows, &pRes->info);
        colDataDestroy(&idata);

        taosArrayDestroy(pBlockList);
      } else {
        return TSDB_CODE_OPS_NOT_SUPPORT;
      }
    } else {
      return TSDB_CODE_OPS_NOT_SUPPORT;
    }
  }

  pRes->info.rows = 1;
  doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);

  /*int32_t status = */ doIngroupLimitOffset(&pProjectInfo->limitInfo, 0, pRes, pOperator);

  pOperator->resultInfo.totalRows += pRes->info.rows;

  setOperatorCompleted(pOperator);
  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }

  return TSDB_CODE_SUCCESS;
}

static void setPseudoOutputColInfo(SSDataBlock* pResult, SqlFunctionCtx* pCtx, SArray* pPseudoList) {
  size_t num = (pPseudoList != NULL) ? taosArrayGetSize(pPseudoList) : 0;
  for (int32_t i = 0; i < num; ++i) {
    pCtx[i].pOutput = taosArrayGet(pResult->pDataBlock, i);
  }
}

int32_t projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx,
                              int32_t numOfOutput, SArray* pPseudoList) {
  setPseudoOutputColInfo(pResult, pCtx, pPseudoList);
  pResult->info.dataLoad = 1;

  if (pSrcBlock == NULL) {
    for (int32_t k = 0; k < numOfOutput; ++k) {
      int32_t outputSlotId = pExpr[k].base.resSchema.slotId;

      ASSERT(pExpr[k].pExpr->nodeType == QUERY_NODE_VALUE);
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);

      int32_t type = pExpr[k].base.pParam[0].param.nType;
      if (TSDB_DATA_TYPE_NULL == type) {
        colDataSetNNULL(pColInfoData, 0, 1);
      } else {
        colDataSetVal(pColInfoData, 0, taosVariantGet(&pExpr[k].base.pParam[0].param, type), false);
      }
    }

    pResult->info.rows = 1;
    return TSDB_CODE_SUCCESS;
  }

  if (pResult != pSrcBlock) {
    pResult->info.id.groupId = pSrcBlock->info.id.groupId;
    memcpy(pResult->info.parTbName, pSrcBlock->info.parTbName, TSDB_TABLE_NAME_LEN);
  }

  // if the source equals to the destination, it is to create a new column as the result of scalar
  // function or some operators.
  bool createNewColModel = (pResult == pSrcBlock);
  if (createNewColModel) {
    blockDataEnsureCapacity(pResult, pResult->info.rows);
  }

  int32_t numOfRows = 0;

  for (int32_t k = 0; k < numOfOutput; ++k) {
    int32_t               outputSlotId = pExpr[k].base.resSchema.slotId;
    SqlFunctionCtx*       pfCtx = &pCtx[k];
    SInputColumnInfoData* pInputData = &pfCtx->input;

    if (pExpr[k].pExpr->nodeType == QUERY_NODE_COLUMN) {  // it is a project query
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      if (pResult->info.rows > 0 && !createNewColModel) {
        if (pInputData->pData[0] == NULL) {
          int32_t slotId = pfCtx->param[0].pCol->slotId;

          SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);

          colDataMergeCol(pColInfoData, pResult->info.rows, (int32_t*)&pResult->info.capacity, pInput,
                          pSrcBlock->info.rows);
        } else {
          colDataMergeCol(pColInfoData, pResult->info.rows, (int32_t*)&pResult->info.capacity, pInputData->pData[0],
                          pInputData->numOfRows);
        }
      } else {
        if (pInputData->pData[0] == NULL) {
          int32_t slotId = pfCtx->param[0].pCol->slotId;

          SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
          colDataAssign(pColInfoData, pInput, pSrcBlock->info.rows, &pResult->info);

          numOfRows = pSrcBlock->info.rows;
        } else {
          colDataAssign(pColInfoData, pInputData->pData[0], pInputData->numOfRows, &pResult->info);
          numOfRows = pInputData->numOfRows;
        }
      }
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_VALUE) {
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);

      int32_t offset = createNewColModel ? 0 : pResult->info.rows;

      int32_t type = pExpr[k].base.pParam[0].param.nType;
      if (TSDB_DATA_TYPE_NULL == type) {
        colDataSetNNULL(pColInfoData, offset, pSrcBlock->info.rows);
      } else {
        char* p = taosVariantGet(&pExpr[k].base.pParam[0].param, type);
        for (int32_t i = 0; i < pSrcBlock->info.rows; ++i) {
          colDataSetVal(pColInfoData, i + offset, p, false);
        }
      }

      numOfRows = pSrcBlock->info.rows;
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_OPERATOR) {
      SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
      taosArrayPush(pBlockList, &pSrcBlock);

      SColumnInfoData* pResColData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      SColumnInfoData  idata = {.info = pResColData->info, .hasNull = true};

      SScalarParam dest = {.columnData = &idata};
      int32_t      code = scalarCalculate(pExpr[k].pExpr->_optrRoot.pRootNode, pBlockList, &dest);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pBlockList);
        return code;
      }

      int32_t startOffset = createNewColModel ? 0 : pResult->info.rows;
      ASSERT(pResult->info.capacity > 0);

      colDataMergeCol(pResColData, startOffset, (int32_t*)&pResult->info.capacity, &idata, dest.numOfRows);
      colDataDestroy(&idata);

      numOfRows = dest.numOfRows;
      taosArrayDestroy(pBlockList);
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_FUNCTION) {
      // _rowts/_c0, not tbname column
      if (fmIsPseudoColumnFunc(pfCtx->functionId) && (!fmIsScanPseudoColumnFunc(pfCtx->functionId))) {
        // do nothing
      } else if (fmIsIndefiniteRowsFunc(pfCtx->functionId)) {
        SResultRowEntryInfo* pResInfo = GET_RES_INFO(pfCtx);
        pfCtx->fpSet.init(pfCtx, pResInfo);

        pfCtx->pOutput = taosArrayGet(pResult->pDataBlock, outputSlotId);
        pfCtx->offset = createNewColModel ? 0 : pResult->info.rows;  // set the start offset

        // set the timestamp(_rowts) output buffer
        if (taosArrayGetSize(pPseudoList) > 0) {
          int32_t* outputColIndex = taosArrayGet(pPseudoList, 0);
          pfCtx->pTsOutput = (SColumnInfoData*)pCtx[*outputColIndex].pOutput;
        }

        // link pDstBlock to set selectivity value
        if (pfCtx->subsidiaries.num > 0) {
          pfCtx->pDstBlock = pResult;
        }

        int32_t code = pfCtx->fpSet.process(pfCtx);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        numOfRows = pResInfo->numOfRes;
      } else if (fmIsAggFunc(pfCtx->functionId)) {
        // selective value output should be set during corresponding function execution
        if (fmIsSelectValueFunc(pfCtx->functionId)) {
          continue;
        }
        // _group_key function for "partition by tbname" + csum(col_name) query
        SColumnInfoData* pOutput = taosArrayGet(pResult->pDataBlock, outputSlotId);
        int32_t          slotId = pfCtx->param[0].pCol->slotId;

        // todo handle the json tag
        SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
        for (int32_t f = 0; f < pSrcBlock->info.rows; ++f) {
          bool isNull = colDataIsNull_s(pInput, f);
          if (isNull) {
            colDataSetNULL(pOutput, pResult->info.rows + f);
          } else {
            char* data = colDataGetData(pInput, f);
            colDataSetVal(pOutput, pResult->info.rows + f, data, isNull);
          }
        }

      } else {
        SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
        taosArrayPush(pBlockList, &pSrcBlock);

        SColumnInfoData* pResColData = taosArrayGet(pResult->pDataBlock, outputSlotId);
        SColumnInfoData  idata = {.info = pResColData->info, .hasNull = true};

        SScalarParam dest = {.columnData = &idata};
        int32_t      code = scalarCalculate((SNode*)pExpr[k].pExpr->_function.pFunctNode, pBlockList, &dest);
        if (code != TSDB_CODE_SUCCESS) {
          taosArrayDestroy(pBlockList);
          return code;
        }

        int32_t startOffset = createNewColModel ? 0 : pResult->info.rows;
        ASSERT(pResult->info.capacity > 0);
        colDataMergeCol(pResColData, startOffset, (int32_t*)&pResult->info.capacity, &idata, dest.numOfRows);
        colDataDestroy(&idata);

        numOfRows = dest.numOfRows;
        taosArrayDestroy(pBlockList);
      }
    } else {
      return TSDB_CODE_OPS_NOT_SUPPORT;
    }
  }

  if (!createNewColModel) {
    pResult->info.rows += numOfRows;
  }

  return TSDB_CODE_SUCCESS;
}

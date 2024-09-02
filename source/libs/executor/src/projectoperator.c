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
  bool           outputIgnoreGroup;
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
static SSDataBlock* doProjectOperation1(SOperatorInfo* pOperator);
static int32_t      doProjectOperation(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static SSDataBlock* doApplyIndefinitFunction1(SOperatorInfo* pOperator);
static int32_t      doApplyIndefinitFunction(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t      setRowTsColumnOutputInfo(SqlFunctionCtx* pCtx, int32_t numOfCols, SArray** pResList);
static int32_t      setFunctionResultOutput(SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SAggSupporter* pSup,
                                            int32_t stage, int32_t numOfExprs);

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

int32_t createProjectOperatorInfo(SOperatorInfo* downstream, SProjectPhysiNode* pProjPhyNode, SExecTaskInfo* pTaskInfo,
                                  SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);

  int32_t code = TSDB_CODE_SUCCESS;
  SProjectOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SProjectOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pOperator->exprSupp.hasWindowOrGroup = false;
  pOperator->pTaskInfo = pTaskInfo;

  int32_t    lino = 0;

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pProjPhyNode->node.pOutputDataBlockDesc);
  TSDB_CHECK_NULL(pResBlock, code, lino, _error, terrno);

  initLimitInfo(pProjPhyNode->node.pLimit, pProjPhyNode->node.pSlimit, &pInfo->limitInfo);

  pInfo->binfo.pRes = pResBlock;
  pInfo->pFinalRes = NULL;

  code = createOneDataBlock(pResBlock, false, &pInfo->pFinalRes);
  TSDB_CHECK_CODE(code, lino, _error);

  pInfo->binfo.inputTsOrder = pProjPhyNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pProjPhyNode->node.outputTsOrder;
  pInfo->inputIgnoreGroup = pProjPhyNode->inputIgnoreGroup;
  pInfo->outputIgnoreGroup = pProjPhyNode->ignoreGroupId;

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
  
  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pProjPhyNode->pProjections, NULL, &pExprInfo, &numOfCols);
  TSDB_CHECK_CODE(code, lino, _error);
  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, keyBufSize, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  TSDB_CHECK_CODE(code, lino, _error);

  initBasicInfo(&pInfo->binfo, pResBlock);
  code = setFunctionResultOutput(pOperator, &pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, numOfCols);
  TSDB_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)pProjPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  TSDB_CHECK_CODE(code, lino, _error);

  code = setRowTsColumnOutputInfo(pOperator->exprSupp.pCtx, numOfCols, &pInfo->pPseudoColInfo);
  TSDB_CHECK_CODE(code, lino, _error);

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

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) destroyProjectOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
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
  if (!(pLimitInfo->remainGroupOffset == 0 || pLimitInfo->remainGroupOffset == -1)) {
    qError("project failed at: %s:%d", __func__, __LINE__);
    return TSDB_CODE_INVALID_PARA;
  }

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
  if (pBlock->info.rows == 0 && 0 != pLimitInfo->limit.limit) {
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

int32_t doProjectOperation(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_OPTR_CHECK(pResBlock);

  SProjectOperatorInfo* pProjectInfo = pOperator->info;
  SOptrBasicInfo*       pInfo = &pProjectInfo->binfo;
  SExprSupp*            pSup = &pOperator->exprSupp;
  SSDataBlock*          pRes = pInfo->pRes;
  SSDataBlock*          pFinalRes = pProjectInfo->pFinalRes;
  int32_t               code = 0;
  int64_t               st = 0;
  int32_t               order = pInfo->inputTsOrder;
  int32_t               scanFlag = 0;

  blockDataCleanup(pFinalRes);
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return code;
  }

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

    if (pProjectInfo->outputIgnoreGroup) {
      pRes->info.id.groupId = 0;
    }

    *pResBlock = (pRes->info.rows > 0)? pRes:NULL;
    return code;
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

        *pResBlock = pBlock;
        return code;
      }

      if (pProjectInfo->inputIgnoreGroup) {
        pBlock->info.id.groupId = 0;
      }

      int32_t status = discardGroupDataBlock(pBlock, pLimitInfo);
      if (status == PROJECT_RETRIEVE_CONTINUE) {
        continue;
      }

      (void) setInfoForNewGroup(pBlock, pLimitInfo, pOperator);
      if (pOperator->status == OP_EXEC_DONE) {
        break;
      }

      if (pProjectInfo->mergeDataBlocks) {
        pFinalRes->info.scanFlag = scanFlag = pBlock->info.scanFlag;
      } else {
        pRes->info.scanFlag = scanFlag = pBlock->info.scanFlag;
      }

      code = setInputDataBlock(pSup, pBlock, order, scanFlag, false);
      if (code) {
        T_LONG_JMP(pTaskInfo->env, code);
      }

      code = blockDataEnsureCapacity(pInfo->pRes, pInfo->pRes->info.rows + pBlock->info.rows);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }

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
        int32_t ret = blockDataMerge(pFinalRes, pRes);
        if (ret < 0) {
          pTaskInfo->code = code;
          return code;
        }

        if (pFinalRes->info.rows + pRes->info.rows <= pOperator->resultInfo.threshold && (pOperator->status != OP_EXEC_DONE)) {
          continue;
        }
      }

      // do apply filter
      code = doFilter(pFinalRes, pOperator->exprSupp.pFilterInfo, NULL);
      if (code) {
        pTaskInfo->code = code;
        return code;
      }

      // when apply the limit/offset for each group, pRes->info.rows may be 0, due to limit constraint.
      if (pFinalRes->info.rows > 0 || (pOperator->status == OP_EXEC_DONE)) {
        qDebug("project return %" PRId64 " rows, status %d", pFinalRes->info.rows, pOperator->status);
        break;
      }
    } else {
      // do apply filter
      if (pRes->info.rows > 0) {
        code = doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
        if (code) {
          pTaskInfo->code = code;
          return code;
        }

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

  if (pProjectInfo->outputIgnoreGroup) {
    p->info.id.groupId = 0;
  }

  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM) {
    printDataBlock(p, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
  }

  *pResBlock = (p->info.rows > 0)? p:NULL;
  return code;
}

int32_t createIndefinitOutputOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pNode,
                                                 SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);
  int32_t code = 0;
  int32_t lino = 0;
  int32_t numOfRows = 4096;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  SIndefOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SIndefOperatorInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pOperator->pTaskInfo = pTaskInfo;

  SExprSupp* pSup = &pOperator->exprSupp;
  pSup->hasWindowOrGroup = false;

  SIndefRowsFuncPhysiNode* pPhyNode = (SIndefRowsFuncPhysiNode*)pNode;

  if (pPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pSExpr = NULL;
    code = createExprInfo(pPhyNode->pExprs, NULL, &pSExpr, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pSExpr, num, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->node.pOutputDataBlockDesc);
  TSDB_CHECK_NULL(pResBlock, code, lino, _error, terrno);

  // Make sure the size of SSDataBlock will never exceed the size of 2MB.
  int32_t TWOMB = 2 * 1024 * 1024;
  if (numOfRows * pResBlock->info.rowSize > TWOMB) {
    numOfRows = TWOMB / pResBlock->info.rowSize;
  }

  initBasicInfo(&pInfo->binfo, pResBlock);
  initResultSizeInfo(&pOperator->resultInfo, numOfRows);
  code = blockDataEnsureCapacity(pResBlock, numOfRows);
  TSDB_CHECK_CODE(code, lino, _error);

  int32_t    numOfExpr = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pPhyNode->pFuncs, NULL, &pExprInfo, &numOfExpr);
  TSDB_CHECK_CODE(code, lino, _error);

  code = initAggSup(pSup, &pInfo->aggSup, pExprInfo, numOfExpr, keyBufSize, pTaskInfo->id.str,
                            pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  TSDB_CHECK_CODE(code, lino, _error);

  code = setFunctionResultOutput(pOperator, &pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, numOfExpr);
  TSDB_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)pPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  TSDB_CHECK_CODE(code, lino, _error);

  pInfo->binfo.pRes = pResBlock;
  pInfo->binfo.inputTsOrder = pNode->inputTsOrder;
  pInfo->binfo.outputTsOrder = pNode->outputTsOrder;
  code = setRowTsColumnOutputInfo(pSup->pCtx, numOfExpr, &pInfo->pPseudoColInfo);
  TSDB_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, "IndefinitOperator", QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doApplyIndefinitFunction, NULL, destroyIndefinitOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) destroyIndefinitOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
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

  code = setInputDataBlock(pSup, pBlock, order, scanFlag, false);
  if (code) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  code = blockDataEnsureCapacity(pInfo->pRes, pInfo->pRes->info.rows + pBlock->info.rows);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  code = projectApplyFunctions(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs,
                               pIndefInfo->pPseudoColInfo);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

SSDataBlock* doApplyIndefinitFunction1(SOperatorInfo* pOperator) {
  SSDataBlock* pResBlock = NULL;
  pOperator->pTaskInfo->code = doApplyIndefinitFunction(pOperator, &pResBlock);
  return pResBlock;
}

int32_t doApplyIndefinitFunction(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_OPTR_CHECK(pResBlock);

  SIndefOperatorInfo* pIndefInfo = pOperator->info;
  SOptrBasicInfo*     pInfo = &pIndefInfo->binfo;
  SExprSupp*          pSup = &pOperator->exprSupp;
  int64_t             st = 0;
  int32_t             code = 0;
  SSDataBlock*        pRes = pInfo->pRes;

  blockDataCleanup(pRes);

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    return 0;
  }

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

    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
    if (code) {
      pTaskInfo->code = code;
      return code;
    }

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

  *pResBlock = (rows > 0) ? pInfo->pRes : NULL;
  return code;
}

int32_t initCtxOutputBuffer(SqlFunctionCtx* pCtx, int32_t size) {
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t j = 0; j < size; ++j) {
    struct SResultRowEntryInfo* pResInfo = GET_RES_INFO(&pCtx[j]);
    if (isRowEntryInitialized(pResInfo) || fmIsPseudoColumnFunc(pCtx[j].functionId) || pCtx[j].functionId == -1 ||
        fmIsScalarFunc(pCtx[j].functionId)) {
      continue;
    }

    code = pCtx[j].fpSet.init(&pCtx[j], pCtx[j].resultInfo);
    if (code) {
      return code;
    }
  }

  return 0;
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
int32_t setFunctionResultOutput(SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SAggSupporter* pSup, int32_t stage,
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
  if (pRow == NULL || pTaskInfo->code != 0) {
    return pTaskInfo->code;
  }

  for (int32_t i = 0; i < numOfExprs; ++i) {
    struct SResultRowEntryInfo* pEntry = getResultEntryInfo(pRow, i, rowEntryInfoOffset);
    cleanupResultRowEntry(pEntry);

    pCtx[i].resultInfo = pEntry;
    pCtx[i].scanFlag = stage;
  }

  return initCtxOutputBuffer(pCtx, numOfExprs);
}

int32_t setRowTsColumnOutputInfo(SqlFunctionCtx* pCtx, int32_t numOfCols, SArray** pResList) {
  QRY_OPTR_CHECK(pResList);
  SArray* pList = taosArrayInit(4, sizeof(int32_t));
  if (pList == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    if (fmIsPseudoColumnFunc(pCtx[i].functionId)) {
      void* px = taosArrayPush(pList, &i);
      if (px == NULL) {
        return terrno;
      }
    }
  }

  *pResList = pList;
  return 0;
}

int32_t doGenerateSourceData(SOperatorInfo* pOperator) {
  SProjectOperatorInfo* pProjectInfo = pOperator->info;

  SExprSupp*   pSup = &pOperator->exprSupp;
  SSDataBlock* pRes = pProjectInfo->binfo.pRes;
  SExprInfo*   pExpr = pSup->pExprInfo;
  int64_t      st = taosGetTimestampUs();
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  int32_t code = blockDataEnsureCapacity(pRes, pOperator->resultInfo.capacity);
  if (code) {
    return code;
  }

  for (int32_t k = 0; k < pSup->numOfExprs; ++k) {
    int32_t outputSlotId = pExpr[k].base.resSchema.slotId;

    if (pExpr[k].pExpr->nodeType == QUERY_NODE_VALUE) {
      SColumnInfoData* pColInfoData = taosArrayGet(pRes->pDataBlock, outputSlotId);
      if (pColInfoData == NULL) {
        return terrno;
      }

      int32_t type = pExpr[k].base.pParam[0].param.nType;
      if (TSDB_DATA_TYPE_NULL == type) {
        colDataSetNNULL(pColInfoData, 0, 1);
      } else {
        code = colDataSetVal(pColInfoData, 0, taosVariantGet(&pExpr[k].base.pParam[0].param, type), false);
        if (code) {
          return code;
        }
      }
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_FUNCTION) {
      SqlFunctionCtx* pfCtx = &pSup->pCtx[k];

      // UDF scalar functions will be calculated here, for example, select foo(n) from (select 1 n).
      // UDF aggregate functions will be handled in agg operator.
      if (fmIsScalarFunc(pfCtx->functionId)) {
        SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
        if (pBlockList == NULL) {
          return terrno;
        }

        void* px = taosArrayPush(pBlockList, &pRes);
        if (px == NULL) {
          return terrno;
        }

        SColumnInfoData* pResColData = taosArrayGet(pRes->pDataBlock, outputSlotId);
        if (pResColData == NULL) {
          return terrno;
        }

        SColumnInfoData  idata = {.info = pResColData->info, .hasNull = true};

        SScalarParam dest = {.columnData = &idata};
        code = scalarCalculate((SNode*)pExpr[k].pExpr->_function.pFunctNode, pBlockList, &dest);
        if (code != TSDB_CODE_SUCCESS) {
          taosArrayDestroy(pBlockList);
          return code;
        }

        int32_t startOffset = pRes->info.rows;
        if (pRes->info.capacity <= 0) {
          qError("project failed at: %s:%d", __func__, __LINE__);
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        }
        code = colDataAssign(pResColData, &idata, dest.numOfRows, &pRes->info);
        if (code) {
          return code;
        }

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
  code = doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
  if (code) {
    pTaskInfo->code = code;
    return code;
  }

  (void) doIngroupLimitOffset(&pProjectInfo->limitInfo, 0, pRes, pOperator);

  pOperator->resultInfo.totalRows += pRes->info.rows;

  setOperatorCompleted(pOperator);
  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }

  return code;
}

static void setPseudoOutputColInfo(SSDataBlock* pResult, SqlFunctionCtx* pCtx, SArray* pPseudoList) {
  size_t num = (pPseudoList != NULL) ? taosArrayGetSize(pPseudoList) : 0;
  for (int32_t i = 0; i < num; ++i) {
    pCtx[i].pOutput = taosArrayGet(pResult->pDataBlock, i);
    if (pCtx[i].pOutput == NULL) {
      qError("failed to get the output buf, ptr is null");
    }
  }
}

int32_t projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx,
                              int32_t numOfOutput, SArray* pPseudoList) {
  int32_t lino = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  setPseudoOutputColInfo(pResult, pCtx, pPseudoList);
  pResult->info.dataLoad = 1;

  SArray* processByRowFunctionCtx = NULL;
  if (pSrcBlock == NULL) {
    for (int32_t k = 0; k < numOfOutput; ++k) {
      int32_t outputSlotId = pExpr[k].base.resSchema.slotId;

      if (pExpr[k].pExpr->nodeType != QUERY_NODE_VALUE) {
        qError("project failed at: %s:%d", __func__, __LINE__);
        code = TSDB_CODE_INVALID_PARA;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      if (pColInfoData == NULL) {
        code = terrno;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      int32_t type = pExpr[k].base.pParam[0].param.nType;
      if (TSDB_DATA_TYPE_NULL == type) {
        colDataSetNNULL(pColInfoData, 0, 1);
      } else {
        code = colDataSetVal(pColInfoData, 0, taosVariantGet(&pExpr[k].base.pParam[0].param, type), false);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    pResult->info.rows = 1;
    goto _exit;
  }

  if (pResult != pSrcBlock) {
    pResult->info.id.groupId = pSrcBlock->info.id.groupId;
    memcpy(pResult->info.parTbName, pSrcBlock->info.parTbName, TSDB_TABLE_NAME_LEN);
  }

  // if the source equals to the destination, it is to create a new column as the result of scalar
  // function or some operators.
  bool createNewColModel = (pResult == pSrcBlock);
  if (createNewColModel) {
    code = blockDataEnsureCapacity(pResult, pResult->info.rows);
    if (code) {
      goto _exit;
    }
  }

  int32_t numOfRows = 0;

  for (int32_t k = 0; k < numOfOutput; ++k) {
    int32_t               outputSlotId = pExpr[k].base.resSchema.slotId;
    SqlFunctionCtx*       pfCtx = &pCtx[k];
    SInputColumnInfoData* pInputData = &pfCtx->input;

    if (pExpr[k].pExpr->nodeType == QUERY_NODE_COLUMN) {  // it is a project query
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      if (pColInfoData == NULL) {
        code = terrno;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      if (pResult->info.rows > 0 && !createNewColModel) {
        int32_t ret = 0;

        if (pInputData->pData[0] == NULL) {
          int32_t slotId = pfCtx->param[0].pCol->slotId;

          SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
          if (pInput == NULL) {
            code = terrno;
            TSDB_CHECK_CODE(code, lino, _exit);
          }

          ret = colDataMergeCol(pColInfoData, pResult->info.rows, (int32_t*)&pResult->info.capacity, pInput,
                                pSrcBlock->info.rows);
        } else {
          ret = colDataMergeCol(pColInfoData, pResult->info.rows, (int32_t*)&pResult->info.capacity,
                                pInputData->pData[0], pInputData->numOfRows);
        }

        if (ret < 0) {
          code = ret;
        }

        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        if (pInputData->pData[0] == NULL) {
          int32_t slotId = pfCtx->param[0].pCol->slotId;

          SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
          if (pInput == NULL) {
            code = terrno;
            TSDB_CHECK_CODE(code, lino, _exit);
          }

          code = colDataAssign(pColInfoData, pInput, pSrcBlock->info.rows, &pResult->info);
          numOfRows = pSrcBlock->info.rows;
        } else {
          code = colDataAssign(pColInfoData, pInputData->pData[0], pInputData->numOfRows, &pResult->info);
          numOfRows = pInputData->numOfRows;
        }

        TSDB_CHECK_CODE(code, lino, _exit);
      }
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_VALUE) {
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      if (pColInfoData == NULL) {
        code = terrno;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      int32_t offset = createNewColModel ? 0 : pResult->info.rows;

      int32_t type = pExpr[k].base.pParam[0].param.nType;
      if (TSDB_DATA_TYPE_NULL == type) {
        colDataSetNNULL(pColInfoData, offset, pSrcBlock->info.rows);
      } else {
        char* p = taosVariantGet(&pExpr[k].base.pParam[0].param, type);
        for (int32_t i = 0; i < pSrcBlock->info.rows; ++i) {
          code = colDataSetVal(pColInfoData, i + offset, p, false);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }

      numOfRows = pSrcBlock->info.rows;
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_OPERATOR) {
      SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
      if (pBlockList == NULL) {
        code = terrno;
        goto _exit;
      }

      void* px = taosArrayPush(pBlockList, &pSrcBlock);
      if (px == NULL) {
        code = terrno;
        taosArrayDestroy(pBlockList);
        goto _exit;
      }

      SColumnInfoData* pResColData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      if (pResColData == NULL) {
        code = terrno;
        taosArrayDestroy(pBlockList);
        goto _exit;
      }

      SColumnInfoData  idata = {.info = pResColData->info, .hasNull = true};

      SScalarParam dest = {.columnData = &idata};
      code = scalarCalculate(pExpr[k].pExpr->_optrRoot.pRootNode, pBlockList, &dest);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pBlockList);
        goto _exit;
      }

      int32_t startOffset = createNewColModel ? 0 : pResult->info.rows;
      if (pResult->info.capacity <= 0) {
        qError("project failed at: %s:%d", __func__, __LINE__);
        code = TSDB_CODE_INVALID_PARA;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      int32_t ret = colDataMergeCol(pResColData, startOffset, (int32_t*)&pResult->info.capacity, &idata, dest.numOfRows);
      if (ret < 0) {
        code = ret;
      }

      colDataDestroy(&idata);
      TSDB_CHECK_CODE(code, lino, _exit);

      numOfRows = dest.numOfRows;
      taosArrayDestroy(pBlockList);
    } else if (pExpr[k].pExpr->nodeType == QUERY_NODE_FUNCTION) {
      // _rowts/_c0, not tbname column
      if (fmIsPseudoColumnFunc(pfCtx->functionId) && (!fmIsScanPseudoColumnFunc(pfCtx->functionId))) {
        // do nothing
      } else if (fmIsIndefiniteRowsFunc(pfCtx->functionId)) {
        SResultRowEntryInfo* pResInfo = GET_RES_INFO(pfCtx);
        code = pfCtx->fpSet.init(pfCtx, pResInfo);
        TSDB_CHECK_CODE(code, lino, _exit);
        pfCtx->pOutput = taosArrayGet(pResult->pDataBlock, outputSlotId);
        if (pfCtx->pOutput == NULL) {
          code = terrno;
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        pfCtx->offset = createNewColModel ? 0 : pResult->info.rows;  // set the start offset

        // set the timestamp(_rowts) output buffer
        if (taosArrayGetSize(pPseudoList) > 0) {
          int32_t* outputColIndex = taosArrayGet(pPseudoList, 0);
          if (outputColIndex == NULL) {
            code = terrno;
            goto _exit;
          }

          pfCtx->pTsOutput = (SColumnInfoData*)pCtx[*outputColIndex].pOutput;
        }

        // link pDstBlock to set selectivity value
        if (pfCtx->subsidiaries.num > 0) {
          pfCtx->pDstBlock = pResult;
        }

        code = pfCtx->fpSet.process(pfCtx);
        if (code != TSDB_CODE_SUCCESS) {
          goto _exit;
        }

        numOfRows = pResInfo->numOfRes;
        if (fmIsProcessByRowFunc(pfCtx->functionId)) {
          if (NULL == processByRowFunctionCtx) {
            processByRowFunctionCtx = taosArrayInit(1, sizeof(SqlFunctionCtx*));
            if (!processByRowFunctionCtx) {
              code = terrno;
              goto _exit;
            }
          }

          void* px = taosArrayPush(processByRowFunctionCtx, &pfCtx);
          if (px == NULL) {
            code = terrno;
            goto _exit;
          }
        }
      } else if (fmIsAggFunc(pfCtx->functionId)) {
        // selective value output should be set during corresponding function execution
        if (fmIsSelectValueFunc(pfCtx->functionId)) {
          continue;
        }
        // _group_key function for "partition by tbname" + csum(col_name) query
        SColumnInfoData* pOutput = taosArrayGet(pResult->pDataBlock, outputSlotId);
        if (pOutput == NULL) {
          code = terrno;
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        int32_t          slotId = pfCtx->param[0].pCol->slotId;

        // todo handle the json tag
        SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
        if (pInput == NULL) {
          code = terrno;
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        for (int32_t f = 0; f < pSrcBlock->info.rows; ++f) {
          bool isNull = colDataIsNull_s(pInput, f);
          if (isNull) {
            colDataSetNULL(pOutput, pResult->info.rows + f);
          } else {
            char* data = colDataGetData(pInput, f);
            code = colDataSetVal(pOutput, pResult->info.rows + f, data, isNull);
            TSDB_CHECK_CODE(code, lino, _exit);
          }
        }

      } else {
        SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
        if (pBlockList == NULL) {
          code = terrno;
          goto _exit;
        }

        void* px = taosArrayPush(pBlockList, &pSrcBlock);
        if (px == NULL) {
          code = terrno;
          goto _exit;
        }

        SColumnInfoData* pResColData = taosArrayGet(pResult->pDataBlock, outputSlotId);
        if (pResColData == NULL) {
          taosArrayDestroy(pBlockList);
          code = terrno;
          goto _exit;
        }

        SColumnInfoData  idata = {.info = pResColData->info, .hasNull = true};

        SScalarParam dest = {.columnData = &idata};
        code = scalarCalculate((SNode*)pExpr[k].pExpr->_function.pFunctNode, pBlockList, &dest);
        if (code != TSDB_CODE_SUCCESS) {
          taosArrayDestroy(pBlockList);
          goto _exit;
        }

        int32_t startOffset = createNewColModel ? 0 : pResult->info.rows;
        if (pResult->info.capacity <= 0) {
          qError("project failed at: %s:%d", __func__, __LINE__);
          code = TSDB_CODE_INVALID_PARA;
          TSDB_CHECK_CODE(code, lino, _exit);
        }
        int32_t ret = colDataMergeCol(pResColData, startOffset, (int32_t*)&pResult->info.capacity, &idata, dest.numOfRows);
        if (ret < 0) {
          code = ret;
        }

        colDataDestroy(&idata);

        numOfRows = dest.numOfRows;
        taosArrayDestroy(pBlockList);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    } else {
      return TSDB_CODE_OPS_NOT_SUPPORT;
    }
  }

  if (processByRowFunctionCtx && taosArrayGetSize(processByRowFunctionCtx) > 0){
    SqlFunctionCtx** pfCtx = taosArrayGet(processByRowFunctionCtx, 0);
    if (pfCtx == NULL) {
      code = terrno;
      goto _exit;
    }

    code = (*pfCtx)->fpSet.processFuncByRow(processByRowFunctionCtx);
    TSDB_CHECK_CODE(code, lino, _exit);
    numOfRows = (*pfCtx)->resultInfo->numOfRes;
  }

  if (!createNewColModel) {
    pResult->info.rows += numOfRows;
  }

_exit:
  if(processByRowFunctionCtx) {
    taosArrayDestroy(processByRowFunctionCtx);
  }
  return code;
}

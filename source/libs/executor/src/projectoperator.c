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
#include "taoserror.h"
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
static int32_t      doProjectOperation(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t      doApplyIndefinitFunction(SOperatorInfo* pOperator, SSDataBlock** pResBlock);

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

static int32_t resetProjectOperState(SOperatorInfo* pOper) {
  SProjectOperatorInfo* pProject = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pProject->binfo);
  SProjectPhysiNode* pPhynode = (SProjectPhysiNode*)pOper->pPhyNode;

  pProject->limitInfo = (SLimitInfo){0};
  initLimitInfo(pPhynode->node.pLimit, pPhynode->node.pSlimit, &pProject->limitInfo);

  blockDataCleanup(pProject->pFinalRes);

  int32_t code = resetAggSup(&pOper->exprSupp, &pProject->aggSup, pTaskInfo, pPhynode->pProjections, NULL,
    sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, pTaskInfo->streamInfo.pState,
    &pTaskInfo->storageAPI.functionStore);
  if (code == 0){
    code = setFunctionResultOutput(pOper, &pProject->binfo, &pProject->aggSup, MAIN_SCAN, pOper->exprSupp.numOfExprs);
  }
  return 0;
}

int32_t createProjectOperatorInfo(SOperatorInfo* downstream, SProjectPhysiNode* pProjPhyNode, SExecTaskInfo* pTaskInfo,
                                  SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t code = TSDB_CODE_SUCCESS;
  SProjectOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SProjectOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pProjPhyNode;
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

  if (pTaskInfo->execModel == OPTR_EXEC_MODEL_QUEUE) {
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

  code = filterInitFromNode((SNode*)pProjPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  TSDB_CHECK_CODE(code, lino, _error);

  code = setRowTsColumnOutputInfo(pOperator->exprSupp.pCtx, numOfCols, &pInfo->pPseudoColInfo);
  TSDB_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, "ProjectOperator", QUERY_NODE_PHYSICAL_PLAN_PROJECT, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doProjectOperation, NULL, destroyProjectOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamOperatorReleaseState, streamOperatorReloadState);
  setOperatorResetStateFn(pOperator, resetProjectOperState);

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
  QRY_PARAM_CHECK(pResBlock);

  SProjectOperatorInfo* pProjectInfo = pOperator->info;
  SOptrBasicInfo*       pInfo = &pProjectInfo->binfo;
  SExprSupp*            pSup = &pOperator->exprSupp;
  SSDataBlock*          pRes = pInfo->pRes;
  SSDataBlock*          pFinalRes = pProjectInfo->pFinalRes;
  int32_t               code = 0;
  int32_t               lino = 0;
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
    QUERY_CHECK_CODE(code, lino, _end);

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
          pBlock->info.type == STREAM_CHECKPOINT || pBlock->info.type == STREAM_NOTIFY_EVENT) {

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
      QUERY_CHECK_CODE(code, lino, _end);

      code = blockDataEnsureCapacity(pInfo->pRes, pInfo->pRes->info.rows + pBlock->info.rows);
      QUERY_CHECK_CODE(code, lino, _end);

      code = projectApplyFunctions(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs,
                                   pProjectInfo->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo));
      QUERY_CHECK_CODE(code, lino, _end);

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
        code = blockDataMerge(pFinalRes, pRes);
        QUERY_CHECK_CODE(code, lino, _end);

        if (pFinalRes->info.rows + pRes->info.rows <= pOperator->resultInfo.threshold && (pOperator->status != OP_EXEC_DONE)) {
          continue;
        }
      }

      // do apply filter
      code = doFilter(pFinalRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

      // when apply the limit/offset for each group, pRes->info.rows may be 0, due to limit constraint.
      if (pFinalRes->info.rows > 0 || (pOperator->status == OP_EXEC_DONE)) {
        qDebug("project return %" PRId64 " rows, status %d", pFinalRes->info.rows, pOperator->status);
        break;
      }
    } else {
      // do apply filter
      if (pRes->info.rows > 0) {
        code = doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
        QUERY_CHECK_CODE(code, lino, _end);

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
    printDataBlock(p, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo), pTaskInfo->id.queryId);
  }

  *pResBlock = (p->info.rows > 0)? p:NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t resetIndefinitOutputOperState(SOperatorInfo* pOper) {
  SIndefOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SIndefRowsFuncPhysiNode* pPhynode = (SIndefRowsFuncPhysiNode*)pOper->pPhyNode;
  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pInfo->binfo);

  pInfo->groupId = 0;
  pInfo->pNextGroupRes = NULL;
  int32_t code = resetAggSup(&pOper->exprSupp, &pInfo->aggSup, pTaskInfo, pPhynode->pFuncs, NULL,
    sizeof(int64_t) * 2 + POINTER_BYTES, pTaskInfo->id.str, pTaskInfo->streamInfo.pState,
    &pTaskInfo->storageAPI.functionStore);
  if (code == 0){
    code = setFunctionResultOutput(pOper, &pInfo->binfo, &pInfo->aggSup, MAIN_SCAN, pOper->exprSupp.numOfExprs);
  }

  if (code == 0) {
    code = resetExprSupp(&pInfo->scalarSup, pTaskInfo, pPhynode->pExprs, NULL,
                         &pTaskInfo->storageAPI.functionStore);
  }
  return 0;
}

int32_t createIndefinitOutputOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pNode,
                                                 SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);
  int32_t code = 0;
  int32_t lino = 0;
  int32_t numOfRows = 4096;
  size_t  keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;

  SIndefOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SIndefOperatorInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pNode;
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

  code = filterInitFromNode((SNode*)pPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
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
                                         
  setOperatorResetStateFn(pOperator, resetIndefinitOutputOperState);
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
                                 pIndefInfo->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo));
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
                               pIndefInfo->pPseudoColInfo, GET_STM_RTINFO(pOperator->pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

int32_t doApplyIndefinitFunction(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);
  SIndefOperatorInfo* pIndefInfo = pOperator->info;
  SOptrBasicInfo*     pInfo = &pIndefInfo->binfo;
  SExprSupp*          pSup = &pOperator->exprSupp;
  int64_t             st = 0;
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SSDataBlock*        pRes = pInfo->pRes;

  blockDataCleanup(pRes);

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    return code;
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

    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
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
int32_t setFunctionResultOutput(struct SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SAggSupporter* pSup, int32_t stage,
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
  QRY_PARAM_CHECK(pResList);
  SArray* pList = taosArrayInit(4, sizeof(int32_t));
  if (pList == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    if (fmIsPseudoColumnFunc(pCtx[i].functionId) && !fmIsPlaceHolderFunc(pCtx[i].functionId)) {
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
        code = scalarCalculate((SNode*)pExpr[k].pExpr->_function.pFunctNode, pBlockList, &dest, GET_STM_RTINFO(pOperator->pTaskInfo), NULL);
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
  code = doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
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

int32_t projectApplyColumn(SSDataBlock* pResult, SSDataBlock* pSrcBlock, int32_t outputSlotId, SqlFunctionCtx* pfCtx, int32_t* numOfRows, bool createNewColModel) {
  int32_t code = 0, lino = 0;
  SInputColumnInfoData* pInputData = &pfCtx->input;
  SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
  TSDB_CHECK_NULL(pColInfoData, code, lino, _exit, terrno);

  if (pResult->info.rows > 0 && !createNewColModel) {
    if (pInputData->pData[0] == NULL) {
      int32_t slotId = pfCtx->param[0].pCol->slotId;

      SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
      TSDB_CHECK_NULL(pInput, code, lino, _exit, terrno);

      TAOS_CHECK_EXIT(colDataMergeCol(pColInfoData, pResult->info.rows, (int32_t*)&pResult->info.capacity, pInput,
                            pSrcBlock->info.rows));
      *numOfRows = pSrcBlock->info.rows;
      return code;
    }
    
    TAOS_CHECK_EXIT(colDataMergeCol(pColInfoData, pResult->info.rows, (int32_t*)&pResult->info.capacity,
                          pInputData->pData[0], pInputData->numOfRows));
    *numOfRows = pInputData->numOfRows;
    return code;
  } 
  
  if (pInputData->pData[0] == NULL) {
    int32_t slotId = pfCtx->param[0].pCol->slotId;

    SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
    TSDB_CHECK_NULL(pInput, code, lino, _exit, terrno);

    TAOS_CHECK_EXIT(colDataAssign(pColInfoData, pInput, pSrcBlock->info.rows, &pResult->info));
    *numOfRows = pSrcBlock->info.rows;

    return code;
  }
  
  TAOS_CHECK_EXIT(colDataAssign(pColInfoData, pInputData->pData[0], pInputData->numOfRows, &pResult->info));
  *numOfRows = pInputData->numOfRows;

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  
  return code;
}


int32_t projectApplyValue(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, int32_t outputSlotId, int32_t* numOfRows, bool createNewColModel) {
  int32_t code = 0, lino = 0;
  SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
  TSDB_CHECK_NULL(pColInfoData, code, lino, _exit, terrno);

  int32_t offset = createNewColModel ? 0 : pResult->info.rows;
  int32_t type = pExpr->base.pParam[0].param.nType;
  if (TSDB_DATA_TYPE_NULL == type) {
    colDataSetNNULL(pColInfoData, offset, pSrcBlock->info.rows);
  } else {
    char* p = taosVariantGet(&pExpr->base.pParam[0].param, type);
    for (int32_t i = 0; i < pSrcBlock->info.rows; ++i) {
      TAOS_CHECK_EXIT(colDataSetVal(pColInfoData, i + offset, p, false));
    }
  }

  *numOfRows = pSrcBlock->info.rows;

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  
  return code;
}



int32_t projectApplyOperator(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, int32_t outputSlotId, int32_t* numOfRows, bool createNewColModel, const void* pExtraParams) {
  int32_t code = 0, lino = 0;
  SArray* pBlockList = taosArrayInit(4, POINTER_BYTES);
  TSDB_CHECK_NULL(pBlockList, code, lino, _exit, terrno);

  void* px = taosArrayPush(pBlockList, &pSrcBlock);
  TSDB_CHECK_NULL(px, code, lino, _exit, terrno);

  SColumnInfoData* pResColData = taosArrayGet(pResult->pDataBlock, outputSlotId);
  TSDB_CHECK_NULL(pResColData, code, lino, _exit, terrno);

  SColumnInfoData idata = {.info = pResColData->info, .hasNull = true};
  SScalarParam dest = {.columnData = &idata};
  TAOS_CHECK_EXIT(scalarCalculate(pExpr->pExpr->_optrRoot.pRootNode, pBlockList, &dest, pExtraParams, NULL));

  if (pResult->info.rows > 0 && !createNewColModel) {
    code = colDataMergeCol(pResColData, pResult->info.rows, (int32_t*)&pResult->info.capacity, &idata, dest.numOfRows);
  } else {
    code = colDataAssign(pResColData, &idata, dest.numOfRows, &pResult->info);
  }

  colDataDestroy(&idata);
  TAOS_CHECK_EXIT(code);

  *numOfRows = dest.numOfRows;
  
_exit:

  if (code < 0) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  taosArrayDestroy(pBlockList);
  
  return code;
}


int32_t projectApplyFunction(SqlFunctionCtx* pCtx, SqlFunctionCtx* pfCtx, SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, 
                                    int32_t outputSlotId, int32_t* numOfRows, bool createNewColModel, const void* pExtraParams, 
                                    SArray* pPseudoList, SArray** processByRowFunctionCtx, bool doSelectFunc) {
  int32_t code = 0, lino = 0;
  SArray* pBlockList = NULL;
  SColumnInfoData* pResColData = taosArrayGet(pResult->pDataBlock, outputSlotId);
  TSDB_CHECK_NULL(pResColData, code, lino, _exit, terrno);

  if (fmIsPlaceHolderFunc(pfCtx->functionId) && pExtraParams && pfCtx->pExpr->base.pParamList && 1 == pfCtx->pExpr->base.pParamList->length) {
    TAOS_CHECK_EXIT(scalarAssignPlaceHolderRes(pResColData, pResult->info.rows, pSrcBlock->info.rows, pfCtx->functionId, pExtraParams));
    *numOfRows = pSrcBlock->info.rows;

    return code;
  }

  if (fmIsScalarFunc(pfCtx->functionId) || fmIsPlaceHolderFunc(pfCtx->functionId)) {
    pBlockList = taosArrayInit(4, POINTER_BYTES);
    TSDB_CHECK_NULL(pBlockList, code, lino, _exit, terrno);

    void* px = taosArrayPush(pBlockList, &pSrcBlock);
    TSDB_CHECK_NULL(px, code, lino, _exit, terrno);

    SColumnInfoData idata = {.info = pResColData->info, .hasNull = true};
    SScalarParam dest = {.columnData = &idata};
    TAOS_CHECK_EXIT(scalarCalculate((SNode*)pExpr->pExpr->_function.pFunctNode, pBlockList, &dest, pExtraParams, NULL));

    if (pResult->info.rows > 0 && !createNewColModel) {
      code = colDataMergeCol(pResColData, pResult->info.rows, (int32_t*)&pResult->info.capacity, &idata, dest.numOfRows);
    } else {
      SColumnInfo oriInfo = pResColData->info;
      code = colDataAssign(pResColData, &idata, dest.numOfRows, &pResult->info);
      // restore the original column info to satisfy the output column schema
      pResColData->info = oriInfo;
    }

    colDataDestroy(&idata);
    taosArrayDestroy(pBlockList);
    TAOS_CHECK_EXIT(code);

    *numOfRows = dest.numOfRows;

    return code;
  }

  if (fmIsIndefiniteRowsFunc(pfCtx->functionId)) {
    SResultRowEntryInfo* pResInfo = GET_RES_INFO(pfCtx);
    TAOS_CHECK_EXIT(pfCtx->fpSet.init(pfCtx, pResInfo));


    pfCtx->pOutput = (char*)pResColData;
    TSDB_CHECK_NULL(pfCtx->pOutput, code, lino, _exit, terrno);

    pfCtx->offset = createNewColModel ? 0 : pResult->info.rows;  // set the start offset

    // set the timestamp(_rowts) output buffer
    if (taosArrayGetSize(pPseudoList) > 0) {
      int32_t* outputColIndex = taosArrayGet(pPseudoList, 0);
      TSDB_CHECK_NULL(outputColIndex, code, lino, _exit, terrno);

      pfCtx->pTsOutput = (SColumnInfoData*)pCtx[*outputColIndex].pOutput;
    }

    // link pDstBlock to set selectivity value
    if (pfCtx->subsidiaries.num > 0) {
      pfCtx->pDstBlock = pResult;
    }

    code = pfCtx->fpSet.process(pfCtx);
    if (code != TSDB_CODE_SUCCESS) {
      if (pfCtx->fpSet.cleanup != NULL) {
        pfCtx->fpSet.cleanup(pfCtx);
      }
      TAOS_CHECK_EXIT(code);
    }

    *numOfRows = pResInfo->numOfRes;
    
    if (fmIsProcessByRowFunc(pfCtx->functionId)) {
      if (NULL == *processByRowFunctionCtx) {
        *processByRowFunctionCtx = taosArrayInit(1, sizeof(SqlFunctionCtx*));
        TSDB_CHECK_NULL(*processByRowFunctionCtx, code, lino, _exit, terrno);
      }

      void* px = taosArrayPush(*processByRowFunctionCtx, &pfCtx);
      TSDB_CHECK_NULL(px, code, lino, _exit, terrno);
    }

    return code;
  } 

  if (fmIsAggFunc(pfCtx->functionId)) {
    // selective value output should be set during corresponding function execution
    if (!doSelectFunc && fmIsSelectValueFunc(pfCtx->functionId)) {
      return code;
    }
    
    // _group_key function for "partition by tbname" + csum(col_name) query
    int32_t slotId = pfCtx->param[0].pCol->slotId;

    // todo handle the json tag
    SColumnInfoData* pInput = taosArrayGet(pSrcBlock->pDataBlock, slotId);
    TSDB_CHECK_NULL(pInput, code, lino, _exit, terrno);

    for (int32_t f = 0; f < pSrcBlock->info.rows; ++f) {
      bool isNull = colDataIsNull_s(pInput, f);
      if (isNull) {
        colDataSetNULL(pResColData, pResult->info.rows + f);
      } else {
        char* data = colDataGetData(pInput, f);
        TAOS_CHECK_EXIT(colDataSetVal(pResColData, pResult->info.rows + f, data, isNull));
      }
    }

    *numOfRows = pSrcBlock->info.rows;

    return code;
  } 
  
  if (fmIsGroupIdFunc(pfCtx->functionId)) {
    for (int32_t f = 0; f < pSrcBlock->info.rows; ++f) {
      TAOS_CHECK_EXIT(colDataSetVal(pResColData, pResult->info.rows + f, (const char*)&pSrcBlock->info.id.groupId, false));
    }

    *numOfRows = pSrcBlock->info.rows;
    return code;
  }
  
_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  taosArrayDestroy(pBlockList);
  
  return code;
}


int32_t projectApplyFunctionsWithSelect(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock,
                                        SqlFunctionCtx* pCtx, int32_t numOfOutput, SArray* pPseudoList,
                                        const void* pExtraParams, bool doSelectFunc, bool hasIndefRowsFunc) {
  int32_t lino = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  if (hasIndefRowsFunc) {
    setPseudoOutputColInfo(pResult, pCtx, pPseudoList);
  }
  pResult->info.dataLoad = 1;

  SArray* processByRowFunctionCtx = NULL;
  if (pSrcBlock == NULL) {
    for (int32_t k = 0; k < numOfOutput; ++k) {
      int32_t outputSlotId = pExpr[k].base.resSchema.slotId;

      if (pExpr[k].pExpr->nodeType != QUERY_NODE_VALUE) {
        qError("project failed at: %s:%d", __func__, __LINE__);
        TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
      }
      SColumnInfoData* pColInfoData = taosArrayGet(pResult->pDataBlock, outputSlotId);
      TSDB_CHECK_NULL(pColInfoData, code, lino, _exit, terrno);

      int32_t type = pExpr[k].base.pParam[0].param.nType;
      if (TSDB_DATA_TYPE_NULL == type) {
        colDataSetNNULL(pColInfoData, 0, 1);
      } else {
        TAOS_CHECK_EXIT(colDataSetVal(pColInfoData, 0, taosVariantGet(&pExpr[k].base.pParam[0].param, type), false));
      }
    }

    pResult->info.rows = 1;
    goto _exit;
  }

  if (pResult != pSrcBlock) {
    pResult->info.id.groupId = pSrcBlock->info.id.groupId;
    if (pSrcBlock->info.parTbName[0]) {
      tstrncpy(pResult->info.parTbName, pSrcBlock->info.parTbName, TSDB_TABLE_NAME_LEN);
    }
    qTrace("%s, parName:%s,groupId:%" PRIu64, __FUNCTION__, pSrcBlock->info.parTbName, pResult->info.id.groupId);
  }

  // if the source equals to the destination, it is to create a new column as the result of scalar
  // function or some operators.
  bool createNewColModel = (pResult == pSrcBlock);
  if (createNewColModel) {
    TAOS_CHECK_EXIT(blockDataEnsureCapacity(pResult, pResult->info.rows));
  }

  int32_t numOfRows = 0;

  for (int32_t k = 0; k < numOfOutput; ++k) {
    int32_t               outputSlotId = pExpr[k].base.resSchema.slotId;
    SqlFunctionCtx*       pfCtx = &pCtx[k];
    switch (pExpr[k].pExpr->nodeType) {
      case QUERY_NODE_COLUMN: {
        TAOS_CHECK_EXIT(projectApplyColumn(pResult, pSrcBlock, outputSlotId, pfCtx, &numOfRows, createNewColModel));
        break;
      } 
      case QUERY_NODE_VALUE: {
        TAOS_CHECK_EXIT(projectApplyValue(&pExpr[k], pResult, pSrcBlock, outputSlotId, &numOfRows, createNewColModel));
        break;
      } 
      case QUERY_NODE_OPERATOR: {
        TAOS_CHECK_EXIT(projectApplyOperator(&pExpr[k], pResult, pSrcBlock, outputSlotId, &numOfRows, createNewColModel, pExtraParams));
        break;
      } 
      case QUERY_NODE_FUNCTION: {
        TAOS_CHECK_EXIT(projectApplyFunction(pCtx, pfCtx, &pExpr[k], pResult, pSrcBlock, outputSlotId, &numOfRows, createNewColModel, pExtraParams, pPseudoList, &processByRowFunctionCtx, doSelectFunc));
        break;
      }
      default: {
        qError("invalid project expr nodeType:%d", pExpr[k].pExpr->nodeType);
        TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);
      }
    }
  }

  if (processByRowFunctionCtx && taosArrayGetSize(processByRowFunctionCtx) > 0) {
    SqlFunctionCtx** pfCtx = taosArrayGet(processByRowFunctionCtx, 0);
    TSDB_CHECK_NULL(pfCtx, code, lino, _exit, terrno);

    TAOS_CHECK_EXIT((*pfCtx)->fpSet.processFuncByRow(processByRowFunctionCtx));
    numOfRows = (*pfCtx)->resultInfo->numOfRes;
  }

  if (!createNewColModel) {
    pResult->info.rows += numOfRows;
  }

_exit:
  if (processByRowFunctionCtx) {
    taosArrayDestroy(processByRowFunctionCtx);
  }
  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx,
                              int32_t numOfOutput, SArray* pPseudoList, const void* pExtraParams) {
  return projectApplyFunctionsWithSelect(pExpr, pResult, pSrcBlock, pCtx, numOfOutput, pPseudoList, pExtraParams, false, true);
}

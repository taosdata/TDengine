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
#include "osMemPool.h"
#include "querytask.h"
#include "tanalytics.h"
#include "taoserror.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tjson.h"
#include "tmsg.h"

#ifdef USE_ANALYTICS

typedef struct {
  SArray*      blocks;  // SSDataBlock*
  uint64_t     groupId;
  int32_t      numOfRows;
  int32_t      numOfBlocks;
  int64_t      timeout;
  int8_t       wncheck;
  int32_t      targetSlot;
  int32_t      targetType;
  int32_t      tsSlot;
  int32_t      tsPrecision;
  int32_t      numOfCols;
  SAnalyticBuf analyBuf;
  STimeWindow  win;
} SImputationSupp;

typedef struct {
  SOptrBasicInfo  binfo;
  SExprSupp       scalarSup;
  char            algoName[TSDB_ANALYTIC_ALGO_NAME_LEN];
  char            algoUrl[TSDB_ANALYTIC_ALGO_URL_LEN];
  char*           options;
  SColumn         targetCol;
  int32_t         resTsSlot;
  int32_t         resTargetSlot;
  int32_t         resmarkSlot;
  SImputationSupp imputatSup;
} SImputationOperatorInfo;

static void    imputatDestroyOperatorInfo(void* param);
static int32_t imputationNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
static int32_t doImputation(SOperatorInfo* pOperator);
static int32_t doCacheBlock(SImputationSupp* pSupp, SSDataBlock* pBlock, const char* id);
static int32_t doParseInput(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, SNodeList* pFuncs, const char* id);
static int32_t doParseOutput(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, SExprSupp* pExprSup);
static int32_t doParseOption(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, const char* id);
static int32_t doCreateBuf(SImputationSupp* pSupp);

int32_t createImputationOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo,
                                     SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  size_t                    keyBufSize = 0;
  int32_t                   num = 0;
  SExprInfo*                pExprInfo = NULL;
  int32_t                   numOfExprs = 0;
  const char*               id = GET_TASKID(pTaskInfo);
  SHashObj*                 pHashMap = NULL;
  SImputationFuncPhysiNode* pImputatNode = (SImputationFuncPhysiNode*)physiNode;
  SExprSupp*                pExprSup = NULL;
  SImputationSupp*          pSupp = NULL;

  SImputationOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SImputationOperatorInfo));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    code = terrno;
    goto _error;
  }

  pSupp = &pInfo->imputatSup;
  pImputatNode = (SImputationFuncPhysiNode*)physiNode;
  pExprSup = &pOperator->exprSupp;

  code = createExprInfo(pImputatNode->pFuncs, NULL, &pExprInfo, &numOfExprs);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExprSupp(pExprSup, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pImputatNode->pExprs != NULL) {
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pImputatNode->pExprs, NULL, &pScalarExprInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pImputatNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  code = doParseInput(pInfo, pSupp, pImputatNode->pFuncs, id);
  QUERY_CHECK_CODE(code, lino, _error);

  code = doParseOutput(pInfo, pSupp, pExprSup);
  QUERY_CHECK_CODE(code, lino, _error);

  code = doParseOption(pInfo, pSupp, id);
  QUERY_CHECK_CODE(code, lino, _error);

  code = doCreateBuf(pSupp);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->binfo.pRes = createDataBlockFromDescNode(physiNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->binfo.pRes, code, lino, _error, terrno);

  setOperatorInfo(pOperator, "ImputationOperator", QUERY_NODE_PHYSICAL_PLAN_IMPUTATION_FUNC, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, imputationNext, NULL, imputatDestroyOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;

  qDebug("%s forecast env is initialized, option:%s", id, pInfo->options);
  return TSDB_CODE_SUCCESS;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", id, __func__, lino, tstrerror(code));
  }

  if (pInfo != NULL) imputatDestroyOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

static int32_t imputationNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SImputationOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SOptrBasicInfo*          pBInfo = &pInfo->binfo;
  SImputationSupp*         pSupp = &pInfo->imputatSup;
  SSDataBlock*             pRes = pInfo->binfo.pRes;
  int64_t                  st = taosGetTimestampUs();
  int32_t                  numOfBlocks = taosArrayGetSize(pSupp->blocks);
  const char*              idstr = GET_TASKID(pTaskInfo);

  blockDataCleanup(pRes);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    if (pSupp->groupId == 0 || pSupp->groupId == pBlock->info.id.groupId) {
      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks++;
      pSupp->numOfRows += pBlock->info.rows;

      qDebug("group:%" PRId64 ", blocks:%d, rows:%" PRId64 ", total rows:%d", pSupp->groupId, numOfBlocks,
             pBlock->info.rows, pSupp->numOfRows);
      code = doCacheBlock(pSupp, pBlock, idstr);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      qDebug("group:%" PRId64 ", read completed for new group coming, blocks:%d", pSupp->groupId, numOfBlocks);
      code = doImputation(pOperator);
      QUERY_CHECK_CODE(code, lino, _end);

      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks = 1;
      pSupp->numOfRows = pBlock->info.rows;
      qDebug("group:%" PRId64 ", new group, rows:%" PRId64 ", total rows:%d", pSupp->groupId, pBlock->info.rows,
             pSupp->numOfRows);
      code = doCacheBlock(pSupp, pBlock, idstr);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pRes->info.rows > 0) {
      (*ppRes) = pRes;
      qDebug("group:%" PRId64 ", return to upstream, blocks:%d", pRes->info.id.groupId, numOfBlocks);
      return code;
    }
  }

  if (numOfBlocks > 0) {
    qDebug("group:%" PRId64 ", read finish, blocks:%d", pInfo->imputatSup.groupId, numOfBlocks);
    code = doImputation(pOperator);
  }

  int64_t cost = taosGetTimestampUs() - st;
  qDebug("%s all groups finished, cost:%" PRId64 "us", idstr, cost);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", idstr, __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  (*ppRes) = /*(pBInfo->pRes->info.rows == 0) ? NULL : */ pBInfo->pRes;
  return code;
}

static void imputatDestroyOperatorInfo(void* param) {
  SImputationOperatorInfo* pInfo = (SImputationOperatorInfo*)param;
  if (pInfo == NULL) return;

  cleanupBasicInfo(&pInfo->binfo);
  cleanupExprSupp(&pInfo->scalarSup);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->imputatSup.blocks); ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pInfo->imputatSup.blocks, i);
    blockDataDestroy(pBlock);
  }

  taosArrayDestroy(pInfo->imputatSup.blocks);
  taosMemoryFreeClear(param);
}

static int32_t doCacheBlock(SImputationSupp* pSupp, SSDataBlock* pBlock, const char* id) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SAnalyticBuf* pBuf = &pSupp->analyBuf;

  if (pSupp->numOfRows > ANALY_IMPUTATION_INPUT_MAX_ROWS) {
    qError("%s too many rows for imputation, maximum allowed:%d, input:%d", id, ANALY_IMPUTATION_INPUT_MAX_ROWS,
           pSupp->numOfRows);
    return TSDB_CODE_ANA_ANODE_TOO_MANY_ROWS;
  }

  pSupp->numOfBlocks++;
  qDebug("%s block:%d, %p rows:%" PRId64, id, pSupp->numOfBlocks, pBlock, pBlock->info.rows);

  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    SColumnInfoData* pValCol = taosArrayGet(pBlock->pDataBlock, pSupp->targetSlot);
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSupp->tsSlot);
    if (pTsCol == NULL || pValCol == NULL) {
      break;
    }

    int32_t index = 0;

    int64_t ts = ((TSKEY*)pTsCol->pData)[j];
    char*   val = colDataGetData(pValCol, j);
    int16_t valType = pValCol->info.type;

    pSupp->win.skey = MIN(pSupp->win.skey, ts);
    pSupp->win.ekey = MAX(pSupp->win.ekey, ts);

    pSupp->numOfRows++;

    // write the primary time stamp column data
    code = taosAnalyBufWriteColData(pBuf, index++, TSDB_DATA_TYPE_TIMESTAMP, &ts);
    if (TSDB_CODE_SUCCESS != code) {
      qError("%s failed to write ts in buf, code:%s", id, tstrerror(code));
      return code;
    }

    // write the main column for imputation
    code = taosAnalyBufWriteColData(pBuf, index++, valType, val);
    if (TSDB_CODE_SUCCESS != code) {
      qError("%s failed to write val in buf, code:%s", id, tstrerror(code));
      return code;
    }
  }

  return 0;
}

static int32_t anomalyParseJson(SJson* pJson, SArray* pWindows, const char* pId) {
  int32_t     code = 0;
  int32_t     rows = 0;
  STimeWindow win = {0};

  taosArrayClear(pWindows);

  tjsonGetInt32ValueFromDouble(pJson, "rows", rows, code);
  if (code < 0) {
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }

  if (rows < 0) {
    char pMsg[1024] = {0};
    code = tjsonGetStringValue(pJson, "msg", pMsg);
    if (code) {
      qError("%s failed to get error msg from rsp, unknown error", pId);
    } else {
      qError("%s failed to exec forecast, msg:%s", pId, pMsg);
    }

    return TSDB_CODE_ANA_ANODE_RETURN_ERROR;
  } else if (rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SJson* res = tjsonGetObjectItem(pJson, "res");
  if (res == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

  int32_t ressize = tjsonGetArraySize(res);
  if (ressize != rows) return TSDB_CODE_INVALID_JSON_FORMAT;

  for (int32_t i = 0; i < rows; ++i) {
    SJson* row = tjsonGetArrayItem(res, i);
    if (row == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

    int32_t colsize = tjsonGetArraySize(row);
    if (colsize != 2) return TSDB_CODE_INVALID_JSON_FORMAT;

    SJson* start = tjsonGetArrayItem(row, 0);
    SJson* end = tjsonGetArrayItem(row, 1);
    if (start == NULL || end == NULL) {
      qError("%s invalid res from analytic sys, code:%s", pId, tstrerror(TSDB_CODE_INVALID_JSON_FORMAT));
      return TSDB_CODE_INVALID_JSON_FORMAT;
    }

    tjsonGetObjectValueBigInt(start, &win.skey);
    tjsonGetObjectValueBigInt(end, &win.ekey);

    if (win.skey >= win.ekey) {
      win.ekey = win.skey + 1;
    }

    if (taosArrayPush(pWindows, &win) == NULL) {
      qError("%s out of memory in generating anomaly_window", pId);
      return TSDB_CODE_OUT_OF_BUFFER;
    }
  }

  int32_t numOfWins = taosArrayGetSize(pWindows);
  qDebug("%s anomaly window recevied, total:%d", pId, numOfWins);
  for (int32_t i = 0; i < numOfWins; ++i) {
    STimeWindow* pWindow = taosArrayGet(pWindows, i);
    qDebug("%s anomaly win:%d [%" PRId64 ", %" PRId64 ")", pId, i, pWindow->skey, pWindow->ekey);
  }

  return code;
}

static int32_t finishBuildRequest(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, const char* id) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int32_t       code = 0;

  for (int32_t i = 0; i < pBuf->numOfCols; ++i) {
    code = taosAnalyBufWriteColEnd(pBuf, i);
    if (code != 0) return code;
  }

  code = taosAnalyBufWriteDataEnd(pBuf);
  if (code != 0) return code;

  code = taosAnalyBufWriteOptStr(pBuf, "option", pInfo->options);
  if (code != 0) return code;

  code = taosAnalyBufWriteOptStr(pBuf, "algo", pInfo->algoName);
  if (code != 0) return code;

  const char* prec = TSDB_TIME_PRECISION_MILLI_STR;
  if (pSupp->tsPrecision == TSDB_TIME_PRECISION_MICRO) prec = TSDB_TIME_PRECISION_MICRO_STR;
  if (pSupp->tsPrecision == TSDB_TIME_PRECISION_NANO) prec = TSDB_TIME_PRECISION_NANO_STR;
  code = taosAnalyBufWriteOptStr(pBuf, "prec", prec);
  if (code != 0) return code;

  code = taosAnalyBufWriteOptInt(pBuf, ALGO_OPT_WNCHECK_NAME, pSupp->wncheck);
  if (code != 0) return code;

  if (pSupp->numOfRows < ANALY_IMPUTATION_INPUT_MIN_ROWS) {
    qError("%s history rows for forecasting not enough, min required:%d, current:%d", id, ANALY_FORECAST_MIN_ROWS,
           pSupp->numOfRows);
    return TSDB_CODE_ANA_ANODE_NOT_ENOUGH_ROWS;
  }

  code = taosAnalyBufClose(pBuf);
  return code;
}

static int32_t doImputationImpl(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, SSDataBlock* pBlock, const char* pId) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int32_t       resCurRow = pBlock->info.rows;
  int64_t       tmpI64 = 0;
  float         tmpFloat = 0;
  double        tmpDouble = 0;
  int32_t       code = 0;

  SColumnInfoData* pResValCol = taosArrayGet(pBlock->pDataBlock, pInfo->resTargetSlot);
  if (NULL == pResValCol) {
    return terrno;
  }

  SColumnInfoData* pResTsCol = ((pInfo->resTsSlot != -1) ? taosArrayGet(pBlock->pDataBlock, pInfo->resTsSlot) : NULL);
  SJson* pJson = taosAnalySendReqRetJson(pInfo->algoUrl, ANALYTICS_HTTP_TYPE_POST, pBuf, pSupp->timeout);
  if (pJson == NULL) {
    return terrno;
  }

  int32_t rows = 0;
  tjsonGetInt32ValueFromDouble(pJson, "rows", rows, code);
  if (rows < 0 && code == 0) {
    char pMsg[1024] = {0};
    code = tjsonGetStringValue(pJson, "msg", pMsg);
    if (code != 0) {
      qError("%s failed to get msg from rsp, unknown error", pId);
    } else {
      qError("%s failed to exec forecast, msg:%s", pId, pMsg);
    }

    tjsonDelete(pJson);
    return TSDB_CODE_ANA_ANODE_RETURN_ERROR;
  }

  if (code < 0) {
    goto _OVER;
  }

  SJson* res = tjsonGetObjectItem(pJson, "res");
  if (res == NULL) goto _OVER;

  if (pResTsCol != NULL) {
    resCurRow = pBlock->info.rows;
    SJson* tsJsonArray = tjsonGetArrayItem(res, 0);
    if (tsJsonArray == NULL) goto _OVER;
    int32_t tsSize = tjsonGetArraySize(tsJsonArray);
    if (tsSize != rows) goto _OVER;
    for (int32_t i = 0; i < tsSize; ++i) {
      SJson* tsJson = tjsonGetArrayItem(tsJsonArray, i);
      tjsonGetObjectValueBigInt(tsJson, &tmpI64);
      colDataSetInt64(pResTsCol, resCurRow, &tmpI64);
      resCurRow++;
    }
  }

  resCurRow = pBlock->info.rows;
  SJson* valJsonArray = tjsonGetArrayItem(res, 1);
  if (valJsonArray == NULL) goto _OVER;
  int32_t valSize = tjsonGetArraySize(valJsonArray);
  if (valSize != rows) goto _OVER;
  for (int32_t i = 0; i < valSize; ++i) {
    SJson* valJson = tjsonGetArrayItem(valJsonArray, i);
    tjsonGetObjectValueDouble(valJson, &tmpDouble);

    colDataSetDouble(pResValCol, resCurRow, &tmpDouble);
    resCurRow++;
  }

  pBlock->info.rows += rows;

  if (pJson != NULL) tjsonDelete(pJson);
  return 0;

_OVER:
  tjsonDelete(pJson);
  if (code == 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
  }

  qError("%s failed to perform forecast finalize since %s", pId, tstrerror(code));
  return code;
}

static int32_t doImputation(SOperatorInfo* pOperator) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SImputationOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  char*                    id = GET_TASKID(pTaskInfo);
  SSDataBlock*             pRes = pInfo->binfo.pRes;
  SImputationSupp*         pSupp = &pInfo->imputatSup;

  int32_t numOfBlocks = (int32_t)taosArrayGetSize(pSupp->blocks);
  if (numOfBlocks == 0) goto _OVER;

  qDebug("%s group:%" PRId64 ", aggregate blocks, blocks:%d", id, pSupp->groupId, numOfBlocks);
  pRes->info.id.groupId = pSupp->groupId;

  code = finishBuildRequest(pInfo, pSupp, id);
  QUERY_CHECK_CODE(code, lino, _end);

  //   if (pBlock->info.rows < pBlock->info.capacity) {
  //   return TSDB_CODE_SUCCESS;
  // }

  // int32_t code = blockDataEnsureCapacity(pBlock, newRowsNum);
  // if (code != TSDB_CODE_SUCCESS) {
  //   qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  //   return code;
  // }

  // QUERY_CHECK_CODE(code, lino, _end);

  code = doImputationImpl(pInfo, pSupp, pRes, id);
  QUERY_CHECK_CODE(code, lino, _end);

  uInfo("%s block:%d, forecast finalize", id, pSupp->numOfBlocks);

_end:
  pSupp->numOfBlocks = 0;
  taosAnalyBufDestroy(&pSupp->analyBuf);
  return code;

_OVER:
  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pSupp->blocks, i);
    qDebug("%s, clear block, pBlock:%p pBlock->pDataBlock:%p", __func__, pBlock, pBlock->pDataBlock);
    blockDataDestroy(pBlock);
  }

  taosArrayClear(pSupp->blocks);
  pSupp->numOfRows = 0;
  return code;
}

static int32_t doParseInput(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, SNodeList* pFuncs, const char* id) {
  int32_t code = 0;
  SNode*  pNode = NULL;
  
  pSupp->tsSlot = -1;
  pSupp->targetSlot = -1;
  pSupp->targetType = -1;
  pSupp->tsPrecision = -1;

  FOREACH(pNode, pFuncs) {
    if ((nodeType(pNode) == QUERY_NODE_TARGET) && (nodeType(((STargetNode*)pNode)->pExpr) == QUERY_NODE_FUNCTION)) {
      SFunctionNode* pFunc = (SFunctionNode*)((STargetNode*)pNode)->pExpr;
      int32_t        numOfParam = LIST_LENGTH(pFunc->pParameterList);

      if (pFunc->funcType == FUNCTION_TYPE_IMPUTATION) {
        // code = validInputParams(pFunc, id);
        // if (code) {
          // return code;
        // }

        pSupp->numOfCols = 2;

        if (numOfParam == 2) {
          // column, ts
          SColumnNode* pValNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
          SColumnNode* pTsNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 1);

          pSupp->tsSlot = pTsNode->slotId;
          pSupp->tsPrecision = pTsNode->node.resType.precision;
          pSupp->targetSlot = pValNode->slotId;
          pSupp->targetType = pValNode->node.resType.type;

          // let's set the moment as the default imputation algorithm
          pInfo->options = taosStrdup("algo=moment-imputation");
        } else {
          // column, options, ts
          SColumnNode* pTargetNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
          if (nodeType(pTargetNode) != QUERY_NODE_COLUMN) {
            // return error
          }

          bool assignTs = false;
          bool assignOpt = false;

          pSupp->targetSlot = pTargetNode->slotId;
          pSupp->targetType = pTargetNode->node.resType.type;

          for (int32_t i = 0; i < pFunc->pParameterList->length; ++i) {
            SNode* pNode = nodesListGetNode(pFunc->pParameterList, i);
            if (nodeType(pNode) == QUERY_NODE_COLUMN) {
              SColumnNode* pColNode = (SColumnNode*)pNode;
              if (pColNode->isPrimTs && (!assignTs)) {
                pSupp->tsSlot = pColNode->slotId;
                pSupp->tsPrecision = pColNode->node.resType.precision;
                assignTs = true;
                continue;
              }
            } else if (nodeType(pNode) == QUERY_NODE_VALUE) {
              if (!assignOpt) {
                SValueNode* pOptNode = (SValueNode*)pNode;
                pInfo->options = taosStrdup(pOptNode->literal);
                assignOpt = true;
                continue;
              }
            }
          }

          if (!assignOpt) {
            qError("%s option is missing, failed to do imputation", id);
            code = TSDB_CODE_ANA_INTERNAL_ERROR;
          }
        }
      }
    }
  }

  if (pInfo->options == NULL) {
    qError("%s option is missing or clone option string failed, failed to do imputation", id);
    code = TSDB_CODE_ANA_INTERNAL_ERROR;
  }

  return code;
}

static int32_t doParseOutput(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, SExprSupp* pExprSup) {
  pInfo->resTsSlot = -1;
  pInfo->resTargetSlot = -1;

  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];
    int32_t    dstSlot = pExprInfo->base.resSchema.slotId;
    if (pExprInfo->pExpr->_function.functionType == FUNCTION_TYPE_FORECAST) {
      pInfo->resTargetSlot = dstSlot;
    } else if (pExprInfo->pExpr->_function.functionType == FUNCTION_TYPE_FORECAST_ROWTS) {
      pInfo->resTsSlot = dstSlot;
    }
  }

  return 0;
}

static void doInitOptions(SImputationSupp* pSupp) {
  // pSupp->win = TIMEWINDOW;
  pSupp->numOfRows = 0;
  pSupp->wncheck = ANALY_DEFAULT_WNCHECK;
}

int32_t doParseOption(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, const char* id) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SHashObj* pHashMap = NULL;

  doInitOptions(pSupp);

  code = taosAnalyGetOpts(pInfo->options, &pHashMap);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = taosAnalysisParseAlgo(pInfo->options, pInfo->algoName, pInfo->algoUrl, ANALY_ALGO_TYPE_IMPUTATION,
                               tListLen(pInfo->algoUrl), pHashMap, id);
  TSDB_CHECK_CODE(code, lino, _end);

  // code = filterNotSupportForecast(pSupp);
  // if (code) {
  //   qError("%s not support forecast model, %s", id, pSupp->algoName);
  //   TSDB_CHECK_CODE(code, lino, _end);
  // }

  // extract the timeout parameter
  pSupp->timeout = taosAnalysisParseTimout(pHashMap, id);
  pSupp->wncheck = taosAnalysisParseWncheck(pHashMap, id);

  // extract the forecast rows
  // char* pRows = taosHashGet(pHashMap, ALGO_OPT_FORECASTROWS_NAME, strlen(ALGO_OPT_FORECASTROWS_NAME));
  // if (pRows != NULL) {
  //   int64_t v = 0;
  //   code = toInteger(pRows, taosHashGetValueSize(pRows), 10, &v);

  //   pSupp->forecastRows = v;
  //   qDebug("%s forecast rows:%"PRId64, id, pSupp->forecastRows);
  // } else {
  //   qDebug("%s forecast rows not found:%s, use default:%" PRId64, id, pInfo->options, pSupp->forecastRows);
  // }

  // if (pSupp->forecastRows > ANALY_FORECAST_RES_MAX_ROWS) {
  //   qError("%s required too many forecast rows, max allowed:%d, required:%" PRId64, id, ANALY_FORECAST_RES_MAX_ROWS,
  //          pSupp->forecastRows);
  //   code = TSDB_CODE_ANA_ANODE_TOO_MANY_ROWS;
  //   goto _end;
  // }

  // // extract the start timestamp
  // char* pStart = taosHashGet(pHashMap, ALGO_OPT_START_NAME, strlen(ALGO_OPT_START_NAME));
  // if (pStart != NULL) {
  //   int64_t v = 0;
  //   code = toInteger(pStart, taosHashGetValueSize(pStart), 10, &v);
  //   pSupp->startTs = v;
  //   pSupp->setStart = 1;
  //   qDebug("%s forecast set start ts:%"PRId64, id, pSupp->startTs);
  // }

  // // extract the time step
  // char* pEvery = taosHashGet(pHashMap, ALGO_OPT_EVERY_NAME, strlen(ALGO_OPT_EVERY_NAME));
  // if (pEvery != NULL) {
  //   int64_t v = 0;
  //   code = toInteger(pEvery, taosHashGetValueSize(pEvery), 10, &v);
  //   pSupp->every = v;
  //   pSupp->setEvery = 1;
  //   qDebug("%s forecast set every ts:%"PRId64, id, pSupp->every);
  // }

_end:
  taosHashCleanup(pHashMap);
  return code;
}

static int32_t doCreateBuf(SImputationSupp* pSupp) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int64_t       ts = 0;  // taosGetTimestampMs();
  int32_t       index = 0;

  pBuf->bufType = ANALYTICS_BUF_TYPE_JSON_COL;
  snprintf(pBuf->fileName, sizeof(pBuf->fileName), "%s/tdengine-imput-%" PRId64, tsTempDir, ts);

  int32_t code = tsosAnalyBufOpen(pBuf, pSupp->numOfCols);
  if (code != 0) goto _OVER;

  code = taosAnalyBufWriteColMeta(pBuf, index++, TSDB_DATA_TYPE_TIMESTAMP, "ts");
  if (code != 0) goto _OVER;

  code = taosAnalyBufWriteColMeta(pBuf, index++, pSupp->targetType, "val");
  if (code != 0) goto _OVER;

  code = taosAnalyBufWriteDataBegin(pBuf);
  if (code != 0) goto _OVER;

  for (int32_t i = 0; i < pSupp->numOfCols; ++i) {
    code = taosAnalyBufWriteColBegin(pBuf, i);
    if (code != 0) goto _OVER;
  }

_OVER:
  if (code != 0) {
    (void)taosAnalyBufClose(pBuf);
    taosAnalyBufDestroy(pBuf);
  }
  return code;
}

#else

int32_t createImputationOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo,
                                     SOperatorInfo** pOptrInfo) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}
void imputatDestroyOperatorInfo(void* param) {}

#endif
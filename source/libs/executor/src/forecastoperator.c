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
#include "tanalytics.h"
#include "taoserror.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tmsg.h"
#include "tmsg.h"

#ifdef USE_ANALYTICS

#define ALGO_OPT_RETCONF_NAME      "return_conf"
#define ALGO_OPT_FORECASTROWS_NAME "rows"
#define ALGO_OPT_CONF_NAME         "conf"
#define ALGO_OPT_START_NAME        "start"
#define ALGO_OPT_EVERY_NAME        "every"

typedef struct {
  char*           pName;
  SColumnInfoData data;
  SColumn         col;
  int32_t         numOfRows;
} SColFutureData;

typedef struct {
  char         algoName[TSDB_ANALYTIC_ALGO_NAME_LEN];
  char         algoUrl[TSDB_ANALYTIC_ALGO_URL_LEN];
  char*        pOptions;
  int64_t      maxTs;
  int64_t      minTs;
  int64_t      numOfRows;
  uint64_t     groupId;
  int64_t      forecastRows;
  int64_t      cachedRows;
  int32_t      numOfBlocks;
  int64_t      timeout;
  int16_t      resTsSlot;
  int16_t      resValSlot;
  int16_t      resLowSlot;
  int16_t      resHighSlot;
  int16_t      inputTsSlot;
  int16_t      targetValSlot;
  int8_t       targetValType;
  int8_t       inputPrecision;
  int8_t       wncheck;
  double       conf;
  int64_t      startTs;
  int64_t      every;
  int8_t       setStart;
  int8_t       setEvery;
  SArray*      pCovariateSlotList;   // covariate slot list
  SArray*      pDynamicRealList;     // dynamic real data list
  int32_t      numOfInputCols;
  SAnalyticBuf analyBuf;
} SForecastSupp;

typedef struct SForecastOperatorInfo {
  SSDataBlock*  pRes;
  SExprSupp     scalarSup;  // scalar calculation
  SForecastSupp forecastSupp;
} SForecastOperatorInfo;

static void    destroyForecastInfo(void* param);
static int32_t forecastParseOpt(SForecastSupp* pSupp, const char* id);

static FORCE_INLINE int32_t forecastEnsureBlockCapacity(SSDataBlock* pBlock, int32_t newRowsNum) {
  if (pBlock->info.rows < pBlock->info.capacity) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = blockDataEnsureCapacity(pBlock, newRowsNum);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t forecastCacheBlock(SForecastSupp* pSupp, SSDataBlock* pBlock, const char* id) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SAnalyticBuf* pBuf = &pSupp->analyBuf;

  if (pSupp->cachedRows > ANALY_FORECAST_MAX_ROWS) {
    code = TSDB_CODE_ANA_ANODE_TOO_MANY_ROWS;
    qError("%s rows:%" PRId64 " for forecast cache, error happens, code:%s, upper limit:%d", id, pSupp->cachedRows,
           tstrerror(code), ANALY_FORECAST_MAX_ROWS);
    return code;
  }

  pSupp->numOfBlocks++;
  qDebug("%s block:%d, %p rows:%" PRId64, id, pSupp->numOfBlocks, pBlock, pBlock->info.rows);

  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    SColumnInfoData* pValCol = taosArrayGet(pBlock->pDataBlock, pSupp->targetValSlot);
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSupp->inputTsSlot);
    if (pTsCol == NULL || pValCol == NULL) break;

    int32_t index = 0;
    int64_t ts = ((TSKEY*)pTsCol->pData)[j];
    char*   val = colDataGetData(pValCol, j);
    int16_t valType = pValCol->info.type;

    pSupp->minTs = MIN(pSupp->minTs, ts);
    pSupp->maxTs = MAX(pSupp->maxTs, ts);
    pSupp->numOfRows++;

    // write the primary time stamp column data
    code = taosAnalyBufWriteColData(pBuf, index++, TSDB_DATA_TYPE_TIMESTAMP, &ts);
    if (TSDB_CODE_SUCCESS != code) {
      qError("%s failed to write ts in buf, code:%s", id, tstrerror(code));
      return code;
    }

    // write the main column for forecasting
    code = taosAnalyBufWriteColData(pBuf, index++, valType, val);
    if (TSDB_CODE_SUCCESS != code) {
      qError("%s failed to write val in buf, code:%s", id, tstrerror(code));
      return code;
    }

    // let's handle the dynamic_real_columns
    for (int32_t i = 0; i < taosArrayGetSize(pSupp->pDynamicRealList); ++i) {
      SColFutureData*  pCol = taosArrayGet(pSupp->pDynamicRealList, i);
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, pCol->col.slotId);

      char* pVal = colDataGetData(pColData, j);
      code = taosAnalyBufWriteColData(pBuf, index++, pCol->col.type, pVal);
      if (TSDB_CODE_SUCCESS != code) {
        qError("%s failed to write val in buf, code:%s", id, tstrerror(code));
        return code;
      }
    }

    // now handle the past_dynamic_real columns and
    for (int32_t i = 0; i < taosArrayGetSize(pSupp->pCovariateSlotList); ++i) {
      SColumn*         pCol = taosArrayGet(pSupp->pCovariateSlotList, i);
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, pCol->slotId);

      char* pVal = colDataGetData(pColData, j);
      code = taosAnalyBufWriteColData(pBuf, index++, pCol->type, pVal);
      if (TSDB_CODE_SUCCESS != code) {
        qError("%s failed to write val in buf, code:%s", id, tstrerror(code));
        return code;
      }
    }
  }

  return 0;
}

static int32_t forecastCloseBuf(SForecastSupp* pSupp, const char* id) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int32_t       code = 0;

  // add the future dynamic real column data

  for (int32_t i = 0; i < pSupp->numOfInputCols; ++i) {
    // the primary timestamp and the forecast column
    // add the future dynamic real column data
    if ((i >= 2) && ((i - 2) < taosArrayGetSize(pSupp->pDynamicRealList))) {
      SColFutureData* pCol = taosArrayGet(pSupp->pDynamicRealList, i - 2);
      int16_t         valType = pCol->col.type;
      for (int32_t j = 0; j < pCol->numOfRows; ++j) {
        char* pVal = colDataGetData(&pCol->data, j);
        code = taosAnalyBufWriteColData(pBuf, i, valType, pVal);
        if (code != 0) {
          return code;
        }
      }
    }

    code = taosAnalyBufWriteColEnd(pBuf, i);
    if (code != 0) return code;
  }

  code = taosAnalyBufWriteDataEnd(pBuf);
  if (code != 0) return code;

  code = taosAnalyBufWriteOptStr(pBuf, "option", pSupp->pOptions);
  if (code != 0) return code;

  code = taosAnalyBufWriteOptStr(pBuf, "algo", pSupp->algoName);
  if (code != 0) return code;

  const char* prec = TSDB_TIME_PRECISION_MILLI_STR;
  if (pSupp->inputPrecision == TSDB_TIME_PRECISION_MICRO) prec = TSDB_TIME_PRECISION_MICRO_STR;
  if (pSupp->inputPrecision == TSDB_TIME_PRECISION_NANO) prec = TSDB_TIME_PRECISION_NANO_STR;
  code = taosAnalyBufWriteOptStr(pBuf, "prec", prec);
  if (code != 0) return code;

  code = taosAnalyBufWriteOptInt(pBuf, ALGO_OPT_WNCHECK_NAME, pSupp->wncheck);
  if (code != 0) return code;

  bool noConf = (pSupp->resHighSlot == -1 && pSupp->resLowSlot == -1);
  code = taosAnalyBufWriteOptInt(pBuf, ALGO_OPT_RETCONF_NAME, !noConf);
  if (code != 0) return code;

  if (pSupp->cachedRows < ANALY_FORECAST_MIN_ROWS) {
    qError("%s history rows for forecasting not enough, min required:%d, current:%" PRId64, id, ANALY_FORECAST_MIN_ROWS,
           pSupp->forecastRows);
    return TSDB_CODE_ANA_ANODE_NOT_ENOUGH_ROWS;
  }

  if (pSupp->cachedRows < ANALY_TDTSFM_FORECAST_MIN_ROWS && strcmp(pSupp->algoName, "tdtsfm_1") == 0) {
    qError("%s history rows for forecasting when using tdtsfm model not enough, min required:%d, current:%" PRId64, id,
           ANALY_TDTSFM_FORECAST_MIN_ROWS, pSupp->forecastRows);
    return TSDB_CODE_ANA_ANODE_NOT_ENOUGH_ROWS;
  }

  code = taosAnalyBufWriteOptInt(pBuf, "forecast_rows", pSupp->forecastRows);
  if (code != 0) return code;

  code = taosAnalyBufWriteOptFloat(pBuf, "conf", pSupp->conf);
  if (code != 0) return code;

  int32_t len = strlen(pSupp->pOptions);
  int64_t every = (pSupp->setEvery != 0) ? pSupp->every : ((pSupp->maxTs - pSupp->minTs) / (pSupp->numOfRows - 1));
  code = taosAnalyBufWriteOptInt(pBuf, "every", every);
  if (code != 0) return code;

  int64_t start = (pSupp->setStart != 0) ? pSupp->startTs : pSupp->maxTs + every;
  code = taosAnalyBufWriteOptInt(pBuf, "start", start);
  if (code != 0) return code;

  if (taosArrayGetSize(pSupp->pCovariateSlotList) + taosArrayGetSize(pSupp->pDynamicRealList) > 0) {
    code = taosAnalyBufWriteOptStr(pBuf, "type", "covariate");
    if (code != 0) return code;
  }

  code = taosAnalyBufClose(pBuf);
  return code;
}

static int32_t forecastAnalysis(SForecastSupp* pSupp, SSDataBlock* pBlock, const char* pId) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int32_t       resCurRow = pBlock->info.rows;
  int64_t       tmpI64 = 0;
  double        tmpDouble = 0;
  int32_t       code = 0;

  SColumnInfoData* pResValCol = taosArrayGet(pBlock->pDataBlock, pSupp->resValSlot);
  if (NULL == pResValCol) {
    return terrno;
  }

  SColumnInfoData* pResTsCol = ((pSupp->resTsSlot != -1) ? taosArrayGet(pBlock->pDataBlock, pSupp->resTsSlot) : NULL);
  SColumnInfoData* pResLowCol =
      ((pSupp->resLowSlot != -1) ? taosArrayGet(pBlock->pDataBlock, pSupp->resLowSlot) : NULL);
  SColumnInfoData* pResHighCol =
      (pSupp->resHighSlot != -1 ? taosArrayGet(pBlock->pDataBlock, pSupp->resHighSlot) : NULL);

  SJson* pJson = taosAnalySendReqRetJson(pSupp->algoUrl, ANALYTICS_HTTP_TYPE_POST, pBuf, pSupp->timeout, pId);
  if (pJson == NULL) {
    return terrno;
  }

  int32_t rows = 0;
  tjsonGetInt32ValueFromDouble(pJson, "rows", rows, code);
  if (rows < 0 && code == 0) {
    code = parseErrorMsgFromAnalyticServer(pJson, pId);
    tjsonDelete(pJson);
    return code;
  }

  // invalid json format
  if (code != 0) {
    goto _OVER;
  }

  SJson* res = tjsonGetObjectItem(pJson, "res");
  if (res == NULL) goto _OVER;
  int32_t ressize = tjsonGetArraySize(res);
  bool    returnConf = (pSupp->resHighSlot != -1 || pSupp->resLowSlot != -1);

  if ((returnConf && (ressize != 4)) || ((!returnConf) && (ressize != 2))) {
    goto _OVER;
  }

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

  if (pResLowCol != NULL) {
    resCurRow = pBlock->info.rows;
    SJson* lowJsonArray = tjsonGetArrayItem(res, 2);
    if (lowJsonArray == NULL) goto _OVER;
    int32_t lowSize = tjsonGetArraySize(lowJsonArray);
    if (lowSize != rows) goto _OVER;
    for (int32_t i = 0; i < lowSize; ++i) {
      SJson* lowJson = tjsonGetArrayItem(lowJsonArray, i);
      tjsonGetObjectValueDouble(lowJson, &tmpDouble);
      colDataSetDouble(pResLowCol, resCurRow, &tmpDouble);
      resCurRow++;
    }
  }

  if (pResHighCol != NULL) {
    resCurRow = pBlock->info.rows;
    SJson* highJsonArray = tjsonGetArrayItem(res, 3);
    if (highJsonArray == NULL) goto _OVER;
    int32_t highSize = tjsonGetArraySize(highJsonArray);
    if (highSize != rows) goto _OVER;
    for (int32_t i = 0; i < highSize; ++i) {
      SJson* highJson = tjsonGetArrayItem(highJsonArray, i);
      tjsonGetObjectValueDouble(highJson, &tmpDouble);
      colDataSetDouble(pResHighCol, resCurRow, &tmpDouble);
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

static int32_t forecastAggregateBlocks(SForecastSupp* pSupp, SSDataBlock* pResBlock, const char* pId) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SAnalyticBuf* pBuf = &pSupp->analyBuf;

  code = forecastCloseBuf(pSupp, pId);
  QUERY_CHECK_CODE(code, lino, _end);

  code = forecastEnsureBlockCapacity(pResBlock, 1);
  QUERY_CHECK_CODE(code, lino, _end);

  code = forecastAnalysis(pSupp, pResBlock, pId);
  QUERY_CHECK_CODE(code, lino, _end);

  uInfo("%s block:%d, forecast finalize", pId, pSupp->numOfBlocks);

_end:
  pSupp->numOfBlocks = 0;
  taosAnalyBufDestroy(&pSupp->analyBuf);
  return code;
}

static int32_t forecastNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  SExecTaskInfo*         pTaskInfo = pOperator->pTaskInfo;
  SForecastOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*           pResBlock = pInfo->pRes;
  SForecastSupp*         pSupp = &pInfo->forecastSupp;
  SExprSupp*             pScalarSupp = &pInfo->scalarSup;
  SAnalyticBuf*          pBuf = &pSupp->analyBuf;
  int64_t                st = taosGetTimestampUs();
  int32_t                numOfBlocks = pSupp->numOfBlocks;
  const char*            pId = GET_TASKID(pOperator->pTaskInfo);

  blockDataCleanup(pResBlock);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    if (pScalarSupp->pExprInfo != NULL) {
      code = projectApplyFunctions(pScalarSupp->pExprInfo, pBlock, pBlock, pScalarSupp->pCtx, pScalarSupp->numOfExprs,
                                   NULL, GET_STM_RTINFO(pOperator->pTaskInfo));
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }

    if (pSupp->groupId == 0 || pSupp->groupId == pBlock->info.id.groupId) {
      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks++;
      pSupp->cachedRows += pBlock->info.rows;
      qDebug("%s group:%" PRId64 ", blocks:%d, rows:%" PRId64 ", total rows:%" PRId64, pId, pSupp->groupId, numOfBlocks,
             pBlock->info.rows, pSupp->cachedRows);
      code = forecastCacheBlock(pSupp, pBlock, pId);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      qDebug("%s group:%" PRId64 ", read finish for new group coming, blocks:%d", pId, pSupp->groupId, numOfBlocks);
      code = forecastAggregateBlocks(pSupp, pResBlock, pId);
      QUERY_CHECK_CODE(code, lino, _end);
      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks = 1;
      pSupp->cachedRows = pBlock->info.rows;
      qDebug("%s group:%" PRId64 ", new group, rows:%" PRId64 ", total rows:%" PRId64, pId, pSupp->groupId,
             pBlock->info.rows, pSupp->cachedRows);
      code = forecastCacheBlock(pSupp, pBlock, pId);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pResBlock->info.rows > 0) {
      (*ppRes) = pResBlock;
      qDebug("%s group:%" PRId64 ", return to upstream, blocks:%d", pId, pResBlock->info.id.groupId, numOfBlocks);
      return code;
    }
  }

  if (numOfBlocks > 0) {
    qDebug("%s group:%" PRId64 ", read finish, blocks:%d", pId, pSupp->groupId, numOfBlocks);
    code = forecastAggregateBlocks(pSupp, pResBlock, pId);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  int64_t cost = taosGetTimestampUs() - st;
  qDebug("%s all groups finished, cost:%" PRId64 "us", pId, cost);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", pId, __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  (*ppRes) = (pResBlock->info.rows == 0) ? NULL : pResBlock;
  return code;
}

static int32_t forecastParseOutput(SForecastSupp* pSupp, SExprSupp* pExprSup) {
  pSupp->resLowSlot = -1;
  pSupp->resHighSlot = -1;
  pSupp->resTsSlot = -1;
  pSupp->resValSlot = -1;

  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];
    int32_t    dstSlot = pExprInfo->base.resSchema.slotId;
    if (pExprInfo->pExpr->_function.functionType == FUNCTION_TYPE_FORECAST) {
      pSupp->resValSlot = dstSlot;
    } else if (pExprInfo->pExpr->_function.functionType == FUNCTION_TYPE_FORECAST_ROWTS) {
      pSupp->resTsSlot = dstSlot;
    } else if (pExprInfo->pExpr->_function.functionType == FUNCTION_TYPE_FORECAST_LOW) {
      pSupp->resLowSlot = dstSlot;
    } else if (pExprInfo->pExpr->_function.functionType == FUNCTION_TYPE_FORECAST_HIGH) {
      pSupp->resHighSlot = dstSlot;
    } else {
    }
  }

  return 0;
}

static int32_t validInputParams(SFunctionNode* pFunc, const char* id) {
  int32_t code = 0;
  int32_t num = LIST_LENGTH(pFunc->pParameterList);

  if (num <= 1) {
    qError("%s invalid number of parameters:%d", id, num);
    code = TSDB_CODE_PLAN_INTERNAL_ERROR;
    goto _end;
  }

  for (int32_t i = 0; i < num; ++i) {
    SNode* p = nodesListGetNode(pFunc->pParameterList, i);
    if (p == NULL) {
      code = TSDB_CODE_PLAN_INTERNAL_ERROR;
      qError("%s %d-th parameter in forecast function is NULL, code:%s", id, i, tstrerror(code));
      goto _end;
    }
  }

  if (num == 2) {  // column_name, timestamp_column_name
    SNode* p1 = nodesListGetNode(pFunc->pParameterList, 0);
    SNode* p2 = nodesListGetNode(pFunc->pParameterList, 1);

    if (nodeType(p1) != QUERY_NODE_COLUMN || nodeType(p2) != QUERY_NODE_COLUMN) {
      qError("%s invalid column type, column 1:%d, column 2:%d", id, nodeType(p1), nodeType(p2));
      code = TSDB_CODE_PLAN_INTERNAL_ERROR;
      goto _end;
    }
  } else if (num >= 3) {
    // column_name_#1, column_name_#2...., analytics_options, timestamp_column_name, primary_key_column if exists
    // column_name_#1, timestamp_column_name, primary_key_column if exists
    // column_name_#1, analytics_options, timestamp_column_name
    SNode* pTarget = nodesListGetNode(pFunc->pParameterList, 0);
    if (nodeType(pTarget) != QUERY_NODE_COLUMN) {
      code = TSDB_CODE_PLAN_INTERNAL_ERROR;
      qError("%s first parameter is not valid column in forecast function", id);
      goto _end;
    }

    SNode* pNode = nodesListGetNode(pFunc->pParameterList, num - 1);
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      qError("%s last parameter is not valid column, actual:%d", id, nodeType(pNode));
      code = TSDB_CODE_PLAN_INTERNAL_ERROR;
    }
  }

_end:
  if (code) {
    qError("%s valid the parameters failed, code:%s", id, tstrerror(code));
  }
  return code;
}

static bool existInList(SForecastSupp* pSupp, int32_t slotId) {
  for (int32_t j = 0; j < taosArrayGetSize(pSupp->pCovariateSlotList); ++j) {
    SColumn* pCol = taosArrayGet(pSupp->pCovariateSlotList, j);

    if (pCol->slotId == slotId) {
      return true;
    }
  }

  return false;
}

static int32_t forecastParseInput(SForecastSupp* pSupp, SNodeList* pFuncs, const char* id) {
  int32_t code = 0;
  SNode*  pNode = NULL;
  
  pSupp->inputTsSlot = -1;
  pSupp->targetValSlot = -1;
  pSupp->targetValType = -1;
  pSupp->inputPrecision = -1;

  FOREACH(pNode, pFuncs) {
    if ((nodeType(pNode) == QUERY_NODE_TARGET) && (nodeType(((STargetNode*)pNode)->pExpr) == QUERY_NODE_FUNCTION)) {
      SFunctionNode* pFunc = (SFunctionNode*)((STargetNode*)pNode)->pExpr;
      int32_t        numOfParam = LIST_LENGTH(pFunc->pParameterList);

      if (pFunc->funcType == FUNCTION_TYPE_FORECAST) {
        code = validInputParams(pFunc, id);
        if (code) {
          return code;
        }

        pSupp->numOfInputCols = 2;

        if (numOfParam == 2) {
          // column, ts
          SColumnNode* pTarget = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
          SColumnNode* pTsNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 1);

          pSupp->inputTsSlot = pTsNode->slotId;
          pSupp->inputPrecision = pTsNode->node.resType.precision;
          pSupp->targetValSlot = pTarget->slotId;
          pSupp->targetValType = pTarget->node.resType.type;

          // let's add the holtwinters as the default forecast algorithm
          pSupp->pOptions = taosStrdup("algo=holtwinters");
          if (pSupp->pOptions == NULL) {
            qError("%s failed to dup forecast option, code:%s", id, tstrerror(terrno));
            return terrno;
          }
        } else {
          SColumnNode* pTarget = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
          bool         assignTs = false;
          bool         assignOpt = false;

          pSupp->targetValSlot = pTarget->slotId;
          pSupp->targetValType = pTarget->node.resType.type;

          // set the primary ts column and option info
          for (int32_t i = 0; i < numOfParam; ++i) {
            SNode* pNode = nodesListGetNode(pFunc->pParameterList, i);
            if (nodeType(pNode) == QUERY_NODE_COLUMN) {
              SColumnNode* pColNode = (SColumnNode*)pNode;
              if (pColNode->isPrimTs && (!assignTs)) {
                pSupp->inputTsSlot = pColNode->slotId;
                pSupp->inputPrecision = pColNode->node.resType.precision;
                assignTs = true;
                continue;
              }
            } else if (nodeType(pNode) == QUERY_NODE_VALUE) {
              if (!assignOpt) {
                SValueNode* pOptNode = (SValueNode*)pNode;
                pSupp->pOptions = taosStrdup(pOptNode->literal);
                assignOpt = true;
                continue;
              }
            }
          }

          if (!assignOpt) {
            // set the default forecast option
            pSupp->pOptions = taosStrdup("algo=holtwinters");
            if (pSupp->pOptions == NULL) {
              qError("%s failed to dup forecast option, code:%s", id, tstrerror(terrno));
              return terrno;
            }
          }

          pSupp->pCovariateSlotList = taosArrayInit(4, sizeof(SColumn));
          pSupp->pDynamicRealList = taosArrayInit(4, sizeof(SColFutureData));

          // the first is the target column
          for (int32_t i = 1; i < numOfParam; ++i) {
            SColumnNode* p = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, i);
            if ((nodeType(p) != QUERY_NODE_COLUMN) || (nodeType(p) == QUERY_NODE_COLUMN && p->isPrimTs)) {
              break;
            }

            if (p->slotId == pSupp->targetValSlot) {
              continue; // duplicate the target column, ignore it
            }

            bool exist = existInList(pSupp, p->slotId);
            if (exist) {
              continue;  // duplicate column, ignore it
            }

            SColumn col = {.slotId = p->slotId,
                           .colType = p->colType,
                           .type = p->node.resType.type,
                           .bytes = p->node.resType.bytes};

            tstrncpy(col.name, p->colName, tListLen(col.name));
            void* pRet = taosArrayPush(pSupp->pCovariateSlotList, &col);
            if (pRet == NULL) {
              code = terrno;
              qError("failed to record the covariate slot index, since %s", tstrerror(code));
            }
          }

          pSupp->numOfInputCols += taosArrayGetSize(pSupp->pCovariateSlotList);
        }
      }
    }
  }

  return code;
}

static void initForecastOpt(SForecastSupp* pSupp) {
  pSupp->maxTs = 0;
  pSupp->minTs = INT64_MAX;
  pSupp->numOfRows = 0;
  pSupp->wncheck = ANALY_DEFAULT_WNCHECK;
  pSupp->forecastRows = ANALY_FORECAST_DEFAULT_ROWS;
  pSupp->conf = ANALY_FORECAST_DEFAULT_CONF;
  pSupp->setEvery = 0;
  pSupp->setStart = 0;
}

static int32_t filterNotSupportForecast(SForecastSupp* pSupp) {
  if (taosArrayGetSize(pSupp->pCovariateSlotList) > 0) {
    if (taosStrcasecmp(pSupp->algoName, "holtwinters") == 0) {
      return TSDB_CODE_ANA_NOT_SUPPORT_FORECAST;
    } else if (taosStrcasecmp(pSupp->algoName, "arima") == 0) {
      return TSDB_CODE_ANA_NOT_SUPPORT_FORECAST;
    } else if (taosStrcasecmp(pSupp->algoName, "timemoe-fc") == 0) {
      return TSDB_CODE_ANA_NOT_SUPPORT_FORECAST;
    }
  }

  return TSDB_CODE_SUCCESS;
}


static int32_t forecastParseOpt(SForecastSupp* pSupp, const char* id) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SHashObj* pHashMap = NULL;

  initForecastOpt(pSupp);

  code = taosAnalyGetOpts(pSupp->pOptions, &pHashMap);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (taosHashGetSize(pHashMap) == 0) {
    code = TSDB_CODE_INVALID_PARA;
    qError("%s no valid options for forecast, failed to exec", id);
    return code;
  }

  if (taosHashGetSize(pHashMap) == 0) {
    code = TSDB_CODE_INVALID_PARA;
    qError("%s no valid options for forecast, failed to exec", id);
    return code;
  }

  code = taosAnalysisParseAlgo(pSupp->pOptions, pSupp->algoName, pSupp->algoUrl, ANALY_ALGO_TYPE_FORECAST,
                               tListLen(pSupp->algoUrl), pHashMap, id);
  TSDB_CHECK_CODE(code, lino, _end);

  code = filterNotSupportForecast(pSupp);
  if (code) {
    qError("%s not support forecast model, %s", id, pSupp->algoName);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  // extract the timeout parameter
  pSupp->timeout = taosAnalysisParseTimout(pHashMap, id);
  pSupp->wncheck = taosAnalysisParseWncheck(pHashMap, id);

  // extract the forecast rows
  char* pRows = taosHashGet(pHashMap, ALGO_OPT_FORECASTROWS_NAME, strlen(ALGO_OPT_FORECASTROWS_NAME));
  if (pRows != NULL) {
    int64_t v = 0;
    code = toInteger(pRows, taosHashGetValueSize(pRows), 10, &v);

    pSupp->forecastRows = v;
    qDebug("%s forecast rows:%"PRId64, id, pSupp->forecastRows);
  } else {
    qDebug("%s forecast rows not found:%s, use default:%" PRId64, id, pSupp->pOptions, pSupp->forecastRows);
  }

  if (pSupp->forecastRows > ANALY_FORECAST_RES_MAX_ROWS) {
    qError("%s required too many forecast rows, max allowed:%d, required:%" PRId64, id, ANALY_FORECAST_RES_MAX_ROWS,
           pSupp->forecastRows);
    code = TSDB_CODE_ANA_ANODE_TOO_MANY_ROWS;
    goto _end;
  }

  if (pSupp->forecastRows <= 0) {
    qError("%s output rows should be greater than 0, input:%" PRId64, id, pSupp->forecastRows);
    code = TSDB_CODE_INVALID_PARA;
    goto _end;
  }

  // extract the confidence interval value
  char* pConf = taosHashGet(pHashMap, ALGO_OPT_CONF_NAME, strlen(ALGO_OPT_CONF_NAME));
  if (pConf != NULL) {
    char*  endPtr = NULL;
    double v = taosStr2Double(pConf, &endPtr);
    pSupp->conf = v;

    if (v <= 0 || v > 1.0) {
      pSupp->conf = ANALY_FORECAST_DEFAULT_CONF;
      qWarn("%s valid conf range is (0, 1], user specified:%.2f out of range, set the default:%.2f", id, v,
             pSupp->conf);
    } else {
      qDebug("%s forecast conf:%.2f", id, pSupp->conf);
    }
  } else {
    qDebug("%s forecast conf not found:%s, use default:%.2f", id, pSupp->pOptions, pSupp->conf);
  }

  // extract the start timestamp
  char* pStart = taosHashGet(pHashMap, ALGO_OPT_START_NAME, strlen(ALGO_OPT_START_NAME));
  if (pStart != NULL) {
    int64_t v = 0;
    code = toInteger(pStart, taosHashGetValueSize(pStart), 10, &v);
    pSupp->startTs = v;
    pSupp->setStart = 1;
    qDebug("%s forecast set start ts:%"PRId64, id, pSupp->startTs);
  }

  // extract the time step
  char* pEvery = taosHashGet(pHashMap, ALGO_OPT_EVERY_NAME, strlen(ALGO_OPT_EVERY_NAME));
  if (pEvery != NULL) {
    int64_t v = 0;
    code = toInteger(pEvery, taosHashGetValueSize(pEvery), 10, &v);
    pSupp->every = v;
    pSupp->setEvery = 1;
    qDebug("%s forecast set every ts:%"PRId64, id, pSupp->every);
  }

  if (pSupp->setEvery && pSupp->every <= 0) {
    qError("%s period should be greater than 0, user specified:%"PRId64, id, pSupp->every);
    code = TSDB_CODE_INVALID_PARA;
    goto _end;
  }

  // extract the dynamic real feature for covariate forecasting
  void*       pIter = NULL;
  size_t      keyLen = 0;
  const char* p = "dynamic_real_";

  while ((pIter = taosHashIterate(pHashMap, pIter))) {
    const char* pVal = pIter;
    char*       pKey = taosHashGetKey((void*)pVal, &keyLen);
    int32_t     idx = 0;
    char        nameBuf[512] = {0};

    if (strncmp(pKey, p, strlen(p)) == 0) {

      if (strncmp(&pKey[keyLen - 4], "_col", 4) == 0) {
        continue;
      }

      int32_t ret = sscanf(pKey, "dynamic_real_%d", &idx);
      if (ret == 0) {
        continue;
      }

      memcpy(nameBuf, pKey, keyLen);
      strncpy(&nameBuf[keyLen], "_col", strlen("_col"));

      void* pCol = taosHashGet(pHashMap, nameBuf, strlen(nameBuf));
      if (pCol == NULL) {
        char* pTmp = taosStrndupi(pKey, keyLen);
        qError("%s dynamic real column related:%s column name:%s not specified", id, pTmp, nameBuf);
        
        taosMemoryFree(pTmp);
        code = TSDB_CODE_INVALID_PARA;
        goto _end;
      } else {
        // build dynamic_real_feature
        SColFutureData d = {.pName = taosStrndupi(pCol, taosHashGetValueSize(pCol))};
        if (d.pName == NULL) {
          qError("%s failed to clone the future dynamic real column name:%s", id, (char*) pCol);
          code = terrno;
          goto _end;
        }

        int32_t index = -1;
        for (int32_t i = 0; i < taosArrayGetSize(pSupp->pCovariateSlotList); ++i) {
          SColumn* pColx = taosArrayGet(pSupp->pCovariateSlotList, i);
          if (strcmp(pColx->name, d.pName) == 0) {
            index = i;
            break;
          }
        }

        if (index == -1) {
          qError("%s not found the required future dynamic real column:%s", id, d.pName);
          code = TSDB_CODE_INVALID_PARA;
          taosMemoryFree(d.pName);
          goto _end;
        }

        SColumn* pColx = taosArrayGet(pSupp->pCovariateSlotList, index);
        d.data.info.slotId = pColx->slotId;
        d.data.info.type = pColx->type;
        d.data.info.bytes = pColx->bytes;

        int32_t len = taosHashGetValueSize((void*)pVal);
        char*   buf = taosStrndupi(pVal, len);
        int32_t unused = strdequote((char*)buf);

        int32_t num = 0;
        char**  pList = strsplit(buf, " ", &num);
        if (num != pSupp->forecastRows) {
          qError("%s the rows:%d of future dynamic real column data is not equalled to the forecasting rows:%" PRId64,
                 id, num, pSupp->forecastRows);
          code = TSDB_CODE_INVALID_PARA;

          taosMemoryFree(d.pName);
          taosMemoryFree(pList);
          taosMemoryFree(buf);
          goto _end;
        }

        d.numOfRows = num;

        code = colInfoDataEnsureCapacity(&d.data, num, true);
        if (code != 0) {
          qError("%s failed to prepare buffer, code:%s", id, tstrerror(code));
          goto _end;
        }

        for (int32_t j = 0; j < num; ++j) {
          char* ps = NULL;
          if (j == 0) {
            ps = strstr(pList[j], "[") + 1;
          } else {
            ps = pList[j];
          }

          code = 0;

          switch(pColx->type) {
            case TSDB_DATA_TYPE_TINYINT: {
              int8_t t1 = taosStr2Int8(ps, NULL, 10);
              code = colDataSetVal(&d.data, j, (const char*)&t1, false);
              break;
            }
            case TSDB_DATA_TYPE_SMALLINT: {
              int16_t t1 = taosStr2Int16(ps, NULL, 10);
              code = colDataSetVal(&d.data, j, (const char*)&t1, false);
              break;
            }
            case TSDB_DATA_TYPE_INT: {
              int32_t t1 = taosStr2Int32(ps, NULL, 10);
              code = colDataSetVal(&d.data, j, (const char*)&t1, false);
              break;
            }
            case TSDB_DATA_TYPE_BIGINT: {
              int64_t t1 = taosStr2Int64(ps, NULL, 10);
              code = colDataSetVal(&d.data, j, (const char*)&t1, false);
              break;
            }
            case TSDB_DATA_TYPE_FLOAT: {
              float t1 = taosStr2Float(ps, NULL);
              code = colDataSetVal(&d.data, j, (const char*)&t1, false);
              break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
              double t1 = taosStr2Double(ps, NULL);
              code = colDataSetVal(&d.data, j, (const char*)&t1, false);
              break;
            }
            case TSDB_DATA_TYPE_UTINYINT: {
              uint8_t t1 = taosStr2UInt8(ps, NULL, 10);
              code = colDataSetVal(&d.data, j, (const char*)&t1, false);
              break;
            }
            case TSDB_DATA_TYPE_USMALLINT: {
              uint16_t t1 = taosStr2UInt16(ps, NULL, 10);
              code = colDataSetVal(&d.data, j, (const char*)&t1, false);
              break;
            }
            case TSDB_DATA_TYPE_UINT: {
              uint32_t t1 = taosStr2UInt32(ps, NULL, 10);
              code = colDataSetVal(&d.data, j, (const char*)&t1, false);
              break;
            }
            case TSDB_DATA_TYPE_UBIGINT: {
              uint64_t t1 = taosStr2UInt64(ps, NULL, 10);
              code = colDataSetVal(&d.data, j, (const char*)&t1, false);
              break;
            }
          }

          if (code != 0) {
            break;
          }
        }

        taosMemoryFree(pList);
        taosMemoryFree(buf);
        if (code != 0) {
          goto _end;
        }

        void* noret = taosArrayPush(pSupp->pDynamicRealList, &d);
        if (noret == NULL) {
          qError("%s failed to add column info in dynamic real column info", id);
          code = terrno;
          goto _end;
        }
      }
    }
  }

_end:
  taosHashCleanup(pHashMap);
  return code;
}

static int32_t forecastCreateBuf(SForecastSupp* pSupp, const char* pId) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int64_t       ts = taosGetTimestampNs();
  int32_t       index = 0;

  pBuf->bufType = ANALYTICS_BUF_TYPE_JSON_COL;
  snprintf(pBuf->fileName, sizeof(pBuf->fileName), "%s/tdengine-forecast-%p-%" PRId64, tsTempDir, pSupp, ts);

  int32_t numOfCols = taosArrayGetSize(pSupp->pCovariateSlotList) + 2;

  int32_t code = tsosAnalyBufOpen(pBuf, numOfCols, pId);
  if (code != 0) goto _OVER;

  code = taosAnalyBufWriteColMeta(pBuf, index++, TSDB_DATA_TYPE_TIMESTAMP, "ts");
  if (code != 0) goto _OVER;

  code = taosAnalyBufWriteColMeta(pBuf, index++, pSupp->targetValType, "val");
  if (code != 0) goto _OVER;

  int32_t numOfDynamicReal = taosArrayGetSize(pSupp->pDynamicRealList);
  int32_t numOfPastDynamicReal = taosArrayGetSize(pSupp->pCovariateSlotList);

  if (numOfPastDynamicReal >= numOfDynamicReal) {
    for(int32_t i = 0; i < numOfDynamicReal; ++i) {
      SColFutureData* pData = taosArrayGet(pSupp->pDynamicRealList, i);

      for(int32_t k = 0; k < taosArrayGetSize(pSupp->pCovariateSlotList); ++k) {
        SColumn* pCol = taosArrayGet(pSupp->pCovariateSlotList, k);
        if (strcmp(pCol->name, pData->pName) == 0) {
          char name[128] = {0};
          (void) tsnprintf(name, tListLen(name), "dynamic_real_%d", i + 1);
          code = taosAnalyBufWriteColMeta(pBuf, index++, pCol->type, name);
          if (code != 0) {
            goto _OVER;
          }

          memcpy(&pData->col, pCol, sizeof(SColumn));
          taosArrayRemove(pSupp->pCovariateSlotList, k);
          break;
        }
      }
    }

    numOfPastDynamicReal = taosArrayGetSize(pSupp->pCovariateSlotList);
    for (int32_t j = 0; j < numOfPastDynamicReal; ++j) {
      SColumn* pCol = taosArrayGet(pSupp->pCovariateSlotList, j);

      char name[128] = {0};
      (void)tsnprintf(name, tListLen(name), "past_dynamic_real_%d", j + 1);

      code = taosAnalyBufWriteColMeta(pBuf, index++, pCol->type, name);
      if (code) {
        goto _OVER;
      }
    }
  }

  code = taosAnalyBufWriteDataBegin(pBuf);
  if (code != 0) goto _OVER;

  for (int32_t i = 0; i < pSupp->numOfInputCols; ++i) {
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

static int32_t resetForecastOperState(SOperatorInfo* pOper) {
  int32_t code = 0, lino = 0;
  SForecastOperatorInfo* pInfo = pOper->info;
  const char*            pId = pOper->pTaskInfo->id.str;
  SForecastFuncPhysiNode* pForecastPhyNode = (SForecastFuncPhysiNode*)pOper->pPhyNode;
  SExecTaskInfo* pTaskInfo = pOper->pTaskInfo;

  pOper->status = OP_NOT_OPENED;

  blockDataCleanup(pInfo->pRes);

  taosArrayDestroy(pInfo->forecastSupp.pCovariateSlotList);
  pInfo->forecastSupp.pCovariateSlotList = NULL;

  taosAnalyBufDestroy(&pInfo->forecastSupp.analyBuf);

  cleanupExprSupp(&pOper->exprSupp);
  cleanupExprSupp(&pInfo->scalarSup);

  int32_t                 numOfExprs = 0;
  SExprInfo*              pExprInfo = NULL;

  TAOS_CHECK_EXIT(createExprInfo(pForecastPhyNode->pFuncs, NULL, &pExprInfo, &numOfExprs));

  TAOS_CHECK_EXIT(initExprSupp(&pOper->exprSupp, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore));

  TAOS_CHECK_EXIT(filterInitFromNode((SNode*)pForecastPhyNode->node.pConditions, &pOper->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo));

  TAOS_CHECK_EXIT(forecastParseInput(&pInfo->forecastSupp, pForecastPhyNode->pFuncs, pId));

  TAOS_CHECK_EXIT(forecastParseOutput(&pInfo->forecastSupp, &pOper->exprSupp));

  TAOS_CHECK_EXIT(forecastParseOpt(&pInfo->forecastSupp, pId));
  TAOS_CHECK_EXIT(forecastCreateBuf(&pInfo->forecastSupp, pId));

  if (pForecastPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pScalarExprInfo = NULL;
    TAOS_CHECK_EXIT(createExprInfo(pForecastPhyNode->pExprs, NULL, &pScalarExprInfo, &num));
    TAOS_CHECK_EXIT(initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num, &pTaskInfo->storageAPI.functionStore));
  }

  initResultSizeInfo(&pOper->resultInfo, 4096);

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;  
}




int32_t createForecastOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                   SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                code = 0;
  int32_t                lino = 0;
  SForecastOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SForecastOperatorInfo));
  SOperatorInfo*         pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pPhyNode;

  const char*             pId = pTaskInfo->id.str;
  SForecastSupp*          pSupp = &pInfo->forecastSupp;
  SForecastFuncPhysiNode* pForecastPhyNode = (SForecastFuncPhysiNode*)pPhyNode;
  SExprSupp*              pExprSup = &pOperator->exprSupp;
  int32_t                 numOfExprs = 0;
  SExprInfo*              pExprInfo = NULL;

  code = createExprInfo(pForecastPhyNode->pFuncs, NULL, &pExprInfo, &numOfExprs);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExprSupp(pExprSup, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pForecastPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pForecastPhyNode->pExprs, NULL, &pScalarExprInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pForecastPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  code = forecastParseInput(pSupp, pForecastPhyNode->pFuncs, pId);
  QUERY_CHECK_CODE(code, lino, _error);

  code = forecastParseOutput(pSupp, pExprSup);
  QUERY_CHECK_CODE(code, lino, _error);

  code = forecastParseOpt(pSupp, pId);
  QUERY_CHECK_CODE(code, lino, _error);

  code = forecastCreateBuf(pSupp, pId);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->pRes = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  setOperatorInfo(pOperator, "ForecastOperator", QUERY_NODE_PHYSICAL_PLAN_FORECAST_FUNC, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, forecastNext, NULL, destroyForecastInfo, optrDefaultBufFn,
                                         NULL, optrDefaultGetNextExtFn, NULL);

  setOperatorResetStateFn(pOperator, resetForecastOperState);

  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;

  qDebug("%s forecast env is initialized, option:%s", pId, pSupp->pOptions);
  return TSDB_CODE_SUCCESS;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", pId, __func__, lino, tstrerror(code));
  }
  if (pInfo != NULL) destroyForecastInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

static void destroyColFutureData(void* p) {
  SColFutureData* pData = p;
  taosMemoryFree(pData->pName);
  colDataDestroy(&pData->data);
}

static void destroyForecastInfo(void* param) {
  SForecastOperatorInfo* pInfo = (SForecastOperatorInfo*)param;

  blockDataDestroy(pInfo->pRes);
  pInfo->pRes = NULL;

  taosArrayDestroy(pInfo->forecastSupp.pCovariateSlotList);
  pInfo->forecastSupp.pCovariateSlotList = NULL;

  taosArrayDestroyEx(pInfo->forecastSupp.pDynamicRealList, destroyColFutureData);
  pInfo->forecastSupp.pDynamicRealList = NULL;

  taosMemoryFree(pInfo->forecastSupp.pOptions);

  cleanupExprSupp(&pInfo->scalarSup);
  taosAnalyBufDestroy(&pInfo->forecastSupp.analyBuf);
  taosMemoryFreeClear(param);
}

#else

int32_t createForecastOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                   SOperatorInfo** pOptrInfo) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

#endif

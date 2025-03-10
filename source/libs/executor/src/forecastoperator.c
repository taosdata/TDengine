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
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "ttime.h"

#ifdef USE_ANALYTICS

typedef struct {
  char         algoName[TSDB_ANALYTIC_ALGO_NAME_LEN];
  char         algoUrl[TSDB_ANALYTIC_ALGO_URL_LEN];
  char         algoOpt[TSDB_ANALYTIC_ALGO_OPTION_LEN];
  int64_t      maxTs;
  int64_t      minTs;
  int64_t      numOfRows;
  uint64_t     groupId;
  int64_t      optRows;
  int64_t      cachedRows;
  int32_t      numOfBlocks;
  int16_t      resTsSlot;
  int16_t      resValSlot;
  int16_t      resLowSlot;
  int16_t      resHighSlot;
  int16_t      inputTsSlot;
  int16_t      inputValSlot;
  int8_t       inputValType;
  int8_t       inputPrecision;
  SAnalyticBuf analyBuf;
} SForecastSupp;

typedef struct SForecastOperatorInfo {
  SSDataBlock*  pRes;
  SExprSupp     scalarSup;  // scalar calculation
  SForecastSupp forecastSupp;
} SForecastOperatorInfo;

static void destroyForecastInfo(void* param);

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

  if (pSupp->cachedRows > ANALY_FORECAST_MAX_HISTORY_ROWS) {
    code = TSDB_CODE_ANA_ANODE_TOO_MANY_ROWS;
    qError("%s rows:%" PRId64 " for forecast cache, error happens, code:%s, upper limit:%d", id, pSupp->cachedRows,
           tstrerror(code), ANALY_FORECAST_MAX_HISTORY_ROWS);
    return code;
  }

  pSupp->numOfBlocks++;
  qDebug("%s block:%d, %p rows:%" PRId64, id, pSupp->numOfBlocks, pBlock, pBlock->info.rows);

  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    SColumnInfoData* pValCol = taosArrayGet(pBlock->pDataBlock, pSupp->inputValSlot);
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSupp->inputTsSlot);
    if (pTsCol == NULL || pValCol == NULL) break;

    int64_t ts = ((TSKEY*)pTsCol->pData)[j];
    char*   val = colDataGetData(pValCol, j);
    int16_t valType = pValCol->info.type;

    pSupp->minTs = MIN(pSupp->minTs, ts);
    pSupp->maxTs = MAX(pSupp->maxTs, ts);
    pSupp->numOfRows++;

    code = taosAnalyBufWriteColData(pBuf, 0, TSDB_DATA_TYPE_TIMESTAMP, &ts);
    if (TSDB_CODE_SUCCESS != code) {
      qError("%s failed to write ts in buf, code:%s", id, tstrerror(code));
      return code;
    }

    code = taosAnalyBufWriteColData(pBuf, 1, valType, val);
    if (TSDB_CODE_SUCCESS != code) {
      qError("%s failed to write val in buf, code:%s", id, tstrerror(code));
      return code;
    }
  }

  return 0;
}

static int32_t forecastCloseBuf(SForecastSupp* pSupp, const char* id) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int32_t       code = 0;

  for (int32_t i = 0; i < 2; ++i) {
    code = taosAnalyBufWriteColEnd(pBuf, i);
    if (code != 0) return code;
  }

  code = taosAnalyBufWriteDataEnd(pBuf);
  if (code != 0) return code;

  code = taosAnalyBufWriteOptStr(pBuf, "option", pSupp->algoOpt);
  if (code != 0) return code;

  code = taosAnalyBufWriteOptStr(pBuf, "algo", pSupp->algoName);
  if (code != 0) return code;

  const char* prec = TSDB_TIME_PRECISION_MILLI_STR;
  if (pSupp->inputPrecision == TSDB_TIME_PRECISION_MICRO) prec = TSDB_TIME_PRECISION_MICRO_STR;
  if (pSupp->inputPrecision == TSDB_TIME_PRECISION_NANO) prec = TSDB_TIME_PRECISION_NANO_STR;
  code = taosAnalyBufWriteOptStr(pBuf, "prec", prec);
  if (code != 0) return code;

  int64_t wncheck = ANALY_FORECAST_DEFAULT_WNCHECK;
  bool    hasWncheck = taosAnalyGetOptInt(pSupp->algoOpt, "wncheck", &wncheck);
  if (!hasWncheck) {
    qDebug("%s forecast wncheck not found from %s, use default:%" PRId64, id, pSupp->algoOpt, wncheck);
  }
  code = taosAnalyBufWriteOptInt(pBuf, "wncheck", wncheck);
  if (code != 0) return code;

  bool noConf = (pSupp->resHighSlot == -1 && pSupp->resLowSlot == -1);
  code = taosAnalyBufWriteOptInt(pBuf, "return_conf", !noConf);
  if (code != 0) return code;

  pSupp->optRows = ANALY_FORECAST_DEFAULT_ROWS;
  bool hasRows = taosAnalyGetOptInt(pSupp->algoOpt, "rows", &pSupp->optRows);
  if (!hasRows) {
    qDebug("%s forecast rows not found from %s, use default:%" PRId64, id, pSupp->algoOpt, pSupp->optRows);
  }

  if (pSupp->optRows > ANALY_MAX_FC_ROWS) {
    qError("%s required too many forecast rows, max allowed:%d, required:%" PRId64, id, ANALY_MAX_FC_ROWS,
           pSupp->optRows);
    return TSDB_CODE_ANA_ANODE_TOO_MANY_ROWS;
  }

  code = taosAnalyBufWriteOptInt(pBuf, "forecast_rows", pSupp->optRows);
  if (code != 0) return code;

  int64_t conf = ANALY_FORECAST_DEFAULT_CONF;
  bool    hasConf = taosAnalyGetOptInt(pSupp->algoOpt, "conf", &conf);
  if (!hasConf) {
    qDebug("%s forecast conf not found from %s, use default:%" PRId64, id, pSupp->algoOpt, conf);
  }
  code = taosAnalyBufWriteOptInt(pBuf, "conf", conf);
  if (code != 0) return code;

  int32_t len = strlen(pSupp->algoOpt);
  int64_t every = (pSupp->maxTs - pSupp->minTs) / (pSupp->numOfRows - 1);
  int64_t start = pSupp->maxTs + every;
  bool    hasStart = taosAnalyGetOptInt(pSupp->algoOpt, "start", &start);
  if (!hasStart) {
    qDebug("%s forecast start not found from %s, use %" PRId64, id, pSupp->algoOpt, start);
  }
  code = taosAnalyBufWriteOptInt(pBuf, "start", start);
  if (code != 0) return code;

  bool hasEvery = taosAnalyGetOptInt(pSupp->algoOpt, "every", &every);
  if (!hasEvery) {
    qDebug("%s forecast every not found from %s, use %" PRId64, id, pSupp->algoOpt, every);
  }
  code = taosAnalyBufWriteOptInt(pBuf, "every", every);
  if (code != 0) return code;

  code = taosAnalyBufClose(pBuf);
  return code;
}

static int32_t forecastAnalysis(SForecastSupp* pSupp, SSDataBlock* pBlock, const char* pId) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int32_t       resCurRow = pBlock->info.rows;
  int8_t        tmpI8;
  int16_t       tmpI16;
  int32_t       tmpI32;
  int64_t       tmpI64;
  float         tmpFloat;
  double        tmpDouble;
  int32_t       code = 0;

  SColumnInfoData* pResValCol = taosArrayGet(pBlock->pDataBlock, pSupp->resValSlot);
  if (NULL == pResValCol) {
    return terrno;
  }

  SColumnInfoData* pResTsCol = (pSupp->resTsSlot != -1 ? taosArrayGet(pBlock->pDataBlock, pSupp->resTsSlot) : NULL);
  SColumnInfoData* pResLowCol = (pSupp->resLowSlot != -1 ? taosArrayGet(pBlock->pDataBlock, pSupp->resLowSlot) : NULL);
  SColumnInfoData* pResHighCol =
      (pSupp->resHighSlot != -1 ? taosArrayGet(pBlock->pDataBlock, pSupp->resHighSlot) : NULL);

  SJson* pJson = taosAnalySendReqRetJson(pSupp->algoUrl, ANALYTICS_HTTP_TYPE_POST, pBuf);
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
    return TSDB_CODE_ANA_WN_DATA;
  }

  if (code < 0) {
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
      tmpFloat = (float)tmpDouble;
      colDataSetFloat(pResLowCol, resCurRow, &tmpFloat);
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
      tmpFloat = (float)tmpDouble;
      colDataSetFloat(pResHighCol, resCurRow, &tmpFloat);
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

    switch (pSupp->inputValType) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_TINYINT: {
        tmpI8 = (int8_t)tmpDouble;
        colDataSetInt8(pResValCol, resCurRow, &tmpI8);
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT:
      case TSDB_DATA_TYPE_SMALLINT: {
        tmpI16 = (int16_t)tmpDouble;
        colDataSetInt16(pResValCol, resCurRow, &tmpI16);
        break;
      }
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_UINT: {
        tmpI32 = (int32_t)tmpDouble;
        colDataSetInt32(pResValCol, resCurRow, &tmpI32);
        break;
      }
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_BIGINT: {
        tmpI64 = (int64_t)tmpDouble;
        colDataSetInt64(pResValCol, resCurRow, &tmpI64);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        tmpFloat = (float)tmpDouble;
        colDataSetFloat(pResValCol, resCurRow, &tmpFloat);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        colDataSetDouble(pResValCol, resCurRow, &tmpDouble);
        break;
      }
      default:
        code = TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
        goto _OVER;
    }
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

static int32_t forecastParseInput(SForecastSupp* pSupp, SNodeList* pFuncs) {
  SNode* pNode = NULL;

  pSupp->inputTsSlot = -1;
  pSupp->inputValSlot = -1;
  pSupp->inputValType = -1;
  pSupp->inputPrecision = -1;

  FOREACH(pNode, pFuncs) {
    if ((nodeType(pNode) == QUERY_NODE_TARGET) && (nodeType(((STargetNode*)pNode)->pExpr) == QUERY_NODE_FUNCTION)) {
      SFunctionNode* pFunc = (SFunctionNode*)((STargetNode*)pNode)->pExpr;
      int32_t        numOfParam = LIST_LENGTH(pFunc->pParameterList);

      if (pFunc->funcType == FUNCTION_TYPE_FORECAST) {
        if (numOfParam == 3) {
          SNode* p1 = nodesListGetNode(pFunc->pParameterList, 0);
          SNode* p2 = nodesListGetNode(pFunc->pParameterList, 1);
          SNode* p3 = nodesListGetNode(pFunc->pParameterList, 2);
          if (p1 == NULL || p2 == NULL || p3 == NULL) return TSDB_CODE_PLAN_INTERNAL_ERROR;
          if (p1->type != QUERY_NODE_COLUMN) return TSDB_CODE_PLAN_INTERNAL_ERROR;
          if (p2->type != QUERY_NODE_VALUE) return TSDB_CODE_PLAN_INTERNAL_ERROR;
          if (p3->type != QUERY_NODE_COLUMN) return TSDB_CODE_PLAN_INTERNAL_ERROR;
          SColumnNode* pValNode = (SColumnNode*)p1;
          SValueNode*  pOptNode = (SValueNode*)p2;
          SColumnNode* pTsNode = (SColumnNode*)p3;
          pSupp->inputTsSlot = pTsNode->slotId;
          pSupp->inputPrecision = pTsNode->node.resType.precision;
          pSupp->inputValSlot = pValNode->slotId;
          pSupp->inputValType = pValNode->node.resType.type;
          tstrncpy(pSupp->algoOpt, pOptNode->literal, sizeof(pSupp->algoOpt));
        } else if (numOfParam == 2) {
          SNode* p1 = nodesListGetNode(pFunc->pParameterList, 0);
          SNode* p2 = nodesListGetNode(pFunc->pParameterList, 1);
          if (p1 == NULL || p2 == NULL) return TSDB_CODE_PLAN_INTERNAL_ERROR;
          if (p1->type != QUERY_NODE_COLUMN) return TSDB_CODE_PLAN_INTERNAL_ERROR;
          if (p2->type != QUERY_NODE_COLUMN) return TSDB_CODE_PLAN_INTERNAL_ERROR;
          SColumnNode* pValNode = (SColumnNode*)p1;
          SColumnNode* pTsNode = (SColumnNode*)p2;
          pSupp->inputTsSlot = pTsNode->slotId;
          pSupp->inputPrecision = pTsNode->node.resType.precision;
          pSupp->inputValSlot = pValNode->slotId;
          pSupp->inputValType = pValNode->node.resType.type;
          tstrncpy(pSupp->algoOpt, "algo=arima", TSDB_ANALYTIC_ALGO_OPTION_LEN);
        } else {
          return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
      }
    }
  }

  return 0;
}

static int32_t forecastParseAlgo(SForecastSupp* pSupp) {
  pSupp->maxTs = 0;
  pSupp->minTs = INT64_MAX;
  pSupp->numOfRows = 0;

  if (!taosAnalyGetOptStr(pSupp->algoOpt, "algo", pSupp->algoName, sizeof(pSupp->algoName))) {
    qError("failed to get forecast algorithm name from %s", pSupp->algoOpt);
    return TSDB_CODE_ANA_ALGO_NOT_FOUND;
  }

  if (taosAnalyGetAlgoUrl(pSupp->algoName, ANALY_ALGO_TYPE_FORECAST, pSupp->algoUrl, sizeof(pSupp->algoUrl)) != 0) {
    qError("failed to get forecast algorithm url from %s", pSupp->algoName);
    return TSDB_CODE_ANA_ALGO_NOT_LOAD;
  }

  return 0;
}

static int32_t forecastCreateBuf(SForecastSupp* pSupp) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int64_t       ts = 0;  // taosGetTimestampMs();

  pBuf->bufType = ANALYTICS_BUF_TYPE_JSON_COL;
  snprintf(pBuf->fileName, sizeof(pBuf->fileName), "%s/tdengine-forecast-%" PRId64, tsTempDir, ts);
  int32_t code = tsosAnalyBufOpen(pBuf, 2);
  if (code != 0) goto _OVER;

  code = taosAnalyBufWriteColMeta(pBuf, 0, TSDB_DATA_TYPE_TIMESTAMP, "ts");
  if (code != 0) goto _OVER;

  code = taosAnalyBufWriteColMeta(pBuf, 1, pSupp->inputValType, "val");
  if (code != 0) goto _OVER;

  code = taosAnalyBufWriteDataBegin(pBuf);
  if (code != 0) goto _OVER;

  for (int32_t i = 0; i < 2; ++i) {
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

  code = filterInitFromNode((SNode*)pForecastPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  code = forecastParseInput(pSupp, pForecastPhyNode->pFuncs);
  QUERY_CHECK_CODE(code, lino, _error);

  code = forecastParseOutput(pSupp, pExprSup);
  QUERY_CHECK_CODE(code, lino, _error);

  code = forecastParseAlgo(pSupp);
  QUERY_CHECK_CODE(code, lino, _error);

  code = forecastCreateBuf(pSupp);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->pRes = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  setOperatorInfo(pOperator, "ForecastOperator", QUERY_NODE_PHYSICAL_PLAN_FORECAST_FUNC, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, forecastNext, NULL, destroyForecastInfo, optrDefaultBufFn,
                                         NULL, optrDefaultGetNextExtFn, NULL);

  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;

  qDebug("forecast env is initialized, option:%s", pSupp->algoOpt);
  return TSDB_CODE_SUCCESS;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pInfo != NULL) destroyForecastInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

static void destroyForecastInfo(void* param) {
  SForecastOperatorInfo* pInfo = (SForecastOperatorInfo*)param;

  blockDataDestroy(pInfo->pRes);
  pInfo->pRes = NULL;
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

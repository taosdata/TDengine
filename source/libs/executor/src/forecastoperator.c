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
#include "functionMgt.h"
#include "operator.h"
#include "querytask.h"
#include "storageapi.h"
#include "tanal.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "ttime.h"

#ifdef USE_ANAL

typedef struct {
  char     algoName[TSDB_ANAL_ALGO_NAME_LEN];
  char     algoUrl[TSDB_ANAL_ALGO_URL_LEN];
  char     algoOpt[TSDB_ANAL_ALGO_OPTION_LEN];
  int64_t  maxTs;
  int64_t  minTs;
  int64_t  numOfRows;
  uint64_t groupId;
  int32_t  numOfBlocks;
  int32_t  optRows;
  int16_t  resTsSlot;
  int16_t  resValSlot;
  int16_t  resLowSlot;
  int16_t  resHighSlot;
  int16_t  inputTsSlot;
  int16_t  inputValSlot;
  int8_t   inputValType;
  int8_t   inputPrecision;
  SAnalBuf analBuf;
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

static int32_t forecastCacheBlock(SForecastSupp* pSupp, SSDataBlock* pBlock) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SAnalBuf* pBuf = &pSupp->analBuf;

  qInfo("block:%d, %p rows:%" PRId64, pSupp->numOfBlocks, pBlock, pBlock->info.rows);
  pSupp->numOfBlocks++;

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

    code = taosAnalBufWriteColData(pBuf, 0, TSDB_DATA_TYPE_TIMESTAMP, &ts);
    if (TSDB_CODE_SUCCESS != code) return code;

    code = taosAnalBufWriteColData(pBuf, 1, valType, val);
    if (TSDB_CODE_SUCCESS != code) return code;
  }

  return 0;
}

static int32_t forecastCloseBuf(SForecastSupp* pSupp) {
  SAnalBuf* pBuf = &pSupp->analBuf;
  int32_t   code = 0;

  for (int32_t i = 0; i < 2; ++i) {
    code = taosAnalBufWriteColEnd(pBuf, i);
    if (code != 0) return code;
  }

  code = taosAnalBufWriteDataEnd(pBuf);
  if (code != 0) return code;

  int32_t len = strlen(pSupp->algoOpt);
  int64_t every = (pSupp->maxTs - pSupp->minTs) / (pSupp->numOfRows + 1);
  int64_t start = pSupp->maxTs + every;
  bool    hasStart = taosAnalGetOptStr(pSupp->algoOpt, "start", NULL, 0);
  if (!hasStart) {
    uInfo("forecast start not found from %s, use %" PRId64, pSupp->algoOpt, start);
    code = taosAnalBufWriteOptInt(pBuf, "start", start);
    if (code != 0) return code;
  }

  bool hasEvery = taosAnalGetOptStr(pSupp->algoOpt, "every", NULL, 0);
  if (!hasEvery) {
    uInfo("forecast every not found from %s, use %" PRId64, pSupp->algoOpt, every);
    code = taosAnalBufWriteOptInt(pBuf, "every", every);
    if (code != 0) return code;
  }

  code = taosAnalBufWriteOptStr(pBuf, "option", pSupp->algoOpt);
  if (code != 0) return code;
  
  code = taosAnalBufClose(pBuf);
  return code;
}

static int32_t forecastAnalysis(SForecastSupp* pSupp, SSDataBlock* pBlock) {
  SAnalBuf* pBuf = &pSupp->analBuf;
  int32_t   resCurRow = pBlock->info.rows;
  int8_t    tmpI8;
  int16_t   tmpI16;
  int32_t   tmpI32;
  int64_t   tmpI64;
  float     tmpFloat;
  double    tmpDouble;
  int32_t   code = 0;

  SColumnInfoData* pResValCol = taosArrayGet(pBlock->pDataBlock, pSupp->resValSlot);
  if (NULL == pResValCol) return TSDB_CODE_OUT_OF_RANGE;

  SColumnInfoData* pResTsCol = (pSupp->resTsSlot != -1 ? taosArrayGet(pBlock->pDataBlock, pSupp->resTsSlot) : NULL);
  SColumnInfoData* pResLowCol = (pSupp->resLowSlot != -1 ? taosArrayGet(pBlock->pDataBlock, pSupp->resLowSlot) : NULL);
  SColumnInfoData* pResHighCol =
      (pSupp->resHighSlot != -1 ? taosArrayGet(pBlock->pDataBlock, pSupp->resHighSlot) : NULL);

  SJson* pJson = taosAnalSendReqRetJson(pSupp->algoUrl, ANAL_HTTP_TYPE_POST, pBuf);
  if (pJson == NULL) return terrno;

  int32_t rows = 0;
  tjsonGetInt32ValueFromDouble(pJson, "rows", rows, code);
  if (code < 0) goto _OVER;
  if (rows <= 0) goto _OVER;

  SJson* res = tjsonGetObjectItem(pJson, "res");
  if (res == NULL) goto _OVER;
  int32_t ressize = tjsonGetArraySize(res);
  bool    returnConf = (pSupp->resHighSlot != -1 || pSupp->resLowSlot != -1);
  if (returnConf) {
    if (ressize != 4) goto _OVER;
  } else if (ressize != 2) {
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

  // for (int32_t i = rows; i < pSupp->optRows; ++i) {
  //   colDataSetNNULL(pResValCol, rows, (pSupp->optRows - rows));
  //   if (pResTsCol != NULL) {
  //     colDataSetNNULL(pResTsCol, rows, (pSupp->optRows - rows));
  //   }
  //   if (pResLowCol != NULL) {
  //     colDataSetNNULL(pResLowCol, rows, (pSupp->optRows - rows));
  //   }
  //   if (pResHighCol != NULL) {
  //     colDataSetNNULL(pResHighCol, rows, (pSupp->optRows - rows));
  //   }
  // }

  // if (rows == pSupp->optRows) {
  //   pResValCol->hasNull = false;
  // }

  pBlock->info.rows += rows;

  if (pJson != NULL) tjsonDelete(pJson);
  return 0;

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (code == 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
  }
  qError("failed to perform forecast finalize since %s", tstrerror(code));
  return TSDB_CODE_INVALID_JSON_FORMAT;
}

static int32_t forecastAggregateBlocks(SForecastSupp* pSupp, SSDataBlock* pResBlock) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SAnalBuf* pBuf = &pSupp->analBuf;

  code = forecastCloseBuf(pSupp);
  QUERY_CHECK_CODE(code, lino, _end);

  code = forecastEnsureBlockCapacity(pResBlock, 1);
  QUERY_CHECK_CODE(code, lino, _end);

  code = forecastAnalysis(pSupp, pResBlock);
  QUERY_CHECK_CODE(code, lino, _end);

  uInfo("block:%d, forecast finalize", pSupp->numOfBlocks);

_end:
  pSupp->numOfBlocks = 0;
  taosAnalBufDestroy(&pSupp->analBuf);
  return code;
}

static int32_t forecastNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  SExecTaskInfo*         pTaskInfo = pOperator->pTaskInfo;
  SForecastOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*           pResBlock = pInfo->pRes;
  SForecastSupp*         pSupp = &pInfo->forecastSupp;
  SAnalBuf*              pBuf = &pSupp->analBuf;
  int64_t                st = taosGetTimestampUs();
  int32_t                numOfBlocks = pSupp->numOfBlocks;

  blockDataCleanup(pResBlock);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    if (pSupp->groupId == 0 || pSupp->groupId == pBlock->info.id.groupId) {
      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks++;
      qDebug("group:%" PRId64 ", blocks:%d, cache block rows:%" PRId64, pSupp->groupId, numOfBlocks, pBlock->info.rows);
      code = forecastCacheBlock(pSupp, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      qInfo("group:%" PRId64 ", read finish for new group coming, blocks:%d", pSupp->groupId, numOfBlocks);
      forecastAggregateBlocks(pSupp, pResBlock);
      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks = 1;
      qDebug("group:%" PRId64 ", new group, cache block rows:%" PRId64, pSupp->groupId, pBlock->info.rows);
      code = forecastCacheBlock(pSupp, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pResBlock->info.rows > 0) {
      (*ppRes) = pResBlock;
      qInfo("group:%" PRId64 ", return to upstream, blocks:%d", pResBlock->info.id.groupId, numOfBlocks);
      return code;
    }
  }

  if (numOfBlocks > 0) {
    qInfo("group:%" PRId64 ", read finish, blocks:%d", pSupp->groupId, numOfBlocks);
    forecastAggregateBlocks(pSupp, pResBlock);
  }

  int64_t cost = taosGetTimestampUs() - st;
  qInfo("all groups finished, cost:%" PRId64 "us", cost);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
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
          tstrncpy(pSupp->algoOpt, "algo=arima", TSDB_ANAL_ALGO_OPTION_LEN);
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

  if (!taosAnalGetOptStr(pSupp->algoOpt, "algo", pSupp->algoName, sizeof(pSupp->algoName))) {
    qError("failed to get forecast algorithm name from %s", pSupp->algoOpt);
    return TSDB_CODE_ANAL_ALGO_NOT_FOUND;
  }

  if (taosAnalGetAlgoUrl(pSupp->algoName, ANAL_ALGO_TYPE_FORECAST, pSupp->algoUrl, sizeof(pSupp->algoUrl)) != 0) {
    qError("failed to get forecast algorithm url from %s", pSupp->algoName);
    return TSDB_CODE_ANAL_ALGO_NOT_LOAD;
  }

  return 0;
}

static int32_t forecastCreateBuf(SForecastSupp* pSupp) {
  SAnalBuf* pBuf = &pSupp->analBuf;
  int64_t   ts = 0;  // taosGetTimestampMs();

  pBuf->bufType = ANAL_BUF_TYPE_JSON_COL;
  snprintf(pBuf->fileName, sizeof(pBuf->fileName), "%s/tdengine-forecast-%" PRId64, tsTempDir, ts);
  int32_t code = tsosAnalBufOpen(pBuf, 2);
  if (code != 0) goto _OVER;

  code = taosAnalBufWriteOptStr(pBuf, "algo", pSupp->algoName);
  if (code != 0) goto _OVER;

  bool returnConf = (pSupp->resHighSlot == -1 || pSupp->resLowSlot == -1);
  code = taosAnalBufWriteOptStr(pBuf, "return_conf", returnConf ? "true" : "false");
  if (code != 0) goto _OVER;

  bool hasAlpha = taosAnalGetOptStr(pSupp->algoOpt, "alpha", NULL, 0);
  if (!hasAlpha) {
    qInfo("forecast alpha not found from %s, use default:%f", pSupp->algoOpt, ANAL_FORECAST_DEFAULT_ALPHA);
    code = taosAnalBufWriteOptFloat(pBuf, "alpha", ANAL_FORECAST_DEFAULT_ALPHA);
    if (code != 0) goto _OVER;
  }

  char tmpOpt[32] = {0};
  bool hasParam = taosAnalGetOptStr(pSupp->algoOpt, "param", tmpOpt, sizeof(tmpOpt));
  if (!hasParam) {
    qInfo("forecast param not found from %s, use default:%s", pSupp->algoOpt, ANAL_FORECAST_DEFAULT_PARAM);
    code = taosAnalBufWriteOptStr(pBuf, "param", ANAL_FORECAST_DEFAULT_PARAM);
    if (code != 0) goto _OVER;
  }

  bool hasPeriod = taosAnalGetOptInt(pSupp->algoOpt, "period", NULL);
  if (!hasPeriod) {
    qInfo("forecast period not found from %s, use default:%d", pSupp->algoOpt, ANAL_FORECAST_DEFAULT_PERIOD);
    code = taosAnalBufWriteOptInt(pBuf, "period", ANAL_FORECAST_DEFAULT_PERIOD);
    if (code != 0) goto _OVER;
  }

  bool hasRows = taosAnalGetOptInt(pSupp->algoOpt, "rows", &pSupp->optRows);
  if (!hasRows) {
    pSupp->optRows = ANAL_FORECAST_DEFAULT_ROWS;
    qInfo("forecast rows not found from %s, use default:%d", pSupp->algoOpt, pSupp->optRows);
    code = taosAnalBufWriteOptInt(pBuf, "forecast_rows", pSupp->optRows);
    if (code != 0) goto _OVER;
  }

  const char* prec = TSDB_TIME_PRECISION_MILLI_STR;
  if (pSupp->inputPrecision == TSDB_TIME_PRECISION_MICRO) prec = TSDB_TIME_PRECISION_MICRO_STR;
  if (pSupp->inputPrecision == TSDB_TIME_PRECISION_NANO) prec = TSDB_TIME_PRECISION_NANO_STR;
  code = taosAnalBufWriteOptStr(pBuf, "prec", prec);
  if (code != 0) goto _OVER;

  if (returnConf) {
    bool hasConf = taosAnalGetOptStr(pSupp->algoOpt, "conf", NULL, 0);
    if (!hasConf) {
      qInfo("forecast conf not found from %s, use default:%d", pSupp->algoOpt, ANAL_FORECAST_DEFAULT_CONF);
      code = taosAnalBufWriteOptInt(pBuf, "conf", ANAL_FORECAST_DEFAULT_CONF);
      if (code != 0) goto _OVER;
    }
  }

  code = taosAnalBufWriteColMeta(pBuf, 0, TSDB_DATA_TYPE_TIMESTAMP, "ts");
  if (code != 0) goto _OVER;

  code = taosAnalBufWriteColMeta(pBuf, 1, pSupp->inputValType, "val");
  if (code != 0) goto _OVER;

  code = taosAnalBufWriteDataBegin(pBuf);
  if (code != 0) goto _OVER;

  for (int32_t i = 0; i < 2; ++i) {
    code = taosAnalBufWriteColBegin(pBuf, i);
    if (code != 0) goto _OVER;
  }

_OVER:
  if (code != 0) {
    taosAnalBufClose(pBuf);
    taosAnalBufDestroy(pBuf);
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

  qInfo("forecast env is initialized, option:%s", pSupp->algoOpt);
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
  taosAnalBufDestroy(&pInfo->forecastSupp.analBuf);
  taosMemoryFreeClear(param);
}

#else

int32_t createForecastOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                   SOperatorInfo** pOptrInfo) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

#endif
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
#include "osMemory.h"
#include "querytask.h"
#include "tanalytics.h"
#include "taoserror.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tdef.h"
#include "thash.h"
#include "tjson.h"
#include "tmsg.h"
#include "regex.h"

#ifdef USE_ANALYTICS

#define FREQ_STR "freq"

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
  char         freq[64];         // frequency of data
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
  int32_t         resMarkSlot;
  SImputationSupp imputatSup;
} SImputationOperatorInfo;

static void    imputatDestroyOperatorInfo(void* param);
static int32_t imputationNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
static int32_t doImputation(SImputationOperatorInfo* pInfo, SExecTaskInfo* pTaskInfo);
static int32_t doCacheBlock(SImputationSupp* pSupp, SSDataBlock* pBlock, const char* id);
static int32_t doParseInput(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, SNodeList* pFuncs, const char* id);
static int32_t doSetResSlot(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, SExprSupp* pExprSup);
static int32_t doParseOption(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, const char* id);
static int32_t doCreateBuf(SImputationSupp* pSupp, const char* pId);
static int32_t estResultRowsAfterImputation(int32_t rows, int64_t skey, int64_t ekey, SImputationSupp* pSupp, const char* id);
static void    doInitOptions(SImputationSupp* pSupp);

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

  code = filterInitFromNode((SNode*)pImputatNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0, pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  doInitOptions(pSupp);

  code = doParseInput(pInfo, pSupp, pImputatNode->pFuncs, id);
  QUERY_CHECK_CODE(code, lino, _error);

  code = doSetResSlot(pInfo, pSupp, pExprSup);
  QUERY_CHECK_CODE(code, lino, _error);

  code = doParseOption(pInfo, pSupp, id);
  QUERY_CHECK_CODE(code, lino, _error);

  code = doCreateBuf(pSupp, id);
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
      code = doCacheBlock(pSupp, pBlock, idstr);

      qDebug("group:%" PRId64 ", blocks:%d, rows:%" PRId64 ", total rows:%d", pSupp->groupId, numOfBlocks,
             pBlock->info.rows, pSupp->numOfRows);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      qDebug("group:%" PRId64 ", read completed for new group coming, blocks:%d", pSupp->groupId, numOfBlocks);
      code = doImputation(pInfo, pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);

      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks = 1;
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
    code = doImputation(pInfo, pTaskInfo);
  }

  int64_t cost = taosGetTimestampUs() - st;
  qDebug("%s all groups finished, cost:%" PRId64 "us", idstr, cost);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", idstr, __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  (*ppRes) = (pBInfo->pRes->info.rows == 0) ? NULL : pBInfo->pRes;
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

  taosMemoryFreeClear(pInfo->options);
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

  code = taosAnalyBufWriteOptStr(pBuf, "freq", pSupp->freq);
  if (code != 0) return code;

  const char* prec = TSDB_TIME_PRECISION_MILLI_STR;
  if (pSupp->tsPrecision == TSDB_TIME_PRECISION_MICRO) prec = TSDB_TIME_PRECISION_MICRO_STR;
  if (pSupp->tsPrecision == TSDB_TIME_PRECISION_NANO) prec = TSDB_TIME_PRECISION_NANO_STR;
  if (pSupp->tsPrecision == TSDB_TIME_PRECISION_SECONDS) prec = "s";

  code = taosAnalyBufWriteOptStr(pBuf, "prec", prec);
  if (code != 0) return code;

  code = taosAnalyBufWriteOptInt(pBuf, ALGO_OPT_WNCHECK_NAME, pSupp->wncheck);
  if (code != 0) return code;

  if (pSupp->numOfRows < ANALY_IMPUTATION_INPUT_MIN_ROWS) {
    qError("%s history rows for forecasting not enough, min required:%d, current:%d", id, ANALY_FORECAST_MIN_ROWS,
           pSupp->numOfRows);
    return TSDB_CODE_ANA_ANODE_NOT_ENOUGH_ROWS;
  }

  code = estResultRowsAfterImputation(pSupp->numOfRows, pSupp->win.skey, pSupp->win.ekey, pSupp, id);
  if (code != 0) {
    return code;
  }

  code = taosAnalyBufClose(pBuf);
  return code;
}

static int32_t doImputationImpl(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, SSDataBlock* pBlock, const char* pId) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int32_t       resCurRow = pBlock->info.rows;
  int64_t       tmpI64 = 0;
  int32_t       tmpI32 = 0;
  float         tmpFloat = 0;
  double        tmpDouble = 0;
  int32_t       code = 0;

  SColumnInfoData* pResTargetCol = taosArrayGet(pBlock->pDataBlock, pInfo->resTargetSlot);
  if (NULL == pResTargetCol) {
    return terrno;
  }

  SJson* pJson = taosAnalySendReqRetJson(pInfo->algoUrl, ANALYTICS_HTTP_TYPE_POST, pBuf, pSupp->timeout, pId);
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

  if (code < 0) {
    goto _OVER;
  }

  SJson* pTarget = tjsonGetObjectItem(pJson, "target");
  if (pTarget == NULL) goto _OVER;

  SJson* pTsList = tjsonGetObjectItem(pJson, "ts");
  if (pTsList == NULL) goto _OVER;

  SJson* pMask = tjsonGetObjectItem(pJson, "mask");
  if (pMask == NULL) goto _OVER;

  int32_t listLen = tjsonGetArraySize(pTarget);
  if (listLen != rows) {
    goto _OVER;
  }

  if (pInfo->resTsSlot != -1) {
    SColumnInfoData* pResTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->resTsSlot);
    if (pResTsCol != NULL) {
      for (int32_t i = 0; i < rows; ++i) {
        SJson* tsJson = tjsonGetArrayItem(pTsList, i);
        tjsonGetObjectValueBigInt(tsJson, &tmpI64);
        colDataSetInt64(pResTsCol, resCurRow, &tmpI64);
        resCurRow++;
      }
    }
  }

  resCurRow = pBlock->info.rows;
  if (pResTargetCol->info.type == TSDB_DATA_TYPE_DOUBLE) {
    for (int32_t i = 0; i < rows; ++i) {
      SJson* targetJson = tjsonGetArrayItem(pTarget, i);
      tjsonGetObjectValueDouble(targetJson, &tmpDouble);
      colDataSetDouble(pResTargetCol, resCurRow, &tmpDouble);
      resCurRow++;
    }
  } else if (pResTargetCol->info.type == TSDB_DATA_TYPE_INT) {
    for (int32_t i = 0; i < rows; ++i) {
      SJson* targetJson = tjsonGetArrayItem(pTarget, i);
      tjsonGetObjectValueDouble(targetJson, &tmpDouble);
      int32_t t = tmpDouble;
      colDataSetInt32(pResTargetCol, resCurRow, &t);
      resCurRow++;
    }
  }  else if (pResTargetCol->info.type == TSDB_DATA_TYPE_FLOAT) {
    for (int32_t i = 0; i < rows; ++i) {
      SJson* targetJson = tjsonGetArrayItem(pTarget, i);
      tjsonGetObjectValueDouble(targetJson, &tmpDouble);
      float t = tmpDouble;
      colDataSetFloat(pResTargetCol, resCurRow, &t);
      resCurRow++;
    }
  }

  if (pInfo->resMarkSlot != -1) {
    SColumnInfoData* pResMaskCol = taosArrayGet(pBlock->pDataBlock, pInfo->resMarkSlot);
    if (pResMaskCol != NULL) {
      resCurRow = pBlock->info.rows;
      for (int32_t i = 0; i < rows; ++i) {
        SJson* maskJson = tjsonGetArrayItem(pMask, i);
        tjsonGetObjectValueBigInt(maskJson, &tmpI64);
        int32_t v = tmpI64;
        colDataSetInt32(pResMaskCol, resCurRow, &v);
        resCurRow++;
      }
    }
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

static int32_t doImputation(SImputationOperatorInfo* pInfo, SExecTaskInfo* pTaskInfo) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  char*                    id = GET_TASKID(pTaskInfo);
  SSDataBlock*             pRes = pInfo->binfo.pRes;
  SImputationSupp*         pSupp = &pInfo->imputatSup;

  if (pSupp->numOfRows <= 0) goto _OVER;

  qDebug("%s group:%" PRId64 ", do imputation, rows:%d", id, pSupp->groupId, pSupp->numOfRows);
  pRes->info.id.groupId = pSupp->groupId;

  code = finishBuildRequest(pInfo, pSupp, id);
  QUERY_CHECK_CODE(code, lino, _end);

  //   if (pBlock->info.rows < pBlock->info.capacity) {
  //   return TSDB_CODE_SUCCESS;
  // }

  // code = blockDataEnsureCapacity(pRes, newRowsNum);
  // if (code != TSDB_CODE_SUCCESS) {
  //   qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  //   return code;
  // }

  // QUERY_CHECK_CODE(code, lino, _end);

  code = doImputationImpl(pInfo, pSupp, pRes, id);
  QUERY_CHECK_CODE(code, lino, _end);

  uInfo("%s block:%d, imputation finalize", id, pSupp->numOfBlocks);

_end:
  pSupp->numOfBlocks = 0;
  taosAnalyBufDestroy(&pSupp->analyBuf);
  return code;

_OVER:
  taosArrayClear(pSupp->blocks);
  pSupp->numOfRows = 0;
  return code;
}

static int32_t doParseInput(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, SNodeList* pFuncs, const char* id) {
  int32_t code = 0;
  SNode*  pNode = NULL;

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
          SColumnNode* pTargetNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
          SColumnNode* pTsNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 1);

          pSupp->tsSlot = pTsNode->slotId;
          pSupp->tsPrecision = pTsNode->node.resType.precision;
          pSupp->targetSlot = pTargetNode->slotId;
          pSupp->targetType = pTargetNode->node.resType.type;

          // let's set the moment as the default imputation algorithm
          pInfo->options = taosStrdup("algo=moment");
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

static int32_t doSetResSlot(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, SExprSupp* pExprSup) {
  pInfo->resTsSlot = -1;
  pInfo->resTargetSlot = -1;
  pInfo->resMarkSlot = -1;

  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];
    int32_t    dstSlot = pExprInfo->base.resSchema.slotId;
    int32_t    functionType = pExprInfo->pExpr->_function.functionType;
    if (functionType == FUNCTION_TYPE_IMPUTATION) {
      pInfo->resTargetSlot = dstSlot;
    } else if (functionType == FUNCTION_TYPE_IMPUTATION_ROWTS) {
      pInfo->resTsSlot = dstSlot;
    } else if (functionType == FUNCTION_TYPE_IMPUTATION_MARK) {
      pInfo->resMarkSlot = dstSlot;
    }
  }

  return 0;
}

void doInitOptions(SImputationSupp* pSupp) {
  pSupp->numOfRows = 0;
  pSupp->wncheck = ANALY_DEFAULT_WNCHECK;

  pSupp->freq[0] = 'S';  // d(day), h(hour), m(minute),s(second), ms(millisecond), us(microsecond)
  pSupp->freq[1] = '\0';

  pSupp->tsSlot = -1;
  pSupp->targetSlot = -1;
  pSupp->targetType = -1;
  pSupp->tsPrecision = -1;

  pSupp->win.skey = INT64_MAX;
  pSupp->win.ekey = INT64_MIN;
}

int32_t parseFreq(SImputationSupp* pSupp, SHashObj* pHashMap, const char* id) {
  int32_t code = 0;
  char*   p = NULL;
  int32_t len = 0;
  regex_t regex;

  char* pFreq = taosHashGet(pHashMap, FREQ_STR, strlen(FREQ_STR));
  if (pFreq != NULL) {
    len = taosHashGetValueSize(pFreq);
    p = taosStrndupi(pFreq, len);
    if (p == NULL) {
      qError("%s failed to clone the freq param, code:%s", id, strerror(terrno));
      return terrno;
    }

    if (regcomp(&regex, "^([1-9][0-9]*|[1-9]*)(ms|us|ns|[smhdw])$", REG_EXTENDED | REG_ICASE) != 0) {
      qError("%s failed to compile regex for freq param", id);
      return TSDB_CODE_INVALID_PARA;
    }

    int32_t res = regexec(&regex, p, 0, NULL, 0);
    regfree(&regex);
    if (res != 0) {
      qError("%s invalid freq parameter: %s", id, p);
      taosMemoryFreeClear(p);
      return TSDB_CODE_INVALID_PARA;
    }

    if (code == TSDB_CODE_SUCCESS) {
      tstrncpy(pSupp->freq, pFreq, len + 1);
      qDebug("%s data freq:%s", id, pSupp->freq);
    }
  } else {
    qDebug("%s not specify data freq, default: %s", id, pSupp->freq);
  }

  taosMemoryFreeClear(p);
  return code;
}

int32_t estResultRowsAfterImputation(int32_t rows, int64_t skey, int64_t ekey, SImputationSupp* pSupp, const char* id) {
  int64_t range = ekey - skey;
  double  factor = 0;
  if (pSupp->tsPrecision == TSDB_TIME_PRECISION_MILLI) {
    if (strcmp(pSupp->freq, "h") == 0) {
      factor = 0.001 * 1/3600;
    } else if (strcmp(pSupp->freq, "m") == 0) {
      factor = 0.001 * 1/60;
    } else if (strcmp(pSupp->freq, "s") == 0) {
      factor = 0.001;
    } else if (strcmp(pSupp->freq, "ms") == 0) {
      factor = 1;
    } else if (strcmp(pSupp->freq, "us") == 0) {
      factor *= 1000;
    } else if (strcmp(pSupp->freq, "ns") == 0) {
      factor *= 1000000;
    }

    int64_t num = range * factor - rows;
    if (num > ANALY_MAX_IMPUT_ROWS) {
      qError("%s too many rows to imputation, est:%"PRId64, id, num);
      return TSDB_CODE_INVALID_PARA;
    }
  } else if (pSupp->tsPrecision == TSDB_TIME_PRECISION_MICRO) {
    if (strcmp(pSupp->freq, "h") == 0) {
      factor = 0.000001 * 1/3600;
    } else if (strcmp(pSupp->freq, "m") == 0) {
      factor = 0.000001 * 1/60;
    } else if (strcmp(pSupp->freq, "s") == 0) {
      factor = 0.000001;
    } else if (strcmp(pSupp->freq, "ms") == 0) {
      factor = 1000;
    } else if (strcmp(pSupp->freq, "us") == 0) {
      factor *= 1;
    } else if (strcmp(pSupp->freq, "ns") == 0) {
      factor *= 1000;
    }

    int64_t num = range * factor - rows;
    if (num > ANALY_MAX_IMPUT_ROWS) {
      qError("%s too many rows to imputation, est:%"PRId64, id, num);
      return TSDB_CODE_INVALID_PARA;
    }
  } else if (pSupp->tsPrecision == TSDB_TIME_PRECISION_NANO) {
    if (strcmp(pSupp->freq, "h") == 0) {
      factor = 0.000000001 * 1/3600;
    } else if (strcmp(pSupp->freq, "m") == 0) {
      factor = 0.000000001 * 1/60;
    } else if (strcmp(pSupp->freq, "s") == 0) {
      factor = 0.000000001;
    } else if (strcmp(pSupp->freq, "ms") == 0) {
      factor = 1000000;
    } else if (strcmp(pSupp->freq, "us") == 0) {
      factor *= 1000;
    } else if (strcmp(pSupp->freq, "ns") == 0) {
      factor *= 1;
    }

    int64_t num = range * factor - rows;
    if (num > ANALY_MAX_IMPUT_ROWS) {
      qError("%s too many rows to imputation, est:%"PRId64, id, num);
      return TSDB_CODE_INVALID_PARA;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doParseOption(SImputationOperatorInfo* pInfo, SImputationSupp* pSupp, const char* id) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SHashObj* pHashMap = NULL;

  code = taosAnalyGetOpts(pInfo->options, &pHashMap);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = taosAnalysisParseAlgo(pInfo->options, pInfo->algoName, pInfo->algoUrl, ANALY_ALGO_TYPE_IMPUTATION,
                               tListLen(pInfo->algoUrl), pHashMap, id);
  TSDB_CHECK_CODE(code, lino, _end);

  // extract the timeout parameter
  pSupp->timeout = taosAnalysisParseTimout(pHashMap, id);
  pSupp->wncheck = taosAnalysisParseWncheck(pHashMap, id);

  // extract data freq
  code = parseFreq(pSupp, pHashMap, id);

_end:
  taosHashCleanup(pHashMap);
  return code;
}

static int32_t doCreateBuf(SImputationSupp* pSupp, const char* pId) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int64_t       ts = taosGetTimestampNs();
  int32_t       index = 0;

  pBuf->bufType = ANALYTICS_BUF_TYPE_JSON_COL;
  snprintf(pBuf->fileName, sizeof(pBuf->fileName), "%s/tdengine-imput-%p-%" PRId64, tsTempDir, pSupp, ts);

  int32_t code = tsosAnalyBufOpen(pBuf, pSupp->numOfCols, pId);
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
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
#include "types.h"

#ifdef USE_ANALYTICS

#define FREQ_STR     "freq"
#define RADIUS_STR   "radius"
#define LAGSTART_STR "lag_start"
#define LAGEND_STR   "lag_end"

typedef struct {
  int64_t      timeout;
  int8_t       wncheck;
  SAnalyticBuf analyBuf;
  int32_t      numOfRows;
  int32_t      numOfBlocks;
  int32_t      numOfCols;
  STimeWindow  win;
  uint64_t     groupId;
  SArray      *pBlocks;    // SSDataBlock*
} SBaseSupp;

typedef struct {
  SBaseSupp    base;
  int32_t      targetSlot;
  int32_t      targetType;
  int32_t      tsSlot;
  int32_t      tsPrecision;
  int32_t      resTsSlot;
  int32_t      resMarkSlot;
  char         freq[64];         // frequency of data
} SImputationSupp;

typedef struct {
  int32_t   radius;
  int32_t   lagStart;
  int32_t   lagEnd;
  SBaseSupp base;

  int32_t targetSlot1;
  int32_t targetSlot2;

  int32_t targetSlot1Type;
  int32_t targetSlot2Type;
} SCorrelationSupp;

typedef struct {
  SOptrBasicInfo   binfo;
  SExprSupp        scalarSup;
  char             algoName[TSDB_ANALYTIC_ALGO_NAME_LEN];
  char             algoUrl[TSDB_ANALYTIC_ALGO_URL_LEN];
  char*            options;
  SColumn          targetCol;
  int32_t          resTargetSlot;
  int32_t          analysisType;
  SImputationSupp  imputatSup;
  SCorrelationSupp corrSupp;
} SAnalysisOperatorInfo;

static void    analysisDestroyOperatorInfo(void* param);
static int32_t imputationNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);
static int32_t doAnalysis(SAnalysisOperatorInfo* pInfo, SExecTaskInfo* pTaskInfo);
static int32_t doCacheBlock(SAnalysisOperatorInfo* pInfo, SSDataBlock* pBlock, const char* id);
static int32_t doParseInputForImputation(SAnalysisOperatorInfo* pInfo, SImputationSupp* pSupp, SNodeList* pFuncs, const char* id);
static int32_t doParseInputForDtw(SAnalysisOperatorInfo* pInfo, SCorrelationSupp* pSupp, SNodeList* pFuncs, const char* id);
static int32_t doParseInputForTlcc(SAnalysisOperatorInfo* pInfo, SCorrelationSupp* pSupp, SNodeList* pFuncs, const char* id);
static int32_t doSetResSlot(SAnalysisOperatorInfo* pInfo, SExprSupp* pExprSup);
static int32_t doParseOption(SAnalysisOperatorInfo* pInfo, const char* id);
static int32_t doCreateBuf(SAnalysisOperatorInfo* pInfo, const char* pId);
static int32_t estResultRowsAfterImputation(int32_t rows, int64_t skey, int64_t ekey, int32_t prec, const char* pFreq, const char* id);
static void    doInitImputOptions(SImputationSupp* pSupp);
static void    doInitDtwOptions(SCorrelationSupp* pSupp);
static int32_t parseFreq(SImputationSupp* pSupp, SHashObj* pHashMap, const char* id);
static void    parseRadius(SCorrelationSupp* pSupp, SHashObj* pHashMap, const char* id);

int32_t createGenericAnalysisOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo,
                                          SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  size_t                     keyBufSize = 0;
  int32_t                    num = 0;
  SExprInfo*                 pExprInfo = NULL;
  int32_t                    numOfExprs = 0;
  const char*                id = GET_TASKID(pTaskInfo);
  SGenericAnalysisPhysiNode* pAnalysisNode = (SGenericAnalysisPhysiNode*)physiNode;
  SExprSupp*                 pExprSup = NULL;

  SAnalysisOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SAnalysisOperatorInfo));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    code = terrno;
    goto _error;
  }

  pAnalysisNode = (SGenericAnalysisPhysiNode*)physiNode;
  pExprSup = &pOperator->exprSupp;

  code = createExprInfo(pAnalysisNode->pFuncs, NULL, &pExprInfo, &numOfExprs);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExprSupp(pExprSup, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pAnalysisNode->pExprs != NULL) {
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pAnalysisNode->pExprs, NULL, &pScalarExprInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pAnalysisNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0, pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  for (int32_t i = 0; i < numOfExprs; ++i) {
    int32_t type = pExprInfo[i].pExpr->_function.functionType;
    if (type == FUNCTION_TYPE_IMPUTATION || type == FUNCTION_TYPE_DTW || type == FUNCTION_TYPE_DTW_PATH ||
        type == FUNCTION_TYPE_TLCC) {
      pInfo->analysisType = type;
      break;
    }
  }

  if (pInfo->analysisType == FUNCTION_TYPE_IMPUTATION) {
    doInitImputOptions(&pInfo->imputatSup);
    code = doParseInputForImputation(pInfo, &pInfo->imputatSup, pAnalysisNode->pFuncs, id);
    QUERY_CHECK_CODE(code, lino, _error);
  } else if (pInfo->analysisType == FUNCTION_TYPE_DTW || pInfo->analysisType == FUNCTION_TYPE_DTW_PATH) {
    doInitDtwOptions(&pInfo->corrSupp);
    code = doParseInputForDtw(pInfo, &pInfo->corrSupp, pAnalysisNode->pFuncs, id);
    QUERY_CHECK_CODE(code, lino, _error);
  } else if (pInfo->analysisType == FUNCTION_TYPE_TLCC) {
    doInitDtwOptions(&pInfo->corrSupp);
    code = doParseInputForTlcc(pInfo, &pInfo->corrSupp, pAnalysisNode->pFuncs, id);
    QUERY_CHECK_CODE(code, lino, _error);
  } else {
    code = TSDB_CODE_INVALID_PARA;
    goto _error;
  }

  code = doParseOption(pInfo, id);
  QUERY_CHECK_CODE(code, lino, _error);

  code = doSetResSlot(pInfo, pExprSup);
  QUERY_CHECK_CODE(code, lino, _error);

  code = doCreateBuf(pInfo, id);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->binfo.pRes = createDataBlockFromDescNode(physiNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->binfo.pRes, code, lino, _error, terrno);

  setOperatorInfo(pOperator, "GenericAnalyOptr", QUERY_NODE_PHYSICAL_PLAN_ANALYSIS_FUNC, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, imputationNext, NULL, analysisDestroyOperatorInfo,
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

  if (pInfo != NULL) analysisDestroyOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

static int32_t getBaseSupp(SAnalysisOperatorInfo* pInfo, SBaseSupp** ppSupp, const char* pId) {
  int32_t code = 0;
  *ppSupp = NULL;

  switch (pInfo->analysisType) {
    case FUNCTION_TYPE_IMPUTATION:
      *ppSupp = &pInfo->imputatSup.base;
      break;
    case FUNCTION_TYPE_DTW:
    case FUNCTION_TYPE_DTW_PATH:
    case FUNCTION_TYPE_TLCC:
      *ppSupp = &pInfo->corrSupp.base;
      break;
    default:
      // Handle error: unknown analysis type
      code = TSDB_CODE_INVALID_PARA;
      qError("%s unknown analysis type: %d", pId, pInfo->analysisType);
  }

  return code;
}

static int32_t imputationNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  SAnalysisOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*         pTaskInfo = pOperator->pTaskInfo;
  SOptrBasicInfo*        pBInfo = &pInfo->binfo;
  SSDataBlock*           pRes = pInfo->binfo.pRes;
  int64_t                st = taosGetTimestampUs();
  const char*            idstr = GET_TASKID(pTaskInfo);
  SBaseSupp*             pSupp = NULL;

  code = getBaseSupp(pInfo, &pSupp, idstr);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t numOfBlocks = taosArrayGetSize(pSupp->pBlocks);
  blockDataCleanup(pRes);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    if (pSupp->groupId == 0 || pSupp->groupId == pBlock->info.id.groupId) {
      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks++;
      code = doCacheBlock(pInfo, pBlock, idstr);

      qDebug("group:%" PRId64 ", blocks:%d, rows:%" PRId64 ", total rows:%d", pSupp->groupId, numOfBlocks,
             pBlock->info.rows, pSupp->numOfRows);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      qDebug("group:%" PRId64 ", read completed for new group coming, blocks:%d", pSupp->groupId, numOfBlocks);
      code = doAnalysis(pInfo, pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);

      pSupp->groupId = pBlock->info.id.groupId;
      numOfBlocks = 1;
      qDebug("group:%" PRId64 ", new group, rows:%" PRId64 ", total rows:%d", pSupp->groupId, pBlock->info.rows,
             pSupp->numOfRows);
      code = doCacheBlock(pInfo, pBlock, idstr);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pRes->info.rows > 0) {
      (*ppRes) = pRes;
      qDebug("group:%" PRId64 ", return to upstream, blocks:%d", pRes->info.id.groupId, numOfBlocks);
      return code;
    }
  }

  if (numOfBlocks > 0) {
    qDebug("group:%" PRId64 ", read finish, blocks:%d", pSupp->groupId, numOfBlocks);
    code = doAnalysis(pInfo, pTaskInfo);
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

static void analysisDestroyOperatorInfo(void* param) {
  SAnalysisOperatorInfo* pInfo = (SAnalysisOperatorInfo*)param;
  if (pInfo == NULL) return;

  cleanupBasicInfo(&pInfo->binfo);
  cleanupExprSupp(&pInfo->scalarSup);

  SArray* pBlocks = NULL;
  if (pInfo->analysisType == FUNCTION_TYPE_IMPUTATION) {
    pBlocks = pInfo->imputatSup.base.pBlocks;
  } else {
    pBlocks = pInfo->corrSupp.base.pBlocks;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pBlocks); ++i) {
    SSDataBlock* pBlock = taosArrayGetP(pBlocks, i);
    blockDataDestroy(pBlock);
  }

  taosArrayDestroy(pBlocks);
  taosMemoryFreeClear(param);
}

static int32_t doCacheBlockForImputation(SImputationSupp* pSupp, const char* id, SSDataBlock* pBlock) {
  SAnalyticBuf* pBuf = &pSupp->base.analyBuf;
  int32_t code = 0;

  if (pSupp->base.numOfRows > ANALY_IMPUTATION_INPUT_MAX_ROWS) {
    qError("%s too many rows for imputation, maximum allowed:%d, input:%d", id, ANALY_IMPUTATION_INPUT_MAX_ROWS,
           pSupp->base.numOfRows);
    return TSDB_CODE_ANA_ANODE_TOO_MANY_ROWS;
  }

  pSupp->base.numOfBlocks++;
  qDebug("%s block:%d, %p rows:%" PRId64, id, pSupp->base.numOfBlocks, pBlock, pBlock->info.rows);

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

    pSupp->base.win.skey = MIN(pSupp->base.win.skey, ts);
    pSupp->base.win.ekey = MAX(pSupp->base.win.ekey, ts);

    pSupp->base.numOfRows++;

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

  return code;
}

int32_t doCacheBlockForDtw(SCorrelationSupp* pSupp, const char* id, SSDataBlock* pBlock) {
  SAnalyticBuf* pBuf = &pSupp->base.analyBuf;
  int32_t code = 0;

  pSupp->base.numOfBlocks++;
  qDebug("%s block:%d, %p rows:%" PRId64, id, pSupp->base.numOfBlocks, pBlock, pBlock->info.rows);

  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, pSupp->targetSlot1);
    SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, pSupp->targetSlot2);
    if (pCol1 == NULL || pCol2 == NULL) {
      break;
    }

    int32_t index = 0;

    char*   val1 = colDataGetData(pCol1, j);
    char*   val2 = colDataGetData(pCol2, j);

    pSupp->base.numOfRows++;

    // write the primary time stamp column data
    code = taosAnalyBufWriteColData(pBuf, index++, pCol1->info.type, val1);
    if (TSDB_CODE_SUCCESS != code) {
      qError("%s failed to write col1 in buf, code:%s", id, tstrerror(code));
      return code;
    }

    // write the main column for imputation
    code = taosAnalyBufWriteColData(pBuf, index++, pCol2->info.type, val2);
    if (TSDB_CODE_SUCCESS != code) {
      qError("%s failed to write col2 in buf, code:%s", id, tstrerror(code));
      return code;
    }
  }

  return code;

}

static int32_t doCacheBlock(SAnalysisOperatorInfo* pInfo, SSDataBlock* pBlock, const char* id) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;

  if (pInfo->analysisType == FUNCTION_TYPE_IMPUTATION) {
    code = doCacheBlockForImputation(&pInfo->imputatSup, id, pBlock);
  } else if (pInfo->analysisType == FUNCTION_TYPE_DTW || pInfo->analysisType == FUNCTION_TYPE_DTW_PATH || pInfo->analysisType == FUNCTION_TYPE_TLCC) {
    code = doCacheBlockForDtw(&pInfo->corrSupp, id, pBlock);
  }

  return code;
}

static int32_t finishBuildRequest(SAnalysisOperatorInfo* pInfo, SBaseSupp* pSupp, const char* id) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int32_t       code = 0;

  // let's check existed rows for imputation
  if (pInfo->analysisType == FUNCTION_TYPE_IMPUTATION) {
    if (pSupp->numOfRows < ANALY_IMPUTATION_INPUT_MIN_ROWS) {
      qError("%s input rows for imputation aren't enough, min required:%d, current:%d", id, ANALY_IMPUTATION_INPUT_MIN_ROWS,
             pSupp->numOfRows);
      return TSDB_CODE_ANA_ANODE_NOT_ENOUGH_ROWS;
    }

    code = estResultRowsAfterImputation(pSupp->numOfRows, pSupp->win.skey, pSupp->win.ekey, 
      pInfo->imputatSup.tsPrecision, pInfo->imputatSup.freq, id);
    if (code != 0) {
      return code;
    }
  }

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

  if (pInfo->analysisType == FUNCTION_TYPE_IMPUTATION) {
    const char* prec = TSDB_TIME_PRECISION_MILLI_STR;
    int32_t p = pInfo->imputatSup.tsPrecision;

    switch (p) {
      case TSDB_TIME_PRECISION_MICRO:
        prec = TSDB_TIME_PRECISION_MICRO_STR;
        break;
      case TSDB_TIME_PRECISION_NANO:
        prec = TSDB_TIME_PRECISION_NANO_STR;
        break;
      case TSDB_TIME_PRECISION_SECONDS:
        prec = "s";
        break;
      default:
        prec = TSDB_TIME_PRECISION_MILLI_STR;
    }

    code = taosAnalyBufWriteOptStr(pBuf, "freq", pInfo->imputatSup.freq);
    if (code != 0) return code;

    code = taosAnalyBufWriteOptStr(pBuf, "prec", prec);
    if (code != 0) return code;
  }

  code = taosAnalyBufWriteOptInt(pBuf, ALGO_OPT_WNCHECK_NAME, pSupp->wncheck);
  if (code != 0) return code;

  code = taosAnalyBufClose(pBuf);
  return code;
}

static int32_t buildDtwPathResult(SJson* pathJson, SColumnInfoData* pResTargetCol, const char* pId, char* buf,
                                  int32_t bufSize) {
  int32_t pair = tjsonGetArraySize(pathJson);
  if (pair != 2) {
    qError("%s invalid path data, should be array of pair", pId);
    return TSDB_CODE_ANA_ANODE_RETURN_ERROR;
  }

  SJson* first = tjsonGetArrayItem(pathJson, 0);
  SJson* second = tjsonGetArrayItem(pathJson, 1);

  int64_t t1, t2;
  tjsonGetObjectValueBigInt(first, &t1);
  tjsonGetObjectValueBigInt(second, &t2);

  int32_t n = snprintf(varDataVal(buf), bufSize - VARSTR_HEADER_SIZE, "(%" PRId64 ", %" PRId64 ")", t1, t2);
  if (n > 0) {
    varDataSetLen(buf, n);
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_ANA_ANODE_RETURN_ERROR;
  }
}

static int32_t doAnalysisImpl(SAnalysisOperatorInfo* pInfo, SBaseSupp* pSupp, SSDataBlock* pBlock, const char* pId) {
  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int32_t       resCurRow = pBlock->info.rows;
  int64_t       tmpI64 = 0;
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
    char pMsg[1024] = {0};
    code = tjsonGetStringValue(pJson, "msg", pMsg);
    if (code != 0) {
      qError("%s failed to get msg from rsp, unknown error", pId);
    } else {
      qError("%s failed to exec analysis, msg:%s", pId, pMsg);
    }

    tjsonDelete(pJson);
    return TSDB_CODE_ANA_ANODE_RETURN_ERROR;
  }

  if (code < 0) {
    goto _OVER;
  }

  if (pInfo->analysisType == FUNCTION_TYPE_IMPUTATION) {
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

    if (pInfo->imputatSup.resTsSlot != -1) {
      SColumnInfoData* pResTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->imputatSup.resTsSlot);
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
    } else if (pResTargetCol->info.type == TSDB_DATA_TYPE_FLOAT) {
      for (int32_t i = 0; i < rows; ++i) {
        SJson* targetJson = tjsonGetArrayItem(pTarget, i);
        tjsonGetObjectValueDouble(targetJson, &tmpDouble);
        float t = tmpDouble;
        colDataSetFloat(pResTargetCol, resCurRow, &t);
        resCurRow++;
      }
    }

    if (pInfo->imputatSup.resMarkSlot != -1) {
      SColumnInfoData* pResMarkCol = taosArrayGet(pBlock->pDataBlock, pInfo->imputatSup.resMarkSlot);
      if (pResMarkCol != NULL) {
        resCurRow = pBlock->info.rows;
        for (int32_t i = 0; i < rows; ++i) {
          SJson* markJson = tjsonGetArrayItem(pMask, i);
          tjsonGetObjectValueBigInt(markJson, &tmpI64);
          int32_t v = tmpI64;
          colDataSetInt32(pResMarkCol, resCurRow, &v);
          resCurRow++;
        }
      }
    }
  } else if (pInfo->analysisType == FUNCTION_TYPE_DTW) {
    // dtw result example:
    // {'option': 'algo=dtw,radius=1', 'rows': 4, 'distance': 1.6, 'path': [(0, 0), (1, 0), (2, 0), (3, 1), (3, 2), (3, 3)]}

    SJson* pTarget = tjsonGetObjectItem(pJson, "distance");
    if (pTarget == NULL) goto _OVER;

    tjsonGetObjectValueDouble(pTarget, &tmpDouble);
    colDataSetDouble(pResTargetCol, resCurRow, &tmpDouble);
    rows = 1;
  } else if (pInfo->analysisType == FUNCTION_TYPE_DTW_PATH) {
    // dtw path results are the same as above
    SJson* pPath = tjsonGetObjectItem(pJson, "path");
    if (pPath == NULL) goto _OVER;

    int32_t listLen = tjsonGetArraySize(pPath);
    if (listLen != rows) {
      goto _OVER;
    }

    for (int32_t i = 0; i < rows; ++i) {
      SJson* pathJson = tjsonGetArrayItem(pPath, i);

      char buf[128 + VARSTR_HEADER_SIZE] = {0};
      code = buildDtwPathResult(pathJson, pResTargetCol, pId, buf, sizeof(buf));
      if (code != 0) {
        qError("%s failed to build path result, code:%s", pId, tstrerror(code));
        goto _OVER;
      }

      code = colDataSetVal(pResTargetCol, resCurRow, buf, false);
      if (code != 0) {
        qError("%s failed to set path result to column, code:%s", pId, tstrerror(code));
        goto _OVER;
      }

      resCurRow++;
    }
  } else if (pInfo->analysisType == FUNCTION_TYPE_TLCC) {
    // tlcc result example:
    // {'option': 'algo=tlcc,lag_start=-1,lag_end=1', 'rows': 3, 'lags': [-1, 0, 1], 'ccf_vals': [-0.24, 0.9, -0.2]}
    SJson* pLags = tjsonGetObjectItem(pJson, "lags");
    if (pLags == NULL) goto _OVER;

    SJson* pCcfVals = tjsonGetObjectItem(pJson, "ccf_vals");
    if (pCcfVals == NULL) goto _OVER;

    for (int32_t i = 0; i < rows; ++i) {
      SJson* ccfValJson = tjsonGetArrayItem(pCcfVals, i);
      tjsonGetObjectValueDouble(ccfValJson, &tmpDouble);

      int64_t index = 0;
      SJson*  pLastIndexJson = tjsonGetArrayItem(pLags, i);
      tjsonGetObjectValueBigInt(pLastIndexJson, &index);

      char    buf[128 + VARSTR_HEADER_SIZE] = {0};
      int32_t n = snprintf(varDataVal(buf), tListLen(buf) - VARSTR_HEADER_SIZE, "(%" PRId64 ", %.4f)", index, tmpDouble);
      if (n > 0) {
        varDataSetLen(buf, n);
      } else {
        return TSDB_CODE_ANA_ANODE_RETURN_ERROR;
      }

      code = colDataSetVal(pResTargetCol, resCurRow, buf, false);
      resCurRow++;
    }

    tjsonGetObjectValueDouble(pLags, &tmpDouble);
    colDataSetDouble(pResTargetCol, resCurRow, &tmpDouble);
  }

  pBlock->info.rows += rows;

  if (pJson != NULL) tjsonDelete(pJson);
  return 0;

_OVER:
  tjsonDelete(pJson);
  if (code == 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
  }

  qError("%s failed to perform analysis finalize since %s", pId, tstrerror(code));
  return code;
}

static int32_t doAnalysis(SAnalysisOperatorInfo* pInfo, SExecTaskInfo* pTaskInfo) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  char*                    id = GET_TASKID(pTaskInfo);
  SSDataBlock*             pRes = pInfo->binfo.pRes;

  SBaseSupp* pSupp = (pInfo->analysisType==FUNCTION_TYPE_IMPUTATION)? &pInfo->imputatSup.base:&pInfo->corrSupp.base;

  if (pSupp->numOfRows <= 0) {
    taosArrayClear(pSupp->pBlocks);
    pSupp->numOfRows = 0;
    return code;
  }

  qDebug("%s group:%" PRId64 ", do analysis, rows:%d", id, pSupp->groupId, pSupp->numOfRows);
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

  code = doAnalysisImpl(pInfo, pSupp, pRes, id);
  QUERY_CHECK_CODE(code, lino, _end);

  uInfo("%s block:%d, analysis finalize", id, pSupp->numOfBlocks);

_end:
  pSupp->numOfBlocks = 0;
  taosAnalyBufDestroy(&pSupp->analyBuf);
  return code;
}

static int32_t doParseInputForImputation(SAnalysisOperatorInfo* pInfo, SImputationSupp* pSupp, SNodeList* pFuncs, const char* id) {
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

        pSupp->base.numOfCols = 2;

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

int32_t doParseInputForDtw(SAnalysisOperatorInfo* pInfo, SCorrelationSupp* pSupp, SNodeList* pFuncs, const char* id) {
  int32_t code = 0;
  SNode*  pNode = NULL;

  FOREACH(pNode, pFuncs) {
    if ((nodeType(pNode) == QUERY_NODE_TARGET) && (nodeType(((STargetNode*)pNode)->pExpr) == QUERY_NODE_FUNCTION)) {
      SFunctionNode* pFunc = (SFunctionNode*)((STargetNode*)pNode)->pExpr;
      int32_t        numOfParam = LIST_LENGTH(pFunc->pParameterList);

      if (pFunc->funcType == FUNCTION_TYPE_DTW || pFunc->funcType == FUNCTION_TYPE_DTW_PATH) {
        // code = validInputParams(pFunc, id);
        // if (code) {
          // return code;
        // }

        pSupp->base.numOfCols = 2;

        if (numOfParam == 2) {
          // column1, column2
          SColumnNode* pTargetNode1 = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
          SColumnNode* pTargetNode2 = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 1);

          pSupp->targetSlot1 = pTargetNode1->slotId;
          pSupp->targetSlot1Type = pTargetNode1->node.resType.type;

          pSupp->targetSlot2 = pTargetNode2->slotId;
          pSupp->targetSlot2Type = pTargetNode2->node.resType.type;

          // let's set the default radius to be 1
          pInfo->options = taosStrdup("algo=dtw,radius=1");
        } else if (numOfParam == 3) {
          // column, options, ts

          SColumnNode* pTargetNode1 = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
          SColumnNode* pTargetNode2 = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 1);

          pSupp->targetSlot1 = pTargetNode1->slotId;
          pSupp->targetSlot1Type = pTargetNode1->node.resType.type;

          pSupp->targetSlot2 = pTargetNode2->slotId;
          pSupp->targetSlot2Type = pTargetNode2->node.resType.type;

          SValueNode* pOptNode = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2);

          int32_t bufLen = strlen(pOptNode->literal) + 30;
          pInfo->options = taosMemoryMalloc(bufLen);
          if (pInfo->options == NULL) {
            code = terrno;
            qError("%s failed to prepare option buffer, code:%s", id, tstrerror(code));
            return code;
          }

          int32_t ret = snprintf(pInfo->options, bufLen, "%s,%s", pOptNode->literal, "algo=dtw");
          if (ret < 0 || ret >= bufLen) {
            code = TSDB_CODE_OUT_OF_MEMORY;
            qError("%s failed to clone options string, code:%s", id, tstrerror(code));
            return code;
          }
        } else {
          qError("%s too many parameters in dtw function", id);
          code = TSDB_CODE_INVALID_PARA;
          return code;
        }
      }
    }
  }

  if (pInfo->options == NULL) {
    qError("%s option is missing or clone option string failed, failed to do correlation analysis", id);
    code = TSDB_CODE_ANA_INTERNAL_ERROR;
  }

  return code;
}

static int32_t doParseInputForTlcc(SAnalysisOperatorInfo* pInfo, SCorrelationSupp* pSupp, SNodeList* pFuncs, const char* id) {
  int32_t code = 0;
  SNode*  pNode = NULL;

  FOREACH(pNode, pFuncs) {
    if ((nodeType(pNode) == QUERY_NODE_TARGET) && (nodeType(((STargetNode*)pNode)->pExpr) == QUERY_NODE_FUNCTION)) {
      SFunctionNode* pFunc = (SFunctionNode*)((STargetNode*)pNode)->pExpr;
      int32_t        numOfParam = LIST_LENGTH(pFunc->pParameterList);

      if (pFunc->funcType == FUNCTION_TYPE_TLCC) {
        // code = validInputParams(pFunc, id);
        // if (code) {
          // return code;
        // }

        pSupp->base.numOfCols = 2;

        if (numOfParam == 2) {
          // column1, column2
          SColumnNode* pTargetNode1 = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
          SColumnNode* pTargetNode2 = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 1);

          pSupp->targetSlot1 = pTargetNode1->slotId;
          pSupp->targetSlot1Type = pTargetNode1->node.resType.type;

          pSupp->targetSlot2 = pTargetNode2->slotId;
          pSupp->targetSlot2Type = pTargetNode2->node.resType.type;

          // let's set the default radius to be 1
          pInfo->options = taosStrdup("algo=tlcc,lag_start=-1,lag_end=1");
        } else if (numOfParam == 3) {
          // column, options, ts
          // SColumnNode* pTargetNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
          // if (nodeType(pTargetNode) != QUERY_NODE_COLUMN) {
          // return error
          // }

          SColumnNode* pTargetNode1 = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
          SColumnNode* pTargetNode2 = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 1);

          pSupp->targetSlot1 = pTargetNode1->slotId;
          pSupp->targetSlot1Type = pTargetNode1->node.resType.type;

          pSupp->targetSlot2 = pTargetNode2->slotId;
          pSupp->targetSlot2Type = pTargetNode2->node.resType.type;

          SValueNode* pOptNode = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2);

          int32_t bufLen = strlen(pOptNode->literal) + 30;
          pInfo->options = taosMemoryMalloc(bufLen);
          if (pInfo->options == NULL) {
            code = terrno;
            qError("%s failed to prepare option buffer, code:%s", id, tstrerror(code));
            return code;
          }

          int32_t ret = snprintf(pInfo->options, bufLen, "%s,%s", pOptNode->literal, "algo=tlcc");
          if (ret < 0 || ret >= bufLen) {
            code = TSDB_CODE_OUT_OF_MEMORY;
            qError("%s failed to clone options string, code:%s", id, tstrerror(code));
            return code;
          }
        } else {
          qError("%s too many parameters in tlcc function", id);
          code = TSDB_CODE_INVALID_PARA;
          return code;
        }
      }
    }
  }

  if (pInfo->options == NULL) {
    qError("%s option is missing or clone option string failed, failed to do correlation analysis", id);
    code = TSDB_CODE_ANA_INTERNAL_ERROR;
  }

  return code;
}

static int32_t doSetResSlot(SAnalysisOperatorInfo* pInfo, SExprSupp* pExprSup) {
  pInfo->imputatSup.resTsSlot = -1;
  pInfo->resTargetSlot = -1;
  pInfo->imputatSup.resMarkSlot = -1;

  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];
    int32_t    dstSlot = pExprInfo->base.resSchema.slotId;
    int32_t    functionType = pExprInfo->pExpr->_function.functionType;
    if (functionType == FUNCTION_TYPE_IMPUTATION || functionType == FUNCTION_TYPE_DTW ||
        functionType == FUNCTION_TYPE_DTW_PATH || functionType == FUNCTION_TYPE_TLCC) {
      pInfo->resTargetSlot = dstSlot;
    } else if (functionType == FUNCTION_TYPE_IMPUTATION_ROWTS) {
      pInfo->imputatSup.resTsSlot = dstSlot;
    } else if (functionType == FUNCTION_TYPE_IMPUTATION_MARK) {
      pInfo->imputatSup.resMarkSlot = dstSlot;
    }
  }

  return 0;
}

void doInitBaseOptions(SBaseSupp* pSupp) {
  pSupp->numOfCols = 0;
  pSupp->numOfRows = 0;
  pSupp->numOfBlocks = 0;
  pSupp->wncheck = ANALY_DEFAULT_WNCHECK;
  pSupp->timeout = ANALY_DEFAULT_TIMEOUT;
  pSupp->groupId = 0;

  pSupp->win.skey = INT64_MAX;
  pSupp->win.ekey = INT64_MIN;
}

void doInitImputOptions(SImputationSupp* pSupp) {
  doInitBaseOptions(&pSupp->base);

  pSupp->freq[0] = 'S';  // d(day), h(hour), m(minute),s(second), ms(millisecond), us(microsecond)
  pSupp->freq[1] = '\0';

  pSupp->tsSlot = -1;
  pSupp->targetSlot = -1;
  pSupp->targetType = -1;
  pSupp->tsPrecision = -1;
}

void doInitDtwOptions(SCorrelationSupp* pSupp) {
  doInitBaseOptions(&pSupp->base);
  pSupp->radius = 1;

  pSupp->lagStart = -1;
  pSupp->lagEnd = 1;

  pSupp->targetSlot1 = -1;
  pSupp->targetSlot1Type = TSDB_DATA_TYPE_INT;
  pSupp->targetSlot2 = -1;
  pSupp->targetSlot2Type = TSDB_DATA_TYPE_INT;
}

int32_t parseFreq(SImputationSupp* pSupp, SHashObj* pHashMap, const char* id) {
  int32_t     code = 0;
  char*       p = NULL;
  int32_t     len = 0;

  char* pFreq = taosHashGet(pHashMap, FREQ_STR, strlen(FREQ_STR));
  if (pFreq != NULL) {
    len = taosHashGetValueSize(pFreq);
    p = taosStrndupi(pSupp->freq, len);
    if (p == NULL) {
      qError("%s failed to clone the freq param, code:%s", id, strerror(terrno));
      return terrno;
    }

    if (len >= tListLen(pSupp->freq)) {
      qError("%s invalid freq parameter: %s", id, p);
      code = TSDB_CODE_INVALID_PARA;
    } else {
      if ((len == 1) && (strncmp(pFreq, "d", 1) != 0) && (strncmp(pFreq, "h", 1) != 0) &&
          (strncmp(pFreq, "m", 1) != 0) && (strncmp(pFreq, "s", 1) != 0)) {
        code = TSDB_CODE_INVALID_PARA;
        qError("%s invalid freq parameter: %s", id, p);
      } else if ((len == 2) && (strncmp(pFreq, "ms", 2) != 0) && (strncmp(pFreq, "us", 2) != 0)) {
        code = TSDB_CODE_INVALID_PARA;
        qError("%s invalid freq parameter: %s", id, p);
      } else if (len > 2) {
        code = TSDB_CODE_INVALID_PARA;
        qError("%s invalid freq parameter: %s", id, p);
      }
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

void parseRadius(SCorrelationSupp* pSupp, SHashObj* pHashMap, const char* id) {
  char* pRadius = taosHashGet(pHashMap, RADIUS_STR, strlen(RADIUS_STR));
  if (pRadius != NULL) {
    pSupp->radius = *(int32_t*)pRadius;
    qDebug("%s dtw search radius:%d", id, pSupp->radius);
  } else {
    qDebug("%s not specify search radius, default: %d", id, pSupp->radius);
  }
}

void parseLag(SCorrelationSupp* pSupp, SHashObj* pHashMap, const char* id) {
  char* pLagStart = taosHashGet(pHashMap, LAGSTART_STR, strlen(LAGSTART_STR));
  if (pLagStart != NULL) {
    pSupp->lagStart = *(int32_t*)pLagStart;
    qDebug("%s tlcc lag start:%d", id, pSupp->lagStart);
  } else {
    qDebug("%s not specify tlcc lag start, default: %d", id, pSupp->lagStart);
  }

  char* pLagEnd = taosHashGet(pHashMap, LAGEND_STR, strlen(LAGEND_STR));
  if (pLagEnd != NULL) {
    pSupp->lagEnd = *(int32_t*)pLagEnd;
    qDebug("%s tlcc lag end:%d", id, pSupp->lagEnd);
  } else {
    qDebug("%s not specify tlcc lag end, default: %d", id, pSupp->lagEnd);
  }
}

int32_t estResultRowsAfterImputation(int32_t rows, int64_t skey, int64_t ekey, int32_t prec, const char* pFreq, const char* id) {
  int64_t range = ekey - skey;
  double  factor = 0;
  if (prec == TSDB_TIME_PRECISION_MILLI) {
    if (strcmp(pFreq, "h") == 0) {
      factor = 0.001 * 1/3600;
    } else if (strcmp(pFreq, "m") == 0) {
      factor = 0.001 * 1/60;
    } else if (strcmp(pFreq, "s") == 0) {
      factor = 0.001;
    } else if (strcmp(pFreq, "ms") == 0) {
      factor = 1;
    } else if (strcmp(pFreq, "us") == 0) {
      factor *= 1000;
    } else if (strcmp(pFreq, "ns") == 0) {
      factor *= 1000000;
    }

    int64_t num = range * factor - rows;
    if (num > ANALY_MAX_IMPUT_ROWS) {
      qError("%s too many rows to imputation, est:%"PRId64, id, num);
      return TSDB_CODE_INVALID_PARA;
    }
  } else if (prec == TSDB_TIME_PRECISION_MICRO) {
    if (strcmp(pFreq, "h") == 0) {
      factor = 0.000001 * 1/3600;
    } else if (strcmp(pFreq, "m") == 0) {
      factor = 0.000001 * 1/60;
    } else if (strcmp(pFreq, "s") == 0) {
      factor = 0.000001;
    } else if (strcmp(pFreq, "ms") == 0) {
      factor = 1000;
    } else if (strcmp(pFreq, "us") == 0) {
      factor *= 1;
    } else if (strcmp(pFreq, "ns") == 0) {
      factor *= 1000;
    }

    int64_t num = range * factor - rows;
    if (num > ANALY_MAX_IMPUT_ROWS) {
      qError("%s too many rows to imputation, est:%"PRId64, id, num);
      return TSDB_CODE_INVALID_PARA;
    }
  } else if (prec == TSDB_TIME_PRECISION_NANO) {
    if (strcmp(pFreq, "h") == 0) {
      factor = 0.000000001 * 1/3600;
    } else if (strcmp(pFreq, "m") == 0) {
      factor = 0.000000001 * 1/60;
    } else if (strcmp(pFreq, "s") == 0) {
      factor = 0.000000001;
    } else if (strcmp(pFreq, "ms") == 0) {
      factor = 1000000;
    } else if (strcmp(pFreq, "us") == 0) {
      factor *= 1000;
    } else if (strcmp(pFreq, "ns") == 0) {
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

int32_t doParseOption(SAnalysisOperatorInfo* pInfo, const char* id) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SHashObj* pHashMap = NULL;

  code = taosAnalyGetOpts(pInfo->options, &pHashMap);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t type = 0;
  if (pInfo->analysisType == FUNCTION_TYPE_IMPUTATION) {
    type = ANALY_ALGO_TYPE_IMPUTATION;
  } else if (pInfo->analysisType == FUNCTION_TYPE_DTW || pInfo->analysisType == FUNCTION_TYPE_DTW_PATH ||
             pInfo->analysisType == FUNCTION_TYPE_TLCC) {
    type = ANALY_ALGO_TYPE_CORREL;
  }

  code = taosAnalysisParseAlgo(pInfo->options, pInfo->algoName, pInfo->algoUrl, type, tListLen(pInfo->algoUrl),
                               pHashMap, id);
  TSDB_CHECK_CODE(code, lino, _end);

  // extract the timeout parameter
  SBaseSupp* pSupp =
      (pInfo->analysisType == FUNCTION_TYPE_IMPUTATION) ? &pInfo->imputatSup.base : &pInfo->corrSupp.base;
  pSupp->timeout = taosAnalysisParseTimout(pHashMap, id);
  pSupp->wncheck = taosAnalysisParseWncheck(pHashMap, id);

  if (pInfo->analysisType == FUNCTION_TYPE_IMPUTATION) {
    // extract data freq
    code = parseFreq(&pInfo->imputatSup, pHashMap, id);
  } else if (pInfo->analysisType == FUNCTION_TYPE_DTW || pInfo->analysisType == FUNCTION_TYPE_DTW_PATH) {
    parseRadius(&pInfo->corrSupp, pHashMap, id);
  } else if (pInfo->analysisType == FUNCTION_TYPE_TLCC) {
    parseLag(&pInfo->corrSupp, pHashMap, id);
  }

_end:
  taosHashCleanup(pHashMap);
  return code;
}

static int32_t doCreateBuf(SAnalysisOperatorInfo* pInfo, const char* pId) {
  SBaseSupp* pSupp =
      (pInfo->analysisType == FUNCTION_TYPE_IMPUTATION) ? &pInfo->imputatSup.base : &pInfo->corrSupp.base;

  SAnalyticBuf* pBuf = &pSupp->analyBuf;
  int64_t       ts = taosGetTimestampNs();
  int32_t       index = 0;

  pBuf->bufType = ANALYTICS_BUF_TYPE_JSON_COL;
  snprintf(pBuf->fileName, sizeof(pBuf->fileName), "%s/tdengine-analysis-%p-%" PRId64, tsTempDir, pInfo, ts);

  int32_t code = tsosAnalyBufOpen(pBuf, pSupp->numOfCols, pId);
  if (code != 0) goto _OVER;

  if (pInfo->analysisType == FUNCTION_TYPE_IMPUTATION) {
    code = taosAnalyBufWriteColMeta(pBuf, index++, TSDB_DATA_TYPE_TIMESTAMP, "ts");
    if (code != 0) goto _OVER;

    code = taosAnalyBufWriteColMeta(pBuf, index++, pInfo->imputatSup.targetType, "val");
    if (code != 0) goto _OVER;
  } else if (pInfo->analysisType == FUNCTION_TYPE_DTW || pInfo->analysisType == FUNCTION_TYPE_DTW_PATH || pInfo->analysisType == FUNCTION_TYPE_TLCC) {
    code = taosAnalyBufWriteColMeta(pBuf, index++, pInfo->corrSupp.targetSlot1Type, "val");
    if (code != 0) goto _OVER;

    code = taosAnalyBufWriteColMeta(pBuf, index++, pInfo->corrSupp.targetSlot2Type, "val1");
    if (code != 0) goto _OVER;
  }

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

int32_t createGenericAnalysisOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo,
                                     SOperatorInfo** pOptrInfo) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}
void analysisDestroyOperatorInfo(void* param) {}

#endif
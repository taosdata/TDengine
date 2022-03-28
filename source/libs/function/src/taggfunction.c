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

#include "os.h"
#include "taosdef.h"
#include "tmsg.h"
#include "thash.h"
#include "ttypes.h"

#include "function.h"
#include "taggfunction.h"
#include "tfill.h"
#include "thistogram.h"
#include "ttszip.h"
#include "tpercentile.h"
#include "tbuffer.h"
#include "tcompression.h"
//#include "queryLog.h"
#include "tdatablock.h"
#include "tudf.h"

#define GET_INPUT_DATA_LIST(x) ((char *)((x)->pInput))
#define GET_INPUT_DATA(x, y) ((char*) colDataGetData((x)->pInput, (y)))

#define GET_TS_LIST(x)    ((TSKEY*)((x)->ptsList))
#define GET_TS_DATA(x, y) (GET_TS_LIST(x)[(y)])

#define GET_TRUE_DATA_TYPE()                          \
  int32_t type = 0;                                   \
  if (pCtx->currentStage == MERGE_STAGE) {            \
    type = pCtx->resDataInfo.type;                          \
    assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY); \
  } else {                                            \
    type = pCtx->inputType;                           \
  }

#define SET_VAL(ctx, numOfElem, res)     \
  do {                                   \
    if ((numOfElem) <= 0) {              \
      break;                             \
    }                                    \
    GET_RES_INFO(ctx)->numOfRes = (res); \
  } while (0)

#define INC_INIT_VAL(ctx, res) (GET_RES_INFO(ctx)->numOfRes += (res));

#define DO_UPDATE_TAG_COLUMNS(ctx, ts)                             \
  do {                                                             \
    for (int32_t _i = 0; _i < (ctx)->tagInfo.numOfTagCols; ++_i) { \
      SqlFunctionCtx *__ctx = (ctx)->tagInfo.pTagCtxList[_i];      \
      if (__ctx->functionId == FUNCTION_TS_DUMMY) {                \
        __ctx->tag.i = (ts);                                       \
        __ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;                  \
      }                                                            \
      aggFunc[FUNCTION_TAG].addInput(__ctx);                       \
    }                                                              \
  } while (0)

#define DO_UPDATE_TAG_COLUMNS_WITHOUT_TS(ctx)                      \
  do {                                                             \
    for (int32_t _i = 0; _i < (ctx)->tagInfo.numOfTagCols; ++_i) { \
      SqlFunctionCtx *__ctx = (ctx)->tagInfo.pTagCtxList[_i];      \
      aggFunc[FUNCTION_TAG].addInput(__ctx);                       \
    }                                                              \
  } while (0);

void noop1(SqlFunctionCtx *UNUSED_PARAM(pCtx)) {}

void doFinalizer(SqlFunctionCtx *pCtx) { cleanupResultRowEntry(GET_RES_INFO(pCtx)); }

typedef struct tValuePair {
  SVariant v;
  int64_t  timestamp;
  char *   pTags;  // the corresponding tags of each record in the final result
} tValuePair;

typedef struct SSpreadInfo {
  double min;
  double max;
  int8_t hasResult;
} SSpreadInfo;

typedef struct SSumInfo {
  union {
    int64_t  isum;
    uint64_t usum;
    double   dsum;
  };
  int8_t hasResult;
} SSumInfo;

// the attribute of hasResult is not needed since the num attribute would server as this purpose
typedef struct SAvgInfo {
  double  sum;
  int64_t num;
} SAvgInfo;

typedef struct SStddevInfo {
  double  avg;
  int64_t num;
  double  res;
  int8_t  stage;
} SStddevInfo;

typedef struct SStddevdstInfo {
  int64_t num;
  double  res;
} SStddevdstInfo;

typedef struct SFirstLastInfo {
  int8_t hasResult;
  TSKEY  ts;
} SFirstLastInfo;

typedef struct SFirstLastInfo SLastrowInfo;
typedef struct SPercentileInfo {
  tMemBucket *pMemBucket;
  int32_t     stage;
  double      minval;
  double      maxval;
  int64_t     numOfElems;
} SPercentileInfo;

typedef struct STopBotInfo {
  int32_t      num;
  tValuePair **res;
} STopBotInfo;

// leastsquares do not apply to super table
typedef struct SLeastsquaresInfo {
  double  mat[2][3];
  double  startVal;
  int64_t num;
} SLeastsquaresInfo;

typedef struct SAPercentileInfo {
  SHistogramInfo *pHisto;
} SAPercentileInfo;

typedef struct STSCompInfo {
  STSBuf *pTSBuf;
} STSCompInfo;

typedef struct SRateInfo {
  double  correctionValue;
  double  firstValue;
  TSKEY   firstKey;
  double  lastValue;
  TSKEY   lastKey;
  int8_t  hasResult;  // flag to denote has value
  bool    isIRate;    // true for IRate functions, false for Rate functions
} SRateInfo;

typedef struct SDerivInfo {
  double   prevValue;     // previous value
  TSKEY    prevTs;        // previous timestamp
  bool     ignoreNegative;// ignore the negative value
  int64_t  tsWindow;      // time window for derivative
  bool     valueSet;      // the value has been set already
} SDerivInfo;

typedef struct SResPair {
  TSKEY  key;
  double avg;
} SResPair;

#define TSDB_BLOCK_DIST_STEP_ROWS 16

typedef struct STableBlockDist {
  uint16_t  rowSize;
  uint16_t  numOfFiles;
  uint32_t  numOfTables;
  uint64_t  totalSize;
  uint64_t  totalRows;
  int32_t   maxRows;
  int32_t   minRows;
  int32_t   firstSeekTimeUs;
  uint32_t  numOfRowsInMemTable;
  uint32_t  numOfSmallBlocks;
  SArray   *dataBlockInfos;
} STableBlockDist;

typedef struct SFileBlockInfo {
  int32_t numBlocksOfStep;
} SFileBlockInfo;

void cleanupResultRowEntry(struct SResultRowEntryInfo* pCell) {
  pCell->initialized = false;
}

int32_t getNumOfResult(SqlFunctionCtx* pCtx, int32_t num, SSDataBlock* pResBlock) {
  int32_t maxRows = 0;

  for (int32_t j = 0; j < num; ++j) {
#if 0
    int32_t id = pCtx[j].functionId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (id == FUNCTION_TS || id == FUNCTION_TAG || id == FUNCTION_TAGPRJ) {
      continue;
    }
#endif
    SResultRowEntryInfo *pResInfo = GET_RES_INFO(&pCtx[j]);
    if (pResInfo != NULL && maxRows < pResInfo->numOfRes) {
      maxRows = pResInfo->numOfRes;
    }
  }

  assert(maxRows >= 0);

  blockDataEnsureCapacity(pResBlock, maxRows);
  for(int32_t i = 0; i < num; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pResBlock->pDataBlock, i);

    SResultRowEntryInfo *pResInfo = GET_RES_INFO(&pCtx[i]);
    if (!pResInfo->hasResult) {
      for(int32_t j = 0; j < pResInfo->numOfRes; ++j) {
        colDataAppend(pCol, j, NULL, true);  // TODO add set null data api
      }
    } else {
      for (int32_t j = 0; j < pResInfo->numOfRes; ++j) {
        colDataAppend(pCol, j, GET_ROWCELL_INTERBUF(pResInfo), false);
      }
    }
  }

  pResBlock->info.rows = maxRows;
  return maxRows;
}

void resetResultRowEntryResult(SqlFunctionCtx* pCtx, int32_t num) {
  for (int32_t j = 0; j < num; ++j) {
    SResultRowEntryInfo *pResInfo = GET_RES_INFO(&pCtx[j]);
    pResInfo->numOfRes = 0;
  }
}

bool isRowEntryCompleted(struct SResultRowEntryInfo* pEntry) {
  assert(pEntry != NULL);
  return pEntry->complete;
}

bool isRowEntryInitialized(struct SResultRowEntryInfo* pEntry) {
  return pEntry->initialized;
}

int32_t getResultDataInfo(int32_t dataType, int32_t dataBytes, int32_t functionId, int32_t param, SResultDataInfo* pInfo, int16_t extLength,
    bool isSuperTable/*, SUdfInfo* pUdfInfo*/) {
  if (!isValidDataType(dataType)) {
//    qError("Illegal data type %d or data type length %d", dataType, dataBytes);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }


  if (functionId == FUNCTION_TS || functionId == FUNCTION_TS_DUMMY || functionId == FUNCTION_TAG_DUMMY ||
      functionId == FUNCTION_DIFF || functionId == FUNCTION_PRJ || functionId == FUNCTION_TAGPRJ ||
      functionId == FUNCTION_TAG || functionId == FUNCTION_INTERP) {
    pInfo->type = (int16_t)dataType;
    pInfo->bytes = (int16_t)dataBytes;

    if (functionId == FUNCTION_INTERP) {
      pInfo->interBufSize = sizeof(SInterpInfoDetail);
    } else {
      pInfo->interBufSize = 0;
    }

    return TSDB_CODE_SUCCESS;
  }
  
  // (uid, tid) + VGID + TAGSIZE + VARSTR_HEADER_SIZE
  if (functionId == FUNCTION_TID_TAG) { // todo use struct
    pInfo->type = TSDB_DATA_TYPE_BINARY;
    pInfo->bytes = (int16_t)(dataBytes + sizeof(int16_t) + sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t) + VARSTR_HEADER_SIZE);
    pInfo->interBufSize = 0;
    return TSDB_CODE_SUCCESS;
  }

  if (functionId == FUNCTION_BLKINFO) {
    pInfo->type = TSDB_DATA_TYPE_BINARY;
    pInfo->bytes = 16384;
    pInfo->interBufSize = 0;
    return TSDB_CODE_SUCCESS;
  }
  
  if (functionId == FUNCTION_COUNT) {
    pInfo->type = TSDB_DATA_TYPE_BIGINT;
    pInfo->bytes = sizeof(int64_t);
    pInfo->interBufSize = 0;
    return TSDB_CODE_SUCCESS;
  }
  
  if (functionId == FUNCTION_ARITHM) {
    pInfo->type = TSDB_DATA_TYPE_DOUBLE;
    pInfo->bytes = sizeof(double);
    pInfo->interBufSize = 0;
    return TSDB_CODE_SUCCESS;
  }
  
  if (functionId == FUNCTION_TS_COMP) {
    pInfo->type = TSDB_DATA_TYPE_BINARY;
    pInfo->bytes = 1;  // this results is compressed ts data, only one byte
    pInfo->interBufSize = POINTER_BYTES;
    return TSDB_CODE_SUCCESS;
  }

  if (functionId == FUNCTION_DERIVATIVE) {
    pInfo->type = TSDB_DATA_TYPE_DOUBLE;
    pInfo->bytes = sizeof(double);  // this results is compressed ts data, only one byte
    pInfo->interBufSize = sizeof(SDerivInfo);
    return TSDB_CODE_SUCCESS;
  }

  if (isSuperTable) {
//    if (functionId < 0) {
//      if (pUdfInfo->bufSize > 0) {
//        pInfo->type = TSDB_DATA_TYPE_BINARY;
//        pInfo->bytes = pUdfInfo->bufSize;
//        pInfo->interBufSize = pInfo->bytes;
//      } else {
//        pInfo->type = pUdfInfo->resType;
//        pInfo->bytes = pUdfInfo->resBytes;
//        pInfo->interBufSize = pInfo->bytes;
//      }
//
//      return TSDB_CODE_SUCCESS;
//    }

    if (functionId == FUNCTION_MIN || functionId == FUNCTION_MAX) {
      pInfo->type = TSDB_DATA_TYPE_BINARY;
      pInfo->bytes = (int16_t)(dataBytes + DATA_SET_FLAG_SIZE);
      pInfo->interBufSize = pInfo->bytes;
      
      return TSDB_CODE_SUCCESS;
    } else if (functionId == FUNCTION_SUM) {
      pInfo->type = TSDB_DATA_TYPE_BINARY;
      pInfo->bytes = sizeof(SSumInfo);
      pInfo->interBufSize = pInfo->bytes;
      
      return TSDB_CODE_SUCCESS;
    } else if (functionId == FUNCTION_AVG) {
      pInfo->type = TSDB_DATA_TYPE_BINARY;
      pInfo->bytes = sizeof(SAvgInfo);
      pInfo->interBufSize = pInfo->bytes;
      return TSDB_CODE_SUCCESS;
      
    } else if (functionId >= FUNCTION_RATE && functionId <= FUNCTION_IRATE) {
      pInfo->type = TSDB_DATA_TYPE_DOUBLE;
      pInfo->bytes = sizeof(SRateInfo);
      pInfo->interBufSize = sizeof(SRateInfo);
      return TSDB_CODE_SUCCESS;
    } else if (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM) {
      pInfo->type = TSDB_DATA_TYPE_BINARY;
      pInfo->bytes = (int16_t)(sizeof(STopBotInfo) + (sizeof(tValuePair) + POINTER_BYTES + extLength) * param);
      pInfo->interBufSize = pInfo->bytes;
      
      return TSDB_CODE_SUCCESS;
    } else if (functionId == FUNCTION_SPREAD) {
      pInfo->type = TSDB_DATA_TYPE_BINARY;
      pInfo->bytes = sizeof(SSpreadInfo);
      pInfo->interBufSize = pInfo->bytes;
      
      return TSDB_CODE_SUCCESS;
    } else if (functionId == FUNCTION_APERCT) {
      pInfo->type = TSDB_DATA_TYPE_BINARY;
      pInfo->bytes = sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1) + sizeof(SHistogramInfo) + sizeof(SAPercentileInfo);
      pInfo->interBufSize = pInfo->bytes;
      
      return TSDB_CODE_SUCCESS;
    } else if (functionId == FUNCTION_LAST_ROW) {
      pInfo->type = TSDB_DATA_TYPE_BINARY;
      pInfo->bytes = (int16_t)(sizeof(SLastrowInfo) + dataBytes);
      pInfo->interBufSize = pInfo->bytes;
      
      return TSDB_CODE_SUCCESS;
    } else if (functionId == FUNCTION_TWA) {
      pInfo->type = TSDB_DATA_TYPE_DOUBLE;
      pInfo->bytes = sizeof(STwaInfo);
      pInfo->interBufSize = pInfo->bytes;
      return TSDB_CODE_SUCCESS;
    }
  }

  if (functionId == FUNCTION_SUM) {
    if (IS_SIGNED_NUMERIC_TYPE(dataType)) {
      pInfo->type = TSDB_DATA_TYPE_BIGINT;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(dataType)) {
      pInfo->type = TSDB_DATA_TYPE_UBIGINT;
    } else {
      pInfo->type = TSDB_DATA_TYPE_DOUBLE;
    }
    
    pInfo->bytes = sizeof(int64_t);
    pInfo->interBufSize = sizeof(SSumInfo);
    return TSDB_CODE_SUCCESS;
  } else if (functionId == FUNCTION_APERCT) {
    pInfo->type = TSDB_DATA_TYPE_DOUBLE;
    pInfo->bytes = sizeof(double);
    pInfo->interBufSize =
        sizeof(SAPercentileInfo) + sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1);
    return TSDB_CODE_SUCCESS;
  } else if (functionId == FUNCTION_TWA) {
    pInfo->type = TSDB_DATA_TYPE_DOUBLE;
    pInfo->bytes = sizeof(double);
    pInfo->interBufSize = sizeof(STwaInfo);
    return TSDB_CODE_SUCCESS;
  }

//  if (functionId < 0) {
//    pInfo->type = pUdfInfo->resType;
//    pInfo->bytes = pUdfInfo->resBytes;
//
//    if (pUdfInfo->bufSize > 0) {
//      pInfo->interBufSize = pUdfInfo->bufSize;
//    } else {
//      pInfo->interBufSize = pInfo->bytes;
//    }
//
//    return TSDB_CODE_SUCCESS;
//  }

  if (functionId == FUNCTION_AVG) {
    pInfo->type = TSDB_DATA_TYPE_DOUBLE;
    pInfo->bytes = sizeof(double);
    pInfo->interBufSize = sizeof(SAvgInfo);
  } else if (functionId >= FUNCTION_RATE && functionId <= FUNCTION_IRATE) {
    pInfo->type = TSDB_DATA_TYPE_DOUBLE;
    pInfo->bytes = sizeof(double);
    pInfo->interBufSize = sizeof(SRateInfo);
  } else if (functionId == FUNCTION_STDDEV) {
    pInfo->type = TSDB_DATA_TYPE_DOUBLE;
    pInfo->bytes = sizeof(double);
    pInfo->interBufSize = sizeof(SStddevInfo);
  } else if (functionId == FUNCTION_MIN || functionId == FUNCTION_MAX) {
    pInfo->type = (int16_t)dataType;
    pInfo->bytes = (int16_t)dataBytes;
    pInfo->interBufSize = dataBytes + DATA_SET_FLAG_SIZE;
  } else if (functionId == FUNCTION_FIRST || functionId == FUNCTION_LAST) {
    pInfo->type = (int16_t)dataType;
    pInfo->bytes = (int16_t)dataBytes;
    pInfo->interBufSize = (int16_t)(dataBytes + sizeof(SFirstLastInfo));
  } else if (functionId == FUNCTION_SPREAD) {
    pInfo->type = (int16_t)TSDB_DATA_TYPE_DOUBLE;
    pInfo->bytes = sizeof(double);
    pInfo->interBufSize = sizeof(SSpreadInfo);
  } else if (functionId == FUNCTION_PERCT) {
    pInfo->type = (int16_t)TSDB_DATA_TYPE_DOUBLE;
    pInfo->bytes = (int16_t)sizeof(double);
    pInfo->interBufSize = (int16_t)sizeof(SPercentileInfo);
  } else if (functionId == FUNCTION_LEASTSQR) {
    pInfo->type = TSDB_DATA_TYPE_BINARY;
    pInfo->bytes = TMAX(AVG_FUNCTION_INTER_BUFFER_SIZE, sizeof(SLeastsquaresInfo));  // string
    pInfo->interBufSize = pInfo->bytes;
  } else if (functionId == FUNCTION_FIRST_DST || functionId == FUNCTION_LAST_DST) {
    pInfo->type = TSDB_DATA_TYPE_BINARY;
    pInfo->bytes = (int16_t)(dataBytes + sizeof(SFirstLastInfo));
    pInfo->interBufSize = pInfo->bytes;
  } else if (functionId == FUNCTION_TOP || functionId == FUNCTION_BOTTOM) {
    pInfo->type = (int16_t)dataType;
    pInfo->bytes = (int16_t)dataBytes;
    
    size_t size = sizeof(STopBotInfo) + (sizeof(tValuePair) + POINTER_BYTES + extLength) * param;
    
    // the output column may be larger than sizeof(STopBotInfo)
    pInfo->interBufSize = (int32_t)size;
  } else if (functionId == FUNCTION_LAST_ROW) {
    pInfo->type = (int16_t)dataType;
    pInfo->bytes = (int16_t)dataBytes;
    pInfo->interBufSize = dataBytes;
  } else if (functionId == FUNCTION_STDDEV_DST) {
    pInfo->type = TSDB_DATA_TYPE_BINARY;
    pInfo->bytes = sizeof(SStddevdstInfo);
    pInfo->interBufSize = (pInfo->bytes);

  } else {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
  
  return TSDB_CODE_SUCCESS;
}

static bool function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return false;
  }
  
  memset(pCtx->pOutput, 0, (size_t)pCtx->resDataInfo.bytes);
  initResultRowEntry(pResultInfo, pCtx->resDataInfo.interBufSize);
  return true;
}

/**
 * in handling the stable query, function_finalizer is called after the secondary
 * merge being completed, during the first merge procedure, which is executed at the
 * vnode side, the finalize will never be called.
 *
 * @param pCtx
 */
static void function_finalizer(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
//  if (pResInfo->hasResult != DATA_SET_FLAG) {  // TODO set the correct null value
//    setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
//  }
  
  doFinalizer(pCtx);
}

/*
 * count function does need the finalize, if data is missing, the default value, which is 0, is used
 * count function does not use the pCtx->interResBuf to keep the intermediate buffer
 */
static void count_function(SqlFunctionCtx *pCtx) {
  int32_t numOfElem = 0;
  
  /*
   * 1. column data missing (schema modified) causes pCtx->hasNull == true. pCtx->isAggSet == true;
   * 2. for general non-primary key columns, pCtx->hasNull may be true or false, pCtx->isAggSet == true;
   * 3. for primary key column, pCtx->hasNull always be false, pCtx->isAggSet == false;
   */
  if (pCtx->isAggSet) {
    numOfElem = pCtx->size - pCtx->agg.numOfNull;
  } else {
    if (pCtx->hasNull) {
      for (int32_t i = 0; i < pCtx->size; ++i) {
        char *val = GET_INPUT_DATA(pCtx, i);
        if (isNull(val, pCtx->inputType)) {
          continue;
        }
        
        numOfElem += 1;
      }
    } else {
      //when counting on the primary time stamp column and no statistics data is presented, use the size value directly.
      numOfElem = pCtx->size;
    }
  }
  
  if (numOfElem > 0) {
//    GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
  }
  
  *((int64_t *)pCtx->pOutput) += numOfElem;
  SET_VAL(pCtx, numOfElem, 1);
}

static void count_func_merge(SqlFunctionCtx *pCtx) {
  int64_t *pData = (int64_t *)GET_INPUT_DATA_LIST(pCtx);
  for (int32_t i = 0; i < pCtx->size; ++i) {
    *((int64_t *)pCtx->pOutput) += pData[i];
  }
  
  SET_VAL(pCtx, pCtx->size, 1);
}

/**
 * 1. If the column value for filter exists, we need to load the SFields, which serves
 *    as the pre-filter to decide if the actual data block is required or not.
 * 2. If it queries on the non-primary timestamp column, SFields is also required to get the not-null value.
 *
 * @param colId
 * @param filterCols
 * @return
 */
int32_t countRequired(SqlFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    return BLK_DATA_NO_NEEDED;
  } else {
    return BLK_DATA_STATIS_NEEDED;
  }
}

int32_t noDataRequired(SqlFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  return BLK_DATA_NO_NEEDED;
}
#define LIST_ADD_N_DOUBLE_FLOAT(x, ctx, p, t, numOfElem, tsdbType)              \
  do {                                                                \
    t *d = (t *)(p);                                               \
    for (int32_t i = 0; i < (ctx)->size; ++i) {                    \
      if (((ctx)->hasNull) && isNull((char *)&(d)[i], tsdbType)) { \
        continue;                                                  \
      };                                                           \
      SET_DOUBLE_VAL(&(x) , GET_DOUBLE_VAL(&(x)) + GET_FLOAT_VAL(&(d)[i]));                                               \
      (numOfElem)++;                                               \
    }                                                              \
  } while(0)
#define LIST_ADD_N_DOUBLE(x, ctx, p, t, numOfElem, tsdbType)              \
  do {                                                                \
    t *d = (t *)(p);                                               \
    for (int32_t i = 0; i < (ctx)->size; ++i) {                    \
      if (((ctx)->hasNull) && isNull((char *)&(d)[i], tsdbType)) { \
        continue;                                                  \
      };                                                           \
      SET_DOUBLE_VAL(&(x) , (x) + (d)[i]);                                               \
      (numOfElem)++;                                               \
    }                                                              \
  } while(0)

#define LIST_ADD_N(x, ctx, p, t, numOfElem, tsdbType)              \
  do {                                                                \
    t *d = (t *)(p);                                               \
    for (int32_t i = 0; i < (ctx)->size; ++i) {                    \
      if (((ctx)->hasNull) && isNull((char *)&(d)[i], tsdbType)) { \
        continue;                                                  \
      };                                                           \
      (x) += (d)[i];                                               \
      (numOfElem)++;                                               \
    }                                                              \
  } while(0)

#define UPDATE_DATA(ctx, left, right, num, sign, k) \
  do {                                              \
    if (((left) < (right)) ^ (sign)) {              \
      (left) = (right);                             \
      DO_UPDATE_TAG_COLUMNS(ctx, k);                \
      (num) += 1;                                   \
    }                                               \
  } while (0)

#define DUPATE_DATA_WITHOUT_TS(ctx, left, right, num, sign) \
  do {                                                      \
    if (((left) < (right)) ^ (sign)) {                      \
      (left) = (right);                                     \
      DO_UPDATE_TAG_COLUMNS_WITHOUT_TS(ctx);                \
      (num) += 1;                                           \
    }                                                       \
  } while (0)

#define LOOPCHECK_N(val, list, ctx, tsdbType, sign, num)          \
  for (int32_t i = 0; i < ((ctx)->size); ++i) {                   \
    if ((ctx)->hasNull && isNull((char *)&(list)[i], tsdbType)) { \
      continue;                                                   \
    }                                                             \
    TSKEY key = (ctx)->ptsList != NULL? GET_TS_DATA(ctx, i):0;    \
    UPDATE_DATA(ctx, val, (list)[i], num, sign, key);             \
  }

#define TYPED_LOOPCHECK_N(type, data, list, ctx, tsdbType, sign, notNullElems) \
  do {                                                                         \
    type *_data = (type *)data;                                                \
    type *_list = (type *)list;                                                \
    LOOPCHECK_N(*_data, _list, ctx, tsdbType, sign, notNullElems);             \
  } while (0)

static void do_sum(SqlFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  // Only the pre-computing information loaded and actual data does not loaded
  if (pCtx->isAggSet) {
    notNullElems = pCtx->size - pCtx->agg.numOfNull;
    assert(pCtx->size >= pCtx->agg.numOfNull);
    
    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      int64_t *retVal = (int64_t *)pCtx->pOutput;
      *retVal += pCtx->agg.sum;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      uint64_t *retVal = (uint64_t *)pCtx->pOutput;
      *retVal += (uint64_t)pCtx->agg.sum;
    } else if (IS_FLOAT_TYPE(pCtx->inputType)) {
      double *retVal = (double*) pCtx->pOutput;
      SET_DOUBLE_VAL(retVal, *retVal + GET_DOUBLE_VAL((const char*)&(pCtx->agg.sum)));
    }
  } else {  // computing based on the true data block
    void *pData = GET_INPUT_DATA_LIST(pCtx);
    notNullElems = 0;

    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      int64_t *retVal = (int64_t *)pCtx->pOutput;

      if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
        LIST_ADD_N(*retVal, pCtx, pData, int8_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
        LIST_ADD_N(*retVal, pCtx, pData, int16_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
        LIST_ADD_N(*retVal, pCtx, pData, int32_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
        LIST_ADD_N(*retVal, pCtx, pData, int64_t, notNullElems, pCtx->inputType);
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      uint64_t *retVal = (uint64_t *)pCtx->pOutput;

      if (pCtx->inputType == TSDB_DATA_TYPE_UTINYINT) {
        LIST_ADD_N(*retVal, pCtx, pData, uint8_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_USMALLINT) {
        LIST_ADD_N(*retVal, pCtx, pData, uint16_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_UINT) {
        LIST_ADD_N(*retVal, pCtx, pData, uint32_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_UBIGINT) {
        LIST_ADD_N(*retVal, pCtx, pData, uint64_t, notNullElems, pCtx->inputType);
      }
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      double *retVal = (double *)pCtx->pOutput;
      LIST_ADD_N_DOUBLE(*retVal, pCtx, pData, double, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      double *retVal = (double *)pCtx->pOutput;
      LIST_ADD_N_DOUBLE_FLOAT(*retVal, pCtx, pData, float, notNullElems, pCtx->inputType);
    }
  }
  
  // data in the check operation are all null, not output
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
//    GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
  }
}

static void sum_function(SqlFunctionCtx *pCtx) {
  do_sum(pCtx);
  
  // keep the result data in output buffer, not in the intermediate buffer
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
//  if (pResInfo->hasResult == DATA_SET_FLAG && pCtx->stableQuery) {
    // set the flag for super table query
    SSumInfo *pSum = (SSumInfo *)pCtx->pOutput;
    pSum->hasResult = DATA_SET_FLAG;
//  }
}

static void sum_func_merge(SqlFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  GET_TRUE_DATA_TYPE();
  assert(pCtx->stableQuery);

  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *    input = GET_INPUT_DATA(pCtx, i);
    SSumInfo *pInput = (SSumInfo *)input;
    if (pInput->hasResult != DATA_SET_FLAG) {
      continue;
    }

    notNullElems++;

    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      *(int64_t *)pCtx->pOutput += pInput->isum;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      *(uint64_t *) pCtx->pOutput += pInput->usum;
    } else {
      SET_DOUBLE_VAL((double *)pCtx->pOutput, *(double *)pCtx->pOutput + pInput->dsum);
    }
  }

  SET_VAL(pCtx, notNullElems, 1);
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  
  if (notNullElems > 0) {
    //pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static int32_t statisRequired(SqlFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  return BLK_DATA_STATIS_NEEDED;
}

static int32_t dataBlockRequired(SqlFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  return BLK_DATA_ALL_NEEDED;
}

// todo: if column in current data block are null, opt for this case
static int32_t firstFuncRequired(SqlFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  if (pCtx->order == TSDB_ORDER_DESC) {
    return BLK_DATA_NO_NEEDED;
  }
  
  // no result for first query, data block is required
  if (GET_RES_INFO(pCtx) == NULL || GET_RES_INFO(pCtx)->numOfRes <= 0) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    return BLK_DATA_NO_NEEDED;
  }
}

static int32_t lastFuncRequired(SqlFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  if (pCtx->order != pCtx->param[0].i) {
    return BLK_DATA_NO_NEEDED;
  }
  
  if (GET_RES_INFO(pCtx) == NULL || GET_RES_INFO(pCtx)->numOfRes <= 0) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    return BLK_DATA_NO_NEEDED;
  }
}

static int32_t firstDistFuncRequired(SqlFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  if (pCtx->order == TSDB_ORDER_DESC) {
    return BLK_DATA_NO_NEEDED;
  }

  // not initialized yet, it is the first block, load it.
  if (pCtx->pOutput == NULL) {
    return BLK_DATA_ALL_NEEDED;
  }

  // the pCtx should be set to current Ctx and output buffer before call this function. Otherwise, pCtx->pOutput is
  // the previous windowRes output buffer, not current unloaded block. In this case, the following filter is invalid
  SFirstLastInfo *pInfo = (SFirstLastInfo*) (pCtx->pOutput + pCtx->inputBytes);
  if (pInfo->hasResult != DATA_SET_FLAG) {
    return BLK_DATA_ALL_NEEDED;
  } else {  // data in current block is not earlier than current result
    return (pInfo->ts <= w->skey) ? BLK_DATA_NO_NEEDED : BLK_DATA_ALL_NEEDED;
  }
}

static int32_t lastDistFuncRequired(SqlFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  if (pCtx->order != pCtx->param[0].i) {
    return BLK_DATA_NO_NEEDED;
  }

  // not initialized yet, it is the first block, load it.
  if (pCtx->pOutput == NULL) {
    return BLK_DATA_ALL_NEEDED;
  }

  // the pCtx should be set to current Ctx and output buffer before call this function. Otherwise, pCtx->pOutput is
  // the previous windowRes output buffer, not current unloaded block. In this case, the following filter is invalid
  SFirstLastInfo *pInfo = (SFirstLastInfo*) (pCtx->pOutput + pCtx->inputBytes);
  if (pInfo->hasResult != DATA_SET_FLAG) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    return (pInfo->ts > w->ekey) ? BLK_DATA_NO_NEEDED : BLK_DATA_ALL_NEEDED;
  }
}

//////////////////////////////////////////////////////////////////////////////////////////////
/*
 * The intermediate result of average is kept in the interResultBuf.
 * For super table query, once the avg_function/avg_function_f is finished, copy the intermediate
 * result into output buffer.
 */
static void avg_function(SqlFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  // NOTE: keep the intermediate result into the interResultBuf
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  
  SAvgInfo *pAvgInfo = (SAvgInfo *)GET_ROWCELL_INTERBUF(pResInfo);
  double   *pVal = &pAvgInfo->sum;
  
  if (pCtx->isAggSet) { // Pre-aggregation
    notNullElems = pCtx->size - pCtx->agg.numOfNull;
    assert(notNullElems >= 0);
    
    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      *pVal += pCtx->agg.sum;
    }  else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      *pVal += (uint64_t) pCtx->agg.sum;
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE || pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      *pVal += GET_DOUBLE_VAL((const char *)&(pCtx->agg.sum));
    }
  } else {
    void *pData = GET_INPUT_DATA_LIST(pCtx);
    
    if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
      LIST_ADD_N(*pVal, pCtx, pData, int8_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
      LIST_ADD_N(*pVal, pCtx, pData, int16_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
      LIST_ADD_N(*pVal, pCtx, pData, int32_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
      LIST_ADD_N(*pVal, pCtx, pData, int64_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      LIST_ADD_N_DOUBLE(*pVal, pCtx, pData, double, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      LIST_ADD_N_DOUBLE_FLOAT(*pVal, pCtx, pData, float, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_UTINYINT) {
      LIST_ADD_N(*pVal, pCtx, pData, uint8_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_USMALLINT) {
      LIST_ADD_N(*pVal, pCtx, pData, uint16_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_UINT) {
      LIST_ADD_N(*pVal, pCtx, pData, uint32_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_UBIGINT) {
      LIST_ADD_N(*pVal, pCtx, pData, uint64_t, notNullElems, pCtx->inputType);
    }
  }
  
  if (!pCtx->hasNull) {
    assert(notNullElems == pCtx->size);
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  pAvgInfo->num += notNullElems;
  
  if (notNullElems > 0) {
    //pResInfo->hasResult = DATA_SET_FLAG;
  }
  
  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pCtx->stableQuery) {
    memcpy(pCtx->pOutput, GET_ROWCELL_INTERBUF(pResInfo), sizeof(SAvgInfo));
  }
}

static void avg_func_merge(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  
  double *sum = (double*) pCtx->pOutput;
  char   *input = GET_INPUT_DATA_LIST(pCtx);
  
  for (int32_t i = 0; i < pCtx->size; ++i, input += pCtx->inputBytes) {
    SAvgInfo *pInput = (SAvgInfo *)input;
    if (pInput->num == 0) {  // current input is null
      continue;
    }
    
    SET_DOUBLE_VAL(sum, *sum + pInput->sum);
    
    // keep the number of data into the temp buffer
    *(int64_t *)GET_ROWCELL_INTERBUF(pResInfo) += pInput->num;
  }
}

/*
 * the average value is calculated in finalize routine, since current routine does not know the exact number of points
 */
static void avg_finalizer(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  
  if (pCtx->currentStage == MERGE_STAGE) {
    assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
    
    if (GET_INT64_VAL(GET_ROWCELL_INTERBUF(pResInfo)) <= 0) {
      setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
      return;
    }

    SET_DOUBLE_VAL((double *)pCtx->pOutput,(*(double *)pCtx->pOutput) / *(int64_t *)GET_ROWCELL_INTERBUF(pResInfo));
  } else {  // this is the secondary merge, only in the secondary merge, the input type is TSDB_DATA_TYPE_BINARY
    assert(IS_NUMERIC_TYPE(pCtx->inputType));
    SAvgInfo *pAvgInfo = (SAvgInfo *)GET_ROWCELL_INTERBUF(pResInfo);
    
    if (pAvgInfo->num == 0) {  // all data are NULL or empty table
      setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
      return;
    }
    
    SET_DOUBLE_VAL((double *)pCtx->pOutput, pAvgInfo->sum / pAvgInfo->num);
  }
  
  // cannot set the numOfIteratedElems again since it is set during previous iteration
  GET_RES_INFO(pCtx)->numOfRes = 1;
  doFinalizer(pCtx);
}

/////////////////////////////////////////////////////////////////////////////////////////////

static bool min_func_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!function_setup(pCtx, pResultInfo)) {
    return false;  // not initialized since it has been initialized
  }
  
  GET_TRUE_DATA_TYPE();
  
  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t *)pCtx->pOutput) = INT8_MAX;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      *(uint8_t *) pCtx->pOutput = UINT8_MAX;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t *)pCtx->pOutput) = INT16_MAX;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      *((uint16_t *)pCtx->pOutput) = UINT16_MAX;
      break;
    case TSDB_DATA_TYPE_INT:
      *((int32_t *)pCtx->pOutput) = INT32_MAX;
      break;
    case TSDB_DATA_TYPE_UINT:
      *((uint32_t *)pCtx->pOutput) = UINT32_MAX;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t *)pCtx->pOutput) = INT64_MAX;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      *((uint64_t *)pCtx->pOutput) = UINT64_MAX;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float *)pCtx->pOutput) = FLT_MAX;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      SET_DOUBLE_VAL(((double *)pCtx->pOutput), DBL_MAX);
      break;
    default:
      assert(0);
//      qError("illegal data type:%d in min/max query", pCtx->inputType);
  }

  return true;
}

static bool max_func_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!function_setup(pCtx, pResultInfo)) {
    return false;  // not initialized since it has been initialized
  }
  
  GET_TRUE_DATA_TYPE();
  
  switch (type) {
    case TSDB_DATA_TYPE_INT:
      *((int32_t *)pCtx->pOutput) = INT32_MIN;
      break;
    case TSDB_DATA_TYPE_UINT:
      *((uint32_t *)pCtx->pOutput) = 0;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float *)pCtx->pOutput) = -FLT_MAX;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      SET_DOUBLE_VAL(((double *)pCtx->pOutput), -DBL_MAX);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t *)pCtx->pOutput) = INT64_MIN;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      *((uint64_t *)pCtx->pOutput) = 0;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t *)pCtx->pOutput) = INT16_MIN;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      *((uint16_t *)pCtx->pOutput) = 0;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t *)pCtx->pOutput) = INT8_MIN;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      *((uint8_t *)pCtx->pOutput) = 0;
      break;
    default:
      assert(0);
//      qError("illegal data type:%d in min/max query", pCtx->inputType);
  }
  
  return true;
}

/*
 * the output result of min/max function is the final output buffer, not the intermediate result buffer
 */
static int32_t minmax_merge_impl(SqlFunctionCtx *pCtx, int32_t bytes, char *output, bool isMin) {
  int32_t notNullElems = 0;
#if 0
  GET_TRUE_DATA_TYPE();
  assert(pCtx->stableQuery);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *input = GET_INPUT_DATA(pCtx, i);
    if (input[bytes] != DATA_SET_FLAG) {
      continue;
    }
    
    switch (type) {
      case TSDB_DATA_TYPE_TINYINT: {
        int8_t v = GET_INT8_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(int8_t *)output, v, notNullElems, isMin);
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t v = GET_INT16_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(int16_t *)output, v, notNullElems, isMin);
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        int32_t v = GET_INT32_VAL(input);
        if ((*(int32_t *)output < v) ^ isMin) {
          *(int32_t *)output = v;
          
          for (int32_t j = 0; j < pCtx->tagInfo.numOfTagCols; ++j) {
            SqlFunctionCtx *__ctx = pCtx->tagInfo.pTagCtxList[j];
            aggFunc[FUNCTION_TAG].addInput(__ctx);
          }
          
          notNullElems++;
        }
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        float v = GET_FLOAT_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(float *)output, v, notNullElems, isMin);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        double v = GET_DOUBLE_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(double *)output, v, notNullElems, isMin);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        int64_t v = GET_INT64_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(int64_t *)output, v, notNullElems, isMin);
        break;
      }

      case TSDB_DATA_TYPE_UTINYINT: {
        uint8_t v = GET_UINT8_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(uint8_t *)output, v, notNullElems, isMin);
        break;
      }

      case TSDB_DATA_TYPE_USMALLINT: {
        uint16_t v = GET_UINT16_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(uint16_t *)output, v, notNullElems, isMin);
        break;
      }

      case TSDB_DATA_TYPE_UINT: {
        uint32_t v = GET_UINT32_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(uint32_t *)output, v, notNullElems, isMin);
        break;
      }

      case TSDB_DATA_TYPE_UBIGINT: {
        uint64_t v = GET_UINT64_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(uint64_t *)output, v, notNullElems, isMin);
        break;
      }

      default:
        break;
    }
  }
#endif

  return notNullElems;
}

static void min_func_merge(SqlFunctionCtx *pCtx) {
  int32_t notNullElems = minmax_merge_impl(pCtx, pCtx->resDataInfo.bytes, pCtx->pOutput, 1);
  
  SET_VAL(pCtx, notNullElems, 1);
  
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  if (notNullElems > 0) {
    //pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void max_func_merge(SqlFunctionCtx *pCtx) {
  int32_t numOfElem = minmax_merge_impl(pCtx, pCtx->resDataInfo.bytes, pCtx->pOutput, 0);
  
  SET_VAL(pCtx, numOfElem, 1);
  
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  if (numOfElem > 0) {
    //pResInfo->hasResult = DATA_SET_FLAG;
  }
}

#define LOOP_STDDEV_IMPL(type, r, d, ctx, delta, _type, num)          \
  for (int32_t i = 0; i < (ctx)->size; ++i) {                         \
    if ((ctx)->hasNull && isNull((char *)&((type *)d)[i], (_type))) { \
      continue;                                                       \
    }                                                                 \
    (num) += 1;                                                       \
    (r) += TPOW2(((type *)d)[i] - (delta));                            \
  }

static void stddev_function(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  SStddevInfo *pStd = GET_ROWCELL_INTERBUF(pResInfo);

  if (pCtx->currentStage == REPEAT_SCAN && pStd->stage == 0) {
    pStd->stage++;
    avg_finalizer(pCtx);

    pResInfo->initialized = true; // set it initialized to avoid re-initialization

    // save average value into tmpBuf, for second stage scan
    SAvgInfo *pAvg = GET_ROWCELL_INTERBUF(pResInfo);

    pStd->avg = GET_DOUBLE_VAL(pCtx->pOutput);
    assert((isnan(pAvg->sum) && pAvg->num == 0) || (pStd->num == pAvg->num && pStd->avg == pAvg->sum));
  }
  
  if (pStd->stage == 0) {
    // the first stage is to calculate average value
    avg_function(pCtx);
  } else if (pStd->num > 0) {
    // the second stage to calculate standard deviation
    // if pStd->num == 0, there are no numbers in the first round check. No need to do the second round
    double *retVal = &pStd->res;
    double  avg = pStd->avg;
    
    void *pData = GET_INPUT_DATA_LIST(pCtx);
    int32_t num = 0;

    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_INT: {
        for (int32_t i = 0; i < pCtx->size; ++i) {
          if (pCtx->hasNull && isNull((const char*) (&((int32_t *)pData)[i]), pCtx->inputType)) {
            continue;
          }
          num += 1;
          *retVal += TPOW2(((int32_t *)pData)[i] - avg);
        }
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        LOOP_STDDEV_IMPL(float, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        LOOP_STDDEV_IMPL(double, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        LOOP_STDDEV_IMPL(int64_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        LOOP_STDDEV_IMPL(int16_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_TINYINT: {
        LOOP_STDDEV_IMPL(int8_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_UBIGINT: {
        LOOP_STDDEV_IMPL(uint64_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT: {
        LOOP_STDDEV_IMPL(uint16_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT: {
        LOOP_STDDEV_IMPL(uint8_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_UINT: {
        LOOP_STDDEV_IMPL(uint32_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      default:
        assert(0);
//        qError("stddev function not support data type:%d", pCtx->inputType);
    }
    
    SET_VAL(pCtx, 1, 1);
  }
}

static void stddev_finalizer(SqlFunctionCtx *pCtx) {
  SStddevInfo *pStd = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  
  if (pStd->num <= 0) {
    setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
  } else {
    double *retValue = (double *)pCtx->pOutput;
    SET_DOUBLE_VAL(retValue, sqrt(pStd->res / pStd->num));
    SET_VAL(pCtx, 1, 1);
  }
  
  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////////
int32_t tsCompare(const void* p1, const void* p2) {
  TSKEY k = *(TSKEY*)p1;
  SResPair* pair = (SResPair*)p2;

  if (k == pair->key) {
    return 0;
  } else {
    return k < pair->key? -1:1;
  }
}

static void stddev_dst_function(SqlFunctionCtx *pCtx) {
  SStddevdstInfo *pStd = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  // the second stage to calculate standard deviation
  double *retVal = &pStd->res;

  // all data are null, no need to proceed
  SArray* resList = (SArray*) pCtx->param[0].pz;
  if (resList == NULL) {
    return;
  }

  // find the correct group average results according to the tag value
  int32_t len = (int32_t) taosArrayGetSize(resList);
  assert(len > 0);

  double avg = 0;
  if (len == 1) {
    SResPair* p = taosArrayGet(resList, 0);
    avg = p->avg;
  } else {  // todo opt performance by using iterator since the timestamp lsit is matched with the output result
    SResPair* p = bsearch(&pCtx->startTs, resList->pData, len, sizeof(SResPair), tsCompare);
    if (p == NULL) {
      return;
    }

    avg = p->avg;
  }

  void *pData = GET_INPUT_DATA_LIST(pCtx);
  int32_t num = 0;

  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      for (int32_t i = 0; i < pCtx->size; ++i) {
        if (pCtx->hasNull && isNull((const char*) (&((int32_t *)pData)[i]), pCtx->inputType)) {
          continue;
        }
        num += 1;
        *retVal += TPOW2(((int32_t *)pData)[i] - avg);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      LOOP_STDDEV_IMPL(float, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      LOOP_STDDEV_IMPL(double, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      LOOP_STDDEV_IMPL(int8_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      LOOP_STDDEV_IMPL(int8_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      LOOP_STDDEV_IMPL(int16_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      LOOP_STDDEV_IMPL(uint16_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      LOOP_STDDEV_IMPL(uint32_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      LOOP_STDDEV_IMPL(int64_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      LOOP_STDDEV_IMPL(uint64_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    default:
      assert(0);
//      qError("stddev function not support data type:%d", pCtx->inputType);
  }

  pStd->num += num;
  SET_VAL(pCtx, num, 1);

  // copy to the final output buffer for super table
  memcpy(pCtx->pOutput, GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx)), sizeof(SAvgInfo));
}

static void stddev_dst_merge(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  SStddevdstInfo* pRes = GET_ROWCELL_INTERBUF(pResInfo);

  char   *input = GET_INPUT_DATA_LIST(pCtx);

  for (int32_t i = 0; i < pCtx->size; ++i, input += pCtx->inputBytes) {
    SStddevdstInfo *pInput = (SStddevdstInfo *)input;
    if (pInput->num == 0) {  // current input is null
      continue;
    }

    pRes->num += pInput->num;
    pRes->res += pInput->res;
  }
}

static void stddev_dst_finalizer(SqlFunctionCtx *pCtx) {
  SStddevdstInfo *pStd = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  if (pStd->num <= 0) {
    setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
  } else {
    double *retValue = (double *)pCtx->pOutput;
    SET_DOUBLE_VAL(retValue, sqrt(pStd->res / pStd->num));
    SET_VAL(pCtx, 1, 1);
  }

  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////////
static bool first_last_function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  
  // used to keep the timestamp for comparison
  pCtx->param[1].nType = 0;
  pCtx->param[1].i = 0;
  
  return true;
}

// todo opt for null block
static void first_function(SqlFunctionCtx *pCtx) {
  if (pCtx->order == TSDB_ORDER_DESC) {
    return;
  }
  
  int32_t notNullElems = 0;
  
  // handle the null value
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    memcpy(pCtx->pOutput, data, pCtx->inputBytes);
    if (pCtx->ptsList != NULL) {
      TSKEY k = GET_TS_DATA(pCtx, i);
//      DO_UPDATE_TAG_COLUMNS(pCtx, k);
    }

    SResultRowEntryInfo *pInfo = GET_RES_INFO(pCtx);
//    pInfo->hasResult = DATA_SET_FLAG;
    pInfo->complete = true;
    
    notNullElems++;
    break;
  }
  
  SET_VAL(pCtx, notNullElems, 1);
}

static void first_data_assign_impl(SqlFunctionCtx *pCtx, char *pData, int32_t index) {
  int64_t *timestamp = GET_TS_LIST(pCtx);
  
  SFirstLastInfo *pInfo = (SFirstLastInfo *)(pCtx->pOutput + pCtx->inputBytes);
  
  if (pInfo->hasResult != DATA_SET_FLAG || timestamp[index] < pInfo->ts) {
    memcpy(pCtx->pOutput, pData, pCtx->inputBytes);
    pInfo->hasResult = DATA_SET_FLAG;
    pInfo->ts = timestamp[index];
    
//    DO_UPDATE_TAG_COLUMNS(pCtx, pInfo->ts);
  }
}

/*
 * format of intermediate result: "timestamp,value" need to compare the timestamp in the first part (before the comma)
 * to decide if the value is earlier than current intermediate result
 */
static void first_dist_function(SqlFunctionCtx *pCtx) {
  /*
   * do not to check data in the following cases:
   * 1. data block that are not loaded
   * 2. scan data files in desc order
   */
  if (pCtx->order == TSDB_ORDER_DESC) {
    return;
  }
  
  int32_t notNullElems = 0;

  // find the first not null value
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    first_data_assign_impl(pCtx, data, i);
    
    SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
    //pResInfo->hasResult = DATA_SET_FLAG;
    
    notNullElems++;
    break;
  }
  
  SET_VAL(pCtx, notNullElems, 1);
}

static void first_dist_func_merge(SqlFunctionCtx *pCtx) {
  assert(pCtx->stableQuery);

  char *          pData = GET_INPUT_DATA_LIST(pCtx);
  SFirstLastInfo *pInput = (SFirstLastInfo*) (pData + pCtx->resDataInfo.bytes);
  if (pInput->hasResult != DATA_SET_FLAG) {
    return;
  }
  
  // The param[1] is used to keep the initial value of max ts value
  if (pCtx->param[1].nType != pCtx->resDataInfo.type || pCtx->param[1].i > pInput->ts) {
    memcpy(pCtx->pOutput, pData, pCtx->resDataInfo.bytes);
    pCtx->param[1].i = pInput->ts;
    pCtx->param[1].nType = pCtx->resDataInfo.type;
    
//    DO_UPDATE_TAG_COLUMNS(pCtx, pInput->ts);
  }
  
  SET_VAL(pCtx, 1, 1);
//  GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
}

//////////////////////////////////////////////////////////////////////////////////////////
/*
 * last function:
 * 1. since the last block may be all null value, so, we simply access the last block is not valid
 *    each block need to be checked.
 * 2. If numOfNull == pBlock->numOfBlocks, the whole block is empty. Otherwise, there is at
 *    least one data in this block that is not null.(TODO opt for this case)
 */
static void last_function(SqlFunctionCtx *pCtx) {
  if (pCtx->order != pCtx->param[0].i) {
    return;
  }

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  int32_t notNullElems = 0;
  if (pCtx->order == TSDB_ORDER_DESC) {

    for (int32_t i = pCtx->size - 1; i >= 0; --i) {
      char *data = GET_INPUT_DATA(pCtx, i);
      if (pCtx->hasNull && isNull(data, pCtx->inputType) && (!pCtx->requireNull)) {
        continue;
      }

      memcpy(pCtx->pOutput, data, pCtx->inputBytes);

      TSKEY ts = pCtx->ptsList ? GET_TS_DATA(pCtx, i) : 0;
//      DO_UPDATE_TAG_COLUMNS(pCtx, ts);

      //pResInfo->hasResult = DATA_SET_FLAG;
      pResInfo->complete = true;  // set query completed on this column
      notNullElems++;
      break;
    }
  } else {  // ascending order
    for (int32_t i = pCtx->size - 1; i >= 0; --i) {
      char *data = GET_INPUT_DATA(pCtx, i);
      if (pCtx->hasNull && isNull(data, pCtx->inputType) && (!pCtx->requireNull)) {
        continue;
      }

      TSKEY ts = pCtx->ptsList ? GET_TS_DATA(pCtx, i) : 0;

      char* buf = GET_ROWCELL_INTERBUF(pResInfo);
//      if (pResInfo->hasResult != DATA_SET_FLAG || (*(TSKEY*)buf) < ts) {
//        //pResInfo->hasResult = DATA_SET_FLAG;
//        memcpy(pCtx->pOutput, data, pCtx->inputBytes);
//
//        *(TSKEY*)buf = ts;
//        DO_UPDATE_TAG_COLUMNS(pCtx, ts);
//      }

      notNullElems++;
      break;
    }
  }

  SET_VAL(pCtx, notNullElems, 1);
}

static void last_data_assign_impl(SqlFunctionCtx *pCtx, char *pData, int32_t index) {
  int64_t *timestamp = GET_TS_LIST(pCtx);
  
  SFirstLastInfo *pInfo = (SFirstLastInfo *)(pCtx->pOutput + pCtx->inputBytes);
  
  if (pInfo->hasResult != DATA_SET_FLAG || pInfo->ts < timestamp[index]) {
#if defined(_DEBUG_VIEW)
    qDebug("assign index:%d, ts:%" PRId64 ", val:%d, ", index, timestamp[index], *(int32_t *)pData);
#endif
    
    memcpy(pCtx->pOutput, pData, pCtx->inputBytes);
    pInfo->hasResult = DATA_SET_FLAG;
    pInfo->ts = timestamp[index];
    
//    DO_UPDATE_TAG_COLUMNS(pCtx, pInfo->ts);
  }
}

static void last_dist_function(SqlFunctionCtx *pCtx) {
  /*
   * 1. for scan data is not the required order
   * 2. for data blocks that are not loaded, no need to check data
   */
  if (pCtx->order != pCtx->param[0].i) {
    return;
  }

  int32_t notNullElems = 0;
  for (int32_t i = pCtx->size - 1; i >= 0; --i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      if (!pCtx->requireNull) {
        continue; 
      }
    }
    
    last_data_assign_impl(pCtx, data, i);
    
    SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
    //pResInfo->hasResult = DATA_SET_FLAG;
    
    notNullElems++;
    break;
  }
  
  SET_VAL(pCtx, notNullElems, 1);
}

/*
 * in the secondary merge(local reduce), the output is limited by the
 * final output size, so the main difference between last_dist_func_merge and second_merge
 * is: the output data format in computing
 */
static void last_dist_func_merge(SqlFunctionCtx *pCtx) {
  char *pData = GET_INPUT_DATA_LIST(pCtx);
  
  SFirstLastInfo *pInput = (SFirstLastInfo*) (pData + pCtx->resDataInfo.bytes);
  if (pInput->hasResult != DATA_SET_FLAG) {
    return;
  }
  
  /*
   * param[1] used to keep the corresponding timestamp to decide if current result is
   * the true last result
   */
  if (pCtx->param[1].nType != pCtx->resDataInfo.type || pCtx->param[1].i < pInput->ts) {
    memcpy(pCtx->pOutput, pData, pCtx->resDataInfo.bytes);
    pCtx->param[1].i = pInput->ts;
    pCtx->param[1].nType = pCtx->resDataInfo.type;
    
//    DO_UPDATE_TAG_COLUMNS(pCtx, pInput->ts);
  }
  
  SET_VAL(pCtx, 1, 1);
//  GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
}

//////////////////////////////////////////////////////////////////////////////////
/*
 * NOTE: last_row does not use the interResultBuf to keep the result
 */
static void last_row_function(SqlFunctionCtx *pCtx) {
  assert(pCtx->size >= 1);
  char *pData = GET_INPUT_DATA_LIST(pCtx);

  // assign the last element in current data block
  assignVal(pCtx->pOutput, pData + (pCtx->size - 1) * pCtx->inputBytes, pCtx->inputBytes, pCtx->inputType);
  
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  //pResInfo->hasResult = DATA_SET_FLAG;
  
  // set the result to final result buffer in case of super table query
  if (pCtx->stableQuery) {
    SLastrowInfo *pInfo1 = (SLastrowInfo *)(pCtx->pOutput + pCtx->inputBytes);
    pInfo1->ts = GET_TS_DATA(pCtx, pCtx->size - 1);
    pInfo1->hasResult = DATA_SET_FLAG;
    
//    DO_UPDATE_TAG_COLUMNS(pCtx, pInfo1->ts);
  } else {
    TSKEY ts = GET_TS_DATA(pCtx, pCtx->size - 1);
//    DO_UPDATE_TAG_COLUMNS(pCtx, ts);
  }

  SET_VAL(pCtx, pCtx->size, 1);
}

static void last_row_finalizer(SqlFunctionCtx *pCtx) {
  // do nothing at the first stage
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
//  if (pResInfo->hasResult != DATA_SET_FLAG) {
//    setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
//    return;
//  }
  
  GET_RES_INFO(pCtx)->numOfRes = 1;
  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////
static void valuePairAssign(tValuePair *dst, int16_t type, const char *val, int64_t tsKey, char *pTags,
                            SSubsidiaryResInfo *pTagInfo, int16_t stage) {
  dst->v.nType = type;
  dst->v.i = *(int64_t *)val;
  dst->timestamp = tsKey;
  
  int32_t size = 0;
  if (stage == MERGE_STAGE) {
//    memcpy(dst->pTags, pTags, (size_t)pTagInfo->tagsLen);
  } else {  // the tags are dumped from the ctx tag fields
//    for (int32_t i = 0; i < pTagInfo->numOfTagCols; ++i) {
//      SqlFunctionCtx* ctx = pTagInfo->pTagCtxList[i];
//      if (ctx->functionId == FUNCTION_TS_DUMMY) {
//        ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;
//        ctx->tag.i = tsKey;
//      }
//
//      taosVariantDump(&ctx->tag, dst->pTags + size, ctx->tag.nType, true);
//      size += pTagInfo->pTagCtxList[i]->resDataInfo.bytes;
//    }
  }
}

#define VALUEPAIRASSIGN(dst, src, __l)                 \
  do {                                                 \
    (dst)->timestamp = (src)->timestamp;               \
    (dst)->v = (src)->v;                               \
    memcpy((dst)->pTags, (src)->pTags, (size_t)(__l)); \
  } while (0)

static int32_t topBotComparFn(const void *p1, const void *p2, const void *param)
{
  uint16_t     type = *(uint16_t *) param;
  tValuePair  *val1 = *(tValuePair **) p1;
  tValuePair  *val2 = *(tValuePair **) p2;

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    if (val1->v.i == val2->v.i) {
      return 0;
    }

    return (val1->v.i > val2->v.i) ? 1 : -1;
   } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
     if (val1->v.u == val2->v.u) {
       return 0;
     }

     return (val1->v.u > val2->v.u) ? 1 : -1;
  }

  if (val1->v.d == val2->v.d) {
    return 0;
  }

  return (val1->v.d > val2->v.d) ? 1 : -1;
}

static void topBotSwapFn(void *dst, void *src, const void *param)
{
  char         tag[32768];
  tValuePair   temp;
  uint16_t     tagLen = *(uint16_t *) param;
  tValuePair  *vdst = *(tValuePair **) dst;
  tValuePair  *vsrc = *(tValuePair **) src;

  memset(tag, 0, sizeof(tag));
  temp.pTags = tag;

  VALUEPAIRASSIGN(&temp, vdst, tagLen);
  VALUEPAIRASSIGN(vdst, vsrc, tagLen);
  VALUEPAIRASSIGN(vsrc, &temp, tagLen);
}

static void do_top_function_add(STopBotInfo *pInfo, int32_t maxLen, void *pData, int64_t ts, uint16_t type,
                                SSubsidiaryResInfo *pTagInfo, char *pTags, int16_t stage) {
  SVariant val = {0};
  taosVariantCreateFromBinary(&val, pData, tDataTypes[type].bytes, type);
  
  tValuePair **pList = pInfo->res;
  assert(pList != NULL);

  if (pInfo->num < maxLen) {
    valuePairAssign(pList[pInfo->num], type, (const char *)&val.i, ts, pTags, pTagInfo, stage);

//    taosheapsort((void *) pList, sizeof(tValuePair **), pInfo->num + 1, (const void *) &type, topBotComparFn, (const void *) &pTagInfo->tagsLen, topBotSwapFn, 0);
 
    pInfo->num++;
  } else {
    if ((IS_SIGNED_NUMERIC_TYPE(type) && val.i > pList[0]->v.i) ||
        (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u > pList[0]->v.u) ||
        (IS_FLOAT_TYPE(type) && val.d > pList[0]->v.d)) {
      valuePairAssign(pList[0], type, (const char *)&val.i, ts, pTags, pTagInfo, stage);
//      taosheapadjust((void *) pList, sizeof(tValuePair **), 0, maxLen - 1, (const void *) &type, topBotComparFn, (const void *) &pTagInfo->tagsLen, topBotSwapFn, 0);
    }
  }
}

static void do_bottom_function_add(STopBotInfo *pInfo, int32_t maxLen, void *pData, int64_t ts, uint16_t type,
                                   SSubsidiaryResInfo *pTagInfo, char *pTags, int16_t stage) {
  SVariant val = {0};
  taosVariantCreateFromBinary(&val, pData, tDataTypes[type].bytes, type);

  tValuePair **pList = pInfo->res;
  assert(pList != NULL);

  if (pInfo->num < maxLen) {
    valuePairAssign(pList[pInfo->num], type, (const char *)&val.i, ts, pTags, pTagInfo, stage);

//    taosheapsort((void *) pList, sizeof(tValuePair **), pInfo->num + 1, (const void *) &type, topBotComparFn, (const void *) &pTagInfo->tagsLen, topBotSwapFn, 1);

    pInfo->num++;
  } else {
    if ((IS_SIGNED_NUMERIC_TYPE(type) && val.i < pList[0]->v.i) ||
        (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u < pList[0]->v.u) ||
        (IS_FLOAT_TYPE(type) && val.d < pList[0]->v.d)) {
      valuePairAssign(pList[0], type, (const char *)&val.i, ts, pTags, pTagInfo, stage);
//      taosheapadjust((void *) pList, sizeof(tValuePair **), 0, maxLen - 1, (const void *) &type, topBotComparFn, (const void *) &pTagInfo->tagsLen, topBotSwapFn, 1);
    }
  }
}

static int32_t resAscComparFn(const void *pLeft, const void *pRight) {
  tValuePair *pLeftElem = *(tValuePair **)pLeft;
  tValuePair *pRightElem = *(tValuePair **)pRight;
  
  if (pLeftElem->timestamp == pRightElem->timestamp) {
    return 0;
  } else {
    return pLeftElem->timestamp > pRightElem->timestamp ? 1 : -1;
  }
}

static int32_t resDescComparFn(const void *pLeft, const void *pRight) { return -resAscComparFn(pLeft, pRight); }

static int32_t resDataAscComparFn(const void *pLeft, const void *pRight) {
  tValuePair *pLeftElem = *(tValuePair **)pLeft;
  tValuePair *pRightElem = *(tValuePair **)pRight;
  
  if (IS_FLOAT_TYPE(pLeftElem->v.nType)) {
    if (pLeftElem->v.d == pRightElem->v.d) {
      return 0;
    } else {
      return pLeftElem->v.d > pRightElem->v.d ? 1 : -1;
    }
  } else if (IS_SIGNED_NUMERIC_TYPE(pLeftElem->v.nType)){
    if (pLeftElem->v.i == pRightElem->v.i) {
      return 0;
    } else {
      return pLeftElem->v.i > pRightElem->v.i ? 1 : -1;
    }
  } else {
    if (pLeftElem->v.u == pRightElem->v.u) {
      return 0;
    } else {
      return pLeftElem->v.u > pRightElem->v.u ? 1 : -1;
    }
  }
}

static int32_t resDataDescComparFn(const void *pLeft, const void *pRight) { return -resDataAscComparFn(pLeft, pRight); }

static void copyTopBotRes(SqlFunctionCtx *pCtx, int32_t type) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  STopBotInfo *pRes = GET_ROWCELL_INTERBUF(pResInfo);
  
  tValuePair **tvp = pRes->res;
  
  int32_t step = QUERY_ASC_FORWARD_STEP;
  int32_t len = (int32_t)(GET_RES_INFO(pCtx)->numOfRes);
  
  switch (type) {
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_INT: {
      int32_t *output = (int32_t *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = (int32_t)tvp[i]->v.i;
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *output = (int64_t *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i]->v.i;
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *output = (double *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        SET_DOUBLE_VAL(output, tvp[i]->v.d);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *output = (float *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = (float)tvp[i]->v.d;
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *output = (int16_t *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = (int16_t)tvp[i]->v.i;
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *output = (int8_t *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = (int8_t)tvp[i]->v.i;
      }
      break;
    }
    default: {
//      qError("top/bottom function not support data type:%d", pCtx->inputType);
      return;
    }
  }
  
  // set the output timestamp of each record.
  TSKEY *output = pCtx->ptsOutputBuf;
  for (int32_t i = 0; i < len; ++i, output += step) {
    *output = tvp[i]->timestamp;
  }
  
  // set the corresponding tag data for each record
  // todo check malloc failure
//  char **pData = taosMemoryCalloc(pCtx->tagInfo.numOfTagCols, POINTER_BYTES);
//  for (int32_t i = 0; i < pCtx->tagInfo.numOfTagCols; ++i) {
//    pData[i] = pCtx->tagInfo.pTagCtxList[i]->pOutput;
//  }
  
//  for (int32_t i = 0; i < len; ++i, output += step) {
//    int16_t offset = 0;
//    for (int32_t j = 0; j < pCtx->tagInfo.numOfTagCols; ++j) {
//      memcpy(pData[j], tvp[i]->pTags + offset, (size_t)pCtx->tagInfo.pTagCtxList[j]->resDataInfo.bytes);
//      offset += pCtx->tagInfo.pTagCtxList[j]->resDataInfo.bytes;
//      pData[j] += pCtx->tagInfo.pTagCtxList[j]->resDataInfo.bytes;
//    }
//  }
  
//  taosMemoryFreeClear(pData);
}

/*
 * Parameters values:
 * 1. param[0]: maximum allowable results
 * 2. param[1]: order by type (time or value)
 * 3. param[2]: asc/desc order
 *
 * top/bottom use the intermediate result buffer to keep the intermediate result
 */
static STopBotInfo *getTopBotOutputInfo(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);

  // only the first_stage_merge is directly written data into final output buffer
  if (pCtx->stableQuery && pCtx->currentStage != MERGE_STAGE) {
    return (STopBotInfo*) pCtx->pOutput;
  } else { // during normal table query and super table at the secondary_stage, result is written to intermediate buffer
    return GET_ROWCELL_INTERBUF(pResInfo);
  }
}


/*
 * keep the intermediate results during scan data blocks in the format of:
 * +-----------------------------------+-------------one value pair-----------+------------next value pair-----------+
 * |-------------pointer area----------|----ts---+-----+-----n tags-----------|----ts---+-----+-----n tags-----------|
 * +..[Value Pointer1][Value Pointer2].|timestamp|value|tags1|tags2|....|tagsn|timestamp|value|tags1|tags2|....|tagsn+
 */
static void buildTopBotStruct(STopBotInfo *pTopBotInfo, SqlFunctionCtx *pCtx) {
  char *tmp = (char *)pTopBotInfo + sizeof(STopBotInfo);
  pTopBotInfo->res = (tValuePair**) tmp;
  tmp += POINTER_BYTES * pCtx->param[0].i;

//  size_t size = sizeof(tValuePair) + pCtx->tagInfo.tagsLen;

//  for (int32_t i = 0; i < pCtx->param[0].i; ++i) {
//    pTopBotInfo->res[i] = (tValuePair*) tmp;
//    pTopBotInfo->res[i]->pTags = tmp + sizeof(tValuePair);
//    tmp += size;
//  }
}

bool topbot_datablock_filter(SqlFunctionCtx *pCtx, const char *minval, const char *maxval) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  if (pResInfo == NULL) {
    return true;
  }

  STopBotInfo *pTopBotInfo = getTopBotOutputInfo(pCtx);
  
  // required number of results are not reached, continue load data block
  if (pTopBotInfo->num < pCtx->param[0].i) {
    return true;
  }
  
  if ((void *)pTopBotInfo->res[0] != (void *)((char *)pTopBotInfo + sizeof(STopBotInfo) + POINTER_BYTES * pCtx->param[0].i)) {
    buildTopBotStruct(pTopBotInfo, pCtx);
  }

  tValuePair **pRes = (tValuePair**) pTopBotInfo->res;
  
  if (pCtx->functionId == FUNCTION_TOP) {
    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_TINYINT:
        return GET_INT8_VAL(maxval) > pRes[0]->v.i;
      case TSDB_DATA_TYPE_SMALLINT:
        return GET_INT16_VAL(maxval) > pRes[0]->v.i;
      case TSDB_DATA_TYPE_INT:
        return GET_INT32_VAL(maxval) > pRes[0]->v.i;
      case TSDB_DATA_TYPE_BIGINT:
        return GET_INT64_VAL(maxval) > pRes[0]->v.i;
      case TSDB_DATA_TYPE_FLOAT:
        return GET_FLOAT_VAL(maxval) > pRes[0]->v.d;
      case TSDB_DATA_TYPE_DOUBLE:
        return GET_DOUBLE_VAL(maxval) > pRes[0]->v.d;
      default:
        return true;
    }
  } else {
    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_TINYINT:
        return GET_INT8_VAL(minval) < pRes[0]->v.i;
      case TSDB_DATA_TYPE_SMALLINT:
        return GET_INT16_VAL(minval) < pRes[0]->v.i;
      case TSDB_DATA_TYPE_INT:
        return GET_INT32_VAL(minval) < pRes[0]->v.i;
      case TSDB_DATA_TYPE_BIGINT:
        return GET_INT64_VAL(minval) < pRes[0]->v.i;
      case TSDB_DATA_TYPE_FLOAT:
        return GET_FLOAT_VAL(minval) < pRes[0]->v.d;
      case TSDB_DATA_TYPE_DOUBLE:
        return GET_DOUBLE_VAL(minval) < pRes[0]->v.d;
      default:
        return true;
    }
  }
}

static bool top_bottom_function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  
  STopBotInfo *pInfo = getTopBotOutputInfo(pCtx);
  buildTopBotStruct(pInfo, pCtx);
  return true;
}

static void top_function(SqlFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  STopBotInfo *pRes = getTopBotOutputInfo(pCtx);
  assert(pRes->num >= 0);

  if ((void *)pRes->res[0] != (void *)((char *)pRes + sizeof(STopBotInfo) + POINTER_BYTES * pCtx->param[0].i)) {
    buildTopBotStruct(pRes, pCtx);
  }
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    notNullElems++;

    // NOTE: Set the default timestamp if it is missing [todo refactor]
    TSKEY ts = (pCtx->ptsList != NULL)? GET_TS_DATA(pCtx, i):0;
//    do_top_function_add(pRes, (int32_t)pCtx->param[0].i, data, ts, pCtx->inputType, &pCtx->tagInfo, NULL, 0);
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }
  
  // treat the result as only one result
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
    //pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void top_func_merge(SqlFunctionCtx *pCtx) {
  STopBotInfo *pInput = (STopBotInfo *)GET_INPUT_DATA_LIST(pCtx);
  
  // construct the input data struct from binary data
  buildTopBotStruct(pInput, pCtx);
  
  STopBotInfo *pOutput = getTopBotOutputInfo(pCtx);
  
  // the intermediate result is binary, we only use the output data type
  for (int32_t i = 0; i < pInput->num; ++i) {
    int16_t type = (pCtx->resDataInfo.type == TSDB_DATA_TYPE_FLOAT)? TSDB_DATA_TYPE_DOUBLE:pCtx->resDataInfo.type;
//    do_top_function_add(pOutput, (int32_t)pCtx->param[0].i, &pInput->res[i]->v.i, pInput->res[i]->timestamp,
//                        type, &pCtx->tagInfo, pInput->res[i]->pTags, pCtx->currentStage);
  }
  
  SET_VAL(pCtx, pInput->num, pOutput->num);
  
  if (pOutput->num > 0) {
    SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
    //pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void bottom_function(SqlFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  STopBotInfo *pRes = getTopBotOutputInfo(pCtx);
  
  if ((void *)pRes->res[0] != (void *)((char *)pRes + sizeof(STopBotInfo) + POINTER_BYTES * pCtx->param[0].i)) {
    buildTopBotStruct(pRes, pCtx);
  }

  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }

    notNullElems++;
    // NOTE: Set the default timestamp if it is missing [todo refactor]
    TSKEY ts = (pCtx->ptsList != NULL)? GET_TS_DATA(pCtx, i):0;
//    do_bottom_function_add(pRes, (int32_t)pCtx->param[0].i, data, ts, pCtx->inputType, &pCtx->tagInfo, NULL, 0);
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }
  
  // treat the result as only one result
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
    //pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void bottom_func_merge(SqlFunctionCtx *pCtx) {
  STopBotInfo *pInput = (STopBotInfo *)GET_INPUT_DATA_LIST(pCtx);
  
  // construct the input data struct from binary data
  buildTopBotStruct(pInput, pCtx);
  
  STopBotInfo *pOutput = getTopBotOutputInfo(pCtx);
  
  // the intermediate result is binary, we only use the output data type
  for (int32_t i = 0; i < pInput->num; ++i) {
    int16_t type = (pCtx->resDataInfo.type == TSDB_DATA_TYPE_FLOAT) ? TSDB_DATA_TYPE_DOUBLE : pCtx->resDataInfo.type;
//    do_bottom_function_add(pOutput, (int32_t)pCtx->param[0].i, &pInput->res[i]->v.i, pInput->res[i]->timestamp, type,
//                           &pCtx->tagInfo, pInput->res[i]->pTags, pCtx->currentStage);
  }

  SET_VAL(pCtx, pInput->num, pOutput->num);
  
  if (pOutput->num > 0) {
    SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
    //pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void top_bottom_func_finalizer(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  
  // data in temporary list is less than the required number of results, not enough qualified number of results
  STopBotInfo *pRes = GET_ROWCELL_INTERBUF(pResInfo);
  if (pRes->num == 0) {  // no result
//    assert(pResInfo->hasResult != DATA_SET_FLAG);
    // TODO:
  }
  
  GET_RES_INFO(pCtx)->numOfRes = pRes->num;
  tValuePair **tvp = pRes->res;
  
  // user specify the order of output by sort the result according to timestamp
  if (pCtx->param[1].i == PRIMARYKEY_TIMESTAMP_COL_ID) {
    __compar_fn_t comparator = (pCtx->param[2].i == TSDB_ORDER_ASC) ? resAscComparFn : resDescComparFn;
    qsort(tvp, (size_t)pResInfo->numOfRes, POINTER_BYTES, comparator);
  } else /*if (pCtx->param[1].i > PRIMARYKEY_TIMESTAMP_COL_ID)*/ {
    __compar_fn_t comparator = (pCtx->param[2].i == TSDB_ORDER_ASC) ? resDataAscComparFn : resDataDescComparFn;
    qsort(tvp, (size_t)pResInfo->numOfRes, POINTER_BYTES, comparator);
  }
  
  GET_TRUE_DATA_TYPE();
  copyTopBotRes(pCtx, type);
  
  doFinalizer(pCtx);
}

///////////////////////////////////////////////////////////////////////////////////////////////
static bool percentile_function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!function_setup(pCtx, pResultInfo)) {
    return false;
  }

  // in the first round, get the min-max value of all involved data
  SPercentileInfo *pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  SET_DOUBLE_VAL(&pInfo->minval, DBL_MAX);
  SET_DOUBLE_VAL(&pInfo->maxval, -DBL_MAX);
  pInfo->numOfElems = 0;

  return true;
}

static void percentile_function(SqlFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  SPercentileInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  if (pCtx->currentStage == REPEAT_SCAN && pInfo->stage == 0) {
    pInfo->stage += 1;

    // all data are null, set it completed
    if (pInfo->numOfElems == 0) {
      pResInfo->complete = true;
      
      return;
    } else {
      pInfo->pMemBucket = tMemBucketCreate(pCtx->inputBytes, pCtx->inputType, pInfo->minval, pInfo->maxval);
    }
  }

  // the first stage, only acquire the min/max value
  if (pInfo->stage == 0) {
    if (pCtx->isAggSet) {
      double tmin = 0.0, tmax = 0.0;
      if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
        tmin = (double)GET_INT64_VAL(&pCtx->agg.min);
        tmax = (double)GET_INT64_VAL(&pCtx->agg.max);
      } else if (IS_FLOAT_TYPE(pCtx->inputType)) {
        tmin = GET_DOUBLE_VAL(&pCtx->agg.min);
        tmax = GET_DOUBLE_VAL(&pCtx->agg.max);
      } else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
        tmin = (double)GET_UINT64_VAL(&pCtx->agg.min);
        tmax = (double)GET_UINT64_VAL(&pCtx->agg.max);
      } else {
        assert(true);
      }

      if (GET_DOUBLE_VAL(&pInfo->minval) > tmin) {
        SET_DOUBLE_VAL(&pInfo->minval, tmin);
      }

      if (GET_DOUBLE_VAL(&pInfo->maxval) < tmax) {
        SET_DOUBLE_VAL(&pInfo->maxval, tmax);
      }

      pInfo->numOfElems += (pCtx->size - pCtx->agg.numOfNull);
    } else {
      for (int32_t i = 0; i < pCtx->size; ++i) {
        char *data = GET_INPUT_DATA(pCtx, i);
        if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
          continue;
        }

        double v = 0;
        GET_TYPED_DATA(v, double, pCtx->inputType, data);

        if (v < GET_DOUBLE_VAL(&pInfo->minval)) {
          SET_DOUBLE_VAL(&pInfo->minval, v);
        }

        if (v > GET_DOUBLE_VAL(&pInfo->maxval)) {
          SET_DOUBLE_VAL(&pInfo->maxval, v);
        }

        pInfo->numOfElems += 1;
      }
    }

    return;
  }

  // the second stage, calculate the true percentile value
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    notNullElems += 1;
    tMemBucketPut(pInfo->pMemBucket, data, 1);
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  //pResInfo->hasResult = DATA_SET_FLAG;
}

static void percentile_finalizer(SqlFunctionCtx *pCtx) {
  double v = pCtx->param[0].nType == TSDB_DATA_TYPE_INT ? pCtx->param[0].i : pCtx->param[0].d;
  
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  SPercentileInfo* ppInfo = (SPercentileInfo *) GET_ROWCELL_INTERBUF(pResInfo);

  tMemBucket * pMemBucket = ppInfo->pMemBucket;
  if (pMemBucket == NULL || pMemBucket->total == 0) {  // check for null
    assert(ppInfo->numOfElems == 0);
    setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
  } else {
    SET_DOUBLE_VAL((double *)pCtx->pOutput, getPercentile(pMemBucket, v));
  }
  
  tMemBucketDestroy(pMemBucket);
  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////
static void buildHistogramInfo(SAPercentileInfo* pInfo) {
  pInfo->pHisto = (SHistogramInfo*) ((char*) pInfo + sizeof(SAPercentileInfo));
  pInfo->pHisto->elems = (SHistBin*) ((char*)pInfo->pHisto + sizeof(SHistogramInfo));
}

static SAPercentileInfo *getAPerctInfo(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo* pInfo = NULL;

  if (pCtx->stableQuery && pCtx->currentStage != MERGE_STAGE) {
    pInfo = (SAPercentileInfo*) pCtx->pOutput;
  } else {
    pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  }

  buildHistogramInfo(pInfo);
  return pInfo;
}

static bool apercentile_function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!function_setup(pCtx, pResultInfo)) {
    return false;
  }
  
  SAPercentileInfo *pInfo = getAPerctInfo(pCtx);
  
  char *tmp = (char *)pInfo + sizeof(SAPercentileInfo);
  pInfo->pHisto = tHistogramCreateFrom(tmp, MAX_HISTOGRAM_BIN);
  return true;
}

static void apercentile_function(SqlFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  SResultRowEntryInfo *     pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo *pInfo = getAPerctInfo(pCtx);

  assert(pInfo->pHisto->elems != NULL);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    notNullElems += 1;

    double v = 0;
    GET_TYPED_DATA(v, double, pCtx->inputType, data);
    tHistogramAdd(&pInfo->pHisto, v);
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    //pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void apercentile_func_merge(SqlFunctionCtx *pCtx) {
  SAPercentileInfo *pInput = (SAPercentileInfo *)GET_INPUT_DATA_LIST(pCtx);
  
  pInput->pHisto = (SHistogramInfo*) ((char *)pInput + sizeof(SAPercentileInfo));
  pInput->pHisto->elems = (SHistBin*) ((char *)pInput->pHisto + sizeof(SHistogramInfo));

  if (pInput->pHisto->numOfElems <= 0) {
    return;
  }
  
  SAPercentileInfo *pOutput = getAPerctInfo(pCtx);
  SHistogramInfo  *pHisto = pOutput->pHisto;
  
  if (pHisto->numOfElems <= 0) {
    memcpy(pHisto, pInput->pHisto, sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
    pHisto->elems = (SHistBin*) ((char *)pHisto + sizeof(SHistogramInfo));
  } else {
    //TODO(dengyihao): avoid memcpy   
    pHisto->elems = (SHistBin*) ((char *)pHisto + sizeof(SHistogramInfo));
    SHistogramInfo *pRes = tHistogramMerge(pHisto, pInput->pHisto, MAX_HISTOGRAM_BIN);
    memcpy(pHisto, pRes, sizeof(SHistogramInfo) + sizeof(SHistBin) * MAX_HISTOGRAM_BIN);
    pHisto->elems = (SHistBin*) ((char *)pHisto + sizeof(SHistogramInfo));
    tHistogramDestroy(&pRes);
  }

  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  //pResInfo->hasResult = DATA_SET_FLAG;
  SET_VAL(pCtx, 1, 1);
}

static void apercentile_finalizer(SqlFunctionCtx *pCtx) {
  double v = (pCtx->param[0].nType == TSDB_DATA_TYPE_INT) ? pCtx->param[0].i : pCtx->param[0].d;
  
  SResultRowEntryInfo *     pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo *pOutput = GET_ROWCELL_INTERBUF(pResInfo);

  if (pCtx->currentStage == MERGE_STAGE) {
//    if (pResInfo->hasResult == DATA_SET_FLAG) {  // check for null
//      assert(pOutput->pHisto->numOfElems > 0);
//
//      double  ratio[] = {v};
//      double *res = tHistogramUniform(pOutput->pHisto, ratio, 1);
//
//      memcpy(pCtx->pOutput, res, sizeof(double));
//      taosMemoryFree(res);
//    } else {
//      setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
//      return;
//    }
  } else {
    if (pOutput->pHisto->numOfElems > 0) {
      double ratio[] = {v};
      
      double *res = tHistogramUniform(pOutput->pHisto, ratio, 1);
      memcpy(pCtx->pOutput, res, sizeof(double));
      taosMemoryFree(res);
    } else {  // no need to free
      setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
      return;
    }
  }
  
  doFinalizer(pCtx);
}

/////////////////////////////////////////////////////////////////////////////////
static bool leastsquares_function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }

  SLeastsquaresInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  // 2*3 matrix
  pInfo->startVal = pCtx->param[0].d;
  return true;
}

#define LEASTSQR_CAL(p, x, y, index, step) \
  do {                                     \
    (p)[0][0] += (double)(x) * (x);        \
    (p)[0][1] += (double)(x);              \
    (p)[0][2] += (double)(x) * (y)[index]; \
    (p)[1][2] += (y)[index];               \
    (x) += step;                           \
  } while (0)

#define LEASTSQR_CAL_LOOP(ctx, param, x, y, tsdbType, n, step) \
  for (int32_t i = 0; i < (ctx)->size; ++i) {                  \
    if ((ctx)->hasNull && isNull((char *)&(y)[i], tsdbType)) { \
      continue;                                                \
    }                                                          \
    (n)++;                                                     \
    LEASTSQR_CAL(param, x, y, i, step);                        \
  }

static void leastsquares_function(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *     pResInfo = GET_RES_INFO(pCtx);
  SLeastsquaresInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  double(*param)[3] = pInfo->mat;
  double x = pInfo->startVal;
  
  void *pData = GET_INPUT_DATA_LIST(pCtx);
  
  int32_t numOfElem = 0;
  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *p = pData;
      //            LEASTSQR_CAL_LOOP(pCtx, param, pParamData, p);
      for (int32_t i = 0; i < pCtx->size; ++i) {
        if (pCtx->hasNull && isNull((const char*) p, pCtx->inputType)) {
          continue;
        }
        
        param[0][0] += x * x;
        param[0][1] += x;
        param[0][2] += x * p[i];
        param[1][2] += p[i];
        
        x += pCtx->param[1].d;
        numOfElem++;
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].d);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].d);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].d);
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].d);
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].d);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].d);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].d);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      uint32_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].d);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].d);
      break;
    }
  }
  
  pInfo->startVal = x;
  pInfo->num += numOfElem;
  
  if (pInfo->num > 0) {
    //pResInfo->hasResult = DATA_SET_FLAG;
  }
  
  SET_VAL(pCtx, numOfElem, 1);
}

static void leastsquares_finalizer(SqlFunctionCtx *pCtx) {
  // no data in query
  SResultRowEntryInfo *     pResInfo = GET_RES_INFO(pCtx);
  SLeastsquaresInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  if (pInfo->num == 0) {
    setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
    return;
  }
  
  double(*param)[3] = pInfo->mat;
  
  param[1][1] = (double)pInfo->num;
  param[1][0] = param[0][1];
  
  param[0][0] -= param[1][0] * (param[0][1] / param[1][1]);
  param[0][2] -= param[1][2] * (param[0][1] / param[1][1]);
  param[0][1] = 0;
  param[1][2] -= param[0][2] * (param[1][0] / param[0][0]);
  param[1][0] = 0;
  param[0][2] /= param[0][0];
  
  param[1][2] /= param[1][1];
  
  int32_t maxOutputSize = AVG_FUNCTION_INTER_BUFFER_SIZE - VARSTR_HEADER_SIZE;
  size_t n = snprintf(varDataVal(pCtx->pOutput), maxOutputSize, "{slop:%.6lf, intercept:%.6lf}",
      param[0][2], param[1][2]);
  
  varDataSetLen(pCtx->pOutput, n);
  doFinalizer(pCtx);
}

static void date_col_output_function(SqlFunctionCtx *pCtx) {
  SET_VAL(pCtx, pCtx->size, 1);
  *(int64_t *)(pCtx->pOutput) = pCtx->startTs;
}

static void col_project_function(SqlFunctionCtx *pCtx) {
  // the number of output rows should not affect the final number of rows, so set it to be 0
  if (pCtx->numOfParams == 2) {
    return;
  }

  // only one row is required.
  if (pCtx->param[0].i == 1) {
    SET_VAL(pCtx, pCtx->size, 1);
  } else {
    INC_INIT_VAL(pCtx, pCtx->size);
  }

  char *pData = GET_INPUT_DATA_LIST(pCtx);
  if (pCtx->order == TSDB_ORDER_ASC) {
    int32_t numOfRows = (pCtx->param[0].i == 1)? 1:pCtx->size;
    memcpy(pCtx->pOutput, pData, (size_t) numOfRows * pCtx->inputBytes);
  } else {
    for(int32_t i = 0; i < pCtx->size; ++i) {
      memcpy(pCtx->pOutput + (pCtx->size - 1 - i) * pCtx->inputBytes, pData + i * pCtx->inputBytes,
             pCtx->inputBytes);
    }
  }
}

/**
 * only used for tag projection query in select clause
 * @param pCtx
 * @return
 */
static void tag_project_function(SqlFunctionCtx *pCtx) {
  INC_INIT_VAL(pCtx, pCtx->size);
  
  assert(pCtx->inputBytes == pCtx->resDataInfo.bytes);

  taosVariantDump(&pCtx->tag, pCtx->pOutput, pCtx->resDataInfo.type, true);
  char* data = pCtx->pOutput;
  pCtx->pOutput += pCtx->resDataInfo.bytes;

  // directly copy from the first one
  for (int32_t i = 1; i < pCtx->size; ++i) {
    memmove(pCtx->pOutput, data, pCtx->resDataInfo.bytes);
    pCtx->pOutput += pCtx->resDataInfo.bytes;
  }
}

/**
 * used in group by clause. when applying group by tags, the tags value is
 * assign by using tag function.
 * NOTE: there is only ONE output for ONE query range
 * @param pCtx
 * @return
 */
static void copy_function(SqlFunctionCtx *pCtx);

static void tag_function(SqlFunctionCtx *pCtx) {
  SET_VAL(pCtx, 1, 1);
  if (pCtx->currentStage == MERGE_STAGE) {
    copy_function(pCtx);
  } else {
    taosVariantDump(&pCtx->tag, pCtx->pOutput, pCtx->resDataInfo.type, true);
  }
}

static void copy_function(SqlFunctionCtx *pCtx) {
  SET_VAL(pCtx, pCtx->size, 1);
  
  char *pData = GET_INPUT_DATA_LIST(pCtx);
  assignVal(pCtx->pOutput, pData, pCtx->inputBytes, pCtx->inputType);
}

enum {
  INITIAL_VALUE_NOT_ASSIGNED = 0,
};

static bool diff_function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  
  // diff function require the value is set to -1
  pCtx->param[1].nType = INITIAL_VALUE_NOT_ASSIGNED;
  return false;
}

static bool deriv_function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!function_setup(pCtx, pResultInfo)) {
    return false;
  }

  // diff function require the value is set to -1
  SDerivInfo* pDerivInfo = GET_ROWCELL_INTERBUF(pResultInfo);

  pDerivInfo->ignoreNegative = pCtx->param[1].i;
  pDerivInfo->prevTs   = -1;
  pDerivInfo->tsWindow = pCtx->param[0].i;
  pDerivInfo->valueSet = false;
  return false;
}

static void deriv_function(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  SDerivInfo* pDerivInfo = GET_ROWCELL_INTERBUF(pResInfo);

  void *data = GET_INPUT_DATA_LIST(pCtx);

  int32_t notNullElems = 0;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  int32_t i = (pCtx->order == TSDB_ORDER_ASC) ? 0 : pCtx->size - 1;

  TSKEY *pTimestamp = pCtx->ptsOutputBuf;
  TSKEY *tsList = GET_TS_LIST(pCtx);

  double *pOutput = (double *)pCtx->pOutput;

  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *pData = (int32_t *)data;
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          SET_DOUBLE_VAL(pOutput, ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs));
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }

      break;
    };

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *pData = (int64_t *)data;
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          *pOutput = ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = (double) pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *pData = (double *)data;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          *pOutput = ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      float *pData = (float *)data;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          *pOutput = ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *pData = (int16_t *)data;
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          *pOutput = ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *pData = (int8_t *)data;
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          *pOutput = ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];

            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }
      break;
    }
    default:
      assert(0);
//      qError("error input type");
  }

  GET_RES_INFO(pCtx)->numOfRes += notNullElems;
}

#define DIFF_IMPL(ctx, d, type)                                                              \
  do {                                                                                       \
    if ((ctx)->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {                               \
      (ctx)->param[1].nType = (ctx)->inputType;                                              \
      *(type *)&(ctx)->param[1].i = *(type *)(d);                                       \
    } else {                                                                                 \
      *(type *)(ctx)->pOutput = *(type *)(d) - (*(type *)(&(ctx)->param[1].i));      \
      *(type *)(&(ctx)->param[1].i) = *(type *)(d);                                     \
      *(int64_t *)(ctx)->ptsOutputBuf = GET_TS_DATA(ctx, index);                             \
    }                                                                                        \
  } while (0);

// TODO difference in date column
static void diff_function(SqlFunctionCtx *pCtx) {
  void *data = GET_INPUT_DATA_LIST(pCtx);
  bool  isFirstBlock = (pCtx->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED);

  int32_t notNullElems = 0;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  int32_t i = (pCtx->order == TSDB_ORDER_ASC) ? 0 : pCtx->size - 1;

  TSKEY* pTimestamp = pCtx->ptsOutputBuf;
  TSKEY* tsList = GET_TS_LIST(pCtx);

  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *pData = (int32_t *)data;
      int32_t *pOutput = (int32_t *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->param[1].nType != INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          *pOutput = (int32_t)(pData[i] - pCtx->param[1].i);  // direct previous may be null
          *pTimestamp = (tsList != NULL)? tsList[i]:0;
          pOutput    += 1;
          pTimestamp += 1;
        }

        pCtx->param[1].i = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *pData = (int64_t *)data;
      int64_t *pOutput = (int64_t *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->param[1].nType != INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          *pOutput = pData[i] - pCtx->param[1].i;  // direct previous may be null
          *pTimestamp = (tsList != NULL)? tsList[i]:0;
          pOutput    += 1;
          pTimestamp += 1;
        }

        pCtx->param[1].i = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *pData = (double *)data;
      double *pOutput = (double *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->param[1].nType != INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          SET_DOUBLE_VAL(pOutput, pData[i] - pCtx->param[1].d);  // direct previous may be null
          *pTimestamp = (tsList != NULL)? tsList[i]:0;
          pOutput    += 1;
          pTimestamp += 1;
        }

        pCtx->param[1].d = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *pData = (float *)data;
      float *pOutput = (float *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->param[1].nType != INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          *pOutput = (float)(pData[i] - pCtx->param[1].d);  // direct previous may be null
          *pTimestamp = (tsList != NULL)? tsList[i]:0;
          pOutput    += 1;
          pTimestamp += 1;
        }

        pCtx->param[1].d = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *pData = (int16_t *)data;
      int16_t *pOutput = (int16_t *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->param[1].nType != INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          *pOutput = (int16_t)(pData[i] - pCtx->param[1].i);  // direct previous may be null
          *pTimestamp = (tsList != NULL)? tsList[i]:0;
          pOutput    += 1;
          pTimestamp += 1;
        }

        pCtx->param[1].i = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    }

    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *pData = (int8_t *)data;
      int8_t *pOutput = (int8_t *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->param[1].nType != INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          *pOutput = (int8_t)(pData[i] - pCtx->param[1].i);  // direct previous may be null
          *pTimestamp = (tsList != NULL)? tsList[i]:0;
          pOutput    += 1;
          pTimestamp += 1;
        }

        pCtx->param[1].i = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    }
    default:
      assert(0);
//      qError("error input type");
  }

  // initial value is not set yet
  if (pCtx->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED || notNullElems <= 0) {
    /*
     * 1. current block and blocks before are full of null
     * 2. current block may be null value
     */
    assert(pCtx->hasNull);
  } else {
    int32_t forwardStep = (isFirstBlock) ? notNullElems - 1 : notNullElems;

    GET_RES_INFO(pCtx)->numOfRes += forwardStep;
  }
}

#if 0
char *getArithColumnData(void *param, const char* name, int32_t colId) {
  SScalarFunctionSupport *pSupport = (SScalarFunctionSupport *)param;
  
  int32_t index = -1;
  for (int32_t i = 0; i < pSupport->numOfCols; ++i) {
    if (colId == pSupport->colList[i].colId) {
      index = i;
      break;
    }
  }
  
  assert(index >= 0);
  return pSupport->data[index] + pSupport->offset * pSupport->colList[index].bytes;
}
#endif

static void arithmetic_function(SqlFunctionCtx *pCtx) {
  GET_RES_INFO(pCtx)->numOfRes += pCtx->size;
  //SScalarFunctionSupport *pSup = (SScalarFunctionSupport *)pCtx->param[1].pz;

  SScalarParam output = {0};
  output.data = pCtx->pOutput;

  //evaluateExprNodeTree(pSup->pExprInfo->pExpr, pCtx->size, &output, pSup, getArithColumnData);
}

#define LIST_MINMAX_N(ctx, minOutput, maxOutput, elemCnt, data, type, tsdbType, numOfNotNullElem) \
  {                                                                                               \
    type *inputData = (type *)data;                                                               \
    for (int32_t i = 0; i < elemCnt; ++i) {                                                       \
      if ((ctx)->hasNull && isNull((char *)&inputData[i], tsdbType)) {                            \
        continue;                                                                                 \
      }                                                                                           \
      if (inputData[i] < minOutput) {                                                             \
        minOutput = (double)inputData[i];                                                                 \
      }                                                                                           \
      if (inputData[i] > maxOutput) {                                                             \
        maxOutput = (double)inputData[i];                                                                 \
      }                                                                                           \
      numOfNotNullElem++;                                                                         \
    }                                                                                             \
  }

/////////////////////////////////////////////////////////////////////////////////
static bool spread_function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  
  SSpreadInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  // this is the server-side setup function in client-side, the secondary merge do not need this procedure
  if (pCtx->currentStage == MERGE_STAGE) {
    pCtx->param[0].d = DBL_MAX;
    pCtx->param[3].d = -DBL_MAX;
  } else {
    pInfo->min = DBL_MAX;
    pInfo->max = -DBL_MAX;
  }
  
  return true;
}

static void spread_function(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  SSpreadInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  int32_t numOfElems = 0;
  
  // todo : opt with pre-calculated result
  // column missing cause the hasNull to be true
  if (pCtx->isAggSet) {
    numOfElems = pCtx->size - pCtx->agg.numOfNull;
    
    // all data are null in current data block, ignore current data block
    if (numOfElems == 0) {
      goto _spread_over;
    }

    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType) || IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType) ||
        (pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP)) {
      if (pInfo->min > pCtx->agg.min) {
        pInfo->min = (double)pCtx->agg.min;
      }

      if (pInfo->max < pCtx->agg.max) {
        pInfo->max = (double)pCtx->agg.max;
      }
    } else if (IS_FLOAT_TYPE(pCtx->inputType)) {
      if (pInfo->min > GET_DOUBLE_VAL((const char *)&(pCtx->agg.min))) {
        pInfo->min = GET_DOUBLE_VAL((const char *)&(pCtx->agg.min));
      }
      
      if (pInfo->max < GET_DOUBLE_VAL((const char *)&(pCtx->agg.max))) {
        pInfo->max = GET_DOUBLE_VAL((const char *)&(pCtx->agg.max));
      }
    }
    
    goto _spread_over;
  }
  
  void *pData = GET_INPUT_DATA_LIST(pCtx);
  numOfElems = 0;
  
  if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, int8_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, int16_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, int32_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT || pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, int64_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, double, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, float, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_UTINYINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, uint8_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_USMALLINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, uint16_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_UINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, uint32_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_UBIGINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, uint64_t, pCtx->inputType, numOfElems);
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == numOfElems);
  }
  
  _spread_over:
  SET_VAL(pCtx, numOfElems, 1);
  
  if (numOfElems > 0) {
    //pResInfo->hasResult = DATA_SET_FLAG;
    pInfo->hasResult = DATA_SET_FLAG;
  }
  
  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pCtx->stableQuery) {
    memcpy(pCtx->pOutput, GET_ROWCELL_INTERBUF(pResInfo), sizeof(SSpreadInfo));
  }
}

/*
 * here we set the result value back to the intermediate buffer, to apply the finalize the function
 * the final result is generated in spread_function_finalizer
 */
void spread_func_merge(SqlFunctionCtx *pCtx) {
  SSpreadInfo *pData = (SSpreadInfo *)GET_INPUT_DATA_LIST(pCtx);
  if (pData->hasResult != DATA_SET_FLAG) {
    return;
  }
  
  if (pCtx->param[0].d > pData->min) {
    pCtx->param[0].d = pData->min;
  }
  
  if (pCtx->param[3].d < pData->max) {
    pCtx->param[3].d = pData->max;
  }
  
//  GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
}

void spread_function_finalizer(SqlFunctionCtx *pCtx) {
  /*
   * here we do not check the input data types, because in case of metric query,
   * the type of intermediate data is binary
   */
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  
  if (pCtx->currentStage == MERGE_STAGE) {
    assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
    
//    if (pResInfo->hasResult != DATA_SET_FLAG) {
//      setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
//      return;
//    }
    
    SET_DOUBLE_VAL((double *)pCtx->pOutput, pCtx->param[3].d - pCtx->param[0].d);
  } else {
    assert(IS_NUMERIC_TYPE(pCtx->inputType) || (pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP));
    
    SSpreadInfo *pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
    if (pInfo->hasResult != DATA_SET_FLAG) {
      setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
      return;
    }
    
    SET_DOUBLE_VAL((double *)pCtx->pOutput, pInfo->max - pInfo->min);
  }
  
  GET_RES_INFO(pCtx)->numOfRes = 1;  // todo add test case
  doFinalizer(pCtx);
}


/**
 * param[1]: start time
 * param[2]: end time
 * @param pCtx
 */
static bool twa_function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }

  STwaInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pInfo->p.key    = INT64_MIN;
  pInfo->win      = TSWINDOW_INITIALIZER;
  return true;
}

static double twa_get_area(SPoint1 s, SPoint1 e) {
  if ((s.val >= 0 && e.val >= 0)|| (s.val <=0 && e.val <= 0)) {
    return (s.val + e.val) * (e.key - s.key) / 2;
  }

  double x = (s.key * e.val - e.key * s.val)/(e.val - s.val);
  double val = (s.val * (x - s.key) + e.val * (e.key - x)) / 2;
  return val;
}

static int32_t twa_function_impl(SqlFunctionCtx* pCtx, int32_t index, int32_t size) {
  int32_t notNullElems = 0;
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);

  STwaInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  TSKEY    *tsList = GET_TS_LIST(pCtx);

  int32_t i = index;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  SPoint1* last = &pInfo->p;

  if (pCtx->start.key != INT64_MIN) {
    assert((pCtx->start.key < tsList[i] && pCtx->order == TSDB_ORDER_ASC) ||
               (pCtx->start.key > tsList[i] && pCtx->order == TSDB_ORDER_DESC));

    assert(last->key == INT64_MIN);

    last->key = tsList[i];
    GET_TYPED_DATA(last->val, double, pCtx->inputType, GET_INPUT_DATA(pCtx, index));

    pInfo->dOutput += twa_get_area(pCtx->start, *last);

    pInfo->hasResult = DATA_SET_FLAG;
    pInfo->win.skey = pCtx->start.key;
    notNullElems++;
    i += step;
  } else if (pInfo->p.key == INT64_MIN) {
    last->key = tsList[i];
    GET_TYPED_DATA(last->val, double, pCtx->inputType, GET_INPUT_DATA(pCtx, index));

    pInfo->hasResult = DATA_SET_FLAG;
    pInfo->win.skey = last->key;
    notNullElems++;
    i += step;
  }

  // calculate the value of
  switch(pCtx->inputType) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *val = (int8_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }

#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *val = (int16_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      int32_t *val = (int32_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *val = (int64_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = (double) val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = (double)val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *val = (float*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = (double)val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *val = (double*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t *val = (uint8_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }

#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t *val = (uint16_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }

#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      uint32_t *val = (uint32_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }

#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t *val = (uint64_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = (double) val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = (double) val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    default: assert(0);
  }

  // the last interpolated time window value
  if (pCtx->end.key != INT64_MIN) {
    pInfo->dOutput  += twa_get_area(pInfo->p, pCtx->end);
    pInfo->p = pCtx->end;
  }

  pInfo->win.ekey  = pInfo->p.key;
  return notNullElems;
}

static void twa_function(SqlFunctionCtx *pCtx) {
  void *data = GET_INPUT_DATA_LIST(pCtx);

  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  STwaInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  // skip null value
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  int32_t i = (pCtx->order == TSDB_ORDER_ASC)? 0:(pCtx->size - 1);
  while (pCtx->hasNull && i < pCtx->size && i >= 0 && isNull((char *)data + pCtx->inputBytes * i, pCtx->inputType)) {
    i += step;
  }

  int32_t notNullElems = 0;
  if (i >= 0 && i < pCtx->size) {
    notNullElems = twa_function_impl(pCtx, i, pCtx->size);
  }

  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    //pResInfo->hasResult = DATA_SET_FLAG;
  }
  
  if (pCtx->stableQuery) {
    memcpy(pCtx->pOutput, pInfo, sizeof(STwaInfo));
  }
}

/*
 * To copy the input to interResBuf to avoid the input buffer space be over writen
 * by next input data. The TWA function only applies to each table, so no merge procedure
 * is required, we simply copy to the resut ot interResBuffer.
 */
void twa_function_copy(SqlFunctionCtx *pCtx) {
  assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  
  memcpy(GET_ROWCELL_INTERBUF(pResInfo), pCtx->pInput, (size_t)pCtx->inputBytes);
//  pResInfo->hasResult = ((STwaInfo *)pCtx->pInput)->hasResult;
}

void twa_function_finalizer(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  
  STwaInfo *pInfo = (STwaInfo *)GET_ROWCELL_INTERBUF(pResInfo);
  if (pInfo->hasResult != DATA_SET_FLAG) {
    setNull(pCtx->pOutput, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
    return;
  }

//  assert(pInfo->win.ekey == pInfo->p.key && pInfo->hasResult == pResInfo->hasResult);
  if (pInfo->win.ekey == pInfo->win.skey) {
    SET_DOUBLE_VAL((double *)pCtx->pOutput, pInfo->p.val);
  } else {
    SET_DOUBLE_VAL((double *)pCtx->pOutput , pInfo->dOutput / (pInfo->win.ekey - pInfo->win.skey));
  }
  
  GET_RES_INFO(pCtx)->numOfRes = 1;
  doFinalizer(pCtx);
}

/**
 *
 * @param pCtx
 */

static void interp_function_impl(SqlFunctionCtx *pCtx) {
  int32_t type = (int32_t) pCtx->param[2].i;
  if (type == TSDB_FILL_NONE) {
    return;
  }

  bool ascQuery = (pCtx->order == TSDB_ORDER_ASC);

  if (pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP) {
    *(TSKEY *)pCtx->pOutput = pCtx->startTs;
  } else if (type == TSDB_FILL_NULL) {
    setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
  } else if (type == TSDB_FILL_SET_VALUE) {
    taosVariantDump(&pCtx->param[1], pCtx->pOutput, pCtx->inputType, true);
  } else {
    if (pCtx->start.key != INT64_MIN && ((ascQuery && pCtx->start.key <= pCtx->startTs && pCtx->end.key >= pCtx->startTs) || ((!ascQuery) && pCtx->start.key >= pCtx->startTs && pCtx->end.key <= pCtx->startTs))) {
      if (type == TSDB_FILL_PREV) {
        if (IS_NUMERIC_TYPE(pCtx->inputType) || pCtx->inputType == TSDB_DATA_TYPE_BOOL) {
          SET_TYPED_DATA(pCtx->pOutput, pCtx->inputType, pCtx->start.val);
        } else {
          assignVal(pCtx->pOutput, pCtx->start.ptr, pCtx->resDataInfo.bytes, pCtx->inputType);
        }
      } else if (type == TSDB_FILL_NEXT) {
        if (IS_NUMERIC_TYPE(pCtx->inputType) || pCtx->inputType == TSDB_DATA_TYPE_BOOL) {
          SET_TYPED_DATA(pCtx->pOutput, pCtx->inputType, pCtx->end.val);
        } else {
          assignVal(pCtx->pOutput, pCtx->end.ptr, pCtx->resDataInfo.bytes, pCtx->inputType);
        }
      } else if (type == TSDB_FILL_LINEAR) {
        SPoint point1 = {.key = pCtx->start.key, .val = &pCtx->start.val};
        SPoint point2 = {.key = pCtx->end.key, .val = &pCtx->end.val};
        SPoint point  = {.key = pCtx->startTs, .val = pCtx->pOutput};

        int32_t srcType = pCtx->inputType;
        if (IS_NUMERIC_TYPE(srcType)) {  // TODO should find the not null data?
          if (isNull((char *)&pCtx->start.val, srcType) || isNull((char *)&pCtx->end.val, srcType)) {
            setNull(pCtx->pOutput, srcType, pCtx->inputBytes);
          } else {
            taosGetLinearInterpolationVal(&point, pCtx->resDataInfo.type, &point1, &point2, TSDB_DATA_TYPE_DOUBLE);
          }
        } else {
          setNull(pCtx->pOutput, srcType, pCtx->inputBytes);
        }
      }
    } else {
      // no data generated yet
      if (pCtx->size < 1) {
        return;
      }

      // check the timestamp in input buffer
      TSKEY skey = GET_TS_DATA(pCtx, 0);

      if (type == TSDB_FILL_PREV) {
        if ((ascQuery && skey > pCtx->startTs) || ((!ascQuery) && skey < pCtx->startTs)) {
          return;
        }

        if (pCtx->size > 1) {
          TSKEY ekey = GET_TS_DATA(pCtx, 1);
          if ((ascQuery &&  ekey > skey && ekey <= pCtx->startTs) ||
             ((!ascQuery) && ekey < skey && ekey >= pCtx->startTs)){
            skey = ekey;
          }
        }
//        assignVal(pCtx->pOutput, pCtx->pInput, pCtx->resDataInfo.bytes, pCtx->inputType);
      } else if (type == TSDB_FILL_NEXT) {
        TSKEY ekey = skey;
        char* val = NULL;
        
        if ((ascQuery && ekey < pCtx->startTs) || ((!ascQuery) && ekey > pCtx->startTs)) {
          if (pCtx->size > 1) {
            ekey = GET_TS_DATA(pCtx, 1);
            if ((ascQuery && ekey < pCtx->startTs) || ((!ascQuery) && ekey > pCtx->startTs)) {
              return;
            }

            val = ((char*)pCtx->pInput) + pCtx->inputBytes;            
          } else {
            return;
          }
        } else {
          val = (char*)pCtx->pInput;
        }
        
        assignVal(pCtx->pOutput, val, pCtx->resDataInfo.bytes, pCtx->inputType);
      } else if (type == TSDB_FILL_LINEAR) {
        if (pCtx->size <= 1) {
          return;
        }
      
        TSKEY ekey = GET_TS_DATA(pCtx, 1);
      
        // no data generated yet
        if ((ascQuery && !(skey <= pCtx->startTs && ekey >= pCtx->startTs))
           || ((!ascQuery) && !(skey >= pCtx->startTs && ekey <= pCtx->startTs))) {
          return;
        }
        
        char *start = GET_INPUT_DATA(pCtx, 0);
        char *end = GET_INPUT_DATA(pCtx, 1);

        SPoint point1 = {.key = skey, .val = start};
        SPoint point2 = {.key = ekey, .val = end};
        SPoint point = {.key = pCtx->startTs, .val = pCtx->pOutput};

        int32_t srcType = pCtx->inputType;
        if (IS_NUMERIC_TYPE(srcType)) {  // TODO should find the not null data?
          if (isNull(start, srcType) || isNull(end, srcType)) {
            setNull(pCtx->pOutput, srcType, pCtx->inputBytes);
          } else {
            taosGetLinearInterpolationVal(&point, pCtx->resDataInfo.type, &point1, &point2, srcType);
          }
        } else {
          setNull(pCtx->pOutput, srcType, pCtx->inputBytes);
        }
      }
    }
  }

  SET_VAL(pCtx, 1, 1);
}

static void interp_function(SqlFunctionCtx *pCtx) {
  // at this point, the value is existed, return directly
  if (pCtx->size > 0) {
    bool ascQuery = (pCtx->order == TSDB_ORDER_ASC);
    TSKEY key;
    char *pData;
    int32_t typedData = 0;
    
    if (ascQuery) {
      key = GET_TS_DATA(pCtx, 0);
      pData = GET_INPUT_DATA(pCtx, 0);
    } else {
      key = pCtx->start.key;
      if (key == INT64_MIN) {
        key = GET_TS_DATA(pCtx, 0);
        pData = GET_INPUT_DATA(pCtx, 0);
      } else {        
        if (!(IS_NUMERIC_TYPE(pCtx->inputType) || pCtx->inputType == TSDB_DATA_TYPE_BOOL)) {
          pData = pCtx->start.ptr;
        } else {
          typedData = 1;
          pData = (char *)&pCtx->start.val;
        }
      }
    }
    
    //if (key == pCtx->startTs && (ascQuery || !(IS_NUMERIC_TYPE(pCtx->inputType) || pCtx->inputType == TSDB_DATA_TYPE_BOOL))) {
    if (key == pCtx->startTs) {
      if (typedData) {
        SET_TYPED_DATA(pCtx->pOutput, pCtx->inputType, *(double *)pData);
      } else {
        assignVal(pCtx->pOutput, pData, pCtx->inputBytes, pCtx->inputType);
      }
      
      SET_VAL(pCtx, 1, 1);
    } else {
      interp_function_impl(pCtx);
    }
  } else {  //no qualified data rows and interpolation is required
    interp_function_impl(pCtx);
  }
}

static bool ts_comp_function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;  // not initialized since it has been initialized
  }

  STSCompInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pInfo->pTSBuf = tsBufCreate(false, pCtx->order);
  pInfo->pTSBuf->tsOrder = pCtx->order;
  return true;
}

static void ts_comp_function(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  STSBuf *     pTSbuf = ((STSCompInfo *)(GET_ROWCELL_INTERBUF(pResInfo)))->pTSBuf;
  
  const char *input = GET_INPUT_DATA_LIST(pCtx);
  
  // primary ts must be existed, so no need to check its existance
  if (pCtx->order == TSDB_ORDER_ASC) {
    tsBufAppend(pTSbuf, (int32_t)pCtx->param[0].i, &pCtx->tag, input, pCtx->size * TSDB_KEYSIZE);
  } else {
    for (int32_t i = pCtx->size - 1; i >= 0; --i) {
      char *d = GET_INPUT_DATA(pCtx, i);
      tsBufAppend(pTSbuf, (int32_t)pCtx->param[0].i, &pCtx->tag, d, (int32_t)TSDB_KEYSIZE);
    }
  }
  
  SET_VAL(pCtx, pCtx->size, 1);
  //pResInfo->hasResult = DATA_SET_FLAG;
}

static void ts_comp_finalize(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  
  STSCompInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  STSBuf *     pTSbuf = pInfo->pTSBuf;
  
  tsBufFlush(pTSbuf);
//  qDebug("total timestamp :%"PRId64, pTSbuf->numOfTotal);

  // TODO refactor transfer ownership of current file
  *(TdFilePtr *)pCtx->pOutput = pTSbuf->pFile;

  pResInfo->complete = true;

  // get the file size
  int64_t file_size;
  if (taosFStatFile(pTSbuf->pFile, &file_size, NULL) == 0) {
    pResInfo->numOfRes = (uint32_t )file_size;
  }

  pTSbuf->remainOpen = true;
  tsBufDestroy(pTSbuf);

  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////////////////
// rate functions
static double do_calc_rate(const SRateInfo* pRateInfo, double tickPerSec) {
  if ((INT64_MIN == pRateInfo->lastKey) || (INT64_MIN == pRateInfo->firstKey) ||
      (pRateInfo->firstKey >= pRateInfo->lastKey)) {
    return 0.0;
  }

  double diff = 0;
  if (pRateInfo->isIRate) {
    // If the previous value of the last is greater than the last value, only keep the last point instead of the delta
    // value between two values.
    diff = pRateInfo->lastValue;
    if (diff >= pRateInfo->firstValue) {
      diff -= pRateInfo->firstValue;
    }
  } else {
    diff = pRateInfo->correctionValue + pRateInfo->lastValue -  pRateInfo->firstValue;
    if (diff <= 0) {
      return 0;
    }
  }
  
  int64_t duration = pRateInfo->lastKey - pRateInfo->firstKey;
  if (duration == 0) {
    return 0;
  }

  return (duration > 0)? ((double)diff) / (duration/tickPerSec):0.0;
}

static bool rate_function_setup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  
  SRateInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pInfo->correctionValue = 0;
  pInfo->firstKey    = INT64_MIN;
  pInfo->lastKey     = INT64_MIN;
  pInfo->firstValue  = (double) INT64_MIN;
  pInfo->lastValue   = (double) INT64_MIN;

  pInfo->hasResult = 0;
  pInfo->isIRate = (pCtx->functionId == FUNCTION_IRATE);
  return true;
}

static void rate_function(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  
  int32_t    notNullElems = 0;
  SRateInfo *pRateInfo = (SRateInfo *)GET_ROWCELL_INTERBUF(pResInfo);
  TSKEY     *primaryKey = GET_TS_LIST(pCtx);
  
//  qDebug("%p rate_function() size:%d, hasNull:%d", pCtx, pCtx->size, pCtx->hasNull);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *pData = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
//      qDebug("%p rate_function() index of null data:%d", pCtx, i);
      continue;
    }
    
    notNullElems++;
    
    double v = 0;
    GET_TYPED_DATA(v, double, pCtx->inputType, pData);
    
    if ((INT64_MIN == pRateInfo->firstValue) || (INT64_MIN == pRateInfo->firstKey)) {
      pRateInfo->firstValue = v;
      pRateInfo->firstKey = primaryKey[i];
    }
    
    if (INT64_MIN == pRateInfo->lastValue) {
      pRateInfo->lastValue = v;
    } else if (v < pRateInfo->lastValue) {
      pRateInfo->correctionValue += pRateInfo->lastValue;
    }
    
    pRateInfo->lastValue = v;
    pRateInfo->lastKey   = primaryKey[i];
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    pRateInfo->hasResult = DATA_SET_FLAG;
//    pResInfo->hasResult  = DATA_SET_FLAG;
  }
  
  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pCtx->stableQuery) {
    memcpy(pCtx->pOutput, GET_ROWCELL_INTERBUF(pResInfo), sizeof(SRateInfo));
  }
}

static void rate_func_copy(SqlFunctionCtx *pCtx) {
  assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
  
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  memcpy(GET_ROWCELL_INTERBUF(pResInfo), pCtx->pInput, (size_t)pCtx->inputBytes);
//  pResInfo->hasResult = ((SRateInfo*)pCtx->pInput)->hasResult;
}

static void rate_finalizer(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo  = GET_RES_INFO(pCtx);
  SRateInfo   *pRateInfo = (SRateInfo *)GET_ROWCELL_INTERBUF(pResInfo);

  if (pRateInfo->hasResult != DATA_SET_FLAG) {
    setNull(pCtx->pOutput, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
    return;
  }
  
  SET_DOUBLE_VAL((double*) pCtx->pOutput, do_calc_rate(pRateInfo, (double) TSDB_TICK_PER_SECOND(pCtx->param[0].i)));

  // cannot set the numOfIteratedElems again since it is set during previous iteration
  pResInfo->numOfRes  = 1;
  //pResInfo->hasResult = DATA_SET_FLAG;
  
  doFinalizer(pCtx);
}

static void irate_function(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo  *pResInfo = GET_RES_INFO(pCtx);

  int32_t    notNullElems = 0;
  SRateInfo *pRateInfo    = (SRateInfo *)GET_ROWCELL_INTERBUF(pResInfo);
  TSKEY     *primaryKey   = GET_TS_LIST(pCtx);

  for (int32_t i = pCtx->size - 1; i >= 0; --i) {
    char *pData = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
      continue;
    }
    
    notNullElems++;
    
    double v = 0;
    GET_TYPED_DATA(v, double, pCtx->inputType, pData);

    if ((INT64_MIN == pRateInfo->lastKey) || primaryKey[i] > pRateInfo->lastKey) {
      pRateInfo->lastValue = v;
      pRateInfo->lastKey   = primaryKey[i];
      continue;
    }
    
    if ((INT64_MIN == pRateInfo->firstKey) || primaryKey[i] > pRateInfo->firstKey) {
      pRateInfo->firstValue = v;
      pRateInfo->firstKey = primaryKey[i];
      break;
    }
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    pRateInfo->hasResult = DATA_SET_FLAG;
//    pResInfo->hasResult  = DATA_SET_FLAG;
  }
  
  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pCtx->stableQuery) {
    memcpy(pCtx->pOutput, GET_ROWCELL_INTERBUF(pResInfo), sizeof(SRateInfo));
  }
}

static void blockDistInfoFromBinary(const char* data, int32_t len, STableBlockDist* pDist) {
  SBufferReader br = tbufInitReader(data, len, false);

  pDist->numOfTables = tbufReadUint32(&br);
  pDist->numOfFiles  = tbufReadUint16(&br);
  pDist->totalSize   = tbufReadUint64(&br);
  pDist->totalRows   = tbufReadUint64(&br);
  pDist->maxRows     = tbufReadInt32(&br);
  pDist->minRows     = tbufReadInt32(&br);
  pDist->numOfRowsInMemTable = tbufReadUint32(&br);
  pDist->numOfSmallBlocks = tbufReadUint32(&br);
  int64_t numSteps = tbufReadUint64(&br);

  bool comp = tbufReadUint8(&br);
  uint32_t compLen = tbufReadUint32(&br);

  size_t originalLen = (size_t) (numSteps *sizeof(SFileBlockInfo));

  char* outputBuf = NULL;
  if (comp) {
    outputBuf = taosMemoryMalloc(originalLen);

    size_t actualLen = compLen;
    const char* compStr = tbufReadBinary(&br, &actualLen);

    int32_t orignalLen = tsDecompressString(compStr, compLen, 1, outputBuf,
                                            (int32_t)originalLen , ONE_STAGE_COMP, NULL, 0);
    assert(orignalLen == numSteps *sizeof(SFileBlockInfo));
  } else {
    outputBuf = (char*) tbufReadBinary(&br, &originalLen);
  }

  pDist->dataBlockInfos = taosArrayFromList(outputBuf, (uint32_t)numSteps, sizeof(SFileBlockInfo));
  if (comp) {
    taosMemoryFreeClear(outputBuf);
  }
}

static void blockInfo_func(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  STableBlockDist* pDist = (STableBlockDist*) GET_ROWCELL_INTERBUF(pResInfo);

  int32_t len = *(int32_t*) pCtx->pInput;
  blockDistInfoFromBinary((char*)pCtx->pInput + sizeof(int32_t), len, pDist);
  pDist->rowSize = (uint16_t)pCtx->param[0].i;

  memcpy(pCtx->pOutput, pCtx->pInput, sizeof(int32_t) + len);

  pResInfo->numOfRes  = 1;
  //pResInfo->hasResult = DATA_SET_FLAG;
}

static void mergeTableBlockDist(SResultRowEntryInfo* pResInfo, const STableBlockDist* pSrc) {
  STableBlockDist* pDist = (STableBlockDist*) GET_ROWCELL_INTERBUF(pResInfo);
  assert(pDist != NULL && pSrc != NULL);

  pDist->numOfTables += pSrc->numOfTables;
  pDist->numOfRowsInMemTable += pSrc->numOfRowsInMemTable;
  pDist->numOfSmallBlocks += pSrc->numOfSmallBlocks;
  pDist->numOfFiles += pSrc->numOfFiles;
  pDist->totalSize += pSrc->totalSize;
  pDist->totalRows += pSrc->totalRows;

//  if (pResInfo->hasResult == DATA_SET_FLAG) {
//    pDist->maxRows = TMAX(pDist->maxRows, pSrc->maxRows);
//    pDist->minRows = TMIN(pDist->minRows, pSrc->minRows);
//  } else {
    pDist->maxRows = pSrc->maxRows;
    pDist->minRows = pSrc->minRows;

    int32_t maxSteps = TSDB_MAX_MAX_ROW_FBLOCK/TSDB_BLOCK_DIST_STEP_ROWS;
    if (TSDB_MAX_MAX_ROW_FBLOCK % TSDB_BLOCK_DIST_STEP_ROWS != 0) {
      ++maxSteps;
    }
    pDist->dataBlockInfos = taosArrayInit(maxSteps, sizeof(SFileBlockInfo));
    taosArraySetSize(pDist->dataBlockInfos, maxSteps);
//  }

  size_t steps = taosArrayGetSize(pSrc->dataBlockInfos);
  for (int32_t i = 0; i < steps; ++i) {
    int32_t srcNumBlocks = ((SFileBlockInfo*)taosArrayGet(pSrc->dataBlockInfos, i))->numBlocksOfStep;
    SFileBlockInfo* blockInfo = (SFileBlockInfo*)taosArrayGet(pDist->dataBlockInfos, i);
    blockInfo->numBlocksOfStep += srcNumBlocks;
  }
}

void block_func_merge(SqlFunctionCtx* pCtx) {
  STableBlockDist info = {0};
  int32_t len = *(int32_t*) pCtx->pInput;
  blockDistInfoFromBinary(((char*)pCtx->pInput) + sizeof(int32_t), len, &info);
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  mergeTableBlockDist(pResInfo, &info);
  taosArrayDestroy(info.dataBlockInfos); 

  pResInfo->numOfRes = 1;
  //pResInfo->hasResult = DATA_SET_FLAG;
}

void getPercentiles(STableBlockDist *pTableBlockDist, int64_t totalBlocks, int32_t numOfPercents,
                    double* percents, int32_t* percentiles) {
  if (totalBlocks == 0) {
    for (int32_t i = 0; i < numOfPercents; ++i) {
      percentiles[i] = 0;
    }
    return;
  }

  SArray *blocksInfos = pTableBlockDist->dataBlockInfos;
  size_t  numSteps = taosArrayGetSize(blocksInfos);
  size_t  cumulativeBlocks = 0;

  int percentIndex = 0;
  for (int32_t indexStep = 0; indexStep < numSteps; ++indexStep) {
    int32_t numStepBlocks = ((SFileBlockInfo *)taosArrayGet(blocksInfos, indexStep))->numBlocksOfStep;
    if (numStepBlocks == 0) continue;
    cumulativeBlocks += numStepBlocks;

    while (percentIndex < numOfPercents) {
      double blockRank = totalBlocks * percents[percentIndex];
      if (blockRank <= cumulativeBlocks) {
        percentiles[percentIndex] = indexStep;
        ++percentIndex;
      } else {
        break;
      }
    }
  }

  for (int32_t i = 0; i < numOfPercents; ++i) {
    percentiles[i] = (percentiles[i]+1) * TSDB_BLOCK_DIST_STEP_ROWS - TSDB_BLOCK_DIST_STEP_ROWS/2;
  }
}

void generateBlockDistResult(STableBlockDist *pTableBlockDist, char* result) {
  if (pTableBlockDist == NULL) {
    return;
  }

  SArray* blockInfos = pTableBlockDist->dataBlockInfos;
  uint64_t totalRows = pTableBlockDist->totalRows;
  size_t   numSteps = taosArrayGetSize(blockInfos);
  int64_t totalBlocks = 0;
  int64_t min = -1, max = -1, avg = 0;

  for (int32_t i = 0; i < numSteps; i++) {
    SFileBlockInfo *blockInfo = taosArrayGet(blockInfos, i);
    int64_t blocks = blockInfo->numBlocksOfStep;
    totalBlocks += blocks;
  }

  avg = totalBlocks > 0 ? (int64_t)(totalRows/totalBlocks) : 0;
  min = totalBlocks > 0 ? pTableBlockDist->minRows : 0;
  max = totalBlocks > 0 ? pTableBlockDist->maxRows : 0;

  double stdDev = 0;
  if (totalBlocks > 0) {
    double variance = 0;
    for (int32_t i = 0; i < numSteps; i++) {
      SFileBlockInfo *blockInfo = taosArrayGet(blockInfos, i);
      int64_t         blocks = blockInfo->numBlocksOfStep;
      int32_t         rows = (i * TSDB_BLOCK_DIST_STEP_ROWS + TSDB_BLOCK_DIST_STEP_ROWS / 2);
      variance += blocks * (rows - avg) * (rows - avg);
    }
    variance = variance / totalBlocks;
    stdDev = sqrt(variance);
  }

  double percents[] = {0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.95, 0.99};
  int32_t percentiles[] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
  assert(sizeof(percents)/sizeof(double) == sizeof(percentiles)/sizeof(int32_t));
  getPercentiles(pTableBlockDist, totalBlocks, sizeof(percents)/sizeof(double), percents, percentiles);

  uint64_t totalLen = pTableBlockDist->totalSize;
  int32_t rowSize = pTableBlockDist->rowSize;
  int32_t smallBlocks = pTableBlockDist->numOfSmallBlocks;
  double compRatio = (totalRows>0) ? ((double)(totalLen)/(rowSize*totalRows)) : 1;
  int sz = sprintf(result + VARSTR_HEADER_SIZE,
                   "summary: \n\t "
                   "5th=[%d], 10th=[%d], 20th=[%d], 30th=[%d], 40th=[%d], 50th=[%d]\n\t "
                   "60th=[%d], 70th=[%d], 80th=[%d], 90th=[%d], 95th=[%d], 99th=[%d]\n\t "
                   "Min=[%"PRId64"(Rows)] Max=[%"PRId64"(Rows)] Avg=[%"PRId64"(Rows)] Stddev=[%.2f] \n\t "
                   "Rows=[%"PRIu64"], Blocks=[%"PRId64"], SmallBlocks=[%d], Size=[%.3f(Kb)] Comp=[%.2f]\n\t "
                   "RowsInMem=[%d] \n\t",
                   percentiles[0], percentiles[1], percentiles[2], percentiles[3], percentiles[4], percentiles[5],
                   percentiles[6], percentiles[7], percentiles[8], percentiles[9], percentiles[10], percentiles[11],
                   min, max, avg, stdDev,
                   totalRows, totalBlocks, smallBlocks, totalLen/1024.0, compRatio,
                   pTableBlockDist->numOfRowsInMemTable);
  varDataSetLen(result, sz);
  UNUSED(sz);
}

void blockinfo_func_finalizer(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  STableBlockDist* pDist = (STableBlockDist*) GET_ROWCELL_INTERBUF(pResInfo);

  pDist->rowSize = (uint16_t)pCtx->param[0].i;
  generateBlockDistResult(pDist, pCtx->pOutput);

  if (pDist->dataBlockInfos != NULL) {
    taosArrayDestroy(pDist->dataBlockInfos);
    pDist->dataBlockInfos = NULL;
  }

  // cannot set the numOfIteratedElems again since it is set during previous iteration
  pResInfo->numOfRes  = 1;
  //pResInfo->hasResult = DATA_SET_FLAG;

  doFinalizer(pCtx);
}

/////////////////////////////////////////////////////////////////////////////////////////////
/*
 * function compatible list.
 * tag and ts are not involved in the compatibility check
 *
 * 1. functions that are not simultaneously present with any other functions. e.g., diff/ts_z/top/bottom
 * 2. functions that are only allowed to be present only with same functions. e.g., last_row, interp
 * 3. functions that are allowed to be present with other functions.
 *    e.g., count/sum/avg/min/max/stddev/percentile/apercentile/first/last...
 *
 */
int32_t functionCompatList[] = {
    // count,   sum,      avg,       min,      max,    stddev,    percentile,   apercentile, first,   last
    1,          1,        1,         1,        1,      1,          1,           1,           1,      1,
    // last_row,top,      bottom,    spread,   twa,    leastsqr,   ts,          ts_dummy, tag_dummy, ts_comp
    4,         -1,       -1,         1,        1,      1,          1,           1,        1,     -1,
    //  tag,    colprj,   tagprj,    arithmetic, diff, first_dist, last_dist,   stddev_dst, interp    rate    irate
    1,          1,        1,         1,       -1,      1,          1,           1,          5,        1,      1,
    // tid_tag, derivative, blk_info
    6,          8,        7,
};

SAggFunctionInfo aggFunc[35] = {{
                              // 0, count function does not invoke the finalize function
                              "count",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_COUNT,
                              FUNCTION_COUNT,
                              BASIC_FUNC_SO,
                              function_setup,
                              count_function,
                              doFinalizer,
                              count_func_merge,
                              countRequired,
                          },
                          {
                              // 1
                              "sum",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_SUM,
                              FUNCTION_SUM,
                              BASIC_FUNC_SO,
                              function_setup,
                              sum_function,
                              function_finalizer,
                              sum_func_merge,
                              statisRequired,
                          },
                          {
                              // 2
                              "avg",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_AVG,
                              FUNCTION_AVG,
                              BASIC_FUNC_SO,
                              function_setup,
                              avg_function,
                              avg_finalizer,
                              avg_func_merge,
                              statisRequired,
                          },
                          {
                              // 3
                              "min",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_MIN,
                              FUNCTION_MIN,
                              BASIC_FUNC_SO | FUNCSTATE_SELECTIVITY,
                              min_func_setup,
                              NULL,
                              function_finalizer,
                              min_func_merge,
                              statisRequired,
                          },
                          {
                              // 4
                              "max",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_MAX,
                              FUNCTION_MAX,
                              BASIC_FUNC_SO | FUNCSTATE_SELECTIVITY,
                              max_func_setup,
                              NULL,
                              function_finalizer,
                              max_func_merge,
                              statisRequired,
                          },
                          {
                              // 5
                              "stddev",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_STDDEV,
                              FUNCTION_STDDEV_DST,
                              FUNCSTATE_SO | FUNCSTATE_STREAM,
                              function_setup,
                              stddev_function,
                              stddev_finalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                              // 6
                              "percentile",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_PERCT,
                              FUNCTION_INVALID_ID,
                              FUNCSTATE_SO | FUNCSTATE_STREAM,
                              percentile_function_setup,
                              percentile_function,
                              percentile_finalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                              // 7
                              "apercentile",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_APERCT,
                              FUNCTION_APERCT,
                              FUNCSTATE_SO | FUNCSTATE_STREAM | FUNCSTATE_STABLE,
                              apercentile_function_setup,
                              apercentile_function,
                              apercentile_finalizer,
                              apercentile_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 8
                              "first",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_FIRST,
                              FUNCTION_FIRST_DST,
                              BASIC_FUNC_SO | FUNCSTATE_SELECTIVITY,
                              function_setup,
                              first_function,
                              function_finalizer,
                              noop1,
                              firstFuncRequired,
                          },
                          {
                              // 9
                              "last",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_LAST,
                              FUNCTION_LAST_DST,
                              BASIC_FUNC_SO | FUNCSTATE_SELECTIVITY,
                              function_setup,
                              last_function,
                              function_finalizer,
                              noop1,
                              lastFuncRequired,
                          },
                          {
                              // 10
                              "last_row",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_LAST_ROW,
                              FUNCTION_LAST_ROW,
                              FUNCSTATE_SO | FUNCSTATE_STABLE | FUNCSTATE_NEED_TS | FUNCSTATE_SELECTIVITY,
                              first_last_function_setup,
                              last_row_function,
                              last_row_finalizer,
                              last_dist_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 11
                              "top",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_TOP,
                              FUNCTION_TOP,
                              FUNCSTATE_MO | FUNCSTATE_STABLE | FUNCSTATE_NEED_TS | FUNCSTATE_SELECTIVITY,
                              top_bottom_function_setup,
                              top_function,
                              top_bottom_func_finalizer,
                              top_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 12
                              "bottom",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_BOTTOM,
                              FUNCTION_BOTTOM,
                              FUNCSTATE_MO | FUNCSTATE_STABLE | FUNCSTATE_NEED_TS | FUNCSTATE_SELECTIVITY,
                              top_bottom_function_setup,
                              bottom_function,
                              top_bottom_func_finalizer,
                              bottom_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 13
                              "spread",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_SPREAD,
                              FUNCTION_SPREAD,
                              BASIC_FUNC_SO,
                              spread_function_setup,
                              spread_function,
                              spread_function_finalizer,
                              spread_func_merge,
                              countRequired,
                          },
                          {
                              // 14
                              "twa",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_TWA,
                              FUNCTION_TWA,
                              BASIC_FUNC_SO | FUNCSTATE_NEED_TS,
                              twa_function_setup,
                              twa_function,
                              twa_function_finalizer,
                              twa_function_copy,
                              dataBlockRequired,
                          },
                          {
                              // 15
                              "leastsquares",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_LEASTSQR,
                              FUNCTION_INVALID_ID,
                              FUNCSTATE_SO | FUNCSTATE_STREAM,
                              leastsquares_function_setup,
                              leastsquares_function,
                              leastsquares_finalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                              // 16
                              "dummy",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_TS,
                              FUNCTION_TS,
                              BASIC_FUNC_SO | FUNCSTATE_NEED_TS,
                              function_setup,
                              date_col_output_function,
                              doFinalizer,
                              copy_function,
                              noDataRequired,
                          },
                          {
                              // 17
                              "ts",
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_TS_DUMMY,
                              FUNCTION_TS_DUMMY,
                              BASIC_FUNC_SO | FUNCSTATE_NEED_TS,
                              function_setup,
                              noop1,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 18
                              "tag_dummy",
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_TAG_DUMMY,
                              FUNCTION_TAG_DUMMY,
                              BASIC_FUNC_SO,
                              function_setup,
                              tag_function,
                              doFinalizer,
                              copy_function,
                              noDataRequired,
                          },
                          {
                              // 19
                              "ts",
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_TS_COMP,
                              FUNCTION_TS_COMP,
                              FUNCSTATE_MO | FUNCSTATE_NEED_TS,
                              ts_comp_function_setup,
                              ts_comp_function,
                              ts_comp_finalize,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 20
                              "tag",
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_TAG,
                              FUNCTION_TAG,
                              BASIC_FUNC_SO,
                              function_setup,
                              tag_function,
                              doFinalizer,
                              copy_function,
                              noDataRequired,
                          },
                          {//TODO this is a scala function
                              // 21, column project sql function
                              "colprj",
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_PRJ,
                              FUNCTION_PRJ,
                              BASIC_FUNC_MO | FUNCSTATE_NEED_TS,
                              function_setup,
                              col_project_function,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 22, multi-output, tag function has only one result
                              "tagprj",
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_TAGPRJ,
                              FUNCTION_TAGPRJ,
                              BASIC_FUNC_MO,
                              function_setup,
                              tag_project_function,
                              doFinalizer,
                              copy_function,
                              noDataRequired,
                          },
                          {
                              // 23
                              "arithmetic",
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_ARITHM,
                              FUNCTION_ARITHM,
                              FUNCSTATE_MO | FUNCSTATE_STABLE | FUNCSTATE_NEED_TS,
                              function_setup,
                              arithmetic_function,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 24
                              "diff",
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_DIFF,
                              FUNCTION_INVALID_ID,
                              FUNCSTATE_MO | FUNCSTATE_STABLE | FUNCSTATE_NEED_TS | FUNCSTATE_SELECTIVITY,
                              diff_function_setup,
                              diff_function,
                              doFinalizer,
                              noop1,
                              dataBlockRequired,
                          },
    // distributed version used in two-stage aggregation processes
                          {
                              // 25
                              "first_dist",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_FIRST_DST,
                              FUNCTION_FIRST_DST,
                              BASIC_FUNC_SO | FUNCSTATE_NEED_TS | FUNCSTATE_SELECTIVITY,
                              first_last_function_setup,
                              first_dist_function,
                              function_finalizer,
                              first_dist_func_merge,
                              firstDistFuncRequired,
                          },
                          {
                              // 26
                              "last_dist",
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_LAST_DST,
                              FUNCTION_LAST_DST,
                              BASIC_FUNC_SO | FUNCSTATE_NEED_TS | FUNCSTATE_SELECTIVITY,
                              first_last_function_setup,
                              last_dist_function,
                              function_finalizer,
                              last_dist_func_merge,
                              lastDistFuncRequired,
                          },
                          {
                              // 27
                              "stddev",   // return table id and the corresponding tags for join match and subscribe
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_STDDEV_DST,
                              FUNCTION_AVG,
                              FUNCSTATE_SO | FUNCSTATE_STABLE,
                              function_setup,
                              stddev_dst_function,
                              stddev_dst_finalizer,
                              stddev_dst_merge,
                              dataBlockRequired,
                          },
                          {
                              // 28
                              "interp",
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_INTERP,
                              FUNCTION_INTERP,
                              FUNCSTATE_SO | FUNCSTATE_STABLE | FUNCSTATE_NEED_TS ,
                              function_setup,
                              interp_function,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 29
                              "rate",
                                    FUNCTION_TYPE_AGG,
                                    FUNCTION_RATE,
                              FUNCTION_RATE,
                              BASIC_FUNC_SO | FUNCSTATE_NEED_TS,
                              rate_function_setup,
                              rate_function,
                              rate_finalizer,
                              rate_func_copy,
                              dataBlockRequired,
                          },
                          {
                              // 30
                              "irate",
                              FUNCTION_TYPE_AGG,
                              FUNCTION_IRATE,
                              FUNCTION_IRATE,
                              BASIC_FUNC_SO | FUNCSTATE_NEED_TS,
                              rate_function_setup,
                              irate_function,
                              rate_finalizer,
                              rate_func_copy,
                              dataBlockRequired,
                          },
                          {
                              // 31
                              "tbid",   // return table id and the corresponding tags for join match and subscribe
                              FUNCTION_TYPE_AGG,
                              FUNCTION_TID_TAG,
                              FUNCTION_TID_TAG,
                              FUNCSTATE_MO | FUNCSTATE_STABLE,
                              function_setup,
                              noop1,
                              noop1,
                              noop1,
                              dataBlockRequired,
                          },
                          {   //32
                              "derivative",   // return table id and the corresponding tags for join match and subscribe
                              FUNCTION_TYPE_AGG,
                              FUNCTION_DERIVATIVE,
                              FUNCTION_INVALID_ID,
                              FUNCSTATE_MO | FUNCSTATE_STABLE | FUNCSTATE_NEED_TS | FUNCSTATE_SELECTIVITY,
                              deriv_function_setup,
                              deriv_function,
                              doFinalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                                // 33
                              "block_dist",   // return table id and the corresponding tags for join match and subscribe
                              FUNCTION_TYPE_AGG,
                              FUNCTION_BLKINFO,
                              FUNCTION_BLKINFO,
                              FUNCSTATE_SO | FUNCSTATE_STABLE,
                              function_setup,
                              blockInfo_func,
                              blockinfo_func_finalizer,
                              block_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 34
                              "cov",   // return table id and the corresponding tags for join match and subscribe
                              FUNCTION_TYPE_AGG,
                              FUNCTION_COV,
                              FUNCTION_COV,
                              FUNCSTATE_SO | FUNCSTATE_STABLE,
                              function_setup,
                              sum_function,
                              function_finalizer,
                              sum_func_merge,
                              statisRequired,
                          }
                          };

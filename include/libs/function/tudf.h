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

#ifndef TDENGINE_TUDF_H
#define TDENGINE_TUDF_H


#include <stdint.h>
#include <stdbool.h>
#include "tmsg.h"
#include "tcommon.h"
#include "function.h"
#include "tdatablock.h"

#ifdef __cplusplus
extern "C" {
#endif

#define UDF_LISTEN_PIPE_NAME_LEN 32
#ifdef _WIN32
#define UDF_LISTEN_PIPE_NAME_PREFIX "\\\\?\\pipe\\udfd.sock"
#else
#define UDF_LISTEN_PIPE_NAME_PREFIX ".udfd.sock."
#endif
#define UDF_DNODE_ID_ENV_NAME "DNODE_ID"

//======================================================================================
//begin API to taosd and qworker

typedef struct SUdfColumnMeta {
  int16_t type;
  int32_t bytes;
  uint8_t precision;
  uint8_t scale;
} SUdfColumnMeta;

typedef struct SUdfColumnData {
  int32_t numOfRows;
  int32_t rowsAlloc;
  union {
    struct {
      int32_t nullBitmapLen;
      char   *nullBitmap;
      int32_t dataLen;
      char   *data;
    } fixLenCol;

    struct {
      int32_t varOffsetsLen;
      int32_t   *varOffsets;
      int32_t payloadLen;
      char   *payload;
      int32_t payloadAllocLen;
    } varLenCol;
  };
} SUdfColumnData;


typedef struct SUdfColumn {
  SUdfColumnMeta colMeta;
  bool           hasNull;
  SUdfColumnData colData;
} SUdfColumn;

typedef struct SUdfDataBlock {
  int32_t numOfRows;
  int32_t numOfCols;
  SUdfColumn **udfCols;
} SUdfDataBlock;

typedef struct SUdfInterBuf {
  int32_t bufLen;
  char* buf;
  int8_t numOfResult; //zero or one
} SUdfInterBuf;
typedef void *UdfcFuncHandle;

/**
 * setup udf
 * @param udf, in
 * @param funcHandle, out
 * @return error code
 */
int32_t doSetupUdf(char udfName[], UdfcFuncHandle *funcHandle);
// output: interBuf
int32_t doCallUdfAggInit(UdfcFuncHandle handle, SUdfInterBuf *interBuf);
// input: block, state
// output: newState
int32_t doCallUdfAggProcess(UdfcFuncHandle handle, SSDataBlock *block, SUdfInterBuf *state, SUdfInterBuf *newState);
// input: interBuf
// output: resultData
int32_t doCallUdfAggFinalize(UdfcFuncHandle handle, SUdfInterBuf *interBuf, SUdfInterBuf *resultData);
// input: interbuf1, interbuf2
// output: resultBuf
int32_t doCallUdfAggMerge(UdfcFuncHandle handle, SUdfInterBuf *interBuf1, SUdfInterBuf *interBuf2, SUdfInterBuf *resultBuf);
// input: block
// output: resultData
int32_t doCallUdfScalarFunc(UdfcFuncHandle handle, SScalarParam *input, int32_t numOfCols, SScalarParam *output);
/**
 * tearn down udf
 * @param handle
 * @return
 */
int32_t doTeardownUdf(UdfcFuncHandle handle);

bool udfAggGetEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
bool udfAggInit(struct SqlFunctionCtx *pCtx, struct SResultRowEntryInfo* pResultCellInfo);
int32_t udfAggProcess(struct SqlFunctionCtx *pCtx);
int32_t udfAggFinalize(struct SqlFunctionCtx *pCtx, SSDataBlock* pBlock);

int32_t callUdfScalarFunc(char *udfName, SScalarParam *input, int32_t numOfCols, SScalarParam *output);
// end API to taosd and qworker
//=============================================================================================================================
// begin API to UDF writer.

// dynamic lib init and destroy
typedef int32_t (*TUdfInitFunc)();
typedef int32_t (*TUdfDestroyFunc)();

//TODO: add API to check function arguments type, number etc.

#define UDF_MEMORY_EXP_GROWTH 1.5

#define udfColDataIsNull_var(pColumn, row) ((pColumn->colData.varLenCol.varOffsets)[row] == -1)
#define udfColDataIsNull_f(pColumn, row) ((BMCharPos(pColumn->colData.fixLenCol.nullBitmap, row) & (1u << (7u - BitPos(row)))) == (1u << (7u - BitPos(row))))
#define udfColDataSetNull_f(pColumn, row)                                                \
  do {                                                                                   \
    BMCharPos(pColumn->colData.fixLenCol.nullBitmap, row) |= (1u << (7u - BitPos(row))); \
  } while (0)

#define udfColDataSetNotNull_f(pColumn, r_)                                               \
  do {                                                                                    \
    BMCharPos(pColumn->colData.fixLenCol.nullBitmap, r_) &= ~(1u << (7u - BitPos(r_)));   \
  } while (0)
#define udfColDataSetNull_var(pColumn, row)  ((pColumn->colData.varLenCol.varOffsets)[row] = -1)


static FORCE_INLINE char* udfColDataGetData(const SUdfColumn* pColumn, int32_t row) {
  if (IS_VAR_DATA_TYPE(pColumn->colMeta.type)) {
    return pColumn->colData.varLenCol.payload + pColumn->colData.varLenCol.varOffsets[row];
  } else {
    return pColumn->colData.fixLenCol.data + pColumn->colMeta.bytes * row;
  }
}

static FORCE_INLINE bool udfColDataIsNull(const SUdfColumn* pColumn, int32_t row) {
  if (IS_VAR_DATA_TYPE(pColumn->colMeta.type)) {
    if (pColumn->colMeta.type == TSDB_DATA_TYPE_JSON) {
      if (udfColDataIsNull_var(pColumn, row)) {
        return true;
      }
      char* data = udfColDataGetData(pColumn, row);
      return (*data == TSDB_DATA_TYPE_NULL);
    } else {
      return udfColDataIsNull_var(pColumn, row);
    }
  } else {
    return udfColDataIsNull_f(pColumn, row);
  }
}

static FORCE_INLINE int32_t udfColEnsureCapacity(SUdfColumn* pColumn, int32_t newCapacity) {
  SUdfColumnMeta *meta = &pColumn->colMeta;
  SUdfColumnData *data = &pColumn->colData;

  if (newCapacity== 0 || newCapacity <= data->rowsAlloc) {
    return TSDB_CODE_SUCCESS;
  }

  int allocCapacity = TMAX(data->rowsAlloc, 8);
  while (allocCapacity < newCapacity) {
    allocCapacity *= UDF_MEMORY_EXP_GROWTH;
  }

  if (IS_VAR_DATA_TYPE(meta->type)) {
    char* tmp = taosMemoryRealloc(data->varLenCol.varOffsets, sizeof(int32_t) * allocCapacity);
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    data->varLenCol.varOffsets = (int32_t*)tmp;
    data->varLenCol.varOffsetsLen = sizeof(int32_t) * allocCapacity;
    // for payload, add data in udfColDataAppend
  } else {
    char* tmp = taosMemoryRealloc(data->fixLenCol.nullBitmap, BitmapLen(allocCapacity));
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    data->fixLenCol.nullBitmap = tmp;
    data->fixLenCol.nullBitmapLen = BitmapLen(allocCapacity);
    if (meta->type == TSDB_DATA_TYPE_NULL) {
      return TSDB_CODE_SUCCESS;
    }

    tmp = taosMemoryRealloc(data->fixLenCol.data, allocCapacity* meta->bytes);
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    data->fixLenCol.data = tmp;
    data->fixLenCol.dataLen = allocCapacity* meta->bytes;
  }

  data->rowsAlloc = allocCapacity;

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void udfColDataSetNull(SUdfColumn* pColumn, int32_t row) {
  udfColEnsureCapacity(pColumn, row+1);
  if (IS_VAR_DATA_TYPE(pColumn->colMeta.type)) {
    udfColDataSetNull_var(pColumn, row);
  } else {
    udfColDataSetNull_f(pColumn, row);
  }
  pColumn->hasNull = true;
}

static FORCE_INLINE int32_t udfColDataSet(SUdfColumn* pColumn, uint32_t currentRow, const char* pData, bool isNull) {
  SUdfColumnMeta *meta = &pColumn->colMeta;
  SUdfColumnData *data = &pColumn->colData;
  udfColEnsureCapacity(pColumn, currentRow+1);
  bool isVarCol = IS_VAR_DATA_TYPE(meta->type);
  if (isNull) {
      udfColDataSetNull(pColumn, currentRow);
  } else {
    if (!isVarCol) {
      colDataSetNotNull_f(data->fixLenCol.nullBitmap, currentRow);
      memcpy(data->fixLenCol.data + meta->bytes * currentRow, pData, meta->bytes);
    } else {
      int32_t dataLen = varDataTLen(pData);
      if (meta->type == TSDB_DATA_TYPE_JSON) {
        if (*pData == TSDB_DATA_TYPE_NULL) {
          dataLen = 0;
        } else if (*pData == TSDB_DATA_TYPE_NCHAR) {
          dataLen = varDataTLen(pData + CHAR_BYTES);
        } else if (*pData == TSDB_DATA_TYPE_BIGINT || *pData == TSDB_DATA_TYPE_DOUBLE) {
          dataLen = LONG_BYTES;
        } else if (*pData == TSDB_DATA_TYPE_BOOL) {
          dataLen = CHAR_BYTES;
        }
        dataLen += CHAR_BYTES;
      }

      if (data->varLenCol.payloadAllocLen < data->varLenCol.payloadLen + dataLen) {
        uint32_t newSize = data->varLenCol.payloadAllocLen;
        if (newSize <= 1) {
          newSize = 8;
        }

        while (newSize < data->varLenCol.payloadLen + dataLen) {
          newSize = newSize * UDF_MEMORY_EXP_GROWTH;
        }

        char *buf = taosMemoryRealloc(data->varLenCol.payload, newSize);
        if (buf == NULL) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }

        data->varLenCol.payload = buf;
        data->varLenCol.payloadAllocLen = newSize;
      }

      uint32_t len = data->varLenCol.payloadLen;
      data->varLenCol.varOffsets[currentRow] = len;

      memcpy(data->varLenCol.payload + len, pData, dataLen);
      data->varLenCol.payloadLen += dataLen;
    }
  }
  data->numOfRows = TMAX(currentRow + 1, data->numOfRows);
  return 0;
}

typedef int32_t (*TUdfScalarProcFunc)(SUdfDataBlock* block, SUdfColumn *resultCol);

typedef int32_t (*TUdfAggStartFunc)(SUdfInterBuf *buf);
typedef int32_t (*TUdfAggProcessFunc)(SUdfDataBlock* block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf);
typedef int32_t (*TUdfAggFinishFunc)(SUdfInterBuf* buf, SUdfInterBuf *resultData);


// end API to UDF writer
//=======================================================================================================================

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TUDF_H

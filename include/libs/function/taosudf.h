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

#ifndef TDENGINE_TAOSUDF_H
#define TDENGINE_TAOSUDF_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <taos.h>
#include <taoserror.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__GNUC__)
#define FORCE_INLINE inline __attribute__((always_inline))
#else
#define FORCE_INLINE
#endif
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
      int32_t  varOffsetsLen;
      int32_t *varOffsets;
      int32_t  payloadLen;
      char    *payload;
      int32_t  payloadAllocLen;
    } varLenCol;
  };
} SUdfColumnData;

typedef struct SUdfColumn {
  SUdfColumnMeta colMeta;
  bool           hasNull;
  SUdfColumnData colData;
} SUdfColumn;

typedef struct SUdfDataBlock {
  int32_t      numOfRows;
  int32_t      numOfCols;
  SUdfColumn **udfCols;
} SUdfDataBlock;

typedef struct SUdfInterBuf {
  int32_t bufLen;
  char   *buf;
  int8_t  numOfResult;  // zero or one
} SUdfInterBuf;
typedef void *UdfcFuncHandle;

#define UDF_MEMORY_EXP_GROWTH 1.5
#define NBIT                  (3u)
#define BitPos(_n)            ((_n) & ((1 << NBIT) - 1))
#define BMCharPos(bm_, r_)    ((bm_)[(r_) >> NBIT])
#define BitmapLen(_n)         (((_n) + ((1 << NBIT) - 1)) >> NBIT)

#define udfColDataIsNull_var(pColumn, row) ((pColumn->colData.varLenCol.varOffsets)[row] == -1)
#define udfColDataIsNull_f(pColumn, row) \
  ((BMCharPos(pColumn->colData.fixLenCol.nullBitmap, row) & (1u << (7u - BitPos(row)))) == (1u << (7u - BitPos(row))))
#define udfColDataSetNull_f(pColumn, row)                                                \
  do {                                                                                   \
    BMCharPos(pColumn->colData.fixLenCol.nullBitmap, row) |= (1u << (7u - BitPos(row))); \
  } while (0)

#define udfColDataSetNotNull_f(pColumn, r_)                                             \
  do {                                                                                  \
    BMCharPos(pColumn->colData.fixLenCol.nullBitmap, r_) &= ~(1u << (7u - BitPos(r_))); \
  } while (0)
#define udfColDataSetNull_var(pColumn, row) ((pColumn->colData.varLenCol.varOffsets)[row] = -1)

typedef uint16_t VarDataLenT;  // maxVarDataLen: 65535
#define VARSTR_HEADER_SIZE     sizeof(VarDataLenT)
#define varDataLen(v)          ((VarDataLenT *)(v))[0]
#define varDataVal(v)          ((char *)(v) + VARSTR_HEADER_SIZE)
#define varDataTLen(v)         (sizeof(VarDataLenT) + varDataLen(v))
#define varDataCopy(dst, v)    memcpy((dst), (void *)(v), varDataTLen(v))
#define varDataLenByData(v)    (*(VarDataLenT *)(((char *)(v)) - VARSTR_HEADER_SIZE))
#define varDataSetLen(v, _len) (((VarDataLenT *)(v))[0] = (VarDataLenT)(_len))
#define IS_VAR_DATA_TYPE(t) \
  (((t) == TSDB_DATA_TYPE_VARCHAR) || ((t) == TSDB_DATA_TYPE_VARBINARY) || ((t) == TSDB_DATA_TYPE_NCHAR) || ((t) == TSDB_DATA_TYPE_JSON) || ((t) == TSDB_DATA_TYPE_GEOMETRY))
#define IS_STR_DATA_TYPE(t) (((t) == TSDB_DATA_TYPE_VARCHAR) || ((t) == TSDB_DATA_TYPE_VARBINARY) || ((t) == TSDB_DATA_TYPE_NCHAR))

static FORCE_INLINE char *udfColDataGetData(const SUdfColumn *pColumn, int32_t row) {
  if (IS_VAR_DATA_TYPE(pColumn->colMeta.type)) {
    return pColumn->colData.varLenCol.payload + pColumn->colData.varLenCol.varOffsets[row];
  } else {
    return pColumn->colData.fixLenCol.data + pColumn->colMeta.bytes * row;
  }
}

static FORCE_INLINE bool udfColDataIsNull(const SUdfColumn *pColumn, int32_t row) {
  if (IS_VAR_DATA_TYPE(pColumn->colMeta.type)) {
    if (pColumn->colMeta.type == TSDB_DATA_TYPE_JSON) {
      if (udfColDataIsNull_var(pColumn, row)) {
        return true;
      }
      char *data = udfColDataGetData(pColumn, row);
      return (*data == TSDB_DATA_TYPE_NULL);
    } else {
      return udfColDataIsNull_var(pColumn, row);
    }
  } else {
    return udfColDataIsNull_f(pColumn, row);
  }
}

static FORCE_INLINE int32_t udfColEnsureCapacity(SUdfColumn *pColumn, int32_t newCapacity) {
  SUdfColumnMeta *meta = &pColumn->colMeta;
  SUdfColumnData *data = &pColumn->colData;

  if (newCapacity == 0 || newCapacity <= data->rowsAlloc) {
    return TSDB_CODE_SUCCESS;
  }

  int allocCapacity = (data->rowsAlloc < 8) ? 8 : data->rowsAlloc;
  while (allocCapacity < newCapacity) {
    allocCapacity *= UDF_MEMORY_EXP_GROWTH;
  }

  int32_t existedRows = data->numOfRows;

  if (IS_VAR_DATA_TYPE(meta->type)) {
    char *tmp = (char *)realloc(data->varLenCol.varOffsets, sizeof(int32_t) * allocCapacity);
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    data->varLenCol.varOffsets = (int32_t *)tmp;
    data->varLenCol.varOffsetsLen = sizeof(int32_t) * allocCapacity;
    memset(&data->varLenCol.varOffsets[existedRows], 0, sizeof(int32_t) * (allocCapacity - existedRows));
    // for payload, add data in udfColDataAppend
  } else {
    char *tmp = (char *)realloc(data->fixLenCol.nullBitmap, BitmapLen(allocCapacity));
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    uint32_t extend = BitmapLen(allocCapacity) - BitmapLen(data->rowsAlloc);
    memset(tmp + BitmapLen(data->rowsAlloc), 0, extend);
    data->fixLenCol.nullBitmap = tmp;
    data->fixLenCol.nullBitmapLen = BitmapLen(allocCapacity);
    int32_t oldLen = BitmapLen(existedRows);
    memset(&data->fixLenCol.nullBitmap[oldLen], 0, BitmapLen(allocCapacity) - oldLen);

    if (meta->type == TSDB_DATA_TYPE_NULL) {
      return TSDB_CODE_SUCCESS;
    }

    tmp = (char *)realloc(data->fixLenCol.data, allocCapacity * meta->bytes);
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    data->fixLenCol.data = tmp;
    data->fixLenCol.dataLen = allocCapacity * meta->bytes;
  }

  data->rowsAlloc = allocCapacity;

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void udfColDataSetNull(SUdfColumn *pColumn, int32_t row) {
  udfColEnsureCapacity(pColumn, row + 1);
  if (IS_VAR_DATA_TYPE(pColumn->colMeta.type)) {
    udfColDataSetNull_var(pColumn, row);
  } else {
    udfColDataSetNull_f(pColumn, row);
  }
  pColumn->hasNull = true;
  pColumn->colData.numOfRows = ((int32_t)(row + 1) > pColumn->colData.numOfRows) ? (int32_t)(row + 1) : pColumn->colData.numOfRows;
}

static FORCE_INLINE int32_t udfColDataSet(SUdfColumn *pColumn, uint32_t currentRow, const char *pData, bool isNull) {
  SUdfColumnMeta *meta = &pColumn->colMeta;
  SUdfColumnData *data = &pColumn->colData;
  udfColEnsureCapacity(pColumn, currentRow + 1);
  bool isVarCol = IS_VAR_DATA_TYPE(meta->type);
  if (isNull) {
    udfColDataSetNull(pColumn, currentRow);
  } else {
    if (!isVarCol) {
      udfColDataSetNotNull_f(pColumn, currentRow);
      memcpy(data->fixLenCol.data + meta->bytes * currentRow, pData, meta->bytes);
    } else {
      int32_t dataLen = varDataTLen(pData);
      if (meta->type == TSDB_DATA_TYPE_JSON) {
        if (*pData == TSDB_DATA_TYPE_NULL) {
          dataLen = 0;
        } else if (*pData == TSDB_DATA_TYPE_NCHAR) {
          dataLen = varDataTLen(pData + sizeof(char));
        } else if (*pData == TSDB_DATA_TYPE_BIGINT || *pData == TSDB_DATA_TYPE_DOUBLE) {
          dataLen = sizeof(int64_t);
        } else if (*pData == TSDB_DATA_TYPE_BOOL) {
          dataLen = sizeof(char);
        }
        dataLen += sizeof(char);
      }

      if (data->varLenCol.payloadAllocLen < data->varLenCol.payloadLen + dataLen) {
        uint32_t newSize = data->varLenCol.payloadAllocLen;
        if (newSize <= 1) {
          newSize = 8;
        }

        while (newSize < (uint32_t)(data->varLenCol.payloadLen + dataLen)) {
          newSize = newSize * UDF_MEMORY_EXP_GROWTH;
        }

        char *buf = (char *)realloc(data->varLenCol.payload, newSize);
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
  data->numOfRows = ((int32_t)(currentRow + 1) > data->numOfRows) ? (int32_t)(currentRow + 1) : data->numOfRows;
  return 0;
}

// dynamic lib init and destroy for C UDF
typedef int32_t (*TUdfInitFunc)();
typedef int32_t (*TUdfDestroyFunc)();

typedef int32_t (*TUdfScalarProcFunc)(SUdfDataBlock *block, SUdfColumn *resultCol);

typedef int32_t (*TUdfAggStartFunc)(SUdfInterBuf *buf);
typedef int32_t (*TUdfAggProcessFunc)(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf);
typedef int32_t (*TUdfAggMergeFunc)(SUdfInterBuf *inputBuf1, SUdfInterBuf *inputBuf2, SUdfInterBuf *outputBuf);
typedef int32_t (*TUdfAggFinishFunc)(SUdfInterBuf *buf, SUdfInterBuf *resultData);

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
typedef struct SScriptUdfEnvItem {
  const char *name;
  const char *value;
} SScriptUdfEnvItem;

typedef enum EUdfFuncType { UDF_FUNC_TYPE_SCALAR = 1, UDF_FUNC_TYPE_AGG = 2 } EUdfFuncType;

typedef struct SScriptUdfInfo {
  const char *name;
  int32_t version;
  int64_t createdTime;

  EUdfFuncType funcType;
  int8_t       scriptType;
  int8_t       outputType;
  int32_t      outputLen;
  int32_t      bufSize;

  const char *path;
} SScriptUdfInfo;

typedef int32_t (*TScriptUdfScalarProcFunc)(SUdfDataBlock *block, SUdfColumn *resultCol, void *udfCtx);

typedef int32_t (*TScriptUdfAggStartFunc)(SUdfInterBuf *buf, void *udfCtx);
typedef int32_t (*TScriptUdfAggProcessFunc)(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf,
                                            void *udfCtx);
typedef int32_t (*TScriptUdfAggMergeFunc)(SUdfInterBuf *inputBuf1, SUdfInterBuf *inputBuf2, SUdfInterBuf *outputBuf,
                                          void *udfCtx);
typedef int32_t (*TScriptUdfAggFinishFunc)(SUdfInterBuf *buf, SUdfInterBuf *resultData, void *udfCtx);
typedef int32_t (*TScriptUdfInitFunc)(SScriptUdfInfo *info, void **pUdfCtx);
typedef int32_t (*TScriptUdfDestoryFunc)(void *udfCtx);

// the following function is for open/close script plugin.
typedef int32_t (*TScriptOpenFunc)(SScriptUdfEnvItem *items, int numItems);
typedef int32_t (*TScriptCloseFunc)();

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TAOSUDF_H

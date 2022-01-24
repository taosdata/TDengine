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

#ifndef _TD_COMMON_ROW_H_
#define _TD_COMMON_ROW_H_

#include "os.h"
#include "tdef.h"
#include "taoserror.h"
#include "talgo.h"
#include "tbuffer.h"
#include "tdataformat.h"
#include "tdef.h"
#include "tschema.h"
#include "ttypes.h"

#ifdef __cplusplus
extern "C" {
#endif

// Target of trow.h:
// 1. Row related definition in dataformat.h of 2.0 could be replaced with trow.h of 3.0.
// 2. The basic definition in dataformat.h is shared with trow.h of 3.0.

// row type
#define TD_ROW_TP 0x0  // default
#define TD_ROW_KV 0x01

// val type
#define TD_VTYPE_NORM 0x0   // normal val: not none, not null
#define TD_VTYPE_NONE 0x01  // none or unknown/undefined
#define TD_VTYPE_NULL 0x02  // null val

#define isSelectKVRow(klen, tlen) ((klen) < (tlen))

#ifdef TD_SUPPORT_BITMAP
static FORCE_INLINE bool tdValTypeIsNorm(int8_t valType) { return (valType & TD_VTYPE_NORM); }
static FORCE_INLINE bool tdValTypeIsNone(int8_t valType) { return (valType & TD_VTYPE_NONE); }
static FORCE_INLINE bool tdValTypeIsNull(int8_t valType) { return (valType & TD_VTYPE_NULL); }
#endif

static FORCE_INLINE bool tdValIsNorm(int8_t valType, const void *val, int32_t colType) {
#ifdef TD_SUPPORT_BITMAP
  return tdValTypeIsNorm(valType);
#else
  return !isNull(val, colType);
#endif
}

static FORCE_INLINE bool tdValIsNone(int8_t valType) {
#ifdef TD_SUPPORT_BITMAP
  return tdValTypeIsNone(valType);
#else
  return false;
#endif
}

static FORCE_INLINE bool tdValIsNull(int8_t valType, const void *val, int32_t colType) {
#ifdef TD_SUPPORT_BITMAP
  return tdValTypeIsNull(valType);
#else
  return isNull(val, colType);
#endif
}

typedef void *SRow;

typedef struct {
  int8_t valType;
  void * val;
} SCellVal;

typedef struct {
  // TODO
} STpRow;  // tuple

typedef struct {
  col_id_t cid;
  uint32_t offset;
} SKvRowIdx;

typedef struct {
  uint16_t  ncols;
  SKvRowIdx cidx[];
} SKvRow;

typedef struct {
  union {
    /// union field for encode and decode
    uint32_t info;
    struct {
      /// row type
      uint32_t type : 2;
      /// is delete row(0 not delete, 1 delete)
      uint32_t del : 1;
      /// endian(0 little endian, 1 big endian)
      uint32_t endian : 1;
      /// reserved for back compatibility
      uint32_t reserve : 12;
      /// row schema version
      uint32_t sver : 16;
    };
  };
  /// row total length
  uint32_t len;
  /// nCols of SRow(only valid for K-V row)
  uint64_t ncols : 16;
  /// row version
  uint64_t ver : 48;
  /// timestamp
  TSKEY ts;
  /// the inline data, maybe a tuple or a k-v tuple
  char data[];
} STSRow;

#define TD_ROW_HEAD_LEN (sizeof(STSRow))

#define TD_ROW_TYPE(r) ((r)->type)
#define TD_ROW_DELETE(r) ((r)->del)
#define TD_ROW_ENDIAN(r) ((r)->endian)
#define TD_ROW_SVER(r) ((r)->sver)
#define TD_ROW_NCOLS(r) ((r)->ncols)
#define TD_ROW_DATA(r) ((r)->data)
#define TD_ROW_LEN(r) ((r)->len)
#define TD_ROW_TSKEY(r) ((r)->ts)

// N.B. If without STSchema, getExtendedRowSize() is used to get the rowMaxBytes and (int)ceil((double)nCols/TD_VTYPE_PARTS)
// should be added if TD_SUPPORT_BITMAP defined.
#define TD_ROW_MAX_BYTES_FROM_SCHEMA(s) (schemaTLen(s) + TD_ROW_HEAD_LEN)

#define TD_ROW_SET_TYPE(r, t) (TD_ROW_TYPE(r) = (t))
#define TD_ROW_SET_DELETE(r) (TD_ROW_DELETE(r) = 1)
#define TD_ROW_SET_SVER(r, v) (TD_ROW_SVER(r) = (v))
#define TD_ROW_SET_LEN(r, l) (TD_ROW_LEN(r) = (l))
#define TD_ROW_CPY(dst, r) memcpy((dst), (r), TD_ROW_LEN(r))

#define TD_ROW_IS_DELETED(r) (TD_ROW_DELETE(r))
#define TD_IS_TP_ROW(r) (TD_ROW_TYPE(r) == TD_ROW_TP)
#define TD_IS_KV_ROW(r) (TD_ROW_TYPE(r) == TD_ROW_KV)
#define TD_BOOL_STR(b) ((b) ? "true" : "false")
#define isUtilizeKVRow(k, d) ((k) < ((d)*KVRatioConvert))

#define TD_ROW_OFFSET(p) ((p)->toffset);

#ifdef TD_SUPPORT_BITMAP
static FORCE_INLINE void *tdRowBitmap(STSRow *pRow, uint16_t flen) {
  switch (pRow->type) {
    case TD_ROW_TP:
      return POINTER_SHIFT(pRow->data, flen);
    case TD_ROW_KV:
      return POINTER_SHIFT(pRow->data, pRow->ncols * sizeof(SKvRowIdx));
    default:
      break;
  }
  return NULL;
}
#endif

STpRow tdNewTpRowFromSchema(STSchema *pSchema);
void   tdFreeTpRow(STpRow row);
void   tdInitTpRow(STpRow row, STSchema *pSchema);
STpRow tdTpRowDup(STpRow row);

// ----------------- Tuple row structure(STpRow)
/*
 * |<----------------------------- tlen ---------------------------------->|
 * |<---------   flen  ------------->|<-- blen  -->|                       |
 * +---------------------------------+-------------+-----------------------+
 * |                                 |             |                       |
 * +---------------------------------+-------------------------------------+
 * |           first part            |   bitmap    |    second part        |
 * +---------------------------------+-------------+-----------------------+
 *
 */

// -----------------  K-V row structure(SKvRow)
/*
 * |<--------- colsIdxLen ---------->|<-- blen -->|                        |
 * +---------------------------------+------------+------------------------+
 * |                                 |            |                        |
 * +-----------------------------------------------------------------------+
 * |           cols index            |   bitmap   |     data part          |
 * +---------------------------------+------------+------------------------+
 *
 */
typedef struct {
  // necessary info
  int8_t  rowType;
  int16_t sver;
  STSRow *pBuf;

  // auxiliary info
  int32_t flen;
  int16_t nBoundCols;
  int16_t nCols;
  int16_t nBitmaps;
  int16_t nBoundBitmaps;
  int32_t offset;
  void *  pBitmap;
  void *  pOffset;
} SRowBuilder;

/**
 * @brief
 *
 * @param pBuilder
 * @param sversion schema version
 * @return int32_t
 */
int32_t tdSRowInit(SRowBuilder *pBuilder, int16_t sversion) {
  pBuilder->rowType = TD_ROW_TP;
  pBuilder->sver = sversion;
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief 一般不建议使用，除非特殊情况
 *
 * @param pBuilder
 * @param rowType
 * @return FORCE_INLINE
 */
static FORCE_INLINE void tdSRowSetRowType(SRowBuilder *pBuilder, int8_t rowType) { pBuilder->rowType = rowType; }

/**
 * @brief 用于判定采用STpRow/SKvRow，
 *
 * @param pBuilder
 * @param allNullLen 无法获取则填写-1
 * @param boundNullLen 无法获取则填写-1
 * @param nCols
 * @param nBoundCols
 * @param flen
 * @return FORCE_INLINE
 */
static FORCE_INLINE int32_t tdSRowSetExtendedInfo(SRowBuilder *pBuilder, int32_t allNullLen, int32_t boundNullLen,
                                                  int32_t nCols, int32_t nBoundCols, int32_t flen) {
  if ((boundNullLen > 0) && (allNullLen > 0) && isSelectKVRow(boundNullLen, allNullLen)) {
    pBuilder->rowType = TD_ROW_KV;
  }
  pBuilder->nBoundCols = nBoundCols;
  pBuilder->nCols = nCols;
  pBuilder->flen = flen;
  if (pBuilder->flen <= 0 || pBuilder->nCols <= 0 || pBuilder->nBoundCols <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return TSDB_CODE_INVALID_PARA;
  }
#ifdef TD_SUPPORT_BITMAP
  pBuilder->nBitmaps = (int16_t)ceil((double)pBuilder->nCols / TD_VTYPE_PARTS);
  pBuilder->nBoundBitmaps = (int16_t)ceil((double)pBuilder->nBoundCols / TD_VTYPE_PARTS);
#else
  pBuilder->nBitmaps = 0;
  pBuilder->nBoundBitmaps = 0;
#endif
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief 在pBuf位置生成SRow
 *
 * @param pBuilder
 * @param pBuf
 */
int32_t tdSRowResetBuf(SRowBuilder *pBuilder, void *pBuf) {
  pBuilder->pBuf = pBuf;
  if (!pBuilder->pBuf) {
    terrno = TSDB_CODE_INVALID_PARA;
    return TSDB_CODE_INVALID_PARA;
  }
  switch (pBuilder->rowType) {
    case TD_ROW_TP:
#ifdef TD_SUPPORT_BITMAP
      pBuilder->pBitmap = POINTER_SHIFT(pBuilder->pBuf, pBuilder->flen);
#endif
      uint32_t len = TD_ROW_HEAD_LEN + pBuilder->flen + pBuilder->nBitmaps;
      TD_ROW_SET_LEN(pBuilder->pBuf, len);
      TD_ROW_SET_SVER(pBuilder->pBuf, pBuilder->sver);
      break;
    case TD_ROW_KV:
      uint32_t len = pBuilder->nBoundCols * sizeof(SKvRowIdx);
#ifdef TD_SUPPORT_BITMAP
      pBuilder->pBitmap = POINTER_SHIFT(pBuilder->pBuf, len);
#endif
      len += (TD_ROW_HEAD_LEN + pBuilder->nBoundBitmaps);
      TD_ROW_SET_LEN(pBuilder->pBuf, len);
      TD_ROW_SET_SVER(pBuilder->pBuf, pBuilder->sver);
      break;
    default:
      terrno = TSDB_CODE_INVALID_PARA;
      return TSDB_CODE_INVALID_PARA;
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief 由调用方管理存储空间的分配及释放，一次输入多个参数
 *
 * @param pBuilder
 * @param pBuf
 * @param allNullLen
 * @param boundNullLen
 * @param nCols
 * @param nBoundCols
 * @param flen
 * @return FORCE_INLINE
 */
static FORCE_INLINE int32_t tdSRowInitEx(SRowBuilder *pBuilder, void *pBuf, uint32_t allNullLen, uint32_t boundNullLen,
                                         int32_t nCols, int32_t nBoundCols, int32_t flen) {
  
  tdSRowSetExtendedInfo(pBuilder, allNullLen, boundNullLen, nCols, nBoundCols, flen);
  return tdSRowResetBuf(pBuilder, pBuf);
}

/**
 * @brief
 *
 * @param pBuilder
 */
void tdSRowReset(SRowBuilder *pBuilder) {
  pBuilder->rowType = TD_ROW_TP;
  pBuilder->pBuf = NULL;
  pBuilder->nBoundCols = -1;
  pBuilder->nCols = -1;
  pBuilder->flen = -1;
  pBuilder->pBitmap = NULL;
}

/**
 * @brief
 *
 * @param pBuilder
 * @param colId start from PRIMARYKEY_TIMESTAMP_COL_ID
 * @param colType
 * @param val
 * @param valType
 * @param offset
 * @param idx sorted column index, start from 0
 * @return FORCE_INLINE
 */
static FORCE_INLINE int32_t tdAppendColValToRow(SRowBuilder *pBuilder, int16_t colId, int8_t colType, const void *val,
                                                int8_t valType, int32_t offset, int16_t idx) {
  STSRow *pRow = pBuilder->pBuf;
  void *  pBitmap = NULL;
  if (!val) {
#ifdef TD_SUPPORT_BITMAP
    if (tdValIsNorm(valType, val, colType)) {
      terrno = TSDB_CODE_INVALID_PTR;
      return terrno;
    }
#else
    terrno = TSDB_CODE_INVALID_PTR;
    return terrno;
#endif
  }
  // TS KEY is stored in STSRow.ts and not included in STSRow.data field.
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    TD_ROW_TSKEY(pRow) = *(TSKEY *)val;
#ifdef TD_SUPPORT_BITMAP
    pBitmap = tdRowBitmap(pRow, pBuilder->flen);
    if (tdSetBitmap(pBitmap, idx, valType) != TSDB_CODE_SUCCESS) {
      return terrno;
    }
#endif
    return TSDB_CODE_SUCCESS;
  }
  // TODO:  We can avoid the type judegement by FP, but would prevent the inline scheme.
  // typedef int (*tdAppendColValToSRowFp)(STSRow *pRow, void *pBitmap, int16_t colId, int8_t colType,
  //                                                    const void *val, int8_t valType, int32_t tOffset, int16_t tIdx);
  if (TD_IS_TP_ROW(pRow)) {
    tdAppendColValToTpRow(pRow->data, pBitmap, val, true, colType, valType, idx, offset);
  } else {
    tdAppendColValToKvRow(pRow->data, pBitmap, val, true, colType, valType, idx, offset, colId);
  }
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int tdAppendColValToTpRow(STSRow *row, void *pBitmap, const void *val, bool isCopyVarData,
                                              int8_t colType, int8_t valType, int16_t idx, int32_t offset) {
  ASSERT(offset >= sizeof(TSDB_DATA_TYPE_TIMESTAMP));
  if (!tdValIsNone(valType)) {
    if (IS_VAR_DATA_TYPE(colType)) {
      // ts key stored in STSRow.ts
      *(VarDataOffsetT *)POINTER_SHIFT(row->data, offset - sizeof(TSDB_DATA_TYPE_TIMESTAMP)) = TD_ROW_LEN(row);
      if (isCopyVarData) {
        memcpy(POINTER_SHIFT(row, TD_ROW_LEN(row)), val, varDataTLen(val));
      }
      TD_ROW_LEN(row) += varDataTLen(val);
    } else {
      memcpy(POINTER_SHIFT(row->data, offset - sizeof(TSDB_DATA_TYPE_TIMESTAMP)), val, TYPE_BYTES[colType]);
    }
  }

#ifdef TD_SUPPORT_BITMAP
  tdSetBitmap(pBitmap, idx, valType);
#endif

  return 0;
}

static FORCE_INLINE int tdAppendColValToKvRow(STSRow *row, void *pBitmap, const void *val, bool isCopyValData,
                                              int8_t colType, int8_t valType, int16_t idx, int32_t offset,
                                              int16_t colId) {
  ASSERT(offset >= sizeof(SKvRowIdx));

  if (!tdValIsNone(valType)) {
    // ts key stored in STSRow.ts
    SColIdx *pColIdx = (SColIdx *)POINTER_SHIFT(row->data, offset - sizeof(SKvRowIdx));
    char *   ptr = (char *)POINTER_SHIFT(row, TD_ROW_LEN(row));
    pColIdx->colId = colId;
    pColIdx->offset = TD_ROW_LEN(row); // the offset include the TD_ROW_HEAD_LEN

    if (IS_VAR_DATA_TYPE(colType)) {
      if (isCopyValData) {
        memcpy(ptr, val, varDataTLen(val));
      }
      TD_ROW_LEN(row) += varDataTLen(val);
    } else {
      memcpy(ptr, val, TYPE_BYTES[colType]);
      TD_ROW_LEN(row) += TYPE_BYTES[colType];
    }
  }
  
#ifdef TD_SUPPORT_BITMAP
  tdSetBitmap(pBitmap, idx, valType);
#endif

  return 0;
}

static FORCE_INLINE int32_t tdSetBitmap(void *pBitmap, int16_t tIdx, int8_t valType) {
  if (!pBitmap) {
    terrno = TSDB_CODE_INVALID_PTR;
    return terrno;
  }
  int16_t nBytes = tIdx / TD_VTYPE_PARTS;
  int16_t nOffset = tIdx & TD_VTYPE_OPTR;
  char *  pDestByte = (char *)POINTER_SHIFT(pBitmap, nBytes);
  switch (nOffset) {
    case 0:
      *pDestByte = ((*pDestByte) & 0x3F) | (valType << 6);
      break;
    case 1:
      *pDestByte = ((*pDestByte) & 0xCF) | (valType << 4);
      break;
    case 2:
      *pDestByte = ((*pDestByte) & 0xF3) | (valType << 2);
      break;
    case 3:
      *pDestByte = ((*pDestByte) & 0xFC) | valType;
      break;
    default:
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

#ifdef TROW_ORIGIN_HZ
typedef struct {
  uint32_t nRows;
  char     rows[];
} STSRowBatch;

static void tdSRowPrint(STSRow *row) {
  printf("type:%d, del:%d, sver:%d\n", row->type, row->del, row->sver);
  printf("isDeleted:%s, isTpRow:%s, isKvRow:%s\n", TD_BOOL_STR(TD_ROW_IS_DELETED(row)), TD_BOOL_STR(TD_IS_TP_ROW(row)),
         TD_BOOL_STR(TD_IS_KV_ROW(row)));
}

typedef enum {
  /// tuple row builder
  TD_TP_ROW_BUILDER = 0,
  /// kv row builder
  TD_KV_ROW_BUILDER,
  /// self-determined row builder
  TD_SD_ROW_BUILDER
} ERowBbuilderT;

typedef struct {
  /// row builder type
  ERowBbuilderT type;
  /// buffer writer
  SBufferWriter bw;
  /// target row
  STSRow *pRow;
} STSRowBuilder;

typedef struct {
  STSchema *pSchema;
  STSRow *    pRow;
} STSRowReader;

typedef struct {
  uint32_t   it;
  STSRowBatch *pRowBatch;
} STSRowBatchIter;

// STSRowBuilder
#define trbInit(rt, allocator, endian, target, size) \
  { .type = (rt), .bw = tbufInitWriter(allocator, endian), .pRow = (target) }
void trbSetRowInfo(STSRowBuilder *pRB, bool del, uint16_t sver);
void trbSetRowVersion(STSRowBuilder *pRB, uint64_t ver);
void trbSetRowTS(STSRowBuilder *pRB, TSKEY ts);
int  trbWriteCol(STSRowBuilder *pRB, void *pData, col_id_t cid);

// STSRowReader
#define tRowReaderInit(schema, row) \
  { .schema = (schema), .row = (row) }
int tRowReaderRead(STSRowReader *pRowReader, col_id_t cid, void *target, uint64_t size);

// STSRowBatchIter
#define tRowBatchIterInit(pRB) \
  { .it = 0, .pRowBatch = (pRB) }
const STSRow *tRowBatchIterNext(STSRowBatchIter *pRowBatchIter);
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_ROW_H_*/
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
#include "talgo.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tbuffer.h"
#include "tdataformat.h"
#include "tdef.h"
#include "tschema.h"
#include "ttypes.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

// Target of tdataformat.h:
// 1. Row related definition in dataformat.h of 2.0 could be replaced with tdataformat.h of 3.0.
// 2. The basic definition in dataformat.h is shared with tdataformat.h of 3.0.

// row type
#define TD_ROW_TP 0x0U  // default
#define TD_ROW_KV 0x01U

/**
 * @brief value type
 *  - for data from client input and STSRow in memory, 3 types of value none/null/norm available
 */
#define TD_VTYPE_NORM 0x00U  // normal val: not none, not null(no need assign value)
#define TD_VTYPE_NULL 0x01U  // null val
#define TD_VTYPE_NONE 0x02U  // none or unknown/undefined
#define TD_VTYPE_MAX  0x03U  //

#define TD_VTYPE_NORM_BYTE 0x0U
#define TD_VTYPE_NULL_BYTE 0x55U
#define TD_VTYPE_NONE_BYTE 0xAAU

#define TD_ROWS_ALL_NORM  0x00U
#define TD_ROWS_NULL_NORM 0x01U

#define TD_COL_ROWS_NORM(c) ((c)->bitmap == TD_ROWS_ALL_NORM)  // all rows of SDataCol/SBlockCol is NORM
#define TD_SET_COL_ROWS_BTIMAP(c, v) ((c)->bitmap = (v))
#define TD_SET_COL_ROWS_NORM(c) TD_SET_COL_ROWS_BTIMAP((c), TD_ROWS_ALL_NORM)
#define TD_SET_COL_ROWS_MISC(c) TD_SET_COL_ROWS_BTIMAP((c), TD_ROWS_NULL_NORM)

#define KvConvertRatio (0.9f)
#define isSelectKVRow(klen, tlen) ((klen) < ((tlen)*KvConvertRatio))

#ifdef TD_SUPPORT_BITMAP
static FORCE_INLINE bool tdValTypeIsNone(TDRowValT valType) { return (valType & 0x03U) == TD_VTYPE_NONE; }
static FORCE_INLINE bool tdValTypeIsNull(TDRowValT valType) { return (valType & 0x03U) == TD_VTYPE_NULL; }
static FORCE_INLINE bool tdValTypeIsNorm(TDRowValT valType) { return (valType & 0x03U) == TD_VTYPE_NORM; }
#endif

static FORCE_INLINE bool tdValIsNorm(TDRowValT valType, const void *val, int32_t colType) {
#ifdef TD_SUPPORT_BITMAP
  return tdValTypeIsNorm(valType);
#else
  return !isNull(val, colType);
#endif
}

static FORCE_INLINE bool tdValIsNone(TDRowValT valType) {
#ifdef TD_SUPPORT_BITMAP
  return tdValTypeIsNone(valType);
#else
  return false;
#endif
}

static FORCE_INLINE bool tdValIsNull(TDRowValT valType, const void *val, int32_t colType) {
#ifdef TD_SUPPORT_BITMAP
  return tdValTypeIsNull(valType);
#else
  return isNull(val, colType);
#endif
}

typedef void *SRow;

typedef struct {
  TDRowValT valType;
  void     *val;
} SCellVal;

typedef struct {
  // TODO
  int tmp;  // TODO: to avoid compile error
} STpRow;   // tuple

#pragma pack(push, 1)
typedef struct {
  col_id_t colId;
  uint32_t offset;
} SKvRowIdx;
#pragma pack(pop)

typedef struct {
  uint16_t  ncols;
  SKvRowIdx cidx[];
} SKvRow;

typedef struct {
  /// timestamp
  TSKEY ts;
  union {
    /// union field for encode and decode
    uint32_t info;
    struct {
      /// row type
      uint16_t type : 2;
      /// is delete row(0 not delete, 1 delete)
      uint16_t del : 1;
      /// endian(0 little endian, 1 big endian)
      uint16_t endian : 1;
      /// reserved for back compatibility
      uint16_t reserve : 12;
      /// row schema version
      uint16_t sver;
    };
  };
  /// row total length
  uint32_t len;
  /// row version
  uint64_t ver;
  /// the inline data, maybe a tuple or a k-v tuple
  char data[];
} STSRow;

typedef struct {
  // basic info
  int8_t       rowType;
  schema_ver_t sver;
  STSRow      *pBuf;

  // extended info
  int32_t  flen;
  col_id_t nBoundCols;
  col_id_t nCols;
  col_id_t nBitmaps;
  col_id_t nBoundBitmaps;
  int32_t  offset;
  void    *pBitmap;
  void    *pOffset;
  int32_t  extendedRowSize;
} SRowBuilder;

#define TD_ROW_HEAD_LEN (sizeof(STSRow))
#define TD_ROW_NCOLS_LEN (sizeof(col_id_t))

#define TD_ROW_INFO(r) ((r)->info)
#define TD_ROW_TYPE(r) ((r)->type)
#define TD_ROW_DELETE(r) ((r)->del)
#define TD_ROW_ENDIAN(r) ((r)->endian)
#define TD_ROW_SVER(r) ((r)->sver)
#define TD_ROW_NCOLS(r) ((r)->data)  // only valid for SKvRow
#define TD_ROW_DATA(r) ((r)->data)
#define TD_ROW_LEN(r) ((r)->len)
#define TD_ROW_KEY(r) ((r)->ts)
#define TD_ROW_KEY_ADDR(r) (r)

// N.B. If without STSchema, getExtendedRowSize() is used to get the rowMaxBytes and
// (int32_t)ceil((double)nCols/TD_VTYPE_PARTS) should be added if TD_SUPPORT_BITMAP defined.
#define TD_ROW_MAX_BYTES_FROM_SCHEMA(s) (schemaTLen(s) + TD_ROW_HEAD_LEN)

#define TD_ROW_SET_INFO(r, i) (TD_ROW_INFO(r) = (i))
#define TD_ROW_SET_TYPE(r, t) (TD_ROW_TYPE(r) = (t))
#define TD_ROW_SET_DELETE(r) (TD_ROW_DELETE(r) = 1)
#define TD_ROW_SET_SVER(r, v) (TD_ROW_SVER(r) = (v))
#define TD_ROW_SET_LEN(r, l) (TD_ROW_LEN(r) = (l))
#define TD_ROW_SET_NCOLS(r, n) (*(col_id_t *)TD_ROW_NCOLS(r) = (n))

#define TD_ROW_IS_DELETED(r) (TD_ROW_DELETE(r) == 1)
#define TD_IS_TP_ROW(r) (TD_ROW_TYPE(r) == TD_ROW_TP)
#define TD_IS_KV_ROW(r) (TD_ROW_TYPE(r) == TD_ROW_KV)
#define TD_IS_TP_ROW_T(t) ((t) == TD_ROW_TP)
#define TD_IS_KV_ROW_T(t) ((t) == TD_ROW_KV)

#define TD_BOOL_STR(b) ((b) ? "true" : "false")
#define isUtilizeKVRow(k, d) ((k) < ((d)*KVRatioConvert))

#define TD_ROW_COL_IDX(r) POINTER_SHIFT(TD_ROW_DATA(r), sizeof(col_id_t))

static FORCE_INLINE void tdRowSetVal(SCellVal *pVal, uint8_t valType, void *val) {
  pVal->valType = valType;
  pVal->val = val;
}
static FORCE_INLINE col_id_t    tdRowGetNCols(STSRow *pRow) { return *(col_id_t *)TD_ROW_NCOLS(pRow); }
static FORCE_INLINE void        tdRowCpy(void *dst, const STSRow *pRow) { memcpy(dst, pRow, TD_ROW_LEN(pRow)); }
static FORCE_INLINE const char *tdRowEnd(STSRow *pRow) { return (const char *)POINTER_SHIFT(pRow, TD_ROW_LEN(pRow)); }

STSRow *tdRowDup(STSRow *row);

static FORCE_INLINE SKvRowIdx *tdKvRowColIdxAt(STSRow *pRow, col_id_t idx) {
  return (SKvRowIdx *)TD_ROW_COL_IDX(pRow) + idx;
}
static FORCE_INLINE void *tdKVRowColVal(STSRow *pRow, SKvRowIdx *pIdx) { return POINTER_SHIFT(pRow, pIdx->offset); }

#define TD_ROW_OFFSET(p) ((p)->toffset);  // During ParseInsert when without STSchema, how to get the offset for STpRow?

void                        tdMergeBitmap(uint8_t *srcBitmap, int32_t srcLen, uint8_t *dstBitmap);
static FORCE_INLINE void    tdRowCopy(void *dst, STSRow *row) { memcpy(dst, row, TD_ROW_LEN(row)); }
static FORCE_INLINE int32_t tdSetBitmapValTypeI(void *pBitmap, int16_t colIdx, TDRowValT valType);
static FORCE_INLINE int32_t tdSetBitmapValTypeII(void *pBitmap, int16_t colIdx, TDRowValT valType);
static FORCE_INLINE int32_t tdSetBitmapValType(void *pBitmap, int16_t colIdx, TDRowValT valType, int8_t bitmapMode);
int32_t                     tdSetBitmapValTypeN(void *pBitmap, int16_t nEle, TDRowValT valType, int8_t bitmapMode);
static FORCE_INLINE int32_t tdGetBitmapValTypeI(void *pBitmap, int16_t colIdx, TDRowValT *pValType);
static FORCE_INLINE int32_t tdGetBitmapValTypeII(void *pBitmap, int16_t colIdx, TDRowValT *pValType);
int32_t tdAppendValToDataCol(SDataCol *pCol, TDRowValT valType, const void *val, int32_t numOfRows, int32_t maxPoints,
                             int8_t bitmapMode);
static FORCE_INLINE int32_t tdAppendColValToTpRow(SRowBuilder *pBuilder, TDRowValT valType, const void *val,
                                                  bool isCopyVarData, int8_t colType, int16_t colIdx, int32_t offset);
static FORCE_INLINE int32_t tdAppendColValToKvRow(SRowBuilder *pBuilder, TDRowValT valType, const void *val,
                                                  bool isCopyVarData, int8_t colType, int16_t colIdx, int32_t offset,
                                                  col_id_t colId);
int32_t tdAppendSTSRowToDataCol(STSRow *pRow, STSchema *pSchema, SDataCols *pCols, bool forceSetNull);

/**
 * @brief
 *
 * @param pRow
 * @param flen flen in STSchema
 * @return FORCE_INLINE*
 */
static FORCE_INLINE void *tdGetBitmapAddrTp(STSRow *pRow, uint32_t flen) {
  // The primary TS key is stored separatedly.
  return POINTER_SHIFT(TD_ROW_DATA(pRow), flen - sizeof(TSKEY));
  // return POINTER_SHIFT(pRow->ts, flen);
}

static FORCE_INLINE void *tdGetBitmapAddrKv(STSRow *pRow, col_id_t nKvCols) {
  // The primary TS key is stored separatedly and is Norm value, thus should minus 1 firstly
  return POINTER_SHIFT(TD_ROW_COL_IDX(pRow), (--nKvCols) * sizeof(SKvRowIdx));
}
static FORCE_INLINE void *tdGetBitmapAddr(STSRow *pRow, uint8_t rowType, uint32_t flen, col_id_t nKvCols) {
#ifdef TD_SUPPORT_BITMAP
  switch (rowType) {
    case TD_ROW_TP:
      return tdGetBitmapAddrTp(pRow, flen);
    case TD_ROW_KV:
      return tdGetBitmapAddrKv(pRow, nKvCols);
    default:
      break;
  }
#endif
  return NULL;
}

static FORCE_INLINE int32_t tdSetBitmapValType(void *pBitmap, int16_t colIdx, TDRowValT valType, int8_t bitmapMode) {
  switch (bitmapMode) {
    case 0:
      tdSetBitmapValTypeII(pBitmap, colIdx, valType);
      break;
    case -1:
    case 1:
      tdSetBitmapValTypeI(pBitmap, colIdx, valType);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Use 2 bits at default
 *
 * @param pBitmap
 * @param colIdx The relative index of colId, may have minus value as parameter.
 * @param valType
 * @return FORCE_INLINE
 */
static FORCE_INLINE int32_t tdSetBitmapValTypeII(void *pBitmap, int16_t colIdx, TDRowValT valType) {
  if (!pBitmap || colIdx < 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int16_t nBytes = colIdx / TD_VTYPE_PARTS;
  int16_t nOffset = colIdx & TD_VTYPE_OPTR;
  char   *pDestByte = (char *)POINTER_SHIFT(pBitmap, nBytes);
  // use literal value directly and not use formula to simplify the codes
  switch (nOffset) {
    case 0:
      // *pDestByte = ((*pDestByte) & 0x3F) | (valType << 6);
      // set the value and clear other partitions for offset 0
      *pDestByte = (valType << 6);
      break;
    case 1:
      // *pDestByte = ((*pDestByte) & 0xCF) | (valType << 4);
      *pDestByte |= (valType << 4);
      break;
    case 2:
      // *pDestByte = ((*pDestByte) & 0xF3) | (valType << 2);
      *pDestByte |= (valType << 2);
      break;
    case 3:
      // *pDestByte = ((*pDestByte) & 0xFC) | valType;
      *pDestByte |= (valType);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t tdGetBitmapValType(void *pBitmap, int16_t colIdx, TDRowValT *pValType, int8_t bitmapMode) {
  switch (bitmapMode) {
    case 0:
      tdGetBitmapValTypeII(pBitmap, colIdx, pValType);
      break;
    case -1:
    case 1:
      tdGetBitmapValTypeI(pBitmap, colIdx, pValType);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Use 2 bits at default
 *
 * @param pBitmap
 * @param colIdx The relative index of colId, may have minus value as parameter.
 * @param pValType
 * @return FORCE_INLINE
 */
static FORCE_INLINE int32_t tdGetBitmapValTypeII(void *pBitmap, int16_t colIdx, TDRowValT *pValType) {
  if (!pBitmap || colIdx < 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int16_t nBytes = colIdx / TD_VTYPE_PARTS;
  int16_t nOffset = colIdx & TD_VTYPE_OPTR;
  char   *pDestByte = (char *)POINTER_SHIFT(pBitmap, nBytes);
  // use literal value directly and not use formula to simplify the codes
  switch (nOffset) {
    case 0:
      *pValType = (((*pDestByte) & 0xC0) >> 6);
      break;
    case 1:
      *pValType = (((*pDestByte) & 0x30) >> 4);
      break;
    case 2:
      *pValType = (((*pDestByte) & 0x0C) >> 2);
      break;
    case 3:
      *pValType = ((*pDestByte) & 0x03);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief
 *
 * @param pBitmap
 * @param colIdx The relative index of colId, may have minus value as parameter.
 * @param valType
 * @return FORCE_INLINE
 */
static FORCE_INLINE int32_t tdSetBitmapValTypeI(void *pBitmap, int16_t colIdx, TDRowValT valType) {
  if (!pBitmap || colIdx < 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int16_t nBytes = colIdx / TD_VTYPE_PARTS_I;
  int16_t nOffset = colIdx & TD_VTYPE_OPTR_I;
  char   *pDestByte = (char *)POINTER_SHIFT(pBitmap, nBytes);
  // use literal value directly and not use formula to simplify the codes
  switch (nOffset) {
    case 0:
      // *pDestByte = ((*pDestByte) & 0x7F) | (valType << 7);
      // set the value and clear other partitions for offset 0
      *pDestByte = (valType << 7);
      break;
    case 1:
      // *pDestByte = ((*pDestByte) & 0xBF) | (valType << 6);
      *pDestByte |= (valType << 6);
      break;
    case 2:
      // *pDestByte = ((*pDestByte) & 0xDF) | (valType << 5);
      *pDestByte |= (valType << 5);
      break;
    case 3:
      // *pDestByte = ((*pDestByte) & 0xEF) | (valType << 4);
      *pDestByte |= (valType << 4);
      break;
    case 4:
      // *pDestByte = ((*pDestByte) & 0xF7) | (valType << 3);
      *pDestByte |= (valType << 3);
      break;
    case 5:
      // *pDestByte = ((*pDestByte) & 0xFB) | (valType << 2);
      *pDestByte |= (valType << 2);
      break;
    case 6:
      // *pDestByte = ((*pDestByte) & 0xFD) | (valType << 1);
      *pDestByte |= (valType << 1);
      break;
    case 7:
      // *pDestByte = ((*pDestByte) & 0xFE) | valType;
      *pDestByte |= (valType);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief
 *
 * @param pBitmap
 * @param colIdx The relative index of colId, may have minus value as parameter.
 * @param pValType
 * @return FORCE_INLINE
 */
static FORCE_INLINE int32_t tdGetBitmapValTypeI(void *pBitmap, int16_t colIdx, TDRowValT *pValType) {
  if (!pBitmap || colIdx < 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int16_t nBytes = colIdx / TD_VTYPE_PARTS_I;
  int16_t nOffset = colIdx & TD_VTYPE_OPTR_I;
  char   *pDestByte = (char *)POINTER_SHIFT(pBitmap, nBytes);
  // use literal value directly and not use formula to simplify the codes
  switch (nOffset) {
    case 0:
      *pValType = (((*pDestByte) & 0x80) >> 7);
      break;
    case 1:
      *pValType = (((*pDestByte) & 0x40) >> 6);
      break;
    case 2:
      *pValType = (((*pDestByte) & 0x20) >> 5);
      break;
    case 3:
      *pValType = (((*pDestByte) & 0x10) >> 4);
      break;
    case 4:
      *pValType = (((*pDestByte) & 0x08) >> 3);
      break;
    case 5:
      *pValType = (((*pDestByte) & 0x04) >> 2);
      break;
    case 6:
      *pValType = (((*pDestByte) & 0x02) >> 1);
      break;
    case 7:
      *pValType = ((*pDestByte) & 0x01);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

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

/**
 * @brief
 *
 * @param pBuilder
 * @param sver schema version
 * @return FORCE_INLINE
 */
static FORCE_INLINE void tdSRowInit(SRowBuilder *pBuilder, int16_t sver) {
  pBuilder->rowType = TD_ROW_TP;  // default STpRow
  pBuilder->sver = sver;
}

/**
 * @brief Not recommended to use
 *
 * @param pBuilder
 * @param rowType
 * @return FORCE_INLINE
 */
static FORCE_INLINE void tdSRowSetRowType(SRowBuilder *pBuilder, int8_t rowType) { pBuilder->rowType = rowType; }

/**
 * @brief
 *
 * @param pBuilder
 * @param nCols
 * @param nBoundCols use -1 if not available
 * @param flen
 * @return FORCE_INLINE
 */
static FORCE_INLINE int32_t tdSRowSetInfo(SRowBuilder *pBuilder, int32_t nCols, int32_t nBoundCols, int32_t flen) {
  pBuilder->flen = flen;
  pBuilder->nCols = nCols;
  pBuilder->nBoundCols = nBoundCols;
  if (pBuilder->flen <= 0 || pBuilder->nCols <= 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifdef TD_SUPPORT_BITMAP
  // the primary TS key is stored separatedly
  pBuilder->nBitmaps = (int16_t)TD_BITMAP_BYTES(pBuilder->nCols - 1);
  if (nBoundCols > 0) {
    pBuilder->nBoundBitmaps = (int16_t)TD_BITMAP_BYTES(pBuilder->nBoundCols - 1);
  } else {
    pBuilder->nBoundBitmaps = 0;
  }
#else
  pBuilder->nBitmaps = 0;
  pBuilder->nBoundBitmaps = 0;
#endif
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief To judge row type: STpRow/SKvRow
 *
 * @param pBuilder
 * @param nCols
 * @param nBoundCols
 * @param flen
 * @param allNullLen use -1 if not available
 * @param boundNullLen use -1 if not available
 * @return FORCE_INLINE
 */
static FORCE_INLINE int32_t tdSRowSetExtendedInfo(SRowBuilder *pBuilder, int32_t nCols, int32_t nBoundCols,
                                                  int32_t flen, int32_t allNullLen, int32_t boundNullLen) {
  if ((boundNullLen > 0) && (allNullLen > 0) && (nBoundCols > 0)) {
    uint32_t tpLen = allNullLen;
    uint32_t kvLen = sizeof(col_id_t) + sizeof(SKvRowIdx) * nBoundCols + boundNullLen;
    if (isSelectKVRow(kvLen, tpLen)) {
      pBuilder->rowType = TD_ROW_KV;
    } else {
      pBuilder->rowType = TD_ROW_TP;
    }

  } else {
    pBuilder->rowType = TD_ROW_TP;
  }

  pBuilder->flen = flen;
  pBuilder->nCols = nCols;
  pBuilder->nBoundCols = nBoundCols;
  if (pBuilder->flen <= 0 || pBuilder->nCols <= 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifdef TD_SUPPORT_BITMAP
  // the primary TS key is stored separatedly
  pBuilder->nBitmaps = (col_id_t)TD_BITMAP_BYTES(pBuilder->nCols - 1);
  if (nBoundCols > 0) {
    pBuilder->nBoundBitmaps = (col_id_t)TD_BITMAP_BYTES(pBuilder->nBoundCols - 1);
  } else {
    pBuilder->nBoundBitmaps = 0;
  }
#else
  pBuilder->nBitmaps = 0;
  pBuilder->nBoundBitmaps = 0;
#endif
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief The invoker is responsible for memory alloc/dealloc.
 *
 * @param pBuilder
 * @param pBuf Output buffer of STSRow
 */
static int32_t tdSRowResetBuf(SRowBuilder *pBuilder, void *pBuf) {
  pBuilder->pBuf = (STSRow *)pBuf;
  if (!pBuilder->pBuf) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  TD_ROW_SET_INFO(pBuilder->pBuf, 0);
  TD_ROW_SET_TYPE(pBuilder->pBuf, pBuilder->rowType);

  uint32_t len = 0;
  switch (pBuilder->rowType) {
    case TD_ROW_TP:
#ifdef TD_SUPPORT_BITMAP
      pBuilder->pBitmap = tdGetBitmapAddrTp(pBuilder->pBuf, pBuilder->flen);
#endif
      // the primary TS key is stored separatedly
      len = TD_ROW_HEAD_LEN + pBuilder->flen - sizeof(TSKEY) + pBuilder->nBitmaps;
      TD_ROW_SET_LEN(pBuilder->pBuf, len);
      TD_ROW_SET_SVER(pBuilder->pBuf, pBuilder->sver);
      break;
    case TD_ROW_KV:
#ifdef TD_SUPPORT_BITMAP
      pBuilder->pBitmap = tdGetBitmapAddrKv(pBuilder->pBuf, pBuilder->nBoundCols);
#endif
      len = TD_ROW_HEAD_LEN + TD_ROW_NCOLS_LEN + (pBuilder->nBoundCols - 1) * sizeof(SKvRowIdx) +
            pBuilder->nBoundBitmaps;  // add
      TD_ROW_SET_LEN(pBuilder->pBuf, len);
      TD_ROW_SET_SVER(pBuilder->pBuf, pBuilder->sver);
      TD_ROW_SET_NCOLS(pBuilder->pBuf, pBuilder->nBoundCols);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
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
  if (tdSRowSetExtendedInfo(pBuilder, allNullLen, boundNullLen, nCols, nBoundCols, flen) < 0) {
    return terrno;
  }
  return tdSRowResetBuf(pBuilder, pBuf);
}

/**
 * @brief
 *
 * @param pBuilder
 */
static FORCE_INLINE void tdSRowReset(SRowBuilder *pBuilder) {
  pBuilder->rowType = TD_ROW_TP;
  pBuilder->pBuf = NULL;
  pBuilder->nBoundCols = -1;
  pBuilder->nCols = -1;
  pBuilder->flen = -1;
  pBuilder->pBitmap = NULL;
}

// internal func
static FORCE_INLINE int32_t tdAppendColValToTpRow(SRowBuilder *pBuilder, TDRowValT valType, const void *val,
                                                  bool isCopyVarData, int8_t colType, int16_t colIdx, int32_t offset) {
  if ((offset < (int32_t)sizeof(TSKEY)) || (colIdx < 1)) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  offset -= sizeof(TSKEY);
  --colIdx;

#ifdef TD_SUPPORT_BITMAP
  if (tdSetBitmapValType(pBuilder->pBitmap, colIdx, valType, 0) != TSDB_CODE_SUCCESS) {
    return terrno;
  }
#endif

  STSRow *row = pBuilder->pBuf;

  // 1. No need to set flen part for Null/None, just use bitmap. When upsert for the same primary TS key, the bitmap
  // should be updated simultaneously if Norm val overwrite Null/None cols.
  // 2. When consume STSRow in memory by taos client/tq, the output of Null/None cols should both be Null.
  if (tdValIsNorm(valType, val, colType)) {
    // TODO: The layout of new data types imported since 3.0 like blob/medium blob is the same with binary/nchar.
    if (IS_VAR_DATA_TYPE(colType)) {
      // ts key stored in STSRow.ts
      *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(row), offset) = TD_ROW_LEN(row);
      if (isCopyVarData) {
        memcpy(POINTER_SHIFT(row, TD_ROW_LEN(row)), val, varDataTLen(val));
      }
      TD_ROW_LEN(row) += varDataTLen(val);
    } else {
      memcpy(POINTER_SHIFT(TD_ROW_DATA(row), offset), val, TYPE_BYTES[colType]);
    }
  }
#ifdef TD_SUPPORT_BACK2
  // NULL/None value
  else {
    // TODO: Null value for new data types imported since 3.0 need to be defined.
    const void *nullVal = getNullValue(colType);
    if (IS_VAR_DATA_TYPE(colType)) {
      // ts key stored in STSRow.ts
      *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(row), offset) = TD_ROW_LEN(row);

      if (isCopyVarData) {
        memcpy(POINTER_SHIFT(row, TD_ROW_LEN(row)), nullVal, varDataTLen(nullVal));
      }
      TD_ROW_LEN(row) += varDataTLen(nullVal);
    } else {
      memcpy(POINTER_SHIFT(TD_ROW_DATA(row), offset), nullVal, TYPE_BYTES[colType]);
    }
  }
#endif

  return 0;
}

// internal func
static FORCE_INLINE int32_t tdAppendColValToKvRow(SRowBuilder *pBuilder, TDRowValT valType, const void *val,
                                                  bool isCopyVarData, int8_t colType, int16_t colIdx, int32_t offset,
                                                  col_id_t colId) {
  if ((offset < (int32_t)sizeof(SKvRowIdx)) || (colIdx < 1)) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  offset -= sizeof(SKvRowIdx);
  --colIdx;

#ifdef TD_SUPPORT_BITMAP
  if (tdSetBitmapValType(pBuilder->pBitmap, colIdx, valType, 0) != TSDB_CODE_SUCCESS) {
    return terrno;
  }
#endif

  STSRow *row = pBuilder->pBuf;
  // No need to store None/Null values.
  if (tdValIsNorm(valType, val, colType)) {
    // ts key stored in STSRow.ts
    SKvRowIdx *pColIdx = (SKvRowIdx *)POINTER_SHIFT(TD_ROW_COL_IDX(row), offset);
    char      *ptr = (char *)POINTER_SHIFT(row, TD_ROW_LEN(row));
    pColIdx->colId = colId;
    pColIdx->offset = TD_ROW_LEN(row);  // the offset include the TD_ROW_HEAD_LEN

    if (IS_VAR_DATA_TYPE(colType)) {
      if (isCopyVarData) {
        memcpy(ptr, val, varDataTLen(val));
      }
      TD_ROW_LEN(row) += varDataTLen(val);
    } else {
      memcpy(ptr, val, TYPE_BYTES[colType]);
      TD_ROW_LEN(row) += TYPE_BYTES[colType];
    }
  }
#ifdef TD_SUPPORT_BACK2
  // NULL/None value
  else {
    SKvRowIdx *pColIdx = (SKvRowIdx *)POINTER_SHIFT(TD_ROW_COL_IDX(row), offset);
    char      *ptr = (char *)POINTER_SHIFT(row, TD_ROW_LEN(row));
    pColIdx->colId = colId;
    pColIdx->offset = TD_ROW_LEN(row);  // the offset include the TD_ROW_HEAD_LEN
    const void *nullVal = getNullValue(colType);

    if (IS_VAR_DATA_TYPE(colType)) {
      if (isCopyVarData) {
        memcpy(ptr, nullVal, varDataTLen(nullVal));
      }
      TD_ROW_LEN(row) += varDataTLen(nullVal);
    } else {
      memcpy(ptr, nullVal, TYPE_BYTES[colType]);
      TD_ROW_LEN(row) += TYPE_BYTES[colType];
    }
  }
#endif

  return 0;
}

/**
 * @brief exposed func
 *
 * @param pBuilder
 * @param colId start from PRIMARYKEY_TIMESTAMP_COL_ID
 * @param colType
 * @param valType
 * @param val
 * @param isCopyVarData
 * @param offset
 * @param colIdx sorted column index, start from 0
 * @return FORCE_INLINE
 */
static FORCE_INLINE int32_t tdAppendColValToRow(SRowBuilder *pBuilder, col_id_t colId, int8_t colType,
                                                TDRowValT valType, const void *val, bool isCopyVarData, int32_t offset,
                                                col_id_t colIdx) {
  STSRow *pRow = pBuilder->pBuf;
  if (!val) {
#ifdef TD_SUPPORT_BITMAP
    if (tdValTypeIsNorm(valType)) {
      terrno = TSDB_CODE_INVALID_PTR;
      return terrno;
    }
#else
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
#endif
  }
  // TS KEY is stored in STSRow.ts and not included in STSRow.data field.
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    TD_ROW_KEY(pRow) = *(TSKEY *)val;
    // The primary TS key is Norm all the time, thus its valType is not stored in bitmap.
    return TSDB_CODE_SUCCESS;
  }
  // TODO:  We can avoid the type judegement by FP, but would prevent the inline scheme.
  if (TD_IS_TP_ROW(pRow)) {
    tdAppendColValToTpRow(pBuilder, valType, val, isCopyVarData, colType, colIdx, offset);
  } else {
    tdAppendColValToKvRow(pBuilder, valType, val, isCopyVarData, colType, colIdx, offset, colId);
  }
  return TSDB_CODE_SUCCESS;
}

// internal
static FORCE_INLINE int32_t tdGetTpRowValOfCol(SCellVal *output, STSRow *pRow, void *pBitmap, int8_t colType,
                                               int32_t offset, int16_t colIdx) {
#ifdef TD_SUPPORT_BITMAP
  if (tdGetBitmapValType(pBitmap, colIdx, &output->valType, 0) != TSDB_CODE_SUCCESS) {
    output->valType = TD_VTYPE_NONE;
    return terrno;
  }
  if (tdValTypeIsNorm(output->valType)) {
    if (IS_VAR_DATA_TYPE(colType)) {
      output->val = POINTER_SHIFT(pRow, *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(pRow), offset));
    } else {
      output->val = POINTER_SHIFT(TD_ROW_DATA(pRow), offset);
    }
  }
#else
  if (IS_VAR_DATA_TYPE(colType)) {
    output->val = POINTER_SHIFT(pRow, *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(pRow), offset));
  } else {
    output->val = POINTER_SHIFT(TD_ROW_DATA(pRow), offset);
  }
  output->valType = isNull(output->val, colType) ? TD_VTYPE_NULL : TD_VTYPE_NORM;
#endif
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t compareKvRowColId(const void *key1, const void *key2) {
  if (*(int16_t *)key1 > ((SColIdx *)key2)->colId) {
    return 1;
  } else if (*(int16_t *)key1 < ((SColIdx *)key2)->colId) {
    return -1;
  } else {
    return 0;
  }
}
// internal
static FORCE_INLINE int32_t tdGetKvRowValOfCol(SCellVal *output, STSRow *pRow, void *pBitmap, int32_t offset,
                                               int16_t colIdx) {
#ifdef TD_SUPPORT_BITMAP
  TASSERT(colIdx < tdRowGetNCols(pRow) - 1);
  if (tdGetBitmapValType(pBitmap, colIdx, &output->valType, 0) != TSDB_CODE_SUCCESS) {
    output->valType = TD_VTYPE_NONE;
    return terrno;
  }
  if (tdValTypeIsNorm(output->valType)) {
    if (offset < 0) {
      terrno = TSDB_CODE_INVALID_PARA;
      output->valType = TD_VTYPE_NONE;
      return terrno;
    }
    output->val = POINTER_SHIFT(pRow, offset);
  }
#else
  TASSERT(0);
  if (offset < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    output->valType = TD_VTYPE_NONE;
    return terrno;
  }
  output->val = POINTER_SHIFT(pRow, offset);
  output->valType = isNull(output->val, colType) ? TD_VTYPE_NULL : TD_VTYPE_NORM;
#endif
  return TSDB_CODE_SUCCESS;
}

typedef struct {
  STSchema *pSchema;
  STSRow   *pRow;
  void     *pBitmap;
  uint32_t  offset;
  col_id_t  maxColId;
  col_id_t  colIdx;  // [PRIMARYKEY_TIMESTAMP_COL_ID, nSchemaCols], PRIMARYKEY_TIMESTAMP_COL_ID equals 1
  col_id_t  kvIdx;   // [0, nKvCols)
} STSRowIter;

static FORCE_INLINE void tdSTSRowIterReset(STSRowIter *pIter, STSRow *pRow) {
  pIter->pRow = pRow;
  pIter->pBitmap = tdGetBitmapAddr(pRow, pRow->type, pIter->pSchema->flen, tdRowGetNCols(pRow));
  pIter->offset = 0;
  pIter->colIdx = PRIMARYKEY_TIMESTAMP_COL_ID;
  pIter->kvIdx = 0;
}

static FORCE_INLINE void tdSTSRowIterInit(STSRowIter *pIter, STSchema *pSchema) {
  pIter->pSchema = pSchema;
  pIter->maxColId = pSchema->columns[pSchema->numOfCols - 1].colId;
}

static int32_t tdCompareColId(const void *arg1, const void *arg2) {
  int32_t   colId = *(int32_t *)arg1;
  STColumn *pCol = (STColumn *)arg2;

  if (colId < pCol->colId) {
    return -1;
  } else if (colId == pCol->colId) {
    return 0;
  } else {
    return 1;
  }
}

/**
 * @brief STSRow method to get value of specified colId/colType by bsearch
 *
 * @param pIter
 * @param colId Start from PRIMARYKEY_TIMESTAMP_COL_ID(1)
 * @param colType
 * @param pVal
 * @return true  Not reach end and pVal is set(None/Null/Norm).
 * @return false Reach end and pVal not set.
 */
static FORCE_INLINE bool tdSTSRowGetVal(STSRowIter *pIter, col_id_t colId, col_type_t colType, SCellVal *pVal) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    pVal->val = &pIter->pRow->ts;
    pVal->valType = TD_VTYPE_NORM;
    return true;
  }

  STSRow *pRow = pIter->pRow;
  int16_t colIdx = -1;
  if (TD_IS_TP_ROW(pRow)) {
    STSchema *pSchema = pIter->pSchema;
    STColumn *pCol =
        (STColumn *)taosbsearch(&colId, pSchema->columns, pSchema->numOfCols, sizeof(STColumn), tdCompareColId, TD_EQ);
    if (!pCol) {
      pVal->valType = TD_VTYPE_NONE;
      if (COL_REACH_END(colId, pIter->maxColId)) return false;
      return true;
    }
#ifdef TD_SUPPORT_BITMAP
    colIdx = POINTER_DISTANCE(pCol, pSchema->columns) / sizeof(STColumn);
#endif
    tdGetTpRowValOfCol(pVal, pRow, pIter->pBitmap, pCol->type, pCol->offset - sizeof(TSKEY), colIdx - 1);
  } else if (TD_IS_KV_ROW(pRow)) {
    SKvRowIdx *pIdx = (SKvRowIdx *)taosbsearch(&colId, TD_ROW_COL_IDX(pRow), tdRowGetNCols(pRow), sizeof(SKvRowIdx),
                                               compareKvRowColId, TD_EQ);
#ifdef TD_SUPPORT_BITMAP
    if (pIdx) {
      colIdx = POINTER_DISTANCE(TD_ROW_COL_IDX(pRow), pIdx) / sizeof(SKvRowIdx);
    }
#endif
    tdGetKvRowValOfCol(pVal, pRow, pIter->pBitmap, pIdx ? pIdx->offset : -1, colIdx);
  } else {
    if (COL_REACH_END(colId, pIter->maxColId)) return false;
    pVal->valType = TD_VTYPE_NONE;
  }

  return true;
}

// internal
static FORCE_INLINE bool tdGetTpRowDataOfCol(STSRowIter *pIter, col_type_t colType, int32_t offset, SCellVal *pVal) {
  STSRow *pRow = pIter->pRow;
  if (IS_VAR_DATA_TYPE(colType)) {
    pVal->val = POINTER_SHIFT(pRow, *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(pRow), offset));
  } else {
    pVal->val = POINTER_SHIFT(TD_ROW_DATA(pRow), offset);
  }

#ifdef TD_SUPPORT_BITMAP
  if (tdGetBitmapValType(pIter->pBitmap, pIter->colIdx - 1, &pVal->valType, 0) != TSDB_CODE_SUCCESS) {
    pVal->valType = TD_VTYPE_NONE;
  }
#else
  pVal->valType = isNull(pVal->val, colType) ? TD_VTYPE_NULL : TD_VTYPE_NORM;
#endif

  return true;
}

// internal
static FORCE_INLINE bool tdGetKvRowValOfColEx(STSRowIter *pIter, col_id_t colId, col_type_t colType, col_id_t *nIdx,
                                              SCellVal *pVal) {
  STSRow    *pRow = pIter->pRow;
  SKvRowIdx *pKvIdx = NULL;
  bool       colFound = false;
  col_id_t   kvNCols = tdRowGetNCols(pRow);
  while (*nIdx < kvNCols) {
    pKvIdx = (SKvRowIdx *)POINTER_SHIFT(TD_ROW_COL_IDX(pRow), *nIdx * sizeof(SKvRowIdx));
    if (pKvIdx->colId == colId) {
      ++(*nIdx);
      pVal->val = POINTER_SHIFT(pRow, pKvIdx->offset);
      colFound = true;
      break;
    } else if (pKvIdx->colId > colId) {
      pVal->valType = TD_VTYPE_NONE;
      return true;
    } else {
      ++(*nIdx);
    }
  }

  if (!colFound) return false;

#ifdef TD_SUPPORT_BITMAP
  int16_t colIdx = -1;
  if (pKvIdx) colIdx = POINTER_DISTANCE(TD_ROW_COL_IDX(pRow), pKvIdx) / sizeof(SKvRowIdx);
  if (tdGetBitmapValType(pIter->pBitmap, colIdx, &pVal->valType, 0) != TSDB_CODE_SUCCESS) {
    pVal->valType = TD_VTYPE_NONE;
  }
#else
  pVal->valType = isNull(pVal->val, colType) ? TD_VTYPE_NULL : TD_VTYPE_NORM;
#endif

  return true;
}

/**
 * @brief STSRow Iter to get value from colId 1 to maxColId ascendingly
 *
 * @param pIter
 * @param pVal
 * @param colId
 * @param colType
 * @param pVal  output
 * @return true  Not reach end and pVal is set(None/Null/Norm).
 * @return false Reach end of row and pVal not set.
 */
static FORCE_INLINE bool tdSTSRowIterNext(STSRowIter *pIter, col_id_t colId, col_type_t colType, SCellVal *pVal) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    pVal->val = &pIter->pRow->ts;
    pVal->valType = TD_VTYPE_NORM;
    return true;
  }

  if (TD_IS_TP_ROW(pIter->pRow)) {
    STColumn *pCol = NULL;
    STSchema *pSchema = pIter->pSchema;
    while (pIter->colIdx <= pSchema->numOfCols) {
      pCol = &pSchema->columns[pIter->colIdx];  // 1st column of schema is primary TS key
      if (colId == pCol->colId) {
        break;
      } else if (colId < pCol->colId) {
        ++pIter->colIdx;
        continue;
      } else {
        return false;
      }
    }
    tdGetTpRowDataOfCol(pIter, pCol->type, pCol->offset - sizeof(TSKEY), pVal);
    ++pIter->colIdx;
  } else if (TD_IS_KV_ROW(pIter->pRow)) {
    return tdGetKvRowValOfColEx(pIter, colId, colType, &pIter->kvIdx, pVal);
  } else {
    pVal->valType = TD_VTYPE_NONE;
    terrno = TSDB_CODE_INVALID_PARA;
    if (COL_REACH_END(colId, pIter->maxColId)) return false;
  }
  return true;
}

STSRow *mergeTwoRows(void *buffer, STSRow *row1, STSRow *row2, STSchema *pSchema1, STSchema *pSchema2);

// Get the data pointer from a column-wised data
static FORCE_INLINE int32_t tdGetColDataOfRow(SCellVal *pVal, SDataCol *pCol, int32_t row, int8_t bitmapMode) {
  if (isAllRowsNone(pCol)) {
    pVal->valType = TD_VTYPE_NULL;
#ifdef TD_SUPPORT_READ2
    pVal->val = (void *)getNullValue(pCol->type);
#else
    pVal->val = NULL;
#endif
    return TSDB_CODE_SUCCESS;
  }

  if (TD_COL_ROWS_NORM(pCol)) {
    pVal->valType = TD_VTYPE_NORM;
  } else if (tdGetBitmapValType(pCol->pBitmap, row, &(pVal->valType), bitmapMode) < 0) {
    return terrno;
  }

  if (tdValTypeIsNorm(pVal->valType)) {
    if (IS_VAR_DATA_TYPE(pCol->type)) {
      pVal->val = POINTER_SHIFT(pCol->pData, pCol->dataOff[row]);
    } else {
      pVal->val = POINTER_SHIFT(pCol->pData, TYPE_BYTES[pCol->type] * row);
    }
  } else {
    pVal->valType = TD_VTYPE_NULL;
#ifdef TD_SUPPORT_READ2
    pVal->val = (void *)getNullValue(pCol->type);
#else
    pVal->val = NULL;
#endif
  }
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE bool tdSTpRowGetVal(STSRow *pRow, col_id_t colId, col_type_t colType, int32_t flen, uint32_t offset,
                                        col_id_t colIdx, SCellVal *pVal) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    tdRowSetVal(pVal, TD_VTYPE_NORM, TD_ROW_KEY_ADDR(pRow));
    return true;
  }
  void *pBitmap = tdGetBitmapAddrTp(pRow, flen);
  tdGetTpRowValOfCol(pVal, pRow, pBitmap, colType, offset - sizeof(TSKEY), colIdx - 1);
  return true;
}

static FORCE_INLINE bool tdSKvRowGetVal(STSRow *pRow, col_id_t colId, uint32_t offset, col_id_t colIdx,
                                        SCellVal *pVal) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    tdRowSetVal(pVal, TD_VTYPE_NORM, TD_ROW_KEY_ADDR(pRow));
    return true;
  }
  void *pBitmap = tdGetBitmapAddrKv(pRow, tdRowGetNCols(pRow));
  tdGetKvRowValOfCol(pVal, pRow, pBitmap, offset, colIdx - 1);
  return true;
}

static FORCE_INLINE int32_t dataColGetNEleLen(SDataCol *pDataCol, int32_t rows, int8_t bitmapMode) {
  ASSERT(rows > 0);
  int32_t result = 0;

  if (IS_VAR_DATA_TYPE(pDataCol->type)) {
    result += pDataCol->dataOff[rows - 1];
    SCellVal val = {0};
    if (tdGetColDataOfRow(&val, pDataCol, rows - 1, bitmapMode) < 0) {
      TASSERT(0);
    }

    // Currently, count the varDataTLen in of Null/None cols considering back compatibility test for 2.4
    result += varDataTLen(val.val);
    // TODO: later on, don't save Null/None for VarDataT for 3.0
    // if (tdValTypeIsNorm(val.valType)) {
    //   result += varDataTLen(val.val);
    // }
  } else {
    result += TYPE_BYTES[pDataCol->type] * rows;
  }

  ASSERT(pDataCol->len == result);

  return result;
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
  STSRow   *pRow;
} STSRowReader;

typedef struct {
  uint32_t     it;
  STSRowBatch *pRowBatch;
} STSRowBatchIter;

// STSRowBuilder
#define trbInit(rt, allocator, endian, target, size) \
  { .type = (rt), .bw = tbufInitWriter(allocator, endian), .pRow = (target) }
void    trbSetRowInfo(STSRowBuilder *pRB, bool del, uint16_t sver);
void    trbSetRowVersion(STSRowBuilder *pRB, uint64_t ver);
void    trbSetRowTS(STSRowBuilder *pRB, TSKEY ts);
int32_t trbWriteCol(STSRowBuilder *pRB, void *pData, col_id_t cid);

// STSRowReader
#define tRowReaderInit(schema, row) \
  { .schema = (schema), .row = (row) }
int32_t tRowReaderRead(STSRowReader *pRowReader, col_id_t cid, void *target, uint64_t size);

// STSRowBatchIter
#define tRowBatchIterInit(pRB) \
  { .it = 0, .pRowBatch = (pRB) }
const STSRow *tRowBatchIterNext(STSRowBatchIter *pRowBatchIter);
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_ROW_H_*/

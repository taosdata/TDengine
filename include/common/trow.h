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
#include "tdataformat.h"
#include "tdef.h"
#include "ttypes.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STSRow {
  TSKEY ts;
  union {
    uint32_t info;
    struct {
      uint16_t type : 2;
      uint16_t del : 1;
      uint16_t endian : 1;
      uint16_t statis : 1;  // 0 all normal, 1 has null or none
      uint16_t reserve : 11;
      uint16_t sver;
    };
  };
  uint32_t len;
  char     data[];
} STSRow;

// Target of tdataformat.h:
// 1. Row related definition in dataformat.h of 2.0 could be replaced with tdataformat.h of 3.0.
// 2. The basic definition in dataformat.h is shared with tdataformat.h of 3.0.

// row type
#define TD_ROW_TP 0x0U  // default
#define TD_ROW_KV 0x01U

#define TD_VTYPE_PARTS       4  // PARTITIONS: 1 byte / 2 bits
#define TD_VTYPE_OPTR        3  // OPERATOR: 4 - 1, utilize to get remainder
#define TD_BITMAP_BYTES(cnt) (((cnt) + TD_VTYPE_OPTR) >> 2)

#define TD_VTYPE_PARTS_I       8  // PARTITIONS: 1 byte / 1 bit
#define TD_VTYPE_OPTR_I        7  // OPERATOR: 8 - 1, utilize to get remainder
#define TD_BITMAP_BYTES_I(cnt) (((cnt) + TD_VTYPE_OPTR_I) >> 3)

/**
 * @brief value type
 *  - for data from client input and STSRow in memory, 3 types of value none/null/norm available
 */
#define TD_VTYPE_NORM 0x00U  // normal val: not none, not null
#define TD_VTYPE_NULL 0x01U  // null val
#define TD_VTYPE_NONE 0x02U  // none or unknown/undefined
#define TD_VTYPE_MAX  0x03U  //

#define TD_VTYPE_NORM_BYTE_I 0x0U
#define TD_VTYPE_NULL_BYTE_I 0xFFU

#define TD_VTYPE_NORM_BYTE_II 0x0U
#define TD_VTYPE_NULL_BYTE_II 0x55U
#define TD_VTYPE_NONE_BYTE_II 0xAAU

#define TD_ROWS_ALL_NORM  0x00U
#define TD_ROWS_NULL_NORM 0x01U

#define TD_COL_ROWS_NORM(c)          ((c)->bitmap == TD_ROWS_ALL_NORM)  // all rows of SDataCol/SBlockCol is NORM
#define TD_SET_COL_ROWS_BTIMAP(c, v) ((c)->bitmap = (v))
#define TD_SET_COL_ROWS_NORM(c)      TD_SET_COL_ROWS_BTIMAP((c), TD_ROWS_ALL_NORM)
#define TD_SET_COL_ROWS_MISC(c)      TD_SET_COL_ROWS_BTIMAP((c), TD_ROWS_NULL_NORM)

#define KvConvertRatio            (0.9f)
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
  bool     hasNone;
  bool     hasNull;
} SRowBuilder;

#define TD_ROW_HEAD_LEN  (sizeof(STSRow))
#define TD_ROW_NCOLS_LEN (sizeof(col_id_t))

#define TD_ROW_INFO(r)   ((r)->info)
#define TD_ROW_TYPE(r)   ((r)->type)
#define TD_ROW_DELETE(r) ((r)->del)
#define TD_ROW_ENDIAN(r) ((r)->endian)
#define TD_ROW_SVER(r)   ((r)->sver)
#define TD_ROW_NCOLS(r)  ((r)->data)  // only valid for SKvRow
#define TD_ROW_DATA(r)   ((r)->data)
#define TD_ROW_LEN(r)    ((r)->len)
#define TD_ROW_KEY(r)    ((r)->ts)
// #define TD_ROW_VER(r)      ((r)->ver)
#define TD_ROW_KEY_ADDR(r) (r)

// N.B. If without STSchema, insGetExtendedRowSize() is used to get the rowMaxBytes and
// (int32_t)ceil((double)nCols/TD_VTYPE_PARTS) should be added if TD_SUPPORT_BITMAP defined.
#define TD_ROW_MAX_BYTES_FROM_SCHEMA(s) ((s)->tlen + TD_ROW_HEAD_LEN)

#define TD_ROW_SET_INFO(r, i)  (TD_ROW_INFO(r) = (i))
#define TD_ROW_SET_TYPE(r, t)  (TD_ROW_TYPE(r) = (t))
#define TD_ROW_SET_DELETE(r)   (TD_ROW_DELETE(r) = 1)
#define TD_ROW_SET_SVER(r, v)  (TD_ROW_SVER(r) = (v))
#define TD_ROW_SET_LEN(r, l)   (TD_ROW_LEN(r) = (l))
#define TD_ROW_SET_NCOLS(r, n) (*(col_id_t *)TD_ROW_NCOLS(r) = (n))

#define TD_ROW_IS_DELETED(r) (TD_ROW_DELETE(r) == 1)
#define TD_IS_TP_ROW(r)      (TD_ROW_TYPE(r) == TD_ROW_TP)
#define TD_IS_KV_ROW(r)      (TD_ROW_TYPE(r) == TD_ROW_KV)
#define TD_IS_TP_ROW_T(t)    ((t) == TD_ROW_TP)
#define TD_IS_KV_ROW_T(t)    ((t) == TD_ROW_KV)

#define TD_BOOL_STR(b)       ((b) ? "true" : "false")
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

static FORCE_INLINE int16_t tdKvRowColIdAt(STSRow *pRow, col_id_t idx) {
  ASSERT(idx >= 0);
  if (idx == 0) {
    return PRIMARYKEY_TIMESTAMP_COL_ID;
  }

  return ((SKvRowIdx *)TD_ROW_COL_IDX(pRow) + idx - 1)->colId;
}

static FORCE_INLINE void *tdKVRowColVal(STSRow *pRow, SKvRowIdx *pIdx) { return POINTER_SHIFT(pRow, pIdx->offset); }

#define TD_ROW_OFFSET(p) ((p)->toffset);  // During ParseInsert when without STSchema, how to get the offset for STpRow?

void                        tdMergeBitmap(uint8_t *srcBitmap, int32_t nBits, uint8_t *dstBitmap);
static FORCE_INLINE void    tdRowCopy(void *dst, STSRow *row) { memcpy(dst, row, TD_ROW_LEN(row)); }
static FORCE_INLINE int32_t tdSetBitmapValTypeII(void *pBitmap, int16_t colIdx, TDRowValT valType);
static FORCE_INLINE int32_t tdSetBitmapValType(void *pBitmap, int16_t colIdx, TDRowValT valType, int8_t bitmapMode);
int32_t                     tdSetBitmapValTypeN(void *pBitmap, int16_t nEle, TDRowValT valType, int8_t bitmapMode);
static FORCE_INLINE int32_t tdGetBitmapValType(const void *pBitmap, int16_t colIdx, TDRowValT *pValType,
                                               int8_t bitmapMode);
bool                        tdIsBitmapBlkNorm(const void *pBitmap, int32_t numOfBits, int8_t bitmapMode);
// int32_t tdAppendValToDataCol(SDataCol *pCol, TDRowValT valType, const void *val, int32_t numOfRows, int32_t
// maxPoints,
//                              int8_t bitmapMode, bool isMerge);
// int32_t tdAppendSTSRowToDataCol(STSRow *pRow, STSchema *pSchema, SDataCols *pCols, bool isMerge);

int32_t tdGetBitmapValTypeII(const void *pBitmap, int16_t colIdx, TDRowValT *pValType);
int32_t tdSetBitmapValTypeI(void *pBitmap, int16_t colIdx, TDRowValT valType);
int32_t tdGetBitmapValTypeI(const void *pBitmap, int16_t colIdx, TDRowValT *pValType);

/**
 * @brief
 *
 * @param pRow
 * @param flen flen in STSchema
 * @return FORCE_INLINE*
 */
static FORCE_INLINE void *tdGetBitmapAddrTp(STSRow *pRow, uint32_t flen) {
  // The primary TS key is stored separatedly.
  return POINTER_SHIFT(TD_ROW_DATA(pRow), flen);
  // return POINTER_SHIFT(pRow->ts, flen);
}

static FORCE_INLINE void *tdGetBitmapAddrKv(STSRow *pRow, col_id_t nKvCols) {
  // The primary TS key is stored separatedly and is Norm value, thus should minus 1 firstly
  return POINTER_SHIFT(TD_ROW_COL_IDX(pRow), (--nKvCols) * sizeof(SKvRowIdx));
}
void   *tdGetBitmapAddr(STSRow *pRow, uint8_t rowType, uint32_t flen, col_id_t nKvCols);
int32_t tdSetBitmapValType(void *pBitmap, int16_t colIdx, TDRowValT valType, int8_t bitmapMode);
int32_t tdSetBitmapValTypeII(void *pBitmap, int16_t colIdx, TDRowValT valType);
// bool    tdIsBitmapValTypeNorm(const void *pBitmap, int16_t idx, int8_t bitmapMode);
int32_t tdGetBitmapValType(const void *pBitmap, int16_t colIdx, TDRowValT *pValType, int8_t bitmapMode);

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

static FORCE_INLINE void tdSRowInit(SRowBuilder *pBuilder, int16_t sver) {
  pBuilder->rowType = pBuilder->rowType;
  pBuilder->sver = sver;
}
int32_t                  tdSRowSetInfo(SRowBuilder *pBuilder, int32_t nCols, int32_t nBoundCols, int32_t flen);
int32_t                  tdSRowSetTpInfo(SRowBuilder *pBuilder, int32_t nCols, int32_t flen);
int32_t                  tdSRowSetExtendedInfo(SRowBuilder *pBuilder, int32_t nCols, int32_t nBoundCols, int32_t flen,
                                               int32_t allNullLen, int32_t boundNullLen);
int32_t                  tdSRowResetBuf(SRowBuilder *pBuilder, void *pBuf);
static FORCE_INLINE void tdSRowEnd(SRowBuilder *pBuilder) {
  STSRow *pRow = (STSRow *)pBuilder->pBuf;
  if (pBuilder->hasNull || pBuilder->hasNone) {
    pRow->statis = 1;
  }
}
int32_t tdSRowGetBuf(SRowBuilder *pBuilder, void *pBuf);
void    tdSRowReset(SRowBuilder *pBuilder);
int32_t tdAppendColValToTpRow(SRowBuilder *pBuilder, TDRowValT valType, const void *val, bool isCopyVarData,
                              int8_t colType, int16_t colIdx, int32_t offset);
int32_t tdAppendColValToKvRow(SRowBuilder *pBuilder, TDRowValT valType, const void *val, bool isCopyVarData,
                              int8_t colType, int16_t colIdx, int32_t offset, col_id_t colId);
int32_t tdAppendColValToRow(SRowBuilder *pBuilder, col_id_t colId, int8_t colType, TDRowValT valType, const void *val,
                            bool isCopyVarData, int32_t offset, col_id_t colIdx);
int32_t tdGetTpRowValOfCol(SCellVal *output, STSRow *pRow, void *pBitmap, int8_t colType, int32_t offset,
                           int16_t colIdx);
int32_t tdGetKvRowValOfCol(SCellVal *output, STSRow *pRow, void *pBitmap, int32_t offset, int16_t colIdx);
void    tTSRowGetVal(STSRow *pRow, STSchema *pTSchema, int16_t iCol, SColVal *pColVal);

typedef struct {
  STSchema *pSchema;
  STSRow   *pRow;
  void     *pBitmap;
  uint32_t  offset;
  col_id_t  maxColId;
  col_id_t  colIdx;  // [PRIMARYKEY_TIMESTAMP_COL_ID, nSchemaCols], PRIMARYKEY_TIMESTAMP_COL_ID equals 1
  col_id_t  kvIdx;   // [0, nKvCols)
} STSRowIter;

void tdSTSRowIterInit(STSRowIter *pIter, STSchema *pSchema);
void tdSTSRowIterReset(STSRowIter *pIter, STSRow *pRow);
bool tdSTSRowIterFetch(STSRowIter *pIter, col_id_t colId, col_type_t colType, SCellVal *pVal);
bool tdSTSRowIterNext(STSRowIter *pIter, SCellVal *pVal);

int32_t tdSTSRowNew(SArray *pArray, STSchema *pTSchema, STSRow **ppRow, int8_t rowType);
bool    tdSTSRowGetVal(STSRowIter *pIter, col_id_t colId, col_type_t colType, SCellVal *pVal);
void    tdSRowPrint(STSRow *row, STSchema *pSchema, const char *tag);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_ROW_H_*/
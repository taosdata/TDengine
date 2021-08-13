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
#ifndef _TD_DATA_FORMAT_H_
#define _TD_DATA_FORMAT_H_

#include "os.h"
#include "talgo.h"
#include "ttype.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

#define STR_TO_VARSTR(x, str)                     \
  do {                                            \
    VarDataLenT __len = (VarDataLenT)strlen(str); \
    *(VarDataLenT *)(x) = __len;                  \
    memcpy(varDataVal(x), (str), __len);          \
  } while (0);

#define STR_TO_NET_VARSTR(x, str)                   \
    do {                                            \
      VarDataLenT __len = (VarDataLenT)strlen(str); \
      *(VarDataLenT *)(x) = htons(__len);           \
      memcpy(varDataVal(x), (str), __len);          \
    } while (0);


#define STR_WITH_MAXSIZE_TO_VARSTR(x, str, _maxs)                         \
  do {                                                                    \
    char *_e = stpncpy(varDataVal(x), (str), (_maxs)-VARSTR_HEADER_SIZE); \
    varDataSetLen(x, (_e - (x)-VARSTR_HEADER_SIZE));                      \
  } while (0)

#define STR_WITH_SIZE_TO_VARSTR(x, str, _size)  \
  do {                                          \
    *(VarDataLenT *)(x) = (VarDataLenT)(_size); \
    memcpy(varDataVal(x), (str), (_size));      \
  } while (0);

// ----------------- TSDB COLUMN DEFINITION
typedef struct {
  int8_t   type;    // Column type
  int16_t  colId;   // column ID
  int16_t  bytes;   // column bytes (restore to int16_t in case of misuse)
  uint16_t offset;  // point offset in SDataRow after the header part.
} STColumn;

#define colType(col) ((col)->type)
#define colColId(col) ((col)->colId)
#define colBytes(col) ((col)->bytes)
#define colOffset(col) ((col)->offset)

#define colSetType(col, t) (colType(col) = (t))
#define colSetColId(col, id) (colColId(col) = (id))
#define colSetBytes(col, b) (colBytes(col) = (b))
#define colSetOffset(col, o) (colOffset(col) = (o))

// ----------------- TSDB SCHEMA DEFINITION
typedef struct {
  int      version;    // version
  int      numOfCols;  // Number of columns appended
  int      tlen;       // maximum length of a SDataRow without the header part (sizeof(VarDataOffsetT) + sizeof(VarDataLenT) + (bytes))
  uint16_t flen;       // First part length in a SDataRow after the header part
  uint16_t vlen;       // pure value part length, excluded the overhead (bytes only)
  STColumn columns[];
} STSchema;

#define schemaNCols(s) ((s)->numOfCols)
#define schemaVersion(s) ((s)->version)
#define schemaTLen(s) ((s)->tlen)
#define schemaFLen(s) ((s)->flen)
#define schemaVLen(s) ((s)->vlen)
#define schemaColAt(s, i) ((s)->columns + i)
#define tdFreeSchema(s) tfree((s))

STSchema *tdDupSchema(STSchema *pSchema);
int       tdEncodeSchema(void **buf, STSchema *pSchema);
void *    tdDecodeSchema(void *buf, STSchema **pRSchema);

static FORCE_INLINE int comparColId(const void *key1, const void *key2) {
  if (*(int16_t *)key1 > ((STColumn *)key2)->colId) {
    return 1;
  } else if (*(int16_t *)key1 < ((STColumn *)key2)->colId) {
    return -1;
  } else {
    return 0;
  }
}

static FORCE_INLINE STColumn *tdGetColOfID(STSchema *pSchema, int16_t colId) {
  void *ptr = bsearch(&colId, (void *)pSchema->columns, schemaNCols(pSchema), sizeof(STColumn), comparColId);
  if (ptr == NULL) return NULL;
  return (STColumn *)ptr;
}

// ----------------- SCHEMA BUILDER DEFINITION
typedef struct {
  int       tCols;
  int       nCols;
  int       tlen;
  uint16_t  flen;
  uint16_t  vlen;
  int       version;
  STColumn *columns;
} STSchemaBuilder;

int       tdInitTSchemaBuilder(STSchemaBuilder *pBuilder, int32_t version);
void      tdDestroyTSchemaBuilder(STSchemaBuilder *pBuilder);
void      tdResetTSchemaBuilder(STSchemaBuilder *pBuilder, int32_t version);
int       tdAddColToSchema(STSchemaBuilder *pBuilder, int8_t type, int16_t colId, int16_t bytes);
STSchema *tdGetSchemaFromBuilder(STSchemaBuilder *pBuilder);

// ----------------- Semantic timestamp key definition
typedef uint64_t TKEY;

#define TKEY_INVALID UINT64_MAX
#define TKEY_NULL TKEY_INVALID
#define TKEY_NEGATIVE_FLAG (((TKEY)1) << 63)
#define TKEY_DELETE_FLAG (((TKEY)1) << 62)
#define TKEY_VALUE_FILTER (~(TKEY_NEGATIVE_FLAG | TKEY_DELETE_FLAG))

#define TKEY_IS_NEGATIVE(tkey) (((tkey)&TKEY_NEGATIVE_FLAG) != 0)
#define TKEY_IS_DELETED(tkey) (((tkey)&TKEY_DELETE_FLAG) != 0)
#define tdSetTKEYDeleted(tkey) ((tkey) | TKEY_DELETE_FLAG)
#define tdGetTKEY(key) (((TKEY)ABS(key)) | (TKEY_NEGATIVE_FLAG & (TKEY)(key)))
#define tdGetKey(tkey) (((TSKEY)((tkey)&TKEY_VALUE_FILTER)) * (TKEY_IS_NEGATIVE(tkey) ? -1 : 1))

#define MIN_TS_KEY ((TSKEY)0x8000000000000001)
#define MAX_TS_KEY ((TSKEY)0x3fffffffffffffff)

#define TD_TO_TKEY(key) tdGetTKEY(((key) < MIN_TS_KEY) ? MIN_TS_KEY : (((key) > MAX_TS_KEY) ? MAX_TS_KEY : key))

static FORCE_INLINE TKEY keyToTkey(TSKEY key) {
  TSKEY lkey = key;
  if (key > MAX_TS_KEY) {
    lkey = MAX_TS_KEY;
  } else if (key < MIN_TS_KEY) {
    lkey = MIN_TS_KEY;
  }

  return tdGetTKEY(lkey);
}

static FORCE_INLINE int tkeyComparFn(const void *tkey1, const void *tkey2) {
  TSKEY key1 = tdGetKey(*(TKEY *)tkey1);
  TSKEY key2 = tdGetKey(*(TKEY *)tkey2);

  if (key1 < key2) {
    return -1;
  } else if (key1 > key2) {
    return 1;
  } else {
    return 0;
  }
}

// ----------------- Data row structure

/* A data row, the format is like below:
 * |<------------------------------------------------ len ---------------------------------->|
 * |<--     Head      -->|<---------   flen -------------->|                                 |
 * +---------------------+---------------------------------+---------------------------------+
 * | uint16_t |  int16_t |                                 |                                 |
 * +----------+----------+---------------------------------+---------------------------------+
 * |   len    | sversion |           First part            |             Second part         |
 * +----------+----------+---------------------------------+---------------------------------+
 *
 * NOTE: timestamp in this row structure is TKEY instead of TSKEY
 */
typedef void *SDataRow;

#define TD_DATA_ROW_HEAD_SIZE (sizeof(uint16_t) + sizeof(int16_t))

#define dataRowLen(r) (*(TDRowLenT *)(r))  // 0~65535
#define dataRowEnd(r) POINTER_SHIFT(r, dataRowLen(r))
#define dataRowVersion(r) (*(int16_t *)POINTER_SHIFT(r, sizeof(int16_t)))
#define dataRowTuple(r) POINTER_SHIFT(r, TD_DATA_ROW_HEAD_SIZE)
#define dataRowTKey(r) (*(TKEY *)(dataRowTuple(r)))
#define dataRowKey(r) tdGetKey(dataRowTKey(r))
#define dataRowSetLen(r, l) (dataRowLen(r) = (l))
#define dataRowSetVersion(r, v) (dataRowVersion(r) = (v))
#define dataRowCpy(dst, r) memcpy((dst), (r), dataRowLen(r))
#define dataRowMaxBytesFromSchema(s) (schemaTLen(s) + TD_DATA_ROW_HEAD_SIZE)
#define dataRowDeleted(r) TKEY_IS_DELETED(dataRowTKey(r))

SDataRow tdNewDataRowFromSchema(STSchema *pSchema);
void     tdFreeDataRow(SDataRow row);
void     tdInitDataRow(SDataRow row, STSchema *pSchema);
SDataRow tdDataRowDup(SDataRow row);


// offset here not include dataRow header length
static FORCE_INLINE int tdAppendDataColVal(SDataRow row, const void *value, bool isCopyVarData, int8_t type,
                                           int32_t offset) {
  ASSERT(value != NULL);
  int32_t toffset = offset + TD_DATA_ROW_HEAD_SIZE;

  if (IS_VAR_DATA_TYPE(type)) {
    *(VarDataOffsetT *)POINTER_SHIFT(row, toffset) = dataRowLen(row);
    if (isCopyVarData) {
      memcpy(POINTER_SHIFT(row, dataRowLen(row)), value, varDataTLen(value));
    }
    dataRowLen(row) += varDataTLen(value);
  } else {
    if (offset == 0) {
      ASSERT(type == TSDB_DATA_TYPE_TIMESTAMP);
      TKEY tvalue = tdGetTKEY(*(TSKEY *)value);
      memcpy(POINTER_SHIFT(row, toffset), (const void *)(&tvalue), TYPE_BYTES[type]);
    } else {
      memcpy(POINTER_SHIFT(row, toffset), value, TYPE_BYTES[type]);
    }
  }

  return 0;
}


// offset here not include dataRow header length
static FORCE_INLINE int tdAppendColVal(SDataRow row, const void *value, int8_t type, int32_t offset) {
  return tdAppendDataColVal(row, value, true, type, offset);
}

// NOTE: offset here including the header size
static FORCE_INLINE void *tdGetRowDataOfCol(SDataRow row, int8_t type, int32_t offset) {
  if (IS_VAR_DATA_TYPE(type)) {
    return POINTER_SHIFT(row, *(VarDataOffsetT *)POINTER_SHIFT(row, offset));
  } else {
    return POINTER_SHIFT(row, offset);
  }
}

static FORCE_INLINE void *tdGetPtrToCol(SDataRow row, STSchema *pSchema, int idx) {
  return POINTER_SHIFT(row, TD_DATA_ROW_HEAD_SIZE + pSchema->columns[idx].offset);
}

static FORCE_INLINE void *tdGetColOfRowBySchema(SDataRow row, STSchema *pSchema, int idx) {
  int16_t offset = TD_DATA_ROW_HEAD_SIZE + pSchema->columns[idx].offset;
  int8_t type = pSchema->columns[idx].type;

  return tdGetRowDataOfCol(row, type, offset);
}

static FORCE_INLINE bool tdIsColOfRowNullBySchema(SDataRow row, STSchema *pSchema, int idx) {
  int16_t offset = TD_DATA_ROW_HEAD_SIZE + pSchema->columns[idx].offset;
  int8_t type = pSchema->columns[idx].type;

  return isNull(tdGetRowDataOfCol(row, type, offset), type);
}

static FORCE_INLINE void tdSetColOfRowNullBySchema(SDataRow row, STSchema *pSchema, int idx) {
  int16_t offset = TD_DATA_ROW_HEAD_SIZE + pSchema->columns[idx].offset;
  int8_t type = pSchema->columns[idx].type;
  int16_t bytes = pSchema->columns[idx].bytes;

  setNull(tdGetRowDataOfCol(row, type, offset), type, bytes);
}

static FORCE_INLINE void tdCopyColOfRowBySchema(SDataRow dst, STSchema *pDstSchema, int dstIdx, SDataRow src, STSchema *pSrcSchema, int srcIdx) {
  int8_t type = pDstSchema->columns[dstIdx].type;
  ASSERT(type == pSrcSchema->columns[srcIdx].type);
  void *pData = tdGetPtrToCol(dst, pDstSchema, dstIdx);
  void *value = tdGetPtrToCol(src, pSrcSchema, srcIdx);

  switch (type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      *(VarDataOffsetT *)pData = *(VarDataOffsetT *)value;
      pData = POINTER_SHIFT(dst, *(VarDataOffsetT *)pData);
      value = POINTER_SHIFT(src, *(VarDataOffsetT *)value);
      memcpy(pData, value, varDataTLen(value));
      break;
    case TSDB_DATA_TYPE_NULL:
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT:
      *(uint8_t *)pData = *(uint8_t *)value;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT:
      *(uint16_t *)pData = *(uint16_t *)value;
      break;
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT:
      *(uint32_t *)pData = *(uint32_t *)value;
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UBIGINT:
      *(uint64_t *)pData = *(uint64_t *)value;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      SET_FLOAT_PTR(pData, value);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      SET_DOUBLE_PTR(pData, value);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      if (pSrcSchema->columns[srcIdx].colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        *(TSKEY *)pData = tdGetKey(*(TKEY *)value);
      } else {
        *(TSKEY *)pData = *(TSKEY *)value;
      }
      break;
    default:
      memcpy(pData, value, pSrcSchema->columns[srcIdx].bytes);
  }
}


// ----------------- Data column structure
typedef struct SDataCol {
  int8_t          type;       // column type
  int16_t         colId;      // column ID
  int             bytes;      // column data bytes defined
  int             offset;     // data offset in a SDataRow (including the header size)
  int             spaceSize;  // Total space size for this column
  int             len;        // column data length
  VarDataOffsetT *dataOff;    // For binary and nchar data, the offset in the data column
  void *          pData;      // Actual data pointer
  TSKEY           ts;         // only used in last NULL column
} SDataCol;

#define isAllRowsNull(pCol) ((pCol)->len == 0)
static FORCE_INLINE void dataColReset(SDataCol *pDataCol) { pDataCol->len = 0; }

int tdAllocMemForCol(SDataCol *pCol, int maxPoints);

void dataColInit(SDataCol *pDataCol, STColumn *pCol, int maxPoints);
int dataColAppendVal(SDataCol *pCol, const void *value, int numOfRows, int maxPoints);
void dataColSetOffset(SDataCol *pCol, int nEle);

bool isNEleNull(SDataCol *pCol, int nEle);

// Get the data pointer from a column-wised data
static FORCE_INLINE const void *tdGetColDataOfRow(SDataCol *pCol, int row) {
  if (isAllRowsNull(pCol)) {
    return getNullValue(pCol->type);
  }
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    return POINTER_SHIFT(pCol->pData, pCol->dataOff[row]);
  } else {
    return POINTER_SHIFT(pCol->pData, TYPE_BYTES[pCol->type] * row);
  }
}

static FORCE_INLINE int32_t dataColGetNEleLen(SDataCol *pDataCol, int rows) {
  ASSERT(rows > 0);

  if (IS_VAR_DATA_TYPE(pDataCol->type)) {
    return pDataCol->dataOff[rows - 1] + varDataTLen(tdGetColDataOfRow(pDataCol, rows - 1));
  } else {
    return TYPE_BYTES[pDataCol->type] * rows;
  }
}

typedef struct {
  int      maxCols;    // max number of columns
  int      maxPoints;  // max number of points
  int      numOfRows;
  int      numOfCols;  // Total number of cols
  int      sversion;   // TODO: set sversion
  SDataCol *cols;
} SDataCols;

#define keyCol(pCols) (&((pCols)->cols[0]))  // Key column
#define dataColsTKeyAt(pCols, idx) ((TKEY *)(keyCol(pCols)->pData))[(idx)]  // the idx row of column-wised data
#define dataColsKeyAt(pCols, idx) tdGetKey(dataColsTKeyAt(pCols, idx))
static FORCE_INLINE TKEY dataColsTKeyFirst(SDataCols *pCols) {
  if (pCols->numOfRows) {
    return dataColsTKeyAt(pCols, 0);
  } else {
    return TKEY_INVALID;
  }
}

static FORCE_INLINE TSKEY dataColsKeyAtRow(SDataCols *pCols, int row) {
  ASSERT(row < pCols->numOfRows);
  return dataColsKeyAt(pCols, row);
}

static FORCE_INLINE TSKEY dataColsKeyFirst(SDataCols *pCols) {
  if (pCols->numOfRows) {
    return dataColsKeyAt(pCols, 0);
  } else {
    return TSDB_DATA_TIMESTAMP_NULL;
  }
}

static FORCE_INLINE TKEY dataColsTKeyLast(SDataCols *pCols) {
  if (pCols->numOfRows) {
    return dataColsTKeyAt(pCols, pCols->numOfRows - 1);
  } else {
    return TKEY_INVALID;
  }
}

static FORCE_INLINE TSKEY dataColsKeyLast(SDataCols *pCols) {
  if (pCols->numOfRows) {
    return dataColsKeyAt(pCols, pCols->numOfRows - 1);
  } else {
    return TSDB_DATA_TIMESTAMP_NULL;
  }
}

SDataCols *tdNewDataCols(int maxCols, int maxRows);
void       tdResetDataCols(SDataCols *pCols);
int        tdInitDataCols(SDataCols *pCols, STSchema *pSchema);
SDataCols *tdDupDataCols(SDataCols *pCols, bool keepData);
SDataCols *tdFreeDataCols(SDataCols *pCols);
int        tdMergeDataCols(SDataCols *target, SDataCols *source, int rowsToMerge, int *pOffset, bool forceSetNull);

// ----------------- K-V data row structure
/* |<-------------------------------------- len -------------------------------------------->|
 * |<----- header  ----->|<--------------------------- body -------------------------------->|
 * +----------+----------+---------------------------------+---------------------------------+
 * | uint16_t |  int16_t |                                 |                                 |
 * +----------+----------+---------------------------------+---------------------------------+
 * |    len   |   ncols  |           cols index            |             data part           |
 * +----------+----------+---------------------------------+---------------------------------+
 */
typedef void *SKVRow;

typedef struct {
  int16_t  colId;
  uint16_t offset;
} SColIdx;

#define TD_KV_ROW_HEAD_SIZE (sizeof(uint16_t) + sizeof(int16_t))

#define kvRowLen(r) (*(TDRowLenT *)(r))
#define kvRowNCols(r) (*(int16_t *)POINTER_SHIFT(r, sizeof(uint16_t)))
#define kvRowSetLen(r, len) kvRowLen(r) = (len)
#define kvRowSetNCols(r, n) kvRowNCols(r) = (n)
#define kvRowColIdx(r) (SColIdx *)POINTER_SHIFT(r, TD_KV_ROW_HEAD_SIZE)
#define kvRowValues(r) POINTER_SHIFT(r, TD_KV_ROW_HEAD_SIZE + sizeof(SColIdx) * kvRowNCols(r))
#define kvRowCpy(dst, r) memcpy((dst), (r), kvRowLen(r))
#define kvRowColVal(r, colIdx) POINTER_SHIFT(kvRowValues(r), (colIdx)->offset)
#define kvRowColIdxAt(r, i) (kvRowColIdx(r) + (i))
#define kvRowFree(r) tfree(r)
#define kvRowEnd(r) POINTER_SHIFT(r, kvRowLen(r))
#define kvRowValLen(r) (kvRowLen(r) - TD_KV_ROW_HEAD_SIZE - sizeof(SColIdx) * kvRowNCols(r))
#define kvRowTKey(r) (*(TKEY *)(kvRowValues(r)))
#define kvRowKey(r) tdGetKey(kvRowTKey(r))
#define kvRowDeleted(r) TKEY_IS_DELETED(kvRowTKey(r))

SKVRow tdKVRowDup(SKVRow row);
int    tdSetKVRowDataOfCol(SKVRow *orow, int16_t colId, int8_t type, void *value);
int    tdEncodeKVRow(void **buf, SKVRow row);
void * tdDecodeKVRow(void *buf, SKVRow *row);
void   tdSortKVRowByColIdx(SKVRow row);

static FORCE_INLINE int comparTagId(const void *key1, const void *key2) {
  if (*(int16_t *)key1 > ((SColIdx *)key2)->colId) {
    return 1;
  } else if (*(int16_t *)key1 < ((SColIdx *)key2)->colId) {
    return -1;
  } else {
    return 0;
  }
}

static FORCE_INLINE void *tdGetKVRowValOfCol(SKVRow row, int16_t colId) {
  void *ret = taosbsearch(&colId, kvRowColIdx(row), kvRowNCols(row), sizeof(SColIdx), comparTagId, TD_EQ);
  if (ret == NULL) return NULL;
  return kvRowColVal(row, (SColIdx *)ret);
}

static FORCE_INLINE void *tdGetKVRowIdxOfCol(SKVRow row, int16_t colId) {
  return taosbsearch(&colId, kvRowColIdx(row), kvRowNCols(row), sizeof(SColIdx), comparTagId, TD_EQ);
}

// offset here not include kvRow header length
static FORCE_INLINE int tdAppendKvColVal(SKVRow row, const void *value, bool isCopyValData, int16_t colId, int8_t type,
                                         int32_t offset) {
  ASSERT(value != NULL);
  int32_t  toffset = offset + TD_KV_ROW_HEAD_SIZE;
  SColIdx *pColIdx = (SColIdx *)POINTER_SHIFT(row, toffset);
  char *   ptr = (char *)POINTER_SHIFT(row, kvRowLen(row));

  pColIdx->colId = colId;
  pColIdx->offset = kvRowLen(row);  // offset of pColIdx including the TD_KV_ROW_HEAD_SIZE

  if (IS_VAR_DATA_TYPE(type)) {
    if (isCopyValData) {
      memcpy(ptr, value, varDataTLen(value));
    }
    kvRowLen(row) += varDataTLen(value);
  } else {
    if (offset == 0) {
      ASSERT(type == TSDB_DATA_TYPE_TIMESTAMP);
      TKEY tvalue = tdGetTKEY(*(TSKEY *)value);
      memcpy(ptr, (void *)(&tvalue), TYPE_BYTES[type]);
    } else {
      memcpy(ptr, value, TYPE_BYTES[type]);
    }
    kvRowLen(row) += TYPE_BYTES[type];
  }

  return 0;
}
// NOTE: offset here including the header size
static FORCE_INLINE void *tdGetKvRowDataOfCol(void *row, int32_t offset) { return POINTER_SHIFT(row, offset); }

static FORCE_INLINE void *tdGetKVRowValOfColEx(SKVRow row, int16_t colId, int32_t *nIdx) {
  while (*nIdx < kvRowNCols(row)) {
    SColIdx *pColIdx = kvRowColIdxAt(row, *nIdx);
    if (pColIdx->colId == colId) {
      ++(*nIdx);
      return tdGetKvRowDataOfCol(row, pColIdx->offset);
    } else if (pColIdx->colId > colId) {
      return NULL;
    } else {
      ++(*nIdx);
    }
  }
  return NULL;
}

// ----------------- K-V data row builder
typedef struct {
  int16_t  tCols;
  int16_t  nCols;
  SColIdx *pColIdx;
  uint16_t alloc;
  uint16_t size;
  void *   buf;
} SKVRowBuilder;

int    tdInitKVRowBuilder(SKVRowBuilder *pBuilder);
void   tdDestroyKVRowBuilder(SKVRowBuilder *pBuilder);
void   tdResetKVRowBuilder(SKVRowBuilder *pBuilder);
SKVRow tdGetKVRowFromBuilder(SKVRowBuilder *pBuilder);

static FORCE_INLINE int tdAddColToKVRow(SKVRowBuilder *pBuilder, int16_t colId, int8_t type, void *value) {
  if (pBuilder->nCols >= pBuilder->tCols) {
    pBuilder->tCols *= 2;
    pBuilder->pColIdx = (SColIdx *)realloc((void *)(pBuilder->pColIdx), sizeof(SColIdx) * pBuilder->tCols);
    if (pBuilder->pColIdx == NULL) return -1;
  }

  pBuilder->pColIdx[pBuilder->nCols].colId = colId;
  pBuilder->pColIdx[pBuilder->nCols].offset = pBuilder->size;

  pBuilder->nCols++;

  int tlen = IS_VAR_DATA_TYPE(type) ? varDataTLen(value) : TYPE_BYTES[type];
  if (tlen > pBuilder->alloc - pBuilder->size) {
    while (tlen > pBuilder->alloc - pBuilder->size) {
      pBuilder->alloc *= 2;
    }
    pBuilder->buf = realloc(pBuilder->buf, pBuilder->alloc);
    if (pBuilder->buf == NULL) return -1;
  }

  memcpy(POINTER_SHIFT(pBuilder->buf, pBuilder->size), value, tlen);
  pBuilder->size += tlen;

  return 0;
}

// ----------------- SMemRow appended with sequential data row structure
/*
 * |---------|------------------------------------------------- len ---------------------------------->|
 * |<--------     Head      ------>|<---------   flen -------------->|                                 |
 * |---------+---------------------+---------------------------------+---------------------------------+
 * | uint8_t | uint16_t |  int16_t |                                 |                                 |
 * |---------+----------+----------+---------------------------------+---------------------------------+
 * |  flag   |   len    | sversion |           First part            |             Second part         |
 * +---------+----------+----------+---------------------------------+---------------------------------+
 *
 * NOTE: timestamp in this row structure is TKEY instead of TSKEY
 */

// ----------------- SMemRow appended with extended K-V data row structure
/* |--------------------|------------------------------------------------  len ---------------------------------->|
 * |<-------------     Head      ------------>|<---------   flen -------------->|                                 |
 * |--------------------+----------+--------------------------------------------+---------------------------------+
 * | uint8_t | int16_t  | uint16_t |  int16_t |                                 |                                 |
 * |---------+----------+----------+----------+---------------------------------+---------------------------------+
 * |   flag  | sversion |   len    |   ncols  |           cols index            |             data part           |
 * |---------+----------+----------+----------+---------------------------------+---------------------------------+
 */

typedef void *SMemRow;

#define TD_MEM_ROW_TYPE_SIZE sizeof(uint8_t)
#define TD_MEM_ROW_KV_VER_SIZE sizeof(int16_t)
#define TD_MEM_ROW_KV_TYPE_VER_SIZE (TD_MEM_ROW_TYPE_SIZE + TD_MEM_ROW_KV_VER_SIZE)
#define TD_MEM_ROW_DATA_HEAD_SIZE (TD_MEM_ROW_TYPE_SIZE + TD_DATA_ROW_HEAD_SIZE)
#define TD_MEM_ROW_KV_HEAD_SIZE (TD_MEM_ROW_TYPE_SIZE + TD_MEM_ROW_KV_VER_SIZE + TD_KV_ROW_HEAD_SIZE)

#define SMEM_ROW_DATA 0x0U      // SDataRow
#define SMEM_ROW_KV 0x01U       // SKVRow
#define SMEM_ROW_CONVERT 0x80U  // SMemRow convert flag

#define KVRatioKV (0.2f)  // all bool
#define KVRatioPredict (0.4f)
#define KVRatioData (0.75f)  // all bigint
#define KVRatioConvert (0.9f)

#define memRowType(r) ((*(uint8_t *)(r)) & 0x01)

#define memRowSetType(r, t) ((*(uint8_t *)(r)) = (t))  // set the total byte in case of dirty memory
#define memRowSetConvert(r) ((*(uint8_t *)(r)) = (((*(uint8_t *)(r)) & 0x7F) | SMEM_ROW_CONVERT))  // highest bit
#define isDataRowT(t) (SMEM_ROW_DATA == (((uint8_t)(t)) & 0x01))
#define isDataRow(r) (SMEM_ROW_DATA == memRowType(r))
#define isKvRowT(t) (SMEM_ROW_KV == (((uint8_t)(t)) & 0x01))
#define isKvRow(r) (SMEM_ROW_KV == memRowType(r))
#define isNeedConvertRow(r) (((*(uint8_t *)(r)) & 0x80) == SMEM_ROW_CONVERT)

#define memRowDataBody(r) POINTER_SHIFT(r, TD_MEM_ROW_TYPE_SIZE)  // section after flag
#define memRowKvBody(r) \
  POINTER_SHIFT(r, TD_MEM_ROW_KV_TYPE_VER_SIZE)  // section after flag + sversion as to reuse SKVRow

#define memRowDataLen(r) (*(TDRowLenT *)memRowDataBody(r))  //  0~65535
#define memRowKvLen(r) (*(TDRowLenT *)memRowKvBody(r))      //  0~65535

#define memRowDataTLen(r) \
  ((TDRowTLenT)(memRowDataLen(r) + TD_MEM_ROW_TYPE_SIZE))  // using uint32_t/int32_t to store the TLen

#define memRowKvTLen(r) ((TDRowTLenT)(memRowKvLen(r) + TD_MEM_ROW_KV_TYPE_VER_SIZE))

#define memRowLen(r) (isDataRow(r) ? memRowDataLen(r) : memRowKvLen(r))
#define memRowTLen(r) (isDataRow(r) ? memRowDataTLen(r) : memRowKvTLen(r))  // using uint32_t/int32_t to store the TLen

static FORCE_INLINE char *memRowEnd(SMemRow row) {
  if (isDataRow(row)) {
    return (char *)dataRowEnd(memRowDataBody(row));
  } else {
    return (char *)kvRowEnd(memRowKvBody(row));
  }
}

#define memRowDataVersion(r) dataRowVersion(memRowDataBody(r))
#define memRowKvVersion(r) (*(int16_t *)POINTER_SHIFT(r, TD_MEM_ROW_TYPE_SIZE))
#define memRowVersion(r) (isDataRow(r) ? memRowDataVersion(r) : memRowKvVersion(r))  // schema version
#define memRowSetKvVersion(r, v) (memRowKvVersion(r) = (v))
#define memRowTuple(r) (isDataRow(r) ? dataRowTuple(memRowDataBody(r)) : kvRowValues(memRowKvBody(r)))

#define memRowTKey(r) (isDataRow(r) ? dataRowTKey(memRowDataBody(r)) : kvRowTKey(memRowKvBody(r)))
#define memRowKey(r) (isDataRow(r) ? dataRowKey(memRowDataBody(r)) : kvRowKey(memRowKvBody(r)))
#define memRowSetTKey(r, k)                 \
  do {                                      \
    if (isDataRow(r)) {                     \
      dataRowTKey(memRowDataBody(r)) = (k); \
    } else {                                \
      kvRowTKey(memRowKvBody(r)) = (k);     \
    }                                       \
  } while (0)

#define memRowSetLen(r, l) (isDataRow(r) ? memRowDataLen(r) = (l) : memRowKvLen(r) = (l))
#define memRowSetVersion(r, v) (isDataRow(r) ? dataRowSetVersion(memRowDataBody(r), v) : memRowSetKvVersion(r, v))
#define memRowCpy(dst, r) memcpy((dst), (r), memRowTLen(r))
#define memRowMaxBytesFromSchema(s) (schemaTLen(s) + TD_MEM_ROW_DATA_HEAD_SIZE)
#define memRowDeleted(r) TKEY_IS_DELETED(memRowTKey(r))

SMemRow tdMemRowDup(SMemRow row);
void    tdAppendMemRowToDataCol(SMemRow row, STSchema *pSchema, SDataCols *pCols, bool forceSetNull);

// NOTE: offset here including the header size
static FORCE_INLINE void *tdGetMemRowDataOfCol(void *row, int16_t colId, int8_t colType, uint16_t offset) {
  if (isDataRow(row)) {
    return tdGetRowDataOfCol(memRowDataBody(row), colType, offset);
  } else {
    return tdGetKVRowValOfCol(memRowKvBody(row), colId);
  }
}

/**
 * NOTE:
 *  1. Applicable to scan columns one by one
 *  2. offset here including the header size
 */
static FORCE_INLINE void *tdGetMemRowDataOfColEx(void *row, int16_t colId, int8_t colType, int32_t offset,
                                                 int32_t *kvNIdx) {
  if (isDataRow(row)) {
    return tdGetRowDataOfCol(memRowDataBody(row), colType, offset);
  } else {
    return tdGetKVRowValOfColEx(memRowKvBody(row), colId, kvNIdx);
  }
}

static FORCE_INLINE int tdAppendMemRowColVal(SMemRow row, const void *value, bool isCopyVarData, int16_t colId,
                                             int8_t type, int32_t offset) {
  if (isDataRow(row)) {
    tdAppendDataColVal(memRowDataBody(row), value, isCopyVarData, type, offset);
  } else {
    tdAppendKvColVal(memRowKvBody(row), value, isCopyVarData, colId, type, offset);
  }
  return 0;
}

// make sure schema->flen appended for SDataRow
static FORCE_INLINE int32_t tdGetColAppendLen(uint8_t rowType, const void *value, int8_t colType) {
  int32_t len = 0;
  if (IS_VAR_DATA_TYPE(colType)) {
    len += varDataTLen(value);
    if (rowType == SMEM_ROW_KV) {
      len += sizeof(SColIdx);
    }
  } else {
    if (rowType == SMEM_ROW_KV) {
      len += TYPE_BYTES[colType];
      len += sizeof(SColIdx);
    }
  }
  return len;
}

/**
 * 1. calculate the delta of AllNullLen for SDataRow.
 * 2. calculate the real len for SKVRow.
 */
static FORCE_INLINE void tdGetColAppendDeltaLen(const void *value, int8_t colType, int32_t *dataLen, int32_t *kvLen) {
  switch (colType) {
    case TSDB_DATA_TYPE_BINARY: {
      int32_t varLen = varDataLen(value);
      *dataLen += (varLen - CHAR_BYTES);
      *kvLen += (varLen + sizeof(SColIdx));
      break;
    }
    case TSDB_DATA_TYPE_NCHAR: {
      int32_t varLen = varDataLen(value);
      *dataLen += (varLen - TSDB_NCHAR_SIZE);
      *kvLen += (varLen + sizeof(SColIdx));
      break;
    }
    default: {
      *kvLen += (TYPE_BYTES[colType] + sizeof(SColIdx));
      break;
    }
  }
}

typedef struct {
  int16_t colId;
  uint8_t colType;
  char*   colVal;
} SColInfo;

static FORCE_INLINE void setSColInfo(SColInfo* colInfo, int16_t colId, uint8_t colType, char* colVal) {
  colInfo->colId = colId;
  colInfo->colType = colType;
  colInfo->colVal = colVal;
}

SMemRow mergeTwoMemRows(void *buffer, SMemRow row1, SMemRow row2, STSchema *pSchema1, STSchema *pSchema2);

#if 0
// ----------------- Raw payload structure for row:
/* |<------------ Head ------------->|<----------- body of column data tuple ------------------->|
 * |                                 |<----------------- flen ------------->|<--- value part --->|
 * |SMemRowType| dataTLen |  nCols   |  colId  | colType | offset   |  ...  | value |...|...|... |
 * +-----------+----------+----------+--------------------------------------|--------------------|
 * | uint8_t   | uint32_t | uint16_t | int16_t | uint8_t | uint16_t |  ...  |.......|...|...|... |
 * +-----------+----------+----------+--------------------------------------+--------------------|
 *  1. offset in column data tuple starts from the value part in case of uint16_t overflow.
 *  2. dataTLen: total length including the header and body.
 */

#define PAYLOAD_NCOLS_LEN sizeof(uint16_t)
#define PAYLOAD_NCOLS_OFFSET (sizeof(uint8_t) + sizeof(TDRowTLenT))
#define PAYLOAD_HEADER_LEN (PAYLOAD_NCOLS_OFFSET + PAYLOAD_NCOLS_LEN)
#define PAYLOAD_ID_LEN sizeof(int16_t)
#define PAYLOAD_ID_TYPE_LEN (sizeof(int16_t) + sizeof(uint8_t))
#define PAYLOAD_COL_HEAD_LEN (PAYLOAD_ID_TYPE_LEN + sizeof(uint16_t))
#define PAYLOAD_PRIMARY_COL_LEN (PAYLOAD_ID_TYPE_LEN + sizeof(TSKEY))

#define payloadBody(r) POINTER_SHIFT(r, PAYLOAD_HEADER_LEN)
#define payloadType(r) (*(uint8_t *)(r))
#define payloadSetType(r, t) (payloadType(r) = (t))
#define payloadTLen(r) (*(TDRowTLenT *)POINTER_SHIFT(r, TD_MEM_ROW_TYPE_SIZE))  // including total header
#define payloadSetTLen(r, l) (payloadTLen(r) = (l))
#define payloadNCols(r) (*(TDRowLenT *)POINTER_SHIFT(r, PAYLOAD_NCOLS_OFFSET))
#define payloadSetNCols(r, n) (payloadNCols(r) = (n))
#define payloadValuesOffset(r) \
  (PAYLOAD_HEADER_LEN + payloadNCols(r) * PAYLOAD_COL_HEAD_LEN)    // avoid using the macro in loop
#define payloadValues(r) POINTER_SHIFT(r, payloadValuesOffset(r))  // avoid using the macro in loop
#define payloadColId(c) (*(int16_t *)(c))
#define payloadColType(c) (*(uint8_t *)POINTER_SHIFT(c, PAYLOAD_ID_LEN))
#define payloadColOffset(c) (*(uint16_t *)POINTER_SHIFT(c, PAYLOAD_ID_TYPE_LEN))
#define payloadColValue(c) POINTER_SHIFT(c, payloadColOffset(c))

#define payloadColSetId(c, i) (payloadColId(c) = (i))
#define payloadColSetType(c, t) (payloadColType(c) = (t))
#define payloadColSetOffset(c, o) (payloadColOffset(c) = (o))

#define payloadTSKey(r) (*(TSKEY *)POINTER_SHIFT(r, payloadValuesOffset(r)))
#define payloadTKey(r) (*(TKEY *)POINTER_SHIFT(r, payloadValuesOffset(r)))
#define payloadKey(r) tdGetKey(payloadTKey(r))


static FORCE_INLINE char *payloadNextCol(char *pCol) { return (char *)POINTER_SHIFT(pCol, PAYLOAD_COL_HEAD_LEN); }

#endif

#ifdef __cplusplus
}
#endif

#endif  // _TD_DATA_FORMAT_H_

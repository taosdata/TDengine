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

#define _DEFAULT_SOURCE
#include "tdataformat.h"
#include "decimal.h"
#include "tRealloc.h"
#include "tdatablock.h"
#include "tlog.h"

enum {
  BLOB_ROW_KV = 0,
  BLOB_ROW_TUPLE = 1,
};

static int32_t (*tColDataAppendValueImpl[8][3])(SColData *pColData, uint8_t *pData, uint32_t nData);
static int32_t (*tColDataUpdateValueImpl[8][3])(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward);

static int32_t tRowMergeWithBlobImpl(SArray *aRowP, STSchema *pTSchema, SBlobSet *pBlob, SBlobSet *pDstBlobSet,
                                     int32_t iStart, int32_t iEnd, int8_t flag);

static int32_t tRowBuildTupleWithBlob2(SArray *aColVal, const SRowBuildScanInfo *sinfo, const STSchema *schema,
                                       SRow **ppRow, SBlobSet *pSrcBlobSet, SBlobSet *pDstBlobSet);

static int32_t tRowBuildKVRowWithBlob2(SArray *aColVal, const SRowBuildScanInfo *sinfo, const STSchema *schema,
                                       SRow **ppRow, SBlobSet *pSrcBlob, SBlobSet *pDstBlobSet);
// ================================
static int32_t tGetTagVal(uint8_t *p, STagVal *pTagVal, int8_t isJson);

// SRow ========================================================================
#define KV_FLG_LIT ((uint8_t)0x10)
#define KV_FLG_MID ((uint8_t)0x20)
#define KV_FLG_BIG ((uint8_t)0x40)

#define BIT_FLG_NONE  ((uint8_t)0x0)
#define BIT_FLG_NULL  ((uint8_t)0x1)
#define BIT_FLG_VALUE ((uint8_t)0x2)

#pragma pack(push, 1)
typedef struct {
  int16_t nCol;
  uint8_t idx[];  // uint8_t * | uint16_t * | uint32_t *
} SKVIdx;
#pragma pack(pop)

#define ROW_SET_BITMAP(PB, FLAG, IDX, VAL)      \
  do {                                          \
    switch (FLAG) {                             \
      case (HAS_NULL | HAS_NONE):               \
        SET_BIT1(PB, IDX, VAL);                 \
        break;                                  \
      case (HAS_VALUE | HAS_NONE):              \
        SET_BIT1(PB, IDX, (VAL) ? (VAL)-1 : 0); \
        break;                                  \
      case (HAS_VALUE | HAS_NULL):              \
        SET_BIT1(PB, IDX, (VAL)-1);             \
        break;                                  \
      case (HAS_VALUE | HAS_NULL | HAS_NONE):   \
        SET_BIT2(PB, IDX, VAL);                 \
        break;                                  \
      default:                                  \
        break;                                  \
    }                                           \
  } while (0)

static int32_t tPutPrimaryKeyIndex(uint8_t *p, const SPrimaryKeyIndex *index) {
  int32_t n = 0;
  n += tPutI8(p ? p + n : p, index->type);
  n += tPutU32v(p ? p + n : p, index->offset);
  return n;
}

static int32_t tGetPrimaryKeyIndex(uint8_t *p, SPrimaryKeyIndex *index) {
  int32_t n = 0;
  n += tGetI8(p + n, &index->type);
  n += tGetU32v(p + n, &index->offset);
  return n;
}

static FORCE_INLINE int32_t tRowBuildScanAddNone(SRowBuildScanInfo *sinfo, const STColumn *pTColumn) {
  if ((pTColumn->flags & COL_IS_KEY)) return TSDB_CODE_PAR_PRIMARY_KEY_IS_NONE;
  sinfo->numOfNone++;
  return 0;
}

static FORCE_INLINE int32_t tRowBuildScanAddNull(SRowBuildScanInfo *sinfo, const STColumn *pTColumn) {
  if ((pTColumn->flags & COL_IS_KEY)) return TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
  sinfo->numOfNull++;
  sinfo->kvMaxOffset = sinfo->kvPayloadSize;
  sinfo->kvPayloadSize += tPutI16v(NULL, -pTColumn->colId);
  return 0;
}

static FORCE_INLINE void tRowBuildScanAddValue(SRowBuildScanInfo *sinfo, SColVal *colVal, const STColumn *pTColumn) {
  bool isPK = ((pTColumn->flags & COL_IS_KEY) != 0);

  if (isPK) {
    sinfo->tupleIndices[sinfo->numOfPKs].type = colVal->value.type;
    sinfo->tupleIndices[sinfo->numOfPKs].offset =
        IS_VAR_DATA_TYPE(pTColumn->type) ? sinfo->tupleVarSize + sinfo->tupleFixedSize : pTColumn->offset;
    sinfo->kvIndices[sinfo->numOfPKs].type = colVal->value.type;
    sinfo->kvIndices[sinfo->numOfPKs].offset = sinfo->kvPayloadSize;
    sinfo->numOfPKs++;
  }

  sinfo->kvMaxOffset = sinfo->kvPayloadSize;
  if (IS_VAR_DATA_TYPE(colVal->value.type)) {
    if (sinfo->hasBlob && IS_STR_DATA_BLOB(colVal->value.type)) {
      sinfo->tupleVarSize += tPutU32v(NULL, colVal->value.nData)     // size
                             + BSE_SEQUECE_SIZE;                     // value
      sinfo->kvPayloadSize += tPutI16v(NULL, colVal->cid)            // colId
                              + tPutU32v(NULL, colVal->value.nData)  // size
                              + BSE_SEQUECE_SIZE;                    // seq offset
    } else {
      sinfo->tupleVarSize += tPutU32v(NULL, colVal->value.nData)  // size
                             + colVal->value.nData;               // value

      sinfo->kvPayloadSize += tPutI16v(NULL, colVal->cid)            // colId
                              + tPutU32v(NULL, colVal->value.nData)  // size
                              + colVal->value.nData;                 // value
    }
  } else {
    sinfo->kvPayloadSize += tPutI16v(NULL, colVal->cid)              // colId
                            + tDataTypes[colVal->value.type].bytes;  // value
  }
  sinfo->numOfValue++;
}

static int32_t tRowBuildScan(SArray *colVals, const STSchema *schema, SRowBuildScanInfo *sinfo) {
  int32_t  code = 0;
  int32_t  colValIndex = 1;
  int32_t  numOfColVals = TARRAY_SIZE(colVals);
  SColVal *colValArray = (SColVal *)TARRAY_DATA(colVals);

  if (!(numOfColVals > 0)) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (!(colValArray[0].cid == PRIMARYKEY_TIMESTAMP_COL_ID)) {
    return TSDB_CODE_PAR_INVALID_FIRST_COLUMN;
  }
  if (!(colValArray[0].value.type == TSDB_DATA_TYPE_TIMESTAMP)) {
    return TSDB_CODE_PAR_INVALID_FIRST_COLUMN;
  }

  *sinfo = (SRowBuildScanInfo){.tupleFixedSize = schema->flen, .hasBlob = sinfo->hasBlob, .scanType = sinfo->scanType};

  // loop scan
  for (int32_t i = 1; i < schema->numOfCols; i++) {
    for (;;) {
      if (colValIndex >= numOfColVals) {
        if ((code = tRowBuildScanAddNone(sinfo, schema->columns + i))) goto _exit;
        break;
      }

      if (colValArray[colValIndex].cid == schema->columns[i].colId) {
        if (!(colValArray[colValIndex].value.type == schema->columns[i].type)) {
          code = TSDB_CODE_INVALID_PARA;
          goto _exit;
        }

        if (COL_VAL_IS_VALUE(&colValArray[colValIndex])) {
          tRowBuildScanAddValue(sinfo, &colValArray[colValIndex], schema->columns + i);
        } else if (COL_VAL_IS_NULL(&colValArray[colValIndex])) {
          if ((code = tRowBuildScanAddNull(sinfo, schema->columns + i))) goto _exit;
        } else if (COL_VAL_IS_NONE(&colValArray[colValIndex])) {
          if ((code = tRowBuildScanAddNone(sinfo, schema->columns + i))) goto _exit;
        }

        colValIndex++;
        break;
      } else if (colValArray[colValIndex].cid > schema->columns[i].colId) {
        if ((code = tRowBuildScanAddNone(sinfo, schema->columns + i))) goto _exit;
        break;
      } else {  // skip useless value
        colValIndex++;
      }
    }
  }

  if (sinfo->numOfNone) {
    sinfo->flag |= HAS_NONE;
  }
  if (sinfo->numOfNull) {
    sinfo->flag |= HAS_NULL;
  }
  if (sinfo->numOfValue) {
    sinfo->flag |= HAS_VALUE;
  }

  // Tuple
  sinfo->tupleFlag = sinfo->flag;
  switch (sinfo->flag) {
    case HAS_NONE:
    case HAS_NULL:
      sinfo->tupleBitmapSize = 0;
      sinfo->tupleFixedSize = 0;
      break;
    case HAS_VALUE:
      sinfo->tupleBitmapSize = 0;
      sinfo->tupleFixedSize = schema->flen;
      break;
    case (HAS_NONE | HAS_NULL):
      sinfo->tupleBitmapSize = BIT1_SIZE(schema->numOfCols - 1);
      sinfo->tupleFixedSize = 0;
      break;
    case (HAS_NONE | HAS_VALUE):
    case (HAS_NULL | HAS_VALUE):
      sinfo->tupleBitmapSize = BIT1_SIZE(schema->numOfCols - 1);
      sinfo->tupleFixedSize = schema->flen;
      break;
    case (HAS_NONE | HAS_NULL | HAS_VALUE):
      sinfo->tupleBitmapSize = BIT2_SIZE(schema->numOfCols - 1);
      sinfo->tupleFixedSize = schema->flen;
      break;
  }
  for (int32_t i = 0; i < sinfo->numOfPKs; i++) {
    sinfo->tupleIndices[i].offset += sinfo->tupleBitmapSize;
    sinfo->tuplePKSize += tPutPrimaryKeyIndex(NULL, sinfo->tupleIndices + i);
  }
  sinfo->tupleRowSize = sizeof(SRow)              // SRow
                        + sinfo->tuplePKSize      // primary keys
                        + sinfo->tupleBitmapSize  // bitmap
                        + sinfo->tupleFixedSize   // fixed part
                        + sinfo->tupleVarSize;    // var part

  // Key-Value
  if (sinfo->kvMaxOffset <= UINT8_MAX) {
    sinfo->kvFlag = (KV_FLG_LIT | sinfo->flag);
    sinfo->kvIndexSize = sizeof(SKVIdx) + (sinfo->numOfNull + sinfo->numOfValue) * sizeof(uint8_t);
  } else if (sinfo->kvMaxOffset <= UINT16_MAX) {
    sinfo->kvFlag = (KV_FLG_MID | sinfo->flag);
    sinfo->kvIndexSize = sizeof(SKVIdx) + (sinfo->numOfNull + sinfo->numOfValue) * sizeof(uint16_t);
  } else {
    sinfo->kvFlag = (KV_FLG_BIG | sinfo->flag);
    sinfo->kvIndexSize = sizeof(SKVIdx) + (sinfo->numOfNull + sinfo->numOfValue) * sizeof(uint32_t);
  }
  for (int32_t i = 0; i < sinfo->numOfPKs; i++) {
    sinfo->kvIndices[i].offset += sinfo->kvIndexSize;
    sinfo->kvPKSize += tPutPrimaryKeyIndex(NULL, sinfo->kvIndices + i);
  }
  sinfo->kvRowSize = sizeof(SRow)             // SRow
                     + sinfo->kvPKSize        // primary keys
                     + sinfo->kvIndexSize     // index array
                     + sinfo->kvPayloadSize;  // payload

_exit:
  return code;
}

static int32_t tRowBuildTupleRow(SArray *aColVal, const SRowBuildScanInfo *sinfo, const STSchema *schema,
                                 SRow **ppRow) {
  SColVal *colValArray = (SColVal *)TARRAY_DATA(aColVal);
  *ppRow = (SRow *)taosMemoryCalloc(1, sinfo->tupleRowSize);
  if (*ppRow == NULL) {
    return terrno;
  }
  (*ppRow)->flag = sinfo->tupleFlag;
  (*ppRow)->numOfPKs = sinfo->numOfPKs;
  (*ppRow)->sver = schema->version;
  (*ppRow)->len = sinfo->tupleRowSize;
  (*ppRow)->ts = VALUE_GET_TRIVIAL_DATUM(&colValArray[0].value);

  if (sinfo->tupleFlag == HAS_NONE || sinfo->tupleFlag == HAS_NULL) {
    return 0;
  }

  uint8_t *primaryKeys = (*ppRow)->data;
  uint8_t *bitmap = primaryKeys + sinfo->tuplePKSize;
  uint8_t *fixed = bitmap + sinfo->tupleBitmapSize;
  uint8_t *varlen = fixed + sinfo->tupleFixedSize;

  // primary keys
  for (int32_t i = 0; i < sinfo->numOfPKs; i++) {
    primaryKeys += tPutPrimaryKeyIndex(primaryKeys, sinfo->tupleIndices + i);
  }

  // bitmap + fixed + varlen
  int32_t numOfColVals = TARRAY_SIZE(aColVal);
  int32_t colValIndex = 1;
  for (int32_t i = 1; i < schema->numOfCols; i++) {
    for (;;) {
      if (colValIndex >= numOfColVals) {  // NONE
        ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NONE);
        break;
      }

      if (colValArray[colValIndex].cid == schema->columns[i].colId) {
        if (COL_VAL_IS_VALUE(&colValArray[colValIndex])) {  // value
          ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_VALUE);

          if (IS_VAR_DATA_TYPE(schema->columns[i].type)) {
            *(int32_t *)(fixed + schema->columns[i].offset) = varlen - fixed - sinfo->tupleFixedSize;
            varlen += tPutU32v(varlen, colValArray[colValIndex].value.nData);
            if (colValArray[colValIndex].value.nData) {
              (void)memcpy(varlen, colValArray[colValIndex].value.pData, colValArray[colValIndex].value.nData);
              varlen += colValArray[colValIndex].value.nData;
            }
          } else {
            (void)memcpy(fixed + schema->columns[i].offset,
                         VALUE_GET_DATUM(&colValArray[colValIndex].value, schema->columns[i].type),
                         tDataTypes[schema->columns[i].type].bytes);
          }
        } else if (COL_VAL_IS_NULL(&colValArray[colValIndex])) {  // NULL
          ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NULL);
        } else if (COL_VAL_IS_NONE(&colValArray[colValIndex])) {  // NONE
          ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NONE);
        }

        colValIndex++;
        break;
      } else if (colValArray[colValIndex].cid > schema->columns[i].colId) {  // NONE
        ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NONE);
        break;
      } else {
        colValIndex++;
      }
    }
  }

  return 0;
}

static int32_t addEmptyItemToBlobSet(SBlobSet *pBlobSet, int8_t type, uint64_t *pSeq) {
  SBlobItem item = {.type = type, .len = 0};
  uint64_t  seq = 0;
  int32_t   code = tBlobSetPush(pBlobSet, &item, &seq, 0);
  if (code != 0) return code;

  if (pSeq != NULL) {
    *pSeq = seq;
  }
  return 0;
}
static int32_t tRowBuildTupleWithBlob(SArray *aColVal, const SRowBuildScanInfo *sinfo, const STSchema *schema,
                                      SRow **ppRow, SBlobSet *pBlobSet) {
  int32_t  code = 0;
  SColVal *colValArray = (SColVal *)TARRAY_DATA(aColVal);
  *ppRow = (SRow *)taosMemoryCalloc(1, sinfo->tupleRowSize);
  if (*ppRow == NULL) {
    return terrno;
  }
  (*ppRow)->flag = sinfo->tupleFlag;
  (*ppRow)->numOfPKs = sinfo->numOfPKs;
  (*ppRow)->sver = schema->version;
  (*ppRow)->len = sinfo->tupleRowSize;
  (*ppRow)->ts = colValArray[0].value.val;

  if (((*ppRow)->flag) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (sinfo->tupleFlag == HAS_NONE || sinfo->tupleFlag == HAS_NULL) {
    code = addEmptyItemToBlobSet(pBlobSet, TSDB_DATA_BLOB_NULL_VALUE, NULL);
    return code;
  }

  uint8_t *primaryKeys = (*ppRow)->data;
  uint8_t *bitmap = primaryKeys + sinfo->tuplePKSize;
  uint8_t *fixed = bitmap + sinfo->tupleBitmapSize;
  uint8_t *varlen = fixed + sinfo->tupleFixedSize;
  uint8_t *start = varlen;

  // primary keys
  for (int32_t i = 0; i < sinfo->numOfPKs; i++) {
    primaryKeys += tPutPrimaryKeyIndex(primaryKeys, sinfo->tupleIndices + i);
  }

  // bitmap + fixed + varlen
  int32_t numOfColVals = TARRAY_SIZE(aColVal);
  int32_t colValIndex = 1;
  int8_t  firstBlobCol = 1;
  for (int32_t i = 1; i < schema->numOfCols; i++) {
    for (;;) {
      if (colValIndex >= numOfColVals) {  // NONE
        ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NONE);
        break;
      }

      int8_t hasBlob = 0;
      if (colValArray[colValIndex].cid == schema->columns[i].colId) {
        if (COL_VAL_IS_VALUE(&colValArray[colValIndex])) {  // value
          ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_VALUE);

          if (IS_VAR_DATA_TYPE(schema->columns[i].type)) {
            if (IS_STR_DATA_BLOB(schema->columns[i].type)) {
              hasBlob = 1;
            }
            *(int32_t *)(fixed + schema->columns[i].offset) = varlen - fixed - sinfo->tupleFixedSize;
            varlen += tPutU32v(varlen, colValArray[colValIndex].value.nData);
            if (colValArray[colValIndex].value.nData) {
              if (hasBlob == 0) {
                (void)memcpy(varlen, colValArray[colValIndex].value.pData, colValArray[colValIndex].value.nData);
                varlen += colValArray[colValIndex].value.nData;
              } else {
                uint64_t  seq = 0;
                SBlobItem item = {.seqOffsetInRow = varlen - (*ppRow)->data,
                                  .data = colValArray[colValIndex].value.pData,
                                  .len = colValArray[colValIndex].value.nData,
                                  .type = TSDB_DATA_BLOB_VALUE};
                code = tBlobSetPush(pBlobSet, &item, &seq, firstBlobCol);
                if (firstBlobCol == 1) {
                  firstBlobCol = 0;
                }
                if (code != 0) return code;
                varlen += tPutU64(varlen, seq);
              }
            } else {
              if (hasBlob) TAOS_CHECK_RETURN(addEmptyItemToBlobSet(pBlobSet, TSDB_DATA_BLOB_EMPTY_VALUE, NULL));
            }
          } else {
            (void)memcpy(fixed + schema->columns[i].offset,
                         VALUE_GET_DATUM(&colValArray[colValIndex].value, schema->columns[i].type),
                         tDataTypes[schema->columns[i].type].bytes);
          }
        } else if (COL_VAL_IS_NULL(&colValArray[colValIndex])) {  // NULL
          ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NULL);
          if (IS_STR_DATA_BLOB(schema->columns[i].type)) {
            TAOS_CHECK_RETURN(addEmptyItemToBlobSet(pBlobSet, TSDB_DATA_BLOB_NULL_VALUE, NULL));
          }
        } else if (COL_VAL_IS_NONE(&colValArray[colValIndex])) {  // NONE
          if (IS_STR_DATA_BLOB(schema->columns[i].type)) {
            TAOS_CHECK_RETURN(addEmptyItemToBlobSet(pBlobSet, TSDB_DATA_BLOB_NULL_VALUE, NULL));
          }
          ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NONE);
        }

        colValIndex++;
        break;
      } else if (colValArray[colValIndex].cid > schema->columns[i].colId) {  // NONE
        ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NONE);
        break;
      } else {
        colValIndex++;
      }
    }
  }

  if (((*ppRow)->flag) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  return 0;
}

static int32_t tRowBuildTupleWithBlob2(SArray *aColVal, const SRowBuildScanInfo *sinfo, const STSchema *schema,
                                       SRow **ppRow, SBlobSet *pSrcBlobSet, SBlobSet *pDstBlobSet) {
  int32_t  code = 0;
  SColVal *colValArray = (SColVal *)TARRAY_DATA(aColVal);
  *ppRow = (SRow *)taosMemoryCalloc(1, sinfo->tupleRowSize);
  if (*ppRow == NULL) {
    return terrno;
  }
  (*ppRow)->flag = sinfo->tupleFlag;
  (*ppRow)->numOfPKs = sinfo->numOfPKs;
  (*ppRow)->sver = schema->version;
  (*ppRow)->len = sinfo->tupleRowSize;
  (*ppRow)->ts = colValArray[0].value.val;

  if (((*ppRow)->flag) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (sinfo->tupleFlag == HAS_NONE || sinfo->tupleFlag == HAS_NULL) {
    code = addEmptyItemToBlobSet(pDstBlobSet, TSDB_DATA_BLOB_NULL_VALUE, NULL);
    return code;
  }

  uint8_t *primaryKeys = (*ppRow)->data;
  uint8_t *bitmap = primaryKeys + sinfo->tuplePKSize;
  uint8_t *fixed = bitmap + sinfo->tupleBitmapSize;
  uint8_t *varlen = fixed + sinfo->tupleFixedSize;
  uint8_t *start = varlen;

  // primary keys
  for (int32_t i = 0; i < sinfo->numOfPKs; i++) {
    primaryKeys += tPutPrimaryKeyIndex(primaryKeys, sinfo->tupleIndices + i);
  }

  // bitmap + fixed + varlen
  int32_t numOfColVals = TARRAY_SIZE(aColVal);
  int32_t colValIndex = 1;
  for (int32_t i = 1; i < schema->numOfCols; i++) {
    for (;;) {
      if (colValIndex >= numOfColVals) {  // NONE
        ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NONE);
        break;
      }

      int8_t hasBlob = 0;
      if (colValArray[colValIndex].cid == schema->columns[i].colId) {
        if (COL_VAL_IS_VALUE(&colValArray[colValIndex])) {  // value
          ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_VALUE);

          if (IS_VAR_DATA_TYPE(schema->columns[i].type)) {
            if (IS_STR_DATA_BLOB(schema->columns[i].type)) {
              hasBlob = 1;
            }
            *(int32_t *)(fixed + schema->columns[i].offset) = varlen - fixed - sinfo->tupleFixedSize;
            varlen += tPutU32v(varlen, colValArray[colValIndex].value.nData);
            if (colValArray[colValIndex].value.nData) {
              if (hasBlob == 0) {
                (void)memcpy(varlen, colValArray[colValIndex].value.pData, colValArray[colValIndex].value.nData);
                varlen += colValArray[colValIndex].value.nData;
              } else {
                uint64_t seq = 0;
                if (tGetU64(colValArray[colValIndex].value.pData, &seq) < 0) {
                  TAOS_CHECK_RETURN(TSDB_CODE_INVALID_PARA);
                }
                SBlobItem item = {0};

                code = tBlobSetGet(pSrcBlobSet, seq, &item);
                TAOS_CHECK_RETURN(code);

                code = tBlobSetPush(pDstBlobSet, &item, &seq, 0);
                TAOS_CHECK_RETURN(code);
                varlen += tPutU64(varlen, seq);
              }
            } else {
              if (hasBlob) TAOS_CHECK_RETURN(addEmptyItemToBlobSet(pDstBlobSet, TSDB_DATA_BLOB_EMPTY_VALUE, NULL));
            }
          } else {
            (void)memcpy(fixed + schema->columns[i].offset,
                         VALUE_GET_DATUM(&colValArray[colValIndex].value, schema->columns[i].type),
                         tDataTypes[schema->columns[i].type].bytes);
          }
        } else if (COL_VAL_IS_NULL(&colValArray[colValIndex])) {  // NULL
          ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NULL);
          if (IS_STR_DATA_BLOB(schema->columns[i].type)) {
            TAOS_CHECK_RETURN(addEmptyItemToBlobSet(pDstBlobSet, TSDB_DATA_BLOB_NULL_VALUE, NULL));
          }
        } else if (COL_VAL_IS_NONE(&colValArray[colValIndex])) {  // NONE
          if (IS_STR_DATA_BLOB(schema->columns[i].type)) {
            TAOS_CHECK_RETURN(addEmptyItemToBlobSet(pDstBlobSet, TSDB_DATA_BLOB_NULL_VALUE, NULL));
          }
          ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NONE);
        }

        colValIndex++;
        break;
      } else if (colValArray[colValIndex].cid > schema->columns[i].colId) {  // NONE
        ROW_SET_BITMAP(bitmap, sinfo->tupleFlag, i - 1, BIT_FLG_NONE);
        break;
      } else {
        colValIndex++;
      }
    }
  }

  if (((*ppRow)->flag) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  return 0;
}
static FORCE_INLINE void tRowBuildKVRowSetIndex(uint8_t flag, SKVIdx *indices, uint32_t offset) {
  if (flag & KV_FLG_LIT) {
    ((uint8_t *)indices->idx)[indices->nCol] = (uint8_t)offset;
  } else if (flag & KV_FLG_MID) {
    ((uint16_t *)indices->idx)[indices->nCol] = (uint16_t)offset;
  } else {
    ((uint32_t *)indices->idx)[indices->nCol] = (uint32_t)offset;
  }
  indices->nCol++;
}

static int32_t tRowBuildKVRow(SArray *aColVal, const SRowBuildScanInfo *sinfo, const STSchema *schema, SRow **ppRow) {
  SColVal *colValArray = (SColVal *)TARRAY_DATA(aColVal);

  *ppRow = (SRow *)taosMemoryCalloc(1, sinfo->kvRowSize);
  if (*ppRow == NULL) {
    return terrno;
  }
  (*ppRow)->flag = sinfo->kvFlag;
  (*ppRow)->numOfPKs = sinfo->numOfPKs;
  (*ppRow)->sver = schema->version;
  (*ppRow)->len = sinfo->kvRowSize;
  (*ppRow)->ts = VALUE_GET_TRIVIAL_DATUM(&colValArray[0].value);

  if (!(sinfo->flag != HAS_NONE && sinfo->flag != HAS_NULL)) {
    return TSDB_CODE_INVALID_PARA;
  }

  uint8_t *primaryKeys = (*ppRow)->data;
  SKVIdx  *indices = (SKVIdx *)(primaryKeys + sinfo->kvPKSize);
  uint8_t *payload = primaryKeys + sinfo->kvPKSize + sinfo->kvIndexSize;
  uint32_t payloadSize = 0;

  // primary keys
  for (int32_t i = 0; i < sinfo->numOfPKs; i++) {
    primaryKeys += tPutPrimaryKeyIndex(primaryKeys, sinfo->kvIndices + i);
  }

  int32_t numOfColVals = TARRAY_SIZE(aColVal);
  int32_t colValIndex = 1;
  for (int32_t i = 1; i < schema->numOfCols; i++) {
    for (;;) {
      if (colValIndex >= numOfColVals) {  // NONE
        break;
      }

      if (colValArray[colValIndex].cid == schema->columns[i].colId) {
        if (COL_VAL_IS_VALUE(&colValArray[colValIndex])) {  // value
          tRowBuildKVRowSetIndex(sinfo->kvFlag, indices, payloadSize);
          if (IS_VAR_DATA_TYPE(schema->columns[i].type)) {
            payloadSize += tPutI16v(payload + payloadSize, colValArray[colValIndex].cid);
            payloadSize += tPutU32v(payload + payloadSize, colValArray[colValIndex].value.nData);
            if (colValArray[colValIndex].value.nData > 0) {
              (void)memcpy(payload + payloadSize, colValArray[colValIndex].value.pData,
                           colValArray[colValIndex].value.nData);
              payloadSize += colValArray[colValIndex].value.nData;
            }
          } else {
            payloadSize += tPutI16v(payload + payloadSize, colValArray[colValIndex].cid);
            (void)memcpy(payload + payloadSize,
                         VALUE_GET_DATUM(&colValArray[colValIndex].value, schema->columns[i].type),
                         tDataTypes[schema->columns[i].type].bytes);
            payloadSize += tDataTypes[schema->columns[i].type].bytes;
          }
        } else if (COL_VAL_IS_NULL(&colValArray[colValIndex])) {  // NULL
          tRowBuildKVRowSetIndex(sinfo->kvFlag, indices, payloadSize);
          payloadSize += tPutI16v(payload + payloadSize, -schema->columns[i].colId);
        }

        colValIndex++;
        break;
      } else if (colValArray[colValIndex].cid > schema->columns[i].colId) {  // NONE
        break;
      } else {
        colValIndex++;
      }
    }
  }
  return 0;
}

static int32_t tRowBuildKVRowWithBlob(SArray *aColVal, const SRowBuildScanInfo *sinfo, const STSchema *schema,
                                      SRow **ppRow, SBlobSet *ppBlobSet) {
  SColVal *colValArray = (SColVal *)TARRAY_DATA(aColVal);

  *ppRow = (SRow *)taosMemoryCalloc(1, sinfo->kvRowSize);
  if (*ppRow == NULL) {
    return terrno;
  }
  (*ppRow)->flag = sinfo->kvFlag;
  (*ppRow)->numOfPKs = sinfo->numOfPKs;
  (*ppRow)->sver = schema->version;
  (*ppRow)->len = sinfo->kvRowSize;
  (*ppRow)->ts = colValArray[0].value.val;

  if (!(sinfo->flag != HAS_NONE && sinfo->flag != HAS_NULL)) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (((*ppRow)->flag) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  uint8_t *primaryKeys = (*ppRow)->data;
  SKVIdx  *indices = (SKVIdx *)(primaryKeys + sinfo->kvPKSize);
  uint8_t *payload = primaryKeys + sinfo->kvPKSize + sinfo->kvIndexSize;
  uint32_t payloadSize = 0;

  // primary keys
  for (int32_t i = 0; i < sinfo->numOfPKs; i++) {
    primaryKeys += tPutPrimaryKeyIndex(primaryKeys, sinfo->kvIndices + i);
  }

  int32_t numOfColVals = TARRAY_SIZE(aColVal);
  int32_t colValIndex = 1;
  int8_t  firstBlobCol = 1;
  for (int32_t i = 1; i < schema->numOfCols; i++) {
    for (;;) {
      if (colValIndex >= numOfColVals) {  // NONE
        break;
      }
      uint8_t hasBlob = 0;

      if (colValArray[colValIndex].cid == schema->columns[i].colId) {
        if (COL_VAL_IS_VALUE(&colValArray[colValIndex])) {  // value
          tRowBuildKVRowSetIndex(sinfo->kvFlag, indices, payloadSize);
          if (IS_VAR_DATA_TYPE(schema->columns[i].type)) {
            if (IS_STR_DATA_BLOB(schema->columns[i].type)) {
              hasBlob = 1;
            }
            payloadSize += tPutI16v(payload + payloadSize, colValArray[colValIndex].cid);
            payloadSize += tPutU32v(payload + payloadSize, colValArray[colValIndex].value.nData);
            if (colValArray[colValIndex].value.nData > 0) {
              if (hasBlob == 0) {
                (void)memcpy(payload + payloadSize, colValArray[colValIndex].value.pData,
                             colValArray[colValIndex].value.nData);
                payloadSize += colValArray[colValIndex].value.nData;
              } else {
                uint64_t  seq = 0;
                uint32_t  seqOffset = payloadSize + payload - (*ppRow)->data;
                SBlobItem item = {.seqOffsetInRow = seqOffset,
                                  .data = colValArray[colValIndex].value.pData,
                                  .len = colValArray[colValIndex].value.nData,
                                  .type = TSDB_DATA_BLOB_VALUE};
                int32_t   code = tBlobSetPush(ppBlobSet, &item, &seq, 0);
                if (code != 0) return code;
                payloadSize += tPutU64(payload + payloadSize, seq);
              }
            } else {
              if (hasBlob) {
                TAOS_CHECK_RETURN(addEmptyItemToBlobSet(ppBlobSet, TSDB_DATA_BLOB_EMPTY_VALUE, NULL));
              }
            }
          } else {
            payloadSize += tPutI16v(payload + payloadSize, colValArray[colValIndex].cid);
            (void)memcpy(payload + payloadSize, &colValArray[colValIndex].value.val,
                         tDataTypes[schema->columns[i].type].bytes);
            payloadSize += tDataTypes[schema->columns[i].type].bytes;
          }
        } else if (COL_VAL_IS_NULL(&colValArray[colValIndex])) {  // NULL
          tRowBuildKVRowSetIndex(sinfo->kvFlag, indices, payloadSize);
          payloadSize += tPutI16v(payload + payloadSize, -schema->columns[i].colId);
          if (IS_STR_DATA_BLOB(schema->columns[i].type)) {
            TAOS_CHECK_RETURN(addEmptyItemToBlobSet(ppBlobSet, TSDB_DATA_BLOB_NULL_VALUE, NULL));
          }
        }
        colValIndex++;
        break;
      } else if (colValArray[colValIndex].cid > schema->columns[i].colId) {  // NONE
        break;
      } else {
        colValIndex++;
      }
    }
  }

  if (((*ppRow)->flag) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  return 0;
}

static int32_t tRowBuildKVRowWithBlob2(SArray *aColVal, const SRowBuildScanInfo *sinfo, const STSchema *schema,
                                       SRow **ppRow, SBlobSet *pSrcBlobSet, SBlobSet *pDstBlobSet) {
  SColVal *colValArray = (SColVal *)TARRAY_DATA(aColVal);

  *ppRow = (SRow *)taosMemoryCalloc(1, sinfo->kvRowSize);
  if (*ppRow == NULL) {
    return terrno;
  }
  (*ppRow)->flag = sinfo->kvFlag;
  (*ppRow)->numOfPKs = sinfo->numOfPKs;
  (*ppRow)->sver = schema->version;
  (*ppRow)->len = sinfo->kvRowSize;
  (*ppRow)->ts = colValArray[0].value.val;

  if (!(sinfo->flag != HAS_NONE && sinfo->flag != HAS_NULL)) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (((*ppRow)->flag) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  uint8_t *primaryKeys = (*ppRow)->data;
  SKVIdx  *indices = (SKVIdx *)(primaryKeys + sinfo->kvPKSize);
  uint8_t *payload = primaryKeys + sinfo->kvPKSize + sinfo->kvIndexSize;
  uint32_t payloadSize = 0;

  // primary keys
  for (int32_t i = 0; i < sinfo->numOfPKs; i++) {
    primaryKeys += tPutPrimaryKeyIndex(primaryKeys, sinfo->kvIndices + i);
  }

  int32_t numOfColVals = TARRAY_SIZE(aColVal);
  int32_t colValIndex = 1;
  for (int32_t i = 1; i < schema->numOfCols; i++) {
    for (;;) {
      if (colValIndex >= numOfColVals) {  // NONE
        break;
      }
      uint8_t hasBlob = 0;

      if (colValArray[colValIndex].cid == schema->columns[i].colId) {
        if (COL_VAL_IS_VALUE(&colValArray[colValIndex])) {  // value
          tRowBuildKVRowSetIndex(sinfo->kvFlag, indices, payloadSize);
          if (IS_VAR_DATA_TYPE(schema->columns[i].type)) {
            if (IS_STR_DATA_BLOB(schema->columns[i].type)) {
              hasBlob = 1;
            }
            payloadSize += tPutI16v(payload + payloadSize, colValArray[colValIndex].cid);
            payloadSize += tPutU32v(payload + payloadSize, colValArray[colValIndex].value.nData);
            if (colValArray[colValIndex].value.nData > 0) {
              if (hasBlob == 0) {
                (void)memcpy(payload + payloadSize, colValArray[colValIndex].value.pData,
                             colValArray[colValIndex].value.nData);
                payloadSize += colValArray[colValIndex].value.nData;
              } else {
                uint64_t seq = 0;
                if (tGetU64(colValArray[colValIndex].value.pData, &seq) < 0) {
                  TAOS_CHECK_RETURN(TSDB_CODE_INVALID_PARA);
                }
                SBlobItem item = {0};

                int32_t code = tBlobSetGet(pSrcBlobSet, seq, &item);
                TAOS_CHECK_RETURN(code);

                code = tBlobSetPush(pDstBlobSet, &item, &seq, 0);
                TAOS_CHECK_RETURN(code);

                payloadSize += tPutU64(payload + payloadSize, seq);
              }
            } else {
              if (hasBlob) {
                TAOS_CHECK_RETURN(addEmptyItemToBlobSet(pDstBlobSet, TSDB_DATA_BLOB_EMPTY_VALUE, NULL));
              }
            }
          } else {
            payloadSize += tPutI16v(payload + payloadSize, colValArray[colValIndex].cid);
            (void)memcpy(payload + payloadSize, &colValArray[colValIndex].value.val,
                         tDataTypes[schema->columns[i].type].bytes);
            payloadSize += tDataTypes[schema->columns[i].type].bytes;
          }
        } else if (COL_VAL_IS_NULL(&colValArray[colValIndex])) {  // NULL
          tRowBuildKVRowSetIndex(sinfo->kvFlag, indices, payloadSize);
          payloadSize += tPutI16v(payload + payloadSize, -schema->columns[i].colId);
          if (IS_STR_DATA_BLOB(schema->columns[i].type)) {
            TAOS_CHECK_RETURN(addEmptyItemToBlobSet(pDstBlobSet, TSDB_DATA_BLOB_NULL_VALUE, NULL));
          }
        }
        colValIndex++;
        break;
      } else if (colValArray[colValIndex].cid > schema->columns[i].colId) {  // NONE
        break;
      } else {
        colValIndex++;
      }
    }
  }

  if (((*ppRow)->flag) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  return 0;
}
int32_t tRowBuild(SArray *aColVal, const STSchema *pTSchema, SRow **ppRow, SRowBuildScanInfo *sinfo) {
  int32_t code;
  code = tRowBuildScan(aColVal, pTSchema, sinfo);
  if (code) return code;

  if (sinfo->tupleRowSize <= sinfo->kvRowSize) {
    code = tRowBuildTupleRow(aColVal, sinfo, pTSchema, ppRow);
  } else {
    code = tRowBuildKVRow(aColVal, sinfo, pTSchema, ppRow);
  }
  return code;
}

int32_t tBlobRowCreate(int64_t cap, int8_t type, SBlobSet **ppBlobRow) {
  int32_t    lino = 0;
  int32_t    code = 0;
  SBlobSet *p = taosMemCalloc(1, sizeof(SBlobSet));
  if (p == NULL) {
    return terrno;
  }

  p->type = type;
  p->pSeqTable = taosArrayInit(128, sizeof(SBlobValue));
  if (p->pSeqTable == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  p->data = taosMemCalloc(1024, cap * sizeof(uint8_t));
  if (p->data == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  p->pSeqToffset = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
  if (p->pSeqToffset == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  p->pSet = taosArrayInit(4, sizeof(int32_t));
  if (p->pSet == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  p->cap = cap;
  p->len = 0;
  p->seq = 1;

  *ppBlobRow = p;
_exit:
  if (code != 0) {
    taosHashCleanup(p->pSeqToffset);
    taosArrayDestroy(p->pSeqTable);
    taosArrayDestroy(p->pSet);

    taosMemoryFree(p->data);
    taosMemoryFree(p);
  }
  uDebug("create blob row %p", p);
  return code;
}
int32_t tBlobRowPush(SBlobSet *pBlobRow, SBlobItem *pItem, uint64_t *seq, int8_t nextRow) {
  if (pBlobRow == NULL || pItem == NULL || seq == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  uTrace("push blob %p, seqOffsetInRow %d, dataLen %d, nextRow %d", pBlobRow, pItem->seqOffsetInRow, pItem->len,
         nextRow);
  int32_t  lino = 0;
  int32_t  code = 0;
  uint64_t offset;
  uint32_t len = pItem->len;
  uint8_t *data = pItem->data;
  int32_t  dataOffset = pItem->seqOffsetInRow;

  uint64_t tlen = pBlobRow->len + len;
  if (tlen > pBlobRow->cap) {
    int64_t cap = pBlobRow->cap;
    // opt later
    do {
      cap = cap * 2;
    } while (tlen > cap);

    uint8_t *data = taosMemRealloc(pBlobRow->data, cap);
    if (data == NULL) {
      return terrno;
    }
    pBlobRow->data = data;
    pBlobRow->cap = cap;
  }
  if (len > 0) {
    (void)memcpy(pBlobRow->data + pBlobRow->len, data, len);
  }

  offset = pBlobRow->len;
  pBlobRow->len += len;

  pBlobRow->seq++;
  *seq = pBlobRow->seq;

  SBlobValue value = {.offset = offset, .len = len, .dataOffset = dataOffset, .nextRow = nextRow};
  if (taosArrayPush(pBlobRow->pSeqTable, &value) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  int32_t sz = taosArrayGetSize(pBlobRow->pSeqTable);
  code = taosHashPut(pBlobRow->pSeqToffset, seq, sizeof(int64_t), &sz, sizeof(int32_t));
  if (code != 0) {
    TAOS_CHECK_EXIT(code);
  }

_exit:
  return code;
}
int32_t tBlobRowUpdate(SBlobSet *pBlobRow, uint64_t seq, SBlobItem *pItem) {
  int32_t code = 0;
  return code;
  if (pBlobRow == NULL || pItem == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pBlobRow->pSeqToffset == NULL || pBlobRow->pSeqTable == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  return code;
}
int32_t tBlobRowGet(SBlobSet *pBlobRow, uint64_t seq, SBlobItem *pItem) {
  if (pBlobRow == NULL || pItem == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pBlobRow->pSeqToffset == NULL || pBlobRow->pSeqTable == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;
  int32_t len = 0;
  int32_t dataOffset = 0;
  int8_t  nextRow = 0;

  int32_t *offset = (int32_t *)taosHashGet(pBlobRow->pSeqToffset, &seq, sizeof(int64_t));
  if (offset == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SBlobValue *value = taosArrayGet(pBlobRow->pSeqTable, *offset - 1);
  if (value == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  len = value->len;
  dataOffset = value->dataOffset;
  nextRow = value->nextRow;

  pItem->seqOffsetInRow = dataOffset + pBlobRow->data - (uint8_t *)pBlobRow;
  pItem->len = len;
  pItem->data = pBlobRow->data + value->offset;

  return code;
}

int32_t tBlobRowSize(SBlobSet *pBlobRow) {
  if (pBlobRow == NULL) return 0;
  return taosArrayGetSize(pBlobRow->pSeqTable);
}
int32_t tBlobRowEnd(SBlobSet *pBlobRow) {
  if (pBlobRow == NULL) return 0;
  int32_t sz = taosArrayGetSize(pBlobRow->pSeqTable);
  if (taosArrayPush(pBlobRow->pSet, &sz) == NULL) {
    return terrno;
  }
  return 0;
}
int32_t tBlobRowRebuild(SBlobSet *p, int32_t sRow, int32_t nrow, SBlobSet **pDst) {
  int32_t code = 0;

  code = tBlobRowCreate(p->cap, p->type, pDst);
  if (code != 0) {
    return code;
  }

  SBlobSet *pBlobRow = *pDst;
  if (p->pSeqToffset == NULL || p->pSeqTable == NULL || p->data == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  uint64_t seq = 0;
  int32_t  count = 0;
  for (int32_t i = sRow; i < taosArrayGetSize(p->pSeqTable); i++) {
    SBlobValue *pValue = taosArrayGet(p->pSeqTable, i);
    uint8_t    *data = p->data + pValue->offset;
    int32_t     len = pValue->len;

    SBlobItem item = {.data = data, .len = len, .seqOffsetInRow = pValue->dataOffset};

    code = tBlobRowPush(pBlobRow, &item, &seq, pValue->nextRow);
    count++;
    if (count >= nrow) {
      break;
    }
  }

  return code;
}

int32_t tBlobRowDestroy(SBlobSet *pBlobRow) {
  if (pBlobRow == NULL) return 0;
  int32_t code = 0;
  uTrace("destroy blob row, seqTable size %p", pBlobRow);
  taosMemoryFree(pBlobRow->data);
  taosArrayDestroy(pBlobRow->pSeqTable);
  taosHashCleanup(pBlobRow->pSeqToffset);
  taosArrayDestroy(pBlobRow->pSet);
  taosMemoryFree(pBlobRow);
  return code;
}
int32_t tBlobRowClear(SBlobSet *pBlobRow) {
  if (pBlobRow == NULL) return 0;
  int32_t code = 0;
  uTrace("clear blob row, seqTable size %p", pBlobRow);
  taosArrayClear(pBlobRow->pSeqTable);
  taosHashClear(pBlobRow->pSeqToffset);
  pBlobRow->len = 0;
  pBlobRow->seq = 1;
  return code;
}

int32_t tRowBuildWithBlob(SArray *aColVal, const STSchema *pTSchema, SRow **ppRow, SBlobSet *pBlobSet,
                          SRowBuildScanInfo *sinfo) {
  int32_t code;

  code = tRowBuildScan(aColVal, pTSchema, sinfo);
  if (code) return code;

  if (sinfo->tupleRowSize <= sinfo->kvRowSize) {
    code = tRowBuildTupleWithBlob(aColVal, sinfo, pTSchema, ppRow, pBlobSet);
  } else {
    code = tRowBuildKVRowWithBlob(aColVal, sinfo, pTSchema, ppRow, pBlobSet);
  }

  return code;
}

int32_t tRowBuildWithBlob2(SArray *aColVal, const STSchema *pTSchema, SRow **ppRow, SBlobSet *pSrcBlobSet,
                           SBlobSet *pDstBlobSet, SRowBuildScanInfo *sinfo) {
  int32_t code;

  code = tRowBuildScan(aColVal, pTSchema, sinfo);
  if (code) return code;

  if (sinfo->tupleRowSize <= sinfo->kvRowSize) {
    code = tRowBuildTupleWithBlob2(aColVal, sinfo, pTSchema, ppRow, pSrcBlobSet, pDstBlobSet);
  } else {
    code = tRowBuildKVRowWithBlob2(aColVal, sinfo, pTSchema, ppRow, pSrcBlobSet, pDstBlobSet);
  }

  return code;
}

int32_t tBlobSetCreate(int64_t cap, int8_t type, SBlobSet **ppBlobSet) {
  int32_t    lino = 0;
  int32_t    code = 0;
  SBlobSet  *p = taosMemCalloc(1, sizeof(SBlobSet));
  if (p == NULL) {
    return terrno;
  }

  p->type = type;
  p->pSeqTable = taosArrayInit(128, sizeof(SBlobValue));
  if (p->pSeqTable == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  p->data = taosMemCalloc(1024, cap * sizeof(uint8_t));
  if (p->data == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  p->pSeqToffset = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
  if (p->pSeqToffset == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  p->pSet = taosArrayInit(4, sizeof(int32_t));
  if (p->pSet == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  p->cap = cap;
  p->len = 0;
  p->seq = 1;

  *ppBlobSet = p;
_exit:
  if (code != 0) {
    taosHashCleanup(p->pSeqToffset);
    taosArrayDestroy(p->pSeqTable);
    taosArrayDestroy(p->pSet);

    taosMemoryFree(p->data);
    taosMemoryFree(p);
  }
  uDebug("create blob row %p", p);
  return code;
}
int32_t tBlobSetPush(SBlobSet *pBlobSet, SBlobItem *pItem, uint64_t *seq, int8_t nextRow) {
  if (pBlobSet == NULL || pItem == NULL || seq == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  uTrace("push blob %p, seqOffsetInRow %d, dataLen %d, nextRow %d", pBlobSet, pItem->seqOffsetInRow, pItem->len,
         nextRow);
  int32_t  lino = 0;
  int32_t  code = 0;
  uint64_t offset;
  uint32_t len = pItem->len;
  uint8_t *data = pItem->data;
  int32_t  dataOffset = pItem->seqOffsetInRow;

  uint64_t tlen = pBlobSet->len + len;
  if (tlen > pBlobSet->cap) {
    int64_t cap = pBlobSet->cap;
    // opt later
    do {
      cap = cap * 2;
    } while (tlen > cap);

    uint8_t *data = taosMemRealloc(pBlobSet->data, cap);
    if (data == NULL) {
      return terrno;
    }
    pBlobSet->data = data;
    pBlobSet->cap = cap;
  }
  if (len > 0) {
    (void)memcpy(pBlobSet->data + pBlobSet->len, data, len);
  }

  offset = pBlobSet->len;
  pBlobSet->len += len;

  pBlobSet->seq++;
  *seq = pBlobSet->seq;

  SBlobValue value = {.offset = offset, .len = len, .dataOffset = dataOffset, .nextRow = nextRow, .type = pItem->type};
  if (taosArrayPush(pBlobSet->pSeqTable, &value) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  int32_t sz = taosArrayGetSize(pBlobSet->pSeqTable);
  code = taosHashPut(pBlobSet->pSeqToffset, seq, sizeof(int64_t), &sz, sizeof(int32_t));
  if (code != 0) {
    TAOS_CHECK_EXIT(code);
  }

_exit:
  return code;
}

void tBlobSetSwap(SBlobSet *p1, SBlobSet *p2) {
  SBlobSet t = {0};

  memcpy(&t, p1, sizeof(SBlobSet));

  memcpy(p1, p2, sizeof(SBlobSet));
  memcpy(p2, &t, sizeof(SBlobSet));
}

int32_t tBlobSetUpdate(SBlobSet *pBlobSet, uint64_t seq, SBlobItem *pItem) {
  int32_t code = 0;
  return code;
  if (pBlobSet == NULL || pItem == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pBlobSet->pSeqToffset == NULL || pBlobSet->pSeqTable == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  return code;
}
int32_t tBlobSetGet(SBlobSet *pBlobSet, uint64_t seq, SBlobItem *pItem) {
  if (pBlobSet == NULL || pItem == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pBlobSet->pSeqToffset == NULL || pBlobSet->pSeqTable == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;
  int32_t len = 0;
  int32_t dataOffset = 0;
  int8_t  nextRow = 0;

  int32_t *offset = (int32_t *)taosHashGet(pBlobSet->pSeqToffset, &seq, sizeof(int64_t));
  if (offset == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SBlobValue *value = taosArrayGet(pBlobSet->pSeqTable, *offset - 1);
  if (value == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  len = value->len;
  dataOffset = value->dataOffset;
  nextRow = value->nextRow;

  pItem->seqOffsetInRow = value->dataOffset;
  pItem->len = len;
  pItem->data = pBlobSet->data + value->offset;
  pItem->type = value->type;

  return code;
}

int32_t tBlobSetSize(SBlobSet *pBlobSet) {
  if (pBlobSet == NULL) return 0;
  return taosArrayGetSize(pBlobSet->pSeqTable);
}

void tBlobSetDestroy(SBlobSet *pBlobSet) {
  if (pBlobSet == NULL) return;
  uTrace("destroy blob row, seqTable size %p", pBlobSet);
  taosMemoryFree(pBlobSet->data);
  taosArrayDestroy(pBlobSet->pSeqTable);
  taosHashCleanup(pBlobSet->pSeqToffset);
  taosArrayDestroy(pBlobSet->pSet);
  taosMemoryFree(pBlobSet);
}
int32_t tBlobSetClear(SBlobSet *pBlobSet) {
  if (pBlobSet == NULL) return 0;
  int32_t code = 0;
  uTrace("clear blob row, seqTable size %p", pBlobSet);
  taosArrayClear(pBlobSet->pSeqTable);
  taosHashClear(pBlobSet->pSeqToffset);
  pBlobSet->len = 0;
  pBlobSet->seq = 1;
  return code;
}

static int32_t tBindInfoCompare(const void *p1, const void *p2, const void *param) {
  if (((SBindInfo *)p1)->columnId < ((SBindInfo *)p2)->columnId) {
    return -1;
  } else if (((SBindInfo *)p1)->columnId > ((SBindInfo *)p2)->columnId) {
    return 1;
  }
  return 0;
}

/* build rows to `rowArray` from bind
 * `infos` is the bind information array
 * `numOfInfos` is the number of bind information
 * `infoSorted` is whether the bind information is sorted by column id
 * `pTSchema` is the schema of the table
 * `rowArray` is the array to store the rows
 * `pOrdered` is the pointer to store ordered
 * `pDupTs` is the pointer to store duplicateTs
 */
int32_t tRowBuildFromBind(SBindInfo *infos, int32_t numOfInfos, bool infoSorted, const STSchema *pTSchema,
                          SArray *rowArray, bool *pOrdered, bool *pDupTs) {
  if (infos == NULL || numOfInfos <= 0 || numOfInfos > pTSchema->numOfCols || pTSchema == NULL || rowArray == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (!infoSorted) {
    taosqsort_r(infos, numOfInfos, sizeof(SBindInfo), NULL, tBindInfoCompare);
  }

  int32_t code = 0;
  int32_t numOfRows = infos[0].bind->num;
  SArray *colValArray;
  SColVal colVal;

  if ((colValArray = taosArrayInit(numOfInfos, sizeof(SColVal))) == NULL) {
    return terrno;
  }

  SRowKey rowKey, lastRowKey;
  for (int32_t iRow = 0; iRow < numOfRows; iRow++) {
    taosArrayClear(colValArray);

    for (int32_t iInfo = 0; iInfo < numOfInfos; iInfo++) {
      if (infos[iInfo].bind->is_null && infos[iInfo].bind->is_null[iRow]) {
        colVal = COL_VAL_NULL(infos[iInfo].columnId, infos[iInfo].type);
      } else {
        SValue value = {
            .type = infos[iInfo].type,
        };
        if (IS_VAR_DATA_TYPE(infos[iInfo].type)) {
          value.nData = infos[iInfo].bind->length[iRow];
          if (value.nData > pTSchema->columns[iInfo].bytes - VARSTR_HEADER_SIZE) {
            code = TSDB_CODE_INVALID_PARA;
            goto _exit;
          }
          value.pData = (uint8_t *)infos[iInfo].bind->buffer + infos[iInfo].bind->buffer_length * iRow;
        } else {
          valueSetDatum(&value, infos[iInfo].type,
                        (uint8_t *)infos[iInfo].bind->buffer + infos[iInfo].bind->buffer_length * iRow,
                        infos[iInfo].bind->buffer_length);
        }
        colVal = COL_VAL_VALUE(infos[iInfo].columnId, value);
      }
      if (taosArrayPush(colValArray, &colVal) == NULL) {
        code = terrno;
        goto _exit;
      }
    }

    SRow             *row;
    SRowBuildScanInfo sinfo = {0};
    if ((code = tRowBuild(colValArray, pTSchema, &row, &sinfo))) {
      goto _exit;
    }

    if ((taosArrayPush(rowArray, &row)) == NULL) {
      code = terrno;
      goto _exit;
    }

    if (pOrdered && pDupTs) {
      tRowGetKey(row, &rowKey);
      if (iRow == 0) {
        *pOrdered = true;
        *pDupTs = false;
      } else {
        if (*pOrdered) {
          int32_t res = tRowKeyCompare(&rowKey, &lastRowKey);
          *pOrdered = (res >= 0);
          if (!*pDupTs) {
            *pDupTs = (res == 0);
          }
        }
      }
      lastRowKey = rowKey;
    }
  }

_exit:
  taosArrayDestroy(colValArray);
  return code;
}

int32_t tRowGet(SRow *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal) {
  if (!(iCol < pTSchema->numOfCols)) return TSDB_CODE_INVALID_PARA;
  if (!(pRow->sver == pTSchema->version)) return TSDB_CODE_INVALID_PARA;

  STColumn *pTColumn = pTSchema->columns + iCol;

  if (iCol == 0) {
    pColVal->cid = pTColumn->colId;
    pColVal->value.type = pTColumn->type;
    pColVal->flag = CV_FLAG_VALUE;
    VALUE_SET_TRIVIAL_DATUM(&pColVal->value, pRow->ts);
    return 0;
  }

  if (pRow->flag == HAS_NONE) {
    *pColVal = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
    return 0;
  }

  if (pRow->flag == HAS_NULL) {
    *pColVal = COL_VAL_NULL(pTColumn->colId, pTColumn->type);
    return 0;
  }

  SPrimaryKeyIndex index;
  uint8_t         *data = pRow->data;
  for (int32_t i = 0; i < pRow->numOfPKs; i++) {
    data += tGetPrimaryKeyIndex(data, &index);
  }

  if (pRow->flag >> 4) {  // KV Row
    SKVIdx  *pIdx = (SKVIdx *)data;
    uint8_t *pv = NULL;

    if (pRow->flag & KV_FLG_LIT) {
      pv = pIdx->idx + pIdx->nCol;
    } else if (pRow->flag & KV_FLG_MID) {
      pv = pIdx->idx + (pIdx->nCol << 1);
    } else {
      pv = pIdx->idx + (pIdx->nCol << 2);
    }

    int16_t lidx = 0;
    int16_t ridx = pIdx->nCol - 1;
    while (lidx <= ridx) {
      int16_t  mid = (lidx + ridx) >> 1;
      uint8_t *pData = NULL;
      if (pRow->flag & KV_FLG_LIT) {
        pData = pv + ((uint8_t *)pIdx->idx)[mid];
      } else if (pRow->flag & KV_FLG_MID) {
        pData = pv + ((uint16_t *)pIdx->idx)[mid];
      } else {
        pData = pv + ((uint32_t *)pIdx->idx)[mid];
      }

      int16_t cid;
      pData += tGetI16v(pData, &cid);

      if (TABS(cid) == pTColumn->colId) {
        if (cid < 0) {
          *pColVal = COL_VAL_NULL(pTColumn->colId, pTColumn->type);
        } else {
          pColVal->cid = pTColumn->colId;
          pColVal->value.type = pTColumn->type;
          pColVal->flag = CV_FLAG_VALUE;

          if (IS_VAR_DATA_TYPE(pTColumn->type)) {
            int8_t isBlob = IS_STR_DATA_BLOB(pTColumn->type) ? 1 : 0;

            pData += tGetU32v(pData, &pColVal->value.nData);
            if (pColVal->value.nData > 0) {
              pColVal->value.pData = pData;
            } else {
              pColVal->value.pData = NULL;
            }
            if (isBlob == 1) {
              pData += BSE_SEQUECE_SIZE;  // skip seq
            }
          } else {
            valueSetDatum(&pColVal->value, pTColumn->type, pData, pTColumn->bytes);
          }
        }
        return 0;
      } else if (TABS(cid) < pTColumn->colId) {
        lidx = mid + 1;
      } else {
        ridx = mid - 1;
      }
    }

    *pColVal = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
  } else {  // Tuple Row
    uint8_t *bitmap = data;
    uint8_t *fixed;
    uint8_t *varlen;
    uint8_t  bit;

    if (pRow->flag == HAS_VALUE) {
      fixed = bitmap;
      bit = BIT_FLG_VALUE;
    } else if (pRow->flag == (HAS_NONE | HAS_NULL | HAS_VALUE)) {
      fixed = BIT2_SIZE(pTSchema->numOfCols - 1) + bitmap;
      bit = GET_BIT2(bitmap, iCol - 1);
    } else {
      fixed = BIT1_SIZE(pTSchema->numOfCols - 1) + bitmap;
      bit = GET_BIT1(bitmap, iCol - 1);

      if (pRow->flag == (HAS_NONE | HAS_VALUE)) {
        if (bit) bit++;
      } else if (pRow->flag == (HAS_NULL | HAS_VALUE)) {
        bit++;
      }
    }
    varlen = fixed + pTSchema->flen;

    if (bit == BIT_FLG_NONE) {
      *pColVal = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
      return 0;
    } else if (bit == BIT_FLG_NULL) {
      *pColVal = COL_VAL_NULL(pTColumn->colId, pTColumn->type);
      return 0;
    }

    pColVal->cid = pTColumn->colId;
    pColVal->value.type = pTColumn->type;
    pColVal->flag = CV_FLAG_VALUE;
    if (IS_VAR_DATA_TYPE(pTColumn->type)) {
      int8_t isBlob = IS_STR_DATA_BLOB(pTColumn->type) ? 1 : 0;
      pColVal->value.pData = varlen + *(int32_t *)(fixed + pTColumn->offset);
      pColVal->value.pData += tGetU32v(pColVal->value.pData, &pColVal->value.nData);
      // TODO(yhDeng): support tuple
      //  if (isBlob) pColVal->value.pData += sizeof(uint64_t);  // skip seq
      //  }
    } else {
      valueSetDatum(&pColVal->value, pTColumn->type, fixed + pTColumn->offset, TYPE_BYTES[pTColumn->type]);
    }
  }

  return 0;
}

void tRowDestroy(SRow *pRow) {
  if (pRow) taosMemoryFree(pRow);
}

static int32_t tRowPCmprFn(const void *p1, const void *p2) {
  SRowKey key1, key2;
  tRowGetKey(*(SRow **)p1, &key1);
  tRowGetKey(*(SRow **)p2, &key2);
  return tRowKeyCompare(&key1, &key2);
}
static void    tRowPDestroy(SRow **ppRow) { tRowDestroy(*ppRow); }

static SColVal* tRowFindColumnValue(SRowIter *iter, int32_t targetCid) {
  SColVal* pColVal = tRowIterNext(iter);
  while (pColVal != NULL && pColVal->cid < targetCid) {
    pColVal = tRowIterNext(iter);
  }
  return pColVal;
}

static int32_t tRowMergeImpl(SArray *aRowP, STSchema *pTSchema, int32_t iStart, int32_t iEnd, ERowMergeStrategy strategy) {
  int32_t code = 0;

  int32_t    nRow = iEnd - iStart;
  SRowIter **aIter = NULL;
  SArray    *aColVal = NULL;
  SRow      *pRow = NULL;
  uint8_t    hasBlob = 0;
  aIter = taosMemoryCalloc(nRow, sizeof(SRowIter *));
  if (aIter == NULL) {
    code = terrno;
    goto _exit;
  }

  for (int32_t i = 0; i < nRow; i++) {
    SRow *pRowT = taosArrayGetP(aRowP, iStart + i);

    code = tRowIterOpen(pRowT, pTSchema, &aIter[i]);
    if (code) goto _exit;
  }

  // merge
  aColVal = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
  if (aColVal == NULL) {
    code = terrno;
    goto _exit;
  }

  for (int32_t iCol = 0; iCol < pTSchema->numOfCols; iCol++) {
    int32_t targetCid = pTSchema->columns[iCol].colId;
    SColVal *pColVal = NULL;

    switch (strategy) {
      case KEEP_CONSISTENCY:
        if (nRow > 0)
          pColVal = tRowFindColumnValue(aIter[nRow - 1], targetCid);
        break;

      default:  // default using PREFER_NON_NULL strategy
      case PREFER_NON_NULL:
        for (int32_t iRow = nRow - 1; iRow >= 0; --iRow) {
          SColVal *pColValT = tRowFindColumnValue(aIter[iRow], targetCid);

          if (COL_VAL_IS_VALUE(pColValT)) {
            pColVal = pColValT;
            break;
          } else if (pColVal == NULL) {
            pColVal = pColValT;
          }
        }
        break;
    }

    if (pColVal) {
      if (taosArrayPush(aColVal, pColVal) == NULL) {
        code = terrno;
        goto _exit;
      }
    }
  }

  // build
  SRowBuildScanInfo sinfo = {0};
  code = tRowBuild(aColVal, pTSchema, &pRow, &sinfo);

  if (code) goto _exit;

  taosArrayRemoveBatch(aRowP, iStart, nRow, (FDelete)tRowPDestroy);
  if (taosArrayInsert(aRowP, iStart, &pRow) == NULL) {
    code = terrno;
    goto _exit;
  }

_exit:
  if (aIter) {
    for (int32_t i = 0; i < nRow; i++) {
      tRowIterClose(&aIter[i]);
    }
    taosMemoryFree(aIter);
  }
  if (aColVal) taosArrayDestroy(aColVal);
  if (code) tRowDestroy(pRow);
  return code;
}
static int32_t tBlobSetTransferTo(SBlobSet *pSrc, SBlobSet *pDst, SColVal *pVal) {
  int32_t code = 0;
  int32_t lino = 0;
  if (COL_VAL_IS_NULL(pVal) || pVal->value.pData == NULL) {
    int8_t type = COL_VAL_IS_NULL(pVal) ? TSDB_DATA_BLOB_NULL_VALUE : TSDB_DATA_BLOB_EMPTY_VALUE;
    code = addEmptyItemToBlobSet(pDst, type, NULL);
    TAOS_CHECK_GOTO(code, &lino, _error);
  } else {
    uint64_t seq = 0;
    if (tGetU64(pVal->value.pData, &seq) < 0) {
      uError("tBlobSetTransferTo: invalid blob value, seq %p", pVal->value.pData);
      return TSDB_CODE_INVALID_PARA;
    }

    SBlobItem item = {0};
    code = tBlobSetGet(pSrc, seq, &item);
    TAOS_CHECK_GOTO(code, &lino, _error);

    code = tBlobSetPush(pDst, &item, &seq, 1);
    TAOS_CHECK_GOTO(code, &lino, _error);
    if (tPutU64(pVal->value.pData, seq) < 0) {
      uError("tBlobSetTransferTo: put seq to colVal failed");
      return TSDB_CODE_INVALID_PARA;
    }
  }

_error:
  return code;
}

static int32_t tRowRebuildBlob(SArray *aRowP, STSchema *pTSchema, SBlobSet *pBlob) {
  int32_t code = 0;
  int32_t   nRow = TARRAY_SIZE(aRowP);
  int32_t   lino = 0;
  SBlobSet *pTempBlob = NULL;

  SArray *aColVal = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
  if (aColVal == NULL) {
    TAOS_CHECK_RETURN(code = terrno);
  }

  SRowIter **aIter = taosMemoryCalloc(nRow, sizeof(SRowIter *));
  if (aIter == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _error);
  }

  for (int32_t i = 0; i < nRow; i++) {
    SRow *pRowT = taosArrayGetP(aRowP, 0 + i);
    code = tRowIterOpen(pRowT, pTSchema, &aIter[i]);
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  for (int32_t i = 0; i < nRow; i++) {
    SColVal *pColVal = tRowIterNext(aIter[i]);
    do {
      if (COL_VAL_IS_VALUE(pColVal) || COL_VAL_IS_NULL(pColVal)) {
        if (IS_STR_DATA_BLOB(pColVal->value.type)) {
          if (taosArrayPush(aColVal, pColVal) == NULL) {
            code = terrno;
            TAOS_CHECK_GOTO(code, &lino, _error);
          }
          break;
        }
      }
    } while ((pColVal = tRowIterNext(aIter[i])) != NULL);
  }

  code = tBlobSetCreate(pBlob->cap, pBlob->type, &pTempBlob);
  TAOS_CHECK_GOTO(code, &lino, _error);

  for (int32_t i = 0; i < taosArrayGetSize(aColVal); i++) {
    uint64_t seq = 0;
    SColVal *pVal = taosArrayGet(aColVal, i);

    code = tBlobSetTransferTo(pBlob, pTempBlob, pVal);
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  tBlobSetSwap(pBlob, pTempBlob);

_error:
  if (code != 0) {
    uError("tRowRebuildBlob failed at line %d, code %d", code, lino);
  }

  if (aIter) {
    for (int32_t i = 0; i < nRow; i++) {
      tRowIterClose(&aIter[i]);
    }
    taosMemoryFree(aIter);
  }
  if (aColVal) {
    taosArrayDestroy(aColVal);
  }
  tBlobSetDestroy(pTempBlob);
  return code;
}

static int32_t tRowMergeAndRebuildBlob(SArray *aRowP, STSchema *pTSchema, SBlobSet *pBlob) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlobSet *pTempBlobSet = NULL;
  int32_t   size = taosArrayGetSize(aRowP);
  if (size <= 1) {
    return code;
  }
  int32_t colBlobIdx = 0;
  for (int32_t i = 0; i < pTSchema->numOfCols; i++) {
    if (IS_STR_DATA_BLOB(pTSchema->columns[i].type)) {
      colBlobIdx = i;
      break;
    }
  }

  code = tBlobSetCreate(pBlob->cap, pBlob->type, &pTempBlobSet);
  TAOS_CHECK_GOTO(code, &lino, _error);

  int32_t iStart = 0;
  while (iStart < aRowP->size) {
    SRowKey key1;
    SRow   *row1 = (SRow *)taosArrayGetP(aRowP, iStart);

    tRowGetKey(row1, &key1);

    int32_t iEnd = iStart + 1;
    while (iEnd < aRowP->size) {
      SRowKey key2;
      SRow   *row2 = (SRow *)taosArrayGetP(aRowP, iEnd);
      tRowGetKey(row2, &key2);

      if (tRowKeyCompare(&key1, &key2) != 0) break;

      iEnd++;
    }

    if (iEnd - iStart > 1) {
      code = tRowMergeWithBlobImpl(aRowP, pTSchema, pBlob, pTempBlobSet, iStart, iEnd, 0);
      TAOS_CHECK_GOTO(code, &lino, _error);
    } else {
      SColVal colVal = {0};
      code = tRowGet(row1, pTSchema, colBlobIdx, &colVal);
      TAOS_CHECK_GOTO(code, &lino, _error);

      code = tBlobSetTransferTo(pBlob, pTempBlobSet, &colVal);
      TAOS_CHECK_GOTO(code, &lino, _error);
    }
    // the array is also changing, so the iStart just ++ instead of iEnd
    iStart++;
  }

_error:
  if (pBlob && pTempBlobSet) {
    tBlobSetSwap(pBlob, pTempBlobSet);
  }

  tBlobSetDestroy(pTempBlobSet);
  return code;
}

static int32_t tRowMergeWithBlobImpl(SArray *aRowP, STSchema *pTSchema, SBlobSet *pBlob, SBlobSet *pDstBlob,
                                     int32_t iStart, int32_t iEnd, int8_t flag) {
  int32_t code = 0;
  int32_t    lino = 0;
  int32_t    nRow = iEnd - iStart;
  SRowIter **aIter = NULL;
  SArray    *aColVal = NULL;
  SRow      *pRow = NULL;

  aColVal = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
  if (aColVal == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  aIter = taosMemoryCalloc(nRow, sizeof(SRowIter *));
  if (aIter == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  for (int32_t i = 0; i < nRow; i++) {
    SRow *pRowT = taosArrayGetP(aRowP, iStart + i);
    code = tRowIterOpen(pRowT, pTSchema, &aIter[i]);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }
  // merge

  for (int32_t iCol = 0; iCol < pTSchema->numOfCols; iCol++) {
    SColVal  *pColVal = NULL;
    STColumn *pCol = pTSchema->columns + iCol;

    for (int32_t iRow = nRow - 1; iRow >= 0; --iRow) {
      SColVal *pColValT = tRowIterNext(aIter[iRow]);
      while (pColValT->cid < pTSchema->columns[iCol].colId) {
        pColValT = tRowIterNext(aIter[iRow]);
      }
      // todo: take strategy according to the flag
      if (COL_VAL_IS_VALUE(pColValT)) {
        pColVal = pColValT;
        break;
      } else if (COL_VAL_IS_NULL(pColValT)) {
        if (pColVal == NULL) {
          pColVal = pColValT;
        }
      }
    }

    if (pColVal) {
      if (taosArrayPush(aColVal, pColVal) == NULL) {
        code = terrno;
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
    }
  }

  // build

  SRowBuildScanInfo sinfo = {.hasBlob = 1};
  code = tRowBuildWithBlob2(aColVal, pTSchema, &pRow, pBlob, pDstBlob, &sinfo);
  TAOS_CHECK_GOTO(code, &lino, _exit);

  taosArrayRemoveBatch(aRowP, iStart, nRow, (FDelete)tRowPDestroy);
  if (taosArrayInsert(aRowP, iStart, &pRow) == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

_exit:
  if (aIter) {
    for (int32_t i = 0; i < nRow; i++) {
      tRowIterClose(&aIter[i]);
    }
    taosMemoryFree(aIter);
  }
  if (aColVal) taosArrayDestroy(aColVal);
  if (code) tRowDestroy(pRow);

  return code;
}

int32_t tRowSort(SArray *aRowP) {
  if (TARRAY_SIZE(aRowP) <= 1) return 0;
  int32_t code = taosArrayMSort(aRowP, tRowPCmprFn);
  if (code != TSDB_CODE_SUCCESS) {
    uError("taosArrayMSort failed caused by %d", code);
  }
  return code;
}

int32_t tRowMerge(SArray *aRowP, STSchema *pTSchema, ERowMergeStrategy strategy) {
  int32_t code = 0;

  int32_t iStart = 0;
  while (iStart < aRowP->size) {
    SRowKey key1;
    SRow   *row1 = (SRow *)taosArrayGetP(aRowP, iStart);

    tRowGetKey(row1, &key1);

    int32_t iEnd = iStart + 1;
    while (iEnd < aRowP->size) {
      SRowKey key2;
      SRow   *row2 = (SRow *)taosArrayGetP(aRowP, iEnd);
      tRowGetKey(row2, &key2);

      if (tRowKeyCompare(&key1, &key2) != 0) break;

      iEnd++;
    }

    if (iEnd - iStart > 1) {
      code = tRowMergeImpl(aRowP, pTSchema, iStart, iEnd, strategy);
      if (code) return code;
    }

    // the array is also changing, so the iStart just ++ instead of iEnd
    iStart++;
  }

  return code;
}

int32_t tRowSortWithBlob(SArray *aRowP, STSchema *pTSchema, SBlobSet *pBlobSet) {
  if (TARRAY_SIZE(aRowP) <= 1) return 0;
  int32_t code = taosArrayMSort(aRowP, tRowPCmprFn);
  if (code != TSDB_CODE_SUCCESS) {
    uError("taosArrayMSort failed caused by %d", code);
    return code;
  }
  return code;
}

int8_t tRowNeedDoMerge(SArray *aRowP) {
  if (aRowP == NULL || aRowP->size <= 1) return 0;

  int8_t  doMerge = 0;
  int32_t iStart = 0;
  while (iStart < aRowP->size) {
    SRowKey key1;
    SRow   *row1 = (SRow *)taosArrayGetP(aRowP, iStart);

    tRowGetKey(row1, &key1);

    int32_t iEnd = iStart + 1;
    while (iEnd < aRowP->size) {
      SRowKey key2;
      SRow   *row2 = (SRow *)taosArrayGetP(aRowP, iEnd);
      tRowGetKey(row2, &key2);

      if (tRowKeyCompare(&key1, &key2) != 0) break;

      iEnd++;
    }

    if (iEnd - iStart > 1) {
      doMerge = 1;
      break;
    }
    iStart++;
  }
  return doMerge;
}

int32_t tRowMergeWithBlob(SArray *aRowP, STSchema *pTSchema, SBlobSet *pBlobSet, int8_t flag) {
  int32_t code = 0;

  int8_t doMerge = tRowNeedDoMerge(aRowP);
  if (!doMerge) {
    code = tRowRebuildBlob(aRowP, pTSchema, pBlobSet);
  } else {
    code = tRowRebuildBlob(aRowP, pTSchema, pBlobSet);
    TAOS_CHECK_RETURN(code);

    code = tRowMergeAndRebuildBlob(aRowP, pTSchema, pBlobSet);
  }
  return code;
}

// SRowIter ========================================
struct SRowIter {
  SRow     *pRow;
  STSchema *pTSchema;

  int32_t iTColumn;
  union {
    struct {  // kv
      int32_t iCol;
      SKVIdx *pIdx;
    };
    struct {  // tuple
      uint8_t *pb;
      uint8_t *pf;
    };
  };
  uint8_t *pv;
  SColVal  cv;
};

int32_t tRowIterOpen(SRow *pRow, STSchema *pTSchema, SRowIter **ppIter) {
  if (!(pRow->sver == pTSchema->version)) return TSDB_CODE_INVALID_PARA;

  int32_t code = 0;

  SRowIter *pIter = taosMemoryCalloc(1, sizeof(*pIter));
  if (pIter == NULL) {
    code = terrno;
    goto _exit;
  }

  pIter->pRow = pRow;
  pIter->pTSchema = pTSchema;
  pIter->iTColumn = 0;

  if (pRow->flag == HAS_NONE || pRow->flag == HAS_NULL) goto _exit;

  uint8_t         *data = pRow->data;
  SPrimaryKeyIndex index;
  for (int32_t i = 0; i < pRow->numOfPKs; i++) {
    data += tGetPrimaryKeyIndex(data, &index);
  }

  if (pRow->flag >> 4) {
    pIter->iCol = 0;
    pIter->pIdx = (SKVIdx *)data;
    if (pRow->flag & KV_FLG_LIT) {
      pIter->pv = pIter->pIdx->idx + pIter->pIdx->nCol;
    } else if (pRow->flag & KV_FLG_MID) {
      pIter->pv = pIter->pIdx->idx + (pIter->pIdx->nCol << 1);  // * sizeof(uint16_t)
    } else {
      pIter->pv = pIter->pIdx->idx + (pIter->pIdx->nCol << 2);  // * sizeof(uint32_t)
    }
  } else {
    switch (pRow->flag) {
      case (HAS_NULL | HAS_NONE):
        pIter->pb = data;
        break;
      case HAS_VALUE:
        pIter->pf = data;
        pIter->pv = pIter->pf + pTSchema->flen;
        break;
      case (HAS_VALUE | HAS_NONE):
      case (HAS_VALUE | HAS_NULL):
        pIter->pb = data;
        pIter->pf = data + BIT1_SIZE(pTSchema->numOfCols - 1);
        pIter->pv = pIter->pf + pTSchema->flen;
        break;
      case (HAS_VALUE | HAS_NULL | HAS_NONE):
        pIter->pb = data;
        pIter->pf = data + BIT2_SIZE(pTSchema->numOfCols - 1);
        pIter->pv = pIter->pf + pTSchema->flen;
        break;
      default:
        break;
    }
  }

_exit:
  if (code) {
    *ppIter = NULL;
  } else {
    *ppIter = pIter;
  }
  return code;
}

void tRowIterClose(SRowIter **ppIter) {
  SRowIter *pIter = *ppIter;
  if (pIter) {
    taosMemoryFree(pIter);
  }
  *ppIter = NULL;
}

SColVal *tRowIterNext(SRowIter *pIter) {
  if (pIter->iTColumn >= pIter->pTSchema->numOfCols) {
    return NULL;
  }

  STColumn *pTColumn = pIter->pTSchema->columns + pIter->iTColumn;

  // timestamp
  if (0 == pIter->iTColumn) {
    pIter->cv.cid = pTColumn->colId;
    pIter->cv.value.type = pTColumn->type;
    pIter->cv.flag = CV_FLAG_VALUE;
    VALUE_SET_TRIVIAL_DATUM(&pIter->cv.value, pIter->pRow->ts);
    goto _exit;
  }

  if (pIter->pRow->flag == HAS_NONE) {
    pIter->cv = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
    goto _exit;
  }

  if (pIter->pRow->flag == HAS_NULL) {
    pIter->cv = COL_VAL_NULL(pTColumn->colId, pTColumn->type);
    goto _exit;
  }

  if (pIter->pRow->flag >> 4) {  // KV
    if (pIter->iCol < pIter->pIdx->nCol) {
      uint8_t *pData;

      if (pIter->pRow->flag & KV_FLG_LIT) {
        pData = pIter->pv + ((uint8_t *)pIter->pIdx->idx)[pIter->iCol];
      } else if (pIter->pRow->flag & KV_FLG_MID) {
        pData = pIter->pv + ((uint16_t *)pIter->pIdx->idx)[pIter->iCol];
      } else {
        pData = pIter->pv + ((uint32_t *)pIter->pIdx->idx)[pIter->iCol];
      }

      int16_t cid;
      pData += tGetI16v(pData, &cid);

      if (TABS(cid) == pTColumn->colId) {
        if (cid < 0) {
          pIter->cv = COL_VAL_NULL(pTColumn->colId, pTColumn->type);
        } else {
          pIter->cv.cid = pTColumn->colId;
          pIter->cv.value.type = pTColumn->type;
          pIter->cv.flag = CV_FLAG_VALUE;

          if (IS_VAR_DATA_TYPE(pTColumn->type)) {
            pData += tGetU32v(pData, &pIter->cv.value.nData);
            if (pIter->cv.value.nData > 0) {
              pIter->cv.value.pData = pData;
            } else {
              pIter->cv.value.pData = NULL;
              // if (IS_STR_DATA_BLOB(pTColumn->type)) {
              //   pIter->cv.value.pData = pData;
              // }
            }
          } else {
            valueSetDatum(&pIter->cv.value, pTColumn->type, pData, pTColumn->bytes);
          }
        }

        pIter->iCol++;
        goto _exit;
      } else if (TABS(cid) > pTColumn->colId) {
        pIter->cv = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
        goto _exit;
      } else {
        uError("unexpected column id %d, %d", cid, pTColumn->colId);
        goto _exit;
      }
    } else {
      pIter->cv = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
      goto _exit;
    }
  } else {  // Tuple
    uint8_t bv = BIT_FLG_VALUE;
    if (pIter->pb) {
      switch (pIter->pRow->flag) {
        case (HAS_NULL | HAS_NONE):
          bv = GET_BIT1(pIter->pb, pIter->iTColumn - 1);
          break;
        case (HAS_VALUE | HAS_NONE):
          bv = GET_BIT1(pIter->pb, pIter->iTColumn - 1);
          if (bv) bv++;
          break;
        case (HAS_VALUE | HAS_NULL):
          bv = GET_BIT1(pIter->pb, pIter->iTColumn - 1) + 1;
          break;
        case (HAS_VALUE | HAS_NULL | HAS_NONE):
          bv = GET_BIT2(pIter->pb, pIter->iTColumn - 1);
          break;
        default:
          break;
      }

      if (bv == BIT_FLG_NONE) {
        pIter->cv = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
        goto _exit;
      } else if (bv == BIT_FLG_NULL) {
        pIter->cv = COL_VAL_NULL(pTColumn->colId, pTColumn->type);
        goto _exit;
      }
    }

    pIter->cv.cid = pTColumn->colId;
    pIter->cv.value.type = pTColumn->type;
    pIter->cv.flag = CV_FLAG_VALUE;
    if (IS_VAR_DATA_TYPE(pTColumn->type)) {
      uint8_t *pData = pIter->pv + *(int32_t *)(pIter->pf + pTColumn->offset);
      pData += tGetU32v(pData, &pIter->cv.value.nData);
      if (pIter->cv.value.nData > 0) {
        pIter->cv.value.pData = pData;
      } else {
        pIter->cv.value.pData = NULL;
        // if (IS_STR_DATA_BLOB(pTColumn->type)) {
        //   pIter->cv.value.pData = pData;
        // }
      }
    } else {
      valueSetDatum(&pIter->cv.value, pTColumn->type, pIter->pf + pTColumn->offset, TYPE_BYTES[pTColumn->type]);
    }
    goto _exit;
  }

_exit:
  pIter->iTColumn++;
  return &pIter->cv;
}

static int32_t tRowNoneUpsertColData(SColData *aColData, int32_t nColData, int32_t flag) {
  int32_t code = 0;

  if (flag) return code;

  for (int32_t iColData = 0; iColData < nColData; iColData++) {
    code = tColDataAppendValueImpl[aColData[iColData].flag][CV_FLAG_NONE](&aColData[iColData], NULL, 0);
    if (code) return code;
  }

  return code;
}
static int32_t tRowNullUpsertColData(SColData *aColData, int32_t nColData, STSchema *pSchema, int32_t flag) {
  int32_t code = 0;

  int32_t   iColData = 0;
  SColData *pColData = &aColData[iColData];
  int32_t   iTColumn = 1;
  STColumn *pTColumn = &pSchema->columns[iTColumn];

  while (pColData) {
    if (pTColumn) {
      if (pTColumn->colId == pColData->cid) {  // NULL
        if (flag == 0) {
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
        } else {
          code = tColDataUpdateValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0, flag > 0);
        }
        if (code) goto _exit;

        pColData = (++iColData < nColData) ? &aColData[iColData] : NULL;
        pTColumn = (++iTColumn < pSchema->numOfCols) ? &pSchema->columns[iTColumn] : NULL;
      } else if (pTColumn->colId > pColData->cid) {  // NONE
        if (flag == 0 && (code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0))) goto _exit;
        pColData = (++iColData < nColData) ? &aColData[iColData] : NULL;
      } else {
        pTColumn = (++iTColumn < pSchema->numOfCols) ? &pSchema->columns[iTColumn] : NULL;
      }
    } else {  // NONE
      if (flag == 0 && (code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0))) goto _exit;
      pColData = (++iColData < nColData) ? &aColData[iColData] : NULL;
    }
  }

_exit:
  return code;
}
static int32_t tRowTupleUpsertColData(SRow *pRow, STSchema *pTSchema, SColData *aColData, int32_t nColData,
                                      int32_t flag) {
  int32_t code = 0;

  int32_t   iColData = 0;
  SColData *pColData = &aColData[iColData];
  int32_t   iTColumn = 1;
  STColumn *pTColumn = &pTSchema->columns[iTColumn];

  uint8_t         *pb = NULL, *pf = NULL, *pv = NULL;
  SPrimaryKeyIndex index;
  uint8_t         *data = pRow->data;
  for (int32_t i = 0; i < pRow->numOfPKs; i++) {
    data += tGetPrimaryKeyIndex(data, &index);
  }

  switch (pRow->flag) {
    case HAS_VALUE:
      pf = data;  // TODO: fix here
      pv = pf + pTSchema->flen;
      break;
    case (HAS_NULL | HAS_NONE):
      pb = data;
      break;
    case (HAS_VALUE | HAS_NONE):
    case (HAS_VALUE | HAS_NULL):
      pb = data;
      pf = pb + BIT1_SIZE(pTSchema->numOfCols - 1);
      pv = pf + pTSchema->flen;
      break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      pb = data;
      pf = pb + BIT2_SIZE(pTSchema->numOfCols - 1);
      pv = pf + pTSchema->flen;
      break;
    default:
      return TSDB_CODE_INVALID_DATA_FMT;
  }

  while (pColData) {
    if (pTColumn) {
      if (pTColumn->colId == pColData->cid) {
        if (!(pTColumn->type == pColData->type)) {
          return TSDB_CODE_INVALID_PARA;
        }
        if (pb) {
          uint8_t bv;
          switch (pRow->flag) {
            case (HAS_NULL | HAS_NONE):
              bv = GET_BIT1(pb, iTColumn - 1);
              break;
            case (HAS_VALUE | HAS_NONE):
              bv = GET_BIT1(pb, iTColumn - 1);
              if (bv) bv++;
              break;
            case (HAS_VALUE | HAS_NULL):
              bv = GET_BIT1(pb, iTColumn - 1) + 1;
              break;
            case (HAS_VALUE | HAS_NULL | HAS_NONE):
              bv = GET_BIT2(pb, iTColumn - 1);
              break;
            default:
              return TSDB_CODE_INVALID_DATA_FMT;
          }

          if (bv == BIT_FLG_NONE) {
            if (flag == 0 && (code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0)))
              goto _exit;
            goto _continue;
          } else if (bv == BIT_FLG_NULL) {
            if (flag == 0) {
              code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
            } else {
              code = tColDataUpdateValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0, flag > 0);
            }
            if (code) goto _exit;
            goto _continue;
          }
        }

        if (IS_VAR_DATA_TYPE(pColData->type)) {
          uint8_t *pData = pv + *(int32_t *)(pf + pTColumn->offset);
          uint32_t nData;
          pData += tGetU32v(pData, &nData);
          if (flag == 0) {
            code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, pData, nData);
          } else {
            code = tColDataUpdateValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, pData, nData, flag > 0);
          }
          if (code) goto _exit;
        } else {
          if (flag == 0) {
            code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, pf + pTColumn->offset,
                                                                          TYPE_BYTES[pColData->type]);
          } else {
            code = tColDataUpdateValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, pf + pTColumn->offset,
                                                                          TYPE_BYTES[pColData->type], flag > 0);
          }
          if (code) goto _exit;
        }

      _continue:
        pColData = (++iColData < nColData) ? &aColData[iColData] : NULL;
        pTColumn = (++iTColumn < pTSchema->numOfCols) ? &pTSchema->columns[iTColumn] : NULL;
      } else if (pTColumn->colId > pColData->cid) {  // NONE
        if (flag == 0 && (code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0))) goto _exit;
        pColData = (++iColData < nColData) ? &aColData[iColData] : NULL;
      } else {
        pTColumn = (++iTColumn < pTSchema->numOfCols) ? &pTSchema->columns[iTColumn] : NULL;
      }
    } else {
      if (flag == 0 && (code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0))) goto _exit;
      pColData = (++iColData < nColData) ? &aColData[iColData] : NULL;
    }
  }

_exit:
  return code;
}
static int32_t tRowKVUpsertColData(SRow *pRow, STSchema *pTSchema, SColData *aColData, int32_t nColData, int32_t flag) {
  int32_t code = 0;

  uint8_t  *pv = NULL;
  int32_t   iColData = 0;
  SColData *pColData = &aColData[iColData];
  int32_t   iTColumn = 1;
  STColumn *pTColumn = &pTSchema->columns[iTColumn];
  int32_t   iCol = 0;

  // primary keys
  uint8_t         *data = pRow->data;
  SPrimaryKeyIndex index;
  for (int32_t i = 0; i < pRow->numOfPKs; i++) {
    data += tGetPrimaryKeyIndex(data, &index);
  }

  SKVIdx *pKVIdx = (SKVIdx *)data;
  if (pRow->flag & KV_FLG_LIT) {
    pv = pKVIdx->idx + pKVIdx->nCol;
  } else if (pRow->flag & KV_FLG_MID) {
    pv = pKVIdx->idx + (pKVIdx->nCol << 1);
  } else if (pRow->flag & KV_FLG_BIG) {
    pv = pKVIdx->idx + (pKVIdx->nCol << 2);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  while (pColData) {
    if (pTColumn) {
      if (pTColumn->colId == pColData->cid) {
        while (iCol < pKVIdx->nCol) {
          uint8_t *pData;
          if (pRow->flag & KV_FLG_LIT) {
            pData = pv + ((uint8_t *)pKVIdx->idx)[iCol];
          } else if (pRow->flag & KV_FLG_MID) {
            pData = pv + ((uint16_t *)pKVIdx->idx)[iCol];
          } else if (pRow->flag & KV_FLG_BIG) {
            pData = pv + ((uint32_t *)pKVIdx->idx)[iCol];
          } else {
            return TSDB_CODE_INVALID_DATA_FMT;
          }

          int16_t cid;
          pData += tGetI16v(pData, &cid);

          if (TABS(cid) == pTColumn->colId) {
            if (cid < 0) {
              if (flag == 0) {
                code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
              } else {
                code = tColDataUpdateValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0, flag > 0);
              }
              if (code) goto _exit;
            } else {
              uint32_t nData;
              if (IS_VAR_DATA_TYPE(pTColumn->type)) {
                pData += tGetU32v(pData, &nData);
              } else {
                nData = 0;
              }
              if (flag == 0) {
                code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, pData, nData);
              } else {
                code = tColDataUpdateValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, pData, nData, flag > 0);
              }
              if (code) goto _exit;
            }
            iCol++;
            goto _continue;
          } else if (TABS(cid) > pTColumn->colId) {  // NONE
            break;
          } else {
            iCol++;
          }
        }

        if (flag == 0 && (code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0))) goto _exit;

      _continue:
        pColData = (++iColData < nColData) ? &aColData[iColData] : NULL;
        pTColumn = (++iTColumn < pTSchema->numOfCols) ? &pTSchema->columns[iTColumn] : NULL;
      } else if (pTColumn->colId > pColData->cid) {
        if (flag == 0 && (code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0))) goto _exit;
        pColData = (++iColData < nColData) ? &aColData[iColData] : NULL;
      } else {
        pTColumn = (++iTColumn < pTSchema->numOfCols) ? &pTSchema->columns[iTColumn] : NULL;
      }
    } else {
      if (flag == 0 && (code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0))) goto _exit;
      pColData = (++iColData < nColData) ? &aColData[iColData] : NULL;
    }
  }

_exit:
  return code;
}
/* flag > 0: forward update
 * flag == 0: append
 * flag < 0: backward update
 */
int32_t tRowUpsertColData(SRow *pRow, STSchema *pTSchema, SColData *aColData, int32_t nColData, int32_t flag) {
  if (!(pRow->sver == pTSchema->version)) return TSDB_CODE_INVALID_PARA;
  if (!(nColData > 0)) return TSDB_CODE_INVALID_PARA;

  if (pRow->flag == HAS_NONE) {
    return tRowNoneUpsertColData(aColData, nColData, flag);
  } else if (pRow->flag == HAS_NULL) {
    return tRowNullUpsertColData(aColData, nColData, pTSchema, flag);
  } else if (pRow->flag >> 4) {  // KV row
    return tRowKVUpsertColData(pRow, pTSchema, aColData, nColData, flag);
  } else {  // TUPLE row
    return tRowTupleUpsertColData(pRow, pTSchema, aColData, nColData, flag);
  }
}

void tRowGetPrimaryKey(SRow *row, SRowKey *key) {
  key->numOfPKs = row->numOfPKs;

  if (key->numOfPKs == 0) {
    return;
  }

  SPrimaryKeyIndex indices[TD_MAX_PK_COLS];

  uint8_t *data = row->data;

  for (int32_t i = 0; i < row->numOfPKs; i++) {
    data += tGetPrimaryKeyIndex(data, &indices[i]);
  }

  // primary keys
  for (int32_t i = 0; i < row->numOfPKs; i++) {
    key->pks[i].type = indices[i].type;

    uint8_t *tdata = data + indices[i].offset;
    if (row->flag >> 4) {
      tdata += tGetI16v(tdata, NULL);
    }

    if (IS_VAR_DATA_TYPE(indices[i].type)) {
      key->pks[i].pData = tdata;
      key->pks[i].pData += tGetU32v(key->pks[i].pData, &key->pks[i].nData);
    } else {
      valueSetDatum(key->pks + i, indices[i].type, tdata, tDataTypes[indices[i].type].bytes);
    }
  }
}

#define T_COMPARE_SCALAR_VALUE(TYPE, V1, V2)    \
  do {                                          \
    if (*(TYPE *)(V1) < *(TYPE *)(V2)) {        \
      return -1;                                \
    } else if (*(TYPE *)(V1) > *(TYPE *)(V2)) { \
      return 1;                                 \
    } else {                                    \
      return 0;                                 \
    }                                           \
  } while (0)

int32_t tValueCompare(const SValue *tv1, const SValue *tv2) {
  switch (tv1->type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
      T_COMPARE_SCALAR_VALUE(int8_t, &VALUE_GET_TRIVIAL_DATUM(tv1), &VALUE_GET_TRIVIAL_DATUM(tv2));
    case TSDB_DATA_TYPE_SMALLINT:
      T_COMPARE_SCALAR_VALUE(int16_t, &VALUE_GET_TRIVIAL_DATUM(tv1), &VALUE_GET_TRIVIAL_DATUM(tv2));
    case TSDB_DATA_TYPE_INT:
      T_COMPARE_SCALAR_VALUE(int32_t, &VALUE_GET_TRIVIAL_DATUM(tv1), &VALUE_GET_TRIVIAL_DATUM(tv2));
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      T_COMPARE_SCALAR_VALUE(int64_t, &VALUE_GET_TRIVIAL_DATUM(tv1), &VALUE_GET_TRIVIAL_DATUM(tv2));
    case TSDB_DATA_TYPE_FLOAT:
      T_COMPARE_SCALAR_VALUE(float, &VALUE_GET_TRIVIAL_DATUM(tv1), &VALUE_GET_TRIVIAL_DATUM(tv2));
    case TSDB_DATA_TYPE_DOUBLE:
      T_COMPARE_SCALAR_VALUE(double, &VALUE_GET_TRIVIAL_DATUM(tv1), &VALUE_GET_TRIVIAL_DATUM(tv2));
    case TSDB_DATA_TYPE_UTINYINT:
      T_COMPARE_SCALAR_VALUE(uint8_t, &VALUE_GET_TRIVIAL_DATUM(tv1), &VALUE_GET_TRIVIAL_DATUM(tv2));
    case TSDB_DATA_TYPE_USMALLINT:
      T_COMPARE_SCALAR_VALUE(uint16_t, &VALUE_GET_TRIVIAL_DATUM(tv1), &VALUE_GET_TRIVIAL_DATUM(tv2));
    case TSDB_DATA_TYPE_UINT:
      T_COMPARE_SCALAR_VALUE(uint32_t, &VALUE_GET_TRIVIAL_DATUM(tv1), &VALUE_GET_TRIVIAL_DATUM(tv2));
    case TSDB_DATA_TYPE_UBIGINT:
      T_COMPARE_SCALAR_VALUE(uint64_t, &VALUE_GET_TRIVIAL_DATUM(tv1), &VALUE_GET_TRIVIAL_DATUM(tv2));
    case TSDB_DATA_TYPE_GEOMETRY:
    case TSDB_DATA_TYPE_BINARY: {
      int32_t ret = strncmp((const char *)tv1->pData, (const char *)tv2->pData, TMIN(tv1->nData, tv2->nData));
      return ret ? ret : (tv1->nData < tv2->nData ? -1 : (tv1->nData > tv2->nData ? 1 : 0));
    }
    case TSDB_DATA_TYPE_NCHAR: {
      int32_t ret = taosUcs4Compare((TdUcs4 *)tv1->pData, (TdUcs4 *)tv2->pData,
                                    tv1->nData < tv2->nData ? tv1->nData : tv2->nData);
      return ret ? ret : (tv1->nData < tv2->nData ? -1 : (tv1->nData > tv2->nData ? 1 : 0));
    }
    case TSDB_DATA_TYPE_VARBINARY: {
      int32_t ret = memcmp(tv1->pData, tv2->pData, tv1->nData < tv2->nData ? tv1->nData : tv2->nData);
      return ret ? ret : (tv1->nData < tv2->nData ? -1 : (tv1->nData > tv2->nData ? 1 : 0));
    }
    default:
      break;
  }

  return 0;
}

// NOTE:
// set key->numOfPKs to 0 as the smallest key with ts
// set key->numOfPKs to (TD_MAX_PK_COLS + 1) as the largest key with ts
FORCE_INLINE int32_t tRowKeyCompare(const SRowKey *key1, const SRowKey *key2) {
  if (key1->ts < key2->ts) {
    return -1;
  } else if (key1->ts > key2->ts) {
    return 1;
  }

  if (key1->numOfPKs == key2->numOfPKs) {
    for (uint8_t iKey = 0; iKey < key1->numOfPKs; iKey++) {
      int32_t ret = tValueCompare(&key1->pks[iKey], &key2->pks[iKey]);
      if (ret) return ret;
    }
  } else if (key1->numOfPKs < key2->numOfPKs) {
    return -1;
  } else {
    return 1;
  }

  return 0;
}

void tRowKeyAssign(SRowKey *pDst, SRowKey *pSrc) {
  pDst->ts = pSrc->ts;
  pDst->numOfPKs = pSrc->numOfPKs;

  if (pSrc->numOfPKs > 0) {
    for (int32_t i = 0; i < pSrc->numOfPKs; ++i) {
      SValue *pVal = &pDst->pks[i];
      pVal->type = pSrc->pks[i].type;

      valueCloneDatum(pVal, pSrc->pks + i, pVal->type);
    }
  }
}

// STag ========================================
static int tTagValCmprFn(const void *p1, const void *p2) {
  if (((STagVal *)p1)->cid < ((STagVal *)p2)->cid) {
    return -1;
  } else if (((STagVal *)p1)->cid > ((STagVal *)p2)->cid) {
    return 1;
  }

  return 0;
}
static int tTagValJsonCmprFn(const void *p1, const void *p2) {
  return strcmp(((STagVal *)p1)[0].pKey, ((STagVal *)p2)[0].pKey);
}

#ifdef TD_DEBUG_PRINT_TAG
static void debugPrintTagVal(int8_t type, const void *val, int32_t vlen, const char *tag, int32_t ln) {
  switch (type) {
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_GEOMETRY: {
      char tmpVal[32] = {0};
      tstrncpy(tmpVal, val, vlen > 31 ? 31 : vlen);
      printf("%s:%d type:%d vlen:%d, val:\"%s\"\n", tag, ln, (int32_t)type, vlen, tmpVal);
    } break;
    case TSDB_DATA_TYPE_FLOAT:
      printf("%s:%d type:%d vlen:%d, val:%f\n", tag, ln, (int32_t)type, vlen, *(float *)val);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      printf("%s:%d type:%d vlen:%d, val:%lf\n", tag, ln, (int32_t)type, vlen, *(double *)val);
      break;
    case TSDB_DATA_TYPE_BOOL:
      printf("%s:%d type:%d vlen:%d, val:%" PRIu8 "\n", tag, ln, (int32_t)type, vlen, *(uint8_t *)val);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      printf("%s:%d type:%d vlen:%d, val:%" PRIi8 "\n", tag, ln, (int32_t)type, vlen, *(int8_t *)val);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      printf("%s:%d type:%d vlen:%d, val:%" PRIi16 "\n", tag, ln, (int32_t)type, vlen, *(int16_t *)val);
      break;
    case TSDB_DATA_TYPE_INT:
      printf("%s:%d type:%d vlen:%d, val:%" PRIi32 "\n", tag, ln, (int32_t)type, vlen, *(int32_t *)val);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      printf("%s:%d type:%d vlen:%d, val:%" PRIi64 "\n", tag, ln, (int32_t)type, vlen, *(int64_t *)val);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      printf("%s:%d type:%d vlen:%d, val:%" PRIi64 "\n", tag, ln, (int32_t)type, vlen, *(int64_t *)val);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      printf("%s:%d type:%d vlen:%d, val:%" PRIu8 "\n", tag, ln, (int32_t)type, vlen, *(uint8_t *)val);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      printf("%s:%d type:%d vlen:%d, val:%" PRIu16 "\n", tag, ln, (int32_t)type, vlen, *(uint16_t *)val);
      break;
    case TSDB_DATA_TYPE_UINT:
      printf("%s:%d type:%d vlen:%d, val:%" PRIu32 "\n", tag, ln, (int32_t)type, vlen, *(uint32_t *)val);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      printf("%s:%d type:%d vlen:%d, val:%" PRIu64 "\n", tag, ln, (int32_t)type, vlen, *(uint64_t *)val);
      break;
    case TSDB_DATA_TYPE_NULL:
      printf("%s:%d type:%d vlen:%d, val:%" PRIi8 "\n", tag, ln, (int32_t)type, vlen, *(int8_t *)val);
      break;
    default:
      break;
  }
}

void debugPrintSTag(STag *pTag, const char *tag, int32_t ln) {
  int8_t   isJson = pTag->flags & TD_TAG_JSON;
  int8_t   isLarge = pTag->flags & TD_TAG_LARGE;
  uint8_t *p = NULL;
  int16_t  offset = 0;

  if (isLarge) {
    p = (uint8_t *)&((int16_t *)pTag->idx)[pTag->nTag];
  } else {
    p = (uint8_t *)&pTag->idx[pTag->nTag];
  }
  printf("%s:%d >>> STAG === %s:%s, len: %d, nTag: %d, sver:%d\n", tag, ln, isJson ? "json" : "normal",
         isLarge ? "large" : "small", (int32_t)pTag->len, (int32_t)pTag->nTag, pTag->ver);
  for (uint16_t n = 0; n < pTag->nTag; ++n) {
    if (isLarge) {
      offset = ((int16_t *)pTag->idx)[n];
    } else {
      offset = pTag->idx[n];
    }
    STagVal tagVal = {0};
    if (isJson) {
      tagVal.pKey = (char *)POINTER_SHIFT(p, offset);
    } else {
      tagVal.cid = *(int16_t *)POINTER_SHIFT(p, offset);
    }
    printf("%s:%d loop[%d-%d] offset=%d\n", __func__, __LINE__, (int32_t)pTag->nTag, (int32_t)n, (int32_t)offset);
    tGetTagVal(p + offset, &tagVal, isJson);
    if (IS_VAR_DATA_TYPE(tagVal.type)) {
      debugPrintTagVal(tagVal.type, tagVal.pData, tagVal.nData, __func__, __LINE__);
    } else {
      debugPrintTagVal(tagVal.type, &tagVal.i64, tDataTypes[tagVal.type].bytes, __func__, __LINE__);
    }
  }
  printf("\n");
}
#endif

static int32_t tPutTagVal(uint8_t *p, STagVal *pTagVal, int8_t isJson) {
  int32_t n = 0;

  // key
  if (isJson) {
    n += tPutCStr(p ? p + n : p, pTagVal->pKey);
  } else {
    n += tPutI16v(p ? p + n : p, pTagVal->cid);
  }

  // type
  n += tPutI8(p ? p + n : p, pTagVal->type);

  // value
  if (IS_VAR_DATA_TYPE(pTagVal->type)) {
    n += tPutBinary(p ? p + n : p, pTagVal->pData, pTagVal->nData);
  } else {
    p = p ? p + n : p;
    n += tDataTypes[pTagVal->type].bytes;
    if (p) (void)memcpy(p, &(pTagVal->i64), tDataTypes[pTagVal->type].bytes);
  }

  return n;
}
static int32_t tGetTagVal(uint8_t *p, STagVal *pTagVal, int8_t isJson) {
  int32_t n = 0;

  // key
  if (isJson) {
    n += tGetCStr(p + n, &pTagVal->pKey);
  } else {
    n += tGetI16v(p + n, &pTagVal->cid);
  }

  // type
  n += tGetI8(p + n, &pTagVal->type);

  // value
  if (IS_VAR_DATA_TYPE(pTagVal->type)) {
    n += tGetBinary(p + n, &pTagVal->pData, &pTagVal->nData);
  } else {
    (void)memcpy(&(pTagVal->i64), p + n, tDataTypes[pTagVal->type].bytes);
    n += tDataTypes[pTagVal->type].bytes;
  }

  return n;
}

bool tTagIsJson(const void *pTag) { return (((const STag *)pTag)->flags & TD_TAG_JSON); }

bool tTagIsJsonNull(void *data) {
  STag  *pTag = (STag *)data;
  int8_t isJson = tTagIsJson(pTag);
  if (!isJson) return false;
  return ((STag *)data)->nTag == 0;
}

int32_t tTagNew(SArray *pArray, int32_t version, int8_t isJson, STag **ppTag) {
  int32_t  code = 0;
  uint8_t *p = NULL;
  int16_t  n = 0;
  int16_t  nTag = taosArrayGetSize(pArray);
  int32_t  szTag = 0;
  int8_t   isLarge = 0;

  // sort
  if (isJson) {
    taosSort(pArray->pData, nTag, sizeof(STagVal), tTagValJsonCmprFn);
  } else {
    taosSort(pArray->pData, nTag, sizeof(STagVal), tTagValCmprFn);
  }

  // get size
  for (int16_t iTag = 0; iTag < nTag; iTag++) {
    szTag += tPutTagVal(NULL, (STagVal *)taosArrayGet(pArray, iTag), isJson);
  }
  if (szTag <= INT8_MAX) {
    szTag = szTag + sizeof(STag) + sizeof(int8_t) * nTag;
  } else {
    szTag = szTag + sizeof(STag) + sizeof(int16_t) * nTag;
    isLarge = 1;
  }

  // build tag
  (*ppTag) = (STag *)taosMemoryCalloc(szTag, 1);
  if ((*ppTag) == NULL) {
    code = terrno;
    goto _err;
  }
  (*ppTag)->flags = 0;
  if (isJson) {
    (*ppTag)->flags |= TD_TAG_JSON;
  }
  if (isLarge) {
    (*ppTag)->flags |= TD_TAG_LARGE;
  }
  (*ppTag)->len = szTag;
  (*ppTag)->nTag = nTag;
  (*ppTag)->ver = version;

  if (isLarge) {
    p = (uint8_t *)&((int16_t *)(*ppTag)->idx)[nTag];
  } else {
    p = (uint8_t *)&(*ppTag)->idx[nTag];
  }
  n = 0;
  for (int16_t iTag = 0; iTag < nTag; iTag++) {
    if (isLarge) {
      ((int16_t *)(*ppTag)->idx)[iTag] = n;
    } else {
      (*ppTag)->idx[iTag] = n;
    }
    n += tPutTagVal(p + n, (STagVal *)taosArrayGet(pArray, iTag), isJson);
  }
#ifdef TD_DEBUG_PRINT_TAG
  debugPrintSTag(*ppTag, __func__, __LINE__);
#endif

  return code;

_err:
  return code;
}

void tTagFree(STag *pTag) {
  if (pTag) taosMemoryFree(pTag);
}

char *tTagValToData(const STagVal *value, bool isJson) {
  if (!value) {
    return NULL;
  }

  char  *data = NULL;
  int8_t typeBytes = 0;
  if (isJson) {
    typeBytes = CHAR_BYTES;
  }

  if (IS_VAR_DATA_TYPE(value->type)) {
    data = taosMemoryCalloc(1, typeBytes + VARSTR_HEADER_SIZE + value->nData);
    if (data == NULL) {
      return NULL;
    }

    if (isJson) {
      *data = value->type;
    }

    varDataLen(data + typeBytes) = value->nData;
    (void)memcpy(varDataVal(data + typeBytes), value->pData, value->nData);
  } else {
    data = ((char *)&(value->i64)) - typeBytes;  // json with type
  }

  return data;
}

bool tTagGet(const STag *pTag, STagVal *pTagVal) {
  if (!pTag || !pTagVal) {
    return false;
  }

  int16_t  lidx = 0;
  int16_t  ridx = pTag->nTag - 1;
  int16_t  midx;
  uint8_t *p;
  int8_t   isJson = pTag->flags & TD_TAG_JSON;
  int8_t   isLarge = pTag->flags & TD_TAG_LARGE;
  int16_t  offset;
  STagVal  tv;
  int      c;

  if (isLarge) {
    p = (uint8_t *)&((int16_t *)pTag->idx)[pTag->nTag];
  } else {
    p = (uint8_t *)&pTag->idx[pTag->nTag];
  }

  pTagVal->type = TSDB_DATA_TYPE_NULL;
  pTagVal->pData = NULL;
  pTagVal->nData = 0;
  while (lidx <= ridx) {
    midx = (lidx + ridx) / 2;
    if (isLarge) {
      offset = ((int16_t *)pTag->idx)[midx];
    } else {
      offset = pTag->idx[midx];
    }

    int32_t nt = tGetTagVal(p + offset, &tv, isJson);
    if (isJson) {
      c = tTagValJsonCmprFn(pTagVal, &tv);
    } else {
      c = tTagValCmprFn(pTagVal, &tv);
    }

    if (c < 0) {
      ridx = midx - 1;
    } else if (c > 0) {
      lidx = midx + 1;
    } else {
      (void)memcpy(pTagVal, &tv, sizeof(tv));
      return true;
    }
  }
  return false;
}

int32_t tEncodeTag(SEncoder *pEncoder, const STag *pTag) {
  return tEncodeBinary(pEncoder, (const uint8_t *)pTag, pTag->len);
}

int32_t tDecodeTag(SDecoder *pDecoder, STag **ppTag) { return tDecodeBinary(pDecoder, (uint8_t **)ppTag, NULL); }

int32_t tTagToValArray(const STag *pTag, SArray **ppArray) {
  int32_t  code = 0;
  uint8_t *p = NULL;
  STagVal  tv = {0};
  int8_t   isLarge = pTag->flags & TD_TAG_LARGE;
  int16_t  offset = 0;

  if (isLarge) {
    p = (uint8_t *)&((int16_t *)pTag->idx)[pTag->nTag];
  } else {
    p = (uint8_t *)&pTag->idx[pTag->nTag];
  }

  (*ppArray) = taosArrayInit(pTag->nTag + 1, sizeof(STagVal));
  if (*ppArray == NULL) {
    code = terrno;
    goto _err;
  }

  for (int16_t iTag = 0; iTag < pTag->nTag; iTag++) {
    if (isLarge) {
      offset = ((int16_t *)pTag->idx)[iTag];
    } else {
      offset = pTag->idx[iTag];
    }
    int32_t nt = tGetTagVal(p + offset, &tv, pTag->flags & TD_TAG_JSON);
    if (taosArrayPush(*ppArray, &tv) == NULL) {
      code = terrno;
      goto _err;
    }
  }

  return code;

_err:
  return code;
}

// STSchema ========================================
STSchema *tBuildTSchema(SSchema *aSchema, int32_t numOfCols, int32_t version) {
  STSchema *pTSchema = taosMemoryCalloc(1, sizeof(STSchema) + sizeof(STColumn) * numOfCols);
  if (pTSchema == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pTSchema->numOfCols = numOfCols;
  pTSchema->version = version;

  // timestamp column
  if (!(aSchema[0].type == TSDB_DATA_TYPE_TIMESTAMP)) {
    terrno = TSDB_CODE_INVALID_PARA;
    taosMemoryFree(pTSchema);
    return NULL;
  }
  if (!(aSchema[0].colId == PRIMARYKEY_TIMESTAMP_COL_ID)) {
    terrno = TSDB_CODE_INVALID_PARA;
    taosMemoryFree(pTSchema);
    return NULL;
  }
  pTSchema->columns[0].colId = aSchema[0].colId;
  pTSchema->columns[0].type = aSchema[0].type;
  pTSchema->columns[0].flags = aSchema[0].flags;
  pTSchema->columns[0].bytes = TYPE_BYTES[aSchema[0].type];
  pTSchema->columns[0].offset = -1;

  // other columns
  for (int32_t iCol = 1; iCol < numOfCols; iCol++) {
    SSchema  *pSchema = &aSchema[iCol];
    STColumn *pTColumn = &pTSchema->columns[iCol];

    pTColumn->colId = pSchema->colId;
    pTColumn->type = pSchema->type;
    pTColumn->flags = pSchema->flags;
    pTColumn->offset = pTSchema->flen;

    if (IS_VAR_DATA_TYPE(pSchema->type)) {
      pTColumn->bytes = pSchema->bytes;
      pTSchema->tlen += (TYPE_BYTES[pSchema->type] + pSchema->bytes);  // todo: remove
    } else {
      pTColumn->bytes = TYPE_BYTES[pSchema->type];
      pTSchema->tlen += TYPE_BYTES[pSchema->type];  // todo: remove
    }

    pTSchema->flen += TYPE_BYTES[pTColumn->type];
  }

#if 1  // todo : remove this
  pTSchema->tlen += (int32_t)TD_BITMAP_BYTES(numOfCols);
#endif

  return pTSchema;
}

static int32_t tTColumnCompare(const void *p1, const void *p2) {
  if (((STColumn *)p1)->colId < ((STColumn *)p2)->colId) {
    return -1;
  } else if (((STColumn *)p1)->colId > ((STColumn *)p2)->colId) {
    return 1;
  }

  return 0;
}

const STColumn *tTSchemaSearchColumn(const STSchema *pTSchema, int16_t cid) {
  STColumn tcol = {
      .colId = cid,
  };

  return taosbsearch(&tcol, pTSchema->columns, pTSchema->numOfCols, sizeof(STColumn), tTColumnCompare, TD_EQ);
}

// SColData ========================================
void tColDataDestroy(void *ph) {
  if (ph) {
    SColData *pColData = (SColData *)ph;

    tFree(pColData->pBitMap);
    tFree(pColData->aOffset);
    tFree(pColData->pData);
  }
}

void tColDataInit(SColData *pColData, int16_t cid, int8_t type, int8_t cflag) {
  pColData->cid = cid;
  pColData->type = type;
  pColData->cflag = cflag;
  tColDataClear(pColData);
}

void tColDataClear(SColData *pColData) {
  pColData->numOfNone = 0;
  pColData->numOfNull = 0;
  pColData->numOfValue = 0;
  pColData->nVal = 0;
  pColData->flag = 0;
  pColData->nData = 0;
}

void tColDataDeepClear(SColData *pColData) {
  pColData->pBitMap = NULL;
  pColData->aOffset = NULL;
  pColData->pData = NULL;

  tColDataClear(pColData);
}

static FORCE_INLINE int32_t tColDataPutValue(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t  code = 0;
  uint32_t cvtNData = sizeof(uint64_t);
  if (IS_VAR_DATA_TYPE(pColData->type)) {
    // TODO
    if (IS_STR_DATA_BLOB(pColData->type)) {
      code = tRealloc((uint8_t **)(&pColData->aOffset), ((int64_t)(pColData->nVal + 1)) << 2);
      if (code) goto _exit;
      pColData->aOffset[pColData->nVal] = pColData->nData;

      if (nData) {
        code = tRealloc(&pColData->pData, pColData->nData + BSE_SEQUECE_SIZE);
        if (code) goto _exit;
        (void)memcpy(pColData->pData + pColData->nData, pData, BSE_SEQUECE_SIZE);
        pColData->nData += BSE_SEQUECE_SIZE;
      } else {
        // uint64_t zero = 0;
        // (void)memcpy(pColData->pData + pColData->nData, &zero, BSE_SEQUECE_SIZE);
        // pColData->nData += BSE_SEQUECE_SIZE;
      }

    } else {
      code = tRealloc((uint8_t **)(&pColData->aOffset), ((int64_t)(pColData->nVal + 1)) << 2);
      if (code) goto _exit;
      pColData->aOffset[pColData->nVal] = pColData->nData;

      if (nData) {
        code = tRealloc(&pColData->pData, pColData->nData + nData);
        if (code) goto _exit;
        (void)memcpy(pColData->pData + pColData->nData, pData, nData);
        pColData->nData += nData;
      }
    }
  } else {
    if (!(pColData->nData == tDataTypes[pColData->type].bytes * pColData->nVal)) {
      return TSDB_CODE_INVALID_PARA;
    }
    code = tRealloc(&pColData->pData, pColData->nData + tDataTypes[pColData->type].bytes);
    if (code) goto _exit;
    if (pData) {
      (void)memcpy(pColData->pData + pColData->nData, pData, TYPE_BYTES[pColData->type]);
    } else {
      memset(pColData->pData + pColData->nData, 0, TYPE_BYTES[pColData->type]);
    }
    pColData->nData += tDataTypes[pColData->type].bytes;
  }
  pColData->nVal++;

_exit:
  return code;
}
static FORCE_INLINE int32_t tColDataAppendValue00(SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->flag = HAS_VALUE;
  pColData->numOfValue++;
  return tColDataPutValue(pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValue01(SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->flag = HAS_NONE;
  pColData->numOfNone++;
  pColData->nVal++;
  return 0;
}
static FORCE_INLINE int32_t tColDataAppendValue02(SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->flag = HAS_NULL;
  pColData->numOfNull++;
  pColData->nVal++;
  return 0;
}
static FORCE_INLINE int32_t tColDataAppendValue10(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 0, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);

  pColData->flag |= HAS_VALUE;
  pColData->numOfValue++;

  if (pColData->nVal) {
    if (IS_VAR_DATA_TYPE(pColData->type)) {
      if (IS_STR_DATA_BLOB(pColData->type)) {
        int32_t nOffset = sizeof(int32_t) * pColData->nVal;
        code = tRealloc((uint8_t **)(&pColData->aOffset), nOffset);
        if (code) return code;
        memset(pColData->aOffset, 0, nOffset);

      } else {
        int32_t nOffset = sizeof(int32_t) * pColData->nVal;
        code = tRealloc((uint8_t **)(&pColData->aOffset), nOffset);
        if (code) return code;
        memset(pColData->aOffset, 0, nOffset);
      }
    } else {
      pColData->nData = tDataTypes[pColData->type].bytes * pColData->nVal;
      code = tRealloc(&pColData->pData, pColData->nData);
      if (code) return code;
      memset(pColData->pData, 0, pColData->nData);
    }
  }

  return tColDataPutValue(pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValue11(SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->nVal++;
  pColData->numOfNone++;
  return 0;
}
static FORCE_INLINE int32_t tColDataAppendValue12(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 0, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);

  pColData->flag |= HAS_NULL;
  pColData->numOfNull++;
  pColData->nVal++;

  return code;
}
static FORCE_INLINE int32_t tColDataAppendValue20(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 0, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);

  pColData->flag |= HAS_VALUE;
  pColData->numOfValue++;

  if (pColData->nVal) {
    if (IS_VAR_DATA_TYPE(pColData->type)) {
      int32_t nOffset = sizeof(int32_t) * pColData->nVal;
      code = tRealloc((uint8_t **)(&pColData->aOffset), nOffset);
      if (code) return code;
      if (!IS_STR_DATA_BLOB(pColData->type)) {
        memset(pColData->aOffset, 0, nOffset);
      }
    } else {
      pColData->nData = tDataTypes[pColData->type].bytes * pColData->nVal;
      code = tRealloc(&pColData->pData, pColData->nData);
      if (code) return code;
      memset(pColData->pData, 0, pColData->nData);
    }
  }

  return tColDataPutValue(pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValue21(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 255, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);

  pColData->flag |= HAS_NONE;
  pColData->numOfNone++;
  pColData->nVal++;

  return code;
}
static FORCE_INLINE int32_t tColDataAppendValue22(SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->nVal++;
  pColData->numOfNull++;
  return 0;
}
static FORCE_INLINE int32_t tColDataAppendValue30(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  pColData->flag |= HAS_VALUE;
  pColData->numOfValue++;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
    SET_BIT2_EX(pBitMap, iVal, GET_BIT1(pColData->pBitMap, iVal));
  }
  SET_BIT2_EX(pBitMap, pColData->nVal, 2);

  tFree(pColData->pBitMap);
  pColData->pBitMap = pBitMap;

  if (pColData->nVal) {
    if (IS_VAR_DATA_TYPE(pColData->type)) {
      int32_t nOffset = sizeof(int32_t) * pColData->nVal;
      code = tRealloc((uint8_t **)(&pColData->aOffset), nOffset);
      if (code) return code;
      memset(pColData->aOffset, 0, nOffset);
    } else {
      pColData->nData = tDataTypes[pColData->type].bytes * pColData->nVal;
      code = tRealloc(&pColData->pData, pColData->nData);
      if (code) return code;
      memset(pColData->pData, 0, pColData->nData);
    }
  }

  return tColDataPutValue(pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValue31(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);
  pColData->numOfNone++;
  pColData->nVal++;

  return code;
}
static FORCE_INLINE int32_t tColDataAppendValue32(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);
  pColData->numOfNull++;
  pColData->nVal++;

  return code;
}
static FORCE_INLINE int32_t tColDataAppendValue40(SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->numOfValue++;
  return tColDataPutValue(pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValue41(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  pColData->flag |= HAS_NONE;
  pColData->numOfNone++;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 255, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);

  return tColDataPutValue(pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValue42(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  pColData->flag |= HAS_NULL;
  pColData->numOfNull++;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 255, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);

  return tColDataPutValue(pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValue50(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);
  pColData->numOfValue++;

  return tColDataPutValue(pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValue51(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);
  pColData->numOfNone++;

  return tColDataPutValue(pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValue52(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  pColData->flag |= HAS_NULL;
  pColData->numOfNull++;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
    SET_BIT2_EX(pBitMap, iVal, GET_BIT1(pColData->pBitMap, iVal) ? 2 : 0);
  }
  SET_BIT2_EX(pBitMap, pColData->nVal, 1);

  tFree(pColData->pBitMap);
  pColData->pBitMap = pBitMap;

  return tColDataPutValue(pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValue60(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);
  pColData->numOfValue++;

  return tColDataPutValue(pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValue61(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  pColData->flag |= HAS_NONE;
  pColData->numOfNone++;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
    SET_BIT2_EX(pBitMap, iVal, GET_BIT1(pColData->pBitMap, iVal) ? 2 : 1);
  }
  SET_BIT2_EX(pBitMap, pColData->nVal, 0);

  tFree(pColData->pBitMap);
  pColData->pBitMap = pBitMap;

  return tColDataPutValue(pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValue62(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);
  pColData->numOfNull++;

  return tColDataPutValue(pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValue70(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT2_EX(pColData->pBitMap, pColData->nVal, 2);
  pColData->numOfValue++;

  return tColDataPutValue(pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValue71(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT2_EX(pColData->pBitMap, pColData->nVal, 0);
  pColData->numOfNone++;

  return tColDataPutValue(pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValue72(SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT2_EX(pColData->pBitMap, pColData->nVal, 1);
  pColData->numOfNull++;

  return tColDataPutValue(pColData, NULL, 0);
}
static int32_t (*tColDataAppendValueImpl[8][3])(SColData *pColData, uint8_t *pData, uint32_t nData) = {
    {tColDataAppendValue00, tColDataAppendValue01, tColDataAppendValue02},  // 0
    {tColDataAppendValue10, tColDataAppendValue11, tColDataAppendValue12},  // HAS_NONE
    {tColDataAppendValue20, tColDataAppendValue21, tColDataAppendValue22},  // HAS_NULL
    {tColDataAppendValue30, tColDataAppendValue31, tColDataAppendValue32},  // HAS_NULL|HAS_NONE
    {tColDataAppendValue40, tColDataAppendValue41, tColDataAppendValue42},  // HAS_VALUE
    {tColDataAppendValue50, tColDataAppendValue51, tColDataAppendValue52},  // HAS_VALUE|HAS_NONE
    {tColDataAppendValue60, tColDataAppendValue61, tColDataAppendValue62},  // HAS_VALUE|HAS_NULL
    {tColDataAppendValue70, tColDataAppendValue71, tColDataAppendValue72},  // HAS_VALUE|HAS_NULL|HAS_NONE

    //       VALUE                  NONE                     NULL
};

static FORCE_INLINE int32_t tColDataPutValueBlob(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t  code = 0;
  uint8_t  buf[sizeof(uint64_t) + 1] = {0};
  uint64_t seq = 0;

  int32_t offset = 0;
  if (IS_STR_DATA_BLOB(pColData->type)) {
    code = tRealloc((uint8_t **)(&pColData->aOffset), ((int64_t)(pColData->nVal + 1)) << 2);
    if (code) goto _exit;
    pColData->aOffset[pColData->nVal] = pColData->nData;
    if (nData) {
      SBlobItem item = {.seqOffsetInRow = seq, .len = nData, .data = pData, .type = TSDB_DATA_BLOB_VALUE};
      code = tBlobSetPush(pArg, &item, &seq, 0);
      if (code != 0) return code;

      offset = tPutU64(buf, seq);
      code = tRealloc(&pColData->pData, pColData->nData + offset);
      if (code != 0) return code;
      memcpy(pColData->pData + pColData->nData, buf, offset);
      pColData->nData += offset;
    } else {
      int8_t type = pData ? TSDB_DATA_BLOB_EMPTY_VALUE : TSDB_DATA_BLOB_NULL_VALUE;
      code = addEmptyItemToBlobSet(pArg, type, NULL);
    }
  }
  pColData->nVal++;

_exit:
  return code;
}

static FORCE_INLINE int32_t tColDataAppendValueBlob00(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->flag = HAS_VALUE;
  pColData->numOfValue++;
  return tColDataPutValueBlob(pArg, pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob01(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->flag = HAS_NONE;
  pColData->numOfNone++;
  pColData->nVal++;
  return addEmptyItemToBlobSet(pArg, TSDB_DATA_BLOB_NULL_VALUE, NULL);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob02(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->flag = HAS_NULL;
  pColData->numOfNull++;
  pColData->nVal++;
  return addEmptyItemToBlobSet(pArg, TSDB_DATA_BLOB_NULL_VALUE, NULL);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob10(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 0, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);

  pColData->flag |= HAS_VALUE;
  pColData->numOfValue++;

  if (pColData->nVal) {
    if (IS_VAR_DATA_TYPE(pColData->type)) {
      int32_t nOffset = sizeof(int32_t) * pColData->nVal;
      code = tRealloc((uint8_t **)(&pColData->aOffset), nOffset);
      if (code) return code;
      memset(pColData->aOffset, 0, nOffset);
    } else {
      pColData->nData = tDataTypes[pColData->type].bytes * pColData->nVal;
      code = tRealloc(&pColData->pData, pColData->nData);
      if (code) return code;
      memset(pColData->pData, 0, pColData->nData);
    }
  }

  return tColDataPutValueBlob(pArg, pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob11(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->nVal++;
  pColData->numOfNone++;
  return addEmptyItemToBlobSet(pArg, TSDB_DATA_BLOB_NULL_VALUE, NULL);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob12(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 0, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);

  pColData->flag |= HAS_NULL;
  pColData->numOfNull++;
  pColData->nVal++;
  return addEmptyItemToBlobSet(pArg, TSDB_DATA_BLOB_NULL_VALUE, NULL);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob20(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 0, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);

  pColData->flag |= HAS_VALUE;
  pColData->numOfValue++;

  if (pColData->nVal) {
    if (IS_STR_DATA_BLOB(pColData->type)) {
      int32_t nOffset = sizeof(int32_t) * pColData->nVal;
      code = tRealloc((uint8_t **)(&pColData->aOffset), nOffset);
      if (code) return code;
      memset(pColData->aOffset, 0, nOffset);
    } else {
      return TSDB_CODE_INVALID_MSG;
    }
  }

  return tColDataPutValueBlob(pArg, pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob21(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 255, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);

  pColData->flag |= HAS_NONE;
  pColData->numOfNone++;
  pColData->nVal++;
  return addEmptyItemToBlobSet(pArg, TSDB_DATA_BLOB_NULL_VALUE, NULL);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob22(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->nVal++;
  pColData->numOfNull++;
  return addEmptyItemToBlobSet(pArg, TSDB_DATA_BLOB_NULL_VALUE, NULL);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob30(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  pColData->flag |= HAS_VALUE;
  pColData->numOfValue++;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
    SET_BIT2_EX(pBitMap, iVal, GET_BIT1(pColData->pBitMap, iVal));
  }
  SET_BIT2_EX(pBitMap, pColData->nVal, 2);

  tFree(pColData->pBitMap);
  pColData->pBitMap = pBitMap;

  if (pColData->nVal) {
    if (IS_STR_DATA_BLOB(pColData->type)) {
      int32_t nOffset = sizeof(int32_t) * pColData->nVal;
      code = tRealloc((uint8_t **)(&pColData->aOffset), nOffset);
      if (code) return code;
      memset(pColData->aOffset, 0, nOffset);
    } else {
      return TSDB_CODE_INVALID_MSG;
    }
  }

  return tColDataPutValueBlob(pArg, pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob31(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);
  pColData->numOfNone++;
  pColData->nVal++;

  return addEmptyItemToBlobSet(pArg, TSDB_DATA_BLOB_NULL_VALUE, NULL);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob32(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);
  pColData->numOfNull++;
  pColData->nVal++;
  return addEmptyItemToBlobSet(pArg, TSDB_DATA_BLOB_NULL_VALUE, NULL);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob40(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  pColData->numOfValue++;
  return tColDataPutValueBlob(pArg, pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob41(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  pColData->flag |= HAS_NONE;
  pColData->numOfNone++;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 255, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);

  return tColDataPutValueBlob(pArg, pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob42(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  pColData->flag |= HAS_NULL;
  pColData->numOfNull++;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 255, nBit);
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);

  return tColDataPutValueBlob(pArg, pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob50(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);
  pColData->numOfValue++;

  return tColDataPutValueBlob(pArg, pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob51(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);
  pColData->numOfNone++;

  return tColDataPutValueBlob(pArg, pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob52(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  pColData->flag |= HAS_NULL;
  pColData->numOfNull++;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
    SET_BIT2_EX(pBitMap, iVal, GET_BIT1(pColData->pBitMap, iVal) ? 2 : 0);
  }
  SET_BIT2_EX(pBitMap, pColData->nVal, 1);

  tFree(pColData->pBitMap);
  pColData->pBitMap = pBitMap;

  return tColDataPutValueBlob(pArg, pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob60(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 1);
  pColData->numOfValue++;

  return tColDataPutValueBlob(pArg, pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob61(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  pColData->flag |= HAS_NONE;
  pColData->numOfNone++;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
    SET_BIT2_EX(pBitMap, iVal, GET_BIT1(pColData->pBitMap, iVal) ? 2 : 1);
  }
  SET_BIT2_EX(pBitMap, pColData->nVal, 0);

  tFree(pColData->pBitMap);
  pColData->pBitMap = pBitMap;

  return tColDataPutValueBlob(pArg, pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob62(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT1_EX(pColData->pBitMap, pColData->nVal, 0);
  pColData->numOfNull++;

  return tColDataPutValueBlob(pArg, pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob70(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT2_EX(pColData->pBitMap, pColData->nVal, 2);
  pColData->numOfValue++;

  return tColDataPutValueBlob(pArg, pColData, pData, nData);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob71(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT2_EX(pColData->pBitMap, pColData->nVal, 0);
  pColData->numOfNone++;

  return tColDataPutValueBlob(pArg, pColData, NULL, 0);
}
static FORCE_INLINE int32_t tColDataAppendValueBlob72(void *pArg, SColData *pColData, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT2_EX(pColData->pBitMap, pColData->nVal, 1);
  pColData->numOfNull++;

  return tColDataPutValueBlob(pArg, pColData, NULL, 0);
}

static int32_t (*tColDataAppendValueBlobImpl[8][3])(void *pDst, SColData *pColData, uint8_t *pData, uint32_t nData) = {
    {tColDataAppendValueBlob00, tColDataAppendValueBlob01, tColDataAppendValueBlob02},  // 0
    {tColDataAppendValueBlob10, tColDataAppendValueBlob11, tColDataAppendValueBlob12},  // HAS_NONE
    {tColDataAppendValueBlob20, tColDataAppendValueBlob21, tColDataAppendValueBlob22},  // HAS_NULL
    {tColDataAppendValueBlob30, tColDataAppendValueBlob31, tColDataAppendValueBlob32},  // HAS_NULL|HAS_NONE
    {tColDataAppendValueBlob40, tColDataAppendValueBlob41, tColDataAppendValueBlob42},  // HAS_VALUE
    {tColDataAppendValueBlob50, tColDataAppendValueBlob51, tColDataAppendValueBlob52},  // HAS_VALUE|HAS_NONE
    {tColDataAppendValueBlob60, tColDataAppendValueBlob61, tColDataAppendValueBlob62},  // HAS_VALUE|HAS_NULL
    {tColDataAppendValueBlob70, tColDataAppendValueBlob71, tColDataAppendValueBlob72}   // HAS_VALUE|HAS_NULL|HAS_NONE
    //       VALUE                  NONE                     NULL
};
int32_t tColDataAppendValue(SColData *pColData, SColVal *pColVal) {
  if (!(pColData->cid == pColVal->cid && pColData->type == pColVal->value.type)) {
    return TSDB_CODE_INVALID_PARA;
  }
  return tColDataAppendValueImpl[pColData->flag][pColVal->flag](
      pColData, VALUE_GET_DATUM(&pColVal->value, pColData->type), pColVal->value.nData);
}

static FORCE_INLINE int32_t tColDataUpdateValue10(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  pColData->numOfNone--;
  pColData->nVal--;
  if (pColData->numOfNone) {
    return tColDataAppendValue10(pColData, pData, nData);
  } else {
    pColData->flag = 0;
    return tColDataAppendValue00(pColData, pData, nData);
  }
}
static FORCE_INLINE int32_t tColDataUpdateValue12(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  pColData->numOfNone--;
  pColData->nVal--;
  if (pColData->numOfNone) {
    return tColDataAppendValue12(pColData, pData, nData);
  } else {
    pColData->flag = 0;
    return tColDataAppendValue02(pColData, pData, nData);
  }
  return 0;
}
static FORCE_INLINE int32_t tColDataUpdateValue20(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  if (forward) {
    pColData->numOfNull--;
    pColData->nVal--;
    if (pColData->numOfNull) {
      return tColDataAppendValue20(pColData, pData, nData);
    } else {
      pColData->flag = 0;
      return tColDataAppendValue00(pColData, pData, nData);
    }
  }
  return 0;
}
static FORCE_INLINE int32_t tColDataUpdateValue30(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  if (GET_BIT1(pColData->pBitMap, pColData->nVal - 1) == 0) {  // NONE ==> VALUE
    pColData->numOfNone--;
    pColData->nVal--;
    if (pColData->numOfNone) {
      return tColDataAppendValue30(pColData, pData, nData);
    } else {
      pColData->flag = HAS_NULL;
      return tColDataAppendValue20(pColData, pData, nData);
    }
  } else if (forward) {  // NULL ==> VALUE
    pColData->numOfNull--;
    pColData->nVal--;
    if (pColData->numOfNull) {
      return tColDataAppendValue30(pColData, pData, nData);
    } else {
      pColData->flag = HAS_NONE;
      return tColDataAppendValue10(pColData, pData, nData);
    }
  }
  return 0;
}
static FORCE_INLINE int32_t tColDataUpdateValue32(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  if (GET_BIT1(pColData->pBitMap, pColData->nVal - 1) == 0) {  // NONE ==> NULL
    pColData->numOfNone--;
    pColData->numOfNull++;
    if (pColData->numOfNone) {
      SET_BIT1(pColData->pBitMap, pColData->nVal - 1, 1);
    } else {
      pColData->flag = HAS_NULL;
    }
  }
  return 0;
}
static FORCE_INLINE int32_t tColDataUpdateValue40(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  if (forward) {  // VALUE ==> VALUE
    pColData->nVal--;
    if (IS_VAR_DATA_TYPE(pColData->type)) {
      pColData->nData = pColData->aOffset[pColData->nVal];
    } else {
      pColData->nData -= TYPE_BYTES[pColData->type];
    }
    return tColDataPutValue(pColData, pData, nData);
  }
  return 0;
}
static FORCE_INLINE int32_t tColDataUpdateValue42(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  if (forward) {  // VALUE ==> NULL
    pColData->numOfValue--;
    pColData->nVal--;
    if (pColData->numOfValue) {
      if (IS_VAR_DATA_TYPE(pColData->type)) {
        pColData->nData = pColData->aOffset[pColData->nVal];
      } else {
        pColData->nData -= TYPE_BYTES[pColData->type];
      }
      return tColDataAppendValue42(pColData, pData, nData);
    } else {
      pColData->flag = 0;
      pColData->nData = 0;
      return tColDataAppendValue02(pColData, pData, nData);
    }
  }
  return 0;
}
static FORCE_INLINE int32_t tColDataUpdateValue50(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  if (GET_BIT1(pColData->pBitMap, pColData->nVal - 1) == 0) {  // NONE ==> VALUE
    pColData->numOfNone--;
    pColData->nVal--;
    if (!IS_VAR_DATA_TYPE(pColData->type)) {
      pColData->nData -= TYPE_BYTES[pColData->type];
    }
    if (pColData->numOfNone) {
      return tColDataAppendValue50(pColData, pData, nData);
    } else {
      pColData->flag = HAS_VALUE;
      return tColDataAppendValue40(pColData, pData, nData);
    }
  } else if (forward) {  // VALUE ==> VALUE
    pColData->nVal--;
    if (IS_VAR_DATA_TYPE(pColData->type)) {
      pColData->nData = pColData->aOffset[pColData->nVal];
    } else {
      pColData->nData -= TYPE_BYTES[pColData->type];
    }
    return tColDataPutValue(pColData, pData, nData);
  }
  return 0;
}
static FORCE_INLINE int32_t tColDataUpdateValue52(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  if (GET_BIT1(pColData->pBitMap, pColData->nVal - 1) == 0) {  // NONE ==> NULL
    pColData->numOfNone--;
    pColData->nVal--;
    if (!IS_VAR_DATA_TYPE(pColData->type)) {
      pColData->nData -= TYPE_BYTES[pColData->type];
    }
    if (pColData->numOfNone) {
      return tColDataAppendValue52(pColData, pData, nData);
    } else {
      pColData->flag = HAS_VALUE;
      return tColDataAppendValue42(pColData, pData, nData);
    }
  } else if (forward) {  // VALUE ==> NULL
    pColData->numOfValue--;
    pColData->nVal--;
    if (pColData->numOfValue) {
      if (IS_VAR_DATA_TYPE(pColData->type)) {
        pColData->nData = pColData->aOffset[pColData->nVal];
      } else {
        pColData->nData -= TYPE_BYTES[pColData->type];
      }
      return tColDataAppendValue52(pColData, pData, nData);
    } else {
      pColData->flag = HAS_NONE;
      pColData->nData = 0;
      return tColDataAppendValue12(pColData, pData, nData);
    }
  }
  return 0;
}
static FORCE_INLINE int32_t tColDataUpdateValue60(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  if (forward) {
    if (GET_BIT1(pColData->pBitMap, pColData->nVal - 1) == 0) {  // NULL ==> VALUE
      pColData->numOfNull--;
      pColData->nVal--;
      if (!IS_VAR_DATA_TYPE(pColData->type)) {
        pColData->nData -= TYPE_BYTES[pColData->type];
      }
      if (pColData->numOfNull) {
        return tColDataAppendValue60(pColData, pData, nData);
      } else {
        pColData->flag = HAS_VALUE;
        return tColDataAppendValue40(pColData, pData, nData);
      }
    } else {  // VALUE ==> VALUE
      pColData->nVal--;
      if (IS_VAR_DATA_TYPE(pColData->type)) {
        pColData->nData = pColData->aOffset[pColData->nVal];
      } else {
        pColData->nData -= TYPE_BYTES[pColData->type];
      }
      return tColDataPutValue(pColData, pData, nData);
    }
  }
  return 0;
}
static FORCE_INLINE int32_t tColDataUpdateValue62(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  if (forward && (GET_BIT1(pColData->pBitMap, pColData->nVal - 1) == 1)) {  // VALUE ==> NULL
    pColData->numOfValue--;
    pColData->nVal--;
    if (pColData->numOfValue) {
      if (IS_VAR_DATA_TYPE(pColData->type)) {
        pColData->nData = pColData->aOffset[pColData->nVal];
      } else {
        pColData->nData -= TYPE_BYTES[pColData->type];
      }
      return tColDataAppendValue62(pColData, pData, nData);
    } else {
      pColData->flag = HAS_NULL;
      pColData->nData = 0;
      return tColDataAppendValue20(pColData, pData, nData);
    }
  }
  return 0;
}
static FORCE_INLINE int32_t tColDataUpdateValue70(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  int32_t code = 0;

  uint8_t bv = GET_BIT2(pColData->pBitMap, pColData->nVal - 1);
  if (bv == 0) {  // NONE ==> VALUE
    pColData->numOfNone--;
    pColData->nVal--;
    if (!IS_VAR_DATA_TYPE(pColData->type)) {
      pColData->nData -= TYPE_BYTES[pColData->type];
    }
    if (pColData->numOfNone) {
      return tColDataAppendValue70(pColData, pData, nData);
    } else {
      for (int32_t iVal = 0; iVal < pColData->nVal; ++iVal) {
        SET_BIT1(pColData->pBitMap, iVal, GET_BIT2(pColData->pBitMap, iVal) - 1);
      }
      pColData->flag = (HAS_VALUE | HAS_NULL);
      return tColDataAppendValue60(pColData, pData, nData);
    }
  } else if (bv == 1) {  // NULL ==> VALUE
    if (forward) {
      pColData->numOfNull--;
      pColData->nVal--;
      if (!IS_VAR_DATA_TYPE(pColData->type)) {
        pColData->nData -= TYPE_BYTES[pColData->type];
      }
      if (pColData->numOfNull) {
        return tColDataAppendValue70(pColData, pData, nData);
      } else {
        for (int32_t iVal = 0; iVal < pColData->nVal; ++iVal) {
          SET_BIT1(pColData->pBitMap, iVal, GET_BIT2(pColData->pBitMap, iVal) ? 1 : 0);
        }
        pColData->flag = (HAS_VALUE | HAS_NONE);
        return tColDataAppendValue50(pColData, pData, nData);
      }
    }
  } else if (bv == 2) {  // VALUE ==> VALUE
    if (forward) {
      pColData->nVal--;
      if (IS_VAR_DATA_TYPE(pColData->type)) {
        pColData->nData = pColData->aOffset[pColData->nVal];
      } else {
        pColData->nData -= TYPE_BYTES[pColData->type];
      }
      return tColDataPutValue(pColData, pData, nData);
    }
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
  return 0;
}
static int32_t tColDataUpdateValue72(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  uint8_t bv = GET_BIT2(pColData->pBitMap, pColData->nVal - 1);
  if (bv == 0) {  // NONE ==> NULL
    pColData->numOfNone--;
    pColData->nVal--;
    if (!IS_VAR_DATA_TYPE(pColData->type)) {
      pColData->nData -= TYPE_BYTES[pColData->type];
    }
    if (pColData->numOfNone) {
      return tColDataAppendValue72(pColData, pData, nData);
    } else {
      for (int32_t iVal = 0; iVal < pColData->nVal; ++iVal) {
        SET_BIT1(pColData->pBitMap, iVal, GET_BIT2(pColData->pBitMap, iVal) - 1);
      }
      pColData->flag = (HAS_VALUE | HAS_NULL);
      return tColDataAppendValue62(pColData, pData, nData);
    }
  } else if (bv == 2 && forward) {  // VALUE ==> NULL
    pColData->numOfValue--;
    pColData->nVal--;
    if (pColData->numOfValue) {
      if (IS_VAR_DATA_TYPE(pColData->type)) {
        pColData->nData = pColData->aOffset[pColData->nVal];
      } else {
        pColData->nData -= TYPE_BYTES[pColData->type];
      }
      return tColDataAppendValue72(pColData, pData, nData);
    } else {
      for (int32_t iVal = 0; iVal < pColData->nVal; ++iVal) {
        SET_BIT1(pColData->pBitMap, iVal, GET_BIT2(pColData->pBitMap, iVal));
      }
      pColData->flag = (HAS_NULL | HAS_NONE);
      pColData->nData = 0;
      return tColDataAppendValue32(pColData, pData, nData);
    }
  }
  return 0;
}
static FORCE_INLINE int32_t tColDataUpdateNothing(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) {
  return 0;
}
static int32_t (*tColDataUpdateValueImpl[8][3])(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward) = {
    {NULL, NULL, NULL},                                                     // 0
    {tColDataUpdateValue10, tColDataUpdateNothing, tColDataUpdateValue12},  // HAS_NONE
    {tColDataUpdateValue20, tColDataUpdateNothing, tColDataUpdateNothing},  // HAS_NULL
    {tColDataUpdateValue30, tColDataUpdateNothing, tColDataUpdateValue32},  // HAS_NULL|HAS_NONE
    {tColDataUpdateValue40, tColDataUpdateNothing, tColDataUpdateValue42},  // HAS_VALUE
    {tColDataUpdateValue50, tColDataUpdateNothing, tColDataUpdateValue52},  // HAS_VALUE|HAS_NONE
    {tColDataUpdateValue60, tColDataUpdateNothing, tColDataUpdateValue62},  // HAS_VALUE|HAS_NULL
    {tColDataUpdateValue70, tColDataUpdateNothing, tColDataUpdateValue72},  // HAS_VALUE|HAS_NULL|HAS_NONE

    //    VALUE             NONE        NULL
};
int32_t tColDataUpdateValue(SColData *pColData, SColVal *pColVal, bool forward) {
  if (!(pColData->cid == pColVal->cid && pColData->type == pColVal->value.type)) return TSDB_CODE_INVALID_PARA;
  if (!(pColData->nVal > 0)) return TSDB_CODE_INVALID_PARA;

  if (tColDataUpdateValueImpl[pColData->flag][pColVal->flag] == NULL) return 0;

  return tColDataUpdateValueImpl[pColData->flag][pColVal->flag](
      pColData, VALUE_GET_DATUM(&pColVal->value, pColData->type), pColVal->value.nData, forward);
}

static FORCE_INLINE void tColDataGetValue1(SColData *pColData, int32_t iVal, SColVal *pColVal) {  // HAS_NONE
  *pColVal = COL_VAL_NONE(pColData->cid, pColData->type);
}
static FORCE_INLINE void tColDataGetValue2(SColData *pColData, int32_t iVal, SColVal *pColVal) {  // HAS_NULL
  *pColVal = COL_VAL_NULL(pColData->cid, pColData->type);
}
static FORCE_INLINE void tColDataGetValue3(SColData *pColData, int32_t iVal,
                                           SColVal *pColVal) {  // HAS_NULL|HAS_NONE
  switch (GET_BIT1(pColData->pBitMap, iVal)) {
    case 0:
      *pColVal = COL_VAL_NONE(pColData->cid, pColData->type);
      break;
    case 1:
      *pColVal = COL_VAL_NULL(pColData->cid, pColData->type);
      break;
    default:
      break;
  }
}
static FORCE_INLINE void tColDataGetValue4(SColData *pColData, int32_t iVal, SColVal *pColVal) {  // HAS_VALUE
  SValue value = {.type = pColData->type};
  if (IS_VAR_DATA_TYPE(pColData->type)) {
    if (iVal + 1 < pColData->nVal) {
      value.nData = pColData->aOffset[iVal + 1] - pColData->aOffset[iVal];
    } else {
      value.nData = pColData->nData - pColData->aOffset[iVal];
    }
    value.pData = pColData->pData + pColData->aOffset[iVal];
  } else {
    valueSetDatum(&value, pColData->type, pColData->pData + tDataTypes[pColData->type].bytes * iVal,
                  tDataTypes[pColData->type].bytes);
  }
  *pColVal = COL_VAL_VALUE(pColData->cid, value);
}
static FORCE_INLINE void tColDataGetValue5(SColData *pColData, int32_t iVal,
                                           SColVal *pColVal) {  // HAS_VALUE|HAS_NONE
  switch (GET_BIT1(pColData->pBitMap, iVal)) {
    case 0:
      *pColVal = COL_VAL_NONE(pColData->cid, pColData->type);
      break;
    case 1:
      tColDataGetValue4(pColData, iVal, pColVal);
      break;
    default:
      break;
  }
}
static FORCE_INLINE void tColDataGetValue6(SColData *pColData, int32_t iVal,
                                           SColVal *pColVal) {  // HAS_VALUE|HAS_NULL
  switch (GET_BIT1(pColData->pBitMap, iVal)) {
    case 0:
      *pColVal = COL_VAL_NULL(pColData->cid, pColData->type);
      break;
    case 1:
      tColDataGetValue4(pColData, iVal, pColVal);
      break;
    default:
      break;
  }
}
static FORCE_INLINE void tColDataGetValue7(SColData *pColData, int32_t iVal,
                                           SColVal *pColVal) {  // HAS_VALUE|HAS_NULL|HAS_NONE
  switch (GET_BIT2(pColData->pBitMap, iVal)) {
    case 0:
      *pColVal = COL_VAL_NONE(pColData->cid, pColData->type);
      break;
    case 1:
      *pColVal = COL_VAL_NULL(pColData->cid, pColData->type);
      break;
    case 2:
      tColDataGetValue4(pColData, iVal, pColVal);
      break;
    default:
      break;
  }
}
static void (*tColDataGetValueImpl[])(SColData *pColData, int32_t iVal, SColVal *pColVal) = {
    NULL,               // 0
    tColDataGetValue1,  // HAS_NONE
    tColDataGetValue2,  // HAS_NULL
    tColDataGetValue3,  // HAS_NULL | HAS_NONE
    tColDataGetValue4,  // HAS_VALUE
    tColDataGetValue5,  // HAS_VALUE | HAS_NONE
    tColDataGetValue6,  // HAS_VALUE | HAS_NULL
    tColDataGetValue7   // HAS_VALUE | HAS_NULL | HAS_NONE
};
int32_t tColDataGetValue(SColData *pColData, int32_t iVal, SColVal *pColVal) {
  if (iVal < 0 || iVal >= pColData->nVal ||
      (pColData->flag <= 0 || pColData->flag >= sizeof(tColDataGetValueImpl) / POINTER_BYTES)) {
    return TSDB_CODE_INVALID_PARA;
  }
  tColDataGetValueImpl[pColData->flag](pColData, iVal, pColVal);
  return TSDB_CODE_SUCCESS;
}

uint8_t tColDataGetBitValue(const SColData *pColData, int32_t iVal) {
  switch (pColData->flag) {
    case HAS_NONE:
      return 0;
    case HAS_NULL:
      return 1;
    case (HAS_NULL | HAS_NONE):
      return GET_BIT1(pColData->pBitMap, iVal);
    case HAS_VALUE:
      return 2;
    case (HAS_VALUE | HAS_NONE):
      return (GET_BIT1(pColData->pBitMap, iVal)) ? 2 : 0;
    case (HAS_VALUE | HAS_NULL):
      return GET_BIT1(pColData->pBitMap, iVal) + 1;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      return GET_BIT2(pColData->pBitMap, iVal);
    default:
      return 0;
  }
}

int32_t tColDataCopy(SColData *pColDataFrom, SColData *pColData, xMallocFn xMalloc, void *arg) {
  int32_t code = 0;

  *pColData = *pColDataFrom;

  // bitmap
  switch (pColData->flag) {
    case (HAS_NULL | HAS_NONE):
    case (HAS_VALUE | HAS_NONE):
    case (HAS_VALUE | HAS_NULL):
      pColData->pBitMap = xMalloc(arg, BIT1_SIZE(pColData->nVal));
      if (pColData->pBitMap == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
      (void)memcpy(pColData->pBitMap, pColDataFrom->pBitMap, BIT1_SIZE(pColData->nVal));
      break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      pColData->pBitMap = xMalloc(arg, BIT2_SIZE(pColData->nVal));
      if (pColData->pBitMap == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
      (void)memcpy(pColData->pBitMap, pColDataFrom->pBitMap, BIT2_SIZE(pColData->nVal));
      break;
    default:
      pColData->pBitMap = NULL;
      break;
  }

  // offset
  if (IS_VAR_DATA_TYPE(pColData->type) && (pColData->flag & HAS_VALUE)) {
    pColData->aOffset = xMalloc(arg, pColData->nVal << 2);
    if (pColData->aOffset == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    (void)memcpy(pColData->aOffset, pColDataFrom->aOffset, pColData->nVal << 2);
  } else {
    pColData->aOffset = NULL;
  }

  // value
  if (pColData->nData) {
    pColData->pData = xMalloc(arg, pColData->nData);
    if (pColData->pData == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    (void)memcpy(pColData->pData, pColDataFrom->pData, pColData->nData);
  } else {
    pColData->pData = NULL;
  }

_exit:
  return code;
}

int32_t tColDataCompress(SColData *colData, SColDataCompressInfo *info, SBuffer *output, SBuffer *assist) {
  int32_t code;
  SBuffer local;

  if (!(colData->nVal > 0)) {
    return TSDB_CODE_INVALID_PARA;
  }

  (*info) = (SColDataCompressInfo){
      .cmprAlg = info->cmprAlg,
      .columnFlag = colData->cflag,
      .flag = colData->flag,
      .dataType = colData->type,
      .columnId = colData->cid,
      .numOfData = colData->nVal,
  };

  if (colData->flag == HAS_NONE || colData->flag == HAS_NULL) {
    return 0;
  }

  tBufferInit(&local);
  if (assist == NULL) {
    assist = &local;
  }

  // bitmap
  if (colData->flag != HAS_VALUE) {
    if (colData->flag == (HAS_NONE | HAS_NULL | HAS_VALUE)) {
      info->bitmapOriginalSize = BIT2_SIZE(colData->nVal);
    } else {
      info->bitmapOriginalSize = BIT1_SIZE(colData->nVal);
    }

    SCompressInfo cinfo = {
        .dataType = TSDB_DATA_TYPE_TINYINT,
        .cmprAlg = info->cmprAlg,
        .originalSize = info->bitmapOriginalSize,
    };

    code = tCompressDataToBuffer(colData->pBitMap, &cinfo, output, assist);
    if (code) {
      tBufferDestroy(&local);
      return code;
    }

    info->bitmapCompressedSize = cinfo.compressedSize;
  }

  if (colData->flag == (HAS_NONE | HAS_NULL)) {
    tBufferDestroy(&local);
    return 0;
  }

  // offset
  if (IS_VAR_DATA_TYPE(colData->type)) {
    info->offsetOriginalSize = sizeof(int32_t) * info->numOfData;

    SCompressInfo cinfo = {
        .dataType = TSDB_DATA_TYPE_INT,
        .cmprAlg = info->cmprAlg,
        .originalSize = info->offsetOriginalSize,
    };

    code = tCompressDataToBuffer(colData->aOffset, &cinfo, output, assist);
    if (code) {
      tBufferDestroy(&local);
      return code;
    }

    info->offsetCompressedSize = cinfo.compressedSize;
  }

  // data
  if (colData->nData > 0) {
    info->dataOriginalSize = colData->nData;

    SCompressInfo cinfo = {
        .dataType = colData->type,
        .cmprAlg = info->cmprAlg,
        .originalSize = info->dataOriginalSize,
    };

    code = tCompressDataToBuffer(colData->pData, &cinfo, output, assist);
    if (code) {
      tBufferDestroy(&local);
      return code;
    }

    info->dataCompressedSize = cinfo.compressedSize;
  }

  tBufferDestroy(&local);
  return 0;
}

int32_t tColDataDecompress(void *input, SColDataCompressInfo *info, SColData *colData, SBuffer *assist) {
  int32_t  code;
  SBuffer  local;
  uint8_t *data = (uint8_t *)input;

  tBufferInit(&local);
  if (assist == NULL) {
    assist = &local;
  }

  tColDataClear(colData);
  colData->cid = info->columnId;
  colData->type = info->dataType;
  colData->cflag = info->columnFlag;
  colData->nVal = info->numOfData;
  colData->flag = info->flag;

  if (info->flag == HAS_NONE || info->flag == HAS_NULL) {
    goto _exit;
  }

  // bitmap
  if (info->bitmapOriginalSize > 0) {
    SCompressInfo cinfo = {
        .dataType = TSDB_DATA_TYPE_TINYINT,
        .cmprAlg = info->cmprAlg,
        .originalSize = info->bitmapOriginalSize,
        .compressedSize = info->bitmapCompressedSize,
    };

    code = tRealloc(&colData->pBitMap, cinfo.originalSize);
    if (code) {
      tBufferDestroy(&local);
      return code;
    }

    code = tDecompressData(data, &cinfo, colData->pBitMap, cinfo.originalSize, assist);
    if (code) {
      tBufferDestroy(&local);
      return code;
    }

    data += cinfo.compressedSize;
  }

  if (info->flag == (HAS_NONE | HAS_NULL)) {
    goto _exit;
  }

  // offset
  if (info->offsetOriginalSize > 0) {
    SCompressInfo cinfo = {
        .cmprAlg = info->cmprAlg,
        .dataType = TSDB_DATA_TYPE_INT,
        .originalSize = info->offsetOriginalSize,
        .compressedSize = info->offsetCompressedSize,
    };

    code = tRealloc((uint8_t **)&colData->aOffset, cinfo.originalSize);
    if (code) {
      tBufferDestroy(&local);
      return code;
    }

    code = tDecompressData(data, &cinfo, colData->aOffset, cinfo.originalSize, assist);
    if (code) {
      tBufferDestroy(&local);
      return code;
    }

    data += cinfo.compressedSize;
  }

  // data
  if (info->dataOriginalSize > 0) {
    colData->nData = info->dataOriginalSize;

    SCompressInfo cinfo = {
        .cmprAlg = info->cmprAlg,
        .dataType = colData->type,
        .originalSize = info->dataOriginalSize,
        .compressedSize = info->dataCompressedSize,
    };

    code = tRealloc((uint8_t **)&colData->pData, cinfo.originalSize);
    if (code) {
      tBufferDestroy(&local);
      return code;
    }

    code = tDecompressData(data, &cinfo, colData->pData, cinfo.originalSize, assist);
    if (code) {
      tBufferDestroy(&local);
      return code;
    }

    data += cinfo.compressedSize;
  }

_exit:
  switch (colData->flag) {
    case HAS_NONE:
      colData->numOfNone = colData->nVal;
      break;
    case HAS_NULL:
      colData->numOfNull = colData->nVal;
      break;
    case HAS_VALUE:
      colData->numOfValue = colData->nVal;
      break;
    default:
      for (int32_t i = 0; i < colData->nVal; i++) {
        uint8_t bitValue = tColDataGetBitValue(colData, i);
        if (bitValue == 0) {
          colData->numOfNone++;
        } else if (bitValue == 1) {
          colData->numOfNull++;
        } else {
          colData->numOfValue++;
        }
      }
  }
  tBufferDestroy(&local);
  return 0;
}

int32_t tColDataAddValueByDataBlock(SColData *pColData, int8_t type, int32_t bytes, int32_t nRows, char *lengthOrbitmap,
                                    char *data) {
  int32_t code = 0;
  if (data == NULL) {
    if (pColData->cflag & COL_IS_KEY) {
      code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
    } else {
      for (int32_t i = 0; i < nRows; ++i) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0);
      }
    }
    goto _exit;
  }

  if (IS_VAR_DATA_TYPE(type)) {  // var-length data type
    if (!IS_STR_DATA_BLOB(type)) {
      for (int32_t i = 0; i < nRows; ++i) {
        int32_t offset = *((int32_t *)lengthOrbitmap + i);
        if (offset == -1) {
          if (pColData->cflag & COL_IS_KEY) {
            code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
            goto _exit;
          }
          if ((code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0))) {
            goto _exit;
          }
        } else {
          if (varDataTLen(data + offset) > bytes) {
            uError("var data length invalid, varDataTLen(data + offset):%d > bytes:%d", (int)varDataTLen(data + offset),
                   bytes);
            code = TSDB_CODE_PAR_VALUE_TOO_LONG;
            goto _exit;
          }
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, (uint8_t *)varDataVal(data + offset),
                                                                        varDataLen(data + offset));
        }
      }
    } else {
      for (int32_t i = 0; i < nRows; ++i) {
        int32_t offset = *((int32_t *)lengthOrbitmap + i);
        if (offset == -1) {
          if (pColData->cflag & COL_IS_KEY) {
            code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
            goto _exit;
          }
          if ((code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0))) {
            goto _exit;
          }
        } else {
          if (blobDataTLen(data + offset) > TSDB_MAX_BLOB_LEN) {
            uError("var data length invalid, varDataTLen(data + offset):%d > bytes:%d",
                   (int)blobDataTLen(data + offset), bytes);
            code = TSDB_CODE_PAR_VALUE_TOO_LONG;
            goto _exit;
          }
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, (uint8_t *)blobDataVal(data + offset),
                                                                        blobDataLen(data + offset));
        }
      }
    }
  } else {  // fixed-length data type
    bool allValue = true;
    bool allNull = true;
    for (int32_t i = 0; i < nRows; ++i) {
      if (!BMIsNull(lengthOrbitmap, i)) {
        allNull = false;
      } else {
        allValue = false;
      }
    }
    if ((pColData->cflag & COL_IS_KEY) && !allValue) {
      code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
      goto _exit;
    }

    if (allValue) {
      // optimize (todo)
      for (int32_t i = 0; i < nRows; ++i) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, (uint8_t *)data + bytes * i, bytes);
      }
    } else if (allNull) {
      // optimize (todo)
      for (int32_t i = 0; i < nRows; ++i) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
        if (code) goto _exit;
      }
    } else {
      for (int32_t i = 0; i < nRows; ++i) {
        if (BMIsNull(lengthOrbitmap, i)) {
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
          if (code) goto _exit;
        } else {
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, (uint8_t *)data + bytes * i, bytes);
        }
      }
    }
  }

_exit:
  return code;
}
int32_t tColDataAddValueByDataBlockWithBlob(SColData *pColData, int8_t type, int32_t bytes, int32_t nRows,
                                            char *lengthOrbitmap, char *data, void *pBlobSet) {
  int32_t code = 0;
  if (data == NULL) {
    if (pColData->cflag & COL_IS_KEY) {
      code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
    } else {
      for (int32_t i = 0; i < nRows; ++i) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0);
      }
    }
    goto _exit;
  }

  if (IS_VAR_DATA_TYPE(type)) {  // var-length data type
    if (!IS_STR_DATA_BLOB(type)) {
      for (int32_t i = 0; i < nRows; ++i) {
        int32_t offset = *((int32_t *)lengthOrbitmap + i);
        if (offset == -1) {
          if (pColData->cflag & COL_IS_KEY) {
            code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
            goto _exit;
          }
          if ((code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0))) {
            goto _exit;
          }
        } else {
          if (varDataTLen(data + offset) > bytes) {
            uError("var data length invalid, varDataTLen(data + offset):%d > bytes:%d", (int)varDataTLen(data + offset),
                   bytes);
            code = TSDB_CODE_PAR_VALUE_TOO_LONG;
            goto _exit;
          }
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, (uint8_t *)varDataVal(data + offset),
                                                                        varDataLen(data + offset));
        }
      }
    } else {
      for (int32_t i = 0; i < nRows; ++i) {
        int32_t offset = *((int32_t *)lengthOrbitmap + i);
        if (offset == -1) {
          if (pColData->cflag & COL_IS_KEY) {
            code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
            goto _exit;
          }
          if ((code = tColDataAppendValueBlobImpl[pColData->flag][CV_FLAG_NULL](pBlobSet, pColData, NULL, 0))) {
            goto _exit;
          }
        } else {
          if (blobDataTLen(data + offset) > TSDB_MAX_BLOB_LEN) {
            uError("var data length invalid, varDataTLen(data + offset):%d > bytes:%d",
                   (int)blobDataTLen(data + offset), bytes);
            code = TSDB_CODE_PAR_VALUE_TOO_LONG;
            goto _exit;
          }
          code = tColDataAppendValueBlobImpl[pColData->flag][CV_FLAG_VALUE](
              pBlobSet, pColData, (uint8_t *)blobDataVal(data + offset), blobDataLen(data + offset));
        }
      }
    }
  } else {  // fixed-length data type
    bool allValue = true;
    bool allNull = true;
    for (int32_t i = 0; i < nRows; ++i) {
      if (!BMIsNull(lengthOrbitmap, i)) {
        allNull = false;
      } else {
        allValue = false;
      }
    }
    if ((pColData->cflag & COL_IS_KEY) && !allValue) {
      code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
      goto _exit;
    }

    if (allValue) {
      // optimize (todo)
      for (int32_t i = 0; i < nRows; ++i) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, (uint8_t *)data + bytes * i, bytes);
      }
    } else if (allNull) {
      // optimize (todo)
      for (int32_t i = 0; i < nRows; ++i) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
        if (code) goto _exit;
      }
    } else {
      for (int32_t i = 0; i < nRows; ++i) {
        if (BMIsNull(lengthOrbitmap, i)) {
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
          if (code) goto _exit;
        } else {
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, (uint8_t *)data + bytes * i, bytes);
        }
      }
    }
  }

_exit:
  return code;
}

int32_t tColDataAddValueByBind(SColData *pColData, TAOS_MULTI_BIND *pBind, int32_t buffMaxLen, initGeosFn igeos,
                               checkWKBGeometryFn cgeos) {
  int32_t code = 0;

  if (!(pBind->num == 1 && pBind->is_null && *pBind->is_null)) {
    if (!(pColData->type == pBind->buffer_type)) {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  if (IS_VAR_DATA_TYPE(pColData->type)) {  // var-length data type
    if (pColData->type == TSDB_DATA_TYPE_GEOMETRY) {
      code = igeos();
      if (code) {
        return code;
      }
    }
    for (int32_t i = 0; i < pBind->num; ++i) {
      if (pBind->is_null && pBind->is_null[i]) {
        if (pColData->cflag & COL_IS_KEY) {
          code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
          goto _exit;
        }
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
        if (code) goto _exit;
      } else if (pBind->length[i] > buffMaxLen) {
        return TSDB_CODE_PAR_VALUE_TOO_LONG;
      } else {
        if (pColData->type == TSDB_DATA_TYPE_GEOMETRY) {
          code = cgeos((char *)pBind->buffer + pBind->buffer_length * i, (size_t)pBind->length[i]);
          if (code) {
            uError("stmt col[%d] bind geometry wrong format", i);
            goto _exit;
          }
        }
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](
            pColData, (uint8_t *)pBind->buffer + pBind->buffer_length * i, pBind->length[i]);
      }
    }
  } else {  // fixed-length data type
    bool allValue;
    bool allNull;
    if (pBind->is_null) {
      bool same = (memcmp(pBind->is_null, pBind->is_null + 1, pBind->num - 1) == 0);
      allNull = (same && pBind->is_null[0] != 0);
      allValue = (same && pBind->is_null[0] == 0);
    } else {
      allNull = false;
      allValue = true;
    }

    if ((pColData->cflag & COL_IS_KEY) && !allValue) {
      code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
      goto _exit;
    }

    if (allValue) {
      // optimize (todo)
      for (int32_t i = 0; i < pBind->num; ++i) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](
            pColData, (uint8_t *)pBind->buffer + TYPE_BYTES[pColData->type] * i, pBind->buffer_length);
      }
    } else if (allNull) {
      // optimize (todo)
      for (int32_t i = 0; i < pBind->num; ++i) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
        if (code) goto _exit;
      }
    } else {
      for (int32_t i = 0; i < pBind->num; ++i) {
        if (pBind->is_null[i]) {
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
          if (code) goto _exit;
        } else {
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](
              pColData, (uint8_t *)pBind->buffer + TYPE_BYTES[pColData->type] * i, pBind->buffer_length);
        }
      }
    }
  }

_exit:
  return code;
}

int32_t tColDataAddValueByBind2(SColData *pColData, TAOS_STMT2_BIND *pBind, int32_t buffMaxLen) {
  int32_t code = 0;

  if (!(pBind->num == 1 && pBind->is_null && *pBind->is_null)) {
    if (!(pColData->type == pBind->buffer_type)) {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  if (IS_VAR_DATA_TYPE(pColData->type)) {  // var-length data type
    uint8_t *buf = pBind->buffer;
    for (int32_t i = 0; i < pBind->num; ++i) {
      if (pBind->is_null && pBind->is_null[i]) {
        if (pColData->cflag & COL_IS_KEY) {
          code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
          goto _exit;
        }
        if (pBind->is_null[i] == 1) {
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
          if (code) goto _exit;
        } else {
          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0);
          if (code) goto _exit;
        }
      } else if (pBind->length[i] > buffMaxLen) {
        return TSDB_CODE_PAR_VALUE_TOO_LONG;
      } else {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, buf, pBind->length[i]);
        buf += pBind->length[i];
      }
    }
  } else {  // fixed-length data type
    bool allValue;
    bool allNull;
    bool allNone;
    if (pBind->is_null) {
      bool same = (memcmp(pBind->is_null, pBind->is_null + 1, pBind->num - 1) == 0);
      allNull = (same && pBind->is_null[0] == 1);
      allNone = (same && pBind->is_null[0] > 1);
      allValue = (same && pBind->is_null[0] == 0);
    } else {
      allNull = false;
      allNone = false;
      allValue = true;
    }

    if ((pColData->cflag & COL_IS_KEY) && !allValue) {
      code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
      goto _exit;
    }

    uint8_t *buf = pBind->buffer;

    if (allValue) {
      // optimize (todo)
      for (int32_t i = 0; i < pBind->num; ++i) {
        uint8_t *val = (uint8_t *)pBind->buffer + TYPE_BYTES[pColData->type] * i;
        if (TSDB_DATA_TYPE_BOOL == pColData->type && *val > 1) {
          *val = 1;
        }
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, val, TYPE_BYTES[pColData->type]);
      }
    } else if (allNull) {
      // optimize (todo)
      for (int32_t i = 0; i < pBind->num; ++i) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
        if (code) goto _exit;
      }
    } else if (allNone) {
      // optimize (todo)
      for (int32_t i = 0; i < pBind->num; ++i) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0);
        if (code) goto _exit;
      }
    } else {
      for (int32_t i = 0; i < pBind->num; ++i) {
        if (pBind->is_null[i]) {
          if (pBind->is_null[i] == 1) {
            code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
            if (code) goto _exit;
          } else {
            code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0);
            if (code) goto _exit;
          }
        } else {
          uint8_t *val = (uint8_t *)pBind->buffer + TYPE_BYTES[pColData->type] * i;
          if (TSDB_DATA_TYPE_BOOL == pColData->type && *val > 1) {
            *val = 1;
          }

          code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, val, TYPE_BYTES[pColData->type]);
        }
      }
    }
  }

_exit:
  return code;
}

int32_t tColDataAddValueByBind2WithGeos(SColData *pColData, TAOS_STMT2_BIND *pBind, int32_t buffMaxLen,
                                        initGeosFn igeos, checkWKBGeometryFn cgeos) {
  int32_t code = 0;

  if (!(pBind->num == 1 && pBind->is_null && *pBind->is_null)) {
    if (!(pColData->type == pBind->buffer_type)) {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  if (pColData->type != TSDB_DATA_TYPE_GEOMETRY) {
    return TSDB_CODE_INVALID_OPTION;
  }

  code = igeos();
  if (code) {
    return code;
  }

  uint8_t *buf = pBind->buffer;
  for (int32_t i = 0; i < pBind->num; ++i) {
    if (pBind->is_null && pBind->is_null[i]) {
      if (pColData->cflag & COL_IS_KEY) {
        code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
        goto _exit;
      }
      if (pBind->is_null[i] == 1) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
        if (code) goto _exit;
      } else {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0);
        if (code) goto _exit;
      }
    } else if (pBind->length[i] > buffMaxLen) {
      return TSDB_CODE_PAR_VALUE_TOO_LONG;
    } else {
      code = cgeos(buf, pBind->length[i]);
      if (code) {
        uError("stmt2 col[%d] bind geometry with wrong format", i);
        goto _exit;
      }
      code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, buf, pBind->length[i]);
      buf += pBind->length[i];
    }
  }

_exit:
  return code;
}

int32_t tColDataAddValueByBind2WithBlob(SColData *pColData, TAOS_STMT2_BIND *pBind, int32_t buffMaxLen,
                                        SBlobSet *pBlobSet) {
  int32_t code = 0;

  if (!(pBind->num == 1 && pBind->is_null && *pBind->is_null)) {
    if (!(pColData->type == pBind->buffer_type)) {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  if (IS_STR_DATA_BLOB(pColData->type)) {  // var-length data type
    uint8_t *buf = pBind->buffer;
    for (int32_t i = 0; i < pBind->num; ++i) {
      if (pBind->is_null && pBind->is_null[i]) {
        if (pColData->cflag & COL_IS_KEY) {
          code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
          goto _exit;
        }
        if (pBind->is_null[i] == 1) {
          code = tColDataAppendValueBlobImpl[pColData->flag][CV_FLAG_NULL](pBlobSet, pColData, NULL, 0);
          if (code) goto _exit;
        } else {
          code = tColDataAppendValueBlobImpl[pColData->flag][CV_FLAG_NONE](pBlobSet, pColData, NULL, 0);
          if (code) goto _exit;
        }
      } else if (pBind->length[i] > buffMaxLen) {
        return TSDB_CODE_PAR_VALUE_TOO_LONG;
      } else {
        code = tColDataAppendValueBlobImpl[pColData->flag][CV_FLAG_VALUE](pBlobSet, pColData, buf, pBind->length[i]);
        buf += pBind->length[i];
      }
    }
  }
_exit:
  return code;
}

int32_t tColDataAddValueByBind2WithDecimal(SColData *pColData, TAOS_STMT2_BIND *pBind, int32_t buffMaxLen,
                                           uint8_t precision, uint8_t scale) {
  int32_t code = 0;

  if (!(pBind->num == 1 && pBind->is_null && *pBind->is_null)) {
    if (!(pColData->type == pBind->buffer_type)) {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  if (!IS_DECIMAL_TYPE(pColData->type)) {
    return TSDB_CODE_INVALID_OPTION;
  }

  uint8_t *buf = pBind->buffer;
  for (int32_t i = 0; i < pBind->num; ++i) {
    if (pBind->is_null && pBind->is_null[i]) {
      if (pColData->cflag & COL_IS_KEY) {
        code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
        goto _exit;
      }
      if (pBind->is_null[i] == 1) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
        if (code) goto _exit;
      } else {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0);
        if (code) goto _exit;
      }
    } else {
      if (pColData->type == TSDB_DATA_TYPE_DECIMAL64) {
        Decimal64 dec = {0};
        int32_t   code = decimal64FromStr((char *)buf, pBind->length[i], precision, scale, &dec);
        buf += pBind->length[i];
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
        int64_t tmp = DECIMAL64_GET_VALUE(&dec);
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, (uint8_t *)&tmp,
                                                                      TYPE_BYTES[pColData->type]);
      } else if (pColData->type == TSDB_DATA_TYPE_DECIMAL) {
        Decimal128 dec = {0};
        int32_t    code = decimal128FromStr((char *)buf, pBind->length[i], precision, scale, &dec);
        buf += pBind->length[i];
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
        uint8_t *pV = taosMemCalloc(1, sizeof(Decimal128));
        if (!pV) return terrno;
        memcpy(pV, &dec, DECIMAL_WORD_NUM(Decimal128) * sizeof(DecimalWord));
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, pV, TYPE_BYTES[pColData->type]);
      }
    }
  }

_exit:
  return code;
}
/* build rows to `rowArray` from bind
 * `infos` is the bind information array
 * `numOfInfos` is the number of bind information
 * `infoSorted` is whether the bind information is sorted by column id
 * `pTSchema` is the schema of the table
 * `rowArray` is the array to store the rows
 * `pOrdered` is the pointer to store ordered
 * `pDupTs` is the pointer to store duplicateTs
 */
int32_t tRowBuildFromBind2(SBindInfo2 *infos, int32_t numOfInfos, SSHashObj *parsedCols, bool infoSorted,
                           const STSchema *pTSchema, const SSchemaExt *pSchemaExt, SArray *rowArray, bool *pOrdered,
                           bool *pDupTs) {
  if (infos == NULL || numOfInfos <= 0 || numOfInfos > pTSchema->numOfCols || pTSchema == NULL || rowArray == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int8_t hasBlob = schemaHasBlob(pTSchema);
  if (!infoSorted) {
    taosqsort_r(infos, numOfInfos, sizeof(SBindInfo2), NULL, tBindInfoCompare);
  }

  int32_t code = 0;
  int32_t numOfRows = -1;
  SArray *colValArray, *bufArray;
  SColVal colVal;
  int32_t numOfFixedValue = 0;
  int32_t lino = 0;

  if ((colValArray = taosArrayInit(numOfInfos, sizeof(SColVal))) == NULL) {
    return terrno;
  }
  if ((bufArray = taosArrayInit(numOfInfos, sizeof(uint8_t *))) == NULL) {
    taosArrayDestroy(colValArray);
    return terrno;
  }
  for (int i = 0; i < numOfInfos; ++i) {
    if (parsedCols) {
      SColVal *pParsedVal = tSimpleHashGet(parsedCols, &infos[i].columnId, sizeof(int16_t));
      if (pParsedVal) {
        continue;
      }
    }
    if (numOfRows == -1) {
      numOfRows = infos[i].bind->num;
    }

    if (!taosArrayPush(bufArray, &infos[i].bind->buffer)) {
      taosArrayDestroy(colValArray);
      taosArrayDestroy(bufArray);
      return terrno;
    }
  }

  SRowKey rowKey, lastRowKey;
  for (int32_t iRow = 0; iRow < numOfRows; iRow++) {
    taosArrayClear(colValArray);

    for (int32_t iInfo = 0; iInfo < numOfInfos; iInfo++) {
      if (parsedCols) {
        SColVal *pParsedVal = tSimpleHashGet(parsedCols, &infos[iInfo].columnId, sizeof(int16_t));
        if (pParsedVal) {
          numOfFixedValue++;
          colVal = *pParsedVal;

          if (taosArrayPush(colValArray, &colVal) == NULL) {
            if (IS_VAR_DATA_TYPE(pParsedVal->value.type)) {
              taosMemoryFree(colVal.value.pData);
            }
            code = terrno;
            TAOS_CHECK_GOTO(code, &lino, _exit);
          }
          continue;
        }
      }

      if (infos[iInfo].bind->is_null && infos[iInfo].bind->is_null[iRow]) {
        if (infos[iInfo].bind->is_null[iRow] == 1) {
          if (iInfo == 0) {
            code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
            TAOS_CHECK_GOTO(code, &lino, _exit);
          }
          colVal = COL_VAL_NULL(infos[iInfo].columnId, infos[iInfo].type);
        } else {
          colVal = COL_VAL_NONE(infos[iInfo].columnId, infos[iInfo].type);
        }
      } else {
        SValue value = {
            .type = infos[iInfo].type,
        };
        if (IS_VAR_DATA_TYPE(infos[iInfo].type)) {
          if (IS_STR_DATA_BLOB(infos[iInfo].type)) {
            int32_t   length = infos[iInfo].bind->length[iRow];
            uint8_t **data = &((uint8_t **)TARRAY_DATA(bufArray))[iInfo - numOfFixedValue];
            value.nData = length;
            if (value.nData > (TSDB_MAX_BLOB_LEN - BLOBSTR_HEADER_SIZE)) {
              code = TSDB_CODE_PAR_VALUE_TOO_LONG;
              uError("stmt2 bind col:%d, row:%d length:%d  greater than type maximum lenght: %d", iInfo, iRow,
                     value.nData + (uint32_t)(BLOBSTR_HEADER_SIZE), infos[iInfo].bytes);
              goto _exit;
            }
            value.pData = *data;
            *data += length;
          } else {
            int32_t   length = infos[iInfo].bind->length[iRow];
            uint8_t **data = &((uint8_t **)TARRAY_DATA(bufArray))[iInfo - numOfFixedValue];
            value.nData = length;
            if (value.nData > infos[iInfo].bytes - VARSTR_HEADER_SIZE) {
              code = TSDB_CODE_PAR_VALUE_TOO_LONG;
              uError("stmt2 bind col:%d, row:%d length:%d  greater than type maximum lenght: %d", iInfo, iRow,
                     value.nData + (uint32_t)(BLOBSTR_HEADER_SIZE), infos[iInfo].bytes);
              goto _exit;
            }
            value.pData = *data;
            *data += length;
          }
          // value.pData = (uint8_t *)infos[iInfo].bind->buffer + infos[iInfo].bind->buffer_length * iRow;
        } else {
          if (infos[iInfo].type == TSDB_DATA_TYPE_DECIMAL) {
            if (!pSchemaExt) {
              uError("stmt2 decimal64 type without ext schema info, cannot parse decimal values");
              code = TSDB_CODE_PAR_INTERNAL_ERROR;
              goto _exit;
            }
            uint8_t precision = 0, scale = 0;
            decimalFromTypeMod(pSchemaExt[iInfo].typeMod, &precision, &scale);
            Decimal128 dec = {0};
            uint8_t  **data = &((uint8_t **)TARRAY_DATA(bufArray))[iInfo - numOfFixedValue];
            int32_t    length = infos[iInfo].bind->length[iRow];
            code = decimal128FromStr(*(char **)data, length, precision, scale, &dec);
            *data += length;
            TAOS_CHECK_GOTO(code, &lino, _exit);

            // precision check
            // scale auto fit

            code = decimal128ToDataVal(&dec, &value);
            TAOS_CHECK_GOTO(code, &lino, _exit);

          } else if (infos[iInfo].type == TSDB_DATA_TYPE_DECIMAL64) {
            if (!pSchemaExt) {
              uError("stmt2 decimal128 type without ext schema info, cannot parse decimal values");
              code = TSDB_CODE_PAR_INTERNAL_ERROR;
              goto _exit;
            }
            uint8_t precision = 0, scale = 0;
            decimalFromTypeMod(pSchemaExt[iInfo].typeMod, &precision, &scale);
            Decimal64 dec = {0};
            uint8_t **data = &((uint8_t **)TARRAY_DATA(bufArray))[iInfo - numOfFixedValue];
            int32_t   length = infos[iInfo].bind->length[iRow];
            code = decimal64FromStr(*(char **)data, length, precision, scale, &dec);
            *data += length;
            TAOS_CHECK_GOTO(code, &lino, _exit);

            code = decimal64ToDataVal(&dec, &value);
            TAOS_CHECK_GOTO(code, &lino, _exit);

          } else {
            uint8_t *val = (uint8_t *)infos[iInfo].bind->buffer + infos[iInfo].bytes * iRow;
            if (TSDB_DATA_TYPE_BOOL == value.type && *val > 1) {
              *val = 1;
            }
            valueSetDatum(&value, infos[iInfo].type, val, infos[iInfo].bytes);
          }
        }
        colVal = COL_VAL_VALUE(infos[iInfo].columnId, value);
      }
      if (taosArrayPush(colValArray, &colVal) == NULL) {
        code = terrno;
        goto _exit;
      }
    }

    SRow *row;

    if (hasBlob == 0) {
      SRowBuildScanInfo sinfo = {0};
      code = tRowBuild(colValArray, pTSchema, &row, &sinfo);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    } else {
      SRowBuildScanInfo sinfo = {.hasBlob = 1, .scanType = ROW_BUILD_UPDATE};
      code = tRowBuildWithBlob(colValArray, pTSchema, &row, NULL, &sinfo);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }

    if ((taosArrayPush(rowArray, &row)) == NULL) {
      code = terrno;
      goto _exit;
    }

    if (pOrdered && pDupTs) {
      tRowGetKey(row, &rowKey);
      if (iRow == 0) {
        *pOrdered = true;
        *pDupTs = false;
      } else {
        if (*pOrdered) {
          int32_t res = tRowKeyCompare(&rowKey, &lastRowKey);
          *pOrdered = (res >= 0);
          if (!*pDupTs) {
            *pDupTs = (res == 0);
          }
        }
      }
      lastRowKey = rowKey;
    }
  }
_exit:
  if (code != 0) {
    uError("tRowBuildFromBind2 failed at line %d, ErrCode=0x%x", lino, code);
  }
  taosArrayDestroy(colValArray);
  taosArrayDestroy(bufArray);
  return code;
}
/* build rows to `rowArray` from bind
 * `infos` is the bind information array
 * `numOfInfos` is the number of bind information
 * `infoSorted` is whether the bind information is sorted by column id
 * `pTSchema` is the schema of the table
 * `rowArray` is the array to store the rows
 * `pOrdered` is the pointer to store ordered
 * `pDupTs` is the pointer to store duplicateTs
 */
int32_t tRowBuildFromBind2WithBlob(SBindInfo2 *infos, int32_t numOfInfos, bool infoSorted, const STSchema *pTSchema,
                                   SArray *rowArray, bool *pOrdered, bool *pDupTs, SBlobSet *pBlobSet) {
  if (infos == NULL || numOfInfos <= 0 || numOfInfos > pTSchema->numOfCols || pTSchema == NULL || rowArray == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int8_t hasBlob = schemaHasBlob(pTSchema);
  if (!infoSorted) {
    taosqsort_r(infos, numOfInfos, sizeof(SBindInfo2), NULL, tBindInfoCompare);
  }

  int32_t code = 0;
  int32_t numOfRows = infos[0].bind->num;
  SArray *colValArray, *bufArray;
  SColVal colVal;

  if ((colValArray = taosArrayInit(numOfInfos, sizeof(SColVal))) == NULL) {
    return terrno;
  }
  if ((bufArray = taosArrayInit(numOfInfos, sizeof(uint8_t *))) == NULL) {
    taosArrayDestroy(colValArray);
    return terrno;
  }
  for (int i = 0; i < numOfInfos; ++i) {
    if (!taosArrayPush(bufArray, &infos[i].bind->buffer)) {
      taosArrayDestroy(colValArray);
      taosArrayDestroy(bufArray);
      return terrno;
    }
  }

  SRowKey rowKey, lastRowKey;
  for (int32_t iRow = 0; iRow < numOfRows; iRow++) {
    taosArrayClear(colValArray);

    for (int32_t iInfo = 0; iInfo < numOfInfos; iInfo++) {
      if (infos[iInfo].bind->is_null && infos[iInfo].bind->is_null[iRow]) {
        if (infos[iInfo].bind->is_null[iRow] == 1) {
          if (iInfo == 0) {
            code = TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
            goto _exit;
          }
          colVal = COL_VAL_NULL(infos[iInfo].columnId, infos[iInfo].type);
        } else {
          colVal = COL_VAL_NONE(infos[iInfo].columnId, infos[iInfo].type);
        }
      } else {
        SValue value = {
            .type = infos[iInfo].type,
        };
        if (IS_VAR_DATA_TYPE(infos[iInfo].type)) {
          if (IS_STR_DATA_BLOB(infos[iInfo].type)) {
            int32_t   length = infos[iInfo].bind->length[iRow];
            uint8_t **data = &((uint8_t **)TARRAY_DATA(bufArray))[iInfo];
            value.nData = length;
            if (value.nData > (TSDB_MAX_BLOB_LEN - BLOBSTR_HEADER_SIZE)) {
              code = TSDB_CODE_PAR_VALUE_TOO_LONG;
              uError("stmt bind param[%d] length:%d  greater than type maximum lenght: %d", iInfo, value.nData,
                     pTSchema->columns[infos[iInfo].columnId - 1].bytes);
              goto _exit;
            }
            value.pData = *data;
            *data += length;
          } else {
            int32_t   length = infos[iInfo].bind->length[iRow];
            uint8_t **data = &((uint8_t **)TARRAY_DATA(bufArray))[iInfo];
            value.nData = length;
            if (value.nData > pTSchema->columns[infos[iInfo].columnId - 1].bytes - VARSTR_HEADER_SIZE) {
              code = TSDB_CODE_PAR_VALUE_TOO_LONG;
              uError("stmt bind param[%d] length:%d  greater than type maximum lenght: %d", iInfo, value.nData,
                     pTSchema->columns[infos[iInfo].columnId - 1].bytes);
              goto _exit;
            }
            value.pData = *data;
            *data += length;
          }

          // value.pData = (uint8_t *)infos[iInfo].bind->buffer + infos[iInfo].bind->buffer_length * iRow;
        } else {
          uint8_t *val = (uint8_t *)infos[iInfo].bind->buffer + infos[iInfo].bytes * iRow;
          if (TSDB_DATA_TYPE_BOOL == value.type && *val > 1) {
            *val = 1;
          }
          valueSetDatum(&value, infos[iInfo].type, val, infos[iInfo].bytes);
        }
        colVal = COL_VAL_VALUE(infos[iInfo].columnId, value);
      }
      if (taosArrayPush(colValArray, &colVal) == NULL) {
        code = terrno;
        goto _exit;
      }
    }

    SRow *row;

    if (hasBlob == 0) {
      SRowBuildScanInfo sinfo = {0};
      if ((code = tRowBuild(colValArray, pTSchema, &row, &sinfo))) {
        goto _exit;
      }
    } else {
      SRowBuildScanInfo sinfo = {.hasBlob = 1, .scanType = ROW_BUILD_UPDATE};
      if ((code = tRowBuildWithBlob(colValArray, pTSchema, &row, pBlobSet, &sinfo))) {
        goto _exit;
      }
    }

    if ((taosArrayPush(rowArray, &row)) == NULL) {
      code = terrno;
      goto _exit;
    }

    if (pOrdered && pDupTs) {
      tRowGetKey(row, &rowKey);
      if (iRow == 0) {
        *pOrdered = true;
        *pDupTs = false;
      } else {
        if (*pOrdered) {
          int32_t res = tRowKeyCompare(&rowKey, &lastRowKey);
          *pOrdered = (res >= 0);
          if (!*pDupTs) {
            *pDupTs = (res == 0);
          }
        }
      }
      lastRowKey = rowKey;
    }
  }
_exit:
  taosArrayDestroy(colValArray);
  taosArrayDestroy(bufArray);
  return code;
}

static int32_t tColDataCopyRowCell(SColData *pFromColData, int32_t iFromRow, SColData *pToColData, int32_t iToRow) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (IS_VAR_DATA_TYPE(pToColData->type)) {
    int32_t nData = (iFromRow < pFromColData->nVal - 1)
                        ? pFromColData->aOffset[iFromRow + 1] - pFromColData->aOffset[iFromRow]
                        : pFromColData->nData - pFromColData->aOffset[iFromRow];
    if (iToRow == 0) {
      pToColData->aOffset[iToRow] = 0;
    }

    if (iToRow < pToColData->nVal - 1) {
      pToColData->aOffset[iToRow + 1] = pToColData->aOffset[iToRow] + nData;
    }

    (void)memcpy(pToColData->pData + pToColData->aOffset[iToRow], pFromColData->pData + pFromColData->aOffset[iFromRow],
                 nData);
  } else {
    (void)memcpy(&pToColData->pData[TYPE_BYTES[pToColData->type] * iToRow],
                 &pFromColData->pData[TYPE_BYTES[pToColData->type] * iFromRow], TYPE_BYTES[pToColData->type]);
  }
  return code;
}

static int32_t tColDataCopyRowSingleCol(SColData *pFromColData, int32_t iFromRow, SColData *pToColData,
                                        int32_t iToRow) {
  int32_t code = TSDB_CODE_SUCCESS;
  int     bit_val = 0;

  switch (pFromColData->flag) {
    case HAS_NONE: {
      ROW_SET_BITMAP(pToColData->pBitMap, pToColData->flag, iToRow, BIT_FLG_NONE);
    } break;
    case HAS_NULL: {
      ROW_SET_BITMAP(pToColData->pBitMap, pToColData->flag, iToRow, BIT_FLG_NULL);
    } break;
    case (HAS_NULL | HAS_NONE): {
      bit_val = GET_BIT1(pFromColData->pBitMap, iFromRow);
      if (0 == bit_val)
        ROW_SET_BITMAP(pToColData->pBitMap, pToColData->flag, iToRow, BIT_FLG_NONE);
      else
        ROW_SET_BITMAP(pToColData->pBitMap, pToColData->flag, iToRow, BIT_FLG_NULL);
    } break;
    case HAS_VALUE: {
      ROW_SET_BITMAP(pToColData->pBitMap, pToColData->flag, iToRow, BIT_FLG_VALUE);
      TAOS_CHECK_RETURN(tColDataCopyRowCell(pFromColData, iFromRow, pToColData, iToRow));
    } break;
    case (HAS_VALUE | HAS_NONE): {
      bit_val = GET_BIT1(pFromColData->pBitMap, iFromRow);
      if (0 == bit_val)
        ROW_SET_BITMAP(pToColData->pBitMap, pToColData->flag, iToRow, BIT_FLG_NONE);
      else
        ROW_SET_BITMAP(pToColData->pBitMap, pToColData->flag, iToRow, BIT_FLG_VALUE);
      TAOS_CHECK_RETURN(tColDataCopyRowCell(pFromColData, iFromRow, pToColData, iToRow));
    } break;
    case (HAS_VALUE | HAS_NULL): {
      bit_val = GET_BIT1(pFromColData->pBitMap, iFromRow);
      if (0 == bit_val)
        ROW_SET_BITMAP(pToColData->pBitMap, pToColData->flag, iToRow, BIT_FLG_NULL);
      else
        ROW_SET_BITMAP(pToColData->pBitMap, pToColData->flag, iToRow, BIT_FLG_VALUE);
      TAOS_CHECK_RETURN(tColDataCopyRowCell(pFromColData, iFromRow, pToColData, iToRow));
    } break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE): {
      SET_BIT2(pToColData->pBitMap, iToRow, GET_BIT2(pFromColData->pBitMap, iFromRow));
      TAOS_CHECK_RETURN(tColDataCopyRowCell(pFromColData, iFromRow, pToColData, iToRow));
    } break;
    default:
      return -1;
  }

  return code;
}

static int32_t tColDataCopyRow(SColData *aFromColData, int32_t iFromRow, SColData *aToColData, int32_t iToRow,
                               int32_t nColData) {
  int32_t code = TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < nColData; i++) {
    code = tColDataCopyRowSingleCol(&aFromColData[i], iFromRow, &aToColData[i], iToRow);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return code;
}

static int32_t tColDataCopyRowAppend(SColData *aFromColData, int32_t iFromRow, SColData *aToColData, int32_t nColData) {
  int32_t code = TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < nColData; i++) {
    SColVal cv = {0};
    code = tColDataGetValue(&aFromColData[i], iFromRow, &cv);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = tColDataAppendValue(&aToColData[i], &cv);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return code;
}

void tColDataArrGetRowKey(SColData *aColData, int32_t nColData, int32_t iRow, SRowKey *key) {
  SColVal cv;

  key->ts = ((TSKEY *)aColData[0].pData)[iRow];
  key->numOfPKs = 0;

  for (int i = 1; i < nColData; i++) {
    if (aColData[i].cflag & COL_IS_KEY) {
      tColDataGetValue4(&aColData[i], iRow, &cv);
      key->pks[key->numOfPKs++] = cv.value;
    } else {
      break;
    }
  }
}

static int32_t tColDataMergeSortMerge(SColData *aColData, int32_t start, int32_t mid, int32_t end, int32_t nColData) {
  SColData *aDstColData = NULL;
  int32_t   i = start, j = mid + 1, k = 0;
  SRowKey   keyi, keyj;

  if (end > start) {
    aDstColData = taosMemoryCalloc(1, sizeof(SColData) * nColData);
    if (aDstColData == NULL) {
      return terrno;
    }
    for (int c = 0; c < nColData; ++c) {
      tColDataInit(&aDstColData[c], aColData[c].cid, aColData[c].type, aColData[c].cflag);
    }
  }

  tColDataArrGetRowKey(aColData, nColData, i, &keyi);
  tColDataArrGetRowKey(aColData, nColData, j, &keyj);
  while (i <= mid && j <= end) {
    if (tRowKeyCompare(&keyi, &keyj) <= 0) {
      TAOS_CHECK_RETURN(tColDataCopyRowAppend(aColData, i++, aDstColData, nColData));
      tColDataArrGetRowKey(aColData, nColData, i, &keyi);
    } else {
      TAOS_CHECK_RETURN(tColDataCopyRowAppend(aColData, j++, aDstColData, nColData));
      if (j <= end) tColDataArrGetRowKey(aColData, nColData, j, &keyj);
    }
  }

  while (i <= mid) {
    TAOS_CHECK_RETURN(tColDataCopyRowAppend(aColData, i++, aDstColData, nColData));
  }

  while (j <= end) {
    TAOS_CHECK_RETURN(tColDataCopyRowAppend(aColData, j++, aDstColData, nColData));
  }

  for (i = start, k = 0; i <= end; ++i, ++k) {
    TAOS_CHECK_RETURN(tColDataCopyRow(aDstColData, k, aColData, i, nColData));
  }

  if (aDstColData) {
    for (int32_t i = 0; i < nColData; i++) {
      tColDataDestroy(&aDstColData[i]);
    }
    taosMemoryFree(aDstColData);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tColDataMergeSort(SColData *aColData, int32_t start, int32_t end, int32_t nColData) {
  int32_t ret = TSDB_CODE_SUCCESS;
  int32_t mid;

  if (start >= end) {
    return TSDB_CODE_SUCCESS;
  }

  mid = (start + end) / 2;

  ret = tColDataMergeSort(aColData, start, mid, nColData);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  ret = tColDataMergeSort(aColData, mid + 1, end, nColData);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  return tColDataMergeSortMerge(aColData, start, mid, end, nColData);
}

static int32_t tColDataSort(SColData *aColData, int32_t nColData) {
  int32_t nVal = aColData[0].nVal;

  if (nVal < 2) return TSDB_CODE_SUCCESS;

  return tColDataMergeSort(aColData, 0, nVal - 1, nColData);
}

static int32_t tColDataMerge(SArray **colArr) {
  int32_t code = 0;
  SArray *src = *colArr;
  SArray *dst = NULL;

  dst = taosArrayInit(taosArrayGetSize(src), sizeof(SColData));
  if (dst == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < taosArrayGetSize(src); i++) {
    SColData *srcCol = taosArrayGet(src, i);

    SColData *dstCol = taosArrayReserve(dst, 1);
    if (dstCol == NULL) {
      code = terrno;
      goto _exit;
    }
    tColDataInit(dstCol, srcCol->cid, srcCol->type, srcCol->cflag);
  }

  int32_t numRows = ((SColData *)TARRAY_DATA(src))->nVal;
  SRowKey lastKey;
  for (int32_t i = 0; i < numRows; i++) {
    SRowKey key;
    tColDataArrGetRowKey((SColData *)TARRAY_DATA(src), taosArrayGetSize(src), i, &key);

    if (i == 0 || tRowKeyCompare(&key, &lastKey) != 0) {  // append new row
      for (int32_t j = 0; j < taosArrayGetSize(src); j++) {
        SColData *srcCol = taosArrayGet(src, j);
        SColData *dstCol = taosArrayGet(dst, j);

        SColVal cv;
        code = tColDataGetValue(srcCol, i, &cv);
        if (code != TSDB_CODE_SUCCESS) {
          goto _exit;
        }
        code = tColDataAppendValue(dstCol, &cv);
        if (code) {
          goto _exit;
        }
      }
      lastKey = key;
    } else {  // update existing row
      for (int32_t j = 0; j < taosArrayGetSize(src); j++) {
        SColData *srcCol = taosArrayGet(src, j);
        SColData *dstCol = taosArrayGet(dst, j);

        SColVal cv;
        code = tColDataGetValue(srcCol, i, &cv);
        if (code != TSDB_CODE_SUCCESS) {
          goto _exit;
        }
        code = tColDataUpdateValue(dstCol, &cv, true);
        if (code) {
          goto _exit;
        }
      }
    }
  }

_exit:
  if (code) {
    taosArrayDestroyEx(dst, tColDataDestroy);
  } else {
    taosArrayDestroyEx(src, tColDataDestroy);
    *colArr = dst;
  }
  return code;
}

int32_t tColDataSortMerge(SArray **arr) {
  SArray   *colDataArr = *arr;
  int32_t   nColData = TARRAY_SIZE(colDataArr);
  SColData *aColData = (SColData *)TARRAY_DATA(colDataArr);

  if (!(aColData[0].type == TSDB_DATA_TYPE_TIMESTAMP)) {
    return TSDB_CODE_PAR_INVALID_FIRST_COLUMN;
  }
  if (!(aColData[0].cid == PRIMARYKEY_TIMESTAMP_COL_ID)) {
    return TSDB_CODE_PAR_INVALID_FIRST_COLUMN;
  }
  if (!(aColData[0].flag == HAS_VALUE)) {
    return TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
  }

  if (aColData[0].nVal <= 1) goto _exit;

  int8_t doSort = 0;
  int8_t doMerge = 0;
  // scan -------
  SRowKey lastKey;
  tColDataArrGetRowKey(aColData, nColData, 0, &lastKey);
  for (int32_t iVal = 1; iVal < aColData[0].nVal; ++iVal) {
    SRowKey key;
    tColDataArrGetRowKey(aColData, nColData, iVal, &key);

    int32_t c = tRowKeyCompare(&lastKey, &key);
    if (c < 0) {
      lastKey = key;
      continue;
    } else if (c > 0) {
      doSort = 1;
      break;
    } else {
      doMerge = 1;
    }
  }

  // sort -------
  if (doSort) {
    TAOS_CHECK_RETURN(tColDataSort(aColData, nColData));
  }

  if ((doMerge != 1) && (doSort == 1)) {
    tColDataArrGetRowKey(aColData, nColData, 0, &lastKey);
    for (int32_t iVal = 1; iVal < aColData[0].nVal; ++iVal) {
      SRowKey key;
      tColDataArrGetRowKey(aColData, nColData, iVal, &key);

      int32_t c = tRowKeyCompare(&lastKey, &key);
      if (c == 0) {
        doMerge = 1;
        break;
      }
      lastKey = key;
    }
  }

  // merge -------
  if (doMerge) {
    int32_t code = tColDataMerge(arr);
    if (code) return code;
  }

_exit:
  return 0;
}

int32_t tColDataSortMergeWithBlob(SArray **arr, SBlobSet *pBlob) {
  SArray   *colDataArr = *arr;
  int32_t   nColData = TARRAY_SIZE(colDataArr);
  SColData *aColData = (SColData *)TARRAY_DATA(colDataArr);

  if (!(aColData[0].type == TSDB_DATA_TYPE_TIMESTAMP)) {
    return TSDB_CODE_PAR_INVALID_FIRST_COLUMN;
  }
  if (!(aColData[0].cid == PRIMARYKEY_TIMESTAMP_COL_ID)) {
    return TSDB_CODE_PAR_INVALID_FIRST_COLUMN;
  }
  if (!(aColData[0].flag == HAS_VALUE)) {
    return TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL;
  }

  if (aColData[0].nVal <= 1) goto _exit;

  int8_t doSort = 0;
  int8_t doMerge = 0;
  // scan -------
  SRowKey lastKey;
  tColDataArrGetRowKey(aColData, nColData, 0, &lastKey);
  for (int32_t iVal = 1; iVal < aColData[0].nVal; ++iVal) {
    SRowKey key;
    tColDataArrGetRowKey(aColData, nColData, iVal, &key);

    int32_t c = tRowKeyCompare(&lastKey, &key);
    if (c < 0) {
      lastKey = key;
      continue;
    } else if (c > 0) {
      doSort = 1;
      break;
    } else {
      doMerge = 1;
    }
  }
  if (doMerge || doSort) {
    return TSDB_CODE_BLOB_NOT_SUPPORT;
  }
  // sort -------
  if (doSort) {
    TAOS_CHECK_RETURN(tColDataSort(aColData, nColData));
  }

  if ((doMerge != 1) && (doSort == 1)) {
    tColDataArrGetRowKey(aColData, nColData, 0, &lastKey);
    for (int32_t iVal = 1; iVal < aColData[0].nVal; ++iVal) {
      SRowKey key;
      tColDataArrGetRowKey(aColData, nColData, iVal, &key);

      int32_t c = tRowKeyCompare(&lastKey, &key);
      if (c == 0) {
        doMerge = 1;
        break;
      }
      lastKey = key;
    }
  }

  // merge -------
  if (doMerge) {
    int32_t code = tColDataMerge(arr);
    if (code) return code;
  }

_exit:
  return 0;
}

static int32_t tEncodeColDataVersion0(SEncoder *pEncoder, SColData *pColData) {
  int32_t code = 0;

  if ((code = tEncodeI16v(pEncoder, pColData->cid))) return code;
  if ((code = tEncodeI8(pEncoder, pColData->type))) return code;
  if ((code = tEncodeI32v(pEncoder, pColData->nVal))) return code;
  if ((code = tEncodeI8(pEncoder, pColData->flag))) return code;

  // bitmap
  switch (pColData->flag) {
    case (HAS_NULL | HAS_NONE):
    case (HAS_VALUE | HAS_NONE):
    case (HAS_VALUE | HAS_NULL):
      code = tEncodeFixed(pEncoder, pColData->pBitMap, BIT1_SIZE(pColData->nVal));
      if (code) return code;
      break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      code = tEncodeFixed(pEncoder, pColData->pBitMap, BIT2_SIZE(pColData->nVal));
      if (code) return code;
      break;
    default:
      break;
  }

  // value
  if (pColData->flag & HAS_VALUE) {
    if (IS_VAR_DATA_TYPE(pColData->type)) {
      code = tEncodeFixed(pEncoder, pColData->aOffset, pColData->nVal << 2);
      if (code) return code;

      code = tEncodeI32v(pEncoder, pColData->nData);
      if (code) return code;

      code = tEncodeFixed(pEncoder, pColData->pData, pColData->nData);
      if (code) return code;
    } else {
      code = tEncodeFixed(pEncoder, pColData->pData, pColData->nData);
      if (code) return code;
    }
  }

  return code;
}

static int32_t tDecodeColDataVersion0(SDecoder *pDecoder, SColData *pColData) {
  int32_t code = 0;

  if ((code = tDecodeI16v(pDecoder, &pColData->cid))) return code;
  if ((code = tDecodeI8(pDecoder, &pColData->type))) return code;
  if ((code = tDecodeI32v(pDecoder, &pColData->nVal))) return code;
  if ((code = tDecodeI8(pDecoder, &pColData->flag))) return code;

  if (pColData->type <= 0 || pColData->type >= TSDB_DATA_TYPE_MAX || pColData->flag <= 0 || pColData->flag >= 8) {
    return TSDB_CODE_INVALID_PARA;
  }

  // bitmap
  switch (pColData->flag) {
    case (HAS_NULL | HAS_NONE):
    case (HAS_VALUE | HAS_NONE):
    case (HAS_VALUE | HAS_NULL):
      code = tDecodeBinaryWithSize(pDecoder, BIT1_SIZE(pColData->nVal), &pColData->pBitMap);
      if (code) return code;
      break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      code = tDecodeBinaryWithSize(pDecoder, BIT2_SIZE(pColData->nVal), &pColData->pBitMap);
      if (code) return code;
      break;
    default:
      break;
  }

  // value
  if (pColData->flag & HAS_VALUE) {
    if (IS_VAR_DATA_TYPE(pColData->type)) {
      code = tDecodeBinaryWithSize(pDecoder, pColData->nVal << 2, (uint8_t **)&pColData->aOffset);
      if (code) return code;

      code = tDecodeI32v(pDecoder, &pColData->nData);
      if (code) return code;

      code = tDecodeBinaryWithSize(pDecoder, pColData->nData, &pColData->pData);
      if (code) return code;
    } else {
      pColData->nData = TYPE_BYTES[pColData->type] * pColData->nVal;
      code = tDecodeBinaryWithSize(pDecoder, pColData->nData, &pColData->pData);
      if (code) return code;
    }
  }
  pColData->cflag = 0;

  return code;
}

static int32_t tEncodeColDataVersion1(SEncoder *pEncoder, SColData *pColData) {
  int32_t code = tEncodeColDataVersion0(pEncoder, pColData);
  if (code) return code;
  return tEncodeI8(pEncoder, pColData->cflag);
}

static int32_t tDecodeColDataVersion1(SDecoder *pDecoder, SColData *pColData) {
  int32_t code = tDecodeColDataVersion0(pDecoder, pColData);
  if (code) return code;

  code = tDecodeI8(pDecoder, &pColData->cflag);
  return code;
}

int32_t tEncodeColData(uint8_t version, SEncoder *pEncoder, SColData *pColData) {
  if (version == 0) {
    return tEncodeColDataVersion0(pEncoder, pColData);
  } else if (version == 1) {
    return tEncodeColDataVersion1(pEncoder, pColData);
  } else if (version == 2) {
    int32_t posStart = pEncoder->pos;
    pEncoder->pos += INT_BYTES;
    int32_t code = tEncodeColDataVersion1(pEncoder, pColData);
    if (code) return code;
    int32_t posEnd = pEncoder->pos;
    int32_t pos = posEnd - posStart;
    pEncoder->pos = posStart;
    code = tEncodeI32(pEncoder, pos);
    pEncoder->pos = posEnd;
    return code;
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

int32_t tDecodeColData(uint8_t version, SDecoder *pDecoder, SColData *pColData, bool jump) {
  if (version == 0) {
    return tDecodeColDataVersion0(pDecoder, pColData);
  } else if (version == 1) {
    return tDecodeColDataVersion1(pDecoder, pColData);
  } else if (version == 2) {
    if (jump) {
      int32_t len = 0;
      int32_t code = tDecodeI32(pDecoder, &len);
      if (code) return code;
      pDecoder->pos += (len - INT_BYTES);
      return TSDB_CODE_SUCCESS;
    } else {
      pDecoder->pos += INT_BYTES;
      return tDecodeColDataVersion1(pDecoder, pColData);
    }    
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

int32_t tEncodeRow(SEncoder *pEncoder, SRow *pRow) { return tEncodeFixed(pEncoder, pRow, pRow->len); }

int32_t tDecodeRow(SDecoder *pDecoder, SRow **ppRow) {
  if (ppRow == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pDecoder->pos + sizeof(SRow) > pDecoder->size) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  SRow *pRow = (SRow *)(pDecoder->data + pDecoder->pos);
  return tDecodeBinaryWithSize(pDecoder, pRow->len, (uint8_t **)ppRow);
}

int32_t tEncodeBlobSet(SEncoder *pEncoder, SBlobSet *pRow) {
  int32_t code = 0;
  int32_t lino = 0;
  uint8_t compressed = 0;
  int32_t compressSize = 0;
  char   *p = NULL;

  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pRow->type));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pRow->len));

  int32_t nSeq = taosArrayGetSize(pRow->pSeqTable);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, nSeq));

  // if (pRow->type) {
  void *pIter = taosHashIterate(pRow->pSeqToffset, NULL);
  while (pIter) {
    uint64_t seq = *(uint64_t *)taosHashGetKey(pIter, NULL);
    int32_t  idx = *(int32_t *)pIter;
    TAOS_CHECK_EXIT(tEncodeU64(pEncoder, seq));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, idx));

    pIter = taosHashIterate(pRow->pSeqToffset, pIter);
  }
  //}
  for (int32_t i = 0; i < nSeq; i++) {
    SBlobValue *p = taosArrayGet(pRow->pSeqTable, i);
    TAOS_CHECK_EXIT(tEncodeU64(pEncoder, p->offset));
    TAOS_CHECK_EXIT(tEncodeU32(pEncoder, p->len));
    TAOS_CHECK_EXIT(tEncodeU32(pEncoder, p->dataOffset));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, p->nextRow));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, p->type));
  }

  TAOS_CHECK_EXIT(tEncodeFixed(pEncoder, pRow->data, pRow->len));

_exit:
  return code;
}

int32_t tDecodeBlobSet(SDecoder *pDecoder, SBlobSet **pBlobSet) {
  int32_t    code = 0;
  int32_t    lino = 0;
  int32_t    nSeq = 0;
  uint8_t    compressd = 0;
  int32_t    compressSize = 0;
  SBlobSet  *pBlob = taosMemCalloc(1, sizeof(SBlobSet));
  if (pBlob == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pBlob->type));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pBlob->len));

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &nSeq));

  // if (pBlob->type) {
  pBlob->pSeqToffset = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
  if (pBlob->pSeqToffset == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < nSeq; i++) {
    uint64_t seq;
    int32_t  idx;
    TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &seq));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &idx));

    code = taosHashPut(pBlob->pSeqToffset, &seq, sizeof(seq), &idx, sizeof(idx));
    TAOS_CHECK_EXIT(code);
  }
  //}

  pBlob->pSeqTable = taosArrayInit(nSeq, sizeof(SBlobValue));
  if (pBlob->pSeqTable == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < nSeq; i++) {
    SBlobValue value;
    TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &value.offset));
    TAOS_CHECK_EXIT(tDecodeU32(pDecoder, &value.len));
    TAOS_CHECK_EXIT(tDecodeU32(pDecoder, &value.dataOffset));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &value.nextRow));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &value.type));
    if (taosArrayPush(pBlob->pSeqTable, &value) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  pBlob->data = taosMemCalloc(1, pBlob->len);
  if (pBlob->data == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  TAOS_CHECK_EXIT(tDecodeFixed(pDecoder, pBlob->data, pBlob->len));
  *pBlobSet = pBlob;

  uTrace("decode blob len:%d", (int32_t)(pBlob->len));

_exit:
  if (code != 0) {
    if (pBlob != NULL) {
      taosMemoryFree(pBlob->data);
      taosArrayDestroy(pBlob->pSeqTable);
      taosMemoryFree(pBlob);
    }
  }
  return code;
}

#define CALC_SUM_MAX_MIN(SUM, MAX, MIN, VAL) \
  do {                                       \
    (SUM) += (VAL);                          \
    if ((MAX) < (VAL)) (MAX) = (VAL);        \
    if ((MIN) > (VAL)) (MIN) = (VAL);        \
  } while (0)

static FORCE_INLINE void tColDataCalcSMABool(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *sum = 0;
  *max = 0;
  *min = 1;
  *numOfNull = 0;

  int8_t val;
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      val = ((int8_t *)pColData->pData)[iVal] ? 1 : 0;
      CALC_SUM_MAX_MIN(*sum, *max, *min, val);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          (*numOfNull)++;
          break;
        case 2:
          val = ((int8_t *)pColData->pData)[iVal] ? 1 : 0;
          CALC_SUM_MAX_MIN(*sum, *max, *min, val);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMATinyInt(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *sum = 0;
  *max = INT8_MIN;
  *min = INT8_MAX;
  *numOfNull = 0;

  int8_t val;
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      val = ((int8_t *)pColData->pData)[iVal];
      CALC_SUM_MAX_MIN(*sum, *max, *min, val);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          (*numOfNull)++;
          break;
        case 2:
          val = ((int8_t *)pColData->pData)[iVal];
          CALC_SUM_MAX_MIN(*sum, *max, *min, val);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMATinySmallInt(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *sum = 0;
  *max = INT16_MIN;
  *min = INT16_MAX;
  *numOfNull = 0;

  int16_t val;
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      val = ((int16_t *)pColData->pData)[iVal];
      CALC_SUM_MAX_MIN(*sum, *max, *min, val);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          (*numOfNull)++;
          break;
        case 2:
          val = ((int16_t *)pColData->pData)[iVal];
          CALC_SUM_MAX_MIN(*sum, *max, *min, val);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAInt(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *sum = 0;
  *max = INT32_MIN;
  *min = INT32_MAX;
  *numOfNull = 0;

  int32_t val;
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      val = ((int32_t *)pColData->pData)[iVal];
      CALC_SUM_MAX_MIN(*sum, *max, *min, val);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          (*numOfNull)++;
          break;
        case 2:
          val = ((int32_t *)pColData->pData)[iVal];
          CALC_SUM_MAX_MIN(*sum, *max, *min, val);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMABigInt(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *sum = 0;
  *max = INT64_MIN;
  *min = INT64_MAX;
  *numOfNull = 0;

  int64_t val;
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      val = ((int64_t *)pColData->pData)[iVal];
      CALC_SUM_MAX_MIN(*sum, *max, *min, val);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          (*numOfNull)++;
          break;
        case 2:
          val = ((int64_t *)pColData->pData)[iVal];
          CALC_SUM_MAX_MIN(*sum, *max, *min, val);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAFloat(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *(double *)sum = 0;
  *(double *)max = -FLT_MAX;
  *(double *)min = FLT_MAX;
  *numOfNull = 0;

  float val;
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      val = ((float *)pColData->pData)[iVal];
      CALC_SUM_MAX_MIN(*(double *)sum, *(double *)max, *(double *)min, val);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          (*numOfNull)++;
          break;
        case 2:
          val = ((float *)pColData->pData)[iVal];
          CALC_SUM_MAX_MIN(*(double *)sum, *(double *)max, *(double *)min, val);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMADouble(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *(double *)sum = 0;
  *(double *)max = -DBL_MAX;
  *(double *)min = DBL_MAX;
  *numOfNull = 0;

  double val;
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      val = ((double *)pColData->pData)[iVal];
      CALC_SUM_MAX_MIN(*(double *)sum, *(double *)max, *(double *)min, val);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          (*numOfNull)++;
          break;
        case 2:
          val = ((double *)pColData->pData)[iVal];
          CALC_SUM_MAX_MIN(*(double *)sum, *(double *)max, *(double *)min, val);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAUTinyInt(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *(uint64_t *)sum = 0;
  *(uint64_t *)max = 0;
  *(uint64_t *)min = UINT8_MAX;
  *numOfNull = 0;

  uint8_t val;
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      val = ((uint8_t *)pColData->pData)[iVal];
      CALC_SUM_MAX_MIN(*(uint64_t *)sum, *(uint64_t *)max, *(uint64_t *)min, val);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          (*numOfNull)++;
          break;
        case 2:
          val = ((uint8_t *)pColData->pData)[iVal];
          CALC_SUM_MAX_MIN(*(uint64_t *)sum, *(uint64_t *)max, *(uint64_t *)min, val);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMATinyUSmallInt(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *(uint64_t *)sum = 0;
  *(uint64_t *)max = 0;
  *(uint64_t *)min = UINT16_MAX;
  *numOfNull = 0;

  uint16_t val;
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      val = ((uint16_t *)pColData->pData)[iVal];
      CALC_SUM_MAX_MIN(*(uint64_t *)sum, *(uint64_t *)max, *(uint64_t *)min, val);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          (*numOfNull)++;
          break;
        case 2:
          val = ((uint16_t *)pColData->pData)[iVal];
          CALC_SUM_MAX_MIN(*(uint64_t *)sum, *(uint64_t *)max, *(uint64_t *)min, val);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAUInt(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *(uint64_t *)sum = 0;
  *(uint64_t *)max = 0;
  *(uint64_t *)min = UINT32_MAX;
  *numOfNull = 0;

  uint32_t val;
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      val = ((uint32_t *)pColData->pData)[iVal];
      CALC_SUM_MAX_MIN(*(uint64_t *)sum, *(uint64_t *)max, *(uint64_t *)min, val);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          (*numOfNull)++;
          break;
        case 2:
          val = ((uint32_t *)pColData->pData)[iVal];
          CALC_SUM_MAX_MIN(*(uint64_t *)sum, *(uint64_t *)max, *(uint64_t *)min, val);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAUBigInt(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *(uint64_t *)sum = 0;
  *(uint64_t *)max = 0;
  *(uint64_t *)min = UINT64_MAX;
  *numOfNull = 0;

  uint64_t val;
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      val = ((uint64_t *)pColData->pData)[iVal];
      CALC_SUM_MAX_MIN(*(uint64_t *)sum, *(uint64_t *)max, *(uint64_t *)min, val);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          (*numOfNull)++;
          break;
        case 2:
          val = ((uint64_t *)pColData->pData)[iVal];
          CALC_SUM_MAX_MIN(*(uint64_t *)sum, *(uint64_t *)max, *(uint64_t *)min, val);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAVarType(SColData *pColData, SColumnDataAgg *pAggs) {
  int64_t *sum = &pAggs->sum, *max = &pAggs->max, *min = &pAggs->min;
  int16_t *numOfNull = &pAggs->numOfNull;
  *(uint64_t *)sum = 0;
  *(uint64_t *)max = 0;
  *(uint64_t *)min = 0;
  *numOfNull = 0;

  switch (pColData->flag) {
    case HAS_NONE:
    case HAS_NULL:
    case (HAS_NONE | HAS_NULL):
      *numOfNull = pColData->nVal;
      break;
    case HAS_VALUE:
      *numOfNull = 0;
      break;
    case (HAS_VALUE | HAS_NULL):
    case (HAS_VALUE | HAS_NONE):
      for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
        if (GET_BIT1(pColData->pBitMap, iVal) == 0) {
          (*numOfNull)++;
        }
      }
      break;
    case (HAS_VALUE | HAS_NONE | HAS_NULL):
      for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
        if (GET_BIT2(pColData->pBitMap, iVal) != 2) {
          (*numOfNull)++;
        }
      }
      break;
    default:
      break;
  }
}

#define CALC_DECIMAL_SUM_MAX_MIN(TYPE, pSumOp, pCompOp, pColData, pSum, pMax, pMin)                   \
  do {                                                                                                \
    if (decimal128AddCheckOverflow((Decimal *)pSum, pVal, DECIMAL_WORD_NUM(TYPE))) *pOverflow = true; \
    pSumOp->add(pSum, pVal, DECIMAL_WORD_NUM(TYPE));                                                  \
    if (pCompOp->gt(pVal, pMax, DECIMAL_WORD_NUM(TYPE))) {                                            \
      *(pMax) = *pVal;                                                                                \
    }                                                                                                 \
    if (pCompOp->lt(pVal, pMin, DECIMAL_WORD_NUM(TYPE))) {                                            \
      *(pMin) = *pVal;                                                                                \
    }                                                                                                 \
  } while (0)

static FORCE_INLINE void tColDataCalcSMADecimal64Type(SColData *pColData, SColumnDataAgg *pAggs) {
  Decimal128 *pSum = (Decimal128 *)pAggs->decimal128Sum;
  Decimal64  *pMax = (Decimal64 *)pAggs->decimal128Max, *pMin = (Decimal64 *)pAggs->decimal128Min;
  uint8_t    *pOverflow = &pAggs->overflow;
  *pSum = DECIMAL128_ZERO;
  *pMax = DECIMAL64_MIN;
  *pMin = DECIMAL64_MAX;
  pAggs->numOfNull = 0;
  pAggs->colId |= DECIMAL_AGG_FLAG;

  Decimal64         *pVal = NULL;
  const SDecimalOps *pSumOps = getDecimalOps(TSDB_DATA_TYPE_DECIMAL);
  const SDecimalOps *pCompOps = getDecimalOps(TSDB_DATA_TYPE_DECIMAL64);
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; ++iVal) {
      pVal = ((Decimal64 *)pColData->pData) + iVal;
      CALC_DECIMAL_SUM_MAX_MIN(Decimal64, pSumOps, pCompOps, pColData, pSum, pMax, pMin);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; ++iVal) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          pAggs->numOfNull++;
          break;
        case 2:
          pVal = ((Decimal64 *)pColData->pData) + iVal;
          CALC_DECIMAL_SUM_MAX_MIN(Decimal64, pSumOps, pCompOps, pColData, pSum, pMax, pMin);
          break;
        default:
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMADecimal128Type(SColData *pColData, SColumnDataAgg *pAggs) {
  Decimal128 *pSum = (Decimal128 *)pAggs->decimal128Sum, *pMax = (Decimal128 *)pAggs->decimal128Max,
             *pMin = (Decimal128 *)pAggs->decimal128Min;
  uint8_t *pOverflow = &pAggs->overflow;
  *pSum = DECIMAL128_ZERO;
  *pMax = DECIMAL128_MIN;
  *pMin = DECIMAL128_MAX;
  pAggs->numOfNull = 0;
  pAggs->colId |= DECIMAL_AGG_FLAG;

  Decimal128        *pVal = NULL;
  const SDecimalOps *pOps = getDecimalOps(TSDB_DATA_TYPE_DECIMAL);
  if (HAS_VALUE == pColData->flag) {
    for (int32_t iVal = 0; iVal < pColData->nVal; ++iVal) {
      pVal = ((Decimal128 *)pColData->pData) + iVal;
      CALC_DECIMAL_SUM_MAX_MIN(Decimal128, pOps, pOps, pColData, pSum, pMax, pMin);
    }
  } else {
    for (int32_t iVal = 0; iVal < pColData->nVal; ++iVal) {
      switch (tColDataGetBitValue(pColData, iVal)) {
        case 0:
        case 1:
          pAggs->numOfNull++;
          break;
        case 2:
          pVal = ((Decimal128 *)pColData->pData) + iVal;
          CALC_DECIMAL_SUM_MAX_MIN(Decimal128, pOps, pOps, pColData, pSum, pMax, pMin);
          break;
        default:
          break;
      }
    }
  }
}

void (*tColDataCalcSMA[])(SColData *pColData, SColumnDataAgg *pAggs) = {
    NULL,
    tColDataCalcSMABool,            // TSDB_DATA_TYPE_BOOL
    tColDataCalcSMATinyInt,         // TSDB_DATA_TYPE_TINYINT
    tColDataCalcSMATinySmallInt,    // TSDB_DATA_TYPE_SMALLINT
    tColDataCalcSMAInt,             // TSDB_DATA_TYPE_INT
    tColDataCalcSMABigInt,          // TSDB_DATA_TYPE_BIGINT
    tColDataCalcSMAFloat,           // TSDB_DATA_TYPE_FLOAT
    tColDataCalcSMADouble,          // TSDB_DATA_TYPE_DOUBLE
    tColDataCalcSMAVarType,         // TSDB_DATA_TYPE_VARCHAR
    tColDataCalcSMABigInt,          // TSDB_DATA_TYPE_TIMESTAMP
    tColDataCalcSMAVarType,         // TSDB_DATA_TYPE_NCHAR
    tColDataCalcSMAUTinyInt,        // TSDB_DATA_TYPE_UTINYINT
    tColDataCalcSMATinyUSmallInt,   // TSDB_DATA_TYPE_USMALLINT
    tColDataCalcSMAUInt,            // TSDB_DATA_TYPE_UINT
    tColDataCalcSMAUBigInt,         // TSDB_DATA_TYPE_UBIGINT
    tColDataCalcSMAVarType,         // TSDB_DATA_TYPE_JSON
    tColDataCalcSMAVarType,         // TSDB_DATA_TYPE_VARBINARY
    tColDataCalcSMADecimal128Type,  // TSDB_DATA_TYPE_DECIMAL
    tColDataCalcSMAVarType,         // TSDB_DATA_TYPE_BLOB
    NULL,                           // TSDB_DATA_TYPE_MEDIUMBLOB
    tColDataCalcSMAVarType,         // TSDB_DATA_TYPE_GEOMETRY
    tColDataCalcSMADecimal64Type,   // TSDB_DATA_TYPE_DECIMAL64
};

// SValueColumn ================================
int32_t tValueColumnInit(SValueColumn *valCol) {
  valCol->type = TSDB_DATA_TYPE_NULL;
  valCol->numOfValues = 0;
  tBufferInit(&valCol->data);
  tBufferInit(&valCol->offsets);
  return 0;
}

void tValueColumnDestroy(SValueColumn *valCol) {
  valCol->type = TSDB_DATA_TYPE_NULL;
  valCol->numOfValues = 0;
  tBufferDestroy(&valCol->data);
  tBufferDestroy(&valCol->offsets);
  return;
}

void tValueColumnClear(SValueColumn *valCol) {
  valCol->type = TSDB_DATA_TYPE_NULL;
  valCol->numOfValues = 0;
  tBufferClear(&valCol->data);
  tBufferClear(&valCol->offsets);
  return;
}

int32_t tValueColumnAppend(SValueColumn *valCol, const SValue *value) {
  int32_t code;

  if (valCol->numOfValues == 0) {
    valCol->type = value->type;
  }

  if (!(value->type == valCol->type)) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (IS_VAR_DATA_TYPE(value->type)) {
    if ((code = tBufferPutI32(&valCol->offsets, tBufferGetSize(&valCol->data)))) {
      return code;
    }
    if ((code = tBufferPut(&valCol->data, value->pData, value->nData))) {
      return code;
    }
  } else {
    code = tBufferPut(&valCol->data, VALUE_GET_DATUM(value, value->type), tDataTypes[value->type].bytes);
    if (code) return code;
  }
  valCol->numOfValues++;

  return 0;
}

int32_t tValueColumnUpdate(SValueColumn *valCol, int32_t idx, const SValue *value) {
  int32_t code;

  if (idx < 0 || idx >= valCol->numOfValues) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  if (IS_VAR_DATA_TYPE(valCol->type)) {
    int32_t *offsets = (int32_t *)tBufferGetData(&valCol->offsets);
    int32_t  nextOffset = (idx == valCol->numOfValues - 1) ? tBufferGetSize(&valCol->data) : offsets[idx + 1];
    int32_t  oldDataSize = nextOffset - offsets[idx];
    int32_t  bytesAdded = value->nData - oldDataSize;

    if (bytesAdded != 0) {
      if ((code = tBufferEnsureCapacity(&valCol->data, tBufferGetSize(&valCol->data) + bytesAdded))) return code;
      memmove(tBufferGetDataAt(&valCol->data, nextOffset + bytesAdded), tBufferGetDataAt(&valCol->data, nextOffset),
              tBufferGetSize(&valCol->data) - nextOffset);
      valCol->data.size += bytesAdded;

      for (int32_t i = idx + 1; i < valCol->numOfValues; i++) {
        offsets[i] += bytesAdded;
      }
    }
    return tBufferPutAt(&valCol->data, offsets[idx], value->pData, value->nData);
  } else {
    return tBufferPutAt(&valCol->data, idx * tDataTypes[valCol->type].bytes, VALUE_GET_DATUM(value, valCol->type),
                        tDataTypes[valCol->type].bytes);
  }
  return 0;
}

int32_t tValueColumnGet(SValueColumn *valCol, int32_t idx, SValue *value) {
  if (idx < 0 || idx >= valCol->numOfValues) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  value->type = valCol->type;
  if (IS_VAR_DATA_TYPE(value->type)) {
    int32_t       offset, nextOffset;
    SBufferReader reader = BUFFER_READER_INITIALIZER(idx * sizeof(offset), &valCol->offsets);

    TAOS_CHECK_RETURN(tBufferGetI32(&reader, &offset));
    if (idx == valCol->numOfValues - 1) {
      nextOffset = tBufferGetSize(&valCol->data);
    } else {
      TAOS_CHECK_RETURN(tBufferGetI32(&reader, &nextOffset));
    }
    value->nData = nextOffset - offset;
    value->pData = (uint8_t *)tBufferGetDataAt(&valCol->data, offset);
  } else {
    SBufferReader reader = BUFFER_READER_INITIALIZER(idx * tDataTypes[value->type].bytes, &valCol->data);
    TAOS_CHECK_RETURN(tBufferGet(&reader, tDataTypes[value->type].bytes, VALUE_GET_DATUM(value, value->type)));
  }
  return 0;
}

int32_t tValueColumnCompress(SValueColumn *valCol, SValueColumnCompressInfo *info, SBuffer *output, SBuffer *assist) {
  int32_t code;

  if (!(valCol->numOfValues > 0)) {
    return TSDB_CODE_INVALID_PARA;
  }

  (*info) = (SValueColumnCompressInfo){
      .cmprAlg = info->cmprAlg,
      .type = valCol->type,
  };

  // offset
  if (IS_VAR_DATA_TYPE(valCol->type)) {
    SCompressInfo cinfo = {
        .cmprAlg = info->cmprAlg,
        .dataType = TSDB_DATA_TYPE_INT,
        .originalSize = valCol->offsets.size,
    };

    code = tCompressDataToBuffer(valCol->offsets.data, &cinfo, output, assist);
    if (code) return code;

    info->offsetOriginalSize = cinfo.originalSize;
    info->offsetCompressedSize = cinfo.compressedSize;
  }

  // data
  SCompressInfo cinfo = {
      .cmprAlg = info->cmprAlg,
      .dataType = valCol->type,
      .originalSize = valCol->data.size,
  };

  code = tCompressDataToBuffer(valCol->data.data, &cinfo, output, assist);
  if (code) return code;

  info->dataOriginalSize = cinfo.originalSize;
  info->dataCompressedSize = cinfo.compressedSize;

  return 0;
}

int32_t tValueColumnDecompress(void *input, const SValueColumnCompressInfo *info, SValueColumn *valCol,
                               SBuffer *assist) {
  int32_t code;

  tValueColumnClear(valCol);
  valCol->type = info->type;
  // offset
  if (IS_VAR_DATA_TYPE(valCol->type)) {
    valCol->numOfValues = info->offsetOriginalSize / tDataTypes[TSDB_DATA_TYPE_INT].bytes;

    SCompressInfo cinfo = {
        .dataType = TSDB_DATA_TYPE_INT,
        .cmprAlg = info->cmprAlg,
        .originalSize = info->offsetOriginalSize,
        .compressedSize = info->offsetCompressedSize,
    };

    code = tDecompressDataToBuffer(input, &cinfo, &valCol->offsets, assist);
    if (code) {
      return code;
    }
  } else {
    valCol->numOfValues = info->dataOriginalSize / tDataTypes[valCol->type].bytes;
  }

  // data
  SCompressInfo cinfo = {
      .dataType = valCol->type,
      .cmprAlg = info->cmprAlg,
      .originalSize = info->dataOriginalSize,
      .compressedSize = info->dataCompressedSize,
  };

  code = tDecompressDataToBuffer((char *)input + info->offsetCompressedSize, &cinfo, &valCol->data, assist);
  if (code) {
    return code;
  }

  return 0;
}

int32_t tValueColumnCompressInfoEncode(const SValueColumnCompressInfo *info, SBuffer *buffer) {
  int32_t code;
  uint8_t fmtVer = 0;

  if ((code = tBufferPutU8(buffer, fmtVer))) return code;
  if ((code = tBufferPutI8(buffer, info->cmprAlg))) return code;
  if ((code = tBufferPutI8(buffer, info->type))) return code;
  if (IS_VAR_DATA_TYPE(info->type)) {
    if ((code = tBufferPutI32v(buffer, info->offsetOriginalSize))) return code;
    if ((code = tBufferPutI32v(buffer, info->offsetCompressedSize))) return code;
  }
  if ((code = tBufferPutI32v(buffer, info->dataOriginalSize))) return code;
  if ((code = tBufferPutI32v(buffer, info->dataCompressedSize))) return code;

  return 0;
}

int32_t tValueColumnCompressInfoDecode(SBufferReader *reader, SValueColumnCompressInfo *info) {
  int32_t code;
  uint8_t fmtVer;

  if ((code = tBufferGetU8(reader, &fmtVer))) return code;
  if (fmtVer == 0) {
    if ((code = tBufferGetI8(reader, &info->cmprAlg))) return code;
    if ((code = tBufferGetI8(reader, &info->type))) return code;
    if (IS_VAR_DATA_TYPE(info->type)) {
      if ((code = tBufferGetI32v(reader, &info->offsetOriginalSize))) return code;
      if ((code = tBufferGetI32v(reader, &info->offsetCompressedSize))) return code;
    } else {
      info->offsetOriginalSize = 0;
      info->offsetCompressedSize = 0;
    }
    if ((code = tBufferGetI32v(reader, &info->dataOriginalSize))) return code;
    if ((code = tBufferGetI32v(reader, &info->dataCompressedSize))) return code;
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  return 0;
}

int32_t tCompressData(void          *input,       // input
                      SCompressInfo *info,        // compress info
                      void          *output,      // output
                      int32_t        outputSize,  // output size
                      SBuffer       *buffer       // assistant buffer provided by caller, can be NULL
) {
  int32_t extraSizeNeeded;
  int32_t code;

  extraSizeNeeded = (info->cmprAlg == NO_COMPRESSION) ? info->originalSize : info->originalSize + COMP_OVERFLOW_BYTES;
  if (!(outputSize >= extraSizeNeeded)) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (info->cmprAlg == NO_COMPRESSION) {
    (void)memcpy(output, input, info->originalSize);
    info->compressedSize = info->originalSize;
  } else if (info->cmprAlg == ONE_STAGE_COMP || info->cmprAlg == TWO_STAGE_COMP) {
    SBuffer local;

    tBufferInit(&local);
    if (buffer == NULL) {
      buffer = &local;
    }

    if (info->cmprAlg == TWO_STAGE_COMP) {
      code = tBufferEnsureCapacity(buffer, extraSizeNeeded);
      if (code) {
        tBufferDestroy(&local);
        return code;
      }
    }

    info->compressedSize = tDataTypes[info->dataType].compFunc(  //
        input,                                                   // input
        info->originalSize,                                      // input size
        info->originalSize / tDataTypes[info->dataType].bytes,   // number of elements
        output,                                                  // output
        outputSize,                                              // output size
        info->cmprAlg,                                           // compression algorithm
        buffer->data,                                            // buffer
        buffer->capacity                                         // buffer size
    );
    if (info->compressedSize < 0) {
      tBufferDestroy(&local);
      return TSDB_CODE_COMPRESS_ERROR;
    }

    tBufferDestroy(&local);
  } else {
    DEFINE_VAR(info->cmprAlg)
    if ((l1 == L1_UNKNOWN && l2 == L2_UNKNOWN) || (l1 == L1_DISABLED && l2 == L2_DISABLED)) {
      (void)memcpy(output, input, info->originalSize);
      info->compressedSize = info->originalSize;
      return 0;
    }
    SBuffer local;

    tBufferInit(&local);
    if (buffer == NULL) {
      buffer = &local;
    }
    code = tBufferEnsureCapacity(buffer, extraSizeNeeded);

    info->compressedSize = tDataCompress[info->dataType].compFunc(  //
        input,                                                      // input
        info->originalSize,                                         // input size
        info->originalSize / tDataTypes[info->dataType].bytes,      // number of elements
        output,                                                     // output
        outputSize,                                                 // output size
        info->cmprAlg,                                              // compression algorithm
        buffer->data,                                               // buffer
        buffer->capacity                                            // buffer size
    );
    if (info->compressedSize < 0) {
      tBufferDestroy(&local);
      return TSDB_CODE_COMPRESS_ERROR;
    }

    tBufferDestroy(&local);
    // new col compress
  }

  return 0;
}

int32_t tDecompressData(void                *input,       // input
                        const SCompressInfo *info,        // compress info
                        void                *output,      // output
                        int32_t              outputSize,  // output size
                        SBuffer             *buffer       // assistant buffer provided by caller, can be NULL
) {
  int32_t code;

  if (!(outputSize >= info->originalSize)) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (info->cmprAlg == NO_COMPRESSION) {
    if (!(info->compressedSize == info->originalSize)) {
      return TSDB_CODE_INVALID_PARA;
    }
    (void)memcpy(output, input, info->compressedSize);
  } else if (info->cmprAlg == ONE_STAGE_COMP || info->cmprAlg == TWO_STAGE_COMP) {
    SBuffer local;

    tBufferInit(&local);
    if (buffer == NULL) {
      buffer = &local;
    }

    if (info->cmprAlg == TWO_STAGE_COMP) {
      code = tBufferEnsureCapacity(buffer, info->originalSize + COMP_OVERFLOW_BYTES);
      if (code) {
        tBufferDestroy(&local);
        return code;
      }
    }

    int32_t decompressedSize = tDataTypes[info->dataType].decompFunc(
        input,                                                  // input
        info->compressedSize,                                   // inputSize
        info->originalSize / tDataTypes[info->dataType].bytes,  // number of elements
        output,                                                 // output
        outputSize,                                             // output size
        info->cmprAlg,                                          // compression algorithm
        buffer->data,                                           // helper buffer
        buffer->capacity                                        // extra buffer size
    );
    if (decompressedSize < 0) {
      tBufferDestroy(&local);
      return TSDB_CODE_COMPRESS_ERROR;
    }

    if (!(decompressedSize == info->originalSize)) {
      return TSDB_CODE_COMPRESS_ERROR;
    }
    tBufferDestroy(&local);
  } else {
    DEFINE_VAR(info->cmprAlg);
    if (l1 == L1_DISABLED && l2 == L2_DISABLED) {
      (void)memcpy(output, input, info->compressedSize);
      return 0;
    }
    SBuffer local;

    tBufferInit(&local);
    if (buffer == NULL) {
      buffer = &local;
    }
    code = tBufferEnsureCapacity(buffer, info->originalSize + COMP_OVERFLOW_BYTES);
    if (code) {
      return code;
    }

    int32_t decompressedSize = tDataCompress[info->dataType].decompFunc(
        input,                                                  // input
        info->compressedSize,                                   // inputSize
        info->originalSize / tDataTypes[info->dataType].bytes,  // number of elements
        output,                                                 // output
        outputSize,                                             // output size
        info->cmprAlg,                                          // compression algorithm
        buffer->data,                                           // helper buffer
        buffer->capacity                                        // extra buffer size
    );
    if (decompressedSize < 0) {
      tBufferDestroy(&local);
      return TSDB_CODE_COMPRESS_ERROR;
    }

    if (!(decompressedSize == info->originalSize)) {
      return TSDB_CODE_COMPRESS_ERROR;
    }
    tBufferDestroy(&local);
  }

  return 0;
}

int32_t tCompressDataToBuffer(void *input, SCompressInfo *info, SBuffer *output, SBuffer *assist) {
  int32_t code;

  code = tBufferEnsureCapacity(output, output->size + info->originalSize + COMP_OVERFLOW_BYTES);
  if (code) return code;

  code = tCompressData(input, info, tBufferGetDataEnd(output), output->capacity - output->size, assist);
  if (code) return code;

  output->size += info->compressedSize;
  return 0;
}

int32_t tDecompressDataToBuffer(void *input, SCompressInfo *info, SBuffer *output, SBuffer *assist) {
  int32_t code;

  code = tBufferEnsureCapacity(output, output->size + info->originalSize);
  if (code) return code;

  code = tDecompressData(input, info, tBufferGetDataEnd(output), output->capacity - output->size, assist);
  if (code) return code;

  output->size += info->originalSize;
  return 0;
}

// handle all types, including var data
void valueSetDatum(SValue *pVal, int8_t type, void *pDatum, uint32_t len) {
  if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_DECIMAL) {
    pVal->pData = pDatum;
    pVal->nData = len;
  } else {
    switch (len) {
      case sizeof(uint8_t):
        pVal->val = *(uint8_t *)pDatum;
        break;
      case sizeof(uint16_t):
        pVal->val = *(uint16_t *)pDatum;
        break;
      case sizeof(uint32_t):
        pVal->val = *(uint32_t *)pDatum;
        break;
      case sizeof(uint64_t):
        pVal->val = *(uint64_t *)pDatum;
        break;
      default:
        break;
    }
  }
}

void valueCloneDatum(SValue *pDst, const SValue *pSrc, int8_t type) {
  if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_DECIMAL) {
    memcpy(pDst->pData, pSrc->pData, pSrc->nData);
    pDst->nData = pSrc->nData;
  } else {
    pDst->val = pSrc->val;
  }
}
void valueClearDatum(SValue *pVal, int8_t type) {
  if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_DECIMAL) {
    taosMemoryFreeClear(pVal->pData);
    pVal->nData = 0;
  } else {
    pVal->val = 0;
  }
}

int8_t schemaHasBlob(const STSchema *pSchema) {
  if (pSchema == NULL) {
    return 0;
  }
  for (int i = 0; i < pSchema->numOfCols; ++i) {
    if (IS_STR_DATA_BLOB(pSchema->columns[i].type)) {
      return 1;
    }
  }
  return 0;
}
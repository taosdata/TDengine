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
#include "tcoding.h"
#include "tdatablock.h"
#include "tlog.h"

int32_t tEncodeTSRow(SEncoder *pEncoder, const STSRow2 *pRow) {
  if (tEncodeI64(pEncoder, pRow->ts) < 0) return -1;
  if (tEncodeU32v(pEncoder, pRow->flags) < 0) return -1;
  if (pRow->flags & TD_KV_ROW) {
    if (tEncodeI32v(pEncoder, pRow->ncols) < 0) return -1;
  } else {
    if (tEncodeI32v(pEncoder, pRow->sver) < 0) return -1;
  }
  if (tEncodeBinary(pEncoder, pRow->pData, pRow->nData) < 0) return -1;
  return 0;
}

int32_t tDecodeTSRow(SDecoder *pDecoder, STSRow2 *pRow) {
  if (tDecodeI64(pDecoder, &pRow->ts) < 0) return -1;
  if (tDecodeU32v(pDecoder, &pRow->flags) < 0) return -1;
  if (pRow->flags & TD_KV_ROW) {
    if (tDecodeI32v(pDecoder, &pRow->ncols) < 0) return -1;
  } else {
    if (tDecodeI32v(pDecoder, &pRow->sver) < 0) return -1;
  }
  if (tDecodeBinary(pDecoder, &pRow->pData, &pRow->nData) < 0) return -1;
  return 0;
}

int32_t tTSchemaCreate(int32_t sver, SSchema *pSchema, int32_t ncols, STSchema **ppTSchema) {
  *ppTSchema = (STSchema *)taosMemoryMalloc(sizeof(STSchema) + sizeof(STColumn) * ncols);
  if (*ppTSchema == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  (*ppTSchema)->numOfCols = ncols;
  (*ppTSchema)->version = sver;
  (*ppTSchema)->flen = 0;
  (*ppTSchema)->vlen = 0;
  (*ppTSchema)->tlen = 0;

  for (int32_t iCol = 0; iCol < ncols; iCol++) {
    SSchema  *pColumn = &pSchema[iCol];
    STColumn *pTColumn = &((*ppTSchema)->columns[iCol]);

    pTColumn->colId = pColumn->colId;
    pTColumn->type = pColumn->type;
    pTColumn->flags = pColumn->flags;
    pTColumn->bytes = pColumn->bytes;
    pTColumn->offset = (*ppTSchema)->flen;

    // skip first column
    if (iCol) {
      (*ppTSchema)->flen += TYPE_BYTES[pColumn->type];
      if (IS_VAR_DATA_TYPE(pColumn->type)) {
        (*ppTSchema)->vlen += (pColumn->bytes + 5);
      }
    }
  }

  return 0;
}

void tTSchemaDestroy(STSchema *pTSchema) { taosMemoryFree(pTSchema); }

int32_t tTSRowBuilderInit(STSRowBuilder *pBuilder, int32_t sver, SSchema *pSchema, int32_t nCols) {
  // TODO
  return 0;
}

int32_t tTSRowBuilderClear(STSRowBuilder *pBuilder) {
  // TODO
  return 0;
}

int32_t tTSRowBuilderReset(STSRowBuilder *pBuilder) {
  for (int32_t iCol = pBuilder->pTSchema->numOfCols - 1; iCol >= 0; iCol--) {
    pBuilder->pTColumn = &pBuilder->pTSchema->columns[iCol];

    pBuilder->pTColumn->flags &= (~COL_VAL_SET);
  }

  return 0;
}

int32_t tTSRowBuilderPut(STSRowBuilder *pBuilder, int32_t cid, const uint8_t *pData, uint32_t nData) {
  if (pBuilder->pTColumn->colId < cid) {
    // right search
  } else if (pBuilder->pTColumn->colId > cid) {
    // left search
  }

  // check if val is set already
  if (pBuilder->pTColumn->flags & COL_VAL_SET) {
    return -1;
  }

  // set value
  if (cid == 0) {
    ASSERT(pData && nData == sizeof(TSKEY));
    pBuilder->row.ts = *(TSKEY *)pData;
  } else {
    if (pData) {
      // set val
    } else {
      // set NULL val
    }
  }

  pBuilder->pTColumn->flags |= COL_VAL_SET;

  // TODO
  return 0;
}

int32_t tTSRowBuilderGetRow(STSRowBuilder *pBuilder, const STSRow2 **ppRow) {
  if ((pBuilder->pTSchema->columns[0].flags & COL_VAL_SET) == 0) {
    return -1;
  }

  // chose which type row to return

  if (true /* tuple row is chosen */) {
    // set non-set values as None
  }

  *ppRow = &pBuilder->row;
  return 0;
}

#if 1  // ====================
static void dataColSetNEleNull(SDataCol *pCol, int nEle);
int         tdAllocMemForCol(SDataCol *pCol, int maxPoints) {
  int spaceNeeded = pCol->bytes * maxPoints;
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    spaceNeeded += sizeof(VarDataOffsetT) * maxPoints;
  }
#ifdef TD_SUPPORT_BITMAP
  int32_t nBitmapBytes = (int32_t)TD_BITMAP_BYTES(maxPoints);
  spaceNeeded += (int)nBitmapBytes;
  // TODO: Currently, the compression of bitmap parts is affiliated to the column data parts, thus allocate 1 more
  // TYPE_BYTES as to comprise complete TYPE_BYTES. Otherwise, invalid read/write would be triggered.
  // spaceNeeded += TYPE_BYTES[pCol->type]; // the bitmap part is append as a single part since 2022.04.03, thus remove
  // the additional space
#endif

  if (pCol->spaceSize < spaceNeeded) {
    void *ptr = taosMemoryRealloc(pCol->pData, spaceNeeded);
    if (ptr == NULL) {
      uDebug("malloc failure, size:%" PRId64 " failed, reason:%s", (int64_t)spaceNeeded, strerror(errno));
      return -1;
    } else {
      pCol->pData = ptr;
      pCol->spaceSize = spaceNeeded;
    }
  }
#ifdef TD_SUPPORT_BITMAP

  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pCol->pBitmap = POINTER_SHIFT(pCol->pData, pCol->bytes * maxPoints);
    pCol->dataOff = POINTER_SHIFT(pCol->pBitmap, nBitmapBytes);
  } else {
    pCol->pBitmap = POINTER_SHIFT(pCol->pData, pCol->bytes * maxPoints);
  }
#else
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pCol->dataOff = POINTER_SHIFT(pCol->pData, pCol->bytes * maxPoints);
  }
#endif
  return 0;
}

/**
 * Duplicate the schema and return a new object
 */
STSchema *tdDupSchema(const STSchema *pSchema) {
  int       tlen = sizeof(STSchema) + sizeof(STColumn) * schemaNCols(pSchema);
  STSchema *tSchema = (STSchema *)taosMemoryMalloc(tlen);
  if (tSchema == NULL) return NULL;

  memcpy((void *)tSchema, (void *)pSchema, tlen);

  return tSchema;
}

/**
 * Encode a schema to dst, and return the next pointer
 */
int tdEncodeSchema(void **buf, STSchema *pSchema) {
  int tlen = 0;
  tlen += taosEncodeFixedI32(buf, schemaVersion(pSchema));
  tlen += taosEncodeFixedI32(buf, schemaNCols(pSchema));

  for (int i = 0; i < schemaNCols(pSchema); i++) {
    STColumn *pCol = schemaColAt(pSchema, i);
    tlen += taosEncodeFixedI8(buf, colType(pCol));
    tlen += taosEncodeFixedI8(buf, colFlags(pCol));
    tlen += taosEncodeFixedI16(buf, colColId(pCol));
    tlen += taosEncodeFixedI16(buf, colBytes(pCol));
  }

  return tlen;
}

/**
 * Decode a schema from a binary.
 */
void *tdDecodeSchema(void *buf, STSchema **pRSchema) {
  int             version = 0;
  int             numOfCols = 0;
  STSchemaBuilder schemaBuilder;

  buf = taosDecodeFixedI32(buf, &version);
  buf = taosDecodeFixedI32(buf, &numOfCols);

  if (tdInitTSchemaBuilder(&schemaBuilder, version) < 0) return NULL;

  for (int i = 0; i < numOfCols; i++) {
    col_type_t  type = 0;
    int8_t      flags = 0;
    col_id_t    colId = 0;
    col_bytes_t bytes = 0;
    buf = taosDecodeFixedI8(buf, &type);
    buf = taosDecodeFixedI8(buf, &flags);
    buf = taosDecodeFixedI16(buf, &colId);
    buf = taosDecodeFixedI32(buf, &bytes);
    if (tdAddColToSchema(&schemaBuilder, type, flags, colId, bytes) < 0) {
      tdDestroyTSchemaBuilder(&schemaBuilder);
      return NULL;
    }
  }

  *pRSchema = tdGetSchemaFromBuilder(&schemaBuilder);
  tdDestroyTSchemaBuilder(&schemaBuilder);
  return buf;
}

int tdInitTSchemaBuilder(STSchemaBuilder *pBuilder, schema_ver_t version) {
  if (pBuilder == NULL) return -1;

  pBuilder->tCols = 256;
  pBuilder->columns = (STColumn *)taosMemoryMalloc(sizeof(STColumn) * pBuilder->tCols);
  if (pBuilder->columns == NULL) return -1;

  tdResetTSchemaBuilder(pBuilder, version);
  return 0;
}

void tdDestroyTSchemaBuilder(STSchemaBuilder *pBuilder) {
  if (pBuilder) {
    taosMemoryFreeClear(pBuilder->columns);
  }
}

void tdResetTSchemaBuilder(STSchemaBuilder *pBuilder, schema_ver_t version) {
  pBuilder->nCols = 0;
  pBuilder->tlen = 0;
  pBuilder->flen = 0;
  pBuilder->vlen = 0;
  pBuilder->version = version;
}

int32_t tdAddColToSchema(STSchemaBuilder *pBuilder, int8_t type, int8_t flags, col_id_t colId, col_bytes_t bytes) {
  if (!isValidDataType(type)) return -1;

  if (pBuilder->nCols >= pBuilder->tCols) {
    pBuilder->tCols *= 2;
    STColumn *columns = (STColumn *)taosMemoryRealloc(pBuilder->columns, sizeof(STColumn) * pBuilder->tCols);
    if (columns == NULL) return -1;
    pBuilder->columns = columns;
  }

  STColumn *pCol = &(pBuilder->columns[pBuilder->nCols]);
  colSetType(pCol, type);
  colSetColId(pCol, colId);
  colSetFlags(pCol, flags);
  if (pBuilder->nCols == 0) {
    colSetOffset(pCol, 0);
  } else {
    STColumn *pTCol = &(pBuilder->columns[pBuilder->nCols - 1]);
    colSetOffset(pCol, pTCol->offset + TYPE_BYTES[pTCol->type]);
  }

  if (IS_VAR_DATA_TYPE(type)) {
    colSetBytes(pCol, bytes);
    pBuilder->tlen += (TYPE_BYTES[type] + bytes);
    pBuilder->vlen += bytes - sizeof(VarDataLenT);
  } else {
    colSetBytes(pCol, TYPE_BYTES[type]);
    pBuilder->tlen += TYPE_BYTES[type];
    pBuilder->vlen += TYPE_BYTES[type];
  }

  pBuilder->nCols++;
  pBuilder->flen += TYPE_BYTES[type];

  ASSERT(pCol->offset < pBuilder->flen);

  return 0;
}

STSchema *tdGetSchemaFromBuilder(STSchemaBuilder *pBuilder) {
  if (pBuilder->nCols <= 0) return NULL;

  int tlen = sizeof(STSchema) + sizeof(STColumn) * pBuilder->nCols;

  STSchema *pSchema = (STSchema *)taosMemoryMalloc(tlen);
  if (pSchema == NULL) return NULL;

  schemaVersion(pSchema) = pBuilder->version;
  schemaNCols(pSchema) = pBuilder->nCols;
  schemaTLen(pSchema) = pBuilder->tlen;
  schemaFLen(pSchema) = pBuilder->flen;
  schemaVLen(pSchema) = pBuilder->vlen;

#ifdef TD_SUPPORT_BITMAP
  schemaTLen(pSchema) += (int)TD_BITMAP_BYTES(schemaNCols(pSchema));
#endif

  memcpy(schemaColAt(pSchema, 0), pBuilder->columns, sizeof(STColumn) * pBuilder->nCols);

  return pSchema;
}

void dataColInit(SDataCol *pDataCol, STColumn *pCol, int maxPoints) {
  pDataCol->type = colType(pCol);
  pDataCol->colId = colColId(pCol);
  pDataCol->bytes = colBytes(pCol);
  pDataCol->offset = colOffset(pCol) + 0;  // TD_DATA_ROW_HEAD_SIZE;

  pDataCol->len = 0;
}

static FORCE_INLINE const void *tdGetColDataOfRowUnsafe(SDataCol *pCol, int row) {
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    return POINTER_SHIFT(pCol->pData, pCol->dataOff[row]);
  } else {
    return POINTER_SHIFT(pCol->pData, TYPE_BYTES[pCol->type] * row);
  }
}

bool isNEleNull(SDataCol *pCol, int nEle) {
  if (isAllRowsNull(pCol)) return true;
  for (int i = 0; i < nEle; ++i) {
    if (!isNull(tdGetColDataOfRowUnsafe(pCol, i), pCol->type)) return false;
  }
  return true;
}

void *dataColSetOffset(SDataCol *pCol, int nEle) {
  ASSERT(((pCol->type == TSDB_DATA_TYPE_BINARY) || (pCol->type == TSDB_DATA_TYPE_NCHAR)));

  void *tptr = pCol->pData;
  // char *tptr = (char *)(pCol->pData);

  VarDataOffsetT offset = 0;
  for (int i = 0; i < nEle; ++i) {
    pCol->dataOff[i] = offset;
    offset += varDataTLen(tptr);
    tptr = POINTER_SHIFT(tptr, varDataTLen(tptr));
  }
  return POINTER_SHIFT(tptr, varDataTLen(tptr));
}

SDataCols *tdNewDataCols(int maxCols, int maxRows) {
  SDataCols *pCols = (SDataCols *)taosMemoryCalloc(1, sizeof(SDataCols));
  if (pCols == NULL) {
    uDebug("malloc failure, size:%" PRId64 " failed, reason:%s", (int64_t)sizeof(SDataCols), strerror(errno));
    return NULL;
  }

  pCols->maxPoints = maxRows;
  pCols->maxCols = maxCols;
  pCols->numOfRows = 0;
  pCols->numOfCols = 0;
  // pCols->bitmapMode = 0; // calloc already set 0

  if (maxCols > 0) {
    pCols->cols = (SDataCol *)taosMemoryCalloc(maxCols, sizeof(SDataCol));
    if (pCols->cols == NULL) {
      uDebug("malloc failure, size:%" PRId64 " failed, reason:%s", (int64_t)sizeof(SDataCol) * maxCols,
             strerror(errno));
      tdFreeDataCols(pCols);
      return NULL;
    }
#if 0  // no need as calloc used
    int i;
    for (i = 0; i < maxCols; i++) {
      pCols->cols[i].spaceSize = 0;
      pCols->cols[i].len = 0;
      pCols->cols[i].pData = NULL;
      pCols->cols[i].dataOff = NULL;
    }
#endif
  }

  return pCols;
}

int tdInitDataCols(SDataCols *pCols, STSchema *pSchema) {
  int i;
  int oldMaxCols = pCols->maxCols;
  if (schemaNCols(pSchema) > oldMaxCols) {
    pCols->maxCols = schemaNCols(pSchema);
    void *ptr = (SDataCol *)taosMemoryRealloc(pCols->cols, sizeof(SDataCol) * pCols->maxCols);
    if (ptr == NULL) return -1;
    pCols->cols = ptr;
    for (i = oldMaxCols; i < pCols->maxCols; ++i) {
      pCols->cols[i].pData = NULL;
      pCols->cols[i].dataOff = NULL;
      pCols->cols[i].pBitmap = NULL;
      pCols->cols[i].spaceSize = 0;
    }
  }
#if 0
  tdResetDataCols(pCols); // redundant loop to reset len/blen to 0, already reset in following dataColInit(...)
#endif

  pCols->numOfRows = 0;
  pCols->bitmapMode = 0;
  pCols->numOfCols = schemaNCols(pSchema);

  for (i = 0; i < schemaNCols(pSchema); ++i) {
    dataColInit(pCols->cols + i, schemaColAt(pSchema, i), pCols->maxPoints);
  }

  return 0;
}

SDataCols *tdFreeDataCols(SDataCols *pCols) {
  int i;
  if (pCols) {
    if (pCols->cols) {
      int maxCols = pCols->maxCols;
      for (i = 0; i < maxCols; ++i) {
        SDataCol *pCol = &pCols->cols[i];
        taosMemoryFreeClear(pCol->pData);
      }
      taosMemoryFree(pCols->cols);
      pCols->cols = NULL;
    }
    taosMemoryFree(pCols);
  }
  return NULL;
}

void tdResetDataCols(SDataCols *pCols) {
  if (pCols != NULL) {
    pCols->numOfRows = 0;
    pCols->bitmapMode = 0;
    for (int i = 0; i < pCols->maxCols; ++i) {
      dataColReset(pCols->cols + i);
    }
  }
}

SKVRow tdKVRowDup(SKVRow row) {
  SKVRow trow = taosMemoryMalloc(kvRowLen(row));
  if (trow == NULL) return NULL;

  kvRowCpy(trow, row);
  return trow;
}

static int compareColIdx(const void *a, const void *b) {
  const SColIdx *x = (const SColIdx *)a;
  const SColIdx *y = (const SColIdx *)b;
  if (x->colId > y->colId) {
    return 1;
  }
  if (x->colId < y->colId) {
    return -1;
  }
  return 0;
}

void tdSortKVRowByColIdx(SKVRow row) { qsort(kvRowColIdx(row), kvRowNCols(row), sizeof(SColIdx), compareColIdx); }

int tdSetKVRowDataOfCol(SKVRow *orow, int16_t colId, int8_t type, void *value) {
  SColIdx *pColIdx = NULL;
  SKVRow   row = *orow;
  SKVRow   nrow = NULL;
  void    *ptr = taosbsearch(&colId, kvRowColIdx(row), kvRowNCols(row), sizeof(SColIdx), comparTagId, TD_GE);

  if (ptr == NULL || ((SColIdx *)ptr)->colId > colId) {  // need to add a column value to the row
    int diff = IS_VAR_DATA_TYPE(type) ? varDataTLen(value) : TYPE_BYTES[type];
    int nRowLen = kvRowLen(row) + sizeof(SColIdx) + diff;
    int oRowCols = kvRowNCols(row);

    ASSERT(diff > 0);
    nrow = taosMemoryMalloc(nRowLen);
    if (nrow == NULL) return -1;

    kvRowSetLen(nrow, nRowLen);
    kvRowSetNCols(nrow, oRowCols + 1);

    memcpy(kvRowColIdx(nrow), kvRowColIdx(row), sizeof(SColIdx) * oRowCols);
    memcpy(kvRowValues(nrow), kvRowValues(row), kvRowValLen(row));

    pColIdx = kvRowColIdxAt(nrow, oRowCols);
    pColIdx->colId = colId;
    pColIdx->offset = kvRowValLen(row);

    memcpy(kvRowColVal(nrow, pColIdx), value, diff);  // copy new value

    tdSortKVRowByColIdx(nrow);

    *orow = nrow;
    taosMemoryFree(row);
  } else {
    ASSERT(((SColIdx *)ptr)->colId == colId);
    if (IS_VAR_DATA_TYPE(type)) {
      void *pOldVal = kvRowColVal(row, (SColIdx *)ptr);

      if (varDataTLen(value) == varDataTLen(pOldVal)) {  // just update the column value in place
        memcpy(pOldVal, value, varDataTLen(value));
      } else {  // need to reallocate the memory
        int16_t nlen = kvRowLen(row) + (varDataTLen(value) - varDataTLen(pOldVal));
        ASSERT(nlen > 0);
        nrow = taosMemoryMalloc(nlen);
        if (nrow == NULL) return -1;

        kvRowSetLen(nrow, nlen);
        kvRowSetNCols(nrow, kvRowNCols(row));

        int zsize = sizeof(SColIdx) * kvRowNCols(row) + ((SColIdx *)ptr)->offset;
        memcpy(kvRowColIdx(nrow), kvRowColIdx(row), zsize);
        memcpy(kvRowColVal(nrow, ((SColIdx *)ptr)), value, varDataTLen(value));
        // Copy left value part
        int lsize = kvRowLen(row) - TD_KV_ROW_HEAD_SIZE - zsize - varDataTLen(pOldVal);
        if (lsize > 0) {
          memcpy(POINTER_SHIFT(nrow, TD_KV_ROW_HEAD_SIZE + zsize + varDataTLen(value)),
                 POINTER_SHIFT(row, TD_KV_ROW_HEAD_SIZE + zsize + varDataTLen(pOldVal)), lsize);
        }

        for (int i = 0; i < kvRowNCols(nrow); i++) {
          pColIdx = kvRowColIdxAt(nrow, i);

          if (pColIdx->offset > ((SColIdx *)ptr)->offset) {
            pColIdx->offset = pColIdx->offset - varDataTLen(pOldVal) + varDataTLen(value);
          }
        }

        *orow = nrow;
        taosMemoryFree(row);
      }
    } else {
      memcpy(kvRowColVal(row, (SColIdx *)ptr), value, TYPE_BYTES[type]);
    }
  }

  return 0;
}

int tdEncodeKVRow(void **buf, SKVRow row) {
  // May change the encode purpose
  if (buf != NULL) {
    kvRowCpy(*buf, row);
    *buf = POINTER_SHIFT(*buf, kvRowLen(row));
  }

  return kvRowLen(row);
}

void *tdDecodeKVRow(void *buf, SKVRow *row) {
  *row = tdKVRowDup(buf);
  if (*row == NULL) return NULL;
  return POINTER_SHIFT(buf, kvRowLen(*row));
}

int tdInitKVRowBuilder(SKVRowBuilder *pBuilder) {
  pBuilder->tCols = 128;
  pBuilder->nCols = 0;
  pBuilder->pColIdx = (SColIdx *)taosMemoryMalloc(sizeof(SColIdx) * pBuilder->tCols);
  if (pBuilder->pColIdx == NULL) return -1;
  pBuilder->alloc = 1024;
  pBuilder->size = 0;
  pBuilder->buf = taosMemoryMalloc(pBuilder->alloc);
  if (pBuilder->buf == NULL) {
    taosMemoryFree(pBuilder->pColIdx);
    return -1;
  }
  return 0;
}

void tdDestroyKVRowBuilder(SKVRowBuilder *pBuilder) {
  taosMemoryFreeClear(pBuilder->pColIdx);
  taosMemoryFreeClear(pBuilder->buf);
}

void tdResetKVRowBuilder(SKVRowBuilder *pBuilder) {
  pBuilder->nCols = 0;
  pBuilder->size = 0;
}

SKVRow tdGetKVRowFromBuilder(SKVRowBuilder *pBuilder) {
  int tlen = sizeof(SColIdx) * pBuilder->nCols + pBuilder->size;
  if (tlen == 0) return NULL;

  tlen += TD_KV_ROW_HEAD_SIZE;

  SKVRow row = taosMemoryMalloc(tlen);
  if (row == NULL) return NULL;

  kvRowSetNCols(row, pBuilder->nCols);
  kvRowSetLen(row, tlen);

  memcpy(kvRowColIdx(row), pBuilder->pColIdx, sizeof(SColIdx) * pBuilder->nCols);
  memcpy(kvRowValues(row), pBuilder->buf, pBuilder->size);

  return row;
}
#endif
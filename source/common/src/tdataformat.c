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

typedef struct SKVIdx {
  int32_t cid;
  int32_t offset;
} SKVIdx;

#pragma pack(push, 1)
typedef struct {
  int16_t nCols;
  SKVIdx  idx[];
} STSKVRow;
#pragma pack(pop)

typedef struct STagIdx {
  int16_t  cid;
  uint16_t offset;
} STagIdx;

#pragma pack(push, 1)
struct STag {
  uint16_t len;
  uint16_t nTag;
  STagIdx  idx[];
};
#pragma pack(pop)

#define TSROW_IS_KV_ROW(r) ((r)->flags & TSROW_KV_ROW)
#define BIT1_SIZE(n)       (((n)-1) / 8 + 1)
#define BIT2_SIZE(n)       (((n)-1) / 4 + 1)
#define SET_BIT1(p, i, v)  ((p)[(i) / 8] = (p)[(i) / 8] & (~(((uint8_t)1) << ((i) % 8))) | ((v) << ((i) % 8)))
#define SET_BIT2(p, i, v)  ((p)[(i) / 4] = (p)[(i) / 4] & (~(((uint8_t)3) << ((i) % 4))) | ((v) << ((i) % 4)))
#define GET_BIT1(p, i)     (((p)[(i) / 8] >> ((i) % 8)) & ((uint8_t)1))
#define GET_BIT2(p, i)     (((p)[(i) / 4] >> ((i) % 4)) & ((uint8_t)3))

static FORCE_INLINE int tSKVIdxCmprFn(const void *p1, const void *p2);

// STSRow2
int32_t tPutTSRow(uint8_t *p, STSRow2 *pRow) {
  int32_t n = 0;

  n += tPutI64(p ? p + n : p, pRow->ts);
  n += tPutI8(p ? p + n : p, pRow->flags);
  n += tPutI32v(p ? p + n : p, pRow->sver);

  ASSERT(pRow->flags & 0xf);

  switch (pRow->flags & 0xf) {
    case TSROW_HAS_NONE:
    case TSROW_HAS_NULL:
      break;
    default:
      n += tPutBinary(p ? p + n : p, pRow->pData, pRow->nData);
      break;
  }

  return n;
}

int32_t tGetTSRow(uint8_t *p, STSRow2 *pRow) {
  int32_t n = 0;
  uint8_t flags;

  n += tGetI64(p + n, pRow ? &pRow->ts : NULL);
  n += tGetI8(p + n, pRow ? &pRow->flags : &flags);
  n += tGetI32v(p + n, pRow ? &pRow->sver : NULL);

  if (pRow) flags = pRow->flags;
  switch (flags & 0xf) {
    case TSROW_HAS_NONE:
    case TSROW_HAS_NULL:
      break;
    default:
      n += tGetBinary(p + n, pRow ? &pRow->pData : NULL, pRow ? &pRow->nData : NULL);
      break;
  }

  return n;
}

int32_t tTSRowDup(const STSRow2 *pRow, STSRow2 **ppRow) {
  (*ppRow) = taosMemoryMalloc(sizeof(*pRow) + pRow->nData);
  if (*ppRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  (*ppRow)->ts = pRow->ts;
  (*ppRow)->flags = pRow->flags;
  (*ppRow)->sver = pRow->sver;
  (*ppRow)->nData = pRow->nData;
  if (pRow->nData) {
    (*ppRow)->pData = (uint8_t *)(&(*ppRow)[1]);
    memcpy((*ppRow)->pData, pRow->pData, pRow->nData);
  } else {
    (*ppRow)->pData = NULL;
  }

  return 0;
}

void tTSRowFree(STSRow2 *pRow) {
  if (pRow) taosMemoryFree(pRow);
}

int32_t tTSRowGet(const STSRow2 *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal) {
  uint32_t  n;
  uint8_t  *p;
  uint8_t   v;
  int32_t   bidx = iCol - 1;
  STColumn *pTColumn = &pTSchema->columns[iCol];
  STSKVRow *pTSKVRow;
  SKVIdx   *pKVIdx;

  ASSERT(iCol != 0);
  ASSERT(pTColumn->colId != 0);

  ASSERT(pRow->flags & 0xf != 0);
  switch (pRow->flags & 0xf) {
    case TSROW_HAS_NONE:
      *pColVal = ColValNONE;
      return 0;
    case TSROW_HAS_NULL:
      *pColVal = ColValNULL;
      return 0;
  }

  if (TSROW_IS_KV_ROW(pRow)) {
    ASSERT((pRow->flags & 0xf) != TSROW_HAS_VAL);

    pTSKVRow = (STSKVRow *)pRow->pData;
    pKVIdx =
        bsearch(&((SKVIdx){.cid = pTColumn->colId}), pTSKVRow->idx, pTSKVRow->nCols, sizeof(SKVIdx), tSKVIdxCmprFn);
    if (pKVIdx == NULL) {
      *pColVal = ColValNONE;
    } else if (pKVIdx->offset < 0) {
      *pColVal = ColValNULL;
    } else {
      p = pRow->pData + sizeof(STSKVRow) + sizeof(SKVIdx) * pTSKVRow->nCols + pKVIdx->offset;
      pColVal->type = COL_VAL_DATA;
      tGetBinary(p, &pColVal->pData, &pColVal->nData);
    }
  } else {
    // get bitmap
    p = pRow->pData;
    switch (pRow->flags & 0xf) {
      case TSROW_HAS_NULL | TSROW_HAS_NONE:
        v = GET_BIT1(p, bidx);
        if (v == 0) {
          *pColVal = ColValNONE;
        } else {
          *pColVal = ColValNULL;
        }
        return 0;
      case TSROW_HAS_VAL | TSROW_HAS_NONE:
        v = GET_BIT1(p, bidx);
        if (v == 1) {
          p = p + BIT1_SIZE(pTSchema->numOfCols - 1);
          break;
        } else {
          *pColVal = ColValNONE;
          return 0;
        }
      case TSROW_HAS_VAL | TSROW_HAS_NULL:
        v = GET_BIT1(p, bidx);
        if (v == 1) {
          p = p + BIT1_SIZE(pTSchema->numOfCols - 1);
          break;
        } else {
          *pColVal = ColValNULL;
          return 0;
        }
      case TSROW_HAS_VAL | TSROW_HAS_NULL | TSROW_HAS_NONE:
        v = GET_BIT2(p, bidx);
        if (v == 0) {
          *pColVal = ColValNONE;
          return 0;
        } else if (v == 1) {
          *pColVal = ColValNULL;
          return 0;
        } else if (v == 2) {
          p = p + BIT2_SIZE(pTSchema->numOfCols - 1);
          break;
        } else {
          ASSERT(0);
        }
      default:
        break;
    }

    // get real value
    p = p + pTColumn->offset;
    pColVal->type = COL_VAL_DATA;
    if (IS_VAR_DATA_TYPE(pTColumn->type)) {
      tGetBinary(p + pTSchema->flen + *(int32_t *)p, &pColVal->pData, &pColVal->nData);
    } else {
      pColVal->pData = p;
      pColVal->nData = pTColumn->bytes;
    }
  }

  return 0;
}

// STSchema
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

void tTSchemaDestroy(STSchema *pTSchema) {
  if (pTSchema) taosMemoryFree(pTSchema);
}

// STSRowBuilder
int32_t tTSRowBuilderInit(STSRowBuilder *pBuilder, int32_t sver, int32_t nCols, SSchema *pSchema) {
  if (tTSchemaCreate(sver, pSchema, nCols, &pBuilder->pTSchema) < 0) return -1;

  pBuilder->szBitMap1 = BIT1_SIZE(nCols - 1);
  pBuilder->szBitMap2 = BIT2_SIZE(nCols - 1);
  pBuilder->szKVBuf =
      sizeof(STSKVRow) + sizeof(SKVIdx) * (nCols - 1) + pBuilder->pTSchema->flen + pBuilder->pTSchema->vlen;
  pBuilder->szTPBuf = pBuilder->szBitMap2 + pBuilder->pTSchema->flen + pBuilder->pTSchema->vlen;
  pBuilder->pKVBuf = taosMemoryMalloc(pBuilder->szKVBuf);
  if (pBuilder->pKVBuf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tTSchemaDestroy(pBuilder->pTSchema);
    return -1;
  }
  pBuilder->pTPBuf = taosMemoryMalloc(pBuilder->szTPBuf);
  if (pBuilder->pTPBuf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pBuilder->pKVBuf);
    tTSchemaDestroy(pBuilder->pTSchema);
    return -1;
  }

  return 0;
}

void tTSRowBuilderClear(STSRowBuilder *pBuilder) {
  if (pBuilder->pTPBuf) {
    taosMemoryFree(pBuilder->pTPBuf);
    pBuilder->pTPBuf = NULL;
  }
  if (pBuilder->pKVBuf) {
    taosMemoryFree(pBuilder->pKVBuf);
    pBuilder->pKVBuf = NULL;
  }
  tTSchemaDestroy(pBuilder->pTSchema);
  pBuilder->pTSchema = NULL;
}

void tTSRowBuilderReset(STSRowBuilder *pBuilder) {
  for (int32_t iCol = pBuilder->pTSchema->numOfCols - 1; iCol >= 0; iCol--) {
    STColumn *pTColumn = &pBuilder->pTSchema->columns[iCol];
    COL_CLR_SET(pTColumn->flags);
  }

  pBuilder->iCol = 0;
  ((STSKVRow *)pBuilder->pKVBuf)->nCols = 0;
  pBuilder->vlenKV = 0;
  pBuilder->vlenTP = 0;
  pBuilder->row.flags = 0;
}

int32_t tTSRowBuilderPut(STSRowBuilder *pBuilder, int32_t cid, uint8_t *pData, uint32_t nData) {
  STColumn *pTColumn = &pBuilder->pTSchema->columns[pBuilder->iCol];
  uint8_t  *p;
  int32_t   iCol;
  STSKVRow *pTSKVRow = (STSKVRow *)pBuilder->pKVBuf;

  // use interp search
  if (pTColumn->colId < cid) {  // right search
    for (iCol = pBuilder->iCol + 1; iCol < pBuilder->pTSchema->numOfCols; iCol++) {
      pTColumn = &pBuilder->pTSchema->columns[iCol];
      if (pTColumn->colId >= cid) break;
    }
  } else if (pTColumn->colId > cid) {  // left search
    for (iCol = pBuilder->iCol - 1; iCol >= 0; iCol--) {
      pTColumn = &pBuilder->pTSchema->columns[iCol];
      if (pTColumn->colId <= cid) break;
    }
  }

  if (pTColumn->colId != cid || COL_IS_SET(pTColumn->flags)) {
    return -1;
  }

  pBuilder->iCol = iCol;

  // set value
  if (cid == 0) {
    ASSERT(pData && nData == sizeof(TSKEY) && iCol == 0);
    pBuilder->row.ts = *(TSKEY *)pData;
    pTColumn->flags |= COL_SET_VAL;
  } else {
    if (pData) {
      // set VAL

      pBuilder->row.flags |= TSROW_HAS_VAL;
      pTColumn->flags |= COL_SET_VAL;

      /* KV */
      if (1) {  // avoid KV at some threshold (todo)
        pTSKVRow->idx[pTSKVRow->nCols].cid = cid;
        pTSKVRow->idx[pTSKVRow->nCols].offset = pBuilder->vlenKV;

        p = pBuilder->pKVBuf + sizeof(STSKVRow) + sizeof(SKVIdx) * (pBuilder->pTSchema->numOfCols - 1) +
            pBuilder->vlenKV;
        if (IS_VAR_DATA_TYPE(pTColumn->type)) {
          ASSERT(nData <= pTColumn->bytes);
          pBuilder->vlenKV += tPutBinary(p, pData, nData);
        } else {
          ASSERT(nData == pTColumn->bytes);
          memcpy(p, pData, nData);
          pBuilder->vlenKV += nData;
        }
      }

      /* TUPLE */
      p = pBuilder->pTPBuf + pBuilder->szBitMap2 + pTColumn->offset;
      if (IS_VAR_DATA_TYPE(pTColumn->type)) {
        ASSERT(nData <= pTColumn->bytes);
        *(int32_t *)p = pBuilder->vlenTP;

        p = pBuilder->pTPBuf + pBuilder->szBitMap2 + pBuilder->pTSchema->flen + pBuilder->vlenTP;
        pBuilder->vlenTP += tPutBinary(p, pData, nData);
      } else {
        ASSERT(nData == pTColumn->bytes);
        memcpy(p, pData, nData);
      }
    } else {
      // set NULL

      pBuilder->row.flags |= TSROW_HAS_NULL;
      pTColumn->flags |= COL_SET_NULL;

      pTSKVRow->idx[pTSKVRow->nCols].cid = cid;
      pTSKVRow->idx[pTSKVRow->nCols].offset = -1;
    }

    pTSKVRow->nCols++;
  }

  return 0;
}

static FORCE_INLINE int tSKVIdxCmprFn(const void *p1, const void *p2) {
  SKVIdx *pKVIdx1 = (SKVIdx *)p1;
  SKVIdx *pKVIdx2 = (SKVIdx *)p2;
  if (pKVIdx1->cid > pKVIdx2->cid) {
    return 1;
  } else if (pKVIdx1->cid < pKVIdx2->cid) {
    return -1;
  }
  return 0;
}
static void setBitMap(uint8_t *p, STSchema *pTSchema, uint8_t flags) {
  int32_t   bidx;
  STColumn *pTColumn;

  for (int32_t iCol = 1; iCol < pTSchema->numOfCols; iCol++) {
    pTColumn = &pTSchema->columns[iCol];
    bidx = iCol - 1;

    switch (flags) {
      case TSROW_HAS_NULL | TSROW_HAS_NONE:
        if (pTColumn->flags & COL_SET_NULL) {
          SET_BIT1(p, bidx, (uint8_t)1);
        } else {
          SET_BIT1(p, bidx, (uint8_t)0);
        }
        break;
      case TSROW_HAS_VAL | TSROW_HAS_NULL | TSROW_HAS_NONE:
        if (pTColumn->flags & COL_SET_NULL) {
          SET_BIT2(p, bidx, (uint8_t)1);
        } else if (pTColumn->flags & COL_SET_VAL) {
          SET_BIT2(p, bidx, (uint8_t)2);
        } else {
          SET_BIT2(p, bidx, (uint8_t)0);
        }
        break;
      default:
        if (pTColumn->flags & COL_SET_VAL) {
          SET_BIT1(p, bidx, (uint8_t)1);
        } else {
          SET_BIT1(p, bidx, (uint8_t)0);
        }

        break;
    }
  }
}
int32_t tTSRowBuilderGetRow(STSRowBuilder *pBuilder, const STSRow2 **ppRow) {
  int32_t   nDataTP, nDataKV;
  uint32_t  flags;
  STSKVRow *pTSKVRow = (STSKVRow *)pBuilder->pKVBuf;
  int32_t   nCols = pBuilder->pTSchema->numOfCols;

  // error not set ts
  if (!COL_IS_SET(pBuilder->pTSchema->columns->flags)) {
    return -1;
  }

  ASSERT(pTSKVRow->nCols < nCols);
  if (pTSKVRow->nCols < nCols - 1) {
    pBuilder->row.flags |= TSROW_HAS_NONE;
  }

  ASSERT(pBuilder->row.flags & 0xf != 0);
  *(ppRow) = &pBuilder->row;
  switch (pBuilder->row.flags & 0xf) {
    case TSROW_HAS_NONE:
    case TSROW_HAS_NULL:
      pBuilder->row.nData = 0;
      pBuilder->row.pData = NULL;
      return 0;
    case TSROW_HAS_NULL | TSROW_HAS_NONE:
      nDataTP = pBuilder->szBitMap1;
      break;
    case TSROW_HAS_VAL:
      nDataTP = pBuilder->pTSchema->flen + pBuilder->vlenTP;
      break;
    case TSROW_HAS_VAL | TSROW_HAS_NONE:
    case TSROW_HAS_VAL | TSROW_HAS_NULL:
      nDataTP = pBuilder->szBitMap1 + pBuilder->pTSchema->flen + pBuilder->vlenTP;
      break;
    case TSROW_HAS_VAL | TSROW_HAS_NULL | TSROW_HAS_NONE:
      nDataTP = pBuilder->szBitMap2 + pBuilder->pTSchema->flen + pBuilder->vlenTP;
      break;
    default:
      ASSERT(0);
  }

  nDataKV = sizeof(STSKVRow) + sizeof(SKVIdx) * pTSKVRow->nCols + pBuilder->vlenKV;
  pBuilder->row.sver = pBuilder->pTSchema->version;
  if (nDataKV < nDataTP) {
    // generate KV row

    ASSERT(pBuilder->row.flags & 0xf != TSROW_HAS_VAL);

    pBuilder->row.flags |= TSROW_KV_ROW;
    pBuilder->row.nData = nDataKV;
    pBuilder->row.pData = pBuilder->pKVBuf;

    qsort(pTSKVRow->idx, pTSKVRow->nCols, sizeof(SKVIdx), tSKVIdxCmprFn);
    if (pTSKVRow->nCols < nCols - 1) {
      memmove(&pTSKVRow->idx[pTSKVRow->nCols], &pTSKVRow->idx[nCols - 1], pBuilder->vlenKV);
    }
  } else {
    // generate TUPLE row

    pBuilder->row.nData = nDataTP;

    uint8_t *p;
    uint8_t  flags = pBuilder->row.flags & 0xf;

    if (flags == TSROW_HAS_VAL) {
      pBuilder->row.pData = pBuilder->pTPBuf + pBuilder->szBitMap2;
    } else {
      if (flags == TSROW_HAS_VAL | TSROW_HAS_NULL | TSROW_HAS_NONE) {
        pBuilder->row.pData = pBuilder->pTPBuf;
      } else {
        pBuilder->row.pData = pBuilder->pTPBuf + pBuilder->szBitMap2 - pBuilder->szBitMap1;
      }

      setBitMap(pBuilder->row.pData, pBuilder->pTSchema, flags);
    }
  }

  return 0;
}

static FORCE_INLINE int tTagIdxCmprFn(const void *p1, const void *p2) {
  STagIdx *pTagIdx1 = (STagIdx *)p1;
  STagIdx *pTagIdx2 = (STagIdx *)p2;
  if (pTagIdx1->cid < pTagIdx1->cid) {
    return -1;
  } else if (pTagIdx1->cid > pTagIdx1->cid) {
    return 1;
  }
  return 0;
}
int32_t tTagNew(STagVal *pTagVals, int16_t nTag, STag **ppTag) {
  STagVal *pTagVal;
  uint8_t *p;
  int32_t  n;
  uint16_t tsize = sizeof(STag) + sizeof(STagIdx) * nTag;

  for (int16_t iTag = 0; iTag < nTag; iTag++) {
    pTagVal = &pTagVals[iTag];

    if (IS_VAR_DATA_TYPE(pTagVal->type)) {
      tsize += tPutBinary(NULL, pTagVal->pData, pTagVal->nData);
    } else {
      ASSERT(pTagVal->nData == TYPE_BYTES[pTagVal->type]);
      tsize += pTagVal->nData;
    }
  }

  (*ppTag) = (STag *)taosMemoryMalloc(tsize);
  if (*ppTag == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  p = (uint8_t *)&((*ppTag)->idx[nTag]);
  n = 0;

  (*ppTag)->len = tsize;
  (*ppTag)->nTag = nTag;
  for (int16_t iTag = 0; iTag < nTag; iTag++) {
    pTagVal = &pTagVals[iTag];

    (*ppTag)->idx[iTag].cid = pTagVal->cid;
    (*ppTag)->idx[iTag].offset = n;

    if (IS_VAR_DATA_TYPE(pTagVal->type)) {
      n += tPutBinary(p + n, pTagVal->pData, pTagVal->nData);
    } else {
      memcpy(p + n, pTagVal->pData, pTagVal->nData);
      n += pTagVal->nData;
    }
  }

  qsort((*ppTag)->idx, (*ppTag)->nTag, sizeof(STagIdx), tTagIdxCmprFn);
  return 0;
}

void tTagFree(STag *pTag) {
  if (pTag) taosMemoryFree(pTag);
}

void tTagGet(STag *pTag, int16_t cid, int8_t type, uint8_t **ppData, int32_t *nData) {
  STagIdx *pTagIdx = bsearch(&((STagIdx){.cid = cid}), pTag->idx, pTag->nTag, sizeof(STagIdx), tTagIdxCmprFn);
  if (pTagIdx == NULL) {
    *ppData = NULL;
    *nData = 0;
  } else {
    uint8_t *p = (uint8_t *)&pTag->idx[pTag->nTag] + pTagIdx->offset;
    if (IS_VAR_DATA_TYPE(type)) {
      tGetBinary(p, ppData, nData);
    } else {
      *ppData = p;
      *nData = TYPE_BYTES[type];
    }
  }
}

int32_t tEncodeTag(SEncoder *pEncoder, STag *pTag) {
  // return tEncodeBinary(pEncoder, (uint8_t *)pTag, pTag->len);
  ASSERT(0);
  return 0;
}

int32_t tDecodeTag(SDecoder *pDecoder, const STag **ppTag) {
  // uint32_t n;
  // return tDecodeBinary(pDecoder, (const uint8_t **)ppTag, &n);
  ASSERT(0);
  return 0;
}

#if 1  // ===================================================================================================================
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
  // spaceNeeded += TYPE_BYTES[pCol->type]; // the bitmap part is append as a single part since 2022.04.03, thus
  // remove the additional space
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
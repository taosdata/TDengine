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

static int32_t tGetTagVal(uint8_t *p, STagVal *pTagVal, int8_t isJson);

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

static void debugPrintTagVal(int8_t type, const void *val, int32_t vlen, const char *tag, int32_t ln) {
  switch (type) {
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_NCHAR: {
      char tmpVal[32] = {0};
      memcpy(tmpVal, val, 32);
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
    default:
      ASSERT(0);
      break;
  }
}

  // if (isLarge) {
  //   p = (uint8_t *)&((int16_t *)pTag->idx)[pTag->nTag];
  // } else {
  //   p = (uint8_t *)&pTag->idx[pTag->nTag];
  // }

  // (*ppArray) = taosArrayInit(pTag->nTag + 1, sizeof(STagVal));
  // if (*ppArray == NULL) {
  //   code = TSDB_CODE_OUT_OF_MEMORY;
  //   goto _err;
  // }

  // for (int16_t iTag = 0; iTag < pTag->nTag; iTag++) {
  //   if (isLarge) {
  //     offset = ((int16_t *)pTag->idx)[iTag];
  //   } else {
  //     offset = pTag->idx[iTag];
  //   }

void debugPrintSTag(STag *pTag, const char *tag, int32_t ln) {
  int8_t isJson = pTag->flags & TD_TAG_JSON;
  int8_t isLarge = pTag->flags & TD_TAG_LARGE;
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
    debugPrintTagVal(tagVal.type, tagVal.pData, tagVal.nData, __func__, __LINE__);
  }
  printf("\n");
}

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

  debugPrintTagVal(pTagVal->type, pTagVal->pData, pTagVal->nData, __func__, __LINE__);
  // value
  if (IS_VAR_DATA_TYPE(pTagVal->type)) {
    n += tPutBinary(p ? p + n : p, pTagVal->pData, pTagVal->nData);
  } else {
    ASSERT(pTagVal->nData == TYPE_BYTES[pTagVal->type]);
    if (p) memcpy(p + n, pTagVal->pData, pTagVal->nData);
    n += pTagVal->nData;
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
    pTagVal->pData = p + n;
    pTagVal->nData = TYPE_BYTES[pTagVal->type];
    n += pTagVal->nData;
  }

  return n;
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
    qsort(pArray->pData, nTag, sizeof(STagVal), tTagValJsonCmprFn);
  } else {
    qsort(pArray->pData, nTag, sizeof(STagVal), tTagValCmprFn);
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

  ASSERT(szTag <= INT16_MAX);

  // build tag
  (*ppTag) = (STag *)taosMemoryCalloc(szTag, 1);
  if ((*ppTag) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
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

  debugPrintSTag(*ppTag, __func__, __LINE__);

  return code;

_err:
  return code;
}

void tTagFree(STag *pTag) {
  if (pTag) taosMemoryFree(pTag);
}

bool tTagGet(const STag *pTag, STagVal *pTagVal) {
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

    tGetTagVal(p + offset, &tv, isJson);
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
      pTagVal->type = tv.type;
      pTagVal->nData = tv.nData;
      pTagVal->pData = tv.pData;
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
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  for (int16_t iTag = 0; iTag < pTag->nTag; iTag++) {
    if (isLarge) {
      offset = ((int16_t *)pTag->idx)[iTag];
    } else {
      offset = pTag->idx[iTag];
    }
    tGetTagVal(p + offset, &tv, pTag->flags & TD_TAG_JSON);
    taosArrayPush(*ppArray, &tv);
  }

  return code;

_err:
  return code;
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
  pCols->bitmapMode = TSDB_BITMODE_DEFAULT;

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
  pCols->bitmapMode = TSDB_BITMODE_DEFAULT;
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

#endif
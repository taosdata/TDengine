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
#include "tRealloc.h"
#include "tcoding.h"
#include "tdatablock.h"
#include "tlog.h"

static int32_t (*tColDataAppendValueImpl[8][3])(SColData *pColData, uint8_t *pData, uint32_t nData);
static int32_t (*tColDataUpdateValueImpl[8][3])(SColData *pColData, uint8_t *pData, uint32_t nData, bool forward);

// SBuffer ================================
#ifdef BUILD_NO_CALL
void tBufferDestroy(SBuffer *pBuffer) {
  tFree(pBuffer->pBuf);
  pBuffer->pBuf = NULL;
}

int32_t tBufferInit(SBuffer *pBuffer, int64_t size) {
  pBuffer->nBuf = 0;
  return tRealloc(&pBuffer->pBuf, size);
}

int32_t tBufferPut(SBuffer *pBuffer, const void *pData, int64_t nData) {
  int32_t code = 0;

  code = tRealloc(&pBuffer->pBuf, pBuffer->nBuf + nData);
  if (code) return code;

  memcpy(pBuffer->pBuf + pBuffer->nBuf, pData, nData);
  pBuffer->nBuf += nData;

  return code;
}

int32_t tBufferReserve(SBuffer *pBuffer, int64_t nData, void **ppData) {
  int32_t code = tRealloc(&pBuffer->pBuf, pBuffer->nBuf + nData);
  if (code) return code;

  *ppData = pBuffer->pBuf + pBuffer->nBuf;
  pBuffer->nBuf += nData;

  return code;
}
#endif
// ================================
static int32_t tGetTagVal(uint8_t *p, STagVal *pTagVal, int8_t isJson);

// SRow ========================================================================
#define KV_FLG_LIT ((uint8_t)0x10)
#define KV_FLG_MID ((uint8_t)0x20)
#define KV_FLG_BIG ((uint8_t)0x30)

#define BIT_FLG_NONE  ((uint8_t)0x0)
#define BIT_FLG_NULL  ((uint8_t)0x1)
#define BIT_FLG_VALUE ((uint8_t)0x2)

#pragma pack(push, 1)
typedef struct {
  int16_t nCol;
  char    idx[];  // uint8_t * | uint16_t * | uint32_t *
} SKVIdx;
#pragma pack(pop)

#define ROW_SET_BITMAP(PB, FLAG, IDX, VAL)        \
  do {                                            \
    if (PB) {                                     \
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
          ASSERT(0);                              \
          break;                                  \
      }                                           \
    }                                             \
  } while (0)

int32_t tRowBuild(SArray *aColVal, const STSchema *pTSchema, SRow **ppRow) {
  int32_t code = 0;

  ASSERT(TARRAY_SIZE(aColVal) > 0);
  ASSERT(((SColVal *)aColVal->pData)[0].cid == PRIMARYKEY_TIMESTAMP_COL_ID);
  ASSERT(((SColVal *)aColVal->pData)[0].type == TSDB_DATA_TYPE_TIMESTAMP);

  // scan ---------------
  SRow           *pRow = NULL;
  SColVal        *colVals = (SColVal *)TARRAY_DATA(aColVal);
  uint8_t         flag = 0;
  int32_t         iColVal = 1;
  const int32_t   nColVal = TARRAY_SIZE(aColVal);
  SColVal        *pColVal = (iColVal < nColVal) ? &colVals[iColVal] : NULL;
  int32_t         iTColumn = 1;
  const STColumn *pTColumn = pTSchema->columns + iTColumn;
  int32_t         ntp = 0;
  int32_t         nkv = 0;
  int32_t         maxIdx = 0;
  int32_t         nIdx = 0;
  while (pTColumn) {
    if (pColVal) {
      if (pColVal->cid == pTColumn->colId) {
        if (COL_VAL_IS_VALUE(pColVal)) {  // VALUE
          flag |= HAS_VALUE;
          maxIdx = nkv;
          if (IS_VAR_DATA_TYPE(pTColumn->type)) {
            ntp = ntp + tPutU32v(NULL, pColVal->value.nData) + pColVal->value.nData;
            nkv = nkv + tPutI16v(NULL, pTColumn->colId) + tPutU32v(NULL, pColVal->value.nData) + pColVal->value.nData;
          } else {
            nkv = nkv + tPutI16v(NULL, pTColumn->colId) + pTColumn->bytes;
          }
          nIdx++;
        } else if (COL_VAL_IS_NONE(pColVal)) {  // NONE
          flag |= HAS_NONE;
        } else if (COL_VAL_IS_NULL(pColVal)) {  // NULL
          flag |= HAS_NULL;
          maxIdx = nkv;
          nkv += tPutI16v(NULL, -pTColumn->colId);
          nIdx++;
        } else {
          if (ASSERTS(0, "invalid input")) {
            code = TSDB_CODE_INVALID_PARA;
            goto _exit;
          }
        }

        pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
        pColVal = (++iColVal < nColVal) ? &colVals[iColVal] : NULL;
      } else if (pColVal->cid > pTColumn->colId) {  // NONE
        flag |= HAS_NONE;
        pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
      } else {
        pColVal = (++iColVal < nColVal) ? &colVals[iColVal] : NULL;
      }
    } else {  // NONE
      flag |= HAS_NONE;
      pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
    }
  }

  // compare ---------------
  switch (flag) {
    case HAS_NONE:
    case HAS_NULL:
      ntp = sizeof(SRow);
      break;
    case HAS_VALUE:
      ntp = sizeof(SRow) + pTSchema->flen + ntp;
      break;
    case (HAS_NULL | HAS_NONE):
      ntp = sizeof(SRow) + BIT1_SIZE(pTSchema->numOfCols - 1);
      break;
    case (HAS_VALUE | HAS_NONE):
    case (HAS_VALUE | HAS_NULL):
      ntp = sizeof(SRow) + BIT1_SIZE(pTSchema->numOfCols - 1) + pTSchema->flen + ntp;
      break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      ntp = sizeof(SRow) + BIT2_SIZE(pTSchema->numOfCols - 1) + pTSchema->flen + ntp;
      break;
    default:
      if (ASSERTS(0, "impossible")) {
        code = TSDB_CODE_INVALID_PARA;
        goto _exit;
      }
  }
  if (maxIdx <= UINT8_MAX) {
    nkv = sizeof(SRow) + sizeof(SKVIdx) + nIdx + nkv;
    flag |= KV_FLG_LIT;
  } else if (maxIdx <= UINT16_MAX) {
    nkv = sizeof(SRow) + sizeof(SKVIdx) + (nIdx << 1) + nkv;
    flag |= KV_FLG_MID;
  } else {
    nkv = sizeof(SRow) + sizeof(SKVIdx) + (nIdx << 2) + nkv;
    flag |= KV_FLG_BIG;
  }
  int32_t nRow;
  if (nkv < ntp) {
    nRow = nkv;
  } else {
    nRow = ntp;
    flag &= ((uint8_t)0x0f);
  }

  // alloc --------------
  pRow = taosMemoryMalloc(nRow);
  if (NULL == pRow) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // build --------------
  pColVal = &colVals[0];

  pRow->flag = flag;
  pRow->rsv = 0;
  pRow->sver = pTSchema->version;
  pRow->len = nRow;
  memcpy(&pRow->ts, &pColVal->value.val, sizeof(TSKEY));

  if (flag == HAS_NONE || flag == HAS_NULL) {
    goto _exit;
  }

  iColVal = 1;
  pColVal = (iColVal < nColVal) ? &colVals[iColVal] : NULL;
  iTColumn = 1;
  pTColumn = pTSchema->columns + iTColumn;
  if (flag >> 4) {  // KV
    SKVIdx  *pIdx = (SKVIdx *)pRow->data;
    int32_t  iIdx = 0;
    int32_t  nv = 0;
    uint8_t *pv = NULL;
    if (flag & KV_FLG_LIT) {
      pv = pIdx->idx + nIdx;
    } else if (flag & KV_FLG_MID) {
      pv = pIdx->idx + (nIdx << 1);
    } else {
      pv = pIdx->idx + (nIdx << 2);
    }
    pIdx->nCol = nIdx;

    while (pTColumn) {
      if (pColVal) {
        if (pColVal->cid == pTColumn->colId) {
          if (COL_VAL_IS_VALUE(pColVal)) {
            if (flag & KV_FLG_LIT) {
              ((uint8_t *)pIdx->idx)[iIdx] = (uint8_t)nv;
            } else if (flag & KV_FLG_MID) {
              ((uint16_t *)pIdx->idx)[iIdx] = (uint16_t)nv;
            } else {
              ((uint32_t *)pIdx->idx)[iIdx] = (uint32_t)nv;
            }
            iIdx++;

            nv += tPutI16v(pv + nv, pTColumn->colId);
            if (IS_VAR_DATA_TYPE(pTColumn->type)) {
              nv += tPutU32v(pv + nv, pColVal->value.nData);
              memcpy(pv + nv, pColVal->value.pData, pColVal->value.nData);
              nv += pColVal->value.nData;
            } else {
              memcpy(pv + nv, &pColVal->value.val, pTColumn->bytes);
              nv += pTColumn->bytes;
            }
          } else if (COL_VAL_IS_NULL(pColVal)) {
            if (flag & KV_FLG_LIT) {
              ((uint8_t *)pIdx->idx)[iIdx] = (uint8_t)nv;
            } else if (flag & KV_FLG_MID) {
              ((uint16_t *)pIdx->idx)[iIdx] = (uint16_t)nv;
            } else {
              ((uint32_t *)pIdx->idx)[iIdx] = (uint32_t)nv;
            }
            iIdx++;
            nv += tPutI16v(pv + nv, -pTColumn->colId);
          }

          pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
          pColVal = (++iColVal < nColVal) ? &colVals[iColVal] : NULL;
        } else if (pColVal->cid > pTColumn->colId) {  // NONE
          pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
        } else {
          pColVal = (++iColVal < nColVal) ? &colVals[iColVal] : NULL;
        }
      } else {  // NONE
        pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
      }
    }
  } else {  // TUPLE
    uint8_t *pb = NULL;
    uint8_t *pf = NULL;
    uint8_t *pv = NULL;
    int32_t  nv = 0;

    switch (flag) {
      case (HAS_NULL | HAS_NONE):
        pb = pRow->data;
        break;
      case HAS_VALUE:
        pf = pRow->data;
        pv = pf + pTSchema->flen;
        break;
      case (HAS_VALUE | HAS_NONE):
      case (HAS_VALUE | HAS_NULL):
        pb = pRow->data;
        pf = pb + BIT1_SIZE(pTSchema->numOfCols - 1);
        pv = pf + pTSchema->flen;
        break;
      case (HAS_VALUE | HAS_NULL | HAS_NONE):
        pb = pRow->data;
        pf = pb + BIT2_SIZE(pTSchema->numOfCols - 1);
        pv = pf + pTSchema->flen;
        break;
      default:
        if (ASSERTS(0, "impossible")) {
          code = TSDB_CODE_INVALID_PARA;
          goto _exit;
        }
    }

    if (pb) {
      if (flag == (HAS_VALUE | HAS_NULL | HAS_NONE)) {
        memset(pb, 0, BIT2_SIZE(pTSchema->numOfCols - 1));
      } else {
        memset(pb, 0, BIT1_SIZE(pTSchema->numOfCols - 1));
      }
    }

    // build impl
    while (pTColumn) {
      if (pColVal) {
        if (pColVal->cid == pTColumn->colId) {
          if (COL_VAL_IS_VALUE(pColVal)) {  // VALUE
            ROW_SET_BITMAP(pb, flag, iTColumn - 1, BIT_FLG_VALUE);

            if (IS_VAR_DATA_TYPE(pTColumn->type)) {
              *(int32_t *)(pf + pTColumn->offset) = nv;
              nv += tPutU32v(pv + nv, pColVal->value.nData);
              if (pColVal->value.nData) {
                memcpy(pv + nv, pColVal->value.pData, pColVal->value.nData);
                nv += pColVal->value.nData;
              }
            } else {
              memcpy(pf + pTColumn->offset, &pColVal->value.val, TYPE_BYTES[pTColumn->type]);
            }
          } else if (COL_VAL_IS_NONE(pColVal)) {  // NONE
            ROW_SET_BITMAP(pb, flag, iTColumn - 1, BIT_FLG_NONE);
            if (pf) memset(pf + pTColumn->offset, 0, TYPE_BYTES[pTColumn->type]);
          } else {  // NULL
            ROW_SET_BITMAP(pb, flag, iTColumn - 1, BIT_FLG_NULL);
            if (pf) memset(pf + pTColumn->offset, 0, TYPE_BYTES[pTColumn->type]);
          }

          pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
          pColVal = (++iColVal < nColVal) ? &colVals[iColVal] : NULL;
        } else if (pColVal->cid > pTColumn->colId) {  // NONE
          ROW_SET_BITMAP(pb, flag, iTColumn - 1, BIT_FLG_NONE);
          if (pf) memset(pf + pTColumn->offset, 0, TYPE_BYTES[pTColumn->type]);
          pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
        } else {
          pColVal = (++iColVal < nColVal) ? &colVals[iColVal] : NULL;
        }
      } else {  // NONE
        ROW_SET_BITMAP(pb, flag, iTColumn - 1, BIT_FLG_NONE);
        if (pf) memset(pf + pTColumn->offset, 0, TYPE_BYTES[pTColumn->type]);
        pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
      }
    }
  }

_exit:
  if (code) {
    *ppRow = NULL;
    tRowDestroy(pRow);
  } else {
    *ppRow = pRow;
  }
  return code;
}

int32_t tRowGet(SRow *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal) {
  ASSERT(iCol < pTSchema->numOfCols);
  ASSERT(pRow->sver == pTSchema->version);

  STColumn *pTColumn = pTSchema->columns + iCol;

  if (iCol == 0) {
    pColVal->cid = pTColumn->colId;
    pColVal->type = pTColumn->type;
    pColVal->flag = CV_FLAG_VALUE;
    memcpy(&pColVal->value.val, &pRow->ts, sizeof(TSKEY));
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

  if (pRow->flag >> 4) {  // KV Row
    SKVIdx  *pIdx = (SKVIdx *)pRow->data;
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
          pColVal->type = pTColumn->type;
          pColVal->flag = CV_FLAG_VALUE;

          if (IS_VAR_DATA_TYPE(pTColumn->type)) {
            pData += tGetU32v(pData, &pColVal->value.nData);
            if (pColVal->value.nData > 0) {
              pColVal->value.pData = pData;
            } else {
              pColVal->value.pData = NULL;
            }
          } else {
            memcpy(&pColVal->value.val, pData, pTColumn->bytes);
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
    if (pRow->flag == HAS_VALUE) {
      pColVal->cid = pTColumn->colId;
      pColVal->type = pTColumn->type;
      pColVal->flag = CV_FLAG_VALUE;
      if (IS_VAR_DATA_TYPE(pTColumn->type)) {
        uint8_t *pData = pRow->data + pTSchema->flen + *(int32_t *)(pRow->data + pTColumn->offset);
        pData += tGetU32v(pData, &pColVal->value.nData);
        if (pColVal->value.nData) {
          pColVal->value.pData = pData;
        } else {
          pColVal->value.pData = NULL;
        }
      } else {
        memcpy(&pColVal->value.val, pRow->data + pTColumn->offset, TYPE_BYTES[pTColumn->type]);
      }
    } else {
      uint8_t *pf;
      uint8_t *pv;
      uint8_t  bv = BIT_FLG_VALUE;

      switch (pRow->flag) {
        case (HAS_NULL | HAS_NONE):
          bv = GET_BIT1(pRow->data, iCol - 1);
          break;
        case (HAS_VALUE | HAS_NONE):
          bv = GET_BIT1(pRow->data, iCol - 1);
          if (bv) bv++;
          pf = pRow->data + BIT1_SIZE(pTSchema->numOfCols - 1);
          pv = pf + pTSchema->flen;
          break;
        case (HAS_VALUE | HAS_NULL):
          bv = GET_BIT1(pRow->data, iCol - 1);
          bv++;
          pf = pRow->data + BIT1_SIZE(pTSchema->numOfCols - 1);
          pv = pf + pTSchema->flen;
          break;
        case (HAS_VALUE | HAS_NULL | HAS_NONE):
          bv = GET_BIT2(pRow->data, iCol - 1);
          pf = pRow->data + BIT2_SIZE(pTSchema->numOfCols - 1);
          pv = pf + pTSchema->flen;
          break;
        default:
          ASSERTS(0, "invalid row format");
          return TSDB_CODE_INVALID_DATA_FMT;
      }

      if (bv == BIT_FLG_NONE) {
        *pColVal = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
        return 0;
      } else if (bv == BIT_FLG_NULL) {
        *pColVal = COL_VAL_NULL(pTColumn->colId, pTColumn->type);
        return 0;
      }

      pColVal->cid = pTColumn->colId;
      pColVal->type = pTColumn->type;
      pColVal->flag = CV_FLAG_VALUE;
      if (IS_VAR_DATA_TYPE(pTColumn->type)) {
        uint8_t *pData = pv + *(int32_t *)(pf + pTColumn->offset);
        pData += tGetU32v(pData, &pColVal->value.nData);
        if (pColVal->value.nData) {
          pColVal->value.pData = pData;
        } else {
          pColVal->value.pData = NULL;
        }
      } else {
        memcpy(&pColVal->value.val, pf + pTColumn->offset, TYPE_BYTES[pTColumn->type]);
      }
    }
  }

  return 0;
}

void tRowDestroy(SRow *pRow) {
  if (pRow) taosMemoryFree(pRow);
}

static int32_t tRowPCmprFn(const void *p1, const void *p2) {
  if ((*(SRow **)p1)->ts < (*(SRow **)p2)->ts) {
    return -1;
  } else if ((*(SRow **)p1)->ts > (*(SRow **)p2)->ts) {
    return 1;
  }

  return 0;
}
static void    tRowPDestroy(SRow **ppRow) { tRowDestroy(*ppRow); }
static int32_t tRowMergeImpl(SArray *aRowP, STSchema *pTSchema, int32_t iStart, int32_t iEnd, int8_t flag) {
  int32_t code = 0;

  int32_t    nRow = iEnd - iStart;
  SRowIter **aIter = NULL;
  SArray    *aColVal = NULL;
  SRow      *pRow = NULL;

  aIter = taosMemoryCalloc(nRow, sizeof(SRowIter *));
  if (aIter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
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
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  for (int32_t iCol = 0; iCol < pTSchema->numOfCols; iCol++) {
    SColVal *pColVal = NULL;
    for (int32_t iRow = 0; iRow < nRow; iRow++) {
      SColVal *pColValT = tRowIterNext(aIter[iRow]);

      // todo: take strategy according to the flag
      if (COL_VAL_IS_VALUE(pColValT)) {
        pColVal = pColValT;
      } else if (COL_VAL_IS_NULL(pColValT)) {
        if (pColVal == NULL) {
          pColVal = pColValT;
        }
      }
    }

    if (pColVal) taosArrayPush(aColVal, pColVal);
  }

  // build
  code = tRowBuild(aColVal, pTSchema, &pRow);
  if (code) goto _exit;

  taosArrayRemoveBatch(aRowP, iStart, nRow, (FDelete)tRowPDestroy);
  taosArrayInsert(aRowP, iStart, &pRow);

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

int32_t tRowMerge(SArray *aRowP, STSchema *pTSchema, int8_t flag) {
  int32_t code = 0;

  int32_t iStart = 0;
  while (iStart < aRowP->size) {
    SRow *pRow = (SRow *)taosArrayGetP(aRowP, iStart);

    int32_t iEnd = iStart + 1;
    while (iEnd < aRowP->size) {
      SRow *pRowT = (SRow *)taosArrayGetP(aRowP, iEnd);

      if (pRow->ts != pRowT->ts) break;

      iEnd++;
    }

    if (iEnd - iStart > 1) {
      code = tRowMergeImpl(aRowP, pTSchema, iStart, iEnd, flag);
      if (code) return code;
    }

    // the array is also changing, so the iStart just ++ instead of iEnd
    iStart++;
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
  ASSERT(pRow->sver == pTSchema->version);

  int32_t code = 0;

  SRowIter *pIter = taosMemoryCalloc(1, sizeof(*pIter));
  if (pIter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  pIter->pRow = pRow;
  pIter->pTSchema = pTSchema;
  pIter->iTColumn = 0;

  if (pRow->flag == HAS_NONE || pRow->flag == HAS_NULL) goto _exit;

  if (pRow->flag >> 4) {
    pIter->iCol = 0;
    pIter->pIdx = (SKVIdx *)pRow->data;
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
        pIter->pb = pRow->data;
        break;
      case HAS_VALUE:
        pIter->pf = pRow->data;
        pIter->pv = pIter->pf + pTSchema->flen;
        break;
      case (HAS_VALUE | HAS_NONE):
      case (HAS_VALUE | HAS_NULL):
        pIter->pb = pRow->data;
        pIter->pf = pRow->data + BIT1_SIZE(pTSchema->numOfCols - 1);
        pIter->pv = pIter->pf + pTSchema->flen;
        break;
      case (HAS_VALUE | HAS_NULL | HAS_NONE):
        pIter->pb = pRow->data;
        pIter->pf = pRow->data + BIT2_SIZE(pTSchema->numOfCols - 1);
        pIter->pv = pIter->pf + pTSchema->flen;
        break;
      default:
        ASSERT(0);
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
    pIter->cv.type = pTColumn->type;
    pIter->cv.flag = CV_FLAG_VALUE;
    memcpy(&pIter->cv.value.val, &pIter->pRow->ts, sizeof(TSKEY));
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
          pIter->cv.type = pTColumn->type;
          pIter->cv.flag = CV_FLAG_VALUE;

          if (IS_VAR_DATA_TYPE(pTColumn->type)) {
            pData += tGetU32v(pData, &pIter->cv.value.nData);
            if (pIter->cv.value.nData > 0) {
              pIter->cv.value.pData = pData;
            } else {
              pIter->cv.value.pData = NULL;
            }
          } else {
            memcpy(&pIter->cv.value.val, pData, pTColumn->bytes);
          }
        }

        pIter->iCol++;
        goto _exit;
      } else if (TABS(cid) > pTColumn->colId) {
        pIter->cv = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
        goto _exit;
      } else {
        ASSERT(0);
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
          ASSERT(0);
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
    pIter->cv.type = pTColumn->type;
    pIter->cv.flag = CV_FLAG_VALUE;
    if (IS_VAR_DATA_TYPE(pTColumn->type)) {
      uint8_t *pData = pIter->pv + *(int32_t *)(pIter->pf + pTColumn->offset);
      pData += tGetU32v(pData, &pIter->cv.value.nData);
      if (pIter->cv.value.nData > 0) {
        pIter->cv.value.pData = pData;
      } else {
        pIter->cv.value.pData = NULL;
      }
    } else {
      memcpy(&pIter->cv.value.val, pIter->pf + pTColumn->offset, TYPE_BYTES[pTColumn->type]);
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

  uint8_t *pb = NULL, *pf = NULL, *pv = NULL;

  switch (pRow->flag) {
    case HAS_VALUE:
      pf = pRow->data;
      pv = pf + pTSchema->flen;
      break;
    case (HAS_NULL | HAS_NONE):
      pb = pRow->data;
      break;
    case (HAS_VALUE | HAS_NONE):
    case (HAS_VALUE | HAS_NULL):
      pb = pRow->data;
      pf = pb + BIT1_SIZE(pTSchema->numOfCols - 1);
      pv = pf + pTSchema->flen;
      break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      pb = pRow->data;
      pf = pb + BIT2_SIZE(pTSchema->numOfCols - 1);
      pv = pf + pTSchema->flen;
      break;
    default:
      ASSERTS(0, "Invalid row flag");
      return TSDB_CODE_INVALID_DATA_FMT;
  }

  while (pColData) {
    if (pTColumn) {
      if (pTColumn->colId == pColData->cid) {
        ASSERT(pTColumn->type == pColData->type);
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
              ASSERTS(0, "Invalid row flag");
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

  SKVIdx   *pKVIdx = (SKVIdx *)pRow->data;
  uint8_t  *pv = NULL;
  int32_t   iColData = 0;
  SColData *pColData = &aColData[iColData];
  int32_t   iTColumn = 1;
  STColumn *pTColumn = &pTSchema->columns[iTColumn];
  int32_t   iCol = 0;

  if (pRow->flag & KV_FLG_LIT) {
    pv = pKVIdx->idx + pKVIdx->nCol;
  } else if (pRow->flag & KV_FLG_MID) {
    pv = pKVIdx->idx + (pKVIdx->nCol << 1);
  } else if (pRow->flag & KV_FLG_BIG) {
    pv = pKVIdx->idx + (pKVIdx->nCol << 2);
  } else {
    ASSERT(0);
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
            ASSERTS(0, "Invalid KV row format");
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
  ASSERT(pRow->sver == pTSchema->version);
  ASSERT(nColData > 0);

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
      strncpy(tmpVal, val, vlen > 31 ? 31 : vlen);
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
      ASSERT(0);
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
    ASSERTS(pTagVal->cid > 0, "Invalid tag cid:%" PRIi16, pTagVal->cid);
  }

  // type
  n += tPutI8(p ? p + n : p, pTagVal->type);

  // value
  if (IS_VAR_DATA_TYPE(pTagVal->type)) {
    n += tPutBinary(p ? p + n : p, pTagVal->pData, pTagVal->nData);
  } else {
    p = p ? p + n : p;
    n += tDataTypes[pTagVal->type].bytes;
    if (p) memcpy(p, &(pTagVal->i64), tDataTypes[pTagVal->type].bytes);
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
    memcpy(&(pTagVal->i64), p + n, tDataTypes[pTagVal->type].bytes);
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
    memcpy(varDataVal(data + typeBytes), value->pData, value->nData);
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
      memcpy(pTagVal, &tv, sizeof(tv));
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

void tTagSetCid(const STag *pTag, int16_t iTag, int16_t cid) {
  uint8_t *p = NULL;
  int8_t   isLarge = pTag->flags & TD_TAG_LARGE;
  int16_t  offset = 0;

  if (isLarge) {
    p = (uint8_t *)&((int16_t *)pTag->idx)[pTag->nTag];
  } else {
    p = (uint8_t *)&pTag->idx[pTag->nTag];
  }

  if (isLarge) {
    offset = ((int16_t *)pTag->idx)[iTag];
  } else {
    offset = pTag->idx[iTag];
  }

  tPutI16v(p + offset, cid);
}

// STSchema ========================================
STSchema *tBuildTSchema(SSchema *aSchema, int32_t numOfCols, int32_t version) {
  STSchema *pTSchema = taosMemoryCalloc(1, sizeof(STSchema) + sizeof(STColumn) * numOfCols);
  if (pTSchema == NULL) {
    return NULL;
  }

  pTSchema->numOfCols = numOfCols;
  pTSchema->version = version;

  // timestamp column
  ASSERT(aSchema[0].type == TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT(aSchema[0].colId == PRIMARYKEY_TIMESTAMP_COL_ID);
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

// SColData ========================================
void tColDataDestroy(void *ph) {
  if (ph) {
    SColData *pColData = (SColData *)ph;

    tFree(pColData->pBitMap);
    tFree(pColData->aOffset);
    tFree(pColData->pData);
  }
}

void tColDataInit(SColData *pColData, int16_t cid, int8_t type, int8_t smaOn) {
  pColData->cid = cid;
  pColData->type = type;
  pColData->smaOn = smaOn;
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
  int32_t code = 0;

  if (IS_VAR_DATA_TYPE(pColData->type)) {
    code = tRealloc((uint8_t **)(&pColData->aOffset), ((int64_t)(pColData->nVal + 1)) << 2);
    if (code) goto _exit;
    pColData->aOffset[pColData->nVal] = pColData->nData;

    if (nData) {
      code = tRealloc(&pColData->pData, pColData->nData + nData);
      if (code) goto _exit;
      memcpy(pColData->pData + pColData->nData, pData, nData);
      pColData->nData += nData;
    }
  } else {
    ASSERT(pColData->nData == tDataTypes[pColData->type].bytes * pColData->nVal);
    code = tRealloc(&pColData->pData, pColData->nData + tDataTypes[pColData->type].bytes);
    if (code) goto _exit;
    if (pData) {
      memcpy(pColData->pData + pColData->nData, pData, TYPE_BYTES[pColData->type]);
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
int32_t tColDataAppendValue(SColData *pColData, SColVal *pColVal) {
  ASSERT(pColData->cid == pColVal->cid && pColData->type == pColVal->type);
  return tColDataAppendValueImpl[pColData->flag][pColVal->flag](
      pColData, IS_VAR_DATA_TYPE(pColData->type) ? pColVal->value.pData : (uint8_t *)&pColVal->value.val,
      pColVal->value.nData);
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
    ASSERT(0);
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
      if (IS_STR_DATA_TYPE(pColData->type)) {
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
  ASSERT(pColData->cid == pColVal->cid && pColData->type == pColVal->type);
  ASSERT(pColData->nVal > 0);

  if (tColDataUpdateValueImpl[pColData->flag][pColVal->flag] == NULL) return 0;

  return tColDataUpdateValueImpl[pColData->flag][pColVal->flag](
      pColData, IS_VAR_DATA_TYPE(pColData->type) ? pColVal->value.pData : (uint8_t *)&pColVal->value.val,
      pColVal->value.nData, forward);
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
      ASSERT(0);
  }
}
static FORCE_INLINE void tColDataGetValue4(SColData *pColData, int32_t iVal, SColVal *pColVal) {  // HAS_VALUE
  SValue value;
  if (IS_VAR_DATA_TYPE(pColData->type)) {
    if (iVal + 1 < pColData->nVal) {
      value.nData = pColData->aOffset[iVal + 1] - pColData->aOffset[iVal];
    } else {
      value.nData = pColData->nData - pColData->aOffset[iVal];
    }
    value.pData = pColData->pData + pColData->aOffset[iVal];
  } else {
    memcpy(&value.val, pColData->pData + tDataTypes[pColData->type].bytes * iVal, tDataTypes[pColData->type].bytes);
  }
  *pColVal = COL_VAL_VALUE(pColData->cid, pColData->type, value);
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
      ASSERT(0);
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
      ASSERT(0);
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
      ASSERT(0);
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
void tColDataGetValue(SColData *pColData, int32_t iVal, SColVal *pColVal) {
  ASSERT(iVal >= 0 && iVal < pColData->nVal && pColData->flag);
  tColDataGetValueImpl[pColData->flag](pColData, iVal, pColVal);
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
      ASSERTS(0, "not possible");
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
      memcpy(pColData->pBitMap, pColDataFrom->pBitMap, BIT1_SIZE(pColData->nVal));
      break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      pColData->pBitMap = xMalloc(arg, BIT2_SIZE(pColData->nVal));
      if (pColData->pBitMap == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
      memcpy(pColData->pBitMap, pColDataFrom->pBitMap, BIT2_SIZE(pColData->nVal));
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
    memcpy(pColData->aOffset, pColDataFrom->aOffset, pColData->nVal << 2);
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

    memcpy(pColData->pData, pColDataFrom->pData, pColData->nData);
  } else {
    pColData->pData = NULL;
  }

_exit:
  return code;
}

int32_t tColDataAddValueByDataBlock(SColData *pColData, int8_t type, int32_t bytes, int32_t nRows, char *lengthOrbitmap,
                                    char *data) {
  int32_t code = 0;
  if (data == NULL) {
    for (int32_t i = 0; i < nRows; ++i) {
      code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NONE](pColData, NULL, 0);
    }
    goto _exit;
  }

  if (IS_VAR_DATA_TYPE(type)) {  // var-length data type
    for (int32_t i = 0; i < nRows; ++i) {
      int32_t offset = *((int32_t *)lengthOrbitmap + i);
      if (offset == -1) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
        if (code) goto _exit;
      } else {
        if (varDataTLen(data + offset) > bytes) {
          uError("var data length invalid, varDataTLen(data + offset):%d <= bytes:%d", (int)varDataTLen(data + offset),
                 bytes);
          code = TSDB_CODE_PAR_VALUE_TOO_LONG;
          goto _exit;
        }
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_VALUE](pColData, (uint8_t *)varDataVal(data + offset),
                                                                      varDataLen(data + offset));
      }
    }
  } else {  // fixed-length data type
    bool allValue = true;
    bool allNull = true;
    for (int32_t i = 0; i < nRows; ++i) {
      if (!colDataIsNull_f(lengthOrbitmap, i)) {
        allNull = false;
      } else {
        allValue = false;
      }
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
        if (colDataIsNull_f(lengthOrbitmap, i)) {
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

int32_t tColDataAddValueByBind(SColData *pColData, TAOS_MULTI_BIND *pBind, int32_t buffMaxLen) {
  int32_t code = 0;

  if (!(pBind->num == 1 && pBind->is_null && *pBind->is_null)) {
    ASSERT(pColData->type == pBind->buffer_type);
  }

  if (IS_VAR_DATA_TYPE(pColData->type)) {  // var-length data type
    for (int32_t i = 0; i < pBind->num; ++i) {
      if (pBind->is_null && pBind->is_null[i]) {
        code = tColDataAppendValueImpl[pColData->flag][CV_FLAG_NULL](pColData, NULL, 0);
        if (code) goto _exit;
      } else if (pBind->length[i] > buffMaxLen) {
        uError("var data length too big, len:%d, max:%d", pBind->length[i], buffMaxLen);
        return TSDB_CODE_INVALID_PARA;
      } else {
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

#ifdef BUILD_NO_CALL
static int32_t tColDataSwapValue(SColData *pColData, int32_t i, int32_t j) {
  int32_t code = 0;

  if (IS_VAR_DATA_TYPE(pColData->type)) {
    int32_t  nData1 = pColData->aOffset[i + 1] - pColData->aOffset[i];
    int32_t  nData2 = (j < pColData->nVal - 1) ? pColData->aOffset[j + 1] - pColData->aOffset[j]
                                               : pColData->nData - pColData->aOffset[j];
    uint8_t *pData = taosMemoryMalloc(TMAX(nData1, nData2));
    if (pData == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    if (nData1 > nData2) {
      memcpy(pData, pColData->pData + pColData->aOffset[i], nData1);
      memcpy(pColData->pData + pColData->aOffset[i], pColData->pData + pColData->aOffset[j], nData2);
      // memmove(pColData->pData + pColData->aOffset[i] + nData2, pColData->pData + pColData->aOffset[i] + nData1,
      //         pColData->aOffset[j] - pColData->aOffset[i + 1]);
      memmove(pColData->pData + pColData->aOffset[i] + nData2, pColData->pData + pColData->aOffset[i + 1],
              pColData->aOffset[j] - pColData->aOffset[i + 1]);
      memcpy(pColData->pData + pColData->aOffset[j] + nData2 - nData1, pData, nData1);
    } else {
      memcpy(pData, pColData->pData + pColData->aOffset[j], nData2);
      memcpy(pColData->pData + pColData->aOffset[j] + nData2 - nData1, pColData->pData + pColData->aOffset[i], nData1);
      // memmove(pColData->pData + pColData->aOffset[j] + nData2 - nData1, pColData->pData + pColData->aOffset[i] +
      // nData1,
      //         pColData->aOffset[j] - pColData->aOffset[i + 1]);
      memmove(pColData->pData + pColData->aOffset[i] + nData2, pColData->pData + pColData->aOffset[i + 1],
              pColData->aOffset[j] - pColData->aOffset[i + 1]);
      memcpy(pColData->pData + pColData->aOffset[i], pData, nData2);
    }
    for (int32_t k = i + 1; k <= j; ++k) {
      pColData->aOffset[k] = pColData->aOffset[k] + nData2 - nData1;
    }

    taosMemoryFree(pData);
  } else {
    uint64_t val;
    memcpy(&val, &pColData->pData[TYPE_BYTES[pColData->type] * i], TYPE_BYTES[pColData->type]);
    memcpy(&pColData->pData[TYPE_BYTES[pColData->type] * i], &pColData->pData[TYPE_BYTES[pColData->type] * j],
           TYPE_BYTES[pColData->type]);
    memcpy(&pColData->pData[TYPE_BYTES[pColData->type] * j], &val, TYPE_BYTES[pColData->type]);
  }

_exit:
  return code;
}

static void tColDataSwap(SColData *pColData, int32_t i, int32_t j) {
  ASSERT(i < j);
  ASSERT(j < pColData->nVal);

  switch (pColData->flag) {
    case HAS_NONE:
    case HAS_NULL:
      break;
    case (HAS_NULL | HAS_NONE): {
      uint8_t bv = GET_BIT1(pColData->pBitMap, i);
      SET_BIT1(pColData->pBitMap, i, GET_BIT1(pColData->pBitMap, j));
      SET_BIT1(pColData->pBitMap, j, bv);
    } break;
    case HAS_VALUE: {
      tColDataSwapValue(pColData, i, j);
    } break;
    case (HAS_VALUE | HAS_NONE):
    case (HAS_VALUE | HAS_NULL): {
      uint8_t bv = GET_BIT1(pColData->pBitMap, i);
      SET_BIT1(pColData->pBitMap, i, GET_BIT1(pColData->pBitMap, j));
      SET_BIT1(pColData->pBitMap, j, bv);
      tColDataSwapValue(pColData, i, j);
    } break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE): {
      uint8_t bv = GET_BIT2(pColData->pBitMap, i);
      SET_BIT2(pColData->pBitMap, i, GET_BIT2(pColData->pBitMap, j));
      SET_BIT2(pColData->pBitMap, j, bv);
      tColDataSwapValue(pColData, i, j);
    } break;
    default:
      ASSERT(0);
      break;
  }
}
#endif

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

    memcpy(pToColData->pData + pToColData->aOffset[iToRow], pFromColData->pData + pFromColData->aOffset[iFromRow],
           nData);
  } else {
    memcpy(&pToColData->pData[TYPE_BYTES[pToColData->type] * iToRow],
           &pFromColData->pData[TYPE_BYTES[pToColData->type] * iFromRow], TYPE_BYTES[pToColData->type]);
  }
  return code;
}

static int32_t tColDataCopyRowSingleCol(SColData *pFromColData, int32_t iFromRow, SColData *pToColData,
                                        int32_t iToRow) {
  int32_t code = TSDB_CODE_SUCCESS;

  switch (pFromColData->flag) {
    case HAS_NONE:
    case HAS_NULL:
      break;
    case (HAS_NULL | HAS_NONE): {
      SET_BIT1(pToColData->pBitMap, iToRow, GET_BIT1(pFromColData->pBitMap, iFromRow));
    } break;
    case HAS_VALUE: {
      tColDataCopyRowCell(pFromColData, iFromRow, pToColData, iToRow);
    } break;
    case (HAS_VALUE | HAS_NONE):
    case (HAS_VALUE | HAS_NULL): {
      SET_BIT1(pToColData->pBitMap, iToRow, GET_BIT1(pFromColData->pBitMap, iFromRow));
      tColDataCopyRowCell(pFromColData, iFromRow, pToColData, iToRow);
    } break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE): {
      SET_BIT2(pToColData->pBitMap, iToRow, GET_BIT2(pFromColData->pBitMap, iFromRow));
      tColDataCopyRowCell(pFromColData, iFromRow, pToColData, iToRow);
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
    tColDataGetValue(&aFromColData[i], iFromRow, &cv);
    code = tColDataAppendValue(&aToColData[i], &cv);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return code;
}

static int32_t tColDataMergeSortMerge(SColData *aColData, int32_t start, int32_t mid, int32_t end, int32_t nColData) {
  SColData *aDstColData = NULL;
  TSKEY    *aKey = (TSKEY *)aColData[0].pData;

  int32_t i = start, j = mid + 1, k = 0;

  if (end > start) {
    aDstColData = taosMemoryCalloc(1, sizeof(SColData) * nColData);
    for (int c = 0; c < nColData; ++c) {
      tColDataInit(&aDstColData[c], aColData[c].cid, aColData[c].type, aColData[c].smaOn);
    }
    if (aDstColData == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    /*
    for (int32_t i = 0; i < nColData; i++) {
      tColDataCopy(&aColData[i], &aDstColData[i], tColDataDefaultMalloc, NULL);
    }
    */
  }

  while (i <= mid && j <= end) {
    if (aKey[i] <= aKey[j]) {
      // tColDataCopyRow(aColData, i++, aDstColData, k++);
      tColDataCopyRowAppend(aColData, i++, aDstColData, nColData);
    } else {
      // tColDataCopyRow(aColData, j++, aDstColData, k++);
      tColDataCopyRowAppend(aColData, j++, aDstColData, nColData);
    }
  }

  while (i <= mid) {
    // tColDataCopyRow(aColData, i++, aDstColData, k++);
    tColDataCopyRowAppend(aColData, i++, aDstColData, nColData);
  }

  while (j <= end) {
    // tColDataCopyRow(aColData, j++, aDstColData, k++);
    tColDataCopyRowAppend(aColData, j++, aDstColData, nColData);
  }

  for (i = start, k = 0; i <= end; ++i, ++k) {
    tColDataCopyRow(aDstColData, k, aColData, i, nColData);
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
static void tColDataMergeImpl(SColData *pColData, int32_t iStart, int32_t iEnd /* not included */) {
  switch (pColData->flag) {
    case HAS_NONE:
    case HAS_NULL: {
      pColData->nVal -= (iEnd - iStart - 1);
    } break;
    case (HAS_NULL | HAS_NONE): {
      if (GET_BIT1(pColData->pBitMap, iStart) == 0) {
        for (int32_t i = iStart + 1; i < iEnd; ++i) {
          if (GET_BIT1(pColData->pBitMap, i) == 1) {
            SET_BIT1(pColData->pBitMap, iStart, 1);
            break;
          }
        }
      }
      for (int32_t i = iEnd, j = iStart + 1; i < pColData->nVal; ++i, ++j) {
        SET_BIT1(pColData->pBitMap, j, GET_BIT1(pColData->pBitMap, i));
      }

      pColData->nVal -= (iEnd - iStart - 1);

      uint8_t flag = 0;
      for (int32_t i = 0; i < pColData->nVal; ++i) {
        uint8_t bv = GET_BIT1(pColData->pBitMap, i);
        if (bv == BIT_FLG_NONE) {
          flag |= HAS_NONE;
        } else if (bv == BIT_FLG_NULL) {
          flag |= HAS_NULL;
        } else {
          ASSERT(0);
        }

        if (flag == pColData->flag) break;
      }
      pColData->flag = flag;
    } break;
    case HAS_VALUE: {
      if (IS_VAR_DATA_TYPE(pColData->type)) {
        int32_t nDiff = pColData->aOffset[iEnd - 1] - pColData->aOffset[iStart];

        memmove(pColData->pData + pColData->aOffset[iStart], pColData->pData + pColData->aOffset[iEnd - 1],
                pColData->nData - pColData->aOffset[iEnd - 1]);
        pColData->nData -= nDiff;

        for (int32_t i = iEnd, j = iStart + 1; i < pColData->nVal; ++i, ++j) {
          pColData->aOffset[j] = pColData->aOffset[i] - nDiff;
        }
      } else {
        memmove(pColData->pData + TYPE_BYTES[pColData->type] * iStart,
                pColData->pData + TYPE_BYTES[pColData->type] * (iEnd - 1),
                TYPE_BYTES[pColData->type] * (pColData->nVal - iEnd + 1));
        pColData->nData -= (TYPE_BYTES[pColData->type] * (iEnd - iStart - 1));
      }

      pColData->nVal -= (iEnd - iStart - 1);
    } break;
    case (HAS_VALUE | HAS_NONE): {
      uint8_t bv;
      int32_t iv;
      for (int32_t i = iEnd - 1; i >= iStart; --i) {
        bv = GET_BIT1(pColData->pBitMap, i);
        if (bv) {
          iv = i;
          break;
        }
      }

      if (bv) {  // has a value
        if (IS_VAR_DATA_TYPE(pColData->type)) {
          if (iv != iStart) {
            memmove(&pColData->pData[pColData->aOffset[iStart]], &pColData->pData[pColData->aOffset[iv]],
                    iv < (pColData->nVal - 1) ? pColData->aOffset[iv + 1] - pColData->aOffset[iv]
                                              : pColData->nData - pColData->aOffset[iv]);
          }
          // TODO
          ASSERT(0);
        } else {
          if (iv != iStart) {
            memcpy(&pColData->pData[TYPE_BYTES[pColData->type] * iStart],
                   &pColData->pData[TYPE_BYTES[pColData->type] * iv], TYPE_BYTES[pColData->type]);
          }
          memmove(&pColData->pData[TYPE_BYTES[pColData->type] * (iStart + 1)],
                  &pColData->pData[TYPE_BYTES[pColData->type] * iEnd],
                  TYPE_BYTES[pColData->type] * (iEnd - iStart - 1));
          pColData->nData -= (TYPE_BYTES[pColData->type] * (iEnd - iStart - 1));
        }

        SET_BIT1(pColData->pBitMap, iStart, 1);
        for (int32_t i = iEnd, j = iStart + 1; i < pColData->nVal; ++i, ++j) {
          SET_BIT1(pColData->pBitMap, j, GET_BIT1(pColData->pBitMap, i));
        }

        uint8_t flag = HAS_VALUE;
        for (int32_t i = 0; i < pColData->nVal - (iEnd - iStart - 1); ++i) {
          if (GET_BIT1(pColData->pBitMap, i) == 0) {
            flag |= HAS_NONE;
          }

          if (flag == pColData->flag) break;
        }
        pColData->flag = flag;
      } else {  // all NONE
        if (IS_VAR_DATA_TYPE(pColData->type)) {
          int32_t nDiff = pColData->aOffset[iEnd - 1] - pColData->aOffset[iStart];

          memmove(&pColData->pData[pColData->aOffset[iStart]], &pColData->pData[pColData->aOffset[iEnd - 1]],
                  pColData->nData - pColData->aOffset[iEnd - 1]);
          pColData->nData -= nDiff;

          for (int32_t i = iEnd, j = iStart + 1; i < pColData->nVal; ++i, ++j) {
            pColData->aOffset[j] = pColData->aOffset[i] - nDiff;
          }
        } else {
          memmove(pColData->pData + TYPE_BYTES[pColData->type] * (iStart + 1),
                  pColData->pData + TYPE_BYTES[pColData->type] * iEnd,
                  TYPE_BYTES[pColData->type] * (pColData->nVal - iEnd + 1));
          pColData->nData -= (TYPE_BYTES[pColData->type] * (iEnd - iStart - 1));
        }

        for (int32_t i = iEnd, j = iStart + 1; i < pColData->nVal; ++i, ++j) {
          SET_BIT1(pColData->pBitMap, j, GET_BIT1(pColData->pBitMap, i));
        }
      }
      pColData->nVal -= (iEnd - iStart - 1);
    } break;
    case (HAS_VALUE | HAS_NULL): {
      if (IS_VAR_DATA_TYPE(pColData->type)) {
        int32_t nDiff = pColData->aOffset[iEnd - 1] - pColData->aOffset[iStart];

        memmove(pColData->pData + pColData->aOffset[iStart], pColData->pData + pColData->aOffset[iEnd - 1],
                pColData->nData - pColData->aOffset[iEnd - 1]);
        pColData->nData -= nDiff;

        for (int32_t i = iEnd, j = iStart + 1; i < pColData->nVal; ++i, ++j) {
          pColData->aOffset[j] = pColData->aOffset[i] - nDiff;
        }
      } else {
        memmove(pColData->pData + TYPE_BYTES[pColData->type] * iStart,
                pColData->pData + TYPE_BYTES[pColData->type] * (iEnd - 1),
                TYPE_BYTES[pColData->type] * (pColData->nVal - iEnd + 1));
        pColData->nData -= (TYPE_BYTES[pColData->type] * (iEnd - iStart - 1));
      }

      for (int32_t i = iEnd - 1, j = iStart; i < pColData->nVal; ++i, ++j) {
        SET_BIT1(pColData->pBitMap, j, GET_BIT1(pColData->pBitMap, i));
      }

      pColData->nVal -= (iEnd - iStart - 1);

      uint8_t flag = 0;
      for (int32_t i = 0; i < pColData->nVal; ++i) {
        if (GET_BIT1(pColData->pBitMap, i)) {
          flag |= HAS_VALUE;
        } else {
          flag |= HAS_NULL;
        }

        if (flag == pColData->flag) break;
      }
      pColData->flag = flag;
    } break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE): {
      uint8_t bv;
      int32_t iv;
      for (int32_t i = iEnd - 1; i >= iStart; --i) {
        bv = GET_BIT2(pColData->pBitMap, i);
        if (bv) {
          iv = i;
          break;
        }
      }

      if (bv) {
        // TODO
        ASSERT(0);
      } else {  // ALL NONE
        if (IS_VAR_DATA_TYPE(pColData->type)) {
          // TODO
          ASSERT(0);
        } else {
          memmove(pColData->pData + TYPE_BYTES[pColData->type] * (iStart + 1),
                  pColData->pData + TYPE_BYTES[pColData->type] * iEnd,
                  TYPE_BYTES[pColData->type] * (pColData->nVal - iEnd));
          pColData->nData -= (TYPE_BYTES[pColData->type] * (iEnd - iStart - 1));
        }

        for (int32_t i = iEnd, j = iStart + 1; i < pColData->nVal; ++i, ++j) {
          SET_BIT2(pColData->pBitMap, j, GET_BIT2(pColData->pBitMap, i));
        }
      }
      pColData->nVal -= (iEnd - iStart - 1);
    } break;
    default:
      ASSERT(0);
      break;
  }
}
static void tColDataMerge(SColData *aColData, int32_t nColData) {
  int32_t iStart = 0;
  for (;;) {
    if (iStart >= aColData[0].nVal - 1) break;

    int32_t iEnd = iStart + 1;
    while (iEnd < aColData[0].nVal) {
      if (((TSKEY *)aColData[0].pData)[iEnd] != ((TSKEY *)aColData[0].pData)[iStart]) break;

      iEnd++;
    }

    if (iEnd - iStart > 1) {
      for (int32_t i = 0; i < nColData; i++) {
        tColDataMergeImpl(&aColData[i], iStart, iEnd);
      }
    }

    iStart++;
  }
}
void tColDataSortMerge(SArray *colDataArr) {
  int32_t   nColData = TARRAY_SIZE(colDataArr);
  SColData *aColData = (SColData *)TARRAY_DATA(colDataArr);

  if (aColData[0].nVal <= 1) goto _exit;

  ASSERT(aColData[0].type == TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT(aColData[0].cid == PRIMARYKEY_TIMESTAMP_COL_ID);
  ASSERT(aColData[0].flag == HAS_VALUE);

  int8_t doSort = 0;
  int8_t doMerge = 0;
  // scan -------
  TSKEY *aKey = (TSKEY *)aColData[0].pData;
  for (int32_t iVal = 1; iVal < aColData[0].nVal; ++iVal) {
    if (aKey[iVal] > aKey[iVal - 1]) {
      continue;
    } else if (aKey[iVal] < aKey[iVal - 1]) {
      doSort = 1;
      break;
    } else {
      doMerge = 1;
    }
  }

  // sort -------
  if (doSort) {
    tColDataSort(aColData, nColData);
  }

  if (doMerge != 1) {
    for (int32_t iVal = 1; iVal < aColData[0].nVal; ++iVal) {
      if (aKey[iVal] == aKey[iVal - 1]) {
        doMerge = 1;
        break;
      }
    }
  }

  // merge -------
  if (doMerge) {
    tColDataMerge(aColData, nColData);
  }

_exit:
  return;
}

int32_t tPutColData(uint8_t *pBuf, SColData *pColData) {
  int32_t n = 0;

  n += tPutI16v(pBuf ? pBuf + n : NULL, pColData->cid);
  n += tPutI8(pBuf ? pBuf + n : NULL, pColData->type);
  n += tPutI32v(pBuf ? pBuf + n : NULL, pColData->nVal);
  n += tPutI8(pBuf ? pBuf + n : NULL, pColData->flag);

  // bitmap
  switch (pColData->flag) {
    case (HAS_NULL | HAS_NONE):
    case (HAS_VALUE | HAS_NONE):
    case (HAS_VALUE | HAS_NULL):
      if (pBuf) memcpy(pBuf + n, pColData->pBitMap, BIT1_SIZE(pColData->nVal));
      n += BIT1_SIZE(pColData->nVal);
      break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      if (pBuf) memcpy(pBuf + n, pColData->pBitMap, BIT2_SIZE(pColData->nVal));
      n += BIT2_SIZE(pColData->nVal);
      break;
    default:
      break;
  }

  // value
  if (pColData->flag & HAS_VALUE) {
    if (IS_VAR_DATA_TYPE(pColData->type)) {
      if (pBuf) memcpy(pBuf + n, pColData->aOffset, pColData->nVal << 2);
      n += (pColData->nVal << 2);

      n += tPutI32v(pBuf ? pBuf + n : NULL, pColData->nData);
      if (pBuf) memcpy(pBuf + n, pColData->pData, pColData->nData);
      n += pColData->nData;
    } else {
      if (pBuf) memcpy(pBuf + n, pColData->pData, pColData->nData);
      n += pColData->nData;
    }
  }

  return n;
}

int32_t tGetColData(uint8_t *pBuf, SColData *pColData) {
  int32_t n = 0;

  n += tGetI16v(pBuf + n, &pColData->cid);
  n += tGetI8(pBuf + n, &pColData->type);
  n += tGetI32v(pBuf + n, &pColData->nVal);
  n += tGetI8(pBuf + n, &pColData->flag);

  // bitmap
  switch (pColData->flag) {
    case (HAS_NULL | HAS_NONE):
    case (HAS_VALUE | HAS_NONE):
    case (HAS_VALUE | HAS_NULL):
      pColData->pBitMap = pBuf + n;
      n += BIT1_SIZE(pColData->nVal);
      break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      pColData->pBitMap = pBuf + n;
      n += BIT2_SIZE(pColData->nVal);
      break;
    default:
      break;
  }

  // value
  if (pColData->flag & HAS_VALUE) {
    if (IS_VAR_DATA_TYPE(pColData->type)) {
      pColData->aOffset = (int32_t *)(pBuf + n);
      n += (pColData->nVal << 2);

      n += tGetI32v(pBuf + n, &pColData->nData);
      pColData->pData = pBuf + n;
      n += pColData->nData;
    } else {
      pColData->pData = pBuf + n;
      pColData->nData = TYPE_BYTES[pColData->type] * pColData->nVal;
      n += pColData->nData;
    }
  }

  return n;
}

#define CALC_SUM_MAX_MIN(SUM, MAX, MIN, VAL) \
  do {                                       \
    (SUM) += (VAL);                          \
    if ((MAX) < (VAL)) (MAX) = (VAL);        \
    if ((MIN) > (VAL)) (MIN) = (VAL);        \
  } while (0)

static FORCE_INLINE void tColDataCalcSMABool(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                             int16_t *numOfNull) {
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
          ASSERT(0);
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMATinyInt(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                                int16_t *numOfNull) {
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
          ASSERT(0);
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMATinySmallInt(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                                     int16_t *numOfNull) {
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
          ASSERT(0);
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAInt(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                            int16_t *numOfNull) {
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
          ASSERT(0);
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMABigInt(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                               int16_t *numOfNull) {
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
          ASSERT(0);
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAFloat(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                              int16_t *numOfNull) {
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
          ASSERT(0);
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMADouble(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                               int16_t *numOfNull) {
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
          ASSERT(0);
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAUTinyInt(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                                 int16_t *numOfNull) {
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
          ASSERT(0);
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMATinyUSmallInt(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                                      int16_t *numOfNull) {
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
          ASSERT(0);
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAUInt(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                             int16_t *numOfNull) {
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
          ASSERT(0);
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAUBigInt(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                                int16_t *numOfNull) {
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
          ASSERT(0);
          break;
      }
    }
  }
}

static FORCE_INLINE void tColDataCalcSMAVarType(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min,
                                                int16_t *numOfNull) {
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
      ASSERT(0);
      break;
  }
}

void (*tColDataCalcSMA[])(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min, int16_t *numOfNull) = {
    NULL,
    tColDataCalcSMABool,           // TSDB_DATA_TYPE_BOOL
    tColDataCalcSMATinyInt,        // TSDB_DATA_TYPE_TINYINT
    tColDataCalcSMATinySmallInt,   // TSDB_DATA_TYPE_SMALLINT
    tColDataCalcSMAInt,            // TSDB_DATA_TYPE_INT
    tColDataCalcSMABigInt,         // TSDB_DATA_TYPE_BIGINT
    tColDataCalcSMAFloat,          // TSDB_DATA_TYPE_FLOAT
    tColDataCalcSMADouble,         // TSDB_DATA_TYPE_DOUBLE
    tColDataCalcSMAVarType,        // TSDB_DATA_TYPE_VARCHAR
    tColDataCalcSMABigInt,         // TSDB_DATA_TYPE_TIMESTAMP
    tColDataCalcSMAVarType,        // TSDB_DATA_TYPE_NCHAR
    tColDataCalcSMAUTinyInt,       // TSDB_DATA_TYPE_UTINYINT
    tColDataCalcSMATinyUSmallInt,  // TSDB_DATA_TYPE_USMALLINT
    tColDataCalcSMAUInt,           // TSDB_DATA_TYPE_UINT
    tColDataCalcSMAUBigInt,        // TSDB_DATA_TYPE_UBIGINT
    tColDataCalcSMAVarType,        // TSDB_DATA_TYPE_JSON
    tColDataCalcSMAVarType,        // TSDB_DATA_TYPE_VARBINARY
    tColDataCalcSMAVarType,        // TSDB_DATA_TYPE_DECIMAL
    tColDataCalcSMAVarType,        // TSDB_DATA_TYPE_BLOB
    NULL,                          // TSDB_DATA_TYPE_MEDIUMBLOB
    tColDataCalcSMAVarType         // TSDB_DATA_TYPE_GEOMETRY
};

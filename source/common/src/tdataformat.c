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

// SBuffer ================================
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

// ================================
static int32_t tGetTagVal(uint8_t *p, STagVal *pTagVal, int8_t isJson);

// SRow ========================================================================
#define KV_FLG_LIT ((uint8_t)0x10)
#define KV_FLG_MID ((uint8_t)0x20)
#define KV_FLG_BIG ((uint8_t)0x30)

#define ROW_BIT_NONE  ((uint8_t)0x0)
#define ROW_BIT_NULL  ((uint8_t)0x1)
#define ROW_BIT_VALUE ((uint8_t)0x2)

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

int32_t tRowBuild(SArray *aColVal, STSchema *pTSchema, SBuffer *pBuffer) {
  int32_t code = 0;

  ASSERT(taosArrayGetSize(aColVal) > 0);
  ASSERT(((SColVal *)aColVal->pData)[0].cid == PRIMARYKEY_TIMESTAMP_COL_ID);
  ASSERT(((SColVal *)aColVal->pData)[0].type == TSDB_DATA_TYPE_TIMESTAMP);

  // scan ---------------
  uint8_t       flag = 0;
  int32_t       iColVal = 1;
  const int32_t nColVal = taosArrayGetSize(aColVal);
  SColVal      *pColVal = (iColVal < nColVal) ? (SColVal *)taosArrayGet(aColVal, iColVal) : NULL;
  int32_t       iTColumn = 1;
  STColumn     *pTColumn = pTSchema->columns + iTColumn;
  int32_t       ntp = 0;
  int32_t       nkv = 0;
  int32_t       maxIdx = 0;
  int32_t       nIdx = 0;
  while (pTColumn) {
    if (pColVal) {
      if (pColVal->cid == pTColumn->colId) {
        ntp += TYPE_BYTES[pTColumn->type];
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
          ASSERT(0);
        }

        pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
        pColVal = (++iColVal < nColVal) ? (SColVal *)taosArrayGet(aColVal, iColVal) : NULL;
      } else if (pColVal->cid > pTColumn->colId) {  // NONE
        flag |= HAS_NONE;
        ntp += TYPE_BYTES[pTColumn->type];
        pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
      } else {
        pColVal = (++iColVal < nColVal) ? (SColVal *)taosArrayGet(aColVal, iColVal) : NULL;
      }
    } else {  // NONE
      flag |= HAS_NONE;
      ntp += TYPE_BYTES[pTColumn->type];
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
      ntp = sizeof(SRow) + ntp;
      break;
    case (HAS_NULL | HAS_NONE):
      ntp = sizeof(SRow) + BIT1_SIZE(pTSchema->numOfCols - 1);
      break;
    case (HAS_VALUE | HAS_NONE):
    case (HAS_VALUE | HAS_NULL):
      ntp = sizeof(SRow) + BIT1_SIZE(pTSchema->numOfCols - 1) + ntp;
      break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      ntp = sizeof(SRow) + BIT2_SIZE(pTSchema->numOfCols - 1) + ntp;
      break;
    default:
      ASSERT(0);
      break;
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
  SRow *pRow = NULL;
  code = tBufferReserve(pBuffer, nRow, (void **)&pRow);
  if (code) return code;

  // build --------------
  pColVal = (SColVal *)taosArrayGet(aColVal, 0);

  pRow->flag = flag;
  pRow->rsv = 0;
  pRow->sver = pTSchema->version;
  pRow->len = nRow;
  memcpy(&pRow->ts, &pColVal->value.val, sizeof(TSKEY));

  if (flag == HAS_NONE || flag == HAS_NULL) {
    goto _exit;
  }

  iColVal = 1;
  pColVal = (iColVal < nColVal) ? (SColVal *)taosArrayGet(aColVal, iColVal) : NULL;
  iTColumn = 1;
  pTColumn = pTSchema->columns + iTColumn;
  if (flag & 0xf0) {  // KV
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
          pColVal = (++iColVal < nColVal) ? (SColVal *)taosArrayGet(aColVal, iColVal) : NULL;
        } else if (pColVal->cid > pTColumn->colId) {  // NONE
          pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
        } else {
          pColVal = (++iColVal < nColVal) ? (SColVal *)taosArrayGet(aColVal, iColVal) : NULL;
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
        ASSERT(0);
        break;
    }

    // build impl
    while (pTColumn) {
      if (pColVal) {
        if (pColVal->cid == pTColumn->colId) {
          if (COL_VAL_IS_VALUE(pColVal)) {  // VALUE
            ROW_SET_BITMAP(pb, flag, iTColumn - 1, ROW_BIT_VALUE);

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
            ROW_SET_BITMAP(pb, flag, iTColumn - 1, ROW_BIT_NONE);
            if (pf) memset(pf + pTColumn->offset, 0, TYPE_BYTES[pTColumn->type]);
          } else {  // NULL
            ROW_SET_BITMAP(pb, flag, iTColumn - 1, ROW_BIT_NULL);
            if (pf) memset(pf + pTColumn->offset, 0, TYPE_BYTES[pTColumn->type]);
          }

          pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
          pColVal = (++iColVal < nColVal) ? (SColVal *)taosArrayGet(aColVal, iColVal) : NULL;
        } else if (pColVal->cid > pTColumn->colId) {  // NONE
          ROW_SET_BITMAP(pb, flag, iTColumn - 1, ROW_BIT_NONE);
          if (pf) memset(pf + pTColumn->offset, 0, TYPE_BYTES[pTColumn->type]);
          pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
        } else {
          pColVal = (++iColVal < nColVal) ? (SColVal *)taosArrayGet(aColVal, iColVal) : NULL;
        }
      } else {  // NONE
        ROW_SET_BITMAP(pb, flag, iTColumn - 1, ROW_BIT_NONE);
        if (pf) memset(pf + pTColumn->offset, 0, TYPE_BYTES[pTColumn->type]);
        pTColumn = (++iTColumn < pTSchema->numOfCols) ? pTSchema->columns + iTColumn : NULL;
      }
    }
  }

_exit:
  return code;
}

void tRowGet(SRow *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal) {
  ASSERT(iCol < pTSchema->numOfCols);
  ASSERT(pRow->sver == pTSchema->version);

  STColumn *pTColumn = pTSchema->columns + iCol;

  if (iCol == 0) {
    pColVal->cid = pTColumn->colId;
    pColVal->type = pTColumn->type;
    pColVal->flag = CV_FLAG_VALUE;
    memcpy(&pColVal->value.val, &pRow->ts, sizeof(TSKEY));
    return;
  }

  if (pRow->flag == HAS_NONE) {
    *pColVal = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
    return;
  }

  if (pRow->flag == HAS_NULL) {
    *pColVal = COL_VAL_NULL(pTColumn->colId, pTColumn->type);
    return;
  }

  if (pRow->flag & 0xf0) {  // KV Row
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
        return;
      } else if (TABS(cid) < pTColumn->colId) {
        lidx = mid + 1;
      } else {
        ridx = mid - 1;
      }
    }

    *pColVal = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
  } else {  // Tuple Row
    uint8_t *pf = NULL;
    uint8_t *pv = NULL;
    uint8_t  bv = ROW_BIT_VALUE;

    switch (pRow->flag) {
      case HAS_VALUE:
        pf = pRow->data;
        pv = pf + pTSchema->flen;
        break;
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
        break;
    }

    if (bv == ROW_BIT_NONE) {
      *pColVal = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
      return;
    } else if (bv == ROW_BIT_NULL) {
      *pColVal = COL_VAL_NULL(pTColumn->colId, pTColumn->type);
      return;
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
      memcpy(&pColVal->value.val, pv + pTColumn->offset, pTColumn->bytes);
    }
  }
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
    uint8_t *pv;
  };

  SColVal cv;
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

  if (pRow->flag & 0xf0) {
    pIter->iCol = 0;
    pIter->pIdx = (SKVIdx *)pRow->data;
    if (pRow->flag & KV_FLG_LIT) {
      pIter->pv = pIter->pIdx->idx + pIter->pIdx->nCol;
    } else if (pRow->flag & KV_FLG_MID) {
      pIter->pv = pIter->pIdx->idx + (pIter->pIdx->nCol << 1);
    } else {
      pIter->pv = pIter->pIdx->idx + (pIter->pIdx->nCol << 2);
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
    if (pIter) taosMemoryFree(pIter);
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
    pIter->cv = COL_VAL_NULL(pTColumn->type, pTColumn->colId);
    goto _exit;
  }

  if (pIter->pRow->flag & 0xf0) {  // KV
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
    uint8_t bv = ROW_BIT_VALUE;
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

      if (bv == ROW_BIT_NONE) {
        pIter->cv = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
        goto _exit;
      } else if (bv == ROW_BIT_NULL) {
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
      memcpy(&pIter->cv.value.val, pIter->pv + pTColumn->offset, pTColumn->bytes);
    }
    goto _exit;
  }

_exit:
  pIter->iTColumn++;
  return &pIter->cv;
}

// STSchema ========================================

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

static void debugPrintTagVal(int8_t type, const void *val, int32_t vlen, const char *tag, int32_t ln) {
  switch (type) {
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_NCHAR: {
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

#if 1  // ===================================================================================================================
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
  pCol->type = type;
  pCol->colId = colId;
  pCol->flags = flags;
  if (pBuilder->nCols == 0) {
    pCol->offset = -1;
  } else {
    pCol->offset = pBuilder->flen;
    pBuilder->flen += TYPE_BYTES[type];
  }

  if (IS_VAR_DATA_TYPE(type)) {
    pCol->bytes = bytes;
    pBuilder->tlen += (TYPE_BYTES[type] + bytes);
  } else {
    pCol->bytes = TYPE_BYTES[type];
    pBuilder->tlen += TYPE_BYTES[type];
  }

  pBuilder->nCols++;

  ASSERT(pCol->offset < pBuilder->flen);

  return 0;
}

STSchema *tdGetSchemaFromBuilder(STSchemaBuilder *pBuilder) {
  if (pBuilder->nCols <= 0) return NULL;

  int tlen = sizeof(STSchema) + sizeof(STColumn) * pBuilder->nCols;

  STSchema *pSchema = (STSchema *)taosMemoryMalloc(tlen);
  if (pSchema == NULL) return NULL;

  pSchema->version = pBuilder->version;
  pSchema->numOfCols = pBuilder->nCols;
  pSchema->tlen = pBuilder->tlen;
  pSchema->flen = pBuilder->flen;

#ifdef TD_SUPPORT_BITMAP
  pSchema->tlen += (int)TD_BITMAP_BYTES(pSchema->numOfCols);
#endif

  memcpy(&pSchema->columns[0], pBuilder->columns, sizeof(STColumn) * pBuilder->nCols);

  return pSchema;
}

#endif

STSchema *tBuildTSchema(SSchema *aSchema, int32_t numOfCols, int32_t version) {
  STSchema *pTSchema = taosMemoryCalloc(1, sizeof(STSchema) + sizeof(STColumn) * numOfCols);
  if (pTSchema == NULL) return NULL;

  pTSchema->numOfCols = numOfCols;
  pTSchema->version = version;

  // timestamp column
  ASSERT(aSchema[0].type == TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT(aSchema[0].colId == PRIMARYKEY_TIMESTAMP_COL_ID);
  pTSchema->columns[0].colId = aSchema[0].colId;
  pTSchema->columns[0].type = aSchema[0].type;
  pTSchema->columns[0].flags = aSchema[0].flags;
  pTSchema->columns[0].bytes = aSchema[0].bytes;
  pTSchema->columns[0].offset = -1;

  // other columns
  for (int32_t iCol = 1; iCol < numOfCols; iCol++) {
    SSchema  *pSchema = &aSchema[iCol];
    STColumn *pTColumn = &pTSchema->columns[iCol];

    pTColumn->colId = pSchema->colId;
    pTColumn->type = pSchema->type;
    pTColumn->flags = pSchema->flags;
    pTColumn->bytes = pSchema->bytes;
    pTColumn->offset = pTSchema->flen;

    pTSchema->flen += TYPE_BYTES[pTColumn->type];
  }

  return pTSchema;
}

void tDestroyTSchema(STSchema *pTSchema) {
  if (pTSchema) taosMemoryFree(pTSchema);
}

// SColData ========================================
void tColDataDestroy(void *ph) {
  SColData *pColData = (SColData *)ph;

  tFree(pColData->pBitMap);
  tFree((uint8_t *)pColData->aOffset);
  tFree(pColData->pData);
}

void tColDataInit(SColData *pColData, int16_t cid, int8_t type, int8_t smaOn) {
  pColData->cid = cid;
  pColData->type = type;
  pColData->smaOn = smaOn;
  tColDataClear(pColData);
}

void tColDataClear(SColData *pColData) {
  pColData->nVal = 0;
  pColData->flag = 0;
  pColData->nData = 0;
}

static FORCE_INLINE int32_t tColDataPutValue(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  if (IS_VAR_DATA_TYPE(pColData->type)) {
    code = tRealloc((uint8_t **)(&pColData->aOffset), sizeof(int32_t) * (pColData->nVal + 1));
    if (code) goto _exit;
    pColData->aOffset[pColData->nVal] = pColData->nData;

    if (pColVal->value.nData) {
      code = tRealloc(&pColData->pData, pColData->nData + pColVal->value.nData);
      if (code) goto _exit;
      memcpy(pColData->pData + pColData->nData, pColVal->value.pData, pColVal->value.nData);
      pColData->nData += pColVal->value.nData;
    }
  } else {
    ASSERT(pColData->nData == tDataTypes[pColData->type].bytes * pColData->nVal);
    code = tRealloc(&pColData->pData, pColData->nData + tDataTypes[pColData->type].bytes);
    if (code) goto _exit;
    memcpy(pColData->pData + pColData->nData, &pColVal->value.val, tDataTypes[pColData->type].bytes);
    pColData->nData += tDataTypes[pColData->type].bytes;
  }
  pColData->nVal++;

_exit:
  return code;
}
static FORCE_INLINE int32_t tColDataAppendValue00(SColData *pColData, SColVal *pColVal) {
  pColData->flag = HAS_VALUE;
  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue01(SColData *pColData, SColVal *pColVal) {
  pColData->flag = HAS_NONE;
  pColData->nVal++;
  return 0;
}
static FORCE_INLINE int32_t tColDataAppendValue02(SColData *pColData, SColVal *pColVal) {
  pColData->flag = HAS_NULL;
  pColData->nVal++;
  return 0;
}
static FORCE_INLINE int32_t tColDataAppendValue10(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 0, nBit);
  SET_BIT1(pColData->pBitMap, pColData->nVal, 1);

  pColData->flag |= HAS_VALUE;

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

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue11(SColData *pColData, SColVal *pColVal) {
  pColData->nVal++;
  return 0;
}
static FORCE_INLINE int32_t tColDataAppendValue12(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 0, nBit);
  SET_BIT1(pColData->pBitMap, pColData->nVal, 1);

  pColData->flag |= HAS_NULL;
  pColData->nVal++;

  return code;
}
static FORCE_INLINE int32_t tColDataAppendValue20(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 0, nBit);
  SET_BIT1(pColData->pBitMap, pColData->nVal, 1);

  pColData->flag |= HAS_VALUE;

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

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue21(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 255, nBit);
  SET_BIT1(pColData->pBitMap, pColData->nVal, 0);

  pColData->flag |= HAS_NONE;
  pColData->nVal++;

  return code;
}
static FORCE_INLINE int32_t tColDataAppendValue22(SColData *pColData, SColVal *pColVal) {
  pColData->nVal++;
  return 0;
}
static FORCE_INLINE int32_t tColDataAppendValue30(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  pColData->flag |= HAS_VALUE;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
    SET_BIT2(pBitMap, iVal, GET_BIT1(pColData->pBitMap, iVal));
  }
  SET_BIT2(pBitMap, pColData->nVal, 2);

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

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue31(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1(pColData->pBitMap, pColData->nVal, 0);
  pColData->nVal++;

  return code;
}
static FORCE_INLINE int32_t tColDataAppendValue32(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1(pColData->pBitMap, pColData->nVal, 1);
  pColData->nVal++;

  return code;
}
#define tColDataAppendValue40 tColDataPutValue
static FORCE_INLINE int32_t tColDataAppendValue41(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  pColData->flag |= HAS_NONE;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 255, nBit);
  SET_BIT1(pColData->pBitMap, pColData->nVal, 0);

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue42(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  pColData->flag |= HAS_NULL;

  int32_t nBit = BIT1_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, nBit);
  if (code) return code;

  memset(pColData->pBitMap, 255, nBit);
  SET_BIT1(pColData->pBitMap, pColData->nVal, 0);

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue50(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1(pColData->pBitMap, pColData->nVal, 1);

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue51(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;

  SET_BIT1(pColData->pBitMap, pColData->nVal, 0);

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue52(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  pColData->flag |= HAS_NULL;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
    SET_BIT2(pBitMap, iVal, GET_BIT1(pColData->pBitMap, iVal) ? 2 : 0);
  }
  SET_BIT2(pBitMap, pColData->nVal, 1);

  tFree(pColData->pBitMap);
  pColData->pBitMap = pBitMap;

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue60(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT1(pColData->pBitMap, pColData->nVal, 1);

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue61(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  pColData->flag |= HAS_NONE;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
    SET_BIT2(pBitMap, iVal, GET_BIT1(pColData->pBitMap, iVal) ? 2 : 1);
  }
  SET_BIT2(pBitMap, pColData->nVal, 0);

  tFree(pColData->pBitMap);
  pColData->pBitMap = pBitMap;

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue62(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT1_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT1(pColData->pBitMap, pColData->nVal, 0);

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue70(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT2(pColData->pBitMap, pColData->nVal, 2);

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue71(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT2(pColData->pBitMap, pColData->nVal, 0);

  return tColDataPutValue(pColData, pColVal);
}
static FORCE_INLINE int32_t tColDataAppendValue72(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pColData->pBitMap, BIT2_SIZE(pColData->nVal + 1));
  if (code) return code;
  SET_BIT2(pColData->pBitMap, pColData->nVal, 1);

  return tColDataPutValue(pColData, pColVal);
}
static int32_t (*tColDataAppendValueImpl[8][3])(SColData *pColData, SColVal *pColVal) = {
    {tColDataAppendValue00, tColDataAppendValue01, tColDataAppendValue02},  // 0
    {tColDataAppendValue10, tColDataAppendValue11, tColDataAppendValue12},  // HAS_NONE
    {tColDataAppendValue20, tColDataAppendValue21, tColDataAppendValue22},  // HAS_NULL
    {tColDataAppendValue30, tColDataAppendValue31, tColDataAppendValue32},  // HAS_NULL|HAS_NONE
    {tColDataAppendValue40, tColDataAppendValue41, tColDataAppendValue42},  // HAS_VALUE
    {tColDataAppendValue50, tColDataAppendValue51, tColDataAppendValue52},  // HAS_VALUE|HAS_NONE
    {tColDataAppendValue60, tColDataAppendValue61, tColDataAppendValue62},  // HAS_VALUE|HAS_NULL
    {tColDataAppendValue70, tColDataAppendValue71, tColDataAppendValue72},  // HAS_VALUE|HAS_NULL|HAS_NONE
};
int32_t tColDataAppendValue(SColData *pColData, SColVal *pColVal) {
  ASSERT(pColData->cid == pColVal->cid && pColData->type == pColVal->type);
  return tColDataAppendValueImpl[pColData->flag][pColVal->flag](pColData, pColVal);
}

static FORCE_INLINE void tColDataGetValue1(SColData *pColData, int32_t iVal, SColVal *pColVal) {  // HAS_NONE
  *pColVal = COL_VAL_NONE(pColData->cid, pColData->type);
}
static FORCE_INLINE void tColDataGetValue2(SColData *pColData, int32_t iVal, SColVal *pColVal) {  // HAS_NULL
  *pColVal = COL_VAL_NULL(pColData->cid, pColData->type);
}
static FORCE_INLINE void tColDataGetValue3(SColData *pColData, int32_t iVal, SColVal *pColVal) {  // HAS_NULL|HAS_NONE
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
  uint8_t v;
  switch (pColData->flag) {
    case HAS_NONE:
      v = 0;
      break;
    case HAS_NULL:
      v = 1;
      break;
    case (HAS_NULL | HAS_NONE):
      v = GET_BIT1(pColData->pBitMap, iVal);
      break;
    case HAS_VALUE:
      v = 2;
      break;
    case (HAS_VALUE | HAS_NONE):
      v = GET_BIT1(pColData->pBitMap, iVal);
      if (v) v = 2;
      break;
    case (HAS_VALUE | HAS_NULL):
      v = GET_BIT1(pColData->pBitMap, iVal) + 1;
      break;
    case (HAS_VALUE | HAS_NULL | HAS_NONE):
      v = GET_BIT2(pColData->pBitMap, iVal);
      break;
    default:
      ASSERT(0);
      break;
  }
  return v;
}

int32_t tColDataCopy(SColData *pColDataSrc, SColData *pColDataDest) {
  int32_t code = 0;
  int32_t size;

  ASSERT(pColDataSrc->nVal > 0);
  ASSERT(pColDataDest->cid == pColDataSrc->cid);
  ASSERT(pColDataDest->type == pColDataSrc->type);

  pColDataDest->smaOn = pColDataSrc->smaOn;
  pColDataDest->nVal = pColDataSrc->nVal;
  pColDataDest->flag = pColDataSrc->flag;

  // bitmap
  if (pColDataSrc->flag != HAS_NONE && pColDataSrc->flag != HAS_NULL && pColDataSrc->flag != HAS_VALUE) {
    size = BIT2_SIZE(pColDataSrc->nVal);
    code = tRealloc(&pColDataDest->pBitMap, size);
    if (code) goto _exit;
    memcpy(pColDataDest->pBitMap, pColDataSrc->pBitMap, size);
  }

  // offset
  if (IS_VAR_DATA_TYPE(pColDataDest->type)) {
    size = sizeof(int32_t) * pColDataSrc->nVal;

    code = tRealloc((uint8_t **)&pColDataDest->aOffset, size);
    if (code) goto _exit;

    memcpy(pColDataDest->aOffset, pColDataSrc->aOffset, size);
  }

  // value
  pColDataDest->nData = pColDataSrc->nData;
  code = tRealloc(&pColDataDest->pData, pColDataSrc->nData);
  if (code) goto _exit;
  memcpy(pColDataDest->pData, pColDataSrc->pData, pColDataDest->nData);

_exit:
  return code;
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

void (*tColDataCalcSMA[])(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min, int16_t *numOfNull) = {
    NULL,
    tColDataCalcSMABool,           // TSDB_DATA_TYPE_BOOL
    tColDataCalcSMATinyInt,        // TSDB_DATA_TYPE_TINYINT
    tColDataCalcSMATinySmallInt,   // TSDB_DATA_TYPE_SMALLINT
    tColDataCalcSMAInt,            // TSDB_DATA_TYPE_INT
    tColDataCalcSMABigInt,         // TSDB_DATA_TYPE_BIGINT
    tColDataCalcSMAFloat,          // TSDB_DATA_TYPE_FLOAT
    tColDataCalcSMADouble,         // TSDB_DATA_TYPE_DOUBLE
    NULL,                          // TSDB_DATA_TYPE_VARCHAR
    tColDataCalcSMABigInt,         // TSDB_DATA_TYPE_TIMESTAMP
    NULL,                          // TSDB_DATA_TYPE_NCHAR
    tColDataCalcSMAUTinyInt,       // TSDB_DATA_TYPE_UTINYINT
    tColDataCalcSMATinyUSmallInt,  // TSDB_DATA_TYPE_USMALLINT
    tColDataCalcSMAUInt,           // TSDB_DATA_TYPE_UINT
    tColDataCalcSMAUBigInt,        // TSDB_DATA_TYPE_UBIGINT
    NULL,                          // TSDB_DATA_TYPE_JSON
    NULL,                          // TSDB_DATA_TYPE_VARBINARY
    NULL,                          // TSDB_DATA_TYPE_DECIMAL
    NULL,                          // TSDB_DATA_TYPE_BLOB
    NULL                           // TSDB_DATA_TYPE_MEDIUMBLOB
};

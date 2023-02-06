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

#include "tdataformat.h"
#include "tsdb.h"

// SMapData =======================================================================
void tMapDataReset(SMapData *pMapData) {
  pMapData->nItem = 0;
  pMapData->nData = 0;
}

void tMapDataClear(SMapData *pMapData) {
  tFree((uint8_t *)pMapData->aOffset);
  tFree(pMapData->pData);
  pMapData->pData = NULL;
  pMapData->aOffset = NULL;
}

int32_t tMapDataPutItem(SMapData *pMapData, void *pItem, int32_t (*tPutItemFn)(uint8_t *, void *)) {
  int32_t code = 0;
  int32_t offset = pMapData->nData;
  int32_t nItem = pMapData->nItem;

  pMapData->nItem++;
  pMapData->nData += tPutItemFn(NULL, pItem);

  // alloc
  code = tRealloc((uint8_t **)&pMapData->aOffset, sizeof(int32_t) * pMapData->nItem);
  if (code) goto _exit;
  code = tRealloc(&pMapData->pData, pMapData->nData);
  if (code) goto _exit;

  // put
  pMapData->aOffset[nItem] = offset;
  tPutItemFn(pMapData->pData + offset, pItem);

_exit:
  return code;
}

int32_t tMapDataCopy(SMapData *pFrom, SMapData *pTo) {
  int32_t code = 0;

  pTo->nItem = pFrom->nItem;
  pTo->nData = pFrom->nData;
  code = tRealloc((uint8_t **)&pTo->aOffset, sizeof(int32_t) * pFrom->nItem);
  if (code) goto _exit;
  code = tRealloc(&pTo->pData, pFrom->nData);
  if (code) goto _exit;
  memcpy(pTo->aOffset, pFrom->aOffset, sizeof(int32_t) * pFrom->nItem);
  memcpy(pTo->pData, pFrom->pData, pFrom->nData);

_exit:
  return code;
}

int32_t tMapDataSearch(SMapData *pMapData, void *pSearchItem, int32_t (*tGetItemFn)(uint8_t *, void *),
                       int32_t (*tItemCmprFn)(const void *, const void *), void *pItem) {
  int32_t code = 0;
  int32_t lidx = 0;
  int32_t ridx = pMapData->nItem - 1;
  int32_t midx;
  int32_t c;

  while (lidx <= ridx) {
    midx = (lidx + ridx) / 2;

    tMapDataGetItemByIdx(pMapData, midx, pItem, tGetItemFn);

    c = tItemCmprFn(pSearchItem, pItem);
    if (c == 0) {
      goto _exit;
    } else if (c < 0) {
      ridx = midx - 1;
    } else {
      lidx = midx + 1;
    }
  }

  code = TSDB_CODE_NOT_FOUND;

_exit:
  return code;
}

void tMapDataGetItemByIdx(SMapData *pMapData, int32_t idx, void *pItem, int32_t (*tGetItemFn)(uint8_t *, void *)) {
  ASSERT(idx >= 0 && idx < pMapData->nItem);
  tGetItemFn(pMapData->pData + pMapData->aOffset[idx], pItem);
}

int32_t tMapDataToArray(SMapData *pMapData, int32_t itemSize, int32_t (*tGetItemFn)(uint8_t *, void *),
                        SArray **ppArray) {
  int32_t code = 0;

  SArray *pArray = taosArrayInit(pMapData->nItem, itemSize);
  if (pArray == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  for (int32_t i = 0; i < pMapData->nItem; i++) {
    tMapDataGetItemByIdx(pMapData, i, taosArrayReserve(pArray, 1), tGetItemFn);
  }

_exit:
  if (code) {
    *ppArray = NULL;
    if (pArray) taosArrayDestroy(pArray);
  } else {
    *ppArray = pArray;
  }
  return code;
}

int32_t tPutMapData(uint8_t *p, SMapData *pMapData) {
  int32_t n = 0;

  n += tPutI32v(p ? p + n : p, pMapData->nItem);
  if (pMapData->nItem) {
    int32_t lOffset = 0;
    for (int32_t iItem = 0; iItem < pMapData->nItem; iItem++) {
      n += tPutI32v(p ? p + n : p, pMapData->aOffset[iItem] - lOffset);
      lOffset = pMapData->aOffset[iItem];
    }

    n += tPutI32v(p ? p + n : p, pMapData->nData);
    if (p) {
      memcpy(p + n, pMapData->pData, pMapData->nData);
    }
    n += pMapData->nData;
  }

  return n;
}

int32_t tGetMapData(uint8_t *p, SMapData *pMapData) {
  int32_t n = 0;
  int32_t offset;

  tMapDataReset(pMapData);

  n += tGetI32v(p + n, &pMapData->nItem);
  if (pMapData->nItem) {
    if (tRealloc((uint8_t **)&pMapData->aOffset, sizeof(int32_t) * pMapData->nItem)) return -1;

    int32_t lOffset = 0;
    for (int32_t iItem = 0; iItem < pMapData->nItem; iItem++) {
      n += tGetI32v(p + n, &pMapData->aOffset[iItem]);
      pMapData->aOffset[iItem] += lOffset;
      lOffset = pMapData->aOffset[iItem];
    }

    n += tGetI32v(p + n, &pMapData->nData);
    if (tRealloc(&pMapData->pData, pMapData->nData)) return -1;
    memcpy(pMapData->pData, p + n, pMapData->nData);
    n += pMapData->nData;
  }

  return n;
}

// TABLEID =======================================================================
int32_t tTABLEIDCmprFn(const void *p1, const void *p2) {
  TABLEID *pId1 = (TABLEID *)p1;
  TABLEID *pId2 = (TABLEID *)p2;

  if (pId1->suid < pId2->suid) {
    return -1;
  } else if (pId1->suid > pId2->suid) {
    return 1;
  }

  if (pId1->uid < pId2->uid) {
    return -1;
  } else if (pId1->uid > pId2->uid) {
    return 1;
  }

  return 0;
}

// SBlockIdx ======================================================
int32_t tPutBlockIdx(uint8_t *p, void *ph) {
  int32_t    n = 0;
  SBlockIdx *pBlockIdx = (SBlockIdx *)ph;

  n += tPutI64(p ? p + n : p, pBlockIdx->suid);
  n += tPutI64(p ? p + n : p, pBlockIdx->uid);
  n += tPutI64v(p ? p + n : p, pBlockIdx->offset);
  n += tPutI64v(p ? p + n : p, pBlockIdx->size);

  return n;
}

int32_t tGetBlockIdx(uint8_t *p, void *ph) {
  int32_t    n = 0;
  SBlockIdx *pBlockIdx = (SBlockIdx *)ph;

  n += tGetI64(p + n, &pBlockIdx->suid);
  n += tGetI64(p + n, &pBlockIdx->uid);
  n += tGetI64v(p + n, &pBlockIdx->offset);
  n += tGetI64v(p + n, &pBlockIdx->size);

  return n;
}

int32_t tCmprBlockIdx(void const *lhs, void const *rhs) {
  SBlockIdx *lBlockIdx = (SBlockIdx *)lhs;
  SBlockIdx *rBlockIdx = (SBlockIdx *)rhs;

  if (lBlockIdx->suid < rBlockIdx->suid) {
    return -1;
  } else if (lBlockIdx->suid > rBlockIdx->suid) {
    return 1;
  }

  if (lBlockIdx->uid < rBlockIdx->uid) {
    return -1;
  } else if (lBlockIdx->uid > rBlockIdx->uid) {
    return 1;
  }

  return 0;
}

int32_t tCmprBlockL(void const *lhs, void const *rhs) {
  SBlockIdx *lBlockIdx = (SBlockIdx *)lhs;
  SSttBlk   *rBlockL = (SSttBlk *)rhs;

  if (lBlockIdx->suid < rBlockL->suid) {
    return -1;
  } else if (lBlockIdx->suid > rBlockL->suid) {
    return 1;
  }

  if (lBlockIdx->uid < rBlockL->minUid) {
    return -1;
  } else if (lBlockIdx->uid > rBlockL->maxUid) {
    return 1;
  }

  return 0;
}

// SDataBlk ======================================================
void tDataBlkReset(SDataBlk *pDataBlk) {
  *pDataBlk = (SDataBlk){.minKey = TSDBKEY_MAX, .maxKey = TSDBKEY_MIN, .minVer = VERSION_MAX, .maxVer = VERSION_MIN};
}

int32_t tPutDataBlk(uint8_t *p, void *ph) {
  int32_t   n = 0;
  SDataBlk *pDataBlk = (SDataBlk *)ph;

  n += tPutI64v(p ? p + n : p, pDataBlk->minKey.version);
  n += tPutI64v(p ? p + n : p, pDataBlk->minKey.ts);
  n += tPutI64v(p ? p + n : p, pDataBlk->maxKey.version);
  n += tPutI64v(p ? p + n : p, pDataBlk->maxKey.ts);
  n += tPutI64v(p ? p + n : p, pDataBlk->minVer);
  n += tPutI64v(p ? p + n : p, pDataBlk->maxVer);
  n += tPutI32v(p ? p + n : p, pDataBlk->nRow);
  n += tPutI8(p ? p + n : p, pDataBlk->hasDup);
  n += tPutI8(p ? p + n : p, pDataBlk->nSubBlock);
  for (int8_t iSubBlock = 0; iSubBlock < pDataBlk->nSubBlock; iSubBlock++) {
    n += tPutI64v(p ? p + n : p, pDataBlk->aSubBlock[iSubBlock].offset);
    n += tPutI32v(p ? p + n : p, pDataBlk->aSubBlock[iSubBlock].szBlock);
    n += tPutI32v(p ? p + n : p, pDataBlk->aSubBlock[iSubBlock].szKey);
  }
  if (pDataBlk->nSubBlock == 1 && !pDataBlk->hasDup) {
    n += tPutI64v(p ? p + n : p, pDataBlk->smaInfo.offset);
    n += tPutI32v(p ? p + n : p, pDataBlk->smaInfo.size);
  }

  return n;
}

int32_t tGetDataBlk(uint8_t *p, void *ph) {
  int32_t   n = 0;
  SDataBlk *pDataBlk = (SDataBlk *)ph;

  n += tGetI64v(p + n, &pDataBlk->minKey.version);
  n += tGetI64v(p + n, &pDataBlk->minKey.ts);
  n += tGetI64v(p + n, &pDataBlk->maxKey.version);
  n += tGetI64v(p + n, &pDataBlk->maxKey.ts);
  n += tGetI64v(p + n, &pDataBlk->minVer);
  n += tGetI64v(p + n, &pDataBlk->maxVer);
  n += tGetI32v(p + n, &pDataBlk->nRow);
  n += tGetI8(p + n, &pDataBlk->hasDup);
  n += tGetI8(p + n, &pDataBlk->nSubBlock);
  for (int8_t iSubBlock = 0; iSubBlock < pDataBlk->nSubBlock; iSubBlock++) {
    n += tGetI64v(p + n, &pDataBlk->aSubBlock[iSubBlock].offset);
    n += tGetI32v(p + n, &pDataBlk->aSubBlock[iSubBlock].szBlock);
    n += tGetI32v(p + n, &pDataBlk->aSubBlock[iSubBlock].szKey);
  }
  if (pDataBlk->nSubBlock == 1 && !pDataBlk->hasDup) {
    n += tGetI64v(p + n, &pDataBlk->smaInfo.offset);
    n += tGetI32v(p + n, &pDataBlk->smaInfo.size);
  } else {
    pDataBlk->smaInfo.offset = 0;
    pDataBlk->smaInfo.size = 0;
  }

  return n;
}

int32_t tDataBlkCmprFn(const void *p1, const void *p2) {
  SDataBlk *pBlock1 = (SDataBlk *)p1;
  SDataBlk *pBlock2 = (SDataBlk *)p2;

  if (tsdbKeyCmprFn(&pBlock1->maxKey, &pBlock2->minKey) < 0) {
    return -1;
  } else if (tsdbKeyCmprFn(&pBlock1->minKey, &pBlock2->maxKey) > 0) {
    return 1;
  }

  return 0;
}

bool tDataBlkHasSma(SDataBlk *pDataBlk) {
  if (pDataBlk->nSubBlock > 1) return false;
  if (pDataBlk->hasDup) return false;

  return pDataBlk->smaInfo.size > 0;
}

// SSttBlk ======================================================
int32_t tPutSttBlk(uint8_t *p, void *ph) {
  int32_t  n = 0;
  SSttBlk *pSttBlk = (SSttBlk *)ph;

  n += tPutI64(p ? p + n : p, pSttBlk->suid);
  n += tPutI64(p ? p + n : p, pSttBlk->minUid);
  n += tPutI64(p ? p + n : p, pSttBlk->maxUid);
  n += tPutI64v(p ? p + n : p, pSttBlk->minKey);
  n += tPutI64v(p ? p + n : p, pSttBlk->maxKey);
  n += tPutI64v(p ? p + n : p, pSttBlk->minVer);
  n += tPutI64v(p ? p + n : p, pSttBlk->maxVer);
  n += tPutI32v(p ? p + n : p, pSttBlk->nRow);
  n += tPutI64v(p ? p + n : p, pSttBlk->bInfo.offset);
  n += tPutI32v(p ? p + n : p, pSttBlk->bInfo.szBlock);
  n += tPutI32v(p ? p + n : p, pSttBlk->bInfo.szKey);

  return n;
}

int32_t tGetSttBlk(uint8_t *p, void *ph) {
  int32_t  n = 0;
  SSttBlk *pSttBlk = (SSttBlk *)ph;

  n += tGetI64(p + n, &pSttBlk->suid);
  n += tGetI64(p + n, &pSttBlk->minUid);
  n += tGetI64(p + n, &pSttBlk->maxUid);
  n += tGetI64v(p + n, &pSttBlk->minKey);
  n += tGetI64v(p + n, &pSttBlk->maxKey);
  n += tGetI64v(p + n, &pSttBlk->minVer);
  n += tGetI64v(p + n, &pSttBlk->maxVer);
  n += tGetI32v(p + n, &pSttBlk->nRow);
  n += tGetI64v(p + n, &pSttBlk->bInfo.offset);
  n += tGetI32v(p + n, &pSttBlk->bInfo.szBlock);
  n += tGetI32v(p + n, &pSttBlk->bInfo.szKey);

  return n;
}

// SBlockCol ======================================================
int32_t tPutBlockCol(uint8_t *p, void *ph) {
  int32_t    n = 0;
  SBlockCol *pBlockCol = (SBlockCol *)ph;

  ASSERT(pBlockCol->flag && (pBlockCol->flag != HAS_NONE));

  n += tPutI16v(p ? p + n : p, pBlockCol->cid);
  n += tPutI8(p ? p + n : p, pBlockCol->type);
  n += tPutI8(p ? p + n : p, pBlockCol->smaOn);
  n += tPutI8(p ? p + n : p, pBlockCol->flag);
  n += tPutI32v(p ? p + n : p, pBlockCol->szOrigin);

  if (pBlockCol->flag != HAS_NULL) {
    if (pBlockCol->flag != HAS_VALUE) {
      n += tPutI32v(p ? p + n : p, pBlockCol->szBitmap);
    }

    if (IS_VAR_DATA_TYPE(pBlockCol->type)) {
      n += tPutI32v(p ? p + n : p, pBlockCol->szOffset);
    }

    if (pBlockCol->flag != (HAS_NULL | HAS_NONE)) {
      n += tPutI32v(p ? p + n : p, pBlockCol->szValue);
    }

    n += tPutI32v(p ? p + n : p, pBlockCol->offset);
  }

_exit:
  return n;
}

int32_t tGetBlockCol(uint8_t *p, void *ph) {
  int32_t    n = 0;
  SBlockCol *pBlockCol = (SBlockCol *)ph;

  n += tGetI16v(p + n, &pBlockCol->cid);
  n += tGetI8(p + n, &pBlockCol->type);
  n += tGetI8(p + n, &pBlockCol->smaOn);
  n += tGetI8(p + n, &pBlockCol->flag);
  n += tGetI32v(p + n, &pBlockCol->szOrigin);

  ASSERT(pBlockCol->flag && (pBlockCol->flag != HAS_NONE));

  pBlockCol->szBitmap = 0;
  pBlockCol->szOffset = 0;
  pBlockCol->szValue = 0;
  pBlockCol->offset = 0;

  if (pBlockCol->flag != HAS_NULL) {
    if (pBlockCol->flag != HAS_VALUE) {
      n += tGetI32v(p + n, &pBlockCol->szBitmap);
    }

    if (IS_VAR_DATA_TYPE(pBlockCol->type)) {
      n += tGetI32v(p + n, &pBlockCol->szOffset);
    }

    if (pBlockCol->flag != (HAS_NULL | HAS_NONE)) {
      n += tGetI32v(p + n, &pBlockCol->szValue);
    }

    n += tGetI32v(p + n, &pBlockCol->offset);
  }

  return n;
}

int32_t tBlockColCmprFn(const void *p1, const void *p2) {
  if (((SBlockCol *)p1)->cid < ((SBlockCol *)p2)->cid) {
    return -1;
  } else if (((SBlockCol *)p1)->cid > ((SBlockCol *)p2)->cid) {
    return 1;
  }

  return 0;
}

// SDelIdx ======================================================
int32_t tCmprDelIdx(void const *lhs, void const *rhs) {
  SDelIdx *lDelIdx = (SDelIdx *)lhs;
  SDelIdx *rDelIdx = (SDelIdx *)rhs;

  if (lDelIdx->suid < rDelIdx->suid) {
    return -1;
  } else if (lDelIdx->suid > rDelIdx->suid) {
    return 1;
  }

  if (lDelIdx->uid < rDelIdx->uid) {
    return -1;
  } else if (lDelIdx->uid > rDelIdx->uid) {
    return 1;
  }

  return 0;
}

int32_t tPutDelIdx(uint8_t *p, void *ph) {
  SDelIdx *pDelIdx = (SDelIdx *)ph;
  int32_t  n = 0;

  n += tPutI64(p ? p + n : p, pDelIdx->suid);
  n += tPutI64(p ? p + n : p, pDelIdx->uid);
  n += tPutI64v(p ? p + n : p, pDelIdx->offset);
  n += tPutI64v(p ? p + n : p, pDelIdx->size);

  return n;
}

int32_t tGetDelIdx(uint8_t *p, void *ph) {
  SDelIdx *pDelIdx = (SDelIdx *)ph;
  int32_t  n = 0;

  n += tGetI64(p + n, &pDelIdx->suid);
  n += tGetI64(p + n, &pDelIdx->uid);
  n += tGetI64v(p + n, &pDelIdx->offset);
  n += tGetI64v(p + n, &pDelIdx->size);

  return n;
}

// SDelData ======================================================
int32_t tPutDelData(uint8_t *p, void *ph) {
  SDelData *pDelData = (SDelData *)ph;
  int32_t   n = 0;

  n += tPutI64v(p ? p + n : p, pDelData->version);
  n += tPutI64(p ? p + n : p, pDelData->sKey);
  n += tPutI64(p ? p + n : p, pDelData->eKey);

  return n;
}

int32_t tGetDelData(uint8_t *p, void *ph) {
  SDelData *pDelData = (SDelData *)ph;
  int32_t   n = 0;

  n += tGetI64v(p + n, &pDelData->version);
  n += tGetI64(p + n, &pDelData->sKey);
  n += tGetI64(p + n, &pDelData->eKey);

  return n;
}

int32_t tsdbKeyFid(TSKEY key, int32_t minutes, int8_t precision) {
  if (key < 0) {
    return (int)((key + 1) / tsTickPerMin[precision] / minutes - 1);
  } else {
    return (int)((key / tsTickPerMin[precision] / minutes));
  }
}

void tsdbFidKeyRange(int32_t fid, int32_t minutes, int8_t precision, TSKEY *minKey, TSKEY *maxKey) {
  *minKey = tsTickPerMin[precision] * fid * minutes;
  *maxKey = *minKey + tsTickPerMin[precision] * minutes - 1;
}

int32_t tsdbFidLevel(int32_t fid, STsdbKeepCfg *pKeepCfg, int64_t now) {
  int32_t aFid[3];
  TSKEY   key;

  if (pKeepCfg->precision == TSDB_TIME_PRECISION_MILLI) {
    now = now * 1000;
  } else if (pKeepCfg->precision == TSDB_TIME_PRECISION_MICRO) {
    now = now * 1000000l;
  } else if (pKeepCfg->precision == TSDB_TIME_PRECISION_NANO) {
    now = now * 1000000000l;
  } else {
    ASSERT(0);
  }

  key = now - pKeepCfg->keep0 * tsTickPerMin[pKeepCfg->precision];
  aFid[0] = tsdbKeyFid(key, pKeepCfg->days, pKeepCfg->precision);
  key = now - pKeepCfg->keep1 * tsTickPerMin[pKeepCfg->precision];
  aFid[1] = tsdbKeyFid(key, pKeepCfg->days, pKeepCfg->precision);
  key = now - pKeepCfg->keep2 * tsTickPerMin[pKeepCfg->precision];
  aFid[2] = tsdbKeyFid(key, pKeepCfg->days, pKeepCfg->precision);

  if (fid >= aFid[0]) {
    return 0;
  } else if (fid >= aFid[1]) {
    return 1;
  } else if (fid >= aFid[2]) {
    return 2;
  } else {
    return -1;
  }
}

// TSDBROW ======================================================
void tsdbRowGetColVal(TSDBROW *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal) {
  STColumn *pTColumn = &pTSchema->columns[iCol];
  SValue    value;

  ASSERT(iCol > 0);

  if (pRow->type == 0) {
    tTSRowGetVal(pRow->pTSRow, pTSchema, iCol, pColVal);
  } else if (pRow->type == 1) {
    SColData *pColData;

    tBlockDataGetColData(pRow->pBlockData, pTColumn->colId, &pColData);

    if (pColData) {
      tColDataGetValue(pColData, pRow->iRow, pColVal);
    } else {
      *pColVal = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
    }
  } else {
    ASSERT(0);
  }
}

// int32_t tPutTSDBRow(uint8_t *p, TSDBROW *pRow) {
//   int32_t n = 0;

//   n += tPutI64(p, pRow->version);
//   if (p) memcpy(p + n, pRow->pTSRow, pRow->pTSRow->len);
//   n += pRow->pTSRow->len;

//   return n;
// }

int32_t tsdbRowCmprFn(const void *p1, const void *p2) {
  return tsdbKeyCmprFn(&TSDBROW_KEY((TSDBROW *)p1), &TSDBROW_KEY((TSDBROW *)p2));
}

// STSDBRowIter ======================================================
void tsdbRowIterInit(STSDBRowIter *pIter, TSDBROW *pRow, STSchema *pTSchema) {
  pIter->pRow = pRow;
  if (pRow->type == 0) {
    ASSERT(pTSchema);
    pIter->pTSchema = pTSchema;
    pIter->i = 1;
  } else if (pRow->type == 1) {
    pIter->pTSchema = NULL;
    pIter->i = 0;
  } else {
    ASSERT(0);
  }
}

SColVal *tsdbRowIterNext(STSDBRowIter *pIter) {
  if (pIter->pRow->type == 0) {
    if (pIter->i < pIter->pTSchema->numOfCols) {
      tTSRowGetVal(pIter->pRow->pTSRow, pIter->pTSchema, pIter->i, &pIter->colVal);
      pIter->i++;

      return &pIter->colVal;
    }
  } else {
    if (pIter->i < pIter->pRow->pBlockData->nColData) {
      SColData *pColData = tBlockDataGetColDataByIdx(pIter->pRow->pBlockData, pIter->i);

      tColDataGetValue(pColData, pIter->pRow->iRow, &pIter->colVal);
      pIter->i++;

      return &pIter->colVal;
    }
  }

  return NULL;
}

// SRowMerger ======================================================

int32_t tRowMergerInit2(SRowMerger *pMerger, STSchema *pResTSchema, TSDBROW *pRow, STSchema *pTSchema) {
  int32_t   code = 0;
  TSDBKEY   key = TSDBROW_KEY(pRow);
  SColVal  *pColVal = &(SColVal){0};
  STColumn *pTColumn;
  int32_t   iCol, jCol = 0;

  pMerger->pTSchema = pResTSchema;
  pMerger->version = key.version;

  pMerger->pArray = taosArrayInit(pResTSchema->numOfCols, sizeof(SColVal));
  if (pMerger->pArray == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // ts
  pTColumn = &pTSchema->columns[jCol++];

  ASSERT(pTColumn->type == TSDB_DATA_TYPE_TIMESTAMP);

  *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.val = key.ts});
  if (taosArrayPush(pMerger->pArray, pColVal) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // other
  for (iCol = 1; jCol < pTSchema->numOfCols && iCol < pResTSchema->numOfCols; ++iCol) {
    pTColumn = &pResTSchema->columns[iCol];
    if (pTSchema->columns[jCol].colId < pTColumn->colId) {
      ++jCol;
      --iCol;
      continue;
    } else if (pTSchema->columns[jCol].colId > pTColumn->colId) {
      taosArrayPush(pMerger->pArray, &COL_VAL_NONE(pTColumn->colId, pTColumn->type));
      continue;
    }

    tsdbRowGetColVal(pRow, pTSchema, jCol++, pColVal);
    if ((!COL_VAL_IS_NONE(pColVal)) && (!COL_VAL_IS_NULL(pColVal)) && IS_VAR_DATA_TYPE(pColVal->type)) {
      uint8_t *pVal = pColVal->value.pData;

      pColVal->value.pData = NULL;
      code = tRealloc(&pColVal->value.pData, pColVal->value.nData);
      if (code) goto _exit;

      if (pColVal->value.nData) {
        memcpy(pColVal->value.pData, pVal, pColVal->value.nData);
      }
    }

    if (taosArrayPush(pMerger->pArray, pColVal) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

  for (; iCol < pResTSchema->numOfCols; ++iCol) {
    pTColumn = &pResTSchema->columns[iCol];
    taosArrayPush(pMerger->pArray, &COL_VAL_NONE(pTColumn->colId, pTColumn->type));
  }

_exit:
  return code;
}

int32_t tRowMergerAdd(SRowMerger *pMerger, TSDBROW *pRow, STSchema *pTSchema) {
  int32_t   code = 0;
  TSDBKEY   key = TSDBROW_KEY(pRow);
  SColVal  *pColVal = &(SColVal){0};
  STColumn *pTColumn;
  int32_t   iCol, jCol = 1;

  ASSERT(((SColVal *)pMerger->pArray->pData)->value.val == key.ts);

  for (iCol = 1; iCol < pMerger->pTSchema->numOfCols && jCol < pTSchema->numOfCols; ++iCol) {
    pTColumn = &pMerger->pTSchema->columns[iCol];
    if (pTSchema->columns[jCol].colId < pTColumn->colId) {
      ++jCol;
      --iCol;
      continue;
    } else if (pTSchema->columns[jCol].colId > pTColumn->colId) {
      continue;
    }

    tsdbRowGetColVal(pRow, pTSchema, jCol++, pColVal);

    if (key.version > pMerger->version) {
#if 0
      if (!COL_VAL_IS_NONE(pColVal)) {
        if ((!COL_VAL_IS_NULL(pColVal)) && IS_VAR_DATA_TYPE(pColVal->type)) {
          SColVal *tColVal = taosArrayGet(pMerger->pArray, iCol);
          code = tRealloc(&tColVal->value.pData, pColVal->value.nData);
          if (code) return code;

          tColVal->value.nData = pColVal->value.nData;
          if (pColVal->value.nData) {
            memcpy(tColVal->value.pData, pColVal->value.pData, pColVal->value.nData);
          }
          tColVal->flag = 0;
        } else {
          taosArraySet(pMerger->pArray, iCol, pColVal);
        }
      }
#endif
      if (!COL_VAL_IS_NONE(pColVal)) {
        if (IS_VAR_DATA_TYPE(pColVal->type)) {
          SColVal *pTColVal = taosArrayGet(pMerger->pArray, iCol);
          if (!COL_VAL_IS_NULL(pColVal)) {
            code = tRealloc(&pTColVal->value.pData, pColVal->value.nData);
            if (code) return code;

            pTColVal->value.nData = pColVal->value.nData;
            if (pTColVal->value.nData) {
              memcpy(pTColVal->value.pData, pColVal->value.pData, pTColVal->value.nData);
            }
            pTColVal->flag = 0;
          } else {
            tFree(pTColVal->value.pData);
            pTColVal->value.pData = NULL;
            taosArraySet(pMerger->pArray, iCol, pColVal);
          }
        } else {
          taosArraySet(pMerger->pArray, iCol, pColVal);
        }
      }
    } else if (key.version < pMerger->version) {
      SColVal *tColVal = (SColVal *)taosArrayGet(pMerger->pArray, iCol);
      if (COL_VAL_IS_NONE(tColVal) && !COL_VAL_IS_NONE(pColVal)) {
        if ((!COL_VAL_IS_NULL(pColVal)) && IS_VAR_DATA_TYPE(pColVal->type)) {
          code = tRealloc(&tColVal->value.pData, pColVal->value.nData);
          if (code) return code;

          tColVal->value.nData = pColVal->value.nData;
          if (pColVal->value.nData) {
            memcpy(tColVal->value.pData, pColVal->value.pData, pColVal->value.nData);
          }
          tColVal->flag = 0;
        } else {
          taosArraySet(pMerger->pArray, iCol, pColVal);
        }
      }
    } else {
      ASSERT(0 && "dup versions not allowed");
    }
  }

  pMerger->version = key.version;
  return code;
}

int32_t tRowMergerInit(SRowMerger *pMerger, TSDBROW *pRow, STSchema *pTSchema) {
  int32_t   code = 0;
  TSDBKEY   key = TSDBROW_KEY(pRow);
  SColVal  *pColVal = &(SColVal){0};
  STColumn *pTColumn;

  pMerger->pTSchema = pTSchema;
  pMerger->version = key.version;

  pMerger->pArray = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
  if (pMerger->pArray == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // ts
  pTColumn = &pTSchema->columns[0];

  ASSERT(pTColumn->type == TSDB_DATA_TYPE_TIMESTAMP);

  *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.val = key.ts});
  if (taosArrayPush(pMerger->pArray, pColVal) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // other
  for (int16_t iCol = 1; iCol < pTSchema->numOfCols; iCol++) {
    tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal);
    if ((!COL_VAL_IS_NONE(pColVal)) && (!COL_VAL_IS_NULL(pColVal)) && IS_VAR_DATA_TYPE(pColVal->type)) {
      uint8_t *pVal = pColVal->value.pData;

      pColVal->value.pData = NULL;
      code = tRealloc(&pColVal->value.pData, pColVal->value.nData);
      if (code) goto _exit;

      if (pColVal->value.nData) {
        memcpy(pColVal->value.pData, pVal, pColVal->value.nData);
      }
    }

    if (taosArrayPush(pMerger->pArray, pColVal) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

_exit:
  return code;
}

void tRowMergerClear(SRowMerger *pMerger) {
  for (int32_t iCol = 1; iCol < pMerger->pTSchema->numOfCols; iCol++) {
    SColVal *pTColVal = taosArrayGet(pMerger->pArray, iCol);
    if (IS_VAR_DATA_TYPE(pTColVal->type)) {
      tFree(pTColVal->value.pData);
    }
  }

  taosArrayDestroy(pMerger->pArray);
}

int32_t tRowMerge(SRowMerger *pMerger, TSDBROW *pRow) {
  int32_t  code = 0;
  TSDBKEY  key = TSDBROW_KEY(pRow);
  SColVal *pColVal = &(SColVal){0};

  ASSERT(((SColVal *)pMerger->pArray->pData)->value.val == key.ts);

  for (int32_t iCol = 1; iCol < pMerger->pTSchema->numOfCols; iCol++) {
    tsdbRowGetColVal(pRow, pMerger->pTSchema, iCol, pColVal);

    if (key.version > pMerger->version) {
      if (!COL_VAL_IS_NONE(pColVal)) {
        if (IS_VAR_DATA_TYPE(pColVal->type)) {
          SColVal *pTColVal = taosArrayGet(pMerger->pArray, iCol);
          if (!COL_VAL_IS_NULL(pColVal)) {
            code = tRealloc(&pTColVal->value.pData, pColVal->value.nData);
            if (code) goto _exit;

            pTColVal->value.nData = pColVal->value.nData;
            if (pTColVal->value.nData) {
              memcpy(pTColVal->value.pData, pColVal->value.pData, pTColVal->value.nData);
            }
            pTColVal->flag = 0;
          } else {
            tFree(pTColVal->value.pData);
            pTColVal->value.pData = NULL;
            taosArraySet(pMerger->pArray, iCol, pColVal);
          }
        } else {
          taosArraySet(pMerger->pArray, iCol, pColVal);
        }
      }
    } else if (key.version < pMerger->version) {
      SColVal *tColVal = (SColVal *)taosArrayGet(pMerger->pArray, iCol);
      if (COL_VAL_IS_NONE(tColVal) && !COL_VAL_IS_NONE(pColVal)) {
        if (IS_VAR_DATA_TYPE(pColVal->type)) {
          if (!COL_VAL_IS_NULL(pColVal)) {
            code = tRealloc(&tColVal->value.pData, pColVal->value.nData);
            if (code) goto _exit;

            tColVal->value.nData = pColVal->value.nData;
            if (tColVal->value.nData) {
              memcpy(tColVal->value.pData, pColVal->value.pData, tColVal->value.nData);
            }
            tColVal->flag = 0;
          } else {
            tFree(tColVal->value.pData);
            tColVal->value.pData = NULL;
            taosArraySet(pMerger->pArray, iCol, pColVal);
          }
        } else {
          taosArraySet(pMerger->pArray, iCol, pColVal);
        }
      }
    } else {
      ASSERT(0);
    }
  }

  pMerger->version = key.version;

_exit:
  return code;
}

int32_t tRowMergerGetRow(SRowMerger *pMerger, STSRow **ppRow) {
  int32_t code = 0;

  code = tdSTSRowNew(pMerger->pArray, pMerger->pTSchema, ppRow);

  return code;
}

/*
// delete skyline ======================================================
static int32_t tsdbMergeSkyline2(SArray *aSkyline1, SArray *aSkyline2, SArray *aSkyline) {
  int32_t  code = 0;
  int32_t  i1 = 0;
  int32_t  n1 = taosArrayGetSize(aSkyline1);
  int32_t  i2 = 0;
  int32_t  n2 = taosArrayGetSize(aSkyline2);
  TSDBKEY *pSkyline1;
  TSDBKEY *pSkyline2;
  TSDBKEY  item;
  int64_t  version1 = 0;
  int64_t  version2 = 0;

  ASSERT(n1 > 0 && n2 > 0);

  taosArrayClear(aSkyline);

  while (i1 < n1 && i2 < n2) {
    pSkyline1 = (TSDBKEY *)taosArrayGet(aSkyline1, i1);
    pSkyline2 = (TSDBKEY *)taosArrayGet(aSkyline2, i2);

    if (pSkyline1->ts < pSkyline2->ts) {
      version1 = pSkyline1->version;
      i1++;
    } else if (pSkyline1->ts > pSkyline2->ts) {
      version2 = pSkyline2->version;
      i2++;
    } else {
      version1 = pSkyline1->version;
      version2 = pSkyline2->version;
      i1++;
      i2++;
    }

    item.ts = TMIN(pSkyline1->ts, pSkyline2->ts);
    item.version = TMAX(version1, version2);
    if (taosArrayPush(aSkyline, &item) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

  while (i1 < n1) {
    pSkyline1 = (TSDBKEY *)taosArrayGet(aSkyline1, i1);
    item.ts = pSkyline1->ts;
    item.version = pSkyline1->version;
    if (taosArrayPush(aSkyline, &item) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    i1++;
  }

  while (i2 < n2) {
    pSkyline2 = (TSDBKEY *)taosArrayGet(aSkyline2, i2);
    item.ts = pSkyline2->ts;
    item.version = pSkyline2->version;
    if (taosArrayPush(aSkyline, &item) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    i2++;
  }

_exit:
  return code;
}
*/

// delete skyline ======================================================
static int32_t tsdbMergeSkyline(SArray *pSkyline1, SArray *pSkyline2, SArray *pSkyline) {
  int32_t  code = 0;
  int32_t  i1 = 0;
  int32_t  n1 = taosArrayGetSize(pSkyline1);
  int32_t  i2 = 0;
  int32_t  n2 = taosArrayGetSize(pSkyline2);
  TSDBKEY *pKey1;
  TSDBKEY *pKey2;
  int64_t  version1 = 0;
  int64_t  version2 = 0;

  ASSERT(n1 > 0 && n2 > 0);

  taosArrayClear(pSkyline);
  TSDBKEY **pItem = TARRAY_GET_ELEM(pSkyline, 0);

  while (i1 < n1 && i2 < n2) {
    pKey1 = (TSDBKEY *)taosArrayGetP(pSkyline1, i1);
    pKey2 = (TSDBKEY *)taosArrayGetP(pSkyline2, i2);

    if (pKey1->ts < pKey2->ts) {
      version1 = pKey1->version;
      *pItem = pKey1;
      i1++;
    } else if (pKey1->ts > pKey2->ts) {
      version2 = pKey2->version;
      *pItem = pKey2;
      i2++;
    } else {
      version1 = pKey1->version;
      version2 = pKey2->version;
      *pItem = pKey1;
      i1++;
      i2++;
    }

    (*pItem)->version = TMAX(version1, version2);
    pItem++;
  }

  while (i1 < n1) {
    pKey1 = (TSDBKEY *)taosArrayGetP(pSkyline1, i1);
    *pItem = pKey1;
    pItem++;
    i1++;
  }

  while (i2 < n2) {
    pKey2 = (TSDBKEY *)taosArrayGetP(pSkyline2, i2);
    *pItem = pKey2;
    pItem++;
    i2++;
  }

  taosArraySetSize(pSkyline, TARRAY_ELEM_IDX(pSkyline, pItem));

_exit:
  return code;
}


int32_t tsdbBuildDeleteSkylineImpl(SArray *aSkyline, int32_t sidx, int32_t eidx, SArray *pSkyline) {
  int32_t   code = 0;
  SDelData *pDelData;
  int32_t   midx;

  taosArrayClear(pSkyline);
  if (sidx == eidx) {    
    TSDBKEY *pItem1 = taosArrayGet(aSkyline, sidx * 2);
    TSDBKEY *pItem2 = taosArrayGet(aSkyline, sidx * 2 + 1);
    taosArrayPush(pSkyline, &pItem1);
    taosArrayPush(pSkyline, &pItem2);
  } else {
    SArray *pSkyline1 = NULL;
    SArray *pSkyline2 = NULL;
    midx = (sidx + eidx) / 2;

    pSkyline1 = taosArrayInit((midx - sidx + 1) * 2, POINTER_BYTES);
    pSkyline2 = taosArrayInit((eidx - midx) * 2, POINTER_BYTES);
    if (pSkyline1 == NULL || pSkyline1 == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _clear;
    }

    code = tsdbBuildDeleteSkylineImpl(aSkyline, sidx, midx, pSkyline1);
    if (code) goto _clear;

    code = tsdbBuildDeleteSkylineImpl(aSkyline, midx + 1, eidx, pSkyline2);
    if (code) goto _clear;

    code = tsdbMergeSkyline(pSkyline1, pSkyline2, pSkyline);

  _clear:
    taosArrayDestroy(pSkyline1);
    taosArrayDestroy(pSkyline2);
  }

  return code;
}


int32_t tsdbBuildDeleteSkyline(SArray *aDelData, int32_t sidx, int32_t eidx, SArray *aSkyline) {
  SDelData *pDelData;
  int32_t code = 0;
  int32_t dataNum = eidx - sidx + 1;
  SArray *aTmpSkyline = taosArrayInit(dataNum * 2, sizeof(TSDBKEY));
  SArray *pSkyline = taosArrayInit(dataNum * 2, POINTER_BYTES);
  
  for (int32_t i = sidx; i <= eidx; ++i) {
    pDelData = (SDelData *)taosArrayGet(aDelData, i);
    taosArrayPush(aTmpSkyline, &(TSDBKEY){.ts = pDelData->sKey, .version = pDelData->version});
    taosArrayPush(aTmpSkyline, &(TSDBKEY){.ts = pDelData->eKey, .version = 0});
  }

  code = tsdbBuildDeleteSkylineImpl(aTmpSkyline, sidx, eidx, pSkyline);
  if (code) goto _clear;

  int32_t skylineNum = taosArrayGetSize(pSkyline);
  for (int32_t i = 0; i < skylineNum; ++i) {
     TSDBKEY *p = taosArrayGetP(pSkyline, i);
     taosArrayPush(aSkyline, p);
  }

_clear:
  taosArrayDestroy(aTmpSkyline);
  taosArrayDestroy(pSkyline);

  return code;
}

/*
int32_t tsdbBuildDeleteSkyline2(SArray *aDelData, int32_t sidx, int32_t eidx, SArray *aSkyline) {
  int32_t   code = 0;
  SDelData *pDelData;
  int32_t   midx;

  taosArrayClear(aSkyline);
  if (sidx == eidx) {
    pDelData = (SDelData *)taosArrayGet(aDelData, sidx);
    taosArrayPush(aSkyline, &(TSDBKEY){.ts = pDelData->sKey, .version = pDelData->version});
    taosArrayPush(aSkyline, &(TSDBKEY){.ts = pDelData->eKey, .version = 0});
  } else {
    SArray *aSkyline1 = NULL;
    SArray *aSkyline2 = NULL;

    aSkyline1 = taosArrayInit(0, sizeof(TSDBKEY));
    aSkyline2 = taosArrayInit(0, sizeof(TSDBKEY));
    if (aSkyline1 == NULL || aSkyline2 == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _clear;
    }
    midx = (sidx + eidx) / 2;

    code = tsdbBuildDeleteSkyline(aDelData, sidx, midx, aSkyline1);
    if (code) goto _clear;

    code = tsdbBuildDeleteSkyline(aDelData, midx + 1, eidx, aSkyline2);
    if (code) goto _clear;

    code = tsdbMergeSkyline(aSkyline1, aSkyline2, aSkyline);

  _clear:
    taosArrayDestroy(aSkyline1);
    taosArrayDestroy(aSkyline2);
  }

  return code;
}
*/

// SBlockData ======================================================
int32_t tBlockDataCreate(SBlockData *pBlockData) {
  int32_t code = 0;

  pBlockData->suid = 0;
  pBlockData->uid = 0;
  pBlockData->nRow = 0;
  pBlockData->aUid = NULL;
  pBlockData->aVersion = NULL;
  pBlockData->aTSKEY = NULL;
  pBlockData->nColData = 0;
  pBlockData->aColData = taosArrayInit(0, sizeof(SColData));
  if (pBlockData->aColData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  return code;
}

void tBlockDataDestroy(SBlockData *pBlockData, int8_t deepClear) {
  tFree((uint8_t *)pBlockData->aUid);
  tFree((uint8_t *)pBlockData->aVersion);
  tFree((uint8_t *)pBlockData->aTSKEY);
  taosArrayDestroyEx(pBlockData->aColData, deepClear ? tColDataDestroy : NULL);
  pBlockData->aUid = NULL;
  pBlockData->aVersion = NULL;
  pBlockData->aTSKEY = NULL;
  pBlockData->aColData = NULL;
}

int32_t tBlockDataInit(SBlockData *pBlockData, TABLEID *pId, STSchema *pTSchema, int16_t *aCid, int32_t nCid) {
  int32_t code = 0;

  ASSERT(pId->suid || pId->uid);

  pBlockData->suid = pId->suid;
  pBlockData->uid = pId->uid;
  pBlockData->nRow = 0;

  pBlockData->nColData = 0;
  if (aCid) {
    int32_t   iColumn = 1;
    STColumn *pTColumn = &pTSchema->columns[iColumn];
    for (int32_t iCid = 0; iCid < nCid; iCid++) {
      while (pTColumn && pTColumn->colId < aCid[iCid]) {
        iColumn++;
        pTColumn = (iColumn < pTSchema->numOfCols) ? &pTSchema->columns[iColumn] : NULL;
      }

      if (pTColumn == NULL) {
        break;
      } else if (pTColumn->colId == aCid[iCid]) {
        SColData *pColData;
        code = tBlockDataAddColData(pBlockData, &pColData);
        if (code) goto _exit;
        tColDataInit(pColData, pTColumn->colId, pTColumn->type, (pTColumn->flags & COL_SMA_ON) ? 1 : 0);

        iColumn++;
        pTColumn = (iColumn < pTSchema->numOfCols) ? &pTSchema->columns[iColumn] : NULL;
      }
    }
  } else {
    for (int32_t iColumn = 1; iColumn < pTSchema->numOfCols; iColumn++) {
      STColumn *pTColumn = &pTSchema->columns[iColumn];

      SColData *pColData;
      code = tBlockDataAddColData(pBlockData, &pColData);
      if (code) goto _exit;

      tColDataInit(pColData, pTColumn->colId, pTColumn->type, (pTColumn->flags & COL_SMA_ON) ? 1 : 0);
    }
  }

_exit:
  return code;
}

void tBlockDataReset(SBlockData *pBlockData) {
  pBlockData->suid = 0;
  pBlockData->uid = 0;
  pBlockData->nRow = 0;
  pBlockData->nColData = 0;
}

void tBlockDataClear(SBlockData *pBlockData) {
  ASSERT(pBlockData->suid || pBlockData->uid);

  pBlockData->nRow = 0;
  for (int32_t iColData = 0; iColData < pBlockData->nColData; iColData++) {
    SColData *pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);
    tColDataClear(pColData);
  }
}

int32_t tBlockDataAddColData(SBlockData *pBlockData, SColData **ppColData) {
  int32_t   code = 0;
  SColData *pColData = NULL;

  if (pBlockData->nColData >= taosArrayGetSize(pBlockData->aColData)) {
    if (taosArrayPush(pBlockData->aColData, &((SColData){0})) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }
  pColData = (SColData *)taosArrayGet(pBlockData->aColData, pBlockData->nColData);

  pBlockData->nColData++;

  *ppColData = pColData;
  return code;

_err:
  *ppColData = NULL;
  return code;
}

static int32_t tBlockDataAppendBlockRow(SBlockData *pBlockData, SBlockData *pBlockDataFrom, int32_t iRow) {
  int32_t code = 0;

  SColVal   cv = {0};
  int32_t   iColDataFrom = 0;
  SColData *pColDataFrom =
      (iColDataFrom < pBlockDataFrom->nColData) ? &((SColData *)pBlockDataFrom->aColData->pData)[iColDataFrom] : NULL;

  for (int32_t iColDataTo = 0; iColDataTo < pBlockData->nColData; iColDataTo++) {
    SColData *pColDataTo = &((SColData *)pBlockData->aColData->pData)[iColDataTo];

    while (pColDataFrom && pColDataFrom->cid < pColDataTo->cid) {
      iColDataFrom++;
      pColDataFrom = (iColDataFrom < pBlockDataFrom->nColData)
                         ? &((SColData *)pBlockDataFrom->aColData->pData)[iColDataFrom]
                         : NULL;
    }

    if (pColDataFrom == NULL || pColDataFrom->cid > pColDataTo->cid) {
      code = tColDataAppendValue(pColDataTo, &COL_VAL_NONE(pColDataTo->cid, pColDataTo->type));
      if (code) goto _exit;
    } else {
      tColDataGetValue(pColDataFrom, iRow, &cv);

      code = tColDataAppendValue(pColDataTo, &cv);
      if (code) goto _exit;

      iColDataFrom++;
      pColDataFrom = (iColDataFrom < pBlockDataFrom->nColData)
                         ? &((SColData *)pBlockDataFrom->aColData->pData)[iColDataFrom]
                         : NULL;
    }
  }

_exit:
  return code;
}

static int32_t tBlockDataAppendTPRow(SBlockData *pBlockData, STSRow *pRow, STSchema *pTSchema) {
  int32_t code = 0;

  int32_t   iTColumn = 1;
  STColumn *pTColumn = (iTColumn < pTSchema->numOfCols) ? &pTSchema->columns[iTColumn] : NULL;
  void     *pBitmap = pRow->statis ? tdGetBitmapAddrTp(pRow, pTSchema->flen) : NULL;

  for (int32_t iColData = 0; iColData < pBlockData->nColData; iColData++) {
    SColData *pColData = &((SColData *)pBlockData->aColData->pData)[iColData];

    while (pTColumn && pTColumn->colId < pColData->cid) {
      iTColumn++;
      pTColumn = (iTColumn < pTSchema->numOfCols) ? &pTSchema->columns[iTColumn] : NULL;
    }

    if (pTColumn == NULL || pTColumn->colId > pColData->cid) {
      code = tColDataAppendValue(pColData, &COL_VAL_NONE(pColData->cid, pColData->type));
      if (code) goto _exit;
    } else {
      ASSERT(pTColumn->type == pColData->type);

      SColVal cv = {.cid = pTColumn->colId, .type = pTColumn->type};

      if (pRow->statis) {
        TDRowValT vt = TD_VTYPE_MAX;
        tdGetBitmapValTypeII(pBitmap, iTColumn - 1, &vt);

        if (vt == TD_VTYPE_NORM) {
          cv.flag = CV_FLAG_VALUE;

          if (IS_VAR_DATA_TYPE(pTColumn->type)) {
            void *pData = (char *)pRow + *(int32_t *)(pRow->data + pTColumn->offset);
            cv.value.nData = varDataLen(pData);
            cv.value.pData = varDataVal(pData);
          } else {
            memcpy(&cv.value.val, pRow->data + pTColumn->offset, pTColumn->bytes);
          }

          code = tColDataAppendValue(pColData, &cv);
          if (code) goto _exit;
        } else if (vt == TD_VTYPE_NONE) {
          code = tColDataAppendValue(pColData, &COL_VAL_NONE(pColData->cid, pColData->type));
          if (code) goto _exit;
        } else if (vt == TD_VTYPE_NULL) {
          code = tColDataAppendValue(pColData, &COL_VAL_NULL(pColData->cid, pColData->type));
          if (code) goto _exit;
        } else {
          ASSERT(0);
        }
      } else {
        cv.flag = CV_FLAG_VALUE;

        if (IS_VAR_DATA_TYPE(pTColumn->type)) {
          void *pData = (char *)pRow + *(int32_t *)(pRow->data + pTColumn->offset);
          cv.value.nData = varDataLen(pData);
          cv.value.pData = varDataVal(pData);
        } else {
          memcpy(&cv.value.val, pRow->data + pTColumn->offset, pTColumn->bytes);
        }

        code = tColDataAppendValue(pColData, &cv);
        if (code) goto _exit;
      }

      iTColumn++;
      pTColumn = (iTColumn < pTSchema->numOfCols) ? &pTSchema->columns[iTColumn] : NULL;
    }
  }

_exit:
  return code;
}

static int32_t tBlockDataAppendKVRow(SBlockData *pBlockData, STSRow *pRow, STSchema *pTSchema) {
  int32_t code = 0;

  col_id_t  kvIter = 0;
  col_id_t  nKvCols = tdRowGetNCols(pRow) - 1;
  void     *pColIdx = TD_ROW_COL_IDX(pRow);
  void     *pBitmap = tdGetBitmapAddrKv(pRow, tdRowGetNCols(pRow));
  int32_t   iTColumn = 1;
  STColumn *pTColumn = (iTColumn < pTSchema->numOfCols) ? &pTSchema->columns[iTColumn] : NULL;

  for (int32_t iColData = 0; iColData < pBlockData->nColData; iColData++) {
    SColData *pColData = &((SColData *)pBlockData->aColData->pData)[iColData];

    while (pTColumn && pTColumn->colId < pColData->cid) {
      iTColumn++;
      pTColumn = (iTColumn < pTSchema->numOfCols) ? &pTSchema->columns[iTColumn] : NULL;
    }

    if (pTColumn == NULL || pTColumn->colId > pColData->cid) {
      code = tColDataAppendValue(pColData, &COL_VAL_NONE(pColData->cid, pColData->type));
      if (code) goto _exit;
    } else {
      ASSERT(pTColumn->type == pColData->type);

      SColVal    cv = {.cid = pTColumn->colId, .type = pTColumn->type};
      TDRowValT  vt = TD_VTYPE_NONE;  // default is NONE
      SKvRowIdx *pKvIdx = NULL;

      while (kvIter < nKvCols) {
        pKvIdx = (SKvRowIdx *)POINTER_SHIFT(pColIdx, kvIter * sizeof(SKvRowIdx));
        if (pKvIdx->colId == pTColumn->colId) {
          tdGetBitmapValTypeII(pBitmap, kvIter, &vt);
          ++kvIter;
          break;
        } else if (pKvIdx->colId > pTColumn->colId) {
          vt = TD_VTYPE_NONE;
          break;
        } else {
          ++kvIter;
        }
      }

      if (vt == TD_VTYPE_NORM) {
        cv.flag = CV_FLAG_VALUE;

        void *pData = POINTER_SHIFT(pRow, pKvIdx->offset);
        if (IS_VAR_DATA_TYPE(pTColumn->type)) {
          cv.value.nData = varDataLen(pData);
          cv.value.pData = varDataVal(pData);
        } else {
          memcpy(&cv.value.val, pData, pTColumn->bytes);
        }

        code = tColDataAppendValue(pColData, &cv);
        if (code) goto _exit;
      } else if (vt == TD_VTYPE_NONE) {
        code = tColDataAppendValue(pColData, &COL_VAL_NONE(pColData->cid, pColData->type));
        if (code) goto _exit;
      } else if (vt == TD_VTYPE_NULL) {
        code = tColDataAppendValue(pColData, &COL_VAL_NULL(pColData->cid, pColData->type));
        if (code) goto _exit;
      } else {
        ASSERT(0);
      }

      iTColumn++;
      pTColumn = (iTColumn < pTSchema->numOfCols) ? &pTSchema->columns[iTColumn] : NULL;
    }
  }

_exit:
  return code;
}

int32_t tBlockDataAppendRow(SBlockData *pBlockData, TSDBROW *pRow, STSchema *pTSchema, int64_t uid) {
  int32_t code = 0;

  ASSERT(pBlockData->suid || pBlockData->uid);

  // uid
  if (pBlockData->uid == 0) {
    ASSERT(uid);
    code = tRealloc((uint8_t **)&pBlockData->aUid, sizeof(int64_t) * (pBlockData->nRow + 1));
    if (code) goto _err;
    pBlockData->aUid[pBlockData->nRow] = uid;
  }
  // version
  code = tRealloc((uint8_t **)&pBlockData->aVersion, sizeof(int64_t) * (pBlockData->nRow + 1));
  if (code) goto _err;
  pBlockData->aVersion[pBlockData->nRow] = TSDBROW_VERSION(pRow);
  // timestamp
  code = tRealloc((uint8_t **)&pBlockData->aTSKEY, sizeof(TSKEY) * (pBlockData->nRow + 1));
  if (code) goto _err;
  pBlockData->aTSKEY[pBlockData->nRow] = TSDBROW_TS(pRow);

  SColVal cv = {0};
  if (pRow->type == 0) {
    if (TD_IS_TP_ROW(pRow->pTSRow)) {
      code = tBlockDataAppendTPRow(pBlockData, pRow->pTSRow, pTSchema);
      if (code) goto _err;
    } else if (TD_IS_KV_ROW(pRow->pTSRow)) {
      code = tBlockDataAppendKVRow(pBlockData, pRow->pTSRow, pTSchema);
      if (code) goto _err;
    } else {
      ASSERT(0);
    }
  } else {
    code = tBlockDataAppendBlockRow(pBlockData, pRow->pBlockData, pRow->iRow);
    if (code) goto _err;
  }
  pBlockData->nRow++;

  return code;

_err:
  return code;
}

int32_t tBlockDataCorrectSchema(SBlockData *pBlockData, SBlockData *pBlockDataFrom) {
  int32_t code = 0;

  int32_t iColData = 0;
  for (int32_t iColDataFrom = 0; iColDataFrom < pBlockDataFrom->nColData; iColDataFrom++) {
    SColData *pColDataFrom = tBlockDataGetColDataByIdx(pBlockDataFrom, iColDataFrom);

    while (true) {
      SColData *pColData;
      if (iColData < pBlockData->nColData) {
        pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);
      } else {
        pColData = NULL;
      }

      if (pColData == NULL || pColData->cid > pColDataFrom->cid) {
        code = tBlockDataAddColData(pBlockData, &pColData);
        if (code) goto _exit;

        tColDataInit(pColData, pColDataFrom->cid, pColDataFrom->type, pColDataFrom->smaOn);
        for (int32_t iRow = 0; iRow < pBlockData->nRow; iRow++) {
          code = tColDataAppendValue(pColData, &COL_VAL_NONE(pColData->cid, pColData->type));
          if (code) goto _exit;
        }

        iColData++;
        break;
      } else if (pColData->cid == pColDataFrom->cid) {
        iColData++;
        break;
      } else {
        iColData++;
      }
    }
  }

_exit:
  return code;
}

int32_t tBlockDataMerge(SBlockData *pBlockData1, SBlockData *pBlockData2, SBlockData *pBlockData) {
  int32_t code = 0;

  ASSERT(pBlockData->suid == pBlockData1->suid);
  ASSERT(pBlockData->uid == pBlockData1->uid);
  ASSERT(pBlockData1->nRow > 0);
  ASSERT(pBlockData2->nRow > 0);

  tBlockDataClear(pBlockData);

  TSDBROW  row1 = tsdbRowFromBlockData(pBlockData1, 0);
  TSDBROW  row2 = tsdbRowFromBlockData(pBlockData2, 0);
  TSDBROW *pRow1 = &row1;
  TSDBROW *pRow2 = &row2;

  while (pRow1 && pRow2) {
    int32_t c = tsdbRowCmprFn(pRow1, pRow2);

    if (c < 0) {
      code = tBlockDataAppendRow(pBlockData, pRow1, NULL,
                                 pBlockData1->uid ? pBlockData1->uid : pBlockData1->aUid[pRow1->iRow]);
      if (code) goto _exit;

      pRow1->iRow++;
      if (pRow1->iRow < pBlockData1->nRow) {
        *pRow1 = tsdbRowFromBlockData(pBlockData1, pRow1->iRow);
      } else {
        pRow1 = NULL;
      }
    } else if (c > 0) {
      code = tBlockDataAppendRow(pBlockData, pRow2, NULL,
                                 pBlockData2->uid ? pBlockData2->uid : pBlockData2->aUid[pRow2->iRow]);
      if (code) goto _exit;

      pRow2->iRow++;
      if (pRow2->iRow < pBlockData2->nRow) {
        *pRow2 = tsdbRowFromBlockData(pBlockData2, pRow2->iRow);
      } else {
        pRow2 = NULL;
      }
    } else {
      ASSERT(0);
    }
  }

  while (pRow1) {
    code = tBlockDataAppendRow(pBlockData, pRow1, NULL,
                               pBlockData1->uid ? pBlockData1->uid : pBlockData1->aUid[pRow1->iRow]);
    if (code) goto _exit;

    pRow1->iRow++;
    if (pRow1->iRow < pBlockData1->nRow) {
      *pRow1 = tsdbRowFromBlockData(pBlockData1, pRow1->iRow);
    } else {
      pRow1 = NULL;
    }
  }

  while (pRow2) {
    code = tBlockDataAppendRow(pBlockData, pRow2, NULL,
                               pBlockData2->uid ? pBlockData2->uid : pBlockData2->aUid[pRow2->iRow]);
    if (code) goto _exit;

    pRow2->iRow++;
    if (pRow2->iRow < pBlockData2->nRow) {
      *pRow2 = tsdbRowFromBlockData(pBlockData2, pRow2->iRow);
    } else {
      pRow2 = NULL;
    }
  }

_exit:
  return code;
}

SColData *tBlockDataGetColDataByIdx(SBlockData *pBlockData, int32_t idx) {
  ASSERT(idx >= 0 && idx < pBlockData->nColData);
  return (SColData *)taosArrayGet(pBlockData->aColData, idx);
}

void tBlockDataGetColData(SBlockData *pBlockData, int16_t cid, SColData **ppColData) {
  ASSERT(cid != PRIMARYKEY_TIMESTAMP_COL_ID);
  int32_t lidx = 0;
  int32_t ridx = pBlockData->nColData - 1;

  while (lidx <= ridx) {
    int32_t   midx = (lidx + ridx) / 2;
    SColData *pColData = tBlockDataGetColDataByIdx(pBlockData, midx);
    int32_t   c = (pColData->cid == cid) ? 0 : ((pColData->cid > cid) ? 1 : -1);

    if (c == 0) {
      *ppColData = pColData;
      return;
    } else if (c < 0) {
      lidx = midx + 1;
    } else {
      ridx = midx - 1;
    }
  }

  *ppColData = NULL;
}

int32_t tCmprBlockData(SBlockData *pBlockData, int8_t cmprAlg, uint8_t **ppOut, int32_t *szOut, uint8_t *aBuf[],
                       int32_t aBufN[]) {
  int32_t code = 0;

  SDiskDataHdr hdr = {.delimiter = TSDB_FILE_DLMT,
                      .fmtVer = 0,
                      .suid = pBlockData->suid,
                      .uid = pBlockData->uid,
                      .nRow = pBlockData->nRow,
                      .cmprAlg = cmprAlg};

  // encode =================
  // columns AND SBlockCol
  aBufN[0] = 0;
  for (int32_t iColData = 0; iColData < pBlockData->nColData; iColData++) {
    SColData *pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);

    ASSERT(pColData->flag);

    if (pColData->flag == HAS_NONE) continue;

    SBlockCol blockCol = {.cid = pColData->cid,
                          .type = pColData->type,
                          .smaOn = pColData->smaOn,
                          .flag = pColData->flag,
                          .szOrigin = pColData->nData};

    if (pColData->flag != HAS_NULL) {
      code = tsdbCmprColData(pColData, cmprAlg, &blockCol, &aBuf[0], aBufN[0], &aBuf[2]);
      if (code) goto _exit;

      blockCol.offset = aBufN[0];
      aBufN[0] = aBufN[0] + blockCol.szBitmap + blockCol.szOffset + blockCol.szValue;
    }

    code = tRealloc(&aBuf[1], hdr.szBlkCol + tPutBlockCol(NULL, &blockCol));
    if (code) goto _exit;
    hdr.szBlkCol += tPutBlockCol(aBuf[1] + hdr.szBlkCol, &blockCol);
  }

  // SBlockCol
  aBufN[1] = hdr.szBlkCol;

  // uid + version + tskey
  aBufN[2] = 0;
  if (pBlockData->uid == 0) {
    code = tsdbCmprData((uint8_t *)pBlockData->aUid, sizeof(int64_t) * pBlockData->nRow, TSDB_DATA_TYPE_BIGINT, cmprAlg,
                        &aBuf[2], aBufN[2], &hdr.szUid, &aBuf[3]);
    if (code) goto _exit;
  }
  aBufN[2] += hdr.szUid;

  code = tsdbCmprData((uint8_t *)pBlockData->aVersion, sizeof(int64_t) * pBlockData->nRow, TSDB_DATA_TYPE_BIGINT,
                      cmprAlg, &aBuf[2], aBufN[2], &hdr.szVer, &aBuf[3]);
  if (code) goto _exit;
  aBufN[2] += hdr.szVer;

  code = tsdbCmprData((uint8_t *)pBlockData->aTSKEY, sizeof(TSKEY) * pBlockData->nRow, TSDB_DATA_TYPE_TIMESTAMP,
                      cmprAlg, &aBuf[2], aBufN[2], &hdr.szKey, &aBuf[3]);
  if (code) goto _exit;
  aBufN[2] += hdr.szKey;

  // hdr
  aBufN[3] = tPutDiskDataHdr(NULL, &hdr);
  code = tRealloc(&aBuf[3], aBufN[3]);
  if (code) goto _exit;
  tPutDiskDataHdr(aBuf[3], &hdr);

  // aggragate
  if (ppOut) {
    *szOut = aBufN[0] + aBufN[1] + aBufN[2] + aBufN[3];
    code = tRealloc(ppOut, *szOut);
    if (code) goto _exit;

    memcpy(*ppOut, aBuf[3], aBufN[3]);
    memcpy(*ppOut + aBufN[3], aBuf[2], aBufN[2]);
    if (aBufN[1]) {
      memcpy(*ppOut + aBufN[3] + aBufN[2], aBuf[1], aBufN[1]);
    }
    if (aBufN[0]) {
      memcpy(*ppOut + aBufN[3] + aBufN[2] + aBufN[1], aBuf[0], aBufN[0]);
    }
  }

_exit:
  return code;
}

int32_t tDecmprBlockData(uint8_t *pIn, int32_t szIn, SBlockData *pBlockData, uint8_t *aBuf[]) {
  int32_t code = 0;

  tBlockDataReset(pBlockData);

  int32_t      n = 0;
  SDiskDataHdr hdr = {0};

  // SDiskDataHdr
  n += tGetDiskDataHdr(pIn + n, &hdr);
  ASSERT(hdr.delimiter == TSDB_FILE_DLMT);

  pBlockData->suid = hdr.suid;
  pBlockData->uid = hdr.uid;
  pBlockData->nRow = hdr.nRow;

  // uid
  if (hdr.uid == 0) {
    ASSERT(hdr.szUid);
    code = tsdbDecmprData(pIn + n, hdr.szUid, TSDB_DATA_TYPE_BIGINT, hdr.cmprAlg, (uint8_t **)&pBlockData->aUid,
                          sizeof(int64_t) * hdr.nRow, &aBuf[0]);
    if (code) goto _exit;
  } else {
    ASSERT(!hdr.szUid);
  }
  n += hdr.szUid;

  // version
  code = tsdbDecmprData(pIn + n, hdr.szVer, TSDB_DATA_TYPE_BIGINT, hdr.cmprAlg, (uint8_t **)&pBlockData->aVersion,
                        sizeof(int64_t) * hdr.nRow, &aBuf[0]);
  if (code) goto _exit;
  n += hdr.szVer;

  // TSKEY
  code = tsdbDecmprData(pIn + n, hdr.szKey, TSDB_DATA_TYPE_TIMESTAMP, hdr.cmprAlg, (uint8_t **)&pBlockData->aTSKEY,
                        sizeof(TSKEY) * hdr.nRow, &aBuf[0]);
  if (code) goto _exit;
  n += hdr.szKey;

  // loop to decode each column data
  if (hdr.szBlkCol == 0) goto _exit;

  int32_t nt = 0;
  while (nt < hdr.szBlkCol) {
    SBlockCol blockCol = {0};
    nt += tGetBlockCol(pIn + n + nt, &blockCol);
    ASSERT(nt <= hdr.szBlkCol);

    SColData *pColData;
    code = tBlockDataAddColData(pBlockData, &pColData);
    if (code) goto _exit;

    tColDataInit(pColData, blockCol.cid, blockCol.type, blockCol.smaOn);
    if (blockCol.flag == HAS_NULL) {
      for (int32_t iRow = 0; iRow < hdr.nRow; iRow++) {
        code = tColDataAppendValue(pColData, &COL_VAL_NULL(blockCol.cid, blockCol.type));
        if (code) goto _exit;
      }
    } else {
      code = tsdbDecmprColData(pIn + n + hdr.szBlkCol + blockCol.offset, &blockCol, hdr.cmprAlg, hdr.nRow, pColData,
                               &aBuf[0]);
      if (code) goto _exit;
    }
  }

_exit:
  return code;
}

// SDiskDataHdr ==============================
int32_t tPutDiskDataHdr(uint8_t *p, const SDiskDataHdr *pHdr) {
  int32_t n = 0;

  n += tPutU32(p ? p + n : p, pHdr->delimiter);
  n += tPutU32v(p ? p + n : p, pHdr->fmtVer);
  n += tPutI64(p ? p + n : p, pHdr->suid);
  n += tPutI64(p ? p + n : p, pHdr->uid);
  n += tPutI32v(p ? p + n : p, pHdr->szUid);
  n += tPutI32v(p ? p + n : p, pHdr->szVer);
  n += tPutI32v(p ? p + n : p, pHdr->szKey);
  n += tPutI32v(p ? p + n : p, pHdr->szBlkCol);
  n += tPutI32v(p ? p + n : p, pHdr->nRow);
  n += tPutI8(p ? p + n : p, pHdr->cmprAlg);

  return n;
}

int32_t tGetDiskDataHdr(uint8_t *p, void *ph) {
  int32_t       n = 0;
  SDiskDataHdr *pHdr = (SDiskDataHdr *)ph;

  n += tGetU32(p + n, &pHdr->delimiter);
  n += tGetU32v(p + n, &pHdr->fmtVer);
  n += tGetI64(p + n, &pHdr->suid);
  n += tGetI64(p + n, &pHdr->uid);
  n += tGetI32v(p + n, &pHdr->szUid);
  n += tGetI32v(p + n, &pHdr->szVer);
  n += tGetI32v(p + n, &pHdr->szKey);
  n += tGetI32v(p + n, &pHdr->szBlkCol);
  n += tGetI32v(p + n, &pHdr->nRow);
  n += tGetI8(p + n, &pHdr->cmprAlg);

  return n;
}

// ALGORITHM ==============================
int32_t tPutColumnDataAgg(uint8_t *p, SColumnDataAgg *pColAgg) {
  int32_t n = 0;

  n += tPutI16v(p ? p + n : p, pColAgg->colId);
  n += tPutI16v(p ? p + n : p, pColAgg->numOfNull);
  n += tPutI64(p ? p + n : p, pColAgg->sum);
  n += tPutI64(p ? p + n : p, pColAgg->max);
  n += tPutI64(p ? p + n : p, pColAgg->min);

  return n;
}

int32_t tGetColumnDataAgg(uint8_t *p, SColumnDataAgg *pColAgg) {
  int32_t n = 0;

  n += tGetI16v(p + n, &pColAgg->colId);
  n += tGetI16v(p + n, &pColAgg->numOfNull);
  n += tGetI64(p + n, &pColAgg->sum);
  n += tGetI64(p + n, &pColAgg->max);
  n += tGetI64(p + n, &pColAgg->min);

  return n;
}

int32_t tsdbCmprData(uint8_t *pIn, int32_t szIn, int8_t type, int8_t cmprAlg, uint8_t **ppOut, int32_t nOut,
                     int32_t *szOut, uint8_t **ppBuf) {
  int32_t code = 0;

  ASSERT(szIn > 0 && ppOut);

  if (cmprAlg == NO_COMPRESSION) {
    code = tRealloc(ppOut, nOut + szIn);
    if (code) goto _exit;

    memcpy(*ppOut + nOut, pIn, szIn);
    *szOut = szIn;
  } else {
    int32_t size = szIn + COMP_OVERFLOW_BYTES;

    code = tRealloc(ppOut, nOut + size);
    if (code) goto _exit;

    if (cmprAlg == TWO_STAGE_COMP) {
      ASSERT(ppBuf);
      code = tRealloc(ppBuf, size);
      if (code) goto _exit;
    }

    *szOut =
        tDataTypes[type].compFunc(pIn, szIn, szIn / tDataTypes[type].bytes, *ppOut + nOut, size, cmprAlg, *ppBuf, size);
    if (*szOut <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _exit;
    }
  }

_exit:
  return code;
}

int32_t tsdbDecmprData(uint8_t *pIn, int32_t szIn, int8_t type, int8_t cmprAlg, uint8_t **ppOut, int32_t szOut,
                       uint8_t **ppBuf) {
  int32_t code = 0;

  code = tRealloc(ppOut, szOut);
  if (code) goto _exit;

  if (cmprAlg == NO_COMPRESSION) {
    ASSERT(szIn == szOut);
    memcpy(*ppOut, pIn, szOut);
  } else {
    if (cmprAlg == TWO_STAGE_COMP) {
      code = tRealloc(ppBuf, szOut + COMP_OVERFLOW_BYTES);
      if (code) goto _exit;
    }

    int32_t size = tDataTypes[type].decompFunc(pIn, szIn, szOut / tDataTypes[type].bytes, *ppOut, szOut, cmprAlg,
                                               *ppBuf, szOut + COMP_OVERFLOW_BYTES);
    if (size <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _exit;
    }

    ASSERT(size == szOut);
  }

_exit:
  return code;
}

int32_t tsdbCmprColData(SColData *pColData, int8_t cmprAlg, SBlockCol *pBlockCol, uint8_t **ppOut, int32_t nOut,
                        uint8_t **ppBuf) {
  int32_t code = 0;

  ASSERT(pColData->flag && (pColData->flag != HAS_NONE) && (pColData->flag != HAS_NULL));

  pBlockCol->szBitmap = 0;
  pBlockCol->szOffset = 0;
  pBlockCol->szValue = 0;

  int32_t size = 0;
  // bitmap
  if (pColData->flag != HAS_VALUE) {
    int32_t szBitMap;
    if (pColData->flag == (HAS_VALUE | HAS_NULL | HAS_NONE)) {
      szBitMap = BIT2_SIZE(pColData->nVal);
    } else {
      szBitMap = BIT1_SIZE(pColData->nVal);
    }

    code = tsdbCmprData(pColData->pBitMap, szBitMap, TSDB_DATA_TYPE_TINYINT, cmprAlg, ppOut, nOut + size,
                        &pBlockCol->szBitmap, ppBuf);
    if (code) goto _exit;
  }
  size += pBlockCol->szBitmap;

  // offset
  if (IS_VAR_DATA_TYPE(pColData->type) && pColData->flag != (HAS_NULL | HAS_NONE)) {
    code = tsdbCmprData((uint8_t *)pColData->aOffset, sizeof(int32_t) * pColData->nVal, TSDB_DATA_TYPE_INT, cmprAlg,
                        ppOut, nOut + size, &pBlockCol->szOffset, ppBuf);
    if (code) goto _exit;
  }
  size += pBlockCol->szOffset;

  // value
  if ((pColData->flag != (HAS_NULL | HAS_NONE)) && pColData->nData) {
    code = tsdbCmprData((uint8_t *)pColData->pData, pColData->nData, pColData->type, cmprAlg, ppOut, nOut + size,
                        &pBlockCol->szValue, ppBuf);
    if (code) goto _exit;
  }
  size += pBlockCol->szValue;

_exit:
  return code;
}

int32_t tsdbDecmprColData(uint8_t *pIn, SBlockCol *pBlockCol, int8_t cmprAlg, int32_t nVal, SColData *pColData,
                          uint8_t **ppBuf) {
  int32_t code = 0;

  ASSERT(pColData->cid == pBlockCol->cid);
  ASSERT(pColData->type == pBlockCol->type);
  pColData->smaOn = pBlockCol->smaOn;
  pColData->flag = pBlockCol->flag;
  pColData->nVal = nVal;
  pColData->nData = pBlockCol->szOrigin;

  uint8_t *p = pIn;
  // bitmap
  if (pBlockCol->szBitmap) {
    int32_t szBitMap;
    if (pColData->flag == (HAS_VALUE | HAS_NULL | HAS_NONE)) {
      szBitMap = BIT2_SIZE(pColData->nVal);
    } else {
      szBitMap = BIT1_SIZE(pColData->nVal);
    }

    code = tsdbDecmprData(p, pBlockCol->szBitmap, TSDB_DATA_TYPE_TINYINT, cmprAlg, &pColData->pBitMap, szBitMap, ppBuf);
    if (code) goto _exit;
  }
  p += pBlockCol->szBitmap;

  // offset
  if (pBlockCol->szOffset) {
    code = tsdbDecmprData(p, pBlockCol->szOffset, TSDB_DATA_TYPE_INT, cmprAlg, (uint8_t **)&pColData->aOffset,
                          sizeof(int32_t) * pColData->nVal, ppBuf);
    if (code) goto _exit;
  }
  p += pBlockCol->szOffset;

  // value
  if (pBlockCol->szValue) {
    code = tsdbDecmprData(p, pBlockCol->szValue, pColData->type, cmprAlg, &pColData->pData, pColData->nData, ppBuf);
    if (code) goto _exit;
  }
  p += pBlockCol->szValue;

_exit:
  return code;
}

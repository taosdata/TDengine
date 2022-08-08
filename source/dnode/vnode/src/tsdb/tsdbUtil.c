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

// SBlock ======================================================
void tBlockReset(SBlock *pBlock) {
  *pBlock = (SBlock){.minKey = TSDBKEY_MAX, .maxKey = TSDBKEY_MIN, .minVer = VERSION_MAX, .maxVer = VERSION_MIN};
}

int32_t tPutBlock(uint8_t *p, void *ph) {
  int32_t n = 0;
  SBlock *pBlock = (SBlock *)ph;

  n += tPutI64v(p ? p + n : p, pBlock->minKey.version);
  n += tPutI64v(p ? p + n : p, pBlock->minKey.ts);
  n += tPutI64v(p ? p + n : p, pBlock->maxKey.version);
  n += tPutI64v(p ? p + n : p, pBlock->maxKey.ts);
  n += tPutI64v(p ? p + n : p, pBlock->minVer);
  n += tPutI64v(p ? p + n : p, pBlock->maxVer);
  n += tPutI32v(p ? p + n : p, pBlock->nRow);
  n += tPutI8(p ? p + n : p, pBlock->hasDup);
  n += tPutI8(p ? p + n : p, pBlock->nSubBlock);
  for (int8_t iSubBlock = 0; iSubBlock < pBlock->nSubBlock; iSubBlock++) {
    n += tPutI64v(p ? p + n : p, pBlock->aSubBlock[iSubBlock].offset);
    n += tPutI32v(p ? p + n : p, pBlock->aSubBlock[iSubBlock].szBlock);
    n += tPutI32v(p ? p + n : p, pBlock->aSubBlock[iSubBlock].szKey);
  }
  if (pBlock->nSubBlock == 1 && !pBlock->hasDup) {
    n += tPutI64v(p ? p + n : p, pBlock->smaInfo.offset);
    n += tPutI32v(p ? p + n : p, pBlock->smaInfo.size);
  }

  return n;
}

int32_t tGetBlock(uint8_t *p, void *ph) {
  int32_t n = 0;
  SBlock *pBlock = (SBlock *)ph;

  n += tGetI64v(p + n, &pBlock->minKey.version);
  n += tGetI64v(p + n, &pBlock->minKey.ts);
  n += tGetI64v(p + n, &pBlock->maxKey.version);
  n += tGetI64v(p + n, &pBlock->maxKey.ts);
  n += tGetI64v(p + n, &pBlock->minVer);
  n += tGetI64v(p + n, &pBlock->maxVer);
  n += tGetI32v(p + n, &pBlock->nRow);
  n += tGetI8(p + n, &pBlock->hasDup);
  n += tGetI8(p + n, &pBlock->nSubBlock);
  for (int8_t iSubBlock = 0; iSubBlock < pBlock->nSubBlock; iSubBlock++) {
    n += tGetI64v(p + n, &pBlock->aSubBlock[iSubBlock].offset);
    n += tGetI32v(p + n, &pBlock->aSubBlock[iSubBlock].szBlock);
    n += tGetI32v(p + n, &pBlock->aSubBlock[iSubBlock].szKey);
  }
  if (pBlock->nSubBlock == 1 && !pBlock->hasDup) {
    n += tGetI64v(p + n, &pBlock->smaInfo.offset);
    n += tGetI32v(p + n, &pBlock->smaInfo.size);
  } else {
    pBlock->smaInfo.offset = 0;
    pBlock->smaInfo.size = 0;
  }

  return n;
}

int32_t tBlockCmprFn(const void *p1, const void *p2) {
  SBlock *pBlock1 = (SBlock *)p1;
  SBlock *pBlock2 = (SBlock *)p2;

  if (tsdbKeyCmprFn(&pBlock1->maxKey, &pBlock2->minKey) < 0) {
    return -1;
  } else if (tsdbKeyCmprFn(&pBlock1->minKey, &pBlock2->maxKey) > 0) {
    return 1;
  }

  return 0;
}

bool tBlockHasSma(SBlock *pBlock) {
  if (pBlock->nSubBlock > 1) return false;
  if (pBlock->hasDup) return false;

  return pBlock->smaInfo.size > 0;
}

// SBlockL ======================================================
int32_t tPutBlockL(uint8_t *p, void *ph) {
  int32_t  n = 0;
  SBlockL *pBlockL = (SBlockL *)ph;

  n += tPutI64(p ? p + n : p, pBlockL->suid);
  n += tPutI64(p ? p + n : p, pBlockL->minUid);
  n += tPutI64(p ? p + n : p, pBlockL->maxUid);
  n += tPutI64v(p ? p + n : p, pBlockL->minVer);
  n += tPutI64v(p ? p + n : p, pBlockL->maxVer);
  n += tPutI32v(p ? p + n : p, pBlockL->nRow);
  n += tPutI64v(p ? p + n : p, pBlockL->bInfo.offset);
  n += tPutI32v(p ? p + n : p, pBlockL->bInfo.szBlock);
  n += tPutI32v(p ? p + n : p, pBlockL->bInfo.szKey);

  return n;
}

int32_t tGetBlockL(uint8_t *p, void *ph) {
  int32_t  n = 0;
  SBlockL *pBlockL = (SBlockL *)ph;

  n += tGetI64(p + n, &pBlockL->suid);
  n += tGetI64(p + n, &pBlockL->minUid);
  n += tGetI64(p + n, &pBlockL->maxUid);
  n += tGetI64v(p + n, &pBlockL->minVer);
  n += tGetI64v(p + n, &pBlockL->maxVer);
  n += tGetI32v(p + n, &pBlockL->nRow);
  n += tGetI64v(p + n, &pBlockL->bInfo.offset);
  n += tGetI32v(p + n, &pBlockL->bInfo.szBlock);
  n += tGetI32v(p + n, &pBlockL->bInfo.szKey);

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
  *minKey = fid * minutes * tsTickPerMin[precision];
  *maxKey = *minKey + minutes * tsTickPerMin[precision] - 1;
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

int32_t tPutTSDBRow(uint8_t *p, TSDBROW *pRow) {
  int32_t n = 0;

  n += tPutI64(p, pRow->version);
  if (p) memcpy(p + n, pRow->pTSRow, pRow->pTSRow->len);
  n += pRow->pTSRow->len;

  return n;
}

int32_t tGetTSDBRow(uint8_t *p, TSDBROW *pRow) {
  int32_t n = 0;

  n += tGetI64(p, &pRow->version);
  pRow->pTSRow = (STSRow *)(p + n);
  n += pRow->pTSRow->len;

  return n;
}

int32_t tsdbRowCmprFn(const void *p1, const void *p2) {
  return tsdbKeyCmprFn(&TSDBROW_KEY((TSDBROW *)p1), &TSDBROW_KEY((TSDBROW *)p2));
}

// SRowIter ======================================================
void tRowIterInit(SRowIter *pIter, TSDBROW *pRow, STSchema *pTSchema) {
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

SColVal *tRowIterNext(SRowIter *pIter) {
  if (pIter->pRow->type == 0) {
    if (pIter->i < pIter->pTSchema->numOfCols) {
      tsdbRowGetColVal(pIter->pRow, pIter->pTSchema, pIter->i, &pIter->colVal);
      pIter->i++;

      return &pIter->colVal;
    }
  } else {
    if (pIter->i < taosArrayGetSize(pIter->pRow->pBlockData->aIdx)) {
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

  *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.ts = key.ts});
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

  ASSERT(((SColVal *)pMerger->pArray->pData)->value.ts == key.ts);

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
      if (!pColVal->isNone) {
        taosArraySet(pMerger->pArray, iCol, pColVal);
      }
    } else if (key.version < pMerger->version) {
      SColVal *tColVal = (SColVal *)taosArrayGet(pMerger->pArray, iCol);
      if (tColVal->isNone && !pColVal->isNone) {
        taosArraySet(pMerger->pArray, iCol, pColVal);
      }
    } else {
      ASSERT(0);
    }
  }

  pMerger->version = key.version;

_exit:
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

  *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.ts = key.ts});
  if (taosArrayPush(pMerger->pArray, pColVal) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // other
  for (int16_t iCol = 1; iCol < pTSchema->numOfCols; iCol++) {
    tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal);
    if (taosArrayPush(pMerger->pArray, pColVal) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

_exit:
  return code;
}

void tRowMergerClear(SRowMerger *pMerger) { taosArrayDestroy(pMerger->pArray); }

int32_t tRowMerge(SRowMerger *pMerger, TSDBROW *pRow) {
  int32_t  code = 0;
  TSDBKEY  key = TSDBROW_KEY(pRow);
  SColVal *pColVal = &(SColVal){0};

  ASSERT(((SColVal *)pMerger->pArray->pData)->value.ts == key.ts);

  for (int32_t iCol = 1; iCol < pMerger->pTSchema->numOfCols; iCol++) {
    tsdbRowGetColVal(pRow, pMerger->pTSchema, iCol, pColVal);

    if (key.version > pMerger->version) {
      if (!pColVal->isNone) {
        taosArraySet(pMerger->pArray, iCol, pColVal);
      }
    } else if (key.version < pMerger->version) {
      SColVal *tColVal = (SColVal *)taosArrayGet(pMerger->pArray, iCol);
      if (tColVal->isNone && !pColVal->isNone) {
        taosArraySet(pMerger->pArray, iCol, pColVal);
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

// delete skyline ======================================================
static int32_t tsdbMergeSkyline(SArray *aSkyline1, SArray *aSkyline2, SArray *aSkyline) {
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
int32_t tsdbBuildDeleteSkyline(SArray *aDelData, int32_t sidx, int32_t eidx, SArray *aSkyline) {
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

// SColData ========================================
void tColDataInit(SColData *pColData, int16_t cid, int8_t type, int8_t smaOn) {
  pColData->cid = cid;
  pColData->type = type;
  pColData->smaOn = smaOn;
  tColDataReset(pColData);
}

void tColDataReset(SColData *pColData) {
  pColData->nVal = 0;
  pColData->flag = 0;
  pColData->nData = 0;
}

void tColDataClear(void *ph) {
  SColData *pColData = (SColData *)ph;

  tFree(pColData->pBitMap);
  tFree((uint8_t *)pColData->aOffset);
  tFree(pColData->pData);
}

int32_t tColDataAppendValue(SColData *pColData, SColVal *pColVal) {
  int32_t code = 0;
  int64_t size;
  SValue  value = {0};
  SValue *pValue = &value;

  ASSERT(pColVal->cid == pColData->cid);
  ASSERT(pColVal->type == pColData->type);

  // realloc bitmap
  size = BIT2_SIZE(pColData->nVal + 1);
  code = tRealloc(&pColData->pBitMap, size);
  if (code) goto _exit;

  // put value
  if (pColVal->isNone) {
    pColData->flag |= HAS_NONE;
    SET_BIT2(pColData->pBitMap, pColData->nVal, 0);
  } else if (pColVal->isNull) {
    pColData->flag |= HAS_NULL;
    SET_BIT2(pColData->pBitMap, pColData->nVal, 1);
  } else {
    pColData->flag |= HAS_VALUE;
    SET_BIT2(pColData->pBitMap, pColData->nVal, 2);
    pValue = &pColVal->value;
  }

  if (IS_VAR_DATA_TYPE(pColData->type)) {
    // offset
    code = tRealloc((uint8_t **)&pColData->aOffset, sizeof(int32_t) * (pColData->nVal + 1));
    if (code) goto _exit;
    pColData->aOffset[pColData->nVal] = pColData->nData;

    // value
    if ((!pColVal->isNone) && (!pColVal->isNull)) {
      code = tRealloc(&pColData->pData, pColData->nData + pColVal->value.nData);
      if (code) goto _exit;
      memcpy(pColData->pData + pColData->nData, pColVal->value.pData, pColVal->value.nData);
      pColData->nData += pColVal->value.nData;
    }
  } else {
    code = tRealloc(&pColData->pData, pColData->nData + tPutValue(NULL, pValue, pColVal->type));
    if (code) goto _exit;
    pColData->nData += tPutValue(pColData->pData + pColData->nData, pValue, pColVal->type);
  }

  pColData->nVal++;

_exit:
  return code;
}

int32_t tColDataCopy(SColData *pColDataSrc, SColData *pColDataDest) {
  int32_t code = 0;
  int32_t size;

  ASSERT(pColDataSrc->nVal > 0);
  ASSERT(pColDataDest->cid = pColDataSrc->cid);
  ASSERT(pColDataDest->type = pColDataSrc->type);

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

int32_t tColDataGetValue(SColData *pColData, int32_t iVal, SColVal *pColVal) {
  int32_t code = 0;

  ASSERT(iVal < pColData->nVal);
  ASSERT(pColData->flag);

  if (pColData->flag == HAS_NONE) {
    *pColVal = COL_VAL_NONE(pColData->cid, pColData->type);
    goto _exit;
  } else if (pColData->flag == HAS_NULL) {
    *pColVal = COL_VAL_NULL(pColData->cid, pColData->type);
    goto _exit;
  } else if (pColData->flag != HAS_VALUE) {
    uint8_t v = GET_BIT2(pColData->pBitMap, iVal);
    if (v == 0) {
      *pColVal = COL_VAL_NONE(pColData->cid, pColData->type);
      goto _exit;
    } else if (v == 1) {
      *pColVal = COL_VAL_NULL(pColData->cid, pColData->type);
      goto _exit;
    }
  }

  // get value
  SValue value;
  if (IS_VAR_DATA_TYPE(pColData->type)) {
    if (iVal + 1 < pColData->nVal) {
      value.nData = pColData->aOffset[iVal + 1] - pColData->aOffset[iVal];
    } else {
      value.nData = pColData->nData - pColData->aOffset[iVal];
    }

    value.pData = pColData->pData + pColData->aOffset[iVal];
  } else {
    tGetValue(pColData->pData + tDataTypes[pColData->type].bytes * iVal, &value, pColData->type);
  }
  *pColVal = COL_VAL_VALUE(pColData->cid, pColData->type, value);

_exit:
  return code;
}

int32_t tPutColData(uint8_t *p, SColData *pColData) {
  int32_t n = 0;

  n += tPutI16v(p ? p + n : p, pColData->cid);
  n += tPutI8(p ? p + n : p, pColData->type);
  n += tPutI8(p ? p + n : p, pColData->smaOn);
  n += tPutI32v(p ? p + n : p, pColData->nVal);
  n += tPutU8(p ? p + n : p, pColData->flag);

  if (pColData->flag == HAS_NONE || pColData->flag == HAS_NULL) goto _exit;
  if (pColData->flag != HAS_VALUE) {
    // bitmap

    int32_t size = BIT2_SIZE(pColData->nVal);
    if (p) {
      memcpy(p + n, pColData->pBitMap, size);
    }
    n += size;
  }
  if (IS_VAR_DATA_TYPE(pColData->type)) {
    // offset

    int32_t size = sizeof(int32_t) * pColData->nVal;
    if (p) {
      memcpy(p + n, pColData->aOffset, size);
    }
    n += size;
  }
  n += tPutI32v(p ? p + n : p, pColData->nData);
  if (p) {
    memcpy(p + n, pColData->pData, pColData->nData);
  }
  n += pColData->nData;

_exit:
  return n;
}

int32_t tGetColData(uint8_t *p, SColData *pColData) {
  int32_t n = 0;

  n += tGetI16v(p + n, &pColData->cid);
  n += tGetI8(p + n, &pColData->type);
  n += tGetI8(p + n, &pColData->smaOn);
  n += tGetI32v(p + n, &pColData->nVal);
  n += tGetU8(p + n, &pColData->flag);

  if (pColData->flag == HAS_NONE || pColData->flag == HAS_NULL) goto _exit;
  if (pColData->flag != HAS_VALUE) {
    // bitmap

    int32_t size = BIT2_SIZE(pColData->nVal);
    pColData->pBitMap = p + n;
    n += size;
  }
  if (IS_VAR_DATA_TYPE(pColData->type)) {
    // offset

    int32_t size = sizeof(int32_t) * pColData->nVal;
    pColData->aOffset = (int32_t *)(p + n);
    n += size;
  }
  n += tGetI32v(p + n, &pColData->nData);
  pColData->pData = p + n;
  n += pColData->nData;

_exit:
  return n;
}

static FORCE_INLINE int32_t tColDataCmprFn(const void *p1, const void *p2) {
  SColData *pColData1 = (SColData *)p1;
  SColData *pColData2 = (SColData *)p2;

  if (pColData1->cid < pColData2->cid) {
    return -1;
  } else if (pColData1->cid > pColData2->cid) {
    return 1;
  }

  return 0;
}

// SBlockData ======================================================
int32_t tBlockDataCreate(SBlockData *pBlockData) {
  int32_t code = 0;

  pBlockData->suid = 0;
  pBlockData->uid = 0;
  pBlockData->nRow = 0;
  pBlockData->aUid = NULL;
  pBlockData->aVersion = NULL;
  pBlockData->aTSKEY = NULL;
  pBlockData->aIdx = taosArrayInit(0, sizeof(int32_t));
  if (pBlockData->aIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  pBlockData->aColData = taosArrayInit(0, sizeof(SColData));
  if (pBlockData->aColData == NULL) {
    taosArrayDestroy(pBlockData->aIdx);
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
  taosArrayDestroy(pBlockData->aIdx);
  taosArrayDestroyEx(pBlockData->aColData, deepClear ? tColDataClear : NULL);
  pBlockData->aUid = NULL;
  pBlockData->aVersion = NULL;
  pBlockData->aTSKEY = NULL;
  pBlockData->aIdx = NULL;
  pBlockData->aColData = NULL;
}

int32_t tBlockDataInit(SBlockData *pBlockData, int64_t suid, int64_t uid, STSchema *pTSchema) {
  int32_t code = 0;

  ASSERT(suid || uid);

  pBlockData->suid = suid;
  pBlockData->uid = uid;
  pBlockData->nRow = 0;

  taosArrayClear(pBlockData->aIdx);
  for (int32_t iColumn = 1; iColumn < pTSchema->numOfCols; iColumn++) {
    STColumn *pTColumn = &pTSchema->columns[iColumn];

    SColData *pColData;
    code = tBlockDataAddColData(pBlockData, iColumn - 1, &pColData);
    if (code) goto _exit;

    tColDataInit(pColData, pTColumn->colId, pTColumn->type, (pTColumn->flags & COL_SMA_ON) ? 1 : 0);
  }

_exit:
  return code;
}

int32_t tBlockDataInitEx(SBlockData *pBlockData, SBlockData *pBlockDataFrom) {
  int32_t code = 0;

  ASSERT(0);
  ASSERT(pBlockDataFrom->suid || pBlockDataFrom->uid);

  pBlockData->suid = pBlockDataFrom->suid;
  pBlockData->uid = pBlockDataFrom->uid;
  pBlockData->nRow = 0;

  taosArrayClear(pBlockData->aIdx);
  for (int32_t iColData = 0; iColData < taosArrayGetSize(pBlockDataFrom->aIdx); iColData++) {
    SColData *pColDataFrom = tBlockDataGetColDataByIdx(pBlockDataFrom, iColData);

    SColData *pColData;
    code = tBlockDataAddColData(pBlockData, iColData, &pColData);
    if (code) goto _exit;

    tColDataInit(pColData, pColDataFrom->cid, pColDataFrom->type, pColDataFrom->smaOn);
  }

_exit:
  return code;
}

void tBlockDataReset(SBlockData *pBlockData) {
  pBlockData->suid = 0;
  pBlockData->uid = 0;
  pBlockData->nRow = 0;
  taosArrayClear(pBlockData->aIdx);
}

void tBlockDataClear(SBlockData *pBlockData) {
  ASSERT(pBlockData->suid || pBlockData->uid);

  pBlockData->nRow = 0;
  for (int32_t iColData = 0; iColData < taosArrayGetSize(pBlockData->aIdx); iColData++) {
    SColData *pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);
    tColDataReset(pColData);
  }
}

int32_t tBlockDataAddColData(SBlockData *pBlockData, int32_t iColData, SColData **ppColData) {
  int32_t   code = 0;
  SColData *pColData = NULL;
  int32_t   idx = taosArrayGetSize(pBlockData->aIdx);

  if (idx >= taosArrayGetSize(pBlockData->aColData)) {
    if (taosArrayPush(pBlockData->aColData, &((SColData){0})) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }
  pColData = (SColData *)taosArrayGet(pBlockData->aColData, idx);

  if (taosArrayInsert(pBlockData->aIdx, iColData, &idx) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  *ppColData = pColData;
  return code;

_err:
  *ppColData = NULL;
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

  // OTHER
  SRowIter rIter = {0};
  SColVal *pColVal;

  tRowIterInit(&rIter, pRow, pTSchema);
  pColVal = tRowIterNext(&rIter);
  for (int32_t iColData = 0; iColData < taosArrayGetSize(pBlockData->aIdx); iColData++) {
    SColData *pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);

    while (pColVal && pColVal->cid < pColData->cid) {
      pColVal = tRowIterNext(&rIter);
    }

    if (pColVal == NULL || pColVal->cid > pColData->cid) {
      code = tColDataAppendValue(pColData, &COL_VAL_NONE(pColData->cid, pColData->type));
      if (code) goto _err;
    } else {
      code = tColDataAppendValue(pColData, pColVal);
      if (code) goto _err;
      pColVal = tRowIterNext(&rIter);
    }
  }

_exit:
  pBlockData->nRow++;
  return code;

_err:
  return code;
}

int32_t tBlockDataCorrectSchema(SBlockData *pBlockData, SBlockData *pBlockDataFrom) {
  int32_t code = 0;

  int32_t iColData = 0;
  for (int32_t iColDataFrom = 0; iColDataFrom < taosArrayGetSize(pBlockDataFrom->aIdx); iColDataFrom++) {
    SColData *pColDataFrom = tBlockDataGetColDataByIdx(pBlockDataFrom, iColDataFrom);

    while (true) {
      SColData *pColData;
      if (iColData < taosArrayGetSize(pBlockData->aIdx)) {
        pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);
      } else {
        pColData = NULL;
      }

      if (pColData == NULL || pColData->cid > pColDataFrom->cid) {
        code = tBlockDataAddColData(pBlockData, iColData, &pColData);
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

int32_t tBlockDataCopy(SBlockData *pSrc, SBlockData *pDest) {
  int32_t code = 0;

  tBlockDataClear(pDest);

  ASSERT(pDest->suid == pSrc->suid);
  ASSERT(pDest->uid == pSrc->uid);
  ASSERT(pSrc->nRow == pDest->nRow);
  ASSERT(taosArrayGetSize(pSrc->aIdx) == taosArrayGetSize(pDest->aIdx));

  pDest->nRow = pSrc->nRow;

  if (pSrc->uid == 0) {
    code = tRealloc((uint8_t **)&pDest->aUid, sizeof(int64_t) * pDest->nRow);
    if (code) goto _exit;
    memcpy(pDest->aUid, pSrc->aUid, sizeof(int64_t) * pDest->nRow);
  }

  code = tRealloc((uint8_t **)&pDest->aVersion, sizeof(int64_t) * pDest->nRow);
  if (code) goto _exit;
  memcpy(pDest->aVersion, pSrc->aVersion, sizeof(int64_t) * pDest->nRow);

  code = tRealloc((uint8_t **)&pDest->aTSKEY, sizeof(TSKEY) * pDest->nRow);
  if (code) goto _exit;
  memcpy(pDest->aTSKEY, pSrc->aTSKEY, sizeof(TSKEY) * pDest->nRow);

  for (int32_t iColData = 0; iColData < taosArrayGetSize(pSrc->aIdx); iColData++) {
    SColData *pColSrc = tBlockDataGetColDataByIdx(pSrc, iColData);
    SColData *pColDest = tBlockDataGetColDataByIdx(pDest, iColData);

    ASSERT(pColSrc->cid == pColDest->cid);
    ASSERT(pColSrc->type == pColDest->type);

    code = tColDataCopy(pColSrc, pColDest);
    if (code) goto _exit;
  }

_exit:
  return code;
}

SColData *tBlockDataGetColDataByIdx(SBlockData *pBlockData, int32_t idx) {
  ASSERT(idx >= 0 && idx < taosArrayGetSize(pBlockData->aIdx));
  return (SColData *)taosArrayGet(pBlockData->aColData, *(int32_t *)taosArrayGet(pBlockData->aIdx, idx));
}

void tBlockDataGetColData(SBlockData *pBlockData, int16_t cid, SColData **ppColData) {
  ASSERT(cid != PRIMARYKEY_TIMESTAMP_COL_ID);
  int32_t lidx = 0;
  int32_t ridx = taosArrayGetSize(pBlockData->aIdx) - 1;

  while (lidx <= ridx) {
    int32_t   midx = (lidx + ridx) / 2;
    SColData *pColData = tBlockDataGetColDataByIdx(pBlockData, midx);
    int32_t   c = tColDataCmprFn(pColData, &(SColData){.cid = cid});

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

int32_t tPutBlockData(uint8_t *p, SBlockData *pBlockData) {
  int32_t n = 0;

  n += tPutI32v(p ? p + n : p, pBlockData->nRow);
  if (p) {
    memcpy(p + n, pBlockData->aVersion, sizeof(int64_t) * pBlockData->nRow);
  }
  n = n + sizeof(int64_t) * pBlockData->nRow;
  if (p) {
    memcpy(p + n, pBlockData->aTSKEY, sizeof(TSKEY) * pBlockData->nRow);
  }
  n = n + sizeof(TSKEY) * pBlockData->nRow;

  int32_t nCol = taosArrayGetSize(pBlockData->aIdx);
  n += tPutI32v(p ? p + n : p, nCol);
  for (int32_t iCol = 0; iCol < nCol; iCol++) {
    SColData *pColData = tBlockDataGetColDataByIdx(pBlockData, iCol);
    n += tPutColData(p ? p + n : p, pColData);
  }

  return n;
}

int32_t tGetBlockData(uint8_t *p, SBlockData *pBlockData) {
  int32_t n = 0;

  tBlockDataReset(pBlockData);

  n += tGetI32v(p + n, &pBlockData->nRow);
  pBlockData->aVersion = (int64_t *)(p + n);
  n = n + sizeof(int64_t) * pBlockData->nRow;
  pBlockData->aTSKEY = (TSKEY *)(p + n);
  n = n + sizeof(TSKEY) * pBlockData->nRow;

  int32_t nCol;
  n += tGetI32v(p + n, &nCol);
  for (int32_t iCol = 0; iCol < nCol; iCol++) {
    SColData *pColData;

    if (tBlockDataAddColData(pBlockData, iCol, &pColData)) return -1;
    n += tGetColData(p + n, pColData);
  }

  return n;
}

// SDiskDataHdr ==============================
int32_t tPutDiskDataHdr(uint8_t *p, void *ph) {
  int32_t       n = 0;
  SDiskDataHdr *pHdr = (SDiskDataHdr *)ph;

  n += tPutU32(p ? p + n : p, pHdr->delimiter);
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

void tsdbCalcColDataSMA(SColData *pColData, SColumnDataAgg *pColAgg) {
  SColVal  colVal;
  SColVal *pColVal = &colVal;

  *pColAgg = (SColumnDataAgg){.colId = pColData->cid};
  for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
    tColDataGetValue(pColData, iVal, pColVal);

    if (pColVal->isNone || pColVal->isNull) {
      pColAgg->numOfNull++;
    } else {
      switch (pColData->type) {
        case TSDB_DATA_TYPE_NULL:
          break;
        case TSDB_DATA_TYPE_BOOL:
          break;
        case TSDB_DATA_TYPE_TINYINT: {
          pColAgg->sum += colVal.value.i8;
          if (pColAgg->min > colVal.value.i8) {
            pColAgg->min = colVal.value.i8;
          }
          if (pColAgg->max < colVal.value.i8) {
            pColAgg->max = colVal.value.i8;
          }
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          pColAgg->sum += colVal.value.i16;
          if (pColAgg->min > colVal.value.i16) {
            pColAgg->min = colVal.value.i16;
          }
          if (pColAgg->max < colVal.value.i16) {
            pColAgg->max = colVal.value.i16;
          }
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          pColAgg->sum += colVal.value.i32;
          if (pColAgg->min > colVal.value.i32) {
            pColAgg->min = colVal.value.i32;
          }
          if (pColAgg->max < colVal.value.i32) {
            pColAgg->max = colVal.value.i32;
          }
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          pColAgg->sum += colVal.value.i64;
          if (pColAgg->min > colVal.value.i64) {
            pColAgg->min = colVal.value.i64;
          }
          if (pColAgg->max < colVal.value.i64) {
            pColAgg->max = colVal.value.i64;
          }
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          pColAgg->sum += colVal.value.f;
          if (pColAgg->min > colVal.value.f) {
            pColAgg->min = colVal.value.f;
          }
          if (pColAgg->max < colVal.value.f) {
            pColAgg->max = colVal.value.f;
          }
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          pColAgg->sum += colVal.value.d;
          if (pColAgg->min > colVal.value.d) {
            pColAgg->min = colVal.value.d;
          }
          if (pColAgg->max < colVal.value.d) {
            pColAgg->max = colVal.value.d;
          }
          break;
        }
        case TSDB_DATA_TYPE_VARCHAR:
          break;
        case TSDB_DATA_TYPE_TIMESTAMP: {
          if (pColAgg->min > colVal.value.i64) {
            pColAgg->min = colVal.value.i64;
          }
          if (pColAgg->max < colVal.value.i64) {
            pColAgg->max = colVal.value.i64;
          }
          break;
        }
        case TSDB_DATA_TYPE_NCHAR:
          break;
        case TSDB_DATA_TYPE_UTINYINT: {
          pColAgg->sum += colVal.value.u8;
          if (pColAgg->min > colVal.value.u8) {
            pColAgg->min = colVal.value.u8;
          }
          if (pColAgg->max < colVal.value.u8) {
            pColAgg->max = colVal.value.u8;
          }
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          pColAgg->sum += colVal.value.u16;
          if (pColAgg->min > colVal.value.u16) {
            pColAgg->min = colVal.value.u16;
          }
          if (pColAgg->max < colVal.value.u16) {
            pColAgg->max = colVal.value.u16;
          }
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          pColAgg->sum += colVal.value.u32;
          if (pColAgg->min > colVal.value.u32) {
            pColAgg->min = colVal.value.u32;
          }
          if (pColAgg->max < colVal.value.u32) {
            pColAgg->max = colVal.value.u32;
          }
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          pColAgg->sum += colVal.value.u64;
          if (pColAgg->min > colVal.value.u64) {
            pColAgg->min = colVal.value.u64;
          }
          if (pColAgg->max < colVal.value.u64) {
            pColAgg->max = colVal.value.u64;
          }
          break;
        }
        case TSDB_DATA_TYPE_JSON:
          break;
        case TSDB_DATA_TYPE_VARBINARY:
          break;
        case TSDB_DATA_TYPE_DECIMAL:
          break;
        case TSDB_DATA_TYPE_BLOB:
          break;
        case TSDB_DATA_TYPE_MEDIUMBLOB:
          break;
        default:
          ASSERT(0);
      }
    }
  }
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

  pBlockCol->szBitmap = 0;
  pBlockCol->szOffset = 0;
  pBlockCol->szValue = 0;

  int32_t size = 0;
  // bitmap
  if (pColData->flag != HAS_VALUE) {
    code = tsdbCmprData(pColData->pBitMap, BIT2_SIZE(pColData->nVal), TSDB_DATA_TYPE_TINYINT, cmprAlg, ppOut,
                        nOut + size, &pBlockCol->szBitmap, ppBuf);
    if (code) goto _exit;
  }
  size += pBlockCol->szBitmap;

  // offset
  if (IS_VAR_DATA_TYPE(pColData->type)) {
    code = tsdbCmprData((uint8_t *)pColData->aOffset, sizeof(int32_t) * pColData->nVal, TSDB_DATA_TYPE_INT, cmprAlg,
                        ppOut, nOut + size, &pBlockCol->szOffset, ppBuf);
    if (code) goto _exit;
  }
  size += pBlockCol->szOffset;

  // value
  if (pColData->flag != (HAS_NULL | HAS_NONE)) {
    code = tsdbCmprData((uint8_t *)pColData->pData, pColData->nData, pColData->type, cmprAlg, ppOut, nOut + size,
                        &pBlockCol->szValue, ppBuf);
    if (code) goto _exit;
  }
  size += pBlockCol->szValue;

  // checksum
  size += sizeof(TSCKSUM);
  code = tRealloc(ppOut, nOut + size);
  if (code) goto _exit;
  taosCalcChecksumAppend(0, *ppOut + nOut, size);

_exit:
  return code;
}

int32_t tsdbDecmprColData(uint8_t *pIn, SBlockCol *pBlockCol, int8_t cmprAlg, int32_t nVal, SColData *pColData,
                          uint8_t **ppBuf) {
  int32_t code = 0;

  int32_t size = pBlockCol->szBitmap + pBlockCol->szOffset + pBlockCol->szValue + sizeof(TSCKSUM);
  if (!taosCheckChecksumWhole(pIn, size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

  ASSERT(pColData->cid == pBlockCol->cid);
  ASSERT(pColData->type == pBlockCol->type);
  pColData->smaOn = pBlockCol->smaOn;
  pColData->flag = pBlockCol->flag;
  pColData->nVal = nVal;
  pColData->nData = pBlockCol->szOrigin;

  uint8_t *p = pIn;
  // bitmap
  if (pBlockCol->szBitmap) {
    code = tsdbDecmprData(p, pBlockCol->szBitmap, TSDB_DATA_TYPE_TINYINT, cmprAlg, &pColData->pBitMap,
                          BIT2_SIZE(pColData->nVal), ppBuf);
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

int32_t tsdbReadAndCheck(TdFilePtr pFD, int64_t offset, uint8_t **ppOut, int32_t size, int8_t toCheck) {
  int32_t code = 0;

  // alloc
  code = tRealloc(ppOut, size);
  if (code) goto _exit;

  // seek
  int64_t n = taosLSeekFile(pFD, offset, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }

  // read
  n = taosReadFile(pFD, *ppOut, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  } else if (n < size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

  // check
  if (toCheck && !taosCheckChecksumWhole(*ppOut, size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

_exit:
  return code;
}
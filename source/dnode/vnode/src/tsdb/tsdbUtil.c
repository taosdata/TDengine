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

#include "tsdb.h"

// SMapData =======================================================================
void tMapDataReset(SMapData *pMapData) {
  pMapData->nItem = 0;
  pMapData->nData = 0;
}

void tMapDataClear(SMapData *pMapData) {
  tFree((uint8_t *)pMapData->aOffset);
  tFree(pMapData->pData);
}

int32_t tMapDataPutItem(SMapData *pMapData, void *pItem, int32_t (*tPutItemFn)(uint8_t *, void *)) {
  int32_t code = 0;
  int32_t offset = pMapData->nData;
  int32_t nItem = pMapData->nItem;

  pMapData->nItem++;
  pMapData->nData += tPutItemFn(NULL, pItem);

  // alloc
  code = tRealloc((uint8_t **)&pMapData->aOffset, sizeof(int32_t) * pMapData->nItem);
  if (code) goto _err;
  code = tRealloc(&pMapData->pData, pMapData->nData);
  if (code) goto _err;

  // put
  pMapData->aOffset[nItem] = offset;
  tPutItemFn(pMapData->pData + offset, pItem);

_err:
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
    for (int32_t iItem = 0; iItem < pMapData->nItem; iItem++) {
      n += tPutI32v(p ? p + n : p, pMapData->aOffset[iItem]);
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

    for (int32_t iItem = 0; iItem < pMapData->nItem; iItem++) {
      n += tGetI32v(p + n, &pMapData->aOffset[iItem]);
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

// TSDBKEY =======================================================================
int32_t tsdbKeyCmprFn(const void *p1, const void *p2) {
  TSDBKEY *pKey1 = (TSDBKEY *)p1;
  TSDBKEY *pKey2 = (TSDBKEY *)p2;

  if (pKey1->ts < pKey2->ts) {
    return -1;
  } else if (pKey1->ts > pKey2->ts) {
    return 1;
  }

  if (pKey1->version < pKey2->version) {
    return -1;
  } else if (pKey1->version > pKey2->version) {
    return 1;
  }

  return 0;
}

// TSDBKEY ======================================================
static FORCE_INLINE int32_t tPutTSDBKEY(uint8_t *p, TSDBKEY *pKey) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pKey->version);
  n += tPutI64(p ? p + n : p, pKey->ts);

  return n;
}

static FORCE_INLINE int32_t tGetTSDBKEY(uint8_t *p, TSDBKEY *pKey) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pKey->version);
  n += tGetI64(p + n, &pKey->ts);

  return n;
}

// SBlockIdx ======================================================
void tBlockIdxReset(SBlockIdx *pBlockIdx) {
  pBlockIdx->minKey = TSKEY_MAX;
  pBlockIdx->maxKey = TSKEY_MIN;
  pBlockIdx->minVersion = VERSION_MAX;
  pBlockIdx->maxVersion = VERSION_MIN;
  pBlockIdx->offset = -1;
  pBlockIdx->size = -1;
}

int32_t tPutBlockIdx(uint8_t *p, void *ph) {
  int32_t    n = 0;
  SBlockIdx *pBlockIdx = (SBlockIdx *)ph;

  n += tPutI64(p ? p + n : p, pBlockIdx->suid);
  n += tPutI64(p ? p + n : p, pBlockIdx->uid);
  n += tPutI64(p ? p + n : p, pBlockIdx->minKey);
  n += tPutI64(p ? p + n : p, pBlockIdx->maxKey);
  n += tPutI64v(p ? p + n : p, pBlockIdx->minVersion);
  n += tPutI64v(p ? p + n : p, pBlockIdx->maxVersion);
  n += tPutI64v(p ? p + n : p, pBlockIdx->offset);
  n += tPutI64v(p ? p + n : p, pBlockIdx->size);

  return n;
}

int32_t tGetBlockIdx(uint8_t *p, void *ph) {
  int32_t    n = 0;
  SBlockIdx *pBlockIdx = (SBlockIdx *)ph;

  n += tGetI64(p + n, &pBlockIdx->suid);
  n += tGetI64(p + n, &pBlockIdx->uid);
  n += tGetI64(p + n, &pBlockIdx->minKey);
  n += tGetI64(p + n, &pBlockIdx->maxKey);
  n += tGetI64v(p + n, &pBlockIdx->minVersion);
  n += tGetI64v(p + n, &pBlockIdx->maxVersion);
  n += tGetI64v(p + n, &pBlockIdx->offset);
  n += tGetI64v(p + n, &pBlockIdx->size);

  return n;
}

int32_t tCmprBlockIdx(void const *lhs, void const *rhs) {
  SBlockIdx *lBlockIdx = *(SBlockIdx **)lhs;
  SBlockIdx *rBlockIdx = *(SBlockIdx **)rhs;

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
  *pBlock =
      (SBlock){.minKey = TSDBKEY_MAX, .maxKey = TSDBKEY_MIN, .minVersion = VERSION_MAX, .maxVersion = VERSION_MIN};
}

int32_t tPutBlock(uint8_t *p, void *ph) {
  int32_t n = 0;
  SBlock *pBlock = (SBlock *)ph;

  n += tPutTSDBKEY(p ? p + n : p, &pBlock->minKey);
  n += tPutTSDBKEY(p ? p + n : p, &pBlock->maxKey);
  n += tPutI64v(p ? p + n : p, pBlock->minVersion);
  n += tPutI64v(p ? p + n : p, pBlock->maxVersion);
  n += tPutI32v(p ? p + n : p, pBlock->nRow);
  n += tPutI8(p ? p + n : p, pBlock->last);
  n += tPutI8(p ? p + n : p, pBlock->hasDup);
  n += tPutI8(p ? p + n : p, pBlock->nSubBlock);
  for (int8_t iSubBlock = 0; iSubBlock < pBlock->nSubBlock; iSubBlock++) {
    n += tPutI32v(p ? p + n : p, pBlock->aSubBlock[iSubBlock].nRow);
    n += tPutI8(p ? p + n : p, pBlock->aSubBlock[iSubBlock].cmprAlg);
    n += tPutI64v(p ? p + n : p, pBlock->aSubBlock[iSubBlock].offset);
    n += tPutI32v(p ? p + n : p, pBlock->aSubBlock[iSubBlock].szBlockCol);
    n += tPutI32v(p ? p + n : p, pBlock->aSubBlock[iSubBlock].szVersion);
    n += tPutI32v(p ? p + n : p, pBlock->aSubBlock[iSubBlock].szTSKEY);
    n += tPutI32v(p ? p + n : p, pBlock->aSubBlock[iSubBlock].szBlock);
    n += tPutI64v(p ? p + n : p, pBlock->aSubBlock[iSubBlock].sOffset);
    n += tPutI32v(p ? p + n : p, pBlock->aSubBlock[iSubBlock].nSma);
  }

  return n;
}

int32_t tGetBlock(uint8_t *p, void *ph) {
  int32_t n = 0;
  SBlock *pBlock = (SBlock *)ph;

  n += tGetTSDBKEY(p + n, &pBlock->minKey);
  n += tGetTSDBKEY(p + n, &pBlock->maxKey);
  n += tGetI64v(p + n, &pBlock->minVersion);
  n += tGetI64v(p + n, &pBlock->maxVersion);
  n += tGetI32v(p + n, &pBlock->nRow);
  n += tGetI8(p + n, &pBlock->last);
  n += tGetI8(p + n, &pBlock->hasDup);
  n += tGetI8(p + n, &pBlock->nSubBlock);
  for (int8_t iSubBlock = 0; iSubBlock < pBlock->nSubBlock; iSubBlock++) {
    n += tGetI32v(p + n, &pBlock->aSubBlock[iSubBlock].nRow);
    n += tGetI8(p + n, &pBlock->aSubBlock[iSubBlock].cmprAlg);
    n += tGetI64v(p + n, &pBlock->aSubBlock[iSubBlock].offset);
    n += tGetI32v(p + n, &pBlock->aSubBlock[iSubBlock].szBlockCol);
    n += tGetI32v(p + n, &pBlock->aSubBlock[iSubBlock].szVersion);
    n += tGetI32v(p + n, &pBlock->aSubBlock[iSubBlock].szTSKEY);
    n += tGetI32v(p + n, &pBlock->aSubBlock[iSubBlock].szBlock);
    n += tGetI64v(p + n, &pBlock->aSubBlock[iSubBlock].sOffset);
    n += tGetI32v(p + n, &pBlock->aSubBlock[iSubBlock].nSma);
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
  if (pBlock->last) return false;
  if (pBlock->hasDup) return false;

  return pBlock->aSubBlock[0].nSma > 0;
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

  if (pBlockCol->flag != HAS_NULL) {
    n += tPutI32v(p ? p + n : p, pBlockCol->offset);
    n += tPutI32v(p ? p + n : p, pBlockCol->szBitmap);
    n += tPutI32v(p ? p + n : p, pBlockCol->szOffset);
    n += tPutI32v(p ? p + n : p, pBlockCol->szValue);
    n += tPutI32v(p ? p + n : p, pBlockCol->szOrigin);
  }

  return n;
}

int32_t tGetBlockCol(uint8_t *p, void *ph) {
  int32_t    n = 0;
  SBlockCol *pBlockCol = (SBlockCol *)ph;

  n += tGetI16v(p + n, &pBlockCol->cid);
  n += tGetI8(p + n, &pBlockCol->type);
  n += tGetI8(p + n, &pBlockCol->smaOn);
  n += tGetI8(p + n, &pBlockCol->flag);

  ASSERT(pBlockCol->flag && (pBlockCol->flag != HAS_NONE));

  if (pBlockCol->flag != HAS_NULL) {
    n += tGetI32v(p + n, &pBlockCol->offset);
    n += tGetI32v(p + n, &pBlockCol->szBitmap);
    n += tGetI32v(p + n, &pBlockCol->szOffset);
    n += tGetI32v(p + n, &pBlockCol->szValue);
    n += tGetI32v(p + n, &pBlockCol->szOrigin);
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
  SDelIdx *lDelIdx = *(SDelIdx **)lhs;
  SDelIdx *rDelIdx = *(SDelIdx **)rhs;

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

// int tsdFidLevel(int fid, TSKEY now, minute) {
//   if (fid >= pRtn->maxFid) {
//     return 0;
//   } else if (fid >= pRtn->midFid) {
//     return 1;
//   } else if (fid >= pRtn->minFid) {
//     return 2;
//   } else {
//     return -1;
//   }
// }

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

  pColDataDest->cid = pColDataSrc->cid;
  pColDataDest->type = pColDataSrc->type;
  pColDataDest->smaOn = pColDataSrc->smaOn;
  pColDataDest->nVal = pColDataSrc->nVal;
  pColDataDest->flag = pColDataSrc->flag;

  size = BIT2_SIZE(pColDataSrc->nVal);
  code = tRealloc(&pColDataDest->pBitMap, size);
  if (code) goto _exit;
  memcpy(pColDataDest->pBitMap, pColDataSrc->pBitMap, size);

  if (IS_VAR_DATA_TYPE(pColDataDest->type)) {
    size = sizeof(int32_t) * pColDataSrc->nVal;

    code = tRealloc((uint8_t **)&pColDataDest->aOffset, size);
    if (code) goto _exit;

    memcpy(pColDataDest->aOffset, pColDataSrc->aOffset, size);
  }

  code = tRealloc(&pColDataDest->pData, pColDataSrc->nData);
  if (code) goto _exit;
  pColDataDest->nData = pColDataSrc->nData;
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
int32_t tBlockDataInit(SBlockData *pBlockData) {
  int32_t code = 0;

  pBlockData->nRow = 0;
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

void tBlockDataReset(SBlockData *pBlockData) {
  pBlockData->nRow = 0;
  taosArrayClear(pBlockData->aIdx);
}

void tBlockDataClear(SBlockData *pBlockData) {
  tFree((uint8_t *)pBlockData->aVersion);
  tFree((uint8_t *)pBlockData->aTSKEY);
  taosArrayDestroy(pBlockData->aIdx);
  taosArrayDestroyEx(pBlockData->aColData, tColDataClear);
}

int32_t tBlockDataSetSchema(SBlockData *pBlockData, STSchema *pTSchema) {
  int32_t   code = 0;
  SColData *pColData;
  STColumn *pTColumn;

  tBlockDataReset(pBlockData);
  for (int32_t iColumn = 1; iColumn < pTSchema->numOfCols; iColumn++) {
    pTColumn = &pTSchema->columns[iColumn];

    code = tBlockDataAddColData(pBlockData, iColumn - 1, &pColData);
    if (code) goto _exit;

    tColDataInit(pColData, pTColumn->colId, pTColumn->type, (pTColumn->flags & COL_SMA_ON) != 0);
  }

_exit:
  return code;
}

void tBlockDataClearData(SBlockData *pBlockData) {
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

int32_t tBlockDataAppendRow(SBlockData *pBlockData, TSDBROW *pRow, STSchema *pTSchema) {
  int32_t code = 0;

  // TSDBKEY
  code = tRealloc((uint8_t **)&pBlockData->aVersion, sizeof(int64_t) * (pBlockData->nRow + 1));
  if (code) goto _err;
  code = tRealloc((uint8_t **)&pBlockData->aTSKEY, sizeof(TSKEY) * (pBlockData->nRow + 1));
  if (code) goto _err;
  pBlockData->aVersion[pBlockData->nRow] = TSDBROW_VERSION(pRow);
  pBlockData->aTSKEY[pBlockData->nRow] = TSDBROW_TS(pRow);

  // OTHER
  int32_t   iColData = 0;
  int32_t   nColData = taosArrayGetSize(pBlockData->aIdx);
  SRowIter  iter = {0};
  SRowIter *pIter = &iter;
  SColData *pColData;
  SColVal  *pColVal;

  ASSERT(nColData > 0);

  tRowIterInit(pIter, pRow, pTSchema);
  pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);
  pColVal = tRowIterNext(pIter);

  while (pColData) {
    if (pColVal) {
      if (pColData->cid == pColVal->cid) {
        code = tColDataAppendValue(pColData, pColVal);
        if (code) goto _err;

        pColVal = tRowIterNext(pIter);
        pColData = ((++iColData) < nColData) ? tBlockDataGetColDataByIdx(pBlockData, iColData) : NULL;
      } else if (pColData->cid < pColVal->cid) {
        code = tColDataAppendValue(pColData, &COL_VAL_NONE(pColData->cid, pColData->type));
        if (code) goto _err;

        pColData = ((++iColData) < nColData) ? tBlockDataGetColDataByIdx(pBlockData, iColData) : NULL;
      } else {
        pColVal = tRowIterNext(pIter);
      }
    } else {
      code = tColDataAppendValue(pColData, &COL_VAL_NONE(pColData->cid, pColData->type));
      if (code) goto _err;

      pColData = ((++iColData) < nColData) ? tBlockDataGetColDataByIdx(pBlockData, iColData) : NULL;
    }
  }

  pBlockData->nRow++;
  return code;

_err:
  return code;
}

int32_t tBlockDataMerge(SBlockData *pBlockData1, SBlockData *pBlockData2, SBlockData *pBlockData) {
  int32_t code = 0;

  // set target
  int32_t   iColData1 = 0;
  int32_t   nColData1 = taosArrayGetSize(pBlockData1->aIdx);
  int32_t   iColData2 = 0;
  int32_t   nColData2 = taosArrayGetSize(pBlockData2->aIdx);
  SColData *pColData1;
  SColData *pColData2;
  SColData *pColData;

  tBlockDataReset(pBlockData);
  while (iColData1 < nColData1 && iColData2 < nColData2) {
    pColData1 = tBlockDataGetColDataByIdx(pBlockData1, iColData1);
    pColData2 = tBlockDataGetColDataByIdx(pBlockData2, iColData2);

    if (pColData1->cid == pColData2->cid) {
      code = tBlockDataAddColData(pBlockData, taosArrayGetSize(pBlockData->aIdx), &pColData);
      if (code) goto _exit;
      tColDataInit(pColData, pColData2->cid, pColData2->type, pColData2->smaOn);

      iColData1++;
      iColData2++;
    } else if (pColData1->cid < pColData2->cid) {
      code = tBlockDataAddColData(pBlockData, taosArrayGetSize(pBlockData->aIdx), &pColData);
      if (code) goto _exit;
      tColDataInit(pColData, pColData1->cid, pColData1->type, pColData1->smaOn);

      iColData1++;
    } else {
      code = tBlockDataAddColData(pBlockData, taosArrayGetSize(pBlockData->aIdx), &pColData);
      if (code) goto _exit;
      tColDataInit(pColData, pColData2->cid, pColData2->type, pColData2->smaOn);

      iColData2++;
    }
  }

  while (iColData1 < nColData1) {
    code = tBlockDataAddColData(pBlockData, taosArrayGetSize(pBlockData->aIdx), &pColData);
    if (code) goto _exit;
    tColDataInit(pColData, pColData1->cid, pColData1->type, pColData1->smaOn);

    iColData1++;
  }

  while (iColData2 < nColData2) {
    code = tBlockDataAddColData(pBlockData, taosArrayGetSize(pBlockData->aIdx), &pColData);
    if (code) goto _exit;
    tColDataInit(pColData, pColData2->cid, pColData2->type, pColData2->smaOn);

    iColData2++;
  }

  // loop to merge
  int32_t iRow1 = 0;
  int32_t nRow1 = pBlockData1->nRow;
  int32_t iRow2 = 0;
  int32_t nRow2 = pBlockData2->nRow;
  TSDBROW row1;
  TSDBROW row2;
  int32_t c;

  while (iRow1 < nRow1 && iRow2 < nRow2) {
    row1 = tsdbRowFromBlockData(pBlockData1, iRow1);
    row2 = tsdbRowFromBlockData(pBlockData2, iRow2);

    c = tsdbKeyCmprFn(&TSDBROW_KEY(&row1), &TSDBROW_KEY(&row2));
    if (c < 0) {
      code = tBlockDataAppendRow(pBlockData, &row1, NULL);
      if (code) goto _exit;
      iRow1++;
    } else if (c > 0) {
      code = tBlockDataAppendRow(pBlockData, &row2, NULL);
      if (code) goto _exit;
      iRow2++;
    } else {
      ASSERT(0);
    }
  }

  while (iRow1 < nRow1) {
    row1 = tsdbRowFromBlockData(pBlockData1, iRow1);
    code = tBlockDataAppendRow(pBlockData, &row1, NULL);
    if (code) goto _exit;
    iRow1++;
  }

  while (iRow2 < nRow2) {
    row2 = tsdbRowFromBlockData(pBlockData2, iRow2);
    code = tBlockDataAppendRow(pBlockData, &row2, NULL);
    if (code) goto _exit;
    iRow2++;
  }

_exit:
  return code;
}

int32_t tBlockDataCopy(SBlockData *pBlockDataSrc, SBlockData *pBlockDataDest) {
  int32_t   code = 0;
  SColData *pColDataSrc;
  SColData *pColDataDest;

  ASSERT(pBlockDataSrc->nRow > 0);

  tBlockDataReset(pBlockDataDest);

  pBlockDataDest->nRow = pBlockDataSrc->nRow;
  // TSDBKEY
  code = tRealloc((uint8_t **)&pBlockDataDest->aVersion, sizeof(int64_t) * pBlockDataSrc->nRow);
  if (code) goto _exit;
  code = tRealloc((uint8_t **)&pBlockDataDest->aTSKEY, sizeof(TSKEY) * pBlockDataSrc->nRow);
  if (code) goto _exit;
  memcpy(pBlockDataDest->aVersion, pBlockDataSrc->aVersion, sizeof(int64_t) * pBlockDataSrc->nRow);
  memcpy(pBlockDataDest->aTSKEY, pBlockDataSrc->aTSKEY, sizeof(TSKEY) * pBlockDataSrc->nRow);

  // other
  for (size_t iColData = 0; iColData < taosArrayGetSize(pBlockDataSrc->aIdx); iColData++) {
    pColDataSrc = tBlockDataGetColDataByIdx(pBlockDataSrc, iColData);
    code = tBlockDataAddColData(pBlockDataDest, iColData, &pColDataDest);
    if (code) goto _exit;

    code = tColDataCopy(pColDataSrc, pColDataDest);
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
  int32_t midx;

  while (lidx <= ridx) {
    SColData *pColData;
    int32_t   c;

    midx = (lidx + midx) / 2;

    pColData = tBlockDataGetColDataByIdx(pBlockData, midx);
    c = tColDataCmprFn(pColData, &(SColData){.cid = cid});
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

// ALGORITHM ==============================
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
        case TSDB_DATA_TYPE_TINYINT:
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          break;
        case TSDB_DATA_TYPE_INT:
          break;
        case TSDB_DATA_TYPE_BIGINT:
          break;
        case TSDB_DATA_TYPE_FLOAT:
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          break;
        case TSDB_DATA_TYPE_VARCHAR:
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          break;
        case TSDB_DATA_TYPE_NCHAR:
          break;
        case TSDB_DATA_TYPE_UTINYINT:
          break;
        case TSDB_DATA_TYPE_USMALLINT:
          break;
        case TSDB_DATA_TYPE_UINT:
          break;
        case TSDB_DATA_TYPE_UBIGINT:
          break;
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

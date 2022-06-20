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

#define TSDB_OFFSET_I32 ((uint8_t)0)
#define TSDB_OFFSET_I16 ((uint8_t)1)
#define TSDB_OFFSET_I8  ((uint8_t)2)

// SMapData =======================================================================
void tMapDataReset(SMapData *pMapData) {
  pMapData->flag = TSDB_OFFSET_I32;
  pMapData->nItem = 0;
  pMapData->nData = 0;
}

void tMapDataClear(SMapData *pMapData) {
  tsdbFree(pMapData->pOfst);
  tsdbFree(pMapData->pData);
  tsdbFree(pMapData->pBuf);
}

int32_t tMapDataPutItem(SMapData *pMapData, void *pItem, int32_t (*tPutItemFn)(uint8_t *, void *)) {
  int32_t code = 0;
  int32_t offset = pMapData->nData;
  int32_t nItem = pMapData->nItem;

  pMapData->nItem++;
  pMapData->nData += tPutItemFn(NULL, pItem);

  // alloc
  code = tsdbRealloc(&pMapData->pOfst, sizeof(int32_t) * pMapData->nItem);
  if (code) goto _err;
  code = tsdbRealloc(&pMapData->pData, pMapData->nData);
  if (code) goto _err;

  // put
  ((int32_t *)pMapData->pOfst)[nItem] = offset;
  tPutItemFn(pMapData->pData + offset, pItem);

_err:
  return code;
}

int32_t tMapDataGetItemByIdx(SMapData *pMapData, int32_t idx, void *pItem, int32_t (*tGetItemFn)(uint8_t *, void *)) {
  int32_t code = 0;
  int32_t offset;

  if (idx < 0 || idx >= pMapData->nItem) {
    code = TSDB_CODE_NOT_FOUND;
    goto _exit;
  }

  switch (pMapData->flag) {
    case TSDB_OFFSET_I8:
      offset = ((int8_t *)pMapData->pOfst)[idx];
      break;
    case TSDB_OFFSET_I16:
      offset = ((int16_t *)pMapData->pOfst)[idx];
      break;
    case TSDB_OFFSET_I32:
      offset = ((int32_t *)pMapData->pOfst)[idx];
      break;

    default:
      ASSERT(0);
  }

  tGetItemFn(pMapData->pData + offset, pItem);

_exit:
  return code;
}

int32_t tPutMapData(uint8_t *p, SMapData *pMapData) {
  int32_t n = 0;
  int32_t maxOffset;

  ASSERT(pMapData->flag == TSDB_OFFSET_I32);
  ASSERT(pMapData->nItem > 0);

  maxOffset = ((int32_t *)pMapData->pOfst)[pMapData->nItem - 1];

  n += tPutI32v(p ? p + n : p, pMapData->nItem);
  if (maxOffset <= INT8_MAX) {
    n += tPutU8(p ? p + n : p, TSDB_OFFSET_I8);
    if (p) {
      for (int32_t iItem = 0; iItem < pMapData->nItem; iItem++) {
        n += tPutI8(p + n, (int8_t)(((int32_t *)pMapData->pData)[iItem]));
      }
    } else {
      n = n + sizeof(int8_t) * pMapData->nItem;
    }
  } else if (maxOffset <= INT16_MAX) {
    n += tPutU8(p ? p + n : p, TSDB_OFFSET_I16);
    if (p) {
      for (int32_t iItem = 0; iItem < pMapData->nItem; iItem++) {
        n += tPutI16(p + n, (int16_t)(((int32_t *)pMapData->pData)[iItem]));
      }
    } else {
      n = n + sizeof(int16_t) * pMapData->nItem;
    }
  } else {
    n += tPutU8(p ? p + n : p, TSDB_OFFSET_I32);
    if (p) {
      for (int32_t iItem = 0; iItem < pMapData->nItem; iItem++) {
        n += tPutI32(p + n, (int32_t)(((int32_t *)pMapData->pData)[iItem]));
      }
    } else {
      n = n + sizeof(int32_t) * pMapData->nItem;
    }
  }
  n += tPutBinary(p ? p + n : p, pMapData->pData, pMapData->nData);

  return n;
}

int32_t tGetMapData(uint8_t *p, SMapData *pMapData) {
  int32_t n = 0;

  n += tGetI32v(p + n, &pMapData->nItem);
  n += tGetU8(p + n, &pMapData->flag);
  pMapData->pOfst = p + n;
  switch (pMapData->flag) {
    case TSDB_OFFSET_I8:
      n = n + sizeof(int8_t) * pMapData->nItem;
      break;
    case TSDB_OFFSET_I16:
      n = n + sizeof(int16_t) * pMapData->nItem;
      break;
    case TSDB_OFFSET_I32:
      n = n + sizeof(int32_t) * pMapData->nItem;
      break;

    default:
      ASSERT(0);
  }
  n += tGetBinary(p ? p + n : p, &pMapData->pData, &pMapData->nData);

  return n;
}

// Memory =======================================================================
int32_t tsdbRealloc(uint8_t **ppBuf, int64_t size) {
  int32_t  code = 0;
  int64_t  bsize = 0;
  uint8_t *pBuf;

  if (*ppBuf) {
    bsize = *(int64_t *)((*ppBuf) - sizeof(int64_t));
  }

  if (bsize >= size) goto _exit;

  if (bsize == 0) bsize = 16;
  while (bsize < size) {
    bsize *= 2;
  }

  pBuf = taosMemoryRealloc(*ppBuf ? (*ppBuf) - sizeof(int64_t) : *ppBuf, bsize + sizeof(int64_t));
  if (pBuf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  *(int64_t *)pBuf = bsize;
  *ppBuf = pBuf + sizeof(int64_t);

_exit:
  return code;
}

void tsdbFree(uint8_t *pBuf) {
  if (pBuf) {
    taosMemoryFree(pBuf - sizeof(int64_t));
  }
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
int32_t tPutBlockIdx(uint8_t *p, void *ph) {
  int32_t    n = 0;
  SBlockIdx *pBlockIdx = (SBlockIdx *)ph;

  n += tPutI64(p ? p + n : p, pBlockIdx->suid);
  n += tPutI64(p ? p + n : p, pBlockIdx->uid);
  n += tPutKEYINFO(p ? p + n : p, &pBlockIdx->info);
  n += tPutI64v(p ? p + n : p, pBlockIdx->offset);
  n += tPutI64v(p ? p + n : p, pBlockIdx->size);

  return n;
}

int32_t tGetBlockIdx(uint8_t *p, void *ph) {
  int32_t    n = 0;
  SBlockIdx *pBlockIdx = (SBlockIdx *)ph;

  n += tGetI64(p + n, &pBlockIdx->suid);
  n += tGetI64(p + n, &pBlockIdx->uid);
  n += tGetKEYINFO(p + n, &pBlockIdx->info);
  n += tGetI64v(p + n, &pBlockIdx->offset);
  n += tGetI64v(p + n, &pBlockIdx->size);

  return n;
}

// SBlock ======================================================
int32_t tPutBlock(uint8_t *p, void *ph) {
  int32_t n = 0;
  SBlock *pBlock = (SBlock *)ph;
  // TODO
  return n;
}

int32_t tGetBlock(uint8_t *p, void *ph) {
  int32_t n = 0;
  SBlock *pBlock = (SBlock *)ph;
  // TODO
  return n;
}

int32_t tBlockCmprFn(const void *p1, const void *p2) {
  int32_t c;
  SBlock *pBlock1 = (SBlock *)p1;
  SBlock *pBlock2 = (SBlock *)p2;

  if (tsdbKeyCmprFn(&pBlock1->info.maxKey, &pBlock2->info.minKey) < 0) {
    return -1;
  } else if (tsdbKeyCmprFn(&pBlock1->info.minKey, &pBlock2->info.maxKey) > 0) {
    return 1;
  }

  return 0;
}

// SBlockCol ======================================================
int32_t tPutBlockCol(uint8_t *p, void *ph) {
  int32_t    n = 0;
  SBlockCol *pBlockCol = (SBlockCol *)ph;

  ASSERT(pBlockCol->flag && (pBlockCol->flag != HAS_NONE));

  n += tPutI16v(p ? p + n : p, pBlockCol->cid);
  n += tPutI8(p ? p + n : p, pBlockCol->type);
  n += tPutI8(p ? p + n : p, pBlockCol->flag);

  if (pBlockCol->flag != HAS_NULL) {
    n += tPutI64v(p ? p + n : p, pBlockCol->offset);
    n += tPutI64v(p ? p + n : p, pBlockCol->size);
  }

  return n;
}

int32_t tGetBlockCol(uint8_t *p, void *ph) {
  int32_t    n = 0;
  SBlockCol *pBlockCol = (SBlockCol *)ph;

  n += tGetI16v(p + n, &pBlockCol->cid);
  n += tGetI8(p + n, &pBlockCol->type);
  n += tGetI8(p + n, &pBlockCol->flag);

  ASSERT(pBlockCol->flag && (pBlockCol->flag != HAS_NONE));

  if (pBlockCol->flag != HAS_NULL) {
    n += tGetI64v(p + n, &pBlockCol->offset);
    n += tGetI64v(p + n, &pBlockCol->size);
  }

  return n;
}

// SDelIdx ======================================================
int32_t tPutDelIdx(uint8_t *p, void *ph) {
  SDelIdx *pDelIdx = (SDelIdx *)ph;
  int32_t  n = 0;

  n += tPutI64(p ? p + n : p, pDelIdx->suid);
  n += tPutI64(p ? p + n : p, pDelIdx->uid);
  n += tPutI64(p ? p + n : p, pDelIdx->minKey);
  n += tPutI64(p ? p + n : p, pDelIdx->maxKey);
  n += tPutI64v(p ? p + n : p, pDelIdx->minVersion);
  n += tPutI64v(p ? p + n : p, pDelIdx->maxVersion);
  n += tPutI64v(p ? p + n : p, pDelIdx->offset);
  n += tPutI64v(p ? p + n : p, pDelIdx->size);

  return n;
}

int32_t tGetDelIdx(uint8_t *p, void *ph) {
  SDelIdx *pDelIdx = (SDelIdx *)ph;
  int32_t  n = 0;

  n += tGetI64(p + n, &pDelIdx->suid);
  n += tGetI64(p + n, &pDelIdx->uid);
  n += tGetI64(p + n, &pDelIdx->minKey);
  n += tGetI64(p + n, &pDelIdx->maxKey);
  n += tGetI64v(p + n, &pDelIdx->minVersion);
  n += tGetI64v(p + n, &pDelIdx->maxVersion);
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

int32_t tPutDelFileHdr(uint8_t *p, SDelFile *pDelFile) {
  int32_t n = 0;

  n += tPutKEYINFO(p ? p + n : p, &pDelFile->info);
  n += tPutI64v(p ? p + n : p, pDelFile->size);
  n += tPutI64v(p ? p + n : p, pDelFile->offset);

  return n;
}

int32_t tGetDelFileHdr(uint8_t *p, SDelFile *pDelFile) {
  int32_t n = 0;

  n += tGetKEYINFO(p + n, &pDelFile->info);
  n += tGetI64v(p + n, &pDelFile->size);
  n += tGetI64v(p + n, &pDelFile->offset);

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
TSDBKEY tsdbRowKey(TSDBROW *pRow) {
  // if (pRow->type == 0) {
  return (TSDBKEY){.version = pRow->version, .ts = pRow->pTSRow->ts};
  // } else {
  // return (TSDBKEY){.version = pRow->pBlockData->aVersion[pRow->iRow], .ts = pRow->pBlockData->aTSKEY[pRow->iRow]};
  // }
}

void tsdbRowGetColVal(TSDBROW *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal) {
  STColumn *pTColumn = &pTSchema->columns[iCol];
  SValue    value;

  if (pRow->type == 0) {
    // get from row (todo);
  } else if (pRow->type == 1) {
    SColData *pColData;
    void     *p;

    p = taosbsearch(&(SColData){.cid = pTColumn->colId}, pRow->pBlockData->aColData, pRow->pBlockData->nCol,
                    sizeof(SBlockCol), tColDataCmprFn, TD_EQ);
    if (p) {
      pColData = (SColData *)p;
      ASSERT(pColData->flags);

      if (pColData->flags == HAS_NONE) {
        goto _return_none;
      } else if (pColData->flags == HAS_NULL) {
        goto _return_null;
      } else {
        uint8_t v = GET_BIT2(pColData->pBitMap, pRow->iRow);
        if (v == 0) {
          goto _return_none;
        } else if (v == 1) {
          goto _return_null;
        } else {
          int32_t offset;
          if (IS_VAR_DATA_TYPE(pTColumn->type)) {
            // offset = ; (todo)
            ASSERT(0);
          } else {
            offset = tDataTypes[pTColumn->type].bytes * pRow->iRow;
          }
          tGetValue(pColData->pData + offset, &value, pTColumn->type);
        }
      }
    } else {
      goto _return_none;
    }
  } else {
    ASSERT(0);
  }

_return_none:
  *pColVal = COL_VAL_NONE(pTColumn->colId);
  return;

_return_null:
  *pColVal = COL_VAL_NULL(pTColumn->colId);
  return;

_return_value:
  *pColVal = COL_VAL_VALUE(pTColumn->colId, value);
  return;
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

// KEYINFO ======================================================
int32_t tPutKEYINFO(uint8_t *p, KEYINFO *pKeyInfo) {
  int32_t n = 0;

  n += tPutTSDBKEY(p ? p + n : p, &pKeyInfo->minKey);
  n += tPutTSDBKEY(p ? p + n : p, &pKeyInfo->maxKey);
  n += tPutI64v(p ? p + n : p, pKeyInfo->minVerion);
  n += tPutI64v(p ? p + n : p, pKeyInfo->maxVersion);

  return n;
}

int32_t tGetKEYINFO(uint8_t *p, KEYINFO *pKeyInfo) {
  int32_t n = 0;

  n += tGetTSDBKEY(p + n, &pKeyInfo->minKey);
  n += tGetTSDBKEY(p + n, &pKeyInfo->maxKey);
  n += tGetI64v(p + n, &pKeyInfo->minVerion);
  n += tGetI64v(p + n, &pKeyInfo->maxVersion);

  return n;
}

// SBlockData ======================================================
static int32_t tsdbBlockDataAppendRow0(SBlockData *pBlockData, TSDBROW *pRow, STSchema *pTSchema) {
  int32_t code = 0;

  // aKey

  // other cols

  return code;
}

static int32_t tsdbBlockDataAppendRow1(SBlockData *pBlockData, TSDBROW *pRow) {
  int32_t code = 0;

  // aKey

  // other cols

  return code;
}

void tsdbBlockDataClear(SBlockData *pBlockData) {
  pBlockData->nRow = 0;
  for (int32_t iCol = 0; iCol < pBlockData->nCol; iCol++) {
    pBlockData->aColData[iCol] = (SColData){.cid = 0, .type = 0, .bytes = 0, .flags = 0, .nData = 0};
  }
}

int32_t tsdbBlockDataAppendRow(SBlockData *pBlockData, TSDBROW *pRow, STSchema *pTSchema) {
  int32_t code = 0;

  if (pRow->type == 0) {
    code = tsdbBlockDataAppendRow0(pBlockData, pRow, pTSchema);
  } else if (pRow->type == 1) {
    code = tsdbBlockDataAppendRow1(pBlockData, pRow);
  }

  return code;
}

void tsdbBlockDataDestroy(SBlockData *pBlockData) {
  tsdbFree((uint8_t *)pBlockData->aVersion);
  tsdbFree((uint8_t *)pBlockData->aTSKEY);
  for (int32_t iCol = 0; iCol < pBlockData->nCol; iCol++) {
    tsdbFree(pBlockData->aColData[iCol].pBitMap);
    tsdbFree(pBlockData->aColData[iCol].pData);
  }
}

// SColData ========================================
int32_t tColDataCmprFn(const void *p1, const void *p2) {
  if (((SColData *)p1)->cid < ((SColData *)p2)->cid) {
    return -1;
  } else if (((SColData *)p1)->cid > ((SColData *)p2)->cid) {
    return 1;
  }

  return 0;
}

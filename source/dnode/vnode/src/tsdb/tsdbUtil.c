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

  if (bsize == 0) bsize = 128;
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
  n += tPutTSDBKEY(p ? p + n : p, &pBlockIdx->minKey);
  n += tPutTSDBKEY(p ? p + n : p, &pBlockIdx->maxKey);
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
  n += tGetTSDBKEY(p + n, &pBlockIdx->minKey);
  n += tGetTSDBKEY(p + n, &pBlockIdx->maxKey);
  n += tGetI64v(p + n, &pBlockIdx->minVersion);
  n += tGetI64v(p + n, &pBlockIdx->maxVersion);
  n += tGetI64v(p + n, &pBlockIdx->offset);
  n += tGetI64v(p + n, &pBlockIdx->size);

  return n;
}

// SBlock ======================================================
int32_t tPutBlock(uint8_t *p, void *ph) {
  int32_t n = 0;
  ASSERT(0);
  // TODO
  return n;
}

int32_t tGetBlock(uint8_t *p, void *ph) {
  int32_t n = 0;
  ASSERT(0);
  // TODO
  return n;
}

int32_t tBlockCmprFn(const void *p1, const void *p2) {
  int32_t c;
  SBlock *pBlock1 = (SBlock *)p1;
  SBlock *pBlock2 = (SBlock *)p2;

  if (tsdbKeyCmprFn(&pBlock1->maxKey, &pBlock2->minKey) < 0) {
    return -1;
  } else if (tsdbKeyCmprFn(&pBlock1->minKey, &pBlock2->maxKey) > 0) {
    return 1;
  }

  return 0;
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

  n += tPutI64(p ? p + n : p, pDelFile->minKey);
  n += tPutI64(p ? p + n : p, pDelFile->maxKey);
  n += tPutI64v(p ? p + n : p, pDelFile->minVersion);
  n += tPutI64v(p ? p + n : p, pDelFile->maxVersion);
  n += tPutI64v(p ? p + n : p, pDelFile->size);
  n += tPutI64v(p ? p + n : p, pDelFile->offset);

  return n;
}

int32_t tGetDelFileHdr(uint8_t *p, SDelFile *pDelFile) {
  int32_t n = 0;

  n += tGetI64(p + n, &pDelFile->minKey);
  n += tGetI64(p + n, &pDelFile->maxKey);
  n += tGetI64v(p + n, &pDelFile->minVersion);
  n += tGetI64v(p + n, &pDelFile->maxVersion);
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
void tsdbRowGetColVal(TSDBROW *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal) {
  // TODO
}

// SColDataBlock ======================================================
void tsdbColDataBlockReset(SColDataBlock *pColDataBlock) {
  // TODO
}

int32_t tsdbColDataBlockAppend(SColDataBlock *pColDataBlock, TSDBROW *pRow, STSchema *pTSchema) {
  int32_t   code = 0;
  int32_t   nRow = pColDataBlock->nRow;
  STColumn *pTColumn;
  SColData *pColData;
  SColVal   colVal;

  pColDataBlock->nRow++;

  // version
  pColDataBlock->aVersion[nRow] = pRow->version;  // TODO

  // ts
  pColDataBlock->aTSKey[nRow] = pRow->pTSRow->ts;  // TODO

  // other rows
  for (int32_t iCol = 1; iCol < pTSchema->numOfCols; iCol++) {
    pTColumn = &pTSchema->columns[iCol];

    tsdbRowGetColVal(pRow, pTSchema, iCol, &colVal);

    if (colVal.isNone) {
      // TODO
    } else if (colVal.isNull) {
      // TODO
    } else {
      pColData->nData += tPutValue(pColData->pData + pColData->nData, &colVal.value, pTColumn->type);
    }
  }

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

// SBlockData ======================================================
int32_t tsdbBlockDataAppendRow(SBlockData *pBlockData, TSDBROW *pRow, STSchema *pTSchema) {
  int32_t code = 0;
  // TODO
  return code;
}
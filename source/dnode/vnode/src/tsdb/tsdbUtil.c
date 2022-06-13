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

// SOffset =======================================================================
#define TSDB_OFFSET_I32 ((uint8_t)0)
#define TSDB_OFFSET_I16 ((uint8_t)1)
#define TSDB_OFFSET_I8  ((uint8_t)2)

static FORCE_INLINE int32_t tsdbOffsetSize(SOffset *pOfst) {
  switch (pOfst->flag) {
    case TSDB_OFFSET_I32:
      return sizeof(int32_t);
    case TSDB_OFFSET_I16:
      return sizeof(int16_t);
    case TSDB_OFFSET_I8:
      return sizeof(int8_t);
    default:
      ASSERT(0);
  }
}

static FORCE_INLINE int32_t tsdbGetOffset(SOffset *pOfst, int32_t idx) {
  int32_t offset = -1;

  if (idx >= 0 && idx < pOfst->nOffset) {
    switch (pOfst->flag) {
      case TSDB_OFFSET_I32:
        offset = ((int32_t *)pOfst->pOffset)[idx];
        break;
      case TSDB_OFFSET_I16:
        offset = ((int16_t *)pOfst->pOffset)[idx];
        break;
      case TSDB_OFFSET_I8:
        offset = ((int8_t *)pOfst->pOffset)[idx];
        break;
      default:
        ASSERT(0);
    }

    ASSERT(offset >= 0);
  }

  return offset;
}

static FORCE_INLINE int32_t tsdbAddOffset(SOffset *pOfst, int32_t offset) {
  int32_t code = 0;
  int32_t nOffset = pOfst->nOffset;

  ASSERT(pOfst->flag == TSDB_OFFSET_I32);
  ASSERT(offset >= 0);

  pOfst->nOffset++;

  // alloc
  code = tsdbRealloc(&pOfst->pOffset, sizeof(int32_t) * pOfst->nOffset);
  if (code) goto _exit;

  // put
  ((int32_t *)pOfst->pOffset)[nOffset] = offset;

_exit:
  return code;
}

static FORCE_INLINE int32_t tPutOffset(uint8_t *p, SOffset *pOfst) {
  int32_t n = 0;
  int32_t maxOffset;

  ASSERT(pOfst->flag == TSDB_OFFSET_I32);
  ASSERT(pOfst->nOffset > 0);

  maxOffset = tsdbGetOffset(pOfst, pOfst->nOffset - 1);

  n += tPutI32v(p ? p + n : p, pOfst->nOffset);
  if (maxOffset <= INT8_MAX) {
    n += tPutU8(p ? p + n : p, TSDB_OFFSET_I8);
    for (int32_t iOffset = 0; iOffset < pOfst->nOffset; iOffset++) {
      n += tPutI8(p ? p + n : p, (int8_t)tsdbGetOffset(pOfst, iOffset));
    }
  } else if (maxOffset <= INT16_MAX) {
    n += tPutU8(p ? p + n : p, TSDB_OFFSET_I16);
    for (int32_t iOffset = 0; iOffset < pOfst->nOffset; iOffset++) {
      n += tPutI16(p ? p + n : p, (int16_t)tsdbGetOffset(pOfst, iOffset));
    }
  } else {
    n += tPutU8(p ? p + n : p, TSDB_OFFSET_I32);
    for (int32_t iOffset = 0; iOffset < pOfst->nOffset; iOffset++) {
      n += tPutI32(p ? p + n : p, (int32_t)tsdbGetOffset(pOfst, iOffset));
    }
  }

  return n;
}

static FORCE_INLINE int32_t tGetOffset(uint8_t *p, SOffset *pOfst) {
  int32_t n = 0;

  n += tGetI32v(p + n, &pOfst->nOffset);
  n += tGetU8(p + n, &pOfst->flag);
  pOfst->pOffset = p + n;
  switch (pOfst->flag) {
    case TSDB_OFFSET_I32:
      n = n + pOfst->nOffset + sizeof(int32_t);
      break;
    case TSDB_OFFSET_I16:
      n = n + pOfst->nOffset + sizeof(int16_t);
      break;
    case TSDB_OFFSET_I8:
      n = n + pOfst->nOffset + sizeof(int8_t);
      break;
    default:
      ASSERT(0);
  }

  return n;
}

// SMapData =======================================================================
int32_t tMapDataClear(SMapData *pMapData) {
  int32_t code = 0;

  tsdbFree(pMapData->pOfst);
  tsdbFree(pMapData->pData);

  return code;
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
    for (int32_t iItem = 0; iItem < pMapData->nItem; iItem++) {
      n += tPutI8(p ? p + n : p, (int8_t)(((int32_t *)pMapData->pData)[iItem]));
    }
  } else if (maxOffset <= INT16_MAX) {
    n += tPutU8(p ? p + n : p, TSDB_OFFSET_I16);
    for (int32_t iItem = 0; iItem < pMapData->nItem; iItem++) {
      n += tPutI8(p ? p + n : p, (int16_t)(((int32_t *)pMapData->pData)[iItem]));
    }
  } else {
    n += tPutU8(p ? p + n : p, TSDB_OFFSET_I32);
    for (int32_t iItem = 0; iItem < pMapData->nItem; iItem++) {
      n += tPutI8(p ? p + n : p, (int32_t)(((int32_t *)pMapData->pData)[iItem]));
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

// SDelIdxItem ======================================================
static FORCE_INLINE int32_t tPutDelIdxItem(uint8_t *p, SDelIdxItem *pDelIdxItem) {
  int32_t n = 0;

  n += tPutI64(p ? p + n : p, pDelIdxItem->suid);
  n += tPutI64(p ? p + n : p, pDelIdxItem->uid);
  n += tPutI64(p ? p + n : p, pDelIdxItem->minKey);
  n += tPutI64(p ? p + n : p, pDelIdxItem->maxKey);
  n += tPutI64v(p ? p + n : p, pDelIdxItem->minVersion);
  n += tPutI64v(p ? p + n : p, pDelIdxItem->maxVersion);
  n += tPutI64v(p ? p + n : p, pDelIdxItem->offset);
  n += tPutI64v(p ? p + n : p, pDelIdxItem->size);

  return n;
}

static FORCE_INLINE int32_t tGetDelIdxItem(uint8_t *p, SDelIdxItem *pDelIdxItem) {
  int32_t n = 0;

  n += tGetI64(p + n, &pDelIdxItem->suid);
  n += tGetI64(p + n, &pDelIdxItem->uid);
  n += tGetI64(p + n, &pDelIdxItem->minKey);
  n += tGetI64(p + n, &pDelIdxItem->maxKey);
  n += tGetI64v(p + n, &pDelIdxItem->minVersion);
  n += tGetI64v(p + n, &pDelIdxItem->maxVersion);
  n += tGetI64v(p + n, &pDelIdxItem->offset);
  n += tGetI64v(p + n, &pDelIdxItem->size);

  return n;
}

// SBlockIdxItem ======================================================
static FORCE_INLINE int32_t tPutBlockIdxItem(uint8_t *p, SBlockIdxItem *pItem) {
  int32_t n = 0;

  n += tPutI64(p ? p + n : p, pItem->suid);
  n += tPutI64(p ? p + n : p, pItem->uid);
  n += tPutTSDBKEY(p ? p + n : p, &pItem->minKey);
  n += tPutTSDBKEY(p ? p + n : p, &pItem->maxKey);
  n += tPutI64v(p ? p + n : p, pItem->minVersion);
  n += tPutI64v(p ? p + n : p, pItem->maxVersion);
  n += tPutI64v(p ? p + n : p, pItem->offset);
  n += tPutI64v(p ? p + n : p, pItem->size);

  return n;
}

static FORCE_INLINE int32_t tGetBlockIdxItem(uint8_t *p, SBlockIdxItem *pItem) {
  int32_t n = 0;

  n += tGetI64(p + n, &pItem->suid);
  n += tGetI64(p + n, &pItem->uid);
  n += tGetTSDBKEY(p + n, &pItem->minKey);
  n += tGetTSDBKEY(p + n, &pItem->maxKey);
  n += tGetI64v(p + n, &pItem->minVersion);
  n += tGetI64v(p + n, &pItem->maxVersion);
  n += tGetI64v(p + n, &pItem->offset);
  n += tGetI64v(p + n, &pItem->size);

  return n;
}

// SBlockIdx ======================================================
int32_t tBlockIdxClear(SBlockIdx *pBlockIdx) {
  int32_t code = 0;
  tsdbFree(pBlockIdx->offset.pOffset);
  tsdbFree(pBlockIdx->pData);
  return code;
}

int32_t tBlockIdxPutItem(SBlockIdx *pBlockIdx, SBlockIdxItem *pItem) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tBlockIdxGetItemByIdx(SBlockIdx *pBlockIdx, SBlockIdxItem *pItem, int32_t idx) {
  int32_t code = 0;
  int32_t offset;

  offset = tsdbGetOffset(&pBlockIdx->offset, idx);
  if (offset < 0) {
    code = TSDB_CODE_NOT_FOUND;
    goto _exit;
  }

  tGetBlockIdxItem(pBlockIdx->pData + offset, pItem);

_exit:
  return code;
}

int32_t tBlockIdxGetItem(SBlockIdx *pBlockIdx, SBlockIdxItem *pItem, TABLEID id) {
  int32_t code = 0;
  int32_t lidx = 0;
  int32_t ridx = pBlockIdx->offset.nOffset - 1;
  int32_t midx;
  int32_t c;

  while (lidx <= ridx) {
    midx = (lidx + midx) / 2;

    code = tBlockIdxGetItemByIdx(pBlockIdx, pItem, midx);
    if (code) goto _exit;

    c = tTABLEIDCmprFn(&id, pItem);
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

int32_t tPutBlockIdx(uint8_t *p, SBlockIdx *pBlockIdx) {
  int32_t n = 0;

  n += tPutU32(p ? p + n : p, pBlockIdx->delimiter);
  n += tPutOffset(p ? p + n : p, &pBlockIdx->offset);
  n += tPutBinary(p ? p + n : p, pBlockIdx->pData, pBlockIdx->nData);

  return n;
}

int32_t tGetBlockIdx(uint8_t *p, SBlockIdx *pBlockIdx) {
  int32_t n = 0;

  n += tGetU32(p + n, &pBlockIdx->delimiter);
  n += tGetOffset(p + n, &pBlockIdx->offset);
  n += tGetBinary(p + n, &pBlockIdx->pData, &pBlockIdx->nData);

  return n;
}

// SDelIdx ======================================================
int32_t tDelIdxClear(SDelIdx *pDelIdx) {
  int32_t code = 0;
  tdbFree(pDelIdx->offset.pOffset);
  tdbFree(pDelIdx->pData);
  return code;
}

int32_t tDelIdxPutItem(SDelIdx *pDelIdx, SDelIdxItem *pItem) {
  int32_t  code = 0;
  uint32_t offset = pDelIdx->nData;

  // offset
  code = tsdbAddOffset(&pDelIdx->offset, offset);
  if (code) goto _exit;

  // alloc
  pDelIdx->nData += tPutDelIdxItem(NULL, pItem);
  code = tsdbRealloc(&pDelIdx->pData, pDelIdx->nData);
  if (code) goto _exit;

  // put
  tPutDelIdxItem(pDelIdx->pData + offset, pItem);

_exit:
  return code;
}

int32_t tDelIdxGetItemByIdx(SDelIdx *pDelIdx, SDelIdxItem *pItem, int32_t idx) {
  int32_t code = 0;
  int32_t offset;

  offset = tsdbGetOffset(&pDelIdx->offset, idx);
  if (offset < 0) {
    code = TSDB_CODE_NOT_FOUND;
    goto _exit;
  }

  tGetDelIdxItem(pDelIdx->pData + offset, pItem);

_exit:
  return code;
}

int32_t tDelIdxGetItem(SDelIdx *pDelIdx, SDelIdxItem *pItem, TABLEID id) {
  int32_t code = 0;
  int32_t lidx = 0;
  int32_t ridx = pDelIdx->offset.nOffset - 1;
  int32_t midx;
  int32_t c;

  while (lidx <= ridx) {
    midx = (lidx + ridx) / 2;

    code = tDelIdxGetItemByIdx(pDelIdx, pItem, midx);
    if (code) goto _exit;

    c = tTABLEIDCmprFn(&id, pItem);
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

int32_t tPutDelIdx(uint8_t *p, SDelIdx *pDelIdx) {
  int32_t n = 0;

  n += tPutU32(p ? p + n : p, pDelIdx->delimiter);
  n += tPutOffset(p ? p + n : p, &pDelIdx->offset);
  n += tPutBinary(p ? p + n : p, pDelIdx->pData, pDelIdx->nData);

  return n;
}

int32_t tGetDelIdx(uint8_t *p, SDelIdx *pDelIdx) {
  int32_t n = 0;

  n += tGetU32(p + n, &pDelIdx->delimiter);
  n += tGetOffset(p + n, &pDelIdx->offset);
  n += tGetBinary(p + n, &pDelIdx->pData, &pDelIdx->nData);

  return n;
}

// SDelDataItem ======================================================
static FORCE_INLINE int32_t tPutDelDataItem(uint8_t *p, SDelDataItem *pItem) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pItem->version);
  n += tPutI64(p ? p + n : p, pItem->sKey);
  n += tPutI64(p ? p + n : p, pItem->eKey);

  return n;
}

static FORCE_INLINE int32_t tGetDelDataItem(uint8_t *p, SDelDataItem *pItem) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pItem->version);
  n += tGetI64(p + n, &pItem->sKey);
  n += tGetI64(p + n, &pItem->eKey);

  return n;
}

// SDelData ======================================================
int32_t tDelDataClear(SDelData *pDelData) {
  int32_t code = 0;
  tsdbFree(pDelData->offset.pOffset);
  tsdbFree(pDelData->pData);
  return code;
}

int32_t tDelDataPutItem(SDelData *pDelData, SDelDataItem *pItem) {
  int32_t  code = 0;
  uint32_t offset = pDelData->nData;

  // offset
  code = tsdbAddOffset(&pDelData->offset, offset);
  if (code) goto _exit;

  // alloc
  pDelData->nData += tPutDelDataItem(NULL, pItem);
  code = tsdbRealloc(&pDelData->pData, pDelData->nData);
  if (code) goto _exit;

  // put
  tPutDelDataItem(pDelData->pData + offset, pItem);

_exit:
  return code;
}

int32_t tDelDataGetItemByIdx(SDelData *pDelData, SDelDataItem *pItem, int32_t idx) {
  int32_t code = 0;
  int32_t offset;

  offset = tsdbGetOffset(&pDelData->offset, idx);
  if (offset < 0) {
    code = TSDB_CODE_NOT_FOUND;
    goto _exit;
  }
  tGetDelDataItem(pDelData->pData + offset, pItem);

_exit:
  return code;
}

int32_t tDelDataGetItem(SDelData *pDelData, SDelDataItem *pItem, int64_t version) {
  int32_t code = 0;
  int32_t lidx = 0;
  int32_t ridx = pDelData->offset.nOffset - 1;
  int32_t midx;

  while (lidx <= ridx) {
    midx = (lidx + ridx) / 2;

    code = tDelDataGetItemByIdx(pDelData, pItem, midx);
    if (code) goto _exit;

    if (version == pItem->version) {
      goto _exit;
    } else if (version < pItem->version) {
      ridx = midx - 1;
    } else {
      ridx = midx + 1;
    }
  }

  code = TSDB_CODE_NOT_FOUND;

_exit:
  return code;
}

int32_t tPutDelData(uint8_t *p, SDelData *pDelData) {
  int32_t n = 0;

  n += tPutU32(p ? p + n : p, pDelData->delimiter);
  n += tPutI64(p ? p + n : p, pDelData->suid);
  n += tPutI64(p ? p + n : p, pDelData->uid);
  n += tPutOffset(p ? p + n : p, &pDelData->offset);
  n += tPutBinary(p ? p + n : p, pDelData->pData, pDelData->nData);

  return n;
}

int32_t tGetDelData(uint8_t *p, SDelData *pDelData) {
  int32_t n = 0;

  n += tGetU32(p + n, &pDelData->delimiter);
  n += tGetI64(p + n, &pDelData->suid);
  n += tGetI64(p + n, &pDelData->uid);
  n += tGetOffset(p + n, &pDelData->offset);
  n += tGetBinary(p + n, &pDelData->pData, &pDelData->nData);

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

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

static FORCE_INLINE int32_t tsdbOffsetSize(uint8_t flags) {
  if (flags & TSDB_OFFSET_U8) {
    return sizeof(uint8_t);
  } else if (flags & TSDB_OFFSET_U16) {
    return sizeof(uint16_t);
  } else if (flags & TSDB_OFFSET_U32) {
    return sizeof(uint32_t);
  } else {
    ASSERT(0);
  }
}

static FORCE_INLINE uint32_t tsdbGetOffset(uint8_t *pOffset, uint8_t flags, int32_t idx) {
  if (flags & TSDB_OFFSET_U8) {
    return ((uint8_t *)pOffset)[idx];
  } else if (flags & TSDB_OFFSET_U16) {
    return ((uint16_t *)pOffset)[idx];
  } else if (flags & TSDB_OFFSET_U32) {
    return ((uint32_t *)pOffset)[idx];
  } else {
    ASSERT(0);
  }
}

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

  n += tGetI64(p, &pDelIdxItem->suid);
  n += tGetI64(p, &pDelIdxItem->uid);
  n += tGetI64(p, &pDelIdxItem->minKey);
  n += tGetI64(p, &pDelIdxItem->maxKey);
  n += tGetI64v(p, &pDelIdxItem->minVersion);
  n += tGetI64v(p, &pDelIdxItem->maxVersion);
  n += tGetI64v(p, &pDelIdxItem->offset);
  n += tGetI64v(p, &pDelIdxItem->size);

  return n;
}

// SDelIdx ======================================================
int32_t tDelIdxGetItem(SDelIdx *pDelIdx, SDelIdxItem *pItem, TABLEID id) {
  int32_t  code = 0;
  int32_t  lidx = 0;
  int32_t  ridx = pDelIdx->nItem - 1;
  int32_t  midx;
  uint64_t offset;
  int32_t  c;

  while (lidx <= ridx) {
    midx = (lidx + ridx) / 2;

    tDelIdxGetItemByIdx(pDelIdx, pItem, midx);
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

int32_t tDelIdxGetItemByIdx(SDelIdx *pDelIdx, SDelIdxItem *pItem, int32_t idx) {
  tGetDelIdxItem(pDelIdx->pData + tsdbGetOffset(pDelIdx->pOffset, pDelIdx->flags, idx), pItem);
  return 0;
}

int32_t tDelIdxPutItem(SDelIdx *pDelIdx, SDelIdxItem *pItem) {
  int32_t  code = 0;
  int32_t  size = tPutDelIdxItem(NULL, pItem);
  uint32_t offset = pDelIdx->nData;
  uint32_t nItem = pDelIdx->nItem;

  pDelIdx->nItem++;
  pDelIdx->nData += size;

  // alloc
  code = tsdbRealloc(&pDelIdx->pOffset, pDelIdx->nItem * sizeof(uint32_t));
  if (code) goto _exit;
  code = tsdbRealloc(&pDelIdx->pData, pDelIdx->nData);
  if (code) goto _exit;

  // put
  ((uint32_t *)pDelIdx->pOffset)[nItem] = offset;
  tPutDelIdxItem(pDelIdx->pData + offset, pItem);

_exit:
  return code;
}

int32_t tPutDelIdx(uint8_t *p, SDelIdx *pDelIdx) {
  int32_t n = 0;

  n += tPutU32(p ? p + n : p, pDelIdx->delimiter);
  n += tPutU8(p ? p + n : p, pDelIdx->flags);
  n += tPutU32v(p ? p + n : p, pDelIdx->nItem);
  n += tPutBinary(p ? p + n : p, pDelIdx->pOffset, pDelIdx->nItem * tsdbOffsetSize(pDelIdx->flags));
  n += tPutBinary(p ? p + n : p, pDelIdx->pData, pDelIdx->nData);

  return n;
}

int32_t tGetDelIdx(uint8_t *p, SDelIdx *pDelIdx) {
  int32_t n = 0;

  n += tGetU32(p, &pDelIdx->delimiter);
  n += tGetU8(p, &pDelIdx->flags);
  n += tGetU32v(p, &pDelIdx->nItem);
  n += tGetBinary(p, &pDelIdx->pOffset, NULL);
  n += tGetBinary(p, &pDelIdx->pData, &pDelIdx->nData);

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

  n += tGetI64v(p, &pItem->version);
  n += tGetI64(p, &pItem->sKey);
  n += tGetI64(p, &pItem->eKey);

  return n;
}

// SDelData ======================================================
int32_t tDelDataGetItem(SDelData *pDelData, SDelDataItem *pItem, int64_t version) {
  int32_t code = 0;
  int32_t lidx = 0;
  int32_t ridx = pDelData->nItem - 1;
  int32_t midx;

  while (lidx <= ridx) {
    midx = (lidx + ridx) / 2;

    tDelDataGetItemByIdx(pDelData, pItem, midx);
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

int32_t tDelDataGetItemByIdx(SDelData *pDelData, SDelDataItem *pItem, int32_t idx) {
  tGetDelDataItem(pDelData->pData + tsdbGetOffset(pDelData->pOffset, pDelData->flags, idx), pItem);
  return 0;
}

int32_t tDelDataPutItem(SDelData *pDelData, SDelDataItem *pItem) {
  int32_t  code = 0;
  uint32_t nItem = pDelData->nItem;
  uint32_t offset = pDelData->nData;

  pDelData->nItem++;
  pDelData->nData += tPutDelDataItem(NULL, pItem);

  // alloc
  code = tsdbRealloc(&pDelData->pOffset, pDelData->nItem * sizeof(uint32_t));
  if (code) goto _exit;
  code = tsdbRealloc(&pDelData->pData, pDelData->nData);
  if (code) goto _exit;

  // put
  ((uint32_t *)pDelData->pOffset)[nItem] = offset;
  tPutDelDataItem(pDelData->pData + offset, pItem);

_exit:
  return code;
}

int32_t tPutDelData(uint8_t *p, SDelData *pDelData) {
  int32_t n = 0;

  n += tPutU32(p ? p + n : p, pDelData->delimiter);
  n += tPutI64(p ? p + n : p, pDelData->suid);
  n += tPutI64(p ? p + n : p, pDelData->uid);
  n += tPutU8(p ? p + n : p, pDelData->flags);
  n += tPutU32v(p ? p + n : p, pDelData->nItem);
  n += tPutBinary(p ? p + n : p, pDelData->pOffset, pDelData->nItem * tsdbOffsetSize(pDelData->flags));
  n += tPutBinary(p ? p + n : p, pDelData->pData, pDelData->nData);

  return n;
}

int32_t tGetDelData(uint8_t *p, SDelData *pDelData) {
  int32_t n = 0;

  n += tGetU32(p, &pDelData->delimiter);
  n += tGetI64(p, &pDelData->suid);
  n += tGetI64(p, &pDelData->uid);
  n += tGetU8(p, &pDelData->flags);
  n += tGetU32v(p, &pDelData->nItem);
  n += tGetBinary(p, &pDelData->pOffset, NULL);
  n += tGetBinary(p, &pDelData->pData, &pDelData->nData);

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

  n += tGetI64(p, &pDelFile->minKey);
  n += tGetI64(p, &pDelFile->maxKey);
  n += tGetI64v(p, &pDelFile->minVersion);
  n += tGetI64v(p, &pDelFile->maxVersion);
  n += tGetI64v(p, &pDelFile->size);
  n += tGetI64v(p, &pDelFile->offset);

  return n;
}

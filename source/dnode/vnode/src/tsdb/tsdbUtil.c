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

int32_t tPutDelIdx(uint8_t *p, SDelIdx *pDelIdx) {
  int32_t n = 0;

  n += tPutU32(p ? p + n : p, pDelIdx->delimiter);
  n += tPutU8(p ? p + n : p, pDelIdx->flags);
  n += tPutBinary(p ? p + n : p, pDelIdx->pOffset, pDelIdx->nOffset);
  n += tPutBinary(p ? p + n : p, pDelIdx->pData, pDelIdx->nData);

  return n;
}

int32_t tGetDelIdx(uint8_t *p, SDelIdx *pDelIdx) {
  int32_t n = 0;

  n += tGetU32(p, &pDelIdx->delimiter);
  n += tGetU8(p, &pDelIdx->flags);
  n += tGetBinary(p, &pDelIdx->pOffset, &pDelIdx->nOffset);
  n += tGetBinary(p, &pDelIdx->pData, &pDelIdx->nData);

  return n;
}

int32_t tPutDelData(uint8_t *p, SDelData *pDelData) {
  int32_t n = 0;

  n += tPutU32(p ? p + n : p, pDelData->delimiter);
  n += tPutU8(p ? p + n : p, pDelData->flags);
  n += tPutBinary(p ? p + n : p, pDelData->pOffset, pDelData->nOffset);
  n += tPutBinary(p ? p + n : p, pDelData->pData, pDelData->nData);

  return n;
}

int32_t tGetDelData(uint8_t *p, SDelData *pDelData) {
  int32_t n = 0;

  n += tGetU32(p, &pDelData->delimiter);
  n += tGetU8(p, &pDelData->flags);
  n += tGetBinary(p, &pDelData->pOffset, &pDelData->nOffset);
  n += tGetBinary(p, &pDelData->pData, &pDelData->nData);

  return n;
}
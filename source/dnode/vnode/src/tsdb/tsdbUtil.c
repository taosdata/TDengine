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

int32_t tPutSDelIdx(uint8_t *p, SDelIdx *pDelIdx) {
  int32_t n = 0;

  n += tPutU32(p ? p + n : p, pDelIdx->delimiter);
  n += tPutU8(p ? p + n : p, pDelIdx->flags);
  n += tPutBinary(p ? p + n : p, pDelIdx->pOffset, pDelIdx->nOffset);
  n += tPutBinary(p ? p + n : p, pDelIdx->pData, pDelIdx->nData);

  return n;
}

int32_t tGetSDelIdx(uint8_t *p, SDelIdx *pDelIdx) {
  int32_t n = 0;

  n += tGetU32(p + n, &pDelIdx->delimiter);
  n += tGetU8(p + n, &pDelIdx->flags);
  n += tGetBinary(p + n, &pDelIdx->pOffset, &pDelIdx->nOffset);
  n += tGetBinary(p + n, &pDelIdx->pData, &pDelIdx->nData);

  return n;
}
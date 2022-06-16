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

typedef struct STsdbReader STsdbReader;

typedef struct {
  TSKEY   ts;
  int64_t version;
} SSkylineItem;

static int32_t tsdbMergeSkyline(SArray *aSkyline1, SArray *aSkyline2, SArray *aSkyline) {
  int32_t       code = 0;
  int32_t       i1 = 0;
  int32_t       n1 = taosArrayGetSize(aSkyline1);
  int32_t       i2 = 0;
  int32_t       n2 = taosArrayGetSize(aSkyline2);
  SSkylineItem *pItem1;
  SSkylineItem *pItem2;
  SSkylineItem  item;
  int64_t       version1 = 0;
  int64_t       version2 = 0;

  ASSERT(n1 > 0 && n2 > 0);

  taosArrayClear(aSkyline);

  while (i1 < n1 && i2 < n2) {
    pItem1 = (SSkylineItem *)taosArrayGet(aSkyline1, i1);
    pItem2 = (SSkylineItem *)taosArrayGet(aSkyline2, i2);

    if (pItem1->ts < pItem2->ts) {
      version1 = pItem1->version;
      item.ts = pItem1->ts;
      item.version = TMAX(version1, version2);
      i1++;
    } else if (pItem1->ts > pItem2->ts) {
      version2 = pItem2->version;
      item.ts = pItem2->ts;
      item.version = TMAX(version1, version2);
      i2++;
    } else {
      version1 = pItem1->version;
      version2 = pItem2->version;
      item.ts = pItem1->ts;
      item.version = TMAX(version1, version2);
      i1++;
      i2++;
    }

    if (taosArrayPush(aSkyline, &item) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

  while (i1 < n1) {
    pItem1 = (SSkylineItem *)taosArrayGet(aSkyline1, i1);
    if (taosArrayPush(aSkyline, &(SSkylineItem){.ts = pItem1->ts, .version = pItem1->version}) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    i1++;
  }

  while (i2 < n2) {
    pItem2 = (SSkylineItem *)taosArrayGet(aSkyline2, i2);
    if (taosArrayPush(aSkyline, &(SSkylineItem){.ts = pItem2->ts, .version = pItem2->version}) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    i2++;
  }

_exit:
  return code;
}
static int32_t tsdbBuildDeleteSkyline(SArray *pDelArray, SArray **ppSkylineArray) {
  int32_t code = 0;
  SArray *pSkeylineArray = NULL;
  int32_t nDel = pDelArray ? taosArrayGetSize(pDelArray) : 0;

  if (nDel == 0) goto _exit;

_exit:
  *ppSkylineArray = pSkeylineArray;
  return code;

_err:
  return code;
}
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

#define _DEFAULT_SOURCE
#include "dmRepairCopy.h"
#include "tlog.h"

static int32_t compareInt32(const void *a, const void *b) {
  int32_t va = *(const int32_t *)a;
  int32_t vb = *(const int32_t *)b;
  if (va < vb) return -1;
  if (va > vb) return 1;
  return 0;
}

SArray *dmParseVnodeIds(const char *str) {
  if (str == NULL || str[0] == '\0') return NULL;

  SArray *pArr = taosArrayInit(8, sizeof(int32_t));
  if (pArr == NULL) return NULL;

  const char *p = str;
  while (*p != '\0') {
    // skip leading whitespace
    while (*p == ' ' || *p == '\t') p++;
    if (*p == '\0') break;

    // parse first number
    char   *end = NULL;
    int32_t lo = taosStr2Int32(p, &end, 10);
    if (end == p || lo <= 0) goto _err;

    // skip whitespace
    while (*end == ' ' || *end == '\t') end++;

    if (*end == '-') {
      // range: lo-hi
      end++;
      int32_t hi = taosStr2Int32(end, &end, 10);
      if (hi < lo) goto _err;
      for (int32_t id = lo; id <= hi; id++) {
        if (taosArrayPush(pArr, &id) == NULL) goto _err;
      }
    } else {
      if (taosArrayPush(pArr, &lo) == NULL) goto _err;
    }

    // skip whitespace
    while (*end == ' ' || *end == '\t') end++;

    if (*end == ',') {
      end++;
    } else if (*end != '\0') {
      goto _err;
    }
    p = end;
  }

  if (taosArrayGetSize(pArr) == 0) goto _err;

  taosArraySort(pArr, compareInt32);
  taosArrayRemoveDuplicate(pArr, compareInt32, NULL);

  return pArr;

_err:
  taosArrayDestroy(pArr);
  return NULL;
}

int32_t dmRepairCopyMode(const SRepairCopyOpts *pOpts) {
  // TODO: implement in later phases
  (void)pOpts;
  return 0;
}

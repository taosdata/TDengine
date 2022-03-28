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
#include "index_util.h"
#include "index.h"
#include "tcompare.h"

typedef struct MergeIndex {
  int idx;
  int len;
} MergeIndex;

static int iBinarySearch(SArray *arr, int s, int e, uint64_t k) {
  uint64_t v;
  int32_t  m;
  while (s <= e) {
    m = s + (e - s) / 2;
    v = *(uint64_t *)taosArrayGet(arr, m);
    if (v >= k) {
      e = m - 1;
    } else {
      s = m + 1;
    }
  }
  return s;
}

void iIntersection(SArray *inters, SArray *final) {
  int32_t sz = taosArrayGetSize(inters);
  if (sz <= 0) {
    return;
  }
  MergeIndex *mi = taosMemoryCalloc(sz, sizeof(MergeIndex));
  for (int i = 0; i < sz; i++) {
    SArray *t = taosArrayGetP(inters, i);
    mi[i].len = taosArrayGetSize(t);
    mi[i].idx = 0;
  }

  SArray *base = taosArrayGetP(inters, 0);
  for (int i = 0; i < taosArrayGetSize(base); i++) {
    uint64_t tgt = *(uint64_t *)taosArrayGet(base, i);
    bool     has = true;
    for (int j = 1; j < taosArrayGetSize(inters); j++) {
      SArray *oth = taosArrayGetP(inters, j);
      int     mid = iBinarySearch(oth, mi[j].idx, mi[j].len - 1, tgt);
      if (mid >= 0 && mid < mi[j].len) {
        uint64_t val = *(uint64_t *)taosArrayGet(oth, mid);
        has = (val == tgt ? true : false);
        mi[j].idx = mid;
      } else {
        has = false;
      }
    }
    if (has == true) {
      taosArrayPush(final, &tgt);
    }
  }
  taosMemoryFreeClear(mi);
}
void iUnion(SArray *inters, SArray *final) {
  int32_t sz = taosArrayGetSize(inters);
  if (sz <= 0) {
    return;
  }
  if (sz == 1) {
    taosArrayAddAll(final, taosArrayGetP(inters, 0));
    return;
  }

  MergeIndex *mi = taosMemoryCalloc(sz, sizeof(MergeIndex));
  for (int i = 0; i < sz; i++) {
    SArray *t = taosArrayGetP(inters, i);
    mi[i].len = taosArrayGetSize(t);
    mi[i].idx = 0;
  }
  while (1) {
    uint64_t mVal = UINT_MAX;
    int      mIdx = -1;

    for (int j = 0; j < sz; j++) {
      SArray *t = taosArrayGetP(inters, j);
      if (mi[j].idx >= mi[j].len) {
        continue;
      }
      uint64_t cVal = *(uint64_t *)taosArrayGet(t, mi[j].idx);
      if (cVal < mVal) {
        mVal = cVal;
        mIdx = j;
      }
    }
    if (mIdx != -1) {
      mi[mIdx].idx++;
      if (taosArrayGetSize(final) > 0) {
        uint64_t lVal = *(uint64_t *)taosArrayGetLast(final);
        if (lVal == mVal) {
          continue;
        }
      }
      taosArrayPush(final, &mVal);
    } else {
      break;
    }
  }
  taosMemoryFreeClear(mi);
}

void iExcept(SArray *total, SArray *except) {
  int32_t tsz = taosArrayGetSize(total);
  int32_t esz = taosArrayGetSize(except);
  if (esz == 0 || tsz == 0) {
    return;
  }

  int vIdx = 0;
  for (int i = 0; i < tsz; i++) {
    uint64_t val = *(uint64_t *)taosArrayGet(total, i);
    int      idx = iBinarySearch(except, 0, esz - 1, val);
    if (idx >= 0 && idx < esz && *(uint64_t *)taosArrayGet(except, idx) == val) {
      continue;
    }
    taosArraySet(total, vIdx, &val);
    vIdx += 1;
  }

  taosArrayPopTailBatch(total, tsz - vIdx);
}

int uidCompare(const void *a, const void *b) {
  // add more version compare
  uint64_t u1 = *(uint64_t *)a;
  uint64_t u2 = *(uint64_t *)b;
  return u1 - u2;
}
int verdataCompare(const void *a, const void *b) {
  SIdxVerdata *va = (SIdxVerdata *)a;
  SIdxVerdata *vb = (SIdxVerdata *)b;

  int32_t cmp = compareUint64Val(&va->data, &vb->data);
  if (cmp == 0) {
    cmp = 0 - compareUint32Val(&va->ver, &vb->data);
    return cmp;
  }
  return cmp;
}

SIdxTempResult *sIdxTempResultCreate() {
  SIdxTempResult *tr = taosMemoryCalloc(1, sizeof(SIdxTempResult));

  tr->total = taosArrayInit(4, sizeof(uint64_t));
  tr->added = taosArrayInit(4, sizeof(uint64_t));
  tr->deled = taosArrayInit(4, sizeof(uint64_t));
  return tr;
}
void sIdxTempResultClear(SIdxTempResult *tr) {
  if (tr == NULL) {
    return;
  }
  taosArrayClear(tr->total);
  taosArrayClear(tr->added);
  taosArrayClear(tr->deled);
}
void sIdxTempResultDestroy(SIdxTempResult *tr) {
  if (tr == NULL) {
    return;
  }
  taosArrayDestroy(tr->total);
  taosArrayDestroy(tr->added);
  taosArrayDestroy(tr->deled);
}
void sIdxTempResultMergeTo(SArray *result, SIdxTempResult *tr) {
  taosArraySort(tr->total, uidCompare);
  taosArraySort(tr->added, uidCompare);
  taosArraySort(tr->deled, uidCompare);

  SArray *arrs = taosArrayInit(2, sizeof(void *));
  taosArrayPush(arrs, &tr->total);
  taosArrayPush(arrs, &tr->added);

  iUnion(arrs, result);
  taosArrayDestroy(arrs);

  iExcept(result, tr->deled);
}

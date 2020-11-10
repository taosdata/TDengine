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

#include "os.h"
#include "tutil.h"
#include "talgo.h"

#define doswap(__left, __right, __size, __buf) do {\
  memcpy((__buf), (__left), (__size));\
  memcpy((__left), (__right),(__size));\
  memcpy((__right), (__buf), (__size));\
} while (0);

static void median(void *src, size_t size, size_t s, size_t e, const void *param, __ext_compar_fn_t comparFn, void* buf) {
  int32_t mid = ((int32_t)(e - s) >> 1u) + (int32_t)s;
  
  if (comparFn(elePtrAt(src, size, mid), elePtrAt(src, size, s), param) == 1) {
    doswap(elePtrAt(src, size, mid), elePtrAt(src, size, s), size, buf);
  }
  
  if (comparFn(elePtrAt(src, size, mid), elePtrAt(src, size, e), param) == 1) {
    doswap(elePtrAt(src, size, mid), elePtrAt(src, size, s), size, buf);
    doswap(elePtrAt(src, size, mid), elePtrAt(src, size, e), size, buf);
  } else if (comparFn(elePtrAt(src, size, s), elePtrAt(src, size, e), param) == 1) {
    doswap(elePtrAt(src, size, s), elePtrAt(src, size, e), size, buf);
  }
  
  assert(comparFn(elePtrAt(src, size, mid), elePtrAt(src, size, s), param) <= 0 && comparFn(elePtrAt(src, size, s), elePtrAt(src, size, e), param) <= 0);

#ifdef _DEBUG_VIEW
//  tTagsPrints(src[s], pOrderDesc->pColumnModel, &pOrderDesc->orderIdx);
//  tTagsPrints(src[mid], pOrderDesc->pColumnModel, &pOrderDesc->orderIdx);
//  tTagsPrints(src[e], pOrderDesc->pColumnModel, &pOrderDesc->orderIdx);
#endif
}

static void tInsertSort(void *src, size_t size, int32_t s, int32_t e, const void *param, __ext_compar_fn_t comparFn,
                        void* buf) {
  for (int32_t i = s + 1; i <= e; ++i) {
    for (int32_t j = i; j > s; --j) {
      if (comparFn(elePtrAt(src, size, j), elePtrAt(src, size, j - 1), param) == -1) {
        doswap(elePtrAt(src, size, j), elePtrAt(src, size, j - 1), size, buf);
      } else {
        break;
      }
    }
  }
}

static void tqsortImpl(void *src, int32_t start, int32_t end, size_t size, const void *param, __ext_compar_fn_t comparFn,
                void* buf) {
  // short array sort, incur another sort procedure instead of quick sort process
  const int32_t THRESHOLD_SIZE = 6;
  if (end - start + 1 <= THRESHOLD_SIZE) {
    tInsertSort(src, size, start, end, param, comparFn, buf);
    return;
  }
  
  median(src, size, start, end, param, comparFn, buf);
  
  int32_t s = start, e = end;
  int32_t endRightS = end, startLeftS = start;
  
  while (s < e) {
    while (e > s) {
      int32_t ret = comparFn(elePtrAt(src, size, e), elePtrAt(src, size, s), param);
      if (ret < 0) {
        break;
      }
      
      //move the data that equals to pivotal value to the right end of the list
      if (ret == 0 && e != endRightS) {
        doswap(elePtrAt(src, size, e), elePtrAt(src, size, endRightS), size, buf);
        endRightS--;
      }
      
      e--;
    }
    
    if (e != s) {
      doswap(elePtrAt(src, size, e), elePtrAt(src, size, s), size, buf);
    }
    
    while (s < e) {
      int32_t ret = comparFn(elePtrAt(src, size, s), elePtrAt(src, size, e), param);
      if (ret > 0) {
        break;
      }
      
      if (ret == 0 && s != startLeftS) {
        doswap(elePtrAt(src, size, s), elePtrAt(src, size, startLeftS), size, buf);
        startLeftS++;
      }
      s++;
    }
    
    if (e != s) {
      doswap(elePtrAt(src, size, s), elePtrAt(src, size, e), size, buf);
    }
  }
  
  int32_t rightPartStart = e + 1;
  if (endRightS != end && e < end) {
    int32_t left = rightPartStart;
    int32_t right = end;
    
    while (right > endRightS && left <= endRightS) {
      doswap(elePtrAt(src, size, left), elePtrAt(src, size, right), size, buf);
      
      left++;
      right--;
    }
    
    rightPartStart += (end - endRightS);
  }
  
  int32_t leftPartEnd = e - 1;
  if (startLeftS != end && s > start) {
    int32_t left = start;
    int32_t right = leftPartEnd;
    
    while (left < startLeftS && right >= startLeftS) {
      doswap(elePtrAt(src, size, left), elePtrAt(src, size, right), size, buf);
      
      left++;
      right--;
    }
    
    leftPartEnd -= (startLeftS - start);
  }
  
  if (leftPartEnd > start) {
    tqsortImpl(src, start, leftPartEnd, size, param, comparFn, buf);
  }
  
  if (rightPartStart < end) {
    tqsortImpl(src, rightPartStart, end, size, param, comparFn, buf);
  }
}

void taosqsort(void *src, size_t numOfElem, size_t size, const void* param, __ext_compar_fn_t comparFn) {
  char *buf = calloc(1, size);   // prepare the swap buffer
  tqsortImpl(src, 0, (int32_t)numOfElem - 1, (int32_t)size, param, comparFn, buf);
  tfree(buf);
}

void * taosbsearch(const void *key, const void *base, size_t nmemb, size_t size, __compar_fn_t compar, int flags) {
  // TODO: need to check the correctness of this function
  int l = 0;
  int r = (int)nmemb;
  int idx = 0;
  int comparison;
  
  if (flags == TD_EQ) {
    return bsearch(key, base, nmemb, size, compar);
  } else if (flags == TD_GE) {
    if (nmemb <= 0) return NULL;
    if ((*compar)(key, elePtrAt(base, size, 0)) <= 0) return elePtrAt(base, size, 0);
    if ((*compar)(key, elePtrAt(base, size, nmemb - 1)) > 0) return NULL;
    
    while (l < r) {
      idx = (l + r) / 2;
      comparison = (*compar)(key, elePtrAt(base, size, idx));
      if (comparison < 0) {
        r = idx;
      } else if (comparison > 0) {
        l = idx + 1;
      } else {
        return elePtrAt(base, size, idx);
      }
    }
    
    if ((*compar)(key, elePtrAt(base, size, idx)) < 0) {
      return elePtrAt(base, size, idx);
    } else {
      if (idx + 1 > nmemb - 1) {
        return NULL;
      } else {
        return elePtrAt(base, size, idx + 1);
      }
    }
  } else if (flags == TD_LE) {
    if (nmemb <= 0) return NULL;
    if ((*compar)(key, elePtrAt(base, size, nmemb - 1)) >= 0) return elePtrAt(base, size, nmemb - 1);
    if ((*compar)(key, elePtrAt(base, size, 0)) < 0) return NULL;
    
    while (l < r) {
      idx = (l + r) / 2;
      comparison = (*compar)(key, elePtrAt(base, size, idx));
      if (comparison < 0) {
        r = idx;
      } else if (comparison > 0) {
        l = idx + 1;
      } else {
        return elePtrAt(base, size, idx);
      }
    }
    
    if ((*compar)(key, elePtrAt(base, size, idx)) > 0) {
      return elePtrAt(base, size, idx);
    } else {
      if (idx == 0) {
        return NULL;
      } else {
        return elePtrAt(base, size, idx - 1);
      }
    }
    
  } else {
    assert(0);
    return NULL;
  }
  
  return NULL;
}

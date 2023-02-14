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
#include "talgo.h"
#include "tlog.h"

#define doswap(__left, __right, __size, __buf) \
  do {                                         \
    memcpy((__buf), (__left), (__size));       \
    memcpy((__left), (__right), (__size));     \
    memcpy((__right), (__buf), (__size));      \
  } while (0);

static void median(void *src, int64_t size, int64_t s, int64_t e, const void *param, __ext_compar_fn_t comparFn,
                   void *buf) {
  int32_t mid = ((int32_t)(e - s) >> 1u) + (int32_t)s;

  if (comparFn(elePtrAt(src, size, mid), elePtrAt(src, size, s), param) > 0) {
    doswap(elePtrAt(src, size, mid), elePtrAt(src, size, s), size, buf);
  }

  if (comparFn(elePtrAt(src, size, mid), elePtrAt(src, size, e), param) > 0) {
    doswap(elePtrAt(src, size, mid), elePtrAt(src, size, s), size, buf);
    doswap(elePtrAt(src, size, mid), elePtrAt(src, size, e), size, buf);
  } else if (comparFn(elePtrAt(src, size, s), elePtrAt(src, size, e), param) > 0) {
    doswap(elePtrAt(src, size, s), elePtrAt(src, size, e), size, buf);
  }

  ASSERT(comparFn(elePtrAt(src, size, mid), elePtrAt(src, size, s), param) <= 0 &&
         comparFn(elePtrAt(src, size, s), elePtrAt(src, size, e), param) <= 0);
}

static void tInsertSort(void *src, int64_t size, int32_t s, int32_t e, const void *param, __ext_compar_fn_t comparFn,
                        void *buf) {
  for (int32_t i = s + 1; i <= e; ++i) {
    for (int32_t j = i; j > s; --j) {
      if (comparFn(elePtrAt(src, size, j), elePtrAt(src, size, j - 1), param) < 0) {
        doswap(elePtrAt(src, size, j), elePtrAt(src, size, j - 1), size, buf);
      } else {
        break;
      }
    }
  }
}

static void tqsortImpl(void *src, int32_t start, int32_t end, int64_t size, const void *param,
                       __ext_compar_fn_t comparFn, void *buf) {
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

      // move the data that equals to pivotal value to the right end of the list
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

void taosqsort(void *src, int64_t numOfElem, int64_t size, const void *param, __ext_compar_fn_t comparFn) {
  char *buf = taosMemoryCalloc(1, size);  // prepare the swap buffer
  tqsortImpl(src, 0, (int32_t)numOfElem - 1, (int32_t)size, param, comparFn, buf);
  taosMemoryFreeClear(buf);
}

void *taosbsearch(const void *key, const void *base, int32_t nmemb, int32_t size, __compar_fn_t compar, int32_t flags) {
  uint8_t *p;
  int32_t  lidx;
  int32_t  ridx;
  int32_t  midx;
  int32_t  c;

  if (nmemb <= 0) return NULL;

  lidx = 0;
  ridx = nmemb - 1;
  while (lidx <= ridx) {
    midx = (lidx + ridx) / 2;
    p = (uint8_t *)base + size * midx;

    c = compar(key, p);
    if (c == 0) {
      if (flags == TD_GT) {
        lidx = midx + 1;
      } else if (flags == TD_LT) {
        ridx = midx - 1;
      } else {
        break;
      }
    } else if (c < 0) {
      ridx = midx - 1;
    } else {
      lidx = midx + 1;
    }
  }

  if (flags == TD_EQ) {
    return c ? NULL : p;
  } else if (flags == TD_GE) {
    return (c <= 0) ? p : (midx + 1 < nmemb ? p + size : NULL);
  } else if (flags == TD_LE) {
    return (c >= 0) ? p : (midx > 0 ? p - size : NULL);
  } else if (flags == TD_GT) {
    return (c < 0) ? p : (midx + 1 < nmemb ? p + size : NULL);
  } else if (flags == TD_LT) {
    return (c > 0) ? p : (midx > 0 ? p - size : NULL);
  } else {
    ASSERT(0);
    return NULL;
  }
}

void taosheapadjust(void *base, int32_t size, int32_t start, int32_t end, const void *parcompar,
                    __ext_compar_fn_t compar, char *buf, bool maxroot) {
  int32_t parent;
  int32_t child;

  char *tmp = NULL;
  if (buf == NULL) {
    tmp = taosMemoryMalloc(size);
  } else {
    tmp = buf;
  }

  if (base && size > 0 && compar) {
    parent = start;
    child = 2 * parent + 1;

    if (maxroot) {
      while (child <= end) {
        if (child + 1 <= end &&
            (*compar)(elePtrAt(base, size, child), elePtrAt(base, size, child + 1), parcompar) < 0) {
          child++;
        }

        if ((*compar)(elePtrAt(base, size, parent), elePtrAt(base, size, child), parcompar) > 0) {
          break;
        }

        doswap(elePtrAt(base, size, parent), elePtrAt(base, size, child), size, tmp);

        parent = child;
        child = 2 * parent + 1;
      }
    } else {
      while (child <= end) {
        if (child + 1 <= end &&
            (*compar)(elePtrAt(base, size, child), elePtrAt(base, size, child + 1), parcompar) > 0) {
          child++;
        }

        if ((*compar)(elePtrAt(base, size, parent), elePtrAt(base, size, child), parcompar) < 0) {
          break;
        }

        doswap(elePtrAt(base, size, parent), elePtrAt(base, size, child), size, tmp);

        parent = child;
        child = 2 * parent + 1;
      }
    }
  }

  if (buf == NULL) {
    taosMemoryFree(tmp);
  }
}

void taosheapsort(void *base, int32_t size, int32_t len, const void *parcompar, __ext_compar_fn_t compar,
                  bool maxroot) {
  int32_t i;

  char *buf = taosMemoryCalloc(1, size);
  if (buf == NULL) {
    return;
  }

  if (base && size > 0) {
    for (i = len / 2 - 1; i >= 0; i--) {
      taosheapadjust(base, size, i, len - 1, parcompar, compar, buf, maxroot);
    }
  }

  taosMemoryFree(buf);
}

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
#include "tarray.h"
#include "tcoding.h"

SArray* taosArrayInit(size_t size, size_t elemSize) {
  assert(elemSize > 0);

  if (size < TARRAY_MIN_SIZE) {
    size = TARRAY_MIN_SIZE;
  }

  SArray* pArray = taosMemoryMalloc(sizeof(SArray));
  if (pArray == NULL) {
    return NULL;
  }

  pArray->size = 0;
  pArray->pData = taosMemoryCalloc(size, elemSize);
  if (pArray->pData == NULL) {
    taosMemoryFree(pArray);
    return NULL;
  }

  pArray->capacity = size;
  pArray->elemSize = elemSize;
  return pArray;
}

static int32_t taosArrayResize(SArray* pArray) {
  assert(pArray->size >= pArray->capacity);

  size_t size = pArray->capacity;
  size = (size << 1u);

  void* tmp = taosMemoryRealloc(pArray->pData, size * pArray->elemSize);
  if (tmp == NULL) {  // reallocate failed, the original buffer remains
    return -1;
  }

  pArray->pData = tmp;
  pArray->capacity = size;

  return 0;
}

int32_t taosArrayEnsureCap(SArray* pArray, size_t newCap) {
  if (newCap > pArray->capacity) {
    size_t tsize = (pArray->capacity << 1u);
    while (newCap > tsize) {
      tsize = (tsize << 1u);
    }

    pArray->pData = taosMemoryRealloc(pArray->pData, tsize * pArray->elemSize);
    if (pArray->pData == NULL) {
      return -1;
    }

    pArray->capacity = tsize;
  }
  return 0;
}

void* taosArrayAddBatch(SArray* pArray, const void* pData, int32_t nEles) {
  if (pData == NULL) {
    return NULL;
  }

  if (taosArrayEnsureCap(pArray, pArray->size + nEles) != 0) {
    return NULL;
  }

  void* dst = TARRAY_GET_ELEM(pArray, pArray->size);
  memcpy(dst, pData, pArray->elemSize * nEles);

  pArray->size += nEles;
  return dst;
}

void taosArrayRemoveBatch(SArray* pArray, const int32_t* pData, int32_t numOfElems) {
  assert(pArray != NULL && pData != NULL);
  if (numOfElems <= 0) {
    return;
  }

  size_t size = taosArrayGetSize(pArray);
  if (numOfElems >= size) {
    taosArrayClear(pArray);
    return;
  }

  int32_t i = pData[0] + 1, j = 0;
  while (i < size) {
    if (j == numOfElems - 1) {
      break;
    }

    char* p = TARRAY_GET_ELEM(pArray, i);
    if (i > pData[j] && i < pData[j + 1]) {
      char* dst = TARRAY_GET_ELEM(pArray, i - (j + 1));
      memmove(dst, p, pArray->elemSize);
    } else if (i == pData[j + 1]) {
      j += 1;
    }

    i += 1;
  }

  assert(i == pData[numOfElems - 1] + 1 && i <= size);

  int32_t srcIndex = pData[numOfElems - 1] + 1;
  int32_t dstIndex = pData[numOfElems - 1] - numOfElems + 1;
  if (pArray->size - srcIndex > 0) {
    char* dst = TARRAY_GET_ELEM(pArray, dstIndex);
    char* src = TARRAY_GET_ELEM(pArray, srcIndex);
    memmove(dst, src, pArray->elemSize * (pArray->size - srcIndex));
  }

  pArray->size -= numOfElems;
}

void taosArrayRemoveDuplicate(SArray* pArray, __compar_fn_t comparFn, void (*fp)(void*)) {
  assert(pArray);

  size_t size = pArray->size;
  if (size <= 1) {
    return;
  }

  int32_t pos = 0;
  for (int32_t i = 1; i < size; ++i) {
    char* p1 = taosArrayGet(pArray, pos);
    char* p2 = taosArrayGet(pArray, i);

    if (comparFn(p1, p2) == 0) {
      // do nothing
    } else {
      if (pos + 1 != i) {
        void* p = taosArrayGet(pArray, pos + 1);
        if (fp != NULL) {
          fp(p);
        }

        taosArraySet(pArray, pos + 1, p2);
        pos += 1;
      } else {
        pos += 1;
      }
    }
  }

  if (fp != NULL) {
    for (int32_t i = pos + 1; i < pArray->size; ++i) {
      void* p = taosArrayGet(pArray, i);
      fp(p);
    }
  }

  pArray->size = pos + 1;
}

void taosArrayRemoveDuplicateP(SArray* pArray, __compar_fn_t comparFn, void (*fp)(void*)) {
  assert(pArray);

  size_t size = pArray->size;
  if (size <= 1) {
    return;
  }

  int32_t pos = 0;
  for (int32_t i = 1; i < size; ++i) {
    char* p1 = taosArrayGet(pArray, pos);
    char* p2 = taosArrayGet(pArray, i);

    if (comparFn(p1, p2) == 0) {
      // do nothing
    } else {
      if (pos + 1 != i) {
        void* p = taosArrayGet(pArray, pos + 1);
        if (fp != NULL) {
          fp(p);
        }

        taosArraySet(pArray, pos + 1, p2);
        pos += 1;
      } else {
        pos += 1;
      }
    }
  }

  if (fp != NULL) {
    for (int32_t i = pos + 1; i < pArray->size; ++i) {
      void* p = taosArrayGetP(pArray, i);
      fp(p);
    }
  }

  pArray->size = pos + 1;
}

void* taosArrayAddAll(SArray* pArray, const SArray* pInput) {
  if (pInput) {
    return taosArrayAddBatch(pArray, pInput->pData, (int32_t)taosArrayGetSize(pInput));
  } else {
    return NULL;
  }
}

void* taosArrayPop(SArray* pArray) {
  assert(pArray != NULL);

  if (pArray->size == 0) {
    return NULL;
  }
  pArray->size -= 1;
  return TARRAY_GET_ELEM(pArray, pArray->size);
}

void* taosArrayGet(const SArray* pArray, size_t index) {
  assert(index < pArray->size);
  return TARRAY_GET_ELEM(pArray, index);
}

void* taosArrayGetP(const SArray* pArray, size_t index) {
  assert(index < pArray->size);

  void* d = TARRAY_GET_ELEM(pArray, index);

  return *(void**)d;
}

void* taosArrayGetLast(const SArray* pArray) { return TARRAY_GET_ELEM(pArray, pArray->size - 1); }

size_t taosArrayGetSize(const SArray* pArray) {
  if (pArray == NULL) {
    return 0;
  }
  return pArray->size;
}

void taosArraySetSize(SArray* pArray, size_t size) {
  assert(size <= pArray->capacity);
  pArray->size = size;
}

void* taosArrayInsert(SArray* pArray, size_t index, void* pData) {
  if (pArray == NULL || pData == NULL) {
    return NULL;
  }

  if (index >= pArray->size) {
    return taosArrayPush(pArray, pData);
  }

  if (pArray->size >= pArray->capacity) {
    int32_t ret = taosArrayResize(pArray);

    if (ret < 0) {
      return NULL;
    }
  }

  void* dst = TARRAY_GET_ELEM(pArray, index);

  int32_t remain = (int32_t)(pArray->size - index);
  memmove((char*)dst + pArray->elemSize, (char*)dst, pArray->elemSize * remain);
  memcpy(dst, pData, pArray->elemSize);

  pArray->size += 1;

  return dst;
}

void taosArraySet(SArray* pArray, size_t index, void* pData) {
  assert(index < pArray->size);
  memcpy(TARRAY_GET_ELEM(pArray, index), pData, pArray->elemSize);
}

void taosArrayPopFrontBatch(SArray* pArray, size_t cnt) {
  assert(cnt <= pArray->size);
  pArray->size = pArray->size - cnt;
  if (pArray->size == 0 || cnt == 0) {
    return;
  }
  memmove(pArray->pData, (char*)pArray->pData + cnt * pArray->elemSize, pArray->size * pArray->elemSize);
}

void taosArrayPopTailBatch(SArray* pArray, size_t cnt) {
  assert(cnt <= pArray->size);
  pArray->size = pArray->size - cnt;
}

void taosArrayRemove(SArray* pArray, size_t index) {
  assert(index < pArray->size);

  if (index == pArray->size - 1) {
    taosArrayPop(pArray);
    return;
  }

  size_t remain = pArray->size - index - 1;
  memmove((char*)pArray->pData + index * pArray->elemSize, (char*)pArray->pData + (index + 1) * pArray->elemSize,
          remain * pArray->elemSize);
  pArray->size -= 1;
}

SArray* taosArrayFromList(const void* src, size_t size, size_t elemSize) {
  assert(src != NULL && elemSize > 0);
  SArray* pDst = taosArrayInit(size, elemSize);

  memcpy(pDst->pData, src, elemSize * size);
  pDst->size = size;

  return pDst;
}

SArray* taosArrayDup(const SArray* pSrc) {
  assert(pSrc != NULL);

  if (pSrc->size == 0) {  // empty array list
    return taosArrayInit(8, pSrc->elemSize);
  }

  SArray* dst = taosArrayInit(pSrc->size, pSrc->elemSize);

  memcpy(dst->pData, pSrc->pData, pSrc->elemSize * pSrc->size);
  dst->size = pSrc->size;
  return dst;
}

void taosArrayClear(SArray* pArray) {
  if (pArray == NULL) return;
  pArray->size = 0;
}

void taosArrayClearEx(SArray* pArray, void (*fp)(void*)) {
  if (pArray == NULL) return;
  if (fp == NULL) {
    pArray->size = 0;
    return;
  }

  for (int32_t i = 0; i < pArray->size; ++i) {
    fp(TARRAY_GET_ELEM(pArray, i));
  }

  pArray->size = 0;
}

void taosArrayClearP(SArray* pArray, FDelete fp) {
  if (pArray == NULL) return;
  if (fp == NULL) {
    pArray->size = 0;
    return;
  }

  for (int32_t i = 0; i < pArray->size; ++i) {
    fp(*(void**)TARRAY_GET_ELEM(pArray, i));
  }

  pArray->size = 0;
}

void* taosArrayDestroy(SArray* pArray) {
  if (pArray) {
    taosMemoryFree(pArray->pData);
    taosMemoryFree(pArray);
  }

  return NULL;
}

void taosArrayDestroyP(SArray* pArray, FDelete fp) {
  for (int32_t i = 0; i < pArray->size; i++) {
    fp(*(void**)TARRAY_GET_ELEM(pArray, i));
  }
  taosArrayDestroy(pArray);
}

void taosArrayDestroyEx(SArray* pArray, FDelete fp) {
  if (pArray == NULL) {
    return;
  }

  if (fp == NULL) {
    taosArrayDestroy(pArray);
    return;
  }

  for (int32_t i = 0; i < pArray->size; ++i) {
    fp(TARRAY_GET_ELEM(pArray, i));
  }

  taosArrayDestroy(pArray);
}

void taosArraySort(SArray* pArray, __compar_fn_t compar) {
  assert(pArray != NULL);
  assert(compar != NULL);

  taosSort(pArray->pData, pArray->size, pArray->elemSize, compar);
}

void* taosArraySearch(const SArray* pArray, const void* key, __compar_fn_t comparFn, int32_t flags) {
  assert(pArray != NULL && comparFn != NULL);
  assert(key != NULL);

  return taosbsearch(key, pArray->pData, pArray->size, pArray->elemSize, comparFn, flags);
}

int32_t taosArraySearchIdx(const SArray* pArray, const void* key, __compar_fn_t comparFn, int32_t flags) {
  void* item = taosArraySearch(pArray, key, comparFn, flags);
  return item == NULL ? -1 : (int32_t)((char*)item - (char*)pArray->pData) / pArray->elemSize;
}

void taosArraySortString(SArray* pArray, __compar_fn_t comparFn) {
  assert(pArray != NULL);
  taosSort(pArray->pData, pArray->size, pArray->elemSize, comparFn);
}

char* taosArraySearchString(const SArray* pArray, const char* key, __compar_fn_t comparFn, int32_t flags) {
  assert(pArray != NULL);
  assert(key != NULL);

  void* p = taosbsearch(&key, pArray->pData, pArray->size, pArray->elemSize, comparFn, flags);
  if (p == NULL) {
    return NULL;
  }
  return *(char**)p;
}

static int32_t taosArrayPartition(SArray* pArray, int32_t i, int32_t j, __ext_compar_fn_t fn, const void* userData) {
  void* key = taosArrayGetP(pArray, i);
  while (i < j) {
    while (i < j && fn(taosArrayGetP(pArray, j), key, userData) >= 0) {
      j--;
    }
    if (i < j) {
      void* a = taosArrayGetP(pArray, j);
      taosArraySet(pArray, i, &a);
    }
    while (i < j && fn(taosArrayGetP(pArray, i), key, userData) <= 0) {
      i++;
    }
    if (i < j) {
      void* a = taosArrayGetP(pArray, i);
      taosArraySet(pArray, j, &a);
    }
  }
  taosArraySet(pArray, i, &key);
  return i;
}

static void taosArrayQuicksortHelper(SArray* pArray, int32_t low, int32_t high, __ext_compar_fn_t fn,
                                     const void* param) {
  if (low < high) {
    int32_t idx = taosArrayPartition(pArray, low, high, fn, param);
    taosArrayQuicksortHelper(pArray, low, idx - 1, fn, param);
    taosArrayQuicksortHelper(pArray, idx + 1, high, fn, param);
  }
}

static void taosArrayQuickSort(SArray* pArray, __ext_compar_fn_t fn, const void* param) {
  if (pArray->size <= 1) {
    return;
  }
  taosArrayQuicksortHelper(pArray, 0, (int32_t)(taosArrayGetSize(pArray) - 1), fn, param);
}

static void taosArrayInsertSort(SArray* pArray, __ext_compar_fn_t fn, const void* param) {
  if (pArray->size <= 1) {
    return;
  }
  for (int32_t i = 1; i <= pArray->size - 1; ++i) {
    for (int32_t j = i; j > 0; --j) {
      if (fn(taosArrayGetP(pArray, j), taosArrayGetP(pArray, j - 1), param) == -1) {
        void* a = taosArrayGetP(pArray, j);
        void* b = taosArrayGetP(pArray, j - 1);
        taosArraySet(pArray, j - 1, &a);
        taosArraySet(pArray, j, &b);
      } else {
        break;
      }
    }
  }
  return;
}

SArray* taosArrayDeepCopy(const SArray* pSrc, FCopy deepCopy) {
  if (NULL == pSrc) {
    return NULL;
  }
  ASSERT(pSrc->elemSize == sizeof(void*));
  SArray* pArray = taosArrayInit(pSrc->size, sizeof(void*));
  for (int32_t i = 0; i < pSrc->size; i++) {
    void* clone = deepCopy(taosArrayGetP(pSrc, i));
    taosArrayPush(pArray, &clone);
  }
  return pArray;
}

int32_t taosEncodeArray(void** buf, const SArray* pArray, FEncode encode) {
  int32_t tlen = 0;
  int32_t sz = pArray->size;
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    void* data = taosArrayGetP(pArray, i);
    tlen += encode(buf, data);
  }
  return tlen;
}

void* taosDecodeArray(const void* buf, SArray** pArray, FDecode decode, int32_t dataSz) {
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  *pArray = taosArrayInit(sz, sizeof(void*));
  for (int32_t i = 0; i < sz; i++) {
    void* data = taosMemoryCalloc(1, dataSz);
    buf = decode(buf, data);
    taosArrayPush(*pArray, &data);
  }
  return (void*)buf;
}

// todo remove it
// order array<type *>
void taosArraySortPWithExt(SArray* pArray, __ext_compar_fn_t fn, const void* param) {
  taosArrayGetSize(pArray) > 8 ? taosArrayQuickSort(pArray, fn, param) : taosArrayInsertSort(pArray, fn, param);
}
// TODO(yihaoDeng) add order array<type>
//

char* taosShowStrArray(const SArray* pArray) {
  int32_t sz = pArray->size;
  int32_t tlen = 0;
  for (int32_t i = 0; i < sz; i++) {
    tlen += strlen(taosArrayGetP(pArray, i)) + 1;
  }
  char* res = taosMemoryCalloc(1, tlen);
  char* buf = res;
  for (int32_t i = 0; i < sz; i++) {
    char*   str = taosArrayGetP(pArray, i);
    int32_t len = strlen(str);
    memcpy(buf, str, len);
    buf += len;
    if (i != sz - 1) *buf = ',';
  }
  return res;
}

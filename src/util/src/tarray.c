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

#include "tarray.h"

void* taosArrayInit(size_t size, size_t elemSize) {
  assert(elemSize > 0);

  if (size < TARRAY_MIN_SIZE) {
    size = TARRAY_MIN_SIZE;
  }

  SArray* pArray = calloc(1, sizeof(SArray));
  if (pArray == NULL) {
    return NULL;
  }

  pArray->pData = calloc(size, elemSize);
  if (pArray->pData == NULL) {
    free(pArray);
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

  void* tmp = realloc(pArray->pData, size * pArray->elemSize);
  if (tmp == NULL) {  // reallocate failed, the original buffer remains
    return -1;
  }

  pArray->pData = tmp;
  pArray->capacity = size;
  
  return 0;
}

void* taosArrayPush(SArray* pArray, void* pData) {
  if (pArray == NULL || pData == NULL) {
    return NULL;
  }

  if (pArray->size >= pArray->capacity) {
    int32_t ret = taosArrayResize(pArray);
    
    // failed to push data into buffer due to the failure of memory allocation
    if (ret != 0) {
      return NULL;
    }
  }

  void* dst = TARRAY_GET_ELEM(pArray, pArray->size);
  memcpy(dst, pData, pArray->elemSize);

  pArray->size += 1;
  return dst;
}

void* taosArrayPop(SArray* pArray) {
  assert( pArray != NULL );

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

void* taosArrayGetLast(const SArray* pArray) {
  return TARRAY_GET_ELEM(pArray, pArray->size - 1);
}

size_t taosArrayGetSize(const SArray* pArray) { return pArray->size; }

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

void taosArrayRemove(SArray* pArray, size_t index) {
  assert(index < pArray->size);
  
  if (index == pArray->size - 1) {
    taosArrayPop(pArray);
    return;
  }
  
  size_t remain = pArray->size - index - 1;
  memmove((char*)pArray->pData + index * pArray->elemSize, (char*)pArray->pData + (index + 1) * pArray->elemSize, remain * pArray->elemSize);
  pArray->size -= 1;
}

void taosArrayCopy(SArray* pDst, const SArray* pSrc) {
  assert(pSrc != NULL && pDst != NULL);
  
  if (pDst->capacity < pSrc->size) {
    void* pData = realloc(pDst->pData, pSrc->size * pSrc->elemSize);
    if (pData == NULL) { // todo handle oom
    
    } else {
      pDst->pData = pData;
      pDst->capacity = pSrc->size;
    }
  }
  
  memcpy(pDst->pData, pSrc->pData, pSrc->elemSize * pSrc->size);
  pDst->elemSize = pSrc->elemSize;
  pDst->capacity = pSrc->size;
  pDst->size = pSrc->size;
}

SArray* taosArrayClone(const SArray* pSrc) {
  assert(pSrc != NULL);
  
  if (pSrc->size == 0) { // empty array list
    return taosArrayInit(8, pSrc->elemSize);
  }
  
  SArray* dst = taosArrayInit(pSrc->size, pSrc->elemSize);
  
  memcpy(dst->pData, pSrc->pData, pSrc->elemSize * pSrc->size);
  dst->size = pSrc->size;
  return dst;
}

void taosArrayClear(SArray* pArray) {
  assert( pArray != NULL );
  pArray->size = 0;
}

void taosArrayDestroy(SArray* pArray) {
  if (pArray == NULL) {
    return;
  }

  free(pArray->pData);
  free(pArray);
}

void taosArrayDestroyEx(SArray* pArray, void (*fp)(void*)) {
  if (pArray == NULL) {
    return;
  }

  if (fp == NULL) {
    return taosArrayDestroy(pArray);
  }

  for(int32_t i = 0; i < pArray->size; ++i) {
    fp(TARRAY_GET_ELEM(pArray, i));
  }

  taosArrayDestroy(pArray);
}

void taosArraySort(SArray* pArray, int (*compar)(const void*, const void*)) {
  assert(pArray != NULL);
  assert(compar != NULL);

  qsort(pArray->pData, pArray->size, pArray->elemSize, compar);
}

void* taosArraySearch(const SArray* pArray, const void* key, __compar_fn_t comparFn) {
  assert(pArray != NULL && comparFn != NULL);
  assert(key != NULL);

  return bsearch(key, pArray->pData, pArray->size, pArray->elemSize, comparFn);
}

void taosArraySortString(SArray* pArray, __compar_fn_t comparFn) {
  assert(pArray != NULL);
  qsort(pArray->pData, pArray->size, pArray->elemSize, comparFn);
}

char* taosArraySearchString(const SArray* pArray, const char* key, __compar_fn_t comparFn) {
  assert(pArray != NULL);
  assert(key != NULL);

  void* p = bsearch(&key, pArray->pData, pArray->size, pArray->elemSize, comparFn);
  if (p == NULL) {
    return NULL;
  }
  return *(char**)p;
}
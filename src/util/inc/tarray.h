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

#ifndef TDENGINE_TAOSARRAY_H
#define TDENGINE_TAOSARRAY_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "talgo.h"

#define TARRAY_MIN_SIZE 8
#define TARRAY_GET_ELEM(array, index) ((void*)((char*)((array)->pData) + (index) * (array)->elemSize))
#define TARRAY_ELEM_IDX(array, ele) (POINTER_DISTANCE(ele, (array)->pData) / (array)->elemSize)

typedef struct SArray {
  size_t size;
  size_t capacity;
  size_t elemSize;
  void*  pData;
} SArray;

/**
 *
 * @param size
 * @param elemSize
 * @return
 */
void* taosArrayInit(size_t size, size_t elemSize);

/**
 *
 * @param pArray
 * @param pData
 * @param nEles
 * @return
 */
void *taosArrayPushBatch(SArray *pArray, const void *pData, int nEles);

/**
 *
 * @param pArray
 * @param pData
 * @return
 */
static FORCE_INLINE void* taosArrayPush(SArray* pArray, const void* pData) {
  return taosArrayPushBatch(pArray, pData, 1);
}

/**
 *
 * @param pArray
 */
void* taosArrayPop(SArray* pArray);

/**
 * get the data from array
 * @param pArray
 * @param index
 * @return
 */
void* taosArrayGet(const SArray* pArray, size_t index);

/**
 * get the pointer data from the array
 * @param pArray
 * @param index
 * @return
 */
void* taosArrayGetP(const SArray* pArray, size_t index);

/**
 * get the last element in the array list
 * @param pArray
 * @return
 */
void* taosArrayGetLast(const SArray* pArray);

/**
 * return the size of array
 * @param pArray
 * @return
 */
size_t taosArrayGetSize(const SArray* pArray);

/**
 * insert data into array
 * @param pArray
 * @param index
 * @param pData
 */
void* taosArrayInsert(SArray* pArray, size_t index, void* pData);

/**
 * set data in array
 * @param pArray
 * @param index
 * @param pData
 */
void taosArraySet(SArray* pArray, size_t index, void* pData);

/**
 * remove data entry of the given index
 * @param pArray
 * @param index
 */
void taosArrayRemove(SArray* pArray, size_t index);

/**
 * copy the whole array from source to destination
 * @param pDst
 * @param pSrc
 */
SArray* taosArrayFromList(const void* src, size_t size, size_t elemSize);

/**
 * clone a new array
 * @param pSrc
 */
SArray* taosArrayDup(const SArray* pSrc);

/**
 * clear the array (remove all element)
 * @param pArray
 */
void taosArrayClear(SArray* pArray);

/**
 * destroy array list
 * @param pArray
 */
void* taosArrayDestroy(SArray* pArray);

/**
 *
 * @param pArray
 * @param fp
 */
void taosArrayDestroyEx(SArray* pArray, void (*fp)(void*));

/**
 * sort the array
 * @param pArray
 * @param compar
 */
void taosArraySort(SArray* pArray, __compar_fn_t comparFn);

/**
 * sort string array
 * @param pArray
 */
void taosArraySortString(SArray* pArray, __compar_fn_t comparFn);

/**
 * search the array
 * @param pArray
 * @param compar
 * @param key
 */
void* taosArraySearch(const SArray* pArray, const void* key, __compar_fn_t comparFn, int flags);

/**
 * search the array
 * @param pArray
 * @param key
 */
char* taosArraySearchString(const SArray* pArray, const char* key, __compar_fn_t comparFn, int flags);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TAOSARRAY_H

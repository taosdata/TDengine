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

#ifndef _TD_UTIL_ARRAY_H_
#define _TD_UTIL_ARRAY_H_

#include "talgo.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TARRAY_MIN_SIZE               4
#define TARRAY_GET_ELEM(array, index) ((void*)((char*)((array)->pData) + (index) * (array)->elemSize))
#define TARRAY_ELEM_IDX(array, ele)   (POINTER_DISTANCE(ele, (array)->pData) / (array)->elemSize)

typedef struct SArray {
  size_t   size;
  uint32_t capacity;
  uint32_t elemSize;
  void*    pData;
} SArray;

#define TARRAY_SIZE(array) ((array)->size)
#define TARRAY_DATA(array) ((array)->pData)

/**
 *
 * @param size
 * @param elemSize
 * @return
 */
SArray* taosArrayInit(size_t size, size_t elemSize);
SArray* taosArrayInit_s(size_t elemSize, size_t initialSize);

/**
 *
 * @param tsize
 * @return
 */
int32_t taosArrayEnsureCap(SArray* pArray, size_t tsize);

/**
 *
 * @param pArray
 * @param pData
 * @param nEles
 * @return
 */
void* taosArrayAddBatch(SArray* pArray, const void* pData, int32_t nEles);

/**
 *
 * @param pArray
 * @param comparFn
 * @param fp
 */
void taosArrayRemoveDuplicate(SArray* pArray, __compar_fn_t comparFn, void (*fp)(void*));

/**
 *  add all element from the source array list into the destination
 * @param pArray
 * @param pInput
 * @return
 */
void* taosArrayAddAll(SArray* pArray, const SArray* pInput);

/**
 *
 * @param pArray
 * @param pData
 * @return
 */
static FORCE_INLINE void* taosArrayPush(SArray* pArray, const void* pData) {
  return taosArrayAddBatch(pArray, pData, 1);
}

/**
 * @brief reserve the capacity of the array
 *
 * @param pArray
 * @param num
 * @return void* the start position of the reserved memory
 */
void* taosArrayReserve(SArray* pArray, int32_t num);

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
void* taosArrayInsert(SArray* pArray, size_t index, const void* pData);

/**
 * set data in array
 * @param pArray
 * @param index
 * @param pData
 */
void taosArraySet(SArray* pArray, size_t index, void* pData);

/**
 * remove some data entry from front
 * @param pArray
 * @param cnt
 */
void taosArrayPopFrontBatch(SArray* pArray, size_t cnt);

/**
 * remove some data entry from front
 * @param pArray
 * @param cnt
 */
void taosArrayPopTailBatch(SArray* pArray, size_t cnt);

/**
 * remove data entry of the given index
 * @param pArray
 * @param index
 */
void taosArrayRemove(SArray* pArray, size_t index);

/**
 * remove batch entry from the given index
 * @param pArray
 * @param index
 */
void taosArrayRemoveBatch(SArray* pArray, size_t index, size_t num, FDelete fp);

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
SArray* taosArrayDup(const SArray* pSrc, __array_item_dup_fn_t fn);

/**
 * clear the array (remove all element)
 * @param pArray
 */
void taosArrayClear(SArray* pArray);

/**
 * clear the array (remove all element)
 * @param pArray
 * @param fp
 */

void taosArrayClearEx(SArray* pArray, void (*fp)(void*));

void taosArrayClearP(SArray* pArray, void (*fp)(void*));

void* taosArrayDestroy(SArray* pArray);

void taosArrayDestroyP(SArray* pArray, FDelete fp);

void taosArrayDestroyEx(SArray* pArray, FDelete fp);

void taosArraySwap(SArray* a, SArray* b);

/**
 * sort the array use qsort
 * @param pArray
 * @param compar
 */
void taosArraySort(SArray* pArray, __compar_fn_t comparFn);

/**
 * sort the array use merge sort
 * @param pArray
 * @param compar
 */
int32_t taosArrayMSort(SArray* pArray, __compar_fn_t comparFn);

/**
 * search the array
 * @param pArray
 * @param compar
 * @param key
 */
void* taosArraySearch(const SArray* pArray, const void* key, __compar_fn_t comparFn, int32_t flags);

/**
 * search the array, return index of the element
 * @param pArray
 * @param compar
 * @param key
 */
int32_t taosArraySearchIdx(const SArray* pArray, const void* key, __compar_fn_t comparFn, int32_t flags);

/**
 * sort the pointer data in the array
 * @param pArray
 * @param compar
 * @param param
 * @return
 */

void taosArraySortPWithExt(SArray* pArray, __ext_compar_fn_t fn, const void* param);

int32_t taosEncodeArray(void** buf, const SArray* pArray, FEncode encode);
void*   taosDecodeArray(const void* buf, SArray** pArray, FDecode decode, int32_t dataSz, int8_t sver);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_ARRAY_H_*/

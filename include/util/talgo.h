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

#ifndef _TD_UTIL_TALGO_H_
#define _TD_UTIL_TALGO_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef __COMPAR_FN_T
#define __COMPAR_FN_T
typedef int32_t (*__compar_fn_t)(const void *, const void *);
#endif

typedef void *(*__array_item_dup_fn_t)(void *);

typedef void (*FDelete)(void *);
typedef int32_t (*FEncode)(void **buf, const void *dst);
typedef void *(*FDecode)(const void *buf, void *dst, int8_t sver);

#define TD_EQ 0x1
#define TD_GT 0x2
#define TD_LT 0x4
#define TD_GE (TD_EQ | TD_GT)
#define TD_LE (TD_EQ | TD_LT)

#define elePtrAt(base, size, idx) (void *)((char *)(base) + (size) * (idx))

typedef int32_t (*__ext_compar_fn_t)(const void *p1, const void *p2, const void *param);

/**
 * quick sort, with the compare function requiring additional parameters support
 *
 * @param src
 * @param numOfElem
 * @param size
 * @param param
 * @param comparFn
 */
void taosqsort(void *src, int64_t numOfElem, int64_t size, const void *param, __ext_compar_fn_t comparFn);

/**
 * merge sort, with the compare function requiring additional parameters support
 *
 * @param src
 * @param numOfElem
 * @param size
 * @param comparFn
 * @return  int32_t 0 for success, other for failure.
 */
int32_t taosMergeSort(void *src, int64_t numOfElem, int64_t size, __compar_fn_t comparFn);

/**
 * binary search, with range support
 *
 * @param key
 * @param base
 * @param nmemb
 * @param size
 * @param fn
 * @param flags
 * @return
 */
void *taosbsearch(const void *key, const void *base, int32_t nmemb, int32_t size, __compar_fn_t compar, int32_t flags);

/**
 * adjust heap
 *
 * @param base: the start address of array
 * @param size: size of every item in array
 * @param start: the first index
 * @param end: the last index
 * @param parcompar: parameters for compare function
 * @param compar: user defined compare function
 * @param parswap: parameters for swap function
 * @param swap: user defined swap function, the default swap function doswap will be used if swap is NULL
 * @param maxroot: if heap is max root heap
 * @return
 */
void taosheapadjust(void *base, int32_t size, int32_t start, int32_t end, const void *parcompar,
                    __ext_compar_fn_t compar, char *buf, bool maxroot);

/**
 * sort heap to make sure it is a max/min root heap
 *
 * @param base: the start address of array
 * @param size: size of every item in array
 * @param len: the length of array
 * @param parcompar: parameters for compare function
 * @param compar: user defined compare function
 * @param parswap: parameters for swap function
 * @param swap: user defined swap function, the default swap function doswap will be used if swap is NULL
 * @param maxroot: if heap is max root heap
 * @return
 */
void taosheapsort(void *base, int32_t size, int32_t len, const void *parcompar, __ext_compar_fn_t compar, bool maxroot);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_TALGO_H_*/

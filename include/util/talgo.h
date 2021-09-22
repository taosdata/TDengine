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

#ifndef TDENGINE_TALGO_H
#define TDENGINE_TALGO_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef __COMPAR_FN_T
# define __COMPAR_FN_T
typedef int (*__compar_fn_t) (const void *, const void *);
#endif

#define TD_EQ 0x1
#define TD_GT 0x2
#define TD_LT 0x4
#define TD_GE (TD_EQ | TD_GT)
#define TD_LE (TD_EQ | TD_LT)

#define elePtrAt(base, size, idx) (void *)((char *)(base) + (size) * (idx))

typedef int32_t (*__ext_compar_fn_t)(const void *p1, const void *p2, const void *param);
typedef void (*__ext_swap_fn_t)(void *p1, void *p2, const void *param);

/**
 * quick sort, with the compare function requiring additional parameters support
 *
 * @param src
 * @param numOfElem
 * @param size
 * @param param
 * @param comparFn
 */
void taosqsort(void *src, size_t numOfElem, size_t size, const void* param, __ext_compar_fn_t comparFn);

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
void *taosbsearch(const void *key, const void *base, size_t nmemb, size_t size, __compar_fn_t fn, int flags);

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
void taosheapadjust(void *base, int32_t size, int32_t start, int32_t end, const void *parcompar, __ext_compar_fn_t compar, const void *parswap, __ext_swap_fn_t swap, bool maxroot);

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
void taosheapsort(void *base, int32_t size, int32_t len, const void *parcompar, __ext_compar_fn_t compar, const void *parswap, __ext_swap_fn_t swap, bool maxroot);


#ifdef __cplusplus
}
#endif
#endif  // TDENGINE_TALGO_H

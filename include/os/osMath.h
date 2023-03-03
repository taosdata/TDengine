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

#ifndef _TD_OS_MATH_H_
#define _TD_OS_MATH_H_

#ifdef __cplusplus
extern "C" {
#endif

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following sectio
#ifndef ALLOW_FORBID_FUNC
#define qsort QSORT_FUNC_TAOS_FORBID
#endif

#define TPOW2(x) ((x) * (x))
#define TABS(x)  ((x) > 0 ? (x) : -(x))

#define TSWAP(a, b)                  \
  do {                               \
    char *__tmp = alloca(sizeof(a)); \
    memcpy(__tmp, &(a), sizeof(a));  \
    memcpy(&(a), &(b), sizeof(a));   \
    memcpy(&(b), __tmp, sizeof(a));  \
  } while (0)

#ifdef WINDOWS

#define TMAX(a, b)         (((a) > (b)) ? (a) : (b))
#define TMIN(a, b)         (((a) < (b)) ? (a) : (b))
#define TRANGE(aa, bb, cc) ((aa) = TMAX((aa), (bb)), (aa) = TMIN((aa), (cc)))

#else

#define TMAX(a, b)           \
  ({                         \
    __typeof(a) __a = (a);   \
    __typeof(b) __b = (b);   \
    (__a > __b) ? __a : __b; \
  })

#define TMIN(a, b)           \
  ({                         \
    __typeof(a) __a = (a);   \
    __typeof(b) __b = (b);   \
    (__a < __b) ? __a : __b; \
  })

#define TRANGE(a, b, c) \
  ({                    \
    a = TMAX(a, b);     \
    a = TMIN(a, c);     \
  })
#endif

#ifndef __COMPAR_FN_T
#define __COMPAR_FN_T
typedef int32_t (*__compar_fn_t)(const void *, const void *);
#endif

void taosSort(void *arr, int64_t sz, int64_t width, __compar_fn_t compar);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_MATH_H_*/

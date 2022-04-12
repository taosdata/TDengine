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

#define TPOW2(x) ((x) * (x))
#define TABS(x) ((x) > 0 ? (x) : -(x))

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

  #define TSWAP(a, b, c) \
    do {                \
      c __tmp = (c)(a); \
      (a) = (c)(b);     \
      (b) = __tmp;      \
    } while (0)
  #define TMAX(a, b) (((a) > (b)) ? (a) : (b))
  #define TMIN(a, b) (((a) < (b)) ? (a) : (b))

#else

  #define TSWAP(a, b, c)       \
    do {                       \
      __typeof(a) __tmp = (a); \
      (a) = (b);               \
      (b) = __tmp;             \
    } while (0)

  #define TMAX(a, b)             \
    ({                           \
      __typeof(a) __a = (a);     \
      __typeof(b) __b = (b);     \
      (__a > __b) ? __a : __b;   \
    })

  #define TMIN(a, b)             \
    ({                           \
      __typeof(a) __a = (a);     \
      __typeof(b) __b = (b);     \
      (__a < __b) ? __a : __b;   \
    })

#define TRANGE(a, b, c) \
  ({                    \
    a = TMAX(a, b);     \
    a = TMIN(a, c);     \
  })
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_MATH_H_*/

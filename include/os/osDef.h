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

#ifndef _TD_OS_DEF_H_
#define _TD_OS_DEF_H_

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__GNUC__)
#define FORCE_INLINE inline __attribute__((always_inline))
#else
#define FORCE_INLINE
#endif

#define POINTER_SHIFT(p, b) ((void *)((char *)(p) + (b)))
#define POINTER_DISTANCE(p1, p2) ((char *)(p1) - (char *)(p2)) 

#if defined(_TD_LINUX_64) || defined(_TD_LINUX_32) || defined(_TD_MIPS_64)  || defined(_TD_ARM_32) || defined(_TD_ARM_64)  || defined(_TD_DARWIN_64)
  #if defined(_TD_DARWIN_64)
    // MacOS
    #if !defined(_GNU_SOURCE)
      #define setThreadName(name) do { pthread_setname_np((name)); } while (0)
    #else
      // pthread_setname_np not defined
      #define setThreadName(name)
    #endif
  #else
    // Linux, length of name must <= 16 (the last '\0' included)
    #define setThreadName(name) do { prctl(PR_SET_NAME, (name)); } while (0)
  #endif
#else
  // Windows
  #define setThreadName(name)
#endif



// TODO: replace and remove code below
#define CHAR_BYTES    sizeof(char)
#define SHORT_BYTES   sizeof(int16_t)
#define INT_BYTES     sizeof(int32_t)
#define LONG_BYTES    sizeof(int64_t)
#define FLOAT_BYTES   sizeof(float)
#define DOUBLE_BYTES  sizeof(double)
#define POINTER_BYTES sizeof(void *)  // 8 by default  assert(sizeof(ptrdiff_t) == sizseof(void*)

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_DEF_H_*/
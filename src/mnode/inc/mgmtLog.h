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

#ifndef TDENGINE_MGMT_LOG_H
#define TDENGINE_MGMT_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

// mnode log function
#define mError(...)                          \
  if (mdebugFlag & DEBUG_ERROR) {            \
    tprintf("ERROR MND ", 255, __VA_ARGS__); \
  }
#define mWarn(...)                                  \
  if (mdebugFlag & DEBUG_WARN) {                    \
    tprintf("WARN  MND ", mdebugFlag, __VA_ARGS__); \
  }
#define mTrace(...)                           \
  if (mdebugFlag & DEBUG_TRACE) {             \
    tprintf("MND ", mdebugFlag, __VA_ARGS__); \
  }
#define mPrint(...) \
  { tprintf("MND ", 255, __VA_ARGS__); }

#define mLError(...) taosLogError(__VA_ARGS__) mError(__VA_ARGS__)
#define mLWarn(...) taosLogWarn(__VA_ARGS__) mWarn(__VA_ARGS__)
#define mLPrint(...) taosLogPrint(__VA_ARGS__) mPrint(__VA_ARGS__)

#define sdbError(...)                            \
  if (sdbDebugFlag & DEBUG_ERROR) {              \
    tprintf("ERROR MND-SDB ", 255, __VA_ARGS__); \
  }
#define sdbWarn(...)                                      \
  if (sdbDebugFlag & DEBUG_WARN) {                        \
    tprintf("WARN  MND-SDB ", sdbDebugFlag, __VA_ARGS__); \
  }
#define sdbTrace(...)                               \
  if (sdbDebugFlag & DEBUG_TRACE) {                 \
    tprintf("MND-SDB ", sdbDebugFlag, __VA_ARGS__); \
  }
#define sdbPrint(...) \
  { tprintf("MND-SDB ", 255, __VA_ARGS__); }

#define sdbLError(...) taosLogError(__VA_ARGS__) sdbError(__VA_ARGS__)
#define sdbLWarn(...) taosLogWarn(__VA_ARGS__) sdbWarn(__VA_ARGS__)
#define sdbLPrint(...) taosLogPrint(__VA_ARGS__) sdbPrint(__VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif

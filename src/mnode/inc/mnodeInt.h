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

#ifndef TDENGINE_MNODE_LOG_H
#define TDENGINE_MNODE_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"
#include "monitor.h"

extern int32_t mDebugFlag;
extern int32_t sdbDebugFlag;

// mnode log function
#define mError(fmt, ...) { if (mDebugFlag & DEBUG_ERROR) { TLOG("ERROR MND ", 255, fmt, ##__VA_ARGS__); }}
#define mWarn(fmt, ...)  { if (mDebugFlag & DEBUG_WARN)  { TLOG("WARN MND ", mDebugFlag, fmt, ##__VA_ARGS__); }}
#define mTrace(fmt, ...) { if (mDebugFlag & DEBUG_TRACE) { TLOG("MND ", mDebugFlag, fmt, ##__VA_ARGS__); }}
#define mPrint(fmt, ...) { TLOG("MND ", 255, fmt, ##__VA_ARGS__); }

#define mLError(fmt, ...) { monitorSaveLog(2, fmt, ##__VA_ARGS__); mError(fmt, ##__VA_ARGS__) }
#define mLWarn(fmt, ...)  { monitorSaveLog(1, fmt, ##__VA_ARGS__); mWarn(fmt, ##__VA_ARGS__)  }
#define mLPrint(fmt, ...) { monitorSaveLog(0, fmt, ##__VA_ARGS__); mPrint(fmt, ##__VA_ARGS__) }

#define sdbError(fmt, ...) { if (sdbDebugFlag & DEBUG_ERROR) { TLOG("ERROR MND-SDB ", 255, fmt, ##__VA_ARGS__); }}
#define sdbWarn(fmt, ...)  { if (sdbDebugFlag & DEBUG_WARN)  { TLOG("WARN MND-SDB ", sdbDebugFlag, fmt, ##__VA_ARGS__); }}
#define sdbTrace(fmt, ...) { if (sdbDebugFlag & DEBUG_TRACE) { TLOG("MND-SDB ", sdbDebugFlag, fmt, ##__VA_ARGS__);}}
#define sdbPrint(fmt, ...) { TLOG("MND-SDB ", 255, fmt, ##__VA_ARGS__); }

#define sdbLError(fmt, ...) { monitorSaveLog(2, fmt, ##__VA_ARGS__); sdbError(fmt, ##__VA_ARGS__) }
#define sdbLWarn(fmt, ...)  { monitorSaveLog(1, fmt, ##__VA_ARGS__); sdbWarn(fmt, ##__VA_ARGS__)  }
#define sdbLPrint(fmt, ...) { monitorSaveLog(0, fmt, ##__VA_ARGS__); sdbPrint(fmt, ##__VA_ARGS__) }

#ifdef __cplusplus
}
#endif

#endif

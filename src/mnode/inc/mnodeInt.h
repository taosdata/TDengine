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
#define mFatal(...) { if (mDebugFlag & DEBUG_FATAL) { taosPrintLog("MND FATAL ", 255, __VA_ARGS__); }}
#define mError(...) { if (mDebugFlag & DEBUG_ERROR) { taosPrintLog("MND ERROR ", 255, __VA_ARGS__); }}
#define mWarn(...)  { if (mDebugFlag & DEBUG_WARN)  { taosPrintLog("MND WARN ", 255, __VA_ARGS__); }}
#define mInfo(...)  { if (mDebugFlag & DEBUG_INFO)  { taosPrintLog("MND ", 255, __VA_ARGS__); }}
#define mDebug(...) { if (mDebugFlag & DEBUG_DEBUG) { taosPrintLog("MND ", mDebugFlag, __VA_ARGS__); }}
#define mTrace(...) { if (mDebugFlag & DEBUG_TRACE) { taosPrintLog("MND ", mDebugFlag, __VA_ARGS__); }}

#define sdbFatal(...) { if (sdbDebugFlag & DEBUG_FATAL) { taosPrintLog("SDB FATAL ", 255, __VA_ARGS__); }}
#define sdbError(...) { if (sdbDebugFlag & DEBUG_ERROR) { taosPrintLog("SDB ERROR ", 255, __VA_ARGS__); }}
#define sdbWarn(...)  { if (sdbDebugFlag & DEBUG_WARN)  { taosPrintLog("SDB WARN ", 255, __VA_ARGS__); }}
#define sdbInfo(...)  { if (sdbDebugFlag & DEBUG_INFO)  { taosPrintLog("SDB ", 255, __VA_ARGS__); }}
#define sdbDebug(...) { if (sdbDebugFlag & DEBUG_DEBUG) { taosPrintLog("SDB ", sdbDebugFlag, __VA_ARGS__); }}
#define sdbTrace(...) { if (sdbDebugFlag & DEBUG_TRACE) { taosPrintLog("SDB ", sdbDebugFlag, __VA_ARGS__); }}

#define mLError(...) { monSaveLog(2, __VA_ARGS__); mError(__VA_ARGS__) }
#define mLWarn(...)  { monSaveLog(1, __VA_ARGS__); mWarn(__VA_ARGS__)  }
#define mLInfo(...)  { monSaveLog(0, __VA_ARGS__); mInfo(__VA_ARGS__) }

#ifdef __cplusplus
}
#endif

#endif

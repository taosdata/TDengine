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

#ifndef _TD_AZ_INT_H_
#define _TD_AZ_INT_H_

#include "os.h"
#include "tarray.h"
#include "tdef.h"
#include "tlog.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define azFatal(...) { if (azDebugFlag & DEBUG_FATAL) { taosPrintLog("AZR FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }}
#define azError(...) { if (azDebugFlag & DEBUG_ERROR) { taosPrintLog("AZR ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }}
#define azWarn(...)  { if (azDebugFlag & DEBUG_WARN)  { taosPrintLog("AZR WARN ",  DEBUG_WARN, 255,         __VA_ARGS__); }}
#define azInfo(...)  { if (azDebugFlag & DEBUG_INFO)  { taosPrintLog("AZR ",       DEBUG_INFO, 255,         __VA_ARGS__); }}
#define azDebug(...) { if (azDebugFlag & DEBUG_DEBUG) { taosPrintLog("AZR ",       DEBUG_DEBUG, azDebugFlag, __VA_ARGS__); }}
#define azTrace(...) { if (azDebugFlag & DEBUG_TRACE) { taosPrintLog("AZR ",       DEBUG_TRACE, azDebugFlag, __VA_ARGS__); }}
// clang-format on

#ifdef __cplusplus
}
#endif

#endif  // _TD_AZ_INT_H_

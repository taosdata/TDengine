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

#ifndef _TD_TSS_INT_H_
#define _TD_TSS_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taoserror.h"
#include "tlog.h"
#include "tss.h"

// For debug purpose
// clang-format off
#define tssFatal(...) { if (tssDebugFlag & DEBUG_FATAL) { taosPrintLog("TSS FATAL ", DEBUG_FATAL, 255,         __VA_ARGS__); }}
#define tssError(...) { if (tssDebugFlag & DEBUG_ERROR) { taosPrintLog("TSS ERROR ", DEBUG_ERROR, 255,         __VA_ARGS__); }}
#define tssWarn(...)  { if (tssDebugFlag & DEBUG_WARN)  { taosPrintLog("TSS WARN  ", DEBUG_WARN,  255,         __VA_ARGS__); }}
#define tssInfo(...)  { if (tssDebugFlag & DEBUG_INFO)  { taosPrintLog("TSS INFO  ", DEBUG_INFO,  255,         __VA_ARGS__); }}
#define tssDebug(...) { if (tssDebugFlag & DEBUG_DEBUG) { taosPrintLog("TSS DEBUG ", DEBUG_DEBUG, fsDebugFlag, __VA_ARGS__); }}
#define tssTrace(...) { if (tssDebugFlag & DEBUG_TRACE) { taosPrintLog("TSS TRACE ", DEBUG_TRACE, fsDebugFlag, __VA_ARGS__); }}
// clang-format on


#define countof(a) (sizeof(a) / sizeof((a)[0]))


#ifdef __cplusplus
}
#endif

#endif /*_TD_TSS_INT_H_*/


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

#ifndef TDENGINE_CLIENTLOG_H
#define TDENGINE_CLIENTLOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

// clang-format off
#define tscFatal(...)  do { if (cDebugFlag & DEBUG_FATAL) { taosPrintLog("TSC FATAL ", DEBUG_FATAL, cDebugFlag, __VA_ARGS__); }} while(0)
#define tscError(...)  do { if (cDebugFlag & DEBUG_ERROR) { taosPrintLog("TSC ERROR ", DEBUG_ERROR, cDebugFlag, __VA_ARGS__); }} while(0)
#define tscErrorL(...) do { if (cDebugFlag & DEBUG_ERROR) { taosPrintLongString("TSC ERROR ", DEBUG_ERROR, cDebugFlag, __VA_ARGS__); }} while(0)
#define tscWarn(...)   do { if (cDebugFlag & DEBUG_WARN)  { taosPrintLog("TSC WARN ", DEBUG_WARN, cDebugFlag, __VA_ARGS__); }}  while(0)
#define tscWarnL(...)  do { if (cDebugFlag & DEBUG_WARN)  { taosPrintLongString("TSC WARN ", DEBUG_WARN, cDebugFlag, __VA_ARGS__); }}  while(0)
#define tscInfo(...)   do { if (cDebugFlag & DEBUG_INFO)  { taosPrintLog("TSC ", DEBUG_INFO, cDebugFlag, __VA_ARGS__); }} while(0)
#define tscDebug(...)  do { if (cDebugFlag & DEBUG_DEBUG) { taosPrintLog("TSC ", DEBUG_DEBUG, cDebugFlag, __VA_ARGS__); }} while(0)
#define tscTrace(...)  do { if (cDebugFlag & DEBUG_TRACE) { taosPrintLog("TSC ", DEBUG_TRACE, cDebugFlag, __VA_ARGS__); }} while(0)
#define tscDebugL(...) do { if (cDebugFlag & DEBUG_DEBUG) { taosPrintLongString("TSC ", DEBUG_DEBUG, cDebugFlag, __VA_ARGS__); }} while(0)
#define tscPerf(...)   do { if (cDebugFlag & DEBUG_INFO)  { taosPrintLog("TSC ", 0, cDebugFlag, __VA_ARGS__); }} while(0)
#define tscLog(...)    do { taosPrintLog("TSC ", 0, DEBUG_FILE, __VA_ARGS__); } while(0)
#define tscLogL(...)   do { taosPrintLongString("TSC ", 0, DEBUG_FILE, __VA_ARGS__); } while(0)
// clang-format on

#ifdef __cplusplus
}
#endif

#endif

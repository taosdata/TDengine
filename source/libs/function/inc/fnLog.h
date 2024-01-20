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

#ifndef TDENGINE_FNLOG_H
#define TDENGINE_FNLOG_H
#include "tlog.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define fnFatal(...) { if (udfDebugFlag & DEBUG_FATAL) { taosPrintLog("UDF FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}
#define fnError(...) { if (udfDebugFlag & DEBUG_ERROR) { taosPrintLog("UDF ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}
#define fnWarn(...)  { if (udfDebugFlag & DEBUG_WARN)  { taosPrintLog("UDF WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}
#define fnInfo(...)  { if (udfDebugFlag & DEBUG_INFO)  { taosPrintLog("UDF ", DEBUG_INFO, 255, __VA_ARGS__); }}
#define fnDebug(...) { if (udfDebugFlag & DEBUG_DEBUG) { taosPrintLog("UDF ", DEBUG_DEBUG, udfDebugFlag, __VA_ARGS__); }}
#define fnTrace(...) { if (udfDebugFlag & DEBUG_TRACE) { taosPrintLog("UDF ", DEBUG_TRACE, udfDebugFlag, __VA_ARGS__); }}
// clang-format on

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FNLOG_H

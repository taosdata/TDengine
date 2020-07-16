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

#ifndef TDENGINE_TSC_LOG_H
#define TDENGINE_TSC_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

extern int32_t cDebugFlag;
extern int32_t tscEmbedded;

#define tscFatal(...) { if (cDebugFlag & DEBUG_FATAL) { taosPrintLog("TSC FATAL ", tscEmbedded ? 255 : cDebugFlag, __VA_ARGS__); }}
#define tscError(...) { if (cDebugFlag & DEBUG_ERROR) { taosPrintLog("TSC ERROR ", tscEmbedded ? 255 : cDebugFlag, __VA_ARGS__); }}
#define tscWarn(...)  { if (cDebugFlag & DEBUG_WARN)  { taosPrintLog("TSC WARN ", tscEmbedded ? 255 : cDebugFlag, __VA_ARGS__); }}
#define tscInfo(...)  { if (cDebugFlag & DEBUG_INFO)  { taosPrintLog("TSC ", tscEmbedded ? 255 : cDebugFlag, __VA_ARGS__); }}
#define tscDebug(...) { if (cDebugFlag & DEBUG_DEBUG) { taosPrintLog("TSC ", cDebugFlag, __VA_ARGS__); }}
#define tscTrace(...) { if (cDebugFlag & DEBUG_TRACE) { taosPrintLog("TSC ", cDebugFlag, __VA_ARGS__); }}
#define tscDebugL(...){ if (cDebugFlag & DEBUG_DEBUG) { taosPrintLongString("TSC ", cDebugFlag, __VA_ARGS__); }}

#ifdef __cplusplus
}
#endif

#endif

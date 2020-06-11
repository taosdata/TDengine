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

#ifndef TDENGINE_COMMON_ULOG_H
#define TDENGINE_COMMON_ULOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

extern int32_t uDebugFlag;
extern int32_t tscEmbedded;

#define uError(fmt, ...)       { if (uDebugFlag & DEBUG_ERROR) { TLOG("ERROR UTL ", uDebugFlag, fmt, ##__VA_ARGS__); }}
#define uWarn(fmt, ...)        { if (uDebugFlag & DEBUG_WARN)  { TLOG("WARN UTL ", uDebugFlag, fmt, ##__VA_ARGS__); }}
#define uTrace(fmt, ...)       { if (uDebugFlag & DEBUG_TRACE) { TLOG("UTL ", uDebugFlag, fmt, ##__VA_ARGS__); }}
#define uDump(x, y)       { if (uDebugFlag & DEBUG_DUMP)  { taosDumpData(x, y); }}
#define uPrint(fmt, ...)       { TLOG("UTL ", tscEmbedded ? 255 : uDebugFlag, fmt, ##__VA_ARGS__); }
#define uForcePrint(fmt, ...)  { TLOG("ERROR UTL ", 255, fmt, ##__VA_ARGS__); }

#define pError(fmt, ...) { TLOG("ERROR APP ", 255, fmt, ##__VA_ARGS__); }  
#define pPrint(fmt, ...) { TLOG("APP ", 255, fmt, ##__VA_ARGS__); }  

#ifdef __cplusplus
}
#endif

#endif

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

#define uError(...)       { if (uDebugFlag & DEBUG_ERROR) { taosPrintLog("ERROR UTL ", uDebugFlag, __VA_ARGS__); }}
#define uWarn(...)        { if (uDebugFlag & DEBUG_WARN)  { taosPrintLog("WARN UTL ", uDebugFlag, __VA_ARGS__); }}
#define uTrace(...)       { if (uDebugFlag & DEBUG_TRACE) { taosPrintLog("UTL ", uDebugFlag, __VA_ARGS__); }}
#define uDump(x, y)       { if (uDebugFlag & DEBUG_DUMP)  { taosDumpData(x, y); }}
#define uPrint(...)       { taosPrintLog("UTL ", tscEmbedded ? 255 : uDebugFlag, __VA_ARGS__); }
#define uForcePrint(...)  { taosPrintLog("ERROR UTL ", 255, __VA_ARGS__); }

#define pError(...) { taosPrintLog("ERROR APP ", 255, __VA_ARGS__); }  
#define pPrint(...) { taosPrintLog("APP ", 255, __VA_ARGS__); }  

#ifdef __cplusplus
}
#endif

#endif

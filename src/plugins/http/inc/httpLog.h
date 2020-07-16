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

#ifndef TDENGINE_HTTP_LOG_H
#define TDENGINE_HTTP_LOG_H

#include "tlog.h"

extern int32_t httpDebugFlag;

#define httpFatal(...) { if (httpDebugFlag & DEBUG_FATAL) { taosPrintLog("HTP FATAL ", 255, __VA_ARGS__); }}
#define httpError(...) { if (httpDebugFlag & DEBUG_ERROR) { taosPrintLog("HTP ERROR ", 255, __VA_ARGS__); }}
#define httpWarn(...)  { if (httpDebugFlag & DEBUG_WARN)  { taosPrintLog("HTP WARN ", 255, __VA_ARGS__); }}
#define httpInfo(...)  { if (httpDebugFlag & DEBUG_INFO)  { taosPrintLog("HTP ", 255, __VA_ARGS__); }}
#define httpDebug(...) { if (httpDebugFlag & DEBUG_DEBUG) { taosPrintLog("HTP ", httpDebugFlag, __VA_ARGS__); }}
#define httpTrace(...) { if (httpDebugFlag & DEBUG_TRACE) { taosPrintLog("HTP ", httpDebugFlag, __VA_ARGS__); }}
#define httpTraceL(...){ if (httpDebugFlag & DEBUG_TRACE) { taosPrintLongString("HTP ", httpDebugFlag, __VA_ARGS__); }}

#endif

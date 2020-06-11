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

#define httpError(fmt, ...) { if (httpDebugFlag & DEBUG_ERROR) { TLOG("ERROR HTP ", 255, fmt, ##__VA_ARGS__); }}
#define httpWarn(fmt, ...)  { if (httpDebugFlag & DEBUG_WARN)  { TLOG("WARN HTP ", httpDebugFlag, fmt, ##__VA_ARGS__); }}
#define httpTrace(fmt, ...) { if (httpDebugFlag & DEBUG_TRACE) { TLOG("HTP ", httpDebugFlag, fmt, ##__VA_ARGS__); }}
#define httpDump(fmt, ...)  { if (httpDebugFlag & DEBUG_TRACE) { TLOGLONG("HTP ", httpDebugFlag, fmt, ##__VA_ARGS__); }}
#define httpPrint(fmt, ...) { TLOG("HTP ", 255, fmt, ##__VA_ARGS__); }

#endif

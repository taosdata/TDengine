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

#ifndef TDENGINE_RPC_LOG_H
#define TDENGINE_RPC_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

extern int32_t rpcDebugFlag;

#define tError(fmt, ...) { if (rpcDebugFlag & DEBUG_ERROR) { TLOG("ERROR RPC ", rpcDebugFlag, fmt, ##__VA_ARGS__); }}
#define tWarn(fmt, ...)  { if (rpcDebugFlag & DEBUG_WARN)  { TLOG("WARN RPC ", rpcDebugFlag, fmt, ##__VA_ARGS__); }}
#define tTrace(fmt, ...) { if (rpcDebugFlag & DEBUG_TRACE) { TLOG("RPC ", rpcDebugFlag, fmt, ##__VA_ARGS__); }}
#define tDump(x, y) { if (rpcDebugFlag & DEBUG_DUMP)  { taosDumpData((unsigned char *)x, y); }}
#define tPrint(fmt, ...) { TLOG("RPC ", 255, fmt, ##__VA_ARGS__); }

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_RPC_LOG_H

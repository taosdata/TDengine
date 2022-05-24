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

#ifndef _TD_TRANSPORT_LOG_H
#define _TD_TRANSPORT_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#include "tlog.h"

#define tFatal(...) do {if (rpcDebugFlag & DEBUG_FATAL){ taosPrintLog("RPC FATAL ", DEBUG_FATAL, rpcDebugFlag, __VA_ARGS__); }} while (0)
#define tError(...)do { if (rpcDebugFlag & DEBUG_ERROR){ taosPrintLog("RPC ERROR ", DEBUG_ERROR, rpcDebugFlag, __VA_ARGS__); } } while(0)
#define tWarn(...) do { if (rpcDebugFlag & DEBUG_WARN) { taosPrintLog("RPC WARN ", DEBUG_WARN, rpcDebugFlag, __VA_ARGS__); }} while(0)
#define tInfo(...) do { if (rpcDebugFlag & DEBUG_INFO) { taosPrintLog("RPC ", DEBUG_INFO, rpcDebugFlag, __VA_ARGS__); }} while(0)
#define tDebug(...) do {if (rpcDebugFlag & DEBUG_DEBUG){ taosPrintLog("RPC ", DEBUG_DEBUG, rpcDebugFlag, __VA_ARGS__); }} while(0)
#define tTrace(...) do {if (rpcDebugFlag & DEBUG_TRACE){ taosPrintLog("RPC ", DEBUG_TRACE, rpcDebugFlag, __VA_ARGS__); }} while(0)                                                              
#define tDump(x, y) do {if (rpcDebugFlag & DEBUG_DUMP) { taosDumpData((unsigned char *)x, y); } } while(0)
// clang-format on 
#ifdef __cplusplus
}
#endif

#endif  // __TRANS_LOG_H

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
#include "ttrace.h"

#define tFatal(...) { if (rpcDebugFlag & DEBUG_FATAL) { taosPrintLog("RPC FATAL ", DEBUG_FATAL, rpcDebugFlag, __VA_ARGS__); }} 
#define tError(...) { if (rpcDebugFlag & DEBUG_ERROR ){ taosPrintLog("RPC ERROR ", DEBUG_ERROR, rpcDebugFlag, __VA_ARGS__); }}
#define tWarn(...)  { if (rpcDebugFlag & DEBUG_WARN)  { taosPrintLog("RPC WARN ",  DEBUG_WARN,  rpcDebugFlag, __VA_ARGS__); }}
#define tInfo(...)  { if (rpcDebugFlag & DEBUG_INFO)  { taosPrintLog("RPC ",       DEBUG_INFO,  rpcDebugFlag, __VA_ARGS__); }}
#define tDebug(...) { if (rpcDebugFlag & DEBUG_DEBUG) { taosPrintLog("RPC ",       DEBUG_DEBUG, rpcDebugFlag, __VA_ARGS__); }}
#define tTrace(...) { if (rpcDebugFlag & DEBUG_TRACE) { taosPrintLog("RPC ",       DEBUG_TRACE, rpcDebugFlag, __VA_ARGS__); }}                                                        
#define tDump(x, y) { if (rpcDebugFlag & DEBUG_DUMP)  { taosDumpData((unsigned char *)x, y); } }

#define tGTrace(param, ...) do { if (rpcDebugFlag & DEBUG_TRACE){char buf[40] = {0}; TRACE_TO_STR(trace, buf); tTrace(param ", gtid:%s", __VA_ARGS__, buf);}} while(0)
#define tGFatal(param, ...) do {if (rpcDebugFlag & DEBUG_FATAL){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); tFatal(param ", gtid:%s", __VA_ARGS__, buf); }} while (0)
#define tGError(param, ...) do { if (rpcDebugFlag & DEBUG_ERROR){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); tError(param ", gtid:%s", __VA_ARGS__, buf);} } while(0)
#define tGWarn(param, ...)  do { if (rpcDebugFlag & DEBUG_WARN) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); tWarn(param ", gtid:%s", __VA_ARGS__, buf); }} while(0)
#define tGInfo(param, ...)  do { if (rpcDebugFlag & DEBUG_INFO) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); tInfo(param ", gtid:%s", __VA_ARGS__, buf); }} while(0)
#define tGDebug(param,...) do {if (rpcDebugFlag & DEBUG_DEBUG){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); tDebug(param ", gtid:%s", __VA_ARGS__, buf); }} while(0)


// clang-format on 
#ifdef __cplusplus
}
#endif

#endif  // __TRANS_LOG_H

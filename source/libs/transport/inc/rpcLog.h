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

#define tFatal(...)                                                       \
  {                                                                       \
    if (rpcDebugFlag & DEBUG_FATAL) {                                     \
      taosPrintLog("RPC FATAL ", DEBUG_FATAL, rpcDebugFlag, __VA_ARGS__); \
    }                                                                     \
  }
#define tError(...)                                                       \
  {                                                                       \
    if (rpcDebugFlag & DEBUG_ERROR) {                                     \
      taosPrintLog("RPC ERROR ", DEBUG_ERROR, rpcDebugFlag, __VA_ARGS__); \
    }                                                                     \
  }
#define tWarn(...)                                                      \
  {                                                                     \
    if (rpcDebugFlag & DEBUG_WARN) {                                    \
      taosPrintLog("RPC WARN ", DEBUG_WARN, rpcDebugFlag, __VA_ARGS__); \
    }                                                                   \
  }
#define tInfo(...)                                                 \
  {                                                                \
    if (rpcDebugFlag & DEBUG_INFO) {                               \
      taosPrintLog("RPC ", DEBUG_INFO, rpcDebugFlag, __VA_ARGS__); \
    }                                                              \
  }
#define tDebug(...)                                                 \
  {                                                                 \
    if (rpcDebugFlag & DEBUG_DEBUG) {                               \
      taosPrintLog("RPC ", DEBUG_DEBUG, rpcDebugFlag, __VA_ARGS__); \
    }                                                               \
  }
#define tTrace(...)                                                 \
  {                                                                 \
    if (rpcDebugFlag & DEBUG_TRACE) {                               \
      taosPrintLog("RPC ", DEBUG_TRACE, rpcDebugFlag, __VA_ARGS__); \
    }                                                               \
  }
#define tDump(x, y)                        \
  {                                        \
    if (rpcDebugFlag & DEBUG_DUMP) {       \
      taosDumpData((unsigned char *)x, y); \
    }                                      \
  }

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_RPC_LOG_H

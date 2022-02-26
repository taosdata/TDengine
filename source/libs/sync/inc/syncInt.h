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

#ifndef _TD_LIBS_SYNC_INT_H
#define _TD_LIBS_SYNC_INT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "taosdef.h"

#define sFatal(...)                                        \
  {                                                        \
    if (sDebugFlag & DEBUG_FATAL) {                        \
      taosPrintLog("SYN FATAL ", sDebugFlag, __VA_ARGS__); \
    }                                                      \
  }
#define sError(...)                                        \
  {                                                        \
    if (sDebugFlag & DEBUG_ERROR) {                        \
      taosPrintLog("SYN ERROR ", sDebugFlag, __VA_ARGS__); \
    }                                                      \
  }
#define sWarn(...)                                        \
  {                                                       \
    if (sDebugFlag & DEBUG_WARN) {                        \
      taosPrintLog("SYN WARN ", sDebugFlag, __VA_ARGS__); \
    }                                                     \
  }
#define sInfo(...)                                   \
  {                                                  \
    if (sDebugFlag & DEBUG_INFO) {                   \
      taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); \
    }                                                \
  }
#define sDebug(...)                                  \
  {                                                  \
    if (sDebugFlag & DEBUG_DEBUG) {                  \
      taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); \
    }                                                \
  }
#define sTrace(...)                                  \
  {                                                  \
    if (sDebugFlag & DEBUG_TRACE) {                  \
      taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); \
    }                                                \
  }

typedef struct SSyncNode {
  char     path[TSDB_FILENAME_LEN];
  int8_t   replica;
  int8_t   quorum;
  int8_t   selfIndex;
  uint32_t vgId;
  int32_t  refCount;
  int64_t  rid;
} SSyncNode;

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_INT_H*/

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

#ifndef TDENGINE_WAL_INT_H
#define TDENGINE_WAL_INT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

extern int32_t wDebugFlag;

#define wFatal(...) { if (wDebugFlag & DEBUG_FATAL) { taosPrintLog("WAL FATAL ", 255, __VA_ARGS__); }}
#define wError(...) { if (wDebugFlag & DEBUG_ERROR) { taosPrintLog("WAL ERROR ", 255, __VA_ARGS__); }}
#define wWarn(...)  { if (wDebugFlag & DEBUG_WARN)  { taosPrintLog("WAL WARN ", 255, __VA_ARGS__); }}
#define wInfo(...)  { if (wDebugFlag & DEBUG_INFO)  { taosPrintLog("WAL ", 255, __VA_ARGS__); }}
#define wDebug(...) { if (wDebugFlag & DEBUG_DEBUG) { taosPrintLog("WAL ", wDebugFlag, __VA_ARGS__); }}
#define wTrace(...) { if (wDebugFlag & DEBUG_TRACE) { taosPrintLog("WAL ", wDebugFlag, __VA_ARGS__); }}

#define walPrefix "wal"
#define walRefreshIntervalMs 1000
#define walSignature (uint32_t)(0xFAFBFDFE)

typedef struct {
  uint64_t version;
  int32_t  vgId;
  int32_t  fd;
  int32_t  keep;
  int32_t  level;
  int32_t  fsyncPeriod;
  int32_t  fsyncSeq;
  int32_t  fileIndex;
  void*    timer;
  void*    signature;
  int      max;  // maximum number of wal files
  uint32_t id;   // increase continuously
  int      num;  // number of wal files
  char     path[TSDB_FILENAME_LEN];
  char     name[TSDB_FILENAME_LEN + 16];
  pthread_mutex_t mutex;
} SWal;

#ifdef __cplusplus
}
#endif

#endif

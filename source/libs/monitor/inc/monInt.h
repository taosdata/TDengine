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

#ifndef _TD_MONITOR_INT_H_
#define _TD_MONITOR_INT_H_

#include "monitor.h"

#include "tarray.h"
#include "tjson.h"

typedef struct {
  int64_t   ts;
  ELogLevel level;
  char      content[MON_LOG_LEN];
} SMonLogItem;

typedef struct {
  int64_t time;
  int64_t req_select;
  int64_t req_insert;
  int64_t req_insert_batch;
  int64_t net_in;
  int64_t net_out;
  int64_t io_read;
  int64_t io_write;
  int64_t io_read_disk;
  int64_t io_write_disk;
} SMonState;

typedef struct SMonInfo {
  int64_t   curTime;
  SMonState lastState;
  SArray   *logs;  // array of SMonLogItem
  SJson    *pJson;
} SMonInfo;

typedef struct {
  TdThreadMutex lock;
  SArray         *logs;  // array of SMonLogItem
  int32_t         maxLogs;
  const char     *server;
  uint16_t        port;
  bool            comp;
  SMonState       state;
} SMonitor;

#ifdef __cplusplus
}
#endif

#endif /*_TD_MONITOR_INT_H_*/

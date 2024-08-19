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

#ifndef TDENGINE_CLIENT_MONITOR_H
#define TDENGINE_CLIENT_MONITOR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "taos_monitor.h"
#include "thash.h"
#include "query.h"
#include "tqueue.h"

typedef enum {
  SQL_RESULT_SUCCESS = 0,
  SQL_RESULT_FAILED = 1,
  SQL_RESULT_CANCEL = 2,
} SQL_RESULT_CODE;

typedef enum {
    SLOW_LOG_WRITE = 0,
    SLOW_LOG_READ_RUNNING = 1,
    SLOW_LOG_READ_BEGINNIG = 2,
    SLOW_LOG_READ_QUIT = 3,
} SLOW_LOG_QUEUE_TYPE;

static char* queueTypeStr[] = {
    "SLOW_LOG_WRITE",
    "SLOW_LOG_READ_RUNNING",
    "SLOW_LOG_READ_BEGINNIG",
    "SLOW_LOG_READ_QUIT"
};

#define SLOW_LOG_SEND_SIZE_MAX 1024*1024

typedef struct {
  int64_t                    clusterId;
  taos_collector_registry_t* registry;
  taos_collector_t*          colector;
  SHashObj*                  counters;
  void*                      timer;
} MonitorClient;

typedef struct {
  TdFilePtr                  pFile;
  int64_t                    lastCheckTime;
  char                       path[PATH_MAX];
  int64_t                    offset;
} SlowLogClient;

typedef struct {
  int64_t             clusterId;
  SLOW_LOG_QUEUE_TYPE type;
  char*               data;
  int64_t             offset;
  TdFilePtr           pFile;
  char*               fileName;
} MonitorSlowLogData;

void            monitorClose();
int32_t         monitorInit();

void            monitorClientSQLReqInit(int64_t clusterKey);
void            monitorClientSlowQueryInit(int64_t clusterId);
void            monitorCreateClient(int64_t clusterId);
void            monitorCreateClientCounter(int64_t clusterId, const char* name, const char* help, size_t label_key_count, const char** label_keys);
void            monitorCounterInc(int64_t clusterId, const char* counterName, const char** label_values);
const char*     monitorResultStr(SQL_RESULT_CODE code);
int32_t         monitorPutData2MonitorQueue(MonitorSlowLogData data);
#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENT_MONITOR_H

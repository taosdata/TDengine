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

typedef enum SQL_RESULT_CODE {
  SQL_RESULT_SUCCESS = 0,
  SQL_RESULT_FAILED = 1,
  SQL_RESULT_CANCEL = 2,
} SQL_RESULT_CODE;

#define SLOW_LOG_SEND_SIZE 1024*1024
extern tsem2_t     monitorSem;
extern STaosQueue* monitorQueue;

typedef struct {
  int64_t                    clusterId;
  taos_collector_registry_t* registry;
  taos_collector_t*          colector;
  SHashObj*                  counters;
  void*                      timer;
} MonitorClient;

typedef struct {
  TdFilePtr                  pFile;
  void*                      timer;
} SlowLogClient;

typedef struct {
  int64_t  clusterId;
  char    *value;
} MonitorSlowLogData;

void            monitorClose();
void            monitorInit();
void            monitorSendAllSlowLogFromTempDir(void* pInst);

void            monitorClientSQLReqInit(int64_t clusterKey);
void            monitorClientSlowQueryInit(int64_t clusterId);
void            monitorCreateClient(int64_t clusterId);
void            monitorCreateClientCounter(int64_t clusterId, const char* name, const char* help, size_t label_key_count, const char** label_keys);
void            monitorCounterInc(int64_t clusterId, const char* counterName, const char** label_values);
void*           monitorThreadFunc(void *param);
void            monitorFreeSlowLogData(MonitorSlowLogData* pData);
const char*     monitorResultStr(SQL_RESULT_CODE code);
void            monitorReadSendSlowLog(TdFilePtr pFile, void* pTransporter, SEpSet *epSet);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENT_MONITOR_H

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

typedef enum SQL_RESULT_CODE {
  SQL_RESULT_SUCCESS = 0,
  SQL_RESULT_FAILED = 1,
  SQL_RESULT_CANCEL = 2,
} SQL_RESULT_CODE;

const char* resultStr(SQL_RESULT_CODE code);

typedef struct {
  char                       clusterKey[512];
  SEpSet                     epSet;
  void*                      pTransporter;
  taos_collector_registry_t* registry;
  taos_collector_t*          colector;
  SHashObj*                  counters;
} ClientMonitor;

void            clusterMonitorInit(const char* clusterKey, SEpSet epSet, void* pTransporter);
void            clusterMonitorClose(const char* clusterKey);
taos_counter_t* createClusterCounter(const char* clusterKey, const char* name, const char* help, size_t label_key_count,
                                     const char** label_keys);
int             taosClusterCounterInc(const char* clusterKey, const char* counterName, const char** label_values);
void            cluster_monitor_stop();

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENT_MONITOR_H

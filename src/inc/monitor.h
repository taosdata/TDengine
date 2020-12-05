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

#ifndef TDENGINE_MONITOR_H
#define TDENGINE_MONITOR_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef struct {
  char *  acctId;
  int64_t currentPointsPerSecond;
  int64_t maxPointsPerSecond;
  int64_t totalTimeSeries;
  int64_t maxTimeSeries;
  int64_t totalStorage;
  int64_t maxStorage;
  int64_t totalQueryTime;
  int64_t maxQueryTime;
  int64_t totalInbound;
  int64_t maxInbound;
  int64_t totalOutbound;
  int64_t maxOutbound;
  int64_t totalDbs;
  int64_t maxDbs;
  int64_t totalUsers;
  int64_t maxUsers;
  int64_t totalStreams;
  int64_t maxStreams;
  int64_t totalConns;
  int64_t maxConns;
  int8_t  accessState;
} SAcctMonitorObj;

int32_t monInitSystem();
int32_t monStartSystem();
void    monStopSystem();
void    monCleanupSystem();
void    monSaveAcctLog(SAcctMonitorObj *pMonObj);
void    monSaveLog(int32_t level, const char *const format, ...);
void    monExecuteSQL(char *sql);

#ifdef __cplusplus
}
#endif

#endif

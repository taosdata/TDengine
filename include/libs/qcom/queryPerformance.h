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

#ifndef TDENGINE_QUERY_PERFORMANCE_H
#define TDENGINE_QUERY_PERFORMANCE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "taosdef.h"

typedef struct SQueryPerformanceMetrics {
  int64_t rowsScanned;
  int64_t rowsReturned;
  int64_t memoryUsed;
  int32_t vnodeId;
} SQueryPerformanceMetrics;

void    qInitPerformanceMetrics(SQueryPerformanceMetrics* pMetrics);
void    qCollectPerformanceMetrics(void* pTaskInfo, SQueryPerformanceMetrics* pMetrics);
void    qCollectScanStats(void* pRecorder, SQueryPerformanceMetrics* pMetrics);
int64_t qGetQueryMemoryUsage(void);

#ifdef __cplusplus
}
#endif

#endif

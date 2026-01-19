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

#include "queryPerformance.h"
// #include "executorInt.h"
// #include "querytask.h"
#include "taos.h"
#include "taoserror.h"
#include "tlog.h"
#include "tmsg.h"

void qInitPerformanceMetrics(SQueryPerformanceMetrics* pMetrics) {
  if (NULL == pMetrics) {
    return;
  }

  pMetrics->rowsScanned = 0;
  pMetrics->rowsReturned = 0;
  pMetrics->memoryUsed = 0;
  pMetrics->vnodeId = 0;
}

void qCollectScanStats(void* pRecorder, SQueryPerformanceMetrics* pMetrics) {
  if (NULL == pRecorder || NULL == pMetrics) {
    return;
  }

  // SFileBlockLoadRecorder* pRec = (SFileBlockLoadRecorder*)pRecorder;
  // pMetrics->rowsScanned += pRec->totalRows;
}

void qCollectPerformanceMetrics(void* pTaskInfo, SQueryPerformanceMetrics* pMetrics) {
  if (NULL == pTaskInfo || NULL == pMetrics) {
    return;
  }

  // SExecTaskInfo* pTask = (SExecTaskInfo*)pTaskInfo;

  // pMetrics->rowsReturned = pTask->pRoot->resultInfo.totalRows;
  // pMetrics->vnodeId = pTask->id.vgId;

  // pMetrics->memoryUsed = qGetQueryMemoryUsage();
}

int64_t qGetQueryMemoryUsage(void) {
  return 0;
  // struct rusage usage;
  // if (getrusage(RUSAGE_SELF, &usage) != 0) {
  //   return 0;
  // }

  // int64_t memoryUsed = usage.ru_maxrss;
  // memoryUsed *= 1024;

  // return memoryUsed;
}

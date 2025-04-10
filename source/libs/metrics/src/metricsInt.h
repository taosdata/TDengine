/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef _TD_METRICS_INT_H_
#define _TD_METRICS_INT_H_

#include "libs/metrics/metrics.h"
#include "taoserror.h"
#include "tarray.h"
#include "thash.h"
#include "tlog.h"
#include "tthread.h"

#ifdef __cplusplus
extern "C" {
#endif

// Internal structure to hold both raw and formatted metrics
typedef struct {
  int32_t          vgId;  // Key for the hash table
  SRawWriteMetrics raw;
  SWriteMetricsEx  formatted;
  // Add a flag or timestamp if lazy update is needed
  // bool             isFormattedUpToDate;
} SWriteMetricsBundle;

// Function declarations used internally in metrics.c
static void destroyMetricsManager();
static uint32_t writeMetricsHashFn(const void *key, uint32_t keyLen, uint32_t seed);
static int32_t  writeMetricsKeyCompareFn(const void *key, int32_t keyLen, const void *pData);
static void     freeWriteMetricsBundle(void *pData);
static void     updateFormattedMetrics(SWriteMetricsBundle *pBundle);

#ifdef __cplusplus
}
#endif

#endif /* _TD_METRICS_INT_H_ */
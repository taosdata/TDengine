/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifndef TD_NODES_INFLUXQL_H_
#define TD_NODES_INFLUXQL_H_

#include "plannodes.h"
#include "tarray.h"
#include "tstrbuild.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum EInfluxQLOutputRole {
  INFLUXQL_OUTPUT_VALUE = 0,
  INFLUXQL_OUTPUT_TIME,
  INFLUXQL_OUTPUT_TAG,
  INFLUXQL_OUTPUT_WEND,
  INFLUXQL_OUTPUT_WDURATION
} EInfluxQLOutputRole;

typedef struct SInfluxQLOutput {
  char name[TSDB_COL_NAME_LEN];
  char sourceName[TSDB_COL_NAME_LEN];
  char countName[TSDB_COL_NAME_LEN];  // hidden sample count for STDDEV n=1
  SDataType dataType;
  EInfluxQLOutputRole role;
  bool emptyCount;
} SInfluxQLOutput;

typedef struct SInfluxQLColumnRef {
  char name[TSDB_COL_NAME_LEN];
} SInfluxQLColumnRef;

// Clause bodies omit their SQL keywords. A NULL pSelect retains the raw scan's
// complete Field anchoring projection. All strings and arrays are owned here.
typedef struct SInfluxQLRender {
  char *pSelect;
  char *pWhere;
  char *pGroupBy;
  char *pOrderBy;
  char *pLimit;
  bool hasLimit;
  int64_t limit;
  int64_t offset;
  SArray *pOutputs;  // SInfluxQLOutput, in remote select-output order
  SArray *pReferencedColumns;  // SInfluxQLColumnRef, original measurement keys
  bool aggregate;
  bool grouped;
  bool tagGrouped;
  bool timeGrouped;
  bool hasTimeRange;
  STimeWindow scanRange;  // inclusive, in rangePrecision
  int8_t rangePrecision;
  int64_t windowStep;  // bucket step in windowPrecision
  int8_t windowPrecision;  // _fq_time_bucket source precision
} SInfluxQLRender;

// Pure rendering. Rejects unsupported trees without returning a partial result.
int32_t nodesRemotePlanToInfluxQL(const SLogicNode *pPlan,
                                 SInfluxQLRender **ppRender);
void nodesDestroyInfluxQLRender(SInfluxQLRender *pRender);

// Shared InfluxQL codec. The caller owns the returned quote string. Appending
// uses the caller's SStringBuilder allocation-failure jump context.
void nodesInfluxQLAppendQuoted(SStringBuilder *pBuilder, const char *pValue,
                               char quote);
char *nodesInfluxQLQuote(const char *pValue, char quote);
// Extract a string constant as UTF-8. The caller owns the returned buffer.
int32_t nodesInfluxQLValueString(const SValueNode *pValue, char **ppString);

#ifdef __cplusplus
}
#endif
#endif  // TD_NODES_INFLUXQL_H_

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

#ifndef _TD_QUERY_INT_H_
#define _TD_QUERY_INT_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "nodes.h"
#include "plannodes.h"
#include "ttime.h"

#define EXPLAIN_MAX_GROUP_NUM 100

//newline area
#define EXPLAIN_TAG_SCAN_FORMAT "Tag Scan on %s"
#define EXPLAIN_TBL_SCAN_FORMAT "Table Scan on %s"
#define EXPLAIN_SYSTBL_SCAN_FORMAT "System Table Scan on %s"
#define EXPLAIN_PROJECTION_FORMAT "Projection"
#define EXPLAIN_JOIN_FORMAT "%s"
#define EXPLAIN_AGG_FORMAT "Aggragate"
#define EXPLAIN_EXCHANGE_FORMAT "Data Exchange %d:1"
#define EXPLAIN_SORT_FORMAT "Sort"
#define EXPLAIN_INTERVAL_FORMAT "Interval on Column %s"
#define EXPLAIN_SESSION_FORMAT "Session"
#define EXPLAIN_ORDER_FORMAT "Order: %s"
#define EXPLAIN_FILTER_FORMAT "Filter: "
#define EXPLAIN_FILL_FORMAT "Fill: %s"
#define EXPLAIN_ON_CONDITIONS_FORMAT "Join Cond: "
#define EXPLAIN_TIMERANGE_FORMAT "Time Range: [%" PRId64 ", %" PRId64 "]"
#define EXPLAIN_OUTPUT_FORMAT "Output: "
#define EXPLAIN_TIME_WINDOWS_FORMAT "Time Window: interval=%" PRId64 "%c offset=%" PRId64 "%c sliding=%" PRId64 "%c"
#define EXPLAIN_WINDOW_FORMAT "Window: gap=%" PRId64
#define EXPLAIN_RATIO_TIME_FORMAT "Ratio: %f"
#define EXPLAIN_PLANNING_TIME_FORMAT "Planning Time: %.3f ms"
#define EXPLAIN_EXEC_TIME_FORMAT "Execution Time: %.3f ms"

//append area
#define EXPLAIN_LEFT_PARENTHESIS_FORMAT " ("
#define EXPLAIN_RIGHT_PARENTHESIS_FORMAT ")"
#define EXPLAIN_BLANK_FORMAT " "
#define EXPLAIN_COST_FORMAT "cost=%.2f..%.2f"
#define EXPLAIN_ROWS_FORMAT "rows=%" PRIu64
#define EXPLAIN_COLUMNS_FORMAT "columns=%d"
#define EXPLAIN_WIDTH_FORMAT "width=%d"
#define EXPLAIN_GROUPS_FORMAT "groups=%d"
#define EXPLAIN_WIDTH_FORMAT "width=%d"
#define EXPLAIN_LOOPS_FORMAT "loops=%d"
#define EXPLAIN_REVERSE_FORMAT "reverse=%d"
#define EXPLAIN_FUNCTIONS_FORMAT "functions=%d"
#define EXPLAIN_EXECINFO_FORMAT "cost=%" PRIu64 "..%" PRIu64 " rows=%" PRIu64

typedef struct SExplainGroup {
  int32_t   nodeNum;
  int32_t   physiPlanExecNum;
  int32_t   physiPlanNum;
  int32_t   physiPlanExecIdx;
  SRWLatch  lock;
  SSubplan *plan;
  SArray   *nodeExecInfo;      //Array<SExplainRsp>
} SExplainGroup;

typedef struct SExplainResNode {
  SNodeList*        pChildren;
  SPhysiNode*       pNode;
  SArray*           pExecInfo; // Array<SExplainExecInfo>
} SExplainResNode;

typedef struct SQueryExplainRowInfo {
  int32_t level;
  int32_t len;
  char   *buf;
} SQueryExplainRowInfo;

typedef struct SExplainCtx {
  EExplainMode mode;
  double       ratio;
  bool         verbose;

  SRWLatch     lock;
  int32_t      rootGroupId;
  int32_t      dataSize;
  bool         execDone;
  int64_t      reqStartTs;
  int64_t      jobStartTs;
  int64_t      jobDoneTs;
  char        *tbuf;
  SArray      *rows;
  int32_t      groupDoneNum;
  SHashObj    *groupHash;     // Hash<SExplainGroup>
} SExplainCtx;

#define EXPLAIN_ORDER_STRING(_order) ((TSDB_ORDER_ASC == _order) ? "Ascending" : "Descending")
#define EXPLAIN_JOIN_STRING(_type) ((JOIN_TYPE_INNER == _type) ? "Inner join" : "Join")

#define INVERAL_TIME_FROM_PRECISION_TO_UNIT(_t, _u, _p) (((_u) == 'n' || (_u) == 'y') ? (_t) : (convertTimeFromPrecisionToUnit(_t, _p, _u)))

#define EXPLAIN_ROW_NEW(level, ...)                                                                               \
  do {                                                                                                            \
    if (isVerboseLine) {                                                                                          \
      tlen = snprintf(tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE - VARSTR_HEADER_SIZE, "%*s", (level) * 2 + 3, "");       \
    } else {                                                                                                      \
      tlen = snprintf(tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE - VARSTR_HEADER_SIZE, "%*s%s", (level) * 2, "", "-> ");  \
    }                                                                                                             \
    tlen += snprintf(tbuf + VARSTR_HEADER_SIZE + tlen, TSDB_EXPLAIN_RESULT_ROW_SIZE - VARSTR_HEADER_SIZE - tlen, __VA_ARGS__);         \
  } while (0)
  
#define EXPLAIN_ROW_APPEND(...) tlen += snprintf(tbuf + VARSTR_HEADER_SIZE + tlen, TSDB_EXPLAIN_RESULT_ROW_SIZE - VARSTR_HEADER_SIZE - tlen, __VA_ARGS__)
#define EXPLAIN_ROW_END() do { varDataSetLen(tbuf, tlen); tlen += VARSTR_HEADER_SIZE; isVerboseLine = true; } while (0)

#define EXPLAIN_SUM_ROW_NEW(...) tlen = snprintf(tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE - VARSTR_HEADER_SIZE, __VA_ARGS__)
#define EXPLAIN_SUM_ROW_END() do { varDataSetLen(tbuf, tlen); tlen += VARSTR_HEADER_SIZE; } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_QUERY_INT_H_*/
